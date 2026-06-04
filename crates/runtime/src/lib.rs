use std::{
    cell::Cell,
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    env, fs,
    io::{Read, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration as StdDuration, Instant},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use common::{
    Canonicality, EventMeta, EventPayload, EventSource, FillEvent, LoadedConfig, NormalizedEvent,
    PubkeyValue, ReasonCode, RuntimeModeName, TokenTerminalEvent, TokenTerminalVariant,
    TradeDecision, TradeDecisionEvent, monotonic_now_ns, unix_now,
};
use decision::{DecisionEngine, DecisionOutcome};
use event_bus::{EventBus, EventBusError, Priority};
use executor::PaperExecutor;
use features::{FeatureEngine, FeatureSnapshot};
use idl::LoadedIdl;
use ingest_geyser::GeyserIngestService;
use ingest_shred::{
    DecodedShredBatch, FixtureShredDecoder, ProductionShredDecoder, ReceivedPacket,
    ReconciliationConfig, ShredIngestService, ShredMetrics,
};
use metrics::QuantMetrics;
use risk::{DiscardPolicyDecision, RiskAssessment, RiskEngine};
use rpc_budget::RpcBudgetManager;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sim::{FeeModel, Simulator};
use state::{StateEngine, StateSnapshot, TokenLifecycle};
use storage::{DatasetKind, StorageEngine, StorageLayout, StoredRecord};
use storage::{RunKind, RunMetadata, RunStatus};
use time::{Duration, OffsetDateTime};
pub mod fixtures;
mod live_source;
mod segments;
mod shred_exit;

pub use fixtures::{
    FixtureExpectation, FixtureScenario, FixtureScenarioKind, FixtureScenarioSpec,
    build_fixture_scenario, builtin_fixture_suite, builtin_shred_exit_fixture_suite,
    load_fixture_spec,
};
pub use live_source::{
    DeshredCapability, DeshredProviderSmokeOptions, DeshredProviderSmokeSummary,
    FreshLaunchCanaryLiveOptions, FreshLaunchCanaryLiveSummary, GeyserProviderSmokeOptions,
    GeyserProviderSmokeSummary, MaterialHunterStreamAction, MaterialHunterStreamOptions,
    MaterialHunterStreamStateHint, MaterialHunterStreamSummary, collect_fresh_launch_canary_events,
    inspect_deshred_capability, material_hunter_subscription_fingerprint,
    run_material_hunter_stream, run_material_hunter_stream_with_progress, smoke_deshred_provider,
    smoke_geyser_provider,
};
use live_source::{
    DeshredStreamConnector, GeyserEventNormalizer, GeyserStreamConnector, RealDeshredConnector,
    RealGeyserConnector, run_deshred_source_with_connector, run_geyser_source_with_connector,
};
use segments::LiveRunSegmentManager;
use shred_exit::TentativeSellManager;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeMode {
    Fixture,
    Replay,
    PaperFromStore,
    LiveDataPaper,
    EdgeCollector,
    ResearchWorker,
    Autopilot,
    GuardedLiveDryRun,
    GuardedLiveEnabled,
}

impl From<RuntimeModeName> for RuntimeMode {
    fn from(value: RuntimeModeName) -> Self {
        match value {
            RuntimeModeName::Fixture => Self::Fixture,
            RuntimeModeName::Replay => Self::Replay,
            RuntimeModeName::PaperFromStore => Self::PaperFromStore,
            RuntimeModeName::LiveDataPaper => Self::LiveDataPaper,
            RuntimeModeName::EdgeCollector => Self::EdgeCollector,
            RuntimeModeName::ResearchWorker => Self::ResearchWorker,
            RuntimeModeName::Autopilot => Self::Autopilot,
            RuntimeModeName::GuardedLiveDryRun => Self::GuardedLiveDryRun,
            RuntimeModeName::GuardedLiveEnabled => Self::GuardedLiveEnabled,
        }
    }
}

impl Default for RuntimeMode {
    fn default() -> Self {
        Self::Fixture
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("storage error: {0}")]
    Storage(#[from] storage::StorageError),
    #[error("state error: {0}")]
    State(#[from] state::StateError),
    #[error("fixture error: {0}")]
    Fixture(String),
    #[error("runtime blocked: {0}")]
    Blocked(String),
    #[error("shred ingest error: {0}")]
    Shred(#[from] ingest_shred::ShredIngestError),
    #[error("production shred decoder not enabled")]
    ProductionShredDecoderDisabled,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeHealth {
    pub early_intent_enabled: bool,
    pub early_intent_sources_active: Vec<String>,
    pub stream_only_enabled: bool,
    pub stream_only_passed: bool,
    pub rpc_network_calls_total: u64,
    pub rpc_credits_used_total: u64,
    pub rpc_denials_total: u64,
    pub market_data_rpc_calls_allowed: bool,
    pub holder_rpc_calls_allowed: bool,
    pub metadata_fetch_allowed: bool,
    pub confirmation_rpc_allowed: bool,
    pub blockhash_rpc_allowed: bool,
    pub geyser_connected: bool,
    pub geyser_endpoint_configured: bool,
    pub geyser_auth_configured: bool,
    pub geyser_last_event_time: Option<OffsetDateTime>,
    pub geyser_reconnect_count: u64,
    pub geyser_gap_count: u64,
    pub geyser_events_received: u64,
    pub deshred_supported: bool,
    pub deshred_connected: bool,
    pub deshred_provider_status: String,
    pub deshred_endpoint_configured: bool,
    pub deshred_auth_configured: bool,
    pub deshred_events_received: u64,
    pub deshred_transactions_decoded: u64,
    pub deshred_decode_errors: u64,
    pub deshred_reconnect_count: u64,
    pub deshred_unsupported_count: u64,
    pub deshred_tentative_sells_total: u64,
    pub deshred_malicious_warnings_total: u64,
    pub deshred_emergency_exits_armed_total: u64,
    pub deshred_emergency_exits_triggered_total: u64,
    pub deshred_confirmed_executed_total: u64,
    pub deshred_false_positive_total: u64,
    pub deshred_saved_loss_quote_total: Decimal,
    pub deshred_opportunity_cost_quote_total: Decimal,
    pub raw_shred_connected: bool,
    pub raw_shred_supported: bool,
    pub raw_shred_tentative_sells_total: u64,
    pub mock_early_intent_active: bool,
    pub mock_early_intent_tentative_sells_total: u64,
    pub fixture_early_intent_tentative_sells_total: u64,
    pub replay_early_intent_tentative_sells_total: u64,
    pub source_dedup_count: u64,
    pub early_intent_dedup_pairs: BTreeMap<String, u64>,
    pub shred_connected: bool,
    pub storage_healthy: bool,
    pub rpc_budget_healthy: bool,
    pub event_queue_depth: usize,
    pub tentative_queue_depth: usize,
    pub canonical_queue_depth: usize,
    pub active_tokens: usize,
    pub active_positions: usize,
    pub data_gap_active: bool,
    pub data_gap_scope: Option<String>,
    pub kill_switch_active: bool,
    pub paper_pnl: Decimal,
    pub realized_pnl: Decimal,
    pub fee_drag: Decimal,
    pub slippage_drag: Decimal,
    pub latency_drag: Decimal,
    pub events_processed: u64,
    pub canonical_events_dropped: u64,
    pub skipped_feature_snapshot_count: u64,
    pub discarded_tokens: usize,
    pub rugged_tokens: usize,
    pub last_event_time: Option<OffsetDateTime>,
    pub last_decision_time: Option<OffsetDateTime>,
    pub last_persist_time: Option<OffsetDateTime>,
    pub runtime_uptime_ms: u64,
    pub mode: RuntimeMode,
    pub live_source_mode: String,
    pub shred_tentative_sells_total: u64,
    pub shred_malicious_sell_warnings_total: u64,
    pub shred_emergency_exits_armed_total: u64,
    pub shred_emergency_exits_triggered_total: u64,
    pub shred_confirmed_executed_total: u64,
    pub shred_confirmed_failed_total: u64,
    pub shred_not_seen_within_ttl_total: u64,
    pub shred_reorged_total: u64,
    pub shred_decode_mismatch_total: u64,
    pub shred_account_effect_confirmed_total: u64,
    pub shred_sell_false_positive_total: u64,
    pub shred_saved_loss_quote_total: Decimal,
    pub shred_opportunity_cost_quote_total: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeSafety {
    pub trade_allowed: bool,
    pub paper_allowed: bool,
    pub live_allowed: bool,
    pub data_quality_allowed: bool,
    pub rpc_budget_allowed: bool,
    pub max_loss_allowed: bool,
    pub stale_data_allowed: bool,
    pub reason_codes: Vec<ReasonCode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeAuditKind {
    RuntimeStarted,
    RuntimeStopped,
    IngestStarted,
    IngestStopped,
    StorageWarning,
    StorageLimitReached,
    DataGapStarted,
    DataGapRecovered,
    QueueOverflow,
    KillSwitchActivated,
    KillSwitchCleared,
    PaperDecision,
    PaperFill,
    LiveOrderRejected,
    RpcBudgetDenied,
    ConfigLoaded,
    IdlLoaded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeAuditEvent {
    pub at: OffsetDateTime,
    pub kind: RuntimeAuditKind,
    pub run_id: String,
    pub details: BTreeMap<String, String>,
    pub reason_codes: Vec<ReasonCode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSummary {
    pub run_id: String,
    pub source_run_id: Option<String>,
    pub scenario_id: Option<String>,
    pub config_hash: String,
    pub idl_hash: String,
    pub mode: RuntimeMode,
    pub health: RuntimeHealth,
    pub safety: RuntimeSafety,
    pub decisions_by_type: BTreeMap<String, u64>,
    pub fills: Vec<FillEvent>,
    pub decision_events: Vec<TradeDecisionEvent>,
    pub decision_outcomes: Vec<DecisionOutcome>,
    pub latest_features: HashMap<String, FeatureSnapshot>,
    pub latest_risk: HashMap<String, RiskAssessment>,
    pub snapshot: StateSnapshot,
    pub audits: Vec<RuntimeAuditEvent>,
    pub replay_profile: RuntimeReplayProfile,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuntimeReplayProfile {
    pub substage_timings_ms: BTreeMap<String, u128>,
    pub counters: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EdgeCollectorStatusReport {
    run_id: String,
    mode: RuntimeMode,
    updated_at: OffsetDateTime,
    stream_only_passed: bool,
    rpc_network_calls_total: u64,
    rpc_credits_used_total: u64,
    events_processed: u64,
    active_tokens: usize,
    data_gap_active: bool,
    data_gap_scope: Option<String>,
    provider_status: String,
    notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EdgeSlotRecord {
    run_id: String,
    source: EventSource,
    slot: u64,
    parent_slot: Option<u64>,
    event_time: OffsetDateTime,
    canonicality: Canonicality,
    data_gap_active: bool,
    stream_only_passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EdgeEventRecord {
    run_id: String,
    sequence_number: u64,
    source: EventSource,
    slot: u64,
    event_time: OffsetDateTime,
    signature: Option<String>,
    mint: Option<String>,
    event_type: String,
    program_id: Option<String>,
    instruction_discriminator: Option<String>,
    decoded_fields: serde_json::Value,
    account_keys: Vec<String>,
    token_balance_deltas: Vec<String>,
    transaction_status: Option<String>,
    compute_budget: BTreeMap<String, String>,
    raw_update_hash: Option<String>,
    canonicality: Canonicality,
    data_gap_flags: Vec<String>,
    stream_only_passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeHttpSnapshot {
    pub health: RuntimeHealth,
    pub safety: RuntimeSafety,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug)]
pub struct MetricsServerHandle {
    stop: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
}

impl MetricsServerHandle {
    fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataGapScope {
    Global,
    Source,
    SlotRange,
    Token,
    Run,
    Scenario,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScopedDataGap {
    pub scope: DataGapScope,
    pub source: EventSource,
    pub start_slot: u64,
    pub end_slot: Option<u64>,
    pub severity: common::GapSeverity,
    pub active: bool,
    pub trade_blocking: bool,
    pub affected_token: Option<String>,
    pub run_id: String,
    pub scenario_id: Option<String>,
    pub reason_codes: Vec<ReasonCode>,
    pub recovery_action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureRunResult {
    pub scenario_name: String,
    pub scenario_id: String,
    pub events_processed: usize,
    pub decisions_made: u64,
    pub fills_simulated: usize,
    pub final_lifecycle: String,
    pub final_pnl: Decimal,
    pub rug_score: Decimal,
    pub bundle_score: Decimal,
    pub discard_reason: Option<String>,
    pub expected_summary: String,
    pub actual_decisions: Vec<String>,
    pub actual_risk_flags: Vec<String>,
    pub actual_discard_state: Option<String>,
    pub actual_paper_fills: usize,
    pub top_blocking_reasons: Vec<String>,
    pub top_positive_signals: Vec<String>,
    pub top_negative_signals: Vec<String>,
    pub data_gap_state: Option<String>,
    pub false_discard_detected: bool,
    pub early_sell_warning: bool,
    pub exit_armed: bool,
    pub emergency_exit: bool,
    pub saved_loss_quote: Decimal,
    pub opportunity_cost_quote: Decimal,
    pub false_positive_tentative: bool,
    pub reconciliation_outcome: Option<String>,
    pub confirmation_level: Option<String>,
    pub passed_expectation: bool,
}

#[derive(Debug, Clone, Default)]
pub struct LiveRunOptions {
    pub max_events: Option<usize>,
    pub max_decisions: Option<usize>,
    pub duration: Option<Duration>,
    pub health_interval_ms: Option<u64>,
    pub geyser_only: bool,
    pub no_shred: bool,
    pub with_deshred: bool,
    pub require_deshred: bool,
    pub deshred_only_capability_check: bool,
    pub dry_run: bool,
    pub mock_live: bool,
    pub mock_early_intent: bool,
    pub mock_early_intent_scenario: Option<String>,
    pub mock_deshred: bool,
    pub mock_deshred_scenario: Option<String>,
    pub run_id: Option<String>,
    pub report_dir: Option<String>,
}

fn filesystem_free_mb(path: &Path) -> Result<u64> {
    let stats = rustix::fs::statvfs(path)
        .map_err(|error| anyhow!("statvfs failed for {}: {error}", path.display()))?;
    Ok(stats
        .f_bavail
        .saturating_mul(stats.f_frsize)
        .saturating_div(1024 * 1024))
}

#[derive(Debug, Clone)]
pub struct FixtureSource {
    pub scenario: FixtureScenario,
}

#[derive(Debug, Clone)]
pub struct StoreReplaySource {
    pub run_id: String,
    pub scenario_id: Option<String>,
}

pub struct GeyserLiveSource {
    pub config: common::GeyserConfig,
    normalizer: GeyserEventNormalizer,
    connector: Arc<dyn GeyserStreamConnector>,
}

#[derive(Debug, Clone)]
pub struct ShredLiveSource {
    pub config: common::ShredConfig,
}

#[derive(Debug, Clone)]
pub struct MockGeyserLiveSource {
    pub events: Vec<NormalizedEvent>,
}

#[derive(Debug, Clone)]
pub struct MockShredLiveSource {
    pub batches: Vec<DecodedShredBatch>,
}

#[derive(Clone)]
pub struct DeshredLiveSource {
    pub config: common::DeshredConfig,
    pub pump_program_ids: Vec<String>,
    normalizer: GeyserEventNormalizer,
    connector: Arc<dyn DeshredStreamConnector>,
}

#[derive(Debug, Clone)]
pub struct MockDeshredLiveSource {
    pub events: Vec<NormalizedEvent>,
}

#[async_trait]
trait CanonicalEventSource: Send {
    async fn run(&mut self, sender: tokio::sync::mpsc::Sender<NormalizedEvent>) -> Result<()>;
}

#[async_trait]
trait EarlyIntentEventSource: Send {
    async fn run(&mut self, sender: tokio::sync::mpsc::Sender<NormalizedEvent>) -> Result<()>;
}

#[async_trait]
impl CanonicalEventSource for MockGeyserLiveSource {
    async fn run(&mut self, sender: tokio::sync::mpsc::Sender<NormalizedEvent>) -> Result<()> {
        for event in self.events.clone() {
            if sender.send(event).await.is_err() {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        Ok(())
    }
}

#[async_trait]
impl CanonicalEventSource for GeyserLiveSource {
    async fn run(&mut self, sender: tokio::sync::mpsc::Sender<NormalizedEvent>) -> Result<()> {
        run_geyser_source_with_connector(
            self.config.clone(),
            self.normalizer.clone(),
            self.connector.clone(),
            sender,
        )
        .await
    }
}

impl GeyserLiveSource {
    fn real(loaded: &LoadedConfig) -> Result<Self> {
        Ok(Self {
            config: loaded.config.geyser.clone(),
            normalizer: GeyserEventNormalizer::from_loaded(loaded)?,
            connector: Arc::new(RealGeyserConnector),
        })
    }
}

#[async_trait]
impl EarlyIntentEventSource for MockDeshredLiveSource {
    async fn run(&mut self, sender: tokio::sync::mpsc::Sender<NormalizedEvent>) -> Result<()> {
        for event in self.events.clone() {
            if sender.send(event).await.is_err() {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        Ok(())
    }
}

#[async_trait]
impl EarlyIntentEventSource for DeshredLiveSource {
    async fn run(&mut self, sender: tokio::sync::mpsc::Sender<NormalizedEvent>) -> Result<()> {
        run_deshred_source_with_connector(
            self.config.clone(),
            self.pump_program_ids.clone(),
            self.normalizer.clone(),
            self.connector.clone(),
            sender,
        )
        .await
    }
}

impl DeshredLiveSource {
    fn real(loaded: &LoadedConfig) -> Result<Self> {
        let config = loaded.config.ingest.deshred.clone().unwrap_or_default();
        Ok(Self {
            config,
            pump_program_ids: loaded.config.pump.program_ids.clone(),
            normalizer: GeyserEventNormalizer::from_loaded(loaded)?,
            connector: Arc::new(RealDeshredConnector),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeResolvedConfig {
    pub loaded: LoadedConfig,
    pub mode: RuntimeMode,
    pub config_hash: String,
    pub idl_hash: String,
}

impl RuntimeResolvedConfig {
    pub fn from_loaded(loaded: LoadedConfig, override_mode: Option<RuntimeMode>) -> Result<Self> {
        let idl_hash = combined_idl_hash(&loaded)?;
        let mode = override_mode.unwrap_or_else(|| RuntimeMode::from(loaded.config.runtime.mode));
        Ok(Self {
            config_hash: loaded.hash.clone(),
            loaded,
            mode,
            idl_hash,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct EventRouter;
#[derive(Debug, Clone, Default)]
pub struct EventPersister;
#[derive(Debug, Clone, Default)]
pub struct StateUpdater;
#[derive(Debug, Clone, Default)]
pub struct FeatureComputer;
#[derive(Debug, Clone, Default)]
pub struct RiskEvaluator;
#[derive(Debug, Clone, Default)]
pub struct DecisionRunner;
#[derive(Debug, Clone, Default)]
pub struct PaperExecutionRunner;
#[derive(Debug, Clone, Default)]
pub struct ReconciliationRunner;
#[derive(Debug, Clone, Default)]
pub struct HealthMonitor;

enum ShredService {
    Fixture(ShredIngestService<FixtureShredDecoder>),
    Production(ShredIngestService<ProductionShredDecoder>),
}

impl ShredService {
    fn process_packet(
        &mut self,
        packet: &ReceivedPacket,
    ) -> Result<Vec<NormalizedEvent>, ingest_shred::ShredIngestError> {
        match self {
            Self::Fixture(service) => service.process_packet(packet),
            Self::Production(service) => service.process_packet(packet),
        }
    }

    fn reconcile_canonical(
        &mut self,
        event: &NormalizedEvent,
    ) -> Option<ingest_shred::ReconciliationResult> {
        match self {
            Self::Fixture(service) => service.reconcile_canonical(event),
            Self::Production(service) => service.reconcile_canonical(event),
        }
    }

    fn expire_tentative(&mut self, now: OffsetDateTime) -> Vec<String> {
        match self {
            Self::Fixture(service) => service.expire_tentative(now),
            Self::Production(service) => service.expire_tentative(now),
        }
    }

    fn metrics(&self) -> ShredMetrics {
        match self {
            Self::Fixture(service) => service.metrics.clone(),
            Self::Production(service) => service.metrics.clone(),
        }
    }
}

pub struct Supervisor {
    resolved: RuntimeResolvedConfig,
    metrics: QuantMetrics,
    storage: StorageEngine,
    rpc_budget: RpcBudgetManager,
    canonical_bus: EventBus<NormalizedEvent>,
    tentative_bus: EventBus<NormalizedEvent>,
    state_engine: StateEngine,
    feature_engine: FeatureEngine,
    risk_engine: RiskEngine,
    decision_engine: DecisionEngine,
    paper_executor: PaperExecutor,
    shred_service: Option<ShredService>,
    tentative_sell_manager: TentativeSellManager,
    health: RuntimeHealth,
    safety: RuntimeSafety,
    decisions_by_type: BTreeMap<String, u64>,
    decisions: Vec<TradeDecisionEvent>,
    decision_outcomes: Vec<DecisionOutcome>,
    latest_features: HashMap<String, FeatureSnapshot>,
    latest_risk: HashMap<String, RiskAssessment>,
    last_feature_snapshot_at_by_mint: HashMap<String, OffsetDateTime>,
    last_edge_slot_recorded: Option<u64>,
    active_data_gaps: Vec<ScopedDataGap>,
    aggregated_snapshot: Option<StateSnapshot>,
    audits: Vec<RuntimeAuditEvent>,
    source_run_id: Option<String>,
    current_scenario_id: Option<String>,
    persist_normalized_events: bool,
    liquidate_on_finalize: bool,
    segment_manager: Option<LiveRunSegmentManager>,
    record_sequence: Cell<u64>,
    calibration_snapshot_hash: Option<String>,
    calibration_snapshot_path: Option<String>,
    started_at: OffsetDateTime,
    replay_profile: RuntimeReplayProfile,
    emitted_feature_snapshot_identities: HashSet<String>,
    replay_seen_mints: HashSet<String>,
}

impl Supervisor {
    pub fn new(resolved: RuntimeResolvedConfig) -> Result<Self> {
        let layout = StorageLayout::from_config(&resolved.loaded.config.storage)?;
        let storage = StorageEngine::new(layout).with_feature_snapshot_persistence(
            resolved.loaded.config.research_worker.persistence.clone(),
        );
        let metrics = QuantMetrics::new()?;
        let rpc_budget = RpcBudgetManager::new(
            resolved.loaded.config.rpc_budget.clone(),
            resolved.loaded.config.execution.clone(),
            resolved.loaded.config.stream_only.clone(),
            resolved.loaded.config.rpc.clone(),
        );
        let fee_model = FeeModel::default();
        let simulator = Simulator::new(fee_model);
        let state_engine = StateEngine::new(resolved.loaded.config.ttl.clone());
        let feature_engine = FeatureEngine::default();
        let risk_engine = RiskEngine::default();
        let decision_engine = DecisionEngine::new(
            resolved.loaded.config.strategy.clone(),
            resolved.loaded.config.edge.clone(),
            resolved.loaded.config.execution.clone(),
            simulator.clone(),
        );
        let paper_executor = PaperExecutor::new(simulator);
        let tentative_sell_manager = TentativeSellManager::new(&resolved.loaded.config);
        let (calibration_snapshot_hash, calibration_snapshot_path) = tentative_sell_manager
            .ensure_calibration_snapshot(&resolved.loaded.config)
            .map(|(hash, path)| (Some(hash), Some(path.display().to_string())))
            .unwrap_or((
                tentative_sell_manager
                    .calibration()
                    .persisted_version_hash
                    .clone(),
                None,
            ));
        let canonical_bus =
            EventBus::bounded(resolved.loaded.config.runtime.queue_capacity_canonical);
        let tentative_bus =
            EventBus::bounded(resolved.loaded.config.runtime.queue_capacity_tentative);
        let shred_service = build_shred_service(&resolved.loaded)?;
        let started_at = unix_now();
        let mut supervisor = Self {
            health: RuntimeHealth {
                early_intent_enabled: resolved.loaded.config.early_intent.enabled,
                early_intent_sources_active: Vec::new(),
                stream_only_enabled: resolved.loaded.config.stream_only.enabled,
                stream_only_passed: false,
                rpc_network_calls_total: 0,
                rpc_credits_used_total: 0,
                rpc_denials_total: 0,
                market_data_rpc_calls_allowed: resolved
                    .loaded
                    .config
                    .stream_only
                    .allow_tracking_rpc,
                holder_rpc_calls_allowed: resolved.loaded.config.stream_only.allow_holder_rpc,
                metadata_fetch_allowed: resolved.loaded.config.stream_only.allow_metadata_rpc,
                confirmation_rpc_allowed: resolved.loaded.config.stream_only.allow_confirmation_rpc,
                blockhash_rpc_allowed: resolved.loaded.config.stream_only.allow_blockhash_rpc,
                geyser_connected: false,
                geyser_endpoint_configured: false,
                geyser_auth_configured: false,
                geyser_last_event_time: None,
                geyser_reconnect_count: 0,
                geyser_gap_count: 0,
                geyser_events_received: 0,
                deshred_supported: false,
                deshred_connected: false,
                deshred_provider_status: "not_requested".to_owned(),
                deshred_endpoint_configured: false,
                deshred_auth_configured: false,
                deshred_events_received: 0,
                deshred_transactions_decoded: 0,
                deshred_decode_errors: 0,
                deshred_reconnect_count: 0,
                deshred_unsupported_count: 0,
                deshred_tentative_sells_total: 0,
                deshred_malicious_warnings_total: 0,
                deshred_emergency_exits_armed_total: 0,
                deshred_emergency_exits_triggered_total: 0,
                deshred_confirmed_executed_total: 0,
                deshred_false_positive_total: 0,
                deshred_saved_loss_quote_total: Decimal::ZERO,
                deshred_opportunity_cost_quote_total: Decimal::ZERO,
                raw_shred_connected: false,
                raw_shred_supported: matches!(
                    resolved.loaded.config.shred.decoder,
                    common::ShredDecoderMode::Production
                ),
                raw_shred_tentative_sells_total: 0,
                mock_early_intent_active: false,
                mock_early_intent_tentative_sells_total: 0,
                fixture_early_intent_tentative_sells_total: 0,
                replay_early_intent_tentative_sells_total: 0,
                source_dedup_count: 0,
                early_intent_dedup_pairs: BTreeMap::new(),
                shred_connected: resolved.loaded.config.shred.enabled && shred_service.is_some(),
                storage_healthy: true,
                rpc_budget_healthy: true,
                event_queue_depth: 0,
                tentative_queue_depth: 0,
                canonical_queue_depth: 0,
                active_tokens: 0,
                active_positions: 0,
                data_gap_active: false,
                data_gap_scope: None,
                kill_switch_active: false,
                paper_pnl: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
                fee_drag: Decimal::ZERO,
                slippage_drag: Decimal::ZERO,
                latency_drag: Decimal::ZERO,
                events_processed: 0,
                canonical_events_dropped: 0,
                skipped_feature_snapshot_count: 0,
                discarded_tokens: 0,
                rugged_tokens: 0,
                last_event_time: None,
                last_decision_time: None,
                last_persist_time: None,
                runtime_uptime_ms: 0,
                mode: resolved.mode,
                live_source_mode: "fixture".to_owned(),
                shred_tentative_sells_total: 0,
                shred_malicious_sell_warnings_total: 0,
                shred_emergency_exits_armed_total: 0,
                shred_emergency_exits_triggered_total: 0,
                shred_confirmed_executed_total: 0,
                shred_confirmed_failed_total: 0,
                shred_not_seen_within_ttl_total: 0,
                shred_reorged_total: 0,
                shred_decode_mismatch_total: 0,
                shred_account_effect_confirmed_total: 0,
                shred_sell_false_positive_total: 0,
                shred_saved_loss_quote_total: Decimal::ZERO,
                shred_opportunity_cost_quote_total: Decimal::ZERO,
            },
            safety: RuntimeSafety {
                trade_allowed: !resolved.loaded.config.live.enabled,
                paper_allowed: resolved.loaded.config.paper.enabled,
                live_allowed: resolved.loaded.config.live.enabled,
                data_quality_allowed: true,
                rpc_budget_allowed: true,
                max_loss_allowed: true,
                stale_data_allowed: true,
                reason_codes: Vec::new(),
            },
            resolved,
            metrics,
            storage,
            rpc_budget,
            canonical_bus,
            tentative_bus,
            state_engine,
            feature_engine,
            risk_engine,
            decision_engine,
            paper_executor,
            shred_service,
            tentative_sell_manager,
            decisions_by_type: BTreeMap::new(),
            decisions: Vec::new(),
            decision_outcomes: Vec::new(),
            latest_features: HashMap::new(),
            latest_risk: HashMap::new(),
            last_feature_snapshot_at_by_mint: HashMap::new(),
            last_edge_slot_recorded: None,
            active_data_gaps: Vec::new(),
            aggregated_snapshot: None,
            audits: Vec::new(),
            source_run_id: None,
            current_scenario_id: None,
            persist_normalized_events: true,
            liquidate_on_finalize: true,
            segment_manager: None,
            record_sequence: Cell::new(0),
            calibration_snapshot_hash,
            calibration_snapshot_path,
            started_at,
            replay_profile: RuntimeReplayProfile::default(),
            emitted_feature_snapshot_identities: HashSet::new(),
            replay_seen_mints: HashSet::new(),
        };
        if supervisor.edge_collector_mode() {
            supervisor.safety.trade_allowed = false;
            supervisor.safety.paper_allowed = false;
            supervisor.safety.live_allowed = false;
            supervisor.persist_normalized_events = false;
            supervisor.liquidate_on_finalize = false;
        }
        supervisor
            .tentative_sell_manager
            .validate_sources(&supervisor.resolved.loaded.config)?;
        supervisor.audit(RuntimeAuditKind::ConfigLoaded, BTreeMap::new(), Vec::new())?;
        supervisor.audit(RuntimeAuditKind::IdlLoaded, BTreeMap::new(), Vec::new())?;
        Ok(supervisor)
    }

    pub fn resolved(&self) -> &RuntimeResolvedConfig {
        &self.resolved
    }

    pub fn health(&self) -> &RuntimeHealth {
        &self.health
    }

    pub fn safety(&self) -> &RuntimeSafety {
        &self.safety
    }

    fn edge_collector_mode(&self) -> bool {
        matches!(self.resolved.mode, RuntimeMode::EdgeCollector)
    }

    fn edge_collector_status_report(&self) -> EdgeCollectorStatusReport {
        EdgeCollectorStatusReport {
            run_id: self.resolved.loaded.config.environment.run_id.clone(),
            mode: self.resolved.mode,
            updated_at: unix_now(),
            stream_only_passed: self.health.stream_only_passed,
            rpc_network_calls_total: self.health.rpc_network_calls_total,
            rpc_credits_used_total: self.health.rpc_credits_used_total,
            events_processed: self.health.events_processed,
            active_tokens: self.health.active_tokens,
            data_gap_active: self.health.data_gap_active,
            data_gap_scope: self.health.data_gap_scope.clone(),
            provider_status: if self.health.deshred_provider_status != "not_requested" {
                self.health.deshred_provider_status.clone()
            } else if self.health.geyser_connected {
                "updates_received".to_owned()
            } else {
                "awaiting_stream".to_owned()
            },
            notes: vec![
                "edge_collector_mode".to_owned(),
                "feature_risk_decision_paper_engines_disabled".to_owned(),
                "research_outputs_deferred_to_research_worker".to_owned(),
            ],
        }
    }

    fn event_type_label(payload: &EventPayload) -> String {
        match payload {
            EventPayload::TokenCreated(_) => "token_created",
            EventPayload::PumpBuy(_) => "pump_buy",
            EventPayload::PumpSell(_) => "pump_sell",
            EventPayload::BondingCurveUpdate(_) => "bonding_curve_update",
            EventPayload::HolderBalanceUpdate(_) => "holder_balance_update",
            EventPayload::WalletFunding(_) => "wallet_funding",
            EventPayload::ObservedTransaction(_) => "observed_transaction",
            EventPayload::DataGap(_) => "data_gap",
            EventPayload::TentativeSellIntentDetected(_) => "tentative_sell_intent",
            EventPayload::TentativeMaliciousSellWarning(_) => "tentative_malicious_sell_warning",
            EventPayload::ShredEmergencyExitArmed(_) => "shred_emergency_exit_armed",
            EventPayload::ShredEmergencyExitTriggered(_) => "shred_emergency_exit_triggered",
            EventPayload::ShredSellIntentResolved(_) => "shred_sell_intent_resolved",
            EventPayload::TokenTerminal(_) => "token_terminal",
            EventPayload::TradeDecision(_) => "trade_decision",
            EventPayload::SimulatedFill(_) => "simulated_fill",
            EventPayload::LiveFill(_) => "live_fill",
        }
        .to_owned()
    }

    fn edge_account_keys(payload: &EventPayload) -> Vec<String> {
        match payload {
            EventPayload::TokenCreated(event) => event
                .raw_account_list
                .iter()
                .map(|value| value.0.clone())
                .collect(),
            EventPayload::HolderBalanceUpdate(event) => {
                vec![event.owner_wallet.0.clone(), event.token_account.0.clone()]
            }
            EventPayload::BondingCurveUpdate(event) => vec![event.mint.0.clone()],
            EventPayload::ObservedTransaction(event) => event.program_ids.clone(),
            _ => Vec::new(),
        }
    }

    fn edge_token_balance_deltas(payload: &EventPayload) -> Vec<String> {
        match payload {
            EventPayload::HolderBalanceUpdate(event) => vec![format!(
                "{}:{}->{} (delta={})",
                event.owner_wallet.0,
                event.old_balance.unwrap_or(Decimal::ZERO),
                event.new_balance,
                event.delta
            )],
            EventPayload::PumpBuy(event) => vec![format!(
                "buyer={} quote_in={} token_out={}",
                event.buyer.0, event.quote_in, event.token_out
            )],
            EventPayload::PumpSell(event) => vec![format!(
                "seller={} token_in={} quote_out={}",
                event.seller.0, event.token_in, event.quote_out
            )],
            _ => Vec::new(),
        }
    }

    fn edge_transaction_status(payload: &EventPayload) -> Option<String> {
        match payload {
            EventPayload::PumpBuy(event) => Some(format!("{:?}", event.status).to_lowercase()),
            EventPayload::PumpSell(event) => Some(format!("{:?}", event.status).to_lowercase()),
            _ => None,
        }
    }

    fn edge_compute_budget(payload: &EventPayload) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        match payload {
            EventPayload::PumpBuy(event) => {
                if let Some(value) = event.compute_unit_limit {
                    map.insert("compute_unit_limit".to_owned(), value.to_string());
                }
                if let Some(value) = event.compute_unit_price {
                    map.insert("compute_unit_price".to_owned(), value.to_string());
                }
                if let Some(value) = event.estimated_priority_fee_lamports {
                    map.insert(
                        "estimated_priority_fee_lamports".to_owned(),
                        value.0.to_string(),
                    );
                }
                if let Some(value) = event.estimated_base_fee_lamports {
                    map.insert(
                        "estimated_base_fee_lamports".to_owned(),
                        value.0.to_string(),
                    );
                }
                if let Some(value) = event.estimated_tip_lamports {
                    map.insert("estimated_tip_lamports".to_owned(), value.0.to_string());
                }
            }
            EventPayload::PumpSell(event) => {
                if let Some(value) = event.compute_unit_limit {
                    map.insert("compute_unit_limit".to_owned(), value.to_string());
                }
                if let Some(value) = event.compute_unit_price {
                    map.insert("compute_unit_price".to_owned(), value.to_string());
                }
                if let Some(value) = event.estimated_priority_fee_lamports {
                    map.insert(
                        "estimated_priority_fee_lamports".to_owned(),
                        value.0.to_string(),
                    );
                }
                if let Some(value) = event.estimated_base_fee_lamports {
                    map.insert(
                        "estimated_base_fee_lamports".to_owned(),
                        value.0.to_string(),
                    );
                }
                if let Some(value) = event.estimated_tip_lamports {
                    map.insert("estimated_tip_lamports".to_owned(), value.0.to_string());
                }
            }
            EventPayload::ObservedTransaction(event) => {
                if let Some(value) = event.compute_unit_limit {
                    map.insert("compute_unit_limit".to_owned(), value.to_string());
                }
                if let Some(value) = event.compute_unit_price {
                    map.insert("compute_unit_price".to_owned(), value.to_string());
                }
                if let Some(value) = event.estimated_priority_fee_lamports {
                    map.insert(
                        "estimated_priority_fee_lamports".to_owned(),
                        value.0.to_string(),
                    );
                }
                if let Some(value) = event.tx_fee_lamports {
                    map.insert("tx_fee_lamports".to_owned(), value.0.to_string());
                }
                if let Some(value) = event.compute_units_consumed {
                    map.insert("compute_units_consumed".to_owned(), value.to_string());
                }
            }
            _ => {}
        }
        map
    }

    fn edge_event_record(&self, event: &NormalizedEvent) -> EdgeEventRecord {
        EdgeEventRecord {
            run_id: self.resolved.loaded.config.environment.run_id.clone(),
            sequence_number: self.record_sequence.get().saturating_add(1),
            source: event.meta.source,
            slot: event.meta.slot,
            event_time: event.meta.received_at_wall_time,
            signature: event.signature().map(ToOwned::to_owned),
            mint: event.mint().map(|mint| mint.0.clone()),
            event_type: Self::event_type_label(&event.payload),
            program_id: match &event.payload {
                EventPayload::ObservedTransaction(tx) => tx.program_ids.first().cloned(),
                _ => None,
            },
            instruction_discriminator: Some(Self::event_type_label(&event.payload)),
            decoded_fields: serde_json::to_value(&event.payload).unwrap_or_else(|_| json!({})),
            account_keys: Self::edge_account_keys(&event.payload),
            token_balance_deltas: Self::edge_token_balance_deltas(&event.payload),
            transaction_status: Self::edge_transaction_status(&event.payload),
            compute_budget: Self::edge_compute_budget(&event.payload),
            raw_update_hash: event
                .meta
                .raw_reference
                .as_ref()
                .map(|reference| reference.source_id.clone()),
            canonicality: event.meta.canonicality,
            data_gap_flags: event
                .meta
                .data_quality_flags
                .iter()
                .map(|flag| format!("{flag:?}").to_lowercase())
                .collect(),
            stream_only_passed: self.health.stream_only_passed,
        }
    }

    fn persist_edge_stream_records(&mut self, event: &NormalizedEvent) -> Result<()> {
        let record = self.tag_record(StoredRecord::new(
            storage::DatasetKind::NormalizedEventLog,
            self.resolved.config_hash.clone(),
            self.resolved.idl_hash.clone(),
            Some(
                self.resolved
                    .loaded
                    .config
                    .environment
                    .strategy_version
                    .clone(),
            ),
            format!("{:?}", event.meta.source).to_lowercase(),
            format!("{:?}", event.meta.canonicality).to_lowercase(),
            Some(event.meta.received_at_wall_time),
            event.clone(),
        ));
        let edge_record = self.edge_event_record(event);
        let slot_record = EdgeSlotRecord {
            run_id: self.resolved.loaded.config.environment.run_id.clone(),
            source: event.meta.source,
            slot: event.meta.slot,
            parent_slot: event.meta.parent_slot,
            event_time: event.meta.received_at_wall_time,
            canonicality: event.meta.canonicality,
            data_gap_active: self.health.data_gap_active,
            stream_only_passed: self.health.stream_only_passed,
        };
        let emit_slot = self.last_edge_slot_recorded != Some(event.meta.slot);
        let Some(segment_manager) = self.segment_manager.as_mut() else {
            return Ok(());
        };
        segment_manager.append_json_record(
            "normalized_events",
            &record,
            Some(event.meta.received_at_wall_time),
        )?;
        segment_manager.append_json_record(
            "source_events",
            &record,
            Some(event.meta.received_at_wall_time),
        )?;
        match &event.payload {
            EventPayload::ObservedTransaction(_) => {
                segment_manager.append_json_record(
                    "edge_transactions",
                    &edge_record,
                    Some(event.meta.received_at_wall_time),
                )?;
            }
            EventPayload::HolderBalanceUpdate(_) | EventPayload::BondingCurveUpdate(_) => {
                segment_manager.append_json_record(
                    "edge_accounts",
                    &edge_record,
                    Some(event.meta.received_at_wall_time),
                )?;
            }
            EventPayload::DataGap(_) => {
                segment_manager.append_json_record(
                    "edge_data_gaps",
                    &edge_record,
                    Some(event.meta.received_at_wall_time),
                )?;
            }
            _ => {
                segment_manager.append_json_record(
                    "edge_events",
                    &edge_record,
                    Some(event.meta.received_at_wall_time),
                )?;
            }
        }
        if emit_slot {
            segment_manager.append_json_record(
                "edge_slots",
                &slot_record,
                Some(event.meta.received_at_wall_time),
            )?;
            self.last_edge_slot_recorded = Some(event.meta.slot);
        }
        self.health.last_persist_time = Some(unix_now());
        Ok(())
    }

    fn current_snapshot(&self) -> StateSnapshot {
        self.aggregated_snapshot
            .clone()
            .unwrap_or_else(|| self.state_engine.snapshot())
    }

    fn tag_record<T>(&self, record: StoredRecord<T>) -> StoredRecord<T> {
        let sequence = self.record_sequence.get().saturating_add(1);
        self.record_sequence.set(sequence);
        let record = record
            .with_run_id(self.resolved.loaded.config.environment.run_id.clone())
            .with_runtime_sequence_number(sequence)
            .with_calibration_snapshot_hash(self.calibration_snapshot_hash.clone());
        if let Some(scenario_id) = &self.current_scenario_id {
            record.with_scenario_id(scenario_id.clone())
        } else {
            record
        }
    }

    fn profile_add_time(&mut self, stage: &str, started: Instant) {
        let elapsed = started.elapsed().as_millis();
        *self
            .replay_profile
            .substage_timings_ms
            .entry(stage.to_owned())
            .or_default() += elapsed;
    }

    fn profile_inc(&mut self, counter: &str, amount: u64) {
        *self
            .replay_profile
            .counters
            .entry(counter.to_owned())
            .or_default() += amount;
    }

    fn profile_set_max(&mut self, counter: &str, value: u64) {
        let entry = self
            .replay_profile
            .counters
            .entry(counter.to_owned())
            .or_default();
        *entry = (*entry).max(value);
    }

    fn feature_snapshot_identity(&self, feature_record: &StoredRecord<FeatureSnapshot>) -> String {
        format!(
            "{}|{}|{}|{}|{}",
            self.source_run_id
                .as_deref()
                .unwrap_or(self.resolved.loaded.config.environment.run_id.as_str()),
            feature_record.run_id.as_deref().unwrap_or_default(),
            feature_record.record.mint.0,
            feature_record.record.observed_at.unix_timestamp_nanos(),
            feature_record.record.vector_hash
        )
    }

    fn deterministic_decision_id(
        &self,
        event: &NormalizedEvent,
        decision: &TradeDecisionEvent,
    ) -> String {
        let replay_identity = self
            .source_run_id
            .as_deref()
            .unwrap_or(self.resolved.loaded.config.environment.run_id.as_str());
        let seed = format!(
            "decision:{}:{}:{}:{:?}:{}:{}:{}",
            replay_identity,
            self.current_scenario_id.as_deref().unwrap_or("_all"),
            event.meta.event_id.0,
            decision.decision,
            decision.strategy,
            decision.feature_snapshot_hash,
            decision.no_lookahead_timestamp.unix_timestamp_nanos(),
        );
        common::EventId::from_seed(&seed).0.to_string()
    }

    pub async fn run_fixture(&mut self, scenario: FixtureScenario) -> Result<RuntimeSummary> {
        self.current_scenario_id = Some(scenario.spec.name.clone());
        self.source_run_id = None;
        self.persist_normalized_events = true;
        self.liquidate_on_finalize = true;
        self.aggregated_snapshot = None;
        self.health.live_source_mode = "fixture".to_owned();
        self.audit(
            RuntimeAuditKind::RuntimeStarted,
            BTreeMap::new(),
            Vec::new(),
        )?;
        self.persist_run_metadata(RunStatus::Running, vec!["fixture_mode".to_owned()])?;
        self.audit(
            RuntimeAuditKind::IngestStarted,
            BTreeMap::from([("scenario".to_owned(), scenario.spec.name.clone())]),
            Vec::new(),
        )?;

        if !scenario.timeline_events.is_empty() {
            for event in &scenario.timeline_events {
                match event.meta.canonicality {
                    Canonicality::Tentative => self.publish_tentative(event.clone())?,
                    _ => self.publish_canonical(event.clone())?,
                }
                self.drain_pipeline().await?;
            }
        } else if self.resolved.loaded.config.shred.enabled && !scenario.shred_batches.is_empty() {
            self.ingest_fixture_shreds(&scenario.shred_batches).await?;
            self.drain_pipeline().await?;
        } else if self.resolved.loaded.config.shred.required && self.shred_service.is_none() {
            return Err(RuntimeError::ProductionShredDecoderDisabled.into());
        }

        if scenario.timeline_events.is_empty() {
            for event in &scenario.canonical_events {
                self.publish_canonical(event.clone())?;
            }
            self.drain_pipeline().await?;
        }
        self.finalize_run().await?;
        self.persist_run_metadata(RunStatus::Completed, vec!["fixture_mode".to_owned()])?;
        self.persist_default_run_report()?;
        self.audit(RuntimeAuditKind::IngestStopped, BTreeMap::new(), Vec::new())?;
        self.audit(
            RuntimeAuditKind::RuntimeStopped,
            BTreeMap::new(),
            Vec::new(),
        )?;
        Ok(self.summary())
    }

    pub async fn run_fixture_suite(
        &mut self,
        specs: &[FixtureScenarioSpec],
    ) -> Result<Vec<FixtureRunResult>> {
        let mut results = Vec::new();
        for spec in specs {
            self.reset_for_next_run();
            let summary = self.run_fixture(build_fixture_scenario(spec)).await?;
            results.push(evaluate_fixture_result(spec, &summary));
        }
        Ok(results)
    }

    pub async fn run_from_store(&mut self) -> Result<RuntimeSummary> {
        let latest_run = self.storage.latest_normalized_event_run_id()?;
        self.run_from_store_selection(latest_run.as_deref(), None)
            .await
    }

    pub async fn run_from_store_for_run(&mut self, run_id: Option<&str>) -> Result<RuntimeSummary> {
        self.run_from_store_selection(run_id, None).await
    }

    pub async fn run_from_store_selection(
        &mut self,
        source_run_id: Option<&str>,
        scenario_id: Option<&str>,
    ) -> Result<RuntimeSummary> {
        let source_run_id = if let Some(source_run_id) = source_run_id {
            source_run_id.to_owned()
        } else {
            let Some(latest_run) = self.storage.latest_normalized_event_run_id()? else {
                return Ok(self.summary());
            };
            latest_run
        };

        let replay_load_started = Instant::now();
        let records = self
            .storage
            .deterministic_replay_for_run_and_scenario(Some(&source_run_id), scenario_id)?;
        self.profile_add_time("event_deserialization", replay_load_started);
        self.write_replay_progress("loaded_records", &source_run_id, Some(records.len()), 0, 0)?;
        let ordering_started = Instant::now();
        let grouped = group_records_by_scenario(records);
        self.profile_add_time("event_ordering", ordering_started);
        self.source_run_id = Some(source_run_id.clone());
        self.persist_normalized_events = false;
        self.liquidate_on_finalize = true;
        self.reset_aggregate();
        self.health.live_source_mode = "store_replay".to_owned();
        self.audit(
            RuntimeAuditKind::RuntimeStarted,
            BTreeMap::from([("source_run_id".to_owned(), source_run_id.clone())]),
            Vec::new(),
        )?;
        self.persist_run_metadata(
            RunStatus::Running,
            vec![format!("source_run_id={source_run_id}")],
        )?;

        for (group_scenario_id, group_records) in grouped {
            let group_len = group_records.len();
            let mut child = Supervisor::new(self.resolved.clone())?;
            child.persist_normalized_events = false;
            child.liquidate_on_finalize = true;
            child.source_run_id = Some(source_run_id.clone());
            child.current_scenario_id = group_scenario_id.clone();
            child.profile_inc("events_processed", group_len as u64);
            child.write_replay_progress(
                "scenario_started",
                &source_run_id,
                Some(group_len),
                0,
                0,
            )?;
            child.audit(
                RuntimeAuditKind::RuntimeStarted,
                BTreeMap::from([
                    ("source_run_id".to_owned(), source_run_id.clone()),
                    (
                        "scenario_id".to_owned(),
                        group_scenario_id
                            .clone()
                            .unwrap_or_else(|| "_all".to_owned()),
                    ),
                ]),
                Vec::new(),
            )?;
            for (index, record) in group_records.into_iter().enumerate() {
                match record.record.meta.canonicality {
                    Canonicality::Tentative => child.publish_tentative(record.record)?,
                    _ => child.publish_canonical(record.record)?,
                }
                child.drain_pipeline().await?;
                let processed = index + 1;
                if processed == group_len || processed % 1_000 == 0 {
                    child.write_replay_progress(
                        "scenario_replaying",
                        &source_run_id,
                        Some(group_len),
                        processed,
                        child.health.events_processed,
                    )?;
                }
            }
            child.write_replay_progress(
                "scenario_finalizing",
                &source_run_id,
                Some(group_len),
                group_len,
                child.health.events_processed,
            )?;
            child.finalize_run().await?;
            child.audit(
                RuntimeAuditKind::RuntimeStopped,
                BTreeMap::new(),
                Vec::new(),
            )?;
            self.merge_summary(child.summary());
            self.write_replay_progress(
                "scenario_completed",
                &source_run_id,
                Some(group_len),
                group_len,
                self.health.events_processed,
            )?;
        }

        self.audit(
            RuntimeAuditKind::RuntimeStopped,
            BTreeMap::new(),
            Vec::new(),
        )?;
        self.persist_run_metadata(
            RunStatus::Completed,
            vec![format!("source_run_id={source_run_id}")],
        )?;
        self.persist_default_run_report()?;
        let active_tokens = self.current_snapshot().tokens.len() as u64;
        self.profile_set_max("active_tokens", active_tokens);
        self.refresh_health();
        Ok(self.summary())
    }

    fn replay_progress_path(&self, source_run_id: &str) -> PathBuf {
        self.storage
            .run_report_dir(&self.resolved.loaded.config.environment.run_id)
            .join(format!("replay_progress_{source_run_id}.json"))
    }

    fn write_replay_progress(
        &self,
        phase: &str,
        source_run_id: &str,
        records_total: Option<usize>,
        records_processed: usize,
        events_processed: u64,
    ) -> Result<()> {
        let path = self.replay_progress_path(source_run_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let payload = json!({
            "phase": phase,
            "source_run_id": source_run_id,
            "derived_run_id": self.resolved.loaded.config.environment.run_id,
            "scenario_id": self.current_scenario_id,
            "records_total": records_total,
            "records_processed": records_processed,
            "events_processed": events_processed,
            "written_at": unix_now(),
        });
        fs::write(path, serde_json::to_vec_pretty(&payload)?)?;
        Ok(())
    }

    pub fn live_data_readiness(&mut self) -> Result<RuntimeSummary> {
        let shred_cfg = &self.resolved.loaded.config.shred;
        let geyser_cfg = &self.resolved.loaded.config.geyser;
        let deshred_cfg = self
            .resolved
            .loaded
            .config
            .ingest
            .deshred
            .clone()
            .unwrap_or_default();
        let deshred_capability = inspect_deshred_capability(&self.resolved.loaded, None, None);
        self.health.geyser_connected = geyser_cfg.enabled;
        self.health.geyser_endpoint_configured = resolved_geyser_endpoint(geyser_cfg).is_ok();
        self.health.geyser_auth_configured = resolved_geyser_metadata(geyser_cfg)
            .ok()
            .flatten()
            .is_some()
            || !geyser_cfg.auth_required;
        self.health.deshred_supported = deshred_capability.can_enable;
        self.health.deshred_endpoint_configured = deshred_capability.endpoint_configured;
        self.health.deshred_auth_configured = deshred_capability.auth_configured;
        self.health.deshred_connected = false;
        self.health.shred_connected = shred_cfg.enabled && self.shred_service.is_some();

        if geyser_cfg.required && !geyser_cfg.enabled {
            return Err(RuntimeError::Blocked("geyser is required but disabled".to_owned()).into());
        }
        if shred_cfg.required && (!shred_cfg.enabled || self.shred_service.is_none()) {
            return Err(
                RuntimeError::Blocked("shred is required but unavailable".to_owned()).into(),
            );
        }
        if deshred_cfg.enabled && deshred_cfg.required && !deshred_capability.can_enable {
            return Err(RuntimeError::Blocked(
                deshred_capability
                    .reason_if_unsupported
                    .unwrap_or_else(|| "deshred is required but unsupported".to_owned()),
            )
            .into());
        }
        if deshred_cfg.enabled && deshred_cfg.required && !self.health.deshred_endpoint_configured {
            return Err(RuntimeError::Blocked(
                "deshred is required but endpoint is unavailable".to_owned(),
            )
            .into());
        }
        if deshred_cfg.enabled && deshred_cfg.required && !self.health.deshred_auth_configured {
            return Err(RuntimeError::Blocked(
                "deshred is required but auth metadata is unavailable".to_owned(),
            )
            .into());
        }
        if shred_cfg.enabled
            && self.shred_service.is_none()
            && !shred_cfg.allow_geyser_only_fallback
        {
            return Err(RuntimeError::ProductionShredDecoderDisabled.into());
        }
        if shred_cfg.enabled && self.shred_service.is_none() {
            self.safety.reason_codes.push(ReasonCode::ShredUnavailable);
            self.health.shred_connected = false;
        }
        if deshred_cfg.enabled && !deshred_capability.can_enable {
            self.safety
                .reason_codes
                .push(ReasonCode::EarlyIntentSourceUnavailable);
            self.health.deshred_unsupported_count =
                self.health.deshred_unsupported_count.saturating_add(1);
        }
        let _geyser_plan = GeyserIngestService::new(geyser_cfg.clone()).subscription_request();
        Ok(self.summary())
    }

    pub async fn run_live_data_paper(&mut self, options: LiveRunOptions) -> Result<RuntimeSummary> {
        if let Some(run_id) = &options.run_id {
            self.resolved.loaded.config.environment.run_id = run_id.clone();
        }
        self.segment_manager = LiveRunSegmentManager::from_loaded(
            &self.resolved.loaded,
            &self.resolved.loaded.config.environment.run_id,
        )?;
        self.audit(
            RuntimeAuditKind::RuntimeStarted,
            BTreeMap::new(),
            Vec::new(),
        )?;
        self.source_run_id = None;
        self.current_scenario_id = None;
        self.persist_normalized_events = true;
        self.liquidate_on_finalize = options.mock_live;
        self.aggregated_snapshot = None;
        self.health.live_source_mode = if options.mock_live {
            if options.mock_deshred && options.mock_early_intent {
                "mock_geyser+mock_deshred+mock_early_intent".to_owned()
            } else if options.mock_deshred {
                "mock_geyser+mock_deshred".to_owned()
            } else if options.mock_early_intent {
                "mock_geyser+mock_early_intent".to_owned()
            } else {
                "mock_geyser".to_owned()
            }
        } else if options.with_deshred {
            "real_geyser+deshred".to_owned()
        } else {
            "real_geyser".to_owned()
        };
        let live_mode_notes = vec![
            format!("mode={}", self.health.live_source_mode),
            format!("mock_early_intent={}", options.mock_early_intent),
            format!("mock_deshred={}", options.mock_deshred),
            format!("with_deshred={}", options.with_deshred),
        ];
        self.persist_run_metadata(RunStatus::Running, live_mode_notes.clone())?;
        let readiness = match self.live_data_readiness() {
            Ok(readiness) => readiness,
            Err(error) => {
                let _ = self.persist_run_metadata(RunStatus::Failed, {
                    let mut notes = live_mode_notes.clone();
                    notes.push(format!("startup_error={error}"));
                    notes
                });
                let _ = self.audit(
                    RuntimeAuditKind::RuntimeStopped,
                    BTreeMap::new(),
                    Vec::new(),
                );
                return Err(error);
            }
        };
        if !readiness.safety.paper_allowed && !self.edge_collector_mode() {
            let error = RuntimeError::Blocked("paper mode is disabled by configuration".to_owned());
            let _ = self.persist_run_metadata(RunStatus::Failed, {
                let mut notes = live_mode_notes.clone();
                notes.push("startup_error=paper mode disabled".to_owned());
                notes
            });
            let _ = self.audit(
                RuntimeAuditKind::RuntimeStopped,
                BTreeMap::new(),
                Vec::new(),
            );
            return Err(error.into());
        }
        if options.geyser_only || options.no_shred {
            self.health.shred_connected = false;
            self.safety
                .reason_codes
                .retain(|reason| *reason != ReasonCode::ShredUnavailable);
        }
        let metrics_state = Arc::new(Mutex::new(RuntimeHttpSnapshot {
            health: self.health.clone(),
            safety: self.safety.clone(),
            updated_at: unix_now(),
        }));
        let mut metrics_handle = if self.resolved.loaded.config.metrics.enabled {
            Some(spawn_metrics_server(
                self.metrics.clone(),
                &self.resolved.loaded.config.metrics.bind_addr,
                metrics_state.clone(),
            )?)
        } else {
            None
        };
        self.sync_http_snapshot(&metrics_state);

        let (canonical_tx, mut canonical_rx) = tokio::sync::mpsc::channel::<NormalizedEvent>(
            self.resolved
                .loaded
                .config
                .runtime
                .queue_capacity_canonical
                .max(1),
        );
        let (tentative_tx, mut tentative_rx) = tokio::sync::mpsc::channel::<NormalizedEvent>(
            self.resolved
                .loaded
                .config
                .runtime
                .queue_capacity_tentative
                .max(1),
        );

        let mut source_tasks = Vec::new();
        let mut inline_mock_events = VecDeque::<(NormalizedEvent, bool)>::new();
        let mock_tentative_allowed = self
            .resolved
            .loaded
            .config
            .early_intent
            .mock
            .allow_in_live_data_paper;
        let mut tentative_source_requested = false;
        if options.mock_live {
            self.health.mock_early_intent_active = options.mock_early_intent;
            let scenario_kind = if options.mock_deshred {
                mock_deshred_fixture_kind(options.mock_deshred_scenario.as_deref())
            } else if options.mock_early_intent {
                mock_early_intent_fixture_kind(options.mock_early_intent_scenario.as_deref())
            } else {
                FixtureScenarioKind::StrongHolderGrowthWinner
            };
            let scenario = build_fixture_scenario(&fixtures::spec(scenario_kind));
            if (options.mock_early_intent || options.mock_deshred)
                && mock_tentative_allowed
                && !scenario.timeline_events.is_empty()
            {
                inline_mock_events = scenario
                    .timeline_events
                    .iter()
                    .cloned()
                    .flat_map(|event| {
                        if event.meta.canonicality != Canonicality::Tentative {
                            return vec![(event, false)];
                        }
                        let mut variants = Vec::new();
                        if options.mock_deshred {
                            variants.extend(
                                apply_mock_source_variant(
                                    vec![event.clone()],
                                    options.mock_deshred_scenario.as_deref(),
                                    true,
                                )
                                .into_iter()
                                .map(|variant| (variant, true)),
                            );
                        }
                        if options.mock_early_intent {
                            variants.extend(
                                apply_mock_source_variant(
                                    vec![event],
                                    options
                                        .mock_early_intent_scenario
                                        .as_deref()
                                        .or(options.mock_deshred_scenario.as_deref()),
                                    false,
                                )
                                .into_iter()
                                .map(|variant| (variant, true)),
                            );
                        }
                        if variants.is_empty() {
                            Vec::new()
                        } else {
                            variants
                        }
                    })
                    .collect();
                tentative_source_requested =
                    inline_mock_events.iter().any(|(_, tentative)| *tentative);
            } else {
                let canonical_events = if !scenario.timeline_events.is_empty() {
                    scenario
                        .timeline_events
                        .iter()
                        .filter(|event| event.meta.canonicality != Canonicality::Tentative)
                        .cloned()
                        .collect::<Vec<_>>()
                } else {
                    scenario.canonical_events.clone()
                };
                let tentative_events = if !scenario.timeline_events.is_empty() {
                    scenario
                        .timeline_events
                        .iter()
                        .filter(|event| event.meta.canonicality == Canonicality::Tentative)
                        .cloned()
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                let early_intent_events = apply_mock_source_variant(
                    tentative_events.clone(),
                    options
                        .mock_early_intent_scenario
                        .as_deref()
                        .or(options.mock_deshred_scenario.as_deref()),
                    false,
                );
                let deshred_events = apply_mock_source_variant(
                    tentative_events,
                    options.mock_deshred_scenario.as_deref(),
                    true,
                );
                source_tasks.push(spawn_mock_geyser_live_source(
                    MockGeyserLiveSource {
                        events: canonical_events,
                    },
                    canonical_tx.clone(),
                ));
                if options.mock_early_intent && options.mock_deshred && mock_tentative_allowed {
                    let mut combined_tentative_events = deshred_events.clone();
                    combined_tentative_events.extend(early_intent_events);
                    source_tasks.push(spawn_mock_geyser_live_source(
                        MockGeyserLiveSource {
                            events: combined_tentative_events,
                        },
                        tentative_tx.clone(),
                    ));
                    tentative_source_requested = true;
                } else if options.mock_early_intent && mock_tentative_allowed {
                    source_tasks.push(spawn_mock_geyser_live_source(
                        MockGeyserLiveSource {
                            events: early_intent_events,
                        },
                        tentative_tx.clone(),
                    ));
                    tentative_source_requested = true;
                }
                if options.mock_deshred && mock_tentative_allowed {
                    source_tasks.push(spawn_mock_deshred_live_source(
                        MockDeshredLiveSource {
                            events: deshred_events,
                        },
                        tentative_tx.clone(),
                    ));
                    tentative_source_requested = true;
                }
            }
            if !options.geyser_only
                && !options.no_shred
                && self.resolved.loaded.config.shred.enabled
            {
                source_tasks.push(spawn_mock_shred_live_source(
                    MockShredLiveSource {
                        batches: scenario.shred_batches.clone(),
                    },
                    tentative_tx.clone(),
                    self.resolved.loaded.config.shred.tentative_ttl_ms,
                ));
                tentative_source_requested = true;
            }
        } else {
            let startup = (|| -> Result<()> {
                let _ = resolved_geyser_metadata(&self.resolved.loaded.config.geyser)?;
                let _ = resolved_geyser_endpoint(&self.resolved.loaded.config.geyser)?;
                source_tasks.push(spawn_real_geyser_live_source(
                    GeyserLiveSource::real(&self.resolved.loaded)?,
                    canonical_tx.clone(),
                ));
                if deshred_requested(&self.resolved.loaded.config, &options) {
                    let capability = inspect_deshred_capability(&self.resolved.loaded, None, None);
                    if !capability.can_enable {
                        if options.require_deshred
                            || self
                                .resolved
                                .loaded
                                .config
                                .ingest
                                .deshred
                                .clone()
                                .unwrap_or_default()
                                .required
                        {
                            return Err(anyhow!(
                                "{}",
                                capability.reason_if_unsupported.unwrap_or_else(|| {
                                    "deshred is unsupported by this build".to_owned()
                                })
                            ));
                        }
                        self.health.deshred_unsupported_count =
                            self.health.deshred_unsupported_count.saturating_add(1);
                    } else if self.health.deshred_endpoint_configured {
                        source_tasks.push(spawn_real_deshred_live_source(
                            DeshredLiveSource::real(&self.resolved.loaded)?,
                            tentative_tx.clone(),
                        ));
                        tentative_source_requested = true;
                    } else if options.require_deshred {
                        return Err(anyhow!("deshred endpoint is required but not configured"));
                    }
                }
                Ok(())
            })();
            if let Err(error) = startup {
                let _ = self.persist_run_metadata(RunStatus::Failed, {
                    let mut notes = live_mode_notes.clone();
                    notes.push(format!("startup_error={error}"));
                    notes
                });
                let _ = self.audit(
                    RuntimeAuditKind::RuntimeStopped,
                    BTreeMap::new(),
                    Vec::new(),
                );
                return Err(error);
            }
        }
        drop(canonical_tx);
        drop(tentative_tx);

        let mut processed = 0usize;
        let mut abort_live_sources = false;
        let mut canonical_closed = false;
        let mut tentative_closed = !tentative_source_requested;
        let mut completion_notes = Vec::new();
        let mut storage_warning_reported = false;
        let mut health_tick = tokio::time::interval(StdDuration::from_millis(
            options
                .health_interval_ms
                .unwrap_or(
                    self.resolved
                        .loaded
                        .config
                        .runtime
                        .health_report_interval_ms,
                )
                .max(50),
        ));
        let mut disk_tick = tokio::time::interval(StdDuration::from_secs(
            self.resolved
                .loaded
                .config
                .autopilot
                .disk
                .check_interval_seconds
                .max(1),
        ));
        let deadline = options
            .duration
            .map(|duration| tokio::time::Instant::now() + duration.unsigned_abs());

        loop {
            if let Some(max_events) = options.max_events {
                if processed >= max_events {
                    abort_live_sources = true;
                    break;
                }
            }
            if let Some(max_decisions) = options.max_decisions {
                if self.decisions.len() >= max_decisions {
                    abort_live_sources = true;
                    break;
                }
            }
            if canonical_closed && tentative_closed {
                break;
            }
            if let Some((event, is_tentative)) = inline_mock_events.pop_front() {
                if is_tentative {
                    self.publish_tentative(event)?;
                } else {
                    self.publish_canonical(event)?;
                }
                self.drain_pipeline().await?;
                self.sync_http_snapshot(&metrics_state);
                processed += 1;
                continue;
            }

            tokio::select! {
                _ = health_tick.tick() => {
                    self.refresh_health();
                    self.sync_http_snapshot(&metrics_state);
                }
                _ = disk_tick.tick() => {
                    self.refresh_health();
                    let low_disk_mode = &self.resolved.loaded.config.storage.low_disk_mode;
                    let warning_free_mb = if low_disk_mode.enabled {
                        low_disk_mode.warning_free_mb
                    } else {
                        self.resolved.loaded.config.autopilot.disk.warning_free_mb
                    };
                    let critical_free_mb = if low_disk_mode.enabled {
                        low_disk_mode.critical_free_mb
                    } else {
                        self.resolved.loaded.config.autopilot.disk.critical_free_mb
                    };
                    let emergency_free_mb = if low_disk_mode.enabled {
                        low_disk_mode.emergency_free_mb
                    } else {
                        self.resolved.loaded.config.autopilot.disk.emergency_free_mb
                    };
                    let close_segments_on_warning =
                        self.resolved.loaded.config.autopilot.disk.close_segments_on_warning;
                    let stop_collection_on_critical =
                        self.resolved.loaded.config.autopilot.disk.stop_collection_on_critical;
                    let pause_before_os_error =
                        self.resolved.loaded.config.autopilot.disk.pause_before_os_error;
                    let mut free_mb = filesystem_free_mb(&self.storage.layout().root).unwrap_or(u64::MAX);
                    if let Some(segment_manager) = self.segment_manager.as_mut() {
                        let now = OffsetDateTime::now_utc();
                        let _ = segment_manager.close_segments_due_to_age(now)?;
                        let background = segment_manager.process_background_uploads().await?;
                        if background.uploaded_count > 0 || background.pruned_bytes > 0 {
                            let _ = segment_manager.record_disk_action(
                                "info",
                                "background_segment_upload",
                                free_mb,
                                background.pruned_bytes,
                                vec![
                                    format!("uploaded_segments={}", background.uploaded_count),
                                    format!("verified_segments={}", background.verified_count),
                                    format!("pending_segments={}", background.pending_count),
                                ],
                            );
                            free_mb = filesystem_free_mb(&self.storage.layout().root).unwrap_or(u64::MAX);
                        }
                        let footprint = segment_manager.enforce_local_footprint_budget(free_mb)?;
                        if footprint.local_runtime_budget_overage_bytes > 0 {
                            let _ = segment_manager.record_disk_action(
                                "warning",
                                "local_footprint_budget_cleanup",
                                free_mb,
                                footprint
                                    .verified_segment_bytes_pruned_total
                                    .saturating_add(footprint.verified_export_bytes_pruned_total),
                                vec![
                                    format!(
                                        "local_runtime_bytes={}",
                                        footprint.total_local_runtime_bytes
                                    ),
                                    format!(
                                        "local_runtime_budget_bytes={}",
                                        footprint.local_runtime_budget_bytes
                                    ),
                                    format!(
                                        "local_runtime_budget_overage_bytes={}",
                                        footprint.local_runtime_budget_overage_bytes
                                    ),
                                ],
                            );
                            free_mb = filesystem_free_mb(&self.storage.layout().root).unwrap_or(u64::MAX);
                        }
                    }
                    self.health.storage_healthy = free_mb >= critical_free_mb;
                    if free_mb < warning_free_mb && close_segments_on_warning {
                        let mut warning_notes = Vec::new();
                        if let Some(segment_manager) = self.segment_manager.as_mut() {
                            let _ = segment_manager.set_disk_warning_active(true);
                            let closed = segment_manager.close_all_open_segments()?;
                            let cleanup = segment_manager.process_background_uploads().await?;
                            warning_notes.push(format!("closed_segments={closed}"));
                            warning_notes.push(format!("uploaded_segments={}", cleanup.uploaded_count));
                            warning_notes.push(format!("verified_segments={}", cleanup.verified_count));
                            warning_notes.push(format!("pruned_segment_bytes={}", cleanup.pruned_bytes));
                            let _ = segment_manager.record_disk_action(
                                "warning",
                                "close_upload_prune_on_warning",
                                free_mb,
                                cleanup.pruned_bytes,
                                warning_notes.clone(),
                            );
                            free_mb = filesystem_free_mb(&self.storage.layout().root).unwrap_or(u64::MAX);
                        }
                        if !storage_warning_reported {
                            let mut details = BTreeMap::new();
                            details.insert("free_mb".to_owned(), free_mb.to_string());
                            details.insert("warning_free_mb".to_owned(), warning_free_mb.to_string());
                            details.insert("critical_free_mb".to_owned(), critical_free_mb.to_string());
                            let _ = self.audit(
                                RuntimeAuditKind::StorageWarning,
                                details,
                                Vec::new(),
                            );
                            let _ = self.persist_default_run_report();
                            let _ = self.capture_runtime_report_segments();
                            storage_warning_reported = true;
                        }
                    } else if free_mb >= warning_free_mb {
                        if let Some(segment_manager) = self.segment_manager.as_mut() {
                            let _ = segment_manager.set_disk_warning_active(false);
                        }
                        storage_warning_reported = false;
                    }
                    if pause_before_os_error && free_mb < emergency_free_mb {
                        if let Some(segment_manager) = self.segment_manager.as_mut() {
                            let _ = segment_manager.record_disk_action(
                                "emergency",
                                "preserve_open_segments_and_stop",
                                free_mb,
                                0,
                                vec!["emergency disk floor reached; preserving open segments".to_owned()],
                            );
                        }
                        let note = format!(
                            "storage_limit_reached=free_mb={} critical_mb={} emergency_mb={}",
                            free_mb, critical_free_mb, emergency_free_mb
                        );
                        if !completion_notes.iter().any(|existing| existing == &note) {
                            completion_notes.push(note);
                        }
                        let mut details = BTreeMap::new();
                        details.insert("free_mb".to_owned(), free_mb.to_string());
                        details.insert("critical_free_mb".to_owned(), critical_free_mb.to_string());
                        details.insert("emergency_free_mb".to_owned(), emergency_free_mb.to_string());
                        details.insert("warning_free_mb".to_owned(), warning_free_mb.to_string());
                        let _ = self.audit(
                            RuntimeAuditKind::StorageLimitReached,
                            details,
                            Vec::new(),
                        );
                        abort_live_sources = true;
                        break;
                    }
                    if stop_collection_on_critical && free_mb < critical_free_mb {
                        if let Some(segment_manager) = self.segment_manager.as_mut() {
                            let closed = segment_manager.close_all_open_segments()?;
                            let cleanup = segment_manager.process_background_uploads().await?;
                            let _ = segment_manager.record_disk_action(
                                "critical",
                                "critical_cleanup_before_stop",
                                free_mb,
                                cleanup.pruned_bytes,
                                vec![
                                    format!("closed_segments={closed}"),
                                    format!("uploaded_segments={}", cleanup.uploaded_count),
                                    format!("verified_segments={}", cleanup.verified_count),
                                    format!("pending_segments={}", cleanup.pending_count),
                                ],
                            );
                            free_mb = filesystem_free_mb(&self.storage.layout().root).unwrap_or(u64::MAX);
                        }
                    }
                    let force_stop = stop_collection_on_critical && free_mb < critical_free_mb;
                    if force_stop {
                        let note = format!(
                            "storage_limit_reached=free_mb={} critical_mb={} emergency_mb={}",
                            free_mb, critical_free_mb, emergency_free_mb
                        );
                        if !completion_notes.iter().any(|existing| existing == &note) {
                            completion_notes.push(note);
                        }
                        let mut details = BTreeMap::new();
                        details.insert("free_mb".to_owned(), free_mb.to_string());
                        details.insert("critical_free_mb".to_owned(), critical_free_mb.to_string());
                        details.insert("emergency_free_mb".to_owned(), emergency_free_mb.to_string());
                        details.insert("warning_free_mb".to_owned(), warning_free_mb.to_string());
                        let _ = self.audit(
                            RuntimeAuditKind::StorageLimitReached,
                            details,
                            Vec::new(),
                        );
                        abort_live_sources = true;
                        break;
                    }
                    if let Some(segment_manager) = self.segment_manager.as_mut() {
                        if segment_manager.should_force_close_and_upload_on_backlog() {
                            let closed = segment_manager.close_all_open_segments()?;
                            let cleanup = segment_manager.process_background_uploads().await?;
                            let note = format!(
                                "segment_upload_backlog_warning=pending_segments={} threshold={}",
                                segment_manager.pending_segment_count(),
                                self.resolved
                                    .loaded
                                    .config
                                    .storage
                                    .segments
                                    .upload
                                    .max_pending_segments_warning
                            );
                            let _ = segment_manager.record_disk_action(
                                "warning",
                                "segment_upload_backlog_warning_cleanup",
                                free_mb,
                                cleanup.pruned_bytes,
                                vec![
                                    note,
                                    format!("closed_segments={closed}"),
                                    format!("uploaded_segments={}", cleanup.uploaded_count),
                                    format!("verified_segments={}", cleanup.verified_count),
                                    format!("pending_segments={}", cleanup.pending_count),
                                ],
                            );
                            free_mb = filesystem_free_mb(&self.storage.layout().root)
                                .unwrap_or(u64::MAX);
                        }
                        if segment_manager.backlog_exceeds_pause_threshold() {
                            let note = format!(
                                "segment_upload_backlog_pause_active=pending_segments={} threshold={}",
                                segment_manager.pending_segment_count(),
                                self.resolved
                                    .loaded
                                    .config
                                    .storage
                                    .segments
                                    .upload
                                    .max_pending_segments_pause
                            );
                            if !completion_notes.iter().any(|existing| existing == &note) {
                                completion_notes.push(note.clone());
                            }
                            let _ = segment_manager.record_disk_action(
                                "warning",
                                "segment_upload_backlog_pause_active",
                                free_mb,
                                0,
                                vec![note],
                            );
                        }
                        if segment_manager.backlog_exceeds_stop_threshold() {
                            let note = format!(
                                "segment_upload_backlog_high=pending_segments={} threshold={}",
                                segment_manager.pending_segment_count(),
                                self.resolved
                                    .loaded
                                    .config
                                    .storage
                                    .segments
                                    .upload
                                    .max_pending_segments_stop
                            );
                            if !completion_notes.iter().any(|existing| existing == &note) {
                                completion_notes.push(note.clone());
                            }
                            let closed = segment_manager.close_all_open_segments()?;
                            let cleanup = segment_manager.process_background_uploads().await?;
                            let _ = segment_manager.record_disk_action(
                                "warning",
                                "segment_upload_backlog_pause",
                                free_mb,
                                cleanup.pruned_bytes,
                                vec![
                                    note.clone(),
                                    format!("closed_segments={closed}"),
                                    format!("uploaded_segments={}", cleanup.uploaded_count),
                                    format!("verified_segments={}", cleanup.verified_count),
                                    format!("pending_segments={}", cleanup.pending_count),
                                ],
                            );
                            abort_live_sources = true;
                            break;
                        }
                    }
                    self.sync_http_snapshot(&metrics_state);
                }
                maybe_event = canonical_rx.recv(), if !canonical_closed => {
                    match maybe_event {
                        Some(event) => {
                            self.publish_canonical(event)?;
                            self.drain_pipeline().await?;
                            self.sync_http_snapshot(&metrics_state);
                            processed += 1;
                        }
                        None => canonical_closed = true,
                    }
                }
                maybe_event = tentative_rx.recv(), if !tentative_closed => {
                    match maybe_event {
                        Some(event) => {
                            self.publish_tentative(event)?;
                            self.drain_pipeline().await?;
                            self.sync_http_snapshot(&metrics_state);
                            processed += 1;
                        }
                        None => tentative_closed = true,
                    }
                }
            }

            if let Some(deadline) = deadline {
                if tokio::time::Instant::now() >= deadline {
                    abort_live_sources = true;
                    break;
                }
            }
        }

        let drain_deadline = tokio::time::Instant::now()
            + StdDuration::from_millis(
                self.resolved
                    .loaded
                    .config
                    .runtime
                    .shutdown_drain_timeout_ms,
            );
        while tokio::time::Instant::now() < drain_deadline {
            let before = self.canonical_bus.len() + self.tentative_bus.len();
            self.drain_pipeline().await?;
            if before == 0 {
                break;
            }
        }

        if abort_live_sources {
            for task in &source_tasks {
                task.abort();
            }
        }

        for task in source_tasks {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    if let Some(handle) = metrics_handle.take() {
                        handle.stop();
                    }
                    let _ = self.persist_run_metadata(RunStatus::Failed, {
                        let mut notes = live_mode_notes.clone();
                        notes.extend(self.source_health_notes());
                        notes.push(format!("source_error={error}"));
                        notes
                    });
                    let _ = self.audit(
                        RuntimeAuditKind::RuntimeStopped,
                        BTreeMap::new(),
                        Vec::new(),
                    );
                    return Err(error);
                }
                Err(error) if abort_live_sources && error.is_cancelled() => {}
                Err(error) => {
                    if let Some(handle) = metrics_handle.take() {
                        handle.stop();
                    }
                    let message = anyhow!("live source task failed: {error}");
                    let _ = self.persist_run_metadata(RunStatus::Failed, {
                        let mut notes = live_mode_notes.clone();
                        notes.extend(self.source_health_notes());
                        notes.push(format!("source_error={message}"));
                        notes
                    });
                    let _ = self.audit(
                        RuntimeAuditKind::RuntimeStopped,
                        BTreeMap::new(),
                        Vec::new(),
                    );
                    return Err(message);
                }
            }
        }

        self.finalize_run().await?;
        self.sync_http_snapshot(&metrics_state);
        let mut completed_notes = live_mode_notes;
        completed_notes.extend(completion_notes);
        completed_notes.extend(self.source_health_notes());
        self.persist_run_metadata(RunStatus::Completed, completed_notes)?;
        self.persist_default_run_report()?;
        self.capture_runtime_report_segments()?;
        self.audit(
            RuntimeAuditKind::RuntimeStopped,
            BTreeMap::new(),
            Vec::new(),
        )?;
        if let Some(segment_manager) = self.segment_manager.as_mut() {
            let _ = segment_manager.close_all_open_segments()?;
            let _ = segment_manager.process_background_uploads().await?;
        }
        if let Some(handle) = metrics_handle.take() {
            handle.stop();
        }
        Ok(self.summary())
    }

    pub fn render_token_report(&self, mint: &str) -> Result<String> {
        let token = self
            .summary()
            .snapshot
            .tokens
            .get(mint)
            .cloned()
            .ok_or_else(|| anyhow!("token {mint} not found"))?;
        let features = self.latest_features.get(mint);
        let risk = self.latest_risk.get(mint);
        let decisions = self
            .decisions
            .iter()
            .filter(|record| record.mint.0 == mint)
            .cloned()
            .collect::<Vec<_>>();
        let decision_outcomes = self
            .decision_outcomes
            .iter()
            .filter(|outcome| outcome.decision_event.mint.0 == mint)
            .cloned()
            .collect::<Vec<_>>();
        let fills = self
            .paper_executor
            .ledger()
            .fills
            .iter()
            .filter(|record| record.mint.0 == mint)
            .cloned()
            .collect::<Vec<_>>();

        let mut out = String::new();
        out.push_str(&format!("# Token Report: {}\n\n", token.mint.0));
        out.push_str(&format!(
            "- lifecycle: {:?}\n- launch_slot: {:?}\n- creator: {:?}\n- quote_mint: {:?}\n- latest_price: {}\n- holders: {}\n- top1_holder_pct: {}\n- creator_sell_pct: {}\n",
            token.lifecycle,
            token.launch_slot,
            token.creator.as_ref().map(|value| value.0.as_str()),
            token.quote_mint.as_ref().map(|value| value.0.as_str()),
            token.latest_price,
            token.holder_state.nonzero_holder_count,
            token.holder_state.top_holder_pct(1),
            token.developer_state.creator_sell_percentage,
        ));
        if let Some(risk) = risk {
            out.push_str("\n## Risk\n");
            out.push_str(&format!(
                "- rug: {} ({:?})\n- bundle: {} ({:?})\n- fake_momentum: {} ({:?})\n- data_quality: {} ({:?})\n",
                risk.rug.score,
                risk.rug.reason_codes,
                risk.bundle.score,
                risk.bundle.reason_codes,
                risk.fake_momentum.score,
                risk.fake_momentum.reason_codes,
                risk.data_quality.score,
                risk.data_quality.reason_codes,
            ));
        }
        if let Some(features) = features {
            out.push_str("\n## Feature Highlights\n");
            for feature_id in [
                "token_relative_strength_score",
                "profit_overhang_score",
                "absorption_success_score",
                "momentum_authenticity_score",
                "holder_stickiness_score",
                "funding_graph_suspicion_score",
                "fee_war_score",
            ] {
                if let Some(value) = features.decimal(feature_id) {
                    out.push_str(&format!("- {feature_id}: {value}\n"));
                }
            }
            out.push_str("\n## Thresholds\n");
            out.push_str(&format!(
                "- min_trade_eligibility_score: {}\n- min_holder_stickiness_score: {}\n- min_momentum_authenticity_score: {}\n- max_profit_overhang_score: {}\n",
                self.resolved.loaded.config.strategy.min_trade_eligibility_score,
                self.resolved.loaded.config.strategy.min_holder_stickiness_score,
                self.resolved.loaded.config.strategy.min_momentum_authenticity_score,
                self.resolved.loaded.config.strategy.max_profit_overhang_score,
            ));
        }
        out.push_str("\n## Decision Timeline\n");
        for record in decisions {
            out.push_str(&format!(
                "- {:?} @ {} reasons={:?}\n",
                record.decision,
                record.no_lookahead_timestamp.unix_timestamp(),
                record.reason_codes
            ));
        }
        if let Some(outcome) = decision_outcomes.last() {
            out.push_str("\n## Decision Breakdown\n");
            for (score_name, score) in &outcome.composite_scores {
                out.push_str(&format!(
                    "- {} raw={} adjusted={} confidence={} positives={:?} negatives={:?}\n",
                    score_name,
                    score.raw_score,
                    score.regime_adjusted_score,
                    score.confidence,
                    score.top_positive_components,
                    score.top_negative_components
                ));
            }
            if !outcome.diagnostics.is_empty() {
                out.push_str("\n## Diagnostics\n");
                for line in &outcome.diagnostics {
                    out.push_str(&format!("- {line}\n"));
                }
            }
        }
        out.push_str("\n## Paper Fills\n");
        for record in fills {
            out.push_str(&format!(
                "- {:?} size={} price={} fees={} failure={:?}\n",
                record.side,
                record.filled_size,
                record.fill_price,
                record.fees,
                record.failure_reason
            ));
        }
        out.push_str(&format!(
            "\n## Run Context\n- config_hash: {}\n- idl_hash: {}\n- strategy_version: {}\n",
            self.resolved.config_hash,
            self.resolved.idl_hash,
            self.resolved.loaded.config.environment.strategy_version
        ));
        Ok(out)
    }

    pub fn render_run_report(&self) -> Result<String> {
        let summary = self.summary();
        let calibration = self.tentative_sell_manager.calibration();
        let budget = self.rpc_budget.summary();
        Ok(format!(
            "# Run Report\n\n- run_id: {}\n- run_kind: {:?}\n- source_run_id: {}\n- scenario_id: {}\n- mode: {:?}\n- active_tokens: {}\n- active_positions: {}\n- paper_pnl: {}\n- realized_pnl: {}\n- fee_drag: {}\n- slippage_drag: {}\n- latency_drag: {}\n- decisions: {:?}\n- audits: {}\n- data_gap_active: {}\n- data_gap_scope: {:?}\n- stream_only_enabled: {}\n- stream_only_passed: {}\n- rpc_network_calls_total: {}\n- rpc_credits_used_total: {}\n- rpc_denials_total: {}\n- market_data_rpc_calls_allowed: {}\n- holder_rpc_calls_allowed: {}\n- metadata_fetch_allowed: {}\n- confirmation_rpc_allowed: {}\n- blockhash_rpc_allowed: {}\n- tentative_sell_warnings: {}\n- shred_emergency_exits_triggered: {}\n- shred_saved_loss_quote_total: {}\n- shred_opportunity_cost_quote_total: {}\n- deshred_supported: {}\n- deshred_connected: {}\n- deshred_provider_status: {}\n- deshred_events_received: {}\n- early_intent_sources_active: {:?}\n- raw_shred_supported: {}\n- raw_shred_connected: {}\n- mock_early_intent_active: {}\n- fixture_early_intent_tentative_sells_total: {}\n- replay_early_intent_tentative_sells_total: {}\n- source_dedup_count: {}\n- rpc_budget_summary: {:?}\n- calibration_version_hash: {}\n- live_execution_disabled: {}\n",
            summary.run_id,
            self.current_run_kind(),
            summary
                .source_run_id
                .clone()
                .unwrap_or_else(|| "none".to_owned()),
            summary
                .scenario_id
                .clone()
                .unwrap_or_else(|| "none".to_owned()),
            summary.mode,
            summary.health.active_tokens,
            summary.health.active_positions,
            summary.health.paper_pnl,
            summary.health.realized_pnl,
            summary.health.fee_drag,
            summary.health.slippage_drag,
            summary.health.latency_drag,
            summary.decisions_by_type,
            summary.audits.len(),
            summary.health.data_gap_active,
            summary.health.data_gap_scope,
            summary.health.stream_only_enabled,
            summary.health.stream_only_passed,
            summary.health.rpc_network_calls_total,
            summary.health.rpc_credits_used_total,
            summary.health.rpc_denials_total,
            summary.health.market_data_rpc_calls_allowed,
            summary.health.holder_rpc_calls_allowed,
            summary.health.metadata_fetch_allowed,
            summary.health.confirmation_rpc_allowed,
            summary.health.blockhash_rpc_allowed,
            summary.health.shred_malicious_sell_warnings_total,
            summary.health.shred_emergency_exits_triggered_total,
            summary.health.shred_saved_loss_quote_total,
            summary.health.shred_opportunity_cost_quote_total,
            summary.health.deshred_supported,
            summary.health.deshred_connected,
            summary.health.deshred_provider_status,
            summary.health.deshred_events_received,
            summary.health.early_intent_sources_active,
            summary.health.raw_shred_supported,
            summary.health.raw_shred_connected,
            summary.health.mock_early_intent_active,
            summary.health.fixture_early_intent_tentative_sells_total,
            summary.health.replay_early_intent_tentative_sells_total,
            summary.health.source_dedup_count,
            budget,
            calibration
                .persisted_version_hash
                .clone()
                .unwrap_or_else(|| "none".to_owned()),
            !summary.safety.live_allowed,
        ))
    }

    pub fn render_strategy_summary(&self) -> String {
        let summary = self.summary();
        let mut fills_by_strategy = BTreeMap::<String, Vec<FillEvent>>::new();
        for fill in &summary.fills {
            fills_by_strategy
                .entry(
                    fill.strategy
                        .clone()
                        .unwrap_or_else(|| "unknown".to_owned()),
                )
                .or_default()
                .push(fill.clone());
        }
        let mut decisions_by_strategy = BTreeMap::<String, BTreeMap<String, u64>>::new();
        for decision in &summary.decision_events {
            *decisions_by_strategy
                .entry(decision.strategy.clone())
                .or_default()
                .entry(format!("{:?}", decision.decision))
                .or_default() += 1;
        }

        let mut out = format!("# Strategy Summary {}\n\n", summary.run_id);
        for (strategy, counts) in decisions_by_strategy {
            let fills = fills_by_strategy.remove(&strategy).unwrap_or_default();
            let net = fills
                .iter()
                .map(|fill| fill.net_pnl_quote.unwrap_or(Decimal::ZERO))
                .sum::<Decimal>();
            let wins = fills
                .iter()
                .filter(|fill| fill.net_pnl_quote.unwrap_or(Decimal::ZERO) > Decimal::ZERO)
                .count();
            let watch_count = counts.get("WatchDeep").copied().unwrap_or_default();
            let enter_count = counts.get("EnterPaper").copied().unwrap_or_default();
            let reject_count = counts.get("StopTracking").copied().unwrap_or_default();
            out.push_str(&format!(
                "## {strategy}\n- enter_paper: {enter_count}\n- watch_deep: {watch_count}\n- stop_tracking: {reject_count}\n- fills: {}\n- wins: {wins}\n- net_pnl: {net}\n",
                fills.len(),
            ));
            if let Some(best) = fills
                .iter()
                .max_by(|left, right| left.net_pnl_quote.cmp(&right.net_pnl_quote))
            {
                out.push_str(&format!(
                    "- best_trade: mint={} net_pnl={} exit={}\n",
                    best.mint.0,
                    best.net_pnl_quote.unwrap_or(Decimal::ZERO),
                    best.exit_classification
                        .clone()
                        .unwrap_or_else(|| "unknown".to_owned())
                ));
            }
            if let Some(worst) = fills
                .iter()
                .min_by(|left, right| left.net_pnl_quote.cmp(&right.net_pnl_quote))
            {
                out.push_str(&format!(
                    "- worst_trade: mint={} net_pnl={} exit={}\n",
                    worst.mint.0,
                    worst.net_pnl_quote.unwrap_or(Decimal::ZERO),
                    worst
                        .exit_classification
                        .clone()
                        .unwrap_or_else(|| "unknown".to_owned())
                ));
            }
            out.push('\n');
        }
        out
    }

    pub fn render_pnl_attribution_report(&self) -> String {
        let summary = self.summary();
        let mut out = format!("# PnL Attribution {}\n\n", summary.run_id);
        if summary.fills.is_empty() {
            out.push_str("- no fills were simulated\n");
            return out;
        }
        for fill in &summary.fills {
            out.push_str(&format!(
                "## {} {:?}\n- strategy: {}\n- entry_decision_id: {}\n- exit_decision_id: {}\n- gross_pnl: {}\n- net_pnl: {}\n- base_fee: {}\n- priority_fee: {}\n- tip: {}\n- slippage_cost: {}\n- curve_impact_cost: {}\n- latency_cost: {}\n- failed_tx_cost: {}\n- hold_time_ms: {:?}\n- expected_edge: {}\n- realized_edge: {}\n- edge_forecast_error: {}\n- exit_reason: {}\n- exit_classification: {}\n- exit_source: {}\n- trigger_event_id: {}\n- malicious_sell_seller: {}\n- malicious_sell_classification: {}\n- estimated_loss_saved_quote: {}\n- realized_loss_saved_quote: {}\n- false_positive_exit: {}\n- opportunity_cost_if_false_positive: {}\n- entry_risk_scores: {:?}\n- exit_risk_scores: {:?}\n\n",
                fill.mint.0,
                fill.side,
                fill.strategy.clone().unwrap_or_else(|| "unknown".to_owned()),
                fill.entry_decision_id.clone().unwrap_or_else(|| "none".to_owned()),
                fill.exit_decision_id.clone().unwrap_or_else(|| "none".to_owned()),
                fill.gross_pnl_quote.unwrap_or(Decimal::ZERO),
                fill.net_pnl_quote.unwrap_or(Decimal::ZERO),
                fill.base_fee_quote.unwrap_or(Decimal::ZERO),
                fill.priority_fee_quote.unwrap_or(Decimal::ZERO),
                fill.tip_quote.unwrap_or(Decimal::ZERO),
                fill.slippage_cost_quote.unwrap_or(Decimal::ZERO),
                fill.curve_impact_cost_quote.unwrap_or(Decimal::ZERO),
                fill.latency_cost_quote.unwrap_or(Decimal::ZERO),
                fill.failed_tx_cost_quote.unwrap_or(Decimal::ZERO),
                fill.hold_time_ms,
                fill.expected_edge_quote.unwrap_or(Decimal::ZERO),
                fill.actual_realized_edge_quote.unwrap_or(Decimal::ZERO),
                fill.edge_forecast_error_quote.unwrap_or(Decimal::ZERO),
                fill.exit_reason.clone().unwrap_or_else(|| "unknown".to_owned()),
                fill.exit_classification.clone().unwrap_or_else(|| "unknown".to_owned()),
                fill.exit_source.clone().unwrap_or_else(|| "unknown".to_owned()),
                fill.trigger_event_id.clone().unwrap_or_else(|| "none".to_owned()),
                fill.malicious_sell_seller.clone().unwrap_or_else(|| "none".to_owned()),
                fill.malicious_sell_classification.clone().unwrap_or_else(|| "none".to_owned()),
                fill.estimated_loss_saved_quote.unwrap_or(Decimal::ZERO),
                fill.realized_loss_saved_quote.unwrap_or(Decimal::ZERO),
                fill.false_positive_exit,
                fill.opportunity_cost_if_false_positive.unwrap_or(Decimal::ZERO),
                fill.entry_risk_scores,
                fill.exit_risk_scores,
            ));
        }
        out
    }

    pub fn render_data_gaps_report(&self) -> String {
        let summary = self.summary();
        let mut out = format!("# Data Gaps {}\n\n", summary.run_id);
        if self.active_data_gaps.is_empty() {
            out.push_str("- no active or recorded data gaps at completion\n");
        }
        for gap in &self.active_data_gaps {
            out.push_str(&format!(
                "- scope={:?} source={:?} active={} blocking={} token={} start_slot={} end_slot={:?} severity={:?} recovery={}\n",
                gap.scope,
                gap.source,
                gap.active,
                gap.trade_blocking,
                gap.affected_token.clone().unwrap_or_else(|| "_none".to_owned()),
                gap.start_slot,
                gap.end_slot,
                gap.severity,
                gap.recovery_action,
            ));
        }
        out
    }

    pub fn render_runtime_health_report(&self) -> String {
        let summary = self.summary();
        format!(
            "# Runtime Health {}\n\n- mode: {:?}\n- stream_only_enabled: {}\n- stream_only_passed: {}\n- rpc_network_calls_total: {}\n- rpc_credits_used_total: {}\n- rpc_denials_total: {}\n- market_data_rpc_calls_allowed: {}\n- holder_rpc_calls_allowed: {}\n- metadata_fetch_allowed: {}\n- confirmation_rpc_allowed: {}\n- blockhash_rpc_allowed: {}\n- early_intent_enabled: {}\n- early_intent_sources_active: {:?}\n- geyser_connected: {}\n- deshred_connected: {}\n- deshred_provider_status: {}\n- deshred_updates_received: {}\n- deshred_transactions_decoded: {}\n- deshred_tentative_sells_total: {}\n- deshred_malicious_warnings_total: {}\n- deshred_emergency_exits_armed_total: {}\n- deshred_emergency_exits_triggered_total: {}\n- deshred_confirmed_executed_total: {}\n- deshred_false_positive_total: {}\n- deshred_saved_loss_quote_total: {}\n- deshred_opportunity_cost_quote_total: {}\n- raw_shred_connected: {}\n- raw_shred_tentative_sells_total: {}\n- mock_early_intent_active: {}\n- fixture_early_intent_tentative_sells_total: {}\n- replay_early_intent_tentative_sells_total: {}\n- source_dedup_count: {}\n- shred_connected: {}\n- storage_healthy: {}\n- rpc_budget_healthy: {}\n- events_processed: {}\n- canonical_queue_depth: {}\n- tentative_queue_depth: {}\n- data_gap_active: {}\n- data_gap_scope: {:?}\n- paper_pnl: {}\n- realized_pnl: {}\n- fee_drag: {}\n- slippage_drag: {}\n- latency_drag: {}\n- uptime_ms: {}\n- live_source_mode: {}\n",
            summary.run_id,
            summary.mode,
            summary.health.stream_only_enabled,
            summary.health.stream_only_passed,
            summary.health.rpc_network_calls_total,
            summary.health.rpc_credits_used_total,
            summary.health.rpc_denials_total,
            summary.health.market_data_rpc_calls_allowed,
            summary.health.holder_rpc_calls_allowed,
            summary.health.metadata_fetch_allowed,
            summary.health.confirmation_rpc_allowed,
            summary.health.blockhash_rpc_allowed,
            summary.health.early_intent_enabled,
            summary.health.early_intent_sources_active,
            summary.health.geyser_connected,
            summary.health.deshred_connected,
            summary.health.deshred_provider_status,
            summary.health.deshred_events_received,
            summary.health.deshred_transactions_decoded,
            summary.health.deshred_tentative_sells_total,
            summary.health.deshred_malicious_warnings_total,
            summary.health.deshred_emergency_exits_armed_total,
            summary.health.deshred_emergency_exits_triggered_total,
            summary.health.deshred_confirmed_executed_total,
            summary.health.deshred_false_positive_total,
            summary.health.deshred_saved_loss_quote_total,
            summary.health.deshred_opportunity_cost_quote_total,
            summary.health.raw_shred_connected,
            summary.health.raw_shred_tentative_sells_total,
            summary.health.mock_early_intent_active,
            summary.health.fixture_early_intent_tentative_sells_total,
            summary.health.replay_early_intent_tentative_sells_total,
            summary.health.source_dedup_count,
            summary.health.shred_connected,
            summary.health.storage_healthy,
            summary.health.rpc_budget_healthy,
            summary.health.events_processed,
            summary.health.canonical_queue_depth,
            summary.health.tentative_queue_depth,
            summary.health.data_gap_active,
            summary.health.data_gap_scope,
            summary.health.paper_pnl,
            summary.health.realized_pnl,
            summary.health.fee_drag,
            summary.health.slippage_drag,
            summary.health.latency_drag,
            summary.health.runtime_uptime_ms,
            summary.health.live_source_mode,
        )
    }

    pub fn render_stream_only_audit_report(&self) -> String {
        let validation = self.resolved.loaded.stream_only_validation_summary();
        let budget = self.rpc_budget.summary();
        format!(
            "# Stream Only Audit {}\n\n- stream_only_enabled: {}\n- stream_only_passed: {}\n- validation_issues: {:?}\n- rpc_network_calls_total: {}\n- rpc_credits_used_total: {}\n- rpc_denials_total: {}\n- market_data_rpc_calls_allowed: {}\n- holder_rpc_calls_allowed: {}\n- metadata_fetch_allowed: {}\n- confirmation_rpc_allowed: {}\n- blockhash_rpc_allowed: {}\n- rpc_hot_path_enabled: {}\n- stream_only_policy: {:?}\n- all_stream_sources_used: {:?}\n",
            self.resolved.loaded.config.environment.run_id,
            validation.stream_only_enabled,
            validation.passed,
            validation.violations,
            budget.network_calls_total,
            budget.daily_used,
            budget.denied_entries,
            budget.market_data_rpc_calls_allowed,
            budget.holder_rpc_calls_allowed,
            budget.metadata_fetch_allowed,
            budget.confirmation_rpc_allowed,
            budget.blockhash_rpc_allowed,
            budget.rpc_hot_path_enabled,
            budget.stream_only_policy,
            self.health.early_intent_sources_active,
        )
    }

    pub fn render_rpc_denials_report(&self) -> String {
        let ledger = self.rpc_budget.ledger().to_vec();
        let mut out = format!(
            "# RPC Denials {}\n\n- total_entries: {}\n- denied_entries: {}\n\n",
            self.resolved.loaded.config.environment.run_id,
            ledger.len(),
            ledger.iter().filter(|entry| entry.denied).count()
        );
        let denied = ledger
            .into_iter()
            .filter(|entry| entry.denied)
            .collect::<Vec<_>>();
        if denied.is_empty() {
            out.push_str("- none\n");
            return out;
        }
        for entry in denied {
            out.push_str(&format!(
                "- timestamp={} category={:?} method={} caller={} reason={} network_touched={} estimated_credits={} actual_credits={}\n",
                entry.request.timestamp,
                entry.request.category,
                entry.request.method,
                entry.request.caller_module,
                entry
                    .denial_reason
                    .clone()
                    .unwrap_or_else(|| "denied".to_owned()),
                entry.network_touched,
                entry.request.estimated_provider_credit_cost,
                entry
                    .request
                    .actual_provider_credit_cost
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_owned()),
            ));
        }
        out
    }

    pub fn render_stream_source_health_report(&self) -> String {
        format!(
            "# Stream Source Health {}\n\n- early_intent_enabled: {}\n- early_intent_sources_active: {:?}\n- geyser_connected: {}\n- deshred_supported: {}\n- deshred_connected: {}\n- deshred_provider_status: {}\n- deshred_events_received: {}\n- deshred_transactions_decoded: {}\n- deshred_tentative_sells_total: {}\n- deshred_malicious_warnings_total: {}\n- deshred_emergency_exits_armed_total: {}\n- deshred_emergency_exits_triggered_total: {}\n- deshred_confirmed_executed_total: {}\n- deshred_false_positive_total: {}\n- deshred_saved_loss_quote_total: {}\n- deshred_opportunity_cost_quote_total: {}\n- raw_shred_supported: {}\n- raw_shred_connected: {}\n- raw_shred_tentative_sells_total: {}\n- mock_early_intent_active: {}\n- mock_early_intent_tentative_sells_total: {}\n- fixture_early_intent_tentative_sells_total: {}\n- replay_early_intent_tentative_sells_total: {}\n- source_dedup_count: {}\n- early_intent_dedup_pairs: {:?}\n",
            self.resolved.loaded.config.environment.run_id,
            self.health.early_intent_enabled,
            self.health.early_intent_sources_active,
            self.health.geyser_connected,
            self.health.deshred_supported,
            self.health.deshred_connected,
            self.health.deshred_provider_status,
            self.health.deshred_events_received,
            self.health.deshred_transactions_decoded,
            self.health.deshred_tentative_sells_total,
            self.health.deshred_malicious_warnings_total,
            self.health.deshred_emergency_exits_armed_total,
            self.health.deshred_emergency_exits_triggered_total,
            self.health.deshred_confirmed_executed_total,
            self.health.deshred_false_positive_total,
            self.health.deshred_saved_loss_quote_total,
            self.health.deshred_opportunity_cost_quote_total,
            self.health.raw_shred_supported,
            self.health.raw_shred_connected,
            self.health.raw_shred_tentative_sells_total,
            self.health.mock_early_intent_active,
            self.health.mock_early_intent_tentative_sells_total,
            self.health.fixture_early_intent_tentative_sells_total,
            self.health.replay_early_intent_tentative_sells_total,
            self.health.source_dedup_count,
            self.health.early_intent_dedup_pairs,
        )
    }

    pub fn render_rejection_reasons_report(&self) -> String {
        let summary = self.summary();
        let mut counts = BTreeMap::<String, u64>::new();
        for decision in &summary.decision_events {
            for reason in &decision.reason_codes {
                *counts.entry(format!("{reason:?}")).or_default() += 1;
            }
        }
        let mut out = format!("# Rejection Reasons {}\n\n", summary.run_id);
        if counts.is_empty() {
            out.push_str("- no decision reason codes recorded\n");
            return out;
        }
        for (reason, count) in counts {
            out.push_str(&format!("- {reason}: {count}\n"));
        }
        out
    }

    pub fn render_edge_calibration_report(&self) -> String {
        let summary = self.summary();
        let enter_count = summary
            .decision_events
            .iter()
            .filter(|event| event.decision == TradeDecision::EnterPaper)
            .count();
        let watch_count = summary
            .decision_events
            .iter()
            .filter(|event| event.decision == TradeDecision::WatchDeep)
            .count();
        let avg_expected = if summary.decision_events.is_empty() {
            Decimal::ZERO
        } else {
            summary
                .decision_events
                .iter()
                .map(|event| event.expected_net_edge_quote)
                .sum::<Decimal>()
                / Decimal::from(summary.decision_events.len() as u64)
        };
        let avg_realized = if summary.fills.is_empty() {
            Decimal::ZERO
        } else {
            summary
                .fills
                .iter()
                .map(|fill| fill.actual_realized_edge_quote.unwrap_or(Decimal::ZERO))
                .sum::<Decimal>()
                / Decimal::from(summary.fills.len() as u64)
        };
        format!(
            "# Edge Calibration {}\n\n- enter_paper: {}\n- watch_deep: {}\n- average_expected_edge_quote: {}\n- average_realized_edge_quote: {}\n- fee_drag: {}\n- slippage_drag: {}\n- latency_drag: {}\n- shred_saved_loss_quote_total: {}\n- shred_opportunity_cost_quote_total: {}\n",
            summary.run_id,
            enter_count,
            watch_count,
            avg_expected,
            avg_realized,
            summary.health.fee_drag,
            summary.health.slippage_drag,
            summary.health.latency_drag,
            summary.health.shred_saved_loss_quote_total,
            summary.health.shred_opportunity_cost_quote_total,
        )
    }

    pub fn render_shred_exit_defense_report(&self) -> String {
        let source_breakdown = self
            .tentative_sell_manager
            .source_metrics()
            .iter()
            .map(|(source, metrics)| (source.clone(), metrics.tentative_sell_total))
            .collect::<BTreeMap<_, _>>();
        let mut primary_source_summary = source_breakdown.clone();
        let mut duplicate_sources_summary = BTreeMap::<String, u64>::new();
        for (pair, count) in self.tentative_sell_manager.dedup_pairs() {
            let mut parts = pair.split('|');
            if let Some(primary_source) = parts.next().filter(|value| !value.is_empty()) {
                *primary_source_summary
                    .entry(primary_source.to_owned())
                    .or_default() += *count;
            }
            if let Some(duplicate_source) = parts.next().filter(|value| !value.is_empty()) {
                if let Some(primary_count) = primary_source_summary.get_mut(duplicate_source) {
                    *primary_count = primary_count.saturating_sub(*count);
                }
                *duplicate_sources_summary
                    .entry(duplicate_source.to_owned())
                    .or_default() += source_breakdown
                    .get(duplicate_source)
                    .copied()
                    .unwrap_or_default()
                    .saturating_add(*count);
            }
        }
        primary_source_summary.retain(|_, count| *count > 0);
        duplicate_sources_summary.retain(|_, count| *count > 0);
        if self.aggregated_snapshot.is_some()
            && !self.tentative_sell_manager.has_tentative_activity()
        {
            let summary = self.summary();
            let calibration = self.tentative_sell_manager.calibration();
            format!(
                "# Shred Exit Defense {}\n\n- total_tentative_sells: {}\n- malicious_sell_warnings: {}\n- emergency_exits_armed: {}\n- emergency_exits_triggered: {}\n- confirmed_true_positives: {}\n- confirmed_failed: {}\n- not_seen_within_ttl: {}\n- reorged: {}\n- decode_mismatches: {}\n- account_effect_confirmed: {}\n- false_positive_exits: {}\n- saved_loss_quote_total: {}\n- opportunity_cost_quote_total: {}\n- source_breakdown: {:?}\n- primary_source_summary: {:?}\n- duplicate_sources_summary: {:?}\n- confirmation_method_breakdown: aggregate_replay\n- early_intent_sources_active: {:?}\n- deshred_supported: {}\n- deshred_connected: {}\n- deshred_events_received: {}\n- deshred_decode_errors: {}\n- raw_shred_supported: {}\n- raw_shred_connected: {}\n- mock_early_intent_active: {}\n- fixture_early_intent_tentative_sells_total: {}\n- replay_early_intent_tentative_sells_total: {}\n- source_dedup_count: {}\n- average_latency_edge_ratio: 0\n- average_net_benefit_quote: 0\n- too_late_cases: {}\n- absorbed_sell_cases: {}\n- calibration_false_positive_rate: {}\n- calibration_missed_sell_rate: {}\n- calibration_true_positive_rate: {}\n- calibration_adjustments: {:?}\n- calibration_path: {}\n- calibration_version_hash: {}\n",
                summary.run_id,
                summary.health.shred_tentative_sells_total,
                summary.health.shred_malicious_sell_warnings_total,
                summary.health.shred_emergency_exits_armed_total,
                summary.health.shred_emergency_exits_triggered_total,
                summary.health.shred_confirmed_executed_total,
                summary.health.shred_confirmed_failed_total,
                summary.health.shred_not_seen_within_ttl_total,
                summary.health.shred_reorged_total,
                summary.health.shred_decode_mismatch_total,
                summary.health.shred_account_effect_confirmed_total,
                summary.health.shred_sell_false_positive_total,
                summary.health.shred_saved_loss_quote_total,
                summary.health.shred_opportunity_cost_quote_total,
                source_breakdown,
                primary_source_summary,
                duplicate_sources_summary,
                summary.health.early_intent_sources_active,
                summary.health.deshred_supported,
                summary.health.deshred_connected,
                summary.health.deshred_events_received,
                summary.health.deshred_decode_errors,
                summary.health.raw_shred_supported,
                summary.health.raw_shred_connected,
                summary.health.mock_early_intent_active,
                summary.health.fixture_early_intent_tentative_sells_total,
                summary.health.replay_early_intent_tentative_sells_total,
                summary.health.source_dedup_count,
                summary
                    .decision_events
                    .iter()
                    .filter(|event| {
                        event.reason_codes.contains(&ReasonCode::ShredSignalStale)
                            && matches!(event.decision, TradeDecision::Hold | TradeDecision::Exit)
                    })
                    .count(),
                summary
                    .decision_events
                    .iter()
                    .filter(|event| {
                        event
                            .reason_codes
                            .contains(&ReasonCode::ShredTopHolderSellWarning)
                            && matches!(event.decision, TradeDecision::Hold)
                    })
                    .count(),
                calibration.false_positive_rate,
                calibration.missed_sell_rate,
                calibration.true_positive_rate,
                calibration.adaptations,
                calibration.persisted_path.clone().unwrap_or_else(|| self
                    .resolved
                    .loaded
                    .config
                    .shred_exit
                    .calibration
                    .path
                    .clone()),
                calibration
                    .persisted_version_hash
                    .clone()
                    .unwrap_or_default(),
            )
        } else {
            let base = self
                .tentative_sell_manager
                .render_report(&self.resolved.loaded.config.environment.run_id);
            format!(
                "{base}\n- primary_source_summary: {:?}\n- duplicate_sources_summary: {:?}\n- early_intent_sources_active: {:?}\n- deshred_supported: {}\n- deshred_connected: {}\n- deshred_provider_status: {}\n- deshred_events_received: {}\n- deshred_malicious_warnings_total: {}\n- deshred_emergency_exits_armed_total: {}\n- deshred_emergency_exits_triggered_total: {}\n- deshred_confirmed_executed_total: {}\n- deshred_false_positive_total: {}\n- deshred_saved_loss_quote_total: {}\n- deshred_opportunity_cost_quote_total: {}\n- deshred_decode_errors: {}\n- deshred_reconnect_count: {}\n- raw_shred_supported: {}\n- raw_shred_connected: {}\n- mock_early_intent_active: {}\n- fixture_early_intent_tentative_sells_total: {}\n- replay_early_intent_tentative_sells_total: {}\n- source_dedup_count: {}\n",
                primary_source_summary,
                duplicate_sources_summary,
                self.health.early_intent_sources_active,
                self.health.deshred_supported,
                self.health.deshred_connected,
                self.health.deshred_provider_status,
                self.health.deshred_events_received,
                self.health.deshred_malicious_warnings_total,
                self.health.deshred_emergency_exits_armed_total,
                self.health.deshred_emergency_exits_triggered_total,
                self.health.deshred_confirmed_executed_total,
                self.health.deshred_false_positive_total,
                self.health.deshred_saved_loss_quote_total,
                self.health.deshred_opportunity_cost_quote_total,
                self.health.deshred_decode_errors,
                self.health.deshred_reconnect_count,
                self.health.raw_shred_supported,
                self.health.raw_shred_connected,
                self.health.mock_early_intent_active,
                self.health.fixture_early_intent_tentative_sells_total,
                self.health.replay_early_intent_tentative_sells_total,
                self.health.source_dedup_count,
            )
        }
    }

    pub fn render_shred_exit_calibration_report(&self) -> String {
        self.tentative_sell_manager.render_calibration_report(
            &self.resolved.loaded.config.environment.run_id,
            &self.resolved.loaded.config,
        )
    }

    pub fn render_top_tokens_report(&self) -> String {
        let summary = self.summary();
        let mut tokens = summary
            .snapshot
            .tokens
            .values()
            .cloned()
            .collect::<Vec<_>>();
        tokens.sort_by(|left, right| {
            right
                .trade_stats
                .buy_volume_quote
                .cmp(&left.trade_stats.buy_volume_quote)
                .then_with(|| right.trade_stats.buy_count.cmp(&left.trade_stats.buy_count))
        });
        let mut out = format!("# Top Tokens {}\n\n", summary.run_id);
        for token in tokens.into_iter().take(10) {
            let risk = summary.latest_risk.get(&token.mint.0);
            out.push_str(&format!(
                "- mint={} lifecycle={:?} buy_volume={} holders={} buy_count={} rug_score={} bundle_score={}\n",
                token.mint.0,
                token.lifecycle,
                token.trade_stats.buy_volume_quote,
                token.holder_state.nonzero_holder_count,
                token.trade_stats.buy_count,
                risk.map(|assessment| assessment.rug.score).unwrap_or(Decimal::ZERO),
                risk.map(|assessment| assessment.bundle.score).unwrap_or(Decimal::ZERO),
            ));
        }
        out
    }

    pub fn render_online_data_collection_report(&self) -> String {
        let summary = self.summary();
        let watch_count = summary
            .decision_events
            .iter()
            .filter(|event| event.decision == TradeDecision::WatchDeep)
            .count();
        let enter_count = summary
            .decision_events
            .iter()
            .filter(|event| event.decision == TradeDecision::EnterPaper)
            .count();
        format!(
            "# Online Data Collection {}\n\n- events_processed: {}\n- tokens_discovered: {}\n- watch_deep_tokens: {}\n- enter_paper_candidates: {}\n- discarded_tokens: {}\n- rugged_tokens: {}\n- fills: {}\n- top_rejection_reasons_report: rejection_reasons.md\n",
            summary.run_id,
            summary.health.events_processed,
            summary.snapshot.tokens.len(),
            watch_count,
            enter_count,
            summary.health.discarded_tokens,
            summary.health.rugged_tokens,
            summary.fills.len(),
        )
    }

    async fn ingest_fixture_shreds(&mut self, batches: &[DecodedShredBatch]) -> Result<()> {
        let Some(_) = self.shred_service.as_ref().map(ShredService::metrics) else {
            if self.resolved.loaded.config.shred.enabled {
                return Err(RuntimeError::ProductionShredDecoderDisabled.into());
            }
            return Ok(());
        };
        for batch in batches {
            let slot = batch.slot.unwrap_or_default();
            let packet = ReceivedPacket {
                data: serde_json::to_vec(batch)?,
                peer_addr: "127.0.0.1:0".parse().expect("loopback"),
                received_at: OffsetDateTime::UNIX_EPOCH + Duration::seconds(slot as i64),
                observed_at_monotonic_ns: slot * 1_000_000_000,
                packet_hash: format!("fixture-{slot}"),
            };
            let events = {
                let Some(shred_service) = self.shred_service.as_mut() else {
                    return Err(RuntimeError::Fixture(
                        "shred service became unavailable".to_owned(),
                    )
                    .into());
                };
                shred_service.process_packet(&packet)?
            };
            for event in events {
                self.publish_tentative(event)?;
            }
        }
        Ok(())
    }

    async fn drain_pipeline(&mut self) -> Result<()> {
        loop {
            let mut handled = false;
            if let Ok(Some(event)) =
                tokio::time::timeout(StdDuration::from_millis(5), self.canonical_bus.recv()).await
            {
                self.handle_event(event, false).await?;
                handled = true;
            } else if let Ok(Some(event)) =
                tokio::time::timeout(StdDuration::from_millis(5), self.tentative_bus.recv()).await
            {
                self.handle_event(event, true).await?;
                handled = true;
            }

            if !handled {
                if let Some(shred_service) = self.shred_service.as_mut() {
                    let expired = shred_service.expire_tentative(unix_now());
                    if !expired.is_empty() {
                        self.audit(
                            RuntimeAuditKind::QueueOverflow,
                            BTreeMap::from([(
                                "expired_tentative".to_owned(),
                                expired.len().to_string(),
                            )]),
                            vec![ReasonCode::TentativeTimedOut],
                        )?;
                    }
                }
                break;
            }
        }
        Ok(())
    }

    async fn handle_event(&mut self, event: NormalizedEvent, tentative: bool) -> Result<()> {
        self.health.events_processed = self.health.events_processed.saturating_add(1);
        self.health.last_event_time = Some(event.meta.received_at_wall_time);
        self.health.canonical_queue_depth = self.canonical_bus.len();
        self.health.tentative_queue_depth = self.tentative_bus.len();
        self.health.event_queue_depth =
            self.health.canonical_queue_depth + self.health.tentative_queue_depth;
        self.health.geyser_connected |= matches!(
            event.meta.source,
            EventSource::GeyserProcessed | EventSource::GeyserConfirmed | EventSource::GeyserRooted
        );
        self.health.deshred_connected |= matches!(event.meta.source, EventSource::DeshredTentative);
        self.health.shred_connected |= matches!(event.meta.source, EventSource::ShredTentative);
        self.health.raw_shred_connected |= matches!(event.meta.source, EventSource::ShredTentative)
            && event
                .meta
                .raw_reference
                .as_ref()
                .map(|reference| {
                    let source_id = reference.source_id.as_str();
                    !source_id.starts_with("mock") && !source_id.starts_with("fixture")
                })
                .unwrap_or(true);
        if matches!(
            event.meta.source,
            EventSource::GeyserProcessed | EventSource::GeyserConfirmed | EventSource::GeyserRooted
        ) {
            self.health.geyser_events_received =
                self.health.geyser_events_received.saturating_add(1);
            self.health.geyser_last_event_time = Some(event.meta.received_at_wall_time);
        }
        if matches!(event.meta.source, EventSource::DeshredTentative) {
            self.health.deshred_events_received =
                self.health.deshred_events_received.saturating_add(1);
            self.health.deshred_provider_status = "connected".to_owned();
        }
        if matches!(
            (&event.meta.source, &event.payload),
            (
                EventSource::DeshredTentative,
                EventPayload::ObservedTransaction(_)
            )
        ) {
            self.health.deshred_transactions_decoded =
                self.health.deshred_transactions_decoded.saturating_add(1);
        }
        self.metrics
            .events_received_total
            .with_label_values(&[
                &format!("{:?}", event.meta.source).to_lowercase(),
                payload_label(&event.payload),
            ])
            .inc();

        if self.edge_collector_mode() {
            self.persist_edge_stream_records(&event)?;
        } else if self.persist_normalized_events {
            let record = self.tag_record(StoredRecord::new(
                DatasetKind::NormalizedEventLog,
                self.resolved.config_hash.clone(),
                self.resolved.idl_hash.clone(),
                Some(
                    self.resolved
                        .loaded
                        .config
                        .environment
                        .strategy_version
                        .clone(),
                ),
                format!("{:?}", event.meta.source).to_lowercase(),
                format!("{:?}", event.meta.canonicality).to_lowercase(),
                Some(event.meta.received_at_wall_time),
                event.clone(),
            ));
            self.storage.append_normalized_event(&record)?;
            if let Some(segment_manager) = self.segment_manager.as_mut() {
                segment_manager.append_json_record(
                    "normalized_events",
                    &record,
                    Some(event.meta.received_at_wall_time),
                )?;
                segment_manager.append_json_record(
                    "source_events",
                    &record,
                    Some(event.meta.received_at_wall_time),
                )?;
            }
            self.health.last_persist_time = Some(unix_now());
        }

        if !tentative {
            if let Some(shred_service) = self.shred_service.as_mut() {
                let _ = shred_service.reconcile_canonical(&event);
            }
        }

        let state_apply_started = Instant::now();
        self.state_engine.apply_event(&event)?;
        self.profile_add_time("state_apply_event", state_apply_started);
        self.profile_inc("token_dirty_set_update", 1);

        if let EventPayload::DataGap(payload) = &event.payload {
            if !tentative {
                self.health.geyser_gap_count = self.health.geyser_gap_count.saturating_add(1);
            }
            self.update_data_gap_state(payload)?;
        }

        if self.edge_collector_mode() {
            self.refresh_health();
            return Ok(());
        }

        let Some(mint) = event.mint().cloned() else {
            self.refresh_health();
            return Ok(());
        };
        self.replay_seen_mints.insert(mint.0.clone());
        self.profile_set_max("unique_mints_seen", self.replay_seen_mints.len() as u64);
        self.profile_set_max("active_tokens", self.state_engine.token_count() as u64);
        let Some(mut token_before_risk) = self.state_engine.token(&mint).cloned() else {
            self.refresh_health();
            return Ok(());
        };
        self.profile_inc("feature_compute_calls", 1);
        let feature_started = Instant::now();
        let (mut features, feature_timings) = self.feature_engine.compute_snapshot_profiled(
            &token_before_risk,
            &self.state_engine,
            event.meta.received_at_wall_time,
        );
        self.profile_add_time("feature_compute", feature_started);
        for (stage, elapsed) in feature_timings {
            *self
                .replay_profile
                .substage_timings_ms
                .entry(stage)
                .or_default() += elapsed;
        }
        self.profile_inc("cohort_recompute_count", 1);
        self.profile_inc("holder_index_recompute_count", 1);
        self.profile_inc("risk_compute_calls", 1);
        let risk_started = Instant::now();
        let mut risk = self.risk_engine.evaluate(
            &token_before_risk,
            &features,
            event.meta.received_at_wall_time,
        );
        self.profile_add_time("risk_compute", risk_started);
        let mut derived_events = Vec::new();
        if tentative && matches!(event.payload, EventPayload::PumpSell(_)) {
            derived_events.extend(self.tentative_sell_manager.detect_tentative_sell(
                &event,
                &token_before_risk,
                &features,
                &risk,
                self.paper_executor.position_context(&mint.0).as_ref(),
            ));
        } else if !tentative {
            derived_events.extend(
                self.tentative_sell_manager
                    .reconcile(&event, self.state_engine.token(&mint)),
            );
        }
        for derived in &derived_events {
            self.apply_derived_runtime_event(derived)?;
        }
        let expired = if self.tentative_sell_manager.has_pending() {
            let snapshot_started = Instant::now();
            let snapshot = self.state_engine.snapshot();
            self.profile_add_time("full_state_scan", snapshot_started);
            self.profile_inc("full_state_scans", 1);
            self.tentative_sell_manager
                .expire(event.meta.received_at_wall_time, &snapshot)
        } else {
            Vec::new()
        };
        for derived in &expired {
            self.apply_derived_runtime_event(derived)?;
        }
        if !derived_events.is_empty() || !expired.is_empty() {
            let Some(updated_token) = self.state_engine.token(&mint).cloned() else {
                self.refresh_health();
                return Ok(());
            };
            token_before_risk = updated_token;
            self.profile_inc("feature_compute_calls", 1);
            let feature_started = Instant::now();
            let (updated_features, feature_timings) =
                self.feature_engine.compute_snapshot_profiled(
                    &token_before_risk,
                    &self.state_engine,
                    event.meta.received_at_wall_time,
                );
            features = updated_features;
            self.profile_add_time("feature_compute", feature_started);
            for (stage, elapsed) in feature_timings {
                *self
                    .replay_profile
                    .substage_timings_ms
                    .entry(stage)
                    .or_default() += elapsed;
            }
            self.profile_inc("cohort_recompute_count", 1);
            self.profile_inc("holder_index_recompute_count", 1);
            self.profile_inc("risk_compute_calls", 1);
            let risk_started = Instant::now();
            risk = self.risk_engine.evaluate(
                &token_before_risk,
                &features,
                event.meta.received_at_wall_time,
            );
            self.profile_add_time("risk_compute", risk_started);
        }
        self.latest_features
            .insert(mint.0.clone(), features.clone());
        self.latest_risk.insert(mint.0.clone(), risk.clone());
        let mut token = token_before_risk;
        if !tentative && self.apply_risk_terminal(&mint, &risk, event.meta.received_at_wall_time)? {
            let Some(updated_token) = self.state_engine.token(&mint).cloned() else {
                self.refresh_health();
                return Ok(());
            };
            token = updated_token;
        };
        self.profile_inc("decision_compute_calls", 1);
        let decision_started = Instant::now();
        let mut decision = self.decision_engine.evaluate(
            &token,
            &features,
            &risk,
            self.paper_executor.position_context(&mint.0).as_ref(),
            &self.resolved.config_hash,
            &self.resolved.loaded.config.environment.strategy_version,
            event.meta.received_at_wall_time,
        );
        self.profile_add_time("decision_compute", decision_started);
        if tentative
            && matches!(
                decision.decision_event.decision,
                TradeDecision::EnterPaper | TradeDecision::EnterLive
            )
        {
            decision.decision_event.decision = TradeDecision::WatchDeep;
            decision
                .decision_event
                .reason_codes
                .push(ReasonCode::TentativeOnly);
        }
        self.apply_shred_defense_annotations(&token, &mut decision);
        self.apply_runtime_safety_for_token(&token, &mut decision);
        decision.decision_event.decision_id =
            self.deterministic_decision_id(&event, &decision.decision_event);
        let decision_label = format!("{:?}", decision.decision_event.decision);
        *self
            .decisions_by_type
            .entry(decision_label.clone())
            .or_default() += 1;
        self.health.last_decision_time = Some(event.meta.received_at_wall_time);
        self.audit(
            RuntimeAuditKind::PaperDecision,
            BTreeMap::from([
                ("mint".to_owned(), mint.0.clone()),
                ("decision".to_owned(), decision_label.clone()),
            ]),
            decision.decision_event.reason_codes.clone(),
        )?;
        let decision_record = self.tag_record(StoredRecord::new(
            DatasetKind::TradeDecisions,
            self.resolved.config_hash.clone(),
            self.resolved.idl_hash.clone(),
            Some(
                self.resolved
                    .loaded
                    .config
                    .environment
                    .strategy_version
                    .clone(),
            ),
            "decision_engine",
            format!("{:?}", event.meta.canonicality).to_lowercase(),
            Some(event.meta.received_at_wall_time),
            decision.decision_event.clone(),
        ));
        let decision_write_started = Instant::now();
        self.storage.append_trade_decision(&decision_record)?;
        if let Some(segment_manager) = self.segment_manager.as_mut() {
            segment_manager.append_json_record(
                "decisions",
                &decision_record,
                decision_record.no_lookahead_timestamp,
            )?;
        }
        self.profile_add_time("decision_write", decision_write_started);
        self.profile_inc("rows_written", 1);
        self.decisions.push(decision.decision_event.clone());
        self.decision_outcomes.push(decision.clone());

        let mut fill_emitted = false;
        if self.safety.paper_allowed {
            let paper_execution_started = Instant::now();
            if let Some(fill) = self.paper_executor.handle_decision(
                &decision,
                &token,
                event.meta.received_at_wall_time,
                force_fail_for_token(&token),
            ) {
                self.audit(
                    RuntimeAuditKind::PaperFill,
                    BTreeMap::from([("mint".to_owned(), fill.mint.0.clone())]),
                    Vec::new(),
                )?;
                let fill_record = self.tag_record(StoredRecord::new(
                    DatasetKind::SimulatedFills,
                    self.resolved.config_hash.clone(),
                    self.resolved.idl_hash.clone(),
                    Some(
                        self.resolved
                            .loaded
                            .config
                            .environment
                            .strategy_version
                            .clone(),
                    ),
                    "paper_executor",
                    format!("{:?}", event.meta.canonicality).to_lowercase(),
                    Some(event.meta.received_at_wall_time),
                    fill.clone(),
                ));
                let fill_write_started = Instant::now();
                self.storage.append_fill(&fill_record)?;
                if let Some(segment_manager) = self.segment_manager.as_mut() {
                    segment_manager.append_json_record(
                        "fills",
                        &fill_record,
                        fill_record.no_lookahead_timestamp,
                    )?;
                }
                self.profile_add_time("fill_write", fill_write_started);
                self.profile_inc("rows_written", 1);
                fill_emitted = true;
            }
            self.profile_add_time("paper_execution", paper_execution_started);
        }

        if self.should_persist_feature_snapshot_for_event(
            &mint.0,
            &event,
            decision.decision_event.decision,
            fill_emitted,
            event.meta.received_at_wall_time,
        ) {
            let feature_record = self.tag_record(StoredRecord::new(
                DatasetKind::TokenFeatureSnapshots,
                self.resolved.config_hash.clone(),
                self.resolved.idl_hash.clone(),
                Some(
                    self.resolved
                        .loaded
                        .config
                        .environment
                        .strategy_version
                        .clone(),
                ),
                "feature_engine",
                format!("{:?}", event.meta.canonicality).to_lowercase(),
                Some(event.meta.received_at_wall_time),
                features.clone(),
            ));
            let dedup_started = Instant::now();
            let identity = self.feature_snapshot_identity(&feature_record);
            let duplicate = !self.emitted_feature_snapshot_identities.insert(identity);
            self.profile_add_time("feature_snapshot_dedup", dedup_started);
            if duplicate {
                self.profile_inc("duplicate_feature_snapshots_suppressed", 1);
            } else {
                let write_started = Instant::now();
                let mut written_path = None;
                if let Some(segment_manager) = self.segment_manager.as_mut() {
                    segment_manager.append_feature_snapshot(&feature_record)?;
                } else {
                    written_path = Some(self.storage.write_feature_snapshot(&feature_record)?);
                }
                self.profile_add_time("feature_snapshot_write", write_started);
                self.profile_inc("feature_snapshots_emitted", 1);
                self.profile_inc("rows_written", 1);
                if let Some(path) = written_path {
                    if path.exists() {
                        if let Ok(bytes) = serde_json::to_vec(&feature_record) {
                            self.profile_inc("bytes_written", bytes.len() as u64 + 1);
                        }
                    }
                }
            }
            self.last_feature_snapshot_at_by_mint
                .insert(mint.0.clone(), event.meta.received_at_wall_time);
        }

        self.refresh_health();
        Ok(())
    }

    fn apply_derived_runtime_event(&mut self, event: &NormalizedEvent) -> Result<()> {
        let state_apply_started = Instant::now();
        self.state_engine.apply_event(event)?;
        self.profile_add_time("state_apply_event", state_apply_started);
        if let EventPayload::DataGap(payload) = &event.payload {
            self.update_data_gap_state(payload)?;
        }
        if self.persist_normalized_events {
            let record = self.tag_record(StoredRecord::new(
                DatasetKind::NormalizedEventLog,
                self.resolved.config_hash.clone(),
                self.resolved.idl_hash.clone(),
                Some(
                    self.resolved
                        .loaded
                        .config
                        .environment
                        .strategy_version
                        .clone(),
                ),
                format!("{:?}", event.meta.source).to_lowercase(),
                format!("{:?}", event.meta.canonicality).to_lowercase(),
                Some(event.meta.received_at_wall_time),
                event.clone(),
            ));
            self.storage.append_normalized_event(&record)?;
            if let Some(segment_manager) = self.segment_manager.as_mut() {
                segment_manager.append_json_record(
                    "normalized_events",
                    &record,
                    Some(event.meta.received_at_wall_time),
                )?;
                segment_manager.append_json_record(
                    "source_events",
                    &record,
                    Some(event.meta.received_at_wall_time),
                )?;
            }
        }
        Ok(())
    }

    fn publish_canonical(&mut self, event: NormalizedEvent) -> Result<()> {
        let publisher = self.canonical_bus.publisher();
        match publisher.try_publish(Priority::High, event.clone()) {
            Ok(()) => Ok(()),
            Err(EventBusError::Full) => {
                self.health.canonical_events_dropped =
                    self.health.canonical_events_dropped.saturating_add(1);
                self.audit(
                    RuntimeAuditKind::QueueOverflow,
                    BTreeMap::from([("queue".to_owned(), "canonical".to_owned())]),
                    vec![ReasonCode::CanonicalQueueOverflow],
                )?;
                let gap = synthetic_gap_event(event.meta.slot, EventSource::GeyserProcessed);
                let state_apply_started = Instant::now();
                self.state_engine.apply_event(&gap)?;
                self.profile_add_time("state_apply_event", state_apply_started);
                self.update_data_gap_state(match &gap.payload {
                    EventPayload::DataGap(payload) => payload,
                    _ => unreachable!("synthetic gap must be a gap event"),
                })?;
                if self.persist_normalized_events {
                    let record = self.tag_record(StoredRecord::new(
                        DatasetKind::NormalizedEventLog,
                        self.resolved.config_hash.clone(),
                        self.resolved.idl_hash.clone(),
                        Some(
                            self.resolved
                                .loaded
                                .config
                                .environment
                                .strategy_version
                                .clone(),
                        ),
                        "runtime",
                        "processed",
                        Some(gap.meta.received_at_wall_time),
                        gap,
                    ));
                    self.storage.append_normalized_event(&record)?;
                }
                Ok(())
            }
            Err(EventBusError::Closed) => {
                Err(RuntimeError::Blocked("canonical queue closed".to_owned()).into())
            }
        }
    }

    fn publish_tentative(&mut self, event: NormalizedEvent) -> Result<()> {
        let publisher = self.tentative_bus.publisher();
        match publisher.try_publish(Priority::Low, event) {
            Ok(()) => Ok(()),
            Err(EventBusError::Full) => {
                self.audit(
                    RuntimeAuditKind::QueueOverflow,
                    BTreeMap::from([("queue".to_owned(), "tentative".to_owned())]),
                    vec![ReasonCode::TentativeQueueOverflow],
                )?;
                Ok(())
            }
            Err(EventBusError::Closed) => {
                Err(RuntimeError::Blocked("tentative queue closed".to_owned()).into())
            }
        }
    }

    fn apply_risk_terminal(
        &mut self,
        mint: &PubkeyValue,
        risk: &RiskAssessment,
        observed_at: OffsetDateTime,
    ) -> Result<bool> {
        let variant = match risk.discard_policy {
            DiscardPolicyDecision::SoftDiscard => Some(TokenTerminalVariant::SoftDiscarded),
            DiscardPolicyDecision::HardDiscard => Some(TokenTerminalVariant::HardDiscarded),
            DiscardPolicyDecision::RugArchive => Some(TokenTerminalVariant::Rugged),
            DiscardPolicyDecision::DataGapStop => Some(TokenTerminalVariant::DataGap),
            DiscardPolicyDecision::Keep | DiscardPolicyDecision::ResearchSample => None,
        };
        let Some(variant) = variant else {
            return Ok(false);
        };
        let event = NormalizedEvent {
            meta: EventMeta {
                event_id: common::EventId::new_v7(),
                source: EventSource::Replay,
                canonicality: Canonicality::Processed,
                slot: 0,
                parent_slot: None,
                block_time: Some(observed_at),
                observed_at_monotonic_ns: monotonic_now_ns(),
                received_at_wall_time: observed_at,
                signature: None,
                transaction_index: None,
                instruction_index: None,
                inner_instruction_index: None,
                account_pubkey: None,
                account_write_version: None,
                source_latency_ms: None,
                decode_confidence: Decimal::ONE,
                data_quality_flags: Vec::new(),
                raw_reference: None,
                schema_version: common::SCHEMA_VERSION,
            },
            payload: EventPayload::TokenTerminal(TokenTerminalEvent {
                mint: mint.clone(),
                variant,
                reason_codes: risk.reason_codes.clone(),
                details: BTreeMap::new(),
            }),
        };
        let state_apply_started = Instant::now();
        self.state_engine.apply_event(&event)?;
        self.profile_add_time("state_apply_event", state_apply_started);
        if self.persist_normalized_events {
            let record = self.tag_record(StoredRecord::new(
                DatasetKind::NormalizedEventLog,
                self.resolved.config_hash.clone(),
                self.resolved.idl_hash.clone(),
                Some(
                    self.resolved
                        .loaded
                        .config
                        .environment
                        .strategy_version
                        .clone(),
                ),
                "risk_engine",
                "processed",
                Some(observed_at),
                event,
            ));
            self.storage.append_normalized_event(&record)?;
        }
        Ok(true)
    }

    async fn finalize_run(&mut self) -> Result<()> {
        let latest_observed = self
            .state_engine
            .snapshot()
            .tokens
            .values()
            .filter_map(|token| {
                token
                    .trade_stats
                    .price_history
                    .back()
                    .map(|(time, _)| *time)
            })
            .max()
            .unwrap_or_else(unix_now);
        let expire_at = latest_observed
            + Duration::milliseconds(
                self.resolved
                    .loaded
                    .config
                    .shred_exit
                    .tentative_reconciliation_ttl_ms as i64,
            );
        let expired = self
            .tentative_sell_manager
            .expire(expire_at, &self.state_engine.snapshot());
        for event in expired {
            self.apply_derived_runtime_event(&event)?;
        }
        if self.liquidate_on_finalize {
            let snapshot = self.state_engine.snapshot();
            let fills = self.paper_executor.liquidate_all(
                &snapshot,
                latest_observed,
                "scenario_end_liquidation",
            );
            for fill in fills {
                self.audit(
                    RuntimeAuditKind::PaperFill,
                    BTreeMap::from([
                        ("mint".to_owned(), fill.mint.0.clone()),
                        ("reason".to_owned(), "scenario_end_liquidation".to_owned()),
                    ]),
                    Vec::new(),
                )?;
                let fill_record = self.tag_record(StoredRecord::new(
                    DatasetKind::SimulatedFills,
                    self.resolved.config_hash.clone(),
                    self.resolved.idl_hash.clone(),
                    Some(
                        self.resolved
                            .loaded
                            .config
                            .environment
                            .strategy_version
                            .clone(),
                    ),
                    "paper_executor",
                    "processed",
                    Some(latest_observed),
                    fill,
                ));
                self.storage.append_fill(&fill_record)?;
                if let Some(segment_manager) = self.segment_manager.as_mut() {
                    segment_manager.append_json_record(
                        "fills",
                        &fill_record,
                        fill_record.no_lookahead_timestamp,
                    )?;
                }
            }
        }
        for token in self.state_engine.snapshot().tokens.values() {
            let summary = state::CompactTokenSummary {
                mint: token.mint.0.clone(),
                lifecycle: token.lifecycle,
                latest_price: token.latest_price,
                holder_count: token.holder_state.nonzero_holder_count,
                top1_holder_pct: token.holder_state.top_holder_pct(1),
                creator_sold_pct: token.developer_state.creator_sell_percentage,
                canonical_trade_count: token.trade_stats.buy_count + token.trade_stats.sell_count,
                reason_codes: self
                    .latest_risk
                    .get(&token.mint.0)
                    .map(|risk| risk.reason_codes.clone())
                    .unwrap_or_default(),
            };
            let record = self.tag_record(StoredRecord::new(
                DatasetKind::TokenSummary,
                self.resolved.config_hash.clone(),
                self.resolved.idl_hash.clone(),
                Some(
                    self.resolved
                        .loaded
                        .config
                        .environment
                        .strategy_version
                        .clone(),
                ),
                "state",
                "snapshot",
                token.launch_time,
                summary,
            ));
            let _ = self.storage.write_token_summary(&record)?;
        }
        self.tentative_sell_manager
            .persist_calibration(&self.resolved.loaded.config)?;
        self.storage.finalize_feature_snapshot_chunks_for_run(
            &self.resolved.loaded.config.environment.run_id,
            self.current_scenario_id.as_deref(),
        )?;
        self.refresh_health();
        Ok(())
    }

    fn refresh_health(&mut self) {
        let (active_tokens, discarded_tokens, rugged_tokens) =
            if let Some(snapshot) = self.aggregated_snapshot.as_ref() {
                (
                    snapshot.tokens.len(),
                    snapshot.discarded_summaries.len(),
                    snapshot
                        .tokens
                        .values()
                        .filter(|token| token.lifecycle == TokenLifecycle::RugArchive)
                        .count(),
                )
            } else {
                (
                    self.state_engine.token_count(),
                    self.state_engine.discarded_token_count(),
                    self.state_engine.rugged_token_count(),
                )
            };
        let budget = self.rpc_budget.summary();
        let stream_only = self.resolved.loaded.stream_only_validation_summary();
        let aggregate_mode = self.aggregated_snapshot.is_some();
        self.health.active_tokens = active_tokens;
        self.health.active_positions = self.paper_executor.positions().len();
        self.health.paper_pnl = self.paper_executor.ledger().closed_pnl;
        self.health.realized_pnl = self.paper_executor.ledger().closed_pnl;
        self.health.fee_drag = self.paper_executor.ledger().fee_drag;
        self.health.slippage_drag = self.paper_executor.ledger().slippage_drag;
        self.health.latency_drag = self.paper_executor.ledger().latency_drag;
        self.health.rpc_budget_healthy = budget.daily_used <= budget.daily_limit;
        self.health.stream_only_enabled = stream_only.stream_only_enabled;
        self.health.stream_only_passed = stream_only.passed;
        self.health.rpc_network_calls_total = budget.network_calls_total;
        self.health.rpc_credits_used_total = budget.daily_used;
        self.health.rpc_denials_total = budget.denied_entries;
        self.health.market_data_rpc_calls_allowed = budget.market_data_rpc_calls_allowed;
        self.health.holder_rpc_calls_allowed = budget.holder_rpc_calls_allowed;
        self.health.metadata_fetch_allowed = budget.metadata_fetch_allowed;
        self.health.confirmation_rpc_allowed = budget.confirmation_rpc_allowed;
        self.health.blockhash_rpc_allowed = budget.blockhash_rpc_allowed;
        self.health.data_gap_active = self.active_data_gaps.iter().any(|gap| gap.active);
        self.health.data_gap_scope = self
            .active_data_gaps
            .iter()
            .find(|gap| gap.active && gap.trade_blocking)
            .map(|gap| format!("{:?}", gap.scope));
        self.health.runtime_uptime_ms =
            (unix_now() - self.started_at).whole_milliseconds().max(0) as u64;
        self.health.discarded_tokens = discarded_tokens;
        self.health.rugged_tokens = rugged_tokens;
        self.health.early_intent_enabled = self.resolved.loaded.config.early_intent.enabled;
        let source_metrics = self.tentative_sell_manager.source_metrics();
        self.health.deshred_tentative_sells_total = source_metrics
            .get("deshredpreexecution")
            .map(|metrics| metrics.tentative_sell_total)
            .unwrap_or_default();
        self.health.deshred_malicious_warnings_total = source_metrics
            .get("deshredpreexecution")
            .map(|metrics| metrics.malicious_sell_warning_total)
            .unwrap_or_default();
        self.health.deshred_emergency_exits_armed_total = source_metrics
            .get("deshredpreexecution")
            .map(|metrics| metrics.emergency_exits_armed_total)
            .unwrap_or_default();
        self.health.deshred_emergency_exits_triggered_total = source_metrics
            .get("deshredpreexecution")
            .map(|metrics| metrics.emergency_exits_triggered_total)
            .unwrap_or_default();
        self.health.deshred_confirmed_executed_total = source_metrics
            .get("deshredpreexecution")
            .map(|metrics| metrics.confirmed_executed_total)
            .unwrap_or_default();
        self.health.deshred_false_positive_total = source_metrics
            .get("deshredpreexecution")
            .map(|metrics| metrics.false_positive_exit_total)
            .unwrap_or_default();
        self.health.deshred_saved_loss_quote_total = source_metrics
            .get("deshredpreexecution")
            .map(|metrics| metrics.saved_loss_quote_total)
            .unwrap_or(Decimal::ZERO);
        self.health.deshred_opportunity_cost_quote_total = source_metrics
            .get("deshredpreexecution")
            .map(|metrics| metrics.opportunity_cost_quote_total)
            .unwrap_or(Decimal::ZERO);
        self.health.raw_shred_tentative_sells_total = source_metrics
            .get("rawshred")
            .map(|metrics| metrics.tentative_sell_total)
            .unwrap_or_default();
        self.health.mock_early_intent_tentative_sells_total = source_metrics
            .get("mockearlyintent")
            .map(|metrics| metrics.tentative_sell_total)
            .unwrap_or_default();
        self.health.fixture_early_intent_tentative_sells_total = source_metrics
            .get("fixturetentative")
            .map(|metrics| metrics.tentative_sell_total)
            .unwrap_or_default();
        self.health.replay_early_intent_tentative_sells_total = source_metrics
            .get("replaytentative")
            .map(|metrics| metrics.tentative_sell_total)
            .unwrap_or_default();
        self.health.early_intent_dedup_pairs = self.tentative_sell_manager.dedup_pairs().clone();
        self.health.source_dedup_count =
            self.health.early_intent_dedup_pairs.values().copied().sum();
        self.health.early_intent_sources_active = source_metrics
            .iter()
            .filter(|(_, metrics)| metrics.tentative_sell_total > 0)
            .map(|(source, _)| source.clone())
            .collect();
        if !aggregate_mode {
            let shred_metrics = self.tentative_sell_manager.metrics();
            self.health.shred_tentative_sells_total = shred_metrics.tentative_sell_total;
            self.health.shred_malicious_sell_warnings_total =
                shred_metrics.malicious_sell_warning_total;
            self.health.shred_emergency_exits_armed_total =
                shred_metrics.emergency_exits_armed_total;
            self.health.shred_emergency_exits_triggered_total =
                shred_metrics.emergency_exits_triggered_total;
            self.health.shred_confirmed_executed_total = shred_metrics.confirmed_executed_total;
            self.health.shred_confirmed_failed_total = shred_metrics.confirmed_failed_total;
            self.health.shred_not_seen_within_ttl_total = shred_metrics.not_seen_within_ttl_total;
            self.health.shred_reorged_total = shred_metrics.reorged_total;
            self.health.shred_decode_mismatch_total = shred_metrics.decode_mismatch_total;
            self.health.shred_account_effect_confirmed_total =
                shred_metrics.account_effect_confirmed_total;
            self.health.shred_sell_false_positive_total = shred_metrics.false_positive_exit_total;
            self.health.shred_saved_loss_quote_total = shred_metrics.saved_loss_quote_total;
            self.health.shred_opportunity_cost_quote_total =
                shred_metrics.opportunity_cost_quote_total;
        }
    }

    fn sync_http_snapshot(&self, state: &Arc<Mutex<RuntimeHttpSnapshot>>) {
        if let Ok(mut guard) = state.lock() {
            guard.health = self.health.clone();
            guard.safety = self.safety.clone();
            guard.updated_at = unix_now();
        }
    }

    fn summary(&self) -> RuntimeSummary {
        RuntimeSummary {
            run_id: self.resolved.loaded.config.environment.run_id.clone(),
            source_run_id: self.source_run_id.clone(),
            scenario_id: self.current_scenario_id.clone(),
            config_hash: self.resolved.config_hash.clone(),
            idl_hash: self.resolved.idl_hash.clone(),
            mode: self.resolved.mode,
            health: self.health.clone(),
            safety: self.safety.clone(),
            decisions_by_type: self.decisions_by_type.clone(),
            fills: self.paper_executor.ledger().fills.clone(),
            decision_events: self.decisions.clone(),
            decision_outcomes: self.decision_outcomes.clone(),
            latest_features: self.latest_features.clone(),
            latest_risk: self.latest_risk.clone(),
            snapshot: self.current_snapshot(),
            audits: self.audits.clone(),
            replay_profile: self.replay_profile.clone(),
        }
    }

    fn reset_for_next_run(&mut self) {
        self.state_engine = StateEngine::new(self.resolved.loaded.config.ttl.clone());
        self.paper_executor = PaperExecutor::new(Simulator::new(FeeModel::default()));
        self.tentative_sell_manager = TentativeSellManager::new(&self.resolved.loaded.config);
        self.latest_features.clear();
        self.latest_risk.clear();
        self.decisions.clear();
        self.decision_outcomes.clear();
        self.decisions_by_type.clear();
        self.active_data_gaps.clear();
        self.aggregated_snapshot = None;
        self.audits.clear();
        self.replay_profile = RuntimeReplayProfile::default();
        self.emitted_feature_snapshot_identities.clear();
        self.replay_seen_mints.clear();
        self.health.data_gap_active = false;
        self.health.data_gap_scope = None;
        self.health.paper_pnl = Decimal::ZERO;
        self.health.realized_pnl = Decimal::ZERO;
        self.health.fee_drag = Decimal::ZERO;
        self.health.slippage_drag = Decimal::ZERO;
        self.health.latency_drag = Decimal::ZERO;
        self.health.events_processed = 0;
        self.health.canonical_events_dropped = 0;
        self.health.discarded_tokens = 0;
        self.health.rugged_tokens = 0;
        self.health.early_intent_enabled = self.resolved.loaded.config.early_intent.enabled;
        self.health.early_intent_sources_active.clear();
        self.health.deshred_provider_status = "not_requested".to_owned();
        self.health.deshred_events_received = 0;
        self.health.deshred_transactions_decoded = 0;
        self.health.deshred_tentative_sells_total = 0;
        self.health.deshred_malicious_warnings_total = 0;
        self.health.deshred_emergency_exits_armed_total = 0;
        self.health.deshred_emergency_exits_triggered_total = 0;
        self.health.deshred_confirmed_executed_total = 0;
        self.health.deshred_false_positive_total = 0;
        self.health.deshred_saved_loss_quote_total = Decimal::ZERO;
        self.health.deshred_opportunity_cost_quote_total = Decimal::ZERO;
        self.health.raw_shred_connected = false;
        self.health.raw_shred_tentative_sells_total = 0;
        self.health.mock_early_intent_active = false;
        self.health.mock_early_intent_tentative_sells_total = 0;
        self.health.fixture_early_intent_tentative_sells_total = 0;
        self.health.replay_early_intent_tentative_sells_total = 0;
        self.health.source_dedup_count = 0;
        self.health.early_intent_dedup_pairs.clear();
        self.safety.reason_codes.clear();
        self.safety.trade_allowed = !self.resolved.loaded.config.live.enabled;
        self.safety.data_quality_allowed = true;
        self.source_run_id = None;
        self.current_scenario_id = None;
        self.record_sequence.set(0);
        self.calibration_snapshot_hash = self
            .tentative_sell_manager
            .ensure_calibration_snapshot(&self.resolved.loaded.config)
            .map(|(hash, path)| {
                self.calibration_snapshot_path = Some(path.display().to_string());
                hash
            })
            .ok()
            .or_else(|| {
                self.tentative_sell_manager
                    .calibration()
                    .persisted_version_hash
                    .clone()
            });
        self.started_at = unix_now();
    }

    fn reset_aggregate(&mut self) {
        self.reset_for_next_run();
    }

    fn merge_summary(&mut self, summary: RuntimeSummary) {
        self.health.events_processed += summary.health.events_processed;
        self.health.canonical_events_dropped += summary.health.canonical_events_dropped;
        for (decision, count) in summary.decisions_by_type {
            *self.decisions_by_type.entry(decision).or_default() += count;
        }
        self.decisions.extend(summary.decision_events);
        self.decision_outcomes.extend(summary.decision_outcomes);
        self.latest_features.extend(summary.latest_features);
        self.latest_risk.extend(summary.latest_risk);
        self.audits.extend(summary.audits);
        self.source_run_id = summary.source_run_id;
        self.current_scenario_id = None;
        self.paper_executor
            .merge_ledger(summary.fills, summary.health.paper_pnl);
        self.active_data_gaps.extend(
            summary
                .health
                .data_gap_scope
                .as_ref()
                .map(|scope| ScopedDataGap {
                    scope: parse_gap_scope(scope),
                    source: EventSource::Replay,
                    start_slot: 0,
                    end_slot: None,
                    severity: common::GapSeverity::High,
                    active: summary.health.data_gap_active,
                    trade_blocking: summary.health.data_gap_active,
                    affected_token: None,
                    run_id: self.resolved.loaded.config.environment.run_id.clone(),
                    scenario_id: summary.scenario_id.clone(),
                    reason_codes: summary.safety.reason_codes.clone(),
                    recovery_action: "merged_summary".to_owned(),
                })
                .into_iter(),
        );
        self.aggregated_snapshot = Some(merge_snapshots(
            self.aggregated_snapshot.take(),
            summary.snapshot,
        ));
        self.health.paper_pnl += summary.health.paper_pnl;
        self.health.deshred_events_received += summary.health.deshred_events_received;
        self.health.deshred_transactions_decoded += summary.health.deshred_transactions_decoded;
        self.health.deshred_tentative_sells_total += summary.health.deshred_tentative_sells_total;
        self.health.deshred_malicious_warnings_total +=
            summary.health.deshred_malicious_warnings_total;
        self.health.deshred_emergency_exits_armed_total +=
            summary.health.deshred_emergency_exits_armed_total;
        self.health.deshred_emergency_exits_triggered_total +=
            summary.health.deshred_emergency_exits_triggered_total;
        self.health.deshred_confirmed_executed_total +=
            summary.health.deshred_confirmed_executed_total;
        self.health.deshred_false_positive_total += summary.health.deshred_false_positive_total;
        self.health.deshred_saved_loss_quote_total += summary.health.deshred_saved_loss_quote_total;
        self.health.deshred_opportunity_cost_quote_total +=
            summary.health.deshred_opportunity_cost_quote_total;
        self.health.raw_shred_tentative_sells_total +=
            summary.health.raw_shred_tentative_sells_total;
        self.health.mock_early_intent_tentative_sells_total +=
            summary.health.mock_early_intent_tentative_sells_total;
        self.health.fixture_early_intent_tentative_sells_total +=
            summary.health.fixture_early_intent_tentative_sells_total;
        self.health.replay_early_intent_tentative_sells_total +=
            summary.health.replay_early_intent_tentative_sells_total;
        self.health.source_dedup_count += summary.health.source_dedup_count;
        for (key, value) in summary.health.early_intent_dedup_pairs {
            *self.health.early_intent_dedup_pairs.entry(key).or_default() += value;
        }
        self.health.shred_tentative_sells_total += summary.health.shred_tentative_sells_total;
        self.health.shred_malicious_sell_warnings_total +=
            summary.health.shred_malicious_sell_warnings_total;
        self.health.shred_emergency_exits_armed_total +=
            summary.health.shred_emergency_exits_armed_total;
        self.health.shred_emergency_exits_triggered_total +=
            summary.health.shred_emergency_exits_triggered_total;
        self.health.shred_confirmed_executed_total += summary.health.shred_confirmed_executed_total;
        self.health.shred_confirmed_failed_total += summary.health.shred_confirmed_failed_total;
        self.health.shred_not_seen_within_ttl_total +=
            summary.health.shred_not_seen_within_ttl_total;
        self.health.shred_reorged_total += summary.health.shred_reorged_total;
        self.health.shred_decode_mismatch_total += summary.health.shred_decode_mismatch_total;
        self.health.shred_account_effect_confirmed_total +=
            summary.health.shred_account_effect_confirmed_total;
        self.health.shred_sell_false_positive_total +=
            summary.health.shred_sell_false_positive_total;
        self.health.shred_saved_loss_quote_total += summary.health.shred_saved_loss_quote_total;
        self.health.shred_opportunity_cost_quote_total +=
            summary.health.shred_opportunity_cost_quote_total;
        self.health.last_event_time =
            max_time(self.health.last_event_time, summary.health.last_event_time);
        self.health.last_decision_time = max_time(
            self.health.last_decision_time,
            summary.health.last_decision_time,
        );
        self.health.last_persist_time = max_time(
            self.health.last_persist_time,
            summary.health.last_persist_time,
        );
        for source in summary.health.early_intent_sources_active {
            if !self.health.early_intent_sources_active.contains(&source) {
                self.health.early_intent_sources_active.push(source);
            }
        }
        self.safety.trade_allowed &= summary.safety.trade_allowed;
        self.safety.paper_allowed &= summary.safety.paper_allowed;
        self.safety.live_allowed &= summary.safety.live_allowed;
        self.safety.data_quality_allowed &= summary.safety.data_quality_allowed;
        self.safety.rpc_budget_allowed &= summary.safety.rpc_budget_allowed;
        self.safety.max_loss_allowed &= summary.safety.max_loss_allowed;
        self.safety.stale_data_allowed &= summary.safety.stale_data_allowed;
        self.safety.reason_codes.extend(summary.safety.reason_codes);
        for (stage, elapsed) in summary.replay_profile.substage_timings_ms {
            *self
                .replay_profile
                .substage_timings_ms
                .entry(stage)
                .or_default() += elapsed;
        }
        for (counter, value) in summary.replay_profile.counters {
            let entry = self
                .replay_profile
                .counters
                .entry(counter.clone())
                .or_default();
            if matches!(counter.as_str(), "unique_mints_seen" | "active_tokens") {
                *entry = (*entry).max(value);
            } else {
                *entry += value;
            }
        }
    }

    fn update_data_gap_state(&mut self, payload: &common::DataGapEvent) -> Result<()> {
        let scoped_gaps = scoped_gaps_from_event(
            payload,
            &self.resolved.loaded.config.environment.run_id,
            self.current_scenario_id.clone(),
        );
        for gap in scoped_gaps {
            if gap.active {
                self.audit(
                    RuntimeAuditKind::DataGapStarted,
                    BTreeMap::from([
                        ("scope".to_owned(), format!("{:?}", gap.scope)),
                        (
                            "token".to_owned(),
                            gap.affected_token
                                .clone()
                                .unwrap_or_else(|| "_none".to_owned()),
                        ),
                    ]),
                    gap.reason_codes.clone(),
                )?;
                self.active_data_gaps.push(gap);
            } else {
                self.active_data_gaps.retain(|existing| {
                    !(existing.scope == gap.scope
                        && existing.source == gap.source
                        && existing.affected_token == gap.affected_token
                        && existing.scenario_id == gap.scenario_id)
                });
                self.audit(
                    RuntimeAuditKind::DataGapRecovered,
                    BTreeMap::from([("scope".to_owned(), format!("{:?}", gap.scope))]),
                    gap.reason_codes.clone(),
                )?;
            }
        }
        self.recompute_safety();
        Ok(())
    }

    fn apply_runtime_safety_for_token(
        &self,
        token: &state::TokenState,
        decision: &mut DecisionOutcome,
    ) {
        let (trade_allowed, reasons) = self.token_trade_allowed(&token.mint.0);
        if !trade_allowed {
            decision.decision_event.reason_codes.extend(reasons.clone());
            decision.decision_event.decision = if token.lifecycle == TokenLifecycle::InPosition {
                TradeDecision::EmergencyExit
            } else {
                TradeDecision::StopTracking
            };
        }
    }

    fn apply_shred_defense_annotations(
        &self,
        token: &state::TokenState,
        decision: &mut DecisionOutcome,
    ) {
        if token.shred_defense.shred_emergency_exit_triggered_flag {
            let active_tracking = token.shred_defense.pending_tentative_sells.values().next();
            decision
                .decision_event
                .reason_codes
                .push(ReasonCode::ShredEmergencyExit);
            if let Some(event_id) = &token.shred_defense.active_triggered_exit_event_id {
                decision
                    .diagnostics
                    .push(format!("trigger_event_id={event_id}"));
            }
            if let Some(seller) = &token.shred_defense.active_dangerous_seller {
                decision
                    .diagnostics
                    .push(format!("malicious_sell_seller={seller}"));
            }
            if let Some(classification) = token.shred_defense.active_seller_classification {
                decision.diagnostics.push(format!(
                    "malicious_sell_classification={}",
                    format!("{classification:?}").to_lowercase()
                ));
            }
            decision.diagnostics.push(format!(
                "expected_exit_price={}",
                active_tracking
                    .map(|tracking| tracking.warning_price)
                    .unwrap_or(token.latest_price)
            ));
            decision.diagnostics.push(format!(
                "estimated_saved_loss_quote={}",
                token.shred_defense.shred_saved_loss_estimate
            ));
            decision.diagnostics.push(format!(
                "realized_loss_saved_quote={}",
                token.shred_defense.shred_saved_loss_realized
            ));
            decision.diagnostics.push(format!(
                "opportunity_cost_if_false_positive={}",
                token.shred_defense.shred_exit_opportunity_cost
            ));
            if let Some(latency) = token.shred_defense.shred_to_geyser_processed_ms {
                decision.diagnostics.push(format!(
                    "early_intent_to_geyser_processed_latency_ms={latency}"
                ));
            }
            if let Some(latency) = token.shred_defense.shred_to_account_effect_confirmation_ms {
                decision.diagnostics.push(format!(
                    "early_intent_to_account_effect_latency_ms={latency}"
                ));
            }
            if let Some(latency) = token.shred_defense.shred_to_rooted_confirmation_ms {
                decision
                    .diagnostics
                    .push(format!("early_intent_to_rooted_latency_ms={latency}"));
            }
            if let Some(signature) = active_tracking.and_then(|tracking| tracking.signature.clone())
            {
                decision
                    .diagnostics
                    .push(format!("malicious_sell_signature={signature}"));
            }
            decision
                .diagnostics
                .push("exit_by=shred_emergency_exit".to_owned());
        } else if token.shred_defense.shred_exit_armed_flag {
            decision
                .decision_event
                .reason_codes
                .push(ReasonCode::ShredExitArmed);
        } else if token.shred_defense.last_warning_level.is_some() {
            decision
                .decision_event
                .reason_codes
                .push(ReasonCode::EarlyIntentSellWarning);
        }
        if token.shred_defense.shred_signal_stale_flag {
            decision
                .decision_event
                .reason_codes
                .push(ReasonCode::ShredSignalStale);
        }
    }

    fn token_trade_allowed(&self, mint: &str) -> (bool, Vec<ReasonCode>) {
        let mut reasons = Vec::new();
        let global_block = self.active_data_gaps.iter().any(|gap| {
            gap.active && gap.trade_blocking && !matches!(gap.scope, DataGapScope::Token)
        });
        if global_block {
            reasons.push(ReasonCode::DataGapActive);
            return (false, reasons);
        }
        let token_block = self.active_data_gaps.iter().any(|gap| {
            gap.active
                && gap.trade_blocking
                && matches!(gap.scope, DataGapScope::Token)
                && gap.affected_token.as_deref() == Some(mint)
        });
        if token_block {
            reasons.push(ReasonCode::DataGapActive);
            return (false, reasons);
        }
        (self.safety.trade_allowed, self.safety.reason_codes.clone())
    }

    fn recompute_safety(&mut self) {
        let global_block = self.active_data_gaps.iter().any(|gap| {
            gap.active && gap.trade_blocking && !matches!(gap.scope, DataGapScope::Token)
        });
        self.safety.trade_allowed = !self.resolved.loaded.config.live.enabled && !global_block;
        self.safety.data_quality_allowed = !global_block;
        self.safety
            .reason_codes
            .retain(|reason| *reason != ReasonCode::DataGapActive);
        if global_block {
            self.safety.reason_codes.push(ReasonCode::DataGapActive);
        }
        self.refresh_health();
    }

    fn audit(
        &mut self,
        kind: RuntimeAuditKind,
        details: BTreeMap<String, String>,
        reason_codes: Vec<ReasonCode>,
    ) -> Result<()> {
        let event = RuntimeAuditEvent {
            at: unix_now(),
            kind,
            run_id: self.resolved.loaded.config.environment.run_id.clone(),
            details,
            reason_codes,
        };
        let record = self.tag_record(StoredRecord::new(
            DatasetKind::RuntimeAuditLog,
            self.resolved.config_hash.clone(),
            self.resolved.idl_hash.clone(),
            Some(
                self.resolved
                    .loaded
                    .config
                    .environment
                    .strategy_version
                    .clone(),
            ),
            "runtime",
            "audit",
            Some(event.at),
            event.clone(),
        ));
        self.storage.append_runtime_audit(&record)?;
        if let Some(segment_manager) = self.segment_manager.as_mut() {
            segment_manager.append_json_record("runtime_audit", &record, Some(event.at))?;
        }
        self.audits.push(event);
        Ok(())
    }

    fn current_run_kind(&self) -> RunKind {
        let run_id = self.resolved.loaded.config.environment.run_id.as_str();
        if run_id.starts_with("fixture-suite-") {
            RunKind::StandardFixtureSuite
        } else if run_id.starts_with("shred-exit-suite-") {
            RunKind::ShredExitFixtureSuite
        } else if run_id.starts_with("multisource-early-intent-suite-") {
            RunKind::MultisourceEarlyIntentSuite
        } else if run_id.starts_with("deshred-smoke-") {
            RunKind::DeshredSmoke
        } else if run_id.starts_with("fixture-") {
            RunKind::FixtureScenario
        } else if run_id.starts_with("paper-replay-") {
            RunKind::PaperReplay
        } else if run_id.starts_with("live-paper-") {
            if self.health.live_source_mode.starts_with("mock_geyser") {
                if self.health.live_source_mode.contains("mock_deshred") {
                    RunKind::MockedLivePaperDeshred
                } else if self.health.shred_tentative_sells_total > 0 {
                    RunKind::MockedLivePaperEarlyIntent
                } else {
                    RunKind::MockedLivePaper
                }
            } else if self.health.live_source_mode == "real_geyser+deshred" {
                RunKind::RealLivePaperDeshred
            } else {
                RunKind::RealLivePaper
            }
        } else if run_id.starts_with("backtest-shred-exit-") {
            RunKind::ShredExitBacktest
        } else if run_id.starts_with("backtest-") {
            RunKind::Backtest
        } else if run_id.starts_with("report-") {
            RunKind::Report
        } else if run_id.starts_with("replay-equivalence-") {
            RunKind::ReplayEquivalence
        } else if run_id.starts_with("calibration-update-") {
            RunKind::CalibrationUpdate
        } else if run_id.starts_with("export-") {
            RunKind::Export
        } else {
            RunKind::Unknown
        }
    }

    fn persist_run_metadata(&mut self, status: RunStatus, notes: Vec<String>) -> Result<()> {
        let metadata = RunMetadata {
            run_id: self.resolved.loaded.config.environment.run_id.clone(),
            parent_run_id: self.source_run_id.clone(),
            source_run_id: self.source_run_id.clone().or_else(|| {
                self.current_run_kind()
                    .is_source_run()
                    .then(|| self.resolved.loaded.config.environment.run_id.clone())
            }),
            scenario_id: self.current_scenario_id.clone(),
            mode: format!("{:?}", self.resolved.mode).to_lowercase(),
            run_kind: self.current_run_kind(),
            run_role: self.current_run_kind().role(),
            created_at_wall_time: self.started_at,
            completed_at_wall_time: matches!(status, RunStatus::Running)
                .then_some(self.started_at)
                .or(Some(unix_now())),
            config_hash: self.resolved.config_hash.clone(),
            idl_hash: self.resolved.idl_hash.clone(),
            event_count: self.health.events_processed,
            decision_count: self.decisions.len() as u64,
            fill_count: self.paper_executor.ledger().fills.len() as u64,
            paper_pnl: self.paper_executor.ledger().closed_pnl,
            contains_tentative_sell_events: self.health.shred_tentative_sells_total > 0,
            contains_shred_exit_defense_events: self.health.shred_tentative_sells_total > 0
                || self.health.shred_malicious_sell_warnings_total > 0
                || self.health.shred_emergency_exits_triggered_total > 0
                || self.health.shred_emergency_exits_armed_total > 0,
            tentative_sell_count: self.health.shred_tentative_sells_total,
            emergency_exit_count: self.health.shred_emergency_exits_triggered_total,
            saved_loss_total: self.health.shred_saved_loss_quote_total,
            opportunity_cost_total: self.health.shred_opportunity_cost_quote_total,
            false_positive_count: self.health.shred_sell_false_positive_total,
            decode_mismatch_count: self.health.shred_decode_mismatch_total,
            reorg_count: self.health.shred_reorged_total,
            not_seen_count: self.health.shred_not_seen_within_ttl_total,
            status,
            report_dir: self
                .storage
                .layout()
                .report_dir
                .join(&self.resolved.loaded.config.environment.run_id)
                .display()
                .to_string(),
            notes,
            calibration_snapshot_hash: self.calibration_snapshot_hash.clone(),
            calibration_snapshot_path: self.calibration_snapshot_path.clone(),
            post_run_calibration_hash: self
                .tentative_sell_manager
                .calibration()
                .persisted_version_hash
                .clone(),
        };
        let record = self.tag_record(StoredRecord::new(
            DatasetKind::RunMetadata,
            self.resolved.config_hash.clone(),
            self.resolved.idl_hash.clone(),
            Some(
                self.resolved
                    .loaded
                    .config
                    .environment
                    .strategy_version
                    .clone(),
            ),
            "runtime",
            "metadata",
            Some(unix_now()),
            metadata,
        ));
        self.storage.append_run_metadata(&record)?;
        Ok(())
    }

    fn source_health_notes(&self) -> Vec<String> {
        let budget = self.rpc_budget.summary();
        let mut notes = vec![
            format!("stream_only_enabled={}", self.health.stream_only_enabled),
            format!("stream_only_passed={}", self.health.stream_only_passed),
            format!(
                "rpc_network_calls_total={}",
                self.health.rpc_network_calls_total
            ),
            format!(
                "rpc_credits_used_total={}",
                self.health.rpc_credits_used_total
            ),
            format!("rpc_denials_total={}", self.health.rpc_denials_total),
            format!(
                "market_data_rpc_calls_allowed={}",
                self.health.market_data_rpc_calls_allowed
            ),
            format!(
                "holder_rpc_calls_allowed={}",
                self.health.holder_rpc_calls_allowed
            ),
            format!(
                "metadata_fetch_allowed={}",
                self.health.metadata_fetch_allowed
            ),
            format!(
                "confirmation_rpc_allowed={}",
                self.health.confirmation_rpc_allowed
            ),
            format!(
                "blockhash_rpc_allowed={}",
                self.health.blockhash_rpc_allowed
            ),
            format!("rpc_hot_path_enabled={}", budget.rpc_hot_path_enabled),
            format!("deshred_supported={}", self.health.deshred_supported),
            format!("deshred_connected={}", self.health.deshred_connected),
            format!(
                "deshred_provider_status={}",
                self.health.deshred_provider_status
            ),
            format!(
                "deshred_events_received={}",
                self.health.deshred_events_received
            ),
            format!(
                "deshred_tentative_sells_total={}",
                self.health.deshred_tentative_sells_total
            ),
            format!(
                "deshred_malicious_warnings_total={}",
                self.health.deshred_malicious_warnings_total
            ),
            format!(
                "deshred_emergency_exits_armed_total={}",
                self.health.deshred_emergency_exits_armed_total
            ),
            format!(
                "deshred_emergency_exits_triggered_total={}",
                self.health.deshred_emergency_exits_triggered_total
            ),
            format!(
                "deshred_confirmed_executed_total={}",
                self.health.deshred_confirmed_executed_total
            ),
            format!(
                "deshred_false_positive_total={}",
                self.health.deshred_false_positive_total
            ),
            format!(
                "deshred_saved_loss_quote_total={}",
                self.health.deshred_saved_loss_quote_total
            ),
            format!(
                "deshred_opportunity_cost_quote_total={}",
                self.health.deshred_opportunity_cost_quote_total
            ),
            format!("raw_shred_supported={}", self.health.raw_shred_supported),
            format!("raw_shred_connected={}", self.health.raw_shred_connected),
            format!(
                "mock_early_intent_active={}",
                self.health.mock_early_intent_active
            ),
            format!("source_dedup_count={}", self.health.source_dedup_count),
            format!(
                "skipped_feature_snapshot_count={}",
                self.health.skipped_feature_snapshot_count
            ),
        ];
        if !self.health.early_intent_sources_active.is_empty() {
            notes.push(format!(
                "early_intent_sources_active={}",
                self.health.early_intent_sources_active.join("|")
            ));
        }
        for (pair, count) in &self.health.early_intent_dedup_pairs {
            notes.push(format!("early_intent_dedup_pair={pair}:{count}"));
        }
        notes
    }

    fn feature_snapshot_pressure_interval_ms(&self) -> Option<u64> {
        let config = &self.resolved.loaded.config.features.pressure;
        if !config.enabled {
            return None;
        }
        let Some(segment_manager) = self.segment_manager.as_ref() else {
            return None;
        };
        if segment_manager.should_pause_feature_snapshot_writes() {
            return Some(config.min_snapshot_interval_ms_upload_backlog.max(1));
        }
        if segment_manager.is_disk_warning_active()
            || segment_manager.backlog_warning_threshold_reached()
        {
            return Some(config.min_snapshot_interval_ms_disk_warning.max(1));
        }
        None
    }

    fn should_persist_feature_snapshot_for_event(
        &mut self,
        mint: &str,
        event: &NormalizedEvent,
        decision: TradeDecision,
        fill_emitted: bool,
        now: OffsetDateTime,
    ) -> bool {
        let config = &self.resolved.loaded.config.features.pressure;
        let pressure_interval_ms = self.feature_snapshot_pressure_interval_ms();
        if pressure_interval_ms.is_none() {
            return true;
        }

        let always_snapshot = matches!(event.payload, EventPayload::DataGap(_))
            || (fill_emitted && config.always_snapshot_on_trade)
            || (config.always_snapshot_on_enter_exit
                && matches!(
                    decision,
                    TradeDecision::EnterPaper
                        | TradeDecision::EnterLive
                        | TradeDecision::Exit
                        | TradeDecision::EmergencyExit
                        | TradeDecision::ScaleOut
                ))
            || (config.always_snapshot_on_decision
                && !matches!(
                    decision,
                    TradeDecision::WatchLight | TradeDecision::WatchDeep
                ));
        if always_snapshot {
            return true;
        }
        if !config.skip_low_value_watch_snapshots_under_pressure
            || !matches!(
                decision,
                TradeDecision::WatchLight | TradeDecision::WatchDeep
            )
        {
            return true;
        }
        if let Some(last_snapshot_at) = self.last_feature_snapshot_at_by_mint.get(mint) {
            let elapsed_ms = (now - *last_snapshot_at).whole_milliseconds().max(0) as u64;
            if let Some(interval_ms) = pressure_interval_ms {
                if elapsed_ms < interval_ms {
                    self.health.skipped_feature_snapshot_count =
                        self.health.skipped_feature_snapshot_count.saturating_add(1);
                    return false;
                }
            }
        }
        true
    }

    fn persist_default_run_report(&self) -> Result<()> {
        let report_root = self
            .storage
            .layout()
            .report_dir
            .join(&self.resolved.loaded.config.environment.run_id);
        if self.edge_collector_mode() {
            let edge_status = self.edge_collector_status_report();
            std::fs::write(
                report_root.join("edge_collector_status.json"),
                serde_json::to_vec_pretty(&edge_status)?,
            )?;
            write_report(
                report_root.join("edge_collector_status.md"),
                &format!(
                    "# Edge Collector Status\n\n- run_id: {}\n- mode: {:?}\n- updated_at: {}\n- stream_only_passed: {}\n- rpc_network_calls_total: {}\n- rpc_credits_used_total: {}\n- events_processed: {}\n- active_tokens: {}\n- data_gap_active: {}\n- data_gap_scope: {:?}\n- provider_status: {}\n- notes: {:?}\n",
                    edge_status.run_id,
                    edge_status.mode,
                    edge_status.updated_at,
                    edge_status.stream_only_passed,
                    edge_status.rpc_network_calls_total,
                    edge_status.rpc_credits_used_total,
                    edge_status.events_processed,
                    edge_status.active_tokens,
                    edge_status.data_gap_active,
                    edge_status.data_gap_scope,
                    edge_status.provider_status,
                    edge_status.notes,
                ),
            )?;
        }
        write_report(
            report_root.join("run_summary.md"),
            &self.render_run_report()?,
        )?;
        std::fs::write(
            report_root.join("rpc_ledger.json"),
            serde_json::to_vec_pretty(self.rpc_budget.ledger())?,
        )?;
        write_report(
            report_root.join("stream_only_audit.md"),
            &self.render_stream_only_audit_report(),
        )?;
        write_report(
            report_root.join("rpc_denials.md"),
            &self.render_rpc_denials_report(),
        )?;
        write_report(
            report_root.join("stream_source_health.md"),
            &self.render_stream_source_health_report(),
        )?;
        if self.resolved.loaded.config.reports.auto_generate && !self.edge_collector_mode() {
            write_report(
                report_root.join("strategy_summary.md"),
                &self.render_strategy_summary(),
            )?;
            write_report(
                report_root.join("pnl_attribution.md"),
                &self.render_pnl_attribution_report(),
            )?;
            write_report(
                report_root.join("data_gaps.md"),
                &self.render_data_gaps_report(),
            )?;
            write_report(
                report_root.join("runtime_health.md"),
                &self.render_runtime_health_report(),
            )?;
            write_report(
                report_root.join("rejection_reasons.md"),
                &self.render_rejection_reasons_report(),
            )?;
            write_report(
                report_root.join("edge_calibration.md"),
                &self.render_edge_calibration_report(),
            )?;
            write_report(
                report_root.join("top_tokens.md"),
                &self.render_top_tokens_report(),
            )?;
            write_report(
                report_root.join("online_data_collection.md"),
                &self.render_online_data_collection_report(),
            )?;
            write_report(
                report_root.join("shred_exit_defense.md"),
                &self.render_shred_exit_defense_report(),
            )?;
            write_report(
                report_root.join("shred_exit_calibration.md"),
                &self.render_shred_exit_calibration_report(),
            )?;
        }
        Ok(())
    }

    fn capture_runtime_report_segments(&mut self) -> Result<()> {
        let data_gaps_report = self.render_data_gaps_report();
        if let Some(segment_manager) = self.segment_manager.as_mut() {
            segment_manager.append_text_snapshot(
                "data_gaps",
                "data_gaps.md",
                &data_gaps_report,
                unix_now(),
            )?;
        }
        Ok(())
    }
}

fn spawn_mock_geyser_live_source(
    source: MockGeyserLiveSource,
    sender: tokio::sync::mpsc::Sender<NormalizedEvent>,
) -> tokio::task::JoinHandle<Result<()>> {
    spawn_canonical_source(source, sender)
}

fn mock_early_intent_fixture_kind(name: Option<&str>) -> FixtureScenarioKind {
    match name.unwrap_or("dev_sell") {
        "dev_sell" => FixtureScenarioKind::ShredDevSellEarlyExit,
        "false_positive" => FixtureScenarioKind::ShredFalsePositiveSell,
        "too_late" => FixtureScenarioKind::ShredExitTooLate,
        "account_effect" => FixtureScenarioKind::ShredAccountEffectConfirmation,
        _ => FixtureScenarioKind::ShredDevSellEarlyExit,
    }
}

fn spawn_real_geyser_live_source(
    source: GeyserLiveSource,
    sender: tokio::sync::mpsc::Sender<NormalizedEvent>,
) -> tokio::task::JoinHandle<Result<()>> {
    spawn_canonical_source(source, sender)
}

fn spawn_mock_deshred_live_source(
    source: MockDeshredLiveSource,
    sender: tokio::sync::mpsc::Sender<NormalizedEvent>,
) -> tokio::task::JoinHandle<Result<()>> {
    spawn_tentative_source(source, sender)
}

fn mock_deshred_fixture_kind(name: Option<&str>) -> FixtureScenarioKind {
    match name.unwrap_or("dev_sell") {
        "dev_sell" => FixtureScenarioKind::ShredDevSellEarlyExit,
        "top_holder_duplicate" => FixtureScenarioKind::ShredTopHolderDumpEarlyExit,
        "false_positive" => FixtureScenarioKind::ShredFalsePositiveSell,
        "duplicate_false_positive" => FixtureScenarioKind::ShredFalsePositiveSell,
        "too_late" => FixtureScenarioKind::ShredExitTooLate,
        "account_effect" => FixtureScenarioKind::ShredAccountEffectConfirmation,
        "decode_mismatch" => FixtureScenarioKind::ShredGeyserDisagreement,
        "reorg" => FixtureScenarioKind::ShredReorgedSell,
        "duplicate_without_signature_fingerprint_match" => {
            FixtureScenarioKind::ShredDevSellEarlyExit
        }
        "duplicate_approximate_match" => FixtureScenarioKind::ShredDevSellEarlyExit,
        "near_duplicate_different_sell" => FixtureScenarioKind::ShredDevSellEarlyExit,
        _ => FixtureScenarioKind::ShredDevSellEarlyExit,
    }
}

fn as_mock_deshred_event(mut event: NormalizedEvent) -> NormalizedEvent {
    if event.meta.canonicality == Canonicality::Tentative {
        event.meta.source = EventSource::DeshredTentative;
        let mut reference = event
            .meta
            .raw_reference
            .unwrap_or_else(|| common::RawEventReference {
                source_id: "mock-deshred".to_owned(),
                cursor: None,
                offset: None,
            });
        reference.source_id = "mock-deshred".to_owned();
        event.meta.raw_reference = Some(reference);
        event.meta.event_id = common::EventId::from_seed(&format!(
            "mock-deshred-event|{}|{}|{}|{}",
            event.meta.event_id.0,
            event.meta.signature.as_deref().unwrap_or_default(),
            event.meta.slot,
            event.meta.transaction_index.unwrap_or_default()
        ));
    }
    event
}

fn as_mock_early_intent_event(mut event: NormalizedEvent) -> NormalizedEvent {
    if event.meta.canonicality == Canonicality::Tentative {
        event.meta.source = EventSource::ShredTentative;
        let mut reference = event
            .meta
            .raw_reference
            .unwrap_or_else(|| common::RawEventReference {
                source_id: "mock-early-intent".to_owned(),
                cursor: None,
                offset: None,
            });
        reference.source_id = "mock-early-intent".to_owned();
        event.meta.raw_reference = Some(reference);
        event.meta.event_id = common::EventId::from_seed(&format!(
            "mock-early-intent-event|{}|{}|{}|{}",
            event.meta.event_id.0,
            event.meta.signature.as_deref().unwrap_or_default(),
            event.meta.slot,
            event.meta.transaction_index.unwrap_or_default()
        ));
    }
    event
}

fn apply_mock_source_variant(
    events: Vec<NormalizedEvent>,
    variant: Option<&str>,
    as_deshred: bool,
) -> Vec<NormalizedEvent> {
    let variant = variant.unwrap_or_default();
    events
        .into_iter()
        .map(|mut event| {
            if event.meta.canonicality == Canonicality::Tentative {
                if as_deshred {
                    event = as_mock_deshred_event(event);
                } else {
                    event = as_mock_early_intent_event(event);
                }
                match variant {
                    "duplicate_without_signature_fingerprint_match" if !as_deshred => {
                        event.meta.signature = None;
                    }
                    "duplicate_approximate_match" if !as_deshred => {
                        if let EventPayload::PumpSell(payload) = &mut event.payload {
                            payload.token_in *= Decimal::new(102, 2);
                        }
                    }
                    "near_duplicate_different_sell" if !as_deshred => {
                        event.meta.slot = event.meta.slot.saturating_add(2);
                        event.meta.signature = event
                            .meta
                            .signature
                            .clone()
                            .map(|value| format!("{value}-near"));
                        if let EventPayload::PumpSell(payload) = &mut event.payload {
                            payload.token_in *= Decimal::new(130, 2);
                        }
                    }
                    _ => {}
                }
            }
            event
        })
        .collect()
}

fn spawn_real_deshred_live_source(
    source: DeshredLiveSource,
    sender: tokio::sync::mpsc::Sender<NormalizedEvent>,
) -> tokio::task::JoinHandle<Result<()>> {
    spawn_tentative_source(source, sender)
}

fn spawn_mock_shred_live_source(
    source: MockShredLiveSource,
    sender: tokio::sync::mpsc::Sender<NormalizedEvent>,
    tentative_ttl_ms: u64,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let ttl = Duration::milliseconds(tentative_ttl_ms as i64);
        let reconciler = ingest_shred::ShredReconciler::new(ReconciliationConfig { ttl });
        let mut service =
            ShredIngestService::new(FixtureShredDecoder, reconciler, ShredMetrics::default());
        for batch in source.batches {
            let slot = batch.slot.unwrap_or_default();
            let packet = ReceivedPacket {
                data: serde_json::to_vec(&batch)?,
                peer_addr: "127.0.0.1:0".parse().expect("loopback"),
                received_at: OffsetDateTime::UNIX_EPOCH + Duration::seconds(slot as i64),
                observed_at_monotonic_ns: slot * 1_000_000_000,
                packet_hash: format!("mock-{slot}"),
            };
            for event in service.process_packet(&packet)? {
                if sender.send(event).await.is_err() {
                    return Ok(());
                }
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
        Ok(())
    })
}

fn spawn_canonical_source<S>(
    mut source: S,
    sender: tokio::sync::mpsc::Sender<NormalizedEvent>,
) -> tokio::task::JoinHandle<Result<()>>
where
    S: CanonicalEventSource + Send + 'static,
{
    tokio::spawn(async move { source.run(sender).await })
}

fn spawn_tentative_source<S>(
    mut source: S,
    sender: tokio::sync::mpsc::Sender<NormalizedEvent>,
) -> tokio::task::JoinHandle<Result<()>>
where
    S: EarlyIntentEventSource + Send + 'static,
{
    tokio::spawn(async move { source.run(sender).await })
}

fn scoped_gaps_from_event(
    payload: &common::DataGapEvent,
    run_id: &str,
    scenario_id: Option<String>,
) -> Vec<ScopedDataGap> {
    let is_recovery = payload.trade_allowed
        || payload.recovery_action.contains("recover")
        || payload.recovery_action.contains("resume");
    if !payload.affected_tokens.is_empty() {
        return payload
            .affected_tokens
            .iter()
            .map(|mint| ScopedDataGap {
                scope: DataGapScope::Token,
                source: payload.source,
                start_slot: payload.start_slot,
                end_slot: payload.end_slot,
                severity: payload.severity,
                active: !is_recovery,
                trade_blocking: !payload.trade_allowed,
                affected_token: Some(mint.0.clone()),
                run_id: run_id.to_owned(),
                scenario_id: scenario_id.clone(),
                reason_codes: vec![ReasonCode::DataGapActive],
                recovery_action: payload.recovery_action.clone(),
            })
            .collect();
    }

    let scope = if payload.recovery_action.contains("enter_data_gap_mode") {
        DataGapScope::Global
    } else if scenario_id.is_some() {
        DataGapScope::Scenario
    } else {
        DataGapScope::Source
    };
    vec![ScopedDataGap {
        scope,
        source: payload.source,
        start_slot: payload.start_slot,
        end_slot: payload.end_slot,
        severity: payload.severity,
        active: !is_recovery,
        trade_blocking: !payload.trade_allowed,
        affected_token: None,
        run_id: run_id.to_owned(),
        scenario_id,
        reason_codes: vec![ReasonCode::DataGapActive],
        recovery_action: payload.recovery_action.clone(),
    }]
}

fn group_records_by_scenario(
    records: Vec<StoredRecord<NormalizedEvent>>,
) -> Vec<(Option<String>, Vec<StoredRecord<NormalizedEvent>>)> {
    let mut groups: BTreeMap<Option<String>, Vec<StoredRecord<NormalizedEvent>>> = BTreeMap::new();
    for record in records {
        groups
            .entry(record.scenario_id.clone())
            .or_default()
            .push(record);
    }
    groups.into_iter().collect()
}

fn merge_snapshots(left: Option<StateSnapshot>, right: StateSnapshot) -> StateSnapshot {
    let mut merged = left.unwrap_or(StateSnapshot {
        tokens: HashMap::new(),
        wallets: HashMap::new(),
        funding_graph: right.funding_graph.clone(),
        cluster_index: right.cluster_index.clone(),
        discarded_summaries: HashMap::new(),
    });
    merged.tokens.extend(right.tokens);
    merged.wallets.extend(right.wallets);
    merged.discarded_summaries.extend(right.discarded_summaries);
    merged.funding_graph = right.funding_graph;
    merged.cluster_index = right.cluster_index;
    merged
}

fn max_time(left: Option<OffsetDateTime>, right: Option<OffsetDateTime>) -> Option<OffsetDateTime> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn parse_gap_scope(scope: &str) -> DataGapScope {
    match scope {
        "Global" => DataGapScope::Global,
        "Source" => DataGapScope::Source,
        "SlotRange" => DataGapScope::SlotRange,
        "Token" => DataGapScope::Token,
        "Run" => DataGapScope::Run,
        "Scenario" => DataGapScope::Scenario,
        _ => DataGapScope::Source,
    }
}

fn now_for_token(token: &state::TokenState) -> OffsetDateTime {
    token
        .trade_stats
        .price_history
        .back()
        .map(|(time, _)| *time)
        .or(token.launch_time)
        .unwrap_or_else(unix_now)
}

fn lifecycle_rank(lifecycle: TokenLifecycle) -> u8 {
    match lifecycle {
        TokenLifecycle::Discovered => 0,
        TokenLifecycle::FirstPass => 1,
        TokenLifecycle::ActiveLight => 2,
        TokenLifecycle::ActiveDeep => 3,
        TokenLifecycle::TradeCandidate => 4,
        TokenLifecycle::InPosition => 5,
        TokenLifecycle::ExitPending => 6,
        TokenLifecycle::Completed => 7,
        TokenLifecycle::SoftDiscarded => 8,
        TokenLifecycle::HardDiscarded => 9,
        TokenLifecycle::RugArchive => 10,
        TokenLifecycle::Migrated => 11,
        TokenLifecycle::DataGap => 12,
    }
}

fn build_shred_service(loaded: &LoadedConfig) -> Result<Option<ShredService>> {
    if !loaded.config.shred.enabled {
        return Ok(None);
    }
    let metrics = ShredMetrics::default();
    let ttl = Duration::milliseconds(loaded.config.shred.tentative_ttl_ms as i64);
    let reconciler = ingest_shred::ShredReconciler::new(ReconciliationConfig { ttl });
    match loaded.config.shred.decoder {
        common::ShredDecoderMode::Fixture => Ok(Some(ShredService::Fixture(
            ShredIngestService::new(FixtureShredDecoder, reconciler, metrics),
        ))),
        common::ShredDecoderMode::Production => {
            if cfg!(feature = "production-shred-decoder") {
                Ok(Some(ShredService::Production(ShredIngestService::new(
                    ProductionShredDecoder,
                    reconciler,
                    metrics,
                ))))
            } else if loaded.config.shred.allow_geyser_only_fallback {
                Ok(None)
            } else {
                Err(RuntimeError::ProductionShredDecoderDisabled.into())
            }
        }
    }
}

fn resolved_geyser_endpoint(config: &common::GeyserConfig) -> Result<String> {
    if !config.endpoint.trim().is_empty() {
        return Ok(config.endpoint.clone());
    }
    if !config.endpoint_env.trim().is_empty() {
        if let Ok(value) = env::var(&config.endpoint_env) {
            if !value.trim().is_empty() {
                return Ok(value);
            }
        }
    }
    let env_name = if config.endpoint_env.trim().is_empty() {
        "GEYSER_ENDPOINT"
    } else {
        config.endpoint_env.as_str()
    };
    Err(anyhow!(
        "{env_name} is required for real live-data paper; use --mock-live for mocked live tests"
    ))
}

fn resolved_geyser_metadata(config: &common::GeyserConfig) -> Result<Option<(String, String)>> {
    let env_name = if !config.auth_metadata_value_env.trim().is_empty() {
        &config.auth_metadata_value_env
    } else {
        &config.auth_token_env
    };
    if env_name.trim().is_empty() {
        return Ok(None);
    }
    match env::var(env_name) {
        Ok(value) if !value.trim().is_empty() => {
            let key = if config.auth_metadata_key.trim().is_empty() {
                "x-token"
            } else {
                config.auth_metadata_key.as_str()
            };
            Ok(Some((key.to_owned(), value)))
        }
        _ if config.auth_required => Err(anyhow!(
            "{env_name} is required for real live-data paper geyser auth"
        )),
        _ => Ok(None),
    }
}

pub(crate) fn resolved_deshred_endpoint(config: &common::DeshredConfig) -> Result<String> {
    if !config.endpoint.trim().is_empty() {
        return Ok(config.endpoint.clone());
    }
    if !config.endpoint_env.trim().is_empty() {
        if let Ok(value) = env::var(&config.endpoint_env) {
            if !value.trim().is_empty() {
                return Ok(value);
            }
        }
    }
    let env_name = if config.endpoint_env.trim().is_empty() {
        "GEYSER_ENDPOINT"
    } else {
        config.endpoint_env.as_str()
    };
    Err(anyhow!(
        "{env_name} is required for deshred live-data paper; use --mock-deshred or leave ingest.deshred.enabled=false"
    ))
}

pub(crate) fn resolved_deshred_metadata(
    config: &common::DeshredConfig,
) -> Result<Option<(String, String)>> {
    if config.auth_token_env.trim().is_empty() {
        if config.auth_required {
            return Err(anyhow!(
                "deshred auth is required but no auth_token_env is configured"
            ));
        }
        return Ok(None);
    }
    match env::var(&config.auth_token_env) {
        Ok(value) if !value.trim().is_empty() => {
            let key = if config.auth_metadata_key.trim().is_empty() {
                "x-token"
            } else {
                config.auth_metadata_key.as_str()
            };
            Ok(Some((key.to_owned(), value)))
        }
        _ if config.auth_required => Err(anyhow!(
            "{} is required for deshred auth",
            config.auth_token_env
        )),
        _ => Ok(None),
    }
}

fn deshred_requested(config: &common::AppConfig, options: &LiveRunOptions) -> bool {
    options.with_deshred
        || options.require_deshred
        || options.mock_deshred
        || config
            .ingest
            .deshred
            .as_ref()
            .map(|deshred| deshred.enabled || deshred.required)
            .unwrap_or(false)
}

fn combined_idl_hash(loaded: &LoadedConfig) -> Result<String> {
    let mut combined = String::new();
    for path in &loaded.config.pump.idl_paths {
        let idl = LoadedIdl::load(loaded.resolve_path(path))?;
        combined.push_str(&idl.hash);
    }
    Ok(StorageEngine::config_fingerprint(&loaded.hash, &combined))
}

fn payload_label(payload: &EventPayload) -> &'static str {
    match payload {
        EventPayload::TokenCreated(_) => "token_created",
        EventPayload::PumpBuy(_) => "pump_buy",
        EventPayload::PumpSell(_) => "pump_sell",
        EventPayload::BondingCurveUpdate(_) => "bonding_curve_update",
        EventPayload::HolderBalanceUpdate(_) => "holder_balance_update",
        EventPayload::WalletFunding(_) => "wallet_funding",
        EventPayload::ObservedTransaction(_) => "observed_transaction",
        EventPayload::TentativeSellIntentDetected(_) => "tentative_sell_intent_detected",
        EventPayload::TentativeMaliciousSellWarning(_) => "tentative_malicious_sell_warning",
        EventPayload::ShredEmergencyExitArmed(_) => "shred_emergency_exit_armed",
        EventPayload::ShredEmergencyExitTriggered(_) => "shred_emergency_exit_triggered",
        EventPayload::ShredSellIntentResolved(_) => "shred_sell_intent_resolved",
        EventPayload::TokenTerminal(_) => "token_terminal",
        EventPayload::TradeDecision(_) => "trade_decision",
        EventPayload::SimulatedFill(_) => "simulated_fill",
        EventPayload::LiveFill(_) => "live_fill",
        EventPayload::DataGap(_) => "data_gap",
    }
}

fn synthetic_gap_event(slot: u64, source: EventSource) -> NormalizedEvent {
    NormalizedEvent {
        meta: EventMeta::new(source, Canonicality::Processed, slot),
        payload: EventPayload::DataGap(common::DataGapEvent {
            gap_type: common::DataGapType::TransactionStreamGap,
            source,
            start_slot: slot,
            end_slot: Some(slot),
            affected_tokens: Vec::new(),
            severity: common::GapSeverity::High,
            trade_allowed: false,
            recovery_action: "enter_data_gap_mode".to_owned(),
        }),
    }
}

fn force_fail_for_token(token: &state::TokenState) -> bool {
    token.symbol.eq_ignore_ascii_case("FL")
}

fn evaluate_fixture_result(
    spec: &FixtureScenarioSpec,
    summary: &RuntimeSummary,
) -> FixtureRunResult {
    let token = summary
        .snapshot
        .tokens
        .values()
        .next()
        .cloned()
        .unwrap_or_else(|| {
            state::TokenState::new(PubkeyValue("missing".to_owned()), EventSource::Replay)
        });
    let risk = summary
        .latest_risk
        .get(&token.mint.0)
        .cloned()
        .unwrap_or_else(|| risk::RiskAssessment {
            mint: token.mint.0.clone(),
            observed_at: unix_now(),
            rug: risk::RiskScore {
                name: "rug".to_owned(),
                score: Decimal::ZERO,
                confidence: Decimal::ZERO,
                severity: risk::RiskSeverity::Low,
                reason_codes: vec![],
                positive_evidence: vec![],
                negative_evidence: vec![],
                missing_data_penalty: Decimal::ZERO,
                recommended_lifecycle_action: risk::LifecycleAction::KeepWatching,
            },
            bundle: risk::RiskScore {
                name: "bundle".to_owned(),
                score: Decimal::ZERO,
                confidence: Decimal::ZERO,
                severity: risk::RiskSeverity::Low,
                reason_codes: vec![],
                positive_evidence: vec![],
                negative_evidence: vec![],
                missing_data_penalty: Decimal::ZERO,
                recommended_lifecycle_action: risk::LifecycleAction::KeepWatching,
            },
            dev: risk::RiskScore {
                name: "dev".to_owned(),
                score: Decimal::ZERO,
                confidence: Decimal::ZERO,
                severity: risk::RiskSeverity::Low,
                reason_codes: vec![],
                positive_evidence: vec![],
                negative_evidence: vec![],
                missing_data_penalty: Decimal::ZERO,
                recommended_lifecycle_action: risk::LifecycleAction::KeepWatching,
            },
            top_holder: risk::RiskScore {
                name: "top_holder".to_owned(),
                score: Decimal::ZERO,
                confidence: Decimal::ZERO,
                severity: risk::RiskSeverity::Low,
                reason_codes: vec![],
                positive_evidence: vec![],
                negative_evidence: vec![],
                missing_data_penalty: Decimal::ZERO,
                recommended_lifecycle_action: risk::LifecycleAction::KeepWatching,
            },
            fake_momentum: risk::RiskScore {
                name: "fake_momentum".to_owned(),
                score: Decimal::ZERO,
                confidence: Decimal::ZERO,
                severity: risk::RiskSeverity::Low,
                reason_codes: vec![],
                positive_evidence: vec![],
                negative_evidence: vec![],
                missing_data_penalty: Decimal::ZERO,
                recommended_lifecycle_action: risk::LifecycleAction::KeepWatching,
            },
            data_quality: risk::RiskScore {
                name: "data_quality".to_owned(),
                score: Decimal::ZERO,
                confidence: Decimal::ZERO,
                severity: risk::RiskSeverity::Low,
                reason_codes: vec![],
                positive_evidence: vec![],
                negative_evidence: vec![],
                missing_data_penalty: Decimal::ZERO,
                recommended_lifecycle_action: risk::LifecycleAction::KeepWatching,
            },
            discard_policy: risk::DiscardPolicyDecision::Keep,
            overall_score: Decimal::ZERO,
            overall_confidence: Decimal::ZERO,
            reason_codes: vec![],
        });
    let feature_engine = FeatureEngine::default();
    let simulator = Simulator::new(FeeModel::default());
    let features =
        feature_engine.compute_snapshot(&token, &summary.snapshot, now_for_token(&token));
    let labels = simulator.generate_labels(&token, &features, Decimal::from(10u64));
    let enter_paper_count = summary
        .decisions_by_type
        .get("EnterPaper")
        .copied()
        .unwrap_or_default();
    let watch_deep_count = summary
        .decisions_by_type
        .get("WatchDeep")
        .copied()
        .unwrap_or_default();
    let trade_interest = enter_paper_count > 0 || watch_deep_count > 0;
    let actual_decisions = summary
        .decision_events
        .iter()
        .map(|event| format!("{:?}", event.decision))
        .collect::<Vec<_>>();
    let actual_risk_flags = risk
        .reason_codes
        .iter()
        .map(|reason| format!("{reason:?}"))
        .collect::<Vec<_>>();
    let actual_discard_state = matches!(
        token.lifecycle,
        TokenLifecycle::SoftDiscarded
            | TokenLifecycle::HardDiscarded
            | TokenLifecycle::RugArchive
            | TokenLifecycle::DataGap
    )
    .then(|| format!("{:?}", token.lifecycle));
    let early_sell_warning = token.shred_defense.last_warning_level.is_some()
        || token.shred_defense.tentative_sell_count_window > 0;
    let exit_armed = token.shred_defense.tentative_sell_count_window > 0
        && (token.shred_defense.tentative_sell_confirmed_total > 0
            || token.shred_defense.tentative_sell_failed_total > 0
            || token.shred_defense.tentative_sell_not_seen_total > 0
            || token.shred_defense.tentative_sell_reorged_total > 0
            || token.shred_defense.tentative_sell_decode_mismatch_total > 0
            || token.shred_defense.shred_exit_armed_flag);
    let emergency_exit = summary
        .decision_events
        .iter()
        .any(|event| matches!(event.decision, TradeDecision::EmergencyExit))
        || token.shred_defense.shred_emergency_exit_triggered_flag
        || summary.fills.iter().any(|fill| {
            fill.exit_source.as_deref() == Some("ShredEmergencyExit")
                || fill.exit_source.as_deref() == Some("DeshredEmergencyExit")
        });
    let inferred_reconciliation_outcome = infer_reconciliation_outcome(&summary.health);
    let reconciliation_outcome = token
        .shred_defense
        .last_resolution_outcome
        .map(|outcome| format!("{outcome:?}"))
        .or_else(|| inferred_reconciliation_outcome.clone());
    let inferred_confirmation_level = infer_confirmation_level(&summary.health);
    let confirmation_level = match token.shred_defense.last_confirmation_level {
        Some(common::TentativeSellConfirmationState::PendingTentative) => {
            inferred_confirmation_level.or_else(|| Some("PendingTentative".to_owned()))
        }
        Some(level) => Some(format!("{level:?}")),
        None => inferred_confirmation_level,
    };
    let false_discard_detected = spec.expected.expected_false_discard
        && matches!(
            token.lifecycle,
            TokenLifecycle::SoftDiscarded | TokenLifecycle::HardDiscarded
        )
        && labels.profitable_after_fees_flag;
    let lifecycle_ok = spec
        .expected
        .expected_min_lifecycle
        .map(|expected| lifecycle_rank(token.lifecycle) >= lifecycle_rank(expected))
        .unwrap_or(true);
    let allowed_ok = spec.expected.expected_allowed_decisions.is_empty()
        || actual_decisions
            .iter()
            .any(|decision| spec.expected.expected_allowed_decisions.contains(decision));
    let forbidden_ok = actual_decisions.iter().all(|decision| {
        !spec
            .expected
            .expected_forbidden_decisions
            .contains(decision)
    });
    let risk_flags_ok = spec
        .expected
        .expected_risk_flags
        .iter()
        .all(|flag| actual_risk_flags.contains(flag));
    let token_gap_active = matches!(token.lifecycle, TokenLifecycle::DataGap);
    let global_gap_active = summary.health.data_gap_scope.as_deref() == Some("Global");
    let passed = (!spec.expected.expect_enter_paper || trade_interest)
        && (!spec.expected.expect_no_enter_paper || enter_paper_count == 0)
        && (!spec.expected.expect_data_gap || summary.health.data_gap_active || token_gap_active)
        && (!spec.expected.expect_discard || actual_discard_state.is_some())
        && spec
            .expected
            .expected_discard_state
            .map(|expected| token.lifecycle == expected)
            .unwrap_or(true)
        && (!spec.expected.expected_false_discard || false_discard_detected)
        && (!spec.expected.expected_global_safety_block || global_gap_active)
        && (!spec.expected.expected_token_safety_block || token_gap_active)
        && (!spec.expected.expected_early_sell_warning || early_sell_warning)
        && (!spec.expected.expected_exit_armed || exit_armed)
        && (!spec.expected.expected_emergency_exit || emergency_exit)
        && (!spec.expected.expected_saved_loss_positive
            || token.shred_defense.shred_saved_loss_realized > Decimal::ZERO
            || token.shred_defense.shred_saved_loss_estimate > Decimal::ZERO)
        && (!spec.expected.expected_false_positive
            || token.shred_defense.tentative_sell_false_positive_total > 0)
        && (!spec.expected.expected_no_live_send || summary.safety.live_allowed == false)
        && spec
            .expected
            .expected_reconciliation_outcome
            .as_ref()
            .map(|expected| reconciliation_outcome.as_deref() == Some(expected.as_str()))
            .unwrap_or(true)
        && spec
            .expected
            .expected_confirmation_level
            .as_ref()
            .map(|expected| confirmation_level.as_deref() == Some(expected.as_str()))
            .unwrap_or(true)
        && spec.expected.expected_paper_fill_min_count <= summary.fills.len()
        && lifecycle_ok
        && allowed_ok
        && forbidden_ok
        && risk_flags_ok
        && risk.rug.score >= spec.expected.min_rug_score
        && risk.bundle.score >= spec.expected.min_bundle_score;
    let latest_outcome = summary.decision_outcomes.last();
    FixtureRunResult {
        scenario_name: spec.name.clone(),
        scenario_id: summary
            .scenario_id
            .clone()
            .unwrap_or_else(|| spec.name.clone()),
        events_processed: summary
            .snapshot
            .tokens
            .values()
            .map(|token| (token.trade_stats.buy_count + token.trade_stats.sell_count) as usize)
            .sum(),
        decisions_made: summary.decisions_by_type.values().sum(),
        fills_simulated: summary.fills.len(),
        final_lifecycle: format!("{:?}", token.lifecycle),
        final_pnl: summary.health.paper_pnl,
        rug_score: risk.rug.score,
        bundle_score: risk.bundle.score,
        discard_reason: summary
            .latest_risk
            .get(&token.mint.0)
            .and_then(|assessment| {
                (!assessment.reason_codes.is_empty())
                    .then(|| format!("{:?}", assessment.reason_codes))
            }),
        expected_summary: format!(
            "allowed={:?} forbidden={:?} risk_flags={:?} min_lifecycle={:?} fills>={}",
            spec.expected.expected_allowed_decisions,
            spec.expected.expected_forbidden_decisions,
            spec.expected.expected_risk_flags,
            spec.expected.expected_min_lifecycle,
            spec.expected.expected_paper_fill_min_count
        ),
        actual_decisions,
        actual_risk_flags,
        actual_discard_state,
        actual_paper_fills: summary.fills.len(),
        top_blocking_reasons: latest_outcome
            .map(|outcome| outcome.diagnostics.clone())
            .unwrap_or_default(),
        top_positive_signals: latest_outcome
            .map(|outcome| {
                outcome
                    .composite_scores
                    .iter()
                    .flat_map(|(name, score)| {
                        score
                            .top_positive_components
                            .iter()
                            .map(move |value| format!("{name}:{value}"))
                    })
                    .take(6)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        top_negative_signals: latest_outcome
            .map(|outcome| {
                outcome
                    .composite_scores
                    .iter()
                    .flat_map(|(name, score)| {
                        score
                            .top_negative_components
                            .iter()
                            .map(move |value| format!("{name}:{value}"))
                    })
                    .take(6)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        data_gap_state: if summary.health.data_gap_active {
            summary
                .health
                .data_gap_scope
                .clone()
                .or_else(|| Some("Token".to_owned()))
        } else {
            None
        },
        false_discard_detected,
        early_sell_warning,
        exit_armed,
        emergency_exit,
        saved_loss_quote: token
            .shred_defense
            .shred_saved_loss_realized
            .max(summary.health.shred_saved_loss_quote_total),
        opportunity_cost_quote: token
            .shred_defense
            .shred_exit_opportunity_cost
            .max(summary.health.shred_opportunity_cost_quote_total),
        false_positive_tentative: token.shred_defense.tentative_sell_false_positive_total > 0
            || summary.health.shred_sell_false_positive_total > 0,
        reconciliation_outcome,
        confirmation_level,
        passed_expectation: passed,
    }
}

fn infer_reconciliation_outcome(health: &RuntimeHealth) -> Option<String> {
    if health.shred_reorged_total > 0 {
        Some("Reorged".to_owned())
    } else if health.shred_decode_mismatch_total > 0 {
        Some("DecodeMismatch".to_owned())
    } else if health.shred_account_effect_confirmed_total > 0 {
        Some("AccountEffectsObserved".to_owned())
    } else if health.shred_confirmed_failed_total > 0 {
        Some("ConfirmedFailed".to_owned())
    } else if health.shred_confirmed_executed_total > 0 {
        Some("ConfirmedExecuted".to_owned())
    } else if health.shred_not_seen_within_ttl_total > 0 {
        Some("NotSeenWithinTtl".to_owned())
    } else {
        None
    }
}

fn infer_confirmation_level(health: &RuntimeHealth) -> Option<String> {
    if health.shred_reorged_total > 0 {
        Some("Reorged".to_owned())
    } else if health.shred_decode_mismatch_total > 0 {
        Some("DecodeMismatch".to_owned())
    } else if health.shred_account_effect_confirmed_total > 0 {
        Some("AccountEffectsObserved".to_owned())
    } else if health.shred_confirmed_failed_total > 0 {
        Some("ConfirmedFailed".to_owned())
    } else if health.shred_confirmed_executed_total > 0 {
        Some("ConfirmedExecuted".to_owned())
    } else if health.shred_not_seen_within_ttl_total > 0 {
        Some("NotSeenWithinTtl".to_owned())
    } else {
        None
    }
}

pub fn write_report(path: impl AsRef<Path>, content: &str) -> Result<()> {
    if let Some(parent) = path.as_ref().parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, content)?;
    Ok(())
}

pub fn spawn_metrics_server(
    metrics: QuantMetrics,
    bind_addr: &str,
    state: Arc<Mutex<RuntimeHttpSnapshot>>,
) -> Result<MetricsServerHandle> {
    let bind_addr = bind_addr.to_owned();
    let registry = Arc::new(Mutex::new(metrics));
    let listener = TcpListener::bind(&bind_addr)?;
    listener.set_nonblocking(true)?;
    let stop = Arc::new(AtomicBool::new(false));
    let stop_flag = stop.clone();
    let join = thread::spawn(move || {
        while !stop_flag.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buffer = [0u8; 2048];
                    let read = stream.read(&mut buffer).unwrap_or_default();
                    let request = String::from_utf8_lossy(&buffer[..read]);
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");
                    let snapshot = state.lock().ok().map(|guard| guard.clone()).unwrap_or(
                        RuntimeHttpSnapshot {
                            health: RuntimeHealth::default(),
                            safety: RuntimeSafety::default(),
                            updated_at: unix_now(),
                        },
                    );
                    let (status, content_type, body) = match path {
                        "/metrics" => (
                            "200 OK",
                            "text/plain; version=0.0.4",
                            render_metrics_body(
                                registry
                                    .lock()
                                    .ok()
                                    .and_then(|metrics| metrics.gather_text().ok())
                                    .unwrap_or_else(|| "# metrics unavailable\n".to_owned()),
                                &snapshot,
                            ),
                        ),
                        "/readyz" => {
                            let (ready, reasons) = ready_state(&snapshot);
                            let status = if ready {
                                "200 OK"
                            } else {
                                "503 Service Unavailable"
                            };
                            let body = serde_json::to_string_pretty(&json!({
                                "ready": ready,
                                "reasons": reasons,
                                "health": snapshot.health,
                                "safety": snapshot.safety,
                                "updated_at": snapshot.updated_at,
                            }))
                            .unwrap_or_else(|_| "{\"ready\":false}".to_owned());
                            (status, "application/json", body)
                        }
                        "/healthz" => {
                            let healthy = snapshot.health.storage_healthy;
                            let status = if healthy {
                                "200 OK"
                            } else {
                                "503 Service Unavailable"
                            };
                            let body = serde_json::to_string_pretty(&json!({
                                "healthy": healthy,
                                "health": snapshot.health,
                                "safety": snapshot.safety,
                                "updated_at": snapshot.updated_at,
                            }))
                            .unwrap_or_else(|_| "{\"healthy\":false}".to_owned());
                            (status, "application/json", body)
                        }
                        _ => ("404 Not Found", "text/plain", "not found".to_owned()),
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(response.as_bytes());
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(StdDuration::from_millis(25));
                }
                Err(_) => break,
            }
        }
    });
    Ok(MetricsServerHandle {
        stop,
        join: Some(join),
    })
}

fn render_metrics_body(base: String, snapshot: &RuntimeHttpSnapshot) -> String {
    let mut body = base;
    body.push_str(&format!(
        "runtime_events_processed_total {}\n\
runtime_active_tokens {}\n\
runtime_active_positions {}\n\
runtime_paper_pnl_realized {}\n\
runtime_paper_pnl_unrealized {}\n\
runtime_uptime_seconds {}\n\
early_intent_enabled {}\n\
geyser_connected {}\n\
geyser_events_received_total {}\n\
geyser_reconnects_total {}\n\
deshred_connected {}\n\
deshred_updates_received_total {}\n\
deshred_transactions_decoded_total {}\n\
deshred_tentative_sells_total {}\n\
deshred_malicious_warnings_total {}\n\
deshred_emergency_exits_armed_total {}\n\
deshred_emergency_exits_triggered_total {}\n\
deshred_confirmed_executed_total {}\n\
deshred_false_positive_total {}\n\
deshred_saved_loss_quote_total {}\n\
deshred_opportunity_cost_quote_total {}\n\
raw_shred_connected {}\n\
raw_shred_tentative_sells_total {}\n\
mock_early_intent_active {}\n\
mock_early_intent_tentative_sells_total {}\n\
fixture_early_intent_tentative_sells_total {}\n\
replay_early_intent_tentative_sells_total {}\n\
runtime_queue_depth{{queue=\"canonical\"}} {}\n\
runtime_queue_depth{{queue=\"tentative\"}} {}\n\
runtime_queue_overflows_total{{queue=\"canonical\"}} {}\n\
shred_tentative_sells_total {}\n\
shred_malicious_sell_warnings_total {}\n\
shred_emergency_exits_armed_total {}\n\
shred_emergency_exits_triggered_total {}\n\
shred_confirmed_executed_total {}\n\
shred_confirmed_failed_total {}\n\
shred_not_seen_within_ttl_total {}\n\
shred_reorged_total {}\n\
shred_decode_mismatch_total {}\n\
shred_account_effect_confirmed_total {}\n\
shred_sell_false_positive_total {}\n\
shred_saved_loss_quote_total {}\n\
shred_opportunity_cost_quote_total {}\n",
        snapshot.health.events_processed,
        snapshot.health.active_tokens,
        snapshot.health.active_positions,
        snapshot.health.realized_pnl,
        snapshot.health.paper_pnl - snapshot.health.realized_pnl,
        Decimal::from(snapshot.health.runtime_uptime_ms) / Decimal::from(1000u64),
        if snapshot.health.early_intent_enabled {
            1
        } else {
            0
        },
        if snapshot.health.geyser_connected {
            1
        } else {
            0
        },
        snapshot.health.geyser_events_received,
        snapshot.health.geyser_reconnect_count,
        if snapshot.health.deshred_connected {
            1
        } else {
            0
        },
        snapshot.health.deshred_events_received,
        snapshot.health.deshred_transactions_decoded,
        snapshot.health.deshred_tentative_sells_total,
        snapshot.health.deshred_malicious_warnings_total,
        snapshot.health.deshred_emergency_exits_armed_total,
        snapshot.health.deshred_emergency_exits_triggered_total,
        snapshot.health.deshred_confirmed_executed_total,
        snapshot.health.deshred_false_positive_total,
        snapshot.health.deshred_saved_loss_quote_total,
        snapshot.health.deshred_opportunity_cost_quote_total,
        if snapshot.health.raw_shred_connected {
            1
        } else {
            0
        },
        snapshot.health.raw_shred_tentative_sells_total,
        if snapshot.health.mock_early_intent_active {
            1
        } else {
            0
        },
        snapshot.health.mock_early_intent_tentative_sells_total,
        snapshot.health.fixture_early_intent_tentative_sells_total,
        snapshot.health.replay_early_intent_tentative_sells_total,
        snapshot.health.canonical_queue_depth,
        snapshot.health.tentative_queue_depth,
        snapshot.health.canonical_events_dropped,
        snapshot.health.shred_tentative_sells_total,
        snapshot.health.shred_malicious_sell_warnings_total,
        snapshot.health.shred_emergency_exits_armed_total,
        snapshot.health.shred_emergency_exits_triggered_total,
        snapshot.health.shred_confirmed_executed_total,
        snapshot.health.shred_confirmed_failed_total,
        snapshot.health.shred_not_seen_within_ttl_total,
        snapshot.health.shred_reorged_total,
        snapshot.health.shred_decode_mismatch_total,
        snapshot.health.shred_account_effect_confirmed_total,
        snapshot.health.shred_sell_false_positive_total,
        snapshot.health.shred_saved_loss_quote_total,
        snapshot.health.shred_opportunity_cost_quote_total,
    ));
    for source in &snapshot.health.early_intent_sources_active {
        body.push_str(&format!(
            "early_intent_source_active{{source=\"{}\"}} 1\n",
            source
        ));
    }
    for (pair, count) in &snapshot.health.early_intent_dedup_pairs {
        if let Some((primary, duplicate)) = pair.split_once('|') {
            body.push_str(&format!(
                "early_intent_deduplicated_total{{primary_source=\"{}\",duplicate_source=\"{}\"}} {}\n",
                primary, duplicate, count
            ));
        }
    }
    body
}

fn ready_state(snapshot: &RuntimeHttpSnapshot) -> (bool, Vec<String>) {
    let mut reasons = Vec::new();
    if !snapshot.health.storage_healthy {
        reasons.push("storage_unhealthy".to_owned());
    }
    if !snapshot.health.rpc_budget_healthy {
        reasons.push("rpc_budget_unhealthy".to_owned());
    }
    if snapshot.health.kill_switch_active {
        reasons.push("kill_switch_active".to_owned());
    }
    if snapshot.health.mode == RuntimeMode::LiveDataPaper && !snapshot.health.geyser_connected {
        reasons.push("geyser_unhealthy".to_owned());
    }
    if snapshot.health.data_gap_active
        && snapshot.health.data_gap_scope.as_deref() == Some("Global")
    {
        reasons.push("global_data_gap".to_owned());
    }
    if !snapshot.safety.trade_allowed && snapshot.health.data_gap_scope.as_deref() == Some("Global")
    {
        reasons.push("global_trade_block".to_owned());
    }
    (reasons.is_empty(), reasons)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpStream,
    };

    use tempfile::tempdir;

    use super::*;

    fn loaded_config() -> LoadedConfig {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("config")
            .join("default.toml");
        let mut loaded = LoadedConfig::from_file(path).expect("config");
        let root = tempdir().expect("tempdir").keep();
        loaded.config.metrics.enabled = false;
        loaded.config.storage.root = root.join("data").display().to_string();
        loaded
    }

    #[tokio::test]
    async fn supervisor_starts_and_stops_fixture_mode() {
        let mut loaded = loaded_config();
        loaded.config.runtime.mode = RuntimeModeName::Fixture;
        let resolved = RuntimeResolvedConfig::from_loaded(loaded, Some(RuntimeMode::Fixture))
            .expect("resolved");
        let mut supervisor = Supervisor::new(resolved).expect("supervisor");
        let summary = supervisor
            .run_fixture(build_fixture_scenario(
                &fixtures::builtin_fixture_suite()[0],
            ))
            .await
            .expect("fixture");
        assert!(summary.snapshot.tokens.len() >= 1);
        assert!(summary.audits.len() >= 2);
    }

    #[tokio::test]
    async fn tentative_overflow_drops_tentative_first() {
        let mut loaded = loaded_config();
        loaded.config.runtime.queue_capacity_tentative = 1;
        let resolved = RuntimeResolvedConfig::from_loaded(loaded, Some(RuntimeMode::Fixture))
            .expect("resolved");
        let mut supervisor = Supervisor::new(resolved).expect("supervisor");
        let scenario =
            fixtures::build_fixture_scenario(&fixtures::spec(FixtureScenarioKind::BundledLaunch));
        supervisor
            .ingest_fixture_shreds(&scenario.shred_batches)
            .await
            .expect("shreds");
        assert!(!supervisor.audits.is_empty());
    }

    #[tokio::test]
    async fn canonical_overflow_blocks_trading() {
        let mut loaded = loaded_config();
        loaded.config.runtime.queue_capacity_canonical = 1;
        let resolved = RuntimeResolvedConfig::from_loaded(loaded, Some(RuntimeMode::Fixture))
            .expect("resolved");
        let mut supervisor = Supervisor::new(resolved).expect("supervisor");
        let scenario = fixtures::build_fixture_scenario(&fixtures::spec(
            FixtureScenarioKind::StrongHolderGrowthWinner,
        ));
        for event in scenario.canonical_events {
            let _ = supervisor.publish_canonical(event);
        }
        assert!(supervisor.health.data_gap_active || !supervisor.safety.trade_allowed);
    }

    #[tokio::test]
    async fn runtime_safety_blocks_live_by_default() {
        let loaded = loaded_config();
        let resolved = RuntimeResolvedConfig::from_loaded(loaded, Some(RuntimeMode::LiveDataPaper))
            .expect("resolved");
        let mut supervisor = Supervisor::new(resolved).expect("supervisor");
        let summary = supervisor.live_data_readiness().expect("readiness");
        assert!(!summary.safety.live_allowed);
    }

    #[tokio::test]
    async fn geyser_only_fallback_marks_shred_unavailable() {
        let mut loaded = loaded_config();
        loaded.config.shred.enabled = false;
        loaded.config.shred.required = false;
        let resolved = RuntimeResolvedConfig::from_loaded(loaded, Some(RuntimeMode::LiveDataPaper))
            .expect("resolved");
        let mut supervisor = Supervisor::new(resolved).expect("supervisor");
        let summary = supervisor.live_data_readiness().expect("readiness");
        assert!(!summary.health.shred_connected);
        assert!(!summary.safety.live_allowed);
    }

    #[tokio::test]
    async fn shred_required_without_decoder_fails() {
        let mut loaded = loaded_config();
        loaded.config.shred.enabled = true;
        loaded.config.shred.required = true;
        loaded.config.shred.allow_geyser_only_fallback = false;
        loaded.config.shred.decoder = common::ShredDecoderMode::Production;
        let resolved = RuntimeResolvedConfig::from_loaded(loaded, Some(RuntimeMode::LiveDataPaper))
            .expect("resolved");
        let error = match Supervisor::new(resolved) {
            Ok(_) => panic!("supervisor should fail"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("production shred decoder not enabled")
        );
    }

    #[tokio::test]
    async fn replay_reproduces_fixture_run_from_store() {
        let loaded = loaded_config();
        let resolved =
            RuntimeResolvedConfig::from_loaded(loaded.clone(), Some(RuntimeMode::Fixture))
                .expect("resolved");
        let mut supervisor = Supervisor::new(resolved).expect("supervisor");
        let scenario = fixtures::build_fixture_scenario(&fixtures::spec(
            FixtureScenarioKind::CleanOrganicLaunch,
        ));
        let online = supervisor.run_fixture(scenario).await.expect("fixture");
        let mut replay_loaded = loaded.clone();
        replay_loaded.config.environment.run_id = loaded.config.environment.run_id.clone();
        let resolved =
            RuntimeResolvedConfig::from_loaded(replay_loaded, Some(RuntimeMode::PaperFromStore))
                .expect("resolved");
        let mut replay = Supervisor::new(resolved).expect("supervisor");
        let replayed = replay
            .run_from_store_for_run(Some(&loaded.config.environment.run_id))
            .await
            .expect("replay");
        let online_token = online
            .snapshot
            .tokens
            .values()
            .next()
            .expect("online token");
        let replayed_token = replayed
            .snapshot
            .tokens
            .values()
            .next()
            .expect("replayed token");
        assert_eq!(online_token.lifecycle, replayed_token.lifecycle);
        assert!(!replayed.decisions_by_type.is_empty());
    }

    #[tokio::test]
    async fn mock_deshred_live_paper_runs() {
        let loaded = loaded_config();
        let resolved = RuntimeResolvedConfig::from_loaded(loaded, Some(RuntimeMode::LiveDataPaper))
            .expect("resolved");
        let mut supervisor = Supervisor::new(resolved).expect("supervisor");
        let summary = supervisor
            .run_live_data_paper(LiveRunOptions {
                mock_live: true,
                mock_deshred: true,
                dry_run: true,
                max_events: Some(100),
                ..LiveRunOptions::default()
            })
            .await
            .expect("mock deshred run");
        assert!(summary.health.deshred_events_received > 0);
        assert!(!summary.safety.live_allowed);
        assert!(
            summary.health.shred_tentative_sells_total > 0
                || summary.health.shred_malicious_sell_warnings_total > 0
        );
    }

    #[tokio::test]
    async fn edge_collector_run_is_not_blocked_when_paper_mode_is_disabled() {
        let loaded = loaded_config();
        let resolved = RuntimeResolvedConfig::from_loaded(loaded, Some(RuntimeMode::EdgeCollector))
            .expect("resolved");
        let mut supervisor = Supervisor::new(resolved).expect("supervisor");
        let summary = supervisor
            .run_live_data_paper(LiveRunOptions {
                mock_live: true,
                mock_deshred: true,
                dry_run: true,
                max_events: Some(100),
                ..LiveRunOptions::default()
            })
            .await
            .expect("edge collector run");
        assert_eq!(summary.health.mode, RuntimeMode::EdgeCollector);
        assert!(!summary.safety.paper_allowed);
        assert!(summary.health.events_processed > 0);
    }

    #[test]
    fn metrics_server_exposes_health_and_readiness() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let bind = listener.local_addr().expect("addr");
        drop(listener);
        let state = Arc::new(Mutex::new(RuntimeHttpSnapshot {
            health: RuntimeHealth {
                geyser_connected: true,
                storage_healthy: true,
                rpc_budget_healthy: true,
                mode: RuntimeMode::LiveDataPaper,
                ..RuntimeHealth::default()
            },
            safety: RuntimeSafety {
                trade_allowed: true,
                paper_allowed: true,
                ..RuntimeSafety::default()
            },
            updated_at: unix_now(),
        }));
        let handle = spawn_metrics_server(
            QuantMetrics::new().expect("metrics"),
            &bind.to_string(),
            state.clone(),
        )
        .expect("server");
        std::thread::sleep(StdDuration::from_millis(100));
        let healthz = http_get(&bind.to_string(), "/healthz");
        let readyz = http_get(&bind.to_string(), "/readyz");
        let metrics = http_get(&bind.to_string(), "/metrics");
        handle.stop();
        assert!(healthz.contains("200 OK"));
        assert!(readyz.contains("200 OK"));
        assert!(metrics.contains("runtime_events_processed_total"));
    }

    #[test]
    fn readyz_blocks_on_global_gap() {
        let snapshot = RuntimeHttpSnapshot {
            health: RuntimeHealth {
                geyser_connected: true,
                storage_healthy: true,
                rpc_budget_healthy: true,
                data_gap_active: true,
                data_gap_scope: Some("Global".to_owned()),
                mode: RuntimeMode::LiveDataPaper,
                ..RuntimeHealth::default()
            },
            safety: RuntimeSafety {
                trade_allowed: false,
                paper_allowed: true,
                ..RuntimeSafety::default()
            },
            updated_at: unix_now(),
        };
        let (ready, reasons) = ready_state(&snapshot);
        assert!(!ready);
        assert!(reasons.iter().any(|reason| reason == "global_data_gap"));
    }

    fn http_get(bind: &str, path: &str) -> String {
        let mut stream = TcpStream::connect(bind).expect("connect");
        let request = format!("GET {path} HTTP/1.1\r\nHost: {bind}\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).expect("write");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read");
        response
    }
}
