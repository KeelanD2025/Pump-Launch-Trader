use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    error::{QuantError, Result},
    schema::SCHEMA_VERSION,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadedConfig {
    pub path: PathBuf,
    pub hash: String,
    pub config: AppConfig,
}

impl LoadedConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_files(path, None::<&Path>)
    }

    pub fn from_files(
        base_path: impl AsRef<Path>,
        override_path: Option<impl AsRef<Path>>,
    ) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        let base_raw = fs::read_to_string(&base_path)?;
        let mut merged = toml::from_str::<toml::Value>(&base_raw)?;
        let path = if let Some(override_path) = override_path {
            let override_path = override_path.as_ref().to_path_buf();
            let override_raw = fs::read_to_string(&override_path)?;
            let override_value = toml::from_str::<toml::Value>(&override_raw)?;
            merge_toml_value(&mut merged, override_value);
            override_path
        } else {
            base_path
        };
        let mut config: AppConfig = merged.try_into()?;
        config.apply_defaults();
        let canonical = toml::to_string_pretty(&config).map_err(|error| {
            QuantError::Config(format!("failed to canonicalize config: {error}"))
        })?;
        let hash = format!("{:x}", Sha256::digest(canonical.as_bytes()));
        Ok(Self { path, hash, config })
    }

    pub fn resolve_path(&self, relative: &str) -> PathBuf {
        if Path::new(relative).is_absolute() {
            PathBuf::from(relative)
        } else {
            self.path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(relative)
        }
    }
}

fn merge_toml_value(base: &mut toml::Value, overlay: toml::Value) {
    match (base, overlay) {
        (toml::Value::Table(base_table), toml::Value::Table(overlay_table)) => {
            for (key, overlay_value) in overlay_table {
                match base_table.get_mut(&key) {
                    Some(base_value) => merge_toml_value(base_value, overlay_value),
                    None => {
                        base_table.insert(key, overlay_value);
                    }
                }
            }
        }
        (base_value, overlay_value) => {
            *base_value = overlay_value;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub environment: EnvironmentConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub edge_collector: EdgeCollectorConfig,
    #[serde(default)]
    pub research_worker: ResearchWorkerConfig,
    #[serde(default)]
    pub enrichment: EnrichmentConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
    #[serde(default)]
    pub early_intent: EarlyIntentConfig,
    #[serde(default)]
    pub stream_only: StreamOnlyConfig,
    #[serde(default)]
    pub rpc: RpcConfig,
    #[serde(default)]
    pub confirmation: ConfirmationConfig,
    #[serde(default)]
    pub metadata: MetadataConfig,
    #[serde(default)]
    pub provider: ProviderCompatibilityConfig,
    #[serde(default)]
    pub autopilot: AutopilotConfig,
    #[serde(default)]
    pub r2: R2Config,
    pub geyser: GeyserConfig,
    pub shred: ShredConfig,
    pub pump: PumpProgramConfig,
    pub storage: StorageConfig,
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub reports: ReportsConfig,
    #[serde(default)]
    pub analysis: AnalysisConfig,
    #[serde(default)]
    pub exports: ExportsConfig,
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub paper: PaperConfig,
    #[serde(default)]
    pub live: LiveConfig,
    #[serde(default)]
    pub shred_exit: ShredExitConfig,
    #[serde(default)]
    pub edge: EdgeConfig,
    pub ttl: TtlConfig,
    pub strategy: StrategyThresholds,
    #[serde(default)]
    pub risk: RiskConfig,
    #[serde(default)]
    pub decision: DecisionConfig,
    pub rpc_budget: RpcBudgetConfig,
    #[serde(default)]
    pub quote_assets: Vec<QuoteAssetConfig>,
    #[serde(default)]
    pub feature_families: BTreeMap<String, FeatureFamilyBudget>,
    #[serde(default)]
    pub features: FeatureToggleConfig,
}

impl AppConfig {
    pub fn apply_defaults(&mut self) {
        if self.environment.schema_version == 0 {
            self.environment.schema_version = SCHEMA_VERSION;
        }
        self.runtime.apply_defaults();
        self.edge_collector.apply_defaults();
        self.research_worker.apply_defaults();
        self.enrichment.apply_defaults();
        self.ingest.apply_defaults();
        self.early_intent.apply_defaults();
        self.stream_only.apply_defaults();
        self.rpc.apply_defaults();
        self.confirmation.apply_defaults();
        self.metadata.apply_defaults();
        self.provider.apply_defaults();
        self.autopilot.apply_defaults();
        self.r2.apply_defaults();
        self.geyser.apply_defaults();
        self.shred.apply_defaults();
        self.storage.apply_defaults();
        self.metrics.apply_defaults();
        self.reports.apply_defaults();
        self.analysis.apply_defaults();
        self.exports.apply_defaults();
        self.execution.apply_defaults();
        self.paper.apply_defaults();
        self.live.apply_defaults();
        self.shred_exit.apply_defaults();
        self.edge.apply_defaults();
        self.strategy.apply_defaults();
        self.risk.apply_defaults();
        self.decision.apply_defaults();
        self.features.apply_defaults();

        if let Some(geyser) = self.ingest.geyser.clone() {
            self.geyser = geyser;
        } else {
            self.ingest.geyser = Some(self.geyser.clone());
        }
        if let Some(shred) = self.ingest.shred.clone() {
            self.shred = shred;
        } else {
            self.ingest.shred = Some(self.shred.clone());
        }

        self.execution.paper_enabled = self.paper.enabled;
        self.execution.live_enabled = self.live.enabled;
        self.execution.dry_run = self.live.dry_run;
        self.execution.max_open_positions = self.paper.max_positions;
        self.execution.max_position_size_quote = self.paper.max_position_size_quote;
        self.execution.max_daily_loss_quote = self.paper.max_daily_loss_quote;
        self.execution.max_fee_quote = self.live.max_fee_per_transaction_quote;
        self.execution.max_slippage_bps = self.live.max_slippage_bps;

        self.strategy.max_rug_risk = self.risk.max_rug_score;
        self.strategy.max_bundle_risk = self.risk.max_bundle_score;
        self.strategy.max_fake_momentum_risk = self.risk.max_fake_momentum_score;
        self.strategy.min_data_quality_score = self.risk.min_data_quality_score;
        self.strategy.min_fee_adjusted_edge_bps = self.decision.min_fee_adjusted_edge_bps;

        if self.stream_only.enabled {
            self.rpc.hot_path_enabled = false;
            self.rpc.daily_credit_budget = self.stream_only.rpc_daily_credit_budget;
            self.rpc.monthly_credit_budget = self.stream_only.rpc_monthly_credit_budget;
            self.execution.use_rpc_send = false;
            self.metadata.fetch_uri = false;
            self.metadata.hot_path_fetch_enabled = false;
            self.confirmation.source = "geyser_stream".to_owned();
            self.confirmation.allow_rpc_status_fallback = false;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentConfig {
    pub cluster: String,
    pub run_id: String,
    pub strategy_version: String,
    #[serde(default = "schema_default")]
    pub schema_version: u32,
}

const fn schema_default() -> u32 {
    SCHEMA_VERSION
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeModeName {
    Fixture,
    Replay,
    #[default]
    PaperFromStore,
    LiveDataPaper,
    EdgeCollector,
    ResearchWorker,
    Autopilot,
    GuardedLiveDryRun,
    GuardedLiveEnabled,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub mode: RuntimeModeName,
    #[serde(default)]
    pub queue_capacity_canonical: usize,
    #[serde(default)]
    pub queue_capacity_tentative: usize,
    #[serde(default)]
    pub queue_capacity_decisions: usize,
    #[serde(default)]
    pub shutdown_drain_timeout_ms: u64,
    #[serde(default)]
    pub health_report_interval_ms: u64,
}

impl RuntimeConfig {
    fn apply_defaults(&mut self) {
        if self.queue_capacity_canonical == 0 {
            self.queue_capacity_canonical = 2048;
        }
        if self.queue_capacity_tentative == 0 {
            self.queue_capacity_tentative = 1024;
        }
        if self.queue_capacity_decisions == 0 {
            self.queue_capacity_decisions = 512;
        }
        if self.shutdown_drain_timeout_ms == 0 {
            self.shutdown_drain_timeout_ms = 2_000;
        }
        if self.health_report_interval_ms == 0 {
            self.health_report_interval_ms = 1_000;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeCollectorStorageConfig {
    #[serde(default = "default_edge_collector_segment_dir")]
    pub segment_dir: String,
    #[serde(default = "default_edge_collector_max_open_segments")]
    pub max_open_segments: usize,
    #[serde(default = "default_edge_collector_max_local_segment_mb")]
    pub max_local_segment_mb: u64,
    #[serde(default = "default_true")]
    pub delete_verified_segments_immediately: bool,
    #[serde(default = "default_true")]
    pub require_r2_verified_before_delete: bool,
    #[serde(default = "default_true")]
    pub forbid_large_local_exports: bool,
    #[serde(default = "default_true")]
    pub forbid_feature_snapshot_files: bool,
    #[serde(default = "default_true")]
    pub forbid_research_reports: bool,
}

impl Default for EdgeCollectorStorageConfig {
    fn default() -> Self {
        Self {
            segment_dir: default_edge_collector_segment_dir(),
            max_open_segments: default_edge_collector_max_open_segments(),
            max_local_segment_mb: default_edge_collector_max_local_segment_mb(),
            delete_verified_segments_immediately: true,
            require_r2_verified_before_delete: true,
            forbid_large_local_exports: true,
            forbid_feature_snapshot_files: true,
            forbid_research_reports: true,
        }
    }
}

impl EdgeCollectorStorageConfig {
    fn apply_defaults(&mut self) {
        if self.segment_dir.trim().is_empty() {
            self.segment_dir = default_edge_collector_segment_dir();
        }
        if self.max_open_segments == 0 {
            self.max_open_segments = default_edge_collector_max_open_segments();
        }
        if self.max_local_segment_mb == 0 {
            self.max_local_segment_mb = default_edge_collector_max_local_segment_mb();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeCollectorConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub stream_only_required: bool,
    #[serde(default)]
    pub paper_decisions_enabled: bool,
    #[serde(default)]
    pub feature_engine_enabled: bool,
    #[serde(default)]
    pub risk_engine_enabled: bool,
    #[serde(default)]
    pub decision_engine_enabled: bool,
    #[serde(default)]
    pub paper_executor_enabled: bool,
    #[serde(default)]
    pub research_reports_enabled: bool,
    #[serde(default)]
    pub large_exports_enabled: bool,
    #[serde(default)]
    pub local_analysis_enabled: bool,
    #[serde(default = "default_true")]
    pub r2_upload_required: bool,
    #[serde(default = "default_true")]
    pub r2_verify_required: bool,
    #[serde(default = "default_true")]
    pub minimal_status_reports: bool,
    #[serde(default = "default_edge_collector_max_local_runtime_mb")]
    pub max_local_runtime_mb: u64,
    #[serde(default = "default_edge_collector_flush_interval_seconds")]
    pub flush_interval_seconds: u64,
    #[serde(default = "default_edge_collector_segment_max_size_mb")]
    pub segment_max_size_mb: u64,
    #[serde(default = "default_edge_collector_segment_max_age_seconds")]
    pub segment_max_age_seconds: u64,
    #[serde(default)]
    pub keep_local_segments_after_verify: bool,
    #[serde(default = "default_true")]
    pub keep_only_last_open_segment: bool,
    #[serde(default)]
    pub allow_rpc_enrichment: bool,
    #[serde(default)]
    pub allow_metadata_fetch: bool,
    #[serde(default)]
    pub allow_social_fetch: bool,
    #[serde(default)]
    pub allow_wallet_history_rpc: bool,
    #[serde(default)]
    pub allow_bundle_enrichment_rpc: bool,
    #[serde(default)]
    pub storage: EdgeCollectorStorageConfig,
}

impl Default for EdgeCollectorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            stream_only_required: true,
            paper_decisions_enabled: false,
            feature_engine_enabled: false,
            risk_engine_enabled: false,
            decision_engine_enabled: false,
            paper_executor_enabled: false,
            research_reports_enabled: false,
            large_exports_enabled: false,
            local_analysis_enabled: false,
            r2_upload_required: true,
            r2_verify_required: true,
            minimal_status_reports: true,
            max_local_runtime_mb: default_edge_collector_max_local_runtime_mb(),
            flush_interval_seconds: default_edge_collector_flush_interval_seconds(),
            segment_max_size_mb: default_edge_collector_segment_max_size_mb(),
            segment_max_age_seconds: default_edge_collector_segment_max_age_seconds(),
            keep_local_segments_after_verify: false,
            keep_only_last_open_segment: true,
            allow_rpc_enrichment: false,
            allow_metadata_fetch: false,
            allow_social_fetch: false,
            allow_wallet_history_rpc: false,
            allow_bundle_enrichment_rpc: false,
            storage: EdgeCollectorStorageConfig::default(),
        }
    }
}

impl EdgeCollectorConfig {
    fn apply_defaults(&mut self) {
        if self.max_local_runtime_mb == 0 {
            self.max_local_runtime_mb = default_edge_collector_max_local_runtime_mb();
        }
        if self.flush_interval_seconds == 0 {
            self.flush_interval_seconds = default_edge_collector_flush_interval_seconds();
        }
        if self.segment_max_size_mb == 0 {
            self.segment_max_size_mb = default_edge_collector_segment_max_size_mb();
        }
        if self.segment_max_age_seconds == 0 {
            self.segment_max_age_seconds = default_edge_collector_segment_max_age_seconds();
        }
        self.storage.apply_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchWorkerConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_research_worker_input")]
    pub input: String,
    #[serde(default = "default_true")]
    pub r2_dataset_index_required: bool,
    #[serde(default = "default_true")]
    pub download_segments: bool,
    #[serde(default = "default_true")]
    pub stream_segments_from_r2: bool,
    #[serde(default = "default_true")]
    pub compute_features: bool,
    #[serde(default = "default_true")]
    pub compute_risk: bool,
    #[serde(default = "default_true")]
    pub compute_decisions: bool,
    #[serde(default = "default_true")]
    pub run_backtests: bool,
    #[serde(default = "default_true")]
    pub generate_reports: bool,
    #[serde(default = "default_true")]
    pub generate_exports: bool,
    #[serde(default = "default_true")]
    pub update_calibration: bool,
    #[serde(default = "default_research_worker_local_output_dir")]
    pub local_output_dir: String,
}

impl Default for ResearchWorkerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            input: default_research_worker_input(),
            r2_dataset_index_required: true,
            download_segments: true,
            stream_segments_from_r2: true,
            compute_features: true,
            compute_risk: true,
            compute_decisions: true,
            run_backtests: true,
            generate_reports: true,
            generate_exports: true,
            update_calibration: true,
            local_output_dir: default_research_worker_local_output_dir(),
        }
    }
}

impl ResearchWorkerConfig {
    fn apply_defaults(&mut self) {
        if self.input.trim().is_empty() {
            self.input = default_research_worker_input();
        }
        if self.local_output_dir.trim().is_empty() {
            self.local_output_dir = default_research_worker_local_output_dir();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichmentConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_enrichment_mode")]
    pub mode: String,
    #[serde(default = "default_true")]
    pub run_after_replay: bool,
    #[serde(default)]
    pub run_on_vps: bool,
    #[serde(default = "default_true")]
    pub require_stream_source_first: bool,
    #[serde(default = "default_true")]
    pub cache_enabled: bool,
    #[serde(default = "default_enrichment_cache_dir")]
    pub cache_dir: String,
    #[serde(default = "default_enrichment_ledger_path")]
    pub ledger_path: String,
    #[serde(default = "default_enrichment_max_daily_rpc_calls")]
    pub max_daily_rpc_calls: u64,
    #[serde(default = "default_enrichment_max_daily_rpc_credits")]
    pub max_daily_rpc_credits: u64,
    #[serde(default = "default_enrichment_max_daily_http_metadata_fetches")]
    pub max_daily_http_metadata_fetches: u64,
    #[serde(default = "default_enrichment_max_wallets_per_run")]
    pub max_wallets_per_run: usize,
    #[serde(default = "default_enrichment_max_tokens_per_run")]
    pub max_tokens_per_run: usize,
    #[serde(default = "default_enrichment_max_signatures_per_wallet")]
    pub max_signatures_per_wallet: usize,
    #[serde(default = "default_enrichment_max_metadata_bytes")]
    pub max_metadata_bytes: u64,
    #[serde(default = "default_enrichment_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_enrichment_max_retries")]
    pub max_retries: usize,
    #[serde(default)]
    pub respect_robots: bool,
    #[serde(default = "default_true")]
    pub block_private_ips: bool,
    #[serde(default = "default_true")]
    pub deny_localhost: bool,
    #[serde(default = "default_true")]
    pub deny_link_local: bool,
    #[serde(default = "default_true")]
    pub deny_private_ranges: bool,
    #[serde(default)]
    pub rpc: EnrichmentRpcConfig,
    #[serde(default)]
    pub metadata: EnrichmentMetadataConfig,
}

impl Default for EnrichmentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: default_enrichment_mode(),
            run_after_replay: true,
            run_on_vps: false,
            require_stream_source_first: true,
            cache_enabled: true,
            cache_dir: default_enrichment_cache_dir(),
            ledger_path: default_enrichment_ledger_path(),
            max_daily_rpc_calls: default_enrichment_max_daily_rpc_calls(),
            max_daily_rpc_credits: default_enrichment_max_daily_rpc_credits(),
            max_daily_http_metadata_fetches:
                default_enrichment_max_daily_http_metadata_fetches(),
            max_wallets_per_run: default_enrichment_max_wallets_per_run(),
            max_tokens_per_run: default_enrichment_max_tokens_per_run(),
            max_signatures_per_wallet: default_enrichment_max_signatures_per_wallet(),
            max_metadata_bytes: default_enrichment_max_metadata_bytes(),
            request_timeout_ms: default_enrichment_request_timeout_ms(),
            max_retries: default_enrichment_max_retries(),
            respect_robots: false,
            block_private_ips: true,
            deny_localhost: true,
            deny_link_local: true,
            deny_private_ranges: true,
            rpc: EnrichmentRpcConfig::default(),
            metadata: EnrichmentMetadataConfig::default(),
        }
    }
}

impl EnrichmentConfig {
    fn apply_defaults(&mut self) {
        if self.mode.trim().is_empty() {
            self.mode = default_enrichment_mode();
        }
        if self.cache_dir.trim().is_empty() {
            self.cache_dir = default_enrichment_cache_dir();
        }
        if self.ledger_path.trim().is_empty() {
            self.ledger_path = default_enrichment_ledger_path();
        }
        if self.max_daily_rpc_calls == 0 {
            self.max_daily_rpc_calls = default_enrichment_max_daily_rpc_calls();
        }
        if self.max_daily_rpc_credits == 0 {
            self.max_daily_rpc_credits = default_enrichment_max_daily_rpc_credits();
        }
        if self.max_daily_http_metadata_fetches == 0 {
            self.max_daily_http_metadata_fetches =
                default_enrichment_max_daily_http_metadata_fetches();
        }
        if self.max_wallets_per_run == 0 {
            self.max_wallets_per_run = default_enrichment_max_wallets_per_run();
        }
        if self.max_tokens_per_run == 0 {
            self.max_tokens_per_run = default_enrichment_max_tokens_per_run();
        }
        if self.max_signatures_per_wallet == 0 {
            self.max_signatures_per_wallet = default_enrichment_max_signatures_per_wallet();
        }
        if self.max_metadata_bytes == 0 {
            self.max_metadata_bytes = default_enrichment_max_metadata_bytes();
        }
        if self.request_timeout_ms == 0 {
            self.request_timeout_ms = default_enrichment_request_timeout_ms();
        }
        if self.max_retries == 0 {
            self.max_retries = default_enrichment_max_retries();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichmentRpcConfig {
    #[serde(default = "default_true")]
    pub allow_funding_graph_rpc: bool,
    #[serde(default = "default_true")]
    pub allow_bundle_evidence_rpc: bool,
    #[serde(default = "default_true")]
    pub allow_wallet_age_rpc: bool,
    #[serde(default = "default_true")]
    pub allow_metadata_account_rpc: bool,
    #[serde(default)]
    pub allow_token_largest_accounts_rpc: bool,
    #[serde(default)]
    pub allow_get_program_accounts: bool,
    #[serde(default)]
    pub allow_backfill_large_history: bool,
    #[serde(default)]
    pub allow_transaction_history_scans: bool,
}

impl Default for EnrichmentRpcConfig {
    fn default() -> Self {
        Self {
            allow_funding_graph_rpc: true,
            allow_bundle_evidence_rpc: true,
            allow_wallet_age_rpc: true,
            allow_metadata_account_rpc: true,
            allow_token_largest_accounts_rpc: false,
            allow_get_program_accounts: false,
            allow_backfill_large_history: false,
            allow_transaction_history_scans: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichmentMetadataConfig {
    #[serde(default = "default_true")]
    pub allow_uri_fetch: bool,
    #[serde(default = "default_true")]
    pub allow_social_detection: bool,
    #[serde(default = "default_true")]
    pub fetch_only_token_metadata_uri: bool,
    #[serde(default = "default_true")]
    pub do_not_fetch_images: bool,
    #[serde(default = "default_true")]
    pub do_not_execute_javascript: bool,
    #[serde(default = "default_enrichment_max_redirects")]
    pub max_redirects: usize,
    #[serde(default = "default_enrichment_allowed_schemes")]
    pub allowed_schemes: Vec<String>,
    #[serde(default = "default_true")]
    pub extract_social_links_only: bool,
}

impl Default for EnrichmentMetadataConfig {
    fn default() -> Self {
        Self {
            allow_uri_fetch: true,
            allow_social_detection: true,
            fetch_only_token_metadata_uri: true,
            do_not_fetch_images: true,
            do_not_execute_javascript: true,
            max_redirects: default_enrichment_max_redirects(),
            allowed_schemes: default_enrichment_allowed_schemes(),
            extract_social_links_only: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IngestConfig {
    #[serde(default)]
    pub geyser: Option<GeyserConfig>,
    #[serde(default)]
    pub shred: Option<ShredConfig>,
    #[serde(default)]
    pub deshred: Option<DeshredConfig>,
}

impl IngestConfig {
    fn apply_defaults(&mut self) {
        if let Some(deshred) = self.deshred.as_mut() {
            deshred.apply_defaults();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamOnlyConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub fail_on_hot_path_rpc: bool,
    #[serde(default = "default_true")]
    pub fail_on_unbudgeted_rpc: bool,
    #[serde(default)]
    pub allow_tracking_rpc: bool,
    #[serde(default)]
    pub allow_holder_rpc: bool,
    #[serde(default)]
    pub allow_top_holder_rpc: bool,
    #[serde(default)]
    pub allow_metadata_rpc: bool,
    #[serde(default)]
    pub allow_backfill_rpc: bool,
    #[serde(default)]
    pub allow_reconciliation_rpc: bool,
    #[serde(default)]
    pub allow_confirmation_rpc: bool,
    #[serde(default)]
    pub allow_blockhash_rpc: bool,
    #[serde(default)]
    pub allow_execution_rpc: bool,
    #[serde(default)]
    pub allow_send_rpc: bool,
    #[serde(default)]
    pub allow_emergency_rpc: bool,
    #[serde(default)]
    pub rpc_daily_credit_budget: u64,
    #[serde(default)]
    pub rpc_monthly_credit_budget: u64,
}

impl Default for StreamOnlyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            fail_on_hot_path_rpc: true,
            fail_on_unbudgeted_rpc: true,
            allow_tracking_rpc: false,
            allow_holder_rpc: false,
            allow_top_holder_rpc: false,
            allow_metadata_rpc: false,
            allow_backfill_rpc: false,
            allow_reconciliation_rpc: false,
            allow_confirmation_rpc: false,
            allow_blockhash_rpc: false,
            allow_execution_rpc: false,
            allow_send_rpc: false,
            allow_emergency_rpc: false,
            rpc_daily_credit_budget: 0,
            rpc_monthly_credit_budget: 0,
        }
    }
}

impl StreamOnlyConfig {
    fn apply_defaults(&mut self) {}
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RpcConfig {
    #[serde(default)]
    pub hot_path_enabled: bool,
    #[serde(default)]
    pub daily_credit_budget: u64,
    #[serde(default)]
    pub monthly_credit_budget: u64,
}

impl RpcConfig {
    fn apply_defaults(&mut self) {}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfirmationConfig {
    #[serde(default = "default_confirmation_source")]
    pub source: String,
    #[serde(default)]
    pub allow_rpc_status_fallback: bool,
}

impl Default for ConfirmationConfig {
    fn default() -> Self {
        Self {
            source: default_confirmation_source(),
            allow_rpc_status_fallback: false,
        }
    }
}

impl ConfirmationConfig {
    fn apply_defaults(&mut self) {
        if self.source.trim().is_empty() {
            self.source = default_confirmation_source();
        }
    }
}

fn default_confirmation_source() -> String {
    "geyser_stream".to_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetadataConfig {
    #[serde(default)]
    pub fetch_uri: bool,
    #[serde(default)]
    pub hot_path_fetch_enabled: bool,
    #[serde(default)]
    pub offline_import_path: String,
}

impl MetadataConfig {
    fn apply_defaults(&mut self) {}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EarlyIntentConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_early_intent_sources")]
    pub sources: Vec<String>,
    #[serde(default = "default_true")]
    pub prefer_deshred_when_available: bool,
    #[serde(default = "default_true")]
    pub allow_raw_shred_fixture_in_dev: bool,
    #[serde(default)]
    pub allow_production_raw_shred: bool,
    #[serde(default = "default_true")]
    pub require_early_intent_for_live_shred_exit: bool,
    #[serde(default)]
    pub max_tentative_event_age_ms: u64,
    #[serde(default = "default_true")]
    pub deduplicate_sources: bool,
    #[serde(default)]
    pub dedup_amount_tolerance_pct: Decimal,
    #[serde(default)]
    pub dedup_slot_tolerance: u64,
    #[serde(default)]
    pub mock: EarlyIntentMockConfig,
    #[serde(default)]
    pub source_precedence: EarlyIntentSourcePrecedenceConfig,
}

impl Default for EarlyIntentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sources: default_early_intent_sources(),
            prefer_deshred_when_available: true,
            allow_raw_shred_fixture_in_dev: true,
            allow_production_raw_shred: false,
            require_early_intent_for_live_shred_exit: true,
            max_tentative_event_age_ms: 750,
            deduplicate_sources: true,
            dedup_amount_tolerance_pct: Decimal::from(5u64),
            dedup_slot_tolerance: 0,
            mock: EarlyIntentMockConfig::default(),
            source_precedence: EarlyIntentSourcePrecedenceConfig::default(),
        }
    }
}

impl EarlyIntentConfig {
    fn apply_defaults(&mut self) {
        if self.sources.is_empty() {
            self.sources = default_early_intent_sources();
        }
        if self.max_tentative_event_age_ms == 0 {
            self.max_tentative_event_age_ms = 750;
        }
        if self.dedup_amount_tolerance_pct <= Decimal::ZERO {
            self.dedup_amount_tolerance_pct = Decimal::from(5u64);
        }
        self.mock.apply_defaults();
        self.source_precedence.apply_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderCompatibilityConfig {
    #[serde(default)]
    pub name: String,
    #[serde(default = "default_true")]
    pub record_compatibility: bool,
    #[serde(default = "default_true")]
    pub endpoint_host_hash_only: bool,
}

impl Default for ProviderCompatibilityConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            record_compatibility: true,
            endpoint_host_hash_only: true,
        }
    }
}

impl ProviderCompatibilityConfig {
    fn apply_defaults(&mut self) {
        if !self.record_compatibility && self.name.is_empty() {
            // Preserve explicit disabled state while still normalizing the struct.
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutopilotConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_autopilot_mode")]
    pub mode: String,
    #[serde(default = "default_true")]
    pub stream_only_required: bool,
    #[serde(default)]
    pub live_trading_allowed: bool,
    #[serde(default = "default_true")]
    pub dry_run: bool,
    #[serde(default)]
    pub allow_mock_when_endpoint_missing: bool,
    #[serde(default = "default_true")]
    pub allow_geyser_only_when_deshred_unsupported: bool,
    #[serde(default = "default_true")]
    pub require_geyser: bool,
    #[serde(default)]
    pub require_deshred: bool,
    #[serde(default = "default_autopilot_parallel_runs")]
    pub max_parallel_runs: usize,
    #[serde(default = "default_autopilot_state_path")]
    pub state_path: String,
    #[serde(default = "default_autopilot_lock_path")]
    pub lock_path: String,
    #[serde(default = "default_autopilot_status_path")]
    pub status_path: String,
    #[serde(default = "default_autopilot_report_dir")]
    pub report_dir: String,
    #[serde(default = "default_autopilot_heartbeat_interval_seconds")]
    pub heartbeat_interval_seconds: u64,
    #[serde(default = "default_autopilot_cycle_sleep_seconds")]
    pub cycle_sleep_seconds: u64,
    #[serde(default = "default_autopilot_max_cycle_runtime_seconds")]
    pub max_cycle_runtime_seconds: u64,
    #[serde(default = "default_autopilot_graceful_shutdown_timeout_seconds")]
    pub graceful_shutdown_timeout_seconds: u64,
    #[serde(default)]
    pub schedule: AutopilotScheduleConfig,
    #[serde(default)]
    pub provider: AutopilotProviderConfig,
    #[serde(default)]
    pub retention: AutopilotRetentionConfig,
    #[serde(default)]
    pub alerts: AutopilotAlertsConfig,
    #[serde(default)]
    pub safety: AutopilotSafetyConfig,
    #[serde(default)]
    pub disk: AutopilotDiskConfig,
    #[serde(default)]
    pub post_cycle_cleanup: AutopilotPostCycleCleanupConfig,
    #[serde(default)]
    pub backtest_thresholds: AutopilotBacktestThresholdsConfig,
}

impl Default for AutopilotConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: default_autopilot_mode(),
            stream_only_required: true,
            live_trading_allowed: false,
            dry_run: true,
            allow_mock_when_endpoint_missing: false,
            allow_geyser_only_when_deshred_unsupported: true,
            require_geyser: true,
            require_deshred: false,
            max_parallel_runs: default_autopilot_parallel_runs(),
            state_path: default_autopilot_state_path(),
            lock_path: default_autopilot_lock_path(),
            status_path: default_autopilot_status_path(),
            report_dir: default_autopilot_report_dir(),
            heartbeat_interval_seconds: default_autopilot_heartbeat_interval_seconds(),
            cycle_sleep_seconds: default_autopilot_cycle_sleep_seconds(),
            max_cycle_runtime_seconds: default_autopilot_max_cycle_runtime_seconds(),
            graceful_shutdown_timeout_seconds: default_autopilot_graceful_shutdown_timeout_seconds(
            ),
            schedule: AutopilotScheduleConfig::default(),
            provider: AutopilotProviderConfig::default(),
            retention: AutopilotRetentionConfig::default(),
            alerts: AutopilotAlertsConfig::default(),
            safety: AutopilotSafetyConfig::default(),
            disk: AutopilotDiskConfig::default(),
            post_cycle_cleanup: AutopilotPostCycleCleanupConfig::default(),
            backtest_thresholds: AutopilotBacktestThresholdsConfig::default(),
        }
    }
}

impl AutopilotConfig {
    fn apply_defaults(&mut self) {
        if self.mode.trim().is_empty() {
            self.mode = default_autopilot_mode();
        }
        if self.state_path.trim().is_empty() {
            self.state_path = default_autopilot_state_path();
        }
        if self.lock_path.trim().is_empty() {
            self.lock_path = default_autopilot_lock_path();
        }
        if self.status_path.trim().is_empty() {
            self.status_path = default_autopilot_status_path();
        }
        if self.report_dir.trim().is_empty() {
            self.report_dir = default_autopilot_report_dir();
        }
        if self.heartbeat_interval_seconds == 0 {
            self.heartbeat_interval_seconds = default_autopilot_heartbeat_interval_seconds();
        }
        if self.cycle_sleep_seconds == 0 {
            self.cycle_sleep_seconds = default_autopilot_cycle_sleep_seconds();
        }
        if self.max_cycle_runtime_seconds == 0 {
            self.max_cycle_runtime_seconds = default_autopilot_max_cycle_runtime_seconds();
        }
        if self.graceful_shutdown_timeout_seconds == 0 {
            self.graceful_shutdown_timeout_seconds =
                default_autopilot_graceful_shutdown_timeout_seconds();
        }
        self.schedule.apply_defaults();
        self.provider.apply_defaults();
        self.retention.apply_defaults();
        self.alerts.apply_defaults();
        self.safety.apply_defaults();
        self.disk.apply_defaults();
        self.post_cycle_cleanup.apply_defaults();
        self.backtest_thresholds.apply_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutopilotScheduleConfig {
    #[serde(default = "default_true")]
    pub startup_smoke: bool,
    #[serde(default = "default_true")]
    pub continuous: bool,
    #[serde(default = "default_autopilot_collection_preset")]
    pub collection_preset: String,
    #[serde(default = "default_autopilot_collection_duration_seconds")]
    pub collection_duration_seconds: u64,
    #[serde(default = "default_autopilot_checkpoint_interval_seconds")]
    pub checkpoint_interval_seconds: u64,
    #[serde(default = "default_true")]
    pub run_research_cycle_after_collection: bool,
    #[serde(default = "default_true")]
    pub run_backtest_when_ready: bool,
    #[serde(default = "default_true")]
    pub export_after_collection: bool,
    #[serde(default = "default_true")]
    pub export_after_research_cycle: bool,
}

impl Default for AutopilotScheduleConfig {
    fn default() -> Self {
        Self {
            startup_smoke: true,
            continuous: true,
            collection_preset: default_autopilot_collection_preset(),
            collection_duration_seconds: default_autopilot_collection_duration_seconds(),
            checkpoint_interval_seconds: default_autopilot_checkpoint_interval_seconds(),
            run_research_cycle_after_collection: true,
            run_backtest_when_ready: true,
            export_after_collection: true,
            export_after_research_cycle: true,
        }
    }
}

impl AutopilotScheduleConfig {
    fn apply_defaults(&mut self) {
        if self.collection_preset.trim().is_empty() {
            self.collection_preset = default_autopilot_collection_preset();
        }
        if self.collection_duration_seconds == 0 {
            self.collection_duration_seconds = default_autopilot_collection_duration_seconds();
        }
        if self.checkpoint_interval_seconds == 0 {
            self.checkpoint_interval_seconds = default_autopilot_checkpoint_interval_seconds();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutopilotProviderConfig {
    #[serde(default = "default_true")]
    pub run_precheck_each_cycle: bool,
    #[serde(default)]
    pub run_geyser_smoke_each_cycle: bool,
    #[serde(default)]
    pub run_deshred_smoke_each_cycle: bool,
    #[serde(default = "default_true")]
    pub run_stream_smoke_on_startup: bool,
    #[serde(default = "default_autopilot_smoke_duration_seconds")]
    pub smoke_duration_seconds: u64,
    #[serde(default = "default_autopilot_provider_error_retry_seconds")]
    pub provider_error_retry_seconds: u64,
    #[serde(default = "default_autopilot_missing_endpoint_retry_seconds")]
    pub missing_endpoint_retry_seconds: u64,
    #[serde(default = "default_autopilot_auth_error_retry_seconds")]
    pub auth_error_retry_seconds: u64,
}

impl Default for AutopilotProviderConfig {
    fn default() -> Self {
        Self {
            run_precheck_each_cycle: true,
            run_geyser_smoke_each_cycle: false,
            run_deshred_smoke_each_cycle: false,
            run_stream_smoke_on_startup: true,
            smoke_duration_seconds: default_autopilot_smoke_duration_seconds(),
            provider_error_retry_seconds: default_autopilot_provider_error_retry_seconds(),
            missing_endpoint_retry_seconds: default_autopilot_missing_endpoint_retry_seconds(),
            auth_error_retry_seconds: default_autopilot_auth_error_retry_seconds(),
        }
    }
}

impl AutopilotProviderConfig {
    fn apply_defaults(&mut self) {
        if self.smoke_duration_seconds == 0 {
            self.smoke_duration_seconds = default_autopilot_smoke_duration_seconds();
        }
        if self.provider_error_retry_seconds == 0 {
            self.provider_error_retry_seconds = default_autopilot_provider_error_retry_seconds();
        }
        if self.missing_endpoint_retry_seconds == 0 {
            self.missing_endpoint_retry_seconds =
                default_autopilot_missing_endpoint_retry_seconds();
        }
        if self.auth_error_retry_seconds == 0 {
            self.auth_error_retry_seconds = default_autopilot_auth_error_retry_seconds();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutopilotRetentionConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_autopilot_max_total_storage_gb")]
    pub max_total_storage_gb: u64,
    #[serde(default = "default_autopilot_max_run_age_days")]
    pub max_run_age_days: u64,
    #[serde(default = "default_autopilot_keep_last_n_runs")]
    pub keep_last_n_runs: usize,
    #[serde(default = "default_autopilot_keep_last_n_backtests")]
    pub keep_last_n_backtests: usize,
    #[serde(default = "default_true")]
    pub keep_failed_runs: bool,
    #[serde(default = "default_true")]
    pub keep_provider_smoke_reports: bool,
    #[serde(default)]
    pub compress_old_reports: bool,
    #[serde(default)]
    pub delete_old_exports: bool,
}

impl Default for AutopilotRetentionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_total_storage_gb: default_autopilot_max_total_storage_gb(),
            max_run_age_days: default_autopilot_max_run_age_days(),
            keep_last_n_runs: default_autopilot_keep_last_n_runs(),
            keep_last_n_backtests: default_autopilot_keep_last_n_backtests(),
            keep_failed_runs: true,
            keep_provider_smoke_reports: true,
            compress_old_reports: false,
            delete_old_exports: false,
        }
    }
}

impl AutopilotRetentionConfig {
    fn apply_defaults(&mut self) {
        if self.max_total_storage_gb == 0 {
            self.max_total_storage_gb = default_autopilot_max_total_storage_gb();
        }
        if self.max_run_age_days == 0 {
            self.max_run_age_days = default_autopilot_max_run_age_days();
        }
        if self.keep_last_n_runs == 0 {
            self.keep_last_n_runs = default_autopilot_keep_last_n_runs();
        }
        if self.keep_last_n_backtests == 0 {
            self.keep_last_n_backtests = default_autopilot_keep_last_n_backtests();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutopilotAlertsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub write_local_alerts: bool,
    #[serde(default = "default_autopilot_alerts_path")]
    pub alerts_path: String,
    #[serde(default)]
    pub webhook_enabled: bool,
    #[serde(default)]
    pub webhook_url_env: String,
    #[serde(default = "default_true")]
    pub alert_on_provider_failure: bool,
    #[serde(default = "default_true")]
    pub alert_on_data_gap: bool,
    #[serde(default = "default_true")]
    pub alert_on_rpc_denial: bool,
    #[serde(default = "default_true")]
    pub alert_on_nonzero_rpc_usage: bool,
    #[serde(default = "default_true")]
    pub alert_on_queue_overflow: bool,
    #[serde(default = "default_true")]
    pub alert_on_backtest_ready: bool,
    #[serde(default = "default_true")]
    pub alert_on_autopilot_crash: bool,
}

impl Default for AutopilotAlertsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            write_local_alerts: true,
            alerts_path: default_autopilot_alerts_path(),
            webhook_enabled: false,
            webhook_url_env: String::new(),
            alert_on_provider_failure: true,
            alert_on_data_gap: true,
            alert_on_rpc_denial: true,
            alert_on_nonzero_rpc_usage: true,
            alert_on_queue_overflow: true,
            alert_on_backtest_ready: true,
            alert_on_autopilot_crash: true,
        }
    }
}

impl AutopilotAlertsConfig {
    fn apply_defaults(&mut self) {
        if self.alerts_path.trim().is_empty() {
            self.alerts_path = default_autopilot_alerts_path();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutopilotSafetyConfig {
    #[serde(default = "default_true")]
    pub stop_on_nonzero_rpc_usage: bool,
    #[serde(default = "default_true")]
    pub stop_on_stream_only_failure: bool,
    #[serde(default = "default_true")]
    pub stop_on_live_enabled: bool,
    #[serde(default)]
    pub stop_on_signer_config_present: bool,
    #[serde(default)]
    pub stop_on_global_data_gap: bool,
    #[serde(default)]
    pub stop_on_repeated_provider_failures: bool,
    #[serde(default = "default_autopilot_max_provider_failures_before_pause")]
    pub max_provider_failures_before_pause: u64,
    #[serde(default = "default_autopilot_max_queue_overflows_before_pause")]
    pub max_queue_overflows_before_pause: u64,
}

impl Default for AutopilotSafetyConfig {
    fn default() -> Self {
        Self {
            stop_on_nonzero_rpc_usage: true,
            stop_on_stream_only_failure: true,
            stop_on_live_enabled: true,
            stop_on_signer_config_present: false,
            stop_on_global_data_gap: false,
            stop_on_repeated_provider_failures: false,
            max_provider_failures_before_pause:
                default_autopilot_max_provider_failures_before_pause(),
            max_queue_overflows_before_pause: default_autopilot_max_queue_overflows_before_pause(),
        }
    }
}

impl AutopilotSafetyConfig {
    fn apply_defaults(&mut self) {
        if self.max_provider_failures_before_pause == 0 {
            self.max_provider_failures_before_pause =
                default_autopilot_max_provider_failures_before_pause();
        }
        if self.max_queue_overflows_before_pause == 0 {
            self.max_queue_overflows_before_pause =
                default_autopilot_max_queue_overflows_before_pause();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutopilotDiskConfig {
    #[serde(default = "default_autopilot_disk_min_free_mb_before_cycle")]
    pub min_free_mb_before_cycle: u64,
    #[serde(default = "default_autopilot_disk_min_free_mb_during_cycle")]
    pub min_free_mb_during_cycle: u64,
    #[serde(default = "default_autopilot_disk_check_interval_seconds")]
    pub check_interval_seconds: u64,
    #[serde(default = "default_autopilot_disk_warning_free_mb")]
    pub warning_free_mb: u64,
    #[serde(default = "default_autopilot_disk_pre_cycle_min_free_mb")]
    pub pre_cycle_min_free_mb: u64,
    #[serde(default = "default_autopilot_disk_critical_free_mb")]
    pub critical_free_mb: u64,
    #[serde(default = "default_autopilot_disk_emergency_free_mb")]
    pub emergency_free_mb: u64,
    #[serde(default = "default_true")]
    pub pause_if_below_min_free: bool,
    #[serde(default = "default_true")]
    pub run_prune_before_cycle: bool,
    #[serde(default = "default_true")]
    pub run_prune_after_verified_upload: bool,
    #[serde(default = "default_true")]
    pub allow_prune_verified_exports: bool,
    #[serde(default)]
    pub allow_prune_verified_reports: bool,
    #[serde(default)]
    pub allow_prune_verified_raw_events: bool,
    #[serde(default = "default_true")]
    pub keep_minimal_manifest: bool,
    #[serde(default = "default_true")]
    pub keep_rpc_ledger: bool,
    #[serde(default = "default_true")]
    pub keep_calibration: bool,
    #[serde(default = "default_true")]
    pub close_segments_on_warning: bool,
    #[serde(default = "default_true")]
    pub stop_collection_on_critical: bool,
    #[serde(default = "default_true")]
    pub pause_before_os_error: bool,
    #[serde(default = "default_true")]
    pub prune_verified_segments_on_warning: bool,
    #[serde(default = "default_true")]
    pub prune_verified_runs_on_startup: bool,
    #[serde(default)]
    pub block_new_cycle_below_warning: bool,
    #[serde(default = "default_true")]
    pub block_new_cycle_below_pre_cycle_min: bool,
    #[serde(default = "default_true")]
    pub allow_cycle_below_warning_if_r2_healthy: bool,
    #[serde(default = "default_true")]
    pub require_no_unuploaded_runs_below_warning: bool,
    #[serde(default = "default_true")]
    pub aggressive_low_disk_mode_below_warning: bool,
}

impl Default for AutopilotDiskConfig {
    fn default() -> Self {
        Self {
            min_free_mb_before_cycle: default_autopilot_disk_min_free_mb_before_cycle(),
            min_free_mb_during_cycle: default_autopilot_disk_min_free_mb_during_cycle(),
            check_interval_seconds: default_autopilot_disk_check_interval_seconds(),
            warning_free_mb: default_autopilot_disk_warning_free_mb(),
            pre_cycle_min_free_mb: default_autopilot_disk_pre_cycle_min_free_mb(),
            critical_free_mb: default_autopilot_disk_critical_free_mb(),
            emergency_free_mb: default_autopilot_disk_emergency_free_mb(),
            pause_if_below_min_free: true,
            run_prune_before_cycle: true,
            run_prune_after_verified_upload: true,
            allow_prune_verified_exports: true,
            allow_prune_verified_reports: false,
            allow_prune_verified_raw_events: false,
            keep_minimal_manifest: true,
            keep_rpc_ledger: true,
            keep_calibration: true,
            close_segments_on_warning: true,
            stop_collection_on_critical: true,
            pause_before_os_error: true,
            prune_verified_segments_on_warning: true,
            prune_verified_runs_on_startup: true,
            block_new_cycle_below_warning: false,
            block_new_cycle_below_pre_cycle_min: true,
            allow_cycle_below_warning_if_r2_healthy: true,
            require_no_unuploaded_runs_below_warning: true,
            aggressive_low_disk_mode_below_warning: true,
        }
    }
}

impl AutopilotDiskConfig {
    fn apply_defaults(&mut self) {
        if self.min_free_mb_before_cycle == 0 {
            self.min_free_mb_before_cycle = default_autopilot_disk_min_free_mb_before_cycle();
        }
        if self.min_free_mb_during_cycle == 0 {
            self.min_free_mb_during_cycle = default_autopilot_disk_min_free_mb_during_cycle();
        }
        if self.check_interval_seconds == 0 {
            self.check_interval_seconds = default_autopilot_disk_check_interval_seconds();
        }
        if self.warning_free_mb == 0 {
            self.warning_free_mb = default_autopilot_disk_warning_free_mb();
        }
        if self.pre_cycle_min_free_mb == 0 {
            self.pre_cycle_min_free_mb = default_autopilot_disk_pre_cycle_min_free_mb();
        }
        if self.critical_free_mb == 0 {
            self.critical_free_mb = default_autopilot_disk_critical_free_mb();
        }
        if self.emergency_free_mb == 0 {
            self.emergency_free_mb = default_autopilot_disk_emergency_free_mb();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutopilotPostCycleCleanupConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_autopilot_post_cycle_cleanup_target_free_mb_after_cycle")]
    pub target_free_mb_after_cycle: u64,
    #[serde(default = "default_true")]
    pub run_clean_build_artifacts: bool,
    #[serde(default = "default_true")]
    pub prune_verified_exports: bool,
    #[serde(default = "default_true")]
    pub prune_verified_segments: bool,
    #[serde(default)]
    pub prune_verified_reports: bool,
    #[serde(default = "default_true")]
    pub stop_if_unable_to_reach_pre_cycle_min: bool,
    #[serde(default = "default_true")]
    pub allow_next_cycle_if_above_pre_cycle_min: bool,
}

impl Default for AutopilotPostCycleCleanupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            target_free_mb_after_cycle:
                default_autopilot_post_cycle_cleanup_target_free_mb_after_cycle(),
            run_clean_build_artifacts: true,
            prune_verified_exports: true,
            prune_verified_segments: true,
            prune_verified_reports: false,
            stop_if_unable_to_reach_pre_cycle_min: true,
            allow_next_cycle_if_above_pre_cycle_min: true,
        }
    }
}

impl AutopilotPostCycleCleanupConfig {
    fn apply_defaults(&mut self) {
        if self.target_free_mb_after_cycle == 0 {
            self.target_free_mb_after_cycle =
                default_autopilot_post_cycle_cleanup_target_free_mb_after_cycle();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutopilotBacktestThresholdsConfig {
    #[serde(default = "default_autopilot_backtest_min_duration_seconds")]
    pub min_duration_seconds: u64,
    #[serde(default = "default_autopilot_backtest_min_tokens_discovered")]
    pub min_tokens_discovered: usize,
    #[serde(default = "default_autopilot_backtest_min_complete_lifecycles")]
    pub min_complete_lifecycles: usize,
    #[serde(default = "default_autopilot_backtest_min_feature_snapshots")]
    pub min_feature_snapshots: usize,
    #[serde(default = "default_autopilot_backtest_min_decisions")]
    pub min_decisions: usize,
    #[serde(default = "default_true")]
    pub require_stream_only: bool,
    #[serde(default = "default_true")]
    pub require_zero_rpc: bool,
    #[serde(default)]
    pub max_global_data_gaps: u64,
    #[serde(default = "default_autopilot_backtest_min_holder_confidence")]
    pub min_holder_confidence: Decimal,
}

impl Default for AutopilotBacktestThresholdsConfig {
    fn default() -> Self {
        Self {
            min_duration_seconds: default_autopilot_backtest_min_duration_seconds(),
            min_tokens_discovered: default_autopilot_backtest_min_tokens_discovered(),
            min_complete_lifecycles: default_autopilot_backtest_min_complete_lifecycles(),
            min_feature_snapshots: default_autopilot_backtest_min_feature_snapshots(),
            min_decisions: default_autopilot_backtest_min_decisions(),
            require_stream_only: true,
            require_zero_rpc: true,
            max_global_data_gaps: 0,
            min_holder_confidence: default_autopilot_backtest_min_holder_confidence(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct R2Config {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub upload_enabled: bool,
    #[serde(default)]
    pub delete_enabled: bool,
    #[serde(default = "default_true")]
    pub dry_run: bool,
    #[serde(default = "default_r2_account_id_env")]
    pub account_id_env: String,
    #[serde(default = "default_r2_endpoint_env")]
    pub endpoint_env: String,
    #[serde(default = "default_r2_access_key_id_env")]
    pub access_key_id_env: String,
    #[serde(default = "default_r2_secret_access_key_env")]
    pub secret_access_key_env: String,
    #[serde(default = "default_r2_managed_prefix")]
    pub managed_prefix: String,
    #[serde(default = "default_r2_region")]
    pub region: String,
    #[serde(default = "default_true")]
    pub force_path_style: bool,
    #[serde(default = "default_r2_max_concurrent_uploads")]
    pub max_concurrent_uploads: usize,
    #[serde(default = "default_r2_multipart_threshold_mb")]
    pub multipart_threshold_mb: u64,
    #[serde(default = "default_r2_part_size_mb")]
    pub part_size_mb: u64,
    #[serde(default = "default_r2_upload_timeout_seconds")]
    pub upload_timeout_seconds: u64,
    #[serde(default = "default_true")]
    pub verify_after_upload: bool,
    #[serde(default = "default_true")]
    pub verify_checksum: bool,
    #[serde(default = "default_true")]
    pub compress_before_upload: bool,
    #[serde(default = "default_r2_compression")]
    pub compression: String,
    #[serde(default = "default_r2_encryption_mode")]
    pub encryption_mode: String,
    #[serde(default)]
    pub fail_collection_if_upload_fails: bool,
    #[serde(default = "default_true")]
    pub prune_local_only_after_verified_upload: bool,
    #[serde(default)]
    pub buckets: R2BucketsConfig,
    #[serde(default)]
    pub paths: R2PathsConfig,
    #[serde(default)]
    pub retention: R2RetentionConfig,
    #[serde(default)]
    pub autopilot: R2AutopilotConfig,
}

impl Default for R2Config {
    fn default() -> Self {
        Self {
            enabled: false,
            upload_enabled: false,
            delete_enabled: false,
            dry_run: true,
            account_id_env: default_r2_account_id_env(),
            endpoint_env: default_r2_endpoint_env(),
            access_key_id_env: default_r2_access_key_id_env(),
            secret_access_key_env: default_r2_secret_access_key_env(),
            managed_prefix: default_r2_managed_prefix(),
            region: default_r2_region(),
            force_path_style: true,
            max_concurrent_uploads: default_r2_max_concurrent_uploads(),
            multipart_threshold_mb: default_r2_multipart_threshold_mb(),
            part_size_mb: default_r2_part_size_mb(),
            upload_timeout_seconds: default_r2_upload_timeout_seconds(),
            verify_after_upload: true,
            verify_checksum: true,
            compress_before_upload: true,
            compression: default_r2_compression(),
            encryption_mode: default_r2_encryption_mode(),
            fail_collection_if_upload_fails: false,
            prune_local_only_after_verified_upload: true,
            buckets: R2BucketsConfig::default(),
            paths: R2PathsConfig::default(),
            retention: R2RetentionConfig::default(),
            autopilot: R2AutopilotConfig::default(),
        }
    }
}

impl R2Config {
    fn apply_defaults(&mut self) {
        if self.account_id_env.trim().is_empty() {
            self.account_id_env = default_r2_account_id_env();
        }
        if self.endpoint_env.trim().is_empty() {
            self.endpoint_env = default_r2_endpoint_env();
        }
        if self.access_key_id_env.trim().is_empty() {
            self.access_key_id_env = default_r2_access_key_id_env();
        }
        if self.secret_access_key_env.trim().is_empty() {
            self.secret_access_key_env = default_r2_secret_access_key_env();
        }
        if self.managed_prefix.trim().is_empty() {
            self.managed_prefix = default_r2_managed_prefix();
        }
        if self.region.trim().is_empty() {
            self.region = default_r2_region();
        }
        if self.max_concurrent_uploads == 0 {
            self.max_concurrent_uploads = default_r2_max_concurrent_uploads();
        }
        if self.multipart_threshold_mb == 0 {
            self.multipart_threshold_mb = default_r2_multipart_threshold_mb();
        }
        if self.part_size_mb == 0 {
            self.part_size_mb = default_r2_part_size_mb();
        }
        if self.upload_timeout_seconds == 0 {
            self.upload_timeout_seconds = default_r2_upload_timeout_seconds();
        }
        if self.compression.trim().is_empty() {
            self.compression = default_r2_compression();
        }
        if self.encryption_mode.trim().is_empty() {
            self.encryption_mode = default_r2_encryption_mode();
        }
        self.buckets.apply_defaults();
        self.paths.apply_defaults();
        self.retention.apply_defaults();
        self.autopilot.apply_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct R2BucketsConfig {
    #[serde(default = "default_r2_datasets_bucket_env")]
    pub datasets_bucket_env: String,
    #[serde(default = "default_r2_reports_bucket_env")]
    pub reports_bucket_env: String,
    #[serde(default = "default_r2_calibration_bucket_env")]
    pub calibration_bucket_env: String,
    #[serde(default = "default_r2_provider_compat_bucket_env")]
    pub provider_compat_bucket_env: String,
    #[serde(default)]
    pub create_if_missing: bool,
    #[serde(default)]
    pub allow_bucket_delete: bool,
    #[serde(default)]
    pub allow_bucket_empty: bool,
    #[serde(default)]
    pub allow_object_delete: bool,
    #[serde(default = "default_true")]
    pub destructive_requires_flag: bool,
}

impl Default for R2BucketsConfig {
    fn default() -> Self {
        Self {
            datasets_bucket_env: default_r2_datasets_bucket_env(),
            reports_bucket_env: default_r2_reports_bucket_env(),
            calibration_bucket_env: default_r2_calibration_bucket_env(),
            provider_compat_bucket_env: default_r2_provider_compat_bucket_env(),
            create_if_missing: false,
            allow_bucket_delete: false,
            allow_bucket_empty: false,
            allow_object_delete: false,
            destructive_requires_flag: true,
        }
    }
}

impl R2BucketsConfig {
    fn apply_defaults(&mut self) {
        if self.datasets_bucket_env.trim().is_empty() {
            self.datasets_bucket_env = default_r2_datasets_bucket_env();
        }
        if self.reports_bucket_env.trim().is_empty() {
            self.reports_bucket_env = default_r2_reports_bucket_env();
        }
        if self.calibration_bucket_env.trim().is_empty() {
            self.calibration_bucket_env = default_r2_calibration_bucket_env();
        }
        if self.provider_compat_bucket_env.trim().is_empty() {
            self.provider_compat_bucket_env = default_r2_provider_compat_bucket_env();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct R2PathsConfig {
    #[serde(default = "default_r2_runs_prefix")]
    pub runs_prefix: String,
    #[serde(default = "default_r2_reports_prefix")]
    pub reports_prefix: String,
    #[serde(default = "default_r2_exports_prefix")]
    pub exports_prefix: String,
    #[serde(default = "default_r2_calibration_prefix")]
    pub calibration_prefix: String,
    #[serde(default = "default_r2_provider_compat_prefix")]
    pub provider_compat_prefix: String,
    #[serde(default = "default_r2_rpc_ledger_prefix")]
    pub rpc_ledger_prefix: String,
    #[serde(default = "default_r2_manifests_prefix")]
    pub manifests_prefix: String,
}

impl Default for R2PathsConfig {
    fn default() -> Self {
        Self {
            runs_prefix: default_r2_runs_prefix(),
            reports_prefix: default_r2_reports_prefix(),
            exports_prefix: default_r2_exports_prefix(),
            calibration_prefix: default_r2_calibration_prefix(),
            provider_compat_prefix: default_r2_provider_compat_prefix(),
            rpc_ledger_prefix: default_r2_rpc_ledger_prefix(),
            manifests_prefix: default_r2_manifests_prefix(),
        }
    }
}

impl R2PathsConfig {
    fn apply_defaults(&mut self) {
        if self.runs_prefix.trim().is_empty() {
            self.runs_prefix = default_r2_runs_prefix();
        }
        if self.reports_prefix.trim().is_empty() {
            self.reports_prefix = default_r2_reports_prefix();
        }
        if self.exports_prefix.trim().is_empty() {
            self.exports_prefix = default_r2_exports_prefix();
        }
        if self.calibration_prefix.trim().is_empty() {
            self.calibration_prefix = default_r2_calibration_prefix();
        }
        if self.provider_compat_prefix.trim().is_empty() {
            self.provider_compat_prefix = default_r2_provider_compat_prefix();
        }
        if self.rpc_ledger_prefix.trim().is_empty() {
            self.rpc_ledger_prefix = default_r2_rpc_ledger_prefix();
        }
        if self.manifests_prefix.trim().is_empty() {
            self.manifests_prefix = default_r2_manifests_prefix();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct R2RetentionConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_r2_keep_local_after_upload_hours")]
    pub keep_local_after_upload_hours: u64,
    #[serde(default = "default_r2_keep_last_n_local_runs")]
    pub keep_last_n_local_runs: usize,
    #[serde(default = "default_true")]
    pub delete_local_exports_after_upload: bool,
    #[serde(default)]
    pub delete_local_reports_after_upload: bool,
    #[serde(default)]
    pub delete_local_raw_events_after_upload: bool,
    #[serde(default = "default_r2_max_local_storage_gb")]
    pub max_local_storage_gb: u64,
}

impl Default for R2RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            keep_local_after_upload_hours: default_r2_keep_local_after_upload_hours(),
            keep_last_n_local_runs: default_r2_keep_last_n_local_runs(),
            delete_local_exports_after_upload: true,
            delete_local_reports_after_upload: false,
            delete_local_raw_events_after_upload: false,
            max_local_storage_gb: default_r2_max_local_storage_gb(),
        }
    }
}

impl R2RetentionConfig {
    fn apply_defaults(&mut self) {
        if self.keep_local_after_upload_hours == 0 {
            self.keep_local_after_upload_hours = default_r2_keep_local_after_upload_hours();
        }
        if self.keep_last_n_local_runs == 0 {
            self.keep_last_n_local_runs = default_r2_keep_last_n_local_runs();
        }
        if self.max_local_storage_gb == 0 {
            self.max_local_storage_gb = default_r2_max_local_storage_gb();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct R2AutopilotConfig {
    #[serde(default = "default_true")]
    pub upload_after_collection: bool,
    #[serde(default = "default_true")]
    pub upload_after_research_cycle: bool,
    #[serde(default = "default_true")]
    pub upload_after_exports: bool,
    #[serde(default = "default_true")]
    pub upload_provider_smoke_reports: bool,
    #[serde(default = "default_true")]
    pub upload_rpc_ledger: bool,
    #[serde(default = "default_true")]
    pub upload_calibration: bool,
    #[serde(default = "default_true")]
    pub upload_artifact_manifest: bool,
    #[serde(default)]
    pub pause_autopilot_on_upload_failure: bool,
}

impl Default for R2AutopilotConfig {
    fn default() -> Self {
        Self {
            upload_after_collection: true,
            upload_after_research_cycle: true,
            upload_after_exports: true,
            upload_provider_smoke_reports: true,
            upload_rpc_ledger: true,
            upload_calibration: true,
            upload_artifact_manifest: true,
            pause_autopilot_on_upload_failure: false,
        }
    }
}

impl R2AutopilotConfig {
    fn apply_defaults(&mut self) {}
}

impl AutopilotBacktestThresholdsConfig {
    fn apply_defaults(&mut self) {
        if self.min_duration_seconds == 0 {
            self.min_duration_seconds = default_autopilot_backtest_min_duration_seconds();
        }
        if self.min_tokens_discovered == 0 {
            self.min_tokens_discovered = default_autopilot_backtest_min_tokens_discovered();
        }
        if self.min_complete_lifecycles == 0 {
            self.min_complete_lifecycles = default_autopilot_backtest_min_complete_lifecycles();
        }
        if self.min_feature_snapshots == 0 {
            self.min_feature_snapshots = default_autopilot_backtest_min_feature_snapshots();
        }
        if self.min_decisions == 0 {
            self.min_decisions = default_autopilot_backtest_min_decisions();
        }
        if self.min_holder_confidence <= Decimal::ZERO {
            self.min_holder_confidence = default_autopilot_backtest_min_holder_confidence();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EarlyIntentSourcePrecedenceConfig {
    #[serde(default)]
    pub deshred: i64,
    #[serde(default)]
    pub raw_shred: i64,
    #[serde(default)]
    pub fixture: i64,
    #[serde(default)]
    pub mock: i64,
    #[serde(default)]
    pub replay: i64,
}

impl Default for EarlyIntentSourcePrecedenceConfig {
    fn default() -> Self {
        Self {
            deshred: 100,
            raw_shred: 90,
            fixture: 10,
            mock: 10,
            replay: 5,
        }
    }
}

impl EarlyIntentSourcePrecedenceConfig {
    fn apply_defaults(&mut self) {
        if self.deshred == 0 {
            self.deshred = 100;
        }
        if self.raw_shred == 0 {
            self.raw_shred = 90;
        }
        if self.fixture == 0 {
            self.fixture = 10;
        }
        if self.mock == 0 {
            self.mock = 10;
        }
        if self.replay == 0 {
            self.replay = 5;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EarlyIntentMockConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub allow_in_live_data_paper: bool,
    #[serde(default)]
    pub allow_in_guarded_live: bool,
}

impl Default for EarlyIntentMockConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allow_in_live_data_paper: true,
            allow_in_guarded_live: false,
        }
    }
}

impl EarlyIntentMockConfig {
    fn apply_defaults(&mut self) {}
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DeshredConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub endpoint_env: String,
    #[serde(default)]
    pub auth_token_env: String,
    #[serde(default)]
    pub auth_metadata_key: String,
    #[serde(default)]
    pub auth_required: bool,
    #[serde(default = "default_true")]
    pub subscribe_transactions: bool,
    #[serde(default = "default_true")]
    pub program_filters_from_pump_ids: bool,
    #[serde(default)]
    pub connect_timeout_ms: u64,
    #[serde(default)]
    pub request_timeout_ms: u64,
    #[serde(default)]
    pub keepalive_interval_ms: u64,
    #[serde(default)]
    pub reconnect_backoff_ms: u64,
    #[serde(default)]
    pub max_reconnect_backoff_ms: u64,
    #[serde(default)]
    pub max_reconnect_attempts: u32,
    #[serde(default)]
    pub fail_if_unsupported: bool,
    #[serde(default)]
    pub max_decoded_message_size: usize,
}

impl DeshredConfig {
    fn apply_defaults(&mut self) {
        if self.endpoint_env.is_empty() {
            self.endpoint_env = "GEYSER_ENDPOINT".to_owned();
        }
        if self.auth_token_env.is_empty() {
            self.auth_token_env = "GEYSER_AUTH_TOKEN".to_owned();
        }
        if self.auth_metadata_key.is_empty() {
            self.auth_metadata_key = "x-token".to_owned();
        }
        if self.connect_timeout_ms == 0 {
            self.connect_timeout_ms = 10_000;
        }
        if self.request_timeout_ms == 0 {
            self.request_timeout_ms = 30_000;
        }
        if self.keepalive_interval_ms == 0 {
            self.keepalive_interval_ms = 10_000;
        }
        if self.reconnect_backoff_ms == 0 {
            self.reconnect_backoff_ms = 1_000;
        }
        if self.max_reconnect_backoff_ms == 0 {
            self.max_reconnect_backoff_ms = 30_000;
        }
        if self.max_reconnect_attempts == 0 {
            self.max_reconnect_attempts = 10;
        }
        if self.max_decoded_message_size == 0 {
            self.max_decoded_message_size = 64 * 1024 * 1024;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommitmentMode {
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeyserConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub required: bool,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub endpoint_env: String,
    #[serde(default)]
    pub auth_token_env: String,
    #[serde(default)]
    pub auth_metadata_key: String,
    #[serde(default)]
    pub auth_metadata_value_env: String,
    #[serde(default)]
    pub auth_required: bool,
    #[serde(default)]
    pub connect_timeout_ms: u64,
    #[serde(default)]
    pub request_timeout_ms: u64,
    #[serde(default)]
    pub keepalive_interval_ms: u64,
    pub commitment: CommitmentMode,
    #[serde(default)]
    pub reconnect_backoff_ms: Vec<u64>,
    #[serde(default)]
    pub max_reconnect_backoff_ms: u64,
    #[serde(default)]
    pub max_reconnect_attempts: Option<u32>,
    #[serde(default = "default_true")]
    pub subscribe_transactions: bool,
    #[serde(default = "default_true")]
    pub subscribe_accounts: bool,
    #[serde(default = "default_true")]
    pub subscribe_slots: bool,
    #[serde(default)]
    pub subscribe_blocks: bool,
    #[serde(default)]
    pub subscribe_blocks_meta: bool,
    #[serde(default)]
    pub ping_interval_ms: u64,
    #[serde(default)]
    pub max_decoded_message_size: usize,
    #[serde(default = "default_true")]
    pub geyser_only_allowed: bool,
    pub max_inflight_messages: usize,
    pub slot_gap_tolerance: u64,
    #[serde(default)]
    pub program_filters: Vec<String>,
    #[serde(default)]
    pub account_filters: Vec<String>,
}

impl GeyserConfig {
    fn apply_defaults(&mut self) {
        if self.endpoint_env.is_empty() {
            self.endpoint_env = "GEYSER_ENDPOINT".to_owned();
        }
        if self.connect_timeout_ms == 0 {
            self.connect_timeout_ms = 10_000;
        }
        if self.request_timeout_ms == 0 {
            self.request_timeout_ms = 30_000;
        }
        if self.keepalive_interval_ms == 0 {
            self.keepalive_interval_ms = 10_000;
        }
        if self.reconnect_backoff_ms.is_empty() {
            self.reconnect_backoff_ms = vec![250, 500, 1_000, 2_000, 5_000];
        }
        if self.max_reconnect_backoff_ms == 0 {
            self.max_reconnect_backoff_ms = 30_000;
        }
        if self.max_reconnect_attempts.is_none() {
            self.max_reconnect_attempts = Some(10);
        }
        if self.ping_interval_ms == 0 {
            self.ping_interval_ms = 15_000;
        }
        if self.max_inflight_messages == 0 {
            self.max_inflight_messages = 8_192;
        }
        if self.max_decoded_message_size == 0 {
            self.max_decoded_message_size = 64 * 1024 * 1024;
        }
        if self.auth_metadata_key.is_empty() && !self.auth_token_env.is_empty() {
            self.auth_metadata_key = "x-token".to_owned();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShredConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub decoder: ShredDecoderMode,
    #[serde(default = "default_true")]
    pub allow_geyser_only_fallback: bool,
    #[serde(alias = "udp_bind")]
    pub bind_addr: String,
    pub max_packet_size: usize,
    pub dedup_window: usize,
    pub tentative_ttl_ms: u64,
}

impl ShredConfig {
    fn apply_defaults(&mut self) {
        if self.max_packet_size == 0 {
            self.max_packet_size = 1_500;
        }
        if self.dedup_window == 0 {
            self.dedup_window = 8_192;
        }
        if self.tentative_ttl_ms == 0 {
            self.tentative_ttl_ms = 4_000;
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ShredDecoderMode {
    #[default]
    Fixture,
    Production,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpProgramConfig {
    #[serde(default)]
    pub program_ids: Vec<String>,
    #[serde(default)]
    pub pump_swap_program_ids: Vec<String>,
    #[serde(default)]
    pub idl_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub root: String,
    pub event_log_dir: String,
    pub snapshot_dir: String,
    pub report_dir: String,
    #[serde(default)]
    pub run_metadata_log_path: String,
    #[serde(default)]
    pub decision_log_path: String,
    #[serde(default)]
    pub fill_log_path: String,
    #[serde(default)]
    pub runtime_audit_log_path: String,
    #[serde(default)]
    pub segments: StorageSegmentsConfig,
    #[serde(default)]
    pub local: StorageLocalConfig,
    #[serde(default)]
    pub low_disk_mode: StorageLowDiskModeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSegmentsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_storage_segment_max_segment_size_mb")]
    pub max_segment_size_mb: u64,
    #[serde(default = "default_storage_segment_max_segment_age_seconds")]
    pub max_segment_age_seconds: u64,
    #[serde(default = "default_true")]
    pub compress_closed_segments: bool,
    #[serde(default = "default_storage_segment_compression")]
    pub compression: String,
    #[serde(default = "default_true")]
    pub upload_closed_segments: bool,
    #[serde(default = "default_true")]
    pub verify_before_local_delete: bool,
    #[serde(default = "default_true")]
    pub delete_verified_segments: bool,
    #[serde(default = "default_storage_segment_keep_last_n_segments_local")]
    pub keep_last_n_segments_local: usize,
    #[serde(default = "default_storage_segment_manifest_path")]
    pub segment_manifest_path: String,
    #[serde(default)]
    pub upload: StorageSegmentUploadConfig,
    #[serde(default)]
    pub finalization: StorageSegmentFinalizationConfig,
}

impl Default for StorageSegmentsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_segment_size_mb: default_storage_segment_max_segment_size_mb(),
            max_segment_age_seconds: default_storage_segment_max_segment_age_seconds(),
            compress_closed_segments: true,
            compression: default_storage_segment_compression(),
            upload_closed_segments: true,
            verify_before_local_delete: true,
            delete_verified_segments: true,
            keep_last_n_segments_local: default_storage_segment_keep_last_n_segments_local(),
            segment_manifest_path: default_storage_segment_manifest_path(),
            upload: StorageSegmentUploadConfig::default(),
            finalization: StorageSegmentFinalizationConfig::default(),
        }
    }
}

impl StorageSegmentsConfig {
    fn apply_defaults(&mut self) {
        if self.max_segment_size_mb == 0 {
            self.max_segment_size_mb = default_storage_segment_max_segment_size_mb();
        }
        if self.max_segment_age_seconds == 0 {
            self.max_segment_age_seconds = default_storage_segment_max_segment_age_seconds();
        }
        if self.compression.trim().is_empty() {
            self.compression = default_storage_segment_compression();
        }
        if self.keep_last_n_segments_local == 0 {
            self.keep_last_n_segments_local = default_storage_segment_keep_last_n_segments_local();
        }
        if self.segment_manifest_path.trim().is_empty() {
            self.segment_manifest_path = default_storage_segment_manifest_path();
        }
        self.upload.apply_defaults();
        self.finalization.apply_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSegmentUploadConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub upload_closed_segments_during_run: bool,
    #[serde(default = "default_true")]
    pub verify_closed_segments: bool,
    #[serde(default = "default_true")]
    pub delete_verified_closed_segments: bool,
    #[serde(default = "default_storage_segment_max_concurrent_uploads")]
    pub max_concurrent_segment_uploads: usize,
    #[serde(default = "default_storage_segment_max_pending_segments")]
    pub max_pending_segments: usize,
    #[serde(default = "default_storage_segment_max_pending_segments_warning")]
    pub max_pending_segments_warning: usize,
    #[serde(default = "default_storage_segment_max_pending_segments_pause")]
    pub max_pending_segments_pause: usize,
    #[serde(default = "default_storage_segment_max_pending_segments_stop")]
    pub max_pending_segments_stop: usize,
    #[serde(default = "default_storage_segment_pause_pending_segments")]
    pub pause_collection_if_pending_segments_exceed: usize,
    #[serde(default = "default_true")]
    pub pause_new_token_tracking_on_backlog: bool,
    #[serde(default)]
    pub pause_feature_snapshot_writes_on_backlog: bool,
    #[serde(default = "default_true")]
    pub force_close_and_upload_on_backlog: bool,
    #[serde(default = "default_true")]
    pub retry_failed_uploads: bool,
    #[serde(default = "default_storage_segment_max_upload_retries")]
    pub max_upload_retries: u64,
    #[serde(default = "default_storage_segment_retry_backoff_seconds")]
    pub retry_backoff_seconds: u64,
    #[serde(default = "default_true")]
    pub alert_on_upload_backlog: bool,
}

impl Default for StorageSegmentUploadConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            upload_closed_segments_during_run: true,
            verify_closed_segments: true,
            delete_verified_closed_segments: true,
            max_concurrent_segment_uploads: default_storage_segment_max_concurrent_uploads(),
            max_pending_segments: default_storage_segment_max_pending_segments(),
            max_pending_segments_warning: default_storage_segment_max_pending_segments_warning(),
            max_pending_segments_pause: default_storage_segment_max_pending_segments_pause(),
            max_pending_segments_stop: default_storage_segment_max_pending_segments_stop(),
            pause_collection_if_pending_segments_exceed:
                default_storage_segment_pause_pending_segments(),
            pause_new_token_tracking_on_backlog: true,
            pause_feature_snapshot_writes_on_backlog: false,
            force_close_and_upload_on_backlog: true,
            retry_failed_uploads: true,
            max_upload_retries: default_storage_segment_max_upload_retries(),
            retry_backoff_seconds: default_storage_segment_retry_backoff_seconds(),
            alert_on_upload_backlog: true,
        }
    }
}

impl StorageSegmentUploadConfig {
    fn apply_defaults(&mut self) {
        if self.max_concurrent_segment_uploads == 0 {
            self.max_concurrent_segment_uploads = default_storage_segment_max_concurrent_uploads();
        }
        if self.max_pending_segments == 0 {
            self.max_pending_segments = default_storage_segment_max_pending_segments();
        }
        if self.max_pending_segments_warning == 0 {
            self.max_pending_segments_warning =
                default_storage_segment_max_pending_segments_warning();
        }
        if self.max_pending_segments_pause == 0 {
            self.max_pending_segments_pause = default_storage_segment_max_pending_segments_pause();
        }
        if self.max_pending_segments_stop == 0 {
            self.max_pending_segments_stop = default_storage_segment_max_pending_segments_stop();
        }
        if self.pause_collection_if_pending_segments_exceed == 0 {
            self.pause_collection_if_pending_segments_exceed =
                default_storage_segment_pause_pending_segments();
        }
        if self.pause_collection_if_pending_segments_exceed < self.max_pending_segments_stop {
            self.pause_collection_if_pending_segments_exceed = self.max_pending_segments_stop;
        }
        if self.max_upload_retries == 0 {
            self.max_upload_retries = default_storage_segment_max_upload_retries();
        }
        if self.retry_backoff_seconds == 0 {
            self.retry_backoff_seconds = default_storage_segment_retry_backoff_seconds();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSegmentFinalizationConfig {
    #[serde(default = "default_true")]
    pub drain_pending_segments_on_finalize: bool,
    #[serde(default = "default_storage_segment_max_finalize_wait_seconds")]
    pub max_finalize_segment_upload_wait_seconds: u64,
    #[serde(default = "default_storage_segment_max_finalize_upload_retries")]
    pub max_finalize_upload_retries: u64,
    #[serde(default = "default_true")]
    pub allow_partial_manifest: bool,
    #[serde(default = "default_true")]
    pub mark_partial_if_pending_segments: bool,
    #[serde(default = "default_true")]
    pub fail_finalization_if_manifest_missing: bool,
}

impl Default for StorageSegmentFinalizationConfig {
    fn default() -> Self {
        Self {
            drain_pending_segments_on_finalize: true,
            max_finalize_segment_upload_wait_seconds:
                default_storage_segment_max_finalize_wait_seconds(),
            max_finalize_upload_retries: default_storage_segment_max_finalize_upload_retries(),
            allow_partial_manifest: true,
            mark_partial_if_pending_segments: true,
            fail_finalization_if_manifest_missing: true,
        }
    }
}

impl StorageSegmentFinalizationConfig {
    fn apply_defaults(&mut self) {
        if self.max_finalize_segment_upload_wait_seconds == 0 {
            self.max_finalize_segment_upload_wait_seconds =
                default_storage_segment_max_finalize_wait_seconds();
        }
        if self.max_finalize_upload_retries == 0 {
            self.max_finalize_upload_retries =
                default_storage_segment_max_finalize_upload_retries();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageLocalConfig {
    #[serde(default = "default_true")]
    pub report_lite_after_upload: bool,
    #[serde(default = "default_true")]
    pub compress_reports_before_upload: bool,
    #[serde(default = "default_true")]
    pub delete_uncompressed_exports_after_upload: bool,
    #[serde(default = "default_true")]
    pub keep_local_markdown_summaries: bool,
    #[serde(default = "default_true")]
    pub keep_local_json_summaries: bool,
}

impl Default for StorageLocalConfig {
    fn default() -> Self {
        Self {
            report_lite_after_upload: true,
            compress_reports_before_upload: true,
            delete_uncompressed_exports_after_upload: true,
            keep_local_markdown_summaries: true,
            keep_local_json_summaries: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageLowDiskModeConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_storage_low_disk_target_local_runtime_mb")]
    pub target_local_runtime_mb: u64,
    #[serde(default = "default_storage_low_disk_warning_free_mb")]
    pub warning_free_mb: u64,
    #[serde(default = "default_storage_low_disk_critical_free_mb")]
    pub critical_free_mb: u64,
    #[serde(default = "default_storage_low_disk_emergency_free_mb")]
    pub emergency_free_mb: u64,
    #[serde(default = "default_storage_low_disk_max_local_verified_segments")]
    pub max_local_verified_segments: usize,
    #[serde(default = "default_storage_low_disk_max_local_unverified_closed_segments")]
    pub max_local_unverified_closed_segments: usize,
    #[serde(default = "default_storage_low_disk_max_local_export_mb")]
    pub max_local_export_mb: u64,
    #[serde(default = "default_true")]
    pub forbid_monolithic_feature_export: bool,
    #[serde(default = "default_true")]
    pub forbid_monolithic_large_exports: bool,
    #[serde(default = "default_true")]
    pub lite_reports_after_upload: bool,
    #[serde(default = "default_true")]
    pub remote_first_analysis: bool,
    #[serde(default = "default_true")]
    pub remote_first_exports: bool,
    #[serde(default = "default_true")]
    pub delete_verified_segments_immediately: bool,
    #[serde(default = "default_true")]
    pub delete_verified_exports_immediately: bool,
    #[serde(default = "default_true")]
    pub keep_open_segments_only: bool,
    #[serde(default = "default_true")]
    pub keep_small_summaries_local: bool,
    #[serde(default = "default_true")]
    pub keep_rpc_ledger_local: bool,
    #[serde(default = "default_true")]
    pub keep_calibration_local: bool,
    #[serde(default = "default_true")]
    pub keep_manifest_local: bool,
}

impl Default for StorageLowDiskModeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            target_local_runtime_mb: default_storage_low_disk_target_local_runtime_mb(),
            warning_free_mb: default_storage_low_disk_warning_free_mb(),
            critical_free_mb: default_storage_low_disk_critical_free_mb(),
            emergency_free_mb: default_storage_low_disk_emergency_free_mb(),
            max_local_verified_segments: default_storage_low_disk_max_local_verified_segments(),
            max_local_unverified_closed_segments:
                default_storage_low_disk_max_local_unverified_closed_segments(),
            max_local_export_mb: default_storage_low_disk_max_local_export_mb(),
            forbid_monolithic_feature_export: true,
            forbid_monolithic_large_exports: true,
            lite_reports_after_upload: true,
            remote_first_analysis: true,
            remote_first_exports: true,
            delete_verified_segments_immediately: true,
            delete_verified_exports_immediately: true,
            keep_open_segments_only: true,
            keep_small_summaries_local: true,
            keep_rpc_ledger_local: true,
            keep_calibration_local: true,
            keep_manifest_local: true,
        }
    }
}

impl StorageLowDiskModeConfig {
    fn apply_defaults(&mut self) {
        if self.target_local_runtime_mb == 0 {
            self.target_local_runtime_mb = default_storage_low_disk_target_local_runtime_mb();
        }
        if self.warning_free_mb == 0 {
            self.warning_free_mb = default_storage_low_disk_warning_free_mb();
        }
        if self.critical_free_mb == 0 {
            self.critical_free_mb = default_storage_low_disk_critical_free_mb();
        }
        if self.emergency_free_mb == 0 {
            self.emergency_free_mb = default_storage_low_disk_emergency_free_mb();
        }
        if self.max_local_verified_segments == 0 {
            self.max_local_verified_segments =
                default_storage_low_disk_max_local_verified_segments();
        }
        if self.max_local_unverified_closed_segments == 0 {
            self.max_local_unverified_closed_segments =
                default_storage_low_disk_max_local_unverified_closed_segments();
        }
        if self.max_local_export_mb == 0 {
            self.max_local_export_mb = default_storage_low_disk_max_local_export_mb();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportsConfig {
    #[serde(default = "default_true")]
    pub chunked_enabled: bool,
    #[serde(default = "default_exports_max_uncompressed_export_mb")]
    pub max_uncompressed_export_mb: u64,
    #[serde(default = "default_exports_feature_export_chunk_rows")]
    pub feature_export_chunk_rows: usize,
    #[serde(default = "default_true")]
    pub compress_exports: bool,
    #[serde(default = "default_exports_compression")]
    pub compression: String,
    #[serde(default = "default_true")]
    pub upload_export_chunks_to_r2: bool,
    #[serde(default = "default_true")]
    pub verify_export_chunks: bool,
    #[serde(default = "default_true")]
    pub delete_local_verified_export_chunks: bool,
    #[serde(default = "default_true")]
    pub prevent_monolithic_large_exports: bool,
}

impl Default for ExportsConfig {
    fn default() -> Self {
        Self {
            chunked_enabled: true,
            max_uncompressed_export_mb: default_exports_max_uncompressed_export_mb(),
            feature_export_chunk_rows: default_exports_feature_export_chunk_rows(),
            compress_exports: true,
            compression: default_exports_compression(),
            upload_export_chunks_to_r2: true,
            verify_export_chunks: true,
            delete_local_verified_export_chunks: true,
            prevent_monolithic_large_exports: true,
        }
    }
}

impl ExportsConfig {
    fn apply_defaults(&mut self) {
        if self.max_uncompressed_export_mb == 0 {
            self.max_uncompressed_export_mb = default_exports_max_uncompressed_export_mb();
        }
        if self.feature_export_chunk_rows == 0 {
            self.feature_export_chunk_rows = default_exports_feature_export_chunk_rows();
        }
        if self.compression.trim().is_empty() {
            self.compression = default_exports_compression();
        }
    }
}

impl StorageConfig {
    fn apply_defaults(&mut self) {
        if self.run_metadata_log_path.is_empty() {
            self.run_metadata_log_path = "runs.jsonl".to_owned();
        }
        if self.decision_log_path.is_empty() {
            self.decision_log_path = "decisions.jsonl".to_owned();
        }
        if self.fill_log_path.is_empty() {
            self.fill_log_path = "fills.jsonl".to_owned();
        }
        if self.runtime_audit_log_path.is_empty() {
            self.runtime_audit_log_path = "runtime_audit.jsonl".to_owned();
        }
        self.segments.apply_defaults();
        self.low_disk_mode.apply_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    #[serde(alias = "bind")]
    pub bind_addr: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub enable_prometheus: bool,
    pub json_tracing: bool,
    #[serde(default = "default_true")]
    pub enable_healthz: bool,
    #[serde(default = "default_true")]
    pub enable_readyz: bool,
    pub service_name: String,
}

impl MetricsConfig {
    fn apply_defaults(&mut self) {
        if self.bind_addr.is_empty() {
            self.bind_addr = "127.0.0.1:9898".to_owned();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReportsConfig {
    #[serde(default = "default_true")]
    pub auto_generate: bool,
    #[serde(default = "default_true")]
    pub include_strategy_summary: bool,
    #[serde(default = "default_true")]
    pub include_edge_calibration: bool,
    #[serde(default = "default_true")]
    pub include_data_gaps: bool,
    #[serde(default = "default_true")]
    pub include_top_tokens: bool,
    #[serde(default)]
    pub low_disk: ReportsLowDiskConfig,
}

impl ReportsConfig {
    fn apply_defaults(&mut self) {
        self.low_disk.apply_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportsLowDiskConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_reports_low_disk_max_local_report_mb")]
    pub max_local_report_mb: u64,
    #[serde(default = "default_true")]
    pub remote_full_reports: bool,
    #[serde(default = "default_true")]
    pub compress_full_reports: bool,
    #[serde(default = "default_true")]
    pub delete_local_full_reports_after_verified_upload: bool,
}

impl Default for ReportsLowDiskConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_local_report_mb: default_reports_low_disk_max_local_report_mb(),
            remote_full_reports: true,
            compress_full_reports: true,
            delete_local_full_reports_after_verified_upload: true,
        }
    }
}

impl ReportsLowDiskConfig {
    fn apply_defaults(&mut self) {
        if self.max_local_report_mb == 0 {
            self.max_local_report_mb = default_reports_low_disk_max_local_report_mb();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AnalysisConfig {
    #[serde(default)]
    pub remote: AnalysisRemoteConfig,
}

impl AnalysisConfig {
    fn apply_defaults(&mut self) {
        self.remote.apply_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisRemoteConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_analysis_remote_max_restore_mb")]
    pub max_restore_mb: u64,
    #[serde(default = "default_true")]
    pub delete_temp_after_segment: bool,
    #[serde(default = "default_true")]
    pub stream_segments_one_at_a_time: bool,
    #[serde(default)]
    pub allow_full_restore: bool,
}

impl Default for AnalysisRemoteConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_restore_mb: default_analysis_remote_max_restore_mb(),
            delete_temp_after_segment: true,
            stream_segments_one_at_a_time: true,
            allow_full_restore: false,
        }
    }
}

impl AnalysisRemoteConfig {
    fn apply_defaults(&mut self) {
        if self.max_restore_mb == 0 {
            self.max_restore_mb = default_analysis_remote_max_restore_mb();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub enabled: bool,
    pub live_enabled: bool,
    pub paper_enabled: bool,
    #[serde(default = "default_true")]
    pub dry_run: bool,
    #[serde(default)]
    pub use_rpc_send: bool,
    #[serde(default = "default_true")]
    pub use_stream_blockhash_cache: bool,
    pub max_daily_loss_quote: Decimal,
    pub max_open_positions: usize,
    #[serde(default)]
    pub max_position_size_quote: Decimal,
    pub max_trades_per_minute: u64,
    pub max_fee_quote: Decimal,
    pub max_slippage_bps: u64,
}

impl ExecutionConfig {
    fn apply_defaults(&mut self) {
        if self.max_position_size_quote <= Decimal::ZERO {
            self.max_position_size_quote = Decimal::ONE;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PaperConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub initial_balance_quote: Decimal,
    #[serde(default)]
    pub max_positions: usize,
    #[serde(default)]
    pub max_position_size_quote: Decimal,
    #[serde(default)]
    pub max_daily_loss_quote: Decimal,
    #[serde(default)]
    pub close_on_shutdown: bool,
    #[serde(default = "default_true")]
    pub scenario_end_liquidation: bool,
}

impl PaperConfig {
    fn apply_defaults(&mut self) {
        if self.initial_balance_quote <= Decimal::ZERO {
            self.initial_balance_quote = Decimal::from(10_000u64);
        }
        if self.max_positions == 0 {
            self.max_positions = 3;
        }
        if self.max_position_size_quote <= Decimal::ZERO {
            self.max_position_size_quote = Decimal::from(1u64);
        }
        if self.max_daily_loss_quote <= Decimal::ZERO {
            self.max_daily_loss_quote = Decimal::from(500u64);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LiveConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub dry_run: bool,
    #[serde(default = "default_true")]
    pub require_explicit_enable: bool,
    #[serde(default)]
    pub explicit_send_confirmation: bool,
    #[serde(default)]
    pub max_fee_per_transaction_quote: Decimal,
    #[serde(default)]
    pub max_priority_fee_lamports: u64,
    #[serde(default)]
    pub max_tip_lamports: u64,
    #[serde(default)]
    pub max_slippage_bps: u64,
    #[serde(default)]
    pub allow_shred_emergency_exit: bool,
    #[serde(default)]
    pub allow_live_exit_on_tentative: bool,
}

impl LiveConfig {
    fn apply_defaults(&mut self) {
        if self.max_fee_per_transaction_quote <= Decimal::ZERO {
            self.max_fee_per_transaction_quote = Decimal::new(25, 2);
        }
        if self.max_slippage_bps == 0 {
            self.max_slippage_bps = 300;
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ShredExitConfirmationLevel {
    Tentative,
    #[default]
    GeyserProcessedTxSeen,
    AccountEffectsObserved,
    ConfirmedExecuted,
    RootedExecuted,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShredExitCalibrationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub paper_only: bool,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub min_samples_before_adapt: u64,
    #[serde(default)]
    pub max_threshold_adjustment_per_hour: Decimal,
    #[serde(default)]
    pub false_positive_rate_limit: Decimal,
    #[serde(default)]
    pub missed_sell_rate_limit: Decimal,
    #[serde(default = "default_true")]
    pub persist_calibration: bool,
    #[serde(default)]
    pub max_live_threshold_adjustment: Decimal,
}

impl ShredExitCalibrationConfig {
    fn apply_defaults(&mut self) {
        if self.path.is_empty() {
            self.path = "data/calibration/shred_exit_calibration.json".to_owned();
        }
        if self.min_samples_before_adapt == 0 {
            self.min_samples_before_adapt = 50;
        }
        if self.max_threshold_adjustment_per_hour <= Decimal::ZERO {
            self.max_threshold_adjustment_per_hour = Decimal::new(5, 2);
        }
        if self.false_positive_rate_limit <= Decimal::ZERO {
            self.false_positive_rate_limit = Decimal::new(10, 2);
        }
        if self.missed_sell_rate_limit <= Decimal::ZERO {
            self.missed_sell_rate_limit = Decimal::new(5, 2);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShredExitConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub paper_enabled: bool,
    #[serde(default)]
    pub live_enabled: bool,
    #[serde(default)]
    pub min_decode_confidence_warn: Decimal,
    #[serde(default)]
    pub min_decode_confidence_arm: Decimal,
    #[serde(default)]
    pub min_decode_confidence_paper_exit: Decimal,
    #[serde(default)]
    pub min_decode_confidence_live_exit: Decimal,
    #[serde(default)]
    pub warn_impact_pct: Decimal,
    #[serde(default)]
    pub arm_impact_pct: Decimal,
    #[serde(default)]
    pub paper_exit_impact_pct: Decimal,
    #[serde(default)]
    pub live_exit_impact_pct: Decimal,
    #[serde(default)]
    pub min_expected_saved_loss_quote: Decimal,
    #[serde(default)]
    pub min_emergency_exit_net_benefit_quote: Decimal,
    #[serde(default)]
    pub min_latency_edge_ratio: Decimal,
    #[serde(default = "default_true")]
    pub allow_arm_when_net_benefit_negative: bool,
    #[serde(default)]
    pub allow_emergency_when_net_benefit_negative: bool,
    #[serde(default = "default_true")]
    pub absorption_downgrade_enabled: bool,
    #[serde(default)]
    pub absorption_downgrade_threshold: Decimal,
    #[serde(default)]
    pub dev_sell_exit_threshold_pct: Decimal,
    #[serde(default)]
    pub top_holder_exit_threshold_pct: Decimal,
    #[serde(default)]
    pub bundle_cluster_exit_threshold_pct: Decimal,
    #[serde(default = "default_true")]
    pub same_slot_multi_sell_escalation: bool,
    #[serde(default)]
    pub max_tentative_event_age_ms: u64,
    #[serde(default)]
    pub tentative_reconciliation_ttl_ms: u64,
    #[serde(default = "default_true")]
    pub false_positive_penalty_enabled: bool,
    #[serde(default = "default_true")]
    pub tighten_stop_on_arm: bool,
    #[serde(default)]
    pub prebuild_live_exit_on_arm: bool,
    #[serde(default = "default_true")]
    pub allow_paper_exit_on_tentative: bool,
    #[serde(default)]
    pub allow_live_exit_on_tentative: bool,
    #[serde(default)]
    pub required_confirmation_level_for_live_exit: ShredExitConfirmationLevel,
    #[serde(default = "default_true")]
    pub prefer_account_effect_confirmation: bool,
    #[serde(default)]
    pub calibration: ShredExitCalibrationConfig,
}

impl ShredExitConfig {
    fn apply_defaults(&mut self) {
        if self.min_decode_confidence_warn <= Decimal::ZERO {
            self.min_decode_confidence_warn = Decimal::new(55, 2);
        }
        if self.min_decode_confidence_arm <= Decimal::ZERO {
            self.min_decode_confidence_arm = Decimal::new(70, 2);
        }
        if self.min_decode_confidence_paper_exit <= Decimal::ZERO {
            self.min_decode_confidence_paper_exit = Decimal::new(82, 2);
        }
        if self.min_decode_confidence_live_exit <= Decimal::ZERO {
            self.min_decode_confidence_live_exit = Decimal::new(95, 2);
        }
        if self.warn_impact_pct <= Decimal::ZERO {
            self.warn_impact_pct = Decimal::from(8u64);
        }
        if self.arm_impact_pct <= Decimal::ZERO {
            self.arm_impact_pct = Decimal::from(15u64);
        }
        if self.paper_exit_impact_pct <= Decimal::ZERO {
            self.paper_exit_impact_pct = Decimal::from(25u64);
        }
        if self.live_exit_impact_pct <= Decimal::ZERO {
            self.live_exit_impact_pct = Decimal::from(35u64);
        }
        if self.min_expected_saved_loss_quote <= Decimal::ZERO {
            self.min_expected_saved_loss_quote = Decimal::new(2, 2);
        }
        if self.min_emergency_exit_net_benefit_quote <= Decimal::ZERO {
            self.min_emergency_exit_net_benefit_quote = Decimal::new(1, 2);
        }
        if self.min_latency_edge_ratio <= Decimal::ZERO {
            self.min_latency_edge_ratio = Decimal::new(11, 1);
        }
        if self.absorption_downgrade_threshold <= Decimal::ZERO {
            self.absorption_downgrade_threshold = Decimal::new(75, 2);
        }
        if self.dev_sell_exit_threshold_pct <= Decimal::ZERO {
            self.dev_sell_exit_threshold_pct = Decimal::from(5u64);
        }
        if self.top_holder_exit_threshold_pct <= Decimal::ZERO {
            self.top_holder_exit_threshold_pct = Decimal::from(10u64);
        }
        if self.bundle_cluster_exit_threshold_pct <= Decimal::ZERO {
            self.bundle_cluster_exit_threshold_pct = Decimal::from(12u64);
        }
        if self.max_tentative_event_age_ms == 0 {
            self.max_tentative_event_age_ms = 750;
        }
        if self.tentative_reconciliation_ttl_ms == 0 {
            self.tentative_reconciliation_ttl_ms = 5_000;
        }
        self.calibration.apply_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EdgeConfig {
    #[serde(default)]
    pub min_expected_net_edge_quote: Decimal,
    #[serde(default)]
    pub min_expected_net_edge_pct: Decimal,
    #[serde(default)]
    pub min_edge_confidence: Decimal,
    #[serde(default)]
    pub fee_safety_margin_multiplier: Decimal,
    #[serde(default)]
    pub latency_safety_margin_multiplier: Decimal,
    #[serde(default = "default_true")]
    pub allow_watch_without_trade: bool,
}

impl EdgeConfig {
    fn apply_defaults(&mut self) {
        if self.min_expected_net_edge_quote <= Decimal::ZERO {
            self.min_expected_net_edge_quote = Decimal::new(2, 2);
        }
        if self.min_expected_net_edge_pct <= Decimal::ZERO {
            self.min_expected_net_edge_pct = Decimal::new(10, 2);
        }
        if self.min_edge_confidence <= Decimal::ZERO {
            self.min_edge_confidence = Decimal::new(35, 2);
        }
        if self.fee_safety_margin_multiplier <= Decimal::ZERO {
            self.fee_safety_margin_multiplier = Decimal::new(12, 1);
        }
        if self.latency_safety_margin_multiplier <= Decimal::ZERO {
            self.latency_safety_margin_multiplier = Decimal::new(12, 1);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StrategyProfileConfig {
    #[serde(default)]
    pub take_profit_pct: Decimal,
    #[serde(default)]
    pub trailing_stop_pct: Decimal,
    #[serde(default)]
    pub stop_loss_pct: Decimal,
    #[serde(default)]
    pub max_hold_ms: u64,
}

impl StrategyProfileConfig {
    fn apply_defaults_with(
        &mut self,
        take_profit_pct: Decimal,
        trailing_stop_pct: Decimal,
        stop_loss_pct: Decimal,
        max_hold_ms: u64,
    ) {
        if self.take_profit_pct <= Decimal::ZERO {
            self.take_profit_pct = take_profit_pct;
        }
        if self.trailing_stop_pct <= Decimal::ZERO {
            self.trailing_stop_pct = trailing_stop_pct;
        }
        if self.stop_loss_pct <= Decimal::ZERO {
            self.stop_loss_pct = stop_loss_pct;
        }
        if self.max_hold_ms == 0 {
            self.max_hold_ms = max_hold_ms;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlConfig {
    pub discovered_secs: u64,
    pub active_light_secs: u64,
    pub active_deep_secs: u64,
    pub discarded_summary_secs: u64,
    pub research_sample_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StrategyThresholds {
    pub min_fee_adjusted_edge_bps: u64,
    pub min_data_quality_score: Decimal,
    pub max_bundle_risk: Decimal,
    pub max_rug_risk: Decimal,
    pub max_fake_momentum_risk: Decimal,
    pub min_holder_growth: Decimal,
    #[serde(default)]
    pub min_trade_eligibility_score: Decimal,
    #[serde(default)]
    pub min_holder_stickiness_score: Decimal,
    #[serde(default)]
    pub min_momentum_authenticity_score: Decimal,
    #[serde(default)]
    pub max_profit_overhang_score: Decimal,
    #[serde(default)]
    pub launch_momentum: StrategyProfileConfig,
    #[serde(default)]
    pub holder_growth: StrategyProfileConfig,
    #[serde(default)]
    pub absorption_bounce: StrategyProfileConfig,
    #[serde(default)]
    pub smart_rotation: StrategyProfileConfig,
    #[serde(default)]
    pub organic_slow_grind: StrategyProfileConfig,
    #[serde(default)]
    pub exit_on_shred: StrategyExitOnShredConfig,
}

impl StrategyThresholds {
    fn apply_defaults(&mut self) {
        if self.min_trade_eligibility_score <= Decimal::ZERO {
            self.min_trade_eligibility_score = Decimal::new(60, 2);
        }
        if self.min_holder_stickiness_score <= Decimal::ZERO {
            self.min_holder_stickiness_score = Decimal::new(25, 2);
        }
        if self.min_momentum_authenticity_score <= Decimal::ZERO {
            self.min_momentum_authenticity_score = Decimal::new(40, 2);
        }
        if self.max_profit_overhang_score <= Decimal::ZERO {
            self.max_profit_overhang_score = Decimal::new(70, 2);
        }
        self.launch_momentum.apply_defaults_with(
            Decimal::new(18, 2),
            Decimal::new(8, 2),
            Decimal::new(9, 2),
            20_000,
        );
        self.holder_growth.apply_defaults_with(
            Decimal::new(28, 2),
            Decimal::new(12, 2),
            Decimal::new(12, 2),
            90_000,
        );
        self.absorption_bounce.apply_defaults_with(
            Decimal::new(14, 2),
            Decimal::new(7, 2),
            Decimal::new(8, 2),
            35_000,
        );
        self.smart_rotation.apply_defaults_with(
            Decimal::new(20, 2),
            Decimal::new(10, 2),
            Decimal::new(10, 2),
            60_000,
        );
        self.organic_slow_grind.apply_defaults_with(
            Decimal::new(24, 2),
            Decimal::new(10, 2),
            Decimal::new(11, 2),
            120_000,
        );
        self.exit_on_shred.apply_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StrategyExitOnShredConfig {
    #[serde(default = "default_true")]
    pub launch_momentum: bool,
    #[serde(default = "default_true")]
    pub holder_growth: bool,
    #[serde(default = "default_true")]
    pub absorption_bounce: bool,
    #[serde(default = "default_true")]
    pub smart_rotation: bool,
    #[serde(default = "default_true")]
    pub organic_slow_grind: bool,
}

impl StrategyExitOnShredConfig {
    fn apply_defaults(&mut self) {}
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RiskConfig {
    #[serde(default)]
    pub max_rug_score: Decimal,
    #[serde(default)]
    pub max_bundle_score: Decimal,
    #[serde(default)]
    pub max_fake_momentum_score: Decimal,
    #[serde(default)]
    pub max_profit_overhang_score: Decimal,
    #[serde(default)]
    pub min_data_quality_score: Decimal,
}

impl RiskConfig {
    fn apply_defaults(&mut self) {
        if self.max_rug_score <= Decimal::ZERO {
            self.max_rug_score = Decimal::new(45, 2);
        }
        if self.max_bundle_score <= Decimal::ZERO {
            self.max_bundle_score = Decimal::new(55, 2);
        }
        if self.max_fake_momentum_score <= Decimal::ZERO {
            self.max_fake_momentum_score = Decimal::new(50, 2);
        }
        if self.max_profit_overhang_score <= Decimal::ZERO {
            self.max_profit_overhang_score = Decimal::new(75, 2);
        }
        if self.min_data_quality_score <= Decimal::ZERO {
            self.min_data_quality_score = Decimal::new(80, 2);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DecisionConfig {
    #[serde(default)]
    pub min_trade_eligibility_score: Decimal,
    #[serde(default)]
    pub min_fee_adjusted_edge_bps: u64,
    #[serde(default)]
    pub min_holder_stickiness_score: Decimal,
    #[serde(default)]
    pub min_momentum_authenticity_score: Decimal,
}

impl DecisionConfig {
    fn apply_defaults(&mut self) {
        if self.min_trade_eligibility_score <= Decimal::ZERO {
            self.min_trade_eligibility_score = Decimal::new(60, 2);
        }
        if self.min_fee_adjusted_edge_bps == 0 {
            self.min_fee_adjusted_edge_bps = 80;
        }
        if self.min_holder_stickiness_score <= Decimal::ZERO {
            self.min_holder_stickiness_score = Decimal::new(25, 2);
        }
        if self.min_momentum_authenticity_score <= Decimal::ZERO {
            self.min_momentum_authenticity_score = Decimal::new(40, 2);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcBudgetConfig {
    pub daily_credit_limit: u64,
    pub monthly_credit_limit: u64,
    pub emergency_reserve: u64,
    pub deny_when_unknown_live_state: bool,
    #[serde(default)]
    pub per_method_limits: BTreeMap<String, u64>,
    #[serde(default)]
    pub providers: Vec<RpcProviderConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcProviderConfig {
    pub name: String,
    pub endpoint: String,
    #[serde(default)]
    pub method_credit_costs: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteAssetConfig {
    pub mint: String,
    pub asset_type: String,
    pub min_trade_size_quote: Decimal,
    pub max_trade_size_quote: Decimal,
    pub min_edge_bps: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureFamilyBudget {
    pub enabled: bool,
    pub max_compute_units: u64,
    pub max_memory_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FeatureToggleConfig {
    #[serde(default = "default_true")]
    pub enable_advanced_cost_basis: bool,
    #[serde(default = "default_true")]
    pub enable_cohort_ranking: bool,
    #[serde(default = "default_true")]
    pub enable_funding_graph: bool,
    #[serde(default = "default_true")]
    pub enable_transaction_fingerprint: bool,
    #[serde(default = "default_true")]
    pub enable_absorption: bool,
    #[serde(default = "default_true")]
    pub enable_fake_momentum: bool,
    #[serde(default)]
    pub pressure: FeaturePressureConfig,
}

impl FeatureToggleConfig {
    fn apply_defaults(&mut self) {
        self.pressure.apply_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeaturePressureConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_feature_pressure_min_snapshot_interval_ms_normal")]
    pub min_snapshot_interval_ms_normal: u64,
    #[serde(default = "default_feature_pressure_min_snapshot_interval_ms_disk_warning")]
    pub min_snapshot_interval_ms_disk_warning: u64,
    #[serde(default = "default_feature_pressure_min_snapshot_interval_ms_upload_backlog")]
    pub min_snapshot_interval_ms_upload_backlog: u64,
    #[serde(default = "default_true")]
    pub always_snapshot_on_trade: bool,
    #[serde(default = "default_true")]
    pub always_snapshot_on_decision: bool,
    #[serde(default = "default_true")]
    pub always_snapshot_on_enter_exit: bool,
    #[serde(default = "default_true")]
    pub skip_low_value_watch_snapshots_under_pressure: bool,
}

impl Default for FeaturePressureConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_snapshot_interval_ms_normal:
                default_feature_pressure_min_snapshot_interval_ms_normal(),
            min_snapshot_interval_ms_disk_warning:
                default_feature_pressure_min_snapshot_interval_ms_disk_warning(),
            min_snapshot_interval_ms_upload_backlog:
                default_feature_pressure_min_snapshot_interval_ms_upload_backlog(),
            always_snapshot_on_trade: true,
            always_snapshot_on_decision: true,
            always_snapshot_on_enter_exit: true,
            skip_low_value_watch_snapshots_under_pressure: true,
        }
    }
}

impl FeaturePressureConfig {
    fn apply_defaults(&mut self) {
        if self.min_snapshot_interval_ms_normal == 0 {
            self.min_snapshot_interval_ms_normal =
                default_feature_pressure_min_snapshot_interval_ms_normal();
        }
        if self.min_snapshot_interval_ms_disk_warning == 0 {
            self.min_snapshot_interval_ms_disk_warning =
                default_feature_pressure_min_snapshot_interval_ms_disk_warning();
        }
        if self.min_snapshot_interval_ms_upload_backlog == 0 {
            self.min_snapshot_interval_ms_upload_backlog =
                default_feature_pressure_min_snapshot_interval_ms_upload_backlog();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StreamOnlyValidationSummary {
    pub stream_only_enabled: bool,
    pub fail_on_hot_path_rpc: bool,
    pub fail_on_unbudgeted_rpc: bool,
    pub market_data_rpc_calls_allowed: bool,
    pub holder_rpc_calls_allowed: bool,
    pub top_holder_rpc_calls_allowed: bool,
    pub metadata_fetch_allowed: bool,
    pub backfill_rpc_allowed: bool,
    pub reconciliation_rpc_allowed: bool,
    pub confirmation_rpc_allowed: bool,
    pub blockhash_rpc_allowed: bool,
    pub execution_rpc_allowed: bool,
    pub send_rpc_allowed: bool,
    pub emergency_rpc_allowed: bool,
    pub rpc_hot_path_enabled: bool,
    pub rpc_daily_credit_budget: u64,
    pub rpc_monthly_credit_budget: u64,
    pub rpc_budget_daily_limit: u64,
    pub rpc_budget_monthly_limit: u64,
    pub confirmation_source: String,
    pub live_execution_enabled: bool,
    pub use_rpc_send: bool,
    pub use_stream_blockhash_cache: bool,
    pub metadata_fetch_uri: bool,
    pub metadata_hot_path_fetch_enabled: bool,
    pub geyser_available: bool,
    pub deshred_available: bool,
    pub replay_available: bool,
    pub passed: bool,
    pub violations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AutopilotValidationSummary {
    pub enabled: bool,
    pub stream_only_required: bool,
    pub stream_only_enabled: bool,
    pub live_enabled: bool,
    pub live_trading_allowed: bool,
    pub max_parallel_runs: usize,
    pub disk_min_free_mb_before_cycle: u64,
    pub disk_min_free_mb_during_cycle: u64,
    pub state_path: String,
    pub lock_path: String,
    pub status_path: String,
    pub report_dir: String,
    pub passed: bool,
    pub violations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct R2ConfigValidationSummary {
    pub enabled: bool,
    pub upload_enabled: bool,
    pub delete_enabled: bool,
    pub dry_run: bool,
    pub managed_prefix: String,
    pub passed: bool,
    pub violations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeCollectorValidationSummary {
    pub runtime_mode: String,
    pub enabled: bool,
    pub stream_only_required: bool,
    pub stream_only_enabled: bool,
    pub live_enabled: bool,
    pub paper_decisions_enabled: bool,
    pub feature_engine_enabled: bool,
    pub risk_engine_enabled: bool,
    pub decision_engine_enabled: bool,
    pub paper_executor_enabled: bool,
    pub research_reports_enabled: bool,
    pub large_exports_enabled: bool,
    pub local_analysis_enabled: bool,
    pub r2_upload_required: bool,
    pub r2_verify_required: bool,
    pub r2_enabled: bool,
    pub r2_upload_enabled: bool,
    pub passed: bool,
    pub violations: Vec<String>,
}

impl LoadedConfig {
    pub fn stream_only_validation_summary(&self) -> StreamOnlyValidationSummary {
        let mut summary = StreamOnlyValidationSummary {
            stream_only_enabled: self.config.stream_only.enabled,
            fail_on_hot_path_rpc: self.config.stream_only.fail_on_hot_path_rpc,
            fail_on_unbudgeted_rpc: self.config.stream_only.fail_on_unbudgeted_rpc,
            market_data_rpc_calls_allowed: self.config.stream_only.allow_tracking_rpc,
            holder_rpc_calls_allowed: self.config.stream_only.allow_holder_rpc,
            top_holder_rpc_calls_allowed: self.config.stream_only.allow_top_holder_rpc,
            metadata_fetch_allowed: self.config.stream_only.allow_metadata_rpc,
            backfill_rpc_allowed: self.config.stream_only.allow_backfill_rpc,
            reconciliation_rpc_allowed: self.config.stream_only.allow_reconciliation_rpc,
            confirmation_rpc_allowed: self.config.stream_only.allow_confirmation_rpc,
            blockhash_rpc_allowed: self.config.stream_only.allow_blockhash_rpc,
            execution_rpc_allowed: self.config.stream_only.allow_execution_rpc,
            send_rpc_allowed: self.config.stream_only.allow_send_rpc,
            emergency_rpc_allowed: self.config.stream_only.allow_emergency_rpc,
            rpc_hot_path_enabled: self.config.rpc.hot_path_enabled,
            rpc_daily_credit_budget: self.config.rpc.daily_credit_budget,
            rpc_monthly_credit_budget: self.config.rpc.monthly_credit_budget,
            rpc_budget_daily_limit: self.config.rpc_budget.daily_credit_limit,
            rpc_budget_monthly_limit: self.config.rpc_budget.monthly_credit_limit,
            confirmation_source: self.config.confirmation.source.clone(),
            live_execution_enabled: self.config.execution.live_enabled
                || self.config.execution.enabled,
            use_rpc_send: self.config.execution.use_rpc_send,
            use_stream_blockhash_cache: self.config.execution.use_stream_blockhash_cache,
            metadata_fetch_uri: self.config.metadata.fetch_uri,
            metadata_hot_path_fetch_enabled: self.config.metadata.hot_path_fetch_enabled,
            geyser_available: self.config.geyser.enabled
                || self
                    .config
                    .ingest
                    .geyser
                    .as_ref()
                    .map(|value| value.enabled)
                    .unwrap_or(false),
            deshred_available: self
                .config
                .ingest
                .deshred
                .as_ref()
                .map(|value| value.enabled)
                .unwrap_or(false),
            replay_available: true,
            passed: true,
            violations: Vec::new(),
        };

        if summary.stream_only_enabled {
            if summary.market_data_rpc_calls_allowed {
                summary
                    .violations
                    .push("stream-only forbids tracking/market-data RPC".to_owned());
            }
            if summary.holder_rpc_calls_allowed {
                summary
                    .violations
                    .push("stream-only forbids holder RPC".to_owned());
            }
            if summary.top_holder_rpc_calls_allowed {
                summary
                    .violations
                    .push("stream-only forbids top-holder RPC".to_owned());
            }
            if summary.metadata_fetch_allowed
                || summary.metadata_fetch_uri
                || summary.metadata_hot_path_fetch_enabled
            {
                summary
                    .violations
                    .push("stream-only forbids metadata HTTP/RPC fetches".to_owned());
            }
            if summary.backfill_rpc_allowed {
                summary
                    .violations
                    .push("stream-only forbids backfill RPC".to_owned());
            }
            if summary.reconciliation_rpc_allowed {
                summary
                    .violations
                    .push("stream-only forbids reconciliation RPC".to_owned());
            }
            if summary.confirmation_rpc_allowed
                || !summary
                    .confirmation_source
                    .eq_ignore_ascii_case("geyser_stream")
            {
                summary.violations.push(
                    "stream-only requires geyser_stream confirmation with no RPC fallback"
                        .to_owned(),
                );
            }
            if summary.blockhash_rpc_allowed {
                summary
                    .violations
                    .push("stream-only forbids blockhash RPC".to_owned());
            }
            if summary.send_rpc_allowed || summary.execution_rpc_allowed || summary.use_rpc_send {
                summary
                    .violations
                    .push("stream-only forbids execution/send RPC".to_owned());
            }
            if summary.rpc_hot_path_enabled {
                summary
                    .violations
                    .push("stream-only requires rpc.hot_path_enabled=false".to_owned());
            }
            if summary.rpc_daily_credit_budget != 0 || summary.rpc_monthly_credit_budget != 0 {
                summary
                    .violations
                    .push("stream-only requires rpc credit budgets to be zero".to_owned());
            }
            if summary.rpc_budget_daily_limit != 0 || summary.rpc_budget_monthly_limit != 0 {
                summary
                    .violations
                    .push("stream-only requires rpc_budget credit limits to be zero".to_owned());
            }
            if !summary.geyser_available && !summary.replay_available {
                summary.violations.push(
                    "stream-only requires geyser, mock, or replay stream availability".to_owned(),
                );
            }
            if summary.live_execution_enabled && !summary.use_stream_blockhash_cache {
                summary.violations.push(
                    "stream-only requires stream blockhash cache when execution is enabled"
                        .to_owned(),
                );
            }
        }

        summary.passed = summary.violations.is_empty();
        summary
    }

    pub fn validate_stream_only(&self) -> Result<StreamOnlyValidationSummary> {
        let summary = self.stream_only_validation_summary();
        if summary.passed {
            Ok(summary)
        } else {
            Err(QuantError::Config(format!(
                "stream-only validation failed: {}",
                summary.violations.join("; ")
            )))
        }
    }

    pub fn autopilot_validation_summary(&self) -> AutopilotValidationSummary {
        let mut summary = AutopilotValidationSummary {
            enabled: self.config.autopilot.enabled,
            stream_only_required: self.config.autopilot.stream_only_required,
            stream_only_enabled: self.config.stream_only.enabled,
            live_enabled: self.config.live.enabled || self.config.execution.live_enabled,
            live_trading_allowed: self.config.autopilot.live_trading_allowed,
            max_parallel_runs: self.config.autopilot.max_parallel_runs,
            disk_min_free_mb_before_cycle: self.config.autopilot.disk.min_free_mb_before_cycle,
            disk_min_free_mb_during_cycle: self.config.autopilot.disk.min_free_mb_during_cycle,
            state_path: self.config.autopilot.state_path.clone(),
            lock_path: self.config.autopilot.lock_path.clone(),
            status_path: self.config.autopilot.status_path.clone(),
            report_dir: self.config.autopilot.report_dir.clone(),
            passed: true,
            violations: Vec::new(),
        };
        if summary.max_parallel_runs == 0 {
            summary
                .violations
                .push("autopilot.max_parallel_runs must be >= 1".to_owned());
        }
        if self.config.autopilot.live_trading_allowed {
            summary
                .violations
                .push("autopilot may not enable live trading".to_owned());
        }
        if summary.stream_only_required && !summary.stream_only_enabled {
            summary
                .violations
                .push("autopilot requires stream_only.enabled=true".to_owned());
        }
        if summary.stream_only_required && !self.stream_only_validation_summary().passed {
            summary
                .violations
                .push("autopilot requires a passing stream-only validation result".to_owned());
        }
        if self.config.autopilot.enabled && summary.live_enabled {
            summary
                .violations
                .push("autopilot requires live trading to remain disabled".to_owned());
        }
        if summary.disk_min_free_mb_before_cycle < summary.disk_min_free_mb_during_cycle {
            summary.violations.push(
                "autopilot.disk.min_free_mb_before_cycle must be >= min_free_mb_during_cycle"
                    .to_owned(),
            );
        }
        if summary.state_path.trim().is_empty()
            || summary.lock_path.trim().is_empty()
            || summary.status_path.trim().is_empty()
            || summary.report_dir.trim().is_empty()
        {
            summary
                .violations
                .push("autopilot paths must not be empty".to_owned());
        }
        summary.passed = summary.violations.is_empty();
        summary
    }

    pub fn validate_autopilot(&self) -> Result<AutopilotValidationSummary> {
        let summary = self.autopilot_validation_summary();
        if summary.passed {
            Ok(summary)
        } else {
            Err(QuantError::Config(format!(
                "autopilot validation failed: {}",
                summary.violations.join("; ")
            )))
        }
    }

    pub fn r2_validation_summary(&self) -> R2ConfigValidationSummary {
        let mut summary = R2ConfigValidationSummary {
            enabled: self.config.r2.enabled,
            upload_enabled: self.config.r2.upload_enabled,
            delete_enabled: self.config.r2.delete_enabled,
            dry_run: self.config.r2.dry_run,
            managed_prefix: self.config.r2.managed_prefix.clone(),
            passed: true,
            violations: Vec::new(),
        };
        if self.config.r2.max_concurrent_uploads == 0 {
            summary
                .violations
                .push("r2.max_concurrent_uploads must be >= 1".to_owned());
        }
        if self.config.r2.managed_prefix.trim().is_empty() {
            summary
                .violations
                .push("r2.managed_prefix must not be empty".to_owned());
        }
        if self.config.r2.delete_enabled
            && (!self.config.r2.buckets.allow_bucket_delete
                && !self.config.r2.buckets.allow_bucket_empty
                && !self.config.r2.buckets.allow_object_delete)
        {
            summary.violations.push(
                "r2.delete_enabled=true requires at least one delete/empty/object-delete capability to be explicitly enabled"
                    .to_owned(),
            );
        }
        summary.passed = summary.violations.is_empty();
        summary
    }

    pub fn validate_r2(&self) -> Result<R2ConfigValidationSummary> {
        let summary = self.r2_validation_summary();
        if summary.passed {
            Ok(summary)
        } else {
            Err(QuantError::Config(format!(
                "r2 validation failed: {}",
                summary.violations.join("; ")
            )))
        }
    }

    pub fn edge_collector_validation_summary(&self) -> EdgeCollectorValidationSummary {
        let runtime_mode = format!("{:?}", self.config.runtime.mode).to_lowercase();
        let enabled = self.config.runtime.mode == RuntimeModeName::EdgeCollector
            || self.config.edge_collector.enabled
            || self
                .config
                .autopilot
                .mode
                .trim()
                .eq_ignore_ascii_case("edge_collector");
        let mut summary = EdgeCollectorValidationSummary {
            runtime_mode,
            enabled,
            stream_only_required: self.config.edge_collector.stream_only_required,
            stream_only_enabled: self.config.stream_only.enabled,
            live_enabled: self.config.live.enabled || self.config.execution.live_enabled,
            paper_decisions_enabled: self.config.edge_collector.paper_decisions_enabled,
            feature_engine_enabled: self.config.edge_collector.feature_engine_enabled,
            risk_engine_enabled: self.config.edge_collector.risk_engine_enabled,
            decision_engine_enabled: self.config.edge_collector.decision_engine_enabled,
            paper_executor_enabled: self.config.edge_collector.paper_executor_enabled,
            research_reports_enabled: self.config.edge_collector.research_reports_enabled,
            large_exports_enabled: self.config.edge_collector.large_exports_enabled,
            local_analysis_enabled: self.config.edge_collector.local_analysis_enabled,
            r2_upload_required: self.config.edge_collector.r2_upload_required,
            r2_verify_required: self.config.edge_collector.r2_verify_required,
            r2_enabled: self.config.r2.enabled,
            r2_upload_enabled: self.config.r2.upload_enabled,
            passed: true,
            violations: Vec::new(),
        };
        if !summary.enabled {
            summary.passed = true;
            return summary;
        }
        if summary.stream_only_required && !summary.stream_only_enabled {
            summary
                .violations
                .push("edge_collector requires stream_only.enabled=true".to_owned());
        }
        if summary.stream_only_required && !self.stream_only_validation_summary().passed {
            summary
                .violations
                .push("edge_collector requires a passing stream-only validation result".to_owned());
        }
        if summary.live_enabled {
            summary
                .violations
                .push("edge_collector must keep live trading disabled".to_owned());
        }
        if summary.paper_decisions_enabled
            || summary.feature_engine_enabled
            || summary.risk_engine_enabled
            || summary.decision_engine_enabled
            || summary.paper_executor_enabled
            || summary.research_reports_enabled
            || summary.large_exports_enabled
            || summary.local_analysis_enabled
        {
            summary.violations.push(
                "edge_collector mode must keep feature/risk/decision/paper/report/export/local-analysis paths disabled".to_owned(),
            );
        }
        if self.config.edge_collector.allow_rpc_enrichment
            || self.config.edge_collector.allow_metadata_fetch
            || self.config.edge_collector.allow_social_fetch
            || self.config.edge_collector.allow_wallet_history_rpc
            || self.config.edge_collector.allow_bundle_enrichment_rpc
        {
            summary.violations.push(
                "edge_collector mode must keep rpc/api/http enrichment disabled".to_owned(),
            );
        }
        if summary.r2_upload_required && !summary.r2_upload_enabled {
            summary
                .violations
                .push("edge_collector requires r2.upload_enabled=true".to_owned());
        }
        if summary.r2_verify_required && !summary.r2_enabled {
            summary
                .violations
                .push("edge_collector verification requires r2.enabled=true".to_owned());
        }
        if self.config.edge_collector.max_local_runtime_mb == 0 {
            summary
                .violations
                .push("edge_collector.max_local_runtime_mb must be > 0".to_owned());
        }
        if self.config.edge_collector.storage.max_local_segment_mb == 0 {
            summary
                .violations
                .push("edge_collector.storage.max_local_segment_mb must be > 0".to_owned());
        }
        summary.passed = summary.violations.is_empty();
        summary
    }

    pub fn validate_edge_mode(&self) -> Result<EdgeCollectorValidationSummary> {
        let summary = self.edge_collector_validation_summary();
        if summary.passed {
            Ok(summary)
        } else {
            Err(QuantError::Config(format!(
                "edge_collector validation failed: {}",
                summary.violations.join("; ")
            )))
        }
    }
}

const fn default_true() -> bool {
    true
}

fn default_edge_collector_segment_dir() -> String {
    "data/edge_segments".to_owned()
}

const fn default_edge_collector_max_open_segments() -> usize {
    4
}

const fn default_edge_collector_max_local_segment_mb() -> u64 {
    64
}

const fn default_edge_collector_max_local_runtime_mb() -> u64 {
    256
}

const fn default_edge_collector_flush_interval_seconds() -> u64 {
    15
}

const fn default_edge_collector_segment_max_size_mb() -> u64 {
    4
}

const fn default_edge_collector_segment_max_age_seconds() -> u64 {
    30
}

fn default_enrichment_mode() -> String {
    "research_only".to_owned()
}

fn default_enrichment_cache_dir() -> String {
    "research_output/enrichment_cache".to_owned()
}

fn default_enrichment_ledger_path() -> String {
    "research_output/enrichment_ledger.jsonl".to_owned()
}

const fn default_enrichment_max_daily_rpc_calls() -> u64 {
    5000
}

const fn default_enrichment_max_daily_rpc_credits() -> u64 {
    5000
}

const fn default_enrichment_max_daily_http_metadata_fetches() -> u64 {
    2000
}

const fn default_enrichment_max_wallets_per_run() -> usize {
    500
}

const fn default_enrichment_max_tokens_per_run() -> usize {
    1000
}

const fn default_enrichment_max_signatures_per_wallet() -> usize {
    25
}

const fn default_enrichment_max_metadata_bytes() -> u64 {
    262_144
}

const fn default_enrichment_request_timeout_ms() -> u64 {
    5000
}

const fn default_enrichment_max_retries() -> usize {
    2
}

const fn default_enrichment_max_redirects() -> usize {
    2
}

fn default_enrichment_allowed_schemes() -> Vec<String> {
    vec!["https".to_owned(), "ipfs".to_owned()]
}

fn default_research_worker_input() -> String {
    "r2".to_owned()
}

fn default_research_worker_local_output_dir() -> String {
    "research_output".to_owned()
}

fn default_autopilot_mode() -> String {
    "paper_collection".to_owned()
}

const fn default_autopilot_parallel_runs() -> usize {
    1
}

fn default_autopilot_state_path() -> String {
    "data/autopilot/autopilot_state.json".to_owned()
}

fn default_autopilot_lock_path() -> String {
    "data/autopilot/autopilot.lock".to_owned()
}

fn default_autopilot_status_path() -> String {
    "data/autopilot/status.json".to_owned()
}

fn default_autopilot_report_dir() -> String {
    "data/reports/autopilot".to_owned()
}

fn default_r2_account_id_env() -> String {
    "CF_ACCOUNT_ID".to_owned()
}

fn default_r2_endpoint_env() -> String {
    "R2_ENDPOINT".to_owned()
}

fn default_r2_access_key_id_env() -> String {
    "R2_ACCESS_KEY_ID".to_owned()
}

fn default_r2_secret_access_key_env() -> String {
    "R2_SECRET_ACCESS_KEY".to_owned()
}

fn default_r2_managed_prefix() -> String {
    "pump-launch-quant".to_owned()
}

fn default_r2_region() -> String {
    "auto".to_owned()
}

const fn default_r2_max_concurrent_uploads() -> usize {
    4
}

const fn default_r2_multipart_threshold_mb() -> u64 {
    64
}

const fn default_r2_part_size_mb() -> u64 {
    16
}

const fn default_r2_upload_timeout_seconds() -> u64 {
    600
}

fn default_r2_compression() -> String {
    "zstd".to_owned()
}

fn default_r2_encryption_mode() -> String {
    "none".to_owned()
}

fn default_r2_datasets_bucket_env() -> String {
    "R2_DATASET_BUCKET".to_owned()
}

fn default_r2_reports_bucket_env() -> String {
    "R2_REPORTS_BUCKET".to_owned()
}

fn default_r2_calibration_bucket_env() -> String {
    "R2_CALIBRATION_BUCKET".to_owned()
}

fn default_r2_provider_compat_bucket_env() -> String {
    "R2_PROVIDER_COMPAT_BUCKET".to_owned()
}

fn default_r2_runs_prefix() -> String {
    "runs".to_owned()
}

fn default_r2_reports_prefix() -> String {
    "reports".to_owned()
}

fn default_r2_exports_prefix() -> String {
    "exports".to_owned()
}

fn default_r2_calibration_prefix() -> String {
    "calibration".to_owned()
}

fn default_r2_provider_compat_prefix() -> String {
    "provider_compatibility".to_owned()
}

fn default_r2_rpc_ledger_prefix() -> String {
    "rpc_ledger".to_owned()
}

fn default_r2_manifests_prefix() -> String {
    "manifests".to_owned()
}

const fn default_r2_keep_local_after_upload_hours() -> u64 {
    24
}

const fn default_r2_keep_last_n_local_runs() -> usize {
    10
}

const fn default_r2_max_local_storage_gb() -> u64 {
    50
}

const fn default_autopilot_heartbeat_interval_seconds() -> u64 {
    30
}

const fn default_autopilot_cycle_sleep_seconds() -> u64 {
    60
}

const fn default_autopilot_max_cycle_runtime_seconds() -> u64 {
    7_200
}

const fn default_autopilot_graceful_shutdown_timeout_seconds() -> u64 {
    30
}

fn default_autopilot_collection_preset() -> String {
    "medium".to_owned()
}

const fn default_autopilot_collection_duration_seconds() -> u64 {
    1_800
}

const fn default_autopilot_checkpoint_interval_seconds() -> u64 {
    300
}

const fn default_autopilot_smoke_duration_seconds() -> u64 {
    30
}

const fn default_autopilot_provider_error_retry_seconds() -> u64 {
    300
}

const fn default_autopilot_missing_endpoint_retry_seconds() -> u64 {
    600
}

const fn default_autopilot_auth_error_retry_seconds() -> u64 {
    1_800
}

const fn default_autopilot_max_total_storage_gb() -> u64 {
    50
}

const fn default_autopilot_max_run_age_days() -> u64 {
    14
}

const fn default_autopilot_keep_last_n_runs() -> usize {
    100
}

const fn default_autopilot_keep_last_n_backtests() -> usize {
    50
}

fn default_autopilot_alerts_path() -> String {
    "data/autopilot/alerts.jsonl".to_owned()
}

const fn default_autopilot_max_provider_failures_before_pause() -> u64 {
    10
}

const fn default_autopilot_max_queue_overflows_before_pause() -> u64 {
    3
}

const fn default_autopilot_disk_min_free_mb_before_cycle() -> u64 {
    1024
}

const fn default_autopilot_disk_min_free_mb_during_cycle() -> u64 {
    512
}

const fn default_autopilot_disk_check_interval_seconds() -> u64 {
    30
}

const fn default_autopilot_disk_warning_free_mb() -> u64 {
    2_048
}

const fn default_autopilot_disk_pre_cycle_min_free_mb() -> u64 {
    1_536
}

const fn default_autopilot_disk_critical_free_mb() -> u64 {
    1_024
}

const fn default_autopilot_disk_emergency_free_mb() -> u64 {
    512
}

const fn default_autopilot_post_cycle_cleanup_target_free_mb_after_cycle() -> u64 {
    2048
}

const fn default_storage_segment_max_segment_size_mb() -> u64 {
    32
}

const fn default_storage_segment_max_segment_age_seconds() -> u64 {
    300
}

fn default_storage_segment_compression() -> String {
    "zstd".to_owned()
}

const fn default_storage_segment_keep_last_n_segments_local() -> usize {
    2
}

fn default_storage_segment_manifest_path() -> String {
    "data/manifests/segments".to_owned()
}

const fn default_storage_segment_max_concurrent_uploads() -> usize {
    2
}

const fn default_storage_segment_max_pending_segments() -> usize {
    20
}

const fn default_storage_segment_max_pending_segments_warning() -> usize {
    20
}

const fn default_storage_segment_max_pending_segments_pause() -> usize {
    40
}

const fn default_storage_segment_max_pending_segments_stop() -> usize {
    60
}

const fn default_storage_segment_pause_pending_segments() -> usize {
    60
}

const fn default_storage_segment_max_upload_retries() -> u64 {
    3
}

const fn default_storage_segment_retry_backoff_seconds() -> u64 {
    30
}

const fn default_storage_segment_max_finalize_wait_seconds() -> u64 {
    300
}

const fn default_storage_segment_max_finalize_upload_retries() -> u64 {
    3
}

const fn default_storage_low_disk_target_local_runtime_mb() -> u64 {
    500
}

const fn default_storage_low_disk_warning_free_mb() -> u64 {
    2_048
}

const fn default_storage_low_disk_critical_free_mb() -> u64 {
    1_536
}

const fn default_storage_low_disk_emergency_free_mb() -> u64 {
    768
}

const fn default_storage_low_disk_max_local_verified_segments() -> usize {
    1
}

const fn default_storage_low_disk_max_local_unverified_closed_segments() -> usize {
    20
}

const fn default_storage_low_disk_max_local_export_mb() -> u64 {
    64
}

const fn default_exports_max_uncompressed_export_mb() -> u64 {
    128
}

const fn default_exports_feature_export_chunk_rows() -> usize {
    50_000
}

fn default_exports_compression() -> String {
    "zstd".to_owned()
}

const fn default_reports_low_disk_max_local_report_mb() -> u64 {
    10
}

const fn default_analysis_remote_max_restore_mb() -> u64 {
    256
}

const fn default_feature_pressure_min_snapshot_interval_ms_normal() -> u64 {
    1_000
}

const fn default_feature_pressure_min_snapshot_interval_ms_disk_warning() -> u64 {
    5_000
}

const fn default_feature_pressure_min_snapshot_interval_ms_upload_backlog() -> u64 {
    10_000
}

const fn default_autopilot_backtest_min_duration_seconds() -> u64 {
    3_600
}

const fn default_autopilot_backtest_min_tokens_discovered() -> usize {
    100
}

const fn default_autopilot_backtest_min_complete_lifecycles() -> usize {
    20
}

const fn default_autopilot_backtest_min_feature_snapshots() -> usize {
    1_000
}

const fn default_autopilot_backtest_min_decisions() -> usize {
    10
}

fn default_autopilot_backtest_min_holder_confidence() -> Decimal {
    Decimal::new(5, 1)
}

fn default_early_intent_sources() -> Vec<String> {
    vec![
        "fixture".to_owned(),
        "mock".to_owned(),
        "deshred".to_owned(),
        "raw_shred".to_owned(),
    ]
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::LoadedConfig;

    #[test]
    fn loads_default_config_and_resolves_relative_paths() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("config")
            .join("default.toml");
        let loaded = LoadedConfig::from_file(&root).expect("config should load");
        assert_eq!(loaded.config.environment.schema_version, 1);
        assert!(
            loaded
                .resolve_path(&loaded.config.pump.idl_paths[0])
                .ends_with("fixtures/idl/pump_mock_idl.json")
        );
        assert_eq!(loaded.hash.len(), 64);
    }

    #[test]
    fn default_config_keeps_autopilot_disabled_and_stream_only_required() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("config")
            .join("default.toml");
        let loaded = LoadedConfig::from_file(&root).expect("config should load");
        assert!(!loaded.config.autopilot.enabled);
        assert!(loaded.config.autopilot.stream_only_required);
        assert!(!loaded.config.autopilot.live_trading_allowed);
        loaded.validate_autopilot().expect("autopilot valid");
    }

    #[test]
    fn autopilot_validation_rejects_zero_parallel_runs() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("config")
            .join("default.toml");
        let mut loaded = LoadedConfig::from_file(&root).expect("config should load");
        loaded.config.autopilot.max_parallel_runs = 0;
        let error = loaded
            .validate_autopilot()
            .expect_err("zero parallel runs must fail");
        assert!(error.to_string().contains("max_parallel_runs"));
    }

    #[test]
    fn config_override_merges_without_requiring_full_copy() {
        let dir = tempfile::tempdir().expect("tempdir");
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("config")
            .join("default.toml");
        let overlay = dir.path().join("local.toml");
        fs::write(
            &overlay,
            r#"
[r2]
enabled = true
upload_enabled = true
dry_run = false

[autopilot]
enabled = true
"#,
        )
        .expect("write overlay");
        let loaded = LoadedConfig::from_files(&base, Some(&overlay)).expect("merged config");
        assert!(loaded.config.r2.enabled);
        assert!(loaded.config.r2.upload_enabled);
        assert!(!loaded.config.r2.dry_run);
        assert!(loaded.config.autopilot.enabled);
        assert_eq!(loaded.path, overlay);
    }
}
