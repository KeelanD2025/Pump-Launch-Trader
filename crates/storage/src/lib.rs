use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

use common::{
    EventPayload, FillEvent, NormalizedEvent, ReasonCode, SCHEMA_VERSION, StorageConfig,
    TradeDecisionEvent, config::ResearchWorkerPersistenceConfig,
};
use features::{FeatureDatum, FeatureEngine, FeatureSnapshot};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use state::{CompactTokenSummary, WalletSummary};
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetKind {
    RawEventLog,
    NormalizedEventLog,
    TokenSummary,
    TokenFeatureSnapshots,
    RiskSnapshots,
    WalletSummary,
    TradeDecisions,
    SimulatedFills,
    LiveFills,
    BacktestRuns,
    RuntimeAuditLog,
    RunMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
    Interrupted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RunKind {
    StandardFixtureSuite,
    ShredExitFixtureSuite,
    FixtureScenario,
    PaperReplay,
    MockedLivePaper,
    MockedLivePaperEarlyIntent,
    MockedLivePaperDeshred,
    RealLivePaper,
    RealLivePaperDeshred,
    Backtest,
    ShredExitBacktest,
    DeshredSmoke,
    MultisourceEarlyIntentSuite,
    Report,
    Export,
    ReplayEquivalence,
    CalibrationUpdate,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RunRole {
    SourceRun,
    DerivedRun,
    AnalysisRun,
    ReportRun,
    ExportRun,
    #[default]
    Unknown,
}

impl RunKind {
    pub fn role(self) -> RunRole {
        match self {
            RunKind::StandardFixtureSuite
            | RunKind::ShredExitFixtureSuite
            | RunKind::FixtureScenario
            | RunKind::MockedLivePaper
            | RunKind::MockedLivePaperEarlyIntent
            | RunKind::MockedLivePaperDeshred
            | RunKind::RealLivePaper
            | RunKind::RealLivePaperDeshred
            | RunKind::MultisourceEarlyIntentSuite => RunRole::SourceRun,
            RunKind::PaperReplay | RunKind::Backtest | RunKind::ShredExitBacktest => {
                RunRole::DerivedRun
            }
            RunKind::ReplayEquivalence | RunKind::CalibrationUpdate | RunKind::DeshredSmoke => {
                RunRole::AnalysisRun
            }
            RunKind::Report => RunRole::ReportRun,
            RunKind::Export => RunRole::ExportRun,
            RunKind::Unknown => RunRole::Unknown,
        }
    }

    pub fn is_default_latest_candidate(self) -> bool {
        matches!(
            self.role(),
            RunRole::SourceRun | RunRole::DerivedRun | RunRole::Unknown
        )
    }

    pub fn is_source_run(self) -> bool {
        self.role() == RunRole::SourceRun
    }

    pub fn is_live_paper(self) -> bool {
        matches!(
            self,
            RunKind::MockedLivePaper
                | RunKind::MockedLivePaperEarlyIntent
                | RunKind::MockedLivePaperDeshred
                | RunKind::RealLivePaper
                | RunKind::RealLivePaperDeshred
        )
    }

    pub fn is_mocked_live(self) -> bool {
        matches!(
            self,
            RunKind::MockedLivePaper
                | RunKind::MockedLivePaperEarlyIntent
                | RunKind::MockedLivePaperDeshred
        )
    }

    pub fn is_paper_run(self) -> bool {
        matches!(
            self,
            RunKind::PaperReplay
                | RunKind::MockedLivePaper
                | RunKind::MockedLivePaperEarlyIntent
                | RunKind::MockedLivePaperDeshred
                | RunKind::RealLivePaper
                | RunKind::RealLivePaperDeshred
        )
    }

    pub fn is_backtest(self) -> bool {
        matches!(self, RunKind::Backtest | RunKind::ShredExitBacktest)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactType {
    Report,
    Export,
    BacktestOutput,
    ReplayEquivalenceDebug,
    CalibrationSnapshot,
    Verification,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactMetadata {
    pub artifact_id: String,
    pub artifact_type: ArtifactType,
    pub associated_run_id: String,
    pub source_run_id: String,
    pub created_at_wall_time: Option<OffsetDateTime>,
    pub path: String,
    pub schema_version: u32,
    pub config_hash: String,
    pub idl_hash: String,
    pub calibration_snapshot_hash: Option<String>,
    pub row_count: Option<u64>,
    pub checksum: String,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMetadata {
    pub run_id: String,
    #[serde(default)]
    pub parent_run_id: Option<String>,
    #[serde(default)]
    pub source_run_id: Option<String>,
    #[serde(default)]
    pub scenario_id: Option<String>,
    pub mode: String,
    #[serde(default)]
    pub run_kind: RunKind,
    #[serde(default)]
    pub run_role: RunRole,
    pub created_at_wall_time: OffsetDateTime,
    #[serde(default)]
    pub completed_at_wall_time: Option<OffsetDateTime>,
    pub config_hash: String,
    pub idl_hash: String,
    pub event_count: u64,
    pub decision_count: u64,
    pub fill_count: u64,
    #[serde(default)]
    pub paper_pnl: Decimal,
    #[serde(default)]
    pub contains_tentative_sell_events: bool,
    #[serde(default)]
    pub contains_shred_exit_defense_events: bool,
    #[serde(default)]
    pub tentative_sell_count: u64,
    #[serde(default)]
    pub emergency_exit_count: u64,
    #[serde(default)]
    pub saved_loss_total: Decimal,
    #[serde(default)]
    pub opportunity_cost_total: Decimal,
    #[serde(default)]
    pub false_positive_count: u64,
    #[serde(default)]
    pub decode_mismatch_count: u64,
    #[serde(default)]
    pub reorg_count: u64,
    #[serde(default)]
    pub not_seen_count: u64,
    pub status: RunStatus,
    pub report_dir: String,
    #[serde(default)]
    pub notes: Vec<String>,
    #[serde(default)]
    pub calibration_snapshot_hash: Option<String>,
    #[serde(default)]
    pub calibration_snapshot_path: Option<String>,
    #[serde(default)]
    pub post_run_calibration_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredRecord<T> {
    pub schema_version: u32,
    pub config_hash: String,
    pub idl_hash: String,
    pub strategy_version: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub scenario_id: Option<String>,
    #[serde(default)]
    pub runtime_sequence_number: Option<u64>,
    #[serde(default)]
    pub source_event_id: Option<String>,
    #[serde(default)]
    pub trigger_event_id: Option<String>,
    #[serde(default)]
    pub calibration_snapshot_hash: Option<String>,
    #[serde(default)]
    pub deterministic_replay_hash: Option<String>,
    #[serde(default)]
    pub replay_equivalence_group_id: Option<String>,
    pub source: String,
    pub canonicality: String,
    pub no_lookahead_timestamp: Option<OffsetDateTime>,
    pub dataset: DatasetKind,
    pub record: T,
}

impl<T> StoredRecord<T> {
    pub fn new(
        dataset: DatasetKind,
        config_hash: impl Into<String>,
        idl_hash: impl Into<String>,
        strategy_version: Option<String>,
        source: impl Into<String>,
        canonicality: impl Into<String>,
        no_lookahead_timestamp: Option<OffsetDateTime>,
        record: T,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            config_hash: config_hash.into(),
            idl_hash: idl_hash.into(),
            strategy_version,
            run_id: None,
            scenario_id: None,
            runtime_sequence_number: None,
            source_event_id: None,
            trigger_event_id: None,
            calibration_snapshot_hash: None,
            deterministic_replay_hash: None,
            replay_equivalence_group_id: None,
            source: source.into(),
            canonicality: canonicality.into(),
            no_lookahead_timestamp,
            dataset,
            record,
        }
    }

    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    pub fn with_scenario_id(mut self, scenario_id: impl Into<String>) -> Self {
        self.scenario_id = Some(scenario_id.into());
        self
    }

    pub fn with_runtime_sequence_number(mut self, runtime_sequence_number: u64) -> Self {
        self.runtime_sequence_number = Some(runtime_sequence_number);
        self
    }

    pub fn with_source_event_id(mut self, source_event_id: Option<String>) -> Self {
        self.source_event_id = source_event_id;
        self
    }

    pub fn with_trigger_event_id(mut self, trigger_event_id: Option<String>) -> Self {
        self.trigger_event_id = trigger_event_id;
        self
    }

    pub fn with_calibration_snapshot_hash(
        mut self,
        calibration_snapshot_hash: Option<String>,
    ) -> Self {
        self.calibration_snapshot_hash = calibration_snapshot_hash;
        self
    }

    pub fn with_deterministic_replay_hash(
        mut self,
        deterministic_replay_hash: Option<String>,
    ) -> Self {
        self.deterministic_replay_hash = deterministic_replay_hash;
        self
    }

    pub fn with_replay_equivalence_group_id(
        mut self,
        replay_equivalence_group_id: Option<String>,
    ) -> Self {
        self.replay_equivalence_group_id = replay_equivalence_group_id;
        self
    }
}

#[derive(Debug, Clone)]
pub struct StorageLayout {
    pub root: PathBuf,
    pub raw_event_log: PathBuf,
    pub normalized_event_log: PathBuf,
    pub run_metadata_log: PathBuf,
    pub decision_log: PathBuf,
    pub fill_log: PathBuf,
    pub runtime_audit_log: PathBuf,
    pub token_summary_dir: PathBuf,
    pub feature_snapshot_dir: PathBuf,
    pub report_dir: PathBuf,
}

impl StorageLayout {
    pub fn from_config(config: &StorageConfig) -> Result<Self, StorageError> {
        let root = PathBuf::from(&config.root);
        let event_dir = root.join(&config.event_log_dir);
        let snapshot_dir = root.join(&config.snapshot_dir);
        let report_dir = root.join(&config.report_dir);

        fs::create_dir_all(&event_dir)?;
        fs::create_dir_all(&snapshot_dir)?;
        fs::create_dir_all(&report_dir)?;

        let token_summary_dir = snapshot_dir.join("token_summary");
        let feature_snapshot_dir = snapshot_dir.join("token_feature_snapshots");
        fs::create_dir_all(&token_summary_dir)?;
        fs::create_dir_all(&feature_snapshot_dir)?;

        Ok(Self {
            root,
            raw_event_log: event_dir.join("raw_event_log.jsonl"),
            normalized_event_log: event_dir.join("normalized_event_log.jsonl"),
            run_metadata_log: event_dir.join(&config.run_metadata_log_path),
            decision_log: event_dir.join(&config.decision_log_path),
            fill_log: event_dir.join(&config.fill_log_path),
            runtime_audit_log: event_dir.join(&config.runtime_audit_log_path),
            token_summary_dir,
            feature_snapshot_dir,
            report_dir,
        })
    }
}

#[derive(Debug, Clone)]
pub struct StorageEngine {
    layout: StorageLayout,
    feature_snapshot_persistence: ResearchWorkerPersistenceConfig,
}

impl StorageEngine {
    pub fn new(layout: StorageLayout) -> Self {
        Self {
            layout,
            feature_snapshot_persistence: ResearchWorkerPersistenceConfig::default(),
        }
    }

    pub fn with_feature_snapshot_persistence(
        mut self,
        config: ResearchWorkerPersistenceConfig,
    ) -> Self {
        self.feature_snapshot_persistence = config;
        self
    }

    pub fn layout(&self) -> &StorageLayout {
        &self.layout
    }

    pub fn run_report_dir(&self, run_id: &str) -> PathBuf {
        self.layout.report_dir.join(run_id)
    }

    pub fn append_raw_event(
        &self,
        record: &StoredRecord<serde_json::Value>,
    ) -> Result<(), StorageError> {
        append_jsonl(&self.layout.raw_event_log, record)
    }

    pub fn append_normalized_event(
        &self,
        record: &StoredRecord<NormalizedEvent>,
    ) -> Result<(), StorageError> {
        append_jsonl(&self.layout.normalized_event_log, record)
    }

    pub fn read_normalized_events(
        &self,
    ) -> Result<Vec<StoredRecord<NormalizedEvent>>, StorageError> {
        read_jsonl(&self.layout.normalized_event_log)
    }

    pub fn read_normalized_events_filtered(
        &self,
        run_id: Option<&str>,
        scenario_id: Option<&str>,
    ) -> Result<Vec<StoredRecord<NormalizedEvent>>, StorageError> {
        read_stored_jsonl_filtered(&self.layout.normalized_event_log, run_id, scenario_id)
    }

    pub fn latest_normalized_event_run_id(&self) -> Result<Option<String>, StorageError> {
        let events = self.read_normalized_events()?;
        let event_run_ids = events
            .iter()
            .filter_map(|record| record.run_id.clone())
            .collect::<std::collections::HashSet<_>>();
        if !event_run_ids.is_empty() {
            let completed_event_runs = self
                .list_runs()?
                .into_iter()
                .filter(|record| {
                    record.record.status == RunStatus::Completed
                        && event_run_ids.contains(&record.record.run_id)
                })
                .collect::<Vec<_>>();
            if let Some(record) = completed_event_runs.into_iter().max_by(|left, right| {
                run_wall_clock(left)
                    .cmp(&run_wall_clock(right))
                    .then_with(|| left.record.run_id.cmp(&right.record.run_id))
            }) {
                return Ok(Some(record.record.run_id));
            }
        }
        Ok(latest_run_id(events.iter().map(|record| {
            (
                record
                    .no_lookahead_timestamp
                    .or(Some(OffsetDateTime::UNIX_EPOCH)),
                record.run_id.clone(),
            )
        })))
    }

    pub fn latest_trade_decision_run_id(&self) -> Result<Option<String>, StorageError> {
        let records = self.read_trade_decisions()?;
        let run_ids = records
            .iter()
            .filter_map(|record| record.run_id.clone())
            .collect::<std::collections::HashSet<_>>();
        if let Some(run_id) = self.latest_completed_run_id_for(&run_ids)? {
            return Ok(Some(run_id));
        }
        Ok(latest_run_id(records.iter().map(|record| {
            (
                record
                    .no_lookahead_timestamp
                    .or(Some(OffsetDateTime::UNIX_EPOCH)),
                record.run_id.clone(),
            )
        })))
    }

    pub fn latest_fill_run_id(&self) -> Result<Option<String>, StorageError> {
        let records = self.read_fills()?;
        let run_ids = records
            .iter()
            .filter_map(|record| record.run_id.clone())
            .collect::<std::collections::HashSet<_>>();
        if let Some(run_id) = self.latest_completed_run_id_for(&run_ids)? {
            return Ok(Some(run_id));
        }
        Ok(latest_run_id(records.iter().map(|record| {
            (
                record
                    .no_lookahead_timestamp
                    .or(Some(OffsetDateTime::UNIX_EPOCH)),
                record.run_id.clone(),
            )
        })))
    }

    pub fn append_trade_decision(
        &self,
        record: &StoredRecord<TradeDecisionEvent>,
    ) -> Result<(), StorageError> {
        append_jsonl(&self.layout.decision_log, record)
    }

    pub fn read_trade_decisions(
        &self,
    ) -> Result<Vec<StoredRecord<TradeDecisionEvent>>, StorageError> {
        read_jsonl(&self.layout.decision_log)
    }

    pub fn read_trade_decisions_filtered(
        &self,
        run_id: Option<&str>,
        scenario_id: Option<&str>,
    ) -> Result<Vec<StoredRecord<TradeDecisionEvent>>, StorageError> {
        let mut records = self.read_trade_decisions()?;
        filter_records(&mut records, run_id, scenario_id);
        Ok(records)
    }

    pub fn append_fill(&self, record: &StoredRecord<FillEvent>) -> Result<(), StorageError> {
        append_jsonl(&self.layout.fill_log, record)
    }

    pub fn read_fills(&self) -> Result<Vec<StoredRecord<FillEvent>>, StorageError> {
        read_jsonl(&self.layout.fill_log)
    }

    pub fn read_fills_filtered(
        &self,
        run_id: Option<&str>,
        scenario_id: Option<&str>,
    ) -> Result<Vec<StoredRecord<FillEvent>>, StorageError> {
        let mut records = self.read_fills()?;
        filter_records(&mut records, run_id, scenario_id);
        Ok(records)
    }

    pub fn append_runtime_audit<T: Serialize>(
        &self,
        record: &StoredRecord<T>,
    ) -> Result<(), StorageError> {
        append_jsonl(&self.layout.runtime_audit_log, record)
    }

    pub fn append_run_metadata(
        &self,
        record: &StoredRecord<RunMetadata>,
    ) -> Result<(), StorageError> {
        append_jsonl(&self.layout.run_metadata_log, record)
    }

    pub fn read_run_metadata(&self) -> Result<Vec<StoredRecord<RunMetadata>>, StorageError> {
        read_jsonl(&self.layout.run_metadata_log)
    }

    pub fn read_run_metadata_filtered(
        &self,
        run_id: Option<&str>,
        scenario_id: Option<&str>,
    ) -> Result<Vec<StoredRecord<RunMetadata>>, StorageError> {
        let mut records = self.read_run_metadata()?;
        filter_records(&mut records, run_id, scenario_id);
        Ok(records)
    }

    pub fn latest_run_id_from_metadata(&self) -> Result<Option<String>, StorageError> {
        Ok(self
            .latest_run_metadata()?
            .map(|record| record.record.run_id))
    }

    pub fn latest_run_id_from_metadata_filtered(
        &self,
        kind: Option<RunKind>,
        require_source_events: bool,
        require_shred_exit: bool,
    ) -> Result<Option<String>, StorageError> {
        Ok(self
            .latest_run_metadata_filtered(kind, require_source_events, require_shred_exit)?
            .map(|record| record.record.run_id))
    }

    pub fn latest_run_metadata(&self) -> Result<Option<StoredRecord<RunMetadata>>, StorageError> {
        self.latest_run_metadata_filtered(None, false, false)
    }

    pub fn latest_run_metadata_filtered(
        &self,
        kind: Option<RunKind>,
        require_source_events: bool,
        require_shred_exit: bool,
    ) -> Result<Option<StoredRecord<RunMetadata>>, StorageError> {
        let runs = self.list_runs()?;
        let pick_latest = |records: Vec<StoredRecord<RunMetadata>>| {
            records.into_iter().max_by(|left, right| {
                run_wall_clock(left)
                    .cmp(&run_wall_clock(right))
                    .then_with(|| left.record.run_id.cmp(&right.record.run_id))
            })
        };
        let filtered = runs
            .into_iter()
            .filter(|record| {
                kind.map(|kind| record.record.run_kind == kind)
                    .unwrap_or(true)
            })
            .filter(|record| !require_source_events || record.record.event_count > 0)
            .filter(|record| {
                !require_shred_exit || record.record.contains_shred_exit_defense_events
            })
            .collect::<Vec<_>>();
        if let Some(record) = pick_latest(
            filtered
                .iter()
                .filter(|record| record.record.status == RunStatus::Completed)
                .cloned()
                .collect(),
        ) {
            return Ok(Some(record));
        }
        if let Some(record) = pick_latest(
            filtered
                .iter()
                .filter(|record| record.record.status != RunStatus::Running)
                .cloned()
                .collect(),
        ) {
            return Ok(Some(record));
        }
        Ok(pick_latest(filtered))
    }

    fn latest_completed_run_id_for(
        &self,
        run_ids: &std::collections::HashSet<String>,
    ) -> Result<Option<String>, StorageError> {
        if run_ids.is_empty() {
            return Ok(None);
        }
        Ok(self
            .list_runs()?
            .into_iter()
            .filter(|record| {
                record.record.status == RunStatus::Completed
                    && run_ids.contains(&record.record.run_id)
            })
            .max_by(|left, right| {
                run_wall_clock(left)
                    .cmp(&run_wall_clock(right))
                    .then_with(|| left.record.run_id.cmp(&right.record.run_id))
            })
            .map(|record| record.record.run_id))
    }

    pub fn list_runs(&self) -> Result<Vec<StoredRecord<RunMetadata>>, StorageError> {
        let event_stats =
            derive_run_event_stats(&self.read_normalized_events()?, &self.read_fills()?);
        let mut runs = latest_metadata_per_run(self.read_run_metadata()?)
            .into_values()
            .map(|mut record| {
                record.scenario_id = None;
                record.record.scenario_id = None;
                let derived = event_stats.get(&record.record.run_id);
                if let Some(derived) = derived {
                    record.record.event_count = derived.event_count;
                    record.record.contains_tentative_sell_events =
                        derived.contains_tentative_sell_events;
                    record.record.contains_shred_exit_defense_events =
                        derived.contains_shred_exit_defense_events;
                    record.record.tentative_sell_count = derived.tentative_sell_count;
                    record.record.emergency_exit_count = derived.emergency_exit_count;
                    record.record.saved_loss_total = derived.saved_loss_total;
                    record.record.opportunity_cost_total = derived.opportunity_cost_total;
                    record.record.false_positive_count = derived.false_positive_count;
                    record.record.decode_mismatch_count = derived.decode_mismatch_count;
                    record.record.reorg_count = derived.reorg_count;
                    record.record.not_seen_count = derived.not_seen_count;
                }
                record.record.run_kind = infer_run_kind(
                    &record.record.run_id,
                    &record.record.mode,
                    &record.record.notes,
                    record.record.contains_tentative_sell_events,
                );
                record.record.run_role =
                    infer_run_role(record.record.run_kind, &record.record.run_id);
                if record.record.source_run_id.is_none() {
                    record.record.source_run_id = record
                        .record
                        .parent_run_id
                        .clone()
                        .or_else(|| {
                            record.record.notes.iter().find_map(|note| {
                                note.strip_prefix("source_run_id=").map(ToOwned::to_owned)
                            })
                        })
                        .or_else(|| {
                            record
                                .record
                                .run_kind
                                .is_source_run()
                                .then(|| record.record.run_id.clone())
                        });
                }
                record
            })
            .collect::<Vec<_>>();
        runs.sort_by(|left, right| {
            run_wall_clock(right)
                .cmp(&run_wall_clock(left))
                .then_with(|| right.record.run_id.cmp(&left.record.run_id))
        });
        Ok(runs)
    }

    pub fn write_token_summary(
        &self,
        record: &StoredRecord<CompactTokenSummary>,
    ) -> Result<PathBuf, StorageError> {
        let path = scoped_snapshot_path(
            &self.layout.token_summary_dir,
            record.run_id.as_deref(),
            record.scenario_id.as_deref(),
            &format!("{}.json", record.record.mint),
        )?;
        write_json(&path, record)?;
        Ok(path)
    }

    pub fn read_token_summary(
        &self,
        run_id: Option<&str>,
        scenario_id: Option<&str>,
        mint: &str,
    ) -> Result<Option<StoredRecord<CompactTokenSummary>>, StorageError> {
        let path = self
            .layout
            .token_summary_dir
            .join(run_id.unwrap_or("_legacy"))
            .join(scenario_id.unwrap_or("_all"))
            .join(format!("{mint}.json"));
        if path.exists() {
            return Ok(Some(read_json(&path)?));
        }
        let legacy = self.layout.token_summary_dir.join(format!("{mint}.json"));
        if legacy.exists() {
            return Ok(Some(read_json(&legacy)?));
        }
        Ok(None)
    }

    pub fn write_wallet_summary(
        &self,
        wallet: &WalletSummary,
        config_hash: &str,
        idl_hash: &str,
    ) -> Result<PathBuf, StorageError> {
        let path = self
            .layout
            .report_dir
            .join(format!("wallet_{}.json", wallet.wallet));
        let record = StoredRecord::new(
            DatasetKind::WalletSummary,
            config_hash,
            idl_hash,
            None,
            "state",
            "snapshot",
            None,
            wallet,
        );
        write_json(&path, &record)?;
        Ok(path)
    }

    pub fn write_feature_snapshot(
        &self,
        record: &StoredRecord<FeatureSnapshot>,
    ) -> Result<PathBuf, StorageError> {
        if self.feature_snapshot_persistence.disable_per_snapshot_files {
            return self.write_feature_snapshot_chunked(record);
        }
        let path = scoped_snapshot_path(
            &self.layout.feature_snapshot_dir,
            record.run_id.as_deref(),
            record.scenario_id.as_deref(),
            &format!(
                "{}_{}.json",
                record.record.mint.0,
                record.record.observed_at.unix_timestamp_nanos()
            ),
        )?;
        write_json(&path, record)?;
        Ok(path)
    }

    fn write_feature_snapshot_chunked(
        &self,
        record: &StoredRecord<FeatureSnapshot>,
    ) -> Result<PathBuf, StorageError> {
        let run_id = record.run_id.as_deref().unwrap_or("_legacy");
        let scenario_id = record.scenario_id.as_deref().unwrap_or("_all");
        let base_dir = self
            .layout
            .feature_snapshot_dir
            .join(run_id)
            .join(scenario_id);
        fs::create_dir_all(&base_dir)?;
        let mut writers = feature_snapshot_writers().lock().map_err(|_| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "feature snapshot writer lock poisoned",
            ))
        })?;
        let chunk_rows = self
            .feature_snapshot_persistence
            .feature_snapshot_chunk_rows
            .max(1);
        if let Some(existing) = writers.get_mut(&base_dir) {
            if existing.row_count >= chunk_rows {
                let existing = writers.remove(&base_dir).expect("writer exists");
                finalize_feature_chunk_writer(existing, &self.feature_snapshot_persistence)?;
            }
        }
        if !writers.contains_key(&base_dir) {
            let chunk_index = next_feature_snapshot_chunk_index(&base_dir)?;
            writers.insert(
                base_dir.clone(),
                FeatureSnapshotChunkWriter::new(base_dir.clone(), chunk_index)?,
            );
        }
        let writer = writers.get_mut(&base_dir).expect("writer inserted");
        let mut raw = serde_json::to_vec(record)?;
        raw.push(b'\n');
        writer.writer.write_all(&raw)?;
        writer.row_count = writer.row_count.saturating_add(1);
        Ok(writer.open_path.clone())
    }

    pub fn finalize_feature_snapshot_chunks_for_run(
        &self,
        run_id: &str,
        scenario_id: Option<&str>,
    ) -> Result<(), StorageError> {
        let run_dir = self.layout.feature_snapshot_dir.join(run_id);
        let target_dir = scenario_id.map(|scenario| run_dir.join(scenario));
        {
            let mut writers = feature_snapshot_writers().lock().map_err(|_| {
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "feature snapshot writer lock poisoned",
                ))
            })?;
            let keys = writers
                .keys()
                .filter(|path| {
                    target_dir
                        .as_ref()
                        .map(|target| path.starts_with(target))
                        .unwrap_or_else(|| path.starts_with(&run_dir))
                })
                .cloned()
                .collect::<Vec<_>>();
            for key in keys {
                if let Some(writer) = writers.remove(&key) {
                    finalize_feature_chunk_writer(writer, &self.feature_snapshot_persistence)?;
                }
            }
        }
        let manifest_roots = if let Some(target) = target_dir {
            vec![target]
        } else {
            feature_snapshot_leaf_dirs(&run_dir)?
        };
        for dir in manifest_roots {
            finalize_open_feature_chunks_in_dir(&dir, &self.feature_snapshot_persistence)?;
            write_feature_snapshot_chunk_manifest(&dir)?;
        }
        Ok(())
    }

    pub fn export_feature_snapshots_csv(
        &self,
        records: &[StoredRecord<FeatureSnapshot>],
        associated_run_id: Option<&str>,
        filename: &str,
    ) -> Result<PathBuf, StorageError> {
        let path = report_artifact_path(&self.layout, associated_run_id, filename);
        let mut file = BufWriter::new(File::create(&path)?);
        let feature_engine = FeatureEngine::default();
        let run_lookup = latest_run_lookup(self.list_runs()?);
        let mut decision_lookup = std::collections::HashMap::<
            (String, Option<String>, String),
            StoredRecord<TradeDecisionEvent>,
        >::new();
        let decision_records = if let Some(run_id) = associated_run_id {
            self.read_trade_decisions_filtered(Some(run_id), None)?
        } else {
            self.read_trade_decisions()?
        };
        for record in decision_records {
            let Some(run_id) = &record.run_id else {
                continue;
            };
            decision_lookup
                .entry((
                    run_id.clone(),
                    record.scenario_id.clone(),
                    record.record.feature_snapshot_hash.clone(),
                ))
                .or_insert(record);
        }
        writeln!(
            file,
            "run_id,parent_run_id,source_run_id,scenario_id,run_kind,run_role,schema_version,config_hash,idl_hash,calibration_snapshot_hash,runtime_sequence_number,event_id,decision_id,mint,strategy,feature_snapshot_hash,feature_name,feature_category,feature_value,feature_status,feature_confidence,feature_timestamp,source,canonicality"
        )?;
        let mut sorted = records.to_vec();
        sorted.sort_by(|left, right| {
            left.run_id
                .cmp(&right.run_id)
                .then_with(|| left.scenario_id.cmp(&right.scenario_id))
                .then_with(|| {
                    left.runtime_sequence_number
                        .cmp(&right.runtime_sequence_number)
                })
                .then_with(|| left.record.mint.0.cmp(&right.record.mint.0))
                .then_with(|| left.record.observed_at.cmp(&right.record.observed_at))
                .then_with(|| left.source_event_id.cmp(&right.source_event_id))
        });
        for record in sorted {
            let run_metadata = record
                .run_id
                .as_deref()
                .and_then(|run_id| run_lookup.get(run_id));
            let key = (
                record.run_id.clone().unwrap_or_default(),
                record.scenario_id.clone(),
                record.record.vector_hash.clone(),
            );
            let decision = decision_lookup.get(&key);
            let mut feature_ids = record.record.values.keys().cloned().collect::<Vec<_>>();
            feature_ids.sort();
            for feature_id in feature_ids {
                let feature = record.record.values.get(&feature_id).expect("feature");
                let value = feature_datum_to_string(&feature.value);
                let row = vec![
                    record.run_id.clone().unwrap_or_default(),
                    run_metadata
                        .and_then(|metadata| metadata.record.parent_run_id.clone())
                        .unwrap_or_default(),
                    run_metadata
                        .and_then(|metadata| metadata.record.source_run_id.clone())
                        .unwrap_or_default(),
                    record.scenario_id.clone().unwrap_or_default(),
                    run_metadata
                        .map(|metadata| run_kind_label(metadata.record.run_kind))
                        .unwrap_or_else(|| run_kind_label(RunKind::Unknown)),
                    run_metadata
                        .map(|metadata| run_role_label(metadata.record.run_role))
                        .unwrap_or_else(|| run_role_label(RunRole::Unknown)),
                    record.schema_version.to_string(),
                    record.config_hash.clone(),
                    record.idl_hash.clone(),
                    record
                        .calibration_snapshot_hash
                        .clone()
                        .or_else(|| {
                            run_metadata.and_then(|metadata| {
                                metadata.record.calibration_snapshot_hash.clone()
                            })
                        })
                        .unwrap_or_default(),
                    record
                        .runtime_sequence_number
                        .unwrap_or_default()
                        .to_string(),
                    record.source_event_id.clone().unwrap_or_default(),
                    decision
                        .map(|decision| decision.record.decision_id.clone())
                        .unwrap_or_default(),
                    record.record.mint.0.clone(),
                    decision
                        .map(|decision| decision.record.strategy.clone())
                        .unwrap_or_default(),
                    record.record.vector_hash.clone(),
                    feature_id.clone(),
                    feature_engine
                        .registry()
                        .descriptor(&feature_id)
                        .map(|descriptor| descriptor.category.clone())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    value,
                    format!("{:?}", feature.status).to_lowercase(),
                    decimal_string(feature.confidence),
                    record.record.observed_at.unix_timestamp_nanos().to_string(),
                    record.source.clone(),
                    record.canonicality.clone(),
                ];
                write_csv_row(&mut file, &row)?;
            }
        }
        Ok(path)
    }

    pub fn deterministic_replay(&self) -> Result<Vec<StoredRecord<NormalizedEvent>>, StorageError> {
        self.deterministic_replay_for_run(None)
    }

    pub fn deterministic_replay_for_run(
        &self,
        run_id: Option<&str>,
    ) -> Result<Vec<StoredRecord<NormalizedEvent>>, StorageError> {
        self.deterministic_replay_for_run_and_scenario(run_id, None)
    }

    pub fn deterministic_replay_for_run_and_scenario(
        &self,
        run_id: Option<&str>,
        scenario_id: Option<&str>,
    ) -> Result<Vec<StoredRecord<NormalizedEvent>>, StorageError> {
        let mut events = self
            .read_normalized_events_filtered(run_id, scenario_id)?
            .into_iter()
            .filter(|record| record.record.is_replay_source_event())
            .collect::<Vec<_>>();
        if events
            .iter()
            .all(|record| record.runtime_sequence_number.is_some())
        {
            events.sort_by(|left, right| {
                left.runtime_sequence_number
                    .cmp(&right.runtime_sequence_number)
                    .then_with(|| {
                        left.record
                            .meta
                            .event_id
                            .0
                            .cmp(&right.record.meta.event_id.0)
                    })
            });
        } else {
            events.sort_by(|left, right| {
                left.no_lookahead_timestamp
                    .cmp(&right.no_lookahead_timestamp)
                    .then_with(|| left.record.meta.slot.cmp(&right.record.meta.slot))
                    .then_with(|| {
                        left.record
                            .meta
                            .event_id
                            .0
                            .cmp(&right.record.meta.event_id.0)
                    })
            });
        }
        Ok(events)
    }

    pub fn config_fingerprint(config_hash: &str, idl_hash: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(config_hash.as_bytes());
        hasher.update(idl_hash.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    pub fn read_feature_snapshots_for_mint(
        &self,
        run_id: Option<&str>,
        scenario_id: Option<&str>,
        mint: &str,
    ) -> Result<Vec<StoredRecord<FeatureSnapshot>>, StorageError> {
        self.read_all_feature_snapshots().map(|records| {
            records
                .into_iter()
                .filter(|record| {
                    record.record.mint.0 == mint
                        && run_id
                            .map(|value| record.run_id.as_deref() == Some(value))
                            .unwrap_or(true)
                        && scenario_id
                            .map(|value| record.scenario_id.as_deref() == Some(value))
                            .unwrap_or(true)
                })
                .collect()
        })
    }

    pub fn read_feature_snapshots_filtered(
        &self,
        run_id: Option<&str>,
        scenario_id: Option<&str>,
    ) -> Result<Vec<StoredRecord<FeatureSnapshot>>, StorageError> {
        match run_id {
            Some(run_id) => {
                let base_dir = match scenario_id {
                    Some(scenario_id) => self
                        .layout
                        .feature_snapshot_dir
                        .join(run_id)
                        .join(scenario_id),
                    None => self.layout.feature_snapshot_dir.join(run_id),
                };
                let mut out = Vec::new();
                read_feature_snapshot_dir(&base_dir, &mut out)?;
                out.retain(|record| {
                    record.run_id.as_deref() == Some(run_id)
                        && scenario_id
                            .map(|value| record.scenario_id.as_deref() == Some(value))
                            .unwrap_or(true)
                });
                out.sort_by(|left, right| left.record.observed_at.cmp(&right.record.observed_at));
                Ok(out)
            }
            None => self.read_all_feature_snapshots(),
        }
    }

    pub fn read_all_feature_snapshots(
        &self,
    ) -> Result<Vec<StoredRecord<FeatureSnapshot>>, StorageError> {
        let mut out: Vec<StoredRecord<FeatureSnapshot>> = Vec::new();
        read_feature_snapshot_dir(&self.layout.feature_snapshot_dir, &mut out)?;
        out.sort_by(|left, right| left.record.observed_at.cmp(&right.record.observed_at));
        Ok(out)
    }

    pub fn latest_feature_snapshot_run_id(&self) -> Result<Option<String>, StorageError> {
        let records = self.read_all_feature_snapshots()?;
        let run_ids = records
            .iter()
            .filter_map(|record| record.run_id.clone())
            .collect::<std::collections::HashSet<_>>();
        if let Some(run_id) = self.latest_completed_run_id_for(&run_ids)? {
            return Ok(Some(run_id));
        }
        Ok(latest_run_id(records.iter().map(|record| {
            (Some(record.record.observed_at), record.run_id.clone())
        })))
    }

    pub fn read_all_token_summaries(
        &self,
    ) -> Result<Vec<StoredRecord<CompactTokenSummary>>, StorageError> {
        let mut out: Vec<StoredRecord<CompactTokenSummary>> = Vec::new();
        read_token_summary_dir(&self.layout.token_summary_dir, &mut out)?;
        out.sort_by(|left, right| left.record.mint.cmp(&right.record.mint));
        Ok(out)
    }

    pub fn read_token_summaries_filtered(
        &self,
        run_id: Option<&str>,
        scenario_id: Option<&str>,
    ) -> Result<Vec<StoredRecord<CompactTokenSummary>>, StorageError> {
        match run_id {
            Some(run_id) => {
                let base_dir = match scenario_id {
                    Some(scenario_id) => {
                        self.layout.token_summary_dir.join(run_id).join(scenario_id)
                    }
                    None => self.layout.token_summary_dir.join(run_id),
                };
                let mut out = Vec::new();
                read_token_summary_dir(&base_dir, &mut out)?;
                out.retain(|record| {
                    record.run_id.as_deref() == Some(run_id)
                        && scenario_id
                            .map(|value| record.scenario_id.as_deref() == Some(value))
                            .unwrap_or(true)
                });
                out.sort_by(|left, right| left.record.mint.cmp(&right.record.mint));
                Ok(out)
            }
            None => self.read_all_token_summaries(),
        }
    }

    pub fn export_trade_decisions_csv(
        &self,
        records: &[StoredRecord<TradeDecisionEvent>],
        associated_run_id: Option<&str>,
        filename: &str,
    ) -> Result<PathBuf, StorageError> {
        let path = report_artifact_path(&self.layout, associated_run_id, filename);
        let mut file = BufWriter::new(File::create(&path)?);
        let run_lookup = latest_run_lookup(self.list_runs()?);
        let feature_records = if let Some(run_id) = associated_run_id {
            self.read_feature_snapshots_filtered(Some(run_id), None)?
        } else {
            self.read_all_feature_snapshots()?
        };
        let feature_lookup = feature_snapshot_lookup(feature_records);
        writeln!(
            file,
            "run_id,source_run_id,scenario_id,run_kind,run_role,schema_version,config_hash,idl_hash,calibration_snapshot_hash,runtime_sequence_number,event_id,source_event_id,trigger_event_id,decision_id,mint,strategy,decision_type,reason_codes,feature_snapshot_hash,risk_snapshot_hash,expected_net_edge_quote,expected_net_edge_pct,expected_executable_edge_confidence,shred_warning_active,emergency_exit_net_benefit,data_gap_scope,source,canonicality,decision_time"
        )?;
        let mut sorted = records.to_vec();
        sorted.sort_by(|left, right| {
            left.run_id
                .cmp(&right.run_id)
                .then_with(|| left.scenario_id.cmp(&right.scenario_id))
                .then_with(|| {
                    left.runtime_sequence_number
                        .cmp(&right.runtime_sequence_number)
                })
                .then_with(|| {
                    left.record
                        .no_lookahead_timestamp
                        .cmp(&right.record.no_lookahead_timestamp)
                })
                .then_with(|| left.record.mint.0.cmp(&right.record.mint.0))
                .then_with(|| left.record.decision_id.cmp(&right.record.decision_id))
        });
        for record in sorted {
            let run_metadata = record
                .run_id
                .as_deref()
                .and_then(|run_id| run_lookup.get(run_id));
            let feature_snapshot = feature_lookup.get(&(
                record.run_id.clone().unwrap_or_default(),
                record.scenario_id.clone(),
                record.record.feature_snapshot_hash.clone(),
            ));
            let reason_codes = sorted_reason_codes(&record.record.reason_codes);
            let row = vec![
                record.run_id.clone().unwrap_or_default(),
                run_metadata
                    .and_then(|metadata| metadata.record.source_run_id.clone())
                    .unwrap_or_default(),
                record.scenario_id.clone().unwrap_or_default(),
                run_metadata
                    .map(|metadata| run_kind_label(metadata.record.run_kind))
                    .unwrap_or_else(|| run_kind_label(RunKind::Unknown)),
                run_metadata
                    .map(|metadata| run_role_label(metadata.record.run_role))
                    .unwrap_or_else(|| run_role_label(RunRole::Unknown)),
                record.schema_version.to_string(),
                record.config_hash.clone(),
                record.idl_hash.clone(),
                record
                    .calibration_snapshot_hash
                    .clone()
                    .or_else(|| {
                        run_metadata
                            .and_then(|metadata| metadata.record.calibration_snapshot_hash.clone())
                    })
                    .unwrap_or_default(),
                record
                    .runtime_sequence_number
                    .unwrap_or_default()
                    .to_string(),
                record.source_event_id.clone().unwrap_or_default(),
                record.source_event_id.clone().unwrap_or_default(),
                record.trigger_event_id.clone().unwrap_or_default(),
                record.record.decision_id.clone(),
                record.record.mint.0.clone(),
                record.record.strategy.clone(),
                format!("{:?}", record.record.decision).to_lowercase(),
                reason_codes,
                record.record.feature_snapshot_hash.clone(),
                risk_vector_hash(&record.record.risk_vector),
                decimal_string(record.record.expected_net_edge_quote),
                decimal_string(record.record.expected_net_edge_pct),
                decimal_string(record.record.expected_edge_confidence),
                record
                    .record
                    .reason_codes
                    .iter()
                    .any(is_shred_reason_code)
                    .to_string(),
                decimal_string(
                    feature_snapshot
                        .and_then(|snapshot| snapshot.record.decimal("emergency_exit_net_benefit"))
                        .unwrap_or(Decimal::ZERO),
                ),
                if record
                    .record
                    .reason_codes
                    .iter()
                    .any(|reason| matches!(reason, ReasonCode::DataGap | ReasonCode::DataGapActive))
                {
                    "active".to_owned()
                } else {
                    String::new()
                },
                record.source.clone(),
                record.canonicality.clone(),
                record
                    .record
                    .no_lookahead_timestamp
                    .unix_timestamp_nanos()
                    .to_string(),
            ];
            write_csv_row(&mut file, &row)?;
        }
        Ok(path)
    }

    pub fn export_fills_csv(
        &self,
        records: &[StoredRecord<FillEvent>],
        associated_run_id: Option<&str>,
        filename: &str,
    ) -> Result<PathBuf, StorageError> {
        let path = report_artifact_path(&self.layout, associated_run_id, filename);
        let mut file = BufWriter::new(File::create(&path)?);
        let run_lookup = latest_run_lookup(self.list_runs()?);
        writeln!(
            file,
            "run_id,source_run_id,scenario_id,run_kind,run_role,schema_version,config_hash,idl_hash,calibration_snapshot_hash,runtime_sequence_number,fill_id,position_id,entry_decision_id,exit_decision_id,trigger_event_id,mint,strategy,side,fill_price,fill_qty,quote_notional,gross_pnl,net_pnl,base_fee,priority_fee,tip_fee,slippage_cost,curve_impact_cost,latency_cost,failed_tx_cost,exit_source,malicious_sell_signature,malicious_sell_seller,malicious_sell_classification,estimated_loss_saved,realized_loss_saved,opportunity_cost_if_false_positive,false_positive_exit,early_intent_to_geyser_processed_latency_ms,early_intent_to_account_effect_latency_ms,early_intent_to_rooted_latency_ms,exit_latency_ms,fill_time"
        )?;
        let mut sorted = records.to_vec();
        sorted.sort_by(|left, right| {
            left.run_id
                .cmp(&right.run_id)
                .then_with(|| left.scenario_id.cmp(&right.scenario_id))
                .then_with(|| {
                    left.runtime_sequence_number
                        .cmp(&right.runtime_sequence_number)
                })
                .then_with(|| left.record.signal_time.cmp(&right.record.signal_time))
                .then_with(|| left.record.mint.0.cmp(&right.record.mint.0))
        });
        for record in sorted {
            let run_metadata = record
                .run_id
                .as_deref()
                .and_then(|run_id| run_lookup.get(run_id));
            let row = vec![
                record.run_id.clone().unwrap_or_default(),
                run_metadata
                    .and_then(|metadata| metadata.record.source_run_id.clone())
                    .unwrap_or_default(),
                record.scenario_id.clone().unwrap_or_default(),
                run_metadata
                    .map(|metadata| run_kind_label(metadata.record.run_kind))
                    .unwrap_or_else(|| run_kind_label(RunKind::Unknown)),
                run_metadata
                    .map(|metadata| run_role_label(metadata.record.run_role))
                    .unwrap_or_else(|| run_role_label(RunRole::Unknown)),
                record.schema_version.to_string(),
                record.config_hash.clone(),
                record.idl_hash.clone(),
                record
                    .calibration_snapshot_hash
                    .clone()
                    .or_else(|| {
                        run_metadata
                            .and_then(|metadata| metadata.record.calibration_snapshot_hash.clone())
                    })
                    .unwrap_or_default(),
                record
                    .runtime_sequence_number
                    .unwrap_or_default()
                    .to_string(),
                fill_row_id(&record),
                fill_position_id(&record),
                record.record.entry_decision_id.clone().unwrap_or_default(),
                record.record.exit_decision_id.clone().unwrap_or_default(),
                record.record.trigger_event_id.clone().unwrap_or_default(),
                record.record.mint.0.clone(),
                record.record.strategy.clone().unwrap_or_default(),
                format!("{:?}", record.record.side).to_lowercase(),
                decimal_string(record.record.fill_price),
                decimal_string(record.record.filled_size),
                decimal_string(record.record.notional),
                decimal_string(record.record.gross_pnl_quote.unwrap_or(Decimal::ZERO)),
                decimal_string(record.record.net_pnl_quote.unwrap_or(Decimal::ZERO)),
                decimal_string(record.record.base_fee_quote.unwrap_or(Decimal::ZERO)),
                decimal_string(record.record.priority_fee_quote.unwrap_or(Decimal::ZERO)),
                decimal_string(record.record.tip_quote.unwrap_or(Decimal::ZERO)),
                decimal_string(record.record.slippage_cost_quote.unwrap_or(Decimal::ZERO)),
                decimal_string(
                    record
                        .record
                        .curve_impact_cost_quote
                        .unwrap_or(Decimal::ZERO),
                ),
                decimal_string(record.record.latency_cost_quote.unwrap_or(Decimal::ZERO)),
                decimal_string(record.record.failed_tx_cost_quote.unwrap_or(Decimal::ZERO)),
                record.record.exit_source.clone().unwrap_or_default(),
                record
                    .record
                    .malicious_sell_signature
                    .clone()
                    .unwrap_or_default(),
                record
                    .record
                    .malicious_sell_seller
                    .clone()
                    .unwrap_or_default(),
                record
                    .record
                    .malicious_sell_classification
                    .clone()
                    .unwrap_or_default(),
                decimal_string(
                    record
                        .record
                        .estimated_loss_saved_quote
                        .unwrap_or(Decimal::ZERO),
                ),
                decimal_string(
                    record
                        .record
                        .realized_loss_saved_quote
                        .unwrap_or(Decimal::ZERO),
                ),
                decimal_string(
                    record
                        .record
                        .opportunity_cost_if_false_positive
                        .unwrap_or(Decimal::ZERO),
                ),
                record.record.false_positive_exit.to_string(),
                record
                    .record
                    .early_intent_to_geyser_processed_latency_ms
                    .unwrap_or_default()
                    .to_string(),
                record
                    .record
                    .early_intent_to_account_effect_latency_ms
                    .unwrap_or_default()
                    .to_string(),
                record
                    .record
                    .early_intent_to_rooted_latency_ms
                    .unwrap_or_default()
                    .to_string(),
                record
                    .record
                    .exit_latency_ms
                    .unwrap_or_default()
                    .to_string(),
                fill_time_for_export(&record)
                    .unix_timestamp_nanos()
                    .to_string(),
            ];
            write_csv_row(&mut file, &row)?;
        }
        Ok(path)
    }
}

fn report_artifact_path(
    layout: &StorageLayout,
    associated_run_id: Option<&str>,
    filename: &str,
) -> PathBuf {
    associated_run_id
        .map(|run_id| layout.report_dir.join(run_id).join(filename))
        .unwrap_or_else(|| layout.report_dir.join(filename))
}

fn latest_run_lookup(
    runs: Vec<StoredRecord<RunMetadata>>,
) -> std::collections::HashMap<String, StoredRecord<RunMetadata>> {
    runs.into_iter()
        .map(|record| (record.record.run_id.clone(), record))
        .collect()
}

fn feature_snapshot_lookup(
    records: Vec<StoredRecord<FeatureSnapshot>>,
) -> std::collections::HashMap<(String, Option<String>, String), StoredRecord<FeatureSnapshot>> {
    let mut lookup = std::collections::HashMap::new();
    for record in records {
        let Some(run_id) = &record.run_id else {
            continue;
        };
        lookup
            .entry((
                run_id.clone(),
                record.scenario_id.clone(),
                record.record.vector_hash.clone(),
            ))
            .or_insert(record);
    }
    lookup
}

fn run_kind_label(kind: RunKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn run_role_label(role: RunRole) -> String {
    serde_json::to_value(role)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn feature_datum_to_string(value: &Option<FeatureDatum>) -> String {
    match value {
        Some(FeatureDatum::Numeric(value)) => decimal_string(*value),
        Some(FeatureDatum::Text(value)) => value.clone(),
        Some(FeatureDatum::Boolean(value)) => value.to_string(),
        None => String::new(),
    }
}

fn decimal_string(value: Decimal) -> String {
    value.normalize().to_string()
}

fn sorted_reason_codes(reason_codes: &[ReasonCode]) -> String {
    let mut items = reason_codes
        .iter()
        .map(|reason| format!("{reason:?}").to_lowercase())
        .collect::<Vec<_>>();
    items.sort();
    items.join("|")
}

fn is_shred_reason_code(reason: &ReasonCode) -> bool {
    matches!(
        reason,
        ReasonCode::ShredDevSellWarning
            | ReasonCode::ShredTopHolderSellWarning
            | ReasonCode::ShredBundleExitWarning
            | ReasonCode::ShredWhaleDumpWarning
            | ReasonCode::ShredSellImpactHigh
            | ReasonCode::ShredSameSlotSellCluster
            | ReasonCode::ShredExitArmed
            | ReasonCode::ShredEmergencyExit
            | ReasonCode::ShredFalsePositive
            | ReasonCode::ShredSavedLoss
            | ReasonCode::ShredSignalStale
            | ReasonCode::ShredLowConfidence
            | ReasonCode::DeshredPreExecutionSellWarning
            | ReasonCode::EarlyIntentSellWarning
    )
}

fn risk_vector_hash(risk_vector: &BTreeMap<String, Decimal>) -> String {
    let mut hasher = Sha256::new();
    for (key, value) in risk_vector {
        hasher.update(key.as_bytes());
        hasher.update(b"=");
        hasher.update(decimal_string(*value).as_bytes());
        hasher.update(b";");
    }
    format!("{:x}", hasher.finalize())
}

fn fill_row_id(record: &StoredRecord<FillEvent>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(record.run_id.clone().unwrap_or_default().as_bytes());
    hasher.update(record.scenario_id.clone().unwrap_or_default().as_bytes());
    hasher.update(record.record.mint.0.as_bytes());
    hasher.update(format!("{:?}", record.record.side).as_bytes());
    hasher.update(
        record
            .record
            .signal_time
            .unix_timestamp_nanos()
            .to_string()
            .as_bytes(),
    );
    hasher.update(
        record
            .record
            .entry_decision_id
            .clone()
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(
        record
            .record
            .exit_decision_id
            .clone()
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(
        record
            .record
            .trigger_event_id
            .clone()
            .unwrap_or_default()
            .as_bytes(),
    );
    format!("{:x}", hasher.finalize())
}

fn fill_position_id(record: &StoredRecord<FillEvent>) -> String {
    record
        .record
        .entry_decision_id
        .clone()
        .or_else(|| record.record.exit_decision_id.clone())
        .unwrap_or_else(|| record.record.mint.0.clone())
}

fn fill_time_for_export(record: &StoredRecord<FillEvent>) -> OffsetDateTime {
    record
        .record
        .confirmation_time
        .or(record.record.landing_time)
        .unwrap_or(record.record.send_time)
}

fn csv_field(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

fn write_csv_row<W: Write>(file: &mut W, row: &[String]) -> Result<(), StorageError> {
    let rendered = row.iter().map(|cell| csv_field(cell)).collect::<Vec<_>>();
    writeln!(file, "{}", rendered.join(","))?;
    Ok(())
}

fn append_jsonl<T: Serialize>(path: &Path, record: &T) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut raw = serde_json::to_vec(record)?;
    raw.push(b'\n');
    file.write_all(&raw)?;
    Ok(())
}

fn read_jsonl<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Vec<T>, StorageError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(row) = serde_json::from_str(&line) {
            rows.push(row);
        }
    }
    Ok(rows)
}

fn read_stored_jsonl_filtered<T: for<'de> Deserialize<'de>>(
    path: &Path,
    run_id: Option<&str>,
    scenario_id: Option<&str>,
) -> Result<Vec<StoredRecord<T>>, StorageError> {
    if run_id.is_none() && scenario_id.is_none() {
        return read_jsonl(path);
    }
    if !path.exists() {
        return Ok(Vec::new());
    }

    let run_hint = run_id
        .map(|value| serde_json::to_string(value).map(|encoded| format!("\"run_id\":{encoded}")))
        .transpose()?;
    let scenario_hint = scenario_id
        .map(|value| {
            serde_json::to_string(value).map(|encoded| format!("\"scenario_id\":{encoded}"))
        })
        .transpose()?;
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if let Some(run_id) = run_id {
            let hint_matches = run_hint
                .as_ref()
                .map(|hint| line.contains(hint))
                .unwrap_or(false);
            if !hint_matches && !line.contains(run_id) {
                continue;
            }
        }
        if let Some(scenario_id) = scenario_id {
            let hint_matches = scenario_hint
                .as_ref()
                .map(|hint| line.contains(hint))
                .unwrap_or(false);
            if !hint_matches && !line.contains(scenario_id) {
                continue;
            }
        }
        if let Ok(row) = serde_json::from_str::<StoredRecord<T>>(&line) {
            if run_id
                .map(|value| row.run_id.as_deref() != Some(value))
                .unwrap_or(false)
            {
                continue;
            }
            if scenario_id
                .map(|value| row.scenario_id.as_deref() != Some(value))
                .unwrap_or(false)
            {
                continue;
            }
            rows.push(row);
        }
    }
    Ok(rows)
}

fn write_json<T: Serialize>(path: &Path, record: &T) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let raw = serde_json::to_vec_pretty(record)?;
    let temp_name = format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("artifact"),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    );
    let temp_path = path.with_file_name(temp_name);
    {
        let mut file = File::create(&temp_path)?;
        file.write_all(&raw)?;
        file.sync_all()?;
    }
    if let Err(err) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(err.into());
    }
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, StorageError> {
    let raw = fs::read(path)?;
    Ok(serde_json::from_slice(&raw)?)
}

fn read_json_best_effort<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<Option<T>, StorageError> {
    let raw = match fs::read(path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    match serde_json::from_slice(&raw) {
        Ok(record) => Ok(Some(record)),
        Err(err) => {
            eprintln!(
                "warning: skipping malformed JSON artifact {}: {}",
                path.display(),
                err
            );
            Ok(None)
        }
    }
}

fn filter_records<T>(
    records: &mut Vec<StoredRecord<T>>,
    run_id: Option<&str>,
    scenario_id: Option<&str>,
) {
    if let Some(run_id) = run_id {
        records.retain(|record| record.run_id.as_deref() == Some(run_id));
    }
    if let Some(scenario_id) = scenario_id {
        records.retain(|record| record.scenario_id.as_deref() == Some(scenario_id));
    }
}

fn scoped_snapshot_path(
    root: &Path,
    run_id: Option<&str>,
    scenario_id: Option<&str>,
    filename: &str,
) -> Result<PathBuf, StorageError> {
    let path = root
        .join(run_id.unwrap_or("_legacy"))
        .join(scenario_id.unwrap_or("_all"))
        .join(filename);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(path)
}

struct FeatureSnapshotChunkWriter {
    base_dir: PathBuf,
    open_path: PathBuf,
    chunk_index: usize,
    row_count: usize,
    writer: BufWriter<File>,
}

impl FeatureSnapshotChunkWriter {
    fn new(base_dir: PathBuf, chunk_index: usize) -> Result<Self, StorageError> {
        fs::create_dir_all(&base_dir)?;
        let open_path = base_dir.join(format!("feature_snapshots_{chunk_index:05}.jsonl.open"));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&open_path)?;
        Ok(Self {
            base_dir,
            open_path,
            chunk_index,
            row_count: 0,
            writer: BufWriter::new(file),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FeatureSnapshotChunkManifest {
    format: String,
    total_rows: usize,
    chunks: Vec<FeatureSnapshotChunkManifestEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FeatureSnapshotChunkManifestEntry {
    path: String,
    chunk_index: usize,
    row_count: usize,
    size_bytes: u64,
    checksum_sha256: String,
    compression: String,
}

static FEATURE_SNAPSHOT_WRITERS: OnceLock<Mutex<BTreeMap<PathBuf, FeatureSnapshotChunkWriter>>> =
    OnceLock::new();

fn feature_snapshot_writers() -> &'static Mutex<BTreeMap<PathBuf, FeatureSnapshotChunkWriter>> {
    FEATURE_SNAPSHOT_WRITERS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn next_feature_snapshot_chunk_index(base_dir: &Path) -> Result<usize, StorageError> {
    let mut max_index = 0usize;
    if base_dir.exists() {
        for entry in fs::read_dir(base_dir)? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if let Some(index) = parse_feature_chunk_index(name) {
                max_index = max_index.max(index);
            }
        }
    }
    Ok(max_index.saturating_add(1))
}

fn parse_feature_chunk_index(name: &str) -> Option<usize> {
    let rest = name.strip_prefix("feature_snapshots_")?;
    let digits = rest.get(..5)?;
    if !digits.chars().all(|value| value.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn finalize_feature_chunk_writer(
    mut writer: FeatureSnapshotChunkWriter,
    config: &ResearchWorkerPersistenceConfig,
) -> Result<PathBuf, StorageError> {
    writer.writer.flush()?;
    writer.writer.get_ref().sync_all()?;
    drop(writer.writer);
    finalize_feature_chunk_file(
        &writer.base_dir,
        writer.chunk_index,
        &writer.open_path,
        config,
    )
}

fn finalize_feature_chunk_file(
    base_dir: &Path,
    chunk_index: usize,
    open_path: &Path,
    config: &ResearchWorkerPersistenceConfig,
) -> Result<PathBuf, StorageError> {
    if !open_path.exists() {
        return Ok(open_path.to_path_buf());
    }
    let compress = config.feature_snapshot_compress
        || config
            .feature_snapshot_format
            .trim()
            .eq_ignore_ascii_case("jsonl.zst");
    let final_path = if compress {
        base_dir.join(format!("feature_snapshots_{chunk_index:05}.jsonl.zst"))
    } else {
        base_dir.join(format!("feature_snapshots_{chunk_index:05}.jsonl"))
    };
    if compress {
        let mut input = File::open(open_path)?;
        let mut output = File::create(&final_path)?;
        zstd::stream::copy_encode(&mut input, &mut output, 3)?;
        output.sync_all()?;
        fs::remove_file(open_path)?;
    } else {
        fs::rename(open_path, &final_path)?;
    }
    Ok(final_path)
}

fn finalize_open_feature_chunks_in_dir(
    dir: &Path,
    config: &ResearchWorkerPersistenceConfig,
) -> Result<(), StorageError> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            finalize_open_feature_chunks_in_dir(&path, config)?;
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if !name.ends_with(".jsonl.open") {
            continue;
        }
        if let Some(index) = parse_feature_chunk_index(name) {
            finalize_feature_chunk_file(dir, index, &path, config)?;
        }
    }
    Ok(())
}

fn feature_snapshot_leaf_dirs(run_dir: &Path) -> Result<Vec<PathBuf>, StorageError> {
    let mut dirs = Vec::new();
    if !run_dir.exists() {
        return Ok(dirs);
    }
    for entry in fs::read_dir(run_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            dirs.extend(feature_snapshot_leaf_dirs(&path)?);
        }
    }
    if dirs.is_empty() {
        dirs.push(run_dir.to_path_buf());
    }
    Ok(dirs)
}

fn write_feature_snapshot_chunk_manifest(dir: &Path) -> Result<(), StorageError> {
    if !dir.exists() {
        return Ok(());
    }
    let mut entries = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !is_feature_snapshot_chunk_file(&path) {
            continue;
        }
        let raw = fs::read(&path)?;
        let decoded = read_feature_snapshot_chunk_bytes(&path)?;
        let row_count = decoded
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count();
        let mut hasher = Sha256::new();
        hasher.update(&raw);
        let checksum_sha256 = format!("{:x}", hasher.finalize());
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_owned();
        entries.push(FeatureSnapshotChunkManifestEntry {
            chunk_index: parse_feature_chunk_index(&name).unwrap_or_default(),
            path: name,
            row_count,
            size_bytes: raw.len() as u64,
            checksum_sha256,
            compression: if path
                .file_name()
                .and_then(|value| value.to_str())
                .map(|value| value.ends_with(".zst"))
                .unwrap_or(false)
            {
                "zstd".to_owned()
            } else {
                "none".to_owned()
            },
        });
    }
    entries.sort_by(|left, right| left.chunk_index.cmp(&right.chunk_index));
    let total_rows = entries.iter().map(|entry| entry.row_count).sum();
    let manifest = FeatureSnapshotChunkManifest {
        format: "stored_record_feature_snapshot_jsonl".to_owned(),
        total_rows,
        chunks: entries,
    };
    write_json(
        &dir.join("feature_snapshot_chunks_manifest.json"),
        &manifest,
    )?;
    Ok(())
}

fn is_ignored_artifact_path(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    matches!(
        name,
        ".DS_Store" | "Thumbs.db" | ".Spotlight-V100" | ".fseventsd" | "__MACOSX"
    ) || name.ends_with('~')
        || name.ends_with(".swp")
        || name.ends_with(".swo")
        || name.ends_with(".tmp")
}

fn is_feature_snapshot_chunk_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    name.starts_with("feature_snapshots_")
        && (name.ends_with(".jsonl") || name.ends_with(".jsonl.zst"))
}

fn read_feature_snapshot_chunk_bytes(path: &Path) -> Result<String, StorageError> {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return Ok(String::new());
    };
    if name.ends_with(".zst") {
        let mut file = File::open(path)?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)?;
        let decoded = zstd::decode_all(raw.as_slice())?;
        Ok(String::from_utf8_lossy(&decoded).to_string())
    } else {
        Ok(fs::read_to_string(path)?)
    }
}

fn read_feature_snapshot_chunk_file(
    path: &Path,
    out: &mut Vec<StoredRecord<FeatureSnapshot>>,
) -> Result<(), StorageError> {
    let decoded = read_feature_snapshot_chunk_bytes(path)?;
    for line in decoded.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<StoredRecord<FeatureSnapshot>>(line) {
            Ok(record) => out.push(record),
            Err(err) => eprintln!(
                "warning: skipping malformed JSON artifact {}: {}",
                path.display(),
                err
            ),
        }
    }
    Ok(())
}

fn read_feature_snapshot_dir(
    dir: &Path,
    out: &mut Vec<StoredRecord<FeatureSnapshot>>,
) -> Result<(), StorageError> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if is_ignored_artifact_path(&path) {
            continue;
        }
        if path.is_dir() {
            read_feature_snapshot_dir(&path, out)?;
        } else if path.is_file() {
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            if name == "feature_snapshot_chunks_manifest.json" || name.ends_with(".open") {
                continue;
            }
            if is_feature_snapshot_chunk_file(&path) {
                read_feature_snapshot_chunk_file(&path, out)?;
            } else if path.extension().and_then(|value| value.to_str()) == Some("json") {
                if let Some(record) = read_json_best_effort(&path)? {
                    out.push(record);
                }
            }
        }
    }
    Ok(())
}

fn read_token_summary_dir(
    dir: &Path,
    out: &mut Vec<StoredRecord<CompactTokenSummary>>,
) -> Result<(), StorageError> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if is_ignored_artifact_path(&path) {
            continue;
        }
        if path.is_dir() {
            read_token_summary_dir(&path, out)?;
        } else if path.is_file()
            && path.extension().and_then(|value| value.to_str()) == Some("json")
        {
            if let Some(record) = read_json_best_effort(&path)? {
                out.push(record);
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Default)]
struct DerivedRunEventStats {
    event_count: u64,
    contains_tentative_sell_events: bool,
    contains_shred_exit_defense_events: bool,
    tentative_sell_count: u64,
    emergency_exit_count: u64,
    saved_loss_total: Decimal,
    opportunity_cost_total: Decimal,
    false_positive_count: u64,
    decode_mismatch_count: u64,
    reorg_count: u64,
    not_seen_count: u64,
}

fn derive_run_event_stats(
    events: &[StoredRecord<NormalizedEvent>],
    fills: &[StoredRecord<FillEvent>],
) -> std::collections::HashMap<String, DerivedRunEventStats> {
    let mut stats = std::collections::HashMap::<String, DerivedRunEventStats>::new();
    for record in events {
        let Some(run_id) = record.run_id.as_ref() else {
            continue;
        };
        let entry = stats.entry(run_id.clone()).or_default();
        entry.event_count = entry.event_count.saturating_add(1);
        match &record.record.payload {
            EventPayload::TentativeSellIntentDetected(_) => {
                entry.contains_tentative_sell_events = true;
                entry.contains_shred_exit_defense_events = true;
                entry.tentative_sell_count = entry.tentative_sell_count.saturating_add(1);
            }
            EventPayload::TentativeMaliciousSellWarning(_)
            | EventPayload::ShredEmergencyExitArmed(_)
            | EventPayload::ShredEmergencyExitTriggered(_) => {
                entry.contains_shred_exit_defense_events = true;
                if matches!(
                    &record.record.payload,
                    EventPayload::ShredEmergencyExitTriggered(_)
                ) {
                    entry.emergency_exit_count = entry.emergency_exit_count.saturating_add(1);
                }
            }
            EventPayload::ShredSellIntentResolved(event) => {
                entry.contains_shred_exit_defense_events = true;
                entry.saved_loss_total += event.actual_loss_saved_if_exited.max(Decimal::ZERO);
                if event.false_positive_flag {
                    entry.false_positive_count = entry.false_positive_count.saturating_add(1);
                }
                match event.outcome {
                    common::TentativeSellResolutionOutcome::NotSeenWithinTtl => {
                        entry.not_seen_count = entry.not_seen_count.saturating_add(1);
                    }
                    common::TentativeSellResolutionOutcome::DecodeMismatch => {
                        entry.decode_mismatch_count = entry.decode_mismatch_count.saturating_add(1);
                    }
                    common::TentativeSellResolutionOutcome::Reorged => {
                        entry.reorg_count = entry.reorg_count.saturating_add(1);
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    for record in fills {
        let Some(run_id) = record.run_id.as_ref() else {
            continue;
        };
        let entry = stats.entry(run_id.clone()).or_default();
        if matches!(
            record.record.exit_source.as_deref(),
            Some("ShredEmergencyExit") | Some("DeshredEmergencyExit")
        ) {
            entry.contains_shred_exit_defense_events = true;
            entry.emergency_exit_count = entry.emergency_exit_count.saturating_add(1);
        }
        entry.opportunity_cost_total += record
            .record
            .opportunity_cost_if_false_positive
            .unwrap_or(Decimal::ZERO);
    }
    stats
}

fn infer_run_kind(
    run_id: &str,
    mode: &str,
    notes: &[String],
    contains_tentative_sell_events: bool,
) -> RunKind {
    if run_id.starts_with("fixture-suite-") {
        RunKind::StandardFixtureSuite
    } else if run_id.starts_with("shred-exit-suite-") {
        RunKind::ShredExitFixtureSuite
    } else if run_id.starts_with("fixture-") {
        RunKind::FixtureScenario
    } else if run_id.starts_with("paper-replay-") {
        RunKind::PaperReplay
    } else if run_id.starts_with("live-paper-") {
        if notes.iter().any(|note| note.contains("mode=mock_geyser")) {
            if notes.iter().any(|note| note.contains("mock_deshred=true")) {
                RunKind::MockedLivePaperDeshred
            } else if contains_tentative_sell_events
                || notes
                    .iter()
                    .any(|note| note.contains("mock_early_intent=true"))
            {
                RunKind::MockedLivePaperEarlyIntent
            } else {
                RunKind::MockedLivePaper
            }
        } else if notes.iter().any(|note| note.contains("with_deshred=true"))
            || notes
                .iter()
                .any(|note| note.contains("mode=real_geyser+deshred"))
        {
            RunKind::RealLivePaperDeshred
        } else {
            RunKind::RealLivePaper
        }
    } else if run_id.starts_with("backtest-shred-exit-") {
        RunKind::ShredExitBacktest
    } else if run_id.starts_with("backtest-") {
        RunKind::Backtest
    } else if run_id.starts_with("deshred-smoke-") {
        RunKind::DeshredSmoke
    } else if run_id.starts_with("multisource-early-intent-suite-") {
        RunKind::MultisourceEarlyIntentSuite
    } else if run_id.starts_with("report-") {
        RunKind::Report
    } else if run_id.starts_with("replay-equivalence-") {
        RunKind::ReplayEquivalence
    } else if run_id.starts_with("export-") {
        RunKind::Export
    } else if run_id.starts_with("calibration-update-") {
        RunKind::CalibrationUpdate
    } else if mode.eq_ignore_ascii_case("fixture") && run_id.contains("shred-exit") {
        RunKind::ShredExitFixtureSuite
    } else if mode.eq_ignore_ascii_case("fixture") && run_id.contains("fixture-suite") {
        RunKind::StandardFixtureSuite
    } else {
        RunKind::Unknown
    }
}

fn infer_run_role(run_kind: RunKind, run_id: &str) -> RunRole {
    match run_kind {
        RunKind::Unknown if run_id.starts_with("inspect-token-") => RunRole::AnalysisRun,
        RunKind::Unknown if run_id.starts_with("report-") => RunRole::ReportRun,
        RunKind::Unknown if run_id.starts_with("export-") => RunRole::ExportRun,
        _ => run_kind.role(),
    }
}

fn latest_run_id<I>(iter: I) -> Option<String>
where
    I: Iterator<Item = (Option<OffsetDateTime>, Option<String>)>,
{
    iter.filter_map(|(timestamp, run_id)| run_id.map(|run_id| (timestamp, run_id)))
        .max_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)))
        .map(|(_, run_id)| run_id)
}

fn latest_metadata_per_run(
    records: Vec<StoredRecord<RunMetadata>>,
) -> std::collections::HashMap<String, StoredRecord<RunMetadata>> {
    let mut folded = std::collections::HashMap::new();
    for record in records {
        let run_id = record.record.run_id.clone();
        match folded.get(&run_id) {
            Some(existing) => {
                let take_new = should_prefer_run_metadata(&record, existing);
                if take_new {
                    folded.insert(run_id, record);
                }
            }
            None => {
                folded.insert(run_id, record);
            }
        }
    }
    folded
}

fn run_wall_clock(record: &StoredRecord<RunMetadata>) -> OffsetDateTime {
    record
        .record
        .completed_at_wall_time
        .unwrap_or(record.record.created_at_wall_time)
}

fn should_prefer_run_metadata(
    candidate: &StoredRecord<RunMetadata>,
    existing: &StoredRecord<RunMetadata>,
) -> bool {
    let candidate_root = candidate.record.scenario_id.is_none();
    let existing_root = existing.record.scenario_id.is_none();
    if candidate_root != existing_root {
        return candidate_root;
    }

    run_wall_clock(candidate) > run_wall_clock(existing)
        || (run_wall_clock(candidate) == run_wall_clock(existing)
            && candidate.record.run_id > existing.record.run_id)
}

#[cfg(test)]
mod tests {
    use common::{
        Canonicality, EventMeta, EventPayload, EventSource, NormalizedEvent, PumpBuyEvent,
        QuoteAssetType, TokenCreatedEvent, TokenProgramType, TransactionStatus, TtlConfig,
        config::StorageLowDiskModeConfig,
    };
    use features::FeatureEngine;
    use rust_decimal::Decimal;
    use state::StateEngine;
    use tempfile::tempdir;
    use time::Duration;

    use super::*;

    fn pubkey(value: &str) -> common::PubkeyValue {
        common::PubkeyValue(value.to_owned())
    }

    fn storage() -> StorageEngine {
        let temp = tempdir().expect("tempdir");
        let root = temp.keep();
        let config = StorageConfig {
            root: root.join("data").display().to_string(),
            event_log_dir: "events".to_owned(),
            snapshot_dir: "snapshots".to_owned(),
            report_dir: "reports".to_owned(),
            run_metadata_log_path: "runs.jsonl".to_owned(),
            decision_log_path: "decisions.jsonl".to_owned(),
            fill_log_path: "fills.jsonl".to_owned(),
            runtime_audit_log_path: "runtime_audit.jsonl".to_owned(),
            segments: common::StorageSegmentsConfig::default(),
            local: common::StorageLocalConfig::default(),
            low_disk_mode: StorageLowDiskModeConfig::default(),
        };
        let layout = StorageLayout::from_config(&config).expect("layout");
        StorageEngine::new(layout)
    }

    fn ttl() -> TtlConfig {
        TtlConfig {
            discovered_secs: 10,
            active_light_secs: 30,
            active_deep_secs: 60,
            discarded_summary_secs: 60,
            research_sample_secs: 120,
        }
    }

    fn meta(slot: u64) -> EventMeta {
        let mut meta = EventMeta::new(EventSource::GeyserProcessed, Canonicality::Processed, slot);
        meta.received_at_wall_time = OffsetDateTime::UNIX_EPOCH + Duration::seconds(slot as i64);
        meta
    }

    fn token_created() -> NormalizedEvent {
        let mut meta = meta(1);
        meta.signature = Some("create".to_owned());
        NormalizedEvent {
            meta,
            payload: EventPayload::TokenCreated(TokenCreatedEvent {
                mint: pubkey("mint"),
                token_program: TokenProgramType::SplToken,
                quote_mint: pubkey("quote"),
                quote_asset_type: QuoteAssetType::WrappedSol,
                creator_wallet: pubkey("creator"),
                payer: pubkey("payer"),
                bonding_curve_account: pubkey("curve"),
                associated_bonding_curve_account: None,
                metadata_account: None,
                name: "Alpha".to_owned(),
                symbol: "ALP".to_owned(),
                uri: "https://example.invalid".to_owned(),
                create_instruction_variant: "create".to_owned(),
                initial_virtual_quote_reserves: Some(Decimal::from(10u64)),
                initial_virtual_token_reserves: Some(Decimal::from(1000u64)),
                initial_real_quote_reserves: Some(Decimal::from(10u64)),
                initial_real_token_reserves: Some(Decimal::from(1000u64)),
                initial_supply: Some(Decimal::from(1000u64)),
                creator_initial_buy: None,
                same_transaction_buys: 0,
                same_slot_buys: 0,
                fee_recipients: vec![],
                raw_account_list: vec![],
                launch_transaction_fingerprint: Some("fp".to_owned()),
                status: common::TransactionStatus::Success,
            }),
        }
    }

    fn buy(slot: u64) -> NormalizedEvent {
        let mut meta = meta(slot);
        meta.signature = Some(format!("buy-{slot}"));
        NormalizedEvent {
            meta,
            payload: EventPayload::PumpBuy(PumpBuyEvent {
                mint: pubkey("mint"),
                buyer: pubkey("buyer"),
                payer: pubkey("buyer"),
                quote_in: Decimal::from(10u64),
                token_out: Decimal::from(100u64),
                price_before: None,
                price_after: None,
                effective_price: Decimal::new(1, 1),
                slippage_estimate: None,
                reserves_before: None,
                reserves_after: None,
                max_quote_cost: None,
                compute_unit_limit: None,
                compute_unit_price: None,
                estimated_priority_fee_lamports: None,
                estimated_base_fee_lamports: None,
                estimated_tip_lamports: None,
                is_creator: false,
                is_known_cluster_member: false,
                is_first_buy: false,
                status: TransactionStatus::Success,
            }),
        }
    }

    #[test]
    fn event_append_and_read_roundtrip() {
        let store = storage();
        let event = token_created();
        let record = StoredRecord::new(
            DatasetKind::NormalizedEventLog,
            "config-hash",
            "idl-hash",
            Some("strategy-v1".to_owned()),
            "geyser_processed",
            "processed",
            Some(event.meta.received_at_wall_time),
            event.clone(),
        );
        store.append_normalized_event(&record).expect("append");
        let rows = store.read_normalized_events().expect("read");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].schema_version, SCHEMA_VERSION);
        assert_eq!(rows[0].record.meta.signature, event.meta.signature);
    }

    #[test]
    fn deterministic_replay_orders_events() {
        let store = storage();
        for slot in [3, 1, 2] {
            let event = buy(slot);
            let record = StoredRecord::new(
                DatasetKind::NormalizedEventLog,
                "config-hash",
                "idl-hash",
                None,
                "geyser_processed",
                "processed",
                Some(event.meta.received_at_wall_time),
                event,
            );
            store.append_normalized_event(&record).expect("append");
        }
        let replay = store.deterministic_replay().expect("replay");
        assert_eq!(replay[0].record.meta.slot, 1);
        assert_eq!(replay[2].record.meta.slot, 3);
    }

    #[test]
    fn latest_run_id_and_run_filtered_replay_work() {
        let store = storage();
        for (slot, run_id) in [(1u64, "run-a"), (2u64, "run-b")] {
            let event = buy(slot);
            let record = StoredRecord::new(
                DatasetKind::NormalizedEventLog,
                "config-hash",
                "idl-hash",
                None,
                "geyser_processed",
                "processed",
                Some(event.meta.received_at_wall_time),
                event,
            )
            .with_run_id(run_id);
            store.append_normalized_event(&record).expect("append");
        }
        let latest = store.latest_normalized_event_run_id().expect("latest");
        assert_eq!(latest.as_deref(), Some("run-b"));
        let filtered = store
            .deterministic_replay_for_run(Some("run-a"))
            .expect("filtered");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].run_id.as_deref(), Some("run-a"));
    }

    #[test]
    fn run_filtered_replay_skips_unrelated_jsonl_before_deserializing() {
        let store = storage();
        for (slot, run_id) in [(1u64, "run-a"), (2u64, "run-b")] {
            let event = buy(slot);
            let record = StoredRecord::new(
                DatasetKind::NormalizedEventLog,
                "config-hash",
                "idl-hash",
                None,
                "geyser_processed",
                "processed",
                Some(event.meta.received_at_wall_time),
                event,
            )
            .with_run_id(run_id);
            store.append_normalized_event(&record).expect("append");
        }
        let mut file = OpenOptions::new()
            .append(true)
            .open(&store.layout.normalized_event_log)
            .expect("open normalized log");
        writeln!(
            file,
            "{{\"run_id\":\"run-b\",\"record\":{{\"malformed\":\"not a StoredRecord\"}}}}"
        )
        .expect("write unrelated malformed row");
        writeln!(file, "not-json-and-not-the-target-run").expect("write malformed row");

        let filtered = store
            .deterministic_replay_for_run(Some("run-a"))
            .expect("filtered replay");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].run_id.as_deref(), Some("run-a"));
        assert_eq!(filtered[0].record.meta.slot, 1);
    }

    #[test]
    fn token_summary_persistence_and_feature_export_work() {
        let store = storage();
        let summary = CompactTokenSummary {
            mint: "mint".to_owned(),
            lifecycle: state::TokenLifecycle::ActiveLight,
            latest_price: Decimal::ONE,
            holder_count: 2,
            top1_holder_pct: Decimal::new(5, 1),
            creator_sold_pct: Decimal::ZERO,
            canonical_trade_count: 3,
            reason_codes: vec![],
        };
        let summary_record = StoredRecord::new(
            DatasetKind::TokenSummary,
            "config-hash",
            "idl-hash",
            None,
            "state",
            "snapshot",
            None,
            summary,
        );
        store
            .write_token_summary(&summary_record)
            .expect("write summary");
        let loaded = store
            .read_token_summary(None, None, "mint")
            .expect("read")
            .expect("exists");
        assert_eq!(loaded.record.mint, "mint");

        let mut engine = StateEngine::new(ttl());
        engine.apply_event(&token_created()).expect("create");
        engine.apply_event(&buy(2)).expect("buy");
        let snapshot = engine.snapshot();
        let token = snapshot.tokens.get("mint").expect("token");
        let feature_snapshot = FeatureEngine::default().compute_snapshot(
            token,
            &snapshot,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(5),
        );
        let feature_record = StoredRecord::new(
            DatasetKind::TokenFeatureSnapshots,
            "config-hash",
            "idl-hash",
            Some("strategy-v1".to_owned()),
            "feature_engine",
            "processed",
            Some(feature_snapshot.observed_at),
            feature_snapshot,
        );
        store
            .write_feature_snapshot(&feature_record)
            .expect("write feature");
        let csv = store
            .export_feature_snapshots_csv(&[feature_record], None, "features.csv")
            .expect("csv");
        assert!(csv.exists());
    }

    #[test]
    fn malformed_feature_snapshot_file_is_skipped_in_bulk_reads() {
        let store = storage();
        let mut engine = StateEngine::new(ttl());
        engine.apply_event(&token_created()).expect("create");
        let snapshot = engine.snapshot();
        let token = snapshot.tokens.get("mint").expect("token");
        let feature_snapshot = FeatureEngine::default().compute_snapshot(
            token,
            &snapshot,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(5),
        );
        let feature_record = StoredRecord::new(
            DatasetKind::TokenFeatureSnapshots,
            "config-hash",
            "idl-hash",
            Some("strategy-v1".to_owned()),
            "feature_engine",
            "processed",
            Some(feature_snapshot.observed_at),
            feature_snapshot,
        )
        .with_run_id("run-good");
        store
            .write_feature_snapshot(&feature_record)
            .expect("write feature");
        store
            .finalize_feature_snapshot_chunks_for_run("run-good", None)
            .expect("finalize feature chunks");
        let bad_dir = store
            .layout
            .feature_snapshot_dir
            .join("run-bad")
            .join("_all");
        fs::create_dir_all(&bad_dir).expect("bad dir");
        fs::write(bad_dir.join("broken.json"), b"{\"unterminated\":\"value").expect("bad write");
        fs::write(bad_dir.join(".DS_Store"), b"mac junk").expect("junk write");

        let records = store.read_all_feature_snapshots().expect("bulk read");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].run_id.as_deref(), Some("run-good"));
    }

    #[test]
    fn feature_snapshots_are_written_in_chunk_manifest() {
        let store = storage();
        let mut engine = StateEngine::new(ttl());
        engine.apply_event(&token_created()).expect("create");
        let snapshot = engine.snapshot();
        let token = snapshot.tokens.get("mint").expect("token");
        for offset in 0..3 {
            let feature_snapshot = FeatureEngine::default().compute_snapshot(
                token,
                &snapshot,
                OffsetDateTime::UNIX_EPOCH + Duration::seconds(5 + offset),
            );
            let feature_record = StoredRecord::new(
                DatasetKind::TokenFeatureSnapshots,
                "config-hash",
                "idl-hash",
                Some("strategy-v1".to_owned()),
                "feature_engine",
                "processed",
                Some(feature_snapshot.observed_at),
                feature_snapshot,
            )
            .with_run_id("run-chunked");
            store
                .write_feature_snapshot(&feature_record)
                .expect("write feature");
        }
        store
            .finalize_feature_snapshot_chunks_for_run("run-chunked", None)
            .expect("finalize chunks");
        let manifest_path = store
            .layout
            .feature_snapshot_dir
            .join("run-chunked")
            .join("_all")
            .join("feature_snapshot_chunks_manifest.json");
        assert!(manifest_path.exists());
        let manifest: FeatureSnapshotChunkManifest =
            serde_json::from_slice(&fs::read(manifest_path).expect("manifest bytes"))
                .expect("manifest json");
        assert_eq!(manifest.total_rows, 3);
        assert_eq!(manifest.chunks.len(), 1);
        let records = store
            .read_feature_snapshots_filtered(Some("run-chunked"), None)
            .expect("read chunks");
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn malformed_token_summary_file_is_skipped_in_bulk_reads() {
        let store = storage();
        let summary = CompactTokenSummary {
            mint: "mint".to_owned(),
            lifecycle: state::TokenLifecycle::ActiveLight,
            latest_price: Decimal::ONE,
            holder_count: 2,
            top1_holder_pct: Decimal::new(5, 1),
            creator_sold_pct: Decimal::ZERO,
            canonical_trade_count: 3,
            reason_codes: vec![],
        };
        let summary_record = StoredRecord::new(
            DatasetKind::TokenSummary,
            "config-hash",
            "idl-hash",
            None,
            "state",
            "snapshot",
            None,
            summary,
        )
        .with_run_id("run-good");
        store
            .write_token_summary(&summary_record)
            .expect("write summary");
        let bad_dir = store.layout.token_summary_dir.join("run-bad").join("_all");
        fs::create_dir_all(&bad_dir).expect("bad dir");
        fs::write(bad_dir.join("broken.json"), b"{\"broken\":").expect("bad write");

        let records = store.read_all_token_summaries().expect("bulk read");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].run_id.as_deref(), Some("run-good"));
    }
}
