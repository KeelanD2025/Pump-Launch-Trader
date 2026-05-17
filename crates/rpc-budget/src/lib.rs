use std::collections::BTreeMap;

use common::config::{ExecutionConfig, RpcBudgetConfig, RpcConfig, StreamOnlyConfig};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcReason {
    ColdStart,
    ProgramConfigValidation,
    ExecutionBlockhash,
    ExecutionSend,
    ExecutionStatusFallback,
    EmergencyReconciliation,
    DevTestFixtures,
    NonProductionVerification,
}

impl RpcReason {
    pub fn allowed_in_live(self) -> bool {
        matches!(
            self,
            Self::ProgramConfigValidation
                | Self::ExecutionBlockhash
                | Self::ExecutionSend
                | Self::ExecutionStatusFallback
                | Self::EmergencyReconciliation
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcCallCategory {
    MarketData,
    HolderScan,
    TopHolderScan,
    MetadataFetch,
    Backfill,
    Reconciliation,
    Confirmation,
    Blockhash,
    TransactionSend,
    TransactionStatus,
    Simulation,
    Emergency,
    DevTest,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcNetworkKind {
    JsonRpc,
    ERpc,
    HttpMetadata,
    HttpExternal,
    GrpcStream,
    UdpShred,
    InboundMetrics,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamOnlyPolicy {
    DenyAllRpc,
    AllowExecutionOnly,
    AllowBlockhashOnly,
    AllowEmergencyOnly,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcCallRequest {
    pub timestamp: OffsetDateTime,
    pub endpoint: String,
    pub method: String,
    pub caller_module: String,
    pub reason: RpcReason,
    pub category: RpcCallCategory,
    pub network_kind: RpcNetworkKind,
    pub related_token: Option<String>,
    pub related_signature: Option<String>,
    pub estimated_provider_credit_cost: u64,
    pub actual_provider_credit_cost: Option<u64>,
    pub config_hash: String,
    pub run_id: String,
    pub live_mode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcLedgerEntry {
    pub request: RpcCallRequest,
    pub allowed: bool,
    pub denied: bool,
    pub budget_remaining: u64,
    pub denial_reason: Option<String>,
    pub stream_only_enabled: bool,
    pub network_touched: bool,
}

#[derive(Debug, Clone)]
pub struct RpcBudgetManager {
    config: RpcBudgetConfig,
    execution: ExecutionConfig,
    stream_only: StreamOnlyConfig,
    rpc: RpcConfig,
    daily_used: u64,
    monthly_used: u64,
    per_method_used: BTreeMap<String, u64>,
    ledger: Vec<RpcLedgerEntry>,
    known_budget_state: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum RpcBudgetError {
    #[error("daily budget exceeded")]
    DailyBudgetExceeded,
    #[error("monthly budget exceeded")]
    MonthlyBudgetExceeded,
    #[error("per-method budget exceeded for {0}")]
    PerMethodBudgetExceeded(String),
    #[error("emergency reserve would be breached")]
    EmergencyReserveBreached,
    #[error("reason {0:?} is not allowed in live mode")]
    ReasonNotAllowed(RpcReason),
    #[error("unknown budget state while live mode requires fail-closed behavior")]
    UnknownBudgetState,
    #[error("stream-only policy denied {category:?}: {reason}")]
    StreamOnlyDenied {
        category: RpcCallCategory,
        reason: String,
    },
}

impl RpcBudgetManager {
    pub fn new(
        config: RpcBudgetConfig,
        execution: ExecutionConfig,
        stream_only: StreamOnlyConfig,
        rpc: RpcConfig,
    ) -> Self {
        Self {
            config,
            execution,
            stream_only,
            rpc,
            daily_used: 0,
            monthly_used: 0,
            per_method_used: BTreeMap::new(),
            ledger: Vec::new(),
            known_budget_state: true,
        }
    }

    pub fn mark_budget_state_unknown(&mut self) {
        self.known_budget_state = false;
    }

    pub fn ledger(&self) -> &[RpcLedgerEntry] {
        &self.ledger
    }

    pub fn usage_ratio(&self) -> Decimal {
        if self.config.daily_credit_limit == 0 {
            return Decimal::ZERO;
        }
        Decimal::from(self.daily_used) / Decimal::from(self.config.daily_credit_limit)
    }

    pub fn check_and_record(
        &mut self,
        request: RpcCallRequest,
    ) -> std::result::Result<RpcLedgerEntry, RpcBudgetError> {
        if let Some(reason) = self.stream_only_denial_reason(&request) {
            let entry = self.record_denial(request, reason.clone());
            return Err(RpcBudgetError::StreamOnlyDenied {
                category: entry.request.category,
                reason,
            });
        }

        if request.live_mode && self.config.deny_when_unknown_live_state && !self.known_budget_state
        {
            let entry = self.record_denial(
                request,
                "unknown budget state while live mode is fail-closed".to_owned(),
            );
            return Err(RpcBudgetError::UnknownBudgetState.with_entry(entry));
        }

        if request.live_mode && !request.reason.allowed_in_live() {
            let denied_reason = format!("reason {:?} is not allowed in live mode", request.reason);
            let entry = self.record_denial(request.clone(), denied_reason);
            return Err(RpcBudgetError::ReasonNotAllowed(request.reason).with_entry(entry));
        }

        if matches!(
            request.network_kind,
            RpcNetworkKind::GrpcStream | RpcNetworkKind::UdpShred | RpcNetworkKind::InboundMetrics
        ) {
            let entry = RpcLedgerEntry {
                request,
                allowed: true,
                denied: false,
                budget_remaining: self
                    .config
                    .daily_credit_limit
                    .saturating_sub(self.daily_used),
                denial_reason: None,
                stream_only_enabled: self.stream_only.enabled,
                network_touched: false,
            };
            self.ledger.push(entry.clone());
            return Ok(entry);
        }

        if self
            .daily_used
            .saturating_add(request.estimated_provider_credit_cost)
            > self.config.daily_credit_limit
        {
            let entry = self.record_denial(request, "daily budget exceeded".to_owned());
            return Err(RpcBudgetError::DailyBudgetExceeded.with_entry(entry));
        }

        if self
            .monthly_used
            .saturating_add(request.estimated_provider_credit_cost)
            > self.config.monthly_credit_limit
        {
            let entry = self.record_denial(request, "monthly budget exceeded".to_owned());
            return Err(RpcBudgetError::MonthlyBudgetExceeded.with_entry(entry));
        }

        let next_method_total = self
            .per_method_used
            .get(&request.method)
            .copied()
            .unwrap_or_default()
            .saturating_add(request.estimated_provider_credit_cost);
        if let Some(limit) = self.config.per_method_limits.get(&request.method) {
            if next_method_total > *limit {
                let entry = self.record_denial(
                    request.clone(),
                    format!("per-method budget exceeded for {}", request.method),
                );
                return Err(
                    RpcBudgetError::PerMethodBudgetExceeded(request.method.clone())
                        .with_entry(entry),
                );
            }
        }

        let remaining_after = self.config.daily_credit_limit.saturating_sub(
            self.daily_used
                .saturating_add(request.estimated_provider_credit_cost),
        );
        if remaining_after < self.config.emergency_reserve {
            let entry =
                self.record_denial(request, "emergency reserve would be breached".to_owned());
            return Err(RpcBudgetError::EmergencyReserveBreached.with_entry(entry));
        }

        self.daily_used = self
            .daily_used
            .saturating_add(request.estimated_provider_credit_cost);
        self.monthly_used = self
            .monthly_used
            .saturating_add(request.estimated_provider_credit_cost);
        self.per_method_used
            .insert(request.method.clone(), next_method_total);

        let entry = RpcLedgerEntry {
            request,
            allowed: true,
            denied: false,
            budget_remaining: remaining_after,
            denial_reason: None,
            stream_only_enabled: self.stream_only.enabled,
            network_touched: false,
        };
        self.ledger.push(entry.clone());
        Ok(entry)
    }

    pub fn summary(&self) -> RpcBudgetSummary {
        let denied_entries = self.ledger.iter().filter(|entry| entry.denied).count() as u64;
        let allowed_entries = self.ledger.iter().filter(|entry| entry.allowed).count() as u64;
        let network_calls_total = self
            .ledger
            .iter()
            .filter(|entry| entry.allowed && entry.network_touched)
            .count() as u64;
        RpcBudgetSummary {
            live_enabled: self.execution.live_enabled,
            stream_only_enabled: self.stream_only.enabled,
            stream_only_policy: self.stream_only_policy(),
            daily_used: self.daily_used,
            daily_limit: self.config.daily_credit_limit,
            monthly_used: self.monthly_used,
            monthly_limit: self.config.monthly_credit_limit,
            emergency_reserve: self.config.emergency_reserve,
            ledger_entries: self.ledger.len() as u64,
            allowed_entries,
            denied_entries,
            network_calls_total,
            market_data_rpc_calls_allowed: self.stream_only.allow_tracking_rpc,
            holder_rpc_calls_allowed: self.stream_only.allow_holder_rpc,
            metadata_fetch_allowed: self.stream_only.allow_metadata_rpc,
            confirmation_rpc_allowed: self.stream_only.allow_confirmation_rpc,
            blockhash_rpc_allowed: self.stream_only.allow_blockhash_rpc,
            rpc_hot_path_enabled: self.rpc.hot_path_enabled,
        }
    }

    fn stream_only_policy(&self) -> StreamOnlyPolicy {
        if !self.stream_only.enabled {
            return StreamOnlyPolicy::Disabled;
        }
        if self.stream_only.allow_execution_rpc || self.stream_only.allow_send_rpc {
            StreamOnlyPolicy::AllowExecutionOnly
        } else if self.stream_only.allow_blockhash_rpc {
            StreamOnlyPolicy::AllowBlockhashOnly
        } else if self.stream_only.allow_emergency_rpc {
            StreamOnlyPolicy::AllowEmergencyOnly
        } else {
            StreamOnlyPolicy::DenyAllRpc
        }
    }

    fn stream_only_denial_reason(&self, request: &RpcCallRequest) -> Option<String> {
        if !self.stream_only.enabled {
            return None;
        }
        if matches!(
            request.network_kind,
            RpcNetworkKind::GrpcStream | RpcNetworkKind::UdpShred | RpcNetworkKind::InboundMetrics
        ) {
            return None;
        }

        let allowed = match request.category {
            RpcCallCategory::MarketData => self.stream_only.allow_tracking_rpc,
            RpcCallCategory::HolderScan => self.stream_only.allow_holder_rpc,
            RpcCallCategory::TopHolderScan => self.stream_only.allow_top_holder_rpc,
            RpcCallCategory::MetadataFetch => self.stream_only.allow_metadata_rpc,
            RpcCallCategory::Backfill => self.stream_only.allow_backfill_rpc,
            RpcCallCategory::Reconciliation => self.stream_only.allow_reconciliation_rpc,
            RpcCallCategory::Confirmation | RpcCallCategory::TransactionStatus => {
                self.stream_only.allow_confirmation_rpc
            }
            RpcCallCategory::Blockhash => self.stream_only.allow_blockhash_rpc,
            RpcCallCategory::TransactionSend | RpcCallCategory::Simulation => {
                self.stream_only.allow_execution_rpc && self.stream_only.allow_send_rpc
            }
            RpcCallCategory::Emergency => self.stream_only.allow_emergency_rpc,
            RpcCallCategory::DevTest => false,
            RpcCallCategory::Unknown => !self.stream_only.fail_on_unbudgeted_rpc,
        };
        if allowed {
            None
        } else {
            Some(format!(
                "stream-only policy {:?} forbids {} ({:?})",
                self.stream_only_policy(),
                request.method,
                request.category
            ))
        }
    }

    fn record_denial(&mut self, request: RpcCallRequest, reason: String) -> RpcLedgerEntry {
        let entry = RpcLedgerEntry {
            request,
            allowed: false,
            denied: true,
            budget_remaining: self
                .config
                .daily_credit_limit
                .saturating_sub(self.daily_used),
            denial_reason: Some(reason),
            stream_only_enabled: self.stream_only.enabled,
            network_touched: false,
        };
        self.ledger.push(entry.clone());
        entry
    }
}

trait RpcBudgetErrorExt {
    fn with_entry(self, _entry: RpcLedgerEntry) -> Self;
}

impl RpcBudgetErrorExt for RpcBudgetError {
    fn with_entry(self, _entry: RpcLedgerEntry) -> Self {
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcBudgetSummary {
    pub live_enabled: bool,
    pub stream_only_enabled: bool,
    pub stream_only_policy: StreamOnlyPolicy,
    pub daily_used: u64,
    pub daily_limit: u64,
    pub monthly_used: u64,
    pub monthly_limit: u64,
    pub emergency_reserve: u64,
    pub ledger_entries: u64,
    pub allowed_entries: u64,
    pub denied_entries: u64,
    pub network_calls_total: u64,
    pub market_data_rpc_calls_allowed: bool,
    pub holder_rpc_calls_allowed: bool,
    pub metadata_fetch_allowed: bool,
    pub confirmation_rpc_allowed: bool,
    pub blockhash_rpc_allowed: bool,
    pub rpc_hot_path_enabled: bool,
}

#[cfg(test)]
mod tests {
    use common::config::LoadedConfig;
    use time::OffsetDateTime;

    use super::{
        RpcBudgetError, RpcBudgetManager, RpcCallCategory, RpcCallRequest, RpcNetworkKind,
        RpcReason,
    };

    fn manager() -> RpcBudgetManager {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("config")
            .join("dev.toml");
        let loaded = LoadedConfig::from_file(path).expect("config");
        RpcBudgetManager::new(
            loaded.config.rpc_budget,
            loaded.config.execution,
            loaded.config.stream_only,
            loaded.config.rpc,
        )
    }

    fn request(category: RpcCallCategory, method: &str) -> RpcCallRequest {
        RpcCallRequest {
            timestamp: OffsetDateTime::UNIX_EPOCH,
            endpoint: "http://127.0.0.1:8899".to_owned(),
            method: method.to_owned(),
            caller_module: "tests".to_owned(),
            reason: RpcReason::DevTestFixtures,
            category,
            network_kind: RpcNetworkKind::JsonRpc,
            related_token: None,
            related_signature: None,
            estimated_provider_credit_cost: 1,
            actual_provider_credit_cost: None,
            config_hash: "abc".to_owned(),
            run_id: "dev".to_owned(),
            live_mode: false,
        }
    }

    #[test]
    fn denies_stream_only_market_data_before_network_touch() {
        let mut manager = manager();
        let error = manager
            .check_and_record(request(RpcCallCategory::MarketData, "getAccountInfo"))
            .expect_err("must fail");
        assert!(matches!(
            error,
            RpcBudgetError::StreamOnlyDenied {
                category: RpcCallCategory::MarketData,
                ..
            }
        ));
        let entry = manager.ledger().last().expect("ledger entry");
        assert!(entry.denied);
        assert!(!entry.network_touched);
    }

    #[test]
    fn allows_stream_sources_without_rpc_budget_path() {
        let mut manager = manager();
        let mut request = request(RpcCallCategory::MarketData, "subscribe");
        request.network_kind = RpcNetworkKind::GrpcStream;
        let entry = manager.check_and_record(request).expect("allowed");
        assert!(entry.allowed);
    }

    #[test]
    fn denies_unknown_live_budget_state() {
        let mut manager = manager();
        manager.mark_budget_state_unknown();
        let mut request = request(RpcCallCategory::Blockhash, "getLatestBlockhash");
        request.reason = RpcReason::ExecutionBlockhash;
        request.live_mode = true;
        let error = manager.check_and_record(request).expect_err("must fail");
        assert!(matches!(error, RpcBudgetError::StreamOnlyDenied { .. }));
        assert_eq!(manager.summary().denied_entries, 1);
    }

    #[test]
    fn summary_reports_zero_network_calls_for_denied_entries() {
        let mut manager = manager();
        let _ = manager.check_and_record(request(
            RpcCallCategory::Confirmation,
            "getSignatureStatuses",
        ));
        let summary = manager.summary();
        assert_eq!(summary.network_calls_total, 0);
        assert_eq!(summary.denied_entries, 1);
    }
}
