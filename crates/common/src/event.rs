use std::{collections::BTreeMap, fmt, str::FromStr};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{math::Lamports, reason::ReasonCode, schema::SCHEMA_VERSION};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(pub Uuid);

impl EventId {
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_seed(seed: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(seed.as_bytes());
        let digest = hasher.finalize();
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        Self(Uuid::from_bytes(bytes))
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new_v7()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PubkeyValue(pub String);

impl FromStr for PubkeyValue {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let bytes = bs58::decode(value)
            .into_vec()
            .map_err(|error| error.to_string())?;
        if bytes.len() != 32 {
            return Err(format!("expected 32-byte pubkey, got {}", bytes.len()));
        }
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Display for PubkeyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSource {
    ShredTentative,
    DeshredTentative,
    GeyserProcessed,
    GeyserConfirmed,
    GeyserRooted,
    Replay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Canonicality {
    Tentative,
    Processed,
    Confirmed,
    Rooted,
    Reverted,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenProgramType {
    SplToken,
    Token2022,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteAssetType {
    NativeSol,
    WrappedSol,
    Stable,
    Other,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionStatus {
    Success,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenTerminalVariant {
    Discarded,
    SoftDiscarded,
    HardDiscarded,
    Rugged,
    Dead,
    Migrated,
    TrackingExpired,
    DataGap,
    ManualStop,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeDecision {
    Ignore,
    WatchLight,
    WatchDeep,
    EnterPaper,
    EnterLive,
    Hold,
    ScaleOut,
    Exit,
    EmergencyExit,
    StopTracking,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataGapType {
    SlotGap,
    TransactionStreamGap,
    AccountStreamGap,
    ShredGap,
    ReconnectGap,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataQualityFlag {
    PartialShred,
    GeyserGap,
    ShredGap,
    ReorgObserved,
    MissingWriteVersion,
    SourceDisagreement,
    UnsupportedInstruction,
    StaleAccount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EarlyIntentSource {
    RawShred,
    DeshredPreExecution,
    FixtureTentative,
    MockEarlyIntent,
    ReplayTentative,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TentativeSellConfirmationState {
    PendingTentative,
    GeyserProcessedTxSeen,
    AccountEffectsObserved,
    ConfirmedExecuted,
    ConfirmedFailed,
    RootedExecuted,
    NotSeenWithinTtl,
    Reorged,
    DecodeMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TentativeSellRiskLevel {
    Info,
    Watch,
    ExitArmed,
    EmergencyExitRecommended,
    EmergencyExitRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DangerousSellerClassification {
    Dev,
    DevCluster,
    Top1Holder,
    Top3Holder,
    Top5Holder,
    Top10Holder,
    BundleWallet,
    BundleCluster,
    Whale,
    SmartWalletTakingProfit,
    ToxicSniper,
    SameFunderCluster,
    SameClientFingerprintCluster,
    HighPnlHolder,
    FreeRollingHolder,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TentativeSellResolutionOutcome {
    ConfirmedExecuted,
    AccountEffectsObserved,
    ConfirmedFailed,
    RootedExecuted,
    NotSeenWithinTtl,
    Reorged,
    DecodeMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawEventReference {
    pub source_id: String,
    pub cursor: Option<String>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMeta {
    pub event_id: EventId,
    pub source: EventSource,
    pub canonicality: Canonicality,
    pub slot: u64,
    pub parent_slot: Option<u64>,
    pub block_time: Option<OffsetDateTime>,
    pub observed_at_monotonic_ns: u64,
    pub received_at_wall_time: OffsetDateTime,
    pub signature: Option<String>,
    pub transaction_index: Option<u32>,
    pub instruction_index: Option<u16>,
    pub inner_instruction_index: Option<u16>,
    pub account_pubkey: Option<PubkeyValue>,
    pub account_write_version: Option<u64>,
    pub source_latency_ms: Option<u64>,
    pub decode_confidence: Decimal,
    pub data_quality_flags: Vec<DataQualityFlag>,
    pub raw_reference: Option<RawEventReference>,
    pub schema_version: u32,
}

impl EventMeta {
    pub fn new(source: EventSource, canonicality: Canonicality, slot: u64) -> Self {
        Self {
            event_id: EventId::new_v7(),
            source,
            canonicality,
            slot,
            parent_slot: None,
            block_time: None,
            observed_at_monotonic_ns: 0,
            received_at_wall_time: OffsetDateTime::UNIX_EPOCH,
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
            schema_version: SCHEMA_VERSION,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenCreatedEvent {
    pub mint: PubkeyValue,
    pub token_program: TokenProgramType,
    pub quote_mint: PubkeyValue,
    pub quote_asset_type: QuoteAssetType,
    pub creator_wallet: PubkeyValue,
    pub payer: PubkeyValue,
    pub bonding_curve_account: PubkeyValue,
    pub associated_bonding_curve_account: Option<PubkeyValue>,
    pub metadata_account: Option<PubkeyValue>,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub create_instruction_variant: String,
    pub initial_virtual_quote_reserves: Option<Decimal>,
    pub initial_virtual_token_reserves: Option<Decimal>,
    pub initial_real_quote_reserves: Option<Decimal>,
    pub initial_real_token_reserves: Option<Decimal>,
    pub initial_supply: Option<Decimal>,
    pub creator_initial_buy: Option<Decimal>,
    pub same_transaction_buys: u32,
    pub same_slot_buys: u32,
    pub fee_recipients: Vec<PubkeyValue>,
    pub raw_account_list: Vec<PubkeyValue>,
    pub launch_transaction_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpBuyEvent {
    pub mint: PubkeyValue,
    pub buyer: PubkeyValue,
    pub payer: PubkeyValue,
    pub quote_in: Decimal,
    pub token_out: Decimal,
    pub price_before: Option<Decimal>,
    pub price_after: Option<Decimal>,
    pub effective_price: Decimal,
    pub slippage_estimate: Option<Decimal>,
    pub reserves_before: Option<ReserveSnapshot>,
    pub reserves_after: Option<ReserveSnapshot>,
    pub max_quote_cost: Option<Decimal>,
    pub compute_unit_limit: Option<u32>,
    pub compute_unit_price: Option<u64>,
    pub estimated_priority_fee_lamports: Option<Lamports>,
    pub estimated_base_fee_lamports: Option<Lamports>,
    pub estimated_tip_lamports: Option<Lamports>,
    pub is_creator: bool,
    pub is_known_cluster_member: bool,
    pub is_first_buy: bool,
    pub status: TransactionStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpSellEvent {
    pub mint: PubkeyValue,
    pub seller: PubkeyValue,
    pub quote_out: Decimal,
    pub token_in: Decimal,
    pub price_before: Option<Decimal>,
    pub price_after: Option<Decimal>,
    pub effective_price: Decimal,
    pub slippage_estimate: Option<Decimal>,
    pub reserves_before: Option<ReserveSnapshot>,
    pub reserves_after: Option<ReserveSnapshot>,
    pub min_quote_output: Option<Decimal>,
    pub compute_unit_limit: Option<u32>,
    pub compute_unit_price: Option<u64>,
    pub estimated_priority_fee_lamports: Option<Lamports>,
    pub estimated_base_fee_lamports: Option<Lamports>,
    pub estimated_tip_lamports: Option<Lamports>,
    pub is_creator: bool,
    pub is_top_holder_pre_sell: bool,
    pub is_known_cluster_member: bool,
    pub status: TransactionStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReserveSnapshot {
    pub virtual_quote_reserves: Decimal,
    pub virtual_token_reserves: Decimal,
    pub real_quote_reserves: Decimal,
    pub real_token_reserves: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BondingCurveUpdateEvent {
    pub mint: PubkeyValue,
    pub virtual_quote_reserves: Decimal,
    pub virtual_token_reserves: Decimal,
    pub real_quote_reserves: Decimal,
    pub real_token_reserves: Decimal,
    #[serde(default)]
    pub token_decimals: Option<u8>,
    #[serde(default)]
    pub price_lamports_per_raw_token: Option<Decimal>,
    #[serde(default)]
    pub price_sol_per_token: Option<Decimal>,
    #[serde(default)]
    pub reserve_price_source: Option<String>,
    #[serde(default)]
    pub reserve_price_confidence: Option<Decimal>,
    pub price: Decimal,
    #[serde(default)]
    pub market_cap_quote_1b: Option<Decimal>,
    #[serde(default)]
    pub market_cap_quote_total_supply: Option<Decimal>,
    #[serde(default)]
    pub market_cap_source: Option<String>,
    #[serde(default)]
    pub market_cap_confidence: Option<Decimal>,
    pub market_cap_proxy: Option<Decimal>,
    #[serde(default)]
    pub curve_complete_flag: Option<bool>,
    #[serde(default)]
    pub curve_progress_pct: Option<Decimal>,
    #[serde(default)]
    pub curve_progress_source: Option<String>,
    #[serde(default)]
    pub curve_progress_confidence: Option<Decimal>,
    pub curve_completion_pct: Option<Decimal>,
    pub quote_reserve_delta: Option<Decimal>,
    pub token_reserve_delta: Option<Decimal>,
    pub update_reason: String,
    pub caused_by_signature: Option<String>,
    pub account_write_version: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HolderBalanceUpdateEvent {
    pub mint: PubkeyValue,
    pub owner_wallet: PubkeyValue,
    pub token_account: PubkeyValue,
    #[serde(default)]
    pub token_decimals: Option<u8>,
    pub old_balance: Option<Decimal>,
    pub new_balance: Decimal,
    pub delta: Decimal,
    pub caused_by_signature: Option<String>,
    pub update_reason: String,
    pub confidence: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFundingEvent {
    pub wallet: PubkeyValue,
    pub funder: PubkeyValue,
    pub asset_label: String,
    pub amount: Decimal,
    pub slot: u64,
    pub signature: String,
    pub relation_to_launch: Option<String>,
    pub near_launch_relation: bool,
    pub funding_graph_edge_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservedTransactionEvent {
    pub signature_hint: Option<String>,
    pub slot_hint: Option<u64>,
    pub entry_index: Option<u32>,
    pub tx_position_estimate: Option<u32>,
    #[serde(default)]
    pub signer: Option<String>,
    pub program_ids: Vec<String>,
    pub account_count: usize,
    pub instruction_count: usize,
    #[serde(default)]
    pub account_list_hash: Option<String>,
    #[serde(default)]
    pub instruction_shape_hash: Option<String>,
    #[serde(default)]
    pub compute_unit_limit: Option<u32>,
    #[serde(default)]
    pub compute_unit_price: Option<u64>,
    #[serde(default)]
    pub estimated_priority_fee_lamports: Option<Lamports>,
    #[serde(default)]
    pub tx_fee_lamports: Option<Lamports>,
    #[serde(default)]
    pub compute_units_consumed: Option<u64>,
    #[serde(default)]
    pub pre_sol_balances_lamports: Vec<Lamports>,
    #[serde(default)]
    pub post_sol_balances_lamports: Vec<Lamports>,
    #[serde(default)]
    pub failed_transaction: bool,
    #[serde(default)]
    pub error_code: Option<String>,
    #[serde(default)]
    pub bundle_like_evidence: Option<String>,
    pub raw_packet_hash: String,
    pub first_seen_by_shred_ns: u64,
    pub decode_confidence: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TentativeSellIntentDetectedEvent {
    pub event_id: EventId,
    pub source: EarlyIntentSource,
    pub canonicality: Canonicality,
    pub slot: Option<u64>,
    pub entry_index: Option<u32>,
    pub shred_index: Option<u32>,
    pub tx_position_estimate: Option<u32>,
    pub observed_at_monotonic_ns: u64,
    pub received_at_wall_time: OffsetDateTime,
    pub signature: Option<String>,
    pub mint: PubkeyValue,
    pub seller_wallet: PubkeyValue,
    pub payer_wallet: Option<PubkeyValue>,
    pub token_in_estimate: Decimal,
    pub quote_out_estimate: Decimal,
    pub min_quote_output: Option<Decimal>,
    pub sell_instruction_variant: String,
    pub decoded_instruction_confidence: Decimal,
    pub account_decode_confidence: Decimal,
    pub wallet_classification_snapshot: String,
    pub seller_balance_before_estimate: Decimal,
    pub seller_holding_pct_estimate: Decimal,
    pub seller_cost_basis_estimate: Decimal,
    pub seller_unrealized_pnl_estimate: Decimal,
    pub estimated_price_before: Decimal,
    pub estimated_price_after: Decimal,
    pub estimated_price_impact_pct: Decimal,
    pub estimated_curve_impact: Decimal,
    pub estimated_top_holder_rank: Option<u64>,
    pub estimated_cluster_id: Option<String>,
    pub estimated_cluster_holding_pct: Decimal,
    pub reason_codes: Vec<ReasonCode>,
    pub raw_packet_hash: Option<String>,
    pub raw_entry_hash: Option<String>,
    pub raw_update_hash: Option<String>,
    pub matched_canonical_signature: Option<String>,
    pub confirmation_state: TentativeSellConfirmationState,
    pub schema_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TentativeMaliciousSellWarningEvent {
    pub mint: PubkeyValue,
    pub seller_wallet: PubkeyValue,
    pub seller_classification: DangerousSellerClassification,
    pub estimated_sell_impact_pct: Decimal,
    pub estimated_cluster_sell_impact_pct: Decimal,
    pub risk_level: TentativeSellRiskLevel,
    pub confidence: Decimal,
    pub reason_codes: Vec<ReasonCode>,
    pub source_latency_advantage_ms: Option<i64>,
    pub required_latency_advantage_ms: Option<i64>,
    pub latency_edge_ratio: Decimal,
    pub exit_can_land_before_estimated_impact: bool,
    pub absorption_health_score: Decimal,
    pub post_sell_absorption_probability: Decimal,
    pub emergency_exit_expected_saved_loss: Decimal,
    pub emergency_exit_expected_opportunity_cost: Decimal,
    pub emergency_exit_net_benefit: Decimal,
    pub emergency_exit_net_benefit_confidence: Decimal,
    pub canonicality: Canonicality,
    pub feature_snapshot_hash: String,
    pub risk_snapshot_hash: String,
    pub trigger_event_id: EventId,
    pub schema_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShredEmergencyExitArmedEvent {
    pub mint: PubkeyValue,
    pub position_id: String,
    pub trigger_event_id: EventId,
    pub seller_wallet: PubkeyValue,
    pub seller_classification: DangerousSellerClassification,
    pub risk_level: TentativeSellRiskLevel,
    pub estimated_impact_pct: Decimal,
    pub confidence: Decimal,
    pub planned_exit_size: Decimal,
    pub planned_exit_reason: String,
    pub source: EarlyIntentSource,
    pub expires_at: OffsetDateTime,
    pub cancel_conditions: Vec<String>,
    pub escalation_conditions: Vec<String>,
    pub schema_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShredEmergencyExitTriggeredEvent {
    pub mint: PubkeyValue,
    pub position_id: String,
    pub trigger_event_id: EventId,
    pub decision_id: String,
    pub seller_wallet: PubkeyValue,
    pub seller_classification: DangerousSellerClassification,
    pub side: ExecutionSide,
    pub exit_size: Decimal,
    pub reason_codes: Vec<ReasonCode>,
    pub source: EarlyIntentSource,
    pub confidence: Decimal,
    pub live_allowed: bool,
    pub paper_allowed: bool,
    pub expected_exit_price: Decimal,
    pub expected_fee_adjusted_exit_value: Decimal,
    pub estimated_saved_loss_vs_waiting_for_geyser: Decimal,
    pub schema_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShredSellIntentResolvedEvent {
    pub original_tentative_event_id: EventId,
    pub canonical_signature: Option<String>,
    pub outcome: TentativeSellResolutionOutcome,
    pub observed_canonical_slot: Option<u64>,
    pub actual_price_impact_pct: Decimal,
    pub actual_quote_out: Decimal,
    pub actual_token_in: Decimal,
    pub actual_loss_saved_if_exited: Decimal,
    pub false_positive_flag: bool,
    pub missed_exit_flag: bool,
    pub reconciliation_latency_ms: Option<i64>,
    pub source: EarlyIntentSource,
    pub mint: PubkeyValue,
    pub seller_wallet: PubkeyValue,
    pub seller_classification: DangerousSellerClassification,
    pub confirmation_state: TentativeSellConfirmationState,
    pub confirmation_method: Option<String>,
    pub schema_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenTerminalEvent {
    pub mint: PubkeyValue,
    pub variant: TokenTerminalVariant,
    pub reason_codes: Vec<ReasonCode>,
    pub details: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeDecisionEvent {
    #[serde(default)]
    pub decision_id: String,
    pub mint: PubkeyValue,
    pub decision: TradeDecision,
    pub strategy: String,
    pub feature_snapshot_hash: String,
    pub score_vector: BTreeMap<String, Decimal>,
    pub risk_vector: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub expected_net_edge_quote: Decimal,
    #[serde(default)]
    pub expected_net_edge_pct: Decimal,
    #[serde(default)]
    pub expected_edge_confidence: Decimal,
    pub reason_codes: Vec<ReasonCode>,
    pub config_hash: String,
    pub strategy_version: String,
    pub data_quality_score: Decimal,
    pub no_lookahead_timestamp: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillEvent {
    pub mint: PubkeyValue,
    pub side: ExecutionSide,
    pub intended_time: OffsetDateTime,
    pub signal_time: OffsetDateTime,
    pub send_time: OffsetDateTime,
    pub landing_time: Option<OffsetDateTime>,
    pub confirmation_time: Option<OffsetDateTime>,
    pub intended_size: Decimal,
    pub filled_size: Decimal,
    pub fill_price: Decimal,
    pub notional: Decimal,
    pub fees: Decimal,
    pub slippage: Decimal,
    pub price_impact: Decimal,
    pub failure_reason: Option<String>,
    pub transaction_signature: Option<String>,
    pub confirmation_source: Option<String>,
    #[serde(default)]
    pub entry_decision_id: Option<String>,
    #[serde(default)]
    pub exit_decision_id: Option<String>,
    #[serde(default)]
    pub strategy: Option<String>,
    #[serde(default)]
    pub entry_price: Option<Decimal>,
    #[serde(default)]
    pub exit_price: Option<Decimal>,
    #[serde(default)]
    pub gross_pnl_quote: Option<Decimal>,
    #[serde(default)]
    pub net_pnl_quote: Option<Decimal>,
    #[serde(default)]
    pub base_fee_quote: Option<Decimal>,
    #[serde(default)]
    pub priority_fee_quote: Option<Decimal>,
    #[serde(default)]
    pub tip_quote: Option<Decimal>,
    #[serde(default)]
    pub slippage_cost_quote: Option<Decimal>,
    #[serde(default)]
    pub curve_impact_cost_quote: Option<Decimal>,
    #[serde(default)]
    pub latency_cost_quote: Option<Decimal>,
    #[serde(default)]
    pub failed_tx_cost_quote: Option<Decimal>,
    #[serde(default)]
    pub hold_time_ms: Option<i64>,
    #[serde(default)]
    pub max_adverse_excursion: Option<Decimal>,
    #[serde(default)]
    pub max_favorable_excursion: Option<Decimal>,
    #[serde(default)]
    pub exit_reason: Option<String>,
    #[serde(default)]
    pub exit_classification: Option<String>,
    #[serde(default)]
    pub expected_edge_quote: Option<Decimal>,
    #[serde(default)]
    pub actual_realized_edge_quote: Option<Decimal>,
    #[serde(default)]
    pub edge_forecast_error_quote: Option<Decimal>,
    #[serde(default)]
    pub entry_risk_scores: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub exit_risk_scores: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub exit_source: Option<String>,
    #[serde(default)]
    pub trigger_event_id: Option<String>,
    #[serde(default)]
    pub malicious_sell_signature: Option<String>,
    #[serde(default)]
    pub malicious_sell_seller: Option<String>,
    #[serde(default)]
    pub malicious_sell_classification: Option<String>,
    #[serde(default)]
    pub estimated_loss_saved_quote: Option<Decimal>,
    #[serde(default)]
    pub realized_loss_saved_quote: Option<Decimal>,
    #[serde(default)]
    pub false_positive_exit: bool,
    #[serde(default)]
    pub opportunity_cost_if_false_positive: Option<Decimal>,
    #[serde(default)]
    pub early_intent_to_geyser_processed_latency_ms: Option<i64>,
    #[serde(default)]
    pub early_intent_to_account_effect_latency_ms: Option<i64>,
    #[serde(default)]
    pub early_intent_to_rooted_latency_ms: Option<i64>,
    #[serde(default)]
    pub exit_latency_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataGapEvent {
    pub gap_type: DataGapType,
    pub source: EventSource,
    pub start_slot: u64,
    pub end_slot: Option<u64>,
    pub affected_tokens: Vec<PubkeyValue>,
    pub severity: GapSeverity,
    pub trade_allowed: bool,
    pub recovery_action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum EventPayload {
    TokenCreated(TokenCreatedEvent),
    PumpBuy(PumpBuyEvent),
    PumpSell(PumpSellEvent),
    BondingCurveUpdate(BondingCurveUpdateEvent),
    HolderBalanceUpdate(HolderBalanceUpdateEvent),
    WalletFunding(WalletFundingEvent),
    ObservedTransaction(ObservedTransactionEvent),
    TentativeSellIntentDetected(TentativeSellIntentDetectedEvent),
    TentativeMaliciousSellWarning(TentativeMaliciousSellWarningEvent),
    ShredEmergencyExitArmed(ShredEmergencyExitArmedEvent),
    ShredEmergencyExitTriggered(ShredEmergencyExitTriggeredEvent),
    ShredSellIntentResolved(ShredSellIntentResolvedEvent),
    TokenTerminal(TokenTerminalEvent),
    TradeDecision(TradeDecisionEvent),
    SimulatedFill(FillEvent),
    LiveFill(FillEvent),
    DataGap(DataGapEvent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedEvent {
    pub meta: EventMeta,
    pub payload: EventPayload,
}

impl NormalizedEvent {
    pub fn signature(&self) -> Option<&str> {
        self.meta
            .signature
            .as_deref()
            .or_else(|| match &self.payload {
                EventPayload::ObservedTransaction(event) => event.signature_hint.as_deref(),
                EventPayload::WalletFunding(event) => Some(event.signature.as_str()),
                EventPayload::TentativeSellIntentDetected(event) => event.signature.as_deref(),
                EventPayload::ShredSellIntentResolved(event) => {
                    event.canonical_signature.as_deref()
                }
                _ => None,
            })
    }

    pub fn mint(&self) -> Option<&PubkeyValue> {
        match &self.payload {
            EventPayload::TokenCreated(event) => Some(&event.mint),
            EventPayload::PumpBuy(event) => Some(&event.mint),
            EventPayload::PumpSell(event) => Some(&event.mint),
            EventPayload::BondingCurveUpdate(event) => Some(&event.mint),
            EventPayload::HolderBalanceUpdate(event) => Some(&event.mint),
            EventPayload::TentativeSellIntentDetected(event) => Some(&event.mint),
            EventPayload::TentativeMaliciousSellWarning(event) => Some(&event.mint),
            EventPayload::ShredEmergencyExitArmed(event) => Some(&event.mint),
            EventPayload::ShredEmergencyExitTriggered(event) => Some(&event.mint),
            EventPayload::ShredSellIntentResolved(event) => Some(&event.mint),
            EventPayload::TokenTerminal(event) => Some(&event.mint),
            EventPayload::TradeDecision(event) => Some(&event.mint),
            EventPayload::SimulatedFill(event) => Some(&event.mint),
            EventPayload::LiveFill(event) => Some(&event.mint),
            EventPayload::WalletFunding(_)
            | EventPayload::ObservedTransaction(_)
            | EventPayload::DataGap(_) => None,
        }
    }

    pub fn is_replay_source_event(&self) -> bool {
        matches!(
            self.payload,
            EventPayload::TokenCreated(_)
                | EventPayload::PumpBuy(_)
                | EventPayload::PumpSell(_)
                | EventPayload::BondingCurveUpdate(_)
                | EventPayload::HolderBalanceUpdate(_)
                | EventPayload::WalletFunding(_)
                | EventPayload::ObservedTransaction(_)
        ) || matches!(self.payload, EventPayload::DataGap(_))
            && !matches!(self.meta.source, EventSource::Replay)
    }
}
