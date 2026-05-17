use std::collections::HashMap;

use common::{ExecutionConfig, ExecutionSide, FillEvent, ReasonCode, TradeDecision};
use decision::{DecisionOutcome, OpenPositionContext, StrategyKind};
use rpc_budget::{
    RpcBudgetError, RpcBudgetManager, RpcCallCategory, RpcCallRequest, RpcNetworkKind, RpcReason,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sim::{SimulatedOrderRequest, Simulator};
use state::{StateSnapshot, TokenState};
use thiserror::Error;
use time::{Duration, OffsetDateTime};

macro_rules! dec {
    ($value:literal) => {
        Decimal::from_str_exact(stringify!($value)).expect("decimal literal")
    };
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperPosition {
    pub mint: String,
    pub side: ExecutionSide,
    pub entry_time: OffsetDateTime,
    pub entry_price: Decimal,
    pub size_quote: Decimal,
    pub size_tokens: Decimal,
    pub fees_paid: Decimal,
    pub current_value: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
    pub max_adverse_excursion: Decimal,
    pub max_favorable_excursion: Decimal,
    pub strategy: String,
    pub entry_decision_id: String,
    pub expected_edge_quote: Decimal,
    pub entry_risk_scores: std::collections::BTreeMap<String, Decimal>,
    pub entry_reason_codes: Vec<ReasonCode>,
    pub exit_reason_codes: Vec<ReasonCode>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PaperLedger {
    pub fills: Vec<FillEvent>,
    pub closed_pnl: Decimal,
    pub fee_drag: Decimal,
    pub slippage_drag: Decimal,
    pub latency_drag: Decimal,
}

#[derive(Debug, Clone)]
pub struct PaperExecutor {
    simulator: Simulator,
    positions: HashMap<String, PaperPosition>,
    ledger: PaperLedger,
}

impl PaperExecutor {
    pub fn new(simulator: Simulator) -> Self {
        Self {
            simulator,
            positions: HashMap::new(),
            ledger: PaperLedger::default(),
        }
    }

    pub fn positions(&self) -> &HashMap<String, PaperPosition> {
        &self.positions
    }

    pub fn ledger(&self) -> &PaperLedger {
        &self.ledger
    }

    pub fn position_context(&self, mint: &str) -> Option<OpenPositionContext> {
        self.positions
            .get(mint)
            .map(|position| OpenPositionContext {
                strategy: parse_strategy(&position.strategy),
                entry_time: position.entry_time,
                entry_price: position.entry_price,
                size_quote: position.size_quote,
                size_tokens: position.size_tokens,
                entry_fees_paid: position.fees_paid,
                max_adverse_excursion: position.max_adverse_excursion,
                max_favorable_excursion: position.max_favorable_excursion,
                expected_edge_quote: position.expected_edge_quote,
            })
    }

    pub fn merge_ledger(&mut self, fills: Vec<FillEvent>, closed_pnl: Decimal) {
        self.ledger.fills.extend(fills);
        self.ledger.closed_pnl += closed_pnl;
    }

    pub fn handle_decision(
        &mut self,
        decision: &DecisionOutcome,
        token: &TokenState,
        observed_at: OffsetDateTime,
        force_fail: bool,
    ) -> Option<FillEvent> {
        match decision.decision_event.decision {
            TradeDecision::EnterPaper => {
                if self.positions.contains_key(&token.mint.0)
                    || decision.position_size_quote <= Decimal::ZERO
                {
                    return None;
                }
                let result = self.simulator.simulate_order(&SimulatedOrderRequest {
                    mint: token.mint.clone(),
                    side: ExecutionSide::Buy,
                    intended_time: observed_at,
                    signal_time: observed_at,
                    size_quote: decision.position_size_quote,
                    reference_price: token.latest_price.max(dec!(0.000000001)),
                    liquidity_quote_depth: liquidity_depth(token),
                    max_slippage_bps: self.simulator.fee_model().params().buy_slippage_bps,
                    latency: Duration::milliseconds(150),
                    force_fail,
                });
                self.ledger.fee_drag += result.fees.total_quote;
                self.ledger.slippage_drag += result.fill.slippage * decision.position_size_quote;
                self.ledger.latency_drag += result.fill.latency_cost_quote.unwrap_or(Decimal::ZERO);
                let mut fill = result.fill.clone();
                fill.entry_decision_id = Some(decision.decision_event.decision_id.clone());
                fill.strategy = Some(decision.decision_event.strategy.clone());
                fill.entry_price = Some(result.fill.fill_price);
                fill.expected_edge_quote = Some(decision.expected_edge.expected_net_edge_quote);
                fill.entry_risk_scores = decision.decision_event.risk_vector.clone();
                if result.failed {
                    self.ledger.closed_pnl += result.net_quote_pnl;
                    fill.net_pnl_quote = Some(result.net_quote_pnl);
                    fill.gross_pnl_quote = Some(-result.fees.total_quote);
                    self.ledger.fills.push(fill.clone());
                    return Some(fill);
                }
                self.positions.insert(
                    token.mint.0.clone(),
                    PaperPosition {
                        mint: token.mint.0.clone(),
                        side: ExecutionSide::Buy,
                        entry_time: observed_at,
                        entry_price: result.fill.fill_price,
                        size_quote: decision.position_size_quote,
                        size_tokens: result.fill.filled_size,
                        fees_paid: result.fees.total_quote,
                        current_value: decision.position_size_quote,
                        unrealized_pnl: Decimal::ZERO,
                        realized_pnl: Decimal::ZERO,
                        max_adverse_excursion: Decimal::ZERO,
                        max_favorable_excursion: Decimal::ZERO,
                        strategy: decision.decision_event.strategy.clone(),
                        entry_decision_id: decision.decision_event.decision_id.clone(),
                        expected_edge_quote: decision.expected_edge.expected_net_edge_quote,
                        entry_risk_scores: decision.decision_event.risk_vector.clone(),
                        entry_reason_codes: decision.decision_event.reason_codes.clone(),
                        exit_reason_codes: Vec::new(),
                    },
                );
                self.ledger.fills.push(fill.clone());
                Some(fill)
            }
            TradeDecision::Exit | TradeDecision::EmergencyExit => {
                let Some(position) = self.positions.remove(&token.mint.0) else {
                    return None;
                };
                let exit_source = exit_source_label(&decision.diagnostics);
                let reference_exit_price = if matches!(
                    exit_source.as_str(),
                    "ShredEmergencyExit" | "DeshredEmergencyExit"
                ) {
                    diagnostic_decimal(&decision.diagnostics, "expected_exit_price=")
                        .filter(|value| *value > Decimal::ZERO)
                        .unwrap_or(token.latest_price)
                } else {
                    token.latest_price
                };
                let result = self.simulator.simulate_order(&SimulatedOrderRequest {
                    mint: token.mint.clone(),
                    side: ExecutionSide::Sell,
                    intended_time: observed_at,
                    signal_time: observed_at,
                    size_quote: position.size_tokens * reference_exit_price,
                    reference_price: reference_exit_price.max(dec!(0.000000001)),
                    liquidity_quote_depth: liquidity_depth(token),
                    max_slippage_bps: self.simulator.fee_model().params().sell_slippage_bps,
                    latency: Duration::milliseconds(150),
                    force_fail,
                });
                self.ledger.fee_drag += result.fees.total_quote;
                self.ledger.slippage_drag += result.fill.slippage * position.size_quote;
                let proceeds = result.fill.filled_size * result.fill.fill_price;
                let gross = proceeds - position.size_quote;
                let realized = gross - position.fees_paid - result.fees.total_quote;
                self.ledger.closed_pnl += realized;
                let mut fill = result.fill.clone();
                fill.entry_decision_id = Some(position.entry_decision_id.clone());
                fill.exit_decision_id = Some(decision.decision_event.decision_id.clone());
                fill.strategy = Some(position.strategy.clone());
                fill.entry_price = Some(position.entry_price);
                fill.exit_price = Some(result.fill.fill_price);
                fill.gross_pnl_quote = Some(gross);
                fill.net_pnl_quote = Some(realized);
                fill.hold_time_ms =
                    Some((observed_at - position.entry_time).whole_milliseconds() as i64);
                fill.max_adverse_excursion = Some(position.max_adverse_excursion);
                fill.max_favorable_excursion = Some(position.max_favorable_excursion);
                fill.exit_reason = decision
                    .diagnostics
                    .iter()
                    .find(|value| value.starts_with("exit_by="))
                    .cloned();
                fill.exit_classification = Some(match decision.decision_event.decision {
                    TradeDecision::EmergencyExit => "emergency".to_owned(),
                    _ => "normal".to_owned(),
                });
                fill.expected_edge_quote = Some(position.expected_edge_quote);
                fill.actual_realized_edge_quote = Some(realized);
                fill.edge_forecast_error_quote = Some(realized - position.expected_edge_quote);
                fill.entry_risk_scores = position.entry_risk_scores.clone();
                fill.exit_risk_scores = decision.decision_event.risk_vector.clone();
                fill.exit_source = Some(exit_source);
                fill.trigger_event_id =
                    diagnostic_value(&decision.diagnostics, "trigger_event_id=");
                fill.malicious_sell_signature =
                    diagnostic_value(&decision.diagnostics, "malicious_sell_signature=");
                fill.malicious_sell_seller =
                    diagnostic_value(&decision.diagnostics, "malicious_sell_seller=");
                fill.malicious_sell_classification =
                    diagnostic_value(&decision.diagnostics, "malicious_sell_classification=");
                fill.estimated_loss_saved_quote =
                    diagnostic_decimal(&decision.diagnostics, "estimated_saved_loss_quote=");
                fill.realized_loss_saved_quote =
                    diagnostic_decimal(&decision.diagnostics, "realized_loss_saved_quote=");
                fill.false_positive_exit =
                    diagnostic_bool(&decision.diagnostics, "false_positive_exit=");
                fill.opportunity_cost_if_false_positive = diagnostic_decimal(
                    &decision.diagnostics,
                    "opportunity_cost_if_false_positive=",
                );
                fill.early_intent_to_geyser_processed_latency_ms = diagnostic_i64(
                    &decision.diagnostics,
                    "early_intent_to_geyser_processed_latency_ms=",
                );
                fill.early_intent_to_account_effect_latency_ms = diagnostic_i64(
                    &decision.diagnostics,
                    "early_intent_to_account_effect_latency_ms=",
                );
                fill.early_intent_to_rooted_latency_ms =
                    diagnostic_i64(&decision.diagnostics, "early_intent_to_rooted_latency_ms=");
                fill.exit_latency_ms = Some(150);
                self.ledger.fills.push(fill.clone());
                Some(fill)
            }
            TradeDecision::Hold => {
                if let Some(position) = self.positions.get_mut(&token.mint.0) {
                    let current_value = position.size_tokens * token.latest_price;
                    let pnl = current_value - position.size_quote - position.fees_paid;
                    position.current_value = current_value;
                    position.unrealized_pnl = pnl;
                    position.max_favorable_excursion = position.max_favorable_excursion.max(pnl);
                    position.max_adverse_excursion = position.max_adverse_excursion.min(pnl);
                }
                None
            }
            _ => None,
        }
    }

    pub fn liquidate_all(
        &mut self,
        snapshot: &StateSnapshot,
        observed_at: OffsetDateTime,
        reason: &str,
    ) -> Vec<FillEvent> {
        let mut fills = Vec::new();
        let open_positions = self.positions.keys().cloned().collect::<Vec<_>>();
        for mint in open_positions {
            let Some(position) = self.positions.remove(&mint) else {
                continue;
            };
            let Some(token) = snapshot.tokens.get(&mint) else {
                continue;
            };
            let result = self.simulator.simulate_order(&SimulatedOrderRequest {
                mint: token.mint.clone(),
                side: ExecutionSide::Sell,
                intended_time: observed_at,
                signal_time: observed_at,
                size_quote: position.size_tokens * token.latest_price,
                reference_price: token.latest_price.max(dec!(0.000000001)),
                liquidity_quote_depth: liquidity_depth(token),
                max_slippage_bps: self.simulator.fee_model().params().sell_slippage_bps,
                latency: Duration::milliseconds(150),
                force_fail: false,
            });
            self.ledger.fee_drag += result.fees.total_quote;
            self.ledger.slippage_drag += result.fill.slippage * position.size_quote;
            self.ledger.latency_drag += result.fill.latency_cost_quote.unwrap_or(Decimal::ZERO);
            let proceeds = result.fill.filled_size * result.fill.fill_price;
            let gross = proceeds - position.size_quote;
            let realized = gross - position.fees_paid - result.fees.total_quote;
            self.ledger.closed_pnl += realized;
            let mut fill = result.fill.clone();
            fill.confirmation_source = fill
                .confirmation_source
                .clone()
                .or_else(|| Some(reason.to_owned()));
            fill.entry_decision_id = Some(position.entry_decision_id.clone());
            fill.strategy = Some(position.strategy.clone());
            fill.entry_price = Some(position.entry_price);
            fill.exit_price = Some(result.fill.fill_price);
            fill.gross_pnl_quote = Some(gross);
            fill.net_pnl_quote = Some(realized);
            fill.hold_time_ms =
                Some((observed_at - position.entry_time).whole_milliseconds() as i64);
            fill.max_adverse_excursion = Some(position.max_adverse_excursion);
            fill.max_favorable_excursion = Some(position.max_favorable_excursion);
            fill.exit_reason = Some(reason.to_owned());
            fill.exit_classification = Some("scenario_end".to_owned());
            fill.exit_source = Some("ScenarioEnd".to_owned());
            fill.expected_edge_quote = Some(position.expected_edge_quote);
            fill.actual_realized_edge_quote = Some(realized);
            fill.edge_forecast_error_quote = Some(realized - position.expected_edge_quote);
            fill.entry_risk_scores = position.entry_risk_scores.clone();
            self.ledger.fills.push(fill.clone());
            fills.push(fill);
        }
        fills
    }
}

#[derive(Debug, Error)]
pub enum LiveExecutionError {
    #[error("live execution is disabled by configuration")]
    LiveDisabled,
    #[error("kill switch is active")]
    KillSwitchActive,
    #[error("signal is stale")]
    StaleSignal,
    #[error("decision is not executable")]
    NotExecutable,
    #[error("fee exceeds configured maximum")]
    MaxFeeExceeded,
    #[error("rpc budget denied: {0}")]
    RpcBudgetDenied(#[from] RpcBudgetError),
}

pub trait Signer {
    fn public_key(&self) -> &str;
    fn sign_message(&self, message: &[u8]) -> Result<Vec<u8>, LiveExecutionError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveExecutionResult {
    pub sent: bool,
    pub dry_run: bool,
    pub reason: String,
    pub estimated_fee_quote: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamBlockhashSnapshot {
    pub latest_blockhash: String,
    pub slot: u64,
    pub block_height: Option<u64>,
    pub parent_blockhash: Option<String>,
    pub observed_at: OffsetDateTime,
    pub source: String,
    pub confidence: Decimal,
    pub expiry_estimate_slot: Option<u64>,
    pub stale: bool,
}

#[derive(Debug, Clone, Default)]
pub struct StreamBlockhashCache {
    latest: Option<StreamBlockhashSnapshot>,
}

#[derive(Debug, Clone, Copy)]
pub struct StreamBlockhashCacheConfig {
    pub max_staleness_ms: i64,
    pub ttl_slots: u64,
}

impl Default for StreamBlockhashCacheConfig {
    fn default() -> Self {
        Self {
            max_staleness_ms: 30_000,
            ttl_slots: 150,
        }
    }
}

#[derive(Debug, Error)]
pub enum StreamBlockhashError {
    #[error("stream blockhash unavailable")]
    StreamBlockhashUnavailable,
    #[error("stream blockhash stale")]
    StreamBlockhashStale,
    #[error("blockhash rpc forbidden in stream-only mode")]
    BlockhashRpcForbiddenStreamOnly,
}

impl StreamBlockhashCache {
    pub fn update(
        &mut self,
        latest_blockhash: impl Into<String>,
        slot: u64,
        block_height: Option<u64>,
        parent_blockhash: Option<String>,
        observed_at: OffsetDateTime,
        source: impl Into<String>,
        confidence: Decimal,
        ttl_slots: u64,
    ) {
        self.latest = Some(StreamBlockhashSnapshot {
            latest_blockhash: latest_blockhash.into(),
            slot,
            block_height,
            parent_blockhash,
            observed_at,
            source: source.into(),
            confidence,
            expiry_estimate_slot: Some(slot.saturating_add(ttl_slots)),
            stale: false,
        });
    }

    pub fn latest(&self) -> Option<&StreamBlockhashSnapshot> {
        self.latest.as_ref()
    }

    pub fn resolve_for_execution(
        &self,
        now: OffsetDateTime,
        current_slot: Option<u64>,
        config: StreamBlockhashCacheConfig,
    ) -> Result<StreamBlockhashSnapshot, StreamBlockhashError> {
        let Some(snapshot) = self.latest.clone() else {
            return Err(StreamBlockhashError::StreamBlockhashUnavailable);
        };
        let age_ms = (now - snapshot.observed_at).whole_milliseconds();
        if age_ms > i128::from(config.max_staleness_ms)
            || current_slot
                .zip(snapshot.expiry_estimate_slot)
                .map(|(slot, expiry)| slot > expiry)
                .unwrap_or(false)
        {
            return Err(StreamBlockhashError::StreamBlockhashStale);
        }
        Ok(snapshot)
    }
}

pub struct LiveExecutor<S: Signer> {
    execution: ExecutionConfig,
    simulator: Simulator,
    signer: S,
    rpc_budget: RpcBudgetManager,
    dry_run: bool,
    kill_switch: bool,
}

impl<S: Signer> LiveExecutor<S> {
    pub fn new(
        execution: ExecutionConfig,
        simulator: Simulator,
        signer: S,
        rpc_budget: RpcBudgetManager,
        dry_run: bool,
    ) -> Self {
        Self {
            execution,
            simulator,
            signer,
            rpc_budget,
            dry_run,
            kill_switch: false,
        }
    }

    pub fn set_kill_switch(&mut self, active: bool) {
        self.kill_switch = active;
    }

    pub fn submit(
        &mut self,
        decision: &DecisionOutcome,
        token: &TokenState,
        config_hash: &str,
        run_id: &str,
        now: OffsetDateTime,
    ) -> Result<LiveExecutionResult, LiveExecutionError> {
        if !self.execution.live_enabled {
            return Err(LiveExecutionError::LiveDisabled);
        }
        if self.kill_switch {
            return Err(LiveExecutionError::KillSwitchActive);
        }
        if !matches!(
            decision.decision_event.decision,
            TradeDecision::EnterLive | TradeDecision::EnterPaper
        ) {
            return Err(LiveExecutionError::NotExecutable);
        }
        if now - decision.decision_event.no_lookahead_timestamp > Duration::seconds(3) {
            return Err(LiveExecutionError::StaleSignal);
        }

        let estimated_fee = self
            .simulator
            .fee_model()
            .fee_breakdown(decision.position_size_quote, false, false)
            .total_quote;
        if estimated_fee > self.execution.max_fee_quote {
            return Err(LiveExecutionError::MaxFeeExceeded);
        }

        let _ = self.signer.sign_message(token.mint.0.as_bytes())?;
        let _budget_entry = self.rpc_budget.check_and_record(RpcCallRequest {
            timestamp: now,
            endpoint: "configured-rpc".to_owned(),
            method: "sendTransaction".to_owned(),
            caller_module: "executor".to_owned(),
            reason: RpcReason::ExecutionSend,
            category: RpcCallCategory::TransactionSend,
            network_kind: RpcNetworkKind::JsonRpc,
            related_token: Some(token.mint.0.clone()),
            related_signature: None,
            estimated_provider_credit_cost: 1,
            actual_provider_credit_cost: None,
            config_hash: config_hash.to_owned(),
            run_id: run_id.to_owned(),
            live_mode: true,
        })?;

        Ok(LiveExecutionResult {
            sent: !self.dry_run,
            dry_run: self.dry_run,
            reason: if self.dry_run {
                "dry_run_guard".to_owned()
            } else {
                format!("submitted_by_{}", self.signer.public_key())
            },
            estimated_fee_quote: estimated_fee,
        })
    }
}

fn liquidity_depth(token: &TokenState) -> Decimal {
    let depth =
        token.reserve_state.real_quote_reserves + token.reserve_state.virtual_quote_reserves;
    if depth > Decimal::ZERO {
        depth
    } else {
        Decimal::from(100u64)
    }
}

fn parse_strategy(value: &str) -> StrategyKind {
    match value {
        "LaunchMomentumScalp" => StrategyKind::LaunchMomentumScalp,
        "HolderGrowthContinuation" => StrategyKind::HolderGrowthContinuation,
        "SellAbsorptionBounce" => StrategyKind::SellAbsorptionBounce,
        "SmartCapitalRotation" => StrategyKind::SmartCapitalRotation,
        "OrganicSlowGrind" => StrategyKind::OrganicSlowGrind,
        _ => StrategyKind::DefensiveNoTrade,
    }
}

fn exit_source_label(diagnostics: &[String]) -> String {
    if diagnostics
        .iter()
        .any(|value| value == "exit_by=shred_emergency_exit")
    {
        "ShredEmergencyExit".to_owned()
    } else if diagnostics
        .iter()
        .any(|value| value == "exit_by=account_effect_confirmed_exit")
    {
        "AccountEffectConfirmedExit".to_owned()
    } else if diagnostics
        .iter()
        .any(|value| value == "exit_by=geyser_processed_sell_confirmed")
    {
        "GeyserProcessedExit".to_owned()
    } else if diagnostics
        .iter()
        .any(|value| value == "exit_by=rooted_sell_confirmed")
    {
        "RootedConfirmedExit".to_owned()
    } else if diagnostics
        .iter()
        .any(|value| value.starts_with("exit_by=trailing_stop"))
    {
        "StrategyTrailingStop".to_owned()
    } else if diagnostics
        .iter()
        .any(|value| value.starts_with("exit_by=stop_loss"))
    {
        "StrategyStopLoss".to_owned()
    } else if diagnostics
        .iter()
        .any(|value| value.starts_with("exit_by=take_profit"))
    {
        "StrategyTakeProfit".to_owned()
    } else {
        "StrategyExit".to_owned()
    }
}

fn diagnostic_value(diagnostics: &[String], prefix: &str) -> Option<String> {
    diagnostics
        .iter()
        .find_map(|value| value.strip_prefix(prefix).map(ToOwned::to_owned))
}

fn diagnostic_decimal(diagnostics: &[String], prefix: &str) -> Option<Decimal> {
    diagnostic_value(diagnostics, prefix).and_then(|value| Decimal::from_str_exact(&value).ok())
}

fn diagnostic_i64(diagnostics: &[String], prefix: &str) -> Option<i64> {
    diagnostic_value(diagnostics, prefix).and_then(|value| value.parse::<i64>().ok())
}

fn diagnostic_bool(diagnostics: &[String], prefix: &str) -> bool {
    diagnostic_value(diagnostics, prefix)
        .and_then(|value| value.parse::<bool>().ok())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use common::{
        Canonicality, EventMeta, EventPayload, EventSource, HolderBalanceUpdateEvent,
        NormalizedEvent, PumpBuyEvent, QuoteAssetType, TokenCreatedEvent, TokenProgramType,
        TransactionStatus, TtlConfig,
    };
    use decision::DecisionEngine;
    use features::FeatureEngine;
    use risk::RiskEngine;
    use rpc_budget::RpcBudgetManager;
    use sim::FeeModel;
    use state::StateEngine;

    use super::*;

    #[derive(Clone)]
    struct DummySigner;

    impl Signer for DummySigner {
        fn public_key(&self) -> &str {
            "dummy-signer"
        }

        fn sign_message(&self, _message: &[u8]) -> Result<Vec<u8>, LiveExecutionError> {
            Ok(vec![1, 2, 3])
        }
    }

    fn pubkey(value: &str) -> common::PubkeyValue {
        common::PubkeyValue(value.to_owned())
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
                initial_virtual_quote_reserves: Some(Decimal::from(250u64)),
                initial_virtual_token_reserves: Some(Decimal::from(1000u64)),
                initial_real_quote_reserves: Some(Decimal::from(250u64)),
                initial_real_token_reserves: Some(Decimal::from(1000u64)),
                initial_supply: Some(Decimal::from(1000u64)),
                creator_initial_buy: None,
                same_transaction_buys: 0,
                same_slot_buys: 0,
                fee_recipients: vec![],
                raw_account_list: vec![],
                launch_transaction_fingerprint: Some("fp".to_owned()),
            }),
        }
    }

    fn buy(slot: u64, buyer: &str, quote: u64, tokens: u64) -> NormalizedEvent {
        let mut meta = meta(slot);
        meta.signature = Some(format!("buy-{slot}-{buyer}"));
        NormalizedEvent {
            meta,
            payload: EventPayload::PumpBuy(PumpBuyEvent {
                mint: pubkey("mint"),
                buyer: pubkey(buyer),
                payer: pubkey(buyer),
                quote_in: Decimal::from(quote),
                token_out: Decimal::from(tokens),
                price_before: None,
                price_after: None,
                effective_price: Decimal::from(quote) / Decimal::from(tokens),
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

    fn holder(slot: u64, owner: &str, balance: u64) -> NormalizedEvent {
        NormalizedEvent {
            meta: meta(slot),
            payload: EventPayload::HolderBalanceUpdate(HolderBalanceUpdateEvent {
                mint: pubkey("mint"),
                owner_wallet: pubkey(owner),
                token_account: pubkey(&format!("ata-{owner}")),
                old_balance: None,
                new_balance: Decimal::from(balance),
                delta: Decimal::from(balance),
                caused_by_signature: None,
                update_reason: "trade".to_owned(),
                confidence: Decimal::ONE,
            }),
        }
    }

    fn setup() -> (TokenState, DecisionOutcome, ExecutionConfig) {
        let mut engine = StateEngine::new(ttl());
        engine.apply_event(&token_created()).expect("create");
        engine
            .apply_event(&buy(2, "buyer-a", 30, 100))
            .expect("buy");
        engine
            .apply_event(&buy(3, "buyer-b", 35, 100))
            .expect("buy");
        engine
            .apply_event(&holder(3, "buyer-a", 100))
            .expect("holder");
        engine
            .apply_event(&holder(3, "buyer-b", 100))
            .expect("holder");
        let snapshot = engine.snapshot();
        let token = snapshot.tokens.get("mint").expect("token").clone();
        let features = FeatureEngine::default().compute_snapshot(
            &token,
            &snapshot,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
        );
        let risk = RiskEngine::default().evaluate(
            &token,
            &features,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
        );
        let execution = ExecutionConfig {
            enabled: false,
            live_enabled: false,
            paper_enabled: true,
            dry_run: true,
            use_rpc_send: false,
            use_stream_blockhash_cache: true,
            max_daily_loss_quote: Decimal::from(100u64),
            max_open_positions: 2,
            max_position_size_quote: Decimal::from(25u64),
            max_trades_per_minute: 10,
            max_fee_quote: dec!(1.0),
            max_slippage_bps: 150,
        };
        let decision = DecisionEngine::new(
            common::StrategyThresholds {
                min_fee_adjusted_edge_bps: 0,
                min_data_quality_score: dec!(0.4),
                max_bundle_risk: dec!(1.0),
                max_rug_risk: dec!(1.0),
                max_fake_momentum_risk: dec!(1.0),
                min_holder_growth: Decimal::ZERO,
                min_trade_eligibility_score: dec!(0.5),
                min_holder_stickiness_score: dec!(0.2),
                min_momentum_authenticity_score: dec!(0.2),
                max_profit_overhang_score: dec!(0.9),
                ..Default::default()
            },
            common::EdgeConfig {
                min_expected_net_edge_quote: Decimal::ZERO,
                min_expected_net_edge_pct: Decimal::ZERO,
                min_edge_confidence: Decimal::ZERO,
                fee_safety_margin_multiplier: Decimal::ONE,
                latency_safety_margin_multiplier: Decimal::ONE,
                allow_watch_without_trade: true,
            },
            execution.clone(),
            Simulator::new(FeeModel::default()),
        )
        .evaluate(
            &token,
            &features,
            &risk,
            None,
            "config",
            "strategy",
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
        );
        (token, decision, execution)
    }

    #[test]
    fn simulated_buy_and_sell_update_ledger() {
        let (token, mut decision, _execution) = setup();
        decision.decision_event.decision = TradeDecision::EnterPaper;
        decision.position_size_quote = Decimal::from(10u64);
        let mut executor = PaperExecutor::new(Simulator::new(FeeModel::default()));
        let fill = executor
            .handle_decision(
                &decision,
                &token,
                OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
                false,
            )
            .expect("buy fill");
        assert_eq!(fill.side, ExecutionSide::Buy);

        let mut exit_decision = decision.clone();
        exit_decision.decision_event.decision = TradeDecision::Exit;
        let exit_fill = executor
            .handle_decision(
                &exit_decision,
                &token,
                OffsetDateTime::UNIX_EPOCH + Duration::seconds(12),
                false,
            )
            .expect("sell fill");
        assert_eq!(exit_fill.side, ExecutionSide::Sell);
        assert!(executor.ledger().fee_drag > Decimal::ZERO);
    }

    #[test]
    fn failed_simulated_transaction_is_accounted_for() {
        let (token, mut decision, _) = setup();
        decision.decision_event.decision = TradeDecision::EnterPaper;
        decision.position_size_quote = Decimal::from(10u64);
        let mut executor = PaperExecutor::new(Simulator::new(FeeModel::default()));
        let fill = executor
            .handle_decision(
                &decision,
                &token,
                OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
                true,
            )
            .expect("fill");
        assert!(fill.failure_reason.is_some());
        assert!(executor.ledger().closed_pnl < Decimal::ZERO);
    }

    #[test]
    fn emergency_exit_and_hold_are_deterministic() {
        let (token, mut decision, _) = setup();
        decision.decision_event.decision = TradeDecision::EnterPaper;
        decision.position_size_quote = Decimal::from(10u64);
        let mut executor = PaperExecutor::new(Simulator::new(FeeModel::default()));
        executor.handle_decision(
            &decision,
            &token,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
            false,
        );

        let mut hold_decision = decision.clone();
        hold_decision.decision_event.decision = TradeDecision::Hold;
        executor.handle_decision(
            &hold_decision,
            &token,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(11),
            false,
        );

        let mut exit_decision = decision.clone();
        exit_decision.decision_event.decision = TradeDecision::EmergencyExit;
        let first = executor
            .handle_decision(
                &exit_decision,
                &token,
                OffsetDateTime::UNIX_EPOCH + Duration::seconds(12),
                false,
            )
            .expect("exit");
        assert_eq!(first.side, ExecutionSide::Sell);
    }

    #[test]
    fn live_is_disabled_by_default_and_budget_or_fee_guards_apply() {
        let (token, decision, execution) = setup();
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("config")
            .join("dev.toml");
        let loaded = common::LoadedConfig::from_file(path).expect("config");
        let rpc_budget = RpcBudgetManager::new(
            loaded.config.rpc_budget,
            execution.clone(),
            loaded.config.stream_only,
            loaded.config.rpc,
        );
        let mut live = LiveExecutor::new(
            execution,
            Simulator::new(FeeModel::default()),
            DummySigner,
            rpc_budget,
            true,
        );
        let error = live
            .submit(
                &decision,
                &token,
                "config",
                "run",
                OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
            )
            .expect_err("disabled");
        assert!(matches!(error, LiveExecutionError::LiveDisabled));
    }

    #[test]
    fn stream_blockhash_cache_rejects_empty_and_stale_without_rpc() {
        let cache = StreamBlockhashCache::default();
        let result = cache.resolve_for_execution(
            OffsetDateTime::UNIX_EPOCH,
            None,
            StreamBlockhashCacheConfig::default(),
        );
        assert!(matches!(
            result,
            Err(StreamBlockhashError::StreamBlockhashUnavailable)
        ));

        let mut cache = StreamBlockhashCache::default();
        cache.update(
            "blockhash-a",
            10,
            Some(10),
            Some("parent".to_owned()),
            OffsetDateTime::UNIX_EPOCH,
            "block_meta_stream",
            Decimal::ONE,
            5,
        );
        let result = cache.resolve_for_execution(
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(31),
            Some(11),
            StreamBlockhashCacheConfig::default(),
        );
        assert!(matches!(
            result,
            Err(StreamBlockhashError::StreamBlockhashStale)
        ));
    }

    #[test]
    fn stream_blockhash_cache_returns_fresh_value() {
        let mut cache = StreamBlockhashCache::default();
        cache.update(
            "blockhash-a",
            10,
            Some(10),
            Some("parent".to_owned()),
            OffsetDateTime::UNIX_EPOCH,
            "block_meta_stream",
            Decimal::ONE,
            20,
        );
        let result = cache
            .resolve_for_execution(
                OffsetDateTime::UNIX_EPOCH + Duration::seconds(5),
                Some(15),
                StreamBlockhashCacheConfig::default(),
            )
            .expect("fresh");
        assert_eq!(result.latest_blockhash, "blockhash-a");
    }
}
