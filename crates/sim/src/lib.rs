use common::{ExecutionSide, FillEvent, NormalizedEvent, PubkeyValue};
use features::FeatureSnapshot;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use state::TokenState;
use thiserror::Error;
use time::{Duration, OffsetDateTime};

macro_rules! dec {
    ($value:literal) => {
        Decimal::from_str_exact(stringify!($value)).expect("decimal literal")
    };
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeParameters {
    pub base_fee_lamports_per_signature: u64,
    pub compute_unit_limit: u32,
    pub compute_unit_price_micro_lamports: u64,
    pub tip_lamports: u64,
    pub ata_rent_lamports: u64,
    pub creator_fee_bps: u64,
    pub protocol_fee_bps: u64,
    pub lp_fee_bps: u64,
    pub buy_slippage_bps: u64,
    pub sell_slippage_bps: u64,
    pub failed_transaction_fee_lamports: u64,
}

impl Default for FeeParameters {
    fn default() -> Self {
        Self {
            base_fee_lamports_per_signature: 5_000,
            compute_unit_limit: 300_000,
            compute_unit_price_micro_lamports: 50_000,
            tip_lamports: 0,
            ata_rent_lamports: 0,
            creator_fee_bps: 30,
            protocol_fee_bps: 95,
            lp_fee_bps: 0,
            buy_slippage_bps: 100,
            sell_slippage_bps: 100,
            failed_transaction_fee_lamports: 5_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeBreakdown {
    pub base_fee_quote: Decimal,
    pub priority_fee_quote: Decimal,
    pub tip_quote: Decimal,
    pub ata_rent_quote: Decimal,
    pub creator_fee_quote: Decimal,
    pub protocol_fee_quote: Decimal,
    pub lp_fee_quote: Decimal,
    pub failed_transaction_fee_quote: Decimal,
    pub total_quote: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedOrderRequest {
    pub mint: PubkeyValue,
    pub side: ExecutionSide,
    pub intended_time: OffsetDateTime,
    pub signal_time: OffsetDateTime,
    pub size_quote: Decimal,
    #[serde(default)]
    pub size_tokens: Option<Decimal>,
    pub reference_price: Decimal,
    pub liquidity_quote_depth: Decimal,
    pub max_slippage_bps: u64,
    pub latency: Duration,
    pub force_fail: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedOrderResult {
    pub fill: FillEvent,
    pub net_quote_pnl: Decimal,
    pub fees: FeeBreakdown,
    pub impacted_price: Decimal,
    pub failed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionIntent {
    pub mint: PubkeyValue,
    pub entry_time: OffsetDateTime,
    pub exit_time: OffsetDateTime,
    pub size_quote: Decimal,
    pub entry_price: Decimal,
    pub exit_price: Decimal,
    pub liquidity_quote_depth: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BacktestSummary {
    pub total_pnl: Decimal,
    pub net_pnl: Decimal,
    pub fee_drag: Decimal,
    pub slippage_drag: Decimal,
    pub failed_transaction_drag: Decimal,
    pub hit_rate: Decimal,
    pub trade_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenLabels {
    pub mint: PubkeyValue,
    pub max_return_30s: Decimal,
    pub max_return_1m: Decimal,
    pub max_return_5m: Decimal,
    pub max_drawdown_30s: Decimal,
    pub max_drawdown_1m: Decimal,
    pub max_drawdown_5m: Decimal,
    pub time_to_20pct_profit_ms: Option<i64>,
    pub time_to_50pct_drawdown_ms: Option<i64>,
    pub time_to_dev_sell_ms: Option<i64>,
    pub time_to_no_buy_gap_ms: Option<i64>,
    pub strategy_realistic_pnl: Decimal,
    pub strategy_oracle_pnl: Decimal,
    pub oracle_minus_realistic_gap: Decimal,
    pub opportunity_duration_ms: i64,
    pub opportunity_half_life_ms: i64,
    pub was_scalpable: bool,
    pub was_rug: bool,
    pub profitable_after_fees_flag: bool,
    pub profitable_after_slippage_flag: bool,
    pub profitable_after_realistic_size_flag: bool,
}

#[derive(Debug, Error)]
pub enum SimError {
    #[error("no-lookahead violation: events are out of order at index {0}")]
    NoLookaheadViolation(usize),
}

#[derive(Debug, Clone)]
pub struct FeeModel {
    params: FeeParameters,
}

impl FeeModel {
    pub fn new(params: FeeParameters) -> Self {
        Self { params }
    }

    pub fn params(&self) -> &FeeParameters {
        &self.params
    }

    pub fn priority_fee_lamports(&self) -> u64 {
        let numerator = u64::from(self.params.compute_unit_limit)
            * self.params.compute_unit_price_micro_lamports;
        numerator.div_ceil(1_000_000)
    }

    pub fn fee_breakdown(
        &self,
        notional_quote: Decimal,
        include_ata: bool,
        failed: bool,
    ) -> FeeBreakdown {
        let base = lamports_to_quote(self.params.base_fee_lamports_per_signature);
        let priority = lamports_to_quote(self.priority_fee_lamports());
        let tip = lamports_to_quote(self.params.tip_lamports);
        let ata = if include_ata {
            lamports_to_quote(self.params.ata_rent_lamports)
        } else {
            Decimal::ZERO
        };
        let creator_fee =
            notional_quote * Decimal::from(self.params.creator_fee_bps) / Decimal::from(10_000u64);
        let protocol_fee =
            notional_quote * Decimal::from(self.params.protocol_fee_bps) / Decimal::from(10_000u64);
        let lp_fee =
            notional_quote * Decimal::from(self.params.lp_fee_bps) / Decimal::from(10_000u64);
        let failed_fee = if failed {
            lamports_to_quote(self.params.failed_transaction_fee_lamports)
        } else {
            Decimal::ZERO
        };
        let total = base + priority + tip + ata + creator_fee + protocol_fee + lp_fee + failed_fee;
        FeeBreakdown {
            base_fee_quote: base,
            priority_fee_quote: priority,
            tip_quote: tip,
            ata_rent_quote: ata,
            creator_fee_quote: creator_fee,
            protocol_fee_quote: protocol_fee,
            lp_fee_quote: lp_fee,
            failed_transaction_fee_quote: failed_fee,
            total_quote: total,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Simulator {
    fee_model: FeeModel,
}

impl Default for FeeModel {
    fn default() -> Self {
        Self::new(FeeParameters::default())
    }
}

impl Simulator {
    pub fn new(fee_model: FeeModel) -> Self {
        Self { fee_model }
    }

    pub fn fee_model(&self) -> &FeeModel {
        &self.fee_model
    }

    pub fn simulate_order(&self, request: &SimulatedOrderRequest) -> SimulatedOrderResult {
        let reference_notional = match request.side {
            ExecutionSide::Buy => request.size_quote,
            ExecutionSide::Sell => request
                .size_tokens
                .map(|tokens| tokens * request.reference_price)
                .unwrap_or(request.size_quote),
        };
        let impact_pct = if request.liquidity_quote_depth > Decimal::ZERO {
            clamp_nonnegative(reference_notional / request.liquidity_quote_depth)
        } else {
            Decimal::ONE
        };
        let configured_slippage_bps = match request.side {
            ExecutionSide::Buy => self.fee_model.params.buy_slippage_bps,
            ExecutionSide::Sell => self.fee_model.params.sell_slippage_bps,
        };
        let slippage_pct = Decimal::from(configured_slippage_bps.min(request.max_slippage_bps))
            / Decimal::from(10_000u64);
        let impact_multiplier = Decimal::ONE + impact_pct + slippage_pct;
        let impacted_price = match request.side {
            ExecutionSide::Buy => request.reference_price * impact_multiplier,
            ExecutionSide::Sell => request.reference_price / impact_multiplier,
        };
        let landed = request.signal_time + request.latency;
        let notional = match request.side {
            ExecutionSide::Buy => request.size_quote,
            ExecutionSide::Sell => request
                .size_tokens
                .map(|tokens| tokens * impacted_price)
                .unwrap_or(request.size_quote),
        };
        let fees = self
            .fee_model
            .fee_breakdown(notional, false, request.force_fail);
        let failed = request.force_fail;
        let filled_size = if failed || impacted_price <= Decimal::ZERO {
            Decimal::ZERO
        } else if matches!(request.side, ExecutionSide::Sell) {
            request.size_tokens.unwrap_or(notional / impacted_price)
        } else {
            request.size_quote / impacted_price
        };
        let fill = FillEvent {
            mint: request.mint.clone(),
            side: request.side,
            intended_time: request.intended_time,
            signal_time: request.signal_time,
            send_time: request.signal_time,
            landing_time: Some(landed),
            confirmation_time: Some(landed + Duration::milliseconds(400)),
            intended_size: request.size_tokens.unwrap_or(request.size_quote),
            filled_size,
            fill_price: impacted_price,
            notional,
            fees: fees.total_quote,
            slippage: slippage_pct,
            price_impact: impact_pct,
            failure_reason: failed.then(|| "forced_failure".to_owned()),
            transaction_signature: None,
            confirmation_source: Some("simulation".to_owned()),
            entry_decision_id: None,
            exit_decision_id: None,
            strategy: None,
            entry_price: None,
            exit_price: None,
            gross_pnl_quote: None,
            net_pnl_quote: None,
            base_fee_quote: Some(fees.base_fee_quote),
            priority_fee_quote: Some(fees.priority_fee_quote),
            tip_quote: Some(fees.tip_quote),
            slippage_cost_quote: Some(slippage_pct * notional),
            curve_impact_cost_quote: Some(impact_pct * notional),
            latency_cost_quote: Some(Decimal::ZERO),
            failed_tx_cost_quote: Some(fees.failed_transaction_fee_quote),
            hold_time_ms: None,
            max_adverse_excursion: None,
            max_favorable_excursion: None,
            exit_reason: None,
            exit_classification: None,
            expected_edge_quote: None,
            actual_realized_edge_quote: None,
            edge_forecast_error_quote: None,
            entry_risk_scores: std::collections::BTreeMap::new(),
            exit_risk_scores: std::collections::BTreeMap::new(),
            exit_source: None,
            trigger_event_id: None,
            malicious_sell_signature: None,
            malicious_sell_seller: None,
            malicious_sell_classification: None,
            estimated_loss_saved_quote: None,
            realized_loss_saved_quote: None,
            false_positive_exit: false,
            opportunity_cost_if_false_positive: None,
            early_intent_to_geyser_processed_latency_ms: None,
            early_intent_to_account_effect_latency_ms: None,
            early_intent_to_rooted_latency_ms: None,
            exit_latency_ms: None,
        };
        SimulatedOrderResult {
            net_quote_pnl: if failed {
                -fees.total_quote
            } else {
                -(fees.total_quote)
            },
            fill,
            fees,
            impacted_price,
            failed,
        }
    }

    pub fn simulate_round_trip(
        &self,
        intent: &PositionIntent,
    ) -> (SimulatedOrderResult, SimulatedOrderResult, Decimal) {
        let buy = self.simulate_order(&SimulatedOrderRequest {
            mint: intent.mint.clone(),
            side: ExecutionSide::Buy,
            intended_time: intent.entry_time,
            signal_time: intent.entry_time,
            size_quote: intent.size_quote,
            reference_price: intent.entry_price,
            liquidity_quote_depth: intent.liquidity_quote_depth,
            max_slippage_bps: self.fee_model.params.buy_slippage_bps,
            size_tokens: None,
            latency: Duration::milliseconds(150),
            force_fail: false,
        });
        let sell = self.simulate_order(&SimulatedOrderRequest {
            mint: intent.mint.clone(),
            side: ExecutionSide::Sell,
            intended_time: intent.exit_time,
            signal_time: intent.exit_time,
            size_quote: buy.fill.filled_size * intent.exit_price,
            size_tokens: Some(buy.fill.filled_size),
            reference_price: intent.exit_price,
            liquidity_quote_depth: intent.liquidity_quote_depth,
            max_slippage_bps: self.fee_model.params.sell_slippage_bps,
            latency: Duration::milliseconds(150),
            force_fail: false,
        });
        let proceeds = sell.fill.filled_size * sell.fill.fill_price;
        let cost = intent.size_quote;
        let net = proceeds - cost - buy.fees.total_quote - sell.fees.total_quote;
        (buy, sell, net)
    }

    pub fn backtest_positions(&self, intents: &[PositionIntent]) -> BacktestSummary {
        let mut summary = BacktestSummary::default();
        let mut wins = 0u64;
        for intent in intents {
            let (buy, sell, pnl) = self.simulate_round_trip(intent);
            summary.total_pnl += sell.fill.filled_size * sell.fill.fill_price - intent.size_quote;
            summary.net_pnl += pnl;
            summary.fee_drag += buy.fees.total_quote + sell.fees.total_quote;
            summary.slippage_drag +=
                buy.fill.slippage * buy.fill.notional + sell.fill.slippage * sell.fill.notional;
            summary.failed_transaction_drag +=
                buy.fees.failed_transaction_fee_quote + sell.fees.failed_transaction_fee_quote;
            summary.trade_count += 1;
            if pnl > Decimal::ZERO {
                wins += 1;
            }
        }
        if summary.trade_count > 0 {
            summary.hit_rate = Decimal::from(wins) / Decimal::from(summary.trade_count);
        }
        summary
    }

    pub fn generate_labels(
        &self,
        token: &TokenState,
        features: &FeatureSnapshot,
        size_quote: Decimal,
    ) -> TokenLabels {
        let max_return_30s =
            window_extreme_return(token, Duration::seconds(30)).unwrap_or(Decimal::ZERO);
        let max_return_1m =
            window_extreme_return(token, Duration::minutes(1)).unwrap_or(Decimal::ZERO);
        let max_return_5m =
            window_extreme_return(token, Duration::minutes(5)).unwrap_or(Decimal::ZERO);
        let max_drawdown_30s =
            window_extreme_drawdown(token, Duration::seconds(30)).unwrap_or(Decimal::ZERO);
        let max_drawdown_1m =
            window_extreme_drawdown(token, Duration::minutes(1)).unwrap_or(Decimal::ZERO);
        let max_drawdown_5m =
            window_extreme_drawdown(token, Duration::minutes(5)).unwrap_or(Decimal::ZERO);
        let oracle_pnl = max_return_1m * size_quote;

        let realistic_pnl =
            if let (Some(entry_price), Some(exit_price)) = (first_price(token), max_price(token)) {
                let (_, _, net) = self.simulate_round_trip(&PositionIntent {
                    mint: token.mint.clone(),
                    entry_time: token.launch_time.unwrap_or(OffsetDateTime::UNIX_EPOCH),
                    exit_time: token
                        .trade_stats
                        .price_history
                        .back()
                        .map(|(time, _)| *time)
                        .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                    size_quote,
                    entry_price,
                    exit_price,
                    liquidity_quote_depth: liquidity_depth(token),
                });
                net
            } else {
                Decimal::ZERO
            };

        TokenLabels {
            mint: token.mint.clone(),
            max_return_30s,
            max_return_1m,
            max_return_5m,
            max_drawdown_30s,
            max_drawdown_1m,
            max_drawdown_5m,
            time_to_20pct_profit_ms: time_to_return(token, dec!(0.2)),
            time_to_50pct_drawdown_ms: time_to_drawdown(token, dec!(0.5)),
            time_to_dev_sell_ms: token
                .developer_state
                .creator_first_sell_time
                .zip(token.launch_time)
                .map(|(sell_time, launch)| (sell_time - launch).whole_milliseconds() as i64),
            time_to_no_buy_gap_ms: Some(token.trade_stats.longest_no_buy_gap_ms),
            strategy_realistic_pnl: realistic_pnl,
            strategy_oracle_pnl: oracle_pnl,
            oracle_minus_realistic_gap: oracle_pnl - realistic_pnl,
            opportunity_duration_ms: token
                .launch_time
                .zip(
                    token
                        .trade_stats
                        .price_history
                        .back()
                        .map(|(time, _)| *time),
                )
                .map(|(launch, end)| (end - launch).whole_milliseconds() as i64)
                .unwrap_or_default(),
            opportunity_half_life_ms: token
                .launch_time
                .zip(
                    token
                        .trade_stats
                        .price_history
                        .back()
                        .map(|(time, _)| *time),
                )
                .map(|(launch, end)| ((end - launch).whole_milliseconds() / 2) as i64)
                .unwrap_or_default(),
            was_scalpable: max_return_1m > dec!(0.10),
            was_rug: features
                .decimal("rug_probability_score")
                .unwrap_or(Decimal::ZERO)
                >= dec!(0.8)
                || features
                    .decimal("price_drop_70pct_from_launch_within_5m")
                    .unwrap_or(Decimal::ZERO)
                    > Decimal::ZERO,
            profitable_after_fees_flag: realistic_pnl > Decimal::ZERO,
            profitable_after_slippage_flag: realistic_pnl > Decimal::ZERO,
            profitable_after_realistic_size_flag: realistic_pnl > Decimal::ZERO,
        }
    }

    pub fn validate_no_lookahead(events: &[ReplayEvent]) -> Result<(), SimError> {
        for (index, window) in events.windows(2).enumerate() {
            if window[0].observed_at > window[1].observed_at {
                return Err(SimError::NoLookaheadViolation(index + 1));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ReplayEvent {
    pub observed_at: OffsetDateTime,
    pub event: NormalizedEvent,
}

fn lamports_to_quote(lamports: u64) -> Decimal {
    Decimal::from(lamports) / Decimal::from(1_000_000_000u64)
}

fn clamp_nonnegative(value: Decimal) -> Decimal {
    value.max(Decimal::ZERO)
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

fn first_price(token: &TokenState) -> Option<Decimal> {
    token
        .trade_stats
        .price_history
        .front()
        .map(|(_, price)| *price)
}

fn max_price(token: &TokenState) -> Option<Decimal> {
    token
        .trade_stats
        .price_history
        .iter()
        .map(|(_, price)| *price)
        .max()
}

fn window_extreme_return(token: &TokenState, window: Duration) -> Option<Decimal> {
    let launch_time = token.launch_time?;
    let end = launch_time + window;
    let start_price = first_price(token)?;
    let max_price = token
        .trade_stats
        .price_history
        .iter()
        .filter(|(time, _)| *time <= end)
        .map(|(_, price)| *price)
        .max()?;
    if start_price > Decimal::ZERO {
        Some((max_price - start_price) / start_price)
    } else {
        None
    }
}

fn window_extreme_drawdown(token: &TokenState, window: Duration) -> Option<Decimal> {
    let launch_time = token.launch_time?;
    let end = launch_time + window;
    let start_price = first_price(token)?;
    let min_price = token
        .trade_stats
        .price_history
        .iter()
        .filter(|(time, _)| *time <= end)
        .map(|(_, price)| *price)
        .min()?;
    if start_price > Decimal::ZERO {
        Some((start_price - min_price) / start_price)
    } else {
        None
    }
}

fn time_to_return(token: &TokenState, threshold: Decimal) -> Option<i64> {
    let launch_time = token.launch_time?;
    let launch_price = first_price(token)?;
    token
        .trade_stats
        .price_history
        .iter()
        .find(|(_, price)| {
            launch_price > Decimal::ZERO && ((*price - launch_price) / launch_price) >= threshold
        })
        .map(|(time, _)| (*time - launch_time).whole_milliseconds() as i64)
}

fn time_to_drawdown(token: &TokenState, threshold: Decimal) -> Option<i64> {
    let launch_time = token.launch_time?;
    let launch_price = first_price(token)?;
    token
        .trade_stats
        .price_history
        .iter()
        .find(|(_, price)| {
            launch_price > Decimal::ZERO && ((launch_price - *price) / launch_price) >= threshold
        })
        .map(|(time, _)| (*time - launch_time).whole_milliseconds() as i64)
}

#[cfg(test)]
mod tests {
    use common::{
        Canonicality, EventMeta, EventPayload, EventSource, NormalizedEvent, PumpBuyEvent,
        PumpSellEvent, QuoteAssetType, TokenCreatedEvent, TokenProgramType, TransactionStatus,
        TtlConfig,
    };
    use features::FeatureEngine;
    use state::StateEngine;

    use super::*;

    fn pubkey(value: &str) -> PubkeyValue {
        PubkeyValue(value.to_owned())
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
            }),
        }
    }

    fn buy(slot: u64, price_num: u64, price_den: u64) -> NormalizedEvent {
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
                effective_price: Decimal::from(price_num) / Decimal::from(price_den),
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

    fn sell(slot: u64, price_num: u64, price_den: u64) -> NormalizedEvent {
        let mut meta = meta(slot);
        meta.signature = Some(format!("sell-{slot}"));
        NormalizedEvent {
            meta,
            payload: EventPayload::PumpSell(PumpSellEvent {
                mint: pubkey("mint"),
                seller: pubkey("buyer"),
                quote_out: Decimal::from(12u64),
                token_in: Decimal::from(100u64),
                price_before: None,
                price_after: None,
                effective_price: Decimal::from(price_num) / Decimal::from(price_den),
                slippage_estimate: None,
                reserves_before: None,
                reserves_after: None,
                min_quote_output: None,
                compute_unit_limit: None,
                compute_unit_price: None,
                estimated_priority_fee_lamports: None,
                estimated_base_fee_lamports: None,
                estimated_tip_lamports: None,
                is_creator: false,
                is_top_holder_pre_sell: false,
                is_known_cluster_member: false,
                status: TransactionStatus::Success,
            }),
        }
    }

    fn token_and_features() -> (TokenState, FeatureSnapshot) {
        let mut engine = StateEngine::new(ttl());
        engine.apply_event(&token_created()).expect("create");
        engine.apply_event(&buy(2, 12, 100)).expect("buy");
        engine.apply_event(&buy(3, 15, 100)).expect("buy");
        engine.apply_event(&sell(4, 8, 100)).expect("sell");
        let snapshot = engine.snapshot();
        let token = snapshot.tokens.get("mint").expect("token").clone();
        let features = FeatureEngine::default().compute_snapshot(
            &token,
            &snapshot,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
        );
        (token, features)
    }

    #[test]
    fn fee_model_computes_priority_fee() {
        let model = FeeModel::default();
        assert!(model.priority_fee_lamports() > 0);
        let breakdown = model.fee_breakdown(Decimal::from(10u64), false, false);
        assert!(breakdown.total_quote > Decimal::ZERO);
        assert_eq!(model.params().creator_fee_bps, 30);
        assert_eq!(model.params().protocol_fee_bps, 95);
        assert_eq!(model.params().lp_fee_bps, 0);
        assert_eq!(
            model.params().creator_fee_bps
                + model.params().protocol_fee_bps
                + model.params().lp_fee_bps,
            125
        );
        assert_eq!(breakdown.creator_fee_quote, dec!(0.03));
        assert_eq!(breakdown.protocol_fee_quote, dec!(0.095));
    }

    #[test]
    fn slippage_and_impact_affect_fills() {
        let simulator = Simulator::new(FeeModel::default());
        let result = simulator.simulate_order(&SimulatedOrderRequest {
            mint: pubkey("mint"),
            side: ExecutionSide::Buy,
            intended_time: OffsetDateTime::UNIX_EPOCH,
            signal_time: OffsetDateTime::UNIX_EPOCH,
            size_quote: Decimal::from(10u64),
            size_tokens: None,
            reference_price: dec!(0.10),
            liquidity_quote_depth: Decimal::from(20u64),
            max_slippage_bps: 150,
            latency: Duration::milliseconds(100),
            force_fail: false,
        });
        assert!(result.fill.fill_price > dec!(0.10));
        assert!(result.fill.price_impact > Decimal::ZERO);
    }

    #[test]
    fn no_lookahead_enforcement_catches_reordering() {
        let events = vec![
            ReplayEvent {
                observed_at: OffsetDateTime::UNIX_EPOCH + Duration::seconds(2),
                event: token_created(),
            },
            ReplayEvent {
                observed_at: OffsetDateTime::UNIX_EPOCH + Duration::seconds(1),
                event: buy(2, 10, 100),
            },
        ];
        let error = Simulator::validate_no_lookahead(&events).expect_err("must fail");
        assert!(matches!(error, SimError::NoLookaheadViolation(_)));
    }

    #[test]
    fn label_generation_and_backtest_include_fees() {
        let (token, features) = token_and_features();
        let simulator = Simulator::new(FeeModel::default());
        let labels = simulator.generate_labels(&token, &features, Decimal::from(10u64));
        assert!(labels.max_return_1m >= Decimal::ZERO);
        let summary = simulator.backtest_positions(&[PositionIntent {
            mint: token.mint.clone(),
            entry_time: token.launch_time.unwrap_or(OffsetDateTime::UNIX_EPOCH),
            exit_time: token
                .trade_stats
                .price_history
                .back()
                .map(|(time, _)| *time)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
            size_quote: Decimal::from(10u64),
            entry_price: first_price(&token).unwrap_or(dec!(0.10)),
            exit_price: max_price(&token).unwrap_or(dec!(0.12)),
            liquidity_quote_depth: Decimal::from(100u64),
        }]);
        assert!(summary.fee_drag > Decimal::ZERO);
    }

    #[test]
    fn failed_tx_fee_accounting_is_included() {
        let simulator = Simulator::new(FeeModel::default());
        let result = simulator.simulate_order(&SimulatedOrderRequest {
            mint: pubkey("mint"),
            side: ExecutionSide::Buy,
            intended_time: OffsetDateTime::UNIX_EPOCH,
            signal_time: OffsetDateTime::UNIX_EPOCH,
            size_quote: Decimal::from(10u64),
            size_tokens: None,
            reference_price: dec!(0.10),
            liquidity_quote_depth: Decimal::from(50u64),
            max_slippage_bps: 100,
            latency: Duration::milliseconds(100),
            force_fail: true,
        });
        assert!(result.failed);
        assert!(result.fees.failed_transaction_fee_quote > Decimal::ZERO);
    }

    #[test]
    fn sell_simulation_preserves_token_size_and_slippage_reduces_proceeds() {
        let simulator = Simulator::new(FeeModel::default());
        let no_slip = simulator.simulate_order(&SimulatedOrderRequest {
            mint: pubkey("mint"),
            side: ExecutionSide::Sell,
            intended_time: OffsetDateTime::UNIX_EPOCH,
            signal_time: OffsetDateTime::UNIX_EPOCH,
            size_quote: Decimal::from(10u64),
            size_tokens: Some(Decimal::from(100u64)),
            reference_price: dec!(0.10),
            liquidity_quote_depth: Decimal::from(1_000_000u64),
            max_slippage_bps: 0,
            latency: Duration::milliseconds(100),
            force_fail: false,
        });
        let slipped = simulator.simulate_order(&SimulatedOrderRequest {
            mint: pubkey("mint"),
            side: ExecutionSide::Sell,
            intended_time: OffsetDateTime::UNIX_EPOCH,
            signal_time: OffsetDateTime::UNIX_EPOCH,
            size_quote: Decimal::from(10u64),
            size_tokens: Some(Decimal::from(100u64)),
            reference_price: dec!(0.10),
            liquidity_quote_depth: Decimal::from(1_000_000u64),
            max_slippage_bps: 100,
            latency: Duration::milliseconds(100),
            force_fail: false,
        });
        assert_eq!(slipped.fill.filled_size, Decimal::from(100u64));
        assert!(slipped.fill.fill_price < no_slip.fill.fill_price);
        assert!(slipped.fill.notional < no_slip.fill.notional);
    }
}
