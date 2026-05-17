use std::collections::BTreeMap;

use common::{
    EdgeConfig, EventId, ExecutionConfig, ReasonCode, StrategyProfileConfig, StrategyThresholds,
    TentativeSellRiskLevel, TradeDecision, TradeDecisionEvent,
};
use features::FeatureSnapshot;
use risk::{DiscardPolicyDecision, RiskAssessment};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sim::{PositionIntent, Simulator};
use state::{TokenLifecycle, TokenState};
use time::OffsetDateTime;

macro_rules! dec {
    ($value:literal) => {
        Decimal::from_str_exact(stringify!($value)).expect("decimal literal")
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyKind {
    LaunchMomentumScalp,
    HolderGrowthContinuation,
    SellAbsorptionBounce,
    SmartCapitalRotation,
    OrganicSlowGrind,
    DefensiveNoTrade,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeScore {
    pub raw_score: Decimal,
    pub regime_adjusted_score: Decimal,
    pub confidence: Decimal,
    pub missing_data_penalty: Decimal,
    pub top_positive_components: Vec<String>,
    pub top_negative_components: Vec<String>,
    pub reason_codes: Vec<ReasonCode>,
    pub recommended_action_influence: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionOutcome {
    pub decision_event: TradeDecisionEvent,
    pub strategy: StrategyKind,
    pub position_size_quote: Decimal,
    pub expected_edge: ExpectedExecutableEdge,
    pub composite_scores: BTreeMap<String, CompositeScore>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpectedExecutableEdge {
    pub expected_price_move_pct: Decimal,
    pub expected_exit_price: Decimal,
    pub expected_hold_time_ms: u64,
    pub expected_fee_drag_quote: Decimal,
    pub expected_slippage_drag_quote: Decimal,
    pub expected_latency_drag_quote: Decimal,
    pub expected_curve_impact_quote: Decimal,
    pub expected_exit_impact_quote: Decimal,
    pub expected_failed_tx_cost_quote: Decimal,
    pub minimum_required_move_pct: Decimal,
    pub expected_net_edge_quote: Decimal,
    pub expected_net_edge_pct: Decimal,
    pub confidence: Decimal,
    pub reason_codes: Vec<ReasonCode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenPositionContext {
    pub strategy: StrategyKind,
    pub entry_time: OffsetDateTime,
    pub entry_price: Decimal,
    pub size_quote: Decimal,
    pub size_tokens: Decimal,
    pub entry_fees_paid: Decimal,
    pub max_adverse_excursion: Decimal,
    pub max_favorable_excursion: Decimal,
    pub expected_edge_quote: Decimal,
}

#[derive(Debug, Clone)]
pub struct DecisionEngine {
    thresholds: StrategyThresholds,
    edge: EdgeConfig,
    execution: ExecutionConfig,
    simulator: Simulator,
    kill_switch: bool,
}

impl DecisionEngine {
    pub fn new(
        thresholds: StrategyThresholds,
        edge: EdgeConfig,
        execution: ExecutionConfig,
        simulator: Simulator,
    ) -> Self {
        Self {
            thresholds,
            edge,
            execution,
            simulator,
            kill_switch: false,
        }
    }

    pub fn set_kill_switch(&mut self, active: bool) {
        self.kill_switch = active;
    }

    pub fn evaluate(
        &self,
        token: &TokenState,
        features: &FeatureSnapshot,
        risk: &RiskAssessment,
        position: Option<&OpenPositionContext>,
        config_hash: &str,
        strategy_version: &str,
        observed_at: OffsetDateTime,
    ) -> DecisionOutcome {
        let composite_scores = self.composite_scores(token, features, risk);
        let trade_eligibility = composite_scores
            .get("TradeEligibilityScore")
            .map(|score| score.regime_adjusted_score)
            .unwrap_or(Decimal::ZERO);
        let fee_edge = composite_scores
            .get("FeeAdjustedEdgeScore")
            .map(|score| score.raw_score)
            .unwrap_or(Decimal::ZERO);
        let holder_stickiness = features
            .decimal("holder_stickiness_score")
            .unwrap_or(Decimal::ZERO);
        let authenticity = features
            .decimal("momentum_authenticity_score")
            .unwrap_or(Decimal::ZERO);
        let profit_overhang = features
            .decimal("profit_overhang_score")
            .unwrap_or(Decimal::ZERO);
        let breadth_ready = token.trade_stats.unique_buyers.len() >= 3
            || token.holder_state.nonzero_holder_count >= 3;
        let bundle_entry_limit = (self.thresholds.max_bundle_risk - dec!(0.10)).max(dec!(0.30));
        let bundle_concentrated = risk
            .bundle
            .reason_codes
            .contains(&ReasonCode::BundleConcentration)
            || risk
                .bundle
                .reason_codes
                .contains(&ReasonCode::HighBundleRisk);
        let fake_momentum_flagged = risk
            .reason_codes
            .contains(&ReasonCode::HighFakeMomentumRisk)
            || risk
                .fake_momentum
                .reason_codes
                .contains(&ReasonCode::HighFakeMomentumRisk);
        let shred_warning_level = token.shred_defense.last_warning_level;
        let shred_exit_armed = token.shred_defense.shred_exit_armed_flag;
        let shred_exit_triggered = token.shred_defense.shred_emergency_exit_triggered_flag;
        let shred_low_confidence = token.shred_defense.preconfirmation_exit_confidence < dec!(0.55);
        let shred_entry_block = matches!(
            shred_warning_level,
            Some(
                TentativeSellRiskLevel::ExitArmed
                    | TentativeSellRiskLevel::EmergencyExitRecommended
                    | TentativeSellRiskLevel::EmergencyExitRequired
            )
        );
        let mut diagnostics = Vec::new();

        let mut reason_codes = BTreeMap::<ReasonCode, ()>::new();
        let decision;
        let mut strategy = position
            .map(|position| position.strategy)
            .unwrap_or(StrategyKind::DefensiveNoTrade);
        if position.is_none() {
            strategy = select_strategy(features, risk, &composite_scores);
        }
        let expected_edge = self.expected_executable_edge(token, features, risk, strategy);

        if self.kill_switch {
            decision = terminal_decision(token);
            reason_codes.insert(ReasonCode::KillSwitchActive, ());
            diagnostics.push("blocked_by=kill_switch".to_owned());
        } else if risk.data_quality.recommended_lifecycle_action
            == risk::LifecycleAction::DataGapStop
            || risk.discard_policy == DiscardPolicyDecision::DataGapStop
        {
            decision = terminal_or_emergency(token);
            reason_codes.insert(ReasonCode::DataGapActive, ());
            diagnostics.push("blocked_by=data_gap".to_owned());
        } else if risk.rug.score >= self.thresholds.max_rug_risk
            || risk.bundle.score >= self.thresholds.max_bundle_risk
            || risk.fake_momentum.score >= self.thresholds.max_fake_momentum_risk
        {
            decision = terminal_or_emergency(token);
            if risk.rug.score >= self.thresholds.max_rug_risk {
                reason_codes.insert(ReasonCode::HighRugRisk, ());
            }
            if risk.bundle.score >= self.thresholds.max_bundle_risk {
                reason_codes.insert(ReasonCode::HighBundleRisk, ());
            }
            if risk.fake_momentum.score >= self.thresholds.max_fake_momentum_risk {
                reason_codes.insert(ReasonCode::HighFakeMomentumRisk, ());
            }
            diagnostics.push(format!(
                "blocked_by=risk rug={} bundle={} fake={} thresholds={}/{}/{}",
                risk.rug.score,
                risk.bundle.score,
                risk.fake_momentum.score,
                self.thresholds.max_rug_risk,
                self.thresholds.max_bundle_risk,
                self.thresholds.max_fake_momentum_risk
            ));
        } else if features
            .decimal("data_quality_score")
            .unwrap_or(Decimal::ZERO)
            < self.thresholds.min_data_quality_score
        {
            decision = TradeDecision::WatchLight;
            reason_codes.insert(ReasonCode::DataGapActive, ());
            diagnostics.push(format!(
                "blocked_by=data_quality value={} threshold={}",
                features
                    .decimal("data_quality_score")
                    .unwrap_or(Decimal::ZERO),
                self.thresholds.min_data_quality_score
            ));
        } else if fee_edge
            < Decimal::from(self.thresholds.min_fee_adjusted_edge_bps) / Decimal::from(10_000u64)
        {
            decision = if position.is_some() {
                TradeDecision::Exit
            } else {
                TradeDecision::WatchDeep
            };
            reason_codes.insert(ReasonCode::FeeAdjustedEdgeTooLow, ());
            diagnostics.push(format!(
                "blocked_by=fee_edge value={} threshold={}",
                fee_edge,
                Decimal::from(self.thresholds.min_fee_adjusted_edge_bps) / Decimal::from(10_000u64)
            ));
        } else {
            match (position.is_some(), token.lifecycle) {
                (true, _) => {
                    let position = position.expect("position checked");
                    let current_value = position.size_tokens * token.latest_price;
                    let current_pnl =
                        current_value - position.size_quote - position.entry_fees_paid;
                    let current_return_pct = if position.size_quote > Decimal::ZERO {
                        current_pnl / position.size_quote
                    } else {
                        Decimal::ZERO
                    };
                    let profile = self.strategy_profile(strategy);
                    let effective_max_hold_ms = if shred_exit_armed {
                        profile.max_hold_ms / 2
                    } else {
                        profile.max_hold_ms
                    };
                    let held_ms = (observed_at - position.entry_time)
                        .whole_milliseconds()
                        .max(0) as u64;
                    let trailing_floor = if position.size_quote > Decimal::ZERO {
                        (position.max_favorable_excursion / position.size_quote)
                            - profile.trailing_stop_pct
                    } else {
                        -profile.trailing_stop_pct
                    };
                    if shred_exit_triggered {
                        decision = TradeDecision::EmergencyExit;
                        diagnostics.push("exit_by=shred_emergency_exit".to_owned());
                        reason_codes.insert(ReasonCode::ShredEmergencyExit, ());
                    } else if shred_exit_armed {
                        decision = TradeDecision::Hold;
                        diagnostics.push("armed_by=shred_exit_armed".to_owned());
                        diagnostics.push(format!("effective_max_hold_ms={effective_max_hold_ms}"));
                        reason_codes.insert(ReasonCode::ShredExitArmed, ());
                    } else if risk.rug.score >= dec!(0.75)
                        || risk.fake_momentum.score >= dec!(0.75)
                        || risk.bundle.score >= dec!(0.80)
                    {
                        decision = TradeDecision::EmergencyExit;
                        diagnostics.push("exit_by=emergency_risk".to_owned());
                    } else if current_return_pct >= profile.take_profit_pct {
                        decision = TradeDecision::Exit;
                        diagnostics.push(format!(
                            "exit_by=take_profit value={} threshold={}",
                            current_return_pct, profile.take_profit_pct
                        ));
                    } else if current_return_pct <= -profile.stop_loss_pct {
                        decision = TradeDecision::Exit;
                        diagnostics.push(format!(
                            "exit_by=stop_loss value={} threshold=-{}",
                            current_return_pct, profile.stop_loss_pct
                        ));
                    } else if position.max_favorable_excursion > Decimal::ZERO
                        && current_return_pct <= trailing_floor
                    {
                        decision = TradeDecision::Exit;
                        diagnostics.push(format!(
                            "exit_by=trailing_stop value={} threshold={}",
                            current_return_pct, trailing_floor
                        ));
                    } else if held_ms >= effective_max_hold_ms {
                        decision = TradeDecision::Exit;
                        diagnostics.push(format!(
                            "exit_by=max_hold held_ms={} threshold={}",
                            held_ms, effective_max_hold_ms
                        ));
                    } else if self.exit_signal_for_strategy(strategy, features, risk) {
                        decision = TradeDecision::Exit;
                        diagnostics.push(format!("exit_by=strategy_decay strategy={strategy:?}"));
                    } else {
                        decision = TradeDecision::Hold;
                    }
                }
                (
                    _,
                    TokenLifecycle::ExitPending
                    | TokenLifecycle::Completed
                    | TokenLifecycle::SoftDiscarded
                    | TokenLifecycle::HardDiscarded
                    | TokenLifecycle::RugArchive
                    | TokenLifecycle::Migrated
                    | TokenLifecycle::DataGap,
                ) => {
                    decision = TradeDecision::StopTracking;
                }
                (_, _) => {
                    let enter_threshold = self.thresholds.min_trade_eligibility_score;
                    let watch_threshold = (enter_threshold / Decimal::from(2u64)).max(dec!(0.15));
                    let edge_ready = expected_edge.expected_net_edge_quote
                        > self.edge.min_expected_net_edge_quote
                        && expected_edge.expected_net_edge_pct
                            > self.edge.min_expected_net_edge_pct
                        && expected_edge.confidence >= self.edge.min_edge_confidence;
                    if shred_entry_block && !shred_low_confidence {
                        diagnostics.push(format!(
                            "blocked_by=early_intent warning_level={:?} confidence={}",
                            shred_warning_level,
                            token.shred_defense.preconfirmation_exit_confidence
                        ));
                        reason_codes.insert(ReasonCode::EarlyIntentSellWarning, ());
                        if token.shred_defense.active_seller_classification.is_some() {
                            reason_codes.insert(ReasonCode::ShredExitArmed, ());
                        }
                    }
                    decision = match strategy {
                        StrategyKind::LaunchMomentumScalp
                        | StrategyKind::HolderGrowthContinuation
                        | StrategyKind::SellAbsorptionBounce
                        | StrategyKind::SmartCapitalRotation
                        | StrategyKind::OrganicSlowGrind
                            if trade_eligibility >= enter_threshold
                                && breadth_ready
                                && risk.bundle.score < bundle_entry_limit
                                && !bundle_concentrated
                                && !fake_momentum_flagged
                                && (!shred_entry_block || shred_low_confidence)
                                && holder_stickiness
                                    >= self.thresholds.min_holder_stickiness_score
                                && authenticity
                                    >= self.thresholds.min_momentum_authenticity_score
                                && profit_overhang <= self.thresholds.max_profit_overhang_score
                                && edge_ready =>
                        {
                            if self.execution.paper_enabled {
                                TradeDecision::EnterPaper
                            } else {
                                TradeDecision::WatchDeep
                            }
                        }
                        StrategyKind::DefensiveNoTrade => TradeDecision::WatchLight,
                        _ if trade_eligibility >= watch_threshold
                            && self.edge.allow_watch_without_trade =>
                        {
                            TradeDecision::WatchDeep
                        }
                        _ => TradeDecision::WatchLight,
                    };
                    diagnostics.push(format!(
                        "eligibility={} enter_threshold={} watch_threshold={} breadth_ready={} bundle_score={} bundle_entry_limit={} bundle_concentrated={} fake_momentum_flagged={} holder_stickiness={} min_holder_stickiness={} authenticity={} min_authenticity={} profit_overhang={} max_profit_overhang={} expected_edge_quote={} min_edge_quote={} expected_edge_pct={} min_edge_pct={} edge_confidence={} min_edge_confidence={}",
                        trade_eligibility,
                        enter_threshold,
                        watch_threshold,
                        breadth_ready,
                        risk.bundle.score,
                        bundle_entry_limit,
                        bundle_concentrated,
                        fake_momentum_flagged,
                        holder_stickiness,
                        self.thresholds.min_holder_stickiness_score,
                        authenticity,
                        self.thresholds.min_momentum_authenticity_score,
                        profit_overhang,
                        self.thresholds.max_profit_overhang_score,
                        expected_edge.expected_net_edge_quote,
                        self.edge.min_expected_net_edge_quote,
                        expected_edge.expected_net_edge_pct,
                        self.edge.min_expected_net_edge_pct,
                        expected_edge.confidence,
                        self.edge.min_edge_confidence,
                    ));
                }
            }
        }

        if matches!(decision, TradeDecision::EnterPaper) {
            reason_codes.insert(ReasonCode::EnterPaper, ());
        }
        if matches!(decision, TradeDecision::Exit | TradeDecision::EmergencyExit) {
            reason_codes.insert(ReasonCode::ExitPaper, ());
        }
        if !self.execution.live_enabled {
            reason_codes.insert(ReasonCode::LiveDisabled, ());
        }

        for reason in &risk.reason_codes {
            reason_codes.insert(reason.clone(), ());
        }
        let position_size_quote = self.position_size_quote(trade_eligibility, token, risk);
        let score_vector = composite_scores
            .iter()
            .map(|(name, score)| (name.clone(), score.raw_score))
            .chain([
                (
                    "expected_net_edge_quote".to_owned(),
                    expected_edge.expected_net_edge_quote,
                ),
                (
                    "expected_net_edge_pct".to_owned(),
                    expected_edge.expected_net_edge_pct,
                ),
            ])
            .collect::<BTreeMap<_, _>>();
        let risk_vector = BTreeMap::from([
            ("rug".to_owned(), risk.rug.score),
            ("bundle".to_owned(), risk.bundle.score),
            ("dev".to_owned(), risk.dev.score),
            ("top_holder".to_owned(), risk.top_holder.score),
            ("fake_momentum".to_owned(), risk.fake_momentum.score),
            ("data_quality".to_owned(), risk.data_quality.score),
        ]);

        let decision_event = TradeDecisionEvent {
            decision_id: EventId::new_v7().0.to_string(),
            mint: token.mint.clone(),
            decision,
            strategy: format!("{strategy:?}"),
            feature_snapshot_hash: features.vector_hash.clone(),
            score_vector,
            risk_vector,
            expected_net_edge_quote: expected_edge.expected_net_edge_quote,
            expected_net_edge_pct: expected_edge.expected_net_edge_pct,
            expected_edge_confidence: expected_edge.confidence,
            reason_codes: reason_codes.into_keys().collect(),
            config_hash: config_hash.to_owned(),
            strategy_version: strategy_version.to_owned(),
            data_quality_score: features
                .decimal("data_quality_score")
                .unwrap_or(Decimal::ZERO),
            no_lookahead_timestamp: observed_at,
        };

        DecisionOutcome {
            decision_event,
            strategy,
            position_size_quote,
            expected_edge,
            composite_scores,
            diagnostics,
        }
    }

    fn composite_scores(
        &self,
        token: &TokenState,
        features: &FeatureSnapshot,
        risk: &RiskAssessment,
    ) -> BTreeMap<String, CompositeScore> {
        let momentum = score(
            weighted_average(&[
                (
                    features.decimal("return_pct_30s").unwrap_or(Decimal::ZERO),
                    dec!(0.45),
                ),
                (
                    features
                        .decimal("organic_flow_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.35),
                ),
                (
                    features
                        .decimal("token_relative_strength_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.20),
                ),
            ]),
            vec!["short-term return".to_owned(), "organic flow".to_owned()],
            vec!["drawdown".to_owned()],
            risk.fake_momentum.score / Decimal::from(2u64),
            vec![],
        );
        let holder_quality = score(
            weighted_average(&[
                (
                    features
                        .decimal("sustained_holder_growth_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.5),
                ),
                (
                    Decimal::ONE
                        - features
                            .decimal("concentration_risk_score")
                            .unwrap_or(Decimal::ZERO),
                    dec!(0.3),
                ),
                (
                    Decimal::ONE
                        - features
                            .decimal("holder_balance_hhi")
                            .unwrap_or(Decimal::ZERO),
                    dec!(0.2),
                ),
            ]),
            vec!["holder growth".to_owned()],
            vec!["concentration".to_owned()],
            risk.top_holder.score / Decimal::from(2u64),
            vec![],
        );
        let fee_adjusted_edge = self.fee_adjusted_edge_score(token, features, risk);
        let liquidity = score(
            features
                .decimal("quote_asset_specific_depth_score")
                .unwrap_or(Decimal::ZERO),
            vec!["curve depth".to_owned()],
            vec![],
            Decimal::ZERO,
            vec![],
        );
        let latency = score(
            features
                .decimal("latency_adjusted_trade_eligibility")
                .unwrap_or(Decimal::ZERO),
            vec!["precomputed latency viability".to_owned()],
            vec![],
            Decimal::ZERO,
            vec![],
        );
        let relative_strength = score(
            features
                .decimal("token_relative_strength_score")
                .unwrap_or(Decimal::ZERO),
            vec!["cohort rank".to_owned()],
            vec![],
            Decimal::ZERO,
            vec![],
        );
        let holder_stickiness = score(
            features
                .decimal("holder_stickiness_score")
                .unwrap_or(Decimal::ZERO),
            vec!["holder stickiness".to_owned()],
            vec!["paper hands".to_owned()],
            Decimal::ZERO,
            vec![],
        );
        let absorption = score(
            features
                .decimal("absorption_success_score")
                .unwrap_or(Decimal::ZERO),
            vec!["sell absorption".to_owned()],
            vec!["failed absorption".to_owned()],
            Decimal::ZERO,
            vec![],
        );
        let capital_rotation = score(
            features
                .decimal("smart_capital_rotation_score")
                .unwrap_or(Decimal::ZERO),
            vec!["capital rotation".to_owned()],
            vec!["rotation decay".to_owned()],
            Decimal::ZERO,
            vec![],
        );
        let profit_overhang = score(
            Decimal::ONE
                - features
                    .decimal("profit_overhang_score")
                    .unwrap_or(Decimal::ZERO),
            vec!["low profit overhang".to_owned()],
            vec!["profit pressure".to_owned()],
            risk.top_holder.score / Decimal::from(3u64),
            vec![ReasonCode::ProfitOverhangTooHigh],
        );
        let momentum_authenticity = score(
            features
                .decimal("momentum_authenticity_score")
                .unwrap_or(Decimal::ZERO),
            vec!["authentic momentum".to_owned()],
            vec!["fake momentum".to_owned()],
            risk.fake_momentum.score / Decimal::from(3u64),
            vec![],
        );
        let anti_rug = score(
            weighted_average(&[
                (
                    features
                        .decimal("organic_survival_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.7),
                ),
                (Decimal::ONE - risk.rug.score, dec!(0.3)),
            ]),
            vec!["survival score".to_owned()],
            vec!["rug risk".to_owned()],
            risk.rug.score / Decimal::from(2u64),
            vec![],
        );
        let trade_eligibility = score(
            weighted_average(&[
                (momentum.regime_adjusted_score, dec!(0.20)),
                (holder_quality.regime_adjusted_score, dec!(0.20)),
                (fee_adjusted_edge.regime_adjusted_score, dec!(0.20)),
                (liquidity.regime_adjusted_score, dec!(0.10)),
                (latency.regime_adjusted_score, dec!(0.05)),
                (relative_strength.regime_adjusted_score, dec!(0.10)),
                (holder_stickiness.regime_adjusted_score, dec!(0.10)),
                (absorption.regime_adjusted_score, dec!(0.05)),
                (capital_rotation.regime_adjusted_score, dec!(0.05)),
                (profit_overhang.regime_adjusted_score, dec!(0.05)),
                (momentum_authenticity.regime_adjusted_score, dec!(0.05)),
                (anti_rug.regime_adjusted_score, dec!(0.05)),
            ]),
            vec!["multi-factor eligibility".to_owned()],
            vec!["risk penalties".to_owned()],
            weighted_average(&[
                (risk.rug.score, dec!(0.35)),
                (risk.bundle.score, dec!(0.20)),
                (risk.top_holder.score, dec!(0.15)),
                (risk.fake_momentum.score, dec!(0.15)),
                (risk.data_quality.score, dec!(0.15)),
            ]),
            vec![ReasonCode::FeeAdjustedEdgeTooLow],
        );

        BTreeMap::from([
            ("MomentumScore".to_owned(), momentum),
            ("HolderQualityScore".to_owned(), holder_quality),
            ("FeeAdjustedEdgeScore".to_owned(), fee_adjusted_edge),
            ("LiquidityDepthScore".to_owned(), liquidity),
            ("LatencyViabilityScore".to_owned(), latency),
            ("RelativeStrengthScore".to_owned(), relative_strength),
            ("HolderStickinessScore".to_owned(), holder_stickiness),
            ("AbsorptionStrengthScore".to_owned(), absorption),
            ("SmartCapitalRotationScore".to_owned(), capital_rotation),
            ("ProfitOverhangRiskScore".to_owned(), profit_overhang),
            (
                "MomentumAuthenticityScore".to_owned(),
                momentum_authenticity,
            ),
            ("AntiRugSurvivalScore".to_owned(), anti_rug),
            ("TradeEligibilityScore".to_owned(), trade_eligibility),
        ])
    }

    fn fee_adjusted_edge_score(
        &self,
        token: &TokenState,
        features: &FeatureSnapshot,
        risk: &RiskAssessment,
    ) -> CompositeScore {
        let reference_price = token.latest_price.max(dec!(0.000000001));
        let size_quote = self.execution.max_daily_loss_quote / Decimal::from(20u64);
        let depth =
            token.reserve_state.real_quote_reserves + token.reserve_state.virtual_quote_reserves;
        let intent = PositionIntent {
            mint: token.mint.clone(),
            entry_time: token.launch_time.unwrap_or(OffsetDateTime::UNIX_EPOCH),
            exit_time: token
                .trade_stats
                .price_history
                .back()
                .map(|(time, _)| *time)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
            size_quote,
            entry_price: reference_price,
            exit_price: reference_price
                * (Decimal::ONE
                    + features
                        .decimal("return_pct_30s")
                        .unwrap_or(Decimal::ZERO)
                        .max(Decimal::ZERO)),
            liquidity_quote_depth: if depth > Decimal::ZERO {
                depth
            } else {
                Decimal::from(100u64)
            },
        };
        let (_, _, net) = self.simulator.simulate_round_trip(&intent);
        let normalized = if size_quote > Decimal::ZERO {
            clamp01((net / size_quote + Decimal::ONE) / Decimal::from(2u64))
        } else {
            Decimal::ZERO
        };
        score(
            normalized,
            vec!["round-trip after fees".to_owned()],
            vec!["fee drag and impact".to_owned()],
            risk.bundle.score / Decimal::from(3u64),
            vec![ReasonCode::FeeAdjustedEdgeTooLow],
        )
    }

    fn position_size_quote(
        &self,
        eligibility: Decimal,
        token: &TokenState,
        risk: &RiskAssessment,
    ) -> Decimal {
        let base = self.execution.max_daily_loss_quote / Decimal::from(10u64);
        let depth_proxy = {
            let reserves = token.reserve_state.real_quote_reserves
                + token.reserve_state.virtual_quote_reserves;
            if reserves > Decimal::ZERO {
                reserves
            } else {
                token.trade_stats.buy_volume_quote
                    + token.trade_stats.sell_volume_quote
                    + Decimal::from(25u64)
            }
        };
        let liquidity_factor = clamp01(depth_proxy / Decimal::from(500u64));
        let risk_factor = clamp01(
            Decimal::ONE
                - weighted_average(&[
                    (risk.rug.score, dec!(0.4)),
                    (risk.bundle.score, dec!(0.2)),
                    (risk.top_holder.score, dec!(0.2)),
                    (risk.fake_momentum.score, dec!(0.2)),
                ]),
        );
        (base * eligibility * liquidity_factor * risk_factor)
            .min(self.execution.max_daily_loss_quote / Decimal::from(2u64))
            .min(self.execution.max_position_size_quote.max(Decimal::ZERO))
    }

    fn expected_executable_edge(
        &self,
        token: &TokenState,
        features: &FeatureSnapshot,
        risk: &RiskAssessment,
        strategy: StrategyKind,
    ) -> ExpectedExecutableEdge {
        let profile = self.strategy_profile(strategy);
        let qualitative = match strategy {
            StrategyKind::LaunchMomentumScalp => weighted_average(&[
                (
                    features
                        .decimal("return_pct_30s")
                        .unwrap_or(Decimal::ZERO)
                        .max(Decimal::ZERO),
                    dec!(0.25),
                ),
                (
                    features
                        .decimal("token_relative_strength_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.20),
                ),
                (
                    features
                        .decimal("momentum_authenticity_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.30),
                ),
                (
                    features
                        .decimal("organic_flow_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.25),
                ),
            ]),
            StrategyKind::HolderGrowthContinuation => weighted_average(&[
                (
                    features
                        .decimal("sustained_holder_growth_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.30),
                ),
                (
                    features
                        .decimal("holder_stickiness_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.30),
                ),
                (
                    features
                        .decimal("token_relative_strength_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.20),
                ),
                (
                    features
                        .decimal("organic_survival_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.20),
                ),
            ]),
            StrategyKind::SellAbsorptionBounce => weighted_average(&[
                (
                    features
                        .decimal("absorption_success_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.40),
                ),
                (
                    features
                        .decimal("momentum_authenticity_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.25),
                ),
                (
                    features
                        .decimal("holder_stickiness_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.20),
                ),
                (
                    features
                        .decimal("organic_flow_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.15),
                ),
            ]),
            StrategyKind::SmartCapitalRotation => weighted_average(&[
                (
                    features
                        .decimal("smart_capital_rotation_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.35),
                ),
                (
                    features
                        .decimal("token_relative_strength_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.25),
                ),
                (
                    features
                        .decimal("holder_stickiness_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.20),
                ),
                (
                    features
                        .decimal("momentum_authenticity_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.20),
                ),
            ]),
            StrategyKind::OrganicSlowGrind => weighted_average(&[
                (
                    features
                        .decimal("organic_survival_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.35),
                ),
                (
                    features
                        .decimal("holder_stickiness_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.25),
                ),
                (
                    features
                        .decimal("sustained_holder_growth_score")
                        .unwrap_or(Decimal::ZERO),
                    dec!(0.20),
                ),
                (Decimal::ONE - risk.bundle.score, dec!(0.20)),
            ]),
            StrategyKind::DefensiveNoTrade => Decimal::ZERO,
        };
        let risk_penalty = weighted_average(&[
            (risk.rug.score, dec!(0.30)),
            (risk.bundle.score, dec!(0.15)),
            (risk.top_holder.score, dec!(0.15)),
            (risk.fake_momentum.score, dec!(0.20)),
            (
                features
                    .decimal("profit_overhang_score")
                    .unwrap_or(Decimal::ZERO),
                dec!(0.10),
            ),
            (
                features.decimal("fee_war_score").unwrap_or(Decimal::ZERO),
                dec!(0.10),
            ),
        ]);
        let confidence = clamp01(weighted_average(&[
            (qualitative, dec!(0.60)),
            (Decimal::ONE - risk_penalty, dec!(0.20)),
            (
                features.decimal("data_quality_score").unwrap_or(dec!(0.50)),
                dec!(0.20),
            ),
        ]));
        let expected_price_move_pct = clamp01(
            profile.take_profit_pct
                * clamp01(qualitative)
                * clamp01(Decimal::ONE - risk_penalty / Decimal::from(2u64)),
        );
        let reference_price = token.latest_price.max(dec!(0.000000001));
        let expected_exit_price = reference_price * (Decimal::ONE + expected_price_move_pct);
        let size_quote = self
            .execution
            .max_position_size_quote
            .min(self.execution.max_daily_loss_quote / Decimal::from(10u64))
            .max(dec!(0.25));
        let depth = {
            let reserve_depth = token.reserve_state.real_quote_reserves
                + token.reserve_state.virtual_quote_reserves;
            if reserve_depth > Decimal::ZERO {
                reserve_depth
            } else {
                token.trade_stats.buy_volume_quote
                    + token.trade_stats.sell_volume_quote
                    + Decimal::from(50u64)
            }
        };
        let (buy, sell, simulator_net) = self.simulator.simulate_round_trip(&sim::PositionIntent {
            mint: token.mint.clone(),
            entry_time: token.launch_time.unwrap_or(OffsetDateTime::UNIX_EPOCH),
            exit_time: token
                .trade_stats
                .price_history
                .back()
                .map(|(time, _)| *time)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH)
                + time::Duration::milliseconds(profile.max_hold_ms as i64),
            size_quote,
            entry_price: reference_price,
            exit_price: expected_exit_price,
            liquidity_quote_depth: depth,
        });
        let fee_drag_quote = buy.fees.total_quote + sell.fees.total_quote;
        let slippage_drag_quote = size_quote * (buy.fill.slippage + sell.fill.slippage);
        let curve_impact_quote = size_quote * (buy.fill.price_impact + sell.fill.price_impact);
        let latency_drag_quote = size_quote
            * (Decimal::ONE
                - features
                    .decimal("latency_adjusted_trade_eligibility")
                    .unwrap_or(dec!(0.50)))
            * dec!(0.10)
            * self.edge.latency_safety_margin_multiplier;
        let expected_failed_tx_cost_quote = buy.fees.failed_transaction_fee_quote
            * weighted_average(&[
                (
                    features.decimal("fee_war_score").unwrap_or(Decimal::ZERO),
                    dec!(0.60),
                ),
                (risk.data_quality.score, dec!(0.40)),
            ]);
        let incremental_cost_buffer = (fee_drag_quote + slippage_drag_quote + curve_impact_quote)
            * (self.edge.fee_safety_margin_multiplier - Decimal::ONE).max(Decimal::ZERO);
        let minimum_required_move_pct = if size_quote > Decimal::ZERO {
            (fee_drag_quote
                + slippage_drag_quote
                + curve_impact_quote
                + latency_drag_quote
                + expected_failed_tx_cost_quote)
                / size_quote
        } else {
            Decimal::ONE
        };
        let expected_net_edge_quote = simulator_net
            - incremental_cost_buffer
            - latency_drag_quote
            - expected_failed_tx_cost_quote;
        let expected_net_edge_pct = if size_quote > Decimal::ZERO {
            expected_net_edge_quote / size_quote
        } else {
            Decimal::ZERO
        };
        let mut reason_codes = Vec::new();
        if expected_net_edge_quote <= self.edge.min_expected_net_edge_quote {
            reason_codes.push(ReasonCode::FeeAdjustedEdgeTooLow);
        }
        if expected_net_edge_pct <= self.edge.min_expected_net_edge_pct {
            reason_codes.push(ReasonCode::FeeAdjustedEdgeTooLow);
        }
        if features.decimal("fee_war_score").unwrap_or(Decimal::ZERO) >= dec!(0.60) {
            reason_codes.push(ReasonCode::FeeWarTooExpensive);
        }
        ExpectedExecutableEdge {
            expected_price_move_pct,
            expected_exit_price,
            expected_hold_time_ms: profile.max_hold_ms,
            expected_fee_drag_quote: fee_drag_quote,
            expected_slippage_drag_quote: slippage_drag_quote,
            expected_latency_drag_quote: latency_drag_quote,
            expected_curve_impact_quote: curve_impact_quote,
            expected_exit_impact_quote: curve_impact_quote / Decimal::from(2u64),
            expected_failed_tx_cost_quote,
            minimum_required_move_pct,
            expected_net_edge_quote,
            expected_net_edge_pct,
            confidence,
            reason_codes,
        }
    }

    fn strategy_profile(&self, strategy: StrategyKind) -> &StrategyProfileConfig {
        match strategy {
            StrategyKind::LaunchMomentumScalp => &self.thresholds.launch_momentum,
            StrategyKind::HolderGrowthContinuation => &self.thresholds.holder_growth,
            StrategyKind::SellAbsorptionBounce => &self.thresholds.absorption_bounce,
            StrategyKind::SmartCapitalRotation => &self.thresholds.smart_rotation,
            StrategyKind::OrganicSlowGrind | StrategyKind::DefensiveNoTrade => {
                &self.thresholds.organic_slow_grind
            }
        }
    }

    fn exit_signal_for_strategy(
        &self,
        strategy: StrategyKind,
        features: &FeatureSnapshot,
        risk: &RiskAssessment,
    ) -> bool {
        match strategy {
            StrategyKind::LaunchMomentumScalp => {
                features
                    .decimal("momentum_authenticity_score")
                    .unwrap_or(Decimal::ZERO)
                    < dec!(0.15)
                    || features.decimal("return_pct_30s").unwrap_or(Decimal::ZERO) < Decimal::ZERO
            }
            StrategyKind::HolderGrowthContinuation => {
                features
                    .decimal("holder_stickiness_score")
                    .unwrap_or(Decimal::ZERO)
                    < self.thresholds.min_holder_stickiness_score
                    || features
                        .decimal("profit_overhang_score")
                        .unwrap_or(Decimal::ZERO)
                        > self.thresholds.max_profit_overhang_score
                    || risk.top_holder.score > dec!(0.55)
            }
            StrategyKind::SellAbsorptionBounce => {
                features
                    .decimal("absorption_success_score")
                    .unwrap_or(Decimal::ZERO)
                    < dec!(0.12)
                    || risk.fake_momentum.score > dec!(0.55)
            }
            StrategyKind::SmartCapitalRotation => {
                features
                    .decimal("smart_capital_rotation_score")
                    .unwrap_or(Decimal::ZERO)
                    < dec!(0.10)
                    || features
                        .decimal("token_relative_strength_score")
                        .unwrap_or(Decimal::ZERO)
                        < dec!(0.15)
            }
            StrategyKind::OrganicSlowGrind => {
                features
                    .decimal("organic_survival_score")
                    .unwrap_or(Decimal::ZERO)
                    < dec!(0.25)
                    || risk.bundle.score > dec!(0.60)
            }
            StrategyKind::DefensiveNoTrade => true,
        }
    }
}

fn score(
    raw_score: Decimal,
    top_positive_components: Vec<String>,
    top_negative_components: Vec<String>,
    missing_penalty: Decimal,
    reason_codes: Vec<ReasonCode>,
) -> CompositeScore {
    let regime_adjusted_score = clamp01(raw_score - missing_penalty / Decimal::from(2u64));
    CompositeScore {
        raw_score: clamp01(raw_score),
        regime_adjusted_score,
        confidence: clamp01(Decimal::ONE - missing_penalty),
        missing_data_penalty: clamp01(missing_penalty),
        top_positive_components,
        top_negative_components,
        reason_codes,
        recommended_action_influence: regime_adjusted_score,
    }
}

fn select_strategy(
    features: &FeatureSnapshot,
    risk: &RiskAssessment,
    scores: &BTreeMap<String, CompositeScore>,
) -> StrategyKind {
    let momentum = scores
        .get("MomentumScore")
        .map(|score| score.regime_adjusted_score)
        .unwrap_or(Decimal::ZERO);
    let holder_quality = scores
        .get("HolderQualityScore")
        .map(|score| score.regime_adjusted_score)
        .unwrap_or(Decimal::ZERO);
    let rotation = features
        .decimal("smart_capital_rotation_score")
        .unwrap_or(Decimal::ZERO);
    let absorption = features
        .decimal("absorption_success_score")
        .unwrap_or(Decimal::ZERO);
    let organic = features
        .decimal("organic_survival_score")
        .unwrap_or(Decimal::ZERO);
    let stickiness = features
        .decimal("holder_stickiness_score")
        .unwrap_or(Decimal::ZERO);
    let authenticity = features
        .decimal("momentum_authenticity_score")
        .unwrap_or(Decimal::ZERO);
    let relative_strength = features
        .decimal("token_relative_strength_score")
        .unwrap_or(Decimal::ZERO);
    let fake_flagged = risk
        .reason_codes
        .contains(&ReasonCode::HighFakeMomentumRisk)
        || risk
            .fake_momentum
            .reason_codes
            .contains(&ReasonCode::HighFakeMomentumRisk);

    if risk.rug.score >= dec!(0.7) || risk.data_quality.score >= dec!(0.75) {
        StrategyKind::DefensiveNoTrade
    } else if holder_quality >= dec!(0.36)
        && stickiness >= dec!(0.22)
        && organic >= dec!(0.30)
        && relative_strength >= dec!(0.45)
        && risk.fake_momentum.score < dec!(0.30)
        && !fake_flagged
    {
        StrategyKind::HolderGrowthContinuation
    } else if momentum >= dec!(0.48)
        && authenticity >= dec!(0.45)
        && risk.fake_momentum.score < dec!(0.30)
        && !fake_flagged
    {
        StrategyKind::LaunchMomentumScalp
    } else if absorption >= dec!(0.22)
        && stickiness >= dec!(0.15)
        && risk.bundle.score < dec!(0.65)
        && risk.top_holder.score < dec!(0.75)
        && risk.fake_momentum.score < dec!(0.60)
    {
        StrategyKind::SellAbsorptionBounce
    } else if rotation >= dec!(0.12) && momentum >= dec!(0.15) {
        StrategyKind::SmartCapitalRotation
    } else if organic >= dec!(0.30)
        && holder_quality >= dec!(0.25)
        && risk.bundle.score < dec!(0.60)
    {
        StrategyKind::OrganicSlowGrind
    } else {
        StrategyKind::DefensiveNoTrade
    }
}

fn terminal_decision(token: &TokenState) -> TradeDecision {
    if token.lifecycle == TokenLifecycle::InPosition {
        TradeDecision::EmergencyExit
    } else {
        TradeDecision::StopTracking
    }
}

fn terminal_or_emergency(token: &TokenState) -> TradeDecision {
    if token.lifecycle == TokenLifecycle::InPosition {
        TradeDecision::EmergencyExit
    } else {
        TradeDecision::StopTracking
    }
}

fn weighted_average(values: &[(Decimal, Decimal)]) -> Decimal {
    let mut weighted_sum = Decimal::ZERO;
    let mut total_weight = Decimal::ZERO;
    for (value, weight) in values {
        weighted_sum += *value * *weight;
        total_weight += *weight;
    }
    if total_weight > Decimal::ZERO {
        weighted_sum / total_weight
    } else {
        Decimal::ZERO
    }
}

fn clamp01(value: Decimal) -> Decimal {
    value.max(Decimal::ZERO).min(Decimal::ONE)
}

#[cfg(test)]
mod tests {
    use common::{
        Canonicality, EventMeta, EventPayload, EventSource, HolderBalanceUpdateEvent,
        NormalizedEvent, PubkeyValue, PumpBuyEvent, QuoteAssetType, TokenCreatedEvent,
        TokenProgramType, TransactionStatus, TtlConfig,
    };
    use features::FeatureEngine;
    use risk::RiskEngine;
    use sim::FeeModel;
    use state::StateEngine;
    use time::Duration;

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
                initial_virtual_quote_reserves: Some(Decimal::from(200u64)),
                initial_virtual_token_reserves: Some(Decimal::from(1000u64)),
                initial_real_quote_reserves: Some(Decimal::from(200u64)),
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
                is_creator: buyer == "creator",
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

    fn setup() -> (TokenState, FeatureSnapshot, RiskAssessment, DecisionEngine) {
        let mut engine = StateEngine::new(ttl());
        engine.apply_event(&token_created()).expect("create");
        engine
            .apply_event(&buy(2, "buyer-a", 20, 100))
            .expect("buy");
        engine
            .apply_event(&buy(3, "buyer-b", 25, 100))
            .expect("buy");
        engine
            .apply_event(&buy(4, "buyer-c", 30, 100))
            .expect("buy");
        engine
            .apply_event(&holder(4, "buyer-a", 100))
            .expect("holder");
        engine
            .apply_event(&holder(4, "buyer-b", 100))
            .expect("holder");
        engine
            .apply_event(&holder(4, "buyer-c", 100))
            .expect("holder");
        let snapshot = engine.snapshot();
        let token = snapshot.tokens.get("mint").expect("token").clone();
        let features = FeatureEngine::default().compute_snapshot(
            &token,
            &snapshot,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(20),
        );
        let risk = RiskEngine::default().evaluate(
            &token,
            &features,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(20),
        );
        let decision_engine = DecisionEngine::new(
            StrategyThresholds {
                min_fee_adjusted_edge_bps: 10,
                min_data_quality_score: dec!(0.4),
                max_bundle_risk: dec!(0.9),
                max_rug_risk: dec!(0.9),
                max_fake_momentum_risk: dec!(0.9),
                min_holder_growth: Decimal::ZERO,
                min_trade_eligibility_score: dec!(0.5),
                min_holder_stickiness_score: dec!(0.2),
                min_momentum_authenticity_score: dec!(0.2),
                max_profit_overhang_score: dec!(0.9),
                ..Default::default()
            },
            EdgeConfig {
                min_expected_net_edge_quote: Decimal::ZERO,
                min_expected_net_edge_pct: Decimal::ZERO,
                min_edge_confidence: Decimal::ZERO,
                fee_safety_margin_multiplier: Decimal::ONE,
                latency_safety_margin_multiplier: Decimal::ONE,
                allow_watch_without_trade: true,
            },
            ExecutionConfig {
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
            },
            Simulator::new(FeeModel::default()),
        );
        (token, features, risk, decision_engine)
    }

    #[test]
    fn hard_filters_block_kill_switch_and_bad_data() {
        let (token, features, risk, mut engine) = setup();
        engine.set_kill_switch(true);
        let outcome = engine.evaluate(
            &token,
            &features,
            &risk,
            None,
            "config",
            "strategy",
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(20),
        );
        assert_eq!(outcome.decision_event.decision, TradeDecision::StopTracking);
        assert!(
            outcome
                .decision_event
                .reason_codes
                .contains(&ReasonCode::KillSwitchActive)
        );
    }

    #[test]
    fn good_token_can_enter_paper() {
        let (token, features, risk, engine) = setup();
        let outcome = engine.evaluate(
            &token,
            &features,
            &risk,
            None,
            "config",
            "strategy",
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(20),
        );
        assert!(matches!(
            outcome.decision_event.decision,
            TradeDecision::EnterPaper | TradeDecision::WatchDeep | TradeDecision::WatchLight
        ));
        assert!(outcome.position_size_quote >= Decimal::ZERO);
    }

    #[test]
    fn position_size_respects_caps_and_is_deterministic() {
        let (token, features, risk, engine) = setup();
        let first = engine.evaluate(
            &token,
            &features,
            &risk,
            None,
            "config",
            "strategy",
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(20),
        );
        let second = engine.evaluate(
            &token,
            &features,
            &risk,
            None,
            "config",
            "strategy",
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(20),
        );
        assert_eq!(first.position_size_quote, second.position_size_quote);
        assert!(first.position_size_quote <= Decimal::from(50u64));
    }

    #[test]
    fn exit_logic_fires_when_in_position_and_risk_spikes() {
        let (mut token, features, mut risk, engine) = setup();
        token.lifecycle = TokenLifecycle::InPosition;
        risk.rug.score = dec!(0.95);
        let position = OpenPositionContext {
            strategy: StrategyKind::LaunchMomentumScalp,
            entry_time: OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
            entry_price: token.latest_price,
            size_quote: Decimal::from(10u64),
            size_tokens: Decimal::from(50u64),
            entry_fees_paid: Decimal::ZERO,
            max_adverse_excursion: Decimal::ZERO,
            max_favorable_excursion: dec!(0.30),
            expected_edge_quote: Decimal::ONE,
        };
        let outcome = engine.evaluate(
            &token,
            &features,
            &risk,
            Some(&position),
            "config",
            "strategy",
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(20),
        );
        assert_eq!(
            outcome.decision_event.decision,
            TradeDecision::EmergencyExit
        );
    }
}
