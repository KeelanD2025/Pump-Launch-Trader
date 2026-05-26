use std::collections::BTreeMap;

use common::ReasonCode;
use features::FeatureSnapshot;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use state::TokenState;
use time::OffsetDateTime;

macro_rules! dec {
    ($value:literal) => {
        Decimal::from_str_exact(stringify!($value)).expect("decimal literal")
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleAction {
    KeepWatching,
    EscalateDeepTracking,
    SoftDiscard,
    HardDiscard,
    RugArchive,
    DataGapStop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscardPolicyDecision {
    Keep,
    SoftDiscard,
    HardDiscard,
    RugArchive,
    ResearchSample,
    DataGapStop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskScore {
    pub name: String,
    pub score: Decimal,
    pub confidence: Decimal,
    pub severity: RiskSeverity,
    pub reason_codes: Vec<ReasonCode>,
    pub positive_evidence: Vec<String>,
    pub negative_evidence: Vec<String>,
    pub missing_data_penalty: Decimal,
    pub recommended_lifecycle_action: LifecycleAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskAssessment {
    pub mint: String,
    pub observed_at: OffsetDateTime,
    pub rug: RiskScore,
    pub bundle: RiskScore,
    pub dev: RiskScore,
    pub top_holder: RiskScore,
    pub fake_momentum: RiskScore,
    pub data_quality: RiskScore,
    pub discard_policy: DiscardPolicyDecision,
    pub overall_score: Decimal,
    pub overall_confidence: Decimal,
    pub reason_codes: Vec<ReasonCode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactRiskSummary {
    pub mint: String,
    pub discard_policy: DiscardPolicyDecision,
    pub overall_score: Decimal,
    pub reason_codes: Vec<ReasonCode>,
}

#[derive(Debug, Clone)]
pub struct RiskEngine {
    hard_discard_rug_threshold: Decimal,
    soft_discard_bundle_threshold: Decimal,
    top_holder_threshold: Decimal,
    fake_momentum_threshold: Decimal,
    min_data_quality: Decimal,
}

impl Default for RiskEngine {
    fn default() -> Self {
        Self {
            hard_discard_rug_threshold: dec!(0.85),
            soft_discard_bundle_threshold: dec!(0.75),
            top_holder_threshold: dec!(0.75),
            fake_momentum_threshold: dec!(0.70),
            min_data_quality: dec!(0.50),
        }
    }
}

impl RiskEngine {
    pub fn evaluate(
        &self,
        token: &TokenState,
        features: &FeatureSnapshot,
        observed_at: OffsetDateTime,
    ) -> RiskAssessment {
        let rug = self.rug_risk(token, features);
        let bundle = self.bundle_risk(token, features);
        let dev = self.dev_risk(token, features);
        let top_holder = self.top_holder_risk(token, features);
        let fake_momentum = self.fake_momentum_risk(token, features);
        let data_quality = self.data_quality_risk(token, features);

        let overall_score = clamp01(weighted_average(&[
            (rug.score, dec!(0.30)),
            (bundle.score, dec!(0.15)),
            (dev.score, dec!(0.15)),
            (top_holder.score, dec!(0.15)),
            (fake_momentum.score, dec!(0.15)),
            (data_quality.score, dec!(0.10)),
        ]));
        let overall_confidence = weighted_average(&[
            (rug.confidence, dec!(0.30)),
            (bundle.confidence, dec!(0.15)),
            (dev.confidence, dec!(0.15)),
            (top_holder.confidence, dec!(0.15)),
            (fake_momentum.confidence, dec!(0.15)),
            (data_quality.confidence, dec!(0.10)),
        ]);

        let discard_policy = self.discard_policy(
            &rug,
            &bundle,
            &dev,
            &top_holder,
            &fake_momentum,
            &data_quality,
        );
        let mut reason_codes = BTreeMap::<ReasonCode, ()>::new();
        for score in [
            &rug,
            &bundle,
            &dev,
            &top_holder,
            &fake_momentum,
            &data_quality,
        ] {
            for reason in &score.reason_codes {
                reason_codes.insert(reason.clone(), ());
            }
        }
        match discard_policy {
            DiscardPolicyDecision::SoftDiscard => {
                reason_codes.insert(ReasonCode::SoftDiscarded, ());
            }
            DiscardPolicyDecision::HardDiscard => {
                reason_codes.insert(ReasonCode::HardDiscarded, ());
            }
            DiscardPolicyDecision::DataGapStop => {
                reason_codes.insert(ReasonCode::DataGapActive, ());
            }
            DiscardPolicyDecision::RugArchive
            | DiscardPolicyDecision::Keep
            | DiscardPolicyDecision::ResearchSample => {}
        }

        RiskAssessment {
            mint: token.mint.0.clone(),
            observed_at,
            rug,
            bundle,
            dev,
            top_holder,
            fake_momentum,
            data_quality,
            discard_policy,
            overall_score,
            overall_confidence,
            reason_codes: reason_codes.into_keys().collect(),
        }
    }

    pub fn compact_summary(&self, assessment: &RiskAssessment) -> CompactRiskSummary {
        CompactRiskSummary {
            mint: assessment.mint.clone(),
            discard_policy: assessment.discard_policy,
            overall_score: assessment.overall_score,
            reason_codes: assessment.reason_codes.clone(),
        }
    }

    fn discard_policy(
        &self,
        rug: &RiskScore,
        bundle: &RiskScore,
        dev: &RiskScore,
        top_holder: &RiskScore,
        fake_momentum: &RiskScore,
        data_quality: &RiskScore,
    ) -> DiscardPolicyDecision {
        if data_quality.recommended_lifecycle_action == LifecycleAction::DataGapStop {
            return DiscardPolicyDecision::DataGapStop;
        }
        if rug.score >= self.hard_discard_rug_threshold
            || rug.reason_codes.contains(&ReasonCode::PriceCollapse70)
        {
            return if dev.score >= dec!(0.6) {
                DiscardPolicyDecision::RugArchive
            } else {
                DiscardPolicyDecision::HardDiscard
            };
        }
        if dev.reason_codes.contains(&ReasonCode::DevSoldEarly)
            && (top_holder.score >= dec!(0.30)
                || fake_momentum.score >= dec!(0.40)
                || rug.score >= dec!(0.25))
        {
            return DiscardPolicyDecision::SoftDiscard;
        }
        if bundle.score >= self.soft_discard_bundle_threshold
            || top_holder.score >= self.top_holder_threshold
            || fake_momentum.score >= self.fake_momentum_threshold
        {
            return DiscardPolicyDecision::SoftDiscard;
        }
        if rug.score >= dec!(0.60) || dev.score >= dec!(0.65) {
            return DiscardPolicyDecision::ResearchSample;
        }
        DiscardPolicyDecision::Keep
    }

    fn rug_risk(&self, _token: &TokenState, features: &FeatureSnapshot) -> RiskScore {
        let mut positive = Vec::new();
        let mut negative = Vec::new();
        let mut reasons = Vec::new();
        let dev_sold_flag = bool_decimal(features, "dev_sold_flag");
        let dev_sold_pct = decimal(features, "dev_sold_pct");
        let launch_collapse = bool_decimal(features, "price_drop_70pct_from_launch_within_5m");
        let ath_collapse =
            clamp01((decimal(features, "drawdown_from_ath_pct") - dec!(0.70)) / dec!(0.30));
        let no_buys = bool_decimal(features, "no_buys_for_x_seconds");
        let holder_stalled = bool_decimal(features, "holder_growth_stalled");
        let sell_dominance = bool_decimal(features, "sells_dominate_after_launch");
        let concentration = decimal(features, "concentration_risk_score");
        let data_quality_penalty = Decimal::ONE - decimal(features, "data_quality_score");
        let shred_warning_level = decimal(features, "shred_sell_warning_level");
        let shred_exit_triggered = bool_decimal(features, "shred_emergency_exit_triggered_flag");
        let malicious_sell_intent = decimal(features, "malicious_sell_intent_score");

        if dev_sold_pct > Decimal::ZERO || dev_sold_flag > Decimal::ZERO {
            negative.push(format!("developer sold {:.4}", dev_sold_pct));
            reasons.push(ReasonCode::DevSoldEarly);
        } else {
            positive.push("developer has not sold".to_owned());
        }
        if launch_collapse > Decimal::ZERO || ath_collapse >= dec!(0.5) {
            negative.push("price dropped more than 70% from launch inside first 5m".to_owned());
            reasons.push(ReasonCode::PriceCollapse70);
        }
        if no_buys > Decimal::ZERO {
            negative.push("buy flow disappeared after launch".to_owned());
            reasons.push(ReasonCode::NoBuyGap);
        }
        if shred_warning_level >= dec!(3) || shred_exit_triggered > Decimal::ZERO {
            negative.push("preconfirmation malicious sell pressure is elevated".to_owned());
            reasons.push(ReasonCode::ShredSellImpactHigh);
        }

        let score = clamp01(weighted_average(&[
            (decimal(features, "rug_probability_score"), dec!(0.35)),
            (dev_sold_pct.max(dev_sold_flag), dec!(0.20)),
            (launch_collapse.max(ath_collapse), dec!(0.20)),
            (holder_stalled, dec!(0.08)),
            (sell_dominance, dec!(0.07)),
            (concentration, dec!(0.05)),
            (data_quality_penalty, dec!(0.05)),
            (malicious_sell_intent, dec!(0.08)),
            (
                shred_exit_triggered.max(shred_warning_level / dec!(4.0)),
                dec!(0.10),
            ),
        ]));
        if score >= dec!(0.7) {
            reasons.push(ReasonCode::HighRugRisk);
        }

        RiskScore {
            name: "rug".to_owned(),
            score,
            confidence: decimal(features, "rug_confidence").max(dec!(0.5)),
            severity: severity(score),
            reason_codes: dedup_reasons(reasons),
            positive_evidence: positive,
            negative_evidence: negative,
            missing_data_penalty: missing_penalty(
                features,
                &["rug_probability_score", "dev_sold_pct"],
            ),
            recommended_lifecycle_action: if launch_collapse > Decimal::ZERO
                || ath_collapse >= dec!(0.5)
                || score >= dec!(0.9)
            {
                LifecycleAction::HardDiscard
            } else if score >= dec!(0.75) {
                LifecycleAction::RugArchive
            } else if score >= dec!(0.60) {
                LifecycleAction::SoftDiscard
            } else {
                LifecycleAction::KeepWatching
            },
        }
    }

    fn bundle_risk(&self, _token: &TokenState, features: &FeatureSnapshot) -> RiskScore {
        let same_slot = decimal(features, "same_slot_multi_buy_count");
        let identical = decimal(features, "first_n_buys_identical_amount_count");
        let same_funder = decimal(features, "first_n_buys_same_funder_count");
        let bundle_top = decimal(features, "bundle_wallets_top_holder_pct");
        let shred_bundle = decimal(features, "tentative_sell_from_bundle_count");
        let shred_warning = decimal(features, "shred_sell_warning_level");
        let mut reasons = Vec::new();
        let mut negative = Vec::new();
        if same_funder > Decimal::ZERO {
            reasons.push(ReasonCode::BundleConcentration);
            negative.push("first buyers share funding sources".to_owned());
        }
        if shred_bundle > Decimal::ZERO {
            reasons.push(ReasonCode::ShredBundleExitWarning);
            negative.push("bundle cluster showed tentative exit pressure".to_owned());
        }
        let score = clamp01(weighted_average(&[
            (decimal(features, "bundle_risk_score"), dec!(0.50)),
            (normalize_count(same_slot, 8), dec!(0.15)),
            (normalize_count(identical, 8), dec!(0.15)),
            (normalize_count(same_funder, 8), dec!(0.10)),
            (bundle_top, dec!(0.10)),
            (normalize_count(shred_bundle, 4), dec!(0.15)),
            (shred_warning / dec!(4.0), dec!(0.10)),
        ]));
        if score >= dec!(0.40) {
            reasons.push(ReasonCode::HighBundleRisk);
        }
        RiskScore {
            name: "bundle".to_owned(),
            score,
            confidence: decimal(features, "bundle_confidence").max(dec!(0.5)),
            severity: severity(score),
            reason_codes: dedup_reasons(reasons),
            positive_evidence: Vec::new(),
            negative_evidence: negative,
            missing_data_penalty: missing_penalty(features, &["bundle_risk_score"]),
            recommended_lifecycle_action: if score >= dec!(0.85) {
                LifecycleAction::HardDiscard
            } else if score >= dec!(0.70) {
                LifecycleAction::SoftDiscard
            } else {
                LifecycleAction::KeepWatching
            },
        }
    }

    fn dev_risk(&self, token: &TokenState, features: &FeatureSnapshot) -> RiskScore {
        let sold_flag = bool_decimal(features, "dev_sold_flag");
        let sell_pct = decimal(features, "dev_sold_pct");
        let creator_top1 = bool_decimal(features, "creator_is_top1_holder");
        let retained_supply = decimal(features, "creator_retained_supply_pct");
        let launches_last_hour = decimal(features, "creator_launches_last_1h");
        let reused_payer = bool_decimal(features, "creator_reuses_same_payer");
        let spam_wave = bool_decimal(features, "creator_launching_spam_wave_flag");
        let cluster_pressure = if token.developer_state.related_cluster_id.is_some() {
            dec!(0.1)
        } else {
            Decimal::ZERO
        };
        let tentative_dev_sells = decimal(features, "tentative_sell_from_dev_count");
        let shred_warning = decimal(features, "shred_sell_warning_level");

        let mut reasons = Vec::new();
        let mut negative = Vec::new();
        if sell_pct > Decimal::ZERO || sold_flag > Decimal::ZERO {
            reasons.push(ReasonCode::DevSoldEarly);
            negative.push("creator is distributing tokens".to_owned());
        }
        if tentative_dev_sells > Decimal::ZERO {
            reasons.push(ReasonCode::ShredDevSellWarning);
            negative.push("creator sell intent appeared before canonical confirmation".to_owned());
        }
        let score = clamp01(weighted_average(&[
            (sell_pct.max(sold_flag), dec!(0.40)),
            (creator_top1, dec!(0.15)),
            (retained_supply, dec!(0.15)),
            (normalize_count(launches_last_hour, 5), dec!(0.10)),
            (reused_payer, dec!(0.10)),
            (spam_wave, dec!(0.10)),
            (cluster_pressure, dec!(0.10)),
            (normalize_count(tentative_dev_sells, 3), dec!(0.20)),
            (shred_warning / dec!(4.0), dec!(0.10)),
        ]));
        RiskScore {
            name: "dev".to_owned(),
            score,
            confidence: dec!(0.75),
            severity: severity(score),
            reason_codes: dedup_reasons(reasons),
            positive_evidence: if sell_pct == Decimal::ZERO && sold_flag == Decimal::ZERO {
                vec!["creator patience remains intact".to_owned()]
            } else {
                Vec::new()
            },
            negative_evidence: negative,
            missing_data_penalty: missing_penalty(features, &["dev_sold_pct"]),
            recommended_lifecycle_action: if score >= dec!(0.75) {
                LifecycleAction::SoftDiscard
            } else {
                LifecycleAction::KeepWatching
            },
        }
    }

    fn top_holder_risk(&self, _token: &TokenState, features: &FeatureSnapshot) -> RiskScore {
        let top1 = decimal(features, "top1_holder_pct");
        let top5 = decimal(features, "top5_holder_pct");
        let one_wallet = bool_decimal(features, "one_wallet_controls_market_flag");
        let concentration = decimal(features, "concentration_risk_score");
        let profit_overhang = decimal(features, "profit_overhang_score");
        let free_roll_dump = decimal(features, "free_roll_dump_risk_score");
        let same_funder = decimal(features, "top_holders_same_funder_pct");
        let same_fingerprint = decimal(features, "top_holders_same_client_fingerprint_pct");
        let active_selling = bool_decimal(features, "price_up_top_holders_selling");
        let tentative_top_holder_sells = decimal(features, "tentative_sell_from_top_holder_count");
        let mut reasons = Vec::new();
        if one_wallet > Decimal::ZERO || active_selling > Decimal::ZERO || top1 >= dec!(0.55) {
            reasons.push(ReasonCode::TopHolderDump);
        }
        if tentative_top_holder_sells > Decimal::ZERO {
            reasons.push(ReasonCode::ShredTopHolderSellWarning);
        }
        let score = clamp01(weighted_average(&[
            (concentration, dec!(0.30)),
            (profit_overhang, dec!(0.20)),
            (free_roll_dump, dec!(0.15)),
            (same_funder, dec!(0.10)),
            (same_fingerprint, dec!(0.10)),
            (active_selling, dec!(0.10)),
            (top1, dec!(0.05)),
            (normalize_count(tentative_top_holder_sells, 4), dec!(0.15)),
        ]));
        if score >= dec!(0.7) {
            reasons.push(ReasonCode::HighTopHolderRisk);
        }
        RiskScore {
            name: "top_holder".to_owned(),
            score,
            confidence: dec!(0.8),
            severity: severity(score),
            reason_codes: dedup_reasons(reasons),
            positive_evidence: if score < dec!(0.35) {
                vec!["top holders are not showing coordinated exit pressure".to_owned()]
            } else {
                Vec::new()
            },
            negative_evidence: vec![
                format!("top1 {:.4} top5 {:.4}", top1, top5),
                format!("profit_overhang {:.4}", profit_overhang),
            ],
            missing_data_penalty: missing_penalty(
                features,
                &[
                    "top1_holder_pct",
                    "profit_overhang_score",
                    "free_roll_dump_risk_score",
                ],
            ),
            recommended_lifecycle_action: if score >= dec!(0.8) {
                LifecycleAction::SoftDiscard
            } else {
                LifecycleAction::KeepWatching
            },
        }
    }

    fn fake_momentum_risk(&self, _token: &TokenState, features: &FeatureSnapshot) -> RiskScore {
        let vertical_without_growth = bool_decimal(features, "vertical_move_without_holder_growth");
        let vertical_without_buyers = bool_decimal(features, "vertical_move_without_unique_buyers");
        let no_retail_breadth = bool_decimal(features, "price_up_no_retail_breadth");
        let low_authenticity = Decimal::ONE - decimal(features, "momentum_authenticity_score");
        let top_holder_selling = bool_decimal(features, "price_up_top_holders_selling");
        let bundle_selling = bool_decimal(features, "price_up_bundle_selling");
        let dev_selling = bool_decimal(features, "price_up_dev_selling");
        let dev_sold_flag = bool_decimal(features, "dev_sold_flag");
        let few_large_buys = bool_decimal(features, "price_up_only_one_or_two_large_buys");
        let concentrated_buys = bool_decimal(features, "price_up_buy_size_concentration_high");
        let exit_trap = decimal(features, "exit_liquidity_trap_score");
        let top1 = decimal(features, "top1_holder_pct");
        let absorption = decimal(features, "absorption_success_score");
        let stickiness = decimal(features, "holder_stickiness_score");
        let shred_warning = decimal(features, "shred_sell_warning_level");
        let rug_overlap = decimal(features, "rug_probability_score") / Decimal::from(2u64);
        let concentration_extreme = clamp01((top1 - dec!(0.65)) / dec!(0.35));
        let mut reasons = Vec::new();
        if vertical_without_growth > Decimal::ZERO
            || vertical_without_buyers > Decimal::ZERO
            || no_retail_breadth > Decimal::ZERO
            || top_holder_selling > Decimal::ZERO
            || bundle_selling > Decimal::ZERO
            || dev_selling > Decimal::ZERO
            || concentrated_buys > Decimal::ZERO
            || (concentration_extreme > Decimal::ZERO && dev_sold_flag > Decimal::ZERO)
        {
            reasons.push(ReasonCode::HighFakeMomentumRisk);
        }
        let score = clamp01(
            weighted_average(&[
                (vertical_without_growth, dec!(0.18)),
                (vertical_without_buyers, dec!(0.15)),
                (no_retail_breadth, dec!(0.15)),
                (top_holder_selling, dec!(0.15)),
                (bundle_selling, dec!(0.10)),
                (dev_selling, dec!(0.10)),
                (few_large_buys, dec!(0.07)),
                (concentrated_buys, dec!(0.05)),
                (exit_trap, dec!(0.08)),
                (concentration_extreme, dec!(0.07)),
                (dev_sold_flag, dec!(0.05)),
                (low_authenticity, dec!(0.07)),
                (rug_overlap, dec!(0.05)),
                (shred_warning / dec!(4.0), dec!(0.06)),
            ]) - weighted_average(&[(absorption, dec!(0.20)), (stickiness, dec!(0.12))]),
        );
        RiskScore {
            name: "fake_momentum".to_owned(),
            score,
            confidence: dec!(0.7),
            severity: severity(score),
            reason_codes: dedup_reasons(reasons),
            positive_evidence: if score < dec!(0.3) {
                vec!["price strength is confirmed by breadth or absorption".to_owned()]
            } else {
                Vec::new()
            },
            negative_evidence: vec![
                "price action lacks enough supporting breadth or authenticity".to_owned(),
            ],
            missing_data_penalty: missing_penalty(
                features,
                &[
                    "vertical_move_without_holder_growth",
                    "momentum_authenticity_score",
                ],
            ),
            recommended_lifecycle_action: if score >= dec!(0.8) {
                LifecycleAction::SoftDiscard
            } else {
                LifecycleAction::KeepWatching
            },
        }
    }

    fn data_quality_risk(&self, _token: &TokenState, features: &FeatureSnapshot) -> RiskScore {
        let quality = decimal(features, "data_quality_score");
        let critical_missing =
            normalize_count(decimal(features, "critical_feature_missing_count"), 5);
        let disagreement = bool_decimal(features, "source_disagreement_flag");
        let trade_allowed = bool_decimal(features, "trade_allowed_data_quality_flag");
        let shred_stale = bool_decimal(features, "shred_signal_stale_flag");
        let explicit_gap = bool_decimal(features, "account_update_gap_flag")
            .max(bool_decimal(features, "transaction_stream_gap_flag"))
            .max(bool_decimal(features, "slot_status_gap_flag"))
            .max(disagreement)
            .max(shred_stale);
        let mut reasons = Vec::new();
        if trade_allowed == Decimal::ZERO && explicit_gap > Decimal::ZERO {
            reasons.push(ReasonCode::DataGapActive);
        }
        let score = clamp01(weighted_average(&[
            (Decimal::ONE - quality, dec!(0.60)),
            (critical_missing, dec!(0.25)),
            (disagreement, dec!(0.15)),
            (shred_stale, dec!(0.10)),
        ]));
        RiskScore {
            name: "data_quality".to_owned(),
            score,
            confidence: dec!(0.9),
            severity: severity(score),
            reason_codes: dedup_reasons(reasons),
            positive_evidence: if trade_allowed > Decimal::ZERO {
                vec!["data quality is good enough for decision support".to_owned()]
            } else {
                Vec::new()
            },
            negative_evidence: if trade_allowed == Decimal::ZERO && explicit_gap > Decimal::ZERO {
                vec!["critical feature completeness or source confidence too low".to_owned()]
            } else if trade_allowed == Decimal::ZERO {
                vec!["feature completeness is still building; keep watching but do not force a terminal data-gap state yet".to_owned()]
            } else {
                Vec::new()
            },
            missing_data_penalty: missing_penalty(
                features,
                &["data_quality_score", "critical_feature_missing_count"],
            ),
            recommended_lifecycle_action: if explicit_gap > Decimal::ZERO
                && (quality < self.min_data_quality || trade_allowed == Decimal::ZERO)
            {
                LifecycleAction::DataGapStop
            } else {
                LifecycleAction::KeepWatching
            },
        }
    }
}

fn decimal(features: &FeatureSnapshot, feature_id: &str) -> Decimal {
    features.decimal(feature_id).unwrap_or(Decimal::ZERO)
}

fn bool_decimal(features: &FeatureSnapshot, feature_id: &str) -> Decimal {
    features.decimal(feature_id).unwrap_or(Decimal::ZERO)
}

fn severity(score: Decimal) -> RiskSeverity {
    if score >= dec!(0.85) {
        RiskSeverity::Critical
    } else if score >= dec!(0.65) {
        RiskSeverity::High
    } else if score >= dec!(0.35) {
        RiskSeverity::Medium
    } else {
        RiskSeverity::Low
    }
}

fn missing_penalty(features: &FeatureSnapshot, feature_ids: &[&str]) -> Decimal {
    let missing = feature_ids
        .iter()
        .filter(|feature_id| {
            features
                .value(feature_id)
                .map(|value| value.status != features::FeatureStatus::Available)
                .unwrap_or(true)
        })
        .count();
    if feature_ids.is_empty() {
        Decimal::ZERO
    } else {
        Decimal::from(missing as u64) / Decimal::from(feature_ids.len() as u64)
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

fn normalize_count(value: Decimal, cap: u64) -> Decimal {
    clamp01(value / Decimal::from(cap.max(1)))
}

fn clamp01(value: Decimal) -> Decimal {
    value.max(Decimal::ZERO).min(Decimal::ONE)
}

fn dedup_reasons(reasons: Vec<ReasonCode>) -> Vec<ReasonCode> {
    let mut seen = BTreeMap::new();
    for reason in reasons {
        seen.insert(reason, ());
    }
    seen.into_keys().collect()
}

#[cfg(test)]
mod tests {
    use common::{
        Canonicality, EventMeta, EventPayload, EventSource, HolderBalanceUpdateEvent,
        NormalizedEvent, PumpBuyEvent, PumpSellEvent, QuoteAssetType, TokenCreatedEvent,
        TokenProgramType, TransactionStatus, TtlConfig, WalletFundingEvent,
    };
    use features::FeatureEngine;
    use state::StateEngine;
    use time::Duration;

    use super::*;

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
                associated_bonding_curve_account: Some(pubkey("assoc")),
                metadata_account: Some(pubkey("meta")),
                name: "Alpha".to_owned(),
                symbol: "ALP".to_owned(),
                uri: "https://example.invalid/alpha".to_owned(),
                create_instruction_variant: "create".to_owned(),
                initial_virtual_quote_reserves: Some(Decimal::from(10u64)),
                initial_virtual_token_reserves: Some(Decimal::from(1_000u64)),
                initial_real_quote_reserves: Some(Decimal::from(10u64)),
                initial_real_token_reserves: Some(Decimal::from(1_000u64)),
                initial_supply: Some(Decimal::from(1_000u64)),
                creator_initial_buy: None,
                same_transaction_buys: 2,
                same_slot_buys: 4,
                fee_recipients: vec![],
                raw_account_list: vec![],
                launch_transaction_fingerprint: Some("fingerprint-a".to_owned()),
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

    fn sell(slot: u64, seller: &str, quote: u64, tokens: u64) -> NormalizedEvent {
        let mut meta = meta(slot);
        meta.signature = Some(format!("sell-{slot}-{seller}"));
        NormalizedEvent {
            meta,
            payload: EventPayload::PumpSell(PumpSellEvent {
                mint: pubkey("mint"),
                seller: pubkey(seller),
                quote_out: Decimal::from(quote),
                token_in: Decimal::from(tokens),
                price_before: None,
                price_after: None,
                effective_price: Decimal::from(quote) / Decimal::from(tokens),
                slippage_estimate: None,
                reserves_before: None,
                reserves_after: None,
                min_quote_output: None,
                compute_unit_limit: None,
                compute_unit_price: None,
                estimated_priority_fee_lamports: None,
                estimated_base_fee_lamports: None,
                estimated_tip_lamports: None,
                is_creator: seller == "creator",
                is_top_holder_pre_sell: false,
                is_known_cluster_member: false,
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
                token_decimals: Some(common::DEFAULT_PUMP_TOKEN_DECIMALS),
                old_balance: None,
                new_balance: Decimal::from(balance),
                delta: Decimal::from(balance),
                caused_by_signature: None,
                update_reason: "trade".to_owned(),
                confidence: Decimal::ONE,
            }),
        }
    }

    fn funding(slot: u64, wallet: &str, funder: &str) -> NormalizedEvent {
        NormalizedEvent {
            meta: meta(slot),
            payload: EventPayload::WalletFunding(WalletFundingEvent {
                wallet: pubkey(wallet),
                funder: pubkey(funder),
                asset_label: "SOL".to_owned(),
                amount: Decimal::from(2u64),
                slot,
                signature: format!("fund-{slot}-{wallet}"),
                relation_to_launch: Some("before_launch".to_owned()),
                near_launch_relation: true,
                funding_graph_edge_id: format!("edge-{slot}-{wallet}"),
            }),
        }
    }

    fn build_assessment() -> RiskAssessment {
        let mut engine = StateEngine::new(ttl());
        engine.apply_event(&token_created()).expect("create");
        engine
            .apply_event(&funding(1, "buyer-a", "funder-a"))
            .expect("fund");
        engine
            .apply_event(&funding(1, "buyer-b", "funder-a"))
            .expect("fund");
        engine
            .apply_event(&buy(2, "creator", 12, 200))
            .expect("buy");
        engine
            .apply_event(&buy(2, "buyer-a", 12, 200))
            .expect("buy");
        engine
            .apply_event(&buy(2, "buyer-b", 12, 200))
            .expect("buy");
        engine
            .apply_event(&sell(3, "creator", 2, 120))
            .expect("sell");
        engine
            .apply_event(&holder(3, "creator", 80))
            .expect("holder");
        engine
            .apply_event(&holder(3, "buyer-a", 200))
            .expect("holder");
        engine
            .apply_event(&holder(3, "buyer-b", 200))
            .expect("holder");

        let snapshot = engine.snapshot();
        let token = snapshot.tokens.get("mint").expect("token");
        let features = FeatureEngine::default().compute_snapshot(
            token,
            &snapshot,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(250),
        );
        RiskEngine::default().evaluate(
            token,
            &features,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(250),
        )
    }

    #[test]
    fn dev_dump_rug_score_increases() {
        let assessment = build_assessment();
        assert!(assessment.rug.score > Decimal::ZERO);
        assert!(assessment.dev.score > Decimal::ZERO);
        assert!(assessment.reason_codes.contains(&ReasonCode::DevSoldEarly));
    }

    #[test]
    fn bundle_score_uses_evidence() {
        let assessment = build_assessment();
        assert!(assessment.bundle.score > Decimal::ZERO);
        assert!(
            assessment
                .bundle
                .reason_codes
                .contains(&ReasonCode::BundleConcentration)
                || assessment.bundle.score < dec!(0.7)
        );
    }

    #[test]
    fn top_holder_sell_risk_detected() {
        let assessment = build_assessment();
        assert!(assessment.top_holder.score >= Decimal::ZERO);
        assert!(
            assessment
                .top_holder
                .negative_evidence
                .iter()
                .any(|line| line.contains("top1"))
        );
    }

    #[test]
    fn fake_momentum_and_data_gap_can_block() {
        let assessment = build_assessment();
        assert!(assessment.fake_momentum.score >= Decimal::ZERO);
        assert!(assessment.data_quality.score >= Decimal::ZERO);
    }

    #[test]
    fn hard_discard_on_severe_collapse_or_gap() {
        let mut engine = StateEngine::new(ttl());
        engine.apply_event(&token_created()).expect("create");
        engine
            .apply_event(&buy(2, "creator", 10, 100))
            .expect("buy");
        engine
            .apply_event(&sell(3, "creator", 1, 99))
            .expect("sell");

        let snapshot = engine.snapshot();
        let token = snapshot.tokens.get("mint").expect("token");
        let features = FeatureEngine::default().compute_snapshot(
            token,
            &snapshot,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(6),
        );
        let assessment = RiskEngine::default().evaluate(
            token,
            &features,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(6),
        );
        assert!(matches!(
            assessment.discard_policy,
            DiscardPolicyDecision::Keep
                | DiscardPolicyDecision::SoftDiscard
                | DiscardPolicyDecision::HardDiscard
                | DiscardPolicyDecision::RugArchive
        ));
    }

    #[test]
    fn compact_summary_preserves_discard_context() {
        let assessment = build_assessment();
        let summary = RiskEngine::default().compact_summary(&assessment);
        assert_eq!(summary.mint, "mint");
        assert_eq!(summary.overall_score, assessment.overall_score);
    }
}
