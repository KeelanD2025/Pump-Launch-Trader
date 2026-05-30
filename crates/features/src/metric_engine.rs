use common::{
    PUMP_TOTAL_SUPPLY_UI, pump_curve_progress_pct_from_real_token_reserves_raw,
    pump_market_cap_quote_1b, pump_market_cap_quote_total_supply,
    pump_virtual_reserve_price_sol_per_token, raw_tokens_to_ui, ui_tokens_to_raw,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum MetricDatum {
    Numeric(Decimal),
    Text(String),
    Boolean(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricConfidence {
    Verified,
    ConfidenceScored,
    Unavailable,
    FailedInvariant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricParityStatus {
    NotChecked,
    Passed,
    Failed,
    Unavailable,
    ConfidenceScored,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricValue {
    pub metric_name: String,
    pub metric_family: String,
    pub value: Option<MetricDatum>,
    pub unit: String,
    pub source_artifact: String,
    pub source_event_ids: Vec<String>,
    pub formula_version: String,
    pub metric_version: String,
    pub confidence: MetricConfidence,
    pub confidence_reason: Option<String>,
    pub unavailable_reason: Option<String>,
    pub parity_status: MetricParityStatus,
    pub required_for_backtest: bool,
    pub required_for_tuning: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricContext {
    pub source_run_id: Option<String>,
    pub derived_run_id: Option<String>,
    pub mint: Option<String>,
    pub token_decimals: u8,
    pub quote_asset: String,
    pub slot: Option<u64>,
    pub sequence: Option<u64>,
    pub observed_at: Option<OffsetDateTime>,
    pub data_quality_state: Option<String>,
    pub math_version: String,
    pub enrichment_version: Option<String>,
}

impl Default for MetricContext {
    fn default() -> Self {
        Self {
            source_run_id: None,
            derived_run_id: None,
            mint: None,
            token_decimals: common::DEFAULT_PUMP_TOKEN_DECIMALS,
            quote_asset: "SOL".to_owned(),
            slot: None,
            sequence: None,
            observed_at: None,
            data_quality_state: None,
            math_version: "phase80_metric_engine_v1".to_owned(),
            enrichment_version: None,
        }
    }
}

pub struct MetricEngine;

impl MetricEngine {
    pub const FORMULA_VERSION: &'static str = "pumpfun_canonical_math_v2";
    pub const METRIC_VERSION: &'static str = "phase80_metric_engine_v1";

    pub fn numeric(
        metric_name: &str,
        metric_family: &str,
        value: Decimal,
        unit: &str,
        source_artifact: &str,
        confidence: MetricConfidence,
        confidence_reason: impl Into<Option<String>>,
        required_for_backtest: bool,
        required_for_tuning: bool,
    ) -> MetricValue {
        MetricValue {
            metric_name: metric_name.to_owned(),
            metric_family: metric_family.to_owned(),
            value: Some(MetricDatum::Numeric(value)),
            unit: unit.to_owned(),
            source_artifact: source_artifact.to_owned(),
            source_event_ids: Vec::new(),
            formula_version: Self::FORMULA_VERSION.to_owned(),
            metric_version: Self::METRIC_VERSION.to_owned(),
            confidence,
            confidence_reason: confidence_reason.into(),
            unavailable_reason: None,
            parity_status: match confidence {
                MetricConfidence::Verified => MetricParityStatus::Passed,
                MetricConfidence::ConfidenceScored => MetricParityStatus::ConfidenceScored,
                MetricConfidence::Unavailable => MetricParityStatus::Unavailable,
                MetricConfidence::FailedInvariant => MetricParityStatus::Failed,
            },
            required_for_backtest,
            required_for_tuning,
        }
    }

    pub fn unavailable(
        metric_name: &str,
        metric_family: &str,
        unit: &str,
        source_artifact: &str,
        reason: impl Into<String>,
        required_for_backtest: bool,
        required_for_tuning: bool,
    ) -> MetricValue {
        MetricValue {
            metric_name: metric_name.to_owned(),
            metric_family: metric_family.to_owned(),
            value: None,
            unit: unit.to_owned(),
            source_artifact: source_artifact.to_owned(),
            source_event_ids: Vec::new(),
            formula_version: Self::FORMULA_VERSION.to_owned(),
            metric_version: Self::METRIC_VERSION.to_owned(),
            confidence: MetricConfidence::Unavailable,
            confidence_reason: None,
            unavailable_reason: Some(reason.into()),
            parity_status: MetricParityStatus::Unavailable,
            required_for_backtest,
            required_for_tuning,
        }
    }

    pub fn reserve_price_sol_per_token(
        virtual_quote_lamports: Decimal,
        virtual_token_raw: Decimal,
        decimals: u8,
    ) -> MetricValue {
        pump_virtual_reserve_price_sol_per_token(
            virtual_quote_lamports,
            virtual_token_raw,
            decimals,
        )
        .map(|price| {
            Self::numeric(
                "price_sol_per_token",
                "reserve-implied price",
                price,
                "SOL/token",
                "normalized_events.bonding_curve_update",
                MetricConfidence::Verified,
                Some("computed from matching virtual SOL/token reserve pair".to_owned()),
                true,
                true,
            )
        })
        .unwrap_or_else(|| {
            Self::unavailable(
                "price_sol_per_token",
                "reserve-implied price",
                "SOL/token",
                "normalized_events.bonding_curve_update",
                "missing or zero virtual reserve denominator",
                true,
                true,
            )
        })
    }

    pub fn market_caps(price_quote_per_token: Option<Decimal>) -> (MetricValue, MetricValue) {
        if let Some(price) = price_quote_per_token {
            (
                Self::numeric(
                    "market_cap_quote_1b",
                    "market cap",
                    pump_market_cap_quote_1b(price),
                    "SOL",
                    "metric_engine.price_sol_per_token",
                    MetricConfidence::Verified,
                    Some("price * 1B Pump.fun convention".to_owned()),
                    true,
                    true,
                ),
                Self::numeric(
                    "market_cap_quote_total_supply",
                    "market cap",
                    pump_market_cap_quote_total_supply(price, Decimal::from(PUMP_TOTAL_SUPPLY_UI)),
                    "SOL",
                    "metric_engine.price_sol_per_token",
                    MetricConfidence::Verified,
                    Some(
                        "price * Pump.fun curve-economic supply denominator; RPC mint supply is diagnostic-only when it diverges"
                            .to_owned(),
                    ),
                    true,
                    true,
                ),
            )
        } else {
            (
                Self::unavailable(
                    "market_cap_quote_1b",
                    "market cap",
                    "SOL",
                    "metric_engine.price_sol_per_token",
                    "price unavailable",
                    true,
                    true,
                ),
                Self::unavailable(
                    "market_cap_quote_total_supply",
                    "market cap",
                    "SOL",
                    "metric_engine.price_sol_per_token",
                    "price unavailable",
                    true,
                    true,
                ),
            )
        }
    }

    pub fn curve_progress_pct(
        real_token_reserves_raw: Option<Decimal>,
        decimals: u8,
    ) -> MetricValue {
        real_token_reserves_raw
            .and_then(|raw| pump_curve_progress_pct_from_real_token_reserves_raw(raw, decimals))
            .map(|progress| {
                Self::numeric(
                    "curve_progress_pct",
                    "curve progress",
                    progress,
                    "percent",
                    "normalized_events.bonding_curve_update.real_token_reserves",
                    MetricConfidence::Verified,
                    Some(
                        "real token reserves converted to UI before reserved-supply formula"
                            .to_owned(),
                    ),
                    true,
                    true,
                )
            })
            .unwrap_or_else(|| {
                Self::unavailable(
                    "curve_progress_pct",
                    "curve progress",
                    "percent",
                    "normalized_events.bonding_curve_update.real_token_reserves",
                    "missing curve-state snapshot or invalid token decimals",
                    true,
                    true,
                )
            })
    }

    pub fn dev_holding_metrics(
        dev_balance_raw: Option<Decimal>,
        decimals: u8,
        source_artifact: &str,
    ) -> (MetricValue, MetricValue, MetricValue) {
        let Some(dev_balance_raw) = dev_balance_raw.map(|value| value.max(Decimal::ZERO)) else {
            return (
                Self::unavailable(
                    "dev_balance",
                    "dev holdings",
                    "tokens",
                    source_artifact,
                    "creator wallet holder balance unavailable",
                    true,
                    true,
                ),
                Self::unavailable(
                    "dev_holding_pct_total_supply",
                    "dev holdings",
                    "ratio",
                    source_artifact,
                    "creator wallet holder balance unavailable",
                    true,
                    true,
                ),
                Self::unavailable(
                    "dev_holding_pct_circulating",
                    "dev holdings",
                    "ratio",
                    source_artifact,
                    "creator wallet holder balance unavailable",
                    false,
                    true,
                ),
            );
        };

        let total_supply_ui = Decimal::from(PUMP_TOTAL_SUPPLY_UI);
        let total_supply_raw = ui_tokens_to_raw(total_supply_ui, decimals);
        let dev_balance_ui = raw_tokens_to_ui(dev_balance_raw, decimals);
        if total_supply_raw <= Decimal::ZERO || dev_balance_raw > total_supply_raw {
            return (
                Self::numeric(
                    "dev_balance",
                    "dev holdings",
                    dev_balance_ui,
                    "tokens",
                    source_artifact,
                    MetricConfidence::FailedInvariant,
                    Some("creator balance exceeds canonical Pump.fun supply".to_owned()),
                    true,
                    true,
                ),
                Self::unavailable(
                    "dev_holding_pct_total_supply",
                    "dev holdings",
                    "ratio",
                    source_artifact,
                    "creator balance exceeds canonical Pump.fun supply",
                    true,
                    true,
                ),
                Self::unavailable(
                    "dev_holding_pct_circulating",
                    "dev holdings",
                    "ratio",
                    source_artifact,
                    "creator balance exceeds canonical Pump.fun supply",
                    false,
                    true,
                ),
            );
        }

        let circulating_supply_raw = (total_supply_raw - dev_balance_raw).max(Decimal::ZERO);
        let circulating = if circulating_supply_raw > Decimal::ZERO {
            Self::numeric(
                "dev_holding_pct_circulating",
                "dev holdings",
                dev_balance_raw / circulating_supply_raw,
                "ratio",
                source_artifact,
                MetricConfidence::ConfidenceScored,
                Some("uses canonical total supply minus creator balance; circulating denominator is a convention".to_owned()),
                false,
                true,
            )
        } else {
            Self::unavailable(
                "dev_holding_pct_circulating",
                "dev holdings",
                "ratio",
                source_artifact,
                "circulating denominator is zero",
                false,
                true,
            )
        };

        (
            Self::numeric(
                "dev_balance",
                "dev holdings",
                dev_balance_ui,
                "tokens",
                source_artifact,
                MetricConfidence::Verified,
                Some("owner-summed holder balance for creator wallet".to_owned()),
                true,
                true,
            ),
            Self::numeric(
                "dev_holding_pct_total_supply",
                "dev holdings",
                dev_balance_raw / total_supply_raw,
                "ratio",
                source_artifact,
                MetricConfidence::Verified,
                Some("creator raw balance / canonical raw total supply".to_owned()),
                true,
                true,
            ),
            circulating,
        )
    }
}

pub fn metric_decimal(metric: &MetricValue) -> Option<Decimal> {
    match metric.value.as_ref() {
        Some(MetricDatum::Numeric(value)) => Some(*value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{PUMP_INITIAL_REAL_TOKEN_RESERVES_UI, PUMP_RESERVED_TOKENS_UI, ui_tokens_to_raw};

    #[test]
    fn missing_metric_is_unavailable_not_zero() {
        let metric = MetricEngine::unavailable(
            "holder_count",
            "holder",
            "wallets",
            "holder_state",
            "missing holder snapshot",
            true,
            true,
        );
        assert!(metric.value.is_none());
        assert_eq!(metric.confidence, MetricConfidence::Unavailable);
        assert_eq!(metric.parity_status, MetricParityStatus::Unavailable);
    }

    #[test]
    fn price_and_market_cap_use_virtual_reserve_formula() {
        let price = MetricEngine::reserve_price_sol_per_token(
            Decimal::from(30_000_000_000u64),
            Decimal::from(1_000_000_000_000_000u64),
            6,
        );
        assert_eq!(metric_decimal(&price), Some(Decimal::new(3, 8)));
        let (cap_1b, cap_total) = MetricEngine::market_caps(metric_decimal(&price));
        assert_eq!(metric_decimal(&cap_1b), Some(Decimal::from(30u64)));
        assert_eq!(metric_decimal(&cap_total), Some(Decimal::from(30u64)));
    }

    #[test]
    fn curve_progress_handles_synthetic_bounds() {
        let zero = MetricEngine::curve_progress_pct(
            Some(ui_tokens_to_raw(
                Decimal::from(PUMP_INITIAL_REAL_TOKEN_RESERVES_UI + PUMP_RESERVED_TOKENS_UI),
                6,
            )),
            6,
        );
        let half = MetricEngine::curve_progress_pct(
            Some(ui_tokens_to_raw(
                Decimal::from(PUMP_RESERVED_TOKENS_UI)
                    + Decimal::from(PUMP_INITIAL_REAL_TOKEN_RESERVES_UI) / Decimal::from(2u64),
                6,
            )),
            6,
        );
        let complete = MetricEngine::curve_progress_pct(
            Some(ui_tokens_to_raw(Decimal::from(PUMP_RESERVED_TOKENS_UI), 6)),
            6,
        );
        assert_eq!(metric_decimal(&zero), Some(Decimal::ZERO));
        assert_eq!(metric_decimal(&half), Some(Decimal::from(50u64)));
        assert_eq!(metric_decimal(&complete), Some(Decimal::from(100u64)));
    }

    #[test]
    fn dev_holding_over_supply_fails_invariant() {
        let over_supply = ui_tokens_to_raw(Decimal::from(PUMP_TOTAL_SUPPLY_UI + 1), 6);
        let (balance, total_pct, circulating_pct) =
            MetricEngine::dev_holding_metrics(Some(over_supply), 6, "holder_state");
        assert_eq!(balance.confidence, MetricConfidence::FailedInvariant);
        assert_eq!(total_pct.parity_status, MetricParityStatus::Unavailable);
        assert_eq!(
            circulating_pct.parity_status,
            MetricParityStatus::Unavailable
        );
    }

    #[test]
    fn dev_holding_from_owner_sum_is_ratio_safe() {
        let dev_balance = ui_tokens_to_raw(Decimal::from(100_000_000u64), 6);
        let (_, total_pct, circulating_pct) =
            MetricEngine::dev_holding_metrics(Some(dev_balance), 6, "holder_state");
        assert_eq!(metric_decimal(&total_pct), Some(Decimal::new(1, 1)));
        assert!(metric_decimal(&circulating_pct).is_some_and(|value| value < Decimal::ONE));
    }
}
