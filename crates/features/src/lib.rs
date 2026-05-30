use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Instant,
};

pub mod metric_engine;
pub mod risk_engine;

use common::{
    EventSource, PUMP_TOTAL_SUPPLY_UI, PubkeyValue, TentativeSellRiskLevel, raw_tokens_to_ui,
    ui_tokens_to_raw,
};
use metric_engine::{MetricConfidence, MetricEngine, metric_decimal};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use state::{
    ClusterIndex, FundingGraph, StateEngine, StateSnapshot, TokenLifecycle, TokenState,
    WalletSummary,
};
use time::{Duration, OffsetDateTime};

const WINDOW_5S: Duration = Duration::seconds(5);
const WINDOW_15S: Duration = Duration::seconds(15);
const WINDOW_30S: Duration = Duration::seconds(30);
const WINDOW_1M: Duration = Duration::minutes(1);
const WINDOW_5M: Duration = Duration::minutes(5);
const SUPPLY_DENOMINATOR_POLICY_VERSION: &str = "phase102.supply_denominator.v1";

macro_rules! dec {
    ($value:literal) => {
        Decimal::from_str_exact(stringify!($value)).expect("decimal literal")
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureStatus {
    Available,
    Missing,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum FeatureDatum {
    Numeric(Decimal),
    Text(String),
    Boolean(bool),
}

impl FeatureDatum {
    pub fn as_decimal(&self) -> Option<Decimal> {
        match self {
            Self::Numeric(value) => Some(*value),
            Self::Boolean(value) => Some(if *value { Decimal::ONE } else { Decimal::ZERO }),
            Self::Text(_) => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureValue {
    pub feature_id: String,
    pub status: FeatureStatus,
    pub value: Option<FeatureDatum>,
    pub confidence: Decimal,
    pub missing_data_penalty: Decimal,
    pub source_confidence: Decimal,
    pub notes: Option<String>,
}

impl FeatureValue {
    fn available(feature_id: &str, value: FeatureDatum, confidence: Decimal) -> Self {
        Self {
            feature_id: feature_id.to_owned(),
            status: FeatureStatus::Available,
            value: Some(value),
            confidence,
            missing_data_penalty: Decimal::ZERO,
            source_confidence: confidence,
            notes: None,
        }
    }

    fn missing(feature_id: &str, notes: impl Into<String>) -> Self {
        Self {
            feature_id: feature_id.to_owned(),
            status: FeatureStatus::Missing,
            value: None,
            confidence: Decimal::ZERO,
            missing_data_penalty: Decimal::ONE,
            source_confidence: Decimal::ZERO,
            notes: Some(notes.into()),
        }
    }

    fn unavailable(feature_id: &str, notes: impl Into<String>) -> Self {
        Self {
            feature_id: feature_id.to_owned(),
            status: FeatureStatus::Unavailable,
            value: None,
            confidence: Decimal::ZERO,
            missing_data_penalty: Decimal::ONE,
            source_confidence: Decimal::ZERO,
            notes: Some(notes.into()),
        }
    }

    pub fn as_decimal(&self) -> Option<Decimal> {
        self.value.as_ref().and_then(FeatureDatum::as_decimal)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureDescriptor {
    pub feature_id: String,
    pub name: String,
    pub category: String,
    pub description: String,
    pub data_dependencies: Vec<String>,
    pub window_or_anchor: String,
    pub update_cost: String,
    pub memory_cost: String,
    pub live_available: bool,
    pub replay_available: bool,
    pub no_lookahead_safe: bool,
    pub missing_data_behavior: String,
    pub normalization_method: String,
    pub regime_conditioning_method: String,
    pub version: u32,
    pub enabled_by_default: bool,
    pub computation_budget_class: String,
    pub default_status: FeatureStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureRegistry {
    descriptors: BTreeMap<String, FeatureDescriptor>,
}

impl FeatureRegistry {
    pub fn register(&mut self, descriptor: FeatureDescriptor) {
        self.descriptors
            .insert(descriptor.feature_id.clone(), descriptor);
    }

    pub fn descriptor(&self, feature_id: &str) -> Option<&FeatureDescriptor> {
        self.descriptors.get(feature_id)
    }

    pub fn descriptors(&self) -> &BTreeMap<String, FeatureDescriptor> {
        &self.descriptors
    }

    fn register_core(
        &mut self,
        feature_id: &str,
        category: &str,
        description: &str,
        dependencies: &[&str],
        window: &str,
    ) {
        self.register(FeatureDescriptor {
            feature_id: feature_id.to_owned(),
            name: feature_id.to_owned(),
            category: category.to_owned(),
            description: description.to_owned(),
            data_dependencies: dependencies
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            window_or_anchor: window.to_owned(),
            update_cost: "streaming".to_owned(),
            memory_cost: "bounded".to_owned(),
            live_available: true,
            replay_available: true,
            no_lookahead_safe: true,
            missing_data_behavior: "mark_missing".to_owned(),
            normalization_method: "direct_or_ranked".to_owned(),
            regime_conditioning_method: "external_regime_snapshot".to_owned(),
            version: 1,
            enabled_by_default: true,
            computation_budget_class: "core".to_owned(),
            default_status: FeatureStatus::Missing,
        });
    }

    fn register_placeholder_group(&mut self, category: &str, feature_ids: &[&str]) {
        for feature_id in feature_ids {
            self.register(FeatureDescriptor {
                feature_id: (*feature_id).to_owned(),
                name: (*feature_id).to_owned(),
                category: category.to_owned(),
                description:
                    "Registered for future implementation; currently guarded as unavailable."
                        .to_owned(),
                data_dependencies: vec!["state_snapshot".to_owned()],
                window_or_anchor: "varies".to_owned(),
                update_cost: "not_computed".to_owned(),
                memory_cost: "minimal".to_owned(),
                live_available: false,
                replay_available: false,
                no_lookahead_safe: true,
                missing_data_behavior: "mark_unavailable".to_owned(),
                normalization_method: "n/a".to_owned(),
                regime_conditioning_method: "n/a".to_owned(),
                version: 1,
                enabled_by_default: false,
                computation_budget_class: "deferred".to_owned(),
                default_status: FeatureStatus::Unavailable,
            });
        }
    }
}

impl Default for FeatureRegistry {
    fn default() -> Self {
        let mut registry = Self {
            descriptors: BTreeMap::new(),
        };

        for feature_id in [
            "launch_slot",
            "time_since_launch_ms",
            "mint",
            "quote_mint",
            "quote_asset_type",
            "creator_wallet",
            "payer_wallet",
            "bonding_curve_pubkey",
            "associated_bonding_curve_pubkey",
            "metadata_pubkey",
            "token_program_type",
            "create_instruction_variant",
            "name_length",
            "symbol_length",
            "uri_length",
            "has_empty_name",
            "has_empty_symbol",
            "has_empty_uri",
            "duplicate_name_seen_historically",
            "duplicate_symbol_seen_historically",
            "duplicate_uri_seen_historically",
            "name_entropy",
            "symbol_entropy",
            "uri_entropy",
            "suspicious_unicode_score",
            "uri_scheme",
            "launch_same_tx_buy_count",
            "launch_same_slot_buy_count",
            "first_seen_source",
            "launch_tx_client_fingerprint",
        ] {
            registry.register_core(
                feature_id,
                "launch_identity",
                feature_id,
                &["token_state"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "creator_age_in_local_index",
            "creator_total_launches_seen",
            "creator_launches_last_1h",
            "creator_cluster_id",
            "creator_reuses_same_payer",
            "creator_reuses_same_metadata_domain",
            "creator_launching_spam_wave_flag",
        ] {
            registry.register_core(
                feature_id,
                "creator",
                feature_id,
                &["wallet_index", "token_state"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "creator_initial_token_balance",
            "creator_retained_supply_pct",
            "creator_net_tokens_after_10s",
            "creator_net_tokens_after_30s",
            "creator_net_tokens_after_1m",
            "creator_net_tokens_after_5m",
            "creator_net_quote_flow_after_10s",
            "creator_net_quote_flow_after_30s",
            "creator_net_quote_flow_after_1m",
            "creator_net_quote_flow_after_5m",
            "creator_has_sold",
            "creator_first_sell_time_ms",
            "creator_dump_velocity",
            "creator_balance_rank",
            "creator_is_top1_holder",
            "creator_is_top3_holder",
            "creator_is_top5_holder",
            "creator_is_top10_holder",
            "dev_balance",
            "creator_ownership_pct",
            "dev_holding_pct_total_supply",
            "dev_holding_pct_curve_economic_supply",
            "token_supply_selected_for_holder_pct",
            "dev_holding_pct_circulating",
        ] {
            registry.register_core(
                feature_id,
                "dev_holdings",
                feature_id,
                &["developer_state", "holder_state"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "virtual_quote_reserves",
            "virtual_token_reserves",
            "real_quote_reserves",
            "real_token_reserves",
            "token_decimals",
            "price_lamports_per_raw_token",
            "price_sol_per_token",
            "reserve_price_confidence",
            "price_current",
            "price_launch",
            "price_change_from_launch_pct",
            "market_cap_quote_1b",
            "market_cap_quote_total_supply",
            "token_supply_curve_economic_ui",
            "token_supply_protocol_constant_ui",
            "token_supply_selected_for_market_cap",
            "token_supply_selected_for_curve_progress",
            "supply_denominator_policy_version",
            "market_cap_confidence",
            "curve_complete_flag",
            "curve_progress_pct",
            "curve_progress_confidence",
            "curve_completion_pct",
            "curve_staleness_ms",
            "quote_asset_specific_depth_score",
            "quote_asset_specific_curve_health",
        ] {
            registry.register_core(
                feature_id,
                "curve",
                feature_id,
                &["bonding_curve_state"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "return_pct_30s",
            "return_pct_1m",
            "return_pct_5m",
            "price_high",
            "price_low",
            "drawdown_from_ath_pct",
            "drawdown_from_launch_pct",
            "realized_volatility_30s",
            "realized_volatility_1m",
            "price_velocity_30s",
            "time_to_ath_ms",
            "time_below_launch_price_pct",
            "new_ath_frequency",
            "post_ath_selloff_velocity",
        ] {
            registry.register_core(
                feature_id,
                "price_path",
                feature_id,
                &["trade_history"],
                "windowed",
            );
        }

        for feature_id in [
            "buy_count",
            "sell_count",
            "trade_count",
            "unique_buyers",
            "unique_sellers",
            "buy_volume_quote",
            "sell_volume_quote",
            "net_flow_quote",
            "buy_sell_count_ratio",
            "buy_sell_volume_ratio",
            "largest_buy_quote",
            "largest_sell_quote",
            "median_buy_size_quote",
            "median_sell_size_quote",
            "p90_buy_size_quote",
            "p90_sell_size_quote",
            "first_10_trades_buy_ratio",
            "no_buy_gap_longest_ms",
            "organic_flow_score",
            "toxic_flow_score",
        ] {
            registry.register_core(
                feature_id,
                "trade_flow",
                feature_id,
                &["trade_history"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "holder_count_current",
            "holder_count_owner_wallets",
            "holder_count_excluding_curve",
            "holder_count_excluding_curve_and_dev",
            "observed_holder_supply",
            "circulating_holder_supply",
            "holder_growth_rate",
            "holders_with_nonzero_balance",
            "average_holder_balance",
            "median_holder_balance",
            "p90_holder_balance",
            "holder_balance_gini",
            "holder_balance_hhi",
            "top1_holder_pct",
            "top1_holder_pct_observed",
            "top1_holder_pct_total_supply",
            "top1_holder_pct_curve_economic_supply",
            "top1_holder_pct_circulating",
            "top3_holder_pct",
            "top5_holder_pct",
            "top10_holder_pct",
            "top20_holder_pct",
            "holder_distribution_improvement_score",
            "sustained_holder_growth_score",
            "holder_metric_confidence",
            "missing_owner_mapping_count",
            "holder_updates_seen",
            "holder_updates_applied",
            "holder_updates_deduped",
            "holder_owner_changes",
            "holder_missing_owner_mapping",
            "holder_fallback_trade_updates_used",
        ] {
            registry.register_core(
                feature_id,
                "holders",
                feature_id,
                &["holder_state"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "whale_count",
            "whale_holding_pct",
            "top1_is_creator",
            "concentration_risk_score",
            "one_wallet_controls_market_flag",
        ] {
            registry.register_core(
                feature_id,
                "top_holders",
                feature_id,
                &["holder_state", "developer_state"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "same_slot_multi_buy_count",
            "first_n_buys_identical_amount_count",
            "first_n_buys_same_funder_count",
            "bundle_wallets_top_holder_pct",
            "bundle_risk_score",
            "bundle_confidence",
        ] {
            registry.register_core(
                feature_id,
                "bundle",
                feature_id,
                &["trade_history", "funding_graph", "cluster_index"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "dev_sold_flag",
            "dev_sold_pct",
            "price_drop_50pct_from_ath",
            "price_drop_70pct_from_launch_within_5m",
            "holder_growth_stalled",
            "sells_dominate_after_launch",
            "no_buys_for_x_seconds",
            "rug_probability_score",
            "rug_confidence",
        ] {
            registry.register_core(
                feature_id,
                "rug",
                feature_id,
                &["developer_state", "trade_history", "holder_state"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "cohort_launch_count",
            "cohort_active_count",
            "cohort_discard_rate",
            "cohort_rug_rate",
            "token_rank_by_buy_volume_30s",
            "token_rank_by_net_flow_30s",
            "token_rank_by_holder_growth_30s",
            "token_rank_by_price_return_30s",
            "token_percentile_buy_volume_30s",
            "token_percentile_net_flow_30s",
            "token_percentile_holder_growth_1m",
            "token_percentile_price_return_1m",
            "token_relative_strength_score",
            "token_relative_weakness_score",
            "cohort_attention_share",
            "this_token_share_of_all_launch_buy_volume",
            "this_token_share_of_all_unique_buyers",
            "token_is_cohort_leader_flag",
            "token_is_cohort_laggard_flag",
            "token_improving_rank_velocity",
            "token_losing_rank_velocity",
            "best_launch_in_cohort_distance",
        ] {
            registry.register_core(
                feature_id,
                "cohort",
                feature_id,
                &["state_snapshot"],
                "same_30s_cohort",
            );
        }

        for feature_id in [
            "wallets_sold_other_token_then_bought_this_count",
            "net_capital_rotated_into_this_from_other_launches",
            "smart_capital_rotation_score",
            "this_token_becoming_primary_capital_destination_flag",
        ] {
            registry.register_core(
                feature_id,
                "capital_rotation",
                feature_id,
                &["wallet_index"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "holder_unrealized_pnl_mean",
            "holder_unrealized_pnl_median",
            "holder_unrealized_pnl_p90",
            "unrealized_profit_supply_pct",
            "underwater_supply_pct",
            "breakeven_supply_pct",
            "free_rolling_supply_pct",
            "top5_unrealized_pnl",
            "top10_unrealized_pnl",
            "profit_overhang_score",
            "underwater_capitulation_risk",
            "free_roll_dump_risk_score",
            "realized_profit_taking_rate",
            "holders_taking_initials_out_count",
            "holders_fully_exiting_count",
        ] {
            registry.register_core(
                feature_id,
                "cost_basis",
                feature_id,
                &["holder_state"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "large_sell_count",
            "large_sell_total_quote",
            "price_impact_of_large_sells",
            "recovery_after_large_sell_ms",
            "recovery_after_large_sell_pct",
            "buyers_after_large_sell_count",
            "unique_buyers_after_large_sell",
            "buy_volume_after_large_sell",
            "holder_count_after_large_sell",
            "absorption_success_score",
            "absorption_failure_score",
            "repeated_absorption_count",
            "distribution_into_buyers_score",
            "price_flat_volume_high_distribution_warning",
            "vertical_move_without_holder_growth",
            "vertical_move_without_unique_buyers",
            "price_up_top_holders_selling",
            "price_up_bundle_selling",
            "price_up_dev_selling",
            "price_up_unique_buyers_down",
            "price_up_holder_count_down",
            "price_up_buy_size_concentration_high",
            "price_up_only_one_or_two_large_buys",
            "price_up_no_retail_breadth",
            "momentum_authenticity_score",
            "exit_liquidity_trap_score",
            "dev_not_selling_while_price_up",
            "organic_survival_score",
            "anti_rug_confidence_score",
        ] {
            registry.register_core(
                feature_id,
                "survival",
                feature_id,
                &["trade_history", "holder_state", "developer_state"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "holder_age_mean",
            "holder_age_median",
            "holder_age_weighted_by_balance",
            "new_holder_survival_30s",
            "new_holder_survival_1m",
            "first_10_buyers_retention",
            "first_20_buyers_retention",
            "average_time_to_first_sell_by_holder",
            "holder_half_life",
            "holder_decay_rate",
            "holder_stickiness_score",
            "holder_paper_hands_score",
            "healthy_churn_score",
            "unhealthy_churn_score",
            "new_holders_replacing_old_holders_score",
        ] {
            registry.register_core(
                feature_id,
                "holder_lifecycle",
                feature_id,
                &["holder_state", "trade_history"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "tx_account_count",
            "tx_instruction_count",
            "tx_inner_instruction_count",
            "compute_budget_instruction_present",
            "compute_unit_limit",
            "compute_unit_price",
            "compute_unit_limit_bucket",
            "compute_unit_price_bucket",
            "duplicate_compute_profile_count",
            "duplicate_compute_profile_among_first_buyers",
            "duplicate_instruction_sequence_count",
            "duplicate_account_pattern_count",
            "identical_client_fingerprint_score",
            "first_buyers_same_client_fingerprint_pct",
            "top_holders_same_client_fingerprint_pct",
            "bot_family_dominance_score",
            "bot_family_diversity_score",
        ] {
            registry.register_core(
                feature_id,
                "transaction_fingerprint",
                feature_id,
                &["trade_history", "observed_transactions"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "creator_recent_funder",
            "buyer_recent_funder_overlap_count",
            "top_holders_same_funder_pct",
            "first_buyers_same_funder_pct",
            "funder_to_creator_edge_age",
            "funder_to_buyer_edge_age",
            "funder_to_multiple_launches_count",
            "same_funder_same_amount_pattern",
            "funding_burst_before_launch",
            "wallets_funded_just_before_buy_count",
            "fresh_wallets_funded_by_known_factory",
            "funding_graph_density",
            "funding_graph_suspicion_score",
            "funding_graph_quality_score",
        ] {
            registry.register_core(
                feature_id,
                "funding_graph",
                feature_id,
                &["funding_graph", "wallet_index"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "launch_priority_fee_median",
            "launch_priority_fee_p90",
            "pump_buy_priority_fee_median",
            "pump_sell_priority_fee_median",
            "first_buyers_priority_fee_median",
            "priority_fee_spike_near_launch",
            "our_required_priority_fee_estimate",
            "fee_war_score",
            "edge_minus_fee_war_cost",
            "high_fee_low_edge_warning",
            "minimum_move_to_cover_observed_fees",
        ] {
            registry.register_core(
                feature_id,
                "fee_competition",
                feature_id,
                &["trade_history"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "first_seen_by_shred_ns",
            "shred_lead_time_ms",
            "token_signal_available_before_geyser_flag",
            "slot_id",
            "launch_priority_fee_median",
            "competition_intensity_score",
            "counterfactual_buy_after_100ms_pnl",
            "latency_adjusted_trade_eligibility",
        ] {
            registry.register_core(
                feature_id,
                "execution",
                feature_id,
                &["event_meta", "trade_history"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "geyser_connected_flag",
            "shred_connected_flag",
            "feature_completeness_pct",
            "critical_feature_missing_count",
            "source_disagreement_flag",
            "data_quality_score",
            "trade_allowed_data_quality_flag",
        ] {
            registry.register_core(
                feature_id,
                "data_quality",
                feature_id,
                &["token_state"],
                "launch_to_now",
            );
        }

        for feature_id in [
            "tentative_sell_count_window",
            "tentative_sell_volume_quote_window",
            "tentative_sell_from_dev_count",
            "tentative_sell_from_top_holder_count",
            "tentative_sell_from_bundle_count",
            "tentative_sell_from_whale_count",
            "tentative_sell_same_slot_cluster_count",
            "tentative_sell_impact_max_pct",
            "tentative_sell_impact_sum_pct",
            "tentative_sell_confidence_max",
            "tentative_sell_confidence_mean",
            "shred_sell_warning_level",
            "shred_exit_armed_flag",
            "shred_emergency_exit_triggered_flag",
            "shred_to_geyser_processed_ms",
            "shred_to_account_effect_confirmation_ms",
            "shred_to_rooted_confirmation_ms",
            "shred_sell_false_positive_rate_wallet",
            "shred_sell_false_positive_rate_source",
            "shred_sell_saved_loss_estimate",
            "shred_sell_saved_loss_realized",
            "shred_exit_opportunity_cost",
            "shred_signal_stale_flag",
            "shred_exit_latency_budget_ms",
            "shred_exit_latency_used_ms",
            "shred_exit_edge_score",
            "malicious_sell_intent_score",
            "preconfirmation_exit_confidence",
            "dangerous_seller_precomputed_impact_score",
            "exit_threat_index_score",
        ] {
            registry.register_core(
                feature_id,
                "shred_exit_defense",
                feature_id,
                &["shred_defense_state"],
                "launch_to_now",
            );
        }

        registry.register_placeholder_group(
            "wallet_embeddings",
            &[
                "wallet_behavior_vector",
                "wallet_risk_vector",
                "wallet_alpha_vector",
                "wallet_toxicity_vector",
                "buyer_cohort_similarity_to_past_winners",
            ],
        );
        registry.register_placeholder_group(
            "factories_and_fingerprints",
            &[
                "creator_launch_factory_score",
                "same_launch_instruction_shape_score",
                "same_compute_budget_pattern_score",
                "client_fingerprint_cluster_id",
                "client_fingerprint_historical_win_rate",
            ],
        );
        registry.register_placeholder_group(
            "event_grammar",
            &[
                "first_10_event_sequence",
                "event_bigram_counts",
                "event_sequence_similarity_to_winners",
                "motif_id",
                "motif_historical_expected_pnl",
            ],
        );
        registry.register_placeholder_group(
            "path_similarity",
            &[
                "price_path_shape_id",
                "combined_multivariate_path_similarity",
                "dynamic_time_warp_distance_price_to_winner_templates",
                "higher_low_accumulation_score",
            ],
        );
        registry.register_placeholder_group(
            "hazard_and_anomaly",
            &[
                "hazard_rug_next_10s",
                "survival_probability_30s",
                "expected_safe_hold_time",
                "anomaly_score_positive",
                "anomaly_requires_deep_tracking_flag",
            ],
        );

        registry
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureSnapshot {
    pub mint: PubkeyValue,
    pub lifecycle: TokenLifecycle,
    pub observed_at: OffsetDateTime,
    pub values: BTreeMap<String, FeatureValue>,
    pub vector_hash: String,
    pub available_count: usize,
    pub missing_count: usize,
    pub unavailable_count: usize,
}

impl FeatureSnapshot {
    pub fn value(&self, feature_id: &str) -> Option<&FeatureValue> {
        self.values.get(feature_id)
    }

    pub fn decimal(&self, feature_id: &str) -> Option<Decimal> {
        self.value(feature_id).and_then(FeatureValue::as_decimal)
    }
}

#[derive(Debug, Clone)]
pub struct FeatureEngine {
    registry: FeatureRegistry,
}

pub trait FeatureStateView {
    fn tokens(&self) -> &HashMap<String, TokenState>;
    fn wallets(&self) -> &HashMap<String, WalletSummary>;
    fn funding_graph(&self) -> &FundingGraph;
    fn cluster_index(&self) -> &ClusterIndex;
}

impl FeatureStateView for StateSnapshot {
    fn tokens(&self) -> &HashMap<String, TokenState> {
        &self.tokens
    }

    fn wallets(&self) -> &HashMap<String, WalletSummary> {
        &self.wallets
    }

    fn funding_graph(&self) -> &FundingGraph {
        &self.funding_graph
    }

    fn cluster_index(&self) -> &ClusterIndex {
        &self.cluster_index
    }
}

impl FeatureStateView for StateEngine {
    fn tokens(&self) -> &HashMap<String, TokenState> {
        self.tokens()
    }

    fn wallets(&self) -> &HashMap<String, WalletSummary> {
        self.wallets()
    }

    fn funding_graph(&self) -> &FundingGraph {
        self.funding_graph()
    }

    fn cluster_index(&self) -> &ClusterIndex {
        self.cluster_index()
    }
}

impl Default for FeatureEngine {
    fn default() -> Self {
        Self {
            registry: FeatureRegistry::default(),
        }
    }
}

impl FeatureEngine {
    pub fn new(registry: FeatureRegistry) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> &FeatureRegistry {
        &self.registry
    }

    pub fn compute_snapshot(
        &self,
        token: &TokenState,
        state: &dyn FeatureStateView,
        observed_at: OffsetDateTime,
    ) -> FeatureSnapshot {
        self.compute_snapshot_profiled(token, state, observed_at).0
    }

    pub fn compute_snapshot_profiled(
        &self,
        token: &TokenState,
        state: &dyn FeatureStateView,
        observed_at: OffsetDateTime,
    ) -> (FeatureSnapshot, BTreeMap<String, u128>) {
        let mut timings = BTreeMap::new();
        let mut values = BTreeMap::new();
        for (feature_id, descriptor) in self.registry.descriptors() {
            let value = match descriptor.default_status {
                FeatureStatus::Available | FeatureStatus::Missing => {
                    FeatureValue::missing(feature_id, "not yet computed")
                }
                FeatureStatus::Unavailable => {
                    FeatureValue::unavailable(feature_id, "feature family not implemented yet")
                }
            };
            values.insert(feature_id.clone(), value);
        }

        compute_launch_identity(token, state, observed_at, &mut values);
        compute_creator_features(token, state, observed_at, &mut values);
        compute_dev_holdings(token, observed_at, &mut values);
        compute_curve_features(token, observed_at, &mut values);
        compute_price_path_features(token, observed_at, &mut values);
        compute_trade_flow_features(token, observed_at, &mut values);
        let holder_started = Instant::now();
        compute_holder_features(token, observed_at, &mut values);
        compute_top_holder_features(token, &mut values);
        timings.insert(
            "holder_feature_compute".to_owned(),
            holder_started.elapsed().as_millis(),
        );
        compute_bundle_features(token, state, &mut values);
        compute_rug_features(token, observed_at, &mut values);
        let cohort_started = Instant::now();
        compute_cohort_features(token, state, observed_at, &mut values);
        compute_rotation_features(token, state, &mut values);
        timings.insert(
            "cohort_relative_feature_compute".to_owned(),
            cohort_started.elapsed().as_millis(),
        );
        compute_cost_basis_features(token, &mut values);
        compute_survival_features(token, observed_at, &mut values);
        compute_execution_features(token, &mut values);
        let holder_lifecycle_started = Instant::now();
        compute_holder_lifecycle_features(token, state, observed_at, &mut values);
        *timings
            .entry("holder_feature_compute".to_owned())
            .or_default() += holder_lifecycle_started.elapsed().as_millis();
        compute_transaction_fingerprint_features(token, &mut values);
        compute_funding_graph_features(token, state, observed_at, &mut values);
        compute_fee_competition_features(token, &mut values);
        compute_shred_exit_defense_features(token, &mut values);
        finalize_data_quality_features(token, &mut values);

        let available_count = values
            .values()
            .filter(|feature| feature.status == FeatureStatus::Available)
            .count();
        let missing_count = values
            .values()
            .filter(|feature| feature.status == FeatureStatus::Missing)
            .count();
        let unavailable_count = values
            .values()
            .filter(|feature| feature.status == FeatureStatus::Unavailable)
            .count();

        let mut hasher = Sha256::new();
        for (feature_id, feature) in &values {
            hasher.update(feature_id.as_bytes());
            hasher.update(format!("{:?}", feature.status).as_bytes());
            if let Some(value) = &feature.value {
                match value {
                    FeatureDatum::Numeric(value) => {
                        hasher.update(value.normalize().to_string().as_bytes())
                    }
                    FeatureDatum::Text(value) => hasher.update(value.as_bytes()),
                    FeatureDatum::Boolean(value) => hasher.update([*value as u8]),
                }
            }
        }
        let vector_hash = format!("{:x}", hasher.finalize());

        (
            FeatureSnapshot {
                mint: token.mint.clone(),
                lifecycle: token.lifecycle,
                observed_at,
                values,
                vector_hash,
                available_count,
                missing_count,
                unavailable_count,
            },
            timings,
        )
    }
}

fn set_numeric(
    values: &mut BTreeMap<String, FeatureValue>,
    feature_id: &str,
    value: Decimal,
    confidence: Decimal,
) {
    if let Some(feature) = values.get_mut(feature_id) {
        *feature = FeatureValue::available(feature_id, FeatureDatum::Numeric(value), confidence);
    }
}

fn set_unavailable(
    values: &mut BTreeMap<String, FeatureValue>,
    feature_id: &str,
    notes: impl Into<String>,
) {
    if let Some(feature) = values.get_mut(feature_id) {
        *feature = FeatureValue::unavailable(feature_id, notes);
    }
}

fn set_bool(
    values: &mut BTreeMap<String, FeatureValue>,
    feature_id: &str,
    value: bool,
    confidence: Decimal,
) {
    if let Some(feature) = values.get_mut(feature_id) {
        *feature = FeatureValue::available(feature_id, FeatureDatum::Boolean(value), confidence);
    }
}

fn set_text(
    values: &mut BTreeMap<String, FeatureValue>,
    feature_id: &str,
    value: impl Into<String>,
    confidence: Decimal,
) {
    if let Some(feature) = values.get_mut(feature_id) {
        *feature =
            FeatureValue::available(feature_id, FeatureDatum::Text(value.into()), confidence);
    }
}

fn compute_launch_identity(
    token: &TokenState,
    state: &dyn FeatureStateView,
    observed_at: OffsetDateTime,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    if let Some(slot) = token.launch_slot {
        set_numeric(values, "launch_slot", Decimal::from(slot), Decimal::ONE);
    }
    if let Some(launch_time) = token.launch_time {
        set_numeric(
            values,
            "time_since_launch_ms",
            Decimal::from((observed_at - launch_time).whole_milliseconds().max(0) as u64),
            Decimal::ONE,
        );
    }
    set_text(values, "mint", token.mint.0.clone(), Decimal::ONE);
    if let Some(quote_mint) = &token.quote_mint {
        set_text(values, "quote_mint", quote_mint.0.clone(), Decimal::ONE);
    }
    set_text(
        values,
        "quote_asset_type",
        format!("{:?}", token.quote_asset_type).to_lowercase(),
        Decimal::ONE,
    );
    if let Some(creator) = &token.creator {
        set_text(values, "creator_wallet", creator.0.clone(), Decimal::ONE);
    }
    if let Some(payer) = &token.payer {
        set_text(values, "payer_wallet", payer.0.clone(), Decimal::ONE);
    }
    if let Some(bonding_curve) = &token.bonding_curve {
        set_text(
            values,
            "bonding_curve_pubkey",
            bonding_curve.0.clone(),
            Decimal::ONE,
        );
    }
    if let Some(associated) = &token.associated_bonding_curve {
        set_text(
            values,
            "associated_bonding_curve_pubkey",
            associated.0.clone(),
            Decimal::ONE,
        );
    }
    if let Some(metadata) = &token.metadata {
        set_text(values, "metadata_pubkey", metadata.0.clone(), Decimal::ONE);
    }
    set_text(
        values,
        "token_program_type",
        format!("{:?}", token.token_program).to_lowercase(),
        Decimal::ONE,
    );
    set_text(
        values,
        "create_instruction_variant",
        token.create_instruction_variant.clone(),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "name_length",
        Decimal::from(token.name.chars().count() as u64),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "symbol_length",
        Decimal::from(token.symbol.chars().count() as u64),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "uri_length",
        Decimal::from(token.uri.chars().count() as u64),
        Decimal::ONE,
    );
    set_bool(
        values,
        "has_empty_name",
        token.name.is_empty(),
        Decimal::ONE,
    );
    set_bool(
        values,
        "has_empty_symbol",
        token.symbol.is_empty(),
        Decimal::ONE,
    );
    set_bool(values, "has_empty_uri", token.uri.is_empty(), Decimal::ONE);
    set_numeric(
        values,
        "name_entropy",
        unique_char_ratio(&token.name),
        dec!(0.8),
    );
    set_numeric(
        values,
        "symbol_entropy",
        unique_char_ratio(&token.symbol),
        dec!(0.8),
    );
    set_numeric(
        values,
        "uri_entropy",
        unique_char_ratio(&token.uri),
        dec!(0.8),
    );
    set_numeric(
        values,
        "suspicious_unicode_score",
        suspicious_unicode_ratio(&token.name)
            .max(suspicious_unicode_ratio(&token.symbol))
            .max(suspicious_unicode_ratio(&token.uri)),
        dec!(0.8),
    );
    set_numeric(
        values,
        "duplicate_name_seen_historically",
        Decimal::from(
            state
                .tokens()
                .values()
                .filter(|other| {
                    other.mint != token.mint && !other.name.is_empty() && other.name == token.name
                })
                .count() as u64,
        ),
        dec!(0.9),
    );
    set_numeric(
        values,
        "duplicate_symbol_seen_historically",
        Decimal::from(
            state
                .tokens()
                .values()
                .filter(|other| {
                    other.mint != token.mint
                        && !other.symbol.is_empty()
                        && other.symbol == token.symbol
                })
                .count() as u64,
        ),
        dec!(0.9),
    );
    set_numeric(
        values,
        "duplicate_uri_seen_historically",
        Decimal::from(
            state
                .tokens()
                .values()
                .filter(|other| {
                    other.mint != token.mint && !other.uri.is_empty() && other.uri == token.uri
                })
                .count() as u64,
        ),
        dec!(0.9),
    );
    set_text(
        values,
        "uri_scheme",
        parse_uri_scheme(&token.uri),
        dec!(0.8),
    );
    set_numeric(
        values,
        "launch_same_tx_buy_count",
        Decimal::from(token.launch_same_transaction_buys),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "launch_same_slot_buy_count",
        Decimal::from(token.launch_same_slot_buys),
        Decimal::ONE,
    );
    set_text(
        values,
        "first_seen_source",
        format!("{:?}", token.first_seen_source).to_lowercase(),
        Decimal::ONE,
    );
    if let Some(fingerprint) = &token.launch_transaction_fingerprint {
        set_text(
            values,
            "launch_tx_client_fingerprint",
            fingerprint.clone(),
            dec!(0.9),
        );
    }
}

fn compute_creator_features(
    token: &TokenState,
    state: &dyn FeatureStateView,
    observed_at: OffsetDateTime,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    let Some(creator) = &token.creator else {
        return;
    };
    if let Some(wallet) = state.wallets().get(&creator.0) {
        if let Some(first_seen) = wallet.first_seen {
            set_numeric(
                values,
                "creator_age_in_local_index",
                Decimal::from((observed_at - first_seen).whole_seconds().max(0) as u64),
                dec!(0.9),
            );
        }
        set_numeric(
            values,
            "creator_total_launches_seen",
            Decimal::from(wallet.creator_launches),
            dec!(0.9),
        );
    }

    let launches_last_hour = state
        .tokens()
        .values()
        .filter(|other| other.creator.as_ref() == Some(creator))
        .filter_map(|other| other.launch_time)
        .filter(|launch_time| *launch_time >= observed_at - Duration::hours(1))
        .count();
    set_numeric(
        values,
        "creator_launches_last_1h",
        Decimal::from(launches_last_hour as u64),
        dec!(0.9),
    );

    if let Some(cluster_id) = state.cluster_index().cluster_id_for(&creator.0) {
        set_text(values, "creator_cluster_id", cluster_id, dec!(0.8));
    }

    let same_payer_reuse = state
        .tokens()
        .values()
        .filter(|other| other.mint != token.mint)
        .filter(|other| other.creator.as_ref() == Some(creator))
        .filter(|other| other.payer == token.payer)
        .count();
    set_bool(
        values,
        "creator_reuses_same_payer",
        same_payer_reuse > 0,
        dec!(0.8),
    );

    let same_uri_domain = if let Some(domain) = uri_host(&token.uri) {
        state
            .tokens()
            .values()
            .filter(|other| other.mint != token.mint)
            .any(|other| uri_host(&other.uri).as_deref() == Some(domain.as_str()))
    } else {
        false
    };
    set_bool(
        values,
        "creator_reuses_same_metadata_domain",
        same_uri_domain,
        dec!(0.8),
    );
    set_bool(
        values,
        "creator_launching_spam_wave_flag",
        launches_last_hour >= 3,
        dec!(0.8),
    );
}

fn compute_dev_holdings(
    token: &TokenState,
    observed_at: OffsetDateTime,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    let dev = &token.developer_state;
    let token_decimals = token.reserve_state.token_decimals;
    let holder_state_dev_balance_raw = token
        .creator
        .as_ref()
        .and_then(|creator| token.holder_state.owner_balances.get(&creator.0))
        .map(|holder| holder.balance.max(Decimal::ZERO));
    let dev_balance_raw =
        holder_state_dev_balance_raw.unwrap_or_else(|| dev.creator_net_tokens.max(Decimal::ZERO));
    let dev_balance_source = if holder_state_dev_balance_raw.is_some() {
        "holder_state.owner_balance"
    } else {
        "developer_state.creator_net_tokens_fallback"
    };
    let (dev_balance_metric, total_pct_metric, circulating_pct_metric) =
        MetricEngine::dev_holding_metrics(
            Some(dev_balance_raw),
            token_decimals,
            dev_balance_source,
        );
    set_numeric(
        values,
        "creator_initial_token_balance",
        raw_tokens_to_ui(dev.creator_initial_holding, token_decimals),
        Decimal::ONE,
    );
    if let Some(dev_balance_ui) = metric_decimal(&dev_balance_metric) {
        set_numeric(
            values,
            "dev_balance",
            dev_balance_ui,
            if holder_state_dev_balance_raw.is_some()
                && dev_balance_metric.confidence == MetricConfidence::Verified
            {
                Decimal::ONE
            } else {
                dec!(0.45)
            },
        );
    } else {
        set_unavailable(
            values,
            "dev_balance",
            dev_balance_metric
                .unavailable_reason
                .clone()
                .unwrap_or_else(|| "dev balance unavailable".to_owned()),
        );
    }
    if let Some(total_pct) = metric_decimal(&total_pct_metric) {
        let confidence = if holder_state_dev_balance_raw.is_some()
            && total_pct_metric.confidence == MetricConfidence::Verified
        {
            Decimal::ONE
        } else {
            dec!(0.45)
        };
        set_numeric(
            values,
            "dev_holding_pct_total_supply",
            total_pct,
            confidence,
        );
        set_numeric(
            values,
            "dev_holding_pct_curve_economic_supply",
            total_pct,
            confidence,
        );
        set_text(
            values,
            "token_supply_selected_for_holder_pct",
            "token_supply_curve_economic_or_protocol_constant",
            Decimal::ONE,
        );
        set_numeric(values, "creator_ownership_pct", total_pct, confidence);
    } else {
        let reason = total_pct_metric
            .unavailable_reason
            .clone()
            .unwrap_or_else(|| "dev total-supply denominator unavailable".to_owned());
        set_unavailable(values, "dev_holding_pct_total_supply", reason.clone());
        set_unavailable(
            values,
            "dev_holding_pct_curve_economic_supply",
            reason.clone(),
        );
        set_unavailable(
            values,
            "token_supply_selected_for_holder_pct",
            reason.clone(),
        );
        set_unavailable(values, "creator_ownership_pct", reason);
    }
    if let Some(circulating_pct) = metric_decimal(&circulating_pct_metric) {
        set_numeric(
            values,
            "dev_holding_pct_circulating",
            circulating_pct,
            if holder_state_dev_balance_raw.is_some() {
                dec!(0.8)
            } else {
                dec!(0.35)
            },
        );
    } else {
        set_unavailable(
            values,
            "dev_holding_pct_circulating",
            circulating_pct_metric
                .unavailable_reason
                .clone()
                .unwrap_or_else(|| "dev circulating denominator unavailable".to_owned()),
        );
    }
    let creator_initial_holding_ui = raw_tokens_to_ui(dev.creator_initial_holding, token_decimals);
    if creator_initial_holding_ui > Decimal::ZERO {
        if let Some(dev_balance_ui) = metric_decimal(&dev_balance_metric) {
            set_numeric(
                values,
                "creator_retained_supply_pct",
                clamp01(dev_balance_ui / creator_initial_holding_ui),
                if holder_state_dev_balance_raw.is_some() {
                    Decimal::ONE
                } else {
                    dec!(0.45)
                },
            );
        } else {
            set_unavailable(
                values,
                "creator_retained_supply_pct",
                "dev balance unavailable for retained-supply calculation",
            );
        }
    }

    for (feature_id, threshold) in [
        ("creator_net_tokens_after_10s", Duration::seconds(10)),
        ("creator_net_tokens_after_30s", Duration::seconds(30)),
        ("creator_net_tokens_after_1m", Duration::minutes(1)),
        ("creator_net_tokens_after_5m", Duration::minutes(5)),
    ] {
        if token
            .launch_time
            .map(|launch_time| observed_at >= launch_time + threshold)
            .unwrap_or(false)
        {
            set_numeric(values, feature_id, dev.creator_net_tokens, Decimal::ONE);
        }
    }
    for (feature_id, threshold) in [
        ("creator_net_quote_flow_after_10s", Duration::seconds(10)),
        ("creator_net_quote_flow_after_30s", Duration::seconds(30)),
        ("creator_net_quote_flow_after_1m", Duration::minutes(1)),
        ("creator_net_quote_flow_after_5m", Duration::minutes(5)),
    ] {
        if token
            .launch_time
            .map(|launch_time| observed_at >= launch_time + threshold)
            .unwrap_or(false)
        {
            set_numeric(values, feature_id, dev.creator_net_quote_flow, Decimal::ONE);
        }
    }
    set_bool(
        values,
        "creator_has_sold",
        dev.creator_first_sell_time.is_some(),
        Decimal::ONE,
    );
    if let (Some(first_sell), Some(launch_time)) = (dev.creator_first_sell_time, token.launch_time)
    {
        set_numeric(
            values,
            "creator_first_sell_time_ms",
            Decimal::from((first_sell - launch_time).whole_milliseconds().max(0) as u64),
            Decimal::ONE,
        );
    }
    set_numeric(
        values,
        "creator_dump_velocity",
        dev.creator_sell_percentage,
        dec!(0.9),
    );
    if let Some(rank) = dev.creator_current_rank {
        set_numeric(
            values,
            "creator_balance_rank",
            Decimal::from(rank as u64),
            Decimal::ONE,
        );
        set_bool(values, "creator_is_top1_holder", rank <= 1, Decimal::ONE);
        set_bool(values, "creator_is_top3_holder", rank <= 3, Decimal::ONE);
        set_bool(values, "creator_is_top5_holder", rank <= 5, Decimal::ONE);
        set_bool(values, "creator_is_top10_holder", rank <= 10, Decimal::ONE);
    } else {
        set_bool(values, "creator_is_top1_holder", false, dec!(0.8));
        set_bool(values, "creator_is_top3_holder", false, dec!(0.8));
        set_bool(values, "creator_is_top5_holder", false, dec!(0.8));
        set_bool(values, "creator_is_top10_holder", false, dec!(0.8));
    }
}

fn compute_curve_features(
    token: &TokenState,
    observed_at: OffsetDateTime,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    let curve = &token.reserve_state;
    set_numeric(
        values,
        "virtual_quote_reserves",
        curve.virtual_quote_reserves,
        Decimal::ONE,
    );
    set_numeric(
        values,
        "virtual_token_reserves",
        curve.virtual_token_reserves,
        Decimal::ONE,
    );
    set_numeric(
        values,
        "real_quote_reserves",
        curve.real_quote_reserves,
        Decimal::ONE,
    );
    set_numeric(
        values,
        "real_token_reserves",
        curve.real_token_reserves,
        Decimal::ONE,
    );
    set_numeric(
        values,
        "token_decimals",
        Decimal::from(curve.token_decimals),
        Decimal::ONE,
    );
    if let Some(price) = curve.price_lamports_per_raw_token {
        set_numeric(values, "price_lamports_per_raw_token", price, Decimal::ONE);
    }
    if let Some(price) = curve.price_sol_per_token {
        set_numeric(
            values,
            "price_sol_per_token",
            price,
            curve.reserve_price_confidence,
        );
    }
    set_numeric(
        values,
        "reserve_price_confidence",
        curve.reserve_price_confidence,
        Decimal::ONE,
    );
    set_numeric(
        values,
        "price_current",
        curve.price_sol_per_token.unwrap_or(token.latest_price),
        curve.reserve_price_confidence.max(dec!(0.5)),
    );
    if let Some(launch_price) = curve.launch_price {
        set_numeric(values, "price_launch", launch_price, Decimal::ONE);
        if launch_price > Decimal::ZERO {
            set_numeric(
                values,
                "price_change_from_launch_pct",
                (token.latest_price - launch_price) / launch_price,
                dec!(0.9),
            );
        }
    }
    if let Some(market_cap) = curve.market_cap_quote_1b {
        set_numeric(
            values,
            "market_cap_quote_1b",
            market_cap,
            curve.market_cap_confidence,
        );
    }
    if let Some(market_cap) = curve.market_cap_quote_total_supply {
        set_numeric(
            values,
            "market_cap_quote_total_supply",
            market_cap,
            curve.market_cap_confidence,
        );
    } else if let Some(price) = curve.price_sol_per_token {
        set_numeric(
            values,
            "market_cap_quote_total_supply",
            price * Decimal::from(PUMP_TOTAL_SUPPLY_UI),
            curve.market_cap_confidence.max(dec!(0.8)),
        );
    }
    set_numeric(
        values,
        "token_supply_curve_economic_ui",
        Decimal::from(PUMP_TOTAL_SUPPLY_UI),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "token_supply_protocol_constant_ui",
        Decimal::from(PUMP_TOTAL_SUPPLY_UI),
        Decimal::ONE,
    );
    set_text(
        values,
        "token_supply_selected_for_market_cap",
        "token_supply_curve_economic_or_protocol_constant",
        Decimal::ONE,
    );
    set_text(
        values,
        "token_supply_selected_for_curve_progress",
        "bonding_curve_real_reserves_and_curve_economic_supply",
        Decimal::ONE,
    );
    set_text(
        values,
        "supply_denominator_policy_version",
        SUPPLY_DENOMINATOR_POLICY_VERSION,
        Decimal::ONE,
    );
    set_numeric(
        values,
        "market_cap_confidence",
        curve.market_cap_confidence,
        Decimal::ONE,
    );
    if let Some(complete) = curve.curve_complete_flag {
        set_bool(values, "curve_complete_flag", complete, Decimal::ONE);
    }
    if let Some(progress) = curve.curve_progress_pct {
        set_numeric(
            values,
            "curve_progress_pct",
            progress,
            curve.curve_progress_confidence,
        );
        set_numeric(
            values,
            "curve_completion_pct",
            progress,
            curve.curve_progress_confidence,
        );
    } else if let Some(completion) = curve.curve_completion_pct {
        set_numeric(values, "curve_completion_pct", completion, dec!(0.4));
    }
    set_numeric(
        values,
        "curve_progress_confidence",
        curve.curve_progress_confidence,
        Decimal::ONE,
    );
    if let Some(staleness_ms) = curve.staleness_ms(observed_at) {
        set_numeric(
            values,
            "curve_staleness_ms",
            Decimal::from(staleness_ms.max(0) as u64),
            dec!(0.9),
        );
    }
    let depth_score =
        clamp01((curve.real_quote_reserves + curve.virtual_quote_reserves) / Decimal::from(100u64));
    set_numeric(
        values,
        "quote_asset_specific_depth_score",
        depth_score,
        dec!(0.8),
    );
    let health = clamp01(
        depth_score
            * (Decimal::ONE - token.holder_state.top_holder_pct(1))
            * (Decimal::ONE - token.developer_state.creator_sell_percentage),
    );
    set_numeric(
        values,
        "quote_asset_specific_curve_health",
        health,
        dec!(0.8),
    );
}

fn compute_price_path_features(
    token: &TokenState,
    observed_at: OffsetDateTime,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    set_numeric(
        values,
        "price_high",
        token.trade_stats.all_time_high,
        Decimal::ONE,
    );
    set_numeric(
        values,
        "price_low",
        token.trade_stats.all_time_low,
        Decimal::ONE,
    );

    if token.trade_stats.all_time_high > Decimal::ZERO {
        set_numeric(
            values,
            "drawdown_from_ath_pct",
            (token.trade_stats.all_time_high - token.latest_price)
                / token.trade_stats.all_time_high,
            dec!(0.9),
        );
    }
    if let Some(launch_price) = token
        .reserve_state
        .launch_price
        .filter(|value| *value > Decimal::ZERO)
    {
        set_numeric(
            values,
            "drawdown_from_launch_pct",
            (launch_price - token.latest_price).max(Decimal::ZERO) / launch_price,
            dec!(0.9),
        );
    }
    for (feature_id, window) in [
        ("return_pct_30s", WINDOW_30S),
        ("return_pct_1m", WINDOW_1M),
        ("return_pct_5m", WINDOW_5M),
    ] {
        if let Some(return_pct) = window_return(token, observed_at, window) {
            set_numeric(values, feature_id, return_pct, dec!(0.9));
        }
    }
    for (feature_id, window) in [
        ("realized_volatility_30s", WINDOW_30S),
        ("realized_volatility_1m", WINDOW_1M),
    ] {
        if let Some(volatility) = window_volatility(token, observed_at, window) {
            set_numeric(values, feature_id, volatility, dec!(0.7));
        }
    }
    if let Some(velocity) = window_velocity(token, observed_at, WINDOW_30S) {
        set_numeric(values, "price_velocity_30s", velocity, dec!(0.8));
    }
    if let Some((launch_time, ath_time)) = token.launch_time.zip(time_of_ath(token)) {
        set_numeric(
            values,
            "time_to_ath_ms",
            Decimal::from((ath_time - launch_time).whole_milliseconds().max(0) as u64),
            dec!(0.8),
        );
    }
    if !token.trade_stats.price_history.is_empty() {
        let below_launch = token
            .reserve_state
            .launch_price
            .filter(|launch_price| *launch_price > Decimal::ZERO)
            .map(|launch_price| {
                let below_count = token
                    .trade_stats
                    .price_history
                    .iter()
                    .filter(|(_, price)| *price < launch_price)
                    .count();
                Decimal::from(below_count as u64)
                    / Decimal::from(token.trade_stats.price_history.len() as u64)
            })
            .unwrap_or(Decimal::ZERO);
        set_numeric(
            values,
            "time_below_launch_price_pct",
            below_launch,
            dec!(0.8),
        );
    }
    let ath_count = count_new_aths(token);
    set_numeric(
        values,
        "new_ath_frequency",
        Decimal::from(ath_count as u64),
        dec!(0.7),
    );
    if token.trade_stats.all_time_high > Decimal::ZERO && token.trade_stats.last_sell_at.is_some() {
        let selloff = (token.trade_stats.all_time_high - token.latest_price).max(Decimal::ZERO);
        set_numeric(values, "post_ath_selloff_velocity", selloff, dec!(0.7));
    }
}

fn compute_trade_flow_features(
    token: &TokenState,
    observed_at: OffsetDateTime,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    set_numeric(
        values,
        "buy_count",
        Decimal::from(token.trade_stats.buy_count),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "sell_count",
        Decimal::from(token.trade_stats.sell_count),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "trade_count",
        Decimal::from(token.trade_stats.buy_count + token.trade_stats.sell_count),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "unique_buyers",
        Decimal::from(token.trade_stats.unique_buyers.len() as u64),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "unique_sellers",
        Decimal::from(token.trade_stats.unique_sellers.len() as u64),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "buy_volume_quote",
        token.trade_stats.buy_volume_quote,
        Decimal::ONE,
    );
    set_numeric(
        values,
        "sell_volume_quote",
        token.trade_stats.sell_volume_quote,
        Decimal::ONE,
    );
    let net_flow = token.trade_stats.buy_volume_quote - token.trade_stats.sell_volume_quote;
    set_numeric(values, "net_flow_quote", net_flow, Decimal::ONE);
    if token.trade_stats.sell_count > 0 {
        set_numeric(
            values,
            "buy_sell_count_ratio",
            Decimal::from(token.trade_stats.buy_count)
                / Decimal::from(token.trade_stats.sell_count),
            dec!(0.9),
        );
    }
    if token.trade_stats.sell_volume_quote > Decimal::ZERO {
        set_numeric(
            values,
            "buy_sell_volume_ratio",
            token.trade_stats.buy_volume_quote / token.trade_stats.sell_volume_quote,
            dec!(0.9),
        );
    }
    let buys: Vec<Decimal> = token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| trade.side == state::TradeSide::Buy)
        .map(|trade| trade.quote)
        .collect();
    let sells: Vec<Decimal> = token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| trade.side == state::TradeSide::Sell)
        .map(|trade| trade.quote)
        .collect();
    if let Some(value) = max_decimal(&buys) {
        set_numeric(values, "largest_buy_quote", value, dec!(0.8));
    }
    if let Some(value) = max_decimal(&sells) {
        set_numeric(values, "largest_sell_quote", value, dec!(0.8));
    }
    if let Some(value) = median_decimal(&buys) {
        set_numeric(values, "median_buy_size_quote", value, dec!(0.8));
    }
    if let Some(value) = median_decimal(&sells) {
        set_numeric(values, "median_sell_size_quote", value, dec!(0.8));
    }
    if let Some(value) = percentile_decimal(&buys, 90) {
        set_numeric(values, "p90_buy_size_quote", value, dec!(0.8));
    }
    if let Some(value) = percentile_decimal(&sells, 90) {
        set_numeric(values, "p90_sell_size_quote", value, dec!(0.8));
    }
    let first_ten = token
        .trade_stats
        .trade_history
        .iter()
        .take(10)
        .collect::<Vec<_>>();
    if !first_ten.is_empty() {
        let buy_ratio = Decimal::from(
            first_ten
                .iter()
                .filter(|trade| trade.side == state::TradeSide::Buy)
                .count() as u64,
        ) / Decimal::from(first_ten.len() as u64);
        set_numeric(values, "first_10_trades_buy_ratio", buy_ratio, dec!(0.8));
    }
    set_numeric(
        values,
        "no_buy_gap_longest_ms",
        Decimal::from(token.trade_stats.longest_no_buy_gap_ms.max(0) as u64),
        dec!(0.9),
    );
    let breadth = if token.trade_stats.buy_count > 0 {
        Decimal::from(token.trade_stats.unique_buyers.len() as u64)
            / Decimal::from(token.trade_stats.buy_count)
    } else {
        Decimal::ZERO
    };
    let organic_score =
        clamp01(breadth * clamp01(Decimal::ONE - token.holder_state.top_holder_pct(5)));
    set_numeric(values, "organic_flow_score", organic_score, dec!(0.8));
    set_numeric(
        values,
        "toxic_flow_score",
        clamp01(Decimal::ONE - organic_score + token.developer_state.creator_sell_percentage),
        dec!(0.8),
    );

    let _ = observed_at;
}

fn compute_holder_features(
    token: &TokenState,
    observed_at: OffsetDateTime,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    set_numeric(
        values,
        "holder_count_current",
        Decimal::from(token.holder_state.nonzero_holder_count as u64),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "holders_with_nonzero_balance",
        Decimal::from(token.holder_state.nonzero_holder_count as u64),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "holder_count_owner_wallets",
        Decimal::from(token.holder_state.nonzero_holder_count as u64),
        Decimal::ONE,
    );
    let holder_counters = token.holder_state.counters;
    set_numeric(
        values,
        "holder_updates_seen",
        Decimal::from(holder_counters.holder_updates_seen),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "holder_updates_applied",
        Decimal::from(holder_counters.holder_updates_applied),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "holder_updates_deduped",
        Decimal::from(holder_counters.holder_updates_deduped),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "holder_owner_changes",
        Decimal::from(holder_counters.holder_owner_changes),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "holder_missing_owner_mapping",
        Decimal::from(holder_counters.holder_missing_owner_mapping),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "holder_fallback_trade_updates_used",
        Decimal::from(holder_counters.holder_fallback_trade_updates_used),
        Decimal::ONE,
    );
    let mut curve_exclusions = HashSet::<String>::new();
    if let Some(curve) = token.bonding_curve.as_ref() {
        curve_exclusions.insert(curve.0.clone());
    }
    if let Some(associated_curve) = token.associated_bonding_curve.as_ref() {
        curve_exclusions.insert(associated_curve.0.clone());
    }
    let mut curve_and_dev_exclusions = curve_exclusions.clone();
    if let Some(creator) = token.creator.as_ref() {
        curve_and_dev_exclusions.insert(creator.0.clone());
    }
    let holder_denominator_context_reliable = token.bonding_curve.is_some()
        || token.associated_bonding_curve.is_some()
        || token.reserve_state.account_update_confidence > Decimal::ZERO
        || token.reserve_state.price_sol_per_token.is_some();
    set_numeric(
        values,
        "holder_count_excluding_curve",
        Decimal::from(token.holder_state.holder_count_excluding(&curve_exclusions) as u64),
        dec!(0.9),
    );
    set_numeric(
        values,
        "holder_count_excluding_curve_and_dev",
        Decimal::from(
            token
                .holder_state
                .holder_count_excluding(&curve_and_dev_exclusions) as u64,
        ),
        dec!(0.9),
    );
    let token_decimals = token.reserve_state.token_decimals;
    let observed_supply_raw = token.holder_state.observed_holder_supply();
    let observed_supply_ui = raw_tokens_to_ui(observed_supply_raw, token_decimals);
    let total_supply = Decimal::from(PUMP_TOTAL_SUPPLY_UI);
    let total_supply_raw = ui_tokens_to_raw(total_supply, token_decimals);
    let dev_balance_raw = token
        .creator
        .as_ref()
        .and_then(|creator| token.holder_state.owner_balances.get(&creator.0))
        .map(|holder| holder.balance.max(Decimal::ZERO))
        .unwrap_or_else(|| token.developer_state.creator_net_tokens.max(Decimal::ZERO));
    let dev_balance_ui = raw_tokens_to_ui(dev_balance_raw, token_decimals);
    let circulating_supply = (total_supply - dev_balance_ui).max(Decimal::ZERO);
    let circulating_supply_raw = (total_supply_raw - dev_balance_raw).max(Decimal::ZERO);
    let holder_supply_invariant_ok = observed_supply_ui <= total_supply + Decimal::new(1, 6);
    if holder_denominator_context_reliable && holder_supply_invariant_ok {
        set_numeric(
            values,
            "observed_holder_supply",
            observed_supply_ui,
            dec!(0.9),
        );
        set_numeric(
            values,
            "circulating_holder_supply",
            circulating_supply,
            dec!(0.8),
        );
    } else if holder_denominator_context_reliable {
        set_unavailable(
            values,
            "observed_holder_supply",
            "holder supply invariant failed; likely transient same-transaction balance group",
        );
        set_numeric(
            values,
            "circulating_holder_supply",
            circulating_supply,
            dec!(0.5),
        );
    } else {
        set_unavailable(
            values,
            "observed_holder_supply",
            "holder denominator requires token decimals plus Pump curve/create context",
        );
        set_unavailable(
            values,
            "circulating_holder_supply",
            "circulating holder supply requires Pump curve/create context",
        );
    }
    if let Some(growth) = holder_growth_rate(token, observed_at) {
        set_numeric(values, "holder_growth_rate", growth, dec!(0.8));
        set_numeric(
            values,
            "sustained_holder_growth_score",
            clamp01(growth),
            dec!(0.8),
        );
    }

    let balances: Vec<Decimal> = token
        .holder_state
        .owner_balances
        .values()
        .map(|holder| holder.balance)
        .filter(|balance| *balance > Decimal::ZERO)
        .collect();
    if let Some(avg) = average_decimal(&balances) {
        set_numeric(values, "average_holder_balance", avg, dec!(0.9));
    }
    if let Some(median) = median_decimal(&balances) {
        set_numeric(values, "median_holder_balance", median, dec!(0.9));
    }
    if let Some(p90) = percentile_decimal(&balances, 90) {
        set_numeric(values, "p90_holder_balance", p90, dec!(0.9));
    }
    set_numeric(
        values,
        "holder_balance_gini",
        token.holder_state.gini,
        Decimal::ONE,
    );
    set_numeric(
        values,
        "holder_balance_hhi",
        token.holder_state.hhi,
        Decimal::ONE,
    );
    set_numeric(
        values,
        "top1_holder_pct",
        token.holder_state.top_holder_pct(1),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "top1_holder_pct_observed",
        token.holder_state.top_holder_pct(1),
        Decimal::ONE,
    );
    if holder_denominator_context_reliable && holder_supply_invariant_ok {
        let top1_total = token.holder_state.top_holder_pct_with_denominator(
            1,
            total_supply_raw,
            &curve_exclusions,
        );
        let top1_circulating = token.holder_state.top_holder_pct_with_denominator(
            1,
            circulating_supply_raw,
            &curve_exclusions,
        );
        set_numeric(
            values,
            "top1_holder_pct_total_supply",
            top1_total,
            dec!(0.8),
        );
        set_numeric(
            values,
            "top1_holder_pct_curve_economic_supply",
            top1_total,
            dec!(0.8),
        );
        set_text(
            values,
            "token_supply_selected_for_holder_pct",
            "token_supply_curve_economic_or_protocol_constant",
            Decimal::ONE,
        );
        if top1_circulating <= Decimal::ONE + Decimal::new(1, 6) {
            set_numeric(
                values,
                "top1_holder_pct_circulating",
                top1_circulating,
                dec!(0.7),
            );
        } else {
            set_unavailable(
                values,
                "top1_holder_pct_circulating",
                "top-holder circulating denominator invariant failed",
            );
        }
    } else if holder_denominator_context_reliable {
        set_unavailable(
            values,
            "top1_holder_pct_total_supply",
            "top-holder total-supply denominator invariant failed",
        );
        set_unavailable(
            values,
            "top1_holder_pct_curve_economic_supply",
            "top-holder curve-economic denominator invariant failed",
        );
        set_unavailable(
            values,
            "top1_holder_pct_circulating",
            "top-holder circulating denominator invariant failed",
        );
    } else {
        set_unavailable(
            values,
            "top1_holder_pct_total_supply",
            "top-holder total-supply denominator requires Pump curve/create context",
        );
        set_unavailable(
            values,
            "top1_holder_pct_curve_economic_supply",
            "top-holder curve-economic denominator requires Pump curve/create context",
        );
        set_unavailable(
            values,
            "top1_holder_pct_circulating",
            "top-holder circulating denominator requires Pump curve/create context",
        );
    }
    set_numeric(
        values,
        "top3_holder_pct",
        token.holder_state.top_holder_pct(3),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "top5_holder_pct",
        token.holder_state.top_holder_pct(5),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "top10_holder_pct",
        token.holder_state.top_holder_pct(10),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "top20_holder_pct",
        token.holder_state.top_holder_pct(20),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "holder_metric_confidence",
        if !holder_denominator_context_reliable {
            dec!(0.3)
        } else if token.holder_state.missing_owner_mapping_count() == 0 {
            dec!(0.9)
        } else {
            dec!(0.5)
        },
        Decimal::ONE,
    );
    set_numeric(
        values,
        "missing_owner_mapping_count",
        Decimal::from(token.holder_state.missing_owner_mapping_count() as u64),
        Decimal::ONE,
    );
    let distribution_improvement = clamp01(
        Decimal::ONE - token.holder_state.top_holder_pct(5)
            + token.holder_state.gini.max(Decimal::ZERO),
    );
    set_numeric(
        values,
        "holder_distribution_improvement_score",
        distribution_improvement / Decimal::from(2u64),
        dec!(0.7),
    );
}

fn compute_top_holder_features(token: &TokenState, values: &mut BTreeMap<String, FeatureValue>) {
    let whale_threshold = dec!(0.1);
    let holder_count = token.holder_state.nonzero_holder_count.max(1) as u64;
    let whale_count = token
        .holder_state
        .top_holders
        .iter()
        .filter(|holder| holder.pct_supply_proxy >= whale_threshold)
        .count();
    let whale_pct: Decimal = token
        .holder_state
        .top_holders
        .iter()
        .filter(|holder| holder.pct_supply_proxy >= whale_threshold)
        .map(|holder| holder.pct_supply_proxy)
        .sum();

    set_numeric(
        values,
        "whale_count",
        Decimal::from(whale_count as u64),
        dec!(0.8),
    );
    set_numeric(values, "whale_holding_pct", whale_pct, dec!(0.8));
    set_bool(
        values,
        "top1_is_creator",
        token
            .holder_state
            .top_holders
            .first()
            .zip(token.creator.as_ref())
            .map(|(top, creator)| top.owner == *creator)
            .unwrap_or(false),
        dec!(0.8),
    );
    let top1 = token.holder_state.top_holder_pct(1);
    let top5 = token.holder_state.top_holder_pct(5);
    let holder_diversity_discount = clamp01(Decimal::from(holder_count) / Decimal::from(5u64));
    let top1_excess = clamp01((top1 - dec!(0.30)) / dec!(0.40));
    let top5_excess = clamp01((top5 - dec!(0.75)) / dec!(0.25));
    let concentration_risk = clamp01(weighted_average(&[
        (top1_excess, dec!(0.45)),
        (top5_excess * holder_diversity_discount, dec!(0.30)),
        (token.developer_state.creator_sell_percentage, dec!(0.15)),
        (token.holder_state.gini, dec!(0.10)),
    ]));
    set_numeric(
        values,
        "concentration_risk_score",
        concentration_risk,
        dec!(0.8),
    );
    set_bool(
        values,
        "one_wallet_controls_market_flag",
        top1 >= dec!(0.55),
        dec!(0.9),
    );
}

fn compute_bundle_features(
    token: &TokenState,
    state: &dyn FeatureStateView,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    let first_buys = token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| trade.side == state::TradeSide::Buy)
        .take(8)
        .collect::<Vec<_>>();
    let same_window_count = if let Some(first) = first_buys.first() {
        first_buys
            .iter()
            .filter(|trade| {
                (trade.timestamp - first.timestamp)
                    .whole_milliseconds()
                    .abs()
                    <= 500
            })
            .count()
    } else {
        0
    };
    set_numeric(
        values,
        "same_slot_multi_buy_count",
        Decimal::from(same_window_count as u64),
        dec!(0.7),
    );

    let identical_amount_count = identical_amounts(&first_buys);
    set_numeric(
        values,
        "first_n_buys_identical_amount_count",
        Decimal::from(identical_amount_count as u64),
        dec!(0.7),
    );

    let buyers: Vec<String> = first_buys
        .iter()
        .map(|trade| trade.wallet.clone())
        .collect();
    let same_funder = same_funder_pair_count(&buyers, state);
    set_numeric(
        values,
        "first_n_buys_same_funder_count",
        Decimal::from(same_funder as u64),
        dec!(0.8),
    );

    let bundle_top_pct: Decimal = token
        .holder_state
        .top_holders
        .iter()
        .filter(|holder| buyers.iter().any(|buyer| buyer == &holder.owner.0))
        .map(|holder| holder.pct_supply_proxy)
        .sum();
    set_numeric(
        values,
        "bundle_wallets_top_holder_pct",
        bundle_top_pct,
        dec!(0.7),
    );

    let bundle_risk = clamp01(
        Decimal::from(same_window_count as u64) / Decimal::from(8u64)
            + Decimal::from(identical_amount_count as u64) / Decimal::from(8u64)
            + bundle_top_pct,
    ) / Decimal::from(3u64);
    set_numeric(values, "bundle_risk_score", bundle_risk, dec!(0.7));
    set_numeric(
        values,
        "bundle_confidence",
        clamp01(Decimal::from(first_buys.len() as u64) / Decimal::from(8u64)),
        dec!(0.9),
    );
}

fn compute_rug_features(
    token: &TokenState,
    observed_at: OffsetDateTime,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    let dev_sold = token.developer_state.creator_first_sell_time.is_some();
    set_bool(values, "dev_sold_flag", dev_sold, Decimal::ONE);
    set_numeric(
        values,
        "dev_sold_pct",
        token.developer_state.creator_sell_percentage,
        Decimal::ONE,
    );
    let drawdown_from_ath = values
        .get("drawdown_from_ath_pct")
        .and_then(FeatureValue::as_decimal)
        .unwrap_or(Decimal::ZERO);
    set_bool(
        values,
        "price_drop_50pct_from_ath",
        drawdown_from_ath >= dec!(0.5),
        dec!(0.9),
    );
    let launch_drop = values
        .get("drawdown_from_launch_pct")
        .and_then(FeatureValue::as_decimal)
        .unwrap_or(Decimal::ZERO);
    let within_5m = token
        .launch_time
        .map(|launch_time| observed_at <= launch_time + WINDOW_5M)
        .unwrap_or(false);
    set_bool(
        values,
        "price_drop_70pct_from_launch_within_5m",
        within_5m && launch_drop >= dec!(0.7),
        dec!(0.9),
    );
    set_bool(
        values,
        "holder_growth_stalled",
        holder_growth_rate(token, observed_at)
            .map(|value| value <= Decimal::ZERO)
            .unwrap_or(false),
        dec!(0.7),
    );
    set_bool(
        values,
        "sells_dominate_after_launch",
        token.trade_stats.sell_volume_quote > token.trade_stats.buy_volume_quote,
        dec!(0.8),
    );
    let no_buys = token
        .trade_stats
        .last_buy_at
        .map(|last_buy| observed_at - last_buy >= Duration::seconds(20))
        .unwrap_or(true);
    set_bool(values, "no_buys_for_x_seconds", no_buys, dec!(0.8));

    let rug_probability = clamp01(
        token.developer_state.creator_sell_percentage
            + if no_buys { dec!(0.3) } else { Decimal::ZERO }
            + if within_5m && launch_drop >= dec!(0.7) {
                dec!(0.6)
            } else {
                Decimal::ZERO
            }
            + if token.holder_state.top_holder_pct(1) >= dec!(0.35) {
                dec!(0.2)
            } else {
                Decimal::ZERO
            },
    );
    set_numeric(values, "rug_probability_score", rug_probability, dec!(0.8));
    set_numeric(
        values,
        "rug_confidence",
        clamp01(
            Decimal::from(token.trade_stats.trade_history.len().min(20) as u64)
                / Decimal::from(20u64),
        ),
        dec!(0.9),
    );
}

fn compute_cohort_features(
    token: &TokenState,
    state: &dyn FeatureStateView,
    observed_at: OffsetDateTime,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    let cohort = same_time_cohort(token, state, observed_at, WINDOW_30S);
    set_numeric(
        values,
        "cohort_launch_count",
        Decimal::from(cohort.len() as u64),
        dec!(0.8),
    );
    let active_count = cohort
        .iter()
        .filter(|other| {
            matches!(
                other.lifecycle,
                TokenLifecycle::ActiveLight
                    | TokenLifecycle::ActiveDeep
                    | TokenLifecycle::TradeCandidate
                    | TokenLifecycle::InPosition
                    | TokenLifecycle::ExitPending
            )
        })
        .count();
    set_numeric(
        values,
        "cohort_active_count",
        Decimal::from(active_count as u64),
        dec!(0.8),
    );
    if !cohort.is_empty() {
        let discard_count = cohort
            .iter()
            .filter(|other| {
                matches!(
                    other.lifecycle,
                    TokenLifecycle::SoftDiscarded | TokenLifecycle::HardDiscarded
                )
            })
            .count();
        let rug_count = cohort
            .iter()
            .filter(|other| matches!(other.lifecycle, TokenLifecycle::RugArchive))
            .count();
        set_numeric(
            values,
            "cohort_discard_rate",
            Decimal::from(discard_count as u64) / Decimal::from(cohort.len() as u64),
            dec!(0.8),
        );
        set_numeric(
            values,
            "cohort_rug_rate",
            Decimal::from(rug_count as u64) / Decimal::from(cohort.len() as u64),
            dec!(0.8),
        );
    }

    if !cohort.is_empty() {
        let buy_rank = descending_rank(token, &cohort, |other| {
            window_buy_volume(other, observed_at, WINDOW_30S)
        });
        let net_flow_rank = descending_rank(token, &cohort, |other| {
            window_net_flow(other, observed_at, WINDOW_30S)
        });
        let holder_rank = descending_rank(token, &cohort, |other| {
            holder_growth_rate(other, observed_at).unwrap_or(Decimal::ZERO)
        });
        let price_rank = descending_rank(token, &cohort, |other| {
            window_return(other, observed_at, WINDOW_30S).unwrap_or(Decimal::ZERO)
        });
        set_numeric(
            values,
            "token_rank_by_buy_volume_30s",
            Decimal::from(buy_rank as u64),
            dec!(0.8),
        );
        set_numeric(
            values,
            "token_rank_by_net_flow_30s",
            Decimal::from(net_flow_rank as u64),
            dec!(0.8),
        );
        set_numeric(
            values,
            "token_rank_by_holder_growth_30s",
            Decimal::from(holder_rank as u64),
            dec!(0.8),
        );
        set_numeric(
            values,
            "token_rank_by_price_return_30s",
            Decimal::from(price_rank as u64),
            dec!(0.8),
        );
        let relative_strength = if cohort.len() <= 1 {
            Decimal::ONE
        } else {
            let normalized_rank_sum = Decimal::from(
                ((buy_rank.saturating_sub(1))
                    + (net_flow_rank.saturating_sub(1))
                    + (holder_rank.saturating_sub(1))
                    + (price_rank.saturating_sub(1))) as u64,
            );
            let max_rank_sum = Decimal::from(((cohort.len() - 1) * 4) as u64);
            Decimal::ONE - (normalized_rank_sum / max_rank_sum.max(Decimal::ONE))
        };
        set_numeric(
            values,
            "token_relative_strength_score",
            clamp01(relative_strength),
            dec!(0.8),
        );
        set_numeric(
            values,
            "token_relative_weakness_score",
            Decimal::ONE - clamp01(relative_strength),
            dec!(0.8),
        );
        let total_buy_volume: Decimal = cohort
            .iter()
            .map(|other| window_buy_volume(other, observed_at, WINDOW_30S))
            .sum();
        let our_volume = window_buy_volume(token, observed_at, WINDOW_30S);
        if total_buy_volume > Decimal::ZERO {
            set_numeric(
                values,
                "cohort_attention_share",
                our_volume / total_buy_volume,
                dec!(0.8),
            );
        }
        let best_volume = cohort
            .iter()
            .map(|other| window_buy_volume(other, observed_at, WINDOW_30S))
            .max()
            .unwrap_or(Decimal::ZERO);
        set_numeric(
            values,
            "best_launch_in_cohort_distance",
            (best_volume - our_volume).max(Decimal::ZERO),
            dec!(0.8),
        );
        if total_buy_volume > Decimal::ZERO {
            set_numeric(
                values,
                "this_token_share_of_all_launch_buy_volume",
                our_volume / total_buy_volume,
                dec!(0.8),
            );
        }
        let total_unique_buyers: usize = cohort
            .iter()
            .map(|other| other.trade_stats.unique_buyers.len())
            .sum();
        if total_unique_buyers > 0 {
            set_numeric(
                values,
                "this_token_share_of_all_unique_buyers",
                Decimal::from(token.trade_stats.unique_buyers.len() as u64)
                    / Decimal::from(total_unique_buyers as u64),
                dec!(0.8),
            );
        }
        set_numeric(
            values,
            "token_percentile_buy_volume_30s",
            percentile_against(
                &cohort,
                window_buy_volume(token, observed_at, WINDOW_30S),
                |other| window_buy_volume(other, observed_at, WINDOW_30S),
            ),
            dec!(0.8),
        );
        set_numeric(
            values,
            "token_percentile_net_flow_30s",
            percentile_against(
                &cohort,
                window_net_flow(token, observed_at, WINDOW_30S),
                |other| window_net_flow(other, observed_at, WINDOW_30S),
            ),
            dec!(0.8),
        );
        set_numeric(
            values,
            "token_percentile_holder_growth_1m",
            percentile_against(
                &cohort,
                holder_growth_rate_window(token, observed_at, WINDOW_1M).unwrap_or(Decimal::ZERO),
                |other| {
                    holder_growth_rate_window(other, observed_at, WINDOW_1M)
                        .unwrap_or(Decimal::ZERO)
                },
            ),
            dec!(0.8),
        );
        set_numeric(
            values,
            "token_percentile_price_return_1m",
            percentile_against(
                &cohort,
                window_return(token, observed_at, WINDOW_1M).unwrap_or(Decimal::ZERO),
                |other| window_return(other, observed_at, WINDOW_1M).unwrap_or(Decimal::ZERO),
            ),
            dec!(0.8),
        );
        set_bool(
            values,
            "token_is_cohort_leader_flag",
            buy_rank == 1 && price_rank <= 2,
            dec!(0.8),
        );
        set_bool(
            values,
            "token_is_cohort_laggard_flag",
            buy_rank == cohort.len() && price_rank == cohort.len(),
            dec!(0.8),
        );
        let rank_5s = descending_rank(
            token,
            &same_time_cohort(token, state, observed_at, WINDOW_5S),
            |other| window_buy_volume(other, observed_at, WINDOW_5S),
        );
        let rank_15s = descending_rank(
            token,
            &same_time_cohort(token, state, observed_at, WINDOW_15S),
            |other| window_buy_volume(other, observed_at, WINDOW_15S),
        );
        let velocity = Decimal::from(rank_15s as u64) - Decimal::from(rank_5s as u64);
        set_numeric(
            values,
            "token_improving_rank_velocity",
            clamp01((-velocity).max(Decimal::ZERO)),
            dec!(0.7),
        );
        set_numeric(
            values,
            "token_losing_rank_velocity",
            clamp01(velocity.max(Decimal::ZERO)),
            dec!(0.7),
        );
    }
}

fn compute_rotation_features(
    token: &TokenState,
    state: &dyn FeatureStateView,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    let mint = token.mint.0.as_str();
    let mut wallets_rotating = 0u64;
    let mut rotated_notional = Decimal::ZERO;

    for wallet in state.wallets().values() {
        if !wallet.tokens_bought.contains_key(mint) {
            continue;
        }
        let sold_elsewhere: Decimal = wallet
            .tokens_sold
            .iter()
            .filter(|(other_mint, _)| other_mint.as_str() != mint)
            .map(|(_, amount)| *amount)
            .sum();
        if sold_elsewhere > Decimal::ZERO {
            wallets_rotating += 1;
            rotated_notional += sold_elsewhere;
        }
    }

    set_numeric(
        values,
        "wallets_sold_other_token_then_bought_this_count",
        Decimal::from(wallets_rotating),
        dec!(0.7),
    );
    set_numeric(
        values,
        "net_capital_rotated_into_this_from_other_launches",
        rotated_notional,
        dec!(0.7),
    );
    let smart_rotation = clamp01(
        Decimal::from(wallets_rotating) / Decimal::from(state.wallets().len().max(1) as u64),
    );
    set_numeric(
        values,
        "smart_capital_rotation_score",
        smart_rotation,
        dec!(0.7),
    );
    set_bool(
        values,
        "this_token_becoming_primary_capital_destination_flag",
        wallets_rotating > 0 && rotated_notional > token.trade_stats.sell_volume_quote,
        dec!(0.7),
    );
}

fn compute_cost_basis_features(token: &TokenState, values: &mut BTreeMap<String, FeatureValue>) {
    let positions = token
        .holder_state
        .owner_balances
        .values()
        .map(|holder| &holder.cost_basis)
        .collect::<Vec<_>>();
    let unrealized: Vec<Decimal> = positions
        .iter()
        .map(|position| position.estimated_unrealized_pnl)
        .collect();
    if let Some(mean) = average_decimal(&unrealized) {
        set_numeric(values, "holder_unrealized_pnl_mean", mean, dec!(0.8));
    }
    if let Some(median) = median_decimal(&unrealized) {
        set_numeric(values, "holder_unrealized_pnl_median", median, dec!(0.8));
    }
    if let Some(p90) = percentile_decimal(&unrealized, 90) {
        set_numeric(values, "holder_unrealized_pnl_p90", p90, dec!(0.8));
    }

    let total_supply: Decimal = token
        .holder_state
        .owner_balances
        .values()
        .map(|holder| holder.balance.max(Decimal::ZERO))
        .sum();
    let profit_supply: Decimal = token
        .holder_state
        .owner_balances
        .values()
        .filter(|holder| holder.cost_basis.estimated_unrealized_pnl > Decimal::ZERO)
        .map(|holder| holder.balance.max(Decimal::ZERO))
        .sum();
    let underwater_supply: Decimal = token
        .holder_state
        .owner_balances
        .values()
        .filter(|holder| holder.cost_basis.estimated_unrealized_pnl < Decimal::ZERO)
        .map(|holder| holder.balance.max(Decimal::ZERO))
        .sum();
    let breakeven_supply: Decimal = token
        .holder_state
        .owner_balances
        .values()
        .filter(|holder| holder.cost_basis.estimated_unrealized_pnl.abs() <= dec!(0.0001))
        .map(|holder| holder.balance.max(Decimal::ZERO))
        .sum();
    let free_roll_supply: Decimal = token
        .holder_state
        .owner_balances
        .values()
        .filter(|holder| holder.cost_basis.has_taken_profit)
        .map(|holder| holder.balance.max(Decimal::ZERO))
        .sum();
    if total_supply > Decimal::ZERO {
        set_numeric(
            values,
            "unrealized_profit_supply_pct",
            profit_supply / total_supply,
            dec!(0.8),
        );
        set_numeric(
            values,
            "underwater_supply_pct",
            underwater_supply / total_supply,
            dec!(0.8),
        );
        set_numeric(
            values,
            "breakeven_supply_pct",
            breakeven_supply / total_supply,
            dec!(0.8),
        );
        set_numeric(
            values,
            "free_rolling_supply_pct",
            free_roll_supply / total_supply,
            dec!(0.8),
        );
    }
    let top_holders = token
        .holder_state
        .top_holders
        .iter()
        .filter_map(|holder| token.holder_state.owner_balances.get(&holder.owner.0))
        .collect::<Vec<_>>();
    let top5_unrealized: Decimal = top_holders
        .iter()
        .take(5)
        .map(|holder| holder.cost_basis.estimated_unrealized_pnl)
        .sum();
    let top10_unrealized: Decimal = top_holders
        .iter()
        .take(10)
        .map(|holder| holder.cost_basis.estimated_unrealized_pnl)
        .sum();
    set_numeric(values, "top5_unrealized_pnl", top5_unrealized, dec!(0.8));
    set_numeric(values, "top10_unrealized_pnl", top10_unrealized, dec!(0.8));
    let realized_profit: Decimal = positions
        .iter()
        .map(|position| position.estimated_realized_pnl.max(Decimal::ZERO))
        .sum();
    let total_realized_abs: Decimal = positions
        .iter()
        .map(|position| position.estimated_realized_pnl.abs())
        .sum();
    if total_realized_abs > Decimal::ZERO {
        set_numeric(
            values,
            "realized_profit_taking_rate",
            realized_profit / total_realized_abs,
            dec!(0.8),
        );
    }
    let overhang = clamp01(weighted_average(&[
        (
            values
                .get("unrealized_profit_supply_pct")
                .and_then(FeatureValue::as_decimal)
                .unwrap_or(Decimal::ZERO),
            dec!(0.35),
        ),
        (
            values
                .get("free_rolling_supply_pct")
                .and_then(FeatureValue::as_decimal)
                .unwrap_or(Decimal::ZERO),
            dec!(0.35),
        ),
        (
            clamp01(
                (values
                    .get("top1_holder_pct")
                    .and_then(FeatureValue::as_decimal)
                    .unwrap_or(Decimal::ZERO)
                    - dec!(0.45))
                    / dec!(0.55),
            ),
            dec!(0.15),
        ),
        (
            clamp01(
                (values
                    .get("top5_holder_pct")
                    .and_then(FeatureValue::as_decimal)
                    .unwrap_or(Decimal::ZERO)
                    - dec!(0.80))
                    / dec!(0.20),
            ),
            dec!(0.15),
        ),
    ]));
    set_numeric(values, "profit_overhang_score", overhang, dec!(0.8));
    set_numeric(
        values,
        "underwater_capitulation_risk",
        clamp01(
            values
                .get("underwater_supply_pct")
                .and_then(FeatureValue::as_decimal)
                .unwrap_or(Decimal::ZERO)
                + values
                    .get("sell_volume_quote")
                    .and_then(FeatureValue::as_decimal)
                    .unwrap_or(Decimal::ZERO)
                    / Decimal::from(100u64),
        ),
        dec!(0.7),
    );
    set_numeric(
        values,
        "free_roll_dump_risk_score",
        clamp01(
            values
                .get("free_rolling_supply_pct")
                .and_then(FeatureValue::as_decimal)
                .unwrap_or(Decimal::ZERO)
                + values
                    .get("top5_holder_pct")
                    .and_then(FeatureValue::as_decimal)
                    .unwrap_or(Decimal::ZERO)
                    / Decimal::from(2u64),
        ),
        dec!(0.8),
    );
    set_numeric(
        values,
        "holders_taking_initials_out_count",
        Decimal::from(
            token
                .holder_state
                .owner_balances
                .values()
                .filter(|holder| holder.cost_basis.has_taken_profit)
                .count() as u64,
        ),
        dec!(0.8),
    );
    set_numeric(
        values,
        "holders_fully_exiting_count",
        Decimal::from(
            token
                .holder_state
                .owner_balances
                .values()
                .filter(|holder| holder.cost_basis.has_round_tripped)
                .count() as u64,
        ),
        dec!(0.8),
    );
}

fn compute_survival_features(
    token: &TokenState,
    observed_at: OffsetDateTime,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    let large_sell_threshold = dec!(5.0);
    let large_sells = token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| trade.side == state::TradeSide::Sell && trade.quote >= large_sell_threshold)
        .collect::<Vec<_>>();
    set_numeric(
        values,
        "large_sell_count",
        Decimal::from(large_sells.len() as u64),
        dec!(0.8),
    );
    let large_sell_total: Decimal = large_sells.iter().map(|trade| trade.quote).sum();
    set_numeric(
        values,
        "large_sell_total_quote",
        large_sell_total,
        dec!(0.8),
    );

    let absorption = if let Some(first_large_sell) = large_sells.first() {
        let buys_after = token
            .trade_stats
            .trade_history
            .iter()
            .filter(|trade| {
                trade.side == state::TradeSide::Buy && trade.timestamp > first_large_sell.timestamp
            })
            .collect::<Vec<_>>();
        let buy_count = buys_after.len();
        let unique_buyers = buys_after
            .iter()
            .map(|trade| trade.wallet.clone())
            .collect::<HashSet<_>>()
            .len();
        let buy_volume: Decimal = buys_after.iter().map(|trade| trade.quote).sum();
        set_numeric(
            values,
            "buyers_after_large_sell_count",
            Decimal::from(buy_count as u64),
            dec!(0.8),
        );
        set_numeric(
            values,
            "unique_buyers_after_large_sell",
            Decimal::from(unique_buyers as u64),
            dec!(0.8),
        );
        set_numeric(values, "buy_volume_after_large_sell", buy_volume, dec!(0.8));
        set_numeric(
            values,
            "holder_count_after_large_sell",
            Decimal::from(token.holder_state.nonzero_holder_count as u64),
            dec!(0.8),
        );
        if token.trade_stats.all_time_high > Decimal::ZERO {
            set_numeric(
                values,
                "price_impact_of_large_sells",
                clamp01(
                    (first_large_sell.price - token.latest_price).abs()
                        / token.trade_stats.all_time_high,
                ),
                dec!(0.7),
            );
        }
        if let Some(recovery_trade) = buys_after
            .iter()
            .find(|trade| trade.price >= first_large_sell.price)
        {
            let recovery_ms = (recovery_trade.timestamp - first_large_sell.timestamp)
                .whole_milliseconds()
                .max(0) as u64;
            set_numeric(
                values,
                "recovery_after_large_sell_ms",
                Decimal::from(recovery_ms),
                dec!(0.8),
            );
            if first_large_sell.price > Decimal::ZERO {
                set_numeric(
                    values,
                    "recovery_after_large_sell_pct",
                    clamp01(
                        (recovery_trade.price - first_large_sell.price).max(Decimal::ZERO)
                            / first_large_sell.price,
                    ),
                    dec!(0.7),
                );
            }
        }
        clamp01(Decimal::from(unique_buyers as u64) / Decimal::from(5u64))
    } else {
        Decimal::ZERO
    };
    set_numeric(values, "absorption_success_score", absorption, dec!(0.7));
    set_numeric(
        values,
        "absorption_failure_score",
        Decimal::ONE - absorption,
        dec!(0.7),
    );
    set_numeric(
        values,
        "repeated_absorption_count",
        Decimal::from(large_sells.len().saturating_sub(1) as u64),
        dec!(0.6),
    );
    set_numeric(
        values,
        "distribution_into_buyers_score",
        clamp01(token.holder_state.top_holder_pct(3) * Decimal::from(2u64) / Decimal::from(3u64)),
        dec!(0.7),
    );

    let recent_return = values
        .get("return_pct_30s")
        .and_then(FeatureValue::as_decimal)
        .unwrap_or(Decimal::ZERO);
    let holder_growth = values
        .get("holder_growth_rate")
        .and_then(FeatureValue::as_decimal)
        .unwrap_or(Decimal::ZERO);
    set_bool(
        values,
        "vertical_move_without_holder_growth",
        recent_return >= dec!(0.5)
            && holder_growth <= Decimal::ZERO
            && token.trade_stats.unique_buyers.len() <= 2
            && token.holder_state.nonzero_holder_count <= 2,
        dec!(0.7),
    );
    set_bool(
        values,
        "vertical_move_without_unique_buyers",
        recent_return >= dec!(0.5) && token.trade_stats.unique_buyers.len() <= 2,
        dec!(0.7),
    );
    set_bool(
        values,
        "price_up_no_retail_breadth",
        recent_return > Decimal::ZERO
            && token.trade_stats.unique_buyers.len() <= 2
            && token.trade_stats.buy_count >= 2,
        dec!(0.7),
    );
    let top_holder_wallets = token
        .holder_state
        .top_holders
        .iter()
        .take(5)
        .map(|holder| holder.owner.0.clone())
        .collect::<HashSet<_>>();
    let top_holder_sells = token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| {
            trade.side == state::TradeSide::Sell && top_holder_wallets.contains(&trade.wallet)
        })
        .count();
    set_bool(
        values,
        "price_up_top_holders_selling",
        recent_return > Decimal::ZERO && top_holder_sells > 0,
        dec!(0.7),
    );
    let first_buyers = token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| trade.side == state::TradeSide::Buy)
        .take(8)
        .map(|trade| trade.wallet.clone())
        .collect::<HashSet<_>>();
    let bundle_sells = token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| {
            trade.side == state::TradeSide::Sell && first_buyers.contains(&trade.wallet)
        })
        .count();
    set_bool(
        values,
        "price_up_bundle_selling",
        recent_return > Decimal::ZERO && bundle_sells > 0,
        dec!(0.7),
    );
    set_bool(
        values,
        "price_up_dev_selling",
        recent_return > Decimal::ZERO && token.developer_state.creator_first_sell_time.is_some(),
        dec!(0.7),
    );
    let unique_buyers_30s = unique_buyers_within(token, observed_at, WINDOW_30S);
    let unique_buyers_5s = unique_buyers_within(token, observed_at, WINDOW_5S);
    set_bool(
        values,
        "price_up_unique_buyers_down",
        recent_return > Decimal::ZERO && unique_buyers_5s < unique_buyers_30s,
        dec!(0.6),
    );
    let holders_1m = holder_count_at_window(token, observed_at, WINDOW_1M)
        .unwrap_or(token.holder_state.nonzero_holder_count);
    set_bool(
        values,
        "price_up_holder_count_down",
        recent_return > Decimal::ZERO && token.holder_state.nonzero_holder_count < holders_1m,
        dec!(0.6),
    );
    let largest_buy = values
        .get("largest_buy_quote")
        .and_then(FeatureValue::as_decimal)
        .unwrap_or(Decimal::ZERO);
    let buy_volume = values
        .get("buy_volume_quote")
        .and_then(FeatureValue::as_decimal)
        .unwrap_or(Decimal::ZERO);
    set_bool(
        values,
        "price_up_buy_size_concentration_high",
        buy_volume > Decimal::ZERO && largest_buy / buy_volume >= dec!(0.6),
        dec!(0.7),
    );
    let large_buys = token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| trade.side == state::TradeSide::Buy && trade.quote >= large_sell_threshold)
        .count();
    set_bool(
        values,
        "price_up_only_one_or_two_large_buys",
        recent_return > Decimal::ZERO
            && (1..=2).contains(&large_buys)
            && (buy_volume > Decimal::ZERO && largest_buy / buy_volume >= dec!(0.55))
            && token.trade_stats.unique_buyers.len() <= 2,
        dec!(0.7),
    );
    let flat_distribution = recent_return.abs() <= dec!(0.05)
        && buy_volume
            + values
                .get("sell_volume_quote")
                .and_then(FeatureValue::as_decimal)
                .unwrap_or(Decimal::ZERO)
            >= dec!(10.0)
        && top_holder_sells > 0;
    set_bool(
        values,
        "price_flat_volume_high_distribution_warning",
        flat_distribution,
        dec!(0.7),
    );

    let authenticity = clamp01(
        weighted_average(&[
            (
                values
                    .get("organic_flow_score")
                    .and_then(FeatureValue::as_decimal)
                    .unwrap_or(Decimal::ZERO),
                dec!(0.35),
            ),
            (holder_growth.max(Decimal::ZERO), dec!(0.25)),
            (
                clamp01(
                    Decimal::from(token.trade_stats.unique_buyers.len() as u64)
                        / Decimal::from(6u64),
                ),
                dec!(0.15),
            ),
            (
                Decimal::ONE
                    - weighted_average(&[
                        (
                            bool_decimal(values, "price_up_no_retail_breadth"),
                            dec!(0.35),
                        ),
                        (
                            bool_decimal(values, "price_up_only_one_or_two_large_buys"),
                            dec!(0.35),
                        ),
                        (
                            bool_decimal(values, "price_up_top_holders_selling"),
                            dec!(0.15),
                        ),
                        (bool_decimal(values, "price_up_bundle_selling"), dec!(0.15)),
                    ]),
                dec!(0.25),
            ),
        ]) - token.developer_state.creator_sell_percentage,
    );
    set_numeric(
        values,
        "momentum_authenticity_score",
        authenticity,
        dec!(0.7),
    );
    set_numeric(
        values,
        "exit_liquidity_trap_score",
        clamp01(
            bool_decimal(values, "price_up_top_holders_selling")
                + bool_decimal(values, "price_up_bundle_selling")
                + bool_decimal(values, "price_up_only_one_or_two_large_buys"),
        ) / Decimal::from(3u64),
        dec!(0.7),
    );

    let dev_not_selling =
        token.developer_state.creator_first_sell_time.is_none() && recent_return >= Decimal::ZERO;
    set_bool(
        values,
        "dev_not_selling_while_price_up",
        dev_not_selling,
        dec!(0.8),
    );

    let survival_score = clamp01(
        authenticity
            + absorption
            + (Decimal::ONE - token.holder_state.top_holder_pct(5))
            + if recent_return >= Decimal::ZERO {
                dec!(0.2)
            } else {
                Decimal::ZERO
            },
    ) / Decimal::from(3u64);
    set_numeric(values, "organic_survival_score", survival_score, dec!(0.8));
    set_numeric(
        values,
        "anti_rug_confidence_score",
        clamp01(
            Decimal::from(token.trade_stats.trade_history.len().min(20) as u64)
                / Decimal::from(20u64),
        ),
        dec!(0.8),
    );

    let _ = observed_at;
}

fn compute_holder_lifecycle_features(
    token: &TokenState,
    state: &dyn FeatureStateView,
    observed_at: OffsetDateTime,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    let holders = token
        .holder_state
        .owner_balances
        .values()
        .collect::<Vec<_>>();
    let ages = holders
        .iter()
        .filter_map(|holder| {
            holder
                .first_seen_at
                .map(|time| Decimal::from((observed_at - time).whole_seconds().max(0) as u64))
        })
        .collect::<Vec<_>>();
    if let Some(mean) = average_decimal(&ages) {
        set_numeric(values, "holder_age_mean", mean, dec!(0.8));
    }
    if let Some(median) = median_decimal(&ages) {
        set_numeric(values, "holder_age_median", median, dec!(0.8));
    }
    let total_balance: Decimal = holders
        .iter()
        .map(|holder| holder.balance.max(Decimal::ZERO))
        .sum();
    if total_balance > Decimal::ZERO {
        let weighted: Decimal = holders
            .iter()
            .filter_map(|holder| {
                holder.first_seen_at.map(|time| {
                    Decimal::from((observed_at - time).whole_seconds().max(0) as u64)
                        * holder.balance.max(Decimal::ZERO)
                })
            })
            .sum();
        set_numeric(
            values,
            "holder_age_weighted_by_balance",
            weighted / total_balance,
            dec!(0.8),
        );
    }
    let new_30s = holders
        .iter()
        .filter(|holder| {
            holder
                .first_seen_at
                .map(|time| observed_at - time <= WINDOW_30S)
                .unwrap_or(false)
        })
        .count();
    let new_1m = holders
        .iter()
        .filter(|holder| {
            holder
                .first_seen_at
                .map(|time| observed_at - time <= WINDOW_1M)
                .unwrap_or(false)
        })
        .count();
    let survived_30s = holders
        .iter()
        .filter(|holder| {
            holder
                .first_seen_at
                .map(|time| observed_at - time >= WINDOW_30S)
                .unwrap_or(false)
        })
        .count();
    let survived_1m = holders
        .iter()
        .filter(|holder| {
            holder
                .first_seen_at
                .map(|time| observed_at - time >= WINDOW_1M)
                .unwrap_or(false)
        })
        .count();
    set_numeric(
        values,
        "new_holder_survival_30s",
        ratio(survived_30s, new_30s.max(survived_30s)),
        dec!(0.7),
    );
    set_numeric(
        values,
        "new_holder_survival_1m",
        ratio(survived_1m, new_1m.max(survived_1m)),
        dec!(0.7),
    );
    set_numeric(
        values,
        "first_10_buyers_retention",
        buyer_retention(token, 10),
        dec!(0.8),
    );
    set_numeric(
        values,
        "first_20_buyers_retention",
        buyer_retention(token, 20),
        dec!(0.8),
    );
    let sell_durations = first_sell_durations(token)
        .into_iter()
        .map(Decimal::from)
        .collect::<Vec<_>>();
    if let Some(avg) = average_decimal(&sell_durations) {
        set_numeric(
            values,
            "average_time_to_first_sell_by_holder",
            avg,
            dec!(0.8),
        );
    }
    let launch_age = token
        .launch_time
        .map(|launch| Decimal::from((observed_at - launch).whole_seconds().max(0) as u64))
        .unwrap_or(Decimal::ZERO);
    let retention = values
        .get("first_20_buyers_retention")
        .and_then(FeatureValue::as_decimal)
        .unwrap_or(Decimal::ZERO);
    set_numeric(
        values,
        "holder_half_life",
        launch_age * (Decimal::ONE - retention),
        dec!(0.6),
    );
    set_numeric(
        values,
        "holder_decay_rate",
        Decimal::ONE - retention,
        dec!(0.7),
    );
    let stickiness = clamp01(weighted_average(&[
        (retention, dec!(0.5)),
        (
            values
                .get("new_holder_survival_30s")
                .and_then(FeatureValue::as_decimal)
                .unwrap_or(Decimal::ZERO),
            dec!(0.25),
        ),
        (Decimal::ONE - token.holder_state.hhi, dec!(0.25)),
    ]));
    set_numeric(values, "holder_stickiness_score", stickiness, dec!(0.8));
    set_numeric(
        values,
        "holder_paper_hands_score",
        Decimal::ONE - stickiness,
        dec!(0.8),
    );
    let replaced = holders
        .iter()
        .filter(|holder| {
            holder
                .first_seen_at
                .map(|time| observed_at - time <= WINDOW_30S)
                .unwrap_or(false)
        })
        .count();
    let exited = state
        .wallets()
        .values()
        .filter(|wallet| {
            wallet
                .tokens_sold
                .get(&token.mint.0)
                .copied()
                .unwrap_or(Decimal::ZERO)
                > Decimal::ZERO
        })
        .count();
    set_numeric(
        values,
        "healthy_churn_score",
        clamp01(ratio(replaced, exited.max(1))),
        dec!(0.7),
    );
    set_numeric(
        values,
        "unhealthy_churn_score",
        clamp01(ratio(exited, replaced.max(1))),
        dec!(0.7),
    );
    set_numeric(
        values,
        "new_holders_replacing_old_holders_score",
        clamp01(ratio(
            replaced,
            token.holder_state.nonzero_holder_count.max(1),
        )),
        dec!(0.7),
    );
}

fn compute_transaction_fingerprint_features(
    token: &TokenState,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    let trades = token.trade_stats.trade_history.iter().collect::<Vec<_>>();
    let compute_limits = trades
        .iter()
        .filter_map(|trade| trade.compute_unit_limit.map(Decimal::from))
        .collect::<Vec<_>>();
    let compute_prices = trades
        .iter()
        .filter_map(|trade| trade.compute_unit_price.map(Decimal::from))
        .collect::<Vec<_>>();
    let account_counts = trades
        .iter()
        .filter_map(|trade| trade.account_count.map(|value| Decimal::from(value as u64)))
        .collect::<Vec<_>>();
    let instruction_counts = trades
        .iter()
        .filter_map(|trade| {
            trade
                .instruction_count
                .map(|value| Decimal::from(value as u64))
        })
        .collect::<Vec<_>>();
    if let Some(value) = median_decimal(&account_counts) {
        set_numeric(values, "tx_account_count", value, dec!(0.8));
    }
    if let Some(value) = median_decimal(&instruction_counts) {
        set_numeric(values, "tx_instruction_count", value, dec!(0.8));
        set_numeric(
            values,
            "tx_inner_instruction_count",
            Decimal::ZERO,
            dec!(0.3),
        );
    }
    set_bool(
        values,
        "compute_budget_instruction_present",
        !compute_limits.is_empty() || !compute_prices.is_empty(),
        dec!(0.8),
    );
    if let Some(value) = median_decimal(&compute_limits) {
        set_numeric(values, "compute_unit_limit", value, dec!(0.8));
        set_numeric(
            values,
            "compute_unit_limit_bucket",
            bucket_decimal(value, &[100_000, 200_000, 400_000]),
            dec!(0.7),
        );
    }
    if let Some(value) = median_decimal(&compute_prices) {
        set_numeric(values, "compute_unit_price", value, dec!(0.8));
        set_numeric(
            values,
            "compute_unit_price_bucket",
            bucket_decimal(value, &[1_000, 5_000, 10_000]),
            dec!(0.7),
        );
    }
    let fingerprint_counts = count_by(
        trades
            .iter()
            .filter_map(|trade| trade.client_fingerprint.clone())
            .collect::<Vec<_>>(),
    );
    let compute_profile_counts = count_by(
        trades
            .iter()
            .filter_map(
                |trade| match (trade.compute_unit_limit, trade.compute_unit_price) {
                    (Some(limit), Some(price)) => Some(format!("{limit}:{price}")),
                    _ => None,
                },
            )
            .collect::<Vec<_>>(),
    );
    set_numeric(
        values,
        "duplicate_compute_profile_count",
        Decimal::from(max_count(&compute_profile_counts).saturating_sub(1) as u64),
        dec!(0.7),
    );
    let first_buyers = trades
        .iter()
        .filter(|trade| trade.side == state::TradeSide::Buy)
        .take(8)
        .collect::<Vec<_>>();
    let first_compute_counts = count_by(
        first_buyers
            .iter()
            .filter_map(
                |trade| match (trade.compute_unit_limit, trade.compute_unit_price) {
                    (Some(limit), Some(price)) => Some(format!("{limit}:{price}")),
                    _ => None,
                },
            )
            .collect::<Vec<_>>(),
    );
    set_numeric(
        values,
        "duplicate_compute_profile_among_first_buyers",
        Decimal::from(max_count(&first_compute_counts).saturating_sub(1) as u64),
        dec!(0.7),
    );
    set_numeric(
        values,
        "duplicate_instruction_sequence_count",
        Decimal::from(
            max_count(&count_by(
                instruction_counts
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
            ))
            .saturating_sub(1) as u64,
        ),
        dec!(0.6),
    );
    set_numeric(
        values,
        "duplicate_account_pattern_count",
        Decimal::from(
            max_count(&count_by(
                account_counts
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
            ))
            .saturating_sub(1) as u64,
        ),
        dec!(0.6),
    );
    let dominant_fingerprint = max_count(&fingerprint_counts);
    let fingerprint_total = fingerprint_counts.values().sum::<usize>();
    let fingerprint_share = if fingerprint_total > 0 {
        Decimal::from(dominant_fingerprint as u64) / Decimal::from(fingerprint_total as u64)
    } else {
        Decimal::ZERO
    };
    set_numeric(
        values,
        "identical_client_fingerprint_score",
        fingerprint_share,
        dec!(0.7),
    );
    set_numeric(
        values,
        "first_buyers_same_client_fingerprint_pct",
        dominant_share(
            &first_buyers
                .iter()
                .filter_map(|trade| trade.client_fingerprint.clone())
                .collect::<Vec<_>>(),
        ),
        dec!(0.7),
    );
    let top_holder_wallets = token
        .holder_state
        .top_holders
        .iter()
        .take(5)
        .map(|holder| holder.owner.0.clone())
        .collect::<HashSet<_>>();
    set_numeric(
        values,
        "top_holders_same_client_fingerprint_pct",
        dominant_share(
            &trades
                .iter()
                .filter(|trade| top_holder_wallets.contains(&trade.wallet))
                .filter_map(|trade| trade.client_fingerprint.clone())
                .collect::<Vec<_>>(),
        ),
        dec!(0.7),
    );
    set_numeric(
        values,
        "bot_family_dominance_score",
        fingerprint_share,
        dec!(0.7),
    );
    set_numeric(
        values,
        "bot_family_diversity_score",
        if fingerprint_total > 0 {
            Decimal::from(fingerprint_counts.len() as u64) / Decimal::from(fingerprint_total as u64)
        } else {
            Decimal::ZERO
        },
        dec!(0.7),
    );
}

fn compute_funding_graph_features(
    token: &TokenState,
    state: &dyn FeatureStateView,
    observed_at: OffsetDateTime,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    let creator_wallet = token.creator.as_ref().map(|creator| creator.0.as_str());
    let creator_funder =
        creator_wallet.and_then(|wallet| latest_funder_for_wallet(state.funding_graph(), wallet));
    if let Some(funder) = creator_funder.as_ref() {
        set_text(
            values,
            "creator_recent_funder",
            funder.funder.clone(),
            dec!(0.8),
        );
        if let Some(launch_time) = token.launch_time {
            set_numeric(
                values,
                "funder_to_creator_edge_age",
                Decimal::from(
                    (launch_time - funder.last_seen)
                        .whole_seconds()
                        .unsigned_abs(),
                ),
                dec!(0.7),
            );
        }
    }
    let first_buyers = token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| trade.side == state::TradeSide::Buy)
        .take(8)
        .map(|trade| trade.wallet.clone())
        .collect::<Vec<_>>();
    let top_holders = token
        .holder_state
        .top_holders
        .iter()
        .take(5)
        .map(|holder| holder.owner.0.clone())
        .collect::<Vec<_>>();
    let first_funders = wallet_funders(state.funding_graph(), &first_buyers);
    let top_funders = wallet_funders(state.funding_graph(), &top_holders);
    set_numeric(
        values,
        "buyer_recent_funder_overlap_count",
        Decimal::from(overlap_count(
            &first_funders,
            creator_funder.as_ref().map(|f| f.funder.as_str()),
        ) as u64),
        dec!(0.7),
    );
    set_numeric(
        values,
        "first_buyers_same_funder_pct",
        dominant_share(&first_funders),
        dec!(0.7),
    );
    set_numeric(
        values,
        "top_holders_same_funder_pct",
        dominant_share(&top_funders),
        dec!(0.7),
    );
    if let Some(funder) = first_funders
        .first()
        .and_then(|_| first_buyers.first())
        .and_then(|wallet| latest_funder_for_wallet(state.funding_graph(), wallet))
    {
        if let Some(launch_time) = token.launch_time {
            set_numeric(
                values,
                "funder_to_buyer_edge_age",
                Decimal::from(
                    (launch_time - funder.last_seen)
                        .whole_seconds()
                        .unsigned_abs(),
                ),
                dec!(0.7),
            );
        }
    }
    if let Some(funder) = creator_funder.as_ref() {
        let launch_count = state
            .tokens()
            .values()
            .filter(|other| {
                other
                    .creator
                    .as_ref()
                    .and_then(|creator| latest_funder_for_wallet(state.funding_graph(), &creator.0))
                    .map(|edge| edge.funder == funder.funder)
                    .unwrap_or(false)
            })
            .count();
        set_numeric(
            values,
            "funder_to_multiple_launches_count",
            Decimal::from(launch_count as u64),
            dec!(0.7),
        );
    }
    let same_amount_pattern = repeated_amount_pattern(
        state.funding_graph(),
        creator_funder.as_ref().map(|edge| edge.funder.as_str()),
    );
    set_numeric(
        values,
        "same_funder_same_amount_pattern",
        same_amount_pattern,
        dec!(0.6),
    );
    let burst = state
        .funding_graph()
        .edges
        .values()
        .filter(|edge| {
            token
                .launch_time
                .map(|launch| edge.last_seen <= launch && launch - edge.last_seen <= WINDOW_5M)
                .unwrap_or(false)
        })
        .count();
    set_numeric(
        values,
        "funding_burst_before_launch",
        Decimal::from(burst as u64),
        dec!(0.7),
    );
    let just_before_buy = first_buyers
        .iter()
        .filter(|wallet| {
            latest_funder_for_wallet(state.funding_graph(), wallet)
                .map(|edge| observed_at - edge.last_seen <= WINDOW_1M)
                .unwrap_or(false)
        })
        .count();
    set_numeric(
        values,
        "wallets_funded_just_before_buy_count",
        Decimal::from(just_before_buy as u64),
        dec!(0.7),
    );
    let known_factory = creator_funder
        .as_ref()
        .map(|funder| {
            state
                .funding_graph()
                .edges
                .values()
                .filter(|edge| edge.funder == funder.funder)
                .count()
        })
        .unwrap_or_default();
    set_numeric(
        values,
        "fresh_wallets_funded_by_known_factory",
        Decimal::from(known_factory as u64),
        dec!(0.6),
    );
    let wallet_count = state.wallets().len().max(1);
    let density = Decimal::from(state.funding_graph().edges.len() as u64)
        / Decimal::from((wallet_count * wallet_count) as u64);
    set_numeric(
        values,
        "funding_graph_density",
        clamp01(density * Decimal::from(10u64)),
        dec!(0.7),
    );
    let suspicion = clamp01(
        values
            .get("first_buyers_same_funder_pct")
            .and_then(FeatureValue::as_decimal)
            .unwrap_or(Decimal::ZERO)
            + values
                .get("top_holders_same_funder_pct")
                .and_then(FeatureValue::as_decimal)
                .unwrap_or(Decimal::ZERO)
            + values
                .get("funding_graph_density")
                .and_then(FeatureValue::as_decimal)
                .unwrap_or(Decimal::ZERO),
    ) / Decimal::from(3u64);
    set_numeric(
        values,
        "funding_graph_suspicion_score",
        suspicion,
        dec!(0.7),
    );
    set_numeric(
        values,
        "funding_graph_quality_score",
        Decimal::ONE - suspicion,
        dec!(0.7),
    );
}

fn compute_fee_competition_features(
    token: &TokenState,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    let priority_fees = token
        .trade_stats
        .trade_history
        .iter()
        .filter_map(|trade| trade.priority_fee_lamports.map(Decimal::from))
        .collect::<Vec<_>>();
    let buy_fees = token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| trade.side == state::TradeSide::Buy)
        .filter_map(|trade| trade.priority_fee_lamports.map(Decimal::from))
        .collect::<Vec<_>>();
    let sell_fees = token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| trade.side == state::TradeSide::Sell)
        .filter_map(|trade| trade.priority_fee_lamports.map(Decimal::from))
        .collect::<Vec<_>>();
    let first_buyer_fees = token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| trade.side == state::TradeSide::Buy)
        .take(8)
        .filter_map(|trade| trade.priority_fee_lamports.map(Decimal::from))
        .collect::<Vec<_>>();
    if let Some(median) = median_decimal(&priority_fees) {
        set_numeric(values, "launch_priority_fee_median", median, dec!(0.8));
        set_numeric(
            values,
            "our_required_priority_fee_estimate",
            median,
            dec!(0.7),
        );
    }
    if let Some(p90) = percentile_decimal(&priority_fees, 90) {
        set_numeric(values, "launch_priority_fee_p90", p90, dec!(0.8));
    }
    if let Some(median) = median_decimal(&buy_fees) {
        set_numeric(values, "pump_buy_priority_fee_median", median, dec!(0.8));
    }
    if let Some(median) = median_decimal(&sell_fees) {
        set_numeric(values, "pump_sell_priority_fee_median", median, dec!(0.8));
    }
    if let Some(median) = median_decimal(&first_buyer_fees) {
        set_numeric(
            values,
            "first_buyers_priority_fee_median",
            median,
            dec!(0.8),
        );
    }
    let spike = percentile_decimal(&priority_fees, 90)
        .zip(median_decimal(&priority_fees))
        .map(|(p90, median)| median > Decimal::ZERO && p90 / median >= dec!(2.0))
        .unwrap_or(false);
    set_bool(values, "priority_fee_spike_near_launch", spike, dec!(0.7));
    let fee_war = clamp01(
        values
            .get("launch_priority_fee_p90")
            .and_then(FeatureValue::as_decimal)
            .unwrap_or(Decimal::ZERO)
            / Decimal::from(10_000u64),
    );
    set_numeric(values, "fee_war_score", fee_war, dec!(0.7));
    let min_move = minimum_move_to_cover_fees(token);
    set_numeric(
        values,
        "minimum_move_to_cover_observed_fees",
        min_move,
        dec!(0.7),
    );
    let edge_minus_fee = values
        .get("return_pct_30s")
        .and_then(FeatureValue::as_decimal)
        .unwrap_or(Decimal::ZERO)
        - min_move;
    set_numeric(values, "edge_minus_fee_war_cost", edge_minus_fee, dec!(0.7));
    set_bool(
        values,
        "high_fee_low_edge_warning",
        fee_war >= dec!(0.5) && edge_minus_fee <= Decimal::ZERO,
        dec!(0.7),
    );
}

fn compute_execution_features(token: &TokenState, values: &mut BTreeMap<String, FeatureValue>) {
    let saw_shred_first = matches!(
        token.first_seen_source,
        EventSource::ShredTentative | EventSource::DeshredTentative
    );
    set_bool(
        values,
        "token_signal_available_before_geyser_flag",
        saw_shred_first,
        dec!(0.9),
    );
    set_numeric(
        values,
        "slot_id",
        Decimal::from(token.launch_slot.unwrap_or_default()),
        Decimal::ONE,
    );
    set_numeric(
        values,
        "competition_intensity_score",
        clamp01(
            Decimal::from(token.trade_stats.buy_count + token.trade_stats.sell_count)
                / Decimal::from(50u64),
        ),
        dec!(0.7),
    );
    let counterfactual_buy = token
        .trade_stats
        .price_history
        .back()
        .map(|(_, price)| token.latest_price - *price)
        .unwrap_or(Decimal::ZERO);
    set_numeric(
        values,
        "counterfactual_buy_after_100ms_pnl",
        counterfactual_buy,
        dec!(0.5),
    );
    let latency_adjusted = clamp01(
        values
            .get("organic_flow_score")
            .and_then(FeatureValue::as_decimal)
            .unwrap_or(Decimal::ZERO)
            - values
                .get("rug_probability_score")
                .and_then(FeatureValue::as_decimal)
                .unwrap_or(Decimal::ZERO),
    );
    set_numeric(
        values,
        "latency_adjusted_trade_eligibility",
        latency_adjusted,
        dec!(0.6),
    );
}

fn compute_shred_exit_defense_features(
    token: &TokenState,
    values: &mut BTreeMap<String, FeatureValue>,
) {
    let defense = &token.shred_defense;
    let false_positive_rate = if defense.tentative_sell_count_window == 0 {
        Decimal::ZERO
    } else {
        Decimal::from(defense.tentative_sell_false_positive_total)
            / Decimal::from(defense.tentative_sell_count_window)
    };
    let latency_used = defense
        .shred_to_geyser_processed_ms
        .map(Decimal::from)
        .or_else(|| {
            defense
                .shred_to_account_effect_confirmation_ms
                .map(Decimal::from)
        })
        .or_else(|| defense.shred_to_rooted_confirmation_ms.map(Decimal::from))
        .unwrap_or(Decimal::ZERO);
    let latency_budget =
        Decimal::from(defense.exit_threat_index.required_early_intent_lead_time_ms);
    let edge_score = (defense.shred_saved_loss_estimate + defense.shred_saved_loss_realized
        - defense.shred_exit_opportunity_cost)
        .max(Decimal::ZERO);
    set_numeric(
        values,
        "tentative_sell_count_window",
        Decimal::from(defense.tentative_sell_count_window),
        dec!(0.9),
    );
    set_numeric(
        values,
        "tentative_sell_volume_quote_window",
        defense.tentative_sell_volume_quote_window,
        dec!(0.85),
    );
    set_numeric(
        values,
        "tentative_sell_from_dev_count",
        Decimal::from(defense.tentative_sell_from_dev_count),
        dec!(0.9),
    );
    set_numeric(
        values,
        "tentative_sell_from_top_holder_count",
        Decimal::from(defense.tentative_sell_from_top_holder_count),
        dec!(0.9),
    );
    set_numeric(
        values,
        "tentative_sell_from_bundle_count",
        Decimal::from(defense.tentative_sell_from_bundle_count),
        dec!(0.9),
    );
    set_numeric(
        values,
        "tentative_sell_from_whale_count",
        Decimal::from(defense.tentative_sell_from_whale_count),
        dec!(0.9),
    );
    set_numeric(
        values,
        "tentative_sell_same_slot_cluster_count",
        Decimal::from(defense.tentative_sell_same_slot_cluster_count),
        dec!(0.8),
    );
    set_numeric(
        values,
        "tentative_sell_impact_max_pct",
        defense.tentative_sell_impact_max_pct,
        dec!(0.85),
    );
    set_numeric(
        values,
        "tentative_sell_impact_sum_pct",
        defense.tentative_sell_impact_sum_pct,
        dec!(0.85),
    );
    set_numeric(
        values,
        "tentative_sell_confidence_max",
        defense.tentative_sell_confidence_max,
        dec!(0.9),
    );
    set_numeric(
        values,
        "tentative_sell_confidence_mean",
        defense.tentative_sell_confidence_mean,
        dec!(0.9),
    );
    set_numeric(
        values,
        "shred_sell_warning_level",
        Decimal::from(shred_warning_level_value(defense.last_warning_level)),
        dec!(0.85),
    );
    set_bool(
        values,
        "shred_exit_armed_flag",
        defense.shred_exit_armed_flag,
        dec!(0.9),
    );
    set_bool(
        values,
        "shred_emergency_exit_triggered_flag",
        defense.shred_emergency_exit_triggered_flag,
        dec!(0.9),
    );
    set_numeric(
        values,
        "shred_to_geyser_processed_ms",
        Decimal::from(defense.shred_to_geyser_processed_ms.unwrap_or_default()),
        dec!(0.75),
    );
    set_numeric(
        values,
        "shred_to_account_effect_confirmation_ms",
        Decimal::from(
            defense
                .shred_to_account_effect_confirmation_ms
                .unwrap_or_default(),
        ),
        dec!(0.75),
    );
    set_numeric(
        values,
        "shred_to_rooted_confirmation_ms",
        Decimal::from(defense.shred_to_rooted_confirmation_ms.unwrap_or_default()),
        dec!(0.75),
    );
    set_numeric(
        values,
        "shred_sell_false_positive_rate_wallet",
        false_positive_rate,
        dec!(0.7),
    );
    set_numeric(
        values,
        "shred_sell_false_positive_rate_source",
        false_positive_rate,
        dec!(0.7),
    );
    set_numeric(
        values,
        "shred_sell_saved_loss_estimate",
        defense.shred_saved_loss_estimate,
        dec!(0.8),
    );
    set_numeric(
        values,
        "shred_sell_saved_loss_realized",
        defense.shred_saved_loss_realized,
        dec!(0.8),
    );
    set_numeric(
        values,
        "shred_exit_opportunity_cost",
        defense.shred_exit_opportunity_cost,
        dec!(0.8),
    );
    set_bool(
        values,
        "shred_signal_stale_flag",
        defense.shred_signal_stale_flag,
        dec!(0.85),
    );
    set_numeric(
        values,
        "shred_exit_latency_budget_ms",
        latency_budget,
        dec!(0.8),
    );
    set_numeric(
        values,
        "shred_exit_latency_used_ms",
        latency_used,
        dec!(0.8),
    );
    set_numeric(values, "shred_exit_edge_score", edge_score, dec!(0.8));
    set_numeric(
        values,
        "early_intent_latency_advantage_ms",
        Decimal::from(
            defense
                .early_intent_latency_advantage_ms
                .unwrap_or_default(),
        ),
        dec!(0.85),
    );
    set_numeric(
        values,
        "required_latency_advantage_ms",
        Decimal::from(defense.required_latency_advantage_ms.unwrap_or_default()),
        dec!(0.8),
    );
    set_numeric(
        values,
        "latency_edge_ratio",
        defense.latency_edge_ratio,
        dec!(0.8),
    );
    set_bool(
        values,
        "exit_can_land_before_estimated_impact",
        defense.exit_can_land_before_estimated_impact,
        dec!(0.8),
    );
    set_numeric(
        values,
        "absorption_health_score",
        defense.absorption_health_score,
        dec!(0.75),
    );
    set_numeric(
        values,
        "post_sell_absorption_probability",
        defense.post_sell_absorption_probability,
        dec!(0.75),
    );
    set_numeric(
        values,
        "emergency_exit_expected_saved_loss",
        defense.emergency_exit_expected_saved_loss,
        dec!(0.8),
    );
    set_numeric(
        values,
        "emergency_exit_expected_opportunity_cost",
        defense.emergency_exit_expected_opportunity_cost,
        dec!(0.8),
    );
    set_numeric(
        values,
        "emergency_exit_net_benefit",
        defense.emergency_exit_net_benefit,
        dec!(0.8),
    );
    set_numeric(
        values,
        "emergency_exit_net_benefit_confidence",
        defense.emergency_exit_net_benefit_confidence,
        dec!(0.8),
    );
    set_numeric(
        values,
        "malicious_sell_intent_score",
        defense.malicious_sell_intent_score,
        dec!(0.85),
    );
    set_numeric(
        values,
        "preconfirmation_exit_confidence",
        defense.preconfirmation_exit_confidence,
        dec!(0.85),
    );
    set_numeric(
        values,
        "dangerous_seller_precomputed_impact_score",
        defense
            .exit_threat_index
            .dangerous_seller_precomputed_impact_score,
        dec!(0.8),
    );
    set_numeric(
        values,
        "exit_threat_index_score",
        defense.exit_threat_index.exit_threat_index_score,
        dec!(0.8),
    );
}

fn finalize_data_quality_features(token: &TokenState, values: &mut BTreeMap<String, FeatureValue>) {
    set_bool(values, "geyser_connected_flag", true, Decimal::ONE);
    set_bool(
        values,
        "shred_connected_flag",
        matches!(
            token.first_seen_source,
            EventSource::ShredTentative | EventSource::DeshredTentative
        ),
        dec!(0.8),
    );
    set_bool(
        values,
        "source_disagreement_flag",
        token.data_quality_flags.contains("data_gap") || token.tentative_only,
        dec!(0.8),
    );
    let critical_missing = values
        .iter()
        .filter(|(feature_id, value)| {
            value.status != FeatureStatus::Available
                && matches!(
                    feature_id.as_str(),
                    "price_current"
                        | "buy_volume_quote"
                        | "holder_count_current"
                        | "bundle_risk_score"
                        | "rug_probability_score"
                )
        })
        .count();
    set_numeric(
        values,
        "critical_feature_missing_count",
        Decimal::from(critical_missing as u64),
        Decimal::ONE,
    );
    let available = values
        .values()
        .filter(|value| value.status == FeatureStatus::Available)
        .count();
    let implemented = values
        .values()
        .filter(|value| value.status != FeatureStatus::Unavailable)
        .count();
    if implemented > 0 {
        set_numeric(
            values,
            "feature_completeness_pct",
            Decimal::from(available as u64) / Decimal::from(implemented as u64),
            dec!(0.9),
        );
    }
    let quality = clamp01(
        values
            .get("feature_completeness_pct")
            .and_then(FeatureValue::as_decimal)
            .unwrap_or(Decimal::ZERO)
            - Decimal::from(critical_missing as u64) / Decimal::from(10u64)
            - if token.tentative_only {
                dec!(0.2)
            } else {
                Decimal::ZERO
            }
            - if token.data_quality_flags.contains("data_gap") {
                dec!(0.5)
            } else {
                Decimal::ZERO
            },
    );
    set_numeric(values, "data_quality_score", quality, dec!(0.9));
    set_bool(
        values,
        "trade_allowed_data_quality_flag",
        quality >= dec!(0.5) && !token.data_quality_flags.contains("data_gap"),
        dec!(0.9),
    );
}

fn shred_warning_level_value(level: Option<TentativeSellRiskLevel>) -> u64 {
    match level.unwrap_or(TentativeSellRiskLevel::Info) {
        TentativeSellRiskLevel::Info => 0,
        TentativeSellRiskLevel::Watch => 1,
        TentativeSellRiskLevel::ExitArmed => 2,
        TentativeSellRiskLevel::EmergencyExitRecommended => 3,
        TentativeSellRiskLevel::EmergencyExitRequired => 4,
    }
}

fn window_return(
    token: &TokenState,
    observed_at: OffsetDateTime,
    window: Duration,
) -> Option<Decimal> {
    let first = token
        .trade_stats
        .price_history
        .iter()
        .find(|(time, _)| *time >= observed_at - window)
        .map(|(_, price)| *price)?;
    if first <= Decimal::ZERO {
        return None;
    }
    Some((token.latest_price - first) / first)
}

fn window_volatility(
    token: &TokenState,
    observed_at: OffsetDateTime,
    window: Duration,
) -> Option<Decimal> {
    let points = token
        .trade_stats
        .price_history
        .iter()
        .filter(|(time, _)| *time >= observed_at - window)
        .collect::<Vec<_>>();
    if points.len() < 2 {
        return None;
    }
    let mut sum = Decimal::ZERO;
    let mut count = 0u64;
    for pair in points.windows(2) {
        let previous = pair[0].1;
        let current = pair[1].1;
        if previous > Decimal::ZERO {
            sum += ((current - previous) / previous).abs();
            count += 1;
        }
    }
    (count > 0).then(|| sum / Decimal::from(count))
}

fn window_velocity(
    token: &TokenState,
    observed_at: OffsetDateTime,
    window: Duration,
) -> Option<Decimal> {
    let first_point = token
        .trade_stats
        .price_history
        .iter()
        .find(|(time, _)| *time >= observed_at - window)?;
    let elapsed_ms = (observed_at - first_point.0).whole_milliseconds();
    (elapsed_ms > 0)
        .then(|| (token.latest_price - first_point.1) / Decimal::from(elapsed_ms as u64))
}

fn time_of_ath(token: &TokenState) -> Option<OffsetDateTime> {
    let ath = token.trade_stats.all_time_high;
    token
        .trade_stats
        .price_history
        .iter()
        .find(|(_, price)| *price == ath)
        .map(|(time, _)| *time)
}

fn count_new_aths(token: &TokenState) -> usize {
    let mut current = Decimal::ZERO;
    let mut count = 0usize;
    for (_, price) in &token.trade_stats.price_history {
        if *price > current {
            current = *price;
            count += 1;
        }
    }
    count
}

fn holder_growth_rate(token: &TokenState, observed_at: OffsetDateTime) -> Option<Decimal> {
    let current = token.holder_state.nonzero_holder_count;
    let first = token
        .holder_state
        .holder_count_history
        .iter()
        .find(|(time, _)| *time >= observed_at - WINDOW_30S)?;
    let elapsed = (observed_at - first.0).whole_seconds();
    (elapsed > 0).then(|| {
        (Decimal::from(current as u64) - Decimal::from(first.1 as u64))
            / Decimal::from(elapsed as u64)
    })
}

fn descending_rank<F>(token: &TokenState, cohort: &[&TokenState], metric: F) -> usize
where
    F: Fn(&TokenState) -> Decimal,
{
    let mut scored = cohort
        .iter()
        .map(|other| (other.mint.0.clone(), metric(other)))
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    scored
        .iter()
        .position(|(mint, _)| mint == &token.mint.0)
        .map(|index| index + 1)
        .unwrap_or(cohort.len())
}

fn same_time_cohort<'a>(
    token: &'a TokenState,
    state: &'a dyn FeatureStateView,
    observed_at: OffsetDateTime,
    window: Duration,
) -> Vec<&'a TokenState> {
    let Some(launch_time) = token.launch_time else {
        return vec![token];
    };
    state
        .tokens()
        .values()
        .filter(|other| {
            other
                .launch_time
                .map(|other_launch| {
                    let delta = (other_launch - launch_time).whole_seconds().abs();
                    other_launch <= observed_at && delta <= window.whole_seconds()
                })
                .unwrap_or(false)
        })
        .collect()
}

fn window_buy_volume(token: &TokenState, observed_at: OffsetDateTime, window: Duration) -> Decimal {
    token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| {
            trade.side == state::TradeSide::Buy && trade.timestamp >= observed_at - window
        })
        .map(|trade| trade.quote)
        .sum()
}

fn window_net_flow(token: &TokenState, observed_at: OffsetDateTime, window: Duration) -> Decimal {
    token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| trade.timestamp >= observed_at - window)
        .map(|trade| match trade.side {
            state::TradeSide::Buy => trade.quote,
            state::TradeSide::Sell => -trade.quote,
        })
        .sum()
}

fn identical_amounts(first_buys: &[&state::TradeObservation]) -> usize {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for trade in first_buys {
        let key = trade.quote.round_dp(4).to_string();
        *counts.entry(key).or_default() += 1;
    }
    counts.values().copied().max().unwrap_or_default()
}

fn same_funder_pair_count(buyers: &[String], state: &dyn FeatureStateView) -> usize {
    let mut funding_map: HashMap<&str, HashSet<&str>> = HashMap::new();
    for edge in state.funding_graph().edges.values() {
        funding_map
            .entry(edge.wallet.as_str())
            .or_default()
            .insert(edge.funder.as_str());
    }

    let mut count = 0usize;
    for (index, left) in buyers.iter().enumerate() {
        for right in buyers.iter().skip(index + 1) {
            let left_funders = funding_map.get(left.as_str());
            let right_funders = funding_map.get(right.as_str());
            if let (Some(left_funders), Some(right_funders)) = (left_funders, right_funders) {
                if !left_funders.is_disjoint(right_funders) {
                    count += 1;
                }
            }
        }
    }
    count
}

fn holder_count_at_window(
    token: &TokenState,
    observed_at: OffsetDateTime,
    window: Duration,
) -> Option<usize> {
    token
        .holder_state
        .holder_count_history
        .iter()
        .find(|(time, _)| *time >= observed_at - window)
        .map(|(_, count)| *count)
}

fn unique_buyers_within(
    token: &TokenState,
    observed_at: OffsetDateTime,
    window: Duration,
) -> usize {
    token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| {
            trade.side == state::TradeSide::Buy && trade.timestamp >= observed_at - window
        })
        .map(|trade| trade.wallet.clone())
        .collect::<HashSet<_>>()
        .len()
}

fn ratio(numerator: usize, denominator: usize) -> Decimal {
    if denominator == 0 {
        Decimal::ZERO
    } else {
        Decimal::from(numerator as u64) / Decimal::from(denominator as u64)
    }
}

fn buyer_retention(token: &TokenState, count: usize) -> Decimal {
    let buyers = token
        .trade_stats
        .trade_history
        .iter()
        .filter(|trade| trade.side == state::TradeSide::Buy)
        .map(|trade| trade.wallet.clone())
        .collect::<Vec<_>>();
    if buyers.is_empty() {
        return Decimal::ZERO;
    }
    let first = buyers.into_iter().take(count).collect::<HashSet<_>>();
    if first.is_empty() {
        return Decimal::ZERO;
    }
    let retained = first
        .iter()
        .filter(|wallet| {
            token
                .holder_state
                .owner_balances
                .get(wallet.as_str())
                .map(|holder| holder.balance > Decimal::ZERO)
                .unwrap_or(false)
        })
        .count();
    ratio(retained, first.len())
}

fn first_sell_durations(token: &TokenState) -> Vec<u64> {
    let mut first_buy = HashMap::<String, OffsetDateTime>::new();
    let mut first_sell = HashMap::<String, OffsetDateTime>::new();
    for trade in &token.trade_stats.trade_history {
        match trade.side {
            state::TradeSide::Buy => {
                first_buy
                    .entry(trade.wallet.clone())
                    .or_insert(trade.timestamp);
            }
            state::TradeSide::Sell => {
                first_sell
                    .entry(trade.wallet.clone())
                    .or_insert(trade.timestamp);
            }
        }
    }
    first_buy
        .into_iter()
        .filter_map(|(wallet, buy_time)| {
            first_sell
                .get(&wallet)
                .map(|sell_time| (*sell_time - buy_time).whole_seconds().max(0) as u64)
        })
        .collect()
}

fn count_by(values: Vec<String>) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for value in values {
        *counts.entry(value).or_default() += 1;
    }
    counts
}

fn max_count(map: &HashMap<String, usize>) -> usize {
    map.values().copied().max().unwrap_or_default()
}

fn dominant_share(values: &[String]) -> Decimal {
    let counts = count_by(values.to_vec());
    let total = counts.values().sum::<usize>();
    if total == 0 {
        Decimal::ZERO
    } else {
        Decimal::from(max_count(&counts) as u64) / Decimal::from(total as u64)
    }
}

fn latest_funder_for_wallet<'a>(
    graph: &'a StateSnapshotFundingGraphShim,
    wallet: &str,
) -> Option<&'a state::FundingEdge> {
    graph
        .edges
        .values()
        .filter(|edge| edge.wallet == wallet)
        .max_by_key(|edge| edge.last_seen)
}

type StateSnapshotFundingGraphShim = state::FundingGraph;

fn wallet_funders(graph: &StateSnapshotFundingGraphShim, wallets: &[String]) -> Vec<String> {
    wallets
        .iter()
        .filter_map(|wallet| {
            latest_funder_for_wallet(graph, wallet).map(|edge| edge.funder.clone())
        })
        .collect()
}

fn overlap_count(values: &[String], target: Option<&str>) -> usize {
    target
        .map(|target| {
            values
                .iter()
                .filter(|value| value.as_str() == target)
                .count()
        })
        .unwrap_or_default()
}

fn repeated_amount_pattern(graph: &StateSnapshotFundingGraphShim, funder: Option<&str>) -> Decimal {
    let mut amounts = Vec::new();
    for edge in graph.edges.values() {
        if funder
            .map(|candidate| candidate == edge.funder)
            .unwrap_or(true)
        {
            amounts.push(edge.amount.round_dp(4).to_string());
        }
    }
    dominant_share(&amounts)
}

fn percentile_against<F>(cohort: &[&TokenState], current: Decimal, metric: F) -> Decimal
where
    F: Fn(&TokenState) -> Decimal,
{
    if cohort.is_empty() {
        return Decimal::ZERO;
    }
    let not_greater = cohort
        .iter()
        .filter(|other| metric(other) <= current)
        .count();
    Decimal::from(not_greater as u64) / Decimal::from(cohort.len() as u64)
}

fn bucket_decimal(value: Decimal, buckets: &[u64]) -> Decimal {
    let mut bucket = 0u64;
    for threshold in buckets {
        if value >= Decimal::from(*threshold) {
            bucket += 1;
        }
    }
    Decimal::from(bucket)
}

fn minimum_move_to_cover_fees(token: &TokenState) -> Decimal {
    let notional = token
        .trade_stats
        .trade_history
        .iter()
        .map(|trade| trade.quote)
        .sum::<Decimal>();
    let total_fees = token
        .trade_stats
        .trade_history
        .iter()
        .map(|trade| {
            Decimal::from(
                trade.priority_fee_lamports.unwrap_or_default()
                    + trade.base_fee_lamports.unwrap_or_default(),
            )
        })
        .sum::<Decimal>();
    if notional > Decimal::ZERO {
        total_fees / Decimal::from(1_000_000_000u64) / notional
    } else {
        Decimal::ZERO
    }
}

fn average_decimal(values: &[Decimal]) -> Option<Decimal> {
    (!values.is_empty())
        .then(|| values.iter().copied().sum::<Decimal>() / Decimal::from(values.len() as u64))
}

fn median_decimal(values: &[Decimal]) -> Option<Decimal> {
    percentile_decimal(values, 50)
}

fn percentile_decimal(values: &[Decimal], percentile: u32) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let index = ((sorted.len() - 1) * percentile as usize) / 100;
    sorted.get(index).copied()
}

fn max_decimal(values: &[Decimal]) -> Option<Decimal> {
    values
        .iter()
        .copied()
        .max_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal))
}

fn parse_uri_scheme(uri: &str) -> String {
    uri.split_once(':')
        .map(|(scheme, _)| scheme.to_lowercase())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn uri_host(uri: &str) -> Option<String> {
    uri.split_once("://")
        .and_then(|(_, rest)| rest.split('/').next())
        .filter(|host| !host.is_empty())
        .map(str::to_owned)
}

fn unique_char_ratio(value: &str) -> Decimal {
    if value.is_empty() {
        return Decimal::ZERO;
    }
    let unique = value.chars().collect::<HashSet<_>>().len();
    Decimal::from(unique as u64) / Decimal::from(value.chars().count() as u64)
}

fn suspicious_unicode_ratio(value: &str) -> Decimal {
    if value.is_empty() {
        return Decimal::ZERO;
    }
    let suspicious = value
        .chars()
        .filter(|character| !character.is_ascii())
        .count();
    Decimal::from(suspicious as u64) / Decimal::from(value.chars().count() as u64)
}

fn holder_growth_rate_window(
    token: &TokenState,
    observed_at: OffsetDateTime,
    window: Duration,
) -> Option<Decimal> {
    let current = token.holder_state.nonzero_holder_count;
    let first = token
        .holder_state
        .holder_count_history
        .iter()
        .find(|(time, _)| *time >= observed_at - window)?;
    let elapsed = (observed_at - first.0).whole_seconds();
    (elapsed > 0).then(|| {
        (Decimal::from(current as u64) - Decimal::from(first.1 as u64))
            / Decimal::from(elapsed as u64)
    })
}

fn bool_decimal(values: &BTreeMap<String, FeatureValue>, feature_id: &str) -> Decimal {
    values
        .get(feature_id)
        .and_then(FeatureValue::as_decimal)
        .unwrap_or(Decimal::ZERO)
}

fn weighted_average(components: &[(Decimal, Decimal)]) -> Decimal {
    let total_weight: Decimal = components.iter().map(|(_, weight)| *weight).sum();
    if total_weight <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    components
        .iter()
        .map(|(value, weight)| *value * *weight)
        .sum::<Decimal>()
        / total_weight
}

fn clamp01(value: Decimal) -> Decimal {
    value.max(Decimal::ZERO).min(Decimal::ONE)
}

#[cfg(test)]
mod tests {
    use common::{
        Canonicality, DEFAULT_PUMP_TOKEN_DECIMALS, EventMeta, EventPayload, EventSource,
        HolderBalanceUpdateEvent, NormalizedEvent, PumpBuyEvent, PumpSellEvent, QuoteAssetType,
        TokenCreatedEvent, TokenProgramType, TransactionStatus, TtlConfig, WalletFundingEvent,
    };
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

    fn token_created(
        mint: &str,
        creator: &str,
        payer: &str,
        name: &str,
        symbol: &str,
    ) -> NormalizedEvent {
        let mut meta = meta(1);
        meta.signature = Some(format!("create-{mint}"));
        NormalizedEvent {
            meta,
            payload: EventPayload::TokenCreated(TokenCreatedEvent {
                mint: pubkey(mint),
                token_program: TokenProgramType::SplToken,
                quote_mint: pubkey("quote"),
                quote_asset_type: QuoteAssetType::WrappedSol,
                creator_wallet: pubkey(creator),
                payer: pubkey(payer),
                bonding_curve_account: pubkey(&format!("curve-{mint}")),
                associated_bonding_curve_account: Some(pubkey(&format!("assoc-{mint}"))),
                metadata_account: Some(pubkey(&format!("meta-{mint}"))),
                name: name.to_owned(),
                symbol: symbol.to_owned(),
                uri: format!("https://example.invalid/{mint}"),
                create_instruction_variant: "create".to_owned(),
                initial_virtual_quote_reserves: Some(Decimal::from(10u64)),
                initial_virtual_token_reserves: Some(Decimal::from(1000u64)),
                initial_real_quote_reserves: Some(Decimal::from(10u64)),
                initial_real_token_reserves: Some(Decimal::from(1000u64)),
                initial_supply: Some(Decimal::from(1_000u64)),
                creator_initial_buy: None,
                same_transaction_buys: 1,
                same_slot_buys: 2,
                fee_recipients: vec![],
                raw_account_list: vec![],
                launch_transaction_fingerprint: Some("fp-a".to_owned()),
                status: common::TransactionStatus::Success,
            }),
        }
    }

    fn buy(slot: u64, mint: &str, buyer: &str, quote: u64, tokens: u64) -> NormalizedEvent {
        let mut meta = meta(slot);
        meta.signature = Some(format!("buy-{slot}-{buyer}"));
        NormalizedEvent {
            meta,
            payload: EventPayload::PumpBuy(PumpBuyEvent {
                mint: pubkey(mint),
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
                is_creator: buyer == "creator-a",
                is_known_cluster_member: false,
                is_first_buy: false,
                status: TransactionStatus::Success,
            }),
        }
    }

    fn sell(slot: u64, mint: &str, seller: &str, quote: u64, tokens: u64) -> NormalizedEvent {
        let mut meta = meta(slot);
        meta.signature = Some(format!("sell-{slot}-{seller}"));
        NormalizedEvent {
            meta,
            payload: EventPayload::PumpSell(PumpSellEvent {
                mint: pubkey(mint),
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
                is_creator: seller == "creator-a",
                is_top_holder_pre_sell: false,
                is_known_cluster_member: false,
                status: TransactionStatus::Success,
            }),
        }
    }

    fn holder(slot: u64, mint: &str, owner: &str, balance: u64) -> NormalizedEvent {
        NormalizedEvent {
            meta: meta(slot),
            payload: EventPayload::HolderBalanceUpdate(HolderBalanceUpdateEvent {
                mint: pubkey(mint),
                owner_wallet: pubkey(owner),
                token_account: pubkey(&format!("ata-{mint}-{owner}")),
                token_decimals: Some(DEFAULT_PUMP_TOKEN_DECIMALS),
                old_balance: None,
                new_balance: Decimal::from(balance),
                delta: Decimal::from(balance),
                caused_by_signature: None,
                update_reason: "trade".to_owned(),
                confidence: Decimal::ONE,
            }),
        }
    }

    #[test]
    fn registry_contains_core_and_placeholder_features() {
        let registry = FeatureRegistry::default();
        assert!(registry.descriptor("launch_slot").is_some());
        assert!(registry.descriptor("wallet_behavior_vector").is_some());
        assert_eq!(
            registry
                .descriptor("wallet_behavior_vector")
                .expect("descriptor")
                .default_status,
            FeatureStatus::Unavailable
        );
    }

    #[test]
    fn feature_snapshot_computes_core_metrics() {
        let mut engine = StateEngine::new(ttl());
        engine
            .apply_event(&token_created(
                "mint-a",
                "creator-a",
                "payer-a",
                "Alpha",
                "ALP",
            ))
            .expect("create");
        engine
            .apply_event(&buy(2, "mint-a", "buyer-a", 10, 100))
            .expect("buy");
        engine
            .apply_event(&buy(3, "mint-a", "buyer-b", 8, 50))
            .expect("buy");
        engine
            .apply_event(&sell(4, "mint-a", "creator-a", 4, 20))
            .expect("sell");
        engine
            .apply_event(&holder(4, "mint-a", "buyer-a", 100))
            .expect("holder");
        engine
            .apply_event(&holder(4, "mint-a", "buyer-b", 50))
            .expect("holder");

        let snapshot = engine.snapshot();
        let token = snapshot.tokens.get("mint-a").expect("token");
        let features = FeatureEngine::default().compute_snapshot(
            token,
            &snapshot,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
        );
        assert_eq!(
            features.value("mint").unwrap().status,
            FeatureStatus::Available
        );
        assert!(features.decimal("buy_count").unwrap() >= Decimal::from(2u64));
        assert!(features.decimal("holder_count_current").unwrap() >= Decimal::from(2u64));
        assert!(features.decimal("rug_probability_score").unwrap() >= Decimal::ZERO);
    }

    #[test]
    fn holder_denominator_features_use_ui_supply_units() {
        let mut engine = StateEngine::new(ttl());
        engine
            .apply_event(&token_created(
                "mint-a",
                "creator-a",
                "payer-a",
                "Alpha",
                "ALP",
            ))
            .expect("create");
        engine
            .apply_event(&holder(2, "mint-a", "buyer-a", 500_000_000_000_000))
            .expect("holder-a");
        engine
            .apply_event(&holder(3, "mint-a", "buyer-b", 250_000_000_000_000))
            .expect("holder-b");

        let snapshot = engine.snapshot();
        let token = snapshot.tokens.get("mint-a").expect("token");
        let features = FeatureEngine::default().compute_snapshot(
            token,
            &snapshot,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
        );

        assert_eq!(
            features.decimal("observed_holder_supply").unwrap(),
            Decimal::from(750_000_000u64)
        );
        assert_eq!(
            features.decimal("top1_holder_pct_total_supply").unwrap(),
            dec!(0.5)
        );
        assert_eq!(
            features
                .decimal("top1_holder_pct_curve_economic_supply")
                .unwrap(),
            dec!(0.5)
        );
        assert_eq!(
            features
                .value("token_supply_selected_for_holder_pct")
                .expect("holder denominator")
                .status,
            FeatureStatus::Available
        );
        assert_eq!(
            features.decimal("top1_holder_pct_circulating").unwrap(),
            dec!(0.5)
        );
        assert_eq!(
            features.decimal("top1_holder_pct_observed").unwrap(),
            dec!(0.6666666666666666666666666667)
        );
    }

    #[test]
    fn dev_holding_prefers_owner_summed_holder_balance() {
        let mut engine = StateEngine::new(ttl());
        engine
            .apply_event(&token_created(
                "mint-a",
                "creator-a",
                "payer-a",
                "Alpha",
                "ALP",
            ))
            .expect("create");
        engine
            .apply_event(&buy(2, "mint-a", "creator-a", 10, 800_000_000_000_000))
            .expect("creator buy flow");
        engine
            .apply_event(&holder(3, "mint-a", "creator-a", 100_000_000_000_000))
            .expect("creator holder snapshot");

        let snapshot = engine.snapshot();
        let token = snapshot.tokens.get("mint-a").expect("token");
        let features = FeatureEngine::default().compute_snapshot(
            token,
            &snapshot,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
        );

        assert_eq!(
            features.decimal("dev_balance"),
            Some(Decimal::from(100_000_000u64))
        );
        assert_eq!(
            features.decimal("dev_holding_pct_total_supply"),
            Some(dec!(0.1))
        );
        assert_eq!(
            features.decimal("dev_holding_pct_curve_economic_supply"),
            Some(dec!(0.1))
        );
        assert_eq!(features.decimal("creator_ownership_pct"), Some(dec!(0.1)));
    }

    #[test]
    fn dev_holding_over_total_supply_is_unavailable_not_clamped() {
        let mut engine = StateEngine::new(ttl());
        engine
            .apply_event(&token_created(
                "mint-a",
                "creator-a",
                "payer-a",
                "Alpha",
                "ALP",
            ))
            .expect("create");
        engine
            .apply_event(&holder(3, "mint-a", "creator-a", 1_000_000_001_000_000))
            .expect("invalid creator holder snapshot");

        let snapshot = engine.snapshot();
        let token = snapshot.tokens.get("mint-a").expect("token");
        let features = FeatureEngine::default().compute_snapshot(
            token,
            &snapshot,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
        );

        assert_eq!(
            features
                .value("dev_holding_pct_total_supply")
                .expect("dev pct")
                .status,
            FeatureStatus::Unavailable
        );
        assert_eq!(
            features
                .value("creator_ownership_pct")
                .expect("creator ownership")
                .status,
            FeatureStatus::Unavailable
        );
    }

    #[test]
    fn cohort_and_rotation_features_use_global_snapshot() {
        let mut engine = StateEngine::new(ttl());
        engine
            .apply_event(&token_created(
                "mint-a",
                "creator-a",
                "payer-a",
                "Alpha",
                "ALP",
            ))
            .expect("create");
        engine
            .apply_event(&token_created(
                "mint-b",
                "creator-b",
                "payer-b",
                "Beta",
                "BET",
            ))
            .expect("create");
        engine
            .apply_event(&buy(2, "mint-a", "buyer-a", 10, 100))
            .expect("buy");
        engine
            .apply_event(&buy(2, "mint-b", "buyer-a", 2, 20))
            .expect("buy");
        engine
            .apply_event(&NormalizedEvent {
                meta: meta(2),
                payload: EventPayload::WalletFunding(WalletFundingEvent {
                    wallet: pubkey("buyer-a"),
                    funder: pubkey("funder-a"),
                    asset_label: "SOL".to_owned(),
                    amount: Decimal::from(2u64),
                    slot: 2,
                    signature: "fund-a".to_owned(),
                    relation_to_launch: Some("before_launch".to_owned()),
                    near_launch_relation: true,
                    funding_graph_edge_id: "edge-a".to_owned(),
                }),
            })
            .expect("fund");
        engine
            .apply_event(&sell(3, "mint-b", "buyer-a", 1, 10))
            .expect("sell");

        let snapshot = engine.snapshot();
        let token = snapshot.tokens.get("mint-a").expect("token");
        let features = FeatureEngine::default().compute_snapshot(
            token,
            &snapshot,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
        );
        assert!(features.decimal("cohort_launch_count").unwrap() >= Decimal::from(2u64));
        assert!(
            features
                .decimal("wallets_sold_other_token_then_bought_this_count")
                .unwrap()
                >= Decimal::ZERO
        );
    }

    #[test]
    fn placeholder_features_remain_guarded_unavailable() {
        let mut engine = StateEngine::new(ttl());
        engine
            .apply_event(&token_created(
                "mint-a",
                "creator-a",
                "payer-a",
                "Alpha",
                "ALP",
            ))
            .expect("create");
        let snapshot = engine.snapshot();
        let token = snapshot.tokens.get("mint-a").expect("token");
        let features = FeatureEngine::default().compute_snapshot(
            token,
            &snapshot,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(2),
        );
        let placeholder = features
            .value("wallet_behavior_vector")
            .expect("placeholder");
        assert_eq!(placeholder.status, FeatureStatus::Unavailable);
        assert_eq!(placeholder.confidence, Decimal::ZERO);
    }
}
