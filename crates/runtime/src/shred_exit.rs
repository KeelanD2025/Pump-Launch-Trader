use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Result, anyhow};
use common::{
    AppConfig, Canonicality, DangerousSellerClassification, EarlyIntentConfig, EarlyIntentSource,
    EventId, EventMeta, EventPayload, EventSource, NormalizedEvent, PumpSellEvent, ReasonCode,
    ShredEmergencyExitArmedEvent, ShredEmergencyExitTriggeredEvent, ShredExitConfig,
    ShredSellIntentResolvedEvent, TentativeMaliciousSellWarningEvent,
    TentativeSellConfirmationState, TentativeSellIntentDetectedEvent,
    TentativeSellResolutionOutcome, TentativeSellRiskLevel,
};
use decision::OpenPositionContext;
use features::FeatureSnapshot;
use risk::RiskAssessment;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use state::{StateSnapshot, ThreatWalletEntry, TokenState};
use time::{Duration, OffsetDateTime};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShredExitMetricsSummary {
    pub tentative_sell_total: u64,
    pub malicious_sell_warning_total: u64,
    pub emergency_exits_armed_total: u64,
    pub emergency_exits_triggered_total: u64,
    pub emergency_exits_rejected_total: u64,
    pub confirmed_executed_total: u64,
    pub confirmed_failed_total: u64,
    pub not_seen_within_ttl_total: u64,
    pub reorged_total: u64,
    pub decode_mismatch_total: u64,
    pub account_effect_confirmed_total: u64,
    pub false_positive_exit_total: u64,
    pub saved_loss_quote_total: Decimal,
    pub opportunity_cost_quote_total: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourceShredExitMetricsSummary {
    pub tentative_sell_total: u64,
    pub malicious_sell_warning_total: u64,
    pub emergency_exits_armed_total: u64,
    pub emergency_exits_triggered_total: u64,
    pub confirmed_executed_total: u64,
    pub confirmed_failed_total: u64,
    pub not_seen_within_ttl_total: u64,
    pub reorged_total: u64,
    pub decode_mismatch_total: u64,
    pub account_effect_confirmed_total: u64,
    pub false_positive_exit_total: u64,
    pub saved_loss_quote_total: Decimal,
    pub opportunity_cost_quote_total: Decimal,
    pub deduplicated_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShredExitCalibrationSummary {
    pub current_paper_exit_impact_pct: Decimal,
    pub current_paper_confidence_threshold: Decimal,
    pub false_positive_rate: Decimal,
    pub missed_sell_rate: Decimal,
    pub true_positive_rate: Decimal,
    pub adaptations: Vec<String>,
    pub persisted_path: Option<String>,
    pub persisted_version_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShredExitCalibrationBucket {
    pub sample_count: u64,
    pub confirmed_executed_count: u64,
    pub confirmed_failed_count: u64,
    pub account_effect_confirmed_count: u64,
    pub not_seen_count: u64,
    pub reorg_count: u64,
    pub decode_mismatch_count: u64,
    pub false_positive_count: u64,
    pub true_positive_count: u64,
    pub emergency_exit_count: u64,
    pub false_exit_count: u64,
    pub missed_exit_count: u64,
    pub saved_loss_total: Decimal,
    pub saved_loss_mean: Decimal,
    pub opportunity_cost_total: Decimal,
    pub opportunity_cost_mean: Decimal,
    pub false_positive_rate: Decimal,
    pub true_positive_rate: Decimal,
    pub precision: Decimal,
    pub recall: Decimal,
    pub average_early_to_geyser_processed_ms: Decimal,
    pub average_early_to_account_effect_ms: Decimal,
    pub average_exit_latency_ms: Decimal,
    pub threshold_adjustment: Decimal,
    pub last_updated_at: Option<OffsetDateTime>,
    pub schema_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistedShredExitCalibration {
    pub schema_version: u32,
    pub current_paper_exit_impact_pct: Decimal,
    pub current_paper_confidence_threshold: Decimal,
    pub threshold_adjustment: Decimal,
    pub buckets: BTreeMap<String, ShredExitCalibrationBucket>,
    pub last_updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CalibrationObservation {
    key: String,
    source: EarlyIntentSource,
    seller_classification: DangerousSellerClassification,
    confirmation_method: Option<String>,
    confidence_bucket: String,
    impact_bucket: String,
    outcome: TentativeSellResolutionOutcome,
    emergency_exit: bool,
    false_positive: bool,
    saved_loss_quote: Decimal,
    opportunity_cost_quote: Decimal,
    early_to_geyser_processed_ms: Option<i64>,
    early_to_account_effect_ms: Option<i64>,
    exit_latency_ms: Option<i64>,
    strategy: Option<String>,
    latency_advantage_ms: Option<i64>,
    required_latency_advantage_ms: Option<i64>,
    latency_edge_ratio: Decimal,
    net_benefit_quote: Decimal,
    absorption_health_score: Decimal,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct PendingSellContext {
    intent: TentativeSellIntentDetectedEvent,
    warning: TentativeMaliciousSellWarningEvent,
    armed: Option<ShredEmergencyExitArmedEvent>,
    triggered: Option<ShredEmergencyExitTriggeredEvent>,
    position_size_tokens: Decimal,
    position_size_quote: Decimal,
    position_strategy: Option<String>,
    warning_price: Decimal,
    latency_advantage_ms: i64,
    required_latency_advantage_ms: i64,
    latency_edge_ratio: Decimal,
    net_benefit_quote: Decimal,
    absorption_health_score: Decimal,
    expires_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct TentativeSellManager {
    early_intent: EarlyIntentConfig,
    shred_exit: ShredExitConfig,
    pending: HashMap<String, PendingSellContext>,
    metrics: ShredExitMetricsSummary,
    source_metrics: BTreeMap<String, SourceShredExitMetricsSummary>,
    calibration: ShredExitCalibrationSummary,
    loaded_persisted: PersistedShredExitCalibration,
    observations: Vec<CalibrationObservation>,
    dedup_pairs: BTreeMap<String, u64>,
}

impl TentativeSellManager {
    pub fn new(config: &AppConfig) -> Self {
        let current_paper_exit_impact_pct = config.shred_exit.paper_exit_impact_pct;
        let current_paper_confidence_threshold = config.shred_exit.min_decode_confidence_paper_exit;
        let mut loaded_persisted = PersistedShredExitCalibration {
            schema_version: common::SCHEMA_VERSION,
            current_paper_exit_impact_pct,
            current_paper_confidence_threshold,
            ..PersistedShredExitCalibration::default()
        };
        let mut manager = Self {
            early_intent: config.early_intent.clone(),
            shred_exit: config.shred_exit.clone(),
            pending: HashMap::new(),
            metrics: ShredExitMetricsSummary::default(),
            source_metrics: BTreeMap::new(),
            calibration: ShredExitCalibrationSummary {
                current_paper_exit_impact_pct,
                current_paper_confidence_threshold,
                persisted_path: Some(calibration_path(config).display().to_string()),
                ..ShredExitCalibrationSummary::default()
            },
            loaded_persisted: loaded_persisted.clone(),
            observations: Vec::new(),
            dedup_pairs: BTreeMap::new(),
        };
        if let Ok(persisted) = load_persisted_calibration(config) {
            loaded_persisted = persisted.clone();
            if persisted.current_paper_exit_impact_pct > Decimal::ZERO {
                manager.calibration.current_paper_exit_impact_pct =
                    persisted.current_paper_exit_impact_pct;
            }
            if persisted.current_paper_confidence_threshold > Decimal::ZERO {
                manager.calibration.current_paper_confidence_threshold =
                    persisted.current_paper_confidence_threshold;
            }
            manager.calibration.persisted_version_hash = Some(persisted_hash(&persisted));
        }
        manager.loaded_persisted = loaded_persisted;
        manager
    }

    pub fn validate_sources(&self, config: &AppConfig) -> Result<()> {
        if let Some(deshred) = config.ingest.deshred.as_ref() {
            let endpoint_available = !deshred.endpoint.trim().is_empty()
                || (!deshred.endpoint_env.trim().is_empty()
                    && std::env::var(&deshred.endpoint_env)
                        .ok()
                        .map(|value| !value.trim().is_empty())
                        .unwrap_or(false));
            if deshred.enabled && deshred.required && !endpoint_available {
                return Err(anyhow!(
                    "deshred is required but no endpoint is configured; set ingest.deshred.endpoint or {}",
                    if deshred.endpoint_env.trim().is_empty() {
                        "GEYSER_ENDPOINT"
                    } else {
                        deshred.endpoint_env.as_str()
                    }
                ));
            }
            let auth_available = !deshred.auth_token_env.trim().is_empty()
                && std::env::var(&deshred.auth_token_env)
                    .ok()
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false);
            if deshred.enabled && deshred.auth_required && !auth_available {
                return Err(anyhow!(
                    "deshred auth is required but no auth token is available in {}",
                    if deshred.auth_token_env.trim().is_empty() {
                        "GEYSER_AUTH_TOKEN"
                    } else {
                        deshred.auth_token_env.as_str()
                    }
                ));
            }
        }
        if config.shred.enabled
            && config.shred.decoder == common::ShredDecoderMode::Production
            && !self.early_intent.allow_production_raw_shred
            && config.shred.required
        {
            return Err(anyhow!(
                "production raw shred early-intent source is unavailable because the production shred decoder is disabled"
            ));
        }
        Ok(())
    }

    pub fn metrics(&self) -> &ShredExitMetricsSummary {
        &self.metrics
    }

    pub fn calibration(&self) -> &ShredExitCalibrationSummary {
        &self.calibration
    }

    pub fn source_metrics(&self) -> &BTreeMap<String, SourceShredExitMetricsSummary> {
        &self.source_metrics
    }

    pub fn dedup_pairs(&self) -> &BTreeMap<String, u64> {
        &self.dedup_pairs
    }

    pub fn has_tentative_activity(&self) -> bool {
        self.metrics.tentative_sell_total > 0 || !self.observations.is_empty()
    }

    pub fn persisted_calibration(&self, _config: &AppConfig) -> PersistedShredExitCalibration {
        let mut persisted = self.loaded_persisted.clone();
        persisted.schema_version = common::SCHEMA_VERSION;
        if persisted.current_paper_exit_impact_pct <= Decimal::ZERO {
            persisted.current_paper_exit_impact_pct =
                self.calibration.current_paper_exit_impact_pct;
        }
        if persisted.current_paper_confidence_threshold <= Decimal::ZERO {
            persisted.current_paper_confidence_threshold =
                self.calibration.current_paper_confidence_threshold;
        }
        apply_observations_to_persisted(&mut persisted, &self.observations, &self.shred_exit);
        persisted.last_updated_at = Some(OffsetDateTime::now_utc());
        persisted
    }

    pub fn persist_calibration(&mut self, config: &AppConfig) -> Result<()> {
        if !self.shred_exit.calibration.enabled || !self.shred_exit.calibration.persist_calibration
        {
            return Ok(());
        }
        let persisted = self.persisted_calibration(config);
        let path = calibration_path(config);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, serde_json::to_vec_pretty(&persisted)?)?;
        self.loaded_persisted = persisted.clone();
        self.calibration.persisted_version_hash = Some(persisted_hash(&persisted));
        Ok(())
    }

    pub fn ensure_calibration_snapshot(&self, config: &AppConfig) -> Result<(String, PathBuf)> {
        let persisted = self.loaded_persisted.clone();
        let hash = persisted_hash(&persisted);
        let base_path = calibration_path(config);
        let snapshot_path = base_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("snapshots")
            .join(format!("{hash}.json"));
        if let Some(parent) = snapshot_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if !snapshot_path.exists() {
            fs::write(&snapshot_path, serde_json::to_vec_pretty(&persisted)?)?;
        }
        Ok((hash, snapshot_path))
    }

    pub fn detect_tentative_sell(
        &mut self,
        event: &NormalizedEvent,
        token: &TokenState,
        features: &FeatureSnapshot,
        risk: &RiskAssessment,
        position: Option<&OpenPositionContext>,
    ) -> Vec<NormalizedEvent> {
        let EventPayload::PumpSell(payload) = &event.payload else {
            return Vec::new();
        };
        if event.meta.canonicality != Canonicality::Tentative || !self.early_intent.enabled {
            return Vec::new();
        }

        let source = infer_early_intent_source(event);
        let seller_entry = find_threat_entry(token, &payload.seller.0);
        let seller_top_rank = top_holder_rank(token, &payload.seller.0);
        let classification = seller_entry
            .as_ref()
            .map(|entry| entry.classification)
            .unwrap_or_else(|| classify_without_index(token, payload));
        let decode_confidence = apply_partial_penalty(event.meta.decode_confidence, event);
        let account_confidence =
            account_confidence_for_sell(token, payload, seller_entry, seller_top_rank);
        let holding_pct = seller_entry
            .as_ref()
            .map(|entry| entry.holding_pct)
            .unwrap_or(Decimal::ZERO);
        let seller_balance = seller_entry
            .as_ref()
            .map(|entry| entry.balance)
            .unwrap_or(payload.token_in);
        let decoded_impact_pct = match (payload.price_before, payload.price_after) {
            (Some(before), Some(after)) => price_impact_pct(before, after),
            _ => Decimal::ZERO,
        };
        let estimated_impact_pct = seller_entry
            .as_ref()
            .map(|entry| {
                if seller_balance > Decimal::ZERO {
                    let fraction = (payload.token_in / seller_balance).min(Decimal::ONE);
                    (entry.estimated_full_exit_impact_pct * fraction).max(decoded_impact_pct)
                } else {
                    entry.estimated_full_exit_impact_pct.max(decoded_impact_pct)
                }
            })
            .unwrap_or_else(|| fallback_impact_pct(token, payload).max(decoded_impact_pct));
        let same_slot_existing = self
            .pending
            .values()
            .filter(|pending| {
                pending.intent.mint == payload.mint && pending.intent.slot == Some(event.meta.slot)
            })
            .collect::<Vec<_>>();
        let effective_impact_pct = (estimated_impact_pct
            + same_slot_existing
                .iter()
                .map(|pending| pending.intent.estimated_price_impact_pct)
                .sum::<Decimal>())
        .min(Decimal::from(100u64));
        let cluster_impact_pct = seller_entry
            .as_ref()
            .map(|entry| entry.cluster_holding_pct * Decimal::from(100u64))
            .unwrap_or(Decimal::ZERO);
        let catastrophic = is_catastrophic_classification(classification);
        let latency_advantage_ms = event.meta.source_latency_ms.unwrap_or_default().max(0);
        let required_latency_ms = token
            .shred_defense
            .exit_threat_index
            .required_early_intent_lead_time_ms
            .max(1);
        let latency_edge_ratio =
            Decimal::from(latency_advantage_ms) / Decimal::from(required_latency_ms);
        let latency_edge_sufficient = latency_edge_ratio >= self.shred_exit.min_latency_edge_ratio;
        let position_size_quote = position
            .map(|value| value.size_quote)
            .unwrap_or(Decimal::ZERO);
        let absorption_health_score = features
            .decimal("absorption_success_score")
            .unwrap_or(Decimal::ZERO);
        let post_sell_absorption_probability = clamp01(
            absorption_health_score
                + features
                    .decimal("holder_stickiness_score")
                    .unwrap_or(Decimal::ZERO)
                    / Decimal::from(3u64)
                + features
                    .decimal("holder_growth_rate")
                    .unwrap_or(Decimal::ZERO)
                    / Decimal::from(4u64),
        );
        let impact_severity_multiplier = (effective_impact_pct / Decimal::from(100u64))
            .min(Decimal::ONE)
            .max(Decimal::ZERO);
        let absorption_preservation_multiplier =
            if !catastrophic && self.shred_exit.absorption_downgrade_enabled {
                (Decimal::ONE - absorption_health_score)
                    .max(Decimal::new(10, 2))
                    .min(Decimal::ONE)
            } else {
                Decimal::ONE
            };
        let estimated_saved_loss_quote = if position_size_quote > Decimal::ZERO {
            position_size_quote
                * (effective_impact_pct / Decimal::from(100u64))
                * latency_edge_ratio.min(Decimal::ONE).max(Decimal::ZERO)
                * impact_severity_multiplier
                * absorption_preservation_multiplier
        } else {
            Decimal::ZERO
        };
        let absorption_penalty_quote = if !catastrophic
            && self.shred_exit.absorption_downgrade_enabled
            && absorption_health_score >= self.shred_exit.absorption_downgrade_threshold
        {
            position_size_quote * absorption_health_score * Decimal::new(20, 2)
        } else {
            Decimal::ZERO
        };
        let expected_opportunity_cost_quote = if position_size_quote > Decimal::ZERO {
            position_size_quote
                * (self.calibration.current_paper_exit_impact_pct / Decimal::from(100u64))
                + absorption_penalty_quote
        } else {
            Decimal::ZERO
        };
        let emergency_net_benefit_quote =
            estimated_saved_loss_quote - expected_opportunity_cost_quote;
        let emergency_net_benefit_positive = estimated_saved_loss_quote
            >= self.shred_exit.min_expected_saved_loss_quote
            && emergency_net_benefit_quote >= self.shred_exit.min_emergency_exit_net_benefit_quote;
        let emergency_net_benefit_confidence = clamp01(
            decode_confidence.min(account_confidence)
                * latency_edge_ratio.min(Decimal::ONE).max(Decimal::ZERO)
                * (Decimal::ONE - post_sell_absorption_probability / Decimal::from(2u64)),
        );
        let mut warning_level = determine_warning_level(
            classification,
            effective_impact_pct,
            decode_confidence.min(account_confidence),
            self.calibration.current_paper_exit_impact_pct,
            &self.shred_exit,
        );
        if matches!(
            warning_level,
            TentativeSellRiskLevel::EmergencyExitRecommended
                | TentativeSellRiskLevel::EmergencyExitRequired
        ) {
            let downgrade_for_absorption = !catastrophic
                && self.shred_exit.absorption_downgrade_enabled
                && absorption_health_score >= self.shred_exit.absorption_downgrade_threshold;
            let downgrade_for_latency = !catastrophic && !latency_edge_sufficient;
            let downgrade_for_net_benefit = position.is_some() && !emergency_net_benefit_positive;
            if downgrade_for_absorption || downgrade_for_latency || downgrade_for_net_benefit {
                warning_level = if self.shred_exit.allow_arm_when_net_benefit_negative {
                    TentativeSellRiskLevel::ExitArmed
                } else {
                    TentativeSellRiskLevel::Watch
                };
            }
        }
        let mut warning_reasons =
            warning_reason_codes(classification, warning_level, source, decode_confidence);
        if self.shred_exit.same_slot_multi_sell_escalation && !same_slot_existing.is_empty() {
            warning_reasons.push(ReasonCode::ShredSameSlotSellCluster);
        }
        if !latency_edge_sufficient {
            warning_reasons.push(ReasonCode::ShredSignalStale);
        }
        let trigger_event_id = EventId::from_seed(&format!(
            "tentative-trigger|{}|{}|{}|{}|{}",
            event.meta.event_id.0,
            payload.mint.0,
            payload.seller.0,
            event.meta.slot,
            event.meta.transaction_index.unwrap_or_default()
        ));
        let wall_time = event.meta.received_at_wall_time;
        let seller_cost_basis = token
            .holder_state
            .owner_balances
            .get(&payload.seller.0)
            .map(|holder| holder.cost_basis.estimated_cost_basis_quote)
            .unwrap_or(Decimal::ZERO);
        let seller_unrealized = token
            .holder_state
            .owner_balances
            .get(&payload.seller.0)
            .map(|holder| holder.cost_basis.estimated_unrealized_pnl)
            .unwrap_or(Decimal::ZERO);
        let intent = TentativeSellIntentDetectedEvent {
            event_id: trigger_event_id.clone(),
            source,
            canonicality: Canonicality::Tentative,
            slot: Some(event.meta.slot),
            entry_index: event.meta.transaction_index,
            shred_index: event
                .meta
                .raw_reference
                .as_ref()
                .and_then(|reference| reference.offset.map(|value| value as u32)),
            tx_position_estimate: event.meta.transaction_index,
            observed_at_monotonic_ns: event.meta.observed_at_monotonic_ns,
            received_at_wall_time: wall_time,
            signature: event.signature().map(ToOwned::to_owned),
            mint: payload.mint.clone(),
            seller_wallet: payload.seller.clone(),
            payer_wallet: None,
            token_in_estimate: payload.token_in,
            quote_out_estimate: payload.quote_out,
            min_quote_output: payload.min_quote_output,
            sell_instruction_variant: "sell".to_owned(),
            decoded_instruction_confidence: decode_confidence,
            account_decode_confidence: account_confidence,
            wallet_classification_snapshot: format!("{classification:?}").to_lowercase(),
            seller_balance_before_estimate: seller_balance,
            seller_holding_pct_estimate: holding_pct,
            seller_cost_basis_estimate: seller_cost_basis,
            seller_unrealized_pnl_estimate: seller_unrealized,
            estimated_price_before: payload.price_before.unwrap_or(token.latest_price),
            estimated_price_after: payload.price_after.unwrap_or_else(|| {
                let move_pct = effective_impact_pct / Decimal::from(100u64);
                token.latest_price * (Decimal::ONE - move_pct).max(Decimal::ZERO)
            }),
            estimated_price_impact_pct: effective_impact_pct,
            estimated_curve_impact: effective_impact_pct / Decimal::from(100u64),
            estimated_top_holder_rank: seller_top_rank.map(|value| value as u64),
            estimated_cluster_id: seller_entry
                .as_ref()
                .and_then(|entry| entry.cluster_id.clone()),
            estimated_cluster_holding_pct: cluster_impact_pct / Decimal::from(100u64),
            reason_codes: warning_reasons.clone(),
            raw_packet_hash: event
                .meta
                .raw_reference
                .as_ref()
                .map(|reference| reference.source_id.clone()),
            raw_entry_hash: event
                .meta
                .raw_reference
                .as_ref()
                .and_then(|reference| reference.cursor.clone()),
            raw_update_hash: event
                .signature()
                .map(|signature| format!("tentative:{signature}")),
            matched_canonical_signature: None,
            confirmation_state: TentativeSellConfirmationState::PendingTentative,
            schema_version: common::SCHEMA_VERSION,
        };
        let warning = TentativeMaliciousSellWarningEvent {
            mint: payload.mint.clone(),
            seller_wallet: payload.seller.clone(),
            seller_classification: classification,
            estimated_sell_impact_pct: effective_impact_pct,
            estimated_cluster_sell_impact_pct: cluster_impact_pct,
            risk_level: warning_level,
            confidence: decode_confidence.min(account_confidence),
            reason_codes: warning_reasons.clone(),
            source_latency_advantage_ms: Some(latency_advantage_ms.min(i64::MAX as u64) as i64),
            required_latency_advantage_ms: Some(required_latency_ms.min(i64::MAX as u64) as i64),
            latency_edge_ratio,
            exit_can_land_before_estimated_impact: latency_edge_sufficient,
            absorption_health_score,
            post_sell_absorption_probability,
            emergency_exit_expected_saved_loss: estimated_saved_loss_quote,
            emergency_exit_expected_opportunity_cost: expected_opportunity_cost_quote,
            emergency_exit_net_benefit: emergency_net_benefit_quote,
            emergency_exit_net_benefit_confidence: emergency_net_benefit_confidence,
            canonicality: Canonicality::Tentative,
            feature_snapshot_hash: features.vector_hash.clone(),
            risk_snapshot_hash: format!(
                "risk:{}:{}",
                risk.mint,
                risk.observed_at.unix_timestamp_nanos()
            ),
            trigger_event_id: trigger_event_id.clone(),
            schema_version: common::SCHEMA_VERSION,
        };
        let source_key = source_label(source);
        self.source_metrics
            .entry(source_key.clone())
            .or_default()
            .tentative_sell_total = self
            .source_metrics
            .get(&source_key)
            .map(|metrics| metrics.tentative_sell_total)
            .unwrap_or_default()
            .saturating_add(1);
        let duplicate_primary_source = if self.early_intent.deduplicate_sources {
            self.pending
                .values()
                .find(|pending| {
                    pending_matches_duplicate_source(
                        pending,
                        &intent,
                        self.early_intent.dedup_amount_tolerance_pct,
                        self.early_intent.dedup_slot_tolerance,
                    )
                })
                .map(|pending| pending.intent.source)
        } else {
            None
        };
        if let Some(primary_source) = duplicate_primary_source {
            self.source_metrics
                .entry(source_key.clone())
                .or_default()
                .deduplicated_total = self
                .source_metrics
                .get(&source_key)
                .map(|metrics| metrics.deduplicated_total)
                .unwrap_or_default()
                .saturating_add(1);
            *self
                .dedup_pairs
                .entry(format!("{}|{}", source_label(primary_source), source_key))
                .or_default() += 1;
            return vec![clone_with_payload(
                event,
                EventPayload::TentativeSellIntentDetected(intent),
            )];
        }
        let armed = position
            .filter(|_| {
                matches!(
                    warning_level,
                    TentativeSellRiskLevel::ExitArmed
                        | TentativeSellRiskLevel::EmergencyExitRecommended
                        | TentativeSellRiskLevel::EmergencyExitRequired
                )
            })
            .map(|position| ShredEmergencyExitArmedEvent {
                mint: payload.mint.clone(),
                position_id: payload.mint.0.clone(),
                trigger_event_id: trigger_event_id.clone(),
                seller_wallet: payload.seller.clone(),
                seller_classification: classification,
                risk_level: warning_level,
                estimated_impact_pct: effective_impact_pct,
                confidence: warning.confidence,
                planned_exit_size: position.size_tokens,
                planned_exit_reason: "tentative_sell_risk".to_owned(),
                source,
                expires_at: wall_time
                    + Duration::milliseconds(
                        self.shred_exit.tentative_reconciliation_ttl_ms as i64,
                    ),
                cancel_conditions: vec!["tentative_resolved_false_positive".to_owned()],
                escalation_conditions: vec!["canonical_confirmation".to_owned()],
                schema_version: common::SCHEMA_VERSION,
            });
        let triggered = if let Some(position) = position {
            if self.shred_exit.paper_enabled
                && self.shred_exit.allow_paper_exit_on_tentative
                && warning.confidence >= self.calibration.current_paper_confidence_threshold
                && effective_impact_pct >= self.calibration.current_paper_exit_impact_pct
                && (emergency_net_benefit_positive
                    || self.shred_exit.allow_emergency_when_net_benefit_negative)
                && matches!(
                    warning_level,
                    TentativeSellRiskLevel::EmergencyExitRecommended
                        | TentativeSellRiskLevel::EmergencyExitRequired
                )
            {
                Some(ShredEmergencyExitTriggeredEvent {
                    mint: payload.mint.clone(),
                    position_id: payload.mint.0.clone(),
                    trigger_event_id: trigger_event_id.clone(),
                    decision_id: trigger_event_id.0.to_string(),
                    seller_wallet: payload.seller.clone(),
                    seller_classification: classification,
                    side: common::ExecutionSide::Sell,
                    exit_size: position.size_tokens,
                    reason_codes: warning_reasons.clone(),
                    source,
                    confidence: warning.confidence,
                    live_allowed: false,
                    paper_allowed: true,
                    expected_exit_price: payload.price_before.unwrap_or(token.latest_price),
                    expected_fee_adjusted_exit_value: position.size_tokens
                        * payload.price_before.unwrap_or(token.latest_price),
                    estimated_saved_loss_vs_waiting_for_geyser: estimated_saved_loss_quote,
                    schema_version: common::SCHEMA_VERSION,
                })
            } else {
                None
            }
        } else {
            None
        };

        self.metrics.tentative_sell_total = self.metrics.tentative_sell_total.saturating_add(1);
        self.metrics.malicious_sell_warning_total =
            self.metrics.malicious_sell_warning_total.saturating_add(1);
        self.source_metrics
            .entry(source_key.clone())
            .or_default()
            .malicious_sell_warning_total = self
            .source_metrics
            .get(&source_key)
            .map(|metrics| metrics.malicious_sell_warning_total)
            .unwrap_or_default()
            .saturating_add(1);
        if armed.is_some() {
            self.metrics.emergency_exits_armed_total =
                self.metrics.emergency_exits_armed_total.saturating_add(1);
            self.source_metrics
                .entry(source_key.clone())
                .or_default()
                .emergency_exits_armed_total = self
                .source_metrics
                .get(&source_key)
                .map(|metrics| metrics.emergency_exits_armed_total)
                .unwrap_or_default()
                .saturating_add(1);
        }
        if triggered.is_some() {
            self.metrics.emergency_exits_triggered_total = self
                .metrics
                .emergency_exits_triggered_total
                .saturating_add(1);
            self.source_metrics
                .entry(source_key.clone())
                .or_default()
                .emergency_exits_triggered_total = self
                .source_metrics
                .get(&source_key)
                .map(|metrics| metrics.emergency_exits_triggered_total)
                .unwrap_or_default()
                .saturating_add(1);
        }

        self.pending.insert(
            intent.event_id.0.to_string(),
            PendingSellContext {
                intent: intent.clone(),
                warning: warning.clone(),
                armed: armed.clone(),
                triggered: triggered.clone(),
                position_size_tokens: position
                    .map(|value| value.size_tokens)
                    .unwrap_or(Decimal::ZERO),
                position_size_quote: position
                    .map(|value| value.size_quote)
                    .unwrap_or(Decimal::ZERO),
                position_strategy: position
                    .map(|value| format!("{:?}", value.strategy).to_lowercase()),
                warning_price: payload.price_before.unwrap_or(token.latest_price),
                latency_advantage_ms: latency_advantage_ms.min(i64::MAX as u64) as i64,
                required_latency_advantage_ms: required_latency_ms.min(i64::MAX as u64) as i64,
                latency_edge_ratio,
                net_benefit_quote: emergency_net_benefit_quote,
                absorption_health_score,
                expires_at: wall_time
                    + Duration::milliseconds(
                        self.shred_exit.tentative_reconciliation_ttl_ms as i64,
                    ),
            },
        );

        let mut derived = vec![
            clone_with_payload(event, EventPayload::TentativeSellIntentDetected(intent)),
            clone_with_payload(event, EventPayload::TentativeMaliciousSellWarning(warning)),
        ];
        if let Some(armed) = armed {
            derived.push(clone_with_payload(
                event,
                EventPayload::ShredEmergencyExitArmed(armed),
            ));
        }
        if let Some(triggered) = triggered {
            derived.push(clone_with_payload(
                event,
                EventPayload::ShredEmergencyExitTriggered(triggered),
            ));
        }
        derived
    }

    pub fn reconcile(
        &mut self,
        event: &NormalizedEvent,
        token: Option<&TokenState>,
    ) -> Vec<NormalizedEvent> {
        let Some(mint) = event.mint().cloned() else {
            return Vec::new();
        };
        let mut matched = Vec::new();
        let keys = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.intent.mint == mint)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in keys {
            let Some(pending) = self.pending.get(&key).cloned() else {
                continue;
            };
            if let Some(resolved) = reconcile_pending_against_event(&pending, event, token) {
                self.pending.remove(&key);
                self.record_resolution(&resolved, Some(&pending));
                matched.push(clone_with_payload(
                    event,
                    EventPayload::ShredSellIntentResolved(resolved),
                ));
            }
        }
        matched
    }

    pub fn expire(
        &mut self,
        now: OffsetDateTime,
        snapshot: &StateSnapshot,
    ) -> Vec<NormalizedEvent> {
        let mut expired = Vec::new();
        let keys = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.expires_at <= now)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in keys {
            let Some(pending) = self.pending.remove(&key) else {
                continue;
            };
            let latest_price = snapshot
                .tokens
                .get(&pending.intent.mint.0)
                .map(|token| token.latest_price)
                .unwrap_or(pending.warning_price);
            let opportunity = if pending.triggered.is_some() && latest_price > pending.warning_price
            {
                (latest_price - pending.warning_price) * pending.position_size_tokens
            } else {
                Decimal::ZERO
            };
            let resolved = ShredSellIntentResolvedEvent {
                original_tentative_event_id: pending.intent.event_id.clone(),
                canonical_signature: None,
                outcome: TentativeSellResolutionOutcome::NotSeenWithinTtl,
                observed_canonical_slot: None,
                actual_price_impact_pct: Decimal::ZERO,
                actual_quote_out: Decimal::ZERO,
                actual_token_in: Decimal::ZERO,
                actual_loss_saved_if_exited: -opportunity,
                false_positive_flag: true,
                missed_exit_flag: false,
                reconciliation_latency_ms: Some(
                    (now - pending.intent.received_at_wall_time).whole_milliseconds() as i64,
                ),
                source: pending.intent.source,
                mint: pending.intent.mint.clone(),
                seller_wallet: pending.intent.seller_wallet.clone(),
                seller_classification: pending.warning.seller_classification,
                confirmation_state: TentativeSellConfirmationState::NotSeenWithinTtl,
                confirmation_method: None,
                schema_version: common::SCHEMA_VERSION,
            };
            self.metrics.opportunity_cost_quote_total += opportunity;
            self.record_resolution(&resolved, Some(&pending));
            let mut event = clone_with_payload(
                &NormalizedEvent {
                    meta: EventMeta::new(
                        EventSource::Replay,
                        Canonicality::Processed,
                        pending.intent.slot.unwrap_or_default(),
                    ),
                    payload: EventPayload::TentativeSellIntentDetected(pending.intent.clone()),
                },
                EventPayload::ShredSellIntentResolved(resolved),
            );
            event.meta.received_at_wall_time = now;
            expired.push(event);
        }
        expired
    }

    pub fn render_report(&self, run_id: &str) -> String {
        let mut source_breakdown = BTreeMap::<String, u64>::new();
        let mut confirmation_breakdown = BTreeMap::<String, u64>::new();
        let mut net_benefit_total = Decimal::ZERO;
        let mut latency_ratio_total = Decimal::ZERO;
        let mut absorption_case_count = 0u64;
        let mut too_late_case_count = 0u64;
        for observation in &self.observations {
            *source_breakdown
                .entry(format!("{:?}", observation.source).to_lowercase())
                .or_default() += 1;
            *confirmation_breakdown
                .entry(
                    observation
                        .confirmation_method
                        .clone()
                        .unwrap_or_else(|| "none".to_owned()),
                )
                .or_default() += 1;
            net_benefit_total += observation.net_benefit_quote;
            latency_ratio_total += observation.latency_edge_ratio;
            if observation.absorption_health_score >= self.shred_exit.absorption_downgrade_threshold
            {
                absorption_case_count = absorption_case_count.saturating_add(1);
            }
            if observation.latency_edge_ratio < self.shred_exit.min_latency_edge_ratio {
                too_late_case_count = too_late_case_count.saturating_add(1);
            }
        }
        let sample_count = Decimal::from(self.observations.len().max(1) as u64);
        format!(
            "# Shred Exit Defense {}\n\n- total_tentative_sells: {}\n- malicious_sell_warnings: {}\n- emergency_exits_armed: {}\n- emergency_exits_triggered: {}\n- confirmed_true_positives: {}\n- confirmed_failed: {}\n- not_seen_within_ttl: {}\n- reorged: {}\n- decode_mismatches: {}\n- account_effect_confirmed: {}\n- false_positive_exits: {}\n- saved_loss_quote_total: {}\n- opportunity_cost_quote_total: {}\n- source_breakdown: {:?}\n- confirmation_method_breakdown: {:?}\n- average_latency_edge_ratio: {}\n- average_net_benefit_quote: {}\n- too_late_cases: {}\n- absorbed_sell_cases: {}\n- calibration_false_positive_rate: {}\n- calibration_missed_sell_rate: {}\n- calibration_true_positive_rate: {}\n- calibration_adjustments: {:?}\n- calibration_path: {}\n- calibration_version_hash: {}\n",
            run_id,
            self.metrics.tentative_sell_total,
            self.metrics.malicious_sell_warning_total,
            self.metrics.emergency_exits_armed_total,
            self.metrics.emergency_exits_triggered_total,
            self.metrics.confirmed_executed_total,
            self.metrics.confirmed_failed_total,
            self.metrics.not_seen_within_ttl_total,
            self.metrics.reorged_total,
            self.metrics.decode_mismatch_total,
            self.metrics.account_effect_confirmed_total,
            self.metrics.false_positive_exit_total,
            self.metrics.saved_loss_quote_total,
            self.metrics.opportunity_cost_quote_total,
            source_breakdown,
            confirmation_breakdown,
            latency_ratio_total / sample_count,
            net_benefit_total / sample_count,
            too_late_case_count,
            absorption_case_count,
            self.calibration.false_positive_rate,
            self.calibration.missed_sell_rate,
            self.calibration.true_positive_rate,
            self.calibration.adaptations,
            self.calibration
                .persisted_path
                .clone()
                .unwrap_or_else(|| "unconfigured".to_owned()),
            self.calibration
                .persisted_version_hash
                .clone()
                .unwrap_or_else(|| "none".to_owned()),
        )
    }

    pub fn render_calibration_report(&self, run_id: &str, config: &AppConfig) -> String {
        let persisted = self.persisted_calibration(config);
        let mut buckets = persisted.buckets.into_iter().collect::<Vec<_>>();
        buckets.sort_by(|left, right| left.0.cmp(&right.0));
        let mut out = format!(
            "# Shred Exit Calibration {}\n\n- calibration_path: {}\n- current_paper_exit_impact_pct: {}\n- current_paper_confidence_threshold: {}\n- threshold_adjustment: {}\n- bucket_count: {}\n- persisted_version_hash: {}\n- min_samples_before_adapt: {}\n- false_positive_rate_limit: {}\n- missed_sell_rate_limit: {}\n\n",
            run_id,
            self.calibration
                .persisted_path
                .clone()
                .unwrap_or_else(|| "unconfigured".to_owned()),
            persisted.current_paper_exit_impact_pct,
            persisted.current_paper_confidence_threshold,
            persisted.threshold_adjustment,
            buckets.len(),
            self.calibration
                .persisted_version_hash
                .clone()
                .unwrap_or_else(|| "none".to_owned()),
            self.shred_exit.calibration.min_samples_before_adapt,
            self.shred_exit.calibration.false_positive_rate_limit,
            self.shred_exit.calibration.missed_sell_rate_limit,
        );
        if buckets.is_empty() {
            out.push_str("- no calibration buckets recorded yet\n");
            return out;
        }
        for (key, bucket) in buckets {
            out.push_str(&format!(
                "- bucket={} samples={} true_positive_rate={} false_positive_rate={} saved_loss_total={} opportunity_cost_total={} threshold_adjustment={}\n",
                key,
                bucket.sample_count,
                bucket.true_positive_rate,
                bucket.false_positive_rate,
                bucket.saved_loss_total,
                bucket.opportunity_cost_total,
                bucket.threshold_adjustment,
            ));
        }
        out
    }

    fn record_resolution(
        &mut self,
        resolved: &ShredSellIntentResolvedEvent,
        pending: Option<&PendingSellContext>,
    ) {
        let source_metrics = self
            .source_metrics
            .entry(source_label(resolved.source))
            .or_default();
        match resolved.outcome {
            TentativeSellResolutionOutcome::ConfirmedExecuted => {
                self.metrics.confirmed_executed_total =
                    self.metrics.confirmed_executed_total.saturating_add(1);
                self.metrics.saved_loss_quote_total += resolved.actual_loss_saved_if_exited;
                source_metrics.confirmed_executed_total =
                    source_metrics.confirmed_executed_total.saturating_add(1);
                source_metrics.saved_loss_quote_total += resolved.actual_loss_saved_if_exited;
            }
            TentativeSellResolutionOutcome::AccountEffectsObserved => {
                self.metrics.account_effect_confirmed_total = self
                    .metrics
                    .account_effect_confirmed_total
                    .saturating_add(1);
                source_metrics.account_effect_confirmed_total = source_metrics
                    .account_effect_confirmed_total
                    .saturating_add(1);
            }
            TentativeSellResolutionOutcome::ConfirmedFailed => {
                self.metrics.confirmed_failed_total =
                    self.metrics.confirmed_failed_total.saturating_add(1);
                source_metrics.confirmed_failed_total =
                    source_metrics.confirmed_failed_total.saturating_add(1);
            }
            TentativeSellResolutionOutcome::RootedExecuted => {
                self.metrics.confirmed_executed_total =
                    self.metrics.confirmed_executed_total.saturating_add(1);
                source_metrics.confirmed_executed_total =
                    source_metrics.confirmed_executed_total.saturating_add(1);
            }
            TentativeSellResolutionOutcome::NotSeenWithinTtl => {
                self.metrics.not_seen_within_ttl_total =
                    self.metrics.not_seen_within_ttl_total.saturating_add(1);
                source_metrics.not_seen_within_ttl_total =
                    source_metrics.not_seen_within_ttl_total.saturating_add(1);
            }
            TentativeSellResolutionOutcome::Reorged => {
                self.metrics.reorged_total = self.metrics.reorged_total.saturating_add(1);
                source_metrics.reorged_total = source_metrics.reorged_total.saturating_add(1);
            }
            TentativeSellResolutionOutcome::DecodeMismatch => {
                self.metrics.decode_mismatch_total =
                    self.metrics.decode_mismatch_total.saturating_add(1);
                source_metrics.decode_mismatch_total =
                    source_metrics.decode_mismatch_total.saturating_add(1);
            }
        }
        if resolved.false_positive_flag {
            self.metrics.false_positive_exit_total =
                self.metrics.false_positive_exit_total.saturating_add(1);
            source_metrics.false_positive_exit_total =
                source_metrics.false_positive_exit_total.saturating_add(1);
        }
        if resolved.actual_loss_saved_if_exited < Decimal::ZERO {
            source_metrics.opportunity_cost_quote_total +=
                (-resolved.actual_loss_saved_if_exited).max(Decimal::ZERO);
        }
        if let Some(pending) = pending {
            self.observations
                .push(build_calibration_observation(pending, resolved));
        }
        self.recompute_calibration();
    }

    fn recompute_calibration(&mut self) {
        let positive = Decimal::from(self.metrics.confirmed_executed_total.max(1));
        let false_positive = Decimal::from(self.metrics.false_positive_exit_total);
        let seen = Decimal::from(self.metrics.tentative_sell_total.max(1));
        self.calibration.false_positive_rate = false_positive / seen;
        self.calibration.true_positive_rate = positive / seen;
        self.calibration.missed_sell_rate =
            Decimal::from(self.metrics.not_seen_within_ttl_total) / seen;
        if self.shred_exit.calibration.enabled
            && self.shred_exit.calibration.paper_only
            && self.metrics.tentative_sell_total
                >= self.shred_exit.calibration.min_samples_before_adapt
        {
            if self.calibration.false_positive_rate
                > self.shred_exit.calibration.false_positive_rate_limit
            {
                self.calibration.current_paper_exit_impact_pct =
                    (self.calibration.current_paper_exit_impact_pct
                        + self
                            .shred_exit
                            .calibration
                            .max_threshold_adjustment_per_hour)
                        .min(Decimal::from(95u64));
                self.calibration.adaptations.push(format!(
                    "raised_paper_exit_impact_to={}",
                    self.calibration.current_paper_exit_impact_pct
                ));
            } else if self.calibration.missed_sell_rate
                > self.shred_exit.calibration.missed_sell_rate_limit
            {
                self.calibration.current_paper_exit_impact_pct =
                    (self.calibration.current_paper_exit_impact_pct
                        - self
                            .shred_exit
                            .calibration
                            .max_threshold_adjustment_per_hour)
                        .max(self.shred_exit.warn_impact_pct);
                self.calibration.adaptations.push(format!(
                    "lowered_paper_exit_impact_to={}",
                    self.calibration.current_paper_exit_impact_pct
                ));
            }
        }
    }
}

fn build_calibration_observation(
    pending: &PendingSellContext,
    resolved: &ShredSellIntentResolvedEvent,
) -> CalibrationObservation {
    let confidence_bucket = decimal_bucket(
        pending.warning.confidence,
        &[
            Decimal::new(55, 2),
            Decimal::new(70, 2),
            Decimal::new(82, 2),
            Decimal::new(95, 2),
        ],
    );
    let impact_bucket = decimal_bucket(
        pending.intent.estimated_price_impact_pct,
        &[
            Decimal::from(8u64),
            Decimal::from(15u64),
            Decimal::from(25u64),
            Decimal::from(35u64),
        ],
    );
    let confirmation_method = resolved.confirmation_method.clone();
    let key = format!(
        "source={:?}|seller={:?}|confidence={}|impact={}|confirmation={}|strategy={}",
        pending.intent.source,
        pending.warning.seller_classification,
        confidence_bucket,
        impact_bucket,
        confirmation_method
            .clone()
            .unwrap_or_else(|| "none".to_owned()),
        pending
            .position_strategy
            .clone()
            .unwrap_or_else(|| "none".to_owned()),
    );
    CalibrationObservation {
        key,
        source: pending.intent.source,
        seller_classification: pending.warning.seller_classification,
        confirmation_method,
        confidence_bucket,
        impact_bucket,
        outcome: resolved.outcome,
        emergency_exit: pending.triggered.is_some(),
        false_positive: resolved.false_positive_flag,
        saved_loss_quote: resolved.actual_loss_saved_if_exited.max(Decimal::ZERO),
        opportunity_cost_quote: (-resolved.actual_loss_saved_if_exited).max(Decimal::ZERO),
        early_to_geyser_processed_ms: matches!(
            resolved.outcome,
            TentativeSellResolutionOutcome::ConfirmedExecuted
                | TentativeSellResolutionOutcome::ConfirmedFailed
                | TentativeSellResolutionOutcome::RootedExecuted
        )
        .then_some(resolved.reconciliation_latency_ms.unwrap_or_default()),
        early_to_account_effect_ms: matches!(
            resolved.outcome,
            TentativeSellResolutionOutcome::AccountEffectsObserved
        )
        .then_some(resolved.reconciliation_latency_ms.unwrap_or_default()),
        exit_latency_ms: pending
            .triggered
            .as_ref()
            .map(|_| pending.intent.received_at_wall_time.unix_timestamp_nanos() as i64)
            .map(|_| 0),
        strategy: pending.position_strategy.clone(),
        latency_advantage_ms: Some(pending.latency_advantage_ms),
        required_latency_advantage_ms: Some(pending.required_latency_advantage_ms),
        latency_edge_ratio: pending.latency_edge_ratio,
        net_benefit_quote: pending.net_benefit_quote,
        absorption_health_score: pending.absorption_health_score,
    }
}

fn apply_observations_to_persisted(
    persisted: &mut PersistedShredExitCalibration,
    observations: &[CalibrationObservation],
    config: &ShredExitConfig,
) {
    for observation in observations {
        let bucket = persisted
            .buckets
            .entry(observation.key.clone())
            .or_default();
        bucket.schema_version = common::SCHEMA_VERSION;
        bucket.sample_count = bucket.sample_count.saturating_add(1);
        bucket.last_updated_at = Some(OffsetDateTime::now_utc());
        if observation.emergency_exit {
            bucket.emergency_exit_count = bucket.emergency_exit_count.saturating_add(1);
        }
        if observation.false_positive {
            bucket.false_positive_count = bucket.false_positive_count.saturating_add(1);
            if observation.emergency_exit {
                bucket.false_exit_count = bucket.false_exit_count.saturating_add(1);
            }
        }
        match observation.outcome {
            TentativeSellResolutionOutcome::ConfirmedExecuted => {
                bucket.confirmed_executed_count = bucket.confirmed_executed_count.saturating_add(1);
                bucket.true_positive_count = bucket.true_positive_count.saturating_add(1);
            }
            TentativeSellResolutionOutcome::ConfirmedFailed => {
                bucket.confirmed_failed_count = bucket.confirmed_failed_count.saturating_add(1);
            }
            TentativeSellResolutionOutcome::AccountEffectsObserved => {
                bucket.account_effect_confirmed_count =
                    bucket.account_effect_confirmed_count.saturating_add(1);
                bucket.true_positive_count = bucket.true_positive_count.saturating_add(1);
            }
            TentativeSellResolutionOutcome::NotSeenWithinTtl => {
                bucket.not_seen_count = bucket.not_seen_count.saturating_add(1);
                bucket.missed_exit_count = bucket.missed_exit_count.saturating_add(1);
            }
            TentativeSellResolutionOutcome::Reorged => {
                bucket.reorg_count = bucket.reorg_count.saturating_add(1);
            }
            TentativeSellResolutionOutcome::DecodeMismatch => {
                bucket.decode_mismatch_count = bucket.decode_mismatch_count.saturating_add(1);
            }
            TentativeSellResolutionOutcome::RootedExecuted => {
                bucket.confirmed_executed_count = bucket.confirmed_executed_count.saturating_add(1);
                bucket.true_positive_count = bucket.true_positive_count.saturating_add(1);
            }
        }
        bucket.saved_loss_total += observation.saved_loss_quote;
        bucket.opportunity_cost_total += observation.opportunity_cost_quote;
        let sample = Decimal::from(bucket.sample_count);
        bucket.saved_loss_mean = if sample > Decimal::ZERO {
            bucket.saved_loss_total / sample
        } else {
            Decimal::ZERO
        };
        bucket.opportunity_cost_mean = if sample > Decimal::ZERO {
            bucket.opportunity_cost_total / sample
        } else {
            Decimal::ZERO
        };
        bucket.false_positive_rate = if sample > Decimal::ZERO {
            Decimal::from(bucket.false_positive_count) / sample
        } else {
            Decimal::ZERO
        };
        bucket.true_positive_rate = if sample > Decimal::ZERO {
            Decimal::from(bucket.true_positive_count) / sample
        } else {
            Decimal::ZERO
        };
        bucket.precision = if bucket.emergency_exit_count > 0 {
            Decimal::from(bucket.true_positive_count) / Decimal::from(bucket.emergency_exit_count)
        } else {
            Decimal::ZERO
        };
        bucket.recall = if sample > Decimal::ZERO {
            Decimal::from(bucket.true_positive_count) / sample
        } else {
            Decimal::ZERO
        };
        bucket.average_early_to_geyser_processed_ms = average_decimal(
            bucket.average_early_to_geyser_processed_ms,
            bucket.sample_count,
            observation.early_to_geyser_processed_ms,
        );
        bucket.average_early_to_account_effect_ms = average_decimal(
            bucket.average_early_to_account_effect_ms,
            bucket.sample_count,
            observation.early_to_account_effect_ms,
        );
        bucket.average_exit_latency_ms = average_decimal(
            bucket.average_exit_latency_ms,
            bucket.sample_count,
            observation.exit_latency_ms,
        );
    }
    persisted.threshold_adjustment =
        persisted.current_paper_exit_impact_pct - config.paper_exit_impact_pct;
}

fn average_decimal(current: Decimal, sample_count: u64, next: Option<i64>) -> Decimal {
    let Some(next) = next else {
        return current;
    };
    if sample_count <= 1 {
        Decimal::from(next)
    } else {
        let previous = Decimal::from(sample_count - 1);
        ((current * previous) + Decimal::from(next)) / Decimal::from(sample_count)
    }
}

fn clamp01(value: Decimal) -> Decimal {
    value.max(Decimal::ZERO).min(Decimal::ONE)
}

fn decimal_bucket(value: Decimal, thresholds: &[Decimal]) -> String {
    let mut lower = Decimal::ZERO;
    for threshold in thresholds {
        if value < *threshold {
            return format!("{}-{}", lower, threshold);
        }
        lower = *threshold;
    }
    format!("{}+", lower)
}

fn calibration_path(config: &AppConfig) -> PathBuf {
    let configured = PathBuf::from(&config.shred_exit.calibration.path);
    if configured.is_absolute() {
        configured
    } else {
        Path::new(&config.storage.root)
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(configured)
    }
}

fn load_persisted_calibration(config: &AppConfig) -> Result<PersistedShredExitCalibration> {
    let path = calibration_path(config);
    let raw = fs::read(path)?;
    Ok(serde_json::from_slice(&raw)?)
}

fn persisted_hash(persisted: &PersistedShredExitCalibration) -> String {
    format!(
        "{}:{}:{}",
        persisted.schema_version,
        persisted.buckets.len(),
        persisted
            .last_updated_at
            .map(|time| time.unix_timestamp_nanos().to_string())
            .unwrap_or_else(|| "none".to_owned())
    )
}

fn clone_with_payload(event: &NormalizedEvent, payload: EventPayload) -> NormalizedEvent {
    let mut meta = event.meta.clone();
    meta.event_id = EventId::from_seed(&format!(
        "derived-event|{}|{}|{:?}|{}",
        event.meta.event_id.0,
        payload_kind(&payload),
        meta.source,
        meta.slot
    ));
    NormalizedEvent { meta, payload }
}

fn payload_kind(payload: &EventPayload) -> &'static str {
    match payload {
        EventPayload::TokenCreated(_) => "token_created",
        EventPayload::PumpBuy(_) => "pump_buy",
        EventPayload::PumpSell(_) => "pump_sell",
        EventPayload::BondingCurveUpdate(_) => "bonding_curve_update",
        EventPayload::HolderBalanceUpdate(_) => "holder_balance_update",
        EventPayload::WalletFunding(_) => "wallet_funding",
        EventPayload::ObservedTransaction(_) => "observed_transaction",
        EventPayload::TokenTerminal(_) => "token_terminal",
        EventPayload::TradeDecision(_) => "trade_decision",
        EventPayload::SimulatedFill(_) => "simulated_fill",
        EventPayload::LiveFill(_) => "live_fill",
        EventPayload::DataGap(_) => "data_gap",
        EventPayload::TentativeSellIntentDetected(_) => "tentative_sell_intent_detected",
        EventPayload::TentativeMaliciousSellWarning(_) => "tentative_malicious_sell_warning",
        EventPayload::ShredEmergencyExitArmed(_) => "shred_emergency_exit_armed",
        EventPayload::ShredEmergencyExitTriggered(_) => "shred_emergency_exit_triggered",
        EventPayload::ShredSellIntentResolved(_) => "shred_sell_intent_resolved",
    }
}

fn infer_early_intent_source(event: &NormalizedEvent) -> EarlyIntentSource {
    match event.meta.source {
        EventSource::Replay => EarlyIntentSource::ReplayTentative,
        EventSource::DeshredTentative => EarlyIntentSource::DeshredPreExecution,
        EventSource::ShredTentative => {
            let source_id = event
                .meta
                .raw_reference
                .as_ref()
                .map(|reference| reference.source_id.as_str())
                .unwrap_or_default();
            if source_id.starts_with("mock") {
                EarlyIntentSource::MockEarlyIntent
            } else if source_id.starts_with("fixture") {
                EarlyIntentSource::FixtureTentative
            } else {
                EarlyIntentSource::RawShred
            }
        }
        _ => EarlyIntentSource::FixtureTentative,
    }
}

fn source_label(source: EarlyIntentSource) -> String {
    format!("{source:?}").to_lowercase()
}

fn pending_matches_duplicate_source(
    pending: &PendingSellContext,
    intent: &TentativeSellIntentDetectedEvent,
    amount_tolerance_pct: Decimal,
    slot_tolerance: u64,
) -> bool {
    if pending.intent.mint != intent.mint {
        return false;
    }
    if let (Some(left), Some(right)) = (&pending.intent.signature, &intent.signature) {
        return left == right;
    }
    if let (Some(left), Some(right)) = (
        pending.intent.raw_update_hash.as_ref(),
        intent.raw_update_hash.as_ref(),
    ) {
        if left == right {
            return true;
        }
    }
    if pending.intent.seller_wallet != intent.seller_wallet {
        return false;
    }
    let left_slot = pending.intent.slot.unwrap_or_default();
    let right_slot = intent.slot.unwrap_or_default();
    if left_slot.abs_diff(right_slot) > slot_tolerance {
        return false;
    }
    let delta = (pending.intent.token_in_estimate - intent.token_in_estimate).abs();
    let baseline = pending
        .intent
        .token_in_estimate
        .max(intent.token_in_estimate)
        .max(Decimal::ONE);
    let tolerance = baseline * (amount_tolerance_pct / Decimal::from(100u64));
    delta <= tolerance
}

fn apply_partial_penalty(mut confidence: Decimal, event: &NormalizedEvent) -> Decimal {
    if event
        .meta
        .data_quality_flags
        .contains(&common::DataQualityFlag::PartialShred)
    {
        confidence = (confidence - Decimal::new(25, 2)).max(Decimal::ZERO);
    }
    confidence
}

fn account_confidence_for_sell(
    token: &TokenState,
    payload: &PumpSellEvent,
    seller_entry: Option<&ThreatWalletEntry>,
    seller_top_rank: Option<usize>,
) -> Decimal {
    if payload.is_creator || token.creator.as_ref() == Some(&payload.seller) {
        Decimal::new(92, 2)
    } else if payload.is_top_holder_pre_sell || seller_top_rank.is_some() {
        Decimal::new(85, 2)
    } else if let Some(entry) = seller_entry {
        if entry.balance > Decimal::ZERO {
            Decimal::new(85, 2)
        } else {
            Decimal::new(60, 2)
        }
    } else if payload.token_in > Decimal::ZERO && payload.quote_out > Decimal::ZERO {
        Decimal::new(60, 2)
    } else {
        Decimal::new(55, 2)
    }
}

fn find_threat_entry<'a>(token: &'a TokenState, seller: &str) -> Option<&'a ThreatWalletEntry> {
    token
        .shred_defense
        .exit_threat_index
        .dangerous_wallets
        .iter()
        .find(|entry| entry.wallet == seller)
}

fn classify_without_index(
    token: &TokenState,
    payload: &PumpSellEvent,
) -> DangerousSellerClassification {
    if token.creator.as_ref() == Some(&payload.seller) || payload.is_creator {
        DangerousSellerClassification::Dev
    } else if payload.is_top_holder_pre_sell {
        DangerousSellerClassification::Top3Holder
    } else {
        DangerousSellerClassification::Unknown
    }
}

fn is_catastrophic_classification(classification: DangerousSellerClassification) -> bool {
    matches!(
        classification,
        DangerousSellerClassification::Dev
            | DangerousSellerClassification::DevCluster
            | DangerousSellerClassification::Top1Holder
            | DangerousSellerClassification::Top3Holder
            | DangerousSellerClassification::BundleWallet
            | DangerousSellerClassification::BundleCluster
            | DangerousSellerClassification::SameFunderCluster
    )
}

fn fallback_impact_pct(token: &TokenState, payload: &PumpSellEvent) -> Decimal {
    let depth = (token.reserve_state.real_quote_reserves
        + token.reserve_state.virtual_quote_reserves)
        .max(Decimal::from(1u64));
    let quote = payload.quote_out.max(payload.token_in * token.latest_price);
    ((quote / depth).min(Decimal::ONE) * Decimal::from(100u64)).max(Decimal::ZERO)
}

fn determine_warning_level(
    classification: DangerousSellerClassification,
    impact_pct: Decimal,
    confidence: Decimal,
    calibrated_paper_exit_impact_pct: Decimal,
    config: &ShredExitConfig,
) -> TentativeSellRiskLevel {
    if confidence < config.min_decode_confidence_warn {
        return TentativeSellRiskLevel::Info;
    }
    let class_threshold = match classification {
        DangerousSellerClassification::Dev | DangerousSellerClassification::DevCluster => {
            config.dev_sell_exit_threshold_pct
        }
        DangerousSellerClassification::Top1Holder
        | DangerousSellerClassification::Top3Holder
        | DangerousSellerClassification::Top5Holder
        | DangerousSellerClassification::Top10Holder => config.top_holder_exit_threshold_pct,
        DangerousSellerClassification::BundleWallet
        | DangerousSellerClassification::BundleCluster => config.bundle_cluster_exit_threshold_pct,
        DangerousSellerClassification::Whale => config.arm_impact_pct,
        _ => config.warn_impact_pct,
    };
    if confidence >= config.min_decode_confidence_paper_exit
        && impact_pct >= calibrated_paper_exit_impact_pct.max(class_threshold)
    {
        TentativeSellRiskLevel::EmergencyExitRequired
    } else if confidence >= config.min_decode_confidence_arm
        && impact_pct >= config.arm_impact_pct.max(class_threshold)
    {
        TentativeSellRiskLevel::ExitArmed
    } else if impact_pct >= config.warn_impact_pct.min(class_threshold) {
        TentativeSellRiskLevel::Watch
    } else {
        TentativeSellRiskLevel::Info
    }
}

fn warning_reason_codes(
    classification: DangerousSellerClassification,
    warning_level: TentativeSellRiskLevel,
    source: EarlyIntentSource,
    confidence: Decimal,
) -> Vec<ReasonCode> {
    let mut reasons = Vec::new();
    reasons.push(match source {
        EarlyIntentSource::DeshredPreExecution => ReasonCode::DeshredPreExecutionSellWarning,
        _ => ReasonCode::EarlyIntentSellWarning,
    });
    match classification {
        DangerousSellerClassification::Dev | DangerousSellerClassification::DevCluster => {
            reasons.push(ReasonCode::ShredDevSellWarning);
        }
        DangerousSellerClassification::Top1Holder
        | DangerousSellerClassification::Top3Holder
        | DangerousSellerClassification::Top5Holder
        | DangerousSellerClassification::Top10Holder => {
            reasons.push(ReasonCode::ShredTopHolderSellWarning);
        }
        DangerousSellerClassification::BundleWallet
        | DangerousSellerClassification::BundleCluster => {
            reasons.push(ReasonCode::ShredBundleExitWarning);
        }
        DangerousSellerClassification::Whale => {
            reasons.push(ReasonCode::ShredWhaleDumpWarning);
        }
        _ => {}
    }
    if matches!(
        warning_level,
        TentativeSellRiskLevel::ExitArmed
            | TentativeSellRiskLevel::EmergencyExitRecommended
            | TentativeSellRiskLevel::EmergencyExitRequired
    ) {
        reasons.push(ReasonCode::ShredExitArmed);
    }
    if confidence < Decimal::new(55, 2) {
        reasons.push(ReasonCode::ShredLowConfidence);
    }
    reasons
}

fn top_holder_rank(token: &TokenState, seller: &str) -> Option<usize> {
    token
        .holder_state
        .top_holders
        .iter()
        .position(|holder| holder.owner.0 == seller)
        .map(|index| index + 1)
}

fn reconcile_pending_against_event(
    pending: &PendingSellContext,
    event: &NormalizedEvent,
    token: Option<&TokenState>,
) -> Option<common::ShredSellIntentResolvedEvent> {
    let signature_matches = match (event.signature(), pending.intent.signature.as_deref()) {
        (Some(actual), Some(expected)) => actual == expected,
        (Some(_), None) => true,
        (None, None) => true,
        (None, Some(_)) => false,
    };
    match &event.payload {
        _ if event.meta.canonicality == Canonicality::Reverted
            && event.signature() == pending.intent.signature.as_deref() =>
        {
            Some(common::ShredSellIntentResolvedEvent {
                original_tentative_event_id: pending.intent.event_id.clone(),
                canonical_signature: event.signature().map(ToOwned::to_owned),
                outcome: TentativeSellResolutionOutcome::Reorged,
                observed_canonical_slot: Some(event.meta.slot),
                actual_price_impact_pct: Decimal::ZERO,
                actual_quote_out: Decimal::ZERO,
                actual_token_in: Decimal::ZERO,
                actual_loss_saved_if_exited: Decimal::ZERO,
                false_positive_flag: true,
                missed_exit_flag: false,
                reconciliation_latency_ms: Some(
                    (event.meta.received_at_wall_time - pending.intent.received_at_wall_time)
                        .whole_milliseconds() as i64,
                ),
                source: pending.intent.source,
                mint: pending.intent.mint.clone(),
                seller_wallet: pending.intent.seller_wallet.clone(),
                seller_classification: pending.warning.seller_classification,
                confirmation_state: TentativeSellConfirmationState::Reorged,
                confirmation_method: Some("reorg".to_owned()),
                schema_version: common::SCHEMA_VERSION,
            })
        }
        EventPayload::PumpSell(payload)
            if payload.mint == pending.intent.mint
                && payload.seller == pending.intent.seller_wallet
                && signature_matches =>
        {
            let outcome = if payload.status == common::TransactionStatus::Failed {
                TentativeSellResolutionOutcome::ConfirmedFailed
            } else {
                TentativeSellResolutionOutcome::ConfirmedExecuted
            };
            let price_after = payload.price_after.unwrap_or_else(|| {
                token
                    .map(|value| value.latest_price)
                    .unwrap_or(pending.warning_price)
            });
            let saved = if pending.triggered.is_some() && pending.warning_price > price_after {
                (pending.warning_price - price_after) * pending.position_size_tokens
            } else {
                Decimal::ZERO
            };
            Some(common::ShredSellIntentResolvedEvent {
                original_tentative_event_id: pending.intent.event_id.clone(),
                canonical_signature: event.signature().map(ToOwned::to_owned),
                outcome,
                observed_canonical_slot: Some(event.meta.slot),
                actual_price_impact_pct: price_impact_pct(
                    payload.price_before.unwrap_or(pending.warning_price),
                    price_after,
                ),
                actual_quote_out: payload.quote_out,
                actual_token_in: payload.token_in,
                actual_loss_saved_if_exited: saved,
                false_positive_flag: matches!(
                    outcome,
                    TentativeSellResolutionOutcome::ConfirmedFailed
                ),
                missed_exit_flag: false,
                reconciliation_latency_ms: Some(
                    (event.meta.received_at_wall_time - pending.intent.received_at_wall_time)
                        .whole_milliseconds() as i64,
                ),
                source: pending.intent.source,
                mint: pending.intent.mint.clone(),
                seller_wallet: pending.intent.seller_wallet.clone(),
                seller_classification: pending.warning.seller_classification,
                confirmation_state: if payload.status == common::TransactionStatus::Failed {
                    TentativeSellConfirmationState::ConfirmedFailed
                } else {
                    TentativeSellConfirmationState::ConfirmedExecuted
                },
                confirmation_method: Some("signature".to_owned()),
                schema_version: common::SCHEMA_VERSION,
            })
        }
        EventPayload::HolderBalanceUpdate(payload)
            if payload.mint == pending.intent.mint
                && payload.owner_wallet == pending.intent.seller_wallet
                && payload.delta < Decimal::ZERO =>
        {
            Some(common::ShredSellIntentResolvedEvent {
                original_tentative_event_id: pending.intent.event_id.clone(),
                canonical_signature: payload.caused_by_signature.clone(),
                outcome: TentativeSellResolutionOutcome::AccountEffectsObserved,
                observed_canonical_slot: Some(event.meta.slot),
                actual_price_impact_pct: Decimal::ZERO,
                actual_quote_out: Decimal::ZERO,
                actual_token_in: payload.delta.abs(),
                actual_loss_saved_if_exited: Decimal::ZERO,
                false_positive_flag: false,
                missed_exit_flag: false,
                reconciliation_latency_ms: Some(
                    (event.meta.received_at_wall_time - pending.intent.received_at_wall_time)
                        .whole_milliseconds() as i64,
                ),
                source: pending.intent.source,
                mint: pending.intent.mint.clone(),
                seller_wallet: pending.intent.seller_wallet.clone(),
                seller_classification: pending.warning.seller_classification,
                confirmation_state: TentativeSellConfirmationState::AccountEffectsObserved,
                confirmation_method: Some("account_effect".to_owned()),
                schema_version: common::SCHEMA_VERSION,
            })
        }
        EventPayload::PumpSell(payload)
            if payload.mint == pending.intent.mint
                && payload.seller == pending.intent.seller_wallet
                && (!signature_matches
                    || payload.token_in != pending.intent.token_in_estimate
                    || payload.quote_out != pending.intent.quote_out_estimate) =>
        {
            Some(common::ShredSellIntentResolvedEvent {
                original_tentative_event_id: pending.intent.event_id.clone(),
                canonical_signature: event.signature().map(ToOwned::to_owned),
                outcome: TentativeSellResolutionOutcome::DecodeMismatch,
                observed_canonical_slot: Some(event.meta.slot),
                actual_price_impact_pct: price_impact_pct(
                    payload.price_before.unwrap_or(pending.warning_price),
                    payload.price_after.unwrap_or(pending.warning_price),
                ),
                actual_quote_out: payload.quote_out,
                actual_token_in: payload.token_in,
                actual_loss_saved_if_exited: Decimal::ZERO,
                false_positive_flag: true,
                missed_exit_flag: false,
                reconciliation_latency_ms: Some(
                    (event.meta.received_at_wall_time - pending.intent.received_at_wall_time)
                        .whole_milliseconds() as i64,
                ),
                source: pending.intent.source,
                mint: pending.intent.mint.clone(),
                seller_wallet: pending.intent.seller_wallet.clone(),
                seller_classification: pending.warning.seller_classification,
                confirmation_state: TentativeSellConfirmationState::DecodeMismatch,
                confirmation_method: Some("fingerprint".to_owned()),
                schema_version: common::SCHEMA_VERSION,
            })
        }
        _ => None,
    }
}

fn price_impact_pct(before: Decimal, after: Decimal) -> Decimal {
    if before <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        ((before - after) / before).abs() * Decimal::from(100u64)
    }
}
