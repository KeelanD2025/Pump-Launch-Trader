use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

macro_rules! dec {
    ($value:literal) => {
        Decimal::from_str_exact(stringify!($value)).expect("decimal literal")
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskSource {
    Stream,
    BoundedRpc,
    MetadataHttp,
    ProviderApi,
    RawShred,
    Deshred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskTimeRole {
    PreEntryFeature,
    DecisionTimeFeature,
    PostEventLabel,
    EnrichmentLateFeature,
}

impl RiskTimeRole {
    pub fn pre_entry_safe(self) -> bool {
        matches!(
            self,
            RiskTimeRole::PreEntryFeature | RiskTimeRole::DecisionTimeFeature
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticRiskClassification {
    LowRiskObserved,
    ElevatedStreamRisk,
    ElevatedEnrichmentRisk,
    BundleLikeStreamSignal,
    ProviderConfirmedBundle,
    FundingClusterSuspected,
    InconclusiveMissingEnrichment,
    ProviderConfirmationRequired,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskEvidence {
    pub risk_id: String,
    pub risk_family: String,
    pub source: RiskSource,
    pub time_role: RiskTimeRole,
    pub score: Option<Decimal>,
    pub confidence: Decimal,
    pub unavailable_reason: Option<String>,
    pub evidence: Vec<String>,
}

impl RiskEvidence {
    pub fn available(
        risk_id: &str,
        risk_family: &str,
        source: RiskSource,
        time_role: RiskTimeRole,
        score: Decimal,
        confidence: Decimal,
        evidence: Vec<String>,
    ) -> Self {
        Self {
            risk_id: risk_id.to_owned(),
            risk_family: risk_family.to_owned(),
            source,
            time_role,
            score: Some(clamp01(score)),
            confidence: clamp01(confidence),
            unavailable_reason: None,
            evidence,
        }
    }

    pub fn unavailable(
        risk_id: &str,
        risk_family: &str,
        source: RiskSource,
        time_role: RiskTimeRole,
        reason: &str,
    ) -> Self {
        Self {
            risk_id: risk_id.to_owned(),
            risk_family: risk_family.to_owned(),
            source,
            time_role,
            score: None,
            confidence: Decimal::ZERO,
            unavailable_reason: Some(reason.to_owned()),
            evidence: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RiskSnapshotInput {
    pub mint: String,
    pub holder_count: Option<Decimal>,
    pub top_holder_pct: Option<Decimal>,
    pub dev_holding_pct: Option<Decimal>,
    pub paperhand_90pct_count: Option<Decimal>,
    pub holder_churn_rate: Option<Decimal>,
    pub buy_count: Option<Decimal>,
    pub sell_count: Option<Decimal>,
    pub unique_buyers: Option<Decimal>,
    pub same_slot_tx_count: Option<Decimal>,
    pub same_instruction_shape_count: Option<Decimal>,
    pub same_account_list_hash_count: Option<Decimal>,
    pub same_signer_cluster_count: Option<Decimal>,
    pub priority_fee_cluster_count: Option<Decimal>,
    pub fake_momentum_stream_proxy: Option<bool>,
    pub sell_absorption_stream_proxy: Option<bool>,
    pub metadata_uri_present: Option<bool>,
    pub metadata_fetch_success: Option<bool>,
    pub social_count: Option<Decimal>,
    pub one_hop_funder_candidate_count: Option<Decimal>,
    pub rpc_supply_matches_curve_supply: Option<bool>,
    pub rpc_supply_mismatch_ratio: Option<Decimal>,
    pub data_gap_active: Option<bool>,
    pub stream_gap_active: Option<bool>,
    pub post_migration_supported: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskSnapshotOutput {
    pub mint: String,
    pub risk_score_total: Decimal,
    pub risk_score_stream_only: Decimal,
    pub risk_score_enrichment: Decimal,
    pub risk_classification: DiagnosticRiskClassification,
    pub risk_evidence_count: usize,
    pub risk_confidence: Decimal,
    pub unavailable_risk_families: Vec<String>,
    pub blocking_data_quality_reasons: Vec<String>,
    pub pre_entry_safe_features: Vec<String>,
    pub post_event_only_labels: Vec<String>,
    pub evidence: Vec<RiskEvidence>,
}

#[derive(Debug, Clone)]
pub struct DiagnosticRiskEngine {
    pub high_concentration_pct: Decimal,
    pub high_same_slot_count: Decimal,
    pub low_unique_buyer_ratio: Decimal,
}

impl Default for DiagnosticRiskEngine {
    fn default() -> Self {
        Self {
            high_concentration_pct: dec!(0.80),
            high_same_slot_count: dec!(2),
            low_unique_buyer_ratio: dec!(0.50),
        }
    }
}

impl DiagnosticRiskEngine {
    pub fn evaluate(&self, input: RiskSnapshotInput) -> RiskSnapshotOutput {
        let mut evidence = Vec::new();
        evidence.push(self.holder_concentration(&input));
        evidence.push(self.dev_exit(&input));
        evidence.push(self.holder_churn(&input));
        evidence.push(self.bundle_like(&input));
        evidence.push(self.fake_momentum(&input));
        evidence.push(self.sell_absorption(&input));
        evidence.push(self.metadata_social(&input));
        evidence.push(self.funding(&input));
        evidence.push(self.supply_semantics(&input));
        evidence.push(self.data_quality(&input));
        evidence.push(self.post_migration(&input));

        let available: Vec<_> = evidence.iter().filter(|row| row.score.is_some()).collect();
        let stream_scores: Vec<_> = available
            .iter()
            .filter(|row| matches!(row.source, RiskSource::Stream))
            .filter_map(|row| row.score)
            .collect();
        let enrichment_scores: Vec<_> = available
            .iter()
            .filter(|row| {
                matches!(
                    row.source,
                    RiskSource::BoundedRpc | RiskSource::MetadataHttp
                )
            })
            .filter_map(|row| row.score)
            .collect();
        let all_scores: Vec<_> = available.iter().filter_map(|row| row.score).collect();
        let risk_score_stream_only = average(&stream_scores);
        let risk_score_enrichment = average(&enrichment_scores);
        let risk_score_total = average(&all_scores);
        let risk_confidence = average(
            &available
                .iter()
                .map(|row| row.confidence)
                .collect::<Vec<_>>(),
        );

        let unavailable_risk_families = evidence
            .iter()
            .filter(|row| row.score.is_none())
            .map(|row| row.risk_family.clone())
            .collect::<Vec<_>>();
        let blocking_data_quality_reasons = evidence
            .iter()
            .filter_map(|row| row.unavailable_reason.clone())
            .filter(|reason| reason.contains("data_gap") || reason.contains("stream_gap"))
            .collect::<Vec<_>>();
        let pre_entry_safe_features = evidence
            .iter()
            .filter(|row| row.time_role.pre_entry_safe())
            .map(|row| row.risk_id.clone())
            .collect::<Vec<_>>();
        let post_event_only_labels = evidence
            .iter()
            .filter(|row| row.time_role == RiskTimeRole::PostEventLabel)
            .map(|row| row.risk_id.clone())
            .collect::<Vec<_>>();

        let has_provider_confirmed_bundle = evidence.iter().any(|row| {
            row.risk_id == "provider_confirmation_required"
                && row
                    .evidence
                    .iter()
                    .any(|value| value == "provider_confirmed_bundle")
        });
        let has_bundle_like = evidence.iter().any(|row| {
            row.risk_id.contains("bundle_like") && row.score.unwrap_or_default() > dec!(0.35)
        });
        let has_enrichment_risk = enrichment_scores.iter().any(|score| *score > dec!(0.35));
        let classification = if has_provider_confirmed_bundle {
            DiagnosticRiskClassification::ProviderConfirmedBundle
        } else if has_bundle_like {
            DiagnosticRiskClassification::BundleLikeStreamSignal
        } else if has_enrichment_risk {
            DiagnosticRiskClassification::ElevatedEnrichmentRisk
        } else if risk_score_stream_only > dec!(0.35) {
            DiagnosticRiskClassification::ElevatedStreamRisk
        } else if all_scores.is_empty() {
            DiagnosticRiskClassification::InconclusiveMissingEnrichment
        } else {
            DiagnosticRiskClassification::LowRiskObserved
        };

        RiskSnapshotOutput {
            mint: input.mint,
            risk_score_total,
            risk_score_stream_only,
            risk_score_enrichment,
            risk_classification: classification,
            risk_evidence_count: available.len(),
            risk_confidence,
            unavailable_risk_families,
            blocking_data_quality_reasons,
            pre_entry_safe_features,
            post_event_only_labels,
            evidence,
        }
    }

    fn holder_concentration(&self, input: &RiskSnapshotInput) -> RiskEvidence {
        match input.top_holder_pct {
            Some(top) => RiskEvidence::available(
                "holder_concentration_risk",
                "holder_concentration_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                top / self.high_concentration_pct,
                dec!(1),
                vec![format!("top_holder_pct={top}")],
            ),
            None => RiskEvidence::unavailable(
                "holder_concentration_risk",
                "holder_concentration_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                "missing_stream_holder_snapshot",
            ),
        }
    }

    fn dev_exit(&self, input: &RiskSnapshotInput) -> RiskEvidence {
        match input.dev_holding_pct {
            Some(value) => RiskEvidence::available(
                "dev_exit_risk",
                "dev_exit_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                if value <= dec!(0.01) {
                    dec!(0.25)
                } else {
                    dec!(0.05)
                },
                dec!(1),
                vec![format!("dev_holding_pct={value}")],
            ),
            None => RiskEvidence::unavailable(
                "dev_exit_risk",
                "dev_exit_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                "missing_stream_dev_holder_state",
            ),
        }
    }

    fn holder_churn(&self, input: &RiskSnapshotInput) -> RiskEvidence {
        if let Some(churn) = input.holder_churn_rate {
            RiskEvidence::available(
                "holder_churn_risk",
                "holder_churn_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                churn,
                dec!(0.8),
                vec![format!("holder_churn_rate={churn}")],
            )
        } else if let Some(count) = input.paperhand_90pct_count {
            RiskEvidence::available(
                "paperhand_90pct_risk",
                "paperhand_90pct_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                count / dec!(10),
                dec!(0.7),
                vec![format!("paperhand_90pct_count={count}")],
            )
        } else {
            RiskEvidence::unavailable(
                "holder_churn_risk",
                "holder_churn_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                "holder_churn_not_in_canary_artifact",
            )
        }
    }

    fn bundle_like(&self, input: &RiskSnapshotInput) -> RiskEvidence {
        let same_slot = input.same_slot_tx_count.unwrap_or_default();
        let same_shape = input.same_instruction_shape_count.unwrap_or_default();
        let account_cluster = input.same_account_list_hash_count.unwrap_or_default();
        let signer_cluster = input.same_signer_cluster_count.unwrap_or_default();
        let priority_cluster = input.priority_fee_cluster_count.unwrap_or_default();
        let score = (same_slot / self.high_same_slot_count)
            + (same_shape / dec!(3))
            + (account_cluster / dec!(3))
            + (signer_cluster / dec!(3))
            + (priority_cluster / dec!(3));
        RiskEvidence::available(
            "same_slot_bundle_like_risk",
            "bundle_like_risk",
            RiskSource::Stream,
            RiskTimeRole::DecisionTimeFeature,
            score / dec!(5),
            dec!(0.8),
            vec![
                format!("same_slot_tx_count={same_slot}"),
                "provider_confirmed_bundle=false".to_owned(),
            ],
        )
    }

    fn fake_momentum(&self, input: &RiskSnapshotInput) -> RiskEvidence {
        if let Some(flag) = input.fake_momentum_stream_proxy {
            return RiskEvidence::available(
                "fake_momentum_risk",
                "fake_momentum_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                if flag { dec!(0.70) } else { dec!(0.05) },
                dec!(0.8),
                vec![format!("fake_momentum_stream_proxy={flag}")],
            );
        }
        match (input.buy_count, input.unique_buyers) {
            (Some(buys), Some(unique)) if buys > Decimal::ZERO => {
                let unique_ratio = unique / buys;
                RiskEvidence::available(
                    "fake_momentum_risk",
                    "fake_momentum_risk",
                    RiskSource::Stream,
                    RiskTimeRole::DecisionTimeFeature,
                    clamp01(
                        (self.low_unique_buyer_ratio - unique_ratio) / self.low_unique_buyer_ratio,
                    ),
                    dec!(0.7),
                    vec![
                        format!("buy_count={buys}"),
                        format!("unique_buyers={unique}"),
                    ],
                )
            }
            _ => RiskEvidence::unavailable(
                "fake_momentum_risk",
                "fake_momentum_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                "requires_unique_buyer_and_holder_context",
            ),
        }
    }

    fn sell_absorption(&self, input: &RiskSnapshotInput) -> RiskEvidence {
        if let Some(flag) = input.sell_absorption_stream_proxy {
            RiskEvidence::available(
                "sell_absorption_failure_risk",
                "sell_absorption_failure_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                if flag { dec!(0.15) } else { dec!(0.55) },
                dec!(0.7),
                vec![format!("sell_absorption_stream_proxy={flag}")],
            )
        } else {
            RiskEvidence::unavailable(
                "sell_absorption_failure_risk",
                "sell_absorption_failure_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                "missing_sell_absorption_context",
            )
        }
    }

    fn metadata_social(&self, input: &RiskSnapshotInput) -> RiskEvidence {
        match (input.metadata_uri_present, input.metadata_fetch_success) {
            (Some(false), _) => RiskEvidence::available(
                "metadata_social_absence_risk",
                "metadata_social_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                dec!(0.50),
                dec!(0.7),
                vec!["metadata_uri_present=false".to_owned()],
            ),
            (_, Some(success)) => {
                let social_count = input.social_count.unwrap_or_default();
                RiskEvidence::available(
                    "metadata_social_absence_risk",
                    "metadata_social_risk",
                    RiskSource::MetadataHttp,
                    RiskTimeRole::EnrichmentLateFeature,
                    if success && social_count > Decimal::ZERO {
                        dec!(0.05)
                    } else {
                        dec!(0.35)
                    },
                    dec!(0.7),
                    vec![
                        format!("metadata_fetch_success={success}"),
                        format!("social_count={social_count}"),
                    ],
                )
            }
            _ => RiskEvidence::unavailable(
                "metadata_social_absence_risk",
                "metadata_social_risk",
                RiskSource::MetadataHttp,
                RiskTimeRole::EnrichmentLateFeature,
                "metadata_http_not_run",
            ),
        }
    }

    fn funding(&self, input: &RiskSnapshotInput) -> RiskEvidence {
        match input.one_hop_funder_candidate_count {
            Some(count) => RiskEvidence::available(
                "common_funder_risk",
                "funding_common_funder_risk",
                RiskSource::BoundedRpc,
                RiskTimeRole::EnrichmentLateFeature,
                count / dec!(3),
                dec!(0.45),
                vec![
                    format!("one_hop_funder_candidate_count={count}"),
                    "bounded_one_hop_only=true".to_owned(),
                ],
            ),
            None => RiskEvidence::unavailable(
                "common_funder_risk",
                "funding_common_funder_risk",
                RiskSource::BoundedRpc,
                RiskTimeRole::EnrichmentLateFeature,
                "bounded_funding_graph_not_run",
            ),
        }
    }

    fn supply_semantics(&self, input: &RiskSnapshotInput) -> RiskEvidence {
        match input.rpc_supply_matches_curve_supply {
            Some(true) => RiskEvidence::available(
                "supply_semantics_risk",
                "supply_semantics_risk",
                RiskSource::BoundedRpc,
                RiskTimeRole::EnrichmentLateFeature,
                dec!(0.05),
                dec!(0.8),
                vec!["rpc_supply_matches_curve_supply=true".to_owned()],
            ),
            Some(false) => RiskEvidence::available(
                "supply_semantics_risk",
                "supply_semantics_risk",
                RiskSource::BoundedRpc,
                RiskTimeRole::EnrichmentLateFeature,
                input.rpc_supply_mismatch_ratio.unwrap_or(dec!(1)),
                dec!(0.9),
                vec![
                    "rpc_supply_matches_curve_supply=false".to_owned(),
                    "rpc_mint_supply_not_canonical_for_pumpfun_denominator".to_owned(),
                ],
            ),
            None => RiskEvidence::unavailable(
                "supply_semantics_risk",
                "supply_semantics_risk",
                RiskSource::BoundedRpc,
                RiskTimeRole::EnrichmentLateFeature,
                "rpc_supply_semantics_not_checked",
            ),
        }
    }

    fn data_quality(&self, input: &RiskSnapshotInput) -> RiskEvidence {
        let active =
            input.data_gap_active.unwrap_or(false) || input.stream_gap_active.unwrap_or(false);
        RiskEvidence::available(
            "data_quality_risk",
            "data_quality_risk",
            RiskSource::Stream,
            RiskTimeRole::DecisionTimeFeature,
            if active { dec!(1) } else { Decimal::ZERO },
            dec!(1),
            vec![format!("data_or_stream_gap_active={active}")],
        )
    }

    fn post_migration(&self, input: &RiskSnapshotInput) -> RiskEvidence {
        match input.post_migration_supported {
            Some(true) => RiskEvidence::available(
                "post_migration_unknown_risk",
                "post_migration_unknown_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                Decimal::ZERO,
                dec!(0.6),
                vec!["post_migration_supported=true".to_owned()],
            ),
            Some(false) => RiskEvidence::available(
                "post_migration_unknown_risk",
                "post_migration_unknown_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                dec!(0.30),
                dec!(0.5),
                vec!["post_migration_supported=false".to_owned()],
            ),
            None => RiskEvidence::unavailable(
                "post_migration_unknown_risk",
                "post_migration_unknown_risk",
                RiskSource::Stream,
                RiskTimeRole::DecisionTimeFeature,
                "migration_not_observed_or_not_applicable",
            ),
        }
    }
}

pub fn validate_no_post_event_pre_entry_leak(evidence: &[RiskEvidence]) -> Result<(), String> {
    let leaked = evidence
        .iter()
        .filter(|row| {
            row.time_role == RiskTimeRole::PostEventLabel && row.time_role.pre_entry_safe()
        })
        .map(|row| row.risk_id.clone())
        .collect::<Vec<_>>();
    if leaked.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "post-event labels marked pre-entry safe: {leaked:?}"
        ))
    }
}

fn average(values: &[Decimal]) -> Decimal {
    if values.is_empty() {
        Decimal::ZERO
    } else {
        values.iter().copied().sum::<Decimal>() / Decimal::from(values.len() as u64)
    }
}

fn clamp01(value: Decimal) -> Decimal {
    value.max(Decimal::ZERO).min(Decimal::ONE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn post_event_labels_are_not_pre_entry_safe() {
        let evidence = RiskEvidence::available(
            "rapid_collapse_label",
            "rug_outcome_post_event_label",
            RiskSource::Stream,
            RiskTimeRole::PostEventLabel,
            dec!(1),
            dec!(1),
            vec!["future collapse".to_owned()],
        );
        assert!(!evidence.time_role.pre_entry_safe());
        assert!(validate_no_post_event_pre_entry_leak(&[evidence]).is_ok());
    }

    #[test]
    fn bundle_proxy_is_not_provider_confirmed() {
        let engine = DiagnosticRiskEngine::default();
        let out = engine.evaluate(RiskSnapshotInput {
            mint: "mint".to_owned(),
            same_slot_tx_count: Some(dec!(3)),
            ..RiskSnapshotInput::default()
        });
        let bundle = out
            .evidence
            .iter()
            .find(|row| row.risk_id == "same_slot_bundle_like_risk")
            .expect("bundle evidence");
        assert!(
            bundle
                .evidence
                .iter()
                .any(|row| row == "provider_confirmed_bundle=false")
        );
        assert_eq!(bundle.source, RiskSource::Stream);
    }

    #[test]
    fn rpc_supply_mismatch_marks_denominator_unsafe() {
        let engine = DiagnosticRiskEngine::default();
        let out = engine.evaluate(RiskSnapshotInput {
            mint: "mint".to_owned(),
            rpc_supply_matches_curve_supply: Some(false),
            rpc_supply_mismatch_ratio: Some(dec!(2)),
            ..RiskSnapshotInput::default()
        });
        let supply = out
            .evidence
            .iter()
            .find(|row| row.risk_id == "supply_semantics_risk")
            .expect("supply evidence");
        assert_eq!(supply.score, Some(Decimal::ONE));
        assert!(
            supply
                .evidence
                .iter()
                .any(|row| row == "rpc_mint_supply_not_canonical_for_pumpfun_denominator")
        );
    }

    #[test]
    fn unavailable_risk_does_not_become_zero_evidence() {
        let engine = DiagnosticRiskEngine::default();
        let out = engine.evaluate(RiskSnapshotInput {
            mint: "mint".to_owned(),
            ..RiskSnapshotInput::default()
        });
        assert!(
            out.evidence
                .iter()
                .any(|row| row.score.is_none() && row.unavailable_reason.is_some())
        );
    }

    #[test]
    fn fake_momentum_needs_unique_buyer_context() {
        let engine = DiagnosticRiskEngine::default();
        let out = engine.evaluate(RiskSnapshotInput {
            mint: "mint".to_owned(),
            buy_count: Some(dec!(10)),
            unique_buyers: None,
            ..RiskSnapshotInput::default()
        });
        let fake = out
            .evidence
            .iter()
            .find(|row| row.risk_id == "fake_momentum_risk")
            .expect("fake momentum evidence");
        assert_eq!(
            fake.unavailable_reason.as_deref(),
            Some("requires_unique_buyer_and_holder_context")
        );
    }
}
