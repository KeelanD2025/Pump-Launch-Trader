use std::{collections::BTreeMap, path::Path};

use common::{
    Canonicality, DEFAULT_PUMP_TOKEN_DECIMALS, DataGapEvent, DataGapType, EventMeta, EventPayload,
    EventSource, GapSeverity, HolderBalanceUpdateEvent, NormalizedEvent, ObservedTransactionEvent,
    PubkeyValue, PumpBuyEvent, PumpSellEvent, QuoteAssetType, ReasonCode, TokenCreatedEvent,
    TokenProgramType, TokenTerminalEvent, TokenTerminalVariant, TransactionStatus,
    WalletFundingEvent,
};
use ingest_shred::{DecodedShredBatch, DecodedShredPayload, DecodedShredTransaction};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use state::TokenLifecycle;
use time::{Duration, OffsetDateTime};

fn dec(value: i64, scale: u32) -> Decimal {
    Decimal::new(value, scale)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FixtureScenarioKind {
    CleanOrganicLaunch,
    DevDumpLaunch,
    BundledLaunch,
    TopHolderDump,
    #[serde(rename = "price_collapse_70_within_5m")]
    PriceCollapse70Within5m,
    StrongHolderGrowthWinner,
    CapitalRotationWinner,
    FakeMomentumTrap,
    SellAbsorptionBounce,
    DataGapToken,
    MigrationTerminalToken,
    FailedTransactionCase,
    HighPriorityFeeCase,
    FalseDiscardWinner,
    ShredDevSellEarlyExit,
    ShredTopHolderDumpEarlyExit,
    ShredBundleClusterExit,
    ShredFalsePositiveSell,
    ShredLowConfidencePartial,
    ShredSellAbsorbed,
    ShredExitTooLate,
    ShredGeyserDisagreement,
    ShredAccountEffectConfirmation,
    ShredReorgedSell,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FixtureExpectation {
    pub expect_enter_paper: bool,
    pub expect_no_enter_paper: bool,
    pub expect_data_gap: bool,
    pub expect_discard: bool,
    pub min_rug_score: Decimal,
    pub min_bundle_score: Decimal,
    #[serde(default)]
    pub expected_min_lifecycle: Option<TokenLifecycle>,
    #[serde(default)]
    pub expected_allowed_decisions: Vec<String>,
    #[serde(default)]
    pub expected_forbidden_decisions: Vec<String>,
    #[serde(default)]
    pub expected_risk_flags: Vec<String>,
    #[serde(default)]
    pub expected_discard_state: Option<TokenLifecycle>,
    #[serde(default)]
    pub expected_paper_fill_min_count: usize,
    #[serde(default)]
    pub expected_false_discard: bool,
    #[serde(default)]
    pub expected_global_safety_block: bool,
    #[serde(default)]
    pub expected_token_safety_block: bool,
    #[serde(default)]
    pub expected_early_sell_warning: bool,
    #[serde(default)]
    pub expected_exit_armed: bool,
    #[serde(default)]
    pub expected_emergency_exit: bool,
    #[serde(default)]
    pub expected_saved_loss_positive: bool,
    #[serde(default)]
    pub expected_false_positive: bool,
    #[serde(default = "default_true")]
    pub expected_no_live_send: bool,
    #[serde(default)]
    pub expected_reconciliation_outcome: Option<String>,
    #[serde(default)]
    pub expected_confirmation_level: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureScenarioSpec {
    pub name: String,
    pub kind: FixtureScenarioKind,
    pub description: String,
    pub expected: FixtureExpectation,
}

#[derive(Debug, Clone)]
pub struct FixtureScenario {
    pub spec: FixtureScenarioSpec,
    pub shred_batches: Vec<DecodedShredBatch>,
    pub canonical_events: Vec<NormalizedEvent>,
    pub timeline_events: Vec<NormalizedEvent>,
}

pub fn load_fixture_spec(path: impl AsRef<Path>) -> anyhow::Result<FixtureScenarioSpec> {
    let raw = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&raw)?)
}

pub fn builtin_fixture_suite() -> Vec<FixtureScenarioSpec> {
    vec![
        spec(FixtureScenarioKind::CleanOrganicLaunch),
        spec(FixtureScenarioKind::DevDumpLaunch),
        spec(FixtureScenarioKind::BundledLaunch),
        spec(FixtureScenarioKind::TopHolderDump),
        spec(FixtureScenarioKind::PriceCollapse70Within5m),
        spec(FixtureScenarioKind::StrongHolderGrowthWinner),
        spec(FixtureScenarioKind::CapitalRotationWinner),
        spec(FixtureScenarioKind::FakeMomentumTrap),
        spec(FixtureScenarioKind::SellAbsorptionBounce),
        spec(FixtureScenarioKind::DataGapToken),
        spec(FixtureScenarioKind::MigrationTerminalToken),
        spec(FixtureScenarioKind::FailedTransactionCase),
        spec(FixtureScenarioKind::HighPriorityFeeCase),
        spec(FixtureScenarioKind::FalseDiscardWinner),
    ]
}

pub fn builtin_shred_exit_fixture_suite() -> Vec<FixtureScenarioSpec> {
    vec![
        spec(FixtureScenarioKind::ShredDevSellEarlyExit),
        spec(FixtureScenarioKind::ShredTopHolderDumpEarlyExit),
        spec(FixtureScenarioKind::ShredBundleClusterExit),
        spec(FixtureScenarioKind::ShredFalsePositiveSell),
        spec(FixtureScenarioKind::ShredLowConfidencePartial),
        spec(FixtureScenarioKind::ShredSellAbsorbed),
        spec(FixtureScenarioKind::ShredExitTooLate),
        spec(FixtureScenarioKind::ShredGeyserDisagreement),
        spec(FixtureScenarioKind::ShredAccountEffectConfirmation),
        spec(FixtureScenarioKind::ShredReorgedSell),
    ]
}

pub fn build_fixture_scenario(spec: &FixtureScenarioSpec) -> FixtureScenario {
    match spec.kind {
        FixtureScenarioKind::CleanOrganicLaunch => clean_organic_launch(spec.clone()),
        FixtureScenarioKind::DevDumpLaunch => dev_dump_launch(spec.clone()),
        FixtureScenarioKind::BundledLaunch => bundled_launch(spec.clone()),
        FixtureScenarioKind::TopHolderDump => top_holder_dump(spec.clone()),
        FixtureScenarioKind::PriceCollapse70Within5m => collapse_70(spec.clone()),
        FixtureScenarioKind::StrongHolderGrowthWinner => strong_holder_growth_winner(spec.clone()),
        FixtureScenarioKind::CapitalRotationWinner => capital_rotation_winner(spec.clone()),
        FixtureScenarioKind::FakeMomentumTrap => fake_momentum_trap(spec.clone()),
        FixtureScenarioKind::SellAbsorptionBounce => sell_absorption_bounce(spec.clone()),
        FixtureScenarioKind::DataGapToken => data_gap_token(spec.clone()),
        FixtureScenarioKind::MigrationTerminalToken => migration_terminal(spec.clone()),
        FixtureScenarioKind::FailedTransactionCase => failed_transaction_case(spec.clone()),
        FixtureScenarioKind::HighPriorityFeeCase => high_priority_fee_case(spec.clone()),
        FixtureScenarioKind::FalseDiscardWinner => false_discard_winner(spec.clone()),
        FixtureScenarioKind::ShredDevSellEarlyExit => shred_dev_sell_early_exit(spec.clone()),
        FixtureScenarioKind::ShredTopHolderDumpEarlyExit => {
            shred_top_holder_dump_early_exit(spec.clone())
        }
        FixtureScenarioKind::ShredBundleClusterExit => shred_bundle_cluster_exit(spec.clone()),
        FixtureScenarioKind::ShredFalsePositiveSell => shred_false_positive_sell(spec.clone()),
        FixtureScenarioKind::ShredLowConfidencePartial => {
            shred_low_confidence_partial(spec.clone())
        }
        FixtureScenarioKind::ShredSellAbsorbed => shred_sell_absorbed(spec.clone()),
        FixtureScenarioKind::ShredExitTooLate => shred_exit_too_late(spec.clone()),
        FixtureScenarioKind::ShredGeyserDisagreement => shred_geyser_disagreement(spec.clone()),
        FixtureScenarioKind::ShredAccountEffectConfirmation => {
            shred_account_effect_confirmation(spec.clone())
        }
        FixtureScenarioKind::ShredReorgedSell => shred_reorged_sell(spec.clone()),
    }
}

pub fn spec(kind: FixtureScenarioKind) -> FixtureScenarioSpec {
    let (name, description, expected) = match kind {
        FixtureScenarioKind::CleanOrganicLaunch => (
            "clean_organic_launch",
            "Broad early buying, healthy holder growth, no creator sell.",
            FixtureExpectation {
                expect_enter_paper: true,
                expect_no_enter_paper: false,
                expect_data_gap: false,
                expect_discard: false,
                min_rug_score: Decimal::ZERO,
                min_bundle_score: Decimal::ZERO,
                expected_min_lifecycle: Some(TokenLifecycle::ActiveDeep),
                expected_allowed_decisions: vec!["WatchDeep".to_owned(), "EnterPaper".to_owned()],
                ..Default::default()
            },
        ),
        FixtureScenarioKind::DevDumpLaunch => (
            "dev_dump_launch",
            "Creator exits early into weak holder growth.",
            FixtureExpectation {
                expect_enter_paper: false,
                expect_no_enter_paper: true,
                expect_data_gap: false,
                expect_discard: true,
                min_rug_score: Decimal::ZERO,
                min_bundle_score: Decimal::ZERO,
                expected_risk_flags: vec!["DevSoldEarly".to_owned()],
                expected_discard_state: Some(TokenLifecycle::SoftDiscarded),
                ..Default::default()
            },
        ),
        FixtureScenarioKind::BundledLaunch => (
            "bundled_launch",
            "First buyers share profiles and funders.",
            FixtureExpectation {
                expect_enter_paper: false,
                expect_no_enter_paper: true,
                expect_data_gap: false,
                expect_discard: false,
                min_rug_score: Decimal::ZERO,
                min_bundle_score: dec(39, 2),
                expected_risk_flags: vec!["BundleConcentration".to_owned()],
                expected_forbidden_decisions: vec!["EnterPaper".to_owned()],
                ..Default::default()
            },
        ),
        FixtureScenarioKind::TopHolderDump => (
            "top_holder_dump",
            "Concentrated holder base with an early whale exit.",
            FixtureExpectation {
                expect_enter_paper: false,
                expect_no_enter_paper: true,
                expect_data_gap: false,
                expect_discard: false,
                min_rug_score: Decimal::ZERO,
                min_bundle_score: Decimal::ZERO,
                expected_risk_flags: vec!["TopHolderDump".to_owned()],
                expected_forbidden_decisions: vec!["EnterPaper".to_owned()],
                ..Default::default()
            },
        ),
        FixtureScenarioKind::PriceCollapse70Within5m => (
            "price_collapse_70_within_5m",
            "Collapse through launch floor inside five minutes.",
            FixtureExpectation {
                expect_enter_paper: false,
                expect_no_enter_paper: false,
                expect_data_gap: false,
                expect_discard: true,
                min_rug_score: Decimal::ZERO,
                min_bundle_score: Decimal::ZERO,
                expected_discard_state: Some(TokenLifecycle::HardDiscarded),
                ..Default::default()
            },
        ),
        FixtureScenarioKind::StrongHolderGrowthWinner => (
            "strong_holder_growth_winner",
            "Wide cohort participation and steady holder expansion.",
            FixtureExpectation {
                expect_enter_paper: true,
                expect_no_enter_paper: false,
                expect_data_gap: false,
                expect_discard: false,
                min_rug_score: Decimal::ZERO,
                min_bundle_score: Decimal::ZERO,
                expected_min_lifecycle: Some(TokenLifecycle::ActiveDeep),
                expected_paper_fill_min_count: 1,
                expected_allowed_decisions: vec!["EnterPaper".to_owned()],
                ..Default::default()
            },
        ),
        FixtureScenarioKind::CapitalRotationWinner => (
            "capital_rotation_winner",
            "Wallets rotate out of a weaker launch and into the winner.",
            FixtureExpectation {
                expect_enter_paper: true,
                expect_no_enter_paper: false,
                expect_data_gap: false,
                expect_discard: false,
                min_rug_score: Decimal::ZERO,
                min_bundle_score: Decimal::ZERO,
                expected_min_lifecycle: Some(TokenLifecycle::ActiveLight),
                expected_allowed_decisions: vec!["WatchDeep".to_owned(), "EnterPaper".to_owned()],
                ..Default::default()
            },
        ),
        FixtureScenarioKind::FakeMomentumTrap => (
            "fake_momentum_trap",
            "Price rises on narrow flow while strong holders distribute.",
            FixtureExpectation {
                expect_enter_paper: false,
                expect_no_enter_paper: true,
                expect_data_gap: false,
                expect_discard: false,
                min_rug_score: Decimal::ZERO,
                min_bundle_score: Decimal::ZERO,
                expected_risk_flags: vec!["HighFakeMomentumRisk".to_owned()],
                expected_forbidden_decisions: vec!["EnterPaper".to_owned()],
                ..Default::default()
            },
        ),
        FixtureScenarioKind::SellAbsorptionBounce => (
            "sell_absorption_bounce",
            "Large sell is absorbed by broad follow-on buying.",
            FixtureExpectation {
                expect_enter_paper: true,
                expect_no_enter_paper: false,
                expect_data_gap: false,
                expect_discard: false,
                min_rug_score: Decimal::ZERO,
                min_bundle_score: Decimal::ZERO,
                expected_min_lifecycle: Some(TokenLifecycle::ActiveLight),
                expected_allowed_decisions: vec!["WatchDeep".to_owned(), "EnterPaper".to_owned()],
                ..Default::default()
            },
        ),
        FixtureScenarioKind::DataGapToken => (
            "data_gap_token",
            "Canonical gap event blocks trading.",
            FixtureExpectation {
                expect_enter_paper: false,
                expect_no_enter_paper: true,
                expect_data_gap: true,
                expect_discard: false,
                min_rug_score: Decimal::ZERO,
                min_bundle_score: Decimal::ZERO,
                expected_risk_flags: vec!["DataGapActive".to_owned()],
                expected_token_safety_block: true,
                expected_global_safety_block: false,
                ..Default::default()
            },
        ),
        FixtureScenarioKind::MigrationTerminalToken => (
            "migration_terminal_token",
            "Migration marks the token terminal for pre-migration strategy.",
            FixtureExpectation {
                expect_enter_paper: false,
                expect_no_enter_paper: true,
                expect_data_gap: false,
                expect_discard: false,
                min_rug_score: Decimal::ZERO,
                min_bundle_score: Decimal::ZERO,
                expected_forbidden_decisions: vec!["EnterPaper".to_owned()],
                ..Default::default()
            },
        ),
        FixtureScenarioKind::FailedTransactionCase => (
            "failed_transaction_case",
            "Weak edge and fee drag produce no executable paper entry.",
            FixtureExpectation {
                expect_enter_paper: false,
                expect_no_enter_paper: true,
                expect_data_gap: false,
                expect_discard: false,
                min_rug_score: Decimal::ZERO,
                min_bundle_score: Decimal::ZERO,
                expected_forbidden_decisions: vec!["EnterPaper".to_owned()],
                ..Default::default()
            },
        ),
        FixtureScenarioKind::HighPriorityFeeCase => (
            "high_priority_fee_case",
            "Fee war makes a marginal move untradeable.",
            FixtureExpectation {
                expect_enter_paper: false,
                expect_no_enter_paper: true,
                expect_data_gap: false,
                expect_discard: false,
                min_rug_score: Decimal::ZERO,
                min_bundle_score: Decimal::ZERO,
                expected_forbidden_decisions: vec!["EnterPaper".to_owned()],
                ..Default::default()
            },
        ),
        FixtureScenarioKind::FalseDiscardWinner => (
            "false_discard_winner",
            "Early weakness recovers into a later winner and should be archived for study.",
            FixtureExpectation {
                expect_enter_paper: false,
                expect_no_enter_paper: false,
                expect_data_gap: false,
                expect_discard: true,
                min_rug_score: Decimal::ZERO,
                min_bundle_score: Decimal::ZERO,
                expected_false_discard: true,
                expected_discard_state: Some(TokenLifecycle::SoftDiscarded),
                ..Default::default()
            },
        ),
        FixtureScenarioKind::ShredDevSellEarlyExit => (
            "shred_dev_sell_early_exit",
            "A dev sell appears tentatively before canonical confirmation and paper exits early.",
            FixtureExpectation {
                expected_allowed_decisions: vec!["EmergencyExit".to_owned()],
                expected_early_sell_warning: true,
                expected_exit_armed: true,
                expected_emergency_exit: true,
                expected_saved_loss_positive: true,
                expected_paper_fill_min_count: 2,
                expected_reconciliation_outcome: Some("ConfirmedExecuted".to_owned()),
                expected_confirmation_level: Some("ConfirmedExecuted".to_owned()),
                ..Default::default()
            },
        ),
        FixtureScenarioKind::ShredTopHolderDumpEarlyExit => (
            "shred_top_holder_dump_early_exit",
            "A top holder dump is seen tentatively and paper exits before full damage.",
            FixtureExpectation {
                expected_allowed_decisions: vec!["EmergencyExit".to_owned()],
                expected_early_sell_warning: true,
                expected_exit_armed: true,
                expected_emergency_exit: true,
                expected_saved_loss_positive: true,
                expected_paper_fill_min_count: 2,
                expected_reconciliation_outcome: Some("ConfirmedExecuted".to_owned()),
                expected_confirmation_level: Some("ConfirmedExecuted".to_owned()),
                ..Default::default()
            },
        ),
        FixtureScenarioKind::ShredBundleClusterExit => (
            "shred_bundle_cluster_exit",
            "Multiple same-funder sellers appear in the same tentative slot and suppress new entries.",
            FixtureExpectation {
                expected_early_sell_warning: true,
                expected_exit_armed: true,
                expected_emergency_exit: false,
                expected_forbidden_decisions: vec!["EnterPaper".to_owned()],
                expected_reconciliation_outcome: Some("ConfirmedExecuted".to_owned()),
                expected_confirmation_level: Some("ConfirmedExecuted".to_owned()),
                ..Default::default()
            },
        ),
        FixtureScenarioKind::ShredFalsePositiveSell => (
            "shred_false_positive_sell",
            "A tentative sell triggers paper defense but the canonical sell later fails.",
            FixtureExpectation {
                expected_early_sell_warning: true,
                expected_exit_armed: true,
                expected_emergency_exit: false,
                expected_false_positive: true,
                expected_reconciliation_outcome: Some("NotSeenWithinTtl".to_owned()),
                expected_confirmation_level: Some("NotSeenWithinTtl".to_owned()),
                ..Default::default()
            },
        ),
        FixtureScenarioKind::ShredLowConfidencePartial => (
            "shred_low_confidence_partial",
            "A partial low-confidence tentative decode warns only and never exits.",
            FixtureExpectation {
                expected_early_sell_warning: true,
                expected_exit_armed: false,
                expected_emergency_exit: false,
                expected_forbidden_decisions: vec!["EmergencyExit".to_owned()],
                expected_reconciliation_outcome: Some("NotSeenWithinTtl".to_owned()),
                expected_confirmation_level: Some("NotSeenWithinTtl".to_owned()),
                ..Default::default()
            },
        ),
        FixtureScenarioKind::ShredSellAbsorbed => (
            "shred_sell_absorbed",
            "A suspicious tentative sell is absorbed and only arms/tightens rather than panic exiting.",
            FixtureExpectation {
                expected_early_sell_warning: true,
                expected_exit_armed: true,
                expected_emergency_exit: false,
                expected_allowed_decisions: vec!["WatchDeep".to_owned(), "Hold".to_owned()],
                expected_reconciliation_outcome: Some("ConfirmedExecuted".to_owned()),
                expected_confirmation_level: Some("ConfirmedExecuted".to_owned()),
                ..Default::default()
            },
        ),
        FixtureScenarioKind::ShredExitTooLate => (
            "shred_exit_too_late",
            "Tentative warning arrives too late, so the defense arms and records the stale signal instead of panic exiting.",
            FixtureExpectation {
                expected_early_sell_warning: true,
                expected_exit_armed: true,
                expected_emergency_exit: false,
                expected_forbidden_decisions: vec!["EmergencyExit".to_owned()],
                expected_reconciliation_outcome: Some("ConfirmedExecuted".to_owned()),
                expected_confirmation_level: Some("ConfirmedExecuted".to_owned()),
                ..Default::default()
            },
        ),
        FixtureScenarioKind::ShredGeyserDisagreement => (
            "shred_geyser_disagreement",
            "Tentative decode disagrees with later canonical sell details and records mismatch.",
            FixtureExpectation {
                expected_early_sell_warning: true,
                expected_reconciliation_outcome: Some("DecodeMismatch".to_owned()),
                expected_confirmation_level: Some("DecodeMismatch".to_owned()),
                ..Default::default()
            },
        ),
        FixtureScenarioKind::ShredAccountEffectConfirmation => (
            "shred_account_effect_confirmation",
            "Tentative sell resolves via holder/account effects when signature matching is weak.",
            FixtureExpectation {
                expected_early_sell_warning: true,
                expected_exit_armed: true,
                expected_emergency_exit: true,
                expected_reconciliation_outcome: Some("AccountEffectsObserved".to_owned()),
                expected_confirmation_level: Some("AccountEffectsObserved".to_owned()),
                ..Default::default()
            },
        ),
        FixtureScenarioKind::ShredReorgedSell => (
            "shred_reorged_sell",
            "Tentative sell is seen, then canonical processing reverts it and reconciliation marks reorg.",
            FixtureExpectation {
                expected_early_sell_warning: true,
                expected_reconciliation_outcome: Some("Reorged".to_owned()),
                expected_confirmation_level: Some("Reorged".to_owned()),
                ..Default::default()
            },
        ),
    };
    FixtureScenarioSpec {
        name: name.to_owned(),
        kind,
        description: description.to_owned(),
        expected,
    }
}

fn clean_organic_launch(spec: FixtureScenarioSpec) -> FixtureScenario {
    single_token_fixture(
        spec,
        "mint-clean",
        vec![
            canonical_observed_tx(1, "sig-clean-create", 10, 3, vec!["pump".to_owned()]),
            canonical_create(
                1,
                "mint-clean",
                "creator-clean",
                "payer-clean",
                "Alpha",
                "ALP",
                "factory-organic",
                "sig-clean-create",
            ),
            buy_event(
                2,
                "mint-clean",
                "buyer-a",
                20,
                100,
                "sig-clean-buy-1",
                180_000,
                800,
                false,
                false,
            ),
            buy_event(
                3,
                "mint-clean",
                "buyer-b",
                24,
                100,
                "sig-clean-buy-2",
                190_000,
                900,
                false,
                false,
            ),
            buy_event(
                4,
                "mint-clean",
                "buyer-c",
                30,
                100,
                "sig-clean-buy-3",
                200_000,
                1_100,
                false,
                false,
            ),
            holder_event(4, "mint-clean", "buyer-a", 100),
            holder_event(4, "mint-clean", "buyer-b", 100),
            holder_event(4, "mint-clean", "buyer-c", 100),
        ],
        vec![
            shred_create_packet(
                1,
                0,
                "sig-clean-create",
                "mint-clean",
                "creator-clean",
                "payer-clean",
                "Alpha",
                "ALP",
                "factory-organic",
            ),
            shred_buy_packet(
                2,
                0,
                "sig-clean-buy-1",
                "mint-clean",
                "buyer-a",
                20,
                100,
                180_000,
                800,
            ),
        ],
    )
}

fn dev_dump_launch(spec: FixtureScenarioSpec) -> FixtureScenario {
    single_token_fixture(
        spec,
        "mint-devdump",
        vec![
            canonical_create(
                1,
                "mint-devdump",
                "creator-devdump",
                "payer-devdump",
                "Dump",
                "DMP",
                "factory-dump",
                "sig-dev-create",
            ),
            buy_event(
                2,
                "mint-devdump",
                "buyer-a",
                10,
                100,
                "sig-dev-buy-1",
                180_000,
                800,
                false,
                false,
            ),
            sell_event(
                3,
                "mint-devdump",
                "creator-devdump",
                3,
                80,
                "sig-dev-sell-1",
                180_000,
                800,
                true,
                true,
            ),
            holder_event(3, "mint-devdump", "buyer-a", 100),
            holder_event(3, "mint-devdump", "creator-devdump", 20),
        ],
        vec![
            shred_create_packet(
                1,
                0,
                "sig-dev-create",
                "mint-devdump",
                "creator-devdump",
                "payer-devdump",
                "Dump",
                "DMP",
                "factory-dump",
            ),
            shred_sell_packet(
                3,
                0,
                "sig-dev-sell-1",
                "mint-devdump",
                "creator-devdump",
                3,
                80,
                180_000,
                800,
                true,
            ),
        ],
    )
}

fn bundled_launch(spec: FixtureScenarioSpec) -> FixtureScenario {
    let events = vec![
        canonical_create(
            1,
            "mint-bundle",
            "creator-bundle",
            "payer-bundle",
            "Bundle",
            "BND",
            "factory-bundle",
            "sig-bundle-create",
        ),
        funding_event(1, "buyer-b1", "funder-b", 5, "sig-fund-b1"),
        funding_event(1, "buyer-b2", "funder-b", 5, "sig-fund-b2"),
        funding_event(1, "buyer-b3", "funder-b", 5, "sig-fund-b3"),
        buy_event(
            2,
            "mint-bundle",
            "buyer-b1",
            15,
            100,
            "sig-bundle-buy-1",
            200_000,
            3_000,
            false,
            false,
        ),
        buy_event(
            2,
            "mint-bundle",
            "buyer-b2",
            15,
            100,
            "sig-bundle-buy-2",
            200_000,
            3_000,
            false,
            false,
        ),
        buy_event(
            2,
            "mint-bundle",
            "buyer-b3",
            15,
            100,
            "sig-bundle-buy-3",
            200_000,
            3_000,
            false,
            false,
        ),
        holder_event(2, "mint-bundle", "buyer-b1", 100),
        holder_event(2, "mint-bundle", "buyer-b2", 100),
        holder_event(2, "mint-bundle", "buyer-b3", 100),
    ];
    FixtureScenario {
        spec,
        canonical_events: events,
        shred_batches: vec![
            shred_create_packet(
                1,
                0,
                "sig-bundle-create",
                "mint-bundle",
                "creator-bundle",
                "payer-bundle",
                "Bundle",
                "BND",
                "factory-bundle",
            ),
            shred_buy_packet(
                2,
                0,
                "sig-bundle-buy-1",
                "mint-bundle",
                "buyer-b1",
                15,
                100,
                200_000,
                3_000,
            ),
        ],
        timeline_events: Vec::new(),
    }
}

fn top_holder_dump(spec: FixtureScenarioSpec) -> FixtureScenario {
    single_token_fixture(
        spec,
        "mint-topdump",
        vec![
            canonical_create(
                1,
                "mint-topdump",
                "creator-top",
                "payer-top",
                "Whale",
                "WHL",
                "factory-top",
                "sig-top-create",
            ),
            buy_event(
                2,
                "mint-topdump",
                "whale-top",
                30,
                300,
                "sig-top-buy-1",
                180_000,
                1_000,
                false,
                false,
            ),
            buy_event(
                3,
                "mint-topdump",
                "retail-top",
                5,
                40,
                "sig-top-buy-2",
                180_000,
                900,
                false,
                false,
            ),
            holder_event(3, "mint-topdump", "whale-top", 300),
            holder_event(3, "mint-topdump", "retail-top", 40),
            sell_event(
                4,
                "mint-topdump",
                "whale-top",
                8,
                180,
                "sig-top-sell-1",
                180_000,
                1_200,
                false,
                true,
            ),
        ],
        vec![shred_buy_packet(
            2,
            0,
            "sig-top-buy-1",
            "mint-topdump",
            "whale-top",
            30,
            300,
            180_000,
            1_000,
        )],
    )
}

fn collapse_70(spec: FixtureScenarioSpec) -> FixtureScenario {
    single_token_fixture(
        spec,
        "mint-collapse",
        vec![
            canonical_create(
                1,
                "mint-collapse",
                "creator-col",
                "payer-col",
                "Collapse",
                "COL",
                "factory-collapse",
                "sig-col-create",
            ),
            buy_event(
                2,
                "mint-collapse",
                "buyer-col",
                20,
                100,
                "sig-col-buy",
                180_000,
                1_000,
                false,
                false,
            ),
            holder_event(2, "mint-collapse", "buyer-col", 100),
            sell_event(
                5,
                "mint-collapse",
                "creator-col",
                1,
                95,
                "sig-col-sell",
                180_000,
                1_000,
                true,
                true,
            ),
            terminal_event(
                5,
                "mint-collapse",
                TokenTerminalVariant::HardDiscarded,
                vec![ReasonCode::PriceCollapse70],
            ),
        ],
        vec![shred_sell_packet(
            5,
            0,
            "sig-col-sell",
            "mint-collapse",
            "creator-col",
            1,
            95,
            180_000,
            1_000,
            true,
        )],
    )
}

fn strong_holder_growth_winner(spec: FixtureScenarioSpec) -> FixtureScenario {
    single_token_fixture(
        spec,
        "mint-grow",
        vec![
            canonical_create(
                1,
                "mint-grow",
                "creator-grow",
                "payer-grow",
                "Growth",
                "GRW",
                "factory-grow",
                "sig-grow-create",
            ),
            buy_event(
                2,
                "mint-grow",
                "buyer-g1",
                10,
                80,
                "sig-grow-buy-1",
                180_000,
                700,
                false,
                false,
            ),
            buy_event(
                3,
                "mint-grow",
                "buyer-g2",
                12,
                80,
                "sig-grow-buy-2",
                180_000,
                700,
                false,
                false,
            ),
            buy_event(
                4,
                "mint-grow",
                "buyer-g3",
                14,
                80,
                "sig-grow-buy-3",
                180_000,
                700,
                false,
                false,
            ),
            buy_event(
                5,
                "mint-grow",
                "buyer-g4",
                16,
                80,
                "sig-grow-buy-4",
                180_000,
                700,
                false,
                false,
            ),
            holder_event(5, "mint-grow", "buyer-g1", 80),
            holder_event(5, "mint-grow", "buyer-g2", 80),
            holder_event(5, "mint-grow", "buyer-g3", 80),
            holder_event(5, "mint-grow", "buyer-g4", 80),
        ],
        vec![shred_create_packet(
            1,
            0,
            "sig-grow-create",
            "mint-grow",
            "creator-grow",
            "payer-grow",
            "Growth",
            "GRW",
            "factory-grow",
        )],
    )
}

fn capital_rotation_winner(spec: FixtureScenarioSpec) -> FixtureScenario {
    let events = vec![
        canonical_create(
            1,
            "mint-weak",
            "creator-weak",
            "payer-weak",
            "Weak",
            "WEK",
            "factory-weak",
            "sig-weak-create",
        ),
        buy_event(
            2,
            "mint-weak",
            "rotator",
            10,
            100,
            "sig-weak-buy",
            180_000,
            800,
            false,
            false,
        ),
        sell_event(
            3,
            "mint-weak",
            "rotator",
            6,
            100,
            "sig-weak-sell",
            180_000,
            800,
            false,
            true,
        ),
        canonical_create(
            4,
            "mint-rot",
            "creator-rot",
            "payer-rot",
            "Rotate",
            "ROT",
            "factory-rot",
            "sig-rot-create",
        ),
        buy_event(
            5,
            "mint-rot",
            "rotator",
            20,
            120,
            "sig-rot-buy-1",
            190_000,
            900,
            false,
            false,
        ),
        buy_event(
            6,
            "mint-rot",
            "buyer-rot-2",
            18,
            100,
            "sig-rot-buy-2",
            190_000,
            900,
            false,
            false,
        ),
        holder_event(6, "mint-rot", "rotator", 120),
        holder_event(6, "mint-rot", "buyer-rot-2", 100),
    ];
    FixtureScenario {
        spec,
        canonical_events: events,
        shred_batches: vec![shred_buy_packet(
            5,
            0,
            "sig-rot-buy-1",
            "mint-rot",
            "rotator",
            20,
            120,
            190_000,
            900,
        )],
        timeline_events: Vec::new(),
    }
}

fn fake_momentum_trap(spec: FixtureScenarioSpec) -> FixtureScenario {
    single_token_fixture(
        spec,
        "mint-fake",
        vec![
            canonical_create(
                1,
                "mint-fake",
                "creator-fake",
                "payer-fake",
                "Fake",
                "FAK",
                "factory-fake",
                "sig-fake-create",
            ),
            buy_event(
                2,
                "mint-fake",
                "whale-fake",
                50,
                100,
                "sig-fake-buy-1",
                220_000,
                5_000,
                false,
                false,
            ),
            holder_event(2, "mint-fake", "whale-fake", 100),
            sell_event(
                3,
                "mint-fake",
                "creator-fake",
                10,
                40,
                "sig-fake-sell-1",
                220_000,
                5_000,
                true,
                false,
            ),
            sell_event(
                4,
                "mint-fake",
                "whale-fake",
                30,
                60,
                "sig-fake-sell-2",
                220_000,
                5_000,
                false,
                true,
            ),
        ],
        vec![shred_buy_packet(
            2,
            0,
            "sig-fake-buy-1",
            "mint-fake",
            "whale-fake",
            50,
            100,
            220_000,
            5_000,
        )],
    )
}

fn sell_absorption_bounce(spec: FixtureScenarioSpec) -> FixtureScenario {
    single_token_fixture(
        spec,
        "mint-absorb",
        vec![
            canonical_create(
                1,
                "mint-absorb",
                "creator-absorb",
                "payer-absorb",
                "Absorb",
                "ABS",
                "factory-absorb",
                "sig-abs-create",
            ),
            buy_event(
                2,
                "mint-absorb",
                "buyer-a1",
                20,
                100,
                "sig-abs-buy-1",
                180_000,
                800,
                false,
                false,
            ),
            holder_event(2, "mint-absorb", "buyer-a1", 100),
            sell_event(
                3,
                "mint-absorb",
                "buyer-a1",
                8,
                60,
                "sig-abs-sell-1",
                180_000,
                800,
                false,
                true,
            ),
            buy_event(
                4,
                "mint-absorb",
                "buyer-a2",
                10,
                60,
                "sig-abs-buy-2",
                180_000,
                800,
                false,
                false,
            ),
            buy_event(
                4,
                "mint-absorb",
                "buyer-a3",
                10,
                60,
                "sig-abs-buy-3",
                180_000,
                800,
                false,
                false,
            ),
            holder_event(4, "mint-absorb", "buyer-a2", 60),
            holder_event(4, "mint-absorb", "buyer-a3", 60),
        ],
        vec![shred_sell_packet(
            3,
            0,
            "sig-abs-sell-1",
            "mint-absorb",
            "buyer-a1",
            8,
            60,
            180_000,
            800,
            true,
        )],
    )
}

fn data_gap_token(spec: FixtureScenarioSpec) -> FixtureScenario {
    single_token_fixture(
        spec,
        "mint-gap",
        vec![
            canonical_create(
                1,
                "mint-gap",
                "creator-gap",
                "payer-gap",
                "Gap",
                "GAP",
                "factory-gap",
                "sig-gap-create",
            ),
            gap_event(2, Some("mint-gap")),
            buy_event(
                3,
                "mint-gap",
                "buyer-gap",
                10,
                100,
                "sig-gap-buy",
                180_000,
                800,
                false,
                false,
            ),
        ],
        vec![shred_create_packet(
            1,
            0,
            "sig-gap-create",
            "mint-gap",
            "creator-gap",
            "payer-gap",
            "Gap",
            "GAP",
            "factory-gap",
        )],
    )
}

fn migration_terminal(spec: FixtureScenarioSpec) -> FixtureScenario {
    single_token_fixture(
        spec,
        "mint-migrate",
        vec![
            canonical_create(
                1,
                "mint-migrate",
                "creator-migrate",
                "payer-migrate",
                "Migrator",
                "MIG",
                "factory-mig",
                "sig-mig-create",
            ),
            buy_event(
                2,
                "mint-migrate",
                "buyer-mig",
                10,
                100,
                "sig-mig-buy",
                180_000,
                800,
                false,
                false,
            ),
            terminal_event(3, "mint-migrate", TokenTerminalVariant::Migrated, vec![]),
        ],
        vec![],
    )
}

fn failed_transaction_case(spec: FixtureScenarioSpec) -> FixtureScenario {
    single_token_fixture(
        spec,
        "mint-fail",
        vec![
            canonical_create(
                1,
                "mint-fail",
                "creator-fail",
                "payer-fail",
                "Fail",
                "FL",
                "factory-fail",
                "sig-fail-create",
            ),
            failed_buy_event(2, "mint-fail", "buyer-fail", 8, 100, "sig-fail-buy"),
        ],
        vec![],
    )
}

fn high_priority_fee_case(spec: FixtureScenarioSpec) -> FixtureScenario {
    single_token_fixture(
        spec,
        "mint-fee",
        vec![
            canonical_create(
                1,
                "mint-fee",
                "creator-fee",
                "payer-fee",
                "Fees",
                "FEE",
                "factory-fee",
                "sig-fee-create",
            ),
            buy_event(
                2,
                "mint-fee",
                "buyer-fee-1",
                10,
                100,
                "sig-fee-buy-1",
                250_000,
                25_000,
                false,
                false,
            ),
            buy_event(
                3,
                "mint-fee",
                "buyer-fee-2",
                10,
                90,
                "sig-fee-buy-2",
                250_000,
                35_000,
                false,
                false,
            ),
            holder_event(3, "mint-fee", "buyer-fee-1", 100),
            holder_event(3, "mint-fee", "buyer-fee-2", 90),
        ],
        vec![shred_buy_packet(
            2,
            0,
            "sig-fee-buy-1",
            "mint-fee",
            "buyer-fee-1",
            10,
            100,
            250_000,
            25_000,
        )],
    )
}

fn false_discard_winner(spec: FixtureScenarioSpec) -> FixtureScenario {
    single_token_fixture(
        spec,
        "mint-false",
        vec![
            canonical_create(
                1,
                "mint-false",
                "creator-false",
                "payer-false",
                "False",
                "FLS",
                "factory-false",
                "sig-false-create",
            ),
            buy_event(
                2,
                "mint-false",
                "buyer-false-1",
                10,
                100,
                "sig-false-buy-1",
                180_000,
                800,
                false,
                false,
            ),
            sell_event(
                3,
                "mint-false",
                "buyer-false-1",
                4,
                40,
                "sig-false-sell-1",
                180_000,
                800,
                false,
                true,
            ),
            buy_event(
                6,
                "mint-false",
                "buyer-false-2",
                30,
                120,
                "sig-false-buy-2",
                180_000,
                800,
                false,
                false,
            ),
            buy_event(
                7,
                "mint-false",
                "buyer-false-3",
                35,
                120,
                "sig-false-buy-3",
                180_000,
                800,
                false,
                false,
            ),
            holder_event(7, "mint-false", "buyer-false-2", 120),
            holder_event(7, "mint-false", "buyer-false-3", 120),
            terminal_event(
                3,
                "mint-false",
                TokenTerminalVariant::SoftDiscarded,
                vec![ReasonCode::SoftDiscarded],
            ),
        ],
        vec![shred_sell_packet(
            3,
            0,
            "sig-false-sell-1",
            "mint-false",
            "buyer-false-1",
            4,
            40,
            180_000,
            800,
            true,
        )],
    )
}

fn shred_dev_sell_early_exit(spec: FixtureScenarioSpec) -> FixtureScenario {
    let mut events = strong_entry_sequence(
        "mint-shred-dev",
        "creator-shred-dev",
        "payer-shred-dev",
        "ShredDev",
        "SDV",
        "sig-shred-dev",
    );
    events.insert(
        5,
        tentative_sell_event_priced(
            6,
            "mint-shred-dev",
            "creator-shred-dev",
            4,
            80,
            "sig-shred-dev-sell",
            250_000,
            2_400,
            true,
            true,
            false,
            dec(20, 2),
            dec(5, 2),
        ),
    );
    events.insert(
        6,
        canonical_sell_event_priced(
            7,
            "mint-shred-dev",
            "creator-shred-dev",
            4,
            80,
            "sig-shred-dev-sell",
            250_000,
            2_400,
            true,
            true,
            TransactionStatus::Success,
            dec(20, 2),
            dec(5, 2),
        ),
    );
    timeline_fixture(spec, events)
}

fn shred_top_holder_dump_early_exit(spec: FixtureScenarioSpec) -> FixtureScenario {
    let events = vec![
        canonical_create(
            1,
            "mint-shred-top",
            "creator-shred-top",
            "payer-shred-top",
            "ShredTop",
            "STP",
            "factory-shred-top",
            "sig-shred-top-create",
        ),
        buy_event(
            2,
            "mint-shred-top",
            "whale-shred-top",
            24,
            160,
            "sig-shred-top-buy-1",
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            3,
            "mint-shred-top",
            "buyer-shred-top-2",
            12,
            80,
            "sig-shred-top-buy-2",
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            4,
            "mint-shred-top",
            "buyer-shred-top-3",
            14,
            80,
            "sig-shred-top-buy-3",
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            5,
            "mint-shred-top",
            "buyer-shred-top-4",
            16,
            80,
            "sig-shred-top-buy-4",
            180_000,
            700,
            false,
            false,
        ),
        tentative_sell_event_priced(
            6,
            "mint-shred-top",
            "whale-shred-top",
            5,
            120,
            "sig-shred-top-sell",
            240_000,
            2_000,
            false,
            true,
            false,
            dec(20, 2),
            dec(4, 2),
        ),
        canonical_sell_event_priced(
            7,
            "mint-shred-top",
            "whale-shred-top",
            5,
            120,
            "sig-shred-top-sell",
            240_000,
            2_000,
            false,
            true,
            TransactionStatus::Success,
            dec(20, 2),
            dec(4, 2),
        ),
        holder_event(8, "mint-shred-top", "whale-shred-top", 160),
        holder_event(8, "mint-shred-top", "buyer-shred-top-2", 80),
        holder_event(8, "mint-shred-top", "buyer-shred-top-3", 80),
        holder_event(8, "mint-shred-top", "buyer-shred-top-4", 80),
    ];
    timeline_fixture(spec, events)
}

fn shred_bundle_cluster_exit(spec: FixtureScenarioSpec) -> FixtureScenario {
    let events = vec![
        canonical_create(
            1,
            "mint-shred-bundle",
            "creator-shred-bundle",
            "payer-shred-bundle",
            "ShredBundle",
            "SBD",
            "factory-shred-bundle",
            "sig-shred-bundle-create",
        ),
        funding_event(
            1,
            "bundle-seller-1",
            "shared-funder",
            5,
            "sig-shred-bundle-fund-1",
        ),
        funding_event(
            1,
            "bundle-seller-2",
            "shared-funder",
            5,
            "sig-shred-bundle-fund-2",
        ),
        buy_event(
            2,
            "mint-shred-bundle",
            "bundle-seller-1",
            12,
            80,
            "sig-shred-bundle-buy-1",
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            3,
            "mint-shred-bundle",
            "bundle-seller-2",
            12,
            80,
            "sig-shred-bundle-buy-2",
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            4,
            "mint-shred-bundle",
            "retail-shred-bundle-1",
            14,
            80,
            "sig-shred-bundle-buy-3",
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            5,
            "mint-shred-bundle",
            "retail-shred-bundle-2",
            16,
            80,
            "sig-shred-bundle-buy-4",
            180_000,
            700,
            false,
            false,
        ),
        tentative_sell_event_priced(
            6,
            "mint-shred-bundle",
            "bundle-seller-1",
            4,
            80,
            "sig-shred-bundle-sell-1",
            250_000,
            1_800,
            false,
            true,
            false,
            dec(20, 2),
            dec(10, 2),
        ),
        tentative_sell_event_priced(
            6,
            "mint-shred-bundle",
            "bundle-seller-2",
            4,
            80,
            "sig-shred-bundle-sell-2",
            250_000,
            1_800,
            false,
            true,
            false,
            dec(20, 2),
            dec(9, 2),
        ),
        canonical_sell_event_priced(
            7,
            "mint-shred-bundle",
            "bundle-seller-1",
            4,
            80,
            "sig-shred-bundle-sell-1",
            250_000,
            1_800,
            false,
            true,
            TransactionStatus::Success,
            dec(20, 2),
            dec(10, 2),
        ),
        canonical_sell_event_priced(
            8,
            "mint-shred-bundle",
            "bundle-seller-2",
            4,
            80,
            "sig-shred-bundle-sell-2",
            250_000,
            1_800,
            false,
            true,
            TransactionStatus::Success,
            dec(10, 2),
            dec(6, 2),
        ),
        holder_event(9, "mint-shred-bundle", "bundle-seller-1", 80),
        holder_event(9, "mint-shred-bundle", "bundle-seller-2", 80),
        holder_event(9, "mint-shred-bundle", "retail-shred-bundle-1", 80),
        holder_event(9, "mint-shred-bundle", "retail-shred-bundle-2", 80),
    ];
    timeline_fixture(spec, events)
}

fn shred_false_positive_sell(spec: FixtureScenarioSpec) -> FixtureScenario {
    let mut events = strong_entry_sequence(
        "mint-shred-false",
        "creator-shred-false",
        "payer-shred-false",
        "ShredFalse",
        "SFS",
        "sig-shred-false",
    );
    events.insert(
        5,
        tentative_sell_event_priced(
            6,
            "mint-shred-false",
            "creator-shred-false",
            4,
            80,
            "sig-shred-false-sell",
            240_000,
            2_100,
            true,
            true,
            false,
            dec(20, 2),
            dec(5, 2),
        ),
    );
    events.push(buy_event(
        12,
        "mint-shred-false",
        "buyer-shred-false-rally",
        20,
        80,
        "sig-shred-false-rally",
        180_000,
        700,
        false,
        false,
    ));
    timeline_fixture(spec, events)
}

fn shred_low_confidence_partial(spec: FixtureScenarioSpec) -> FixtureScenario {
    let mut events = strong_entry_sequence(
        "mint-shred-low",
        "creator-shred-low",
        "payer-shred-low",
        "ShredLow",
        "SLW",
        "sig-shred-low",
    );
    events.insert(
        5,
        tentative_sell_event_priced(
            6,
            "mint-shred-low",
            "buyer-shred-low-1",
            2,
            20,
            "sig-shred-low-sell",
            180_000,
            700,
            false,
            false,
            true,
            dec(20, 2),
            dec(18, 2),
        ),
    );
    events.push(buy_event(
        7,
        "mint-shred-low",
        "buyer-shred-low-rally",
        18,
        80,
        "sig-shred-low-rally",
        180_000,
        700,
        false,
        false,
    ));
    timeline_fixture(spec, events)
}

fn shred_sell_absorbed(spec: FixtureScenarioSpec) -> FixtureScenario {
    let events = vec![
        canonical_create(
            1,
            "mint-shred-absorb",
            "creator-shred-absorb",
            "payer-shred-absorb",
            "ShredAbsorb",
            "SAB",
            "factory-shred-absorb",
            "sig-shred-absorb-create",
        ),
        buy_event(
            2,
            "mint-shred-absorb",
            "buyer-shred-absorb-1",
            10,
            80,
            "sig-shred-absorb-buy-1",
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            3,
            "mint-shred-absorb",
            "buyer-shred-absorb-2",
            12,
            80,
            "sig-shred-absorb-buy-2",
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            4,
            "mint-shred-absorb",
            "buyer-shred-absorb-3",
            14,
            80,
            "sig-shred-absorb-buy-3",
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            5,
            "mint-shred-absorb",
            "buyer-shred-absorb-4",
            16,
            80,
            "sig-shred-absorb-buy-4",
            180_000,
            700,
            false,
            false,
        ),
        canonical_sell_event_priced(
            6,
            "mint-shred-absorb",
            "buyer-shred-absorb-1",
            10,
            80,
            "sig-shred-absorb-prior-sell",
            200_000,
            900,
            false,
            true,
            TransactionStatus::Success,
            dec(20, 2),
            dec(17, 2),
        ),
        buy_event(
            7,
            "mint-shred-absorb",
            "buyer-shred-absorb-r1",
            18,
            80,
            "sig-shred-absorb-buy-5",
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            8,
            "mint-shred-absorb",
            "buyer-shred-absorb-r2",
            20,
            80,
            "sig-shred-absorb-buy-6",
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            9,
            "mint-shred-absorb",
            "buyer-shred-absorb-r3",
            22,
            80,
            "sig-shred-absorb-buy-7",
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            10,
            "mint-shred-absorb",
            "buyer-shred-absorb-r4",
            24,
            80,
            "sig-shred-absorb-buy-8",
            180_000,
            700,
            false,
            false,
        ),
        tentative_sell_event_priced(
            11,
            "mint-shred-absorb",
            "buyer-shred-absorb-1",
            12,
            80,
            "sig-shred-absorb-sell",
            200_000,
            1_000,
            false,
            true,
            false,
            dec(20, 2),
            dec(17, 2),
        ),
        canonical_sell_event_priced(
            12,
            "mint-shred-absorb",
            "buyer-shred-absorb-1",
            12,
            80,
            "sig-shred-absorb-sell",
            200_000,
            1_000,
            false,
            true,
            TransactionStatus::Success,
            dec(20, 2),
            dec(17, 2),
        ),
    ];
    timeline_fixture(spec, events)
}

fn shred_exit_too_late(spec: FixtureScenarioSpec) -> FixtureScenario {
    let mut events = strong_entry_sequence(
        "mint-shred-late",
        "creator-shred-late",
        "payer-shred-late",
        "ShredLate",
        "SLT",
        "sig-shred-late",
    );
    let mut tentative = tentative_sell_event_priced(
        6,
        "mint-shred-late",
        "creator-shred-late",
        6,
        80,
        "sig-shred-late-sell",
        240_000,
        1_800,
        true,
        true,
        false,
        dec(8, 2),
        dec(5, 2),
    );
    tentative.meta.source_latency_ms = Some(40);
    events.insert(5, tentative);
    events.insert(
        6,
        canonical_sell_event_priced(
            7,
            "mint-shred-late",
            "creator-shred-late",
            5,
            80,
            "sig-shred-late-sell",
            240_000,
            1_800,
            true,
            true,
            TransactionStatus::Success,
            dec(8, 2),
            dec(4, 2),
        ),
    );
    timeline_fixture(spec, events)
}

fn shred_geyser_disagreement(spec: FixtureScenarioSpec) -> FixtureScenario {
    let mut events = strong_entry_sequence(
        "mint-shred-mismatch",
        "creator-shred-mismatch",
        "payer-shred-mismatch",
        "ShredMismatch",
        "SMI",
        "sig-shred-mismatch",
    );
    events.insert(
        5,
        tentative_sell_event_priced(
            6,
            "mint-shred-mismatch",
            "buyer-shred-mismatch-1",
            4,
            80,
            "sig-shred-mismatch-sell-tentative",
            200_000,
            1_400,
            false,
            true,
            false,
            dec(20, 2),
            dec(5, 2),
        ),
    );
    events.insert(
        6,
        canonical_sell_event_priced(
            7,
            "mint-shred-mismatch",
            "buyer-shred-mismatch-1",
            3,
            60,
            "sig-shred-mismatch-sell-canonical",
            200_000,
            1_400,
            false,
            true,
            TransactionStatus::Success,
            dec(20, 2),
            dec(6, 2),
        ),
    );
    timeline_fixture(spec, events)
}

fn shred_account_effect_confirmation(spec: FixtureScenarioSpec) -> FixtureScenario {
    let mut events = strong_entry_sequence(
        "mint-shred-account",
        "creator-shred-account",
        "payer-shred-account",
        "ShredAcct",
        "SAC",
        "sig-shred-account",
    );
    events.insert(
        5,
        tentative_sell_event_priced(
            6,
            "mint-shred-account",
            "buyer-shred-account-1",
            4,
            80,
            "sig-shred-account-sell",
            210_000,
            1_500,
            false,
            true,
            false,
            dec(20, 2),
            dec(5, 2),
        ),
    );
    events.insert(
        6,
        holder_delta_event(
            7,
            "mint-shred-account",
            "buyer-shred-account-1",
            80,
            0,
            Some("sig-shred-account-sell"),
            Some(1),
        ),
    );
    timeline_fixture(spec, events)
}

fn shred_reorged_sell(spec: FixtureScenarioSpec) -> FixtureScenario {
    let mut events = strong_entry_sequence(
        "mint-shred-reorg",
        "creator-shred-reorg",
        "payer-shred-reorg",
        "ShredReorg",
        "SRE",
        "sig-shred-reorg",
    );
    events.insert(
        5,
        tentative_sell_event_priced(
            6,
            "mint-shred-reorg",
            "creator-shred-reorg",
            4,
            80,
            "sig-shred-reorg-sell",
            240_000,
            1_600,
            true,
            true,
            false,
            dec(20, 2),
            dec(5, 2),
        ),
    );
    events.insert(
        6,
        reverted_sell_event(
            7,
            "mint-shred-reorg",
            "creator-shred-reorg",
            4,
            80,
            "sig-shred-reorg-sell",
            dec(20, 2),
            dec(20, 2),
        ),
    );
    timeline_fixture(spec, events)
}

fn timeline_fixture(
    spec: FixtureScenarioSpec,
    timeline_events: Vec<NormalizedEvent>,
) -> FixtureScenario {
    FixtureScenario {
        spec,
        shred_batches: Vec::new(),
        canonical_events: Vec::new(),
        timeline_events,
    }
}

fn single_token_fixture(
    spec: FixtureScenarioSpec,
    _mint: &str,
    canonical_events: Vec<NormalizedEvent>,
    shred_batches: Vec<DecodedShredBatch>,
) -> FixtureScenario {
    FixtureScenario {
        spec,
        canonical_events,
        shred_batches,
        timeline_events: Vec::new(),
    }
}

fn strong_entry_sequence(
    mint: &str,
    creator: &str,
    payer: &str,
    name: &str,
    symbol: &str,
    signature_prefix: &str,
) -> Vec<NormalizedEvent> {
    vec![
        canonical_create(
            1,
            mint,
            creator,
            payer,
            name,
            symbol,
            &format!("factory-{mint}"),
            &format!("{signature_prefix}-create"),
        ),
        buy_event(
            2,
            mint,
            &format!("buyer-{mint}-1"),
            10,
            80,
            &format!("{signature_prefix}-buy-1"),
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            3,
            mint,
            &format!("buyer-{mint}-2"),
            12,
            80,
            &format!("{signature_prefix}-buy-2"),
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            4,
            mint,
            &format!("buyer-{mint}-3"),
            14,
            80,
            &format!("{signature_prefix}-buy-3"),
            180_000,
            700,
            false,
            false,
        ),
        buy_event(
            5,
            mint,
            &format!("buyer-{mint}-4"),
            16,
            80,
            &format!("{signature_prefix}-buy-4"),
            180_000,
            700,
            false,
            false,
        ),
        holder_event(5, mint, &format!("buyer-{mint}-1"), 80),
        holder_event(5, mint, &format!("buyer-{mint}-2"), 80),
        holder_event(5, mint, &format!("buyer-{mint}-3"), 80),
        holder_event(5, mint, &format!("buyer-{mint}-4"), 80),
    ]
}

fn tentative_sell_event_priced(
    slot: u64,
    mint: &str,
    seller: &str,
    quote: u64,
    tokens: u64,
    signature: &str,
    compute_limit: u32,
    compute_price: u64,
    is_creator: bool,
    is_top_holder: bool,
    partial: bool,
    price_before: Decimal,
    price_after: Decimal,
) -> NormalizedEvent {
    let mut event = sell_event(
        slot,
        mint,
        seller,
        quote,
        tokens,
        signature,
        compute_limit,
        compute_price,
        is_creator,
        is_top_holder,
    );
    event.meta = meta(
        slot,
        Some(signature),
        EventSource::ShredTentative,
        Canonicality::Tentative,
    );
    event.meta.decode_confidence = if partial { dec(45, 2) } else { dec(95, 2) };
    event.meta.observed_at_monotonic_ns = slot * 1_000_000_000;
    event.meta.source_latency_ms = Some(250);
    event.meta.raw_reference = Some(common::RawEventReference {
        source_id: if partial {
            "fixture-partial-shred".to_owned()
        } else {
            "fixture-shred".to_owned()
        },
        cursor: Some(format!("{slot}:0")),
        offset: Some(0),
    });
    if partial {
        event
            .meta
            .data_quality_flags
            .push(common::DataQualityFlag::PartialShred);
    }
    if let EventPayload::PumpSell(payload) = &mut event.payload {
        payload.price_before = Some(price_before);
        payload.price_after = Some(price_after);
    }
    event
}

fn canonical_sell_event_priced(
    slot: u64,
    mint: &str,
    seller: &str,
    quote: u64,
    tokens: u64,
    signature: &str,
    compute_limit: u32,
    compute_price: u64,
    is_creator: bool,
    is_top_holder: bool,
    status: TransactionStatus,
    price_before: Decimal,
    price_after: Decimal,
) -> NormalizedEvent {
    let mut event = sell_event(
        slot,
        mint,
        seller,
        quote,
        tokens,
        signature,
        compute_limit,
        compute_price,
        is_creator,
        is_top_holder,
    );
    if let EventPayload::PumpSell(payload) = &mut event.payload {
        payload.status = status;
        payload.price_before = Some(price_before);
        payload.price_after = Some(price_after);
    }
    event
}

fn holder_delta_event(
    slot: u64,
    mint: &str,
    owner: &str,
    old_balance: u64,
    new_balance: u64,
    signature: Option<&str>,
    write_version: Option<u64>,
) -> NormalizedEvent {
    let mut event = NormalizedEvent {
        meta: meta(
            slot,
            signature,
            EventSource::GeyserProcessed,
            Canonicality::Processed,
        ),
        payload: EventPayload::HolderBalanceUpdate(HolderBalanceUpdateEvent {
            mint: pubkey(mint),
            owner_wallet: pubkey(owner),
            token_account: pubkey(&format!("ata-{mint}-{owner}")),
            token_decimals: Some(DEFAULT_PUMP_TOKEN_DECIMALS),
            old_balance: Some(Decimal::from(old_balance)),
            new_balance: Decimal::from(new_balance),
            delta: Decimal::from(new_balance) - Decimal::from(old_balance),
            caused_by_signature: signature.map(ToOwned::to_owned),
            update_reason: "fixture_account_effect".to_owned(),
            confidence: Decimal::ONE,
        }),
    };
    event.meta.account_write_version = write_version;
    event
}

fn reverted_sell_event(
    slot: u64,
    mint: &str,
    seller: &str,
    quote: u64,
    tokens: u64,
    signature: &str,
    price_before: Decimal,
    price_after: Decimal,
) -> NormalizedEvent {
    let mut event = canonical_sell_event_priced(
        slot,
        mint,
        seller,
        quote,
        tokens,
        signature,
        200_000,
        1_200,
        false,
        true,
        TransactionStatus::Success,
        price_before,
        price_after,
    );
    event.meta.canonicality = Canonicality::Reverted;
    event
}

fn pubkey(value: &str) -> PubkeyValue {
    PubkeyValue(value.to_owned())
}

fn meta(
    slot: u64,
    signature: Option<&str>,
    source: EventSource,
    canonicality: Canonicality,
) -> EventMeta {
    let mut meta = EventMeta::new(source, canonicality, slot);
    meta.signature = signature.map(ToOwned::to_owned);
    meta.received_at_wall_time = OffsetDateTime::UNIX_EPOCH + Duration::seconds(slot as i64);
    meta
}

fn canonical_create(
    slot: u64,
    mint: &str,
    creator: &str,
    payer: &str,
    name: &str,
    symbol: &str,
    fingerprint: &str,
    signature: &str,
) -> NormalizedEvent {
    NormalizedEvent {
        meta: meta(
            slot,
            Some(signature),
            EventSource::GeyserProcessed,
            Canonicality::Processed,
        ),
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
            initial_virtual_quote_reserves: Some(Decimal::from(50u64)),
            initial_virtual_token_reserves: Some(Decimal::from(1_000u64)),
            initial_real_quote_reserves: Some(Decimal::from(50u64)),
            initial_real_token_reserves: Some(Decimal::from(1_000u64)),
            initial_supply: Some(Decimal::from(1_000u64)),
            creator_initial_buy: None,
            same_transaction_buys: 0,
            same_slot_buys: 0,
            fee_recipients: vec![],
            raw_account_list: vec![],
            launch_transaction_fingerprint: Some(fingerprint.to_owned()),
        }),
    }
}

fn canonical_observed_tx(
    slot: u64,
    signature: &str,
    account_count: usize,
    instruction_count: usize,
    program_ids: Vec<String>,
) -> NormalizedEvent {
    NormalizedEvent {
        meta: meta(
            slot,
            Some(signature),
            EventSource::GeyserProcessed,
            Canonicality::Processed,
        ),
        payload: EventPayload::ObservedTransaction(ObservedTransactionEvent {
            signature_hint: Some(signature.to_owned()),
            slot_hint: Some(slot),
            entry_index: Some(0),
            tx_position_estimate: Some(0),
            signer: None,
            program_ids,
            account_count,
            instruction_count,
            account_list_hash: None,
            instruction_shape_hash: None,
            compute_unit_limit: None,
            compute_unit_price: None,
            estimated_priority_fee_lamports: None,
            tx_fee_lamports: None,
            compute_units_consumed: None,
            failed_transaction: false,
            error_code: None,
            bundle_like_evidence: None,
            raw_packet_hash: format!("packet-{signature}"),
            first_seen_by_shred_ns: slot * 1_000_000,
            decode_confidence: dec(90, 2),
        }),
    }
}

fn buy_event(
    slot: u64,
    mint: &str,
    buyer: &str,
    quote: u64,
    tokens: u64,
    signature: &str,
    compute_limit: u32,
    compute_price: u64,
    is_creator: bool,
    is_first_buy: bool,
) -> NormalizedEvent {
    NormalizedEvent {
        meta: meta(
            slot,
            Some(signature),
            EventSource::GeyserProcessed,
            Canonicality::Processed,
        ),
        payload: EventPayload::PumpBuy(PumpBuyEvent {
            mint: pubkey(mint),
            buyer: pubkey(buyer),
            payer: pubkey(buyer),
            quote_in: Decimal::from(quote),
            token_out: Decimal::from(tokens),
            price_before: None,
            price_after: None,
            effective_price: Decimal::from(quote) / Decimal::from(tokens),
            slippage_estimate: Some(dec(5, 2)),
            reserves_before: None,
            reserves_after: None,
            max_quote_cost: Some(Decimal::from(quote + 2)),
            compute_unit_limit: Some(compute_limit),
            compute_unit_price: Some(compute_price),
            estimated_priority_fee_lamports: Some(common::Lamports(
                (compute_limit as u64 * compute_price) / 1_000_000,
            )),
            estimated_base_fee_lamports: Some(common::Lamports(5_000)),
            estimated_tip_lamports: None,
            is_creator,
            is_known_cluster_member: false,
            is_first_buy,
            status: TransactionStatus::Success,
        }),
    }
}

fn failed_buy_event(
    slot: u64,
    mint: &str,
    buyer: &str,
    quote: u64,
    tokens: u64,
    signature: &str,
) -> NormalizedEvent {
    let mut event = buy_event(
        slot, mint, buyer, quote, tokens, signature, 180_000, 800, false, false,
    );
    if let EventPayload::PumpBuy(payload) = &mut event.payload {
        payload.status = TransactionStatus::Failed;
    }
    event
}

fn sell_event(
    slot: u64,
    mint: &str,
    seller: &str,
    quote: u64,
    tokens: u64,
    signature: &str,
    compute_limit: u32,
    compute_price: u64,
    is_creator: bool,
    is_top_holder: bool,
) -> NormalizedEvent {
    NormalizedEvent {
        meta: meta(
            slot,
            Some(signature),
            EventSource::GeyserProcessed,
            Canonicality::Processed,
        ),
        payload: EventPayload::PumpSell(PumpSellEvent {
            mint: pubkey(mint),
            seller: pubkey(seller),
            quote_out: Decimal::from(quote),
            token_in: Decimal::from(tokens),
            price_before: None,
            price_after: None,
            effective_price: Decimal::from(quote) / Decimal::from(tokens),
            slippage_estimate: Some(dec(10, 2)),
            reserves_before: None,
            reserves_after: None,
            min_quote_output: Some(Decimal::ONE),
            compute_unit_limit: Some(compute_limit),
            compute_unit_price: Some(compute_price),
            estimated_priority_fee_lamports: Some(common::Lamports(
                (compute_limit as u64 * compute_price) / 1_000_000,
            )),
            estimated_base_fee_lamports: Some(common::Lamports(5_000)),
            estimated_tip_lamports: None,
            is_creator,
            is_top_holder_pre_sell: is_top_holder,
            is_known_cluster_member: false,
            status: TransactionStatus::Success,
        }),
    }
}

fn holder_event(slot: u64, mint: &str, owner: &str, balance: u64) -> NormalizedEvent {
    NormalizedEvent {
        meta: meta(
            slot,
            None,
            EventSource::GeyserProcessed,
            Canonicality::Processed,
        ),
        payload: EventPayload::HolderBalanceUpdate(HolderBalanceUpdateEvent {
            mint: pubkey(mint),
            owner_wallet: pubkey(owner),
            token_account: pubkey(&format!("ata-{mint}-{owner}")),
            token_decimals: Some(DEFAULT_PUMP_TOKEN_DECIMALS),
            old_balance: None,
            new_balance: Decimal::from(balance),
            delta: Decimal::from(balance),
            caused_by_signature: None,
            update_reason: "fixture_balance".to_owned(),
            confidence: Decimal::ONE,
        }),
    }
}

fn funding_event(
    slot: u64,
    wallet: &str,
    funder: &str,
    amount: u64,
    signature: &str,
) -> NormalizedEvent {
    NormalizedEvent {
        meta: meta(
            slot,
            Some(signature),
            EventSource::GeyserProcessed,
            Canonicality::Processed,
        ),
        payload: EventPayload::WalletFunding(WalletFundingEvent {
            wallet: pubkey(wallet),
            funder: pubkey(funder),
            asset_label: "SOL".to_owned(),
            amount: Decimal::from(amount),
            slot,
            signature: signature.to_owned(),
            relation_to_launch: Some("before_launch".to_owned()),
            near_launch_relation: true,
            funding_graph_edge_id: format!("edge-{signature}"),
        }),
    }
}

fn gap_event(slot: u64, mint: Option<&str>) -> NormalizedEvent {
    NormalizedEvent {
        meta: meta(
            slot,
            None,
            EventSource::GeyserProcessed,
            Canonicality::Processed,
        ),
        payload: EventPayload::DataGap(DataGapEvent {
            gap_type: DataGapType::SlotGap,
            source: EventSource::GeyserProcessed,
            start_slot: slot,
            end_slot: Some(slot),
            affected_tokens: mint.map(pubkey).into_iter().collect(),
            severity: GapSeverity::High,
            trade_allowed: false,
            recovery_action: "pause".to_owned(),
        }),
    }
}

fn terminal_event(
    slot: u64,
    mint: &str,
    variant: TokenTerminalVariant,
    reason_codes: Vec<ReasonCode>,
) -> NormalizedEvent {
    NormalizedEvent {
        meta: meta(
            slot,
            None,
            EventSource::GeyserProcessed,
            Canonicality::Processed,
        ),
        payload: EventPayload::TokenTerminal(TokenTerminalEvent {
            mint: pubkey(mint),
            variant,
            reason_codes,
            details: BTreeMap::new(),
        }),
    }
}

fn shred_create_packet(
    slot: u64,
    entry_index: u32,
    signature: &str,
    mint: &str,
    creator: &str,
    payer: &str,
    name: &str,
    symbol: &str,
    fingerprint: &str,
) -> DecodedShredBatch {
    DecodedShredBatch {
        slot: Some(slot),
        entry_index: Some(entry_index),
        partial: false,
        transactions: vec![DecodedShredTransaction {
            signature: Some(signature.to_owned()),
            tx_position_estimate: Some(0),
            decode_confidence: dec(95, 2),
            payload: DecodedShredPayload::TokenCreated {
                event: TokenCreatedEvent {
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
                    initial_virtual_quote_reserves: Some(Decimal::from(50u64)),
                    initial_virtual_token_reserves: Some(Decimal::from(1_000u64)),
                    initial_real_quote_reserves: Some(Decimal::from(50u64)),
                    initial_real_token_reserves: Some(Decimal::from(1_000u64)),
                    initial_supply: Some(Decimal::from(1_000u64)),
                    creator_initial_buy: None,
                    same_transaction_buys: 0,
                    same_slot_buys: 0,
                    fee_recipients: vec![],
                    raw_account_list: vec![],
                    launch_transaction_fingerprint: Some(fingerprint.to_owned()),
                },
            },
        }],
    }
}

fn shred_buy_packet(
    slot: u64,
    entry_index: u32,
    signature: &str,
    mint: &str,
    buyer: &str,
    quote: u64,
    tokens: u64,
    compute_limit: u32,
    compute_price: u64,
) -> DecodedShredBatch {
    DecodedShredBatch {
        slot: Some(slot),
        entry_index: Some(entry_index),
        partial: false,
        transactions: vec![DecodedShredTransaction {
            signature: Some(signature.to_owned()),
            tx_position_estimate: Some(0),
            decode_confidence: dec(95, 2),
            payload: DecodedShredPayload::PumpBuy {
                event: match buy_event(
                    slot,
                    mint,
                    buyer,
                    quote,
                    tokens,
                    signature,
                    compute_limit,
                    compute_price,
                    false,
                    false,
                )
                .payload
                {
                    EventPayload::PumpBuy(event) => event,
                    _ => unreachable!(),
                },
            },
        }],
    }
}

fn shred_sell_packet(
    slot: u64,
    entry_index: u32,
    signature: &str,
    mint: &str,
    seller: &str,
    quote: u64,
    tokens: u64,
    compute_limit: u32,
    compute_price: u64,
    top_holder: bool,
) -> DecodedShredBatch {
    DecodedShredBatch {
        slot: Some(slot),
        entry_index: Some(entry_index),
        partial: false,
        transactions: vec![DecodedShredTransaction {
            signature: Some(signature.to_owned()),
            tx_position_estimate: Some(0),
            decode_confidence: dec(95, 2),
            payload: DecodedShredPayload::PumpSell {
                event: match sell_event(
                    slot,
                    mint,
                    seller,
                    quote,
                    tokens,
                    signature,
                    compute_limit,
                    compute_price,
                    false,
                    top_holder,
                )
                .payload
                {
                    EventPayload::PumpSell(event) => event,
                    _ => unreachable!(),
                },
            },
        }],
    }
}
