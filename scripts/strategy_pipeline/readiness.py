from __future__ import annotations

from typing import Any

from .io import read_json
from .schemas import PIPELINE_ROOT


BASE_BLOCKED_ACTIONS = [
    "replay",
    "formal_backtesting",
    "threshold_tuning",
    "paper_trading",
    "live_trading",
    "wallet_execution",
    "profitability_claim",
]


def build_readiness_decision(
    *,
    architecture_readiness: dict[str, Any],
    data_mart: dict[str, Any],
    leakage_passed: bool,
    splits_passed: bool,
    registries_passed: bool,
    models_configured: bool,
    positive_outcome_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    clean_positives = int(architecture_readiness.get("clean_positive_count", 0))
    replay_eligible = int(architecture_readiness.get("replay_eligible_candidate_count", 0))
    strategy_ready = bool(architecture_readiness.get("strategy_research_ready")) and leakage_passed
    buy_arch_ready = bool(architecture_readiness.get("buy_strategy_architecture_ready")) and registries_passed
    positive_summary = positive_outcome_summary or {}
    positive_or_high = int(positive_summary.get("positive_or_high_count", 0))
    positive_outcome_research_ready = leakage_passed and int(positive_summary.get("total_rows", 0)) > 0
    reason_codes: list[str] = []
    if clean_positives <= 0:
        reason_codes.append("no_clean_positives")
    if replay_eligible <= 0:
        reason_codes.append("no_replay_eligible_candidates")
    if positive_or_high > 0 and replay_eligible <= 0:
        reason_codes.append("positive_outcomes_exist_but_replay_not_allowed")
        reason_codes.append("candidate_replay_labels_missing")
    if not leakage_passed:
        reason_codes.append("leakage_audit_required")
    if not splits_passed:
        reason_codes.append("walk_forward_splits_required")
    if not registries_passed:
        reason_codes.append("strategy_hypotheses_not_locked")
    if not models_configured:
        reason_codes.append("cost_slippage_latency_models_required")
    reason_codes.extend(["threshold_tuning_disabled", "operator_approval_missing"])
    backtesting_ready = (
        strategy_ready
        and buy_arch_ready
        and clean_positives > 0
        and leakage_passed
        and splits_passed
        and registries_passed
        and models_configured
        and False
    )
    return {
        "schema_version": "phase107i.trading_strategy_pipeline_readiness.v1",
        "strategy_research_ready": strategy_ready,
        "buy_strategy_architecture_ready": buy_arch_ready,
        "trading_strategy_pipeline_ready": strategy_ready and buy_arch_ready and leakage_passed and splits_passed and registries_passed and models_configured,
        "positive_outcome_research_ready": positive_outcome_research_ready,
        "backtesting_ready": backtesting_ready,
        "replay_ready": False,
        "threshold_tuning_ready": False,
        "paper_trading_ready": False,
        "live_trading_ready": False,
        "wallet_execution_ready": False,
        "profitability_claim_allowed": False,
        "clean_positive_count": clean_positives,
        "replay_eligible_candidate_count": replay_eligible,
        "positive_outcome_count": int(positive_summary.get("positive_count", 0)),
        "high_positive_outcome_count": int(positive_summary.get("high_positive_count", 0)),
        "positive_or_high_outcome_count": positive_or_high,
        "data_mart_rows": data_mart.get("mint_rows", 0),
        "reason_codes": reason_codes,
        "blocked_actions": BASE_BLOCKED_ACTIONS,
        "launch_caps_blocked": True,
    }


def load_readiness(root=PIPELINE_ROOT) -> dict[str, Any]:
    return read_json(root / "READINESS_DECISION.json")


def gate_blockers(readiness: dict[str, Any], *, action: str) -> list[str]:
    blockers = list(readiness.get("reason_codes", []))
    if action == "backtest" and not readiness.get("backtesting_ready"):
        blockers.extend(["BACKTESTING_BLOCKED_BY_READINESS_GATE", "operator_approval_missing"])
    if action == "replay" and not readiness.get("replay_ready"):
        blockers.extend(["REPLAY_BLOCKED_NO_REPLAY_ELIGIBLE_CANDIDATES", "countability_replay_not_allowed"])
    if action == "threshold_tuning" and not readiness.get("threshold_tuning_ready"):
        blockers.extend(["THRESHOLD_TUNING_BLOCKED_BY_READINESS_GATE", "backtesting_not_passed"])
    if action == "paper_trading" and not readiness.get("paper_trading_ready"):
        blockers.extend(["PAPER_TRADING_BLOCKED_BY_READINESS_GATE", "backtesting_not_passed"])
    if action == "live_trading" and not readiness.get("live_trading_ready"):
        blockers.extend(["LIVE_TRADING_DISABLED", "WALLET_EXECUTION_DISABLED", "OPERATOR_APPROVAL_REQUIRED"])
    if action == "profitability_claim" and not readiness.get("profitability_claim_allowed"):
        blockers.extend(["PROFITABILITY_CLAIM_BLOCKED", "out_of_sample_backtest_missing", "paper_trading_not_passed"])
    return sorted(set(blockers))
