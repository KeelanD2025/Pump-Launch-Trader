from __future__ import annotations

from typing import Any


def readiness_decision(*, label_summary: dict[str, Any], leakage_passed: bool, modules_exist: bool) -> dict[str, Any]:
    clean_positives = int(label_summary.get("clean_positives", 0))
    replay_eligible = int(label_summary.get("replay_eligible", 0))
    clean_negatives = int(label_summary.get("clean_negatives", 0))
    strategy_research_ready = leakage_passed and clean_negatives > 0
    buy_strategy_architecture_ready = strategy_research_ready and modules_exist
    reason_codes: list[str] = []
    if clean_positives == 0:
        reason_codes.append("no_clean_positives")
    if replay_eligible == 0:
        reason_codes.append("no_replay_eligible_candidates")
    reason_codes.extend(["threshold_tuning_disabled", "operator_approval_missing"])
    return {
        "schema_version": "phase107h.buy_strategy_architecture_readiness.v1",
        "strategy_research_ready": strategy_research_ready,
        "buy_strategy_architecture_ready": buy_strategy_architecture_ready,
        "backtesting_ready": False if reason_codes else True,
        "replay_ready": False,
        "threshold_tuning_ready": False,
        "paper_trading_ready": False,
        "live_trading_ready": False,
        "wallet_execution_ready": False,
        "clean_negative_count": clean_negatives,
        "clean_positive_count": clean_positives,
        "replay_eligible_candidate_count": replay_eligible,
        "reason_codes": reason_codes,
    }

