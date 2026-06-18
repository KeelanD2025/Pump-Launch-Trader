from __future__ import annotations

from typing import Any

from .buy_setup import BuySetupDraft
from .gates import CandidateEligibilityGateV2, ContinueTrackingGateV1, EarlyAvoidFilterV1
from .risk_exit import RiskAndExitDraft


def list_strategies() -> list[dict[str, Any]]:
    return [
        {"name": "early_avoid_v1", "version": "v1", "mode": "research_only", "execution_enabled": False},
        {"name": "continue_tracking_v1", "version": "v1", "mode": "research_only", "execution_enabled": False},
        {"name": "candidate_eligibility_v2", "version": "v2", "mode": "research_only", "execution_enabled": False},
        {"name": "buy_setup_draft_v0", "version": "v0", "mode": "disabled", "execution_enabled": False},
        {"name": "risk_exit_draft_v0", "version": "v0", "mode": "disabled", "execution_enabled": False},
    ]


def load_strategy_config(name: str) -> dict[str, Any]:
    for entry in list_strategies():
        if entry["name"] == name:
            return entry | {"threshold_tuning_enabled": False, "live_trading_enabled": False}
    raise KeyError(name)


def validate_strategy_config(config: dict[str, Any]) -> dict[str, Any]:
    blockers = []
    if config.get("execution_enabled"):
        blockers.append("execution_enabled_forbidden")
    if config.get("threshold_tuning_enabled"):
        blockers.append("threshold_tuning_enabled_forbidden")
    if config.get("live_trading_enabled"):
        blockers.append("live_trading_enabled_forbidden")
    return {"passed": not blockers, "blockers": blockers}


def run_strategy_research_mode(name: str, row: dict[str, Any], label: dict[str, Any] | None = None) -> Any:
    if name == "early_avoid_v1":
        return EarlyAvoidFilterV1().score(row)
    if name == "continue_tracking_v1":
        return ContinueTrackingGateV1().score(row)
    if name == "candidate_eligibility_v2":
        return CandidateEligibilityGateV2().score(row, label)
    if name == "buy_setup_draft_v0":
        return BuySetupDraft().describe()
    if name == "risk_exit_draft_v0":
        return RiskAndExitDraft().describe()
    raise KeyError(name)


def block_strategy_execution_mode(name: str) -> dict[str, Any]:
    return {
        "strategy_name": name,
        "execution_allowed": False,
        "blocker": "STRATEGY_EXECUTION_DISABLED_RESEARCH_ONLY",
    }

