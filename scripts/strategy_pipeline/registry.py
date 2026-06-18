from __future__ import annotations

from typing import Any


def strategy_registry() -> dict[str, Any]:
    entries = [
        ("early_avoid_v1", "research_only"),
        ("continue_tracking_v1", "research_only"),
        ("candidate_eligibility_v2", "research_only"),
        ("early_burst_setup_v0", "research_only_disabled"),
        ("buy_setup_draft_v0", "research_only_disabled"),
        ("risk_exit_draft_v0", "research_only_disabled"),
    ]
    return {
        "schema_version": "phase107i.strategy_registry.v1",
        "strategies": [
            {
                "name": name,
                "version": name.rsplit("_", 1)[-1],
                "execution_mode": mode,
                "enabled": name.startswith(("early_avoid", "continue_tracking", "candidate_eligibility")),
                "allow_replay": False,
                "allow_backtest": False,
                "allow_threshold_tuning": False,
                "allow_paper_trade": False,
                "allow_live_trade": False,
                "wallet_execution": False,
            }
            for name, mode in entries
        ],
    }


def config_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Research-only strategy config",
        "type": "object",
        "required": ["name", "execution_mode", "allow_live_trade", "wallet_execution"],
        "properties": {
            "name": {"type": "string"},
            "execution_mode": {"enum": ["research_only", "research_only_disabled"]},
            "allow_replay": {"const": False},
            "allow_backtest": {"const": False},
            "allow_threshold_tuning": {"const": False},
            "allow_paper_trade": {"const": False},
            "allow_live_trade": {"const": False},
            "wallet_execution": {"const": False},
        },
    }


def validate_registry(registry: dict[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    for entry in registry.get("strategies", []):
        if entry.get("allow_live_trade") or entry.get("wallet_execution"):
            blockers.append(f"execution_enabled:{entry.get('name')}")
        if entry.get("allow_backtest") or entry.get("allow_threshold_tuning"):
            blockers.append(f"forbidden_eval_enabled:{entry.get('name')}")
    return {"passed": not blockers, "blockers": blockers, "strategy_count": len(registry.get("strategies", []))}
