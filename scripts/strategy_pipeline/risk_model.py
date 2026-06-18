from __future__ import annotations


def default_risk_model() -> dict[str, object]:
    return {
        "schema_version": "phase107i.risk_model.v1",
        "max_position_size": "placeholder_requires_approval",
        "max_loss": "placeholder_requires_approval",
        "kill_switch": "required_before_paper_or_live",
        "per_mint_risk_flags": [
            "provider_gap_exposed",
            "terminal_inconclusive",
            "degraded_audit_only",
            "liquidity_exit_risk",
            "holder_concentration_risk",
        ],
        "used_for_live_orders": False,
    }
