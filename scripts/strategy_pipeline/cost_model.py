from __future__ import annotations


def default_cost_model() -> dict[str, object]:
    return {
        "schema_version": "phase107i.cost_model.v1",
        "fee_model": "configured_placeholder",
        "priority_fee_model": "configured_placeholder",
        "requires_validation_before_backtest": True,
        "used_for_live_orders": False,
    }
