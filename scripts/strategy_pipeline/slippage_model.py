from __future__ import annotations


def default_slippage_model() -> dict[str, object]:
    return {
        "schema_version": "phase107i.slippage_model.v1",
        "model": "liquidity_constrained_placeholder",
        "requires_stream_authoritative_liquidity": True,
        "requires_validation_before_backtest": True,
        "used_for_live_orders": False,
    }
