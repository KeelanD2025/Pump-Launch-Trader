from __future__ import annotations


def default_latency_model() -> dict[str, object]:
    return {
        "schema_version": "phase107i.latency_model.v1",
        "model": "event_latency_placeholder",
        "requires_measurement_before_backtest": True,
        "used_for_live_orders": False,
    }
