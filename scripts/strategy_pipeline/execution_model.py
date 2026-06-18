from __future__ import annotations


def execution_model() -> dict[str, object]:
    return {
        "schema_version": "phase107i.execution_model.v1",
        "order_submission_enabled": False,
        "wallet_execution_enabled": False,
        "private_key_loading_enabled": False,
        "allowed_modes": ["research_only", "dry_run_blocked"],
        "forbidden_actions": ["submit_order", "load_private_key", "send_transaction", "enable_live_trading"],
    }
