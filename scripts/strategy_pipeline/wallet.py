from __future__ import annotations

from .schemas import GateResult


def wallet_gate() -> GateResult:
    return GateResult(
        allowed=False,
        blocker="WALLET_EXECUTION_DISABLED",
        reason_codes=["private_key_loading_disabled", "real_order_submission_disabled", "operator_approval_missing"],
        forbidden_actions=["load_private_key", "sign_transaction", "send_transaction"],
        details={"config_cannot_enable_wallet_execution": True},
    )
