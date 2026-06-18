from __future__ import annotations

from .readiness import gate_blockers
from .schemas import GateResult


def live_trading_gate(readiness: dict) -> GateResult:
    return GateResult(
        allowed=False,
        blocker="LIVE_TRADING_DISABLED",
        reason_codes=gate_blockers(readiness, action="live_trading") + [
            "PAPER_TRADING_NOT_PASSED",
            "BACKTESTING_NOT_PASSED",
            "THRESHOLD_TUNING_NOT_PASSED",
            "RISK_LIMITS_NOT_APPROVED",
        ],
        forbidden_actions=["load_private_key", "submit_order", "send_transaction", "enable_wallet_execution"],
        details={"hard_disabled": True, "config_cannot_enable_live": True},
    )
