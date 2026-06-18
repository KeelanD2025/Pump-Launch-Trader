from __future__ import annotations

from .readiness import gate_blockers
from .schemas import GateResult


def paper_trading_gate(readiness: dict) -> GateResult:
    return GateResult(
        allowed=False,
        blocker="PAPER_TRADING_BLOCKED_BY_READINESS_GATE",
        reason_codes=gate_blockers(readiness, action="paper_trading"),
        forbidden_actions=["simulated_order_loop", "position_ledger_updates", "paper_wallet"],
        details={"dry_run_order_model": "architected_but_disabled", "real_private_keys": False, "real_rpc_order_submission": False},
    )
