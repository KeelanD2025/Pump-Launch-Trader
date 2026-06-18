from __future__ import annotations

from .readiness import gate_blockers
from .schemas import GateResult


def threshold_tuning_gate(readiness: dict) -> GateResult:
    return GateResult(
        allowed=False,
        blocker="THRESHOLD_TUNING_BLOCKED_BY_READINESS_GATE",
        reason_codes=gate_blockers(readiness, action="threshold_tuning"),
        forbidden_actions=["optimize_thresholds", "use_test_split_for_tuning"],
        details={"requires_backtesting_ready": True, "requires_locked_splits": True, "operator_approval_required": True},
    )
