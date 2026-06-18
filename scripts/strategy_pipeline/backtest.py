from __future__ import annotations

from .readiness import gate_blockers
from .schemas import GateResult


def backtest_gate(readiness: dict) -> GateResult:
    if not readiness.get("backtesting_ready"):
        return GateResult(
            allowed=False,
            blocker="BACKTESTING_BLOCKED_BY_READINESS_GATE",
            reason_codes=gate_blockers(readiness, action="backtest"),
            forbidden_actions=["formal_performance_evaluation", "threshold_tuning", "test_split_optimization"],
            details={"architecture_components": ["walk_forward_splits", "cost_model", "slippage_model", "latency_model", "risk_model"]},
        )
    return GateResult(allowed=True, blocker="", details={"note": "architecture only; execution requires explicit operator approval"})
