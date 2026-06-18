from __future__ import annotations

from .readiness import gate_blockers
from .schemas import GateResult


def backtest_gate(readiness: dict, *, strategy: str = "") -> GateResult:
    if strategy.startswith("early_burst") and not readiness.get("early_burst_backtesting_ready"):
        return GateResult(
            allowed=False,
            blocker="EARLY_BURST_BACKTEST_BLOCKED_BY_READINESS_GATE",
            reason_codes=sorted(set(gate_blockers(readiness, action="backtest") + list(readiness.get("reason_codes", [])))),
            forbidden_actions=[
                "formal_performance_evaluation",
                "threshold_tuning",
                "profit_metrics",
                "roi",
                "sharpe",
                "win_rate",
                "live_signal_language",
            ],
            details={
                "requires_early_burst_backtesting_ready": True,
                "requires_operator_approval": True,
                "sample_size_checks_required": True,
            },
        )
    if not readiness.get("backtesting_ready"):
        return GateResult(
            allowed=False,
            blocker="BACKTESTING_BLOCKED_BY_READINESS_GATE",
            reason_codes=gate_blockers(readiness, action="backtest"),
            forbidden_actions=["formal_performance_evaluation", "threshold_tuning", "test_split_optimization"],
            details={"architecture_components": ["walk_forward_splits", "cost_model", "slippage_model", "latency_model", "risk_model"]},
        )
    return GateResult(allowed=True, blocker="", details={"note": "architecture only; execution requires explicit operator approval"})
