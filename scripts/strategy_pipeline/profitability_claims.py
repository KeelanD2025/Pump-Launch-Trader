from __future__ import annotations

from .readiness import gate_blockers
from .schemas import GateResult, PROFITABILITY_FORBIDDEN_WORDS


def profitability_claim_gate(readiness: dict) -> GateResult:
    return GateResult(
        allowed=False,
        blocker="PROFITABILITY_CLAIM_BLOCKED",
        reason_codes=gate_blockers(readiness, action="profitability_claim") + [
            "out_of_sample_backtest_required",
            "walk_forward_stability_required",
            "baseline_comparison_required",
            "sample_size_insufficient",
            "operator_approval_required",
        ],
        forbidden_actions=["claim_profitable", "claim_validated_edge", "claim_live_ready"],
        details={"forbidden_phrases": list(PROFITABILITY_FORBIDDEN_WORDS)},
    )


def report_text_allowed(text: str, gate: GateResult) -> dict[str, object]:
    lower = text.lower()
    blockers = [phrase for phrase in PROFITABILITY_FORBIDDEN_WORDS if phrase in lower]
    if blockers and not gate.allowed:
        return {"passed": False, "blockers": blockers}
    return {"passed": True, "blockers": []}
