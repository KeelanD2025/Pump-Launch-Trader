from __future__ import annotations

from .readiness import gate_blockers
from .schemas import GateResult


def replay_gate(readiness: dict, *, candidate_pack_present: bool = False) -> GateResult:
    if not readiness.get("replay_ready"):
        return GateResult(
            allowed=False,
            blocker="REPLAY_BLOCKED_NO_REPLAY_ELIGIBLE_CANDIDATES",
            reason_codes=gate_blockers(readiness, action="replay"),
            forbidden_actions=["candidate_replay", "historical_replay_execution"],
            details={"candidate_pack_present": candidate_pack_present, "requires_countability_replay_allow": True},
        )
    return GateResult(allowed=True, blocker="", details={"note": "replay gate passed; operator approval still required"})
