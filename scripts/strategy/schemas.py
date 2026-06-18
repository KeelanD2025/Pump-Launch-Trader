from __future__ import annotations

import hashlib
import json
import pathlib
from dataclasses import asdict, dataclass, field
from typing import Any


ARCHITECTURE_SCHEMA_VERSION = "phase107h.buy_strategy_architecture.v1"
REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
STRATEGY_READINESS_ROOT = REPO_ROOT / "research_output" / "strategy_readiness"
STRATEGY_ARCHITECTURE_ROOT = REPO_ROOT / "research_output" / "strategy_architecture"
HORIZONS = (5, 10, 30, 60, 120, 300, 900)

ALLOWED_SIGNAL_DECISIONS = {
    "avoid",
    "continue_tracking",
    "candidate_watch",
    "candidate_review",
    "audit_only",
    "censored",
    "insufficient_data",
    "no_action",
}
FORBIDDEN_SIGNAL_DECISIONS = {
    "buy",
    "sell",
    "enter_position",
    "exit_position",
    "size_position",
    "submit_order",
}
FORBIDDEN_ALPHA_COLUMNS = {
    "final_outcome",
    "final_outcome_reason",
    "rejection_reason",
    "terminal_inconclusive_reason",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "off_vps_candidate_replay_allowed",
    "ready_for_off_vps_candidate_replay",
    "r2_verified",
    "artifact_consistency_ok",
}
DATA_QUALITY_PREFIX = "data_quality_"


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def intish(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def floatish(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def stable_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


@dataclass
class SignalOutput:
    strategy_name: str
    strategy_version: str
    mint: str
    horizon_seconds: int
    decision: str
    score_optional: str = ""
    confidence_bin: str = "MISSING"
    reason_codes: list[str] = field(default_factory=list)
    feature_snapshot_hash: str = ""
    data_quality_status: str = "unknown"
    allowed_actions: list[str] = field(default_factory=lambda: ["research_report"])
    blocked_actions: list[str] = field(
        default_factory=lambda: [
            "replay",
            "backtesting",
            "threshold_tuning",
            "paper_trading",
            "live_trading",
            "wallet_execution",
        ]
    )
    explanation: str = ""

    def to_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["reason_codes"] = "|".join(self.reason_codes)
        row["allowed_actions"] = "|".join(self.allowed_actions)
        row["blocked_actions"] = "|".join(self.blocked_actions)
        return row

    def validate(self) -> None:
        if self.decision in FORBIDDEN_SIGNAL_DECISIONS:
            raise ValueError(f"forbidden signal decision emitted: {self.decision}")
        if self.decision not in ALLOWED_SIGNAL_DECISIONS:
            raise ValueError(f"unknown signal decision emitted: {self.decision}")
        if any(action in FORBIDDEN_SIGNAL_DECISIONS for action in self.allowed_actions):
            raise ValueError("trade action leaked into allowed_actions")


SIGNAL_FIELDS = [
    "strategy_name",
    "strategy_version",
    "mint",
    "horizon_seconds",
    "decision",
    "score_optional",
    "confidence_bin",
    "reason_codes",
    "feature_snapshot_hash",
    "data_quality_status",
    "allowed_actions",
    "blocked_actions",
    "explanation",
]

