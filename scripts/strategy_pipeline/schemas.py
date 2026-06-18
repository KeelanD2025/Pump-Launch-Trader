from __future__ import annotations

import hashlib
import json
import pathlib
from dataclasses import dataclass, field
from typing import Any


PIPELINE_SCHEMA_VERSION = "phase107i.trading_strategy_pipeline.v1"
REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
STRATEGY_ARCHITECTURE_ROOT = REPO_ROOT / "research_output" / "strategy_architecture"
PIPELINE_ROOT = REPO_ROOT / "research_output" / "trading_strategy_pipeline"
DATA_MART_ROOT = PIPELINE_ROOT / "data_mart"
SPLITS_ROOT = PIPELINE_ROOT / "splits"
HORIZONS = (5, 10, 30, 60, 120, 300, 900)

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
    "positive_outcome_label",
    "positive_outcome_strength_bin",
    "positive_outcome_basis",
    "positive_outcome_reason_codes",
    "outcome_label",
    "outcome_label_quality",
    "outcome_basis",
    "outcome_window_seconds",
    "outcome_known_at_end_only",
    "allowed_for_alpha_features",
    "curve_progress_proxy_end",
    "curve_progress_proxy_max",
    "liquidity_delta_forward",
    "reserve_delta_forward",
    "volume_delta_forward",
    "buy_sell_delta_forward",
    "holder_growth_forward",
    "holder_concentration_risk_forward",
    "max_adverse_proxy",
    "max_favorable_proxy",
}

PROFITABILITY_FORBIDDEN_WORDS = (
    "proven profitable",
    "validated profitable",
    "guaranteed edge",
    "buy signal",
    "live-ready",
)


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def stable_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def file_sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass
class GateResult:
    allowed: bool
    blocker: str
    reason_codes: list[str] = field(default_factory=list)
    forbidden_actions: list[str] = field(default_factory=list)
    architecture_ready: bool = True
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "blocker": self.blocker,
            "reason_codes": self.reason_codes,
            "forbidden_actions": self.forbidden_actions,
            "architecture_ready": self.architecture_ready,
            "details": self.details,
        }
