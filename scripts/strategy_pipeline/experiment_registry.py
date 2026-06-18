from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .schemas import stable_hash


def default_experiment(readiness: dict[str, Any], data_mart_manifest: dict[str, Any], splits: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "strategy_name": "candidate_eligibility_v2",
        "strategy_version": "v2",
        "data_mart_manifest_hash": stable_hash(data_mart_manifest),
        "split_id": splits.get("split_id", "missing"),
        "hypothesis_id": "research_only_candidate_watch_v0",
    }
    return {
        "experiment_id": stable_hash(payload)[:16],
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        **payload,
        "feature_manifest_hash": stable_hash(data_mart_manifest.get("feature_files", [])),
        "label_manifest_hash": stable_hash(data_mart_manifest.get("label_counts", {})),
        "readiness_gate_snapshot": readiness,
        "allowed_actions": ["research_report", "candidate_review_pack"],
        "blocked_actions": readiness.get("blocked_actions", []),
        "operator_approval_status": "missing",
        "result_path": "",
    }
