from __future__ import annotations

from typing import Any

from .feature_store import PipelineFeatureStore
from .label_store import PipelineLabelStore


def run_leakage_audit(features: PipelineFeatureStore, labels: PipelineLabelStore) -> dict[str, Any]:
    feature_result = features.validate()
    label_result = labels.validate()
    blockers = list(feature_result["blockers"]) + list(label_result["blockers"])
    return {
        "passed": not blockers,
        "blockers": blockers,
        "feature_rows_checked": feature_result["rows_checked"],
        "label_rows_checked": label_result["summary"]["total"],
        "provider_relay_quality_alpha_use": False,
        "r2_artifact_status_alpha_use": False,
    }
