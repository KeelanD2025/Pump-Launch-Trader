from __future__ import annotations

from typing import Any

from .feature_store import FeatureStore
from .label_store import LabelStore
from .schemas import FORBIDDEN_ALPHA_COLUMNS


def leakage_audit(feature_store: FeatureStore, label_store: LabelStore) -> dict[str, Any]:
    blockers: list[str] = []
    feature_validation = feature_store.validate_asof_safety()
    label_validation = label_store.validate()
    blockers.extend(feature_validation["blockers"])
    blockers.extend(label_validation["blockers"])
    for horizon in feature_store.get_feature_horizons_with_rows():
        for row in feature_store.load_asof_features(horizon):
            leaked = sorted(field for field in FORBIDDEN_ALPHA_COLUMNS if field in row)
            if leaked:
                blockers.append(f"feature_leakage_columns:{horizon}:{','.join(leaked)}")
                break
    return {
        "passed": not blockers,
        "blockers": blockers,
        "feature_rows_checked": feature_validation["rows_checked"],
        "label_rows_checked": label_validation["rows_checked"],
    }


def _feature_horizons_with_rows(self: FeatureStore) -> list[int]:
    return [horizon for horizon in (5, 10, 30, 60, 120, 300, 900) if self.load_asof_features(horizon)]


FeatureStore.get_feature_horizons_with_rows = _feature_horizons_with_rows  # type: ignore[attr-defined]

