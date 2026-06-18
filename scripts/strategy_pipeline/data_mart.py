from __future__ import annotations

import pathlib
from typing import Any

from .io import read_csv, read_json, write_csv, write_json, write_text
from .schemas import DATA_MART_ROOT, HORIZONS, STRATEGY_ARCHITECTURE_ROOT, boolish, file_sha256


def _fields(rows: list[dict[str, Any]], fallback: list[str]) -> list[str]:
    if not rows:
        return fallback
    ordered = list(rows[0].keys())
    for row in rows:
        for key in row:
            if key not in ordered:
                ordered.append(key)
    return ordered


def build_data_mart(
    *,
    architecture_root: pathlib.Path = STRATEGY_ARCHITECTURE_ROOT,
    output_root: pathlib.Path = DATA_MART_ROOT,
) -> dict[str, Any]:
    source_root = architecture_root / "buy_quality_dataset"
    output_root.mkdir(parents=True, exist_ok=True)
    labels = read_csv(source_root / "buy_quality_mint_table.csv")
    label_manifest = read_json(source_root / "buy_quality_label_manifest.json")
    feature_manifest = read_json(source_root / "buy_quality_feature_manifest.json")
    data_quality_manifest = read_json(source_root / "buy_quality_data_quality_manifest.json")

    label_fields = _fields(labels, ["mint", "slice_id", "segment_id", "final_outcome"])
    write_csv(output_root / "strategy_mint_table.csv", labels, label_fields)
    write_csv(output_root / "strategy_labels.csv", labels, label_fields)

    feature_files: list[dict[str, Any]] = []
    for horizon in HORIZONS:
        rows = read_csv(source_root / f"buy_quality_asof_features_{horizon:03d}s.csv")
        fields = _fields(rows, ["mint", "slice_id", "horizon_seconds"])
        dest = output_root / f"strategy_asof_features_{horizon:03d}s.csv"
        write_csv(dest, rows, fields)
        feature_files.append({
            "horizon_seconds": horizon,
            "path": str(dest),
            "rows": len(rows),
            "sha256": file_sha256(dest),
        })

    clean_negative = sum(1 for row in labels if boolish(row.get("clean_negative_label")))
    censored = sum(1 for row in labels if boolish(row.get("censored_label")))
    clean_positive = sum(1 for row in labels if boolish(row.get("clean_positive_label")))
    replay_eligible = sum(1 for row in labels if boolish(row.get("replay_eligible")))
    candidate_checkpoints = sum(1 for row in labels if boolish(row.get("candidate_checkpoint_seen")))
    manifest = {
        "schema_version": "phase107i.strategy_data_mart.v1",
        "source_architecture_root": str(architecture_root),
        "mint_rows": len(labels),
        "label_counts": {
            "clean_negative": clean_negative,
            "censored": censored,
            "clean_positive": clean_positive,
            "candidate_checkpoint": candidate_checkpoints,
            "replay_eligible": replay_eligible,
        },
        "included_slices": data_quality_manifest.get("included_slices", 0),
        "excluded_slices": data_quality_manifest.get("excluded_slices", 0),
        "feature_files": feature_files,
        "source_label_manifest": label_manifest,
        "source_feature_manifest": feature_manifest,
        "rules": [
            "counted R2-verified artifact-consistent slices only",
            "features are separated from labels",
            "terminal_inconclusive is censored",
            "candidate checkpoints are audit-only",
            "holder RPC disabled",
            "RPC mint supply non-canonical",
        ],
    }
    write_json(output_root / "strategy_data_mart_manifest.json", manifest)
    write_text(
        output_root / "strategy_data_mart_summary.md",
        "\n".join([
            "# Strategy Data Mart Summary",
            "",
            f"- mint_rows: `{len(labels)}`",
            f"- included_slices: `{manifest['included_slices']}`",
            f"- clean_negative: `{clean_negative}`",
            f"- censored: `{censored}`",
            f"- clean_positive: `{clean_positive}`",
            f"- replay_eligible: `{replay_eligible}`",
            f"- candidate_checkpoints: `{candidate_checkpoints}`",
            "- raw relay frames: `excluded`",
            "- labels and alpha features: `separate`",
        ]) + "\n",
    )
    return manifest
