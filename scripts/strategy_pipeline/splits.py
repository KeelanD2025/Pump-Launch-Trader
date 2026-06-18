from __future__ import annotations

from typing import Any

from .schemas import stable_hash


def build_walk_forward_splits(labels: list[dict[str, str]], *, embargo_rows: int = 5) -> dict[str, Any]:
    ordered = sorted(labels, key=lambda row: (row.get("first_seen_at", ""), row.get("mint", "")))
    unique: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in ordered:
        mint = row.get("mint", "")
        if mint and mint not in seen:
            seen.add(mint)
            unique.append(row)
    total = len(unique)
    train_end = int(total * 0.6)
    validation_start = min(total, train_end + embargo_rows)
    validation_end = min(total, validation_start + int(total * 0.2))
    test_start = min(total, validation_end + embargo_rows)
    splits = {
        "split_id": stable_hash({"mints": [row.get("mint") for row in unique], "embargo_rows": embargo_rows})[:16],
        "method": "chronological_walk_forward",
        "embargo_rows": embargo_rows,
        "train": [row.get("mint", "") for row in unique[:train_end]],
        "validation": [row.get("mint", "") for row in unique[validation_start:validation_end]],
        "test": [row.get("mint", "") for row in unique[test_start:]],
    }
    splits["manifest_hash"] = stable_hash(splits)
    return splits


def validate_splits(splits: dict[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    if splits.get("method") != "chronological_walk_forward":
        blockers.append("random_split_rejected")
    membership: dict[str, str] = {}
    for name in ("train", "validation", "test"):
        for mint in splits.get(name, []):
            if mint in membership:
                blockers.append(f"same_mint_multiple_splits:{mint}:{membership[mint]}:{name}")
            membership[mint] = name
    if not splits.get("embargo_rows", 0):
        blockers.append("embargo_required")
    return {"passed": not blockers, "blockers": blockers, "mint_count": len(membership)}
