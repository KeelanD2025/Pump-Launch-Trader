from __future__ import annotations

from typing import Any


def build_chronological_splits(labels: list[dict[str, str]], *, embargo_rows: int = 5) -> dict[str, Any]:
    ordered = sorted(labels, key=lambda row: (row.get("first_seen_at", ""), row.get("mint", "")))
    seen: set[str] = set()
    unique = []
    for row in ordered:
        mint = row.get("mint", "")
        if mint and mint not in seen:
            seen.add(mint)
            unique.append(row)
    total = len(unique)
    train_end = max(0, int(total * 0.6))
    validation_start = min(total, train_end + embargo_rows)
    validation_end = min(total, validation_start + int(total * 0.2))
    test_start = min(total, validation_end + embargo_rows)
    splits = {
        "method": "chronological_walk_forward",
        "embargo_rows": embargo_rows,
        "train": [row.get("mint", "") for row in unique[:train_end]],
        "validation": [row.get("mint", "") for row in unique[validation_start:validation_end]],
        "test": [row.get("mint", "") for row in unique[test_start:]],
    }
    return splits


def validate_splits(splits: dict[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    if splits.get("method") != "chronological_walk_forward":
        blockers.append("random_or_unknown_split_method")
    membership: dict[str, str] = {}
    for split_name in ("train", "validation", "test"):
        for mint in splits.get(split_name, []):
            if mint in membership:
                blockers.append(f"mint_in_multiple_splits:{mint}:{membership[mint]}:{split_name}")
            membership[mint] = split_name
    return {"passed": not blockers, "blockers": blockers, "mint_count": len(membership)}

