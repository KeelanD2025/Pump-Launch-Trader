from __future__ import annotations

import pathlib
from datetime import datetime, timezone
from typing import Any

from .io import read_csv, write_csv, write_json, write_text
from .schemas import DATA_MART_ROOT, PIPELINE_ROOT, boolish


def build_candidate_review_pack(output_root: pathlib.Path = PIPELINE_ROOT, data_mart_root: pathlib.Path = DATA_MART_ROOT) -> pathlib.Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    pack = output_root / f"candidate_review_pack_{timestamp}"
    labels = read_csv(data_mart_root / "strategy_labels.csv")
    candidates = [row for row in labels if boolish(row.get("candidate_checkpoint_seen")) or boolish(row.get("replay_eligible"))]
    fields = list(candidates[0].keys()) if candidates else ["mint", "slice_id", "segment_id", "candidate_checkpoint_seen", "replay_eligible"]
    write_csv(pack / "candidate_mints.csv", candidates, fields)
    write_json(pack / "candidate_review_decision.json", {
        "candidate_count": len(candidates),
        "replay_eligible_candidate_count": sum(1 for row in candidates if boolish(row.get("replay_eligible"))),
        "decision": "NO_CANDIDATES_YET" if not candidates else "CANDIDATE_REVIEW_REQUIRED",
        "replay_was_run": False,
        "backtesting_was_run": False,
        "trading_was_run": False,
    })
    write_text(
        pack / "README.md",
        "# Candidate Review Pack\n\n"
        f"- candidate_count: `{len(candidates)}`\n"
        "- replay_was_run: `false`\n"
        "- formal_backtesting_was_run: `false`\n"
        "- live_trading_was_run: `false`\n"
        + ("- reason: `NO_CANDIDATES_YET`\n" if not candidates else "- reason: `operator_review_required_before_replay`\n"),
    )
    return pack
