from __future__ import annotations

import pathlib
import time
from typing import Any

from .io import write_csv, write_json, write_text
from .schemas import HORIZONS


def build_candidate_review_pack(
    output_root: pathlib.Path,
    labels: list[dict[str, str]],
    alpha_by_horizon: dict[int, list[dict[str, str]]],
    gate_rows: list[dict[str, Any]],
) -> pathlib.Path:
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    pack = output_root / f"candidate_review_pack_{timestamp}"
    candidates = [
        row for row in labels
        if str(row.get("candidate_checkpoint_seen", "")).lower() == "true"
        or str(row.get("replay_eligible", "")).lower() == "true"
    ]
    fields = ["mint", "slice_id", "segment_id", "relay_session_id", "candidate_checkpoint_seen", "replay_eligible", "final_outcome", "label_quality"]
    write_csv(pack / "candidate_mints.csv", candidates, fields)
    candidate_mints = {row.get("mint", "") for row in candidates}
    alpha_rows = [row for horizon in HORIZONS for row in alpha_by_horizon.get(horizon, []) if row.get("mint") in candidate_mints]
    if alpha_rows:
        write_csv(pack / "candidate_asof_alpha_features.csv", alpha_rows, list(alpha_rows[0].keys()))
    else:
        write_csv(pack / "candidate_asof_alpha_features.csv", [], ["mint", "horizon_seconds"])
    matching_gate_rows = [row for row in gate_rows if row.get("mint") in candidate_mints]
    write_csv(pack / "candidate_gate_decisions.csv", matching_gate_rows, list(matching_gate_rows[0].keys()) if matching_gate_rows else ["mint", "decision"])
    write_json(pack / "candidate_review_decision.json", {
        "candidate_count": len(candidates),
        "replay_was_run": False,
        "backtesting_was_run": False,
        "live_trading_was_run": False,
        "operator_review_required": bool(candidates),
    })
    write_text(
        pack / "README.md",
        "# Candidate Review Pack\n\n"
        f"- candidate_count: `{len(candidates)}`\n"
        "- replay_was_run: `false`\n"
        "- backtesting_was_run: `false`\n"
        "- live_trading_was_run: `false`\n"
        "- candidate checkpoints are audit-only until countability and operator approval allow replay.\n",
    )
    return pack

