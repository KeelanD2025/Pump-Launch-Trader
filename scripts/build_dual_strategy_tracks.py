#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import sys

from strategy_pipeline.dual_strategy_tracks import DUAL_OUTPUT_ROOT, build_dual_strategy_tracks
from strategy_pipeline.schemas import DATA_MART_ROOT, PIPELINE_ROOT


def main() -> int:
    parser = argparse.ArgumentParser(description="Build research-only dual strategy review tracks from existing data.")
    parser.add_argument("--data-mart-root", type=pathlib.Path, default=DATA_MART_ROOT)
    parser.add_argument("--pipeline-root", type=pathlib.Path, default=PIPELINE_ROOT)
    parser.add_argument("--output-root", type=pathlib.Path, default=DUAL_OUTPUT_ROOT)
    parser.add_argument("--no-update-readiness", action="store_true")
    args = parser.parse_args()
    summary = build_dual_strategy_tracks(
        data_mart_root=args.data_mart_root,
        pipeline_root=args.pipeline_root,
        output_root=args.output_root,
        update_readiness_files=not args.no_update_readiness,
    )
    concise = {
        "classification": summary["classification"],
        "currently_more_promising_track": summary["currently_more_promising_track"],
        "early_review_candidates": summary["early_burst_in_out_v0"]["review_candidates"],
        "early_positive_high_rows_captured": summary["early_burst_in_out_v0"]["positive_high_rows_captured"],
        "survivor_review_candidates": summary["survivor_runner_v0"]["review_candidates"],
        "survivor_positive_high_rows_captured": summary["survivor_runner_v0"]["positive_high_rows_captured"],
        "replay_ready": summary["replay_ready"],
        "formal_backtesting_ready": summary["formal_backtesting_ready"],
        "collection_allowed": summary["collection_allowed"],
        "output_root": summary["output_root"],
        "zip_path": summary["zip_path"],
    }
    print(json.dumps(concise, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
