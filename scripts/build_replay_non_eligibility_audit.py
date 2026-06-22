#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import sys

from strategy_pipeline.replay_non_eligibility import build_replay_non_eligibility_audit
from strategy_pipeline.schemas import DATA_MART_ROOT, PIPELINE_ROOT, REPO_ROOT


def main() -> int:
    parser = argparse.ArgumentParser(description="Build replay non-eligibility root-cause audit artifacts from existing data only.")
    parser.add_argument("--pipeline-root", type=pathlib.Path, default=PIPELINE_ROOT)
    parser.add_argument("--data-mart-root", type=pathlib.Path, default=DATA_MART_ROOT)
    parser.add_argument("--local-collector-root", type=pathlib.Path, default=REPO_ROOT / "research_output" / "local_stream_collector")
    parser.add_argument("--output-root", type=pathlib.Path, default=PIPELINE_ROOT / "replay_non_eligibility_audit")
    parser.add_argument("--no-update-readiness", action="store_true")
    args = parser.parse_args()
    payload = build_replay_non_eligibility_audit(
        pipeline_root=args.pipeline_root,
        data_mart_root=args.data_mart_root,
        local_collector_root=args.local_collector_root,
        output_root=args.output_root,
        update_readiness_files=not args.no_update_readiness,
    )
    concise = {
        key: payload.get(key)
        for key in (
            "classification",
            "rows",
            "positive_or_high_rows",
            "high_positive_rows",
            "candidate_checkpoint_rows",
            "replay_eligible_rows",
            "clean_counted_r2_verified_positive_rows_blocked_without_candidate_checkpoint",
            "collection_reliability_primary_blocker",
            "gate_strictness_primary_blocker",
            "separate_early_burst_replay_candidate_type_needed",
            "output_root",
            "zip_path",
        )
    }
    concise["top_root_cause_counts"] = dict(
        sorted(payload.get("root_cause_counts", {}).items(), key=lambda item: (-item[1], item[0]))[:10]
    )
    print(json.dumps(concise, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
