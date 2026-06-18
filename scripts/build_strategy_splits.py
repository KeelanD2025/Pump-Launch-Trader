#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import sys

from strategy_pipeline.io import write_json, write_text
from strategy_pipeline.label_store import PipelineLabelStore
from strategy_pipeline.schemas import DATA_MART_ROOT, SPLITS_ROOT
from strategy_pipeline.splits import build_walk_forward_splits, validate_splits


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-mart-root", type=pathlib.Path, default=DATA_MART_ROOT)
    parser.add_argument("--output-root", type=pathlib.Path, default=SPLITS_ROOT)
    args = parser.parse_args()
    labels = PipelineLabelStore(args.data_mart_root).load()
    splits = build_walk_forward_splits(labels)
    validation = validate_splits(splits)
    args.output_root.mkdir(parents=True, exist_ok=True)
    write_json(args.output_root / "splits.json", {**splits, "validation": validation})
    write_text(args.output_root / "splits.md", "# Walk-Forward Splits\n\n" f"- validation_passed: `{str(validation['passed']).lower()}`\n")
    print(json.dumps({"ok": validation["passed"], "split_id": splits["split_id"], "blockers": validation["blockers"]}, sort_keys=True))
    return 0 if validation["passed"] else 2


if __name__ == "__main__":
    sys.exit(main())
