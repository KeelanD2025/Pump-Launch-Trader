#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import sys

from strategy_pipeline.positive_outcomes import build_positive_outcome_labels
from strategy_pipeline.schemas import DATA_MART_ROOT, PIPELINE_ROOT, STRATEGY_ARCHITECTURE_ROOT


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-mart-root", type=pathlib.Path, default=DATA_MART_ROOT)
    parser.add_argument("--architecture-root", type=pathlib.Path, default=STRATEGY_ARCHITECTURE_ROOT)
    parser.add_argument("--output-root", type=pathlib.Path, default=PIPELINE_ROOT)
    args = parser.parse_args()
    summary = build_positive_outcome_labels(
        data_mart_root=args.data_mart_root,
        architecture_root=args.architecture_root,
        output_root=args.output_root,
    )
    print(json.dumps({"ok": True, "summary": summary}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
