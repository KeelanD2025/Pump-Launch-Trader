#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from strategy_pipeline.early_burst_in_out_v1 import build_early_burst_in_out_v1


def main() -> int:
    parser = argparse.ArgumentParser(description="Build EarlyBurstInOutV1 research review artifacts.")
    parser.add_argument("--pipeline-root", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=None)
    args = parser.parse_args()
    kwargs = {}
    if args.pipeline_root is not None:
        kwargs["pipeline_root"] = args.pipeline_root
    if args.output_root is not None:
        kwargs["output_root"] = args.output_root
    summary = build_early_burst_in_out_v1(**kwargs)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

