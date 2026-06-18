#!/usr/bin/env python3
from __future__ import annotations

import argparse
import pathlib
import sys

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from strategy_pipeline.early_burst_backtest_readiness import (
    BACKTEST_READINESS_ROOT,
    VALIDATION_ROOT,
    build_early_burst_backtest_readiness,
)
from strategy_pipeline.schemas import PIPELINE_ROOT


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the fail-closed early-burst backtest-readiness pack.")
    parser.add_argument("--output-root", type=pathlib.Path, default=PIPELINE_ROOT)
    parser.add_argument("--validation-root", type=pathlib.Path, default=VALIDATION_ROOT)
    parser.add_argument("--readiness-root", type=pathlib.Path, default=BACKTEST_READINESS_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = build_early_burst_backtest_readiness(
        output_root=args.output_root,
        validation_root=args.validation_root,
        readiness_root=args.readiness_root,
    )
    print(summary["classification"])
    print(f"decision={summary['decision_path']}")
    print(f"gpt_pack={summary['gpt_pack_path']}")
    print(f"gpt_pack_zip={summary['gpt_pack_zip_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
