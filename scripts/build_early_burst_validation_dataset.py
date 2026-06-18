#!/usr/bin/env python3
from __future__ import annotations

import argparse
import pathlib
import sys

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from strategy_pipeline.early_burst_validation import build_early_burst_validation_dataset
from strategy_pipeline.schemas import DATA_MART_ROOT, PIPELINE_ROOT, STRATEGY_ARCHITECTURE_ROOT


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the research-only early-burst validation dataset.")
    parser.add_argument("--output-root", type=pathlib.Path, default=PIPELINE_ROOT)
    parser.add_argument("--data-mart-root", type=pathlib.Path, default=DATA_MART_ROOT)
    parser.add_argument("--architecture-root", type=pathlib.Path, default=STRATEGY_ARCHITECTURE_ROOT)
    parser.add_argument("--validation-root", type=pathlib.Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = build_early_burst_validation_dataset(
        output_root=args.output_root,
        data_mart_root=args.data_mart_root,
        architecture_root=args.architecture_root,
        validation_root=args.validation_root,
    )
    print(summary["classification"])
    print(f"validation_root={summary['validation_root']}")
    print(f"gpt_pack={summary['gpt_pack_path']}")
    print(f"gpt_pack_zip={summary['gpt_pack_zip_path']}")
    return 0 if summary["classification"] == "EARLY_BURST_VALIDATION_DATASET_PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
