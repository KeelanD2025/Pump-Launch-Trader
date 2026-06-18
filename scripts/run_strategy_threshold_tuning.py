#!/usr/bin/env python3
from __future__ import annotations

import json
import argparse
import pathlib
import sys

from strategy.io import read_json
from strategy_pipeline.schemas import PIPELINE_ROOT
from strategy_pipeline.threshold_tuning import threshold_tuning_gate


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-root", type=pathlib.Path, default=PIPELINE_ROOT)
    args = parser.parse_args()
    readiness = read_json(args.pipeline_root / "READINESS_DECISION.json")
    gate = threshold_tuning_gate(readiness).to_dict()
    print(json.dumps(gate, sort_keys=True))
    return 2


if __name__ == "__main__":
    sys.exit(main())
