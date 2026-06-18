#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import sys

from strategy.io import read_json
from strategy.schemas import STRATEGY_ARCHITECTURE_ROOT
from strategy_pipeline.backtest import backtest_gate
from strategy_pipeline.schemas import PIPELINE_ROOT


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--architecture-root", type=pathlib.Path, default=STRATEGY_ARCHITECTURE_ROOT)
    parser.add_argument("--pipeline-root", type=pathlib.Path, default=PIPELINE_ROOT)
    args = parser.parse_args()
    readiness = read_json(args.pipeline_root / "READINESS_DECISION.json") or read_json(args.architecture_root / "readiness_decision.json")
    gate = backtest_gate(readiness, strategy=args.strategy).to_dict() | {"strategy": args.strategy}
    if not gate["allowed"]:
        print(json.dumps(gate, sort_keys=True))
        return 2
    print(json.dumps(gate, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
