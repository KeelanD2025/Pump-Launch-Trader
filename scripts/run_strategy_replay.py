#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import sys

from strategy.io import read_json
from strategy.schemas import STRATEGY_ARCHITECTURE_ROOT
from strategy_pipeline.replay import replay_gate
from strategy_pipeline.schemas import PIPELINE_ROOT


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--architecture-root", type=pathlib.Path, default=STRATEGY_ARCHITECTURE_ROOT)
    parser.add_argument("--pipeline-root", type=pathlib.Path, default=PIPELINE_ROOT)
    args = parser.parse_args()
    readiness = read_json(args.pipeline_root / "READINESS_DECISION.json") or read_json(args.architecture_root / "readiness_decision.json")
    gate = replay_gate(readiness).to_dict()
    if not gate["allowed"]:
        print(json.dumps(gate, sort_keys=True))
        return 2
    print(json.dumps(gate, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
