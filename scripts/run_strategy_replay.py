#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import sys

from strategy.io import read_json
from strategy.schemas import STRATEGY_ARCHITECTURE_ROOT


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--architecture-root", type=pathlib.Path, default=STRATEGY_ARCHITECTURE_ROOT)
    args = parser.parse_args()
    readiness = read_json(args.architecture_root / "readiness_decision.json")
    if not readiness.get("replay_ready"):
        print(json.dumps({
            "allowed": False,
            "blocker": "REPLAY_BLOCKED_BY_READINESS_GATE",
            "reason_codes": readiness.get("reason_codes", ["readiness_missing"]),
        }, sort_keys=True))
        return 2
    print(json.dumps({"allowed": True, "note": "not implemented in this phase"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
