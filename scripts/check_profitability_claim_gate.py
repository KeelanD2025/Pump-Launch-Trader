#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import sys

from strategy_pipeline.io import read_json
from strategy_pipeline.profitability_claims import profitability_claim_gate, report_text_allowed
from strategy_pipeline.schemas import PIPELINE_ROOT


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-root", type=pathlib.Path, default=PIPELINE_ROOT)
    parser.add_argument("--text", default="research-only hypothesis")
    args = parser.parse_args()
    readiness = read_json(args.pipeline_root / "READINESS_DECISION.json")
    gate = profitability_claim_gate(readiness)
    text_check = report_text_allowed(args.text, gate)
    payload = gate.to_dict() | {"text_check": text_check}
    print(json.dumps(payload, sort_keys=True))
    return 0 if gate.allowed and text_check["passed"] else 2


if __name__ == "__main__":
    sys.exit(main())
