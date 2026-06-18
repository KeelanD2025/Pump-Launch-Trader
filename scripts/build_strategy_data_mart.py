#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import sys

from strategy_pipeline.data_mart import build_data_mart
from strategy_pipeline.schemas import DATA_MART_ROOT, STRATEGY_ARCHITECTURE_ROOT


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--architecture-root", type=pathlib.Path, default=STRATEGY_ARCHITECTURE_ROOT)
    parser.add_argument("--output-root", type=pathlib.Path, default=DATA_MART_ROOT)
    args = parser.parse_args()
    manifest = build_data_mart(architecture_root=args.architecture_root, output_root=args.output_root)
    print(json.dumps({"ok": True, "data_mart": str(args.output_root), "mint_rows": manifest["mint_rows"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
