#!/usr/bin/env python3
from __future__ import annotations

import argparse
import pathlib
import sys

from strategy.candidate_review import build_candidate_review_pack
from strategy.feature_store import FeatureStore
from strategy.io import read_csv
from strategy.label_store import LabelStore
from strategy.schemas import HORIZONS, STRATEGY_ARCHITECTURE_ROOT, STRATEGY_READINESS_ROOT


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--readiness-root", type=pathlib.Path, default=STRATEGY_READINESS_ROOT)
    parser.add_argument("--output-root", type=pathlib.Path, default=STRATEGY_ARCHITECTURE_ROOT)
    args = parser.parse_args()
    labels = LabelStore(args.readiness_root).load_mint_labels()
    store = FeatureStore(args.readiness_root)
    alpha = {horizon: store.load_asof_features(horizon) for horizon in HORIZONS}
    gates = read_csv(args.output_root / "candidate_eligibility_v2_scores.csv")
    pack = build_candidate_review_pack(args.output_root, labels, alpha, gates)
    print(pack)
    return 0


if __name__ == "__main__":
    sys.exit(main())
