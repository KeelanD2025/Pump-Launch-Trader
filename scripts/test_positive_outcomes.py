#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))

from strategy_pipeline.feature_store import PipelineFeatureStore
from strategy_pipeline.io import write_csv, write_json
from strategy_pipeline.positive_outcomes import build_positive_outcome_labels
from strategy_pipeline.schemas import HORIZONS


def label(**overrides: str) -> dict[str, str]:
    row = {
        "mint": "mint1",
        "slice_id": "slice1",
        "segment_id": "1",
        "relay_session_id": "relay1",
        "first_seen_at": "2026-06-18T00:00:00Z",
        "final_outcome": "",
        "rejection_reason": "",
        "terminal_inconclusive_reason": "",
        "provider_gap_exposed": "false",
        "relay_gap_exposed": "false",
        "sequence_gap_exposed": "false",
        "hash_mismatch_exposed": "false",
        "receiver_backpressure_exposed": "false",
        "candidate_checkpoint_seen": "false",
        "replay_eligible": "false",
        "clean_negative_label": "false",
        "clean_positive_label": "false",
        "censored_label": "false",
    }
    row.update(overrides)
    return row


def feature(horizon: int, **overrides: str) -> dict[str, str]:
    row = {
        "mint": "mint1",
        "slice_id": "slice1",
        "segment_id": "1",
        "relay_session_id": "relay1",
        "horizon_seconds": str(horizon),
        "feature_asof_timestamp": f"2026-06-18T00:00:{horizon:02d}Z" if horizon < 60 else "2026-06-18T00:01:00Z",
        "mint_first_seen_timestamp": "2026-06-18T00:00:00Z",
        "horizon_reached": "true",
        "data_complete_for_horizon": "true",
        "data_quality_exclusion": "false",
        "provider_gap_exposed": "false",
        "relay_gap_exposed": "false",
        "sequence_gap_exposed": "false",
        "hash_mismatch_exposed": "false",
        "receiver_backpressure_exposed": "false",
        "terminal_inconclusive_before_horizon": "false",
        "rejected_before_horizon": "false",
        "degraded_audit_only_before_horizon": "false",
        "high_throughput_before_horizon": "false",
        "curve_progress_proxy_asof": "20",
        "liquidity_delta_asof": "0",
        "reserve_delta_asof": "0",
        "volume_delta_asof": "0",
        "net_buy_sell_delta_asof": "0",
        "unique_holder_accounts_seen_asof": "1",
        "top_holder_concentration_asof": "0.5",
        "holder_rpc_used": "false",
        "rpc_mint_supply_canonical": "false",
        "threshold_tuning_allowed": "false",
        "live_trading_enabled": "false",
    }
    row.update(overrides)
    return row


def write_mart(root: Path, labels: list[dict[str, str]], features_by_horizon: dict[int, list[dict[str, str]]]) -> None:
    label_fields = list(labels[0].keys())
    write_csv(root / "strategy_labels.csv", labels, label_fields)
    write_csv(root / "strategy_mint_table.csv", labels, label_fields)
    for horizon in HORIZONS:
        rows = features_by_horizon.get(horizon, [])
        fields = list(rows[0].keys()) if rows else list(feature(horizon).keys())
        write_csv(root / f"strategy_asof_features_{horizon:03d}s.csv", rows, fields)


def row_labels(path: Path) -> list[dict[str, str]]:
    with path.open() as handle:
        return list(csv.DictReader(handle))


class PositiveOutcomeTests(unittest.TestCase):
    def test_positive_outcome_labels_are_separate_from_features(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_mart(root / "mart", [label()], {
                5: [feature(5, curve_progress_proxy_asof="20")],
                60: [feature(60, curve_progress_proxy_asof="45", net_buy_sell_delta_asof="5", volume_delta_asof="10")],
            })
            build_positive_outcome_labels(data_mart_root=root / "mart", architecture_root=root / "arch", output_root=root / "out")
            rows = row_labels(root / "out" / "positive_outcome_labels.csv")
            self.assertTrue(any(row["positive_outcome_label"] == "positive" for row in rows))
            self.assertTrue(PipelineFeatureStore(root / "mart").validate()["passed"])

    def test_labeler_does_not_use_holder_rpc_or_canonical_supply(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_mart(root / "mart", [label()], {5: [feature(5, holder_rpc_used="true", rpc_mint_supply_canonical="true")]})
            result = PipelineFeatureStore(root / "mart").validate()
            self.assertFalse(result["passed"])
            build_positive_outcome_labels(data_mart_root=root / "mart", architecture_root=root / "arch", output_root=root / "out")
            self.assertTrue((root / "out" / "positive_outcome_labels.csv").exists())

    def test_terminal_inconclusive_remains_censored(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_mart(root / "mart", [label(final_outcome="terminal_inconclusive", censored_label="true")], {
                5: [feature(5, curve_progress_proxy_asof="80")]
            })
            summary = build_positive_outcome_labels(data_mart_root=root / "mart", architecture_root=root / "arch", output_root=root / "out")
            self.assertGreaterEqual(summary["counts"].get("censored", 0), 1)

    def test_provider_gap_exposed_cannot_be_clean_positive_outcome(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_mart(root / "mart", [label(provider_gap_exposed="true")], {
                5: [feature(5, provider_gap_exposed="true", curve_progress_proxy_asof="90")]
            })
            summary = build_positive_outcome_labels(data_mart_root=root / "mart", architecture_root=root / "arch", output_root=root / "out")
            self.assertEqual(summary["counts"].get("positive", 0), 0)
            self.assertGreaterEqual(summary["counts"].get("invalid_quality", 0), 1)

    def test_high_positive_does_not_imply_replay_eligibility(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_mart(root / "mart", [label(candidate_checkpoint_seen="false", replay_eligible="false")], {
                5: [feature(5, curve_progress_proxy_asof="20")],
                60: [feature(60, curve_progress_proxy_asof="85", net_buy_sell_delta_asof="12", volume_delta_asof="10")],
            })
            summary = build_positive_outcome_labels(data_mart_root=root / "mart", architecture_root=root / "arch", output_root=root / "out")
            self.assertGreaterEqual(summary["high_positive_count"], 1)
            self.assertEqual(summary["replay_eligible_positive_or_high"], 0)

    def test_candidate_checkpoint_absence_does_not_erase_outcome_label(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_mart(root / "mart", [label(candidate_checkpoint_seen="false")], {
                5: [feature(5, curve_progress_proxy_asof="20")],
                60: [feature(60, curve_progress_proxy_asof="50")],
            })
            build_positive_outcome_labels(data_mart_root=root / "mart", architecture_root=root / "arch", output_root=root / "out")
            rows = row_labels(root / "out" / "positive_outcome_labels.csv")
            self.assertTrue(any(row["positive_outcome_label"] == "positive" for row in rows))

    def test_leakage_audit_blocks_outcome_label_in_alpha_features(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_mart(root / "mart", [label()], {5: [feature(5, positive_outcome_label="positive")]})
            result = PipelineFeatureStore(root / "mart").validate()
            self.assertFalse(result["passed"])
            self.assertTrue(any("forbidden_alpha_columns" in blocker for blocker in result["blockers"]))

    def test_forbidden_harnesses_still_block(self) -> None:
        commands = [
            [sys.executable, str(SCRIPT_DIR / "run_strategy_backtest.py"), "--strategy", "candidate_eligibility_v2"],
            [sys.executable, str(SCRIPT_DIR / "run_strategy_replay.py")],
            [sys.executable, str(SCRIPT_DIR / "run_strategy_threshold_tuning.py")],
            [sys.executable, str(SCRIPT_DIR / "run_strategy_live.py")],
            [sys.executable, str(SCRIPT_DIR / "check_profitability_claim_gate.py")],
        ]
        for command in commands:
            proc = subprocess.run(command, cwd=REPO, text=True, capture_output=True)
            self.assertNotEqual(proc.returncode, 0, command)
            self.assertIn("blocker", proc.stdout)


if __name__ == "__main__":
    unittest.main()
