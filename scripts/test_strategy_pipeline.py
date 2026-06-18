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

from strategy_pipeline.backtest import backtest_gate
from strategy_pipeline.data_mart import build_data_mart
from strategy_pipeline.feature_store import PipelineFeatureStore
from strategy_pipeline.io import write_csv, write_json
from strategy_pipeline.label_store import PipelineLabelStore
from strategy_pipeline.live_trading import live_trading_gate
from strategy_pipeline.profitability_claims import profitability_claim_gate, report_text_allowed
from strategy_pipeline.registry import strategy_registry, validate_registry
from strategy_pipeline.replay import replay_gate
from strategy_pipeline.schemas import HORIZONS
from strategy_pipeline.splits import validate_splits


def write_pipeline_readiness(root: Path, **overrides: object) -> None:
    readiness = {
        "strategy_research_ready": True,
        "buy_strategy_architecture_ready": True,
        "backtesting_ready": False,
        "replay_ready": False,
        "threshold_tuning_ready": False,
        "paper_trading_ready": False,
        "live_trading_ready": False,
        "wallet_execution_ready": False,
        "profitability_claim_allowed": False,
        "reason_codes": ["no_clean_positives", "no_replay_eligible_candidates"],
    }
    readiness.update(overrides)
    write_json(root / "READINESS_DECISION.json", readiness)


def label_row(**overrides: str) -> dict[str, str]:
    row = {
        "mint": "mint1",
        "slice_id": "slice1",
        "segment_id": "segment1",
        "first_seen_at": "2026-06-18T00:00:00Z",
        "final_outcome": "early_rejected_dead",
        "provider_gap_exposed": "false",
        "candidate_checkpoint_seen": "false",
        "replay_eligible": "false",
        "clean_negative_label": "true",
        "clean_positive_label": "false",
        "censored_label": "false",
    }
    row.update(overrides)
    return row


def feature_row(**overrides: str) -> dict[str, str]:
    row = {
        "mint": "mint1",
        "slice_id": "slice1",
        "segment_id": "segment1",
        "horizon_seconds": "60",
        "feature_asof_timestamp": "2026-06-18T00:01:00Z",
        "mint_first_seen_timestamp": "2026-06-18T00:00:00Z",
        "horizon_reached": "true",
        "data_complete_for_horizon": "true",
        "provider_gap_exposed": "false",
        "relay_gap_exposed": "false",
        "sequence_gap_exposed": "false",
        "hash_mismatch_exposed": "false",
        "receiver_backpressure_exposed": "false",
        "terminal_inconclusive_before_horizon": "false",
        "rejected_before_horizon": "false",
        "holder_rpc_used": "false",
        "rpc_mint_supply_canonical": "false",
        "trade_update_count_asof": "1",
    }
    row.update(overrides)
    return row


def write_data_mart(root: Path, labels: list[dict[str, str]], features: list[dict[str, str]]) -> None:
    label_fields = list(labels[0].keys()) if labels else ["mint"]
    write_csv(root / "strategy_labels.csv", labels, label_fields)
    write_csv(root / "strategy_mint_table.csv", labels, label_fields)
    for horizon in HORIZONS:
        rows = [dict(row, horizon_seconds=str(horizon)) for row in features]
        fields = list(rows[0].keys()) if rows else ["mint", "slice_id", "horizon_seconds"]
        write_csv(root / f"strategy_asof_features_{horizon:03d}s.csv", rows, fields)


def write_architecture_dataset(root: Path) -> None:
    dataset = root / "buy_quality_dataset"
    labels = [label_row()]
    features = [feature_row()]
    write_csv(dataset / "buy_quality_mint_table.csv", labels, list(labels[0].keys()))
    for horizon in HORIZONS:
        rows = [dict(features[0], horizon_seconds=str(horizon))]
        write_csv(dataset / f"buy_quality_asof_features_{horizon:03d}s.csv", rows, list(rows[0].keys()))
    write_json(dataset / "buy_quality_label_manifest.json", {"rows": 1, "label_counts": {"clean_negative": 1}})
    write_json(dataset / "buy_quality_feature_manifest.json", {"files": []})
    write_json(dataset / "buy_quality_data_quality_manifest.json", {"included_slices": 1, "excluded_slices": 0})


class StrategyPipelineTests(unittest.TestCase):
    def test_data_mart_builds_from_architecture_export(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            arch = root / "architecture"
            out = root / "data_mart"
            write_architecture_dataset(arch)
            manifest = build_data_mart(architecture_root=arch, output_root=out)
            self.assertEqual(manifest["mint_rows"], 1)
            self.assertTrue((out / "strategy_data_mart_manifest.json").exists())

    def test_backtest_replay_tuning_paper_live_wallet_profitability_commands_block(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            pipeline = Path(td)
            write_pipeline_readiness(pipeline)
            commands = [
                [sys.executable, str(SCRIPT_DIR / "run_strategy_backtest.py"), "--strategy", "candidate_eligibility_v2", "--pipeline-root", str(pipeline)],
                [sys.executable, str(SCRIPT_DIR / "run_strategy_replay.py"), "--pipeline-root", str(pipeline)],
                [sys.executable, str(SCRIPT_DIR / "run_strategy_threshold_tuning.py"), "--pipeline-root", str(pipeline)],
                [sys.executable, str(SCRIPT_DIR / "run_strategy_paper.py"), "--pipeline-root", str(pipeline)],
                [sys.executable, str(SCRIPT_DIR / "run_strategy_live.py"), "--pipeline-root", str(pipeline)],
                [sys.executable, str(SCRIPT_DIR / "run_strategy_wallet.py")],
                [sys.executable, str(SCRIPT_DIR / "check_profitability_claim_gate.py"), "--pipeline-root", str(pipeline)],
            ]
            for command in commands:
                proc = subprocess.run(command, cwd=REPO, text=True, capture_output=True)
                self.assertNotEqual(proc.returncode, 0, command)
                payload = json.loads(proc.stdout)
                self.assertFalse(payload["allowed"])
                self.assertNotIn("submit_order", payload.get("allowed_actions", []))

    def test_candidate_checkpoint_is_not_replay_eligibility(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_data_mart(root, [label_row(final_outcome="candidate_checkpoint", candidate_checkpoint_seen="true", replay_eligible="false", clean_negative_label="false")], [feature_row()])
            summary = PipelineLabelStore(root).summary()
            self.assertEqual(summary["candidate_checkpoint"], 1)
            self.assertEqual(summary["replay_eligible"], 0)

    def test_terminal_inconclusive_is_censored_not_dead(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_data_mart(root, [label_row(final_outcome="terminal_inconclusive", clean_negative_label="false", censored_label="true")], [feature_row()])
            self.assertTrue(PipelineLabelStore(root).validate()["passed"])
            self.assertEqual(PipelineLabelStore(root).summary()["censored"], 1)

    def test_r2_success_cannot_override_blockers(self) -> None:
        readiness = {"backtesting_ready": False, "replay_ready": False, "reason_codes": ["no_clean_positives"], "r2_verified": True}
        self.assertFalse(backtest_gate(readiness).allowed)
        self.assertFalse(replay_gate(readiness).allowed)

    def test_holder_rpc_and_rpc_supply_are_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_data_mart(root, [label_row()], [feature_row(holder_rpc_used="true", rpc_mint_supply_canonical="true")])
            result = PipelineFeatureStore(root).validate()
            self.assertFalse(result["passed"])
            self.assertTrue(any("holder_rpc_used" in blocker for blocker in result["blockers"]))
            self.assertTrue(any("rpc_mint_supply_canonical" in blocker for blocker in result["blockers"]))

    def test_asof_features_reject_future_data_and_forbidden_columns(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_data_mart(root, [label_row()], [feature_row(feature_asof_timestamp="2026-06-18T00:02:01Z", final_outcome="dead")])
            result = PipelineFeatureStore(root).validate()
            self.assertFalse(result["passed"])
            self.assertTrue(any("post_horizon_timestamp" in blocker for blocker in result["blockers"]))
            self.assertTrue(any("forbidden_alpha_columns" in blocker for blocker in result["blockers"]))

    def test_provider_and_artifact_fields_are_not_alpha_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_data_mart(root, [label_row()], [feature_row(r2_verified="true", artifact_consistency_ok="true")])
            result = PipelineFeatureStore(root).validate()
            self.assertFalse(result["passed"])
            self.assertTrue(any("forbidden_alpha_columns" in blocker for blocker in result["blockers"]))

    def test_random_split_and_duplicate_mint_are_rejected(self) -> None:
        random_split = {"method": "random", "embargo_rows": 5, "train": ["m1"], "validation": [], "test": []}
        self.assertFalse(validate_splits(random_split)["passed"])
        duplicate = {"method": "chronological_walk_forward", "embargo_rows": 5, "train": ["m1"], "validation": ["m1"], "test": []}
        self.assertFalse(validate_splits(duplicate)["passed"])

    def test_profitability_report_blocks_forbidden_claims(self) -> None:
        gate = profitability_claim_gate({"profitability_claim_allowed": False, "reason_codes": []})
        self.assertFalse(gate.allowed)
        self.assertFalse(report_text_allowed("this is a proven profitable buy signal", gate)["passed"])
        self.assertTrue(report_text_allowed("research-only hypothesis needs validation", gate)["passed"])

    def test_live_trading_cannot_be_enabled_by_config_alone(self) -> None:
        registry = strategy_registry()
        registry["strategies"][0]["allow_live_trade"] = True
        self.assertFalse(validate_registry(registry)["passed"])
        self.assertFalse(live_trading_gate({"live_trading_ready": True}).allowed)


if __name__ == "__main__":
    unittest.main()
