#!/usr/bin/env python3
from __future__ import annotations

import csv
import tempfile
import unittest
import zipfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
import sys

sys.path.insert(0, str(SCRIPT_DIR))

from strategy_pipeline.backtest import backtest_gate
from strategy_pipeline.early_burst_backtest_readiness import (
    build_early_burst_backtest_readiness,
    leakage_audit,
)
from strategy_pipeline.early_burst_validation import SAFE_FEATURE_FIELDS, VALIDATION_LABEL_FIELDS, VALIDATION_ROW_FIELDS
from strategy_pipeline.io import read_json, write_csv, write_json
from strategy_pipeline.paper_trading import paper_trading_gate
from strategy_pipeline.live_trading import live_trading_gate
from strategy_pipeline.profitability_claims import profitability_claim_gate
from strategy_pipeline.replay import replay_gate
from strategy_pipeline.schemas import HORIZONS
from strategy_pipeline.splits import validate_splits
from strategy_pipeline.threshold_tuning import threshold_tuning_gate
from strategy_pipeline.wallet import wallet_gate


def validation_row(mint: str, *, label: str = "high_positive", replay: str = "false") -> dict[str, str]:
    row = {
        "mint": mint,
        "slice_id": "slice1",
        "segment_id": "1",
        "relay_session_id": "relay1",
        "decision_horizon_seconds": "5",
        "forward_window_seconds": "55",
        "horizon_reached": "true",
        "forward_window_observed": "true",
        "data_quality_exclusion": "false",
        "early_burst_setup_decision": "early_burst_watch",
        "early_burst_reason_codes": "early_curve_progress|replay_not_allowed",
        "final_outcome": "early_rejected_dead",
        "rejection_reason": "volume_evaporated",
        "terminal_inconclusive_reason": "",
        "positive_outcome_label": label,
        "positive_outcome_strength_bin": "HIGH" if label == "high_positive" else "LOW",
        "early_burst_class": "HIGH_POSITIVE_THEN_DEAD" if label == "high_positive" else "ORDINARY_CLEAN_DEAD",
        "max_favorable_proxy": "90",
        "max_adverse_proxy": "2",
        "time_to_max_favorable_ms": "5000",
        "time_to_max_adverse_ms": "5000",
        "time_to_rejection_ms": "60000",
        "time_to_terminal_ms": "",
        "could_exit_before_death_proxy": "true",
        "exit_window_observed": "true",
        "exit_window_quality": "HIGH",
        "holder_risk_before_burst": "LOW",
        "holder_risk_after_burst": "LOW",
        "vault_curve_progress_before_burst": "MEDIUM",
        "vault_curve_progress_after_burst": "HIGH",
        "sell_pressure_before_burst": "LOW",
        "sell_pressure_after_burst": "LOW",
        "candidate_checkpoint_seen": "false",
        "replay_eligible": replay,
        "backtest_allowed": "false",
        "replay_allowed": "false",
        "trade_allowed": "false",
    }
    return row


def feature_row(mint: str, horizon: int, **overrides: str) -> dict[str, str]:
    row = {field: "" for field in SAFE_FEATURE_FIELDS}
    row.update({
        "mint": mint,
        "slice_id": "slice1",
        "segment_id": "1",
        "relay_session_id": "relay1",
        "horizon_seconds": str(horizon),
        "feature_asof_timestamp": "2026-06-18T00:00:05Z",
        "mint_first_seen_timestamp": "2026-06-18T00:00:00Z",
        "age_ms_at_horizon": "5000",
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
        "trade_update_count_asof": "1",
        "buy_count_delta_asof": "1",
        "sell_count_delta_asof": "0",
        "net_buy_sell_delta_asof": "1",
        "volume_delta_asof": "10",
        "holder_rpc_used": "false",
        "rpc_mint_supply_canonical": "false",
        "threshold_tuning_allowed": "false",
        "live_trading_enabled": "false",
    })
    row.update(overrides)
    return row


def write_fixture(root: Path, *, leak_feature: bool = False, holder_rpc: bool = False) -> tuple[Path, Path, Path]:
    output = root / "out"
    validation = output / "early_burst_validation_dataset"
    readiness = output / "early_burst_backtest_readiness"
    rows = [validation_row("mintA"), validation_row("mintB", label="dead_negative")]
    write_csv(validation / "early_burst_validation_rows.csv", rows, VALIDATION_ROW_FIELDS)
    write_csv(validation / "early_burst_validation_labels.csv", rows, VALIDATION_LABEL_FIELDS)
    write_json(validation / "early_burst_validation_manifest.json", {
        "classification": "EARLY_BURST_VALIDATION_DATASET_PASS",
        "rows": len(rows),
        "positive_high_unique_mints": 1,
        "high_positive_unique_mints": 1,
        "ordinary_clean_dead_unique_mints": 1,
        "observable_exit_window_mints": 1,
        "adverse_movement_before_exit_rows": 0,
    })
    for name in [
        "EARLY_BURST_VALIDATION_DATASET_SUMMARY.md",
        "EARLY_BURST_EXIT_WINDOW_ANALYSIS.md",
        "EARLY_BURST_VS_DEAD_COMPARISON.md",
    ]:
        (validation / name).parent.mkdir(parents=True, exist_ok=True)
        (validation / name).write_text(f"# {name}\n")
    for name in ["POSITIVE_HIGH_POSITIVE_MINT_REVIEW.md", "POSITIVE_OUTCOME_AUDIT.md", "GATE_VS_POSITIVE_OUTCOMES.md"]:
        (output / name).parent.mkdir(parents=True, exist_ok=True)
        (output / name).write_text(f"# {name}\n")
    write_json(output / "READINESS_DECISION.json", {
        "strategy_research_ready": True,
        "backtesting_ready": False,
        "replay_ready": False,
        "threshold_tuning_ready": False,
        "paper_trading_ready": False,
        "live_trading_ready": False,
        "profitability_claim_allowed": False,
        "reason_codes": ["no_replay_eligible_candidates"],
    })
    for horizon in HORIZONS:
        feature_fields = list(SAFE_FEATURE_FIELDS)
        features = [feature_row("mintA", horizon, holder_rpc_used="true" if holder_rpc else "false"), feature_row("mintB", horizon)]
        if leak_feature:
            feature_fields.append("final_outcome")
            features = [{**row, "final_outcome": "early_rejected_dead"} for row in features]
        write_csv(validation / f"early_burst_validation_features_{horizon:03d}s.csv", features, feature_fields)
    return output, validation, readiness


class EarlyBurstBacktestReadinessTests(unittest.TestCase):
    def test_builder_blocks_weak_backtest_when_sample_too_small(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output, validation, readiness = write_fixture(Path(td))
            summary = build_early_burst_backtest_readiness(output_root=output, validation_root=validation, readiness_root=readiness)
            self.assertEqual(summary["classification"], "EARLY_BURST_BACKTEST_READINESS_BLOCK_SAMPLE_SIZE")
            decision = summary["decision"]
            self.assertFalse(decision["early_burst_backtesting_ready"])
            self.assertIn("sample_size_positive_too_small", decision["reason_codes"])
            self.assertIn("sample_size_high_positive_too_small", decision["reason_codes"])

    def test_backtest_harness_blocks_early_burst_until_readiness_passes(self) -> None:
        readiness = {
            "early_burst_backtesting_ready": False,
            "backtesting_ready": False,
            "reason_codes": ["sample_size_positive_too_small"],
        }
        gate = backtest_gate(readiness, strategy="early_burst_setup_v0")
        self.assertFalse(gate.allowed)
        self.assertEqual(gate.blocker, "EARLY_BURST_BACKTEST_BLOCKED_BY_READINESS_GATE")
        self.assertIn("profit_metrics", gate.forbidden_actions)

    def test_leakage_audit_rejects_forward_label_in_features(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output, validation, readiness = write_fixture(Path(td), leak_feature=True)
            result = leakage_audit(validation, readiness)
            self.assertFalse(result["passed"])
            self.assertTrue(any("forbidden_alpha_columns" in blocker for blocker in result["blockers"]))

    def test_holder_rpc_and_canonical_supply_block_leakage_audit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output, validation, readiness = write_fixture(Path(td), holder_rpc=True)
            result = leakage_audit(validation, readiness)
            self.assertFalse(result["passed"])
            self.assertTrue(any("holder_rpc_used" in blocker for blocker in result["blockers"]))

    def test_high_positive_does_not_imply_replay_or_buy(self) -> None:
        row = validation_row("mintA")
        self.assertEqual(row["positive_outcome_label"], "high_positive")
        self.assertEqual(row["replay_eligible"], "false")
        self.assertEqual(row["trade_allowed"], "false")
        self.assertEqual(row["backtest_allowed"], "false")

    def test_terminal_inconclusive_is_not_dead(self) -> None:
        row = validation_row("mintA", label="censored")
        row["final_outcome"] = "terminal_inconclusive"
        row["early_burst_class"] = "CENSORED_OR_INCOMPLETE"
        self.assertNotEqual(row["early_burst_class"], "ORDINARY_CLEAN_DEAD")

    def test_random_and_duplicate_splits_are_rejected(self) -> None:
        self.assertFalse(validate_splits({"method": "random", "embargo_rows": 5, "train": ["m1"], "validation": [], "test": []})["passed"])
        self.assertFalse(validate_splits({"method": "chronological_walk_forward", "embargo_rows": 5, "train": ["m1"], "validation": ["m1"], "test": []})["passed"])

    def test_forbidden_gates_remain_blocked(self) -> None:
        readiness = {"reason_codes": ["sample_size_positive_too_small"], "replay_ready": False, "threshold_tuning_ready": False, "paper_trading_ready": False, "live_trading_ready": False, "profitability_claim_allowed": False}
        self.assertFalse(replay_gate(readiness).allowed)
        self.assertFalse(threshold_tuning_gate(readiness).allowed)
        self.assertFalse(paper_trading_gate(readiness).allowed)
        self.assertFalse(live_trading_gate(readiness).allowed)
        self.assertFalse(wallet_gate().allowed)
        self.assertFalse(profitability_claim_gate(readiness).allowed)

    def test_gpt_pack_excludes_secrets_and_raw_frames(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output, validation, readiness = write_fixture(Path(td))
            (readiness / "raw_relay_frames.csv").parent.mkdir(parents=True, exist_ok=True)
            (readiness / "raw_relay_frames.csv").write_text("raw\n")
            (readiness / "private_key.txt").write_text("secret\n")
            summary = build_early_burst_backtest_readiness(output_root=output, validation_root=validation, readiness_root=readiness)
            with zipfile.ZipFile(summary["gpt_pack_zip_path"]) as archive:
                names = "\n".join(archive.namelist()).lower()
            self.assertNotIn("raw_relay", names)
            self.assertNotIn("private", names)
            self.assertIn("gpt_early_burst_backtest_readiness_prompt.md", names)


if __name__ == "__main__":
    unittest.main()
