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

from strategy_pipeline.early_burst_validation import build_early_burst_validation_dataset
from strategy_pipeline.io import read_json, write_csv, write_json
from strategy_pipeline.schemas import HORIZONS


def label_row(**overrides: str) -> dict[str, str]:
    row = {
        "mint": "mintA",
        "batch_id": "batch1",
        "slice_id": "slice1",
        "segment_id": "1",
        "relay_session_id": "relay1",
        "first_seen_at": "2026-06-18T00:00:00Z",
        "created_at": "2026-06-18T00:00:00Z",
        "final_outcome": "early_rejected_dead",
        "final_outcome_reason": "volume_evaporated",
        "rejection_reason": "volume_evaporated",
        "terminal_inconclusive_reason": "",
        "time_to_rejection_ms": "60000",
        "time_to_terminal_ms": "",
        "provider_gap_exposed": "false",
        "relay_gap_exposed": "false",
        "sequence_gap_exposed": "false",
        "hash_mismatch_exposed": "false",
        "receiver_backpressure_exposed": "false",
        "high_throughput_mint": "false",
        "degraded_active_mint": "false",
        "degraded_reason": "",
        "candidate_checkpoint_seen": "false",
        "replay_eligible": "false",
        "clean_negative_label": "true",
        "clean_positive_label": "false",
        "censored_label": "false",
        "label_quality": "clean",
        "source_artifacts": "attempt_ledger.csv|rejected_summary.csv",
    }
    row.update(overrides)
    return row


def feature_row(horizon: int, **overrides: str) -> dict[str, str]:
    row = {
        "mint": "mintA",
        "slice_id": "slice1",
        "segment_id": "1",
        "relay_session_id": "relay1",
        "horizon_seconds": str(horizon),
        "feature_asof_timestamp": f"2026-06-18T00:00:{horizon:02d}Z" if horizon < 60 else "2026-06-18T00:01:00Z",
        "mint_first_seen_timestamp": "2026-06-18T00:00:00Z",
        "age_ms_at_horizon": str(horizon * 1000),
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
        "trade_update_count_asof": "10",
        "transaction_active_mint_count_asof": "10",
        "pump_trade_active_mint_count_asof": "10",
        "buy_count_delta_asof": "6",
        "sell_count_delta_asof": "1",
        "net_buy_sell_delta_asof": "5",
        "volume_delta_asof": "100",
        "unique_trade_accounts_asof": "5",
        "last_trade_age_ms_asof": "1000",
        "trade_burst_score_asof": "0.8",
        "trade_direction_imbalance_asof": "0.6",
        "holder_update_count_asof": "4",
        "unique_holder_accounts_seen_asof": "6",
        "top_holder_concentration_asof": "0.4",
        "dev_or_creator_holding_proxy_asof": "0",
        "holder_churn_proxy_asof": "0",
        "holder_collapse_proxy_asof": "false",
        "new_holder_count_delta_asof": "4",
        "exiting_holder_count_delta_asof": "0",
        "vault_update_count_asof": "1",
        "bonding_curve_update_count_asof": "1",
        "liquidity_delta_asof": "2",
        "reserve_delta_asof": "2",
        "curve_progress_proxy_asof": "50",
        "liquidity_exit_proxy_asof": "0",
        "price_or_curve_move_proxy_asof": "1",
        "holder_rpc_used": "false",
        "rpc_mint_supply_canonical": "false",
        "threshold_tuning_allowed": "false",
        "live_trading_enabled": "false",
    }
    row.update(overrides)
    return row


def outcome_row(**overrides: str) -> dict[str, str]:
    row = {
        "mint": "mintA",
        "slice_id": "slice1",
        "segment_id": "1",
        "relay_session_id": "relay1",
        "decision_horizon_seconds": "5",
        "forward_window_seconds": "55",
        "horizon_reached": "true",
        "forward_window_observed": "true",
        "data_quality_exclusion": "false",
        "final_outcome": "early_rejected_dead",
        "rejection_reason": "volume_evaporated",
        "terminal_inconclusive_reason": "",
        "censored_label": "false",
        "clean_negative_label": "true",
        "clean_positive_candidate_label": "false",
        "candidate_checkpoint_seen": "false",
        "replay_eligible": "false",
        "positive_outcome_label": "high_positive",
        "positive_outcome_strength_bin": "HIGH",
        "positive_outcome_basis": "stream_authoritative_proxy_bins",
        "positive_outcome_reason_codes": "fixed_bin_high_positive",
        "curve_progress_proxy_start": "10",
        "curve_progress_proxy_end": "80",
        "curve_progress_proxy_max": "90",
        "liquidity_delta_forward": "1",
        "reserve_delta_forward": "1",
        "volume_delta_forward": "100",
        "buy_sell_delta_forward": "8",
        "holder_growth_forward": "5",
        "holder_concentration_risk_forward": "0",
        "max_adverse_proxy": "2",
        "max_favorable_proxy": "90",
        "outcome_known_at_end_only": "true",
        "allowed_for_alpha_features": "false",
    }
    row.update(overrides)
    return row


def score_row(**overrides: str) -> dict[str, str]:
    row = {
        "strategy_name": "early_burst_setup_v0",
        "strategy_version": "v0",
        "mint": "mintA",
        "horizon_seconds": "5",
        "decision": "early_burst_watch",
        "confidence_bin": "LOW",
        "reason_codes": "early_curve_progress|early_volume_followthrough|replay_not_allowed",
        "allowed_actions": "research_report",
        "blocked_actions": "replay|backtesting|threshold_tuning|paper_trading|live_trading|wallet_execution",
        "trade_action": "none",
        "uses_final_outcome_inputs": "false",
        "explanation": "research only",
    }
    row.update(overrides)
    return row


def review_row(**overrides: str) -> dict[str, str]:
    row = {
        "mint": "mintA",
        "best_outcome_label": "high_positive",
        "best_strength_bin": "HIGH",
        "positive_rows": "0",
        "high_positive_rows": "1",
        "first_positive_horizon": "5",
        "last_positive_horizon": "5",
        "max_curve_progress_end": "80",
        "max_curve_progress_proxy": "90",
        "max_buy_sell_delta": "8",
        "max_holder_growth": "5",
        "final_outcome": "early_rejected_dead",
        "rejection_reason": "volume_evaporated",
        "candidate_checkpoint_seen": "false",
        "replay_eligible": "false",
        "candidate_eligibility_decision": "not_eligible",
        "first_failed_candidate_gate": "candidate_checkpoint_absent",
        "candidate_reason_codes": "candidate_checkpoint_absent|replay_not_countability_allowed",
        "data_quality_status": "clean",
        "censored_status": "not_censored",
        "why_not_candidate": "volume_evaporated",
        "early_burst_research_class": "HIGH_POSITIVE_THEN_DEAD",
    }
    row.update(overrides)
    return row


def write_fixture(root: Path) -> tuple[Path, Path, Path, Path]:
    output = root / "out"
    mart = root / "mart"
    arch = root / "arch"
    validation = root / "validation"
    outcome_fields = list(outcome_row().keys())
    write_csv(output / "positive_outcome_labels.csv", [outcome_row()], outcome_fields)
    write_csv(output / "early_burst_setup_v0_scores.csv", [score_row()], list(score_row().keys()))
    write_csv(output / "positive_high_positive_mint_review.csv", [review_row()], list(review_row().keys()))
    for name in [
        "POSITIVE_HIGH_POSITIVE_MINT_REVIEW.md",
        "EARLY_BURST_STRATEGY_FAMILY_DIAGNOSTICS.md",
        "EARLY_BURST_SETUP_V0_REPORT.md",
        "EARLY_BURST_EXIT_RISK_DRAFT.md",
    ]:
        (output / name).parent.mkdir(parents=True, exist_ok=True)
        (output / name).write_text(f"# {name}\n")
    write_json(output / "READINESS_DECISION.json", {
        "strategy_research_ready": True,
        "buy_strategy_architecture_ready": True,
        "trading_strategy_pipeline_ready": True,
        "backtesting_ready": False,
        "replay_ready": False,
        "threshold_tuning_ready": False,
        "paper_trading_ready": False,
        "live_trading_ready": False,
        "wallet_execution_ready": False,
        "profitability_claim_allowed": False,
        "reason_codes": ["no_replay_eligible_candidates"],
    })
    label = label_row()
    write_csv(mart / "strategy_labels.csv", [label], list(label.keys()))
    for horizon in HORIZONS:
        row = feature_row(horizon, curve_progress_proxy_asof=str(20 + horizon))
        write_csv(mart / f"strategy_asof_features_{horizon:03d}s.csv", [row], list(row.keys()))
    for name in ["feature_store_report.md", "label_store_report.md", "candidate_eligibility_v2_report.md"]:
        (arch / name).parent.mkdir(parents=True, exist_ok=True)
        (arch / name).write_text(f"# {name}\n")
    return output, mart, arch, validation


class EarlyBurstValidationTests(unittest.TestCase):
    def test_validation_features_do_not_contain_forward_labels(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output, mart, arch, validation = write_fixture(Path(td))
            build_early_burst_validation_dataset(output_root=output, data_mart_root=mart, architecture_root=arch, validation_root=validation)
            header = (validation / "early_burst_validation_features_005s.csv").read_text().splitlines()[0].split(",")
            forbidden = {
                "final_outcome",
                "rejection_reason",
                "candidate_checkpoint_seen",
                "replay_eligible",
                "positive_outcome_label",
                "max_favorable_proxy",
                "max_adverse_proxy",
            }
            self.assertFalse(forbidden.intersection(header))

    def test_forward_labels_are_kept_separate_and_gates_remain_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output, mart, arch, validation = write_fixture(Path(td))
            summary = build_early_burst_validation_dataset(output_root=output, data_mart_root=mart, architecture_root=arch, validation_root=validation)
            with (validation / "early_burst_validation_labels.csv").open() as handle:
                labels = list(csv.DictReader(handle))
            self.assertEqual(labels[0]["positive_outcome_label"], "high_positive")
            self.assertEqual(labels[0]["replay_eligible"], "false")
            self.assertEqual(labels[0]["replay_allowed"], "false")
            self.assertFalse(summary["backtesting_ready"])
            self.assertFalse(summary["replay_ready"])
            self.assertFalse(summary["threshold_tuning_ready"])
            self.assertFalse(summary["live_trading_ready"])
            self.assertFalse(summary["profitability_claim_allowed"])
            readiness = read_json(output / "READINESS_DECISION.json")
            self.assertTrue(readiness["early_burst_validation_dataset_ready"])
            self.assertFalse(readiness["backtesting_ready"])

    def test_terminal_inconclusive_remains_censored(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output, mart, arch, validation = write_fixture(Path(td))
            write_csv(output / "positive_outcome_labels.csv", [outcome_row(
                positive_outcome_label="censored",
                censored_label="true",
                final_outcome="terminal_inconclusive",
                terminal_inconclusive_reason="still_active",
            )], list(outcome_row().keys()))
            summary = build_early_burst_validation_dataset(output_root=output, data_mart_root=mart, architecture_root=arch, validation_root=validation)
            self.assertEqual(summary["censored_rows"], 1)
            with (validation / "early_burst_validation_rows.csv").open() as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["exit_window_quality"], "CENSORED")
            self.assertEqual(rows[0]["trade_allowed"], "false")

    def test_pack_excludes_secrets_and_raw_relay_frames(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output, mart, arch, validation = write_fixture(Path(td))
            (output / "raw_relay_frames.csv").write_text("raw\n")
            (output / "private_key.txt").write_text("secret\n")
            summary = build_early_burst_validation_dataset(output_root=output, data_mart_root=mart, architecture_root=arch, validation_root=validation)
            with zipfile.ZipFile(summary["gpt_pack_zip_path"]) as archive:
                names = archive.namelist()
            joined = "\n".join(names).lower()
            self.assertNotIn("raw_relay", joined)
            self.assertNotIn("private", joined)
            self.assertIn("GPT_EARLY_BURST_VALIDATION_PROMPT.md", names)

    def test_holder_rpc_and_rpc_supply_remain_noncanonical(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output, mart, arch, validation = write_fixture(Path(td))
            build_early_burst_validation_dataset(output_root=output, data_mart_root=mart, architecture_root=arch, validation_root=validation)
            with (validation / "early_burst_validation_features_005s.csv").open() as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["holder_rpc_used"], "false")
            self.assertEqual(rows[0]["rpc_mint_supply_canonical"], "false")


if __name__ == "__main__":
    unittest.main()
