#!/usr/bin/env python3
from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
import sys

sys.path.insert(0, str(SCRIPT_DIR))

from strategy_pipeline.early_burst import build_early_burst_research, score_feature_row
from strategy_pipeline.feature_store import PipelineFeatureStore
from strategy_pipeline.io import read_json, write_csv
from strategy_pipeline.registry import strategy_registry, validate_registry
from strategy_pipeline.schemas import HORIZONS


def feature_row(**overrides: str) -> dict[str, str]:
    row = {
        "mint": "mintA",
        "slice_id": "slice1",
        "segment_id": "1",
        "relay_session_id": "relay1",
        "horizon_seconds": "5",
        "feature_asof_timestamp": "2026-06-18T00:00:05Z",
        "mint_first_seen_timestamp": "2026-06-18T00:00:00Z",
        "horizon_reached": "true",
        "data_complete_for_horizon": "true",
        "data_quality_exclusion": "false",
        "provider_gap_exposed": "false",
        "relay_gap_exposed": "false",
        "terminal_inconclusive_before_horizon": "false",
        "rejected_before_horizon": "false",
        "curve_progress_proxy_asof": "44",
        "buy_count_delta_asof": "5",
        "sell_count_delta_asof": "1",
        "net_buy_sell_delta_asof": "4",
        "volume_delta_asof": "100",
        "new_holder_count_delta_asof": "4",
        "top_holder_concentration_asof": "0.4",
        "dev_or_creator_holding_proxy_asof": "0",
        "holder_rpc_used": "false",
        "rpc_mint_supply_canonical": "false",
        "threshold_tuning_allowed": "false",
        "live_trading_enabled": "false",
    }
    row.update(overrides)
    return row


def positive_row(**overrides: str) -> dict[str, str]:
    row = {
        "mint": "mintA",
        "slice_id": "slice1",
        "segment_id": "1",
        "relay_session_id": "relay1",
        "decision_horizon_seconds": "5",
        "forward_window_seconds": "60",
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
        "positive_outcome_reason_codes": "curve_progress|buy_followthrough",
        "curve_progress_proxy_start": "10",
        "curve_progress_proxy_end": "80",
        "curve_progress_proxy_max": "90",
        "liquidity_delta_forward": "0",
        "reserve_delta_forward": "0",
        "volume_delta_forward": "100",
        "buy_sell_delta_forward": "7",
        "holder_growth_forward": "5",
        "holder_concentration_risk_forward": "0",
        "max_adverse_proxy": "2",
        "max_favorable_proxy": "30",
        "outcome_known_at_end_only": "true",
        "allowed_for_alpha_features": "false",
    }
    row.update(overrides)
    return row


def write_architecture_files(root: Path) -> None:
    candidate = {
        "strategy_name": "candidate_eligibility_v2",
        "strategy_version": "v2",
        "mint": "mintA",
        "horizon_seconds": "60",
        "decision": "not_eligible",
        "score_optional": "",
        "confidence_bin": "MISSING",
        "reason_codes": "candidate_checkpoint_absent|replay_not_countability_allowed",
        "feature_snapshot_hash": "abc",
        "data_quality_status": "clean",
        "allowed_actions": "research_report",
        "blocked_actions": "replay|backtesting|threshold_tuning|paper_trading|live_trading|wallet_execution",
        "explanation": "not replay eligible",
    }
    common_score = {
        "strategy_name": "",
        "strategy_version": "",
        "mint": "mintA",
        "horizon_seconds": "60",
        "decision": "",
        "score_optional": "",
        "confidence_bin": "LOW",
        "reason_codes": "",
        "feature_snapshot_hash": "abc",
        "data_quality_status": "clean",
        "allowed_actions": "research_report",
        "blocked_actions": "replay|backtesting|threshold_tuning|paper_trading|live_trading|wallet_execution",
        "explanation": "",
    }
    write_csv(root / "candidate_eligibility_v2_scores.csv", [candidate], list(candidate.keys()))
    early = {**common_score, "strategy_name": "early_avoid_v1", "decision": "continue_tracking"}
    cont = {**common_score, "strategy_name": "continue_tracking_v1", "decision": "continue_tracking"}
    write_csv(root / "early_avoid_filter_v1_scores.csv", [early], list(early.keys()))
    write_csv(root / "continue_tracking_gate_v1_scores.csv", [cont], list(cont.keys()))
    buy = {
        "mint": "mintA",
        "setup_decision": "disabled_research_only",
        "disabled_by_default": "true",
        "trade_action": "none",
        "gate_decision": "blocked",
        "reason_codes": "backtesting_not_ready",
        "required_evidence_before_activation": "clean_positives|replay_permission|operator_approval",
        "blocked_actions": "replay|backtesting|threshold_tuning|paper_trading|live_trading|wallet_execution",
    }
    write_csv(root / "buy_setup_draft_v0_scores.csv", [buy], list(buy.keys()))
    (root / "feature_store_report.md").write_text("# Feature Store\n")
    (root / "label_store_report.md").write_text("# Label Store\n")
    (root / "candidate_eligibility_v2_report.md").write_text("# Candidate Eligibility v2\n")


def write_data_mart(root: Path) -> None:
    for horizon in HORIZONS:
        rows = [feature_row(horizon_seconds=str(horizon))]
        write_csv(root / f"strategy_asof_features_{horizon:03d}s.csv", rows, list(rows[0].keys()))


class EarlyBurstStrategyTests(unittest.TestCase):
    def test_early_burst_setup_emits_no_trade_actions(self) -> None:
        row = score_feature_row(feature_row())
        self.assertEqual(row["decision"], "early_burst_watch")
        self.assertEqual(row["trade_action"], "none")
        self.assertEqual(row["allowed_actions"], "research_report")
        for forbidden in ("replay", "backtesting", "threshold_tuning", "paper_trading", "live_trading", "wallet_execution"):
            self.assertIn(forbidden, row["blocked_actions"])

    def test_early_burst_setup_uses_no_final_outcome_inputs(self) -> None:
        clean = score_feature_row(feature_row())
        leaking = score_feature_row(feature_row(final_outcome="early_rejected_dead", positive_outcome_label="high_positive", replay_eligible="true"))
        self.assertEqual(clean["decision"], leaking["decision"])
        self.assertEqual(clean["reason_codes"], leaking["reason_codes"])
        self.assertEqual(leaking["uses_final_outcome_inputs"], "false")

    def test_positive_labels_are_rejected_as_alpha_feature_columns(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            row = feature_row(positive_outcome_label="high_positive")
            write_csv(root / "strategy_asof_features_005s.csv", [row], list(row.keys()))
            for horizon in HORIZONS:
                if horizon != 5:
                    write_csv(root / f"strategy_asof_features_{horizon:03d}s.csv", [], list(row.keys()))
            result = PipelineFeatureStore(root).validate()
            self.assertFalse(result["passed"])
            self.assertTrue(any("forbidden_alpha_columns" in blocker for blocker in result["blockers"]))

    def test_high_positive_review_does_not_imply_replay_or_loosen_candidate_gate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out = root / "out"
            arch = root / "arch"
            mart = root / "mart"
            write_architecture_files(arch)
            write_data_mart(mart)
            row = positive_row()
            write_csv(out / "positive_outcome_labels.csv", [row], list(row.keys()))
            summary = build_early_burst_research(output_root=out, data_mart_root=mart, architecture_root=arch)
            self.assertTrue(summary["early_burst_strategy_research_ready"])
            self.assertFalse(summary["replay_ready"])
            self.assertFalse(summary["backtesting_ready"])
            with (out / "positive_high_positive_mint_review.csv").open() as handle:
                review_rows = list(csv.DictReader(handle))
            self.assertEqual(review_rows[0]["early_burst_research_class"], "HIGH_POSITIVE_THEN_DEAD")
            self.assertEqual(review_rows[0]["replay_eligible"], "false")
            self.assertEqual(review_rows[0]["candidate_checkpoint_seen"], "false")
            self.assertIn("candidate_checkpoint_absent", review_rows[0]["candidate_reason_codes"])

    def test_early_burst_research_pack_keeps_forbidden_gates_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out = root / "out"
            arch = root / "arch"
            mart = root / "mart"
            write_architecture_files(arch)
            write_data_mart(mart)
            row = positive_row()
            write_csv(out / "positive_outcome_labels.csv", [row], list(row.keys()))
            summary = build_early_burst_research(output_root=out, data_mart_root=mart, architecture_root=arch)
            pack = Path(summary["gpt_pack_path"])
            self.assertTrue((pack / "GPT_EARLY_BURST_STRATEGY_PROMPT.md").exists())
            prompt = (pack / "GPT_EARLY_BURST_STRATEGY_PROMPT.md").read_text()
            self.assertIn("Do not claim profitability", prompt)
            self.assertIn("Do not output trade entries", prompt)
            readiness = read_json(out / "early_burst_research_summary.json")
            self.assertFalse(readiness["profitability_claim_allowed"])
            self.assertFalse(readiness["threshold_tuning_ready"])
            self.assertFalse(readiness["live_trading_ready"])

    def test_holder_rpc_and_canonical_supply_remain_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            row = feature_row(holder_rpc_used="true", rpc_mint_supply_canonical="true")
            for horizon in HORIZONS:
                write_csv(root / f"strategy_asof_features_{horizon:03d}s.csv", [dict(row, horizon_seconds=str(horizon))], list(row.keys()))
            result = PipelineFeatureStore(root).validate()
            self.assertFalse(result["passed"])
            self.assertTrue(any("holder_rpc_used" in blocker for blocker in result["blockers"]))
            self.assertTrue(any("rpc_mint_supply_canonical" in blocker for blocker in result["blockers"]))

    def test_early_burst_registry_is_disabled_for_execution(self) -> None:
        registry = strategy_registry()
        early_burst = [entry for entry in registry["strategies"] if entry["name"] == "early_burst_setup_v0"][0]
        self.assertEqual(early_burst["execution_mode"], "research_only_disabled")
        self.assertFalse(early_burst["allow_backtest"])
        self.assertFalse(early_burst["allow_live_trade"])
        self.assertFalse(early_burst["wallet_execution"])
        self.assertTrue(validate_registry(registry)["passed"])


if __name__ == "__main__":
    unittest.main()
