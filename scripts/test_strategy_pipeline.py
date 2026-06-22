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
from strategy_pipeline.dual_strategy_tracks import build_dual_strategy_tracks, score_early_burst_in_out, score_survivor_runner
from strategy_pipeline.early_burst_in_out_v1 import build_early_burst_in_out_v1, score_early_burst_in_out_v1
from strategy_pipeline.feature_store import PipelineFeatureStore
from strategy_pipeline.io import write_csv, write_json
from strategy_pipeline.label_store import PipelineLabelStore
from strategy_pipeline.live_trading import live_trading_gate
from strategy_pipeline.profitability_claims import profitability_claim_gate, report_text_allowed
from strategy_pipeline.promotion_priority import score_promotion_priority_v1
from strategy_pipeline.registry import strategy_registry, validate_registry
from strategy_pipeline.replay_non_eligibility import build_replay_non_eligibility_audit, classify_replay_blocker
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

    def test_promotion_priority_v1_shadow_emits_no_trade_or_replay_side_effects(self) -> None:
        score = score_promotion_priority_v1(
            {
                "horizon_reached": "true",
                "trade_burst_score_asof": "0.6",
                "buy_count_delta_asof": "3",
                "sell_count_delta_asof": "0",
                "net_buy_sell_delta_asof": "3",
                "volume_delta_asof": "6000000000",
                "curve_progress_proxy_asof": "60",
                "top_holder_concentration_asof": "0.55",
                "dev_or_creator_holding_proxy_asof": "0.03",
                "liquidity_exit_proxy_asof": "false",
                "holder_collapse_proxy_asof": "false",
            },
            dead_launch_avoider_decision="continue_observation",
        )
        self.assertEqual(score.decision, "promote_to_rich_tracking_research_only")
        self.assertEqual(score.trade_action, "none")
        self.assertFalse(score.candidate_eligible)
        self.assertFalse(score.replay_eligible)
        self.assertFalse(score.countability_affects)
        fields = score.as_shadow_fields()
        self.assertEqual(fields["promotion_priority_v1_shadow_only"], "true")
        self.assertEqual(fields["promotion_priority_v1_would_promote"], "true")

    def test_promotion_priority_v1_shadow_respects_data_quality_and_censored_rows(self) -> None:
        data_quality = score_promotion_priority_v1(
            {"horizon_reached": "true", "data_quality_exclusion": "true", "final_outcome": "early_rejected_dead"}
        )
        self.assertEqual(data_quality.decision, "audit_only")
        self.assertIn("data_quality_excluded", data_quality.reason_codes)
        censored = score_promotion_priority_v1(
            {"horizon_reached": "true", "final_outcome": "terminal_inconclusive"}
        )
        self.assertEqual(censored.decision, "censored")
        self.assertIn("terminal_inconclusive_censored", censored.reason_codes)

    def test_promotion_priority_v1_shadow_does_not_depend_on_future_outcome_label(self) -> None:
        base = {
            "horizon_reached": "true",
            "trade_burst_score_asof": "0.6",
            "buy_count_delta_asof": "3",
            "sell_count_delta_asof": "0",
            "net_buy_sell_delta_asof": "3",
            "volume_delta_asof": "6000000000",
            "curve_progress_proxy_asof": "60",
        }
        positive = score_promotion_priority_v1({**base, "positive_outcome_label": "high_positive"})
        dead = score_promotion_priority_v1({**base, "positive_outcome_label": "dead_negative"})
        self.assertEqual(positive.decision, dead.decision)
        self.assertEqual(positive.reason_codes, dead.reason_codes)

    def test_replay_non_eligibility_classifies_positive_without_candidate_checkpoint(self) -> None:
        blocker, root_cause, action = classify_replay_blocker(
            {
                "positive_outcome_label": "high_positive",
                "candidate_checkpoint_seen": "false",
                "replay_eligible": "false",
                "r2_verified": "true",
                "artifact_consistency_ok": "true",
                "counted_run": "true",
                "counted_segment": "true",
                "provider_gap_exposed": "false",
                "relay_gap_exposed": "false",
                "terminal_inconclusive": "false",
                "censored": "false",
                "cheap_followup_horizons_reached": "5|10|30|60|120|300",
            }
        )
        self.assertEqual(blocker, "positive_high_without_candidate_checkpoint")
        self.assertEqual(root_cause, "POSITIVE_OUTCOME_NOT_MATERIAL_CANDIDATE")
        self.assertIn("early_burst_replay_candidate", action)

    def test_replay_non_eligibility_keeps_terminal_inconclusive_censored(self) -> None:
        blocker, root_cause, _ = classify_replay_blocker(
            {
                "positive_outcome_label": "positive",
                "candidate_checkpoint_seen": "false",
                "replay_eligible": "false",
                "r2_verified": "true",
                "artifact_consistency_ok": "true",
                "counted_run": "true",
                "counted_segment": "true",
                "terminal_inconclusive": "true",
            }
        )
        self.assertEqual(blocker, "terminal_inconclusive_or_censored")
        self.assertEqual(root_cause, "TERMINAL_INCONCLUSIVE_CENSORED")

    def test_replay_non_eligibility_r2_blocker_precedes_candidate_policy(self) -> None:
        blocker, root_cause, _ = classify_replay_blocker(
            {
                "positive_outcome_label": "high_positive",
                "candidate_checkpoint_seen": "false",
                "replay_eligible": "false",
                "r2_verified": "false",
                "artifact_consistency_ok": "true",
                "counted_run": "true",
                "counted_segment": "true",
            }
        )
        self.assertEqual(blocker, "r2_or_artifact_blocker")
        self.assertEqual(root_cause, "R2_OR_ARTIFACT_BLOCKED")

    def test_replay_non_eligibility_audit_updates_readiness_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            pipeline = root / "pipeline"
            data_mart = pipeline / "data_mart"
            local = root / "local_stream_collector"
            out = pipeline / "replay_non_eligibility_audit"
            write_pipeline_readiness(pipeline)
            labels = [
                label_row(
                    mint="mint-positive",
                    final_outcome="early_rejected_dead",
                    clean_negative_label="false",
                    clean_positive_label="false",
                    censored_label="false",
                )
            ]
            write_data_mart(data_mart, labels, [feature_row(mint="mint-positive")])
            write_csv(
                pipeline / "positive_outcome_labels.csv",
                [
                    {
                        "mint": "mint-positive",
                        "slice_id": "slice1",
                        "segment_id": "segment1",
                        "relay_session_id": "relay1",
                        "positive_outcome_label": "high_positive",
                        "positive_outcome_strength_bin": "HIGH",
                        "candidate_checkpoint_seen": "false",
                        "replay_eligible": "false",
                        "final_outcome": "early_rejected_dead",
                        "rejection_reason": "volume_evaporated",
                    }
                ],
                [
                    "mint",
                    "slice_id",
                    "segment_id",
                    "relay_session_id",
                    "positive_outcome_label",
                    "positive_outcome_strength_bin",
                    "candidate_checkpoint_seen",
                    "replay_eligible",
                    "final_outcome",
                    "rejection_reason",
                ],
            )
            payload = build_replay_non_eligibility_audit(
                pipeline_root=pipeline,
                data_mart_root=data_mart,
                local_collector_root=local,
                output_root=out,
            )
            readiness = json.loads((pipeline / "READINESS_DECISION.json").read_text())
            self.assertEqual(payload["classification"], "REPLAY_NON_ELIGIBILITY_ROOT_CAUSE_PASS")
            self.assertTrue(readiness["replay_non_eligibility_audit_ready"])
            self.assertFalse(readiness["replay_ready"])
            self.assertFalse(readiness["backtesting_ready"])
            self.assertFalse(readiness["collection_allowed"])
            self.assertTrue((out / "replay_non_eligibility_audit_export.zip").exists())

    def test_dual_strategy_early_burst_requires_v1_review_signal_and_emits_no_trade(self) -> None:
        row = feature_row(
            horizon_seconds="60",
            horizon_reached="true",
            data_complete_for_horizon="true",
            curve_progress_proxy_asof="60",
            net_buy_sell_delta_asof="5",
            buy_count_delta_asof="5",
            sell_count_delta_asof="0",
            volume_delta_asof="1000000000",
            top_holder_concentration_asof="0.2",
            dev_or_creator_holding_proxy_asof="0.01",
            liquidity_exit_proxy_asof="false",
        )
        ctx = {
            "labels": {},
            "positives_by_horizon": {("mint1", "slice1", "segment1", "60"): {"forward_window_observed": "true"}},
            "positives_by_key": {},
            "v1_by_horizon": {("mint1", "slice1", "segment1", "60"): {"promotion_priority_v1_would_promote": "true"}},
            "exit_by_horizon": {},
        }
        score = score_early_burst_in_out(row, ctx)
        self.assertEqual(score["decision"], "early_burst_candidate_review")
        self.assertEqual(score["trade_action"], "none")
        self.assertEqual(score["replay_eligible"], "false")
        ctx["v1_by_horizon"][("mint1", "slice1", "segment1", "60")]["promotion_priority_v1_would_promote"] = "false"
        blocked = score_early_burst_in_out(row, ctx)
        self.assertEqual(blocked["decision"], "reject")
        self.assertIn("v1_not_promoted", blocked["reason_codes"])

    def test_dual_strategy_survivor_requires_long_horizon_and_emits_no_trade(self) -> None:
        base = feature_row(
            horizon_seconds="300",
            horizon_reached="true",
            data_complete_for_horizon="true",
            curve_progress_proxy_asof="75",
            new_holder_count_delta_asof="10",
            unique_holder_accounts_seen_asof="20",
            top_holder_concentration_asof="0.3",
            dev_or_creator_holding_proxy_asof="0.01",
            sell_count_delta_asof="0",
            liquidity_exit_proxy_asof="false",
        )
        ctx = {"labels": {}, "positives_by_horizon": {}, "positives_by_key": {}, "v1_by_horizon": {}, "exit_by_horizon": {}}
        score = score_survivor_runner(base, ctx)
        self.assertEqual(score["decision"], "survivor_candidate_review")
        self.assertEqual(score["trade_action"], "none")
        short = score_survivor_runner({**base, "horizon_seconds": "60"}, ctx)
        self.assertEqual(short["decision"], "insufficient_data")

    def test_dual_strategy_tracks_do_not_use_future_label_as_alpha_input(self) -> None:
        row = feature_row(
            horizon_seconds="60",
            horizon_reached="true",
            data_complete_for_horizon="true",
            curve_progress_proxy_asof="60",
            net_buy_sell_delta_asof="5",
            buy_count_delta_asof="5",
            sell_count_delta_asof="0",
            volume_delta_asof="1000000000",
            top_holder_concentration_asof="0.2",
            dev_or_creator_holding_proxy_asof="0.01",
            liquidity_exit_proxy_asof="false",
        )
        key = ("mint1", "slice1", "segment1", "60")
        ctx_positive = {
            "labels": {},
            "positives_by_horizon": {key: {"forward_window_observed": "true", "positive_outcome_label": "high_positive"}},
            "positives_by_key": {},
            "v1_by_horizon": {key: {"promotion_priority_v1_would_promote": "true"}},
            "exit_by_horizon": {},
        }
        ctx_dead = {
            "labels": {},
            "positives_by_horizon": {key: {"forward_window_observed": "true", "positive_outcome_label": "dead_negative"}},
            "positives_by_key": {},
            "v1_by_horizon": {key: {"promotion_priority_v1_would_promote": "true"}},
            "exit_by_horizon": {},
        }
        self.assertEqual(score_early_burst_in_out(row, ctx_positive)["decision"], score_early_burst_in_out(row, ctx_dead)["decision"])

    def test_dual_strategy_builder_updates_readiness_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            pipeline = root / "pipeline"
            data_mart = pipeline / "data_mart"
            out = pipeline / "dual_strategy_tracks"
            write_pipeline_readiness(pipeline)
            write_data_mart(
                data_mart,
                [label_row(mint="mint1", final_outcome="early_rejected_dead", clean_negative_label="true")],
                [feature_row(mint="mint1", horizon_seconds="60", horizon_reached="true", data_complete_for_horizon="true")],
            )
            summary = build_dual_strategy_tracks(pipeline_root=pipeline, data_mart_root=data_mart, output_root=out)
            readiness = json.loads((pipeline / "READINESS_DECISION.json").read_text())
            self.assertEqual(summary["classification"], "DUAL_STRATEGY_TRACK_RESEARCH_PASS")
            self.assertTrue(readiness["dual_strategy_track_research_ready"])
            self.assertFalse(readiness["replay_ready"])
            self.assertFalse(readiness["backtesting_ready"])
            self.assertFalse(readiness["collection_allowed"])
            self.assertTrue((out / "dual_strategy_tracks_export.zip").exists())

    def test_early_burst_v1_review_emits_no_trade_or_replay_side_effects(self) -> None:
        row = {
            "mint": "mint1",
            "slice_id": "slice1",
            "segment_id": "segment1",
            "relay_session_id": "relay1",
            "horizon_seconds": "5",
            "decision": "early_burst_candidate_review",
            "reason_codes": "",
            "top_blocker": "",
            "curve_progress_bin": "HIGH",
            "buy_sell_followthrough_bin": "LOW",
            "volume_followthrough_bin": "HIGH",
            "holder_growth_bin": "MEDIUM",
            "sell_pressure_bin": "LOW",
            "holder_dev_risk_bin": "LOW",
            "liquidity_risk_bin": "LOW",
            "v1_promoted_or_would_promote": "true",
            "dead_launch_avoider_prefilter": "continue_observation",
            "exit_window_observed_or_measurable": "true",
            "positive_outcome_label": "high_positive",
            "high_positive": "true",
            "final_outcome": "early_rejected_dead",
            "clean_negative_label": "true",
            "censored_label": "false",
            "candidate_checkpoint_seen": "false",
            "replay_eligible": "false",
        }
        score = score_early_burst_in_out_v1(row)
        self.assertEqual(score["decision"], "early_burst_candidate_review")
        self.assertEqual(score["trade_action"], "none")
        self.assertEqual(score["wallet_action"], "none")
        self.assertEqual(score["replay_eligible"], "false")
        self.assertEqual(score["backtest_eligible"], "false")
        self.assertEqual(score["paper_trading_eligible"], "false")
        self.assertEqual(score["live_trading_eligible"], "false")

    def test_early_burst_v1_high_positive_does_not_create_replay_eligibility(self) -> None:
        base = {
            "mint": "mint1",
            "slice_id": "slice1",
            "segment_id": "segment1",
            "horizon_seconds": "5",
            "curve_progress_bin": "HIGH",
            "buy_sell_followthrough_bin": "MEDIUM",
            "volume_followthrough_bin": "HIGH",
            "sell_pressure_bin": "LOW",
            "holder_dev_risk_bin": "LOW",
            "liquidity_risk_bin": "LOW",
            "v1_promoted_or_would_promote": "true",
            "dead_launch_avoider_prefilter": "continue_observation",
            "exit_window_observed_or_measurable": "true",
            "high_positive": "true",
            "positive_outcome_label": "high_positive",
            "candidate_checkpoint_seen": "true",
            "replay_eligible": "true",
        }
        score = score_early_burst_in_out_v1(base)
        self.assertEqual(score["decision"], "audit_only")
        self.assertEqual(score["replay_eligible"], "false")
        self.assertIn("replay_not_allowed", score["reason_codes"])

    def test_early_burst_v1_terminal_inconclusive_remains_censored(self) -> None:
        score = score_early_burst_in_out_v1(
            {
                "mint": "mint1",
                "slice_id": "slice1",
                "segment_id": "segment1",
                "horizon_seconds": "5",
                "v1_promoted_or_would_promote": "true",
                "dead_launch_avoider_prefilter": "continue_observation",
                "exit_window_observed_or_measurable": "true",
                "curve_progress_bin": "HIGH",
                "final_outcome": "terminal_inconclusive",
                "censored_label": "true",
            }
        )
        self.assertEqual(score["decision"], "censored")
        self.assertIn("terminal_inconclusive_censored", score["reason_codes"])

    def test_early_burst_v1_does_not_use_future_outcome_labels_as_inputs(self) -> None:
        base = {
            "mint": "mint1",
            "slice_id": "slice1",
            "segment_id": "segment1",
            "horizon_seconds": "5",
            "curve_progress_bin": "HIGH",
            "buy_sell_followthrough_bin": "MEDIUM",
            "volume_followthrough_bin": "HIGH",
            "sell_pressure_bin": "LOW",
            "holder_dev_risk_bin": "LOW",
            "liquidity_risk_bin": "LOW",
            "v1_promoted_or_would_promote": "true",
            "dead_launch_avoider_prefilter": "continue_observation",
            "exit_window_observed_or_measurable": "true",
            "censored_label": "false",
            "candidate_checkpoint_seen": "false",
        }
        positive = score_early_burst_in_out_v1({**base, "positive_outcome_label": "high_positive", "high_positive": "true"})
        negative = score_early_burst_in_out_v1({**base, "positive_outcome_label": "dead_negative", "high_positive": "false"})
        self.assertEqual(positive["decision"], negative["decision"])
        self.assertEqual(positive["reason_codes"], negative["reason_codes"])

    def test_early_burst_v1_builder_updates_readiness_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            pipeline = root / "pipeline"
            out = pipeline / "early_burst_in_out_v1"
            dual = pipeline / "dual_strategy_tracks"
            write_pipeline_readiness(pipeline)
            write_csv(
                dual / "early_burst_in_out_v0_scores.csv",
                [
                    {
                        "mint": "mint1",
                        "slice_id": "slice1",
                        "segment_id": "segment1",
                        "relay_session_id": "relay1",
                        "horizon_seconds": "5",
                        "decision": "early_burst_candidate_review",
                        "reason_codes": "",
                        "top_blocker": "",
                        "curve_progress_bin": "HIGH",
                        "buy_sell_followthrough_bin": "LOW",
                        "volume_followthrough_bin": "HIGH",
                        "holder_growth_bin": "MEDIUM",
                        "sell_pressure_bin": "LOW",
                        "holder_dev_risk_bin": "LOW",
                        "liquidity_risk_bin": "LOW",
                        "v1_promoted_or_would_promote": "true",
                        "dead_launch_avoider_prefilter": "continue_observation",
                        "exit_window_observed_or_measurable": "true",
                        "positive_outcome_label": "high_positive",
                        "high_positive": "true",
                        "final_outcome": "early_rejected_dead",
                        "clean_negative_label": "true",
                        "censored_label": "false",
                        "candidate_checkpoint_seen": "false",
                        "replay_eligible": "false",
                        "trade_action": "none",
                        "review_gate_only": "true",
                    }
                ],
                [
                    "mint",
                    "slice_id",
                    "segment_id",
                    "relay_session_id",
                    "horizon_seconds",
                    "decision",
                    "reason_codes",
                    "top_blocker",
                    "curve_progress_bin",
                    "buy_sell_followthrough_bin",
                    "volume_followthrough_bin",
                    "holder_growth_bin",
                    "sell_pressure_bin",
                    "holder_dev_risk_bin",
                    "liquidity_risk_bin",
                    "v1_promoted_or_would_promote",
                    "dead_launch_avoider_prefilter",
                    "exit_window_observed_or_measurable",
                    "positive_outcome_label",
                    "high_positive",
                    "final_outcome",
                    "clean_negative_label",
                    "censored_label",
                    "candidate_checkpoint_seen",
                    "replay_eligible",
                    "trade_action",
                    "review_gate_only",
                ],
            )
            summary = build_early_burst_in_out_v1(pipeline_root=pipeline, output_root=out)
            readiness = json.loads((pipeline / "READINESS_DECISION.json").read_text())
            self.assertEqual(summary["classification"], "EARLY_BURST_IN_OUT_V1_REVIEW_ARTIFACT_PASS")
            self.assertTrue(readiness["early_burst_in_out_v1_ready"])
            self.assertFalse(readiness["replay_ready"])
            self.assertFalse(readiness["backtesting_ready"])
            self.assertFalse(readiness["collection_allowed"])
            self.assertTrue((out / "early_burst_in_out_v1_export.zip").exists())


if __name__ == "__main__":
    unittest.main()
