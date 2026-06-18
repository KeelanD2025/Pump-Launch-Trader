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

from strategy.buy_setup import BuySetupDraft
from strategy.candidate_review import build_candidate_review_pack
from strategy.feature_store import FeatureStore
from strategy.gates import CandidateEligibilityGateV2, ContinueTrackingGateV1, EarlyAvoidFilterV1
from strategy.io import write_csv, write_json
from strategy.label_store import LabelStore
from strategy.registry import list_strategies, validate_strategy_config
from strategy.reports import write_gpt_export
from strategy.risk_exit import RiskAndExitDraft
from strategy.schemas import HORIZONS
from strategy.splits import build_chronological_splits, validate_splits
from strategy.readiness import readiness_decision


def write_alpha(root: Path, rows: list[dict[str, str]], horizon: int = 60) -> None:
    fields = [
        "mint",
        "slice_id",
        "segment_id",
        "relay_session_id",
        "horizon_seconds",
        "feature_asof_timestamp",
        "mint_first_seen_timestamp",
        "horizon_reached",
        "data_complete_for_horizon",
        "provider_gap_exposed",
        "relay_gap_exposed",
        "sequence_gap_exposed",
        "hash_mismatch_exposed",
        "receiver_backpressure_exposed",
        "terminal_inconclusive_before_horizon",
        "rejected_before_horizon",
        "degraded_audit_only_before_horizon",
        "high_throughput_before_horizon",
        "trade_update_count_asof",
        "buy_count_delta_asof",
        "sell_count_delta_asof",
        "net_buy_sell_delta_asof",
        "holder_update_count_asof",
        "unique_holder_accounts_seen_asof",
        "top_holder_concentration_asof",
        "dev_or_creator_holding_proxy_asof",
        "vault_update_count_asof",
        "bonding_curve_update_count_asof",
        "curve_progress_proxy_asof",
        "liquidity_exit_proxy_asof",
        "holder_rpc_used",
        "rpc_mint_supply_canonical",
        "threshold_tuning_allowed",
        "live_trading_enabled",
    ]
    extra = sorted({key for row in rows for key in row if key not in fields})
    write_csv(root / "asof_alpha_features" / f"asof_alpha_features_{horizon:03d}s.csv", rows, fields + extra)


def write_labels(root: Path, rows: list[dict[str, str]]) -> None:
    fields = [
        "mint",
        "slice_id",
        "segment_id",
        "relay_session_id",
        "first_seen_at",
        "final_outcome",
        "provider_gap_exposed",
        "candidate_checkpoint_seen",
        "replay_eligible",
        "clean_negative_label",
        "clean_positive_label",
        "censored_label",
        "label_quality",
    ]
    write_csv(root / "mint_labels.csv", rows, fields)


def base_alpha(**overrides: str) -> dict[str, str]:
    row = {
        "mint": "mint",
        "slice_id": "slice",
        "segment_id": "1",
        "relay_session_id": "relay",
        "horizon_seconds": "60",
        "feature_asof_timestamp": "2026-06-18T00:01:00+00:00",
        "mint_first_seen_timestamp": "2026-06-18T00:00:00+00:00",
        "horizon_reached": "true",
        "data_complete_for_horizon": "true",
        "provider_gap_exposed": "false",
        "relay_gap_exposed": "false",
        "sequence_gap_exposed": "false",
        "hash_mismatch_exposed": "false",
        "receiver_backpressure_exposed": "false",
        "terminal_inconclusive_before_horizon": "false",
        "rejected_before_horizon": "false",
        "degraded_audit_only_before_horizon": "false",
        "high_throughput_before_horizon": "false",
        "trade_update_count_asof": "3",
        "buy_count_delta_asof": "2",
        "sell_count_delta_asof": "1",
        "net_buy_sell_delta_asof": "1",
        "holder_update_count_asof": "3",
        "unique_holder_accounts_seen_asof": "3",
        "top_holder_concentration_asof": "0.4",
        "dev_or_creator_holding_proxy_asof": "0",
        "vault_update_count_asof": "2",
        "bonding_curve_update_count_asof": "2",
        "curve_progress_proxy_asof": "0.2",
        "liquidity_exit_proxy_asof": "0",
        "holder_rpc_used": "false",
        "rpc_mint_supply_canonical": "false",
        "threshold_tuning_allowed": "false",
        "live_trading_enabled": "false",
    }
    row.update(overrides)
    return row


class StrategyArchitectureTests(unittest.TestCase):
    def test_feature_store_rejects_final_outcome_columns(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_alpha(root, [base_alpha(final_outcome="early_rejected_dead")])
            result = FeatureStore(root).validate_asof_safety()
        self.assertFalse(result["passed"])
        self.assertTrue(any("forbidden_alpha_columns" in blocker for blocker in result["blockers"]))

    def test_feature_store_rejects_post_horizon_data(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_alpha(root, [base_alpha(feature_asof_timestamp="2026-06-18T00:02:01+00:00")])
            result = FeatureStore(root).validate_asof_safety()
        self.assertFalse(result["passed"])
        self.assertTrue(any("post_horizon_timestamp" in blocker for blocker in result["blockers"]))

    def test_label_store_treats_terminal_inconclusive_as_censored(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_labels(root, [dict(mint="mint", slice_id="s", segment_id="1", relay_session_id="r", first_seen_at="1", final_outcome="terminal_inconclusive", provider_gap_exposed="false", candidate_checkpoint_seen="false", replay_eligible="false", clean_negative_label="false", clean_positive_label="false", censored_label="true", label_quality="censored")])
            store = LabelStore(root)
            self.assertEqual(len(store.censored()), 1)
            self.assertTrue(store.validate()["passed"])

    def test_candidate_checkpoint_is_not_positive(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_labels(root, [dict(mint="mint", slice_id="s", segment_id="1", relay_session_id="r", first_seen_at="1", final_outcome="candidate_checkpoint", provider_gap_exposed="false", candidate_checkpoint_seen="true", replay_eligible="false", clean_negative_label="false", clean_positive_label="false", censored_label="false", label_quality="audit_only")])
            store = LabelStore(root)
        self.assertTrue(store.validate()["passed"])
        self.assertEqual(len(store.clean_positives()), 0)

    def test_replay_ineligible_candidate_cannot_be_positive(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_labels(root, [dict(mint="mint", slice_id="s", segment_id="1", relay_session_id="r", first_seen_at="1", final_outcome="candidate_checkpoint", provider_gap_exposed="false", candidate_checkpoint_seen="true", replay_eligible="false", clean_negative_label="false", clean_positive_label="true", censored_label="false", label_quality="bad")])
            result = LabelStore(root).validate()
        self.assertFalse(result["passed"])

    def test_research_gates_emit_no_trade_actions(self) -> None:
        row = base_alpha()
        label = {"candidate_checkpoint_seen": "false", "replay_eligible": "false"}
        outputs = [
            EarlyAvoidFilterV1().score(row),
            ContinueTrackingGateV1().score(row),
            CandidateEligibilityGateV2().score(row, label),
        ]
        for output in outputs:
            self.assertNotIn(output.decision, {"buy", "sell", "enter_position", "submit_order"})
            self.assertNotIn("submit_order", output.allowed_actions)

    def test_candidate_eligibility_emits_no_replay_permission(self) -> None:
        output = CandidateEligibilityGateV2().score(base_alpha(), {"candidate_checkpoint_seen": "true", "replay_eligible": "false"})
        self.assertIn("replay", output.blocked_actions)
        self.assertNotIn("replay", output.allowed_actions)

    def test_buy_setup_draft_is_disabled_and_not_buy(self) -> None:
        draft = BuySetupDraft()
        row = draft.score("mint", "candidate_watch", ["candidate_checkpoint_absent"])
        self.assertTrue(row["disabled_by_default"])
        self.assertEqual(row["trade_action"], "none")
        self.assertEqual(row["setup_decision"], "candidate_setup_only")

    def test_risk_exit_draft_emits_no_orders(self) -> None:
        desc = RiskAndExitDraft().describe()
        self.assertFalse(desc["emits_orders"])
        self.assertFalse(desc["wallet_execution_enabled"])

    def test_backtest_replay_and_execution_harnesses_block(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            arch = Path(td)
            write_json(arch / "readiness_decision.json", {"backtesting_ready": False, "replay_ready": False, "reason_codes": ["no_clean_positives"]})
            commands = [
                [sys.executable, str(SCRIPT_DIR / "run_strategy_backtest.py"), "--strategy", "early_avoid_v1", "--architecture-root", str(arch)],
                [sys.executable, str(SCRIPT_DIR / "run_strategy_replay.py"), "--architecture-root", str(arch)],
                [sys.executable, str(SCRIPT_DIR / "run_strategy_threshold_tuning.py")],
                [sys.executable, str(SCRIPT_DIR / "run_strategy_paper.py")],
                [sys.executable, str(SCRIPT_DIR / "run_strategy_live.py")],
                [sys.executable, str(SCRIPT_DIR / "run_strategy_wallet.py")],
            ]
            for command in commands:
                proc = subprocess.run(command, cwd=REPO, text=True, capture_output=True)
                self.assertNotEqual(proc.returncode, 0, command)
                self.assertIn("blocker", proc.stdout)

    def test_holder_rpc_and_rpc_supply_are_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            write_alpha(root, [base_alpha(holder_rpc_used="true", rpc_mint_supply_canonical="true")])
            result = FeatureStore(root).validate_asof_safety()
        self.assertFalse(result["passed"])
        self.assertTrue(any("holder_rpc_used" in blocker for blocker in result["blockers"]))
        self.assertTrue(any("rpc_mint_supply_canonical" in blocker for blocker in result["blockers"]))

    def test_r2_success_cannot_override_no_positive_blocker(self) -> None:
        decision = readiness_decision(
            label_summary={"clean_negatives": 10, "clean_positives": 0, "replay_eligible": 0},
            leakage_passed=True,
            modules_exist=True,
        )
        self.assertTrue(decision["buy_strategy_architecture_ready"])
        self.assertFalse(decision["backtesting_ready"])
        self.assertFalse(decision["replay_ready"])

    def test_strategy_registry_loads_all_modules(self) -> None:
        entries = list_strategies()
        self.assertGreaterEqual(len(entries), 5)
        for entry in entries:
            self.assertTrue(validate_strategy_config(entry)["passed"])

    def test_candidate_review_pack_handles_zero_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            pack = build_candidate_review_pack(Path(td), [], {horizon: [] for horizon in HORIZONS}, [])
            self.assertTrue((pack / "candidate_mints.csv").exists())
            decision = json.loads((pack / "candidate_review_decision.json").read_text())
        self.assertEqual(decision["candidate_count"], 0)
        self.assertFalse(decision["replay_was_run"])

    def test_chronological_splits_are_valid(self) -> None:
        labels = [{"mint": f"mint{i}", "first_seen_at": f"2026-06-18T00:{i:02d}:00Z"} for i in range(40)]
        splits = build_chronological_splits(labels, embargo_rows=2)
        self.assertTrue(validate_splits(splits)["passed"])

    def test_gpt_export_contains_no_secret_named_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "normal.md").write_text("ok")
            (root / ".env").write_text("SECRET=bad")
            zip_path = write_gpt_export(root, {"strategy_research_ready": True, "buy_strategy_architecture_ready": True})
            import zipfile
            with zipfile.ZipFile(zip_path) as archive:
                names = archive.namelist()
        self.assertIn("normal.md", names)
        self.assertFalse(any("env" in name.lower() for name in names))

    def test_real_architecture_builder_runs_on_current_data(self) -> None:
        proc = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "build_buy_strategy_architecture.py"), "all"],
            cwd=REPO,
            text=True,
            capture_output=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
        self.assertIn("BUY_STRATEGY_ARCHITECTURE_READY_PASS", proc.stdout)


if __name__ == "__main__":
    unittest.main()
