#!/usr/bin/env python3
from __future__ import annotations

import contextlib
import csv
import importlib.util
import json
import pathlib
import tempfile
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("run_background_24h_collector.py")
SPEC = importlib.util.spec_from_file_location("background_24h_collector", SCRIPT)
collector = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(collector)


class Background24hCollectorTests(unittest.TestCase):
    @contextlib.contextmanager
    def patch_paths(self, root: pathlib.Path):
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(collector, "STATUS_ROOT", root))
            stack.enter_context(mock.patch.object(collector, "STATUS_PATH", root / "status.json"))
            stack.enter_context(mock.patch.object(collector, "LIVE_SUMMARY_PATH", root / "live_summary.md"))
            stack.enter_context(mock.patch.object(collector, "EVENTS_PATH", root / "events.ndjson"))
            stack.enter_context(mock.patch.object(collector, "SLICE_SUMMARIES_PATH", root / "slice_summaries.csv"))
            stack.enter_context(mock.patch.object(collector, "REVIEW_QUEUE_PATH", root / "review_queue.csv"))
            stack.enter_context(mock.patch.object(collector, "MASTER_JUSTIFICATION_JSON", root / "BACKGROUND_24H_COLLECTION_JUSTIFICATION.json"))
            stack.enter_context(mock.patch.object(collector, "MASTER_JUSTIFICATION_MD", root / "BACKGROUND_24H_COLLECTION_JUSTIFICATION.md"))
            yield

    def test_master_justification_is_targeted_and_forbidden_modes_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                payload = collector.write_master_justification(4)
            self.assertTrue(payload["collection_allowed"])
            self.assertEqual(payload["reason"], "targeted_early_burst_feature_complete_collection")
            self.assertFalse(payload["generic_collection_allowed"])
            self.assertEqual(payload["replay_backtesting_tuning_paper_live_wallet_execution"], "blocked")
            self.assertTrue(payload["launch_caps_remain_blocked"])
            saved = json.loads((root / "BACKGROUND_24H_COLLECTION_JUSTIFICATION.json").read_text())
            self.assertEqual(saved["max_slices_per_batch"], 10)
            self.assertEqual(saved["max_slices_total"], 96)

    def test_batch_gate_is_accepted_by_relay_supervisor_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                path = collector.write_batch_gate(1, 10)
            gate = json.loads(path.read_text())
            self.assertTrue(gate["collection_allowed"])
            self.assertEqual(gate["reason"], "targeted_early_burst_sample_collection")
            self.assertEqual(gate["target_gate"], collector.TARGET_GATE)
            self.assertEqual(gate["maximum_allowed_slices"], 10)
            self.assertFalse(gate["replay_allowed"])
            self.assertFalse(gate["formal_backtesting_allowed"])
            self.assertFalse(gate["threshold_tuning_allowed"])
            self.assertFalse(gate["paper_trading_enabled"])
            self.assertFalse(gate["live_trading_enabled"])
            self.assertFalse(gate["wallet_execution_enabled"])
            self.assertFalse(gate["old_vps_material_hunter_allowed"])
            self.assertFalse(gate["holder_rpc_enabled"])
            self.assertFalse(gate["rpc_mint_supply_canonical"])

    def test_stop_classification_triggers_on_candidate_or_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                collector.write_csv_rows(
                    root / "slice_summaries.csv",
                    [
                        {
                            "slice_id": "slice-1",
                            "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                            "candidate_checkpoint_count": "1",
                            "replay_eligible_candidate_count": "0",
                        }
                    ],
                    collector.SLICE_FIELDS,
                )
                stop, classification, reason = collector.stop_classification({}, 0)
            self.assertTrue(stop)
            self.assertEqual(classification, "CANDIDATE_REVIEW_TRIGGERED")
            self.assertEqual(reason, "candidate_or_replay_trigger")

    def test_status_marks_missing_worker_as_recovery_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "status.json").write_text(json.dumps({"state": "running", "pid": 999999}) + "\n")
            with self.patch_paths(root):
                with mock.patch.object(collector, "pid_running", return_value=False):
                    rc = collector.status(type("Args", (), {})())
            self.assertEqual(rc, 0)
            payload = json.loads((root / "status.json").read_text())
            self.assertEqual(payload["state"], "needs_recovery")
            self.assertEqual(payload["classification"], "BACKGROUND_24H_COLLECTION_BLOCK_LOCAL_FINALIZATION")
            self.assertFalse(payload["replay_allowed"])
            self.assertTrue(payload["launch_caps_remain_blocked"])

    def test_mirror_slice_records_required_monitor_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            batch = root / "batch"
            batch.mkdir()
            (batch / "batch_summary.ndjson").write_text(
                json.dumps(
                    {
                        "run": "slice-1",
                        "relay_session_id": "relay-1",
                        "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                        "frames_received": 100,
                        "all_launches_seen": 10,
                        "all_launches_indexed": 10,
                        "cheap_followup_rows": 70,
                        "rich_tracked_launches": 3,
                        "promotion_recommended_count": 12,
                        "promotion_admitted_count": 2,
                        "promotion_blocked_budget_count": 8,
                        "attempted_launches": 3,
                        "rejected_dead_count": 2,
                        "terminal_inconclusive_count": 1,
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                        "early_burst_review_candidate_count": 2,
                        "early_burst_review_unique_mint_count": 2,
                        "early_burst_review_replay_eligible_candidate_count": 0,
                        "r2_failed": 0,
                        "artifact_consistency_ok": True,
                        "vps_safety": {"forbidden_recent": 0, "material_candidate_service": "inactive", "material_hunter_service": "inactive"},
                    }
                )
                + "\n"
            )
            with self.patch_paths(root):
                with mock.patch.object(collector, "current_high_positive_count", return_value=4):
                    row = collector.mirror_slice(1, batch, {"positive_outcome_rows": 2, "high_positive_outcome_rows": 1})
            self.assertEqual(row["slice_id"], "slice-1")
            self.assertEqual(row["r2_verified"], "true")
            with (root / "slice_summaries.csv").open() as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["cheap_followup_rows"], "70")
            self.assertEqual(rows[0]["early_burst_review_candidate_count"], "2")
            self.assertEqual(rows[0]["high_positive_unique_total"], "4")


if __name__ == "__main__":
    unittest.main()
