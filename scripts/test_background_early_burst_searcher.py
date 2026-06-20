#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import csv
import json
import os
import pathlib
import tempfile
import types
import unittest
from contextlib import ExitStack
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("run_background_early_burst_searcher.py")
SPEC = importlib.util.spec_from_file_location("background_searcher", SCRIPT)
assert SPEC and SPEC.loader
background_searcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(background_searcher)


class BackgroundEarlyBurstSearcherTests(unittest.TestCase):
    def patch_status_paths(self, root: pathlib.Path) -> ExitStack:
        stack = ExitStack()
        stack.enter_context(mock.patch.object(background_searcher, "STATUS_ROOT", root))
        stack.enter_context(mock.patch.object(background_searcher, "STATUS_PATH", root / "status.json"))
        stack.enter_context(mock.patch.object(background_searcher, "LIVE_SUMMARY_PATH", root / "live_summary.md"))
        stack.enter_context(mock.patch.object(background_searcher, "EVENTS_PATH", root / "events.ndjson"))
        stack.enter_context(mock.patch.object(background_searcher, "REVIEW_QUEUE_PATH", root / "review_queue.csv"))
        stack.enter_context(mock.patch.object(background_searcher, "SLICE_SUMMARIES_PATH", root / "slice_summaries.csv"))
        return stack

    def write_running_status(self, root: pathlib.Path, pid: int = 12345) -> None:
        batch_log_dir = root / "batch"
        (root / "status.json").write_text(
            json.dumps(
                {
                    "schema_version": "phase107j.background_early_burst_searcher.v1",
                    "state": "running",
                    "pid": pid,
                    "batch_log_dir": str(batch_log_dir),
                    "updated_at_utc": "2026-06-20T09:38:37Z",
                    "max_slices_authorized": 10,
                    "generic_collection_allowed": False,
                    "replay_allowed": False,
                    "formal_backtesting_allowed": False,
                    "threshold_tuning_allowed": False,
                    "paper_trading_enabled": False,
                    "live_trading_enabled": False,
                    "wallet_execution_enabled": False,
                    "launch_caps_remain_blocked": True,
                }
            )
            + "\n"
        )
        (root / "events.ndjson").write_text(
            json.dumps({"event": "started", "pid": pid, "ts": "2026-06-20T09:38:37Z"}) + "\n"
        )
        (root / "review_queue.csv").write_text("created_at_utc,run_id,mint,reason,status\n")

    def test_start_blocks_when_relay_control_preflight_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_status_paths(root):
                with mock.patch.object(
                    background_searcher,
                    "run_preflight",
                    return_value={
                        "classification": "RELAY_CONTROL_CONFIG_BLOCK_MISSING_ENV",
                        "ok": False,
                        "missing_config_names": ["PUMP_RELAY_VPS_SSH_TARGET"],
                    },
                ):
                    with mock.patch.object(background_searcher.subprocess, "Popen") as popen:
                        args = types.SimpleNamespace(
                            control_env=root / "missing.env",
                            slices=10,
                            duration_seconds=900,
                            justification_id="targeted-early-burst-sample-20260620-001",
                            max_slices=10,
                            run_prefix="relay-r2-early-burst-targeted",
                        )
                        rc = background_searcher.start(args)
                        self.assertEqual(rc, 2)
                        self.assertFalse(popen.called)
                        status = json.loads((root / "status.json").read_text())
                        self.assertEqual(status["state"], "blocked")
                        self.assertEqual(
                            status["preflight_classification"],
                            "RELAY_CONTROL_CONFIG_BLOCK_MISSING_ENV",
                        )
                        self.assertFalse(status["replay_allowed"])
                        self.assertFalse(status["formal_backtesting_allowed"])
                        self.assertTrue(status["launch_caps_remain_blocked"])

    def test_status_reports_not_started_without_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with mock.patch.object(background_searcher, "STATUS_PATH", root / "status.json"):
                with mock.patch.object(background_searcher, "LIVE_SUMMARY_PATH", root / "live_summary.md"):
                    args = types.SimpleNamespace()
                    rc = background_searcher.status(args)
        self.assertEqual(rc, 0)

    def test_status_updates_running_monitor_fields_without_slice_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            self.write_running_status(root)
            with self.patch_status_paths(root):
                with mock.patch.object(background_searcher, "pid_running", return_value=True):
                    rc = background_searcher.status(types.SimpleNamespace())
            self.assertEqual(rc, 0)
            status = json.loads((root / "status.json").read_text())
            self.assertTrue(status["process_alive"])
            self.assertEqual(status["current_slice_state"], "in_progress")
            self.assertEqual(status["slices_completed"], 0)
            self.assertEqual(status["review_queue_rows"], 0)
            self.assertFalse(status["candidate_replay_trigger_visible"])
            self.assertFalse(status["blocker_visible"])
            self.assertFalse(status["replay_allowed"])
            self.assertFalse(status["formal_backtesting_allowed"])
            self.assertTrue(status["launch_caps_remain_blocked"])
            self.assertIn("slice_summaries_csv", (root / "live_summary.md").read_text())

    def test_status_warns_stale_only_after_35_minutes_without_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            self.write_running_status(root)
            old = background_searcher.time.time() - background_searcher.STALE_STATUS_WARNING_SECONDS - 60
            for path in [root / "status.json", root / "events.ndjson", root / "review_queue.csv"]:
                os.utime(path, (old, old))
            with self.patch_status_paths(root):
                with mock.patch.object(background_searcher, "pid_running", return_value=True):
                    rc = background_searcher.status(types.SimpleNamespace())
            self.assertEqual(rc, 0)
            status = json.loads((root / "status.json").read_text())
            self.assertTrue(status["stale_status_warning"])
            self.assertEqual(status["state"], "running")
            self.assertFalse(status["blocker_visible"])

    def test_summarize_reads_slice_summaries_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            self.write_running_status(root)
            (root / "slice_summaries.csv").write_text(
                "run,classification,candidate_checkpoint_count,replay_eligible_candidate_count,"
                "high_positive_unique_mints_total,positive_high_rows,candidate_watch_rows,"
                "r2_verification_failures,artifact_consistency_ok\n"
                "slice-1,RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE,0,0,3,4,2,0,true\n"
            )
            with self.patch_status_paths(root):
                with mock.patch.object(background_searcher, "pid_running", return_value=True):
                    rc = background_searcher.summarize(types.SimpleNamespace(batch_log_dir=None))
            self.assertEqual(rc, 0)
            status = json.loads((root / "status.json").read_text())
            self.assertEqual(status["slices_completed"], 1)
            self.assertEqual(status["baseline_high_positive_unique_mints"], 4)
            self.assertEqual(status["new_high_positive_unique_mints_this_run"], 3)
            self.assertEqual(status["total_high_positive_unique_mints"], 7)
            self.assertEqual(status["high_positive_unique_mints_total"], 7)
            self.assertEqual(status["additional_high_positive_needed"], 13)
            self.assertEqual(status["positive_high_rows_visible"], 4)
            self.assertEqual(status["candidate_watch_rows_visible"], 2)
            self.assertFalse(status["candidate_replay_trigger_visible"])

    def test_status_mirrors_batch_summary_after_producer_files_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            self.write_running_status(root)
            batch = root / "batch"
            batch.mkdir()
            (batch / "batch_summary.ndjson").write_text(
                json.dumps(
                    {
                        "slice_index": 1,
                        "run": "slice-1",
                        "relay_session_id": "relay-1",
                        "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                        "frames_received": 100,
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                        "r2_failed": 0,
                        "artifact_consistency_ok": True,
                    }
                )
                + "\n"
            )
            old = background_searcher.time.time() - background_searcher.STALE_STATUS_WARNING_SECONDS - 60
            for path in [root / "events.ndjson", root / "review_queue.csv"]:
                os.utime(path, (old, old))
            with self.patch_status_paths(root):
                with mock.patch.object(background_searcher, "pid_running", return_value=True):
                    rc = background_searcher.status(types.SimpleNamespace())
            self.assertEqual(rc, 0)
            self.assertTrue((root / "slice_summaries.csv").exists())
            status = json.loads((root / "status.json").read_text())
            self.assertEqual(status["slices_completed"], 1)
            self.assertTrue(status["batch_summary_mirrored_to_slice_summaries"])
            self.assertFalse(status["stale_status_warning"])

    def test_status_mirrors_new_batch_rows_when_slice_summary_already_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            self.write_running_status(root)
            (root / "slice_summaries.csv").write_text(
                "slice_index,run,relay_session_id,classification,candidate_checkpoint_count,"
                "replay_eligible_candidate_count,r2_failures,artifact_consistency_ok\n"
                "1,slice-1,relay-1,RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE,0,0,0,true\n"
            )
            batch = root / "batch"
            batch.mkdir()
            (batch / "batch_summary.ndjson").write_text(
                json.dumps(
                    {
                        "slice_index": 1,
                        "run": "slice-1",
                        "relay_session_id": "relay-1",
                        "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                        "r2_failed": 0,
                        "artifact_consistency_ok": True,
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "slice_index": 2,
                        "run": "slice-2",
                        "relay_session_id": "relay-2",
                        "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                        "r2_failed": 0,
                        "artifact_consistency_ok": True,
                    }
                )
                + "\n"
            )
            old = background_searcher.time.time() - background_searcher.STALE_STATUS_WARNING_SECONDS - 60
            for path in [root / "events.ndjson", root / "review_queue.csv", root / "slice_summaries.csv"]:
                os.utime(path, (old, old))
            with self.patch_status_paths(root):
                with mock.patch.object(background_searcher, "pid_running", return_value=True):
                    rc = background_searcher.status(types.SimpleNamespace())
            self.assertEqual(rc, 0)
            with (root / "slice_summaries.csv").open() as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[-1]["run"], "slice-2")
            status = json.loads((root / "status.json").read_text())
            self.assertEqual(status["slices_completed"], 2)
            self.assertTrue(status["batch_summary_mirrored_to_slice_summaries"])

    def test_candidate_review_visible_from_review_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            self.write_running_status(root)
            (root / "review_queue.csv").write_text(
                "created_at_utc,run_id,mint,reason,status\n"
                "2026-06-20T10:00:00Z,slice-1,MINT,candidate_checkpoint,open\n"
            )
            with self.patch_status_paths(root):
                with mock.patch.object(background_searcher, "pid_running", return_value=True):
                    with mock.patch.object(background_searcher, "request_stop_for_monitor", return_value=True):
                        rc = background_searcher.status(types.SimpleNamespace())
            self.assertEqual(rc, 0)
            status = json.loads((root / "status.json").read_text())
            self.assertEqual(status["review_queue_rows"], 1)
            self.assertTrue(status["candidate_replay_trigger_visible"])
            self.assertEqual(status["monitor_stop_reason"], "candidate_or_replay_trigger_visible")
            self.assertTrue((pathlib.Path(status["candidate_review_pack_path"]) / "review_queue.csv").exists())

    def test_max_slices_without_target_writes_sample_scarcity_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            self.write_running_status(root)
            header = (
                "slice_index,run,relay_session_id,classification,frames_received,attempted_launches,"
                "unique_attempted_mints,rejected_dead_count,terminal_inconclusive_count,"
                "candidate_checkpoint_count,replay_eligible_candidate_count,r2_failures,"
                "artifact_consistency_ok,sequence_gap_count,hash_mismatch_count,malformed_frame_count,"
                "receiver_backpressure_count,receiver_unavailable_count\n"
            )
            body = "".join(
                f"{idx},slice-{idx},relay-{idx},RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE,100,1,1,1,0,0,0,0,true,0,0,0,0,0\n"
                for idx in range(1, 11)
            )
            (root / "slice_summaries.csv").write_text(header + body)
            with self.patch_status_paths(root):
                with mock.patch.object(background_searcher, "pid_running", return_value=True):
                    with mock.patch.object(background_searcher, "request_stop_for_monitor", return_value=True):
                        with mock.patch.object(background_searcher, "run_reports", return_value={"ok": True}):
                            rc = background_searcher.status(types.SimpleNamespace())
            self.assertEqual(rc, 0)
            status = json.loads((root / "status.json").read_text())
            self.assertEqual(status["monitor_stop_reason"], "max_slices_completed")
            self.assertEqual(status["state"], "stopping")
            self.assertTrue(pathlib.Path(status["sample_scarcity_report_path"]).exists())
            report = json.loads((root / "sample_scarcity_report.json").read_text())
            self.assertFalse(report["collection_allowed"])
            self.assertEqual(report["classification"], "EARLY_BURST_TARGETED_SAMPLE_SCARCITY")
            self.assertEqual(report["slices_completed"], 10)
            self.assertEqual(report["new_high_positive_unique_mints_this_run"], 0)
            self.assertEqual(report["total_high_positive_unique_mints"], 4)

    def test_completed_worker_mirrors_final_batch_row_and_writes_scarcity_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            self.write_running_status(root)
            status = json.loads((root / "status.json").read_text())
            status["state"] = "complete"
            status["pid"] = None
            (root / "status.json").write_text(json.dumps(status) + "\n")
            existing_rows = (
                "slice_index,run,relay_session_id,classification,candidate_checkpoint_count,"
                "replay_eligible_candidate_count,r2_failures,artifact_consistency_ok\n"
                + "".join(
                    f"{idx},slice-{idx},relay-{idx},RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE,0,0,0,true\n"
                    for idx in range(1, 10)
                )
            )
            (root / "slice_summaries.csv").write_text(existing_rows)
            batch = root / "batch"
            batch.mkdir()
            (batch / "batch_summary.ndjson").write_text(
                "".join(
                    json.dumps(
                        {
                            "slice_index": idx,
                            "run": f"slice-{idx}",
                            "relay_session_id": f"relay-{idx}",
                            "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                            "candidate_checkpoint_count": 0,
                            "replay_eligible_candidate_count": 0,
                            "r2_failed": 0,
                            "artifact_consistency_ok": True,
                        }
                    )
                    + "\n"
                    for idx in range(1, 11)
                )
            )
            with self.patch_status_paths(root):
                with mock.patch.object(background_searcher, "pid_running", return_value=False):
                    with mock.patch.object(background_searcher, "run_reports", return_value={"ok": True}):
                        rc = background_searcher.status(types.SimpleNamespace())
            self.assertEqual(rc, 0)
            with (root / "slice_summaries.csv").open() as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 10)
            status = json.loads((root / "status.json").read_text())
            self.assertEqual(status["monitor_stop_reason"], "max_slices_completed")
            self.assertFalse(status["monitor_stop_requested"])
            self.assertTrue(pathlib.Path(status["sample_scarcity_report_path"]).exists())


if __name__ == "__main__":
    unittest.main()
