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
import zipfile


SCRIPT = pathlib.Path(__file__).with_name("run_clean_24h_r2_streaming_mission.py")
SPEC = importlib.util.spec_from_file_location("clean_mission", SCRIPT)
mission = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(mission)


class Clean24hMissionTests(unittest.TestCase):
    @contextlib.contextmanager
    def patch_paths(self, root: pathlib.Path):
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(mission, "MISSION_ROOT", root))
            stack.enter_context(mock.patch.object(mission, "STATUS_PATH", root / "status.json"))
            stack.enter_context(mock.patch.object(mission, "LIVE_SUMMARY_PATH", root / "live_summary.md"))
            stack.enter_context(mock.patch.object(mission, "EVENTS_PATH", root / "events.ndjson"))
            stack.enter_context(mock.patch.object(mission, "SLICE_SUMMARIES_PATH", root / "slice_summaries.csv"))
            stack.enter_context(mock.patch.object(mission, "REVIEW_QUEUE_PATH", root / "review_queue.csv"))
            stack.enter_context(mock.patch.object(mission, "RECOVERY_ACTIONS_PATH", root / "recovery_actions.ndjson"))
            stack.enter_context(mock.patch.object(mission, "BLOCKER_HISTORY_PATH", root / "blocker_history.csv"))
            stack.enter_context(mock.patch.object(mission, "BATCH_SUMMARY_DIR", root / "batch_summaries"))
            yield

    def write_rows(self, root: pathlib.Path, rows: list[dict[str, object]]) -> None:
        fields = [
            "slice_index",
            "slice_id",
            "classification",
            "all_launches_indexed",
            "cheap_followup_rows",
            "rich_tracked_launches",
            "candidate_checkpoint_count",
            "replay_eligible_candidate_count",
            "r2_failed_files",
            "artifact_consistency_ok",
            "r2_streaming_uploaded_chunks",
            "r2_streaming_verified_chunks",
            "r2_streaming_deleted_local_chunks",
            "r2_streaming_unverified_chunks",
            "blocker_if_any",
        ]
        with (root / "slice_summaries.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def test_successful_slice_accounting_accepts_clean_no_signal(self) -> None:
        row = {
            "classification": "RELAY_LOCAL_DATASET_PASS_NO_SIGNAL",
            "artifact_consistency_ok": "true",
            "r2_failed_files": "0",
            "r2_streaming_unverified_chunks": "0",
            "blocker_if_any": "",
        }
        self.assertTrue(mission.is_successful_slice(row))

    def test_provider_quarantine_does_not_count_as_successful(self) -> None:
        row = {
            "classification": "RELAY_COLLECTION_PASS_PROVIDER_GAP_QUARANTINED_NO_COUNT",
            "artifact_consistency_ok": "true",
            "r2_failed_files": "0",
            "r2_streaming_unverified_chunks": "0",
            "blocker_if_any": "",
        }
        self.assertFalse(mission.is_successful_slice(row))

    def test_final_outputs_create_zip_and_keep_blocked_modes_false(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root), mock.patch.object(mission, "TARGET_SUCCESSFUL_SLICES", 2):
                self.write_rows(
                    root,
                    [
                        {
                            "slice_index": 1,
                            "slice_id": "slice-1",
                            "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                            "all_launches_indexed": 10,
                            "cheap_followup_rows": 70,
                            "rich_tracked_launches": 2,
                            "candidate_checkpoint_count": 0,
                            "replay_eligible_candidate_count": 0,
                            "r2_failed_files": 0,
                            "artifact_consistency_ok": "true",
                            "r2_streaming_uploaded_chunks": 1,
                            "r2_streaming_verified_chunks": 1,
                            "r2_streaming_deleted_local_chunks": 1,
                            "r2_streaming_unverified_chunks": 0,
                            "blocker_if_any": "",
                        },
                        {
                            "slice_index": 2,
                            "slice_id": "slice-2",
                            "classification": "RELAY_LOCAL_DATASET_PASS_NO_SIGNAL",
                            "all_launches_indexed": 5,
                            "cheap_followup_rows": 35,
                            "rich_tracked_launches": 0,
                            "candidate_checkpoint_count": 0,
                            "replay_eligible_candidate_count": 0,
                            "r2_failed_files": 0,
                            "artifact_consistency_ok": "true",
                            "r2_streaming_uploaded_chunks": 1,
                            "r2_streaming_verified_chunks": 1,
                            "r2_streaming_deleted_local_chunks": 1,
                            "r2_streaming_unverified_chunks": 0,
                            "blocker_if_any": "",
                        },
                    ],
                )
                (root / "review_queue.csv").write_text("created_at_utc,run_id,mint,reason,status\n")
                (root / "blocker_history.csv").write_text("ts,classification,blocker,successful_slices,attempted_slices\n")
                (root / "recovery_actions.ndjson").write_text("")
                zip_path = mission.write_final_outputs(mission.SUCCESS_CLASSIFICATION)
                decision = json.loads((root / "CLEAN_24H_R2_STREAMING_DATASET_FINAL_DECISION.json").read_text())
                self.assertEqual(decision["classification"], mission.SUCCESS_CLASSIFICATION)
                self.assertFalse(decision["formal_backtesting_allowed"])
                self.assertFalse(decision["live_trading_enabled"])
                self.assertTrue(decision["launch_caps_remain_blocked"])
                self.assertTrue(zip_path.exists())
                with zipfile.ZipFile(zip_path) as archive:
                    names = set(archive.namelist())
                self.assertIn("CLEAN_24H_R2_STREAMING_DATASET_REPORT.md", names)
                self.assertIn("strategy_dataset_manifest.json", names)

    def test_terminal_classification_detects_candidate_review(self) -> None:
        status = {"candidate_checkpoint_count": 1, "replay_eligible_candidate_count": 0, "successful_slices": 0}
        self.assertEqual(mission.terminal_classification(status), mission.REVIEW_CLASSIFICATION)


if __name__ == "__main__":
    unittest.main()
