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

GC_SCRIPT = pathlib.Path(__file__).with_name("local_r2_primary_storage_gc.py")
GC_SPEC = importlib.util.spec_from_file_location("local_r2_primary_storage_gc", GC_SCRIPT)
storage_gc = importlib.util.module_from_spec(GC_SPEC)
assert GC_SPEC.loader is not None
GC_SPEC.loader.exec_module(storage_gc)


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

    def write_verified_run(self, root: pathlib.Path, run_id: str = "run-1") -> pathlib.Path:
        run = root / run_id
        (run / "relay_frames").mkdir(parents=True)
        (run / "relay_frames" / "part-000001.ndjson").write_text("frame\n" * 100)
        (run / "countability_decision.json").write_text("{}\n")
        (run / "run_countability_decision.json").write_text("{}\n")
        (run / "attempt_ledger.csv").write_text("mint\n")
        (run / "r2_upload_result.json").write_text(
            json.dumps({"verified": True, "failed_files": [], "uploaded_files": ["k"]}) + "\n"
        )
        (run / "local_retention_summary.json").write_text(
            json.dumps({"ok": True, "r2_verified": True}) + "\n"
        )
        (run / "artifact_consistency_summary.json").write_text(json.dumps({"ok": True}) + "\n")
        (run / "service_exit_status.json").write_text(
            json.dumps({"hunter_exit_status": 0, "service_exit_reason": "local_relay_collector_completed"}) + "\n"
        )
        (run / "local_collector_summary.json").write_text(
            json.dumps(
                {
                    "frames_received": 10,
                    "sequence_gap_count": 0,
                    "hash_mismatch_count": 0,
                    "malformed_frame_count": 0,
                    "downstream_backpressure_count": 0,
                    "receiver_unavailable_count": 0,
                }
            )
            + "\n"
        )
        (run / "local_relay_dataset_proof_summary.json").write_text("{}\n")
        return run

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
            self.assertEqual(saved["storage_mode"], "r2-streaming")
            self.assertEqual(saved["r2_streaming_min_free_mb"], 4096)
            self.assertEqual(saved["r2_streaming_spool_mb"], 2048)

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

    def test_worker_clears_stale_blocker_before_next_slice(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                (root / "status.json").write_text(
                    json.dumps(
                        {
                            "state": "blocked",
                            "classification": "BACKGROUND_24H_COLLECTION_BLOCK_RELAY",
                            "blocker": "supervisor_slice_failed",
                            "last_slice_returncode": 2,
                        }
                    )
                    + "\n"
                )
                with mock.patch.object(collector, "run_one_slice", return_value=2) as run_one_slice:
                    rc = collector.worker(mock.Mock(control_env=root / "relay.env"))
            self.assertEqual(rc, 2)
            run_one_slice.assert_called_once()

    def test_json_from_stdout_accepts_pretty_printed_json(self) -> None:
        payload = collector.json_from_stdout('info line\n{\n  "free_mb_output": 10038,\n  "ok": true\n}\n')
        self.assertEqual(payload["free_mb_output"], 10038)
        self.assertTrue(payload["ok"])

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

    def test_provider_quarantined_no_count_row_is_not_counted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                collector.write_csv_rows(
                    root / "slice_summaries.csv",
                    [
                        {
                            "slice_id": "slice-1",
                            "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                        },
                        {
                            "slice_id": "slice-2",
                            "classification": "RELAY_COLLECTION_PASS_PROVIDER_GAP_QUARANTINED_NO_COUNT",
                        },
                    ],
                    collector.SLICE_FIELDS,
                )
                status = collector.aggregate_status({})
            self.assertEqual(status["slices_attempted"], 2)
            self.assertEqual(status["counted_slices"], 1)

    def test_run_one_slice_mirrors_safe_provider_quarantine_and_continues(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)

            def fake_run(cmd, **kwargs):  # noqa: ANN001 - test shim.
                batch_dir = pathlib.Path(cmd[cmd.index("--batch-log-dir") + 1])
                batch_dir.mkdir(parents=True)
                (batch_dir / "batch_summary.ndjson").write_text(
                    json.dumps(
                        {
                            "run": "slice-provider-quarantined",
                            "relay_session_id": "relay-1",
                            "classification": "RELAY_COLLECTION_PASS_PROVIDER_GAP_QUARANTINED_NO_COUNT",
                            "counted_phase107b_result": False,
                            "partial_outputs_audit_only": True,
                            "safe_provider_quarantine_no_count": True,
                            "provider_blocker_class": "provider_reconnect_exhausted",
                            "candidate_checkpoint_count": 0,
                            "replay_eligible_candidate_count": 0,
                            "sequence_gap_count": 0,
                            "hash_mismatch_count": 0,
                            "malformed_frame_count": 0,
                            "receiver_backpressure_count": 0,
                            "receiver_unavailable_count": 0,
                            "r2_failed": 0,
                            "r2_streaming_unverified_chunks": 0,
                            "artifact_consistency_ok": True,
                            "vps_safety": {
                                "forbidden_recent": 0,
                                "material_candidate_service": "inactive",
                                "material_hunter_service": "inactive",
                            },
                        }
                    )
                    + "\n"
                )
                return type("Proc", (), {"returncode": 4})()

            with self.patch_paths(root):
                with mock.patch.object(collector, "ensure_local_storage_ready", return_value={"ok": True}), mock.patch.object(
                    collector.subprocess,
                    "run",
                    side_effect=fake_run,
                ), mock.patch.object(collector, "run_reports", return_value={"ok": True}), mock.patch.object(
                    collector, "current_high_positive_count", return_value=4
                ):
                    rc = collector.run_one_slice(pathlib.Path("env"), 1, 1)
            self.assertEqual(rc, 0)
            status = json.loads((root / "status.json").read_text())
            self.assertEqual(status["state"], "running")
            self.assertEqual(status["blocker"], "")
            self.assertEqual(status["last_slice_returncode"], 4)
            self.assertEqual(status["slices_attempted"], 1)
            self.assertEqual(status["counted_slices"], 0)
            with (root / "slice_summaries.csv").open() as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["classification"], "RELAY_COLLECTION_PASS_PROVIDER_GAP_QUARANTINED_NO_COUNT")

    def test_live_summary_uses_top_level_storage_fallbacks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                collector.write_live_summary(
                    {
                        "state": "blocked",
                        "classification": "BACKGROUND_24H_COLLECTION_BLOCK_RELAY",
                        "pid": None,
                        "process_alive": False,
                        "local_collector_usage_mb": 8,
                        "max_local_collector_usage_mb": 5000,
                        "local_spool_bytes_current": 0,
                        "local_spool_bytes_peak": 33554432,
                        "local_spool_bytes_limit": 67108864,
                        "local_disk_free_mb": 121917,
                        "r2_streaming_uploaded_chunks": 28,
                        "r2_streaming_verified_chunks": 28,
                        "r2_streaming_deleted_local_chunks": 28,
                        "r2_streaming_unverified_chunks": 0,
                    }
                )
            text = (root / "live_summary.md").read_text()
            self.assertIn("- process_alive: `false`", text)
            self.assertIn("- local_collector_usage_mb: `8`", text)
            self.assertIn("- local_spool_bytes_peak: `33554432`", text)
            self.assertIn("- local_spool_bytes_limit: `67108864`", text)
            self.assertIn("- local_disk_free_mb: `121917`", text)

    def test_gc_dry_run_does_not_delete_verified_bulk(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "local_stream_collector"
            report = pathlib.Path(tmp) / "reports"
            run = self.write_verified_run(root)
            args = type("Args", (), {"output_root": root, "report_root": report, "min_free_mb": 10000, "apply": False, "skip_validator": True, "include_operator_review_tier": False})()
            audit = storage_gc.build_audit(args)
            storage_gc.write_outputs(args, audit, [])
            self.assertTrue((run / "relay_frames" / "part-000001.ndjson").exists())
            self.assertGreater(audit["safe_delete_bytes"], 0)
            self.assertTrue((report / "local_r2_primary_storage_gc_dry_run.json").exists())
            self.assertTrue((report / "local_r2_primary_storage_gc_v2_dry_run.json").exists())
            self.assertTrue((report / "LOCAL_STORAGE_CAPACITY_AUDIT_V2.md").exists())

    def test_gc_apply_deletes_only_verified_bulk_and_preserves_compact_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "local_stream_collector"
            report = pathlib.Path(tmp) / "reports"
            run = self.write_verified_run(root)
            args = type("Args", (), {"output_root": root, "report_root": report, "min_free_mb": 10000, "apply": True, "skip_validator": True, "include_operator_review_tier": False})()
            audit = storage_gc.build_audit(args)
            deleted = storage_gc.apply_deletions(audit, output_root=root)
            storage_gc.write_outputs(args, audit, deleted)
            self.assertFalse((run / "relay_frames").exists())
            self.assertTrue((run / "countability_decision.json").exists())
            self.assertTrue((run / "r2_upload_result.json").exists())
            self.assertTrue((run / "attempt_ledger.csv").exists())
            self.assertGreater(sum(item["bytes"] for item in deleted), 0)
            self.assertTrue((report / "local_r2_primary_storage_gc_v2_apply.json").exists())
            self.assertTrue((report / "LOCAL_R2_PRIMARY_STORAGE_GC_V2_REPORT.md").exists())

    def test_gc_refuses_unverified_r2_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "local_stream_collector"
            run = self.write_verified_run(root)
            (run / "r2_upload_result.json").write_text(json.dumps({"verified": False, "failed_files": ["x"]}) + "\n")
            args = type("Args", (), {"output_root": root, "report_root": pathlib.Path(tmp) / "reports", "min_free_mb": 10000, "apply": False, "skip_validator": True, "include_operator_review_tier": False})()
            audit = storage_gc.build_audit(args)
            self.assertEqual(audit["safe_delete_bytes"], 0)
            self.assertIn("r2_not_verified", audit["unsafe_to_delete_candidates"][0]["unsafe_reasons"])

    def test_gc_refuses_current_inflight_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "local_stream_collector"
            run = self.write_verified_run(root, "active-run")
            with mock.patch.object(storage_gc, "pgrep_lines", return_value=[f"123 local-stream-collector {run}"]):
                args = type("Args", (), {"output_root": root, "report_root": pathlib.Path(tmp) / "reports", "min_free_mb": 10000, "apply": False, "skip_validator": True, "include_operator_review_tier": False})()
                audit = storage_gc.build_audit(args)
            self.assertEqual(audit["safe_delete_bytes"], 0)
            self.assertIn("current_or_inflight_run", audit["unsafe_to_delete_candidates"][0]["unsafe_reasons"])

    def test_below_disk_preflight_triggers_gc_before_local_disk_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                with mock.patch.object(
                    collector,
                    "run_local_r2_primary_preflight",
                    side_effect=[
                        {"returncode": 2, "free_mb_output": 8909, "free_mb_spool": 8909, "required_mb": 10000},
                        {"returncode": 0, "free_mb_output": 16000, "free_mb_spool": 16000, "required_mb": 10000},
                    ],
                ), mock.patch.object(
                    collector,
                    "run_storage_gc",
                    side_effect=[
                        {"returncode": 0, "safe_delete_bytes": 2 * 1024 * 1024 * 1024},
                        {"returncode": 0, "deleted_bytes": 2 * 1024 * 1024 * 1024},
                    ],
                ):
                    result = collector.ensure_local_storage_ready(pathlib.Path("env"))
            self.assertTrue(result["ok"])
            self.assertTrue(result["gc_ran"])

    def test_supervisor_refuses_24h_resume_below_recommended_without_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                with mock.patch.object(
                    collector,
                    "run_local_r2_primary_preflight",
                    return_value={
                        "returncode": 0,
                        "storage_mode": "r2_primary",
                        "free_mb_output": 12000,
                        "free_mb_spool": 12000,
                        "required_mb": 10000,
                    },
                ), mock.patch.object(
                    collector,
                    "run_storage_gc",
                    return_value={"returncode": 0, "safe_delete_bytes": 0},
                ), mock.patch.object(
                    collector,
                    "low_disk_override_enabled",
                    return_value=False,
                ), mock.patch.object(
                    collector,
                    "run_storage_gc",
                    return_value={"returncode": 0, "safe_delete_bytes_available": 0, "local_stream_collector_mb_after": 4000},
                ):
                    result = collector.ensure_local_storage_ready(pathlib.Path("env"))
            self.assertFalse(result["ok"])
            self.assertIn("local_disk_below_recommended_resume:12000<15000", result["blockers"])

    def test_r2_streaming_storage_ready_uses_streaming_floor_not_old_10gb_gate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                with mock.patch.object(
                    collector,
                    "run_local_r2_primary_preflight",
                    return_value={
                        "returncode": 0,
                        "storage_mode": "r2_streaming",
                        "free_mb_output": 5000,
                        "free_mb_spool": 5000,
                        "required_mb": 4096,
                    },
                ), mock.patch.object(
                    collector,
                    "low_disk_override_enabled",
                    return_value=False,
                ):
                    result = collector.ensure_local_storage_ready(pathlib.Path("env"))
            self.assertTrue(result["ok"])
            self.assertFalse(result["gc_ran"])
            self.assertEqual(result["hard_min_free_mb"], 4096)
            self.assertEqual(result["recommended_resume_free_mb"], 4096)
            self.assertEqual(result["storage_mode"], "r2_streaming")

    def test_low_disk_operator_override_allows_resume_above_hard_floor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                with mock.patch.object(
                    collector,
                    "run_local_r2_primary_preflight",
                    return_value={"returncode": 0, "free_mb_output": 12000, "free_mb_spool": 12000, "required_mb": 10000},
                ), mock.patch.object(
                    collector,
                    "low_disk_override_enabled",
                    return_value=True,
                ), mock.patch.object(
                    collector,
                    "run_storage_gc",
                    return_value={"returncode": 0, "safe_delete_bytes_available": 0, "local_stream_collector_mb_after": 4000},
                ):
                    result = collector.ensure_local_storage_ready(pathlib.Path("env"))
            self.assertTrue(result["ok"])
            self.assertFalse(result["gc_ran"])
            self.assertTrue(result["low_disk_operator_override"])

    def test_local_collector_usage_budget_triggers_enforcement_before_slice(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                with mock.patch.object(
                    collector,
                    "run_local_r2_primary_preflight",
                    return_value={
                        "returncode": 0,
                        "storage_mode": "r2_streaming",
                        "free_mb_output": 7000,
                        "free_mb_spool": 7000,
                        "required_mb": 4096,
                    },
                ), mock.patch.object(
                    collector,
                    "run_storage_gc",
                    side_effect=[
                        {"returncode": 0, "safe_delete_bytes_available": 1024, "local_stream_collector_mb_after": 6000},
                        {"returncode": 0, "deleted_bytes": 1024, "local_stream_collector_mb_after": 4200},
                    ],
                ), mock.patch.object(
                    collector,
                    "low_disk_override_enabled",
                    return_value=False,
                ):
                    result = collector.ensure_local_storage_ready(pathlib.Path("env"))
            self.assertTrue(result["ok"])
            self.assertTrue(result["gc_ran"])
            self.assertEqual(result["max_local_collector_usage_mb"], 5000)
            self.assertEqual(result["local_stream_collector_usage_mb"], 4200)

    def test_local_collector_usage_budget_blocks_if_cleanup_cannot_get_under_cap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                with mock.patch.object(
                    collector,
                    "run_local_r2_primary_preflight",
                    return_value={
                        "returncode": 0,
                        "storage_mode": "r2_streaming",
                        "free_mb_output": 7000,
                        "free_mb_spool": 7000,
                        "required_mb": 4096,
                    },
                ), mock.patch.object(
                    collector,
                    "run_storage_gc",
                    side_effect=[
                        {"returncode": 0, "safe_delete_bytes_available": 1024, "local_stream_collector_mb_after": 6000},
                        {"returncode": 0, "deleted_bytes": 1024, "local_stream_collector_mb_after": 5500},
                    ],
                ), mock.patch.object(
                    collector,
                    "low_disk_override_enabled",
                    return_value=False,
                ):
                    result = collector.ensure_local_storage_ready(pathlib.Path("env"))
            self.assertFalse(result["ok"])
            self.assertIn("local_stream_collector_usage_above_budget:5500>5000", result["blockers"])

    def test_if_gc_cannot_fix_disk_supervisor_blocks_as_local_disk_not_relay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                with mock.patch.object(
                    collector,
                    "ensure_local_storage_ready",
                    return_value={"ok": False, "blockers": ["local_output_disk_below_required:8909<10000"]},
                ):
                    rc = collector.run_one_slice(pathlib.Path("env"), 1, 1)
            self.assertEqual(rc, 3)
            status = json.loads((root / "status.json").read_text())
            self.assertEqual(status["classification"], collector.LOCAL_DISK_BLOCK_CLASSIFICATION)

    def test_resume_allowed_after_local_disk_only_block_with_clean_prior_slices(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                (root / "status.json").write_text(
                    json.dumps({"state": "blocked", "classification": collector.LOCAL_DISK_BLOCK_CLASSIFICATION, "blocker": "local_output_disk_below_required:8909<10000"}) + "\n"
                )
                collector.write_csv_rows(
                    root / "slice_summaries.csv",
                    [
                        {
                            "slice_id": "slice-1",
                            "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                            "r2_failed_files": "0",
                            "artifact_consistency_ok": "true",
                            "candidate_checkpoint_count": "0",
                            "replay_eligible_candidate_count": "0",
                        }
                    ],
                    collector.SLICE_FIELDS,
                )
                with mock.patch.object(collector, "run_local_r2_primary_preflight", return_value={"returncode": 0, "free_mb_output": 16000, "free_mb_spool": 16000, "required_mb": 10000}), mock.patch.object(collector, "supervisor_status", return_value={"vps_safety": {"forbidden_recent": 0, "relay_running": 0, "material_candidate_service": "inactive", "material_hunter_service": "inactive"}}):
                    decision = collector.resume_decision(pathlib.Path("env"))
            self.assertTrue(decision["resume_allowed"])
            self.assertEqual(decision["next_slice_index"], 2)

    def test_resume_refused_below_recommended_without_operator_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                (root / "status.json").write_text(
                    json.dumps({"state": "blocked", "classification": collector.LOCAL_DISK_CAPACITY_BLOCK_CLASSIFICATION, "blocker": "local_disk_below_recommended_resume:12000<15000"}) + "\n"
                )
                collector.write_csv_rows(
                    root / "slice_summaries.csv",
                    [
                        {
                            "slice_id": "slice-1",
                            "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                            "r2_failed_files": "0",
                            "artifact_consistency_ok": "true",
                            "candidate_checkpoint_count": "0",
                            "replay_eligible_candidate_count": "0",
                        }
                    ],
                    collector.SLICE_FIELDS,
                )
                with mock.patch.object(
                    collector,
                    "run_local_r2_primary_preflight",
                    return_value={
                        "returncode": 0,
                        "storage_mode": "r2_primary",
                        "free_mb_output": 12000,
                        "free_mb_spool": 12000,
                        "required_mb": 10000,
                    },
                ), mock.patch.object(collector, "run_storage_gc", return_value={"returncode": 0, "safe_delete_bytes": 0}), mock.patch.object(collector, "supervisor_status", return_value={"vps_safety": {"forbidden_recent": 0, "relay_running": 0, "material_candidate_service": "inactive", "material_hunter_service": "inactive"}}):
                    decision = collector.resume_decision(pathlib.Path("env"))
            self.assertFalse(decision["resume_allowed"])
            self.assertIn("local_disk_below_recommended_resume:12000<15000", decision["blockers"])

    def test_resume_command_mirrors_capacity_block_into_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                with mock.patch.object(
                    collector,
                    "resume_decision",
                    return_value={
                        "resume_allowed": False,
                        "blockers": ["local_disk_below_recommended_resume:12000<15000"],
                        "local_storage_recovery": {"ok": False},
                    },
                ):
                    rc = collector.resume(type("Args", (), {"control_env": pathlib.Path("env")})())
            self.assertEqual(rc, 2)
            status = json.loads((root / "status.json").read_text())
            self.assertEqual(status["classification"], collector.LOCAL_DISK_CAPACITY_BLOCK_CLASSIFICATION)
            self.assertIn("local_disk_below_recommended_resume", status["blocker"])

    def test_output_spool_root_override_rejects_codex_runtime_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = pathlib.Path(tmp)
            bad = repo / ".codex_runtime_env" / "spool"
            bad.mkdir(parents=True)
            with mock.patch.object(collector, "REPO", repo):
                validation = collector.validate_output_spool_overrides(
                    {
                        collector.OUTPUT_ROOT_ENV: str(bad),
                        collector.SPOOL_ROOT_ENV: str(bad),
                    }
                )
            self.assertFalse(validation["ok"])
            self.assertIn("output_root_inside_codex_runtime_env", validation["blockers"])
            self.assertIn("spool_root_inside_codex_runtime_env", validation["blockers"])

    def test_run_one_slice_passes_r2_streaming_storage_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            captured: dict[str, object] = {}

            def fake_run(cmd, **kwargs):  # noqa: ANN001 - test shim.
                captured["cmd"] = cmd
                return type("Proc", (), {"returncode": 1})()

            with self.patch_paths(root):
                with mock.patch.object(collector, "ensure_local_storage_ready", return_value={"ok": True}), mock.patch.object(
                    collector.subprocess,
                    "run",
                    side_effect=fake_run,
                ), mock.patch.object(collector, "run_reports", return_value={"ok": True}):
                    rc = collector.run_one_slice(pathlib.Path("env"), 1, 1)
            self.assertEqual(rc, 1)
            cmd = captured["cmd"]
            self.assertIsInstance(cmd, list)
            self.assertIn("--storage-mode", cmd)
            self.assertIn("r2-streaming", cmd)
            self.assertIn("--r2-streaming-spool-mb", cmd)
            self.assertIn("--r2-streaming-min-free-mb", cmd)
            self.assertIn("--r2-streaming-chunk-mb", cmd)

    def test_resume_refused_after_r2_artifact_or_candidate_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.patch_paths(root):
                (root / "status.json").write_text(
                    json.dumps({"state": "blocked", "classification": collector.LOCAL_DISK_BLOCK_CLASSIFICATION}) + "\n"
                )
                collector.write_csv_rows(
                    root / "slice_summaries.csv",
                    [
                        {
                            "slice_id": "slice-1",
                            "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                            "r2_failed_files": "1",
                            "artifact_consistency_ok": "true",
                            "candidate_checkpoint_count": "0",
                            "replay_eligible_candidate_count": "0",
                        }
                    ],
                    collector.SLICE_FIELDS,
                )
                with mock.patch.object(collector, "run_local_r2_primary_preflight", return_value={"returncode": 0, "free_mb_output": 12000, "free_mb_spool": 12000, "required_mb": 10000}), mock.patch.object(collector, "supervisor_status", return_value={"vps_safety": {"forbidden_recent": 0, "relay_running": 0, "material_candidate_service": "inactive", "material_hunter_service": "inactive"}}):
                    decision = collector.resume_decision(pathlib.Path("env"))
            self.assertFalse(decision["resume_allowed"])
            self.assertIn("prior_r2_failures_present", decision["blockers"])

    def test_resume_allowed_after_recovered_zero_attempt_provider_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "status"
            repo = pathlib.Path(tmp) / "repo"
            run_root = repo / "research_output" / "local_stream_collector"
            recovered = run_root / "background-24h-early-burst-20260624T105439Z"
            recovered.mkdir(parents=True)
            root.mkdir(parents=True, exist_ok=True)
            (recovered / "local_relay_dataset_proof_summary.json").write_text(
                json.dumps(
                    {
                        "classification": "RELAY_LOCAL_DATASET_BLOCK_PROVIDER",
                        "provider_blocker_class": "provider_reconnect_exhausted",
                        "attempted_launches": 0,
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                        "sequence_gap_count": 0,
                        "hash_mismatch_count": 0,
                        "malformed_frame_count": 0,
                        "receiver_backpressure_count": 0,
                        "receiver_unavailable_count": 0,
                    }
                )
                + "\n"
            )
            (recovered / "local_collector_exit_status.json").write_text(json.dumps({}) + "\n")
            (recovered / "r2_upload_result.json").write_text(json.dumps({"verified": True, "failed_files": []}) + "\n")
            (recovered / "local_retention_summary.json").write_text(json.dumps({"ok": True}) + "\n")
            (recovered / "r2_streaming_upload_manifest.json").write_text(json.dumps({"unverified_chunks": 0}) + "\n")
            (recovered / "service_exit_status.json").write_text(
                json.dumps({"service_exit_reason": "local_relay_collector_completed"}) + "\n"
            )
            with self.patch_paths(root), mock.patch.object(collector, "REPO", repo):
                (root / "status.json").write_text(
                    json.dumps(
                        {
                            "state": "blocked",
                            "classification": "BACKGROUND_24H_COLLECTION_BLOCK_RELAY",
                            "blocker": "supervisor_slice_failed",
                            "pid": None,
                        }
                    )
                    + "\n"
                )
                collector.write_csv_rows(
                    root / "slice_summaries.csv",
                    [
                        {
                            "slice_id": "background-24h-early-burst-20260624T103603Z",
                            "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                            "r2_failed_files": "0",
                            "artifact_consistency_ok": "true",
                            "candidate_checkpoint_count": "0",
                            "replay_eligible_candidate_count": "0",
                        }
                    ],
                    collector.SLICE_FIELDS,
                )
                with mock.patch.object(
                    collector,
                    "ensure_local_storage_ready",
                    return_value={
                        "ok": True,
                        "storage_mode": "r2_streaming",
                        "preflight_after": {"returncode": 0, "free_mb_output": 6000, "required_mb": 4096},
                    },
                ), mock.patch.object(
                    collector,
                    "supervisor_status",
                    return_value={
                        "vps_safety": {
                            "forbidden_recent": 0,
                            "relay_running": 0,
                            "material_candidate_service": "inactive",
                            "material_hunter_service": "inactive",
                        }
                    },
                ):
                    decision = collector.resume_decision(pathlib.Path("env"))
            self.assertTrue(decision["resume_allowed"])
            self.assertTrue(decision["previous_stop_was_recovered_provider_only"])
            self.assertEqual(decision["next_slice_index"], 2)

    def test_resume_mirrors_recovered_provider_quarantine_without_counting_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "status"
            repo = pathlib.Path(tmp) / "repo"
            run_root = repo / "research_output" / "local_stream_collector"
            recovered = run_root / "background-24h-early-burst-20260624T201405Z"
            batch_log = root / "batch_002" / "slice_014_20260624T201403Z"
            recovered.mkdir(parents=True)
            batch_log.mkdir(parents=True)
            root.mkdir(parents=True, exist_ok=True)
            (recovered / "local_relay_dataset_proof_summary.json").write_text(
                json.dumps(
                    {
                        "classification": "RELAY_LOCAL_DATASET_BLOCK_PROVIDER",
                        "provider_blocker_class": "provider_reconnect_exhausted",
                        "attempted_launches": 4,
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                        "sequence_gap_count": 0,
                        "hash_mismatch_count": 0,
                        "malformed_frame_count": 0,
                        "receiver_backpressure_count": 0,
                        "receiver_unavailable_count": 0,
                    }
                )
                + "\n"
            )
            (recovered / "local_collector_exit_status.json").write_text(json.dumps({}) + "\n")
            (recovered / "r2_upload_result.json").write_text(json.dumps({"verified": True, "failed_files": []}) + "\n")
            (recovered / "local_retention_summary.json").write_text(json.dumps({"ok": True}) + "\n")
            (recovered / "r2_streaming_upload_manifest.json").write_text(json.dumps({"unverified_chunks": 0}) + "\n")
            (recovered / "service_exit_status.json").write_text(
                json.dumps({"service_exit_reason": "local_relay_collector_completed"}) + "\n"
            )
            (batch_log / "batch_summary.ndjson").write_text(
                json.dumps(
                    {
                        "run": recovered.name,
                        "relay_session_id": "relay-1",
                        "classification": "RELAY_COLLECTION_PASS_PROVIDER_GAP_QUARANTINED_NO_COUNT",
                        "counted_phase107b_result": False,
                        "partial_outputs_audit_only": True,
                        "safe_provider_quarantine_no_count": True,
                        "provider_blocker_class": "provider_reconnect_exhausted",
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                        "sequence_gap_count": 0,
                        "hash_mismatch_count": 0,
                        "malformed_frame_count": 0,
                        "receiver_backpressure_count": 0,
                        "receiver_unavailable_count": 0,
                        "r2_failed": 0,
                        "r2_streaming_unverified_chunks": 0,
                        "artifact_consistency_ok": True,
                        "vps_safety": {
                            "forbidden_recent": 0,
                            "material_candidate_service": "inactive",
                            "material_hunter_service": "inactive",
                        },
                    }
                )
                + "\n"
            )
            with self.patch_paths(root), mock.patch.object(collector, "REPO", repo):
                (root / "status.json").write_text(
                    json.dumps(
                        {
                            "state": "blocked",
                            "classification": "BACKGROUND_24H_COLLECTION_BLOCK_RELAY",
                            "blocker": "supervisor_slice_failed",
                            "pid": None,
                        }
                    )
                    + "\n"
                )
                collector.write_csv_rows(
                    root / "slice_summaries.csv",
                    [
                        {
                            "slice_id": "background-24h-early-burst-20260624T103603Z",
                            "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                            "r2_failed_files": "0",
                            "artifact_consistency_ok": "true",
                            "candidate_checkpoint_count": "0",
                            "replay_eligible_candidate_count": "0",
                        }
                    ],
                    collector.SLICE_FIELDS,
                )
                with mock.patch.object(
                    collector,
                    "ensure_local_storage_ready",
                    return_value={
                        "ok": True,
                        "storage_mode": "r2_streaming",
                        "preflight_after": {"returncode": 0, "free_mb_output": 6000, "required_mb": 4096},
                    },
                ), mock.patch.object(
                    collector,
                    "supervisor_status",
                    return_value={
                        "vps_safety": {
                            "forbidden_recent": 0,
                            "relay_running": 0,
                            "material_candidate_service": "inactive",
                            "material_hunter_service": "inactive",
                        }
                    },
                ), mock.patch.object(collector, "current_high_positive_count", return_value=4):
                    rc = collector.resume(type("Args", (), {"control_env": pathlib.Path("env")})())
            self.assertEqual(rc, 0)
            with (root / "slice_summaries.csv").open() as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[1]["slice_id"], recovered.name)
            payload = json.loads((root / "status.json").read_text())
            self.assertEqual(payload["slices_attempted"], 2)
            self.assertEqual(payload["counted_slices"], 1)
            self.assertEqual(payload["state"], "ready_to_resume")

    def test_resume_mirrors_recovered_zero_attempt_provider_gap_no_signal_as_counted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "status"
            repo = pathlib.Path(tmp) / "repo"
            run_root = repo / "research_output" / "local_stream_collector"
            recovered = run_root / "background-24h-early-burst-20260625T123655Z"
            batch_log = root / "batch_003" / "slice_029_20260625T123653Z"
            recovered.mkdir(parents=True)
            batch_log.mkdir(parents=True)
            root.mkdir(parents=True, exist_ok=True)
            (recovered / "local_relay_dataset_proof_summary.json").write_text(
                json.dumps(
                    {
                        "classification": "RELAY_LOCAL_DATASET_BLOCK_COUNTABILITY",
                        "counted_phase107b_result": False,
                        "attempted_launches": 0,
                        "provider_data_loss_seen": False,
                        "provider_blocker_class": None,
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                        "upstream_provider_blocker_count": 2,
                        "upstream_reconnect_count": 2,
                        "upstream_reconnect_exhausted_count": 0,
                        "sequence_gap_count": 0,
                        "hash_mismatch_count": 0,
                        "malformed_frame_count": 0,
                        "receiver_backpressure_count": 0,
                        "receiver_unavailable_count": 0,
                    }
                )
                + "\n"
            )
            (recovered / "countability_decision.json").write_text(
                json.dumps(
                    {
                        "counted_phase107b_result": False,
                        "provider_data_loss_seen": False,
                        "provider_blocker_class": None,
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                        "off_vps_candidate_replay_allowed": False,
                        "run_provider_data_loss_seen": True,
                    }
                )
                + "\n"
            )
            (recovered / "local_collector_exit_status.json").write_text(
                json.dumps({"upstream_provider_blocker_count": 2, "upstream_reconnect_exhausted_count": 0}) + "\n"
            )
            (recovered / "r2_upload_result.json").write_text(json.dumps({"verified": True, "failed_files": []}) + "\n")
            (recovered / "local_retention_summary.json").write_text(json.dumps({"ok": True}) + "\n")
            (recovered / "r2_streaming_upload_manifest.json").write_text(
                json.dumps({"verified_chunks": 16, "unverified_chunks": 0}) + "\n"
            )
            (recovered / "service_exit_status.json").write_text(
                json.dumps({"service_exit_reason": "local_relay_collector_completed"}) + "\n"
            )
            (batch_log / "batch_stop.json").write_text(
                json.dumps(
                    {
                        "blockers": ["not_counted"],
                        "result": {
                            "run": recovered.name,
                            "relay_session_id": "relay-1",
                            "classification": "RELAY_LOCAL_DATASET_BLOCK_COUNTABILITY",
                            "counted_phase107b_result": False,
                            "attempted_launches": 0,
                            "all_launches_seen": 96,
                            "all_launches_indexed": 96,
                            "cheap_followup_rows": 672,
                            "rich_tracked_launches": 0,
                            "candidate_checkpoint_count": 0,
                            "replay_eligible_candidate_count": 0,
                            "off_vps_candidate_replay_allowed": False,
                            "sequence_gap_count": 0,
                            "hash_mismatch_count": 0,
                            "malformed_frame_count": 0,
                            "receiver_backpressure_count": 0,
                            "receiver_unavailable_count": 0,
                            "upstream_provider_blocker_count": 2,
                            "upstream_reconnect_count": 2,
                            "upstream_reconnect_exhausted_count": 0,
                            "provider_data_loss_seen": False,
                            "provider_blocker_class": None,
                            "r2_failed": 0,
                            "r2_streaming_uploaded_chunks": 16,
                            "r2_streaming_verified_chunks": 16,
                            "r2_streaming_deleted_local_chunks": 16,
                            "r2_streaming_unverified_chunks": 0,
                            "r2_streaming_upload_timeout_count": 0,
                            "r2_streaming_backpressure_detected": False,
                            "artifact_consistency_ok": True,
                            "remote_rc": 0,
                            "local_rc": 0,
                            "vps_safety": {
                                "forbidden_recent": 0,
                                "relay_running": 0,
                                "material_candidate_service": "inactive",
                                "material_hunter_service": "inactive",
                            },
                        },
                    }
                )
                + "\n"
            )
            with self.patch_paths(root), mock.patch.object(collector, "REPO", repo):
                (root / "status.json").write_text(
                    json.dumps(
                        {
                            "state": "blocked",
                            "classification": "BACKGROUND_24H_COLLECTION_BLOCK_RELAY",
                            "blocker": "supervisor_slice_failed",
                            "pid": None,
                        }
                    )
                    + "\n"
                )
                collector.write_csv_rows(
                    root / "slice_summaries.csv",
                    [
                        {
                            "slice_id": "background-24h-early-burst-20260624T103603Z",
                            "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                            "r2_failed_files": "0",
                            "artifact_consistency_ok": "true",
                            "candidate_checkpoint_count": "0",
                            "replay_eligible_candidate_count": "0",
                        }
                    ],
                    collector.SLICE_FIELDS,
                )
                with mock.patch.object(
                    collector,
                    "ensure_local_storage_ready",
                    return_value={
                        "ok": True,
                        "storage_mode": "r2_streaming",
                        "preflight_after": {"returncode": 0, "free_mb_output": 6000, "required_mb": 4096},
                    },
                ), mock.patch.object(
                    collector,
                    "supervisor_status",
                    return_value={
                        "vps_safety": {
                            "forbidden_recent": 0,
                            "relay_running": 0,
                            "material_candidate_service": "inactive",
                            "material_hunter_service": "inactive",
                        }
                    },
                ), mock.patch.object(collector, "current_high_positive_count", return_value=4):
                    rc = collector.resume(type("Args", (), {"control_env": pathlib.Path("env")})())
            self.assertEqual(rc, 0)
            with (root / "slice_summaries.csv").open() as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[1]["slice_id"], recovered.name)
            self.assertEqual(rows[1]["classification"], "RELAY_COLLECTION_PASS_PROVIDER_GAP_CONTINUED")
            payload = json.loads((root / "status.json").read_text())
            self.assertEqual(payload["slices_attempted"], 2)
            self.assertEqual(payload["counted_slices"], 2)
            self.assertEqual(payload["state"], "ready_to_resume")
            self.assertTrue(payload["last_resume_decision"]["previous_stop_was_recovered_zero_attempt_no_signal"])

    def test_resume_recovers_zero_attempt_provider_finalization_hang(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "status"
            repo = pathlib.Path(tmp) / "repo"
            run_root = repo / "research_output" / "local_stream_collector"
            recovered = run_root / "background-24h-early-burst-20260625T215545Z"
            batch_log = root / "batch_004" / "slice_039_20260625T214023Z"
            recovered.mkdir(parents=True)
            batch_log.mkdir(parents=True)
            root.mkdir(parents=True, exist_ok=True)
            (recovered / "local_collector_summary.json").write_text(
                json.dumps(
                    {
                        "relay_session_id": "relay-1",
                        "subscription_fingerprint": "sub",
                        "frames_received": 15370,
                        "data_frames_received": 15365,
                        "control_frames_received": 5,
                        "sequence_gap_count": 0,
                        "hash_mismatch_count": 0,
                        "malformed_frame_count": 0,
                        "downstream_backpressure_count": 0,
                        "receiver_unavailable_count": 0,
                        "upstream_provider_blocker_count": 2,
                        "upstream_reconnect_count": 1,
                        "upstream_reconnect_exhausted_count": 1,
                        "r2_streaming_upload_timeout_count": 0,
                        "r2_streaming_backpressure_detected": False,
                        "local_spool_bytes_peak": 1024,
                        "local_spool_bytes_limit": 2048,
                    }
                )
                + "\n"
            )
            (recovered / "local_collector_exit_status.json").write_text(
                json.dumps(
                    {
                        "ok": True,
                        "sequence_gap_count": 0,
                        "hash_mismatch_count": 0,
                        "malformed_frame_count": 0,
                        "receiver_unavailable_count": 0,
                    }
                )
                + "\n"
            )
            (recovered / "hunter_summary.json").write_text(
                json.dumps(
                    {
                        "attempted_launches": 0,
                        "rich_tracked_launches": 0,
                        "all_launches_seen": 37,
                        "all_launches_indexed": 37,
                        "cheap_only_launches": 37,
                        "cheap_followup_rows": 259,
                        "promotion_recommended_count": 26,
                        "promotion_admitted_count": 0,
                        "promotion_blocked_budget_count": 0,
                        "provider_blocker_class": "provider_reconnect_exhausted",
                        "provider_data_loss_seen": True,
                        "early_burst_review_candidate_count": 0,
                    }
                )
                + "\n"
            )
            (recovered / "countability_decision.json").write_text(
                json.dumps(
                    {
                        "counted_phase107b_result": False,
                        "partial_outputs_audit_only": True,
                        "provider_data_loss_seen": True,
                        "provider_blocker_class": "provider_reconnect_exhausted",
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                        "off_vps_candidate_replay_allowed": False,
                        "clean_segment_count": 0,
                        "blocked_segment_count": 2,
                    }
                )
                + "\n"
            )
            (recovered / "run_countability_decision.json").write_text(
                json.dumps(
                    {
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                        "clean_segment_count": 0,
                        "blocked_segment_count": 2,
                    }
                )
                + "\n"
            )
            (recovered / "r2_upload_result.json").write_text(
                json.dumps({"verified": True, "failed_files": [], "uploaded_files": ["k"]}) + "\n"
            )
            (recovered / "relay_frame_manifest.json").write_text(
                json.dumps(
                    {
                        "storage_mode": "r2_streaming",
                        "streaming_shards": [
                            {
                                "uploaded": True,
                                "verified": True,
                                "local_deleted": True,
                                "local_path": str(recovered / "relay_frames" / "part-000001.ndjson"),
                                "object_key": "r2/key",
                            }
                        ],
                    }
                )
                + "\n"
            )
            (batch_log / "batch_stop.json").write_text(
                json.dumps(
                    {
                        "blockers": ["retention_not_ok", "artifact_consistency", "not_counted", "local_rc", "remote_rc"],
                        "result": {
                            "run": recovered.name,
                            "relay_session_id": "relay-1",
                            "classification": "RELAY_LOCAL_DATASET_BLOCK_R2",
                            "counted_phase107b_result": False,
                            "attempted_launches": 0,
                            "all_launches_seen": 37,
                            "all_launches_indexed": 37,
                            "cheap_followup_rows": 259,
                            "rich_tracked_launches": 0,
                            "candidate_checkpoint_count": 0,
                            "replay_eligible_candidate_count": 0,
                            "off_vps_candidate_replay_allowed": False,
                            "sequence_gap_count": 0,
                            "hash_mismatch_count": 0,
                            "malformed_frame_count": 0,
                            "receiver_backpressure_count": 0,
                            "receiver_unavailable_count": 0,
                            "upstream_provider_blocker_count": 2,
                            "upstream_reconnect_count": 1,
                            "upstream_reconnect_exhausted_count": 1,
                            "provider_data_loss_seen": True,
                            "provider_blocker_class": "provider_reconnect_exhausted",
                            "r2_failed": 0,
                            "r2_streaming_uploaded_chunks": 1,
                            "r2_streaming_verified_chunks": 1,
                            "r2_streaming_deleted_local_chunks": 1,
                            "r2_streaming_unverified_chunks": 0,
                            "artifact_consistency_ok": False,
                            "remote_rc": 1,
                            "local_rc": -9,
                            "vps_safety": {
                                "forbidden_recent": 0,
                                "relay_running": 0,
                                "material_candidate_service": "inactive",
                                "material_hunter_service": "inactive",
                            },
                        },
                    }
                )
                + "\n"
            )
            with self.patch_paths(root), mock.patch.object(collector, "REPO", repo):
                (root / "status.json").write_text(
                    json.dumps(
                        {
                            "state": "blocked",
                            "classification": "BACKGROUND_24H_COLLECTION_BLOCK_RELAY",
                            "blocker": "supervisor_slice_failed",
                            "pid": None,
                        }
                    )
                    + "\n"
                )
                collector.write_csv_rows(
                    root / "slice_summaries.csv",
                    [
                        {
                            "slice_id": "background-24h-early-burst-20260625T212133Z",
                            "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                            "r2_failed_files": "0",
                            "artifact_consistency_ok": "true",
                            "candidate_checkpoint_count": "0",
                            "replay_eligible_candidate_count": "0",
                        }
                    ],
                    collector.SLICE_FIELDS,
                )
                validator = mock.Mock(returncode=0, stdout=json.dumps({"ok": True, "blockers": []}), stderr="")
                with mock.patch.object(collector, "run_capture", return_value=validator), mock.patch.object(
                    collector,
                    "ensure_local_storage_ready",
                    return_value={
                        "ok": True,
                        "storage_mode": "r2_streaming",
                        "preflight_after": {"returncode": 0, "free_mb_output": 6000, "required_mb": 4096},
                    },
                ), mock.patch.object(
                    collector,
                    "supervisor_status",
                    return_value={
                        "vps_safety": {
                            "forbidden_recent": 0,
                            "relay_running": 0,
                            "material_candidate_service": "inactive",
                            "material_hunter_service": "inactive",
                        }
                    },
                ), mock.patch.object(collector, "current_high_positive_count", return_value=4):
                    rc = collector.resume(type("Args", (), {"control_env": pathlib.Path("env")})())
            self.assertEqual(rc, 0)
            self.assertTrue((recovered / "service_exit_status.json").exists())
            self.assertTrue((recovered / "local_relay_dataset_proof_summary.json").exists())
            with (root / "slice_summaries.csv").open() as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[1]["slice_id"], recovered.name)
            self.assertEqual(rows[1]["classification"], "RELAY_COLLECTION_PASS_PROVIDER_GAP_QUARANTINED_NO_COUNT")
            payload = json.loads((root / "status.json").read_text())
            self.assertEqual(payload["state"], "ready_to_resume")
            self.assertTrue(payload["last_resume_decision"]["previous_stop_was_recovered_zero_attempt_finalization_hang"])

    def test_resume_mirrors_clean_remote_broken_pipe_slice_as_counted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "status"
            repo = pathlib.Path(tmp) / "repo"
            run_root = repo / "research_output" / "local_stream_collector"
            recovered = run_root / "background-24h-early-burst-20260625T072427Z"
            logs = run_root / "background-24h-early-burst-20260625T072427Z-logs"
            batch_log = root / "batch_002" / "slice_015_20260625T072425Z"
            recovered.mkdir(parents=True)
            logs.mkdir(parents=True)
            batch_log.mkdir(parents=True)
            root.mkdir(parents=True, exist_ok=True)
            (recovered / "local_relay_dataset_proof_summary.json").write_text(
                json.dumps(
                    {
                        "classification": "RELAY_LOCAL_DATASET_PASS",
                        "counted_phase107b_result": True,
                        "provider_data_loss_seen": False,
                        "provider_blocker_class": None,
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                        "upstream_provider_blocker_count": 1,
                        "sequence_gap_count": 0,
                        "hash_mismatch_count": 0,
                        "malformed_frame_count": 0,
                        "receiver_backpressure_count": 0,
                        "receiver_unavailable_count": 0,
                    }
                )
                + "\n"
            )
            (recovered / "countability_decision.json").write_text(
                json.dumps(
                    {
                        "counted_phase107b_result": True,
                        "provider_data_loss_seen": False,
                        "provider_blocker_class": None,
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                    }
                )
                + "\n"
            )
            (recovered / "local_collector_exit_status.json").write_text(json.dumps({}) + "\n")
            (recovered / "r2_upload_result.json").write_text(json.dumps({"verified": True, "failed_files": []}) + "\n")
            (recovered / "local_retention_summary.json").write_text(json.dumps({"ok": True}) + "\n")
            (recovered / "r2_streaming_upload_manifest.json").write_text(json.dumps({"unverified_chunks": 0}) + "\n")
            (recovered / "service_exit_status.json").write_text(
                json.dumps({"service_exit_reason": "local_relay_collector_completed"}) + "\n"
            )
            (logs / "vps_after.txt").write_text("relay_err_tail\nError: Broken pipe (os error 32)\n")
            (batch_log / "batch_stop.json").write_text(
                json.dumps(
                    {
                        "blockers": ["remote_rc"],
                        "result": {
                            "run": recovered.name,
                            "relay_session_id": "relay-1",
                            "classification": "RELAY_LOCAL_DATASET_BLOCK_ORCHESTRATION",
                            "counted_phase107b_result": True,
                            "candidate_checkpoint_count": 0,
                            "replay_eligible_candidate_count": 0,
                            "sequence_gap_count": 0,
                            "hash_mismatch_count": 0,
                            "malformed_frame_count": 0,
                            "receiver_backpressure_count": 0,
                            "receiver_unavailable_count": 0,
                            "upstream_provider_blocker_count": 1,
                            "provider_data_loss_seen": False,
                            "provider_blocker_class": None,
                            "r2_failed": 0,
                            "r2_streaming_unverified_chunks": 0,
                            "artifact_consistency_ok": True,
                            "remote_rc": 1,
                            "local_rc": 0,
                            "vps_safety": {
                                "forbidden_recent": 0,
                                "relay_running": 0,
                                "material_candidate_service": "inactive",
                                "material_hunter_service": "inactive",
                            },
                        },
                    }
                )
                + "\n"
            )
            with self.patch_paths(root), mock.patch.object(collector, "REPO", repo):
                (root / "status.json").write_text(
                    json.dumps(
                        {
                            "state": "blocked",
                            "classification": "BACKGROUND_24H_COLLECTION_BLOCK_RELAY",
                            "blocker": "supervisor_slice_failed",
                            "pid": None,
                        }
                    )
                    + "\n"
                )
                collector.write_csv_rows(
                    root / "slice_summaries.csv",
                    [
                        {
                            "slice_id": "background-24h-early-burst-20260624T103603Z",
                            "classification": "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
                            "r2_failed_files": "0",
                            "artifact_consistency_ok": "true",
                            "candidate_checkpoint_count": "0",
                            "replay_eligible_candidate_count": "0",
                        }
                    ],
                    collector.SLICE_FIELDS,
                )
                with mock.patch.object(
                    collector,
                    "ensure_local_storage_ready",
                    return_value={
                        "ok": True,
                        "storage_mode": "r2_streaming",
                        "preflight_after": {"returncode": 0, "free_mb_output": 6000, "required_mb": 4096},
                    },
                ), mock.patch.object(
                    collector,
                    "supervisor_status",
                    return_value={
                        "vps_safety": {
                            "forbidden_recent": 0,
                            "relay_running": 0,
                            "material_candidate_service": "inactive",
                            "material_hunter_service": "inactive",
                        }
                    },
                ), mock.patch.object(collector, "current_high_positive_count", return_value=4):
                    rc = collector.resume(type("Args", (), {"control_env": pathlib.Path("env")})())
            self.assertEqual(rc, 0)
            with (root / "slice_summaries.csv").open() as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[1]["slice_id"], recovered.name)
            self.assertEqual(rows[1]["classification"], "RELAY_COLLECTION_PASS_PROVIDER_GAP_CONTINUED")
            payload = json.loads((root / "status.json").read_text())
            self.assertEqual(payload["slices_attempted"], 2)
            self.assertEqual(payload["counted_slices"], 2)
            self.assertEqual(payload["state"], "ready_to_resume")

    def test_resume_refuses_recovered_provider_block_with_unverified_streaming_chunk(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "status"
            repo = pathlib.Path(tmp) / "repo"
            recovered = repo / "research_output" / "local_stream_collector" / "background-24h-early-burst-20260624T105439Z"
            recovered.mkdir(parents=True)
            root.mkdir(parents=True)
            (recovered / "local_relay_dataset_proof_summary.json").write_text(
                json.dumps(
                    {
                        "classification": "RELAY_LOCAL_DATASET_BLOCK_PROVIDER",
                        "provider_blocker_class": "provider_reconnect_exhausted",
                        "attempted_launches": 0,
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                    }
                )
                + "\n"
            )
            (recovered / "local_collector_exit_status.json").write_text(json.dumps({}) + "\n")
            (recovered / "r2_upload_result.json").write_text(json.dumps({"verified": True, "failed_files": []}) + "\n")
            (recovered / "local_retention_summary.json").write_text(json.dumps({"ok": True}) + "\n")
            (recovered / "r2_streaming_upload_manifest.json").write_text(json.dumps({"unverified_chunks": 1}) + "\n")
            (recovered / "service_exit_status.json").write_text(
                json.dumps({"service_exit_reason": "local_relay_collector_completed"}) + "\n"
            )
            with self.patch_paths(root), mock.patch.object(collector, "REPO", repo):
                (root / "status.json").write_text(
                    json.dumps(
                        {
                            "state": "blocked",
                            "classification": "BACKGROUND_24H_COLLECTION_BLOCK_RELAY",
                            "blocker": "supervisor_slice_failed",
                        }
                    )
                    + "\n"
                )
                collector.write_csv_rows(root / "slice_summaries.csv", [], collector.SLICE_FIELDS)
                with mock.patch.object(
                    collector,
                    "ensure_local_storage_ready",
                    return_value={
                        "ok": True,
                        "storage_mode": "r2_streaming",
                        "preflight_after": {"returncode": 0, "free_mb_output": 6000, "required_mb": 4096},
                    },
                ), mock.patch.object(
                    collector,
                    "supervisor_status",
                    return_value={
                        "vps_safety": {
                            "forbidden_recent": 0,
                            "relay_running": 0,
                            "material_candidate_service": "inactive",
                            "material_hunter_service": "inactive",
                        }
                    },
                ):
                    decision = collector.resume_decision(pathlib.Path("env"))
            self.assertFalse(decision["resume_allowed"])
            self.assertIn("previous_stop_not_proven_local_disk_or_recovered_provider_only", decision["blockers"])
            self.assertIn(
                "provider_block_r2_streaming_unverified_chunks",
                decision["provider_block_recovery"]["blockers"],
            )


if __name__ == "__main__":
    unittest.main()
