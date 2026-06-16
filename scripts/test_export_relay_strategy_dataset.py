#!/usr/bin/env python3
"""Unit tests for relay strategy export raw-frame availability auditing."""

from __future__ import annotations

import importlib.util
import json
import pathlib
import tempfile
import unittest
import zipfile


SCRIPT = pathlib.Path(__file__).with_name("export_relay_strategy_dataset.py")
SPEC = importlib.util.spec_from_file_location("relay_strategy_export", SCRIPT)
assert SPEC and SPEC.loader
exporter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(exporter)


class RawFrameExportTests(unittest.TestCase):
    def test_classifies_pruned_after_verification(self) -> None:
        classification, reconstructable, reason = exporter.classify_raw_frames(
            frames_received=100,
            local_present=False,
            r2_count=0,
            pruned=True,
            relay_manifest_present=True,
            r2_present=True,
            retention_ok=True,
            completed=True,
        )
        self.assertEqual(classification, "RAW_FRAMES_PRUNED_AFTER_VERIFICATION")
        self.assertFalse(reconstructable)
        self.assertIn("pruned", reason)

    def test_classifies_local_and_r2_available(self) -> None:
        classification, reconstructable, reason = exporter.classify_raw_frames(
            frames_received=100,
            local_present=True,
            r2_count=2,
            pruned=False,
            relay_manifest_present=True,
            r2_present=True,
            retention_ok=True,
            completed=True,
        )
        self.assertEqual(classification, "RAW_FRAMES_AVAILABLE_LOCAL_AND_R2")
        self.assertTrue(reconstructable)
        self.assertEqual(reason, "")

    def test_sanitized_sample_redacts_payload(self) -> None:
        sample = exporter.sanitize_frame(
            {
                "schema_version": "phase107g.relay_frame.v1",
                "relay_session_id": "relay-1",
                "sequence": 7,
                "payload_hash": "abc",
                "payload_len": 12,
                "payload_base64": "secret-ish-raw-payload",
                "relay_error": None,
            }
        )
        self.assertTrue(sample["payload_base64_redacted"])
        self.assertNotIn("payload_base64", sample)

    def test_main_zip_manifest_only_excludes_sample_and_raw_shards(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            export_dir = pathlib.Path(tmp) / "strategy_export_test"
            export_dir.mkdir()
            for name in exporter.REPORT_FILES:
                (export_dir / name).write_text(name)
            (export_dir / exporter.SAMPLE_FILE).write_text("{}\n")
            zip_path = exporter.create_main_zip(export_dir, "manifest-only")
            with zipfile.ZipFile(zip_path) as zipf:
                names = set(zipf.namelist())
            self.assertIn("RAW_FRAME_MANIFEST.csv", names)
            self.assertIn("RAW_FRAME_AVAILABILITY_REPORT.md", names)
            self.assertNotIn(exporter.SAMPLE_FILE, names)
            self.assertFalse(any(name.startswith("relay_frames/") for name in names))

    def test_inspect_run_sees_skipped_then_pruned_raw_frames(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = pathlib.Path(tmp) / "relay-r2-primary-batch-20260616T000000Z"
            run.mkdir()
            (run / "local_collector_summary.json").write_text(
                json.dumps(
                    {
                        "frames_received": 10,
                        "data_frames_received": 8,
                        "control_frames_received": 2,
                        "relay_session_id": "relay-1",
                        "subscription_fingerprint": "fingerprint",
                    }
                )
            )
            (run / "relay_frame_manifest.json").write_text(
                json.dumps({"frames_received": 10, "shards": ["relay_frames/part-000001.ndjson"]})
            )
            (run / "r2_upload_result.json").write_text(
                json.dumps(
                    {
                        "verified": True,
                        "uploaded_files": ["prefix/run/attempt_ledger.csv"],
                        "verified_files": ["prefix/run/attempt_ledger.csv"],
                        "skipped_files": [
                            {
                                "relative_path": "relay_frames/part-000001.ndjson",
                                "reason": "local_relay_raw_frames_transient_not_material_artifact",
                            }
                        ],
                    }
                )
            )
            (run / "local_retention_summary.json").write_text(
                json.dumps(
                    {
                        "ok": True,
                        "retention_mode": "keep_manifests_after_verified_r2",
                        "deleted_bulk_bytes": 123,
                        "local_retained_bytes": 45,
                        "deleted_bulk_paths": [
                            {
                                "path": str(run / "relay_frames"),
                                "reason": "local_relay_integrity_verified_transient_raw_frames_removed_after_r2_compact_verification",
                            }
                        ],
                    }
                )
            )
            row = exporter.inspect_run(run, {})
        self.assertEqual(row["raw_frame_classification"], "RAW_FRAMES_PRUNED_AFTER_VERIFICATION")
        self.assertTrue(row["relay_frames_pruned_by_retention"])
        self.assertFalse(row["raw_frame_reconstructable"])


if __name__ == "__main__":
    unittest.main()
