#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import json
import pathlib
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).with_name("recover_r2_streaming_run.py")
SPEC = importlib.util.spec_from_file_location("recover_r2_streaming_run", SCRIPT)
recover = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(recover)


class R2StreamingRecoveryTests(unittest.TestCase):
    def write_common_manifests(self, run: pathlib.Path, *, verified: bool) -> None:
        for name in (
            "artifact_stream_manifest.json",
            "material_artifact_manifest.json",
            "local_spool_manifest.json",
            "countability_decision.json",
            "run_countability_decision.json",
        ):
            (run / name).write_text(json.dumps({"schema_version": "test"}) + "\n")
        (run / "r2_streaming_upload_manifest.json").write_text(
            json.dumps({"storage_mode": "r2_streaming", "verified_chunks": 1 if verified else 0}) + "\n"
        )
        (run / "r2_upload_result.json").write_text(
            json.dumps({"verified": True, "failed_files": []}) + "\n"
        )

    def test_recovery_classifies_clean_compact_manifest_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = pathlib.Path(tmp) / "run"
            run.mkdir()
            self.write_common_manifests(run, verified=True)
            (run / "relay_frame_manifest.json").write_text(
                json.dumps(
                    {
                        "storage_mode": "r2_streaming",
                        "streaming_shards": [
                            {
                                "part_index": 1,
                                "local_path": str(run / "relay_frames" / "part-000001.ndjson"),
                                "object_key": "r2/key",
                                "verified": True,
                                "local_deleted": True,
                                "sha256": "abc",
                            }
                        ],
                    }
                )
            )
            summary = recover.build_summary(run)
            self.assertEqual(summary["classification"], "R2_STREAMING_RECOVERY_CLEAN_COMPACT_MANIFESTS_ONLY")
            self.assertEqual(summary["unverified_local_chunk_count"], 0)

    def test_recovery_retains_unverified_local_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = pathlib.Path(tmp) / "run"
            shard = run / "relay_frames" / "part-000001.ndjson"
            shard.parent.mkdir(parents=True)
            shard.write_text("frame\n")
            self.write_common_manifests(run, verified=False)
            (run / "relay_frame_manifest.json").write_text(
                json.dumps(
                    {
                        "storage_mode": "r2_streaming",
                        "streaming_shards": [
                            {
                                "part_index": 1,
                                "local_path": str(shard),
                                "object_key": None,
                                "verified": False,
                                "local_deleted": False,
                                "sha256": "abc",
                            }
                        ],
                    }
                )
            )
            summary = recover.build_summary(run)
            self.assertEqual(
                summary["classification"],
                "R2_STREAMING_RECOVERY_UNVERIFIED_LOCAL_CHUNKS_RETAINED",
            )
            self.assertEqual(summary["unverified_local_chunk_count"], 1)
            self.assertTrue(shard.exists())
            self.assertFalse(summary["safe_to_delete_unverified_local_chunks"])

    def test_recovery_retry_blocks_without_r2_health(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = pathlib.Path(tmp) / "run"
            shard = run / "relay_frames" / "part-000001.ndjson"
            shard.parent.mkdir(parents=True)
            shard.write_text("frame\n")
            self.write_common_manifests(run, verified=False)
            (run / "relay_frame_manifest.json").write_text(
                json.dumps(
                    {
                        "storage_mode": "r2_streaming",
                        "streaming_shards": [
                            {
                                "part_index": 1,
                                "local_path": str(shard),
                                "object_key": None,
                                "verified": False,
                                "local_deleted": False,
                                "sha256": "abc",
                            }
                        ],
                    }
                )
            )
            summary = recover.build_summary(run, retry_requested=True, r2_health_verified=False)
            self.assertEqual(summary["classification"], "R2_STREAMING_RECOVERY_RETRY_BLOCKED_R2_HEALTH")
            self.assertTrue(shard.exists())

    def test_recovery_retry_blocks_without_approved_uploader(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = pathlib.Path(tmp) / "run"
            shard = run / "relay_frames" / "part-000001.ndjson"
            shard.parent.mkdir(parents=True)
            shard.write_text("frame\n")
            self.write_common_manifests(run, verified=False)
            (run / "relay_frame_manifest.json").write_text(
                json.dumps(
                    {
                        "storage_mode": "r2_streaming",
                        "streaming_shards": [
                            {
                                "part_index": 1,
                                "local_path": str(shard),
                                "object_key": None,
                                "verified": False,
                                "local_deleted": False,
                                "sha256": "abc",
                            }
                        ],
                    }
                )
            )
            summary = recover.build_summary(run, retry_requested=True, r2_health_verified=True)
            self.assertEqual(summary["classification"], "R2_STREAMING_RECOVERY_RETRY_BLOCKED_NO_UPLOADER")
            self.assertTrue(shard.exists())


if __name__ == "__main__":
    unittest.main()
