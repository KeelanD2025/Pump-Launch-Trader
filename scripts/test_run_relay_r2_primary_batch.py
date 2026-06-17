#!/usr/bin/env python3
"""Unit tests for the relay R2-primary supervisor."""

from __future__ import annotations

import importlib.util
import pathlib
import tempfile
import types
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("run_relay_r2_primary_batch.py")
SPEC = importlib.util.spec_from_file_location("relay_supervisor", SCRIPT)
assert SPEC and SPEC.loader
relay_supervisor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(relay_supervisor)


def dummy_args(**overrides: object) -> types.SimpleNamespace:
    base = {
        "vps_config": "config/default.toml",
        "vps_config_override": "config/local.toml",
        "vps_env_file": "/home/ubuntu/pump-launch-quant.env",
        "vps_repo_dir": "/home/ubuntu/pump-launch-quant",
        "receiver_url": "tcp://127.0.0.1:19097",
        "listen_url": "tcp://127.0.0.1:19097",
        "duration_seconds": 900,
        "expected_latest_run_id": "material-candidate-hunter-stable",
        "ssh_key": None,
        "ssh_option": [],
        "vps_ssh_target": "ubuntu@example.invalid",
        "dry_run": True,
        "cleanup_min_age_minutes": 0,
        "manage_reverse_tunnel": True,
        "tunnel_timeout_seconds": 60,
    }
    base.update(overrides)
    return types.SimpleNamespace(**base)


class RelaySupervisorTests(unittest.TestCase):
    def test_listener_readiness_is_required_before_relay_start(self) -> None:
        proc = types.SimpleNamespace(returncode=1)
        with mock.patch.object(relay_supervisor.subprocess, "run", return_value=proc):
            with mock.patch.object(relay_supervisor.os, "kill", return_value=None):
                with mock.patch.object(relay_supervisor.time, "sleep", return_value=None):
                    with self.assertRaises(relay_supervisor.BatchError):
                        relay_supervisor.wait_for_listener(19097, 12345, 1)

    def test_remote_script_rendering_preserves_health_path(self) -> None:
        health = "/run/user/1000/pump relay health"
        script = relay_supervisor.make_remote_script(dummy_args(), "run-1", health)
        self.assertIn(f"HEALTH={relay_supervisor.shlex.quote(health)}", script)
        self.assertIn('"$HEALTH/relay.log"', script)
        self.assertIn('"$HEALTH/relay.err"', script)

    def test_relay_process_status_is_read_from_command_rc(self) -> None:
        script = relay_supervisor.make_remote_script(dummy_args(), "run-1", "/run/user/1000/relay")
        self.assertIn('echo "$RC" >"$HEALTH/relay_command_rc"', script)
        self.assertIn('exit "$RC"', script)

    def test_no_secrets_are_rendered_into_remote_script(self) -> None:
        script = relay_supervisor.make_remote_script(
            dummy_args(vps_env_file="/home/ubuntu/pump-launch-quant.env"),
            "run-1",
            "/run/user/1000/relay",
        )
        self.assertIn("/home/ubuntu/pump-launch-quant.env", script)
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", script)
        self.assertNotIn("R2_SECRET", script)

    def test_parse_args_reads_vps_config_from_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_file = pathlib.Path(tmp) / "relay.env"
            env_file.write_text(
                "\n".join(
                    [
                        "PUMP_RELAY_VPS_SSH_TARGET=ubuntu@example.invalid",
                        "PUMP_RELAY_SSH_KEY=/tmp/relay-key",
                        "EXPECTED_MATERIAL_LATEST_RUN_ID=material-candidate-hunter-stable",
                        "CONFIG_OVERRIDE=config/local.relay.toml",
                    ]
                )
            )
            with mock.patch.dict(relay_supervisor.os.environ, {}, clear=True):
                args = relay_supervisor.parse_args(["proof", "--env-file", str(env_file)])
            self.assertEqual(args.vps_ssh_target, "ubuntu@example.invalid")
            self.assertEqual(str(args.ssh_key), "/tmp/relay-key")
            self.assertEqual(args.expected_latest_run_id, "material-candidate-hunter-stable")
            self.assertEqual(args.config_override, "config/local.relay.toml")

    def test_survivor_extension_mode_defaults_disabled(self) -> None:
        with mock.patch.dict(
            relay_supervisor.os.environ,
            {
                "PUMP_RELAY_VPS_SSH_TARGET": "ubuntu@example.invalid",
                "EXPECTED_MATERIAL_LATEST_RUN_ID": "material-candidate-hunter-stable",
            },
            clear=True,
        ):
            args = relay_supervisor.parse_args(["proof"])
        self.assertFalse(args.survivor_extension_mode)

    def test_survivor_extension_mode_preserves_same_caps(self) -> None:
        with mock.patch.dict(
            relay_supervisor.os.environ,
            {
                "PUMP_RELAY_VPS_SSH_TARGET": "ubuntu@example.invalid",
                "EXPECTED_MATERIAL_LATEST_RUN_ID": "material-candidate-hunter-stable",
            },
            clear=True,
        ):
            args = relay_supervisor.parse_args(
                [
                    "proof",
                    "--survivor-extension-mode",
                    "--max-attempted-launches",
                    "15",
                    "--target-candidates",
                    "2",
                ]
            )
        self.assertTrue(args.survivor_extension_mode)
        self.assertEqual(args.max_attempted_launches, 15)
        self.assertEqual(args.target_candidates, 2)

    def test_remote_receiver_verifier_parses_listener_status(self) -> None:
        stdout = '{"ok":true,"host":"127.0.0.1","port":19097,"listener":"LISTEN 0 128 127.0.0.1:19097"}\n'
        with mock.patch.object(
            relay_supervisor,
            "ssh",
            return_value=types.SimpleNamespace(stdout=stdout, stderr="", returncode=0),
        ):
            result = relay_supervisor.verify_remote_receiver(dummy_args())
        self.assertTrue(result["ok"])
        self.assertEqual(result["host"], "127.0.0.1")
        self.assertEqual(result["port"], 19097)

    def test_reverse_tunnel_command_uses_private_loopback_remote_bind(self) -> None:
        command = relay_supervisor.reverse_tunnel_command(dummy_args())
        rendered = " ".join(command)
        self.assertIn("ExitOnForwardFailure=yes", rendered)
        self.assertIn("127.0.0.1:19097:127.0.0.1:19097", command)
        self.assertNotIn("0.0.0.0:19097", rendered)

    def test_reverse_tunnel_rejects_public_remote_bind(self) -> None:
        with self.assertRaises(relay_supervisor.BatchError):
            relay_supervisor.reverse_tunnel_command(
                dummy_args(receiver_url="tcp://0.0.0.0:19097")
            )

    def test_reverse_tunnel_reuses_existing_remote_receiver(self) -> None:
        ready = {"ok": True, "host": "127.0.0.1", "port": 19097}
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(relay_supervisor, "verify_remote_receiver", return_value=ready):
                proc, stdout, stderr, result = relay_supervisor.start_or_reuse_reverse_tunnel(
                    dummy_args(),
                    pathlib.Path(tmp),
                )
        self.assertIsNone(proc)
        self.assertIsNone(stdout)
        self.assertIsNone(stderr)
        self.assertTrue(result["tunnel_reused"])

    def test_timeout_blockers_classify_as_orchestration_or_r2(self) -> None:
        self.assertEqual(
            relay_supervisor.classify_blockers(["local_finalization_timeout"], {}),
            "RELAY_LOCAL_DATASET_BLOCK_ORCHESTRATION",
        )
        self.assertEqual(
            relay_supervisor.classify_blockers(["remote_relay_timeout"], {}),
            "RELAY_LOCAL_DATASET_BLOCK_ORCHESTRATION",
        )
        self.assertEqual(
            relay_supervisor.classify_blockers(["r2_timeout"], {}),
            "RELAY_LOCAL_DATASET_BLOCK_R2",
        )

    def test_cleanup_aborted_deletes_only_unverified_incomplete_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            aborted = root / "relay-r2-primary-aborted"
            verified = root / "relay-r2-primary-verified"
            aborted.mkdir()
            verified.mkdir()
            (verified / "local_relay_dataset_proof_summary.json").write_text("{}")
            (verified / "r2_upload_result.json").write_text('{"verified":true,"failed_files":[]}')
            args = dummy_args(output_root=root, dry_run=False, cleanup_min_age_minutes=0)
            result = relay_supervisor.cleanup_aborted(args)
            self.assertEqual(result["deleted_count"], 1)
            self.assertFalse(aborted.exists())
            self.assertTrue(verified.exists())

    def test_cleanup_aborted_dry_run_keeps_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            aborted = root / "relay-r2-primary-aborted"
            aborted.mkdir()
            args = dummy_args(output_root=root, dry_run=True, cleanup_min_age_minutes=0)
            result = relay_supervisor.cleanup_aborted(args)
            self.assertEqual(result["candidate_count"], 1)
            self.assertTrue(aborted.exists())

    def test_forbidden_vps_artifacts_block(self) -> None:
        self.assertEqual(
            relay_supervisor.classify_blockers(["vps_forbidden_artifacts"], {}),
            "RELAY_LOCAL_DATASET_BLOCK_VPS_FORBIDDEN_ARTIFACTS",
        )

    def test_latest_run_id_mutation_blocks(self) -> None:
        self.assertEqual(
            relay_supervisor.classify_blockers(["latest_run_id_changed"], {}),
            "RELAY_LOCAL_DATASET_BLOCK_STRUCTURAL",
        )

    def test_recover_classifies_completed_proof(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            run = root / "relay-r2-primary-complete"
            run.mkdir()
            for name in relay_supervisor.REQUIRED_FINAL_FILES:
                (run / name).write_text("{}")
            args = dummy_args(output_root=root, run_id=run.name)
            with mock.patch.object(relay_supervisor, "validate_slice", return_value=({"counted_phase107b_result": True}, [])):
                result, blockers = relay_supervisor.recover_run(args)
            self.assertEqual(blockers, [])
            self.assertEqual(result["classification"], "RELAY_LOCAL_DATASET_PASS")

    def test_recover_classifies_r2_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            run = root / "relay-r2-primary-r2-fail"
            run.mkdir()
            for name in relay_supervisor.REQUIRED_FINAL_FILES:
                (run / name).write_text("{}")
            args = dummy_args(output_root=root, run_id=run.name)
            with mock.patch.object(relay_supervisor, "validate_slice", return_value=({}, ["r2_failed"])):
                result, blockers = relay_supervisor.recover_run(args)
            self.assertEqual(blockers, ["r2_failed"])
            self.assertEqual(result["classification"], "RELAY_LOCAL_DATASET_BLOCK_R2")

    def test_recover_classifies_missing_final_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            run = root / "relay-r2-primary-missing"
            run.mkdir()
            args = dummy_args(output_root=root, run_id=run.name)
            with mock.patch.object(relay_supervisor, "validate_slice", return_value=({}, [])):
                result, blockers = relay_supervisor.recover_run(args)
            self.assertIn("missing_final_artifacts", blockers)
            self.assertEqual(result["classification"], "RELAY_LOCAL_DATASET_BLOCK_STRUCTURAL")

    def test_missing_remote_rc_recovery_requires_clean_safety(self) -> None:
        clean_result = {
            "remote_rc": None,
            "remote_rc_poll_timeout_seen": True,
            "local_rc": 0,
            "counted_phase107b_result": True,
            "artifact_consistency_ok": True,
            "r2_failed": 0,
            "sequence_gap_count": 0,
            "hash_mismatch_count": 0,
            "malformed_frame_count": 0,
            "receiver_backpressure_count": 0,
            "receiver_unavailable_count": 0,
            "off_vps_candidate_replay_allowed": False,
        }
        clean_safety = {
            "forbidden_recent": 0,
            "relay_running": 0,
            "material_candidate_service": "inactive",
            "material_hunter_service": "inactive",
            "latest_run_id": "material-candidate-hunter-stable",
        }
        self.assertTrue(
            relay_supervisor.can_recover_missing_remote_rc(
                clean_result,
                ["remote_rc"],
                clean_safety,
                "material-candidate-hunter-stable",
            )
        )
        dirty_safety = dict(clean_safety, forbidden_recent=1)
        self.assertFalse(
            relay_supervisor.can_recover_missing_remote_rc(
                clean_result,
                ["remote_rc"],
                dirty_safety,
                "material-candidate-hunter-stable",
            )
        )


if __name__ == "__main__":
    unittest.main()
