#!/usr/bin/env python3
"""Unit tests for the relay R2-primary supervisor."""

from __future__ import annotations

import importlib.util
import csv
import json
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
        "storage_mode": "r2-primary",
        "r2_streaming_spool_mb": 2048,
        "r2_streaming_min_free_mb": 4096,
        "r2_streaming_chunk_mb": 32,
        "output_root": pathlib.Path("research_output/local_stream_collector"),
    }
    base.update(overrides)
    return types.SimpleNamespace(**base)


def write_asof_alpha_fixture(root: pathlib.Path, *, missing_trade: bool = False) -> None:
    asof_root = root / "asof_alpha_features"
    asof_root.mkdir(parents=True, exist_ok=True)
    (asof_root / "asof_alpha_feature_manifest.json").write_text('{"schema_version":"test"}')
    (asof_root / "asof_alpha_feature_completeness.json").write_text(
        json.dumps(
            {
                "schema_version": "test",
                "groups": {
                    "trade_delta": {"available": not missing_trade, "holder_rpc_used": False, "rpc_mint_supply_canonical": False},
                    "holder_state": {"available": True, "holder_rpc_used": False, "rpc_mint_supply_canonical": False},
                    "vault_curve": {"available": True, "holder_rpc_used": False, "rpc_mint_supply_canonical": False},
                },
            }
        )
    )
    fields = [
        "mint",
        "horizon_seconds",
        "age_ms_at_horizon",
        "trade_update_count_asof",
        "buy_count_delta_asof",
        "holder_update_count_asof",
        "unique_holder_accounts_seen_asof",
        "vault_update_count_asof",
        "bonding_curve_update_count_asof",
        "curve_progress_proxy_asof",
        "holder_rpc_used",
        "rpc_mint_supply_canonical",
    ]
    for horizon in relay_supervisor.ASOF_ALPHA_HORIZONS:
        row = {
            "mint": "mintA",
            "horizon_seconds": str(horizon),
            "age_ms_at_horizon": str(horizon * 1000),
            "trade_update_count_asof": "" if missing_trade else "1",
            "buy_count_delta_asof": "" if missing_trade else "1",
            "holder_update_count_asof": "1",
            "unique_holder_accounts_seen_asof": "1",
            "vault_update_count_asof": "1",
            "bonding_curve_update_count_asof": "1",
            "curve_progress_proxy_asof": "1",
            "holder_rpc_used": "false",
            "rpc_mint_supply_canonical": "false",
        }
        with (asof_root / f"asof_alpha_features_{horizon:03d}s.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerow(row)


def write_empty_asof_alpha_fixture(root: pathlib.Path, *, forbidden_column: bool = False) -> None:
    asof_root = root / "asof_alpha_features"
    asof_root.mkdir(parents=True, exist_ok=True)
    (asof_root / "asof_alpha_feature_manifest.json").write_text('{"schema_version":"test"}')
    (asof_root / "asof_alpha_feature_completeness.json").write_text(
        json.dumps(
            {
                "schema_version": "test",
                "groups": {
                    "trade_delta": {
                        "available": False,
                        "holder_rpc_used": False,
                        "rpc_mint_supply_canonical": False,
                    },
                    "holder_state": {
                        "available": False,
                        "holder_rpc_used": False,
                        "rpc_mint_supply_canonical": False,
                    },
                    "vault_curve": {
                        "available": False,
                        "holder_rpc_used": False,
                        "rpc_mint_supply_canonical": False,
                    },
                },
            }
        )
    )
    fields = [
        "mint",
        "horizon_seconds",
        "age_ms_at_horizon",
        "trade_update_count_asof",
        "holder_update_count_asof",
        "vault_update_count_asof",
        "holder_rpc_used",
        "rpc_mint_supply_canonical",
    ]
    if forbidden_column:
        fields.append("final_outcome")
    for horizon in relay_supervisor.ASOF_ALPHA_HORIZONS:
        with (asof_root / f"asof_alpha_features_{horizon:03d}s.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()


def write_zero_attempt_slice_fixture(
    root: pathlib.Path,
    *,
    all_launches_seen: int,
    asof_forbidden_column: bool = False,
) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "local_relay_dataset_proof_summary.json").write_text(
        json.dumps(
            {
                "classification": "RELAY_LOCAL_DATASET_BLOCK_ASOF_ALPHA_FEATURES",
                "relay_session_id": "relay-test",
                "duration_seconds": 900,
                "frames_received": 100,
                "sequence_gap_count": 0,
                "hash_mismatch_count": 0,
                "malformed_frame_count": 0,
                "receiver_backpressure_count": 0,
                "receiver_unavailable_count": 0,
                "upstream_provider_blocker_count": 0,
                "upstream_reconnect_count": 0,
                "attempted_launches": 0,
                "unique_attempted_mints": 0,
                "rejected_dead_count": 0,
            }
        )
    )
    (root / "local_collector_summary.json").write_text("{}")
    (root / "hunter_summary.json").write_text("{}")
    (root / "countability_decision.json").write_text(
        json.dumps(
            {
                "counted_phase107b_result": False,
                "candidate_checkpoint_count": 0,
                "replay_eligible_candidate_count": 0,
                "off_vps_candidate_replay_allowed": False,
                "ready_for_off_vps_candidate_replay": False,
                "provider_data_loss_seen": False,
            }
        )
    )
    (root / "run_countability_decision.json").write_text(
        json.dumps({"candidate_checkpoint_count": 0, "replay_eligible_candidate_count": 0})
    )
    (root / "r2_upload_result.json").write_text(
        json.dumps({"verified": True, "uploaded_files": ["manifest.json"], "failed_files": []})
    )
    (root / "local_retention_summary.json").write_text(json.dumps({"ok": True}))
    (root / "all_launch_intake_summary.json").write_text(
        json.dumps(
            {
                "all_launches_seen": all_launches_seen,
                "all_launches_indexed": all_launches_seen,
                "rich_tracked_launches": 0,
                "cheap_only_launches": all_launches_seen,
            }
        )
    )
    (root / "all_launch_followup_manifest.json").write_text(
        json.dumps({"total_rows": all_launches_seen * 7})
    )
    (root / "promotion_queue_summary.json").write_text(
        json.dumps(
            {
                "promotion_recommended_count": all_launches_seen,
                "promotion_admitted_count": 0,
                "promotion_blocked_budget_count": 0,
            }
        )
    )
    write_empty_asof_alpha_fixture(root, forbidden_column=asof_forbidden_column)


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

    def test_parse_args_reads_relay_control_env_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_file = pathlib.Path(tmp) / "relay_control.env"
            env_file.write_text(
                "\n".join(
                    [
                        "PUMP_RELAY_VPS_SSH_TARGET=ubuntu@example.invalid",
                        "PUMP_RELAY_VPS_SSH_KEY=/tmp/relay-key",
                        "PUMP_RELAY_REMOTE_APP_DIR=/srv/pump-launch-quant",
                        "PUMP_RELAY_REMOTE_CONFIG=config/relay.local.toml",
                        "PUMP_RELAY_REMOTE_HEALTH_ROOT=/run/user/1000/relay",
                        "PUMP_RELAY_LOCAL_LISTEN_URL=tcp://127.0.0.1:19111",
                        "PUMP_RELAY_REVERSE_TUNNEL_REMOTE=tcp://127.0.0.1:19112",
                        "PUMP_RELAY_REVERSE_TUNNEL_LOCAL=tcp://127.0.0.1:19113",
                    ]
                )
            )
            with mock.patch.dict(relay_supervisor.os.environ, {}, clear=True):
                args = relay_supervisor.parse_args(["proof", "--env-file", str(env_file)])
            self.assertEqual(args.vps_ssh_target, "ubuntu@example.invalid")
            self.assertEqual(str(args.ssh_key), "/tmp/relay-key")
            self.assertEqual(args.vps_repo_dir, "/srv/pump-launch-quant")
            self.assertEqual(args.vps_config_override, "config/relay.local.toml")
            self.assertEqual(args.vps_health_root, "/run/user/1000/relay")
            self.assertEqual(args.listen_url, "tcp://127.0.0.1:19113")
            self.assertEqual(args.receiver_url, "tcp://127.0.0.1:19112")

    def test_merged_env_loads_referenced_r2_env_without_committing_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            r2_env = root / "r2.env"
            r2_env.write_text(
                "\n".join(
                    [
                        "R2_ACCESS_KEY_ID=test-access",
                        "R2_SECRET_ACCESS_KEY=test-secret",
                        "R2_DATASET_BUCKET=test-bucket",
                    ]
                )
            )
            relay_env = root / "relay_control.env"
            relay_env.write_text(
                "\n".join(
                    [
                        f"PUMP_RELAY_R2_ENV_FILE={r2_env}",
                        "PUMP_RELAY_VPS_SSH_TARGET=ubuntu@example.invalid",
                    ]
                )
            )
            with mock.patch.dict(relay_supervisor.os.environ, {}, clear=True):
                env = relay_supervisor.merged_env(relay_env)
            self.assertEqual(env["R2_ACCESS_KEY_ID"], "test-access")
            self.assertEqual(env["R2_DATASET_BUCKET"], "test-bucket")
            self.assertEqual(env["PUMP_RELAY_VPS_SSH_TARGET"], "ubuntu@example.invalid")

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

    def test_early_burst_review_flags_parse_fail_closed(self) -> None:
        with mock.patch.dict(
            relay_supervisor.os.environ,
            {
                "PUMP_RELAY_VPS_SSH_TARGET": "ubuntu@example.invalid",
                "EXPECTED_MATERIAL_LATEST_RUN_ID": "material-candidate-hunter-stable",
            },
            clear=True,
        ):
            with self.assertRaises(SystemExit):
                relay_supervisor.parse_args(
                    ["proof", "--early-burst-in-out-v1-review-artifacts-enabled"]
                )
            args = relay_supervisor.parse_args(
                [
                    "proof",
                    "--early-burst-in-out-v1-review-artifacts-enabled",
                    "--early-burst-in-out-v1-review-artifacts-mode",
                    "emit_review_only",
                    "--promotion-policy",
                    "v1_controlled",
                ]
            )
        self.assertTrue(args.early_burst_in_out_v1_review_artifacts_enabled)
        self.assertEqual(args.early_burst_in_out_v1_review_artifacts_mode, "emit_review_only")
        self.assertEqual(args.promotion_policy, "v1_controlled")

    def test_collection_justification_required_by_default_for_live_runs(self) -> None:
        with mock.patch.dict(
            relay_supervisor.os.environ,
            {
                "PUMP_RELAY_VPS_SSH_TARGET": "ubuntu@example.invalid",
                "EXPECTED_MATERIAL_LATEST_RUN_ID": "material-candidate-hunter-stable",
            },
            clear=True,
        ):
            args = relay_supervisor.parse_args(["batch"])
        self.assertTrue(args.require_collection_justification)

    def test_local_preflight_r2_streaming_uses_streaming_disk_gate(self) -> None:
        captured: dict[str, object] = {}

        def fake_run_capture(cmd, **kwargs):  # noqa: ANN001 - test shim.
            captured["cmd"] = cmd
            return types.SimpleNamespace(
                stdout=json.dumps({"ok": True, "storage_mode": "r2_streaming", "required_mb": 4096}),
                stderr="",
                returncode=0,
            )

        args = dummy_args(storage_mode="r2-streaming", output_root=pathlib.Path("/tmp/out"))
        with mock.patch.object(relay_supervisor, "run_capture", side_effect=fake_run_capture):
            payload = relay_supervisor.local_preflight(args, {"PUMP_R2_PRIMARY_SPOOL_ROOT": "/tmp/spool"})
        self.assertTrue(payload["ok"])
        cmd = captured["cmd"]
        self.assertIsInstance(cmd, list)
        self.assertIn("--storage-mode", cmd)
        self.assertIn("r2-streaming", cmd)
        self.assertIn("--output-dir", cmd)
        self.assertIn("/tmp/out", cmd)
        self.assertIn("--spool-dir", cmd)
        self.assertIn("/tmp/spool", cmd)
        self.assertIn("--r2-spool-max-mb", cmd)
        self.assertIn("--min-free-mb", cmd)

    def test_collection_justification_blocks_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            args = dummy_args(
                collection_justification_path=pathlib.Path(tmp) / "missing.json",
                justification_id="J-1",
                target_gate="EARLY_BURST_BACKTEST_READINESS",
                slices=1,
                max_total_slices=1,
                max_slices=1,
            )
            with self.assertRaises(relay_supervisor.BatchError):
                relay_supervisor.validate_collection_justification(args)

    def test_collection_justification_blocks_denied_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "decision.json"
            path.write_text('{"collection_allowed": false, "reason": "generic_collection_blocked"}')
            args = dummy_args(
                collection_justification_path=path,
                justification_id="J-1",
                target_gate="EARLY_BURST_BACKTEST_READINESS",
                slices=1,
                max_total_slices=1,
                max_slices=1,
            )
            with self.assertRaises(relay_supervisor.BatchError):
                relay_supervisor.validate_collection_justification(args)

    def test_collection_justification_accepts_targeted_reason_and_caps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "decision.json"
            path.write_text(
                json.dumps(
                    {
                        "collection_allowed": True,
                        "reason": "targeted_early_burst_sample_collection",
                        "justification_id": "EB-001",
                        "target_gate": "EARLY_BURST_BACKTEST_READINESS",
                        "objective": "Collect feature-complete early-burst samples.",
                        "exact_blocker_being_targeted": "sample_size_positive_too_small",
                        "current_sample_counts": {"positive_high_unique_mints": 96},
                        "required_sample_counts": {"positive_high_unique_mints": 100},
                        "expected_number_of_slices": 1,
                        "maximum_allowed_slices": 2,
                        "stop_conditions": ["stop_on_candidate_review"],
                        "proof_batch_mode": "batch",
                        "launch_caps_remain_blocked": True,
                        "launch_caps_changed": False,
                        "max_attempted_launches": 15,
                        "target_candidates": 2,
                        "replay_allowed": False,
                        "formal_backtesting_allowed": False,
                        "threshold_tuning_allowed": False,
                        "paper_trading_enabled": False,
                        "live_trading_enabled": False,
                        "wallet_execution_enabled": False,
                        "old_vps_material_hunter_allowed": False,
                        "holder_rpc_enabled": False,
                        "rpc_mint_supply_canonical": False,
                    }
                )
            )
            args = dummy_args(
                collection_justification_path=path,
                justification_id="EB-001",
                target_gate="EARLY_BURST_BACKTEST_READINESS",
                slices=1,
                max_total_slices=1,
                max_slices=2,
                max_attempted_launches=15,
                target_candidates=2,
            )
            decision = relay_supervisor.validate_collection_justification(args)
            self.assertTrue(decision["collection_allowed"])

    def test_collection_justification_blocks_excess_slices(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "decision.json"
            payload = {
                "collection_allowed": True,
                "reason": "proof_after_source_patch",
                "justification_id": "PATCH-001",
                "target_gate": "RELAY_LOCAL_DATASET",
                "objective": "One proof after source patch.",
                "exact_blocker_being_targeted": "source_patch_validation",
                "current_sample_counts": {},
                "required_sample_counts": {},
                "expected_number_of_slices": 1,
                "maximum_allowed_slices": 1,
                "stop_conditions": ["stop_on_any_blocker"],
                "proof_batch_mode": "proof",
                "launch_caps_remain_blocked": True,
                "launch_caps_changed": False,
                "replay_allowed": False,
                "formal_backtesting_allowed": False,
                "threshold_tuning_allowed": False,
                "paper_trading_enabled": False,
                "live_trading_enabled": False,
                "wallet_execution_enabled": False,
                "old_vps_material_hunter_allowed": False,
                "holder_rpc_enabled": False,
                "rpc_mint_supply_canonical": False,
            }
            path.write_text(json.dumps(payload))
            args = dummy_args(
                collection_justification_path=path,
                justification_id="PATCH-001",
                target_gate="RELAY_LOCAL_DATASET",
                slices=2,
                max_total_slices=2,
                max_slices=2,
            )
            with self.assertRaises(relay_supervisor.BatchError):
                relay_supervisor.validate_collection_justification(args)

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

    def test_asof_alpha_validation_accepts_complete_feature_groups(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "slice"
            write_asof_alpha_fixture(root)
            result = relay_supervisor.validate_asof_alpha_features(root)
            self.assertTrue(result["ok"])
            self.assertEqual(result["total_rows"], 7)
            self.assertGreater(result["group_counts"]["trade_delta"], 0)

    def test_asof_alpha_validation_blocks_missing_feature_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "slice"
            write_asof_alpha_fixture(root, missing_trade=True)
            result = relay_supervisor.validate_asof_alpha_features(root)
            self.assertFalse(result["ok"])
            self.assertIn("asof_alpha_group_missing:trade_delta", result["blockers"])
            self.assertEqual(
                relay_supervisor.classify_blockers(["asof_alpha_feature_validation"], {}),
                "RELAY_LOCAL_DATASET_BLOCK_ASOF_ALPHA_FEATURES",
            )

    def test_clean_zero_attempt_visible_launches_is_no_signal_not_asof_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "slice"
            write_zero_attempt_slice_fixture(root, all_launches_seen=226)
            validator = types.SimpleNamespace(
                returncode=0,
                stdout=json.dumps({"ok": True, "blockers": []}),
                stderr="",
            )
            with mock.patch.object(relay_supervisor, "run_capture", return_value=validator):
                result, blockers = relay_supervisor.validate_slice(root)
            self.assertEqual(blockers, [])
            self.assertTrue(result["zero_attempt_no_signal"])
            self.assertEqual(result["no_signal_reason"], "clean_cheap_only_no_rich_admission")
            self.assertEqual(result["classification"], "RELAY_LOCAL_DATASET_PASS_NO_SIGNAL")
            self.assertEqual(result["all_launches_seen"], 226)
            self.assertEqual(result["all_launches_indexed"], 226)
            self.assertEqual(result["attempted_launches"], 0)
            self.assertEqual(result["rich_tracked_launches"], 0)
            self.assertTrue(result["asof_alpha_feature_ok"])
            self.assertEqual(result["asof_alpha_feature_blockers"], [])

    def test_clean_zero_attempt_empty_launches_is_empty_no_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "slice"
            write_zero_attempt_slice_fixture(root, all_launches_seen=0)
            validator = types.SimpleNamespace(
                returncode=0,
                stdout=json.dumps({"ok": True, "blockers": []}),
                stderr="",
            )
            with mock.patch.object(relay_supervisor, "run_capture", return_value=validator):
                result, blockers = relay_supervisor.validate_slice(root)
            self.assertEqual(blockers, [])
            self.assertEqual(result["classification"], "RELAY_LOCAL_DATASET_PASS_EMPTY_NO_ATTEMPTS")
            self.assertEqual(result["no_signal_reason"], "clean_empty_no_attempts")

    def test_zero_attempt_does_not_hide_asof_schema_leakage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "slice"
            write_zero_attempt_slice_fixture(root, all_launches_seen=10, asof_forbidden_column=True)
            validator = types.SimpleNamespace(
                returncode=0,
                stdout=json.dumps({"ok": True, "blockers": []}),
                stderr="",
            )
            with mock.patch.object(relay_supervisor, "run_capture", return_value=validator):
                result, blockers = relay_supervisor.validate_slice(root)
            self.assertIn("asof_alpha_feature_validation", blockers)
            self.assertFalse(result.get("zero_attempt_no_signal", False))
            self.assertEqual(
                relay_supervisor.classify_blockers(blockers, result),
                "RELAY_LOCAL_DATASET_BLOCK_ASOF_ALPHA_FEATURES",
            )

    def test_zero_attempt_provider_block_is_not_asof_alpha_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "slice"
            write_zero_attempt_slice_fixture(root, all_launches_seen=19)
            (root / "countability_decision.json").write_text(
                json.dumps(
                    {
                        "counted_phase107b_result": False,
                        "candidate_checkpoint_count": 0,
                        "replay_eligible_candidate_count": 0,
                        "off_vps_candidate_replay_allowed": False,
                        "ready_for_off_vps_candidate_replay": False,
                        "provider_data_loss_seen": True,
                        "provider_blocker_class": "provider_reconnect_exhausted",
                    }
                )
            )
            (root / "hunter_summary.json").write_text(
                json.dumps(
                    {
                        "attempted_launches": 0,
                        "rich_tracked_launches": 0,
                        "all_launches_seen": 19,
                        "all_launches_indexed": 19,
                        "provider_data_loss_seen": True,
                        "provider_blocker_class": "provider_reconnect_exhausted",
                    }
                )
            )
            validator = types.SimpleNamespace(
                returncode=0,
                stdout=json.dumps({"ok": True, "blockers": []}),
                stderr="",
            )
            with mock.patch.object(relay_supervisor, "run_capture", return_value=validator):
                result, blockers = relay_supervisor.validate_slice(root)
            self.assertNotIn("asof_alpha_feature_validation", blockers)
            self.assertIn("not_counted", blockers)
            self.assertFalse(result.get("zero_attempt_no_signal", False))
            self.assertTrue(result["asof_alpha_zero_attempt_expected"])
            self.assertTrue(result["asof_alpha_feature_ok"])
            self.assertEqual(
                relay_supervisor.classify_blockers(blockers, result),
                "RELAY_LOCAL_DATASET_BLOCK_PROVIDER",
            )

    def test_zero_attempt_recoverable_provider_gap_is_no_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "slice"
            write_zero_attempt_slice_fixture(root, all_launches_seen=96)
            (root / "local_relay_dataset_proof_summary.json").write_text(
                json.dumps(
                    {
                        "classification": "RELAY_LOCAL_DATASET_BLOCK_COUNTABILITY",
                        "relay_session_id": "relay-test",
                        "duration_seconds": 900,
                        "frames_received": 100,
                        "sequence_gap_count": 0,
                        "hash_mismatch_count": 0,
                        "malformed_frame_count": 0,
                        "receiver_backpressure_count": 0,
                        "receiver_unavailable_count": 0,
                        "upstream_provider_blocker_count": 2,
                        "upstream_reconnect_count": 2,
                        "upstream_reconnect_exhausted_count": 0,
                        "provider_blocker_class": None,
                        "provider_data_loss_seen": False,
                        "attempted_launches": 0,
                        "unique_attempted_mints": 0,
                        "rejected_dead_count": 0,
                        "r2_streaming_unverified_chunks": 0,
                        "r2_streaming_upload_timeout_count": 0,
                        "r2_streaming_backpressure_detected": False,
                    }
                )
            )
            validator = types.SimpleNamespace(
                returncode=0,
                stdout=json.dumps({"ok": True, "blockers": []}),
                stderr="",
            )
            with mock.patch.object(relay_supervisor, "run_capture", return_value=validator):
                result, blockers = relay_supervisor.validate_slice(root)
            self.assertEqual(blockers, [])
            self.assertTrue(result["zero_attempt_no_signal"])
            self.assertEqual(result["no_signal_reason"], "clean_cheap_only_no_rich_admission")
            self.assertEqual(
                relay_supervisor.classify_blockers(blockers, result),
                "RELAY_LOCAL_DATASET_PASS_NO_SIGNAL",
            )

    def test_no_signal_slice_satisfies_one_slice_batch_without_counted_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            gate = root / "collection_gate.json"
            gate.write_text(
                json.dumps(
                    {
                        "collection_allowed": True,
                        "reason": "targeted_early_burst_sample_collection",
                        "justification_id": "test-no-signal",
                        "target_gate": "EARLY_BURST_BACKTEST_READINESS",
                        "objective": "test no-signal slice handling",
                        "exact_blocker_being_targeted": "none",
                        "current_sample_counts": {},
                        "required_sample_counts": {},
                        "expected_number_of_slices": 1,
                        "maximum_allowed_slices": 1,
                        "stop_conditions": ["test"],
                        "proof_batch_mode": "batch",
                        "launch_caps_remain_blocked": True,
                        "replay_allowed": False,
                        "formal_backtesting_allowed": False,
                        "threshold_tuning_allowed": False,
                        "paper_trading_enabled": False,
                        "live_trading_enabled": False,
                        "wallet_execution_enabled": False,
                        "old_vps_material_hunter_allowed": False,
                        "holder_rpc_enabled": False,
                        "rpc_mint_supply_canonical": False,
                    }
                )
            )
            no_signal_result = {
                "classification": "RELAY_LOCAL_DATASET_PASS_NO_SIGNAL",
                "zero_attempt_no_signal": True,
                "all_launches_seen": 5,
                "all_launches_indexed": 5,
                "attempted_launches": 0,
                "rich_tracked_launches": 0,
                "counted_phase107b_result": False,
                "candidate_checkpoint_count": 0,
                "replay_eligible_candidate_count": 0,
                "off_vps_candidate_replay_allowed": False,
                "artifact_consistency_ok": True,
                "r2_failed": 0,
            }
            with mock.patch.dict(
                relay_supervisor.os.environ,
                {"PUMP_RELAY_VPS_SSH_TARGET": "ubuntu@example.invalid"},
                clear=True,
            ):
                with mock.patch.object(
                    relay_supervisor,
                    "verify_vps_safety",
                    return_value={"ok": True},
                ):
                    with mock.patch.object(
                        relay_supervisor,
                        "run_slice",
                        return_value=(no_signal_result, []),
                    ):
                        rc = relay_supervisor.main(
                            [
                                "batch",
                                "--slices",
                                "1",
                                "--counted-slices-target",
                                "1",
                                "--max-total-slices",
                                "1",
                                "--skip-preflight",
                                "--batch-log-dir",
                                str(root / "logs"),
                                "--output-root",
                                str(root / "out"),
                                "--collection-justification-path",
                                str(gate),
                                "--justification-id",
                                "test-no-signal",
                                "--target-gate",
                                "EARLY_BURST_BACKTEST_READINESS",
                                "--max-slices",
                                "1",
                            ]
                        )
            self.assertEqual(rc, 0)
            rollup = json.loads((root / "logs" / "batch_rollup.json").read_text())
            self.assertEqual(rollup["accepted_slices"], 1)
            self.assertEqual(rollup["counted_slices"], 0)
            self.assertEqual(rollup["no_signal_slices"], 1)
            self.assertEqual(rollup["classification"], "RELAY_R2_PRIMARY_BATCH_PASS_NO_SIGNAL")

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

    def test_candidate_checkpoint_triggers_review_before_replay(self) -> None:
        self.assertEqual(
            relay_supervisor.classify_slice(
                {
                    "candidate_checkpoint_count": 1,
                    "replay_eligible_candidate_count": 0,
                    "off_vps_candidate_replay_allowed": False,
                    "upstream_provider_blocker_count": 0,
                }
            ),
            "RELAY_COLLECTION_PASS_REVIEW_CANDIDATE",
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

    def test_remote_broken_pipe_recovery_requires_clean_local_slice(self) -> None:
        clean_result = {
            "remote_rc": 1,
            "remote_rc_poll_timeout_seen": False,
            "local_rc": 0,
            "counted_phase107b_result": True,
            "artifact_consistency_ok": True,
            "r2_failed": 0,
            "provider_data_loss_seen": False,
            "provider_blocker_class": None,
            "upstream_provider_blocker_count": 0,
            "sequence_gap_count": 0,
            "hash_mismatch_count": 0,
            "malformed_frame_count": 0,
            "receiver_backpressure_count": 0,
            "receiver_unavailable_count": 0,
            "candidate_checkpoint_count": 0,
            "replay_eligible_candidate_count": 0,
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
            relay_supervisor.can_recover_remote_broken_pipe_after_clean_local_close(
                clean_result,
                ["remote_rc"],
                clean_safety,
                "material-candidate-hunter-stable",
                "relay_err_tail\nError: Broken pipe (os error 32)\n",
            )
        )
        clean_provider_gap_result = dict(clean_result, upstream_provider_blocker_count=1, upstream_reconnect_count=1)
        self.assertTrue(
            relay_supervisor.can_recover_remote_broken_pipe_after_clean_local_close(
                clean_provider_gap_result,
                ["remote_rc"],
                clean_safety,
                "material-candidate-hunter-stable",
                "relay_err_tail\nError: Broken pipe (os error 32)\n",
            )
        )
        dirty_result = dict(clean_result, sequence_gap_count=1)
        self.assertFalse(
            relay_supervisor.can_recover_remote_broken_pipe_after_clean_local_close(
                dirty_result,
                ["remote_rc"],
                clean_safety,
                "material-candidate-hunter-stable",
                "relay_err_tail\nError: Broken pipe (os error 32)\n",
            )
        )
        self.assertFalse(
            relay_supervisor.can_recover_remote_broken_pipe_after_clean_local_close(
                clean_result,
                ["remote_rc"],
                clean_safety,
                "material-candidate-hunter-stable",
                "relay_err_tail\nError: provider auth failed\n",
            )
        )


if __name__ == "__main__":
    unittest.main()
