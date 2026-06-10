#!/usr/bin/env python3
"""Run sequential relay-only R2-primary material-hunter collection slices.

The script intentionally reads deployment and credential details from the
environment or a local env file. Do not commit the env file.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import shlex
import subprocess
import sys
import tempfile
import textwrap
import time
from typing import Any


REPO = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = REPO / "research_output" / "local_stream_collector"
DEFAULT_LOG_ROOT = pathlib.Path(tempfile.gettempdir()) / "pump_relay_r2_primary_batch"
FORBIDDEN_VPS_ARTIFACT_NAMES = (
    "attempt_ledger.csv",
    "candidate_summary.csv",
    "rejected_summary.csv",
    "run_countability_decision.json",
    "countability_decision.json",
    "r2_upload_result.json",
)


class BatchError(RuntimeError):
    pass


def utc_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def load_env_file(path: pathlib.Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        raise BatchError(f"env file not found: {path}")
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key.startswith("export "):
            key = key.removeprefix("export ").strip()
        value = value.strip().strip("'\"")
        if key:
            env[key] = value
    return env


def merged_env(env_file: pathlib.Path | None) -> dict[str, str]:
    env: dict[str, str] = {}
    if env_file is not None:
        env.update(load_env_file(env_file))
    # Explicit shell exports should take precedence over local env-file defaults.
    env.update(os.environ.copy())
    return env


def run_capture(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: int | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        cmd,
        cwd=REPO,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    if check and proc.returncode != 0:
        raise BatchError(
            f"command failed rc={proc.returncode}: {shlex.join(cmd)}\n"
            f"stdout={proc.stdout[-4000:]}\nstderr={proc.stderr[-4000:]}"
        )
    return proc


def run_streaming(
    cmd: list[str],
    log_path: pathlib.Path,
    *,
    env: dict[str, str] | None = None,
) -> tuple[int, list[str]]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    with log_path.open("w") as log:
        proc = subprocess.Popen(
            cmd,
            cwd=REPO,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            print(line, end="", flush=True)
            log.write(line)
            log.flush()
            lines.append(line.rstrip("\n"))
        rc = proc.wait()
    return rc, lines


def ssh_base(args: argparse.Namespace) -> list[str]:
    cmd = ["ssh"]
    if args.ssh_key:
        cmd.extend(["-i", str(args.ssh_key)])
    cmd.extend(
        [
            "-o",
            "ConnectTimeout=20",
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "StrictHostKeyChecking=accept-new",
        ]
    )
    if args.ssh_option:
        for option in args.ssh_option:
            cmd.extend(["-o", option])
    cmd.append(args.vps_ssh_target)
    return cmd


def scp_base(args: argparse.Namespace) -> list[str]:
    cmd = ["scp"]
    if args.ssh_key:
        cmd.extend(["-i", str(args.ssh_key)])
    cmd.extend(["-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=accept-new"])
    if args.ssh_option:
        for option in args.ssh_option:
            cmd.extend(["-o", option])
    return cmd


def ssh(args: argparse.Namespace, remote: str, *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return run_capture(ssh_base(args) + [remote], timeout=args.ssh_timeout_seconds, check=check)


def read_json(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open() as handle:
        return json.load(handle)


def latest_run_id_remote_command(args: argparse.Namespace) -> str:
    candidates = [
        "data/reports/phase107b_material_candidate_hunter/latest_run_id",
        "research_output/phase107b_material_candidate_hunter/latest_run_id",
    ]
    reads = " ".join(shlex.quote(path) for path in candidates)
    return (
        "latest=''; "
        f"for p in {reads}; do if [ -f \"$p\" ]; then latest=$(cat \"$p\"); break; fi; done; "
        "printf '%s' \"$latest\""
    )


def verify_vps_safety(args: argparse.Namespace, *, recent_minutes: int = 30) -> dict[str, Any]:
    names = " ".join(shlex.quote(name) for name in FORBIDDEN_VPS_ARTIFACT_NAMES)
    expected_latest_check = ""
    if args.expected_latest_run_id:
        expected = shlex.quote(args.expected_latest_run_id)
        expected_latest_check = f'[ "$latest" = {expected} ] || exit 12;'
    remote = textwrap.dedent(
        f"""
        set -eu
        cd {shlex.quote(args.vps_repo_dir)}
        material_a=$(systemctl is-active pump-launch-quant-material-candidate-hunter.service 2>/dev/null || true)
        material_b=$(systemctl is-active pump-launch-quant-material-hunter.service 2>/dev/null || true)
        latest=$({latest_run_id_remote_command(args)})
        forbidden=$(find /home/ubuntu -mmin -{int(recent_minutes)} \\( {' -o '.join(f'-name {shlex.quote(name)}' for name in FORBIDDEN_VPS_ARTIFACT_NAMES)} \\) 2>/dev/null | wc -l | tr -d ' ')
        relay_running=$(ps -eo args= | grep -E '[v]ps-stream-relay' | wc -l | tr -d ' ')
        printf '{{"material_candidate_service":"%s","material_hunter_service":"%s","latest_run_id":"%s","forbidden_recent":%s,"relay_running":%s}}\\n' "$material_a" "$material_b" "$latest" "$forbidden" "$relay_running"
        [ "$material_a" != active ] || exit 10
        [ "$material_b" != active ] || exit 11
        {expected_latest_check}
        [ "$forbidden" -eq 0 ] || exit 13
        [ "$relay_running" -eq 0 ] || exit 14
        """
    ).strip()
    proc = ssh(args, remote, check=True)
    line = proc.stdout.strip().splitlines()[-1]
    return json.loads(line)


def local_preflight(args: argparse.Namespace, env: dict[str, str]) -> dict[str, Any]:
    cmd = [
        "./scripts/local_stream_collector_preflight.sh",
        "--storage-mode",
        "r2-primary",
        "--mode",
        "collection",
        "--verify-r2-health-live",
    ]
    proc = run_capture(cmd, env=env, check=True)
    return json.loads(proc.stdout)


def wait_for_listener(port: int, pid: int, timeout_seconds: int) -> None:
    for _ in range(timeout_seconds):
        ready = subprocess.run(
            ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"],
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if ready.returncode == 0:
            return
        try:
            os.kill(pid, 0)
        except ProcessLookupError as exc:
            raise BatchError("local collector exited before listener was ready") from exc
        time.sleep(1)
    raise BatchError(f"local collector did not bind port {port} within {timeout_seconds}s")


def make_remote_script(args: argparse.Namespace, run_id: str, health_dir: str) -> str:
    remote_config = " ".join(
        [
            "--config",
            shlex.quote(args.vps_config),
            "--config-override",
            shlex.quote(args.vps_config_override),
        ]
    )
    return textwrap.dedent(
        f"""\
        #!/usr/bin/env bash
        set -uo pipefail
        HEALTH={shlex.quote(health_dir)}
        mkdir -p "$HEALTH"
        cd {shlex.quote(args.vps_repo_dir)}
        set -a
        if [ -f {shlex.quote(args.vps_env_file)} ]; then . {shlex.quote(args.vps_env_file)}; fi
        set +a
        ./target/release/cli {remote_config} vps-stream-relay \\
          --receiver-url {shlex.quote(args.receiver_url)} \\
          --duration-seconds {int(args.duration_seconds)} \\
          --health-dir "$HEALTH" \\
          --json >"$HEALTH/relay.log" 2>"$HEALTH/relay.err"
        RC=$?
        echo "$RC" >"$HEALTH/relay_command_rc"
        exit "$RC"
        """
    )


def collect_remote_after(args: argparse.Namespace, health_dir: str, log_path: pathlib.Path) -> None:
    remote = textwrap.dedent(
        f"""
        set -uo pipefail
        echo relay_exit_status
        cat {shlex.quote(health_dir)}/relay_exit_status.json 2>/dev/null || true
        echo relay_health_summary
        cat {shlex.quote(health_dir)}/relay_health_summary.json 2>/dev/null || true
        echo relay_err_tail
        tail -80 {shlex.quote(health_dir)}/relay.err 2>/dev/null || true
        echo relay_log_tail
        tail -80 {shlex.quote(health_dir)}/relay.log 2>/dev/null || true
        cd {shlex.quote(args.vps_repo_dir)}
        echo material_candidate_active=$(systemctl is-active pump-launch-quant-material-candidate-hunter.service 2>/dev/null || true)
        echo material_hunter_active=$(systemctl is-active pump-launch-quant-material-hunter.service 2>/dev/null || true)
        echo relay_proc_count=$(pgrep -af '[v]ps-stream-relay' | wc -l | tr -d ' ')
        echo latest_run_id=$({latest_run_id_remote_command(args)})
        """
    ).strip()
    proc = ssh(args, remote, check=False)
    log_path.write_text(f"rc={proc.returncode}\nSTDOUT\n{proc.stdout}\nSTDERR\n{proc.stderr}\n")


def validate_slice(out: pathlib.Path) -> tuple[dict[str, Any], list[str]]:
    summary = read_json(out / "local_relay_dataset_proof_summary.json")
    collector = read_json(out / "local_collector_summary.json")
    hunter = read_json(out / "hunter_summary.json")
    countability = read_json(out / "countability_decision.json")
    run_countability = read_json(out / "run_countability_decision.json")
    r2 = read_json(out / "r2_upload_result.json")
    retention = read_json(out / "local_retention_summary.json")
    validator_proc = run_capture(
        [
            "target/release/cli",
            "validate-material-hunter-artifacts",
            "--output-dir",
            str(out),
            "--json",
        ]
    )
    validator_json: dict[str, Any] = {}
    if validator_proc.stdout.strip():
        try:
            validator_json = json.loads(validator_proc.stdout)
        except json.JSONDecodeError:
            validator_json = {"parse_error": validator_proc.stdout[:2000]}
    uploaded_count = len(r2.get("uploaded_files") or [])
    failed_count = len(r2.get("failed_files") or [])
    result = {
        "run": out.name,
        "relay_session_id": summary.get("relay_session_id") or collector.get("relay_session_id"),
        "classification": summary.get("classification"),
        "duration_seconds": summary.get("duration_seconds") or collector.get("duration_seconds"),
        "frames_received": summary.get("frames_received") or collector.get("frames_received"),
        "sequence_gap_count": summary.get("sequence_gap_count") or collector.get("sequence_gap_count") or 0,
        "hash_mismatch_count": summary.get("hash_mismatch_count") or collector.get("hash_mismatch_count") or 0,
        "malformed_frame_count": summary.get("malformed_frame_count") or collector.get("malformed_frame_count") or 0,
        "receiver_backpressure_count": summary.get("receiver_backpressure_count")
        or collector.get("downstream_backpressure_count")
        or 0,
        "receiver_unavailable_count": summary.get("receiver_unavailable_count")
        or collector.get("receiver_unavailable_count")
        or 0,
        "upstream_provider_blocker_count": summary.get("upstream_provider_blocker_count")
        or collector.get("upstream_provider_blocker_count")
        or 0,
        "upstream_reconnect_count": summary.get("upstream_reconnect_count")
        or collector.get("upstream_reconnect_count")
        or 0,
        "clean_segment_count": summary.get("clean_segment_count")
        or run_countability.get("clean_segment_count")
        or 0,
        "blocked_segment_count": summary.get("blocked_segment_count")
        or run_countability.get("blocked_segment_count")
        or 0,
        "gap_count": summary.get("gap_count") or run_countability.get("gap_count") or 0,
        "provider_blocker_class": summary.get("provider_blocker_class")
        or countability.get("provider_blocker_class")
        or hunter.get("provider_blocker_class"),
        "provider_data_loss_seen": summary.get("provider_data_loss_seen")
        if "provider_data_loss_seen" in summary
        else countability.get("provider_data_loss_seen"),
        "partial_outputs_audit_only": countability.get("partial_outputs_audit_only"),
        "attempted_launches": summary.get("attempted_launches") or hunter.get("attempted_launches") or 0,
        "unique_attempted_mints": summary.get("unique_attempted_mints")
        or countability.get("unique_attempted_mint_count")
        or 0,
        "rejected_dead_count": summary.get("rejected_dead_count") or hunter.get("rejected_dead_count") or 0,
        "terminal_inconclusive_count": summary.get("terminal_inconclusive")
        or hunter.get("rejected_inconclusive_count")
        or 0,
        "candidate_checkpoint_count": countability.get("candidate_checkpoint_count")
        or run_countability.get("candidate_checkpoint_count")
        or 0,
        "replay_eligible_candidate_count": countability.get("replay_eligible_candidate_count")
        or run_countability.get("replay_eligible_candidate_count")
        or 0,
        "counted_phase107b_result": countability.get("counted_phase107b_result"),
        "off_vps_candidate_replay_allowed": countability.get("off_vps_candidate_replay_allowed"),
        "ready_for_off_vps_candidate_replay": countability.get("ready_for_off_vps_candidate_replay"),
        "r2_uploaded": uploaded_count,
        "r2_failed": failed_count,
        "local_retained_bytes": retention.get("local_retained_bytes") or 0,
        "retention_deleted_bytes": retention.get("deleted_bulk_bytes") or 0,
        "artifact_consistency_ok": validator_proc.returncode == 0 and not validator_json.get("blockers"),
        "artifact_consistency_blockers": validator_json.get("blockers") or [],
    }
    blockers: list[str] = []
    for key in (
        "sequence_gap_count",
        "hash_mismatch_count",
        "malformed_frame_count",
        "receiver_backpressure_count",
        "receiver_unavailable_count",
    ):
        if result[key] != 0:
            blockers.append(key)
    if failed_count != 0:
        blockers.append("r2_failed")
    if not retention.get("ok", False):
        blockers.append("retention_not_ok")
    if validator_proc.returncode != 0 or validator_json.get("blockers"):
        blockers.append("artifact_consistency")
    if countability.get("counted_phase107b_result") is not True:
        blockers.append("not_counted")
    if (
        result["replay_eligible_candidate_count"] > 0
        and result["off_vps_candidate_replay_allowed"] is True
    ):
        blockers.append("candidate_review_required")
    return result, blockers


def is_safe_provider_quarantine(result: dict[str, Any], blockers: list[str]) -> bool:
    """True when a provider-only dirty slice is complete but intentionally non-counted.

    This does not make the slice countable. It only allows the batch supervisor to
    keep collecting replacement slices when the local/R2/VPS path is healthy and
    the only reason the slice did not count is a quarantined provider boundary.
    """
    if set(blockers) != {"not_counted"}:
        return False
    if result.get("provider_blocker_class") not in {
        "provider_lagged_data_loss",
        "provider_reconnect_exhausted",
    }:
        return False
    if result.get("provider_data_loss_seen") is not True:
        return False
    if result.get("partial_outputs_audit_only") is not True:
        return False
    if int(result.get("upstream_provider_blocker_count") or 0) < 1:
        return False
    if result.get("artifact_consistency_ok") is not True:
        return False
    if int(result.get("r2_failed") or 0) != 0:
        return False
    for key in (
        "sequence_gap_count",
        "hash_mismatch_count",
        "malformed_frame_count",
        "receiver_backpressure_count",
        "receiver_unavailable_count",
    ):
        if int(result.get(key) or 0) != 0:
            return False
    if result.get("off_vps_candidate_replay_allowed") is True:
        return False
    return True


def classify_slice(result: dict[str, Any]) -> str:
    if (
        result.get("replay_eligible_candidate_count", 0) > 0
        and result.get("off_vps_candidate_replay_allowed") is True
    ):
        return "RELAY_COLLECTION_PASS_REVIEW_CANDIDATE"
    if result.get("upstream_provider_blocker_count", 0) > 0:
        return "RELAY_COLLECTION_PASS_PROVIDER_GAP_CONTINUED"
    return "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE"


def run_slice(
    args: argparse.Namespace,
    env: dict[str, str],
    batch_log_dir: pathlib.Path,
    idx: int,
) -> tuple[dict[str, Any], list[str]]:
    run_id = f"{args.run_prefix}-{utc_stamp()}"
    out = args.output_root / run_id
    log_dir = args.output_root / f"{run_id}-logs"
    health_dir = f"{args.vps_health_root.rstrip('/')}/pump-launch-quant-stream-relay-{run_id}"
    remote_script_local = log_dir / "remote_relay.sh"
    remote_script_remote = f"/tmp/{run_id}-remote-relay.sh"
    out.mkdir(parents=True, exist_ok=False)
    log_dir.mkdir(parents=True, exist_ok=False)
    remote_script_local.write_text(make_remote_script(args, run_id, health_dir))
    remote_script_local.chmod(0o700)

    local_cmd = [
        "target/release/cli",
        "--config",
        args.config,
        "--config-override",
        args.config_override,
        "local-stream-collector",
        "--listen-url",
        args.listen_url,
        "--duration-seconds",
        str(args.local_receiver_window_seconds),
        "--material-duration-seconds",
        str(args.duration_seconds),
        "--output-dir",
        str(out),
        "--run-material-hunter",
        "--run-id",
        run_id,
        "--max-attempted-launches",
        str(args.max_attempted_launches),
        "--target-material-candidates",
        str(args.target_candidates),
        "--max-concurrent-tracked-mints",
        str(args.max_concurrent_tracked_mints),
        "--no-live-trading",
        "--no-rpc",
        "--upload-r2",
        "--verify-r2",
        "--json",
    ]
    local_log = (log_dir / "local.log").open("w")
    local_err = (log_dir / "local.err").open("w")
    local_proc = subprocess.Popen(
        local_cmd,
        cwd=REPO,
        env=env,
        stdout=local_log,
        stderr=local_err,
        text=True,
    )
    print(f"slice={idx} run_id={run_id} out={out.relative_to(REPO)} local_pid={local_proc.pid}", flush=True)
    try:
        wait_for_listener(args.listen_port, local_proc.pid, args.listen_timeout_seconds)
        print(f"slice={idx} listen_ready=true", flush=True)
        scp_cmd = scp_base(args) + [
            str(remote_script_local),
            f"{args.vps_ssh_target}:{remote_script_remote}",
        ]
        run_capture(scp_cmd, check=True)
        start_remote = (
            f"set -euo pipefail; mkdir -p {shlex.quote(health_dir)}; "
            f"chmod +x {shlex.quote(remote_script_remote)}; "
            f"nohup {shlex.quote(remote_script_remote)} >{shlex.quote(health_dir)}/nohup.out 2>&1 & "
            f"echo $! >{shlex.quote(health_dir)}/relay.pid; "
            f"printf 'remote_relay_pid=%s\\n' \"$(cat {shlex.quote(health_dir)}/relay.pid)\""
        )
        remote_start = ssh(args, start_remote, check=True)
        print(remote_start.stdout, end="", flush=True)
        remote_rc = ""
        deadline = time.time() + args.duration_seconds + args.remote_completion_grace_seconds
        while time.time() < deadline:
            proc = ssh(args, f"cat {shlex.quote(health_dir)}/relay_command_rc 2>/dev/null || true", check=False)
            remote_rc = proc.stdout.strip()
            if remote_rc:
                break
            time.sleep(1)
        print(f"slice={idx} remote_rc={remote_rc or 'missing'}", flush=True)
        local_rc = local_proc.wait()
        print(f"slice={idx} local_rc={local_rc}", flush=True)
    finally:
        if local_proc.poll() is None:
            local_proc.terminate()
            try:
                local_proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                local_proc.kill()
        local_log.close()
        local_err.close()

    collect_remote_after(args, health_dir, log_dir / "vps_after.txt")
    result, blockers = validate_slice(out)
    result["slice_index"] = idx
    result["run"] = run_id
    result["remote_rc"] = int(remote_rc) if remote_rc.isdigit() else None
    result["local_rc"] = local_rc
    result["classification"] = classify_slice(result) if not blockers else result.get("classification")
    if local_rc != 0:
        blockers.append("local_rc")
    if remote_rc != "0":
        blockers.append("remote_rc")
    safety = verify_vps_safety(args)
    result["vps_safety"] = safety
    if safety.get("forbidden_recent") != 0:
        blockers.append("vps_forbidden_artifacts")
    if args.expected_latest_run_id and safety.get("latest_run_id") != args.expected_latest_run_id:
        blockers.append("latest_run_id_changed")
    return result, blockers


def rollup(results: list[dict[str, Any]]) -> dict[str, Any]:
    def total(key: str) -> int:
        return sum(int(item.get(key) or 0) for item in results)

    return {
        "total_slices": len(results),
        "counted_slices": sum(1 for item in results if item.get("counted_phase107b_result") is True),
        "provider_gap_continued_slices": sum(
            1
            for item in results
            if int(item.get("upstream_provider_blocker_count") or 0) > 0
            and item.get("counted_phase107b_result") is True
        ),
        "safe_provider_quarantined_slices": sum(
            1 for item in results if item.get("safe_provider_quarantine_no_count") is True
        ),
        "blocked_slices": 0,
        "total_frames": total("frames_received"),
        "total_attempted_launches": total("attempted_launches"),
        "total_unique_attempted_mints": total("unique_attempted_mints"),
        "total_rejected_dead": total("rejected_dead_count"),
        "total_terminal_inconclusive": total("terminal_inconclusive_count"),
        "candidate_checkpoints": total("candidate_checkpoint_count"),
        "replay_eligible_candidates": total("replay_eligible_candidate_count"),
        "provider_blockers": total("upstream_provider_blocker_count"),
        "reconnects": total("upstream_reconnect_count"),
        "sequence_gaps": total("sequence_gap_count"),
        "hash_mismatches": total("hash_mismatch_count"),
        "receiver_backpressure": total("receiver_backpressure_count"),
        "receiver_unavailable": total("receiver_unavailable_count"),
        "r2_uploaded_verified_objects": total("r2_uploaded"),
        "r2_failures": total("r2_failed"),
        "artifact_consistency_failures": sum(1 for item in results if not item.get("artifact_consistency_ok")),
        "local_retained_bytes": total("local_retained_bytes"),
        "retention_deleted_bytes": total("retention_deleted_bytes"),
        "runs": results,
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slices", type=int, default=20)
    parser.add_argument("--start-index", type=int, default=1)
    parser.add_argument("--counted-slices-target", type=int, default=None)
    parser.add_argument("--max-total-slices", type=int, default=None)
    parser.add_argument(
        "--replace-safe-provider-quarantine",
        action="store_true",
        help=(
            "Allow fully validated provider-only audit slices to be recorded and "
            "replaced until --counted-slices-target is reached."
        ),
    )
    parser.add_argument("--duration-seconds", type=int, default=900)
    parser.add_argument("--local-receiver-window-seconds", type=int, default=1020)
    parser.add_argument("--max-attempted-launches", type=int, default=15)
    parser.add_argument("--target-candidates", type=int, default=2)
    parser.add_argument("--max-concurrent-tracked-mints", type=int, default=3)
    parser.add_argument("--run-prefix", default="relay-r2-primary-batch")
    parser.add_argument("--output-root", type=pathlib.Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--batch-log-dir", type=pathlib.Path, default=None)
    parser.add_argument("--env-file", type=pathlib.Path, default=None)
    parser.add_argument("--config", default="config/default.toml")
    parser.add_argument("--config-override", default=os.environ.get("CONFIG_OVERRIDE", "config/local.example.toml"))
    parser.add_argument("--vps-config", default="config/default.toml")
    parser.add_argument("--vps-config-override", default="config/local.toml")
    parser.add_argument("--vps-env-file", default="/home/ubuntu/pump-launch-quant.env")
    parser.add_argument("--vps-repo-dir", default="/home/ubuntu/pump-launch-quant")
    parser.add_argument("--vps-health-root", default="/tmp")
    parser.add_argument("--vps-ssh-target", default=os.environ.get("PUMP_RELAY_VPS_SSH_TARGET", ""))
    parser.add_argument("--ssh-key", type=pathlib.Path, default=os.environ.get("PUMP_RELAY_SSH_KEY"))
    parser.add_argument("--ssh-option", action="append", default=[])
    parser.add_argument("--ssh-timeout-seconds", type=int, default=60)
    parser.add_argument("--listen-url", default="tcp://127.0.0.1:19097")
    parser.add_argument("--receiver-url", default="tcp://127.0.0.1:19097")
    parser.add_argument("--listen-port", type=int, default=19097)
    parser.add_argument("--listen-timeout-seconds", type=int, default=180)
    parser.add_argument("--remote-completion-grace-seconds", type=int, default=360)
    parser.add_argument("--expected-latest-run-id", default=os.environ.get("EXPECTED_MATERIAL_LATEST_RUN_ID", ""))
    parser.add_argument("--skip-preflight", action="store_true")
    args = parser.parse_args(argv)
    if args.slices < 1:
        parser.error("--slices must be positive")
    if args.counted_slices_target is None:
        args.counted_slices_target = args.slices
    if args.counted_slices_target < 1:
        parser.error("--counted-slices-target must be positive")
    if args.max_total_slices is None:
        args.max_total_slices = args.slices
        if args.replace_safe_provider_quarantine:
            args.max_total_slices += max(3, args.slices // 2)
    if args.max_total_slices < args.counted_slices_target:
        parser.error("--max-total-slices must be >= --counted-slices-target")
    if not args.vps_ssh_target:
        parser.error("--vps-ssh-target or PUMP_RELAY_VPS_SSH_TARGET is required")
    if args.local_receiver_window_seconds < args.duration_seconds:
        parser.error("--local-receiver-window-seconds must be >= --duration-seconds")
    args.output_root = args.output_root.resolve()
    if args.batch_log_dir is None:
        args.batch_log_dir = (DEFAULT_LOG_ROOT / utc_stamp()).resolve()
    else:
        args.batch_log_dir = args.batch_log_dir.resolve()
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    env = merged_env(args.env_file)
    args.batch_log_dir.mkdir(parents=True, exist_ok=True)
    print(f"batch_log_dir={args.batch_log_dir}", flush=True)
    if not args.skip_preflight:
        preflight = local_preflight(args, env)
        (args.batch_log_dir / "local_preflight.json").write_text(json.dumps(preflight, indent=2, sort_keys=True))
        print("LOCAL_PREFLIGHT", json.dumps(preflight, sort_keys=True), flush=True)
    safety = verify_vps_safety(args)
    print("VPS_SAFETY", json.dumps(safety, sort_keys=True), flush=True)

    passed: list[dict[str, Any]] = []
    counted_slices = 0
    attempted_slices = 0
    while attempted_slices < args.max_total_slices and counted_slices < args.counted_slices_target:
        idx = args.start_index + attempted_slices
        attempted_slices += 1
        print(f"=== batch_slice={idx} start={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} ===", flush=True)
        result, blockers = run_slice(args, env, args.batch_log_dir, idx)
        safe_provider_quarantine = is_safe_provider_quarantine(result, blockers)
        if safe_provider_quarantine:
            result["safe_provider_quarantine_no_count"] = True
            result["classification"] = "RELAY_COLLECTION_PASS_PROVIDER_GAP_QUARANTINED_NO_COUNT"
            blockers = []
            print("SLICE_PROVIDER_QUARANTINED", json.dumps(result, sort_keys=True), flush=True)
        elif blockers:
            stop = {"slice": idx, "blockers": blockers, "result": result}
            (args.batch_log_dir / "batch_stop.json").write_text(json.dumps(stop, indent=2, sort_keys=True))
            print("BATCH_STOP", json.dumps(stop, sort_keys=True), flush=True)
            return 2
        with (args.batch_log_dir / "batch_summary.ndjson").open("a") as handle:
            handle.write(json.dumps(result, sort_keys=True) + "\n")
        print("SLICE_SUMMARY", json.dumps(result, sort_keys=True), flush=True)
        passed.append(result)
        if result.get("counted_phase107b_result") is True:
            counted_slices += 1
        if result["classification"] == "RELAY_COLLECTION_PASS_REVIEW_CANDIDATE":
            stop = {"slice": idx, "reason": "candidate_review_required", "result": result}
            (args.batch_log_dir / "batch_stop.json").write_text(json.dumps(stop, indent=2, sort_keys=True))
            print("BATCH_STOP", json.dumps(stop, sort_keys=True), flush=True)
            return 3
        print(f"=== batch_slice={idx} pass={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} ===", flush=True)

    final = rollup(passed)
    if counted_slices < args.counted_slices_target:
        final["blocked_slices"] = args.counted_slices_target - counted_slices
        final["counted_slices_target"] = args.counted_slices_target
        stop = {
            "reason": "counted_slice_target_not_met",
            "counted_slices": counted_slices,
            "counted_slices_target": args.counted_slices_target,
            "max_total_slices": args.max_total_slices,
            "rollup": final,
        }
        (args.batch_log_dir / "batch_stop.json").write_text(json.dumps(stop, indent=2, sort_keys=True))
        print("BATCH_STOP", json.dumps(stop, sort_keys=True), flush=True)
        return 4
    (args.batch_log_dir / "batch_rollup.json").write_text(json.dumps(final, indent=2, sort_keys=True))
    print("BATCH_ROLLUP", json.dumps(final, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
