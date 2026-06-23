#!/usr/bin/env python3
"""Run sequential relay-only R2-primary material-hunter collection slices.

The script intentionally reads deployment and credential details from the
environment or a local env file. Do not commit the env file.
"""

from __future__ import annotations

import argparse
import csv
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
from urllib.parse import urlparse


REPO = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = REPO / "research_output" / "local_stream_collector"
DEFAULT_LOG_ROOT = pathlib.Path(tempfile.gettempdir()) / "pump_relay_r2_primary_batch"
DEFAULT_RELAY_CONTROL_ENV = REPO / ".codex_runtime_env" / "relay_control.env"
DEFAULT_COLLECTION_JUSTIFICATION_PATH = (
    REPO
    / "research_output"
    / "trading_strategy_pipeline"
    / "COLLECTION_JUSTIFICATION_DECISION.json"
)
FORBIDDEN_VPS_ARTIFACT_NAMES = (
    "attempt_ledger.csv",
    "candidate_summary.csv",
    "rejected_summary.csv",
    "run_countability_decision.json",
    "countability_decision.json",
    "r2_upload_result.json",
)
SUPERVISOR_COMMANDS = {"proof", "batch", "recover", "cleanup-aborted", "status"}
REQUIRED_FINAL_FILES = (
    "local_relay_dataset_proof_summary.json",
    "local_collector_summary.json",
    "service_exit_status.json",
    "countability_decision.json",
    "run_countability_decision.json",
    "r2_upload_result.json",
    "local_retention_summary.json",
)
ASOF_ALPHA_HORIZONS = (5, 10, 30, 60, 120, 300, 900)
ASOF_REQUIRED_PREFIX_HORIZONS = (5, 10, 30, 60, 120)
ASOF_TRADE_FIELDS = {
    "trade_update_count_asof",
    "transaction_active_mint_count_asof",
    "pump_trade_active_mint_count_asof",
    "buy_count_delta_asof",
    "sell_count_delta_asof",
    "net_buy_sell_delta_asof",
    "volume_delta_asof",
    "unique_trade_accounts_asof",
}
ASOF_HOLDER_FIELDS = {
    "holder_update_count_asof",
    "unique_holder_accounts_seen_asof",
    "top_holder_concentration_asof",
    "dev_or_creator_holding_proxy_asof",
    "holder_churn_proxy_asof",
    "holder_collapse_proxy_asof",
    "new_holder_count_delta_asof",
    "exiting_holder_count_delta_asof",
}
ASOF_VAULT_FIELDS = {
    "vault_update_count_asof",
    "bonding_curve_update_count_asof",
    "liquidity_delta_asof",
    "reserve_delta_asof",
    "curve_progress_proxy_asof",
    "liquidity_exit_proxy_asof",
    "price_or_curve_move_proxy_asof",
}
ASOF_FORBIDDEN_ALPHA_COLUMNS = {
    "final_outcome",
    "positive_outcome_label",
    "rejection_reason",
    "terminal_inconclusive_reason",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "off_vps_candidate_replay_allowed",
    "r2_verified",
    "artifact_consistency_ok",
}
ALLOWED_COLLECTION_JUSTIFICATION_REASONS = {
    "proof_after_source_patch",
    "targeted_early_burst_sample_collection",
    "candidate_review_trigger_recovery",
    "interrupted_run_recovery",
}


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
    r2_env = env.get("PUMP_RELAY_R2_ENV_FILE")
    if r2_env:
        r2_env_path = pathlib.Path(r2_env).expanduser()
        if r2_env_path.exists():
            r2_defaults = load_env_file(r2_env_path)
            r2_defaults.update(env)
            env = r2_defaults
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


def validate_collection_justification(args: argparse.Namespace) -> dict[str, Any]:
    """Fail closed unless a written collection justification allows this run."""
    path = args.collection_justification_path
    if not path.exists():
        raise BatchError(f"collection justification file missing: {path}")
    decision = read_json(path)
    if decision.get("collection_allowed") is not True:
        raise BatchError(
            "collection justification denied: "
            f"{decision.get('reason', 'generic_collection_blocked')}"
        )
    reason = str(decision.get("reason", ""))
    if reason not in ALLOWED_COLLECTION_JUSTIFICATION_REASONS:
        raise BatchError(f"collection justification reason not allowed: {reason}")
    if not args.justification_id:
        raise BatchError("--justification-id is required when collection justification is enforced")
    if str(decision.get("justification_id", "")) != args.justification_id:
        raise BatchError("collection justification id mismatch")
    target = str(decision.get("target_gate") or decision.get("target") or "")
    if not args.target_gate:
        raise BatchError("--target-gate is required when collection justification is enforced")
    if target != args.target_gate:
        raise BatchError(f"collection target gate mismatch: decision={target!r} requested={args.target_gate!r}")
    max_allowed = int(decision.get("maximum_allowed_slices") or 0)
    if args.max_slices is not None:
        max_allowed = min(max_allowed, int(args.max_slices)) if max_allowed else int(args.max_slices)
    if max_allowed < 1:
        raise BatchError("collection justification maximum_allowed_slices missing or invalid")
    requested = int(args.max_total_slices or args.slices)
    if requested > max_allowed:
        raise BatchError(f"requested slices exceed justified maximum: requested={requested} max={max_allowed}")
    required_fields = {
        "objective",
        "exact_blocker_being_targeted",
        "current_sample_counts",
        "required_sample_counts",
        "expected_number_of_slices",
        "maximum_allowed_slices",
        "stop_conditions",
        "proof_batch_mode",
    }
    missing = sorted(field for field in required_fields if field not in decision)
    if missing:
        raise BatchError(f"collection justification missing required fields: {','.join(missing)}")
    expected_slices = int(decision.get("expected_number_of_slices") or 0)
    if expected_slices > max_allowed:
        raise BatchError("collection justification expected slices exceed maximum allowed slices")
    if decision.get("launch_caps_remain_blocked") is not True:
        raise BatchError("collection justification must confirm launch caps remain blocked")
    if decision.get("launch_caps_changed") is True:
        raise BatchError("collection justification indicates launch caps changed")
    if decision.get("max_attempted_launches") is not None and int(decision["max_attempted_launches"]) != args.max_attempted_launches:
        raise BatchError("requested max_attempted_launches does not match justification")
    if decision.get("target_candidates") is not None and int(decision["target_candidates"]) != args.target_candidates:
        raise BatchError("requested target_candidates does not match justification")
    forbidden_enabled = [
        "replay_allowed",
        "formal_backtesting_allowed",
        "threshold_tuning_allowed",
        "paper_trading_enabled",
        "live_trading_enabled",
        "wallet_execution_enabled",
        "old_vps_material_hunter_allowed",
        "holder_rpc_enabled",
        "rpc_mint_supply_canonical",
    ]
    enabled = [field for field in forbidden_enabled if decision.get(field) is True]
    if enabled:
        raise BatchError(f"collection justification enables forbidden modes: {','.join(enabled)}")
    return decision


def asof_field_group_present(row: dict[str, str], fields: set[str]) -> bool:
    return any(str(row.get(field, "")).strip() not in {"", "MISSING"} for field in fields)


def validate_asof_alpha_features(out: pathlib.Path) -> dict[str, Any]:
    root = out / "asof_alpha_features"
    blockers: list[str] = []
    rows_by_horizon: dict[str, int] = {}
    manifest_path = root / "asof_alpha_feature_manifest.json"
    completeness_path = root / "asof_alpha_feature_completeness.json"
    manifest = read_json(manifest_path)
    completeness = read_json(completeness_path)
    if not manifest_path.exists():
        blockers.append("asof_alpha_manifest_missing")
    if not completeness_path.exists():
        blockers.append("asof_alpha_completeness_missing")
    total_rows = 0
    group_counts = {"trade_delta": 0, "holder_state": 0, "vault_curve": 0}
    for horizon in ASOF_ALPHA_HORIZONS:
        path = root / f"asof_alpha_features_{horizon:03d}s.csv"
        if not path.exists():
            if horizon in ASOF_REQUIRED_PREFIX_HORIZONS:
                blockers.append(f"asof_alpha_horizon_file_missing:{horizon}")
            rows_by_horizon[str(horizon)] = 0
            continue
        with path.open(newline="") as handle:
            reader = csv.DictReader(handle)
            header = set(reader.fieldnames or [])
            leaked = sorted(ASOF_FORBIDDEN_ALPHA_COLUMNS & header)
            if leaked:
                blockers.append(f"asof_alpha_forbidden_columns:{horizon}:{','.join(leaked)}")
            row_count = 0
            for row in reader:
                row_count += 1
                if row.get("horizon_seconds") and str(row.get("horizon_seconds")) != str(horizon):
                    blockers.append(f"asof_alpha_wrong_horizon:{horizon}:{row.get('mint','')}")
                try:
                    age_ms = int(float(row.get("age_ms_at_horizon") or 0))
                except ValueError:
                    age_ms = -1
                if age_ms > horizon * 1000:
                    blockers.append(f"asof_alpha_post_horizon_age:{horizon}:{row.get('mint','')}")
                if str(row.get("holder_rpc_used", "")).lower() == "true":
                    blockers.append(f"asof_alpha_holder_rpc_used:{horizon}:{row.get('mint','')}")
                if str(row.get("rpc_mint_supply_canonical", "")).lower() == "true":
                    blockers.append(f"asof_alpha_rpc_mint_supply_canonical:{horizon}:{row.get('mint','')}")
                if asof_field_group_present(row, ASOF_TRADE_FIELDS):
                    group_counts["trade_delta"] += 1
                if asof_field_group_present(row, ASOF_HOLDER_FIELDS):
                    group_counts["holder_state"] += 1
                if asof_field_group_present(row, ASOF_VAULT_FIELDS):
                    group_counts["vault_curve"] += 1
            rows_by_horizon[str(horizon)] = row_count
            total_rows += row_count
    if total_rows == 0:
        blockers.append("asof_alpha_no_rows")
    for group, count in group_counts.items():
        if count == 0:
            blockers.append(f"asof_alpha_group_missing:{group}")
    for group in ("trade_delta", "holder_state", "vault_curve"):
        payload = (completeness.get("groups") or {}).get(group) or {}
        if payload and payload.get("available") is not True:
            blockers.append(f"asof_alpha_completeness_group_unavailable:{group}")
        if payload and payload.get("holder_rpc_used") is True:
            blockers.append(f"asof_alpha_completeness_holder_rpc:{group}")
        if payload and payload.get("rpc_mint_supply_canonical") is True:
            blockers.append(f"asof_alpha_completeness_canonical_supply:{group}")
    return {
        "ok": not blockers,
        "blockers": sorted(set(blockers)),
        "rows_by_horizon": rows_by_horizon,
        "total_rows": total_rows,
        "group_counts": group_counts,
        "manifest_present": manifest_path.exists(),
        "completeness_present": completeness_path.exists(),
        "manifest_schema_version": manifest.get("schema_version"),
        "completeness_schema_version": completeness.get("schema_version"),
    }


def parse_tcp_url(url: str) -> tuple[str, int]:
    parsed = urlparse(url)
    if parsed.scheme != "tcp" or not parsed.hostname or not parsed.port:
        raise BatchError(f"expected tcp://host:port URL, got {url!r}")
    return parsed.hostname, int(parsed.port)


def classify_blockers(blockers: list[str], result: dict[str, Any] | None = None) -> str:
    result = result or {}
    blocker_set = set(blockers)
    if not blockers:
        if result.get("zero_attempt_no_signal") is True:
            if int(result.get("all_launches_seen") or 0) == 0:
                return "RELAY_LOCAL_DATASET_PASS_EMPTY_NO_ATTEMPTS"
            return "RELAY_LOCAL_DATASET_PASS_NO_SIGNAL"
        if int(result.get("upstream_provider_blocker_count") or 0) > 0:
            return "RELAY_PROVIDER_GAP_CONTINUATION_PASS"
        return "RELAY_LOCAL_DATASET_PASS"
    if "sequence_gap_count" in blocker_set:
        return "RELAY_LOCAL_DATASET_BLOCK_SEQUENCE_GAP"
    if "hash_mismatch_count" in blocker_set:
        return "RELAY_LOCAL_DATASET_BLOCK_HASH_MISMATCH"
    if "receiver_backpressure_count" in blocker_set:
        return "RELAY_LOCAL_DATASET_BLOCK_RECEIVER_BACKPRESSURE"
    if "receiver_unavailable_count" in blocker_set:
        return "RELAY_LOCAL_DATASET_BLOCK_RECEIVER_UNAVAILABLE"
    if "r2_failed" in blocker_set or "retention_not_ok" in blocker_set or "r2_timeout" in blocker_set:
        return "RELAY_LOCAL_DATASET_BLOCK_R2"
    if "r2_local_spool_full" in blocker_set:
        return "RELAY_LOCAL_DATASET_BLOCK_SPOOL"
    if "artifact_consistency" in blocker_set:
        return "RELAY_LOCAL_DATASET_BLOCK_ARTIFACT_CONSISTENCY"
    if "asof_alpha_feature_validation" in blocker_set:
        return "RELAY_LOCAL_DATASET_BLOCK_ASOF_ALPHA_FEATURES"
    if "vps_forbidden_artifacts" in blocker_set:
        return "RELAY_LOCAL_DATASET_BLOCK_VPS_FORBIDDEN_ARTIFACTS"
    if "not_counted" in blocker_set and result.get("provider_blocker_class"):
        return "RELAY_LOCAL_DATASET_BLOCK_PROVIDER"
    if "not_counted" in blocker_set:
        return "RELAY_LOCAL_DATASET_BLOCK_COUNTABILITY"
    if blocker_set & {"local_rc", "remote_rc", "local_finalization_timeout", "remote_relay_timeout"}:
        return "RELAY_LOCAL_DATASET_BLOCK_ORCHESTRATION"
    if "missing_final_artifacts" in blocker_set:
        return "RELAY_LOCAL_DATASET_BLOCK_STRUCTURAL"
    return "RELAY_LOCAL_DATASET_BLOCK_STRUCTURAL"


EXPECTED_ZERO_ATTEMPT_ASOF_BLOCKERS = {
    "asof_alpha_no_rows",
    "asof_alpha_group_missing:trade_delta",
    "asof_alpha_group_missing:holder_state",
    "asof_alpha_group_missing:vault_curve",
    "asof_alpha_completeness_group_unavailable:trade_delta",
    "asof_alpha_completeness_group_unavailable:holder_state",
    "asof_alpha_completeness_group_unavailable:vault_curve",
}


def is_clean_zero_attempt_no_signal(
    result: dict[str, Any],
    blockers: list[str],
    asof_alpha: dict[str, Any],
) -> bool:
    """True when a slice cleanly saw no rich-tracked material attempts.

    A zero-attempt slice can still have all-launch/cheap-follow-up rows. In that
    case as-of alpha feature rows are expected to be empty because as-of alpha
    shards are emitted for rich-tracked material attempts, not every visible
    launch. Only tolerate the expected empty-feature blockers; schema, leakage,
    post-horizon, RPC, R2, artifact, transport, and candidate/replay blockers
    must still fail closed.
    """
    if int(result.get("attempted_launches") or 0) != 0:
        return False
    if int(result.get("rich_tracked_launches") or 0) != 0:
        return False
    if int(result.get("candidate_checkpoint_count") or 0) != 0:
        return False
    if int(result.get("replay_eligible_candidate_count") or 0) != 0:
        return False
    if result.get("off_vps_candidate_replay_allowed") is True:
        return False
    if result.get("artifact_consistency_ok") is not True:
        return False
    if int(result.get("r2_failed") or 0) != 0:
        return False
    if int(result.get("upstream_provider_blocker_count") or 0) != 0:
        return False
    if result.get("provider_blocker_class"):
        return False
    if result.get("provider_data_loss_seen") is True:
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
    tolerated_blockers = {"asof_alpha_feature_validation", "not_counted"}
    if not set(blockers).issubset(tolerated_blockers):
        return False
    asof_blockers = set(asof_alpha.get("blockers") or [])
    if not asof_blockers:
        return True
    return asof_blockers.issubset(EXPECTED_ZERO_ATTEMPT_ASOF_BLOCKERS)


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
        relay_running=$(ps -eo args= | grep -E '[t]arget/release/cli .*vps-stream-relay' | wc -l | tr -d ' ')
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


def verify_remote_receiver(args: argparse.Namespace) -> dict[str, Any]:
    host, port = parse_tcp_url(args.receiver_url)
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise BatchError(f"receiver URL must use VPS loopback, got host {host!r}")
    remote = textwrap.dedent(
        f"""
        set -eu
        host={shlex.quote(host)}
        port={int(port)}
        if ! command -v ss >/dev/null 2>&1; then
          printf '{{"ok":false,"blocker":"ss_unavailable","host":"%s","port":%s}}\\n' "$host" "$port"
          exit 20
        fi
        listeners=$(ss -H -ltn "sport = :$port" 2>/dev/null || true)
        if [ -z "$listeners" ]; then
          printf '{{"ok":false,"blocker":"receiver_not_bound","host":"%s","port":%s}}\\n' "$host" "$port"
          exit 22
        fi
        local_addrs=$(printf '%s\\n' "$listeners" | awk '{{print $4}}')
        if printf '%s\\n' "$local_addrs" | grep -Eq '^(0\\.0\\.0\\.0|\\*|\\[::\\]|::):'; then
          printf '{{"ok":false,"blocker":"receiver_bound_publicly","host":"%s","port":%s}}\\n' "$host" "$port"
          exit 21
        fi
        python3 - "$host" "$port" "$listeners" <<'PY'
import json
import sys
host = sys.argv[1]
port = int(sys.argv[2])
listeners = sys.argv[3]
print(json.dumps({{"ok": True, "host": host, "port": port, "listener": listeners}}))
PY
        """
    ).strip()
    proc = ssh(args, remote, check=False)
    try:
        line = proc.stdout.strip().splitlines()[-1]
        payload = json.loads(line)
    except (IndexError, json.JSONDecodeError) as exc:
        raise BatchError(
            f"remote receiver probe failed rc={proc.returncode}: stdout={proc.stdout[:1000]} stderr={proc.stderr[:1000]}"
        ) from exc
    if proc.returncode != 0 or payload.get("ok") is not True:
        raise BatchError(f"remote receiver not ready: {json.dumps(payload, sort_keys=True)}")
    return payload


def reverse_tunnel_command(args: argparse.Namespace) -> list[str]:
    remote_host, remote_port = parse_tcp_url(args.receiver_url)
    local_host, local_port = parse_tcp_url(args.listen_url)
    if remote_host not in {"127.0.0.1", "localhost"}:
        raise BatchError(f"reverse tunnel remote bind must be IPv4 loopback, got {remote_host!r}")
    if local_host not in {"127.0.0.1", "localhost"}:
        raise BatchError(f"reverse tunnel local target must be IPv4 loopback, got {local_host!r}")
    return ssh_base(args) + [
        "-N",
        "-T",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
        "-R",
        f"{remote_host}:{remote_port}:{local_host}:{local_port}",
    ]


def start_or_reuse_reverse_tunnel(
    args: argparse.Namespace,
    log_dir: pathlib.Path,
) -> tuple[subprocess.Popen[str] | None, Any, Any, dict[str, Any]]:
    try:
        receiver = verify_remote_receiver(args)
        receiver["tunnel_reused"] = True
        return None, None, None, receiver
    except BatchError as exc:
        rendered = str(exc)
        if "receiver_bound_publicly" in rendered:
            raise
        if not args.manage_reverse_tunnel:
            raise

    tunnel_stdout = (log_dir / "reverse_tunnel.log").open("w")
    tunnel_stderr = (log_dir / "reverse_tunnel.err").open("w")
    proc = subprocess.Popen(
        reverse_tunnel_command(args),
        cwd=REPO,
        stdout=tunnel_stdout,
        stderr=tunnel_stderr,
        text=True,
    )
    deadline = time.time() + args.tunnel_timeout_seconds
    last_error = ""
    while time.time() < deadline:
        if proc.poll() is not None:
            tunnel_stdout.flush()
            tunnel_stderr.flush()
            tunnel_stdout.close()
            tunnel_stderr.close()
            raise BatchError(f"reverse tunnel exited before receiver became ready rc={proc.returncode}")
        try:
            receiver = verify_remote_receiver(args)
            receiver["tunnel_started"] = True
            return proc, tunnel_stdout, tunnel_stderr, receiver
        except BatchError as exc:
            last_error = str(exc)
        time.sleep(1)
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
    tunnel_stdout.close()
    tunnel_stderr.close()
    raise BatchError(f"reverse tunnel did not become ready within {args.tunnel_timeout_seconds}s: {last_error}")


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
        echo relay_proc_count=$(pgrep -af '[t]arget/release/cli .*vps-stream-relay' | wc -l | tr -d ' ')
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
    all_launch = read_json(out / "all_launch_intake_summary.json")
    followup = read_json(out / "all_launch_followup_manifest.json")
    promotion = read_json(out / "promotion_queue_summary.json")
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
    asof_alpha = validate_asof_alpha_features(out)
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
        "all_launches_seen": all_launch.get("all_launches_seen") or hunter.get("all_launches_seen") or 0,
        "all_launches_indexed": all_launch.get("all_launches_indexed")
        or hunter.get("all_launches_indexed")
        or 0,
        "rich_tracked_launches": all_launch.get("rich_tracked_launches")
        or hunter.get("rich_tracked_launches")
        or 0,
        "cheap_only_launches": all_launch.get("cheap_only_launches")
        or hunter.get("cheap_only_launches")
        or 0,
        "skipped_due_budget": all_launch.get("skipped_due_budget")
        or hunter.get("skipped_due_budget")
        or 0,
        "fast_dead_dropped": all_launch.get("fast_dead_dropped")
        or hunter.get("fast_dead_dropped")
        or 0,
        "missed_good_token_count": all_launch.get("missed_good_token_count")
        or hunter.get("missed_good_token_count")
        or 0,
        "tracking_slots_released": all_launch.get("tracking_slots_released")
        or hunter.get("tracking_slots_released")
        or 0,
        "cheap_followup_rows": followup.get("total_rows") or hunter.get("cheap_followup_rows") or 0,
        "promotion_recommended_count": promotion.get("promotion_recommended_count")
        or hunter.get("promotion_recommended_count")
        or 0,
        "promotion_admitted_count": promotion.get("promotion_admitted_count")
        or hunter.get("promotion_admitted_count")
        or 0,
        "promotion_blocked_budget_count": promotion.get("promotion_blocked_budget_count")
        or hunter.get("promotion_blocked_budget_count")
        or 0,
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
        "early_burst_review_candidate_count": summary.get("early_burst_review_candidate_count")
        or hunter.get("early_burst_review_candidate_count")
        or countability.get("early_burst_review_candidate_count")
        or 0,
        "early_burst_review_unique_mint_count": summary.get("early_burst_review_unique_mint_count")
        or 0,
        "early_burst_review_replay_eligible_candidate_count": summary.get(
            "early_burst_review_replay_eligible_candidate_count"
        )
        or hunter.get("early_burst_review_replay_eligible_candidate_count")
        or countability.get("early_burst_review_replay_eligible_candidate_count")
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
        "asof_alpha_feature_ok": asof_alpha.get("ok") is True,
        "asof_alpha_feature_blockers": asof_alpha.get("blockers") or [],
        "asof_alpha_feature_rows_by_horizon": asof_alpha.get("rows_by_horizon") or {},
        "asof_alpha_feature_total_rows": asof_alpha.get("total_rows") or 0,
        "asof_alpha_feature_group_counts": asof_alpha.get("group_counts") or {},
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
    if asof_alpha.get("ok") is not True:
        blockers.append("asof_alpha_feature_validation")
    if countability.get("counted_phase107b_result") is not True:
        blockers.append("not_counted")
    if result["candidate_checkpoint_count"] > 0 or (
        result["replay_eligible_candidate_count"] > 0
        and result["off_vps_candidate_replay_allowed"] is True
    ):
        blockers.append("candidate_review_required")
    if is_clean_zero_attempt_no_signal(result, blockers, asof_alpha):
        result["zero_attempt_no_signal"] = True
        result["no_signal_reason"] = (
            "clean_empty_no_attempts"
            if int(result.get("all_launches_seen") or 0) == 0
            else "clean_cheap_only_no_rich_admission"
        )
        result["asof_alpha_zero_attempt_expected"] = True
        result["asof_alpha_feature_expected_empty_reasons"] = sorted(
            set(asof_alpha.get("blockers") or [])
        )
        result["asof_alpha_feature_ok"] = True
        result["asof_alpha_feature_blockers"] = []
        blockers = []
        result["classification"] = classify_blockers(blockers, result)
    return result, blockers


def is_safe_provider_quarantine(result: dict[str, Any], blockers: list[str]) -> bool:
    """True when a provider-only dirty slice is complete but intentionally non-counted.

    This does not make the slice countable. It only allows the batch supervisor to
    keep collecting replacement slices when the local/R2/VPS path is healthy and
    the only reason the slice did not count is a quarantined provider boundary.
    """
    tolerated_blockers = {"not_counted", "remote_rc"}
    if not set(blockers).issubset(tolerated_blockers) or "not_counted" not in blockers:
        return False
    remote_rc = result.get("remote_rc")
    if "remote_rc" in blockers and remote_rc not in {None, 1}:
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


def can_recover_missing_remote_rc(
    result: dict[str, Any],
    blockers: list[str],
    safety: dict[str, Any],
    expected_latest_run_id: str,
) -> bool:
    """Recover when only the tiny remote RC artifact is missing.

    The relay writes health artifacts on the VPS only for operational
    observability. Local collector sequence/hash checks, material-hunter
    countability, R2 verification, artifact validation, and explicit VPS safety
    checks are the source of truth for the data slice. If the VPS root disk is
    tight, the detached wrapper can leave an empty relay_command_rc even though
    the relay stopped and the local/R2 dataset is complete. Treat that as
    recoverable only when every data and safety signal is clean.
    """
    if blockers != ["remote_rc"]:
        return False
    if result.get("remote_rc") is not None:
        return False
    if result.get("local_rc") != 0:
        return False
    if result.get("counted_phase107b_result") is not True:
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
    if int(safety.get("forbidden_recent") or 0) != 0:
        return False
    if int(safety.get("relay_running") or 0) != 0:
        return False
    if safety.get("material_candidate_service") == "active":
        return False
    if safety.get("material_hunter_service") == "active":
        return False
    if expected_latest_run_id and safety.get("latest_run_id") != expected_latest_run_id:
        return False
    return True


def classify_slice(result: dict[str, Any]) -> str:
    if result.get("zero_attempt_no_signal") is True:
        if int(result.get("all_launches_seen") or 0) == 0:
            return "RELAY_LOCAL_DATASET_PASS_EMPTY_NO_ATTEMPTS"
        return "RELAY_LOCAL_DATASET_PASS_NO_SIGNAL"
    if (
        result.get("candidate_checkpoint_count", 0) > 0
        or
        result.get("replay_eligible_candidate_count", 0) > 0
        and result.get("off_vps_candidate_replay_allowed") is True
    ):
        return "RELAY_COLLECTION_PASS_REVIEW_CANDIDATE"
    if result.get("upstream_provider_blocker_count", 0) > 0:
        return "RELAY_COLLECTION_PASS_PROVIDER_GAP_CONTINUED"
    return "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE"


def missing_required_final_files(out: pathlib.Path) -> list[str]:
    return [name for name in REQUIRED_FINAL_FILES if not (out / name).exists()]


def r2_result_verified(out: pathlib.Path) -> bool:
    r2 = read_json(out / "r2_upload_result.json")
    return r2.get("verified") is True and not r2.get("failed_files")


def recover_run(args: argparse.Namespace) -> tuple[dict[str, Any], list[str]]:
    if not args.run_id:
        raise BatchError("--run-id is required for recover")
    out = pathlib.Path(args.run_id)
    if not out.is_absolute():
        out = args.output_root / args.run_id
    result, blockers = validate_slice(out)
    result["run"] = out.name
    result["output_dir"] = str(out)
    missing = missing_required_final_files(out)
    if missing:
        result["missing_final_files"] = missing
        blockers.append("missing_final_artifacts")
    result["classification"] = classify_blockers(blockers, result)
    return result, blockers


def cleanup_aborted(args: argparse.Namespace) -> dict[str, Any]:
    root = args.output_root
    now = time.time()
    min_age_seconds = int(args.cleanup_min_age_minutes) * 60
    candidates: list[dict[str, Any]] = []
    deleted: list[str] = []
    for out in sorted(root.glob("relay-r2-primary-*")):
        if not out.is_dir() or out.name.endswith("-logs"):
            continue
        age_seconds = max(0, int(now - out.stat().st_mtime))
        if age_seconds < min_age_seconds:
            continue
        verified = r2_result_verified(out)
        has_final_summary = (out / "local_relay_dataset_proof_summary.json").exists()
        if verified or has_final_summary:
            continue
        logs = out.with_name(f"{out.name}-logs")
        entry = {
            "output_dir": str(out),
            "logs_dir": str(logs) if logs.exists() else None,
            "age_seconds": age_seconds,
            "reason": "missing_final_summary_and_unverified_r2",
        }
        candidates.append(entry)
        if not args.dry_run:
            import shutil

            shutil.rmtree(out, ignore_errors=True)
            if logs.exists():
                shutil.rmtree(logs, ignore_errors=True)
            deleted.append(str(out))
    return {
        "dry_run": args.dry_run,
        "candidate_count": len(candidates),
        "deleted_count": len(deleted),
        "candidates": candidates,
        "deleted": deleted,
    }


def status(args: argparse.Namespace, env: dict[str, str]) -> dict[str, Any]:
    result: dict[str, Any] = {"schema_version": "phase107g.relay_supervisor_status.v1"}
    if not args.skip_preflight:
        try:
            result["local_preflight"] = local_preflight(args, env)
        except Exception as exc:  # noqa: BLE001 - surfaced as structured status.
            result["local_preflight"] = {"ok": False, "error": str(exc)}
    try:
        result["vps_safety"] = verify_vps_safety(args)
    except Exception as exc:  # noqa: BLE001 - surfaced as structured status.
        result["vps_safety"] = {"ok": False, "error": str(exc)}
    try:
        result["remote_receiver"] = verify_remote_receiver(args)
    except Exception as exc:  # noqa: BLE001 - receiver is expected to be absent unless tunnel/listener is up.
        result["remote_receiver"] = {"ok": False, "error": str(exc)}
    return result


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
    remote_script_remote = f"{health_dir}/remote_relay.sh"
    out.mkdir(parents=True, exist_ok=False)
    log_dir.mkdir(parents=True, exist_ok=False)
    if args.survivor_extension_mode:
        survivor_policy = {
            "schema_version": "phase107h.survivor_extension_mode.v1",
            "enabled": True,
            "research_only": True,
            "raises_launch_caps": False,
            "runs_replay": False,
            "runs_backtesting": False,
            "runs_threshold_tuning": False,
            "runs_live_trading": False,
            "wallet_execution_enabled": False,
            "max_attempted_launches": args.max_attempted_launches,
            "target_candidates": args.target_candidates,
            "max_concurrent_tracked_mints": args.max_concurrent_tracked_mints,
            "reason": "candidate_discovery_research_only_same_caps",
        }
        (out / "survivor_extension_mode.json").write_text(
            json.dumps(survivor_policy, indent=2, sort_keys=True) + "\n"
        )
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
    if args.early_burst_in_out_v1_review_artifacts_enabled:
        local_cmd.append("--early-burst-in-out-v1-review-artifacts-enabled")
    if args.early_burst_in_out_v1_review_artifacts_mode != "disabled":
        local_cmd.extend(
            [
                "--early-burst-in-out-v1-review-artifacts-mode",
                args.early_burst_in_out_v1_review_artifacts_mode,
            ]
        )
    if args.promotion_policy != "current":
        local_cmd.extend(["--promotion-policy", args.promotion_policy])
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
    tunnel_proc: subprocess.Popen[str] | None = None
    tunnel_log = None
    tunnel_err = None
    print(f"slice={idx} run_id={run_id} out={out.relative_to(REPO)} local_pid={local_proc.pid}", flush=True)
    try:
        wait_for_listener(args.listen_port, local_proc.pid, args.listen_timeout_seconds)
        print(f"slice={idx} listen_ready=true", flush=True)
        tunnel_proc, tunnel_log, tunnel_err, remote_receiver = start_or_reuse_reverse_tunnel(args, log_dir)
        print(f"slice={idx} remote_receiver={json.dumps(remote_receiver, sort_keys=True)}", flush=True)
        ssh(args, f"mkdir -p {shlex.quote(health_dir)} && chmod 700 {shlex.quote(health_dir)}", check=True)
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
        remote_poll_timeout_seen = False
        deadline = time.time() + args.duration_seconds + args.remote_completion_grace_seconds
        while time.time() < deadline:
            try:
                proc = ssh(
                    args,
                    f"cat {shlex.quote(health_dir)}/relay_command_rc 2>/dev/null || true",
                    check=False,
                )
            except subprocess.TimeoutExpired:
                remote_poll_timeout_seen = True
                print(f"slice={idx} remote_rc_poll_timeout=true", flush=True)
                time.sleep(5)
                continue
            remote_rc = proc.stdout.strip()
            if remote_rc:
                break
            time.sleep(1)
        print(f"slice={idx} remote_rc={remote_rc or 'missing'}", flush=True)
        try:
            local_rc = local_proc.wait(timeout=args.local_finalization_timeout_seconds)
        except subprocess.TimeoutExpired:
            local_rc = -1
            print(f"slice={idx} local_finalization_timeout=true", flush=True)
        print(f"slice={idx} local_rc={local_rc}", flush=True)
    finally:
        if tunnel_proc is not None and tunnel_proc.poll() is None:
            tunnel_proc.terminate()
            try:
                tunnel_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                tunnel_proc.kill()
        if tunnel_log is not None:
            tunnel_log.close()
        if tunnel_err is not None:
            tunnel_err.close()
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
    result["remote_rc_poll_timeout_seen"] = remote_poll_timeout_seen
    result["local_rc"] = local_rc
    result["classification"] = classify_slice(result) if not blockers else result.get("classification")
    result["survivor_extension_mode_enabled"] = bool(args.survivor_extension_mode)
    result["survivor_extension_mode_same_caps"] = bool(args.survivor_extension_mode)
    if local_rc != 0:
        blockers.append("local_rc")
    if local_rc == -1:
        blockers.append("local_finalization_timeout")
    if remote_rc != "0":
        try:
            refreshed = ssh(
                args,
                f"cat {shlex.quote(health_dir)}/relay_command_rc 2>/dev/null || true",
                check=False,
            ).stdout.strip()
        except subprocess.TimeoutExpired:
            refreshed = ""
            remote_poll_timeout_seen = True
            result["remote_rc_poll_timeout_seen"] = True
        if refreshed:
            remote_rc = refreshed
            result["remote_rc"] = int(remote_rc) if remote_rc.isdigit() else None
        if remote_rc != "0":
            blockers.append("remote_rc")
    safety = verify_vps_safety(args)
    result["vps_safety"] = safety
    if safety.get("forbidden_recent") != 0:
        blockers.append("vps_forbidden_artifacts")
    if args.expected_latest_run_id and safety.get("latest_run_id") != args.expected_latest_run_id:
        blockers.append("latest_run_id_changed")
    if can_recover_missing_remote_rc(result, blockers, safety, args.expected_latest_run_id):
        result["remote_rc_recovered_from_local_validation"] = True
        result["remote_rc_recovery_reason"] = "missing_remote_rc_with_clean_local_r2_and_vps_safety"
        blockers = []
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
        "no_signal_slices": sum(1 for item in results if item.get("zero_attempt_no_signal") is True),
        "blocked_slices": 0,
        "total_frames": total("frames_received"),
        "total_all_launches_seen": total("all_launches_seen"),
        "total_all_launches_indexed": total("all_launches_indexed"),
        "total_rich_tracked_launches": total("rich_tracked_launches"),
        "total_cheap_only_launches": total("cheap_only_launches"),
        "total_skipped_due_budget": total("skipped_due_budget"),
        "total_fast_dead_dropped": total("fast_dead_dropped"),
        "total_missed_good_token_count": total("missed_good_token_count"),
        "total_tracking_slots_released": total("tracking_slots_released"),
        "total_cheap_followup_rows": total("cheap_followup_rows"),
        "total_promotion_recommended_count": total("promotion_recommended_count"),
        "total_promotion_admitted_count": total("promotion_admitted_count"),
        "total_promotion_blocked_budget_count": total("promotion_blocked_budget_count"),
        "total_attempted_launches": total("attempted_launches"),
        "total_unique_attempted_mints": total("unique_attempted_mints"),
        "total_rejected_dead": total("rejected_dead_count"),
        "total_terminal_inconclusive": total("terminal_inconclusive_count"),
        "candidate_checkpoints": total("candidate_checkpoint_count"),
        "replay_eligible_candidates": total("replay_eligible_candidate_count"),
        "early_burst_review_candidates": total("early_burst_review_candidate_count"),
        "early_burst_review_replay_eligible_candidates": total(
            "early_burst_review_replay_eligible_candidate_count"
        ),
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
    command = "batch"
    if argv and argv[0] in SUPERVISOR_COMMANDS:
        command = argv[0]
        argv = argv[1:]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slices", type=int, default=1 if command == "proof" else 20)
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
    parser.add_argument(
        "--run-prefix",
        default="relay-r2-primary-proof" if command == "proof" else "relay-r2-primary-batch",
    )
    parser.add_argument("--output-root", type=pathlib.Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--batch-log-dir", type=pathlib.Path, default=None)
    parser.add_argument("--env-file", type=pathlib.Path, default=None)
    parser.add_argument("--config", default="config/default.toml")
    parser.add_argument("--config-override", default=os.environ.get("CONFIG_OVERRIDE", "config/local.example.toml"))
    parser.add_argument("--vps-config", default="config/default.toml")
    parser.add_argument("--vps-config-override", default="config/local.toml")
    parser.add_argument("--vps-env-file", default="/home/ubuntu/pump-launch-quant.env")
    parser.add_argument("--vps-repo-dir", default="/home/ubuntu/pump-launch-quant")
    parser.add_argument(
        "--vps-health-root",
        default=os.environ.get("PUMP_RELAY_VPS_HEALTH_ROOT", "/run/user/1000"),
        help=(
            "Remote tmpfs root for relay health and wrapper scripts. Keep this "
            "off the VPS root-backed /tmp path so relay-only operation does not "
            "depend on material-hunter disk headroom."
        ),
    )
    parser.add_argument("--vps-ssh-target", default=os.environ.get("PUMP_RELAY_VPS_SSH_TARGET", ""))
    parser.add_argument("--ssh-key", type=pathlib.Path, default=os.environ.get("PUMP_RELAY_SSH_KEY"))
    parser.add_argument("--ssh-option", action="append", default=[])
    parser.add_argument("--ssh-timeout-seconds", type=int, default=60)
    parser.add_argument("--listen-url", default="tcp://127.0.0.1:19097")
    parser.add_argument("--receiver-url", default="tcp://127.0.0.1:19097")
    parser.add_argument("--listen-port", type=int, default=19097)
    parser.add_argument("--listen-timeout-seconds", type=int, default=180)
    parser.add_argument("--tunnel-timeout-seconds", type=int, default=60)
    parser.add_argument("--no-manage-reverse-tunnel", dest="manage_reverse_tunnel", action="store_false")
    parser.set_defaults(manage_reverse_tunnel=True)
    parser.add_argument("--remote-completion-grace-seconds", type=int, default=360)
    parser.add_argument("--local-finalization-timeout-seconds", type=int, default=900)
    parser.add_argument("--expected-latest-run-id", default=os.environ.get("EXPECTED_MATERIAL_LATEST_RUN_ID", ""))
    parser.add_argument("--skip-preflight", action="store_true")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cleanup-min-age-minutes", type=int, default=60)
    parser.add_argument(
        "--require-collection-justification",
        action="store_true",
        default=True,
        help="Require a fail-closed collection justification decision before proof/batch runs.",
    )
    parser.add_argument(
        "--collection-justification-path",
        type=pathlib.Path,
        default=DEFAULT_COLLECTION_JUSTIFICATION_PATH,
    )
    parser.add_argument("--justification-id", default="")
    parser.add_argument("--max-slices", type=int, default=None)
    parser.add_argument("--target-gate", default="")
    parser.add_argument(
        "--survivor-extension-mode",
        action="store_true",
        help=(
            "Mark the slice as a research-only survivor-extension proof "
            "without raising caps or enabling replay/backtesting/tuning/trading."
        ),
    )
    parser.add_argument("--early-burst-in-out-v1-review-artifacts-enabled", action="store_true")
    parser.add_argument(
        "--early-burst-in-out-v1-review-artifacts-mode",
        choices=("disabled", "shadow", "emit_review_only"),
        default="disabled",
    )
    parser.add_argument(
        "--promotion-policy",
        choices=("current", "v1_shadow", "v1_controlled"),
        default="current",
    )
    args = parser.parse_args(argv)
    args.command = command
    if (
        args.early_burst_in_out_v1_review_artifacts_enabled
        and args.early_burst_in_out_v1_review_artifacts_mode == "disabled"
    ):
        parser.error(
            "--early-burst-in-out-v1-review-artifacts-enabled requires "
            "--early-burst-in-out-v1-review-artifacts-mode shadow or emit_review_only"
        )
    if args.env_file is None and DEFAULT_RELAY_CONTROL_ENV.exists():
        args.env_file = DEFAULT_RELAY_CONTROL_ENV
    env_file_defaults: dict[str, str] = {}
    if args.env_file is not None:
        try:
            env_file_defaults = load_env_file(args.env_file)
        except BatchError as exc:
            parser.error(str(exc))
    if not args.vps_ssh_target:
        args.vps_ssh_target = env_file_defaults.get("PUMP_RELAY_VPS_SSH_TARGET", "")
    ssh_key_default = env_file_defaults.get("PUMP_RELAY_VPS_SSH_KEY") or env_file_defaults.get("PUMP_RELAY_SSH_KEY")
    if args.ssh_key is None and ssh_key_default:
        args.ssh_key = pathlib.Path(ssh_key_default)
    if not args.expected_latest_run_id:
        args.expected_latest_run_id = env_file_defaults.get("EXPECTED_MATERIAL_LATEST_RUN_ID", "")
    remote_health_root = env_file_defaults.get("PUMP_RELAY_REMOTE_HEALTH_ROOT") or env_file_defaults.get(
        "PUMP_RELAY_VPS_HEALTH_ROOT"
    )
    if args.vps_health_root == "/run/user/1000" and remote_health_root:
        args.vps_health_root = remote_health_root
    if args.vps_repo_dir == "/home/ubuntu/pump-launch-quant" and env_file_defaults.get("PUMP_RELAY_REMOTE_APP_DIR"):
        args.vps_repo_dir = env_file_defaults["PUMP_RELAY_REMOTE_APP_DIR"]
    if args.vps_config_override == "config/local.toml" and env_file_defaults.get("PUMP_RELAY_REMOTE_CONFIG"):
        args.vps_config_override = env_file_defaults["PUMP_RELAY_REMOTE_CONFIG"]
    if args.listen_url == "tcp://127.0.0.1:19097":
        args.listen_url = (
            env_file_defaults.get("PUMP_RELAY_REVERSE_TUNNEL_LOCAL")
            or env_file_defaults.get("PUMP_RELAY_LOCAL_LISTEN_URL")
            or args.listen_url
        )
    if args.receiver_url == "tcp://127.0.0.1:19097":
        args.receiver_url = env_file_defaults.get("PUMP_RELAY_REVERSE_TUNNEL_REMOTE") or args.receiver_url
    if args.config_override == "config/local.example.toml" and env_file_defaults.get("CONFIG_OVERRIDE"):
        args.config_override = env_file_defaults["CONFIG_OVERRIDE"]
    if command == "proof":
        args.slices = 1
        args.counted_slices_target = 1
        args.max_total_slices = 1
    if args.slices < 1 and command in {"proof", "batch"}:
        parser.error("--slices must be positive")
    if args.counted_slices_target is None:
        args.counted_slices_target = args.slices
    if args.counted_slices_target < 1 and command in {"proof", "batch"}:
        parser.error("--counted-slices-target must be positive")
    if args.max_total_slices is None:
        args.max_total_slices = args.slices
        if args.replace_safe_provider_quarantine:
            args.max_total_slices += max(3, args.slices // 2)
    if args.max_total_slices < args.counted_slices_target and command in {"proof", "batch"}:
        parser.error("--max-total-slices must be >= --counted-slices-target")
    if not args.vps_ssh_target and command in {"proof", "batch", "status"}:
        parser.error("--vps-ssh-target or PUMP_RELAY_VPS_SSH_TARGET is required")
    if args.local_receiver_window_seconds < args.duration_seconds:
        parser.error("--local-receiver-window-seconds must be >= --duration-seconds")
    args.output_root = args.output_root.resolve()
    if args.batch_log_dir is None:
        args.batch_log_dir = (DEFAULT_LOG_ROOT / utc_stamp()).resolve()
    else:
        args.batch_log_dir = args.batch_log_dir.resolve()
    args.collection_justification_path = args.collection_justification_path.resolve()
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    env = merged_env(args.env_file)
    if args.command == "recover":
        result, blockers = recover_run(args)
        payload = {"blockers": blockers, "result": result}
        print("RECOVER", json.dumps(payload, sort_keys=True), flush=True)
        return 0 if not blockers else 2
    if args.command == "cleanup-aborted":
        payload = cleanup_aborted(args)
        print("CLEANUP_ABORTED", json.dumps(payload, sort_keys=True), flush=True)
        return 0
    if args.command == "status":
        payload = status(args, env)
        print("STATUS", json.dumps(payload, sort_keys=True), flush=True)
        safety_ok = not payload.get("vps_safety", {}).get("error")
        preflight_ok = args.skip_preflight or payload.get("local_preflight", {}).get("ok") is True
        return 0 if safety_ok and preflight_ok else 2

    args.batch_log_dir.mkdir(parents=True, exist_ok=True)
    print(f"batch_log_dir={args.batch_log_dir}", flush=True)
    if args.require_collection_justification:
        justification = validate_collection_justification(args)
        (args.batch_log_dir / "collection_justification_decision.json").write_text(
            json.dumps(justification, indent=2, sort_keys=True)
        )
        print(
            "COLLECTION_JUSTIFICATION",
            json.dumps(
                {
                    "collection_allowed": justification.get("collection_allowed"),
                    "justification_id": justification.get("justification_id"),
                    "reason": justification.get("reason"),
                    "target_gate": justification.get("target_gate") or justification.get("target"),
                    "maximum_allowed_slices": justification.get("maximum_allowed_slices"),
                },
                sort_keys=True,
            ),
            flush=True,
        )
    if not args.skip_preflight:
        preflight = local_preflight(args, env)
        (args.batch_log_dir / "local_preflight.json").write_text(json.dumps(preflight, indent=2, sort_keys=True))
        print("LOCAL_PREFLIGHT", json.dumps(preflight, sort_keys=True), flush=True)
    safety = verify_vps_safety(args)
    print("VPS_SAFETY", json.dumps(safety, sort_keys=True), flush=True)

    passed: list[dict[str, Any]] = []
    counted_slices = 0
    accepted_slices = 0
    attempted_slices = 0
    while attempted_slices < args.max_total_slices and accepted_slices < args.counted_slices_target:
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
            result["classification"] = classify_blockers(blockers, result)
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
            accepted_slices += 1
        elif result.get("zero_attempt_no_signal") is True:
            accepted_slices += 1
        if result["classification"] == "RELAY_COLLECTION_PASS_REVIEW_CANDIDATE":
            stop = {"slice": idx, "reason": "candidate_review_required", "result": result}
            (args.batch_log_dir / "batch_stop.json").write_text(json.dumps(stop, indent=2, sort_keys=True))
            print("BATCH_STOP", json.dumps(stop, sort_keys=True), flush=True)
            return 3
        print(f"=== batch_slice={idx} pass={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} ===", flush=True)

    final = rollup(passed)
    final["accepted_slices"] = accepted_slices
    if args.command == "proof":
        if len(passed) == 1 and passed[0].get("zero_attempt_no_signal") is True:
            final["classification"] = passed[0]["classification"]
        else:
            final["classification"] = (
                "RELAY_PROVIDER_GAP_CONTINUATION_PASS"
                if final["provider_gap_continued_slices"] > 0
                else "RELAY_LOCAL_DATASET_PASS"
            )
    elif final["no_signal_slices"] > 0 and final["counted_slices"] == 0:
        final["classification"] = "RELAY_R2_PRIMARY_BATCH_PASS_NO_SIGNAL"
    else:
        final["classification"] = "RELAY_R2_PRIMARY_BATCH_PASS_WITH_GAP_CONTINUATION"
    if accepted_slices < args.counted_slices_target:
        final["blocked_slices"] = args.counted_slices_target - accepted_slices
        final["counted_slices_target"] = args.counted_slices_target
        stop = {
            "reason": "counted_slice_target_not_met",
            "counted_slices": counted_slices,
            "accepted_slices": accepted_slices,
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
    try:
        raise SystemExit(main(sys.argv[1:]))
    except BatchError as exc:
        print("BATCH_ERROR", json.dumps({"error": str(exc)}, sort_keys=True), file=sys.stderr)
        raise SystemExit(2)
