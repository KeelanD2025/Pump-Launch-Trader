#!/usr/bin/env python3
"""Hands-off targeted early-burst collector using bounded relay-only slices.

This is intentionally an orchestration wrapper around
scripts/run_relay_r2_primary_batch.py. It does not run replay, formal
backtesting, threshold tuning, paper/live trading, wallet execution, the old
VPS material-hunter service, or unsliced collection.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import pathlib
import signal
import subprocess
import sys
import time
from typing import Any


REPO = pathlib.Path(__file__).resolve().parents[1]
STATUS_ROOT = REPO / "research_output" / "trading_strategy_pipeline" / "background_24h_collector"
STATUS_PATH = STATUS_ROOT / "status.json"
LIVE_SUMMARY_PATH = STATUS_ROOT / "live_summary.md"
EVENTS_PATH = STATUS_ROOT / "events.ndjson"
SLICE_SUMMARIES_PATH = STATUS_ROOT / "slice_summaries.csv"
REVIEW_QUEUE_PATH = STATUS_ROOT / "review_queue.csv"
MASTER_JUSTIFICATION_JSON = (
    REPO / "research_output" / "trading_strategy_pipeline" / "BACKGROUND_24H_COLLECTION_JUSTIFICATION.json"
)
MASTER_JUSTIFICATION_MD = (
    REPO / "research_output" / "trading_strategy_pipeline" / "BACKGROUND_24H_COLLECTION_JUSTIFICATION.md"
)
DEFAULT_CONTROL_ENV = REPO / ".codex_runtime_env" / "relay_control.env"
TARGET_GATE = "EARLY_BURST_BACKTEST_READINESS"
BASELINE_HIGH_POSITIVE_MINTS = 4
TARGET_HIGH_POSITIVE_MINTS = 20
MAX_TOTAL_RUNTIME_HOURS = 24
MAX_TOTAL_SLICES = 96
MAX_SLICES_PER_BATCH = 10
SLICE_DURATION_SECONDS = 900
LOCAL_RECEIVER_WINDOW_SECONDS = 1020
MAX_ATTEMPTED_LAUNCHES = 15
MAX_CONCURRENT_TRACKED_MINTS = 3
TARGET_CANDIDATES = 2

SLICE_FIELDS = [
    "batch_index",
    "slice_index",
    "slice_id",
    "relay_session_id",
    "classification",
    "frames_received",
    "all_launches_seen",
    "all_launches_indexed",
    "cheap_followup_rows",
    "rich_tracked_launches",
    "promotion_recommended_count",
    "promotion_admitted_count",
    "promotion_blocked_count",
    "missed_good_token_audit_v2_rows",
    "cheap_only_later_positive_count",
    "cheap_only_later_high_positive_count",
    "attempted_launches",
    "unique_attempted_mints",
    "rejected_dead_count",
    "terminal_inconclusive_count",
    "candidate_checkpoint_count",
    "replay_eligible_candidate_count",
    "early_burst_review_candidate_count",
    "early_burst_review_unique_mint_count",
    "early_burst_review_replay_eligible_candidate_count",
    "positive_outcome_rows",
    "high_positive_outcome_rows",
    "high_positive_unique_total",
    "sequence_gap_count",
    "hash_mismatch_count",
    "malformed_frame_count",
    "receiver_backpressure_count",
    "receiver_unavailable_count",
    "r2_verified",
    "r2_failed_files",
    "artifact_consistency_ok",
    "retention_deleted_bytes",
    "local_retained_bytes",
    "vps_safety_ok",
    "blocker_if_any",
]


class CollectorError(RuntimeError):
    pass


def utc_stamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def compact_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def read_json(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: pathlib.Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_event(event: dict[str, Any]) -> None:
    STATUS_ROOT.mkdir(parents=True, exist_ok=True)
    with EVENTS_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"ts": utc_stamp(), **event}, sort_keys=True) + "\n")


def read_csv_rows(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def write_csv_rows(path: pathlib.Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def append_csv_row(path: pathlib.Path, row: dict[str, Any], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in fields})


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value)))
    except (TypeError, ValueError):
        return default


def boolish(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def load_env_file(path: pathlib.Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().removeprefix("export ").strip()
        env[key] = value.strip().strip("'\"")
    return env


def merged_env(control_env: pathlib.Path) -> dict[str, str]:
    env = os.environ.copy()
    control = load_env_file(control_env)
    r2_env = control.get("PUMP_RELAY_R2_ENV_FILE")
    if r2_env:
        env.update(load_env_file(pathlib.Path(r2_env).expanduser()))
    env.update(control)
    return env


def pid_running(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def run_capture(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: int | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, cwd=REPO, env=env, text=True, capture_output=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise CollectorError(
            f"command failed rc={proc.returncode}: {' '.join(cmd)}\n"
            f"stdout={proc.stdout[-4000:]}\nstderr={proc.stderr[-4000:]}"
        )
    return proc


def run_reports() -> dict[str, Any]:
    commands = [
        ["python3", "scripts/build_positive_outcome_labels.py"],
        ["python3", "scripts/build_early_burst_validation_dataset.py"],
        ["python3", "scripts/build_early_burst_backtest_readiness.py"],
    ]
    results: list[dict[str, Any]] = []
    for cmd in commands:
        proc = run_capture(cmd, timeout=900)
        results.append(
            {
                "cmd": cmd,
                "returncode": proc.returncode,
                "stdout_tail": proc.stdout[-1200:],
                "stderr_tail": proc.stderr[-1200:],
            }
        )
        if proc.returncode != 0:
            break
    payload = {"updated_at_utc": utc_stamp(), "ok": all(item["returncode"] == 0 for item in results), "results": results}
    write_json(STATUS_ROOT / "report_refresh.json", payload)
    return payload


def current_high_positive_count() -> int:
    readiness = read_json(REPO / "research_output" / "trading_strategy_pipeline" / "READINESS_DECISION.json")
    if readiness.get("unique_high_positive_mints") is not None:
        return safe_int(readiness.get("unique_high_positive_mints"), BASELINE_HIGH_POSITIVE_MINTS)
    decision = read_json(
        REPO
        / "research_output"
        / "trading_strategy_pipeline"
        / "early_burst_backtest_readiness"
        / "early_burst_backtest_readiness_decision.json"
    )
    sample = decision.get("sample_checks", {})
    return safe_int(sample.get("global_high_positive_unique_mints"), BASELINE_HIGH_POSITIVE_MINTS)


def ensure_review_queue() -> None:
    if not REVIEW_QUEUE_PATH.exists():
        REVIEW_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
        REVIEW_QUEUE_PATH.write_text("created_at_utc,run_id,mint,reason,status\n", encoding="utf-8")


def write_live_summary(status: dict[str, Any]) -> None:
    lines = [
        "# Background 24h Targeted Collector",
        "",
        f"- updated_at_utc: `{utc_stamp()}`",
        f"- state: `{status.get('state', '')}`",
        f"- classification: `{status.get('classification', '')}`",
        f"- pid: `{status.get('pid', '')}`",
        f"- process_alive: `{str(status.get('process_alive', False)).lower()}`",
        f"- slices_attempted: `{status.get('slices_attempted', 0)}`",
        f"- counted_slices: `{status.get('counted_slices', 0)}`",
        f"- current_batch_index: `{status.get('current_batch_index', '')}`",
        f"- target_gate: `{TARGET_GATE}`",
        f"- current_high_positive_unique_mints: `{status.get('current_high_positive_unique_mints', current_high_positive_count())}`",
        f"- target_high_positive_unique_mints: `{TARGET_HIGH_POSITIVE_MINTS}`",
        f"- candidate_checkpoint_count: `{status.get('candidate_checkpoint_count', 0)}`",
        f"- replay_eligible_candidate_count: `{status.get('replay_eligible_candidate_count', 0)}`",
        f"- blocker: `{status.get('blocker', '')}`",
        f"- replay/backtesting/tuning/paper/live/wallet: `blocked`",
        f"- launch_caps: `blocked`",
        "",
        "## Paths",
        f"- status_json: `{STATUS_PATH}`",
        f"- events_ndjson: `{EVENTS_PATH}`",
        f"- slice_summaries_csv: `{SLICE_SUMMARIES_PATH}`",
        f"- review_queue_csv: `{REVIEW_QUEUE_PATH}`",
    ]
    LIVE_SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    LIVE_SUMMARY_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_master_justification(current_high_positive: int) -> dict[str, Any]:
    payload = {
        "schema_version": "phase107k.background_24h_collection_justification.v1",
        "written_at_utc": utc_stamp(),
        "collection_allowed": True,
        "reason": "targeted_early_burst_feature_complete_collection",
        "target_gate": TARGET_GATE,
        "target_high_positive_unique_mints": TARGET_HIGH_POSITIVE_MINTS,
        "current_high_positive_unique_mints": current_high_positive,
        "additional_high_positive_needed": max(0, TARGET_HIGH_POSITIVE_MINTS - current_high_positive),
        "max_total_runtime_hours": MAX_TOTAL_RUNTIME_HOURS,
        "max_slices_total": MAX_TOTAL_SLICES,
        "max_slices_per_batch": MAX_SLICES_PER_BATCH,
        "slice_duration_seconds": SLICE_DURATION_SECONDS,
        "local_receiver_window_seconds": LOCAL_RECEIVER_WINDOW_SECONDS,
        "max_attempted_launches": MAX_ATTEMPTED_LAUNCHES,
        "max_concurrent_tracked_mints": MAX_CONCURRENT_TRACKED_MINTS,
        "target_candidates": TARGET_CANDIDATES,
        "storage_mode": "r2-primary",
        "retention_mode": "keep-manifests-after-verified-r2",
        "generic_collection_allowed": False,
        "launch_caps_remain_blocked": True,
        "replay_backtesting_tuning_paper_live_wallet_execution": "blocked",
    }
    write_json(MASTER_JUSTIFICATION_JSON, payload)
    MASTER_JUSTIFICATION_MD.write_text(
        "# Background 24h Collection Justification\n\n"
        f"- collection_allowed: `true`\n"
        f"- reason: `{payload['reason']}`\n"
        f"- target_gate: `{TARGET_GATE}`\n"
        f"- current_high_positive_unique_mints: `{current_high_positive}`\n"
        f"- target_high_positive_unique_mints: `{TARGET_HIGH_POSITIVE_MINTS}`\n"
        f"- max_total_runtime_hours: `{MAX_TOTAL_RUNTIME_HOURS}`\n"
        f"- max_slices_total: `{MAX_TOTAL_SLICES}`\n"
        f"- max_slices_per_batch: `{MAX_SLICES_PER_BATCH}`\n"
        f"- slice_duration_seconds: `{SLICE_DURATION_SECONDS}`\n"
        f"- generic_collection_allowed: `false`\n"
        f"- replay/backtesting/tuning/paper/live/wallet: `blocked`\n"
        f"- launch_caps: `blocked`\n",
        encoding="utf-8",
    )
    return payload


def write_batch_gate(batch_index: int, expected_slices: int) -> pathlib.Path:
    gate = {
        "schema_version": "phase107j.collection_justification_decision.v1",
        "written_at_utc": utc_stamp(),
        "collection_allowed": True,
        "reason": "targeted_early_burst_sample_collection",
        "justification_id": f"background-24h-early-burst-batch-{batch_index:03d}",
        "target_gate": TARGET_GATE,
        "objective": "Targeted feature-complete early-burst sample collection using bounded relay-only R2-primary slices.",
        "exact_blocker_being_targeted": "sample_size_high_positive_too_small",
        "current_sample_counts": {"high_positive_unique_mints": current_high_positive_count()},
        "required_sample_counts": {"target_high_positive_unique_mints": TARGET_HIGH_POSITIVE_MINTS},
        "expected_number_of_slices": expected_slices,
        "maximum_allowed_slices": MAX_SLICES_PER_BATCH,
        "stop_conditions": [
            "sequence/hash/malformed/receiver blocker",
            "R2 verification failure",
            "artifact consistency failure",
            "local collector finalization hang",
            "VPS forbidden artifact or latest_run_id mutation",
            "candidate checkpoint or replay eligible candidate trigger",
            "high_positive_unique_mints target reached",
            "24h runtime reached",
            "max total slices reached",
        ],
        "proof_batch_mode": "batch",
        "max_attempted_launches": MAX_ATTEMPTED_LAUNCHES,
        "target_candidates": TARGET_CANDIDATES,
        "launch_caps_changed": False,
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
    path = STATUS_ROOT / f"batch_{batch_index:03d}_collection_justification.json"
    write_json(path, gate)
    return path


def run_preflight(control_env: pathlib.Path) -> dict[str, Any]:
    env = merged_env(control_env)
    env["PUMP_RELAY_CONTROL_ENV_FILE"] = str(control_env)
    proc = run_capture(["./scripts/relay_control_config_preflight.sh"], env=env, timeout=300)
    payload = read_json(REPO / "research_output" / "trading_strategy_pipeline" / "relay_control_config_preflight.json")
    payload.setdefault("returncode", proc.returncode)
    if proc.returncode != 0:
        payload.setdefault("classification", "RELAY_CONTROL_CONFIG_BLOCK_STRUCTURAL")
        payload.setdefault("blockers", []).append("relay_control_config_preflight_failed")
    return payload


def supervisor_status(control_env: pathlib.Path) -> dict[str, Any]:
    proc = run_capture(
        ["python3", "scripts/run_relay_r2_primary_batch.py", "status", "--env-file", str(control_env)],
        timeout=300,
    )
    for line in proc.stdout.splitlines():
        if line.startswith("STATUS "):
            return json.loads(line.removeprefix("STATUS "))
    return {"ok": False, "error": "missing_supervisor_status", "stdout_tail": proc.stdout[-1200:], "stderr_tail": proc.stderr[-1200:]}


def verify_deployed_sha(control_env: pathlib.Path) -> dict[str, Any]:
    env = merged_env(control_env)
    ssh_target = env.get("PUMP_RELAY_VPS_SSH_TARGET", "")
    ssh_key = env.get("PUMP_RELAY_VPS_SSH_KEY", "")
    app_dir = env.get("PUMP_RELAY_REMOTE_APP_DIR", "/home/ubuntu/pump-launch-quant")
    local_sha = run_capture(["git", "rev-parse", "HEAD"], timeout=30, check=True).stdout.strip()
    if not ssh_target or not ssh_key:
        return {"ok": False, "blocker": "missing_ssh_config", "local_sha": local_sha}
    remote_cmd = (
        "python3 - <<'PY'\n"
        "import json\n"
        f"from pathlib import Path\np=Path({app_dir!r})/'target/release/build_info.json'\n"
        "print(json.loads(p.read_text()).get('git_sha',''))\n"
        "PY"
    )
    proc = run_capture(
        ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-i", ssh_key, ssh_target, remote_cmd],
        timeout=60,
    )
    remote_sha = proc.stdout.strip()
    return {"ok": proc.returncode == 0 and remote_sha == local_sha, "local_sha": local_sha, "deployed_sha": remote_sha}


def local_inflight_processes() -> list[str]:
    proc = run_capture(
        ["pgrep", "-af", r"[l]ocal-stream-collector|[v]ps-stream-relay|[r]un_relay_r2_primary_batch"],
        timeout=30,
    )
    return [line for line in proc.stdout.splitlines() if line.strip()]


def pre_start_checks(control_env: pathlib.Path) -> dict[str, Any]:
    preflight = run_preflight(control_env)
    status = supervisor_status(control_env)
    deployed = verify_deployed_sha(control_env)
    inflight = local_inflight_processes()
    local = status.get("local_preflight", {})
    vps = status.get("vps_safety", {})
    blockers: list[str] = []
    if inflight:
        blockers.append("local_relay_or_supervisor_process_already_running")
    if preflight.get("classification") != "RELAY_CONTROL_CONFIG_PASS":
        blockers.append("relay_control_preflight_failed")
    if not local.get("ok"):
        blockers.append("local_r2_primary_preflight_failed")
    if safe_int(local.get("free_mb_output")) < safe_int(local.get("required_mb"), 10000):
        blockers.append("local_disk_below_r2_primary_gate")
    if vps.get("material_candidate_service") != "inactive" or vps.get("material_hunter_service") != "inactive":
        blockers.append("old_vps_material_hunter_active")
    if safe_int(vps.get("relay_running")) != 0:
        blockers.append("pump_vps_relay_already_running")
    if safe_int(vps.get("forbidden_recent")) != 0:
        blockers.append("vps_forbidden_artifacts_recent")
    if not deployed.get("ok"):
        blockers.append("deployed_sha_mismatch")
    return {
        "ok": not blockers,
        "blockers": blockers,
        "relay_control_preflight": preflight,
        "supervisor_status": status,
        "deployed_sha_check": deployed,
        "local_inflight_processes": inflight,
    }


def completed_slice_rows() -> list[dict[str, str]]:
    return read_csv_rows(SLICE_SUMMARIES_PATH)


def aggregate_status(base: dict[str, Any] | None = None) -> dict[str, Any]:
    rows = completed_slice_rows()
    high_positive = current_high_positive_count()
    candidate = sum(safe_int(r.get("candidate_checkpoint_count")) for r in rows)
    replay = sum(safe_int(r.get("replay_eligible_candidate_count")) for r in rows)
    blockers = [r for r in rows if r.get("blocker_if_any")]
    payload = {
        **(base or read_json(STATUS_PATH) or {}),
        "updated_at_utc": utc_stamp(),
        "slices_attempted": len(rows),
        "counted_slices": sum(1 for r in rows if "PASS" in r.get("classification", "")),
        "current_high_positive_unique_mints": high_positive,
        "target_high_positive_unique_mints": TARGET_HIGH_POSITIVE_MINTS,
        "additional_high_positive_needed": max(0, TARGET_HIGH_POSITIVE_MINTS - high_positive),
        "candidate_checkpoint_count": candidate,
        "replay_eligible_candidate_count": replay,
        "early_burst_review_candidate_count": sum(
            safe_int(r.get("early_burst_review_candidate_count")) for r in rows
        ),
        "blocker_rows": len(blockers),
        "total_all_launches_indexed": sum(safe_int(r.get("all_launches_indexed")) for r in rows),
        "total_cheap_followup_rows": sum(safe_int(r.get("cheap_followup_rows")) for r in rows),
        "total_rich_tracked_launches": sum(safe_int(r.get("rich_tracked_launches")) for r in rows),
        "total_retention_deleted_bytes": sum(safe_int(r.get("retention_deleted_bytes")) for r in rows),
        "generic_collection_allowed": False,
        "replay_allowed": False,
        "formal_backtesting_allowed": False,
        "threshold_tuning_allowed": False,
        "paper_trading_enabled": False,
        "live_trading_enabled": False,
        "wallet_execution_enabled": False,
        "launch_caps_remain_blocked": True,
    }
    return payload


def blocker_from_slice(row: dict[str, Any]) -> str:
    if safe_int(row.get("sequence_gap_count")) > 0:
        return "sequence_gap"
    if safe_int(row.get("hash_mismatch_count")) > 0:
        return "hash_mismatch"
    if safe_int(row.get("malformed_frame_count")) > 0:
        return "malformed_frame"
    if safe_int(row.get("receiver_backpressure_count")) > 0:
        return "receiver_backpressure"
    if safe_int(row.get("receiver_unavailable_count")) > 0:
        return "receiver_unavailable"
    if safe_int(row.get("r2_failed", row.get("r2_failures", 0))) > 0:
        return "r2_verification_failure"
    if row.get("artifact_consistency_ok") is False or str(row.get("artifact_consistency_ok")).lower() == "false":
        return "artifact_consistency_failure"
    vps = row.get("vps_safety") if isinstance(row.get("vps_safety"), dict) else {}
    if safe_int(vps.get("forbidden_recent")) != 0:
        return "vps_forbidden_artifact"
    if vps.get("material_candidate_service") != "inactive" or vps.get("material_hunter_service") != "inactive":
        return "old_vps_material_hunter_active"
    return ""


def mirror_slice(batch_index: int, batch_log_dir: pathlib.Path, report_summary: dict[str, Any]) -> dict[str, Any]:
    summary_path = batch_log_dir / "batch_summary.ndjson"
    rows = [json.loads(raw) for raw in summary_path.read_text(encoding="utf-8").splitlines() if raw.strip()]
    if not rows:
        raise CollectorError(f"missing batch summary row: {summary_path}")
    row = rows[-1]
    blocker = blocker_from_slice(row)
    high_positive = current_high_positive_count()
    rendered = {
        "batch_index": batch_index,
        "slice_index": len(completed_slice_rows()) + 1,
        "slice_id": row.get("run", ""),
        "relay_session_id": row.get("relay_session_id", ""),
        "classification": row.get("classification", ""),
        "frames_received": row.get("frames_received", ""),
        "all_launches_seen": row.get("all_launches_seen", ""),
        "all_launches_indexed": row.get("all_launches_indexed", ""),
        "cheap_followup_rows": row.get("cheap_followup_rows", ""),
        "rich_tracked_launches": row.get("rich_tracked_launches", ""),
        "promotion_recommended_count": row.get("promotion_recommended_count", ""),
        "promotion_admitted_count": row.get("promotion_admitted_count", ""),
        "promotion_blocked_count": row.get("promotion_blocked_budget_count", ""),
        "missed_good_token_audit_v2_rows": row.get("missed_good_token_count", ""),
        "cheap_only_later_positive_count": row.get("cheap_only_later_positive_count", 0),
        "cheap_only_later_high_positive_count": row.get("cheap_only_later_high_positive_count", 0),
        "attempted_launches": row.get("attempted_launches", ""),
        "unique_attempted_mints": row.get("unique_attempted_mints", ""),
        "rejected_dead_count": row.get("rejected_dead_count", ""),
        "terminal_inconclusive_count": row.get("terminal_inconclusive_count", ""),
        "candidate_checkpoint_count": row.get("candidate_checkpoint_count", ""),
        "replay_eligible_candidate_count": row.get("replay_eligible_candidate_count", ""),
        "early_burst_review_candidate_count": row.get("early_burst_review_candidate_count", ""),
        "early_burst_review_unique_mint_count": row.get("early_burst_review_unique_mint_count", ""),
        "early_burst_review_replay_eligible_candidate_count": row.get(
            "early_burst_review_replay_eligible_candidate_count", ""
        ),
        "positive_outcome_rows": report_summary.get("positive_outcome_rows", ""),
        "high_positive_outcome_rows": report_summary.get("high_positive_outcome_rows", ""),
        "high_positive_unique_total": high_positive,
        "sequence_gap_count": row.get("sequence_gap_count", ""),
        "hash_mismatch_count": row.get("hash_mismatch_count", ""),
        "malformed_frame_count": row.get("malformed_frame_count", ""),
        "receiver_backpressure_count": row.get("receiver_backpressure_count", ""),
        "receiver_unavailable_count": row.get("receiver_unavailable_count", ""),
        "r2_verified": str(safe_int(row.get("r2_failed", 0)) == 0).lower(),
        "r2_failed_files": row.get("r2_failed", row.get("r2_failures", "")),
        "artifact_consistency_ok": row.get("artifact_consistency_ok", ""),
        "retention_deleted_bytes": row.get("retention_deleted_bytes", ""),
        "local_retained_bytes": row.get("local_retained_bytes", ""),
        "vps_safety_ok": str(not blocker.startswith("vps") and blocker != "old_vps_material_hunter_active").lower(),
        "blocker_if_any": blocker,
    }
    append_csv_row(SLICE_SUMMARIES_PATH, rendered, SLICE_FIELDS)
    return rendered


def report_summary_from_outputs() -> dict[str, Any]:
    positive_path = REPO / "research_output" / "trading_strategy_pipeline" / "positive_outcome_labels.csv"
    positive_rows = read_csv_rows(positive_path)
    return {
        "positive_outcome_rows": len([r for r in positive_rows if r.get("positive_outcome_label") in {"positive", "high_positive"}]),
        "high_positive_outcome_rows": len([r for r in positive_rows if r.get("positive_outcome_label") == "high_positive"]),
    }


def write_batch_summary(batch_index: int) -> None:
    rows = [r for r in completed_slice_rows() if safe_int(r.get("batch_index")) == batch_index]
    if not rows:
        return
    md = STATUS_ROOT / f"BATCH_{batch_index}_SUMMARY.md"
    md.write_text(
        "# Background 24h Batch Summary\n\n"
        f"- batch_index: `{batch_index}`\n"
        f"- slices: `{len(rows)}`\n"
        f"- frames_received: `{sum(safe_int(r.get('frames_received')) for r in rows)}`\n"
        f"- all_launches_indexed: `{sum(safe_int(r.get('all_launches_indexed')) for r in rows)}`\n"
        f"- cheap_followup_rows: `{sum(safe_int(r.get('cheap_followup_rows')) for r in rows)}`\n"
        f"- rich_tracked_launches: `{sum(safe_int(r.get('rich_tracked_launches')) for r in rows)}`\n"
        f"- candidate_checkpoints: `{sum(safe_int(r.get('candidate_checkpoint_count')) for r in rows)}`\n"
        f"- replay_eligible_candidates: `{sum(safe_int(r.get('replay_eligible_candidate_count')) for r in rows)}`\n"
        f"- early_burst_review_candidates: `{sum(safe_int(r.get('early_burst_review_candidate_count')) for r in rows)}`\n"
        f"- high_positive_unique_total: `{current_high_positive_count()}`\n"
        f"- blockers: `{sum(1 for r in rows if r.get('blocker_if_any'))}`\n"
        "\nReplay/backtesting/tuning/paper/live/wallet remain blocked. Launch caps remain blocked.\n",
        encoding="utf-8",
    )


def write_final_report(classification: str, blocker: str = "") -> None:
    rows = completed_slice_rows()
    high_positive = current_high_positive_count()
    md = STATUS_ROOT / "BACKGROUND_24H_COLLECTION_REPORT.md"
    md.write_text(
        "# Background 24h Collection Report\n\n"
        f"- classification: `{classification}`\n"
        f"- blocker: `{blocker}`\n"
        f"- total_slices_attempted: `{len(rows)}`\n"
        f"- total_counted_slices: `{sum(1 for r in rows if 'PASS' in r.get('classification', ''))}`\n"
        f"- total_all_launches_indexed: `{sum(safe_int(r.get('all_launches_indexed')) for r in rows)}`\n"
        f"- total_cheap_followup_rows: `{sum(safe_int(r.get('cheap_followup_rows')) for r in rows)}`\n"
        f"- total_rich_tracked_launches: `{sum(safe_int(r.get('rich_tracked_launches')) for r in rows)}`\n"
        f"- total_high_positive_examples_found: `{max(0, high_positive - BASELINE_HIGH_POSITIVE_MINTS)}`\n"
        f"- current_high_positive_unique_count: `{high_positive}`\n"
        f"- target_high_positive_count: `{TARGET_HIGH_POSITIVE_MINTS}`\n"
        f"- candidate_checkpoints: `{sum(safe_int(r.get('candidate_checkpoint_count')) for r in rows)}`\n"
        f"- replay_eligible_candidates: `{sum(safe_int(r.get('replay_eligible_candidate_count')) for r in rows)}`\n"
        f"- early_burst_review_candidates: `{sum(safe_int(r.get('early_burst_review_candidate_count')) for r in rows)}`\n"
        f"- blockers_encountered: `{sum(1 for r in rows if r.get('blocker_if_any'))}`\n"
        f"- more_collection_justified: `{str(high_positive < TARGET_HIGH_POSITIVE_MINTS and not blocker).lower()}`\n"
        f"- backtesting_remains_blocked: `true`\n"
        f"- replay_remains_blocked: `true`\n"
        f"- launch_caps_remain_blocked: `true`\n",
        encoding="utf-8",
    )


def stop_classification(status: dict[str, Any], started_at: float) -> tuple[bool, str, str]:
    rows = completed_slice_rows()
    high_positive = current_high_positive_count()
    if status.get("blocker"):
        return True, "BACKGROUND_24H_COLLECTION_BLOCK_RELAY", str(status.get("blocker"))
    if rows:
        last = rows[-1]
        blocker = last.get("blocker_if_any", "")
        if blocker:
            if blocker.startswith("r2"):
                return True, "BACKGROUND_24H_COLLECTION_BLOCK_R2", blocker
            if blocker in {"artifact_consistency_failure"}:
                return True, "BACKGROUND_24H_COLLECTION_BLOCK_ARTIFACT", blocker
            if blocker in {"receiver_backpressure", "receiver_unavailable", "sequence_gap", "hash_mismatch", "malformed_frame"}:
                return True, "BACKGROUND_24H_COLLECTION_BLOCK_RELAY", blocker
            return True, "BACKGROUND_24H_COLLECTION_BLOCK_ARTIFACT", blocker
    candidate = sum(safe_int(r.get("candidate_checkpoint_count")) for r in rows)
    replay = sum(safe_int(r.get("replay_eligible_candidate_count")) for r in rows)
    if candidate > 0 or replay > 0:
        return True, "CANDIDATE_REVIEW_TRIGGERED", "candidate_or_replay_trigger"
    if high_positive >= TARGET_HIGH_POSITIVE_MINTS:
        return True, "BACKGROUND_24H_COLLECTION_STOPPED_TARGET_REACHED", "high_positive_target_reached"
    if len(rows) >= MAX_TOTAL_SLICES:
        return True, "BACKGROUND_24H_COLLECTION_PASS", "max_slices_total_reached"
    if time.time() - started_at >= MAX_TOTAL_RUNTIME_HOURS * 3600:
        return True, "BACKGROUND_24H_COLLECTION_PASS", "max_runtime_reached"
    return False, "", ""


def run_one_slice(control_env: pathlib.Path, batch_index: int, slice_global_index: int) -> int:
    gate_path = write_batch_gate(batch_index, MAX_SLICES_PER_BATCH)
    batch_log_dir = STATUS_ROOT / f"batch_{batch_index:03d}" / f"slice_{slice_global_index:03d}_{compact_stamp()}"
    cmd = [
        "python3",
        "scripts/run_relay_r2_primary_batch.py",
        "batch",
        "--env-file",
        str(control_env),
        "--slices",
        "1",
        "--counted-slices-target",
        "1",
        "--max-total-slices",
        "1",
        "--duration-seconds",
        str(SLICE_DURATION_SECONDS),
        "--local-receiver-window-seconds",
        str(LOCAL_RECEIVER_WINDOW_SECONDS),
        "--max-attempted-launches",
        str(MAX_ATTEMPTED_LAUNCHES),
        "--target-candidates",
        str(TARGET_CANDIDATES),
        "--max-concurrent-tracked-mints",
        str(MAX_CONCURRENT_TRACKED_MINTS),
        "--run-prefix",
        "background-24h-early-burst",
        "--batch-log-dir",
        str(batch_log_dir),
        "--require-collection-justification",
        "--collection-justification-path",
        str(gate_path),
        "--justification-id",
        f"background-24h-early-burst-batch-{batch_index:03d}",
        "--max-slices",
        str(MAX_SLICES_PER_BATCH),
        "--target-gate",
        TARGET_GATE,
        "--early-burst-in-out-v1-review-artifacts-enabled",
        "--early-burst-in-out-v1-review-artifacts-mode",
        "emit_review_only",
        "--promotion-policy",
        "v1_controlled",
    ]
    env = merged_env(control_env)
    append_event({"event": "slice_start", "batch_index": batch_index, "slice_global_index": slice_global_index})
    proc = subprocess.run(cmd, cwd=REPO, env=env, text=True)
    reports = run_reports()
    if proc.returncode == 0:
        mirror_slice(batch_index, batch_log_dir, report_summary_from_outputs())
    else:
        append_event({"event": "slice_blocked", "batch_index": batch_index, "returncode": proc.returncode})
    status = aggregate_status(read_json(STATUS_PATH))
    status.update(
        {
            "state": "running" if proc.returncode == 0 else "blocked",
            "blocker": "" if proc.returncode == 0 else "supervisor_slice_failed",
            "last_slice_returncode": proc.returncode,
            "last_report_refresh_ok": reports.get("ok"),
            "current_batch_index": batch_index,
        }
    )
    write_json(STATUS_PATH, status)
    write_live_summary(status)
    append_event({"event": "slice_complete", "batch_index": batch_index, "returncode": proc.returncode})
    return proc.returncode


def worker(args: argparse.Namespace) -> int:
    started_at = time.time()
    current_high = current_high_positive_count()
    write_master_justification(current_high)
    status = aggregate_status(read_json(STATUS_PATH))
    status.update(
        {
            "schema_version": "phase107k.background_24h_collector.v1",
            "state": "running",
            "classification": "BACKGROUND_24H_COLLECTION_RUNNING",
            "pid": os.getpid(),
            "started_at_utc": utc_stamp(),
            "max_total_runtime_hours": MAX_TOTAL_RUNTIME_HOURS,
            "max_slices_total": MAX_TOTAL_SLICES,
            "max_slices_per_batch": MAX_SLICES_PER_BATCH,
            "target_gate": TARGET_GATE,
        }
    )
    write_json(STATUS_PATH, status)
    write_live_summary(status)
    append_event({"event": "worker_started", "pid": os.getpid()})
    rc = 0
    while True:
        rows = completed_slice_rows()
        stop, classification, reason = stop_classification(read_json(STATUS_PATH), started_at)
        if stop:
            status = aggregate_status(read_json(STATUS_PATH))
            status.update({"state": "complete", "classification": classification, "stop_reason": reason, "pid": None})
            write_json(STATUS_PATH, status)
            write_live_summary(status)
            write_final_report(classification, reason)
            append_event({"event": "worker_complete", "classification": classification, "reason": reason})
            return rc
        batch_index = len(rows) // MAX_SLICES_PER_BATCH + 1
        slice_index = len(rows) + 1
        rc = run_one_slice(args.control_env, batch_index, slice_index)
        if rc != 0:
            status = aggregate_status(read_json(STATUS_PATH))
            status.update(
                {
                    "state": "blocked",
                    "classification": "BACKGROUND_24H_COLLECTION_BLOCK_RELAY",
                    "blocker": "supervisor_slice_failed",
                    "pid": None,
                }
            )
            write_json(STATUS_PATH, status)
            write_live_summary(status)
            write_final_report("BACKGROUND_24H_COLLECTION_BLOCK_RELAY", "supervisor_slice_failed")
            return rc
        if slice_index % MAX_SLICES_PER_BATCH == 0:
            write_batch_summary(batch_index)


def start(args: argparse.Namespace) -> int:
    STATUS_ROOT.mkdir(parents=True, exist_ok=True)
    ensure_review_queue()
    existing = read_json(STATUS_PATH)
    if pid_running(safe_int(existing.get("pid"), 0)):
        raise CollectorError(f"background 24h collector already running pid={existing.get('pid')}")
    checks = pre_start_checks(args.control_env)
    if not checks.get("ok"):
        status = aggregate_status(
            {
                "schema_version": "phase107k.background_24h_collector.v1",
                "state": "blocked",
                "classification": "BACKGROUND_24H_COLLECTION_BLOCK_EXTERNAL",
                "blocker": ",".join(checks.get("blockers", [])),
                "pre_start_checks": checks,
                "pid": None,
            }
        )
        write_json(STATUS_PATH, status)
        write_live_summary(status)
        append_event({"event": "blocked", "blockers": checks.get("blockers", [])})
        print(json.dumps(status, sort_keys=True))
        return 2
    current_high = current_high_positive_count()
    write_master_justification(current_high)
    worker_cmd = [sys.executable, str(pathlib.Path(__file__).resolve()), "worker", "--control-env", str(args.control_env)]
    log = (STATUS_ROOT / "worker.log").open("a", encoding="utf-8")
    err = (STATUS_ROOT / "worker.err").open("a", encoding="utf-8")
    proc = subprocess.Popen(worker_cmd, cwd=REPO, stdout=log, stderr=err, start_new_session=True, text=True)
    status = aggregate_status(
        {
            "schema_version": "phase107k.background_24h_collector.v1",
            "state": "running",
            "classification": "BACKGROUND_24H_COLLECTION_RUNNING",
            "pid": proc.pid,
            "started_at_utc": utc_stamp(),
            "target_gate": TARGET_GATE,
            "max_total_runtime_hours": MAX_TOTAL_RUNTIME_HOURS,
            "max_slices_total": MAX_TOTAL_SLICES,
            "max_slices_per_batch": MAX_SLICES_PER_BATCH,
            "pre_start_checks": checks,
        }
    )
    write_json(STATUS_PATH, status)
    write_live_summary(status)
    append_event({"event": "started", "pid": proc.pid})
    print(json.dumps(status, sort_keys=True))
    return 0


def status(_: argparse.Namespace) -> int:
    payload = aggregate_status(read_json(STATUS_PATH))
    pid = safe_int(payload.get("pid"), 0) or None
    payload["process_alive"] = pid_running(pid)
    if payload.get("state") == "running" and not payload["process_alive"]:
        payload["state"] = "needs_recovery"
        payload["classification"] = "BACKGROUND_24H_COLLECTION_BLOCK_LOCAL_FINALIZATION"
        payload["blocker"] = "worker_pid_missing"
    write_json(STATUS_PATH, payload)
    write_live_summary(payload)
    print(json.dumps(payload, sort_keys=True))
    return 0


def stop(_: argparse.Namespace) -> int:
    payload = read_json(STATUS_PATH)
    pid = safe_int(payload.get("pid"), 0) or None
    if pid_running(pid):
        os.killpg(os.getpgid(pid), signal.SIGTERM)
        payload["state"] = "stopping"
        payload["stop_requested_at_utc"] = utc_stamp()
        append_event({"event": "stop_requested", "pid": pid})
    payload = aggregate_status(payload)
    write_json(STATUS_PATH, payload)
    write_live_summary(payload)
    print(json.dumps(payload, sort_keys=True))
    return 0


def recover(_: argparse.Namespace) -> int:
    payload = aggregate_status(read_json(STATUS_PATH))
    pid = safe_int(payload.get("pid"), 0) or None
    payload["process_alive"] = pid_running(pid)
    if not payload["process_alive"] and payload.get("state") == "running":
        payload["state"] = "recovered"
        payload["classification"] = "BACKGROUND_24H_COLLECTION_BLOCK_LOCAL_FINALIZATION"
        payload["blocker"] = "worker_exited_before_terminal_classification"
    write_json(STATUS_PATH, payload)
    write_live_summary(payload)
    print(json.dumps(payload, sort_keys=True))
    return 0


def summarize(_: argparse.Namespace) -> int:
    rows = completed_slice_rows()
    summary = aggregate_status(read_json(STATUS_PATH))
    summary["source"] = "background_24h_collector_files"
    summary["review_queue_rows"] = len(read_csv_rows(REVIEW_QUEUE_PATH))
    summary["slice_summary_rows"] = len(rows)
    print(json.dumps(summary, sort_keys=True))
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["start", "status", "stop", "recover", "summarize", "worker"])
    parser.add_argument("--control-env", type=pathlib.Path, default=DEFAULT_CONTROL_ENV)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.command == "start":
        return start(args)
    if args.command == "worker":
        return worker(args)
    if args.command == "status":
        return status(args)
    if args.command == "stop":
        return stop(args)
    if args.command == "recover":
        return recover(args)
    if args.command == "summarize":
        return summarize(args)
    raise CollectorError(f"unknown command {args.command}")


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except CollectorError as exc:
        status_payload = aggregate_status(
            {
                "schema_version": "phase107k.background_24h_collector.v1",
                "state": "blocked",
                "classification": "BACKGROUND_24H_COLLECTION_BLOCK_EXTERNAL",
                "blocker": str(exc),
                "pid": None,
            }
        )
        write_json(STATUS_PATH, status_payload)
        write_live_summary(status_payload)
        print(json.dumps(status_payload, sort_keys=True))
        raise SystemExit(2)
