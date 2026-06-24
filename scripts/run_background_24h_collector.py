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
import shutil
import signal
import subprocess
import sys
import time
from typing import Any


REPO = pathlib.Path(__file__).resolve().parents[1]
STATUS_ROOT = pathlib.Path(
    os.environ.get(
        "PUMP_BACKGROUND_24H_STATUS_ROOT",
        str(REPO / "research_output" / "trading_strategy_pipeline" / "background_24h_collector"),
    )
).expanduser()
STATUS_PATH = STATUS_ROOT / "status.json"
LIVE_SUMMARY_PATH = STATUS_ROOT / "live_summary.md"
EVENTS_PATH = STATUS_ROOT / "events.ndjson"
SLICE_SUMMARIES_PATH = STATUS_ROOT / "slice_summaries.csv"
REVIEW_QUEUE_PATH = STATUS_ROOT / "review_queue.csv"
JUSTIFICATION_BASENAME = os.environ.get(
    "PUMP_BACKGROUND_24H_JUSTIFICATION_BASENAME",
    "BACKGROUND_24H_COLLECTION_JUSTIFICATION",
)
MASTER_JUSTIFICATION_JSON = (
    STATUS_ROOT / f"{JUSTIFICATION_BASENAME}.json"
)
MASTER_JUSTIFICATION_MD = (
    STATUS_ROOT / f"{JUSTIFICATION_BASENAME}.md"
)
DEFAULT_CONTROL_ENV = REPO / ".codex_runtime_env" / "relay_control.env"
TARGET_GATE = os.environ.get("PUMP_BACKGROUND_24H_TARGET_GATE", "EARLY_BURST_BACKTEST_READINESS")
COLLECTION_REASON = os.environ.get(
    "PUMP_BACKGROUND_24H_COLLECTION_REASON",
    "targeted_early_burst_feature_complete_collection",
)
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
LOCAL_DISK_BLOCK_CLASSIFICATION = "BACKGROUND_24H_COLLECTION_BLOCK_LOCAL_DISK"
LOCAL_DISK_CAPACITY_BLOCK_CLASSIFICATION = "BACKGROUND_24H_COLLECTION_BLOCK_LOCAL_DISK_CAPACITY"
DEFAULT_STORAGE_MODE = os.environ.get("PUMP_BACKGROUND_24H_STORAGE_MODE", "r2-streaming")
R2_STREAMING_MIN_FREE_MB = int(os.environ.get("PUMP_R2_STREAMING_MIN_FREE_MB", "4096"))
R2_STREAMING_SPOOL_MB = int(os.environ.get("PUMP_R2_STREAMING_SPOOL_MB", "2048"))
R2_STREAMING_CHUNK_MB = int(os.environ.get("PUMP_R2_STREAMING_CHUNK_MB", "32"))
MAX_LOCAL_COLLECTOR_USAGE_MB = int(os.environ.get("PUMP_MAX_LOCAL_COLLECTOR_USAGE_MB", "5000"))
HARD_MIN_FREE_MB = 10_000
RECOMMENDED_RESUME_FREE_MB = 15_000
PREFERRED_24H_FREE_MB = 20_000
LOW_DISK_OVERRIDE_ENV = "PUMP_ALLOW_24H_RESUME_BELOW_RECOMMENDED_DISK"
OUTPUT_ROOT_ENV = "PUMP_LOCAL_COLLECTOR_OUTPUT_ROOT"
SPOOL_ROOT_ENV = "PUMP_R2_PRIMARY_SPOOL_ROOT"
LOCAL_DISK_BLOCKER_PREFIXES = (
    "local_output_disk_below_required",
    "local_spool_disk_below_required",
    "local_disk_below_r2_primary_gate",
    "local_disk_below_r2_streaming_gate",
    "r2_primary_local_disk_preflight_failed",
    "r2_streaming_local_disk_preflight_failed",
)

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
    "storage_mode",
    "local_collector_usage_mb",
    "max_local_collector_usage_mb",
    "local_spool_bytes_current",
    "local_spool_bytes_peak",
    "local_spool_bytes_limit",
    "local_disk_free_mb",
    "r2_streaming_uploaded_chunks",
    "r2_streaming_verified_chunks",
    "r2_streaming_deleted_local_chunks",
    "r2_streaming_unverified_chunks",
    "r2_streaming_retry_count",
    "r2_streaming_upload_timeout_count",
    "r2_streaming_backpressure_detected",
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
    # Allow explicit runtime bucket overrides to win over sourced env files.
    # The proven R2-streaming path can route report/calibration/compat objects
    # to the dataset bucket when legacy bucket names are not provisioned.
    for key in ("R2_REPORTS_BUCKET", "R2_CALIBRATION_BUCKET", "R2_PROVIDER_COMPAT_BUCKET"):
        if os.environ.get(key):
            env[key] = os.environ[key]
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


def json_from_stdout(stdout: str) -> dict[str, Any]:
    stripped = stdout.strip()
    if stripped:
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(stripped[start : end + 1])
            except json.JSONDecodeError:
                pass
    for raw in reversed(stdout.splitlines()):
        line = raw.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return {}


def run_local_r2_primary_preflight(control_env: pathlib.Path) -> dict[str, Any]:
    env = merged_env(control_env)
    output_root = configured_output_root(env)
    spool_root = configured_spool_root(env)
    storage_mode = env.get("PUMP_BACKGROUND_24H_STORAGE_MODE", DEFAULT_STORAGE_MODE)
    streaming_mode = storage_mode == "r2-streaming"
    env.update(
        {
            "PUMP_RELAY_CONTROL_ENV_FILE": str(control_env),
            "LOCAL_COLLECTOR_STORAGE_MODE": storage_mode,
            "LOCAL_COLLECTOR_PREFLIGHT_MODE": "collection",
            "LOCAL_COLLECTOR_RETENTION_MODE": "keep-manifests-after-verified-r2",
            "LOCAL_COLLECTOR_R2_UPLOAD_REQUIRED": "true",
            "LOCAL_COLLECTOR_VERIFY_R2_HEALTH_LIVE": "true",
            "UPLOAD_R2": "true",
        }
    )
    if output_root:
        env["LOCAL_COLLECTOR_OUTPUT_DIR"] = str(output_root)
    if spool_root:
        env["LOCAL_COLLECTOR_R2_SPOOL_DIR"] = str(spool_root)
    cmd = [
        "./scripts/local_stream_collector_preflight.sh",
        "--storage-mode",
        storage_mode,
        "--mode",
        "collection",
        "--verify-r2-health-live",
    ]
    if streaming_mode:
        cmd.extend(["--r2-spool-max-mb", str(R2_STREAMING_SPOOL_MB)])
        cmd.extend(["--min-free-mb", str(R2_STREAMING_MIN_FREE_MB)])
    proc = run_capture(
        cmd,
        env=env,
        timeout=600,
    )
    payload = json_from_stdout(proc.stdout)
    payload["configured_output_root"] = str(output_root) if output_root else ""
    payload["configured_spool_root"] = str(spool_root) if spool_root else ""
    payload["background_storage_mode"] = storage_mode
    payload["output_spool_override_validation"] = validate_output_spool_overrides(
        env,
        min_free_mb=R2_STREAMING_MIN_FREE_MB if streaming_mode else HARD_MIN_FREE_MB,
    )
    payload.setdefault("returncode", proc.returncode)
    payload.setdefault("stdout_tail", proc.stdout[-1200:])
    payload.setdefault("stderr_tail", proc.stderr[-1200:])
    return payload


def configured_output_root(env: dict[str, str]) -> pathlib.Path | None:
    value = env.get(OUTPUT_ROOT_ENV, "").strip()
    return pathlib.Path(value).expanduser() if value else None


def configured_spool_root(env: dict[str, str]) -> pathlib.Path | None:
    value = env.get(SPOOL_ROOT_ENV, "").strip()
    return pathlib.Path(value).expanduser() if value else None


def path_free_mb(path: pathlib.Path) -> int:
    target = path if path.exists() else path.parent
    usage = shutil.disk_usage(target)
    return usage.free // (1024 * 1024)


def is_r2_streaming_storage_mode(storage_mode: str) -> bool:
    return storage_mode in {"r2-streaming", "r2_streaming"}


def storage_recommended_resume_mb(storage_mode: str) -> int:
    return R2_STREAMING_MIN_FREE_MB if is_r2_streaming_storage_mode(storage_mode) else RECOMMENDED_RESUME_FREE_MB


def storage_preferred_24h_mb(storage_mode: str) -> int:
    if is_r2_streaming_storage_mode(storage_mode):
        return max(R2_STREAMING_MIN_FREE_MB, R2_STREAMING_SPOOL_MB * 2)
    return PREFERRED_24H_FREE_MB


def validate_storage_override_path(
    path: pathlib.Path | None,
    *,
    label: str,
    min_free_mb: int,
) -> dict[str, Any]:
    if path is None:
        return {"configured": False, "ok": True}
    resolved = path.resolve()
    codex_env = (REPO / ".codex_runtime_env").resolve()
    blockers: list[str] = []
    if codex_env in resolved.parents or resolved == codex_env:
        blockers.append(f"{label}_inside_codex_runtime_env")
    if not resolved.exists():
        blockers.append(f"{label}_missing")
    elif not resolved.is_dir():
        blockers.append(f"{label}_not_directory")
    elif not os.access(resolved, os.W_OK):
        blockers.append(f"{label}_not_writable")
    free = path_free_mb(resolved)
    if free < min_free_mb:
        blockers.append(f"{label}_disk_below_hard_min:{free}<{min_free_mb}")
    return {
        "configured": True,
        "path": str(resolved),
        "ok": not blockers,
        "free_mb": free,
        "blockers": blockers,
    }


def validate_output_spool_overrides(env: dict[str, str], *, min_free_mb: int | None = None) -> dict[str, Any]:
    storage_mode = env.get("PUMP_BACKGROUND_24H_STORAGE_MODE", DEFAULT_STORAGE_MODE)
    floor = min_free_mb if min_free_mb is not None else storage_recommended_resume_mb(storage_mode)
    output = validate_storage_override_path(configured_output_root(env), label="output_root", min_free_mb=floor)
    spool = validate_storage_override_path(configured_spool_root(env), label="spool_root", min_free_mb=floor)
    blockers = list(output.get("blockers") or []) + list(spool.get("blockers") or [])
    return {"ok": not blockers, "output_root": output, "spool_root": spool, "blockers": blockers}


def local_disk_blockers(preflight: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    required = safe_int(preflight.get("required_mb"), 10000)
    free_output = safe_int(preflight.get("free_mb_output"))
    free_spool = safe_int(preflight.get("free_mb_spool"), free_output)
    if free_output and free_output < required:
        blockers.append(f"local_output_disk_below_required:{free_output}<{required}")
    if free_spool and free_spool < required:
        blockers.append(f"local_spool_disk_below_required:{free_spool}<{required}")
    for blocker in preflight.get("blockers") or []:
        text = str(blocker)
        if "disk" in text or "spool" in text:
            blockers.append(text)
    if preflight.get("returncode") != 0 and not blockers:
        stdout_tail = str(preflight.get("stdout_tail", ""))
        stderr_tail = str(preflight.get("stderr_tail", ""))
        if "disk_below" in stdout_tail or "disk_below" in stderr_tail:
            blockers.append("r2_primary_local_disk_preflight_failed")
    override = preflight.get("output_spool_override_validation") or {}
    blockers.extend(str(item) for item in override.get("blockers") or [])
    return blockers


def is_local_disk_blocker(blocker: str) -> bool:
    return any(str(blocker).startswith(prefix) or prefix in str(blocker) for prefix in LOCAL_DISK_BLOCKER_PREFIXES)


def run_storage_gc(*, apply: bool, min_free_mb: int) -> dict[str, Any]:
    cmd = [
        "python3",
        "scripts/local_r2_first_storage_enforcer.py",
        "--max-local-stream-mb",
        str(MAX_LOCAL_COLLECTOR_USAGE_MB),
        "--include-legacy-unverified-raw-spool",
    ]
    if apply:
        cmd.append("--apply")
    proc = run_capture(cmd, timeout=900)
    payload = json_from_stdout(proc.stdout)
    payload.setdefault("returncode", proc.returncode)
    payload.setdefault("stdout_tail", proc.stdout[-1200:])
    payload.setdefault("stderr_tail", proc.stderr[-1200:])
    payload.setdefault(
        "report_path",
        str(
            STATUS_ROOT
            / ("local_storage_enforcement_apply.json" if apply else "local_storage_enforcement_dry_run.json")
        ),
    )
    return payload


def low_disk_override_enabled(control_env: pathlib.Path) -> bool:
    env = merged_env(control_env)
    return boolish(env.get(LOW_DISK_OVERRIDE_ENV))


def ensure_local_storage_ready(control_env: pathlib.Path) -> dict[str, Any]:
    before = run_local_r2_primary_preflight(control_env)
    blockers = local_disk_blockers(before)
    required = safe_int(before.get("required_mb"), 10000)
    free_output = safe_int(before.get("free_mb_output"))
    storage_mode = str(before.get("storage_mode") or before.get("background_storage_mode") or DEFAULT_STORAGE_MODE)
    recommended = storage_recommended_resume_mb(storage_mode)
    preferred = storage_preferred_24h_mb(storage_mode)
    override = low_disk_override_enabled(control_env)
    budget_dry_run = run_storage_gc(apply=False, min_free_mb=max(required, recommended))
    local_usage_mb = safe_int(budget_dry_run.get("local_stream_collector_mb_after"))
    budget_apply: dict[str, Any] | None = None
    if local_usage_mb > MAX_LOCAL_COLLECTOR_USAGE_MB:
        budget_apply = run_storage_gc(apply=True, min_free_mb=max(required, recommended))
        local_usage_mb = safe_int(budget_apply.get("local_stream_collector_mb_after"), local_usage_mb)
        if local_usage_mb > MAX_LOCAL_COLLECTOR_USAGE_MB:
            return {
                "ok": False,
                "preflight_before": before,
                "preflight_after": before,
                "gc_ran": True,
                "gc_dry_run": budget_dry_run,
                "gc_apply": budget_apply,
                "blockers": blockers
                + [f"local_stream_collector_usage_above_budget:{local_usage_mb}>{MAX_LOCAL_COLLECTOR_USAGE_MB}"],
                "hard_min_free_mb": required,
                "recommended_resume_free_mb": recommended,
                "preferred_24h_free_mb": preferred,
                "max_local_collector_usage_mb": MAX_LOCAL_COLLECTOR_USAGE_MB,
                "local_stream_collector_usage_mb": local_usage_mb,
                "storage_mode": storage_mode,
                "low_disk_operator_override": override,
            }
    if not blockers and before.get("returncode") == 0 and (free_output >= recommended or override):
        return {
            "ok": True,
            "preflight_before": before,
            "preflight_after": before,
            "gc_ran": budget_apply is not None,
            "gc_dry_run": budget_dry_run,
            "gc_apply": budget_apply,
            "blockers": [],
            "hard_min_free_mb": required,
            "recommended_resume_free_mb": recommended,
            "preferred_24h_free_mb": preferred,
            "max_local_collector_usage_mb": MAX_LOCAL_COLLECTOR_USAGE_MB,
            "local_stream_collector_usage_mb": local_usage_mb,
            "storage_mode": storage_mode,
            "low_disk_operator_override": override,
        }

    if not blockers and before.get("returncode") != 0:
        return {
            "ok": False,
            "preflight_before": before,
            "preflight_after": before,
            "gc_ran": False,
            "blockers": ["local_r2_primary_preflight_failed_non_disk"],
            "hard_min_free_mb": required,
            "recommended_resume_free_mb": recommended,
            "preferred_24h_free_mb": preferred,
            "max_local_collector_usage_mb": MAX_LOCAL_COLLECTOR_USAGE_MB,
            "local_stream_collector_usage_mb": local_usage_mb,
            "storage_mode": storage_mode,
            "low_disk_operator_override": override,
        }
    if not blockers and free_output < recommended and not override:
        blockers = [f"local_disk_below_recommended_resume:{free_output}<{recommended}"]

    dry_run = budget_dry_run
    free_mb = safe_int(before.get("free_mb_output"))
    target_mb = required if free_mb < required else recommended
    needed_bytes = max(0, (target_mb - free_mb) * 1024 * 1024)
    safe_delete_bytes = safe_int(dry_run.get("safe_delete_bytes"), safe_int(dry_run.get("safe_delete_bytes_available")))
    if dry_run.get("returncode") != 0 or safe_delete_bytes < needed_bytes:
        return {
            "ok": False,
            "preflight_before": before,
            "preflight_after": before,
            "gc_ran": True,
            "gc_dry_run": dry_run,
            "blockers": blockers + ["insufficient_safe_verified_bulk_artifacts_for_gc"],
            "hard_min_free_mb": required,
            "recommended_resume_free_mb": recommended,
            "preferred_24h_free_mb": preferred,
            "max_local_collector_usage_mb": MAX_LOCAL_COLLECTOR_USAGE_MB,
            "local_stream_collector_usage_mb": local_usage_mb,
            "storage_mode": storage_mode,
            "low_disk_operator_override": override,
        }

    apply_result = budget_apply or run_storage_gc(apply=True, min_free_mb=max(required, recommended))
    after = run_local_r2_primary_preflight(control_env)
    after_blockers = local_disk_blockers(after)
    after_free = safe_int(after.get("free_mb_output"))
    if after.get("returncode") == 0 and after_free < recommended and not override:
        after_blockers.append(f"local_disk_below_recommended_resume:{after_free}<{recommended}")
    after_usage_mb = safe_int(apply_result.get("local_stream_collector_mb_after"), local_usage_mb)
    if after_usage_mb > MAX_LOCAL_COLLECTOR_USAGE_MB:
        after_blockers.append(f"local_stream_collector_usage_above_budget:{after_usage_mb}>{MAX_LOCAL_COLLECTOR_USAGE_MB}")
    return {
        "ok": not after_blockers and after.get("returncode") == 0,
        "preflight_before": before,
        "preflight_after": after,
        "gc_ran": True,
        "gc_dry_run": dry_run,
        "gc_apply": apply_result,
        "blockers": after_blockers,
        "hard_min_free_mb": required,
        "recommended_resume_free_mb": recommended,
        "preferred_24h_free_mb": preferred,
        "max_local_collector_usage_mb": MAX_LOCAL_COLLECTOR_USAGE_MB,
        "local_stream_collector_usage_mb": after_usage_mb,
        "storage_mode": storage_mode,
        "low_disk_operator_override": override,
    }


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
    storage = status.get("last_storage_recovery") or {}
    preflight = storage.get("preflight_after") or storage.get("preflight_before") or {}
    storage_mode = str(storage.get("storage_mode") or preflight.get("storage_mode") or DEFAULT_STORAGE_MODE)
    hard_min = storage.get("hard_min_free_mb", preflight.get("required_mb", ""))
    recommended = storage.get("recommended_resume_free_mb", storage_recommended_resume_mb(storage_mode))
    preferred = storage.get("preferred_24h_free_mb", storage_preferred_24h_mb(storage_mode))

    def first_present(*values: Any) -> Any:
        for value in values:
            if value is not None and value != "":
                return value
        return ""

    local_usage_mb = first_present(
        status.get("local_collector_usage_mb"),
        storage.get("local_stream_collector_usage_mb"),
    )
    max_local_usage_mb = first_present(
        status.get("max_local_collector_usage_mb"),
        storage.get("max_local_collector_usage_mb"),
        MAX_LOCAL_COLLECTOR_USAGE_MB,
    )
    spool_current = first_present(
        status.get("local_spool_bytes_current"),
        preflight.get("local_spool_bytes_current"),
    )
    spool_peak = first_present(
        status.get("local_spool_bytes_peak"),
        preflight.get("local_spool_bytes_peak"),
    )
    spool_limit = first_present(
        status.get("local_spool_bytes_limit"),
        preflight.get("local_spool_bytes_limit"),
    )
    disk_free_mb = first_present(
        status.get("local_disk_free_mb"),
        preflight.get("local_disk_free_mb"),
        preflight.get("free_mb_output"),
    )
    output_root = first_present(status.get("configured_output_root"), preflight.get("configured_output_root"))
    spool_root = first_present(status.get("configured_spool_root"), preflight.get("configured_spool_root"))
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
        f"- storage_mode: `{storage_mode}`",
        f"- local_free_mb_output: `{preflight.get('free_mb_output', '')}`",
        f"- local_free_mb_spool: `{preflight.get('free_mb_spool', '')}`",
        f"- hard_min_free_mb: `{hard_min}`",
        f"- recommended_resume_free_mb: `{recommended}`",
        f"- preferred_24h_free_mb: `{preferred}`",
        f"- local_collector_usage_mb: `{local_usage_mb}`",
        f"- max_local_collector_usage_mb: `{max_local_usage_mb}`",
        f"- local_spool_bytes_current: `{spool_current}`",
        f"- local_spool_bytes_peak: `{spool_peak}`",
        f"- local_spool_bytes_limit: `{spool_limit}`",
        f"- local_disk_free_mb: `{disk_free_mb}`",
        f"- r2_streaming_spool_mb: `{R2_STREAMING_SPOOL_MB if is_r2_streaming_storage_mode(storage_mode) else ''}`",
        f"- r2_streaming_chunk_mb: `{R2_STREAMING_CHUNK_MB if is_r2_streaming_storage_mode(storage_mode) else ''}`",
        f"- r2_streaming_uploaded_chunks: `{status.get('r2_streaming_uploaded_chunks', '')}`",
        f"- r2_streaming_verified_chunks: `{status.get('r2_streaming_verified_chunks', '')}`",
        f"- r2_streaming_deleted_local_chunks: `{status.get('r2_streaming_deleted_local_chunks', '')}`",
        f"- r2_streaming_unverified_chunks: `{status.get('r2_streaming_unverified_chunks', '')}`",
        f"- r2_streaming_retry_count: `{status.get('r2_streaming_retry_count', '')}`",
        f"- r2_streaming_upload_timeout_count: `{status.get('r2_streaming_upload_timeout_count', '')}`",
        f"- r2_streaming_backpressure_detected: `{status.get('r2_streaming_backpressure_detected', '')}`",
        f"- configured_output_root: `{output_root}`",
        f"- configured_spool_root: `{spool_root}`",
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
    storage_mode = DEFAULT_STORAGE_MODE
    payload = {
        "schema_version": "phase107k.background_24h_collection_justification.v1",
        "written_at_utc": utc_stamp(),
        "collection_allowed": True,
        "reason": COLLECTION_REASON,
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
        "storage_mode": storage_mode,
        "r2_streaming_spool_mb": R2_STREAMING_SPOOL_MB if is_r2_streaming_storage_mode(storage_mode) else None,
        "r2_streaming_min_free_mb": R2_STREAMING_MIN_FREE_MB if is_r2_streaming_storage_mode(storage_mode) else None,
        "r2_streaming_chunk_mb": R2_STREAMING_CHUNK_MB if is_r2_streaming_storage_mode(storage_mode) else None,
        "retention_mode": "keep-manifests-after-verified-r2",
        "generic_collection_allowed": False,
        "unsliced_collection_allowed": False,
        "v1_controls_promotion": True,
        "early_burst_review_artifacts_enabled": True,
        "launch_caps_remain_blocked": True,
        "replay_allowed": False,
        "formal_backtesting_allowed": False,
        "threshold_tuning_allowed": False,
        "paper_trading_enabled": False,
        "live_trading_enabled": False,
        "wallet_execution_enabled": False,
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
        f"- storage_mode: `{storage_mode}`\n"
        f"- r2_streaming_min_free_mb: `{payload.get('r2_streaming_min_free_mb') or ''}`\n"
        f"- r2_streaming_spool_mb: `{payload.get('r2_streaming_spool_mb') or ''}`\n"
        f"- generic_collection_allowed: `false`\n"
        f"- replay/backtesting/tuning/paper/live/wallet: `blocked`\n"
        f"- launch_caps: `blocked`\n",
        encoding="utf-8",
    )
    return payload


def write_batch_gate(batch_index: int, expected_slices: int) -> pathlib.Path:
    storage_mode = DEFAULT_STORAGE_MODE
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
        "storage_mode": storage_mode,
        "r2_streaming_spool_mb": R2_STREAMING_SPOOL_MB if is_r2_streaming_storage_mode(storage_mode) else None,
        "r2_streaming_min_free_mb": R2_STREAMING_MIN_FREE_MB if is_r2_streaming_storage_mode(storage_mode) else None,
        "r2_streaming_chunk_mb": R2_STREAMING_CHUNK_MB if is_r2_streaming_storage_mode(storage_mode) else None,
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
    storage = ensure_local_storage_ready(control_env)
    deployed = verify_deployed_sha(control_env)
    inflight = local_inflight_processes()
    local = storage.get("preflight_after") or storage.get("preflight_before") or {}
    vps = status.get("vps_safety", {})
    storage_mode = str(storage.get("storage_mode") or local.get("storage_mode") or DEFAULT_STORAGE_MODE)
    hard_min = safe_int(storage.get("hard_min_free_mb"), safe_int(local.get("required_mb"), HARD_MIN_FREE_MB))
    recommended = safe_int(storage.get("recommended_resume_free_mb"), storage_recommended_resume_mb(storage_mode))
    blockers: list[str] = []
    if inflight:
        blockers.append("local_relay_or_supervisor_process_already_running")
    if preflight.get("classification") != "RELAY_CONTROL_CONFIG_PASS":
        blockers.append("relay_control_preflight_failed")
    if not storage.get("ok"):
        blockers.extend(str(item) for item in storage.get("blockers") or ["local_r2_primary_storage_not_ready"])
    free_output = safe_int(local.get("free_mb_output"))
    if free_output < hard_min:
        gate = "local_disk_below_r2_streaming_gate" if is_r2_streaming_storage_mode(storage_mode) else "local_disk_below_r2_primary_gate"
        blockers.append(f"{gate}:{free_output}<{hard_min}")
    elif free_output < recommended and not low_disk_override_enabled(control_env):
        blockers.append(f"local_disk_below_recommended_resume:{free_output}<{recommended}")
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
        "storage_recovery": storage,
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
        "max_local_collector_usage_mb": MAX_LOCAL_COLLECTOR_USAGE_MB,
        "local_collector_usage_mb": max([safe_int(r.get("local_collector_usage_mb")) for r in rows] or [0]),
        "local_spool_bytes_current": sum(safe_int(r.get("local_spool_bytes_current")) for r in rows),
        "local_spool_bytes_peak": max([safe_int(r.get("local_spool_bytes_peak")) for r in rows] or [0]),
        "local_spool_bytes_limit": max([safe_int(r.get("local_spool_bytes_limit")) for r in rows] or [0]),
        "local_disk_free_mb": min(
            [safe_int(r.get("local_disk_free_mb")) for r in rows if safe_int(r.get("local_disk_free_mb")) > 0]
            or [0]
        ),
        "r2_streaming_uploaded_chunks": sum(safe_int(r.get("r2_streaming_uploaded_chunks")) for r in rows),
        "r2_streaming_verified_chunks": sum(safe_int(r.get("r2_streaming_verified_chunks")) for r in rows),
        "r2_streaming_deleted_local_chunks": sum(safe_int(r.get("r2_streaming_deleted_local_chunks")) for r in rows),
        "r2_streaming_unverified_chunks": sum(safe_int(r.get("r2_streaming_unverified_chunks")) for r in rows),
        "r2_streaming_retry_count": sum(safe_int(r.get("r2_streaming_retry_count")) for r in rows),
        "r2_streaming_upload_timeout_count": sum(safe_int(r.get("r2_streaming_upload_timeout_count")) for r in rows),
        "r2_streaming_backpressure_detected": any(
            str(r.get("r2_streaming_backpressure_detected", "")).lower() == "true" for r in rows
        ),
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
        "storage_mode": row.get("storage_mode", DEFAULT_STORAGE_MODE),
        "local_collector_usage_mb": row.get("local_collector_usage_mb", ""),
        "max_local_collector_usage_mb": row.get("max_local_collector_usage_mb", MAX_LOCAL_COLLECTOR_USAGE_MB),
        "local_spool_bytes_current": row.get("local_spool_bytes_current", ""),
        "local_spool_bytes_peak": row.get("local_spool_bytes_peak", ""),
        "local_spool_bytes_limit": row.get("local_spool_bytes_limit", ""),
        "local_disk_free_mb": row.get("local_disk_free_mb", ""),
        "r2_streaming_uploaded_chunks": row.get("r2_streaming_uploaded_chunks", ""),
        "r2_streaming_verified_chunks": row.get("r2_streaming_verified_chunks", ""),
        "r2_streaming_deleted_local_chunks": row.get("r2_streaming_deleted_local_chunks", ""),
        "r2_streaming_unverified_chunks": row.get("r2_streaming_unverified_chunks", ""),
        "r2_streaming_retry_count": row.get("r2_streaming_retry_count", ""),
        "r2_streaming_upload_timeout_count": row.get("r2_streaming_upload_timeout_count", ""),
        "r2_streaming_backpressure_detected": row.get("r2_streaming_backpressure_detected", ""),
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
        f"- storage_mode: `{DEFAULT_STORAGE_MODE}`\n"
        f"- max_local_collector_usage_mb: `{MAX_LOCAL_COLLECTOR_USAGE_MB}`\n"
        f"- max_local_collector_usage_seen_mb: `{max([safe_int(r.get('local_collector_usage_mb')) for r in rows] or [0])}`\n"
        f"- local_spool_bytes_peak: `{max([safe_int(r.get('local_spool_bytes_peak')) for r in rows] or [0])}`\n"
        f"- local_spool_bytes_limit: `{max([safe_int(r.get('local_spool_bytes_limit')) for r in rows] or [0])}`\n"
        f"- r2_streaming_uploaded_chunks: `{sum(safe_int(r.get('r2_streaming_uploaded_chunks')) for r in rows)}`\n"
        f"- r2_streaming_verified_chunks: `{sum(safe_int(r.get('r2_streaming_verified_chunks')) for r in rows)}`\n"
        f"- r2_streaming_deleted_local_chunks: `{sum(safe_int(r.get('r2_streaming_deleted_local_chunks')) for r in rows)}`\n"
        f"- r2_streaming_unverified_chunks: `{sum(safe_int(r.get('r2_streaming_unverified_chunks')) for r in rows)}`\n"
        f"- r2_streaming_retry_count: `{sum(safe_int(r.get('r2_streaming_retry_count')) for r in rows)}`\n"
        f"- r2_streaming_upload_timeout_count: `{sum(safe_int(r.get('r2_streaming_upload_timeout_count')) for r in rows)}`\n"
        f"- r2_streaming_backpressure_detected: `{str(any(str(r.get('r2_streaming_backpressure_detected', '')).lower() == 'true' for r in rows)).lower()}`\n"
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
        f"- storage_mode: `{DEFAULT_STORAGE_MODE}`\n"
        f"- max_local_collector_usage_mb: `{MAX_LOCAL_COLLECTOR_USAGE_MB}`\n"
        f"- max_local_collector_usage_seen_mb: `{max([safe_int(r.get('local_collector_usage_mb')) for r in rows] or [0])}`\n"
        f"- local_spool_bytes_peak: `{max([safe_int(r.get('local_spool_bytes_peak')) for r in rows] or [0])}`\n"
        f"- local_spool_bytes_limit: `{max([safe_int(r.get('local_spool_bytes_limit')) for r in rows] or [0])}`\n"
        f"- r2_streaming_uploaded_chunks: `{sum(safe_int(r.get('r2_streaming_uploaded_chunks')) for r in rows)}`\n"
        f"- r2_streaming_verified_chunks: `{sum(safe_int(r.get('r2_streaming_verified_chunks')) for r in rows)}`\n"
        f"- r2_streaming_deleted_local_chunks: `{sum(safe_int(r.get('r2_streaming_deleted_local_chunks')) for r in rows)}`\n"
        f"- r2_streaming_unverified_chunks: `{sum(safe_int(r.get('r2_streaming_unverified_chunks')) for r in rows)}`\n"
        f"- r2_streaming_retry_count: `{sum(safe_int(r.get('r2_streaming_retry_count')) for r in rows)}`\n"
        f"- r2_streaming_upload_timeout_count: `{sum(safe_int(r.get('r2_streaming_upload_timeout_count')) for r in rows)}`\n"
        f"- r2_streaming_backpressure_detected: `{str(any(str(r.get('r2_streaming_backpressure_detected', '')).lower() == 'true' for r in rows)).lower()}`\n"
        f"- blockers_encountered: `{sum(1 for r in rows if r.get('blocker_if_any'))}`\n"
        f"- more_collection_justified: `{str(high_positive < TARGET_HIGH_POSITIVE_MINTS and not blocker).lower()}`\n"
        f"- backtesting_remains_blocked: `true`\n"
        f"- replay_remains_blocked: `true`\n"
        f"- launch_caps_remain_blocked: `true`\n",
        encoding="utf-8",
    )


def write_resume_decision(payload: dict[str, Any]) -> None:
    path_json = STATUS_ROOT / "RESUME_DECISION.json"
    path_md = STATUS_ROOT / "RESUME_DECISION.md"
    path_json_v2 = STATUS_ROOT / "RESUME_DECISION_V2.json"
    path_md_v2 = STATUS_ROOT / "RESUME_DECISION_V2.md"
    write_json(path_json, payload)
    write_json(path_json_v2, payload)
    path_md.write_text(
        "# Background 24h Resume Decision\n\n"
        f"- written_at_utc: `{payload.get('written_at_utc')}`\n"
        f"- resume_allowed: `{str(payload.get('resume_allowed')).lower()}`\n"
        f"- reason: `{payload.get('reason')}`\n"
        f"- completed_slices_preserved: `{payload.get('completed_slices_preserved')}`\n"
        f"- next_slice_index: `{payload.get('next_slice_index')}`\n"
        f"- local_preflight_ok: `{str(payload.get('local_preflight_ok')).lower()}`\n"
        f"- vps_safety_ok: `{str(payload.get('vps_safety_ok')).lower()}`\n"
        f"- blockers: `{', '.join(payload.get('blockers') or []) or 'none'}`\n"
        f"- replay/backtesting/tuning/paper/live/wallet: `blocked`\n"
        f"- launch_caps: `blocked`\n",
        encoding="utf-8",
    )
    path_md_v2.write_text(path_md.read_text(encoding="utf-8"), encoding="utf-8")


def previous_stop_was_local_disk_only(status_payload: dict[str, Any]) -> bool:
    if status_payload.get("classification") in {LOCAL_DISK_BLOCK_CLASSIFICATION, LOCAL_DISK_CAPACITY_BLOCK_CLASSIFICATION}:
        return True
    blocker = str(status_payload.get("blocker", ""))
    if is_local_disk_blocker(blocker):
        return True
    for log_name in ("worker.err", "worker.log"):
        path = STATUS_ROOT / log_name
        if path.exists():
            tail = path.read_text(encoding="utf-8", errors="replace")[-12000:]
            if any(prefix in tail for prefix in LOCAL_DISK_BLOCKER_PREFIXES):
                return True
    return False


def local_stream_collector_root() -> pathlib.Path:
    return REPO / "research_output" / "local_stream_collector"


def latest_unmirrored_local_run_dir(rows: list[dict[str, str]]) -> pathlib.Path | None:
    known_slice_ids = {row.get("slice_id", "") for row in rows}
    root = local_stream_collector_root()
    if not root.exists():
        return None
    candidates = [
        path
        for path in root.glob("background-24h-early-burst-*")
        if path.is_dir() and not path.name.endswith("-logs") and path.name not in known_slice_ids
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def recovered_provider_block_resume_status(rows: list[dict[str, str]], status_payload: dict[str, Any]) -> dict[str, Any]:
    if status_payload.get("classification") != "BACKGROUND_24H_COLLECTION_BLOCK_RELAY":
        return {"ok": False, "reason": "previous_stop_not_relay_block"}
    if str(status_payload.get("blocker", "")) != "supervisor_slice_failed":
        return {"ok": False, "reason": "previous_relay_blocker_not_supervisor_slice_failed"}
    run_dir = latest_unmirrored_local_run_dir(rows)
    if run_dir is None:
        return {"ok": False, "reason": "no_unmirrored_failed_run_dir"}

    proof = read_json(run_dir / "local_relay_dataset_proof_summary.json")
    collector_exit = read_json(run_dir / "local_collector_exit_status.json")
    r2_upload = read_json(run_dir / "r2_upload_result.json")
    retention = read_json(run_dir / "local_retention_summary.json")
    r2_streaming = read_json(run_dir / "r2_streaming_upload_manifest.json")
    service_exit = read_json(run_dir / "service_exit_status.json")
    blockers: list[str] = []
    if proof.get("classification") != "RELAY_LOCAL_DATASET_BLOCK_PROVIDER":
        blockers.append("failed_run_not_provider_block")
    if proof.get("provider_blocker_class") != "provider_reconnect_exhausted":
        blockers.append("provider_block_not_reconnect_exhausted")
    if safe_int(proof.get("attempted_launches")) != 0:
        blockers.append("provider_block_had_attempted_launches")
    if safe_int(proof.get("candidate_checkpoint_count")) != 0:
        blockers.append("provider_block_candidate_checkpoint_present")
    if safe_int(proof.get("replay_eligible_candidate_count")) != 0:
        blockers.append("provider_block_replay_candidate_present")
    for key in (
        "sequence_gap_count",
        "hash_mismatch_count",
        "malformed_frame_count",
        "receiver_backpressure_count",
        "receiver_unavailable_count",
    ):
        if safe_int(proof.get(key, collector_exit.get(key))) != 0:
            blockers.append(f"provider_block_{key}_nonzero")
    if r2_upload.get("verified") is not True or r2_upload.get("failed_files"):
        blockers.append("provider_block_r2_not_verified")
    if retention.get("ok") is not True:
        blockers.append("provider_block_retention_not_ok")
    if r2_streaming and safe_int(r2_streaming.get("unverified_chunks")) != 0:
        blockers.append("provider_block_r2_streaming_unverified_chunks")
    if service_exit.get("service_exit_reason") != "local_relay_collector_completed":
        blockers.append("provider_block_service_exit_not_clean")
    return {
        "ok": not blockers,
        "reason": "recovered_provider_reconnect_exhausted_zero_attempt_slice" if not blockers else "provider_block_recovery_failed",
        "blockers": blockers,
        "run_dir": str(run_dir),
        "run_id": run_dir.name,
    }


def resume_decision(control_env: pathlib.Path) -> dict[str, Any]:
    rows = completed_slice_rows()
    status_payload = read_json(STATUS_PATH)
    active = pid_running(safe_int(status_payload.get("pid"), 0) or None)
    provider_recovery = recovered_provider_block_resume_status(rows, status_payload)
    row_blockers = [r.get("blocker_if_any", "") for r in rows if r.get("blocker_if_any")]
    r2_failures = sum(safe_int(r.get("r2_failed_files")) for r in rows)
    artifact_failures = sum(1 for r in rows if str(r.get("artifact_consistency_ok")).lower() == "false")
    candidate = sum(safe_int(r.get("candidate_checkpoint_count")) for r in rows)
    replay = sum(safe_int(r.get("replay_eligible_candidate_count")) for r in rows)
    storage = ensure_local_storage_ready(control_env)
    preflight = storage.get("preflight_after") or storage.get("preflight_before") or {}
    preflight_disk_blockers = local_disk_blockers(preflight)
    supervisor = supervisor_status(control_env)
    vps = supervisor.get("vps_safety", {})
    vps_ok = (
        safe_int(vps.get("forbidden_recent")) == 0
        and safe_int(vps.get("relay_running")) == 0
        and vps.get("material_candidate_service") == "inactive"
        and vps.get("material_hunter_service") == "inactive"
    )
    blockers: list[str] = []
    if active:
        blockers.append("collector_process_still_active")
    if not previous_stop_was_local_disk_only(status_payload) and not provider_recovery.get("ok"):
        blockers.append("previous_stop_not_proven_local_disk_or_recovered_provider_only")
    if row_blockers:
        blockers.append("prior_slice_blockers_present")
    if r2_failures:
        blockers.append("prior_r2_failures_present")
    if artifact_failures:
        blockers.append("prior_artifact_failures_present")
    if candidate or replay:
        blockers.append("candidate_or_replay_trigger_present")
    free_mb = safe_int(preflight.get("free_mb_output"))
    override = low_disk_override_enabled(control_env)
    storage_mode = str(storage.get("storage_mode") or preflight.get("storage_mode") or DEFAULT_STORAGE_MODE)
    hard_min = safe_int(storage.get("hard_min_free_mb"), safe_int(preflight.get("required_mb"), HARD_MIN_FREE_MB))
    recommended = safe_int(storage.get("recommended_resume_free_mb"), storage_recommended_resume_mb(storage_mode))
    preferred = safe_int(storage.get("preferred_24h_free_mb"), storage_preferred_24h_mb(storage_mode))
    if not storage.get("ok") or preflight.get("returncode") != 0 or preflight_disk_blockers:
        blockers.append("local_r2_primary_preflight_not_passing_or_capacity_low")
    if free_mb < hard_min:
        blockers.append(f"local_disk_below_hard_min:{free_mb}<{hard_min}")
    elif free_mb < recommended and not override:
        blockers.append(f"local_disk_below_recommended_resume:{free_mb}<{recommended}")
    if not vps_ok:
        blockers.append("vps_safety_not_clean")
    allowed = not blockers
    return {
        "schema_version": "phase107k.background_24h_resume_decision.v2",
        "written_at_utc": utc_stamp(),
        "resume_allowed": allowed,
        "reason": (
            provider_recovery.get("reason")
            if allowed and provider_recovery.get("ok")
            else "local_disk_capacity_recovered"
            if allowed
            else "local_disk_capacity_block"
        ),
        "next_action": "resume_next_slice" if allowed else "free_local_disk_or_configure_larger_output_root",
        "completed_slices_preserved": len(rows),
        "next_slice_index": len(rows) + 1,
        "previous_stop_was_local_disk_only": previous_stop_was_local_disk_only(status_payload),
        "previous_stop_was_recovered_provider_only": provider_recovery.get("ok") is True,
        "provider_block_recovery": provider_recovery,
        "local_preflight_ok": preflight.get("returncode") == 0 and not preflight_disk_blockers,
        "local_storage_ready": storage.get("ok") is True,
        "local_storage_recovery": storage,
        "local_preflight": preflight,
        "free_mb_output": free_mb,
        "storage_mode": storage_mode,
        "hard_min_free_mb": hard_min,
        "recommended_resume_free_mb": recommended,
        "preferred_24h_free_mb": preferred,
        "low_disk_operator_override": override,
        "larger_output_root_configured": bool(preflight.get("configured_output_root")),
        "larger_spool_root_configured": bool(preflight.get("configured_spool_root")),
        "configured_output_root": preflight.get("configured_output_root", ""),
        "configured_spool_root": preflight.get("configured_spool_root", ""),
        "vps_safety_ok": vps_ok,
        "vps_safety": vps,
        "r2_failures": r2_failures,
        "artifact_consistency_failures": artifact_failures,
        "candidate_checkpoint_count": candidate,
        "replay_eligible_candidate_count": replay,
        "blockers": blockers,
        "collection_allowed_after_resume_decision": allowed,
        "replay_allowed": False,
        "formal_backtesting_allowed": False,
        "threshold_tuning_allowed": False,
        "paper_trading_enabled": False,
        "live_trading_enabled": False,
        "wallet_execution_enabled": False,
        "launch_caps_remain_blocked": True,
    }


def stop_classification(status: dict[str, Any], started_at: float) -> tuple[bool, str, str]:
    rows = completed_slice_rows()
    high_positive = current_high_positive_count()
    if status.get("blocker"):
        if status.get("classification") == LOCAL_DISK_CAPACITY_BLOCK_CLASSIFICATION:
            return True, LOCAL_DISK_CAPACITY_BLOCK_CLASSIFICATION, str(status.get("blocker"))
        if is_local_disk_blocker(str(status.get("blocker"))) or status.get("classification") == LOCAL_DISK_BLOCK_CLASSIFICATION:
            return True, LOCAL_DISK_BLOCK_CLASSIFICATION, str(status.get("blocker"))
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
    storage = ensure_local_storage_ready(control_env)
    if not storage.get("ok"):
        blocker = ",".join(str(item) for item in storage.get("blockers", [])) or "local_disk_preflight_failed"
        classification = (
            LOCAL_DISK_CAPACITY_BLOCK_CLASSIFICATION
            if "recommended" in blocker or "insufficient_safe_verified_bulk_artifacts_for_gc" in blocker
            else LOCAL_DISK_BLOCK_CLASSIFICATION
        )
        status = aggregate_status(read_json(STATUS_PATH))
        status.update(
            {
                "state": "blocked",
                "classification": classification,
                "blocker": blocker,
                "last_storage_recovery": storage,
                "current_batch_index": batch_index,
            }
        )
        write_json(STATUS_PATH, status)
        write_live_summary(status)
        append_event({"event": "local_disk_block", "batch_index": batch_index, "blocker": blocker})
        return 3
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
        "--storage-mode",
        DEFAULT_STORAGE_MODE,
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
    if is_r2_streaming_storage_mode(DEFAULT_STORAGE_MODE):
        cmd.extend(["--r2-streaming-spool-mb", str(R2_STREAMING_SPOOL_MB)])
        cmd.extend(["--r2-streaming-min-free-mb", str(R2_STREAMING_MIN_FREE_MB)])
        cmd.extend(["--r2-streaming-chunk-mb", str(R2_STREAMING_CHUNK_MB)])
    env = merged_env(control_env)
    output_root = configured_output_root(env)
    if output_root:
        cmd.extend(["--output-root", str(output_root)])
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
            "blocker": "",
            "stop_reason": "",
            "last_slice_returncode": 0,
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
            classification = (
                status.get("classification")
                if status.get("classification") in {LOCAL_DISK_BLOCK_CLASSIFICATION, LOCAL_DISK_CAPACITY_BLOCK_CLASSIFICATION}
                else "BACKGROUND_24H_COLLECTION_BLOCK_RELAY"
            )
            blocker = status.get("blocker") if classification == LOCAL_DISK_BLOCK_CLASSIFICATION else "supervisor_slice_failed"
            if classification == LOCAL_DISK_CAPACITY_BLOCK_CLASSIFICATION:
                blocker = status.get("blocker")
            status.update(
                {
                    "state": "blocked",
                    "classification": classification,
                    "blocker": blocker,
                    "pid": None,
                }
            )
            write_json(STATUS_PATH, status)
            write_live_summary(status)
            write_final_report(classification, blocker)
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
            "blocker": "",
            "stop_reason": "",
            "last_slice_returncode": 0,
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
    if not payload["process_alive"] and previous_stop_was_local_disk_only(payload):
        payload["state"] = "blocked"
        payload["classification"] = LOCAL_DISK_BLOCK_CLASSIFICATION
        payload["blocker"] = "local_r2_primary_disk_preflight_block"
    write_json(STATUS_PATH, payload)
    write_live_summary(payload)
    print(json.dumps(payload, sort_keys=True))
    return 0


def resume(args: argparse.Namespace) -> int:
    payload = resume_decision(args.control_env)
    write_resume_decision(payload)
    status_payload = aggregate_status(read_json(STATUS_PATH))
    status_payload.update(
        {
            "state": "ready_to_resume" if payload.get("resume_allowed") else "blocked",
            "classification": "BACKGROUND_24H_COLLECTION_RESUME_ALLOWED"
            if payload.get("resume_allowed")
            else LOCAL_DISK_CAPACITY_BLOCK_CLASSIFICATION,
            "blocker": "" if payload.get("resume_allowed") else ",".join(payload.get("blockers") or []),
            "last_resume_decision": payload,
            "last_storage_recovery": payload.get("local_storage_recovery", {}),
            "pid": None,
        }
    )
    write_json(STATUS_PATH, status_payload)
    write_live_summary(status_payload)
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload.get("resume_allowed") else 2


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
    parser.add_argument("command", choices=["start", "status", "stop", "recover", "resume", "summarize", "worker"])
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
    if args.command == "resume":
        return resume(args)
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
