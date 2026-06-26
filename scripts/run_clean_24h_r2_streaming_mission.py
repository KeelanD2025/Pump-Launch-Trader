#!/usr/bin/env python3
"""Mission runner for the clean 24h R2-streaming buy-strategy dataset.

This runner is intentionally boring and explicit. It does not implement a new
collection architecture; it owns the long mission loop and delegates each slice
to the committed background/slice supervisors. Its job is to prevent ad-hoc
terminal orchestration from becoming the source of truth.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import pathlib
import shutil
import subprocess
import sys
import time
import zipfile
from typing import Any


REPO = pathlib.Path(__file__).resolve().parents[1]
MISSION_ROOT = (
    REPO
    / "research_output"
    / "trading_strategy_pipeline"
    / "clean_24h_r2_streaming_mission"
)
STATUS_PATH = MISSION_ROOT / "status.json"
LIVE_SUMMARY_PATH = MISSION_ROOT / "live_summary.md"
EVENTS_PATH = MISSION_ROOT / "events.ndjson"
SLICE_SUMMARIES_PATH = MISSION_ROOT / "slice_summaries.csv"
REVIEW_QUEUE_PATH = MISSION_ROOT / "review_queue.csv"
RECOVERY_ACTIONS_PATH = MISSION_ROOT / "recovery_actions.ndjson"
BLOCKER_HISTORY_PATH = MISSION_ROOT / "blocker_history.csv"
BATCH_SUMMARY_DIR = MISSION_ROOT / "batch_summaries"
BACKGROUND_SCRIPT = REPO / "scripts" / "run_background_24h_collector.py"
DEFAULT_CONTROL_ENV = REPO / ".codex_runtime_env" / "relay_control.env"
SUCCESS_CLASSIFICATION = "CLEAN_24H_R2_STREAMING_BUY_STRATEGY_DATASET_PASS"
REVIEW_CLASSIFICATION = "CANDIDATE_REVIEW_TRIGGERED_REPLAY_STILL_BLOCKED"
EXTERNAL_BLOCKER = "TRUE_EXTERNAL_BLOCKER_UNRECOVERABLE"
RUNNING_CLASSIFICATION = "CLEAN_24H_R2_STREAMING_MISSION_RUNNING"
TARGET_SUCCESSFUL_SLICES = 96
POLL_SECONDS = 60
MAX_RECOVERY_ATTEMPTS_PER_BLOCKER = 8
MISSION_ENV = {
    "PUMP_BACKGROUND_24H_STATUS_ROOT": str(MISSION_ROOT),
    "PUMP_BACKGROUND_24H_JUSTIFICATION_BASENAME": "CLEAN_24H_R2_STREAMING_COLLECTION_JUSTIFICATION",
    "PUMP_BACKGROUND_24H_COLLECTION_REASON": "targeted_early_burst_sample_collection",
    "PUMP_BACKGROUND_24H_TARGET_GATE": "BACKGROUND_24H_R2_STREAMING_COLLECTION",
    "PUMP_BACKGROUND_24H_STORAGE_MODE": "r2-streaming",
    "PUMP_R2_STREAMING_MIN_FREE_MB": "4096",
    "PUMP_R2_STREAMING_SPOOL_MB": "2048",
    "PUMP_R2_STREAMING_CHUNK_MB": "32",
    "PUMP_MAX_LOCAL_COLLECTOR_USAGE_MB": "5000",
}
FORBIDDEN_PROCESS_PATTERNS = (
    "local-stream-collector",
    "vps-stream-relay",
    "run_relay_r2_primary_batch.py",
    "run_background_24h_collector.py",
)
FINAL_EXPORT_FILES = (
    "CLEAN_24H_R2_STREAMING_DATASET_REPORT.md",
    "CLEAN_24H_R2_STREAMING_DATASET_FINAL_DECISION.json",
    "strategy_dataset_manifest.json",
    "slice_summaries.csv",
    "review_queue.csv",
    "blocker_history.csv",
    "recovery_actions.ndjson",
    "BUY_STRATEGY_DATASET_READINESS_FROM_24H.md",
    "STRATEGY_PLAYBOOK_V1_FORWARD_EVIDENCE_REPORT.md",
    "live_summary.md",
    "status.json",
)


class MissionError(RuntimeError):
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


def append_ndjson(path: pathlib.Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"ts": utc_stamp(), **payload}, sort_keys=True) + "\n")


def read_csv_rows(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value)))
    except (TypeError, ValueError):
        return default


def boolish(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def mission_env(control_env: pathlib.Path) -> dict[str, str]:
    env = os.environ.copy()
    env.update(MISSION_ENV)
    env["PUMP_RELAY_CONTROL_ENV"] = str(control_env)
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
        raise MissionError(
            f"command failed rc={proc.returncode}: {' '.join(cmd)}\n"
            f"stdout={proc.stdout[-4000:]}\nstderr={proc.stderr[-4000:]}"
        )
    return proc


def run_background(command: str, control_env: pathlib.Path, *, timeout: int | None = None) -> dict[str, Any]:
    proc = run_capture(
        [
            sys.executable,
            str(BACKGROUND_SCRIPT),
            command,
            "--control-env",
            str(control_env),
        ],
        env=mission_env(control_env),
        timeout=timeout,
    )
    payload: dict[str, Any] = {
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
    }
    stripped = proc.stdout.strip()
    if stripped:
        try:
            parsed = json.loads(stripped.splitlines()[-1])
            if isinstance(parsed, dict):
                payload.update(parsed)
        except json.JSONDecodeError:
            payload["parse_error"] = stripped[-1000:]
    return payload


def active_forbidden_processes() -> list[str]:
    proc = run_capture(["pgrep", "-af", "|".join(FORBIDDEN_PROCESS_PATTERNS)], check=False)
    current_pid = str(os.getpid())
    rows = []
    for raw in proc.stdout.splitlines():
        if current_pid in raw:
            continue
        if "run_clean_24h_r2_streaming_mission.py" in raw:
            continue
        if "pgrep -af" in raw:
            continue
        rows.append(raw)
    return rows


def git_dirty_paths() -> list[str]:
    proc = run_capture(["git", "status", "--short"], check=False)
    return [line for line in proc.stdout.splitlines() if line.strip()]


def write_live_summary(status: dict[str, Any]) -> None:
    MISSION_ROOT.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Clean 24h R2-Streaming Mission",
        "",
        f"- updated_at_utc: `{utc_stamp()}`",
        f"- state: `{status.get('state', '')}`",
        f"- classification: `{status.get('classification', '')}`",
        f"- pid: `{status.get('pid', '')}`",
        f"- process_alive: `{str(status.get('process_alive', False)).lower()}`",
        f"- successful_slices: `{status.get('successful_slices', 0)}`",
        f"- attempted_slices: `{status.get('attempted_slices', 0)}`",
        f"- target_successful_slices: `{TARGET_SUCCESSFUL_SLICES}`",
        f"- candidate_checkpoint_count: `{status.get('candidate_checkpoint_count', 0)}`",
        f"- replay_eligible_candidate_count: `{status.get('replay_eligible_candidate_count', 0)}`",
        f"- blocker: `{status.get('blocker', '')}`",
        f"- storage_mode: `r2-streaming`",
        f"- r2_streaming_unverified_chunks: `{status.get('r2_streaming_unverified_chunks', 0)}`",
        f"- replay/backtesting/tuning/paper/live/wallet: `blocked`",
        f"- launch_caps: `blocked`",
        "",
        "## Paths",
        f"- status_json: `{STATUS_PATH.relative_to(REPO)}`",
        f"- slice_summaries_csv: `{SLICE_SUMMARIES_PATH.relative_to(REPO)}`",
        f"- recovery_actions_ndjson: `{RECOVERY_ACTIONS_PATH.relative_to(REPO)}`",
        f"- blocker_history_csv: `{BLOCKER_HISTORY_PATH.relative_to(REPO)}`",
    ]
    LIVE_SUMMARY_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def aggregate_status(base: dict[str, Any] | None = None) -> dict[str, Any]:
    rows = read_csv_rows(SLICE_SUMMARIES_PATH)
    candidate = sum(safe_int(row.get("candidate_checkpoint_count")) for row in rows)
    replay = sum(safe_int(row.get("replay_eligible_candidate_count")) for row in rows)
    blocker_rows = [row for row in rows if row.get("blocker_if_any")]
    successful = sum(1 for row in rows if is_successful_slice(row))
    payload = {
        **(base or read_json(STATUS_PATH) or {}),
        "updated_at_utc": utc_stamp(),
        "attempted_slices": len(rows),
        "successful_slices": successful,
        "target_successful_slices": TARGET_SUCCESSFUL_SLICES,
        "candidate_checkpoint_count": candidate,
        "replay_eligible_candidate_count": replay,
        "blocker_rows": len(blocker_rows),
        "all_launches_indexed": sum(safe_int(row.get("all_launches_indexed")) for row in rows),
        "cheap_followup_rows": sum(safe_int(row.get("cheap_followup_rows")) for row in rows),
        "rich_tracked_launches": sum(safe_int(row.get("rich_tracked_launches")) for row in rows),
        "r2_streaming_uploaded_chunks": sum(safe_int(row.get("r2_streaming_uploaded_chunks")) for row in rows),
        "r2_streaming_verified_chunks": sum(safe_int(row.get("r2_streaming_verified_chunks")) for row in rows),
        "r2_streaming_deleted_local_chunks": sum(safe_int(row.get("r2_streaming_deleted_local_chunks")) for row in rows),
        "r2_streaming_unverified_chunks": sum(safe_int(row.get("r2_streaming_unverified_chunks")) for row in rows),
        "replay_allowed": False,
        "formal_backtesting_allowed": False,
        "threshold_tuning_allowed": False,
        "paper_trading_enabled": False,
        "live_trading_enabled": False,
        "wallet_execution_enabled": False,
        "launch_caps_remain_blocked": True,
    }
    pid = safe_int(payload.get("pid"), 0) or None
    payload["process_alive"] = pid_running(pid)
    return payload


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


def is_successful_slice(row: dict[str, Any]) -> bool:
    classification = str(row.get("classification", ""))
    if row.get("blocker_if_any"):
        return False
    if safe_int(row.get("r2_failed_files")) != 0:
        return False
    if safe_int(row.get("r2_streaming_unverified_chunks")) != 0:
        return False
    if str(row.get("artifact_consistency_ok", "")).lower() not in {"true", "1", "yes"}:
        return False
    return classification in {
        "RELAY_COLLECTION_PASS_COUNTED_NO_CANDIDATE",
        "RELAY_COLLECTION_PASS_PROVIDER_GAP_CONTINUED",
        "RELAY_LOCAL_DATASET_PASS",
        "RELAY_PROVIDER_GAP_CONTINUATION_PASS",
        "RELAY_LOCAL_DATASET_PASS_NO_SIGNAL",
        "RELAY_LOCAL_DATASET_PASS_EMPTY_NO_ATTEMPTS",
        "RELAY_COLLECTION_PASS_NO_SIGNAL",
        "RELAY_COLLECTION_PASS_EMPTY_NO_ATTEMPTS",
    }


def preflight(control_env: pathlib.Path, *, allow_dirty: bool = False) -> dict[str, Any]:
    dirty = git_dirty_paths()
    active = active_forbidden_processes()
    relay_preflight = run_capture(
        [
            sys.executable,
            "scripts/relay_control_preflight.py",
            "--control-env",
            str(control_env),
            "--json",
        ],
        timeout=120,
    )
    r2_preflight = run_capture(
        [
            "./scripts/local_stream_collector_preflight.sh",
            "--storage-mode",
            "r2-streaming",
            "--mode",
            "collection",
            "--verify-r2-health-live",
        ],
        timeout=240,
    )
    blockers: list[str] = []
    if dirty and not allow_dirty:
        blockers.append("repo_dirty_commit_required")
    if active:
        blockers.append("active_project_processes_present")
    if relay_preflight.returncode != 0:
        blockers.append("relay_control_preflight_failed")
    if r2_preflight.returncode != 0:
        blockers.append("r2_streaming_preflight_failed")
    payload = {
        "schema_version": "phase107n.clean_24h_mission_preflight.v1",
        "generated_at_utc": utc_stamp(),
        "ok": not blockers,
        "blockers": blockers,
        "dirty_paths": dirty,
        "active_project_processes": active,
        "relay_control_preflight": {
            "returncode": relay_preflight.returncode,
            "stdout_tail": relay_preflight.stdout[-4000:],
            "stderr_tail": relay_preflight.stderr[-4000:],
        },
        "r2_streaming_preflight": {
            "returncode": r2_preflight.returncode,
            "stdout_tail": r2_preflight.stdout[-4000:],
            "stderr_tail": r2_preflight.stderr[-4000:],
        },
        "replay_allowed": False,
        "formal_backtesting_allowed": False,
        "threshold_tuning_allowed": False,
        "paper_trading_enabled": False,
        "live_trading_enabled": False,
        "wallet_execution_enabled": False,
        "launch_caps_remain_blocked": True,
    }
    write_json(MISSION_ROOT / "MISSION_PREFLIGHT.json", payload)
    return payload


def write_blocker_history(status: dict[str, Any]) -> None:
    fields = ["ts", "classification", "blocker", "successful_slices", "attempted_slices"]
    exists = BLOCKER_HISTORY_PATH.exists()
    BLOCKER_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with BLOCKER_HISTORY_PATH.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        if not exists:
            writer.writeheader()
        writer.writerow(
            {
                "ts": utc_stamp(),
                "classification": status.get("classification", ""),
                "blocker": status.get("blocker", ""),
                "successful_slices": status.get("successful_slices", 0),
                "attempted_slices": status.get("attempted_slices", 0),
            }
        )


def sync_batch_summaries() -> None:
    BATCH_SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    for src in MISSION_ROOT.glob("BATCH_*_SUMMARY.md"):
        dst = BATCH_SUMMARY_DIR / src.name
        if src.exists():
            shutil.copy2(src, dst)


def terminal_classification(status: dict[str, Any]) -> str:
    if safe_int(status.get("candidate_checkpoint_count")) > 0 or safe_int(status.get("replay_eligible_candidate_count")) > 0:
        return REVIEW_CLASSIFICATION
    if safe_int(status.get("successful_slices")) >= TARGET_SUCCESSFUL_SLICES:
        return SUCCESS_CLASSIFICATION
    return ""


def write_final_outputs(classification: str, blocker: str = "") -> pathlib.Path:
    status = aggregate_status(read_json(STATUS_PATH))
    rows = read_csv_rows(SLICE_SUMMARIES_PATH)
    sync_batch_summaries()
    review_zip_path = ""
    if classification == REVIEW_CLASSIFICATION:
        review_zip = MISSION_ROOT / f"candidate_review_pack_{compact_stamp()}.zip"
        with zipfile.ZipFile(review_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in (SLICE_SUMMARIES_PATH, REVIEW_QUEUE_PATH, STATUS_PATH, LIVE_SUMMARY_PATH):
                if path.exists():
                    archive.write(path, path.relative_to(MISSION_ROOT))
        review_zip_path = str(review_zip)

    decision = {
        "schema_version": "phase107n.clean_24h_dataset_final_decision.v1",
        "generated_at_utc": utc_stamp(),
        "classification": classification,
        "blocker": blocker,
        "candidate_review_pack_zip": review_zip_path,
        "successful_slices": status.get("successful_slices", 0),
        "attempted_slices": status.get("attempted_slices", 0),
        "candidate_checkpoint_count": status.get("candidate_checkpoint_count", 0),
        "replay_eligible_candidate_count": status.get("replay_eligible_candidate_count", 0),
        "r2_streaming_unverified_chunks": status.get("r2_streaming_unverified_chunks", 0),
        "strategy_research_dataset_ready": classification == SUCCESS_CLASSIFICATION,
        "replay_allowed": False,
        "formal_backtesting_allowed": False,
        "threshold_tuning_allowed": False,
        "paper_trading_enabled": False,
        "live_trading_enabled": False,
        "wallet_execution_enabled": False,
        "launch_caps_remain_blocked": True,
    }
    write_json(MISSION_ROOT / "CLEAN_24H_R2_STREAMING_DATASET_FINAL_DECISION.json", decision)
    manifest = {
        "schema_version": "phase107n.clean_24h_strategy_dataset_manifest.v1",
        "generated_at_utc": utc_stamp(),
        "mission_root": str(MISSION_ROOT),
        "slice_summaries_csv": str(SLICE_SUMMARIES_PATH),
        "review_queue_csv": str(REVIEW_QUEUE_PATH),
        "slice_count": len(rows),
        "successful_slices": status.get("successful_slices", 0),
        "all_launches_indexed": status.get("all_launches_indexed", 0),
        "cheap_followup_rows": status.get("cheap_followup_rows", 0),
        "rich_tracked_launches": status.get("rich_tracked_launches", 0),
        "r2_streaming_uploaded_chunks": status.get("r2_streaming_uploaded_chunks", 0),
        "r2_streaming_verified_chunks": status.get("r2_streaming_verified_chunks", 0),
        "r2_streaming_deleted_local_chunks": status.get("r2_streaming_deleted_local_chunks", 0),
        "r2_streaming_unverified_chunks": status.get("r2_streaming_unverified_chunks", 0),
    }
    write_json(MISSION_ROOT / "strategy_dataset_manifest.json", manifest)
    report = [
        "# Clean 24h R2-Streaming Dataset Report",
        "",
        f"- classification: `{classification}`",
        f"- blocker: `{blocker or 'none'}`",
        f"- total_successful_slices: `{status.get('successful_slices', 0)}`",
        f"- total_attempted_slices: `{status.get('attempted_slices', 0)}`",
        f"- all_launches_indexed: `{status.get('all_launches_indexed', 0)}`",
        f"- cheap_followup_rows: `{status.get('cheap_followup_rows', 0)}`",
        f"- rich_tracked_launches: `{status.get('rich_tracked_launches', 0)}`",
        f"- candidate_checkpoints: `{status.get('candidate_checkpoint_count', 0)}`",
        f"- replay_eligible_candidates: `{status.get('replay_eligible_candidate_count', 0)}`",
        f"- r2_chunks_uploaded_verified_deleted: `{status.get('r2_streaming_uploaded_chunks', 0)}` / `{status.get('r2_streaming_verified_chunks', 0)}` / `{status.get('r2_streaming_deleted_local_chunks', 0)}`",
        f"- r2_streaming_unverified_chunks: `{status.get('r2_streaming_unverified_chunks', 0)}`",
        "- replay_remains_blocked: `true`",
        "- formal_backtesting_remains_blocked: `true`",
        "- threshold_tuning_remains_blocked: `true`",
        "- paper_live_wallet_remain_blocked: `true`",
        "- launch_caps_remain_blocked: `true`",
    ]
    (MISSION_ROOT / "CLEAN_24H_R2_STREAMING_DATASET_REPORT.md").write_text(
        "\n".join(report) + "\n",
        encoding="utf-8",
    )
    readiness = [
        "# Buy Strategy Dataset Readiness From 24h",
        "",
        f"- strategy_research_dataset_ready: `{str(classification == SUCCESS_CLASSIFICATION).lower()}`",
        "- replay_ready: `false`",
        "- formal_backtesting_ready: `false`",
        "- threshold_tuning_ready: `false`",
        "- paper_live_wallet_ready: `false`",
    ]
    (MISSION_ROOT / "BUY_STRATEGY_DATASET_READINESS_FROM_24H.md").write_text(
        "\n".join(readiness) + "\n",
        encoding="utf-8",
    )
    evidence = [
        "# Strategy Playbook V1 Forward Evidence Report",
        "",
        "Frozen V1 rules were not modified by this mission runner.",
        f"- successful_slices: `{status.get('successful_slices', 0)}`",
        f"- early_burst_review_candidates: `{sum(safe_int(row.get('early_burst_review_candidate_count')) for row in rows)}`",
        f"- candidate_checkpoints: `{status.get('candidate_checkpoint_count', 0)}`",
        f"- replay_eligible_candidates: `{status.get('replay_eligible_candidate_count', 0)}`",
    ]
    (MISSION_ROOT / "STRATEGY_PLAYBOOK_V1_FORWARD_EVIDENCE_REPORT.md").write_text(
        "\n".join(evidence) + "\n",
        encoding="utf-8",
    )
    zip_path = MISSION_ROOT / "clean_24h_dataset_export.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative in FINAL_EXPORT_FILES:
            path = MISSION_ROOT / relative
            if path.exists():
                archive.write(path, path.relative_to(MISSION_ROOT))
        if BATCH_SUMMARY_DIR.exists():
            for path in sorted(BATCH_SUMMARY_DIR.glob("*.md")):
                archive.write(path, path.relative_to(MISSION_ROOT))
    return zip_path


def status_command(_: argparse.Namespace) -> int:
    background = run_background("status", DEFAULT_CONTROL_ENV, timeout=120)
    status = aggregate_status(background)
    terminal = terminal_classification(status)
    if terminal:
        status["classification"] = terminal
        status["state"] = "complete" if terminal == SUCCESS_CLASSIFICATION else "review_triggered"
        write_final_outputs(terminal)
    else:
        status.setdefault("classification", RUNNING_CLASSIFICATION)
    write_json(STATUS_PATH, status)
    write_live_summary(status)
    print(json.dumps(status, sort_keys=True))
    return 0


def start_command(args: argparse.Namespace) -> int:
    MISSION_ROOT.mkdir(parents=True, exist_ok=True)
    (REVIEW_QUEUE_PATH).touch(exist_ok=True)
    pre = preflight(args.control_env, allow_dirty=args.allow_dirty_source)
    if not pre.get("ok"):
        status = aggregate_status(
            {
                "state": "blocked",
                "classification": EXTERNAL_BLOCKER,
                "blocker": ",".join(pre.get("blockers") or []),
                "preflight": pre,
            }
        )
        write_json(STATUS_PATH, status)
        write_live_summary(status)
        print(json.dumps(status, sort_keys=True))
        return 2
    result = run_background("start", args.control_env, timeout=120)
    status = aggregate_status(result)
    status["state"] = "running" if result.get("returncode") == 0 else "blocked"
    status["classification"] = RUNNING_CLASSIFICATION if result.get("returncode") == 0 else EXTERNAL_BLOCKER
    write_json(STATUS_PATH, status)
    write_live_summary(status)
    append_ndjson(EVENTS_PATH, {"event": "mission_started", "background_returncode": result.get("returncode")})
    print(json.dumps(status, sort_keys=True))
    return 0 if result.get("returncode") == 0 else 2


def recover_and_resume(control_env: pathlib.Path) -> dict[str, Any]:
    recover = run_background("recover", control_env, timeout=120)
    resume = run_background("resume", control_env, timeout=300)
    append_ndjson(
        RECOVERY_ACTIONS_PATH,
        {
            "event": "recover_and_resume",
            "recover_returncode": recover.get("returncode"),
            "resume_returncode": resume.get("returncode"),
            "resume_allowed": resume.get("resume_allowed"),
            "resume_reason": resume.get("reason"),
        },
    )
    if resume.get("returncode") == 0 and resume.get("resume_allowed") is True:
        start = run_background("start", control_env, timeout=120)
        append_ndjson(
            RECOVERY_ACTIONS_PATH,
            {
                "event": "resume_start",
                "start_returncode": start.get("returncode"),
                "pid": start.get("pid"),
            },
        )
        return start
    return resume


def run_command(args: argparse.Namespace) -> int:
    if not STATUS_PATH.exists() or args.restart_if_stopped:
        rc = start_command(args)
        if rc != 0:
            return rc
    recovery_attempts: dict[str, int] = {}
    while True:
        status_payload = run_background("status", args.control_env, timeout=120)
        status = aggregate_status(status_payload)
        terminal = terminal_classification(status)
        if terminal:
            status["classification"] = terminal
            status["state"] = "complete" if terminal == SUCCESS_CLASSIFICATION else "review_triggered"
            write_json(STATUS_PATH, status)
            write_live_summary(status)
            zip_path = write_final_outputs(terminal)
            print(json.dumps({"classification": terminal, "zip_path": str(zip_path)}, sort_keys=True))
            return 0 if terminal == SUCCESS_CLASSIFICATION else 3
        if status.get("state") in {"blocked", "needs_recovery", "recovered"} or status_payload.get("returncode") != 0:
            blocker = str(status.get("blocker") or status_payload.get("blocker") or "unknown_blocker")
            recovery_attempts[blocker] = recovery_attempts.get(blocker, 0) + 1
            write_blocker_history(status)
            if recovery_attempts[blocker] > args.max_recovery_attempts:
                status["classification"] = EXTERNAL_BLOCKER
                status["state"] = "blocked"
                write_json(STATUS_PATH, status)
                write_live_summary(status)
                zip_path = write_final_outputs(EXTERNAL_BLOCKER, blocker)
                print(json.dumps({"classification": EXTERNAL_BLOCKER, "blocker": blocker, "zip_path": str(zip_path)}, sort_keys=True))
                return 2
            recover_and_resume(args.control_env)
            continue
        status["classification"] = RUNNING_CLASSIFICATION
        write_json(STATUS_PATH, status)
        write_live_summary(status)
        time.sleep(args.poll_seconds)


def finalize_command(_: argparse.Namespace) -> int:
    status = aggregate_status(read_json(STATUS_PATH))
    classification = terminal_classification(status) or str(status.get("classification") or RUNNING_CLASSIFICATION)
    zip_path = write_final_outputs(classification, str(status.get("blocker", "")))
    print(json.dumps({"classification": classification, "zip_path": str(zip_path)}, sort_keys=True))
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["preflight", "start", "status", "run", "finalize"])
    parser.add_argument("--control-env", type=pathlib.Path, default=DEFAULT_CONTROL_ENV)
    parser.add_argument("--allow-dirty-source", action="store_true")
    parser.add_argument("--restart-if-stopped", action="store_true")
    parser.add_argument("--poll-seconds", type=int, default=POLL_SECONDS)
    parser.add_argument("--max-recovery-attempts", type=int, default=MAX_RECOVERY_ATTEMPTS_PER_BLOCKER)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.command == "preflight":
        payload = preflight(args.control_env, allow_dirty=args.allow_dirty_source)
        print(json.dumps(payload, sort_keys=True))
        return 0 if payload.get("ok") else 2
    if args.command == "start":
        return start_command(args)
    if args.command == "status":
        return status_command(args)
    if args.command == "run":
        return run_command(args)
    if args.command == "finalize":
        return finalize_command(args)
    raise MissionError(f"unknown command {args.command}")


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except MissionError as exc:
        status = aggregate_status(
            {
                "state": "blocked",
                "classification": EXTERNAL_BLOCKER,
                "blocker": str(exc),
            }
        )
        write_json(STATUS_PATH, status)
        write_live_summary(status)
        print(json.dumps(status, sort_keys=True))
        raise SystemExit(2)
