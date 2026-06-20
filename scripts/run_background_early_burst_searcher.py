#!/usr/bin/env python3
"""Fail-closed targeted early-burst background searcher.

This wrapper exists to keep targeted early-burst collection out of ad-hoc shell
snippets. It never starts replay, backtesting, threshold tuning, paper/live
trading, wallet execution, or the old VPS material-hunter path.
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
STATUS_ROOT = REPO / "research_output" / "trading_strategy_pipeline" / "background_searcher"
STATUS_PATH = STATUS_ROOT / "status.json"
LIVE_SUMMARY_PATH = STATUS_ROOT / "live_summary.md"
EVENTS_PATH = STATUS_ROOT / "events.ndjson"
REVIEW_QUEUE_PATH = STATUS_ROOT / "review_queue.csv"
SLICE_SUMMARIES_PATH = STATUS_ROOT / "slice_summaries.csv"
DEFAULT_CONTROL_ENV = REPO / ".codex_runtime_env" / "relay_control.env"
DEFAULT_JUSTIFICATION_ID = "targeted-early-burst-sample-20260620-001"
TARGET_GATE = "EARLY_BURST_BACKTEST_READINESS"
BASELINE_HIGH_POSITIVE_MINTS = 4
TARGET_HIGH_POSITIVE_MINTS = 20
STALE_STATUS_WARNING_SECONDS = 35 * 60


class SearcherError(RuntimeError):
    pass


def utc_stamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def parse_utc_stamp(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return None


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def read_json(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open() as handle:
        return json.load(handle)


def write_json(path: pathlib.Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def append_event(event: dict[str, Any]) -> None:
    STATUS_ROOT.mkdir(parents=True, exist_ok=True)
    payload = {"ts": utc_stamp(), **event}
    with EVENTS_PATH.open("a") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def read_events() -> list[dict[str, Any]]:
    if not EVENTS_PATH.exists():
        return []
    rows: list[dict[str, Any]] = []
    for raw in EVENTS_PATH.read_text().splitlines():
        if not raw.strip():
            continue
        try:
            rows.append(json.loads(raw))
        except json.JSONDecodeError:
            rows.append({"event": "malformed_event_row", "raw": raw[:240]})
    return rows


def read_csv_rows(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def safe_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return default


def approved_monitor_paths() -> list[pathlib.Path]:
    return [STATUS_PATH, LIVE_SUMMARY_PATH, EVENTS_PATH, SLICE_SUMMARIES_PATH, REVIEW_QUEUE_PATH]


def producer_monitor_paths() -> list[pathlib.Path]:
    return [EVENTS_PATH, SLICE_SUMMARIES_PATH, REVIEW_QUEUE_PATH]


def latest_started_event(events: list[dict[str, Any]], pid: int | None) -> dict[str, Any] | None:
    started = [event for event in events if event.get("event") == "started"]
    if pid is not None:
        matching = [event for event in started if safe_int(event.get("pid"), -1) == pid]
        if matching:
            return matching[-1]
    return started[-1] if started else None


def write_live_summary(status: dict[str, Any]) -> None:
    STATUS_ROOT.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Background Early-Burst Searcher",
        "",
        f"- updated_at_utc: `{utc_stamp()}`",
        f"- monitor_checked_at_utc: `{status.get('monitor_checked_at_utc', '')}`",
        f"- state: `{status.get('state', 'unknown')}`",
        f"- blocker: `{status.get('blocker', '')}`",
        f"- pid: `{status.get('pid', '')}`",
        f"- process_alive: `{str(status.get('process_alive', status.get('process_running', False))).lower()}`",
        f"- elapsed_seconds_since_start: `{status.get('elapsed_seconds_since_start', '')}`",
        f"- current_slice_state: `{status.get('current_slice_state', '')}`",
        f"- slices_completed: `{status.get('slices_completed', 0)}`",
        f"- review_queue_rows: `{status.get('review_queue_rows', 0)}`",
        f"- baseline_high_positive_unique_mints: `{status.get('baseline_high_positive_unique_mints', BASELINE_HIGH_POSITIVE_MINTS)}`",
        f"- new_high_positive_unique_mints_this_run: `{status.get('new_high_positive_unique_mints_this_run', '')}`",
        f"- total_high_positive_unique_mints: `{status.get('total_high_positive_unique_mints', '')}`",
        f"- target_high_positive_unique_mints: `{status.get('target_high_positive_unique_mints', TARGET_HIGH_POSITIVE_MINTS)}`",
        f"- additional_high_positive_needed: `{status.get('additional_high_positive_needed', '')}`",
        f"- candidate_replay_trigger_visible: `{str(status.get('candidate_replay_trigger_visible', False)).lower()}`",
        f"- blocker_visible: `{str(status.get('blocker_visible', False)).lower()}`",
        f"- stale_status_warning: `{str(status.get('stale_status_warning', False)).lower()}`",
        f"- run_prefix: `{status.get('run_prefix', '')}`",
        f"- target_gate: `{status.get('target_gate', TARGET_GATE)}`",
        f"- max_slices_authorized: `{status.get('max_slices_authorized', 10)}`",
        f"- replay_backtesting_tuning_trading: `blocked`",
        f"- launch_caps: `blocked`",
        "",
        "## Paths",
        f"- status_json: `{STATUS_PATH}`",
        f"- events_ndjson: `{EVENTS_PATH}`",
        f"- slice_summaries_csv: `{SLICE_SUMMARIES_PATH}`",
        f"- review_queue_csv: `{REVIEW_QUEUE_PATH}`",
    ]
    LIVE_SUMMARY_PATH.write_text("\n".join(lines) + "\n")


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


def load_env_file(path: pathlib.Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key.startswith("export "):
            key = key.removeprefix("export ").strip()
        env[key] = value.strip().strip("'\"")
    return env


def merged_env(control_env: pathlib.Path) -> dict[str, str]:
    env = os.environ.copy()
    env.update(load_env_file(control_env))
    return env


def run_preflight(control_env: pathlib.Path) -> dict[str, Any]:
    env = merged_env(control_env)
    env["PUMP_RELAY_CONTROL_ENV_FILE"] = str(control_env)
    proc = subprocess.run(
        ["./scripts/relay_control_config_preflight.sh"],
        cwd=REPO,
        env=env,
        text=True,
        capture_output=True,
        timeout=240,
    )
    payload = read_json(REPO / "research_output" / "trading_strategy_pipeline" / "relay_control_config_preflight.json")
    if not payload:
        payload = {
            "classification": "RELAY_CONTROL_CONFIG_BLOCK_STRUCTURAL",
            "ok": False,
            "blockers": ["missing_preflight_output"],
            "stderr_tail": proc.stderr[-1200:],
        }
    return payload


def write_blocked_status(blocker: str, preflight: dict[str, Any] | None = None) -> dict[str, Any]:
    status = {
        "schema_version": "phase107j.background_early_burst_searcher.v1",
        "updated_at_utc": utc_stamp(),
        "state": "blocked",
        "blocker": blocker,
        "preflight_classification": (preflight or {}).get("classification"),
        "missing_config_names": (preflight or {}).get("missing_config_names") or [],
        "status_path": str(STATUS_PATH),
        "live_summary_path": str(LIVE_SUMMARY_PATH),
        "review_queue_path": str(REVIEW_QUEUE_PATH),
        "generic_collection_allowed": False,
        "replay_allowed": False,
        "formal_backtesting_allowed": False,
        "threshold_tuning_allowed": False,
        "paper_trading_enabled": False,
        "live_trading_enabled": False,
        "wallet_execution_enabled": False,
        "launch_caps_remain_blocked": True,
    }
    write_json(STATUS_PATH, status)
    write_live_summary(status)
    append_event({"event": "blocked", "blocker": blocker, "preflight": (preflight or {}).get("classification")})
    ensure_review_queue()
    return status


def ensure_review_queue() -> None:
    STATUS_ROOT.mkdir(parents=True, exist_ok=True)
    if not REVIEW_QUEUE_PATH.exists():
        REVIEW_QUEUE_PATH.write_text(
            "created_at_utc,run_id,mint,reason,status\n",
        )


def aggregate_slice_monitor(rows: list[dict[str, str]]) -> dict[str, Any]:
    candidate_checkpoints = sum(safe_int(row.get("candidate_checkpoint_count")) for row in rows)
    replay_eligible = sum(safe_int(row.get("replay_eligible_candidate_count")) for row in rows)
    sequence_gaps = sum(safe_int(row.get("sequence_gap_count")) for row in rows)
    hash_mismatches = sum(safe_int(row.get("hash_mismatch_count")) for row in rows)
    malformed_frames = sum(safe_int(row.get("malformed_frame_count")) for row in rows)
    receiver_backpressure = sum(safe_int(row.get("receiver_backpressure_count")) for row in rows)
    receiver_unavailable = sum(safe_int(row.get("receiver_unavailable_count")) for row in rows)
    blockers = [
        row
        for row in rows
        if "BLOCK" in (row.get("classification") or row.get("status") or "").upper()
        or safe_int(row.get("r2_verification_failures")) > 0
        or safe_int(row.get("r2_failures")) > 0
        or safe_int(row.get("sequence_gap_count")) > 0
        or safe_int(row.get("hash_mismatch_count")) > 0
        or safe_int(row.get("malformed_frame_count")) > 0
        or safe_int(row.get("receiver_backpressure_count")) > 0
        or safe_int(row.get("receiver_unavailable_count")) > 0
        or str(row.get("artifact_consistency_result", "")).upper() in {"FAIL", "FAILED", "BLOCK"}
    ]
    high_positive_total = 0
    positive_high_rows = 0
    candidate_watch_rows = 0
    for row in rows:
        high_positive_total = max(
            high_positive_total,
            safe_int(row.get("high_positive_unique_mints_total")),
            safe_int(row.get("high_positive_unique_mints")),
            safe_int(row.get("high_positive_count")),
        )
        positive_high_rows += safe_int(row.get("positive_high_rows"))
        positive_high_rows += safe_int(row.get("positive_high_count"))
        candidate_watch_rows += safe_int(row.get("candidate_watch_rows"))
        candidate_watch_rows += safe_int(row.get("candidate_watch_count"))
    return {
        "slices_completed": len(rows),
        "candidate_checkpoint_count_visible": candidate_checkpoints,
        "replay_eligible_candidate_count_visible": replay_eligible,
        "sequence_gap_count_visible": sequence_gaps,
        "hash_mismatch_count_visible": hash_mismatches,
        "malformed_frame_count_visible": malformed_frames,
        "receiver_backpressure_count_visible": receiver_backpressure,
        "receiver_unavailable_count_visible": receiver_unavailable,
        "slice_blocker_rows_visible": len(blockers),
        "r2_verification_failures_visible": sum(
            safe_int(row.get("r2_verification_failures")) + safe_int(row.get("r2_failures")) for row in rows
        ),
        "artifact_consistency_failures_visible": sum(
            1
            for row in rows
            if str(row.get("artifact_consistency_result", "")).upper() in {"FAIL", "FAILED", "BLOCK"}
            or str(row.get("artifact_consistency_ok", "")).lower() == "false"
        ),
        "new_high_positive_unique_mints_this_run": high_positive_total,
        "positive_high_rows_visible": positive_high_rows,
        "candidate_watch_rows_visible": candidate_watch_rows,
    }


def count_review_rows(rows: list[dict[str, str]]) -> int:
    return sum(1 for row in rows if any(str(value).strip() for value in row.values()))


def approved_files_max_age_seconds(now: dt.datetime) -> int | None:
    mtimes = [path.stat().st_mtime for path in approved_monitor_paths() if path.exists()]
    if not mtimes:
        return None
    return max(0, int(now.timestamp() - max(mtimes)))


def producer_files_max_age_seconds(now: dt.datetime) -> int | None:
    mtimes = [path.stat().st_mtime for path in producer_monitor_paths() if path.exists()]
    if not mtimes:
        return None
    return max(0, int(now.timestamp() - max(mtimes)))


def read_batch_summary_rows(batch_log_dir: pathlib.Path) -> list[dict[str, Any]]:
    summary_path = batch_log_dir / "batch_summary.ndjson"
    if not summary_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for raw in summary_path.read_text().splitlines():
        if raw.strip():
            rows.append(json.loads(raw))
    return rows


def mirror_batch_summary_to_slice_summaries(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    fields = [
        "slice_index",
        "run",
        "relay_session_id",
        "classification",
        "duration_seconds",
        "frames_received",
        "sequence_gap_count",
        "hash_mismatch_count",
        "malformed_frame_count",
        "receiver_backpressure_count",
        "receiver_unavailable_count",
        "upstream_provider_blocker_count",
        "upstream_reconnect_count",
        "attempted_launches",
        "unique_attempted_mints",
        "rejected_dead_count",
        "terminal_inconclusive_count",
        "candidate_checkpoint_count",
        "replay_eligible_candidate_count",
        "r2_uploaded",
        "r2_failures",
        "artifact_consistency_ok",
        "asof_alpha_feature_total_rows",
        "retention_deleted_bytes",
        "local_retained_bytes",
        "vps_forbidden_artifact_count",
        "latest_run_id_changed",
        "old_material_hunter_active",
        "vps_r2_evidence_count",
    ]
    STATUS_ROOT.mkdir(parents=True, exist_ok=True)
    existing = SLICE_SUMMARIES_PATH.read_text() if SLICE_SUMMARIES_PATH.exists() else ""
    rendered_rows: list[dict[str, Any]] = []
    for row in rows:
        rendered_rows.append(
            {
                "slice_index": row.get("slice_index", ""),
                "run": row.get("run", ""),
                "relay_session_id": row.get("relay_session_id", ""),
                "classification": row.get("classification", ""),
                "duration_seconds": row.get("duration_seconds", ""),
                "frames_received": row.get("frames_received", ""),
                "sequence_gap_count": row.get("sequence_gap_count", ""),
                "hash_mismatch_count": row.get("hash_mismatch_count", ""),
                "malformed_frame_count": row.get("malformed_frame_count", ""),
                "receiver_backpressure_count": row.get("receiver_backpressure_count", ""),
                "receiver_unavailable_count": row.get("receiver_unavailable_count", ""),
                "upstream_provider_blocker_count": row.get("upstream_provider_blocker_count", ""),
                "upstream_reconnect_count": row.get("upstream_reconnect_count", ""),
                "attempted_launches": row.get("attempted_launches", ""),
                "unique_attempted_mints": row.get("unique_attempted_mints", ""),
                "rejected_dead_count": row.get("rejected_dead_count", ""),
                "terminal_inconclusive_count": row.get("terminal_inconclusive_count", ""),
                "candidate_checkpoint_count": row.get("candidate_checkpoint_count", ""),
                "replay_eligible_candidate_count": row.get("replay_eligible_candidate_count", ""),
                "r2_uploaded": row.get("r2_uploaded", ""),
                "r2_failures": row.get("r2_failed", row.get("r2_failures", "")),
                "artifact_consistency_ok": row.get("artifact_consistency_ok", ""),
                "asof_alpha_feature_total_rows": row.get("asof_alpha_feature_total_rows", ""),
                "retention_deleted_bytes": row.get("retention_deleted_bytes", ""),
                "local_retained_bytes": row.get("local_retained_bytes", ""),
                "vps_forbidden_artifact_count": row.get("vps_forbidden_artifact_count", ""),
                "latest_run_id_changed": row.get("latest_run_id_changed", ""),
                "old_material_hunter_active": row.get("old_material_hunter_active", ""),
                "vps_r2_evidence_count": row.get("vps_r2_evidence_count", ""),
            }
        )
    import io

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rendered_rows)
    rendered = buf.getvalue()
    if rendered == existing:
        return False
    SLICE_SUMMARIES_PATH.write_text(rendered)
    return True


def build_candidate_review_pack_from_monitor(monitor: dict[str, Any]) -> str:
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    pack = STATUS_ROOT / f"candidate_review_pack_{stamp}"
    pack.mkdir(parents=True, exist_ok=True)
    if REVIEW_QUEUE_PATH.exists():
        (pack / "review_queue.csv").write_text(REVIEW_QUEUE_PATH.read_text())
    if SLICE_SUMMARIES_PATH.exists():
        (pack / "slice_summaries.csv").write_text(SLICE_SUMMARIES_PATH.read_text())
    write_json(pack / "candidate_trigger.json", monitor)
    (pack / "README.md").write_text(
        "# Candidate Review Pack\n\n"
        "Generated from approved background-searcher monitor files only. Replay, backtesting, "
        "threshold tuning, paper/live trading, wallet execution, and launch-cap changes remain blocked.\n"
    )
    return str(pack)


def write_sample_scarcity_report(monitor: dict[str, Any]) -> str:
    rows = read_csv_rows(SLICE_SUMMARIES_PATH)
    total_attempts = sum(safe_int(row.get("attempted_launches")) for row in rows)
    total_unique = sum(safe_int(row.get("unique_attempted_mints")) for row in rows)
    total_frames = sum(safe_int(row.get("frames_received")) for row in rows)
    total_rejected = sum(safe_int(row.get("rejected_dead_count")) for row in rows)
    total_inconclusive = sum(safe_int(row.get("terminal_inconclusive_count")) for row in rows)
    payload = {
        "schema_version": "phase107j.background_early_burst_sample_scarcity.v1",
        "generated_at_utc": utc_stamp(),
        "classification": "EARLY_BURST_TARGETED_SAMPLE_SCARCITY",
        "collection_allowed": False,
        "next_collection_requires_explicit_approval": True,
        "slices_completed": monitor.get("slices_completed", len(rows)),
        "baseline_high_positive_unique_mints": monitor.get("baseline_high_positive_unique_mints", BASELINE_HIGH_POSITIVE_MINTS),
        "new_high_positive_unique_mints_this_run": monitor.get("new_high_positive_unique_mints_this_run", 0),
        "total_high_positive_unique_mints": monitor.get("total_high_positive_unique_mints", BASELINE_HIGH_POSITIVE_MINTS),
        "target_high_positive_unique_mints": TARGET_HIGH_POSITIVE_MINTS,
        "additional_high_positive_needed": monitor.get("additional_high_positive_needed", TARGET_HIGH_POSITIVE_MINTS),
        "total_frames": total_frames,
        "total_attempted_launches": total_attempts,
        "total_unique_attempted_mints": total_unique,
        "total_rejected_dead": total_rejected,
        "total_terminal_inconclusive": total_inconclusive,
        "candidate_checkpoint_count": monitor.get("candidate_checkpoint_count_visible", 0),
        "replay_eligible_candidate_count": monitor.get("replay_eligible_candidate_count_visible", 0),
        "r2_verification_failures": monitor.get("r2_verification_failures_visible", 0),
        "artifact_consistency_failures": monitor.get("artifact_consistency_failures_visible", 0),
        "sample_size_status": "target_not_reached",
        "high_positive_rate_observation": (
            "No new high-positive examples appeared in this targeted run, so the observed incremental "
            "high-positive rate is below the collection target for this batch."
        ),
        "recommendation": (
            "Do not auto-authorize another batch. Review the targeted-search filters and the early-burst "
            "data-needed plan before approving any further targeted collection."
        ),
        "blocked_actions": [
            "replay",
            "formal_backtesting",
            "threshold_tuning",
            "paper_trading",
            "live_trading",
            "wallet_execution",
            "launch_cap_raise",
            "generic_collection",
        ],
    }
    json_path = STATUS_ROOT / "sample_scarcity_report.json"
    md_path = STATUS_ROOT / "sample_scarcity_report.md"
    write_json(json_path, payload)
    md_path.write_text(
        "# Early-Burst Sample Scarcity Report\n\n"
        f"- classification: `{payload['classification']}`\n"
        f"- collection_allowed: `false`\n"
        f"- slices_completed: `{payload['slices_completed']}`\n"
        f"- baseline_high_positive_unique_mints: `{payload['baseline_high_positive_unique_mints']}`\n"
        f"- new_high_positive_unique_mints_this_run: `{payload['new_high_positive_unique_mints_this_run']}`\n"
        f"- total_high_positive_unique_mints: `{payload['total_high_positive_unique_mints']}`\n"
        f"- target_high_positive_unique_mints: `{payload['target_high_positive_unique_mints']}`\n"
        f"- additional_high_positive_needed: `{payload['additional_high_positive_needed']}`\n"
        f"- total_attempted_launches: `{payload['total_attempted_launches']}`\n"
        f"- candidate_checkpoint_count: `{payload['candidate_checkpoint_count']}`\n"
        f"- replay_eligible_candidate_count: `{payload['replay_eligible_candidate_count']}`\n"
        f"- r2_verification_failures: `{payload['r2_verification_failures']}`\n"
        f"- artifact_consistency_failures: `{payload['artifact_consistency_failures']}`\n\n"
        "## Interpretation\n"
        f"{payload['high_positive_rate_observation']}\n\n"
        "## Recommendation\n"
        f"{payload['recommendation']}\n\n"
        "Replay, formal backtesting, threshold tuning, paper/live trading, wallet execution, "
        "generic collection, and launch-cap raises remain blocked.\n"
    )
    return str(md_path)


def request_stop_for_monitor(pid: int | None, reason: str) -> bool:
    if not pid_running(pid):
        return False
    try:
        os.killpg(os.getpgid(int(pid)), signal.SIGTERM)
    except ProcessLookupError:
        return False
    append_event({"event": "stop_requested", "pid": pid, "reason": reason})
    return True


def monitor_payload(payload: dict[str, Any], *, enforce_stop_conditions: bool = True) -> dict[str, Any]:
    if not payload:
        return {"state": "not_started", "status_path": str(STATUS_PATH)}
    now = utc_now()
    checked_at = utc_stamp()
    pid = safe_int(payload.get("pid"), 0) or None
    process_alive = pid_running(pid)
    events = read_events()
    review_rows = read_csv_rows(REVIEW_QUEUE_PATH)
    producer_max_age = producer_files_max_age_seconds(now)
    batch_summary_mirrored = False
    slice_rows = read_csv_rows(SLICE_SUMMARIES_PATH)
    should_check_batch_summary = bool(
        payload.get("batch_log_dir")
        and (
            not process_alive
            or (producer_max_age is not None and producer_max_age > STALE_STATUS_WARNING_SECONDS)
        )
    )
    if should_check_batch_summary:
        batch_rows = read_batch_summary_rows(pathlib.Path(str(payload.get("batch_log_dir"))))
        if len(batch_rows) > len(slice_rows):
            batch_summary_mirrored = mirror_batch_summary_to_slice_summaries(batch_rows)
            slice_rows = read_csv_rows(SLICE_SUMMARIES_PATH)
    slice_metrics = aggregate_slice_monitor(slice_rows)
    review_queue_rows = count_review_rows(review_rows)
    started = latest_started_event(events, pid)
    started_index = events.index(started) if started in events else -1
    events_after_started = events[started_index + 1 :] if started_index >= 0 else events
    started_at = parse_utc_stamp(started.get("ts") if started else payload.get("updated_at_utc"))
    elapsed_seconds = int((now - started_at).total_seconds()) if started_at else None
    candidate_replay_trigger_visible = bool(
        review_queue_rows > 0
        or slice_metrics["candidate_checkpoint_count_visible"] > 0
        or slice_metrics["replay_eligible_candidate_count_visible"] > 0
    )
    event_blocker_visible = any(event.get("event") == "blocked" for event in events_after_started)
    blocker_visible = bool(
        payload.get("blocker")
        or str(payload.get("state", "")).lower() == "blocked"
        or slice_metrics["slice_blocker_rows_visible"] > 0
        or event_blocker_visible
    )
    baseline_high_positive = safe_int(payload.get("baseline_high_positive_unique_mints"), BASELINE_HIGH_POSITIVE_MINTS)
    new_high_positive = max(
        safe_int(payload.get("new_high_positive_unique_mints_this_run")),
        slice_metrics["new_high_positive_unique_mints_this_run"],
    )
    total_high_positive = baseline_high_positive + new_high_positive
    additional_high_positive_needed = max(0, TARGET_HIGH_POSITIVE_MINTS - total_high_positive)
    max_slices = safe_int(payload.get("max_slices_authorized"), 10)
    if process_alive:
        current_slice_state = "in_progress"
    elif str(payload.get("state", "")).lower() in {"complete", "blocked", "recovered"}:
        current_slice_state = str(payload.get("state", "")).lower()
    else:
        current_slice_state = "needs_recovery"
    max_age = approved_files_max_age_seconds(now)
    stale_status_warning = bool(
        process_alive
        and not slice_rows
        and (max_age is not None and max_age > STALE_STATUS_WARNING_SECONDS)
    )
    stop_condition = ""
    if candidate_replay_trigger_visible:
        stop_condition = "candidate_or_replay_trigger_visible"
    elif total_high_positive >= TARGET_HIGH_POSITIVE_MINTS:
        stop_condition = "high_positive_target_reached"
    elif slice_metrics["slices_completed"] >= max_slices and max_slices > 0:
        stop_condition = "max_slices_completed"

    updated = {
        **payload,
        "updated_at_utc": checked_at,
        "monitor_checked_at_utc": checked_at,
        "process_alive": process_alive,
        "process_running": process_alive,
        "elapsed_seconds_since_start": elapsed_seconds,
        "current_slice_state": current_slice_state,
        "slices_completed": slice_metrics["slices_completed"],
        "review_queue_rows": review_queue_rows,
        "candidate_replay_trigger_visible": candidate_replay_trigger_visible,
        "candidate_checkpoint_count_visible": slice_metrics["candidate_checkpoint_count_visible"],
        "replay_eligible_candidate_count_visible": slice_metrics["replay_eligible_candidate_count_visible"],
        "sequence_gap_count_visible": slice_metrics["sequence_gap_count_visible"],
        "hash_mismatch_count_visible": slice_metrics["hash_mismatch_count_visible"],
        "malformed_frame_count_visible": slice_metrics["malformed_frame_count_visible"],
        "receiver_backpressure_count_visible": slice_metrics["receiver_backpressure_count_visible"],
        "receiver_unavailable_count_visible": slice_metrics["receiver_unavailable_count_visible"],
        "blocker_visible": blocker_visible,
        "baseline_high_positive_unique_mints": baseline_high_positive,
        "new_high_positive_unique_mints_this_run": new_high_positive,
        "total_high_positive_unique_mints": total_high_positive,
        "high_positive_unique_mints_total": total_high_positive,
        "target_high_positive_unique_mints": TARGET_HIGH_POSITIVE_MINTS,
        "target_high_positive_mints": TARGET_HIGH_POSITIVE_MINTS,
        "additional_high_positive_needed": additional_high_positive_needed,
        "positive_high_rows_visible": slice_metrics["positive_high_rows_visible"],
        "candidate_watch_rows_visible": slice_metrics["candidate_watch_rows_visible"],
        "r2_verification_failures_visible": slice_metrics["r2_verification_failures_visible"],
        "artifact_consistency_failures_visible": slice_metrics["artifact_consistency_failures_visible"],
        "stale_status_warning": stale_status_warning,
        "approved_files_max_age_seconds": max_age,
        "producer_files_max_age_seconds": producer_max_age,
        "batch_summary_mirrored_to_slice_summaries": batch_summary_mirrored,
        "stop_condition_visible": stop_condition,
        "generic_collection_allowed": False,
        "replay_allowed": False,
        "formal_backtesting_allowed": False,
        "threshold_tuning_allowed": False,
        "paper_trading_enabled": False,
        "live_trading_enabled": False,
        "wallet_execution_enabled": False,
        "launch_caps_remain_blocked": True,
    }
    if enforce_stop_conditions and stop_condition:
        if process_alive:
            stopped = request_stop_for_monitor(pid, stop_condition)
            updated["monitor_stop_requested"] = stopped
            updated["state"] = "stopping" if stopped else updated.get("state", "running")
        else:
            updated["monitor_stop_requested"] = False
        updated["monitor_stop_reason"] = stop_condition
        report_refresh_ok = bool(
            isinstance(updated.get("report_refresh_after_stop"), dict)
            and updated["report_refresh_after_stop"].get("ok")
        )
        if stop_condition == "candidate_or_replay_trigger_visible" and not updated.get("candidate_review_pack_path"):
            updated["candidate_review_pack_path"] = build_candidate_review_pack_from_monitor(updated)
        elif stop_condition == "high_positive_target_reached" and not report_refresh_ok:
            updated["report_refresh_after_stop"] = run_reports()
        elif stop_condition == "max_slices_completed":
            if not report_refresh_ok:
                updated["report_refresh_after_stop"] = run_reports()
            if not updated.get("sample_scarcity_report_path"):
                updated["sample_scarcity_report_path"] = write_sample_scarcity_report(updated)
        if isinstance(updated.get("report_refresh_after_stop"), dict):
            updated["report_refresh_ok"] = bool(updated["report_refresh_after_stop"].get("ok"))
    return updated


def summarize_from_approved_monitor_files(payload: dict[str, Any]) -> dict[str, Any]:
    monitored = payload
    return {
        "state": monitored.get("state", "unknown"),
        "process_alive": monitored.get("process_alive", False),
        "pid": monitored.get("pid"),
        "current_slice_state": monitored.get("current_slice_state"),
        "slices_completed": monitored.get("slices_completed", 0),
        "review_queue_rows": monitored.get("review_queue_rows", 0),
        "candidate_replay_trigger_visible": monitored.get("candidate_replay_trigger_visible", False),
        "blocker_visible": monitored.get("blocker_visible", False),
        "baseline_high_positive_unique_mints": monitored.get(
            "baseline_high_positive_unique_mints",
            BASELINE_HIGH_POSITIVE_MINTS,
        ),
        "new_high_positive_unique_mints_this_run": monitored.get("new_high_positive_unique_mints_this_run", 0),
        "total_high_positive_unique_mints": monitored.get("total_high_positive_unique_mints", 0),
        "target_high_positive_unique_mints": TARGET_HIGH_POSITIVE_MINTS,
        "additional_high_positive_needed": monitored.get("additional_high_positive_needed", TARGET_HIGH_POSITIVE_MINTS),
        "positive_high_rows_visible": monitored.get("positive_high_rows_visible", 0),
        "candidate_watch_rows_visible": monitored.get("candidate_watch_rows_visible", 0),
        "r2_verification_failures_visible": monitored.get("r2_verification_failures_visible", 0),
        "artifact_consistency_failures_visible": monitored.get("artifact_consistency_failures_visible", 0),
        "stop_condition_visible": monitored.get("stop_condition_visible", ""),
        "stale_status_warning": monitored.get("stale_status_warning", False),
    }


def summarize_from_batch_log(batch_log_dir: pathlib.Path | None = None) -> dict[str, Any]:
    if batch_log_dir is None:
        status = read_json(STATUS_PATH)
        batch_log_dir = pathlib.Path(status.get("batch_log_dir") or "")
    rows: list[dict[str, Any]] = []
    summary_path = batch_log_dir / "batch_summary.ndjson" if batch_log_dir else pathlib.Path()
    if summary_path.exists():
        for raw in summary_path.read_text().splitlines():
            if raw.strip():
                rows.append(json.loads(raw))
    total = {
        "slice_count": len(rows),
        "frames_received": sum(int(row.get("frames_received") or 0) for row in rows),
        "attempted_launches": sum(int(row.get("attempted_launches") or 0) for row in rows),
        "candidate_checkpoint_count": sum(int(row.get("candidate_checkpoint_count") or 0) for row in rows),
        "replay_eligible_candidate_count": sum(int(row.get("replay_eligible_candidate_count") or 0) for row in rows),
        "r2_failures": sum(int(row.get("r2_failed") or 0) for row in rows),
        "artifact_consistency_failures": sum(0 if row.get("artifact_consistency_ok") else 1 for row in rows),
        "runs": [row.get("run") for row in rows],
    }
    write_json(STATUS_ROOT / "summary.json", total)
    return total


def run_reports() -> dict[str, Any]:
    commands = [
        ["python3", "scripts/build_positive_outcome_labels.py"],
        ["python3", "scripts/build_early_burst_validation_dataset.py"],
        ["python3", "scripts/build_early_burst_backtest_readiness.py"],
    ]
    results = []
    for cmd in commands:
        proc = subprocess.run(cmd, cwd=REPO, text=True, capture_output=True, timeout=600)
        results.append(
            {
                "cmd": cmd,
                "returncode": proc.returncode,
                "stdout_tail": proc.stdout[-1000:],
                "stderr_tail": proc.stderr[-1000:],
            }
        )
        if proc.returncode != 0:
            break
    payload = {"updated_at_utc": utc_stamp(), "results": results, "ok": all(item["returncode"] == 0 for item in results)}
    write_json(STATUS_ROOT / "report_refresh.json", payload)
    return payload


def start(args: argparse.Namespace) -> int:
    STATUS_ROOT.mkdir(parents=True, exist_ok=True)
    ensure_review_queue()
    existing = read_json(STATUS_PATH)
    if pid_running(existing.get("pid")):
        raise SearcherError(f"background searcher already running pid={existing.get('pid')}")
    preflight = run_preflight(args.control_env)
    if preflight.get("classification") != "RELAY_CONTROL_CONFIG_PASS":
        write_blocked_status("relay_control_config_preflight_failed", preflight)
        print(json.dumps(read_json(STATUS_PATH), sort_keys=True))
        return 2

    batch_log_dir = STATUS_ROOT / f"batch_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
    worker_cmd = [
        sys.executable,
        str(pathlib.Path(__file__).resolve()),
        "worker",
        "--control-env",
        str(args.control_env),
        "--batch-log-dir",
        str(batch_log_dir),
        "--slices",
        str(args.slices),
        "--duration-seconds",
        str(args.duration_seconds),
        "--justification-id",
        args.justification_id,
        "--max-slices",
        str(args.max_slices),
    ]
    log = (STATUS_ROOT / "worker.log").open("a")
    err = (STATUS_ROOT / "worker.err").open("a")
    proc = subprocess.Popen(worker_cmd, cwd=REPO, stdout=log, stderr=err, start_new_session=True, text=True)
    status = {
        "schema_version": "phase107j.background_early_burst_searcher.v1",
        "updated_at_utc": utc_stamp(),
        "state": "running",
        "pid": proc.pid,
        "batch_log_dir": str(batch_log_dir),
        "run_prefix": args.run_prefix,
        "target_gate": TARGET_GATE,
        "max_slices_authorized": args.max_slices,
        "status_path": str(STATUS_PATH),
        "live_summary_path": str(LIVE_SUMMARY_PATH),
        "review_queue_path": str(REVIEW_QUEUE_PATH),
        "generic_collection_allowed": False,
        "replay_allowed": False,
        "formal_backtesting_allowed": False,
        "threshold_tuning_allowed": False,
        "paper_trading_enabled": False,
        "live_trading_enabled": False,
        "wallet_execution_enabled": False,
        "launch_caps_remain_blocked": True,
    }
    write_json(STATUS_PATH, status)
    write_live_summary(status)
    append_event({"event": "started", "pid": proc.pid, "batch_log_dir": str(batch_log_dir)})
    print(json.dumps(status, sort_keys=True))
    return 0


def worker(args: argparse.Namespace) -> int:
    env = merged_env(args.control_env)
    cmd = [
        "python3",
        "scripts/run_relay_r2_primary_batch.py",
        "batch",
        "--env-file",
        str(args.control_env),
        "--slices",
        str(args.slices),
        "--counted-slices-target",
        str(args.slices),
        "--max-total-slices",
        str(args.slices),
        "--duration-seconds",
        str(args.duration_seconds),
        "--run-prefix",
        args.run_prefix,
        "--batch-log-dir",
        str(args.batch_log_dir),
        "--justification-id",
        args.justification_id,
        "--max-slices",
        str(args.max_slices),
        "--target-gate",
        TARGET_GATE,
    ]
    started = time.time()
    proc = subprocess.run(cmd, cwd=REPO, env=env, text=True)
    reports = run_reports()
    batch_summary = summarize_from_batch_log(args.batch_log_dir)
    state = "complete" if proc.returncode == 0 else "blocked"
    blocker = "" if proc.returncode == 0 else "supervisor_batch_failed"
    status = {
        **read_json(STATUS_PATH),
        "updated_at_utc": utc_stamp(),
        "state": state,
        "blocker": blocker,
        "pid": None,
        "worker_returncode": proc.returncode,
        "elapsed_seconds": int(time.time() - started),
        "batch_summary": batch_summary,
        "report_refresh_ok": reports.get("ok"),
        "generic_collection_allowed": False,
        "replay_allowed": False,
        "formal_backtesting_allowed": False,
        "threshold_tuning_allowed": False,
        "paper_trading_enabled": False,
        "live_trading_enabled": False,
        "wallet_execution_enabled": False,
        "launch_caps_remain_blocked": True,
    }
    write_json(STATUS_PATH, status)
    write_live_summary(status)
    append_event({"event": state, "returncode": proc.returncode})
    return proc.returncode


def status(_: argparse.Namespace) -> int:
    payload = read_json(STATUS_PATH)
    if payload:
        payload = monitor_payload(payload, enforce_stop_conditions=True)
        write_json(STATUS_PATH, payload)
        write_live_summary(payload)
    print(json.dumps(payload or {"state": "not_started", "status_path": str(STATUS_PATH)}, sort_keys=True))
    return 0


def stop(_: argparse.Namespace) -> int:
    payload = read_json(STATUS_PATH)
    pid = payload.get("pid")
    if pid_running(pid):
        os.killpg(os.getpgid(int(pid)), signal.SIGTERM)
        payload["state"] = "stopping"
        payload["updated_at_utc"] = utc_stamp()
        write_json(STATUS_PATH, payload)
        write_live_summary(payload)
        append_event({"event": "stop_requested", "pid": pid})
    print(json.dumps(payload or {"state": "not_started"}, sort_keys=True))
    return 0


def recover(args: argparse.Namespace) -> int:
    status_payload = read_json(STATUS_PATH)
    batch_log = pathlib.Path(args.batch_log_dir or status_payload.get("batch_log_dir") or "")
    summary = summarize_from_batch_log(batch_log if str(batch_log) else None)
    reports = run_reports()
    payload = {
        **status_payload,
        "updated_at_utc": utc_stamp(),
        "state": "recovered",
        "batch_summary": summary,
        "report_refresh_ok": reports.get("ok"),
    }
    write_json(STATUS_PATH, payload)
    write_live_summary(payload)
    append_event({"event": "recovered"})
    print(json.dumps(payload, sort_keys=True))
    return 0


def summarize(args: argparse.Namespace) -> int:
    status_payload = read_json(STATUS_PATH)
    if status_payload:
        monitored = monitor_payload(status_payload, enforce_stop_conditions=True)
        write_json(STATUS_PATH, monitored)
        write_live_summary(monitored)
        summary = summarize_from_approved_monitor_files(monitored)
        payload: dict[str, Any] = {"summary": summary, "source": "approved_monitor_files"}
        if not monitored.get("process_alive") and not monitored.get("slices_completed"):
            batch_log = pathlib.Path(args.batch_log_dir or monitored.get("batch_log_dir") or "")
            if str(batch_log):
                payload["batch_log_recovery_summary"] = summarize_from_batch_log(batch_log)
                payload["source"] = "approved_monitor_files_then_batch_log_recovery"
        print(json.dumps(payload, sort_keys=True))
        return 0
    payload = {"summary": {"state": "not_started", "status_path": str(STATUS_PATH)}, "source": "approved_monitor_files"}
    print(json.dumps(payload, sort_keys=True))
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["start", "status", "stop", "recover", "summarize", "worker"])
    parser.add_argument("--control-env", type=pathlib.Path, default=DEFAULT_CONTROL_ENV)
    parser.add_argument("--slices", type=int, default=10)
    parser.add_argument("--max-slices", type=int, default=10)
    parser.add_argument("--duration-seconds", type=int, default=900)
    parser.add_argument("--justification-id", default=DEFAULT_JUSTIFICATION_ID)
    parser.add_argument("--run-prefix", default="relay-r2-early-burst-targeted")
    parser.add_argument("--batch-log-dir", type=pathlib.Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.command == "start":
        return start(args)
    if args.command == "worker":
        if args.batch_log_dir is None:
            raise SearcherError("--batch-log-dir is required for worker")
        return worker(args)
    if args.command == "status":
        return status(args)
    if args.command == "stop":
        return stop(args)
    if args.command == "recover":
        return recover(args)
    if args.command == "summarize":
        return summarize(args)
    raise SearcherError(f"unknown command {args.command}")


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except SearcherError as exc:
        write_blocked_status(str(exc))
        print(json.dumps(read_json(STATUS_PATH), sort_keys=True))
        raise SystemExit(2)
