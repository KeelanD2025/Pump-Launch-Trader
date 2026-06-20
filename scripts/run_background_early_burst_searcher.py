#!/usr/bin/env python3
"""Fail-closed targeted early-burst background searcher.

This wrapper exists to keep targeted early-burst collection out of ad-hoc shell
snippets. It never starts replay, backtesting, threshold tuning, paper/live
trading, wallet execution, or the old VPS material-hunter path.
"""

from __future__ import annotations

import argparse
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
DEFAULT_CONTROL_ENV = REPO / ".codex_runtime_env" / "relay_control.env"
DEFAULT_JUSTIFICATION_ID = "targeted-early-burst-sample-20260620-001"
TARGET_GATE = "EARLY_BURST_BACKTEST_READINESS"


class SearcherError(RuntimeError):
    pass


def utc_stamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


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


def write_live_summary(status: dict[str, Any]) -> None:
    STATUS_ROOT.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Background Early-Burst Searcher",
        "",
        f"- updated_at_utc: `{utc_stamp()}`",
        f"- state: `{status.get('state', 'unknown')}`",
        f"- blocker: `{status.get('blocker', '')}`",
        f"- pid: `{status.get('pid', '')}`",
        f"- run_prefix: `{status.get('run_prefix', '')}`",
        f"- target_gate: `{status.get('target_gate', TARGET_GATE)}`",
        f"- max_slices_authorized: `{status.get('max_slices_authorized', 10)}`",
        f"- replay_backtesting_tuning_trading: `blocked`",
        f"- launch_caps: `blocked`",
        "",
        "## Paths",
        f"- status_json: `{STATUS_PATH}`",
        f"- events_ndjson: `{EVENTS_PATH}`",
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
        ["python3", "scripts/build_positive_outcomes.py"],
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
    if payload and payload.get("pid"):
        payload["process_running"] = pid_running(payload.get("pid"))
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
    summary = summarize_from_batch_log(pathlib.Path(args.batch_log_dir) if args.batch_log_dir else None)
    reports = run_reports()
    payload = {"summary": summary, "report_refresh": reports}
    print(json.dumps(payload, sort_keys=True))
    return 0 if reports.get("ok") else 2


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
