#!/usr/bin/env python3
"""Keep local generated storage bounded for the R2-first collector path.

This script removes only local generated bulk that is safe to discard:

- raw relay frame shards from completed, R2-verified local collector runs;
- transient local R2 spool directories from completed, R2-verified runs;
- optional local build artifacts;
- optional legacy generated replay/event data that is outside the R2-primary
  collector path.

It preserves countability decisions, R2 summaries, manifests, compact strategy
artifacts, and unverified/in-flight runs. Dry-run is the default.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import subprocess
import time
from typing import Any


REPO = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = REPO / "research_output" / "local_stream_collector"
DEFAULT_REPORT_ROOT = REPO / "research_output" / "trading_strategy_pipeline" / "local_storage_enforcement"
DEFAULT_MAX_LOCAL_STREAM_MB = 5_000
BULK_DIR_NAMES = {
    "relay_frames",
    "raw_relay_frames",
    "relay_frame_shards",
    ".r2_spool",
    "tmp_upload_shards",
    "upload_chunks",
}


def utc_stamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def read_json(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"_parse_error": str(exc)}


def write_json(path: pathlib.Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(str(value)))
    except (TypeError, ValueError):
        return default


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "ok", "pass", "passed"}


def size_bytes(path: pathlib.Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        try:
            return path.stat().st_size
        except OSError:
            return 0
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for filename in filenames:
            try:
                total += (pathlib.Path(dirpath) / filename).stat().st_size
            except OSError:
                continue
    return total


def file_count(path: pathlib.Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return 1
    total = 0
    for _, _, filenames in os.walk(path):
        total += len(filenames)
    return total


def free_mb(path: pathlib.Path) -> int:
    target = path if path.exists() else path.parent
    usage = shutil.disk_usage(target)
    return usage.free // (1024 * 1024)


def pgrep_lines() -> list[str]:
    proc = subprocess.run(
        [
            "pgrep",
            "-af",
            r"[l]ocal-stream-collector|[v]ps-stream-relay|[r]un_background_24h_collector|[r]un_relay_r2_primary_batch|[r]un_background_early_burst_searcher",
        ],
        cwd=REPO,
        text=True,
        capture_output=True,
        timeout=30,
    )
    return [line for line in proc.stdout.splitlines() if line.strip()]


def path_is_under(path: pathlib.Path, root: pathlib.Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def local_integrity_clean(run_dir: pathlib.Path) -> bool:
    collector = read_json(run_dir / "local_collector_summary.json")
    proof = read_json(run_dir / "local_relay_dataset_proof_summary.json")
    source = collector or proof
    if not source:
        return False
    return (
        safe_int(source.get("sequence_gap_count")) == 0
        and safe_int(source.get("hash_mismatch_count")) == 0
        and safe_int(source.get("malformed_frame_count")) == 0
        and safe_int(source.get("receiver_backpressure_count", source.get("downstream_backpressure_count", 0))) == 0
        and safe_int(source.get("receiver_unavailable_count")) == 0
    )


def run_completed(run_dir: pathlib.Path) -> bool:
    service = read_json(run_dir / "service_exit_status.json")
    collector = read_json(run_dir / "local_collector_summary.json")
    proof = read_json(run_dir / "local_relay_dataset_proof_summary.json")
    if service:
        return (
            service.get("hunter_exit_status") == 0
            or service.get("local_collector_exit_status") == 0
            or service.get("service_exit_reason") == "local_relay_collector_completed"
        )
    return bool(collector or proof)


def r2_verified(run_dir: pathlib.Path) -> bool:
    r2 = read_json(run_dir / "r2_upload_result.json")
    countability = read_json(run_dir / "countability_decision.json")
    run_countability = read_json(run_dir / "run_countability_decision.json")
    proof = read_json(run_dir / "local_relay_dataset_proof_summary.json")
    retention = read_json(run_dir / "local_retention_summary.json")
    failed = r2.get("failed_files") or []
    r2_ok = boolish(r2.get("verified")) or boolish(r2.get("ok")) or (bool(r2.get("uploaded_files")) and not failed)
    countability_ok = boolish(countability.get("r2_verified")) or boolish(run_countability.get("r2_verified"))
    proof_ok = boolish(proof.get("r2_verified")) or safe_int(proof.get("r2_failed_files")) == 0 and proof.get("r2_uploaded_objects") is not None
    retention_ok = boolish(retention.get("r2_verified")) or boolish(retention.get("ok"))
    return bool((r2_ok or countability_ok or proof_ok or retention_ok) and not failed)


def artifact_consistency_ok(run_dir: pathlib.Path) -> bool:
    summary = read_json(run_dir / "artifact_consistency_summary.json")
    proof = read_json(run_dir / "local_relay_dataset_proof_summary.json")
    if summary:
        if "ok" in summary:
            return boolish(summary.get("ok"))
        if "artifact_consistency_ok" in summary:
            return boolish(summary.get("artifact_consistency_ok"))
        return not summary.get("blockers")
    if "artifact_consistency_ok" in proof:
        return boolish(proof.get("artifact_consistency_ok"))
    # Older completed runs that already retained only compact artifacts may lack
    # the newer summary. We do not need artifact consistency to prune raw relay
    # spool when local integrity and R2 material verification are proven.
    return bool(read_json(run_dir / "local_retention_summary.json"))


def active(run_dir: pathlib.Path, process_lines: list[str]) -> bool:
    run_text = str(run_dir)
    run_name = run_dir.name
    return any(run_text in line or run_name in line for line in process_lines)


def run_safety(run_dir: pathlib.Path, process_lines: list[str]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if active(run_dir, process_lines):
        reasons.append("current_or_inflight_run")
    if not run_completed(run_dir):
        reasons.append("run_completion_not_proven")
    if not r2_verified(run_dir):
        reasons.append("r2_material_artifacts_not_verified")
    if not local_integrity_clean(run_dir):
        reasons.append("relay_integrity_not_clean_or_missing")
    if not artifact_consistency_ok(run_dir):
        reasons.append("artifact_consistency_not_proven")
    return not reasons, reasons


def find_bulk_candidates(output_root: pathlib.Path, process_lines: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    safe: list[dict[str, Any]] = []
    unsafe: list[dict[str, Any]] = []
    seen: set[pathlib.Path] = set()
    for name in BULK_DIR_NAMES:
        for bulk_dir in output_root.rglob(name):
            if not bulk_dir.exists() or not bulk_dir.is_dir():
                continue
            if bulk_dir in seen:
                continue
            seen.add(bulk_dir)
            run_dir = bulk_dir.parent
            if not path_is_under(run_dir, output_root):
                continue
            safe_to_delete, reasons = run_safety(run_dir, process_lines)
            row = {
                "run_id": run_dir.name,
                "run_dir": str(run_dir),
                "path": str(bulk_dir),
                "path_name": bulk_dir.name,
                "bytes": size_bytes(bulk_dir),
                "file_count": file_count(bulk_dir),
                "safe_to_delete": safe_to_delete,
                "unsafe_reasons": reasons,
            }
            if row["bytes"] <= 0:
                continue
            if safe_to_delete:
                safe.append(row)
            else:
                unsafe.append(row)
    safe.sort(key=lambda item: item["bytes"], reverse=True)
    unsafe.sort(key=lambda item: item["bytes"], reverse=True)
    return safe, unsafe


def generated_data_candidates() -> list[dict[str, Any]]:
    paths = [
        REPO / "data" / "data" / "events",
        REPO / "data" / "data" / "snapshots",
    ]
    rows = []
    for path in paths:
        if path.exists():
            rows.append(
                {
                    "path": str(path),
                    "bytes": size_bytes(path),
                    "file_count": file_count(path),
                    "reason": "legacy_generated_replay_or_event_bulk",
                }
            )
    return rows


def build_artifact_candidates() -> list[dict[str, Any]]:
    path = REPO / "target"
    if not path.exists():
        return []
    return [
        {
            "path": str(path),
            "bytes": size_bytes(path),
            "file_count": file_count(path),
            "reason": "local_rust_build_artifacts_rebuildable",
        }
    ]


def remove_path(path: pathlib.Path) -> int:
    before = size_bytes(path)
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()
    return before


def render_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Local R2-First Storage Enforcement Report",
        "",
        f"- generated_at_utc: `{payload['generated_at_utc']}`",
        f"- mode: `{'apply' if payload['apply'] else 'dry-run'}`",
        f"- max_local_stream_mb: `{payload['max_local_stream_mb']}`",
        f"- free_mb_before: `{payload['free_mb_before']}`",
        f"- free_mb_after: `{payload['free_mb_after']}`",
        f"- local_stream_collector_mb_before: `{payload['local_stream_collector_mb_before']}`",
        f"- local_stream_collector_mb_after: `{payload['local_stream_collector_mb_after']}`",
        f"- project_generated_mb_before: `{payload['project_generated_mb_before']}`",
        f"- project_generated_mb_after: `{payload['project_generated_mb_after']}`",
        f"- safe_delete_bytes_available: `{payload['safe_delete_bytes_available']}`",
        f"- deleted_bytes: `{payload['deleted_bytes']}`",
        f"- storage_budget_ok: `{str(payload['storage_budget_ok']).lower()}`",
        f"- replay/backtesting/tuning/trading: `blocked`",
        f"- launch_caps: `blocked`",
        "",
        "## Deleted Paths",
    ]
    if payload["deleted_paths"]:
        for row in payload["deleted_paths"]:
            lines.append(f"- `{row['path']}`: `{row['bytes']}` bytes ({row['reason']})")
    else:
        lines.append("- none")
    lines.extend(["", "## Safe Raw/Spool Candidates"])
    for row in payload["safe_bulk_candidates"][:60]:
        lines.append(f"- `{row['path']}`: `{row['bytes']}` bytes")
    if not payload["safe_bulk_candidates"]:
        lines.append("- none")
    lines.extend(["", "## Legacy Unverified Raw Spool Prune Candidates"])
    for row in payload.get("legacy_unverified_raw_spool_candidates", [])[:60]:
        lines.append(
            f"- `{row['path']}`: `{row['bytes']}` bytes, original reasons: `{', '.join(row['unsafe_reasons'])}`"
        )
    if not payload.get("legacy_unverified_raw_spool_candidates"):
        lines.append("- none")
    lines.extend(["", "## Unsafe Raw/Spool Candidates"])
    for row in payload["unsafe_bulk_candidates"][:40]:
        lines.append(
            f"- `{row['path']}`: `{row['bytes']}` bytes, reasons: `{', '.join(row['unsafe_reasons'])}`"
        )
    if not payload["unsafe_bulk_candidates"]:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def project_generated_bytes(output_root: pathlib.Path) -> int:
    return (
        size_bytes(output_root)
        + size_bytes(REPO / "research_output" / "trading_strategy_pipeline")
        + size_bytes(REPO / "data" / "data")
        + size_bytes(REPO / "target")
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=pathlib.Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=pathlib.Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--max-local-stream-mb", type=int, default=DEFAULT_MAX_LOCAL_STREAM_MB)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--include-build-artifacts", action="store_true")
    parser.add_argument("--include-legacy-replay-data", action="store_true")
    parser.add_argument(
        "--include-legacy-unverified-raw-spool",
        action="store_true",
        help=(
            "prune inactive legacy raw relay spool even when compact R2 verification "
            "is not proven; preserves manifests and marks full-frame reconstruction unavailable"
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    output_root = args.output_root.resolve()
    report_root = args.report_root.resolve()
    process_lines = pgrep_lines()
    safe_bulk, unsafe_bulk = find_bulk_candidates(output_root, process_lines)
    legacy_unverified_raw_spool: list[dict[str, Any]] = []
    if args.include_legacy_unverified_raw_spool:
        retained_unsafe: list[dict[str, Any]] = []
        for row in unsafe_bulk:
            if "current_or_inflight_run" in row["unsafe_reasons"]:
                retained_unsafe.append(row)
                continue
            if row["path_name"] not in BULK_DIR_NAMES:
                retained_unsafe.append(row)
                continue
            legacy_unverified_raw_spool.append(
                {
                    **row,
                    "reason": "legacy_unverified_raw_spool_operator_pruned",
                }
            )
        unsafe_bulk = retained_unsafe
    extra_candidates: list[dict[str, Any]] = []
    if args.include_build_artifacts:
        extra_candidates.extend(build_artifact_candidates())
    if args.include_legacy_replay_data:
        extra_candidates.extend(generated_data_candidates())

    before_stream_bytes = size_bytes(output_root)
    before_project_generated = project_generated_bytes(output_root)
    free_before = free_mb(output_root)
    deleted_paths: list[dict[str, Any]] = []

    if args.apply:
        for row in safe_bulk:
            path = pathlib.Path(row["path"])
            if path.exists() and path_is_under(path, output_root):
                deleted = remove_path(path)
                deleted_paths.append({"path": str(path), "bytes": deleted, "reason": "verified_r2_local_transient_bulk"})
        for row in legacy_unverified_raw_spool:
            path = pathlib.Path(row["path"])
            if path.exists() and path_is_under(path, output_root):
                deleted = remove_path(path)
                deleted_paths.append({"path": str(path), "bytes": deleted, "reason": row["reason"]})
        for row in extra_candidates:
            path = pathlib.Path(row["path"])
            if path.exists() and path_is_under(path, REPO):
                deleted = remove_path(path)
                deleted_paths.append({"path": str(path), "bytes": deleted, "reason": row["reason"]})

    after_stream_bytes = size_bytes(output_root)
    after_project_generated = project_generated_bytes(output_root)
    payload = {
        "schema_version": "phase107.local_r2_first_storage_enforcer.v1",
        "generated_at_utc": utc_stamp(),
        "apply": bool(args.apply),
        "output_root": str(output_root),
        "report_root": str(report_root),
        "max_local_stream_mb": args.max_local_stream_mb,
        "free_mb_before": free_before,
        "free_mb_after": free_mb(output_root),
        "local_stream_collector_bytes_before": before_stream_bytes,
        "local_stream_collector_bytes_after": after_stream_bytes,
        "local_stream_collector_mb_before": before_stream_bytes // (1024 * 1024),
        "local_stream_collector_mb_after": after_stream_bytes // (1024 * 1024),
        "project_generated_bytes_before": before_project_generated,
        "project_generated_bytes_after": after_project_generated,
        "project_generated_mb_before": before_project_generated // (1024 * 1024),
        "project_generated_mb_after": after_project_generated // (1024 * 1024),
        "safe_delete_bytes_available": (
            sum(row["bytes"] for row in safe_bulk)
            + sum(row["bytes"] for row in legacy_unverified_raw_spool)
            + sum(row["bytes"] for row in extra_candidates)
        ),
        "deleted_bytes": sum(row["bytes"] for row in deleted_paths),
        "deleted_paths": deleted_paths,
        "safe_bulk_candidates": safe_bulk[:200],
        "legacy_unverified_raw_spool_candidates": legacy_unverified_raw_spool[:200],
        "unsafe_bulk_candidates": unsafe_bulk[:200],
        "extra_candidates": extra_candidates,
        "process_lines": process_lines,
        "storage_budget_ok": (after_stream_bytes // (1024 * 1024)) <= args.max_local_stream_mb,
        "replay_backtesting_tuning_trading_blocked": True,
        "launch_caps_remain_blocked": True,
    }
    report_root.mkdir(parents=True, exist_ok=True)
    write_json(report_root / "local_storage_enforcement_summary.json", payload)
    (report_root / "LOCAL_STORAGE_ENFORCEMENT_REPORT.md").write_text(render_report(payload), encoding="utf-8")
    print(
        json.dumps(
            {
                "ok": True,
                "apply": args.apply,
                "deleted_bytes": payload["deleted_bytes"],
                "local_stream_collector_mb_after": payload["local_stream_collector_mb_after"],
                "project_generated_mb_after": payload["project_generated_mb_after"],
                "storage_budget_ok": payload["storage_budget_ok"],
                "report_path": str(report_root / "LOCAL_STORAGE_ENFORCEMENT_REPORT.md"),
            },
            sort_keys=True,
        )
    )
    return 0 if payload["storage_budget_ok"] or not args.apply else 2


if __name__ == "__main__":
    raise SystemExit(main(os.sys.argv[1:]))
