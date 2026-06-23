#!/usr/bin/env python3
"""Audit and safely garbage collect local R2-primary collector bulk files.

The collector's durable material artifacts live in R2. This script only removes
verified local bulk/spool artifacts, such as raw relay frame shards, after the
run proves local integrity, R2 upload success, artifact consistency, and final
collector status. It is dry-run by default; pass --apply to delete.
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
DEFAULT_REPORT_ROOT = REPO / "research_output" / "trading_strategy_pipeline" / "background_24h_collector"
REQUIRED_COLLECTION_MB = 10_000
RECOMMENDED_RESUME_MB = 15_000
PREFERRED_24H_MB = 20_000
COMPACT_ARTIFACT_NAMES = {
    "countability_decision.json",
    "run_countability_decision.json",
    "local_relay_dataset_proof_summary.json",
    "r2_upload_result.json",
    "local_retention_summary.json",
    "service_exit_status.json",
    "hunter_summary.json",
    "attempt_ledger.csv",
    "rejected_summary.csv",
    "candidate_summary.csv",
    "all_launch_intake_ledger.csv",
    "all_launch_intake_summary.json",
    "all_launch_followup_manifest.json",
    "promotion_queue_ledger.csv",
    "promotion_queue_summary.json",
    "early_burst_review_candidate_summary.csv",
    "review_queue.csv",
    "slice_summaries.csv",
    "manifest.json",
    "stable_manifest_upload.json",
}
BULK_DIR_NAMES = {
    "relay_frames",
    "raw_relay_frames",
    "relay_frame_shards",
    ".r2_spool",
    "tmp_upload_shards",
    "upload_chunks",
}
NEVER_DELETE_PARTS = {
    ".git",
    ".codex_runtime_env",
    "config",
    "target",
    "scripts",
    "crates",
    ".ssh",
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


def file_size(path: pathlib.Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def tree_size(path: pathlib.Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return file_size(path)
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += file_size(item)
    return total


def file_count(path: pathlib.Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return 1
    return sum(1 for item in path.rglob("*") if item.is_file())


def free_mb(path: pathlib.Path) -> int:
    target = path if path.exists() else path.parent
    usage = shutil.disk_usage(target)
    return usage.free // (1024 * 1024)


def pgrep_lines() -> list[str]:
    proc = subprocess.run(
        ["pgrep", "-af", r"[l]ocal-stream-collector|[v]ps-stream-relay|[r]un_background_24h_collector|[r]un_relay_r2_primary_batch"],
        cwd=REPO,
        text=True,
        capture_output=True,
        timeout=30,
    )
    return [line for line in proc.stdout.splitlines() if line.strip()]


def path_is_under(path: pathlib.Path, root: pathlib.Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def is_never_delete_path(path: pathlib.Path) -> bool:
    parts = set(path.resolve().parts)
    return bool(parts & NEVER_DELETE_PARTS)


def r2_status(run_dir: pathlib.Path) -> dict[str, Any]:
    r2 = read_json(run_dir / "r2_upload_result.json")
    failed = r2.get("failed_files") or []
    uploaded = r2.get("uploaded_files") or []
    verified = r2.get("verified") is True or (r2 and not failed and bool(uploaded))
    verification_source = "verified_field" if r2.get("verified") is True else "legacy_empty_failed_files"
    if not r2:
        verification_source = "missing"
    return {
        "present": bool(r2),
        "verified": bool(verified),
        "verification_source": verification_source,
        "failed_files": len(failed),
        "uploaded_files": len(uploaded),
        "phase_prefix": r2.get("phase_prefix"),
    }


def retention_status(run_dir: pathlib.Path) -> dict[str, Any]:
    retention = read_json(run_dir / "local_retention_summary.json")
    return {
        "present": bool(retention),
        "ok": retention.get("ok") is True,
        "r2_verified": retention.get("r2_verified") is True,
        "deleted_bulk_bytes": safe_int(retention.get("deleted_bulk_bytes")),
        "local_retained_bytes": safe_int(retention.get("local_retained_bytes")),
    }


def service_status(run_dir: pathlib.Path) -> dict[str, Any]:
    service = read_json(run_dir / "service_exit_status.json")
    collector = read_json(run_dir / "local_collector_summary.json")
    completed = (
        service.get("hunter_exit_status") == 0
        or service.get("service_exit_reason") == "local_relay_collector_completed"
        or collector.get("frames_received") is not None
    )
    return {
        "service_exit_status_present": bool(service),
        "local_collector_summary_present": bool(collector),
        "completed": bool(completed),
        "service_exit_reason": service.get("service_exit_reason"),
        "hunter_exit_status": service.get("hunter_exit_status"),
    }


def local_integrity_status(run_dir: pathlib.Path) -> dict[str, Any]:
    collector = read_json(run_dir / "local_collector_summary.json")
    summary = read_json(run_dir / "local_relay_dataset_proof_summary.json")
    source = collector or summary
    metrics = {
        "sequence_gap_count": safe_int(source.get("sequence_gap_count")),
        "hash_mismatch_count": safe_int(source.get("hash_mismatch_count")),
        "malformed_frame_count": safe_int(source.get("malformed_frame_count")),
        "receiver_backpressure_count": safe_int(
            source.get("receiver_backpressure_count", source.get("downstream_backpressure_count", 0))
        ),
        "receiver_unavailable_count": safe_int(source.get("receiver_unavailable_count")),
    }
    metrics["clean"] = all(value == 0 for value in metrics.values())
    return metrics


def artifact_consistency_status(run_dir: pathlib.Path, *, run_validator: bool) -> dict[str, Any]:
    summary = read_json(run_dir / "artifact_consistency_summary.json")
    if summary:
        ok = summary.get("ok")
        if ok is None:
            ok = summary.get("artifact_consistency_ok")
        blockers = summary.get("blockers") or []
        return {"present": True, "ok": ok is True or (ok is None and not blockers), "blockers": blockers, "source": "summary"}
    embedded = read_json(run_dir / "local_relay_dataset_proof_summary.json").get("artifact_consistency_ok")
    if embedded is True:
        return {"present": False, "ok": True, "blockers": [], "source": "proof_summary"}
    if run_validator and (REPO / "target/release/cli").exists():
        proc = subprocess.run(
            [
                "target/release/cli",
                "validate-material-hunter-artifacts",
                "--output-dir",
                str(run_dir),
                "--json",
            ],
            cwd=REPO,
            text=True,
            capture_output=True,
            timeout=120,
        )
        parsed: dict[str, Any] = {}
        if proc.stdout.strip():
            try:
                parsed = json.loads(proc.stdout)
            except json.JSONDecodeError:
                parsed = {"parse_error": proc.stdout[:1000]}
        blockers = parsed.get("blockers") or []
        return {
            "present": False,
            "ok": proc.returncode == 0 and not blockers,
            "blockers": blockers,
            "source": "validator",
            "validator_returncode": proc.returncode,
        }
    return {"present": False, "ok": False, "blockers": ["artifact_consistency_not_proven"], "source": "missing"}


def active_run_names(process_lines: list[str]) -> set[str]:
    names: set[str] = set()
    for line in process_lines:
        for token in line.split():
            if "local_stream_collector" not in token:
                continue
            path = pathlib.Path(token)
            if path.name == "local_stream_collector":
                continue
            names.add(path.name)
    return names


def deletion_paths(run_dir: pathlib.Path) -> list[pathlib.Path]:
    paths: list[pathlib.Path] = []
    for name in BULK_DIR_NAMES:
        candidate = run_dir / name
        if candidate.exists() and tree_size(candidate) > 0:
            paths.append(candidate)
    return paths


def directory_children_size(root: pathlib.Path, predicate) -> int:
    if not root.exists():
        return 0
    return sum(tree_size(path) for path in root.iterdir() if predicate(path))


def find_largest_paths(root: pathlib.Path, limit: int = 50) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if not root.exists():
        return items
    for child in root.iterdir():
        items.append(
            {
                "path": str(child),
                "bytes": tree_size(child),
                "kind": "dir" if child.is_dir() else "file",
            }
        )
    return sorted(items, key=lambda item: item["bytes"], reverse=True)[:limit]


def find_operator_review_candidates(output_root: pathlib.Path, report_root: pathlib.Path) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for root in (output_root, report_root, REPO / "research_output"):
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if is_never_delete_path(path):
                continue
            if path.is_file() and path.suffix == ".zip":
                candidates.append(
                    {
                        "path": str(path),
                        "bytes": file_size(path),
                        "tier": "C",
                        "reason": "old_archive_zip_operator_review_only",
                    }
                )
            elif path.is_file() and path.suffix in {".log", ".err", ".out"}:
                candidates.append(
                    {
                        "path": str(path),
                        "bytes": file_size(path),
                        "tier": "C",
                        "reason": "old_log_operator_review_only",
                    }
                )
    return sorted(candidates, key=lambda item: item["bytes"], reverse=True)


def assess_run(
    run_dir: pathlib.Path,
    *,
    output_root: pathlib.Path,
    active_names: set[str],
    run_validator: bool,
) -> dict[str, Any]:
    size = tree_size(run_dir)
    relay_frames = run_dir / "relay_frames"
    relay_frame_size = tree_size(relay_frames)
    r2 = r2_status(run_dir)
    retention = retention_status(run_dir)
    service = service_status(run_dir)
    integrity = local_integrity_status(run_dir)
    artifacts = artifact_consistency_status(run_dir, run_validator=run_validator) if r2["verified"] else {
        "ok": False,
        "blockers": ["r2_not_verified"],
        "source": "skipped",
    }
    active = run_dir.name in active_names
    paths = deletion_paths(run_dir)
    if (
        not artifacts["ok"]
        and retention["ok"]
        and retention["r2_verified"]
        and r2["verified"]
        and service["completed"]
        and integrity["clean"]
        and paths
        and all(path.name == "relay_frames" for path in paths)
    ):
        artifacts = {
            "present": False,
            "ok": True,
            "blockers": [],
            "source": "legacy_retention_ok_relay_frames_only",
            "legacy_validator_blockers": artifacts.get("blockers") or [],
        }
    tier = ""
    reasons: list[str] = []
    if active:
        reasons.append("current_or_inflight_run")
    if not path_is_under(run_dir, output_root):
        reasons.append("outside_output_root")
    if is_never_delete_path(run_dir):
        reasons.append("never_delete_path")
    if not r2["present"]:
        reasons.append("missing_r2_upload_result")
    elif not r2["verified"]:
        reasons.append("r2_not_verified")
    if not artifacts["ok"]:
        reasons.extend(str(item) for item in (artifacts.get("blockers") or ["artifact_consistency_not_proven"]))
    if not service["completed"]:
        reasons.append("run_not_final_or_completion_not_proven")
    if not integrity["clean"]:
        reasons.append("local_relay_integrity_not_clean")
    if not paths:
        reasons.append("no_bulk_paths_present")
    if not retention["ok"] and paths and any(path.name != "relay_frames" for path in paths):
        reasons.append("retention_missing_for_non_relay_bulk")
    safe = not reasons
    # Tier A: locally clean, inactive raw relay frame shards with verified R2
    # material upload are safe transient bulk even when legacy runs fail newer
    # all-launch artifact checks that did not exist when they were collected.
    if not safe and paths and all(path.name == "relay_frames" for path in paths):
        tier_a_reasons = []
        if active:
            tier_a_reasons.append("current_or_inflight_run")
        if not r2["verified"]:
            tier_a_reasons.append("r2_not_verified")
        if not service["completed"]:
            tier_a_reasons.append("run_not_final_or_completion_not_proven")
        if not integrity["clean"]:
            tier_a_reasons.append("local_relay_integrity_not_clean")
        if is_never_delete_path(run_dir):
            tier_a_reasons.append("never_delete_path")
        if not tier_a_reasons:
            safe = True
            tier = "A"
            reasons = []
    if safe and not tier:
        tier = "B" if artifacts.get("source") not in {"summary", "proof_summary"} else "A"
    return {
        "run_id": run_dir.name,
        "path": str(run_dir),
        "total_bytes": size,
        "relay_frames_local_present": relay_frames.exists(),
        "relay_frames_local_file_count": file_count(relay_frames),
        "relay_frames_local_total_bytes": relay_frame_size,
        "r2": r2,
        "retention": retention,
        "service": service,
        "local_integrity": integrity,
        "artifact_consistency": artifacts,
        "active": active,
        "bulk_delete_paths": [
            {"path": str(path), "bytes": tree_size(path), "file_count": file_count(path)} for path in paths
        ],
        "gc_tier": tier,
        "safe_to_delete": safe,
        "unsafe_reasons": sorted(set(reasons)),
        "safe_delete_bytes": sum(tree_size(path) for path in paths) if safe else 0,
    }


def build_audit(args: argparse.Namespace) -> dict[str, Any]:
    output_root = args.output_root.resolve()
    report_root = args.report_root.resolve()
    include_operator_review_tier = bool(getattr(args, "include_operator_review_tier", False))
    skip_validator = bool(getattr(args, "skip_validator", False))
    process_lines = pgrep_lines()
    active_names = active_run_names(process_lines)
    run_rows: list[dict[str, Any]] = []
    for run_dir in sorted(output_root.iterdir() if output_root.exists() else []):
        if not run_dir.is_dir() or run_dir.name.startswith(".") or run_dir.name.endswith("-logs"):
            continue
        run_rows.append(
            assess_run(
                run_dir,
                output_root=output_root,
                active_names=active_names,
                run_validator=not skip_validator,
            )
        )
    safe = [row for row in run_rows if row["safe_to_delete"]]
    unsafe = [row for row in run_rows if not row["safe_to_delete"]]
    largest = sorted(run_rows, key=lambda row: row["total_bytes"], reverse=True)[:25]
    operator_review = find_operator_review_candidates(output_root, report_root)
    if include_operator_review_tier:
        for item in operator_review:
            safe.append(
                {
                    "run_id": pathlib.Path(item["path"]).name,
                    "path": item["path"],
                    "total_bytes": item["bytes"],
                    "relay_frames_local_total_bytes": 0,
                    "r2": {"verified": False, "present": False},
                    "retention": {"ok": False},
                    "service": {"completed": False},
                    "local_integrity": {"clean": False},
                    "artifact_consistency": {"ok": False},
                    "active": False,
                    "bulk_delete_paths": [{"path": item["path"], "bytes": item["bytes"], "file_count": 1}],
                    "gc_tier": "C",
                    "safe_to_delete": True,
                    "unsafe_reasons": [],
                    "safe_delete_bytes": item["bytes"],
                }
            )
    return {
        "schema_version": "phase107k.local_r2_primary_storage_gc.v2",
        "generated_at_utc": utc_stamp(),
        "apply": bool(args.apply),
        "operator_review_tier_included": include_operator_review_tier,
        "output_root": str(output_root),
        "report_root": str(report_root),
        "local_free_mb": free_mb(output_root),
        "required_r2_primary_collection_mb": args.min_free_mb,
        "recommended_resume_mb": RECOMMENDED_RESUME_MB,
        "preferred_24h_mb": PREFERRED_24H_MB,
        "project_root_total_bytes": tree_size(REPO),
        "research_output_total_bytes": tree_size(REPO / "research_output"),
        "research_output_local_stream_collector_total_bytes": tree_size(output_root),
        "background_24h_collector_total_bytes": tree_size(report_root),
        "old_proof_dirs_total_bytes": directory_children_size(output_root, lambda path: path.is_dir() and "proof" in path.name),
        "old_batch_dirs_total_bytes": directory_children_size(output_root, lambda path: path.is_dir() and "batch" in path.name),
        "archived_status_and_zip_total_bytes": sum(item["bytes"] for item in operator_review if "zip" in item["reason"])
        + tree_size(report_root / "archived_status_20260623T002640Z"),
        "relay_frame_shard_total_bytes": sum(row["relay_frames_local_total_bytes"] for row in run_rows),
        "raw_bulk_artifact_total_bytes": sum(
            sum(item["bytes"] for item in row["bulk_delete_paths"]) for row in run_rows
        ),
        "verified_compact_artifact_total_bytes": sum(
            row["total_bytes"] - row["relay_frames_local_total_bytes"] for row in run_rows if row["r2"]["verified"]
        ),
        "unverified_artifact_total_bytes": sum(row["total_bytes"] for row in run_rows if not row["r2"]["verified"]),
        "logs_total_bytes": sum(item["bytes"] for item in operator_review if "log" in item["reason"]),
        "local_r2_spool_total_bytes": tree_size(output_root / ".r2_spool"),
        "system_temp_project_total_bytes": tree_size(pathlib.Path("/tmp") / "pump_relay_r2_primary_batch"),
        "verified_completed_slice_dirs": len(
            [row for row in run_rows if row["r2"]["verified"] and row["service"]["completed"]]
        ),
        "unverified_slice_dirs": len([row for row in run_rows if not row["r2"]["verified"]]),
        "current_or_inflight_dirs": [row["run_id"] for row in run_rows if row["active"]],
        "failed_orchestration_or_proof_dirs": [
            row["run_id"]
            for row in run_rows
            if not row["r2"]["verified"] or not row["service"]["completed"] or not row["artifact_consistency"]["ok"]
        ],
        "largest_directories": [
            {
                "run_id": row["run_id"],
                "total_bytes": row["total_bytes"],
                "relay_frames_local_total_bytes": row["relay_frames_local_total_bytes"],
                "safe_to_delete": row["safe_to_delete"],
                "unsafe_reasons": row["unsafe_reasons"],
            }
            for row in largest
        ],
        "top_50_largest_project_output_paths": find_largest_paths(REPO / "research_output", 50),
        "safe_to_delete_candidates": safe,
        "unsafe_to_delete_candidates": unsafe,
        "verified_but_not_covered_by_current_gc": [
            row
            for row in unsafe
            if row["r2"]["verified"] and row["relay_frames_local_total_bytes"] > 0
        ],
        "operator_review_tier_candidates": operator_review[:100],
        "safe_delete_bytes": sum(row["safe_delete_bytes"] for row in safe),
        "safe_delete_file_count": sum(
            sum(item["file_count"] for item in row["bulk_delete_paths"]) for row in safe
        ),
        "process_lines": process_lines,
        "replay_backtesting_tuning_trading_blocked": True,
        "launch_caps_remain_blocked": True,
    }


def render_report(audit: dict[str, Any], deleted_bytes: int = 0) -> str:
    safe = audit["safe_to_delete_candidates"]
    unsafe = audit["unsafe_to_delete_candidates"]
    lines = [
        "# Local R2-Primary Storage GC Report",
        "",
        f"- generated_at_utc: `{audit['generated_at_utc']}`",
        f"- mode: `{'apply' if audit['apply'] else 'dry-run'}`",
        f"- local_free_mb: `{audit['local_free_mb']}`",
        f"- required_r2_primary_collection_mb: `{audit['required_r2_primary_collection_mb']}`",
        f"- recommended_resume_mb: `{audit.get('recommended_resume_mb', RECOMMENDED_RESUME_MB)}`",
        f"- preferred_24h_mb: `{audit.get('preferred_24h_mb', PREFERRED_24H_MB)}`",
        f"- project_root_total_bytes: `{audit.get('project_root_total_bytes', '')}`",
        f"- research_output_total_bytes: `{audit.get('research_output_total_bytes', '')}`",
        f"- local_stream_collector_total_bytes: `{audit['research_output_local_stream_collector_total_bytes']}`",
        f"- background_24h_collector_total_bytes: `{audit['background_24h_collector_total_bytes']}`",
        f"- old_proof_dirs_total_bytes: `{audit.get('old_proof_dirs_total_bytes', '')}`",
        f"- old_batch_dirs_total_bytes: `{audit.get('old_batch_dirs_total_bytes', '')}`",
        f"- archived_status_and_zip_total_bytes: `{audit.get('archived_status_and_zip_total_bytes', '')}`",
        f"- relay_frame_shard_total_bytes: `{audit['relay_frame_shard_total_bytes']}`",
        f"- raw_bulk_artifact_total_bytes: `{audit.get('raw_bulk_artifact_total_bytes', '')}`",
        f"- verified_compact_artifact_total_bytes: `{audit.get('verified_compact_artifact_total_bytes', '')}`",
        f"- unverified_artifact_total_bytes: `{audit.get('unverified_artifact_total_bytes', '')}`",
        f"- logs_total_bytes: `{audit.get('logs_total_bytes', '')}`",
        f"- local_r2_spool_total_bytes: `{audit.get('local_r2_spool_total_bytes', '')}`",
        f"- system_temp_project_total_bytes: `{audit.get('system_temp_project_total_bytes', '')}`",
        f"- safe_delete_bytes: `{audit['safe_delete_bytes']}`",
        f"- deleted_bytes: `{deleted_bytes}`",
        f"- current_or_inflight_dirs: `{', '.join(audit['current_or_inflight_dirs']) or 'none'}`",
        f"- replay/backtesting/tuning/trading: `blocked`",
        f"- launch_caps: `blocked`",
        "",
        "## Safe-To-Delete Candidates",
    ]
    for row in sorted(safe, key=lambda item: item["safe_delete_bytes"], reverse=True)[:40]:
        lines.append(
            f"- `{row['run_id']}` tier `{row.get('gc_tier','')}`: `{row['safe_delete_bytes']}` bytes from "
            f"`{', '.join(pathlib.Path(item['path']).name for item in row['bulk_delete_paths'])}`"
        )
    if not safe:
        lines.append("- none")
    lines.extend(["", "## Largest Unsafe Candidates"])
    for row in sorted(unsafe, key=lambda item: item["total_bytes"], reverse=True)[:40]:
        lines.append(
            f"- `{row['run_id']}`: `{row['total_bytes']}` bytes, reasons: "
            f"`{', '.join(row['unsafe_reasons'])}`"
        )
    lines.extend(["", "## Operator-Review Tier C Candidates"])
    for item in (audit.get("operator_review_tier_candidates") or [])[:40]:
        lines.append(f"- `{item['path']}`: `{item['bytes']}` bytes, reason `{item['reason']}`")
    return "\n".join(lines) + "\n"


def apply_deletions(audit: dict[str, Any], *, output_root: pathlib.Path) -> list[dict[str, Any]]:
    deleted: list[dict[str, Any]] = []
    for row in audit["safe_to_delete_candidates"]:
        for item in row["bulk_delete_paths"]:
            path = pathlib.Path(item["path"])
            if not path_is_under(path, output_root) or is_never_delete_path(path):
                continue
            before = tree_size(path)
            if path.exists():
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
            deleted.append(
                {
                    "run_id": row["run_id"],
                    "path": str(path),
                    "bytes": before,
                    "file_count": item.get("file_count", 0),
                }
            )
    return deleted


def write_outputs(args: argparse.Namespace, audit: dict[str, Any], deleted: list[dict[str, Any]]) -> None:
    report_root = args.report_root
    report_root.mkdir(parents=True, exist_ok=True)
    write_json(report_root / "local_storage_audit.json", audit)
    (report_root / "LOCAL_STORAGE_AUDIT.md").write_text(render_report(audit), encoding="utf-8")
    write_json(report_root / "local_storage_capacity_audit_v2.json", audit)
    (report_root / "LOCAL_STORAGE_CAPACITY_AUDIT_V2.md").write_text(render_report(audit), encoding="utf-8")
    if args.apply:
        apply_payload = {
            **audit,
            "deleted_paths": deleted,
            "deleted_bytes": sum(item["bytes"] for item in deleted),
            "free_mb_after": free_mb(args.output_root),
        }
        write_json(report_root / "local_r2_primary_storage_gc_apply.json", apply_payload)
        write_json(report_root / "local_r2_primary_storage_gc_v2_apply.json", apply_payload)
        (report_root / "LOCAL_R2_PRIMARY_STORAGE_GC_REPORT.md").write_text(
            render_report(apply_payload, deleted_bytes=apply_payload["deleted_bytes"]),
            encoding="utf-8",
        )
        (report_root / "LOCAL_R2_PRIMARY_STORAGE_GC_V2_REPORT.md").write_text(
            render_report(apply_payload, deleted_bytes=apply_payload["deleted_bytes"]),
            encoding="utf-8",
        )
    else:
        write_json(report_root / "local_r2_primary_storage_gc_dry_run.json", audit)
        write_json(report_root / "local_r2_primary_storage_gc_v2_dry_run.json", audit)
        (report_root / "LOCAL_R2_PRIMARY_STORAGE_GC_REPORT.md").write_text(
            render_report(audit),
            encoding="utf-8",
        )
        (report_root / "LOCAL_R2_PRIMARY_STORAGE_GC_V2_REPORT.md").write_text(
            render_report(audit),
            encoding="utf-8",
        )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=pathlib.Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=pathlib.Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--min-free-mb", type=int, default=REQUIRED_COLLECTION_MB)
    parser.add_argument("--apply", action="store_true", help="delete safe verified bulk artifacts")
    parser.add_argument(
        "--include-operator-review-tier",
        action="store_true",
        help="include Tier C operator-review candidates such as logs/zips in deletion set",
    )
    parser.add_argument("--skip-validator", action="store_true", help="use stored artifact summaries only")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    args.output_root = args.output_root.resolve()
    args.report_root = args.report_root.resolve()
    audit = build_audit(args)
    deleted: list[dict[str, Any]] = []
    if args.apply:
        deleted = apply_deletions(audit, output_root=args.output_root)
        audit["free_mb_after"] = free_mb(args.output_root)
    write_outputs(args, audit, deleted)
    print(json.dumps({"ok": True, "apply": args.apply, "safe_delete_bytes": audit["safe_delete_bytes"], "deleted_bytes": sum(item["bytes"] for item in deleted), "free_mb": free_mb(args.output_root)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(os.sys.argv[1:]))
