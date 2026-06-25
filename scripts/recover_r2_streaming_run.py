#!/usr/bin/env python3
"""Recover and audit an interrupted R2-streaming local collector run.

This script is intentionally conservative:
- it never deletes unverified local chunks;
- it does not print secrets;
- it reconstructs recovery state from compact local manifests;
- optional retry is fail-closed unless an approved uploader is configured.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import time
from typing import Any


MANIFEST_NAMES = (
    "r2_streaming_upload_manifest.json",
    "artifact_stream_manifest.json",
    "relay_frame_manifest.json",
    "material_artifact_manifest.json",
    "local_spool_manifest.json",
    "r2_upload_result.json",
    "countability_decision.json",
    "run_countability_decision.json",
)


def utc_stamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def read_json(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"parse_error": str(path)}


def write_json(path: pathlib.Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def file_sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def path_size(path: pathlib.Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def streaming_shards(run_dir: pathlib.Path) -> list[dict[str, Any]]:
    relay = read_json(run_dir / "relay_frame_manifest.json")
    return list(relay.get("streaming_shards") or [])


def _mark_verified_entries(
    entries: list[dict[str, Any]],
    *,
    object_key: str,
    delete_verified_local: bool,
    deferred_delete_paths: set[pathlib.Path] | None = None,
) -> int:
    changed = 0
    for shard in entries:
        local_path = pathlib.Path(str(shard.get("local_path") or ""))
        entry_key = str(shard.get("object_key") or object_key)
        if entry_key != object_key and shard.get("object_key"):
            continue
        if shard.get("verified") is True and shard.get("local_deleted") is True:
            continue
        if not local_path.exists():
            continue
        expected_sha = str(shard.get("sha256") or "")
        actual_sha = file_sha256(local_path)
        if expected_sha and expected_sha != actual_sha:
            raise ValueError(
                f"refusing to mark verified chunk with sha mismatch: {local_path}"
            )
        shard["object_key"] = object_key
        shard["uploaded"] = True
        shard["verified"] = True
        shard["error"] = None
        if delete_verified_local:
            shard["local_deleted"] = True
            if deferred_delete_paths is not None:
                deferred_delete_paths.add(local_path)
        else:
            shard["local_deleted"] = False
        changed += 1
    return changed


def apply_verified_upload(
    run_dir: pathlib.Path,
    *,
    object_key: str,
    delete_verified_local: bool,
) -> dict[str, Any]:
    """Mark retained chunks verified after an independently verified R2 upload.

    This never fabricates verification: callers must pass an object key only
    after the upload tool has reported uploaded=true and verified=true.
    """
    relay_path = run_dir / "relay_frame_manifest.json"
    relay = read_json(relay_path)
    relay_entries = list(relay.get("streaming_shards") or [])
    deferred_delete_paths: set[pathlib.Path] = set()
    changed = _mark_verified_entries(
        relay_entries,
        object_key=object_key,
        delete_verified_local=delete_verified_local,
        deferred_delete_paths=deferred_delete_paths,
    )
    if changed == 0:
        raise ValueError("no retained local streaming chunks matched the verified object key")
    relay["streaming_shards"] = relay_entries
    write_json(relay_path, relay)

    artifact_path = run_dir / "artifact_stream_manifest.json"
    artifact = read_json(artifact_path)
    artifact_entries = list(artifact.get("relay_frame_chunks") or [])
    _mark_verified_entries(
        artifact_entries,
        object_key=object_key,
        delete_verified_local=delete_verified_local,
        deferred_delete_paths=deferred_delete_paths,
    )
    artifact["relay_frame_chunks"] = artifact_entries
    write_json(artifact_path, artifact)

    for path in sorted(deferred_delete_paths):
        if path.exists():
            path.unlink()

    uploaded = sum(1 for entry in relay_entries if entry.get("uploaded") is True)
    verified = sum(1 for entry in relay_entries if entry.get("verified") is True)
    deleted = sum(1 for entry in relay_entries if entry.get("local_deleted") is True)
    unverified = sum(1 for entry in relay_entries if entry.get("verified") is not True)
    local_spool_bytes = path_size(run_dir / "relay_frames")
    local_retained_bytes = path_size(run_dir)

    stream_path = run_dir / "r2_streaming_upload_manifest.json"
    stream = read_json(stream_path)
    blockers = [
        blocker
        for blocker in stream.get("blockers", [])
        if blocker != "R2_STREAMING_BLOCK_UNVERIFIED_RELAY_SHARD"
    ]
    if unverified:
        blockers.append("R2_STREAMING_BLOCK_UNVERIFIED_RELAY_SHARD")
    stream.update(
        {
            "ok": unverified == 0 and not blockers,
            "uploaded_chunks": uploaded,
            "verified_chunks": verified,
            "deleted_local_chunks": deleted,
            "unverified_chunks": unverified,
            "blockers": blockers,
            "local_spool_bytes_current": local_spool_bytes,
            "local_retained_bytes": local_retained_bytes,
            "recovery_applied_at_utc": utc_stamp(),
            "recovery_verified_object_key": object_key,
        }
    )
    write_json(stream_path, stream)

    spool_path = run_dir / "local_spool_manifest.json"
    spool = read_json(spool_path)
    spool.update(
        {
            "verified_chunks_deleted_local": deleted,
            "unverified_chunks_retained_local": unverified,
            "local_spool_bytes_current": local_spool_bytes,
            "local_retained_bytes": local_retained_bytes,
            "recovery_applied_at_utc": utc_stamp(),
        }
    )
    write_json(spool_path, spool)

    retention_path = run_dir / "local_retention_summary.json"
    retention = read_json(retention_path)
    retention["skipped_paths"] = [
        item
        for item in retention.get("skipped_paths", [])
        if item.get("reason") != "r2_streaming_unverified_relay_chunks_retained"
    ]
    retention.update(
        {
            "local_retained_bytes": local_retained_bytes,
            "r2_streaming_recovery_applied": True,
            "r2_streaming_recovery_verified_object_key": object_key,
            "r2_streaming_recovery_deleted_local": delete_verified_local,
        }
    )
    write_json(retention_path, retention)

    for name in ("local_collector_summary.json", "local_relay_dataset_proof_summary.json"):
        path = run_dir / name
        if not path.exists():
            continue
        data = read_json(path)
        data.update(
            {
                "r2_streaming_uploaded_chunks": uploaded,
                "r2_streaming_verified_chunks": verified,
                "r2_streaming_deleted_local_chunks": deleted,
                "r2_streaming_unverified_chunks": unverified,
                "local_spool_bytes_current": local_spool_bytes,
            }
        )
        if name == "local_relay_dataset_proof_summary.json":
            data["local_retained_bytes_final"] = local_retained_bytes
            data["local_retention_summary"] = retention
        write_json(path, data)

    return {
        "verified_marked_chunks": changed,
        "uploaded_chunks": uploaded,
        "verified_chunks": verified,
        "deleted_local_chunks": deleted,
        "unverified_chunks": unverified,
        "local_spool_bytes_current": local_spool_bytes,
        "local_retained_bytes": local_retained_bytes,
    }


def classify_recovery(summary: dict[str, Any]) -> str:
    if summary.get("retry_requested") and summary["unverified_local_chunk_count"] > 0:
        if not summary.get("r2_health_verified"):
            return "R2_STREAMING_RECOVERY_RETRY_BLOCKED_R2_HEALTH"
        if not summary.get("retry_uploader_available"):
            return "R2_STREAMING_RECOVERY_RETRY_BLOCKED_NO_UPLOADER"
    if summary["unverified_local_chunk_count"] > 0:
        return "R2_STREAMING_RECOVERY_UNVERIFIED_LOCAL_CHUNKS_RETAINED"
    if summary["missing_manifest_count"] > 0:
        return "R2_STREAMING_RECOVERY_INCOMPLETE_MANIFESTS"
    if summary["verified_deleted_chunk_count"] == summary["streaming_chunk_count"]:
        return "R2_STREAMING_RECOVERY_CLEAN_COMPACT_MANIFESTS_ONLY"
    return "R2_STREAMING_RECOVERY_NEEDS_MANUAL_REVIEW"


def build_summary(
    run_dir: pathlib.Path,
    *,
    retry_requested: bool = False,
    r2_health_verified: bool = False,
    retry_uploader: str | None = None,
    recovery_update: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifests = {name: read_json(run_dir / name) for name in MANIFEST_NAMES}
    missing = [name for name in MANIFEST_NAMES if not (run_dir / name).exists()]
    shards = streaming_shards(run_dir)
    unverified_local: list[dict[str, Any]] = []
    verified_deleted = 0
    verified_local_present = 0
    for shard in shards:
        local_path = pathlib.Path(str(shard.get("local_path") or ""))
        local_exists = local_path.exists()
        verified = shard.get("verified") is True
        local_deleted = shard.get("local_deleted") is True
        if verified and local_deleted:
            verified_deleted += 1
        elif verified and local_exists:
            verified_local_present += 1
        elif not verified and local_exists:
            unverified_local.append(
                {
                    "part_index": shard.get("part_index"),
                    "local_path": str(local_path),
                    "bytes": path_size(local_path),
                    "object_key": shard.get("object_key"),
                    "sha256": shard.get("sha256"),
                }
            )
    r2_streaming = manifests.get("r2_streaming_upload_manifest.json") or {}
    summary = {
        "schema_version": "phase107m.r2_streaming_recovery_summary.v1",
        "generated_at_utc": utc_stamp(),
        "run_dir": str(run_dir),
        "storage_mode": r2_streaming.get("storage_mode") or manifests["relay_frame_manifest.json"].get("storage_mode"),
        "manifest_paths_present": [name for name in MANIFEST_NAMES if (run_dir / name).exists()],
        "missing_manifests": missing,
        "missing_manifest_count": len(missing),
        "streaming_chunk_count": len(shards),
        "verified_deleted_chunk_count": verified_deleted,
        "verified_local_present_chunk_count": verified_local_present,
        "unverified_local_chunk_count": len(unverified_local),
        "unverified_local_chunks": unverified_local,
        "local_spool_bytes_current": path_size(run_dir / "relay_frames"),
        "local_retained_bytes": path_size(run_dir),
        "r2_verified": (manifests.get("r2_upload_result.json") or {}).get("verified") is True,
        "r2_failed_files": len((manifests.get("r2_upload_result.json") or {}).get("failed_files") or []),
        "replay_allowed": False,
        "formal_backtesting_allowed": False,
        "threshold_tuning_allowed": False,
        "live_trading_enabled": False,
        "wallet_execution_enabled": False,
        "safe_to_delete_unverified_local_chunks": False,
        "retry_requested": retry_requested,
        "r2_health_verified": r2_health_verified,
        "retry_uploader": retry_uploader or "",
        "retry_uploader_available": bool(retry_uploader),
        "retry_performed": False,
        "retry_blocked_reason": "",
        "recovery_update": recovery_update or {},
    }
    if recovery_update:
        summary["retry_performed"] = True
    summary["classification"] = classify_recovery(summary)
    if summary["classification"] == "R2_STREAMING_RECOVERY_RETRY_BLOCKED_R2_HEALTH":
        summary["retry_blocked_reason"] = "r2_health_not_verified"
    elif summary["classification"] == "R2_STREAMING_RECOVERY_RETRY_BLOCKED_NO_UPLOADER":
        summary["retry_blocked_reason"] = "retry_uploader_not_configured"
    summary["ok"] = summary["classification"] == "R2_STREAMING_RECOVERY_CLEAN_COMPACT_MANIFESTS_ONLY"
    return summary


def write_report(report_root: pathlib.Path, summary: dict[str, Any]) -> None:
    report_root.mkdir(parents=True, exist_ok=True)
    (report_root / "r2_streaming_recovery_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# R2 Streaming Recovery Report",
        "",
        f"- classification: `{summary['classification']}`",
        f"- run_dir: `{summary['run_dir']}`",
        f"- storage_mode: `{summary.get('storage_mode')}`",
        f"- streaming_chunk_count: `{summary['streaming_chunk_count']}`",
        f"- verified_deleted_chunk_count: `{summary['verified_deleted_chunk_count']}`",
        f"- verified_local_present_chunk_count: `{summary['verified_local_present_chunk_count']}`",
        f"- unverified_local_chunk_count: `{summary['unverified_local_chunk_count']}`",
        f"- retry_requested: `{str(summary['retry_requested']).lower()}`",
        f"- r2_health_verified: `{str(summary['r2_health_verified']).lower()}`",
        f"- retry_uploader_available: `{str(summary['retry_uploader_available']).lower()}`",
        f"- retry_performed: `{str(summary['retry_performed']).lower()}`",
        f"- retry_blocked_reason: `{summary['retry_blocked_reason']}`",
        f"- local_spool_bytes_current: `{summary['local_spool_bytes_current']}`",
        f"- local_retained_bytes: `{summary['local_retained_bytes']}`",
        f"- r2_verified: `{str(summary['r2_verified']).lower()}`",
        f"- r2_failed_files: `{summary['r2_failed_files']}`",
        f"- missing_manifests: `{', '.join(summary['missing_manifests']) or 'none'}`",
        "",
        "Unverified local chunks are retained. This script does not delete them.",
        "Retry mode is fail-closed unless R2 health is verified and an approved uploader is configured.",
        "Replay, backtesting, tuning, trading, and wallet execution remain disabled.",
    ]
    (report_root / "R2_STREAMING_RECOVERY_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=pathlib.Path, required=True)
    parser.add_argument("--report-root", type=pathlib.Path, default=None)
    parser.add_argument("--retry-unverified", action="store_true")
    parser.add_argument("--r2-health-verified", action="store_true")
    parser.add_argument(
        "--mark-verified-object-key",
        default="",
        help="object key from a separately verified upload; updates manifests without printing secrets",
    )
    parser.add_argument(
        "--delete-verified-local",
        action="store_true",
        help="delete retained local chunks only after --mark-verified-object-key verification",
    )
    parser.add_argument(
        "--retry-uploader",
        default="",
        help="approved uploader identifier; no secrets or command lines are accepted here",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = args.run_dir.resolve()
    report_root = (args.report_root or run_dir).resolve()
    recovery_update = None
    if args.mark_verified_object_key:
        if not args.r2_health_verified:
            raise SystemExit("--mark-verified-object-key requires --r2-health-verified")
        recovery_update = apply_verified_upload(
            run_dir,
            object_key=args.mark_verified_object_key,
            delete_verified_local=args.delete_verified_local,
        )
    summary = build_summary(
        run_dir,
        retry_requested=args.retry_unverified,
        r2_health_verified=args.r2_health_verified,
        retry_uploader=args.retry_uploader or None,
        recovery_update=recovery_update,
    )
    write_report(report_root, summary)
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary["classification"] != "R2_STREAMING_RECOVERY_NEEDS_MANUAL_REVIEW" else 2


if __name__ == "__main__":
    raise SystemExit(main())
