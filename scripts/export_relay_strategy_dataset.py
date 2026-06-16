#!/usr/bin/env python3
"""Create compact relay/R2-primary strategy exports plus raw-frame availability audits.

The main GPT-facing archive intentionally excludes full raw relay frame shards by
default. Full frames are tracked in RAW_FRAME_MANIFEST.csv so an operator can see
whether they still exist locally, were uploaded to R2, or were pruned after local
integrity verification and compact artifact upload.
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
import tempfile
import time
import zipfile
from collections import Counter
from typing import Any


REPO = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_COLLECTOR_ROOT = REPO / "research_output" / "local_stream_collector"
DEFAULT_EXPORT_ROOT = REPO / "research_output" / "strategy_exports"
DEFAULT_BATCH_LOG_ROOT = pathlib.Path(tempfile.gettempdir()) / "pump_relay_r2_primary_batch"
RAW_FRAME_MODES = ("manifest-only", "sample-only", "local-copy", "r2-copy")
REPORT_FILES = (
    "RAW_FRAME_AVAILABILITY_REPORT.md",
    "RAW_FRAME_AVAILABILITY_REPORT.json",
    "RAW_FRAME_MANIFEST.csv",
    "OPTIONAL_FULL_FRAME_EXPORT_INSTRUCTIONS.md",
)
SAMPLE_FILE = "RAW_FRAME_SAMPLE.ndjson"
FINAL_SUMMARY_FILES = (
    "local_relay_dataset_proof_summary.json",
    "local_collector_summary.json",
    "service_exit_status.json",
    "local_collector_exit_status.json",
    "countability_decision.json",
    "run_countability_decision.json",
    "r2_upload_result.json",
    "local_retention_summary.json",
    "manifest.json",
    "stable_manifest_upload.json",
    "relay_frame_manifest.json",
)


class ExportError(RuntimeError):
    pass


def utc_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def read_json(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open() as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def safe_bool(value: Any) -> bool:
    return bool(value) if value is not None else False


def int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def common_prefix(keys: list[str]) -> str:
    if not keys:
        return ""
    split = [key.split("/") for key in keys]
    prefix: list[str] = []
    for parts in zip(*split):
        if len(set(parts)) != 1:
            break
        prefix.append(parts[0])
    return "/".join(prefix)


def load_batch_map(batch_log_root: pathlib.Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    if not batch_log_root.exists():
        return mapping
    for summary in batch_log_root.glob("*/batch_summary.ndjson"):
        batch_id = summary.parent.name
        try:
            lines = summary.read_text().splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            run = row.get("run")
            if isinstance(run, str) and run:
                mapping[run] = batch_id
    return mapping


def infer_batch_id(run_name: str, batch_map: dict[str, str]) -> str:
    if run_name in batch_map:
        return batch_map[run_name]
    if "-slice-" in run_name:
        return run_name.rsplit("-slice-", 1)[0]
    if "-batch-" in run_name:
        return run_name.split("-batch-", 1)[0] + "-batch"
    if "-proof-" in run_name:
        return run_name.split("-proof-", 1)[0] + "-proof"
    return "unmapped"


def is_candidate_run_dir(path: pathlib.Path) -> bool:
    if not path.is_dir():
        return False
    name = path.name
    if name.endswith("-logs") or name.startswith("."):
        return False
    if not name.startswith("relay"):
        return False
    return any((path / marker).exists() for marker in FINAL_SUMMARY_FILES) or (path / "relay_frames").exists()


def list_local_raw_files(run_dir: pathlib.Path) -> list[pathlib.Path]:
    relay_dir = run_dir / "relay_frames"
    if not relay_dir.exists():
        return []
    return sorted(p for p in relay_dir.glob("part-*.ndjson") if p.is_file())


def raw_files_total_bytes(files: list[pathlib.Path]) -> int:
    total = 0
    for path in files:
        try:
            total += path.stat().st_size
        except OSError:
            pass
    return total


def retention_deleted_relay_frames(retention: dict[str, Any]) -> bool:
    for entry in retention.get("deleted_bulk_paths") or []:
        if not isinstance(entry, dict):
            continue
        path = str(entry.get("path") or "")
        reason = str(entry.get("reason") or "")
        if "relay_frames" in path or "relay_frames" in reason or "raw_frames" in reason:
            return True
    return False


def artifact_consistency_ok(run_dir: pathlib.Path, proof: dict[str, Any]) -> str:
    summary = read_json(run_dir / "artifact_consistency_summary.json")
    if summary:
        if "ok" in summary:
            return str(bool(summary.get("ok"))).lower()
        if "passed" in summary:
            return str(bool(summary.get("passed"))).lower()
        if "blockers" in summary:
            return str(not bool(summary.get("blockers"))).lower()
    if proof and str(proof.get("classification") or "").endswith("PASS"):
        return "true"
    return "unknown"


def r2_raw_keys(r2: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for field in ("verified_files", "uploaded_files"):
        for value in r2.get(field) or []:
            if isinstance(value, str) and "/relay_frames/" in value:
                keys.append(value)
    return sorted(set(keys))


def r2_skipped_raw_count(r2: dict[str, Any]) -> int:
    count = 0
    for item in r2.get("skipped_files") or []:
        if isinstance(item, dict) and str(item.get("relative_path") or "").startswith("relay_frames/"):
            count += 1
    return count


def classify_raw_frames(
    *,
    frames_received: int,
    local_present: bool,
    r2_count: int,
    pruned: bool,
    relay_manifest_present: bool,
    r2_present: bool,
    retention_ok: bool,
    completed: bool,
) -> tuple[str, bool, str]:
    if local_present and r2_count > 0:
        return "RAW_FRAMES_AVAILABLE_LOCAL_AND_R2", True, ""
    if local_present:
        return "RAW_FRAMES_AVAILABLE_LOCAL", True, ""
    if r2_count > 0:
        return "RAW_FRAMES_AVAILABLE_R2", True, ""
    if pruned:
        return (
            "RAW_FRAMES_PRUNED_AFTER_VERIFICATION",
            False,
            "raw relay frames were verified locally, skipped from compact R2 material upload, and pruned by retention",
        )
    if frames_received == 0 and not relay_manifest_present:
        return "RAW_FRAMES_NOT_CAPTURED", False, "no relay frame manifest or frame count was found"
    if not completed or (r2_present and not retention_ok):
        return "RAW_FRAMES_UNVERIFIED_ABORTED_RUN", False, "run appears incomplete, unverified, or retention did not complete"
    return "RAW_FRAMES_UNKNOWN_NEEDS_MANUAL_REVIEW", False, "no local/R2 raw-frame copy found and pruning could not be proven"


def inspect_run(run_dir: pathlib.Path, batch_map: dict[str, str]) -> dict[str, Any]:
    run_name = run_dir.name
    local_summary = read_json(run_dir / "local_collector_summary.json")
    proof = read_json(run_dir / "local_relay_dataset_proof_summary.json")
    relay_manifest = read_json(run_dir / "relay_frame_manifest.json")
    r2 = read_json(run_dir / "r2_upload_result.json")
    retention = read_json(run_dir / "local_retention_summary.json")
    countability = read_json(run_dir / "countability_decision.json")
    service_exit = read_json(run_dir / "service_exit_status.json")
    local_exit = read_json(run_dir / "local_collector_exit_status.json")

    local_files = list_local_raw_files(run_dir)
    local_bytes = raw_files_total_bytes(local_files)
    raw_keys = r2_raw_keys(r2)
    uploaded_keys = [v for v in (r2.get("verified_files") or r2.get("uploaded_files") or []) if isinstance(v, str)]
    skipped_raw_count = r2_skipped_raw_count(r2)
    pruned = retention_deleted_relay_frames(retention) or (skipped_raw_count > 0 and not local_files)

    frames_received = int_or_zero(
        local_summary.get("frames_received")
        or proof.get("frames_received")
        or relay_manifest.get("frames_received")
    )
    data_frames = int_or_zero(
        local_summary.get("data_frames_received")
        or proof.get("data_frames_received")
        or relay_manifest.get("data_frames_received")
    )
    control_frames = int_or_zero(
        local_summary.get("control_frames_received")
        or proof.get("control_frames_received")
        or relay_manifest.get("control_frames_received")
    )
    retention_ok = bool(retention.get("ok")) if retention else False
    completed = bool(proof) or bool(service_exit) or bool(local_exit)
    classification, reconstructable, unavailable = classify_raw_frames(
        frames_received=frames_received,
        local_present=bool(local_files),
        r2_count=len(raw_keys),
        pruned=pruned,
        relay_manifest_present=bool(relay_manifest),
        r2_present=bool(r2),
        retention_ok=retention_ok,
        completed=completed,
    )
    if len(raw_keys) > 0 and local_bytes:
        r2_total_bytes = local_bytes
    else:
        r2_total_bytes = 0
    if len(raw_keys) > 0 and not local_bytes:
        unavailable = unavailable or "R2 raw-frame object sizes are not present in local upload manifests"

    return {
        "batch_id": infer_batch_id(run_name, batch_map),
        "slice_id": run_name,
        "relay_session_id": local_summary.get("relay_session_id") or proof.get("relay_session_id") or "",
        "source_path": str(run_dir),
        "r2_prefix": common_prefix(uploaded_keys),
        "subscription_fingerprint": local_summary.get("subscription_fingerprint")
        or local_summary.get("config_subscription_fingerprint")
        or proof.get("subscription_fingerprint")
        or "",
        "frames_received": frames_received,
        "data_frames_forwarded": data_frames,
        "control_frames_forwarded": control_frames,
        "sequence_gap_count": int_or_zero(local_summary.get("sequence_gap_count") or proof.get("sequence_gap_count")),
        "hash_mismatch_count": int_or_zero(local_summary.get("hash_mismatch_count") or proof.get("hash_mismatch_count")),
        "malformed_frame_count": int_or_zero(local_summary.get("malformed_frame_count") or proof.get("malformed_frame_count")),
        "receiver_backpressure_count": int_or_zero(
            local_summary.get("downstream_backpressure_count")
            or local_summary.get("receiver_backpressure_count")
            or proof.get("receiver_backpressure_count")
        ),
        "receiver_unavailable_count": int_or_zero(local_summary.get("receiver_unavailable_count") or proof.get("receiver_unavailable_count")),
        "relay_frames_local_present": bool(local_files),
        "relay_frames_local_path": str(run_dir / "relay_frames") if local_files else "",
        "relay_frames_local_file_count": len(local_files),
        "relay_frames_local_total_bytes": local_bytes,
        "relay_frames_r2_uploaded": len(raw_keys) > 0,
        "relay_frames_r2_object_count": len(raw_keys),
        "relay_frames_r2_total_bytes": r2_total_bytes,
        "relay_frames_r2_keys": raw_keys,
        "relay_frames_pruned_by_retention": pruned,
        "retention_mode": str(retention.get("retention_mode") or ""),
        "retention_deleted_bytes": int_or_zero(retention.get("deleted_bulk_bytes")),
        "retention_retained_bytes": int_or_zero(retention.get("local_retained_bytes")),
        "raw_frame_reconstructable": reconstructable,
        "raw_frame_unavailable_reason": unavailable,
        "raw_frame_classification": classification,
        "local_collector_summary_present": bool(local_summary),
        "relay_exit_status_present": bool(local_exit),
        "r2_upload_result_present": bool(r2),
        "local_retention_summary_present": bool(retention),
        "countability_decision_present": bool(countability),
        "artifact_consistency_ok": artifact_consistency_ok(run_dir, proof),
        "r2_verified": bool(r2.get("verified")) if r2 else False,
        "r2_skipped_raw_frame_count": skipped_raw_count,
        "relay_frame_manifest_present": bool(relay_manifest),
    }


def sanitize_frame(frame: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": frame.get("schema_version"),
        "relay_session_id": frame.get("relay_session_id"),
        "stream_id": frame.get("stream_id"),
        "provider": frame.get("provider"),
        "source_kind": frame.get("source_kind"),
        "subscription_fingerprint": frame.get("subscription_fingerprint"),
        "sequence": frame.get("sequence"),
        "received_at_unix_nanos": frame.get("received_at_unix_nanos"),
        "slot": frame.get("slot"),
        "commitment": frame.get("commitment"),
        "payload_codec": frame.get("payload_codec"),
        "payload_compressed": frame.get("payload_compressed"),
        "payload_hash": frame.get("payload_hash"),
        "payload_len": frame.get("payload_len"),
        "payload_base64_redacted": "payload_base64" in frame,
        "control_kind": frame.get("control_kind"),
        "relay_error_present": frame.get("relay_error") is not None,
    }


def collect_sample(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for row in rows:
        local_path = row.get("relay_frames_local_path")
        if not local_path:
            continue
        for shard in sorted(pathlib.Path(local_path).glob("part-*.ndjson")):
            try:
                with shard.open() as handle:
                    for line in handle:
                        if len(samples) >= limit:
                            return samples
                        try:
                            frame = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        samples.append(sanitize_frame(frame))
            except OSError:
                continue
    return samples


def write_csv(rows: list[dict[str, Any]], path: pathlib.Path) -> None:
    fieldnames = [
        "batch_id",
        "slice_id",
        "relay_session_id",
        "source_path",
        "r2_prefix",
        "subscription_fingerprint",
        "frames_received",
        "data_frames_forwarded",
        "control_frames_forwarded",
        "sequence_gap_count",
        "hash_mismatch_count",
        "malformed_frame_count",
        "receiver_backpressure_count",
        "receiver_unavailable_count",
        "relay_frames_local_present",
        "relay_frames_local_path",
        "relay_frames_local_file_count",
        "relay_frames_local_total_bytes",
        "relay_frames_r2_uploaded",
        "relay_frames_r2_object_count",
        "relay_frames_r2_total_bytes",
        "relay_frames_pruned_by_retention",
        "retention_mode",
        "retention_deleted_bytes",
        "retention_retained_bytes",
        "raw_frame_reconstructable",
        "raw_frame_unavailable_reason",
        "raw_frame_classification",
        "local_collector_summary_present",
        "relay_exit_status_present",
        "r2_upload_result_present",
        "local_retention_summary_present",
        "countability_decision_present",
        "artifact_consistency_ok",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def totals(rows: list[dict[str, Any]]) -> dict[str, Any]:
    classes = Counter(row["raw_frame_classification"] for row in rows)
    total_known = sum(int_or_zero(row.get("frames_received")) for row in rows)
    local_available = sum(
        int_or_zero(row.get("frames_received"))
        for row in rows
        if row.get("relay_frames_local_present")
    )
    r2_available = sum(
        int_or_zero(row.get("frames_received"))
        for row in rows
        if row.get("relay_frames_r2_uploaded")
    )
    pruned = sum(
        int_or_zero(row.get("frames_received"))
        for row in rows
        if row.get("relay_frames_pruned_by_retention")
        and not row.get("relay_frames_local_present")
        and not row.get("relay_frames_r2_uploaded")
    )
    return {
        "run_count": len(rows),
        "classification_counts": dict(classes),
        "total_full_frames_known_across_manifests": total_known,
        "total_raw_frames_still_locally_available": local_available,
        "total_raw_frames_available_in_r2": r2_available,
        "total_raw_frames_pruned_by_retention": pruned,
        "local_raw_file_count": sum(int_or_zero(row.get("relay_frames_local_file_count")) for row in rows),
        "local_raw_total_bytes": sum(int_or_zero(row.get("relay_frames_local_total_bytes")) for row in rows),
        "r2_raw_object_count": sum(int_or_zero(row.get("relay_frames_r2_object_count")) for row in rows),
        "r2_raw_total_bytes_known": sum(int_or_zero(row.get("relay_frames_r2_total_bytes")) for row in rows),
        "retention_deleted_bytes": sum(int_or_zero(row.get("retention_deleted_bytes")) for row in rows),
        "retention_retained_bytes": sum(int_or_zero(row.get("retention_retained_bytes")) for row in rows),
    }


def write_report_md(rows: list[dict[str, Any]], totals_payload: dict[str, Any], path: pathlib.Path) -> None:
    lines = [
        "# Raw Frame Availability Report",
        "",
        "This report audits relay-only/local R2-primary collector runs under `research_output/local_stream_collector/`.",
        "",
        "The main GPT strategy export is compact by design and does **not** include full raw relay frame shards.",
        "",
        "## Summary",
        "",
        f"- Runs inspected: {totals_payload['run_count']}",
        f"- Total full frames known across manifests: {totals_payload['total_full_frames_known_across_manifests']}",
        f"- Total raw frames still locally available: {totals_payload['total_raw_frames_still_locally_available']}",
        f"- Total raw frames available in R2: {totals_payload['total_raw_frames_available_in_r2']}",
        f"- Total raw frames pruned by retention: {totals_payload['total_raw_frames_pruned_by_retention']}",
        f"- Local raw frame files still present: {totals_payload['local_raw_file_count']}",
        f"- Local raw frame bytes still present: {totals_payload['local_raw_total_bytes']}",
        f"- R2 raw frame object keys known: {totals_payload['r2_raw_object_count']}",
        f"- Retention deleted bytes across inspected runs: {totals_payload['retention_deleted_bytes']}",
        "",
        "## Classification Counts",
        "",
    ]
    for name, count in sorted(totals_payload["classification_counts"].items()):
        lines.append(f"- {name}: {count}")
    lines.extend(
        [
            "",
            "## Reconstruction Notes",
            "",
            "- `RAW_FRAMES_AVAILABLE_LOCAL`: full shards are still on local disk.",
            "- `RAW_FRAMES_AVAILABLE_R2`: full raw shard object keys are listed in R2 manifests.",
            "- `RAW_FRAMES_PRUNED_AFTER_VERIFICATION`: raw frames were verified locally, skipped from compact material artifact upload, then removed by retention.",
            "- Compact material artifacts, ledgers, countability decisions, and summaries remain available even when raw relay frames were pruned.",
            "- If raw frames were skipped from R2 and deleted locally, they cannot be reconstructed from the compact GPT strategy export.",
            "",
            "## Recent / Interesting Runs",
            "",
            "| slice_id | class | frames | local_files | r2_objects | pruned | reconstructable |",
            "| --- | --- | ---: | ---: | ---: | --- | --- |",
        ]
    )
    interesting = rows[-25:]
    for row in interesting:
        lines.append(
            "| {slice_id} | {cls} | {frames} | {local_files} | {r2_objects} | {pruned} | {reconstructable} |".format(
                slice_id=row["slice_id"],
                cls=row["raw_frame_classification"],
                frames=row["frames_received"],
                local_files=row["relay_frames_local_file_count"],
                r2_objects=row["relay_frames_r2_object_count"],
                pruned=str(row["relay_frames_pruned_by_retention"]).lower(),
                reconstructable=str(row["raw_frame_reconstructable"]).lower(),
            )
        )
    path.write_text("\n".join(lines) + "\n")


def write_instructions(path: pathlib.Path, rows: list[dict[str, Any]], raw_archive_path: pathlib.Path | None) -> None:
    local_runs = [row for row in rows if row.get("relay_frames_local_present")]
    r2_runs = [row for row in rows if row.get("relay_frames_r2_uploaded")]
    lines = [
        "# Optional Full-Frame Export Instructions",
        "",
        "Full raw relay frames are intentionally excluded from the compact GPT strategy zip.",
        "",
        "## Local Full-Frame Archive",
        "",
        "To create a separate archive from local shards only:",
        "",
        "```bash",
        "python3 scripts/export_relay_strategy_dataset.py --include-raw-frames local-copy",
        "```",
        "",
        f"Runs with local raw shards currently visible: {len(local_runs)}",
    ]
    if raw_archive_path:
        lines.append(f"Last local raw archive created by this run: `{raw_archive_path}`")
    lines.extend(
        [
            "",
            "## R2 Full-Frame Archive",
            "",
            "Only use this when explicitly needed; it can download a lot of data.",
            "",
            "```bash",
            "R2_REPORTS_BUCKET=<bucket> R2_ENDPOINT_URL=<endpoint> \\",
            "  python3 scripts/export_relay_strategy_dataset.py --include-raw-frames r2-copy",
            "```",
            "",
            f"Runs with R2 raw-frame object keys: {len(r2_runs)}",
            "",
            "The script records R2 object keys/prefixes in `RAW_FRAME_MANIFEST.csv`; it does not download R2 raw frames in default exports.",
            "",
            "Do not include `.codex_runtime_env`, SSH config, provider credentials, or R2 credentials in any strategy export.",
        ]
    )
    path.write_text("\n".join(lines) + "\n")


def write_json_report(rows: list[dict[str, Any]], totals_payload: dict[str, Any], path: pathlib.Path) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": "phase107g.raw_frame_availability_report.v1",
                "generated_at": utc_stamp(),
                "collector_root": str(DEFAULT_COLLECTOR_ROOT),
                "totals": totals_payload,
                "runs": rows,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def write_sample(samples: list[dict[str, Any]], path: pathlib.Path) -> None:
    with path.open("w") as handle:
        for sample in samples:
            handle.write(json.dumps(sample, sort_keys=True) + "\n")


def add_file_to_zip(zipf: zipfile.ZipFile, path: pathlib.Path, base: pathlib.Path) -> None:
    zipf.write(path, path.relative_to(base))


def create_main_zip(export_dir: pathlib.Path, include_raw_frames: str) -> pathlib.Path:
    zip_path = export_dir.with_name(f"{export_dir.name}_gpt.zip")
    include_sample = include_raw_frames == "sample-only"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for name in REPORT_FILES:
            add_file_to_zip(zipf, export_dir / name, export_dir)
        if include_sample and (export_dir / SAMPLE_FILE).exists():
            add_file_to_zip(zipf, export_dir / SAMPLE_FILE, export_dir)
    return zip_path


def create_local_raw_archive(export_dir: pathlib.Path, rows: list[dict[str, Any]]) -> pathlib.Path | None:
    local_rows = [row for row in rows if row.get("relay_frames_local_present")]
    if not local_rows:
        return None
    archive_path = export_dir.with_name(f"{export_dir.name}_raw_frames_local.zip")
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_STORED) as zipf:
        for row in local_rows:
            root = pathlib.Path(str(row["relay_frames_local_path"]))
            for shard in sorted(root.glob("part-*.ndjson")):
                arcname = pathlib.Path(row["slice_id"]) / "relay_frames" / shard.name
                zipf.write(shard, arcname)
    return archive_path


def create_r2_raw_archive(export_dir: pathlib.Path, rows: list[dict[str, Any]], args: argparse.Namespace) -> pathlib.Path | None:
    r2_keys = sorted({key for row in rows for key in row.get("relay_frames_r2_keys", [])})
    if not r2_keys:
        return None
    bucket = args.r2_bucket or os.environ.get("R2_REPORTS_BUCKET")
    endpoint = args.r2_endpoint_url or os.environ.get("R2_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL")
    if not bucket or not endpoint:
        raise ExportError("r2-copy requires R2_REPORTS_BUCKET and R2_ENDPOINT_URL/AWS_ENDPOINT_URL")
    aws = shutil.which("aws")
    if not aws:
        raise ExportError("r2-copy requires aws CLI on PATH")
    staging = export_dir / "r2_raw_frame_download"
    staging.mkdir(parents=True, exist_ok=True)
    for key in r2_keys:
        dest = staging / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        cmd = [aws, "s3", "cp", f"s3://{bucket}/{key}", str(dest), "--endpoint-url", endpoint]
        subprocess.run(cmd, check=True)
    archive_path = export_dir.with_name(f"{export_dir.name}_raw_frames_r2.zip")
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_STORED) as zipf:
        for path in sorted(staging.rglob("*")):
            if path.is_file():
                zipf.write(path, path.relative_to(staging))
    return archive_path


def run_export(args: argparse.Namespace) -> dict[str, Any]:
    collector_root = args.collector_root
    export_root = args.output_root
    export_dir = export_root / f"strategy_export_{utc_stamp()}"
    export_dir.mkdir(parents=True, exist_ok=False)
    batch_map = load_batch_map(args.batch_log_root)
    rows = [
        inspect_run(path, batch_map)
        for path in sorted(collector_root.iterdir())
        if is_candidate_run_dir(path)
    ]
    rows.sort(key=lambda row: row["slice_id"])
    totals_payload = totals(rows)
    samples = collect_sample(rows, args.max_sample_frames)
    raw_archive_path: pathlib.Path | None = None
    if args.include_raw_frames == "local-copy":
        raw_archive_path = create_local_raw_archive(export_dir, rows)
    elif args.include_raw_frames == "r2-copy":
        raw_archive_path = create_r2_raw_archive(export_dir, rows, args)

    write_csv(rows, export_dir / "RAW_FRAME_MANIFEST.csv")
    write_json_report(rows, totals_payload, export_dir / "RAW_FRAME_AVAILABILITY_REPORT.json")
    write_report_md(rows, totals_payload, export_dir / "RAW_FRAME_AVAILABILITY_REPORT.md")
    write_sample(samples, export_dir / SAMPLE_FILE)
    write_instructions(export_dir / "OPTIONAL_FULL_FRAME_EXPORT_INSTRUCTIONS.md", rows, raw_archive_path)
    main_zip = create_main_zip(export_dir, args.include_raw_frames)
    result = {
        "schema_version": "phase107g.strategy_export.v1",
        "export_dir": str(export_dir),
        "main_gpt_strategy_zip": str(main_zip),
        "optional_raw_frame_archive": str(raw_archive_path) if raw_archive_path else "",
        "include_raw_frames": args.include_raw_frames,
        "main_zip_includes_all_raw_frames": False,
        "raw_frame_manifest": str(export_dir / "RAW_FRAME_MANIFEST.csv"),
        "raw_frame_report_md": str(export_dir / "RAW_FRAME_AVAILABILITY_REPORT.md"),
        "raw_frame_report_json": str(export_dir / "RAW_FRAME_AVAILABILITY_REPORT.json"),
        "raw_frame_sample": str(export_dir / SAMPLE_FILE),
        "optional_full_frame_export_instructions": str(export_dir / "OPTIONAL_FULL_FRAME_EXPORT_INSTRUCTIONS.md"),
        "sample_frame_count": len(samples),
        "totals": totals_payload,
    }
    (export_dir / "strategy_export_summary.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collector-root", type=pathlib.Path, default=DEFAULT_COLLECTOR_ROOT)
    parser.add_argument("--output-root", type=pathlib.Path, default=DEFAULT_EXPORT_ROOT)
    parser.add_argument("--batch-log-root", type=pathlib.Path, default=DEFAULT_BATCH_LOG_ROOT)
    parser.add_argument("--include-raw-frames", choices=RAW_FRAME_MODES, default="manifest-only")
    parser.add_argument("--max-sample-frames", type=int, default=20)
    parser.add_argument("--r2-bucket", default="")
    parser.add_argument("--r2-endpoint-url", default="")
    args = parser.parse_args(argv)
    if args.max_sample_frames < 0 or args.max_sample_frames > 20:
        parser.error("--max-sample-frames must be between 0 and 20")
    return args


def main(argv: list[str]) -> int:
    try:
        run_export(parse_args(argv))
        return 0
    except ExportError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
