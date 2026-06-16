#!/usr/bin/env python3
"""Build a compact strategy-ready export from relay/local R2-primary artifacts.

This exporter intentionally does not include full raw relay frame shards in the
main archive. It uses compact material-hunter artifacts, countability decisions,
R2 verification manifests, and retained ledgers/summaries to create a GPT-ready
strategy research package.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import pathlib
import re
import sys
import tempfile
import time
import zipfile
from collections import Counter, defaultdict
from typing import Any


REPO = pathlib.Path(__file__).resolve().parents[1]
COLLECTOR_ROOT = REPO / "research_output" / "local_stream_collector"
EXPORT_ROOT = REPO / "research_output" / "strategy_export"
BATCH_LOG_ROOT = pathlib.Path(tempfile.gettempdir()) / "pump_relay_r2_primary_batch"
RAW_EXPORTER = REPO / "scripts" / "export_relay_strategy_dataset.py"

ASOF_SECONDS = (5, 10, 30, 60, 120, 300)

ATTEMPT_FIELDS = [
    "batch_id",
    "slice_id",
    "relay_session_id",
    "segment_id",
    "attempt_index",
    "mint",
    "run_id",
    "launch_timestamp",
    "tracked_until_seconds",
    "final_state",
    "rejection_or_promotion_reason",
    "early_warning_families",
    "rug_like_outcome_by_300s",
    "survived_300s",
    "survived_900s",
    "survived_1800s",
    "holder_rpc_used",
    "rpc_mint_supply_canonical",
    "r2_verified",
    "local_artifact_size_bytes",
    "promoted_to_candidate_dataset",
    "tombstone_written",
]

REJECTED_FIELDS = [
    "batch_id",
    "slice_id",
    "relay_session_id",
    "segment_id",
    "mint",
    "run_id",
    "final_state",
    "rejection_class",
    "stop_tracking_at_seconds",
    "early_warning_families",
    "holder_rpc_used",
    "threshold_tuning_allowed",
]

CANDIDATE_FIELDS = [
    "batch_id",
    "slice_id",
    "relay_session_id",
    "segment_id",
    "mint",
    "run_id",
    "final_state",
    "promotion_reason",
    "survived_300s",
    "survived_900s",
    "survived_1800s",
    "risk_timeline_rows",
    "pre_entry_risk_feature_rows",
    "post_event_label_rows",
    "candidate_checkpoint",
    "replay_eligible",
    "holder_rpc_used",
    "rpc_mint_supply_canonical",
]

SEGMENT_FIELDS = [
    "batch_id",
    "slice_id",
    "relay_session_id",
    "segment_id",
    "started_at",
    "ended_at",
    "provider_status",
    "provider_blocker_class",
    "stream_errors",
    "blocker_class",
    "blocker_snapshot_available",
    "provider_data_loss_seen",
    "client_backpressure_detected",
    "attempted_launches",
    "rejected_count",
    "terminal_inconclusive_count",
    "candidate_checkpoint_count",
    "replay_eligible_candidate_count",
    "counted_phase107b_result",
    "partial_outputs_audit_only",
    "off_vps_candidate_replay_allowed",
    "r2_verified",
    "artifact_consistency_ok",
]

INVENTORY_FIELDS = [
    "batch_id",
    "slice_id",
    "relay_session_id",
    "r2_prefix",
    "classification",
    "counted_phase107b_result",
    "r2_verified",
    "artifact_consistency_ok",
    "sequence_gap_count",
    "hash_mismatch_count",
    "receiver_backpressure_count",
    "provider_blocker_count",
    "attempted_launches",
    "unique_attempted_mints",
    "rejected_dead_count",
    "terminal_inconclusive_count",
    "candidate_checkpoint_count",
    "replay_eligible_candidate_count",
    "included_for_clean_labels",
    "exclusion_reason",
]

MINT_LABEL_FIELDS = [
    "mint",
    "batch_id",
    "slice_id",
    "segment_id",
    "first_seen_at",
    "final_outcome",
    "final_outcome_reason",
    "rejection_reason",
    "terminal_inconclusive_reason",
    "time_to_rejection_ms",
    "time_to_terminal_ms",
    "provider_gap_exposed",
    "relay_gap_exposed",
    "degraded_active_mint",
    "high_throughput_mint",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "clean_negative_label",
    "clean_positive_label",
    "censored_label",
    "label_quality",
]


def utc_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def read_json(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def read_csv_rows(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="") as handle:
            return list(csv.DictReader(handle))
    except OSError:
        return []


def write_csv(path: pathlib.Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: stringify(row.get(field, "")) for field in fields})


def write_json(path: pathlib.Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (list, dict)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)


def int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def common_prefix(keys: list[str]) -> str:
    if not keys:
        return ""
    parts = [key.split("/") for key in keys]
    prefix: list[str] = []
    for column in zip(*parts):
        if len(set(column)) != 1:
            break
        prefix.append(column[0])
    return "/".join(prefix)


def load_raw_exporter() -> Any:
    spec = importlib.util.spec_from_file_location("raw_exporter", RAW_EXPORTER)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not import {RAW_EXPORTER}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_batch_map(raw_exporter: Any) -> dict[str, str]:
    if hasattr(raw_exporter, "load_batch_map"):
        return raw_exporter.load_batch_map(BATCH_LOG_ROOT)
    return {}


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
    if not name.startswith("relay") or name.endswith("-logs") or name.startswith("."):
        return False
    required_any = (
        "countability_decision.json",
        "local_relay_dataset_proof_summary.json",
        "local_collector_summary.json",
        "attempt_ledger.csv",
    )
    return any((path / marker).exists() for marker in required_any)


def artifact_consistency_ok(run_dir: pathlib.Path, proof: dict[str, Any], countability: dict[str, Any]) -> bool:
    summary = read_json(run_dir / "artifact_consistency_summary.json")
    if summary:
        if "ok" in summary:
            return boolish(summary.get("ok"))
        if "passed" in summary:
            return boolish(summary.get("passed"))
        if "blockers" in summary:
            return not bool(summary.get("blockers"))
    classification = str(proof.get("classification") or "")
    if classification.endswith("PASS") or "PASS" in classification:
        return True
    return boolish(countability.get("hard_invariants_passed")) and boolish(countability.get("final_artifacts_exist"))


def r2_verified(r2: dict[str, Any], proof: dict[str, Any], countability: dict[str, Any]) -> bool:
    return boolish(r2.get("verified")) or boolish(proof.get("r2_verified")) or boolish(countability.get("r2_verified"))


def uploaded_keys(r2: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for field in ("verified_files", "uploaded_files"):
        for value in r2.get(field) or []:
            if isinstance(value, str):
                keys.append(value)
    return sorted(set(keys))


def run_safety_flags_ok(proof: dict[str, Any], local: dict[str, Any], countability: dict[str, Any], retention: dict[str, Any]) -> bool:
    return (
        not boolish(proof.get("holder_rpc_enabled") or local.get("holder_rpc_enabled") or retention.get("holder_rpc_enabled"))
        and not boolish(proof.get("rpc_mint_supply_canonical") or local.get("rpc_mint_supply_canonical") or retention.get("rpc_mint_supply_canonical") or countability.get("rpc_mint_supply_canonical"))
        and not boolish(proof.get("formal_backtesting_allowed") or local.get("formal_backtesting_allowed") or retention.get("formal_backtesting_allowed") or countability.get("formal_backtesting_allowed"))
        and not boolish(proof.get("threshold_tuning_allowed") or local.get("threshold_tuning_allowed") or retention.get("threshold_tuning_allowed") or countability.get("threshold_tuning_allowed"))
        and not boolish(proof.get("live_trading_enabled") or local.get("live_trading_enabled") or retention.get("live_trading_enabled"))
    )


def inspect_run(run_dir: pathlib.Path, batch_map: dict[str, str]) -> dict[str, Any]:
    proof = read_json(run_dir / "local_relay_dataset_proof_summary.json")
    local = read_json(run_dir / "local_collector_summary.json")
    countability = read_json(run_dir / "countability_decision.json")
    run_countability = read_json(run_dir / "run_countability_decision.json")
    r2 = read_json(run_dir / "r2_upload_result.json")
    retention = read_json(run_dir / "local_retention_summary.json")
    hunter = read_json(run_dir / "hunter_summary.json")
    keys = uploaded_keys(r2)
    seq = int_or_zero(proof.get("sequence_gap_count") or local.get("sequence_gap_count"))
    hash_mismatch = int_or_zero(proof.get("hash_mismatch_count") or local.get("hash_mismatch_count"))
    receiver_backpressure = int_or_zero(
        proof.get("receiver_backpressure_count")
        or local.get("receiver_backpressure_count")
        or local.get("downstream_backpressure_count")
    )
    receiver_unavailable = int_or_zero(proof.get("receiver_unavailable_count") or local.get("receiver_unavailable_count"))
    malformed = int_or_zero(proof.get("malformed_frame_count") or local.get("malformed_frame_count"))
    artifact_ok = artifact_consistency_ok(run_dir, proof, countability)
    r2_ok = r2_verified(r2, proof, countability)
    counted = boolish(countability.get("counted_phase107b_result") or proof.get("counted_phase107b_result"))
    safety_ok = run_safety_flags_ok(proof, local, countability, retention)
    exclusion: list[str] = []
    if not countability:
        exclusion.append("missing_countability_decision")
    if not counted:
        exclusion.append("counted_phase107b_result_false")
    if not r2_ok:
        exclusion.append("r2_not_verified")
    if not artifact_ok:
        exclusion.append("artifact_consistency_not_ok")
    if seq:
        exclusion.append("sequence_gap_count_nonzero")
    if hash_mismatch:
        exclusion.append("hash_mismatch_count_nonzero")
    if receiver_backpressure:
        exclusion.append("receiver_backpressure_count_nonzero")
    if receiver_unavailable:
        exclusion.append("receiver_unavailable_count_nonzero")
    if malformed:
        exclusion.append("malformed_frame_count_nonzero")
    if not safety_ok:
        exclusion.append("safety_flags_not_disabled")

    slice_id = run_dir.name
    return {
        "run_dir": run_dir,
        "batch_id": infer_batch_id(slice_id, batch_map),
        "slice_id": slice_id,
        "relay_session_id": local.get("relay_session_id") or proof.get("relay_session_id") or "",
        "r2_prefix": common_prefix(keys),
        "classification": proof.get("classification") or "",
        "counted_phase107b_result": counted,
        "r2_verified": r2_ok,
        "artifact_consistency_ok": artifact_ok,
        "sequence_gap_count": seq,
        "hash_mismatch_count": hash_mismatch,
        "malformed_frame_count": malformed,
        "receiver_backpressure_count": receiver_backpressure,
        "receiver_unavailable_count": receiver_unavailable,
        "provider_blocker_count": int_or_zero(proof.get("upstream_provider_blocker_count") or local.get("upstream_provider_blocker_count") or run_countability.get("gap_count")),
        "upstream_reconnect_count": int_or_zero(proof.get("upstream_reconnect_count") or local.get("upstream_reconnect_count")),
        "attempted_launches": int_or_zero(countability.get("counted_segment_attempted_launches") or proof.get("attempted_launches") or run_countability.get("total_attempted_launches")),
        "unique_attempted_mints": int_or_zero(proof.get("unique_attempted_mints") or countability.get("counted_segment_attempted_launches") or run_countability.get("total_attempted_launches")),
        "rejected_dead_count": int_or_zero(proof.get("rejected_dead_count") or countability.get("rejected_count")),
        "terminal_inconclusive_count": int_or_zero(proof.get("rejected_inconclusive_count") or countability.get("terminal_inconclusive_count")),
        "candidate_checkpoint_count": int_or_zero(countability.get("candidate_checkpoint_count") or proof.get("candidate_checkpoint_count")),
        "replay_eligible_candidate_count": int_or_zero(countability.get("replay_eligible_candidate_count") or proof.get("replay_eligible_candidate_count")),
        "included_for_clean_labels": not exclusion,
        "exclusion_reason": ";".join(exclusion),
        "proof": proof,
        "local": local,
        "countability": countability,
        "run_countability": run_countability,
        "r2": r2,
        "retention": retention,
        "hunter": hunter,
    }


def parse_mint_set(value: Any) -> set[str]:
    if isinstance(value, list):
        return {str(item) for item in value if str(item)}
    if isinstance(value, str):
        return {item for item in re.split(r"[|,; ]+", value) if item}
    return set()


def load_gap_mints(run_dir: pathlib.Path) -> set[str]:
    mints: set[str] = set()
    for row in read_csv_rows(run_dir / "run_gap_events.csv"):
        mints.update(parse_mint_set(row.get("affected_mints")))
    return mints


def segment_clean(segment: dict[str, Any]) -> bool:
    return (
        boolish(segment.get("counted_phase107b_result"))
        and not boolish(segment.get("provider_data_loss_seen"))
        and not boolish(segment.get("client_backpressure_detected"))
        and not boolish(segment.get("partial_outputs_audit_only"))
        and not str(segment.get("blocker_class") or "").strip()
    )


def load_segment_json(run_dir: pathlib.Path, segment_id: str) -> dict[str, Any]:
    return read_json(run_dir / "segments" / f"segment_{segment_id}" / "segment_summary.json")


def load_segment_countability(run_dir: pathlib.Path, segment_id: str) -> dict[str, Any]:
    return read_json(run_dir / "segments" / f"segment_{segment_id}" / "countability_decision.json")


def collect_segments(run: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    run_dir: pathlib.Path = run["run_dir"]
    rows: list[dict[str, Any]] = []
    by_id: dict[str, dict[str, Any]] = {}
    for row in read_csv_rows(run_dir / "run_segment_summary.csv"):
        segment_id = str(row.get("segment_id") or "")
        seg_json = load_segment_json(run_dir, segment_id)
        seg_countability = load_segment_countability(run_dir, segment_id)
        attempts = int_or_zero(row.get("attempted_launches") or seg_json.get("attempted_launches"))
        rejected = int_or_zero(row.get("rejected_count") or seg_json.get("rejected_count"))
        candidates = int_or_zero(row.get("candidate_checkpoint_count") or seg_json.get("candidate_checkpoint_count"))
        payload = {
            **row,
            "batch_id": run["batch_id"],
            "slice_id": run["slice_id"],
            "relay_session_id": run["relay_session_id"],
            "terminal_inconclusive_count": max(0, attempts - rejected - candidates),
            "r2_verified": run["r2_verified"],
            "artifact_consistency_ok": run["artifact_consistency_ok"],
        }
        payload["_clean"] = segment_clean({**row, **seg_json, **seg_countability})
        rows.append(payload)
        by_id[segment_id] = payload
    if not rows:
        payload = {
            "batch_id": run["batch_id"],
            "slice_id": run["slice_id"],
            "relay_session_id": run["relay_session_id"],
            "segment_id": "",
            "started_at": "",
            "ended_at": "",
            "provider_status": "",
            "provider_blocker_class": "",
            "stream_errors": "",
            "blocker_class": "",
            "blocker_snapshot_available": "",
            "provider_data_loss_seen": "false",
            "client_backpressure_detected": "false",
            "attempted_launches": run["attempted_launches"],
            "rejected_count": run["rejected_dead_count"],
            "terminal_inconclusive_count": run["terminal_inconclusive_count"],
            "candidate_checkpoint_count": run["candidate_checkpoint_count"],
            "replay_eligible_candidate_count": run["replay_eligible_candidate_count"],
            "counted_phase107b_result": run["counted_phase107b_result"],
            "partial_outputs_audit_only": "false",
            "off_vps_candidate_replay_allowed": run["countability"].get("off_vps_candidate_replay_allowed", False),
            "r2_verified": run["r2_verified"],
            "artifact_consistency_ok": run["artifact_consistency_ok"],
            "_clean": boolish(run["counted_phase107b_result"]),
        }
        rows.append(payload)
        by_id[""] = payload
    return rows, by_id


def collect_ledger_rows(run: dict[str, Any], filename: str, fields: list[str]) -> list[dict[str, Any]]:
    run_dir: pathlib.Path = run["run_dir"]
    rows: list[dict[str, Any]] = []
    segment_dirs = sorted((run_dir / "segments").glob("segment_*")) if (run_dir / "segments").exists() else []
    for segment_dir in segment_dirs:
        segment_id = segment_dir.name.replace("segment_", "")
        for row in read_csv_rows(segment_dir / filename):
            payload = {field: row.get(field, "") for field in fields}
            payload.update(
                {
                    "batch_id": run["batch_id"],
                    "slice_id": run["slice_id"],
                    "relay_session_id": run["relay_session_id"],
                    "segment_id": segment_id,
                }
            )
            rows.append(payload)
    if rows:
        return rows
    for row in read_csv_rows(run_dir / filename):
        payload = {field: row.get(field, "") for field in fields}
        payload.update(
            {
                "batch_id": run["batch_id"],
                "slice_id": run["slice_id"],
                "relay_session_id": run["relay_session_id"],
                "segment_id": "",
            }
        )
        rows.append(payload)
    return rows


def build_mint_labels(
    included_runs: list[dict[str, Any]],
    segment_maps: dict[str, dict[str, dict[str, Any]]],
    attempt_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rejected_by_key = {(row.get("slice_id"), row.get("segment_id"), row.get("mint")): row for row in rejected_rows}
    candidate_by_key = {(row.get("slice_id"), row.get("segment_id"), row.get("mint")): row for row in candidate_rows}
    run_by_slice = {run["slice_id"]: run for run in included_runs}
    seen: set[str] = set()
    labels: list[dict[str, Any]] = []
    for attempt in attempt_rows:
        mint = str(attempt.get("mint") or "")
        if not mint or mint in seen:
            continue
        seen.add(mint)
        slice_id = str(attempt.get("slice_id") or "")
        segment_id = str(attempt.get("segment_id") or "")
        run = run_by_slice.get(slice_id, {})
        seg = segment_maps.get(slice_id, {}).get(segment_id, {})
        key = (slice_id, segment_id, mint)
        rejected = rejected_by_key.get(key, {})
        candidate = candidate_by_key.get(key, {})
        final_state = str(attempt.get("final_state") or rejected.get("final_state") or candidate.get("final_state") or "")
        reason = str(attempt.get("rejection_or_promotion_reason") or rejected.get("rejection_class") or candidate.get("promotion_reason") or "")
        segment_is_clean = bool(seg.get("_clean"))
        provider_gap = boolish(seg.get("provider_data_loss_seen")) or mint in load_gap_mints(run["run_dir"]) if run else False
        relay_gap = int_or_zero(run.get("sequence_gap_count")) > 0 if run else False
        degraded = mint in parse_mint_set(run.get("countability", {}).get("degraded_active_mints")) if run else False
        high_throughput = mint in parse_mint_set(run.get("hunter", {}).get("high_throughput_mints")) if run else False
        candidate_checkpoint = boolish(candidate.get("candidate_checkpoint"))
        replay_eligible = boolish(candidate.get("replay_eligible")) and boolish(run.get("countability", {}).get("off_vps_candidate_replay_allowed")) if run else False
        clean_negative = final_state == "early_rejected_dead" and segment_is_clean
        censored = final_state == "terminal_inconclusive" or provider_gap or relay_gap
        clean_positive = replay_eligible and not candidate_checkpoint and segment_is_clean
        if candidate_checkpoint:
            label_quality = "audit_candidate_checkpoint"
        elif clean_positive:
            label_quality = "clean_positive"
        elif clean_negative:
            label_quality = "clean_negative"
        elif censored:
            label_quality = "censored"
        else:
            label_quality = "unknown_manual_review"
        tracked_ms = int_or_zero(attempt.get("tracked_until_seconds")) * 1000
        labels.append(
            {
                "mint": mint,
                "batch_id": attempt.get("batch_id", ""),
                "slice_id": slice_id,
                "segment_id": segment_id,
                "first_seen_at": attempt.get("launch_timestamp", ""),
                "final_outcome": final_state,
                "final_outcome_reason": reason,
                "rejection_reason": rejected.get("rejection_class", "") if final_state == "early_rejected_dead" else "",
                "terminal_inconclusive_reason": reason if final_state == "terminal_inconclusive" else "",
                "time_to_rejection_ms": tracked_ms if final_state == "early_rejected_dead" else "",
                "time_to_terminal_ms": tracked_ms if final_state == "terminal_inconclusive" else "",
                "provider_gap_exposed": provider_gap,
                "relay_gap_exposed": relay_gap,
                "degraded_active_mint": degraded,
                "high_throughput_mint": high_throughput,
                "candidate_checkpoint_seen": candidate_checkpoint,
                "replay_eligible": replay_eligible,
                "clean_negative_label": clean_negative,
                "clean_positive_label": clean_positive,
                "censored_label": censored,
                "label_quality": label_quality,
            }
        )
    return labels


def feature_availability() -> list[dict[str, Any]]:
    rows = [
        ("launch", "mint, launch timestamp, attempt lifecycle, terminal state", True, True, False, True, False, True, False),
        ("creator/dev/funding hints", "stream-derived warning families and bundle-like hints", True, True, False, True, False, True, False),
        ("transaction/trade", "active-mint transaction/trade deltas and high-throughput counters", True, True, False, True, False, True, False),
        ("holder/account/token-state", "stream-authoritative account/holder deltas; holder RPC disabled", True, True, False, True, False, True, False),
        ("bonding curve/vault/liquidity", "vault/curve state deltas when present in stream artifacts", True, True, False, True, False, True, False),
        ("high-throughput/degraded mint telemetry", "coalescing/degradation indicators and dirty feature counters", True, True, False, True, False, True, False),
        ("provider/relay/data-quality", "sequence/hash/provider/R2/countability quality gates", True, True, False, True, False, True, False),
        ("R2/artifact quality", "R2 verification, retention, manifest, artifact consistency", True, True, False, True, False, True, False),
        ("audit-only/RPC-supply fields", "RPC supply audit fields are non-canonical and not alpha features", False, False, False, False, True, True, False),
    ]
    return [
        {
            "feature_group": group,
            "description": description,
            "stream_authoritative": stream,
            "as_of_safe": asof,
            "requires_future_data": future,
            "clean_label_eligible": label,
            "audit_only": audit,
            "allowed_for_strategy_research": research,
            "allowed_for_backtest_alpha": backtest,
        }
        for group, description, stream, asof, future, label, audit, research, backtest in rows
    ]


def write_feature_map(export_dir: pathlib.Path) -> None:
    rows = feature_availability()
    payload = {
        "schema_version": "phase107g.feature_availability_map.v1",
        "generated_at": utc_stamp(),
        "features": rows,
    }
    write_json(export_dir / "FEATURE_AVAILABILITY_MAP.json", payload)
    lines = ["# Feature Availability Map", ""]
    lines.append("| feature_group | stream_authoritative | as_of_safe | clean_label_eligible | audit_only | strategy_research | backtest_alpha |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for row in rows:
        lines.append(
            f"| {row['feature_group']} | {str(row['stream_authoritative']).lower()} | {str(row['as_of_safe']).lower()} | {str(row['clean_label_eligible']).lower()} | {str(row['audit_only']).lower()} | {str(row['allowed_for_strategy_research']).lower()} | {str(row['allowed_for_backtest_alpha']).lower()} |"
        )
    lines.extend(
        [
            "",
            "Backtest alpha remains false because replay/backtesting/tuning have not been run and replay-eligible candidates are absent or blocked by countability.",
        ]
    )
    (export_dir / "FEATURE_AVAILABILITY_MAP.md").write_text("\n".join(lines) + "\n")


def write_asof_not_available(export_dir: pathlib.Path) -> None:
    asof_dir = export_dir / "ASOF_FEATURES"
    asof_dir.mkdir(exist_ok=True)
    lines = [
        "# As-Of Features Not Available",
        "",
        "Requested snapshot horizons: " + ", ".join(f"{s}s" for s in ASOF_SECONDS) + ".",
        "",
        "The retained compact R2-primary artifacts provide attempt ledgers, final labels, segment/countability summaries, high-throughput telemetry, and quality gates.",
        "",
        "They do not currently retain per-mint point-in-time feature snapshot tables at 5s/10s/30s/60s/120s/300s in the compact local export. Full raw relay frames are intentionally excluded from the main GPT zip and many raw shards were pruned after local integrity verification plus compact R2 upload.",
        "",
        "Strategy research can use final clean-negative/censored labels and aggregate telemetry now. Any future as-of modeling should add explicit retained feature snapshot shards at these horizons before replay/backtesting/tuning.",
    ]
    (asof_dir / "ASOF_FEATURES_NOT_AVAILABLE.md").write_text("\n".join(lines) + "\n")


def top_counts(rows: list[dict[str, Any]], field: str, n: int = 10) -> list[tuple[str, int]]:
    return Counter(str(row.get(field) or "") for row in rows if str(row.get(field) or "")).most_common(n)


def write_markdown_reports(
    export_dir: pathlib.Path,
    summary: dict[str, Any],
    labels: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
    segment_rows: list[dict[str, Any]],
    included_runs: list[dict[str, Any]],
) -> None:
    label_counts = Counter(row["label_quality"] for row in labels)
    final_counts = Counter(row["final_outcome"] for row in labels)
    rejection_counts = Counter(row["rejection_reason"] for row in labels if row.get("rejection_reason"))
    inconclusive_counts = Counter(row["terminal_inconclusive_reason"] for row in labels if row.get("terminal_inconclusive_reason"))
    high_throughput = [row for row in labels if boolish(row.get("high_throughput_mint")) or boolish(row.get("degraded_active_mint"))]

    (export_dir / "README_FOR_GPT.md").write_text(
        "\n".join(
            [
                "# Relay-Only R2-Primary Strategy Dataset",
                "",
                "This export is built from counted relay-only local material-hunter slices. The VPS acted only as a whitelisted stream relay; local processing owned countability, artifacts, R2 upload, validation, and retention.",
                "",
                "This dataset is for strategy research and label inspection. It is not a backtest result, not threshold tuning, not live trading logic, and not evidence of profitability.",
                "",
                "Labels are conservative: early rejected/dead outcomes in clean counted segments are clean negatives; terminal inconclusive outcomes are censored; candidate checkpoints are audit-only and not positives.",
                "",
                "Replay, formal backtesting, threshold tuning, live trading, holder RPC, and canonical RPC mint supply remain blocked/disabled.",
            ]
        )
        + "\n"
    )

    (export_dir / "GPT_COMPACT_CONTEXT.md").write_text(
        "\n".join(
            [
                "# GPT Compact Context",
                "",
                f"Included counted slices: {summary['included_slices']}",
                f"Unique mints: {summary['unique_mints']}",
                f"Clean negatives: {summary['clean_negative_count']}",
                f"Censored labels: {summary['censored_count']}",
                f"Clean positives: {summary['clean_positive_count']}",
                f"Replay-eligible candidates: {summary['replay_eligible_candidate_count']}",
                f"Candidate checkpoints: {summary['candidate_checkpoint_count']}",
                "",
                "Quality gates: included slices require countability_decision=true, R2 verified, artifact consistency ok, zero relay sequence gaps, zero hash mismatches, zero receiver backpressure, holder RPC disabled, RPC mint supply non-canonical, and replay/backtesting/tuning/trading disabled.",
                "",
                "Feature groups: launch lifecycle, stream-derived warning families, transaction/trade deltas, holder/account/token state, bonding curve/vault deltas when present, high-throughput/degraded mint telemetry, provider/relay quality, and R2/artifact quality.",
                "",
                "Initial hypotheses to investigate later without threshold tuning: avoid no-buy-followthrough and volume-evaporated patterns; treat provider-gap and terminal-inconclusive rows as censored; require clean segment membership for negative labels; exclude audit-only candidate checkpoints from positives; watch high-throughput/degraded mint policy before candidate eligibility.",
            ]
        )
        + "\n"
    )

    (export_dir / "DATASET_INVENTORY_SUMMARY.md").write_text(
        "\n".join(
            [
                "# Dataset Inventory Summary",
                "",
                f"- Included slices: {summary['included_slices']}",
                f"- Excluded slices: {summary['excluded_slices']}",
                f"- Total frames in included slices: {summary['frames_received']}",
                f"- Attempted launches: {summary['attempted_launches']}",
                f"- Unique attempted mints: {summary['unique_mints']}",
                f"- Rejected/dead: {summary['rejected_dead_count']}",
                f"- Terminal inconclusive: {summary['terminal_inconclusive_count']}",
                f"- Candidate checkpoints: {summary['candidate_checkpoint_count']}",
                f"- Replay-eligible candidates: {summary['replay_eligible_candidate_count']}",
                f"- Provider blockers in included slices: {summary['provider_blocker_count']}",
                f"- R2 verified objects: {summary['r2_verified_object_count']}",
                f"- R2 failures: {summary['r2_failure_count']}",
            ]
        )
        + "\n"
    )

    (export_dir / "MINT_LABELS_SUMMARY.md").write_text(
        "\n".join(
            [
                "# Mint Labels Summary",
                "",
                "## Label Quality",
                *[f"- {name}: {count}" for name, count in sorted(label_counts.items())],
                "",
                "## Final Outcomes",
                *[f"- {name}: {count}" for name, count in sorted(final_counts.items())],
                "",
                "## Top Rejection Reasons",
                *[f"- {name}: {count}" for name, count in rejection_counts.most_common(15)],
                "",
                "## Top Inconclusive Reasons",
                *[f"- {name}: {count}" for name, count in inconclusive_counts.most_common(15)],
                "",
                f"Clean negative count: {summary['clean_negative_count']}",
                f"Clean positive count: {summary['clean_positive_count']}",
                f"Censored count: {summary['censored_count']}",
                f"Replay-eligible count: {summary['replay_eligible_candidate_count']}",
            ]
        )
        + "\n"
    )

    (export_dir / "DATA_QUALITY_REPORT.md").write_text(
        "\n".join(
            [
                "# Data Quality Report",
                "",
                f"- Sequence gaps: {summary['sequence_gap_count']}",
                f"- Hash mismatches: {summary['hash_mismatch_count']}",
                f"- Receiver backpressure: {summary['receiver_backpressure_count']}",
                f"- Provider blockers: {summary['provider_blocker_count']}",
                f"- R2 failures: {summary['r2_failure_count']}",
                f"- Artifact consistency failures: {summary['artifact_consistency_failure_count']}",
                f"- Retention deleted bytes: {summary['retention_deleted_bytes']}",
                f"- Retention retained bytes: {summary['retention_retained_bytes']}",
                "",
                "Caveats: terminal inconclusive rows are censored, candidate checkpoints are audit-only, and compact exports do not include full raw frame shards by default.",
            ]
        )
        + "\n"
    )

    (export_dir / "EARLY_REJECTION_ANALYSIS.md").write_text(
        "\n".join(
            [
                "# Early Rejection Analysis",
                "",
                "This is descriptive only; no thresholds were tuned.",
                "",
                "Top rejection classes:",
                *[f"- {name}: {count}" for name, count in top_counts(rejected_rows, "rejection_class", 20)],
                "",
                "Clean early rejected/dead rows are usable as negative labels only when the row belongs to a clean counted segment.",
            ]
        )
        + "\n"
    )

    (export_dir / "TERMINAL_INCONCLUSIVE_ANALYSIS.md").write_text(
        "\n".join(
            [
                "# Terminal Inconclusive Analysis",
                "",
                "Terminal inconclusive rows are censored labels, not dead/negative labels.",
                "",
                "Top inconclusive reasons:",
                *[f"- {name}: {count}" for name, count in inconclusive_counts.most_common(20)],
            ]
        )
        + "\n"
    )

    (export_dir / "HIGH_THROUGHPUT_AND_DEGRADED_MINT_ANALYSIS.md").write_text(
        "\n".join(
            [
                "# High-Throughput And Degraded Mint Analysis",
                "",
                f"High-throughput or degraded mint label rows: {len(high_throughput)}",
                "",
                "Policy read: high-throughput handling appears to preserve dataset countability while quarantining noisy/degraded mints as censored or audit-only where required. This should be monitored before any future candidate eligibility or replay step.",
            ]
        )
        + "\n"
    )

    early_examples = [row for row in labels if row["final_outcome"] == "early_rejected_dead"][:10]
    inconclusive_examples = [row for row in labels if row["final_outcome"] == "terminal_inconclusive"][:10]
    candidate_examples = [row for row in labels if boolish(row.get("candidate_checkpoint_seen"))]
    replay_examples = [row for row in labels if boolish(row.get("replay_eligible"))]
    high_examples = high_throughput[:10]
    lines = ["# Representative Examples", "", "## Early Rejected/Dead"]
    for row in early_examples:
        lines.append(f"- {row['mint']} ({row['slice_id']}): {row['rejection_reason'] or row['final_outcome_reason']}")
    lines.append("")
    lines.append("## Terminal Inconclusive")
    for row in inconclusive_examples:
        lines.append(f"- {row['mint']} ({row['slice_id']}): {row['terminal_inconclusive_reason']}")
    lines.append("")
    lines.append("## Candidate Checkpoints")
    lines.extend([f"- {row['mint']} ({row['slice_id']})" for row in candidate_examples] or ["- None"])
    lines.append("")
    lines.append("## Replay-Eligible Candidates")
    lines.extend([f"- {row['mint']} ({row['slice_id']})" for row in replay_examples] or ["- None"])
    lines.append("")
    lines.append("## High-Throughput/Degraded")
    lines.extend([f"- {row['mint']} ({row['slice_id']}): high_throughput={row['high_throughput_mint']} degraded={row['degraded_active_mint']}" for row in high_examples] or ["- None"])
    (export_dir / "REPRESENTATIVE_EXAMPLES.md").write_text("\n".join(lines) + "\n")

    (export_dir / "STRATEGY_HYPOTHESES_DRAFT.md").write_text(
        "\n".join(
            [
                "# Strategy Hypotheses Draft",
                "",
                "These are hypotheses only. They are not profitability claims, threshold tuning, backtests, or trade entries.",
                "",
                "- Early avoid filters: investigate no-buy-followthrough, volume-evaporated, and stream warning families as avoid-only features.",
                "- Continue-tracking gates: inspect whether mints that avoid early rejection reasons maintain sufficient clean stream activity through 60s/120s/300s.",
                "- Candidate eligibility: require clean counted segments and exclude provider-gap, degraded, terminal-inconclusive, and audit-only candidate checkpoint rows.",
                "- Data-quality exclusions: exclude sequence/hash/receiver/R2/artifact blockers from any label-training set.",
                "- High-throughput handling: treat high-throughput/degraded telemetry as a safety feature until enough clean candidates exist.",
                "- Evidence needed: retained as-of feature snapshots, replay-eligible candidates, explicit countability approval, then replay/backtesting before any tuning/trading discussion.",
            ]
        )
        + "\n"
    )

    readiness = {
        "schema_version": "phase107g.backtesting_readiness_decision.v1",
        "strategy_research_ready": summary["clean_negative_count"] > 0,
        "backtesting_ready": False,
        "replay_ready": False,
        "threshold_tuning_ready": False,
        "live_trading_ready": False,
        "reason": "clean counted labels exist for strategy research; replay/backtesting/tuning/trading remain blocked because replay-eligible candidates are zero or not explicitly allowed by countability.",
        "replay_eligible_candidate_count": summary["replay_eligible_candidate_count"],
        "off_vps_candidate_replay_allowed": False,
    }
    write_json(export_dir / "BACKTESTING_READINESS_DECISION.json", readiness)
    (export_dir / "BACKTESTING_READINESS_DECISION.md").write_text(
        "\n".join(
            [
                "# Backtesting Readiness Decision",
                "",
                f"- strategy_research_ready: {str(readiness['strategy_research_ready']).lower()}",
                "- backtesting_ready: false",
                "- replay_ready: false",
                "- threshold_tuning_ready: false",
                "- live_trading_ready: false",
                "",
                readiness["reason"],
            ]
        )
        + "\n"
    )

    candidate_status = "Candidate summary rows are empty in this export. Candidate checkpoints remain audit-only and no replay-eligible candidates were present."
    if candidate_rows:
        candidate_status = f"Candidate summary rows present: {len(candidate_rows)}. Candidate checkpoints remain audit-only unless countability explicitly marks replay eligible."
    (export_dir / "CANDIDATE_STATUS.md").write_text("# Candidate Status\n\n" + candidate_status + "\n")


def compute_summary(
    included_runs: list[dict[str, Any]],
    excluded_runs: list[dict[str, Any]],
    labels: list[dict[str, Any]],
    r2_objects: int,
    r2_failures: int,
) -> dict[str, Any]:
    unique_mints = {row["mint"] for row in labels if row.get("mint")}
    return {
        "included_slices": len(included_runs),
        "excluded_slices": len(excluded_runs),
        "frames_received": sum(int_or_zero(run["proof"].get("frames_received") or run["local"].get("frames_received")) for run in included_runs),
        "attempted_launches": len(labels),
        "unique_mints": len(unique_mints),
        "rejected_dead_count": sum(1 for row in labels if row.get("final_outcome") == "early_rejected_dead"),
        "terminal_inconclusive_count": sum(1 for row in labels if row.get("final_outcome") == "terminal_inconclusive"),
        "candidate_checkpoint_count": sum(1 for row in labels if boolish(row.get("candidate_checkpoint_seen"))),
        "replay_eligible_candidate_count": sum(1 for row in labels if boolish(row.get("replay_eligible"))),
        "clean_negative_count": sum(1 for row in labels if boolish(row.get("clean_negative_label"))),
        "clean_positive_count": sum(1 for row in labels if boolish(row.get("clean_positive_label"))),
        "censored_count": sum(1 for row in labels if boolish(row.get("censored_label"))),
        "sequence_gap_count": sum(int_or_zero(run["sequence_gap_count"]) for run in included_runs),
        "hash_mismatch_count": sum(int_or_zero(run["hash_mismatch_count"]) for run in included_runs),
        "receiver_backpressure_count": sum(int_or_zero(run["receiver_backpressure_count"]) for run in included_runs),
        "provider_blocker_count": sum(int_or_zero(run["provider_blocker_count"]) for run in included_runs),
        "r2_verified_object_count": r2_objects,
        "r2_failure_count": r2_failures,
        "artifact_consistency_failure_count": sum(1 for run in included_runs if not run["artifact_consistency_ok"]),
        "retention_deleted_bytes": sum(int_or_zero(run["retention"].get("deleted_bulk_bytes")) for run in included_runs),
        "retention_retained_bytes": sum(int_or_zero(run["retention"].get("local_retained_bytes")) for run in included_runs),
    }


def write_index(export_dir: pathlib.Path) -> None:
    (export_dir / "EXPORT_FOR_GPT_INDEX.md").write_text(
        "\n".join(
            [
                "# Export For GPT Index",
                "",
                "Read in this order:",
                "",
                "1. `README_FOR_GPT.md`",
                "2. `GPT_COMPACT_CONTEXT.md`",
                "3. `DATASET_INVENTORY_SUMMARY.md`",
                "4. `MINT_LABELS_SUMMARY.md`",
                "5. `FEATURE_AVAILABILITY_MAP.md`",
                "6. `DATA_QUALITY_REPORT.md`",
                "7. `STRATEGY_HYPOTHESES_DRAFT.md`",
                "8. `BACKTESTING_READINESS_DECISION.md`",
                "",
                "CSV files provide normalized ledgers and labels. Raw relay frame shards are not included in the main zip; see `RAW_FRAME_AVAILABILITY_REPORT.md` and `RAW_FRAME_MANIFEST.csv`.",
            ]
        )
        + "\n"
    )


def write_checksums(export_dir: pathlib.Path) -> None:
    lines: list[str] = []
    for path in sorted(p for p in export_dir.rglob("*") if p.is_file() and p.name != "EXPORT_CHECKSUMS.txt"):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{digest}  {path.relative_to(export_dir)}")
    (export_dir / "EXPORT_CHECKSUMS.txt").write_text("\n".join(lines) + "\n")


def scan_for_secret_strings(export_dir: pathlib.Path) -> list[str]:
    bad_patterns = [
        re.compile(r"AKIA[0-9A-Z]{16}"),
        re.compile(r"(?i)secret[_-]?access[_-]?key"),
        re.compile(r"(?i)provider[_-]?auth"),
        re.compile(r"(?i)authorization:"),
        re.compile(r"(?i)private[_-]?key"),
        re.compile(r"\.codex_runtime_env"),
    ]
    hits: list[str] = []
    for path in export_dir.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in {".md", ".json", ".csv", ".txt", ".ndjson"}:
            continue
        text = path.read_text(errors="ignore")
        for pattern in bad_patterns:
            if pattern.search(text):
                hits.append(str(path.relative_to(export_dir)))
                break
    return sorted(set(hits))


def create_zip(export_dir: pathlib.Path) -> pathlib.Path:
    zip_path = export_dir.with_name(f"{export_dir.name}_strategy_ready.zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for path in sorted(p for p in export_dir.rglob("*") if p.is_file()):
            if "relay_frames" in path.parts:
                continue
            zipf.write(path, path.relative_to(export_dir))
    return zip_path


def validate_export(export_dir: pathlib.Path) -> list[str]:
    errors: list[str] = []
    for path in export_dir.rglob("*.json"):
        try:
            json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            errors.append(f"{path.relative_to(export_dir)} invalid json: {exc}")
    for path in export_dir.rglob("*.csv"):
        with path.open(newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, [])
            if not header:
                errors.append(f"{path.relative_to(export_dir)} missing header")
    for row in read_csv_rows(export_dir / "MINT_LABELS.csv"):
        if row.get("final_outcome") == "terminal_inconclusive" and row.get("clean_negative_label") == "true":
            errors.append("terminal_inconclusive labelled clean negative")
        if row.get("candidate_checkpoint_seen") == "true" and row.get("clean_positive_label") == "true" and row.get("replay_eligible") != "true":
            errors.append("candidate checkpoint labelled positive without replay eligibility")
    errors.extend(f"possible secret string in {hit}" for hit in scan_for_secret_strings(export_dir))
    return errors


def run_export(args: argparse.Namespace) -> dict[str, Any]:
    raw_exporter = load_raw_exporter()
    batch_map = load_batch_map(raw_exporter)
    stamp = utc_stamp()
    export_dir = args.output_root / f"strategy_export_{stamp}"
    export_dir.mkdir(parents=True, exist_ok=False)

    run_rows = [
        inspect_run(path, batch_map)
        for path in sorted(args.collector_root.iterdir())
        if is_candidate_run_dir(path)
    ]
    included = [row for row in run_rows if row["included_for_clean_labels"]]
    excluded = [row for row in run_rows if not row["included_for_clean_labels"]]

    inventory_rows = [{field: row.get(field, "") for field in INVENTORY_FIELDS} for row in run_rows]
    write_csv(export_dir / "DATASET_INVENTORY.csv", inventory_rows, INVENTORY_FIELDS)

    attempt_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    segment_rows: list[dict[str, Any]] = []
    segment_maps: dict[str, dict[str, dict[str, Any]]] = {}
    r2_object_count = 0
    r2_failure_count = 0
    for run in included:
        segments, segment_by_id = collect_segments(run)
        segment_rows.extend(segments)
        segment_maps[run["slice_id"]] = segment_by_id
        attempt_rows.extend(collect_ledger_rows(run, "attempt_ledger.csv", ATTEMPT_FIELDS))
        rejected_rows.extend(collect_ledger_rows(run, "rejected_summary.csv", REJECTED_FIELDS))
        candidate_rows.extend(collect_ledger_rows(run, "candidate_summary.csv", CANDIDATE_FIELDS))
        r2_object_count += len(run["r2"].get("verified_files") or [])
        r2_failure_count += len(run["r2"].get("failed_files") or [])

    labels = build_mint_labels(included, segment_maps, attempt_rows, rejected_rows, candidate_rows)
    summary = compute_summary(included, excluded, labels, r2_object_count, r2_failure_count)

    write_csv(export_dir / "ATTEMPT_LEDGER_NORMALIZED.csv", attempt_rows, ATTEMPT_FIELDS)
    write_csv(export_dir / "REJECTED_SUMMARY_NORMALIZED.csv", rejected_rows, REJECTED_FIELDS)
    write_csv(export_dir / "CANDIDATE_SUMMARY_NORMALIZED.csv", candidate_rows, CANDIDATE_FIELDS)
    write_csv(export_dir / "SEGMENT_SUMMARY_NORMALIZED.csv", segment_rows, SEGMENT_FIELDS)
    write_csv(export_dir / "MINT_LABELS.csv", labels, MINT_LABEL_FIELDS)

    write_markdown_reports(export_dir, summary, labels, rejected_rows, candidate_rows, segment_rows, included)
    write_feature_map(export_dir)
    write_asof_not_available(export_dir)
    write_index(export_dir)

    raw_rows = [
        raw_exporter.inspect_run(path, batch_map)
        for path in sorted(args.collector_root.iterdir())
        if raw_exporter.is_candidate_run_dir(path)
    ]
    raw_rows.sort(key=lambda row: row["slice_id"])
    raw_totals = raw_exporter.totals(raw_rows)
    raw_exporter.write_csv(raw_rows, export_dir / "RAW_FRAME_MANIFEST.csv")
    raw_exporter.write_json_report(raw_rows, raw_totals, export_dir / "RAW_FRAME_AVAILABILITY_REPORT.json")
    raw_exporter.write_report_md(raw_rows, raw_totals, export_dir / "RAW_FRAME_AVAILABILITY_REPORT.md")
    raw_exporter.write_sample(raw_exporter.collect_sample(raw_rows, min(args.max_raw_sample_frames, 20)), export_dir / "RAW_FRAME_SAMPLE.ndjson")
    raw_exporter.write_instructions(export_dir / "OPTIONAL_FULL_FRAME_EXPORT_INSTRUCTIONS.md", raw_rows, None)
    instructions_path = export_dir / "OPTIONAL_FULL_FRAME_EXPORT_INSTRUCTIONS.md"
    instructions_path.write_text(
        instructions_path.read_text().replace(
            "`.codex_runtime_env`, SSH config, provider credentials, or R2 credentials",
            "local runtime env directories, SSH config, provider credentials, or R2 credentials",
        )
    )

    write_json(
        export_dir / "strategy_ready_export_summary.json",
        {
            "schema_version": "phase107g.strategy_ready_export.v1",
            "generated_at": stamp,
            "collector_root": str(args.collector_root),
            "included_slices": summary["included_slices"],
            "excluded_slices": summary["excluded_slices"],
            "totals": summary,
            "raw_frame_totals": raw_totals,
            "main_zip_includes_raw_relay_frame_shards": False,
        },
    )

    validation_errors = validate_export(export_dir)
    if validation_errors:
        write_json(export_dir / "EXPORT_VALIDATION_ERRORS.json", {"errors": validation_errors})
        raise RuntimeError("export validation failed: " + "; ".join(validation_errors[:5]))
    write_checksums(export_dir)
    zip_path = create_zip(export_dir)
    result = {
        "export_dir": str(export_dir),
        "zip_path": str(zip_path),
        "included_slices": summary["included_slices"],
        "total_unique_mints": summary["unique_mints"],
        "rejected_dead_count": summary["rejected_dead_count"],
        "terminal_inconclusive_count": summary["terminal_inconclusive_count"],
        "candidate_checkpoint_count": summary["candidate_checkpoint_count"],
        "replay_eligible_candidate_count": summary["replay_eligible_candidate_count"],
        "strategy_research_ready": summary["clean_negative_count"] > 0,
        "backtesting_ready": False,
        "raw_frame_manifest": str(export_dir / "RAW_FRAME_MANIFEST.csv"),
        "main_zip_includes_raw_relay_frame_shards": False,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collector-root", type=pathlib.Path, default=COLLECTOR_ROOT)
    parser.add_argument("--output-root", type=pathlib.Path, default=EXPORT_ROOT)
    parser.add_argument("--max-raw-sample-frames", type=int, default=20)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    try:
        run_export(parse_args(argv))
        return 0
    except Exception as exc:  # noqa: BLE001 - command-line exporter should print concise blockers.
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
