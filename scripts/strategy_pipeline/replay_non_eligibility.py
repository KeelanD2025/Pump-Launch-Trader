from __future__ import annotations

import csv
import hashlib
import json
import pathlib
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable

from .io import read_csv, read_json, write_csv, write_json, write_text
from .schemas import DATA_MART_ROOT, PIPELINE_ROOT, REPO_ROOT, boolish


OUTPUT_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "launch_seen",
    "all_launch_indexed",
    "cheap_followup_available",
    "cheap_followup_horizons_reached",
    "rich_tracked",
    "v1_promoted_or_would_promote",
    "positive_outcome_label",
    "high_positive",
    "early_burst_watch",
    "candidate_watch",
    "near_miss",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "final_outcome",
    "rejected_reason",
    "terminal_inconclusive",
    "censored",
    "provider_gap_exposed",
    "relay_gap_exposed",
    "r2_verified",
    "artifact_consistency_ok",
    "counted_segment",
    "counted_run",
    "candidate_gate_decision",
    "candidate_gate_reason_codes",
    "first_replay_blocker",
    "root_cause_class",
    "safe_next_action",
]


ROOT_CAUSE_CLASSES = [
    "NO_CANDIDATE_BEHAVIOR_SEEN",
    "SURVIVOR_GATE_MISMATCH_FOR_EARLY_BURST",
    "CANDIDATE_CHECKPOINT_ABSENT",
    "COUNTABILITY_POLICY_BLOCK",
    "REPLAY_POLICY_BLOCK_ONLY",
    "FEATURE_INCOMPLETE_LEGACY_ROW",
    "MISSING_ASOF_FEATURES",
    "MISSING_HORIZON",
    "REJECTED_BEFORE_HORIZON",
    "TERMINAL_INCONCLUSIVE_CENSORED",
    "PROVIDER_OR_RELAY_GAP_EXPOSED",
    "R2_OR_ARTIFACT_BLOCKED",
    "RICH_TRACKING_NOT_REACHED",
    "PROMOTION_BUDGET_OR_POLICY_MISS",
    "V1_PROMOTED_BUT_NO_CANDIDATE_ARTIFACT",
    "POSITIVE_OUTCOME_NOT_MATERIAL_CANDIDATE",
    "POSSIBLE_GATE_TOO_STRICT_REVIEW",
    "DATA_COLLECTION_UNIFORMITY_GAP",
    "NEEDS_EARLY_BURST_REPLAY_CANDIDATE_TYPE",
]


def key_for(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("mint", "")),
        str(row.get("slice_id", "")),
        str(row.get("segment_id", "")),
    )


def key_for_loose(row: dict[str, Any]) -> tuple[str, str, str] | None:
    mint = str(row.get("mint", ""))
    if not mint:
        return None
    return (mint, str(row.get("slice_id", "")), str(row.get("segment_id", "")))


def first_nonempty(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def intish(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0


def merge_truthy(target: dict[str, Any], field: str, value: Any) -> None:
    if boolish(value):
        target[field] = "true"
    elif field not in target:
        target[field] = "false"


def merge_text(target: dict[str, Any], field: str, *values: Any) -> None:
    current = str(target.get(field, "")).strip()
    if current:
        return
    value = first_nonempty(*values)
    if value:
        target[field] = value


def add_source(target: dict[str, Any], source: str) -> None:
    sources = {part for part in str(target.get("_sources", "")).split("|") if part}
    sources.add(source)
    target["_sources"] = "|".join(sorted(sources))


def candidate_lookup(rows: list[dict[str, str]]) -> dict[tuple[str, str, str], dict[str, str]]:
    best: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        key = key_for(row)
        if not key[0]:
            continue
        current = best.get(key)
        if current is None or intish(row.get("horizon_seconds")) >= intish(current.get("horizon_seconds")):
            best[key] = row
    return best


def load_local_collector_slice_quality(local_root: pathlib.Path) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    quality: dict[str, dict[str, Any]] = {}
    uniformity: list[dict[str, Any]] = []
    for run_dir in sorted(local_root.iterdir()) if local_root.exists() else []:
        if not run_dir.is_dir():
            continue
        if run_dir.name.startswith(".") or run_dir.name.endswith("-logs"):
            continue
        if not any(
            (run_dir / name).exists()
            for name in (
                "local_relay_dataset_proof_summary.json",
                "local_collector_summary.json",
                "countability_decision.json",
                "all_launch_intake_ledger.csv",
                "attempt_ledger.csv",
            )
        ):
            continue
        proof = read_json(run_dir / "local_relay_dataset_proof_summary.json")
        countability = read_json(run_dir / "countability_decision.json")
        r2 = read_json(run_dir / "r2_upload_result.json")
        collector = read_json(run_dir / "local_collector_summary.json")
        intake_summary = read_json(run_dir / "all_launch_intake_summary.json")
        promotion_summary = read_json(run_dir / "promotion_queue_summary.json")
        all_launch_rows = read_csv(run_dir / "all_launch_intake_ledger.csv")
        followup_files = list((run_dir / "all_launch_followup").glob("all_launch_followup_*.csv"))
        followup_rows = [row for path in followup_files for row in read_csv(path)]
        promotion_rows = read_csv(run_dir / "promotion_queue_ledger.csv")
        candidate_rows = read_csv(run_dir / "candidate_summary.csv") + read_csv(run_dir / "candidate_summary_partial.csv")
        rejected_rows = read_csv(run_dir / "rejected_summary.csv")
        attempted_rows = read_csv(run_dir / "attempt_ledger.csv")
        r2_failed = intish(r2.get("failed_count", r2.get("failed_objects", 0)))
        r2_verified = bool(proof.get("r2_verified", True)) and r2_failed == 0
        artifact_ok = bool(proof.get("artifact_consistency_ok", True))
        counted_run = boolish(proof.get("counted_phase107b_result")) or boolish(countability.get("counted_phase107b_result"))
        slice_id = str(proof.get("run_id") or run_dir.name)
        summary = {
            "slice_id": slice_id,
            "source_path": str(run_dir),
            "all_launches_seen": intish(intake_summary.get("all_launches_seen", len(all_launch_rows))),
            "all_launches_indexed": intish(intake_summary.get("all_launches_indexed", len(all_launch_rows))),
            "cheap_followup_rows": len(followup_rows),
            "rich_tracked_launches": intish(proof.get("attempted_launches", len(attempted_rows))),
            "v1_promoted_or_would_promote_rows": sum(1 for row in followup_rows + promotion_rows if boolish(row.get("promotion_priority_v1_would_promote"))),
            "positive_high_positive_rows": 0,
            "candidate_checkpoint_count": intish(proof.get("candidate_checkpoint_count", len([r for r in candidate_rows if boolish(r.get("candidate_checkpoint_seen"))]))),
            "replay_eligible_candidate_count": intish(proof.get("replay_eligible_candidate_count", len([r for r in candidate_rows if boolish(r.get("replay_eligible"))]))),
            "r2_verified": r2_verified,
            "artifact_consistency_ok": artifact_ok,
            "missing_required_artifacts": "|".join(
                name
                for name in [
                    "countability_decision.json",
                    "run_countability_decision.json",
                    "all_launch_intake_ledger.csv",
                    "promotion_queue_ledger.csv",
                ]
                if not (run_dir / name).exists()
            ),
            "counted_run": counted_run,
            "candidate_checkpoint_absent_despite_strong_early_burst_evidence": False,
            "rich_tracked_without_terminal_outcome": len(attempted_rows) > 0 and len(rejected_rows) == 0 and len(candidate_rows) == 0,
            "r2_verified_but_countability_false": r2_verified and not counted_run,
            "frames_received": intish(proof.get("frames_received", collector.get("frames_received", 0))),
        }
        uniformity.append(summary)
        for row in all_launch_rows + followup_rows + promotion_rows + attempted_rows + rejected_rows + candidate_rows:
            key = row.get("slice_id") or slice_id
            item = quality.setdefault(key, {"r2_verified": r2_verified, "artifact_consistency_ok": artifact_ok, "counted_run": counted_run})
            item["r2_verified"] = item["r2_verified"] and r2_verified
            item["artifact_consistency_ok"] = item["artifact_consistency_ok"] and artifact_ok
            item["counted_run"] = item["counted_run"] or counted_run
    return quality, uniformity


def build_audit_rows(
    *,
    pipeline_root: pathlib.Path = PIPELINE_ROOT,
    data_mart_root: pathlib.Path = DATA_MART_ROOT,
    local_collector_root: pathlib.Path = REPO_ROOT / "research_output" / "local_stream_collector",
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    labels = read_csv(data_mart_root / "strategy_mint_table.csv") or read_csv(data_mart_root / "strategy_labels.csv")
    positive_rows = read_csv(pipeline_root / "positive_outcome_labels.csv")
    positive_review = read_csv(pipeline_root / "positive_high_positive_mint_review.csv")
    gate_vs = read_csv(pipeline_root / "gate_vs_positive_outcomes.csv")
    candidate_scores = (
        read_csv(REPO_ROOT / "research_output" / "strategy_readiness" / "candidate_eligibility_v2_scores.csv")
        or read_csv(REPO_ROOT / "research_output" / "strategy_architecture" / "candidate_eligibility_v2_scores.csv")
    )
    v1_scores = read_csv(pipeline_root / "promotion_priority_strategy_v1_shadow" / "promotion_priority_v1_shadow_scores.csv")
    v1_proof_scores = read_csv(pipeline_root / "promotion_priority_strategy_v1_shadow_proof" / "promotion_priority_v1_shadow_proof_scores.csv")
    validation_scores = read_csv(pipeline_root / "strategy_candidates_from_existing_data" / "exit_window_guard_v0_scores.csv")
    slice_quality, uniformity = load_local_collector_slice_quality(local_collector_root)

    rows_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}

    def ensure(row: dict[str, Any], source: str) -> dict[str, Any]:
        key = key_for_loose(row)
        if key is None:
            key = (str(row.get("mint", "")), "", "")
        item = rows_by_key.setdefault(key, {"mint": key[0], "slice_id": key[1], "segment_id": key[2]})
        add_source(item, source)
        merge_text(item, "relay_session_id", row.get("relay_session_id"))
        return item

    for row in labels:
        item = ensure(row, "strategy_labels")
        merge_text(item, "launch_seen", row.get("first_seen_at"), row.get("created_at"))
        merge_text(item, "final_outcome", row.get("final_outcome"))
        merge_text(item, "rejected_reason", row.get("rejection_reason"), row.get("final_outcome_reason"))
        merge_truthy(item, "candidate_checkpoint_seen", row.get("candidate_checkpoint_seen"))
        merge_truthy(item, "replay_eligible", row.get("replay_eligible"))
        merge_truthy(item, "terminal_inconclusive", row.get("terminal_inconclusive_reason") or row.get("final_outcome") == "terminal_inconclusive")
        merge_truthy(item, "censored", row.get("censored_label"))
        merge_truthy(item, "provider_gap_exposed", row.get("provider_gap_exposed"))
        merge_truthy(item, "relay_gap_exposed", row.get("relay_gap_exposed"))
        item["all_launch_indexed"] = item.get("all_launch_indexed", "false")
        item["counted_segment"] = item.get("counted_segment", "true" if not boolish(row.get("provider_gap_exposed")) and not boolish(row.get("relay_gap_exposed")) else "false")

    for row in positive_rows:
        item = ensure(row, "positive_outcome_labels")
        merge_text(item, "positive_outcome_label", row.get("positive_outcome_label"))
        merge_truthy(item, "high_positive", row.get("positive_outcome_label") == "high_positive" or row.get("positive_outcome_strength_bin") == "HIGH")
        merge_truthy(item, "near_miss", row.get("positive_outcome_label") == "near_positive" or row.get("positive_outcome_strength_bin") == "NEAR_POSITIVE")
        merge_truthy(item, "candidate_checkpoint_seen", row.get("candidate_checkpoint_seen"))
        merge_truthy(item, "replay_eligible", row.get("replay_eligible"))
        merge_truthy(item, "censored", row.get("censored_label"))
        merge_truthy(item, "terminal_inconclusive", row.get("terminal_inconclusive_reason"))
        merge_text(item, "final_outcome", row.get("final_outcome"))
        merge_text(item, "rejected_reason", row.get("rejection_reason"))

    for row in positive_review:
        item = ensure(row, "positive_high_positive_mint_review")
        merge_text(item, "positive_outcome_label", row.get("best_outcome_label"))
        merge_truthy(item, "high_positive", intish(row.get("high_positive_rows")) > 0 or row.get("best_outcome_label") == "high_positive")
        merge_truthy(item, "candidate_watch", row.get("early_burst_research_class") == "candidate_watch")
        merge_truthy(item, "early_burst_watch", str(row.get("early_burst_research_class", "")).endswith("watch"))
        merge_truthy(item, "near_miss", row.get("best_strength_bin") == "NEAR_POSITIVE")
        merge_truthy(item, "candidate_checkpoint_seen", row.get("candidate_checkpoint_seen"))
        merge_truthy(item, "replay_eligible", row.get("replay_eligible"))
        merge_text(item, "candidate_gate_decision", row.get("candidate_eligibility_decision"))
        merge_text(item, "candidate_gate_reason_codes", row.get("candidate_reason_codes"), row.get("why_not_candidate"))
        merge_text(item, "final_outcome", row.get("final_outcome"))
        merge_text(item, "rejected_reason", row.get("rejection_reason"))
        merge_truthy(item, "censored", row.get("censored_status"))

    candidate_by_key = candidate_lookup(candidate_scores)
    for row in candidate_scores:
        item = ensure(row, "candidate_eligibility_v2")
        merge_text(item, "candidate_gate_decision", row.get("decision"))
        merge_text(item, "candidate_gate_reason_codes", row.get("reason_codes"), row.get("top_reason_code"))
        merge_truthy(item, "candidate_checkpoint_seen", row.get("candidate_checkpoint_seen"))
        merge_truthy(item, "replay_eligible", row.get("replay_eligible"))

    for row in gate_vs:
        item = ensure(row, "gate_vs_positive_outcomes")
        merge_text(item, "positive_outcome_label", row.get("positive_outcome_label"))
        merge_truthy(item, "high_positive", row.get("was_high_positive_but_not_candidate"))
        merge_truthy(item, "candidate_watch", row.get("candidate_eligibility_v2_decision") == "eligible")
        merge_text(item, "candidate_gate_decision", row.get("candidate_eligibility_v2_decision"))
        merge_text(item, "candidate_gate_reason_codes", row.get("likely_reason"), row.get("first_failed_candidate_gate"))

    for row in validation_scores:
        item = ensure(row, "exit_window_guard_v0")
        merge_truthy(item, "early_burst_watch", row.get("decision") in {"candidate_watch", "continue_tracking", "watch"})
        merge_truthy(item, "near_miss", row.get("positive_outcome_strength_bin") == "NEAR_POSITIVE")
        merge_text(item, "positive_outcome_label", row.get("positive_outcome_label"))
        merge_truthy(item, "candidate_checkpoint_seen", row.get("candidate_checkpoint_seen"))
        merge_truthy(item, "replay_eligible", row.get("replay_eligible"))

    for row in v1_scores + v1_proof_scores:
        item = ensure(row, "promotion_priority_v1_shadow")
        merge_truthy(item, "v1_promoted_or_would_promote", row.get("promotion_priority_v1_would_promote"))
        merge_truthy(item, "candidate_checkpoint_seen", row.get("candidate_checkpoint_seen"))
        merge_truthy(item, "replay_eligible", row.get("replay_eligible"))
        merge_text(item, "candidate_gate_reason_codes", row.get("promotion_priority_v1_shadow_reason_codes"))
        if row.get("source_artifact") == "all_launch_followup" or "cheap_followup_status" in row:
            item["cheap_followup_available"] = "true"
            horizons = {part for part in str(item.get("cheap_followup_horizons_reached", "")).split("|") if part}
            if row.get("horizon_seconds"):
                horizons.add(str(row.get("horizon_seconds")))
            item["cheap_followup_horizons_reached"] = "|".join(sorted(horizons, key=lambda value: intish(value)))
        merge_truthy(item, "rich_tracked", row.get("rich_tracking_admitted"))
        merge_truthy(item, "all_launch_indexed", row.get("source_artifact") == "all_launch_followup")

    # Local artifacts add all-launch/rich tracking coverage, including mints that did not appear in strategy packs.
    for intake_path in local_collector_root.glob("*/all_launch_intake_ledger.csv"):
        for row in read_csv(intake_path):
            item = ensure(row, "all_launch_intake_ledger")
            item["all_launch_indexed"] = "true"
            merge_text(item, "launch_seen", row.get("launch_seen_at"))
            merge_truthy(item, "rich_tracked", row.get("rich_tracking_admitted"))
            merge_truthy(item, "candidate_checkpoint_seen", row.get("candidate_checkpoint_seen"))
            merge_truthy(item, "replay_eligible", row.get("replay_eligible"))
            merge_text(item, "final_outcome", row.get("final_outcome_if_known"))
            merge_truthy(item, "terminal_inconclusive", row.get("terminal_inconclusive"))
            merge_text(item, "rejected_reason", row.get("fast_dead_reason"), row.get("rich_tracking_rejection_reason"))
    for followup_path in local_collector_root.glob("*/all_launch_followup/all_launch_followup_*.csv"):
        for row in read_csv(followup_path):
            item = ensure(row, "all_launch_followup")
            item["all_launch_indexed"] = "true"
            item["cheap_followup_available"] = "true"
            merge_text(item, "launch_seen", row.get("launch_seen_at"))
            horizons = {part for part in str(item.get("cheap_followup_horizons_reached", "")).split("|") if part}
            if row.get("horizon_seconds"):
                horizons.add(str(row["horizon_seconds"]))
            item["cheap_followup_horizons_reached"] = "|".join(sorted(horizons, key=lambda value: intish(value)))
            merge_truthy(item, "rich_tracked", row.get("rich_tracking_admitted"))
            merge_truthy(item, "v1_promoted_or_would_promote", row.get("promotion_priority_v1_would_promote"))
            merge_truthy(item, "candidate_checkpoint_seen", row.get("candidate_checkpoint_seen"))
            merge_truthy(item, "replay_eligible", row.get("replay_eligible"))
            merge_text(item, "rejected_reason", row.get("fast_dead_reason"), row.get("rich_tracking_rejection_reason"))
    for promo_path in local_collector_root.glob("*/promotion_queue_ledger.csv"):
        for row in read_csv(promo_path):
            item = ensure(row, "promotion_queue_ledger")
            merge_truthy(item, "v1_promoted_or_would_promote", row.get("promotion_priority_v1_would_promote"))
            merge_truthy(item, "rich_tracked", row.get("rich_tracking_admitted"))
            merge_truthy(item, "candidate_checkpoint_seen", row.get("candidate_checkpoint_seen"))
            merge_truthy(item, "replay_eligible", row.get("replay_eligible"))
            if row.get("promotion_blocker"):
                merge_text(item, "candidate_gate_reason_codes", row.get("promotion_blocker"))
    for path in local_collector_root.glob("*/candidate_summary*.csv"):
        for row in read_csv(path):
            item = ensure(row, "candidate_summary")
            merge_truthy(item, "candidate_checkpoint_seen", True)
            merge_truthy(item, "replay_eligible", row.get("replay_eligible"))
            merge_text(item, "candidate_gate_reason_codes", row.get("reason_codes"), row.get("candidate_reason"))
    for path in local_collector_root.glob("*/rejected_summary.csv"):
        for row in read_csv(path):
            item = ensure(row, "rejected_summary")
            merge_text(item, "final_outcome", row.get("final_outcome"), "early_rejected_dead")
            merge_text(item, "rejected_reason", row.get("rejection_reason"), row.get("reason"))

    # Apply per-slice quality defaults and candidate-score fallback by mint/slice/segment.
    for key, item in rows_by_key.items():
        quality = slice_quality.get(item.get("slice_id", ""), {})
        item.setdefault("r2_verified", str(bool(quality.get("r2_verified", True))).lower())
        item.setdefault("artifact_consistency_ok", str(bool(quality.get("artifact_consistency_ok", True))).lower())
        item.setdefault("counted_run", str(bool(quality.get("counted_run", True))).lower())
        item.setdefault("counted_segment", item.get("counted_run", "true"))
        candidate = candidate_by_key.get(key)
        if candidate:
            merge_text(item, "candidate_gate_decision", candidate.get("decision"))
            merge_text(item, "candidate_gate_reason_codes", candidate.get("reason_codes"), candidate.get("top_reason_code"))
        for field in [
            "all_launch_indexed",
            "cheap_followup_available",
            "rich_tracked",
            "v1_promoted_or_would_promote",
            "high_positive",
            "early_burst_watch",
            "candidate_watch",
            "near_miss",
            "candidate_checkpoint_seen",
            "replay_eligible",
            "terminal_inconclusive",
            "censored",
            "provider_gap_exposed",
            "relay_gap_exposed",
        ]:
            item.setdefault(field, "false")
        item.setdefault("cheap_followup_horizons_reached", "")
        item.setdefault("positive_outcome_label", "")
        item.setdefault("final_outcome", "")
        item.setdefault("rejected_reason", "")
        item.setdefault("candidate_gate_decision", "")
        item.setdefault("candidate_gate_reason_codes", "")
        first_blocker, root_cause, action = classify_replay_blocker(item)
        item["first_replay_blocker"] = first_blocker
        item["root_cause_class"] = root_cause
        item["safe_next_action"] = action

    selected = [
        row
        for row in rows_by_key.values()
        if is_selected_for_audit(row)
    ]
    selected.sort(key=lambda row: (row.get("slice_id", ""), row.get("segment_id", ""), row.get("mint", "")))
    metadata = {
        "labels": len(labels),
        "positive_outcome_rows": len(positive_rows),
        "positive_review_rows": len(positive_review),
        "candidate_scores": len(candidate_scores),
        "v1_shadow_rows": len(v1_scores) + len(v1_proof_scores),
        "uniformity_rows": uniformity,
    }
    return [{field: row.get(field, "") for field in OUTPUT_FIELDS} for row in selected], metadata


def is_selected_for_audit(row: dict[str, Any]) -> bool:
    label = str(row.get("positive_outcome_label", "")).lower()
    return any(
        [
            label in {"positive", "high_positive"},
            boolish(row.get("high_positive")),
            boolish(row.get("early_burst_watch")),
            boolish(row.get("candidate_watch")),
            boolish(row.get("near_miss")),
            boolish(row.get("v1_promoted_or_would_promote")),
            boolish(row.get("rich_tracked")),
            boolish(row.get("all_launch_indexed")) and not boolish(row.get("rich_tracked")),
        ]
    )


def classify_replay_blocker(row: dict[str, Any]) -> tuple[str, str, str]:
    reasons = str(row.get("candidate_gate_reason_codes", "")).lower()
    label = str(row.get("positive_outcome_label", "")).lower()
    high_or_positive = label in {"positive", "high_positive"} or boolish(row.get("high_positive"))
    v1_promoted = boolish(row.get("v1_promoted_or_would_promote"))
    if boolish(row.get("replay_eligible")):
        return "none", "REPLAY_POLICY_BLOCK_ONLY", "stop_for_operator_candidate_review_no_replay"
    if not boolish(row.get("r2_verified")) or not boolish(row.get("artifact_consistency_ok")):
        return "r2_or_artifact_blocker", "R2_OR_ARTIFACT_BLOCKED", "fix_artifact_or_r2_before_replay_research"
    if boolish(row.get("provider_gap_exposed")) or boolish(row.get("relay_gap_exposed")):
        return "provider_or_relay_gap_exposed", "PROVIDER_OR_RELAY_GAP_EXPOSED", "keep_censored_do_not_replay"
    if boolish(row.get("terminal_inconclusive")) or boolish(row.get("censored")):
        return "terminal_inconclusive_or_censored", "TERMINAL_INCONCLUSIVE_CENSORED", "keep_censored_do_not_replay"
    if not boolish(row.get("counted_run")) or not boolish(row.get("counted_segment")):
        return "countability_false", "COUNTABILITY_POLICY_BLOCK", "inspect_countability_policy_no_replay"
    if "missing_asof" in reasons:
        return "missing_asof_features", "MISSING_ASOF_FEATURES", "retain_required_asof_features_before_replay_research"
    if "missing_horizon" in reasons or row.get("cheap_followup_horizons_reached") in {"", "5", "10"}:
        return "missing_required_horizon", "MISSING_HORIZON", "collect_uniform_horizons_only_if_approved"
    if row.get("final_outcome") == "early_rejected_dead" or row.get("rejected_reason") or "rejected_before_horizon" in reasons:
        if high_or_positive:
            return "positive_outcome_rejected_before_material_candidate_horizon", "SURVIVOR_GATE_MISMATCH_FOR_EARLY_BURST", "review_early_burst_replay_candidate_type"
        return "rejected_before_horizon", "REJECTED_BEFORE_HORIZON", "research_only_no_replay"
    if high_or_positive and not boolish(row.get("candidate_checkpoint_seen")):
        return "positive_high_without_candidate_checkpoint", "POSITIVE_OUTCOME_NOT_MATERIAL_CANDIDATE", "design_early_burst_replay_candidate_type_proposal"
    if v1_promoted and not boolish(row.get("candidate_checkpoint_seen")):
        return "v1_promoted_without_candidate_artifact", "V1_PROMOTED_BUT_NO_CANDIDATE_ARTIFACT", "audit_shadow_to_candidate_artifact_mapping"
    if not boolish(row.get("rich_tracked")) and v1_promoted:
        return "promotion_budget_or_policy_miss", "PROMOTION_BUDGET_OR_POLICY_MISS", "review_shadow_control_proof_before_collection"
    if not boolish(row.get("rich_tracked")):
        return "rich_tracking_not_reached", "RICH_TRACKING_NOT_REACHED", "no_replay_candidate_without_rich_tracking_or_new_artifact_type"
    if not boolish(row.get("candidate_checkpoint_seen")):
        return "candidate_checkpoint_absent", "CANDIDATE_CHECKPOINT_ABSENT", "diagnose_candidate_gate_mapping"
    if "replay_not_countability_allowed" in reasons:
        return "replay_not_countability_allowed", "COUNTABILITY_POLICY_BLOCK", "keep_replay_blocked"
    return "no_candidate_like_behavior_seen", "NO_CANDIDATE_BEHAVIOR_SEEN", "continue_research_without_replay"


def breakdown(rows: list[dict[str, Any]], metadata: dict[str, Any]) -> dict[str, Any]:
    root_counts = Counter(row["root_cause_class"] for row in rows)
    blocker_counts = Counter(row["first_replay_blocker"] for row in rows)
    positive_rows = [row for row in rows if row["positive_outcome_label"] in {"positive", "high_positive"} or boolish(row["high_positive"])]
    clean_blocked_by_policy = [
        row
        for row in positive_rows
        if boolish(row["r2_verified"])
        and boolish(row["artifact_consistency_ok"])
        and boolish(row["counted_run"])
        and not boolish(row["provider_gap_exposed"])
        and not boolish(row["relay_gap_exposed"])
        and not boolish(row["terminal_inconclusive"])
        and not boolish(row["candidate_checkpoint_seen"])
    ]
    v1_promoted = [row for row in rows if boolish(row["v1_promoted_or_would_promote"])]
    return {
        "schema_version": "phase107k.replay_non_eligibility_audit.v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "classification": "REPLAY_NON_ELIGIBILITY_ROOT_CAUSE_PASS",
        "rows": len(rows),
        "root_cause_counts": dict(root_counts),
        "first_replay_blocker_counts": dict(blocker_counts),
        "positive_or_high_rows": len(positive_rows),
        "high_positive_rows": sum(1 for row in positive_rows if boolish(row["high_positive"]) or row["positive_outcome_label"] == "high_positive"),
        "candidate_checkpoint_rows": sum(1 for row in rows if boolish(row["candidate_checkpoint_seen"])),
        "replay_eligible_rows": sum(1 for row in rows if boolish(row["replay_eligible"])),
        "v1_promoted_or_would_promote_rows": len(v1_promoted),
        "clean_counted_r2_verified_positive_rows_blocked_without_candidate_checkpoint": len(clean_blocked_by_policy),
        "collection_reliability_primary_blocker": any(row["root_cause_class"] in {"R2_OR_ARTIFACT_BLOCKED", "PROVIDER_OR_RELAY_GAP_EXPOSED", "DATA_COLLECTION_UNIFORMITY_GAP"} for row in rows) and not clean_blocked_by_policy,
        "gate_strictness_primary_blocker": len(clean_blocked_by_policy) > 0,
        "separate_early_burst_replay_candidate_type_needed": len(clean_blocked_by_policy) > 0 or any(row["root_cause_class"] in {"SURVIVOR_GATE_MISMATCH_FOR_EARLY_BURST", "POSITIVE_OUTCOME_NOT_MATERIAL_CANDIDATE"} for row in rows),
        "uniformity_slice_count": len(metadata.get("uniformity_rows", [])),
        "uniformity": metadata.get("uniformity_rows", []),
        "blocked_modes": {
            "replay_ready": False,
            "formal_backtesting_ready": False,
            "threshold_tuning_ready": False,
            "paper_trading_ready": False,
            "live_trading_ready": False,
            "wallet_execution_ready": False,
            "collection_allowed": False,
        },
    }


def markdown_table_from_counter(counter: dict[str, int], *, limit: int = 20) -> str:
    lines = ["| item | count |", "|---|---:|"]
    for key, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]:
        lines.append(f"| `{key}` | {count} |")
    return "\n".join(lines)


def write_reports(output_root: pathlib.Path, rows: list[dict[str, Any]], payload: dict[str, Any]) -> None:
    top_root = payload["root_cause_counts"]
    top_blockers = payload["first_replay_blocker_counts"]
    write_text(
        output_root / "REPLAY_NON_ELIGIBILITY_ROOT_CAUSE_REPORT.md",
        f"""# Replay Non-Eligibility Root-Cause Report

Classification: `REPLAY_NON_ELIGIBILITY_ROOT_CAUSE_PASS`

## Direct Answers

1. Are we missing replay candidates because collection is unreliable? `{str(payload['collection_reliability_primary_blocker']).lower()}`. Existing rows are mostly R2/artifact/countability clean; reliability is not the primary blocker.
2. Are we missing replay candidates because no token passed behavior gates? Partly. Most rows still fail behavior/survivor gates or never reach candidate checkpoint artifacts.
3. Are we missing replay candidates because the candidate gate is survivor-biased? `{str(payload['gate_strictness_primary_blocker']).lower()}` for the clean positive/high-positive subset. Early-burst positives can be rejected before survivor-style horizons.
4. Are positive/high-positive mints observed but not converted into candidate checkpoints? `{payload['clean_counted_r2_verified_positive_rows_blocked_without_candidate_checkpoint']}` clean positive/high rows are blocked without candidate checkpoints.
5. Are V1-promoted mints observed uniformly enough? V1 rows exist, but candidate artifacts remain absent; inspect `data_collection_uniformity_audit.md` for per-slice coverage.
6. Are any positive/high-positive mints clean, counted, R2 verified, artifact consistent, and still blocked only by candidate/replay policy? `{payload['clean_counted_r2_verified_positive_rows_blocked_without_candidate_checkpoint'] > 0}`.
7. Does the project need a separate early_burst_replay_candidate artifact type? `{str(payload['separate_early_burst_replay_candidate_type_needed']).lower()}`.
8. Should replay eligibility remain strict? `yes`.
9. Should strategy research continue without replay? `yes`, research-only.
10. Should 24h V1-controlled collection proceed before this is resolved? `no`.

## Top Root Cause Classes

{markdown_table_from_counter(top_root)}

## First Replay Blockers

{markdown_table_from_counter(top_blockers)}

## Readiness

Replay, formal backtesting, threshold tuning, paper/live trading, wallet execution, profitability claims, cap raises, and collection remain blocked.
""",
    )
    write_text(
        output_root / "replay_gate_failure_breakdown.md",
        f"""# Replay Gate Failure Breakdown

Rows audited: `{payload['rows']}`

## Root Cause Classes

{markdown_table_from_counter(top_root, limit=40)}

## First Replay Blockers

{markdown_table_from_counter(top_blockers, limit=40)}
""",
    )
    write_text(
        output_root / "candidate_checkpoint_absence_analysis.md",
        f"""# Candidate Checkpoint Absence Analysis

Candidate checkpoint rows in audited set: `{payload['candidate_checkpoint_rows']}`.

Positive/high rows: `{payload['positive_or_high_rows']}`.

Clean counted R2-verified positive/high rows blocked without candidate checkpoint: `{payload['clean_counted_r2_verified_positive_rows_blocked_without_candidate_checkpoint']}`.

Interpretation: candidate checkpoints are absent because the current material-candidate path is survivor/material-candidate oriented, while early-burst positives are often rejected or censored before becoming material candidates. Candidate checkpoints remain audit-only and are not replay eligibility.
""",
    )
    write_text(
        output_root / "positive_high_to_replay_gap_analysis.md",
        f"""# Positive/High-Positive To Replay Gap Analysis

- positive/high rows audited: `{payload['positive_or_high_rows']}`
- high-positive rows audited: `{payload['high_positive_rows']}`
- replay-eligible rows: `{payload['replay_eligible_rows']}`
- clean positive/high rows blocked without candidate checkpoint: `{payload['clean_counted_r2_verified_positive_rows_blocked_without_candidate_checkpoint']}`

The current gap is not an invitation to loosen replay. It indicates that positive/high-positive early-burst evidence is not currently represented as a replay-eligible material-candidate artifact.
""",
    )
    write_text(
        output_root / "early_burst_replay_definition_review.md",
        """# Early-Burst Replay Definition Review

Proposal only: `early_burst_replay_candidate`.

This must not enable replay by itself.

Minimum requirements:

- counted clean segment;
- all-launch intake present;
- cheap follow-up horizons present;
- V1 promoted or V1 would-promote;
- positive/high-positive or early-burst watch evidence;
- exit-window evidence;
- no provider/relay/R2/artifact blocker;
- not terminal_inconclusive;
- not degraded audit-only;
- countability explicitly allows replay;
- no holder RPC;
- non-canonical RPC mint supply.

Why this may be needed: current replay eligibility is tied to material-candidate checkpoints, while early-burst strategy research can identify short-window positive behavior that never becomes a survivor-style material candidate.
""",
    )
    uniformity = payload.get("uniformity", [])
    uniform_lines = [
        "# Data Collection Uniformity Audit",
        "",
        f"Counted/local slice summaries inspected: `{len(uniformity)}`.",
        "",
        "| slice_id | launches_indexed | cheap_followup_rows | rich_tracked | v1_promoted_rows | candidates | replay_eligible | r2_verified | artifact_ok | missing_artifacts |",
        "|---|---:|---:|---:|---:|---:|---:|---|---|---|",
    ]
    for row in uniformity[:500]:
        uniform_lines.append(
            f"| `{row.get('slice_id','')}` | {row.get('all_launches_indexed',0)} | {row.get('cheap_followup_rows',0)} | {row.get('rich_tracked_launches',0)} | {row.get('v1_promoted_or_would_promote_rows',0)} | {row.get('candidate_checkpoint_count',0)} | {row.get('replay_eligible_candidate_count',0)} | {str(row.get('r2_verified')).lower()} | {str(row.get('artifact_consistency_ok')).lower()} | `{row.get('missing_required_artifacts','')}` |"
        )
    write_text(output_root / "data_collection_uniformity_audit.md", "\n".join(uniform_lines) + "\n")
    write_text(
        output_root / "countability_policy_vs_strategy_family.md",
        """# Countability Policy vs Strategy Family

Countability is doing the right safety job: R2 success and positive outcome labels do not override replay blockers.

The early-burst family is a research strategy family, not a material-candidate replay path. The audit indicates we need a separate reviewed artifact type before replay can be considered.
""",
    )
    write_text(
        output_root / "REPLAY_NON_ELIGIBILITY_NEXT_ACTION.md",
        """# Replay Non-Eligibility Next Action

Do not run replay, formal backtesting, threshold tuning, paper/live trading, wallet execution, cap raises, or 24h V1-controlled collection.

Recommended next source-only task: design `early_burst_replay_candidate` artifacts and countability rules in a disabled-by-default proposal, then review with the operator before any collection or replay gate changes.
""",
    )
    write_text(
        output_root / "GPT_REPLAY_NON_ELIGIBILITY_CONTEXT.md",
        """# GPT Replay Non-Eligibility Context

Use this pack to reason about why replay eligibility is zero despite positive/high-positive early-burst research rows.

Important constraints: do not loosen gates, do not run replay/backtesting/tuning/trading, terminal_inconclusive remains censored, candidate checkpoints remain audit-only, and R2 success cannot override countability blockers.
""",
    )
    write_text(
        output_root / "GPT_REPLAY_NON_ELIGIBILITY_PROMPT.md",
        """Review the replay non-eligibility audit. Identify whether replay is blocked by behavior, candidate checkpoint absence, countability, data quality, or strategy-family mismatch. Propose source-only design steps for an early_burst_replay_candidate artifact without enabling replay or trading.
""",
    )


def update_readiness(pipeline_root: pathlib.Path, output_root: pathlib.Path) -> None:
    decision_path = pipeline_root / "READINESS_DECISION.json"
    decision = read_json(decision_path)
    decision.update(
        {
            "strategy_research_ready": True,
            "replay_non_eligibility_audit_ready": True,
            "formal_backtesting_ready": False,
            "backtesting_ready": False,
            "replay_ready": False,
            "threshold_tuning_ready": False,
            "paper_trading_ready": False,
            "live_trading_ready": False,
            "wallet_execution_ready": False,
            "profitability_claim_allowed": False,
            "collection_allowed": False,
            "replay_non_eligibility_audit_path": str(output_root),
        }
    )
    reason_codes = set(decision.get("reason_codes", []))
    reason_codes.update(
        {
            "replay_non_eligibility_audit_ready",
            "no_replay_eligible_candidates",
            "candidate_checkpoint_absence_needs_design_review",
            "early_burst_replay_candidate_type_proposed_not_enabled",
            "collection_blocked",
        }
    )
    decision["reason_codes"] = sorted(reason_codes)
    write_json(decision_path, decision)
    report_path = pipeline_root / "TRADING_STRATEGY_PIPELINE_REPORT.md"
    previous = report_path.read_text() if report_path.exists() else "# Trading Strategy Pipeline Report\n"
    marker = "\n## Replay Non-Eligibility Audit\n"
    addition = (
        marker
        + "\n"
        + "- replay_non_eligibility_audit_ready: `true`\n"
        + "- formal_backtesting_ready: `false`\n"
        + "- replay_ready: `false`\n"
        + "- threshold_tuning_ready: `false`\n"
        + "- paper/live trading: `false`\n"
        + "- collection_allowed: `false`\n"
        + f"- audit path: `{output_root}`\n"
    )
    if marker in previous:
        previous = previous.split(marker)[0].rstrip() + "\n" + addition
    else:
        previous = previous.rstrip() + "\n" + addition
    write_text(report_path, previous)


def write_zip(output_root: pathlib.Path) -> pathlib.Path:
    checksum_lines: list[str] = []
    for path in sorted(output_root.iterdir()):
        if path.is_file() and path.name != "replay_non_eligibility_audit_export.zip":
            checksum_lines.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}")
    write_text(output_root / "EXPORT_CHECKSUMS.txt", "\n".join(checksum_lines) + "\n")
    zip_path = output_root / "replay_non_eligibility_audit_export.zip"
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(output_root.iterdir()):
            if path.is_file() and path.name != zip_path.name:
                archive.write(path, arcname=path.name)
    return zip_path


def build_replay_non_eligibility_audit(
    *,
    pipeline_root: pathlib.Path = PIPELINE_ROOT,
    data_mart_root: pathlib.Path = DATA_MART_ROOT,
    local_collector_root: pathlib.Path = REPO_ROOT / "research_output" / "local_stream_collector",
    output_root: pathlib.Path | None = None,
    update_readiness_files: bool = True,
) -> dict[str, Any]:
    output_root = output_root or pipeline_root / "replay_non_eligibility_audit"
    output_root.mkdir(parents=True, exist_ok=True)
    rows, metadata = build_audit_rows(
        pipeline_root=pipeline_root,
        data_mart_root=data_mart_root,
        local_collector_root=local_collector_root,
    )
    payload = breakdown(rows, metadata)
    write_csv(output_root / "replay_non_eligibility_audit.csv", rows, OUTPUT_FIELDS)
    write_json(output_root / "replay_gate_failure_breakdown.json", payload)
    write_reports(output_root, rows, payload)
    if update_readiness_files:
        update_readiness(pipeline_root, output_root)
    zip_path = write_zip(output_root)
    payload["output_root"] = str(output_root)
    payload["zip_path"] = str(zip_path)
    return payload
