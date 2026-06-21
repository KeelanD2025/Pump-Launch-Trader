#!/usr/bin/env python3
"""Build strategy-readiness artifacts from relay-only R2-primary datasets.

The command is deliberately research-only. It builds clean inventories,
conservative labels, point-in-time feature scaffolds, leakage audits,
chronological splits, and disabled-by-default strategy modules without running
replay, formal backtesting, threshold tuning, paper trading, live trading, or
wallet execution.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import pathlib
import random
import re
import sys
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


REPO = pathlib.Path(__file__).resolve().parents[1]
COLLECTOR_ROOT = REPO / "research_output" / "local_stream_collector"
OUTPUT_ROOT = REPO / "research_output" / "strategy_readiness"
BATCH_LOG_ROOT = pathlib.Path(tempfile.gettempdir()) / "pump_relay_r2_primary_batch"
STRATEGY_EXPORTER = REPO / "scripts" / "export_strategy_ready_dataset.py"
HORIZONS = (5, 10, 30, 60, 120, 300, 900)


DATASET_FIELDS = [
    "slice_id",
    "batch_id",
    "relay_session_id",
    "source_path",
    "r2_prefix",
    "classification",
    "counted_phase107b_result",
    "r2_verified",
    "artifact_consistency_ok",
    "sequence_gap_count",
    "hash_mismatch_count",
    "receiver_backpressure_count",
    "receiver_unavailable_count",
    "malformed_frame_count",
    "provider_blocker_count",
    "upstream_reconnect_count",
    "frames_received",
    "all_launches_seen",
    "all_launches_indexed",
    "rich_tracked_launches",
    "cheap_only_launches",
    "skipped_due_budget",
    "fast_dead_dropped",
    "missed_good_token_count",
    "tracking_slots_released",
    "cheap_followup_rows",
    "promotion_recommended_count",
    "promotion_admitted_count",
    "promotion_blocked_budget_count",
    "attempted_launches",
    "unique_attempted_mints",
    "attempt_ledger_rows",
    "rejected_summary_rows",
    "candidate_summary_rows",
    "segment_attempt_total",
    "segment_rejected_total",
    "segment_candidate_total",
    "rejected_dead_count",
    "terminal_inconclusive_count",
    "candidate_checkpoint_count",
    "replay_eligible_candidate_count",
    "r2_uploaded",
    "retention_deleted_bytes",
    "local_retained_bytes",
    "holder_rpc_disabled",
    "rpc_mint_supply_non_canonical",
    "replay_disabled",
    "backtesting_disabled",
    "threshold_tuning_disabled",
    "trading_disabled",
    "included",
    "exclusion_reason",
    "reconciliation_ok",
    "reconciliation_notes",
]

MINT_FIELDS = [
    "mint",
    "batch_id",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "first_seen_at",
    "created_at",
    "final_outcome",
    "final_outcome_reason",
    "rejection_reason",
    "terminal_inconclusive_reason",
    "time_to_rejection_ms",
    "time_to_terminal_ms",
    "provider_gap_exposed",
    "relay_gap_exposed",
    "sequence_gap_exposed",
    "hash_mismatch_exposed",
    "receiver_backpressure_exposed",
    "high_throughput_mint",
    "degraded_active_mint",
    "degraded_reason",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "clean_negative_label",
    "clean_positive_label",
    "censored_label",
    "label_quality",
    "source_artifacts",
]

ASOF_FIELDS = [
    "mint",
    "batch_id",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "first_seen_at",
    "horizon_seconds",
    "feature_available",
    "asof_safe",
    "launch_hour_utc",
    "launch_day_of_week_utc",
    "tracked_at_least_horizon",
    "data_quality_sequence_gap",
    "data_quality_hash_mismatch",
    "data_quality_receiver_backpressure",
    "data_quality_provider_gap_exposed",
    "data_quality_relay_gap_exposed",
    "data_quality_high_throughput_mint",
    "data_quality_degraded_active_mint",
    "label_clean_negative",
    "label_clean_positive",
    "label_censored",
    "label_quality",
]

ASOF_ALPHA_FIELDS = [
    "mint",
    "batch_id",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "horizon_seconds",
    "feature_asof_timestamp",
    "mint_first_seen_timestamp",
    "age_ms_at_horizon",
    "horizon_reached",
    "data_complete_for_horizon",
    "data_quality_exclusion",
    "provider_gap_exposed",
    "relay_gap_exposed",
    "sequence_gap_exposed",
    "hash_mismatch_exposed",
    "receiver_backpressure_exposed",
    "terminal_inconclusive_before_horizon",
    "rejected_before_horizon",
    "degraded_audit_only_before_horizon",
    "high_throughput_before_horizon",
    "trade_update_count_asof",
    "transaction_active_mint_count_asof",
    "pump_trade_active_mint_count_asof",
    "buy_count_delta_asof",
    "sell_count_delta_asof",
    "net_buy_sell_delta_asof",
    "volume_delta_asof",
    "unique_trade_accounts_asof",
    "last_trade_age_ms_asof",
    "trade_burst_score_asof",
    "trade_direction_imbalance_asof",
    "holder_update_count_asof",
    "unique_holder_accounts_seen_asof",
    "top_holder_concentration_asof",
    "dev_or_creator_holding_proxy_asof",
    "holder_churn_proxy_asof",
    "holder_collapse_proxy_asof",
    "new_holder_count_delta_asof",
    "exiting_holder_count_delta_asof",
    "vault_update_count_asof",
    "bonding_curve_update_count_asof",
    "liquidity_delta_asof",
    "reserve_delta_asof",
    "curve_progress_proxy_asof",
    "liquidity_exit_proxy_asof",
    "price_or_curve_move_proxy_asof",
    "early_avoid_decision_asof",
    "continue_tracking_decision_asof",
    "candidate_gate_status_asof",
    "candidate_first_failed_gate_asof",
    "candidate_failed_gate_reason_codes_asof",
    "survivor_extension_active_asof",
    "survival_horizon_reached_asof",
    "holder_rpc_used",
    "rpc_mint_supply_canonical",
    "threshold_tuning_allowed",
    "live_trading_enabled",
]

EARLY_AVOID_SCORE_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "horizon_seconds",
    "decision",
    "score",
    "reason_codes",
    "explanation",
    "trade_action",
]

CONTINUE_TRACKING_SCORE_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "horizon_seconds",
    "decision",
    "score",
    "reason_codes",
    "explanation",
    "trade_action",
]

CANDIDATE_ELIGIBILITY_SCORE_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "horizon_seconds",
    "decision",
    "score",
    "reason_codes",
    "explanation",
    "replay_eligible",
    "trade_action",
]

CANDIDATE_ELIGIBILITY_V1_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "horizon_seconds",
    "decision",
    "score",
    "reason_codes",
    "first_failed_candidate_gate",
    "replay_eligible",
    "candidate_checkpoint_seen",
    "label_quality",
    "final_outcome",
    "trade_action",
]

CANDIDATE_ELIGIBILITY_V2_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "horizon_seconds",
    "decision",
    "score",
    "trade_delta_bin",
    "buy_followthrough_bin",
    "sell_pressure_bin",
    "holder_growth_bin",
    "holder_risk_bin",
    "vault_curve_progress_bin",
    "liquidity_risk_bin",
    "reason_codes",
    "top_reason_code",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "trade_action",
]

CANDIDATE_POSITIVE_DISCOVERY_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "horizons_reached",
    "early_avoid_v1_decision",
    "continue_tracking_v1_decision",
    "candidate_eligibility_v2_decision",
    "top_reason_codes",
    "final_outcome",
    "censored_label",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "why_failed_or_passed_candidate_discovery",
    "next_data_needed",
]

CONTINUE_TO_CANDIDATE_GAP_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "final_outcome",
    "censored_label",
    "rejection_reason",
    "terminal_inconclusive_reason",
    "candidate_eligibility_decision",
    "candidate_eligibility_reason_codes",
    "first_failed_candidate_gate",
    "observation_horizon_reached",
    "as_of_feature_available",
    "provider_relay_data_quality_exposure",
    "high_throughput_mint",
    "degraded_active_mint",
    "why_not_candidate",
    "gap_classification",
]

SURVIVOR_DIAGNOSTIC_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "continued_reason",
    "observed_seconds",
    "horizons_reached",
    "candidate_gates_passed",
    "first_failed_candidate_gate",
    "censored",
    "data_quality_excluded",
    "high_throughput_mint",
    "degraded_active_mint",
    "missing_asof_features_prevented_evaluation",
    "candidate_eligibility_decision",
    "candidate_eligibility_reason_codes",
]

EXTENDED_ASOF_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "horizon_seconds",
    "feature_available",
    "asof_safe",
    "source_artifact",
    "missing_reason",
    "future_collection_required",
]

MISSING_ASOF_GROUPS = {
    "trade_delta": "per-mint event-time transaction/trade deltas are not retained in compact artifacts yet",
    "holder_state": "stream-authoritative holder/account/token-state fixed-horizon snapshots are not retained yet; holder RPC remains disabled",
    "vault_curve": "bonding curve/vault/liquidity fixed-horizon snapshots are not retained yet",
}


def utc_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def load_module(path: pathlib.Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_json(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def read_csv(path: pathlib.Path) -> list[dict[str, str]]:
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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (dict, list)):
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


def parse_mints(value: Any) -> set[str]:
    if isinstance(value, list):
        return {str(item) for item in value if str(item)}
    if isinstance(value, str):
        return {part for part in re.split(r"[|,;\\s]+", value) if part}
    return set()


def parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    cleaned = value.replace(" +00:00:00", "+00:00").replace(" UTC", "+00:00")
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        return None


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


def is_run_dir(path: pathlib.Path) -> bool:
    if not path.is_dir() or not path.name.startswith("relay") or path.name.endswith("-logs"):
        return False
    return any((path / name).exists() for name in ("countability_decision.json", "local_relay_dataset_proof_summary.json", "attempt_ledger.csv"))


@dataclass
class StrategyModuleResult:
    score: str
    decision: str
    reason_codes: list[str]
    explanation: str


class EarlyAvoidFilter:
    """Research-only avoid filter. No trading action is possible."""

    enabled = True
    tradeable = False

    def score(self, features: dict[str, Any]) -> StrategyModuleResult:
        reasons: list[str] = []
        if boolish(features.get("data_quality_provider_gap_exposed")):
            reasons.append("provider_gap_censored")
        if boolish(features.get("data_quality_hash_mismatch")):
            reasons.append("hash_mismatch_exclusion")
        if boolish(features.get("data_quality_receiver_backpressure")):
            reasons.append("receiver_backpressure_exclusion")
        if boolish(features.get("data_quality_degraded_active_mint")):
            reasons.append("degraded_audit_only")
        if boolish(features.get("data_quality_sequence_gap")):
            reasons.append("relay_sequence_gap")
        if not boolish(features.get("tracked_at_least_horizon")):
            reasons.append("insufficient_horizon_observation")
        if any(reason.endswith("_exclusion") or "gap" in reason for reason in reasons):
            decision = "audit_only"
        elif "insufficient_horizon_observation" in reasons:
            decision = "insufficient_data"
        elif boolish(features.get("label_clean_negative")) and boolish(features.get("tracked_at_least_horizon")):
            reasons.append("historical_clean_negative_pattern")
            decision = "avoid"
        else:
            decision = "continue_tracking"
        return StrategyModuleResult(
            "HIGH" if decision == "avoid" else "MEDIUM" if decision == "audit_only" else "LOW",
            decision,
            reasons,
            "Research-only avoid score; fixed descriptive bins, no threshold tuning, no trade action.",
        )


class ContinueTrackingGate:
    """Research-only continued observation gate."""

    enabled = True
    tradeable = False

    def score(self, features: dict[str, Any]) -> StrategyModuleResult:
        reasons: list[str] = []
        if not boolish(features.get("tracked_at_least_horizon")):
            reasons.append("insufficient_observation_at_horizon")
        if boolish(features.get("data_quality_provider_gap_exposed")):
            reasons.append("provider_gap_censored")
        if boolish(features.get("data_quality_sequence_gap")):
            reasons.append("relay_gap_censored")
        if boolish(features.get("data_quality_hash_mismatch")):
            reasons.append("hash_mismatch_censored")
        if boolish(features.get("data_quality_receiver_backpressure")):
            reasons.append("receiver_backpressure_censored")
        if boolish(features.get("data_quality_degraded_active_mint")):
            reasons.append("degraded_audit_only")
        if boolish(features.get("label_censored")):
            reasons.append("label_censored_not_dead")
        if any("censored" in reason or "gap" in reason or "mismatch" in reason for reason in reasons):
            decision = "censored"
        elif "degraded_audit_only" in reasons:
            decision = "audit_only"
        elif "insufficient_observation_at_horizon" in reasons:
            decision = "stop_tracking"
        else:
            decision = "continue_tracking"
        return StrategyModuleResult("MEDIUM", decision, reasons, "Research-only continue-tracking gate; no thresholds tuned.")


class CandidateEligibilityGate:
    """Research-only material candidate eligibility gate."""

    enabled = True
    tradeable = False

    def score(self, features: dict[str, Any]) -> StrategyModuleResult:
        reasons: list[str] = []
        for key in (
            "data_quality_sequence_gap",
            "data_quality_hash_mismatch",
            "data_quality_receiver_backpressure",
            "data_quality_provider_gap_exposed",
            "data_quality_degraded_active_mint",
        ):
            if boolish(features.get(key)):
                reasons.append(key)
        if boolish(features.get("label_censored")):
            reasons.append("censored_label")
        if not boolish(features.get("tracked_at_least_horizon")):
            reasons.append("insufficient_observation")
        if boolish(features.get("label_clean_negative")):
            reasons.append("historical_clean_negative")
        if boolish(features.get("label_clean_positive")):
            decision = "candidate_eligible_research_only"
        elif any(reason in {"censored_label", "data_quality_provider_gap_exposed", "data_quality_sequence_gap", "data_quality_hash_mismatch"} for reason in reasons):
            decision = "censored"
        else:
            decision = "not_eligible"
        return StrategyModuleResult("MEDIUM", decision, reasons, "Eligibility is descriptive, research-only, and disabled for trading.")


class BuySetupDraft:
    enabled = False
    tradeable = False
    wallet_execution_enabled = False

    def describe(self) -> dict[str, Any]:
        return {
            "status": "disabled_by_default",
            "tradeable": False,
            "wallet_execution_enabled": False,
            "setup_classes": ["future_clean_survivor_continuation", "future_liquidity_confirmation"],
            "required_evidence": ["clean positives", "replay allowed", "formal backtesting readiness", "operator approval"],
        }


class RiskAndExitDraft:
    enabled = False
    tradeable = False
    wallet_execution_enabled = False

    def describe(self) -> dict[str, Any]:
        return {
            "status": "disabled_by_default",
            "tradeable": False,
            "wallet_execution_enabled": False,
            "framework": ["max_loss_hypotheses", "liquidity_exit_hypotheses", "holder_concentration_invalidation"],
            "required_evidence": ["as-of survivor positives", "replay/backtest gate pass", "paper trading gate"],
        }


def load_batch_map(exporter: Any) -> dict[str, str]:
    if hasattr(exporter, "load_batch_map"):
        return exporter.load_batch_map(BATCH_LOG_ROOT)
    return {}


def uploaded_keys(r2: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for field in ("verified_files", "uploaded_files"):
        for value in r2.get(field) or []:
            if isinstance(value, str):
                keys.append(value)
    return sorted(set(keys))


def segment_clean(row: dict[str, Any], run_included: bool) -> bool:
    return (
        run_included
        and boolish(row.get("counted_phase107b_result"))
        and not boolish(row.get("provider_data_loss_seen"))
        and not boolish(row.get("client_backpressure_detected"))
        and not boolish(row.get("partial_outputs_audit_only"))
        and not str(row.get("blocker_class") or "").strip()
    )


def inspect_run(run_dir: pathlib.Path, exporter: Any, batch_map: dict[str, str]) -> dict[str, Any]:
    base = exporter.inspect_run(run_dir, batch_map)
    proof = read_json(run_dir / "local_relay_dataset_proof_summary.json")
    local = read_json(run_dir / "local_collector_summary.json")
    countability = read_json(run_dir / "countability_decision.json")
    run_countability = read_json(run_dir / "run_countability_decision.json")
    r2 = read_json(run_dir / "r2_upload_result.json")
    retention = read_json(run_dir / "local_retention_summary.json")
    hunter = read_json(run_dir / "hunter_summary.json")
    all_launch = read_json(run_dir / "all_launch_intake_summary.json")
    attempt_rows = read_csv(run_dir / "attempt_ledger.csv")
    rejected_rows = read_csv(run_dir / "rejected_summary.csv")
    candidate_rows = read_csv(run_dir / "candidate_summary.csv")
    all_launch_rows = read_csv(run_dir / "all_launch_intake_ledger.csv")
    rich_slot_rows = read_csv(run_dir / "rich_tracking_slot_ledger.csv")
    missed_good_rows = read_csv(run_dir / "missed_good_token_audit.csv")
    followup_manifest = read_json(run_dir / "all_launch_followup_manifest.json")
    promotion_summary = read_json(run_dir / "promotion_queue_summary.json")
    segment_rows = read_csv(run_dir / "run_segment_summary.csv")
    segment_attempt_total = sum(int_or_zero(row.get("attempted_launches")) for row in segment_rows)
    segment_rejected_total = sum(int_or_zero(row.get("rejected_count")) for row in segment_rows)
    segment_candidate_total = sum(int_or_zero(row.get("candidate_checkpoint_count")) for row in segment_rows)
    r2_ok = boolish(base.get("r2_verified")) or boolish(r2.get("verified")) or boolish(countability.get("r2_verified"))
    artifact_ok = str(base.get("artifact_consistency_ok")) in {"true", "True"} or boolish(countability.get("hard_invariants_passed"))
    counted = boolish(countability.get("counted_phase107b_result")) or boolish(proof.get("counted_phase107b_result"))
    holder_rpc_disabled = not boolish(proof.get("holder_rpc_enabled") or local.get("holder_rpc_enabled") or retention.get("holder_rpc_enabled"))
    rpc_noncanonical = not boolish(proof.get("rpc_mint_supply_canonical") or local.get("rpc_mint_supply_canonical") or countability.get("rpc_mint_supply_canonical") or retention.get("rpc_mint_supply_canonical"))
    replay_disabled = not boolish(countability.get("off_vps_candidate_replay_allowed") or proof.get("off_vps_candidate_replay_allowed") or local.get("off_vps_candidate_replay_allowed") or retention.get("replay_allowed"))
    backtesting_disabled = not boolish(proof.get("formal_backtesting_allowed") or local.get("formal_backtesting_allowed") or countability.get("formal_backtesting_allowed") or retention.get("formal_backtesting_allowed"))
    tuning_disabled = not boolish(proof.get("threshold_tuning_allowed") or local.get("threshold_tuning_allowed") or countability.get("threshold_tuning_allowed") or retention.get("threshold_tuning_allowed"))
    trading_disabled = not boolish(proof.get("live_trading_enabled") or local.get("live_trading_enabled") or retention.get("live_trading_enabled"))
    notes: list[str] = []
    if attempt_rows and len(attempt_rows) != int_or_zero(proof.get("attempted_launches") or run_countability.get("total_attempted_launches")):
        notes.append("attempt_rows_differ_from_proof_total_due_provider_gap_segment_accounting")
    if rejected_rows and len(rejected_rows) < int_or_zero(proof.get("rejected_dead_count")):
        notes.append("rejected_rows_less_than_proof_dead_count")
    reconciliation_ok = True
    exclusion: list[str] = []
    checks = [
        (counted, "counted_phase107b_result_false"),
        (r2_ok, "r2_not_verified"),
        (artifact_ok, "artifact_consistency_not_ok"),
        (int_or_zero(base.get("sequence_gap_count")) == 0, "sequence_gap_count_nonzero"),
        (int_or_zero(base.get("hash_mismatch_count")) == 0, "hash_mismatch_count_nonzero"),
        (int_or_zero(base.get("receiver_backpressure_count")) == 0, "receiver_backpressure_count_nonzero"),
        (holder_rpc_disabled, "holder_rpc_enabled"),
        (rpc_noncanonical, "rpc_mint_supply_canonical"),
        (replay_disabled, "replay_enabled"),
        (backtesting_disabled, "formal_backtesting_enabled"),
        (tuning_disabled, "threshold_tuning_enabled"),
        (trading_disabled, "trading_enabled"),
    ]
    for ok, reason in checks:
        if not ok:
            exclusion.append(reason)
    keys = uploaded_keys(r2)
    return {
        **base,
        "source_path": str(run_dir),
        "proof": proof,
        "local": local,
        "countability": countability,
        "run_countability": run_countability,
        "r2": r2,
        "retention": retention,
        "hunter": hunter,
        "attempt_rows": attempt_rows,
        "rejected_rows": rejected_rows,
        "candidate_rows": candidate_rows,
        "segment_rows": segment_rows,
        "r2_prefix": common_prefix(keys),
        "counted_phase107b_result": counted,
        "r2_verified": r2_ok,
        "artifact_consistency_ok": artifact_ok,
        "frames_received": int_or_zero(proof.get("frames_received") or local.get("frames_received")),
        "all_launches_seen": int_or_zero(all_launch.get("all_launches_seen")),
        "all_launches_indexed": int_or_zero(all_launch.get("all_launches_indexed") or len(all_launch_rows)),
        "rich_tracked_launches": int_or_zero(all_launch.get("rich_tracked_launches") or len(rich_slot_rows)),
        "cheap_only_launches": int_or_zero(all_launch.get("cheap_only_launches")),
        "skipped_due_budget": int_or_zero(all_launch.get("skipped_due_budget")),
        "fast_dead_dropped": int_or_zero(all_launch.get("fast_dead_dropped")),
        "missed_good_token_count": int_or_zero(all_launch.get("missed_good_token_count") or len([row for row in missed_good_rows if row.get("missed_good_token_classification") == "missed_due_to_budget"])),
        "tracking_slots_released": int_or_zero(all_launch.get("tracking_slots_released")),
        "cheap_followup_rows": int_or_zero(followup_manifest.get("total_rows")),
        "promotion_recommended_count": int_or_zero(promotion_summary.get("promotion_recommended_count")),
        "promotion_admitted_count": int_or_zero(promotion_summary.get("promotion_admitted_count")),
        "promotion_blocked_budget_count": int_or_zero(promotion_summary.get("promotion_blocked_budget_count")),
        "r2_uploaded": len(keys),
        "retention_deleted_bytes": int_or_zero(retention.get("deleted_bulk_bytes")),
        "local_retained_bytes": int_or_zero(retention.get("local_retained_bytes")),
        "attempt_ledger_rows": len(attempt_rows),
        "rejected_summary_rows": len(rejected_rows),
        "candidate_summary_rows": len(candidate_rows),
        "segment_attempt_total": segment_attempt_total,
        "segment_rejected_total": segment_rejected_total,
        "segment_candidate_total": segment_candidate_total,
        "holder_rpc_disabled": holder_rpc_disabled,
        "rpc_mint_supply_non_canonical": rpc_noncanonical,
        "replay_disabled": replay_disabled,
        "backtesting_disabled": backtesting_disabled,
        "threshold_tuning_disabled": tuning_disabled,
        "trading_disabled": trading_disabled,
        "included": not exclusion,
        "exclusion_reason": ";".join(exclusion),
        "reconciliation_ok": reconciliation_ok,
        "reconciliation_notes": ";".join(notes),
    }


def collect_segment_map(run: dict[str, Any]) -> dict[str, dict[str, Any]]:
    segment_map: dict[str, dict[str, Any]] = {}
    for row in run["segment_rows"]:
        segment_id = str(row.get("segment_id") or "")
        segment_json = read_json(pathlib.Path(run["source_path"]) / "segments" / f"segment_{segment_id}" / "segment_summary.json")
        segment_countability = read_json(pathlib.Path(run["source_path"]) / "segments" / f"segment_{segment_id}" / "countability_decision.json")
        payload = {**row, **segment_json, **segment_countability}
        payload["_clean"] = segment_clean(payload, boolish(run["included"]))
        segment_map[segment_id] = payload
    if not segment_map:
        segment_map[""] = {
            "segment_id": "",
            "counted_phase107b_result": run["counted_phase107b_result"],
            "_clean": boolish(run["included"]),
        }
    return segment_map


def collect_segment_attempts(run_dir: pathlib.Path, fallback: list[dict[str, str]], filename: str) -> list[tuple[str, dict[str, str]]]:
    rows: list[tuple[str, dict[str, str]]] = []
    segments_dir = run_dir / "segments"
    if segments_dir.exists():
        for segment_dir in sorted(segments_dir.glob("segment_*")):
            segment_id = segment_dir.name.replace("segment_", "")
            for row in read_csv(segment_dir / filename):
                rows.append((segment_id, row))
    if rows:
        return rows
    return [("", row) for row in fallback]


def build_labels(runs: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, dict[str, dict[str, Any]]]]:
    labels: list[dict[str, Any]] = []
    segment_maps = {run["slice_id"]: collect_segment_map(run) for run in runs}
    seen: set[str] = set()
    for run in runs:
        run_dir = pathlib.Path(run["source_path"])
        gap_mints: set[str] = set()
        for gap in read_csv(run_dir / "run_gap_events.csv"):
            gap_mints.update(parse_mints(gap.get("affected_mints")))
        degraded_mints = parse_mints(run["countability"].get("degraded_active_mints")) | parse_mints(run["hunter"].get("degraded_active_mints"))
        high_mints = parse_mints(run["hunter"].get("high_throughput_mints"))
        rejected_by = {
            (segment_id, row.get("mint", "")): row
            for segment_id, row in collect_segment_attempts(run_dir, run["rejected_rows"], "rejected_summary.csv")
        }
        candidate_by = {
            (segment_id, row.get("mint", "")): row
            for segment_id, row in collect_segment_attempts(run_dir, run["candidate_rows"], "candidate_summary.csv")
        }
        for segment_id, attempt in collect_segment_attempts(run_dir, run["attempt_rows"], "attempt_ledger.csv"):
            mint = str(attempt.get("mint") or "")
            if not mint or mint in seen:
                continue
            seen.add(mint)
            segment = segment_maps[run["slice_id"]].get(segment_id, {})
            rejected = rejected_by.get((segment_id, mint), {})
            candidate = candidate_by.get((segment_id, mint), {})
            final = str(attempt.get("final_state") or rejected.get("final_state") or candidate.get("final_state") or "")
            reason = str(attempt.get("rejection_or_promotion_reason") or rejected.get("rejection_class") or candidate.get("promotion_reason") or "")
            provider_gap = mint in gap_mints or boolish(segment.get("provider_data_loss_seen"))
            relay_gap = int_or_zero(run.get("sequence_gap_count")) > 0
            hash_gap = int_or_zero(run.get("hash_mismatch_count")) > 0
            receiver_gap = int_or_zero(run.get("receiver_backpressure_count")) > 0
            segment_is_clean = boolish(segment.get("_clean"))
            candidate_checkpoint = boolish(candidate.get("candidate_checkpoint"))
            countability_allows_replay = boolish(run["countability"].get("off_vps_candidate_replay_allowed")) and int_or_zero(run["countability"].get("replay_eligible_candidate_count")) > 0
            replay_eligible = boolish(candidate.get("replay_eligible")) and countability_allows_replay
            clean_negative = final == "early_rejected_dead" and segment_is_clean and not provider_gap and not relay_gap
            clean_positive = replay_eligible and not candidate_checkpoint and segment_is_clean
            censored = final == "terminal_inconclusive" or provider_gap or relay_gap or hash_gap or receiver_gap
            if clean_positive:
                label_quality = "clean_positive"
            elif clean_negative:
                label_quality = "clean_negative"
            elif censored:
                label_quality = "censored"
            elif candidate_checkpoint:
                label_quality = "audit_candidate_checkpoint"
            else:
                label_quality = "unknown_manual_review"
            tracked_ms = int_or_zero(attempt.get("tracked_until_seconds")) * 1000
            labels.append(
                {
                    "mint": mint,
                    "batch_id": run["batch_id"],
                    "slice_id": run["slice_id"],
                    "segment_id": segment_id,
                    "relay_session_id": run["relay_session_id"],
                    "first_seen_at": attempt.get("launch_timestamp", ""),
                    "created_at": attempt.get("launch_timestamp", ""),
                    "final_outcome": final,
                    "final_outcome_reason": reason,
                    "rejection_reason": rejected.get("rejection_class", "") if final == "early_rejected_dead" else "",
                    "terminal_inconclusive_reason": reason if final == "terminal_inconclusive" else "",
                    "time_to_rejection_ms": tracked_ms if final == "early_rejected_dead" else "",
                    "time_to_terminal_ms": tracked_ms if final == "terminal_inconclusive" else "",
                    "provider_gap_exposed": provider_gap,
                    "relay_gap_exposed": relay_gap,
                    "sequence_gap_exposed": int_or_zero(run.get("sequence_gap_count")) > 0,
                    "hash_mismatch_exposed": hash_gap,
                    "receiver_backpressure_exposed": receiver_gap,
                    "high_throughput_mint": mint in high_mints,
                    "degraded_active_mint": mint in degraded_mints,
                    "degraded_reason": "degraded_active_mint" if mint in degraded_mints else "",
                    "candidate_checkpoint_seen": candidate_checkpoint,
                    "replay_eligible": replay_eligible,
                    "clean_negative_label": clean_negative,
                    "clean_positive_label": clean_positive,
                    "censored_label": censored,
                    "label_quality": label_quality,
                    "source_artifacts": "|".join(
                        [
                            f"{run['source_path']}/attempt_ledger.csv",
                            f"{run['source_path']}/rejected_summary.csv",
                            f"{run['source_path']}/candidate_summary.csv",
                            f"{run['source_path']}/run_segment_summary.csv",
                        ]
                    ),
                    "_tracked_seconds": int_or_zero(attempt.get("tracked_until_seconds")),
                }
            )
    return labels, segment_maps


def build_asof_features(labels: list[dict[str, Any]], output_dir: pathlib.Path) -> list[dict[str, Any]]:
    asof_dir = output_dir / "asof_features"
    asof_dir.mkdir(parents=True, exist_ok=True)
    manifest_features = {
        "schema_version": "phase107h.asof_feature_manifest.v1",
        "horizons_seconds": list(HORIZONS),
        "feature_tables": [],
        "notes": [
            "Only point-in-time-safe launch timestamp and data-quality/filter features are emitted from currently retained compact artifacts.",
            "Final outcome, rejection reason, candidate/replay fields, R2/artifact status, and post-horizon data are excluded from feature columns.",
            "Trade/holder/vault deltas are marked unavailable until explicit event-time snapshot shards are retained going forward.",
        ],
        "unavailable_feature_groups": {
            "early_transaction_trade_count_deltas": "not retained as per-mint event-time snapshots in compact artifacts",
            "buy_sell_volume_deltas": "not retained as per-mint event-time snapshots in compact artifacts",
            "holder_account_token_state": "not retained as fixed-horizon snapshots; holder RPC remains disabled",
            "bonding_curve_vault_liquidity": "not retained as fixed-horizon snapshots",
        },
    }
    all_rows: list[dict[str, Any]] = []
    for horizon in HORIZONS:
        rows: list[dict[str, Any]] = []
        for label in labels:
            ts = parse_timestamp(str(label.get("first_seen_at") or ""))
            tracked = int_or_zero(label.get("_tracked_seconds"))
            row = {
                "mint": label["mint"],
                "batch_id": label["batch_id"],
                "slice_id": label["slice_id"],
                "segment_id": label["segment_id"],
                "relay_session_id": label["relay_session_id"],
                "first_seen_at": label["first_seen_at"],
                "horizon_seconds": horizon,
                "feature_available": True,
                "asof_safe": True,
                "launch_hour_utc": ts.hour if ts else "",
                "launch_day_of_week_utc": ts.weekday() if ts else "",
                "tracked_at_least_horizon": tracked >= horizon,
                "data_quality_sequence_gap": label["sequence_gap_exposed"],
                "data_quality_hash_mismatch": label["hash_mismatch_exposed"],
                "data_quality_receiver_backpressure": label["receiver_backpressure_exposed"],
                "data_quality_provider_gap_exposed": label["provider_gap_exposed"],
                "data_quality_relay_gap_exposed": label["relay_gap_exposed"],
                "data_quality_high_throughput_mint": label["high_throughput_mint"],
                "data_quality_degraded_active_mint": label["degraded_active_mint"],
                "label_clean_negative": label["clean_negative_label"],
                "label_clean_positive": label["clean_positive_label"],
                "label_censored": label["censored_label"],
                "label_quality": label["label_quality"],
            }
            rows.append(row)
        table = asof_dir / f"asof_features_{horizon:03d}s.csv"
        write_csv(table, rows, ASOF_FIELDS)
        manifest_features["feature_tables"].append({"horizon_seconds": horizon, "path": str(table), "rows": len(rows)})
        all_rows.extend(rows)
    write_json(asof_dir / "asof_feature_manifest.json", manifest_features)
    return all_rows


def alpha_feature_key(row: dict[str, Any]) -> tuple[str, str, str, int]:
    return (
        str(row.get("mint") or ""),
        str(row.get("slice_id") or ""),
        str(row.get("segment_id") or ""),
        int_or_zero(row.get("horizon_seconds")),
    )


def alpha_group_available(row: dict[str, Any], group: str) -> bool:
    fields = {
        "trade_delta": ("trade_update_count_asof", "buy_count_delta_asof", "sell_count_delta_asof"),
        "holder_state": ("holder_update_count_asof", "unique_holder_accounts_seen_asof", "top_holder_concentration_asof"),
        "vault_curve": ("vault_update_count_asof", "bonding_curve_update_count_asof", "curve_progress_proxy_asof"),
    }[group]
    return boolish(row.get("data_complete_for_horizon")) and any(str(row.get(field) or "").strip() for field in fields)


def collect_asof_alpha_features(
    runs: list[dict[str, Any]],
    output_dir: pathlib.Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    alpha_dir = output_dir / "asof_alpha_features"
    alpha_dir.mkdir(parents=True, exist_ok=True)
    all_rows: list[dict[str, Any]] = []
    source_runs: list[dict[str, Any]] = []
    for run in runs:
        run_dir = pathlib.Path(run["source_path"])
        run_alpha_dir = run_dir / "asof_alpha_features"
        manifest = read_json(run_alpha_dir / "asof_alpha_feature_manifest.json")
        if not manifest:
            continue
        run_rows = 0
        for horizon in HORIZONS:
            for row in read_csv(run_alpha_dir / f"asof_alpha_features_{horizon:03d}s.csv"):
                enriched = dict(row)
                enriched["batch_id"] = run.get("batch_id", "")
                enriched["slice_id"] = enriched.get("slice_id") or run.get("slice_id", "")
                enriched["relay_session_id"] = enriched.get("relay_session_id") or run.get("relay_session_id", "")
                enriched["source_path"] = str(run_dir)
                all_rows.append(enriched)
                run_rows += 1
        source_runs.append(
            {
                "slice_id": run.get("slice_id", ""),
                "source_path": str(run_dir),
                "rows": run_rows,
                "manifest": str(run_alpha_dir / "asof_alpha_feature_manifest.json"),
            }
        )

    for horizon in HORIZONS:
        rows = [row for row in all_rows if int_or_zero(row.get("horizon_seconds")) == horizon]
        write_csv(alpha_dir / f"asof_alpha_features_{horizon:03d}s.csv", rows, ASOF_ALPHA_FIELDS)

    groups: dict[str, Any] = {}
    for group in MISSING_ASOF_GROUPS:
        rows_with_group = [row for row in all_rows if alpha_group_available(row, group)]
        groups[group] = {
            "available": bool(rows_with_group),
            "rows": len(rows_with_group),
            "source_artifact": "asof_alpha_features/asof_alpha_features_<horizon>.csv" if rows_with_group else "legacy_slices_without_asof_alpha_features",
            "stream_authoritative": True,
            "as_of_safe": True,
            "missing_reason": "" if rows_with_group else "feature_unavailable_in_prior_slices",
            "future_collection_required": not bool(rows_with_group),
        }

    completeness = {
        "schema_version": "phase107h.strategy_asof_alpha_feature_completeness.v1",
        "total_rows": len(all_rows),
        "source_run_count": len(source_runs),
        "source_runs": source_runs,
        "groups": groups,
        "future_collection_required": any(not payload["available"] for payload in groups.values()),
        "holder_rpc_used": False,
        "rpc_mint_supply_canonical": False,
    }
    manifest = {
        "schema_version": "phase107h.strategy_asof_alpha_feature_manifest.v1",
        "horizons_seconds": list(HORIZONS),
        "total_rows": len(all_rows),
        "feature_tables": [
            {
                "horizon_seconds": horizon,
                "path": str(alpha_dir / f"asof_alpha_features_{horizon:03d}s.csv"),
                "rows": sum(1 for row in all_rows if int_or_zero(row.get("horizon_seconds")) == horizon),
            }
            for horizon in HORIZONS
        ],
        "source_run_count": len(source_runs),
        "holder_rpc_used": False,
        "rpc_mint_supply_canonical": False,
    }
    write_json(alpha_dir / "asof_alpha_feature_manifest.json", manifest)
    write_json(alpha_dir / "asof_alpha_feature_completeness.json", completeness)
    lines = [
        "# As-Of Alpha Feature Source Map",
        "",
        "These feature groups are retained from stream-authoritative local collector state only. No holder RPC or canonical RPC mint supply is used.",
        "",
        "## Groups",
    ]
    for group, payload in groups.items():
        lines.append(f"- {group}: available=`{str(payload['available']).lower()}` rows=`{payload['rows']}` source=`{payload['source_artifact']}` missing_reason=`{payload['missing_reason']}`")
    lines.extend(["", "## Source Runs", *[f"- {row['slice_id']}: rows={row['rows']}" for row in source_runs[:200]]])
    (output_dir / "asof_alpha_feature_source_map.md").write_text("\n".join(lines) + "\n")
    write_json(
        output_dir / "asof_alpha_feature_source_map.json",
        {
            "schema_version": "phase107h.asof_alpha_feature_source_map.v1",
            "groups": groups,
            "source_runs": source_runs,
            "holder_rpc_used": False,
            "rpc_mint_supply_canonical": False,
        },
    )
    return all_rows, completeness


def merge_asof_alpha_into_base(
    asof_rows: list[dict[str, Any]],
    alpha_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    alpha_by_key = {alpha_feature_key(row): row for row in alpha_rows}
    merged: list[dict[str, Any]] = []
    for row in asof_rows:
        next_row = dict(row)
        alpha = alpha_by_key.get(alpha_feature_key(row))
        next_row["asof_alpha_feature_available"] = bool(alpha)
        for group in MISSING_ASOF_GROUPS:
            next_row[f"asof_alpha_{group}_available"] = bool(alpha and alpha_group_available(alpha, group))
        if alpha:
            next_row["asof_alpha_source_artifact"] = alpha.get("source_path", "")
            next_row["feature_available"] = True
            next_row["tracked_at_least_horizon"] = boolish(alpha.get("horizon_reached"))
            next_row["data_quality_provider_gap_exposed"] = boolish(alpha.get("provider_gap_exposed"))
            next_row["data_quality_relay_gap_exposed"] = boolish(alpha.get("relay_gap_exposed"))
            next_row["data_quality_sequence_gap"] = boolish(alpha.get("sequence_gap_exposed"))
            next_row["data_quality_hash_mismatch"] = boolish(alpha.get("hash_mismatch_exposed"))
            next_row["data_quality_receiver_backpressure"] = boolish(alpha.get("receiver_backpressure_exposed"))
        merged.append(next_row)
    return merged


def feature_availability(alpha_completeness: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    alpha_groups = (alpha_completeness or {}).get("groups") or {}
    trade_available = boolish(alpha_groups.get("trade_delta", {}).get("available"))
    holder_available = boolish(alpha_groups.get("holder_state", {}).get("available"))
    vault_available = boolish(alpha_groups.get("vault_curve", {}).get("available"))
    return [
        {
            "name": "launch_hour_utc",
            "description": "UTC hour from first-seen launch timestamp.",
            "source_artifact": "attempt_ledger.csv",
            "stream_authoritative": True,
            "as_of_safe": True,
            "requires_future_data": False,
            "allowed_for_strategy_research": True,
            "allowed_for_backtest_alpha": False,
            "audit_only": False,
            "data_quality_only": False,
            "missing_reason": "",
            "future_collection_required": False,
        },
        {
            "name": "launch_day_of_week_utc",
            "description": "UTC day of week from first-seen launch timestamp.",
            "source_artifact": "attempt_ledger.csv",
            "stream_authoritative": True,
            "as_of_safe": True,
            "requires_future_data": False,
            "allowed_for_strategy_research": True,
            "allowed_for_backtest_alpha": False,
            "audit_only": False,
            "data_quality_only": False,
            "missing_reason": "",
            "future_collection_required": False,
        },
        {
            "name": "tracked_at_least_horizon",
            "description": "Observation coverage flag for a requested as-of horizon; data quality/availability only.",
            "source_artifact": "attempt_ledger.csv",
            "stream_authoritative": True,
            "as_of_safe": True,
            "requires_future_data": False,
            "allowed_for_strategy_research": True,
            "allowed_for_backtest_alpha": False,
            "audit_only": True,
            "data_quality_only": True,
            "missing_reason": "",
            "future_collection_required": False,
        },
        {
            "name": "early_transaction_trade_count_deltas",
            "description": "Fixed-horizon trade/transaction deltas retained from stream-authoritative local collector state.",
            "source_artifact": "asof_alpha_features/asof_alpha_features_<horizon>.csv" if trade_available else "legacy_slices_without_asof_alpha_features",
            "stream_authoritative": True,
            "as_of_safe": trade_available,
            "requires_future_data": False,
            "allowed_for_strategy_research": trade_available,
            "allowed_for_backtest_alpha": trade_available,
            "audit_only": False,
            "data_quality_only": False,
            "missing_reason": "" if trade_available else "feature_unavailable_in_prior_slices",
            "future_collection_required": not trade_available,
        },
        {
            "name": "holder_account_token_state",
            "description": "Stream-authoritative holder/account deltas; holder RPC remains disabled.",
            "source_artifact": "asof_alpha_features/asof_alpha_features_<horizon>.csv" if holder_available else "legacy_slices_without_asof_alpha_features",
            "stream_authoritative": True,
            "as_of_safe": holder_available,
            "requires_future_data": False,
            "allowed_for_strategy_research": holder_available,
            "allowed_for_backtest_alpha": holder_available,
            "audit_only": False,
            "data_quality_only": False,
            "missing_reason": "" if holder_available else "feature_unavailable_in_prior_slices",
            "future_collection_required": not holder_available,
        },
        {
            "name": "bonding_curve_vault_liquidity",
            "description": "Stream-authoritative vault/curve/liquidity deltas.",
            "source_artifact": "asof_alpha_features/asof_alpha_features_<horizon>.csv" if vault_available else "legacy_slices_without_asof_alpha_features",
            "stream_authoritative": True,
            "as_of_safe": vault_available,
            "requires_future_data": False,
            "allowed_for_strategy_research": vault_available,
            "allowed_for_backtest_alpha": vault_available,
            "audit_only": False,
            "data_quality_only": False,
            "missing_reason": "" if vault_available else "feature_unavailable_in_prior_slices",
            "future_collection_required": not vault_available,
        },
        {
            "name": "data_quality_*",
            "description": "Provider/relay/receiver/degraded flags for exclusions only.",
            "source_artifact": "countability_decision.json, run_gap_events.csv, hunter_summary.json",
            "stream_authoritative": True,
            "as_of_safe": True,
            "requires_future_data": False,
            "allowed_for_strategy_research": True,
            "allowed_for_backtest_alpha": False,
            "audit_only": True,
            "data_quality_only": True,
            "missing_reason": "",
            "future_collection_required": False,
        },
    ]


def write_feature_availability(output_dir: pathlib.Path, alpha_completeness: dict[str, Any] | None = None) -> None:
    rows = feature_availability(alpha_completeness)
    write_json(output_dir / "feature_availability_map.json", {"schema_version": "phase107h.feature_availability.v1", "features": rows})
    lines = ["# Feature Availability Map", ""]
    lines.append("| name | as_of_safe | research | backtest_alpha | audit_only | data_quality_only | missing_reason |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for row in rows:
        lines.append(
            f"| {row['name']} | {str(row['as_of_safe']).lower()} | {str(row['allowed_for_strategy_research']).lower()} | {str(row['allowed_for_backtest_alpha']).lower()} | {str(row['audit_only']).lower()} | {str(row['data_quality_only']).lower()} | {row['missing_reason']} |"
        )
    (output_dir / "feature_availability_map.md").write_text("\n".join(lines) + "\n")


def score_strategy_gates(output_dir: pathlib.Path, asof_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Score research-only strategy gates from as-of rows.

    The 60s horizon is used as the descriptive v0 scoring point because it is
    early enough for avoid/continue decisions and commonly available in current
    compact attempt ledgers. This is not threshold tuning.
    """

    scoring_rows = [row for row in asof_rows if int_or_zero(row.get("horizon_seconds")) == 60]
    early = EarlyAvoidFilter()
    continue_gate = ContinueTrackingGate()
    eligibility_gate = CandidateEligibilityGate()
    early_rows: list[dict[str, Any]] = []
    continue_rows: list[dict[str, Any]] = []
    eligibility_rows: list[dict[str, Any]] = []
    for row in scoring_rows:
        early_result = early.score(row)
        continue_result = continue_gate.score(row)
        eligibility_result = eligibility_gate.score(row)
        common = {
            "mint": row.get("mint"),
            "slice_id": row.get("slice_id"),
            "segment_id": row.get("segment_id"),
            "horizon_seconds": row.get("horizon_seconds"),
            "trade_action": "none",
        }
        early_rows.append(
            {
                **common,
                "decision": early_result.decision,
                "score": early_result.score,
                "reason_codes": "|".join(early_result.reason_codes),
                "explanation": early_result.explanation,
            }
        )
        continue_rows.append(
            {
                **common,
                "decision": continue_result.decision,
                "score": continue_result.score,
                "reason_codes": "|".join(continue_result.reason_codes),
                "explanation": continue_result.explanation,
            }
        )
        eligibility_rows.append(
            {
                **common,
                "decision": eligibility_result.decision,
                "score": eligibility_result.score,
                "reason_codes": "|".join(eligibility_result.reason_codes),
                "explanation": eligibility_result.explanation,
                "replay_eligible": False,
            }
        )
    write_csv(output_dir / "early_avoid_filter_v0_scores.csv", early_rows, EARLY_AVOID_SCORE_FIELDS)
    write_csv(output_dir / "continue_tracking_gate_v0_scores.csv", continue_rows, CONTINUE_TRACKING_SCORE_FIELDS)
    write_csv(output_dir / "candidate_eligibility_v0_scores.csv", eligibility_rows, CANDIDATE_ELIGIBILITY_SCORE_FIELDS)
    write_score_report(
        output_dir / "early_avoid_filter_v0_report.md",
        "EarlyAvoidFilter v0",
        early_rows,
        "Research-only early avoid logic. It emits no trade entries and uses fixed descriptive bins only.",
    )
    write_score_report(
        output_dir / "continue_tracking_gate_v0_report.md",
        "ContinueTrackingGate v0",
        continue_rows,
        "Research-only continue-tracking logic. Terminal inconclusive and gap-exposed rows remain censored.",
    )
    write_score_report(
        output_dir / "candidate_eligibility_v0_report.md",
        "CandidateEligibilityGate v0",
        eligibility_rows,
        "Research-only candidate eligibility structure. Candidate checkpoint alone is not positive and replay eligibility remains blocked.",
    )
    return early_rows, continue_rows, eligibility_rows


def label_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (str(row.get("mint") or ""), str(row.get("slice_id") or ""), str(row.get("segment_id") or ""))


def asof_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (str(row.get("mint") or ""), str(row.get("slice_id") or ""), str(row.get("segment_id") or ""))


def label_for_asof(row: dict[str, Any], labels_by_key: dict[tuple[str, str, str], dict[str, Any]]) -> dict[str, Any]:
    return labels_by_key.get(asof_key(row), {})


def v1_reason_codes(features: dict[str, Any], label: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if boolish(features.get("data_quality_provider_gap_exposed")) or boolish(label.get("provider_gap_exposed")):
        reasons.append("provider_gap_exposed")
    if boolish(features.get("data_quality_relay_gap_exposed")) or boolish(label.get("relay_gap_exposed")):
        reasons.append("relay_gap_exposed")
    if boolish(features.get("data_quality_sequence_gap")):
        reasons.append("relay_sequence_gap")
    if boolish(features.get("data_quality_hash_mismatch")):
        reasons.append("hash_mismatch_exposed")
    if boolish(features.get("data_quality_receiver_backpressure")):
        reasons.append("receiver_backpressure_exposed")
    if str(label.get("final_outcome") or "") == "terminal_inconclusive" or boolish(label.get("censored_label")):
        reasons.append("terminal_inconclusive")
    if boolish(features.get("data_quality_degraded_active_mint")) or boolish(label.get("degraded_active_mint")):
        reasons.append("degraded_audit_only")
    if not boolish(features.get("tracked_at_least_horizon")):
        reasons.append("insufficient_observation_horizon")
        reasons.append("no_survival_to_required_horizon")
    if boolish(label.get("clean_negative_label")):
        reason = str(label.get("rejection_reason") or label.get("final_outcome_reason") or "")
        if reason:
            normalized = re.sub(r"[^a-z0-9]+", "_", reason.lower()).strip("_")
            reasons.append(f"early_rejection_reason_{normalized}")
        else:
            reasons.append("early_rejection_reason_unknown")
    if not boolish(label.get("candidate_checkpoint_seen")):
        reasons.append("candidate_checkpoint_absent")
    if not boolish(label.get("replay_eligible")):
        reasons.append("replay_not_countability_allowed")
    feature_groups = [
        ("trade_delta", "missing_trade_delta_features"),
        ("holder_state", "missing_holder_snapshot_features"),
        ("vault_curve", "missing_vault_curve_features"),
    ]
    for group, missing_code in feature_groups:
        if boolish(features.get(f"asof_alpha_{group}_available")):
            reasons.append(f"feature_available_in_new_asof_alpha_slices:{group}")
        else:
            reasons.append(f"feature_unavailable_in_prior_slices:{group}")
            reasons.append(missing_code)
    if not boolish(label.get("clean_negative_label")) and not boolish(label.get("censored_label")):
        reasons.append("gate_too_strict_review")
    # Preserve order but remove duplicates.
    return list(dict.fromkeys(reasons))


def first_failed_candidate_gate(reasons: list[str]) -> str:
    gate_order = [
        "provider_gap_exposed",
        "relay_gap_exposed",
        "relay_sequence_gap",
        "hash_mismatch_exposed",
        "receiver_backpressure_exposed",
        "terminal_inconclusive",
        "degraded_audit_only",
        "insufficient_observation_horizon",
        "no_survival_to_required_horizon",
        "early_rejection_reason_volume_evaporated",
        "early_rejection_reason_no_buy_followthrough",
        "early_rejection_reason_top_holder_or_dev_dumped",
        "early_rejection_reason_holder_collapse",
        "early_rejection_reason_rug_like_by_60s",
        "early_rejection_reason_rug_like_by_300s",
        "early_rejection_reason_liquidity_exit_proxy",
        "early_rejection_reason_unknown",
        "missing_trade_delta_features",
        "missing_holder_snapshot_features",
        "missing_vault_curve_features",
        "feature_unavailable_in_prior_slices:trade_delta",
        "feature_unavailable_in_prior_slices:holder_state",
        "feature_unavailable_in_prior_slices:vault_curve",
        "candidate_checkpoint_absent",
        "replay_not_countability_allowed",
        "gate_too_strict_review",
    ]
    for gate in gate_order:
        if gate in reasons:
            return gate
    for reason in reasons:
        if reason.startswith("early_rejection_reason_"):
            return reason
    return ""


def candidate_decision_from_reasons(label: dict[str, Any], reasons: list[str]) -> str:
    if boolish(label.get("replay_eligible")) and not reasons:
        return "candidate_eligible_research_only"
    if boolish(label.get("censored_label")) or any(
        reason
        in {
            "provider_gap_exposed",
            "relay_gap_exposed",
            "relay_sequence_gap",
            "hash_mismatch_exposed",
            "receiver_backpressure_exposed",
            "terminal_inconclusive",
            "degraded_audit_only",
        }
        for reason in reasons
    ):
        return "censored"
    return "not_eligible"


def score_candidate_eligibility_v1(
    output_dir: pathlib.Path,
    labels: list[dict[str, Any]],
    asof_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    labels_by_key = {label_key(row): row for row in labels}
    scoring_rows = [row for row in asof_rows if int_or_zero(row.get("horizon_seconds")) == 60]
    rows: list[dict[str, Any]] = []
    for features in scoring_rows:
        label = label_for_asof(features, labels_by_key)
        reasons = v1_reason_codes(features, label)
        decision = candidate_decision_from_reasons(label, reasons)
        rows.append(
            {
                "mint": features.get("mint"),
                "slice_id": features.get("slice_id"),
                "segment_id": features.get("segment_id"),
                "horizon_seconds": features.get("horizon_seconds"),
                "decision": decision,
                "score": "MEDIUM",
                "reason_codes": "|".join(reasons),
                "first_failed_candidate_gate": first_failed_candidate_gate(reasons),
                "replay_eligible": boolish(label.get("replay_eligible")),
                "candidate_checkpoint_seen": boolish(label.get("candidate_checkpoint_seen")),
                "label_quality": label.get("label_quality", ""),
                "final_outcome": label.get("final_outcome", ""),
                "trade_action": "none",
            }
        )
    write_csv(output_dir / "candidate_eligibility_v1_scores.csv", rows, CANDIDATE_ELIGIBILITY_V1_FIELDS)
    write_score_report(
        output_dir / "candidate_eligibility_v1_report.md",
        "CandidateEligibilityGate v1",
        rows,
        "Diagnostic-only v1 eligibility. It diagnoses missing gates/features and does not loosen eligibility, tune thresholds, or emit trade actions.",
    )
    return rows


def alpha_rows_at_horizon(alpha_rows: list[dict[str, Any]], horizon: int = 60) -> list[dict[str, Any]]:
    return [row for row in alpha_rows if int_or_zero(row.get("horizon_seconds")) == horizon]


def numeric(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def alpha_quality_blocked(row: dict[str, Any]) -> bool:
    return any(
        boolish(row.get(field))
        for field in (
            "provider_gap_exposed",
            "relay_gap_exposed",
            "sequence_gap_exposed",
            "hash_mismatch_exposed",
            "receiver_backpressure_exposed",
        )
    )


def alpha_group_missing(row: dict[str, Any], group: str) -> bool:
    return not alpha_group_available(row, group)


def bin_trade_delta(row: dict[str, Any]) -> str:
    if alpha_group_missing(row, "trade_delta"):
        return "MISSING"
    trades = int_or_zero(row.get("trade_update_count_asof"))
    buys = int_or_zero(row.get("buy_count_delta_asof"))
    sells = int_or_zero(row.get("sell_count_delta_asof"))
    net = int_or_zero(row.get("net_buy_sell_delta_asof"))
    if trades <= 0 or buys <= 0:
        return "LOW"
    if buys > sells and net > 0 and trades >= 3:
        return "HIGH"
    return "MEDIUM"


def bin_buy_followthrough(row: dict[str, Any]) -> str:
    if alpha_group_missing(row, "trade_delta"):
        return "MISSING"
    buys = int_or_zero(row.get("buy_count_delta_asof"))
    net = int_or_zero(row.get("net_buy_sell_delta_asof"))
    if buys <= 0:
        return "LOW"
    if net > 0 and buys >= 2:
        return "HIGH"
    return "MEDIUM"


def bin_sell_pressure(row: dict[str, Any]) -> str:
    if alpha_group_missing(row, "trade_delta"):
        return "MISSING"
    buys = int_or_zero(row.get("buy_count_delta_asof"))
    sells = int_or_zero(row.get("sell_count_delta_asof"))
    net = int_or_zero(row.get("net_buy_sell_delta_asof"))
    if sells > buys or net < 0:
        return "HIGH"
    if sells == 0:
        return "LOW"
    return "MEDIUM"


def bin_holder_growth(row: dict[str, Any]) -> str:
    if alpha_group_missing(row, "holder_state"):
        return "MISSING"
    holders = int_or_zero(row.get("unique_holder_accounts_seen_asof"))
    new_holders = int_or_zero(row.get("new_holder_count_delta_asof"))
    exiting = int_or_zero(row.get("exiting_holder_count_delta_asof"))
    if holders <= 1 or new_holders <= exiting:
        return "LOW"
    if holders >= 3 and new_holders > exiting:
        return "HIGH"
    return "MEDIUM"


def bin_holder_risk(row: dict[str, Any]) -> str:
    if alpha_group_missing(row, "holder_state"):
        return "MISSING"
    concentration = numeric(row.get("top_holder_concentration_asof"))
    collapse = numeric(row.get("holder_collapse_proxy_asof"))
    creator_proxy = numeric(row.get("dev_or_creator_holding_proxy_asof"))
    exiting = int_or_zero(row.get("exiting_holder_count_delta_asof"))
    new_holders = int_or_zero(row.get("new_holder_count_delta_asof"))
    if (concentration is not None and concentration >= 0.75) or (collapse is not None and collapse > 0) or (creator_proxy is not None and creator_proxy > 0) or exiting > new_holders:
        return "HIGH"
    if concentration is not None and concentration >= 0.5:
        return "MEDIUM"
    return "LOW"


def bin_vault_curve_progress(row: dict[str, Any]) -> str:
    if alpha_group_missing(row, "vault_curve"):
        return "MISSING"
    updates = int_or_zero(row.get("vault_update_count_asof")) + int_or_zero(row.get("bonding_curve_update_count_asof"))
    curve = numeric(row.get("curve_progress_proxy_asof"))
    if updates <= 0 or curve is None or curve <= 0:
        return "LOW"
    if curve >= 0.5 or updates >= 3:
        return "HIGH"
    return "MEDIUM"


def bin_liquidity_risk(row: dict[str, Any]) -> str:
    if alpha_group_missing(row, "vault_curve"):
        return "MISSING"
    exit_proxy = numeric(row.get("liquidity_exit_proxy_asof"))
    liquidity_delta = numeric(row.get("liquidity_delta_asof"))
    if (exit_proxy is not None and exit_proxy > 0) or (liquidity_delta is not None and liquidity_delta < 0):
        return "HIGH"
    return "LOW"


def v2_alpha_reason_codes(alpha: dict[str, Any], label: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    forbidden_alpha_columns = {
        "final_outcome",
        "final_outcome_reason",
        "rejection_reason",
        "terminal_inconclusive_reason",
        "candidate_checkpoint_seen",
        "replay_eligible",
        "r2_verified",
        "artifact_consistency_ok",
    }
    if any(str(alpha.get(field) or "").strip() for field in forbidden_alpha_columns):
        reasons.append("forbidden_future_leakage_column")
    if boolish(alpha.get("provider_gap_exposed")) or boolish(label.get("provider_gap_exposed")):
        reasons.append("provider_gap_exposed")
    if boolish(alpha.get("relay_gap_exposed")) or boolish(label.get("relay_gap_exposed")):
        reasons.append("relay_gap_exposed")
    if boolish(alpha.get("sequence_gap_exposed")):
        reasons.append("relay_gap_exposed")
    if boolish(alpha.get("hash_mismatch_exposed")):
        reasons.append("hash_mismatch_exposed")
    if boolish(alpha.get("receiver_backpressure_exposed")):
        reasons.append("receiver_backpressure_exposed")
    if not boolish(alpha.get("horizon_reached")):
        reasons.append("missing_horizon")
    if boolish(alpha.get("terminal_inconclusive_before_horizon")) or str(label.get("final_outcome") or "") == "terminal_inconclusive":
        reasons.append("terminal_inconclusive")
    if boolish(alpha.get("rejected_before_horizon")):
        reasons.append("rejected_before_horizon")
    if boolish(alpha.get("degraded_audit_only_before_horizon")) or boolish(label.get("degraded_active_mint")):
        reasons.append("high_throughput_degraded_audit_only")
    elif boolish(alpha.get("high_throughput_before_horizon")):
        reasons.append("high_throughput_clean_observation")

    if alpha_group_missing(alpha, "trade_delta"):
        reasons.append("missing_asof_trade_delta")
    else:
        if bin_trade_delta(alpha) == "LOW":
            reasons.append("insufficient_trade_delta_strength")
        if bin_buy_followthrough(alpha) == "LOW":
            reasons.append("insufficient_buy_followthrough")
        if bin_sell_pressure(alpha) == "HIGH":
            reasons.append("adverse_sell_pressure")

    if alpha_group_missing(alpha, "holder_state"):
        reasons.append("missing_asof_holder_state")
    else:
        if bin_holder_risk(alpha) == "HIGH":
            reasons.append("holder_concentration_risk")
            if numeric(alpha.get("holder_collapse_proxy_asof")) and numeric(alpha.get("holder_collapse_proxy_asof")) > 0:
                reasons.append("holder_collapse_risk")
            if numeric(alpha.get("dev_or_creator_holding_proxy_asof")) and numeric(alpha.get("dev_or_creator_holding_proxy_asof")) > 0:
                reasons.append("dev_or_creator_holding_risk")
        if bin_holder_growth(alpha) == "LOW":
            reasons.append("weak_holder_growth")

    if alpha_group_missing(alpha, "vault_curve"):
        reasons.append("missing_asof_vault_curve")
    else:
        if bin_vault_curve_progress(alpha) == "LOW":
            reasons.append("weak_vault_curve_progress")
            reasons.append("insufficient_curve_progress")
        if bin_liquidity_risk(alpha) == "HIGH":
            reasons.append("liquidity_exit_risk")

    if not boolish(label.get("candidate_checkpoint_seen")):
        reasons.append("candidate_checkpoint_absent")
    if not boolish(label.get("replay_eligible")):
        reasons.append("replay_not_countability_allowed")
    return list(dict.fromkeys(reasons))


def v2_decision(label: dict[str, Any], reasons: list[str]) -> str:
    if "forbidden_future_leakage_column" in reasons:
        return "audit_only"
    if any(reason in reasons for reason in ("provider_gap_exposed", "relay_gap_exposed", "hash_mismatch_exposed", "receiver_backpressure_exposed", "terminal_inconclusive")):
        return "censored"
    if "high_throughput_degraded_audit_only" in reasons:
        return "audit_only"
    if any(reason.startswith("missing_asof_") or reason == "missing_horizon" for reason in reasons):
        return "insufficient_data"
    if boolish(label.get("candidate_checkpoint_seen")) and boolish(label.get("replay_eligible")):
        blocking = {
            "rejected_before_horizon",
            "insufficient_trade_delta_strength",
            "insufficient_buy_followthrough",
            "adverse_sell_pressure",
            "holder_concentration_risk",
            "holder_collapse_risk",
            "dev_or_creator_holding_risk",
            "weak_holder_growth",
            "weak_vault_curve_progress",
            "liquidity_exit_risk",
            "insufficient_curve_progress",
        }
        if not any(reason in blocking for reason in reasons):
            return "eligible"
    return "not_eligible"


def v2_score_bin(decision: str, reasons: list[str]) -> str:
    if decision == "censored":
        return "CENSORED"
    if decision == "audit_only" or any(reason.endswith("_risk") for reason in reasons):
        return "UNSAFE"
    if decision == "insufficient_data":
        return "MISSING"
    if decision == "eligible":
        return "HIGH"
    if "candidate_checkpoint_absent" in reasons or "replay_not_countability_allowed" in reasons:
        return "LOW"
    return "MEDIUM"


def score_alpha_strategy_gates_v1_v2(
    output_dir: pathlib.Path,
    labels: list[dict[str, Any]],
    alpha_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    labels_by_key = {label_key(row): row for row in labels}
    scoring_rows = alpha_rows_at_horizon(alpha_rows, 60)
    early_rows: list[dict[str, Any]] = []
    continue_rows: list[dict[str, Any]] = []
    v2_rows: list[dict[str, Any]] = []
    for alpha in scoring_rows:
        label = labels_by_key.get(asof_key(alpha), {})
        reasons = v2_alpha_reason_codes(alpha, label)
        decision = v2_decision(label, reasons)
        quality_blocked = alpha_quality_blocked(alpha)
        trade_bin = bin_trade_delta(alpha)
        buy_bin = bin_buy_followthrough(alpha)
        sell_bin = bin_sell_pressure(alpha)
        holder_growth_bin = bin_holder_growth(alpha)
        holder_risk_bin = bin_holder_risk(alpha)
        vault_bin = bin_vault_curve_progress(alpha)
        liquidity_bin = bin_liquidity_risk(alpha)

        if quality_blocked:
            early_decision = "audit_only"
            early_reasons = ["data_quality_exclusion"]
        elif boolish(alpha.get("terminal_inconclusive_before_horizon")):
            early_decision = "audit_only"
            early_reasons = ["terminal_inconclusive_censored"]
        elif not boolish(alpha.get("horizon_reached")):
            early_decision = "insufficient_data"
            early_reasons = ["missing_horizon"]
        elif boolish(alpha.get("rejected_before_horizon")) or buy_bin == "LOW" or sell_bin == "HIGH":
            early_decision = "avoid"
            early_reasons = [reason for reason in ("rejected_before_horizon", "insufficient_buy_followthrough", "adverse_sell_pressure") if reason in reasons]
        else:
            early_decision = "continue_tracking"
            early_reasons = ["asof_alpha_no_early_avoid_blocker"]

        if quality_blocked or boolish(alpha.get("terminal_inconclusive_before_horizon")):
            continue_decision = "censored"
            continue_reasons = ["data_quality_or_terminal_censored"]
        elif boolish(alpha.get("degraded_audit_only_before_horizon")):
            continue_decision = "audit_only"
            continue_reasons = ["high_throughput_degraded_audit_only"]
        elif not boolish(alpha.get("horizon_reached")):
            continue_decision = "stop_tracking"
            continue_reasons = ["missing_horizon"]
        elif boolish(alpha.get("rejected_before_horizon")):
            continue_decision = "stop_tracking"
            continue_reasons = ["rejected_before_horizon"]
        elif buy_bin in {"MEDIUM", "HIGH"} and holder_risk_bin != "HIGH" and liquidity_bin != "HIGH":
            continue_decision = "continue_tracking"
            continue_reasons = ["asof_alpha_continue_observation"]
        else:
            continue_decision = "stop_tracking"
            continue_reasons = [reason for reason in reasons if reason not in {"candidate_checkpoint_absent", "replay_not_countability_allowed"}] or ["weak_alpha_profile"]

        common = {
            "mint": alpha.get("mint"),
            "slice_id": alpha.get("slice_id"),
            "segment_id": alpha.get("segment_id"),
            "horizon_seconds": alpha.get("horizon_seconds"),
            "trade_action": "none",
        }
        early_rows.append(
            {
                **common,
                "decision": early_decision,
                "score": "HIGH" if early_decision == "avoid" else "MISSING" if early_decision == "insufficient_data" else "LOW",
                "reason_codes": "|".join(dict.fromkeys(early_reasons)),
                "explanation": "EarlyAvoidFilter v1 uses as-of alpha diagnostics only; no trade action or threshold tuning.",
            }
        )
        continue_rows.append(
            {
                **common,
                "decision": continue_decision,
                "score": "MEDIUM" if continue_decision == "continue_tracking" else "CENSORED" if continue_decision == "censored" else "LOW",
                "reason_codes": "|".join(dict.fromkeys(continue_reasons)),
                "explanation": "ContinueTrackingGate v1 uses as-of alpha diagnostics only; terminal inconclusive remains censored.",
            }
        )
        v2_rows.append(
            {
                "mint": alpha.get("mint"),
                "slice_id": alpha.get("slice_id"),
                "segment_id": alpha.get("segment_id"),
                "relay_session_id": alpha.get("relay_session_id"),
                "horizon_seconds": alpha.get("horizon_seconds"),
                "decision": decision,
                "score": v2_score_bin(decision, reasons),
                "trade_delta_bin": trade_bin,
                "buy_followthrough_bin": buy_bin,
                "sell_pressure_bin": sell_bin,
                "holder_growth_bin": holder_growth_bin,
                "holder_risk_bin": holder_risk_bin,
                "vault_curve_progress_bin": vault_bin,
                "liquidity_risk_bin": liquidity_bin,
                "reason_codes": "|".join(reasons),
                "top_reason_code": first_failed_candidate_gate(reasons) or (reasons[0] if reasons else ""),
                "candidate_checkpoint_seen": boolish(label.get("candidate_checkpoint_seen")),
                "replay_eligible": boolish(label.get("replay_eligible")),
                "trade_action": "none",
            }
        )
    write_csv(output_dir / "early_avoid_filter_v1_scores.csv", early_rows, EARLY_AVOID_SCORE_FIELDS)
    write_csv(output_dir / "continue_tracking_gate_v1_scores.csv", continue_rows, CONTINUE_TRACKING_SCORE_FIELDS)
    write_csv(output_dir / "candidate_eligibility_v2_scores.csv", v2_rows, CANDIDATE_ELIGIBILITY_V2_FIELDS)
    write_score_report(
        output_dir / "early_avoid_filter_v1_report.md",
        "EarlyAvoidFilter v1",
        early_rows,
        "Research-only v1 using as-of alpha feature bins. No buy entries, no threshold optimization, no replay/backtesting.",
    )
    write_score_report(
        output_dir / "continue_tracking_gate_v1_report.md",
        "ContinueTrackingGate v1",
        continue_rows,
        "Research-only v1 using as-of alpha feature bins. Terminal inconclusive remains censored.",
    )
    write_score_report(
        output_dir / "candidate_eligibility_v2_report.md",
        "CandidateEligibilityGate v2",
        v2_rows,
        "Diagnostic-only v2 using as-of trade/holder/vault alpha groups. Candidate checkpoints are not replay-eligible unless countability allows replay.",
    )
    return early_rows, continue_rows, v2_rows


def candidate_positive_discovery_report(
    output_dir: pathlib.Path,
    labels: list[dict[str, Any]],
    alpha_rows: list[dict[str, Any]],
    early_v1: list[dict[str, Any]],
    continue_v1: list[dict[str, Any]],
    v2_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    labels_by_key = {label_key(row): row for row in labels}
    alpha_by_key: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in alpha_rows:
        alpha_by_key[asof_key(row)].append(row)
    early_by_key = {label_key(row): row for row in early_v1}
    continue_by_key = {label_key(row): row for row in continue_v1}
    v2_by_key = {label_key(row): row for row in v2_rows}
    rows: list[dict[str, Any]] = []
    for key, horizons in sorted(alpha_by_key.items()):
        label = labels_by_key.get(key, {})
        reached = sorted(int_or_zero(row.get("horizon_seconds")) for row in horizons if boolish(row.get("horizon_reached")))
        v2 = v2_by_key.get(key, {})
        reasons = [part for part in str(v2.get("reason_codes") or "").split("|") if part]
        decision = str(v2.get("decision") or "insufficient_data")
        if decision == "eligible":
            why = "candidate_discovery_passed_research_only"
            next_data = "operator_review_before_replay_or_backtesting"
        elif boolish(label.get("censored_label")):
            why = "censored_or_incomplete_observation"
            next_data = "clean_uninterrupted_survivor_observation"
        elif any(reason.startswith("missing_asof") or reason == "missing_horizon" for reason in reasons):
            why = "insufficient_horizon_or_missing_alpha_features"
            next_data = "more as-of-alpha-complete slices at required horizons"
        elif "candidate_checkpoint_absent" in reasons:
            why = "no_candidate_checkpoint_observed"
            next_data = "fresh survivor-extension collection under same caps"
        else:
            why = "failed_behavior_or_conservative_gate"
            next_data = "more clean survivor examples; do not loosen gates without evidence"
        rows.append(
            {
                "mint": key[0],
                "slice_id": key[1],
                "segment_id": key[2],
                "relay_session_id": horizons[0].get("relay_session_id", "") if horizons else "",
                "horizons_reached": "|".join(str(item) for item in reached),
                "early_avoid_v1_decision": early_by_key.get(key, {}).get("decision", ""),
                "continue_tracking_v1_decision": continue_by_key.get(key, {}).get("decision", ""),
                "candidate_eligibility_v2_decision": decision,
                "top_reason_codes": "|".join(reasons[:5]),
                "final_outcome": label.get("final_outcome", ""),
                "censored_label": boolish(label.get("censored_label")),
                "candidate_checkpoint_seen": boolish(label.get("candidate_checkpoint_seen")),
                "replay_eligible": boolish(label.get("replay_eligible")),
                "why_failed_or_passed_candidate_discovery": why,
                "next_data_needed": next_data,
            }
        )
    write_csv(output_dir / "candidate_positive_discovery.csv", rows, CANDIDATE_POSITIVE_DISCOVERY_FIELDS)
    decisions = Counter(row["candidate_eligibility_v2_decision"] for row in rows)
    reasons = Counter()
    for row in rows:
        for reason in str(row.get("top_reason_codes") or "").split("|"):
            if reason:
                reasons[reason] += 1
    lines = [
        "# Candidate Positive Discovery Report",
        "",
        f"As-of-alpha-complete mint rows: {len(rows)}",
        "",
        "## CandidateEligibilityGate v2 Decisions",
        *[f"- {key}: {count}" for key, count in sorted(decisions.items())],
        "",
        "## Top Reason Codes",
        *[f"- {key}: {count}" for key, count in reasons.most_common(25)],
        "",
        "No replay, formal backtesting, threshold tuning, live trading, wallet execution, holder RPC, or canonical RPC mint supply was used.",
    ]
    (output_dir / "candidate_positive_discovery_report.md").write_text("\n".join(lines) + "\n")
    return rows


def write_candidate_review_pack_if_needed(
    output_dir: pathlib.Path,
    labels: list[dict[str, Any]],
    alpha_rows: list[dict[str, Any]],
    v2_rows: list[dict[str, Any]],
    readiness: dict[str, Any],
) -> str:
    candidates = [
        row
        for row in labels
        if boolish(row.get("candidate_checkpoint_seen")) or boolish(row.get("replay_eligible"))
    ]
    if not candidates:
        return ""
    stamp = utc_stamp()
    pack_dir = output_dir / f"candidate_review_pack_{stamp}"
    pack_dir.mkdir(parents=True, exist_ok=True)
    candidate_keys = {label_key(row) for row in candidates}
    alpha_subset = [row for row in alpha_rows if asof_key(row) in candidate_keys]
    v2_subset = [row for row in v2_rows if label_key(row) in candidate_keys]
    write_csv(pack_dir / "candidate_mints.csv", candidates, MINT_FIELDS)
    write_csv(pack_dir / "candidate_asof_alpha_features.csv", alpha_subset, ASOF_ALPHA_FIELDS)
    write_csv(pack_dir / "candidate_eligibility_v2_scores.csv", v2_subset, CANDIDATE_ELIGIBILITY_V2_FIELDS)
    write_json(
        pack_dir / "candidate_review_decision.json",
        {
            "schema_version": "phase107h.candidate_review_pack.v1",
            "candidate_count": len(candidates),
            "replay_eligible_count": sum(1 for row in candidates if boolish(row.get("replay_eligible"))),
            "replay_ready": bool(readiness.get("replay_ready")),
            "backtesting_ready": bool(readiness.get("backtesting_ready")),
            "threshold_tuning_ready": bool(readiness.get("threshold_tuning_ready")),
            "live_trading_ready": bool(readiness.get("live_trading_ready")),
            "operator_review_required": True,
            "replay_was_run": False,
            "backtesting_was_run": False,
            "trading_was_enabled": False,
        },
    )
    lines = [
        "# Candidate Review Pack",
        "",
        f"Candidate rows: {len(candidates)}",
        f"Replay-eligible rows: {sum(1 for row in candidates if boolish(row.get('replay_eligible')))}",
        "",
        "Replay, formal backtesting, threshold tuning, live trading, wallet execution, holder RPC, and canonical RPC mint supply remain blocked unless separate gates and operator approval allow them.",
    ]
    (pack_dir / "README.md").write_text("\n".join(lines) + "\n")
    return str(pack_dir)


def classify_continue_gap(label: dict[str, Any], reasons: list[str]) -> str:
    if any(reason in reasons for reason in ("provider_gap_exposed", "relay_gap_exposed", "relay_sequence_gap", "hash_mismatch_exposed", "receiver_backpressure_exposed")):
        return "DATA_QUALITY_EXCLUDED"
    if boolish(label.get("censored_label")) or "terminal_inconclusive" in reasons:
        return "CENSORED_OR_INCOMPLETE"
    if any(reason.startswith("early_rejection_reason_") for reason in reasons):
        return "FAILED_BEHAVIOR"
    if any(reason.startswith("missing_") or reason.startswith("feature_unavailable_in_prior_slices") for reason in reasons):
        return "MISSING_ASOF_FEATURES"
    if "gate_too_strict_review" in reasons:
        return "GATE_TOO_STRICT_REVIEW"
    return "UNKNOWN_NEEDS_TELEMETRY"


def continue_to_candidate_gap_analysis(
    output_dir: pathlib.Path,
    labels: list[dict[str, Any]],
    asof_rows: list[dict[str, Any]],
    continue_scores: list[dict[str, Any]],
    v1_scores: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    labels_by_key = {label_key(row): row for row in labels}
    asof_by_key = {asof_key(row): row for row in asof_rows if int_or_zero(row.get("horizon_seconds")) == 60}
    v1_by_key = {label_key(row): row for row in v1_scores}
    rows: list[dict[str, Any]] = []
    for score in continue_scores:
        if score.get("decision") != "continue_tracking":
            continue
        key = label_key(score)
        label = labels_by_key.get(key, {})
        asof = asof_by_key.get(key, {})
        v1 = v1_by_key.get(key, {})
        reasons = [part for part in str(v1.get("reason_codes") or "").split("|") if part]
        failed_gate = str(v1.get("first_failed_candidate_gate") or first_failed_candidate_gate(reasons))
        gap_class = classify_continue_gap(label, reasons)
        rows.append(
            {
                "mint": score.get("mint"),
                "slice_id": score.get("slice_id"),
                "segment_id": score.get("segment_id"),
                "final_outcome": label.get("final_outcome", ""),
                "censored_label": boolish(label.get("censored_label")),
                "rejection_reason": label.get("rejection_reason", ""),
                "terminal_inconclusive_reason": label.get("terminal_inconclusive_reason", ""),
                "candidate_eligibility_decision": v1.get("decision", ""),
                "candidate_eligibility_reason_codes": v1.get("reason_codes", ""),
                "first_failed_candidate_gate": failed_gate,
                "observation_horizon_reached": boolish(asof.get("tracked_at_least_horizon")),
                "as_of_feature_available": boolish(asof.get("feature_available")),
                "provider_relay_data_quality_exposure": any(
                    boolish(label.get(field))
                    for field in (
                        "provider_gap_exposed",
                        "relay_gap_exposed",
                        "sequence_gap_exposed",
                        "hash_mismatch_exposed",
                        "receiver_backpressure_exposed",
                    )
                ),
                "high_throughput_mint": boolish(label.get("high_throughput_mint")),
                "degraded_active_mint": boolish(label.get("degraded_active_mint")),
                "why_not_candidate": failed_gate or "no_replay_eligible_candidate_checkpoint",
                "gap_classification": gap_class,
            }
        )
    write_csv(output_dir / "continue_to_candidate_gap_analysis.csv", rows, CONTINUE_TO_CANDIDATE_GAP_FIELDS)
    classes = Counter(row["gap_classification"] for row in rows)
    gates = Counter(row["first_failed_candidate_gate"] for row in rows)
    lines = [
        "# Continue-To-Candidate Gap Analysis",
        "",
        f"Continue-tracking rows analyzed: {len(rows)}",
        "",
        "## Gap Classes",
        *[f"- {key}: {count}" for key, count in sorted(classes.items())],
        "",
        "## First Failed Candidate Gates",
        *[f"- {key}: {count}" for key, count in gates.most_common(20)],
        "",
        "This is diagnostic only. No gates were loosened, no replay/backtesting/tuning/trading was run, and terminal_inconclusive remains censored.",
    ]
    (output_dir / "continue_to_candidate_gap_analysis.md").write_text("\n".join(lines) + "\n")
    return rows


def write_extended_asof_feature_placeholders(
    output_dir: pathlib.Path,
    labels: list[dict[str, Any]],
    alpha_rows: list[dict[str, Any]] | None = None,
    alpha_completeness: dict[str, Any] | None = None,
) -> dict[str, Any]:
    asof_dir = output_dir / "asof_features"
    alpha_rows = alpha_rows or []
    alpha_by_key = {alpha_feature_key(row): row for row in alpha_rows}
    alpha_groups = (alpha_completeness or {}).get("groups") or {}
    completeness: dict[str, Any] = {
        "schema_version": "phase107h.asof_feature_completeness.v1",
        "groups": {},
        "future_collection_required": True,
    }
    for group, missing_reason in MISSING_ASOF_GROUPS.items():
        total_rows = 0
        available_rows = 0
        for horizon in HORIZONS:
            rows = []
            for label in labels:
                alpha = alpha_by_key.get((label["mint"], label["slice_id"], str(label["segment_id"]), horizon))
                available = bool(alpha and alpha_group_available(alpha, group))
                if available:
                    available_rows += 1
                rows.append(
                    {
                    "mint": label["mint"],
                    "slice_id": label["slice_id"],
                    "segment_id": label["segment_id"],
                    "relay_session_id": label["relay_session_id"],
                    "horizon_seconds": horizon,
                    "feature_available": available,
                    "asof_safe": True,
                    "source_artifact": "asof_alpha_features/asof_alpha_features_<horizon>.csv" if available else "legacy_slices_without_asof_alpha_features",
                    "missing_reason": "" if available else missing_reason,
                    "future_collection_required": not available,
                }
                )
            total_rows += len(rows)
            write_csv(asof_dir / f"asof_{group}_features_{horizon:03d}s.csv", rows, EXTENDED_ASOF_FIELDS)
        group_available = boolish(alpha_groups.get(group, {}).get("available")) or available_rows > 0
        completeness["groups"][group] = {
            "available": group_available,
            "rows": total_rows,
            "available_rows": available_rows,
            "missing_reason": "" if group_available else missing_reason,
            "future_collection_required": not group_available,
        }
    completeness["future_collection_required"] = any(
        payload["future_collection_required"] for payload in completeness["groups"].values()
    )
    write_json(asof_dir / "asof_feature_completeness_report.json", completeness)
    lines = ["# As-Of Feature Completeness Report", ""]
    for group, payload in completeness["groups"].items():
        lines.append(f"## {group}")
        lines.append(f"- available: {str(payload['available']).lower()}")
        lines.append(f"- rows: {payload['rows']}")
        lines.append(f"- available_rows: {payload['available_rows']}")
        lines.append(f"- missing_reason: {payload['missing_reason']}")
        lines.append(f"- future_collection_required: {str(payload['future_collection_required']).lower()}")
        lines.append("")
    lines.append("No holder RPC or canonical RPC mint supply was introduced. Legacy slices without alpha shards remain label-usable but feature-incomplete for alpha research.")
    (output_dir / "asof_feature_completeness_report.md").write_text("\n".join(lines) + "\n")
    (asof_dir / "asof_feature_completeness_report.md").write_text("\n".join(lines) + "\n")
    return completeness


def survivor_extension_diagnostics(
    output_dir: pathlib.Path,
    labels: list[dict[str, Any]],
    asof_rows: list[dict[str, Any]],
    continue_scores: list[dict[str, Any]],
    v1_scores: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    labels_by_key = {label_key(row): row for row in labels}
    asof_by_mint = defaultdict(list)
    for row in asof_rows:
        asof_by_mint[asof_key(row)].append(row)
    continue_by_key = {label_key(row): row for row in continue_scores}
    v1_by_key = {label_key(row): row for row in v1_scores}
    survivor_slice_ids = {
        path.parent.name
        for path in COLLECTOR_ROOT.glob("*/survivor_extension_mode.json")
        if boolish(read_json(path).get("enabled"))
    }
    rows: list[dict[str, Any]] = []
    for key, label in labels_by_key.items():
        if label.get("slice_id") not in survivor_slice_ids:
            continue
        asofs = asof_by_mint.get(key, [])
        reached = sorted(int_or_zero(row.get("horizon_seconds")) for row in asofs if boolish(row.get("tracked_at_least_horizon")))
        continue_score = continue_by_key.get(key, {})
        v1 = v1_by_key.get(key, {})
        reasons = [part for part in str(v1.get("reason_codes") or "").split("|") if part]
        first_failed = str(v1.get("first_failed_candidate_gate") or first_failed_candidate_gate(reasons))
        data_quality_excluded = any(
            boolish(label.get(field))
            for field in (
                "provider_gap_exposed",
                "relay_gap_exposed",
                "sequence_gap_exposed",
                "hash_mismatch_exposed",
                "receiver_backpressure_exposed",
            )
        )
        rows.append(
            {
                "mint": label.get("mint"),
                "slice_id": label.get("slice_id"),
                "segment_id": label.get("segment_id"),
                "continued_reason": continue_score.get("reason_codes") or "survivor_extension_same_caps_research_only",
                "observed_seconds": int_or_zero(label.get("time_to_rejection_ms") or label.get("time_to_terminal_ms")) // 1000,
                "horizons_reached": "|".join(str(item) for item in reached),
                "candidate_gates_passed": "counted_clean_segment" if not data_quality_excluded else "",
                "first_failed_candidate_gate": first_failed,
                "censored": boolish(label.get("censored_label")),
                "data_quality_excluded": data_quality_excluded,
                "high_throughput_mint": boolish(label.get("high_throughput_mint")),
                "degraded_active_mint": boolish(label.get("degraded_active_mint")),
                "missing_asof_features_prevented_evaluation": any(reason.startswith("missing_") for reason in reasons),
                "candidate_eligibility_decision": v1.get("decision", ""),
                "candidate_eligibility_reason_codes": v1.get("reason_codes", ""),
            }
        )
    write_csv(output_dir / "survivor_extension_diagnostics.csv", rows, SURVIVOR_DIAGNOSTIC_FIELDS)
    failures = Counter(row["first_failed_candidate_gate"] for row in rows)
    lines = [
        "# Survivor Extension Diagnostics",
        "",
        f"Rows: {len(rows)}",
        "",
        "## First Failed Candidate Gates",
        *[f"- {key}: {count}" for key, count in failures.most_common(20)],
        "",
        "Diagnostics are research-only. Survivor extension does not raise caps, run replay, run backtesting, tune thresholds, or trade.",
    ]
    (output_dir / "survivor_extension_diagnostics.md").write_text("\n".join(lines) + "\n")
    return rows


def write_score_report(path: pathlib.Path, title: str, rows: list[dict[str, Any]], intro: str) -> None:
    counts = Counter(str(row.get("decision") or "") for row in rows)
    reasons = Counter()
    for row in rows:
        for reason in str(row.get("reason_codes") or "").split("|"):
            if reason:
                reasons[reason] += 1
    lines = [f"# {title}", "", intro, "", "## Decision Counts"]
    lines.extend(f"- {name}: {count}" for name, count in sorted(counts.items()))
    lines.extend(["", "## Top Reason Codes"])
    lines.extend(f"- {name}: {count}" for name, count in reasons.most_common(20))
    lines.extend(["", "No replay, backtesting, threshold tuning, live trading, wallet execution, or buy entries were produced."])
    path.write_text("\n".join(lines) + "\n")


def survivor_extension_runs(inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for row in inventory:
        source = pathlib.Path(str(row.get("source_path") or ""))
        policy = read_json(source / "survivor_extension_mode.json")
        if not boolish(policy.get("enabled")):
            continue
        runs.append({**row, "survivor_extension_policy": policy})
    return runs


def survivor_extension_proof_classification(runs: list[dict[str, Any]]) -> str:
    if not runs:
        return "NOT_RUN_SOURCE_READY"
    if any(int_or_zero(row.get("replay_eligible_candidate_count")) > 0 for row in runs):
        return "CANDIDATE_REVIEW_TRIGGERED"
    if any(boolish(row.get("included")) and boolish(row.get("counted_phase107b_result")) for row in runs):
        return "SURVIVOR_EXTENSION_PROOF_PASS"
    return "SURVIVOR_EXTENSION_BLOCK"


def survivor_int(row: dict[str, Any], flat_key: str, nested_key: str, nested_field: str) -> int:
    flat = int_or_zero(row.get(flat_key))
    if flat:
        return flat
    nested = row.get(nested_key)
    if isinstance(nested, dict):
        return int_or_zero(nested.get(nested_field))
    return 0


def leakage_audit(
    output_dir: pathlib.Path,
    asof_rows: list[dict[str, Any]],
    labels: list[dict[str, Any]],
    splits: dict[str, Any] | None = None,
    alpha_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    blockers: list[str] = []
    feature_columns = set(ASOF_FIELDS)
    alpha_rows = alpha_rows or []
    alpha_feature_columns = set(ASOF_ALPHA_FIELDS)
    forbidden = {
        "final_outcome",
        "final_outcome_reason",
        "rejection_reason",
        "terminal_inconclusive_reason",
        "candidate_checkpoint_seen",
        "replay_eligible",
        "r2_verified",
        "artifact_consistency_ok",
    }
    overlap = feature_columns & forbidden
    if overlap:
        blockers.append("forbidden_label_or_artifact_columns_in_features:" + ",".join(sorted(overlap)))
    alpha_overlap = alpha_feature_columns & forbidden
    if alpha_overlap:
        blockers.append("forbidden_label_or_artifact_columns_in_asof_alpha_features:" + ",".join(sorted(alpha_overlap)))
    for row in alpha_rows:
        if boolish(row.get("holder_rpc_used")):
            blockers.append(f"holder_rpc_used_in_asof_alpha:{row.get('mint', '')}")
        if boolish(row.get("rpc_mint_supply_canonical")):
            blockers.append(f"canonical_rpc_supply_in_asof_alpha:{row.get('mint', '')}")
        if boolish(row.get("threshold_tuning_allowed")) or boolish(row.get("live_trading_enabled")):
            blockers.append(f"execution_or_tuning_enabled_in_asof_alpha:{row.get('mint', '')}")
        asof_ts = parse_timestamp(str(row.get("feature_asof_timestamp") or ""))
        first_seen = parse_timestamp(str(row.get("mint_first_seen_timestamp") or ""))
        horizon = int_or_zero(row.get("horizon_seconds"))
        if asof_ts and first_seen and horizon:
            delta = (asof_ts - first_seen).total_seconds()
            if delta > horizon + 1:
                blockers.append(f"post_horizon_asof_alpha_timestamp:{row.get('mint', '')}:{horizon}")
    if any(row.get("final_outcome") == "terminal_inconclusive" and boolish(row.get("clean_negative_label")) for row in labels):
        blockers.append("terminal_inconclusive_treated_as_dead")
    if any(boolish(row.get("candidate_checkpoint_seen")) and boolish(row.get("clean_positive_label")) and not boolish(row.get("replay_eligible")) for row in labels):
        blockers.append("replay_ineligible_candidate_checkpoint_treated_positive")
    if splits:
        mint_to_split: dict[str, str] = {}
        for split_name, split_rows in splits.get("splits", {}).items():
            for mint in split_rows.get("mints", []):
                previous = mint_to_split.get(mint)
                if previous and previous != split_name:
                    blockers.append(f"mint_in_multiple_splits:{mint}")
                mint_to_split[mint] = split_name
        if splits.get("method") == "random":
            blockers.append("random_split_used")
    payload = {
        "schema_version": "phase107h.strategy_leakage_audit.v1",
        "passed": not blockers,
        "blockers": blockers,
        "audited_feature_columns": sorted(feature_columns),
        "audited_asof_alpha_feature_columns": sorted(alpha_feature_columns),
        "asof_alpha_rows_audited": len(alpha_rows),
        "rules": [
            "final outcomes/reasons cannot enter features",
            "candidate/replay fields cannot enter features",
            "R2/artifact status cannot be alpha features",
            "provider/relay quality fields are exclusion filters only",
            "terminal_inconclusive remains censored",
            "splits are chronological and mint-grouped",
        ],
    }
    write_json(output_dir / "leakage_audit.json", payload)
    lines = ["# Leakage Audit", "", f"Passed: {str(payload['passed']).lower()}", ""]
    if blockers:
        lines.extend(["## Blockers", *[f"- {blocker}" for blocker in blockers]])
    else:
        lines.append("No leakage blockers found.")
    (output_dir / "leakage_audit.md").write_text("\n".join(lines) + "\n")
    return payload


def build_splits(labels: list[dict[str, Any]], output_dir: pathlib.Path) -> dict[str, Any]:
    sorted_labels = sorted(labels, key=lambda row: (str(row.get("first_seen_at") or ""), str(row.get("mint") or "")))
    n = len(sorted_labels)
    train_end = int(n * 0.6)
    val_end = int(n * 0.8)
    embargo = min(25, max(1, n // 100)) if n else 0
    train = sorted_labels[: max(0, train_end - embargo)]
    validation = sorted_labels[min(n, train_end + embargo) : max(train_end + embargo, val_end - embargo)]
    test = sorted_labels[min(n, val_end + embargo) :]
    payload = {
        "schema_version": "phase107h.strategy_splits.v1",
        "method": "chronological_walk_forward",
        "random_split_used": False,
        "group_by": "mint",
        "embargo_rows": embargo,
        "splits": {
            "train": {"rows": len(train), "mints": [row["mint"] for row in train], "slice_ids": sorted({row["slice_id"] for row in train})},
            "validation": {"rows": len(validation), "mints": [row["mint"] for row in validation], "slice_ids": sorted({row["slice_id"] for row in validation})},
            "test": {"rows": len(test), "mints": [row["mint"] for row in test], "slice_ids": sorted({row["slice_id"] for row in test})},
        },
        "terminal_inconclusive_policy": "censored_only",
        "clean_positive_count": sum(1 for row in labels if boolish(row.get("clean_positive_label"))),
    }
    write_json(output_dir / "splits.json", payload)
    (output_dir / "splits.md").write_text(
        "\n".join(
            [
                "# Chronological Splits",
                "",
                "Method: chronological walk-forward with mint grouping and row embargo. No random split is used.",
                "",
                f"- Train rows: {len(train)}",
                f"- Validation rows: {len(validation)}",
                f"- Test rows: {len(test)}",
                f"- Embargo rows between windows: {embargo}",
                "- Terminal inconclusive labels are censored only.",
            ]
        )
        + "\n"
    )
    return payload


def write_strategy_modules(output_dir: pathlib.Path) -> dict[str, Any]:
    modules = {
        "EarlyAvoidFilter": {
            "status": "research_mode",
            "tradeable": False,
            "outputs": ["score", "reason_codes", "explanation"],
            "latest_version": "v1_asof_alpha_diagnostic",
            "thresholds_tuned": False,
        },
        "ContinueTrackingGate": {
            "status": "research_mode",
            "tradeable": False,
            "outputs": ["continue_tracking", "stop_tracking", "audit_only", "reason_codes"],
            "latest_version": "v1_asof_alpha_diagnostic",
            "thresholds_tuned": False,
        },
        "CandidateEligibilityGate": {
            "status": "research_mode",
            "tradeable": False,
            "outputs": ["candidate_eligible", "not_eligible", "censored", "reason_codes"],
            "latest_version": "v2_asof_alpha_diagnostic",
            "thresholds_tuned": False,
        },
        "BuySetupDraft": BuySetupDraft().describe(),
        "RiskAndExitDraft": RiskAndExitDraft().describe(),
        "SurvivorExtensionMode": {
            "status": "disabled_by_default",
            "raises_launch_caps": False,
            "runs_replay": False,
            "trades": False,
            "description": "Future collection-mode extension to track clean survivors longer within existing caps; not enabled by this readiness build.",
        },
    }
    write_json(output_dir / "strategy_modules.json", {"schema_version": "phase107h.strategy_modules.v1", "modules": modules})
    lines = ["# Strategy Modules", ""]
    for name, payload in modules.items():
        lines.append(f"## {name}")
        for key, value in payload.items():
            lines.append(f"- {key}: {stringify(value)}")
        lines.append("")
    (output_dir / "strategy_modules.md").write_text("\n".join(lines))
    return modules


def readiness_decision(labels: list[dict[str, Any]], leakage: dict[str, Any], modules: dict[str, Any], asof_rows: list[dict[str, Any]]) -> dict[str, Any]:
    clean_neg = sum(1 for row in labels if boolish(row.get("clean_negative_label")))
    clean_pos = sum(1 for row in labels if boolish(row.get("clean_positive_label")))
    replay_eligible = sum(1 for row in labels if boolish(row.get("replay_eligible")))
    asof_exists = bool(asof_rows)
    strategy_research_ready = clean_neg > 0 and asof_exists
    buy_strategy_build_ready = strategy_research_ready and bool(leakage.get("passed")) and all(
        name in modules for name in ("EarlyAvoidFilter", "ContinueTrackingGate", "CandidateEligibilityGate", "BuySetupDraft", "RiskAndExitDraft")
    )
    reason_codes: list[str] = []
    if clean_pos == 0:
        reason_codes.append("no_clean_positives")
    if replay_eligible == 0:
        reason_codes.append("no_replay_eligible_candidates")
    if not leakage.get("passed"):
        reason_codes.append("leakage_audit_failed")
    payload = {
        "schema_version": "phase107h.backtesting_readiness_decision.v1",
        "strategy_research_ready": strategy_research_ready,
        "buy_strategy_build_ready": buy_strategy_build_ready,
        "backtesting_ready": False,
        "replay_ready": False,
        "threshold_tuning_ready": False,
        "live_trading_ready": False,
        "paper_trading_ready": False,
        "reason_codes": reason_codes,
        "clean_negative_count": clean_neg,
        "clean_positive_count": clean_pos,
        "replay_eligible_candidate_count": replay_eligible,
        "asof_features_available": asof_exists,
        "leakage_audit_passed": bool(leakage.get("passed")),
    }
    return payload


def write_reports(
    output_dir: pathlib.Path,
    inventory: list[dict[str, Any]],
    labels: list[dict[str, Any]],
    readiness: dict[str, Any],
    leakage: dict[str, Any],
    early_scores: list[dict[str, Any]],
    continue_scores: list[dict[str, Any]],
    eligibility_scores: list[dict[str, Any]],
    alpha_rows: list[dict[str, Any]] | None = None,
    alpha_completeness: dict[str, Any] | None = None,
    early_v1_scores: list[dict[str, Any]] | None = None,
    continue_v1_scores: list[dict[str, Any]] | None = None,
    v2_scores: list[dict[str, Any]] | None = None,
    positive_discovery_rows: list[dict[str, Any]] | None = None,
) -> None:
    label_counts = Counter(row["label_quality"] for row in labels)
    final_counts = Counter(row["final_outcome"] for row in labels)
    included = [row for row in inventory if boolish(row.get("included"))]
    alpha_rows = alpha_rows or []
    alpha_completeness = alpha_completeness or {}
    early_v1_scores = early_v1_scores or []
    continue_v1_scores = continue_v1_scores or []
    v2_scores = v2_scores or []
    positive_discovery_rows = positive_discovery_rows or []
    alpha_slice_ids = {str(row.get("slice_id") or "") for row in alpha_rows if row.get("slice_id")}
    alpha_complete_slices = len(alpha_slice_ids)
    legacy_label_only_slices = max(0, len(included) - alpha_complete_slices)
    alpha_rows_by_horizon = Counter(int_or_zero(row.get("horizon_seconds")) for row in alpha_rows)
    alpha_groups = (alpha_completeness.get("groups") or {})
    clean_neg = readiness["clean_negative_count"]
    clean_pos = readiness["clean_positive_count"]
    censored = sum(1 for row in labels if boolish(row.get("censored_label")))
    candidate_count = sum(1 for row in labels if boolish(row.get("candidate_checkpoint_seen")))
    replay_count = readiness["replay_eligible_candidate_count"]
    early_decisions = Counter(row["decision"] for row in early_scores)
    continue_decisions = Counter(row["decision"] for row in continue_scores)
    eligibility_decisions = Counter(row["decision"] for row in eligibility_scores)
    survivor_runs = survivor_extension_runs(inventory)
    survivor_classification = survivor_extension_proof_classification(survivor_runs)
    survivor_frames = sum(survivor_int(row, "frames_received", "proof", "frames_received") for row in survivor_runs)
    survivor_attempts = sum(int_or_zero(row.get("attempted_launches")) for row in survivor_runs)
    survivor_rejected = sum(int_or_zero(row.get("rejected_dead_count")) for row in survivor_runs)
    survivor_inconclusive = sum(int_or_zero(row.get("terminal_inconclusive_count")) for row in survivor_runs)
    survivor_r2 = sum(survivor_int(row, "r2_uploaded", "r2", "uploaded_files") for row in survivor_runs)
    survivor_retention_deleted = sum(
        survivor_int(row, "retention_deleted_bytes", "retention", "deleted_bulk_bytes")
        for row in survivor_runs
    )

    readiness_lines = [
        "# Strategy Readiness Report",
        "",
        f"- Included slices: {len(included)}",
        f"- Mint labels: {len(labels)}",
        f"- Clean negatives: {clean_neg}",
        f"- Clean positives: {clean_pos}",
        f"- Censored: {censored}",
        f"- Candidate checkpoints: {candidate_count}",
        f"- Replay eligible: {replay_count}",
        f"- Legacy label-only / feature-incomplete slices: {legacy_label_only_slices}",
        f"- As-of-alpha-complete slices: {alpha_complete_slices}",
        f"- As-of alpha rows: {len(alpha_rows)}",
        f"- Trade delta coverage rows: {alpha_groups.get('trade_delta', {}).get('rows', 0)}",
        f"- Holder state coverage rows: {alpha_groups.get('holder_state', {}).get('rows', 0)}",
        f"- Vault/curve coverage rows: {alpha_groups.get('vault_curve', {}).get('rows', 0)}",
        f"- Leakage audit passed: {str(leakage.get('passed')).lower()}",
        f"- Strategy research ready: {str(readiness['strategy_research_ready']).lower()}",
        f"- Buy strategy build ready: {str(readiness['buy_strategy_build_ready']).lower()}",
        f"- Backtesting ready: {str(readiness['backtesting_ready']).lower()}",
        "",
        "## As-Of Alpha Rows By Horizon",
        *[f"- {horizon}s: {count}" for horizon, count in sorted(alpha_rows_by_horizon.items()) if horizon],
        "",
        "Trading, replay, formal backtesting, threshold tuning, wallet execution, holder RPC, and canonical RPC mint supply remain disabled/blocked.",
    ]
    (output_dir / "STRATEGY_READINESS_REPORT.md").write_text("\n".join(readiness_lines) + "\n")

    (output_dir / "BUY_STRATEGY_BUILD_REPORT.md").write_text(
        "\n".join(
            [
                "# Buy Strategy Build Report",
                "",
                "Research modules exist for early avoid filtering, continue-tracking, candidate eligibility, disabled buy setup drafts, and disabled risk/exit drafts.",
                "",
                "CandidateEligibilityGate v2, EarlyAvoidFilter v1, and ContinueTrackingGate v1 use as-of alpha feature groups diagnostically only. They emit descriptive bins and reason codes, not trade entries.",
                "",
                "No buy entries are produced. No thresholds are tuned. No backtests or replay were run.",
                "",
                f"Build ready: {str(readiness['buy_strategy_build_ready']).lower()}",
                f"Reason codes: {', '.join(readiness['reason_codes']) or 'none'}",
                f"CandidateEligibilityGate v2 rows: {len(v2_scores)}",
                f"Candidate positive discovery rows: {len(positive_discovery_rows)}",
            ]
        )
        + "\n"
    )

    (output_dir / "OVERFITTING_RISK_REPORT.md").write_text(
        "\n".join(
            [
                "# Overfitting Risk Report",
                "",
                "- Clean positives are absent, so any buy strategy validation is blocked.",
                "- Current rows are dominated by clean negatives and censored terminal inconclusive outcomes.",
                "- Strategy hypotheses are pre-registered before evaluation.",
                "- Splits are chronological, not random.",
                "- Threshold tuning remains blocked.",
            ]
        )
        + "\n"
    )

    (output_dir / "NEXT_DATA_NEEDED.md").write_text(
        "\n".join(
            [
                "# Next Data Needed",
                "",
                "- Continue collecting as-of-alpha-complete survivor/candidate examples under the same caps.",
                "- Collect clean survivor/candidate examples without raising launch caps.",
                "- Keep terminal inconclusive and provider-gap rows censored.",
                "- Do not run replay/backtesting/tuning/trading until readiness gates pass and operator approval is explicit.",
                "- Launch caps remain blocked.",
            ]
        )
        + "\n"
    )

    (output_dir / "NEXT_STRATEGY_ACTION_PLAN.md").write_text(
        "\n".join(
            [
                "# Next Strategy Action Plan",
                "",
                "## What Clean Negatives Teach Us",
                "",
                f"The current retained dataset has {clean_neg} clean negatives. These are useful for avoid-filter research: they describe launches that became early rejected/dead in clean counted segments. They do not by themselves define buys.",
                "",
                "## Why Clean Positives Are Absent",
                "",
                f"Clean positives: {clean_pos}. Replay-eligible candidates: {replay_count}. Candidate checkpoints: {candidate_count}. The system has mostly captured early failures and censored observations, not validated survivor/candidate examples.",
                "",
                "## Candidate Gates",
                "",
                "The candidate gates are intentionally conservative: counted clean segment, no relay/provider/receiver quality blocker, no provider-gap exposure for the mint, not terminal_inconclusive, not degraded audit-only, and replay eligibility only when countability allows it. With zero clean positives, this is more a lack of survivor/candidate data than evidence that the gate should be loosened.",
                "",
                "## As-Of Features Available",
                "",
                f"Available now: launch timing, observation coverage, data-quality/high-throughput/degraded flags, and compact as-of alpha groups. As-of-alpha-complete slices: {alpha_complete_slices}. Trade delta rows: {alpha_groups.get('trade_delta', {}).get('rows', 0)}. Holder state rows: {alpha_groups.get('holder_state', {}).get('rows', 0)}. Vault/curve rows: {alpha_groups.get('vault_curve', {}).get('rows', 0)}.",
                "",
                "## Legacy Feature-Incomplete Slices",
                "",
                f"Legacy label-only / feature-incomplete slices: {legacy_label_only_slices}. They remain useful for labels and avoid-filter context, but candidate-positive discovery should prefer as-of-alpha-complete slices.",
                "",
                "## Censored Labels",
                "",
                f"Censored rows: {censored}. Terminal_inconclusive, provider-gap-exposed, relay-gap-exposed, degraded/audit-only, and insufficient-observation rows are unsafe as dead labels.",
                "",
                "## Data Quality Exclusions",
                "",
                "Sequence gaps, hash mismatches, receiver backpressure, provider gaps, degraded active mints, R2/artifact failures, and non-counted segments are exclusion/audit filters, not alpha features.",
                "",
                "## Before Replay/Backtesting",
                "",
                "Need clean positives/replay-eligible candidates from countability-approved clean segments, explicit retained as-of feature snapshots, leakage audit still passing, and operator approval. Until then replay/backtesting/tuning/trading remain blocked.",
                "",
                "## Candidate Positive Discovery",
                "",
                f"CandidateEligibilityGate v2 rows: {len(v2_scores)}. Candidate positive discovery rows: {len(positive_discovery_rows)}. Candidate checkpoints: {candidate_count}. Replay eligible: {replay_count}.",
            ]
        )
        + "\n"
    )

    (output_dir / "strategy_hypotheses_registry.md").write_text(
        "\n".join(
            [
                "# Strategy Hypotheses Registry",
                "",
                "Pre-registered hypotheses only:",
                "",
                "- Early avoid filters may reduce obvious dead launches using no-buy-followthrough and volume-evaporated label patterns.",
                "- Continue-tracking gates should prioritize clean, uninterrupted observations with no provider/relay quality blockers.",
                "- Candidate eligibility must exclude censored, degraded, provider-gap, and replay-ineligible checkpoint rows.",
                "- Buy setup drafts require clean positives and replay/backtesting readiness before evaluation.",
                "- Risk/exit drafts require separate paper/backtest readiness and no wallet execution.",
            ]
        )
        + "\n"
    )

    (output_dir / "survivor_extension_proof_report.md").write_text(
        "\n".join(
            [
                "# Survivor Extension Proof Report",
                "",
                f"Classification: {survivor_classification}",
                "",
                "Survivor extension mode is research-only and disabled by default. Proof runs must not raise launch caps, run replay, run backtesting, tune thresholds, trade, or call wallet/RPC execution paths.",
                "",
                f"Survivor proof runs discovered: {len(survivor_runs)}",
                f"Frames: {survivor_frames}",
                f"Attempts: {survivor_attempts}",
                f"Rejected/dead: {survivor_rejected}",
                f"Terminal inconclusive: {survivor_inconclusive}",
                f"Replay-eligible candidates: {sum(int_or_zero(row.get('replay_eligible_candidate_count')) for row in survivor_runs)}",
                f"R2 uploaded/verified objects: {survivor_r2}",
                f"Retention deleted bytes: {survivor_retention_deleted}",
                "",
                "Gap-crossing mints remain terminal_inconclusive and never replay-eligible. Candidate review is required before any replay/backtesting if a replay-eligible candidate appears.",
            ]
        )
        + "\n"
    )
    (output_dir / "survivor_extension_batch_report.md").write_text(
        "\n".join(
            [
                "# Survivor Extension Batch Report",
                "",
                "Classification: NOT_RUN",
                "",
                "No survivor-extension proof/batch was run by this readiness build. Collection remains on the proven conservative relay-only R2-primary path until an explicit survivor-extension proof is launched.",
            ]
        )
        + "\n"
    )

    (output_dir / "mint_labels_summary.md").write_text(
        "\n".join(
            [
                "# Mint Labels Summary",
                "",
                "## Label Quality",
                *[f"- {key}: {count}" for key, count in sorted(label_counts.items())],
                "",
                "## Final Outcomes",
                *[f"- {key}: {count}" for key, count in sorted(final_counts.items())],
            ]
        )
        + "\n"
    )


def write_inventory_summary(output_dir: pathlib.Path, inventory: list[dict[str, Any]]) -> None:
    included = [row for row in inventory if boolish(row.get("included"))]
    excluded = [row for row in inventory if not boolish(row.get("included"))]
    lines = [
        "# Dataset Inventory Summary",
        "",
        f"- Included slices: {len(included)}",
        f"- Excluded slices: {len(excluded)}",
        f"- Attempts in included slices: {sum(int_or_zero(row.get('attempt_ledger_rows')) for row in included)}",
        f"- All launches indexed in included slices: {sum(int_or_zero(row.get('all_launches_indexed')) for row in included)}",
        f"- Rich-tracked launches in included slices: {sum(int_or_zero(row.get('rich_tracked_launches')) for row in included)}",
        f"- Cheap-only launches in included slices: {sum(int_or_zero(row.get('cheap_only_launches')) for row in included)}",
        f"- Skipped due rich budget in included slices: {sum(int_or_zero(row.get('skipped_due_budget')) for row in included)}",
        f"- Missed good-token audit count in included slices: {sum(int_or_zero(row.get('missed_good_token_count')) for row in included)}",
        f"- Rich tracking slots released in included slices: {sum(int_or_zero(row.get('tracking_slots_released')) for row in included)}",
        f"- Cheap follow-up rows in included slices: {sum(int_or_zero(row.get('cheap_followup_rows')) for row in included)}",
        f"- Promotion recommendations in included slices: {sum(int_or_zero(row.get('promotion_recommended_count')) for row in included)}",
        f"- Promotion admissions in included slices: {sum(int_or_zero(row.get('promotion_admitted_count')) for row in included)}",
        f"- Promotion blocked by budget in included slices: {sum(int_or_zero(row.get('promotion_blocked_budget_count')) for row in included)}",
        f"- R2 verified included slices: {sum(1 for row in included if boolish(row.get('r2_verified')))}",
        f"- Artifact-consistent included slices: {sum(1 for row in included if boolish(row.get('artifact_consistency_ok')))}",
    ]
    if excluded:
        reasons = Counter()
        for row in excluded:
            for reason in str(row.get("exclusion_reason") or "unknown").split(";"):
                if reason:
                    reasons[reason] += 1
        lines.extend(["", "## Exclusion Reasons", *[f"- {key}: {count}" for key, count in sorted(reasons.items())]])
    (output_dir / "dataset_inventory_summary.md").write_text("\n".join(lines) + "\n")


def write_checksums(output_dir: pathlib.Path) -> None:
    lines: list[str] = []
    for path in sorted(p for p in output_dir.rglob("*") if p.is_file() and p.name != "checksums.txt"):
        lines.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(output_dir)}")
    (output_dir / "checksums.txt").write_text("\n".join(lines) + "\n")


def validate_outputs(output_dir: pathlib.Path) -> list[str]:
    errors: list[str] = []
    for path in output_dir.rglob("*.json"):
        try:
            json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            errors.append(f"invalid json {path}: {exc}")
    for path in output_dir.rglob("*.csv"):
        with path.open(newline="") as handle:
            header = next(csv.reader(handle), [])
        if not header:
            errors.append(f"csv missing header {path}")
    for row in read_csv(output_dir / "mint_labels.csv"):
        if row.get("final_outcome") == "terminal_inconclusive" and row.get("clean_negative_label") == "true":
            errors.append("terminal_inconclusive_is_clean_negative")
        if row.get("candidate_checkpoint_seen") == "true" and row.get("clean_positive_label") == "true" and row.get("replay_eligible") != "true":
            errors.append("candidate_checkpoint_positive_without_replay")
    return errors


def run_build(args: argparse.Namespace) -> dict[str, Any]:
    exporter = load_module(STRATEGY_EXPORTER, "strategy_exporter")
    batch_map = load_batch_map(exporter)
    output_dir = args.output_root
    output_dir.mkdir(parents=True, exist_ok=True)

    runs = [
        inspect_run(path, exporter, batch_map)
        for path in sorted(args.collector_root.iterdir())
        if is_run_dir(path)
    ]
    inventory = [{field: run.get(field, "") for field in DATASET_FIELDS} for run in runs]
    included = [run for run in runs if boolish(run["included"])]

    labels, _segment_maps = build_labels(included)
    asof_rows = build_asof_features(labels, output_dir)
    alpha_rows, alpha_completeness = collect_asof_alpha_features(included, output_dir)
    asof_rows = merge_asof_alpha_into_base(asof_rows, alpha_rows)
    write_feature_availability(output_dir, alpha_completeness)
    splits = build_splits(labels, output_dir)
    leakage = leakage_audit(output_dir, asof_rows, labels, splits, alpha_rows)
    early_scores, continue_scores, eligibility_scores = score_strategy_gates(output_dir, asof_rows)
    v1_scores = score_candidate_eligibility_v1(output_dir, labels, asof_rows)
    early_v1_scores, continue_v1_scores, v2_scores = score_alpha_strategy_gates_v1_v2(
        output_dir,
        labels,
        alpha_rows,
    )
    positive_discovery_rows = candidate_positive_discovery_report(
        output_dir,
        labels,
        alpha_rows,
        early_v1_scores,
        continue_v1_scores,
        v2_scores,
    )
    gap_rows = continue_to_candidate_gap_analysis(
        output_dir,
        labels,
        asof_rows,
        continue_scores,
        v1_scores,
    )
    completeness = write_extended_asof_feature_placeholders(
        output_dir,
        labels,
        alpha_rows,
        alpha_completeness,
    )
    survivor_diagnostics = survivor_extension_diagnostics(
        output_dir,
        labels,
        asof_rows,
        continue_scores,
        v1_scores,
    )
    modules = write_strategy_modules(output_dir)
    readiness = readiness_decision(labels, leakage, modules, asof_rows)
    candidate_review_pack_path = write_candidate_review_pack_if_needed(
        output_dir,
        labels,
        alpha_rows,
        v2_scores,
        readiness,
    )

    write_csv(output_dir / "dataset_inventory.csv", inventory, DATASET_FIELDS)
    write_json(output_dir / "dataset_inventory.json", {"schema_version": "phase107h.dataset_inventory.v1", "slices": inventory})
    write_inventory_summary(output_dir, inventory)
    write_csv(output_dir / "mint_labels.csv", labels, MINT_FIELDS)
    write_json(output_dir / "mint_labels.json", {"schema_version": "phase107h.mint_labels.v1", "mints": [{k: v for k, v in row.items() if not k.startswith("_")} for row in labels]})
    write_reports(
        output_dir,
        inventory,
        labels,
        readiness,
        leakage,
        early_scores,
        continue_scores,
        eligibility_scores,
        alpha_rows,
        alpha_completeness,
        early_v1_scores,
        continue_v1_scores,
        v2_scores,
        positive_discovery_rows,
    )
    write_json(output_dir / "backtesting_readiness_decision.json", readiness)
    (output_dir / "backtesting_readiness_decision.md").write_text(
        "\n".join(
            [
                "# Backtesting Readiness Decision",
                "",
                f"- strategy_research_ready: {str(readiness['strategy_research_ready']).lower()}",
                f"- buy_strategy_build_ready: {str(readiness['buy_strategy_build_ready']).lower()}",
                f"- backtesting_ready: {str(readiness['backtesting_ready']).lower()}",
                f"- replay_ready: {str(readiness['replay_ready']).lower()}",
                f"- threshold_tuning_ready: {str(readiness['threshold_tuning_ready']).lower()}",
                f"- live_trading_ready: {str(readiness['live_trading_ready']).lower()}",
                f"- paper_trading_ready: {str(readiness['paper_trading_ready']).lower()}",
                f"- reason_codes: {', '.join(readiness['reason_codes']) or 'none'}",
            ]
        )
        + "\n"
    )
    if candidate_review_pack_path:
        classification = "CANDIDATE_REVIEW_TRIGGERED"
    elif (
        readiness["buy_strategy_build_ready"]
        and alpha_rows
        and v2_scores
        and positive_discovery_rows
        and leakage.get("passed")
    ):
        classification = "CANDIDATE_POSITIVE_DISCOVERY_PASS"
    elif readiness["buy_strategy_build_ready"] and early_scores and continue_scores and eligibility_scores and v1_scores and gap_rows is not None and survivor_diagnostics is not None:
        classification = "CANDIDATE_DISCOVERY_READY_PASS"
    else:
        classification = "STRATEGY_RESEARCH_READY_PASS"
    summary = {
        "schema_version": "phase107h.strategy_readiness_summary.v1",
        "generated_at": utc_stamp(),
        "classification": classification,
        "included_slices": len(included),
        "total_mints": len(labels),
        "clean_negative_count": readiness["clean_negative_count"],
        "clean_positive_count": readiness["clean_positive_count"],
        "replay_eligible_candidate_count": readiness["replay_eligible_candidate_count"],
        "strategy_research_ready": readiness["strategy_research_ready"],
        "buy_strategy_build_ready": readiness["buy_strategy_build_ready"],
        "backtesting_ready": readiness["backtesting_ready"],
        "replay_ready": readiness["replay_ready"],
        "threshold_tuning_ready": readiness["threshold_tuning_ready"],
        "live_trading_ready": readiness["live_trading_ready"],
        "launch_caps_remain_blocked": True,
        "survivor_extension_mode_enabled": bool(survivor_extension_runs(inventory)),
        "survivor_extension_proof_classification": survivor_extension_proof_classification(
            survivor_extension_runs(inventory)
        ),
        "candidate_eligibility_v1_rows": len(v1_scores),
        "candidate_eligibility_v2_rows": len(v2_scores),
        "candidate_positive_discovery_rows": len(positive_discovery_rows),
        "candidate_review_pack_path": candidate_review_pack_path,
        "continue_to_candidate_gap_rows": len(gap_rows),
        "survivor_extension_diagnostic_rows": len(survivor_diagnostics),
        "asof_trade_delta_features_available": bool(completeness["groups"]["trade_delta"]["available"]),
        "asof_holder_state_features_available": bool(completeness["groups"]["holder_state"]["available"]),
        "asof_vault_curve_features_available": bool(completeness["groups"]["vault_curve"]["available"]),
    }
    write_json(output_dir / "strategy_readiness_summary.json", summary)
    errors = validate_outputs(output_dir)
    if errors:
        write_json(output_dir / "validation_errors.json", {"errors": errors})
        raise RuntimeError("; ".join(errors[:10]))
    write_checksums(output_dir)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return summary


def run_leakage_only(args: argparse.Namespace) -> dict[str, Any]:
    labels = read_csv(args.output_root / "mint_labels.csv")
    asof_rows: list[dict[str, Any]] = []
    for table in (args.output_root / "asof_features").glob("asof_features_*.csv"):
        asof_rows.extend(read_csv(table))
    alpha_rows: list[dict[str, Any]] = []
    for table in (args.output_root / "asof_alpha_features").glob("asof_alpha_features_*.csv"):
        alpha_rows.extend(read_csv(table))
    splits = read_json(args.output_root / "splits.json")
    return leakage_audit(args.output_root, asof_rows, labels, splits, alpha_rows)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("inventory", "build", "leakage-audit"):
        p = sub.add_parser(name)
        p.add_argument("--collector-root", type=pathlib.Path, default=COLLECTOR_ROOT)
        p.add_argument("--output-root", type=pathlib.Path, default=OUTPUT_ROOT)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        if args.command in {"inventory", "build"}:
            run_build(args)
        elif args.command == "leakage-audit":
            payload = run_leakage_only(args)
            print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    except Exception as exc:  # noqa: BLE001 - command-line diagnostics.
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
