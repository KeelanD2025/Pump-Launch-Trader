#!/usr/bin/env python3
"""Materialize fresh-launch-only stream holder state and conservative readiness gates."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "research_output/trading_strategy_pipeline/exact_holder_fresh_lifecycle"
DEFAULT_AMM_ROOT = REPO_ROOT / "research_output/trading_strategy_pipeline/pumpswap_reserve_state_capture"
DEFAULT_STRATEGY_ROOT = REPO_ROOT / "research_output/trading_strategy_pipeline/post_migration_strategy"

EXACT = "exact_stream_full_account_state"
NEAR_EXACT = "near_exact_stream_from_launch"
OBSERVED = "observed_subset_stream"
PROXY = "proxy_only"
ALLOWED_STRATEGY_QUALITY = {EXACT, NEAR_EXACT}
CONFIRMED_LAUNCH_INSTRUCTIONS = {"create", "create_v2"}
CONFIRMED_MIGRATION_INSTRUCTIONS = {"migrate", "migrate_v2"}

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMPSWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
SYSTEM_PROGRAM = "11111111111111111111111111111111"
INCINERATOR = "1nc1nerator11111111111111111111111111111111"
NULL_ADDRESSES = {"", SYSTEM_PROGRAM, INCINERATOR}

SAFETY_FALSE = {
    "live_trading_enabled": False,
    "paper_trading_enabled": False,
    "wallet_execution_enabled": False,
    "signing_enabled": False,
    "rpc_send_enabled": False,
    "order_submission_enabled": False,
    "replay_allowed": False,
    "actual_fill_claim_allowed": False,
    "threshold_tuning_enabled": False,
}

ACCOUNT_FIELDS = [
    "mint",
    "launch_id",
    "token_account",
    "owner_wallet",
    "amount_raw",
    "amount_ui",
    "decimals",
    "source_ts",
    "slot",
    "signature",
    "source_type",
    "source_quality",
    "is_nonzero",
    "excluded_from_holder_count",
    "exclusion_reason",
    "update_classification",
    "holder_balance_before",
    "holder_balance_after",
]

SNAPSHOT_FIELDS = [
    "mint",
    "launch_id",
    "phase",
    "source_ts",
    "slot",
    "source_quality",
    "nonzero_token_account_count",
    "holder_count_exact",
    "holder_count_near_exact",
    "holder_count_observed_subset",
    "holder_growth_exact",
    "holder_growth_near_exact",
    "top_1_holder_pct",
    "top_5_holder_pct",
    "top_10_holder_pct",
    "top_20_holder_pct",
    "creator_holding_pct",
    "dev_wallet_holding_pct",
    "holder_concentration_delta",
    "circulating_balance_ex_pool_vaults",
]

CARRY_FIELDS = [
    "mint",
    "launch_id",
    "migration_ts",
    "pumpswap_pool",
    "holder_count_at_migration",
    "holder_count_after_migration",
    "holder_growth_after_migration",
    "pre_to_post_holder_retention",
    "top_1_holder_pct",
    "top_5_holder_pct",
    "top_10_holder_pct",
    "creator_holding_pct",
    "dev_wallet_holding_pct",
    "source_quality",
    "pool_vaults_identified",
    "pool_vaults_excluded",
    "carry_forward_complete",
    "gap_reason",
]

DECISION_FIELDS = [
    "mint",
    "launch_id",
    "decision_ts",
    "latest_holder_state_ts",
    "holder_state_age_seconds",
    "holder_count_exact",
    "holder_count_near_exact",
    "holder_growth_to_entry",
    "top_holder_concentration_to_entry",
    "creator_holding_pct_to_entry",
    "dev_wallet_holding_pct_to_entry",
    "source_quality",
    "leakage_safe",
    "missing_reason",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_time(value: Any) -> datetime | None:
    if isinstance(value, (list, tuple)):
        if len(value) < 6:
            return None
        try:
            year, ordinal, hour, minute, second, nanosecond = (int(part) for part in value[:6])
            offset_parts = [int(part) for part in value[6:9]]
            offset_parts.extend([0] * (3 - len(offset_parts)))
            offset_seconds = offset_parts[0] * 3600 + offset_parts[1] * 60 + offset_parts[2]
            parsed = datetime(
                year,
                1,
                1,
                hour,
                minute,
                second,
                nanosecond // 1_000,
                tzinfo=timezone(timedelta(seconds=offset_seconds)),
            ) + timedelta(days=ordinal - 1)
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed.astimezone(timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    if text.startswith("["):
        try:
            return parse_time(json.loads(text))
        except json.JSONDecodeError:
            return None
    text = text.replace(" +00:00:00", "+00:00").replace(" UTC", "+00:00")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    text = re.sub(r"^(\d{4}-\d{2}-\d{2}) (\d):", r"\1 0\2:", text)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def time_from_nanos(value: Any) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(value) / 1_000_000_000, tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def fmt_time(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def decimalish(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0").strip() or "0")
    except (InvalidOperation, ValueError):
        return Decimal(0)


def intish(value: Any) -> int:
    try:
        return int(Decimal(str(value or "0").strip() or "0"))
    except (InvalidOperation, ValueError):
        return 0


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def read_csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open(newline="") as handle:
            return list(csv.DictReader(handle))
    except OSError:
        return []


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def first(row: dict[str, Any], *fields: str) -> str:
    for field in fields:
        value = str(row.get(field, "") or "").strip()
        if value:
            return value
    return ""


def launch_id_for(row: dict[str, Any], mint: str) -> str:
    launch_id = first(row, "launch_id", "linked_launch_id")
    if launch_id:
        return launch_id
    signature = first(row, "signature", "launch_signature")
    return hashlib.sha256(f"{mint}|{signature}".encode()).hexdigest()[:16]


def pct(numerator: Decimal, denominator: Decimal) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator / denominator * Decimal(100)), 8)


def source_type_for(row: dict[str, Any]) -> str:
    source = "|".join(
        [
            first(row, "update_source"),
            first(row, "source_mode"),
            first(row, "parse_status"),
            first(row, "update_type"),
        ]
    ).lower()
    if (
        "spl_token_account_subscription" in source
        or "account_subscription" in source
        or "geyser_spl_token_account_update" in source
    ):
        return "geyser_spl_token_account_update"
    if "token_balance" in source or "prepost" in source or "pre_post" in source:
        return "transaction_pre_post_token_balances"
    return "stream_holder_balance_update"


def update_classification(row: dict[str, Any]) -> str:
    before = first(row, "holder_balance_before", "balance_before")
    after = first(row, "holder_balance_after", "balance_after", "amount_raw")
    update_type = first(row, "update_type").lower()
    if not before or "created" in update_type:
        return "account_created_observed"
    if decimalish(after) == 0:
        return "account_closed_observed"
    return "balance_update_from_pre_post_token_balances" if source_type_for(row).startswith("transaction") else "account_balance_update_observed"


def tracker_rows(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    rows = manifest.get("tracker_rows")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def quality_for_mint(
    mint: str,
    tracker: dict[str, Any],
    holder_rows: list[dict[str, Any]],
    launch_signature: str,
    source_integrity: bool,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    account_rows = [row for row in holder_rows if source_type_for(row) == "geyser_spl_token_account_update"]
    tx_rows = [row for row in holder_rows if source_type_for(row) == "transaction_pre_post_token_balances"]
    baseline_rows = [row for row in tx_rows if launch_signature and first(row, "signature") == launch_signature]
    full_snapshot = any(
        boolish(row.get("complete_initial_snapshot_proven"))
        or boolish(row.get("account_subscription_initial_snapshot_proven"))
        for row in holder_rows
    )
    if not boolish(tracker.get("tracker_created")):
        reasons.append("holder_tracker_not_created")
    if not tx_rows:
        reasons.append("transaction_balance_rows_missing")
    if not baseline_rows:
        reasons.append("launch_transaction_balance_baseline_missing")
    if not account_rows:
        reasons.append("mint_scoped_token_account_updates_missing")
    if not source_integrity:
        reasons.append("stream_continuity_not_proven")
    if full_snapshot and source_integrity and boolish(tracker.get("tracker_created")):
        return EXACT, reasons
    if (
        boolish(tracker.get("tracker_created"))
        and baseline_rows
        and account_rows
        and source_integrity
    ):
        return NEAR_EXACT, reasons
    if holder_rows:
        return OBSERVED, reasons
    return PROXY, reasons


def aggregate_state(
    state: dict[str, dict[str, Any]], creator_wallet: str, quality: str
) -> dict[str, Any]:
    owner_balances: dict[str, Decimal] = defaultdict(Decimal)
    nonzero_accounts = 0
    for account in state.values():
        amount = decimalish(account.get("amount_raw"))
        if amount <= 0 or boolish(account.get("excluded_from_holder_count")):
            continue
        nonzero_accounts += 1
        owner = str(account.get("owner_wallet", "") or account.get("token_account", ""))
        owner_balances[owner] += amount
    balances = sorted(owner_balances.values(), reverse=True)
    total = sum(balances, Decimal(0))
    holder_count = len(balances)
    creator_balance = owner_balances.get(creator_wallet, Decimal(0))
    exact_count: int | str = holder_count if quality == EXACT else ""
    near_count: int | str = holder_count if quality == NEAR_EXACT else ""
    return {
        "nonzero_token_account_count": nonzero_accounts,
        "holder_count_exact": exact_count,
        "holder_count_near_exact": near_count,
        "holder_count_observed_subset": holder_count,
        "top_1_holder_pct": pct(sum(balances[:1], Decimal(0)), total),
        "top_5_holder_pct": pct(sum(balances[:5], Decimal(0)), total),
        "top_10_holder_pct": pct(sum(balances[:10], Decimal(0)), total),
        "top_20_holder_pct": pct(sum(balances[:20], Decimal(0)), total),
        "creator_holding_pct": pct(creator_balance, total),
        "dev_wallet_holding_pct": pct(creator_balance, total),
        "circulating_balance_ex_pool_vaults": str(total),
    }


def state_at(
    rows: list[dict[str, Any]], cutoff: datetime | None, creator_wallet: str, quality: str
) -> tuple[dict[str, dict[str, Any]], dict[str, Any], datetime | None]:
    state: dict[str, dict[str, Any]] = {}
    latest: datetime | None = None
    for row in rows:
        source_ts = parse_time(row.get("source_ts"))
        if cutoff is not None and (source_ts is None or source_ts > cutoff):
            continue
        state[str(row.get("token_account", ""))] = row
        if source_ts is not None and (latest is None or source_ts > latest):
            latest = source_ts
    return state, aggregate_state(state, creator_wallet, quality), latest


def build(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo_root).resolve()
    run_dir = Path(args.run_dir).resolve()
    output = Path(args.output_dir).resolve() if args.output_dir else DEFAULT_OUTPUT
    amm_root = Path(args.amm_root).resolve() if args.amm_root else DEFAULT_AMM_ROOT
    strategy_root = Path(args.strategy_root).resolve() if args.strategy_root else DEFAULT_STRATEGY_ROOT
    manifest_path = Path(args.relay_manifest).resolve()
    output.mkdir(parents=True, exist_ok=True)

    manifest = read_json(manifest_path)
    trackers = tracker_rows(manifest)
    launch_rows = read_csv(run_dir / "decoded_launch_event_rows.csv")
    holder_input = read_csv(run_dir / "decoded_holder_event_rows.csv")
    migration_rows = read_csv(run_dir / "quant_pumpfun_migration_event_rows.csv")
    pair_rows = read_csv(run_dir / "quant_pumpswap_pair_event_rows.csv")
    gap_rows = read_csv(run_dir / "run_gap_events.csv")
    local_summary = read_json(run_dir / "local_collector_summary.json")

    root_pair_rows = read_csv(amm_root / "pumpswap_live_relay_pumpswap_pair_event_rows.csv")
    pool_vault_rows = read_csv(amm_root / "pumpswap_pool_vault_rows.csv")
    decision_rows = read_csv(strategy_root / "post_migration_strategy_feature_rows.csv")
    if not decision_rows:
        decision_rows = read_csv(amm_root / "pumpfun_pumpswap_fresh_lifecycle_dataset_v1/strategy_decision_rows.csv")

    activation_dt = time_from_nanos(manifest.get("tracker_activation_unix_nanos"))
    manifest_session = str(manifest.get("relay_session_id", ""))
    integrity_gap_rows = [
        row
        for row in gap_rows
        if boolish(row.get("provider_data_loss_seen"))
        or boolish(row.get("client_backpressure_detected"))
        or first(row, "blocker_class", "provider_blocker_class")
        in {"provider_lagged_data_loss", "relay_sequence_gap", "relay_downstream_backpressure"}
    ]
    provider_gap_count = len(integrity_gap_rows)
    gap_times = [parse_time(first(row, "created_at", "event_observed_at_utc", "source_ts")) for row in integrity_gap_rows]
    summary_gap_count = max(
        intish(local_summary.get("sequence_gap_count")),
        intish(local_summary.get("downstream_backpressure_count")),
    )
    base_source_integrity = (
        boolish(manifest.get("dynamic_fresh_launch_tracking_enabled"))
        and intish(manifest.get("subscription_update_failures")) == 0
        and summary_gap_count <= provider_gap_count
        and intish(local_summary.get("unverified_chunk_count")) == 0
    )

    confirmed_launch_rows: list[dict[str, Any]] = []
    rejected_launch_rows: list[dict[str, Any]] = []
    for row in launch_rows:
        instruction = first(row, "decoded_instruction_name").lower()
        rejection_reasons: list[str] = []
        if first(row, "event_type") != "launch_create":
            rejection_reasons.append("event_type_not_launch_create")
        if instruction not in CONFIRMED_LAUNCH_INSTRUCTIONS:
            rejection_reasons.append("instruction_not_confirmed_create")
        if not boolish(row.get("source_is_non_rpc")):
            rejection_reasons.append("non_rpc_source_not_proven")
        if boolish(row.get("rpc_used")):
            rejection_reasons.append("rpc_used")
        if not boolish(row.get("strict_timing_eligible")):
            rejection_reasons.append("strict_timing_not_proven")
        if first(row, "parse_status") != "non_rpc_decoded_launch_create_event":
            rejection_reasons.append("launch_decode_status_not_confirmed")
        if rejection_reasons:
            rejected_launch_rows.append(
                {
                    "mint": first(row, "mint"),
                    "launch_id": first(row, "launch_id"),
                    "event_observed_at_utc": first(row, "event_observed_at_utc", "source_ts"),
                    "slot": first(row, "slot"),
                    "signature": first(row, "signature"),
                    "event_type": first(row, "event_type"),
                    "decoded_instruction_name": instruction,
                    "source_mode": first(row, "source_mode"),
                    "source_is_non_rpc": row.get("source_is_non_rpc", ""),
                    "rpc_used": row.get("rpc_used", ""),
                    "strict_timing_eligible": row.get("strict_timing_eligible", ""),
                    "parse_status": first(row, "parse_status"),
                    "tracker_manifest_member": False,
                    "rejection_reason": "|".join(rejection_reasons),
                }
            )
            continue
        confirmed_launch_rows.append(row)

    launch_by_mint: dict[str, dict[str, Any]] = {}
    for row in confirmed_launch_rows:
        mint = first(row, "mint")
        launch_ts = parse_time(first(row, "event_observed_at_utc", "source_ts"))
        if not mint or launch_ts is None:
            continue
        previous = launch_by_mint.get(mint)
        if previous is None or launch_ts < parse_time(first(previous, "event_observed_at_utc")):
            launch_by_mint[mint] = row

    tracker_by_mint: dict[str, dict[str, Any]] = {}
    for row in trackers:
        mint = str(row.get("mint", ""))
        if mint and mint not in tracker_by_mint:
            tracker_by_mint[mint] = row

    manifest_launch_slot_cutoff = max(
        (intish(row.get("launch_slot")) for row in tracker_by_mint.values()),
        default=0,
    )

    def launch_is_within_manifest_window(row: dict[str, Any]) -> bool:
        if not tracker_by_mint:
            return True
        launch_slot = intish(first(row, "slot"))
        return launch_slot <= manifest_launch_slot_cutoff if launch_slot else True

    for row in rejected_launch_rows:
        row["tracker_manifest_member"] = row["mint"] in tracker_by_mint

    rejected_launch_fields = [
        "mint",
        "launch_id",
        "event_observed_at_utc",
        "slot",
        "signature",
        "event_type",
        "decoded_instruction_name",
        "source_mode",
        "source_is_non_rpc",
        "rpc_used",
        "strict_timing_eligible",
        "parse_status",
        "tracker_manifest_member",
        "rejection_reason",
    ]
    write_csv(
        output / "exact_holder_rejected_launch_evidence_rows.csv",
        rejected_launch_rows,
        rejected_launch_fields,
    )

    launch_tracker_rows: list[dict[str, Any]] = []
    tracker_gap_rows: list[dict[str, Any]] = []
    launch_ids: dict[str, str] = {}
    eligible_mints: set[str] = set()
    for mint in sorted(set(tracker_by_mint) | set(launch_by_mint)):
        tracker = tracker_by_mint.get(mint, {})
        launch = launch_by_mint.get(mint, {})
        launch_ts = parse_time(first(launch, "event_observed_at_utc"))
        tracker_created_dt = time_from_nanos(tracker.get("tracker_created_at_unix_nanos"))
        launch_id = launch_id_for(launch or tracker, mint)
        launch_ids[mint] = launch_id
        after_activation = bool(launch_ts and activation_dt and launch_ts >= activation_dt)
        decoded = bool(launch)
        tracker_created = boolish(tracker.get("tracker_created"))
        ineligible: list[str] = []
        if not decoded:
            ineligible.append("internal_launch_row_missing")
        if not after_activation:
            ineligible.append("launch_not_after_tracker_activation")
        if mint not in tracker_by_mint:
            if launch and not launch_is_within_manifest_window(launch):
                ineligible.append("launch_after_manifest_snapshot_not_assessed")
            else:
                ineligible.append("tracker_manifest_row_missing")
        elif not tracker_created:
            ineligible.append(first(tracker, "ineligible_reason") or "tracker_not_created")
        if tracker_created and decoded and after_activation:
            eligible_mints.add(mint)
        launch_tracker_rows.append(
            {
                "mint": mint,
                "launch_id": launch_id,
                "launch_ts": fmt_time(launch_ts),
                "confirmed_internal_launch": decoded,
                "tracker_manifest_member": mint in tracker_by_mint,
                "tracker_created": tracker_created,
                "tracker_active": tracker.get("active", ""),
                "tracker_retired_ts": fmt_time(
                    time_from_nanos(tracker.get("retired_at_unix_nanos"))
                ),
                "tracker_retired_reason": first(tracker, "retired_reason"),
                "tracker_created_ts": fmt_time(tracker_created_dt),
                "tracker_delay_ms": tracker.get("tracker_delay_ms", ""),
                "tracker_source": tracker.get("tracker_source", ""),
                "eligible_for_fresh_tracker_scope": mint in eligible_mints,
                "eligible_for_exact_holder_acceptance": False,
                "currently_eligible_for_exact_holder_acceptance": False,
                "acceptance_valid_until": "",
                "acceptance_valid_until_reason": "",
                "ineligible_reason": "|".join(ineligible),
            }
        )
        if ineligible:
            gap_type = (
                "manifest_snapshot_boundary"
                if "launch_after_manifest_snapshot_not_assessed" in ineligible
                else "tracker_acceptance_gap"
            )
            tracker_gap_rows.append(
                {
                    "mint": mint,
                    "launch_id": launch_id,
                    "gap_type": gap_type,
                    "gap_reason": "|".join(ineligible),
                }
            )

    launch_fields = [
        "mint",
        "launch_id",
        "launch_ts",
        "confirmed_internal_launch",
        "tracker_manifest_member",
        "tracker_created",
        "tracker_active",
        "tracker_retired_ts",
        "tracker_retired_reason",
        "tracker_created_ts",
        "tracker_delay_ms",
        "tracker_source",
        "eligible_for_fresh_tracker_scope",
        "eligible_for_exact_holder_acceptance",
        "currently_eligible_for_exact_holder_acceptance",
        "acceptance_valid_until",
        "acceptance_valid_until_reason",
        "ineligible_reason",
    ]
    write_csv(output / "exact_holder_launch_tracker_rows.csv", launch_tracker_rows, launch_fields)
    write_csv(output / "exact_holder_tracker_gap_audit.csv", tracker_gap_rows, ["mint", "launch_id", "gap_type", "gap_reason"])

    confirmed_post_activation_mints = {
        mint
        for mint, row in launch_by_mint.items()
        if activation_dt
        and (launch_ts := parse_time(first(row, "event_observed_at_utc", "source_ts")))
        and launch_ts >= activation_dt
    }
    confirmed_launch_mints_within_manifest_window = {
        mint
        for mint in confirmed_post_activation_mints
        if launch_is_within_manifest_window(launch_by_mint[mint])
    }
    post_manifest_confirmed_launch_mints = (
        confirmed_post_activation_mints - confirmed_launch_mints_within_manifest_window
    )
    confirmed_launches_with_tracker = {
        mint
        for mint in confirmed_launch_mints_within_manifest_window
        if boolish(tracker_by_mint.get(mint, {}).get("tracker_created"))
    }
    confirmed_launches_without_tracker = (
        confirmed_launch_mints_within_manifest_window - confirmed_launches_with_tracker
    )
    manifest_trackers_without_confirmed_launch = set(tracker_by_mint) - set(launch_by_mint)
    confirmed_launch_tracker_coverage_pct = (
        round(
            len(confirmed_launches_with_tracker)
            / len(confirmed_launch_mints_within_manifest_window)
            * 100,
            4,
        )
        if confirmed_launch_mints_within_manifest_window
        else 0.0
    )
    launch_tracker_gate = (
        bool(confirmed_launch_mints_within_manifest_window)
        and not confirmed_launches_without_tracker
        and not manifest_trackers_without_confirmed_launch
    )

    all_gap_times_parseable = all(gap_time is not None for gap_time in gap_times)
    retired_tracker_mints = {
        mint
        for mint, tracker in tracker_by_mint.items()
        if tracker.get("retired_at_unix_nanos") is not None
        or tracker.get("active") is False
        or (
            isinstance(tracker.get("active"), str)
            and str(tracker.get("active")).lower() == "false"
        )
    }
    capacity_evicted_tracker_mints = {
        mint
        for mint in retired_tracker_mints
        if "capacity" in first(tracker_by_mint[mint], "retired_reason").lower()
    }
    ttl_expired_tracker_mints = {
        mint
        for mint in retired_tracker_mints
        if "ttl" in first(tracker_by_mint[mint], "retired_reason").lower()
    }
    first_gap_by_mint: dict[str, datetime | None] = {}
    continuity_cutoff_reason_by_mint: dict[str, str] = {}
    pre_gap_integrity_by_mint: dict[str, bool] = {}
    source_integrity_by_mint: dict[str, bool] = {}
    for mint in eligible_mints:
        tracker = tracker_by_mint[mint]
        tracker_started = time_from_nanos(tracker.get("tracker_created_at_unix_nanos"))
        launch_started = parse_time(first(launch_by_mint.get(mint, {}), "event_observed_at_utc", "source_ts"))
        tracking_started = tracker_started or launch_started
        relevant_gaps = sorted(
            gap_time
            for gap_time in gap_times
            if gap_time is not None and tracking_started is not None and gap_time >= tracking_started
        )
        cutoff_candidates: list[tuple[datetime, str]] = [
            (gap_time, "provider_or_sequence_gap") for gap_time in relevant_gaps
        ]
        retired_at = time_from_nanos(tracker.get("retired_at_unix_nanos"))
        active_value = tracker.get("active")
        retirement_declared = retired_at is not None or active_value is False or (
            isinstance(active_value, str) and active_value.lower() == "false"
        )
        retired_reason = first(tracker, "retired_reason") or "tracker_retired"
        if retired_at is not None:
            cutoff_candidates.append((retired_at, retired_reason))
        elif retirement_declared and tracking_started is not None:
            cutoff_candidates.append((tracking_started, "tracker_retirement_timestamp_missing"))
        if cutoff_candidates:
            cutoff, cutoff_reason = min(cutoff_candidates, key=lambda item: item[0])
            first_gap_by_mint[mint] = cutoff
            continuity_cutoff_reason_by_mint[mint] = cutoff_reason
        else:
            first_gap_by_mint[mint] = None
            continuity_cutoff_reason_by_mint[mint] = ""
        pre_gap_integrity_by_mint[mint] = (
            base_source_integrity and all_gap_times_parseable and tracking_started is not None
            and not (retirement_declared and retired_at is None)
        )
        source_integrity_by_mint[mint] = (
            pre_gap_integrity_by_mint[mint] and first_gap_by_mint[mint] is None
        )
    source_integrity = bool(eligible_mints) and all(source_integrity_by_mint.values())

    migration_by_mint: dict[str, dict[str, Any]] = {}
    rejected_migration_mints: set[str] = set()
    rejected_migration_rows: list[dict[str, Any]] = []
    confirmed_migration_evidence_rows = 0
    confirmed_fresh_migration_rows = 0
    non_fresh_migration_rows_rejected = 0
    unconfirmed_migration_rows_rejected = 0
    for row in migration_rows:
        mint = first(row, "mint")
        ts = parse_time(first(row, "event_observed_at_utc"))
        instruction = first(row, "decoded_instruction_name")
        transaction_status = first(row, "transaction_status", "status").lower()
        explicitly_confirmed = boolish(row.get("migration_confirmed"))
        confirmed = (
            explicitly_confirmed
            and transaction_status == "success"
            and instruction in CONFIRMED_MIGRATION_INSTRUCTIONS
        )
        if confirmed:
            confirmed_migration_evidence_rows += 1

        rejection_reasons: list[str] = []
        if mint not in eligible_mints:
            rejection_reasons.append("mint_not_in_fresh_tracker_scope")
            non_fresh_migration_rows_rejected += 1
        if ts is None:
            rejection_reasons.append("migration_timestamp_missing")
        if not explicitly_confirmed:
            rejection_reasons.append("migration_confirmation_missing_or_false")
        if transaction_status != "success":
            rejection_reasons.append("migration_transaction_not_successful")
        if instruction not in CONFIRMED_MIGRATION_INSTRUCTIONS:
            rejection_reasons.append("not_liquidity_migrate_instruction")
        if not confirmed:
            unconfirmed_migration_rows_rejected += 1

        if rejection_reasons:
            if mint:
                rejected_migration_mints.add(mint)
            rejected_migration_rows.append(
                {
                    "mint": mint,
                    "launch_id": first(row, "launch_id"),
                    "event_observed_at_utc": first(row, "event_observed_at_utc"),
                    "slot": first(row, "slot"),
                    "signature": first(row, "signature"),
                    "decoded_instruction_name": instruction,
                    "transaction_status": transaction_status,
                    "migration_confirmed": explicitly_confirmed,
                    "migration_evidence_type": first(row, "migration_evidence_type"),
                    "post_migration_pool": first(row, "post_migration_pool", "pool"),
                    "rejection_reason": "|".join(rejection_reasons),
                }
            )
            continue
        confirmed_fresh_migration_rows += 1
        previous = migration_by_mint.get(mint)
        if previous is None or ts < parse_time(first(previous, "event_observed_at_utc")):
            migration_by_mint[mint] = row
    mints_with_rejected_migration_evidence = sorted(rejected_migration_mints)
    rejected_migration_mints.difference_update(migration_by_mint)

    pair_candidates = pair_rows + root_pair_rows
    pair_by_mint: dict[str, dict[str, Any]] = {}
    for row in pair_candidates:
        mint = first(row, "mint", "linked_launch_mint", "base_mint")
        if mint not in eligible_mints:
            continue
        ts = parse_time(first(row, "event_observed_at_utc", "first_pumpswap_timestamp", "source_ts"))
        migration_ts = parse_time(first(migration_by_mint.get(mint, {}), "event_observed_at_utc"))
        if migration_ts and ts and ts < migration_ts:
            continue
        if mint not in pair_by_mint:
            pair_by_mint[mint] = row

    excluded_accounts: dict[str, str] = {}
    excluded_account_mints: dict[str, set[str]] = defaultdict(set)
    excluded_account_pools: dict[str, set[str]] = defaultdict(set)
    for mint in eligible_mints:
        launch = launch_by_mint.get(mint, {})
        for field in ("bonding_curve", "associated_bonding_curve"):
            account = first(launch, field)
            if account:
                excluded_accounts[account] = field
                excluded_account_mints[account].add(mint)
    # Migration instruction accounts remain authoritative stream evidence even
    # when the transaction fails; they identify curve/program accounts that
    # must never be counted as circulating holders.
    for row in migration_rows:
        mint = first(row, "mint")
        if mint not in eligible_mints:
            continue
        for field in ("bonding_curve", "associated_bonding_curve"):
            account = first(row, field)
            if account:
                excluded_accounts[account] = field
                excluded_account_mints[account].add(mint)
    # Pool vaults are accepted only from a confirmed successful migration.
    for mint, row in migration_by_mint.items():
        pool = first(row, "post_migration_pool", "pool")
        for field in ("pool_base_token_account", "pool_quote_token_account"):
            account = first(row, field)
            if account:
                excluded_accounts[account] = field
                excluded_account_mints[account].add(mint)
                if pool:
                    excluded_account_pools[account].add(pool)
    for row in pool_vault_rows:
        mint = first(row, "mint", "base_mint")
        if mint not in eligible_mints:
            continue
        for field in ("pool_base_token_account", "pool_quote_token_account"):
            account = first(row, field)
            if account:
                excluded_accounts[account] = field
                excluded_account_mints[account].add(mint)
                pool = first(row, "pool")
                if pool:
                    excluded_account_pools[account].add(pool)
    for mint, row in pair_by_mint.items():
        for field in ("base_vault", "quote_vault", "pool_base_token_account", "pool_quote_token_account"):
            account = first(row, field)
            if account:
                excluded_accounts[account] = field
                excluded_account_mints[account].add(mint)
                pool = first(row, "pool")
                if pool:
                    excluded_account_pools[account].add(pool)

    holder_by_mint_raw: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in holder_input:
        mint = first(row, "mint", "linked_launch_mint")
        if mint in eligible_mints:
            holder_by_mint_raw[mint].append(row)

    proven_quality_by_mint: dict[str, str] = {}
    proven_quality_reasons: dict[str, list[str]] = {}
    quality_by_mint: dict[str, str] = {}
    quality_reasons: dict[str, list[str]] = {}
    for mint in eligible_mints:
        launch_signature = first(launch_by_mint.get(mint, {}), "signature")
        first_gap = first_gap_by_mint.get(mint)
        pre_gap_rows = [
            row
            for row in holder_by_mint_raw.get(mint, [])
            if (row_ts := parse_time(first(row, "source_ts", "event_observed_at_utc"))) is not None
            and (first_gap is None or row_ts < first_gap)
        ]
        proven_quality, reasons = quality_for_mint(
            mint,
            tracker_by_mint[mint],
            pre_gap_rows,
            launch_signature,
            pre_gap_integrity_by_mint.get(mint, False),
        )
        proven_quality_by_mint[mint] = proven_quality
        proven_quality_reasons[mint] = reasons
        if first_gap is None:
            quality_by_mint[mint] = proven_quality
            quality_reasons[mint] = list(reasons)
        else:
            quality_by_mint[mint] = OBSERVED if holder_by_mint_raw.get(mint) else PROXY
            cutoff_reason = continuity_cutoff_reason_by_mint.get(mint, "unknown")
            quality_reasons[mint] = [
                *reasons,
                f"stream_continuity_lost_{cutoff_reason}_at_{fmt_time(first_gap)}",
            ]

    def quality_at_time(mint: str, source_ts: datetime | None) -> str:
        proven_quality = proven_quality_by_mint.get(mint, PROXY)
        first_gap = first_gap_by_mint.get(mint)
        if (
            source_ts is not None
            and proven_quality in ALLOWED_STRATEGY_QUALITY
            and (first_gap is None or source_ts < first_gap)
        ):
            return proven_quality
        return quality_by_mint.get(mint, PROXY)

    for row in launch_tracker_rows:
        mint = str(row["mint"])
        proven_quality = proven_quality_by_mint.get(mint, PROXY)
        accepted = mint in eligible_mints and proven_quality in ALLOWED_STRATEGY_QUALITY
        current_accepted = mint in eligible_mints and quality_by_mint.get(mint) in ALLOWED_STRATEGY_QUALITY
        row["eligible_for_exact_holder_acceptance"] = accepted
        row["currently_eligible_for_exact_holder_acceptance"] = current_accepted
        row["acceptance_valid_until"] = fmt_time(first_gap_by_mint.get(mint))
        row["acceptance_valid_until_reason"] = continuity_cutoff_reason_by_mint.get(mint, "")
        if mint in eligible_mints and not accepted:
            row["ineligible_reason"] = f"source_quality_{proven_quality}_not_exact_or_near_exact"
    write_csv(output / "exact_holder_launch_tracker_rows.csv", launch_tracker_rows, launch_fields)

    normalized_rows: list[dict[str, Any]] = []
    for mint in sorted(eligible_mints):
        for row in holder_by_mint_raw.get(mint, []):
            source_ts = first(row, "source_ts", "event_observed_at_utc")
            row_quality = quality_at_time(mint, parse_time(source_ts))
            token_account = first(row, "token_account", "token_account_pubkey")
            owner_wallet = first(row, "owner_wallet", "holder_wallet")
            amount_raw = first(row, "amount_raw", "holder_balance_after", "balance_after") or "0"
            decimals = first(row, "decimals") or "6"
            amount_ui = first(row, "amount_ui")
            if not amount_ui:
                amount_ui = str(decimalish(amount_raw) / (Decimal(10) ** intish(decimals)))
            excluded = boolish(row.get("excluded_from_holder_count"))
            exclusion_reason = first(row, "exclusion_reason")
            if token_account in excluded_accounts:
                excluded = True
                exclusion_reason = excluded_accounts[token_account]
            if token_account in NULL_ADDRESSES or owner_wallet in NULL_ADDRESSES:
                excluded = True
                exclusion_reason = exclusion_reason or "burn_null_or_system_account"
            if owner_wallet in {PUMP_PROGRAM, PUMPSWAP_PROGRAM}:
                excluded = True
                exclusion_reason = exclusion_reason or "program_owned_account"
            normalized_rows.append(
                {
                    "mint": mint,
                    "launch_id": launch_ids[mint],
                    "token_account": token_account,
                    "owner_wallet": owner_wallet,
                    "amount_raw": amount_raw,
                    "amount_ui": amount_ui,
                    "decimals": decimals,
                    "source_ts": source_ts,
                    "slot": first(row, "slot"),
                    "signature": first(row, "signature"),
                    "source_type": source_type_for(row),
                    "source_quality": row_quality,
                    "is_nonzero": decimalish(amount_raw) > 0,
                    "excluded_from_holder_count": excluded,
                    "exclusion_reason": exclusion_reason,
                    "update_classification": update_classification(row),
                    "holder_balance_before": first(row, "holder_balance_before", "balance_before"),
                    "holder_balance_after": first(row, "holder_balance_after", "balance_after", "amount_raw"),
                }
            )
    normalized_rows.sort(key=lambda row: (parse_time(row["source_ts"]) or datetime.min.replace(tzinfo=timezone.utc), intish(row["slot"]), row["signature"], row["token_account"]))

    normalized_by_mint: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in normalized_rows:
        normalized_by_mint[row["mint"]].append(row)

    tx_rows = [row for row in normalized_rows if row["source_type"] == "transaction_pre_post_token_balances"]
    account_rows = [row for row in normalized_rows if row["source_type"] == "geyser_spl_token_account_update"]
    write_csv(output / "exact_holder_token_account_update_rows.csv", account_rows, ACCOUNT_FIELDS)
    write_csv(output / "exact_holder_balance_state_rows.csv", normalized_rows, ACCOUNT_FIELDS)
    write_csv(output / "exact_holder_tx_balance_reconstruction_rows.csv", tx_rows, ACCOUNT_FIELDS)

    source_audit_rows: list[dict[str, Any]] = []
    tx_gap_rows: list[dict[str, Any]] = []
    for mint in sorted(eligible_mints):
        mint_rows = normalized_by_mint.get(mint, [])
        source_audit_rows.append(
            {
                "mint": mint,
                "launch_id": launch_ids[mint],
                "source_quality": quality_by_mint[mint],
                "best_proven_source_quality": proven_quality_by_mint[mint],
                "near_exact_valid_until": fmt_time(first_gap_by_mint.get(mint)),
                "transaction_balance_rows": sum(row["source_type"].startswith("transaction") for row in mint_rows),
                "account_subscription_rows": sum(row["source_type"] == "geyser_spl_token_account_update" for row in mint_rows),
                "pre_gap_transaction_balance_rows": sum(
                    row["source_type"].startswith("transaction")
                    and row["source_quality"] in ALLOWED_STRATEGY_QUALITY
                    for row in mint_rows
                ),
                "pre_gap_account_subscription_rows": sum(
                    row["source_type"] == "geyser_spl_token_account_update"
                    and row["source_quality"] in ALLOWED_STRATEGY_QUALITY
                    for row in mint_rows
                ),
                "source_integrity_proven": source_integrity_by_mint.get(mint, False),
                "pre_gap_source_integrity_proven": pre_gap_integrity_by_mint.get(mint, False),
                "quality_reasons": "|".join(quality_reasons[mint]),
            }
        )
        if quality_by_mint[mint] not in ALLOWED_STRATEGY_QUALITY:
            tx_gap_rows.append(
                {
                    "mint": mint,
                    "launch_id": launch_ids[mint],
                    "gap_type": "holder_state_completeness_gap",
                    "gap_reason": "|".join(quality_reasons[mint]) or "source_quality_below_near_exact",
                }
            )
    source_fields = [
        "mint",
        "launch_id",
        "source_quality",
        "best_proven_source_quality",
        "near_exact_valid_until",
        "transaction_balance_rows",
        "account_subscription_rows",
        "pre_gap_transaction_balance_rows",
        "pre_gap_account_subscription_rows",
        "source_integrity_proven",
        "pre_gap_source_integrity_proven",
        "quality_reasons",
    ]
    write_csv(output / "exact_holder_state_source_audit.csv", source_audit_rows, source_fields)
    write_csv(output / "exact_holder_tx_balance_reconstruction_gap_audit.csv", tx_gap_rows, ["mint", "launch_id", "gap_type", "gap_reason"])
    write_json(
        output / "exact_holder_tx_balance_reconstruction_audit.json",
        {
            "schema_version": "exact_holder_tx_balance_reconstruction_audit.v1",
            "generated_at_utc": utc_now(),
            "fresh_mints": len(eligible_mints),
            "transaction_balance_rows": len(tx_rows),
            "account_created_rows": sum(row["update_classification"] == "account_created_observed" for row in tx_rows),
            "account_closed_rows": sum(row["update_classification"] == "account_closed_observed" for row in tx_rows),
            "owner_missing_rows": sum(not row["owner_wallet"] for row in tx_rows),
            "token_account_missing_rows": sum(not row["token_account"] for row in tx_rows),
            "order_gap_rows": provider_gap_count,
            "rpc_used": False,
            "dex_as_holder_truth": False,
        },
    )

    pumpfun_snapshots: list[dict[str, Any]] = []
    concentration_rows: list[dict[str, Any]] = []
    creator_rows: list[dict[str, Any]] = []
    initial_metrics: dict[str, dict[str, Any]] = {}
    for mint in sorted(eligible_mints):
        rows = normalized_by_mint.get(mint, [])
        migration_ts = parse_time(first(migration_by_mint.get(mint, {}), "event_observed_at_utc"))
        pump_rows = [row for row in rows if migration_ts is None or (parse_time(row["source_ts"]) and parse_time(row["source_ts"]) <= migration_ts)]
        state: dict[str, dict[str, Any]] = {}
        creator = first(launch_by_mint.get(mint, {}), "creator_wallet")
        initial_count: int | None = None
        initial_top1: float | None = None
        for row in pump_rows:
            state[row["token_account"]] = row
            row_quality = str(row["source_quality"])
            metrics = aggregate_state(state, creator, row_quality)
            if initial_count is None:
                initial_count = int(metrics["holder_count_observed_subset"])
                initial_top1 = float(metrics["top_1_holder_pct"])
                initial_metrics[mint] = dict(metrics)
            current_count = int(metrics["holder_count_observed_subset"])
            snapshot = {
                "mint": mint,
                "launch_id": launch_ids[mint],
                "phase": "pumpfun",
                "source_ts": row["source_ts"],
                "slot": row["slot"],
                "source_quality": row_quality,
                **metrics,
                "holder_growth_exact": current_count - initial_count if row_quality == EXACT else "",
                "holder_growth_near_exact": current_count - initial_count if row_quality == NEAR_EXACT else "",
                "holder_concentration_delta": round(float(metrics["top_1_holder_pct"]) - float(initial_top1 or 0), 8),
            }
            pumpfun_snapshots.append(snapshot)
        mint_snapshots = [snapshot for snapshot in pumpfun_snapshots if snapshot["mint"] == mint]
        if mint_snapshots:
            final = mint_snapshots[-1]
            last_strategy_quality = next(
                (
                    snapshot
                    for snapshot in reversed(mint_snapshots)
                    if snapshot["source_quality"] in ALLOWED_STRATEGY_QUALITY
                ),
                None,
            )
            selected_snapshots = []
            if last_strategy_quality is not None:
                selected_snapshots.append(last_strategy_quality)
            if last_strategy_quality is None or final["source_ts"] != last_strategy_quality["source_ts"]:
                selected_snapshots.append(final)
            for selected in selected_snapshots:
                concentration_rows.append(dict(selected))
                creator_rows.append(
                    {
                        "mint": mint,
                        "launch_id": launch_ids[mint],
                        "source_ts": selected["source_ts"],
                        "creator_wallet": creator,
                        "dev_wallet": creator,
                        "creator_holding_pct": selected["creator_holding_pct"],
                        "dev_wallet_holding_pct": selected["dev_wallet_holding_pct"],
                        "source_quality": selected["source_quality"],
                    }
                )
    write_csv(output / "exact_holder_pumpfun_phase_state_rows.csv", pumpfun_snapshots, SNAPSHOT_FIELDS)
    write_csv(output / "exact_holder_concentration_rows.csv", concentration_rows, SNAPSHOT_FIELDS)
    write_csv(output / "exact_holder_creator_dev_holding_rows.csv", creator_rows, ["mint", "launch_id", "source_ts", "creator_wallet", "dev_wallet", "creator_holding_pct", "dev_wallet_holding_pct", "source_quality"])

    exclusion_rows = [
        {
            "mint": "|".join(sorted(excluded_account_mints.get(account, set()))),
            "pool": "|".join(sorted(excluded_account_pools.get(account, set()))),
            "excluded_account": account,
            "exclusion_reason": reason,
            "excluded_from_holder_count": True,
            "source": "internal_launch_or_pumpswap_pool_stream_artifact",
        }
        for account, reason in sorted(excluded_accounts.items())
    ]
    pool_exclusions = [row for row in exclusion_rows if "vault" in row["exclusion_reason"] or "token_account" in row["exclusion_reason"]]
    exclusion_fields = ["mint", "pool", "excluded_account", "exclusion_reason", "excluded_from_holder_count", "source"]
    write_csv(output / "exact_holder_exclusion_audit.csv", exclusion_rows, exclusion_fields)
    write_csv(output / "exact_holder_pool_vault_exclusion_rows.csv", pool_exclusions, exclusion_fields)

    carry_rows: list[dict[str, Any]] = []
    post_rows: list[dict[str, Any]] = []
    continuity_rows: list[dict[str, Any]] = []
    for mint, migration in sorted(migration_by_mint.items()):
        migration_ts = parse_time(first(migration, "event_observed_at_utc"))
        pool = first(migration, "post_migration_pool") or first(pair_by_mint.get(mint, {}), "pool")
        creator = first(launch_by_mint.get(mint, {}), "creator_wallet")
        rows = normalized_by_mint.get(mint, [])
        migration_quality = quality_at_time(mint, migration_ts)
        before_state, before_metrics, before_latest = state_at(rows, migration_ts, creator, migration_quality)
        post_source_rows = [row for row in rows if migration_ts and parse_time(row["source_ts"]) and parse_time(row["source_ts"]) > migration_ts]
        first_gap = first_gap_by_mint.get(mint)
        valid_post_rows = [
            row
            for row in post_source_rows
            if first_gap is None
            or migration_ts is None
            or first_gap <= migration_ts
            or (parse_time(row["source_ts"]) is not None and parse_time(row["source_ts"]) < first_gap)
        ]
        after_cutoff = max(
            (parse_time(row["source_ts"]) for row in valid_post_rows if parse_time(row["source_ts"]) is not None),
            default=migration_ts,
        )
        after_quality = quality_at_time(mint, after_cutoff)
        after_state, after_metrics, after_latest = state_at(rows, after_cutoff, creator, after_quality)
        vault_accounts = {
            row["excluded_account"]
            for row in pool_exclusions
            if mint in str(row.get("mint", "")).split("|")
            and (not pool or pool in str(row.get("pool", "")).split("|"))
        }
        gaps: list[str] = []
        if before_latest is None:
            gaps.append("holder_state_before_migration_missing")
        if not pool:
            gaps.append("pumpswap_pool_link_missing")
        if not vault_accounts:
            gaps.append("pool_vault_accounts_missing")
        if not valid_post_rows:
            gaps.append("post_migration_holder_updates_missing")
        if migration_quality not in ALLOWED_STRATEGY_QUALITY:
            gaps.append("source_quality_below_near_exact")
        if valid_post_rows and not any(
            row["source_quality"] in ALLOWED_STRATEGY_QUALITY for row in valid_post_rows
        ):
            gaps.append("post_migration_source_quality_below_near_exact")
        complete = not gaps
        before_count = int(before_metrics["holder_count_observed_subset"])
        after_count = int(after_metrics["holder_count_observed_subset"])
        before_owners = {
            str(row.get("owner_wallet", "") or row.get("token_account", ""))
            for row in before_state.values()
            if decimalish(row.get("amount_raw")) > 0
            and not boolish(row.get("excluded_from_holder_count"))
        }
        after_owners = {
            str(row.get("owner_wallet", "") or row.get("token_account", ""))
            for row in after_state.values()
            if decimalish(row.get("amount_raw")) > 0
            and not boolish(row.get("excluded_from_holder_count"))
        }
        retention = round(len(before_owners & after_owners) / len(before_owners) * 100, 8) if before_owners else 0.0
        carry = {
            "mint": mint,
            "launch_id": launch_ids[mint],
            "migration_ts": fmt_time(migration_ts),
            "pumpswap_pool": pool,
            "holder_count_at_migration": before_count,
            "holder_count_after_migration": after_count,
            "holder_growth_after_migration": after_count - before_count,
            "pre_to_post_holder_retention": retention,
            "top_1_holder_pct": after_metrics["top_1_holder_pct"],
            "top_5_holder_pct": after_metrics["top_5_holder_pct"],
            "top_10_holder_pct": after_metrics["top_10_holder_pct"],
            "creator_holding_pct": after_metrics["creator_holding_pct"],
            "dev_wallet_holding_pct": after_metrics["dev_wallet_holding_pct"],
            "source_quality": migration_quality,
            "pool_vaults_identified": bool(vault_accounts),
            "pool_vaults_excluded": bool(vault_accounts),
            "carry_forward_complete": complete,
            "gap_reason": "|".join(gaps),
        }
        carry_rows.append(carry)
        continuity_rows.append(dict(carry))
        for row in post_source_rows:
            post_rows.append(row)
    write_csv(output / "exact_holder_migration_carry_forward_rows.csv", carry_rows, CARRY_FIELDS)
    write_csv(output / "exact_holder_pre_to_post_state_continuity.csv", continuity_rows, CARRY_FIELDS)
    write_csv(output / "exact_holder_post_migration_state_rows.csv", post_rows, ACCOUNT_FIELDS)
    rejected_migration_fields = [
        "mint",
        "launch_id",
        "event_observed_at_utc",
        "slot",
        "signature",
        "decoded_instruction_name",
        "transaction_status",
        "migration_confirmed",
        "migration_evidence_type",
        "post_migration_pool",
        "rejection_reason",
    ]
    write_csv(
        output / "exact_holder_rejected_migration_evidence_rows.csv",
        rejected_migration_rows,
        rejected_migration_fields,
    )
    write_json(
        output / "exact_holder_migration_carry_forward_audit.json",
        {
            "schema_version": "exact_holder_migration_carry_forward_audit.v2",
            "generated_at_utc": utc_now(),
            "observed_migration_rows": len(migration_rows),
            "confirmed_migration_evidence_rows": confirmed_migration_evidence_rows,
            "confirmed_fresh_migration_rows": confirmed_fresh_migration_rows,
            "fresh_migrations": len(carry_rows),
            "rejected_migration_rows": len(rejected_migration_rows),
            "non_fresh_migration_rows_rejected": non_fresh_migration_rows_rejected,
            "unconfirmed_migration_rows_rejected": unconfirmed_migration_rows_rejected,
            "mints_with_rejected_migration_evidence": mints_with_rejected_migration_evidence,
            "rejected_migration_mints": sorted(rejected_migration_mints),
            "carry_forward_complete_rows": sum(boolish(row["carry_forward_complete"]) for row in carry_rows),
            "pool_vaults_counted_as_holders": False,
            "rpc_used": False,
            "dex_as_holder_truth": False,
        },
    )

    decision_output: list[dict[str, Any]] = []
    leakage_rows: list[dict[str, Any]] = []
    for decision in decision_rows:
        mint = first(decision, "mint", "linked_launch_mint")
        if mint not in eligible_mints:
            continue
        decision_ts = parse_time(first(decision, "decision_ts", "entry_decision_timestamp", "entry_timestamp"))
        creator = first(launch_by_mint.get(mint, {}), "creator_wallet")
        quality = quality_at_time(mint, decision_ts)
        _, metrics, latest = state_at(normalized_by_mint.get(mint, []), decision_ts, creator, quality)
        leakage_safe = bool(decision_ts and latest and latest <= decision_ts)
        age = (decision_ts - latest).total_seconds() if leakage_safe and decision_ts and latest else ""
        missing = []
        if latest is None:
            missing.append("holder_state_at_or_before_decision_missing")
        if quality not in ALLOWED_STRATEGY_QUALITY:
            missing.append("source_quality_below_near_exact")
        if not leakage_safe:
            missing.append("no_timestamp_safe_holder_state")
        initial_count = int(initial_metrics.get(mint, {}).get("holder_count_observed_subset", 0))
        current_count = int(metrics["holder_count_observed_subset"])
        row = {
            "mint": mint,
            "launch_id": launch_ids[mint],
            "decision_ts": fmt_time(decision_ts),
            "latest_holder_state_ts": fmt_time(latest),
            "holder_state_age_seconds": age,
            "holder_count_exact": metrics["holder_count_exact"] if leakage_safe else "",
            "holder_count_near_exact": metrics["holder_count_near_exact"] if leakage_safe else "",
            "holder_growth_to_entry": current_count - initial_count if quality in ALLOWED_STRATEGY_QUALITY and leakage_safe else "",
            "top_holder_concentration_to_entry": metrics["top_1_holder_pct"] if quality in ALLOWED_STRATEGY_QUALITY and leakage_safe else "",
            "creator_holding_pct_to_entry": metrics["creator_holding_pct"] if quality in ALLOWED_STRATEGY_QUALITY and leakage_safe else "",
            "dev_wallet_holding_pct_to_entry": metrics["dev_wallet_holding_pct"] if quality in ALLOWED_STRATEGY_QUALITY and leakage_safe else "",
            "source_quality": quality,
            "leakage_safe": leakage_safe,
            "missing_reason": "|".join(missing),
        }
        decision_output.append(row)
        leakage_rows.append(
            {
                "mint": mint,
                "launch_id": launch_ids[mint],
                "decision_ts": row["decision_ts"],
                "latest_holder_state_ts": row["latest_holder_state_ts"],
                "future_holder_state_used": False,
                "exact_fields_filled_from_proxy": False,
                "leakage_safe": leakage_safe,
                "violation_reason": "" if leakage_safe else row["missing_reason"],
            }
        )
    write_csv(output / "exact_holder_decision_state_rows.csv", decision_output, DECISION_FIELDS)
    write_csv(output / "exact_holder_strategy_feature_rows.csv", decision_output, DECISION_FIELDS)
    leakage_fields = ["mint", "launch_id", "decision_ts", "latest_holder_state_ts", "future_holder_state_used", "exact_fields_filled_from_proxy", "leakage_safe", "violation_reason"]
    write_csv(output / "exact_holder_leakage_audit.csv", leakage_rows, leakage_fields)

    quality_counts = Counter(quality_by_mint.values())
    proven_quality_counts = Counter(proven_quality_by_mint.values())
    latest_times = [
        parsed
        for parsed in [
            *(parse_time(first(row, "event_observed_at_utc")) for row in confirmed_launch_rows),
            *(parse_time(first(row, "source_ts", "event_observed_at_utc")) for row in holder_input),
        ]
        if parsed is not None
    ]
    latest_observed = max(latest_times) if latest_times else None
    observed_minutes = max(0.0, (latest_observed - activation_dt).total_seconds() / 60) if latest_observed and activation_dt else 0.0
    proof_window_met = observed_minutes >= args.min_proof_minutes or len(eligible_mints) >= args.min_fresh_launches
    complete_carry = sum(boolish(row["carry_forward_complete"]) for row in carry_rows)
    decision_ready_rows = sum(
        boolish(row["leakage_safe"]) and row["source_quality"] in ALLOWED_STRATEGY_QUALITY
        for row in decision_output
    )
    if not confirmed_post_activation_mints:
        proof_verdict = "blocked_fresh_source_no_launches"
    elif not launch_tracker_gate:
        proof_verdict = "partial_fresh_launches_tracked_no_migration_yet"
    elif not normalized_rows:
        proof_verdict = "blocked_provider_no_token_account_or_balance_updates"
    elif proven_quality_counts[EXACT] and proof_window_met:
        proof_verdict = "exact_holder_fresh_launch_tracking_ready"
    elif proven_quality_counts[NEAR_EXACT] and proof_window_met:
        proof_verdict = "near_exact_holder_fresh_launch_tracking_ready"
    elif migration_by_mint:
        proof_verdict = "partial_source_lacks_token_balance_state"
    else:
        proof_verdict = "partial_fresh_launches_tracked_no_migration_yet"
    if proof_verdict in {
        "exact_holder_fresh_launch_tracking_ready",
        "near_exact_holder_fresh_launch_tracking_ready",
    } and complete_carry:
        lifecycle_verdict = proof_verdict
    elif not carry_rows:
        lifecycle_verdict = "partial_fresh_launches_tracked_no_migration_yet"
    else:
        lifecycle_verdict = "partial_source_lacks_token_balance_state"

    proof_rows = []
    for mint in sorted(set(tracker_by_mint) | set(launch_by_mint)):
        tracker = tracker_by_mint.get(mint, {})
        carry = next((row for row in carry_rows if row["mint"] == mint), {})
        proof_rows.append(
            {
                "mint": mint,
                "launch_id": launch_ids.get(mint, ""),
                "internal_launch_row": mint in launch_by_mint,
                "tracker_created": boolish(tracker.get("tracker_created")),
                "token_account_update_rows": sum(row["mint"] == mint for row in account_rows),
                "tx_balance_reconstruction_rows": sum(row["mint"] == mint for row in tx_rows),
                "holder_balance_state_rows": sum(row["mint"] == mint for row in normalized_rows),
                "source_quality": quality_by_mint.get(mint, PROXY),
                "best_proven_source_quality": proven_quality_by_mint.get(mint, PROXY),
                "near_exact_valid_until": fmt_time(first_gap_by_mint.get(mint)),
                "near_exact_valid_until_reason": continuity_cutoff_reason_by_mint.get(
                    mint, ""
                ),
                "migration_seen": mint in migration_by_mint,
                "carry_forward_complete": carry.get("carry_forward_complete", False),
                "decision_time_rows": sum(row["mint"] == mint for row in decision_output),
                "eligible_for_exact_holder_acceptance": mint in eligible_mints and proven_quality_by_mint.get(mint) in ALLOWED_STRATEGY_QUALITY,
                "currently_eligible_for_exact_holder_acceptance": mint in eligible_mints and quality_by_mint.get(mint) in ALLOWED_STRATEGY_QUALITY,
                "gap_reason": "|".join(quality_reasons.get(mint, [])),
            }
        )
    proof_fields = ["mint", "launch_id", "internal_launch_row", "tracker_created", "token_account_update_rows", "tx_balance_reconstruction_rows", "holder_balance_state_rows", "source_quality", "best_proven_source_quality", "near_exact_valid_until", "near_exact_valid_until_reason", "migration_seen", "carry_forward_complete", "decision_time_rows", "eligible_for_exact_holder_acceptance", "currently_eligible_for_exact_holder_acceptance", "gap_reason"]
    write_csv(output / "exact_holder_proof_window_rows.csv", proof_rows, proof_fields)

    proof_report = {
        "schema_version": "exact_holder_fresh_launch_proof_report.v1",
        "generated_at_utc": utc_now(),
        "verdict": proof_verdict,
        "overall_lifecycle_verdict": lifecycle_verdict,
        "relay_session_id": manifest_session,
        "proof_window_observed_minutes": round(observed_minutes, 4),
        "proof_window_requirement_met": proof_window_met,
        "observed_launch_evidence_rows": len(launch_rows),
        "confirmed_internal_launch_rows": len(confirmed_launch_rows),
        "confirmed_internal_launch_mints": len(launch_by_mint),
        "confirmed_post_activation_launch_mints": len(confirmed_post_activation_mints),
        "manifest_launch_slot_cutoff": manifest_launch_slot_cutoff,
        "confirmed_launch_mints_within_manifest_window": len(
            confirmed_launch_mints_within_manifest_window
        ),
        "post_manifest_confirmed_launch_mints": len(post_manifest_confirmed_launch_mints),
        "post_manifest_confirmed_launch_mint_ids": sorted(post_manifest_confirmed_launch_mints),
        "rejected_launch_evidence_rows": len(rejected_launch_rows),
        "pending_create_backfill_rows_rejected": sum(
            row["decoded_instruction_name"] == "pending_create_backfill"
            for row in rejected_launch_rows
        ),
        "confirmed_launches_with_tracker": len(confirmed_launches_with_tracker),
        "confirmed_launches_without_tracker": len(confirmed_launches_without_tracker),
        "confirmed_launches_without_tracker_mints": sorted(confirmed_launches_without_tracker),
        "confirmed_launch_tracker_coverage_pct": confirmed_launch_tracker_coverage_pct,
        "launch_tracker_coverage_complete": launch_tracker_gate,
        "manifest_trackers_without_confirmed_internal_launch": len(manifest_trackers_without_confirmed_launch),
        "manifest_trackers_without_confirmed_internal_launch_mints": sorted(
            manifest_trackers_without_confirmed_launch
        ),
        "fresh_launches": len(eligible_mints),
        "trackers_created": sum(boolish(row.get("tracker_created")) for row in trackers),
        "retired_trackers": len(retired_tracker_mints),
        "retired_tracker_mints": sorted(retired_tracker_mints),
        "capacity_evicted_trackers": len(capacity_evicted_tracker_mints),
        "capacity_evicted_tracker_mints": sorted(capacity_evicted_tracker_mints),
        "ttl_expired_trackers": len(ttl_expired_tracker_mints),
        "ttl_expired_tracker_mints": sorted(ttl_expired_tracker_mints),
        "token_account_update_rows": len(account_rows),
        "tx_balance_reconstruction_rows": len(tx_rows),
        "holder_balance_state_rows": len(normalized_rows),
        "holder_concentration_rows": len(concentration_rows),
        "fresh_migrations": len(migration_by_mint),
        "observed_migration_rows": len(migration_rows),
        "confirmed_migration_evidence_rows": confirmed_migration_evidence_rows,
        "confirmed_fresh_migration_rows": confirmed_fresh_migration_rows,
        "rejected_migration_rows": len(rejected_migration_rows),
        "non_fresh_migration_rows_rejected": non_fresh_migration_rows_rejected,
        "unconfirmed_migration_rows_rejected": unconfirmed_migration_rows_rejected,
        "mints_with_rejected_migration_evidence": mints_with_rejected_migration_evidence,
        "rejected_migration_mints": sorted(rejected_migration_mints),
        "post_migration_holder_rows": len(post_rows),
        "decision_time_exact_or_near_exact_rows": decision_ready_rows,
        "source_quality_counts": dict(quality_counts),
        "best_proven_source_quality_counts": dict(proven_quality_counts),
        "pre_gap_exact_or_near_exact_mints": proven_quality_counts[EXACT] + proven_quality_counts[NEAR_EXACT],
        "provider_or_sequence_gap_count": provider_gap_count,
        "source_integrity_proven": source_integrity,
        "source_integrity_mint_count": sum(source_integrity_by_mint.values()),
        "pre_gap_source_integrity_mint_count": sum(pre_gap_integrity_by_mint.values()),
        "source_gap_affected_mint_count": sum(not value for value in source_integrity_by_mint.values()),
        "rpc_used": False,
        "dex_as_holder_truth": False,
        "proxy_as_exact": False,
        "pool_vaults_counted_as_holders": False,
        "safety_flags": SAFETY_FALSE,
    }
    write_json(output / "exact_holder_fresh_launch_proof_report.json", proof_report)
    write_json(
        output / "exact_holder_fresh_launch_to_migration_proof_report.json",
        {
            **proof_report,
            "schema_version": "exact_holder_fresh_launch_to_migration_proof_report.v1",
            "verdict": lifecycle_verdict,
            "fresh_migrations": len(carry_rows),
            "carry_forward_complete_rows": complete_carry,
            "migration_proof_complete": complete_carry > 0,
        },
    )

    policy = {
        "old_migrated_mints_allowed_for_acceptance": False,
        "event_only_mints_allowed_for_acceptance": False,
        "stale_dead_mints_allowed_for_acceptance": False,
        "launch_after_tracker_activation_required": True,
        "holder_tracker_created_at_launch_required": True,
        "confirmed_internal_create_instruction_required": True,
        "pending_create_backfill_allowed_for_acceptance": False,
        "tracker_retirement_ends_near_exact_validity": True,
        "stream_only_required": True,
        "rpc_holder_snapshot_allowed": False,
        "dex_as_holder_truth": False,
        "proxy_as_exact": False,
    }
    contract = {
        "schema_version": "exact_holder_fresh_lifecycle_contract.v1",
        "generated_at_utc": utc_now(),
        "tracker_activation_timestamp": fmt_time(activation_dt),
        "relay_session_id": manifest_session,
        "quality_taxonomy": [EXACT, NEAR_EXACT, OBSERVED, PROXY],
        "full_strategy_quality_allowed": [EXACT, NEAR_EXACT],
        "quality_is_time_bounded": True,
        "provider_gap_downgrades_rows_at_or_after_gap": True,
        "tracker_retirement_ends_near_exact_validity": True,
        "policy": policy,
        "proxy_holder_allowed_for_research_only": True,
        "safety_flags": SAFETY_FALSE,
    }
    write_json(output / "exact_holder_fresh_lifecycle_contract.json", contract)
    (output / "exact_holder_fresh_lifecycle_contract.md").write_text(
        "# Fresh Stream Holder Contract\n\n"
        "Only Pump.fun mints decoded after relay tracker activation and enrolled by the same create update enter the acceptance scope; "
        "the launch row must be a strict-timing, non-RPC decoded create or create_v2 instruction, and pending create backfills are rejected. "
        "Exact or near-exact source quality is still required to pass. Quality is timestamp-bounded: a provider gap downgrades states at and after the gap without relabeling earlier continuous states. "
        "RPC and Dex holder truth are forbidden. Trade-participant proxies remain research-only. Pool, curve, program, and burn accounts are excluded.\n"
    )
    write_json(output / "exact_holder_acceptance_policy.json", {"schema_version": "exact_holder_acceptance_policy.v1", **policy})
    write_json(
        output / "exact_holder_no_stale_mint_guard.json",
        {
            "schema_version": "exact_holder_no_stale_mint_guard.v1",
            **policy,
            "tracker_manifest_mints": sorted(tracker_by_mint),
            "fresh_tracker_scope_mints": sorted(eligible_mints),
            "accepted_mints": sorted(
                mint for mint in eligible_mints if proven_quality_by_mint.get(mint) in ALLOWED_STRATEGY_QUALITY
            ),
            "currently_accepted_mints": sorted(
                mint for mint in eligible_mints if quality_by_mint.get(mint) in ALLOWED_STRATEGY_QUALITY
            ),
            "source_quality_rejected_mints": sorted(
                mint for mint in eligible_mints if proven_quality_by_mint.get(mint) not in ALLOWED_STRATEGY_QUALITY
            ),
            "observed_launch_evidence_rows": len(launch_rows),
            "confirmed_internal_launch_rows": len(confirmed_launch_rows),
            "rejected_launch_evidence_rows": len(rejected_launch_rows),
            "pending_create_backfill_rows_rejected": sum(
                row["decoded_instruction_name"] == "pending_create_backfill"
                for row in rejected_launch_rows
            ),
            "non_manifest_launch_rows_rejected": sum(
                first(row, "mint") not in tracker_by_mint for row in launch_rows
            ),
            "non_manifest_confirmed_launch_rows_rejected": sum(
                first(row, "mint") not in tracker_by_mint for row in confirmed_launch_rows
            ),
            "non_manifest_confirmed_launch_rows_within_snapshot_rejected": sum(
                first(row, "mint") not in tracker_by_mint
                and launch_is_within_manifest_window(row)
                for row in confirmed_launch_rows
            ),
            "confirmed_launches_without_tracker_mints": sorted(confirmed_launches_without_tracker),
            "post_manifest_confirmed_launch_mints": sorted(post_manifest_confirmed_launch_mints),
            "confirmed_launch_tracker_coverage_pct": confirmed_launch_tracker_coverage_pct,
            "retired_tracker_mints": sorted(retired_tracker_mints),
            "capacity_evicted_tracker_mints": sorted(capacity_evicted_tracker_mints),
            "ttl_expired_tracker_mints": sorted(ttl_expired_tracker_mints),
            "manifest_trackers_without_confirmed_internal_launch_mints": sorted(
                manifest_trackers_without_confirmed_launch
            ),
        },
    )
    write_json(output / "exact_holder_tracker_activation_manifest.json", manifest)

    amm_gate = read_json(amm_root / "pumpswap_amm_coverage_gate.json")
    if not amm_gate:
        amm_gate = {
            "schema_version": "pumpswap_amm_coverage_gate.v1",
            "amm_research_usable": False,
            "amm_complete_coverage": False,
            "current_limitation": "amm_coverage_artifact_missing",
            "safety_flags": SAFETY_FALSE,
        }
    amm_gate["holder_coverage_evaluated_separately"] = True
    amm_gate["dex_as_source_truth"] = False
    write_json(output / "pumpswap_amm_coverage_gate.json", amm_gate)
    write_csv(
        output / "pumpswap_amm_coverage_gap_audit.csv",
        [
            {
                "decision_rows": amm_gate.get("decision_rows", 0),
                "decisions_with_pool_state": amm_gate.get("decisions_with_pool_state", 0),
                "decisions_missing_pool_state": amm_gate.get("decisions_missing_pool_state", 0),
                "stale_decision_rows": amm_gate.get("stale_decision_rows", 0),
                "decision_time_pool_state_coverage_pct": amm_gate.get("decision_time_pool_state_coverage_pct", 0),
                "amm_research_usable": amm_gate.get("amm_research_usable", False),
                "amm_complete_coverage": amm_gate.get("amm_complete_coverage", False),
                "gap_reason": amm_gate.get("current_limitation", ""),
            }
        ],
        ["decision_rows", "decisions_with_pool_state", "decisions_missing_pool_state", "stale_decision_rows", "decision_time_pool_state_coverage_pct", "amm_research_usable", "amm_complete_coverage", "gap_reason"],
    )

    decision_coverage_pct = round(decision_ready_rows / len(decision_output) * 100, 4) if decision_output else 0.0
    leakage_passed = bool(leakage_rows) and all(boolish(row["leakage_safe"]) for row in leakage_rows)
    holder_quality_ready = proven_quality_counts[EXACT] + proven_quality_counts[NEAR_EXACT] > 0
    migration_gate = bool(carry_rows) and complete_carry == len(carry_rows)
    sample_gate = len(eligible_mints) >= args.min_fresh_launches
    decision_gate = bool(decision_output) and decision_coverage_pct >= args.min_decision_coverage_pct
    amm_ready = boolish(amm_gate.get("coverage_ready")) and boolish(amm_gate.get("amm_research_usable"))
    full_ready = all(
        [
            launch_tracker_gate,
            holder_quality_ready,
            migration_gate,
            decision_gate,
            amm_ready,
            leakage_passed,
            sample_gate,
        ]
    )
    if full_ready:
        readiness_status = "full_strategy_ready"
    elif holder_quality_ready and not sample_gate:
        readiness_status = "strategy_ready_exact_holder_pending_sample"
    elif launch_tracker_gate and proven_quality_counts[EXACT]:
        readiness_status = "exact_holder_tracking_ready_no_strategy_yet"
    elif holder_quality_ready:
        readiness_status = "research_ready_near_exact_holder_partial"
    else:
        readiness_status = "research_ready_proxy_holder_only"
    blockers = []
    for passed, reason in [
        (launch_tracker_gate, "confirmed_launch_tracker_coverage_incomplete"),
        (holder_quality_ready, "exact_or_near_exact_holder_rows_missing"),
        (migration_gate, "fresh_migration_carry_forward_incomplete"),
        (decision_gate, "decision_time_holder_coverage_below_threshold"),
        (amm_ready, "amm_coverage_gate_not_ready"),
        (leakage_passed, "holder_no_lookahead_audit_not_proven"),
        (sample_gate, "fresh_launch_sample_below_minimum"),
    ]:
        if not passed:
            blockers.append(reason)
    holder_gate = {
        "schema_version": "strategy_dataset_holder_gate.v1",
        "generated_at_utc": utc_now(),
        "status": readiness_status,
        "full_strategy_dataset_ready": full_ready,
        "fresh_launches": len(eligible_mints),
        "confirmed_post_activation_launch_mints": len(confirmed_post_activation_mints),
        "manifest_launch_slot_cutoff": manifest_launch_slot_cutoff,
        "confirmed_launch_mints_within_manifest_window": len(
            confirmed_launch_mints_within_manifest_window
        ),
        "post_manifest_confirmed_launch_mints": len(post_manifest_confirmed_launch_mints),
        "confirmed_launches_with_tracker": len(confirmed_launches_with_tracker),
        "confirmed_launches_without_tracker": len(confirmed_launches_without_tracker),
        "manifest_trackers_without_confirmed_internal_launch": len(
            manifest_trackers_without_confirmed_launch
        ),
        "confirmed_launch_tracker_coverage_pct": confirmed_launch_tracker_coverage_pct,
        "launch_tracker_coverage_complete": launch_tracker_gate,
        "retired_trackers": len(retired_tracker_mints),
        "capacity_evicted_trackers": len(capacity_evicted_tracker_mints),
        "ttl_expired_trackers": len(ttl_expired_tracker_mints),
        "exact_mints": proven_quality_counts[EXACT],
        "near_exact_mints": proven_quality_counts[NEAR_EXACT],
        "observed_subset_mints": quality_counts[OBSERVED],
        "proxy_only_mints": quality_counts[PROXY],
        "current_exact_mints": quality_counts[EXACT],
        "current_near_exact_mints": quality_counts[NEAR_EXACT],
        "time_bounded_quality": True,
        "decision_time_holder_coverage_pct": decision_coverage_pct,
        "holder_state_carries_through_migration": migration_gate,
        "proxy_holder_allowed_for_research_only": True,
        "proxy_holder_allowed_for_strategy_ready": False,
        "blockers": blockers,
        "routine_rpc_allowed": False,
        "holder_snapshot_rpc_allowed": False,
        "dex_as_holder_truth": False,
        "proxy_as_exact": False,
        "pool_vaults_counted_as_holders": False,
        "safety_flags": SAFETY_FALSE,
    }
    write_json(output / "strategy_dataset_holder_gate.json", holder_gate)
    full = {
        **holder_gate,
        "schema_version": "full_strategy_dataset_readiness.v1",
        "amm_research_usable": boolish(amm_gate.get("amm_research_usable")),
        "amm_complete_coverage": boolish(amm_gate.get("amm_complete_coverage")),
        "amm_decision_time_coverage_pct": amm_gate.get("decision_time_pool_state_coverage_pct", 0),
        "no_lookahead_audit_passed": leakage_passed,
        "sample_size_sufficient": sample_gate,
        "strategy_ready_dataset_allowed": full_ready,
    }
    write_json(output / "full_strategy_dataset_readiness.json", full)
    write_json(
        output / "post_migration_strategy_readiness.json",
        {
            **full,
            "schema_version": "post_migration_strategy_readiness.v1.fresh_stream_holder",
            "strategy_dataset_ready": full_ready,
            "exact_holder_parity_ready": holder_quality_ready and migration_gate and decision_gate,
        },
    )
    write_json(
        output / "exact_holder_safety_contract.json",
        {
            "schema_version": "exact_holder_safety_contract.v1",
            "generated_at_utc": utc_now(),
            **SAFETY_FALSE,
            "routine_rpc_allowed": False,
            "holder_snapshot_rpc_allowed": False,
            "dex_as_source_truth": False,
            "dex_as_holder_truth": False,
            "proxy_as_exact": False,
            "pool_vaults_counted_as_holders": False,
        },
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--relay-manifest", required=True)
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--amm-root", default="")
    parser.add_argument("--strategy-root", default="")
    parser.add_argument("--min-proof-minutes", type=float, default=30.0)
    parser.add_argument("--min-fresh-launches", type=int, default=5)
    parser.add_argument("--min-decision-coverage-pct", type=float, default=75.0)
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(build(parse_args()))
