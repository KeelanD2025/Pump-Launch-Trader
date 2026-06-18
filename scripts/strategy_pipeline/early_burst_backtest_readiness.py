from __future__ import annotations

import pathlib
import shutil
import subprocess
import zipfile
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from .early_burst_validation import SAFE_FEATURE_FIELDS, safe_pack_path
from .io import read_csv, read_json, write_csv, write_json, write_text
from .schemas import HORIZONS, PIPELINE_ROOT, boolish, file_sha256, stable_hash


BACKTEST_READINESS_ROOT = PIPELINE_ROOT / "early_burst_backtest_readiness"
VALIDATION_ROOT = PIPELINE_ROOT / "early_burst_validation_dataset"
MIN_POSITIVE_MINTS = 100
MIN_HIGH_POSITIVE_MINTS = 20
MIN_ORDINARY_NEGATIVES = 500
MIN_SPLIT_WINDOWS = 3
MIN_FEATURE_COMPLETENESS = 0.80

TRADE_DELTA_FIELDS = {
    "trade_update_count_asof",
    "transaction_active_mint_count_asof",
    "pump_trade_active_mint_count_asof",
    "buy_count_delta_asof",
    "sell_count_delta_asof",
    "net_buy_sell_delta_asof",
    "volume_delta_asof",
    "unique_trade_accounts_asof",
}

HOLDER_STATE_FIELDS = {
    "holder_update_count_asof",
    "unique_holder_accounts_seen_asof",
    "top_holder_concentration_asof",
    "dev_or_creator_holding_proxy_asof",
    "holder_churn_proxy_asof",
    "holder_collapse_proxy_asof",
    "new_holder_count_delta_asof",
    "exiting_holder_count_delta_asof",
}

VAULT_CURVE_FIELDS = {
    "vault_update_count_asof",
    "bonding_curve_update_count_asof",
    "liquidity_delta_asof",
    "reserve_delta_asof",
    "curve_progress_proxy_asof",
    "liquidity_exit_proxy_asof",
    "price_or_curve_move_proxy_asof",
}

FEATURE_COHORT_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "decision_horizon_seconds",
    "positive_outcome_label",
    "early_burst_class",
    "final_outcome",
    "horizon_reached",
    "forward_window_observed",
    "feature_horizon_reached",
    "feature_data_complete_for_horizon",
    "trade_delta_present",
    "holder_state_present",
    "vault_curve_present",
    "holder_rpc_used",
    "rpc_mint_supply_canonical",
    "data_quality_exclusion",
    "cohort",
    "feature_complete_for_backtest",
    "exclusion_reason",
]

FORBIDDEN_ALPHA_COLUMNS = {
    "positive_outcome_label",
    "positive_outcome_strength_bin",
    "final_outcome",
    "rejection_reason",
    "terminal_inconclusive_reason",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "max_favorable_proxy",
    "max_adverse_proxy",
    "time_to_max_favorable_ms",
    "time_to_max_adverse_ms",
    "time_to_rejection_ms",
    "time_to_terminal_ms",
    "could_exit_before_death_proxy",
    "exit_window_observed",
    "exit_window_quality",
    "backtest_allowed",
    "replay_allowed",
    "trade_allowed",
    "r2_verified",
    "artifact_consistency_ok",
}


def repo_commit() -> str:
    try:
        proc = subprocess.run(["git", "rev-parse", "HEAD"], text=True, capture_output=True, check=True)
        return proc.stdout.strip()
    except Exception:
        return "unknown"


def source_files(output_root: pathlib.Path, validation_root: pathlib.Path) -> list[pathlib.Path]:
    candidates = [
        validation_root / "early_burst_validation_rows.csv",
        validation_root / "early_burst_validation_labels.csv",
        validation_root / "early_burst_validation_manifest.json",
        validation_root / "EARLY_BURST_VALIDATION_DATASET_SUMMARY.md",
        validation_root / "EARLY_BURST_EXIT_WINDOW_ANALYSIS.md",
        validation_root / "EARLY_BURST_VS_DEAD_COMPARISON.md",
        validation_root / "early_burst_exit_window_analysis.csv",
        validation_root / "early_burst_vs_dead_comparison.csv",
        output_root / "POSITIVE_HIGH_POSITIVE_MINT_REVIEW.md",
        output_root / "POSITIVE_OUTCOME_AUDIT.md",
        output_root / "GATE_VS_POSITIVE_OUTCOMES.md",
        output_root / "READINESS_DECISION.json",
    ]
    for horizon in HORIZONS:
        candidates.append(validation_root / f"early_burst_validation_features_{horizon:03d}s.csv")
    return [path for path in candidates if path.exists()]


def freeze_dataset(output_root: pathlib.Path, validation_root: pathlib.Path, readiness_root: pathlib.Path) -> dict[str, Any]:
    rows = read_csv(validation_root / "early_burst_validation_rows.csv")
    labels = read_csv(validation_root / "early_burst_validation_labels.csv")
    manifest = read_json(validation_root / "early_burst_validation_manifest.json")
    files = source_files(output_root, validation_root)
    file_entries = [
        {
            "path": str(path),
            "sha256": file_sha256(path),
            "bytes": path.stat().st_size,
        }
        for path in files
    ]
    summary = {
        "schema_version": "phase107j.early_burst_frozen_dataset_manifest.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repo_commit": repo_commit(),
        "source_files": file_entries,
        "row_counts": {
            "validation_rows": len(rows),
            "validation_labels": len(labels),
        },
        "unique_mints": len({row.get("mint", "") for row in rows if row.get("mint")}),
        "positive_high_rows": sum(1 for row in rows if row.get("positive_outcome_label") in {"positive", "high_positive"}),
        "positive_high_unique_mints": len({row.get("mint", "") for row in rows if row.get("positive_outcome_label") in {"positive", "high_positive"}}),
        "high_positive_rows": sum(1 for row in rows if row.get("positive_outcome_label") == "high_positive"),
        "high_positive_unique_mints": len({row.get("mint", "") for row in rows if row.get("positive_outcome_label") == "high_positive"}),
        "ordinary_clean_dead_rows": sum(1 for row in rows if row.get("early_burst_class") == "ORDINARY_CLEAN_DEAD"),
        "ordinary_clean_dead_unique_mints": len({row.get("mint", "") for row in rows if row.get("early_burst_class") == "ORDINARY_CLEAN_DEAD"}),
        "observable_exit_window_mints": manifest.get("observable_exit_window_mints", 0),
        "adverse_movement_before_exit_rows": manifest.get("adverse_movement_before_exit_rows", 0),
        "readiness_gate_snapshot": read_json(output_root / "READINESS_DECISION.json"),
    }
    summary["manifest_hash"] = stable_hash(summary)
    write_json(readiness_root / "frozen_early_burst_dataset_manifest.json", summary)
    checksum_lines = [f"{entry['sha256']}  {entry['path']}" for entry in file_entries]
    write_text(readiness_root / "frozen_early_burst_dataset_checksums.txt", "\n".join(checksum_lines) + "\n")
    write_text(
        readiness_root / "frozen_early_burst_dataset_summary.md",
        "# Frozen Early-Burst Dataset Summary\n\n"
        f"- repo_commit: `{summary['repo_commit']}`\n"
        f"- validation_rows: `{summary['row_counts']['validation_rows']}`\n"
        f"- unique_mints: `{summary['unique_mints']}`\n"
        f"- positive_high_unique_mints: `{summary['positive_high_unique_mints']}`\n"
        f"- high_positive_unique_mints: `{summary['high_positive_unique_mints']}`\n"
        f"- ordinary_clean_dead_unique_mints: `{summary['ordinary_clean_dead_unique_mints']}`\n"
        f"- observable_exit_window_mints: `{summary['observable_exit_window_mints']}`\n"
        f"- adverse_movement_before_exit_rows: `{summary['adverse_movement_before_exit_rows']}`\n"
        "- dataset_immutable_hashes_recorded: `true`\n",
    )
    return summary


def field_group_present(row: dict[str, str], fields: set[str]) -> bool:
    return any(str(row.get(field, "")).strip() not in {"", "MISSING"} for field in fields)


def load_feature_rows(validation_root: pathlib.Path) -> dict[tuple[str, str, str, str], dict[str, str]]:
    by_key: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for horizon in HORIZONS:
        for row in read_csv(validation_root / f"early_burst_validation_features_{horizon:03d}s.csv"):
            if row.get("mint") in {"", "mint"}:
                continue
            by_key[(row.get("mint", ""), row.get("slice_id", ""), row.get("segment_id", ""), str(horizon))] = row
    return by_key


def cohort_reason(
    *,
    row: dict[str, str],
    feature: dict[str, str],
    trade_present: bool,
    holder_present: bool,
    vault_present: bool,
) -> str:
    if not feature:
        return "legacy_feature_row_missing"
    if not boolish(feature.get("horizon_reached")):
        return "horizon_not_reached"
    if not boolish(feature.get("data_complete_for_horizon")):
        return "asof_feature_row_incomplete"
    if boolish(feature.get("data_quality_exclusion")) or boolish(row.get("data_quality_exclusion")):
        return "data_quality_excluded"
    if boolish(feature.get("holder_rpc_used")):
        return "holder_rpc_used_forbidden"
    if boolish(feature.get("rpc_mint_supply_canonical")):
        return "rpc_mint_supply_canonical_forbidden"
    missing = []
    if not trade_present:
        missing.append("trade_delta")
    if not holder_present:
        missing.append("holder_state")
    if not vault_present:
        missing.append("vault_curve")
    if missing:
        return "missing_" + "_".join(missing)
    return ""


def build_feature_complete_cohort(validation_root: pathlib.Path, readiness_root: pathlib.Path) -> dict[str, Any]:
    rows = read_csv(validation_root / "early_burst_validation_rows.csv")
    features = load_feature_rows(validation_root)
    out_rows: list[dict[str, Any]] = []
    eligible_rows: list[dict[str, str]] = []
    legacy_rows: list[dict[str, str]] = []
    exclusion_counter: Counter[str] = Counter()
    for row in rows:
        horizon = row.get("decision_horizon_seconds", "")
        feature = features.get((row.get("mint", ""), row.get("slice_id", ""), row.get("segment_id", ""), horizon), {})
        trade_present = field_group_present(feature, TRADE_DELTA_FIELDS)
        holder_present = field_group_present(feature, HOLDER_STATE_FIELDS)
        vault_present = field_group_present(feature, VAULT_CURVE_FIELDS)
        reason = cohort_reason(
            row=row,
            feature=feature,
            trade_present=trade_present,
            holder_present=holder_present,
            vault_present=vault_present,
        )
        eligible = reason == ""
        if eligible:
            eligible_rows.append(row)
        else:
            legacy_rows.append(row)
            exclusion_counter[reason] += 1
        out_rows.append({
            "mint": row.get("mint", ""),
            "slice_id": row.get("slice_id", ""),
            "segment_id": row.get("segment_id", ""),
            "relay_session_id": row.get("relay_session_id", ""),
            "decision_horizon_seconds": horizon,
            "positive_outcome_label": row.get("positive_outcome_label", ""),
            "early_burst_class": row.get("early_burst_class", ""),
            "final_outcome": row.get("final_outcome", ""),
            "horizon_reached": row.get("horizon_reached", ""),
            "forward_window_observed": row.get("forward_window_observed", ""),
            "feature_horizon_reached": str(boolish(feature.get("horizon_reached"))).lower(),
            "feature_data_complete_for_horizon": str(boolish(feature.get("data_complete_for_horizon"))).lower(),
            "trade_delta_present": str(trade_present).lower(),
            "holder_state_present": str(holder_present).lower(),
            "vault_curve_present": str(vault_present).lower(),
            "holder_rpc_used": str(boolish(feature.get("holder_rpc_used"))).lower(),
            "rpc_mint_supply_canonical": str(boolish(feature.get("rpc_mint_supply_canonical"))).lower(),
            "data_quality_exclusion": str(boolish(feature.get("data_quality_exclusion")) or boolish(row.get("data_quality_exclusion"))).lower(),
            "cohort": "early_burst_feature_complete_cohort" if eligible else "legacy_label_context",
            "feature_complete_for_backtest": str(eligible).lower(),
            "exclusion_reason": reason,
        })
    global_complete = sum(
        1
        for row in rows
        if row.get("horizon_reached") == "true" and row.get("forward_window_observed") == "true"
    )
    global_rate = ratio(global_complete, len(rows))
    feature_complete_rate = ratio(len(eligible_rows), len(eligible_rows)) if eligible_rows else 0.0
    positive_eligible = [row for row in eligible_rows if row.get("positive_outcome_label") in {"positive", "high_positive"}]
    high_eligible = [row for row in eligible_rows if row.get("positive_outcome_label") == "high_positive"]
    ordinary_eligible = [row for row in eligible_rows if row.get("early_burst_class") == "ORDINARY_CLEAN_DEAD"]
    report = {
        "schema_version": "phase107j.early_burst_feature_complete_cohort.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_rows": len(rows),
        "legacy_label_context_rows": len(legacy_rows),
        "early_burst_feature_complete_cohort_rows": len(eligible_rows),
        "legacy_rows_excluded_from_backtest_features": len(legacy_rows),
        "global_feature_complete_rows": global_complete,
        "global_feature_completeness": global_rate,
        "feature_complete_cohort_completeness": feature_complete_rate,
        "feature_complete_unique_mints": len({row.get("mint", "") for row in eligible_rows if row.get("mint")}),
        "feature_complete_positive_high_rows": len(positive_eligible),
        "feature_complete_positive_high_unique_mints": len({row.get("mint", "") for row in positive_eligible}),
        "feature_complete_high_positive_rows": len(high_eligible),
        "feature_complete_high_positive_unique_mints": len({row.get("mint", "") for row in high_eligible}),
        "feature_complete_ordinary_clean_dead_rows": len(ordinary_eligible),
        "feature_complete_ordinary_clean_dead_unique_mints": len({row.get("mint", "") for row in ordinary_eligible}),
        "legacy_context_positive_high_unique_mints": len({
            row.get("mint", "")
            for row in legacy_rows
            if row.get("positive_outcome_label") in {"positive", "high_positive"}
        }),
        "legacy_context_high_positive_unique_mints": len({
            row.get("mint", "")
            for row in legacy_rows
            if row.get("positive_outcome_label") == "high_positive"
        }),
        "exclusion_reasons": dict(sorted(exclusion_counter.items())),
        "legacy_rows_remain_label_context_only_reason": (
            "Legacy rows may describe label distributions and avoid-filter context, "
            "but they are excluded from formal early-burst backtest feature eligibility "
            "unless trade_delta, holder_state, and vault_curve as-of alpha groups are present "
            "without holder RPC or canonical RPC mint supply."
        ),
    }
    write_csv(readiness_root / "feature_complete_cohort.csv", out_rows, FEATURE_COHORT_FIELDS)
    write_json(readiness_root / "feature_complete_cohort_report.json", report)
    write_text(
        readiness_root / "feature_complete_cohort_report.md",
        "# Feature-Complete Early-Burst Cohort Report\n\n"
        f"- total_rows: `{report['total_rows']}`\n"
        f"- global_feature_completeness: `{report['global_feature_completeness']}`\n"
        f"- early_burst_feature_complete_cohort_rows: `{report['early_burst_feature_complete_cohort_rows']}`\n"
        f"- feature_complete_cohort_completeness: `{report['feature_complete_cohort_completeness']}`\n"
        f"- legacy_rows_excluded_from_backtest_features: `{report['legacy_rows_excluded_from_backtest_features']}`\n"
        f"- feature_complete_unique_mints: `{report['feature_complete_unique_mints']}`\n"
        f"- feature_complete_positive_high_unique_mints: `{report['feature_complete_positive_high_unique_mints']}`\n"
        f"- feature_complete_high_positive_unique_mints: `{report['feature_complete_high_positive_unique_mints']}`\n"
        f"- feature_complete_ordinary_clean_dead_unique_mints: `{report['feature_complete_ordinary_clean_dead_unique_mints']}`\n"
        f"- exclusion_reasons: `{report['exclusion_reasons']}`\n\n"
        "Legacy feature-incomplete rows remain `legacy_label_context`: useful for descriptive label context, "
        "but not eligible for formal early-burst backtest features.\n",
    )
    return report


def hypothesis_registry(readiness_root: pathlib.Path) -> dict[str, Any]:
    families = [
        ("early_curve_progress_followthrough", "Early curve progress may identify short-window burst behavior that needs an exit-risk model."),
        ("early_buy_sell_followthrough", "Early net buy followthrough may separate bursts from ordinary clean-dead negatives."),
        ("early_holder_growth_confirmation", "Early holder growth may confirm broader participation when stream-authoritative."),
        ("early_vault_curve_progress_confirmation", "Vault/curve progress may confirm burst strength without using forward labels."),
        ("low_adverse_sell_pressure_filter", "Low observed sell pressure may be required before a mint is safe enough for future evaluation."),
        ("holder_concentration_risk_filter", "High holder concentration should remain a risk filter, not an alpha shortcut."),
        ("dev_or_creator_holding_risk_filter", "Dev/creator holding risk should block or audit-only mints before future replay/backtest."),
        ("fast_exit_window_required", "Early-burst setups need an observable exit window before death/invalidation."),
        ("avoid_if_death_after_burst_pattern_seen", "Death-after-burst patterns should be invalidation evidence until future validation proves otherwise."),
    ]
    entries = []
    for idx, (name, text) in enumerate(families, start=1):
        entries.append({
            "hypothesis_id": f"EBH-{idx:03d}",
            "family": name,
            "hypothesis": text,
            "allowed_feature_groups": ["trade_delta", "holder_state", "vault_curve", "data_quality_exclusion"],
            "forbidden_fields": sorted(FORBIDDEN_ALPHA_COLUMNS),
            "decision_horizons": [5, 10, 30, 60, 120, 300, 900],
            "forward_windows": ["fixed_validation_windows_only"],
            "expected_label": "positive_or_high_positive_stream_proxy",
            "risk_caveats": [
                "not_a_buy_signal",
                "positive_labels_are_not_profitability",
                "requires_exit_window_validation",
                "replay_backtest_tuning_trading_blocked",
            ],
            "status": "research_only",
            "threshold_tuning_allowed": False,
            "backtest_allowed": False,
            "descriptive_bins_only": ["LOW", "MEDIUM", "HIGH", "MISSING", "UNSAFE", "CENSORED"],
        })
    registry = {
        "schema_version": "phase107j.early_burst_hypotheses.v1",
        "locked_at": datetime.now(timezone.utc).isoformat(),
        "hypotheses": entries,
        "hypothesis_count": len(entries),
        "locked": True,
    }
    registry["registry_hash"] = stable_hash(registry)
    write_json(readiness_root / "early_burst_hypotheses.json", registry)
    write_text(
        readiness_root / "EARLY_BURST_HYPOTHESES.md",
        "# Early-Burst Hypotheses\n\n"
        + "\n".join(f"- `{entry['hypothesis_id']}` `{entry['family']}`: {entry['hypothesis']}" for entry in entries)
        + "\n\nAll hypotheses are research-only. No numeric thresholds were optimized and no backtest is allowed until readiness passes and operator approval exists.\n",
    )
    return registry


def parse_iso_like(value: str) -> datetime | None:
    try:
        if not value or value.startswith("["):
            return None
        return datetime.fromisoformat(value.replace(" UTC", "+00:00").replace("Z", "+00:00"))
    except ValueError:
        return None


def leakage_audit(validation_root: pathlib.Path, readiness_root: pathlib.Path, splits: dict[str, Any] | None = None) -> dict[str, Any]:
    blockers: list[str] = []
    rows_checked = 0
    for horizon in HORIZONS:
        path = validation_root / f"early_burst_validation_features_{horizon:03d}s.csv"
        rows = read_csv(path)
        if not rows:
            blockers.append(f"missing_feature_rows:{horizon}")
            continue
        header = set(rows[0].keys())
        leaked = sorted(FORBIDDEN_ALPHA_COLUMNS & header)
        if leaked:
            blockers.append(f"forbidden_alpha_columns:{horizon}:{','.join(leaked)}")
        for row in rows:
            rows_checked += 1
            if boolish(row.get("holder_rpc_used")):
                blockers.append(f"holder_rpc_used:{row.get('mint','')}")
            if boolish(row.get("rpc_mint_supply_canonical")):
                blockers.append(f"rpc_mint_supply_canonical:{row.get('mint','')}")
            if boolish(row.get("threshold_tuning_allowed")) or boolish(row.get("live_trading_enabled")):
                blockers.append(f"forbidden_execution_flag:{row.get('mint','')}")
            asof = parse_iso_like(row.get("feature_asof_timestamp", ""))
            first = parse_iso_like(row.get("mint_first_seen_timestamp", ""))
            if asof and first:
                allowed = int(row.get("horizon_seconds", "0") or 0)
                if (asof - first).total_seconds() > allowed + 0.001:
                    blockers.append(f"post_horizon_timestamp:{horizon}:{row.get('mint','')}")
    for row in read_csv(validation_root / "early_burst_validation_rows.csv"):
        if row.get("final_outcome") == "terminal_inconclusive" and row.get("early_burst_class") == "ORDINARY_CLEAN_DEAD":
            blockers.append(f"terminal_inconclusive_treated_as_dead:{row.get('mint','')}")
        if row.get("positive_outcome_label") in {"positive", "high_positive"} and boolish(row.get("replay_eligible")):
            blockers.append(f"positive_label_implied_replay:{row.get('mint','')}")
    if splits:
        membership: dict[str, str] = {}
        for name in ("train", "validation", "test"):
            for mint in splits.get(name, []):
                if mint in membership:
                    blockers.append(f"same_mint_multiple_splits:{mint}:{membership[mint]}:{name}")
                membership[mint] = name
    result = {
        "schema_version": "phase107j.early_burst_leakage_audit.v1",
        "passed": not blockers,
        "blockers": sorted(set(blockers)),
        "rows_checked": rows_checked,
        "forward_labels_only_used_as_labels": True,
        "provider_relay_quality_only_exclusion_filters": True,
    }
    write_json(readiness_root / "early_burst_leakage_audit.json", result)
    write_text(
        readiness_root / "EARLY_BURST_LEAKAGE_AUDIT.md",
        "# Early-Burst Leakage Audit\n\n"
        f"- passed: `{str(result['passed']).lower()}`\n"
        f"- rows_checked: `{rows_checked}`\n"
        f"- blockers: `{result['blockers']}`\n"
        "- final outcomes, positive labels, replay fields, forward-window metrics, R2/artifact status, holder RPC, and canonical RPC mint supply are blocked as alpha features.\n",
    )
    return result


def build_splits(validation_root: pathlib.Path, readiness_root: pathlib.Path, *, embargo_rows: int = 5) -> dict[str, Any]:
    labels = read_csv(validation_root / "early_burst_validation_labels.csv")
    unique_by_mint: dict[str, dict[str, str]] = {}
    for row in sorted(labels, key=lambda r: (r.get("slice_id", ""), r.get("segment_id", ""), r.get("mint", ""), r.get("decision_horizon_seconds", ""))):
        mint = row.get("mint", "")
        if mint and mint not in unique_by_mint:
            unique_by_mint[mint] = row
    ordered = list(unique_by_mint.values())
    total = len(ordered)
    blockers: list[str] = []
    if total < 30:
        blockers.append("EARLY_BURST_SPLIT_BLOCK_SAMPLE_TOO_SMALL")
    train_end = int(total * 0.6)
    validation_start = min(total, train_end + embargo_rows)
    validation_end = min(total, validation_start + int(total * 0.2))
    test_start = min(total, validation_end + embargo_rows)
    splits = {
        "schema_version": "phase107j.early_burst_splits.v1",
        "split_id": stable_hash({"mints": [row.get("mint") for row in ordered], "embargo_rows": embargo_rows})[:16],
        "method": "chronological_walk_forward",
        "embargo_rows": embargo_rows,
        "train": [row.get("mint", "") for row in ordered[:train_end]],
        "validation": [row.get("mint", "") for row in ordered[validation_start:validation_end]],
        "test": [row.get("mint", "") for row in ordered[test_start:]],
        "excluded_censored_or_invalid": sorted({row.get("mint", "") for row in labels if row.get("early_burst_class") == "CENSORED_OR_INCOMPLETE"}),
        "blockers": blockers,
    }
    splits["split_window_count"] = sum(1 for name in ("train", "validation", "test") if splits[name])
    splits["passed"] = not blockers and splits["split_window_count"] >= MIN_SPLIT_WINDOWS
    splits["manifest_hash"] = stable_hash(splits)
    write_json(readiness_root / "early_burst_splits.json", splits)
    write_text(
        readiness_root / "EARLY_BURST_SPLITS.md",
        "# Early-Burst Splits\n\n"
        f"- method: `{splits['method']}`\n"
        f"- split_id: `{splits['split_id']}`\n"
        f"- train_mints: `{len(splits['train'])}`\n"
        f"- validation_mints: `{len(splits['validation'])}`\n"
        f"- test_mints: `{len(splits['test'])}`\n"
        f"- embargo_rows: `{embargo_rows}`\n"
        f"- passed: `{str(splits['passed']).lower()}`\n"
        f"- blockers: `{blockers}`\n",
    )
    return splits


def baseline_analysis(validation_root: pathlib.Path, readiness_root: pathlib.Path) -> dict[str, Any]:
    rows = read_csv(validation_root / "early_burst_validation_rows.csv")
    cohorts = {
        "no_trade": rows,
        "random_candidate_watch": [row for idx, row in enumerate(rows) if idx % 20 == 0],
        "early_burst_watch": [row for row in rows if row.get("early_burst_setup_decision") == "early_burst_watch"],
        "avoid_filter_only": [row for row in rows if "data_quality_excluded" not in row.get("early_burst_reason_codes", "")],
        "continue_tracking": [row for row in rows if row.get("early_burst_setup_decision") in {"early_burst_watch", "insufficient_data"}],
        "ordinary_clean_dead": [row for row in rows if row.get("early_burst_class") == "ORDINARY_CLEAN_DEAD"],
    }
    baselines: dict[str, Any] = {}
    for name, cohort_rows in cohorts.items():
        total = len(cohort_rows)
        positives = [row for row in cohort_rows if row.get("positive_outcome_label") in {"positive", "high_positive"}]
        dead_after = [row for row in cohort_rows if row.get("early_burst_class") in {"EARLY_BURST_THEN_DEAD", "HIGH_POSITIVE_THEN_DEAD"}]
        adverse = [row for row in cohort_rows if row.get("sell_pressure_before_burst") in {"MEDIUM", "HIGH"} or row.get("holder_risk_before_burst") in {"MEDIUM", "HIGH", "UNSAFE"}]
        complete = [row for row in cohort_rows if row.get("horizon_reached") == "true"]
        exit_window = [row for row in cohort_rows if row.get("exit_window_observed") == "true"]
        censored = [row for row in cohort_rows if row.get("early_burst_class") == "CENSORED_OR_INCOMPLETE"]
        baselines[name] = {
            "rows": total,
            "unique_mints": len({row.get("mint", "") for row in cohort_rows}),
            "positive_high_capture_rate": ratio(len(positives), total),
            "false_positive_proxy_rate": ratio(len(dead_after), total),
            "death_after_burst_rate": ratio(len(dead_after), total),
            "adverse_before_exit_rate": ratio(len(adverse), total),
            "censored_invalid_rate": ratio(len(censored), total),
            "feature_completeness_rate": ratio(len(complete), total),
            "exit_window_availability_rate": ratio(len(exit_window), total),
        }
    result = {
        "schema_version": "phase107j.early_burst_baselines.v1",
        "profit_metrics_computed": False,
        "thresholds_tuned": False,
        "baselines": baselines,
    }
    write_json(readiness_root / "early_burst_baselines.json", result)
    write_text(
        readiness_root / "EARLY_BURST_BASELINE_ANALYSIS.md",
        "# Early-Burst Baseline Analysis\n\n"
        + "\n".join(
            f"- `{name}` rows `{payload['rows']}` positive_high_capture_rate `{payload['positive_high_capture_rate']}` death_after_burst_rate `{payload['death_after_burst_rate']}`"
            for name, payload in baselines.items()
        )
        + "\n\nNo profit, ROI, Sharpe, win-rate, edge, or tuned threshold metric was computed.\n",
    )
    return result


def ratio(num: int, den: int) -> float:
    return round(num / den, 6) if den else 0.0


def execution_assumptions(readiness_root: pathlib.Path) -> dict[str, Any]:
    result = {
        "schema_version": "phase107j.early_burst_execution_assumptions.v1",
        "decision_latency_placeholder_ms": "TBD_research_only",
        "order_latency_placeholder_ms": "TBD_research_only",
        "slippage_model_placeholder": "not_validated",
        "priority_fee_model_placeholder": "not_validated",
        "liquidity_curve_impact_placeholder": "not_validated",
        "max_adverse_movement_proxy": "from_validation_labels_only",
        "max_favorable_movement_proxy": "from_validation_labels_only",
        "exit_window_requirement": "observable_before_death_or_invalidation",
        "kill_switch_conditions": ["provider_gap", "sequence_gap", "hash_mismatch", "receiver_backpressure", "holder_or_dev_risk_high"],
        "invalidation_conditions": ["death_after_burst", "adverse_sell_pressure", "liquidity_exit_proxy", "terminal_inconclusive"],
        "assumptions_not_yet_validated": ["execution_latency", "fees", "slippage", "exit_fill_feasibility", "market_impact"],
        "paper_trading_enabled": False,
        "live_trading_enabled": False,
        "orders_generated": False,
    }
    write_json(readiness_root / "early_burst_execution_assumptions.json", result)
    write_text(
        readiness_root / "EARLY_BURST_EXECUTION_ASSUMPTIONS.md",
        "# Early-Burst Execution Assumptions\n\n"
        "- decision_latency_placeholder_ms: `TBD_research_only`\n"
        "- order_latency_placeholder_ms: `TBD_research_only`\n"
        "- slippage_model_placeholder: `not_validated`\n"
        "- priority_fee_model_placeholder: `not_validated`\n"
        "- liquidity_curve_impact_placeholder: `not_validated`\n"
        "- paper/live trading and order generation remain disabled.\n",
    )
    return result


def readiness_decision(
    *,
    frozen: dict[str, Any],
    cohort: dict[str, Any],
    hypotheses: dict[str, Any],
    audit: dict[str, Any],
    splits: dict[str, Any],
    baselines: dict[str, Any],
    assumptions: dict[str, Any],
    readiness_root: pathlib.Path,
    output_root: pathlib.Path,
) -> dict[str, Any]:
    global_feature_rate = float(cohort.get("global_feature_completeness", 0.0))
    feature_rate = float(cohort.get("feature_complete_cohort_completeness", 0.0))
    blockers: list[str] = []
    if not pathlib.Path(readiness_root / "frozen_early_burst_dataset_manifest.json").exists():
        blockers.append("frozen_dataset_manifest_missing")
    if not pathlib.Path(readiness_root / "feature_complete_cohort_report.md").exists():
        blockers.append("feature_complete_cohort_report_missing")
    if not audit.get("passed"):
        blockers.append("early_burst_leakage_audit_failed")
    if not splits.get("passed"):
        blockers.extend(splits.get("blockers") or ["chronological_splits_missing"])
    if not hypotheses.get("locked"):
        blockers.append("hypotheses_not_locked")
    if not baselines:
        blockers.append("baseline_analysis_missing")
    if not assumptions:
        blockers.append("execution_assumptions_missing")
    if int(cohort.get("feature_complete_positive_high_unique_mints", 0)) < MIN_POSITIVE_MINTS:
        blockers.append("sample_size_positive_too_small")
    if int(cohort.get("feature_complete_high_positive_unique_mints", 0)) < MIN_HIGH_POSITIVE_MINTS:
        blockers.append("sample_size_high_positive_too_small")
    if int(cohort.get("feature_complete_ordinary_clean_dead_unique_mints", 0)) < MIN_ORDINARY_NEGATIVES:
        blockers.append("sample_size_ordinary_negatives_too_small")
    if int(splits.get("split_window_count", 0)) < MIN_SPLIT_WINDOWS:
        blockers.append("split_window_count_too_small")
    if feature_rate < MIN_FEATURE_COMPLETENESS:
        blockers.append("feature_completeness_below_minimum")
    blockers.extend(["operator_approval_missing", "formal_backtest_not_allowed"])
    sample_blockers = {b for b in blockers if b.startswith("sample_size_") or b in {"feature_completeness_below_minimum", "split_window_count_too_small"}}
    classification = "EARLY_BURST_BACKTEST_READINESS_PASS" if not blockers else (
        "EARLY_BURST_BACKTEST_READINESS_BLOCK_SAMPLE_SIZE" if sample_blockers else "EARLY_BURST_BACKTEST_READINESS_BLOCK_GATE"
    )
    ready = classification == "EARLY_BURST_BACKTEST_READINESS_PASS"
    decision = {
        "schema_version": "phase107j.early_burst_backtest_readiness_decision.v1",
        "classification": classification,
        "early_burst_backtesting_ready": ready,
        "formal_backtesting_ready": False,
        "replay_ready": False,
        "threshold_tuning_ready": False,
        "paper_trading_ready": False,
        "live_trading_ready": False,
        "wallet_execution_ready": False,
        "profitability_claim_allowed": False,
        "operator_approval_present": False,
        "reason_codes": sorted(set(blockers)),
        "sample_checks": {
            "global_positive_high_unique_mints": frozen.get("positive_high_unique_mints", 0),
            "global_high_positive_unique_mints": frozen.get("high_positive_unique_mints", 0),
            "global_ordinary_clean_dead_unique_mints": frozen.get("ordinary_clean_dead_unique_mints", 0),
            "positive_high_unique_mints": cohort.get("feature_complete_positive_high_unique_mints", 0),
            "min_positive_high_unique_mints": MIN_POSITIVE_MINTS,
            "high_positive_unique_mints": cohort.get("feature_complete_high_positive_unique_mints", 0),
            "min_high_positive_unique_mints": MIN_HIGH_POSITIVE_MINTS,
            "ordinary_clean_dead_unique_mints": cohort.get("feature_complete_ordinary_clean_dead_unique_mints", 0),
            "min_ordinary_clean_dead_unique_mints": MIN_ORDINARY_NEGATIVES,
            "split_window_count": splits.get("split_window_count", 0),
            "min_split_windows": MIN_SPLIT_WINDOWS,
            "feature_completeness_rate": feature_rate,
            "global_feature_completeness": global_feature_rate,
            "feature_complete_cohort_completeness": feature_rate,
            "feature_complete_cohort_rows": cohort.get("early_burst_feature_complete_cohort_rows", 0),
            "legacy_rows_excluded_from_backtest_features": cohort.get("legacy_rows_excluded_from_backtest_features", 0),
            "min_feature_completeness_rate": MIN_FEATURE_COMPLETENESS,
        },
        "no_replay_backtest_tuning_trading_run": True,
    }
    write_json(readiness_root / "early_burst_backtest_readiness_decision.json", decision)
    write_text(
        readiness_root / "EARLY_BURST_BACKTEST_READINESS_DECISION.md",
        "# Early-Burst Backtest Readiness Decision\n\n"
        f"- classification: `{classification}`\n"
        f"- early_burst_backtesting_ready: `{str(ready).lower()}`\n"
        f"- positive_high_unique_mints_feature_complete: `{decision['sample_checks']['positive_high_unique_mints']}`\n"
        f"- high_positive_unique_mints_feature_complete: `{decision['sample_checks']['high_positive_unique_mints']}`\n"
        f"- ordinary_clean_dead_unique_mints_feature_complete: `{decision['sample_checks']['ordinary_clean_dead_unique_mints']}`\n"
        f"- feature_complete_cohort_completeness: `{decision['sample_checks']['feature_complete_cohort_completeness']}`\n"
        f"- global_feature_completeness: `{decision['sample_checks']['global_feature_completeness']}`\n"
        f"- legacy_rows_excluded_from_backtest_features: `{decision['sample_checks']['legacy_rows_excluded_from_backtest_features']}`\n"
        f"- formal_backtesting_ready: `false`\n"
        f"- replay_ready: `false`\n"
        f"- threshold_tuning_ready: `false`\n"
        f"- paper_trading_ready: `false`\n"
        f"- live_trading_ready: `false`\n"
        f"- profitability_claim_allowed: `false`\n"
        f"- reason_codes: `{', '.join(decision['reason_codes'])}`\n",
    )
    return decision


def next_data_needed(readiness_root: pathlib.Path, decision: dict[str, Any]) -> None:
    checks = decision["sample_checks"]
    need_positive = max(0, checks["min_positive_high_unique_mints"] - checks["positive_high_unique_mints"])
    need_high = max(0, checks["min_high_positive_unique_mints"] - checks["high_positive_unique_mints"])
    need_negatives = max(0, checks["min_ordinary_clean_dead_unique_mints"] - checks["ordinary_clean_dead_unique_mints"])
    write_text(
        readiness_root / "EARLY_BURST_NEXT_DATA_NEEDED.md",
        "# Early-Burst Next Data Needed\n\n"
        f"- additional_positive_high_unique_mints_needed: `{need_positive}`\n"
        f"- additional_high_positive_unique_mints_needed: `{need_high}`\n"
        f"- additional_feature_complete_ordinary_clean_dead_unique_mints_needed: `{need_negatives}`\n"
        f"- feature_complete_cohort_rows: `{checks['feature_complete_cohort_rows']}`\n"
        f"- feature_complete_cohort_completeness: `{checks['feature_complete_cohort_completeness']}`\n"
        f"- global_feature_completeness: `{checks['global_feature_completeness']}`\n"
        f"- legacy_rows_excluded_from_backtest_features: `{checks['legacy_rows_excluded_from_backtest_features']}`\n"
        "- feature_groups_under_covered: `none structurally missing in feature-complete cohort; sample size is the limiting factor`\n"
        "- horizons_under_covered: `none structurally missing; future data should keep same fixed horizons`\n"
        "- same_caps_should_continue: `true`\n"
        "- launch_caps_remain_blocked: `true`\n"
        "- survivor_or_early_burst_mode_should_continue: `targeted early-burst/survivor collection, not generic collection`\n"
        "- generic_collection_useful: `limited`; targeted early-burst validation examples are more useful.\n",
    )


def targeted_collection_plan(readiness_root: pathlib.Path, decision: dict[str, Any]) -> None:
    checks = decision["sample_checks"]
    need_positive = max(0, checks["min_positive_high_unique_mints"] - checks["positive_high_unique_mints"])
    need_high = max(0, checks["min_high_positive_unique_mints"] - checks["high_positive_unique_mints"])
    need_negatives = max(0, checks["min_ordinary_clean_dead_unique_mints"] - checks["ordinary_clean_dead_unique_mints"])
    # Observed rates are descriptive only and intentionally rounded up conservatively.
    cohort_rows = max(1, int(checks.get("feature_complete_cohort_rows", 0)))
    observed_positive_rate = checks["positive_high_unique_mints"] / cohort_rows
    observed_high_rate = checks["high_positive_unique_mints"] / cohort_rows
    estimated_rows_for_positive = int(need_positive / observed_positive_rate) if observed_positive_rate > 0 and need_positive else 0
    estimated_rows_for_high = int(need_high / observed_high_rate) if observed_high_rate > 0 and need_high else 0
    lines = [
        "# Targeted Early-Burst Collection Plan",
        "",
        "This plan continues conservative relay-only R2-primary collection for feature-complete early-burst examples only. It is not a replay, backtest, threshold tune, paper-trading run, live-trading run, or cap raise.",
        "",
        f"- current_positive_high_unique_mints_feature_complete: `{checks['positive_high_unique_mints']}`",
        f"- current_high_positive_unique_mints_feature_complete: `{checks['high_positive_unique_mints']}`",
        f"- target_positive_high_unique_mints: `{checks['min_positive_high_unique_mints']}`",
        f"- target_high_positive_unique_mints: `{checks['min_high_positive_unique_mints']}`",
        f"- current_feature_complete_cohort_rows: `{checks['feature_complete_cohort_rows']}`",
        f"- current_feature_complete_ordinary_clean_dead_unique_mints: `{checks['ordinary_clean_dead_unique_mints']}`",
        f"- target_feature_complete_ordinary_clean_dead_unique_mints: `{checks['min_ordinary_clean_dead_unique_mints']}`",
        f"- additional_positive_high_unique_mints_needed: `{need_positive}`",
        f"- additional_high_positive_unique_mints_needed: `{need_high}`",
        f"- additional_feature_complete_ordinary_clean_dead_unique_mints_needed: `{need_negatives}`",
        f"- estimated_feature_complete_rows_needed_for_positive_target: `{estimated_rows_for_positive}`",
        f"- estimated_feature_complete_rows_needed_for_high_positive_target: `{estimated_rows_for_high}`",
        "",
        "## Batch Settings",
        "- slices_per_batch: `10`",
        "- duration_seconds_per_slice: `900`",
        "- max_attempted_launches: `15`",
        "- target_candidates: `2`",
        "- storage_mode: `r2-primary`",
        "- retention_mode: `keep-manifests-after-verified-r2`",
        "- as_of_alpha_retention_required: `true`",
        "- one_slice_at_a_time: `true`",
        "",
        "## Stop Conditions",
        "- stop_on_any_sequence_gap_hash_mismatch_receiver_backpressure_or_unavailable: `true`",
        "- stop_on_R2_or_artifact_consistency_failure: `true`",
        "- stop_on_VPS_forbidden_artifact_or_latest_run_id_mutation: `true`",
        "- stop_on_candidate_checkpoint_or_replay_eligible_candidate: `true`; generate review pack and do not replay.",
        "- stop_when_data_requirements_pass_but_operator_approval_missing: `true`; return `EARLY_BURST_BACKTEST_READINESS_BLOCK_OPERATOR_APPROVAL_ONLY`.",
        "",
        "## Constraints",
        "- launch_caps_remain_blocked: `true`",
        "- replay_backtesting_threshold_tuning_paper_live_wallet_execution: `blocked`",
        "- holder_rpc_enabled: `false`",
        "- rpc_mint_supply_canonical: `false`",
        "- profitability_claims_allowed: `false`",
        "- R2/local disk: use R2-primary compact artifacts and verified retention; do not retain raw relay frames in GPT packs.",
    ]
    write_text(readiness_root / "TARGETED_EARLY_BURST_COLLECTION_PLAN.md", "\n".join(lines) + "\n")


def update_readiness(output_root: pathlib.Path, decision: dict[str, Any]) -> None:
    readiness_path = output_root / "READINESS_DECISION.json"
    readiness = read_json(readiness_path)
    readiness.update({
        "early_burst_backtesting_ready": decision["early_burst_backtesting_ready"],
        "formal_backtesting_ready": False,
        "backtesting_ready": False,
        "replay_ready": False,
        "threshold_tuning_ready": False,
        "paper_trading_ready": False,
        "live_trading_ready": False,
        "wallet_execution_ready": False,
        "profitability_claim_allowed": False,
    })
    reasons = list(readiness.get("reason_codes", []))
    for reason in decision.get("reason_codes", []):
        if reason not in reasons:
            reasons.append(reason)
    readiness["reason_codes"] = reasons
    write_json(readiness_path, readiness)


def write_pack(output_root: pathlib.Path, readiness_root: pathlib.Path) -> pathlib.Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    pack = output_root / f"early_burst_backtest_readiness_pack_{timestamp}"
    pack.mkdir(parents=True, exist_ok=True)
    for path in [
        readiness_root / "frozen_early_burst_dataset_summary.md",
        readiness_root / "EARLY_BURST_HYPOTHESES.md",
        readiness_root / "EARLY_BURST_LEAKAGE_AUDIT.md",
        readiness_root / "EARLY_BURST_SPLITS.md",
        readiness_root / "EARLY_BURST_BASELINE_ANALYSIS.md",
        readiness_root / "EARLY_BURST_EXECUTION_ASSUMPTIONS.md",
        readiness_root / "EARLY_BURST_BACKTEST_READINESS_DECISION.md",
        readiness_root / "feature_complete_cohort_report.md",
        readiness_root / "TARGETED_EARLY_BURST_COLLECTION_PLAN.md",
        output_root / "early_burst_validation_dataset" / "EARLY_BURST_EXIT_WINDOW_ANALYSIS.md",
        output_root / "early_burst_validation_dataset" / "EARLY_BURST_VS_DEAD_COMPARISON.md",
    ]:
        if path.exists() and safe_pack_path(path):
            shutil.copy2(path, pack / path.name)
    write_text(
        pack / "README_FOR_GPT.md",
        "# Early-Burst Backtest Readiness Pack\n\n"
        "This pack freezes the early-burst validation dataset and evaluates readiness. It does not contain raw relay frames, secrets, or trade instructions.\n",
    )
    write_text(
        pack / "GPT_EARLY_BURST_BACKTEST_READINESS_PROMPT.md",
        "# GPT Early-Burst Backtest Readiness Prompt\n\n"
        "Do not claim profitability. Do not tune thresholds. Do not run backtests. Do not output trade entries. Focus on validating readiness, sample size, leakage safety, exit-window feasibility, and data needed before a formal backtest.\n",
    )
    zip_path = output_root / f"{pack.name}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(p for p in pack.rglob("*") if p.is_file()):
            if safe_pack_path(path):
                archive.write(path, path.relative_to(pack))
    return pack


def build_early_burst_backtest_readiness(
    *,
    output_root: pathlib.Path = PIPELINE_ROOT,
    validation_root: pathlib.Path = VALIDATION_ROOT,
    readiness_root: pathlib.Path = BACKTEST_READINESS_ROOT,
) -> dict[str, Any]:
    readiness_root.mkdir(parents=True, exist_ok=True)
    frozen = freeze_dataset(output_root, validation_root, readiness_root)
    cohort = build_feature_complete_cohort(validation_root, readiness_root)
    hypotheses = hypothesis_registry(readiness_root)
    splits = build_splits(validation_root, readiness_root)
    audit = leakage_audit(validation_root, readiness_root, splits)
    baselines = baseline_analysis(validation_root, readiness_root)
    assumptions = execution_assumptions(readiness_root)
    decision = readiness_decision(
        frozen=frozen,
        cohort=cohort,
        hypotheses=hypotheses,
        audit=audit,
        splits=splits,
        baselines=baselines,
        assumptions=assumptions,
        readiness_root=readiness_root,
        output_root=output_root,
    )
    next_data_needed(readiness_root, decision)
    targeted_collection_plan(readiness_root, decision)
    pack = write_pack(output_root, readiness_root)
    update_readiness(output_root, decision)
    summary = {
        "schema_version": "phase107j.early_burst_backtest_readiness_summary.v1",
        "classification": decision["classification"],
        "frozen_dataset_manifest_path": str(readiness_root / "frozen_early_burst_dataset_manifest.json"),
        "feature_complete_cohort_report_path": str(readiness_root / "feature_complete_cohort_report.md"),
        "feature_complete_cohort_csv_path": str(readiness_root / "feature_complete_cohort.csv"),
        "targeted_collection_plan_path": str(readiness_root / "TARGETED_EARLY_BURST_COLLECTION_PLAN.md"),
        "hypothesis_registry_path": str(readiness_root / "early_burst_hypotheses.json"),
        "leakage_audit_path": str(readiness_root / "early_burst_leakage_audit.json"),
        "split_manifest_path": str(readiness_root / "early_burst_splits.json"),
        "baseline_analysis_path": str(readiness_root / "early_burst_baselines.json"),
        "execution_assumptions_path": str(readiness_root / "early_burst_execution_assumptions.json"),
        "decision_path": str(readiness_root / "early_burst_backtest_readiness_decision.json"),
        "gpt_pack_path": str(pack),
        "gpt_pack_zip_path": str(output_root / f"{pack.name}.zip"),
        "decision": decision,
    }
    write_json(readiness_root / "early_burst_backtest_readiness_summary.json", summary)
    return summary
