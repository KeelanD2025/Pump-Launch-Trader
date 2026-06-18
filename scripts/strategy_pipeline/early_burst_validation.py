from __future__ import annotations

import pathlib
import shutil
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from .io import read_csv, read_json, write_csv, write_json, write_text
from .schemas import DATA_MART_ROOT, HORIZONS, PIPELINE_ROOT, STRATEGY_ARCHITECTURE_ROOT, boolish, file_sha256


VALIDATION_ROOT = PIPELINE_ROOT / "early_burst_validation_dataset"

VALIDATION_ROW_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "decision_horizon_seconds",
    "forward_window_seconds",
    "horizon_reached",
    "forward_window_observed",
    "data_quality_exclusion",
    "early_burst_setup_decision",
    "early_burst_reason_codes",
    "final_outcome",
    "rejection_reason",
    "terminal_inconclusive_reason",
    "positive_outcome_label",
    "positive_outcome_strength_bin",
    "early_burst_class",
    "max_favorable_proxy",
    "max_adverse_proxy",
    "time_to_max_favorable_ms",
    "time_to_max_adverse_ms",
    "time_to_rejection_ms",
    "time_to_terminal_ms",
    "could_exit_before_death_proxy",
    "exit_window_observed",
    "exit_window_quality",
    "holder_risk_before_burst",
    "holder_risk_after_burst",
    "vault_curve_progress_before_burst",
    "vault_curve_progress_after_burst",
    "sell_pressure_before_burst",
    "sell_pressure_after_burst",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "backtest_allowed",
    "replay_allowed",
    "trade_allowed",
]

VALIDATION_LABEL_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "decision_horizon_seconds",
    "forward_window_seconds",
    "final_outcome",
    "rejection_reason",
    "terminal_inconclusive_reason",
    "positive_outcome_label",
    "positive_outcome_strength_bin",
    "early_burst_class",
    "max_favorable_proxy",
    "max_adverse_proxy",
    "time_to_max_favorable_ms",
    "time_to_max_adverse_ms",
    "time_to_rejection_ms",
    "time_to_terminal_ms",
    "could_exit_before_death_proxy",
    "exit_window_observed",
    "exit_window_quality",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "backtest_allowed",
    "replay_allowed",
    "trade_allowed",
]

SAFE_FEATURE_FIELDS = [
    "mint",
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
    "holder_rpc_used",
    "rpc_mint_supply_canonical",
    "threshold_tuning_allowed",
    "live_trading_enabled",
]

FORBIDDEN_PACK_NAME_TOKENS = (
    "secret",
    "credential",
    "private",
    ".env",
    "raw_relay",
    "relay_frames",
    "frame_shard",
    "ssh",
    "key",
)


def num(value: Any) -> float:
    try:
        if value is None or str(value).strip() == "":
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def key_for(row: dict[str, str]) -> tuple[str, str, str]:
    return (row.get("mint", ""), row.get("slice_id", ""), row.get("segment_id", ""))


def validation_key_for(row: dict[str, str]) -> tuple[str, str, str, int]:
    return (*key_for(row), int(row.get("decision_horizon_seconds", row.get("horizon_seconds", "0")) or 0))


def load_labels(data_mart_root: pathlib.Path) -> dict[tuple[str, str, str], dict[str, str]]:
    labels: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in read_csv(data_mart_root / "strategy_labels.csv"):
        if row.get("mint") in {"", "mint"}:
            continue
        labels[key_for(row)] = row
    return labels


def load_features(data_mart_root: pathlib.Path) -> dict[tuple[str, str, str], dict[int, dict[str, str]]]:
    features: dict[tuple[str, str, str], dict[int, dict[str, str]]] = defaultdict(dict)
    for horizon in HORIZONS:
        for row in read_csv(data_mart_root / f"strategy_asof_features_{horizon:03d}s.csv"):
            if row.get("mint") in {"", "mint"}:
                continue
            features[key_for(row)][horizon] = row
    return features


def load_score_index(output_root: pathlib.Path) -> dict[tuple[str, int], dict[str, str]]:
    scores: dict[tuple[str, int], dict[str, str]] = {}
    for row in read_csv(output_root / "early_burst_setup_v0_scores.csv"):
        if row.get("mint") in {"", "mint"}:
            continue
        scores[(row.get("mint", ""), int(row.get("horizon_seconds", "0") or 0))] = row
    return scores


def load_review_index(output_root: pathlib.Path) -> dict[str, dict[str, str]]:
    return {
        row.get("mint", ""): row
        for row in read_csv(output_root / "positive_high_positive_mint_review.csv")
        if row.get("mint") not in {"", "mint"}
    }


def include_for_validation(row: dict[str, str]) -> bool:
    label = row.get("positive_outcome_label", "")
    if label in {"positive", "high_positive", "dead_negative", "censored"}:
        return True
    return boolish(row.get("clean_negative_label")) or boolish(row.get("censored_label"))


def descriptive_bin(value: float, *, high: float, medium: float, unsafe: bool = False, missing: bool = False) -> str:
    if missing:
        return "MISSING"
    if unsafe:
        return "UNSAFE"
    if value >= high:
        return "HIGH"
    if value >= medium:
        return "MEDIUM"
    return "LOW"


def favorable_proxy(row: dict[str, str], start: dict[str, str] | None) -> float:
    if not row:
        return 0.0
    base_curve = num(start.get("curve_progress_proxy_asof")) if start else 0.0
    curve = max(num(row.get("curve_progress_proxy_asof")) - base_curve, num(row.get("curve_progress_proxy_asof")))
    return max(
        curve,
        min(max(num(row.get("net_buy_sell_delta_asof")), 0.0) * 5.0, 50.0),
        25.0 if num(row.get("volume_delta_asof")) > 0 else 0.0,
        25.0 if num(row.get("new_holder_count_delta_asof")) >= 3 else 0.0,
    )


def adverse_proxy(row: dict[str, str], start: dict[str, str] | None) -> float:
    if not row:
        return 0.0
    base_liq = num(start.get("liquidity_delta_asof")) if start else 0.0
    base_reserve = num(start.get("reserve_delta_asof")) if start else 0.0
    return max(
        abs(min(num(row.get("liquidity_delta_asof")) - base_liq, 0.0)),
        abs(min(num(row.get("reserve_delta_asof")) - base_reserve, 0.0)),
        max(num(row.get("top_holder_concentration_asof")) - 0.85, 0.0) * 100.0,
        100.0 if boolish(row.get("degraded_audit_only_before_horizon")) else 0.0,
        max(num(row.get("sell_count_delta_asof")) - num(row.get("buy_count_delta_asof")), 0.0) * 5.0,
    )


def time_to_peak_ms(
    by_horizon: dict[int, dict[str, str]],
    *,
    decision_horizon: int,
    proxy_fn,
) -> tuple[str, float]:
    start = by_horizon.get(decision_horizon)
    best_horizon = ""
    best_value = -1.0
    for horizon in HORIZONS:
        if horizon < decision_horizon or horizon not in by_horizon:
            continue
        row = by_horizon[horizon]
        if not boolish(row.get("horizon_reached")):
            continue
        value = proxy_fn(row, start)
        if value > best_value:
            best_value = value
            best_horizon = str(horizon)
    if not best_horizon:
        return "", 0.0
    return str(max(0, int(best_horizon) - decision_horizon) * 1000), max(best_value, 0.0)


def horizon_row(by_horizon: dict[int, dict[str, str]], horizon: int) -> dict[str, str] | None:
    return by_horizon.get(horizon)


def end_row(by_horizon: dict[int, dict[str, str]], decision_horizon: int) -> dict[str, str] | None:
    candidates = [by_horizon[h] for h in HORIZONS if h >= decision_horizon and h in by_horizon and boolish(by_horizon[h].get("horizon_reached"))]
    return candidates[-1] if candidates else by_horizon.get(decision_horizon)


def build_validation_row(
    row: dict[str, str],
    *,
    label_row: dict[str, str],
    by_horizon: dict[int, dict[str, str]],
    score: dict[str, str],
    review: dict[str, str],
) -> dict[str, Any]:
    decision_horizon = int(row.get("decision_horizon_seconds", "0") or 0)
    start = horizon_row(by_horizon, decision_horizon)
    end = end_row(by_horizon, decision_horizon)
    time_to_fav, favorable = time_to_peak_ms(by_horizon, decision_horizon=decision_horizon, proxy_fn=favorable_proxy)
    time_to_adv, adverse = time_to_peak_ms(by_horizon, decision_horizon=decision_horizon, proxy_fn=adverse_proxy)
    label_fav = num(row.get("max_favorable_proxy"))
    label_adv = num(row.get("max_adverse_proxy"))
    favorable = max(favorable, label_fav)
    adverse = max(adverse, label_adv)
    time_to_rejection = label_row.get("time_to_rejection_ms", "")
    time_to_terminal = label_row.get("time_to_terminal_ms", "")
    exit_observed = bool(time_to_fav) and boolish(row.get("forward_window_observed"))
    if exit_observed and time_to_rejection:
        exit_observed = int(float(time_to_fav)) < int(float(time_to_rejection))
    could_exit = (
        row.get("positive_outcome_label") in {"positive", "high_positive"}
        and exit_observed
        and favorable > 0
        and favorable >= adverse
    )
    holder_before = descriptive_bin(
        max(num(start.get("top_holder_concentration_asof")) if start else 0.0, abs(num(start.get("dev_or_creator_holding_proxy_asof")) if start else 0.0)),
        high=0.85,
        medium=0.65,
        missing=start is None,
    )
    holder_after = descriptive_bin(
        max(num(end.get("top_holder_concentration_asof")) if end else 0.0, abs(num(end.get("dev_or_creator_holding_proxy_asof")) if end else 0.0)),
        high=0.85,
        medium=0.65,
        missing=end is None,
    )
    curve_before = descriptive_bin(num(start.get("curve_progress_proxy_asof")) if start else 0.0, high=75, medium=35, missing=start is None)
    curve_after = descriptive_bin(num(end.get("curve_progress_proxy_asof")) if end else 0.0, high=75, medium=35, missing=end is None)
    sell_before = descriptive_bin(num(start.get("sell_count_delta_asof")) if start else 0.0, high=5, medium=1, missing=start is None)
    sell_after = descriptive_bin(num(end.get("sell_count_delta_asof")) if end else 0.0, high=5, medium=1, missing=end is None)
    if boolish(row.get("censored_label")) or row.get("positive_outcome_label") == "censored":
        exit_quality = "CENSORED"
    elif not exit_observed:
        exit_quality = "MISSING"
    elif could_exit and row.get("positive_outcome_label") == "high_positive":
        exit_quality = "HIGH"
    elif could_exit:
        exit_quality = "MEDIUM"
    else:
        exit_quality = "UNSAFE"
    return {
        "mint": row.get("mint", ""),
        "slice_id": row.get("slice_id", ""),
        "segment_id": row.get("segment_id", ""),
        "relay_session_id": row.get("relay_session_id", ""),
        "decision_horizon_seconds": decision_horizon,
        "forward_window_seconds": row.get("forward_window_seconds", ""),
        "horizon_reached": row.get("horizon_reached", ""),
        "forward_window_observed": row.get("forward_window_observed", ""),
        "data_quality_exclusion": row.get("data_quality_exclusion", ""),
        "early_burst_setup_decision": score.get("decision", ""),
        "early_burst_reason_codes": score.get("reason_codes", ""),
        "final_outcome": row.get("final_outcome", ""),
        "rejection_reason": row.get("rejection_reason", ""),
        "terminal_inconclusive_reason": row.get("terminal_inconclusive_reason", ""),
        "positive_outcome_label": row.get("positive_outcome_label", ""),
        "positive_outcome_strength_bin": row.get("positive_outcome_strength_bin", ""),
        "early_burst_class": review.get("early_burst_research_class", validation_class(row))
        if row.get("positive_outcome_label") in {"positive", "high_positive"}
        else validation_class(row),
        "max_favorable_proxy": favorable,
        "max_adverse_proxy": adverse,
        "time_to_max_favorable_ms": time_to_fav,
        "time_to_max_adverse_ms": time_to_adv,
        "time_to_rejection_ms": time_to_rejection,
        "time_to_terminal_ms": time_to_terminal,
        "could_exit_before_death_proxy": could_exit,
        "exit_window_observed": exit_observed,
        "exit_window_quality": exit_quality,
        "holder_risk_before_burst": holder_before,
        "holder_risk_after_burst": holder_after,
        "vault_curve_progress_before_burst": curve_before,
        "vault_curve_progress_after_burst": curve_after,
        "sell_pressure_before_burst": sell_before,
        "sell_pressure_after_burst": sell_after,
        "candidate_checkpoint_seen": row.get("candidate_checkpoint_seen", "false"),
        "replay_eligible": row.get("replay_eligible", "false"),
        "backtest_allowed": False,
        "replay_allowed": False,
        "trade_allowed": False,
    }


def validation_class(row: dict[str, str]) -> str:
    if row.get("positive_outcome_label") == "high_positive" and row.get("final_outcome") == "early_rejected_dead":
        return "HIGH_POSITIVE_THEN_DEAD"
    if row.get("positive_outcome_label") == "positive" and row.get("final_outcome") == "early_rejected_dead":
        return "EARLY_BURST_THEN_DEAD"
    if row.get("positive_outcome_label") == "censored" or boolish(row.get("censored_label")):
        return "CENSORED_OR_INCOMPLETE"
    if row.get("positive_outcome_label") == "dead_negative":
        return "ORDINARY_CLEAN_DEAD"
    if boolish(row.get("data_quality_exclusion")):
        return "DATA_QUALITY_UNSAFE"
    return "NEEDS_MANUAL_REVIEW"


def safe_feature_row(source: dict[str, str] | None, fallback: dict[str, str], horizon: int) -> dict[str, Any]:
    row = source or {}
    return {
        field: row.get(field, fallback.get(field, ""))
        for field in SAFE_FEATURE_FIELDS
    } | {
        "horizon_seconds": horizon,
        "horizon_reached": row.get("horizon_reached", "false") if row else "false",
        "data_complete_for_horizon": row.get("data_complete_for_horizon", "false") if row else "false",
        "holder_rpc_used": row.get("holder_rpc_used", "false") if row else "false",
        "rpc_mint_supply_canonical": row.get("rpc_mint_supply_canonical", "false") if row else "false",
        "threshold_tuning_allowed": row.get("threshold_tuning_allowed", "false") if row else "false",
        "live_trading_enabled": row.get("live_trading_enabled", "false") if row else "false",
    }


def write_feature_files(
    root: pathlib.Path,
    validation_rows: list[dict[str, Any]],
    raw_positive_rows: dict[tuple[str, str, str, int], dict[str, str]],
    features: dict[tuple[str, str, str], dict[int, dict[str, str]]],
) -> dict[int, int]:
    counts: dict[int, int] = {}
    for horizon in HORIZONS:
        out_rows: list[dict[str, Any]] = []
        for validation_row in validation_rows:
            if int(validation_row["decision_horizon_seconds"]) != horizon:
                continue
            key = (validation_row["mint"], validation_row["slice_id"], validation_row["segment_id"])
            source = features.get(key, {}).get(horizon)
            fallback = raw_positive_rows.get((*key, horizon), {})
            out_rows.append(safe_feature_row(source, fallback, horizon))
        write_csv(root / f"early_burst_validation_features_{horizon:03d}s.csv", out_rows, SAFE_FEATURE_FIELDS)
        counts[horizon] = len(out_rows)
    return counts


def summarize_validation(rows: list[dict[str, Any]], feature_counts: dict[int, int]) -> dict[str, Any]:
    positive_rows = [row for row in rows if row["positive_outcome_label"] in {"positive", "high_positive"}]
    high_rows = [row for row in rows if row["positive_outcome_label"] == "high_positive"]
    positive_mints = {row["mint"] for row in positive_rows}
    high_mints = {row["mint"] for row in high_rows}
    exit_mints = {row["mint"] for row in positive_rows if boolish(row["exit_window_observed"])}
    adverse_before = [row for row in positive_rows if row["sell_pressure_before_burst"] in {"MEDIUM", "HIGH"} or row["holder_risk_before_burst"] in {"MEDIUM", "HIGH", "UNSAFE"}]
    return {
        "schema_version": "phase107i.early_burst_validation_manifest.v1",
        "classification": "EARLY_BURST_VALIDATION_DATASET_PASS",
        "rows": len(rows),
        "unique_mints": len({row["mint"] for row in rows}),
        "positive_high_rows": len(positive_rows),
        "positive_high_unique_mints": len(positive_mints),
        "high_positive_rows": len(high_rows),
        "high_positive_unique_mints": len(high_mints),
        "ordinary_clean_dead_rows": sum(1 for row in rows if row["early_burst_class"] == "ORDINARY_CLEAN_DEAD"),
        "censored_rows": sum(1 for row in rows if row["early_burst_class"] == "CENSORED_OR_INCOMPLETE"),
        "observable_exit_window_mints": len(exit_mints),
        "adverse_movement_before_exit_rows": len(adverse_before),
        "feature_rows_by_horizon": {str(k): v for k, v in feature_counts.items()},
        "backtesting_ready": False,
        "replay_ready": False,
        "threshold_tuning_ready": False,
        "paper_trading_ready": False,
        "live_trading_ready": False,
        "wallet_execution_ready": False,
        "profitability_claim_allowed": False,
    }


def write_summary(root: pathlib.Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Early-Burst Validation Dataset Summary",
        "",
        f"- classification: `{summary['classification']}`",
        f"- rows: `{summary['rows']}`",
        f"- unique_mints: `{summary['unique_mints']}`",
        f"- positive_high_rows: `{summary['positive_high_rows']}`",
        f"- positive_high_unique_mints: `{summary['positive_high_unique_mints']}`",
        f"- high_positive_rows: `{summary['high_positive_rows']}`",
        f"- high_positive_unique_mints: `{summary['high_positive_unique_mints']}`",
        f"- ordinary_clean_dead_rows: `{summary['ordinary_clean_dead_rows']}`",
        f"- censored_rows: `{summary['censored_rows']}`",
        f"- observable_exit_window_mints: `{summary['observable_exit_window_mints']}`",
        f"- adverse_movement_before_exit_rows: `{summary['adverse_movement_before_exit_rows']}`",
        "- as_of_features_and_forward_labels_separated: `true`",
        "- replay/backtesting/threshold_tuning/paper/live/wallet/profitability_claims: `blocked`",
        "- positive/high-positive labels are validation targets only, not buy signals.",
    ]
    write_text(root / "EARLY_BURST_VALIDATION_DATASET_SUMMARY.md", "\n".join(lines) + "\n")


def write_exit_window_analysis(root: pathlib.Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    positive = [row for row in rows if row["positive_outcome_label"] in {"positive", "high_positive"}]
    summary = {
        "positive_high_rows": len(positive),
        "positive_high_unique_mints": len({row["mint"] for row in positive}),
        "observable_exit_window_rows": sum(1 for row in positive if boolish(row["exit_window_observed"])),
        "observable_exit_window_unique_mints": len({row["mint"] for row in positive if boolish(row["exit_window_observed"])}),
        "died_before_exit_window_rows": sum(1 for row in positive if row["final_outcome"] == "early_rejected_dead" and not boolish(row["exit_window_observed"])),
        "adverse_sell_pressure_before_rows": sum(1 for row in positive if row["sell_pressure_before_burst"] in {"MEDIUM", "HIGH"}),
        "adverse_sell_pressure_after_rows": sum(1 for row in positive if row["sell_pressure_after_burst"] in {"MEDIUM", "HIGH"}),
        "holder_or_dev_risk_before_rows": sum(1 for row in positive if row["holder_risk_before_burst"] in {"MEDIUM", "HIGH", "UNSAFE"}),
        "vault_curve_progress_before_rows": sum(1 for row in positive if row["vault_curve_progress_before_burst"] in {"MEDIUM", "HIGH"}),
        "high_positive_but_dead_rows": sum(1 for row in positive if row["positive_outcome_label"] == "high_positive" and row["final_outcome"] == "early_rejected_dead"),
    }
    out_rows = [
        {
            "metric": key,
            "value": value,
        }
        for key, value in summary.items()
    ]
    write_csv(root / "early_burst_exit_window_analysis.csv", out_rows, ["metric", "value"])
    write_text(
        root / "EARLY_BURST_EXIT_WINDOW_ANALYSIS.md",
        "# Early-Burst Exit Window Analysis\n\n"
        + "\n".join(f"- {key}: `{value}`" for key, value in summary.items())
        + "\n\nNo trade entries, tuned thresholds, backtests, replay, or profitability claims were produced.\n",
    )
    return summary


def write_vs_dead_comparison(root: pathlib.Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    cohorts = {
        "early_burst_watch": [row for row in rows if row["early_burst_setup_decision"] == "early_burst_watch"],
        "positive_high": [row for row in rows if row["positive_outcome_label"] in {"positive", "high_positive"}],
        "ordinary_clean_dead": [row for row in rows if row["early_burst_class"] == "ORDINARY_CLEAN_DEAD"],
        "censored": [row for row in rows if row["early_burst_class"] == "CENSORED_OR_INCOMPLETE"],
    }
    fields = [
        "cohort",
        "rows",
        "unique_mints",
        "buy_sell_followthrough",
        "volume_followthrough",
        "holder_growth",
        "holder_concentration_risk",
        "dev_creator_holding_risk",
        "curve_progress",
        "liquidity_reserve_movement",
        "adverse_sell_pressure",
        "time_to_death_observed",
        "time_to_peak_observed",
        "high_throughput_degraded_status",
    ]
    out_rows: list[dict[str, Any]] = []
    for cohort, cohort_rows in cohorts.items():
        out_rows.append({
            "cohort": cohort,
            "rows": len(cohort_rows),
            "unique_mints": len({row["mint"] for row in cohort_rows}),
            "buy_sell_followthrough": common_bin(cohort_rows, "early_burst_reason_codes", "early_buy_sell_followthrough"),
            "volume_followthrough": common_bin(cohort_rows, "early_burst_reason_codes", "early_volume_followthrough"),
            "holder_growth": common_bin(cohort_rows, "early_burst_reason_codes", "early_holder_growth"),
            "holder_concentration_risk": common_row_bin(cohort_rows, "holder_risk_before_burst"),
            "dev_creator_holding_risk": common_bin(cohort_rows, "early_burst_reason_codes", "dev_or_creator_holding_risk"),
            "curve_progress": common_row_bin(cohort_rows, "vault_curve_progress_before_burst"),
            "liquidity_reserve_movement": "MISSING",
            "adverse_sell_pressure": common_row_bin(cohort_rows, "sell_pressure_before_burst"),
            "time_to_death_observed": "HIGH" if sum(1 for row in cohort_rows if row["time_to_rejection_ms"]) > len(cohort_rows) / 2 else "LOW",
            "time_to_peak_observed": "HIGH" if sum(1 for row in cohort_rows if row["time_to_max_favorable_ms"]) > len(cohort_rows) / 2 else "LOW",
            "high_throughput_degraded_status": "UNSAFE" if any("degraded" in row["early_burst_reason_codes"] for row in cohort_rows) else "LOW",
        })
    write_csv(root / "early_burst_vs_dead_comparison.csv", out_rows, fields)
    write_text(
        root / "EARLY_BURST_VS_DEAD_COMPARISON.md",
        "# Early-Burst vs Dead Comparison\n\n"
        + "\n".join(f"- `{row['cohort']}` rows `{row['rows']}` unique_mints `{row['unique_mints']}` curve `{row['curve_progress']}` sell_pressure `{row['adverse_sell_pressure']}`" for row in out_rows)
        + "\n\nDescriptive bins only: LOW, MEDIUM, HIGH, MISSING, UNSAFE, CENSORED. No thresholds were tuned.\n",
    )
    return {"cohorts": {row["cohort"]: row for row in out_rows}}


def common_bin(rows: list[dict[str, Any]], field: str, token: str) -> str:
    if not rows:
        return "MISSING"
    hits = sum(1 for row in rows if token in str(row.get(field, "")))
    ratio = hits / max(1, len(rows))
    if ratio >= 0.5:
        return "HIGH"
    if ratio > 0:
        return "MEDIUM"
    return "LOW"


def common_row_bin(rows: list[dict[str, Any]], field: str) -> str:
    if not rows:
        return "MISSING"
    counts = Counter(str(row.get(field, "MISSING")) or "MISSING" for row in rows)
    return counts.most_common(1)[0][0]


def write_pack(
    *,
    output_root: pathlib.Path,
    validation_root: pathlib.Path,
    architecture_root: pathlib.Path,
    rows: list[dict[str, Any]],
) -> pathlib.Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    pack = output_root / f"early_burst_validation_pack_{timestamp}"
    pack.mkdir(parents=True, exist_ok=True)
    include = [
        validation_root / "EARLY_BURST_VALIDATION_DATASET_SUMMARY.md",
        validation_root / "early_burst_validation_rows.csv",
        validation_root / "early_burst_exit_window_analysis.csv",
        validation_root / "EARLY_BURST_EXIT_WINDOW_ANALYSIS.md",
        validation_root / "early_burst_vs_dead_comparison.csv",
        validation_root / "EARLY_BURST_VS_DEAD_COMPARISON.md",
        output_root / "POSITIVE_HIGH_POSITIVE_MINT_REVIEW.md",
        output_root / "EARLY_BURST_STRATEGY_FAMILY_DIAGNOSTICS.md",
        output_root / "EARLY_BURST_SETUP_V0_REPORT.md",
        output_root / "EARLY_BURST_EXIT_RISK_DRAFT.md",
        architecture_root / "feature_store_report.md",
        architecture_root / "label_store_report.md",
        architecture_root / "candidate_eligibility_v2_report.md",
    ]
    for path in include:
        if path.exists() and safe_pack_path(path):
            shutil.copy2(path, pack / path.name)
    write_csv(pack / "top_20_early_burst_examples.csv", sorted([row for row in rows if row["early_burst_setup_decision"] == "early_burst_watch"], key=lambda r: num(r["max_favorable_proxy"]), reverse=True)[:20], VALIDATION_ROW_FIELDS)
    write_csv(pack / "all_high_positive_examples.csv", [row for row in rows if row["positive_outcome_label"] == "high_positive"], VALIDATION_ROW_FIELDS)
    write_csv(pack / "top_20_ordinary_dead_comparison_examples.csv", sorted([row for row in rows if row["early_burst_class"] == "ORDINARY_CLEAN_DEAD"], key=lambda r: num(r["time_to_rejection_ms"]) or 10**12)[:20], VALIDATION_ROW_FIELDS)
    write_text(
        pack / "README_FOR_GPT.md",
        "# Early-Burst Validation Pack\n\n"
        "This pack separates as-of decision features from forward outcome labels. It is research-only and contains no raw relay frames or secrets.\n",
    )
    write_text(
        pack / "GPT_EARLY_BURST_VALIDATION_PROMPT.md",
        "# GPT Early-Burst Validation Prompt\n\n"
        "Do not claim profitability. Do not tune thresholds. Do not run backtests. Do not output live trade entries. Focus on exit-window feasibility, risk invalidation, feature sufficiency, and evidence needed before replay/backtesting.\n",
    )
    checksums = []
    for path in sorted(p for p in pack.rglob("*") if p.is_file()):
        checksums.append(f"{file_sha256(path)}  {path.relative_to(pack)}")
    write_text(pack / "EXPORT_CHECKSUMS.txt", "\n".join(checksums) + "\n")
    zip_path = output_root / f"{pack.name}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(p for p in pack.rglob("*") if p.is_file()):
            if safe_pack_path(path):
                archive.write(path, path.relative_to(pack))
    return pack


def safe_pack_path(path: pathlib.Path) -> bool:
    name = path.name.lower()
    return not any(token in name for token in FORBIDDEN_PACK_NAME_TOKENS)


def update_readiness_and_reports(output_root: pathlib.Path, summary: dict[str, Any]) -> None:
    readiness_path = output_root / "READINESS_DECISION.json"
    readiness = read_json(readiness_path)
    readiness.update({
        "early_burst_validation_dataset_ready": True,
        "backtesting_ready": False,
        "replay_ready": False,
        "threshold_tuning_ready": False,
        "paper_trading_ready": False,
        "live_trading_ready": False,
        "wallet_execution_ready": False,
        "profitability_claim_allowed": False,
    })
    reasons = list(readiness.get("reason_codes", []))
    for reason in [
        "early_burst_validation_dataset_ready",
        "positive_outcomes_exist",
        "high_positive_outcomes_exist",
        "exit_window_needs_validation",
        "no_replay_eligible_candidates",
        "formal_backtest_not_allowed",
        "threshold_tuning_disabled",
        "operator_approval_missing",
    ]:
        if reason not in reasons:
            reasons.append(reason)
    readiness["reason_codes"] = reasons
    write_json(readiness_path, readiness)
    for report_name in [
        "TRADING_STRATEGY_PIPELINE_REPORT.md",
        "BACKTEST_HARNESS_REPORT.md",
        "REPLAY_HARNESS_REPORT.md",
        "PROFITABILITY_CLAIM_GATE_REPORT.md",
    ]:
        path = output_root / report_name
        if path.exists():
            text = path.read_text()
        else:
            text = f"# {report_name.removesuffix('.md').replace('_', ' ').title()}\n\n"
        if "early_burst_validation_dataset_ready" not in text:
            text += (
                "\n## Early-Burst Validation Dataset\n"
                f"- early_burst_validation_dataset_ready: `true`\n"
                f"- validation_rows: `{summary['rows']}`\n"
                "- replay/backtesting/threshold_tuning/paper/live/wallet/profitability_claims remain blocked.\n"
            )
            write_text(path, text)


def build_early_burst_validation_dataset(
    *,
    output_root: pathlib.Path = PIPELINE_ROOT,
    data_mart_root: pathlib.Path = DATA_MART_ROOT,
    architecture_root: pathlib.Path = STRATEGY_ARCHITECTURE_ROOT,
    validation_root: pathlib.Path | None = None,
) -> dict[str, Any]:
    validation_root = validation_root or (output_root / "early_burst_validation_dataset")
    validation_root.mkdir(parents=True, exist_ok=True)
    positive_rows = [
        row for row in read_csv(output_root / "positive_outcome_labels.csv")
        if row.get("mint") not in {"", "mint"} and include_for_validation(row)
    ]
    labels = load_labels(data_mart_root)
    features = load_features(data_mart_root)
    scores = load_score_index(output_root)
    reviews = load_review_index(output_root)
    raw_by_key = {validation_key_for(row): row for row in positive_rows}
    validation_rows: list[dict[str, Any]] = []
    for row in positive_rows:
        key = key_for(row)
        decision_horizon = int(row.get("decision_horizon_seconds", "0") or 0)
        validation_rows.append(build_validation_row(
            row,
            label_row=labels.get(key, {}),
            by_horizon=features.get(key, {}),
            score=scores.get((row.get("mint", ""), decision_horizon), {}),
            review=reviews.get(row.get("mint", ""), {}),
        ))
    write_csv(validation_root / "early_burst_validation_rows.csv", validation_rows, VALIDATION_ROW_FIELDS)
    write_csv(validation_root / "early_burst_validation_labels.csv", validation_rows, VALIDATION_LABEL_FIELDS)
    feature_counts = write_feature_files(validation_root, validation_rows, raw_by_key, features)
    summary = summarize_validation(validation_rows, feature_counts)
    exit_summary = write_exit_window_analysis(validation_root, validation_rows)
    comparison = write_vs_dead_comparison(validation_root, validation_rows)
    summary["exit_window_analysis"] = exit_summary
    summary["comparison"] = comparison
    pack = write_pack(output_root=output_root, validation_root=validation_root, architecture_root=architecture_root, rows=validation_rows)
    summary["validation_root"] = str(validation_root)
    summary["gpt_pack_path"] = str(pack)
    summary["gpt_pack_zip_path"] = str(output_root / f"{pack.name}.zip")
    write_json(validation_root / "early_burst_validation_manifest.json", summary)
    write_summary(validation_root, summary)
    update_readiness_and_reports(output_root, summary)
    return summary
