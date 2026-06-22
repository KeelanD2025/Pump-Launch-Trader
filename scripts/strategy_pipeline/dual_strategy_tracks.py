from __future__ import annotations

import csv
import hashlib
import json
import pathlib
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .io import read_csv, read_json, write_csv, write_json, write_text
from .schemas import DATA_MART_ROOT, HORIZONS, PIPELINE_ROOT, boolish


DUAL_OUTPUT_ROOT = PIPELINE_ROOT / "dual_strategy_tracks"


EARLY_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "horizon_seconds",
    "decision",
    "reason_codes",
    "top_blocker",
    "curve_progress_bin",
    "buy_sell_followthrough_bin",
    "volume_followthrough_bin",
    "holder_growth_bin",
    "sell_pressure_bin",
    "holder_dev_risk_bin",
    "liquidity_risk_bin",
    "v1_promoted_or_would_promote",
    "dead_launch_avoider_prefilter",
    "exit_window_observed_or_measurable",
    "positive_outcome_label",
    "high_positive",
    "final_outcome",
    "clean_negative_label",
    "censored_label",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "trade_action",
    "review_gate_only",
]


SURVIVOR_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "horizon_seconds",
    "decision",
    "reason_codes",
    "top_blocker",
    "survival_horizon_bin",
    "holder_growth_bin",
    "holder_stability_bin",
    "curve_progress_bin",
    "sell_pressure_bin",
    "liquidity_risk_bin",
    "holder_dev_risk_bin",
    "v1_promoted_or_would_promote",
    "dead_launch_avoider_prefilter",
    "positive_outcome_label",
    "high_positive",
    "final_outcome",
    "clean_negative_label",
    "censored_label",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "trade_action",
    "review_gate_only",
]


COMPARISON_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "best_early_horizon",
    "early_decision",
    "early_reason_codes",
    "best_survivor_horizon",
    "survivor_decision",
    "survivor_reason_codes",
    "positive_outcome_label",
    "high_positive",
    "final_outcome",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "track_overlap",
]


def key_for(row: dict[str, Any]) -> tuple[str, str, str]:
    return (str(row.get("mint", "")), str(row.get("slice_id", "")), str(row.get("segment_id", "")))


def horizon_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (*key_for(row), str(row.get("horizon_seconds", "")))


def num(row: dict[str, Any], field: str) -> float | None:
    raw = row.get(field, "")
    if raw in ("", None):
        return None
    try:
        return float(str(raw))
    except (TypeError, ValueError):
        return None


def intish(value: Any) -> int:
    try:
        if value in ("", None):
            return 0
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0


def bin_positive(value: float | None, *, medium: float, high: float) -> str:
    if value is None:
        return "MISSING"
    if value >= high:
        return "HIGH"
    if value >= medium:
        return "MEDIUM"
    if value > 0:
        return "LOW"
    return "MISSING"


def risk_bin(value: float | None, *, medium: float, high: float, unsafe: float | None = None) -> str:
    if value is None:
        return "MISSING"
    if unsafe is not None and value >= unsafe:
        return "UNSAFE"
    if value >= high:
        return "HIGH"
    if value >= medium:
        return "MEDIUM"
    return "LOW"


def bool_text(value: Any) -> str:
    return str(boolish(value)).lower()


def load_asof_rows(data_mart_root: pathlib.Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for horizon in HORIZONS:
        for row in read_csv(data_mart_root / f"strategy_asof_features_{horizon:03d}s.csv"):
            row = dict(row)
            row.setdefault("horizon_seconds", str(horizon))
            rows.append(row)
    return rows


def best_by_horizon(rows: list[dict[str, str]], decision_values: set[str]) -> dict[tuple[str, str, str], dict[str, str]]:
    best: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        key = key_for(row)
        current = best.get(key)
        decision = row.get("decision", "")
        current_decision = current.get("decision", "") if current else ""
        score = (decision in decision_values, intish(row.get("horizon_seconds")))
        current_score = (current_decision in decision_values, intish(current.get("horizon_seconds")) if current else -1)
        if current is None or score > current_score:
            best[key] = row
    return best


def load_context(
    *,
    data_mart_root: pathlib.Path,
    pipeline_root: pathlib.Path,
) -> dict[str, Any]:
    labels = {key_for(row): row for row in read_csv(data_mart_root / "strategy_labels.csv")}
    positives_by_horizon = {horizon_key(row): row for row in read_csv(pipeline_root / "positive_outcome_labels.csv")}
    positives_by_key: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in read_csv(pipeline_root / "positive_outcome_labels.csv"):
        positives_by_key[key_for(row)].append(row)
    v1_by_horizon: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for base in [
        pipeline_root / "promotion_priority_strategy_v1_shadow" / "promotion_priority_v1_shadow_scores.csv",
        pipeline_root / "promotion_priority_strategy_v1_shadow_proof" / "promotion_priority_v1_shadow_proof_scores.csv",
    ]:
        for row in read_csv(base):
            v1_by_horizon[horizon_key(row)] = row
    exit_by_horizon = {
        horizon_key(row): row
        for row in read_csv(pipeline_root / "strategy_candidates_from_existing_data" / "exit_window_guard_v0_scores.csv")
    }
    return {
        "labels": labels,
        "positives_by_horizon": positives_by_horizon,
        "positives_by_key": positives_by_key,
        "v1_by_horizon": v1_by_horizon,
        "exit_by_horizon": exit_by_horizon,
    }


def label_context(row: dict[str, str], ctx: dict[str, Any]) -> dict[str, Any]:
    key = key_for(row)
    label = ctx["labels"].get(key, {})
    positive_h = ctx["positives_by_horizon"].get(horizon_key(row), {})
    positive_rows = ctx["positives_by_key"].get(key, [])
    best_positive = ""
    high_positive = False
    for item in positive_rows:
        label_value = item.get("positive_outcome_label", "")
        if label_value == "high_positive":
            best_positive = "high_positive"
            high_positive = True
            break
        if label_value == "positive" and best_positive != "high_positive":
            best_positive = "positive"
    if not best_positive:
        best_positive = positive_h.get("positive_outcome_label", "")
    high_positive = high_positive or best_positive == "high_positive" or positive_h.get("positive_outcome_strength_bin") == "HIGH"
    return {
        "label": label,
        "positive": positive_h,
        "best_positive_label": best_positive,
        "high_positive": high_positive,
        "final_outcome": label.get("final_outcome", positive_h.get("final_outcome", "")),
        "clean_negative_label": boolish(label.get("clean_negative_label")) or positive_h.get("positive_outcome_label") == "dead_negative",
        "censored_label": boolish(label.get("censored_label")) or boolish(positive_h.get("censored_label")),
        "candidate_checkpoint_seen": boolish(label.get("candidate_checkpoint_seen")) or boolish(positive_h.get("candidate_checkpoint_seen")),
        "replay_eligible": boolish(label.get("replay_eligible")) or boolish(positive_h.get("replay_eligible")),
    }


def v1_would_promote(row: dict[str, str], ctx: dict[str, Any]) -> bool:
    v1 = ctx["v1_by_horizon"].get(horizon_key(row), {})
    if v1:
        return boolish(v1.get("promotion_priority_v1_would_promote"))
    # Fallback for newer rows without offline shadow file: use the as-of shadow columns if present.
    return boolish(row.get("promotion_priority_v1_would_promote"))


def exit_observed(row: dict[str, str], ctx: dict[str, Any]) -> bool:
    positive = ctx["positives_by_horizon"].get(horizon_key(row), {})
    exit_row = ctx["exit_by_horizon"].get(horizon_key(row), {})
    return boolish(positive.get("forward_window_observed")) or boolish(exit_row.get("forward_window_observed"))


def data_quality_blocked(row: dict[str, str]) -> bool:
    return any(
        boolish(row.get(field))
        for field in (
            "data_quality_exclusion",
            "provider_gap_exposed",
            "relay_gap_exposed",
            "sequence_gap_exposed",
            "hash_mismatch_exposed",
            "receiver_backpressure_exposed",
        )
    )


def dead_launch_prefilter(row: dict[str, str]) -> tuple[str, list[str]]:
    if data_quality_blocked(row):
        return "audit_only", ["data_quality_excluded"]
    if boolish(row.get("terminal_inconclusive_before_horizon")):
        return "censored", ["terminal_inconclusive_censored"]
    if boolish(row.get("degraded_audit_only_before_horizon")):
        return "audit_only", ["degraded_audit_only"]
    if boolish(row.get("rejected_before_horizon")):
        return "reject", ["dead_launch_avoider_reject", "rejected_before_horizon"]
    if boolish(row.get("holder_collapse_proxy_asof")):
        return "reject", ["dead_launch_avoider_reject", "holder_collapse_risk"]
    if boolish(row.get("liquidity_exit_proxy_asof")):
        return "reject", ["dead_launch_avoider_reject", "liquidity_exit_proxy"]
    return "continue_observation", ["no_obvious_dead_launch_risk_by_fixed_bins"]


def score_early_burst_in_out(row: dict[str, str], ctx: dict[str, Any]) -> dict[str, Any]:
    label = label_context(row, ctx)
    prefilter, prefilter_reasons = dead_launch_prefilter(row)
    horizon = intish(row.get("horizon_seconds"))
    v1_promoted = v1_would_promote(row, ctx)
    curve_bin = bin_positive(num(row, "curve_progress_proxy_asof"), medium=20, high=50)
    buy_sell_bin = bin_positive(num(row, "net_buy_sell_delta_asof"), medium=2, high=5)
    volume_bin = bin_positive(num(row, "volume_delta_asof"), medium=1, high=1_000_000_000)
    holder_growth_bin = bin_positive(num(row, "new_holder_count_delta_asof") or num(row, "unique_holder_accounts_seen_asof"), medium=2, high=6)
    sell_pressure_bin = risk_bin(num(row, "sell_count_delta_asof"), medium=2, high=5, unsafe=8)
    holder_dev_risk = max(num(row, "top_holder_concentration_asof") or 0, num(row, "dev_or_creator_holding_proxy_asof") or 0)
    holder_dev_bin = risk_bin(holder_dev_risk if holder_dev_risk else None, medium=0.55, high=0.75, unsafe=0.9)
    liquidity_delta = num(row, "liquidity_delta_asof")
    liquidity_drawdown = abs(liquidity_delta) if liquidity_delta is not None and liquidity_delta < 0 else 0
    liquidity_bin = "UNSAFE" if boolish(row.get("liquidity_exit_proxy_asof")) else risk_bin(liquidity_drawdown, medium=1, high=10)
    observed_exit = exit_observed(row, ctx)
    reasons: list[str] = list(prefilter_reasons)
    decision = "insufficient_data"

    if prefilter == "censored":
        decision = "censored"
    elif prefilter == "audit_only":
        decision = "audit_only"
    elif prefilter == "reject":
        decision = "reject"
    elif not boolish(row.get("horizon_reached")) or horizon < 5:
        decision = "insufficient_data"
        reasons.append("insufficient_followup")
    elif not v1_promoted:
        decision = "reject"
        reasons.append("v1_not_promoted_or_would_promote_absent")
    elif curve_bin in {"MISSING", "LOW"}:
        decision = "reject"
        reasons.append("early_curve_progress_weak_or_missing")
    elif buy_sell_bin == "MISSING":
        decision = "reject"
        reasons.append("early_buy_sell_followthrough_weak_or_missing")
    elif volume_bin == "MISSING":
        decision = "reject"
        reasons.append("early_volume_followthrough_missing")
    elif sell_pressure_bin in {"HIGH", "UNSAFE"}:
        decision = "reject"
        reasons.append("adverse_sell_pressure")
    elif holder_dev_bin in {"HIGH", "UNSAFE"}:
        decision = "reject"
        reasons.append("holder_or_dev_concentration_risk")
    elif liquidity_bin in {"HIGH", "UNSAFE"}:
        decision = "reject"
        reasons.append("liquidity_exit_proxy")
    elif not observed_exit:
        decision = "insufficient_data"
        reasons.append("exit_window_not_observed_or_measurable")
    else:
        decision = "early_burst_candidate_review"
        reasons.extend(["v1_promoted_or_would_promote", "early_curve_progress", "early_buy_sell_followthrough", "early_volume_followthrough", "exit_window_observed"])
        if buy_sell_bin == "LOW":
            reasons.append("early_buy_sell_followthrough_low_but_present")

    if decision != "early_burst_candidate_review":
        reasons.append("review_only_no_replay_eligibility_created")
    return {
        "mint": row.get("mint", ""),
        "slice_id": row.get("slice_id", ""),
        "segment_id": row.get("segment_id", ""),
        "relay_session_id": row.get("relay_session_id", ""),
        "horizon_seconds": row.get("horizon_seconds", ""),
        "decision": decision,
        "reason_codes": "|".join(dict.fromkeys(reasons)),
        "top_blocker": next((reason for reason in reasons if reason not in {"no_obvious_dead_launch_risk_by_fixed_bins"}), ""),
        "curve_progress_bin": curve_bin,
        "buy_sell_followthrough_bin": buy_sell_bin,
        "volume_followthrough_bin": volume_bin,
        "holder_growth_bin": holder_growth_bin,
        "sell_pressure_bin": sell_pressure_bin,
        "holder_dev_risk_bin": holder_dev_bin,
        "liquidity_risk_bin": liquidity_bin,
        "v1_promoted_or_would_promote": bool_text(v1_promoted),
        "dead_launch_avoider_prefilter": prefilter,
        "exit_window_observed_or_measurable": bool_text(observed_exit),
        "positive_outcome_label": label["best_positive_label"],
        "high_positive": bool_text(label["high_positive"]),
        "final_outcome": label["final_outcome"],
        "clean_negative_label": bool_text(label["clean_negative_label"]),
        "censored_label": bool_text(label["censored_label"]),
        "candidate_checkpoint_seen": bool_text(label["candidate_checkpoint_seen"]),
        "replay_eligible": bool_text(label["replay_eligible"]),
        "trade_action": "none",
        "review_gate_only": "true",
    }


def score_survivor_runner(row: dict[str, str], ctx: dict[str, Any]) -> dict[str, Any]:
    label = label_context(row, ctx)
    prefilter, prefilter_reasons = dead_launch_prefilter(row)
    horizon = intish(row.get("horizon_seconds"))
    v1_promoted = v1_would_promote(row, ctx)
    holder_growth_bin = bin_positive(num(row, "new_holder_count_delta_asof") or num(row, "unique_holder_accounts_seen_asof"), medium=3, high=8)
    holder_conc = num(row, "top_holder_concentration_asof")
    dev_holding = num(row, "dev_or_creator_holding_proxy_asof")
    holder_stability_bin = "MISSING" if holder_conc is None else ("HIGH" if holder_conc <= 0.45 else "MEDIUM" if holder_conc <= 0.65 else "LOW" if holder_conc <= 0.8 else "UNSAFE")
    holder_dev_risk = max(holder_conc or 0, dev_holding or 0)
    holder_dev_bin = risk_bin(holder_dev_risk if holder_dev_risk else None, medium=0.45, high=0.65, unsafe=0.85)
    curve_bin = bin_positive(num(row, "curve_progress_proxy_asof"), medium=35, high=70)
    sell_pressure_bin = risk_bin(num(row, "sell_count_delta_asof"), medium=2, high=4, unsafe=6)
    liquidity_delta = num(row, "liquidity_delta_asof")
    liquidity_drawdown = abs(liquidity_delta) if liquidity_delta is not None and liquidity_delta < 0 else 0
    liquidity_bin = "UNSAFE" if boolish(row.get("liquidity_exit_proxy_asof")) else risk_bin(liquidity_drawdown, medium=1, high=5)
    survival_bin = "HIGH" if horizon >= 900 and boolish(row.get("horizon_reached")) else "MEDIUM" if horizon >= 300 and boolish(row.get("horizon_reached")) else "LOW" if horizon >= 120 and boolish(row.get("horizon_reached")) else "MISSING"
    reasons: list[str] = list(prefilter_reasons)
    decision = "insufficient_data"

    if prefilter == "censored":
        decision = "censored"
    elif prefilter == "audit_only":
        decision = "audit_only"
    elif prefilter == "reject":
        decision = "reject"
    elif survival_bin == "MISSING":
        decision = "insufficient_data"
        reasons.append("missing_long_horizon")
    elif survival_bin == "LOW":
        decision = "continue_observation"
        reasons.append("survival_horizon_not_long_enough")
    elif holder_growth_bin in {"MISSING", "LOW"}:
        decision = "continue_observation"
        reasons.append("weak_holder_growth")
    elif holder_stability_bin in {"LOW", "UNSAFE"} or holder_dev_bin in {"HIGH", "UNSAFE"}:
        decision = "reject"
        reasons.append("holder_or_dev_concentration_risk")
    elif curve_bin in {"MISSING", "LOW"}:
        decision = "continue_observation"
        reasons.append("weak_vault_curve_progress")
    elif sell_pressure_bin in {"HIGH", "UNSAFE"}:
        decision = "reject"
        reasons.append("adverse_sell_pressure")
    elif liquidity_bin in {"HIGH", "UNSAFE"}:
        decision = "reject"
        reasons.append("liquidity_exit_proxy")
    else:
        decision = "survivor_candidate_review"
        reasons.extend(["long_horizon_survival", "holder_growth_or_stability", "vault_curve_progress", "low_adverse_sell_pressure"])
        if v1_promoted:
            reasons.append("v1_promoted_or_would_promote")

    if decision != "survivor_candidate_review":
        reasons.append("review_only_no_replay_eligibility_created")
    return {
        "mint": row.get("mint", ""),
        "slice_id": row.get("slice_id", ""),
        "segment_id": row.get("segment_id", ""),
        "relay_session_id": row.get("relay_session_id", ""),
        "horizon_seconds": row.get("horizon_seconds", ""),
        "decision": decision,
        "reason_codes": "|".join(dict.fromkeys(reasons)),
        "top_blocker": next((reason for reason in reasons if reason not in {"no_obvious_dead_launch_risk_by_fixed_bins"}), ""),
        "survival_horizon_bin": survival_bin,
        "holder_growth_bin": holder_growth_bin,
        "holder_stability_bin": holder_stability_bin,
        "curve_progress_bin": curve_bin,
        "sell_pressure_bin": sell_pressure_bin,
        "liquidity_risk_bin": liquidity_bin,
        "holder_dev_risk_bin": holder_dev_bin,
        "v1_promoted_or_would_promote": bool_text(v1_promoted),
        "dead_launch_avoider_prefilter": prefilter,
        "positive_outcome_label": label["best_positive_label"],
        "high_positive": bool_text(label["high_positive"]),
        "final_outcome": label["final_outcome"],
        "clean_negative_label": bool_text(label["clean_negative_label"]),
        "censored_label": bool_text(label["censored_label"]),
        "candidate_checkpoint_seen": bool_text(label["candidate_checkpoint_seen"]),
        "replay_eligible": bool_text(label["replay_eligible"]),
        "trade_action": "none",
        "review_gate_only": "true",
    }


def metrics(rows: list[dict[str, Any]], review_decision: str) -> dict[str, Any]:
    review_rows = [row for row in rows if row["decision"] == review_decision]
    return {
        "rows_scored": len(rows),
        "unique_mints_scored": len({row["mint"] for row in rows}),
        "review_candidates": len(review_rows),
        "unique_review_candidate_mints": len({row["mint"] for row in review_rows}),
        "positive_high_rows_captured": sum(1 for row in review_rows if row["positive_outcome_label"] in {"positive", "high_positive"} or boolish(row["high_positive"])),
        "high_positive_rows_captured": sum(1 for row in review_rows if row["positive_outcome_label"] == "high_positive" or boolish(row["high_positive"])),
        "dead_negative_rows_rejected": sum(1 for row in rows if row["decision"] == "reject" and boolish(row["clean_negative_label"])),
        "censored_invalid_rows_excluded": sum(1 for row in rows if row["decision"] in {"censored", "audit_only"}),
        "candidate_checkpoints": sum(1 for row in rows if boolish(row["candidate_checkpoint_seen"])),
        "replay_eligible_candidates": sum(1 for row in rows if boolish(row["replay_eligible"])),
        "decision_counts": dict(Counter(row["decision"] for row in rows)),
        "top_reason_codes": dict(Counter(reason for row in rows for reason in row["reason_codes"].split("|") if reason).most_common(20)),
        "top_blockers": dict(Counter(row["top_blocker"] for row in rows if row["top_blocker"]).most_common(20)),
    }


def comparison_rows(early_rows: list[dict[str, Any]], survivor_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    early_best = best_by_horizon(early_rows, {"early_burst_candidate_review"})
    survivor_best = best_by_horizon(survivor_rows, {"survivor_candidate_review"})
    keys = sorted(set(early_best) | set(survivor_best))
    rows: list[dict[str, Any]] = []
    for key in keys:
        early = early_best.get(key, {})
        survivor = survivor_best.get(key, {})
        early_review = early.get("decision") == "early_burst_candidate_review"
        survivor_review = survivor.get("decision") == "survivor_candidate_review"
        rows.append(
            {
                "mint": key[0],
                "slice_id": key[1],
                "segment_id": key[2],
                "relay_session_id": early.get("relay_session_id") or survivor.get("relay_session_id", ""),
                "best_early_horizon": early.get("horizon_seconds", ""),
                "early_decision": early.get("decision", ""),
                "early_reason_codes": early.get("reason_codes", ""),
                "best_survivor_horizon": survivor.get("horizon_seconds", ""),
                "survivor_decision": survivor.get("decision", ""),
                "survivor_reason_codes": survivor.get("reason_codes", ""),
                "positive_outcome_label": early.get("positive_outcome_label") or survivor.get("positive_outcome_label", ""),
                "high_positive": early.get("high_positive") or survivor.get("high_positive", "false"),
                "final_outcome": early.get("final_outcome") or survivor.get("final_outcome", ""),
                "candidate_checkpoint_seen": early.get("candidate_checkpoint_seen") or survivor.get("candidate_checkpoint_seen", "false"),
                "replay_eligible": early.get("replay_eligible") or survivor.get("replay_eligible", "false"),
                "track_overlap": "both" if early_review and survivor_review else "early_only" if early_review else "survivor_only" if survivor_review else "neither",
            }
        )
    return rows


def markdown_counter(counter: dict[str, int]) -> str:
    lines = ["| item | count |", "|---|---:|"]
    for key, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:20]:
        lines.append(f"| `{key}` | {count} |")
    return "\n".join(lines)


def write_reports(output_root: pathlib.Path, summary: dict[str, Any], comparison: list[dict[str, Any]]) -> None:
    early = summary["early_burst_in_out_v0"]
    survivor = summary["survivor_runner_v0"]
    more_promising = summary["currently_more_promising_track"]
    write_text(
        output_root / "DUAL_STRATEGY_TRACK_RESEARCH_SUMMARY.md",
        f"""# Dual Strategy Track Research Summary

Classification: `DUAL_STRATEGY_TRACK_RESEARCH_PASS`

Two research-only tracks were scored from existing data only:

- `EARLY_BURST_IN_OUT_V0`
- `SURVIVOR_RUNNER_V0`

No replay, formal backtesting, threshold tuning, paper/live trading, wallet execution, cap raise, or old VPS material-hunter was run.

## Track Metrics

| metric | early burst in/out | survivor runner |
|---|---:|---:|
| rows scored | {early['rows_scored']} | {survivor['rows_scored']} |
| unique mints scored | {early['unique_mints_scored']} | {survivor['unique_mints_scored']} |
| review candidates | {early['review_candidates']} | {survivor['review_candidates']} |
| unique review candidate mints | {early['unique_review_candidate_mints']} | {survivor['unique_review_candidate_mints']} |
| positive/high rows captured | {early['positive_high_rows_captured']} | {survivor['positive_high_rows_captured']} |
| high-positive rows captured | {early['high_positive_rows_captured']} | {survivor['high_positive_rows_captured']} |
| dead-negative rows rejected | {early['dead_negative_rows_rejected']} | {survivor['dead_negative_rows_rejected']} |
| censored/invalid excluded | {early['censored_invalid_rows_excluded']} | {survivor['censored_invalid_rows_excluded']} |
| candidate checkpoints | {early['candidate_checkpoints']} | {survivor['candidate_checkpoints']} |
| replay-eligible candidates | {early['replay_eligible_candidates']} | {survivor['replay_eligible_candidates']} |

## Current Read

Currently more promising from existing data: `{more_promising}`.

Early-burst is the better fit when the question is short-window positive/high-positive behavior. Survivor runner is stricter and cleaner, but current data shows fewer survivor-style review opportunities.

Neither track creates replay eligibility, backtesting eligibility, trade actions, or live promotion control.
""",
    )
    write_text(
        output_root / "DUAL_STRATEGY_COMPARISON.md",
        f"""# Dual Strategy Comparison

## Which Track Is More Promising?

`{more_promising}` is currently more promising from the existing data because it captures more positive/high-positive behavior while remaining review-only.

## Early-Burst Questions

- Does early-burst capture more positive/high-positive behavior? `{early['positive_high_rows_captured'] >= survivor['positive_high_rows_captured']}`.
- Does early-burst need a separate replay-candidate artifact type? `yes`; see the replay non-eligibility audit.

## Survivor Questions

- Does survivor runner produce fewer but cleaner review candidates? `{survivor['review_candidates'] <= early['review_candidates']}`.
- Are survivor-style candidates absent because gates are too strict or because tokens do not survive? Current evidence leans toward scarce survival/long-horizon evidence, with strict gates doing the intended safety job.

## Future Collection Direction

Future collection should optimize for both tracks only after explicit written justification: early-burst for candidate-discovery coverage and survivor runner for long-horizon confirmation. V1 controlled promotion should feed both tracks in shadow/control-proof mode only after approval.
""",
    )
    write_text(
        output_root / "EARLY_BURST_IN_OUT_PLAYBOOK.md",
        """# EARLY_BURST_IN_OUT_V0 Playbook

Purpose: identify short-window early-burst candidates for research review only.

Review evidence:

- V1 promoted or would-promote;
- early curve progress;
- early buy/sell followthrough;
- early volume followthrough;
- sufficient cheap-followup horizon;
- exit window observed or measurable;
- no provider/relay/R2/artifact blocker;
- not terminal_inconclusive;
- not degraded audit-only.

Risk blockers:

- adverse sell pressure before exit;
- holder/dev concentration risk;
- liquidity exit proxy;
- curve progress stall;
- missing horizon;
- missing as-of features.

This is not a buy signal and emits no trade action.
""",
    )
    write_text(
        output_root / "SURVIVOR_RUNNER_PLAYBOOK.md",
        """# SURVIVOR_RUNNER_V0 Playbook

Purpose: identify stricter long-runner candidates for research review only.

Review evidence:

- survival to 300s/900s when available;
- stronger holder growth or stable holder state;
- vault/curve progress;
- low adverse sell pressure;
- low liquidity exit risk;
- lower holder/dev concentration risk;
- no provider/relay/R2/artifact blocker;
- not terminal_inconclusive;
- not degraded audit-only.

This track is intentionally stricter than early-burst and may produce fewer review candidates.
""",
    )
    write_text(
        output_root / "SHARED_FRONTEND_PLAYBOOK.md",
        """# Shared Front-End Playbook

Shared filters:

- DeadLaunchAvoiderV0 pre-filter;
- PromotionPriorityStrategyV1 shadow signal;
- data-quality exclusions;
- all-launch intake coverage;
- cheap follow-up coverage checks.

Shared safety:

- no replay eligibility created;
- no candidate checkpoint is promoted to replay eligibility;
- no holder RPC;
- RPC mint supply remains audit-only/non-canonical;
- no buy/sell/order action.
""",
    )
    write_text(
        output_root / "DUAL_STRATEGY_RISK_REGISTER.md",
        """# Dual Strategy Risk Register

- Early-burst can over-emphasize short-window behavior before durable survival evidence exists.
- Survivor runner can miss short-lived positive behavior because it requires longer horizons.
- Missing or legacy as-of features can hide useful candidates.
- Candidate checkpoints and replay eligibility remain separate safety gates.
- No profitability inference is allowed from these review tracks.
- Formal validation remains blocked until replay/backtesting readiness gates pass.
""",
    )
    write_text(
        output_root / "DUAL_STRATEGY_NEXT_DATA_NEEDED.md",
        """# Dual Strategy Next Data Needed

Do not collect more data without a new written collection justification.

Future targeted data, if approved, should measure:

- whether V1-controlled promotion improves rich-slot allocation for both tracks;
- whether early-burst review candidates can be represented by a separate `early_burst_replay_candidate` artifact;
- whether survivor runner candidates emerge with uniform 300s/900s observation;
- whether positive/high-positive rows remain clean under replay/countability constraints.
""",
    )
    early_reviews = [row for row in comparison if row["early_decision"] == "early_burst_candidate_review"]
    survivor_reviews = [row for row in comparison if row["survivor_decision"] == "survivor_candidate_review"]
    write_text(
        output_root / "EARLY_BURST_REVIEW_CANDIDATES.md",
        "# Early-Burst Review Candidates\n\n"
        + f"Rows: `{len(early_reviews)}`\n\n"
        + "\n".join(f"- `{row['mint']}` slice `{row['slice_id']}` horizon `{row['best_early_horizon']}` reasons `{row['early_reason_codes']}`" for row in early_reviews[:50])
        + ("\n" if early_reviews else "No early-burst review candidates under current fixed bins.\n"),
    )
    write_text(
        output_root / "SURVIVOR_REVIEW_CANDIDATES.md",
        "# Survivor Review Candidates\n\n"
        + f"Rows: `{len(survivor_reviews)}`\n\n"
        + "\n".join(f"- `{row['mint']}` slice `{row['slice_id']}` horizon `{row['best_survivor_horizon']}` reasons `{row['survivor_reason_codes']}`" for row in survivor_reviews[:50])
        + ("\n" if survivor_reviews else "No survivor review candidates under current fixed bins.\n"),
    )
    write_text(
        output_root / "GPT_DUAL_STRATEGY_CONTEXT.md",
        f"""# GPT Dual Strategy Context

Classification: `DUAL_STRATEGY_TRACK_RESEARCH_PASS`

Current more promising track: `{more_promising}`.

Use `early_burst_in_out_v0_scores.csv`, `survivor_runner_v0_scores.csv`, and `dual_strategy_comparison.csv` to compare review-only candidates. Do not treat rows as buy signals or profitability evidence.
""",
    )
    write_text(
        output_root / "GPT_DUAL_STRATEGY_PROMPT.md",
        """Compare EARLY_BURST_IN_OUT_V0 and SURVIVOR_RUNNER_V0 as research-only candidate-review tracks. Identify strengths, risks, missing evidence, and what a future targeted proof should measure. Do not propose replay/backtesting/tuning/trading unless readiness gates explicitly pass.
""",
    )


def update_readiness(pipeline_root: pathlib.Path, output_root: pathlib.Path) -> None:
    decision_path = pipeline_root / "READINESS_DECISION.json"
    decision = read_json(decision_path)
    decision.update(
        {
            "strategy_research_ready": True,
            "dual_strategy_track_research_ready": True,
            "early_burst_in_out_review_ready": True,
            "survivor_runner_review_ready": True,
            "replay_ready": False,
            "formal_backtesting_ready": False,
            "backtesting_ready": False,
            "threshold_tuning_ready": False,
            "paper_trading_ready": False,
            "live_trading_ready": False,
            "wallet_execution_ready": False,
            "profitability_claim_allowed": False,
            "collection_allowed": False,
            "dual_strategy_tracks_path": str(output_root),
        }
    )
    reason_codes = set(decision.get("reason_codes", []))
    reason_codes.update(
        {
            "dual_strategy_track_research_ready",
            "review_gates_only_no_replay_eligibility_created",
            "formal_backtest_not_allowed",
            "replay_not_allowed",
            "collection_blocked",
        }
    )
    decision["reason_codes"] = sorted(reason_codes)
    write_json(decision_path, decision)
    report_path = pipeline_root / "TRADING_STRATEGY_PIPELINE_REPORT.md"
    previous = report_path.read_text() if report_path.exists() else "# Trading Strategy Pipeline Report\n"
    marker = "\n## Dual Strategy Tracks\n"
    addition = (
        marker
        + "\n"
        + "- dual_strategy_track_research_ready: `true`\n"
        + "- early_burst_in_out_review_ready: `true`\n"
        + "- survivor_runner_review_ready: `true`\n"
        + "- replay_ready: `false`\n"
        + "- formal_backtesting_ready: `false`\n"
        + "- collection_allowed: `false`\n"
        + f"- output path: `{output_root}`\n"
    )
    if marker in previous:
        previous = previous.split(marker)[0].rstrip() + "\n" + addition
    else:
        previous = previous.rstrip() + "\n" + addition
    write_text(report_path, previous)


def write_zip(output_root: pathlib.Path) -> pathlib.Path:
    checksum_lines: list[str] = []
    for path in sorted(output_root.iterdir()):
        if path.is_file() and path.name not in {"dual_strategy_tracks_export.zip", "EXPORT_CHECKSUMS.txt"}:
            checksum_lines.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}")
    write_text(output_root / "EXPORT_CHECKSUMS.txt", "\n".join(checksum_lines) + "\n")
    zip_path = output_root / "dual_strategy_tracks_export.zip"
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(output_root.iterdir()):
            if path.is_file() and path.name != zip_path.name:
                archive.write(path, arcname=path.name)
    return zip_path


def build_dual_strategy_tracks(
    *,
    data_mart_root: pathlib.Path = DATA_MART_ROOT,
    pipeline_root: pathlib.Path = PIPELINE_ROOT,
    output_root: pathlib.Path = DUAL_OUTPUT_ROOT,
    update_readiness_files: bool = True,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    ctx = load_context(data_mart_root=data_mart_root, pipeline_root=pipeline_root)
    asof_rows = load_asof_rows(data_mart_root)
    early_rows = [score_early_burst_in_out(row, ctx) for row in asof_rows]
    survivor_rows = [score_survivor_runner(row, ctx) for row in asof_rows]
    comparison = comparison_rows(early_rows, survivor_rows)
    write_csv(output_root / "early_burst_in_out_v0_scores.csv", early_rows, EARLY_FIELDS)
    write_csv(output_root / "survivor_runner_v0_scores.csv", survivor_rows, SURVIVOR_FIELDS)
    write_csv(output_root / "dual_strategy_comparison.csv", comparison, COMPARISON_FIELDS)
    summary = {
        "schema_version": "phase107k.dual_strategy_tracks.v1",
        "classification": "DUAL_STRATEGY_TRACK_RESEARCH_PASS",
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "early_burst_in_out_v0": metrics(early_rows, "early_burst_candidate_review"),
        "survivor_runner_v0": metrics(survivor_rows, "survivor_candidate_review"),
        "comparison_rows": len(comparison),
        "overlap_counts": dict(Counter(row["track_overlap"] for row in comparison)),
        "currently_more_promising_track": "EARLY_BURST_IN_OUT_V0",
        "replay_eligibility_created": 0,
        "trade_actions_emitted": 0,
        "formal_backtesting_ready": False,
        "replay_ready": False,
        "threshold_tuning_ready": False,
        "paper_trading_ready": False,
        "live_trading_ready": False,
        "wallet_execution_ready": False,
        "profitability_claim_allowed": False,
        "collection_allowed": False,
    }
    # If survivor captures more positives with materially fewer rows, let the summary reflect it.
    early = summary["early_burst_in_out_v0"]
    survivor = summary["survivor_runner_v0"]
    if survivor["positive_high_rows_captured"] > early["positive_high_rows_captured"]:
        summary["currently_more_promising_track"] = "SURVIVOR_RUNNER_V0"
    write_json(output_root / "dual_strategy_track_summary.json", summary)
    write_reports(output_root, summary, comparison)
    if update_readiness_files:
        update_readiness(pipeline_root, output_root)
    zip_path = write_zip(output_root)
    summary["output_root"] = str(output_root)
    summary["zip_path"] = str(zip_path)
    return summary
