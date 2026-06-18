from __future__ import annotations

import pathlib
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from .io import read_csv, write_csv, write_json, write_text
from .schemas import DATA_MART_ROOT, HORIZONS, PIPELINE_ROOT, STRATEGY_ARCHITECTURE_ROOT, boolish


OUTCOME_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "decision_horizon_seconds",
    "forward_window_seconds",
    "horizon_reached",
    "forward_window_observed",
    "data_quality_exclusion",
    "final_outcome",
    "rejection_reason",
    "terminal_inconclusive_reason",
    "censored_label",
    "clean_negative_label",
    "clean_positive_candidate_label",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "positive_outcome_label",
    "positive_outcome_strength_bin",
    "positive_outcome_basis",
    "positive_outcome_reason_codes",
    "curve_progress_proxy_start",
    "curve_progress_proxy_end",
    "curve_progress_proxy_max",
    "liquidity_delta_forward",
    "reserve_delta_forward",
    "volume_delta_forward",
    "buy_sell_delta_forward",
    "holder_growth_forward",
    "holder_concentration_risk_forward",
    "max_adverse_proxy",
    "max_favorable_proxy",
    "outcome_known_at_end_only",
    "allowed_for_alpha_features",
]


def key_for(row: dict[str, str]) -> tuple[str, str, str]:
    return (row.get("mint", ""), row.get("slice_id", ""), row.get("segment_id", ""))


def num(row: dict[str, str] | None, field: str) -> float | None:
    if not row:
        return None
    raw = row.get(field, "")
    if raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def flag(row: dict[str, str] | None, field: str) -> bool:
    return boolish(row.get(field)) if row else False


def load_feature_rows(data_mart_root: pathlib.Path = DATA_MART_ROOT) -> dict[tuple[str, str, str], dict[int, dict[str, str]]]:
    by_key: dict[tuple[str, str, str], dict[int, dict[str, str]]] = {}
    for horizon in HORIZONS:
        for row in read_csv(data_mart_root / f"strategy_asof_features_{horizon:03d}s.csv"):
            by_key.setdefault(key_for(row), {})[horizon] = row
    return by_key


def strength_from_proxies(
    *,
    curve_end: float | None,
    curve_max: float | None,
    volume_forward: float,
    buy_sell_forward: float,
    holder_growth: float,
    liquidity_forward: float,
    reserve_forward: float,
) -> tuple[str, list[str], float]:
    reasons: list[str] = []
    max_favorable = max(
        [
            curve_max or 0,
            curve_end or 0,
            min(max(buy_sell_forward, 0) * 5, 50),
            25 if volume_forward > 0 else 0,
            25 if holder_growth >= 3 else 0,
            20 if liquidity_forward > 0 and reserve_forward > 0 else 0,
        ]
    )
    high = (
        (curve_end is not None and curve_end >= 75)
        or (curve_max is not None and curve_max >= 80)
        or (buy_sell_forward >= 10 and volume_forward > 0 and holder_growth >= 5)
        or (liquidity_forward > 0 and reserve_forward > 0 and curve_end is not None and curve_end >= 50)
    )
    positive = (
        high
        or (curve_end is not None and curve_end >= 35)
        or (curve_max is not None and curve_max >= 45)
        or (buy_sell_forward >= 3 and volume_forward > 0)
        or (liquidity_forward > 0 and reserve_forward > 0 and buy_sell_forward > 0)
        or (holder_growth >= 3 and buy_sell_forward > 0)
    )
    near = (
        not positive
        and (
            (curve_end is not None and curve_end >= 28)
            or buy_sell_forward >= 2
            or (volume_forward > 0 and holder_growth > 0)
        )
    )
    if curve_end is not None:
        reasons.append(f"curve_progress_end_{curve_end:.2f}")
    if curve_max is not None:
        reasons.append(f"curve_progress_max_{curve_max:.2f}")
    if buy_sell_forward:
        reasons.append(f"buy_sell_forward_{buy_sell_forward:.2f}")
    if volume_forward:
        reasons.append("volume_forward_positive")
    if holder_growth:
        reasons.append(f"holder_growth_forward_{holder_growth:.2f}")
    if liquidity_forward > 0 and reserve_forward > 0:
        reasons.append("liquidity_and_reserve_forward_positive")
    if high:
        return "HIGH", reasons + ["fixed_bin_high_positive"], max_favorable
    if positive:
        return "POSITIVE", reasons + ["fixed_bin_positive"], max_favorable
    if near:
        return "NEAR_POSITIVE", reasons + ["fixed_bin_near_positive"], max_favorable
    return "LOW", reasons + ["fixed_bin_weak_or_flat"], max_favorable


def label_for(
    label: dict[str, str],
    start: dict[str, str] | None,
    future_rows: list[dict[str, str]],
    *,
    decision_horizon: int,
) -> dict[str, Any]:
    end = future_rows[-1] if future_rows else start
    reached = flag(start, "horizon_reached")
    forward_observed = bool(future_rows) and reached
    quality_exclusion = any(
        flag(row, field)
        for row in [label, start, end]
        for field in ("data_quality_exclusion", "provider_gap_exposed", "relay_gap_exposed", "sequence_gap_exposed", "hash_mismatch_exposed", "receiver_backpressure_exposed")
    )
    curve_values = [num(row, "curve_progress_proxy_asof") for row in future_rows if num(row, "curve_progress_proxy_asof") is not None]
    curve_start = num(start, "curve_progress_proxy_asof")
    curve_end = curve_values[-1] if curve_values else num(end, "curve_progress_proxy_asof")
    curve_max = max(curve_values) if curve_values else curve_end

    liquidity_forward = (num(end, "liquidity_delta_asof") or 0) - (num(start, "liquidity_delta_asof") or 0)
    reserve_forward = (num(end, "reserve_delta_asof") or 0) - (num(start, "reserve_delta_asof") or 0)
    volume_forward = (num(end, "volume_delta_asof") or 0) - (num(start, "volume_delta_asof") or 0)
    buy_sell_forward = (num(end, "net_buy_sell_delta_asof") or 0) - (num(start, "net_buy_sell_delta_asof") or 0)
    holder_growth = (num(end, "unique_holder_accounts_seen_asof") or 0) - (num(start, "unique_holder_accounts_seen_asof") or 0)
    holder_conc = num(end, "top_holder_concentration_asof") or 0
    max_adverse = max(
        [
            abs(min(liquidity_forward, 0)),
            abs(min(reserve_forward, 0)),
            max(holder_conc - 0.85, 0) * 100,
            100 if flag(end, "degraded_audit_only_before_horizon") else 0,
        ]
    )
    strength, strength_reasons, max_favorable = strength_from_proxies(
        curve_end=curve_end,
        curve_max=curve_max,
        volume_forward=volume_forward,
        buy_sell_forward=buy_sell_forward,
        holder_growth=holder_growth,
        liquidity_forward=liquidity_forward,
        reserve_forward=reserve_forward,
    )
    final_outcome = label.get("final_outcome", "")
    censored = boolish(label.get("censored_label")) or final_outcome == "terminal_inconclusive"
    clean_negative = boolish(label.get("clean_negative_label"))

    reason_codes = list(strength_reasons)
    if quality_exclusion:
        outcome = "invalid_quality"
        reason_codes.append("quality_exclusion_or_gap_exposed")
    elif censored:
        outcome = "censored"
        reason_codes.append("terminal_or_countability_censored")
    elif not forward_observed and not clean_negative:
        outcome = "unknown_insufficient_data"
        reason_codes.append("forward_window_not_observed")
    elif strength == "HIGH":
        outcome = "high_positive"
        reason_codes.append("stream_proxy_high_positive")
    elif strength == "POSITIVE":
        outcome = "positive"
        reason_codes.append("stream_proxy_positive")
    elif clean_negative or "rejected" in final_outcome or final_outcome == "dead":
        outcome = "dead_negative"
        reason_codes.append("final_rejected_or_dead")
    else:
        outcome = "weak_or_flat"
        reason_codes.append("no_fixed_positive_proxy")

    observed_horizons = [int(row.get("horizon_seconds", "0") or 0) for row in future_rows]
    end_horizon = max(observed_horizons) if observed_horizons else decision_horizon
    return {
        "mint": label.get("mint", start.get("mint", "") if start else ""),
        "slice_id": label.get("slice_id", start.get("slice_id", "") if start else ""),
        "segment_id": label.get("segment_id", start.get("segment_id", "") if start else ""),
        "relay_session_id": label.get("relay_session_id", start.get("relay_session_id", "") if start else ""),
        "decision_horizon_seconds": decision_horizon,
        "forward_window_seconds": max(0, end_horizon - decision_horizon),
        "horizon_reached": reached,
        "forward_window_observed": forward_observed,
        "data_quality_exclusion": quality_exclusion,
        "final_outcome": final_outcome,
        "rejection_reason": label.get("rejection_reason", ""),
        "terminal_inconclusive_reason": label.get("terminal_inconclusive_reason", ""),
        "censored_label": censored,
        "clean_negative_label": clean_negative,
        "clean_positive_candidate_label": boolish(label.get("clean_positive_label")),
        "candidate_checkpoint_seen": boolish(label.get("candidate_checkpoint_seen")),
        "replay_eligible": boolish(label.get("replay_eligible")),
        "positive_outcome_label": outcome,
        "positive_outcome_strength_bin": strength,
        "positive_outcome_basis": "stream_authoritative_proxy_bins",
        "positive_outcome_reason_codes": "|".join(reason_codes),
        "curve_progress_proxy_start": curve_start,
        "curve_progress_proxy_end": curve_end,
        "curve_progress_proxy_max": curve_max,
        "liquidity_delta_forward": liquidity_forward,
        "reserve_delta_forward": reserve_forward,
        "volume_delta_forward": volume_forward,
        "buy_sell_delta_forward": buy_sell_forward,
        "holder_growth_forward": holder_growth,
        "holder_concentration_risk_forward": holder_conc,
        "max_adverse_proxy": max_adverse,
        "max_favorable_proxy": max_favorable,
        "outcome_known_at_end_only": True,
        "allowed_for_alpha_features": False,
    }


def build_positive_outcome_labels(
    *,
    data_mart_root: pathlib.Path = DATA_MART_ROOT,
    architecture_root: pathlib.Path = STRATEGY_ARCHITECTURE_ROOT,
    output_root: pathlib.Path = PIPELINE_ROOT,
) -> dict[str, Any]:
    labels = read_csv(data_mart_root / "strategy_labels.csv")
    label_by_key = {key_for(row): row for row in labels}
    features = load_feature_rows(data_mart_root)
    rows: list[dict[str, Any]] = []
    for key, label in label_by_key.items():
        by_horizon = features.get(key, {})
        for decision_horizon in HORIZONS:
            start = by_horizon.get(decision_horizon)
            future_rows = [
                by_horizon[h]
                for h in HORIZONS
                if h >= decision_horizon and h in by_horizon and flag(by_horizon[h], "horizon_reached")
            ]
            rows.append(label_for(label, start, future_rows, decision_horizon=decision_horizon))

    write_source_map(output_root, data_mart_root, architecture_root)
    write_label_schema(output_root)
    write_csv(output_root / "positive_outcome_labels.csv", rows, OUTCOME_FIELDS)
    summary = summarize(rows)
    write_json(output_root / "positive_outcome_labels.json", {"summary": summary, "rows": rows})
    write_audit(output_root, rows, summary)
    write_gate_comparison(output_root, rows, architecture_root)
    pack = maybe_write_high_positive_pack(output_root, rows)
    summary["high_positive_review_pack_path"] = str(pack) if pack else ""
    write_json(output_root / "positive_outcome_labels.json", {"summary": summary, "rows": rows})
    write_json(output_root / "positive_outcome_audit.json", summary | {"example_rows": [row for row in rows if row["positive_outcome_label"] in {"positive", "high_positive"}][:20]})
    return summary


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(row["positive_outcome_label"]) for row in rows)
    high = [row for row in rows if row["positive_outcome_label"] == "high_positive"]
    pos = [row for row in rows if row["positive_outcome_label"] in {"positive", "high_positive"}]
    positive_or_high_strength = [row for row in rows if row["positive_outcome_strength_bin"] in {"POSITIVE", "HIGH"}]
    return {
        "total_rows": len(rows),
        "unique_mints": len({row["mint"] for row in rows}),
        "forward_window_observed": sum(1 for row in rows if row["forward_window_observed"]),
        "counts": dict(counts),
        "positive_count": len([row for row in rows if row["positive_outcome_label"] == "positive"]),
        "high_positive_count": len(high),
        "positive_or_high_count": len(pos),
        "positive_or_high_strength_rows": len(positive_or_high_strength),
        "positive_or_high_not_candidates": sum(1 for row in pos if not row["candidate_checkpoint_seen"]),
        "positive_or_high_censored_or_invalid": sum(1 for row in positive_or_high_strength if row["positive_outcome_label"] in {"censored", "invalid_quality"}),
        "replay_eligible_positive_or_high": sum(1 for row in pos if row["replay_eligible"]),
    }


def write_source_map(output_root: pathlib.Path, data_mart_root: pathlib.Path, architecture_root: pathlib.Path) -> None:
    source_map = {
        "schema_version": "phase107i.positive_outcome_source_map.v1",
        "data_mart_root": str(data_mart_root),
        "architecture_root": str(architecture_root),
        "stream_authoritative_fields": {
            "trade_delta": ["trade_update_count_asof", "buy_count_delta_asof", "sell_count_delta_asof", "net_buy_sell_delta_asof", "volume_delta_asof"],
            "holder_state": ["unique_holder_accounts_seen_asof", "top_holder_concentration_asof", "holder_churn_proxy_asof", "holder_collapse_proxy_asof"],
            "vault_curve": ["curve_progress_proxy_asof", "liquidity_delta_asof", "reserve_delta_asof", "liquidity_exit_proxy_asof", "price_or_curve_move_proxy_asof"],
            "quality": ["provider_gap_exposed", "relay_gap_exposed", "sequence_gap_exposed", "hash_mismatch_exposed", "receiver_backpressure_exposed", "data_quality_exclusion"],
            "labels": ["final_outcome", "rejection_reason", "terminal_inconclusive_reason", "time_to_rejection_ms", "time_to_terminal_ms"],
        },
        "not_used": ["holder RPC", "canonical RPC mint supply", "R2 status as alpha", "artifact consistency as alpha"],
    }
    write_json(output_root / "positive_outcome_source_map.json", source_map)
    write_text(
        output_root / "positive_outcome_source_map.md",
        "# Positive Outcome Source Map\n\n"
        "- Outcome labels use stream-authoritative trade, holder-state, vault/curve, and quality fields from the strategy data mart.\n"
        "- Outcome labels are labels/evaluation targets only, never alpha feature columns.\n"
        "- Holder RPC is not used. RPC mint supply remains audit-only/non-canonical.\n"
        f"- data_mart_root: `{data_mart_root}`\n",
    )


def write_label_schema(output_root: pathlib.Path) -> None:
    schema = {
        "schema_version": "phase107i.positive_outcome_label_schema.v1",
        "labels": ["dead_negative", "weak_or_flat", "positive", "high_positive", "censored", "invalid_quality", "unknown_insufficient_data"],
        "strength_bins": ["LOW", "NEAR_POSITIVE", "POSITIVE", "HIGH"],
        "outcome_known_at_end_only": True,
        "allowed_for_alpha_features": False,
        "fixed_descriptive_bins": {
            "positive": ["curve_progress_proxy_end>=35", "curve_progress_proxy_max>=45", "buy_sell_delta_forward>=3 with volume", "liquidity/reserve positive with buy pressure", "holder growth with buy pressure"],
            "high_positive": ["curve_progress_proxy_end>=75", "curve_progress_proxy_max>=80", "strong buy/volume/holder growth", "liquidity/reserve positive with curve progress>=50"],
        },
    }
    write_json(output_root / "positive_outcome_label_schema.json", schema)
    write_text(
        output_root / "positive_outcome_label_schema.md",
        "# Positive Outcome Label Schema\n\n"
        "- Labels: `dead_negative`, `weak_or_flat`, `positive`, `high_positive`, `censored`, `invalid_quality`, `unknown_insufficient_data`.\n"
        "- Outcome labels may use post-decision-horizon data only as labels, never as alpha inputs.\n"
        "- Candidate/replay labels remain separate from positive/high-positive market outcome labels.\n"
        "- Thresholds are fixed descriptive bins, not tuned thresholds.\n",
    )


def write_audit(output_root: pathlib.Path, rows: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    counts = summary["counts"]
    positive_or_high = [row for row in rows if row["positive_outcome_label"] in {"positive", "high_positive"}]
    strength_censored = [row for row in rows if row["positive_outcome_strength_bin"] in {"POSITIVE", "HIGH"} and row["positive_outcome_label"] in {"censored", "invalid_quality"}]
    examples = positive_or_high[:20]
    if not examples:
        examples = [row for row in rows if row["positive_outcome_strength_bin"] == "NEAR_POSITIVE"][:20]
    write_json(output_root / "positive_outcome_audit.json", summary | {"example_rows": examples})
    lines = [
        "# Positive Outcome Audit",
        "",
        f"- total_rows: `{summary['total_rows']}`",
        f"- unique_mints: `{summary['unique_mints']}`",
        f"- forward_window_observed_rows: `{summary['forward_window_observed']}`",
        f"- dead_negative: `{counts.get('dead_negative', 0)}`",
        f"- weak_or_flat: `{counts.get('weak_or_flat', 0)}`",
        f"- positive: `{counts.get('positive', 0)}`",
        f"- high_positive: `{counts.get('high_positive', 0)}`",
        f"- censored: `{counts.get('censored', 0)}`",
        f"- invalid_quality: `{counts.get('invalid_quality', 0)}`",
        f"- unknown_insufficient_data: `{counts.get('unknown_insufficient_data', 0)}`",
        f"- positive_or_high_not_candidates: `{summary['positive_or_high_not_candidates']}`",
        f"- positive_strength_but_censored_or_invalid: `{len(strength_censored)}`",
        f"- replay_eligible_positive_or_high: `{summary['replay_eligible_positive_or_high']}`",
        "",
        "## Interpretation",
        "- `positive`/`high_positive` here means a stream-proxy market outcome label, not replay eligibility and not a buy signal.",
        "- Censored or invalid-quality rows can show positive-strength proxies, but they remain unsafe for clean replay/backtest labels.",
    ]
    if examples:
        lines.extend(["", "## Examples"])
        for row in examples:
            lines.append(f"- `{row['mint']}` label `{row['positive_outcome_label']}` strength `{row['positive_outcome_strength_bin']}` reasons `{row['positive_outcome_reason_codes']}`")
    write_text(output_root / "POSITIVE_OUTCOME_AUDIT.md", "\n".join(lines) + "\n")


def index_scores(path: pathlib.Path) -> dict[str, dict[str, str]]:
    return {row.get("mint", ""): row for row in read_csv(path)}


def write_gate_comparison(output_root: pathlib.Path, rows: list[dict[str, Any]], architecture_root: pathlib.Path) -> None:
    early = index_scores(architecture_root / "early_avoid_filter_v1_scores.csv")
    cont = index_scores(architecture_root / "continue_tracking_gate_v1_scores.csv")
    cand = index_scores(architecture_root / "candidate_eligibility_v2_scores.csv")
    buy = index_scores(architecture_root / "buy_setup_draft_v0_scores.csv")
    out_rows = []
    for row in rows:
        mint = str(row["mint"])
        c = cand.get(mint, {})
        positive = row["positive_outcome_label"] in {"positive", "high_positive"}
        positive_strength = row["positive_outcome_strength_bin"] in {"POSITIVE", "HIGH"}
        high = row["positive_outcome_label"] == "high_positive"
        out_rows.append({
            "mint": mint,
            "slice_id": row["slice_id"],
            "segment_id": row["segment_id"],
            "decision_horizon_seconds": row["decision_horizon_seconds"],
            "early_avoid_decision": early.get(mint, {}).get("decision", ""),
            "continue_tracking_decision": cont.get(mint, {}).get("decision", ""),
            "candidate_eligibility_v2_decision": c.get("decision", ""),
            "buy_setup_draft_family": buy.get(mint, {}).get("setup_decision", ""),
            "positive_outcome_label": row["positive_outcome_label"],
            "positive_outcome_strength_bin": row["positive_outcome_strength_bin"],
            "first_failed_candidate_gate": str(c.get("reason_codes", "")).split("|")[0] if c.get("reason_codes") else "",
            "was_positive_but_rejected": positive and c.get("decision") not in {"candidate_watch", "candidate_review"},
            "was_positive_but_censored": positive_strength and row["positive_outcome_label"] in {"censored", "invalid_quality"},
            "was_high_positive_but_not_candidate": high and c.get("decision") not in {"candidate_watch", "candidate_review"},
            "likely_reason": c.get("reason_codes", row["positive_outcome_reason_codes"]),
        })
    fields = [
        "mint",
        "slice_id",
        "segment_id",
        "decision_horizon_seconds",
        "early_avoid_decision",
        "continue_tracking_decision",
        "candidate_eligibility_v2_decision",
        "buy_setup_draft_family",
        "positive_outcome_label",
        "positive_outcome_strength_bin",
        "first_failed_candidate_gate",
        "was_positive_but_rejected",
        "was_positive_but_censored",
        "was_high_positive_but_not_candidate",
        "likely_reason",
    ]
    write_csv(output_root / "gate_vs_positive_outcomes.csv", out_rows, fields)
    positives = [row for row in out_rows if row["positive_outcome_label"] in {"positive", "high_positive"}]
    high_not_candidate = [row for row in out_rows if row["was_high_positive_but_not_candidate"]]
    write_text(
        output_root / "GATE_VS_POSITIVE_OUTCOMES.md",
        "# Gate vs Positive Outcomes\n\n"
        f"- rows_compared: `{len(out_rows)}`\n"
        f"- positive_or_high_rows: `{len(positives)}`\n"
        f"- high_positive_not_candidate_rows: `{len(high_not_candidate)}`\n"
        "- Candidate gates were not loosened and no thresholds were tuned.\n"
        "- These comparisons are diagnostics only, not buy signals.\n",
    )
    candidate_watch = [row for row in out_rows if row["candidate_eligibility_v2_decision"] == "candidate_watch"]
    near_miss = [row for row in out_rows if row["positive_outcome_strength_bin"] in {"NEAR_POSITIVE", "POSITIVE", "HIGH"} and row["candidate_eligibility_v2_decision"] != "candidate_watch"]
    write_csv(output_root / "candidate_watch_review.csv", candidate_watch, fields)
    write_csv(output_root / "near_miss_mints.csv", near_miss, fields)
    write_text(output_root / "candidate_watch_review.md", f"# Candidate Watch Review\n\n- rows: `{len(candidate_watch)}`\n- replay_was_run: `false`\n")
    write_text(output_root / "near_miss_mints.md", f"# Near Miss Mints\n\n- rows: `{len(near_miss)}`\n- thresholds_tuned: `false`\n")


def maybe_write_high_positive_pack(output_root: pathlib.Path, rows: list[dict[str, Any]]) -> pathlib.Path | None:
    high = [row for row in rows if row["positive_outcome_label"] == "high_positive"]
    if not high:
        return None
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    pack = output_root / f"high_positive_review_pack_{timestamp}"
    write_csv(pack / "high_positive_mints.csv", high, OUTCOME_FIELDS)
    write_json(pack / "review_decision.json", {
        "high_positive_count": len(high),
        "replay_was_run": False,
        "backtesting_was_run": False,
        "threshold_tuning_was_run": False,
        "trading_was_run": False,
        "operator_review_required": True,
    })
    write_text(pack / "README.md", "# High Positive Review Pack\n\nHigh-positive market outcome rows exist. Replay/backtesting/trading remain blocked until separate gates pass.\n")
    return pack
