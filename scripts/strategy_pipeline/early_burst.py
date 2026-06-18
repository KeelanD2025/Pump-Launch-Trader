from __future__ import annotations

import pathlib
import shutil
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from .io import read_csv, write_csv, write_json, write_text
from .schemas import DATA_MART_ROOT, PIPELINE_ROOT, STRATEGY_ARCHITECTURE_ROOT, boolish


EARLY_BURST_SCORE_FIELDS = [
    "strategy_name",
    "strategy_version",
    "mint",
    "horizon_seconds",
    "decision",
    "confidence_bin",
    "reason_codes",
    "allowed_actions",
    "blocked_actions",
    "trade_action",
    "uses_final_outcome_inputs",
    "explanation",
]

MINT_REVIEW_FIELDS = [
    "mint",
    "best_outcome_label",
    "best_strength_bin",
    "positive_rows",
    "high_positive_rows",
    "first_positive_horizon",
    "last_positive_horizon",
    "max_curve_progress_end",
    "max_curve_progress_proxy",
    "max_buy_sell_delta",
    "max_holder_growth",
    "final_outcome",
    "rejection_reason",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "candidate_eligibility_decision",
    "first_failed_candidate_gate",
    "candidate_reason_codes",
    "data_quality_status",
    "censored_status",
    "why_not_candidate",
    "early_burst_research_class",
]


def num(value: Any) -> float:
    try:
        if value is None or str(value).strip() == "":
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def score_feature_row(row: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    blocked = "replay|backtesting|threshold_tuning|paper_trading|live_trading|wallet_execution"
    if boolish(row.get("provider_gap_exposed")) or boolish(row.get("relay_gap_exposed")) or boolish(row.get("data_quality_exclusion")):
        return score_row(row, "audit_only", "UNSAFE", ["data_quality_excluded"], blocked)
    if boolish(row.get("terminal_inconclusive_before_horizon")):
        return score_row(row, "censored", "CENSORED", ["terminal_inconclusive"], blocked)
    if not boolish(row.get("horizon_reached")):
        return score_row(row, "insufficient_data", "MISSING", ["insufficient_horizon"], blocked)
    curve = num(row.get("curve_progress_proxy_asof"))
    net = num(row.get("net_buy_sell_delta_asof"))
    volume = num(row.get("volume_delta_asof"))
    holder_growth = num(row.get("new_holder_count_delta_asof")) or num(row.get("unique_holder_accounts_seen_asof"))
    sells = num(row.get("sell_count_delta_asof"))
    buys = num(row.get("buy_count_delta_asof"))
    concentration = num(row.get("top_holder_concentration_asof"))
    dev_holding = num(row.get("dev_or_creator_holding_proxy_asof"))
    if curve >= 35:
        reasons.append("early_curve_progress")
    if net >= 3 or buys >= 3:
        reasons.append("early_buy_sell_followthrough")
    if volume > 0:
        reasons.append("early_volume_followthrough")
    if holder_growth >= 3:
        reasons.append("early_holder_growth")
    if sells > buys or net < 0:
        reasons.append("adverse_sell_pressure")
    if concentration >= 0.85:
        reasons.append("holder_concentration_risk")
    if dev_holding != 0:
        reasons.append("dev_or_creator_holding_risk")
    if boolish(row.get("rejected_before_horizon")):
        reasons.append("death_after_burst")
        reasons.append("weak_exit_window")
    if reasons and any(reason.startswith("early_") for reason in reasons):
        decision = "early_burst_watch"
        confidence = "LOW"
    elif "adverse_sell_pressure" in reasons or "holder_concentration_risk" in reasons:
        decision = "reject"
        confidence = "UNSAFE"
    else:
        decision = "insufficient_data"
        confidence = "MISSING"
        reasons.append("insufficient_horizon")
    reasons.append("replay_not_allowed")
    return score_row(row, decision, confidence, sorted(set(reasons)), blocked)


def score_row(row: dict[str, Any], decision: str, confidence: str, reasons: list[str], blocked: str) -> dict[str, Any]:
    return {
        "strategy_name": "early_burst_setup_v0",
        "strategy_version": "v0",
        "mint": row.get("mint", ""),
        "horizon_seconds": row.get("horizon_seconds", ""),
        "decision": decision,
        "confidence_bin": confidence,
        "reason_codes": "|".join(reasons),
        "allowed_actions": "research_report",
        "blocked_actions": blocked,
        "trade_action": "none",
        "uses_final_outcome_inputs": "false",
        "explanation": "Research-only early burst setup diagnostic. Not a buy signal and not tradeable.",
    }


def index_by_mint(path: pathlib.Path) -> dict[str, dict[str, str]]:
    return {row.get("mint", ""): row for row in read_csv(path)}


def classify_mint(best: dict[str, Any], cand: dict[str, str]) -> tuple[str, str]:
    if best["data_quality_status"] != "clean":
        return "POSITIVE_BUT_DATA_QUALITY_UNSAFE", "quality_exclusion_or_gap"
    if best["censored_status"] == "censored":
        return "POSITIVE_BUT_DATA_QUALITY_UNSAFE", "censored_or_terminal_inconclusive"
    if best["replay_eligible"] != "true":
        reason = "replay_not_countability_allowed"
    else:
        reason = ""
    candidate_decision = best["candidate_eligibility_decision"]
    reason_codes = best["candidate_reason_codes"]
    final = best["final_outcome"]
    rejection = best["rejection_reason"]
    if best["best_outcome_label"] == "high_positive" and final == "early_rejected_dead":
        return "HIGH_POSITIVE_THEN_DEAD", rejection or reason
    if final == "early_rejected_dead":
        return "EARLY_BURST_THEN_DEAD", rejection or reason
    if "holder" in reason_codes or "dev_or_creator" in reason_codes:
        return "POSITIVE_BUT_UNSAFE_HOLDER_OR_DEV", reason_codes
    if reason:
        return "POSITIVE_BUT_NO_REPLAY_PERMISSION", reason
    if candidate_decision not in {"candidate_watch", "candidate_review"}:
        return "POSITIVE_BUT_CANDIDATE_GATE_MISMATCH", reason_codes or "candidate_gate_not_watch_or_review"
    if num(best["max_favorable_proxy"]) > 0 and best["last_positive_horizon"] in {"5", "10"}:
        return "POSITIVE_BUT_INSUFFICIENT_EXIT_WINDOW", "positive_only_short_window"
    return "NEEDS_MANUAL_REVIEW", reason_codes or "manual_review_required"


def build_positive_high_review(output_root: pathlib.Path, architecture_root: pathlib.Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = [row for row in read_csv(output_root / "positive_outcome_labels.csv") if row.get("positive_outcome_label") in {"positive", "high_positive"}]
    by_mint: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_mint[row.get("mint", "")].append(row)
    cand = index_by_mint(architecture_root / "candidate_eligibility_v2_scores.csv")
    review_rows: list[dict[str, Any]] = []
    for mint, mint_rows in sorted(by_mint.items()):
        high_rows = [row for row in mint_rows if row["positive_outcome_label"] == "high_positive"]
        best_label = "high_positive" if high_rows else "positive"
        best_strength = "HIGH" if high_rows else "POSITIVE"
        horizons = sorted(int(row.get("decision_horizon_seconds", "0") or 0) for row in mint_rows)
        c = cand.get(mint, {})
        reason_codes = c.get("reason_codes", "")
        first_failed = reason_codes.split("|")[0] if reason_codes else ""
        best = {
            "mint": mint,
            "best_outcome_label": best_label,
            "best_strength_bin": best_strength,
            "positive_rows": sum(1 for row in mint_rows if row["positive_outcome_label"] == "positive"),
            "high_positive_rows": len(high_rows),
            "first_positive_horizon": min(horizons) if horizons else "",
            "last_positive_horizon": max(horizons) if horizons else "",
            "max_curve_progress_end": max(num(row.get("curve_progress_proxy_end")) for row in mint_rows),
            "max_curve_progress_proxy": max(num(row.get("curve_progress_proxy_max")) for row in mint_rows),
            "max_buy_sell_delta": max(num(row.get("buy_sell_delta_forward")) for row in mint_rows),
            "max_holder_growth": max(num(row.get("holder_growth_forward")) for row in mint_rows),
            "max_favorable_proxy": max(num(row.get("max_favorable_proxy")) for row in mint_rows),
            "final_outcome": mint_rows[0].get("final_outcome", ""),
            "rejection_reason": mint_rows[0].get("rejection_reason", ""),
            "candidate_checkpoint_seen": str(any(boolish(row.get("candidate_checkpoint_seen")) for row in mint_rows)).lower(),
            "replay_eligible": str(any(boolish(row.get("replay_eligible")) for row in mint_rows)).lower(),
            "candidate_eligibility_decision": c.get("decision", ""),
            "first_failed_candidate_gate": first_failed,
            "candidate_reason_codes": reason_codes,
            "data_quality_status": "unsafe" if any(boolish(row.get("data_quality_exclusion")) for row in mint_rows) else "clean",
            "censored_status": "censored" if any(boolish(row.get("censored_label")) for row in mint_rows) else "not_censored",
        }
        cls, why = classify_mint(best, c)
        best["why_not_candidate"] = why
        best["early_burst_research_class"] = cls
        review_rows.append(best)
    write_csv(output_root / "positive_high_positive_mint_review.csv", review_rows, MINT_REVIEW_FIELDS)
    counts = Counter(row["early_burst_research_class"] for row in review_rows)
    write_json(output_root / "positive_high_positive_mint_review.json", {"counts": dict(counts), "rows": review_rows})
    write_text(
        output_root / "POSITIVE_HIGH_POSITIVE_MINT_REVIEW.md",
        "# Positive/High-Positive Mint Review\n\n"
        f"- unique_positive_or_high_mints: `{len(review_rows)}`\n"
        f"- unique_high_positive_mints: `{sum(1 for row in review_rows if row['best_outcome_label'] == 'high_positive')}`\n"
        f"- classes: `{dict(counts)}`\n"
        "- These are stream-proxy market outcome labels, not buy signals and not replay permission.\n"
        "- Candidate gates were not loosened.\n",
    )
    return review_rows, {"class_counts": dict(counts), "unique_mints": len(review_rows)}


def build_family_diagnostics(output_root: pathlib.Path, architecture_root: pathlib.Path, review_rows: list[dict[str, Any]]) -> dict[str, Any]:
    early = index_by_mint(architecture_root / "early_avoid_filter_v1_scores.csv")
    cont = index_by_mint(architecture_root / "continue_tracking_gate_v1_scores.csv")
    cand = index_by_mint(architecture_root / "candidate_eligibility_v2_scores.csv")
    buy = index_by_mint(architecture_root / "buy_setup_draft_v0_scores.csv")
    rows = []
    for row in review_rows:
        mint = row["mint"]
        e = early.get(mint, {})
        ct = cont.get(mint, {})
        c = cand.get(mint, {})
        b = buy.get(mint, {})
        rows.append({
            "mint": mint,
            "best_outcome_label": row["best_outcome_label"],
            "early_avoid_decision": e.get("decision", ""),
            "continue_tracking_decision": ct.get("decision", ""),
            "candidate_eligibility_v2_decision": c.get("decision", ""),
            "buy_setup_draft_family": b.get("setup_decision", ""),
            "candidate_reason_codes": c.get("reason_codes", row["candidate_reason_codes"]),
            "early_burst_research_class": row["early_burst_research_class"],
            "survivor_bias_note": "candidate_gate_requires_checkpoint_or_replay_permission",
            "required_exit_risk_evidence": "clean_exit_window|adverse_proxy_model|replay_permission|formal_backtest_gate",
        })
    fields = [
        "mint",
        "best_outcome_label",
        "early_avoid_decision",
        "continue_tracking_decision",
        "candidate_eligibility_v2_decision",
        "buy_setup_draft_family",
        "candidate_reason_codes",
        "early_burst_research_class",
        "survivor_bias_note",
        "required_exit_risk_evidence",
    ]
    write_csv(output_root / "early_burst_strategy_family_diagnostics.csv", rows, fields)
    decisions = Counter(row["candidate_eligibility_v2_decision"] or "missing" for row in rows)
    early_decisions = Counter(row["early_avoid_decision"] or "missing" for row in rows)
    continue_decisions = Counter(row["continue_tracking_decision"] or "missing" for row in rows)
    failed = Counter(reason for row in rows for reason in row["candidate_reason_codes"].split("|") if reason)
    summary = {
        "rows": len(rows),
        "avoided": early_decisions.get("avoid", 0),
        "continued_tracking": continue_decisions.get("continue_tracking", 0),
        "candidate_watch": decisions.get("candidate_watch", 0),
        "failed_candidate_eligibility": len(rows) - decisions.get("candidate_watch", 0) - decisions.get("candidate_review", 0),
        "top_failed_gates": dict(failed.most_common(12)),
    }
    write_text(
        output_root / "EARLY_BURST_STRATEGY_FAMILY_DIAGNOSTICS.md",
        "# Early-Burst Strategy Family Diagnostics\n\n"
        f"- positive_or_high_mints: `{len(rows)}`\n"
        f"- avoided_by_early_avoid: `{summary['avoided']}`\n"
        f"- continue_tracking: `{summary['continued_tracking']}`\n"
        f"- candidate_watch: `{summary['candidate_watch']}`\n"
        f"- failed_candidate_eligibility: `{summary['failed_candidate_eligibility']}`\n"
        f"- top_failed_gates: `{summary['top_failed_gates']}`\n"
        "- candidate_gate_survivor_biased: `true`, by design, because candidate/replay gates require clean countability and replay permission.\n"
        "- early_burst_should_be_separate_family: `true`, research-only, with exit/risk evidence required before any replay/backtest discussion.\n"
        "- no thresholds were tuned and no buy/sell entries were emitted.\n",
    )
    return summary


def build_early_burst_scores(data_mart_root: pathlib.Path, output_root: pathlib.Path) -> dict[str, Any]:
    rows = []
    for horizon in (5, 10, 30):
        for feature_row in read_csv(data_mart_root / f"strategy_asof_features_{horizon:03d}s.csv"):
            rows.append(score_feature_row(feature_row))
    write_csv(output_root / "early_burst_setup_v0_scores.csv", rows, EARLY_BURST_SCORE_FIELDS)
    decisions = Counter(row["decision"] for row in rows)
    reasons = Counter(reason for row in rows for reason in str(row["reason_codes"]).split("|") if reason)
    write_text(
        output_root / "EARLY_BURST_SETUP_V0_REPORT.md",
        "# EarlyBurstSetupDraft v0 Report\n\n"
        "- status: `disabled_by_default`\n"
        "- execution_mode: `research_only`\n"
        "- trade_actions_emitted: `0`\n"
        f"- rows_scored: `{len(rows)}`\n"
        f"- decisions: `{dict(decisions)}`\n"
        f"- top_reason_codes: `{dict(reasons.most_common(12))}`\n"
        "- blocked_actions: `replay`, `backtesting`, `threshold_tuning`, `paper_trading`, `live_trading`, `wallet_execution`\n",
    )
    return {"rows": len(rows), "decisions": dict(decisions), "top_reasons": dict(reasons.most_common(12))}


def write_exit_risk_draft(output_root: pathlib.Path, review_summary: dict[str, Any]) -> None:
    write_text(
        output_root / "EARLY_BURST_EXIT_RISK_DRAFT.md",
        "# Early-Burst Exit/Risk Draft\n\n"
        "This is research-only. No replay, backtest, threshold tuning, paper trading, live trading, wallet execution, or order generation was run.\n\n"
        "## Why Current Positives Still Fail\n"
        "- Positive/high-positive rows are short-window stream-proxy outcomes, mostly ending `early_rejected_dead` or remaining non-replayable.\n"
        "- Candidate gates are survivor/countability oriented, so early bursts without replay permission remain outside candidate eligibility.\n\n"
        "## Required Future Evidence\n"
        "- Clean replay-eligible candidate labels.\n"
        "- A validated exit window using only as-of features at decision time and forward labels only for evaluation.\n"
        "- Adverse movement proxy validation from liquidity, reserve, curve movement, sell pressure, and holder concentration.\n"
        "- Formal backtesting gate pass before any threshold tuning or profitability language.\n\n"
        "## Current Review Classes\n"
        f"- class_counts: `{review_summary.get('class_counts', {})}`\n",
    )


def write_gpt_pack(output_root: pathlib.Path, architecture_root: pathlib.Path) -> pathlib.Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    pack = output_root / f"early_burst_strategy_research_pack_{timestamp}"
    pack.mkdir(parents=True, exist_ok=True)
    files = [
        output_root / "POSITIVE_HIGH_POSITIVE_MINT_REVIEW.md",
        output_root / "positive_high_positive_mint_review.csv",
        output_root / "EARLY_BURST_STRATEGY_FAMILY_DIAGNOSTICS.md",
        output_root / "early_burst_strategy_family_diagnostics.csv",
        output_root / "EARLY_BURST_SETUP_V0_REPORT.md",
        output_root / "early_burst_setup_v0_scores.csv",
        output_root / "EARLY_BURST_EXIT_RISK_DRAFT.md",
        output_root / "GATE_VS_POSITIVE_OUTCOMES.md",
        output_root / "POSITIVE_OUTCOME_AUDIT.md",
        architecture_root / "feature_store_report.md",
        architecture_root / "label_store_report.md",
        architecture_root / "candidate_eligibility_v2_report.md",
    ]
    for path in files:
        if path.exists():
            shutil.copy2(path, pack / path.name)
    write_text(
        pack / "README_FOR_GPT.md",
        "# Early-Burst Strategy Research Pack\n\n"
        "This pack reviews short-window positive/high-positive stream-proxy outcomes. It is not a backtest, replay, tuning run, buy signal, or profitability claim.\n",
    )
    write_text(
        pack / "GPT_EARLY_BURST_STRATEGY_PROMPT.md",
        "# GPT Early-Burst Strategy Prompt\n\n"
        "Do not claim profitability. Do not tune thresholds. Do not backtest. Do not output trade entries. Focus on early-burst strategy architecture, exit/risk requirements, validation design, candidate/replay blockers, and evidence required before any replay/backtest/tuning/trading gate can be considered.\n",
    )
    return pack


def build_early_burst_research(
    *,
    output_root: pathlib.Path = PIPELINE_ROOT,
    data_mart_root: pathlib.Path = DATA_MART_ROOT,
    architecture_root: pathlib.Path = STRATEGY_ARCHITECTURE_ROOT,
) -> dict[str, Any]:
    review_rows, review_summary = build_positive_high_review(output_root, architecture_root)
    diagnostics = build_family_diagnostics(output_root, architecture_root, review_rows)
    scores = build_early_burst_scores(data_mart_root, output_root)
    write_exit_risk_draft(output_root, review_summary)
    pack = write_gpt_pack(output_root, architecture_root)
    summary = {
        "early_burst_strategy_research_ready": True,
        "unique_positive_high_mints": review_summary["unique_mints"],
        "unique_high_positive_mints": sum(1 for row in review_rows if row["best_outcome_label"] == "high_positive"),
        "review_summary": review_summary,
        "diagnostics": diagnostics,
        "scores": scores,
        "gpt_pack_path": str(pack),
        "backtesting_ready": False,
        "replay_ready": False,
        "threshold_tuning_ready": False,
        "live_trading_ready": False,
        "profitability_claim_allowed": False,
    }
    write_json(output_root / "early_burst_research_summary.json", summary)
    return summary
