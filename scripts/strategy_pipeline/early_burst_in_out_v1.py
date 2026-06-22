from __future__ import annotations

import hashlib
import json
import pathlib
import zipfile
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from .io import read_csv, read_json, write_csv, write_json, write_text
from .schemas import PIPELINE_ROOT, boolish


V1_OUTPUT_ROOT = PIPELINE_ROOT / "early_burst_in_out_v1"
DUAL_ROOT = PIPELINE_ROOT / "dual_strategy_tracks"
EXIT_GUARD_ROOT = PIPELINE_ROOT / "strategy_candidates_from_existing_data"


V1_DECISIONS = {
    "early_burst_candidate_review",
    "reject",
    "audit_only",
    "censored",
    "insufficient_data",
}


V1_SCORE_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "horizon_seconds",
    "decision",
    "reason_codes",
    "top_blocker",
    "v1_promoted_or_would_promote",
    "dead_launch_avoider_decision",
    "exit_window_guard_decision",
    "exit_window_observed_or_measurable",
    "curve_progress_bin",
    "buy_sell_followthrough_bin",
    "volume_followthrough_bin",
    "holder_growth_bin",
    "sell_pressure_bin",
    "holder_dev_risk_bin",
    "liquidity_risk_bin",
    "positive_outcome_label",
    "high_positive",
    "max_favorable_proxy",
    "max_adverse_proxy",
    "final_outcome",
    "clean_negative_label",
    "censored_label",
    "candidate_checkpoint_seen",
    "replay_eligible",
    "backtest_eligible",
    "paper_trading_eligible",
    "live_trading_eligible",
    "wallet_action",
    "trade_action",
    "review_gate_only",
]


REVIEW_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "decision_horizon",
    "forward_window",
    "v1_promotion_decision",
    "dead_launch_avoider_decision",
    "exit_window_guard_decision",
    "positive_high_positive_label",
    "high_positive",
    "max_favorable_proxy",
    "max_adverse_proxy",
    "exit_window_evidence",
    "adverse_sell_pressure",
    "holder_dev_risk",
    "vault_curve_progress",
    "final_outcome",
    "censored_status",
    "candidate_checkpoint_status",
    "replay_eligibility_status",
    "first_blocker_to_replay",
    "why_review_only",
    "safe_next_action",
]


COMPARISON_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "horizon_seconds",
    "v0_decision",
    "v1_decision",
    "v0_reason_codes",
    "v1_reason_codes",
    "decision_change",
    "positive_outcome_label",
    "high_positive",
    "clean_negative_label",
    "censored_label",
    "candidate_checkpoint_seen",
    "replay_eligible",
]


EXIT_FIELDS = [
    "mint",
    "slice_id",
    "segment_id",
    "relay_session_id",
    "horizon_seconds",
    "exit_window_guard_v1_decision",
    "exit_window_guard_v1_reason_codes",
    "exit_window_observed_or_measurable",
    "max_favorable_proxy",
    "max_adverse_proxy",
    "adverse_sell_pressure",
    "liquidity_exit_proxy",
    "replay_eligible",
    "trade_action",
]


def key_for(row: dict[str, Any]) -> tuple[str, str, str]:
    return (str(row.get("mint", "")), str(row.get("slice_id", "")), str(row.get("segment_id", "")))


def horizon_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (*key_for(row), str(row.get("horizon_seconds") or row.get("decision_horizon_seconds") or ""))


def numeric(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        return float(str(value))
    except (TypeError, ValueError):
        return None


def truth(value: Any) -> str:
    return str(boolish(value)).lower()


def split_reasons(value: Any) -> list[str]:
    return [item for item in str(value or "").split("|") if item]


def join_reasons(reasons: list[str]) -> str:
    seen: list[str] = []
    for reason in reasons:
        if reason and reason not in seen:
            seen.append(reason)
    return "|".join(seen)


def evidence_present(row: dict[str, str]) -> list[str]:
    evidence: list[str] = []
    if row.get("curve_progress_bin") in {"LOW", "MEDIUM", "HIGH"}:
        evidence.append("early_curve_progress")
    if row.get("buy_sell_followthrough_bin") in {"LOW", "MEDIUM", "HIGH"}:
        evidence.append("early_buy_sell_followthrough")
    if row.get("volume_followthrough_bin") in {"LOW", "MEDIUM", "HIGH"}:
        evidence.append("early_volume_followthrough")
    return evidence


def load_auxiliary(pipeline_root: pathlib.Path) -> dict[str, dict[tuple[str, str, str, str], dict[str, str]]]:
    positives = {
        horizon_key(row): row
        for row in read_csv(pipeline_root / "positive_outcome_labels.csv")
    }
    exits = {
        horizon_key(row): row
        for row in read_csv(pipeline_root / "strategy_candidates_from_existing_data" / "exit_window_guard_v0_scores.csv")
    }
    return {"positives": positives, "exits": exits}


def score_early_burst_in_out_v1(
    row: dict[str, str],
    *,
    positive_row: dict[str, str] | None = None,
    exit_row: dict[str, str] | None = None,
) -> dict[str, Any]:
    positive_row = positive_row or {}
    exit_row = exit_row or {}
    reasons: list[str] = []
    decision = "insufficient_data"
    prefilter = row.get("dead_launch_avoider_prefilter", "")
    v1_promoted = boolish(row.get("v1_promoted_or_would_promote"))
    exit_observed = boolish(row.get("exit_window_observed_or_measurable")) or boolish(
        positive_row.get("forward_window_observed")
    ) or boolish(exit_row.get("forward_window_observed"))
    evidence = evidence_present(row)
    candidate_checkpoint = boolish(row.get("candidate_checkpoint_seen"))
    replay_eligible = False

    if boolish(row.get("censored_label")) or row.get("final_outcome") == "terminal_inconclusive":
        decision = "censored"
        reasons.append("terminal_inconclusive_censored")
    elif row.get("decision") == "audit_only" or row.get("top_blocker") in {
        "data_quality_excluded",
        "degraded_audit_only",
    }:
        decision = "audit_only"
        reasons.append("provider_or_relay_gap_exposed" if "gap" in row.get("reason_codes", "") else "missing_asof_features")
    elif candidate_checkpoint:
        decision = "audit_only"
        reasons.extend(["candidate_checkpoint_audit_only", "replay_not_allowed"])
    elif prefilter == "reject":
        decision = "reject"
        reasons.append("dead_launch_avoider_reject")
    elif not v1_promoted:
        decision = "reject"
        reasons.append("v1_not_promoted_or_would_promote_absent")
    elif not exit_observed:
        decision = "insufficient_data"
        reasons.append("missing_horizon")
    elif not evidence:
        decision = "reject"
        reasons.append("missing_asof_features")
    else:
        decision = "early_burst_candidate_review"
        reasons.extend(["v1_promoted_or_would_promote", *evidence, "research_only_not_signal", "replay_not_allowed"])

    if row.get("sell_pressure_bin") in {"HIGH", "UNSAFE"}:
        reasons.append("adverse_sell_pressure_before_exit")
    if row.get("holder_dev_risk_bin") in {"HIGH", "UNSAFE"}:
        reasons.append("holder_concentration_risk")
        reasons.append("dev_or_creator_holding_risk")
    if row.get("liquidity_risk_bin") in {"HIGH", "UNSAFE"}:
        reasons.append("liquidity_exit_proxy")
    if row.get("curve_progress_bin") in {"MISSING", "LOW"} and decision != "early_burst_candidate_review":
        reasons.append("curve_progress_stall")
    if decision != "early_burst_candidate_review":
        reasons.append("replay_not_allowed")

    max_favorable = positive_row.get("max_favorable_proxy") or exit_row.get("max_favorable_proxy", "")
    max_adverse = positive_row.get("max_adverse_proxy") or exit_row.get("max_adverse_proxy", "")
    exit_guard_decision = exit_row.get("decision") or ("observable" if exit_observed else "missing_horizon")
    return {
        "mint": row.get("mint", ""),
        "slice_id": row.get("slice_id", ""),
        "segment_id": row.get("segment_id", ""),
        "relay_session_id": row.get("relay_session_id", ""),
        "horizon_seconds": row.get("horizon_seconds", ""),
        "decision": decision,
        "reason_codes": join_reasons(reasons),
        "top_blocker": next((reason for reason in reasons if reason not in {"research_only_not_signal"}), ""),
        "v1_promoted_or_would_promote": truth(v1_promoted),
        "dead_launch_avoider_decision": prefilter,
        "exit_window_guard_decision": exit_guard_decision,
        "exit_window_observed_or_measurable": truth(exit_observed),
        "curve_progress_bin": row.get("curve_progress_bin", ""),
        "buy_sell_followthrough_bin": row.get("buy_sell_followthrough_bin", ""),
        "volume_followthrough_bin": row.get("volume_followthrough_bin", ""),
        "holder_growth_bin": row.get("holder_growth_bin", ""),
        "sell_pressure_bin": row.get("sell_pressure_bin", ""),
        "holder_dev_risk_bin": row.get("holder_dev_risk_bin", ""),
        "liquidity_risk_bin": row.get("liquidity_risk_bin", ""),
        "positive_outcome_label": row.get("positive_outcome_label", ""),
        "high_positive": row.get("high_positive", "false"),
        "max_favorable_proxy": max_favorable,
        "max_adverse_proxy": max_adverse,
        "final_outcome": row.get("final_outcome", ""),
        "clean_negative_label": row.get("clean_negative_label", "false"),
        "censored_label": row.get("censored_label", "false"),
        "candidate_checkpoint_seen": row.get("candidate_checkpoint_seen", "false"),
        "replay_eligible": truth(replay_eligible),
        "backtest_eligible": "false",
        "paper_trading_eligible": "false",
        "live_trading_eligible": "false",
        "wallet_action": "none",
        "trade_action": "none",
        "review_gate_only": "true",
    }


def metrics(rows: list[dict[str, Any]], review_decision: str = "early_burst_candidate_review") -> dict[str, Any]:
    review_rows = [row for row in rows if row["decision"] == review_decision]
    return {
        "rows_scored": len(rows),
        "unique_mints_scored": len({row["mint"] for row in rows}),
        "review_candidates": len(review_rows),
        "unique_review_candidate_mints": len({row["mint"] for row in review_rows}),
        "positive_high_rows_captured": sum(
            1 for row in review_rows if row["positive_outcome_label"] in {"positive", "high_positive"} or boolish(row["high_positive"])
        ),
        "high_positive_rows_captured": sum(
            1 for row in review_rows if row["positive_outcome_label"] == "high_positive" or boolish(row["high_positive"])
        ),
        "dead_negative_rows_rejected": sum(
            1 for row in rows if row["decision"] == "reject" and boolish(row["clean_negative_label"])
        ),
        "censored_invalid_rows_excluded": sum(
            1 for row in rows if row["decision"] in {"censored", "audit_only"}
        ),
        "candidate_checkpoints": sum(1 for row in rows if boolish(row["candidate_checkpoint_seen"])),
        "replay_eligible_candidates": sum(1 for row in rows if boolish(row["replay_eligible"])),
        "trade_actions_emitted": sum(1 for row in rows if row.get("trade_action") not in {"", "none"}),
        "decision_counts": dict(Counter(row["decision"] for row in rows)),
        "top_reason_codes": dict(Counter(reason for row in rows for reason in split_reasons(row.get("reason_codes"))).most_common(20)),
    }


def review_candidate_rows(scores: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in scores:
        if row["decision"] != "early_burst_candidate_review":
            continue
        replay_blocker = "replay_not_allowed"
        if boolish(row.get("candidate_checkpoint_seen")):
            replay_blocker = "candidate_checkpoint_audit_only"
        rows.append(
            {
                "mint": row["mint"],
                "slice_id": row["slice_id"],
                "segment_id": row["segment_id"],
                "relay_session_id": row["relay_session_id"],
                "decision_horizon": row["horizon_seconds"],
                "forward_window": "existing_exit_window_proxy",
                "v1_promotion_decision": "would_review",
                "dead_launch_avoider_decision": row["dead_launch_avoider_decision"],
                "exit_window_guard_decision": row["exit_window_guard_decision"],
                "positive_high_positive_label": row["positive_outcome_label"],
                "high_positive": row["high_positive"],
                "max_favorable_proxy": row["max_favorable_proxy"],
                "max_adverse_proxy": row["max_adverse_proxy"],
                "exit_window_evidence": row["exit_window_observed_or_measurable"],
                "adverse_sell_pressure": row["sell_pressure_bin"],
                "holder_dev_risk": row["holder_dev_risk_bin"],
                "vault_curve_progress": row["curve_progress_bin"],
                "final_outcome": row["final_outcome"],
                "censored_status": row["censored_label"],
                "candidate_checkpoint_status": row["candidate_checkpoint_seen"],
                "replay_eligibility_status": "not_replay_eligible",
                "first_blocker_to_replay": replay_blocker,
                "why_review_only": "research gate only; no replay, formal validation, tuning, order, or wallet execution is enabled",
                "safe_next_action": "operator review or future explicitly approved shadow proof only",
            }
        )
    return rows


def comparison_rows(v0_rows: list[dict[str, str]], v1_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key = {horizon_key(row): row for row in v1_rows}
    rows: list[dict[str, Any]] = []
    for v0 in v0_rows:
        v1 = by_key.get(horizon_key(v0), {})
        v0_decision = v0.get("decision", "")
        v1_decision = v1.get("decision", "")
        rows.append(
            {
                "mint": v0.get("mint", ""),
                "slice_id": v0.get("slice_id", ""),
                "segment_id": v0.get("segment_id", ""),
                "relay_session_id": v0.get("relay_session_id", ""),
                "horizon_seconds": v0.get("horizon_seconds", ""),
                "v0_decision": v0_decision,
                "v1_decision": v1_decision,
                "v0_reason_codes": v0.get("reason_codes", ""),
                "v1_reason_codes": v1.get("reason_codes", ""),
                "decision_change": "unchanged" if v0_decision == v1_decision else f"{v0_decision}_to_{v1_decision}",
                "positive_outcome_label": v1.get("positive_outcome_label", v0.get("positive_outcome_label", "")),
                "high_positive": v1.get("high_positive", v0.get("high_positive", "false")),
                "clean_negative_label": v1.get("clean_negative_label", v0.get("clean_negative_label", "false")),
                "censored_label": v1.get("censored_label", v0.get("censored_label", "false")),
                "candidate_checkpoint_seen": v1.get("candidate_checkpoint_seen", v0.get("candidate_checkpoint_seen", "false")),
                "replay_eligible": v1.get("replay_eligible", "false"),
            }
        )
    return rows


def exit_guard_v1_rows(scores: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in scores:
        reasons: list[str] = []
        if row["exit_window_observed_or_measurable"] != "true":
            reasons.append("missing_horizon")
        if row["sell_pressure_bin"] in {"HIGH", "UNSAFE"}:
            reasons.append("adverse_sell_pressure_before_exit")
        if row["liquidity_risk_bin"] in {"HIGH", "UNSAFE"}:
            reasons.append("liquidity_exit_proxy")
        if not reasons:
            reasons.append("exit_window_observable_for_review")
        rows.append(
            {
                "mint": row["mint"],
                "slice_id": row["slice_id"],
                "segment_id": row["segment_id"],
                "relay_session_id": row["relay_session_id"],
                "horizon_seconds": row["horizon_seconds"],
                "exit_window_guard_v1_decision": "observable_for_review" if row["exit_window_observed_or_measurable"] == "true" else "insufficient_data",
                "exit_window_guard_v1_reason_codes": join_reasons(reasons),
                "exit_window_observed_or_measurable": row["exit_window_observed_or_measurable"],
                "max_favorable_proxy": row["max_favorable_proxy"],
                "max_adverse_proxy": row["max_adverse_proxy"],
                "adverse_sell_pressure": row["sell_pressure_bin"],
                "liquidity_exit_proxy": row["liquidity_risk_bin"],
                "replay_eligible": "false",
                "trade_action": "none",
            }
        )
    return rows


def markdown_counter(counter: dict[str, int]) -> str:
    lines = ["| item | count |", "|---|---:|"]
    for key, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:25]:
        lines.append(f"| `{key}` | {count} |")
    return "\n".join(lines)


def write_reports(output_root: pathlib.Path, summary: dict[str, Any], review_rows: list[dict[str, Any]], comparison: list[dict[str, Any]]) -> None:
    v1 = summary["early_burst_in_out_v1"]
    v0 = summary["early_burst_in_out_v0"]
    write_text(
        output_root / "EARLY_BURST_IN_OUT_V1_REPORT.md",
        f"""# EarlyBurstInOutV1 Report

Classification: `EARLY_BURST_IN_OUT_V1_REVIEW_ARTIFACT_PASS`

This pack uses existing data only. No relay, collection, replay, formal validation, threshold tuning, paper/live trading, wallet execution, profitability claim, or cap raise was run.

## What V1 Does

V1 is a research-only early-burst review allocator. It requires:

- PromotionPriorityStrategyV1 promoted or would-promote the row.
- DeadLaunchAvoiderV0 does not hard-reject the row.
- At least one as-of follow-through family is present: curve progress, buy/sell follow-through, or volume follow-through.
- Exit window is observable or measurable.
- The row is not terminal-inconclusive, degraded audit-only, or blocked by provider/relay/R2/artifact quality.

It emits no trade or wallet actions and creates no replay/backtest eligibility.

## V1 Metrics

| metric | value |
|---|---:|
| rows scored | {v1['rows_scored']} |
| unique mints scored | {v1['unique_mints_scored']} |
| review candidates | {v1['review_candidates']} |
| unique review mints | {v1['unique_review_candidate_mints']} |
| positive/high-positive rows captured | {v1['positive_high_rows_captured']} |
| high-positive rows captured | {v1['high_positive_rows_captured']} |
| dead-negative rows rejected | {v1['dead_negative_rows_rejected']} |
| censored/invalid rows excluded | {v1['censored_invalid_rows_excluded']} |
| candidate checkpoints | {v1['candidate_checkpoints']} |
| replay-eligible candidates | {v1['replay_eligible_candidates']} |
| trade actions emitted | {v1['trade_actions_emitted']} |

## Decision Counts

{markdown_counter(v1['decision_counts'])}

## Top Reason Codes

{markdown_counter(v1['top_reason_codes'])}

## V0 Context

V0 review candidates: `{v0['review_candidates']}`. V1 review candidates: `{v1['review_candidates']}`.

V1 preserves every high-positive row that V0 captured: `{str(summary['v1_preserves_v0_high_positive_review_rows']).lower()}`.
""",
    )
    write_text(
        output_root / "EARLY_BURST_REVIEW_CANDIDATES_V1.md",
        "# Early-Burst Review Candidates V1\n\n"
        + f"Rows: `{len(review_rows)}`\n\n"
        + "\n".join(
            f"- `{row['mint']}` slice `{row['slice_id']}` horizon `{row['decision_horizon']}` evidence curve `{row['vault_curve_progress']}` sell pressure `{row['adverse_sell_pressure']}` replay `{row['replay_eligibility_status']}`"
            for row in review_rows[:100]
        )
        + ("\n" if review_rows else "No V1 review candidates in existing data.\n"),
    )
    schema = {
        "schema_version": "phase107k.early_burst_in_out_v1.review_candidate.v1",
        "decision": "early_burst_candidate_review",
        "fields": {field: "string" for field in REVIEW_FIELDS},
        "replay_policy": "not_replay_eligible; countability/replay gates remain separate and blocked",
        "trade_policy": "research_only_no_trade_or_wallet_action",
    }
    write_json(output_root / "EARLY_BURST_CANDIDATE_REVIEW_SCHEMA.json", schema)
    write_text(
        output_root / "EARLY_BURST_CANDIDATE_REVIEW_POLICY.md",
        """# Early-Burst Candidate Review Policy

Rows in `early_burst_review_candidates_v1.csv` are review rows only. They are not trade actions, not replay eligibility, not formal validation rows, and not profitability evidence.

Review candidates must be clean enough to inspect and must carry at least one as-of early-burst evidence family. Candidate checkpoints remain audit-only. Terminal-inconclusive rows remain censored.
""",
    )
    write_text(
        output_root / "EARLY_BURST_EXIT_WINDOW_V1_REPORT.md",
        f"""# Early-Burst Exit Window V1 Report

V1 scored `{v1['rows_scored']}` rows and found `{v1['review_candidates']}` review rows with observable or measurable exit windows.

The exit-window guard remains research-only. It is used to explain whether an existing row has enough forward-window evidence for review. It does not authorize replay, formal validation, tuning, or trading.
""",
    )
    write_text(
        output_root / "EARLY_BURST_V1_VS_V0_COMPARISON.md",
        f"""# EarlyBurstInOut V1 vs V0 Comparison

| metric | V0 | V1 |
|---|---:|---:|
| review candidates | {v0['review_candidates']} | {v1['review_candidates']} |
| unique review mints | {v0['unique_review_candidate_mints']} | {v1['unique_review_candidate_mints']} |
| positive/high-positive rows captured | {v0['positive_high_rows_captured']} | {v1['positive_high_rows_captured']} |
| high-positive rows captured | {v0['high_positive_rows_captured']} | {v1['high_positive_rows_captured']} |
| dead-negative rows rejected | {v0['dead_negative_rows_rejected']} | {v1['dead_negative_rows_rejected']} |
| censored/invalid rows excluded | {v0['censored_invalid_rows_excluded']} | {v1['censored_invalid_rows_excluded']} |
| replay-eligible candidates | {v0['replay_eligible_candidates']} | {v1['replay_eligible_candidates']} |

V1 is a widened research-review lens compared with V0 because it accepts curve OR buy/sell OR volume evidence after the V1 promotion and DeadLaunchAvoider gates. It is better suited for scarce manual review because it keeps the safety gates intact while surfacing more early-burst rows for inspection.

V1 preserves all V0 high-positive review rows: `{str(summary['v1_preserves_v0_high_positive_review_rows']).lower()}`.
""",
    )
    write_text(
        output_root / "SURVIVOR_TRACK_MONITORING_NOTE.md",
        """# Survivor Track Monitoring Note

SURVIVOR_RUNNER_V0 remains secondary. It produced zero review candidates in the current dual-track pass, and current data lacks true long-horizon survivor evidence. Keep recording long-horizon evidence, but do not prioritize survivor runner as the first review strategy until real survivor-style review rows appear.
""",
    )
    write_text(
        output_root / "EARLY_BURST_V1_RISK_REGISTER.md",
        """# EarlyBurstInOutV1 Risk Register

- Review rows may still be early-death rows; this is allocation research, not execution.
- Labels are used only after scoring for measurement, never as alpha inputs.
- Short-window evidence can overstate durability when long-horizon survival is missing.
- Holder/dev and liquidity proxy risks remain approximate stream-derived features.
- Candidate checkpoints remain audit-only and do not imply replay eligibility.
- Replay, formal validation, tuning, paper/live trading, wallet execution, and cap raises remain blocked.
""",
    )
    write_text(
        output_root / "EARLY_BURST_V1_NEXT_PROOF_PLAN.md",
        """# EarlyBurstInOutV1 Next Proof Plan

Collection remains blocked.

If later explicitly approved, the next proof should be a single targeted shadow proof:

- one 900s relay-only R2-primary slice;
- all-launch intake enabled;
- cheap follow-up enabled;
- EarlyBurstInOutV1 shadow review scoring enabled;
- live promotion behavior unchanged;
- no replay;
- no formal validation;
- no threshold tuning;
- no trading;
- no wallet execution;
- no cap raise.

The proof should measure whether V1 continues to preserve high-positive review coverage while keeping replay/backtesting/trading gates blocked.
""",
    )
    write_text(
        output_root / "GPT_EARLY_BURST_V1_CONTEXT.md",
        f"""# GPT EarlyBurstInOutV1 Context

Classification: `EARLY_BURST_IN_OUT_V1_REVIEW_ARTIFACT_PASS`

Read `EARLY_BURST_IN_OUT_V1_REPORT.md`, `EARLY_BURST_V1_VS_V0_COMPARISON.md`, and `early_burst_review_candidates_v1.csv`.

V1 review rows: `{v1['review_candidates']}`.
Unique review mints: `{v1['unique_review_candidate_mints']}`.
High-positive rows captured: `{v1['high_positive_rows_captured']}`.
Replay-eligible candidates: `{v1['replay_eligible_candidates']}`.

Treat this as research-only allocation analysis. Do not infer execution readiness or profitability.
""",
    )
    write_text(
        output_root / "GPT_EARLY_BURST_V1_PROMPT.md",
        """Review EarlyBurstInOutV1 as a research-only early-burst candidate-review allocator. Compare it with V0, summarize what it surfaces, identify risks, and propose what an explicitly approved future shadow proof should measure. Do not propose replay, formal validation, tuning, trading, wallet execution, or cap changes unless readiness gates explicitly pass.
""",
    )


def update_readiness(pipeline_root: pathlib.Path, output_root: pathlib.Path) -> None:
    decision_path = pipeline_root / "READINESS_DECISION.json"
    decision = read_json(decision_path)
    decision.update(
        {
            "strategy_research_ready": True,
            "dual_strategy_track_research_ready": True,
            "early_burst_in_out_v1_ready": True,
            "survivor_runner_monitoring_ready": True,
            "formal_backtesting_ready": False,
            "backtesting_ready": False,
            "replay_ready": False,
            "threshold_tuning_ready": False,
            "paper_trading_ready": False,
            "live_trading_ready": False,
            "wallet_execution_ready": False,
            "profitability_claim_allowed": False,
            "collection_allowed": False,
            "early_burst_in_out_v1_path": str(output_root),
        }
    )
    reason_codes = set(decision.get("reason_codes", []))
    reason_codes.update(
        {
            "early_burst_in_out_v1_review_artifact_ready",
            "review_only_no_replay_eligibility_created",
            "formal_backtest_not_allowed",
            "replay_not_allowed",
            "collection_blocked",
        }
    )
    decision["reason_codes"] = sorted(reason_codes)
    write_json(decision_path, decision)
    report_path = pipeline_root / "TRADING_STRATEGY_PIPELINE_REPORT.md"
    previous = report_path.read_text() if report_path.exists() else "# Trading Strategy Pipeline Report\n"
    marker = "\n## EarlyBurstInOutV1\n"
    addition = (
        marker
        + "\n"
        + "- early_burst_in_out_v1_ready: `true`\n"
        + "- survivor_runner_monitoring_ready: `true`\n"
        + "- replay_ready: `false`\n"
        + "- formal_backtesting_ready: `false`\n"
        + "- threshold_tuning_ready: `false`\n"
        + "- paper/live/wallet execution: `false`\n"
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
        if path.is_file() and path.name not in {"early_burst_in_out_v1_export.zip", "EXPORT_CHECKSUMS.txt"}:
            checksum_lines.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}")
    write_text(output_root / "EXPORT_CHECKSUMS.txt", "\n".join(checksum_lines) + "\n")
    zip_path = output_root / "early_burst_in_out_v1_export.zip"
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(output_root.iterdir()):
            if path.is_file() and path.name != zip_path.name:
                archive.write(path, arcname=path.name)
    return zip_path


def build_early_burst_in_out_v1(
    *,
    pipeline_root: pathlib.Path = PIPELINE_ROOT,
    output_root: pathlib.Path = V1_OUTPUT_ROOT,
    update_readiness_files: bool = True,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    v0_rows = read_csv(pipeline_root / "dual_strategy_tracks" / "early_burst_in_out_v0_scores.csv")
    aux = load_auxiliary(pipeline_root)
    scores = [
        score_early_burst_in_out_v1(
            row,
            positive_row=aux["positives"].get(horizon_key(row), {}),
            exit_row=aux["exits"].get(horizon_key(row), {}),
        )
        for row in v0_rows
    ]
    reviews = review_candidate_rows(scores)
    comparisons = comparison_rows(v0_rows, scores)
    exit_rows = exit_guard_v1_rows(scores)
    v1_metrics = metrics(scores)
    v0_metrics = metrics(v0_rows)
    v0_high_review_keys = {
        horizon_key(row)
        for row in v0_rows
        if row.get("decision") == "early_burst_candidate_review"
        and (row.get("positive_outcome_label") == "high_positive" or boolish(row.get("high_positive")))
    }
    v1_review_keys = {horizon_key(row) for row in scores if row.get("decision") == "early_burst_candidate_review"}
    summary = {
        "schema_version": "phase107k.early_burst_in_out_v1.v1",
        "classification": "EARLY_BURST_IN_OUT_V1_REVIEW_ARTIFACT_PASS",
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "early_burst_in_out_v0": v0_metrics,
        "early_burst_in_out_v1": v1_metrics,
        "v1_preserves_v0_high_positive_review_rows": v0_high_review_keys.issubset(v1_review_keys),
        "v1_high_positive_review_rows_preserved": len(v0_high_review_keys & v1_review_keys),
        "v0_high_positive_review_rows": len(v0_high_review_keys),
        "review_candidates_path": str(output_root / "early_burst_review_candidates_v1.csv"),
        "formal_backtesting_ready": False,
        "replay_ready": False,
        "threshold_tuning_ready": False,
        "paper_trading_ready": False,
        "live_trading_ready": False,
        "wallet_execution_ready": False,
        "profitability_claim_allowed": False,
        "collection_allowed": False,
    }
    write_csv(output_root / "early_burst_in_out_v1_scores.csv", scores, V1_SCORE_FIELDS)
    write_csv(output_root / "early_burst_review_candidates_v1.csv", reviews, REVIEW_FIELDS)
    write_csv(output_root / "early_burst_v1_vs_v0_comparison.csv", comparisons, COMPARISON_FIELDS)
    write_csv(output_root / "EXIT_WINDOW_GUARD_V1_SCORES.csv", exit_rows, EXIT_FIELDS)
    write_json(output_root / "early_burst_in_out_v1_summary.json", summary)
    write_reports(output_root, summary, reviews, comparisons)
    if update_readiness_files:
        update_readiness(pipeline_root, output_root)
    zip_path = write_zip(output_root)
    summary["output_root"] = str(output_root)
    summary["zip_path"] = str(zip_path)
    return summary
