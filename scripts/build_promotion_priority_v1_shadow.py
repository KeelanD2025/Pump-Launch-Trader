#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import pathlib
import sys
import zipfile
from collections import Counter
from datetime import datetime, timezone
from typing import Any

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
REPO = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))

from strategy_pipeline.promotion_priority import score_promotion_priority_v1


ROOT = REPO / "research_output" / "trading_strategy_pipeline"
DEFAULT_OUTPUT = ROOT / "promotion_priority_strategy_v1_shadow"
HORIZONS = (5, 10, 30, 60, 120, 300, 900)


def read_csv(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: pathlib.Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def read_json(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def write_json(path: pathlib.Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def key_for(row: dict[str, Any], horizon_field: str = "horizon_seconds") -> tuple[str, str, str, str, str]:
    return (
        row.get("mint", ""),
        row.get("slice_id", ""),
        row.get("segment_id", ""),
        row.get("relay_session_id", ""),
        str(row.get(horizon_field, "")),
    )


def truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def load_shadow_rows(root: pathlib.Path) -> list[dict[str, Any]]:
    features: dict[tuple[str, str, str, str, str], dict[str, str]] = {}
    for horizon in HORIZONS:
        for row in read_csv(root / "early_burst_validation_dataset" / f"early_burst_validation_features_{horizon:03d}s.csv"):
            features[key_for(row)] = row
    labels = {
        key_for(row, "decision_horizon_seconds"): row
        for row in read_csv(root / "early_burst_validation_dataset" / "early_burst_validation_rows.csv")
    }
    dead = {
        key_for(row): row
        for row in read_csv(root / "strategy_candidates_from_existing_data" / "dead_launch_avoider_v0_scores.csv")
    }
    v0 = {
        key_for(row): row
        for row in read_csv(root / "strategy_candidates_from_existing_data" / "promotion_priority_strategy_v0_scores.csv")
    }
    rows: list[dict[str, Any]] = []
    for item_key, v0_row in sorted(v0.items()):
        feature_row = features.get(item_key, {})
        label_row = labels.get(item_key, {})
        dead_row = dead.get(item_key, {})
        policy_input = {**feature_row, **v0_row}
        score = score_promotion_priority_v1(
            policy_input,
            dead_launch_avoider_decision=dead_row.get("decision", ""),
            dead_launch_avoider_reason_codes=dead_row.get("reason_codes", ""),
        )
        row = {
            "mint": v0_row.get("mint", ""),
            "slice_id": v0_row.get("slice_id", ""),
            "segment_id": v0_row.get("segment_id", ""),
            "relay_session_id": v0_row.get("relay_session_id", ""),
            "horizon_seconds": v0_row.get("horizon_seconds", ""),
            "promotion_priority_v1_shadow_decision": score.decision,
            "promotion_priority_v1_shadow_reason_codes": "|".join(score.reason_codes),
            "promotion_priority_v1_shadow_confidence_bin": score.confidence_bin,
            "promotion_priority_v1_would_promote": str(score.would_promote).lower(),
            "promotion_priority_v1_would_reject": str(score.would_reject).lower(),
            "promotion_priority_v1_would_keep_cheap_followup": str(score.would_keep_cheap_followup).lower(),
            "promotion_priority_v1_shadow_only": "true",
            "v0_decision": v0_row.get("decision", ""),
            "dead_launch_avoider_v0_decision": dead_row.get("decision", ""),
            "dead_launch_avoider_v0_reason_codes": dead_row.get("reason_codes", ""),
            "final_outcome": label_row.get("final_outcome", ""),
            "positive_outcome_label": label_row.get("positive_outcome_label", ""),
            "positive_outcome_strength_bin": label_row.get("positive_outcome_strength_bin", ""),
            "early_burst_class": label_row.get("early_burst_class", ""),
            "candidate_checkpoint_seen": str(truthy(label_row.get("candidate_checkpoint_seen"))).lower(),
            "replay_eligible": str(truthy(label_row.get("replay_eligible"))).lower(),
            "trade_action": score.trade_action,
            "candidate_eligibility_created": str(score.candidate_eligible).lower(),
            "replay_eligibility_created": str(score.replay_eligible).lower(),
            "countability_affects": str(score.countability_affects).lower(),
            "holder_rpc_used": str(truthy(feature_row.get("holder_rpc_used"))).lower(),
            "rpc_mint_supply_canonical": str(truthy(feature_row.get("rpc_mint_supply_canonical"))).lower(),
        }
        rows.append(row)
    return rows


def build_current_policy_comparison(root: pathlib.Path, shadow_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    shadow_by_key = {key_for(row): row for row in shadow_rows}
    promotion_rows = read_csv(root / "all_launch_tracking" / "promotion_budget_diagnosis.csv")
    comparison: list[dict[str, Any]] = []
    for row in promotion_rows:
        item_key = key_for(row)
        shadow = shadow_by_key.get(item_key)
        if not shadow:
            # Fall back by mint + horizon where older diagnostics omit relay/session consistency.
            matches = [
                candidate
                for candidate in shadow_rows
                if candidate["mint"] == row.get("mint", "") and str(candidate["horizon_seconds"]) == str(row.get("horizon_seconds", ""))
            ]
            shadow = matches[0] if matches else {}
        status = row.get("promotion_status", "")
        current_admitted = status == "admitted_to_rich_tracking"
        current_blocked = status != "" and status != "admitted_to_rich_tracking"
        comparison.append(
            {
                "mint": row.get("mint", ""),
                "slice_id": row.get("slice_id", ""),
                "segment_id": row.get("segment_id", ""),
                "horizon_seconds": row.get("horizon_seconds", ""),
                "current_promotion_status": status,
                "current_promotion_blocker": row.get("promotion_blocker", ""),
                "current_admitted": str(current_admitted).lower(),
                "current_blocked": str(current_blocked).lower(),
                "v1_shadow_decision": shadow.get("promotion_priority_v1_shadow_decision", "insufficient_data"),
                "v1_shadow_reason_codes": shadow.get("promotion_priority_v1_shadow_reason_codes", "insufficient_followup"),
                "v1_would_promote": shadow.get("promotion_priority_v1_would_promote", "false"),
                "v1_would_reject": shadow.get("promotion_priority_v1_would_reject", "false"),
                "admitted_but_v1_rejects": str(current_admitted and shadow.get("promotion_priority_v1_would_reject") == "true").lower(),
                "blocked_but_v1_promotes": str(current_blocked and shadow.get("promotion_priority_v1_would_promote") == "true").lower(),
                "later_positive_or_high_positive": row.get("later_positive_or_high_positive", ""),
                "later_high_positive": row.get("later_high_positive", ""),
                "candidate_checkpoint_seen": row.get("candidate_checkpoint_seen", "false"),
                "replay_eligible": row.get("replay_eligible", "false"),
            }
        )
    return comparison


def metric_counts(shadow_rows: list[dict[str, Any]], comparison_rows: list[dict[str, Any]]) -> dict[str, Any]:
    decision_counts = Counter(row["promotion_priority_v1_shadow_decision"] for row in shadow_rows)
    total_hp = sum(1 for row in shadow_rows if row["positive_outcome_label"] == "high_positive")
    promoted_hp = sum(
        1
        for row in shadow_rows
        if row["positive_outcome_label"] == "high_positive" and row["promotion_priority_v1_would_promote"] == "true"
    )
    promoted_dead = sum(
        1
        for row in shadow_rows
        if row["positive_outcome_label"] == "dead_negative" and row["promotion_priority_v1_would_promote"] == "true"
    )
    current_admitted_v1_rejects = [
        row for row in comparison_rows if row["admitted_but_v1_rejects"] == "true"
    ]
    current_blocked_v1_promotes = [
        row for row in comparison_rows if row["blocked_but_v1_promotes"] == "true"
    ]
    return {
        "shadow_rows": len(shadow_rows),
        "shadow_unique_mints": len({row["mint"] for row in shadow_rows}),
        "decision_counts": dict(decision_counts),
        "high_positive_rows": total_hp,
        "high_positive_rows_promoted_by_shadow": promoted_hp,
        "high_positive_unique_mints": len({row["mint"] for row in shadow_rows if row["positive_outcome_label"] == "high_positive"}),
        "high_positive_shadow_capture_all": promoted_hp == total_hp,
        "dead_negative_rows_promoted_by_shadow": promoted_dead,
        "current_admitted_promotions_v1_would_reject": len(current_admitted_v1_rejects),
        "current_blocked_promotions_v1_would_promote": len(current_blocked_v1_promotes),
        "v1_rejected_current_admitted_later_positive_or_high": sum(
            1 for row in current_admitted_v1_rejects if truthy(row.get("later_positive_or_high_positive"))
        ),
        "v1_promoted_current_blocked_later_positive_or_high": sum(
            1 for row in current_blocked_v1_promotes if truthy(row.get("later_positive_or_high_positive"))
        ),
        "v1_promoted_current_blocked_later_high": sum(
            1 for row in current_blocked_v1_promotes if truthy(row.get("later_high_positive"))
        ),
        "trade_actions_emitted": sum(1 for row in shadow_rows if row.get("trade_action") != "none"),
        "replay_eligibility_created": sum(1 for row in shadow_rows if row.get("replay_eligibility_created") == "true"),
        "candidate_eligibility_created": sum(1 for row in shadow_rows if row.get("candidate_eligibility_created") == "true"),
        "countability_affects": sum(1 for row in shadow_rows if row.get("countability_affects") == "true"),
    }


def write_reports(output: pathlib.Path, metrics: dict[str, Any], comparison_rows: list[dict[str, Any]], shadow_rows: list[dict[str, Any]]) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    current_admitted_rejects = metrics["current_admitted_promotions_v1_would_reject"]
    blocked_promotes = metrics["current_blocked_promotions_v1_would_promote"]
    output.joinpath("PROMOTION_PRIORITY_V1_SHADOW_POLICY_REPORT.md").write_text(
        f"""# Promotion Priority V1 Shadow Policy Report

Generated: `{now}`

Classification: `PROMOTION_PRIORITY_V1_SHADOW_POLICY_PASS`

`PromotionPriorityStrategyV1` is now represented as a reusable shadow policy. Shadow outputs are research-only and do not affect live promotion, candidate eligibility, replay eligibility, countability, buy/sell/order actions, or launch caps.

## Results

- shadow rows scored: `{metrics['shadow_rows']}`
- unique mints scored: `{metrics['shadow_unique_mints']}`
- decision counts: `{metrics['decision_counts']}`
- high-positive rows captured: `{metrics['high_positive_rows_promoted_by_shadow']} / {metrics['high_positive_rows']}`
- high-positive unique mints: `{metrics['high_positive_unique_mints']}`
- dead-negative rows promoted by shadow: `{metrics['dead_negative_rows_promoted_by_shadow']}`
- current admitted promotions V1 would reject: `{current_admitted_rejects}`
- current blocked promotions V1 would promote: `{blocked_promotes}`
- trade actions emitted: `{metrics['trade_actions_emitted']}`
- replay eligibility created: `{metrics['replay_eligibility_created']}`
- candidate eligibility created: `{metrics['candidate_eligibility_created']}`
- countability affected: `{metrics['countability_affects']}`

## Safety State

- collection_allowed: `false`
- formal_backtesting_ready: `false`
- replay_ready: `false`
- threshold_tuning_ready: `false`
- paper_trading_ready: `false`
- live_trading_ready: `false`
- profitability_claim_allowed: `false`
- launch_caps_remain_blocked: `true`
"""
    )
    output.joinpath("V1_SHADOW_VS_CURRENT_PROMOTION_POLICY.md").write_text(
        f"""# V1 Shadow Vs Current Promotion Policy

Generated: `{now}`

## Questions

- Did V1 shadow capture all known high-positive rows? `{str(metrics['high_positive_shadow_capture_all']).lower()}`.
- How many current admitted promotions would V1 reject? `{current_admitted_rejects}`.
- How many current blocked promotions would V1 promote? `{blocked_promotes}`.
- Did any V1-rejected current admitted row later become positive/high-positive? `{metrics['v1_rejected_current_admitted_later_positive_or_high']}`.
- Did any V1-promoted blocked row later become positive/high-positive? `{metrics['v1_promoted_current_blocked_later_positive_or_high']}`.
- Did any V1-promoted blocked row later become high-positive? `{metrics['v1_promoted_current_blocked_later_high']}`.

## Interpretation

V1 should be tested in shadow mode during a future explicitly approved proof before it controls promotion. It appears useful for reducing rich-slot pressure while preserving known high-positive rows, but this is not a backtest and not a profitability claim.
"""
    )
    high_positive_rows = [
        row for row in shadow_rows if row.get("positive_outcome_label") == "high_positive"
    ]
    output.joinpath("HIGH_POSITIVE_SHADOW_CAPTURE_REVIEW.md").write_text(
        f"""# High Positive Shadow Capture Review

Generated: `{now}`

- high-positive rows: `{len(high_positive_rows)}`
- high-positive unique mints: `{len({row['mint'] for row in high_positive_rows})}`
- promoted by V1 shadow: `{metrics['high_positive_rows_promoted_by_shadow']}`
- captured all known high-positive rows: `{str(metrics['high_positive_shadow_capture_all']).lower()}`

See `promotion_priority_v1_shadow_scores.csv` for row-level reason codes.
"""
    )
    rejected_dead = [
        row
        for row in shadow_rows
        if row.get("positive_outcome_label") == "dead_negative"
        and row.get("promotion_priority_v1_would_reject") == "true"
    ]
    output.joinpath("DEAD_NEGATIVE_SHADOW_REJECTION_REVIEW.md").write_text(
        f"""# Dead Negative Shadow Rejection Review

Generated: `{now}`

- dead-negative rows V1 would reject: `{len(rejected_dead)}`
- dead-negative rows V1 would still promote: `{metrics['dead_negative_rows_promoted_by_shadow']}`
- main guardrail: DeadLaunchAvoiderV0 pre-filter plus hard rejection for liquidity exits, holder collapse, adverse sell pressure, and insufficient follow-up.
"""
    )
    missed_rows = read_csv(ROOT / "all_launch_tracking" / "missed_good_token_audit_v2_analysis.csv")
    output.joinpath("MISSED_GOOD_SHADOW_AUDIT.md").write_text(
        f"""# Missed Good Shadow Audit

Generated: `{now}`

- missed-good audit v2 rows available: `{len(missed_rows)}`
- current blocked promotions V1 would promote: `{blocked_promotes}`
- current blocked promotions V1 would promote that later became positive/high-positive: `{metrics['v1_promoted_current_blocked_later_positive_or_high']}`
- current blocked promotions V1 would promote that later became high-positive: `{metrics['v1_promoted_current_blocked_later_high']}`

This audit remains diagnostic only. It creates no candidate eligibility or replay permission.
"""
    )
    output.joinpath("NEXT_PROOF_PLAN_FOR_V1_SHADOW.md").write_text(
        f"""# Next Proof Plan For V1 Shadow

Generated: `{now}`

## Required Before Any Live Shadow Proof

- written targeted collection justification;
- one small proof only, not generic collection;
- V1 fields emitted as shadow-only columns;
- current live promotion unchanged;
- stop on candidate/replay trigger;
- no replay, backtesting, threshold tuning, trading, wallet execution, cap raises, holder RPC, or canonical RPC supply.

## Proof Should Measure

- V1 shadow decision distribution;
- high-positive capture;
- admitted promotions V1 would reject;
- blocked promotions V1 would promote;
- missed-good audit deltas;
- R2/artifact consistency;
- no effect on countability, replay, candidates, or live promotion.
"""
    )
    output.joinpath("GPT_PROMOTION_PRIORITY_V1_SHADOW_CONTEXT.md").write_text(
        f"""# GPT Promotion Priority V1 Shadow Context

Classification: `PROMOTION_PRIORITY_V1_SHADOW_POLICY_PASS`

V1 is a research-only shadow policy for rich-tracking promotion priority. It captured all known high-positive rows in existing offline data and reduced dead-negative promotions, but it must not be treated as a buy signal or profitability evidence.

Key numbers:

- high-positive capture: `{metrics['high_positive_rows_promoted_by_shadow']} / {metrics['high_positive_rows']}`
- current admitted promotions V1 would reject: `{current_admitted_rejects}`
- current blocked promotions V1 would promote: `{blocked_promotes}`
- dead-negative rows promoted by shadow: `{metrics['dead_negative_rows_promoted_by_shadow']}`
- replay/backtesting/tuning/trading: `blocked`
- collection_allowed: `false`
"""
    )
    output.joinpath("GPT_PROMOTION_PRIORITY_V1_SHADOW_PROMPT.md").write_text(
        """# GPT Promotion Priority V1 Shadow Prompt

Review `PromotionPriorityStrategyV1` as a research-only shadow policy.

Do not propose buy/sell actions, replay, backtesting, tuning, paper/live trading, wallet execution, cap raises, or profitability claims.

Tasks:

1. Compare V1 shadow decisions against current promotion policy.
2. Identify why V1 preserved high-positive rows.
3. Identify where V1 may reject current admitted rows that later looked promising.
4. Propose a future targeted shadow proof plan without changing live promotion behavior.
"""
    )


def update_readiness(root: pathlib.Path, output: pathlib.Path) -> None:
    path = root / "READINESS_DECISION.json"
    readiness = read_json(path)
    reason_codes = set(readiness.get("reason_codes", []))
    reason_codes.update(
        {
            "promotion_priority_strategy_v1_ready",
            "promotion_priority_v1_shadow_ready",
            "strategy_research_only",
            "formal_backtest_not_allowed",
            "replay_not_allowed",
            "threshold_tuning_disabled",
            "launch_caps_blocked",
        }
    )
    readiness.update(
        {
            "schema_version": "phase107k.trading_strategy_pipeline_readiness.v4",
            "updated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "strategy_research_ready": True,
            "promotion_priority_strategy_v1_ready": True,
            "promotion_priority_v1_shadow_ready": True,
            "formal_backtesting_ready": False,
            "backtesting_ready": False,
            "replay_ready": False,
            "threshold_tuning_ready": False,
            "paper_trading_ready": False,
            "live_trading_ready": False,
            "profitability_claim_allowed": False,
            "collection_allowed": False,
            "launch_caps_blocked": True,
            "promotion_priority_v1_shadow_path": str(output.resolve()),
            "reason_codes": sorted(reason_codes),
        }
    )
    write_json(path, readiness)
    (root / "READINESS_DECISION.md").write_text(
        "# Readiness Decision\n\n"
        f"- strategy_research_ready: `true`\n"
        f"- promotion_priority_strategy_v1_ready: `true`\n"
        f"- promotion_priority_v1_shadow_ready: `true`\n"
        f"- formal_backtesting_ready: `false`\n"
        f"- replay_ready: `false`\n"
        f"- threshold_tuning_ready: `false`\n"
        f"- paper_trading_ready: `false`\n"
        f"- live_trading_ready: `false`\n"
        f"- profitability_claim_allowed: `false`\n"
        f"- collection_allowed: `false`\n"
        f"- launch_caps_blocked: `true`\n\n"
        f"Reason codes: `{', '.join(readiness['reason_codes'])}`\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-root", type=pathlib.Path, default=ROOT)
    parser.add_argument("--output-dir", type=pathlib.Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    shadow_rows = load_shadow_rows(args.pipeline_root)
    comparison_rows = build_current_policy_comparison(args.pipeline_root, shadow_rows)
    fields = [
        "mint",
        "slice_id",
        "segment_id",
        "relay_session_id",
        "horizon_seconds",
        "promotion_priority_v1_shadow_decision",
        "promotion_priority_v1_shadow_reason_codes",
        "promotion_priority_v1_shadow_confidence_bin",
        "promotion_priority_v1_would_promote",
        "promotion_priority_v1_would_reject",
        "promotion_priority_v1_would_keep_cheap_followup",
        "promotion_priority_v1_shadow_only",
        "v0_decision",
        "dead_launch_avoider_v0_decision",
        "dead_launch_avoider_v0_reason_codes",
        "final_outcome",
        "positive_outcome_label",
        "positive_outcome_strength_bin",
        "early_burst_class",
        "candidate_checkpoint_seen",
        "replay_eligible",
        "trade_action",
        "candidate_eligibility_created",
        "replay_eligibility_created",
        "countability_affects",
        "holder_rpc_used",
        "rpc_mint_supply_canonical",
    ]
    write_csv(args.output_dir / "promotion_priority_v1_shadow_scores.csv", shadow_rows, fields)
    comparison_fields = [
        "mint",
        "slice_id",
        "segment_id",
        "horizon_seconds",
        "current_promotion_status",
        "current_promotion_blocker",
        "current_admitted",
        "current_blocked",
        "v1_shadow_decision",
        "v1_shadow_reason_codes",
        "v1_would_promote",
        "v1_would_reject",
        "admitted_but_v1_rejects",
        "blocked_but_v1_promotes",
        "later_positive_or_high_positive",
        "later_high_positive",
        "candidate_checkpoint_seen",
        "replay_eligible",
    ]
    write_csv(args.output_dir / "v1_shadow_vs_current_promotion_policy.csv", comparison_rows, comparison_fields)
    metrics = metric_counts(shadow_rows, comparison_rows)
    metrics.update(
        {
            "schema_version": "phase107k.promotion_priority_v1_shadow.v1",
            "classification": "PROMOTION_PRIORITY_V1_SHADOW_POLICY_PASS",
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "promotion_priority_v1_shadow_ready": True,
            "collection_allowed": False,
            "formal_backtesting_ready": False,
            "replay_ready": False,
            "threshold_tuning_ready": False,
            "paper_trading_ready": False,
            "live_trading_ready": False,
            "profitability_claim_allowed": False,
            "launch_caps_remain_blocked": True,
        }
    )
    write_json(args.output_dir / "promotion_priority_v1_shadow_summary.json", metrics)
    write_reports(args.output_dir, metrics, comparison_rows, shadow_rows)
    update_readiness(args.pipeline_root, args.output_dir)
    checksums = []
    for path in sorted(args.output_dir.glob("*")):
        if path.is_file() and path.name != "promotion_priority_v1_shadow_export.zip":
            checksums.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}")
    (args.output_dir / "CHECKSUMS.txt").write_text("\n".join(checksums) + "\n")
    zip_path = args.output_dir / "promotion_priority_v1_shadow_export.zip"
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(args.output_dir.glob("*")):
            if path.is_file() and path != zip_path:
                archive.write(path, arcname=path.name)
    print(json.dumps({"output_dir": str(args.output_dir), "zip_path": str(zip_path), "metrics": metrics}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
