#!/usr/bin/env python3
"""Build the research-only buy strategy architecture artifacts.

This command does not run replay, formal backtesting, threshold tuning, paper
trading, live trading, wallet execution, or any VPS material-hunter service.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
from collections import Counter, defaultdict
from typing import Any

from strategy.buy_setup import BuySetupDraft
from strategy.candidate_review import build_candidate_review_pack
from strategy.data_quality import included_counted_slice
from strategy.feature_store import FeatureStore
from strategy.gates import CandidateEligibilityGateV2, ContinueTrackingGateV1, EarlyAvoidFilterV1
from strategy.io import read_csv, read_json, write_csv, write_json, write_text
from strategy.label_store import LabelStore
from strategy.leakage import leakage_audit
from strategy.readiness import readiness_decision
from strategy.registry import list_strategies, validate_strategy_config
from strategy.reports import write_architecture_summary, write_gpt_export
from strategy.risk_exit import RiskAndExitDraft
from strategy.schemas import HORIZONS, REPO_ROOT, SIGNAL_FIELDS, STRATEGY_ARCHITECTURE_ROOT, STRATEGY_READINESS_ROOT, boolish
from strategy.splits import build_chronological_splits, validate_splits


BUY_QUALITY_ROOT_NAME = "buy_quality_dataset"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["all", "dataset", "reports"], nargs="?", default="all")
    parser.add_argument("--readiness-root", type=pathlib.Path, default=STRATEGY_READINESS_ROOT)
    parser.add_argument("--output-root", type=pathlib.Path, default=STRATEGY_ARCHITECTURE_ROOT)
    return parser.parse_args()


def write_architecture_audit(output_root: pathlib.Path) -> None:
    write_text(
        output_root / "ARCHITECTURE_AUDIT.md",
        "\n".join([
            "# Architecture Audit",
            "",
            "## Existing Strategy Pieces",
            "- `scripts/build_strategy_readiness.py` builds inventories, labels, as-of alpha reports, readiness reports, and research-only v0/v1/v2 score CSVs.",
            "- `scripts/run_relay_r2_primary_batch.py` supervises relay/local R2-primary collection and stops before replay/candidate review.",
            "- `research_output/strategy_readiness/` contains reports and CSV/JSON artifacts, not a reusable strategy engine.",
            "",
            "## Converted Into Reusable Code",
            "- `scripts/strategy/feature_store.py`: as-of alpha feature access and safety validation.",
            "- `scripts/strategy/label_store.py`: clean negative/positive/censored/replay label handling.",
            "- `scripts/strategy/gates.py`: EarlyAvoidFilter v1, ContinueTrackingGate v1, CandidateEligibilityGate v2.",
            "- `scripts/strategy/buy_setup.py` and `risk_exit.py`: disabled-by-default draft engines.",
            "- `scripts/strategy/registry.py`: strategy registry and execution-mode block.",
            "- `scripts/strategy/readiness.py`: strategy/backtest/replay/paper/live readiness gates.",
            "",
            "## Still Blocked By Design",
            "- Replay: blocked until countability explicitly allows replay and replay-eligible candidates exist.",
            "- Formal backtesting: blocked while clean positives and replay-eligible candidates are zero.",
            "- Threshold tuning, paper trading, live trading, and wallet execution: disabled at this stage.",
            "",
            "## Commands Added",
            "- `python scripts/build_buy_strategy_architecture.py all`",
            "- `python scripts/run_strategy_backtest.py --strategy <name>`",
            "- `python scripts/run_strategy_replay.py`",
            "- `python scripts/run_strategy_threshold_tuning.py`",
            "- `python scripts/run_strategy_paper.py`",
            "- `python scripts/run_strategy_live.py`",
            "- `python scripts/build_candidate_review_pack.py`",
            "",
            "## Tests Added",
            "- `python scripts/test_strategy_architecture.py` proves safety gates, stores, registry, harness blocks, and GPT export secret hygiene.",
        ]) + "\n",
    )


def included_slice_ids(readiness_root: pathlib.Path) -> tuple[set[str], list[dict[str, str]], list[dict[str, str]]]:
    inventory = read_csv(readiness_root / "dataset_inventory.csv")
    included = [row for row in inventory if included_counted_slice(row)]
    return {row.get("slice_id", "") for row in included}, included, inventory


def build_buy_quality_dataset(readiness_root: pathlib.Path, output_root: pathlib.Path) -> dict[str, Any]:
    dataset_root = output_root / BUY_QUALITY_ROOT_NAME
    slice_ids, included, inventory = included_slice_ids(readiness_root)
    labels = [row for row in read_csv(readiness_root / "mint_labels.csv") if row.get("slice_id") in slice_ids]
    write_csv(dataset_root / "buy_quality_mint_table.csv", labels, list(labels[0].keys()) if labels else ["mint"])

    alpha_counts: dict[str, int] = {}
    feature_files: list[dict[str, Any]] = []
    for horizon in HORIZONS:
        rows = [row for row in read_csv(readiness_root / "asof_alpha_features" / f"asof_alpha_features_{horizon:03d}s.csv") if row.get("slice_id") in slice_ids]
        alpha_counts[f"{horizon}s"] = len(rows)
        fields = list(rows[0].keys()) if rows else ["mint", "slice_id", "horizon_seconds"]
        path = dataset_root / f"buy_quality_asof_features_{horizon:03d}s.csv"
        write_csv(path, rows, fields)
        feature_files.append({"horizon_seconds": horizon, "path": str(path), "rows": len(rows)})

    label_summary = Counter()
    for row in labels:
        if boolish(row.get("clean_negative_label")):
            label_summary["clean_negative"] += 1
        if boolish(row.get("clean_positive_label")):
            label_summary["clean_positive"] += 1
        if boolish(row.get("censored_label")):
            label_summary["censored"] += 1
        if boolish(row.get("candidate_checkpoint_seen")):
            label_summary["candidate_checkpoint"] += 1
        if boolish(row.get("replay_eligible")):
            label_summary["replay_eligible"] += 1

    write_json(dataset_root / "buy_quality_feature_manifest.json", {"files": feature_files, "horizon_counts": alpha_counts})
    write_json(dataset_root / "buy_quality_label_manifest.json", {"label_counts": dict(label_summary), "rows": len(labels)})
    write_json(dataset_root / "buy_quality_data_quality_manifest.json", {
        "included_slices": len(included),
        "excluded_slices": len(inventory) - len(included),
        "rules": [
            "counted_phase107b_result=true",
            "R2 verified",
            "artifact consistency passed",
            "zero sequence/hash/receiver blockers",
            "holder RPC disabled",
            "RPC mint supply non-canonical",
            "replay/backtesting/tuning/trading disabled",
        ],
    })
    write_text(
        dataset_root / "buy_quality_dataset_summary.md",
        "\n".join([
            "# Buy Quality Dataset Summary",
            "",
            f"- included_slices: `{len(included)}`",
            f"- mint_rows: `{len(labels)}`",
            f"- clean_negatives: `{label_summary['clean_negative']}`",
            f"- clean_positives: `{label_summary['clean_positive']}`",
            f"- censored: `{label_summary['censored']}`",
            f"- candidate_checkpoints: `{label_summary['candidate_checkpoint']}`",
            f"- replay_eligible: `{label_summary['replay_eligible']}`",
            "",
            "Labels are separate from features. Terminal inconclusive rows are censored, not dead. Candidate checkpoints are audit-only.",
        ]) + "\n",
    )
    return {
        "included_slices": len(included),
        "excluded_slices": len(inventory) - len(included),
        "mint_rows": len(labels),
        "label_counts": dict(label_summary),
        "alpha_counts": alpha_counts,
    }


def labels_by_mint(label_store: LabelStore) -> dict[str, dict[str, str]]:
    return {row.get("mint", ""): row for row in label_store.load_mint_labels()}


def run_research_gates(readiness_root: pathlib.Path, output_root: pathlib.Path) -> dict[str, Any]:
    label_store = LabelStore(readiness_root)
    label_map = labels_by_mint(label_store)
    feature_store = FeatureStore(readiness_root)
    rows_60 = feature_store.load_asof_features(60)
    early = EarlyAvoidFilterV1()
    continue_gate = ContinueTrackingGateV1()
    candidate = CandidateEligibilityGateV2()
    early_rows = [early.score(row).to_row() for row in rows_60]
    continue_rows = [continue_gate.score(row).to_row() for row in rows_60]
    candidate_outputs = [candidate.score(row, label_map.get(row.get("mint", ""), {})) for row in rows_60]
    candidate_rows = [output.to_row() for output in candidate_outputs]
    write_csv(output_root / "early_avoid_filter_v1_scores.csv", early_rows, SIGNAL_FIELDS)
    write_csv(output_root / "continue_tracking_gate_v1_scores.csv", continue_rows, SIGNAL_FIELDS)
    write_csv(output_root / "candidate_eligibility_v2_scores.csv", candidate_rows, SIGNAL_FIELDS)

    def report(name: str, rows: list[dict[str, Any]], path: pathlib.Path) -> None:
        decisions = Counter(row.get("decision", "") for row in rows)
        reasons = Counter(reason for row in rows for reason in str(row.get("reason_codes", "")).split("|") if reason)
        write_text(
            path,
            "\n".join([
                f"# {name}",
                "",
                f"- rows: `{len(rows)}`",
                f"- decisions: `{dict(decisions)}`",
                f"- top_reason_codes: `{dict(reasons.most_common(12))}`",
                "- trade_actions: `none`",
            ]) + "\n",
        )

    report("EarlyAvoidFilter v1 Report", early_rows, output_root / "early_avoid_filter_v1_report.md")
    report("ContinueTrackingGate v1 Report", continue_rows, output_root / "continue_tracking_gate_v1_report.md")
    report("CandidateEligibilityGate v2 Report", candidate_rows, output_root / "candidate_eligibility_v2_report.md")
    return {"early_rows": len(early_rows), "continue_rows": len(continue_rows), "candidate_rows": len(candidate_rows)}


def write_buy_setup_and_risk(output_root: pathlib.Path) -> dict[str, Any]:
    gate_rows = read_csv(output_root / "candidate_eligibility_v2_scores.csv")
    buy = BuySetupDraft()
    buy_rows = [
        buy.score(row.get("mint", ""), row.get("decision", ""), str(row.get("reason_codes", "")).split("|"))
        for row in gate_rows
    ]
    buy_fields = ["mint", "setup_decision", "disabled_by_default", "trade_action", "gate_decision", "reason_codes", "required_evidence_before_activation", "blocked_actions"]
    write_csv(output_root / "buy_setup_draft_v0_scores.csv", buy_rows, buy_fields)
    write_text(
        output_root / "buy_setup_draft_v0_report.md",
        "# BuySetupDraft v0 Report\n\n"
        "- disabled_by_default: `true`\n"
        "- emitted_trade_actions: `0`\n"
        "- allowed_output: `candidate_setup_only`\n"
        "- blocked_actions: `replay`, `backtesting`, `threshold_tuning`, `live_trading`, `wallet_execution`\n",
    )
    risk = RiskAndExitDraft().describe()
    write_json(output_root / "risk_exit_draft_v0.json", risk)
    write_text(
        output_root / "risk_exit_draft_v0_report.md",
        "# RiskAndExitDraft v0 Report\n\n"
        "- disabled_by_default: `true`\n"
        "- emits_orders: `false`\n"
        "- wallet_execution_enabled: `false`\n"
        f"- invalidation_rules: `{', '.join(risk['invalidation_rules'])}`\n"
        "- slippage/latency model: `TODO after paper-trading readiness gate`\n",
    )
    return {"buy_setup_rows": len(buy_rows), "risk_exit_disabled": True}


def write_registry(output_root: pathlib.Path) -> dict[str, Any]:
    entries = list_strategies()
    validation = {entry["name"]: validate_strategy_config(entry) for entry in entries}
    write_json(output_root / "strategy_registry.json", {"entries": entries, "validation": validation})
    write_text(
        output_root / "strategy_registry.md",
        "# Strategy Registry\n\n"
        + "\n".join(f"- `{entry['name']}`: mode `{entry['mode']}`, execution_enabled `{str(entry['execution_enabled']).lower()}`" for entry in entries)
        + "\n",
    )
    return {"strategy_count": len(entries), "all_configs_valid": all(item["passed"] for item in validation.values())}


def write_readiness(output_root: pathlib.Path, label_store: LabelStore, leakage_passed: bool, modules_exist: bool) -> dict[str, Any]:
    decision = readiness_decision(label_summary=label_store.summary(), leakage_passed=leakage_passed, modules_exist=modules_exist)
    write_json(output_root / "readiness_decision.json", decision)
    write_text(
        output_root / "readiness_decision.md",
        "\n".join([
            "# Readiness Decision",
            "",
            f"- strategy_research_ready: `{str(decision['strategy_research_ready']).lower()}`",
            f"- buy_strategy_architecture_ready: `{str(decision['buy_strategy_architecture_ready']).lower()}`",
            f"- backtesting_ready: `{str(decision['backtesting_ready']).lower()}`",
            f"- replay_ready: `{str(decision['replay_ready']).lower()}`",
            f"- threshold_tuning_ready: `{str(decision['threshold_tuning_ready']).lower()}`",
            f"- paper_trading_ready: `{str(decision['paper_trading_ready']).lower()}`",
            f"- live_trading_ready: `{str(decision['live_trading_ready']).lower()}`",
            f"- reason_codes: `{', '.join(decision['reason_codes'])}`",
        ]) + "\n",
    )
    return decision


def write_harness_gate_reports(output_root: pathlib.Path, decision: dict[str, Any]) -> None:
    for name, blocker in [
        ("backtest_gate_result.json", "BACKTESTING_BLOCKED_BY_READINESS_GATE"),
        ("replay_gate_result.json", "REPLAY_BLOCKED_BY_READINESS_GATE"),
        ("paper_gate_result.json", "PAPER_TRADING_DISABLED"),
        ("live_gate_result.json", "LIVE_TRADING_DISABLED"),
        ("threshold_tuning_gate_result.json", "THRESHOLD_TUNING_DISABLED"),
    ]:
        write_json(output_root / name, {"allowed": False, "blocker": blocker, "readiness": decision})


def write_main_report(output_root: pathlib.Path, summary: dict[str, Any]) -> None:
    write_text(
        output_root / "BUY_STRATEGY_ARCHITECTURE_REPORT.md",
        "\n".join([
            "# Buy Strategy Architecture Report",
            "",
            f"- classification: `{summary['classification']}`",
            f"- included_slices: `{summary['dataset']['included_slices']}`",
            f"- mint_rows: `{summary['dataset']['mint_rows']}`",
            f"- clean_positives: `{summary['readiness']['clean_positive_count']}`",
            f"- replay_eligible_candidates: `{summary['readiness']['replay_eligible_candidate_count']}`",
            f"- strategy_research_ready: `{str(summary['readiness']['strategy_research_ready']).lower()}`",
            f"- buy_strategy_architecture_ready: `{str(summary['readiness']['buy_strategy_architecture_ready']).lower()}`",
            f"- backtesting_ready: `{str(summary['readiness']['backtesting_ready']).lower()}`",
            f"- replay_ready: `{str(summary['readiness']['replay_ready']).lower()}`",
            f"- live_trading_ready: `{str(summary['readiness']['live_trading_ready']).lower()}`",
            "",
            "## Modules",
            "- FeatureStore, LabelStore, LeakageValidator, SplitBuilder",
            "- EarlyAvoidFilter v1, ContinueTrackingGate v1, CandidateEligibilityGate v2",
            "- BuySetupDraft v0 and RiskAndExitDraft v0 are disabled by default.",
            "- Backtest, replay, paper, live, wallet, and threshold-tuning harnesses fail closed.",
            "",
            "## Current Blockers",
            "- No clean positives.",
            "- No replay-eligible candidates.",
            "- Operator approval missing for any future threshold tuning or execution stages.",
            "",
            "No live trading is enabled.",
        ]) + "\n",
    )


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_root = args.output_root
    readiness_root = args.readiness_root
    output_root.mkdir(parents=True, exist_ok=True)
    write_architecture_audit(output_root)
    dataset_summary = build_buy_quality_dataset(readiness_root, output_root)

    feature_store = FeatureStore(readiness_root)
    label_store = LabelStore(readiness_root)
    feature_store.write_reports(output_root)
    label_store.write_report(output_root)

    leakage = leakage_audit(feature_store, label_store)
    write_json(output_root / "leakage_audit.json", leakage)
    write_text(output_root / "leakage_audit.md", "# Leakage Audit\n\n" f"- passed: `{str(leakage['passed']).lower()}`\n" f"- blockers: `{leakage['blockers']}`\n")

    splits = build_chronological_splits(label_store.load_mint_labels())
    split_validation = validate_splits(splits)
    write_json(output_root / "splits.json", splits | {"validation": split_validation})
    write_text(output_root / "splits.md", "# Splits\n\n- method: `chronological_walk_forward`\n- validation_passed: `" + str(split_validation["passed"]).lower() + "`\n")

    gate_summary = run_research_gates(readiness_root, output_root)
    draft_summary = write_buy_setup_and_risk(output_root)
    registry_summary = write_registry(output_root)
    decision = write_readiness(output_root, label_store, leakage["passed"], registry_summary["all_configs_valid"])
    write_harness_gate_reports(output_root, decision)

    alpha_by_horizon = {horizon: feature_store.load_asof_features(horizon) for horizon in HORIZONS}
    candidate_pack = build_candidate_review_pack(output_root, label_store.load_mint_labels(), alpha_by_horizon, read_csv(output_root / "candidate_eligibility_v2_scores.csv"))

    classification = "BUY_STRATEGY_ARCHITECTURE_READY_PASS" if decision["buy_strategy_architecture_ready"] and leakage["passed"] else "BUY_STRATEGY_ARCHITECTURE_BLOCK"
    summary = {
        "classification": classification,
        "dataset": dataset_summary,
        "feature_store_validation": read_json(output_root / "feature_store_validation.json"),
        "label_store_validation": read_json(output_root / "label_store_validation.json"),
        "leakage": leakage,
        "gates": gate_summary,
        "drafts": draft_summary,
        "registry": registry_summary,
        "readiness": decision,
        "candidate_review_pack_path": str(candidate_pack),
        "live_trading_enabled": False,
        "wallet_execution_enabled": False,
        "launch_caps_blocked": True,
    }
    write_main_report(output_root, summary)
    write_architecture_summary(output_root, summary)
    zip_path = write_gpt_export(output_root, decision)
    summary["gpt_export_path"] = str(zip_path)
    write_json(output_root / "architecture_summary.json", summary)
    return summary


def main() -> int:
    args = parse_args()
    summary = run(args)
    print(summary["classification"])
    print(f"architecture_report={args.output_root / 'BUY_STRATEGY_ARCHITECTURE_REPORT.md'}")
    print(f"readiness_decision={args.output_root / 'readiness_decision.json'}")
    print(f"gpt_export={summary['gpt_export_path']}")
    return 0 if summary["classification"] == "BUY_STRATEGY_ARCHITECTURE_READY_PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
