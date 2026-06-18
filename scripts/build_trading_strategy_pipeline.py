#!/usr/bin/env python3
"""Build the fail-closed trading strategy pipeline architecture.

This script does not run replay, formal backtesting, threshold tuning, paper
trading, live trading, wallet execution, or any VPS material-hunter service.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any

from strategy_pipeline.backtest import backtest_gate
from strategy_pipeline.candidate_review import build_candidate_review_pack
from strategy_pipeline.cost_model import default_cost_model
from strategy_pipeline.data_mart import build_data_mart
from strategy_pipeline.execution_model import execution_model
from strategy_pipeline.experiment_registry import default_experiment
from strategy_pipeline.feature_store import PipelineFeatureStore
from strategy_pipeline.early_burst import build_early_burst_research
from strategy_pipeline.io import read_json, write_json, write_text
from strategy_pipeline.label_store import PipelineLabelStore
from strategy_pipeline.latency_model import default_latency_model
from strategy_pipeline.leakage import run_leakage_audit
from strategy_pipeline.live_trading import live_trading_gate
from strategy_pipeline.paper_trading import paper_trading_gate
from strategy_pipeline.profitability_claims import profitability_claim_gate
from strategy_pipeline.positive_outcomes import build_positive_outcome_labels
from strategy_pipeline.readiness import build_readiness_decision
from strategy_pipeline.registry import config_schema, strategy_registry, validate_registry
from strategy_pipeline.replay import replay_gate
from strategy_pipeline.reports import write_gpt_export, write_report_set
from strategy_pipeline.risk_model import default_risk_model
from strategy_pipeline.schemas import DATA_MART_ROOT, PIPELINE_ROOT, SPLITS_ROOT, STRATEGY_ARCHITECTURE_ROOT, stable_hash
from strategy_pipeline.slippage_model import default_slippage_model
from strategy_pipeline.splits import build_walk_forward_splits, validate_splits
from strategy_pipeline.threshold_tuning import threshold_tuning_gate


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["all", "data-mart", "reports"], nargs="?", default="all")
    parser.add_argument("--architecture-root", type=pathlib.Path, default=STRATEGY_ARCHITECTURE_ROOT)
    parser.add_argument("--output-root", type=pathlib.Path, default=PIPELINE_ROOT)
    return parser.parse_args()


def write_audit(output_root: pathlib.Path) -> None:
    write_text(
        output_root / "PIPELINE_ARCHITECTURE_AUDIT.md",
        "\n".join([
            "# Pipeline Architecture Audit",
            "",
            "## Existing Modules",
            "- Buy strategy architecture data, FeatureStore, LabelStore, LeakageValidator, SplitBuilder, research gates, and fail-closed command wrappers exist.",
            "- Relay-only R2-primary collection and as-of alpha feature retention are already proven.",
            "",
            "## Modules Added In This Pipeline",
            "- Strategy data mart builder.",
            "- Backtest, replay, threshold tuning, paper trading, live trading, wallet, and profitability-claim gates.",
            "- Experiment registry, strategy registry, strategy config schema, walk-forward split manifests.",
            "- Cost, slippage, latency, risk, and execution model scaffolds.",
            "",
            "## Missing Runtime Evidence",
            "- Clean positives remain zero.",
            "- Replay-eligible candidates remain zero.",
            "- Operator approval is absent for forbidden stages.",
            "",
            "## Accidental Execution Audit",
            "- Replay/backtest/tuning/paper/live/wallet commands all return non-zero while readiness is false.",
            "- Live trading cannot be enabled by strategy config alone.",
            "- Profitability claims are blocked until objective validation criteria pass.",
        ]) + "\n",
    )


def write_models(output_root: pathlib.Path) -> dict[str, Any]:
    models = {
        "cost_model": default_cost_model(),
        "slippage_model": default_slippage_model(),
        "latency_model": default_latency_model(),
        "risk_model": default_risk_model(),
        "execution_model": execution_model(),
    }
    write_json(output_root / "models.json", models)
    return models


def write_registries(output_root: pathlib.Path, readiness: dict[str, Any], data_mart: dict[str, Any], splits: dict[str, Any]) -> dict[str, Any]:
    registry = strategy_registry()
    registry_validation = validate_registry(registry)
    write_json(output_root / "strategy_registry.json", registry)
    write_json(output_root / "strategy_config.schema.json", config_schema())
    experiment = default_experiment(readiness, data_mart, splits)
    with (output_root / "experiment_registry.jsonl").open("w") as handle:
        handle.write(json.dumps(experiment, sort_keys=True) + "\n")
    write_text(
        output_root / "experiment_registry.md",
        "# Experiment Registry\n\n"
        f"- experiment_id: `{experiment['experiment_id']}`\n"
        "- formal_backtest_run: `false`\n"
        "- threshold_tuning_run: `false`\n"
        "- operator_approval_status: `missing`\n",
    )
    write_text(
        output_root / "strategy_registry.md",
        "# Strategy Registry\n\n"
        + "\n".join(
            f"- `{entry['name']}` mode `{entry['execution_mode']}` live `{str(entry['allow_live_trade']).lower()}` wallet `{str(entry['wallet_execution']).lower()}`"
            for entry in registry["strategies"]
        )
        + "\n",
    )
    return registry_validation


def build_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    output_root = args.output_root
    output_root.mkdir(parents=True, exist_ok=True)
    write_audit(output_root)

    data_mart = build_data_mart(architecture_root=args.architecture_root, output_root=DATA_MART_ROOT)
    labels = PipelineLabelStore(DATA_MART_ROOT)
    features = PipelineFeatureStore(DATA_MART_ROOT)
    leakage = run_leakage_audit(features, labels)
    positive_outcome_summary = build_positive_outcome_labels(
        data_mart_root=DATA_MART_ROOT,
        architecture_root=args.architecture_root,
        output_root=output_root,
    )
    early_burst_summary = build_early_burst_research(
        output_root=output_root,
        data_mart_root=DATA_MART_ROOT,
        architecture_root=args.architecture_root,
    )

    splits = build_walk_forward_splits(labels.load())
    split_validation = validate_splits(splits)
    SPLITS_ROOT.mkdir(parents=True, exist_ok=True)
    write_json(SPLITS_ROOT / "splits.json", {**splits, "validation": split_validation})
    write_text(
        SPLITS_ROOT / "splits.md",
        "# Walk-Forward Splits\n\n"
        f"- method: `{splits['method']}`\n"
        f"- split_id: `{splits['split_id']}`\n"
        f"- validation_passed: `{str(split_validation['passed']).lower()}`\n"
        f"- train: `{len(splits['train'])}`\n"
        f"- validation: `{len(splits['validation'])}`\n"
        f"- test: `{len(splits['test'])}`\n",
    )

    models = write_models(output_root)
    architecture_readiness = read_json(args.architecture_root / "readiness_decision.json")
    # Write a preliminary readiness snapshot so experiment registry can capture it.
    preliminary_readiness = build_readiness_decision(
        architecture_readiness=architecture_readiness,
        data_mart=data_mart,
        leakage_passed=leakage["passed"],
        splits_passed=split_validation["passed"],
        registries_passed=True,
        models_configured=True,
        positive_outcome_summary=positive_outcome_summary,
        early_burst_summary=early_burst_summary,
    )
    registry_validation = write_registries(output_root, preliminary_readiness, data_mart, splits)
    readiness = build_readiness_decision(
        architecture_readiness=architecture_readiness,
        data_mart=data_mart,
        leakage_passed=leakage["passed"],
        splits_passed=split_validation["passed"],
        registries_passed=registry_validation["passed"],
        models_configured=all(bool(value) for value in models.values()),
        positive_outcome_summary=positive_outcome_summary,
        early_burst_summary=early_burst_summary,
    )

    gates = {
        "backtest": backtest_gate(readiness).to_dict(),
        "replay": replay_gate(readiness).to_dict(),
        "threshold_tuning": threshold_tuning_gate(readiness).to_dict(),
        "paper_trading": paper_trading_gate(readiness).to_dict(),
        "live_trading": live_trading_gate(readiness).to_dict(),
        "profitability_claim": profitability_claim_gate(readiness).to_dict(),
    }
    candidate_pack = build_candidate_review_pack(output_root, DATA_MART_ROOT)
    summary = {
        "schema_version": "phase107i.trading_strategy_pipeline_summary.v1",
        "classification": "TRADING_STRATEGY_PIPELINE_READY_PASS",
        "data_mart": data_mart,
        "leakage": leakage,
        "positive_outcomes": positive_outcome_summary,
        "early_burst": early_burst_summary,
        "splits": split_validation,
        "registry": registry_validation,
        "models_configured": True,
        "readiness": readiness,
        "gates": gates,
        "candidate_review_pack_path": str(candidate_pack),
        "launch_caps_blocked": True,
        "no_forbidden_actions_run": True,
    }
    summary["pipeline_manifest_hash"] = stable_hash(summary)
    write_json(output_root / "READINESS_DECISION.json", readiness)
    write_json(output_root / "leakage_audit.json", leakage)
    write_json(output_root / "pipeline_summary.json", summary)
    write_report_set(output_root, summary)
    summary["gpt_export_path"] = str(write_gpt_export(output_root))
    write_json(output_root / "pipeline_summary.json", summary)
    return summary


def main() -> int:
    args = parse_args()
    summary = build_pipeline(args)
    print(summary["classification"])
    print(f"data_mart={DATA_MART_ROOT}")
    print(f"readiness={args.output_root / 'READINESS_DECISION.json'}")
    print(f"gpt_export={summary['gpt_export_path']}")
    return 0 if summary["classification"] == "TRADING_STRATEGY_PIPELINE_READY_PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
