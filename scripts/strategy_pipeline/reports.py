from __future__ import annotations

import pathlib
import zipfile
from typing import Any

from .io import write_json, write_text
from .schemas import PIPELINE_ROOT, file_sha256


def write_checksums(root: pathlib.Path) -> None:
    lines = []
    for path in sorted(p for p in root.rglob("*") if p.is_file() and p.name != "checksums.txt"):
        lines.append(f"{file_sha256(path)}  {path.relative_to(root)}")
    write_text(root / "checksums.txt", "\n".join(lines) + "\n")


def write_report_set(root: pathlib.Path, summary: dict[str, Any]) -> None:
    reports = {
        "TRADING_STRATEGY_PIPELINE_REPORT.md": [
            "# Trading Strategy Pipeline Report",
            "",
            f"- classification: `{summary['classification']}`",
            f"- data_mart_rows: `{summary['readiness']['data_mart_rows']}`",
            f"- positive_outcome_research_ready: `{str(summary['readiness'].get('positive_outcome_research_ready')).lower()}`",
            f"- positive_outcomes: `{summary['readiness'].get('positive_outcome_count', 0)}`",
            f"- high_positive_outcomes: `{summary['readiness'].get('high_positive_outcome_count', 0)}`",
            f"- early_burst_strategy_research_ready: `{str(summary['readiness'].get('early_burst_strategy_research_ready')).lower()}`",
            f"- backtesting_ready: `{str(summary['readiness']['backtesting_ready']).lower()}`",
            f"- replay_ready: `{str(summary['readiness']['replay_ready']).lower()}`",
            f"- profitability_claim_allowed: `{str(summary['readiness']['profitability_claim_allowed']).lower()}`",
            "- no replay/backtesting/tuning/paper/live/wallet execution was run.",
        ],
        "BACKTEST_HARNESS_REPORT.md": [
            "# Backtest Harness Report",
            "",
            "- architecture: `present`",
            f"- blocker: `{summary['gates']['backtest']['blocker']}`",
            "- formal_performance_evaluation_run: `false`",
        ],
        "REPLAY_HARNESS_REPORT.md": [
            "# Replay Harness Report",
            "",
            "- architecture: `present`",
            f"- blocker: `{summary['gates']['replay']['blocker']}`",
            "- replay_run: `false`",
        ],
        "THRESHOLD_TUNING_HARNESS_REPORT.md": [
            "# Threshold Tuning Harness Report",
            "",
            "- architecture: `present`",
            f"- blocker: `{summary['gates']['threshold_tuning']['blocker']}`",
            "- threshold_tuning_run: `false`",
        ],
        "PAPER_TRADING_HARNESS_REPORT.md": [
            "# Paper Trading Harness Report",
            "",
            "- dry_run_order_model: `architected_but_disabled`",
            f"- blocker: `{summary['gates']['paper_trading']['blocker']}`",
            "- real_private_keys_loaded: `false`",
        ],
        "LIVE_TRADING_GATE_REPORT.md": [
            "# Live Trading Gate Report",
            "",
            "- live_trading_hard_disabled: `true`",
            f"- blocker: `{summary['gates']['live_trading']['blocker']}`",
            "- real_orders_sent: `false`",
        ],
        "PROFITABILITY_CLAIM_GATE_REPORT.md": [
            "# Profitability Claim Gate Report",
            "",
            f"- blocker: `{summary['gates']['profitability_claim']['blocker']}`",
            "- profitability_claim_allowed: `false`",
            "- allowed_language: `hypothesis`, `research-only`, `needs validation`",
            "- forbidden_language: `proven profitable`, `validated profitable`, `buy signal`, `guaranteed edge`, `live-ready`",
        ],
        "READINESS_DECISION.md": [
            "# Readiness Decision",
            "",
            *[f"- {key}: `{str(value).lower() if isinstance(value, bool) else value}`" for key, value in summary["readiness"].items() if key != "reason_codes"],
            f"- reason_codes: `{', '.join(summary['readiness']['reason_codes'])}`",
        ],
    }
    for name, lines in reports.items():
        write_text(root / name, "\n".join(lines) + "\n")
    write_json(root / "READINESS_DECISION.json", summary["readiness"])
    write_json(root / "pipeline_summary.json", summary)


def write_gpt_export(root: pathlib.Path = PIPELINE_ROOT) -> pathlib.Path:
    context = root / "trading_strategy_pipeline_context.md"
    prompt = root / "trading_strategy_pipeline_prompt.md"
    write_text(
        context,
        "# Trading Strategy Pipeline Context\n\n"
        "- Relay-only R2-primary collection and buy strategy architecture are ready.\n"
        "- This export contains gated architecture for future backtest/replay/tuning/paper/live stages.\n"
        "- Current gates block replay, formal backtesting, threshold tuning, paper trading, live trading, wallet execution, and profitability claims.\n"
        "- No raw relay frames, secrets, env files, SSH config, R2 credentials, or private keys are included.\n",
    )
    write_text(
        prompt,
        "# GPT Pipeline Prompt\n\n"
        "Review the trading-strategy pipeline architecture. Discuss only research-safe next steps, evidence required to unlock gates, and anti-overfit controls. Do not claim profitability or produce trade entries.\n",
    )
    write_checksums(root)
    zip_path = root / "trading_strategy_pipeline_export.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(p for p in root.rglob("*") if p.is_file() and p != zip_path):
            parts = [part.lower() for part in path.parts]
            if any(token in path.name.lower() for token in ("secret", "credential", "private", ".env")):
                continue
            if ".codex_runtime_env" in parts:
                continue
            archive.write(path, path.relative_to(root))
    return zip_path
