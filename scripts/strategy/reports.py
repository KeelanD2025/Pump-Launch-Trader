from __future__ import annotations

import hashlib
import pathlib
import zipfile
from typing import Any

from .io import write_json, write_text


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_checksums(root: pathlib.Path) -> None:
    lines = []
    for path in sorted(p for p in root.rglob("*") if p.is_file() and p.name != "checksums.txt"):
        lines.append(f"{sha256_file(path)}  {path.relative_to(root)}")
    write_text(root / "checksums.txt", "\n".join(lines) + "\n")


def write_gpt_export(root: pathlib.Path, summary: dict[str, Any]) -> pathlib.Path:
    context = root / "gpt_buy_strategy_architecture_context.md"
    prompt = root / "gpt_buy_strategy_architecture_prompt.md"
    write_text(
        context,
        "\n".join([
            "# GPT Buy Strategy Architecture Context",
            "",
            f"- strategy_research_ready: `{str(summary.get('strategy_research_ready')).lower()}`",
            f"- buy_strategy_architecture_ready: `{str(summary.get('buy_strategy_architecture_ready')).lower()}`",
            f"- clean_positive_count: `{summary.get('clean_positive_count', 0)}`",
            f"- replay_eligible_candidate_count: `{summary.get('replay_eligible_candidate_count', 0)}`",
            "- available_feature_groups: `launch`, `trade_delta`, `holder_state`, `vault_curve`, `high_throughput`, `data_quality`, `gate_diagnostics`",
            "- blocked: replay, formal backtesting, threshold tuning, paper trading, live trading, wallet execution",
            "",
            "Use this context to discuss robust architecture, hypotheses, gates, and validation plans only.",
        ]) + "\n",
    )
    write_text(
        prompt,
        "# GPT Prompt\n\n"
        "Design research-only buy strategy hypotheses from the attached architecture outputs. "
        "Do not claim profitability. Do not tune thresholds. Do not run or request backtesting, replay, or live trade entries. "
        "Focus on leakage-safe features, gates, missing evidence, and validation plans.\n",
    )
    write_checksums(root)
    zip_path = root / "buy_strategy_architecture_export.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(p for p in root.rglob("*") if p.is_file() and p != zip_path):
            if any(secret in path.name.lower() for secret in ("env", "secret", "key", "credential")):
                continue
            archive.write(path, path.relative_to(root))
    return zip_path


def write_architecture_summary(root: pathlib.Path, summary: dict[str, Any]) -> None:
    write_json(root / "architecture_summary.json", summary)

