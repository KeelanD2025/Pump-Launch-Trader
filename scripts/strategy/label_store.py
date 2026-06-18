from __future__ import annotations

import pathlib
from collections import Counter
from typing import Any

from .io import read_csv, write_json, write_text
from .schemas import STRATEGY_READINESS_ROOT, boolish


class LabelStore:
    def __init__(self, readiness_root: pathlib.Path = STRATEGY_READINESS_ROOT):
        self.root = pathlib.Path(readiness_root)
        self._labels: list[dict[str, str]] | None = None

    def load_mint_labels(self) -> list[dict[str, str]]:
        if self._labels is None:
            self._labels = read_csv(self.root / "mint_labels.csv")
        return self._labels

    def get_label(self, mint: str) -> dict[str, str] | None:
        for row in self.load_mint_labels():
            if row.get("mint") == mint:
                return row
        return None

    def clean_negatives(self) -> list[dict[str, str]]:
        return [row for row in self.load_mint_labels() if boolish(row.get("clean_negative_label"))]

    def clean_positives(self) -> list[dict[str, str]]:
        return [row for row in self.load_mint_labels() if boolish(row.get("clean_positive_label"))]

    def censored(self) -> list[dict[str, str]]:
        return [row for row in self.load_mint_labels() if boolish(row.get("censored_label"))]

    def replay_eligible(self) -> list[dict[str, str]]:
        return [row for row in self.load_mint_labels() if boolish(row.get("replay_eligible"))]

    def candidate_checkpoints(self) -> list[dict[str, str]]:
        return [row for row in self.load_mint_labels() if boolish(row.get("candidate_checkpoint_seen"))]

    def validate(self) -> dict[str, Any]:
        blockers: list[str] = []
        for row in self.load_mint_labels():
            mint = row.get("mint", "")
            if row.get("final_outcome") == "terminal_inconclusive" and boolish(row.get("clean_negative_label")):
                blockers.append(f"terminal_inconclusive_labeled_dead:{mint}")
            if boolish(row.get("candidate_checkpoint_seen")) and boolish(row.get("clean_positive_label")) and not boolish(row.get("replay_eligible")):
                blockers.append(f"checkpoint_positive_without_replay:{mint}")
            if boolish(row.get("replay_eligible")) and not boolish(row.get("clean_positive_label")):
                blockers.append(f"replay_eligible_not_clean_positive:{mint}")
            if boolish(row.get("provider_gap_exposed")) and boolish(row.get("clean_negative_label")):
                blockers.append(f"provider_gap_clean_label:{mint}")
        return {"passed": not blockers, "blockers": blockers, "rows_checked": len(self.load_mint_labels())}

    def summary(self) -> dict[str, Any]:
        labels = self.load_mint_labels()
        outcomes = Counter(row.get("final_outcome", "") or "missing" for row in labels)
        return {
            "total_labels": len(labels),
            "clean_negatives": len(self.clean_negatives()),
            "clean_positives": len(self.clean_positives()),
            "censored": len(self.censored()),
            "candidate_checkpoints": len(self.candidate_checkpoints()),
            "replay_eligible": len(self.replay_eligible()),
            "outcomes": dict(outcomes),
        }

    def write_report(self, output_root: pathlib.Path) -> None:
        validation = self.validate()
        summary = self.summary()
        write_json(output_root / "label_store_validation.json", validation)
        write_json(output_root / "label_store_summary.json", summary)
        write_text(
            output_root / "label_store_report.md",
            "\n".join([
                "# Label Store Report",
                "",
                f"- validation_passed: `{str(validation['passed']).lower()}`",
                f"- total_labels: `{summary['total_labels']}`",
                f"- clean_negatives: `{summary['clean_negatives']}`",
                f"- clean_positives: `{summary['clean_positives']}`",
                f"- censored: `{summary['censored']}`",
                f"- candidate_checkpoints: `{summary['candidate_checkpoints']}`",
                f"- replay_eligible: `{summary['replay_eligible']}`",
                "",
                "Terminal inconclusive rows are censored, not dead. Candidate checkpoints remain audit-only unless countability explicitly allows replay.",
            ]) + "\n",
        )

