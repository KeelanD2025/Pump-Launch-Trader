from __future__ import annotations

import pathlib
from typing import Any

from .io import read_csv
from .schemas import DATA_MART_ROOT, boolish


class PipelineLabelStore:
    def __init__(self, data_mart_root: pathlib.Path = DATA_MART_ROOT):
        self.root = pathlib.Path(data_mart_root)

    def load(self) -> list[dict[str, str]]:
        return read_csv(self.root / "strategy_labels.csv")

    def summary(self) -> dict[str, int]:
        rows = self.load()
        return {
            "total": len(rows),
            "clean_negative": sum(1 for row in rows if boolish(row.get("clean_negative_label"))),
            "censored": sum(1 for row in rows if boolish(row.get("censored_label"))),
            "clean_positive": sum(1 for row in rows if boolish(row.get("clean_positive_label"))),
            "candidate_checkpoint": sum(1 for row in rows if boolish(row.get("candidate_checkpoint_seen"))),
            "replay_eligible": sum(1 for row in rows if boolish(row.get("replay_eligible"))),
        }

    def validate(self) -> dict[str, Any]:
        blockers: list[str] = []
        for row in self.load():
            mint = row.get("mint", "")
            if row.get("final_outcome") == "terminal_inconclusive" and boolish(row.get("clean_negative_label")):
                blockers.append(f"terminal_inconclusive_labeled_dead:{mint}")
            if boolish(row.get("candidate_checkpoint_seen")) and boolish(row.get("clean_positive_label")) and not boolish(row.get("replay_eligible")):
                blockers.append(f"candidate_checkpoint_positive_without_replay:{mint}")
            if boolish(row.get("provider_gap_exposed")) and boolish(row.get("clean_negative_label")):
                blockers.append(f"provider_gap_clean_label:{mint}")
        return {"passed": not blockers, "blockers": blockers, "summary": self.summary()}
