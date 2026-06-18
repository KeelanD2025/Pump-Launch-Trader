from __future__ import annotations

import pathlib
from datetime import datetime
from typing import Any

from .io import read_csv
from .schemas import DATA_MART_ROOT, FORBIDDEN_ALPHA_COLUMNS, HORIZONS, boolish


class PipelineFeatureStore:
    def __init__(self, data_mart_root: pathlib.Path = DATA_MART_ROOT):
        self.root = pathlib.Path(data_mart_root)

    def load(self, horizon: int) -> list[dict[str, str]]:
        return read_csv(self.root / f"strategy_asof_features_{horizon:03d}s.csv")

    def validate(self) -> dict[str, Any]:
        blockers: list[str] = []
        rows_checked = 0
        for horizon in HORIZONS:
            for row in self.load(horizon):
                rows_checked += 1
                forbidden = sorted(col for col in FORBIDDEN_ALPHA_COLUMNS if col in row)
                if forbidden:
                    blockers.append(f"forbidden_alpha_columns:{horizon}:{','.join(forbidden)}")
                if boolish(row.get("holder_rpc_used")):
                    blockers.append(f"holder_rpc_used:{row.get('mint','')}")
                if boolish(row.get("rpc_mint_supply_canonical")):
                    blockers.append(f"rpc_mint_supply_canonical:{row.get('mint','')}")
                if boolish(row.get("threshold_tuning_allowed")) or boolish(row.get("live_trading_enabled")):
                    blockers.append(f"forbidden_execution_flag:{row.get('mint','')}")
                if boolish(row.get("horizon_reached")) and not _timestamp_safe(row):
                    blockers.append(f"post_horizon_timestamp:{horizon}:{row.get('mint','')}")
        return {"passed": not blockers, "blockers": blockers, "rows_checked": rows_checked}


def _parse_iso(value: str) -> datetime | None:
    try:
        if not value or value.startswith("["):
            return None
        return datetime.fromisoformat(value.replace(" UTC", "+00:00").replace("Z", "+00:00"))
    except ValueError:
        return None


def _timestamp_safe(row: dict[str, str]) -> bool:
    asof = _parse_iso(row.get("feature_asof_timestamp", ""))
    first = _parse_iso(row.get("mint_first_seen_timestamp", ""))
    if asof is None or first is None:
        return True
    horizon = float(row.get("horizon_seconds", "0") or 0)
    return (asof - first).total_seconds() <= horizon + 0.001
