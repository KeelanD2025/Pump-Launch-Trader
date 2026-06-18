from __future__ import annotations

from typing import Any

from .schemas import boolish, intish


def included_counted_slice(row: dict[str, Any]) -> bool:
    return (
        boolish(row.get("included"))
        and boolish(row.get("counted_phase107b_result"))
        and boolish(row.get("r2_verified"))
        and boolish(row.get("artifact_consistency_ok"))
        and intish(row.get("sequence_gap_count")) == 0
        and intish(row.get("hash_mismatch_count")) == 0
        and intish(row.get("receiver_backpressure_count")) == 0
        and boolish(row.get("holder_rpc_disabled"))
        and boolish(row.get("rpc_mint_supply_non_canonical"))
        and boolish(row.get("replay_disabled"))
        and boolish(row.get("backtesting_disabled"))
        and boolish(row.get("threshold_tuning_disabled"))
        and boolish(row.get("trading_disabled"))
    )

