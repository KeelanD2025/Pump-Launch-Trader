from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .schemas import boolish


PROMOTION_PRIORITY_V1_DECISIONS = {
    "promote_to_rich_tracking_research_only",
    "keep_cheap_followup",
    "reject",
    "audit_only",
    "censored",
    "insufficient_data",
}

PROMOTION_PRIORITY_V1_REASON_CODES = {
    "early_curve_progress",
    "early_buy_sell_followthrough",
    "early_volume_followthrough",
    "early_holder_growth",
    "low_adverse_sell_pressure",
    "vault_curve_progress",
    "clean_high_throughput_watch",
    "absence_of_holder_dev_risk",
    "sufficient_horizon_coverage",
    "dead_launch_avoider_reject",
    "holder_concentration_risk",
    "dev_or_creator_holding_risk",
    "liquidity_exit_proxy",
    "adverse_sell_pressure",
    "insufficient_followup",
    "data_quality_excluded",
    "terminal_inconclusive_censored",
}


def num(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def descriptive_bin(value: float | None, *, low: float, high: float) -> str:
    if value is None:
        return "MISSING"
    if value >= high:
        return "HIGH"
    if value >= low:
        return "MEDIUM"
    return "LOW"


def feature_bins(row: dict[str, Any]) -> dict[str, str]:
    return {
        "trade_burst_bin": row.get("trade_burst_bin")
        or descriptive_bin(num(row.get("trade_burst_score_asof")), low=0.1, high=0.5),
        "buy_followthrough_bin": row.get("buy_followthrough_bin")
        or descriptive_bin(num(row.get("buy_count_delta_asof")), low=1, high=3),
        "sell_pressure_bin": row.get("sell_pressure_bin")
        or sell_pressure_bin(row),
        "volume_bin": row.get("volume_bin")
        or descriptive_bin(num(row.get("volume_delta_asof")), low=500_000_000, high=5_000_000_000),
        "holder_concentration_risk_bin": row.get("holder_concentration_risk_bin")
        or descriptive_bin(num(row.get("top_holder_concentration_asof")), low=0.5, high=0.8),
        "dev_creator_holding_risk_bin": row.get("dev_creator_holding_risk_bin")
        or descriptive_bin(num(row.get("dev_or_creator_holding_proxy_asof")), low=0.02, high=0.10),
        "curve_progress_bin": row.get("curve_progress_bin")
        or descriptive_bin(num(row.get("curve_progress_proxy_asof")), low=20, high=50),
    }


def sell_pressure_bin(row: dict[str, Any]) -> str:
    buys = num(row.get("buy_count_delta_asof")) or 0.0
    sells = num(row.get("sell_count_delta_asof"))
    net = num(row.get("net_buy_sell_delta_asof"))
    if sells is None and net is None:
        return "MISSING"
    if (net is not None and net < 0) or (sells is not None and sells >= buys + 2):
        return "HIGH"
    if sells and sells > 0:
        return "MEDIUM"
    return "LOW"


def clean_reason_codes(reason_codes: list[str]) -> list[str]:
    seen: list[str] = []
    for code in reason_codes:
        if code in PROMOTION_PRIORITY_V1_REASON_CODES and code not in seen:
            seen.append(code)
    return seen


@dataclass(frozen=True)
class PromotionPriorityV1Score:
    decision: str
    reason_codes: tuple[str, ...]
    confidence_bin: str
    shadow_only: bool = True
    trade_action: str = "none"
    candidate_eligible: bool = False
    replay_eligible: bool = False
    countability_affects: bool = False

    @property
    def would_promote(self) -> bool:
        return self.decision == "promote_to_rich_tracking_research_only"

    @property
    def would_reject(self) -> bool:
        return self.decision == "reject"

    @property
    def would_keep_cheap_followup(self) -> bool:
        return self.decision == "keep_cheap_followup"

    def as_shadow_fields(self, prefix: str = "promotion_priority_v1_shadow") -> dict[str, str]:
        return {
            f"{prefix}_decision": self.decision,
            f"{prefix}_reason_codes": "|".join(self.reason_codes),
            f"{prefix}_confidence_bin": self.confidence_bin,
            "promotion_priority_v1_would_promote": str(self.would_promote).lower(),
            "promotion_priority_v1_would_reject": str(self.would_reject).lower(),
            "promotion_priority_v1_would_keep_cheap_followup": str(self.would_keep_cheap_followup).lower(),
            "promotion_priority_v1_shadow_only": str(self.shadow_only).lower(),
            "trade_action": self.trade_action,
        }


def score_promotion_priority_v1(
    row: dict[str, Any],
    *,
    dead_launch_avoider_decision: str = "",
    dead_launch_avoider_reason_codes: str = "",
) -> PromotionPriorityV1Score:
    bins = feature_bins(row)
    reasons: list[str] = []
    data_quality = any(
        boolish(row.get(field))
        for field in (
            "data_quality_exclusion",
            "provider_gap_exposed",
            "relay_gap_exposed",
            "sequence_gap_exposed",
            "hash_mismatch_exposed",
            "receiver_backpressure_exposed",
        )
    )
    terminal_censored = boolish(row.get("terminal_inconclusive_before_horizon")) or row.get(
        "final_outcome"
    ) == "terminal_inconclusive"
    invalid_quality = row.get("positive_outcome_label") == "invalid_quality"
    horizon_reached = boolish(row.get("horizon_reached"))
    liquidity_exit = boolish(row.get("liquidity_exit_proxy_asof")) or "liquidity_exit_proxy" in dead_launch_avoider_reason_codes
    holder_collapse = boolish(row.get("holder_collapse_proxy_asof")) or "holder_collapse_proxy" in dead_launch_avoider_reason_codes

    if terminal_censored:
        return PromotionPriorityV1Score("censored", ("terminal_inconclusive_censored",), "CENSORED")
    if data_quality or invalid_quality:
        return PromotionPriorityV1Score("audit_only", ("data_quality_excluded",), "UNSAFE")
    if not horizon_reached:
        return PromotionPriorityV1Score("insufficient_data", ("insufficient_followup",), "MISSING")

    trade = bins["trade_burst_bin"]
    buy = bins["buy_followthrough_bin"]
    sell = bins["sell_pressure_bin"]
    volume = bins["volume_bin"]
    holder = bins["holder_concentration_risk_bin"]
    dev = bins["dev_creator_holding_risk_bin"]
    curve = bins["curve_progress_bin"]

    buy_ok = buy in {"HIGH", "MEDIUM"}
    sell_ok = sell in {"LOW", "MEDIUM"}
    strong_curve = curve == "HIGH"
    decent_curve = curve in {"HIGH", "MEDIUM"}
    strong_volume = volume == "HIGH"
    decent_volume = volume in {"HIGH", "MEDIUM"}
    hard_dead = liquidity_exit or holder_collapse or sell == "HIGH" or (
        dead_launch_avoider_decision == "avoid"
        and (buy == "LOW" or volume == "LOW" or curve in {"LOW", "MISSING"})
    )
    strong_shape = strong_curve and strong_volume and buy_ok and sell_ok and not liquidity_exit and not holder_collapse
    secondary_shape = (
        decent_curve
        and strong_volume
        and buy_ok
        and sell_ok
        and trade in {"HIGH", "MEDIUM"}
        and not liquidity_exit
        and not holder_collapse
    )

    if strong_shape or secondary_shape:
        reasons.extend(
            [
                "sufficient_horizon_coverage",
                "early_curve_progress",
                "early_volume_followthrough",
                "early_buy_sell_followthrough",
                "low_adverse_sell_pressure",
                "vault_curve_progress",
            ]
        )
        if boolish(row.get("high_throughput_before_horizon")):
            reasons.append("clean_high_throughput_watch")
        if holder not in {"HIGH", "MISSING"} and dev not in {"HIGH", "MISSING"}:
            reasons.append("absence_of_holder_dev_risk")
        if holder == "HIGH":
            reasons.append("holder_concentration_risk")
        if dev == "HIGH":
            reasons.append("dev_or_creator_holding_risk")
        return PromotionPriorityV1Score(
            "promote_to_rich_tracking_research_only",
            tuple(clean_reason_codes(reasons)),
            "HIGH" if strong_shape else "MEDIUM",
        )

    if hard_dead:
        reasons.append("dead_launch_avoider_reject")
        if liquidity_exit:
            reasons.append("liquidity_exit_proxy")
        if sell == "HIGH":
            reasons.append("adverse_sell_pressure")
        if holder == "HIGH":
            reasons.append("holder_concentration_risk")
        if dev == "HIGH":
            reasons.append("dev_or_creator_holding_risk")
        if not buy_ok or not decent_volume:
            reasons.append("insufficient_followup")
        return PromotionPriorityV1Score("reject", tuple(clean_reason_codes(reasons)), "UNSAFE")

    reasons.append("insufficient_followup")
    if dead_launch_avoider_decision == "avoid":
        reasons.append("dead_launch_avoider_reject")
    if decent_curve:
        reasons.append("early_curve_progress")
    if decent_volume:
        reasons.append("early_volume_followthrough")
    if buy_ok:
        reasons.append("early_buy_sell_followthrough")
    return PromotionPriorityV1Score("keep_cheap_followup", tuple(clean_reason_codes(reasons)), "LOW")
