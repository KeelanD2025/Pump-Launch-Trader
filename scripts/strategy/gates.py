from __future__ import annotations

from typing import Any

from .schemas import boolish, floatish, intish
from .signals import signal_output


def _group_missing(row: dict[str, Any], columns: tuple[str, ...]) -> bool:
    return any(str(row.get(column, "")).strip() == "" for column in columns)


def _risk_quality_reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if boolish(row.get("provider_gap_exposed")):
        reasons.append("provider_gap_exposed")
    if boolish(row.get("relay_gap_exposed")):
        reasons.append("relay_gap_exposed")
    if boolish(row.get("terminal_inconclusive_before_horizon")):
        reasons.append("terminal_inconclusive")
    if boolish(row.get("rejected_before_horizon")):
        reasons.append("rejected_before_horizon")
    if boolish(row.get("degraded_audit_only_before_horizon")):
        reasons.append("high_throughput_degraded_audit_only")
    elif boolish(row.get("high_throughput_before_horizon")):
        reasons.append("high_throughput_clean_observation")
    if not boolish(row.get("horizon_reached")):
        reasons.append("missing_horizon")
    return reasons


class EarlyAvoidFilterV1:
    name = "early_avoid_v1"
    version = "v1"

    def score(self, row: dict[str, Any]) -> Any:
        reasons = _risk_quality_reasons(row)
        if boolish(row.get("data_quality_exclusion")):
            reasons.append("data_quality_exclusion")
        if "terminal_inconclusive" in reasons or "provider_gap_exposed" in reasons or "relay_gap_exposed" in reasons:
            decision = "censored"
        elif "missing_horizon" in reasons:
            decision = "insufficient_data"
        elif boolish(row.get("rejected_before_horizon")):
            decision = "avoid"
        else:
            decision = "continue_tracking"
        return signal_output(
            strategy_name=self.name,
            strategy_version=self.version,
            mint=row.get("mint", ""),
            horizon_seconds=intish(row.get("horizon_seconds")),
            decision=decision,
            reason_codes=reasons or ["no_avoid_reason_seen"],
            confidence_bin="MEDIUM",
            features=row,
            explanation="Research-only early avoid decision using as-of alpha features; no trade action.",
        )


class ContinueTrackingGateV1:
    name = "continue_tracking_v1"
    version = "v1"

    def score(self, row: dict[str, Any]) -> Any:
        reasons = _risk_quality_reasons(row)
        if "terminal_inconclusive" in reasons:
            decision = "censored"
        elif any(reason.endswith("_exposed") for reason in reasons):
            decision = "audit_only"
        elif "missing_horizon" in reasons:
            decision = "insufficient_data"
        elif boolish(row.get("rejected_before_horizon")):
            decision = "avoid"
            reasons.append("stop_tracking_after_rejection")
        else:
            decision = "continue_tracking"
            reasons.append("survived_current_horizon")
        return signal_output(
            strategy_name=self.name,
            strategy_version=self.version,
            mint=row.get("mint", ""),
            horizon_seconds=intish(row.get("horizon_seconds")),
            decision=decision,
            reason_codes=reasons,
            confidence_bin="MEDIUM",
            features=row,
            explanation="Research-only continue tracking gate; no trade action.",
        )


class CandidateEligibilityGateV2:
    name = "candidate_eligibility_v2"
    version = "v2"

    def score(self, row: dict[str, Any], label: dict[str, Any] | None = None) -> Any:
        label = label or {}
        reasons = _risk_quality_reasons(row)
        if _group_missing(row, ("trade_update_count_asof", "buy_count_delta_asof", "sell_count_delta_asof")):
            reasons.append("missing_asof_trade_delta")
        else:
            buys = intish(row.get("buy_count_delta_asof"))
            sells = intish(row.get("sell_count_delta_asof"))
            net = intish(row.get("net_buy_sell_delta_asof"))
            if buys <= 0:
                reasons.append("insufficient_buy_followthrough")
            if sells > buys or net < 0:
                reasons.append("adverse_sell_pressure")
            if intish(row.get("trade_update_count_asof")) <= 0:
                reasons.append("insufficient_trade_delta_strength")
        if _group_missing(row, ("holder_update_count_asof", "unique_holder_accounts_seen_asof")):
            reasons.append("missing_asof_holder_state")
        else:
            if intish(row.get("unique_holder_accounts_seen_asof")) <= 1:
                reasons.append("weak_holder_growth")
            concentration = floatish(row.get("top_holder_concentration_asof"))
            if concentration is not None and concentration >= 0.75:
                reasons.append("holder_concentration_risk")
            if floatish(row.get("dev_or_creator_holding_proxy_asof")) not in (None, 0.0):
                reasons.append("dev_or_creator_holding_risk")
        if _group_missing(row, ("vault_update_count_asof", "bonding_curve_update_count_asof", "curve_progress_proxy_asof")):
            reasons.append("missing_asof_vault_curve")
        else:
            curve = floatish(row.get("curve_progress_proxy_asof"))
            if curve is None or curve <= 0:
                reasons.append("weak_vault_curve_progress")
                reasons.append("insufficient_curve_progress")
            if floatish(row.get("liquidity_exit_proxy_asof")) not in (None, 0.0):
                reasons.append("liquidity_exit_risk")
        if not boolish(label.get("candidate_checkpoint_seen")):
            reasons.append("candidate_checkpoint_absent")
        if not boolish(label.get("replay_eligible")):
            reasons.append("replay_not_countability_allowed")
        if boolish(label.get("censored_label")):
            reasons.append("terminal_inconclusive")
        hard = {"provider_gap_exposed", "relay_gap_exposed", "terminal_inconclusive", "high_throughput_degraded_audit_only"}
        if any(reason in hard for reason in reasons):
            decision = "censored"
            confidence = "CENSORED"
        elif any(reason.startswith("missing_") or reason == "missing_horizon" for reason in reasons):
            decision = "insufficient_data"
            confidence = "MISSING"
        elif "candidate_checkpoint_absent" in reasons or "replay_not_countability_allowed" in reasons:
            decision = "candidate_watch"
            confidence = "LOW"
        else:
            decision = "candidate_review"
            confidence = "MEDIUM"
        return signal_output(
            strategy_name=self.name,
            strategy_version=self.version,
            mint=row.get("mint", ""),
            horizon_seconds=intish(row.get("horizon_seconds")),
            decision=decision,
            reason_codes=sorted(set(reasons)),
            confidence_bin=confidence,
            features=row,
            explanation="Research-only candidate eligibility diagnostics. Candidate checkpoint is audit-only and never replay permission.",
        )

