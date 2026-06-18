from __future__ import annotations

from typing import Any

from .schemas import SignalOutput, stable_hash


def signal_output(
    *,
    strategy_name: str,
    strategy_version: str,
    mint: str,
    horizon_seconds: int,
    decision: str,
    reason_codes: list[str],
    features: dict[str, Any],
    confidence_bin: str = "MISSING",
    explanation: str = "",
) -> SignalOutput:
    output = SignalOutput(
        strategy_name=strategy_name,
        strategy_version=strategy_version,
        mint=mint,
        horizon_seconds=horizon_seconds,
        decision=decision,
        confidence_bin=confidence_bin,
        reason_codes=reason_codes,
        feature_snapshot_hash=stable_hash(features),
        data_quality_status=data_quality_status(features),
        explanation=explanation,
    )
    output.validate()
    return output


def data_quality_status(features: dict[str, Any]) -> str:
    if any(str(features.get(field, "")).lower() == "true" for field in (
        "provider_gap_exposed",
        "relay_gap_exposed",
        "sequence_gap_exposed",
        "hash_mismatch_exposed",
        "receiver_backpressure_exposed",
    )):
        return "excluded"
    return "clean"

