from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class RiskAndExitDraft:
    disabled_by_default: bool = True
    emits_orders: bool = False
    wallet_execution_enabled: bool = False

    def describe(self) -> dict[str, Any]:
        return {
            "name": "risk_exit_draft_v0",
            "disabled_by_default": True,
            "emits_orders": False,
            "wallet_execution_enabled": False,
            "invalidation_rules": [
                "holder_concentration_risk",
                "liquidity_exit_risk",
                "adverse_sell_pressure",
                "provider_or_relay_data_quality_loss",
            ],
            "kill_switch_hypotheses": [
                "stream_gap",
                "R2_verification_failure",
                "artifact_consistency_failure",
            ],
            "data_requirements": [
                "clean_positive_examples",
                "replay_allowed_candidates",
                "paper_trading_gate",
                "latency_and_slippage_model",
            ],
        }

