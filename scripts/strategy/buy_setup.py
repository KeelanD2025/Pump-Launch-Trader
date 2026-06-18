from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class BuySetupDraft:
    disabled_by_default: bool = True
    tradeable: bool = False
    wallet_execution_enabled: bool = False
    setup_families: list[str] = field(
        default_factory=lambda: [
            "early_followthrough_candidate",
            "holder_growth_candidate",
            "vault_curve_progress_candidate",
            "clean_high_throughput_candidate",
            "delayed_survivor_candidate",
        ]
    )

    def score(self, mint: str, gate_decision: str, reason_codes: list[str]) -> dict[str, Any]:
        return {
            "mint": mint,
            "setup_decision": "candidate_setup_only",
            "disabled_by_default": True,
            "trade_action": "none",
            "gate_decision": gate_decision,
            "reason_codes": "|".join(reason_codes),
            "required_evidence_before_activation": "clean_positives|replay_allowed|backtesting_ready|operator_approval",
            "blocked_actions": "replay|backtesting|threshold_tuning|live_trading|wallet_execution",
        }

    def describe(self) -> dict[str, Any]:
        return {
            "name": "buy_setup_draft_v0",
            "disabled_by_default": self.disabled_by_default,
            "tradeable": self.tradeable,
            "wallet_execution_enabled": self.wallet_execution_enabled,
            "setup_families": self.setup_families,
            "allowed_output": "candidate_setup_only",
        }

