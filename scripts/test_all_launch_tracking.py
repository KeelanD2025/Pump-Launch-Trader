#!/usr/bin/env python3
"""Tests for all-launch intake/tiered tracking source and audit artifacts."""

from __future__ import annotations

import json
import pathlib
import unittest


REPO = pathlib.Path(__file__).resolve().parents[1]
AUDIT_ROOT = REPO / "research_output" / "trading_strategy_pipeline" / "all_launch_tracking"
CLI_MAIN = REPO / "crates" / "cli" / "src" / "main.rs"
SUPERVISOR = REPO / "scripts" / "run_relay_r2_primary_batch.py"
READINESS = REPO / "scripts" / "build_strategy_readiness.py"


class AllLaunchTrackingTests(unittest.TestCase):
    def test_launch_admission_audit_exists_and_preserves_safety_flags(self) -> None:
        payload = json.loads((AUDIT_ROOT / "launch_admission_audit.json").read_text())
        self.assertEqual(payload["schema_version"], "phase107i.launch_admission_audit.v1")
        self.assertTrue(payload["capped_or_skipped_launches_recorded"])
        self.assertFalse(payload["holder_rpc_used"])
        self.assertFalse(payload["rpc_mint_supply_canonical"])
        self.assertFalse(payload["replay_allowed"])
        self.assertFalse(payload["formal_backtesting_allowed"])
        self.assertFalse(payload["threshold_tuning_allowed"])
        self.assertFalse(payload["live_trading_enabled"])

    def test_cli_declares_required_all_launch_artifacts(self) -> None:
        source = CLI_MAIN.read_text()
        for required in (
            "all_launch_intake_ledger.csv",
            "all_launch_intake_summary.json",
            "rich_tracking_slot_ledger.csv",
            "rich_tracking_slot_summary.json",
            "missed_good_token_audit.csv",
            "all_launch_followup_manifest.json",
            "all_launch_followup_summary.md",
            "promotion_queue_ledger.csv",
            "promotion_queue_summary.json",
            "missed_good_token_audit_v2.csv",
        ):
            self.assertIn(required, source)
        self.assertIn("max_attempted_launches_applies_to", source)
        self.assertIn("tier_3_rich_tracking_not_tier_1_visibility", source)
        self.assertIn("max_attempted_launches_semantics", source)
        self.assertIn("visible launch intake and cheap follow-up are separate", source)

    def test_supervisor_rollup_surfaces_all_launch_counts(self) -> None:
        source = SUPERVISOR.read_text()
        for field in (
            "all_launches_indexed",
            "rich_tracked_launches",
            "cheap_only_launches",
            "skipped_due_budget",
            "missed_good_token_count",
            "cheap_followup_rows",
            "promotion_recommended_count",
            "promotion_admitted_count",
            "promotion_blocked_budget_count",
        ):
            self.assertIn(field, source)

    def test_strategy_readiness_inventory_has_all_launch_fields(self) -> None:
        source = READINESS.read_text()
        for field in (
            "all_launches_indexed",
            "rich_tracked_launches",
            "cheap_only_launches",
            "skipped_due_budget",
            "tracking_slots_released",
            "cheap_followup_rows",
            "promotion_recommended_count",
            "promotion_admitted_count",
            "promotion_blocked_budget_count",
        ):
            self.assertIn(field, source)

    def test_cli_preserves_followup_safety_invariants(self) -> None:
        source = CLI_MAIN.read_text()
        for invariant in (
            "all_launch_followup_replay_eligible",
            "all_launch_followup_row_safety_flag_enabled",
            "missed_good_token_audit_v2_replay_eligible",
            "holder_rpc_used",
            "rpc_mint_supply_canonical",
            "threshold_tuning_allowed",
            "live_trading_enabled",
        ):
            self.assertIn(invariant, source)


if __name__ == "__main__":
    unittest.main()
