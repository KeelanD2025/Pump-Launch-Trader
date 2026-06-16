#!/usr/bin/env python3
import csv
import tempfile
import unittest
from pathlib import Path

import build_strategy_readiness as sr


def write_csv(path: Path, rows: list[dict[str, str]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def fake_run(tmp: Path, attempts: list[dict[str, str]], *, segment_clean: bool = True, gap_mint: str = "") -> dict:
    run_dir = tmp / "relay-test-run"
    run_dir.mkdir()
    write_csv(
        run_dir / "run_gap_events.csv",
        [{"affected_mints": gap_mint}] if gap_mint else [],
        ["affected_mints"],
    )
    segment = {
        "segment_id": "1",
        "counted_phase107b_result": "true" if segment_clean else "false",
        "provider_data_loss_seen": "false" if segment_clean else "true",
        "client_backpressure_detected": "false",
        "partial_outputs_audit_only": "false" if segment_clean else "true",
        "blocker_class": "" if segment_clean else "provider_lagged_data_loss",
    }
    return {
        "source_path": str(run_dir),
        "batch_id": "batch",
        "slice_id": "slice",
        "relay_session_id": "relay",
        "included": True,
        "sequence_gap_count": 0,
        "hash_mismatch_count": 0,
        "receiver_backpressure_count": 0,
        "countability": {
            "off_vps_candidate_replay_allowed": False,
            "replay_eligible_candidate_count": 0,
            "degraded_active_mints": [],
        },
        "hunter": {"high_throughput_mints": [], "degraded_active_mints": []},
        "attempt_rows": attempts,
        "rejected_rows": [],
        "candidate_rows": [],
        "segment_rows": [segment],
    }


class StrategyReadinessTests(unittest.TestCase):
    def test_terminal_inconclusive_is_censored_not_dead(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run = fake_run(
                Path(td),
                [
                    {
                        "mint": "mint1",
                        "final_state": "terminal_inconclusive",
                        "rejection_or_promotion_reason": "provider_gap",
                        "tracked_until_seconds": "60",
                        "launch_timestamp": "2026-06-16 12:00:00 +00:00:00",
                    }
                ],
            )
            labels, _ = sr.build_labels([run])
        self.assertTrue(labels[0]["censored_label"])
        self.assertFalse(labels[0]["clean_negative_label"])

    def test_candidate_checkpoint_is_not_positive_without_replay(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run = fake_run(
                Path(td),
                [
                    {
                        "mint": "mint2",
                        "final_state": "candidate_checkpoint",
                        "tracked_until_seconds": "300",
                        "launch_timestamp": "2026-06-16 12:00:00 +00:00:00",
                    }
                ],
            )
            run["candidate_rows"] = [{"mint": "mint2", "candidate_checkpoint": "true", "replay_eligible": "false"}]
            labels, _ = sr.build_labels([run])
        self.assertTrue(labels[0]["candidate_checkpoint_seen"])
        self.assertFalse(labels[0]["clean_positive_label"])
        self.assertFalse(labels[0]["replay_eligible"])

    def test_gap_exposed_mint_is_censored(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run = fake_run(
                Path(td),
                [
                    {
                        "mint": "mint3",
                        "final_state": "early_rejected_dead",
                        "tracked_until_seconds": "60",
                        "launch_timestamp": "2026-06-16 12:00:00 +00:00:00",
                    }
                ],
                segment_clean=False,
                gap_mint="mint3",
            )
            labels, _ = sr.build_labels([run])
        self.assertTrue(labels[0]["provider_gap_exposed"])
        self.assertTrue(labels[0]["censored_label"])
        self.assertFalse(labels[0]["clean_negative_label"])

    def test_final_outcomes_are_not_feature_columns(self) -> None:
        forbidden = {"final_outcome", "rejection_reason", "candidate_checkpoint_seen", "replay_eligible", "r2_verified"}
        self.assertFalse(forbidden & set(sr.ASOF_FIELDS))

    def test_random_splits_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = sr.leakage_audit(Path(td), [], [], {"method": "random", "splits": {}})
        self.assertFalse(payload["passed"])
        self.assertIn("random_split_used", payload["blockers"])

    def test_same_mint_cannot_appear_in_multiple_splits(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = sr.leakage_audit(
                Path(td),
                [],
                [],
                {
                    "method": "chronological_walk_forward",
                    "splits": {
                        "train": {"mints": ["mint"]},
                        "test": {"mints": ["mint"]},
                    },
                },
            )
        self.assertFalse(payload["passed"])
        self.assertTrue(any(blocker.startswith("mint_in_multiple_splits") for blocker in payload["blockers"]))

    def test_buy_and_risk_modules_are_disabled(self) -> None:
        self.assertFalse(sr.BuySetupDraft.enabled)
        self.assertFalse(sr.BuySetupDraft().describe()["wallet_execution_enabled"])
        self.assertFalse(sr.RiskAndExitDraft.enabled)
        self.assertFalse(sr.RiskAndExitDraft().describe()["tradeable"])

    def test_backtesting_blocks_without_clean_positives(self) -> None:
        labels = [{"clean_negative_label": True, "clean_positive_label": False, "replay_eligible": False}]
        decision = sr.readiness_decision(labels, {"passed": True}, {"EarlyAvoidFilter": {}, "ContinueTrackingGate": {}, "CandidateEligibilityGate": {}, "BuySetupDraft": {}, "RiskAndExitDraft": {}}, [{"feature_available": True}])
        self.assertTrue(decision["strategy_research_ready"])
        self.assertTrue(decision["buy_strategy_build_ready"])
        self.assertFalse(decision["backtesting_ready"])
        self.assertFalse(decision["replay_ready"])

    def test_early_avoid_filter_does_not_output_trade_entries(self) -> None:
        result = sr.EarlyAvoidFilter().score(
            {
                "tracked_at_least_horizon": True,
                "label_clean_negative": True,
                "data_quality_provider_gap_exposed": False,
            }
        )
        self.assertEqual(result.decision, "avoid")
        self.assertFalse(sr.EarlyAvoidFilter.tradeable)

    def test_early_avoid_filter_does_not_need_future_outcome_fields(self) -> None:
        features = set(sr.ASOF_FIELDS)
        self.assertNotIn("final_outcome", features)
        self.assertNotIn("rejection_reason", features)

    def test_continue_tracking_treats_terminal_inconclusive_as_censored(self) -> None:
        result = sr.ContinueTrackingGate().score(
            {
                "tracked_at_least_horizon": True,
                "label_censored": True,
            }
        )
        self.assertEqual(result.decision, "censored")
        self.assertIn("label_censored_not_dead", result.reason_codes)

    def test_candidate_eligibility_rejects_provider_gap_exposed_mints(self) -> None:
        result = sr.CandidateEligibilityGate().score(
            {
                "tracked_at_least_horizon": True,
                "data_quality_provider_gap_exposed": True,
            }
        )
        self.assertEqual(result.decision, "censored")
        self.assertIn("data_quality_provider_gap_exposed", result.reason_codes)

    def test_candidate_eligibility_requires_countability_for_replay(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run = fake_run(
                Path(td),
                [
                    {
                        "mint": "mint4",
                        "final_state": "candidate_checkpoint",
                        "tracked_until_seconds": "300",
                        "launch_timestamp": "2026-06-16 12:00:00 +00:00:00",
                    }
                ],
            )
            run["candidate_rows"] = [{"mint": "mint4", "candidate_checkpoint": "true", "replay_eligible": "true"}]
            labels, _ = sr.build_labels([run])
        self.assertFalse(labels[0]["replay_eligible"])
        self.assertFalse(labels[0]["clean_positive_label"])

    def test_survivor_extension_defaults_do_not_raise_caps_or_run_execution(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            modules = sr.write_strategy_modules(Path(td))
        survivor = modules["SurvivorExtensionMode"]
        self.assertEqual(survivor["status"], "disabled_by_default")
        self.assertFalse(survivor["raises_launch_caps"])
        self.assertFalse(survivor["runs_replay"])
        self.assertFalse(survivor["trades"])

    def test_survivor_extension_proof_classifies_clean_counted_run(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run = Path(td) / "relay-r2-primary-proof"
            run.mkdir()
            (run / "survivor_extension_mode.json").write_text('{"enabled":true}')
            rows = [
                {
                    "source_path": str(run),
                    "included": True,
                    "counted_phase107b_result": True,
                    "replay_eligible_candidate_count": 0,
                }
            ]
            survivor_runs = sr.survivor_extension_runs(rows)
        self.assertEqual(len(survivor_runs), 1)
        self.assertEqual(
            sr.survivor_extension_proof_classification(survivor_runs),
            "SURVIVOR_EXTENSION_PROOF_PASS",
        )


if __name__ == "__main__":
    unittest.main()
