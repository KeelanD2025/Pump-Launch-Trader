import argparse
import csv
import importlib.util
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


SCRIPT = Path(__file__).with_name("build_fresh_stream_holder_tracking.py")
SPEC = importlib.util.spec_from_file_location("fresh_stream_holder_tracking", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0]) if rows else ["mint"]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class FreshStreamHolderTrackingTest(unittest.TestCase):
    def test_fresh_manifest_scope_near_exact_carry_and_vault_exclusion(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            run = root / "run"
            output = root / "output"
            amm = root / "amm"
            strategy = root / "strategy"
            run.mkdir()
            amm.mkdir()
            strategy.mkdir()

            mint = "FreshMintpump"
            old_mint = "OldMintpump"
            launch_signature = "launch-signature"
            activation = datetime(2026, 1, 1, tzinfo=timezone.utc)
            activation_nanos = int(activation.timestamp() * 1_000_000_000)
            manifest = {
                "schema_version": "exact_holder_tracker_activation_manifest.v1",
                "relay_session_id": "relay-fresh",
                "tracker_activation_unix_nanos": activation_nanos,
                "dynamic_fresh_launch_tracking_enabled": True,
                "max_active_mints": 64,
                "mint_ttl_seconds": 7200,
                "subscription_update_failures": 0,
                "active_mint_count": 0,
                "active_mints": [],
                "stream_only_required": True,
                "rpc_holder_snapshot_allowed": False,
                "dex_as_holder_truth": False,
                "proxy_as_exact": False,
                "pool_vaults_counted_as_holders": False,
                "tracker_rows": [
                    {
                        "mint": mint,
                        "launch_slot": 10,
                        "launch_signature": launch_signature,
                        "launch_observed_at_unix_nanos": activation_nanos + 1_000_000_000,
                        "tracker_created": True,
                        "tracker_created_at_unix_nanos": activation_nanos + 1_010_000_000,
                        "tracker_delay_ms": 10,
                        "tracker_source": "yellowstone_pump_create_dynamic_token_account_filter",
                        "active": False,
                        "retired_at_unix_nanos": activation_nanos + 18_000_000_000,
                        "retired_reason": "capacity_evicted",
                    }
                ],
            }
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest))

            write_csv(
                run / "decoded_launch_event_rows.csv",
                [
                    {
                        "mint": mint,
                        "launch_id": "launch-fresh",
                        "event_observed_at_utc": "2026-01-01T00:00:01Z",
                        "slot": 10,
                        "signature": launch_signature,
                        "event_type": "launch_create",
                        "decoded_instruction_name": "create_v2",
                        "source_is_non_rpc": True,
                        "rpc_used": False,
                        "strict_timing_eligible": True,
                        "parse_status": "non_rpc_decoded_launch_create_event",
                        "creator_wallet": "creator-wallet",
                        "bonding_curve": "curve-account",
                        "associated_bonding_curve": "curve-token-account",
                    },
                    {
                        "mint": old_mint,
                        "launch_id": "launch-old",
                        "event_observed_at_utc": "2025-12-01T00:00:00Z",
                        "slot": 1,
                        "signature": "old-signature",
                        "event_type": "launch_create",
                        "decoded_instruction_name": "pending_create_backfill",
                        "source_is_non_rpc": True,
                        "rpc_used": False,
                        "strict_timing_eligible": True,
                        "parse_status": "non_rpc_decoded_launch_create_event",
                        "creator_wallet": "old-creator",
                        "bonding_curve": "old-curve",
                        "associated_bonding_curve": "old-curve-token",
                    },
                    {
                        "mint": mint,
                        "launch_id": "launch-fresh-pending-backfill",
                        "event_observed_at_utc": "2026-01-01T00:00:00.500000Z",
                        "slot": 9,
                        "signature": "pending-backfill-signature",
                        "event_type": "launch_create",
                        "decoded_instruction_name": "pending_create_backfill",
                        "source_is_non_rpc": True,
                        "rpc_used": False,
                        "strict_timing_eligible": True,
                        "parse_status": "non_rpc_decoded_launch_create_event",
                        "creator_wallet": "creator-wallet",
                        "bonding_curve": "curve-account",
                        "associated_bonding_curve": "curve-token-account",
                    },
                ],
            )
            holder_rows = [
                {
                    "mint": mint,
                    "launch_id": "launch-fresh",
                    "event_observed_at_utc": "2026-01-01T00:00:01Z",
                    "slot": 10,
                    "signature": launch_signature,
                    "token_account": "curve-token-account",
                    "owner_wallet": "curve-account",
                    "amount_raw": "900000000",
                    "amount_ui": "900",
                    "decimals": "6",
                    "holder_balance_before": "",
                    "holder_balance_after": "900000000",
                    "update_source": "geyser_token_balance",
                    "update_type": "token_account_balance_created_or_observed",
                },
                {
                    "mint": mint,
                    "launch_id": "launch-fresh",
                    "event_observed_at_utc": "2026-01-01T00:00:01Z",
                    "slot": 10,
                    "signature": launch_signature,
                    "token_account": "creator-token-account",
                    "owner_wallet": "creator-wallet",
                    "amount_raw": "100000000",
                    "amount_ui": "100",
                    "decimals": "6",
                    "holder_balance_before": "",
                    "holder_balance_after": "100000000",
                    "update_source": "geyser_token_balance",
                    "update_type": "token_account_balance_created_or_observed",
                },
                {
                    "mint": mint,
                    "launch_id": "launch-fresh",
                    "event_observed_at_utc": "2026-01-01T00:00:05Z",
                    "slot": 15,
                    "signature": "transfer-one",
                    "token_account": "buyer-token-account",
                    "owner_wallet": "buyer-wallet",
                    "amount_raw": "50000000",
                    "amount_ui": "50",
                    "decimals": "6",
                    "holder_balance_before": "0",
                    "holder_balance_after": "50000000",
                    "update_source": "geyser_spl_token_account_update",
                    "update_type": "token_account_balance_update",
                },
                {
                    "mint": mint,
                    "launch_id": "launch-fresh",
                    "event_observed_at_utc": "2026-01-01T00:00:11Z",
                    "slot": 21,
                    "signature": "post-migration-transfer",
                    "token_account": "pool-base-vault",
                    "owner_wallet": "pool-authority",
                    "amount_raw": "400000000",
                    "amount_ui": "400",
                    "decimals": "6",
                    "holder_balance_before": "0",
                    "holder_balance_after": "400000000",
                    "update_source": "geyser_spl_token_account_update",
                    "update_type": "token_account_balance_update",
                },
                {
                    "mint": mint,
                    "launch_id": "launch-fresh",
                    "event_observed_at_utc": "2026-01-01T00:00:21Z",
                    "slot": 31,
                    "signature": "post-gap-transfer",
                    "token_account": "buyer-token-account",
                    "owner_wallet": "buyer-wallet",
                    "amount_raw": "60000000",
                    "amount_ui": "60",
                    "decimals": "6",
                    "holder_balance_before": "50000000",
                    "holder_balance_after": "60000000",
                    "update_source": "geyser_spl_token_account_update",
                    "update_type": "token_account_balance_update",
                },
                {
                    "mint": old_mint,
                    "launch_id": "launch-old",
                    "event_observed_at_utc": "2026-01-01T00:00:03Z",
                    "slot": 12,
                    "signature": "old-update",
                    "token_account": "old-token-account",
                    "owner_wallet": "old-wallet",
                    "amount_raw": "1",
                    "amount_ui": "0.000001",
                    "decimals": "6",
                    "holder_balance_before": "0",
                    "holder_balance_after": "1",
                    "update_source": "geyser_spl_token_account_update",
                    "update_type": "token_account_balance_update",
                },
            ]
            write_csv(run / "decoded_holder_event_rows.csv", holder_rows)
            write_csv(
                run / "run_gap_events.csv",
                [
                    {
                        "provider_data_loss_seen": True,
                        "client_backpressure_detected": False,
                        "blocker_class": "provider_lagged_data_loss",
                        "created_at": "[2025,365,23,59,59,0,0,0,0]",
                    },
                    {
                        "provider_data_loss_seen": True,
                        "client_backpressure_detected": False,
                        "blocker_class": "provider_lagged_data_loss",
                        "created_at": "[2026,1,0,0,20,0,0,0,0]",
                    },
                ],
            )
            write_csv(
                run / "quant_pumpfun_migration_event_rows.csv",
                [
                    {
                        "mint": mint,
                        "launch_id": "launch-fresh",
                        "event_observed_at_utc": "2026-01-01T00:00:09Z",
                        "slot": 19,
                        "signature": "failed-migration-signature",
                        "decoded_instruction_name": "migrate",
                        "transaction_status": "failed",
                        "migration_confirmed": False,
                        "migration_evidence_type": "decoded_failed_pump_liquidity_migration_attempt",
                        "bonding_curve": "curve-account",
                        "associated_bonding_curve": "curve-token-account",
                        "post_migration_pool": "pool-one",
                        "pool_base_token_account": "pool-base-vault",
                        "pool_quote_token_account": "pool-quote-vault",
                    },
                    {
                        "mint": mint,
                        "launch_id": "launch-fresh",
                        "event_observed_at_utc": "2026-01-01T00:00:09.500000Z",
                        "slot": 19,
                        "signature": "creator-migration-signature",
                        "decoded_instruction_name": "migrate_bonding_curve_creator",
                        "transaction_status": "success",
                        "migration_confirmed": False,
                        "migration_evidence_type": "decoded_non_liquidity_migration_instruction",
                        "bonding_curve": "curve-account",
                        "associated_bonding_curve": "curve-token-account",
                    },
                    {
                        "mint": mint,
                        "launch_id": "launch-fresh",
                        "event_observed_at_utc": "2026-01-01T00:00:10Z",
                        "slot": 20,
                        "signature": "migration-signature",
                        "decoded_instruction_name": "migrate",
                        "transaction_status": "success",
                        "migration_confirmed": True,
                        "migration_evidence_type": "decoded_successful_pump_liquidity_migration",
                        "bonding_curve": "curve-account",
                        "associated_bonding_curve": "curve-token-account",
                        "post_migration_pool": "pool-one",
                        "pool_base_token_account": "pool-base-vault",
                        "pool_quote_token_account": "pool-quote-vault",
                    }
                ],
            )
            write_csv(run / "quant_pumpswap_pair_event_rows.csv", [])
            (run / "local_collector_summary.json").write_text(
                json.dumps(
                    {
                        "sequence_gap_count": 0,
                        "downstream_backpressure_count": 0,
                        "unverified_chunk_count": 0,
                    }
                )
            )
            write_csv(
                amm / "pumpswap_pool_vault_rows.csv",
                [],
            )
            write_csv(amm / "pumpswap_live_relay_pumpswap_pair_event_rows.csv", [])
            (amm / "pumpswap_amm_coverage_gate.json").write_text(
                json.dumps(
                    {
                        "coverage_ready": True,
                        "amm_research_usable": True,
                        "amm_complete_coverage": False,
                        "decision_time_pool_state_coverage_pct": 90,
                    }
                )
            )
            write_csv(
                strategy / "post_migration_strategy_feature_rows.csv",
                [
                    {
                        "mint": mint,
                        "launch_id": "launch-fresh",
                        "decision_ts": "2026-01-01T00:00:12Z",
                    }
                ],
            )

            rc = MODULE.build(
                argparse.Namespace(
                    repo_root=str(root),
                    run_dir=str(run),
                    relay_manifest=str(manifest_path),
                    output_dir=str(output),
                    amm_root=str(amm),
                    strategy_root=str(strategy),
                    min_proof_minutes=30.0,
                    min_fresh_launches=1,
                    min_decision_coverage_pct=75.0,
                )
            )
            self.assertEqual(rc, 0)

            proof = json.loads((output / "exact_holder_fresh_launch_proof_report.json").read_text())
            self.assertEqual(proof["verdict"], "near_exact_holder_fresh_launch_tracking_ready")
            self.assertEqual(
                proof["overall_lifecycle_verdict"],
                "near_exact_holder_fresh_launch_tracking_ready",
            )
            self.assertGreater(proof["token_account_update_rows"], 0)
            self.assertEqual(proof["provider_or_sequence_gap_count"], 2)
            self.assertFalse(proof["source_integrity_proven"])
            self.assertEqual(proof["pre_gap_source_integrity_mint_count"], 1)
            self.assertEqual(proof["source_quality_counts"][MODULE.OBSERVED], 1)
            self.assertEqual(proof["best_proven_source_quality_counts"][MODULE.NEAR_EXACT], 1)
            self.assertEqual(proof["observed_migration_rows"], 3)
            self.assertEqual(proof["confirmed_migration_evidence_rows"], 1)
            self.assertEqual(proof["confirmed_fresh_migration_rows"], 1)
            self.assertEqual(proof["rejected_migration_rows"], 2)
            self.assertEqual(proof["unconfirmed_migration_rows_rejected"], 2)
            self.assertEqual(proof["non_fresh_migration_rows_rejected"], 0)
            self.assertEqual(proof["rejected_migration_mints"], [])
            self.assertEqual(proof["mints_with_rejected_migration_evidence"], [mint])
            self.assertEqual(proof["observed_launch_evidence_rows"], 3)
            self.assertEqual(proof["confirmed_internal_launch_rows"], 1)
            self.assertEqual(proof["rejected_launch_evidence_rows"], 2)
            self.assertEqual(proof["pending_create_backfill_rows_rejected"], 2)
            self.assertEqual(proof["confirmed_launches_with_tracker"], 1)
            self.assertEqual(proof["confirmed_launches_without_tracker"], 0)
            self.assertEqual(proof["confirmed_launches_without_tracker_mints"], [])
            self.assertEqual(proof["confirmed_launch_tracker_coverage_pct"], 100.0)
            self.assertTrue(proof["launch_tracker_coverage_complete"])
            self.assertEqual(proof["retired_trackers"], 1)
            self.assertEqual(proof["capacity_evicted_trackers"], 1)
            self.assertEqual(proof["ttl_expired_trackers"], 0)
            guard = json.loads((output / "exact_holder_no_stale_mint_guard.json").read_text())
            self.assertEqual(guard["accepted_mints"], [mint])
            self.assertEqual(guard["currently_accepted_mints"], [])
            self.assertNotIn(old_mint, guard["accepted_mints"])
            self.assertEqual(guard["non_manifest_confirmed_launch_rows_rejected"], 0)
            self.assertEqual(guard["confirmed_launches_without_tracker_mints"], [])

            with (output / "exact_holder_rejected_launch_evidence_rows.csv").open(
                newline=""
            ) as handle:
                rejected_launch_evidence = list(csv.DictReader(handle))
            self.assertEqual(len(rejected_launch_evidence), 2)
            self.assertEqual(
                {row["mint"] for row in rejected_launch_evidence},
                {old_mint, mint},
            )
            self.assertTrue(
                all(
                    "instruction_not_confirmed_create" in row["rejection_reason"]
                    for row in rejected_launch_evidence
                )
            )

            with (output / "exact_holder_launch_tracker_rows.csv").open(newline="") as handle:
                tracker = next(csv.DictReader(handle))
            self.assertEqual(tracker["eligible_for_fresh_tracker_scope"], "True")
            self.assertEqual(tracker["eligible_for_exact_holder_acceptance"], "True")
            self.assertEqual(tracker["currently_eligible_for_exact_holder_acceptance"], "False")
            self.assertEqual(tracker["tracker_active"], "False")
            self.assertEqual(tracker["tracker_retired_reason"], "capacity_evicted")
            self.assertEqual(tracker["acceptance_valid_until"], "2026-01-01T00:00:18Z")
            self.assertEqual(tracker["acceptance_valid_until_reason"], "capacity_evicted")

            with (output / "exact_holder_balance_state_rows.csv").open(newline="") as handle:
                balances = list(csv.DictReader(handle))
            vault = next(row for row in balances if row["token_account"] == "pool-base-vault")
            self.assertEqual(vault["excluded_from_holder_count"], "True")
            pre_gap = next(row for row in balances if row["signature"] == "transfer-one")
            post_gap = next(row for row in balances if row["signature"] == "post-gap-transfer")
            self.assertEqual(pre_gap["source_quality"], MODULE.NEAR_EXACT)
            self.assertEqual(post_gap["source_quality"], MODULE.OBSERVED)

            with (output / "exact_holder_concentration_rows.csv").open(newline="") as handle:
                concentrations = list(csv.DictReader(handle))
            self.assertEqual(
                {row["source_quality"] for row in concentrations},
                {MODULE.NEAR_EXACT},
            )

            with (output / "exact_holder_decision_state_rows.csv").open(newline="") as handle:
                decision = next(csv.DictReader(handle))
            self.assertEqual(decision["source_quality"], MODULE.NEAR_EXACT)
            self.assertNotEqual(decision["holder_count_near_exact"], "")

            migration_audit = json.loads(
                (output / "exact_holder_migration_carry_forward_audit.json").read_text()
            )
            self.assertEqual(migration_audit["carry_forward_complete_rows"], 1)
            self.assertEqual(migration_audit["confirmed_migration_evidence_rows"], 1)
            self.assertEqual(migration_audit["unconfirmed_migration_rows_rejected"], 2)
            with (output / "exact_holder_rejected_migration_evidence_rows.csv").open(
                newline=""
            ) as handle:
                rejected_migrations = list(csv.DictReader(handle))
            self.assertEqual(len(rejected_migrations), 2)
            self.assertTrue(all(row["rejection_reason"] for row in rejected_migrations))
            migration_proof = json.loads(
                (output / "exact_holder_fresh_launch_to_migration_proof_report.json").read_text()
            )
            self.assertEqual(
                migration_proof["verdict"],
                "near_exact_holder_fresh_launch_tracking_ready",
            )

            with (output / "exact_holder_leakage_audit.csv").open(newline="") as handle:
                leakage = list(csv.DictReader(handle))
            self.assertEqual(leakage[0]["future_holder_state_used"], "False")
            self.assertEqual(leakage[0]["exact_fields_filled_from_proxy"], "False")

    def test_confirmed_launch_without_tracker_blocks_ready_verdicts(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            run = root / "run"
            output = root / "output"
            amm = root / "amm"
            strategy = root / "strategy"
            run.mkdir()
            amm.mkdir()
            strategy.mkdir()

            tracked_mint = "TrackedFreshMintpump"
            tracked_anchor_mint = "TrackedAnchorMintpump"
            untracked_mint = "UntrackedFreshMintpump"
            post_manifest_mint = "PostManifestFreshMintpump"
            activation = datetime(2026, 1, 1, tzinfo=timezone.utc)
            activation_nanos = int(activation.timestamp() * 1_000_000_000)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "schema_version": "exact_holder_tracker_activation_manifest.v1",
                        "relay_session_id": "relay-launch-gap",
                        "tracker_activation_unix_nanos": activation_nanos,
                        "dynamic_fresh_launch_tracking_enabled": True,
                        "max_active_mints": 64,
                        "mint_ttl_seconds": 7200,
                        "subscription_update_failures": 0,
                        "active_mint_count": 2,
                        "active_mints": [tracked_mint, tracked_anchor_mint],
                        "stream_only_required": True,
                        "rpc_holder_snapshot_allowed": False,
                        "dex_as_holder_truth": False,
                        "proxy_as_exact": False,
                        "pool_vaults_counted_as_holders": False,
                        "tracker_rows": [
                            {
                                "mint": tracked_mint,
                                "launch_slot": 10,
                                "launch_signature": "tracked-launch",
                                "launch_observed_at_unix_nanos": activation_nanos
                                + 1_000_000_000,
                                "tracker_created": True,
                                "tracker_created_at_unix_nanos": activation_nanos
                                + 1_010_000_000,
                                "tracker_delay_ms": 10,
                                "tracker_source": "yellowstone_pump_create_dynamic_token_account_filter",
                                "active": True,
                            },
                            {
                                "mint": tracked_anchor_mint,
                                "launch_slot": 12,
                                "launch_signature": "tracked-anchor-launch",
                                "launch_observed_at_unix_nanos": activation_nanos
                                + 3_000_000_000,
                                "tracker_created": True,
                                "tracker_created_at_unix_nanos": activation_nanos
                                + 3_010_000_000,
                                "tracker_delay_ms": 10,
                                "tracker_source": "yellowstone_pump_create_dynamic_token_account_filter",
                                "active": True,
                            }
                        ],
                    }
                )
            )
            write_csv(
                run / "decoded_launch_event_rows.csv",
                [
                    {
                        "mint": tracked_mint,
                        "launch_id": "launch-tracked",
                        "event_observed_at_utc": "2026-01-01T00:00:01Z",
                        "slot": 10,
                        "signature": "tracked-launch",
                        "event_type": "launch_create",
                        "decoded_instruction_name": "create_v2",
                        "source_is_non_rpc": True,
                        "rpc_used": False,
                        "strict_timing_eligible": True,
                        "parse_status": "non_rpc_decoded_launch_create_event",
                        "creator_wallet": "tracked-creator",
                        "bonding_curve": "tracked-curve",
                        "associated_bonding_curve": "tracked-curve-token",
                    },
                    {
                        "mint": untracked_mint,
                        "launch_id": "launch-untracked",
                        "event_observed_at_utc": "2026-01-01T00:00:02Z",
                        "slot": 11,
                        "signature": "untracked-launch",
                        "event_type": "launch_create",
                        "decoded_instruction_name": "create_v2",
                        "source_is_non_rpc": True,
                        "rpc_used": False,
                        "strict_timing_eligible": True,
                        "parse_status": "non_rpc_decoded_launch_create_event",
                        "creator_wallet": "untracked-creator",
                        "bonding_curve": "untracked-curve",
                        "associated_bonding_curve": "untracked-curve-token",
                    },
                    {
                        "mint": tracked_anchor_mint,
                        "launch_id": "launch-tracked-anchor",
                        "event_observed_at_utc": "2026-01-01T00:00:03Z",
                        "slot": 12,
                        "signature": "tracked-anchor-launch",
                        "event_type": "launch_create",
                        "decoded_instruction_name": "create_v2",
                        "source_is_non_rpc": True,
                        "rpc_used": False,
                        "strict_timing_eligible": True,
                        "parse_status": "non_rpc_decoded_launch_create_event",
                        "creator_wallet": "tracked-anchor-creator",
                        "bonding_curve": "tracked-anchor-curve",
                        "associated_bonding_curve": "tracked-anchor-curve-token",
                    },
                    {
                        "mint": post_manifest_mint,
                        "launch_id": "launch-post-manifest",
                        "event_observed_at_utc": "2026-01-01T00:00:04Z",
                        "slot": 13,
                        "signature": "post-manifest-launch",
                        "event_type": "launch_create",
                        "decoded_instruction_name": "create_v2",
                        "source_is_non_rpc": True,
                        "rpc_used": False,
                        "strict_timing_eligible": True,
                        "parse_status": "non_rpc_decoded_launch_create_event",
                        "creator_wallet": "post-manifest-creator",
                        "bonding_curve": "post-manifest-curve",
                        "associated_bonding_curve": "post-manifest-curve-token",
                    },
                ],
            )
            write_csv(
                run / "decoded_holder_event_rows.csv",
                [
                    {
                        "mint": tracked_mint,
                        "launch_id": "launch-tracked",
                        "event_observed_at_utc": "2026-01-01T00:00:01Z",
                        "slot": 10,
                        "signature": "tracked-launch",
                        "token_account": "tracked-holder-account",
                        "owner_wallet": "tracked-holder",
                        "amount_raw": "1000000",
                        "amount_ui": "1",
                        "decimals": "6",
                        "holder_balance_before": "0",
                        "holder_balance_after": "1000000",
                        "update_source": "geyser_token_balance",
                        "update_type": "token_account_balance_created_or_observed",
                    }
                ],
            )
            write_csv(run / "run_gap_events.csv", [])
            write_csv(run / "quant_pumpfun_migration_event_rows.csv", [])
            write_csv(run / "quant_pumpswap_pair_event_rows.csv", [])
            (run / "local_collector_summary.json").write_text(
                json.dumps(
                    {
                        "sequence_gap_count": 0,
                        "downstream_backpressure_count": 0,
                        "unverified_chunk_count": 0,
                    }
                )
            )
            write_csv(amm / "pumpswap_pool_vault_rows.csv", [])
            write_csv(amm / "pumpswap_live_relay_pumpswap_pair_event_rows.csv", [])
            (amm / "pumpswap_amm_coverage_gate.json").write_text(
                json.dumps(
                    {
                        "coverage_ready": True,
                        "amm_research_usable": True,
                        "amm_complete_coverage": False,
                        "decision_time_pool_state_coverage_pct": 90,
                    }
                )
            )
            write_csv(strategy / "post_migration_strategy_feature_rows.csv", [])

            rc = MODULE.build(
                argparse.Namespace(
                    repo_root=str(root),
                    run_dir=str(run),
                    relay_manifest=str(manifest_path),
                    output_dir=str(output),
                    amm_root=str(amm),
                    strategy_root=str(strategy),
                    min_proof_minutes=30.0,
                    min_fresh_launches=1,
                    min_decision_coverage_pct=75.0,
                )
            )
            self.assertEqual(rc, 0)

            proof = json.loads((output / "exact_holder_fresh_launch_proof_report.json").read_text())
            self.assertEqual(
                proof["verdict"],
                "partial_fresh_launches_tracked_no_migration_yet",
            )
            self.assertEqual(proof["confirmed_launches_with_tracker"], 2)
            self.assertEqual(proof["confirmed_launches_without_tracker"], 1)
            self.assertEqual(
                proof["confirmed_launch_tracker_coverage_pct"],
                66.6667,
            )
            self.assertEqual(proof["post_manifest_confirmed_launch_mints"], 1)
            self.assertEqual(
                proof["post_manifest_confirmed_launch_mint_ids"],
                [post_manifest_mint],
            )
            self.assertFalse(proof["launch_tracker_coverage_complete"])

            readiness = json.loads((output / "full_strategy_dataset_readiness.json").read_text())
            self.assertFalse(readiness["full_strategy_dataset_ready"])
            self.assertFalse(readiness["launch_tracker_coverage_complete"])
            self.assertIn(
                "confirmed_launch_tracker_coverage_incomplete",
                readiness["blockers"],
            )

            with (output / "exact_holder_tracker_gap_audit.csv").open(newline="") as handle:
                tracker_gaps = list(csv.DictReader(handle))
            untracked_gap = next(row for row in tracker_gaps if row["mint"] == untracked_mint)
            self.assertIn("tracker_manifest_row_missing", untracked_gap["gap_reason"])
            post_manifest_gap = next(
                row for row in tracker_gaps if row["mint"] == post_manifest_mint
            )
            self.assertEqual(post_manifest_gap["gap_type"], "manifest_snapshot_boundary")
            self.assertIn(
                "launch_after_manifest_snapshot_not_assessed",
                post_manifest_gap["gap_reason"],
            )

    def test_cross_slice_tracker_scope_carries_without_overstating_stream_continuity(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            run_one = root / "run-one"
            run_two = root / "run-two"
            output = root / "output"
            amm = root / "amm"
            strategy = root / "strategy"
            for path in (run_one, run_two, amm, strategy):
                path.mkdir()

            mint = "FreshCrossSliceMintpump"
            launch_signature = "cross-slice-launch"
            activation = datetime(2026, 1, 1, tzinfo=timezone.utc)
            activation_nanos = int(activation.timestamp() * 1_000_000_000)
            tracker_row = {
                "mint": mint,
                "launch_slot": 10,
                "launch_signature": launch_signature,
                "launch_observed_at_unix_nanos": activation_nanos + 1_000_000_000,
                "tracker_created": True,
                "tracker_created_at_unix_nanos": activation_nanos + 1_010_000_000,
                "tracker_delay_ms": 10,
                "tracker_source": "yellowstone_pump_create_dynamic_token_account_filter",
                "subscription_generation": 1,
                "active_mint_count": 1,
                "active": True,
                "retired_at_unix_nanos": None,
                "retired_reason": None,
                "eligible_for_exact_holder_acceptance": True,
                "ineligible_reason": None,
            }
            manifest_one = {
                "schema_version": "exact_holder_tracker_activation_manifest.v1",
                "relay_session_id": "relay-one",
                "relay_started_at_unix_nanos": activation_nanos,
                "tracker_activation_unix_nanos": activation_nanos,
                "dynamic_fresh_launch_tracking_enabled": True,
                "max_active_mints": 64,
                "mint_ttl_seconds": 7200,
                "subscription_update_failures": 0,
                "active_mint_count": 1,
                "active_mints": [mint],
                "tracker_rows": [tracker_row],
                "stream_only_required": True,
                "rpc_holder_snapshot_allowed": False,
                "dex_as_holder_truth": False,
                "proxy_as_exact": False,
                "pool_vaults_counted_as_holders": False,
            }
            manifest_two = {
                **manifest_one,
                "relay_session_id": "relay-two",
                "relay_started_at_unix_nanos": activation_nanos + 900_000_000_000,
                "bootstrap_applied": True,
                "bootstrap_source_relay_session_id": "relay-one",
                "bootstrap_source_run_id": "run-one",
                "bootstrap_preserves_tracker_scope_only": True,
                "bootstrap_stream_continuity_proven": False,
                "handoff_generation": 1,
                "tracker_rows": [
                    {
                        **tracker_row,
                        "bootstrapped_from_prior_relay": True,
                        "origin_relay_session_id": "relay-one",
                        "last_bootstrap_source_run_id": "run-one",
                    }
                ],
            }
            manifest_one_path = root / "manifest-one.json"
            manifest_two_path = root / "manifest-two.json"
            manifest_one_path.write_text(json.dumps(manifest_one))
            manifest_two_path.write_text(json.dumps(manifest_two))

            write_csv(
                run_one / "decoded_launch_event_rows.csv",
                [
                    {
                        "mint": mint,
                        "launch_id": "launch-cross-slice",
                        "event_observed_at_utc": "2026-01-01T00:00:01Z",
                        "slot": 10,
                        "signature": launch_signature,
                        "event_type": "launch_create",
                        "decoded_instruction_name": "create_v2",
                        "source_is_non_rpc": True,
                        "rpc_used": False,
                        "strict_timing_eligible": True,
                        "parse_status": "non_rpc_decoded_launch_create_event",
                        "creator_wallet": "creator-wallet",
                        "bonding_curve": "curve-account",
                        "associated_bonding_curve": "curve-token-account",
                    }
                ],
            )
            write_csv(
                run_one / "decoded_holder_event_rows.csv",
                [
                    {
                        "mint": mint,
                        "launch_id": "launch-cross-slice",
                        "event_observed_at_utc": "2026-01-01T00:00:01Z",
                        "slot": 10,
                        "signature": launch_signature,
                        "token_account": "creator-token-account",
                        "owner_wallet": "creator-wallet",
                        "amount_raw": "100000000",
                        "amount_ui": "100",
                        "decimals": "6",
                        "holder_balance_before": "0",
                        "holder_balance_after": "100000000",
                        "update_source": "geyser_token_balance",
                        "update_type": "token_account_balance_created_or_observed",
                    },
                    {
                        "mint": mint,
                        "launch_id": "launch-cross-slice",
                        "event_observed_at_utc": "2026-01-01T00:00:05Z",
                        "slot": 15,
                        "signature": "pumpfun-transfer",
                        "token_account": "buyer-token-account",
                        "owner_wallet": "buyer-wallet",
                        "amount_raw": "50000000",
                        "amount_ui": "50",
                        "decimals": "6",
                        "holder_balance_before": "0",
                        "holder_balance_after": "50000000",
                        "update_source": "geyser_spl_token_account_update",
                        "update_type": "token_account_balance_update",
                    },
                ],
            )
            write_csv(run_one / "run_gap_events.csv", [])
            write_csv(run_one / "quant_pumpfun_migration_event_rows.csv", [])
            write_csv(run_one / "quant_pumpswap_pair_event_rows.csv", [])

            write_csv(run_two / "decoded_launch_event_rows.csv", [])
            write_csv(
                run_two / "decoded_holder_event_rows.csv",
                [
                    {
                        "mint": mint,
                        "launch_id": "launch-cross-slice",
                        "event_observed_at_utc": "2026-01-01T00:15:11Z",
                        "slot": 111,
                        "signature": "post-migration-transfer",
                        "token_account": "buyer-token-account",
                        "owner_wallet": "buyer-wallet",
                        "amount_raw": "60000000",
                        "amount_ui": "60",
                        "decimals": "6",
                        "holder_balance_before": "50000000",
                        "holder_balance_after": "60000000",
                        "update_source": "geyser_spl_token_account_update",
                        "update_type": "token_account_balance_update",
                    },
                    {
                        "mint": mint,
                        "launch_id": "launch-cross-slice",
                        "event_observed_at_utc": "2026-01-01T00:15:11Z",
                        "slot": 111,
                        "signature": "post-migration-transfer",
                        "token_account": "pool-base-vault",
                        "owner_wallet": "pool-authority",
                        "amount_raw": "400000000",
                        "amount_ui": "400",
                        "decimals": "6",
                        "holder_balance_before": "0",
                        "holder_balance_after": "400000000",
                        "update_source": "geyser_spl_token_account_update",
                        "update_type": "token_account_balance_update",
                    },
                ],
            )
            write_csv(run_two / "run_gap_events.csv", [])
            write_csv(
                run_two / "quant_pumpfun_migration_event_rows.csv",
                [
                    {
                        "mint": mint,
                        "launch_id": "launch-cross-slice",
                        "event_observed_at_utc": "2026-01-01T00:15:10Z",
                        "slot": 110,
                        "signature": "migration-signature",
                        "decoded_instruction_name": "migrate_v2",
                        "transaction_status": "success",
                        "migration_confirmed": True,
                        "migration_evidence_type": "decoded_successful_pump_liquidity_migration",
                        "bonding_curve": "curve-account",
                        "associated_bonding_curve": "curve-token-account",
                        "post_migration_pool": "pool-one",
                        "pool_base_token_account": "pool-base-vault",
                        "pool_quote_token_account": "pool-quote-vault",
                    }
                ],
            )
            write_csv(run_two / "quant_pumpswap_pair_event_rows.csv", [])
            for run in (run_one, run_two):
                (run / "local_collector_summary.json").write_text(
                    json.dumps(
                        {
                            "sequence_gap_count": 0,
                            "downstream_backpressure_count": 0,
                            "r2_streaming_unverified_chunks": 0,
                        }
                    )
                )

            write_csv(amm / "pumpswap_pool_vault_rows.csv", [])
            write_csv(amm / "pumpswap_live_relay_pumpswap_pair_event_rows.csv", [])
            (amm / "pumpswap_amm_coverage_gate.json").write_text(
                json.dumps(
                    {
                        "coverage_ready": True,
                        "amm_research_usable": True,
                        "amm_complete_coverage": False,
                        "decision_time_pool_state_coverage_pct": 90,
                    }
                )
            )
            write_csv(
                strategy / "post_migration_strategy_feature_rows.csv",
                [
                    {
                        "mint": mint,
                        "launch_id": "launch-cross-slice",
                        "decision_ts": "2026-01-01T00:15:12Z",
                    }
                ],
            )

            rc = MODULE.build(
                argparse.Namespace(
                    repo_root=str(root),
                    run_dir=[str(run_one), str(run_two)],
                    relay_manifest=[str(manifest_one_path), str(manifest_two_path)],
                    output_dir=str(output),
                    amm_root=str(amm),
                    strategy_root=str(strategy),
                    min_proof_minutes=30.0,
                    min_fresh_launches=1,
                    min_decision_coverage_pct=75.0,
                )
            )
            self.assertEqual(rc, 0)

            proof = json.loads((output / "exact_holder_fresh_launch_proof_report.json").read_text())
            self.assertEqual(proof["input_run_count"], 2)
            self.assertEqual(proof["input_manifest_count"], 2)
            self.assertEqual(proof["tracker_handoff_gap_count"], 1)
            self.assertEqual(proof["tracker_scope_handoff_gap_count"], 0)
            self.assertEqual(proof["stream_continuity_handoff_gap_count"], 1)
            self.assertEqual(proof["fresh_migrations"], 1)
            self.assertGreater(proof["post_migration_holder_rows"], 0)
            self.assertEqual(proof["decision_time_exact_or_near_exact_rows"], 0)

            migration_audit = json.loads(
                (output / "exact_holder_migration_carry_forward_audit.json").read_text()
            )
            self.assertEqual(migration_audit["carry_forward_complete_rows"], 0)
            readiness = json.loads((output / "full_strategy_dataset_readiness.json").read_text())
            self.assertFalse(readiness["tracker_handoff_continuity_complete"])
            self.assertIn(
                "holder_tracker_handoff_continuity_incomplete",
                readiness["blockers"],
            )
            with (output / "exact_holder_post_migration_state_rows.csv").open(
                newline=""
            ) as handle:
                post_rows = list(csv.DictReader(handle))
            self.assertTrue(post_rows)
            vault = next(row for row in post_rows if row["token_account"] == "pool-base-vault")
            self.assertEqual(vault["excluded_from_holder_count"], "True")
            self.assertEqual(vault["source_quality"], MODULE.OBSERVED)

    def test_missing_cross_slice_bootstrap_time_bounds_quality(self) -> None:
        previous = {
            "schema_version": "exact_holder_tracker_activation_manifest.v1",
            "relay_session_id": "relay-one",
            "tracker_activation_unix_nanos": 100,
            "dynamic_fresh_launch_tracking_enabled": True,
            "max_active_mints": 64,
            "mint_ttl_seconds": 7200,
            "active_mints": ["FreshMintpump"],
            "tracker_rows": [
                {
                    "mint": "FreshMintpump",
                    "tracker_created": True,
                    "active": True,
                    "eligible_for_exact_holder_acceptance": True,
                    "tracker_created_at_unix_nanos": 200,
                    "launch_observed_at_unix_nanos": 150,
                    "launch_slot": 1,
                }
            ],
            "stream_only_required": True,
            "rpc_holder_snapshot_allowed": False,
            "dex_as_holder_truth": False,
            "proxy_as_exact": False,
            "pool_vaults_counted_as_holders": False,
        }
        current = {
            **previous,
            "relay_session_id": "relay-two",
            "relay_started_at_unix_nanos": 1_000,
            "active_mints": [],
            "tracker_rows": [],
            "bootstrap_applied": False,
        }

        combined, gaps, errors = MODULE.merge_tracker_manifest_lineage(
            [previous, current]
        )

        self.assertEqual(errors, [])
        self.assertEqual(len(gaps), 1)
        self.assertEqual(
            gaps[0]["gap_reason"],
            "relay_handoff_bootstrap_missing_or_lineage_mismatch",
        )
        row = next(row for row in combined["tracker_rows"] if row["mint"] == "FreshMintpump")
        self.assertFalse(row["active"])
        self.assertEqual(row["retired_at_unix_nanos"], 1_000)


if __name__ == "__main__":
    unittest.main()
