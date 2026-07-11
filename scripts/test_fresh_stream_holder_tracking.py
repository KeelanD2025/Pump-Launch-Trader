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
                "subscription_update_failures": 0,
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
                        "active": True,
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
                        "creator_wallet": "old-creator",
                        "bonding_curve": "old-curve",
                        "associated_bonding_curve": "old-curve-token",
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
                    }
                ],
            )
            write_csv(
                run / "quant_pumpfun_migration_event_rows.csv",
                [
                    {
                        "mint": mint,
                        "launch_id": "launch-fresh",
                        "event_observed_at_utc": "2026-01-01T00:00:10Z",
                        "slot": 20,
                        "signature": "migration-signature",
                        "post_migration_pool": "pool-one",
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
                [
                    {
                        "pool": "pool-one",
                        "mint": mint,
                        "pool_base_token_account": "pool-base-vault",
                        "pool_quote_token_account": "pool-quote-vault",
                    }
                ],
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
            self.assertEqual(proof["source_quality_counts"][MODULE.NEAR_EXACT], 1)
            self.assertGreater(proof["token_account_update_rows"], 0)
            self.assertEqual(proof["provider_or_sequence_gap_count"], 1)
            self.assertTrue(proof["source_integrity_proven"])
            guard = json.loads((output / "exact_holder_no_stale_mint_guard.json").read_text())
            self.assertEqual(guard["accepted_mints"], [mint])
            self.assertNotIn(old_mint, guard["accepted_mints"])

            with (output / "exact_holder_launch_tracker_rows.csv").open(newline="") as handle:
                tracker = next(csv.DictReader(handle))
            self.assertEqual(tracker["eligible_for_fresh_tracker_scope"], "True")
            self.assertEqual(tracker["eligible_for_exact_holder_acceptance"], "True")

            with (output / "exact_holder_balance_state_rows.csv").open(newline="") as handle:
                balances = list(csv.DictReader(handle))
            vault = next(row for row in balances if row["token_account"] == "pool-base-vault")
            self.assertEqual(vault["excluded_from_holder_count"], "True")

            with (output / "exact_holder_leakage_audit.csv").open(newline="") as handle:
                leakage = list(csv.DictReader(handle))
            self.assertEqual(leakage[0]["future_holder_state_used"], "False")
            self.assertEqual(leakage[0]["exact_fields_filled_from_proxy"], "False")


if __name__ == "__main__":
    unittest.main()
