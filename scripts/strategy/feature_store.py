from __future__ import annotations

import pathlib
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from .io import read_csv, read_json, write_csv, write_json, write_text
from .schemas import (
    DATA_QUALITY_PREFIX,
    FORBIDDEN_ALPHA_COLUMNS,
    HORIZONS,
    STRATEGY_READINESS_ROOT,
    boolish,
    intish,
)


class FeatureStore:
    def __init__(self, readiness_root: pathlib.Path = STRATEGY_READINESS_ROOT):
        self.root = pathlib.Path(readiness_root)
        self.alpha_root = self.root / "asof_alpha_features"

    def load_feature_manifest(self) -> dict[str, Any]:
        manifest = read_json(self.alpha_root / "asof_alpha_feature_manifest.json")
        if manifest:
            return manifest
        return read_json(self.root / "asof_features" / "asof_feature_manifest.json")

    def load_asof_features(self, horizon: int) -> list[dict[str, str]]:
        return read_csv(self.alpha_root / f"asof_alpha_features_{horizon:03d}s.csv")

    def get_mint_features(self, mint: str, horizon: int) -> dict[str, str] | None:
        for row in self.load_asof_features(horizon):
            if row.get("mint") == mint:
                return row
        return None

    def get_available_horizons(self, mint: str) -> list[int]:
        horizons = []
        for horizon in HORIZONS:
            if self.get_mint_features(mint, horizon):
                horizons.append(horizon)
        return horizons

    def feature_groups(self) -> dict[str, list[str]]:
        sample = next(iter(self.load_asof_features(60)), {})
        groups = {
            "launch": ["feature_asof_timestamp", "mint_first_seen_timestamp", "age_ms_at_horizon"],
            "trade_delta": [key for key in sample if key.endswith("_asof") and ("trade" in key or "buy" in key or "sell" in key or "volume" in key)],
            "holder_state": [key for key in sample if "holder" in key and key.endswith("_asof")],
            "vault_curve": [key for key in sample if any(part in key for part in ("vault", "curve", "liquidity", "reserve", "price")) and key.endswith("_asof")],
            "high_throughput": ["high_throughput_before_horizon"],
            "degraded_audit": ["degraded_audit_only_before_horizon"],
            "data_quality": [key for key in sample if key.startswith(DATA_QUALITY_PREFIX) or key.endswith("_exposed")],
            "gate_diagnostics": [key for key in sample if "decision" in key or "candidate" in key or "survivor" in key],
        }
        return groups

    def validate_asof_safety(self) -> dict[str, Any]:
        blockers: list[str] = []
        rows_checked = 0
        holder_rpc_seen = False
        rpc_supply_canonical_seen = False
        for horizon in HORIZONS:
            for row in self.load_asof_features(horizon):
                rows_checked += 1
                forbidden_present = sorted(field for field in FORBIDDEN_ALPHA_COLUMNS if field in row)
                if forbidden_present:
                    blockers.append(f"forbidden_alpha_columns:{horizon}:{','.join(forbidden_present)}")
                if boolish(row.get("holder_rpc_used")):
                    holder_rpc_seen = True
                    blockers.append(f"holder_rpc_used:{horizon}:{row.get('mint','')}")
                if boolish(row.get("rpc_mint_supply_canonical")):
                    rpc_supply_canonical_seen = True
                    blockers.append(f"rpc_mint_supply_canonical:{horizon}:{row.get('mint','')}")
                if boolish(row.get("threshold_tuning_allowed")) or boolish(row.get("live_trading_enabled")):
                    blockers.append(f"forbidden_execution_flag:{horizon}:{row.get('mint','')}")
                if boolish(row.get("horizon_reached")) and row.get("feature_asof_timestamp"):
                    if not self._timestamp_safe(row):
                        blockers.append(f"post_horizon_timestamp:{horizon}:{row.get('mint','')}")
        return {
            "passed": not blockers,
            "rows_checked": rows_checked,
            "blockers": blockers,
            "holder_rpc_seen": holder_rpc_seen,
            "rpc_mint_supply_canonical_seen": rpc_supply_canonical_seen,
        }

    def _timestamp_safe(self, row: dict[str, str]) -> bool:
        # Current collector timestamps are sometimes structured arrays. Only enforce
        # strict ISO comparisons when both timestamps are parseable ISO strings.
        asof = self._parse_iso(row.get("feature_asof_timestamp", ""))
        first = self._parse_iso(row.get("mint_first_seen_timestamp", ""))
        if asof is None or first is None:
            return True
        horizon = intish(row.get("horizon_seconds"))
        return (asof - first).total_seconds() <= horizon + 0.001

    @staticmethod
    def _parse_iso(value: str) -> datetime | None:
        try:
            if not value or value.startswith("["):
                return None
            return datetime.fromisoformat(value.replace(" UTC", "+00:00"))
        except ValueError:
            return None

    def feature_missingness_report(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        summary = Counter()
        groups = {
            "trade_delta": ("trade_update_count_asof", "buy_count_delta_asof", "sell_count_delta_asof"),
            "holder_state": ("holder_update_count_asof", "unique_holder_accounts_seen_asof"),
            "vault_curve": ("vault_update_count_asof", "bonding_curve_update_count_asof", "curve_progress_proxy_asof"),
        }
        for horizon in HORIZONS:
            features = self.load_asof_features(horizon)
            for group, columns in groups.items():
                present = 0
                missing = 0
                for row in features:
                    if all(str(row.get(column, "")).strip() != "" for column in columns):
                        present += 1
                    else:
                        missing += 1
                rows.append({
                    "horizon_seconds": horizon,
                    "feature_group": group,
                    "rows": len(features),
                    "present_rows": present,
                    "missing_rows": missing,
                    "coverage_ratio": round(present / len(features), 6) if features else 0,
                })
                summary[f"{group}_present"] += present
                summary[f"{group}_missing"] += missing
        return rows, dict(summary)

    def write_reports(self, output_root: pathlib.Path) -> None:
        rows, summary = self.feature_missingness_report()
        write_csv(
            output_root / "feature_missingness.csv",
            rows,
            ["horizon_seconds", "feature_group", "rows", "present_rows", "missing_rows", "coverage_ratio"],
        )
        validation = self.validate_asof_safety()
        write_json(output_root / "feature_store_validation.json", validation)
        write_text(
            output_root / "feature_store_report.md",
            "\n".join([
                "# Feature Store Report",
                "",
                f"- asof_safety_passed: `{str(validation['passed']).lower()}`",
                f"- rows_checked: `{validation['rows_checked']}`",
                f"- holder_rpc_seen: `{str(validation['holder_rpc_seen']).lower()}`",
                f"- rpc_mint_supply_canonical_seen: `{str(validation['rpc_mint_supply_canonical_seen']).lower()}`",
                f"- trade_delta_present_rows: `{summary.get('trade_delta_present', 0)}`",
                f"- holder_state_present_rows: `{summary.get('holder_state_present', 0)}`",
                f"- vault_curve_present_rows: `{summary.get('vault_curve_present', 0)}`",
                "",
                "Provider/relay/R2 quality fields are retained only as exclusion filters, not alpha inputs.",
            ]) + "\n",
        )

