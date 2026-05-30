# Supply Denominator Policy

Policy version: `phase102.supply_denominator.v1`

This policy separates Pump.fun curve-economic supply from RPC mint supply so market-cap, curve-progress, holder, and risk metrics do not silently mix incompatible denominators.

## Canonical Sources

- `token_supply_curve_economic_raw/ui`: the BondingCurve `token_total_supply` decoded from the official Pump.fun IDL when present.
- `token_supply_protocol_constant_raw/ui`: the Pump.fun economic supply convention used only when the BondingCurve supply is not present in the stream artifact.
- `token_supply_rpc_mint_raw/ui`: the current RPC mint supply returned by `getTokenSupply` or mint account decode. This is audit-only when it diverges from curve-economic supply.
- `token_supply_mint_account_raw/ui`: the current mint-account supply from RPC account decode. This is also audit-only when it diverges.

## Denominator Rules

- Reserve-implied market cap uses `token_supply_curve_economic_raw/ui`, falling back to `token_supply_protocol_constant_raw/ui` with explicit provenance.
- Curve progress uses BondingCurve real reserves with the curve-economic supply convention.
- Holder count, holder growth, holder churn, holder stickiness, paperhand status, top-holder, and dev-holding state are stream owner-summed token-account metrics. They do not require RPC supply and must not call holder RPC APIs.
- Top-holder and dev-holding percentages report the canonical `pct_of_curve_economic_supply`. A separate `pct_of_rpc_mint_supply` may be shown only as diagnostic audit output.
- RPC mint supply must not become canonical while `token_supply_semantics_status` is `mint_supply_differs_from_curve_economic_supply`, `v2_supply_semantic_difference`, `current_snapshot_timebase_mismatch`, or `unresolved_requires_protocol_confirmation`.
- Supply mismatch increases diagnostic uncertainty and `supply_semantics_risk`; it is not a malicious label.
- `threshold_tuning_allowed` remains false while any strategy-used denominator is unresolved.

## Timebase Rules

RPC supply calls are current snapshots unless a same-slot proof exists. A current RPC snapshot must not invalidate launch-time stream state. If the RPC context slot differs from the BondingCurve stream slot, classify it as `current_rpc_snapshot_not_same_slot` or `current_rpc_snapshot_after_stream_but_recent`.

## Audit Display

Reports and dashboards should display both economic and RPC supply where available, with:

- selected denominator
- reason selected
- RPC diagnostic-only flag
- mismatch ratio
- policy version
