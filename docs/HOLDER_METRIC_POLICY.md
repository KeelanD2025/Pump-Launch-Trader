# Holder Metric Policy

Phase 96 makes stream-derived holder state the only production and research authority for holder count, holder growth, holder churn, holder stickiness, top-holder concentration, dev holdings, and paperhand behaviour.

## Policy

1. Stream token-account balance events are authoritative.
2. Holder count is owner-summed token-account balance state.
3. RPC holder APIs are audit-only.
4. RPC current snapshots must not invalidate launch-time stream state.
5. Missing stream holder data is unavailable, not repaired silently by RPC.
6. Holder metrics are not allowed to spend RPC credits in normal runs.

## Canonical Algorithm

- Token-account balance updates are keyed by `(mint, token_account)`.
- Owner balances are rebuilt from token-account truth after every holder update.
- Holder count includes owners whose summed raw balance is above dust.
- Top-holder concentration uses max owner-summed balance, excluding configured curve/program/burn accounts where applicable.
- Dev holding uses the owner-summed creator/dev wallet balance.
- `sold_90pct` is behaviour only; it does not remove a holder unless the owner balance crosses to zero/dust.

## RPC Boundary

RPC may be used only for explicit offline audit micro-tests with budget, cache, and ledger enabled. RPC audit output must not overwrite stream holder state or block stream-holder readiness.
