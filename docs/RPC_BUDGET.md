# RPC Budget

`RpcBudgetManager` is the sole gatekeeper for non-hot-path RPC usage.

## Current policy features

- daily and monthly credit budgets
- per-method limits
- emergency reserve protection
- live-mode fail-closed behavior when budget state is unknown
- caller/reason attribution
- provider-specific cost maps
- append-only ledger entries suitable for later durable storage
- guarded integration with the live executor path

## Allowed reason categories

- cold start
- program config validation
- execution blockhash
- execution send
- execution status fallback
- emergency reconciliation
- dev/test fixtures
- non-production IDL/account verification
