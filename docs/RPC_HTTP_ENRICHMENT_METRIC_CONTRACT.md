# RPC / HTTP Enrichment Metric Contract

Phase 101 defines the bounded enrichment layer for Pump.fun metric validation. This layer is diagnostic only: it must not tune thresholds, enable live trading, run broad enrichment, or overwrite stream-authoritative holder state.

## Non-Negotiable Boundaries

- Holder count, holder growth, holder churn, holder stickiness, top-holder concentration, dev holdings, and paperhand metrics use stream owner-summed token-account state only.
- RPC holder APIs are audit-only and disabled by default.
- RPC mint supply is not canonical for Pump.fun market-cap or holder-denominator metrics while supply semantics remain unresolved.
- Bundle-like stream evidence is a proxy. It is not provider-confirmed bundle evidence.
- Bounded same-shape transaction checks are not provider-confirmed bundles.
- Risk classifications are diagnostic only and must not label a token malicious as fact.
- Missing or unavailable enrichment evidence is recorded with an explicit reason and is never coerced to zero.

## Budget Profile

`rpc_risk_hardening_micro` is the only approved Phase 101 profile:

- `max_tokens = 3`
- `max_rpc_calls_total = 90`
- `max_rpc_calls_per_token = 30`
- `max_rpc_calls_per_family_per_token = 10`
- `max_wallets_per_token = 10`
- `max_signatures_per_wallet = 10`
- `max_transactions_per_token = 25`
- `max_http_metadata_fetches_per_token = 3`
- `cache_required = true`
- `ledger_required = true`

Every RPC/API call must be ledgered without secrets, full provider URLs, or private key material.

## Provider Confirmation Rules

`provider_confirmed_bundle` can only be true when an explicit provider/API response confirms a bundle. Stream clustering and bounded RPC same-shape checks may raise diagnostic risk, but they must remain `stream_bundle_like_signal` or `bounded_rpc_same_shape_cluster`.

## Supply Semantics Policy

The Pump.fun official-IDL BondingCurve economic supply and explicit protocol constant remain the canonical denominator for stream curve/market-cap diagnostics unless disproven. `getTokenSupply` and mint account supply are diagnostic comparisons only until mismatches are resolved.

## Time Safety

Enrichment fetched after the decision is `enrichment_late_feature`. Post-event labels such as MFE/MAE, rapid collapse, future dev sells, and future top-holder dumps must not be marked pre-entry safe.
