# Rug / Bundle / Risk Engine

This engine is a diagnostic, edge-ready evidence layer for Pump.fun launches. It does not label tokens as malicious, does not tune strategy thresholds, and does not enable trading. It turns stream evidence and explicitly bounded enrichment evidence into time-safe risk features and post-event labels.

## Core Policy

- Holder metrics are stream-authoritative owner-summed token-account state.
- RPC must not be used for holder count, top-holder, dev holding, holder growth, churn, or stickiness.
- Bundle-like means stream proxy evidence only unless a provider/API confirms a bundle.
- Risk classifications are diagnostic labels, not accusations.
- Missing evidence is unavailable with a reason, never zero.
- Post-event labels must not be used as pre-entry features.

## Risk Families

The engine tracks dev exit, top-holder dump, holder concentration, holder churn, paperhand behaviour, fake momentum, sell-absorption failure, no-buy-gap, same-slot/shape/account/signer bundle-like signals, priority-fee clusters, common-funder, wallet-activity, metadata/social absence or spam, supply semantics, post-migration unknowns, data quality, stream gaps, and provider confirmation requirements.

## Time Safety

- `pre_entry_feature`: available before the entry decision.
- `decision_time_feature`: available at the decision timestamp.
- `post_event_label`: future outcome label only.
- `enrichment_late_feature`: available after RPC/HTTP/provider work, not live edge unless explicitly budgeted.

## Readiness

The current expected state is diagnostic-ready, formal-backtest blocked, and threshold-tuning blocked until provider confirmation, supply semantics, sample size, and leakage gates pass.
