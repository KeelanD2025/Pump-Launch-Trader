# Early Rug Detection Rules

Phase 106 diagnostic-only candidate rules.

These rules are not production thresholds, not trading rules, and not maliciousness labels. They are candidate pre-entry or decision-time signals for later controlled validation.

## Boundaries

- Rug/death outcomes remain post-event labels.
- Dexscreener/manual observations are calibration evidence only.
- Holder count, top-holder, dev holding, holder growth, churn, stickiness, and paperhand metrics remain stream-authoritative.
- RPC holder repair is not allowed for these rules.
- Bundle-like means stream proxy unless a provider/API explicitly confirms a bundle.
- Funding graph signals are diagnostic cluster candidates, not attribution.

## Candidate Rules

- `no_buy_followthrough_by_30s`: buy count remains at or below one while sell pressure appears by 30s.
- `no_buy_followthrough_by_60s`: buy count remains at or below one while sell pressure appears by 60s.
- `volume_evaporated_by_60s`: buy activity ends early and sells or no-buy gap appear by 60s.
- `holder_collapse_by_60s`: owner-summed holder count contracts materially by 60s.
- `top_holder_or_dev_dump_before_180s`: stream holder/trade evidence shows dev or top-holder sell pressure by 180s.
- `fake_momentum_no_holder_growth`: price or buy count rises without unique-buyer/holder growth.
- `high_buy_count_low_unique_buyers`: many buys but a low unique-buyer ratio.
- `same_slot_bundle_like_plus_holder_concentration`: stream bundle-like evidence appears with concentrated early holdings.
- `common_funder_plus_bundle_like_signal`: bounded funding candidate and stream bundle-like evidence both appear.
- `high_top_holder_plus_no_buy_gap`: top-holder concentration and no-buy gap co-occur.
- `supply_semantics_uncertain_plus_fake_momentum`: supply uncertainty combines with fake-momentum evidence.
- `liquidity_exit_proxy_before_300s`: price/flow evidence shows exit-like liquidity pressure before 300s.

## Unavailable Behaviour

If a required stream, enrichment, or provider field is missing, the rule must emit `unavailable` with a reason. Missing values must never be converted to zero.

