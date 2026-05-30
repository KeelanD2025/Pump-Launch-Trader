# Rug Outcome Labels

Phase 104 adds explicit post-event outcome labels for Pump.fun launch diagnostics. These labels are used to validate whether the risk layer missed rug-like outcomes. They are not live entry features and must never be used as pre-entry signals.

## Label Policy

- `rug_like_outcome` is a post-event outcome label, not a maliciousness claim.
- External manual checks, such as Dexscreener review, may be used for calibration evidence but are not live edge features.
- Missing stream evidence is `unavailable`, never zero.
- Holder metrics remain stream-authoritative and must not be repaired by RPC.
- RPC mint supply remains diagnostic-only and must not be canonical.
- Threshold tuning remains blocked until outcome labels and false negatives are validated on a larger controlled sample.

## Labels

- `dead_within_60s`: price path shows at least 95% drawdown within 60 seconds.
- `dead_within_180s`: price path shows at least 95% drawdown within 180 seconds.
- `dead_within_300s`: price path shows at least 95% drawdown within 300 seconds.
- `dead_within_900s`: reserved for longer post-launch windows when enough horizon exists.
- `rapid_collapse_80pct`: price path shows at least 80% drawdown from observed peak.
- `rapid_collapse_95pct`: price path shows at least 95% drawdown from observed peak.
- `no_buy_followthrough`: launch has no meaningful follow-through buys before sell pressure.
- `volume_evaporated`: early buy flow stops quickly and sell pressure appears.
- `holder_collapse`: stream holder churn indicates rapid loss of holders.
- `top_holder_or_dev_dumped`: stream state observes creator or top-holder sell/dump behavior.
- `curve_stalled`: curve progress stalls after launch when curve data is available.
- `liquidity_exit_proxy`: stream reserves/price imply liquidity exit behavior.
- `post_launch_price_never_recovered`: price does not recover after collapse within the observation horizon.
- `rug_like_outcome`: aggregate post-event outcome, true when one or more rug-like outcome labels are active.
- `inconclusive_insufficient_horizon`: not enough stream horizon to compute a canonical post-event outcome.

## External Manual Evidence

Manual Dexscreener review may be recorded as:

- `external_dexscreener_user_observation = user_reported_dead`
- `external_observation_source = user_manual_check`
- `external_observation_used_for_calibration = true`

This evidence can identify false negatives in the stream labels, but it is never a pre-entry feature.
