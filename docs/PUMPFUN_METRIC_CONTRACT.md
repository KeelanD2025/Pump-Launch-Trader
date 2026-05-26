# Pump.fun Metric Contract

This is the authoritative Phase 88 metric contract. Missing values are unavailable, never zero. Threshold tuning is blocked unless this contract, real-run parity, sample-size, and walk-forward gates pass.

| # | Metric family | Required layer | Status | Code owner |
|---:|---|---|---|---|
| 1 | run identity and timing | edge_collector | implemented | crates/runtime + crates/cli |
| 2 | R2 / manifest proof | edge_collector | implemented | crates/r2-storage + crates/cli |
| 3 | stream-only / RPC proof | edge_collector | implemented | crates/runtime + rpc_budget |
| 4 | normalized event counts | research_replay | implemented | crates/runtime + crates/cli |
| 5 | Pump create / launch metadata | edge_collector | implemented | crates/runtime/src/live_source.rs |
| 6 | bonding curve reserves | edge_collector | implemented | crates/runtime/src/live_source.rs |
| 7 | reserve-implied price | research_replay | implemented | crates/features/src/metric_engine.rs |
| 8 | realized buy/sell trade price | research_replay | partial | crates/runtime/src/live_source.rs + research replay |
| 9 | OHLC price path | research_replay | implemented | generate-canonical-labels |
| 10 | market cap | research_replay | implemented | crates/features/src/metric_engine.rs |
| 11 | curve progress | research_replay | implemented | crates/features/src/metric_engine.rs |
| 12 | buy/sell flow | research_replay | implemented | crates/state + crates/features |
| 13 | unique buyers / sellers | research_replay | implemented | crates/state |
| 14 | holder count | research_replay | partial | crates/state + crates/features |
| 15 | holder growth | research_replay | partial | crates/features |
| 16 | holder churn | research_replay | partial | crates/features |
| 17 | holder stickiness | research_replay | partial | crates/features |
| 18 | top-holder concentration | research_replay | partial | crates/state + crates/features |
| 19 | dev holdings | research_replay | partial | crates/features/src/metric_engine.rs |
| 20 | bundle detection | enrichment_worker | requires_enrichment | run-enrichment-worker |
| 21 | common funder | enrichment_worker | requires_enrichment | run-enrichment-worker |
| 22 | funding graph | enrichment_worker | requires_enrichment | run-enrichment-worker |
| 23 | wallet age / wallet history | enrichment_worker | requires_enrichment | run-enrichment-worker |
| 24 | transaction fingerprints | edge_collector | partial | crates/runtime/src/live_source.rs |
| 25 | fee / priority fee / compute budget | edge_collector | implemented | crates/runtime/src/live_source.rs |
| 26 | Jito/bundle-like evidence where observable | research_replay | partial | features/enrichment |
| 27 | fake momentum | research_replay | implemented | crates/features + crates/risk |
| 28 | sell absorption | research_replay | partial | crates/features |
| 29 | cost basis / profit pressure | research_replay | partial | crates/features |
| 30 | cohort relative strength | research_replay | implemented | crates/features |
| 31 | rug detection | research_replay | implemented | crates/risk + crates/features |
| 32 | early sell defense | deshred_worker | requires_deshred | crates/runtime/src/shred_exit.rs |
| 33 | deshred metrics | deshred_worker | requires_deshred | crates/runtime/src/shred_exit.rs |
| 34 | raw-shred metrics | raw_shred_worker | requires_raw_shred | ingest-shred |
| 35 | metadata URI | edge_collector | implemented | crates/runtime/src/live_source.rs |
| 36 | website / X / Telegram / Discord | enrichment_worker | requires_enrichment | run-enrichment-worker |
| 37 | social count | enrichment_worker | requires_enrichment | run-enrichment-worker |
| 38 | MFE / MAE | research_replay | implemented | generate-canonical-labels |
| 39 | return-window labels | research_replay | implemented | generate-canonical-labels |
| 40 | missed winners | research_replay | implemented | scripts/phase85_diagnostic_lab.mjs |
| 41 | false discards | research_replay | implemented | scripts/phase85_diagnostic_lab.mjs |
| 42 | decisions / rejections | research_replay | implemented | crates/decision |
| 43 | fills / exits | research_replay | implemented | crates/sim + crates/executor |
| 44 | raw PnL | research_replay | implemented | crates/sim + dossiers |
| 45 | executable PnL | research_replay | implemented | crates/sim + phase73 sanity |
| 46 | baseline strategy metrics | research_replay | implemented | quant diagnostics |
| 47 | regime metrics | research_replay | implemented | quant diagnostics |
| 48 | execution sensitivity | research_replay | implemented | quant diagnostics |
| 49 | readiness / backtest gates | research_replay | implemented | validate-backtest-readiness-v2 + validate-metric-contract |
| 50 | post-migration / PumpSwap metrics | enrichment_worker | unsupported | run-enrichment-worker |
