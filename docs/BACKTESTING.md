# Backtesting

Deterministic replay and no-lookahead validation are now implemented in a practical first pass.

## Implemented now

- append-only normalized event logs
- deterministic replay ordering from persisted records
- replay-by-run and replay-by-scenario filtering for stored runtimes
- no-lookahead timestamp validation
- fee-aware and slippage-aware round-trip simulation
- token label generation for replayed state
- CLI `replay`, `backtest`, `label`, `run-fixture-suite`, and `run-paper --from-store` flows

Phase 14 additions:

- fixture suite writes isolated run/scenario records so repeated runs do not mix state
- `run-paper --from-store --latest-run` replays the selected run through the same supervisor path used online
- scenario-end liquidation is available for deterministic fixture PnL measurement
- data-gap state is scoped and reset across scenarios unless the gap is explicitly global

Phase 16 adds:

- replay/live equivalence validation with `validate-replay-equivalence`
- auto-generated per-run research artifacts and attribution reports
- wall-clock-based latest-run selection so replay/export commands do not drift onto stale synthetic event timestamps

Phase 17 adds:

- deterministic shred-exit fixture scenarios for early-warning exits
- saved-loss versus opportunity-cost reporting for tentative exits
- confirmation-level comparisons across tentative, Geyser-processed, account-effect, and reorg outcomes

Phase 18 adds:

- `--latest-shred-exit-run` selection so replay targets the newest completed shred-defense run directly
- persisted shred-exit calibration loaded from prior completed runs only
- `backtest-shred-exit` counterfactual comparisons for early warning, Geyser-processed, account-effect, confirmed, and no-exit baselines
- latency sensitivity and threshold sensitivity CSV artifacts for offline research

Phase 19 adds:

- deterministic early-intent replay equivalence for mocked live-data paper
- replay ordering from persisted `runtime_sequence_number`
- calibration snapshot hashes persisted in run metadata and reused for equivalence replay
- `validate-replay-equivalence --latest-run --explain`, which writes both `replay_equivalence.md` and `replay_equivalence_debug.md`
- replayed shred-exit saved-loss, opportunity-cost, false-positive, mismatch, and reorg counters in `run-paper --from-store --latest-shred-exit-run`
- run selection now distinguishes source runs, derived runs, and report/export runs so equivalence/backtest commands do not silently target report artifacts
- `--latest-run` excludes report/export/analysis runs by default; use `--latest-source-run`, `--latest-mocked-early-intent-run`, or `--latest-shred-exit-run` when the replay target must be a specific source family
- CSV backtest/export artifacts now carry run/source/calibration metadata so offline analysis can reconstruct the replay context without re-querying local state
- mock deshred live-data paper follows the same replay-equivalence rules as mock early-intent paper before any real provider-backed deshred stream should be trusted
- the multisource early-intent suite now covers deshred-plus-duplicate-source cases so source precedence and dedup behavior are proven under replay before any real raw-shred work is attempted
- `smoke-multisource-early-intent` is the operational companion to that suite: it can run in pure mock mode for deterministic validation or write a missing-endpoint/unsupported report for a real-provider attempt without changing replay assumptions
- `collect-live-paper` presets are data-collection wrappers around the same replayable paper pipeline, so checkpoint artifacts can later be compared against stored replay/export output instead of treated as a separate execution path
- `collect-first-live-paper` is the more explicit first real collection profile. It stores the same replayable artifacts, but it also writes stream-only proof and first-run operational summaries intended for the first real Geyser/deshred environment validation
- `analyze-live-collection` grades a collected run as `insufficient_data`, `smoke_only`, `usable_for_basic_feature_distribution`, `usable_for_strategy_backtest`, `usable_for_walk_forward_candidate`, or `unusable_due_to_gaps`
- `check-backtest-readiness` is the hard gate before drawing strategy conclusions. It checks duration, token/lifecycle coverage, feature snapshots, fills, holder confidence, data gaps, replayability, and zero-RPC proof
- `run-research-cycle` wraps those steps and only runs replay/export/backtest work when readiness passes; short smoke collections are expected to fail readiness and produce a blocker report instead of a misleading backtest summary
- Phase 26 adds autopilot thresholds on top of that manual flow. `run-autopilot` uses `autopilot.backtest_thresholds` so unattended collection does not run replay/backtest work on tiny smoke datasets or on runs that violate stream-only proof
- the autopilot path still writes the same readiness blocker reports. If duration, token coverage, feature snapshots, holder confidence, or zero-RPC proof are insufficient, the daemon records the blockers and skips the research cycle instead of forcing a backtest

What-if replay is still intentionally separate from equivalence replay: equivalence uses the recorded calibration snapshot, while a future what-if mode may opt into newer calibration.

Historical regime-segmented optimization is still not implemented yet.
