# Features

The feature surface described in the project brief is intentionally broad. The current implementation computes a real core subset and explicitly marks deeper families as unavailable instead of inventing values.

## Implemented now

- launch identity and metadata-shape basics
- creator/dev state and dev-holding features
- bonding-curve and reserve features
- price-path and trade-flow features
- holder growth, concentration, and cost-basis features
- bundle-evidence and rug-precursor features
- cohort-rank, capital-rotation, survival, and data-quality features
- advanced computed subsets for:
  - cohort ranking and rank velocity
  - holder cost basis, realized/unrealized PnL pressure, and free-roll risk
  - absorption and fake-momentum detection
  - holder lifecycle/stickiness
  - transaction/client fingerprint clustering
  - funding graph suspicion and quality
  - fee-war / competition pressure

## Guarded for later implementation

- wallet embeddings
- factory and client-fingerprint families
- event grammar and motif models
- path-similarity templates
- hazard/survival forecasts and anomaly models

All unavailable families are registered in `FeatureRegistry` with `Unavailable` status and zero confidence.

Every feature value now carries:

- status
- confidence
- source confidence
- missing-data penalty
- deterministic feature snapshot hashing for decisions and replay

- explicit identifiers and versions
- declared dependencies
- no-lookahead safety
- missing-data behavior
- confidence and normalization metadata
- cost and memory budget awareness

Phase 14 calibration relies heavily on the currently computed advanced subset:

- cohort leadership, cohort attention share, and rank velocity
- holder cost basis, unrealized PnL overhang, and free-roll pressure
- absorption recovery and fake-momentum authenticity checks
- holder stickiness, holder half-life, and churn quality
- transaction fingerprint concentration
- funding graph suspicion and quality
- observed fee-war and minimum-move-to-cover-fees pressure

Phase 15 adds an explicit expected executable edge layer on top of those computed features. Paper entries now require:

- positive expected net edge after fee, slippage, impact, and latency safety buffers
- sufficient edge confidence
- acceptable bundle/fake-momentum/dev/top-holder risk
- strategy-specific fit rather than generic signal quality alone

Phase 16 keeps that edge model online during real or mocked Geyser runs. Feature snapshots in live-data paper therefore continue to record:

- source confidence and missing-shred penalties
- expected edge, minimum required move, and fee/impact drag proxies
- replay-safe hashes for the exact snapshot used by each decision

Unavailable features remain explicit `Unavailable` entries with zero confidence so Geyser-only or partial-data runs never fabricate edge.

Phase 17 adds `shred_exit_defense` features, including:

- tentative sell counts, source confidence, and warning levels
- dev/top-holder/bundle/whale tentative sell counts
- preconfirmation exit confidence and exit-threat index score
- early-intent to Geyser/account-effect/rooted confirmation latencies
- saved-loss and opportunity-cost measurements for tentative exits

Phase 18 extends that family with:

- latency advantage, required latency budget, and latency-edge ratio
- exit-can-land-before-impact and post-sell absorption probability
- expected saved loss, expected opportunity cost, and net emergency-exit benefit
- persisted shred-exit calibration buckets that can be replayed and exported without using future information during a live decision

Phase 20 keeps the feature export layer reproducible:

- feature CSV rows now include run/source/calibration metadata alongside the snapshot hash
- stable feature hashing and sorted export order make repeated CSV exports byte-equivalent for the same run

Phase 21 adds source-aware early-intent context:

- deshred tentative events now surface as a distinct early-intent source instead of being lumped into generic tentative flow
- source-specific latency and reconciliation metrics are exported for deshred-backed paper runs

Phase 22 adds source-specific operational measurements:

- deshred lead time versus Geyser/account-effect/rooted confirmation is recorded explicitly
- source-specific health counters separate deshred, raw-shred, mock, fixture, and replay tentative behavior
- mixed-source dedup evidence is exported with primary-source and duplicate-source labels for offline analysis

Phase 23 makes those measurements operationally consistent:

- `inspect-run`, `runtime_health.md`, `shred_exit_defense.md`, `deshred_status.md`, and `run_summary.md` now read from the same source-specific early-intent summary builder
- provider smoke, provider dry-run, and collection wrappers expose deshred provider status and early-intent totals without inventing canonical execution facts
