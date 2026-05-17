# Architecture

## System shape

The workspace is split into low-coupling crates so the hot path can stay small while research, export, and simulation modules evolve independently.

### Event path

1. `ingest-shred` accepts raw shreds from a local proxy and emits tentative observations through a bounded queue.
2. `ingest-geyser` accepts canonical Yellowstone/Geyser stream updates and normalizes them.
3. `runtime` owns the supervisor lifecycle and wires bounded canonical/tentative queues into the online loop.
4. `event-bus` fans bounded messages into downstream consumers.
5. `decoder` converts raw transaction/account payloads into Pump-aware semantic events using `idl`.
6. `state`, `features`, `risk`, and `decision` consume those events in the same order for fixture, store-replay, and paper flows.
7. `executor` handles deterministic paper trading and guarded live submission paths.
8. `storage` and `sim` persist replay-safe data, export features, and model fees/latency/impact.

### Supervisor model

- `Supervisor` owns runtime config resolution, IDL hashing, event queues, state, feature, risk, decision, paper execution, persistence, and health/safety snapshots.
- Tentative shred events flow through the same state/feature/risk path but are prevented from escalating into paper/live entries without later canonical confirmation.
- Phase 17 adds an explicit tentative sell defense lane: tentative sell detection, malicious-sell warnings, exit-arming, paper emergency exits, and later reconciliation against canonical Geyser outcomes.
- Canonical queue overflow is treated as a data gap and blocks trading; tentative overflow drops tentative events first and records audit events.
- Runtime audit events include startup/shutdown, ingest lifecycle, queue overflow, data-gap state, decisions, fills, and RPC-budget denials.
- Live-data paper uses the same supervisor and queue model as fixture/store replay. In this build, mocked live adapters are wired end-to-end and the real Geyser adapter now constructs a real Yellowstone subscribe stream, emits source-scoped data gaps on disconnect, and recovers on resume.

### Source adapters

- `FixtureSource`: deterministic canonical + shred fixture batches.
- `StoreReplaySource`: persisted normalized events grouped by run/scenario and replayed in deterministic order.
- Replay prefers persisted `runtime_sequence_number` ordering so tentative and canonical queues merge in the same order they were seen online; older records still fall back to deterministic timestamp/slot/event-id ordering.
- `MockGeyserLiveSource`: async live-style canonical stream used by tests and `--mock-live`.
- `MockShredLiveSource`: async live-style tentative stream used by tests and `--mock-live`.
- `MockDeshredLiveSource`: async pre-execution tentative stream used by `--mock-deshred`.
- `GeyserLiveSource`: real Yellowstone/tonic adapter that validates auth metadata, sends subscribe requests, streams slot/account/transaction updates, and normalizes them into the runtime event model.
- `DeshredLiveSource`: real Yellowstone `subscribe_deshred` adapter that streams tentative pre-execution transactions, normalizes them through the same IDL path, and leaves reconciliation to the canonical Geyser/account-effect lane.
- `TentativeSellManager`: consumes tentative sells from fixture/replay/raw-shred early-intent sources, emits warning/armed/triggered events, and resolves them through signature, account-effect, mismatch, not-seen, or reorg outcomes.
- `smoke_deshred_provider`: an operator-only smoke harness that exercises capability detection, optional provider connection, and report/compatibility recording without enabling live orders or hot-path RPC.
- `provider_dry_run`: a higher-level wrapper that sequences config/budget/capability checks, smoke execution, and an optional short paper run when deshred actually looks usable.
- `smoke_multisource_early_intent`: a mixed-source smoke path that can combine primary deshred intent with a secondary tentative source for dedup validation without requiring guarded live mode.
- Phase 18 adds persisted shred-exit calibration and run-kind-aware replay selection, so shred-defense runs can be reloaded with prior false-positive/saved-loss history instead of being treated as isolated fixture proof.
- Phase 19 adds calibration snapshots at run start. Equivalence replay uses the original snapshot hash/path recorded in run metadata instead of the current calibration file, while a future what-if replay can intentionally opt into newer calibration state.
- Phase 20 adds explicit `run_kind` and `run_role` metadata plus artifact discovery. Source runs, derived runs, analysis runs, report runs, and export runs are tracked separately so report/export activity stays discoverable without polluting default latest-run selection.
- Phase 23 adds a canonical source-specific summary builder that feeds `inspect-run`, markdown/JSON reports, and sidecar CSV summaries so deshred/raw/mock/replay totals do not drift between operator views.
- Phase 24 adds an explicit stream-only data plane. `LoadedConfig::validate_stream_only()` enforces the contract at config/runtime startup, while `RpcBudgetManager` remains the single gate for every non-stream JSON-RPC/eRPC/HTTP metadata attempt.

### Data-gap scoping

- `Token`: blocks only the affected mint.
- `Scenario`: cleared at scenario boundaries inside the fixture suite.
- `Source`: blocks while a required source remains unhealthy.
- `Global`: used for canonical queue overflow or explicit global loss of truth.

Supervisor safety checks use token-specific gating before forcing `StopTracking` or `EmergencyExit`, so one gap fixture does not poison unrelated tokens or future replay runs.

### Observability and replay validation

- Runtime health snapshots are exposed through `/healthz` and `/readyz`, with Prometheus text on `/metrics`.
- Every run writes auto-generated markdown artifacts under `reports/<run_id>/`.
- `validate-replay-equivalence` replays a completed run's persisted source events and compares decision/fill output for drift.
- `validate-replay-equivalence --explain` also writes `replay_equivalence_debug.md`, including the first divergence point, pending tentative sell state, and the calibration snapshot used for replay.
- `shred_exit_defense.md` captures tentative sell totals, emergency exits, reconciliation outcomes, saved loss, false positives, and opportunity cost for every run.
- `deshred_status.md`, `runtime_health.md`, and `run_summary.md` now share the same source-specific early-intent summary data, including provider status, per-source counters, dedup totals, and primary/duplicate source breakdowns.
- `stream_only_audit.md`, `rpc_denials.md`, `stream_source_health.md`, `rpc_ledger.json`, and `rpc_ledger.csv` provide the zero-eRPC proof layer for a run, including denied attempts and whether the network was ever touched.
- `shred_exit_calibration.md` and `backtest-shred-exit` quantify whether early exits beat Geyser/account-effect/confirmed/no-exit baselines under deterministic latency and threshold sweeps.
- `list-artifacts` enumerates report/export/backtest/calibration artifacts for a run, with deterministic checksums and row counts for CSV outputs.
- CSV exports are intentionally deterministic: stable header order, row order keyed by run/scenario/runtime-sequence/id, normalized decimal formatting, and persisted runtime sequence numbers for replay-sensitive datasets.
- Early-intent source precedence is explicit: deshred outranks raw-shred, and fixture/mock/replay evidence is retained for auditability without double-triggering exits.
- Mixed-source mock runs now use a single deterministic inline tentative timeline so dedup behavior reflects source precedence rather than Tokio task scheduling.
- `collect_live_paper` presets layer on top of the same supervisor path and write checkpoint summaries; they remain paper-only data collection wrappers and never bypass live safety gates.

### Implemented boundaries

- Shared types and config live in `common`.
- IDL/version truth lives in `idl`.
- Stream-source truth and health tracking lives in `ingest-geyser`, while tentative-first visibility and reconciliation live in `ingest-shred`.
- Local launch/holder/dev/cost-basis truth lives in `state`.
- Derived streaming analytics live in `features` and `risk`.
- Replay, labeling, and fee-aware execution realism live in `storage` and `sim`.
- Trade gating and execution live in `decision` and `executor`.
- RPC policy truth lives in `rpc-budget`.
- Stream-only contract truth lives in `common` config validation plus `docs/STREAM_ONLY_CONTRACT.md`.

### Core invariants

- No unbounded queues.
- No hidden RPC polling.
- No JSON-RPC/eRPC market-data, holder, metadata, reconciliation, confirmation, or blockhash fallback in stream-only mode.
- Every event carries source and canonicality context.
- Every future execution path must be auditable and replayable.
- Safety filters must be able to fail closed on stale data or unknown budget state.
