# pump-launch-quant

`pump-launch-quant` is a Rust workspace for adaptive quantitative research, event-driven simulation, paper trading, and heavily guarded optional live trading for Pump.fun launch-phase tokens on Solana.

The design assumption is simple:

1. Shreds provide the earliest tentative visibility.
2. Yellowstone/Geyser provides the canonical low-latency truth stream.
3. In stream-only mode, RPC/eRPC is forbidden in the hot path and denied before network touch.
4. Every decision must be replayable, attributable, and safety-gated.

This implementation now delivers a working vertical slice through Phase 22, with a real runtime supervisor, scoped data-gap handling, fixture/store paper loops, mocked live-data paper wiring, a real Yellowstone/Geyser streaming adapter, HTTP health/metrics endpoints, replay-equivalence validation, executable-edge calibration for paper entries, and shred-first early sell defense for paper exits with persisted calibration and replay/backtest tooling. Live execution remains guarded and disabled by default:

- Phase 1: shared types, config, schema/versioning, bounded event bus, metrics helpers, and RPC budgeting scaffolding.
- Phase 2: configurable Pump IDL loading, discriminator mapping, generic instruction/account decoding, and decode fixtures/tests.
- Phase 3: Yellowstone/Geyser ingestion scaffolding with subscription planning, reconnect-aware health tracking, deduplication, slot-gap detection, and normalized raw stream output.
- Phase 4: real shred UDP intake, bounded queues, fixture-backed shred decoding, tentative event generation, and shred/Geyser reconciliation metrics.
- Phase 5: local token, holder, dev, wallet, funding, cluster, and lifecycle state from streamed events only.
- Phase 6: a real feature registry plus core launch, flow, holder, concentration, cohort, cost-basis, survival, and data-quality feature snapshots; deeper families are registered as unavailable instead of fabricated.
- Phase 7: evidence-based rug, bundle, dev, top-holder, fake-momentum, and data-quality risk scoring with discard recommendations.
- Phase 8: append-only JSONL storage, token summary persistence, deterministic replay ordering, and CSV feature export.
- Phase 9: fee-aware simulation, no-lookahead validation, round-trip PnL modeling, and token label generation.
- Phase 10: transparent decision matrix for watch, enter-paper, hold, exit, emergency-exit, and stop-tracking outcomes.
- Phase 11: deterministic paper executor with fills, fee drag, slippage drag, and PnL tracking.
- Phase 12 foundations: guarded live executor interfaces, RPC-budget gating, dry-run behavior, and fail-closed enablement checks.
- Phase 13: runtime supervisor wiring across ingest, storage, state, features, risk, decision, paper execution, audit logs, and report generation; fixture-suite and paper-from-store flows now use the same pipeline.
- Phase 14: mocked live-data paper loop, scoped token/global/scenario data-gap behavior, fixture expectation calibration, run/scenario storage isolation, deterministic scenario-end liquidation, and research-ready CSV export metadata.
- Phase 15: real Geyser source adapter integration and fail-closed startup checks, paper-daemon controls, expected executable edge modeling, PnL attribution, strategy summary reporting, and wall-clock run metadata for latest-run selection.
- Phase 16: Yellowstone proto-backed live stream normalization, real Geyser subscribe flow behind the runtime source abstraction, `/metrics` + `/healthz` + `/readyz`, auto-generated run reports, `inspect-run`, and replay/live equivalence validation.
- Phase 17: tentative sell-intent events, malicious-sell classification, confirmation/reconciliation state machine, precomputed exit-threat indices, paper emergency exits on high-confidence early sell warnings, false-positive/opportunity-cost tracking, and a dedicated shred-exit fixture suite.
- Phase 18: `latest-shred-exit-run` selection, persisted shred-exit calibration, mock early-intent live-data paper, shred-exit backtesting/CSV exports, and more selective latency/absorption-aware emergency-exit gating.
- Phase 19: deterministic early-intent replay equivalence, runtime-sequence-based replay ordering, calibration snapshots for equivalence replay, replay debug reports, and replayed shred-exit saved-loss/opportunity-cost summaries.
- Phase 20: explicit run kinds/roles, artifact discovery, non-polluting report/export runs, metadata-complete CSV research exports, and export schema verification.
- Phase 21: provider-backed deshred capability inspection, a real `subscribe_deshred` adapter when the Yellowstone build and endpoint support it, mock deshred live-data paper, deshred/Geyser reconciliation, and deshred-aware research exports.
- Phase 22: provider deshred smoke harnesses, mixed-source dedup integration coverage, provider-compatibility recording, source-specific early-intent health counters, deshred latency/edge reporting, and first-class deshred run selectors.
- Phase 23: strict provider smoke env validation, `provider-dry-run`, mixed-source smoke reports, short/medium/long live-data paper collection presets, source-specific report-consistency builders, and provider-compatibility history tracking.
- Phase 24: strict stream-only validation, zero-budget RPC policy, persisted RPC denial ledgers, stream-only proof exports, and explicit stream-only data-contract / RPC-audit docs.
- Phase 25: provider environment prechecks, Geyser-only and combined stream smoke wrappers, first live-paper collection helpers, collection-quality analysis, backtest-readiness gating, and a research-cycle wrapper for the first real stream-only datasets.
- Phase 26: a hands-off autopilot daemon for stream-only paper collection, alerts, retention, systemd deployment artifacts, and automatic readiness/research/export cycles with duplicate-run prevention.
- Phase 27: Cloudflare R2-ready artifact manifests, dry-run/verified upload commands, VPS storage-status reporting, guarded local prune-after-upload flows, and autopilot-integrated offload summaries.

Key crate status:

- `common`: shared config, event schema, math, errors, reason codes.
- `idl`: Pump/PumpSwap IDL loading and layout decoding.
- `ingest-geyser`: Yellowstone/Geyser client abstractions and stream processing.
- `ingest-shred`: implemented for UDP intake, fixture-backed decoding, tentative event extraction, and reconciliation. Production decoding remains fail-safe unless a real Solana shred decoder is plugged in.
- `event-bus`: bounded priority fan-out.
- `decoder`: higher-level transaction and account decoding orchestration.
- `state`, `features`, `risk`, `decision`, `sim`, `storage`, `executor`: working end-to-end research and paper-trading components.
- `rpc-budget`, `metrics`, `cli`, `tests`: shared operations, telemetry, command surface, and integration coverage.

## Quick start

```bash
cargo test
cargo run -p cli -- validate-config --config config/default.toml
cargo run -p cli -- validate-stream-only --config config/default.toml
cargo run -p cli -- inspect-rpc-budget --config config/default.toml
cargo run -p cli -- run-fixture-suite --config config/default.toml --explain-failures
cargo run -p cli -- run-paper --config config/default.toml --from-store --latest-run
cargo run -p cli -- export-features --config config/default.toml --format csv --latest-run
```

## Build and deploy

Production VPS instances should not compile normal releases. The expected operating model is:

1. GitHub Actions builds `target/release/cli` on `ubuntu-22.04`.
2. The workflow publishes `dist/cli`, `dist/cli.sha256`, and `dist/build_info.json`.
3. A deploy step or operator copies only the verified binary to `/home/ubuntu/pump-launch-quant/target/release/cli`.
4. Systemd continues to run the prebuilt release binary for timer-based edge collection.

Use [.github/workflows/build-linux-cli.yml](/Users/keelandavey/Documents/Codex/2026-05-08-you-are-codex-acting-as-a/pump-launch-quant/.github/workflows/build-linux-cli.yml) for the Linux build artifact and [docs/BUILD_AND_DEPLOY.md](/Users/keelandavey/Documents/Codex/2026-05-08-you-are-codex-acting-as-a/pump-launch-quant/docs/BUILD_AND_DEPLOY.md) for target detection, deploy, and rollback details.

## Runtime modes

- `fixture`: deterministic shred + Geyser fixture pipeline through the full supervisor.
- `paper_from_store`: replay a selected stored run or scenario through the same supervisor path.
- `live_data_paper`: a real bounded async supervisor loop. It is fully usable with mocked live adapters and Geyser-only fallback. Real Geyser mode now constructs a Yellowstone client, sends a real subscribe request, and normalizes slot/transaction/account updates when endpoint/auth config is present. The path is implemented, but it is still paper-only and not a claim of production trading readiness.
- `guarded_live_dry_run` / `guarded_live_enabled`: guarded live foundations only; still not production-ready.

## Stream-only mode

- `stream_only.enabled = true` is now the default in the shipped configs.
- Market-data RPC, holder RPC, top-holder RPC, metadata fetch, reconciliation RPC, confirmation RPC fallback, blockhash RPC, and send RPC are all disabled under the default profile.
- `validate-stream-only` is the explicit contract check before live-data paper collection.
- `export-rpc-ledger --latest-run` is the proof artifact for denied or absent RPC usage on a run.
- `docs/STREAM_ONLY_CONTRACT.md` and `docs/RPC_AUDIT.md` are the operator references for what is and is not allowed.
- `docs/SECRETS_AND_ENV.md` and `docs/R2_OFFLOAD.md` are the operator references for env-only secret handling and verified remote artifact offload.
- The real endpoint runbook is now: `provider-env-precheck`, `smoke-geyser-provider`, `smoke-deshred-provider`, `smoke-streams`, `collect-first-live-paper`, `analyze-live-collection`, `check-backtest-readiness`, then `run-research-cycle`.

## Cloudflare R2 offload

- R2 offload is env-driven only. Bucket names, access keys, and account identifiers come from env vars, never from committed config values.
- Use `config/local.toml` or `--config-override config/local.toml` for local-only enablement. Keep `config/local.toml` out of git and keep secrets in env files only.
- Start with `env-bootstrap-check --config config/default.toml --require-geyser --require-r2` to confirm the VPS env is ready without printing secrets.
- `smoke-r2-upload --dry-run` is the safe credential/bootstrap check before any real artifact upload.
- `inspect-r2` shows whether the current env is ready for real upload without echoing secrets.
- `upload-artifacts-r2 --latest-run --dry-run` builds an `artifact_manifest.json` and `r2_upload_summary.{md,json}` before any real network touch.
- `build-dataset-index` writes `data/dataset_index.json`, and `upload-dataset-index-r2 --dry-run` plans the remote index update.
- `verify-r2-upload` checks remote size/checksum when real uploads are enabled and credentials are present.
- `prune-local-after-r2` refuses to remove local files until the manifest is verified unless you explicitly force an unverified prune.
- `local_prune_audit.{md,json}` explains exactly what the VPS cleanup step would remove or keep.
- Destructive bucket/prefix reset commands stay dry-run-first and require explicit confirmation plus `r2.delete_enabled=true`.

## Useful commands

```bash
cargo run -p cli -- run-fixture --config config/default.toml --fixture fixtures/scenarios/clean_organic_launch.json
cargo run -p cli -- run-fixture-suite --config config/default.toml --explain-failures
cargo run -p cli -- run-paper --config config/default.toml --from-store --latest-run
cargo run -p cli -- run-paper --config config/default.toml --live-data --geyser-only --max-events 100 --dry-run --mock-live
cargo run -p cli -- run-shred-exit-fixture-suite --config config/default.toml --explain-failures
cargo run -p cli -- list-runs --config config/default.toml --kind shred_exit_fixture_suite
cargo run -p cli -- list-runs --config config/default.toml --role source_run
cargo run -p cli -- run-paper --config config/default.toml --from-store --latest-shred-exit-run
cargo run -p cli -- run-paper --config config/default.toml --live-data --mock-live --mock-early-intent --max-events 100 --dry-run
cargo run -p cli -- run-paper --config config/default.toml --live-data --mock-live --mock-deshred --max-events 100 --dry-run
cargo run -p cli -- inspect-deshred-capability --config config/default.toml
cargo run -p cli -- inspect-raw-shred-capability --config config/default.toml
cargo run -p cli -- validate-stream-only --config config/default.toml
cargo run -p cli -- provider-env-precheck --config config/default.toml
cargo run -p cli -- smoke-geyser-provider --config config/default.toml --duration-seconds 30 --stream-only --dry-run --allow-missing-endpoint-report
cargo run -p cli -- smoke-deshred-provider --config config/default.toml --duration-seconds 30 --dry-run
cargo run -p cli -- smoke-streams --config config/default.toml --duration-seconds 30 --stream-only --dry-run --allow-missing-endpoint-report
cargo run -p cli -- provider-dry-run --config config/default.toml --duration-seconds 30
cargo run -p cli -- run-autopilot --config config/default.toml --once --mock-live --mock-deshred --duration-seconds 10
cargo run -p cli -- autopilot-status --config config/default.toml
cargo run -p cli -- autopilot-pause --config config/default.toml --reason "manual"
cargo run -p cli -- autopilot-resume --config config/default.toml
cargo run -p cli -- autopilot-stop --config config/default.toml
cargo run -p cli -- list-alerts --config config/default.toml
cargo run -p cli -- autopilot-retention-report --config config/default.toml
cargo run -p cli -- autopilot-prune --config config/default.toml --dry-run
cargo run -p cli -- print-systemd-unit --config config/default.toml
cargo run -p cli -- install-systemd-example --config config/default.toml --output-dir deploy/systemd/generated
cargo run -p cli -- inspect-r2 --config config/default.toml
cargo run -p cli -- env-bootstrap-check --config config/default.toml --require-geyser --require-r2
cargo run -p cli -- smoke-r2-upload --config config/default.toml --dry-run
cargo run -p cli -- upload-artifacts-r2 --config config/default.toml --latest-run --dry-run --include-reports --include-exports --include-rpc-ledger
cargo run -p cli -- upload-pending-r2 --config config/default.toml --dry-run
cargo run -p cli -- verify-r2-upload --config config/default.toml --latest-run --dry-run
cargo run -p cli -- prune-local-after-r2 --config config/default.toml --latest-run --dry-run
cargo run -p cli -- build-dataset-index --config config/default.toml
cargo run -p cli -- upload-dataset-index-r2 --config config/default.toml --dry-run
cargo run -p cli -- vps-storage-status --config config/default.toml
cargo run -p cli -- export-r2-manifests --config config/default.toml
cargo run -p cli -- smoke-multisource-early-intent --config config/default.toml --with-mock-secondary --dry-run --max-updates 100
cargo run -p cli -- collect-live-paper --config config/default.toml --preset short --mock-live --mock-deshred
cargo run -p cli -- collect-first-live-paper --config config/default.toml --duration-seconds 30 --stream-only --dry-run --mock-live --mock-deshred
cargo run -p cli -- analyze-live-collection --config config/default.toml --latest-live-paper-run
cargo run -p cli -- check-backtest-readiness --config config/default.toml --latest-live-paper-run --require-zero-rpc --require-stream-only
cargo run -p cli -- run-research-cycle --config config/default.toml --latest-live-paper-run
cargo run -p cli -- run-paper --config config/default.toml --stream-only --live-data --mock-live --mock-deshred --max-events 100 --dry-run
cargo run -p cli -- collect-live-paper --config config/default.toml --stream-only --preset short --mock-live --mock-deshred --duration-seconds 10
cargo run -p cli -- smoke-deshred-provider --config config/default.toml --stream-only --duration-seconds 30 --dry-run --allow-missing-endpoint-report
cargo run -p cli -- inspect-provider-compatibility --config config/default.toml
cargo run -p cli -- export-provider-compatibility --config config/default.toml --format csv
cargo run -p cli -- export-rpc-ledger --config config/default.toml --latest-run
cargo run -p cli -- run-multisource-early-intent-suite --config config/default.toml --explain-failures
cargo run -p cli -- inspect-run --config config/default.toml --latest-mock-deshred-run --include-shred-exit --include-artifacts
cargo run -p cli -- validate-replay-equivalence --config config/default.toml --latest-run --explain
cargo run -p cli -- backtest-shred-exit --config config/default.toml --latest-shred-exit-run
cargo run -p cli -- inspect-shred-calibration --config config/default.toml
cargo run -p cli -- export-shred-calibration --config config/default.toml --format csv
cargo run -p cli -- list-artifacts --config config/default.toml --latest-shred-exit-run
cargo run -p cli -- verify-export-schema --config config/default.toml
cargo run -p cli -- run-paper --config config/default.toml --live-data --geyser-only --max-events 10 --dry-run
cargo run -p cli -- list-runs --config config/default.toml
cargo run -p cli -- inspect-run --config config/default.toml --latest-run
cargo run -p cli -- validate-replay-equivalence --config config/default.toml --latest-run
cargo run -p cli -- export-decisions --config config/default.toml --latest-run
cargo run -p cli -- export-fills --config config/default.toml --latest-run
cargo run -p cli -- inspect-token --config config/default.toml --mint <mint> --run-id <run_id>
cargo run -p cli -- generate-report --config config/default.toml --run <run_id>
cargo run -p cli -- generate-report --config config/default.toml --run <run_id> --strategy-summary
cargo run -p cli -- generate-report --config config/default.toml --latest-run --shred-exit-defense
```

## Current status

- The runtime supervisor is real for fixture, stored-event, and mocked live-data paper loops.
- `run-fixture-suite` and `run-paper --from-store --latest-run` exercise the same end-to-end research path.
- `run-paper --live-data --mock-live` exercises the bounded async live-data paper loop without requiring external network dependencies.
- `run-paper --live-data` without `--mock-live` now goes through the real Geyser adapter path, requires endpoint/auth config when configured, and fails clearly when `GEYSER_ENDPOINT` is missing, suggesting `--mock-live` for tests.
- Tentative shred/deshred signals are treated as early intent only. They can arm or trigger paper emergency exits, but every tentative sell is later reconciled against canonical Geyser outcome and recorded as confirmed, failed, not-seen, mismatch, or reorged.
- This build now contains a provider-backed deshred adapter behind the runtime source abstraction. Whether it actually streams depends on both build support and provider endpoint support; `inspect-deshred-capability` and `deshred_status.md` are the honest source of truth for a given environment.
- `smoke-deshred-provider` is the safe operator check before trusting a real endpoint. It records whether the provider was missing, unimplemented, auth-rejected, connected-but-silent, or actively streaming, and it never enables live orders.
- `provider-env-precheck` is the zero-network readiness gate before any real stream attempt. It validates endpoint/auth presence, stream-only posture, zero RPC budgets, and whether the current environment is ready for Geyser smoke or deshred smoke.
- `smoke-geyser-provider` validates the canonical Yellowstone/Geyser stream independently of deshred and writes a zero-RPC smoke artifact even when the endpoint is missing.
- `smoke-streams` combines provider precheck, Geyser smoke, and optional deshred smoke into one go/no-go summary for a first real collection attempt.
- `provider-dry-run` is the one-command readiness wrapper. It runs config/budget/capability checks, executes the smoke harness, and only attempts a short paper collection if the provider actually looks stream-capable.
- `run-autopilot` is the hands-off stream-only orchestrator. In `--once` mode it validates config and stream-only posture, checks provider readiness, decides between real-stream and explicit mock collection, runs collection, analyzes quality, checks readiness, exports artifacts, rotates storage, and writes alerts/status reports without ever enabling live orders.
- `autopilot-status`, `autopilot-pause`, `autopilot-resume`, and `autopilot-stop` manage the persisted daemon state in `data/autopilot/` so a long-running collector can be inspected and controlled without editing files by hand.
- `autopilot-retention-report` and `autopilot-prune --dry-run` are the storage-management preview tools for the autopilot data paths; they keep active runs intact and report what would be pruned before any deletion is allowed.
- `print-systemd-unit` and `install-systemd-example` generate deployment-ready systemd artifacts for a hands-off `run-autopilot --continuous` service without embedding secrets in the unit file.
- `print-systemd-unit --with-r2` and `install-systemd-example --with-r2` target `config/local.toml` so the service can stay repo-clean while R2 upload is enabled locally.
- The generated env example now includes placeholder slots for Geyser/ShredStream/R2 env vars only. Secrets stay outside the repo and should be protected with `chmod 600`.
- R2 offload is available for manual runs and autopilot cycles. Every manifest/upload summary is local-first, dry-run-capable, and explicit about whether a remote upload was real, planned, verified, or skipped.
- `artifact_manifest.json` lives under each run report directory and records checksums, remote keys, stream-only proof, and upload/prune outcomes for that run.
- `r2_upload_summary.md` and `r2_upload_summary.json` sit next to the manifest and show the upload/verification/prune result without exposing secrets.
- `r2_upload_audit.md`, `local_prune_audit.md`, and `data/dataset_index.json` are the new operational artifacts for verified offload, safe cleanup, and remote-first dataset navigation.
- `export-r2-manifests` materializes a deterministic CSV of local manifest state across runs so VPS cleanup decisions can be audited.
- `collect-first-live-paper` is the first real collection profile: it stays paper-only, stores stream events/decisions/fills/features, writes checkpoint/final summaries, and includes stream-only proof plus the RPC ledger for the run.
- `analyze-live-collection` and `check-backtest-readiness` are the post-collection gates. They decide whether a dataset is smoke-only, usable for feature-distribution work, or ready for replay/backtest research without spending any RPC credits.
- `run-research-cycle` wraps that loop and only runs replay/export/backtest work when readiness passes; otherwise it writes a blocker report instead of implying that a short collection is enough.
- `docs/AUTOPILOT.md` is the operator guide for the hands-off daemon, including the state machine, alerts, retention behavior, and systemd installation flow.
- `stream_only` is now the default data-plane posture. Geyser, deshred, shred, replay, stored events, and offline imports are allowed; JSON-RPC/eRPC, metadata/web fetches, and confirmation/blockhash fallbacks are not.
- `validate-stream-only` fails if any forbidden RPC category is enabled in config, if RPC budgets are nonzero, or if the confirmation/blockhash path would need RPC.
- `rpc_ledger.json`, `rpc_ledger.csv`, `stream_only_audit.md`, `rpc_denials.md`, and `stream_source_health.md` are written as run artifacts so a mock or live-data paper run can prove it stayed at zero allowed network RPC calls.
- `smoke-multisource-early-intent` is the safe mixed-source dedup check. In mock mode it proves deshred-primary plus secondary-tentative dedup without needing a real endpoint; in real mode it writes a missing-endpoint or unsupported report instead of pretending the provider was exercised.
- `collect-live-paper` provides safe `short`, `medium`, and `long` paper-only collection presets with checkpoint summaries. Missing endpoints fail clearly unless you intentionally stay in mock mode.
- The same missing-endpoint rule now applies to `provider-env-precheck`, `smoke-geyser-provider`, `smoke-streams`, and `collect-first-live-paper`: report mode writes an honest not-run artifact, while strict mode fails clearly before any network touch.
- `data/provider_compatibility/deshred_provider_matrix.json` records provider capability observations using an endpoint host hash instead of the raw endpoint and never stores auth tokens.
- Missing `GEYSER_ENDPOINT` is now reported explicitly by the smoke and dry-run wrappers. If you pass the missing-endpoint report mode, they write artifacts that say the provider was not attempted; otherwise they fail clearly without a misleading success.
- `run-multisource-early-intent-suite` proves that duplicate deshred/mock tentative sources do not trigger duplicate emergency exits and that replay equivalence still holds for the mixed-source cases.
- Paper entries are now gated by an expected executable edge model instead of purely structural signals, and paper fills carry fee/slippage/impact/latency attribution.
- `run-shred-exit-fixture-suite` exercises dev/top-holder/bundle false-positive, account-effect-confirmed, mismatch, and reorg early-exit behavior with saved-loss and opportunity-cost reporting.
- `run-paper --from-store --latest-shred-exit-run` now replays the latest completed shred-defense run directly, preserving early exits and reconciliation artifacts instead of drifting back to the standard fixture suite.
- `run-paper --live-data --mock-live --mock-early-intent` exercises the tentative-intent pipeline online without requiring a real deshred/raw-shred provider.
- Shred-exit calibration now persists to `data/calibration/shred_exit_calibration.json`; `inspect-shred-calibration`, `export-shred-calibration`, and `backtest-shred-exit` expose the saved-loss/opportunity-cost buckets across runs.
- Replay equivalence for mock early-intent live-data paper now uses persisted `runtime_sequence_number` ordering plus the original run's calibration snapshot, so `validate-replay-equivalence --latest-run --explain` can reproduce tentative warnings, emergency exits, fills, saved loss, and false positives without wall-clock drift.
- `data/reports/<run_id>/replay_equivalence_debug.md` is written next to `replay_equivalence.md` and shows the first divergence point, online vs replay decision/fill tables, and the calibration snapshot used for equivalence replay.
- Run metadata now distinguishes `run_kind` and `run_role`, so `--latest-run` excludes report/export/analysis runs by default while `--latest-source-run`, `--latest-mocked-early-intent-run`, and `--latest-shred-exit-run` remain explicit.
- `list-artifacts` exposes reports, exports, backtest outputs, replay debug files, and calibration snapshots without making those artifacts the default latest run target.
- CSV exports are deterministic and metadata-rich: runtime sequence numbers, source/trigger ids, run/source linkage, and calibration snapshot hashes are now exported where relevant, and `verify-export-schema` checks the headers.
- Early-exit gating now considers latency advantage, net benefit, and absorption strength, so too-late and absorbed tentative sells can be downgraded instead of always forcing a panic exit.
- Geyser-only fallback is supported when shred is disabled or unavailable and config allows it.
- Data gaps are scoped. Token-scoped gaps block only the affected mint, while queue-overflow/global gaps block all trading.
- `--latest-run` now resolves from persisted run metadata wall-clock timestamps rather than synthetic event time.
- Every completed run auto-generates `run_summary.md`, `strategy_summary.md`, `pnl_attribution.md`, `edge_calibration.md`, `data_gaps.md`, `runtime_health.md`, `rejection_reasons.md`, `top_tokens.md`, and `online_data_collection.md`.
- `inspect-run` summarizes a stored run and `validate-replay-equivalence` checks replay determinism against persisted events.
- `/metrics`, `/healthz`, and `/readyz` are implemented for daemon observability and bind to localhost by default.
- Live trading is still disabled by default and not production-ready.
- The shred path is real for fixture-driven development and reconciliation metrics, but the production raw shred decoder intentionally fails closed until a Solana shred decoder backend is linked.
- Deshred/pre-execution intent is now runtime-wired when the Yellowstone build and provider endpoint support `SubscribeDeshred`. Unsupported or unimplemented providers still skip or fail clearly depending on config, and deshred remains tentative-only rather than canonical execution truth.
- Source-specific early-intent counters now separate deshred, raw-shred, mock, fixture, and replay tentative behavior in runtime health, metrics, and CSV/report artifacts.
- Deshred lead time versus Geyser/account-effect confirmation is measured and exported; the system treats a zero or negative lead time as an observation, not as proof of edge.
- Live shred-triggered exits are disabled by default. High-confidence paper exits are implemented; guarded live tentative exits still require stricter confirmation and remain off by default.
- Research export is still CSV-first. `features.csv`, `decisions.csv`, `fills.csv`, shred-exit CSVs, and run-summary/data-gap/rejection CSVs are deterministic and metadata-rich; Parquet is still not implemented.
- The RPC/export proof layer is also CSV-first: `export-rpc-ledger --latest-run` materializes every denied or allowed RPC attempt recorded for the run, including whether the network was touched.
- Paper replay, feature export, token inspection, audit reporting, and backtest-style labeling are available through the CLI with run/scenario isolation.

See the `docs/` directory for architecture, data-source policy, safety posture, and phase breakdown.
