# Operations

## Current commands

- `validate-config`
- `inspect-rpc-budget`
- `validate-stream-only`
- `run-paper --from-store`
- `run-paper --live-data`
- `run-fixture`
- `run-fixture-suite`
- `run-shred-exit-fixture-suite`
- `run-live` (guarded dry-run/live-enabled gate only)
- `replay`
- `backtest`
- `label`
- `export-features`
- `export-decisions`
- `export-fills`
- `backtest-shred-exit`
- `inspect-shred-calibration`
- `export-shred-calibration`
- `list-artifacts`
- `verify-export-schema`
- `inspect-deshred-capability`
- `inspect-raw-shred-capability`
- `provider-env-precheck`
- `smoke-geyser-provider`
- `smoke-deshred-provider`
- `smoke-streams`
- `provider-dry-run`
- `inspect-provider-compatibility`
- `provider-compatibility-report`
- `export-provider-compatibility`
- `export-rpc-ledger`
- `run-multisource-early-intent-suite`
- `smoke-multisource-early-intent`
- `collect-live-paper`
- `collect-first-live-paper`
- `analyze-live-collection`
- `check-backtest-readiness`
- `run-research-cycle`
- `run-autopilot`
- `autopilot-status`
- `autopilot-pause`
- `autopilot-resume`
- `autopilot-stop`
- `list-alerts`
- `clear-alerts`
- `autopilot-retention-report`
- `autopilot-prune`
- `print-systemd-unit`
- `install-systemd-example`
- `inspect-r2`
- `env-bootstrap-check`
- `smoke-r2-upload`
- `upload-artifacts-r2`
- `upload-pending-r2`
- `verify-r2-upload`
- `prune-local-after-r2`
- `build-dataset-index`
- `upload-dataset-index-r2`
- `r2-create-managed-buckets`
- `r2-reset-managed-storage`
- `vps-storage-status`
- `export-r2-manifests`
- `list-runs`
- `inspect-run`
- `validate-replay-equivalence`
- `inspect-token`
- `inspect-wallet`
- `inspect-cluster`
- `inspect-factory`
- `benchmark-decoders`
- `generate-report`

## Recommended workflow

1. Validate config and IDL wiring locally.
2. Run the fixture suite with `--explain-failures` to populate deterministic runtime artifacts and verify scenario expectations.
3. Replay the latest stored run in paper mode with `--latest-run`.
4. Replay the latest shred-defense run with `--latest-shred-exit-run` when you want to inspect early exits, saved loss, and false positives directly.
5. Verify RPC budgets before any future execution mode.
6. Keep live mode disabled unless you have explicitly reviewed the guarded executor path and RPC budgets.

## Observability

Prometheus registry helpers and tracing initialization are available now. The runtime supervisor maintains health and safety snapshots, queue-depth tracking, audit events, and report generation for fixture/store/live-data paper flows.

HTTP endpoints:

- `GET /metrics`
- `GET /healthz`
- `GET /readyz`

Useful flags:

- `run-paper --from-store --latest-run`
- `run-paper --from-store --latest-shred-exit-run`
- `run-paper --from-store --run-id <id>`
- `run-paper --live-data --geyser-only --max-events 100 --dry-run --mock-live`
- `run-paper --live-data --mock-live --mock-early-intent --max-events 100 --dry-run`
- `run-paper --live-data --mock-live --mock-deshred --max-events 100 --dry-run`
- `run-paper --stream-only --live-data --mock-live --mock-deshred --max-events 100 --dry-run`
- `run-paper --live-data --with-deshred --duration-seconds 30 --dry-run`
- `collect-live-paper --stream-only --preset short --mock-live --mock-deshred --duration-seconds 10`
- `smoke-deshred-provider --duration-seconds 30 --dry-run`
- `smoke-deshred-provider --duration-seconds 30 --dry-run --allow-missing-endpoint-report`
- `smoke-deshred-provider --stream-only --duration-seconds 30 --dry-run --allow-missing-endpoint-report`
- `provider-env-precheck`
- `smoke-geyser-provider --stream-only --duration-seconds 30 --dry-run --allow-missing-endpoint-report`
- `smoke-streams --stream-only --duration-seconds 30 --dry-run --allow-missing-endpoint-report`
- `provider-dry-run --duration-seconds 30`
- `smoke-multisource-early-intent --with-mock-secondary --dry-run --max-updates 100`
- `collect-live-paper --preset short --mock-live --mock-deshred`
- `collect-first-live-paper --duration-seconds 30 --stream-only --dry-run --mock-live --mock-deshred`
- `analyze-live-collection --latest-live-paper-run`
- `check-backtest-readiness --latest-live-paper-run --require-zero-rpc --require-stream-only`
- `run-research-cycle --latest-live-paper-run`
- `run-autopilot --once --mock-live --mock-deshred --duration-seconds 10`

## Low-disk remote-first workflow

For tiny VPS deployments, prefer:

1. `disk-preflight`
2. `clean-build-artifacts --dry-run`
3. `clean-build-artifacts --force`
4. `prune-verified-local-runs --delete-exports --keep-manifest --keep-rpc-ledger --keep-calibration --force-prune-verified`
5. `refresh-dataset-index`
6. `upload-dataset-index-r2 --verify`

Run-local observability now includes:

- `segment_manifest.json`
- `segment_status.{md,json}`
- `disk_actions.{md,json}`
- `local_footprint_budget.{md,json}`

In low-disk mode, do not expect the VPS to retain full local exports or full local report sets after verified upload. Use the local lite summaries plus the verified remote keys in `artifact_manifest.json`, `export_chunks_manifest.json`, and `data/dataset_index.json`.

## Build and deploy on a tiny VPS

Do not run routine release builds on the VPS. The supported path is:

1. Build the Linux CLI in GitHub Actions with [build-linux-cli.yml](/Users/keelandavey/Documents/Codex/2026-05-08-you-are-codex-acting-as-a/pump-launch-quant/.github/workflows/build-linux-cli.yml).
2. Deploy only the prebuilt `cli` binary, checksum, and `build_info.json`.
3. Validate `validate-config`, `validate-stream-only`, `validate-edge-mode`, and `scan-secrets` on the VPS before restarting timer-based collection.
4. Roll back by restoring the previous `cli.prev.<timestamp>` binary if validation fails.

The local fallback is:

```bash
scripts/deploy_prebuilt_cli.sh \
  --binary /path/to/cli \
  --sha256 /path/to/cli.sha256 \
  --build-info /path/to/build_info.json \
  --app-dir /home/ubuntu/pump-launch-quant \
  --config /home/ubuntu/pump-launch-quant/config/default.toml \
  --config-override /home/ubuntu/pump-launch-quant/config/local.toml \
  --env-file /home/ubuntu/pump-launch-quant.env \
  --restart-timer
```

That script never invokes Cargo on the VPS. For the full runbook, including target detection and rollback, use [docs/BUILD_AND_DEPLOY.md](/Users/keelandavey/Documents/Codex/2026-05-08-you-are-codex-acting-as-a/pump-launch-quant/docs/BUILD_AND_DEPLOY.md).

## Additional VPS commands

- `run-autopilot --continuous --stream-only`
- `autopilot-status`
- `autopilot-pause --reason manual`
- `autopilot-resume`
- `autopilot-stop`
- `list-alerts`
- `autopilot-retention-report`
- `autopilot-prune --dry-run`
- `print-systemd-unit`
- `install-systemd-example --output-dir deploy/systemd/generated`
- `inspect-r2`
- `upload-artifacts-r2 --latest-run --dry-run --include-reports --include-exports --include-rpc-ledger`
- `upload-pending-r2 --dry-run`
- `verify-r2-upload --latest-run --dry-run`
- `prune-local-after-r2 --latest-run --dry-run`
- `r2-create-managed-buckets --dry-run`
- `r2-reset-managed-storage --dry-run`
- `vps-storage-status`
- `export-r2-manifests`
- `run-paper --live-data --geyser-only --max-events 10 --dry-run`
- `run-fixture-suite --explain-failures`
- `run-shred-exit-fixture-suite --explain-failures`
- `list-runs --kind shred_exit_fixture_suite`
- `list-runs --role source_run`
- `list-runs --role analysis_run`
- `inspect-run --latest-shred-exit-run`
- `inspect-run --latest-deshred-run`
- `inspect-run --latest-mock-deshred-run`
- `inspect-run --latest-source-run`
- `inspect-run --latest-mocked-early-intent-run`
- `backtest-shred-exit --latest-shred-exit-run`
- `inspect-shred-calibration`
- `export-shred-calibration --format csv`
- `list-artifacts --latest-shred-exit-run`
- `verify-export-schema`
- `export-rpc-ledger --latest-run`
- `export-features --format csv --latest-run`
- `export-decisions --latest-run`
- `export-fills --latest-run`
- `inspect-provider-compatibility`
- `inspect-provider-compatibility --json`
- `provider-compatibility-report`
- `export-provider-compatibility --format csv`
- `list-runs`
- `inspect-run --latest-run`
- `validate-replay-equivalence --latest-run`
- `validate-replay-equivalence --latest-run --explain`
- `generate-report --latest-run --shred-exit-defense`

Shutdown behavior:

- canonical and tentative queues are bounded
- canonical queue overflow creates a global data-gap event
- tentative overflow is dropped first
- runtime drains queues for the configured shutdown timeout before stopping

## Phase 15 runtime notes

- `run-paper --live-data --mock-live` is the supported no-network daemon test path.
- `run-paper --live-data` without `--mock-live` now goes through the real Geyser adapter and fails clearly if `GEYSER_ENDPOINT` is not configured.
- when endpoint/auth config is present, the runtime attempts a real Yellowstone subscription and feeds decoded canonical events into the same supervisor path used by replay and fixtures.
- `generate-report --run <run_id> --strategy-summary` renders strategy-level PnL, drag, and fill classification.
- `--latest-run` resolves from persisted wall-clock run metadata, not synthetic event timestamps.
- `--latest-shred-exit-run` resolves from completed runs that actually contain shred-exit defense events.
- every completed run auto-generates run, strategy, edge-calibration, rejection-reason, PnL attribution, top-token, data-gap, runtime-health, and online-data-collection reports.
- Phase 17 also auto-generates `shred_exit_defense.md` so early-intent warnings, emergency exits, reconciliation outcomes, saved loss, and false positives are visible even on no-live-send paper runs.
- Phase 18 adds persisted calibration inspection/export plus `backtest-shred-exit` sensitivity artifacts so too-late cases and absorption downgrades can be analyzed after the run completes.
- Phase 19 adds replay debug artifacts and calibration snapshot pinning. Equivalence replay uses the original run's snapshot hash/path, and `replay_equivalence_debug.md` shows the first divergence point if equivalence fails.
- Phase 20 adds explicit run-role filtering and artifact discovery. Reports and exports remain visible through `list-artifacts`, but `--latest-run` now skips report/export/analysis runs unless you explicitly request them.
- Phase 21 adds deshred capability inspection plus a provider-backed pre-execution adapter. Use `inspect-deshred-capability` before enabling `--with-deshred`, and treat `deshred_status.md` as the per-run truth for whether the source actually connected or the provider returned `unimplemented`.
- Phase 22 adds `smoke-deshred-provider`, the provider compatibility matrix, first-class deshred run selectors, and the multisource dedup suite. Treat provider smoke as an operator readiness check only; it does not enable live trading.
- Phase 23 adds `provider-dry-run`, strict missing-endpoint reporting, mixed-source smoke reporting, and checkpointed collection presets. Use them in this order when a real environment is available:
- Phase 24 adds strict stream-only enforcement. Use this order when you want zero-eRPC proof instead of general paper validation:
  1. `validate-stream-only`
  2. `inspect-rpc-budget`
  3. `run-paper --stream-only --live-data --mock-live --mock-deshred --max-events 100 --dry-run`
  4. `collect-live-paper --stream-only --preset short --mock-live --mock-deshred --duration-seconds 10`
  5. `smoke-deshred-provider --stream-only --allow-missing-endpoint-report`
  6. `export-rpc-ledger --latest-run`
- Phase 25 adds the real-endpoint first-collection runbook. Use this exact operator sequence in a real environment:
  1. `provider-env-precheck --config config/default.toml`
  2. `smoke-geyser-provider --config config/default.toml --duration-seconds 30 --stream-only --dry-run --strict-env`
  3. `smoke-deshred-provider --config config/default.toml --duration-seconds 30 --stream-only --dry-run --strict-env`
  4. `smoke-streams --config config/default.toml --duration-seconds 30 --stream-only --dry-run --with-deshred`
  5. `collect-first-live-paper --config config/default.toml --duration-seconds 300 --stream-only --dry-run --with-deshred`
  6. `analyze-live-collection --config config/default.toml --latest-live-paper-run`
  7. `check-backtest-readiness --config config/default.toml --latest-live-paper-run --require-zero-rpc --require-stream-only`
  8. `run-research-cycle --config config/default.toml --latest-live-paper-run`
- Phase 26 adds the hands-off daemon. Use this sequence when the goal is unattended paper collection plus automatic reporting:
  1. `provider-env-precheck --config config/default.toml`
  2. `run-autopilot --config config/default.toml --once --mock-live --mock-deshred --duration-seconds 10` for a no-network shakeout
  3. `autopilot-status --config config/default.toml`
  4. `list-alerts --config config/default.toml`
  5. `print-systemd-unit --config config/default.toml`
  6. `install-systemd-example --config config/default.toml --output-dir deploy/systemd/generated`
  7. Populate the generated env file with `GEYSER_ENDPOINT` and, if needed, `GEYSER_AUTH_TOKEN`
  8. `run-autopilot --config config/default.toml --continuous` once the environment is ready
- Phase 28 keeps the VPS clean with env-only R2 offload. Use this sequence:
  1. create an env file outside the repo and protect it with `chmod 600`
  2. create `config/local.toml` from `config/local.example.toml`
  3. `env-bootstrap-check --config config/default.toml --config-override config/local.toml --require-geyser --require-r2`
  4. `smoke-r2-upload --config config/default.toml --config-override config/local.toml --dry-run`
  5. `upload-artifacts-r2 --config config/default.toml --config-override config/local.toml --latest-run --dry-run --include-reports --include-exports --include-rpc-ledger`
  6. `verify-r2-upload --config config/default.toml --config-override config/local.toml --latest-run --dry-run`
  7. `build-dataset-index --config config/default.toml --config-override config/local.toml`
  8. `upload-dataset-index-r2 --config config/default.toml --config-override config/local.toml --dry-run`
  9. only after a real verified upload, consider `prune-local-after-r2 --config config/default.toml --config-override config/local.toml --run-id <id> --dry-run`
- Phase 31 hardens small VPS storage. Use this sequence when the collector stopped with `No space left on device` or before enabling long-running service mode:
  1. `disk-preflight --config config/default.toml --config-override config/local.toml`
  2. `prune-verified-local-runs --config config/default.toml --config-override config/local.toml --dry-run --delete-exports --keep-manifest --keep-rpc-ledger --keep-calibration`
  3. if the plan is safe, rerun with `--force-prune-verified`
  4. `refresh-dataset-index --config config/default.toml --config-override config/local.toml`
  5. `upload-dataset-index-r2 --config config/default.toml --config-override config/local.toml --verify`
  6. `build-release-service --config config/default.toml --config-override config/local.toml`
  7. `print-systemd-unit --config config/default.toml --config-override config/local.toml --with-r2 --release-binary`
  8. `autopilot-recover-storage --config config/default.toml --config-override config/local.toml --dry-run`
  9. restart the service only after free space is back above the configured disk threshold
- Phase 34 adds recovery/finalization for runs that finished collection but missed upload/final reports because disk pressure hit before post-cycle work:
  1. `disk-preflight --config config/default.toml --config-override config/local.toml`
  2. `prune-verified-local-runs --config config/default.toml --config-override config/local.toml --delete-exports --keep-manifest --keep-rpc-ledger --keep-calibration --force-prune-verified`
  3. `finalize-run-from-local-artifacts --config config/default.toml --config-override config/local.toml --run-id <id> --upload-r2 --verify-r2 --refresh-dataset-index --mark-partial-if-missing-analysis`
  4. `verify-r2-upload --config config/default.toml --config-override config/local.toml --run-id <id>`
  5. restart the service only after the run is either verified or honestly marked partial and the free-space threshold is healthy again
- The stream-only reports to inspect after a run are:
  - `stream_only_audit.md`
  - `rpc_denials.md`
  - `stream_source_health.md`
  - `rpc_ledger.json`
  - `rpc_ledger.csv`
- The new provider/collection reports to inspect are:
  - `provider_env_precheck.md`
  - `geyser_provider_smoke.md`
  - `deshred_provider_smoke.md`
  - `stream_smoke_summary.md`
  - `first_live_collection_summary.md`
  - `live_collection_quality.md`
  - `backtest_readiness.md`
  - `research_cycle_summary.md`
- `validate-stream-only` fails if holder/top-holder RPC, metadata fetch, confirmation fallback, reconciliation/backfill RPC, blockhash RPC, send RPC, or nonzero RPC budgets are enabled.
- `inspect-rpc-budget` should stay at zero daily/monthly usage in the default stream-only profile.
- `export-rpc-ledger` is the proof artifact for “network RPC calls allowed = 0” on a run; denials stay visible instead of being hidden.
- Missing `GEYSER_ENDPOINT` is acceptable for tests and dry-runs only when you explicitly request the not-run artifact path with `--allow-missing-endpoint-report`.
- `provider-env-precheck` does no network work; it only reports whether the current environment is ready for a real Geyser smoke, a deshred smoke, or neither.
- `smoke-geyser-provider` validates canonical Yellowstone/Geyser streaming independently of deshred. Use it first when the endpoint is new or when deshred support is unknown.
- `smoke-streams` is the combined go/no-go wrapper. If deshred is optional and unsupported, it can still produce a Geyser-only pass with an honest deshred warning.
- `collect-first-live-paper` is the safe first real collection profile. It is paper-only, signer-free, and writes checkpoint/final reports plus the RPC ledger and stream-only proof for the run.
- `analyze-live-collection` and `check-backtest-readiness` are the gates after collection. Do not treat a smoke-only or insufficient-data verdict as a strategy signal.
- `run-research-cycle` only runs replay/export/backtest work after readiness passes. Otherwise it writes a blocker report explaining what more collection is needed.
- `run-autopilot` wraps those commands into a persisted state machine. It writes `data/autopilot/autopilot_state.json`, `data/autopilot/status.json`, `data/autopilot/alerts.jsonl`, cycle summaries under `data/reports/autopilot/`, and a lock file to prevent duplicate concurrent daemons.
- If `GEYSER_ENDPOINT` is absent, autopilot writes a `ProviderEndpointMissing` alert and either sleeps or falls back to explicit mock mode only when `--mock-live` or `allow_mock_when_endpoint_missing` is set.
- `autopilot-status` is the operator check for the current phase, cycle id, last provider status, last collection verdict, and zero-RPC proof from the most recent cycle.
- `autopilot-pause`, `autopilot-resume`, and `autopilot-stop` are graceful control surfaces. They update persisted state instead of requiring manual file edits or abrupt process termination.
- `autopilot-retention-report` and `autopilot-prune --dry-run` are the storage controls. They never delete the active run and report which runs or report trees would be pruned under the current retention policy.
- `artifact_manifest.json` and `r2_upload_summary.json` are the R2 offload controls. They show exactly which files were planned or uploaded, which bucket/key they map to, and whether verification passed.
- `verified_prune_report.{md,json}` and `disk_preflight.{md,json}` are the local safety controls. Use them before any emergency cleanup so you only delete verified artifacts.
- `r2-reset-managed-storage` and other destructive R2 commands are dry-run-first and require explicit confirmation plus `r2.delete_enabled=true`.
- `list-alerts` reads the local alert ledger. Webhook delivery is optional and disabled by default; local alerts remain the primary record of provider failures, stream-only violations, queue pressure, and backtest-ready milestones.
- Stream-only does not make live trading ready. It only proves that market data, tracking, paper, replay, reporting, and calibration can run without JSON-RPC/eRPC spend.
  1. `inspect-deshred-capability`
  2. `smoke-deshred-provider --strict-env`
  3. `provider-dry-run`
  4. `run-paper --live-data --with-deshred --duration-seconds 30 --dry-run`
- If `GEYSER_ENDPOINT` is absent, `smoke-deshred-provider --allow-missing-endpoint-report` and `provider-dry-run` still write honest not-run artifacts instead of claiming success.
- `collect-live-paper` presets stay paper-only. `short` is a 5-minute sanity collection, `medium` is a 30-minute observation window, and `long` is a 2-hour checkpointed run with stricter queue/memory reporting.
