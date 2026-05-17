# Autopilot

`run-autopilot` is the hands-off stream-only paper/data-collection daemon for this workspace.

It is intentionally conservative:

- stream-only is required by default
- live trading remains disabled
- signer material is never required
- provider readiness is checked before collection
- missing endpoint, auth rejection, unsupported deshred, data gaps, queue pressure, nonzero RPC usage, and export failures stay visible through persisted state and alerts

## State machine

Persisted state lives under `data/autopilot/` (or the config-specific equivalent):

- `autopilot_state.json`
- `status.json`
- `alerts.jsonl`
- `autopilot.lock`

Phases:

- `Idle`
- `Starting`
- `ValidatingConfig`
- `ValidatingStreamOnly`
- `InspectingProvider`
- `SmokingGeyser`
- `SmokingDeshred`
- `ChoosingCollectionMode`
- `Collecting`
- `AnalyzingCollection`
- `CheckingBacktestReadiness`
- `RunningResearchCycle`
- `Exporting`
- `RotatingStorage`
- `Sleeping`
- `Paused`
- `Stopping`
- `Stopped`
- `Failed`

The lock file prevents duplicate concurrent collectors. A stale lock can be reclaimed, but an active lock blocks a second daemon.

## Commands

Manual control:

- `run-autopilot --once`
- `run-autopilot --continuous`
- `autopilot-status`
- `autopilot-pause --reason manual`
- `autopilot-resume`
- `autopilot-stop`
- `list-alerts`
- `clear-alerts --dry-run`

Storage and deployment:

- `autopilot-retention-report`
- `autopilot-prune --dry-run`
- `disk-preflight`
- `prune-verified-local-runs --dry-run`
- `autopilot-recover-storage --dry-run`
- `print-systemd-unit`
- `install-systemd-example --output-dir deploy/systemd/generated`
- `inspect-r2`
- `upload-artifacts-r2 --latest-run --dry-run`
- `refresh-dataset-index`
- `upload-dataset-index-r2 --verify`
- `vps-storage-status`

## Cycle behavior

Each cycle performs:

1. config validation
2. stream-only validation
3. RPC budget inspection
4. provider environment precheck
5. optional Geyser smoke
6. optional deshred smoke
7. collection-mode choice
8. stream-only paper collection
9. collection analysis
10. backtest-readiness check
11. optional research cycle
12. exports and schema verification
13. artifact manifest build and optional R2 offload
14. dataset index refresh + optional dataset index upload
15. verified local prune / retention review
16. status + alert updates

Collection mode is chosen honestly:

- real Geyser + optional deshred when endpoint/auth/provider support exist
- Geyser-only when deshred is unsupported and fallback is allowed
- mock only when explicitly configured
- no collection when required sources are unavailable

## Stream-only proof

Every autopilot cycle summary and status report includes:

- `stream_only_enabled`
- `stream_only_passed`
- `rpc_network_calls_total`
- `rpc_credits_used_total`
- `rpc_denials_total`
- `rpc_ledger_path`
- allowed/forbidden RPC policy flags

This is the proof that the daemon stayed at zero market-data RPC spend.

## R2 offload

- Each collection/research cycle now writes `artifact_manifest.json` and `r2_upload_summary.{md,json}` in the run report directory.
- `r2_upload_audit.{md,json}` captures planned/uploaded/verified artifacts, remote manifest location, and prune eligibility for that cycle.
- When `r2.upload_enabled=true`, autopilot uploads after collection/exports according to the R2 config.

## Ultra-low-disk mode

When `storage.low_disk_mode.enabled=true`, autopilot treats the VPS as a temporary stream processor:

- closed segments are uploaded and verified quickly
- verified closed segments are deleted immediately unless retained by `keep_last_n_segments_local`
- large exports stay chunked/compressed and should not remain as giant local CSVs
- `local_footprint_budget.{md,json}` records the run-local byte budget and overage
- autopilot refuses the next cycle if verified cleanup cannot restore free space above the low-disk warning floor

The intended tiny-VPS posture is:

- `storage.low_disk_mode.enabled=true`
- `storage.segments.max_segment_size_mb=8`
- `storage.segments.max_segment_age_seconds=60`
- `storage.segments.upload.max_concurrent_segment_uploads=6`
- `storage.segments.keep_last_n_segments_local=1`
- `autopilot.disk.warning_free_mb` aligned with the low-disk warning floor
- When R2 is disabled or still in dry-run mode, autopilot still writes the manifest and a planned upload summary so the operator can review the offload plan before enabling credentials.
- `data/dataset_index.json` is the local remote-first dataset catalog; it can also be uploaded to `<managed_prefix>/manifests/dataset_index.json`.
- Local prune is skipped unless the upload is verified and the retention policy allows deletion.
- After a verified upload, autopilot can rebuild the dataset index, upload the refreshed index, and prune only verified local artifacts according to `autopilot.disk` and `r2.retention`.

## Alerts

Local alerts are append-only JSON lines in `alerts.jsonl`.

Current alert types include:

- `ProviderEndpointMissing`
- `ProviderAuthRejected`
- `ProviderStreamError`
- `DeshredUnsupported`
- `StreamOnlyViolation`
- `RpcNetworkCallDetected`
- `RpcBudgetNonzero`
- `GlobalDataGap`
- `QueueOverflow`
- `StorageLimitReached`
- `R2UploadFailed`
- `R2VerificationFailed`
- `R2PruneSkippedUnverified`
- `R2CredentialsMissing`
- `R2BucketMissing`
- `R2StorageResetRequested`
- `R2StorageResetCompleted`
- `BacktestReady`
- `ResearchCycleCompleted`
- `AutopilotPaused`
- `AutopilotFailed`
- `AutopilotRecovered`

Webhook forwarding is optional and disabled by default. Secret URLs must come from env only and are never written into the reports.

## Retention

Retention is meant to keep the VPS usable without deleting active work:

- active runs are never deleted
- latest source runs and latest backtests are preserved
- failed runs can be preserved
- provider smoke reports can be preserved
- storage and age limits are reported before deletion

Use `autopilot-prune --dry-run` first to inspect what would be pruned.

R2-aware hygiene adds two more rules:

- unverified uploaded runs are not eligible for local prune when `prune_local_only_after_verified_upload=true`
- a minimal local manifest trail is retained even when larger exports or reports are cleaned up
- `disk-preflight` reports root free space, `target` size, `/var/log` size, verified/unverified run counts, and prune candidate counts before the next cycle starts
- `prune-verified-local-runs` batches safe cleanup across all verified runs without touching active or unverified data

## No-space recovery

When autopilot stops because the VPS disk is full:

1. `disk-preflight --config config/default.toml --config-override config/local.toml`
2. `prune-verified-local-runs --config config/default.toml --config-override config/local.toml --dry-run --delete-exports --keep-manifest --keep-rpc-ledger --keep-calibration`
3. if the plan is safe, rerun with `--force-prune-verified`
4. `refresh-dataset-index --config config/default.toml --config-override config/local.toml`
5. `upload-dataset-index-r2 --config config/default.toml --config-override config/local.toml --verify`
6. `autopilot-recover-storage --config config/default.toml --config-override config/local.toml --dry-run`

Rules:

- active runs are never pruned
- unverified runs are never pruned
- RPC ledger and calibration stay local by default
- local prune is not a substitute for R2 verification

## Rolling disk control

Phase 34 adds small-VPS guardrails for long real collections:

- `finalize-run-from-local-artifacts` can recover a completed run that never reached `artifact_manifest.json`
- `list-run-segments`, `verify-run-segments`, and `restore-run-segments` expose the per-run segment ledger
- `segment_manifest.json` is written alongside the run reports and is updated after finalize/upload/recovery actions
- the live runtime now checkpoints storage health during collection and stops early with a recorded `storage_limit_reached=...` note instead of drifting into `os error 28`

The important safety rule stays the same:

- verified remote state may replace local segment files
- unverified local data must stay on disk
- active open segments must never be deleted

## Systemd

The generated R2-aware service should prefer a built binary on small VPS instances:

`/home/ubuntu/pump-launch-quant/target/release/cli run-autopilot --config /home/ubuntu/pump-launch-quant/config/default.toml --config-override /home/ubuntu/pump-launch-quant/config/local.toml --stream-only --continuous`

The generated env example keeps provider and R2 env vars in a separate env file rather than embedding them in the unit. Protect that env file with `chmod 600`.

Production posture for the tiny VPS is:

- GitHub Actions builds the Linux release binary.
- The VPS receives only `/home/ubuntu/pump-launch-quant/target/release/cli` plus checksum/build metadata.
- Timer-based edge collection remains the production autonomy path.
- Research worker, replay, backtests, and exports stay off-VPS.

Do not rely on `cargo build` on the VPS for normal releases. Use [docs/BUILD_AND_DEPLOY.md](/Users/keelandavey/Documents/Codex/2026-05-08-you-are-codex-acting-as-a/pump-launch-quant/docs/BUILD_AND_DEPLOY.md) for the supported build, deploy, and rollback flow.

## Limits

- autopilot is paper/data-collection only
- it does not enable live trading
- it does not send orders
- it does not make production raw shred decoding available
- it does not turn missing endpoints into success
