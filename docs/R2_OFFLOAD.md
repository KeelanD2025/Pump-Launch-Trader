# R2 offload

## Purpose

Cloudflare R2 offload keeps the VPS focused on collection and replay work while preserving datasets, reports, exports, calibration artifacts, provider compatibility files, and RPC ledgers remotely.

## Architecture

1. A run completes locally.
2. The CLI/autopilot builds `artifact_manifest.json` in the run report directory.
3. Upload candidates are assigned to managed R2 prefixes by artifact type.
4. Uploads run in dry-run or real mode.
5. Optional verification checks remote size and stored checksum metadata.
6. `r2_upload_audit.{md,json}` records what was planned, uploaded, verified, and still prune-eligible.
7. Local prune is allowed only after verification when configured.

## Local override

- Keep committed configs dry-run/disabled for R2.
- Enable real upload in `config/local.toml` or via `--config-override config/local.toml`.
- `config/local.example.toml` shows the intended shape without embedding credentials.
- Never put secrets in `config/local.toml`; use env vars only.

## Managed prefixes

The default managed prefix is `pump-launch-quant`.

Default logical prefixes:

- `runs`
- `reports`
- `exports`
- `calibration`
- `provider_compatibility`
- `rpc_ledger`
- `manifests`

## Buckets

- datasets bucket: run datasets and CSV exports
- reports bucket: markdown/json reports, RPC ledgers, manifests
- calibration bucket: calibration snapshots and calibration exports
- provider compatibility bucket: provider capability history; falls back to the reports bucket if not configured

## Manifest format

Each manifest records:

- run identity and hashes
- stream-only proof
- RPC totals
- artifact list
- checksums
- remote keys
- compression flags
- upload summary
- prune summary

The manifest is local-first and can be uploaded to:

- `<managed_prefix>/manifests/<run_id>/artifact_manifest.json`

## Commands

- `env-bootstrap-check`
- `smoke-r2-upload`
- `inspect-r2`
- `upload-artifacts-r2`
- `upload-pending-r2`
- `verify-r2-upload`
- `list-r2-artifacts`
- `prune-local-after-r2`
- `prune-verified-local-runs`
- `build-dataset-index`
- `refresh-dataset-index`
- `upload-dataset-index-r2`
- `inspect-dataset-index`
- `r2-plan-managed-buckets`
- `r2-create-managed-buckets --dry-run`
- `r2-reset-managed-storage --dry-run`
- `vps-storage-status`
- `export-r2-manifests`

## Verification and prune behavior

- Dry-run upload creates the manifest and summary without touching the network.
- Real upload requires env credentials and bucket configuration.
- `smoke-r2-upload --dry-run` is the first no-network credential/layout check.
- Verification checks remote size and checksum metadata where available.
- Local prune refuses unverified manifests unless `--force-prune-unverified` is explicitly passed.
- Minimal local manifest state is preserved so the VPS still has a durable audit trail after cleanup.
- `prune-local-after-r2` writes `local_prune_audit.{md,json}` so cleanup is reviewable before or after deletion.
- `prune-verified-local-runs` batches that same safety model across all verified runs and writes `reports/autopilot/verified_prune_report.{md,json}`.
- `data/dataset_index.json` and `<managed_prefix>/manifests/dataset_index.json` provide a remote-first catalog of uploaded and pending runs.
- Autopilot refreshes the dataset index after verified upload, prune, and explicit recovery actions so the local and remote catalogs stay in sync.
- `segment_manifest.json` records closed run segments, their checksums, remote keys, verification state, and whether the local segment file was pruned after verification.
- `finalize-run-from-local-artifacts` is the recovery path when a completed run has reports/local data but no `artifact_manifest.json` yet.

## Remote-first low-disk behavior

In `storage.low_disk_mode`:

- closed segments are uploaded immediately
- verified closed segments are deleted immediately unless retained by policy
- `export_chunks_manifest.json` tracks chunked exports, remote keys, verification state, and local-pruned state
- large local exports should move to `chunked_remote_verified` rather than staying as monolithic CSVs
- large full reports may be reduced to local lite stubs after verified upload, with the remote key preserved in the manifest

This means a healthy low-disk run may have:

- verified remote segments with `pruned_local=true`
- verified remote export chunks with small or zero local chunk bytes remaining
- local summaries and manifests only

That is expected and should not be treated as data loss when the manifest proves remote verification.

## Destructive safety

Bucket-empty, object-delete, and bucket-delete actions are guarded by:

- `r2.delete_enabled=true`
- dry-run-first workflow
- managed-prefix checks
- explicit destructive CLI confirmation
- the extra reset phrase `--i-understand-this-deletes-cloudflare-r2-data`

No destructive command should operate outside the configured managed buckets/prefix without a deliberate code/config change.

## Troubleshooting

- If a secret was previously pasted into chat, rotate it before production use and keep only env var names in local files or service units.
- If `inspect-r2` shows missing bucket or key envs, stay in dry-run until the env file is fixed.
- If upload verification fails, do not prune local data.
- If autopilot records `R2UploadFailed`, inspect the local manifest and `r2_upload_summary.json` before retrying.
- If the VPS fills up, use `disk-preflight`, `prune-verified-local-runs --dry-run`, and `vps-storage-status` before any manual deletion.
