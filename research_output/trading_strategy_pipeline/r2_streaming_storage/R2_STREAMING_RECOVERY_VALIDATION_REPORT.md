# R2 Streaming Recovery Validation Report

- generated_at_utc: `2026-06-23T17:55:00Z`
- status: `SOURCE_VALIDATION_READY`
- recovery_script: `scripts/recover_r2_streaming_run.py`
- replay/backtesting/tuning/paper/live/wallet: `blocked`
- launch_caps: `blocked`

## Recovery Capabilities

`scripts/recover_r2_streaming_run.py` reads compact local manifests and classifies interrupted streaming runs without touching secrets or deleting data.

It reads:

- `r2_streaming_upload_manifest.json`
- `artifact_stream_manifest.json`
- `relay_frame_manifest.json`
- `material_artifact_manifest.json`
- `local_spool_manifest.json`
- `r2_upload_result.json`
- `countability_decision.json`
- `run_countability_decision.json`

It detects:

- verified chunks deleted locally;
- verified chunks still present locally;
- unverified local chunks retained for recovery;
- incomplete or missing manifests;
- current R2 verification/failure status.

## Retry Safety

Recovery retry is explicit and fail-closed:

- `--retry-unverified` never deletes unverified chunks.
- retry is blocked if R2 health is not verified.
- retry is blocked if an approved uploader is not configured.
- no secrets, source files, `.git`, `.codex_runtime_env`, SSH keys, or env files are touched.

The current source adds structured retry-block classifications instead of a silent TODO:

- `R2_STREAMING_RECOVERY_RETRY_BLOCKED_R2_HEALTH`
- `R2_STREAMING_RECOVERY_RETRY_BLOCKED_NO_UPLOADER`

## Required Follow-Up

If an actual unverified streaming shard appears in a live run, recovery must preserve it and either:

- retry through an approved uploader after R2 health verification, or
- block for operator review.

It must never delete unverified chunks.

