# R2 Streaming Artifact Sink Report

- generated_at_utc: `2026-06-23T17:55:00Z`
- status: `SOURCE_VALIDATION_READY`
- live_collection_started: `false`
- replay/backtesting/tuning/paper/live/wallet: `blocked`
- launch_caps: `blocked`

## Artifact Behavior

The local relay collector writes raw relay frames as bounded shards. In `r2-streaming` mode, a shard is uploaded to R2 when it reaches either the row rotation threshold or the configured streaming chunk byte threshold.

Each shard upload records:

- `part_index`
- `local_path`
- `object_key`
- `sequence_start`
- `sequence_end`
- `frame_count`
- `byte_length`
- `sha256`
- `uploaded`
- `verified`
- `local_deleted`
- `error`

The local chunk is deleted only after R2 upload and verification succeed. If verification fails, the collector blocks with `R2_STREAMING_BLOCK_UNVERIFIED_RELAY_SHARD` and does not delete the local shard.

## Required Manifests

The R2-streaming finalization path writes:

- `artifact_stream_manifest.json`
- `relay_frame_manifest.json`
- `material_artifact_manifest.json`
- `r2_streaming_upload_manifest.json`
- `local_spool_manifest.json`

Compact local manifests, countability decisions, R2 verification summaries, retention summaries, and service exit status remain local. Raw relay shards are not retained locally after verified streaming deletion.

## Telemetry Fields

The collector/proof path now carries:

- `storage_mode`
- `local_collector_usage_mb`
- `max_local_collector_usage_mb`
- `local_spool_bytes_current`
- `local_spool_bytes_peak`
- `local_spool_bytes_limit`
- `local_disk_free_mb`
- `r2_streaming_uploaded_chunks`
- `r2_streaming_verified_chunks`
- `r2_streaming_deleted_local_chunks`
- `r2_streaming_unverified_chunks`
- `r2_streaming_retry_count`
- `r2_streaming_upload_timeout_count`
- `r2_streaming_backpressure_detected`

These fields are now surfaced through local collector summaries, local proof summaries, relay supervisor slice summaries, the background collector live summary, and the final background collector report.

## Remaining Live Proof Gate

This report confirms source behavior. `R2_FIRST_STREAMING_STORAGE_PASS` still requires exactly one 900s live `r2-streaming` proof after validation and deployment, followed by a 10-slice batch only if that proof passes.

