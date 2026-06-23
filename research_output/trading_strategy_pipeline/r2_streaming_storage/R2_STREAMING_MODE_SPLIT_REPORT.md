# R2 Streaming Mode Split Report

- generated_at_utc: `2026-06-23T17:55:00Z`
- status: `SOURCE_VALIDATION_READY`
- live_collection_started: `false`
- replay/backtesting/tuning/paper/live/wallet: `blocked`
- launch_caps: `blocked`

## Mode Split

`local_mirror` remains the full local artifact mode. It keeps local artifacts as the primary store and retains the 50GB-style collection gate.

`r2_primary` remains the conservative R2-backed collection mode with a larger local staging gate. Its collection preflight defaults to `10000 MB`, so it does not get weakened by the streaming work.

`r2_streaming` is the new bounded-spool mode. It uses R2 as the durable artifact store and treats local raw relay shards as a streaming spool. Its collection preflight defaults to `4096 MB` with a default `2048 MB` spool cap and the wrapper-level `PUMP_MAX_LOCAL_COLLECTOR_USAGE_MB=5000` budget.

## Confirmations

- `r2-primary` collection gate remains `10000 MB`.
- `r2-streaming` collection floor is `4096 MB`.
- `r2-streaming` requires R2 upload and live R2 health for collection.
- `r2-streaming` blocks `keep-all` retention.
- `r2-streaming` reports `r2_streaming_primary=true` and `local_spool_only=true` in preflight.
- `r2-streaming` does not silently fall back to `r2-primary` or `local-mirror`.
- `scripts/run_relay_r2_primary_batch.py` defaults to `r2-streaming`; explicit `--storage-mode r2-primary` is still available for fallback/manual diagnostics.
- `scripts/run_background_24h_collector.py` defaults to `r2-streaming` and passes streaming spool/min/chunk flags to the relay supervisor.
- `scripts/local_stream_collector_preflight.sh` supports `--storage-mode r2-streaming --mode collection --verify-r2-health-live`.

## Operational Meaning

`r2-streaming` is now the intended default for future bounded collection. It should be used for all new relay/local collection unless the operator explicitly chooses `r2-primary` or `local-mirror` for a diagnostic fallback.

The storage enforcer is a guardrail, not the durable store. Durable artifacts still have to be uploaded and verified in R2 before local chunks can be deleted.

