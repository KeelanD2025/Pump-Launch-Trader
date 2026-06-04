# Material Hunter Pre-Run Readiness

This document defines the release gate before any longer material-candidate hunter collection.

## Allowed Next Action

- Service-owned, sequential, sliced material-candidate collection only.
- Each slice must use mandatory VPS housekeeping, startup sentinels, watchdog checks, bounded R2 checkpoints, `RuntimeMaxSec`, final artifact collection, and artifact consistency validation.
- Source release-gate readiness is necessary but not sufficient for live collection: provider acceptance must also pass after any provider-lagged or data-loss slice.

## Blocked Actions

- Foreground material hunter runs.
- Unsliced or full campaign collection.
- Any material-hunter collection while provider acceptance is failing.
- Off-VPS replay unless a normal counted slice produces `replay_eligible_candidate_count > 0` and `countability_decision.json` explicitly allows replay.
- Formal backtesting.
- Threshold tuning.
- Live trading.
- Holder RPC for holder count, top-holder, dev holdings, churn, stickiness, or paperhand metrics.
- Canonical RPC mint supply.
- Provider-confirmed bundle claims without explicit provider/API evidence.

## Failure Taxonomy

Provider and control-plane failures must be represented as structured non-countable blockers, including:

- `provider_lagged_data_loss`
- `provider_reconnect_exhausted`
- `provider_stream_closed_before_deadline`
- `provider_progress_stalled`
- `pump_progress_stalled`
- `auth_rejected`
- `permission_denied`
- `unsupported_provider`
- `provider_connect_failed`
- `provider_first_update_timeout`
- `provider_zero_updates_timeout`
- `provider_decode_error_limit_exceeded`
- `provider_malformed_update_limit_exceeded`
- `provider_slot_regression`
- `provider_duplicate_update_overflow`
- `provider_backpressure_detected`
- `client_backpressure_detected`
- `signal_interrupted`
- `systemd_timeout`
- `startup_sentinel_failed`
- `watchdog_failed`
- `r2_checkpoint_failed`
- `r2_final_upload_failed`
- `disk_space_low`
- `artifact_write_failed`
- `config_invalid`
- `env_missing`
- `unknown_structured_blocker`

Any such blocker must produce non-countable, audit-only artifacts with replay, formal backtesting, and threshold tuning disabled.

`provider_lagged_data_loss` during a collection slice is a safe structural outcome when final artifacts exist, countability is false, replay is false, and R2/artifact consistency pass. It is still an operational provider-quality blocker. Longer collection must not resume after repeated `provider_lagged_data_loss` until provider acceptance and exact material-hunter stream acceptance pass or the Geyser endpoint/configuration is remediated. Launch caps must not be raised while either acceptance gate is failing.

## Countability Rules

`countability_decision.json` is the strict source of truth. A slice is counted only when final artifacts exist, R2 final verification succeeds, the latest checkpoint is verified, hard invariants pass, no provider blocker is present, no provider data loss is seen, and row counts reconcile across the attempt ledger and summaries.

R2 verification cannot turn an interrupted, stalled, early-closed, or provider-blocked slice into a counted result.

## Replay Eligibility

Candidate checkpoints are audit checkpoints only. They are not replay-eligible.

`off_vps_candidate_replay_allowed` and `ready_for_off_vps_candidate_replay` must equal:

```text
counted_phase107b_result && replay_eligible_candidate_count > 0
```

A mint must never be replay-eligible while also appearing as `terminal_inconclusive` in the rejected summary or attempt ledger.

## R2 Behavior

Runtime checkpoints are bounded to health, progress, partial summaries, the attempt ledger, and manifest files. They must not recursively upload rich candidate/rejected token directories from the stream polling path. Final upload may upload the full slice artifact set, but final upload success does not override countability blockers.

## Workflow And Systemd Guardrails

- `latest_run_id` is written only after overlap checks, successful `systemd-run`, startup heartbeat, attempt ledger sentinel, watchdog fail-fast, and startup R2 checkpoint.
- Service starts clamp requested duration, attempts, and target candidates to slice policy defaults.
- `RuntimeMaxSec` is set to the effective duration plus a safety margin.
- Status/stop/collect must target the intended run id and must not silently inspect the wrong slice.
- A deployed binary commit mismatch blocks collection.

## Pre-Run Gate

Run:

```bash
./scripts/material_hunter_pre_run_gate.sh
```

This gate is offline/source-oriented by default. It can pass while provider acceptance blocks live collection.

## Provider Acceptance Gate

After a provider-lagged, data-loss, reconnect-exhausted, early-closed, or progress-stalled slice, run a provider-only gate before resuming material-hunter collection:

```bash
target/release/cli provider-health-probe \
  --config config/default.toml \
  --config-override config/local.example.toml \
  --duration-seconds 900 \
  --json
```

or:

```bash
./scripts/material_hunter_provider_acceptance_gate.sh
```

In the VPS/GitHub control plane, use the workflow input `run_provider_health_probe=true`. This mode deploys/uses the selected CLI binary, confirms the deployed commit, runs `provider-health-probe`, downloads probe artifacts, and emits a GitHub summary. It must not start or stop the material-hunter service and must not modify `phase107b_material_candidate_hunter/latest_run_id`.

The probe connects to the configured Geyser stream path but does not track candidates, write token artifacts, run replay, backtest, tune thresholds, or trade. It writes:

- `provider_health_probe_summary.json`
- `provider_health_probe_liveness.csv`
- `provider_health_probe_exit_status.json`

Provider acceptance is `PASS` only when provider updates arrive, the stream reaches the configured proof condition normally, no provider lag/data loss is seen, no reconnect exhaustion or early close occurs, progress does not stall, and all probe artifacts are written. Provider acceptance is `BLOCK` for `provider_lagged_data_loss`, unrecoverable data loss/corruption, reconnect exhaustion, early stream close, progress stalls, missing first update, zero provider updates, rejected credentials, unsupported endpoint, invalid/missing provider environment, or artifact-write failure.

A structured provider `BLOCK` in this probe is not a repo crash. It means material-hunter collection remains blocked until provider/config remediation is complete and provider acceptance later passes. Launch caps remain blocked while provider acceptance fails.

If provider acceptance blocks, do not run material-hunter collection. Recommended remediation is a higher-throughput or dedicated Geyser endpoint, an alternate provider, narrower subscription if it still preserves stream-authoritative holder metrics and fresh Pump.fun launch detection, or provider-side support investigation.

## Exact Stream Acceptance Gate

`provider-health-probe` is a base provider gate. If a real material-hunter slice later hits `provider_lagged_data_loss`, the base gate is not representative enough by itself. Collection remains blocked until the exact material-hunter stream acceptance gate passes.

Run:

```bash
target/release/cli material-hunter-stream-acceptance-probe \
  --config config/default.toml \
  --config-override config/local.example.toml \
  --duration-seconds 900 \
  --stage all \
  --json
```

In the VPS/GitHub control plane, use `run_material_hunter_stream_acceptance_probe=true`. This probe must not start or stop the material-hunter service and must not modify `phase107b_material_candidate_hunter/latest_run_id`.

The exact stream acceptance probe writes operational artifacts only:

- `stream_acceptance_probe_summary.json`
- `stream_acceptance_probe_liveness.csv`
- `stream_acceptance_probe_exit_status.json`
- `stream_acceptance_probe_stage_results.json`

It does not write candidate or rejected token research artifacts and it never enables replay, formal backtesting, threshold tuning, or trading.

The staged gate is:

- Stage A, `raw_drain`: exact material-hunter subscription with minimal event handling.
- Stage B, `decode`: exact material-hunter subscription with decode/update handling.
- Stage C, `hunter_dry_run`: exact material-hunter stream path with minimal in-memory active-mint handling, but no research token artifacts.

Acceptance is `PASS` only when all requested stages complete without provider lag/data loss, reconnect exhaustion, early close, provider progress stall, Pump progress stall, or artifact contradiction. A Stage A failure is classified as `PROVIDER_OR_SUBSCRIPTION_EXTERNAL_LAG_LIKELY`. A Stage B failure is classified as `CLIENT_DECODE_BACKPRESSURE_LIKELY`. A Stage C failure is classified as `CLIENT_HUNTER_WORKLOAD_BACKPRESSURE_LIKELY` or `R2_CHECKPOINT_INTERFERENCE_LIKELY` depending on the blocker.

Real collection remains blocked while exact stream acceptance fails. Launch caps must not be raised until exact-subscription acceptance passes.

## Same-Endpoint Backpressure Guardrails

The material hunter uses the same configured Geyser endpoint as the provider probes, but collection work is heavier than raw provider acceptance. A low-ping endpoint can still lag if the client receive loop, decoder, artifact writer, or checkpoint path falls behind the gRPC stream.

The hunter stream path must therefore be drain-first:

- one gRPC reader polls one stream and handles slot/liveness/counter-only updates on a cheap reader-side path;
- worker-relevant updates go through a bounded router queue into deterministic partition worker queues;
- partition workers decode/normalize concurrently while preserving arrival order for keys assigned to the same mint/account partition;
- the coordinator remains the only owner of attempt lifecycle, active mints, segment closure, countability, and replay eligibility;
- ledger writes, tombstones, candidate artifacts, heartbeat writes, and R2 checkpoint coordination must not run inline in the gRPC reader task;
- R2 checkpoints must remain bounded and must not recursively upload rich token artifact directories from the stream polling path;
- if the router queue, any partition queue, or the worker backlog exceeds its hard threshold, the current segment is classified as `client_backpressure_detected`, non-countable, audit-only, replay-disabled, backtesting-disabled, and threshold-tuning-disabled.

Heartbeat and summaries expose:

- `grpc_reader_update_count`
- `grpc_reader_poll_latency_ms_p50/p95/p99/max`
- `grpc_update_interarrival_ms_p50/p95/p99/max`
- `internal_queue_depth_current/max/capacity`
- `internal_queue_full_count`
- `decode_worker_lag_ms_max`
- `worker_partitions`
- `partitioning_enabled`
- `router_updates_received/routed`
- `router_fallback_count`
- `router_queue_depth_current/max`
- `router_queue_full_count`
- `partition_queue_depth_current_max`
- `partition_queue_depth_max_overall`
- `partition_queue_full_count_total/by_partition`
- `partition_updates_processed_total/by_partition`
- `partition_updates_per_second_total/by_partition`
- `partition_worker_lag_ms_p50/p95/p99/max`
- `partition_worker_lag_ms_max_by_partition`
- `partition_worker_lag_ms_p95_by_partition`
- `partition_queue_depth_max_by_partition`
- `partition_backlog_oldest_update_age_ms_by_partition`
- `partition_batch_size_max_by_partition`
- `partition_backpressure_trigger_partition`
- `partition_backpressure_trigger_reason`
- `backpressure_threshold_ms`
- `backpressure_observed_lag_ms`
- `backpressure_update_class`
- `backpressure_partition_id`
- `unknown_mint_route_count`
- `skipped_untracked_account_updates`
- `update_class_telemetry`
- `top_partition_keys_by_update_count`
- `top_mints_by_worker_updates`
- `top_accounts_by_worker_updates`
- `top_update_classes_by_lag`
- `top_update_classes_by_count`
- `pump_trade_fast_prefilter_count`
- `pump_trade_deep_processed_count`
- `pump_trade_skipped_untracked_count`
- `pump_trade_skipped_tombstoned_count`
- `pump_trade_unknown_mint_count`
- `pump_trade_deferred_feature_count`
- `pump_trade_feature_recompute_count`
- `pump_trade_deep_process_duration_ms_p95/max`
- `pump_trade_prefilter_duration_ms_p95/max`
- `pump_trade_state_update_duration_ms_p95/max`
- `pump_trade_risk_feature_duration_ms_p95/max`
- `unknown_mint_route_count_by_class`
- `account_pinned_update_count`
- `backpressure_hot_key`
- `backpressure_hot_mint`
- `backpressure_hot_account`
- `backpressure_deep_processed_count_at_trigger`
- `backpressure_skipped_count_at_trigger`
- `partition_decode_duration_ms_p50/p95/p99/max`
- `partition_lock_wait_ms_max`
- `partition_batch_size_p50/p95/max`
- `worker_backpressure_detected`
- `dirty_partition_queued_updates_discarded`
- `partition_worker_reset_count`
- `artifact_queue_depth_max`
- `artifact_queue_full_count`
- `artifact_worker_lag_ms_max`
- `r2_worker_lag_ms_max`
- `stream_reader_blocked_by_processing`
- `client_backpressure_detected`

These fields are release-gate evidence. A stale green heartbeat is not enough; provider counters must continue to advance and queue/backpressure telemetry must remain safe.

Pump trade/instruction traffic is prefiltered before deep worker processing. `pump_token_created` remains launch-critical and is processed immediately. Pump trades for active in-segment mints are deep-processed only while they can affect death/candidate gates; untracked, unknown-mint, malformed, other, and tombstoned-mint Pump traffic is cheap-counted and skipped from rich processing. Skipped Pump noise must not create attempt rows, rejected rows, candidate rows, replay eligibility, backtesting eligibility, threshold-tuning eligibility, or worker-backpressure lag by itself. Rich feature/risk recomputation is deferred to gates/checkpoints/finalization rather than recomputed for every Pump trade.

Worker-side lag must be diagnosed by partition and update class before launch caps are raised. Slot/liveness traffic and empty untracked account updates are reader-side cheap-counted and must not enter the heavy partition worker path. A `client_backpressure_detected` blocker should include the triggering partition, update class, observed lag, threshold, and bounded top-key summaries so the next patch can distinguish hot-key skew from unnecessary worker traffic.

## Gap-Segmented Artifact Policy

Provider gaps are label-boundary events. When a slice observes `provider_lagged_data_loss`, early stream close, reconnect exhaustion, progress stall, or client backpressure, any active mint whose observation window crosses that gap must be finalized as `terminal_inconclusive` for that segment and cannot become replay-eligible.

The service-owned run may continue as ordered segments when the overall run deadline, attempt budget, candidate target, and reconnect policy still allow it. The blocked segment is closed first, then the next stream segment starts after reconnect. A token created after reconnect can be counted inside the new clean segment if that segment has no blocker and normal material-candidate criteria are satisfied.

Segment artifacts are written under `segments/segment_<n>/` with a segment-level summary, countability decision, attempt ledger, candidate summary, and rejected summary. Run-level artifacts also include:

- `run_gap_events.csv`
- `run_segment_summary.csv`
- `run_countability_decision.json`

Run-level `run_provider_data_loss_seen=true` does not by itself make clean future post-reconnect segments dirty, but every gap-affected segment remains audit-only. Replay can be allowed at run level only when at least one clean segment has `replay_eligible_candidate_count > 0`, no artifact contradictions exist, final R2/artifact checks pass, and the run-level countability decision explicitly allows replay. Formal backtesting and threshold tuning remain false.

Run-level outcome validation groups rows by unique mint. Segment-level rows may document intermediate/audit-only outcomes, but a mint must not have contradictory final run outcomes such as `terminal_inconclusive` plus a replay-eligible/final candidate state or final dead rejection. `countability_decision.json` remains the strict source of truth and artifact consistency must block any contradiction.

Current conservative release policy keeps launch caps blocked until same-endpoint stream acceptance and at least one short real service-owned proof slice validate segment continuation without structural contradictions. Do not raise caps based on ping or provider-health probe success alone.

By default, the gate skips workspace clippy because existing broad workspace warnings are not yet release-gate clean. To enforce clippy too:

```bash
MATERIAL_HUNTER_RUN_CLIPPY=1 ./scripts/material_hunter_pre_run_gate.sh
```

## Decision Definitions

`PRE_RUN_RELEASE_PASS` means no known source/config/workflow/artifact/countability/replay/R2/service pre-run blocker remains. Expected failures are prevented before start or produce structured non-countable artifacts.

`PRE_RUN_RELEASE_BLOCK` means any known error path can still cause opaque exit, missing final artifacts, ambiguous countability, replayable partial candidates, stale green heartbeat, latest-run corruption, unbounded slice, blocking R2 checkpoint, unsafe config, commit mismatch, overlapping service, or countability/artifact contradiction.
