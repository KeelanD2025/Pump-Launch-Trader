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
- `transaction_signature_seen_count`
- `transaction_duplicate_signature_count`
- `transaction_duplicate_signature_skipped_count`
- `transaction_prefilter_count`
- `transaction_deep_processed_count`
- `transaction_mapping_hint_only_count`
- `transaction_untracked_pump_skipped_count`
- `transaction_account_pinned_unknown_count`
- `transaction_tombstoned_mint_skipped_count`
- `transaction_malformed_or_unknown_count`
- `transaction_other_untracked_skipped_count`
- `account_pinned_active_count`
- `account_pinned_unknown_count`
- `account_pinned_skipped_count`
- `account_pinned_deep_processed_count`
- `active_mint_transaction_update_count`
- `active_mint_transaction_deep_processed_count`
- `active_mint_transaction_skipped_count`
- `active_mint_transaction_coalesced_count`
- `active_mint_transaction_dirty_feature_count`
- `active_mint_transaction_delta_flush_count`
- `active_mint_transaction_budget_exceeded_count`
- `active_mint_transaction_degraded_count`
- `active_mint_transaction_queue_pressure_count`
- `top_active_mints_by_transaction_count`
- `top_active_mints_by_coalesced_count`
- `top_active_mints_by_deep_processed_count`
- `top_active_mints_by_queue_pressure`
- `top_active_mints_by_transaction_lag`
- `active_mint_delta_flush_duration_ms_p95/max`
- `degraded_active_mint_count`
- `degraded_active_mints`
- `partition_queue_pressure_preempted_count`
- `partition_queue_pressure_dominant_mint`
- `partition_queue_pressure_dominant_mint_update_count`
- `partition_queue_pressure_degraded_mint`
- `partition_queue_pressure_preempted_before_full`
- `partition_queue_full_after_preemption`
- `preemptive_noisy_mint_degraded`
- `classified_update_count_by_class`
- `processing_lane_count`
- `deep_processed_count_by_class`
- `cheap_skipped_count_by_class`
- `mapping_hint_count`
- `vault_delta_count`
- `trade_delta_count`
- `holder_delta_count`
- `deferred_feature_count`
- `high_throughput_mint_count`
- `high_throughput_mints`
- `per_mint_batch_deep_limit_hits`
- `fair_scheduler_rotations`
- `top_mints_by_lane_count`
- `top_mints_by_deferred_features`
- `top_mints_by_high_throughput_events`
- `feature_flush_count_by_reason`
- `feature_flush_duration_ms_p95/max`
- `classification_duration_ms_p95/max`
- `module_dispatch_duration_ms_p95/max`
- `transaction_feature_deferred_count`
- `transaction_feature_recompute_count`
- `transaction_deep_process_duration_ms_p95/max`
- `transaction_prefilter_duration_ms_p95/max`
- `transaction_state_update_duration_ms_p95/max`
- `transaction_risk_feature_duration_ms_p95/max`
- `backpressure_transaction_class`
- `backpressure_transaction_signature`
- `backpressure_transaction_mint`
- `backpressure_transaction_account`
- `backpressure_deep_transaction_count_at_trigger`
- `backpressure_skipped_transaction_count_at_trigger`
- `backpressure_account_pinned_count_at_trigger`
- `partition_decode_duration_ms_p50/p95/p99/max`
- `partition_lock_wait_ms_max`
- `partition_batch_size_p50/p95/max`
- `worker_backpressure_detected`
- `dirty_partition_queued_updates_discarded`
- `partition_worker_reset_count`

Transaction updates use a fast prefilter before rich ingest. Token-created and active tracked
mint/account transactions remain deep-processed. Mapping hints update stream-authoritative
account-to-mint state and then skip rich processing. Untracked Pump transactions,
account-pinned unknown traffic, duplicate signatures, tombstoned mints, malformed transactions,
and other unrelated transactions are cheap-counted only. Skipped transaction noise must not create
attempt rows, candidate rows, rejected rows, replay eligibility, backtesting eligibility, tuning
eligibility, provider-confirmed bundle claims, holder RPC data, or canonical RPC mint supply.
Active-mint transaction feature work is marked dirty/deferred and recomputed only at gates,
checkpoints, segment close, or finalization.

Noisy active mints are governed by per-mint queue/rate/deep-processing budgets. Repeated
`transaction_active_mint` or `pump_trade_active_mint` updates are cheap-delta accumulated and
coalesced between configured flush windows. If one mint threatens partition queue safety, the
runtime may preemptively degrade only that mint to audit-only `terminal_inconclusive` tracking,
remove it from rich active tracking, and continue the segment when global queues remain safe. A
degraded mint must never be replay-eligible, must not create fake candidate/rejected rows, and R2
verification cannot override that exclusion. If preemption fails and a partition queue fills, the
segment is still classified as `client_backpressure_detected`.

The worker path uses an explicit classified-update dispatcher. A cheap `FastClassifier` assigns
each worker-relevant update to a stable `MaterialUpdateClass`, `ProcessingLane`, routing key,
optional mint/account/signature, and safety flags before full decode. The processing modules are:

- `CriticalLaunchModule` for `PumpTokenCreated` and `TransactionTokenCreated`; these updates must never be cheap-skipped.
- `AccountMintMapper` for stream-authoritative account/mint/vault hints; mapping updates do not run rich risk/feature processing.
- `VaultCurveDeltaProcessor` for active bonding-curve/vault deltas; these mark mint state dirty and can trigger gates without per-update full recompute.
- `TradeDeltaProcessor` for active tracked Pump/transaction trades; repeated updates are coalesced and rich feature work is deferred to gates/checkpoints/finalization.
- `HolderStateProcessor` for active token-account/owner deltas; holder state remains stream-authoritative and holder RPC remains disabled.
- `CheapCounterModule` for untracked, tombstoned, duplicate, malformed, account-pinned unknown, and other noise; these updates must not create attempts, candidates, rejected rows, replay eligibility, backtesting eligibility, tuning eligibility, provider-confirmed bundle claims, holder RPC data, or canonical RPC supply.
- `DeferredFeatureModule` for gate/checkpoint/segment-close/finalization recomputes outside the gRPC reader hot path.

Per-mint tracking modes are `Rich`, `HighThroughput`, `DegradedAuditOnly`, `Tombstoned`, and
`Finalized`. `HighThroughput` mints coalesce high-frequency events while preserving gate-critical
events and may remain candidate-eligible only when observation is complete and clean.
`DegradedAuditOnly` mints are terminal-inconclusive/audit-only, cannot become replay-eligible, and
future updates are cheap-counted. The partition fair scheduler rotates across active mint/account
keys within each batch and preserves arrival order for the same key, so one noisy mint cannot
monopolize a partition batch while independent keys wait.

- `artifact_queue_depth_max`
- `artifact_queue_full_count`
- `artifact_worker_lag_ms_max`
- `r2_worker_lag_ms_max`
- `stream_reader_blocked_by_processing`
- `client_backpressure_detected`

These fields are release-gate evidence. A stale green heartbeat is not enough; provider counters must continue to advance and queue/backpressure telemetry must remain safe.

Pump trade/instruction traffic is prefiltered before deep worker processing. `pump_token_created` remains launch-critical and is processed immediately. Pump trades for active in-segment mints are deep-processed only while they can affect death/candidate gates; untracked, unknown-mint, malformed, other, and tombstoned-mint Pump traffic is cheap-counted and skipped from rich processing. Skipped Pump noise must not create attempt rows, rejected rows, candidate rows, replay eligibility, backtesting eligibility, threshold-tuning eligibility, or worker-backpressure lag by itself. Rich feature/risk recomputation is deferred to gates/checkpoints/finalization rather than recomputed for every Pump trade.

Worker-side lag must be diagnosed by partition and update class before launch caps are raised. Slot/liveness traffic and empty untracked account updates are reader-side cheap-counted and must not enter the heavy partition worker path. A `client_backpressure_detected` blocker should include the triggering partition, update class, observed lag, threshold, and bounded top-key summaries so the next patch can distinguish hot-key skew from unnecessary worker traffic.

## VPS Relay-Only Architecture

The existing service-owned VPS material-hunter mode remains valid but stays gated by the full
disk/artifact readiness policy. Relay-only mode is a separate opt-in architecture for cases where
the VPS is whitelisted for provider streams but should not be the material-hunter processing or
artifact host.

In relay-only mode, the VPS acts only as stream ingress. It connects to the configured
Geyser/SREDs provider, drains updates as fast as possible, wraps each update in a stable
`RelayFrame`, and forwards frames to a local collector over `mtls_grpc_over_private_tunnel`
transport. The relay writes only capped operational health such as `relay_health_summary.json` and
`relay_exit_status.json` under `/run/pump-launch-quant/stream-relay` or another explicit relay
health directory. It must not create `phase107b_material_candidate_hunter` run directories, attempt
ledgers, candidate summaries, rejected summaries, segment summaries, run countability decisions, R2
manifests, or final material-hunter artifacts on the VPS. It must not mutate
`phase107b_material_candidate_hunter/latest_run_id`.

The local machine owns material-hunter processing in relay mode. `local-stream-collector` receives
relayed frames, verifies per-stream sequence monotonicity and payload hashes, maps relay control
frames into the material-hunter blocker taxonomy, and runs the classified dispatcher, partition
workers, active-mint state machine, segmenting, countability, artifact writing, and R2 uploads
locally. `local-collector-preflight` must pass before accepting relay data; it validates writable
local output, local free disk, stream-only safety, R2 readiness when upload is requested, disabled
holder RPC, non-canonical RPC mint supply, and disabled replay/backtesting/tuning/trading.

Raw relay capture alone is only a transport proof. A relay-local dataset proof must run
`local-stream-collector --run-material-hunter` so verified `RelayFrame` payloads are decoded back
into Yellowstone `SubscribeUpdate` messages and injected into the same classified
material-hunter stream path used by the live Geyser connector. Use a receiver window longer than the
VPS relay duration, but set `--material-duration-seconds` to the intended proof slice duration so a
normal relay stop is not misclassified as a provider close before deadline. In this mode
`countability_decision.json` and `run_countability_decision.json` are local-source-of-truth artifacts,
and local R2 upload/verification is required for counted R2-primary proof results.

`RelayFrame` fields include `schema_version`, `relay_session_id`, `stream_id`, `provider`,
`source_kind`, `subscription_fingerprint`, `sequence`, `received_at_unix_nanos`, optional `slot`,
optional `commitment`, `payload_codec`, `payload_compressed`, `payload_hash`, `payload_len`,
`payload_bytes`, optional `control_kind`, optional `relay_error`, optional `blocker_class`,
optional `provider_status`, optional safe provider error code/message,
`upstream_reconnect_attempt`, and `will_reconnect`. Control frames include `relay_started`,
`relay_heartbeat`, `relay_upstream_connected`, `relay_upstream_reconnect_started`,
`relay_upstream_reconnected`, `relay_upstream_reconnect_exhausted`, `relay_upstream_blocker`,
`relay_receiver_backpressure`, `relay_receiver_unavailable`, `relay_stopped`,
`relay_sequence_gap`, and `relay_shutdown`.

Relay gaps are label-boundary events. `relay_sequence_gap`, `relay_receiver_unavailable`,
`relay_downstream_backpressure`, and provider upstream controls such as
`relay_upstream_blocker(provider_lagged_data_loss)` are mapped to structured material-hunter
segment blockers. On recoverable upstream blockers the VPS relay emits the blocker frame before any
bounded reconnect/backoff attempt, then emits `relay_upstream_reconnect_started` and
`relay_upstream_reconnected` if the provider stream resumes within the relay budget. The local
collector closes only the dirty segment, finalizes active gap-crossing mints as
`terminal_inconclusive`, and starts a clean post-reconnect segment when new data arrives. Clean
post-gap local segments may become countable only when normal countability criteria are met and the
local `countability_decision.json` allows it. Run-level provider data loss may be true while clean
post-gap segment countability remains internally clean. R2 success from the local machine cannot
override relay/provider blockers.

Workflow relay control uses the separate `relay_control_action` input and the separate
`pump-launch-quant-stream-relay` service name. It must not start the material-hunter service, stop
the material-hunter service, inspect the wrong material-hunter slice, or overwrite material-hunter
`latest_run_id`. Relay mode has its own smaller relay health/disk policy and does not lower the
existing VPS material-hunter disk floors.

Local helper scripts:

```bash
./scripts/local_stream_collector_preflight.sh
./scripts/local_stream_collector_run.sh
./scripts/local_stream_collector_validate_artifacts.sh
```

### Local Collector Storage Modes

`local-collector-preflight` supports explicit storage modes so the local collector does not treat
every relay proof as a full local artifact mirror:

- `local_mirror`: local disk is the primary artifact store. This is the conservative fallback and
  still requires `50000 MB` free for normal collection by default. R2 may upload copies, but local
  artifacts remain durable locally.
- `r2_primary`: R2 is the durable artifact store. Local disk is bounded staging/spool plus compact
  manifests, pointers, countability summaries, exit status, and verification summaries. R2 upload
  and R2 health verification are mandatory before any material-hunter artifacts are produced.
- `r2_primary` with `collector_proof`: short proof mode. It requires `5000 MB` by default and emits
  a warning that it is not sufficient for repeated collection.
- `r2_primary` with `collection`: capped collection mode. It requires `10000 MB` by default.
- `extended_local_mirror` and `extended_r2_primary`: longer modes with higher disk expectations.

The old `50000 MB` threshold applies to `local_mirror` collection, not R2-primary collector proof.
For example, `21421 MB` free local disk is enough for `r2_primary` collector proof and capped
R2-primary collection when R2 upload is enabled and verified, but it still blocks `local_mirror`
collection.

R2-primary writes bulk artifacts as bounded chunks/shards such as
`attempt_ledger/part-000001.ndjson`, `candidate_summary/part-000001.ndjson`,
`rejected_summary/part-000001.ndjson`, `provider_liveness/part-000001.csv`, and
`checkpoint_status/part-000001.csv`. The local spool has a configured maximum size
(`2048 MB` for proof and `8192 MB` for capped collection by default). If the spool fills or R2 upload
backpressure prevents verification, the collector must stop or segment-block safely with
`r2_local_spool_full`, `r2_upload_backpressure`, `r2_checkpoint_failed`, or
`r2_final_upload_failed`; it must never silently continue into unbounded local storage.

Before a live R2-primary collector proof, run preflight with live R2 health verification, for
example `./scripts/local_stream_collector_preflight.sh --storage-mode r2-primary --mode
collector-proof --verify-r2-health-live`. The live check writes a small health object under the
managed R2 research prefix, verifies it through the existing R2 object verifier, and fails closed
without printing secrets if credentials, bucket, prefix, upload, or verification are invalid.

Retention modes are explicit:

- `keep_all`: keeps local artifacts and therefore needs larger disk.
- `keep_manifests_after_verified_r2`: preferred for R2-primary. Verified bulk chunks may be removed
  after R2 object verification while local manifests, pointer files, countability decisions, exit
  status, R2 verification summaries, and final manifests are retained.
- `delete_verified_bulk_artifacts`: removes verified bulk chunks after R2 verification. It is
  forbidden when R2 upload is disabled or unverified.

Local artifact consistency may validate from the R2 manifest in R2-primary mode. R2 success still
cannot make a blocked run countable, cannot make degraded or terminal-inconclusive mints
replay-eligible, and cannot enable replay, formal backtesting, threshold tuning, or live trading.

Relay-only mode is not the default. Existing full VPS material-hunter mode remains available for
fallback, but it is still blocked whenever the VPS disk/artifact gate is below threshold.

## Gap-Segmented Artifact Policy

Provider gaps are label-boundary events. When a slice observes `provider_lagged_data_loss`, early stream close, reconnect exhaustion, progress stall, or client backpressure, any active mint whose observation window crosses that gap must be finalized as `terminal_inconclusive` for that segment and cannot become replay-eligible.

The service-owned run may continue as ordered segments when the overall run deadline, attempt budget, candidate target, and reconnect policy still allow it. The blocked segment is closed first, then the next stream segment starts after reconnect. A token created after reconnect can be counted inside the new clean segment if that segment has no blocker and normal material-candidate criteria are satisfied.

Segment artifacts are written under `segments/segment_<n>/` with a segment-level summary, countability decision, attempt ledger, candidate summary, and rejected summary. Run-level artifacts also include:

- `run_gap_events.csv`
- `run_segment_summary.csv`
- `run_countability_decision.json`

For `client_backpressure_detected` or `worker_backpressure_detected`, the blocked segment must persist a `blocker_snapshot` before queue/router/worker reset. The snapshot captures the blocker source, triggering update class, partition id, observed lag, threshold, hot key/mint/account, transaction trigger fields, partition queue/lag summaries, and bounded top-N update/key summaries. `run_gap_events.csv` must flatten the core trigger fields, and `run_segment_summary.csv` must mark `blocker_snapshot_available=true`. New segment artifacts without that snapshot are release-gate blockers; older pre-snapshot artifacts remain parseable for audit only.

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
