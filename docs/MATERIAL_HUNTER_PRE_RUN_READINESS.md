# Material Hunter Pre-Run Readiness

This document defines the release gate before any longer material-candidate hunter collection.

## Allowed Next Action

- Service-owned, sequential, sliced material-candidate collection only.
- Each slice must use mandatory VPS housekeeping, startup sentinels, watchdog checks, bounded R2 checkpoints, `RuntimeMaxSec`, final artifact collection, and artifact consistency validation.

## Blocked Actions

- Foreground material hunter runs.
- Unsliced or full campaign collection.
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

By default, the gate skips workspace clippy because existing broad workspace warnings are not yet release-gate clean. To enforce clippy too:

```bash
MATERIAL_HUNTER_RUN_CLIPPY=1 ./scripts/material_hunter_pre_run_gate.sh
```

## Decision Definitions

`PRE_RUN_RELEASE_PASS` means no known source/config/workflow/artifact/countability/replay/R2/service pre-run blocker remains. Expected failures are prevented before start or produce structured non-countable artifacts.

`PRE_RUN_RELEASE_BLOCK` means any known error path can still cause opaque exit, missing final artifacts, ambiguous countability, replayable partial candidates, stale green heartbeat, latest-run corruption, unbounded slice, blocking R2 checkpoint, unsafe config, commit mismatch, overlapping service, or countability/artifact contradiction.
