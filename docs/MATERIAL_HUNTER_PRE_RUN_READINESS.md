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

`provider_lagged_data_loss` during a collection slice is a safe structural outcome when final artifacts exist, countability is false, replay is false, and R2/artifact consistency pass. It is still an operational provider-quality blocker. Longer collection must not resume after repeated `provider_lagged_data_loss` until provider acceptance passes or the Geyser endpoint/configuration is remediated. Launch caps must not be raised while provider acceptance is failing.

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

By default, the gate skips workspace clippy because existing broad workspace warnings are not yet release-gate clean. To enforce clippy too:

```bash
MATERIAL_HUNTER_RUN_CLIPPY=1 ./scripts/material_hunter_pre_run_gate.sh
```

## Decision Definitions

`PRE_RUN_RELEASE_PASS` means no known source/config/workflow/artifact/countability/replay/R2/service pre-run blocker remains. Expected failures are prevented before start or produce structured non-countable artifacts.

`PRE_RUN_RELEASE_BLOCK` means any known error path can still cause opaque exit, missing final artifacts, ambiguous countability, replayable partial candidates, stale green heartbeat, latest-run corruption, unbounded slice, blocking R2 checkpoint, unsafe config, commit mismatch, overlapping service, or countability/artifact contradiction.
