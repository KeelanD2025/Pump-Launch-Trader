# Material Hunter Health Sentinels

Phase 107F makes the material-candidate hunter a streaming, checkpointed service. GitHub may start, inspect, stop, and collect the service, but GitHub must not be the long-running owner of live collection.

## Startup Sentinels

- `attempt_ledger.csv` must be created before the Geyser subscription starts.
- `health/heartbeat.json` must be written by the CLI hunter, not only by the shell wrapper.
- The service run id, CLI run id, heartbeat run id, and artifact directory must match.
- A startup watchdog check must fail fast if heartbeat or attempt ledger is missing.

## Runtime Sentinels

- Provider liveness is tracked in `health/provider_liveness.csv`.
- Raw provider updates refresh CLI heartbeat even when no normalized Pump event is emitted.
- `no_launches_seen_but_stream_alive` is distinct from `provider_zero_updates`.
- CLI heartbeat includes attempted launches, active tracked mints, rejected count, candidate count, provider updates, Pump updates, disk, RSS, and blocker state.
- R2 checkpoints are required during the run; failure writes `early_failure_blocker` and stops the slice as not countable.
- Missing required evidence is recorded as unavailable, never zero.
- Holder metrics remain stream-authoritative; holder RPC stays disabled.

## Candidate Horizon

- A 300s survivor writes candidate checkpoint artifacts but remains active unless the configured final horizon is 300s.
- A 900s survivor updates candidate artifacts but remains active unless the configured final horizon is 900s.
- Off-VPS candidate replay is not allowed from a 300s-only checkpoint unless explicitly configured later.
- Long campaign slices must use service start/status/stop/collect mode with `material-hunter-watchdog`; foreground SSH hunter execution is disabled.

## Stop And Countability

- Stop mode must verify the service is inactive.
- Interrupted runs must write `hunter_summary_interrupted.json` or `countability_decision.json`.
- Countability must be computed from actual artifacts, R2 verification, and hard invariants.
- Interrupted or failed slices remain audit-only unless countability explicitly says otherwise.
