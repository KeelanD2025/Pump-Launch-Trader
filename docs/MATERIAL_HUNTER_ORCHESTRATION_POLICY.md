# Material Hunter Orchestration Policy

Phase 107E moves long material-candidate hunting out of foreground GitHub
Actions SSH sessions. GitHub may build, deploy, start, inspect, stop, and
collect reports, but it must not own an 8-hour live hunter process.

## Execution Model

- GitHub Actions builds and tests the Linux CLI, deploys the prebuilt binary,
  runs `vps-pre-run-housekeeping`, starts a VPS-owned hunter service, verifies
  that the service started, uploads start/preflight artifacts, and exits.
- The VPS owns the long-running process through a run-specific systemd unit:
  `pump-launch-quant-material-hunter-<run_id>.service`.
- The hunter wrapper writes a heartbeat and resource telemetry every 30 seconds.
- The wrapper writes interrupted summaries and countability reports on
  `SIGTERM`, `SIGINT`, or `SIGHUP`.
- Candidate replay is allowed only for candidates from counted slices or counted
  service runs with final R2 verification.

## Countability

- GitHub workflow success is not sufficient.
- A hunter slice/run is counted only when final hunter artifacts exist, R2
  verification passes, hard invariants pass, and `countability_decision.json`
  explicitly says `counted_phase107b_result=true`.
- Interrupted, cancelled, or failed slices are audit-only unless their own
  finalized countability report says otherwise.
- Partial outputs from failed or interrupted runs remain useful for diagnosis
  but must not be used for off-VPS candidate replay.

## Safety

- Live trading remains disabled.
- Holder RPC remains disabled.
- RPC mint supply remains diagnostic-only and non-canonical.
- Binary malicious labels are forbidden.
- Provider-confirmed bundle evidence is false unless a provider/API explicitly
  confirms it.
- Missing evidence is unavailable, not zero.

## Slice Campaigns

Campaigns should be assembled from verified counted slices:

- `max_wall_clock_seconds_per_slice <= 5400`
- `max_attempted_launches_per_slice = 10..15`
- `max_material_candidates_per_slice = 1..2`
- each slice runs housekeeping before start
- each slice emits final or interrupted countability
- the campaign manifest aggregates counted slices only
