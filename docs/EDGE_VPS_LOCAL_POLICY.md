# Edge VPS Local File Policy

The VPS is an edge collector only. It should keep the smallest local footprint needed to stream, segment, upload, verify, and recover safely.

## Allowed

- `/home/ubuntu/pump-launch-quant/target/release/cli`
- At most two rollback binaries: `target/release/cli.prev.*`
- `config/default.toml` and `config/local.toml`
- `/home/ubuntu/pump-launch-quant.env`
- Active open edge segments for the currently running edge cycle only
- Latest local edge run manifests and small status summaries
- `data/reports/autopilot`
- `data/autopilot`
- `data/dataset_index.json`
- RPC ledger and stream-only audit outputs
- Minimal calibration/provider compatibility files needed by validation
- Quarantine directories preserving unsafe-to-delete residue until reviewed

## Forbidden

- `research_output` on the VPS
- Feature exports such as `features_*.csv.zst`
- Decision/fill exports such as `decisions.csv` and `fills.csv`
- Backtest reports from research runs
- Heavy token feature snapshot directories
- Historical local segment archives after R2 verification, beyond a small latest-run spool
- Duplicate `/home/ubuntu/data` roots
- Build caches in `/dev/shm`
- Duplicate binaries outside `target/release/cli`
- Rollback binaries beyond the newest two
- Giant journals or syslogs
- Any RPC/API/metadata/social enrichment cache

## Cleanup Rules

- Never delete unverified run data.
- Never delete active open segments.
- Quarantine stale `.open` files instead of deleting them.
- Only compact historical edge report directories after `unuploaded_run_count=0` and R2 verification status is clean.
- Keep enough local state for the current and latest few edge runs; R2 is the durable archive.
- Do not keep unrelated executors or trading bots running on the edge VPS.

## Operational Check

An edge-only VPS should satisfy:

- `research_output` missing or empty.
- Feature/decision/fill exports count is zero.
- Duplicate `/home/ubuntu/data` root is absent.
- `/dev/shm` has no `pump*` build directories.
- Old rollback binary count is at most two.
- `disk-preflight` reports `pre_cycle_blocked=false`.
- `validate-stream-only` and `validate-edge-mode` pass.
- `rpc_network_calls_total=0` for edge runs.
