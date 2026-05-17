# RPC Audit

This audit classifies every external-call path in the workspace and defines how stream-only mode treats it.

| Module | Call path | External dependency | Category | Allowed | Config gate | Stream-only behavior | Reason | Test coverage |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `crates/ingest-geyser/src/lib.rs` | Yellowstone/Geyser subscribe stream | tonic gRPC | Stream source | Yes | `geyser.enabled` / ingest config | Allowed | Canonical truth stream | integration tests + runtime smoke/mock runs |
| `crates/runtime/src/live_source.rs` | Deshred `SubscribeDeshred` stream | tonic gRPC | Stream source | Yes | `ingest.deshred.*` | Allowed | Tentative pre-execution stream, never canonical by itself | capability + smoke + mock deshred tests |
| `crates/ingest-shred` | UDP shred intake | UDP | Stream source | Yes | shred config / feature gates | Allowed | Tentative earliest visibility | fixture + runtime tests |
| `crates/runtime/src/lib.rs` | `/metrics`, `/healthz`, `/readyz` listener | inbound HTTP server | Stream source | Yes | runtime metrics config | Allowed | Observability only; no outbound spend | existing runtime/health tests |
| `crates/executor/src/lib.rs` | `sendTransaction` gate | JSON-RPC | Execution send path | No by default | `execution.enabled`, `execution.use_rpc_send`, `stream_only.*` | Denied in stream-only unless explicitly loosened later | Live execution disabled by default; no accidental spend | executor tests + rpc-budget tests |
| `crates/rpc-budget/src/lib.rs` | Any JSON-RPC/eRPC method request | JSON-RPC/eRPC | MarketData / HolderScan / TopHolderScan / MetadataFetch / Backfill / Reconciliation / Confirmation / Blockhash / TransactionSend / Simulation / Emergency | No by default | `stream_only` + `rpc_budget` + execution config | Denied before network touch | Single gate for non-stream external calls | rpc-budget unit tests |
| `crates/common/src/config.rs` | Stream-only validation | config only | Policy enforcement | Yes | `stream_only.enabled` | Fails config validation if forbidden RPC toggles are on | Prevents invalid runtime startup | config integration tests |
| `crates/cli/src/main.rs` | provider compatibility export / report generation | local filesystem only | Test/dev-only | Yes | explicit CLI commands | Allowed | Offline artifact generation | CLI tests / end-to-end commands |

## Explicitly forbidden hot-path paths

The audit found no implemented hot-path client usage for:

- `RpcClient`
- `JsonRpcClient`
- `reqwest::Client`
- `reqwest::get`
- `hyper::Client`
- metadata URI fetch
- hidden REST market-data calls
- holder or wallet-history RPC scans

Those patterns are checked by the source-scan test in `crates/tests/tests/phase_one_to_three.rs`.

## Stream-only interpretation

- Geyser/deshred gRPC streams are allowed and do not count as market-data RPC
- UDP shred intake is allowed and does not count as RPC
- inbound metrics HTTP is allowed and does not count as outbound RPC
- every JSON-RPC/eRPC/HTTP metadata call must pass through `RpcBudgetManager`
- in stream-only mode, `RpcBudgetManager` denies forbidden categories before any network touch and records the denial in the RPC ledger

## Current conclusion

The workspace currently has:

- real stream ingestion paths for Geyser, deshred, and fixture/mock/replay
- a disabled-by-default execution send path guarded by `RpcBudgetManager`
- no hidden market-data RPC client implementation
- no hot-path metadata/web fetch implementation

Production raw shred decoding remains fail-closed until a supported backend is compiled in; that limitation does not weaken the zero-eRPC hot-path guarantee.
