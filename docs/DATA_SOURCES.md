# Data Sources

## Primary sources

### ShredStream

Implemented as a real UDP receiver with bounded backpressure, packet/drop/decode metrics, fixture-backed decoding, tentative Pump event extraction, and a reconciliation cache against canonical Geyser events.

Important limitation:

- the production-facing decoder path currently fails closed unless a real Solana shred decoder backend is linked
- shred-only observations remain tentative and never bypass safety filters
- Geyser-only fallback is supported when config disables shreds or allows fallback because the production decoder is unavailable
- tentative raw-shred or deshred sell warnings are early intent only; they can arm or trigger paper exits but must later reconcile to canonical Geyser outcome

### Yellowstone/Geyser

Implemented as the canonical processed/confirmed/rooted stream interface. The ingestion crate models:

- transaction updates
- account updates
- slot status updates
- block metadata
- reconnects and slot gaps
- deduplication keys keyed by slot, signature, tx index, account pubkey, and write version

Phase 15 status:

- mocked live-data paper uses a real async supervisor loop and mocked Geyser/Shred source adapters
- Geyser-only fallback is supported when shreds are disabled or unavailable and config allows it
- if shreds are required and the production shred decoder is unavailable, startup fails closed
- if Geyser is required and disabled, startup fails closed
- the real network Geyser source adapter is now wired behind the runtime source abstraction
- if `GEYSER_ENDPOINT` is missing in real live-data mode, startup fails clearly and suggests `--mock-live`
- auth resolution honors `auth_token_env` and fails clearly when auth is required but unavailable
- real Yellowstone subscribe requests are now issued through tonic when endpoint/auth config is present
- transaction, account, slot, and optional block-meta updates are normalized into the same event pipeline used by fixture and replay paths
- transport connections and stream failures are handled honestly; data gaps are emitted on disconnect, reconnect, slot jump, or canonical queue overflow
- this is still a paper/data-collection path, not a claim of production trading readiness

### Deshred / pre-execution intent

- the runtime now exposes a real `deshred_pre_execution` source adapter when the Yellowstone build and provider endpoint support `SubscribeDeshred`
- `inspect-deshred-capability` reports build/client/filter/address support plus endpoint/auth configuration without requiring a live connection
- if the configured provider/proto path does not support deshred for the selected endpoint, startup can skip it or fail clearly depending on config
- deshred/pre-execution events are treated exactly like other tentative intent: never canonical, always later reconciled
- pre-execution deshred updates do not include `TransactionStatusMeta`; the runtime never infers canonical execution success from that stream alone
- when multiple tentative sources are present, the runtime records source precedence and deduplicates by signature/fingerprint instead of double-counting the same sell intent
- `smoke-deshred-provider` is the safe way to test a real endpoint: it records missing-endpoint, auth-rejected, unimplemented, connected-but-zero-update, and updates-received outcomes without treating deshred as canonical truth
- `provider-env-precheck` is the zero-network readiness gate before any real endpoint attempt; it verifies stream-only posture, endpoint/auth presence, and whether the current environment is ready for Geyser-only or deshred-inclusive smoke
- `smoke-geyser-provider` is the canonical-stream counterpart to deshred smoke; it validates Yellowstone/Geyser connectivity and decoded canonical updates independently of tentative pre-execution support
- `smoke-streams` combines Geyser smoke and optional deshred smoke so a first real collection run can get a single go/no-go report without enabling paper strategy behavior
- `provider-dry-run` wraps that smoke step plus optional short paper validation, but it still treats a missing endpoint or unsupported provider as an operational observation rather than a stream success
- `collect-first-live-paper` is the first real stream-only collection wrapper; it stays paper-only, records checkpoint/final reports, and stores the RPC ledger plus stream-only proof alongside the collected events
- `analyze-live-collection`, `check-backtest-readiness`, and `run-research-cycle` are post-collection analysis tools. They do not fetch missing data; they decide whether the observed stream dataset is sufficient for offline replay/backtest work
- `run-autopilot` sits above those commands and chooses between real streams, Geyser-only fallback, or explicit mock collection according to config and provider status. It never treats missing endpoints or unsupported deshred as a hidden success
- zero-update outcomes are reported honestly: a provider can connect and still deliver no pre-execution updates during the window, which is evidence about availability, not evidence of canonical inactivity
- provider compatibility observations are stored separately from run data and hash the endpoint host instead of storing the raw endpoint when that safety option is enabled
- mixed-source smoke can layer a mock secondary tentative source on top of deshred so dedup behavior is testable without requiring production raw-shred support
- tentative early-intent events are persisted with runtime sequence numbers so replay can reproduce the original tentative/canonical merge order instead of re-synthesizing it from wall-clock timing
- report/export/backtest artifacts are not canonical data sources; they are derivative research outputs attached to a run and excluded from default latest source selection

## RPC policy

RPC is intentionally off the hot path. In the default Phase 24 profile, stream-only mode is enabled and every JSON-RPC/eRPC/HTTP metadata attempt must route through `RpcBudgetManager`, where it is denied before network touch unless explicitly allowed by config.

All future RPC activity must carry:

- reason
- caller
- related token/signature
- provider endpoint
- estimated credit cost
- allow/deny result

Unknown or budgetless live-mode RPC requests must fail closed.

Forbidden in the hot path:

- per-token `getAccountInfo`
- holder polling
- `getTokenLargestAccounts`
- `getSignaturesForAddress` scans
- metadata/web fetches
- any hidden runtime polling loop that scales with token count

Stream-only interpretation:

- Geyser and deshred gRPC streams are allowed and do not count as market-data RPC
- shred UDP intake is allowed and does not count as RPC
- inbound metrics HTTP is allowed and does not count as outbound RPC
- blockhash, confirmation, reconciliation, and metadata enrichment RPC are all disabled in the default configs
- live execution stays disabled by default, so send RPC remains off as well
- every provider smoke, collection, quality, readiness, and research-cycle artifact now includes the stream-only proof block: zero RPC network calls, zero credits used, whether any denials occurred, and the run-local RPC ledger path

See `docs/STREAM_ONLY_CONTRACT.md` for the full allowed/forbidden matrix and `docs/RPC_AUDIT.md` for the module-by-module audit.
