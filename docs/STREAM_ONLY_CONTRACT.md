# Stream Only Contract

`stream_only` means the runtime may consume stream data, stored stream logs, replay inputs, and offline datasets prepared before the run. It may not spend JSON-RPC/eRPC credits for market data, holder scans, metadata enrichment, confirmation, reconciliation, or blockhash fallback.

## Allowed live inputs

- Yellowstone/Geyser transaction, account, slot, and block/block-meta streams
- Deshred pre-execution streams when the provider and build support them
- Raw shred UDP intake when a production backend is actually compiled in
- Mock, fixture, replay, and stored-event sources
- Offline imported datasets loaded before runtime starts

## Forbidden hot-path inputs

- `getAccountInfo`
- `getProgramAccounts`
- `getTokenLargestAccounts`
- `getTokenAccountsByOwner`
- `getSignaturesForAddress`
- `getTransaction`
- `getParsedTransaction`
- `getSignatureStatuses`
- `getLatestBlockhash`
- metadata URI fetching
- HTTP/social/metadata enrichment APIs
- hidden REST/eRPC market-data calls

## Data contract by domain

### Token launch discovery

- Allowed: streamed Pump transactions, streamed account updates, replay/store imports
- Forbidden: RPC scans for recent launches

### Pump buys and sells

- Allowed: Geyser transaction stream, Pump instruction decode, account effects
- Forbidden: transaction polling via RPC

### Bonding curve state

- Allowed: streamed account updates plus transaction decode
- Forbidden: `getAccountInfo` polling

### Holder balances and top holders

- Allowed: token account updates, token-balance metadata, local holder index
- Forbidden: `getTokenAccountsByOwner`, `getTokenLargestAccounts`

### Dev holdings and wallet history

- Allowed: locally observed stream history, creator tracking from streamed events
- Forbidden: wallet-history RPC scans

### Funding graph

- Allowed: streamed System Program transfers and streamed account relationships
- Forbidden: RPC history backfills

### Metadata

- Allowed: create-instruction fields, streamed metadata-account bytes, local URI/domain parsing
- Forbidden: URI fetch, image fetch, social fetch, web fetch in the runtime hot path

### Confirmation and reconciliation

- Allowed: Geyser transaction/account/slot updates and locally persisted stream history
- Forbidden: `getSignatureStatuses`, `getTransaction`, `getParsedTransaction` fallback

### Blockhash

- Allowed: streamed block/block-meta cache
- Forbidden: `getLatestBlockhash` in stream-only mode

### Backtesting and paper replay

- Allowed: stored normalized event logs and derived offline exports
- Forbidden: live RPC backfill or repair

## Validation rules

When `stream_only.enabled = true`:

- market-data RPC must be disabled
- holder/top-holder RPC must be disabled
- metadata fetch must be disabled
- reconciliation/backfill/confirmation RPC fallback must be disabled
- blockhash RPC must be disabled
- send/execution RPC must remain disabled by default
- RPC budgets must be zero
- at least one stream or replay source must be available

## Execution boundary

Live execution remains disabled by default. If execution is ever enabled in the future, the send path must stay separate from market-data collection and must still pass through `RpcBudgetManager`. In stream-only mode, missing or stale stream blockhash state must reject execution rather than call RPC.
