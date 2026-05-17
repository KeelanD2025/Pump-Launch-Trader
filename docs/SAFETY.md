# Safety

Safety is a first-class design constraint, not an afterthought.

## Current safeguards

- live mode disabled by default in config
- explicit config hashing
- IDL hashing
- data-gap representation in the shared event schema
- RPC fail-closed rules
- structured reason-code plumbing
- risk-driven discard recommendations
- deterministic paper-only execution by default
- live executor dry-run and stale-signal guards
- max-fee and RPC-budget checks before guarded live submission
- tentative shred events cannot directly escalate into paper/live entries
- tentative early-intent sell events may trigger paper emergency exits when confidence and impact are high enough, but they still never become canonical state by themselves
- live shred-triggered exits remain disabled by default
- persisted shred-exit calibration is paper-only by default and does not adapt guarded-live thresholds unless explicitly enabled
- mock early-intent sources are allowed in paper research mode but forbidden in guarded live mode
- replay correctness is treated as a live-safety prerequisite: mocked early-intent paper runs must reproduce tentative warnings, emergency exits, and fills from stored events before any provider-backed early-intent source is trusted for future guarded-live work
- deshred/pre-execution transactions are tentative intent only; they never bypass Geyser/account-effect confirmation and they do not imply production raw shred support
- `smoke-deshred-provider` is an environment-gated paper/data-collection tool only; it may prove that a provider can or cannot stream deshred, but it does not loosen guarded-live restrictions
- `provider-env-precheck` is a zero-network readiness check only; it validates the environment without attempting any stream connection or requiring any signer material
- `smoke-geyser-provider`, `smoke-streams`, and `provider-dry-run` are environment-gated paper/data-collection tools only; they never require a signer, never send live orders, and do not mutate guarded-live enablement
- `smoke-multisource-early-intent`, `collect-live-paper`, and `collect-first-live-paper` are also paper/data-collection commands only; they never require a signer, never send live orders, and do not mutate guarded-live enablement
- `run-autopilot` is also paper/data-collection only in this build; it never enables live trading, never sends orders, never requires signer material, and stops or pauses if stream-only proof fails or nonzero RPC usage is detected
- stream-only mode prevents accidental eRPC/JSON-RPC spend for market data, holder scans, metadata fetch, confirmation fallback, reconciliation fallback, blockhash fallback, and send paths in the default profile
- denied RPC attempts are persisted in the run’s RPC ledger instead of being silently dropped
- provider compatibility records intentionally hash endpoints and exclude auth tokens so operational readiness tracking does not leak secrets
- R2 credentials, provider auth tokens, and VPS credentials are env-only inputs and must never be committed, rendered into reports, or embedded in generated service units
- R2 offload never removes local files before a verified upload when `prune_local_only_after_verified_upload=true`
- `smoke-r2-upload`, `env-bootstrap-check`, `r2_upload_audit`, and `local_prune_audit` exist so the operator can prove R2 readiness and cleanup safety without exposing secrets
- `disk-preflight`, `verified_prune_report`, and `autopilot-recover-storage` exist so no-space incidents can be handled without touching unverified data
- `finalize-run-from-local-artifacts` exists so a completed-but-unuploaded run can be recovered without fabricating success or rewriting source event logs
- `segment_manifest.json` is allowed to prove that remote verified segments exist, but verified status must be explicit before any local segment file is deleted
- destructive R2 bucket/prefix operations are disabled by default, dry-run-first, scoped to managed buckets/prefixes, and require explicit confirmation when enabled
- if credentials were exposed outside a secure env file, rotate them immediately and do not preserve the old values anywhere in the repo or reports
- missing-endpoint reports are allowed for smoke/dry-run collection tooling, but they must say the provider was not attempted instead of implying a successful validation
- every provider smoke, collection, quality, readiness, and research-cycle artifact now carries explicit stream-only proof so the operator can verify zero market-data RPC spend after the fact
- autopilot reports carry the same stream-only proof block, plus alert/state history, so a hands-off daemon cannot quietly spend RPC credits or hide provider failure states
- autopilot disk guardrails prevent new collection cycles from starting when free space is below the configured minimum, and emergency prune is limited to verified artifacts only
- low-disk mode may delete verified closed segments and verified export chunks immediately, but it must never delete open segments or unverified data
- remote-first analysis is allowed to stream verified remote segments one at a time; it must not silently treat missing unverified segments as available
- low-disk report-lite mode may replace a large local report with a small pointer stub only after the full remote report has been uploaded and verified
- systemd deployment artifacts intentionally keep endpoint/auth values in a separate env file and never embed secrets in the generated unit
- release-binary service mode reduces VPS build cache pressure by running `/target/release/cli` instead of keeping a `cargo run` service warm
- Geyser-only fallback does not fabricate shred-edge features
- paper mode cannot send live orders
- token-scoped data gaps block only the affected mint; only true global data loss blocks all trading
- metadata-rich reports and exports are research aids only; they do not imply provider-backed deshred support, production raw shred decoding, or live readiness

## Still not production-ready

- the live executor is a guarded foundation, not a production trading path
- the shred decoder is fixture-backed unless a real Solana shred backend is linked
- production raw shred decoding still fails closed in this build
- stream-only does not make live execution ready; it only constrains the data plane to streams and local state
- provider-backed deshred is only as good as the configured endpoint: build support does not guarantee that every provider implements `SubscribeDeshred`, and unsupported endpoints must fail or skip honestly
- mocked live-data paper is the supported no-network test path
- the real network Geyser adapter now constructs and consumes a Yellowstone subscribe stream, but this remains a paper/data-collection path rather than a production trading claim
- autopilot continuous mode is an unattended paper collector, not a production execution daemon
- advanced feature families and forecasting models remain explicitly unavailable rather than partially inferred
- low-disk mode is not the same as storage abundance: a run can finalize honestly and still fail the next-cycle preflight if the VPS cannot recover enough free space after verified cleanup

## Future safeguards

- global and strategy kill switches
- stale-data kill switch
- max loss limits
- position and fee caps
- confirmation and reconciliation checks
- trade audit logs
