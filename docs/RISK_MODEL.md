# Risk Model

Risk scoring is now implemented as a transparent evidence layer on top of state and feature snapshots.

## Implemented engines

- `RugRiskEngine`
- `BundleRiskEngine`
- `DevRiskEngine`
- `TopHolderRiskEngine`
- `FakeMomentumRiskEngine`
- `DataQualityRiskEngine`

Each score outputs severity, confidence, reason codes, positive/negative evidence, missing-data penalties, and a recommended lifecycle action.

Current scoring now consumes advanced computed features such as:

- cohort leadership and rank velocity
- profit overhang and free-roll dump pressure
- absorption success/failure
- momentum authenticity and fake-momentum evidence
- holder stickiness
- funding graph suspicion
- fee-war pressure
- transaction fingerprint concentration
- token-scoped versus global data-gap state

Discard outputs are conservative and explicit:

- `Keep`
- `SoftDiscard`
- `HardDiscard`
- `RugArchive`
- `ResearchSample`
- `DataGapStop`

Phase 14 calibration changes:

- large sells are no longer treated as automatic rug evidence if absorption and stickiness recover
- Geyser-only runs are penalized through missing confidence, not hard-blocked by default
- token-scoped data gaps block only the affected mint
- 70% collapse fixtures now escalate to `HardDiscard` or `RugArchive` through collapse evidence instead of lingering in soft-discard
- bundle evidence is surfaced early enough to block default `EnterPaper` decisions without forcing every bundle-like token into rug status

Phase 16 adds source-health and readiness implications around that same model:

- source-scoped Geyser gaps mark readiness false and emit recoverable risk pressure instead of fabricating stale safety
- global canonical queue overflow still escalates to a global data-gap stop
- expected executable edge must remain positive after fees and impact before paper entry is allowed

Phase 17 adds early malicious-sell risk integration:

- dev/top-holder/bundle/whale tentative sells can raise `ShredSellImpactHigh` before canonical confirmation
- tentative false positives are tracked and fed back into paper-only calibration
- reorg, mismatch, and account-effect-confirmed outcomes are recorded explicitly instead of silently disappearing

Phase 18 sharpens that layer:

- too-late tentative signals can be downgraded when latency advantage is insufficient
- strong absorption can reduce emergency severity for non-catastrophic sellers
- persisted paper-only calibration tracks false-positive buckets, saved-loss buckets, and threshold adjustments without adapting guarded-live thresholds by default

Phase 21 extends calibration and confidence by source:

- deshred false-positive and saved-loss buckets are tracked separately from fixture/mock/raw-shred buckets
- source-specific confidence stays paper-only by default and does not imply guarded-live readiness

Phase 22 adds source-precedence and provider-readiness context:

- duplicate tentative sources are suppressed before they can double-trigger exits, while lower-priority evidence is still exported for auditability
- provider smoke and compatibility results are operational observations only; they do not override the core rule that Geyser/account effects remain canonical truth

Phase 23 adds operator-readiness wrappers without changing those core assumptions:

- provider dry-run and collection presets report whether deshred was available, unsupported, or simply absent in the environment, but they do not turn that operational status into extra risk confidence by themselves
