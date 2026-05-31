# Material Candidate Dataset Policy

Phase 107B separates live hunter evidence from strategy datasets.

## Policy

- Rejected or dead tokens are retained as compact tombstone/audit records only.
- Promoted material candidates receive richer candidate artifacts.
- Rejection tombstones are still retained for calibration, false-positive analysis, and future negative-label joins.
- Candidate datasets must not silently drop all negatives when formal modelling begins.
- Holder count, top-holder, dev holdings, holder growth/churn/stickiness, and paperhand metrics remain stream-authoritative.
- RPC holder repair is audit-only and disabled by default.
- RPC mint supply is diagnostic-only and must not be canonical.
- Same-slot bundle-like evidence is a stream proxy and must not be treated as provider-confirmed.
- No binary malicious labels are emitted.
- This phase does not tune thresholds, train models, enable trading, or claim alpha.

## Promotion Boundary

A token may be promoted only after it survives early death/rug-like gates with time-safety intact. A rejected token must not be written into the material candidate dataset, but its tombstone remains available for later calibration.

