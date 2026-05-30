# Rug Label Collection Requirements

Phase 105 policy for post-event rug/death labels.

These requirements are for label validation and later diagnostic dataset collection only. They are not production thresholds, trading rules, or maliciousness labels.

## Required observation windows

Every tracked fresh launch used for rug/death outcome labels must retain stream-local evidence for:

- 60 seconds after launch
- 180 seconds after launch
- 300 seconds after launch
- 900 seconds after launch

An 1800 second canary window is sufficient only if the artifacts retain the full price, curve, holder, and flow paths for the full window. A run that observes for 1800 seconds but only writes summary rows is not sufficient for post-event label validation.

## Required retained paths

Each run must retain:

- Normalized events for the tracked mint.
- Reserve-implied and realized trade price path.
- BondingCurve reserve and curve-progress path.
- Buy/sell flow path, including timestamps and no-buy gaps.
- Stream owner-summed holder path.
- Dev and top-holder balance/dump evidence from stream holder state.
- Post-event labels with explicit unavailable reasons.

## Label rules

- Rug/death labels are post-event labels only.
- Manual Dexscreener/user observations are calibration evidence only.
- Missing price, curve, holder, or flow evidence is unavailable or inconclusive, never zero.
- No binary malicious labels are emitted.
- Provider-confirmed bundle evidence remains false unless provider/API evidence exists.

## Before a small controlled dataset

Before starting a small controlled dataset, the pipeline must either:

- Reproduce manual rug/death outcomes from retained stream evidence, or
- Record explicit artifact/horizon blockers and implement the retention requirements above for new collection.

