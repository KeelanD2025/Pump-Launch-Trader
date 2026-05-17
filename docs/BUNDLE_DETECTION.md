# Bundle Detection

Bundle detection is not claimed to be exact. The architecture is designed around evidence accumulation, confidence, and uncertainty rather than perfect labels.

Implemented bundle evidence currently combines:

- first-buy timing density
- repeated quote sizing
- shared-funder relationships from the observed funding graph
- bundle-holder concentration among early buyers
- repeated compute-budget and client-fingerprint patterns among first buyers

This is intentionally probabilistic. Missing fingerprint families are still marked unavailable rather than inferred from incomplete data.
