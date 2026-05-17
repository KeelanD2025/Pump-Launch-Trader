# Fee Model

Gross returns are not acceptable for trading decisions in this project.

The current fee model includes:

- base fee per signature
- compute-unit-derived priority fee
- optional tips
- optional ATA creation/rent inputs
- protocol/platform fee bps
- curve impact and slippage
- failed transaction drag

The simulator uses these fees in single-order fills, round-trip paper trades, and label generation.
