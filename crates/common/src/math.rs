use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Lamports(pub u64);

impl Lamports {
    pub fn saturating_add(self, rhs: Lamports) -> Lamports {
        Lamports(self.0.saturating_add(rhs.0))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct QuoteAmount(pub Decimal);

pub fn bps_to_decimal(bps: u64) -> Decimal {
    Decimal::from(bps) / Decimal::from(10_000u64)
}
