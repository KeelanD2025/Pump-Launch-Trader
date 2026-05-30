use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
pub const DEFAULT_PUMP_TOKEN_DECIMALS: u8 = 6;
pub const PUMP_TOTAL_SUPPLY_UI: u64 = 1_000_000_000;
pub const PUMP_RESERVED_TOKENS_UI: u64 = 206_900_000;
pub const PUMP_INITIAL_REAL_TOKEN_RESERVES_UI: u64 = 793_100_000;

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

pub fn decimal_pow10(decimals: u8) -> Decimal {
    Decimal::from(10u64.pow(u32::from(decimals)))
}

pub fn raw_tokens_to_ui(raw: Decimal, decimals: u8) -> Decimal {
    raw / decimal_pow10(decimals)
}

pub fn ui_tokens_to_raw(ui: Decimal, decimals: u8) -> Decimal {
    ui * decimal_pow10(decimals)
}

pub fn lamports_to_sol(lamports: Decimal) -> Decimal {
    lamports / Decimal::from(LAMPORTS_PER_SOL)
}

pub fn sol_to_lamports(sol: Decimal) -> Decimal {
    sol * Decimal::from(LAMPORTS_PER_SOL)
}

pub fn price_lamports_per_raw_token(
    quote_lamports: Decimal,
    raw_tokens: Decimal,
) -> Option<Decimal> {
    (quote_lamports > Decimal::ZERO && raw_tokens > Decimal::ZERO)
        .then(|| quote_lamports / raw_tokens)
}

pub fn price_sol_per_ui_token(
    quote_lamports: Decimal,
    raw_tokens: Decimal,
    decimals: u8,
) -> Option<Decimal> {
    let quote_sol = lamports_to_sol(quote_lamports);
    let tokens_ui = raw_tokens_to_ui(raw_tokens, decimals);
    (quote_sol > Decimal::ZERO && tokens_ui > Decimal::ZERO).then(|| quote_sol / tokens_ui)
}

pub fn pump_virtual_reserve_price_sol_per_token(
    virtual_quote_lamports: Decimal,
    virtual_token_raw: Decimal,
    decimals: u8,
) -> Option<Decimal> {
    price_sol_per_ui_token(virtual_quote_lamports, virtual_token_raw, decimals)
}

pub fn pump_market_cap_quote_1b(price_quote_per_token: Decimal) -> Decimal {
    price_quote_per_token * Decimal::from(PUMP_TOTAL_SUPPLY_UI)
}

pub fn pump_market_cap_quote_total_supply(
    price_quote_per_token: Decimal,
    curve_economic_supply_ui: Decimal,
) -> Decimal {
    price_quote_per_token * curve_economic_supply_ui
}

pub fn pump_curve_progress_pct_from_real_token_reserves_ui(balance_ui: Decimal) -> Option<Decimal> {
    let reserved = Decimal::from(PUMP_RESERVED_TOKENS_UI);
    let initial = Decimal::from(PUMP_INITIAL_REAL_TOKEN_RESERVES_UI);
    if initial <= Decimal::ZERO {
        return None;
    }
    let remaining_above_reserved = (balance_ui - reserved).max(Decimal::ZERO);
    Some(
        (Decimal::from(100u64) - (remaining_above_reserved * Decimal::from(100u64) / initial))
            .clamp(Decimal::ZERO, Decimal::from(100u64)),
    )
}

pub fn pump_curve_progress_pct_from_real_token_reserves_raw(
    real_token_reserves_raw: Decimal,
    decimals: u8,
) -> Option<Decimal> {
    pump_curve_progress_pct_from_real_token_reserves_ui(raw_tokens_to_ui(
        real_token_reserves_raw,
        decimals,
    ))
}

pub fn fill_price_to_reserve_ratio(fill_price: Decimal, reserve_price: Decimal) -> Option<Decimal> {
    (fill_price > Decimal::ZERO && reserve_price > Decimal::ZERO)
        .then(|| fill_price / reserve_price)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_conversions_do_not_drift_by_1e9() {
        assert_eq!(
            lamports_to_sol(Decimal::from(LAMPORTS_PER_SOL)),
            Decimal::ONE
        );
        assert_eq!(
            sol_to_lamports(Decimal::ONE),
            Decimal::from(LAMPORTS_PER_SOL)
        );
        assert_eq!(
            raw_tokens_to_ui(Decimal::from(1_000_000u64), 6),
            Decimal::ONE
        );
        assert_eq!(
            ui_tokens_to_raw(Decimal::ONE, 6),
            Decimal::from(1_000_000u64)
        );
    }

    #[test]
    fn virtual_reserve_price_uses_matching_pair() {
        let price = pump_virtual_reserve_price_sol_per_token(
            Decimal::from(30_000_000_000u64),
            Decimal::from(1_000_000_000_000_000u64),
            6,
        )
        .expect("price");
        assert_eq!(price, Decimal::new(3, 8));
    }

    #[test]
    fn market_cap_uses_price_times_supply_not_reserves() {
        let price = Decimal::new(3, 5);
        assert_eq!(pump_market_cap_quote_1b(price), Decimal::from(30_000u64));
        assert_eq!(
            pump_market_cap_quote_total_supply(price, Decimal::from(2_000_000u64)),
            Decimal::from(60u64)
        );
    }

    #[test]
    fn curve_progress_handles_zero_half_and_complete() {
        assert_eq!(
            pump_curve_progress_pct_from_real_token_reserves_ui(Decimal::from(
                PUMP_INITIAL_REAL_TOKEN_RESERVES_UI + PUMP_RESERVED_TOKENS_UI
            )),
            Some(Decimal::ZERO)
        );
        assert_eq!(
            pump_curve_progress_pct_from_real_token_reserves_ui(
                Decimal::from(PUMP_RESERVED_TOKENS_UI)
                    + Decimal::from(PUMP_INITIAL_REAL_TOKEN_RESERVES_UI) / Decimal::from(2u64)
            ),
            Some(Decimal::from(50u64))
        );
        assert_eq!(
            pump_curve_progress_pct_from_real_token_reserves_ui(Decimal::from(
                PUMP_RESERVED_TOKENS_UI
            )),
            Some(Decimal::from(100u64))
        );
    }
}
