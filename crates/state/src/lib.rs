use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    fmt,
};

use common::{
    BondingCurveUpdateEvent, Canonicality, DEFAULT_PUMP_TOKEN_DECIMALS,
    DangerousSellerClassification, DataGapEvent, EarlyIntentSource, EventPayload, EventSource,
    HolderBalanceUpdateEvent, NormalizedEvent, PUMP_TOTAL_SUPPLY_UI, PubkeyValue, PumpBuyEvent,
    PumpSellEvent, QuoteAssetType, ReasonCode, ShredEmergencyExitArmedEvent,
    ShredEmergencyExitTriggeredEvent, ShredSellIntentResolvedEvent,
    TentativeMaliciousSellWarningEvent, TentativeSellConfirmationState,
    TentativeSellIntentDetectedEvent, TentativeSellResolutionOutcome, TentativeSellRiskLevel,
    TokenProgramType, TtlConfig, WalletFundingEvent, price_lamports_per_raw_token,
    price_sol_per_ui_token, pump_curve_progress_pct_from_real_token_reserves_raw,
    pump_market_cap_quote_1b, pump_market_cap_quote_total_supply,
    pump_virtual_reserve_price_sol_per_token, raw_tokens_to_ui,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::OffsetDateTime;

const MAX_PRICE_HISTORY: usize = 512;
const MAX_TRADE_HISTORY: usize = 512;
const MAX_HOLDER_HISTORY: usize = 256;

#[derive(Debug, Error)]
pub enum StateError {
    #[error("invalid state update: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenLifecycle {
    Discovered,
    FirstPass,
    ActiveLight,
    ActiveDeep,
    TradeCandidate,
    InPosition,
    ExitPending,
    Completed,
    SoftDiscarded,
    HardDiscarded,
    RugArchive,
    Migrated,
    DataGap,
}

impl fmt::Display for TokenLifecycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Discovered => "discovered",
            Self::FirstPass => "first_pass",
            Self::ActiveLight => "active_light",
            Self::ActiveDeep => "active_deep",
            Self::TradeCandidate => "trade_candidate",
            Self::InPosition => "in_position",
            Self::ExitPending => "exit_pending",
            Self::Completed => "completed",
            Self::SoftDiscarded => "soft_discarded",
            Self::HardDiscarded => "hard_discarded",
            Self::RugArchive => "rug_archive",
            Self::Migrated => "migrated",
            Self::DataGap => "data_gap",
        };
        f.write_str(label)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleTransition {
    pub mint: PubkeyValue,
    pub from: TokenLifecycle,
    pub to: TokenLifecycle,
    pub reason: String,
    pub observed_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BondingCurveState {
    pub virtual_quote_reserves: Decimal,
    pub virtual_token_reserves: Decimal,
    pub real_quote_reserves: Decimal,
    pub real_token_reserves: Decimal,
    pub token_decimals: u8,
    pub price_lamports_per_raw_token: Option<Decimal>,
    pub price_sol_per_token: Option<Decimal>,
    pub reserve_price_source: Option<String>,
    pub reserve_price_confidence: Decimal,
    pub latest_price: Decimal,
    pub launch_price: Option<Decimal>,
    pub market_cap_quote_1b: Option<Decimal>,
    pub market_cap_quote_total_supply: Option<Decimal>,
    pub market_cap_source: Option<String>,
    pub market_cap_confidence: Decimal,
    pub curve_complete_flag: Option<bool>,
    pub curve_progress_pct: Option<Decimal>,
    pub curve_progress_source: Option<String>,
    pub curve_progress_confidence: Decimal,
    pub curve_completion_pct: Option<Decimal>,
    pub market_cap_proxy: Option<Decimal>,
    pub update_slot: u64,
    pub update_write_version: u64,
    pub last_updated_at: Option<OffsetDateTime>,
    pub account_update_confidence: Decimal,
    pub quote_asset_type: QuoteAssetType,
}

impl Default for BondingCurveState {
    fn default() -> Self {
        Self {
            virtual_quote_reserves: Decimal::ZERO,
            virtual_token_reserves: Decimal::ZERO,
            real_quote_reserves: Decimal::ZERO,
            real_token_reserves: Decimal::ZERO,
            token_decimals: DEFAULT_PUMP_TOKEN_DECIMALS,
            price_lamports_per_raw_token: None,
            price_sol_per_token: None,
            reserve_price_source: None,
            reserve_price_confidence: Decimal::ZERO,
            latest_price: Decimal::ZERO,
            launch_price: None,
            market_cap_quote_1b: None,
            market_cap_quote_total_supply: None,
            market_cap_source: None,
            market_cap_confidence: Decimal::ZERO,
            curve_complete_flag: None,
            curve_progress_pct: None,
            curve_progress_source: None,
            curve_progress_confidence: Decimal::ZERO,
            curve_completion_pct: None,
            market_cap_proxy: None,
            update_slot: 0,
            update_write_version: 0,
            last_updated_at: None,
            account_update_confidence: Decimal::ZERO,
            quote_asset_type: QuoteAssetType::Unknown,
        }
    }
}

impl BondingCurveState {
    pub fn apply_update(&mut self, update: &BondingCurveUpdateEvent, slot: u64) -> bool {
        let write_version = update.account_write_version.unwrap_or_default();
        if slot < self.update_slot
            || (slot == self.update_slot && write_version < self.update_write_version)
        {
            return false;
        }
        self.virtual_quote_reserves = update.virtual_quote_reserves;
        self.virtual_token_reserves = update.virtual_token_reserves;
        self.real_quote_reserves = update.real_quote_reserves;
        self.real_token_reserves = update.real_token_reserves;
        self.token_decimals = update.token_decimals.unwrap_or(DEFAULT_PUMP_TOKEN_DECIMALS);
        self.price_lamports_per_raw_token = update.price_lamports_per_raw_token.or_else(|| {
            price_lamports_per_raw_token(self.virtual_quote_reserves, self.virtual_token_reserves)
        });
        self.price_sol_per_token = update.price_sol_per_token.or_else(|| {
            pump_virtual_reserve_price_sol_per_token(
                self.virtual_quote_reserves,
                self.virtual_token_reserves,
                self.token_decimals,
            )
        });
        self.reserve_price_source = update
            .reserve_price_source
            .clone()
            .or_else(|| Some("virtual_reserves".to_owned()));
        self.reserve_price_confidence = update.reserve_price_confidence.unwrap_or_else(|| {
            if self.price_sol_per_token.is_some() {
                Decimal::ONE
            } else {
                Decimal::ZERO
            }
        });
        self.latest_price = self.price_sol_per_token.unwrap_or(update.price);
        self.launch_price.get_or_insert(self.latest_price);
        self.market_cap_quote_1b = update
            .market_cap_quote_1b
            .or_else(|| self.price_sol_per_token.map(pump_market_cap_quote_1b));
        self.market_cap_quote_total_supply = update.market_cap_quote_total_supply.or_else(|| {
            self.price_sol_per_token.map(|price| {
                pump_market_cap_quote_total_supply(price, Decimal::from(PUMP_TOTAL_SUPPLY_UI))
            })
        });
        self.market_cap_source = update
            .market_cap_source
            .clone()
            .or_else(|| Some("price_times_supply".to_owned()));
        self.market_cap_confidence = update.market_cap_confidence.unwrap_or_else(|| {
            if self.market_cap_quote_1b.is_some() {
                Decimal::ONE
            } else {
                Decimal::ZERO
            }
        });
        self.curve_complete_flag = update.curve_complete_flag;
        self.curve_progress_pct = update.curve_progress_pct.or_else(|| {
            pump_curve_progress_pct_from_real_token_reserves_raw(
                self.real_token_reserves,
                self.token_decimals,
            )
        });
        self.curve_progress_source = update
            .curve_progress_source
            .clone()
            .or_else(|| Some("real_token_reserves_ui_minus_reserved".to_owned()));
        self.curve_progress_confidence = update.curve_progress_confidence.unwrap_or_else(|| {
            if self.curve_progress_pct.is_some() {
                Decimal::ONE
            } else {
                Decimal::ZERO
            }
        });
        self.curve_completion_pct = self.curve_progress_pct.or(update.curve_completion_pct);
        self.market_cap_proxy = update.market_cap_proxy;
        self.update_slot = slot;
        self.update_write_version = write_version;
        true
    }

    pub fn staleness_ms(&self, now: OffsetDateTime) -> Option<i64> {
        self.last_updated_at
            .map(|updated_at| (now - updated_at).whole_milliseconds() as i64)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CostBasisPosition {
    pub estimated_cost_basis_quote: Decimal,
    pub estimated_average_entry_price: Decimal,
    pub estimated_realized_pnl: Decimal,
    pub estimated_unrealized_pnl: Decimal,
    pub remaining_position_size: Decimal,
    pub original_position_size: Decimal,
    pub original_quote_spent: Decimal,
    pub position_opened_at: Option<OffsetDateTime>,
    pub last_updated_at: Option<OffsetDateTime>,
    pub has_taken_profit: bool,
    pub has_round_tripped: bool,
}

impl CostBasisPosition {
    pub fn apply_buy(
        &mut self,
        quote_spent: Decimal,
        token_out: Decimal,
        timestamp: OffsetDateTime,
        latest_price: Decimal,
    ) {
        if self.position_opened_at.is_none() {
            self.position_opened_at = Some(timestamp);
        }
        self.original_quote_spent += quote_spent;
        self.original_position_size += token_out;
        self.estimated_cost_basis_quote += quote_spent;
        self.remaining_position_size += token_out;
        if self.remaining_position_size > Decimal::ZERO {
            self.estimated_average_entry_price =
                self.estimated_cost_basis_quote / self.remaining_position_size;
        }
        self.estimated_unrealized_pnl =
            latest_price * self.remaining_position_size - self.estimated_cost_basis_quote;
        self.last_updated_at = Some(timestamp);
    }

    pub fn apply_sell(
        &mut self,
        quote_out: Decimal,
        token_in: Decimal,
        timestamp: OffsetDateTime,
        latest_price: Decimal,
    ) {
        if self.remaining_position_size <= Decimal::ZERO {
            self.has_round_tripped = true;
            self.last_updated_at = Some(timestamp);
            return;
        }
        let token_sold = token_in.min(self.remaining_position_size);
        let average_cost = if self.remaining_position_size > Decimal::ZERO {
            self.estimated_cost_basis_quote / self.remaining_position_size
        } else {
            Decimal::ZERO
        };
        let cost_removed = average_cost * token_sold;
        self.remaining_position_size -= token_sold;
        self.estimated_cost_basis_quote -= cost_removed;
        self.estimated_realized_pnl += quote_out - cost_removed;
        self.has_taken_profit |= quote_out > cost_removed;
        if self.remaining_position_size <= Decimal::ZERO {
            self.remaining_position_size = Decimal::ZERO;
            self.estimated_cost_basis_quote = Decimal::ZERO;
            self.has_round_tripped = true;
        }
        self.estimated_average_entry_price = if self.remaining_position_size > Decimal::ZERO {
            self.estimated_cost_basis_quote / self.remaining_position_size
        } else {
            Decimal::ZERO
        };
        self.estimated_unrealized_pnl =
            latest_price * self.remaining_position_size - self.estimated_cost_basis_quote;
        self.last_updated_at = Some(timestamp);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HolderRetentionStatus {
    Active,
    ReducedPosition,
    Sold90Pct,
    ExitedZeroBalance,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HolderBalance {
    pub balance: Decimal,
    #[serde(default)]
    pub ui_balance_sum: Decimal,
    #[serde(default)]
    pub account_count: usize,
    #[serde(default)]
    pub excluded_reason: Option<String>,
    pub last_updated_at: Option<OffsetDateTime>,
    pub first_seen_at: Option<OffsetDateTime>,
    pub last_trade_at: Option<OffsetDateTime>,
    pub cost_basis: CostBasisPosition,
    #[serde(default)]
    pub wallet_sell_through_pct: Decimal,
    #[serde(default)]
    pub wallet_sold_90pct_flag: bool,
    #[serde(default)]
    pub holder_retention_status: HolderRetentionStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenAccountState {
    pub mint: PubkeyValue,
    pub token_account: PubkeyValue,
    pub owner: PubkeyValue,
    pub raw_balance: Decimal,
    pub ui_balance: Decimal,
    pub decimals: u8,
    pub last_slot: u64,
    pub last_sequence: Option<u64>,
    pub source: EventSource,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct HolderUpdateCounters {
    pub holder_updates_seen: u64,
    pub holder_updates_applied: u64,
    pub holder_updates_deduped: u64,
    pub holder_owner_changes: u64,
    pub holder_missing_owner_mapping: u64,
    pub holder_fallback_trade_updates_used: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HolderDistributionSnapshot {
    pub owner: PubkeyValue,
    pub balance: Decimal,
    pub pct_supply_proxy: Decimal,
}

impl Default for HolderDistributionSnapshot {
    fn default() -> Self {
        Self {
            owner: PubkeyValue(String::new()),
            balance: Decimal::ZERO,
            pct_supply_proxy: Decimal::ZERO,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HolderState {
    #[serde(default)]
    pub token_accounts: HashMap<String, TokenAccountState>,
    pub token_account_to_owner: HashMap<String, PubkeyValue>,
    pub token_account_balances: HashMap<String, Decimal>,
    pub owner_balances: HashMap<String, HolderBalance>,
    #[serde(default)]
    pub applied_holder_update_keys: HashSet<String>,
    #[serde(default)]
    pub counters: HolderUpdateCounters,
    pub nonzero_holder_count: usize,
    pub top_holders: Vec<HolderDistributionSnapshot>,
    pub gini: Decimal,
    pub hhi: Decimal,
    #[serde(default)]
    pub paperhand_90pct_wallet_count: usize,
    #[serde(default)]
    pub exited_holder_count_zero_balance: usize,
    #[serde(default)]
    pub net_holder_change: i64,
    #[serde(default)]
    pub holder_churn_rate: Decimal,
    #[serde(default)]
    pub holder_stickiness: Decimal,
    pub holder_count_history: VecDeque<(OffsetDateTime, usize)>,
    pub last_updated_at: Option<OffsetDateTime>,
}

impl HolderState {
    pub fn apply_balance_update(
        &mut self,
        update: &HolderBalanceUpdateEvent,
        event: &NormalizedEvent,
        token_decimals: u8,
    ) {
        self.counters.holder_updates_seen += 1;
        let observed_at = event.meta.received_at_wall_time;
        let dedupe_key = holder_update_dedupe_key(update, event);
        if !self.applied_holder_update_keys.insert(dedupe_key) {
            self.counters.holder_updates_deduped += 1;
            return;
        }

        let token_account_key = update.token_account.0.clone();
        let new_owner = update.owner_wallet.clone();
        let new_balance = update.new_balance.max(Decimal::ZERO);
        let old_state = self.token_accounts.get(&token_account_key).cloned();
        let old_owner = old_state
            .as_ref()
            .map(|state| state.owner.clone())
            .or_else(|| self.token_account_to_owner.get(&token_account_key).cloned());
        let old_balance = old_state
            .as_ref()
            .map(|state| state.raw_balance)
            .or_else(|| self.token_account_balances.get(&token_account_key).copied())
            .unwrap_or(Decimal::ZERO);

        match old_owner {
            Some(owner) if owner != new_owner => {
                self.counters.holder_owner_changes += 1;
                self.apply_owner_delta(&owner, -old_balance, token_decimals, observed_at);
                self.apply_owner_delta(&new_owner, new_balance, token_decimals, observed_at);
            }
            Some(owner) => {
                self.apply_owner_delta(
                    &owner,
                    new_balance - old_balance,
                    token_decimals,
                    observed_at,
                );
            }
            None => {
                self.apply_owner_delta(&new_owner, new_balance, token_decimals, observed_at);
            }
        }

        self.token_account_to_owner
            .insert(token_account_key.clone(), new_owner.clone());
        self.token_account_balances
            .insert(token_account_key.clone(), new_balance);
        self.token_accounts.insert(
            token_account_key.clone(),
            TokenAccountState {
                mint: update.mint.clone(),
                token_account: update.token_account.clone(),
                owner: new_owner,
                raw_balance: new_balance,
                ui_balance: raw_tokens_to_ui(new_balance, token_decimals),
                decimals: token_decimals,
                last_slot: event.meta.slot,
                last_sequence: event
                    .meta
                    .account_write_version
                    .or_else(|| event.meta.transaction_index.map(u64::from)),
                source: event.meta.source,
            },
        );
        if new_balance <= Decimal::ZERO {
            self.token_account_balances.remove(&token_account_key);
            self.token_accounts.remove(&token_account_key);
        }
        self.counters.holder_updates_applied += 1;
        self.last_updated_at = Some(observed_at);
        self.rebuild_owner_balances_from_token_accounts(observed_at, token_decimals);
    }

    fn apply_owner_delta(
        &mut self,
        owner: &PubkeyValue,
        delta_raw: Decimal,
        decimals: u8,
        observed_at: OffsetDateTime,
    ) {
        if delta_raw == Decimal::ZERO && self.owner_balances.contains_key(&owner.0) {
            return;
        }
        let entry = self.owner_balances.entry(owner.0.clone()).or_default();
        if entry.first_seen_at.is_none() {
            entry.first_seen_at = Some(observed_at);
        }
        entry.balance += delta_raw;
        entry.ui_balance_sum = raw_tokens_to_ui(entry.balance.max(Decimal::ZERO), decimals);
        entry.last_updated_at = Some(observed_at);
        entry.last_trade_at = Some(observed_at);
        entry.account_count = self
            .token_accounts
            .values()
            .filter(|account| account.owner == *owner && account.raw_balance > Decimal::ZERO)
            .count();
        if delta_raw > Decimal::ZERO {
            entry.account_count += 1;
        }
        if entry.balance <= Decimal::ZERO {
            self.owner_balances.remove(&owner.0);
        }
    }

    pub fn apply_trade_cost_basis(
        &mut self,
        owner: &PubkeyValue,
        is_buy: bool,
        quote: Decimal,
        tokens: Decimal,
        observed_at: OffsetDateTime,
        latest_price: Decimal,
    ) {
        let entry = self.owner_balances.entry(owner.0.clone()).or_default();
        if entry.first_seen_at.is_none() {
            entry.first_seen_at = Some(observed_at);
        }
        entry.last_trade_at = Some(observed_at);
        if is_buy {
            entry
                .cost_basis
                .apply_buy(quote, tokens, observed_at, latest_price);
        } else {
            entry
                .cost_basis
                .apply_sell(quote, tokens, observed_at, latest_price);
        }
        Self::refresh_holder_behaviour(entry);
    }

    pub fn rebuild_owner_balances_from_token_accounts(
        &mut self,
        observed_at: OffsetDateTime,
        decimals: u8,
    ) {
        let previous_holder_count = self.nonzero_holder_count;
        let mut rebuilt = HashMap::<String, HolderBalance>::new();
        for account in self.token_accounts.values() {
            if account.raw_balance > Decimal::ZERO {
                let old = self.owner_balances.get(&account.owner.0);
                let entry =
                    rebuilt
                        .entry(account.owner.0.clone())
                        .or_insert_with(|| HolderBalance {
                            first_seen_at: old.and_then(|holder| holder.first_seen_at),
                            last_trade_at: old.and_then(|holder| holder.last_trade_at),
                            cost_basis: old
                                .map(|holder| holder.cost_basis.clone())
                                .unwrap_or_default(),
                            excluded_reason: old.and_then(|holder| holder.excluded_reason.clone()),
                            ..HolderBalance::default()
                        });
                if entry.first_seen_at.is_none() {
                    entry.first_seen_at = account
                        .last_sequence
                        .map(|_| observed_at)
                        .or(Some(observed_at));
                }
                entry.balance += account.raw_balance;
                entry.ui_balance_sum += if account.ui_balance > Decimal::ZERO {
                    account.ui_balance
                } else {
                    raw_tokens_to_ui(account.raw_balance, decimals)
                };
                entry.account_count += 1;
                entry.last_updated_at = Some(observed_at);
            }
        }

        for (owner, old) in &self.owner_balances {
            if let Some(new_holder) = rebuilt.get_mut(owner) {
                if new_holder.first_seen_at.is_none() {
                    new_holder.first_seen_at = old.first_seen_at;
                }
                if new_holder.last_trade_at.is_none() {
                    new_holder.last_trade_at = old.last_trade_at;
                }
                if new_holder.cost_basis.original_position_size <= Decimal::ZERO {
                    new_holder.cost_basis = old.cost_basis.clone();
                }
                new_holder.wallet_sell_through_pct = old.wallet_sell_through_pct;
                new_holder.wallet_sold_90pct_flag = old.wallet_sold_90pct_flag;
                new_holder.holder_retention_status = old.holder_retention_status.clone();
            }
        }

        self.owner_balances = rebuilt;
        for holder in self.owner_balances.values_mut() {
            Self::refresh_holder_behaviour(holder);
        }
        self.recompute_distribution_with_previous(observed_at, previous_holder_count);
    }

    pub fn recompute_distribution(&mut self, observed_at: OffsetDateTime) {
        let previous_holder_count = self.nonzero_holder_count;
        self.recompute_distribution_with_previous(observed_at, previous_holder_count);
    }

    fn recompute_distribution_with_previous(
        &mut self,
        observed_at: OffsetDateTime,
        previous_holder_count: usize,
    ) {
        self.nonzero_holder_count = self
            .owner_balances
            .values()
            .filter(|holder| holder.balance > Decimal::ZERO)
            .count();
        let total: Decimal = self
            .owner_balances
            .values()
            .map(|holder| holder.balance)
            .sum();
        let mut balances: Vec<(PubkeyValue, Decimal)> = self
            .owner_balances
            .iter()
            .filter_map(|(owner, holder)| {
                (holder.balance > Decimal::ZERO)
                    .then(|| (PubkeyValue(owner.clone()), holder.balance))
            })
            .collect();
        balances.sort_by(|(_, left), (_, right)| {
            right.partial_cmp(left).unwrap_or(std::cmp::Ordering::Equal)
        });

        self.top_holders = balances
            .iter()
            .take(20)
            .map(|(owner, balance)| HolderDistributionSnapshot {
                owner: owner.clone(),
                balance: *balance,
                pct_supply_proxy: if total > Decimal::ZERO {
                    *balance / total
                } else {
                    Decimal::ZERO
                },
            })
            .collect();
        self.gini = compute_gini(balances.iter().map(|(_, balance)| *balance).collect());
        self.hhi = compute_hhi(balances.iter().map(|(_, balance)| *balance).collect());
        self.paperhand_90pct_wallet_count = self
            .owner_balances
            .values()
            .filter(|holder| holder.wallet_sold_90pct_flag)
            .count();
        self.exited_holder_count_zero_balance =
            previous_holder_count.saturating_sub(self.nonzero_holder_count);
        self.net_holder_change = self.nonzero_holder_count as i64 - previous_holder_count as i64;
        self.holder_churn_rate = if previous_holder_count > 0 {
            Decimal::from(self.exited_holder_count_zero_balance as u64)
                / Decimal::from(previous_holder_count as u64)
        } else {
            Decimal::ZERO
        };
        self.holder_stickiness =
            (Decimal::ONE - self.holder_churn_rate).clamp(Decimal::ZERO, Decimal::ONE);
        self.holder_count_history
            .push_back((observed_at, self.nonzero_holder_count));
        while self.holder_count_history.len() > MAX_HOLDER_HISTORY {
            self.holder_count_history.pop_front();
        }
    }

    fn refresh_holder_behaviour(holder: &mut HolderBalance) {
        holder.wallet_sell_through_pct = if holder.cost_basis.original_position_size > Decimal::ZERO
        {
            let sold = (holder.cost_basis.original_position_size
                - holder.cost_basis.remaining_position_size)
                .max(Decimal::ZERO);
            (sold / holder.cost_basis.original_position_size).clamp(Decimal::ZERO, Decimal::ONE)
        } else {
            Decimal::ZERO
        };
        holder.wallet_sold_90pct_flag = holder.wallet_sell_through_pct >= Decimal::new(90, 2);
        holder.holder_retention_status = if holder.balance <= Decimal::ZERO {
            HolderRetentionStatus::ExitedZeroBalance
        } else if holder.wallet_sold_90pct_flag {
            HolderRetentionStatus::Sold90Pct
        } else if holder.wallet_sell_through_pct > Decimal::ZERO {
            HolderRetentionStatus::ReducedPosition
        } else {
            HolderRetentionStatus::Active
        };
    }

    pub fn top_holder_pct(&self, rank: usize) -> Decimal {
        self.top_holders
            .iter()
            .take(rank)
            .map(|holder| holder.pct_supply_proxy)
            .sum()
    }

    pub fn observed_holder_supply(&self) -> Decimal {
        self.owner_balances
            .values()
            .map(|holder| holder.balance.max(Decimal::ZERO))
            .sum()
    }

    pub fn holder_count_excluding(&self, excluded_owners: &HashSet<String>) -> usize {
        self.owner_balances
            .iter()
            .filter(|(owner, holder)| {
                holder.balance > Decimal::ZERO && !excluded_owners.contains(owner.as_str())
            })
            .count()
    }

    pub fn top_holder_pct_with_denominator(
        &self,
        rank: usize,
        denominator: Decimal,
        excluded_owners: &HashSet<String>,
    ) -> Decimal {
        if denominator <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let mut balances = self
            .owner_balances
            .iter()
            .filter(|(owner, holder)| {
                holder.balance > Decimal::ZERO && !excluded_owners.contains(owner.as_str())
            })
            .map(|(_, holder)| holder.balance)
            .collect::<Vec<_>>();
        balances
            .sort_by(|left, right| right.partial_cmp(left).unwrap_or(std::cmp::Ordering::Equal));
        balances
            .into_iter()
            .take(rank)
            .fold(Decimal::ZERO, |acc, value| acc + value)
            / denominator
    }

    pub fn missing_owner_mapping_count(&self) -> usize {
        self.token_account_balances
            .keys()
            .filter(|token_account| !self.token_account_to_owner.contains_key(*token_account))
            .count()
    }

    pub fn holder_invariant_violations(
        &self,
        total_supply_raw: Decimal,
        excluded_owners: &HashSet<String>,
    ) -> Vec<String> {
        let mut violations = Vec::new();
        let tolerance = Decimal::new(1, 6);
        let observed = self.observed_holder_supply();
        if observed > total_supply_raw + tolerance {
            violations.push(format!(
                "observed_owner_supply_raw_exceeds_total_supply: observed={observed} total={total_supply_raw}"
            ));
        }
        let excluding_curve: Decimal = self
            .owner_balances
            .iter()
            .filter(|(owner, holder)| {
                holder.balance > Decimal::ZERO && !excluded_owners.contains(owner.as_str())
            })
            .map(|(_, holder)| holder.balance.max(Decimal::ZERO))
            .sum();
        if excluding_curve > total_supply_raw + tolerance {
            violations.push(format!(
                "observed_owner_supply_excluding_curve_raw_exceeds_total_supply: observed={excluding_curve} total={total_supply_raw}"
            ));
        }
        for (owner, holder) in &self.owner_balances {
            if holder.balance < Decimal::ZERO {
                violations.push(format!(
                    "negative_owner_balance: owner={owner} balance={}",
                    holder.balance
                ));
            }
        }
        for (account, balance) in &self.token_account_balances {
            if *balance < Decimal::ZERO {
                violations.push(format!(
                    "negative_token_account_balance: token_account={account} balance={balance}"
                ));
            }
        }
        if self.top_holder_pct(1) > Decimal::ONE + tolerance {
            violations.push(format!(
                "top_holder_pct_observed_exceeds_one: value={}",
                self.top_holder_pct(1)
            ));
        }
        if self.top_holder_pct_with_denominator(1, total_supply_raw, excluded_owners)
            > Decimal::ONE + tolerance
        {
            violations.push("top_holder_pct_total_supply_exceeds_one".to_owned());
        }
        violations
    }
}

fn holder_update_dedupe_key(update: &HolderBalanceUpdateEvent, event: &NormalizedEvent) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        update.mint.0,
        update.token_account.0,
        update.owner_wallet.0,
        event.meta.slot,
        event.meta.signature.as_deref().unwrap_or(""),
        event
            .meta
            .transaction_index
            .map(|value| value.to_string())
            .unwrap_or_default(),
        event
            .meta
            .account_write_version
            .map(|value| value.to_string())
            .unwrap_or_default(),
        update.new_balance
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DeveloperState {
    pub creator_initial_holding: Decimal,
    pub creator_net_tokens: Decimal,
    pub creator_net_quote_flow: Decimal,
    pub creator_first_sell_time: Option<OffsetDateTime>,
    pub creator_sell_percentage: Decimal,
    pub creator_current_rank: Option<usize>,
    pub related_cluster_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WalletSummary {
    pub wallet: String,
    pub first_seen: Option<OffsetDateTime>,
    pub last_seen: Option<OffsetDateTime>,
    pub tokens_bought: HashMap<String, Decimal>,
    pub tokens_sold: HashMap<String, Decimal>,
    pub launches_participated: u64,
    pub creator_launches: u64,
    pub realized_pnl_estimate: Decimal,
    pub unrealized_pnl_estimate: Decimal,
    pub rug_exposure: u64,
    pub average_entry_delay_ms: Option<i64>,
    pub average_hold_duration_ms: Option<i64>,
    pub alpha_score: Decimal,
    pub toxicity_score: Decimal,
    pub cluster_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingEdge {
    pub wallet: String,
    pub funder: String,
    pub amount: Decimal,
    pub last_seen: OffsetDateTime,
    pub count: u64,
}

impl Default for FundingEdge {
    fn default() -> Self {
        Self {
            wallet: String::new(),
            funder: String::new(),
            amount: Decimal::ZERO,
            last_seen: OffsetDateTime::UNIX_EPOCH,
            count: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FundingGraph {
    pub edges: HashMap<(String, String), FundingEdge>,
    funder_to_wallets: HashMap<String, BTreeSet<String>>,
}

impl FundingGraph {
    pub fn apply(
        &mut self,
        event: &WalletFundingEvent,
        observed_at: OffsetDateTime,
    ) -> Vec<(String, String)> {
        let key = (event.funder.0.clone(), event.wallet.0.clone());
        let edge = self.edges.entry(key).or_insert(FundingEdge {
            wallet: event.wallet.0.clone(),
            funder: event.funder.0.clone(),
            amount: Decimal::ZERO,
            last_seen: OffsetDateTime::UNIX_EPOCH,
            count: 0,
        });
        edge.amount += event.amount;
        edge.last_seen = observed_at;
        edge.count += 1;

        let wallets = self
            .funder_to_wallets
            .entry(event.funder.0.clone())
            .or_default();
        let related = wallets
            .iter()
            .filter(|wallet| wallet.as_str() != event.wallet.0.as_str())
            .map(|wallet| (wallet.clone(), event.wallet.0.clone()))
            .collect();
        wallets.insert(event.wallet.0.clone());
        related
    }

    pub fn wallets_for_funder(&self, funder: &str) -> BTreeSet<String> {
        self.funder_to_wallets
            .get(funder)
            .cloned()
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterEvidenceSummary {
    pub same_funder_edges: u64,
    pub same_payer_edges: u64,
    pub shared_creator_edges: u64,
    pub confidence: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterIndex {
    pub edges: HashMap<(String, String), ClusterEvidenceSummary>,
    #[serde(default)]
    wallet_neighbors: HashMap<String, BTreeSet<String>>,
    #[serde(default)]
    same_funder_wallets: BTreeSet<String>,
}

impl ClusterIndex {
    pub fn record_same_funder(&mut self, left: &str, right: &str) {
        let key = normalize_pair(left, right);
        let entry = self.edges.entry(key).or_default();
        entry.same_funder_edges += 1;
        entry.confidence += Decimal::new(10, 2);
        self.record_neighbors(left, right);
        self.same_funder_wallets.insert(left.to_owned());
        self.same_funder_wallets.insert(right.to_owned());
    }

    pub fn record_same_payer(&mut self, left: &str, right: &str) {
        let key = normalize_pair(left, right);
        let entry = self.edges.entry(key).or_default();
        entry.same_payer_edges += 1;
        entry.confidence += Decimal::new(15, 2);
        self.record_neighbors(left, right);
    }

    pub fn cluster_id_for(&self, wallet: &str) -> Option<String> {
        let mut members = vec![wallet.to_owned()];
        if let Some(neighbors) = self.wallet_neighbors.get(wallet) {
            members.extend(neighbors.iter().cloned());
        } else {
            for (edge, evidence) in &self.edges {
                if evidence.confidence <= Decimal::ZERO {
                    continue;
                }
                if edge.0 == wallet {
                    members.push(edge.1.clone());
                } else if edge.1 == wallet {
                    members.push(edge.0.clone());
                }
            }
        }
        members.sort();
        members.dedup();
        (members.len() > 1).then(|| {
            let digest = Sha256::digest(members.join("|").as_bytes());
            format!("cluster-{:x}", digest)[..20].to_owned()
        })
    }

    pub fn related_wallets(&self, wallet: &str) -> BTreeSet<String> {
        let mut wallets = BTreeSet::from([wallet.to_owned()]);
        if let Some(neighbors) = self.wallet_neighbors.get(wallet) {
            wallets.extend(neighbors.iter().cloned());
        } else {
            for (edge, evidence) in &self.edges {
                if evidence.confidence <= Decimal::ZERO {
                    continue;
                }
                if edge.0 == wallet {
                    wallets.insert(edge.1.clone());
                } else if edge.1 == wallet {
                    wallets.insert(edge.0.clone());
                }
            }
        }
        wallets
    }

    pub fn wallet_has_same_funder_cluster(&self, wallet: &str) -> bool {
        if !self.same_funder_wallets.is_empty() {
            return self.same_funder_wallets.contains(wallet);
        }
        self.edges.iter().any(|(edge, evidence)| {
            (edge.0 == wallet || edge.1 == wallet) && evidence.same_funder_edges > 0
        })
    }

    fn record_neighbors(&mut self, left: &str, right: &str) {
        if left == right {
            return;
        }
        self.wallet_neighbors
            .entry(left.to_owned())
            .or_default()
            .insert(right.to_owned());
        self.wallet_neighbors
            .entry(right.to_owned())
            .or_default()
            .insert(left.to_owned());
    }
}

fn normalize_pair(left: &str, right: &str) -> (String, String) {
    if left <= right {
        (left.to_owned(), right.to_owned())
    } else {
        (right.to_owned(), left.to_owned())
    }
}

fn trade_client_fingerprint(
    compute_unit_limit: Option<u32>,
    compute_unit_price: Option<u64>,
    tx_shape: Option<&ObservedTransactionSummary>,
) -> Option<String> {
    if compute_unit_limit.is_none() && compute_unit_price.is_none() && tx_shape.is_none() {
        return None;
    }
    Some(format!(
        "cul:{}|cup:{}|acct:{}|ix:{}|pgm:{}",
        compute_unit_limit.unwrap_or_default(),
        compute_unit_price.unwrap_or_default(),
        tx_shape
            .map(|shape| shape.account_count)
            .unwrap_or_default(),
        tx_shape
            .map(|shape| shape.instruction_count)
            .unwrap_or_default(),
        tx_shape
            .map(|shape| shape.program_ids.len())
            .unwrap_or_default(),
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeObservation {
    pub timestamp: OffsetDateTime,
    pub slot: u64,
    pub signature: Option<String>,
    pub side: TradeSide,
    pub wallet: String,
    pub quote: Decimal,
    pub tokens: Decimal,
    pub price: Decimal,
    pub compute_unit_limit: Option<u32>,
    pub compute_unit_price: Option<u64>,
    pub priority_fee_lamports: Option<u64>,
    pub base_fee_lamports: Option<u64>,
    pub account_count: Option<usize>,
    pub instruction_count: Option<usize>,
    pub program_count: Option<usize>,
    pub client_fingerprint: Option<String>,
    pub is_creator: bool,
    pub is_top_holder_pre_sell: bool,
    pub is_tentative: bool,
}

impl Default for TradeObservation {
    fn default() -> Self {
        Self {
            timestamp: OffsetDateTime::UNIX_EPOCH,
            slot: 0,
            signature: None,
            side: TradeSide::Buy,
            wallet: String::new(),
            quote: Decimal::ZERO,
            tokens: Decimal::ZERO,
            price: Decimal::ZERO,
            compute_unit_limit: None,
            compute_unit_price: None,
            priority_fee_lamports: None,
            base_fee_lamports: None,
            account_count: None,
            instruction_count: None,
            program_count: None,
            client_fingerprint: None,
            is_creator: false,
            is_top_holder_pre_sell: false,
            is_tentative: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ObservedTransactionSummary {
    pub signature: String,
    pub slot_hint: Option<u64>,
    pub entry_index: Option<u32>,
    pub tx_position_estimate: Option<u32>,
    pub program_ids: Vec<String>,
    pub account_count: usize,
    pub instruction_count: usize,
    pub raw_packet_hash: String,
    pub first_seen_by_shred_ns: u64,
    pub decode_confidence: Decimal,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TradeSide {
    Buy,
    Sell,
}

impl Default for TradeSide {
    fn default() -> Self {
        Self::Buy
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TokenTradeStats {
    pub buy_count: u64,
    pub sell_count: u64,
    pub unique_buyers: HashSet<String>,
    pub unique_sellers: HashSet<String>,
    pub buy_volume_quote: Decimal,
    pub sell_volume_quote: Decimal,
    pub latest_price: Decimal,
    pub all_time_high: Decimal,
    pub all_time_low: Decimal,
    pub last_buy_at: Option<OffsetDateTime>,
    pub last_sell_at: Option<OffsetDateTime>,
    pub longest_no_buy_gap_ms: i64,
    pub first_large_sell_at: Option<OffsetDateTime>,
    pub price_history: VecDeque<(OffsetDateTime, Decimal)>,
    pub trade_history: VecDeque<TradeObservation>,
}

impl TokenTradeStats {
    fn record_trade(&mut self, observation: TradeObservation) {
        self.latest_price = observation.price;
        if self.all_time_high < observation.price || self.all_time_high == Decimal::ZERO {
            self.all_time_high = observation.price;
        }
        if self.all_time_low > observation.price || self.all_time_low == Decimal::ZERO {
            self.all_time_low = observation.price;
        }
        self.price_history
            .push_back((observation.timestamp, observation.price));
        self.trade_history.push_back(observation.clone());
        while self.price_history.len() > MAX_PRICE_HISTORY {
            self.price_history.pop_front();
        }
        while self.trade_history.len() > MAX_TRADE_HISTORY {
            self.trade_history.pop_front();
        }

        match observation.side {
            TradeSide::Buy => {
                self.buy_count += 1;
                self.unique_buyers.insert(observation.wallet.clone());
                self.buy_volume_quote += observation.quote;
                if let Some(last_buy) = self.last_buy_at {
                    let gap = (observation.timestamp - last_buy).whole_milliseconds() as i64;
                    if gap > self.longest_no_buy_gap_ms {
                        self.longest_no_buy_gap_ms = gap;
                    }
                }
                self.last_buy_at = Some(observation.timestamp);
            }
            TradeSide::Sell => {
                self.sell_count += 1;
                self.unique_sellers.insert(observation.wallet.clone());
                self.sell_volume_quote += observation.quote;
                self.last_sell_at = Some(observation.timestamp);
                if observation.quote >= Decimal::from(5u64) && self.first_large_sell_at.is_none() {
                    self.first_large_sell_at = Some(observation.timestamp);
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreatWalletEntry {
    pub wallet: String,
    pub classification: DangerousSellerClassification,
    pub balance: Decimal,
    pub holding_pct: Decimal,
    pub estimated_full_exit_impact_pct: Decimal,
    pub estimated_partial_exit_25_pct: Decimal,
    pub estimated_partial_exit_50_pct: Decimal,
    pub estimated_partial_exit_100_pct: Decimal,
    pub cost_basis_quote: Decimal,
    pub unrealized_pnl_quote: Decimal,
    pub free_roll: bool,
    pub last_sell_time: Option<OffsetDateTime>,
    pub historical_rug_association: u64,
    pub cluster_id: Option<String>,
    pub cluster_holding_pct: Decimal,
}

impl Default for ThreatWalletEntry {
    fn default() -> Self {
        Self {
            wallet: String::new(),
            classification: DangerousSellerClassification::Unknown,
            balance: Decimal::ZERO,
            holding_pct: Decimal::ZERO,
            estimated_full_exit_impact_pct: Decimal::ZERO,
            estimated_partial_exit_25_pct: Decimal::ZERO,
            estimated_partial_exit_50_pct: Decimal::ZERO,
            estimated_partial_exit_100_pct: Decimal::ZERO,
            cost_basis_quote: Decimal::ZERO,
            unrealized_pnl_quote: Decimal::ZERO,
            free_roll: false,
            last_sell_time: None,
            historical_rug_association: 0,
            cluster_id: None,
            cluster_holding_pct: Decimal::ZERO,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExitThreatIndex {
    pub dangerous_wallets: Vec<ThreatWalletEntry>,
    pub warn_threshold_impact_pct: Decimal,
    pub arm_exit_threshold_impact_pct: Decimal,
    pub trigger_exit_threshold_impact_pct: Decimal,
    pub emergency_exit_threshold_impact_pct: Decimal,
    pub dangerous_seller_precomputed_impact_score: Decimal,
    pub exit_threat_index_score: Decimal,
    pub minimum_dangerous_sell_size_to_trigger_emergency: Decimal,
    pub current_distance_to_stop_pct: Decimal,
    pub current_distance_to_trailing_stop_pct: Decimal,
    pub current_distance_to_launch_floor_pct: Decimal,
    pub current_distance_to_vwap_pct: Decimal,
    pub current_distance_to_cost_basis_support_pct: Decimal,
    pub combined_our_exit_plus_dangerous_sell_impact_pct: Decimal,
    pub max_safe_wait_time_ms: u64,
    pub required_early_intent_lead_time_ms: u64,
    pub updated_at: Option<OffsetDateTime>,
}

impl Default for ExitThreatIndex {
    fn default() -> Self {
        Self {
            dangerous_wallets: Vec::new(),
            warn_threshold_impact_pct: Decimal::from(8u64),
            arm_exit_threshold_impact_pct: Decimal::from(15u64),
            trigger_exit_threshold_impact_pct: Decimal::from(25u64),
            emergency_exit_threshold_impact_pct: Decimal::from(35u64),
            dangerous_seller_precomputed_impact_score: Decimal::ZERO,
            exit_threat_index_score: Decimal::ZERO,
            minimum_dangerous_sell_size_to_trigger_emergency: Decimal::ZERO,
            current_distance_to_stop_pct: Decimal::ZERO,
            current_distance_to_trailing_stop_pct: Decimal::ZERO,
            current_distance_to_launch_floor_pct: Decimal::ZERO,
            current_distance_to_vwap_pct: Decimal::ZERO,
            current_distance_to_cost_basis_support_pct: Decimal::ZERO,
            combined_our_exit_plus_dangerous_sell_impact_pct: Decimal::ZERO,
            max_safe_wait_time_ms: 0,
            required_early_intent_lead_time_ms: 0,
            updated_at: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TentativeSellTracking {
    pub event_id: String,
    pub source: EarlyIntentSource,
    pub signature: Option<String>,
    pub seller_wallet: String,
    pub seller_classification: DangerousSellerClassification,
    pub confirmation_state: TentativeSellConfirmationState,
    pub warning_level: TentativeSellRiskLevel,
    pub confidence: Decimal,
    pub estimated_impact_pct: Decimal,
    pub estimated_cluster_impact_pct: Decimal,
    pub warning_price: Decimal,
    pub observed_at: OffsetDateTime,
    pub expires_at: Option<OffsetDateTime>,
    pub matched_canonical_signature: Option<String>,
    pub false_positive_flag: bool,
    pub missed_exit_flag: bool,
    pub paper_exit_triggered: bool,
    pub saved_loss_quote: Decimal,
    pub opportunity_cost_quote: Decimal,
    pub confirmation_method: Option<String>,
    pub reconciliation_latency_ms: Option<i64>,
}

impl Default for TentativeSellTracking {
    fn default() -> Self {
        Self {
            event_id: String::new(),
            source: EarlyIntentSource::FixtureTentative,
            signature: None,
            seller_wallet: String::new(),
            seller_classification: DangerousSellerClassification::Unknown,
            confirmation_state: TentativeSellConfirmationState::PendingTentative,
            warning_level: TentativeSellRiskLevel::Info,
            confidence: Decimal::ZERO,
            estimated_impact_pct: Decimal::ZERO,
            estimated_cluster_impact_pct: Decimal::ZERO,
            warning_price: Decimal::ZERO,
            observed_at: OffsetDateTime::UNIX_EPOCH,
            expires_at: None,
            matched_canonical_signature: None,
            false_positive_flag: false,
            missed_exit_flag: false,
            paper_exit_triggered: false,
            saved_loss_quote: Decimal::ZERO,
            opportunity_cost_quote: Decimal::ZERO,
            confirmation_method: None,
            reconciliation_latency_ms: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShredDefenseState {
    pub exit_threat_index: ExitThreatIndex,
    pub pending_tentative_sells: HashMap<String, TentativeSellTracking>,
    pub last_warning_level: Option<TentativeSellRiskLevel>,
    pub last_warning_reason_codes: Vec<ReasonCode>,
    pub active_warning_event_id: Option<String>,
    pub active_armed_exit_event_id: Option<String>,
    pub active_triggered_exit_event_id: Option<String>,
    pub active_dangerous_seller: Option<String>,
    pub active_seller_classification: Option<DangerousSellerClassification>,
    pub tentative_sell_count_window: u64,
    pub tentative_sell_volume_quote_window: Decimal,
    pub tentative_sell_from_dev_count: u64,
    pub tentative_sell_from_top_holder_count: u64,
    pub tentative_sell_from_bundle_count: u64,
    pub tentative_sell_from_whale_count: u64,
    pub tentative_sell_same_slot_cluster_count: u64,
    pub tentative_sell_impact_max_pct: Decimal,
    pub tentative_sell_impact_sum_pct: Decimal,
    pub tentative_sell_confidence_max: Decimal,
    pub tentative_sell_confidence_mean: Decimal,
    pub shred_emergency_exit_triggered_flag: bool,
    pub shred_exit_armed_flag: bool,
    pub shred_signal_stale_flag: bool,
    pub shred_saved_loss_estimate: Decimal,
    pub shred_saved_loss_realized: Decimal,
    pub shred_exit_opportunity_cost: Decimal,
    pub early_intent_latency_advantage_ms: Option<i64>,
    pub required_latency_advantage_ms: Option<i64>,
    pub latency_edge_ratio: Decimal,
    pub exit_can_land_before_estimated_impact: bool,
    pub absorption_health_score: Decimal,
    pub post_sell_absorption_probability: Decimal,
    pub emergency_exit_expected_saved_loss: Decimal,
    pub emergency_exit_expected_opportunity_cost: Decimal,
    pub emergency_exit_net_benefit: Decimal,
    pub emergency_exit_net_benefit_confidence: Decimal,
    pub shred_to_geyser_processed_ms: Option<i64>,
    pub shred_to_account_effect_confirmation_ms: Option<i64>,
    pub shred_to_rooted_confirmation_ms: Option<i64>,
    pub tentative_sell_false_positive_total: u64,
    pub tentative_sell_confirmed_total: u64,
    pub tentative_sell_failed_total: u64,
    pub tentative_sell_not_seen_total: u64,
    pub tentative_sell_reorged_total: u64,
    pub tentative_sell_decode_mismatch_total: u64,
    pub malicious_sell_intent_score: Decimal,
    pub preconfirmation_exit_confidence: Decimal,
    pub last_confirmation_level: Option<TentativeSellConfirmationState>,
    pub last_resolution_outcome: Option<TentativeSellResolutionOutcome>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenState {
    pub mint: PubkeyValue,
    pub token_program: TokenProgramType,
    pub quote_mint: Option<PubkeyValue>,
    pub quote_asset_type: QuoteAssetType,
    pub creator: Option<PubkeyValue>,
    pub payer: Option<PubkeyValue>,
    pub bonding_curve: Option<PubkeyValue>,
    pub associated_bonding_curve: Option<PubkeyValue>,
    pub metadata: Option<PubkeyValue>,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub create_instruction_variant: String,
    pub launch_transaction_fingerprint: Option<String>,
    pub launch_same_transaction_buys: u32,
    pub launch_same_slot_buys: u32,
    pub lifecycle: TokenLifecycle,
    pub launch_slot: Option<u64>,
    pub launch_time: Option<OffsetDateTime>,
    pub first_seen_source: EventSource,
    pub latest_price: Decimal,
    pub reserve_state: BondingCurveState,
    pub holder_state: HolderState,
    pub developer_state: DeveloperState,
    pub trade_stats: TokenTradeStats,
    pub risk_summary: HashMap<String, Decimal>,
    pub feature_summary: HashMap<String, Decimal>,
    pub shred_defense: ShredDefenseState,
    pub approximate_memory_bytes: usize,
    pub expires_at: Option<OffsetDateTime>,
    pub tentative_signatures: BTreeSet<String>,
    pub canonical_signatures: BTreeSet<String>,
    pub tentative_only: bool,
    pub data_quality_flags: BTreeSet<String>,
}

impl TokenState {
    pub fn new(mint: PubkeyValue, source: EventSource) -> Self {
        Self {
            mint,
            token_program: TokenProgramType::Unknown,
            quote_mint: None,
            quote_asset_type: QuoteAssetType::Unknown,
            creator: None,
            payer: None,
            bonding_curve: None,
            associated_bonding_curve: None,
            metadata: None,
            name: String::new(),
            symbol: String::new(),
            uri: String::new(),
            create_instruction_variant: String::new(),
            launch_transaction_fingerprint: None,
            launch_same_transaction_buys: 0,
            launch_same_slot_buys: 0,
            lifecycle: TokenLifecycle::Discovered,
            launch_slot: None,
            launch_time: None,
            first_seen_source: source,
            latest_price: Decimal::ZERO,
            reserve_state: BondingCurveState::default(),
            holder_state: HolderState::default(),
            developer_state: DeveloperState::default(),
            trade_stats: TokenTradeStats::default(),
            risk_summary: HashMap::new(),
            feature_summary: HashMap::new(),
            shred_defense: ShredDefenseState::default(),
            approximate_memory_bytes: 0,
            expires_at: None,
            tentative_signatures: BTreeSet::new(),
            canonical_signatures: BTreeSet::new(),
            tentative_only: matches!(
                source,
                EventSource::ShredTentative | EventSource::DeshredTentative
            ),
            data_quality_flags: BTreeSet::new(),
        }
    }

    pub fn update_memory_bytes(&mut self) {
        self.approximate_memory_bytes = 512
            + self.holder_state.owner_balances.len() * 192
            + self.holder_state.token_account_to_owner.len() * 96
            + self.trade_stats.trade_history.len() * 112
            + self.trade_stats.price_history.len() * 48
            + self.tentative_signatures.len() * 72
            + self.canonical_signatures.len() * 72
            + self.shred_defense.pending_tentative_sells.len() * 192
            + self.shred_defense.exit_threat_index.dangerous_wallets.len() * 160;
    }

    pub fn recalc_developer_rank(&mut self) {
        let creator = self.creator.as_ref().map(|creator| creator.0.as_str());
        self.developer_state.creator_current_rank = creator.and_then(|creator_wallet| {
            self.holder_state
                .top_holders
                .iter()
                .position(|holder| holder.owner.0 == creator_wallet)
                .map(|index| index + 1)
        });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactTokenSummary {
    pub mint: String,
    pub lifecycle: TokenLifecycle,
    pub latest_price: Decimal,
    pub holder_count: usize,
    pub top1_holder_pct: Decimal,
    pub creator_sold_pct: Decimal,
    pub canonical_trade_count: u64,
    pub reason_codes: Vec<ReasonCode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StateSnapshot {
    pub tokens: HashMap<String, TokenState>,
    pub wallets: HashMap<String, WalletSummary>,
    pub funding_graph: FundingGraph,
    pub cluster_index: ClusterIndex,
    pub discarded_summaries: HashMap<String, CompactTokenSummary>,
}

pub struct StateEngine {
    ttl: TtlConfig,
    tokens: HashMap<String, TokenState>,
    wallets: HashMap<String, WalletSummary>,
    funding_graph: FundingGraph,
    cluster_index: ClusterIndex,
    discarded_summaries: HashMap<String, CompactTokenSummary>,
    tentative_signatures: HashMap<String, String>,
    observed_transactions: HashMap<String, ObservedTransactionSummary>,
}

impl StateEngine {
    pub fn new(ttl: TtlConfig) -> Self {
        Self {
            ttl,
            tokens: HashMap::new(),
            wallets: HashMap::new(),
            funding_graph: FundingGraph::default(),
            cluster_index: ClusterIndex::default(),
            discarded_summaries: HashMap::new(),
            tentative_signatures: HashMap::new(),
            observed_transactions: HashMap::new(),
        }
    }

    pub fn snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            tokens: self.tokens.clone(),
            wallets: self.wallets.clone(),
            funding_graph: self.funding_graph.clone(),
            cluster_index: self.cluster_index.clone(),
            discarded_summaries: self.discarded_summaries.clone(),
        }
    }

    pub fn token(&self, mint: &PubkeyValue) -> Option<&TokenState> {
        self.tokens.get(&mint.0)
    }

    pub fn wallet(&self, wallet: &PubkeyValue) -> Option<&WalletSummary> {
        self.wallets.get(&wallet.0)
    }

    pub fn tokens(&self) -> &HashMap<String, TokenState> {
        &self.tokens
    }

    pub fn wallets(&self) -> &HashMap<String, WalletSummary> {
        &self.wallets
    }

    pub fn funding_graph(&self) -> &FundingGraph {
        &self.funding_graph
    }

    pub fn cluster_index(&self) -> &ClusterIndex {
        &self.cluster_index
    }

    pub fn discarded_summary(&self, mint: &PubkeyValue) -> Option<&CompactTokenSummary> {
        self.discarded_summaries.get(&mint.0)
    }

    pub fn token_count(&self) -> usize {
        self.tokens.len()
    }

    pub fn discarded_token_count(&self) -> usize {
        self.discarded_summaries.len()
    }

    pub fn rugged_token_count(&self) -> usize {
        self.tokens
            .values()
            .filter(|token| token.lifecycle == TokenLifecycle::RugArchive)
            .count()
    }

    pub fn apply_event(
        &mut self,
        event: &NormalizedEvent,
    ) -> Result<Vec<LifecycleTransition>, StateError> {
        if matches!(event.meta.canonicality, Canonicality::Reverted) {
            if let Some(signature) = event.signature() {
                self.tentative_signatures.remove(signature);
            }
            return Ok(Vec::new());
        }

        match &event.payload {
            EventPayload::TokenCreated(payload) => self.apply_token_created(payload, event),
            EventPayload::PumpBuy(payload) => self.apply_buy(payload, event),
            EventPayload::PumpSell(payload) => {
                if !matches!(event.meta.canonicality, Canonicality::Tentative) {
                    self.apply_sell(payload, event)
                }
            }
            EventPayload::BondingCurveUpdate(payload) => self.apply_curve_update(payload, event)?,
            EventPayload::HolderBalanceUpdate(payload) => {
                self.apply_holder_update(payload, event)?
            }
            EventPayload::WalletFunding(payload) => self.apply_funding(payload, event),
            EventPayload::ObservedTransaction(payload) => {
                self.apply_observed_transaction(payload, event)
            }
            EventPayload::TentativeSellIntentDetected(payload) => {
                self.apply_tentative_sell_intent(payload, event)
            }
            EventPayload::TentativeMaliciousSellWarning(payload) => {
                self.apply_tentative_sell_warning(payload, event)
            }
            EventPayload::ShredEmergencyExitArmed(payload) => {
                self.apply_shred_exit_armed(payload, event)
            }
            EventPayload::ShredEmergencyExitTriggered(payload) => {
                self.apply_shred_exit_triggered(payload, event)
            }
            EventPayload::ShredSellIntentResolved(payload) => {
                self.apply_shred_sell_resolved(payload, event)
            }
            EventPayload::TokenTerminal(payload) => self.apply_terminal(payload, event),
            EventPayload::DataGap(payload) => self.apply_data_gap(payload, event),
            EventPayload::TradeDecision(_)
            | EventPayload::SimulatedFill(_)
            | EventPayload::LiveFill(_) => {}
        }

        self.refresh_derived_indices_for_event(event);
        Ok(self.recompute_transitions(event))
    }

    fn apply_token_created(
        &mut self,
        payload: &common::TokenCreatedEvent,
        event: &NormalizedEvent,
    ) {
        let ttl_cfg = self.ttl.clone();
        {
            let token = self
                .tokens
                .entry(payload.mint.0.clone())
                .or_insert_with(|| TokenState::new(payload.mint.clone(), event.meta.source));
            token.token_program = payload.token_program;
            token.quote_mint = Some(payload.quote_mint.clone());
            token.quote_asset_type = payload.quote_asset_type;
            token.creator = Some(payload.creator_wallet.clone());
            token.payer = Some(payload.payer.clone());
            token.bonding_curve = Some(payload.bonding_curve_account.clone());
            token.associated_bonding_curve = payload.associated_bonding_curve_account.clone();
            token.metadata = payload.metadata_account.clone();
            token.name = payload.name.clone();
            token.symbol = payload.symbol.clone();
            token.uri = payload.uri.clone();
            token.create_instruction_variant = payload.create_instruction_variant.clone();
            token.launch_transaction_fingerprint = payload.launch_transaction_fingerprint.clone();
            token.launch_same_transaction_buys = payload.same_transaction_buys;
            token.launch_same_slot_buys = payload.same_slot_buys;
            token.launch_slot.get_or_insert(event.meta.slot);
            token
                .launch_time
                .get_or_insert(event.meta.received_at_wall_time);
            token.reserve_state.quote_asset_type = payload.quote_asset_type;
            token
                .reserve_state
                .launch_price
                .get_or_insert(Decimal::ZERO);
            token.expires_at =
                Some(event.meta.received_at_wall_time + ttl_for_config(&ttl_cfg, token.lifecycle));
            if let Some(signature) = event.signature() {
                if matches!(event.meta.canonicality, Canonicality::Tentative) {
                    token.tentative_signatures.insert(signature.to_owned());
                    self.tentative_signatures
                        .insert(signature.to_owned(), token.mint.0.clone());
                } else {
                    token.canonical_signatures.insert(signature.to_owned());
                    token.tentative_signatures.remove(signature);
                    self.tentative_signatures.remove(signature);
                    token.tentative_only = false;
                }
            }
            token.update_memory_bytes();
        }
        self.touch_wallet(&payload.creator_wallet.0, event.meta.received_at_wall_time)
            .creator_launches += 1;
        if payload.creator_wallet.0 != payload.payer.0 {
            self.cluster_index
                .record_same_payer(&payload.creator_wallet.0, &payload.payer.0);
        }
    }

    fn apply_buy(&mut self, payload: &PumpBuyEvent, event: &NormalizedEvent) {
        let ttl_cfg = self.ttl.clone();
        let is_tentative = matches!(event.meta.canonicality, Canonicality::Tentative);
        let token_decimals = self
            .tokens
            .get(&payload.mint.0)
            .map(|token| token.reserve_state.token_decimals)
            .unwrap_or(DEFAULT_PUMP_TOKEN_DECIMALS);
        let price = price_sol_per_ui_token(payload.quote_in, payload.token_out, token_decimals)
            .unwrap_or(payload.effective_price);
        let tx_shape = event
            .signature()
            .and_then(|signature| self.observed_transactions.get(signature))
            .cloned();
        let client_fingerprint = trade_client_fingerprint(
            payload.compute_unit_limit,
            payload.compute_unit_price,
            tx_shape.as_ref(),
        );

        {
            let token = self
                .tokens
                .entry(payload.mint.0.clone())
                .or_insert_with(|| TokenState::new(payload.mint.clone(), event.meta.source));
            if let Some(signature) = event.signature() {
                if is_tentative {
                    token.tentative_signatures.insert(signature.to_owned());
                    self.tentative_signatures
                        .insert(signature.to_owned(), token.mint.0.clone());
                } else {
                    token.canonical_signatures.insert(signature.to_owned());
                    token.tentative_signatures.remove(signature);
                    self.tentative_signatures.remove(signature);
                    token.tentative_only = false;
                }
            }
            token.latest_price = price;
            token.trade_stats.record_trade(TradeObservation {
                timestamp: event.meta.received_at_wall_time,
                slot: event.meta.slot,
                signature: event.signature().map(ToOwned::to_owned),
                side: TradeSide::Buy,
                wallet: payload.buyer.0.clone(),
                quote: payload.quote_in,
                tokens: payload.token_out,
                price,
                compute_unit_limit: payload.compute_unit_limit,
                compute_unit_price: payload.compute_unit_price,
                priority_fee_lamports: payload
                    .estimated_priority_fee_lamports
                    .map(|lamports| lamports.0),
                base_fee_lamports: payload
                    .estimated_base_fee_lamports
                    .map(|lamports| lamports.0),
                account_count: tx_shape.as_ref().map(|shape| shape.account_count),
                instruction_count: tx_shape.as_ref().map(|shape| shape.instruction_count),
                program_count: tx_shape.as_ref().map(|shape| shape.program_ids.len()),
                client_fingerprint,
                is_creator: payload.is_creator || token.creator.as_ref() == Some(&payload.buyer),
                is_top_holder_pre_sell: false,
                is_tentative,
            });
            token.holder_state.apply_trade_cost_basis(
                &payload.buyer,
                true,
                payload.quote_in,
                payload.token_out,
                event.meta.received_at_wall_time,
                price,
            );
            if payload.is_creator || token.creator.as_ref() == Some(&payload.buyer) {
                token.developer_state.creator_initial_holding = token
                    .developer_state
                    .creator_initial_holding
                    .max(token.developer_state.creator_net_tokens + payload.token_out);
                token.developer_state.creator_net_tokens += payload.token_out;
                token.developer_state.creator_net_quote_flow -= payload.quote_in;
            }
            token.expires_at =
                Some(event.meta.received_at_wall_time + ttl_for_config(&ttl_cfg, token.lifecycle));
            token.update_memory_bytes();
        }

        let wallet = self.touch_wallet(&payload.buyer.0, event.meta.received_at_wall_time);
        *wallet
            .tokens_bought
            .entry(payload.mint.0.clone())
            .or_default() += payload.token_out;
    }

    fn apply_sell(&mut self, payload: &PumpSellEvent, event: &NormalizedEvent) {
        let ttl_cfg = self.ttl.clone();
        let is_tentative = matches!(event.meta.canonicality, Canonicality::Tentative);
        let token_decimals = self
            .tokens
            .get(&payload.mint.0)
            .map(|token| token.reserve_state.token_decimals)
            .unwrap_or(DEFAULT_PUMP_TOKEN_DECIMALS);
        let price = price_sol_per_ui_token(payload.quote_out, payload.token_in, token_decimals)
            .unwrap_or(payload.effective_price);
        let tx_shape = event
            .signature()
            .and_then(|signature| self.observed_transactions.get(signature))
            .cloned();
        let client_fingerprint = trade_client_fingerprint(
            payload.compute_unit_limit,
            payload.compute_unit_price,
            tx_shape.as_ref(),
        );

        {
            let token = self
                .tokens
                .entry(payload.mint.0.clone())
                .or_insert_with(|| TokenState::new(payload.mint.clone(), event.meta.source));
            if let Some(signature) = event.signature() {
                if is_tentative {
                    token.tentative_signatures.insert(signature.to_owned());
                    self.tentative_signatures
                        .insert(signature.to_owned(), token.mint.0.clone());
                } else {
                    token.canonical_signatures.insert(signature.to_owned());
                    token.tentative_signatures.remove(signature);
                    self.tentative_signatures.remove(signature);
                    token.tentative_only = false;
                }
            }
            token.latest_price = price;
            token.trade_stats.record_trade(TradeObservation {
                timestamp: event.meta.received_at_wall_time,
                slot: event.meta.slot,
                signature: event.signature().map(ToOwned::to_owned),
                side: TradeSide::Sell,
                wallet: payload.seller.0.clone(),
                quote: payload.quote_out,
                tokens: payload.token_in,
                price,
                compute_unit_limit: payload.compute_unit_limit,
                compute_unit_price: payload.compute_unit_price,
                priority_fee_lamports: payload
                    .estimated_priority_fee_lamports
                    .map(|lamports| lamports.0),
                base_fee_lamports: payload
                    .estimated_base_fee_lamports
                    .map(|lamports| lamports.0),
                account_count: tx_shape.as_ref().map(|shape| shape.account_count),
                instruction_count: tx_shape.as_ref().map(|shape| shape.instruction_count),
                program_count: tx_shape.as_ref().map(|shape| shape.program_ids.len()),
                client_fingerprint,
                is_creator: payload.is_creator || token.creator.as_ref() == Some(&payload.seller),
                is_top_holder_pre_sell: payload.is_top_holder_pre_sell,
                is_tentative,
            });
            token.holder_state.apply_trade_cost_basis(
                &payload.seller,
                false,
                payload.quote_out,
                payload.token_in,
                event.meta.received_at_wall_time,
                price,
            );
            if payload.is_creator || token.creator.as_ref() == Some(&payload.seller) {
                if token.developer_state.creator_first_sell_time.is_none() {
                    token.developer_state.creator_first_sell_time =
                        Some(event.meta.received_at_wall_time);
                }
                token.developer_state.creator_initial_holding = token
                    .developer_state
                    .creator_initial_holding
                    .max(token.developer_state.creator_net_tokens);
                token.developer_state.creator_net_tokens -= payload.token_in;
                token.developer_state.creator_net_quote_flow += payload.quote_out;
            }
            token.expires_at =
                Some(event.meta.received_at_wall_time + ttl_for_config(&ttl_cfg, token.lifecycle));
            token.update_memory_bytes();
        }

        let wallet = self.touch_wallet(&payload.seller.0, event.meta.received_at_wall_time);
        *wallet
            .tokens_sold
            .entry(payload.mint.0.clone())
            .or_default() += payload.token_in;
    }

    fn apply_curve_update(
        &mut self,
        payload: &BondingCurveUpdateEvent,
        event: &NormalizedEvent,
    ) -> Result<(), StateError> {
        let token = self
            .tokens
            .entry(payload.mint.0.clone())
            .or_insert_with(|| TokenState::new(payload.mint.clone(), event.meta.source));
        if !token.reserve_state.apply_update(payload, event.meta.slot) {
            return Ok(());
        }
        token.reserve_state.last_updated_at = Some(event.meta.received_at_wall_time);
        token.latest_price = payload.price;
        token.update_memory_bytes();
        Ok(())
    }

    fn apply_holder_update(
        &mut self,
        payload: &HolderBalanceUpdateEvent,
        event: &NormalizedEvent,
    ) -> Result<(), StateError> {
        {
            let token = self
                .tokens
                .entry(payload.mint.0.clone())
                .or_insert_with(|| TokenState::new(payload.mint.clone(), event.meta.source));
            token.holder_state.apply_balance_update(
                payload,
                event,
                payload
                    .token_decimals
                    .unwrap_or(token.reserve_state.token_decimals),
            );
            if token.creator.as_ref() == Some(&payload.owner_wallet) {
                token.developer_state.creator_net_tokens = payload.new_balance;
                token.developer_state.creator_initial_holding = token
                    .developer_state
                    .creator_initial_holding
                    .max(payload.new_balance);
            }
            token.recalc_developer_rank();
            token.update_memory_bytes();
        }
        self.touch_wallet(&payload.owner_wallet.0, event.meta.received_at_wall_time);
        Ok(())
    }

    fn apply_funding(&mut self, payload: &WalletFundingEvent, event: &NormalizedEvent) {
        let related = self
            .funding_graph
            .apply(payload, event.meta.received_at_wall_time);
        for (left, right) in related {
            self.cluster_index.record_same_funder(&left, &right);
        }
        self.touch_wallet(&payload.wallet.0, event.meta.received_at_wall_time);
        self.touch_wallet(&payload.funder.0, event.meta.received_at_wall_time);
    }

    fn apply_observed_transaction(
        &mut self,
        payload: &common::ObservedTransactionEvent,
        event: &NormalizedEvent,
    ) {
        let Some(signature) = event.signature().map(ToOwned::to_owned) else {
            return;
        };
        self.observed_transactions.insert(
            signature.clone(),
            ObservedTransactionSummary {
                signature,
                slot_hint: payload.slot_hint,
                entry_index: payload.entry_index,
                tx_position_estimate: payload.tx_position_estimate,
                program_ids: payload.program_ids.clone(),
                account_count: payload.account_count,
                instruction_count: payload.instruction_count,
                raw_packet_hash: payload.raw_packet_hash.clone(),
                first_seen_by_shred_ns: payload.first_seen_by_shred_ns,
                decode_confidence: payload.decode_confidence,
            },
        );
    }

    fn apply_tentative_sell_intent(
        &mut self,
        payload: &TentativeSellIntentDetectedEvent,
        event: &NormalizedEvent,
    ) {
        let token = self
            .tokens
            .entry(payload.mint.0.clone())
            .or_insert_with(|| TokenState::new(payload.mint.clone(), event.meta.source));
        let tracking = TentativeSellTracking {
            event_id: payload.event_id.0.to_string(),
            source: payload.source,
            signature: payload.signature.clone(),
            seller_wallet: payload.seller_wallet.0.clone(),
            seller_classification: parse_seller_classification(
                &payload.wallet_classification_snapshot,
            ),
            confirmation_state: payload.confirmation_state,
            warning_level: TentativeSellRiskLevel::Info,
            confidence: payload
                .decoded_instruction_confidence
                .min(payload.account_decode_confidence),
            estimated_impact_pct: payload.estimated_price_impact_pct,
            estimated_cluster_impact_pct: payload.estimated_cluster_holding_pct,
            warning_price: payload.estimated_price_before,
            observed_at: payload.received_at_wall_time,
            expires_at: Some(payload.received_at_wall_time + time::Duration::milliseconds(5_000)),
            matched_canonical_signature: payload.matched_canonical_signature.clone(),
            false_positive_flag: false,
            missed_exit_flag: false,
            paper_exit_triggered: false,
            saved_loss_quote: Decimal::ZERO,
            opportunity_cost_quote: Decimal::ZERO,
            confirmation_method: None,
            reconciliation_latency_ms: None,
        };
        token
            .shred_defense
            .pending_tentative_sells
            .insert(tracking.event_id.clone(), tracking);
        token.shred_defense.tentative_sell_count_window = token
            .shred_defense
            .tentative_sell_count_window
            .saturating_add(1);
        token.shred_defense.tentative_sell_volume_quote_window += payload.quote_out_estimate;
        token.shred_defense.tentative_sell_impact_max_pct = token
            .shred_defense
            .tentative_sell_impact_max_pct
            .max(payload.estimated_price_impact_pct);
        token.shred_defense.tentative_sell_impact_sum_pct += payload.estimated_price_impact_pct;
        token.shred_defense.tentative_sell_confidence_max = token
            .shred_defense
            .tentative_sell_confidence_max
            .max(payload.decoded_instruction_confidence);
        token.shred_defense.tentative_sell_confidence_mean = average_decimal(
            token.shred_defense.tentative_sell_confidence_mean,
            payload.decoded_instruction_confidence,
            token.shred_defense.tentative_sell_count_window,
        );
        token.shred_defense.preconfirmation_exit_confidence =
            token.shred_defense.tentative_sell_confidence_max;
        token.shred_defense.last_confirmation_level = Some(payload.confirmation_state);
        token.update_memory_bytes();
    }

    fn apply_tentative_sell_warning(
        &mut self,
        payload: &TentativeMaliciousSellWarningEvent,
        event: &NormalizedEvent,
    ) {
        let token = self
            .tokens
            .entry(payload.mint.0.clone())
            .or_insert_with(|| TokenState::new(payload.mint.clone(), event.meta.source));
        token.shred_defense.last_warning_level = Some(payload.risk_level);
        token.shred_defense.last_warning_reason_codes = payload.reason_codes.clone();
        token.shred_defense.active_warning_event_id = Some(payload.trigger_event_id.0.to_string());
        token.shred_defense.active_dangerous_seller = Some(payload.seller_wallet.0.clone());
        token.shred_defense.active_seller_classification = Some(payload.seller_classification);
        token.shred_defense.malicious_sell_intent_score = clamp01_decimal(
            payload.estimated_sell_impact_pct / Decimal::from(100u64)
                + payload.confidence / Decimal::from(2u64),
        );
        token.shred_defense.preconfirmation_exit_confidence = payload.confidence;
        token.shred_defense.early_intent_latency_advantage_ms = payload.source_latency_advantage_ms;
        token.shred_defense.required_latency_advantage_ms = payload.required_latency_advantage_ms;
        token.shred_defense.latency_edge_ratio = payload.latency_edge_ratio;
        token.shred_defense.exit_can_land_before_estimated_impact =
            payload.exit_can_land_before_estimated_impact;
        token.shred_defense.absorption_health_score = payload.absorption_health_score;
        token.shred_defense.post_sell_absorption_probability =
            payload.post_sell_absorption_probability;
        token.shred_defense.emergency_exit_expected_saved_loss =
            payload.emergency_exit_expected_saved_loss;
        token.shred_defense.emergency_exit_expected_opportunity_cost =
            payload.emergency_exit_expected_opportunity_cost;
        token.shred_defense.emergency_exit_net_benefit = payload.emergency_exit_net_benefit;
        token.shred_defense.emergency_exit_net_benefit_confidence =
            payload.emergency_exit_net_benefit_confidence;
        token.shred_defense.shred_signal_stale_flag =
            !payload.exit_can_land_before_estimated_impact;
        match payload.seller_classification {
            DangerousSellerClassification::Dev | DangerousSellerClassification::DevCluster => {
                token.shred_defense.tentative_sell_from_dev_count += 1;
            }
            DangerousSellerClassification::Top1Holder
            | DangerousSellerClassification::Top3Holder
            | DangerousSellerClassification::Top5Holder
            | DangerousSellerClassification::Top10Holder => {
                token.shred_defense.tentative_sell_from_top_holder_count += 1;
            }
            DangerousSellerClassification::BundleWallet
            | DangerousSellerClassification::BundleCluster => {
                token.shred_defense.tentative_sell_from_bundle_count += 1;
            }
            DangerousSellerClassification::Whale => {
                token.shred_defense.tentative_sell_from_whale_count += 1;
            }
            _ => {}
        }
        if payload
            .reason_codes
            .iter()
            .any(|reason| matches!(reason, ReasonCode::ShredSameSlotSellCluster))
        {
            token.shred_defense.tentative_sell_same_slot_cluster_count += 1;
        }
        token.update_memory_bytes();
    }

    fn apply_shred_exit_armed(
        &mut self,
        payload: &ShredEmergencyExitArmedEvent,
        event: &NormalizedEvent,
    ) {
        let token = self
            .tokens
            .entry(payload.mint.0.clone())
            .or_insert_with(|| TokenState::new(payload.mint.clone(), event.meta.source));
        token.shred_defense.shred_exit_armed_flag = true;
        token.shred_defense.active_armed_exit_event_id =
            Some(payload.trigger_event_id.0.to_string());
        token.shred_defense.active_dangerous_seller = Some(payload.seller_wallet.0.clone());
        token.shred_defense.active_seller_classification = Some(payload.seller_classification);
        token.shred_defense.last_warning_level = Some(payload.risk_level);
        token.update_memory_bytes();
    }

    fn apply_shred_exit_triggered(
        &mut self,
        payload: &ShredEmergencyExitTriggeredEvent,
        event: &NormalizedEvent,
    ) {
        let token = self
            .tokens
            .entry(payload.mint.0.clone())
            .or_insert_with(|| TokenState::new(payload.mint.clone(), event.meta.source));
        token.shred_defense.shred_emergency_exit_triggered_flag = true;
        token.shred_defense.active_triggered_exit_event_id =
            Some(payload.trigger_event_id.0.to_string());
        token.shred_defense.active_dangerous_seller = Some(payload.seller_wallet.0.clone());
        token.shred_defense.active_seller_classification = Some(payload.seller_classification);
        token.shred_defense.shred_saved_loss_estimate =
            payload.estimated_saved_loss_vs_waiting_for_geyser;
        if let Some(tracking) = token
            .shred_defense
            .pending_tentative_sells
            .get_mut(&payload.trigger_event_id.0.to_string())
        {
            tracking.paper_exit_triggered = payload.paper_allowed;
            tracking.warning_level = TentativeSellRiskLevel::EmergencyExitRequired;
        }
        token.update_memory_bytes();
    }

    fn apply_shred_sell_resolved(
        &mut self,
        payload: &ShredSellIntentResolvedEvent,
        event: &NormalizedEvent,
    ) {
        let token = self
            .tokens
            .entry(payload.mint.0.clone())
            .or_insert_with(|| TokenState::new(payload.mint.clone(), event.meta.source));
        let key = payload.original_tentative_event_id.0.to_string();
        let mut tracking = token
            .shred_defense
            .pending_tentative_sells
            .remove(&key)
            .unwrap_or_default();
        tracking.confirmation_state = payload.confirmation_state;
        tracking.matched_canonical_signature = payload.canonical_signature.clone();
        tracking.reconciliation_latency_ms = payload.reconciliation_latency_ms;
        tracking.saved_loss_quote = payload.actual_loss_saved_if_exited;
        tracking.false_positive_flag = payload.false_positive_flag;
        tracking.missed_exit_flag = payload.missed_exit_flag;
        tracking.confirmation_method = payload.confirmation_method.clone();
        tracking.opportunity_cost_quote = if payload.false_positive_flag {
            payload.actual_loss_saved_if_exited.abs()
        } else {
            Decimal::ZERO
        };
        token.shred_defense.last_confirmation_level = Some(payload.confirmation_state);
        token.shred_defense.last_resolution_outcome = Some(payload.outcome);
        match payload.outcome {
            TentativeSellResolutionOutcome::ConfirmedExecuted => {
                token.shred_defense.tentative_sell_confirmed_total += 1;
                token.shred_defense.shred_saved_loss_realized +=
                    payload.actual_loss_saved_if_exited;
                token.shred_defense.shred_to_geyser_processed_ms =
                    payload.reconciliation_latency_ms;
            }
            TentativeSellResolutionOutcome::AccountEffectsObserved => {
                token.shred_defense.tentative_sell_confirmed_total += 1;
                token.shred_defense.shred_to_account_effect_confirmation_ms =
                    payload.reconciliation_latency_ms;
            }
            TentativeSellResolutionOutcome::ConfirmedFailed => {
                token.shred_defense.tentative_sell_failed_total += 1;
            }
            TentativeSellResolutionOutcome::RootedExecuted => {
                token.shred_defense.tentative_sell_confirmed_total += 1;
                token.shred_defense.shred_to_rooted_confirmation_ms =
                    payload.reconciliation_latency_ms;
            }
            TentativeSellResolutionOutcome::NotSeenWithinTtl => {
                token.shred_defense.tentative_sell_not_seen_total += 1;
            }
            TentativeSellResolutionOutcome::Reorged => {
                token.shred_defense.tentative_sell_reorged_total += 1;
            }
            TentativeSellResolutionOutcome::DecodeMismatch => {
                token.shred_defense.tentative_sell_decode_mismatch_total += 1;
            }
        }
        if payload.false_positive_flag {
            token.shred_defense.tentative_sell_false_positive_total += 1;
            token.shred_defense.shred_exit_opportunity_cost += tracking.opportunity_cost_quote;
        }
        token.shred_defense.shred_exit_armed_flag = false;
        token.shred_defense.active_armed_exit_event_id = None;
        token.shred_defense.active_warning_event_id = None;
        token.shred_defense.active_triggered_exit_event_id = None;
        token.shred_defense.active_dangerous_seller = None;
        token.shred_defense.active_seller_classification = None;
        token.shred_defense.early_intent_latency_advantage_ms = None;
        token.shred_defense.required_latency_advantage_ms = None;
        token.shred_defense.latency_edge_ratio = Decimal::ZERO;
        token.shred_defense.exit_can_land_before_estimated_impact = false;
        token.shred_defense.absorption_health_score = Decimal::ZERO;
        token.shred_defense.post_sell_absorption_probability = Decimal::ZERO;
        token.shred_defense.emergency_exit_expected_saved_loss = Decimal::ZERO;
        token.shred_defense.emergency_exit_expected_opportunity_cost = Decimal::ZERO;
        token.shred_defense.emergency_exit_net_benefit = Decimal::ZERO;
        token.shred_defense.emergency_exit_net_benefit_confidence = Decimal::ZERO;
        token.update_memory_bytes();
    }

    fn apply_terminal(&mut self, payload: &common::TokenTerminalEvent, event: &NormalizedEvent) {
        if let Some(token) = self.tokens.get_mut(&payload.mint.0) {
            let target = match payload.variant {
                common::TokenTerminalVariant::Discarded => TokenLifecycle::SoftDiscarded,
                common::TokenTerminalVariant::SoftDiscarded => TokenLifecycle::SoftDiscarded,
                common::TokenTerminalVariant::HardDiscarded => TokenLifecycle::HardDiscarded,
                common::TokenTerminalVariant::Rugged => TokenLifecycle::RugArchive,
                common::TokenTerminalVariant::Dead => TokenLifecycle::Completed,
                common::TokenTerminalVariant::Migrated => TokenLifecycle::Migrated,
                common::TokenTerminalVariant::TrackingExpired => TokenLifecycle::Completed,
                common::TokenTerminalVariant::DataGap => TokenLifecycle::DataGap,
                common::TokenTerminalVariant::ManualStop => TokenLifecycle::Completed,
                common::TokenTerminalVariant::Completed => TokenLifecycle::Completed,
            };
            let reason = terminal_variant_label(payload.variant);
            let _ = transition_token(
                token,
                target,
                reason,
                event.meta.received_at_wall_time,
                ttl_for_config(&self.ttl, target),
            );
            self.discarded_summaries.insert(
                token.mint.0.clone(),
                CompactTokenSummary {
                    mint: token.mint.0.clone(),
                    lifecycle: token.lifecycle,
                    latest_price: token.latest_price,
                    holder_count: token.holder_state.nonzero_holder_count,
                    top1_holder_pct: token.holder_state.top_holder_pct(1),
                    creator_sold_pct: token.developer_state.creator_sell_percentage,
                    canonical_trade_count: token.trade_stats.buy_count
                        + token.trade_stats.sell_count,
                    reason_codes: payload.reason_codes.clone(),
                },
            );
        }
    }

    fn apply_data_gap(&mut self, payload: &DataGapEvent, event: &NormalizedEvent) {
        for mint in &payload.affected_tokens {
            if let Some(token) = self.tokens.get_mut(&mint.0) {
                token.data_quality_flags.insert("data_gap".to_owned());
                let _ = transition_token(
                    token,
                    TokenLifecycle::DataGap,
                    "data_gap",
                    event.meta.received_at_wall_time,
                    ttl_for_config(&self.ttl, TokenLifecycle::DataGap),
                );
            }
        }
    }

    fn refresh_clusters(&mut self) {
        for wallet in self.wallets.values_mut() {
            wallet.cluster_id = self.cluster_index.cluster_id_for(&wallet.wallet);
        }
        for token in self.tokens.values_mut() {
            refresh_token_cluster_derived(token, &self.cluster_index);
        }
    }

    fn refresh_derived_indices_for_event(&mut self, event: &NormalizedEvent) {
        if let Some(affected_wallets) = self.cluster_affected_wallets(event) {
            self.refresh_clusters_for_wallets(&affected_wallets, event.meta.received_at_wall_time);
            return;
        }
        let Some(mint) = event.mint().map(|mint| mint.0.clone()) else {
            return;
        };
        let cluster_index = &self.cluster_index;
        if let Some(token) = self.tokens.get_mut(&mint) {
            refresh_token_cluster_derived(token, cluster_index);
            if event_updates_exit_threat_inputs(event) {
                refresh_exit_threat_index(token, cluster_index, event.meta.received_at_wall_time);
            }
        }
    }

    fn cluster_affected_wallets(&self, event: &NormalizedEvent) -> Option<BTreeSet<String>> {
        let mut wallets = BTreeSet::new();
        match &event.payload {
            EventPayload::TokenCreated(payload) => {
                wallets.insert(payload.creator_wallet.0.clone());
                wallets.insert(payload.payer.0.clone());
            }
            EventPayload::WalletFunding(payload) => {
                wallets.insert(payload.wallet.0.clone());
                wallets.extend(self.funding_graph.wallets_for_funder(&payload.funder.0));
            }
            _ => return None,
        }
        let mut expanded = BTreeSet::new();
        for wallet in wallets {
            expanded.extend(self.cluster_index.related_wallets(&wallet));
        }
        Some(expanded)
    }

    fn refresh_clusters_for_wallets(
        &mut self,
        affected_wallets: &BTreeSet<String>,
        observed_at: OffsetDateTime,
    ) {
        if affected_wallets.is_empty() {
            return;
        }
        for wallet in affected_wallets {
            if let Some(summary) = self.wallets.get_mut(wallet) {
                summary.cluster_id = self.cluster_index.cluster_id_for(wallet);
            }
        }
        let cluster_index = &self.cluster_index;
        for token in self.tokens.values_mut() {
            if token_touches_any_wallet(token, affected_wallets) {
                refresh_token_cluster_derived(token, cluster_index);
                refresh_exit_threat_index(token, cluster_index, observed_at);
            }
        }
    }

    fn recompute_transitions(&mut self, event: &NormalizedEvent) -> Vec<LifecycleTransition> {
        let Some(mint) = event.mint().map(|mint| mint.0.clone()) else {
            return Vec::new();
        };
        let mut transitions = Vec::new();
        if let Some(token) = self.tokens.get_mut(&mint) {
            let target = if token.lifecycle == TokenLifecycle::DataGap {
                TokenLifecycle::DataGap
            } else if token.trade_stats.buy_count + token.trade_stats.sell_count >= 4
                || token.holder_state.nonzero_holder_count >= 4
                || token.trade_stats.unique_buyers.len() >= 3
            {
                TokenLifecycle::ActiveDeep
            } else if token.trade_stats.buy_count + token.trade_stats.sell_count >= 2
                || token.holder_state.nonzero_holder_count >= 2
            {
                TokenLifecycle::ActiveLight
            } else if token.trade_stats.buy_count + token.trade_stats.sell_count >= 1 {
                TokenLifecycle::FirstPass
            } else {
                TokenLifecycle::Discovered
            };
            if token.lifecycle != target
                && !matches!(
                    token.lifecycle,
                    TokenLifecycle::SoftDiscarded
                        | TokenLifecycle::HardDiscarded
                        | TokenLifecycle::RugArchive
                        | TokenLifecycle::Migrated
                        | TokenLifecycle::Completed
                )
            {
                if let Some(transition) = transition_token(
                    token,
                    target,
                    "activity_threshold",
                    event.meta.received_at_wall_time,
                    ttl_for_config(&self.ttl, target),
                ) {
                    transitions.push(transition);
                }
            }
            token.expires_at =
                Some(event.meta.received_at_wall_time + ttl_for_config(&self.ttl, token.lifecycle));
            token.update_memory_bytes();
        }
        transitions
    }

    pub fn expire_tokens(&mut self, now: OffsetDateTime) -> Vec<CompactTokenSummary> {
        let expired: Vec<String> = self
            .tokens
            .iter()
            .filter_map(|(mint, token)| {
                token
                    .expires_at
                    .filter(|expires_at| *expires_at <= now)
                    .map(|_| mint.clone())
            })
            .collect();
        let mut summaries = Vec::new();
        for mint in expired {
            if let Some(token) = self.tokens.remove(&mint) {
                let summary = CompactTokenSummary {
                    mint: token.mint.0.clone(),
                    lifecycle: token.lifecycle,
                    latest_price: token.latest_price,
                    holder_count: token.holder_state.nonzero_holder_count,
                    top1_holder_pct: token.holder_state.top_holder_pct(1),
                    creator_sold_pct: token.developer_state.creator_sell_percentage,
                    canonical_trade_count: token.trade_stats.buy_count
                        + token.trade_stats.sell_count,
                    reason_codes: vec![ReasonCode::SoftDiscarded],
                };
                self.discarded_summaries
                    .insert(summary.mint.clone(), summary.clone());
                summaries.push(summary);
            }
        }
        summaries
    }

    fn touch_wallet(&mut self, wallet: &str, timestamp: OffsetDateTime) -> &mut WalletSummary {
        let entry = self
            .wallets
            .entry(wallet.to_owned())
            .or_insert(WalletSummary {
                wallet: wallet.to_owned(),
                ..WalletSummary::default()
            });
        if entry.first_seen.is_none() {
            entry.first_seen = Some(timestamp);
        }
        entry.last_seen = Some(timestamp);
        entry
    }

    fn refresh_exit_threat_indices(&mut self, observed_at: OffsetDateTime) {
        let cluster_index = self.cluster_index.clone();
        for token in self.tokens.values_mut() {
            refresh_exit_threat_index(token, &cluster_index, observed_at);
        }
    }
}

fn event_updates_exit_threat_inputs(event: &NormalizedEvent) -> bool {
    matches!(
        event.payload,
        EventPayload::TokenCreated(_)
            | EventPayload::PumpBuy(_)
            | EventPayload::PumpSell(_)
            | EventPayload::BondingCurveUpdate(_)
            | EventPayload::HolderBalanceUpdate(_)
            | EventPayload::TokenTerminal(_)
            | EventPayload::DataGap(_)
    )
}

fn terminal_variant_label(variant: common::TokenTerminalVariant) -> &'static str {
    match variant {
        common::TokenTerminalVariant::Discarded => "discarded",
        common::TokenTerminalVariant::SoftDiscarded => "soft_discarded",
        common::TokenTerminalVariant::HardDiscarded => "hard_discarded",
        common::TokenTerminalVariant::Rugged => "rugged",
        common::TokenTerminalVariant::Dead => "dead",
        common::TokenTerminalVariant::Migrated => "migrated",
        common::TokenTerminalVariant::TrackingExpired => "tracking_expired",
        common::TokenTerminalVariant::DataGap => "data_gap",
        common::TokenTerminalVariant::ManualStop => "manual_stop",
        common::TokenTerminalVariant::Completed => "completed",
    }
}

fn transition_token(
    token: &mut TokenState,
    target: TokenLifecycle,
    reason: &str,
    observed_at: OffsetDateTime,
    ttl: time::Duration,
) -> Option<LifecycleTransition> {
    if token.lifecycle == target {
        return None;
    }
    let from = token.lifecycle;
    token.lifecycle = target;
    token.expires_at = Some(observed_at + ttl);
    Some(LifecycleTransition {
        mint: token.mint.clone(),
        from,
        to: target,
        reason: reason.to_owned(),
        observed_at,
    })
}

fn ttl_for_config(ttl: &TtlConfig, lifecycle: TokenLifecycle) -> time::Duration {
    match lifecycle {
        TokenLifecycle::Discovered => time::Duration::seconds(ttl.discovered_secs as i64),
        TokenLifecycle::FirstPass | TokenLifecycle::ActiveLight => {
            time::Duration::seconds(ttl.active_light_secs as i64)
        }
        TokenLifecycle::ActiveDeep
        | TokenLifecycle::TradeCandidate
        | TokenLifecycle::InPosition
        | TokenLifecycle::ExitPending => time::Duration::seconds(ttl.active_deep_secs as i64),
        TokenLifecycle::SoftDiscarded | TokenLifecycle::HardDiscarded => {
            time::Duration::seconds(ttl.discarded_summary_secs as i64)
        }
        TokenLifecycle::RugArchive => time::Duration::seconds(ttl.research_sample_secs as i64),
        TokenLifecycle::Completed | TokenLifecycle::Migrated | TokenLifecycle::DataGap => {
            time::Duration::seconds(ttl.discarded_summary_secs as i64)
        }
    }
}

fn refresh_exit_threat_index(
    token: &mut TokenState,
    cluster_index: &ClusterIndex,
    observed_at: OffsetDateTime,
) {
    let total_supply_proxy: Decimal = token
        .holder_state
        .owner_balances
        .values()
        .map(|holder| holder.balance.max(Decimal::ZERO))
        .sum();
    let liquidity_depth = (token.reserve_state.real_quote_reserves
        + token.reserve_state.virtual_quote_reserves)
        .max(Decimal::from(1u64));
    let launch_price = token
        .reserve_state
        .launch_price
        .unwrap_or(token.latest_price.max(Decimal::ONE));
    let price = token.latest_price.max(launch_price).max(Decimal::new(1, 9));
    let creator_cluster = token.developer_state.related_cluster_id.clone();
    let mut dangerous_wallets = Vec::new();
    for (index, holder) in token.holder_state.top_holders.iter().take(10).enumerate() {
        let Some(balance_state) = token.holder_state.owner_balances.get(&holder.owner.0) else {
            continue;
        };
        let classification = classify_wallet(
            token,
            cluster_index,
            &holder.owner.0,
            index,
            holder.pct_supply_proxy,
            creator_cluster.as_deref(),
        );
        let balance = balance_state.balance.max(Decimal::ZERO);
        let cost_basis = balance_state
            .cost_basis
            .estimated_cost_basis_quote
            .max(Decimal::ZERO);
        let unrealized = price * balance - cost_basis;
        let cluster_id = cluster_index.cluster_id_for(&holder.owner.0);
        dangerous_wallets.push(ThreatWalletEntry {
            wallet: holder.owner.0.clone(),
            classification,
            balance,
            holding_pct: holder.pct_supply_proxy,
            estimated_full_exit_impact_pct: impact_pct(
                balance,
                total_supply_proxy,
                liquidity_depth,
            ),
            estimated_partial_exit_25_pct: impact_pct(
                balance * Decimal::new(25, 2),
                total_supply_proxy,
                liquidity_depth,
            ),
            estimated_partial_exit_50_pct: impact_pct(
                balance * Decimal::new(50, 2),
                total_supply_proxy,
                liquidity_depth,
            ),
            estimated_partial_exit_100_pct: impact_pct(
                balance,
                total_supply_proxy,
                liquidity_depth,
            ),
            cost_basis_quote: cost_basis,
            unrealized_pnl_quote: unrealized,
            free_roll: balance_state.cost_basis.estimated_realized_pnl
                > balance_state.cost_basis.original_quote_spent / Decimal::from(2u64),
            last_sell_time: token
                .trade_stats
                .trade_history
                .iter()
                .rev()
                .find(|trade| trade.wallet == holder.owner.0 && trade.side == TradeSide::Sell)
                .map(|trade| trade.timestamp),
            historical_rug_association: 0,
            cluster_id,
            cluster_holding_pct: holder.pct_supply_proxy,
        });
    }
    dangerous_wallets.sort_by(|left, right| {
        right
            .estimated_full_exit_impact_pct
            .partial_cmp(&left.estimated_full_exit_impact_pct)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    dangerous_wallets.truncate(16);
    let max_impact = dangerous_wallets
        .iter()
        .map(|entry| entry.estimated_full_exit_impact_pct)
        .max()
        .unwrap_or(Decimal::ZERO);
    let latest_vwap = token
        .trade_stats
        .trade_history
        .iter()
        .rev()
        .take(12)
        .map(|trade| trade.price)
        .sum::<Decimal>();
    let latest_vwap = if token.trade_stats.trade_history.is_empty() {
        price
    } else {
        latest_vwap
            / Decimal::from(
                token
                    .trade_stats
                    .trade_history
                    .iter()
                    .rev()
                    .take(12)
                    .count() as u64,
            )
    };
    let cost_basis_support = token
        .holder_state
        .owner_balances
        .values()
        .filter(|holder| holder.cost_basis.remaining_position_size > Decimal::ZERO)
        .map(|holder| holder.cost_basis.estimated_average_entry_price)
        .sum::<Decimal>();
    let cost_basis_support = if token.holder_state.owner_balances.is_empty() {
        price
    } else {
        cost_basis_support / Decimal::from(token.holder_state.owner_balances.len() as u64)
    };
    token.shred_defense.exit_threat_index = ExitThreatIndex {
        dangerous_wallets,
        warn_threshold_impact_pct: Decimal::from(8u64),
        arm_exit_threshold_impact_pct: Decimal::from(15u64),
        trigger_exit_threshold_impact_pct: Decimal::from(25u64),
        emergency_exit_threshold_impact_pct: Decimal::from(35u64),
        dangerous_seller_precomputed_impact_score: clamp01_decimal(
            max_impact / Decimal::from(100u64),
        ),
        exit_threat_index_score: clamp01_decimal(max_impact / Decimal::from(100u64)),
        minimum_dangerous_sell_size_to_trigger_emergency: total_supply_proxy * Decimal::new(25, 2),
        current_distance_to_stop_pct: (price - launch_price).max(Decimal::ZERO)
            / price.max(Decimal::ONE),
        current_distance_to_trailing_stop_pct: (token.trade_stats.all_time_high - price)
            .max(Decimal::ZERO)
            / token.trade_stats.all_time_high.max(Decimal::ONE),
        current_distance_to_launch_floor_pct: (price - launch_price).max(Decimal::ZERO)
            / price.max(Decimal::ONE),
        current_distance_to_vwap_pct: (price - latest_vwap).abs() / price.max(Decimal::ONE),
        current_distance_to_cost_basis_support_pct: (price - cost_basis_support).abs()
            / price.max(Decimal::ONE),
        combined_our_exit_plus_dangerous_sell_impact_pct: max_impact * Decimal::new(12, 2),
        max_safe_wait_time_ms: if max_impact >= Decimal::from(25u64) {
            250
        } else {
            750
        },
        required_early_intent_lead_time_ms: if max_impact >= Decimal::from(25u64) {
            120
        } else {
            350
        },
        updated_at: Some(observed_at),
    };
}

fn token_touches_any_wallet(token: &TokenState, wallets: &BTreeSet<String>) -> bool {
    token
        .creator
        .as_ref()
        .map(|wallet| wallets.contains(&wallet.0))
        .unwrap_or(false)
        || token
            .payer
            .as_ref()
            .map(|wallet| wallets.contains(&wallet.0))
            .unwrap_or(false)
        || token
            .holder_state
            .top_holders
            .iter()
            .take(10)
            .any(|holder| wallets.contains(&holder.owner.0))
        || token
            .holder_state
            .owner_balances
            .keys()
            .any(|wallet| wallets.contains(wallet))
}

fn refresh_token_cluster_derived(token: &mut TokenState, cluster_index: &ClusterIndex) {
    if let Some(creator) = &token.creator {
        token.developer_state.related_cluster_id = cluster_index.cluster_id_for(&creator.0);
    }
    token.recalc_developer_rank();
    token.developer_state.creator_sell_percentage =
        if token.developer_state.creator_initial_holding > Decimal::ZERO {
            let sold = token.developer_state.creator_initial_holding
                - token.developer_state.creator_net_tokens.max(Decimal::ZERO);
            sold / token.developer_state.creator_initial_holding
        } else {
            Decimal::ZERO
        };
}

fn classify_wallet(
    token: &TokenState,
    cluster_index: &ClusterIndex,
    wallet: &str,
    top_holder_index: usize,
    pct_supply_proxy: Decimal,
    creator_cluster: Option<&str>,
) -> DangerousSellerClassification {
    if token.creator.as_ref().map(|creator| creator.0.as_str()) == Some(wallet) {
        return DangerousSellerClassification::Dev;
    }
    if let (Some(cluster), Some(creator_cluster)) =
        (cluster_index.cluster_id_for(wallet), creator_cluster)
    {
        if cluster == creator_cluster {
            return DangerousSellerClassification::DevCluster;
        }
    }
    let same_funder_cluster = cluster_index.wallet_has_same_funder_cluster(wallet);
    if same_funder_cluster && top_holder_index <= 9 {
        return DangerousSellerClassification::BundleCluster;
    }
    if same_funder_cluster {
        return DangerousSellerClassification::SameFunderCluster;
    }
    match top_holder_index {
        0 => return DangerousSellerClassification::Top1Holder,
        1 | 2 => return DangerousSellerClassification::Top3Holder,
        3 | 4 => return DangerousSellerClassification::Top5Holder,
        5..=9 => return DangerousSellerClassification::Top10Holder,
        _ => {}
    }
    if pct_supply_proxy >= Decimal::new(15, 2) {
        return DangerousSellerClassification::Whale;
    }
    DangerousSellerClassification::Unknown
}

fn impact_pct(balance: Decimal, total_supply_proxy: Decimal, liquidity_depth: Decimal) -> Decimal {
    if balance <= Decimal::ZERO || total_supply_proxy <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let supply_share = balance / total_supply_proxy;
    let reserve_share = (balance * Decimal::new(5, 1)) / liquidity_depth.max(Decimal::ONE);
    clamp01_decimal((supply_share * Decimal::from(70u64)) + (reserve_share * Decimal::from(30u64)))
        * Decimal::from(100u64)
}

fn average_decimal(current_average: Decimal, next: Decimal, count: u64) -> Decimal {
    if count <= 1 {
        return next;
    }
    ((current_average * Decimal::from(count - 1)) + next) / Decimal::from(count)
}

fn clamp01_decimal(value: Decimal) -> Decimal {
    value.max(Decimal::ZERO).min(Decimal::ONE)
}

fn parse_seller_classification(value: &str) -> DangerousSellerClassification {
    match value {
        "dev" => DangerousSellerClassification::Dev,
        "dev_cluster" => DangerousSellerClassification::DevCluster,
        "top1_holder" => DangerousSellerClassification::Top1Holder,
        "top3_holder" => DangerousSellerClassification::Top3Holder,
        "top5_holder" => DangerousSellerClassification::Top5Holder,
        "top10_holder" => DangerousSellerClassification::Top10Holder,
        "bundle_wallet" => DangerousSellerClassification::BundleWallet,
        "bundle_cluster" => DangerousSellerClassification::BundleCluster,
        "whale" => DangerousSellerClassification::Whale,
        "same_funder_cluster" => DangerousSellerClassification::SameFunderCluster,
        "same_client_fingerprint_cluster" => {
            DangerousSellerClassification::SameClientFingerprintCluster
        }
        "high_pnl_holder" => DangerousSellerClassification::HighPnlHolder,
        "free_rolling_holder" => DangerousSellerClassification::FreeRollingHolder,
        _ => DangerousSellerClassification::Unknown,
    }
}

pub fn compute_gini(mut balances: Vec<Decimal>) -> Decimal {
    if balances.is_empty() {
        return Decimal::ZERO;
    }
    balances.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let n = Decimal::from(balances.len() as u64);
    let total: Decimal = balances.iter().copied().sum();
    if total <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let mut weighted_sum = Decimal::ZERO;
    for (index, balance) in balances.iter().enumerate() {
        weighted_sum += Decimal::from((index + 1) as u64) * *balance;
    }
    (Decimal::from(2u64) * weighted_sum) / (n * total) - (n + Decimal::ONE) / n
}

pub fn compute_hhi(balances: Vec<Decimal>) -> Decimal {
    let total: Decimal = balances.iter().copied().sum();
    if total <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    balances
        .into_iter()
        .map(|balance| {
            let share = balance / total;
            share * share
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use common::{
        BondingCurveUpdateEvent, EventMeta, EventPayload, EventSource, HolderBalanceUpdateEvent,
        NormalizedEvent, ObservedTransactionEvent, PumpBuyEvent, PumpSellEvent, QuoteAssetType,
        TokenCreatedEvent, TokenProgramType, TransactionStatus, TtlConfig,
    };

    use super::*;

    fn pubkey(value: &str) -> PubkeyValue {
        PubkeyValue(value.to_owned())
    }

    fn ttl() -> TtlConfig {
        TtlConfig {
            discovered_secs: 1,
            active_light_secs: 5,
            active_deep_secs: 10,
            discarded_summary_secs: 30,
            research_sample_secs: 60,
        }
    }

    fn meta(source: EventSource, canonicality: Canonicality, slot: u64) -> EventMeta {
        let mut meta = EventMeta::new(source, canonicality, slot);
        meta.received_at_wall_time =
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(slot as i64);
        meta
    }

    fn token_created() -> NormalizedEvent {
        let mut meta = meta(EventSource::GeyserProcessed, Canonicality::Processed, 1);
        meta.signature = Some("create-sig".to_owned());
        NormalizedEvent {
            meta,
            payload: EventPayload::TokenCreated(TokenCreatedEvent {
                mint: pubkey("mint"),
                token_program: TokenProgramType::SplToken,
                quote_mint: pubkey("quote"),
                quote_asset_type: QuoteAssetType::WrappedSol,
                creator_wallet: pubkey("creator"),
                payer: pubkey("payer"),
                bonding_curve_account: pubkey("curve"),
                associated_bonding_curve_account: None,
                metadata_account: None,
                name: "alpha".to_owned(),
                symbol: "ALP".to_owned(),
                uri: "https://example.invalid".to_owned(),
                create_instruction_variant: "create".to_owned(),
                initial_virtual_quote_reserves: None,
                initial_virtual_token_reserves: None,
                initial_real_quote_reserves: None,
                initial_real_token_reserves: None,
                initial_supply: Some(Decimal::from(1_000u64)),
                creator_initial_buy: None,
                same_transaction_buys: 0,
                same_slot_buys: 0,
                fee_recipients: vec![],
                raw_account_list: vec![],
                launch_transaction_fingerprint: Some("fp".to_owned()),
                status: common::TransactionStatus::Success,
            }),
        }
    }

    fn buy(signature: &str, buyer: &str, quote: u64, token_out: u64) -> NormalizedEvent {
        let mut meta = meta(EventSource::GeyserProcessed, Canonicality::Processed, 2);
        meta.signature = Some(signature.to_owned());
        NormalizedEvent {
            meta,
            payload: EventPayload::PumpBuy(PumpBuyEvent {
                mint: pubkey("mint"),
                buyer: pubkey(buyer),
                payer: pubkey(buyer),
                quote_in: Decimal::from(quote),
                token_out: Decimal::from(token_out),
                price_before: Some(Decimal::ONE),
                price_after: Some(Decimal::from(2u64)),
                effective_price: Decimal::from(2u64),
                slippage_estimate: None,
                reserves_before: None,
                reserves_after: None,
                max_quote_cost: None,
                compute_unit_limit: None,
                compute_unit_price: None,
                estimated_priority_fee_lamports: None,
                estimated_base_fee_lamports: None,
                estimated_tip_lamports: None,
                is_creator: buyer == "creator",
                is_known_cluster_member: false,
                is_first_buy: false,
                status: TransactionStatus::Success,
            }),
        }
    }

    fn sell(signature: &str, seller: &str, quote: u64, token_in: u64) -> NormalizedEvent {
        let mut meta = meta(EventSource::GeyserProcessed, Canonicality::Processed, 3);
        meta.signature = Some(signature.to_owned());
        NormalizedEvent {
            meta,
            payload: EventPayload::PumpSell(PumpSellEvent {
                mint: pubkey("mint"),
                seller: pubkey(seller),
                quote_out: Decimal::from(quote),
                token_in: Decimal::from(token_in),
                price_before: Some(Decimal::from(2u64)),
                price_after: Some(Decimal::ONE),
                effective_price: Decimal::ONE,
                slippage_estimate: None,
                reserves_before: None,
                reserves_after: None,
                min_quote_output: None,
                compute_unit_limit: None,
                compute_unit_price: None,
                estimated_priority_fee_lamports: None,
                estimated_base_fee_lamports: None,
                estimated_tip_lamports: None,
                is_creator: seller == "creator",
                is_top_holder_pre_sell: false,
                is_known_cluster_member: false,
                status: TransactionStatus::Success,
            }),
        }
    }

    fn holder_update(owner: &str, balance: u64) -> NormalizedEvent {
        holder_update_account(owner, &format!("token-account-{owner}"), balance)
    }

    fn holder_update_account(owner: &str, token_account: &str, balance: u64) -> NormalizedEvent {
        NormalizedEvent {
            meta: meta(EventSource::GeyserProcessed, Canonicality::Processed, 4),
            payload: EventPayload::HolderBalanceUpdate(HolderBalanceUpdateEvent {
                mint: pubkey("mint"),
                owner_wallet: pubkey(owner),
                token_account: pubkey(token_account),
                token_decimals: Some(DEFAULT_PUMP_TOKEN_DECIMALS),
                old_balance: None,
                new_balance: Decimal::from(balance),
                delta: Decimal::from(balance),
                caused_by_signature: None,
                update_reason: "trade".to_owned(),
                confidence: Decimal::ONE,
            }),
        }
    }

    fn holder_update_account_slot(
        owner: &str,
        token_account: &str,
        balance: u64,
        slot: u64,
        signature: &str,
    ) -> NormalizedEvent {
        let mut event = holder_update_account(owner, token_account, balance);
        event.meta.slot = slot;
        event.meta.signature = Some(signature.to_owned());
        event
    }

    #[test]
    fn exit_threat_recompute_is_limited_to_relevant_state_inputs() {
        assert!(event_updates_exit_threat_inputs(&buy(
            "b1", "buyer-a", 2, 100
        )));
        assert!(event_updates_exit_threat_inputs(&holder_update(
            "holder-a", 10
        )));
        let observed = NormalizedEvent {
            meta: meta(EventSource::ShredTentative, Canonicality::Tentative, 9),
            payload: EventPayload::ObservedTransaction(ObservedTransactionEvent {
                signature_hint: Some("sig".to_owned()),
                slot_hint: Some(9),
                entry_index: Some(0),
                tx_position_estimate: Some(0),
                signer: None,
                program_ids: vec!["pump".to_owned()],
                account_count: 4,
                instruction_count: 2,
                account_list_hash: None,
                instruction_shape_hash: None,
                compute_unit_limit: None,
                compute_unit_price: None,
                estimated_priority_fee_lamports: None,
                tx_fee_lamports: None,
                compute_units_consumed: None,
                pre_sol_balances_lamports: Vec::new(),
                post_sol_balances_lamports: Vec::new(),
                failed_transaction: false,
                error_code: None,
                bundle_like_evidence: None,
                raw_packet_hash: "hash".to_owned(),
                first_seen_by_shred_ns: 1,
                decode_confidence: Decimal::ONE,
            }),
        };
        assert!(!event_updates_exit_threat_inputs(&observed));
    }

    #[test]
    fn token_lifecycle_transitions() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        assert_eq!(
            engine.token(&pubkey("mint")).unwrap().lifecycle,
            TokenLifecycle::Discovered
        );
        let transitions = engine
            .apply_event(&buy("b1", "buyer-a", 2, 100))
            .expect("buy");
        assert_eq!(transitions[0].to, TokenLifecycle::FirstPass);
        let _ = engine
            .apply_event(&buy("b2", "buyer-b", 3, 110))
            .expect("buy");
        let _ = engine
            .apply_event(&sell("s1", "buyer-a", 1, 50))
            .expect("sell");
        assert_eq!(
            engine.token(&pubkey("mint")).unwrap().lifecycle,
            TokenLifecycle::ActiveLight
        );
    }

    #[test]
    fn bonding_curve_update_honors_write_version_ordering() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let update = |slot, write_version, price| NormalizedEvent {
            meta: meta(EventSource::GeyserProcessed, Canonicality::Processed, slot),
            payload: EventPayload::BondingCurveUpdate(BondingCurveUpdateEvent {
                mint: pubkey("mint"),
                virtual_quote_reserves: Decimal::from(1u64),
                virtual_token_reserves: Decimal::from(1u64),
                real_quote_reserves: Decimal::from(1u64),
                real_token_reserves: Decimal::from(1u64),
                token_decimals: Some(DEFAULT_PUMP_TOKEN_DECIMALS),
                price_lamports_per_raw_token: None,
                price_sol_per_token: Some(Decimal::from(price)),
                reserve_price_source: Some("test".to_owned()),
                reserve_price_confidence: Some(Decimal::ONE),
                price: Decimal::from(price),
                market_cap_quote_1b: None,
                market_cap_quote_total_supply: None,
                market_cap_source: None,
                market_cap_confidence: None,
                market_cap_proxy: None,
                curve_complete_flag: None,
                curve_progress_pct: None,
                curve_progress_source: None,
                curve_progress_confidence: None,
                curve_completion_pct: None,
                quote_reserve_delta: None,
                token_reserve_delta: None,
                update_reason: "trade".to_owned(),
                caused_by_signature: None,
                account_write_version: Some(write_version),
            }),
        };
        let _ = engine.apply_event(&update(10, 2, 5)).expect("apply");
        let _ = engine.apply_event(&update(10, 1, 3)).expect("apply");
        assert_eq!(
            engine.token(&pubkey("mint")).unwrap().latest_price,
            Decimal::from(5u64)
        );
    }

    #[test]
    fn holder_balance_updates_recompute_top_holders_and_distribution() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let _ = engine
            .apply_event(&holder_update("holder-a", 100))
            .expect("holder");
        let _ = engine
            .apply_event(&holder_update("holder-b", 50))
            .expect("holder");
        let token = engine.token(&pubkey("mint")).expect("token");
        assert_eq!(token.holder_state.nonzero_holder_count, 2);
        assert_eq!(token.holder_state.top_holders[0].owner.0, "holder-a");
        assert!(token.holder_state.gini > Decimal::ZERO);
        assert!(token.holder_state.hhi > Decimal::ZERO);
    }

    #[test]
    fn holder_balance_sums_multiple_token_accounts_per_owner() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let _ = engine
            .apply_event(&holder_update_account("holder-a", "ata-a-1", 100))
            .expect("holder");
        let _ = engine
            .apply_event(&holder_update_account("holder-a", "ata-a-2", 50))
            .expect("holder");
        let _ = engine
            .apply_event(&holder_update_account("holder-b", "ata-b-1", 125))
            .expect("holder");
        let token = engine.token(&pubkey("mint")).expect("token");
        assert_eq!(token.holder_state.nonzero_holder_count, 2);
        assert_eq!(
            token
                .holder_state
                .owner_balances
                .get("holder-a")
                .map(|holder| holder.balance),
            Some(Decimal::from(150u64))
        );
        assert_eq!(token.holder_state.top_holders[0].owner.0, "holder-a");
        assert_eq!(token.holder_state.missing_owner_mapping_count(), 0);
    }

    #[test]
    fn absolute_holder_update_replaces_previous_balance_without_adding() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let _ = engine
            .apply_event(&holder_update_account_slot(
                "holder-a", "ata-a-1", 100, 4, "sig-a",
            ))
            .expect("first holder update");
        let _ = engine
            .apply_event(&holder_update_account_slot(
                "holder-a", "ata-a-1", 125, 5, "sig-b",
            ))
            .expect("replacement holder update");
        let token = engine.token(&pubkey("mint")).expect("token");
        assert_eq!(
            token
                .holder_state
                .owner_balances
                .get("holder-a")
                .map(|holder| holder.balance),
            Some(Decimal::from(125u64))
        );
        assert_eq!(
            token.holder_state.observed_holder_supply(),
            Decimal::from(125u64)
        );
    }

    #[test]
    fn duplicate_holder_snapshot_does_not_increase_supply() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let event = holder_update_account_slot("holder-a", "ata-a-1", 100, 4, "sig-a");
        let _ = engine.apply_event(&event).expect("first holder update");
        let _ = engine.apply_event(&event).expect("duplicate holder update");
        let token = engine.token(&pubkey("mint")).expect("token");
        assert_eq!(
            token.holder_state.observed_holder_supply(),
            Decimal::from(100u64)
        );
        assert_eq!(token.holder_state.counters.holder_updates_seen, 2);
        assert_eq!(token.holder_state.counters.holder_updates_applied, 1);
        assert_eq!(token.holder_state.counters.holder_updates_deduped, 1);
    }

    #[test]
    fn realized_trade_price_is_sol_per_ui_token_not_lamports_per_raw_token() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let _ = engine
            .apply_event(&buy("buy-price", "holder-a", 1_000_000_000, 1_000_000))
            .expect("buy");
        assert_eq!(
            engine.token(&pubkey("mint")).unwrap().latest_price,
            Decimal::ONE
        );
        let _ = engine
            .apply_event(&sell("sell-price", "holder-a", 2_000_000_000, 1_000_000))
            .expect("sell");
        assert_eq!(
            engine.token(&pubkey("mint")).unwrap().latest_price,
            Decimal::from(2u64)
        );
    }

    #[test]
    fn token_account_owner_change_moves_balance_between_owners() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let _ = engine
            .apply_event(&holder_update_account_slot(
                "holder-a",
                "shared-token-account",
                100,
                4,
                "sig-a",
            ))
            .expect("holder-a update");
        let _ = engine
            .apply_event(&holder_update_account_slot(
                "holder-b",
                "shared-token-account",
                80,
                5,
                "sig-b",
            ))
            .expect("owner change update");
        let token = engine.token(&pubkey("mint")).expect("token");
        assert!(!token.holder_state.owner_balances.contains_key("holder-a"));
        assert_eq!(
            token
                .holder_state
                .owner_balances
                .get("holder-b")
                .map(|holder| holder.balance),
            Some(Decimal::from(80u64))
        );
        assert_eq!(
            token.holder_state.observed_holder_supply(),
            Decimal::from(80u64)
        );
        assert_eq!(token.holder_state.counters.holder_owner_changes, 1);
    }

    #[test]
    fn zero_balance_removes_positive_holder_count() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let _ = engine
            .apply_event(&holder_update_account_slot(
                "holder-a", "ata-a-1", 100, 4, "sig-a",
            ))
            .expect("holder update");
        let _ = engine
            .apply_event(&holder_update_account_slot(
                "holder-a", "ata-a-1", 0, 5, "sig-b",
            ))
            .expect("zero update");
        let token = engine.token(&pubkey("mint")).expect("token");
        assert_eq!(token.holder_state.nonzero_holder_count, 0);
        assert_eq!(token.holder_state.observed_holder_supply(), Decimal::ZERO);
        assert!(!token.holder_state.owner_balances.contains_key("holder-a"));
        assert_eq!(token.holder_state.exited_holder_count_zero_balance, 1);
        assert_eq!(token.holder_state.net_holder_change, -1);
    }

    #[test]
    fn sold_90pct_with_positive_balance_is_paperhand_not_holder_exit() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let _ = engine
            .apply_event(&holder_update_account_slot(
                "holder-a", "ata-a-1", 100, 4, "sig-a",
            ))
            .expect("holder update");
        let _ = engine.apply_event(&buy("buy-a", "holder-a", 100, 100));
        let _ = engine.apply_event(&sell("sell-a", "holder-a", 90, 90));
        let _ = engine
            .apply_event(&holder_update_account_slot(
                "holder-a", "ata-a-1", 10, 5, "sig-b",
            ))
            .expect("reduced holder update");
        let token = engine.token(&pubkey("mint")).expect("token");
        let holder = token
            .holder_state
            .owner_balances
            .get("holder-a")
            .expect("holder still present");
        assert_eq!(token.holder_state.nonzero_holder_count, 1);
        assert_eq!(holder.balance, Decimal::from(10u64));
        assert!(holder.wallet_sold_90pct_flag);
        assert!(matches!(
            holder.holder_retention_status,
            HolderRetentionStatus::Sold90Pct
        ));
        assert_eq!(token.holder_state.paperhand_90pct_wallet_count, 1);
        assert_eq!(token.holder_state.exited_holder_count_zero_balance, 0);
    }

    #[test]
    fn sell_to_zero_marks_exited_zero_balance_separate_from_paperhand() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let _ = engine
            .apply_event(&holder_update_account_slot(
                "holder-a", "ata-a-1", 100, 4, "sig-a",
            ))
            .expect("holder update");
        let _ = engine.apply_event(&buy("buy-a", "holder-a", 100, 100));
        let _ = engine.apply_event(&sell("sell-a", "holder-a", 100, 100));
        let _ = engine
            .apply_event(&holder_update_account_slot(
                "holder-a", "ata-a-1", 0, 5, "sig-b",
            ))
            .expect("zero holder update");
        let token = engine.token(&pubkey("mint")).expect("token");
        assert_eq!(token.holder_state.nonzero_holder_count, 0);
        assert_eq!(token.holder_state.exited_holder_count_zero_balance, 1);
        assert_eq!(token.holder_state.paperhand_90pct_wallet_count, 0);
    }

    #[test]
    fn curve_account_exclusion_removes_curve_from_holder_concentration() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let _ = engine
            .apply_event(&holder_update_account_slot(
                "curve",
                "curve-ata",
                900,
                4,
                "sig-a",
            ))
            .expect("curve update");
        let _ = engine
            .apply_event(&holder_update_account_slot(
                "holder-a", "ata-a-1", 100, 5, "sig-b",
            ))
            .expect("holder update");
        let token = engine.token(&pubkey("mint")).expect("token");
        let mut excluded = HashSet::new();
        excluded.insert("curve".to_owned());
        assert_eq!(token.holder_state.holder_count_excluding(&excluded), 1);
        assert_eq!(
            token.holder_state.top_holder_pct_with_denominator(
                1,
                Decimal::from(1_000u64),
                &excluded
            ),
            Decimal::new(1, 1)
        );
    }

    #[test]
    fn holder_invariant_reports_supply_over_total() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let _ = engine
            .apply_event(&holder_update_account_slot(
                "holder-a", "ata-a-1", 1_001, 4, "sig-a",
            ))
            .expect("holder update");
        let token = engine.token(&pubkey("mint")).expect("token");
        let violations = token
            .holder_state
            .holder_invariant_violations(Decimal::from(1_000u64), &HashSet::new());
        assert!(
            violations
                .iter()
                .any(|violation| violation
                    .contains("observed_owner_supply_raw_exceeds_total_supply"))
        );
    }

    #[test]
    fn dev_sell_detection_updates_developer_state() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let _ = engine
            .apply_event(&buy("b1", "creator", 5, 200))
            .expect("creator buy");
        let _ = engine
            .apply_event(&sell("s1", "creator", 2, 80))
            .expect("creator sell");
        let token = engine.token(&pubkey("mint")).expect("token");
        assert!(token.developer_state.creator_first_sell_time.is_some());
        assert!(token.developer_state.creator_sell_percentage > Decimal::ZERO);
    }

    #[test]
    fn wallet_index_and_funding_graph_update() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let funding = NormalizedEvent {
            meta: meta(EventSource::GeyserProcessed, Canonicality::Processed, 5),
            payload: EventPayload::WalletFunding(WalletFundingEvent {
                wallet: pubkey("buyer-a"),
                funder: pubkey("funder-1"),
                asset_label: "SOL".to_owned(),
                amount: Decimal::from(3u64),
                slot: 5,
                signature: "fund-1".to_owned(),
                relation_to_launch: Some("before_launch".to_owned()),
                near_launch_relation: true,
                funding_graph_edge_id: "edge-1".to_owned(),
            }),
        };
        let _ = engine.apply_event(&funding).expect("fund");
        assert!(engine.wallet(&pubkey("buyer-a")).is_some());
        assert_eq!(engine.snapshot().funding_graph.edges.len(), 1);
    }

    #[test]
    fn cluster_evidence_accumulates_on_same_funder() {
        let mut engine = StateEngine::new(ttl());
        let make_funding = |wallet: &str| NormalizedEvent {
            meta: meta(EventSource::GeyserProcessed, Canonicality::Processed, 6),
            payload: EventPayload::WalletFunding(WalletFundingEvent {
                wallet: pubkey(wallet),
                funder: pubkey("funder-z"),
                asset_label: "SOL".to_owned(),
                amount: Decimal::from(1u64),
                slot: 6,
                signature: format!("fund-{wallet}"),
                relation_to_launch: None,
                near_launch_relation: false,
                funding_graph_edge_id: format!("edge-{wallet}"),
            }),
        };
        let _ = engine
            .apply_event(&make_funding("wallet-a"))
            .expect("funding");
        let _ = engine
            .apply_event(&make_funding("wallet-b"))
            .expect("funding");
        assert_eq!(engine.cluster_index().edges.len(), 1);
    }

    #[test]
    fn tentative_to_canonical_correction_clears_tentative_flag() {
        let mut engine = StateEngine::new(ttl());
        let mut tentative = token_created();
        tentative.meta.source = EventSource::ShredTentative;
        tentative.meta.canonicality = Canonicality::Tentative;
        let _ = engine.apply_event(&tentative).expect("tentative");
        assert!(engine.token(&pubkey("mint")).unwrap().tentative_only);
        let _ = engine.apply_event(&token_created()).expect("canonical");
        assert!(!engine.token(&pubkey("mint")).unwrap().tentative_only);
    }

    #[test]
    fn ttl_expiration_preserves_compact_summary() {
        let mut engine = StateEngine::new(ttl());
        let _ = engine.apply_event(&token_created()).expect("create");
        let summaries =
            engine.expire_tokens(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(2));
        assert_eq!(summaries.len(), 1);
        assert!(engine.discarded_summary(&pubkey("mint")).is_some());
    }
}
