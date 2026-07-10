use std::{
    collections::{HashMap, VecDeque},
    str::FromStr,
    time::Duration as StdDuration,
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use common::{
    Canonicality, DataGapEvent, DataGapType, EventMeta, EventSource, GapSeverity, PubkeyValue,
    config::{CommitmentMode, GeyserConfig},
};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, warn};
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, CompiledInstruction, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterMemcmp,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions, SubscribeUpdate, SubscribeUpdateAccount,
    SubscribeUpdateBlock, SubscribeUpdateBlockMeta, SubscribeUpdateDeshredTransaction,
    SubscribeUpdateSlot, SubscribeUpdateTransaction,
    subscribe_request_filter_accounts_filter::Filter as AccountFilter,
    subscribe_request_filter_accounts_filter_memcmp::Data as MemcmpData,
    subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::prost_types::Timestamp;

#[derive(Debug, thiserror::Error)]
pub enum GeyserIngestError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("stream error: {0}")]
    Stream(String),
    #[error("publish error: {0}")]
    Publish(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SlotCommitment {
    Processed,
    Confirmed,
    Rooted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YellowstoneSubscriptionRequest {
    pub commitment: SlotCommitment,
    pub program_filters: Vec<String>,
    pub account_filters: Vec<String>,
    pub account_owner_filters: Vec<String>,
    pub account_data_size_filters: Vec<u64>,
    pub account_token_account_state_filter: bool,
    pub account_token_mint_offset: u64,
    pub account_token_mint_filters: Vec<String>,
    pub account_nonempty_txn_signature_required: bool,
    pub exact_holder_startup_snapshots_enabled: bool,
    pub exact_holder_nonempty_txn_signature_required: bool,
    pub max_inflight_messages: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeyserEnvelope {
    pub received_at: OffsetDateTime,
    pub observed_at_monotonic_ns: u64,
    pub source_latency_ms: Option<u64>,
    pub message: GeyserMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GeyserMessage {
    Slot(SlotUpdate),
    Transaction(TransactionUpdate),
    Account(AccountUpdate),
    Block(BlockUpdate),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotUpdate {
    pub slot: u64,
    pub parent: Option<u64>,
    pub status: SlotCommitment,
    pub block_time: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionInstruction {
    pub program_id: String,
    pub accounts: Vec<String>,
    pub data_hex: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TransactionInstructionGroup {
    pub index: u32,
    #[serde(default)]
    pub instructions: Vec<TransactionInstruction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TransactionTokenBalance {
    pub account_index: u32,
    pub mint: String,
    pub owner: Option<String>,
    pub program_id: Option<String>,
    pub amount: String,
    pub decimals: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionUpdate {
    pub slot: u64,
    pub signature: String,
    pub transaction_index: Option<u32>,
    pub succeeded: bool,
    #[serde(default)]
    pub error_code: Option<String>,
    pub account_keys: Vec<String>,
    pub instructions: Vec<TransactionInstruction>,
    #[serde(default)]
    pub inner_instructions: Vec<TransactionInstructionGroup>,
    #[serde(default)]
    pub pre_balances: Vec<u64>,
    #[serde(default)]
    pub post_balances: Vec<u64>,
    #[serde(default)]
    pub pre_token_balances: Vec<TransactionTokenBalance>,
    #[serde(default)]
    pub post_token_balances: Vec<TransactionTokenBalance>,
    #[serde(default)]
    pub loaded_writable_addresses: Vec<String>,
    #[serde(default)]
    pub loaded_readonly_addresses: Vec<String>,
    #[serde(default)]
    pub compute_units_consumed: Option<u64>,
    #[serde(default)]
    pub fee_lamports: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountUpdate {
    pub slot: u64,
    pub pubkey: String,
    pub owner: String,
    #[serde(default)]
    pub is_startup: bool,
    #[serde(default)]
    pub lamports: u64,
    #[serde(default)]
    pub executable: bool,
    pub write_version: u64,
    pub data_base64: String,
    pub transaction_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockUpdate {
    pub slot: u64,
    pub transaction_count: Option<u64>,
    pub block_time: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IngestOutput {
    Slot {
        meta: EventMeta,
        update: SlotUpdate,
    },
    Transaction {
        meta: EventMeta,
        update: TransactionUpdate,
    },
    Account {
        meta: EventMeta,
        update: AccountUpdate,
    },
    Block {
        meta: EventMeta,
        update: BlockUpdate,
    },
    DataGap {
        meta: EventMeta,
        update: DataGapEvent,
    },
}

#[async_trait]
pub trait YellowstoneClient {
    type MessageStream: Stream<Item = Result<GeyserEnvelope, GeyserIngestError>> + Send + Unpin;

    async fn subscribe(
        &mut self,
        request: YellowstoneSubscriptionRequest,
    ) -> Result<Self::MessageStream, GeyserIngestError>;
}

#[derive(Debug, Clone)]
pub struct YellowstoneEndpoint {
    pub channel: Channel,
}

impl YellowstoneEndpoint {
    pub async fn connect(config: &GeyserConfig) -> Result<Self, GeyserIngestError> {
        let keep_alive_interval = config
            .keep_alive_interval_seconds
            .map(StdDuration::from_secs)
            .unwrap_or_else(|| StdDuration::from_millis(config.keepalive_interval_ms.max(1)));
        let keep_alive_timeout =
            StdDuration::from_secs(config.keep_alive_timeout_seconds.unwrap_or(20).max(1));
        let mut endpoint = Endpoint::from_shared(config.endpoint.clone())?
            .connect_timeout(StdDuration::from_millis(config.connect_timeout_ms.max(1)))
            .timeout(StdDuration::from_millis(config.request_timeout_ms.max(1)))
            .http2_keep_alive_interval(keep_alive_interval)
            .keep_alive_timeout(keep_alive_timeout)
            .keep_alive_while_idle(config.keep_alive_while_idle)
            .tcp_nodelay(config.tcp_nodelay);
        if config.http2_adaptive_window {
            endpoint = endpoint.http2_adaptive_window(true);
        } else {
            if let Some(size) = config.initial_stream_window_size_bytes {
                endpoint = endpoint.initial_stream_window_size(size);
            }
            if let Some(size) = config.initial_connection_window_size_bytes {
                endpoint = endpoint.initial_connection_window_size(size);
            }
        }
        let channel = endpoint.connect().await?;
        Ok(Self { channel })
    }
}

#[derive(Debug, Clone, Default)]
pub struct StreamHealth {
    pub connected: bool,
    pub reconnect_count: u64,
    pub last_slot_seen: Option<u64>,
    pub recent_gap: Option<DataGapEvent>,
}

#[derive(Debug)]
pub struct GeyserIngestService {
    config: GeyserConfig,
    health: StreamHealth,
    dedup: Deduplicator,
    write_versions: HashMap<(u64, String), u64>,
}

impl GeyserIngestService {
    pub fn new(config: GeyserConfig) -> Self {
        Self {
            dedup: Deduplicator::new(config.max_inflight_messages.max(1)),
            config,
            health: StreamHealth::default(),
            write_versions: HashMap::new(),
        }
    }

    pub fn subscription_request(&self) -> YellowstoneSubscriptionRequest {
        YellowstoneSubscriptionRequest {
            commitment: match self.config.commitment {
                CommitmentMode::Processed => SlotCommitment::Processed,
                CommitmentMode::Confirmed => SlotCommitment::Confirmed,
                CommitmentMode::Finalized => SlotCommitment::Rooted,
            },
            program_filters: self.config.program_filters.clone(),
            account_filters: self.config.account_filters.clone(),
            account_owner_filters: self.config.resolved_account_owner_filters(),
            account_data_size_filters: self.config.account_data_size_filters.clone(),
            account_token_account_state_filter: self.config.account_token_account_state_filter,
            account_token_mint_offset: self.config.account_token_mint_offset,
            account_token_mint_filters: self.config.account_token_mint_filters.clone(),
            account_nonempty_txn_signature_required: self
                .config
                .account_nonempty_txn_signature_required,
            exact_holder_startup_snapshots_enabled: self
                .config
                .exact_holder_startup_snapshots_enabled,
            exact_holder_nonempty_txn_signature_required: self
                .config
                .exact_holder_nonempty_txn_signature_required,
            max_inflight_messages: self.config.max_inflight_messages,
        }
    }

    pub fn proto_subscription_request(&self) -> SubscribeRequest {
        let mut request = SubscribeRequest::default();
        request.commitment = Some(match self.config.commitment {
            CommitmentMode::Processed => CommitmentLevel::Processed as i32,
            CommitmentMode::Confirmed => CommitmentLevel::Confirmed as i32,
            CommitmentMode::Finalized => CommitmentLevel::Finalized as i32,
        });
        if self.config.subscribe_transactions {
            request.transactions.insert(
                "pump_programs".to_owned(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: None,
                    signature: None,
                    account_include: self.config.program_filters.clone(),
                    account_exclude: Vec::new(),
                    account_required: Vec::new(),
                },
            );
        }
        if self.config.subscribe_accounts {
            let account_owner_filters = self.config.resolved_account_owner_filters();
            if self.config.account_token_mint_filters.is_empty() {
                request.accounts.insert(
                    "pump_accounts".to_owned(),
                    SubscribeRequestFilterAccounts {
                        account: self.config.account_filters.clone(),
                        owner: account_owner_filters,
                        filters: self.account_proto_filters(None),
                        nonempty_txn_signature: self.account_nonempty_txn_signature_policy(),
                    },
                );
            } else {
                if !self.config.account_filters.is_empty() {
                    request.accounts.insert(
                        "pump_accounts_explicit".to_owned(),
                        SubscribeRequestFilterAccounts {
                            account: self.config.account_filters.clone(),
                            owner: account_owner_filters.clone(),
                            filters: self.account_proto_filters(None),
                            nonempty_txn_signature: self.account_nonempty_txn_signature_policy(),
                        },
                    );
                }
                for (idx, mint) in self.config.account_token_mint_filters.iter().enumerate() {
                    request.accounts.insert(
                        format!("token_mint_{idx}"),
                        SubscribeRequestFilterAccounts {
                            account: Vec::new(),
                            owner: account_owner_filters.clone(),
                            filters: self.account_proto_filters(Some(mint)),
                            nonempty_txn_signature: self
                                .exact_holder_nonempty_txn_signature_policy(),
                        },
                    );
                }
            }
        }
        if self.config.subscribe_slots {
            request.slots.insert(
                "slot_updates".to_owned(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(true),
                    interslot_updates: Some(true),
                },
            );
        }
        if self.config.subscribe_blocks {
            request.blocks.insert(
                "block_updates".to_owned(),
                SubscribeRequestFilterBlocks {
                    account_include: self.config.program_filters.clone(),
                    include_transactions: Some(true),
                    include_accounts: Some(self.config.subscribe_accounts),
                    include_entries: Some(false),
                },
            );
        }
        if self.config.subscribe_blocks_meta {
            request
                .blocks_meta
                .insert("block_meta".to_owned(), SubscribeRequestFilterBlocksMeta {});
        }
        request
    }

    fn account_nonempty_txn_signature_policy(&self) -> Option<bool> {
        Some(self.config.account_nonempty_txn_signature_required)
    }

    fn exact_holder_nonempty_txn_signature_policy(&self) -> Option<bool> {
        if self.config.exact_holder_startup_snapshots_enabled
            && !self.config.exact_holder_nonempty_txn_signature_required
        {
            None
        } else {
            Some(self.config.exact_holder_nonempty_txn_signature_required)
        }
    }

    fn account_proto_filters(
        &self,
        mint: Option<&str>,
    ) -> Vec<SubscribeRequestFilterAccountsFilter> {
        let mut filters = Vec::new();
        for data_size in &self.config.account_data_size_filters {
            filters.push(SubscribeRequestFilterAccountsFilter {
                filter: Some(AccountFilter::Datasize(*data_size)),
            });
        }
        if self.config.account_token_account_state_filter {
            filters.push(SubscribeRequestFilterAccountsFilter {
                filter: Some(AccountFilter::TokenAccountState(true)),
            });
        }
        if let Some(mint) = mint {
            filters.push(SubscribeRequestFilterAccountsFilter {
                filter: Some(AccountFilter::Memcmp(
                    SubscribeRequestFilterAccountsFilterMemcmp {
                        offset: self.config.account_token_mint_offset,
                        data: Some(MemcmpData::Base58(mint.to_owned())),
                    },
                )),
            });
        }
        filters
    }

    pub fn health(&self) -> &StreamHealth {
        &self.health
    }

    pub fn note_disconnect(&mut self) {
        self.health.connected = false;
        self.health.reconnect_count = self.health.reconnect_count.saturating_add(1);
    }

    pub fn process_envelope(&mut self, envelope: GeyserEnvelope) -> Vec<IngestOutput> {
        self.health.connected = true;
        let mut outputs = Vec::new();
        match &envelope.message {
            GeyserMessage::Slot(slot_update) => {
                if let Some(gap) = self.detect_gap(slot_update.slot, slot_update.parent) {
                    outputs.push(IngestOutput::DataGap {
                        meta: meta_for_gap(&envelope, slot_update.slot),
                        update: gap.clone(),
                    });
                    self.health.recent_gap = Some(gap);
                }
                self.health.last_slot_seen = Some(slot_update.slot);
                outputs.push(IngestOutput::Slot {
                    meta: meta_for_slot(&envelope, slot_update),
                    update: slot_update.clone(),
                });
            }
            GeyserMessage::Transaction(update) => {
                let key = DedupKey::Transaction {
                    slot: update.slot,
                    signature: update.signature.clone(),
                    tx_index: update.transaction_index.unwrap_or_default(),
                };
                if self.dedup.insert(key) {
                    outputs.push(IngestOutput::Transaction {
                        meta: meta_for_tx(&envelope, update),
                        update: update.clone(),
                    });
                } else {
                    debug!("suppressed duplicate transaction {}", update.signature);
                }
            }
            GeyserMessage::Account(update) => {
                let key = (update.slot, update.pubkey.clone());
                let is_newer = self
                    .write_versions
                    .get(&key)
                    .map(|last| update.write_version > *last)
                    .unwrap_or(true);
                if is_newer {
                    self.write_versions
                        .insert(key.clone(), update.write_version);
                    let dedup_key = DedupKey::Account {
                        slot: update.slot,
                        pubkey: update.pubkey.clone(),
                        write_version: update.write_version,
                    };
                    if self.dedup.insert(dedup_key) {
                        outputs.push(IngestOutput::Account {
                            meta: meta_for_account(&envelope, update),
                            update: update.clone(),
                        });
                    }
                } else {
                    warn!(
                        pubkey = update.pubkey,
                        slot = update.slot,
                        write_version = update.write_version,
                        "ignored stale or duplicate account update"
                    );
                }
            }
            GeyserMessage::Block(update) => outputs.push(IngestOutput::Block {
                meta: meta_for_block(&envelope, update),
                update: update.clone(),
            }),
        }
        outputs
    }

    pub async fn run_stream<S, F>(
        &mut self,
        mut stream: S,
        mut sink: F,
    ) -> Result<(), GeyserIngestError>
    where
        S: Stream<Item = Result<GeyserEnvelope, GeyserIngestError>> + Unpin,
        F: FnMut(IngestOutput) -> Result<(), GeyserIngestError>,
    {
        while let Some(item) = stream.next().await {
            let envelope = item?;
            for output in self.process_envelope(envelope) {
                sink(output)?;
            }
        }
        Ok(())
    }

    pub fn process_subscribe_update(
        &mut self,
        update: SubscribeUpdate,
        observed_at_monotonic_ns: u64,
    ) -> Vec<IngestOutput> {
        match envelope_from_subscribe_update(update, observed_at_monotonic_ns) {
            Some(envelope) => self.process_envelope(envelope),
            None => Vec::new(),
        }
    }

    fn detect_gap(&self, slot: u64, parent: Option<u64>) -> Option<DataGapEvent> {
        let Some(last) = self.health.last_slot_seen else {
            return None;
        };
        if slot
            > last
                .saturating_add(self.config.slot_gap_tolerance)
                .saturating_add(1)
        {
            return Some(DataGapEvent {
                gap_type: DataGapType::SlotGap,
                source: EventSource::GeyserProcessed,
                start_slot: last + 1,
                end_slot: Some(slot - 1),
                affected_tokens: Vec::<PubkeyValue>::new(),
                severity: GapSeverity::High,
                trade_allowed: false,
                recovery_action: "pause trading until gap is reconciled".to_owned(),
            });
        }
        if let Some(parent) = parent {
            if parent > last.saturating_add(self.config.slot_gap_tolerance) {
                return Some(DataGapEvent {
                    gap_type: DataGapType::ReconnectGap,
                    source: EventSource::GeyserProcessed,
                    start_slot: last + 1,
                    end_slot: Some(parent),
                    affected_tokens: Vec::<PubkeyValue>::new(),
                    severity: GapSeverity::Medium,
                    trade_allowed: false,
                    recovery_action: "reconnect and reconcile slot ancestry".to_owned(),
                });
            }
        }
        None
    }
}

pub fn envelope_from_subscribe_update(
    update: SubscribeUpdate,
    observed_at_monotonic_ns: u64,
) -> Option<GeyserEnvelope> {
    let received_at = update
        .created_at
        .as_ref()
        .and_then(timestamp_to_offset)
        .unwrap_or_else(OffsetDateTime::now_utc);
    let message = match update.update_oneof? {
        UpdateOneof::Slot(slot) => GeyserMessage::Slot(slot_update_from_proto(slot)),
        UpdateOneof::Transaction(tx) => {
            GeyserMessage::Transaction(transaction_update_from_proto(tx)?)
        }
        UpdateOneof::Account(account) => {
            GeyserMessage::Account(account_update_from_proto(account)?)
        }
        UpdateOneof::Block(block) => GeyserMessage::Block(block_update_from_proto(block)),
        UpdateOneof::BlockMeta(meta) => GeyserMessage::Block(block_meta_update_from_proto(meta)),
        UpdateOneof::TransactionStatus(status) => {
            GeyserMessage::Transaction(transaction_status_update_from_proto(status))
        }
        UpdateOneof::Ping(_) | UpdateOneof::Pong(_) | UpdateOneof::Entry(_) => return None,
    };
    Some(GeyserEnvelope {
        received_at,
        observed_at_monotonic_ns,
        source_latency_ms: None,
        message,
    })
}

pub fn timestamp_to_offset(timestamp: &Timestamp) -> Option<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp(timestamp.seconds)
        .ok()
        .and_then(|value| value.checked_add(time::Duration::nanoseconds(timestamp.nanos as i64)))
}

fn slot_update_from_proto(update: SubscribeUpdateSlot) -> SlotUpdate {
    let status = match update.status() {
        yellowstone_grpc_proto::prelude::SlotStatus::SlotConfirmed => SlotCommitment::Confirmed,
        yellowstone_grpc_proto::prelude::SlotStatus::SlotFinalized => SlotCommitment::Rooted,
        _ => SlotCommitment::Processed,
    };
    SlotUpdate {
        slot: update.slot,
        parent: update.parent,
        status,
        block_time: None,
    }
}

fn transaction_update_from_proto(update: SubscribeUpdateTransaction) -> Option<TransactionUpdate> {
    let info = update.transaction?;
    let tx = info.transaction?;
    let message = tx.message?;
    let meta = info.meta;
    let loaded_writable_addresses = meta
        .as_ref()
        .map(|value| {
            value
                .loaded_writable_addresses
                .iter()
                .map(|bytes| bytes_to_pubkey(bytes))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let loaded_readonly_addresses = meta
        .as_ref()
        .map(|value| {
            value
                .loaded_readonly_addresses
                .iter()
                .map(|bytes| bytes_to_pubkey(bytes))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut account_keys = message
        .account_keys
        .iter()
        .map(|bytes| bytes_to_pubkey(bytes))
        .collect::<Vec<_>>();
    account_keys.extend(loaded_writable_addresses.clone());
    account_keys.extend(loaded_readonly_addresses.clone());
    let instructions = message
        .instructions
        .iter()
        .map(|instruction| instruction_from_proto(instruction, &account_keys))
        .collect::<Vec<_>>();
    let inner_instructions = meta
        .as_ref()
        .map(|value| {
            value
                .inner_instructions
                .iter()
                .map(|group| TransactionInstructionGroup {
                    index: group.index,
                    instructions: group
                        .instructions
                        .iter()
                        .map(|instruction| {
                            let compiled = CompiledInstruction {
                                program_id_index: instruction.program_id_index,
                                accounts: instruction.accounts.clone(),
                                data: instruction.data.clone(),
                            };
                            instruction_from_proto(&compiled, &account_keys)
                        })
                        .collect(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Some(TransactionUpdate {
        slot: update.slot,
        signature: bytes_to_signature(&info.signature),
        transaction_index: Some(info.index as u32),
        succeeded: meta
            .as_ref()
            .map(|value| value.err.is_none())
            .unwrap_or(true),
        error_code: meta
            .as_ref()
            .and_then(|value| value.err.as_ref().map(|error| format!("{error:?}"))),
        account_keys,
        instructions,
        inner_instructions,
        pre_balances: meta
            .as_ref()
            .map(|value| value.pre_balances.clone())
            .unwrap_or_default(),
        post_balances: meta
            .as_ref()
            .map(|value| value.post_balances.clone())
            .unwrap_or_default(),
        pre_token_balances: meta
            .as_ref()
            .map(|value| {
                value
                    .pre_token_balances
                    .iter()
                    .map(token_balance_from_proto)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        post_token_balances: meta
            .as_ref()
            .map(|value| {
                value
                    .post_token_balances
                    .iter()
                    .map(token_balance_from_proto)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        loaded_writable_addresses,
        loaded_readonly_addresses,
        compute_units_consumed: meta.as_ref().and_then(|value| value.compute_units_consumed),
        fee_lamports: meta.as_ref().map(|value| value.fee).unwrap_or_default(),
    })
}

pub fn transaction_update_from_deshred_proto(
    update: SubscribeUpdateDeshredTransaction,
) -> Option<TransactionUpdate> {
    let info = update.transaction?;
    let tx = info.transaction?;
    let message = tx.message?;
    let loaded_writable_addresses = info
        .loaded_writable_addresses
        .iter()
        .map(|bytes| bytes_to_pubkey(bytes))
        .collect::<Vec<_>>();
    let loaded_readonly_addresses = info
        .loaded_readonly_addresses
        .iter()
        .map(|bytes| bytes_to_pubkey(bytes))
        .collect::<Vec<_>>();
    let mut account_keys = message
        .account_keys
        .iter()
        .map(|bytes| bytes_to_pubkey(bytes))
        .collect::<Vec<_>>();
    account_keys.extend(loaded_writable_addresses.clone());
    account_keys.extend(loaded_readonly_addresses.clone());
    let instructions = message
        .instructions
        .iter()
        .map(|instruction| instruction_from_proto(instruction, &account_keys))
        .collect::<Vec<_>>();
    Some(TransactionUpdate {
        slot: update.slot,
        signature: bytes_to_signature(&info.signature),
        transaction_index: None,
        succeeded: true,
        error_code: None,
        account_keys,
        instructions,
        inner_instructions: Vec::new(),
        pre_balances: Vec::new(),
        post_balances: Vec::new(),
        pre_token_balances: Vec::new(),
        post_token_balances: Vec::new(),
        loaded_writable_addresses,
        loaded_readonly_addresses,
        compute_units_consumed: None,
        fee_lamports: 0,
    })
}

fn transaction_status_update_from_proto(
    update: yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionStatus,
) -> TransactionUpdate {
    TransactionUpdate {
        slot: update.slot,
        signature: bytes_to_signature(&update.signature),
        transaction_index: Some(update.index as u32),
        succeeded: update.err.is_none(),
        error_code: update.err.as_ref().map(|error| format!("{error:?}")),
        account_keys: Vec::new(),
        instructions: Vec::new(),
        inner_instructions: Vec::new(),
        pre_balances: Vec::new(),
        post_balances: Vec::new(),
        pre_token_balances: Vec::new(),
        post_token_balances: Vec::new(),
        loaded_writable_addresses: Vec::new(),
        loaded_readonly_addresses: Vec::new(),
        compute_units_consumed: None,
        fee_lamports: 0,
    }
}

fn account_update_from_proto(update: SubscribeUpdateAccount) -> Option<AccountUpdate> {
    let account = update.account?;
    Some(AccountUpdate {
        slot: update.slot,
        pubkey: bytes_to_pubkey(&account.pubkey),
        owner: bytes_to_pubkey(&account.owner),
        is_startup: update.is_startup,
        lamports: account.lamports,
        executable: account.executable,
        write_version: account.write_version,
        data_base64: BASE64_STANDARD.encode(account.data),
        transaction_signature: account
            .txn_signature
            .map(|signature| bytes_to_signature(&signature)),
    })
}

fn block_update_from_proto(update: SubscribeUpdateBlock) -> BlockUpdate {
    BlockUpdate {
        slot: update.slot,
        transaction_count: Some(update.executed_transaction_count),
        block_time: update
            .block_time
            .as_ref()
            .and_then(unix_timestamp_to_offset),
    }
}

fn block_meta_update_from_proto(update: SubscribeUpdateBlockMeta) -> BlockUpdate {
    BlockUpdate {
        slot: update.slot,
        transaction_count: Some(update.executed_transaction_count),
        block_time: update
            .block_time
            .as_ref()
            .and_then(unix_timestamp_to_offset),
    }
}

fn unix_timestamp_to_offset(
    timestamp: &yellowstone_grpc_proto::prelude::UnixTimestamp,
) -> Option<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp(timestamp.timestamp).ok()
}

fn token_balance_from_proto(
    balance: &yellowstone_grpc_proto::prelude::TokenBalance,
) -> TransactionTokenBalance {
    TransactionTokenBalance {
        account_index: balance.account_index,
        mint: balance.mint.clone(),
        owner: (!balance.owner.is_empty()).then(|| balance.owner.clone()),
        program_id: (!balance.program_id.is_empty()).then(|| balance.program_id.clone()),
        amount: balance
            .ui_token_amount
            .as_ref()
            .map(|value| value.amount.clone())
            .unwrap_or_default(),
        decimals: balance
            .ui_token_amount
            .as_ref()
            .map(|value| value.decimals)
            .unwrap_or_default(),
    }
}

fn instruction_from_proto(
    instruction: &CompiledInstruction,
    account_keys: &[String],
) -> TransactionInstruction {
    let program_id = account_keys
        .get(instruction.program_id_index as usize)
        .cloned()
        .unwrap_or_else(|| "unknown_program".to_owned());
    let accounts = instruction
        .accounts
        .iter()
        .map(|index| {
            account_keys
                .get(*index as usize)
                .cloned()
                .unwrap_or_else(|| format!("unknown_account_{index}"))
        })
        .collect();
    TransactionInstruction {
        program_id,
        accounts,
        data_hex: hex::encode(&instruction.data),
    }
}

fn bytes_to_pubkey(bytes: &[u8]) -> String {
    bs58::encode(bytes).into_string()
}

fn bytes_to_signature(bytes: &[u8]) -> String {
    bs58::encode(bytes).into_string()
}

fn meta_for_slot(envelope: &GeyserEnvelope, slot: &SlotUpdate) -> EventMeta {
    let source = match slot.status {
        SlotCommitment::Processed => EventSource::GeyserProcessed,
        SlotCommitment::Confirmed => EventSource::GeyserConfirmed,
        SlotCommitment::Rooted => EventSource::GeyserRooted,
    };
    let canonicality = match slot.status {
        SlotCommitment::Processed => Canonicality::Processed,
        SlotCommitment::Confirmed => Canonicality::Confirmed,
        SlotCommitment::Rooted => Canonicality::Rooted,
    };
    let mut meta = EventMeta::new(source, canonicality, slot.slot);
    meta.parent_slot = slot.parent;
    meta.block_time = slot.block_time;
    meta.received_at_wall_time = envelope.received_at;
    meta.observed_at_monotonic_ns = envelope.observed_at_monotonic_ns;
    meta.source_latency_ms = envelope.source_latency_ms;
    meta
}

fn meta_for_tx(envelope: &GeyserEnvelope, tx: &TransactionUpdate) -> EventMeta {
    let mut meta = EventMeta::new(
        EventSource::GeyserProcessed,
        Canonicality::Processed,
        tx.slot,
    );
    meta.signature = Some(tx.signature.clone());
    meta.transaction_index = tx.transaction_index;
    meta.received_at_wall_time = envelope.received_at;
    meta.observed_at_monotonic_ns = envelope.observed_at_monotonic_ns;
    meta.source_latency_ms = envelope.source_latency_ms;
    meta
}

fn meta_for_account(envelope: &GeyserEnvelope, account: &AccountUpdate) -> EventMeta {
    let mut meta = EventMeta::new(
        EventSource::GeyserProcessed,
        Canonicality::Processed,
        account.slot,
    );
    meta.account_pubkey = PubkeyValue::from_str(&account.pubkey).ok();
    meta.account_write_version = Some(account.write_version);
    meta.signature = account.transaction_signature.clone();
    meta.received_at_wall_time = envelope.received_at;
    meta.observed_at_monotonic_ns = envelope.observed_at_monotonic_ns;
    meta.source_latency_ms = envelope.source_latency_ms;
    meta
}

fn meta_for_block(envelope: &GeyserEnvelope, block: &BlockUpdate) -> EventMeta {
    let mut meta = EventMeta::new(
        EventSource::GeyserProcessed,
        Canonicality::Processed,
        block.slot,
    );
    meta.block_time = block.block_time;
    meta.received_at_wall_time = envelope.received_at;
    meta.observed_at_monotonic_ns = envelope.observed_at_monotonic_ns;
    meta.source_latency_ms = envelope.source_latency_ms;
    meta
}

fn meta_for_gap(envelope: &GeyserEnvelope, slot: u64) -> EventMeta {
    let mut meta = EventMeta::new(EventSource::GeyserProcessed, Canonicality::Unknown, slot);
    meta.received_at_wall_time = envelope.received_at;
    meta.observed_at_monotonic_ns = envelope.observed_at_monotonic_ns;
    meta.source_latency_ms = envelope.source_latency_ms;
    meta
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum DedupKey {
    Transaction {
        slot: u64,
        signature: String,
        tx_index: u32,
    },
    Account {
        slot: u64,
        pubkey: String,
        write_version: u64,
    },
}

#[derive(Debug)]
struct Deduplicator {
    seen: HashMap<DedupKey, ()>,
    queue: VecDeque<DedupKey>,
    capacity: usize,
}

impl Deduplicator {
    fn new(capacity: usize) -> Self {
        Self {
            seen: HashMap::new(),
            queue: VecDeque::new(),
            capacity,
        }
    }

    fn insert(&mut self, key: DedupKey) -> bool {
        if self.seen.contains_key(&key) {
            return false;
        }
        self.seen.insert(key.clone(), ());
        self.queue.push_back(key);
        while self.queue.len() > self.capacity {
            if let Some(evicted) = self.queue.pop_front() {
                self.seen.remove(&evicted);
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use common::config::LoadedConfig;
    use futures::stream;
    use time::OffsetDateTime;
    use yellowstone_grpc_proto::prelude::{
        subscribe_request_filter_accounts_filter::Filter as AccountFilter,
        subscribe_request_filter_accounts_filter_memcmp::Data as MemcmpData,
    };

    use super::{
        AccountUpdate, GeyserEnvelope, GeyserIngestService, GeyserMessage, IngestOutput,
        SlotCommitment, SlotUpdate, TransactionInstruction, TransactionUpdate,
    };

    fn service() -> GeyserIngestService {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("config")
            .join("default.toml");
        let loaded = LoadedConfig::from_file(path).expect("config");
        GeyserIngestService::new(loaded.config.geyser)
    }

    #[test]
    fn builds_subscription_request_from_config() {
        let service = service();
        let request = service.subscription_request();
        assert_eq!(request.commitment, SlotCommitment::Processed);
        assert!(
            request
                .program_filters
                .iter()
                .any(|program| program == "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
        );
        assert!(
            request
                .program_filters
                .iter()
                .any(|program| program == "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA")
        );
        assert_eq!(request.account_owner_filters, request.program_filters);
    }

    #[test]
    fn account_owner_filters_can_target_spl_token_vault_accounts() {
        let mut service = service();
        service.config.account_filters = vec!["vault-token-account".to_owned()];
        service.config.account_owner_filters =
            vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned()];
        let request = service.subscription_request();
        assert_eq!(request.account_filters, vec!["vault-token-account"]);
        assert_eq!(
            request.account_owner_filters,
            vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]
        );

        let proto = service.proto_subscription_request();
        let account_filter = proto
            .accounts
            .get("pump_accounts")
            .expect("account subscription configured");
        assert_eq!(account_filter.account, vec!["vault-token-account"]);
        assert_eq!(
            account_filter.owner,
            vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]
        );
    }

    #[test]
    fn normal_program_account_filter_can_keep_nonempty_txn_signature_policy() {
        let mut service = service();
        service.config.account_filters = vec!["vault-token-account".to_owned()];
        service.config.account_owner_filters =
            vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned()];
        service.config.account_nonempty_txn_signature_required = true;

        let proto = service.proto_subscription_request();
        let account_filter = proto
            .accounts
            .get("pump_accounts")
            .expect("normal account subscription configured");

        assert_eq!(account_filter.nonempty_txn_signature, Some(true));
    }

    #[test]
    fn exact_holder_mint_filter_allows_startup_snapshots() {
        let mut service = service();
        service.config.account_filters = Vec::new();
        service.config.account_owner_filters =
            vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned()];
        service.config.account_data_size_filters = vec![165];
        service.config.account_token_account_state_filter = true;
        service.config.account_token_mint_offset = 0;
        service.config.account_token_mint_filters = vec!["Mint111".to_owned()];
        service.config.exact_holder_startup_snapshots_enabled = true;
        service.config.exact_holder_nonempty_txn_signature_required = false;

        let proto = service.proto_subscription_request();
        let filter = proto
            .accounts
            .get("token_mint_0")
            .expect("mint-scoped account subscription");

        assert_eq!(filter.nonempty_txn_signature, None);
    }

    #[test]
    fn account_token_mint_filters_build_one_account_subscription_per_mint() {
        let mut service = service();
        service.config.account_filters = Vec::new();
        service.config.account_owner_filters =
            vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned()];
        service.config.account_data_size_filters = vec![165];
        service.config.account_token_account_state_filter = true;
        service.config.account_token_mint_offset = 0;
        service.config.account_token_mint_filters =
            vec!["Mint111".to_owned(), "Mint222".to_owned()];

        let request = service.subscription_request();
        assert_eq!(
            request.account_token_mint_filters,
            vec!["Mint111", "Mint222"]
        );
        assert_eq!(request.account_data_size_filters, vec![165]);
        assert!(request.account_token_account_state_filter);

        let proto = service.proto_subscription_request();
        assert!(!proto.accounts.contains_key("pump_accounts"));
        for (idx, mint) in ["Mint111", "Mint222"].iter().enumerate() {
            let filter = proto
                .accounts
                .get(&format!("token_mint_{idx}"))
                .expect("mint-scoped account subscription");
            assert_eq!(
                filter.owner,
                vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]
            );
            assert_eq!(filter.nonempty_txn_signature, None);
            assert_eq!(filter.filters.len(), 3);
            assert!(matches!(
                filter.filters[0].filter,
                Some(AccountFilter::Datasize(165))
            ));
            assert!(matches!(
                filter.filters[1].filter,
                Some(AccountFilter::TokenAccountState(true))
            ));
            match &filter.filters[2].filter {
                Some(AccountFilter::Memcmp(memcmp)) => {
                    assert_eq!(memcmp.offset, 0);
                    assert_eq!(memcmp.data, Some(MemcmpData::Base58((*mint).to_owned())));
                }
                other => panic!("expected mint memcmp filter, got {other:?}"),
            }
        }
    }

    #[test]
    fn exact_holder_mint_filter_uses_spl_token_owner_datasize_and_mint_memcmp() {
        let mut service = service();
        service.config.account_filters = Vec::new();
        service.config.account_owner_filters =
            vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned()];
        service.config.account_data_size_filters = vec![165];
        service.config.account_token_account_state_filter = true;
        service.config.account_token_mint_offset = 0;
        service.config.account_token_mint_filters = vec!["MintExactHolder".to_owned()];

        let proto = service.proto_subscription_request();
        let filter = proto
            .accounts
            .get("token_mint_0")
            .expect("mint-scoped account subscription");

        assert_eq!(
            filter.owner,
            vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]
        );
        assert!(matches!(
            filter.filters[0].filter,
            Some(AccountFilter::Datasize(165))
        ));
        assert!(matches!(
            filter.filters[1].filter,
            Some(AccountFilter::TokenAccountState(true))
        ));
        match &filter.filters[2].filter {
            Some(AccountFilter::Memcmp(memcmp)) => {
                assert_eq!(memcmp.offset, 0);
                assert_eq!(
                    memcmp.data,
                    Some(MemcmpData::Base58("MintExactHolder".to_owned()))
                );
            }
            other => panic!("expected mint memcmp filter, got {other:?}"),
        }
    }

    #[test]
    fn deduplicates_transactions() {
        let mut service = service();
        let tx = TransactionUpdate {
            slot: 10,
            signature: "sig-1".to_owned(),
            transaction_index: Some(1),
            succeeded: true,
            error_code: None,
            account_keys: vec![],
            instructions: vec![TransactionInstruction {
                program_id: "Pump111111111111111111111111111111111111111".to_owned(),
                accounts: vec![],
                data_hex: "".to_owned(),
            }],
            inner_instructions: Vec::new(),
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            compute_units_consumed: None,
            fee_lamports: 0,
        };
        let envelope = GeyserEnvelope {
            received_at: OffsetDateTime::UNIX_EPOCH,
            observed_at_monotonic_ns: 42,
            source_latency_ms: Some(3),
            message: GeyserMessage::Transaction(tx.clone()),
        };
        assert_eq!(service.process_envelope(envelope.clone()).len(), 1);
        assert!(service.process_envelope(envelope).is_empty());
    }

    #[test]
    fn honors_write_version_ordering() {
        let mut service = service();
        let first = AccountUpdate {
            slot: 11,
            pubkey: "11111111111111111111111111111111".to_owned(),
            owner: "11111111111111111111111111111111".to_owned(),
            is_startup: false,
            lamports: 1,
            executable: false,
            write_version: 10,
            data_base64: "".to_owned(),
            transaction_signature: None,
        };
        let stale = AccountUpdate {
            write_version: 9,
            ..first.clone()
        };
        let first_outputs = service.process_envelope(GeyserEnvelope {
            received_at: OffsetDateTime::UNIX_EPOCH,
            observed_at_monotonic_ns: 1,
            source_latency_ms: None,
            message: GeyserMessage::Account(first),
        });
        let stale_outputs = service.process_envelope(GeyserEnvelope {
            received_at: OffsetDateTime::UNIX_EPOCH,
            observed_at_monotonic_ns: 2,
            source_latency_ms: None,
            message: GeyserMessage::Account(stale),
        });
        assert_eq!(first_outputs.len(), 1);
        assert!(stale_outputs.is_empty());
    }

    #[test]
    fn emits_gap_event_on_slot_jump() {
        let mut service = service();
        let first = GeyserEnvelope {
            received_at: OffsetDateTime::UNIX_EPOCH,
            observed_at_monotonic_ns: 1,
            source_latency_ms: None,
            message: GeyserMessage::Slot(SlotUpdate {
                slot: 100,
                parent: Some(99),
                status: SlotCommitment::Processed,
                block_time: None,
            }),
        };
        let second = GeyserEnvelope {
            received_at: OffsetDateTime::UNIX_EPOCH,
            observed_at_monotonic_ns: 2,
            source_latency_ms: None,
            message: GeyserMessage::Slot(SlotUpdate {
                slot: 104,
                parent: Some(103),
                status: SlotCommitment::Processed,
                block_time: None,
            }),
        };
        service.process_envelope(first);
        let outputs = service.process_envelope(second);
        assert!(
            outputs
                .iter()
                .any(|item| matches!(item, IngestOutput::DataGap { .. }))
        );
    }

    #[tokio::test]
    async fn runs_async_stream() {
        let mut service = service();
        let envelopes = vec![Ok(GeyserEnvelope {
            received_at: OffsetDateTime::UNIX_EPOCH,
            observed_at_monotonic_ns: 1,
            source_latency_ms: None,
            message: GeyserMessage::Slot(SlotUpdate {
                slot: 1,
                parent: None,
                status: SlotCommitment::Processed,
                block_time: None,
            }),
        })];
        let mut seen = Vec::new();
        service
            .run_stream(stream::iter(envelopes), |output| {
                seen.push(output);
                Ok(())
            })
            .await
            .expect("stream run");
        assert_eq!(seen.len(), 1);
    }
}
