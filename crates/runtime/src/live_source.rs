use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::Duration as StdDuration,
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use base64::Engine as _;
use common::{
    BondingCurveUpdateEvent, Canonicality, DEFAULT_PUMP_TOKEN_DECIMALS, DataGapEvent, DataGapType,
    EventMeta, EventPayload, EventSource, GapSeverity, HolderBalanceUpdateEvent, Lamports,
    LoadedConfig, NormalizedEvent, ObservedTransactionEvent, PUMP_TOTAL_SUPPLY_UI, PubkeyValue,
    PumpBuyEvent, PumpSellEvent, QuoteAssetType, RawEventReference, TokenCreatedEvent,
    TokenProgramType, TransactionStatus, WalletFundingEvent, monotonic_now_ns,
    price_lamports_per_raw_token, pump_curve_progress_pct_from_real_token_reserves_raw,
    pump_market_cap_quote_1b, pump_market_cap_quote_total_supply,
    pump_virtual_reserve_price_sol_per_token, raw_tokens_to_ui,
};
use futures::Stream;
use idl::{AccountDecode, DecodedAccount, InstructionDecode, LoadedIdl, anchor_discriminator};
use ingest_geyser::{
    AccountUpdate, GeyserIngestService, IngestOutput, TransactionInstruction,
    TransactionTokenBalance, TransactionUpdate, YellowstoneEndpoint,
    transaction_update_from_deshred_proto,
};
use rust_decimal::Decimal;
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{
    Request, Status,
    metadata::{Ascii, MetadataKey, MetadataValue},
    service::Interceptor,
};
use tracing::warn;
use yellowstone_grpc_proto::prelude::{
    SubscribeDeshredRequest, SubscribeRequest, SubscribeRequestFilterDeshredTransactions,
    SubscribeRequestPing, SubscribeUpdate, SubscribeUpdateDeshred, geyser_client::GeyserClient,
    subscribe_update::UpdateOneof, subscribe_update_deshred,
};
use yellowstone_grpc_proto::prost::Message as _;

use crate::{resolved_geyser_endpoint, resolved_geyser_metadata};

const SYSTEM_PROGRAM_ID: &str = "11111111111111111111111111111111";
const COMPUTE_BUDGET_PROGRAM_ID: &str = "ComputeBudget111111111111111111111111111111";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const PUMP_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

pub type SubscribeUpdateStream =
    Pin<Box<dyn Stream<Item = std::result::Result<SubscribeUpdate, Status>> + Send>>;
pub type SubscribeDeshredUpdateStream =
    Pin<Box<dyn Stream<Item = std::result::Result<SubscribeUpdateDeshred, Status>> + Send>>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct DeshredCapability {
    pub supported_by_proto: bool,
    pub supported_by_client: bool,
    pub supports_program_filters: bool,
    pub supports_account_filters: bool,
    pub exposes_loaded_addresses: bool,
    pub exposes_signature: bool,
    pub exposes_slot: bool,
    pub exposes_raw_transaction: bool,
    pub exposes_instruction_data: bool,
    pub exposes_transaction_status_meta: bool,
    pub endpoint_configured: bool,
    pub auth_configured: bool,
    pub can_enable: bool,
    pub reason_if_unsupported: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct DeshredProviderSmokeOptions {
    pub duration_seconds: u64,
    pub max_updates: Option<usize>,
    pub require_deshred: bool,
    pub strict: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct DeshredProviderSmokeSummary {
    pub endpoint_configured: bool,
    pub auth_configured: bool,
    pub auth_metadata_key_configured: bool,
    pub proto_support: bool,
    pub client_support: bool,
    pub provider_status: String,
    pub duration_seconds: u64,
    pub updates_received: u64,
    pub tentative_transactions_decoded: u64,
    pub pump_relevant_transactions: u64,
    pub tentative_sells_detected: u64,
    pub decoded_sell_instructions: u64,
    pub malicious_warnings: u64,
    pub emergency_exits_armed: u64,
    pub paper_emergency_exits_triggered: u64,
    pub geyser_reconciliations: u64,
    pub errors: Vec<String>,
    pub limitations: Vec<String>,
    pub no_live_orders: bool,
    pub rpc_calls_used: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct GeyserProviderSmokeOptions {
    pub duration_seconds: u64,
    pub max_updates: Option<usize>,
    pub strict: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct GeyserProviderSmokeSummary {
    pub endpoint_configured: bool,
    pub auth_configured: bool,
    pub provider_status: String,
    pub connected: bool,
    pub duration_seconds: u64,
    pub transaction_updates: u64,
    pub account_updates: u64,
    pub slot_updates: u64,
    pub block_updates: u64,
    pub block_meta_updates: u64,
    pub pump_relevant_transactions: u64,
    pub pump_create_decoded: u64,
    pub pump_buy_decoded: u64,
    pub pump_sell_decoded: u64,
    pub bonding_curve_updates: u64,
    pub holder_updates: u64,
    pub funding_events: u64,
    pub decode_errors: u64,
    pub unknown_instructions: u64,
    pub errors: Vec<String>,
    pub limitations: Vec<String>,
    pub no_live_orders: bool,
    pub rpc_calls_used: u64,
}

#[async_trait]
pub trait GeyserStreamConnector: Send + Sync {
    async fn connect_and_subscribe(
        &self,
        config: &common::GeyserConfig,
        request: SubscribeRequest,
    ) -> Result<SubscribeUpdateStream>;
}

#[async_trait]
pub trait DeshredStreamConnector: Send + Sync {
    async fn connect_and_subscribe(
        &self,
        config: &common::DeshredConfig,
        request: SubscribeDeshredRequest,
    ) -> Result<SubscribeDeshredUpdateStream>;
}

#[derive(Debug, Clone, Default)]
pub struct RealGeyserConnector;

#[derive(Debug, Clone, Default)]
pub struct RealDeshredConnector;

#[async_trait]
impl GeyserStreamConnector for RealGeyserConnector {
    async fn connect_and_subscribe(
        &self,
        config: &common::GeyserConfig,
        request: SubscribeRequest,
    ) -> Result<SubscribeUpdateStream> {
        let endpoint = resolved_geyser_endpoint(config)?;
        let mut resolved = config.clone();
        resolved.endpoint = endpoint.clone();
        let channel = YellowstoneEndpoint::connect(&resolved).await?.channel;
        let max_size = resolved
            .max_decoding_message_size_bytes
            .unwrap_or(resolved.max_decoded_message_size)
            .max(1024 * 1024);
        let auth = resolved_geyser_metadata(&resolved)?;
        let interceptor = match auth {
            Some((key, value)) => MetadataInjector::new(Some((&key, &value)))?,
            None => MetadataInjector::new(None)?,
        };
        let mut client = GeyserClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(max_size);

        let (request_tx, request_rx) = mpsc::channel::<SubscribeRequest>(4);
        request_tx
            .send(request)
            .await
            .map_err(|_| anyhow!("failed to seed geyser subscription request"))?;
        if resolved.ping_interval_ms > 0 {
            let ping_tx = request_tx.clone();
            let ping_every = resolved.ping_interval_ms.max(1);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(StdDuration::from_millis(ping_every));
                let mut counter = 1u64;
                loop {
                    interval.tick().await;
                    let ping = SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: counter as i32 }),
                        ..SubscribeRequest::default()
                    };
                    counter = counter.saturating_add(1);
                    if ping_tx.send(ping).await.is_err() {
                        break;
                    }
                }
            });
        }
        let stream = ReceiverStream::new(request_rx);
        let response: tonic::Response<tonic::codec::Streaming<SubscribeUpdate>> =
            client.subscribe(stream).await?;
        Ok(Box::pin(response.into_inner()))
    }
}

#[async_trait]
impl DeshredStreamConnector for RealDeshredConnector {
    async fn connect_and_subscribe(
        &self,
        config: &common::DeshredConfig,
        request: SubscribeDeshredRequest,
    ) -> Result<SubscribeDeshredUpdateStream> {
        let endpoint = crate::resolved_deshred_endpoint(config)?;
        let channel = tonic::transport::Endpoint::from_shared(endpoint)?
            .connect_timeout(StdDuration::from_millis(config.connect_timeout_ms.max(1)))
            .timeout(StdDuration::from_millis(config.request_timeout_ms.max(1)))
            .http2_keep_alive_interval(StdDuration::from_millis(
                config.keepalive_interval_ms.max(1),
            ))
            .keep_alive_while_idle(true)
            .tcp_nodelay(true)
            .connect()
            .await?;
        let auth = crate::resolved_deshred_metadata(config)?;
        let interceptor = match auth {
            Some((key, value)) => MetadataInjector::new(Some((&key, &value)))?,
            None => MetadataInjector::new(None)?,
        };
        let mut client = GeyserClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(config.max_decoded_message_size.max(1024 * 1024));
        let (request_tx, request_rx) = mpsc::channel::<SubscribeDeshredRequest>(4);
        request_tx
            .send(request)
            .await
            .map_err(|_| anyhow!("failed to seed deshred subscription request"))?;
        let stream = ReceiverStream::new(request_rx);
        let response: tonic::Response<tonic::codec::Streaming<SubscribeUpdateDeshred>> =
            client.subscribe_deshred(stream).await?;
        Ok(Box::pin(response.into_inner()))
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub struct MockConnectorBatch {
    pub updates: Vec<std::result::Result<SubscribeUpdate, String>>,
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub struct MockGeyserConnector {
    pub batches: Arc<std::sync::Mutex<Vec<MockConnectorBatch>>>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub struct MockDeshredConnectorBatch {
    pub updates: Vec<std::result::Result<SubscribeUpdateDeshred, Status>>,
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub struct MockDeshredConnector {
    pub batches: Arc<std::sync::Mutex<Vec<MockDeshredConnectorBatch>>>,
}

#[cfg(test)]
#[async_trait]
impl GeyserStreamConnector for MockGeyserConnector {
    async fn connect_and_subscribe(
        &self,
        _config: &common::GeyserConfig,
        _request: SubscribeRequest,
    ) -> Result<SubscribeUpdateStream> {
        let batch = self
            .batches
            .lock()
            .map_err(|_| anyhow!("mock connector lock poisoned"))?
            .drain(..1)
            .next()
            .unwrap_or(MockConnectorBatch {
                updates: Vec::new(),
            });
        Ok(Box::pin(tokio_stream::iter(batch.updates.into_iter().map(
            |item| match item {
                Ok(update) => Ok(update),
                Err(message) => Err(Status::unavailable(message)),
            },
        ))))
    }
}

#[cfg(test)]
#[async_trait]
impl DeshredStreamConnector for MockDeshredConnector {
    async fn connect_and_subscribe(
        &self,
        _config: &common::DeshredConfig,
        _request: SubscribeDeshredRequest,
    ) -> Result<SubscribeDeshredUpdateStream> {
        let batch = self
            .batches
            .lock()
            .map_err(|_| anyhow!("mock deshred connector lock poisoned"))?
            .drain(..1)
            .next()
            .unwrap_or(MockDeshredConnectorBatch {
                updates: Vec::new(),
            });
        Ok(Box::pin(tokio_stream::iter(batch.updates.into_iter().map(
            |item| match item {
                Ok(update) => Ok(update),
                Err(status) => Err(status),
            },
        ))))
    }
}

#[derive(Debug, Clone)]
pub struct GeyserEventNormalizer {
    idls: Vec<LoadedIdl>,
    curve_to_mint: HashMap<String, String>,
    mint_to_creator: HashMap<String, String>,
    seen_buy_mints: HashSet<String>,
    pending_curve_updates_by_curve_pubkey: HashMap<String, Vec<PendingCurveUpdate>>,
}

#[derive(Debug, Clone)]
struct PendingCurveUpdate {
    meta: EventMeta,
    curve_pubkey: String,
    virtual_quote: Decimal,
    virtual_token: Decimal,
    real_quote: Decimal,
    real_token: Decimal,
    token_total_supply_raw: Option<Decimal>,
    creator: Option<PubkeyValue>,
    quote_mint: Option<PubkeyValue>,
    reserve_field_schema: String,
    complete: bool,
    transaction_signature: Option<String>,
    write_version: u64,
}

impl GeyserEventNormalizer {
    pub fn from_loaded(loaded: &LoadedConfig) -> Result<Self> {
        let mut idls = Vec::new();
        for path in &loaded.config.pump.idl_paths {
            idls.push(LoadedIdl::load(loaded.resolve_path(path))?);
        }
        Ok(Self {
            idls,
            curve_to_mint: HashMap::new(),
            mint_to_creator: HashMap::new(),
            seen_buy_mints: HashSet::new(),
            pending_curve_updates_by_curve_pubkey: HashMap::new(),
        })
    }

    pub fn normalize_output(&mut self, output: IngestOutput) -> Vec<NormalizedEvent> {
        match output {
            IngestOutput::Transaction { meta, update } => self.normalize_transaction(meta, update),
            IngestOutput::Account { meta, update } => self.normalize_account(meta, update),
            IngestOutput::DataGap { meta, update } => vec![NormalizedEvent {
                meta,
                payload: EventPayload::DataGap(update),
            }],
            IngestOutput::Slot { .. } | IngestOutput::Block { .. } => Vec::new(),
        }
    }

    fn normalize_transaction(
        &mut self,
        meta: EventMeta,
        update: TransactionUpdate,
    ) -> Vec<NormalizedEvent> {
        let mut events = Vec::new();
        let compute_budget = parse_compute_budget(&update.instructions);
        let observed_source_prefix = match meta.source {
            EventSource::DeshredTentative => "deshred",
            EventSource::ShredTentative => "shred",
            _ => "geyser",
        };
        let status = if meta.canonicality == Canonicality::Tentative {
            TransactionStatus::Unknown
        } else {
            tx_status(update.succeeded)
        };
        events.push(NormalizedEvent {
            meta: meta.clone(),
            payload: EventPayload::ObservedTransaction(ObservedTransactionEvent {
                signature_hint: Some(update.signature.clone()),
                slot_hint: Some(update.slot),
                entry_index: Some(0),
                tx_position_estimate: update.transaction_index,
                signer: update.account_keys.first().cloned(),
                program_ids: update
                    .instructions
                    .iter()
                    .map(|instruction| instruction.program_id.clone())
                    .collect(),
                account_count: update.account_keys.len(),
                instruction_count: update.instructions.len(),
                account_list_hash: Some(hash_strings(&update.account_keys)),
                instruction_shape_hash: Some(hash_instruction_shape(&update.instructions)),
                compute_unit_limit: compute_budget.0,
                compute_unit_price: compute_budget.1,
                estimated_priority_fee_lamports: compute_budget.0.zip(compute_budget.1).map(
                    |(limit, price)| Lamports((limit as u64).saturating_mul(price) / 1_000_000),
                ),
                tx_fee_lamports: Some(Lamports(update.fee_lamports)),
                compute_units_consumed: update.compute_units_consumed,
                pre_sol_balances_lamports: update
                    .pre_balances
                    .iter()
                    .copied()
                    .map(Lamports)
                    .collect(),
                post_sol_balances_lamports: update
                    .post_balances
                    .iter()
                    .copied()
                    .map(Lamports)
                    .collect(),
                failed_transaction: !update.succeeded,
                error_code: update.error_code.clone(),
                bundle_like_evidence: bundle_like_evidence(&update, compute_budget),
                raw_packet_hash: format!("{observed_source_prefix}:{}", update.signature),
                first_seen_by_shred_ns: if meta.canonicality == Canonicality::Tentative {
                    meta.observed_at_monotonic_ns
                } else {
                    0
                },
                decode_confidence: meta.decode_confidence,
            }),
        });

        for (instruction_index, instruction) in update.instructions.iter().enumerate() {
            let Some(decoded) = self.decode_instruction(instruction) else {
                continue;
            };
            let account_map = decoded
                .accounts
                .iter()
                .cloned()
                .zip(instruction.accounts.iter().cloned())
                .collect::<HashMap<_, _>>();
            let mut instruction_meta = meta.clone();
            instruction_meta.instruction_index = Some(instruction_index as u16);
            instruction_meta.raw_reference = Some(RawEventReference {
                source_id: update.signature.clone(),
                cursor: Some(format!("ix:{instruction_index}")),
                offset: update.transaction_index.map(u64::from),
            });

            match decoded.name.as_str() {
                "create" | "create_v2" => {
                    let mint =
                        account_alias(&account_map, &["mint", "base_mint"]).unwrap_or_default();
                    let creator = account_alias(&account_map, &["creator", "user"])
                        .or_else(|| value_pubkey(&decoded.args, "creator").map(|value| value.0))
                        .unwrap_or_default();
                    let payer = account_alias(&account_map, &["user", "creator"])
                        .unwrap_or_else(|| creator.clone());
                    let bonding_curve = account_map
                        .get("bonding_curve")
                        .cloned()
                        .unwrap_or_default();
                    let associated_bonding_curve = account_alias(
                        &account_map,
                        &[
                            "associated_bonding_curve",
                            "associated_base_bonding_curve",
                            "associated_quote_bonding_curve",
                        ],
                    )
                    .map(PubkeyValue);
                    if mint.is_empty() || creator.is_empty() || bonding_curve.is_empty() {
                        continue;
                    }
                    let pending_curve_updates = if status != TransactionStatus::Failed {
                        self.curve_to_mint
                            .insert(bonding_curve.clone(), mint.clone());
                        self.mint_to_creator.insert(mint.clone(), creator.clone());
                        self.flush_pending_curve_updates(&bonding_curve, &mint)
                    } else {
                        Vec::new()
                    };
                    events.push(NormalizedEvent {
                        meta: instruction_meta,
                        payload: EventPayload::TokenCreated(TokenCreatedEvent {
                            mint: PubkeyValue(mint.clone()),
                            token_program: TokenProgramType::SplToken,
                            quote_mint: value_pubkey(&decoded.args, "quote_mint")
                                .or_else(|| {
                                    account_alias(&account_map, &["quote_mint"]).map(PubkeyValue)
                                })
                                .unwrap_or_else(|| PubkeyValue("quote".to_owned())),
                            quote_asset_type: quote_asset_type(&decoded.args),
                            creator_wallet: PubkeyValue(creator.clone()),
                            payer: PubkeyValue(payer),
                            bonding_curve_account: PubkeyValue(bonding_curve),
                            associated_bonding_curve_account: associated_bonding_curve,
                            metadata_account: None,
                            name: value_string(&decoded.args, "name")
                                .unwrap_or_else(|| "unknown".to_owned()),
                            symbol: value_string(&decoded.args, "symbol")
                                .unwrap_or_else(|| "UNK".to_owned()),
                            uri: value_string(&decoded.args, "uri").unwrap_or_default(),
                            create_instruction_variant: decoded.name.clone(),
                            initial_virtual_quote_reserves: None,
                            initial_virtual_token_reserves: None,
                            initial_real_quote_reserves: None,
                            initial_real_token_reserves: None,
                            initial_supply: None,
                            creator_initial_buy: None,
                            same_transaction_buys: count_decoded_buys(
                                &update.instructions,
                                &self.idls,
                            ),
                            same_slot_buys: 0,
                            fee_recipients: Vec::new(),
                            raw_account_list: instruction
                                .accounts
                                .iter()
                                .cloned()
                                .map(PubkeyValue)
                                .collect(),
                            launch_transaction_fingerprint: Some(format!(
                                "accounts:{}|ix:{}|cu:{:?}|cup:{:?}",
                                update.account_keys.len(),
                                update.instructions.len(),
                                compute_budget.0,
                                compute_budget.1
                            )),
                            status,
                        }),
                    });
                    events.extend(pending_curve_updates);
                }
                "buy" | "buy_v2" | "buy_exact_quote_in_v2" => {
                    let Some(mint) = account_alias(&account_map, &["mint", "base_mint"]) else {
                        continue;
                    };
                    let Some(buyer) = account_alias(&account_map, &["buyer", "user"]) else {
                        continue;
                    };
                    let token_out = estimate_token_delta(
                        &update.pre_token_balances,
                        &update.post_token_balances,
                        &mint,
                        Some(&buyer),
                    )
                    .filter(|value| *value > Decimal::ZERO)
                    .unwrap_or_else(|| {
                        value_decimal_alias(
                            &decoded.args,
                            &["min_token_out", "min_tokens_out", "amount"],
                        )
                        .unwrap_or(Decimal::ONE)
                    });
                    let quote_in = estimate_lamport_spend(&update, &buyer)
                        .or_else(|| {
                            value_decimal_alias(
                                &decoded.args,
                                &["quote_in", "spendable_quote_in", "max_sol_cost"],
                            )
                        })
                        .unwrap_or(Decimal::ZERO);
                    let first_buy = self.seen_buy_mints.insert(mint.clone());
                    let creator = self.mint_to_creator.get(&mint).cloned();
                    events.push(NormalizedEvent {
                        meta: instruction_meta,
                        payload: EventPayload::PumpBuy(PumpBuyEvent {
                            mint: PubkeyValue(mint.clone()),
                            buyer: PubkeyValue(buyer.clone()),
                            payer: PubkeyValue(buyer.clone()),
                            quote_in,
                            token_out,
                            price_before: None,
                            price_after: None,
                            effective_price: safe_price(quote_in, token_out),
                            slippage_estimate: None,
                            reserves_before: None,
                            reserves_after: None,
                            max_quote_cost: value_decimal_alias(
                                &decoded.args,
                                &[
                                    "max_quote_cost",
                                    "max_sol_cost",
                                    "spendable_quote_in",
                                    "quote_in",
                                ],
                            )
                            .or_else(|| Some(quote_in)),
                            compute_unit_limit: compute_budget.0,
                            compute_unit_price: compute_budget.1,
                            estimated_priority_fee_lamports: compute_budget
                                .0
                                .zip(compute_budget.1)
                                .map(|(limit, price)| {
                                    Lamports((limit as u64).saturating_mul(price) / 1_000_000)
                                }),
                            estimated_base_fee_lamports: Some(Lamports(update.fee_lamports)),
                            estimated_tip_lamports: None,
                            is_creator: creator.as_deref() == Some(buyer.as_str()),
                            is_known_cluster_member: false,
                            is_first_buy: first_buy,
                            status,
                        }),
                    });
                }
                "sell" | "sell_v2" => {
                    let Some(mint) = account_alias(&account_map, &["mint", "base_mint"]) else {
                        continue;
                    };
                    let Some(seller) = account_alias(&account_map, &["seller", "user"]) else {
                        continue;
                    };
                    let token_in = estimate_token_delta(
                        &update.post_token_balances,
                        &update.pre_token_balances,
                        &mint,
                        Some(&seller),
                    )
                    .filter(|value| *value > Decimal::ZERO)
                    .unwrap_or_else(|| {
                        value_decimal_alias(&decoded.args, &["token_in", "amount"])
                            .unwrap_or(Decimal::ONE)
                    });
                    let quote_out = estimate_lamport_gain(&update, &seller)
                        .filter(|value| *value > Decimal::ZERO)
                        .unwrap_or_else(|| {
                            value_decimal_alias(&decoded.args, &["min_quote_out", "min_sol_output"])
                                .unwrap_or(Decimal::ZERO)
                        });
                    let creator = self.mint_to_creator.get(&mint).cloned();
                    events.push(NormalizedEvent {
                        meta: instruction_meta,
                        payload: EventPayload::PumpSell(PumpSellEvent {
                            mint: PubkeyValue(mint),
                            seller: PubkeyValue(seller.clone()),
                            quote_out,
                            token_in,
                            price_before: None,
                            price_after: None,
                            effective_price: safe_price(quote_out, token_in),
                            slippage_estimate: None,
                            reserves_before: None,
                            reserves_after: None,
                            min_quote_output: value_decimal(&decoded.args, "min_quote_out"),
                            compute_unit_limit: compute_budget.0,
                            compute_unit_price: compute_budget.1,
                            estimated_priority_fee_lamports: compute_budget
                                .0
                                .zip(compute_budget.1)
                                .map(|(limit, price)| {
                                    Lamports((limit as u64).saturating_mul(price) / 1_000_000)
                                }),
                            estimated_base_fee_lamports: Some(Lamports(update.fee_lamports)),
                            estimated_tip_lamports: None,
                            is_creator: creator.as_deref() == Some(seller.as_str()),
                            is_top_holder_pre_sell: false,
                            is_known_cluster_member: false,
                            status,
                        }),
                    });
                }
                _ => {}
            }
        }

        events.extend(holder_balance_events(&meta, &update));
        if let Some(funding) = funding_event_from_transaction(&meta, &update) {
            events.push(funding);
        }
        events
    }

    fn normalize_account(
        &mut self,
        meta: EventMeta,
        update: AccountUpdate,
    ) -> Vec<NormalizedEvent> {
        let Ok(bytes) =
            base64::engine::general_purpose::STANDARD.decode(update.data_base64.as_bytes())
        else {
            return Vec::new();
        };
        for idl in &self.idls {
            let Ok(decoded) = idl.decode_account(&bytes) else {
                continue;
            };
            let AccountDecode::Known { decoded } = decoded else {
                continue;
            };
            if decoded.name != "BondingCurve" {
                continue;
            }
            let pending = pending_curve_update_from_decoded(
                meta.clone(),
                update.pubkey.clone(),
                &decoded,
                &update,
            );
            let Some(mint) = self.curve_to_mint.get(&update.pubkey).cloned() else {
                self.pending_curve_updates_by_curve_pubkey
                    .entry(update.pubkey.clone())
                    .or_default()
                    .push(pending);
                continue;
            };
            return vec![bonding_curve_event_from_pending(pending, &mint)];
        }
        Vec::new()
    }

    fn flush_pending_curve_updates(
        &mut self,
        curve_pubkey: &str,
        mint: &str,
    ) -> Vec<NormalizedEvent> {
        self.pending_curve_updates_by_curve_pubkey
            .remove(curve_pubkey)
            .unwrap_or_default()
            .into_iter()
            .map(|pending| bonding_curve_event_from_pending(pending, mint))
            .collect()
    }

    fn decode_instruction(
        &self,
        instruction: &TransactionInstruction,
    ) -> Option<idl::DecodedInstruction> {
        let data = hex::decode(&instruction.data_hex).ok()?;
        for idl in &self.idls {
            let Ok(decoded) = idl.decode_instruction(&data) else {
                continue;
            };
            if let InstructionDecode::Known { decoded } = decoded {
                return Some(decoded);
            }
        }
        None
    }
}

fn geyser_endpoint_configured(config: &common::GeyserConfig) -> bool {
    if !config.endpoint.trim().is_empty() {
        return true;
    }
    if config.endpoint_env.trim().is_empty() {
        return false;
    }
    std::env::var(&config.endpoint_env)
        .ok()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn geyser_auth_configured(config: &common::GeyserConfig) -> bool {
    if !config.auth_required {
        return true;
    }
    if config.auth_token_env.trim().is_empty() {
        return false;
    }
    std::env::var(&config.auth_token_env)
        .ok()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

pub async fn smoke_geyser_provider(
    loaded: &LoadedConfig,
    options: GeyserProviderSmokeOptions,
) -> Result<GeyserProviderSmokeSummary> {
    smoke_geyser_provider_with_connector(loaded, options, Arc::new(RealGeyserConnector)).await
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct FreshLaunchCanaryLiveOptions {
    pub duration_seconds: u64,
    pub max_launches: usize,
    pub stop_when_max_launches_seen: bool,
    #[serde(default)]
    pub retain_only_tracked_mints: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct FreshLaunchCanaryLiveSummary {
    pub provider_status: String,
    pub connected: bool,
    pub duration_seconds: u64,
    pub transaction_updates: u64,
    pub account_updates: u64,
    pub slot_updates: u64,
    pub normalized_events: u64,
    #[serde(default)]
    pub retained_events: u64,
    pub pump_create_decoded: u64,
    pub tracked_mint: Option<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct MaterialHunterStreamOptions {
    pub duration_seconds: u64,
    #[serde(default)]
    pub gap_tolerant_segments: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct MaterialHunterUpdateClassSummary {
    pub class_name: String,
    pub count: u64,
    pub rate_per_second: u64,
    pub decode_duration_ms_p95: u64,
    pub decode_duration_ms_max: u64,
    pub state_update_duration_ms_p95: u64,
    pub state_update_duration_ms_max: u64,
    pub worker_lag_ms_p95: u64,
    pub worker_lag_ms_max: u64,
    pub routed_partition_distribution: Vec<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct MaterialHunterTopKeySummary {
    pub key: String,
    pub count: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct MaterialHunterStreamSummary {
    pub provider_status: String,
    #[serde(default)]
    pub provider_blocker_class: Option<String>,
    #[serde(default)]
    pub provider_data_loss_seen: bool,
    #[serde(default)]
    pub provider_lagged_count: u64,
    #[serde(default)]
    pub reconnect_attempts: u64,
    #[serde(default)]
    pub stream_completed_normally: bool,
    #[serde(default)]
    pub provider_progress_stalled_seconds: u64,
    #[serde(default)]
    pub pump_progress_stalled_seconds: u64,
    pub connected: bool,
    pub duration_seconds: u64,
    pub transaction_updates: u64,
    pub account_updates: u64,
    pub slot_updates: u64,
    pub normalized_events: u64,
    pub pump_create_decoded: u64,
    #[serde(default)]
    pub grpc_reader_update_count: u64,
    #[serde(default)]
    pub grpc_reader_poll_latency_ms_p50: u64,
    #[serde(default)]
    pub grpc_reader_poll_latency_ms_p95: u64,
    #[serde(default)]
    pub grpc_reader_poll_latency_ms_p99: u64,
    #[serde(default)]
    pub grpc_reader_poll_latency_ms_max: u64,
    #[serde(default)]
    pub grpc_update_interarrival_ms_p50: u64,
    #[serde(default)]
    pub grpc_update_interarrival_ms_p95: u64,
    #[serde(default)]
    pub grpc_update_interarrival_ms_p99: u64,
    #[serde(default)]
    pub grpc_update_interarrival_ms_max: u64,
    #[serde(default)]
    pub internal_queue_depth_current: u64,
    #[serde(default)]
    pub internal_queue_depth_max: u64,
    #[serde(default)]
    pub internal_queue_capacity: u64,
    #[serde(default)]
    pub internal_queue_full_count: u64,
    #[serde(default)]
    pub decode_worker_lag_ms_max: u64,
    #[serde(default)]
    pub artifact_worker_lag_ms_max: u64,
    #[serde(default)]
    pub r2_worker_lag_ms_max: u64,
    #[serde(default)]
    pub stream_reader_blocked_by_processing: bool,
    #[serde(default)]
    pub client_backpressure_detected: bool,
    #[serde(default)]
    pub raw_queue_enqueue_latency_ms_p50: u64,
    #[serde(default)]
    pub raw_queue_enqueue_latency_ms_p95: u64,
    #[serde(default)]
    pub raw_queue_enqueue_latency_ms_p99: u64,
    #[serde(default)]
    pub raw_queue_enqueue_latency_ms_max: u64,
    #[serde(default)]
    pub raw_queue_wait_before_decode_ms_p50: u64,
    #[serde(default)]
    pub raw_queue_wait_before_decode_ms_p95: u64,
    #[serde(default)]
    pub raw_queue_wait_before_decode_ms_p99: u64,
    #[serde(default)]
    pub raw_queue_wait_before_decode_ms_max: u64,
    #[serde(default)]
    pub decode_duration_ms_p50: u64,
    #[serde(default)]
    pub decode_duration_ms_p95: u64,
    #[serde(default)]
    pub decode_duration_ms_p99: u64,
    #[serde(default)]
    pub decode_duration_ms_max: u64,
    #[serde(default)]
    pub state_update_duration_ms_p50: u64,
    #[serde(default)]
    pub state_update_duration_ms_p95: u64,
    #[serde(default)]
    pub state_update_duration_ms_p99: u64,
    #[serde(default)]
    pub state_update_duration_ms_max: u64,
    #[serde(default)]
    pub risk_feature_duration_ms_p50: u64,
    #[serde(default)]
    pub risk_feature_duration_ms_p95: u64,
    #[serde(default)]
    pub risk_feature_duration_ms_p99: u64,
    #[serde(default)]
    pub risk_feature_duration_ms_max: u64,
    #[serde(default)]
    pub artifact_enqueue_duration_ms_p50: u64,
    #[serde(default)]
    pub artifact_enqueue_duration_ms_p95: u64,
    #[serde(default)]
    pub artifact_enqueue_duration_ms_p99: u64,
    #[serde(default)]
    pub artifact_enqueue_duration_ms_max: u64,
    #[serde(default)]
    pub artifact_write_duration_ms_p50: u64,
    #[serde(default)]
    pub artifact_write_duration_ms_p95: u64,
    #[serde(default)]
    pub artifact_write_duration_ms_p99: u64,
    #[serde(default)]
    pub artifact_write_duration_ms_max: u64,
    #[serde(default)]
    pub worker_batch_size_p50: u64,
    #[serde(default)]
    pub worker_batch_size_p95: u64,
    #[serde(default)]
    pub worker_batch_size_max: u64,
    #[serde(default)]
    pub worker_updates_processed: u64,
    #[serde(default)]
    pub worker_updates_per_second: u64,
    #[serde(default)]
    pub worker_backlog_oldest_update_age_ms: u64,
    #[serde(default)]
    pub segment_queue_dropped_dirty_updates: u64,
    #[serde(default)]
    pub segment_worker_reset_count: u64,
    #[serde(default)]
    pub backpressure_threshold_crossed_at: Option<String>,
    #[serde(default)]
    pub backpressure_queue_depth_at_blocker: u64,
    #[serde(default)]
    pub worker_partitions: u64,
    #[serde(default)]
    pub partitioning_enabled: bool,
    #[serde(default)]
    pub router_updates_received: u64,
    #[serde(default)]
    pub router_updates_routed: u64,
    #[serde(default)]
    pub router_fallback_count: u64,
    #[serde(default)]
    pub router_error_count: u64,
    #[serde(default)]
    pub router_queue_depth_current: u64,
    #[serde(default)]
    pub router_queue_depth_max: u64,
    #[serde(default)]
    pub router_queue_full_count: u64,
    #[serde(default)]
    pub partition_queue_depth_current_max: u64,
    #[serde(default)]
    pub partition_queue_depth_max_overall: u64,
    #[serde(default)]
    pub partition_queue_full_count_total: u64,
    #[serde(default)]
    pub partition_queue_full_count_by_partition: Vec<u64>,
    #[serde(default)]
    pub partition_updates_processed_total: u64,
    #[serde(default)]
    pub partition_updates_processed_by_partition: Vec<u64>,
    #[serde(default)]
    pub partition_updates_per_second_total: u64,
    #[serde(default)]
    pub partition_updates_per_second_by_partition: Vec<u64>,
    #[serde(default)]
    pub partition_worker_lag_ms_p50: u64,
    #[serde(default)]
    pub partition_worker_lag_ms_p95: u64,
    #[serde(default)]
    pub partition_worker_lag_ms_p99: u64,
    #[serde(default)]
    pub partition_worker_lag_ms_max: u64,
    #[serde(default)]
    pub partition_worker_lag_ms_max_by_partition: Vec<u64>,
    #[serde(default)]
    pub partition_worker_lag_ms_p95_by_partition: Vec<u64>,
    #[serde(default)]
    pub partition_queue_depth_max_by_partition: Vec<u64>,
    #[serde(default)]
    pub partition_backlog_oldest_update_age_ms_by_partition: Vec<u64>,
    #[serde(default)]
    pub partition_batch_size_max_by_partition: Vec<u64>,
    #[serde(default)]
    pub partition_backpressure_trigger_partition: Option<u64>,
    #[serde(default)]
    pub partition_backpressure_trigger_reason: Option<String>,
    #[serde(default)]
    pub backpressure_threshold_ms: u64,
    #[serde(default)]
    pub backpressure_observed_lag_ms: u64,
    #[serde(default)]
    pub backpressure_update_class: Option<String>,
    #[serde(default)]
    pub backpressure_partition_id: Option<u64>,
    #[serde(default)]
    pub backpressure_segment_id: Option<u64>,
    #[serde(default)]
    pub unknown_mint_route_count: u64,
    #[serde(default)]
    pub skipped_untracked_account_updates: u64,
    #[serde(default)]
    pub update_class_telemetry: Vec<MaterialHunterUpdateClassSummary>,
    #[serde(default)]
    pub top_partition_keys_by_update_count: Vec<MaterialHunterTopKeySummary>,
    #[serde(default)]
    pub top_mints_by_worker_updates: Vec<MaterialHunterTopKeySummary>,
    #[serde(default)]
    pub top_accounts_by_worker_updates: Vec<MaterialHunterTopKeySummary>,
    #[serde(default)]
    pub top_update_classes_by_lag: Vec<MaterialHunterTopKeySummary>,
    #[serde(default)]
    pub top_update_classes_by_count: Vec<MaterialHunterTopKeySummary>,
    #[serde(default)]
    pub pump_trade_fast_prefilter_count: u64,
    #[serde(default)]
    pub pump_trade_deep_processed_count: u64,
    #[serde(default)]
    pub pump_trade_skipped_untracked_count: u64,
    #[serde(default)]
    pub pump_trade_skipped_tombstoned_count: u64,
    #[serde(default)]
    pub pump_trade_unknown_mint_count: u64,
    #[serde(default)]
    pub pump_trade_deferred_feature_count: u64,
    #[serde(default)]
    pub pump_trade_feature_recompute_count: u64,
    #[serde(default)]
    pub pump_trade_deep_process_duration_ms_p95: u64,
    #[serde(default)]
    pub pump_trade_deep_process_duration_ms_max: u64,
    #[serde(default)]
    pub pump_trade_prefilter_duration_ms_p95: u64,
    #[serde(default)]
    pub pump_trade_prefilter_duration_ms_max: u64,
    #[serde(default)]
    pub pump_trade_state_update_duration_ms_p95: u64,
    #[serde(default)]
    pub pump_trade_state_update_duration_ms_max: u64,
    #[serde(default)]
    pub pump_trade_risk_feature_duration_ms_p95: u64,
    #[serde(default)]
    pub pump_trade_risk_feature_duration_ms_max: u64,
    #[serde(default)]
    pub unknown_mint_route_count_by_class: Vec<MaterialHunterTopKeySummary>,
    #[serde(default)]
    pub account_pinned_update_count: u64,
    #[serde(default)]
    pub backpressure_hot_key: Option<String>,
    #[serde(default)]
    pub backpressure_hot_mint: Option<String>,
    #[serde(default)]
    pub backpressure_hot_account: Option<String>,
    #[serde(default)]
    pub backpressure_deep_processed_count_at_trigger: u64,
    #[serde(default)]
    pub backpressure_skipped_count_at_trigger: u64,
    #[serde(default)]
    pub transaction_signature_seen_count: u64,
    #[serde(default)]
    pub transaction_duplicate_signature_count: u64,
    #[serde(default)]
    pub transaction_duplicate_signature_skipped_count: u64,
    #[serde(default)]
    pub transaction_prefilter_count: u64,
    #[serde(default)]
    pub transaction_deep_processed_count: u64,
    #[serde(default)]
    pub transaction_mapping_hint_only_count: u64,
    #[serde(default)]
    pub transaction_untracked_pump_skipped_count: u64,
    #[serde(default)]
    pub transaction_account_pinned_unknown_count: u64,
    #[serde(default)]
    pub transaction_tombstoned_mint_skipped_count: u64,
    #[serde(default)]
    pub transaction_malformed_or_unknown_count: u64,
    #[serde(default)]
    pub transaction_other_untracked_skipped_count: u64,
    #[serde(default)]
    pub account_pinned_active_count: u64,
    #[serde(default)]
    pub account_pinned_unknown_count: u64,
    #[serde(default)]
    pub account_pinned_skipped_count: u64,
    #[serde(default)]
    pub account_pinned_deep_processed_count: u64,
    #[serde(default)]
    pub active_mint_transaction_update_count: u64,
    #[serde(default)]
    pub active_mint_transaction_deep_processed_count: u64,
    #[serde(default)]
    pub active_mint_transaction_skipped_count: u64,
    #[serde(default)]
    pub active_mint_transaction_coalesced_count: u64,
    #[serde(default)]
    pub active_mint_transaction_dirty_feature_count: u64,
    #[serde(default)]
    pub active_mint_transaction_delta_flush_count: u64,
    #[serde(default)]
    pub active_mint_transaction_budget_exceeded_count: u64,
    #[serde(default)]
    pub active_mint_transaction_degraded_count: u64,
    #[serde(default)]
    pub active_mint_transaction_queue_pressure_count: u64,
    #[serde(default)]
    pub top_active_mints_by_transaction_count: Vec<MaterialHunterTopKeySummary>,
    #[serde(default)]
    pub top_active_mints_by_coalesced_count: Vec<MaterialHunterTopKeySummary>,
    #[serde(default)]
    pub top_active_mints_by_deep_processed_count: Vec<MaterialHunterTopKeySummary>,
    #[serde(default)]
    pub top_active_mints_by_queue_pressure: Vec<MaterialHunterTopKeySummary>,
    #[serde(default)]
    pub top_active_mints_by_transaction_lag: Vec<MaterialHunterTopKeySummary>,
    #[serde(default)]
    pub active_mint_delta_flush_duration_ms_p95: u64,
    #[serde(default)]
    pub active_mint_delta_flush_duration_ms_max: u64,
    #[serde(default)]
    pub degraded_active_mint_count: u64,
    #[serde(default)]
    pub degraded_active_mints: Vec<String>,
    #[serde(default)]
    pub partition_queue_pressure_preempted_count: u64,
    #[serde(default)]
    pub partition_queue_pressure_dominant_mint: Option<String>,
    #[serde(default)]
    pub partition_queue_pressure_dominant_mint_update_count: u64,
    #[serde(default)]
    pub partition_queue_pressure_degraded_mint: Option<String>,
    #[serde(default)]
    pub partition_queue_pressure_preempted_before_full: bool,
    #[serde(default)]
    pub partition_queue_full_after_preemption: bool,
    #[serde(default)]
    pub preemptive_noisy_mint_degraded: bool,
    #[serde(default)]
    pub transaction_feature_deferred_count: u64,
    #[serde(default)]
    pub transaction_feature_recompute_count: u64,
    #[serde(default)]
    pub transaction_risk_feature_duration_ms_p95: u64,
    #[serde(default)]
    pub transaction_risk_feature_duration_ms_max: u64,
    #[serde(default)]
    pub transaction_state_update_duration_ms_p95: u64,
    #[serde(default)]
    pub transaction_state_update_duration_ms_max: u64,
    #[serde(default)]
    pub transaction_deep_process_duration_ms_p95: u64,
    #[serde(default)]
    pub transaction_deep_process_duration_ms_max: u64,
    #[serde(default)]
    pub transaction_prefilter_duration_ms_p95: u64,
    #[serde(default)]
    pub transaction_prefilter_duration_ms_max: u64,
    #[serde(default)]
    pub backpressure_transaction_class: Option<String>,
    #[serde(default)]
    pub backpressure_transaction_signature: Option<String>,
    #[serde(default)]
    pub backpressure_transaction_mint: Option<String>,
    #[serde(default)]
    pub backpressure_transaction_account: Option<String>,
    #[serde(default)]
    pub backpressure_deep_transaction_count_at_trigger: u64,
    #[serde(default)]
    pub backpressure_skipped_transaction_count_at_trigger: u64,
    #[serde(default)]
    pub backpressure_account_pinned_count_at_trigger: u64,
    #[serde(default)]
    pub partition_decode_duration_ms_p50: u64,
    #[serde(default)]
    pub partition_decode_duration_ms_p95: u64,
    #[serde(default)]
    pub partition_decode_duration_ms_p99: u64,
    #[serde(default)]
    pub partition_decode_duration_ms_max: u64,
    #[serde(default)]
    pub partition_state_update_duration_ms_p50: u64,
    #[serde(default)]
    pub partition_state_update_duration_ms_p95: u64,
    #[serde(default)]
    pub partition_state_update_duration_ms_p99: u64,
    #[serde(default)]
    pub partition_state_update_duration_ms_max: u64,
    #[serde(default)]
    pub partition_lock_wait_ms_max: u64,
    #[serde(default)]
    pub partition_batch_size_p50: u64,
    #[serde(default)]
    pub partition_batch_size_p95: u64,
    #[serde(default)]
    pub partition_batch_size_max: u64,
    #[serde(default)]
    pub worker_backpressure_detected: bool,
    #[serde(default)]
    pub dirty_partition_queued_updates_discarded: u64,
    #[serde(default)]
    pub partition_worker_reset_count: u64,
    #[serde(default)]
    pub artifact_queue_depth_max: u64,
    #[serde(default)]
    pub artifact_queue_full_count: u64,
    pub errors: Vec<String>,
}

#[derive(Debug)]
enum MaterialHunterReaderMessage {
    Update(SubscribeUpdate, tokio::time::Instant),
    StreamError(Status),
    StreamClosed,
}

#[derive(Debug)]
struct MaterialHunterPartitionUpdate {
    update: SubscribeUpdate,
    read_at: tokio::time::Instant,
    sequence: u64,
    update_class: &'static str,
    route_key_label: String,
    transaction_signature: Option<String>,
    transaction_mint: Option<String>,
    transaction_account: Option<String>,
}

#[derive(Debug)]
enum MaterialHunterWorkerOutput {
    Event {
        event: NormalizedEvent,
        read_at: tokio::time::Instant,
        partition: usize,
        sequence: u64,
    },
    StreamError(Status),
    StreamClosed,
}

#[derive(Debug, Default, Clone)]
struct MaterialHunterUpdateClassStats {
    count: u64,
    decode_duration_ms: Vec<u64>,
    worker_lag_ms: Vec<u64>,
    routed_partition_distribution: Vec<u64>,
}

#[derive(Debug, Default)]
struct MaterialHunterReaderStats {
    update_count: u64,
    transaction_updates: u64,
    account_updates: u64,
    slot_updates: u64,
    poll_latency_ms: Vec<u64>,
    interarrival_ms: Vec<u64>,
    enqueue_latency_ms: Vec<u64>,
    queue_wait_before_decode_ms: Vec<u64>,
    decode_duration_ms: Vec<u64>,
    worker_batch_sizes: Vec<u64>,
    worker_started_at: Option<tokio::time::Instant>,
    worker_updates_processed: u64,
    worker_backlog_oldest_update_age_ms: u64,
    queue_depth_current: u64,
    queue_depth_max: u64,
    queue_capacity: u64,
    queue_full_count: u64,
    decode_worker_lag_ms_max: u64,
    client_backpressure_detected: bool,
    segment_queue_dropped_dirty_updates: u64,
    segment_worker_reset_count: u64,
    backpressure_threshold_crossed_at: Option<String>,
    backpressure_queue_depth_at_blocker: u64,
    worker_partitions: u64,
    partitioning_enabled: bool,
    router_updates_received: u64,
    router_updates_routed: u64,
    router_fallback_count: u64,
    router_error_count: u64,
    router_queue_depth_current: u64,
    router_queue_depth_max: u64,
    router_queue_full_count: u64,
    partition_queue_depth_current: Vec<u64>,
    partition_queue_depth_max: Vec<u64>,
    partition_queue_full_count_by_partition: Vec<u64>,
    partition_updates_processed_by_partition: Vec<u64>,
    partition_worker_lag_ms_by_partition: Vec<Vec<u64>>,
    partition_backlog_oldest_update_age_ms_by_partition: Vec<u64>,
    partition_batch_size_max_by_partition: Vec<u64>,
    partition_started_at: Option<tokio::time::Instant>,
    partition_worker_lag_ms: Vec<u64>,
    partition_decode_duration_ms: Vec<u64>,
    partition_batch_sizes: Vec<u64>,
    worker_backpressure_detected: bool,
    dirty_partition_queued_updates_discarded: u64,
    partition_worker_reset_count: u64,
    artifact_queue_depth_max: u64,
    artifact_queue_full_count: u64,
    partition_backpressure_trigger_partition: Option<u64>,
    partition_backpressure_trigger_reason: Option<String>,
    backpressure_threshold_ms: u64,
    backpressure_observed_lag_ms: u64,
    backpressure_update_class: Option<String>,
    backpressure_partition_id: Option<u64>,
    backpressure_segment_id: Option<u64>,
    unknown_mint_route_count: u64,
    skipped_untracked_account_updates: u64,
    update_class_stats: BTreeMap<&'static str, MaterialHunterUpdateClassStats>,
    top_partition_key_counts: BTreeMap<String, u64>,
    top_mint_counts: BTreeMap<String, u64>,
    top_account_counts: BTreeMap<String, u64>,
    pump_trade_fast_prefilter_count: u64,
    pump_trade_deep_processed_count: u64,
    pump_trade_skipped_untracked_count: u64,
    pump_trade_skipped_tombstoned_count: u64,
    pump_trade_unknown_mint_count: u64,
    pump_trade_deferred_feature_count: u64,
    pump_trade_feature_recompute_count: u64,
    pump_trade_deep_process_duration_ms: Vec<u64>,
    pump_trade_prefilter_duration_ms: Vec<u64>,
    pump_trade_state_update_duration_ms: Vec<u64>,
    pump_trade_risk_feature_duration_ms: Vec<u64>,
    unknown_mint_route_count_by_class: BTreeMap<String, u64>,
    account_pinned_update_count: u64,
    backpressure_hot_key: Option<String>,
    backpressure_hot_mint: Option<String>,
    backpressure_hot_account: Option<String>,
    backpressure_deep_processed_count_at_trigger: u64,
    backpressure_skipped_count_at_trigger: u64,
    transaction_signature_seen_count: u64,
    transaction_duplicate_signature_count: u64,
    transaction_duplicate_signature_skipped_count: u64,
    transaction_prefilter_count: u64,
    transaction_deep_processed_count: u64,
    transaction_mapping_hint_only_count: u64,
    transaction_untracked_pump_skipped_count: u64,
    transaction_account_pinned_unknown_count: u64,
    transaction_tombstoned_mint_skipped_count: u64,
    transaction_malformed_or_unknown_count: u64,
    transaction_other_untracked_skipped_count: u64,
    account_pinned_active_count: u64,
    account_pinned_unknown_count: u64,
    account_pinned_skipped_count: u64,
    account_pinned_deep_processed_count: u64,
    active_mint_transaction_update_count: u64,
    active_mint_transaction_deep_processed_count: u64,
    active_mint_transaction_skipped_count: u64,
    active_mint_transaction_coalesced_count: u64,
    active_mint_transaction_dirty_feature_count: u64,
    active_mint_transaction_delta_flush_count: u64,
    active_mint_transaction_budget_exceeded_count: u64,
    active_mint_transaction_degraded_count: u64,
    active_mint_transaction_queue_pressure_count: u64,
    top_active_mint_transaction_counts: BTreeMap<String, u64>,
    top_active_mint_coalesced_counts: BTreeMap<String, u64>,
    top_active_mint_deep_processed_counts: BTreeMap<String, u64>,
    top_active_mint_queue_pressure_counts: BTreeMap<String, u64>,
    top_active_mint_transaction_lag: BTreeMap<String, u64>,
    active_mint_delta_flush_duration_ms: Vec<u64>,
    degraded_active_mints: HashSet<String>,
    partition_queue_pressure_preempted_count: u64,
    partition_queue_pressure_dominant_mint: Option<String>,
    partition_queue_pressure_dominant_mint_update_count: u64,
    partition_queue_pressure_degraded_mint: Option<String>,
    partition_queue_pressure_preempted_before_full: bool,
    partition_queue_full_after_preemption: bool,
    transaction_feature_deferred_count: u64,
    transaction_feature_recompute_count: u64,
    transaction_risk_feature_duration_ms: Vec<u64>,
    transaction_state_update_duration_ms: Vec<u64>,
    transaction_deep_process_duration_ms: Vec<u64>,
    transaction_prefilter_duration_ms: Vec<u64>,
    backpressure_transaction_class: Option<String>,
    backpressure_transaction_signature: Option<String>,
    backpressure_transaction_mint: Option<String>,
    backpressure_transaction_account: Option<String>,
    backpressure_deep_transaction_count_at_trigger: u64,
    backpressure_skipped_transaction_count_at_trigger: u64,
    backpressure_account_pinned_count_at_trigger: u64,
}

impl MaterialHunterReaderStats {
    fn record_poll_latency(&mut self, millis: u64) {
        if self.poll_latency_ms.len() < 100_000 {
            self.poll_latency_ms.push(millis);
        }
    }

    fn record_interarrival(&mut self, millis: u64) {
        if self.interarrival_ms.len() < 100_000 {
            self.interarrival_ms.push(millis);
        }
    }

    fn record_enqueue_latency(&mut self, millis: u64) {
        if self.enqueue_latency_ms.len() < 100_000 {
            self.enqueue_latency_ms.push(millis);
        }
    }

    fn record_queue_wait(&mut self, millis: u64) {
        if self.queue_wait_before_decode_ms.len() < 100_000 {
            self.queue_wait_before_decode_ms.push(millis);
        }
    }

    fn record_decode_duration(&mut self, millis: u64) {
        if self.decode_duration_ms.len() < 100_000 {
            self.decode_duration_ms.push(millis);
        }
    }

    fn record_worker_batch(&mut self, size: u64) {
        if self.worker_batch_sizes.len() < 100_000 {
            self.worker_batch_sizes.push(size);
        }
    }

    fn record_partition_lag(&mut self, millis: u64) {
        if self.partition_worker_lag_ms.len() < 100_000 {
            self.partition_worker_lag_ms.push(millis);
        }
    }

    fn record_partition_decode_duration(&mut self, millis: u64) {
        if self.partition_decode_duration_ms.len() < 100_000 {
            self.partition_decode_duration_ms.push(millis);
        }
    }

    fn record_partition_batch(&mut self, size: u64) {
        if self.partition_batch_sizes.len() < 100_000 {
            self.partition_batch_sizes.push(size);
        }
    }

    fn record_partition_lag_for_partition(&mut self, partition: usize, millis: u64) {
        if let Some(values) = self.partition_worker_lag_ms_by_partition.get_mut(partition) {
            if values.len() < 25_000 {
                values.push(millis);
            }
        }
        if let Some(oldest) = self
            .partition_backlog_oldest_update_age_ms_by_partition
            .get_mut(partition)
        {
            *oldest = (*oldest).max(millis);
        }
    }

    fn record_update_class_route(&mut self, class_name: &'static str, partition: usize) {
        let partitions = self.worker_partitions.max(1) as usize;
        let entry = self
            .update_class_stats
            .entry(class_name)
            .or_insert_with(|| MaterialHunterUpdateClassStats {
                routed_partition_distribution: vec![0; partitions],
                ..MaterialHunterUpdateClassStats::default()
            });
        entry.count = entry.count.saturating_add(1);
        if entry.routed_partition_distribution.len() < partitions {
            entry.routed_partition_distribution.resize(partitions, 0);
        }
        if let Some(count) = entry.routed_partition_distribution.get_mut(partition) {
            *count = count.saturating_add(1);
        }
    }

    fn record_update_class_skipped(&mut self, class_name: &'static str) {
        let partitions = self.worker_partitions.max(1) as usize;
        let entry = self
            .update_class_stats
            .entry(class_name)
            .or_insert_with(|| MaterialHunterUpdateClassStats {
                routed_partition_distribution: vec![0; partitions],
                ..MaterialHunterUpdateClassStats::default()
            });
        entry.count = entry.count.saturating_add(1);
    }

    fn record_pump_trade_prefilter_duration(&mut self, millis: u64) {
        if self.pump_trade_prefilter_duration_ms.len() < 100_000 {
            self.pump_trade_prefilter_duration_ms.push(millis);
        }
    }

    fn record_pump_trade_deep_duration(&mut self, millis: u64) {
        if self.pump_trade_deep_process_duration_ms.len() < 100_000 {
            self.pump_trade_deep_process_duration_ms.push(millis);
        }
    }

    fn record_transaction_prefilter_duration(&mut self, millis: u64) {
        if self.transaction_prefilter_duration_ms.len() < 100_000 {
            self.transaction_prefilter_duration_ms.push(millis);
        }
    }

    fn record_transaction_deep_duration(&mut self, millis: u64) {
        if self.transaction_deep_process_duration_ms.len() < 100_000 {
            self.transaction_deep_process_duration_ms.push(millis);
        }
    }

    fn record_update_class_worker(
        &mut self,
        class_name: &'static str,
        partition: usize,
        lag_ms: u64,
        decode_ms: u64,
    ) {
        let partitions = self.worker_partitions.max(1) as usize;
        let entry = self
            .update_class_stats
            .entry(class_name)
            .or_insert_with(|| MaterialHunterUpdateClassStats {
                routed_partition_distribution: vec![0; partitions],
                ..MaterialHunterUpdateClassStats::default()
            });
        if entry.worker_lag_ms.len() < 25_000 {
            entry.worker_lag_ms.push(lag_ms);
        }
        if entry.decode_duration_ms.len() < 25_000 {
            entry.decode_duration_ms.push(decode_ms);
        }
        if entry.routed_partition_distribution.len() < partitions {
            entry.routed_partition_distribution.resize(partitions, 0);
        }
        if let Some(count) = entry.routed_partition_distribution.get_mut(partition) {
            *count = (*count).max(1);
        }
    }

    fn increment_top_count(map: &mut BTreeMap<String, u64>, key: String) {
        if key.trim().is_empty() {
            return;
        }
        let next = map.get(&key).copied().unwrap_or(0).saturating_add(1);
        map.insert(key, next);
        if map.len() > 256 {
            if let Some(remove_key) = map
                .iter()
                .min_by_key(|(_, count)| *count)
                .map(|(key, _)| key.clone())
            {
                map.remove(&remove_key);
            }
        }
    }

    fn max_top_value(map: &mut BTreeMap<String, u64>, key: String, value: u64) {
        if key.trim().is_empty() {
            return;
        }
        let current = map.get(&key).copied().unwrap_or(0);
        map.insert(key, current.max(value));
        if map.len() > 256 {
            if let Some(remove_key) = map
                .iter()
                .min_by_key(|(_, count)| *count)
                .map(|(key, _)| key.clone())
            {
                map.remove(&remove_key);
            }
        }
    }

    fn record_update_kind(&mut self, update: &SubscribeUpdate) {
        match update.update_oneof.as_ref() {
            Some(UpdateOneof::Transaction(_)) | Some(UpdateOneof::TransactionStatus(_)) => {
                self.transaction_updates = self.transaction_updates.saturating_add(1);
            }
            Some(UpdateOneof::Account(_)) => {
                self.account_updates = self.account_updates.saturating_add(1);
            }
            Some(UpdateOneof::Slot(_)) => {
                self.slot_updates = self.slot_updates.saturating_add(1);
            }
            _ => {}
        }
    }
}

fn top_key_summaries(
    map: &BTreeMap<String, u64>,
    limit: usize,
) -> Vec<MaterialHunterTopKeySummary> {
    let mut rows = map
        .iter()
        .map(|(key, count)| MaterialHunterTopKeySummary {
            key: key.clone(),
            count: *count,
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.key.cmp(&right.key))
    });
    rows.truncate(limit);
    rows
}

fn percentile(values: &[u64], numerator: usize, denominator: usize) -> u64 {
    if values.is_empty() || denominator == 0 {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let index = sorted.len().saturating_sub(1).saturating_mul(numerator) / denominator;
    sorted[index]
}

fn apply_reader_stats_to_summary(
    summary: &mut MaterialHunterStreamSummary,
    stats: &MaterialHunterReaderStats,
) {
    summary.grpc_reader_update_count = stats.update_count;
    summary.transaction_updates = stats.transaction_updates;
    summary.account_updates = stats.account_updates;
    summary.slot_updates = stats.slot_updates;
    summary.grpc_reader_poll_latency_ms_p50 = percentile(&stats.poll_latency_ms, 50, 100);
    summary.grpc_reader_poll_latency_ms_p95 = percentile(&stats.poll_latency_ms, 95, 100);
    summary.grpc_reader_poll_latency_ms_p99 = percentile(&stats.poll_latency_ms, 99, 100);
    summary.grpc_reader_poll_latency_ms_max =
        stats.poll_latency_ms.iter().copied().max().unwrap_or(0);
    summary.grpc_update_interarrival_ms_p50 = percentile(&stats.interarrival_ms, 50, 100);
    summary.grpc_update_interarrival_ms_p95 = percentile(&stats.interarrival_ms, 95, 100);
    summary.grpc_update_interarrival_ms_p99 = percentile(&stats.interarrival_ms, 99, 100);
    summary.grpc_update_interarrival_ms_max =
        stats.interarrival_ms.iter().copied().max().unwrap_or(0);
    summary.internal_queue_depth_current = stats.queue_depth_current;
    summary.internal_queue_depth_max = stats.queue_depth_max;
    summary.internal_queue_capacity = stats.queue_capacity;
    summary.internal_queue_full_count = stats.queue_full_count;
    summary.decode_worker_lag_ms_max = stats.decode_worker_lag_ms_max;
    summary.stream_reader_blocked_by_processing = stats.queue_full_count > 0;
    summary.client_backpressure_detected = stats.client_backpressure_detected;
    summary.raw_queue_enqueue_latency_ms_p50 = percentile(&stats.enqueue_latency_ms, 50, 100);
    summary.raw_queue_enqueue_latency_ms_p95 = percentile(&stats.enqueue_latency_ms, 95, 100);
    summary.raw_queue_enqueue_latency_ms_p99 = percentile(&stats.enqueue_latency_ms, 99, 100);
    summary.raw_queue_enqueue_latency_ms_max =
        stats.enqueue_latency_ms.iter().copied().max().unwrap_or(0);
    summary.raw_queue_wait_before_decode_ms_p50 =
        percentile(&stats.queue_wait_before_decode_ms, 50, 100);
    summary.raw_queue_wait_before_decode_ms_p95 =
        percentile(&stats.queue_wait_before_decode_ms, 95, 100);
    summary.raw_queue_wait_before_decode_ms_p99 =
        percentile(&stats.queue_wait_before_decode_ms, 99, 100);
    summary.raw_queue_wait_before_decode_ms_max = stats
        .queue_wait_before_decode_ms
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.decode_duration_ms_p50 = percentile(&stats.decode_duration_ms, 50, 100);
    summary.decode_duration_ms_p95 = percentile(&stats.decode_duration_ms, 95, 100);
    summary.decode_duration_ms_p99 = percentile(&stats.decode_duration_ms, 99, 100);
    summary.decode_duration_ms_max = stats.decode_duration_ms.iter().copied().max().unwrap_or(0);
    summary.state_update_duration_ms_p50 = 0;
    summary.state_update_duration_ms_p95 = 0;
    summary.state_update_duration_ms_p99 = 0;
    summary.state_update_duration_ms_max = 0;
    summary.risk_feature_duration_ms_p50 = 0;
    summary.risk_feature_duration_ms_p95 = 0;
    summary.risk_feature_duration_ms_p99 = 0;
    summary.risk_feature_duration_ms_max = 0;
    summary.artifact_enqueue_duration_ms_p50 = 0;
    summary.artifact_enqueue_duration_ms_p95 = 0;
    summary.artifact_enqueue_duration_ms_p99 = 0;
    summary.artifact_enqueue_duration_ms_max = 0;
    summary.artifact_write_duration_ms_p50 = 0;
    summary.artifact_write_duration_ms_p95 = 0;
    summary.artifact_write_duration_ms_p99 = 0;
    summary.artifact_write_duration_ms_max = 0;
    summary.worker_batch_size_p50 = percentile(&stats.worker_batch_sizes, 50, 100);
    summary.worker_batch_size_p95 = percentile(&stats.worker_batch_sizes, 95, 100);
    summary.worker_batch_size_max = stats.worker_batch_sizes.iter().copied().max().unwrap_or(0);
    summary.worker_updates_processed = stats.worker_updates_processed;
    summary.worker_updates_per_second = stats
        .worker_started_at
        .map(|started| {
            let secs = started.elapsed().as_secs().max(1);
            stats.worker_updates_processed / secs
        })
        .unwrap_or(0);
    summary.worker_backlog_oldest_update_age_ms = stats.worker_backlog_oldest_update_age_ms;
    summary.segment_queue_dropped_dirty_updates = stats.segment_queue_dropped_dirty_updates;
    summary.segment_worker_reset_count = stats.segment_worker_reset_count;
    summary.backpressure_threshold_crossed_at = stats.backpressure_threshold_crossed_at.clone();
    summary.backpressure_queue_depth_at_blocker = stats.backpressure_queue_depth_at_blocker;
    summary.worker_partitions = stats.worker_partitions;
    summary.partitioning_enabled = stats.partitioning_enabled;
    summary.router_updates_received = stats.router_updates_received;
    summary.router_updates_routed = stats.router_updates_routed;
    summary.router_fallback_count = stats.router_fallback_count;
    summary.router_error_count = stats.router_error_count;
    summary.router_queue_depth_current = stats.router_queue_depth_current;
    summary.router_queue_depth_max = stats.router_queue_depth_max;
    summary.router_queue_full_count = stats.router_queue_full_count;
    summary.partition_queue_depth_current_max = stats
        .partition_queue_depth_current
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.partition_queue_depth_max_overall = stats
        .partition_queue_depth_max
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.partition_queue_full_count_total =
        stats.partition_queue_full_count_by_partition.iter().sum();
    summary.partition_queue_full_count_by_partition =
        stats.partition_queue_full_count_by_partition.clone();
    summary.partition_updates_processed_total =
        stats.partition_updates_processed_by_partition.iter().sum();
    summary.partition_updates_processed_by_partition =
        stats.partition_updates_processed_by_partition.clone();
    summary.partition_updates_per_second_total = stats
        .partition_started_at
        .map(|started| {
            let secs = started.elapsed().as_secs().max(1);
            summary.partition_updates_processed_total / secs
        })
        .unwrap_or(0);
    summary.partition_updates_per_second_by_partition = stats
        .partition_started_at
        .map(|started| {
            let secs = started.elapsed().as_secs().max(1);
            stats
                .partition_updates_processed_by_partition
                .iter()
                .map(|count| count / secs)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    summary.partition_worker_lag_ms_p50 = percentile(&stats.partition_worker_lag_ms, 50, 100);
    summary.partition_worker_lag_ms_p95 = percentile(&stats.partition_worker_lag_ms, 95, 100);
    summary.partition_worker_lag_ms_p99 = percentile(&stats.partition_worker_lag_ms, 99, 100);
    summary.partition_worker_lag_ms_max = stats
        .partition_worker_lag_ms
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.partition_worker_lag_ms_max_by_partition = stats
        .partition_worker_lag_ms_by_partition
        .iter()
        .map(|values| values.iter().copied().max().unwrap_or(0))
        .collect();
    summary.partition_worker_lag_ms_p95_by_partition = stats
        .partition_worker_lag_ms_by_partition
        .iter()
        .map(|values| percentile(values, 95, 100))
        .collect();
    summary.partition_queue_depth_max_by_partition = stats.partition_queue_depth_max.clone();
    summary.partition_backlog_oldest_update_age_ms_by_partition = stats
        .partition_backlog_oldest_update_age_ms_by_partition
        .clone();
    summary.partition_batch_size_max_by_partition =
        stats.partition_batch_size_max_by_partition.clone();
    summary.partition_backpressure_trigger_partition =
        stats.partition_backpressure_trigger_partition;
    summary.partition_backpressure_trigger_reason =
        stats.partition_backpressure_trigger_reason.clone();
    summary.backpressure_threshold_ms = stats.backpressure_threshold_ms;
    summary.backpressure_observed_lag_ms = stats.backpressure_observed_lag_ms;
    summary.backpressure_update_class = stats.backpressure_update_class.clone();
    summary.backpressure_partition_id = stats.backpressure_partition_id;
    summary.backpressure_segment_id = stats.backpressure_segment_id;
    summary.unknown_mint_route_count = stats.unknown_mint_route_count;
    summary.skipped_untracked_account_updates = stats.skipped_untracked_account_updates;
    let secs = stats
        .partition_started_at
        .map(|started| started.elapsed().as_secs().max(1))
        .unwrap_or(1);
    summary.update_class_telemetry = stats
        .update_class_stats
        .iter()
        .map(
            |(class_name, class_stats)| MaterialHunterUpdateClassSummary {
                class_name: (*class_name).to_owned(),
                count: class_stats.count,
                rate_per_second: class_stats.count / secs,
                decode_duration_ms_p95: percentile(&class_stats.decode_duration_ms, 95, 100),
                decode_duration_ms_max: class_stats
                    .decode_duration_ms
                    .iter()
                    .copied()
                    .max()
                    .unwrap_or(0),
                state_update_duration_ms_p95: 0,
                state_update_duration_ms_max: 0,
                worker_lag_ms_p95: percentile(&class_stats.worker_lag_ms, 95, 100),
                worker_lag_ms_max: class_stats.worker_lag_ms.iter().copied().max().unwrap_or(0),
                routed_partition_distribution: class_stats.routed_partition_distribution.clone(),
            },
        )
        .collect();
    summary.update_class_telemetry.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.class_name.cmp(&right.class_name))
    });
    summary.top_partition_keys_by_update_count =
        top_key_summaries(&stats.top_partition_key_counts, 20);
    summary.top_mints_by_worker_updates = top_key_summaries(&stats.top_mint_counts, 20);
    summary.top_accounts_by_worker_updates = top_key_summaries(&stats.top_account_counts, 20);
    let mut classes_by_lag = stats
        .update_class_stats
        .iter()
        .map(|(class_name, class_stats)| MaterialHunterTopKeySummary {
            key: (*class_name).to_owned(),
            count: class_stats.worker_lag_ms.iter().copied().max().unwrap_or(0),
        })
        .collect::<Vec<_>>();
    classes_by_lag.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.key.cmp(&right.key))
    });
    classes_by_lag.truncate(20);
    summary.top_update_classes_by_lag = classes_by_lag;
    let mut classes_by_count = stats
        .update_class_stats
        .iter()
        .map(|(class_name, class_stats)| MaterialHunterTopKeySummary {
            key: (*class_name).to_owned(),
            count: class_stats.count,
        })
        .collect::<Vec<_>>();
    classes_by_count.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.key.cmp(&right.key))
    });
    classes_by_count.truncate(20);
    summary.top_update_classes_by_count = classes_by_count;
    summary.pump_trade_fast_prefilter_count = stats.pump_trade_fast_prefilter_count;
    summary.pump_trade_deep_processed_count = stats.pump_trade_deep_processed_count;
    summary.pump_trade_skipped_untracked_count = stats.pump_trade_skipped_untracked_count;
    summary.pump_trade_skipped_tombstoned_count = stats.pump_trade_skipped_tombstoned_count;
    summary.pump_trade_unknown_mint_count = stats.pump_trade_unknown_mint_count;
    summary.pump_trade_deferred_feature_count = stats.pump_trade_deferred_feature_count;
    summary.pump_trade_feature_recompute_count = stats.pump_trade_feature_recompute_count;
    summary.pump_trade_deep_process_duration_ms_p95 =
        percentile(&stats.pump_trade_deep_process_duration_ms, 95, 100);
    summary.pump_trade_deep_process_duration_ms_max = stats
        .pump_trade_deep_process_duration_ms
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.pump_trade_prefilter_duration_ms_p95 =
        percentile(&stats.pump_trade_prefilter_duration_ms, 95, 100);
    summary.pump_trade_prefilter_duration_ms_max = stats
        .pump_trade_prefilter_duration_ms
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.pump_trade_state_update_duration_ms_p95 =
        percentile(&stats.pump_trade_state_update_duration_ms, 95, 100);
    summary.pump_trade_state_update_duration_ms_max = stats
        .pump_trade_state_update_duration_ms
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.pump_trade_risk_feature_duration_ms_p95 =
        percentile(&stats.pump_trade_risk_feature_duration_ms, 95, 100);
    summary.pump_trade_risk_feature_duration_ms_max = stats
        .pump_trade_risk_feature_duration_ms
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.unknown_mint_route_count_by_class =
        top_key_summaries(&stats.unknown_mint_route_count_by_class, 20);
    summary.account_pinned_update_count = stats.account_pinned_update_count;
    summary.backpressure_hot_key = stats.backpressure_hot_key.clone();
    summary.backpressure_hot_mint = stats.backpressure_hot_mint.clone();
    summary.backpressure_hot_account = stats.backpressure_hot_account.clone();
    summary.backpressure_deep_processed_count_at_trigger =
        stats.backpressure_deep_processed_count_at_trigger;
    summary.backpressure_skipped_count_at_trigger = stats.backpressure_skipped_count_at_trigger;
    summary.transaction_signature_seen_count = stats.transaction_signature_seen_count;
    summary.transaction_duplicate_signature_count = stats.transaction_duplicate_signature_count;
    summary.transaction_duplicate_signature_skipped_count =
        stats.transaction_duplicate_signature_skipped_count;
    summary.transaction_prefilter_count = stats.transaction_prefilter_count;
    summary.transaction_deep_processed_count = stats.transaction_deep_processed_count;
    summary.transaction_mapping_hint_only_count = stats.transaction_mapping_hint_only_count;
    summary.transaction_untracked_pump_skipped_count =
        stats.transaction_untracked_pump_skipped_count;
    summary.transaction_account_pinned_unknown_count =
        stats.transaction_account_pinned_unknown_count;
    summary.transaction_tombstoned_mint_skipped_count =
        stats.transaction_tombstoned_mint_skipped_count;
    summary.transaction_malformed_or_unknown_count = stats.transaction_malformed_or_unknown_count;
    summary.transaction_other_untracked_skipped_count =
        stats.transaction_other_untracked_skipped_count;
    summary.account_pinned_active_count = stats.account_pinned_active_count;
    summary.account_pinned_unknown_count = stats.account_pinned_unknown_count;
    summary.account_pinned_skipped_count = stats.account_pinned_skipped_count;
    summary.account_pinned_deep_processed_count = stats.account_pinned_deep_processed_count;
    summary.active_mint_transaction_update_count = stats.active_mint_transaction_update_count;
    summary.active_mint_transaction_deep_processed_count =
        stats.active_mint_transaction_deep_processed_count;
    summary.active_mint_transaction_skipped_count = stats.active_mint_transaction_skipped_count;
    summary.active_mint_transaction_coalesced_count = stats.active_mint_transaction_coalesced_count;
    summary.active_mint_transaction_dirty_feature_count =
        stats.active_mint_transaction_dirty_feature_count;
    summary.active_mint_transaction_delta_flush_count =
        stats.active_mint_transaction_delta_flush_count;
    summary.active_mint_transaction_budget_exceeded_count =
        stats.active_mint_transaction_budget_exceeded_count;
    summary.active_mint_transaction_degraded_count = stats.active_mint_transaction_degraded_count;
    summary.active_mint_transaction_queue_pressure_count =
        stats.active_mint_transaction_queue_pressure_count;
    summary.top_active_mints_by_transaction_count =
        top_key_summaries(&stats.top_active_mint_transaction_counts, 20);
    summary.top_active_mints_by_coalesced_count =
        top_key_summaries(&stats.top_active_mint_coalesced_counts, 20);
    summary.top_active_mints_by_deep_processed_count =
        top_key_summaries(&stats.top_active_mint_deep_processed_counts, 20);
    summary.top_active_mints_by_queue_pressure =
        top_key_summaries(&stats.top_active_mint_queue_pressure_counts, 20);
    summary.top_active_mints_by_transaction_lag =
        top_key_summaries(&stats.top_active_mint_transaction_lag, 20);
    summary.active_mint_delta_flush_duration_ms_p95 =
        percentile(&stats.active_mint_delta_flush_duration_ms, 95, 100);
    summary.active_mint_delta_flush_duration_ms_max = stats
        .active_mint_delta_flush_duration_ms
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.degraded_active_mint_count = stats.degraded_active_mints.len() as u64;
    let mut degraded_active_mints = stats
        .degraded_active_mints
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    degraded_active_mints.sort();
    degraded_active_mints.truncate(100);
    summary.degraded_active_mints = degraded_active_mints;
    summary.partition_queue_pressure_preempted_count =
        stats.partition_queue_pressure_preempted_count;
    summary.partition_queue_pressure_dominant_mint =
        stats.partition_queue_pressure_dominant_mint.clone();
    summary.partition_queue_pressure_dominant_mint_update_count =
        stats.partition_queue_pressure_dominant_mint_update_count;
    summary.partition_queue_pressure_degraded_mint =
        stats.partition_queue_pressure_degraded_mint.clone();
    summary.partition_queue_pressure_preempted_before_full =
        stats.partition_queue_pressure_preempted_before_full;
    summary.partition_queue_full_after_preemption = stats.partition_queue_full_after_preemption;
    summary.preemptive_noisy_mint_degraded = stats.partition_queue_pressure_preempted_before_full;
    summary.transaction_feature_deferred_count = stats.transaction_feature_deferred_count;
    summary.transaction_feature_recompute_count = stats.transaction_feature_recompute_count;
    summary.transaction_risk_feature_duration_ms_p95 =
        percentile(&stats.transaction_risk_feature_duration_ms, 95, 100);
    summary.transaction_risk_feature_duration_ms_max = stats
        .transaction_risk_feature_duration_ms
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.transaction_state_update_duration_ms_p95 =
        percentile(&stats.transaction_state_update_duration_ms, 95, 100);
    summary.transaction_state_update_duration_ms_max = stats
        .transaction_state_update_duration_ms
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.transaction_deep_process_duration_ms_p95 =
        percentile(&stats.transaction_deep_process_duration_ms, 95, 100);
    summary.transaction_deep_process_duration_ms_max = stats
        .transaction_deep_process_duration_ms
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.transaction_prefilter_duration_ms_p95 =
        percentile(&stats.transaction_prefilter_duration_ms, 95, 100);
    summary.transaction_prefilter_duration_ms_max = stats
        .transaction_prefilter_duration_ms
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.backpressure_transaction_class = stats.backpressure_transaction_class.clone();
    summary.backpressure_transaction_signature = stats.backpressure_transaction_signature.clone();
    summary.backpressure_transaction_mint = stats.backpressure_transaction_mint.clone();
    summary.backpressure_transaction_account = stats.backpressure_transaction_account.clone();
    summary.backpressure_deep_transaction_count_at_trigger =
        stats.backpressure_deep_transaction_count_at_trigger;
    summary.backpressure_skipped_transaction_count_at_trigger =
        stats.backpressure_skipped_transaction_count_at_trigger;
    summary.backpressure_account_pinned_count_at_trigger =
        stats.backpressure_account_pinned_count_at_trigger;
    summary.partition_decode_duration_ms_p50 =
        percentile(&stats.partition_decode_duration_ms, 50, 100);
    summary.partition_decode_duration_ms_p95 =
        percentile(&stats.partition_decode_duration_ms, 95, 100);
    summary.partition_decode_duration_ms_p99 =
        percentile(&stats.partition_decode_duration_ms, 99, 100);
    summary.partition_decode_duration_ms_max = stats
        .partition_decode_duration_ms
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.partition_state_update_duration_ms_p50 = 0;
    summary.partition_state_update_duration_ms_p95 = 0;
    summary.partition_state_update_duration_ms_p99 = 0;
    summary.partition_state_update_duration_ms_max = 0;
    summary.partition_lock_wait_ms_max = 0;
    summary.partition_batch_size_p50 = percentile(&stats.partition_batch_sizes, 50, 100);
    summary.partition_batch_size_p95 = percentile(&stats.partition_batch_sizes, 95, 100);
    summary.partition_batch_size_max = stats
        .partition_batch_sizes
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    summary.worker_backpressure_detected = stats.worker_backpressure_detected;
    summary.dirty_partition_queued_updates_discarded =
        stats.dirty_partition_queued_updates_discarded;
    summary.partition_worker_reset_count = stats.partition_worker_reset_count;
    summary.artifact_queue_depth_max = stats.artifact_queue_depth_max;
    summary.artifact_queue_full_count = stats.artifact_queue_full_count;
}

pub fn material_hunter_subscription_fingerprint(loaded: &LoadedConfig) -> Result<String> {
    let ingest = GeyserIngestService::new(loaded.config.geyser.clone());
    let request = ingest.proto_subscription_request();
    let bytes = request.encode_to_vec();
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MaterialHunterStreamStateHint {
    pub active_mints: Vec<String>,
    pub tombstoned_mints: Vec<String>,
    pub inactive_mints: Vec<String>,
}

impl MaterialHunterStreamStateHint {
    pub fn is_empty(&self) -> bool {
        self.active_mints.is_empty()
            && self.tombstoned_mints.is_empty()
            && self.inactive_mints.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterialHunterStreamAction {
    Continue,
    ContinueWithStateHint(MaterialHunterStreamStateHint),
    Stop,
}

impl MaterialHunterStreamAction {
    fn is_stop(&self) -> bool {
        matches!(self, Self::Stop)
    }

    fn state_hint(&self) -> Option<&MaterialHunterStreamStateHint> {
        match self {
            Self::ContinueWithStateHint(hint) => Some(hint),
            Self::Continue | Self::Stop => None,
        }
    }
}

#[derive(Debug, Default)]
struct MaterialHunterRelevanceState {
    active_mints: HashSet<String>,
    tombstoned_mints: HashSet<String>,
}

#[derive(Debug, Clone)]
struct MaterialHunterActiveMintPressureConfig {
    max_queued_updates_per_mint: u64,
    max_updates_per_second: u64,
    max_deep_updates_per_checkpoint: u64,
    noisy_degrade_enabled: bool,
    noisy_degrade_reason: String,
    coalesce_window: StdDuration,
    delta_flush_interval: StdDuration,
    partition_soft_queue_threshold_ratio: f64,
}

impl MaterialHunterActiveMintPressureConfig {
    fn from_geyser(config: &common::GeyserConfig) -> Self {
        Self {
            max_queued_updates_per_mint: config
                .material_hunter_active_mint_max_queued_updates_per_mint
                .max(1),
            max_updates_per_second: config
                .material_hunter_active_mint_max_updates_per_second
                .max(1),
            max_deep_updates_per_checkpoint: config
                .material_hunter_active_mint_max_deep_updates_per_checkpoint
                .max(1),
            noisy_degrade_enabled: config.material_hunter_active_mint_noisy_degrade_enabled,
            noisy_degrade_reason: config
                .material_hunter_active_mint_noisy_degrade_reason
                .clone(),
            coalesce_window: StdDuration::from_millis(
                config.material_hunter_active_mint_coalesce_window_ms.max(1),
            ),
            delta_flush_interval: StdDuration::from_millis(
                config
                    .material_hunter_active_mint_delta_flush_interval_ms
                    .max(1),
            ),
            partition_soft_queue_threshold_ratio: config
                .material_hunter_partition_soft_queue_threshold_ratio
                .clamp(0.01, 0.99),
        }
    }
}

#[derive(Debug, Default, Clone)]
struct MaterialHunterActiveMintPressureEntry {
    window_started_at: Option<tokio::time::Instant>,
    window_update_count: u64,
    deep_updates_since_checkpoint: u64,
    coalesced_updates_since_flush: u64,
    last_deep_process_at: Option<tokio::time::Instant>,
    last_delta_flush_at: Option<tokio::time::Instant>,
    degraded: bool,
}

#[derive(Debug, Default)]
struct MaterialHunterActiveMintPressureState {
    by_mint: HashMap<String, MaterialHunterActiveMintPressureEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MaterialHunterActiveMintPressureDecision {
    DeepProcess,
    Coalesce,
    Degrade {
        reason: String,
        queue_pressure: bool,
    },
}

fn material_hunter_active_mint_pressure_decision(
    state: &mut MaterialHunterActiveMintPressureState,
    mint: &str,
    now: tokio::time::Instant,
    partition_depth: u64,
    partition_capacity: u64,
    config: &MaterialHunterActiveMintPressureConfig,
) -> MaterialHunterActiveMintPressureDecision {
    let entry = state.by_mint.entry(mint.to_owned()).or_default();
    if entry.degraded {
        return MaterialHunterActiveMintPressureDecision::Coalesce;
    }
    let window_started_at = entry.window_started_at.get_or_insert(now);
    if now.duration_since(*window_started_at) >= StdDuration::from_secs(1) {
        entry.window_started_at = Some(now);
        entry.window_update_count = 0;
    }
    entry.window_update_count = entry.window_update_count.saturating_add(1);

    let soft_depth =
        ((partition_capacity as f64) * config.partition_soft_queue_threshold_ratio).ceil() as u64;
    let per_mint_queue_pressure = entry
        .window_update_count
        .saturating_add(entry.coalesced_updates_since_flush)
        >= config.max_queued_updates_per_mint;
    let partition_queue_pressure = partition_capacity > 0
        && partition_depth >= soft_depth.max(1)
        && entry.window_update_count >= config.max_updates_per_second.saturating_div(4).max(1);
    let budget_exceeded = entry.window_update_count > config.max_updates_per_second
        || entry.deep_updates_since_checkpoint >= config.max_deep_updates_per_checkpoint;
    if config.noisy_degrade_enabled
        && (per_mint_queue_pressure || partition_queue_pressure || budget_exceeded)
    {
        entry.degraded = true;
        return MaterialHunterActiveMintPressureDecision::Degrade {
            reason: if budget_exceeded {
                "active_mint_processing_budget_exceeded".to_owned()
            } else {
                config.noisy_degrade_reason.clone()
            },
            queue_pressure: per_mint_queue_pressure || partition_queue_pressure,
        };
    }

    let should_flush = entry
        .last_delta_flush_at
        .map(|last| now.duration_since(last) >= config.delta_flush_interval)
        .unwrap_or(true);
    let should_deep = entry
        .last_deep_process_at
        .map(|last| now.duration_since(last) >= config.coalesce_window)
        .unwrap_or(true);
    if should_flush || should_deep {
        entry.last_deep_process_at = Some(now);
        entry.last_delta_flush_at = Some(now);
        entry.deep_updates_since_checkpoint = entry.deep_updates_since_checkpoint.saturating_add(1);
        entry.coalesced_updates_since_flush = 0;
        return MaterialHunterActiveMintPressureDecision::DeepProcess;
    }
    entry.coalesced_updates_since_flush = entry.coalesced_updates_since_flush.saturating_add(1);
    MaterialHunterActiveMintPressureDecision::Coalesce
}

fn apply_material_hunter_state_hint(
    relevance: &Arc<std::sync::Mutex<MaterialHunterRelevanceState>>,
    hint: &MaterialHunterStreamStateHint,
) {
    let Ok(mut state) = relevance.lock() else {
        return;
    };
    for mint in &hint.active_mints {
        state.tombstoned_mints.remove(mint);
        state.active_mints.insert(mint.clone());
    }
    for mint in &hint.inactive_mints {
        state.active_mints.remove(mint);
    }
    for mint in &hint.tombstoned_mints {
        state.active_mints.remove(mint);
        state.tombstoned_mints.insert(mint.clone());
    }
}

fn material_hunter_status_class(status: &Status) -> (&'static str, bool, bool) {
    let rendered = status.to_string().to_ascii_lowercase();
    let message = status.message().to_ascii_lowercase();
    if rendered.contains("lagged")
        || message.contains("lagged")
        || rendered.contains("unrecoverable data loss")
        || message.contains("unrecoverable data loss")
        || (rendered.contains("corruption") && rendered.contains("data loss"))
        || (message.contains("corruption") && message.contains("data loss"))
    {
        return ("provider_lagged_data_loss", false, true);
    }
    match status.code() {
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
            ("auth_rejected", false, false)
        }
        tonic::Code::Unimplemented => ("unsupported", false, false),
        tonic::Code::Unavailable
        | tonic::Code::Unknown
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::ResourceExhausted => ("transient_stream_error", true, false),
        _ => ("stream_error", false, false),
    }
}

fn should_retain_fresh_launch_event(
    event: &NormalizedEvent,
    retain_only_tracked_mints: bool,
    tracked_mints: &HashSet<String>,
    tracked_launch_slots: &HashSet<u64>,
    tracked_related_signatures: &HashSet<String>,
) -> bool {
    if !retain_only_tracked_mints {
        return true;
    }
    match &event.payload {
        EventPayload::TokenCreated(payload) => {
            payload.status != TransactionStatus::Failed && tracked_mints.contains(&payload.mint.0)
        }
        EventPayload::ObservedTransaction(payload) => {
            tracked_launch_slots.contains(&event.meta.slot)
                || payload
                    .signature_hint
                    .as_ref()
                    .map(|signature| tracked_related_signatures.contains(signature))
                    .unwrap_or(false)
                || (payload
                    .program_ids
                    .iter()
                    .any(|program| program == "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
                    && tracked_launch_slots.iter().any(|launch_slot| {
                        event.meta.slot >= *launch_slot
                            && event.meta.slot <= launch_slot.saturating_add(64)
                    }))
        }
        _ => event
            .mint()
            .map(|mint| tracked_mints.contains(&mint.0))
            .unwrap_or(false),
    }
}

const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPq9sJqzQdbqT6qhHV4";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterialHunterPumpPrefilterDecision {
    DeepProcess,
    SkipUntracked,
    SkipTombstoned,
    SkipUnknownMint,
    SkipMalformed,
    SkipOther,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MaterialHunterPumpPrefilter {
    update_class: &'static str,
    decision: MaterialHunterPumpPrefilterDecision,
    mint: Option<String>,
    account: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterialHunterTransactionPrefilterDecision {
    DeepProcess,
    SkipMappingHintOnly,
    SkipUntrackedPump,
    SkipAccountPinnedUnknown,
    SkipTombstoned,
    SkipDuplicateSignature,
    SkipMalformedOrUnknown,
    SkipOtherUntracked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MaterialHunterTransactionPrefilter {
    update_class: &'static str,
    decision: MaterialHunterTransactionPrefilterDecision,
    mint: Option<String>,
    account: Option<String>,
    signature: Option<String>,
}

fn material_hunter_pump_discriminator_name(data: &[u8]) -> Option<&'static str> {
    if data.len() < 8 {
        return None;
    }
    let discriminator = &data[..8];
    if discriminator == anchor_discriminator("global", "create") {
        return Some("create");
    }
    if discriminator == anchor_discriminator("global", "create_v2") {
        return Some("create_v2");
    }
    if discriminator == anchor_discriminator("global", "buy")
        || discriminator == anchor_discriminator("global", "buy_v2")
        || discriminator == anchor_discriminator("global", "buy_exact_quote_in_v2")
    {
        return Some("buy");
    }
    if discriminator == anchor_discriminator("global", "sell")
        || discriminator == anchor_discriminator("global", "sell_v2")
    {
        return Some("sell");
    }
    Some("other")
}

fn material_hunter_transaction_mint_hint(update: &SubscribeUpdate) -> Option<String> {
    let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_ref() else {
        return None;
    };
    let info = tx.transaction.as_ref()?;
    let meta = info.meta.as_ref()?;
    meta.post_token_balances
        .iter()
        .chain(meta.pre_token_balances.iter())
        .find_map(|balance| {
            let mint = balance.mint.trim();
            (!mint.is_empty()).then(|| mint.to_owned())
        })
}

fn material_hunter_transaction_signature_hint(update: &SubscribeUpdate) -> Option<String> {
    let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_ref() else {
        return None;
    };
    let info = tx.transaction.as_ref()?;
    if !info.signature.is_empty() {
        return Some(bs58::encode(&info.signature).into_string());
    }
    info.transaction
        .as_ref()
        .and_then(|transaction| transaction.signatures.first())
        .filter(|signature| !signature.is_empty())
        .map(|signature| bs58::encode(signature).into_string())
}

fn material_hunter_transaction_account_hint(update: &SubscribeUpdate) -> Option<String> {
    let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_ref() else {
        return None;
    };
    let info = tx.transaction.as_ref()?;
    let message = info.transaction.as_ref()?.message.as_ref()?;
    message
        .account_keys
        .first()
        .map(|key| bs58::encode(key).into_string())
}

fn material_hunter_transaction_account_keys(update: &SubscribeUpdate) -> Vec<Vec<u8>> {
    let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_ref() else {
        return Vec::new();
    };
    tx.transaction
        .as_ref()
        .and_then(|info| info.transaction.as_ref())
        .and_then(|transaction| transaction.message.as_ref())
        .map(|message| message.account_keys.clone())
        .unwrap_or_default()
}

fn material_hunter_transaction_has_pump_program(update: &SubscribeUpdate) -> bool {
    material_hunter_transaction_account_keys(update)
        .iter()
        .any(|key| bs58::encode(key).into_string() == PUMP_PROGRAM_ID)
}

fn material_hunter_transaction_has_pump_create(update: &SubscribeUpdate) -> bool {
    let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_ref() else {
        return false;
    };
    let Some(message) = tx
        .transaction
        .as_ref()
        .and_then(|info| info.transaction.as_ref())
        .and_then(|transaction| transaction.message.as_ref())
    else {
        return false;
    };
    message.instructions.iter().any(|instruction| {
        message
            .account_keys
            .get(instruction.program_id_index as usize)
            .map(|program_id| bs58::encode(program_id).into_string() == PUMP_PROGRAM_ID)
            .unwrap_or(false)
            && matches!(
                material_hunter_pump_discriminator_name(&instruction.data),
                Some("create") | Some("create_v2")
            )
    })
}

fn material_hunter_signature_seen_or_insert(
    seen_signatures: &mut HashSet<String>,
    signature_lru: &mut VecDeque<String>,
    signature: &str,
    capacity: usize,
) -> bool {
    if signature.trim().is_empty() {
        return false;
    }
    if seen_signatures.contains(signature) {
        return true;
    }
    seen_signatures.insert(signature.to_owned());
    signature_lru.push_back(signature.to_owned());
    while seen_signatures.len() > capacity.max(1) {
        if let Some(old) = signature_lru.pop_front() {
            seen_signatures.remove(&old);
        } else {
            break;
        }
    }
    false
}

fn material_hunter_prefilter_transaction_update(
    update: &SubscribeUpdate,
    active_mints: &HashSet<String>,
    tombstoned_mints: &HashSet<String>,
    token_account_to_mint: &HashMap<Vec<u8>, String>,
    account_partition_pins: &HashMap<Vec<u8>, usize>,
    duplicate_signature: bool,
) -> Option<MaterialHunterTransactionPrefilter> {
    let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_ref() else {
        return None;
    };
    let signature = material_hunter_transaction_signature_hint(update);
    if duplicate_signature {
        return Some(MaterialHunterTransactionPrefilter {
            update_class: "transaction_duplicate_signature",
            decision: MaterialHunterTransactionPrefilterDecision::SkipDuplicateSignature,
            mint: material_hunter_transaction_mint_hint(update),
            account: material_hunter_transaction_account_hint(update),
            signature,
        });
    }
    let Some(info) = tx.transaction.as_ref() else {
        return Some(MaterialHunterTransactionPrefilter {
            update_class: "transaction_malformed_or_unknown",
            decision: MaterialHunterTransactionPrefilterDecision::SkipMalformedOrUnknown,
            mint: None,
            account: None,
            signature,
        });
    };
    let account_keys = material_hunter_transaction_account_keys(update);
    if account_keys.is_empty() && info.meta.is_none() {
        return Some(MaterialHunterTransactionPrefilter {
            update_class: "transaction_malformed_or_unknown",
            decision: MaterialHunterTransactionPrefilterDecision::SkipMalformedOrUnknown,
            mint: None,
            account: None,
            signature,
        });
    }
    let mint = material_hunter_transaction_mint_hint(update);
    let account = material_hunter_transaction_account_hint(update);
    let has_pump = material_hunter_transaction_has_pump_program(update);
    if material_hunter_transaction_has_pump_create(update) {
        return Some(MaterialHunterTransactionPrefilter {
            update_class: "transaction_token_created",
            decision: MaterialHunterTransactionPrefilterDecision::DeepProcess,
            mint,
            account,
            signature,
        });
    }
    let token_balance_mints = info
        .meta
        .as_ref()
        .map(|meta| {
            meta.post_token_balances
                .iter()
                .chain(meta.pre_token_balances.iter())
                .filter_map(|balance| {
                    let mint = balance.mint.trim();
                    (!mint.is_empty()).then(|| mint.to_owned())
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mapped_mints = account_keys
        .iter()
        .filter_map(|key| token_account_to_mint.get(key).cloned())
        .collect::<Vec<_>>();
    let active_mint = token_balance_mints
        .iter()
        .chain(mapped_mints.iter())
        .find(|mint| active_mints.contains(*mint))
        .cloned();
    if let Some(active) = active_mint {
        let account = account_keys
            .iter()
            .find(|key| {
                token_account_to_mint
                    .get(*key)
                    .map(|mint| mint == &active)
                    .unwrap_or(false)
            })
            .map(|key| bs58::encode(key).into_string())
            .or(account);
        let update_class = if token_balance_mints.iter().any(|mint| mint == &active) {
            "transaction_active_mint"
        } else {
            "transaction_active_account"
        };
        return Some(MaterialHunterTransactionPrefilter {
            update_class,
            decision: MaterialHunterTransactionPrefilterDecision::DeepProcess,
            mint: Some(active),
            account,
            signature,
        });
    }
    let tombstoned_mint = token_balance_mints
        .iter()
        .chain(mapped_mints.iter())
        .find(|mint| tombstoned_mints.contains(*mint))
        .cloned();
    if let Some(tombstoned) = tombstoned_mint {
        return Some(MaterialHunterTransactionPrefilter {
            update_class: "transaction_tombstoned_mint",
            decision: MaterialHunterTransactionPrefilterDecision::SkipTombstoned,
            mint: Some(tombstoned),
            account,
            signature,
        });
    }
    if account_keys
        .iter()
        .any(|account| account_partition_pins.contains_key(account))
    {
        return Some(MaterialHunterTransactionPrefilter {
            update_class: "transaction_account_pinned_unknown",
            decision: MaterialHunterTransactionPrefilterDecision::SkipAccountPinnedUnknown,
            mint,
            account,
            signature,
        });
    }
    if has_pump {
        return Some(MaterialHunterTransactionPrefilter {
            update_class: "transaction_untracked_pump",
            decision: MaterialHunterTransactionPrefilterDecision::SkipUntrackedPump,
            mint,
            account,
            signature,
        });
    }
    if !token_balance_mints.is_empty() || !mapped_mints.is_empty() {
        return Some(MaterialHunterTransactionPrefilter {
            update_class: "transaction_mapping_hint_only",
            decision: MaterialHunterTransactionPrefilterDecision::SkipMappingHintOnly,
            mint,
            account,
            signature,
        });
    }
    Some(MaterialHunterTransactionPrefilter {
        update_class: "transaction_other_untracked",
        decision: MaterialHunterTransactionPrefilterDecision::SkipOtherUntracked,
        mint,
        account,
        signature,
    })
}

fn material_hunter_prefilter_pump_instruction(
    update: &SubscribeUpdate,
    active_mints: &HashSet<String>,
    tombstoned_mints: &HashSet<String>,
) -> Option<MaterialHunterPumpPrefilter> {
    let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_ref() else {
        return None;
    };
    let info = tx.transaction.as_ref()?;
    let transaction = info.transaction.as_ref()?;
    let message = transaction.message.as_ref()?;
    let mint = material_hunter_transaction_mint_hint(update);
    let account = material_hunter_transaction_account_hint(update);
    let mut saw_pump = false;
    let mut saw_malformed = false;
    let mut saw_create = false;
    let mut saw_trade = false;
    for instruction in &message.instructions {
        let Some(program_id) = message
            .account_keys
            .get(instruction.program_id_index as usize)
        else {
            saw_malformed = true;
            continue;
        };
        if bs58::encode(program_id).into_string() != PUMP_PROGRAM_ID {
            continue;
        }
        saw_pump = true;
        match material_hunter_pump_discriminator_name(&instruction.data) {
            Some("create") | Some("create_v2") => saw_create = true,
            Some("buy") | Some("sell") => saw_trade = true,
            Some("other") => {}
            None => saw_malformed = true,
            _ => {}
        }
    }
    if !saw_pump {
        return None;
    }
    if saw_create {
        return Some(MaterialHunterPumpPrefilter {
            update_class: "pump_token_created",
            decision: MaterialHunterPumpPrefilterDecision::DeepProcess,
            mint,
            account,
        });
    }
    if saw_trade {
        let Some(mint_value) = mint.clone() else {
            return Some(MaterialHunterPumpPrefilter {
                update_class: "pump_trade_unknown_mint",
                decision: MaterialHunterPumpPrefilterDecision::SkipUnknownMint,
                mint,
                account,
            });
        };
        if tombstoned_mints.contains(&mint_value) {
            return Some(MaterialHunterPumpPrefilter {
                update_class: "pump_trade_tombstoned_mint",
                decision: MaterialHunterPumpPrefilterDecision::SkipTombstoned,
                mint,
                account,
            });
        }
        if active_mints.contains(&mint_value) {
            return Some(MaterialHunterPumpPrefilter {
                update_class: "pump_trade_active_mint",
                decision: MaterialHunterPumpPrefilterDecision::DeepProcess,
                mint,
                account,
            });
        }
        return Some(MaterialHunterPumpPrefilter {
            update_class: "pump_trade_untracked_mint",
            decision: MaterialHunterPumpPrefilterDecision::SkipUntracked,
            mint,
            account,
        });
    }
    if saw_malformed {
        return Some(MaterialHunterPumpPrefilter {
            update_class: "pump_instruction_malformed",
            decision: MaterialHunterPumpPrefilterDecision::SkipMalformed,
            mint,
            account,
        });
    }
    Some(MaterialHunterPumpPrefilter {
        update_class: "pump_instruction_other",
        decision: MaterialHunterPumpPrefilterDecision::SkipOther,
        mint,
        account,
    })
}

fn material_hunter_update_class(update: &SubscribeUpdate) -> &'static str {
    match update.update_oneof.as_ref() {
        Some(UpdateOneof::Slot(_)) | Some(UpdateOneof::Ping(_)) | Some(UpdateOneof::Pong(_)) => {
            "slot_or_liveness_reader_side"
        }
        Some(UpdateOneof::Transaction(tx)) => {
            if let Some(info) = tx.transaction.as_ref() {
                if let Some(transaction) = info.transaction.as_ref() {
                    if let Some(message) = transaction.message.as_ref() {
                        if message
                            .account_keys
                            .iter()
                            .any(|key| bs58::encode(key).into_string() == PUMP_PROGRAM_ID)
                        {
                            return "pump_trade_or_instruction";
                        }
                    }
                }
                if info
                    .meta
                    .as_ref()
                    .map(|meta| {
                        !meta.post_token_balances.is_empty() || !meta.pre_token_balances.is_empty()
                    })
                    .unwrap_or(false)
                {
                    return "transaction_update_relevant";
                }
            }
            "transaction_update_untracked"
        }
        Some(UpdateOneof::TransactionStatus(_)) => "transaction_update_untracked",
        Some(UpdateOneof::Account(account_update)) => {
            let Some(account) = account_update.account.as_ref() else {
                return "account_update_untracked";
            };
            if account.data.is_empty() {
                return "account_update_untracked";
            }
            let owner = bs58::encode(&account.owner).into_string();
            if owner == SPL_TOKEN_PROGRAM_ID || owner == TOKEN_2022_PROGRAM_ID {
                "token_account_update_untracked"
            } else {
                "owner_account_update"
            }
        }
        _ => "unknown_worker_relevant",
    }
}

fn material_hunter_update_needs_worker(update: &SubscribeUpdate) -> bool {
    !matches!(
        material_hunter_update_class(update),
        "slot_or_liveness_reader_side" | "account_update_untracked"
    )
}

fn material_hunter_partition_hash(bytes: &[u8]) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    u64::from_le_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ])
}

fn material_hunter_partition_for_key(key: &[u8], partitions: usize) -> usize {
    if partitions <= 1 {
        return 0;
    }
    (material_hunter_partition_hash(key) as usize) % partitions
}

fn material_hunter_partition_key(update: &SubscribeUpdate) -> (Vec<u8>, bool, String) {
    match update.update_oneof.as_ref() {
        Some(UpdateOneof::Transaction(tx)) => {
            if let Some(info) = tx.transaction.as_ref() {
                if let Some(meta) = info.meta.as_ref() {
                    if let Some(balance) = meta
                        .post_token_balances
                        .iter()
                        .chain(meta.pre_token_balances.iter())
                        .find(|balance| !balance.mint.trim().is_empty())
                    {
                        return (
                            balance.mint.as_bytes().to_vec(),
                            false,
                            format!("mint:{}", balance.mint),
                        );
                    }
                }
                if let Some(transaction) = info.transaction.as_ref() {
                    if let Some(message) = transaction.message.as_ref() {
                        if let Some(key) = message.account_keys.first() {
                            return (
                                key.clone(),
                                false,
                                format!("account:{}", bs58::encode(key).into_string()),
                            );
                        }
                    }
                    if let Some(signature) = transaction.signatures.first() {
                        return (
                            signature.clone(),
                            false,
                            format!("signature:{}", bs58::encode(signature).into_string()),
                        );
                    }
                }
                if !info.signature.is_empty() {
                    return (
                        info.signature.clone(),
                        false,
                        format!("signature:{}", bs58::encode(&info.signature).into_string()),
                    );
                }
            }
            (
                tx.slot.to_le_bytes().to_vec(),
                true,
                format!("slot:{}", tx.slot),
            )
        }
        Some(UpdateOneof::TransactionStatus(status)) => {
            if !status.signature.is_empty() {
                (
                    status.signature.clone(),
                    false,
                    format!(
                        "signature:{}",
                        bs58::encode(&status.signature).into_string()
                    ),
                )
            } else {
                (
                    status.slot.to_le_bytes().to_vec(),
                    true,
                    format!("slot:{}", status.slot),
                )
            }
        }
        Some(UpdateOneof::Account(account_update)) => {
            if let Some(account) = account_update.account.as_ref() {
                if !account.pubkey.is_empty() {
                    return (
                        account.pubkey.clone(),
                        false,
                        format!("account:{}", bs58::encode(&account.pubkey).into_string()),
                    );
                }
                if !account.owner.is_empty() {
                    return (
                        account.owner.clone(),
                        false,
                        format!("owner:{}", bs58::encode(&account.owner).into_string()),
                    );
                }
            }
            (
                account_update.slot.to_le_bytes().to_vec(),
                true,
                format!("slot:{}", account_update.slot),
            )
        }
        Some(UpdateOneof::Block(block)) => (
            block.slot.to_le_bytes().to_vec(),
            true,
            format!("slot:{}", block.slot),
        ),
        Some(UpdateOneof::BlockMeta(block)) => (
            block.slot.to_le_bytes().to_vec(),
            true,
            format!("slot:{}", block.slot),
        ),
        Some(UpdateOneof::Entry(entry)) => (
            entry.slot.to_le_bytes().to_vec(),
            true,
            format!("slot:{}", entry.slot),
        ),
        _ => (
            b"material-hunter-fallback".to_vec(),
            true,
            "fallback:unknown".to_owned(),
        ),
    }
}

fn material_hunter_partition_for_update(
    update: &SubscribeUpdate,
    partitions: usize,
) -> (usize, bool, String) {
    let (key, fallback, label) = material_hunter_partition_key(update);
    (
        material_hunter_partition_for_key(&key, partitions),
        fallback,
        label,
    )
}

fn material_hunter_token_account_mint_mappings(update: &SubscribeUpdate) -> Vec<(Vec<u8>, String)> {
    let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_ref() else {
        return Vec::new();
    };
    let Some(info) = tx.transaction.as_ref() else {
        return Vec::new();
    };
    let account_keys = info
        .transaction
        .as_ref()
        .and_then(|transaction| transaction.message.as_ref())
        .map(|message| message.account_keys.as_slice())
        .unwrap_or(&[]);
    let Some(meta) = info.meta.as_ref() else {
        return Vec::new();
    };
    let mut mappings = Vec::new();
    for balance in meta
        .post_token_balances
        .iter()
        .chain(meta.pre_token_balances.iter())
    {
        if balance.mint.trim().is_empty() {
            continue;
        }
        let Some(account_key) = account_keys.get(balance.account_index as usize) else {
            continue;
        };
        if account_key.is_empty() {
            continue;
        }
        mappings.push((account_key.clone(), balance.mint.clone()));
    }
    mappings
}

fn material_hunter_partition_for_update_with_account_map(
    update: &SubscribeUpdate,
    partitions: usize,
    token_account_to_mint: &HashMap<Vec<u8>, String>,
    account_partition_pins: &mut HashMap<Vec<u8>, usize>,
) -> (usize, bool, String) {
    if let Some(UpdateOneof::Account(account_update)) = update.update_oneof.as_ref() {
        if let Some(account) = account_update.account.as_ref() {
            if !account.pubkey.is_empty() {
                if let Some(partition) = account_partition_pins.get(&account.pubkey).copied() {
                    return (
                        partition,
                        false,
                        format!(
                            "account_pinned:{}",
                            bs58::encode(&account.pubkey).into_string()
                        ),
                    );
                }
                if let Some(mint) = token_account_to_mint.get(&account.pubkey) {
                    let partition = material_hunter_partition_for_key(mint.as_bytes(), partitions);
                    account_partition_pins.insert(account.pubkey.clone(), partition);
                    return (partition, false, format!("mint:{mint}"));
                }
                let (partition, fallback, label) =
                    material_hunter_partition_for_update(update, partitions);
                account_partition_pins.insert(account.pubkey.clone(), partition);
                return (partition, fallback, label);
            }
        }
    }
    material_hunter_partition_for_update(update, partitions)
}

pub async fn run_material_hunter_stream<F>(
    loaded: &LoadedConfig,
    options: MaterialHunterStreamOptions,
    on_event: F,
) -> Result<MaterialHunterStreamSummary>
where
    F: FnMut(NormalizedEvent, &MaterialHunterStreamSummary) -> Result<MaterialHunterStreamAction>,
{
    run_material_hunter_stream_with_progress(loaded, options, on_event, |_summary| {
        Ok(MaterialHunterStreamAction::Continue)
    })
    .await
}

pub async fn run_material_hunter_stream_with_progress<F, P>(
    loaded: &LoadedConfig,
    options: MaterialHunterStreamOptions,
    on_event: F,
    on_progress: P,
) -> Result<MaterialHunterStreamSummary>
where
    F: FnMut(NormalizedEvent, &MaterialHunterStreamSummary) -> Result<MaterialHunterStreamAction>,
    P: FnMut(&MaterialHunterStreamSummary) -> Result<MaterialHunterStreamAction>,
{
    run_material_hunter_stream_with_connector(
        loaded,
        options,
        Arc::new(RealGeyserConnector),
        on_event,
        on_progress,
    )
    .await
}

pub async fn run_material_hunter_stream_with_connector<F, P>(
    loaded: &LoadedConfig,
    options: MaterialHunterStreamOptions,
    connector: Arc<dyn GeyserStreamConnector>,
    mut on_event: F,
    mut on_progress: P,
) -> Result<MaterialHunterStreamSummary>
where
    F: FnMut(NormalizedEvent, &MaterialHunterStreamSummary) -> Result<MaterialHunterStreamAction>,
    P: FnMut(&MaterialHunterStreamSummary) -> Result<MaterialHunterStreamAction>,
{
    let config = loaded.config.geyser.clone();
    let mut summary = MaterialHunterStreamSummary {
        provider_status: "not_attempted".to_owned(),
        duration_seconds: options.duration_seconds.max(1),
        ..MaterialHunterStreamSummary::default()
    };
    if !geyser_endpoint_configured(&config) {
        summary.provider_status = "not_attempted_missing_endpoint".to_owned();
        summary
            .errors
            .push("geyser endpoint is not configured".to_owned());
        return Err(anyhow!("geyser endpoint is not configured"));
    }

    let deadline =
        tokio::time::Instant::now() + StdDuration::from_secs(options.duration_seconds.max(1));
    let ingest = GeyserIngestService::new(config.clone());
    let request = ingest.proto_subscription_request();
    let max_attempts = config.max_reconnect_attempts.unwrap_or(10).max(1) as usize;
    let mut reconnect_attempts = 0usize;
    let stream_started_at = tokio::time::Instant::now();
    let mut last_provider_progress_at = stream_started_at;
    let mut last_pump_progress_at = stream_started_at;
    if on_progress(&summary)?.is_stop() {
        summary.provider_status = "stopped_by_hunter".to_owned();
        summary.stream_completed_normally = true;
        return Ok(summary);
    }

    'streaming: while tokio::time::Instant::now() < deadline {
        let mut stream = match connector
            .connect_and_subscribe(&config, request.clone())
            .await
        {
            Ok(stream) => {
                summary.provider_status = if reconnect_attempts > 0 {
                    "reconnected".to_owned()
                } else {
                    "connected".to_owned()
                };
                summary.connected = true;
                if on_progress(&summary)?.is_stop() {
                    summary.provider_status = "stopped_by_hunter".to_owned();
                    summary.stream_completed_normally = true;
                    return Ok(summary);
                }
                stream
            }
            Err(error) => {
                if let Some(status) = error.downcast_ref::<Status>() {
                    let (class, retryable, data_loss) = material_hunter_status_class(status);
                    summary.provider_status = class.to_owned();
                    summary.provider_blocker_class = Some(class.to_owned());
                    summary.errors.push(status.to_string());
                    if data_loss {
                        summary.provider_data_loss_seen = true;
                        summary.provider_lagged_count =
                            summary.provider_lagged_count.saturating_add(1);
                        let _ = on_progress(&summary)?;
                        if options.gap_tolerant_segments && tokio::time::Instant::now() < deadline {
                            reconnect_attempts = reconnect_attempts.saturating_add(1);
                            summary.reconnect_attempts =
                                summary.reconnect_attempts.saturating_add(1);
                            if reconnect_attempts < max_attempts {
                                summary.provider_status = "reconnecting_after_gap".to_owned();
                                summary.provider_blocker_class = None;
                                tokio::time::sleep(next_backoff_ms(&config, reconnect_attempts))
                                    .await;
                                continue 'streaming;
                            }
                            summary.provider_status = "provider_reconnect_exhausted".to_owned();
                            summary.provider_blocker_class =
                                Some("provider_reconnect_exhausted".to_owned());
                            let _ = on_progress(&summary)?;
                        }
                        return Ok(summary);
                    }
                    if retryable {
                        reconnect_attempts = reconnect_attempts.saturating_add(1);
                        summary.reconnect_attempts = summary.reconnect_attempts.saturating_add(1);
                        if reconnect_attempts < max_attempts {
                            summary.provider_blocker_class = None;
                            let _ = on_progress(&summary)?;
                            tokio::time::sleep(next_backoff_ms(&config, reconnect_attempts)).await;
                            continue 'streaming;
                        }
                        summary.provider_status = "provider_reconnect_exhausted".to_owned();
                        summary.provider_blocker_class =
                            Some("provider_reconnect_exhausted".to_owned());
                        let _ = on_progress(&summary)?;
                        return Ok(summary);
                    }
                } else {
                    summary.provider_status = "connection_failed".to_owned();
                    summary.provider_blocker_class = Some("connection_failed".to_owned());
                    summary.errors.push(error.to_string());
                }
                let _ = on_progress(&summary)?;
                return Ok(summary);
            }
        };

        let partitioning_enabled = config.material_hunter_partitioning_enabled;
        let worker_partitions = if partitioning_enabled {
            config.material_hunter_worker_partitions.clamp(1, 16)
        } else {
            1
        };
        let queue_capacity = config
            .material_hunter_router_queue_capacity
            .max(config.max_inflight_messages)
            .max(128);
        let partition_queue_capacity = config.material_hunter_partition_queue_capacity.max(1);
        let partition_batch_size = config
            .material_hunter_partition_batch_size
            .clamp(1, partition_queue_capacity);
        let worker_lag_blocker_ms = config.material_hunter_worker_lag_blocker_ms.max(1);
        let (reader_tx, mut router_rx) =
            mpsc::channel::<MaterialHunterReaderMessage>(queue_capacity);
        let (worker_output_tx, mut worker_output_rx) =
            mpsc::channel::<MaterialHunterWorkerOutput>(queue_capacity);
        let reader_stats = Arc::new(std::sync::Mutex::new(MaterialHunterReaderStats {
            queue_capacity: queue_capacity as u64,
            worker_started_at: Some(tokio::time::Instant::now()),
            worker_partitions: worker_partitions as u64,
            partitioning_enabled,
            partition_queue_depth_current: vec![0; worker_partitions],
            partition_queue_depth_max: vec![0; worker_partitions],
            partition_queue_full_count_by_partition: vec![0; worker_partitions],
            partition_updates_processed_by_partition: vec![0; worker_partitions],
            partition_worker_lag_ms_by_partition: vec![Vec::new(); worker_partitions],
            partition_backlog_oldest_update_age_ms_by_partition: vec![0; worker_partitions],
            partition_batch_size_max_by_partition: vec![0; worker_partitions],
            backpressure_threshold_ms: worker_lag_blocker_ms,
            partition_started_at: Some(tokio::time::Instant::now()),
            ..MaterialHunterReaderStats::default()
        }));
        let relevance_state = Arc::new(std::sync::Mutex::new(
            MaterialHunterRelevanceState::default(),
        ));
        let reader_stats_for_task = reader_stats.clone();
        tokio::spawn(async move {
            let mut last_read_at: Option<tokio::time::Instant> = None;
            let mut sequence = 0u64;
            loop {
                let poll_started = tokio::time::Instant::now();
                let next = stream.next().await;
                let read_at = tokio::time::Instant::now();
                if let Ok(mut stats) = reader_stats_for_task.lock() {
                    stats.record_poll_latency(
                        read_at.duration_since(poll_started).as_millis() as u64
                    );
                    if let Some(previous) = last_read_at {
                        stats.record_interarrival(
                            read_at.duration_since(previous).as_millis() as u64
                        );
                    }
                    last_read_at = Some(read_at);
                }
                let update = match next {
                    Some(Ok(update)) => {
                        if let Ok(mut stats) = reader_stats_for_task.lock() {
                            stats.update_count = stats.update_count.saturating_add(1);
                            stats.record_update_kind(&update);
                        }
                        let update_class = material_hunter_update_class(&update);
                        if !material_hunter_update_needs_worker(&update) {
                            if update_class == "account_update_untracked" {
                                if let Ok(mut stats) = reader_stats_for_task.lock() {
                                    stats.skipped_untracked_account_updates =
                                        stats.skipped_untracked_account_updates.saturating_add(1);
                                }
                            }
                            continue;
                        }
                        update
                    }
                    Some(Err(status)) => {
                        let _ = reader_tx
                            .send(MaterialHunterReaderMessage::StreamError(status))
                            .await;
                        break;
                    }
                    None => {
                        let _ = reader_tx
                            .send(MaterialHunterReaderMessage::StreamClosed)
                            .await;
                        break;
                    }
                };
                let enqueue_started = tokio::time::Instant::now();
                sequence = sequence.saturating_add(1);
                match reader_tx.try_send(MaterialHunterReaderMessage::Update(update, read_at)) {
                    Ok(()) => {
                        if let Ok(mut stats) = reader_stats_for_task.lock() {
                            stats.record_enqueue_latency(
                                enqueue_started.elapsed().as_millis() as u64
                            );
                            let current_depth = reader_tx
                                .max_capacity()
                                .saturating_sub(reader_tx.capacity());
                            stats.queue_depth_current = current_depth as u64;
                            stats.queue_depth_max = stats.queue_depth_max.max(current_depth as u64);
                            let soft_threshold =
                                (stats.queue_capacity.saturating_mul(3) / 4).max(1);
                            if current_depth as u64 >= soft_threshold
                                && stats.backpressure_threshold_crossed_at.is_none()
                            {
                                stats.backpressure_threshold_crossed_at =
                                    Some(OffsetDateTime::now_utc().to_string());
                            }
                        }
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        if let Ok(mut stats) = reader_stats_for_task.lock() {
                            stats.queue_full_count = stats.queue_full_count.saturating_add(1);
                            stats.client_backpressure_detected = true;
                            stats.worker_backpressure_detected = true;
                            stats.router_queue_full_count =
                                stats.router_queue_full_count.saturating_add(1);
                            stats.partition_backpressure_trigger_reason =
                                Some("router_queue_full".to_owned());
                            stats.backpressure_update_class =
                                Some("unknown_worker_relevant".to_owned());
                            stats.queue_depth_current = stats.queue_capacity;
                            stats.queue_depth_max = stats.queue_capacity;
                            stats.router_queue_depth_current = stats.queue_capacity;
                            stats.router_queue_depth_max = stats.queue_capacity;
                            stats.backpressure_queue_depth_at_blocker = stats.queue_capacity;
                            stats.segment_queue_dropped_dirty_updates = stats.queue_capacity;
                            stats.dirty_partition_queued_updates_discarded = stats
                                .dirty_partition_queued_updates_discarded
                                .saturating_add(stats.queue_capacity);
                            stats.segment_worker_reset_count =
                                stats.segment_worker_reset_count.saturating_add(1);
                            stats.partition_worker_reset_count =
                                stats.partition_worker_reset_count.saturating_add(1);
                            if stats.backpressure_threshold_crossed_at.is_none() {
                                stats.backpressure_threshold_crossed_at =
                                    Some(OffsetDateTime::now_utc().to_string());
                            }
                        }
                        break;
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
        });

        let mut partition_txs = Vec::with_capacity(worker_partitions);
        for partition in 0..worker_partitions {
            let (partition_tx, mut partition_rx) =
                mpsc::channel::<MaterialHunterPartitionUpdate>(partition_queue_capacity);
            partition_txs.push(partition_tx);
            let output_tx = worker_output_tx.clone();
            let stats_for_worker = reader_stats.clone();
            let loaded_for_worker = loaded.clone();
            let config_for_worker = config.clone();
            tokio::spawn(async move {
                let mut ingest = GeyserIngestService::new(config_for_worker);
                let mut normalizer = match GeyserEventNormalizer::from_loaded(&loaded_for_worker) {
                    Ok(normalizer) => normalizer,
                    Err(error) => {
                        let _ = output_tx
                            .send(MaterialHunterWorkerOutput::StreamError(Status::internal(
                                error.to_string(),
                            )))
                            .await;
                        return;
                    }
                };
                while let Some(first) = partition_rx.recv().await {
                    let mut batch = vec![first];
                    while batch.len() < partition_batch_size {
                        match partition_rx.try_recv() {
                            Ok(update) => batch.push(update),
                            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                        }
                    }
                    if let Ok(mut stats) = stats_for_worker.lock() {
                        stats.record_partition_batch(batch.len() as u64);
                        stats.record_worker_batch(batch.len() as u64);
                        if let Some(max_batch) = stats
                            .partition_batch_size_max_by_partition
                            .get_mut(partition)
                        {
                            *max_batch = (*max_batch).max(batch.len() as u64);
                        }
                    }
                    for item in batch {
                        let now = tokio::time::Instant::now();
                        let worker_lag = now.duration_since(item.read_at).as_millis() as u64;
                        if let Ok(mut stats) = stats_for_worker.lock() {
                            stats.decode_worker_lag_ms_max =
                                stats.decode_worker_lag_ms_max.max(worker_lag);
                            stats.record_queue_wait(worker_lag);
                            stats.record_partition_lag(worker_lag);
                            stats.record_partition_lag_for_partition(partition, worker_lag);
                            stats.worker_backlog_oldest_update_age_ms =
                                stats.worker_backlog_oldest_update_age_ms.max(worker_lag);
                            stats.queue_depth_current =
                                stats.partition_queue_depth_current.iter().sum();
                            if let Some(depth) =
                                stats.partition_queue_depth_current.get_mut(partition)
                            {
                                *depth = partition_rx.len() as u64;
                            }
                            if worker_lag >= worker_lag_blocker_ms {
                                stats.client_backpressure_detected = true;
                                stats.worker_backpressure_detected = true;
                                stats.backpressure_observed_lag_ms =
                                    stats.backpressure_observed_lag_ms.max(worker_lag);
                                stats.backpressure_update_class =
                                    Some(item.update_class.to_owned());
                                stats.backpressure_partition_id = Some(partition as u64);
                                stats.backpressure_segment_id.get_or_insert(1);
                                stats.backpressure_hot_key = Some(item.route_key_label.clone());
                                if let Some(mint) = item.route_key_label.strip_prefix("mint:") {
                                    stats.backpressure_hot_mint = Some(mint.to_owned());
                                }
                                if let Some(account) = item.route_key_label.strip_prefix("account:")
                                {
                                    stats.backpressure_hot_account = Some(account.to_owned());
                                }
                                stats.backpressure_deep_processed_count_at_trigger =
                                    stats.pump_trade_deep_processed_count;
                                stats.backpressure_skipped_count_at_trigger = stats
                                    .pump_trade_skipped_untracked_count
                                    .saturating_add(stats.pump_trade_skipped_tombstoned_count)
                                    .saturating_add(stats.pump_trade_unknown_mint_count);
                                if item.update_class.starts_with("transaction_") {
                                    stats.backpressure_transaction_class =
                                        Some(item.update_class.to_owned());
                                    stats.backpressure_transaction_signature =
                                        item.transaction_signature.clone();
                                    stats.backpressure_transaction_mint =
                                        item.transaction_mint.clone();
                                    stats.backpressure_transaction_account =
                                        item.transaction_account.clone();
                                    stats.backpressure_deep_transaction_count_at_trigger =
                                        stats.transaction_deep_processed_count;
                                    stats.backpressure_skipped_transaction_count_at_trigger = stats
                                        .transaction_mapping_hint_only_count
                                        .saturating_add(
                                            stats.transaction_untracked_pump_skipped_count,
                                        )
                                        .saturating_add(
                                            stats.transaction_account_pinned_unknown_count,
                                        )
                                        .saturating_add(
                                            stats.transaction_tombstoned_mint_skipped_count,
                                        )
                                        .saturating_add(
                                            stats.transaction_duplicate_signature_skipped_count,
                                        )
                                        .saturating_add(
                                            stats.transaction_malformed_or_unknown_count,
                                        )
                                        .saturating_add(
                                            stats.transaction_other_untracked_skipped_count,
                                        );
                                    stats.backpressure_account_pinned_count_at_trigger =
                                        stats.account_pinned_update_count;
                                }
                                stats.partition_backpressure_trigger_partition =
                                    Some(partition as u64);
                                stats.partition_backpressure_trigger_reason =
                                    Some("worker_lag_threshold_exceeded".to_owned());
                                stats.backpressure_queue_depth_at_blocker =
                                    stats.queue_depth_current;
                                stats.dirty_partition_queued_updates_discarded = stats
                                    .dirty_partition_queued_updates_discarded
                                    .saturating_add(partition_rx.len() as u64);
                                stats.segment_queue_dropped_dirty_updates = stats
                                    .segment_queue_dropped_dirty_updates
                                    .saturating_add(partition_rx.len() as u64);
                                stats.partition_worker_reset_count =
                                    stats.partition_worker_reset_count.saturating_add(1);
                                stats.segment_worker_reset_count =
                                    stats.segment_worker_reset_count.saturating_add(1);
                                if stats.backpressure_threshold_crossed_at.is_none() {
                                    stats.backpressure_threshold_crossed_at =
                                        Some(OffsetDateTime::now_utc().to_string());
                                }
                            }
                        }
                        let decode_started = tokio::time::Instant::now();
                        let outputs =
                            ingest.process_subscribe_update(item.update, monotonic_now_ns());
                        let normalized = outputs
                            .into_iter()
                            .flat_map(|output| normalizer.normalize_output(output))
                            .collect::<Vec<_>>();
                        let decode_ms = decode_started.elapsed().as_millis() as u64;
                        if let Ok(mut stats) = stats_for_worker.lock() {
                            stats.record_decode_duration(decode_ms);
                            stats.record_partition_decode_duration(decode_ms);
                            stats.record_update_class_worker(
                                item.update_class,
                                partition,
                                worker_lag,
                                decode_ms,
                            );
                            if matches!(
                                item.update_class,
                                "pump_trade_active_mint" | "pump_token_created"
                            ) {
                                stats.record_pump_trade_deep_duration(decode_ms);
                                if stats.pump_trade_state_update_duration_ms.len() < 100_000 {
                                    stats.pump_trade_state_update_duration_ms.push(decode_ms);
                                }
                                if stats.pump_trade_risk_feature_duration_ms.len() < 100_000 {
                                    stats.pump_trade_risk_feature_duration_ms.push(0);
                                }
                            }
                            if item.update_class.starts_with("transaction_") {
                                stats.record_transaction_deep_duration(decode_ms);
                                if stats.transaction_state_update_duration_ms.len() < 100_000 {
                                    stats.transaction_state_update_duration_ms.push(decode_ms);
                                }
                                if stats.transaction_risk_feature_duration_ms.len() < 100_000 {
                                    stats.transaction_risk_feature_duration_ms.push(0);
                                }
                                if let Some(mint) = item.transaction_mint.as_ref() {
                                    MaterialHunterReaderStats::max_top_value(
                                        &mut stats.top_active_mint_transaction_lag,
                                        mint.clone(),
                                        worker_lag,
                                    );
                                }
                            }
                            stats.worker_updates_processed =
                                stats.worker_updates_processed.saturating_add(1);
                            if let Some(count) = stats
                                .partition_updates_processed_by_partition
                                .get_mut(partition)
                            {
                                *count = count.saturating_add(1);
                            }
                        }
                        for event in normalized {
                            if output_tx
                                .send(MaterialHunterWorkerOutput::Event {
                                    event,
                                    read_at: item.read_at,
                                    partition,
                                    sequence: item.sequence,
                                })
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
            });
        }

        let router_stats = reader_stats.clone();
        let router_output_tx = worker_output_tx.clone();
        let relevance_state_for_router = relevance_state.clone();
        let config_for_router = config.clone();
        tokio::spawn(async move {
            let mut token_account_to_mint = HashMap::<Vec<u8>, String>::new();
            let mut account_partition_pins = HashMap::<Vec<u8>, usize>::new();
            let mut seen_transaction_signatures = HashSet::<String>::new();
            let mut transaction_signature_lru = VecDeque::<String>::new();
            let pressure_config =
                MaterialHunterActiveMintPressureConfig::from_geyser(&config_for_router);
            let mut active_mint_pressure = MaterialHunterActiveMintPressureState::default();
            while let Some(message) = router_rx.recv().await {
                match message {
                    MaterialHunterReaderMessage::Update(update, read_at) => {
                        let prefilter_started = tokio::time::Instant::now();
                        let (active_mints, tombstoned_mints) = relevance_state_for_router
                            .lock()
                            .map(|state| {
                                (state.active_mints.clone(), state.tombstoned_mints.clone())
                            })
                            .unwrap_or_default();
                        for (account, mint) in material_hunter_token_account_mint_mappings(&update)
                        {
                            token_account_to_mint.entry(account).or_insert(mint);
                        }
                        let signature = material_hunter_transaction_signature_hint(&update);
                        let duplicate_signature = signature
                            .as_ref()
                            .map(|signature| {
                                material_hunter_signature_seen_or_insert(
                                    &mut seen_transaction_signatures,
                                    &mut transaction_signature_lru,
                                    signature,
                                    4096,
                                )
                            })
                            .unwrap_or(false);
                        if let Ok(mut stats) = router_stats.lock() {
                            if signature.is_some() {
                                stats.transaction_signature_seen_count =
                                    stats.transaction_signature_seen_count.saturating_add(1);
                            }
                            if duplicate_signature {
                                stats.transaction_duplicate_signature_count = stats
                                    .transaction_duplicate_signature_count
                                    .saturating_add(1);
                            }
                        }
                        let pump_prefilter = material_hunter_prefilter_pump_instruction(
                            &update,
                            &active_mints,
                            &tombstoned_mints,
                        );
                        let prefilter_ms = prefilter_started.elapsed().as_millis() as u64;
                        let mut update_class = material_hunter_update_class(&update);
                        let mut transaction_hint_signature = signature.clone();
                        let mut transaction_hint_mint =
                            material_hunter_transaction_mint_hint(&update);
                        let mut transaction_hint_account =
                            material_hunter_transaction_account_hint(&update);
                        if duplicate_signature {
                            if let Some(prefilter) = material_hunter_prefilter_transaction_update(
                                &update,
                                &active_mints,
                                &tombstoned_mints,
                                &token_account_to_mint,
                                &account_partition_pins,
                                true,
                            ) {
                                if let Ok(mut stats) = router_stats.lock() {
                                    stats.transaction_prefilter_count =
                                        stats.transaction_prefilter_count.saturating_add(1);
                                    stats.transaction_duplicate_signature_skipped_count = stats
                                        .transaction_duplicate_signature_skipped_count
                                        .saturating_add(1);
                                    stats.record_transaction_prefilter_duration(prefilter_ms);
                                    stats.record_update_class_skipped(prefilter.update_class);
                                }
                                continue;
                            }
                        }
                        if let Some(prefilter) = pump_prefilter.as_ref() {
                            update_class = prefilter.update_class;
                            if let Ok(mut stats) = router_stats.lock() {
                                stats.pump_trade_fast_prefilter_count =
                                    stats.pump_trade_fast_prefilter_count.saturating_add(1);
                                stats.record_pump_trade_prefilter_duration(prefilter_ms);
                            }
                            if prefilter.update_class == "pump_token_created" {
                                if let Some(mint) = prefilter.mint.as_ref() {
                                    apply_material_hunter_state_hint(
                                        &relevance_state_for_router,
                                        &MaterialHunterStreamStateHint {
                                            active_mints: vec![mint.clone()],
                                            ..MaterialHunterStreamStateHint::default()
                                        },
                                    );
                                }
                            }
                            match prefilter.decision {
                                MaterialHunterPumpPrefilterDecision::DeepProcess => {}
                                MaterialHunterPumpPrefilterDecision::SkipUntracked
                                | MaterialHunterPumpPrefilterDecision::SkipTombstoned
                                | MaterialHunterPumpPrefilterDecision::SkipUnknownMint
                                | MaterialHunterPumpPrefilterDecision::SkipMalformed
                                | MaterialHunterPumpPrefilterDecision::SkipOther => {
                                    if let Ok(mut stats) = router_stats.lock() {
                                        match prefilter.decision {
                                            MaterialHunterPumpPrefilterDecision::SkipUntracked => {
                                                stats.pump_trade_skipped_untracked_count = stats
                                                    .pump_trade_skipped_untracked_count
                                                    .saturating_add(1);
                                            }
                                            MaterialHunterPumpPrefilterDecision::SkipTombstoned => {
                                                stats.pump_trade_skipped_tombstoned_count = stats
                                                    .pump_trade_skipped_tombstoned_count
                                                    .saturating_add(1);
                                            }
                                            MaterialHunterPumpPrefilterDecision::SkipUnknownMint => {
                                                stats.pump_trade_unknown_mint_count = stats
                                                    .pump_trade_unknown_mint_count
                                                    .saturating_add(1);
                                                stats.unknown_mint_route_count = stats
                                                    .unknown_mint_route_count
                                                    .saturating_add(1);
                                                MaterialHunterReaderStats::increment_top_count(
                                                    &mut stats.unknown_mint_route_count_by_class,
                                                    prefilter.update_class.to_owned(),
                                                );
                                            }
                                            MaterialHunterPumpPrefilterDecision::SkipMalformed
                                            | MaterialHunterPumpPrefilterDecision::SkipOther
                                            | MaterialHunterPumpPrefilterDecision::DeepProcess => {}
                                        }
                                        stats.record_update_class_skipped(prefilter.update_class);
                                        if let Some(mint) = prefilter.mint.as_ref() {
                                            MaterialHunterReaderStats::increment_top_count(
                                                &mut stats.top_mint_counts,
                                                mint.clone(),
                                            );
                                        }
                                        if let Some(account) = prefilter.account.as_ref() {
                                            MaterialHunterReaderStats::increment_top_count(
                                                &mut stats.top_account_counts,
                                                account.clone(),
                                            );
                                        }
                                    }
                                    continue;
                                }
                            }
                        }
                        if pump_prefilter.is_none() {
                            if let Some(prefilter) = material_hunter_prefilter_transaction_update(
                                &update,
                                &active_mints,
                                &tombstoned_mints,
                                &token_account_to_mint,
                                &account_partition_pins,
                                false,
                            ) {
                                update_class = prefilter.update_class;
                                transaction_hint_signature = prefilter.signature.clone();
                                transaction_hint_mint = prefilter.mint.clone();
                                transaction_hint_account = prefilter.account.clone();
                                if let Ok(mut stats) = router_stats.lock() {
                                    stats.transaction_prefilter_count =
                                        stats.transaction_prefilter_count.saturating_add(1);
                                    stats.record_transaction_prefilter_duration(prefilter_ms);
                                    match prefilter.decision {
                                        MaterialHunterTransactionPrefilterDecision::DeepProcess => {
                                            if prefilter.update_class == "transaction_active_mint"
                                                || prefilter.update_class
                                                    == "transaction_active_account"
                                            {
                                                stats.active_mint_transaction_update_count = stats
                                                    .active_mint_transaction_update_count
                                                    .saturating_add(1);
                                                stats.active_mint_transaction_dirty_feature_count =
                                                    stats
                                                        .active_mint_transaction_dirty_feature_count
                                                        .saturating_add(1);
                                                if let Some(mint) = prefilter.mint.as_ref() {
                                                    MaterialHunterReaderStats::increment_top_count(
                                                        &mut stats
                                                            .top_active_mint_transaction_counts,
                                                        mint.clone(),
                                                    );
                                                }
                                            } else {
                                                stats.transaction_deep_processed_count = stats
                                                    .transaction_deep_processed_count
                                                    .saturating_add(1);
                                                stats.transaction_feature_deferred_count = stats
                                                    .transaction_feature_deferred_count
                                                    .saturating_add(1);
                                            }
                                            if prefilter.update_class == "transaction_active_account"
                                            {
                                                stats.account_pinned_active_count = stats
                                                    .account_pinned_active_count
                                                    .saturating_add(1);
                                                stats.account_pinned_deep_processed_count = stats
                                                    .account_pinned_deep_processed_count
                                                    .saturating_add(1);
                                            }
                                        }
                                        MaterialHunterTransactionPrefilterDecision::SkipMappingHintOnly
                                        | MaterialHunterTransactionPrefilterDecision::SkipUntrackedPump
                                        | MaterialHunterTransactionPrefilterDecision::SkipAccountPinnedUnknown
                                        | MaterialHunterTransactionPrefilterDecision::SkipTombstoned
                                        | MaterialHunterTransactionPrefilterDecision::SkipDuplicateSignature
                                        | MaterialHunterTransactionPrefilterDecision::SkipMalformedOrUnknown
                                        | MaterialHunterTransactionPrefilterDecision::SkipOtherUntracked => {
                                            match prefilter.decision {
                                                MaterialHunterTransactionPrefilterDecision::SkipMappingHintOnly => {
                                                    stats.transaction_mapping_hint_only_count = stats
                                                        .transaction_mapping_hint_only_count
                                                        .saturating_add(1);
                                                }
                                                MaterialHunterTransactionPrefilterDecision::SkipUntrackedPump => {
                                                    stats.transaction_untracked_pump_skipped_count = stats
                                                        .transaction_untracked_pump_skipped_count
                                                        .saturating_add(1);
                                                }
                                                MaterialHunterTransactionPrefilterDecision::SkipAccountPinnedUnknown => {
                                                    stats.transaction_account_pinned_unknown_count = stats
                                                        .transaction_account_pinned_unknown_count
                                                        .saturating_add(1);
                                                    stats.account_pinned_unknown_count = stats
                                                        .account_pinned_unknown_count
                                                        .saturating_add(1);
                                                    stats.account_pinned_skipped_count = stats
                                                        .account_pinned_skipped_count
                                                        .saturating_add(1);
                                                }
                                                MaterialHunterTransactionPrefilterDecision::SkipTombstoned => {
                                                    stats.transaction_tombstoned_mint_skipped_count = stats
                                                        .transaction_tombstoned_mint_skipped_count
                                                        .saturating_add(1);
                                                }
                                                MaterialHunterTransactionPrefilterDecision::SkipDuplicateSignature => {
                                                    stats.transaction_duplicate_signature_skipped_count = stats
                                                        .transaction_duplicate_signature_skipped_count
                                                        .saturating_add(1);
                                                }
                                                MaterialHunterTransactionPrefilterDecision::SkipMalformedOrUnknown => {
                                                    stats.transaction_malformed_or_unknown_count = stats
                                                        .transaction_malformed_or_unknown_count
                                                        .saturating_add(1);
                                                }
                                                MaterialHunterTransactionPrefilterDecision::SkipOtherUntracked => {
                                                    stats.transaction_other_untracked_skipped_count = stats
                                                        .transaction_other_untracked_skipped_count
                                                        .saturating_add(1);
                                                }
                                                MaterialHunterTransactionPrefilterDecision::DeepProcess => {}
                                            }
                                            if prefilter.update_class.starts_with("transaction_active_") {
                                                stats.active_mint_transaction_skipped_count = stats
                                                    .active_mint_transaction_skipped_count
                                                    .saturating_add(1);
                                            }
                                            stats.record_update_class_skipped(prefilter.update_class);
                                            if let Some(mint) = prefilter.mint.as_ref() {
                                                MaterialHunterReaderStats::increment_top_count(
                                                    &mut stats.top_mint_counts,
                                                    mint.clone(),
                                                );
                                            }
                                            if let Some(account) = prefilter.account.as_ref() {
                                                MaterialHunterReaderStats::increment_top_count(
                                                    &mut stats.top_account_counts,
                                                    account.clone(),
                                                );
                                            }
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                        let (partition, fallback, route_key_label) =
                            material_hunter_partition_for_update_with_account_map(
                                &update,
                                worker_partitions,
                                &token_account_to_mint,
                                &mut account_partition_pins,
                            );
                        if let Ok(mut stats) = router_stats.lock() {
                            stats.router_updates_received =
                                stats.router_updates_received.saturating_add(1);
                            if fallback {
                                stats.router_fallback_count =
                                    stats.router_fallback_count.saturating_add(1);
                                stats.unknown_mint_route_count =
                                    stats.unknown_mint_route_count.saturating_add(1);
                                MaterialHunterReaderStats::increment_top_count(
                                    &mut stats.unknown_mint_route_count_by_class,
                                    update_class.to_owned(),
                                );
                            }
                            if route_key_label.starts_with("account_pinned:") {
                                stats.account_pinned_update_count =
                                    stats.account_pinned_update_count.saturating_add(1);
                            }
                            stats.record_update_class_route(update_class, partition);
                            MaterialHunterReaderStats::increment_top_count(
                                &mut stats.top_partition_key_counts,
                                route_key_label.clone(),
                            );
                            if let Some(mint) = route_key_label.strip_prefix("mint:") {
                                MaterialHunterReaderStats::increment_top_count(
                                    &mut stats.top_mint_counts,
                                    mint.to_owned(),
                                );
                            }
                            if let Some(account) = route_key_label.strip_prefix("account:") {
                                MaterialHunterReaderStats::increment_top_count(
                                    &mut stats.top_account_counts,
                                    account.to_owned(),
                                );
                            }
                            stats.router_queue_depth_current = router_rx.len() as u64;
                            stats.router_queue_depth_max =
                                stats.router_queue_depth_max.max(router_rx.len() as u64);
                        }
                        let Some(partition_tx) = partition_txs.get(partition) else {
                            if let Ok(mut stats) = router_stats.lock() {
                                stats.router_error_count =
                                    stats.router_error_count.saturating_add(1);
                                stats.client_backpressure_detected = true;
                                stats.worker_backpressure_detected = true;
                            }
                            break;
                        };
                        let active_mint_for_pressure = if update_class == "pump_trade_active_mint" {
                            pump_prefilter
                                .as_ref()
                                .and_then(|prefilter| prefilter.mint.clone())
                        } else if update_class == "transaction_active_mint"
                            || update_class == "transaction_active_account"
                        {
                            transaction_hint_mint.clone()
                        } else {
                            None
                        };
                        if let Some(mint) = active_mint_for_pressure.as_ref() {
                            let depth = partition_tx
                                .max_capacity()
                                .saturating_sub(partition_tx.capacity())
                                as u64;
                            match material_hunter_active_mint_pressure_decision(
                                &mut active_mint_pressure,
                                mint,
                                tokio::time::Instant::now(),
                                depth,
                                partition_tx.max_capacity() as u64,
                                &pressure_config,
                            ) {
                                MaterialHunterActiveMintPressureDecision::DeepProcess => {
                                    if let Ok(mut stats) = router_stats.lock() {
                                        stats.active_mint_transaction_deep_processed_count = stats
                                            .active_mint_transaction_deep_processed_count
                                            .saturating_add(1);
                                        stats.active_mint_transaction_delta_flush_count = stats
                                            .active_mint_transaction_delta_flush_count
                                            .saturating_add(1);
                                        if stats.active_mint_delta_flush_duration_ms.len() < 100_000
                                        {
                                            stats.active_mint_delta_flush_duration_ms.push(0);
                                        }
                                        MaterialHunterReaderStats::increment_top_count(
                                            &mut stats.top_active_mint_deep_processed_counts,
                                            mint.clone(),
                                        );
                                        if update_class == "pump_trade_active_mint" {
                                            stats.active_mint_transaction_update_count = stats
                                                .active_mint_transaction_update_count
                                                .saturating_add(1);
                                            stats.active_mint_transaction_dirty_feature_count =
                                                stats
                                                    .active_mint_transaction_dirty_feature_count
                                                    .saturating_add(1);
                                            MaterialHunterReaderStats::increment_top_count(
                                                &mut stats.top_active_mint_transaction_counts,
                                                mint.clone(),
                                            );
                                            stats.pump_trade_deep_processed_count = stats
                                                .pump_trade_deep_processed_count
                                                .saturating_add(1);
                                            stats.pump_trade_deferred_feature_count = stats
                                                .pump_trade_deferred_feature_count
                                                .saturating_add(1);
                                        }
                                        if update_class.starts_with("transaction_active_") {
                                            stats.transaction_deep_processed_count = stats
                                                .transaction_deep_processed_count
                                                .saturating_add(1);
                                            stats.transaction_feature_deferred_count = stats
                                                .transaction_feature_deferred_count
                                                .saturating_add(1);
                                        }
                                    }
                                }
                                MaterialHunterActiveMintPressureDecision::Coalesce => {
                                    if let Ok(mut stats) = router_stats.lock() {
                                        stats.active_mint_transaction_coalesced_count = stats
                                            .active_mint_transaction_coalesced_count
                                            .saturating_add(1);
                                        stats.active_mint_transaction_skipped_count = stats
                                            .active_mint_transaction_skipped_count
                                            .saturating_add(1);
                                        stats.active_mint_transaction_dirty_feature_count = stats
                                            .active_mint_transaction_dirty_feature_count
                                            .saturating_add(1);
                                        MaterialHunterReaderStats::increment_top_count(
                                            &mut stats.top_active_mint_coalesced_counts,
                                            mint.clone(),
                                        );
                                        stats.record_update_class_skipped(update_class);
                                    }
                                    continue;
                                }
                                MaterialHunterActiveMintPressureDecision::Degrade {
                                    reason,
                                    queue_pressure,
                                } => {
                                    apply_material_hunter_state_hint(
                                        &relevance_state_for_router,
                                        &MaterialHunterStreamStateHint {
                                            inactive_mints: vec![mint.clone()],
                                            tombstoned_mints: vec![mint.clone()],
                                            ..MaterialHunterStreamStateHint::default()
                                        },
                                    );
                                    if let Ok(mut stats) = router_stats.lock() {
                                        stats.active_mint_transaction_degraded_count = stats
                                            .active_mint_transaction_degraded_count
                                            .saturating_add(1);
                                        stats.active_mint_transaction_skipped_count = stats
                                            .active_mint_transaction_skipped_count
                                            .saturating_add(1);
                                        if reason == "active_mint_processing_budget_exceeded" {
                                            stats.active_mint_transaction_budget_exceeded_count =
                                                stats
                                                    .active_mint_transaction_budget_exceeded_count
                                                    .saturating_add(1);
                                        }
                                        if queue_pressure {
                                            stats.active_mint_transaction_queue_pressure_count =
                                                stats
                                                    .active_mint_transaction_queue_pressure_count
                                                    .saturating_add(1);
                                            stats.partition_queue_pressure_preempted_count = stats
                                                .partition_queue_pressure_preempted_count
                                                .saturating_add(1);
                                            stats.partition_queue_pressure_dominant_mint =
                                                Some(mint.clone());
                                            stats.partition_queue_pressure_dominant_mint_update_count =
                                                stats
                                                    .top_active_mint_transaction_counts
                                                    .get(mint)
                                                    .copied()
                                                    .unwrap_or(0)
                                                    .max(1);
                                            stats.partition_queue_pressure_degraded_mint =
                                                Some(mint.clone());
                                            stats.partition_queue_pressure_preempted_before_full =
                                                true;
                                            MaterialHunterReaderStats::increment_top_count(
                                                &mut stats.top_active_mint_queue_pressure_counts,
                                                mint.clone(),
                                            );
                                        }
                                        stats.degraded_active_mints.insert(mint.clone());
                                        MaterialHunterReaderStats::increment_top_count(
                                            &mut stats.top_active_mint_coalesced_counts,
                                            mint.clone(),
                                        );
                                        stats.record_update_class_skipped(update_class);
                                    }
                                    continue;
                                }
                            }
                        }
                        let sequence = if let Ok(stats) = router_stats.lock() {
                            stats.router_updates_received
                        } else {
                            0
                        };
                        match partition_tx.try_send(MaterialHunterPartitionUpdate {
                            update,
                            read_at,
                            sequence,
                            update_class,
                            route_key_label: route_key_label.clone(),
                            transaction_signature: transaction_hint_signature.clone(),
                            transaction_mint: transaction_hint_mint.clone(),
                            transaction_account: transaction_hint_account.clone(),
                        }) {
                            Ok(()) => {
                                if let Ok(mut stats) = router_stats.lock() {
                                    stats.router_updates_routed =
                                        stats.router_updates_routed.saturating_add(1);
                                    let depth = partition_tx
                                        .max_capacity()
                                        .saturating_sub(partition_tx.capacity())
                                        as u64;
                                    if let Some(current) =
                                        stats.partition_queue_depth_current.get_mut(partition)
                                    {
                                        *current = depth;
                                    }
                                    if let Some(max_depth) =
                                        stats.partition_queue_depth_max.get_mut(partition)
                                    {
                                        *max_depth = (*max_depth).max(depth);
                                    }
                                    stats.queue_depth_current =
                                        stats.partition_queue_depth_current.iter().sum();
                                    stats.queue_depth_max =
                                        stats.queue_depth_max.max(stats.queue_depth_current);
                                    let soft_threshold =
                                        ((partition_tx.max_capacity() as f64) * 0.75).ceil() as u64;
                                    if depth >= soft_threshold.max(1)
                                        && stats.backpressure_threshold_crossed_at.is_none()
                                    {
                                        stats.backpressure_threshold_crossed_at =
                                            Some(OffsetDateTime::now_utc().to_string());
                                    }
                                }
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                if let Ok(mut stats) = router_stats.lock() {
                                    if let Some(count) = stats
                                        .partition_queue_full_count_by_partition
                                        .get_mut(partition)
                                    {
                                        *count = count.saturating_add(1);
                                    }
                                    if stats.partition_queue_pressure_preempted_before_full {
                                        stats.partition_queue_full_after_preemption = true;
                                    }
                                    stats.client_backpressure_detected = true;
                                    stats.worker_backpressure_detected = true;
                                    stats.backpressure_partition_id = Some(partition as u64);
                                    stats.partition_backpressure_trigger_partition =
                                        Some(partition as u64);
                                    stats.partition_backpressure_trigger_reason =
                                        Some("partition_queue_full".to_owned());
                                    stats.backpressure_update_class = Some(update_class.to_owned());
                                    stats.backpressure_hot_key = Some(route_key_label.clone());
                                    if let Some(mint) = route_key_label.strip_prefix("mint:") {
                                        stats.backpressure_hot_mint = Some(mint.to_owned());
                                    }
                                    if let Some(account) = route_key_label.strip_prefix("account:")
                                    {
                                        stats.backpressure_hot_account = Some(account.to_owned());
                                    }
                                    stats.backpressure_deep_processed_count_at_trigger =
                                        stats.pump_trade_deep_processed_count;
                                    stats.backpressure_skipped_count_at_trigger = stats
                                        .pump_trade_skipped_untracked_count
                                        .saturating_add(stats.pump_trade_skipped_tombstoned_count)
                                        .saturating_add(stats.pump_trade_unknown_mint_count);
                                    if update_class.starts_with("transaction_") {
                                        stats.backpressure_transaction_class =
                                            Some(update_class.to_owned());
                                        stats.backpressure_transaction_signature =
                                            transaction_hint_signature.clone();
                                        stats.backpressure_transaction_mint =
                                            transaction_hint_mint.clone();
                                        stats.backpressure_transaction_account =
                                            transaction_hint_account.clone();
                                        stats.backpressure_deep_transaction_count_at_trigger =
                                            stats.transaction_deep_processed_count;
                                        stats.backpressure_skipped_transaction_count_at_trigger =
                                            stats.transaction_mapping_hint_only_count
                                                .saturating_add(
                                                    stats.transaction_untracked_pump_skipped_count,
                                                )
                                                .saturating_add(
                                                    stats
                                                        .transaction_account_pinned_unknown_count,
                                                )
                                                .saturating_add(
                                                    stats
                                                        .transaction_tombstoned_mint_skipped_count,
                                                )
                                                .saturating_add(
                                                    stats
                                                        .transaction_duplicate_signature_skipped_count,
                                                )
                                                .saturating_add(
                                                    stats.transaction_malformed_or_unknown_count,
                                                )
                                                .saturating_add(
                                                    stats.transaction_other_untracked_skipped_count,
                                                );
                                        stats.backpressure_account_pinned_count_at_trigger =
                                            stats.account_pinned_update_count;
                                    }
                                    stats.backpressure_queue_depth_at_blocker =
                                        partition_tx.max_capacity() as u64;
                                    stats.dirty_partition_queued_updates_discarded = stats
                                        .dirty_partition_queued_updates_discarded
                                        .saturating_add(partition_tx.max_capacity() as u64);
                                    stats.segment_queue_dropped_dirty_updates = stats
                                        .segment_queue_dropped_dirty_updates
                                        .saturating_add(partition_tx.max_capacity() as u64);
                                    stats.partition_worker_reset_count =
                                        stats.partition_worker_reset_count.saturating_add(1);
                                    stats.segment_worker_reset_count =
                                        stats.segment_worker_reset_count.saturating_add(1);
                                    if stats.backpressure_threshold_crossed_at.is_none() {
                                        stats.backpressure_threshold_crossed_at =
                                            Some(OffsetDateTime::now_utc().to_string());
                                    }
                                }
                                break;
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                        }
                    }
                    MaterialHunterReaderMessage::StreamError(status) => {
                        let _ = router_output_tx
                            .send(MaterialHunterWorkerOutput::StreamError(status))
                            .await;
                        break;
                    }
                    MaterialHunterReaderMessage::StreamClosed => {
                        let _ = router_output_tx
                            .send(MaterialHunterWorkerOutput::StreamClosed)
                            .await;
                        break;
                    }
                }
            }
        });
        drop(worker_output_tx);

        let mut last_progress_tick = tokio::time::Instant::now();
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let poll_window = remaining.min(StdDuration::from_millis(250));
            if poll_window.is_zero() {
                break 'streaming;
            }
            if let Ok(stats) = reader_stats.lock() {
                apply_reader_stats_to_summary(&mut summary, &stats);
                if stats.client_backpressure_detected {
                    summary.provider_status = "client_backpressure_detected".to_owned();
                    summary.provider_blocker_class =
                        Some("client_backpressure_detected".to_owned());
                    summary.stream_completed_normally = false;
                    let _ = on_progress(&summary)?;
                    if options.gap_tolerant_segments && tokio::time::Instant::now() < deadline {
                        reconnect_attempts = reconnect_attempts.saturating_add(1);
                        summary.reconnect_attempts = summary.reconnect_attempts.saturating_add(1);
                        if reconnect_attempts < max_attempts {
                            summary.provider_status = "reconnecting_after_gap".to_owned();
                            summary.provider_blocker_class = None;
                            tokio::time::sleep(next_backoff_ms(&config, reconnect_attempts)).await;
                            continue 'streaming;
                        }
                        summary.provider_status = "provider_reconnect_exhausted".to_owned();
                        summary.provider_blocker_class =
                            Some("provider_reconnect_exhausted".to_owned());
                        let _ = on_progress(&summary)?;
                    }
                    return Ok(summary);
                }
            }
            match tokio::time::timeout(poll_window, worker_output_rx.recv()).await {
                Ok(Some(MaterialHunterWorkerOutput::Event {
                    event,
                    read_at,
                    partition,
                    sequence,
                })) => {
                    let _partition_sequence_marker = (partition, sequence);
                    reconnect_attempts = 0;
                    last_provider_progress_at = tokio::time::Instant::now();
                    summary.provider_progress_stalled_seconds = 0;
                    if let Ok(stats) = reader_stats.lock() {
                        apply_reader_stats_to_summary(&mut summary, &stats);
                    }
                    if on_progress(&summary)?.is_stop() {
                        summary.provider_status = "stopped_by_hunter".to_owned();
                        summary.stream_completed_normally = true;
                        return Ok(summary);
                    }
                    last_progress_tick = tokio::time::Instant::now();
                    if matches!(&event.payload, EventPayload::TokenCreated(payload) if payload.status != TransactionStatus::Failed)
                    {
                        summary.pump_create_decoded = summary.pump_create_decoded.saturating_add(1);
                        last_pump_progress_at = tokio::time::Instant::now();
                        summary.pump_progress_stalled_seconds = 0;
                    }
                    summary.normalized_events = summary.normalized_events.saturating_add(1);
                    let action = on_event(event, &summary)?;
                    if let Some(hint) = action.state_hint() {
                        apply_material_hunter_state_hint(&relevance_state, hint);
                    }
                    if action.is_stop() {
                        summary.provider_status = "stopped_by_hunter".to_owned();
                        summary.stream_completed_normally = true;
                        return Ok(summary);
                    }
                    if let Ok(stats) = reader_stats.lock() {
                        if stats.client_backpressure_detected {
                            summary.provider_status = "client_backpressure_detected".to_owned();
                            summary.provider_blocker_class =
                                Some("client_backpressure_detected".to_owned());
                            summary.stream_completed_normally = false;
                            apply_reader_stats_to_summary(&mut summary, &stats);
                            let _ = on_progress(&summary)?;
                            return Ok(summary);
                        }
                    }
                    let _ = read_at;
                }
                Ok(Some(MaterialHunterWorkerOutput::StreamError(status))) => {
                    let (class, retryable, data_loss) = material_hunter_status_class(&status);
                    summary.provider_status = class.to_owned();
                    summary.provider_blocker_class = Some(class.to_owned());
                    summary.errors.push(status.to_string());
                    if data_loss {
                        summary.provider_data_loss_seen = true;
                        summary.provider_lagged_count =
                            summary.provider_lagged_count.saturating_add(1);
                        let _ = on_progress(&summary)?;
                        if options.gap_tolerant_segments && tokio::time::Instant::now() < deadline {
                            reconnect_attempts = reconnect_attempts.saturating_add(1);
                            summary.reconnect_attempts =
                                summary.reconnect_attempts.saturating_add(1);
                            if reconnect_attempts < max_attempts {
                                summary.provider_status = "reconnecting_after_gap".to_owned();
                                summary.provider_blocker_class = None;
                                tokio::time::sleep(next_backoff_ms(&config, reconnect_attempts))
                                    .await;
                                continue 'streaming;
                            }
                            summary.provider_status = "provider_reconnect_exhausted".to_owned();
                            summary.provider_blocker_class =
                                Some("provider_reconnect_exhausted".to_owned());
                            let _ = on_progress(&summary)?;
                        }
                        return Ok(summary);
                    }
                    if !retryable {
                        let _ = on_progress(&summary)?;
                        return Ok(summary);
                    }
                    reconnect_attempts = reconnect_attempts.saturating_add(1);
                    summary.reconnect_attempts = summary.reconnect_attempts.saturating_add(1);
                    if reconnect_attempts < max_attempts {
                        summary.provider_blocker_class = None;
                        let _ = on_progress(&summary)?;
                        tokio::time::sleep(next_backoff_ms(&config, reconnect_attempts)).await;
                        continue 'streaming;
                    }
                    summary.provider_status = "provider_reconnect_exhausted".to_owned();
                    summary.provider_blocker_class =
                        Some("provider_reconnect_exhausted".to_owned());
                    let _ = on_progress(&summary)?;
                    return Ok(summary);
                }
                Ok(Some(MaterialHunterWorkerOutput::StreamClosed)) | Ok(None) => {
                    if let Ok(stats) = reader_stats.lock() {
                        apply_reader_stats_to_summary(&mut summary, &stats);
                    }
                    if on_progress(&summary)?.is_stop() {
                        summary.provider_status = "stopped_by_hunter".to_owned();
                        summary.stream_completed_normally = true;
                        return Ok(summary);
                    }
                    if tokio::time::Instant::now() < deadline {
                        summary.provider_status =
                            "provider_stream_closed_before_deadline".to_owned();
                        summary.provider_blocker_class =
                            Some("provider_stream_closed_before_deadline".to_owned());
                        summary.stream_completed_normally = false;
                        summary.provider_progress_stalled_seconds =
                            last_provider_progress_at.elapsed().as_secs();
                        summary.pump_progress_stalled_seconds =
                            last_pump_progress_at.elapsed().as_secs();
                        let _ = on_progress(&summary)?;
                        if options.gap_tolerant_segments && tokio::time::Instant::now() < deadline {
                            reconnect_attempts = reconnect_attempts.saturating_add(1);
                            summary.reconnect_attempts =
                                summary.reconnect_attempts.saturating_add(1);
                            if reconnect_attempts < max_attempts {
                                summary.provider_status = "reconnecting_after_gap".to_owned();
                                summary.provider_blocker_class = None;
                                tokio::time::sleep(next_backoff_ms(&config, reconnect_attempts))
                                    .await;
                                continue 'streaming;
                            }
                            summary.provider_status = "provider_reconnect_exhausted".to_owned();
                            summary.provider_blocker_class =
                                Some("provider_reconnect_exhausted".to_owned());
                            let _ = on_progress(&summary)?;
                        }
                        return Ok(summary);
                    }
                    break 'streaming;
                }
                Err(_) => {
                    if let Ok(stats) = reader_stats.lock() {
                        apply_reader_stats_to_summary(&mut summary, &stats);
                        if stats.client_backpressure_detected {
                            summary.provider_status = "client_backpressure_detected".to_owned();
                            summary.provider_blocker_class =
                                Some("client_backpressure_detected".to_owned());
                            summary.stream_completed_normally = false;
                            let _ = on_progress(&summary)?;
                            if options.gap_tolerant_segments
                                && tokio::time::Instant::now() < deadline
                            {
                                reconnect_attempts = reconnect_attempts.saturating_add(1);
                                summary.reconnect_attempts =
                                    summary.reconnect_attempts.saturating_add(1);
                                if reconnect_attempts < max_attempts {
                                    summary.provider_status = "reconnecting_after_gap".to_owned();
                                    summary.provider_blocker_class = None;
                                    tokio::time::sleep(next_backoff_ms(
                                        &config,
                                        reconnect_attempts,
                                    ))
                                    .await;
                                    continue 'streaming;
                                }
                                summary.provider_status = "provider_reconnect_exhausted".to_owned();
                                summary.provider_blocker_class =
                                    Some("provider_reconnect_exhausted".to_owned());
                                let _ = on_progress(&summary)?;
                            }
                            return Ok(summary);
                        }
                    }
                    if last_progress_tick.elapsed() >= StdDuration::from_secs(30) {
                        summary.provider_progress_stalled_seconds =
                            last_provider_progress_at.elapsed().as_secs();
                        summary.pump_progress_stalled_seconds =
                            last_pump_progress_at.elapsed().as_secs();
                        if on_progress(&summary)?.is_stop() {
                            summary.provider_status = "stopped_by_hunter".to_owned();
                            summary.stream_completed_normally = true;
                            return Ok(summary);
                        }
                        last_progress_tick = tokio::time::Instant::now();
                    }
                    continue;
                }
            }
        }
    }
    if summary.transaction_updates + summary.account_updates + summary.slot_updates == 0 {
        summary.provider_status = "connected_zero_updates".to_owned();
    } else if summary.pump_create_decoded == 0 {
        summary.provider_status = "no_launch_detected".to_owned();
    } else {
        summary.provider_status = "completed".to_owned();
    }
    summary.stream_completed_normally = summary.provider_blocker_class.is_none()
        && !summary.provider_data_loss_seen
        && matches!(
            summary.provider_status.as_str(),
            "connected_zero_updates" | "no_launch_detected" | "completed" | "stopped_by_hunter"
        );
    summary.provider_progress_stalled_seconds = last_provider_progress_at.elapsed().as_secs();
    summary.pump_progress_stalled_seconds = last_pump_progress_at.elapsed().as_secs();
    let _ = on_progress(&summary)?;
    Ok(summary)
}

pub async fn collect_fresh_launch_canary_events(
    loaded: &LoadedConfig,
    options: FreshLaunchCanaryLiveOptions,
) -> Result<(FreshLaunchCanaryLiveSummary, Vec<NormalizedEvent>)> {
    collect_fresh_launch_canary_events_with_connector(
        loaded,
        options,
        Arc::new(RealGeyserConnector),
    )
    .await
}

pub async fn collect_fresh_launch_canary_events_with_connector(
    loaded: &LoadedConfig,
    options: FreshLaunchCanaryLiveOptions,
    connector: Arc<dyn GeyserStreamConnector>,
) -> Result<(FreshLaunchCanaryLiveSummary, Vec<NormalizedEvent>)> {
    let config = loaded.config.geyser.clone();
    let mut summary = FreshLaunchCanaryLiveSummary {
        provider_status: "not_attempted".to_owned(),
        duration_seconds: options.duration_seconds.max(1),
        ..FreshLaunchCanaryLiveSummary::default()
    };
    if !geyser_endpoint_configured(&config) {
        summary.provider_status = "not_attempted_missing_endpoint".to_owned();
        summary
            .errors
            .push("geyser endpoint is not configured".to_owned());
        return Err(anyhow!("geyser endpoint is not configured"));
    }

    let mut normalizer = GeyserEventNormalizer::from_loaded(loaded)?;
    let mut ingest = GeyserIngestService::new(config.clone());
    let request = ingest.proto_subscription_request();
    let mut stream = match connector.connect_and_subscribe(&config, request).await {
        Ok(stream) => {
            summary.provider_status = "connected".to_owned();
            summary.connected = true;
            stream
        }
        Err(error) => {
            if let Some(status) = error.downcast_ref::<Status>() {
                summary.provider_status = match status.code() {
                    tonic::Code::Unimplemented => "unsupported".to_owned(),
                    tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
                        "auth_rejected".to_owned()
                    }
                    _ => "connection_failed".to_owned(),
                };
                summary.errors.push(status.to_string());
            } else {
                summary.provider_status = "connection_failed".to_owned();
                summary.errors.push(error.to_string());
            }
            return Err(anyhow!(summary.errors.join("; ")));
        }
    };

    let deadline =
        tokio::time::Instant::now() + StdDuration::from_secs(options.duration_seconds.max(1));
    let mut events = Vec::<NormalizedEvent>::new();
    let mut launches_seen = 0usize;
    let mut tracked_mints = HashSet::<String>::new();
    let mut tracked_launch_slots = HashSet::<u64>::new();
    let mut tracked_related_signatures = HashSet::<String>::new();
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let poll_window = remaining.min(StdDuration::from_millis(250));
        if poll_window.is_zero() {
            break;
        }
        match tokio::time::timeout(poll_window, stream.next()).await {
            Ok(Some(Ok(update))) => {
                match update.update_oneof.as_ref() {
                    Some(UpdateOneof::Transaction(_)) | Some(UpdateOneof::TransactionStatus(_)) => {
                        summary.transaction_updates = summary.transaction_updates.saturating_add(1);
                    }
                    Some(UpdateOneof::Account(_)) => {
                        summary.account_updates = summary.account_updates.saturating_add(1);
                    }
                    Some(UpdateOneof::Slot(_)) => {
                        summary.slot_updates = summary.slot_updates.saturating_add(1);
                    }
                    _ => {}
                }
                let outputs = ingest.process_subscribe_update(update, monotonic_now_ns());
                let normalized = outputs
                    .into_iter()
                    .flat_map(|output| normalizer.normalize_output(output))
                    .collect::<Vec<_>>();
                for event in normalized {
                    if let EventPayload::TokenCreated(payload) = &event.payload {
                        launches_seen = launches_seen.saturating_add(1);
                        summary.pump_create_decoded = summary.pump_create_decoded.saturating_add(1);
                        if payload.status != TransactionStatus::Failed {
                            if tracked_mints.len() < options.max_launches.max(1) {
                                tracked_mints.insert(payload.mint.0.clone());
                                tracked_launch_slots.insert(event.meta.slot);
                                if let Some(signature) = event.signature() {
                                    tracked_related_signatures.insert(signature.to_owned());
                                }
                            }
                            if summary.tracked_mint.is_none() {
                                summary.tracked_mint = Some(payload.mint.to_string());
                            }
                        }
                    }
                    summary.normalized_events = summary.normalized_events.saturating_add(1);
                    if event
                        .mint()
                        .map(|mint| tracked_mints.contains(&mint.0))
                        .unwrap_or(false)
                    {
                        if let Some(signature) = event.signature() {
                            tracked_related_signatures.insert(signature.to_owned());
                        }
                    }
                    if should_retain_fresh_launch_event(
                        &event,
                        options.retain_only_tracked_mints,
                        &tracked_mints,
                        &tracked_launch_slots,
                        &tracked_related_signatures,
                    ) {
                        summary.retained_events = summary.retained_events.saturating_add(1);
                        events.push(event);
                    }
                }
                if summary.tracked_mint.is_some()
                    && launches_seen >= options.max_launches.max(1)
                    && (options.stop_when_max_launches_seen
                        || tokio::time::Instant::now() >= deadline)
                {
                    break;
                }
            }
            Ok(Some(Err(status))) => {
                summary.provider_status = match status.code() {
                    tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
                        "auth_rejected".to_owned()
                    }
                    tonic::Code::Unimplemented => "unsupported".to_owned(),
                    _ => "stream_error".to_owned(),
                };
                summary.errors.push(status.to_string());
                return Err(anyhow!(summary.errors.join("; ")));
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    if summary.tracked_mint.is_some() {
        summary.provider_status = "launch_detected".to_owned();
    } else if summary.transaction_updates + summary.account_updates + summary.slot_updates == 0 {
        summary.provider_status = "connected_zero_updates".to_owned();
    } else {
        summary.provider_status = "no_launch_detected".to_owned();
    }
    Ok((summary, events))
}

pub async fn smoke_geyser_provider_with_connector(
    loaded: &LoadedConfig,
    options: GeyserProviderSmokeOptions,
    connector: Arc<dyn GeyserStreamConnector>,
) -> Result<GeyserProviderSmokeSummary> {
    let config = loaded.config.geyser.clone();
    let mut summary = GeyserProviderSmokeSummary {
        endpoint_configured: geyser_endpoint_configured(&config),
        auth_configured: geyser_auth_configured(&config),
        provider_status: "not_attempted".to_owned(),
        duration_seconds: options.duration_seconds.max(1),
        no_live_orders: true,
        limitations: vec![
            "geyser_transactions_accounts_slots_remain_canonical_truth".to_owned(),
            "stream_only_forbids_rpc_market_data_fallbacks".to_owned(),
            "raw_production_shred_decoder_remains_fail_closed".to_owned(),
        ],
        ..GeyserProviderSmokeSummary::default()
    };
    if !summary.endpoint_configured {
        summary.provider_status = "not_attempted_missing_endpoint".to_owned();
        summary
            .errors
            .push("geyser endpoint is not configured".to_owned());
        if options.strict {
            return Err(anyhow!("geyser endpoint is not configured"));
        }
        return Ok(summary);
    }

    let mut normalizer = GeyserEventNormalizer::from_loaded(loaded)?;
    let mut ingest = GeyserIngestService::new(config.clone());
    let request = ingest.proto_subscription_request();
    let stream_result = connector.connect_and_subscribe(&config, request).await;
    let mut stream = match stream_result {
        Ok(stream) => {
            summary.provider_status = "connected".to_owned();
            summary.connected = true;
            stream
        }
        Err(error) => {
            if let Some(status) = error.downcast_ref::<Status>() {
                summary.provider_status = match status.code() {
                    tonic::Code::Unimplemented => "unsupported".to_owned(),
                    tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
                        "auth_rejected".to_owned()
                    }
                    _ => "connection_failed".to_owned(),
                };
                summary.errors.push(status.to_string());
            } else {
                summary.provider_status = "connection_failed".to_owned();
                summary.errors.push(error.to_string());
            }
            if options.strict {
                return Err(anyhow!(summary.errors.join("; ")));
            }
            return Ok(summary);
        }
    };

    let deadline =
        tokio::time::Instant::now() + StdDuration::from_secs(options.duration_seconds.max(1));
    while tokio::time::Instant::now() < deadline {
        if let Some(max_updates) = options.max_updates {
            let total_updates = summary.transaction_updates
                + summary.account_updates
                + summary.slot_updates
                + summary.block_updates
                + summary.block_meta_updates;
            if total_updates as usize >= max_updates {
                break;
            }
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let poll_window = remaining.min(StdDuration::from_millis(250));
        if poll_window.is_zero() {
            break;
        }
        match tokio::time::timeout(poll_window, stream.next()).await {
            Ok(Some(Ok(update))) => {
                match update.update_oneof.as_ref() {
                    Some(UpdateOneof::Transaction(_)) => {
                        summary.transaction_updates = summary.transaction_updates.saturating_add(1);
                    }
                    Some(UpdateOneof::TransactionStatus(_)) => {
                        summary.transaction_updates = summary.transaction_updates.saturating_add(1);
                    }
                    Some(UpdateOneof::Account(_)) => {
                        summary.account_updates = summary.account_updates.saturating_add(1);
                    }
                    Some(UpdateOneof::Slot(_)) => {
                        summary.slot_updates = summary.slot_updates.saturating_add(1);
                    }
                    Some(UpdateOneof::Block(_)) => {
                        summary.block_updates = summary.block_updates.saturating_add(1);
                    }
                    Some(UpdateOneof::BlockMeta(_)) => {
                        summary.block_meta_updates = summary.block_meta_updates.saturating_add(1);
                    }
                    _ => {}
                }

                let pump_programs = &loaded.config.pump.program_ids;
                let pump_candidate = match update.update_oneof.as_ref() {
                    Some(UpdateOneof::Transaction(tx)) => tx
                        .transaction
                        .as_ref()
                        .and_then(|info| info.transaction.as_ref())
                        .and_then(|tx| tx.message.as_ref())
                        .map(|message| {
                            message.instructions.iter().any(|instruction| {
                                message
                                    .account_keys
                                    .get(instruction.program_id_index as usize)
                                    .map(|bytes| bs58::encode(bytes).into_string())
                                    .map(|program_id| {
                                        pump_programs.iter().any(|pump| pump == &program_id)
                                    })
                                    .unwrap_or(false)
                            })
                        })
                        .unwrap_or(false),
                    Some(UpdateOneof::TransactionStatus(_)) => false,
                    _ => false,
                };

                let outputs = ingest.process_subscribe_update(update, monotonic_now_ns());
                if outputs.is_empty() {
                    summary.decode_errors = summary.decode_errors.saturating_add(1);
                    continue;
                }
                let normalized = outputs
                    .into_iter()
                    .flat_map(|output| normalizer.normalize_output(output))
                    .collect::<Vec<_>>();
                let mut decoded_domain_events = 0u64;
                for event in normalized {
                    match event.payload {
                        EventPayload::TokenCreated(_) => {
                            summary.pump_create_decoded =
                                summary.pump_create_decoded.saturating_add(1);
                            decoded_domain_events = decoded_domain_events.saturating_add(1);
                        }
                        EventPayload::PumpBuy(_) => {
                            summary.pump_buy_decoded = summary.pump_buy_decoded.saturating_add(1);
                            decoded_domain_events = decoded_domain_events.saturating_add(1);
                        }
                        EventPayload::PumpSell(_) => {
                            summary.pump_sell_decoded = summary.pump_sell_decoded.saturating_add(1);
                            decoded_domain_events = decoded_domain_events.saturating_add(1);
                        }
                        EventPayload::BondingCurveUpdate(_) => {
                            summary.bonding_curve_updates =
                                summary.bonding_curve_updates.saturating_add(1);
                            decoded_domain_events = decoded_domain_events.saturating_add(1);
                        }
                        EventPayload::HolderBalanceUpdate(_) => {
                            summary.holder_updates = summary.holder_updates.saturating_add(1);
                            decoded_domain_events = decoded_domain_events.saturating_add(1);
                        }
                        EventPayload::WalletFunding(_) => {
                            summary.funding_events = summary.funding_events.saturating_add(1);
                            decoded_domain_events = decoded_domain_events.saturating_add(1);
                        }
                        EventPayload::ObservedTransaction(_) => {}
                        _ => {}
                    }
                }
                if decoded_domain_events > 0 {
                    summary.pump_relevant_transactions =
                        summary.pump_relevant_transactions.saturating_add(1);
                } else if pump_candidate {
                    summary.unknown_instructions = summary.unknown_instructions.saturating_add(1);
                }
            }
            Ok(Some(Err(status))) => {
                summary.provider_status = match status.code() {
                    tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
                        "auth_rejected".to_owned()
                    }
                    tonic::Code::Unimplemented => "unsupported".to_owned(),
                    _ => "stream_error".to_owned(),
                };
                summary.errors.push(status.to_string());
                if options.strict {
                    return Err(anyhow!(summary.errors.join("; ")));
                }
                return Ok(summary);
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    let total_updates = summary.transaction_updates
        + summary.account_updates
        + summary.slot_updates
        + summary.block_updates
        + summary.block_meta_updates;
    if total_updates == 0 {
        summary.provider_status = "connected_zero_updates".to_owned();
    } else {
        summary.provider_status = "updates_received".to_owned();
    }
    Ok(summary)
}

pub fn inspect_deshred_capability(
    loaded: &LoadedConfig,
    endpoint_override: Option<&str>,
    auth_override_present: Option<bool>,
) -> DeshredCapability {
    let config = loaded.config.ingest.deshred.clone().unwrap_or_default();
    let endpoint_configured = endpoint_override
        .map(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            (!config.endpoint.trim().is_empty())
                || (!config.endpoint_env.trim().is_empty()
                    && std::env::var(&config.endpoint_env)
                        .ok()
                        .map(|value| !value.trim().is_empty())
                        .unwrap_or(false))
        });
    let auth_configured = auth_override_present.unwrap_or_else(|| {
        if !config.auth_required {
            true
        } else if config.auth_token_env.trim().is_empty() {
            false
        } else {
            std::env::var(&config.auth_token_env)
                .ok()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
        }
    });
    let supported_by_proto = true;
    let supported_by_client = true;
    let reason_if_unsupported = if !supported_by_proto {
        Some("yellowstone_grpc_proto does not expose SubscribeDeshred".to_owned())
    } else if !supported_by_client {
        Some("runtime does not compile a deshred stream connector".to_owned())
    } else {
        None
    };
    DeshredCapability {
        supported_by_proto,
        supported_by_client,
        supports_program_filters: config.program_filters_from_pump_ids,
        supports_account_filters: true,
        exposes_loaded_addresses: true,
        exposes_signature: true,
        exposes_slot: true,
        exposes_raw_transaction: true,
        exposes_instruction_data: true,
        exposes_transaction_status_meta: false,
        endpoint_configured,
        auth_configured,
        can_enable: supported_by_proto && supported_by_client,
        reason_if_unsupported,
    }
}

pub async fn smoke_deshred_provider(
    loaded: &LoadedConfig,
    options: DeshredProviderSmokeOptions,
) -> Result<DeshredProviderSmokeSummary> {
    smoke_deshred_provider_with_connector(loaded, options, Arc::new(RealDeshredConnector)).await
}

pub async fn smoke_deshred_provider_with_connector(
    loaded: &LoadedConfig,
    options: DeshredProviderSmokeOptions,
    connector: Arc<dyn DeshredStreamConnector>,
) -> Result<DeshredProviderSmokeSummary> {
    let config = loaded.config.ingest.deshred.clone().unwrap_or_default();
    let capability = inspect_deshred_capability(loaded, None, None);
    let mut summary = DeshredProviderSmokeSummary {
        endpoint_configured: capability.endpoint_configured,
        auth_configured: capability.auth_configured,
        auth_metadata_key_configured: !config.auth_metadata_key.trim().is_empty(),
        proto_support: capability.supported_by_proto,
        client_support: capability.supported_by_client,
        provider_status: "not_attempted".to_owned(),
        duration_seconds: options.duration_seconds.max(1),
        no_live_orders: true,
        limitations: vec![
            "deshred_is_pre_execution_tentative_only".to_owned(),
            "geyser_account_effects_remain_canonical_truth".to_owned(),
            "raw_production_shred_decoder_remains_fail_closed".to_owned(),
        ],
        ..DeshredProviderSmokeSummary::default()
    };
    if !capability.can_enable {
        summary.provider_status = "unsupported_build".to_owned();
        summary.errors.push(
            capability
                .reason_if_unsupported
                .unwrap_or_else(|| "deshred unsupported by this build".to_owned()),
        );
        if options.strict || options.require_deshred {
            return Err(anyhow!(summary.errors.join("; ")));
        }
        return Ok(summary);
    }
    if !capability.endpoint_configured {
        summary.provider_status = "not_attempted_missing_endpoint".to_owned();
        summary
            .errors
            .push("deshred endpoint is not configured".to_owned());
        if options.strict || options.require_deshred {
            return Err(anyhow!("deshred endpoint is not configured"));
        }
        return Ok(summary);
    }

    let mut normalizer = GeyserEventNormalizer::from_loaded(loaded)?;
    let request = build_deshred_request(&config, &loaded.config.pump.program_ids);
    let stream_result = connector.connect_and_subscribe(&config, request).await;
    let mut stream = match stream_result {
        Ok(stream) => {
            summary.provider_status = "connected".to_owned();
            stream
        }
        Err(error) => {
            if let Some(status) = error.downcast_ref::<Status>() {
                summary.provider_status = match status.code() {
                    tonic::Code::Unimplemented => "unimplemented".to_owned(),
                    tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
                        "auth_rejected".to_owned()
                    }
                    _ => "connection_failed".to_owned(),
                };
                summary.errors.push(status.to_string());
            } else {
                summary.provider_status = "connection_failed".to_owned();
                summary.errors.push(error.to_string());
            }
            if options.strict || options.require_deshred {
                return Err(anyhow!(summary.errors.join("; ")));
            }
            return Ok(summary);
        }
    };

    let deadline =
        tokio::time::Instant::now() + StdDuration::from_secs(options.duration_seconds.max(1));
    while tokio::time::Instant::now() < deadline {
        if let Some(max_updates) = options.max_updates {
            if summary.updates_received as usize >= max_updates {
                break;
            }
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let poll_window = remaining.min(StdDuration::from_millis(250));
        if poll_window.is_zero() {
            break;
        }
        let next = tokio::time::timeout(poll_window, stream.next()).await;
        match next {
            Ok(Some(Ok(update))) => {
                summary.updates_received = summary.updates_received.saturating_add(1);
                let normalized = normalized_events_from_deshred_update(
                    &mut normalizer,
                    update,
                    monotonic_now_ns(),
                );
                if !normalized.is_empty() {
                    summary.tentative_transactions_decoded =
                        summary.tentative_transactions_decoded.saturating_add(1);
                }
                if normalized.iter().any(|event| {
                    matches!(
                        event.payload,
                        EventPayload::PumpBuy(_)
                            | EventPayload::PumpSell(_)
                            | EventPayload::TokenCreated(_)
                            | EventPayload::BondingCurveUpdate(_)
                    )
                }) {
                    summary.pump_relevant_transactions =
                        summary.pump_relevant_transactions.saturating_add(1);
                }
                for event in normalized {
                    if matches!(event.payload, EventPayload::PumpSell(_)) {
                        summary.tentative_sells_detected =
                            summary.tentative_sells_detected.saturating_add(1);
                        summary.decoded_sell_instructions =
                            summary.decoded_sell_instructions.saturating_add(1);
                    }
                }
            }
            Ok(Some(Err(status))) => {
                summary.provider_status = match status.code() {
                    tonic::Code::Unimplemented => "unimplemented".to_owned(),
                    tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
                        "auth_rejected".to_owned()
                    }
                    _ => "stream_error".to_owned(),
                };
                summary.errors.push(status.to_string());
                if options.strict || options.require_deshred {
                    return Err(anyhow!(summary.errors.join("; ")));
                }
                return Ok(summary);
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    if summary.updates_received == 0 {
        summary.provider_status = "zero_updates".to_owned();
    } else {
        summary.provider_status = "updates_received".to_owned();
    }
    Ok(summary)
}

pub fn build_deshred_request(
    config: &common::DeshredConfig,
    pump_program_ids: &[String],
) -> SubscribeDeshredRequest {
    let mut request = SubscribeDeshredRequest::default();
    if config.subscribe_transactions {
        request.deshred_transactions.insert(
            "pump_programs".to_owned(),
            SubscribeRequestFilterDeshredTransactions {
                vote: Some(false),
                account_include: if config.program_filters_from_pump_ids {
                    pump_program_ids.to_vec()
                } else {
                    Vec::new()
                },
                account_exclude: Vec::new(),
                account_required: Vec::new(),
            },
        );
    }
    request
}

fn meta_for_deshred_tx(
    update: &SubscribeUpdateDeshred,
    tx: &TransactionUpdate,
    observed_at_monotonic_ns: u64,
) -> EventMeta {
    let mut meta = EventMeta::new(
        EventSource::DeshredTentative,
        Canonicality::Tentative,
        tx.slot,
    );
    meta.signature = Some(tx.signature.clone());
    meta.transaction_index = tx.transaction_index;
    meta.received_at_wall_time = update
        .created_at
        .as_ref()
        .and_then(ingest_geyser::timestamp_to_offset)
        .unwrap_or_else(OffsetDateTime::now_utc);
    meta.observed_at_monotonic_ns = observed_at_monotonic_ns;
    meta.decode_confidence = Decimal::new(90, 2);
    meta.raw_reference = Some(RawEventReference {
        source_id: "deshred".to_owned(),
        cursor: Some(match update.update_oneof.as_ref() {
            Some(subscribe_update_deshred::UpdateOneof::DeshredTransaction(tx_update)) => format!(
                "{}:{}",
                tx_update
                    .transaction
                    .as_ref()
                    .map(|value| value.completed_data_set_starting_shred_index)
                    .unwrap_or_default(),
                tx_update
                    .transaction
                    .as_ref()
                    .map(|value| value.completed_data_set_ending_shred_index_exclusive)
                    .unwrap_or_default()
            ),
            _ => "deshred".to_owned(),
        }),
        offset: update.update_oneof.as_ref().and_then(|value| match value {
            subscribe_update_deshred::UpdateOneof::DeshredTransaction(tx_update) => tx_update
                .transaction
                .as_ref()
                .map(|inner| inner.completed_data_set_starting_shred_index as u64),
            _ => None,
        }),
    });
    meta
}

fn normalized_events_from_deshred_update(
    normalizer: &mut GeyserEventNormalizer,
    update: SubscribeUpdateDeshred,
    observed_at_monotonic_ns: u64,
) -> Vec<NormalizedEvent> {
    let Some(subscribe_update_deshred::UpdateOneof::DeshredTransaction(tx_update)) =
        update.update_oneof.clone()
    else {
        return Vec::new();
    };
    let Some(tx) = transaction_update_from_deshred_proto(tx_update) else {
        return Vec::new();
    };
    let meta = meta_for_deshred_tx(&update, &tx, observed_at_monotonic_ns);
    normalizer.normalize_transaction(meta, tx)
}

pub async fn run_geyser_source_with_connector(
    config: common::GeyserConfig,
    mut normalizer: GeyserEventNormalizer,
    connector: Arc<dyn GeyserStreamConnector>,
    sender: mpsc::Sender<NormalizedEvent>,
) -> Result<()> {
    let endpoint = resolved_geyser_endpoint(&config)?;
    let mut resolved = config.clone();
    resolved.endpoint = endpoint.clone();
    let mut ingest = GeyserIngestService::new(resolved.clone());
    let subscription = ingest.proto_subscription_request();
    let mut attempts = 0usize;
    let max_attempts = resolved.max_reconnect_attempts.unwrap_or(10).max(1) as usize;
    let mut emitted_disconnect_gap = false;
    loop {
        let stream = connector
            .connect_and_subscribe(&resolved, subscription.clone())
            .await;
        let mut stream = match stream {
            Ok(stream) => {
                if emitted_disconnect_gap {
                    let recovery = source_gap_event(
                        ingest.health().last_slot_seen.unwrap_or_default(),
                        true,
                        "recover_stream_resumed",
                    );
                    if sender.send(recovery).await.is_err() {
                        return Ok(());
                    }
                    emitted_disconnect_gap = false;
                }
                attempts = 0;
                stream
            }
            Err(error) => {
                attempts = attempts.saturating_add(1);
                if attempts >= max_attempts {
                    return Err(error);
                }
                tokio::time::sleep(next_backoff_ms(&resolved, attempts)).await;
                continue;
            }
        };

        while let Some(update) = stream.next().await {
            match update {
                Ok(update) => {
                    let observed = monotonic_now_ns();
                    let outputs = ingest.process_subscribe_update(update, observed);
                    for output in outputs {
                        for event in normalizer.normalize_output(output) {
                            if sender.send(event).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
                Err(status) => {
                    warn!(error = %status, endpoint = resolved.endpoint, "geyser stream update failed");
                    ingest.note_disconnect();
                    let gap = source_gap_event(
                        ingest.health().last_slot_seen.unwrap_or_default(),
                        false,
                        "reconnect_stream_gap",
                    );
                    if sender.send(gap).await.is_err() {
                        return Ok(());
                    }
                    emitted_disconnect_gap = true;
                    attempts = attempts.saturating_add(1);
                    if attempts >= max_attempts {
                        return Err(anyhow!(
                            "geyser stream closed after {attempts} reconnect attempts: {status}"
                        ));
                    }
                    tokio::time::sleep(next_backoff_ms(&resolved, attempts)).await;
                    break;
                }
            }
        }

        if !emitted_disconnect_gap {
            return Ok(());
        }
    }
}

pub async fn run_deshred_source_with_connector(
    config: common::DeshredConfig,
    pump_program_ids: Vec<String>,
    mut normalizer: GeyserEventNormalizer,
    connector: Arc<dyn DeshredStreamConnector>,
    sender: mpsc::Sender<NormalizedEvent>,
) -> Result<()> {
    let request = build_deshred_request(&config, &pump_program_ids);
    let mut attempts = 0usize;
    let max_attempts = config.max_reconnect_attempts.max(1) as usize;
    let mut emitted_disconnect_gap = false;
    loop {
        let stream = connector
            .connect_and_subscribe(&config, request.clone())
            .await;
        let mut stream = match stream {
            Ok(stream) => {
                if emitted_disconnect_gap {
                    let recovery = source_gap_event_for_source(
                        EventSource::DeshredTentative,
                        0,
                        true,
                        "recover_deshred_stream_resumed",
                    );
                    if sender.send(recovery).await.is_err() {
                        return Ok(());
                    }
                    emitted_disconnect_gap = false;
                }
                attempts = 0;
                stream
            }
            Err(error) => {
                if let Some(status) = error.downcast_ref::<tonic::Status>() {
                    if status.code() == tonic::Code::Unimplemented
                        && !(config.required || config.fail_if_unsupported)
                    {
                        return Ok(());
                    }
                }
                attempts = attempts.saturating_add(1);
                if attempts >= max_attempts {
                    return Err(error);
                }
                let backoff = (config.reconnect_backoff_ms as usize)
                    .saturating_mul(attempts.max(1))
                    .min(config.max_reconnect_backoff_ms.max(1) as usize)
                    as u64;
                tokio::time::sleep(StdDuration::from_millis(backoff)).await;
                continue;
            }
        };

        while let Some(update) = stream.next().await {
            match update {
                Ok(update) => {
                    let observed = monotonic_now_ns();
                    for event in
                        normalized_events_from_deshred_update(&mut normalizer, update, observed)
                    {
                        if sender.send(event).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                Err(status) => {
                    if status.code() == tonic::Code::Unimplemented {
                        if config.required || config.fail_if_unsupported {
                            return Err(anyhow!(
                                "provider returned unimplemented for SubscribeDeshred"
                            ));
                        }
                        return Ok(());
                    }
                    warn!(error = %status, "deshred stream update failed");
                    let gap = source_gap_event_for_source(
                        EventSource::DeshredTentative,
                        0,
                        false,
                        "reconnect_deshred_stream_gap",
                    );
                    if sender.send(gap).await.is_err() {
                        return Ok(());
                    }
                    emitted_disconnect_gap = true;
                    attempts = attempts.saturating_add(1);
                    if attempts >= max_attempts {
                        return Err(anyhow!(
                            "deshred stream closed after {attempts} reconnect attempts: {status}"
                        ));
                    }
                    let backoff = (config.reconnect_backoff_ms as usize)
                        .saturating_mul(attempts.max(1))
                        .min(config.max_reconnect_backoff_ms.max(1) as usize)
                        as u64;
                    tokio::time::sleep(StdDuration::from_millis(backoff)).await;
                    break;
                }
            }
        }

        if !emitted_disconnect_gap {
            return Ok(());
        }
    }
}

#[derive(Debug, Clone)]
struct MetadataInjector {
    key: Option<MetadataKey<Ascii>>,
    value: Option<MetadataValue<Ascii>>,
}

impl MetadataInjector {
    fn new(metadata: Option<(&str, &str)>) -> Result<Self> {
        let Some((key, value)) = metadata else {
            return Ok(Self {
                key: None,
                value: None,
            });
        };
        Ok(Self {
            key: Some(
                MetadataKey::from_str(key)
                    .map_err(|error| anyhow!("invalid geyser auth metadata key: {error}"))?,
            ),
            value: Some(
                MetadataValue::from_str(value)
                    .map_err(|error| anyhow!("invalid geyser auth metadata value: {error}"))?,
            ),
        })
    }
}

impl Interceptor for MetadataInjector {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        if let (Some(key), Some(value)) = (self.key.clone(), self.value.clone()) {
            request.metadata_mut().insert(key, value);
        }
        Ok(request)
    }
}

fn source_gap_event(slot: u64, recovered: bool, action: &str) -> NormalizedEvent {
    source_gap_event_for_source(EventSource::GeyserProcessed, slot, recovered, action)
}

fn source_gap_event_for_source(
    source: EventSource,
    slot: u64,
    recovered: bool,
    action: &str,
) -> NormalizedEvent {
    let mut meta = EventMeta::new(source, Canonicality::Processed, slot);
    meta.received_at_wall_time = OffsetDateTime::now_utc();
    meta.observed_at_monotonic_ns = monotonic_now_ns();
    NormalizedEvent {
        meta,
        payload: EventPayload::DataGap(DataGapEvent {
            gap_type: DataGapType::ReconnectGap,
            source,
            start_slot: slot,
            end_slot: Some(slot),
            affected_tokens: Vec::new(),
            severity: if recovered {
                GapSeverity::Low
            } else {
                GapSeverity::High
            },
            trade_allowed: recovered,
            recovery_action: action.to_owned(),
        }),
    }
}

fn next_backoff_ms(config: &common::GeyserConfig, attempt: usize) -> StdDuration {
    let backoff = config
        .reconnect_backoff_ms
        .get(attempt.saturating_sub(1))
        .copied()
        .or_else(|| config.reconnect_backoff_ms.last().copied())
        .unwrap_or(1000)
        .min(config.max_reconnect_backoff_ms.max(1));
    StdDuration::from_millis(backoff)
}

fn tx_status(succeeded: bool) -> TransactionStatus {
    if succeeded {
        TransactionStatus::Success
    } else {
        TransactionStatus::Failed
    }
}

fn safe_price(quote: Decimal, token: Decimal) -> Decimal {
    if token <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        quote / token
    }
}

fn value_decimal(map: &BTreeMap<String, Value>, key: &str) -> Option<Decimal> {
    map.get(key).and_then(json_decimal)
}

fn value_decimal_alias(map: &BTreeMap<String, Value>, keys: &[&str]) -> Option<Decimal> {
    keys.iter().find_map(|key| value_decimal(map, key))
}

fn value_string(map: &BTreeMap<String, Value>, key: &str) -> Option<String> {
    map.get(key)
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
}

fn value_pubkey(map: &BTreeMap<String, Value>, key: &str) -> Option<PubkeyValue> {
    value_string(map, key).map(PubkeyValue)
}

fn account_alias(map: &HashMap<String, String>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| map.get(*key).cloned())
}

fn quote_asset_type(args: &BTreeMap<String, Value>) -> QuoteAssetType {
    match value_string(args, "quote_mint").as_deref() {
        Some(WSOL_MINT) => QuoteAssetType::WrappedSol,
        Some(_) => QuoteAssetType::Other,
        None => QuoteAssetType::Unknown,
    }
}

fn json_decimal(value: &Value) -> Option<Decimal> {
    if let Some(number) = value.as_u64() {
        return Some(Decimal::from(number));
    }
    if let Some(number) = value.as_i64() {
        return Some(Decimal::from(number));
    }
    value.as_str().and_then(|raw| Decimal::from_str(raw).ok())
}

fn count_decoded_buys(instructions: &[TransactionInstruction], idls: &[LoadedIdl]) -> u32 {
    instructions
        .iter()
        .filter(|instruction| {
            let Ok(data) = hex::decode(&instruction.data_hex) else {
                return false;
            };
            idls.iter().any(|idl| {
                matches!(
                    idl.decode_instruction(&data),
                    Ok(InstructionDecode::Known { decoded })
                    if matches!(decoded.name.as_str(), "buy" | "buy_v2" | "buy_exact_quote_in_v2")
                )
            })
        })
        .count() as u32
}

fn parse_compute_budget(instructions: &[TransactionInstruction]) -> (Option<u32>, Option<u64>) {
    let mut unit_limit = None;
    let mut unit_price = None;
    for instruction in instructions {
        if instruction.program_id != COMPUTE_BUDGET_PROGRAM_ID {
            continue;
        }
        let Ok(data) = hex::decode(&instruction.data_hex) else {
            continue;
        };
        match data.first().copied() {
            Some(2) if data.len() >= 5 => {
                let mut raw = [0u8; 4];
                raw.copy_from_slice(&data[1..5]);
                unit_limit = Some(u32::from_le_bytes(raw));
            }
            Some(3) if data.len() >= 9 => {
                let mut raw = [0u8; 8];
                raw.copy_from_slice(&data[1..9]);
                unit_price = Some(u64::from_le_bytes(raw));
            }
            _ => {}
        }
    }
    (unit_limit, unit_price)
}

fn hash_strings(values: &[String]) -> String {
    let mut hasher = Sha256::new();
    for value in values {
        hasher.update(value.as_bytes());
        hasher.update([0xff]);
    }
    format!("{:x}", hasher.finalize())
}

fn hash_instruction_shape(instructions: &[TransactionInstruction]) -> String {
    let mut hasher = Sha256::new();
    for instruction in instructions {
        hasher.update(instruction.program_id.as_bytes());
        hasher.update([0xfe]);
        hasher.update(instruction.accounts.len().to_string().as_bytes());
        hasher.update([0xfd]);
        hasher.update(instruction.data_hex.len().to_string().as_bytes());
        hasher.update([0xfc]);
    }
    format!("{:x}", hasher.finalize())
}

fn bundle_like_evidence(
    update: &TransactionUpdate,
    compute_budget: (Option<u32>, Option<u64>),
) -> Option<String> {
    let mut evidence = Vec::new();
    if update.transaction_index.is_some() {
        evidence.push("transaction_index_observed");
    }
    if compute_budget.1.unwrap_or_default() > 0 {
        evidence.push("priority_fee_bid_observed");
    }
    if update.compute_units_consumed.is_some() {
        evidence.push("compute_units_observed");
    }
    (!evidence.is_empty()).then(|| evidence.join("|"))
}

#[derive(Debug, Clone)]
struct PreTokenBalance {
    owner: Option<String>,
    token_account: String,
    amount_raw: Decimal,
    account_index: u32,
    decimals: u32,
}

fn holder_balance_events(meta: &EventMeta, update: &TransactionUpdate) -> Vec<NormalizedEvent> {
    let mut pre = HashMap::<(String, String), PreTokenBalance>::new();
    for balance in &update.pre_token_balances {
        let token_account = account_key_at(&update.account_keys, balance.account_index);
        pre.insert(
            (balance.mint.clone(), token_account.clone()),
            PreTokenBalance {
                owner: balance.owner.clone(),
                token_account,
                amount_raw: token_amount(balance),
                account_index: balance.account_index,
                decimals: balance.decimals,
            },
        );
    }
    let mut events = Vec::new();
    for balance in &update.post_token_balances {
        let token_account = account_key_at(&update.account_keys, balance.account_index);
        let previous = pre.remove(&(balance.mint.clone(), token_account.clone()));
        let owner = balance
            .owner
            .clone()
            .or_else(|| previous.as_ref().and_then(|old| old.owner.clone()))
            .unwrap_or_else(|| format!("owner_{}", balance.account_index));
        let old = previous.as_ref().map(|old| old.amount_raw);
        let new_balance = token_amount(balance);
        let delta = new_balance - old.unwrap_or(Decimal::ZERO);
        let token_decimals = u8::try_from(balance.decimals).ok();
        let mut holder_meta = meta.clone();
        holder_meta.account_pubkey = Some(PubkeyValue(token_account.clone()));
        events.push(NormalizedEvent {
            meta: holder_meta,
            payload: EventPayload::HolderBalanceUpdate(HolderBalanceUpdateEvent {
                mint: PubkeyValue(balance.mint.clone()),
                owner_wallet: PubkeyValue(owner.clone()),
                token_account: PubkeyValue(token_account),
                token_decimals,
                old_balance: old,
                new_balance,
                delta,
                caused_by_signature: Some(update.signature.clone()),
                update_reason: "geyser_token_balance".to_owned(),
                confidence: Decimal::ONE,
            }),
        });
    }
    for ((mint, _), previous) in pre {
        let owner = previous
            .owner
            .clone()
            .unwrap_or_else(|| format!("owner_{}", previous.account_index));
        let token_decimals = u8::try_from(previous.decimals).ok();
        let mut holder_meta = meta.clone();
        holder_meta.account_pubkey = Some(PubkeyValue(previous.token_account.clone()));
        events.push(NormalizedEvent {
            meta: holder_meta,
            payload: EventPayload::HolderBalanceUpdate(HolderBalanceUpdateEvent {
                mint: PubkeyValue(mint),
                owner_wallet: PubkeyValue(owner),
                token_account: PubkeyValue(previous.token_account),
                token_decimals,
                old_balance: Some(previous.amount_raw),
                new_balance: Decimal::ZERO,
                delta: -previous.amount_raw,
                caused_by_signature: Some(update.signature.clone()),
                update_reason: "geyser_token_balance_closed_or_zeroed".to_owned(),
                confidence: Decimal::ONE,
            }),
        });
    }
    events
}

fn funding_event_from_transaction(
    meta: &EventMeta,
    update: &TransactionUpdate,
) -> Option<NormalizedEvent> {
    let instruction = update
        .instructions
        .iter()
        .find(|instruction| instruction.program_id == SYSTEM_PROGRAM_ID)?;
    let wallet = instruction.accounts.get(1)?.clone();
    let funder = instruction.accounts.first()?.clone();
    let amount = estimate_lamport_gain(update, &wallet)?;
    if amount <= Decimal::ZERO {
        return None;
    }
    Some(NormalizedEvent {
        meta: meta.clone(),
        payload: EventPayload::WalletFunding(WalletFundingEvent {
            wallet: PubkeyValue(wallet),
            funder: PubkeyValue(funder),
            asset_label: "SOL".to_owned(),
            amount,
            slot: update.slot,
            signature: update.signature.clone(),
            relation_to_launch: None,
            near_launch_relation: false,
            funding_graph_edge_id: format!("funding:{}", update.signature),
        }),
    })
}

fn estimate_token_delta(
    from: &[TransactionTokenBalance],
    to: &[TransactionTokenBalance],
    mint: &str,
    owner: Option<&str>,
) -> Option<Decimal> {
    let from_total = total_token_balance(from, mint, owner);
    let to_total = total_token_balance(to, mint, owner);
    Some(to_total - from_total)
}

fn total_token_balance(
    balances: &[TransactionTokenBalance],
    mint: &str,
    owner: Option<&str>,
) -> Decimal {
    balances
        .iter()
        .filter(|balance| {
            balance.mint == mint
                && owner.is_none_or(|candidate| balance.owner.as_deref() == Some(candidate))
        })
        .map(token_amount)
        .fold(Decimal::ZERO, |acc, value| acc + value)
}

fn token_amount(balance: &TransactionTokenBalance) -> Decimal {
    Decimal::from_str(&balance.amount).unwrap_or(Decimal::ZERO)
}

fn estimate_lamport_gain(update: &TransactionUpdate, account: &str) -> Option<Decimal> {
    let index = update.account_keys.iter().position(|key| key == account)?;
    let pre = update.pre_balances.get(index).copied().unwrap_or_default();
    let post = update.post_balances.get(index).copied().unwrap_or_default();
    Some(Decimal::from(post.saturating_sub(pre)))
}

fn estimate_lamport_spend(update: &TransactionUpdate, account: &str) -> Option<Decimal> {
    let index = update.account_keys.iter().position(|key| key == account)?;
    let pre = update.pre_balances.get(index).copied().unwrap_or_default();
    let post = update.post_balances.get(index).copied().unwrap_or_default();
    let total_spend = pre.saturating_sub(post);
    let net_spend = total_spend.saturating_sub(update.fee_lamports);
    Some(Decimal::from(net_spend))
}

fn account_key_at(account_keys: &[String], index: u32) -> String {
    account_keys
        .get(index as usize)
        .cloned()
        .unwrap_or_else(|| format!("account_{index}"))
}

fn pending_curve_update_from_decoded(
    meta: EventMeta,
    curve_pubkey: String,
    decoded: &DecodedAccount,
    update: &AccountUpdate,
) -> PendingCurveUpdate {
    PendingCurveUpdate {
        meta,
        curve_pubkey,
        virtual_quote: value_decimal_alias(
            &decoded.fields,
            &["virtual_quote_reserves", "virtual_sol_reserves"],
        )
        .unwrap_or(Decimal::ZERO),
        virtual_token: value_decimal(&decoded.fields, "virtual_token_reserves")
            .unwrap_or(Decimal::ZERO),
        real_quote: value_decimal_alias(
            &decoded.fields,
            &["real_quote_reserves", "real_sol_reserves"],
        )
        .unwrap_or(Decimal::ZERO),
        real_token: value_decimal(&decoded.fields, "real_token_reserves").unwrap_or(Decimal::ZERO),
        token_total_supply_raw: value_decimal(&decoded.fields, "token_total_supply"),
        creator: value_pubkey(&decoded.fields, "creator"),
        quote_mint: value_pubkey(&decoded.fields, "quote_mint"),
        reserve_field_schema: if decoded.fields.contains_key("virtual_quote_reserves")
            || decoded.fields.contains_key("real_quote_reserves")
        {
            "quote_reserves_v2".to_owned()
        } else {
            "legacy_sol_reserves".to_owned()
        },
        complete: decoded
            .fields
            .get("complete")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        transaction_signature: update.transaction_signature.clone(),
        write_version: update.write_version,
    }
}

fn bonding_curve_event_from_pending(pending: PendingCurveUpdate, mint: &str) -> NormalizedEvent {
    let token_decimals = DEFAULT_PUMP_TOKEN_DECIMALS;
    let price_lamports_per_raw =
        price_lamports_per_raw_token(pending.virtual_quote, pending.virtual_token);
    let price_sol_per_token = pump_virtual_reserve_price_sol_per_token(
        pending.virtual_quote,
        pending.virtual_token,
        token_decimals,
    )
    .unwrap_or(Decimal::ZERO);
    let curve_progress_pct =
        pump_curve_progress_pct_from_real_token_reserves_raw(pending.real_token, token_decimals);
    let market_cap_quote_1b = pump_market_cap_quote_1b(price_sol_per_token);
    let token_total_supply_ui = pending
        .token_total_supply_raw
        .map(|raw| raw_tokens_to_ui(raw, token_decimals));
    let confidence = if price_lamports_per_raw.is_some() {
        Decimal::ONE
    } else {
        Decimal::ZERO
    };
    let mut meta = pending.meta;
    meta.account_pubkey = Some(PubkeyValue(pending.curve_pubkey));
    NormalizedEvent {
        meta,
        payload: EventPayload::BondingCurveUpdate(BondingCurveUpdateEvent {
            mint: PubkeyValue(mint.to_owned()),
            virtual_quote_reserves: pending.virtual_quote,
            virtual_token_reserves: pending.virtual_token,
            real_quote_reserves: pending.real_quote,
            real_token_reserves: pending.real_token,
            token_decimals: Some(token_decimals),
            token_total_supply_raw: pending.token_total_supply_raw,
            token_total_supply_ui,
            token_total_supply_source: pending
                .token_total_supply_raw
                .map(|_| "bonding_curve_observed".to_owned()),
            token_total_supply_confidence: pending
                .token_total_supply_raw
                .map(|_| "observed".to_owned()),
            quote_mint: pending.quote_mint,
            creator: pending.creator,
            reserve_field_schema: Some(pending.reserve_field_schema),
            price_lamports_per_raw_token: price_lamports_per_raw,
            price_sol_per_token: Some(price_sol_per_token),
            reserve_price_source: Some("virtual_reserves".to_owned()),
            reserve_price_confidence: Some(confidence),
            price: price_sol_per_token,
            market_cap_quote_1b: Some(market_cap_quote_1b),
            market_cap_quote_total_supply: Some(pump_market_cap_quote_total_supply(
                price_sol_per_token,
                Decimal::from(PUMP_TOTAL_SUPPLY_UI),
            )),
            market_cap_source: Some("price_times_curve_economic_supply".to_owned()),
            market_cap_confidence: Some(confidence),
            market_cap_proxy: None,
            curve_complete_flag: Some(pending.complete),
            curve_progress_pct,
            curve_progress_source: Some("real_token_reserves_ui_minus_reserved".to_owned()),
            curve_progress_confidence: curve_progress_pct.map(|_| Decimal::ONE),
            curve_completion_pct: curve_progress_pct,
            quote_reserve_delta: None,
            token_reserve_delta: None,
            update_reason: "geyser_account_update".to_owned(),
            caused_by_signature: pending.transaction_signature,
            account_write_version: Some(pending.write_version),
        }),
    }
}

#[cfg(test)]
mod tests {
    use common::{
        Canonicality, EventMeta, EventPayload, EventSource, NormalizedEvent, PubkeyValue,
        QuoteAssetType, TokenCreatedEvent, TokenProgramType, TransactionStatus,
        config::LoadedConfig,
    };
    use ingest_geyser::{GeyserIngestService, TransactionTokenBalance, TransactionUpdate};
    use yellowstone_grpc_proto::prelude::{
        CompiledInstruction, Message, SlotStatus, SubscribeUpdate, SubscribeUpdateAccount,
        SubscribeUpdateAccountInfo, SubscribeUpdateDeshred, SubscribeUpdateDeshredTransaction,
        SubscribeUpdateDeshredTransactionInfo, SubscribeUpdatePing, SubscribeUpdateSlot,
        SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo, TokenBalance, Transaction,
        TransactionStatusMeta, UiTokenAmount, subscribe_update_deshred,
    };

    use super::*;

    fn loaded_config() -> LoadedConfig {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("config")
            .join("default.toml");
        let mut loaded = LoadedConfig::from_file(path).expect("config");
        loaded.config.metrics.enabled = false;
        loaded
    }

    fn update_wrap(
        update_oneof: yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof,
    ) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: Vec::new(),
            update_oneof: Some(update_oneof),
            created_at: None,
        }
    }

    fn holder_test_meta() -> EventMeta {
        let mut meta = EventMeta::new(EventSource::GeyserProcessed, Canonicality::Processed, 123);
        meta.signature = Some("holder-sig".to_owned());
        meta
    }

    fn test_meta(slot: u64, signature: &str) -> EventMeta {
        let mut meta = EventMeta::new(EventSource::GeyserProcessed, Canonicality::Processed, slot);
        meta.signature = Some(signature.to_owned());
        meta
    }

    fn test_pubkey(value: &str) -> PubkeyValue {
        PubkeyValue(value.to_owned())
    }

    fn test_create_event(mint: &str, slot: u64, status: TransactionStatus) -> NormalizedEvent {
        NormalizedEvent {
            meta: test_meta(slot, "create-sig"),
            payload: EventPayload::TokenCreated(TokenCreatedEvent {
                mint: test_pubkey(mint),
                token_program: TokenProgramType::SplToken,
                quote_mint: test_pubkey("So11111111111111111111111111111111111111112"),
                quote_asset_type: QuoteAssetType::NativeSol,
                creator_wallet: test_pubkey("creator"),
                payer: test_pubkey("payer"),
                bonding_curve_account: test_pubkey("curve"),
                associated_bonding_curve_account: Some(test_pubkey("associated_curve")),
                metadata_account: None,
                name: "Test".to_owned(),
                symbol: "TEST".to_owned(),
                uri: String::new(),
                create_instruction_variant: "create".to_owned(),
                initial_virtual_quote_reserves: None,
                initial_virtual_token_reserves: None,
                initial_real_quote_reserves: None,
                initial_real_token_reserves: None,
                initial_supply: None,
                creator_initial_buy: None,
                same_transaction_buys: 0,
                same_slot_buys: 0,
                fee_recipients: Vec::new(),
                raw_account_list: Vec::new(),
                launch_transaction_fingerprint: None,
                status,
            }),
        }
    }

    fn test_observed_transaction(slot: u64, signature: &str) -> NormalizedEvent {
        NormalizedEvent {
            meta: test_meta(slot, signature),
            payload: EventPayload::ObservedTransaction(common::ObservedTransactionEvent {
                signature_hint: Some(signature.to_owned()),
                slot_hint: Some(slot),
                entry_index: None,
                tx_position_estimate: None,
                signer: Some("signer".to_owned()),
                program_ids: Vec::new(),
                account_count: 0,
                instruction_count: 0,
                account_list_hash: Some("accounts".to_owned()),
                instruction_shape_hash: Some("shape".to_owned()),
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
                raw_packet_hash: "packet".to_owned(),
                first_seen_by_shred_ns: 0,
                decode_confidence: Decimal::ONE,
            }),
        }
    }

    fn token_balance(
        account_index: u32,
        mint: &str,
        owner: Option<&str>,
        amount: &str,
    ) -> TransactionTokenBalance {
        TransactionTokenBalance {
            account_index,
            mint: mint.to_owned(),
            owner: owner.map(str::to_owned),
            program_id: Some("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned()),
            amount: amount.to_owned(),
            decimals: 6,
        }
    }

    #[test]
    fn fresh_launch_retention_keeps_everything_when_unbounded() {
        let tracked_mints = HashSet::new();
        let tracked_launch_slots = HashSet::new();
        let unrelated = test_observed_transaction(100, "unrelated-sig");
        assert!(should_retain_fresh_launch_event(
            &unrelated,
            false,
            &tracked_mints,
            &tracked_launch_slots,
            &HashSet::new()
        ));
    }

    #[test]
    fn fresh_launch_retention_keeps_tracked_mints_and_launch_slot_only() {
        let tracked_mints = HashSet::from(["tracked-mint".to_owned()]);
        let tracked_launch_slots = HashSet::from([123_u64]);
        let tracked_related_signatures = HashSet::from(["buy-sig".to_owned()]);
        let create = test_create_event("tracked-mint", 123, TransactionStatus::Success);
        let same_slot_observed = test_observed_transaction(123, "same-slot");
        let early_buy_observed = test_observed_transaction(124, "buy-sig");
        let other_create = test_create_event("other-mint", 124, TransactionStatus::Success);
        let failed_create = test_create_event("tracked-mint", 123, TransactionStatus::Failed);
        let unrelated_observed = test_observed_transaction(124, "unrelated");

        assert!(should_retain_fresh_launch_event(
            &create,
            true,
            &tracked_mints,
            &tracked_launch_slots,
            &tracked_related_signatures
        ));
        assert!(should_retain_fresh_launch_event(
            &same_slot_observed,
            true,
            &tracked_mints,
            &tracked_launch_slots,
            &tracked_related_signatures
        ));
        assert!(should_retain_fresh_launch_event(
            &early_buy_observed,
            true,
            &tracked_mints,
            &tracked_launch_slots,
            &tracked_related_signatures
        ));
        assert!(!should_retain_fresh_launch_event(
            &other_create,
            true,
            &tracked_mints,
            &tracked_launch_slots,
            &tracked_related_signatures
        ));
        assert!(!should_retain_fresh_launch_event(
            &failed_create,
            true,
            &tracked_mints,
            &tracked_launch_slots,
            &tracked_related_signatures
        ));
        assert!(!should_retain_fresh_launch_event(
            &unrelated_observed,
            true,
            &tracked_mints,
            &tracked_launch_slots,
            &tracked_related_signatures
        ));
    }

    fn holder_test_update(
        account_keys: Vec<&str>,
        pre_token_balances: Vec<TransactionTokenBalance>,
        post_token_balances: Vec<TransactionTokenBalance>,
    ) -> TransactionUpdate {
        TransactionUpdate {
            slot: 123,
            signature: "holder-sig".to_owned(),
            transaction_index: Some(1),
            succeeded: true,
            error_code: None,
            account_keys: account_keys.into_iter().map(str::to_owned).collect(),
            instructions: Vec::new(),
            inner_instructions: Vec::new(),
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            pre_token_balances,
            post_token_balances,
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            compute_units_consumed: None,
            fee_lamports: 0,
        }
    }

    fn geyser_pump_candidate_update(loaded: &LoadedConfig) -> SubscribeUpdate {
        let pump_program = loaded
            .config
            .pump
            .program_ids
            .first()
            .cloned()
            .expect("pump program");
        let pump_program_bytes = bs58::decode(&pump_program)
            .into_vec()
            .expect("pump program bytes");
        update_wrap(
            yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Transaction(
                SubscribeUpdateTransaction {
                    slot: 9,
                    transaction: Some(SubscribeUpdateTransactionInfo {
                        signature: vec![1; 64],
                        is_vote: false,
                        transaction: Some(Transaction {
                            signatures: vec![vec![1; 64]],
                            message: Some(Message {
                                header: None,
                                account_keys: vec![pump_program_bytes, vec![3; 32]],
                                recent_blockhash: vec![0; 32],
                                instructions: vec![CompiledInstruction {
                                    program_id_index: 0,
                                    accounts: vec![0, 1],
                                    data: anchor_discriminator("global", "create").to_vec(),
                                }],
                                versioned: false,
                                address_table_lookups: Vec::new(),
                            }),
                        }),
                        meta: Some(TransactionStatusMeta {
                            err: None,
                            fee: 5000,
                            pre_balances: vec![100, 0],
                            post_balances: vec![90, 10],
                            inner_instructions: Vec::new(),
                            inner_instructions_none: false,
                            log_messages: Vec::new(),
                            log_messages_none: false,
                            pre_token_balances: Vec::new(),
                            post_token_balances: vec![TokenBalance {
                                account_index: 0,
                                mint: bs58::encode([3u8; 32]).into_string(),
                                ui_token_amount: Some(UiTokenAmount {
                                    ui_amount: 20.0,
                                    decimals: 6,
                                    amount: "20".to_owned(),
                                    ui_amount_string: "20".to_owned(),
                                }),
                                owner: bs58::encode([2u8; 32]).into_string(),
                                program_id: "".to_owned(),
                            }],
                            rewards: Vec::new(),
                            loaded_writable_addresses: Vec::new(),
                            loaded_readonly_addresses: Vec::new(),
                            return_data: None,
                            return_data_none: true,
                            compute_units_consumed: Some(100),
                            cost_units: Some(100),
                        }),
                        index: 0,
                    }),
                },
            ),
        )
    }

    fn geyser_pump_instruction_update(
        instruction_name: &str,
        mint: Option<&str>,
    ) -> SubscribeUpdate {
        let pump_program_bytes = bs58::decode(PUMP_PROGRAM_ID)
            .into_vec()
            .expect("pump program bytes");
        let account_key = vec![7; 32];
        let mut data = Vec::new();
        data.extend_from_slice(&anchor_discriminator("global", instruction_name));
        data.extend_from_slice(&[0; 8]);
        let token_balances = mint
            .map(|mint| {
                vec![TokenBalance {
                    account_index: 1,
                    mint: mint.to_owned(),
                    ui_token_amount: Some(UiTokenAmount {
                        ui_amount: 20.0,
                        decimals: 6,
                        amount: "20".to_owned(),
                        ui_amount_string: "20".to_owned(),
                    }),
                    owner: bs58::encode([2u8; 32]).into_string(),
                    program_id: "".to_owned(),
                }]
            })
            .unwrap_or_default();
        update_wrap(
            yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Transaction(
                SubscribeUpdateTransaction {
                    slot: 12,
                    transaction: Some(SubscribeUpdateTransactionInfo {
                        signature: vec![9; 64],
                        is_vote: false,
                        transaction: Some(Transaction {
                            signatures: vec![vec![9; 64]],
                            message: Some(Message {
                                header: None,
                                account_keys: vec![pump_program_bytes, account_key],
                                recent_blockhash: vec![0; 32],
                                instructions: vec![CompiledInstruction {
                                    program_id_index: 0,
                                    accounts: vec![0, 1],
                                    data,
                                }],
                                versioned: false,
                                address_table_lookups: Vec::new(),
                            }),
                        }),
                        meta: Some(TransactionStatusMeta {
                            err: None,
                            fee: 5000,
                            pre_balances: vec![100, 0],
                            post_balances: vec![90, 10],
                            inner_instructions: Vec::new(),
                            inner_instructions_none: false,
                            log_messages: Vec::new(),
                            log_messages_none: false,
                            pre_token_balances: Vec::new(),
                            post_token_balances: token_balances,
                            rewards: Vec::new(),
                            loaded_writable_addresses: Vec::new(),
                            loaded_readonly_addresses: Vec::new(),
                            return_data: None,
                            return_data_none: true,
                            compute_units_consumed: Some(100),
                            cost_units: Some(100),
                        }),
                        index: 0,
                    }),
                },
            ),
        )
    }

    fn geyser_token_balance_update(mint: &str, first_account_byte: u8) -> SubscribeUpdate {
        update_wrap(
            yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Transaction(
                SubscribeUpdateTransaction {
                    slot: 11,
                    transaction: Some(SubscribeUpdateTransactionInfo {
                        signature: vec![first_account_byte; 64],
                        is_vote: false,
                        transaction: Some(Transaction {
                            signatures: vec![vec![first_account_byte; 64]],
                            message: Some(Message {
                                header: None,
                                account_keys: vec![vec![first_account_byte; 32]],
                                recent_blockhash: vec![0; 32],
                                instructions: Vec::new(),
                                versioned: false,
                                address_table_lookups: Vec::new(),
                            }),
                        }),
                        meta: Some(TransactionStatusMeta {
                            err: None,
                            fee: 5000,
                            pre_balances: Vec::new(),
                            post_balances: Vec::new(),
                            inner_instructions: Vec::new(),
                            inner_instructions_none: false,
                            log_messages: Vec::new(),
                            log_messages_none: false,
                            pre_token_balances: Vec::new(),
                            post_token_balances: vec![TokenBalance {
                                account_index: 0,
                                mint: mint.to_owned(),
                                ui_token_amount: Some(UiTokenAmount {
                                    ui_amount: 1.0,
                                    decimals: 6,
                                    amount: "1".to_owned(),
                                    ui_amount_string: "1".to_owned(),
                                }),
                                owner: String::new(),
                                program_id: String::new(),
                            }],
                            rewards: Vec::new(),
                            loaded_writable_addresses: Vec::new(),
                            loaded_readonly_addresses: Vec::new(),
                            return_data: None,
                            return_data_none: true,
                            compute_units_consumed: None,
                            cost_units: None,
                        }),
                        index: 0,
                    }),
                },
            ),
        )
    }

    fn geyser_transaction_without_token_balances(first_account_byte: u8) -> SubscribeUpdate {
        update_wrap(
            yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Transaction(
                SubscribeUpdateTransaction {
                    slot: 11,
                    transaction: Some(SubscribeUpdateTransactionInfo {
                        signature: vec![first_account_byte; 64],
                        is_vote: false,
                        transaction: Some(Transaction {
                            signatures: vec![vec![first_account_byte; 64]],
                            message: Some(Message {
                                header: None,
                                account_keys: vec![vec![first_account_byte; 32]],
                                recent_blockhash: vec![0; 32],
                                instructions: Vec::new(),
                                versioned: false,
                                address_table_lookups: Vec::new(),
                            }),
                        }),
                        meta: Some(TransactionStatusMeta {
                            err: None,
                            fee: 5000,
                            pre_balances: Vec::new(),
                            post_balances: Vec::new(),
                            inner_instructions: Vec::new(),
                            inner_instructions_none: false,
                            log_messages: Vec::new(),
                            log_messages_none: false,
                            pre_token_balances: Vec::new(),
                            post_token_balances: Vec::new(),
                            rewards: Vec::new(),
                            loaded_writable_addresses: Vec::new(),
                            loaded_readonly_addresses: Vec::new(),
                            return_data: None,
                            return_data_none: true,
                            compute_units_consumed: None,
                            cost_units: None,
                        }),
                        index: 0,
                    }),
                },
            ),
        )
    }

    fn geyser_account_update(pubkey_byte: u8, owner_byte: u8) -> SubscribeUpdate {
        update_wrap(
            yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Account(
                SubscribeUpdateAccount {
                    slot: 12,
                    is_startup: false,
                    account: Some(SubscribeUpdateAccountInfo {
                        pubkey: vec![pubkey_byte; 32],
                        lamports: 1,
                        owner: vec![owner_byte; 32],
                        executable: false,
                        rent_epoch: 0,
                        data: Vec::new(),
                        write_version: 1,
                        txn_signature: None,
                    }),
                },
            ),
        )
    }

    fn geyser_token_account_update(pubkey_byte: u8) -> SubscribeUpdate {
        update_wrap(
            yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Account(
                SubscribeUpdateAccount {
                    slot: 12,
                    is_startup: false,
                    account: Some(SubscribeUpdateAccountInfo {
                        pubkey: vec![pubkey_byte; 32],
                        lamports: 1,
                        owner: bs58::decode(SPL_TOKEN_PROGRAM_ID)
                            .into_vec()
                            .expect("token owner"),
                        executable: false,
                        rent_epoch: 0,
                        data: vec![1; 165],
                        write_version: 1,
                        txn_signature: None,
                    }),
                },
            ),
        )
    }

    #[test]
    fn material_hunter_untracked_empty_account_update_is_reader_side_only() {
        let update = geyser_account_update(7, 8);
        assert_eq!(
            material_hunter_update_class(&update),
            "account_update_untracked"
        );
        assert!(!material_hunter_update_needs_worker(&update));
    }

    #[test]
    fn material_hunter_token_account_update_remains_worker_relevant() {
        let update = geyser_token_account_update(7);
        assert_eq!(
            material_hunter_update_class(&update),
            "token_account_update_untracked"
        );
        assert!(material_hunter_update_needs_worker(&update));
    }

    #[test]
    fn material_hunter_partition_router_same_mint_same_partition() {
        let mint = bs58::encode([9u8; 32]).into_string();
        let left = geyser_token_balance_update(&mint, 1);
        let right = geyser_token_balance_update(&mint, 2);
        let (left_partition, left_fallback, left_label) =
            material_hunter_partition_for_update(&left, 4);
        let (right_partition, right_fallback, right_label) =
            material_hunter_partition_for_update(&right, 4);
        assert_eq!(left_partition, right_partition);
        assert!(!left_fallback);
        assert!(!right_fallback);
        assert_eq!(left_label, format!("mint:{mint}"));
        assert_eq!(right_label, format!("mint:{mint}"));
    }

    #[test]
    fn material_hunter_token_account_to_mint_routing_is_deterministic_without_migration() {
        let mint = bs58::encode([9u8; 32]).into_string();
        let tx_update = geyser_token_balance_update(&mint, 7);
        let account_update = geyser_token_account_update(7);
        let mut token_account_to_mint = HashMap::<Vec<u8>, String>::new();
        let mut account_partition_pins = HashMap::<Vec<u8>, usize>::new();
        for (account, mapped_mint) in material_hunter_token_account_mint_mappings(&tx_update) {
            token_account_to_mint.insert(account, mapped_mint);
        }
        let (tx_partition, _, _) = material_hunter_partition_for_update_with_account_map(
            &tx_update,
            4,
            &token_account_to_mint,
            &mut account_partition_pins,
        );
        let (account_partition, account_fallback, account_label) =
            material_hunter_partition_for_update_with_account_map(
                &account_update,
                4,
                &token_account_to_mint,
                &mut account_partition_pins,
            );
        assert_eq!(tx_partition, account_partition);
        assert!(!account_fallback);
        assert_eq!(account_label, format!("mint:{mint}"));

        let unknown_account = geyser_token_account_update(8);
        let (first_partition, _, first_label) =
            material_hunter_partition_for_update_with_account_map(
                &unknown_account,
                4,
                &token_account_to_mint,
                &mut account_partition_pins,
            );
        token_account_to_mint.insert(vec![8; 32], mint);
        let (second_partition, _, second_label) =
            material_hunter_partition_for_update_with_account_map(
                &unknown_account,
                4,
                &token_account_to_mint,
                &mut account_partition_pins,
            );
        assert_eq!(first_partition, second_partition);
        assert!(first_label.starts_with("account:"));
        assert!(second_label.starts_with("account_pinned:"));
    }

    #[test]
    fn pump_trade_untracked_mint_is_cheap_counted_and_skipped() {
        let mint = bs58::encode([11u8; 32]).into_string();
        let update = geyser_pump_instruction_update("buy", Some(&mint));
        let prefilter =
            material_hunter_prefilter_pump_instruction(&update, &HashSet::new(), &HashSet::new())
                .expect("pump prefilter");
        assert_eq!(prefilter.update_class, "pump_trade_untracked_mint");
        assert_eq!(
            prefilter.decision,
            MaterialHunterPumpPrefilterDecision::SkipUntracked
        );
        assert_eq!(prefilter.mint.as_deref(), Some(mint.as_str()));
    }

    #[test]
    fn pump_trade_tombstoned_mint_is_cheap_counted_and_skipped() {
        let mint = bs58::encode([12u8; 32]).into_string();
        let update = geyser_pump_instruction_update("sell", Some(&mint));
        let mut tombstoned = HashSet::new();
        tombstoned.insert(mint);
        let prefilter =
            material_hunter_prefilter_pump_instruction(&update, &HashSet::new(), &tombstoned)
                .expect("pump prefilter");
        assert_eq!(prefilter.update_class, "pump_trade_tombstoned_mint");
        assert_eq!(
            prefilter.decision,
            MaterialHunterPumpPrefilterDecision::SkipTombstoned
        );
    }

    #[test]
    fn pump_trade_active_mint_is_deep_processed() {
        let mint = bs58::encode([13u8; 32]).into_string();
        let update = geyser_pump_instruction_update("buy", Some(&mint));
        let mut active = HashSet::new();
        active.insert(mint);
        let prefilter =
            material_hunter_prefilter_pump_instruction(&update, &active, &HashSet::new())
                .expect("pump prefilter");
        assert_eq!(prefilter.update_class, "pump_trade_active_mint");
        assert_eq!(
            prefilter.decision,
            MaterialHunterPumpPrefilterDecision::DeepProcess
        );
    }

    #[test]
    fn pump_token_created_remains_high_priority() {
        let mint = bs58::encode([14u8; 32]).into_string();
        let update = geyser_pump_instruction_update("create", Some(&mint));
        let prefilter =
            material_hunter_prefilter_pump_instruction(&update, &HashSet::new(), &HashSet::new())
                .expect("pump prefilter");
        assert_eq!(prefilter.update_class, "pump_token_created");
        assert_eq!(
            prefilter.decision,
            MaterialHunterPumpPrefilterDecision::DeepProcess
        );
    }

    #[test]
    fn pump_trade_unknown_mint_minimal_decode_does_not_panic() {
        let update = geyser_pump_instruction_update("buy", None);
        let prefilter =
            material_hunter_prefilter_pump_instruction(&update, &HashSet::new(), &HashSet::new())
                .expect("pump prefilter");
        assert_eq!(prefilter.update_class, "pump_trade_unknown_mint");
        assert_eq!(
            prefilter.decision,
            MaterialHunterPumpPrefilterDecision::SkipUnknownMint
        );
        assert!(prefilter.mint.is_none());
    }

    #[test]
    fn skipped_pump_trade_noise_does_not_trigger_worker_backpressure() {
        let mut stats = MaterialHunterReaderStats {
            worker_partitions: 4,
            ..MaterialHunterReaderStats::default()
        };
        stats.pump_trade_skipped_untracked_count = 10_000;
        stats.record_update_class_skipped("pump_trade_untracked_mint");
        let mut summary = MaterialHunterStreamSummary::default();
        apply_reader_stats_to_summary(&mut summary, &stats);
        assert_eq!(summary.pump_trade_skipped_untracked_count, 10_000);
        assert!(!summary.client_backpressure_detected);
        assert!(!summary.worker_backpressure_detected);
        assert_eq!(summary.partition_worker_lag_ms_max, 0);
    }

    #[test]
    fn transaction_mapping_hint_updates_token_account_to_mint_without_rich_processing() {
        let mint = bs58::encode([31u8; 32]).into_string();
        let update = geyser_token_balance_update(&mint, 31);
        let mappings = material_hunter_token_account_mint_mappings(&update);
        let token_account_to_mint = mappings.into_iter().collect::<HashMap<_, _>>();
        let account_partition_pins = HashMap::<Vec<u8>, usize>::new();
        let prefilter = material_hunter_prefilter_transaction_update(
            &update,
            &HashSet::new(),
            &HashSet::new(),
            &token_account_to_mint,
            &account_partition_pins,
            false,
        )
        .expect("transaction prefilter");
        assert_eq!(prefilter.update_class, "transaction_mapping_hint_only");
        assert_eq!(
            prefilter.decision,
            MaterialHunterTransactionPrefilterDecision::SkipMappingHintOnly
        );
    }

    #[test]
    fn transaction_active_mint_is_deep_processed_or_delta_accumulated() {
        let mint = bs58::encode([32u8; 32]).into_string();
        let update = geyser_token_balance_update(&mint, 32);
        let mut active = HashSet::new();
        active.insert(mint.clone());
        let token_account_to_mint = material_hunter_token_account_mint_mappings(&update)
            .into_iter()
            .collect::<HashMap<_, _>>();
        let prefilter = material_hunter_prefilter_transaction_update(
            &update,
            &active,
            &HashSet::new(),
            &token_account_to_mint,
            &HashMap::new(),
            false,
        )
        .expect("transaction prefilter");
        assert_eq!(prefilter.update_class, "transaction_active_mint");
        assert_eq!(
            prefilter.decision,
            MaterialHunterTransactionPrefilterDecision::DeepProcess
        );
        assert_eq!(prefilter.mint.as_deref(), Some(mint.as_str()));
    }

    fn test_active_mint_pressure_config() -> MaterialHunterActiveMintPressureConfig {
        MaterialHunterActiveMintPressureConfig {
            max_queued_updates_per_mint: 4,
            max_updates_per_second: 4,
            max_deep_updates_per_checkpoint: 2,
            noisy_degrade_enabled: true,
            noisy_degrade_reason: "noisy_active_mint_backpressure".to_owned(),
            coalesce_window: StdDuration::from_millis(250),
            delta_flush_interval: StdDuration::from_millis(5_000),
            partition_soft_queue_threshold_ratio: 0.75,
        }
    }

    #[test]
    fn active_mint_transaction_updates_are_coalesced() {
        let mut state = MaterialHunterActiveMintPressureState::default();
        let config = test_active_mint_pressure_config();
        let now = tokio::time::Instant::now();
        assert_eq!(
            material_hunter_active_mint_pressure_decision(
                &mut state, "mint-a", now, 0, 64, &config
            ),
            MaterialHunterActiveMintPressureDecision::DeepProcess
        );
        assert_eq!(
            material_hunter_active_mint_pressure_decision(
                &mut state,
                "mint-a",
                now + StdDuration::from_millis(10),
                0,
                64,
                &config
            ),
            MaterialHunterActiveMintPressureDecision::Coalesce
        );
    }

    #[test]
    fn active_mint_noisy_budget_exceeded_degrades_mint_audit_only() {
        let mut state = MaterialHunterActiveMintPressureState::default();
        let config = test_active_mint_pressure_config();
        let now = tokio::time::Instant::now();
        for offset in [0, 300] {
            assert_eq!(
                material_hunter_active_mint_pressure_decision(
                    &mut state,
                    "mint-a",
                    now + StdDuration::from_millis(offset),
                    0,
                    64,
                    &config
                ),
                MaterialHunterActiveMintPressureDecision::DeepProcess
            );
        }
        assert_eq!(
            material_hunter_active_mint_pressure_decision(
                &mut state,
                "mint-a",
                now + StdDuration::from_millis(600),
                0,
                64,
                &config
            ),
            MaterialHunterActiveMintPressureDecision::Degrade {
                reason: "active_mint_processing_budget_exceeded".to_owned(),
                queue_pressure: false,
            }
        );
    }

    #[test]
    fn partition_queue_pressure_preempts_dominant_mint_before_full() {
        let mut state = MaterialHunterActiveMintPressureState::default();
        let config = test_active_mint_pressure_config();
        let now = tokio::time::Instant::now();
        assert_eq!(
            material_hunter_active_mint_pressure_decision(
                &mut state, "mint-a", now, 47, 64, &config
            ),
            MaterialHunterActiveMintPressureDecision::DeepProcess
        );
        assert_eq!(
            material_hunter_active_mint_pressure_decision(
                &mut state,
                "mint-a",
                now + StdDuration::from_millis(10),
                48,
                64,
                &config
            ),
            MaterialHunterActiveMintPressureDecision::Degrade {
                reason: "noisy_active_mint_backpressure".to_owned(),
                queue_pressure: true,
            }
        );
    }

    #[test]
    fn active_mint_pressure_telemetry_surfaces_degraded_mints() {
        let mut stats = MaterialHunterReaderStats {
            active_mint_transaction_update_count: 10,
            active_mint_transaction_coalesced_count: 6,
            active_mint_transaction_degraded_count: 1,
            active_mint_transaction_queue_pressure_count: 1,
            partition_queue_pressure_preempted_count: 1,
            partition_queue_pressure_dominant_mint: Some("mint-a".to_owned()),
            partition_queue_pressure_dominant_mint_update_count: 7,
            partition_queue_pressure_degraded_mint: Some("mint-a".to_owned()),
            partition_queue_pressure_preempted_before_full: true,
            ..MaterialHunterReaderStats::default()
        };
        stats.degraded_active_mints.insert("mint-a".to_owned());
        MaterialHunterReaderStats::increment_top_count(
            &mut stats.top_active_mint_coalesced_counts,
            "mint-a".to_owned(),
        );
        MaterialHunterReaderStats::increment_top_count(
            &mut stats.top_active_mint_queue_pressure_counts,
            "mint-a".to_owned(),
        );
        let mut summary = MaterialHunterStreamSummary::default();
        apply_reader_stats_to_summary(&mut summary, &stats);
        assert_eq!(summary.active_mint_transaction_degraded_count, 1);
        assert_eq!(summary.degraded_active_mint_count, 1);
        assert_eq!(summary.degraded_active_mints, vec!["mint-a".to_owned()]);
        assert_eq!(summary.partition_queue_pressure_preempted_count, 1);
        assert_eq!(
            summary.partition_queue_pressure_degraded_mint.as_deref(),
            Some("mint-a")
        );
        assert!(summary.partition_queue_pressure_preempted_before_full);
        assert_eq!(summary.top_active_mints_by_queue_pressure[0].key, "mint-a");
    }

    #[test]
    fn transaction_token_created_high_priority_not_skipped() {
        let loaded = loaded_config();
        let update = geyser_pump_candidate_update(&loaded);
        let prefilter = material_hunter_prefilter_transaction_update(
            &update,
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
            &HashMap::new(),
            false,
        )
        .expect("transaction prefilter");
        assert_eq!(prefilter.update_class, "transaction_token_created");
        assert_eq!(
            prefilter.decision,
            MaterialHunterTransactionPrefilterDecision::DeepProcess
        );
    }

    #[test]
    fn transaction_tombstoned_mint_is_skipped() {
        let mint = bs58::encode([33u8; 32]).into_string();
        let update = geyser_token_balance_update(&mint, 33);
        let mut tombstoned = HashSet::new();
        tombstoned.insert(mint.clone());
        let token_account_to_mint = material_hunter_token_account_mint_mappings(&update)
            .into_iter()
            .collect::<HashMap<_, _>>();
        let prefilter = material_hunter_prefilter_transaction_update(
            &update,
            &HashSet::new(),
            &tombstoned,
            &token_account_to_mint,
            &HashMap::new(),
            false,
        )
        .expect("transaction prefilter");
        assert_eq!(prefilter.update_class, "transaction_tombstoned_mint");
        assert_eq!(
            prefilter.decision,
            MaterialHunterTransactionPrefilterDecision::SkipTombstoned
        );
    }

    #[test]
    fn transaction_duplicate_signature_is_skipped() {
        let update = geyser_token_balance_update(&bs58::encode([34u8; 32]).into_string(), 34);
        let signature = material_hunter_transaction_signature_hint(&update).expect("signature");
        let mut seen = HashSet::new();
        let mut lru = VecDeque::new();
        assert!(!material_hunter_signature_seen_or_insert(
            &mut seen, &mut lru, &signature, 4096,
        ));
        assert!(material_hunter_signature_seen_or_insert(
            &mut seen, &mut lru, &signature, 4096,
        ));
        let prefilter = material_hunter_prefilter_transaction_update(
            &update,
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
            &HashMap::new(),
            true,
        )
        .expect("transaction prefilter");
        assert_eq!(prefilter.update_class, "transaction_duplicate_signature");
        assert_eq!(
            prefilter.decision,
            MaterialHunterTransactionPrefilterDecision::SkipDuplicateSignature
        );
    }

    #[test]
    fn transaction_account_pinned_unknown_is_skipped_unless_active_mapping_exists() {
        let update = geyser_transaction_without_token_balances(35);
        let mut pins = HashMap::new();
        pins.insert(vec![35; 32], 2);
        let prefilter = material_hunter_prefilter_transaction_update(
            &update,
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
            &pins,
            false,
        )
        .expect("transaction prefilter");
        assert_eq!(prefilter.update_class, "transaction_account_pinned_unknown");
        assert_eq!(
            prefilter.decision,
            MaterialHunterTransactionPrefilterDecision::SkipAccountPinnedUnknown
        );

        let mint = bs58::encode([35u8; 32]).into_string();
        let active = HashSet::from([mint.clone()]);
        let mapped = HashMap::from([(vec![35; 32], mint)]);
        let prefilter = material_hunter_prefilter_transaction_update(
            &update,
            &active,
            &HashSet::new(),
            &mapped,
            &pins,
            false,
        )
        .expect("transaction prefilter");
        assert_eq!(prefilter.update_class, "transaction_active_account");
        assert_eq!(
            prefilter.decision,
            MaterialHunterTransactionPrefilterDecision::DeepProcess
        );
    }

    #[test]
    fn malformed_transaction_update_does_not_panic() {
        let update = update_wrap(
            yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Transaction(
                SubscribeUpdateTransaction {
                    slot: 1,
                    transaction: None,
                },
            ),
        );
        let prefilter = material_hunter_prefilter_transaction_update(
            &update,
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
            &HashMap::new(),
            false,
        )
        .expect("transaction prefilter");
        assert_eq!(prefilter.update_class, "transaction_malformed_or_unknown");
        assert_eq!(
            prefilter.decision,
            MaterialHunterTransactionPrefilterDecision::SkipMalformedOrUnknown
        );
    }

    #[test]
    fn skipped_transaction_noise_does_not_trigger_worker_backpressure() {
        let mut stats = MaterialHunterReaderStats {
            worker_partitions: 4,
            transaction_mapping_hint_only_count: 2_000,
            transaction_account_pinned_unknown_count: 500,
            transaction_other_untracked_skipped_count: 500,
            ..MaterialHunterReaderStats::default()
        };
        stats.record_update_class_skipped("transaction_mapping_hint_only");
        stats.record_update_class_skipped("transaction_account_pinned_unknown");
        let mut summary = MaterialHunterStreamSummary::default();
        apply_reader_stats_to_summary(&mut summary, &stats);
        assert_eq!(summary.transaction_mapping_hint_only_count, 2_000);
        assert_eq!(summary.transaction_account_pinned_unknown_count, 500);
        assert!(!summary.client_backpressure_detected);
        assert!(!summary.worker_backpressure_detected);
        assert_eq!(summary.partition_worker_lag_ms_max, 0);
    }

    #[test]
    fn transaction_hot_class_telemetry_reports_counts_durations_and_trigger_fields() {
        let mut stats = MaterialHunterReaderStats {
            worker_partitions: 4,
            transaction_deep_processed_count: 12,
            transaction_mapping_hint_only_count: 40,
            account_pinned_update_count: 7,
            backpressure_transaction_class: Some("transaction_active_mint".to_owned()),
            backpressure_transaction_signature: Some("sig".to_owned()),
            backpressure_transaction_mint: Some("mint".to_owned()),
            backpressure_transaction_account: Some("account".to_owned()),
            backpressure_deep_transaction_count_at_trigger: 12,
            backpressure_skipped_transaction_count_at_trigger: 40,
            backpressure_account_pinned_count_at_trigger: 7,
            ..MaterialHunterReaderStats::default()
        };
        stats.record_transaction_prefilter_duration(2);
        stats.record_transaction_deep_duration(5);
        MaterialHunterReaderStats::increment_top_count(
            &mut stats.top_active_mint_transaction_counts,
            "mint".to_owned(),
        );
        MaterialHunterReaderStats::max_top_value(
            &mut stats.top_active_mint_transaction_lag,
            "mint".to_owned(),
            99,
        );
        let mut summary = MaterialHunterStreamSummary::default();
        apply_reader_stats_to_summary(&mut summary, &stats);
        assert_eq!(summary.transaction_deep_processed_count, 12);
        assert_eq!(summary.transaction_mapping_hint_only_count, 40);
        assert_eq!(summary.transaction_prefilter_duration_ms_max, 2);
        assert_eq!(summary.transaction_deep_process_duration_ms_max, 5);
        assert_eq!(
            summary.backpressure_transaction_class.as_deref(),
            Some("transaction_active_mint")
        );
        assert_eq!(summary.backpressure_deep_transaction_count_at_trigger, 12);
        assert_eq!(
            summary.backpressure_skipped_transaction_count_at_trigger,
            40
        );
        assert_eq!(summary.backpressure_account_pinned_count_at_trigger, 7);
        assert_eq!(summary.top_active_mints_by_transaction_lag[0].count, 99);
    }

    #[test]
    fn material_hunter_hot_partition_telemetry_reports_triggering_partition_and_class() {
        let mut stats = MaterialHunterReaderStats {
            worker_partitions: 4,
            partition_worker_lag_ms_by_partition: vec![vec![10], vec![20, 30], vec![42], vec![]],
            partition_queue_depth_max: vec![1, 9, 2, 0],
            partition_backlog_oldest_update_age_ms_by_partition: vec![10, 30, 42, 0],
            partition_batch_size_max_by_partition: vec![2, 8, 1, 0],
            partition_backpressure_trigger_partition: Some(1),
            partition_backpressure_trigger_reason: Some("worker_lag_threshold_exceeded".to_owned()),
            backpressure_threshold_ms: 25,
            backpressure_observed_lag_ms: 30,
            backpressure_update_class: Some("transaction_update_relevant".to_owned()),
            backpressure_partition_id: Some(1),
            backpressure_segment_id: Some(1),
            partition_started_at: Some(tokio::time::Instant::now()),
            ..MaterialHunterReaderStats::default()
        };
        stats.record_update_class_route("transaction_update_relevant", 1);
        stats.record_update_class_worker("transaction_update_relevant", 1, 30, 3);
        MaterialHunterReaderStats::increment_top_count(
            &mut stats.top_partition_key_counts,
            "mint:hot".to_owned(),
        );
        let mut summary = MaterialHunterStreamSummary::default();
        apply_reader_stats_to_summary(&mut summary, &stats);
        assert_eq!(
            summary.partition_worker_lag_ms_max_by_partition,
            vec![10, 30, 42, 0]
        );
        assert_eq!(summary.partition_worker_lag_ms_p95_by_partition[1], 20);
        assert_eq!(summary.partition_backpressure_trigger_partition, Some(1));
        assert_eq!(
            summary.partition_backpressure_trigger_reason.as_deref(),
            Some("worker_lag_threshold_exceeded")
        );
        assert_eq!(
            summary.backpressure_update_class.as_deref(),
            Some("transaction_update_relevant")
        );
        assert_eq!(summary.backpressure_observed_lag_ms, 30);
        assert_eq!(
            summary.top_partition_keys_by_update_count[0].key,
            "mint:hot"
        );
        assert_eq!(
            summary.update_class_telemetry[0].class_name,
            "transaction_update_relevant"
        );
    }

    #[derive(Debug, Clone)]
    struct FailingGeyserConnector {
        status: Status,
    }

    #[async_trait]
    impl GeyserStreamConnector for FailingGeyserConnector {
        async fn connect_and_subscribe(
            &self,
            _config: &common::GeyserConfig,
            _request: SubscribeRequest,
        ) -> Result<SubscribeUpdateStream> {
            Err(anyhow!(self.status.clone()))
        }
    }

    #[test]
    fn missing_endpoint_fails_clearly() {
        let loaded = loaded_config();
        let result = resolved_geyser_endpoint(&loaded.config.geyser);
        assert!(result.is_err());
    }

    #[test]
    fn missing_required_auth_fails_clearly() {
        let mut loaded = loaded_config();
        loaded.config.geyser.auth_required = true;
        loaded.config.geyser.auth_token_env = "MISSING_TOKEN_ENV".to_owned();
        let result = resolved_geyser_metadata(&loaded.config.geyser);
        assert!(result.is_err());
    }

    #[test]
    fn holder_balance_events_are_keyed_by_token_account_not_owner() {
        let update = holder_test_update(
            vec![
                "payer",
                "owner-a-token-account-1",
                "owner-a-token-account-2",
            ],
            vec![
                token_balance(1, "mint-a", Some("owner-a"), "100"),
                token_balance(2, "mint-a", Some("owner-a"), "5"),
            ],
            vec![
                token_balance(1, "mint-a", Some("owner-a"), "110"),
                token_balance(2, "mint-a", Some("owner-a"), "7"),
            ],
        );

        let events = holder_balance_events(&holder_test_meta(), &update);
        let holder_updates = events
            .iter()
            .filter_map(|event| match &event.payload {
                EventPayload::HolderBalanceUpdate(update) => Some(update),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(holder_updates.len(), 2);
        let first = holder_updates
            .iter()
            .find(|event| event.token_account.0 == "owner-a-token-account-1")
            .expect("first token account update");
        assert_eq!(first.old_balance, Some(Decimal::new(100, 0)));
        assert_eq!(first.new_balance, Decimal::new(110, 0));
        assert_eq!(first.delta, Decimal::new(10, 0));

        let second = holder_updates
            .iter()
            .find(|event| event.token_account.0 == "owner-a-token-account-2")
            .expect("second token account update");
        assert_eq!(second.old_balance, Some(Decimal::new(5, 0)));
        assert_eq!(second.new_balance, Decimal::new(7, 0));
        assert_eq!(second.delta, Decimal::new(2, 0));
    }

    #[test]
    fn holder_balance_events_emit_closed_token_account_zero_snapshot() {
        let update = holder_test_update(
            vec!["payer", "owner-a-token-account-1"],
            vec![token_balance(1, "mint-a", Some("owner-a"), "100")],
            Vec::new(),
        );

        let events = holder_balance_events(&holder_test_meta(), &update);
        let holder_update = events
            .iter()
            .find_map(|event| match &event.payload {
                EventPayload::HolderBalanceUpdate(update) => Some(update),
                _ => None,
            })
            .expect("closed token account update");

        assert_eq!(holder_update.owner_wallet.0, "owner-a");
        assert_eq!(holder_update.token_account.0, "owner-a-token-account-1");
        assert_eq!(holder_update.old_balance, Some(Decimal::new(100, 0)));
        assert_eq!(holder_update.new_balance, Decimal::ZERO);
        assert_eq!(holder_update.delta, Decimal::new(-100, 0));
        assert_eq!(
            holder_update.update_reason,
            "geyser_token_balance_closed_or_zeroed"
        );
    }

    #[test]
    fn curve_update_before_token_created_is_buffered_and_flushed() {
        let loaded = loaded_config();
        let mut normalizer = GeyserEventNormalizer::from_loaded(&loaded).expect("normalizer");
        normalizer
            .pending_curve_updates_by_curve_pubkey
            .entry("curve-a".to_owned())
            .or_default()
            .push(PendingCurveUpdate {
                meta: EventMeta::new(EventSource::GeyserProcessed, Canonicality::Processed, 7),
                curve_pubkey: "curve-a".to_owned(),
                virtual_quote: Decimal::from(30_000_000_000u64),
                virtual_token: Decimal::from(1_000_000_000_000_000u64),
                real_quote: Decimal::ZERO,
                real_token: Decimal::from(793_100_000_000_000u64),
                token_total_supply_raw: Some(Decimal::from(1_000_000_000_000_000u64)),
                creator: None,
                quote_mint: None,
                reserve_field_schema: "quote_reserves_v2".to_owned(),
                complete: false,
                transaction_signature: Some("curve-sig".to_owned()),
                write_version: 12,
            });

        let flushed = normalizer.flush_pending_curve_updates("curve-a", "mint-a");

        assert_eq!(flushed.len(), 1);
        assert!(
            !normalizer
                .pending_curve_updates_by_curve_pubkey
                .contains_key("curve-a")
        );
        match &flushed[0].payload {
            EventPayload::BondingCurveUpdate(update) => {
                assert_eq!(update.mint.0, "mint-a");
                assert_eq!(
                    update.reserve_price_source.as_deref(),
                    Some("virtual_reserves")
                );
                assert_eq!(
                    update.market_cap_source.as_deref(),
                    Some("price_times_curve_economic_supply")
                );
                assert_eq!(
                    update.curve_progress_source.as_deref(),
                    Some("real_token_reserves_ui_minus_reserved")
                );
                assert_eq!(update.account_write_version, Some(12));
            }
            other => panic!("unexpected event payload: {other:?}"),
        }
    }

    #[test]
    fn proto_transaction_normalizes_into_pump_events() {
        let loaded = loaded_config();
        let mut normalizer = GeyserEventNormalizer::from_loaded(&loaded).expect("normalizer");
        let mut service = GeyserIngestService::new(loaded.config.geyser.clone());
        let create = crate::fixtures::build_fixture_scenario(&crate::fixtures::spec(
            crate::fixtures::FixtureScenarioKind::CleanOrganicLaunch,
        ))
        .canonical_events
        .into_iter()
        .find(|event| matches!(event.payload, EventPayload::TokenCreated(_)))
        .expect("fixture create");
        if let EventPayload::TokenCreated(payload) = create.payload {
            normalizer
                .curve_to_mint
                .insert(payload.bonding_curve_account.0, payload.mint.0.clone());
            normalizer
                .mint_to_creator
                .insert(payload.mint.0.clone(), payload.creator_wallet.0);
        }
        let mut data = Vec::new();
        data.extend_from_slice(&idl::anchor_discriminator("global", "buy"));
        data.extend_from_slice(&10u64.to_le_bytes());
        data.extend_from_slice(&20u64.to_le_bytes());
        let tx_update = SubscribeUpdateTransaction {
            slot: 9,
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: vec![1; 64],
                is_vote: false,
                transaction: Some(Transaction {
                    signatures: vec![vec![1; 64]],
                    message: Some(Message {
                        header: None,
                        account_keys: vec![vec![2; 32], vec![3; 32]],
                        recent_blockhash: vec![0; 32],
                        instructions: vec![CompiledInstruction {
                            program_id_index: 0,
                            accounts: vec![0, 1],
                            data,
                        }],
                        versioned: false,
                        address_table_lookups: Vec::new(),
                    }),
                }),
                meta: Some(TransactionStatusMeta {
                    err: None,
                    fee: 5000,
                    pre_balances: vec![100, 0],
                    post_balances: vec![90, 10],
                    inner_instructions: Vec::new(),
                    inner_instructions_none: false,
                    log_messages: Vec::new(),
                    log_messages_none: false,
                    pre_token_balances: Vec::new(),
                    post_token_balances: vec![TokenBalance {
                        account_index: 0,
                        mint: bs58::encode([3u8; 32]).into_string(),
                        ui_token_amount: Some(UiTokenAmount {
                            ui_amount: 20.0,
                            decimals: 6,
                            amount: "20".to_owned(),
                            ui_amount_string: "20".to_owned(),
                        }),
                        owner: bs58::encode([2u8; 32]).into_string(),
                        program_id: "".to_owned(),
                    }],
                    rewards: Vec::new(),
                    loaded_writable_addresses: Vec::new(),
                    loaded_readonly_addresses: Vec::new(),
                    return_data: None,
                    return_data_none: true,
                    compute_units_consumed: Some(100),
                    cost_units: Some(100),
                }),
                index: 0,
            }),
        };
        let outputs = service.process_subscribe_update(
            update_wrap(
                yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Transaction(
                    tx_update,
                ),
            ),
            1,
        );
        let normalized = outputs
            .into_iter()
            .flat_map(|output| normalizer.normalize_output(output))
            .collect::<Vec<_>>();
        assert!(
            normalized
                .iter()
                .any(|event| matches!(event.payload, EventPayload::ObservedTransaction(_)))
        );
    }

    #[test]
    fn slot_update_statuses_are_processed() {
        let loaded = loaded_config();
        let mut service = GeyserIngestService::new(loaded.config.geyser.clone());
        let outputs = service.process_subscribe_update(
            update_wrap(
                yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Slot(
                    SubscribeUpdateSlot {
                        slot: 42,
                        parent: Some(41),
                        status: SlotStatus::SlotConfirmed as i32,
                        dead_error: None,
                    },
                ),
            ),
            1,
        );
        assert!(
            outputs
                .iter()
                .any(|output| matches!(output, IngestOutput::Slot { .. }))
        );
    }

    #[test]
    fn unknown_update_variants_do_not_panic() {
        let loaded = loaded_config();
        let mut service = GeyserIngestService::new(loaded.config.geyser.clone());
        let outputs = service.process_subscribe_update(
            update_wrap(
                yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Ping(
                    SubscribeUpdatePing {},
                ),
            ),
            1,
        );
        assert!(outputs.is_empty());
    }

    #[test]
    fn deshred_capability_reports_supported_proto() {
        let loaded = loaded_config();
        let capability = inspect_deshred_capability(&loaded, None, None);
        assert!(capability.supported_by_proto);
        assert!(capability.supported_by_client);
        assert!(capability.exposes_signature);
        assert!(capability.exposes_loaded_addresses);
    }

    #[test]
    fn deshred_update_normalizes_as_tentative_observed_transaction() {
        let loaded = loaded_config();
        let mut normalizer = GeyserEventNormalizer::from_loaded(&loaded).expect("normalizer");
        let update = test_deshred_update();
        let normalized = normalized_events_from_deshred_update(&mut normalizer, update, 55);
        assert!(normalized.iter().any(|event| {
            matches!(
                &event.payload,
                EventPayload::ObservedTransaction(tx)
                    if event.meta.source == common::EventSource::DeshredTentative
                        && event.meta.canonicality == common::Canonicality::Tentative
                        && tx.signature_hint.is_some()
            )
        }));
    }

    fn test_deshred_update() -> SubscribeUpdateDeshred {
        SubscribeUpdateDeshred {
            filters: Vec::new(),
            created_at: None,
            update_oneof: Some(subscribe_update_deshred::UpdateOneof::DeshredTransaction(
                SubscribeUpdateDeshredTransaction {
                    slot: 77,
                    transaction: Some(SubscribeUpdateDeshredTransactionInfo {
                        signature: vec![7; 64],
                        is_vote: false,
                        transaction: Some(Transaction {
                            signatures: vec![vec![7; 64]],
                            message: Some(Message {
                                header: None,
                                account_keys: vec![vec![2; 32], vec![3; 32]],
                                recent_blockhash: vec![0; 32],
                                instructions: vec![CompiledInstruction {
                                    program_id_index: 0,
                                    accounts: vec![0, 1],
                                    data: Vec::new(),
                                }],
                                versioned: false,
                                address_table_lookups: Vec::new(),
                            }),
                        }),
                        loaded_writable_addresses: vec![vec![4; 32]],
                        loaded_readonly_addresses: vec![vec![5; 32]],
                        completed_data_set_starting_shred_index: 10,
                        completed_data_set_ending_shred_index_exclusive: 12,
                    }),
                },
            )),
        }
    }

    #[tokio::test]
    async fn smoke_deshred_missing_endpoint_reports_not_attempted() {
        let loaded = loaded_config();
        let summary = smoke_deshred_provider_with_connector(
            &loaded,
            DeshredProviderSmokeOptions {
                duration_seconds: 1,
                ..DeshredProviderSmokeOptions::default()
            },
            Arc::new(MockDeshredConnector::default()),
        )
        .await
        .expect("summary");
        assert_eq!(summary.provider_status, "not_attempted_missing_endpoint");
    }

    #[tokio::test]
    async fn smoke_deshred_zero_updates_reports_zero_updates() {
        let mut loaded = loaded_config();
        if let Some(deshred) = loaded.config.ingest.deshred.as_mut() {
            deshred.endpoint = "http://example.invalid:10000".to_owned();
        }
        let connector = MockDeshredConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![MockDeshredConnectorBatch {
                updates: Vec::new(),
            }])),
        };
        let summary = smoke_deshred_provider_with_connector(
            &loaded,
            DeshredProviderSmokeOptions {
                duration_seconds: 1,
                max_updates: Some(1),
                ..DeshredProviderSmokeOptions::default()
            },
            Arc::new(connector),
        )
        .await
        .expect("summary");
        assert_eq!(summary.provider_status, "zero_updates");
    }

    #[tokio::test]
    async fn smoke_deshred_unimplemented_reports_clearly() {
        let mut loaded = loaded_config();
        if let Some(deshred) = loaded.config.ingest.deshred.as_mut() {
            deshred.endpoint = "http://example.invalid:10000".to_owned();
        }
        let connector = MockDeshredConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![MockDeshredConnectorBatch {
                updates: vec![Err(Status::unimplemented("no subscribe_deshred"))],
            }])),
        };
        let summary = smoke_deshred_provider_with_connector(
            &loaded,
            DeshredProviderSmokeOptions {
                duration_seconds: 1,
                max_updates: Some(1),
                ..DeshredProviderSmokeOptions::default()
            },
            Arc::new(connector),
        )
        .await
        .expect("summary");
        assert_eq!(summary.provider_status, "unimplemented");
    }

    #[tokio::test]
    async fn smoke_deshred_auth_rejected_reports_clearly() {
        let mut loaded = loaded_config();
        if let Some(deshred) = loaded.config.ingest.deshred.as_mut() {
            deshred.endpoint = "http://example.invalid:10000".to_owned();
        }
        let connector = MockDeshredConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![MockDeshredConnectorBatch {
                updates: vec![Err(Status::unauthenticated("bad token"))],
            }])),
        };
        let summary = smoke_deshred_provider_with_connector(
            &loaded,
            DeshredProviderSmokeOptions {
                duration_seconds: 1,
                max_updates: Some(1),
                ..DeshredProviderSmokeOptions::default()
            },
            Arc::new(connector),
        )
        .await
        .expect("summary");
        assert_eq!(summary.provider_status, "auth_rejected");
    }

    #[tokio::test]
    async fn smoke_deshred_updates_produce_counts() {
        let mut loaded = loaded_config();
        if let Some(deshred) = loaded.config.ingest.deshred.as_mut() {
            deshred.endpoint = "http://example.invalid:10000".to_owned();
        }
        let connector = MockDeshredConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![MockDeshredConnectorBatch {
                updates: vec![Ok(test_deshred_update())],
            }])),
        };
        let summary = smoke_deshred_provider_with_connector(
            &loaded,
            DeshredProviderSmokeOptions {
                duration_seconds: 1,
                max_updates: Some(1),
                ..DeshredProviderSmokeOptions::default()
            },
            Arc::new(connector),
        )
        .await
        .expect("summary");
        assert_eq!(summary.provider_status, "updates_received");
        assert!(summary.updates_received > 0);
        assert!(summary.tentative_transactions_decoded > 0);
    }

    #[tokio::test]
    async fn smoke_geyser_missing_endpoint_reports_not_attempted() {
        let loaded = loaded_config();
        let summary = smoke_geyser_provider_with_connector(
            &loaded,
            GeyserProviderSmokeOptions {
                duration_seconds: 1,
                ..GeyserProviderSmokeOptions::default()
            },
            Arc::new(MockGeyserConnector::default()),
        )
        .await
        .expect("summary");
        assert_eq!(summary.provider_status, "not_attempted_missing_endpoint");
    }

    #[tokio::test]
    async fn smoke_geyser_zero_updates_reports_clearly() {
        let mut loaded = loaded_config();
        loaded.config.geyser.endpoint = "http://example.invalid:10000".to_owned();
        let connector = MockGeyserConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![MockConnectorBatch {
                updates: Vec::new(),
            }])),
        };
        let summary = smoke_geyser_provider_with_connector(
            &loaded,
            GeyserProviderSmokeOptions {
                duration_seconds: 1,
                max_updates: Some(1),
                ..GeyserProviderSmokeOptions::default()
            },
            Arc::new(connector),
        )
        .await
        .expect("summary");
        assert_eq!(summary.provider_status, "connected_zero_updates");
        assert!(summary.connected);
    }

    #[tokio::test]
    async fn smoke_geyser_auth_rejected_reports_clearly() {
        let mut loaded = loaded_config();
        loaded.config.geyser.endpoint = "http://example.invalid:10000".to_owned();
        let summary = smoke_geyser_provider_with_connector(
            &loaded,
            GeyserProviderSmokeOptions {
                duration_seconds: 1,
                ..GeyserProviderSmokeOptions::default()
            },
            Arc::new(FailingGeyserConnector {
                status: Status::unauthenticated("bad token"),
            }),
        )
        .await
        .expect("summary");
        assert_eq!(summary.provider_status, "auth_rejected");
    }

    #[tokio::test]
    async fn smoke_geyser_updates_produce_counts() {
        let mut loaded = loaded_config();
        loaded.config.geyser.endpoint = "http://example.invalid:10000".to_owned();
        let connector = MockGeyserConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![MockConnectorBatch {
                updates: vec![
                    Ok(update_wrap(
                        yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Slot(
                            SubscribeUpdateSlot {
                                slot: 42,
                                parent: Some(41),
                                status: SlotStatus::SlotProcessed as i32,
                                dead_error: None,
                            },
                        ),
                    )),
                    Ok(geyser_pump_candidate_update(&loaded)),
                ],
            }])),
        };
        let summary = smoke_geyser_provider_with_connector(
            &loaded,
            GeyserProviderSmokeOptions {
                duration_seconds: 1,
                max_updates: Some(2),
                ..GeyserProviderSmokeOptions::default()
            },
            Arc::new(connector),
        )
        .await
        .expect("summary");
        assert_eq!(summary.provider_status, "updates_received");
        assert!(summary.slot_updates > 0);
        assert!(summary.transaction_updates > 0);
    }

    #[tokio::test]
    async fn material_hunter_progress_callback_fires_on_raw_provider_updates() {
        let mut loaded = loaded_config();
        loaded.config.geyser.endpoint = "http://example.invalid:10000".to_owned();
        let connector = MockGeyserConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![MockConnectorBatch {
                updates: vec![Ok(update_wrap(
                    yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Slot(
                        SubscribeUpdateSlot {
                            slot: 77,
                            parent: Some(76),
                            status: SlotStatus::SlotProcessed as i32,
                            dead_error: None,
                        },
                    ),
                ))],
            }])),
        };
        let progress_calls = Arc::new(std::sync::Mutex::new(0usize));
        let progress_calls_for_cb = progress_calls.clone();
        let summary = run_material_hunter_stream_with_connector(
            &loaded,
            MaterialHunterStreamOptions {
                duration_seconds: 1,
                ..MaterialHunterStreamOptions::default()
            },
            Arc::new(connector),
            |_event, _summary| Ok(MaterialHunterStreamAction::Continue),
            move |summary| {
                if summary.provider_status != "not_attempted" {
                    assert!(summary.connected);
                }
                *progress_calls_for_cb.lock().expect("progress lock") += 1;
                if summary.slot_updates > 0 {
                    return Ok(MaterialHunterStreamAction::Stop);
                }
                Ok(MaterialHunterStreamAction::Continue)
            },
        )
        .await
        .expect("material hunter summary");
        assert_eq!(summary.provider_status, "stopped_by_hunter");
        assert!(summary.slot_updates > 0);
        assert!(*progress_calls.lock().expect("progress lock") >= 2);
    }

    #[tokio::test]
    async fn material_hunter_bounded_reader_queue_reports_client_backpressure() {
        let mut loaded = loaded_config();
        loaded.config.geyser.endpoint = "http://example.invalid:10000".to_owned();
        loaded.config.geyser.max_inflight_messages = 1;
        loaded.config.geyser.material_hunter_router_queue_capacity = 1;
        loaded.config.geyser.material_hunter_worker_partitions = 1;
        loaded
            .config
            .geyser
            .material_hunter_partition_queue_capacity = 1;
        let mut updates = Vec::new();
        for index in 0..512u64 {
            let mut update = geyser_pump_candidate_update(&loaded);
            if let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_mut() {
                if let Some(info) = tx.transaction.as_mut() {
                    let mut signature = vec![0; 64];
                    signature[..8].copy_from_slice(&index.to_le_bytes());
                    info.signature = signature.clone();
                    if let Some(transaction) = info.transaction.as_mut() {
                        transaction.signatures = vec![signature];
                    }
                }
            }
            updates.push(Ok(update));
        }
        let connector = MockGeyserConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![MockConnectorBatch { updates }])),
        };
        let summary = run_material_hunter_stream_with_connector(
            &loaded,
            MaterialHunterStreamOptions {
                duration_seconds: 1,
                ..MaterialHunterStreamOptions::default()
            },
            Arc::new(connector),
            |_event, _summary| Ok(MaterialHunterStreamAction::Continue),
            |_summary| {
                std::thread::sleep(StdDuration::from_millis(5));
                Ok(MaterialHunterStreamAction::Continue)
            },
        )
        .await
        .expect("client backpressure should be structured");
        assert_eq!(summary.provider_status, "client_backpressure_detected");
        assert_eq!(
            summary.provider_blocker_class.as_deref(),
            Some("client_backpressure_detected")
        );
        assert!(summary.client_backpressure_detected);
        assert!(
            summary.internal_queue_full_count > 0
                || summary.router_queue_full_count > 0
                || summary.partition_queue_full_count_total > 0
                || summary.worker_backpressure_detected
        );
        assert!(summary.worker_partitions >= 1);
        assert!(!summary.stream_completed_normally);
    }

    #[test]
    fn partition_router_same_mint_same_partition() {
        let a = geyser_token_balance_update("Mint111111111111111111111111111111111111111", 1);
        let b = geyser_token_balance_update("Mint111111111111111111111111111111111111111", 2);
        let c = geyser_token_balance_update("Mint222222222222222222222222222222222222222", 1);
        let (pa, fa, _) = material_hunter_partition_for_update(&a, 4);
        let (pb, fb, _) = material_hunter_partition_for_update(&b, 4);
        let (pc, _, _) = material_hunter_partition_for_update(&c, 4);
        assert_eq!(pa, pb);
        assert!(!fa);
        assert!(!fb);
        assert!(pa < 4);
        assert!(pc < 4);
    }

    #[test]
    fn partition_router_same_account_same_partition() {
        let a = geyser_token_account_update(7);
        let b = geyser_token_account_update(7);
        let (pa, fa, _) = material_hunter_partition_for_update(&a, 4);
        let (pb, fb, _) = material_hunter_partition_for_update(&b, 4);
        assert_eq!(pa, pb);
        assert!(!fa);
        assert!(!fb);
    }

    #[test]
    fn partition_router_fallback_is_deterministic() {
        let update = update_wrap(
            yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Ping(
                SubscribeUpdatePing {},
            ),
        );
        let first = material_hunter_partition_for_update(&update, 4);
        let second = material_hunter_partition_for_update(&update, 4);
        assert_eq!(first, second);
        assert!(first.1);
    }

    #[tokio::test]
    async fn material_hunter_slot_liveness_does_not_fill_worker_queue() {
        let mut loaded = loaded_config();
        loaded.config.geyser.endpoint = "http://example.invalid:10000".to_owned();
        loaded.config.geyser.max_inflight_messages = 1;
        let mut updates = Vec::new();
        for slot in 0..512u64 {
            updates.push(Ok(update_wrap(
                yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Slot(
                    SubscribeUpdateSlot {
                        slot,
                        parent: slot.checked_sub(1),
                        status: SlotStatus::SlotProcessed as i32,
                        dead_error: None,
                    },
                ),
            )));
        }
        let connector = MockGeyserConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![MockConnectorBatch { updates }])),
        };
        let summary = run_material_hunter_stream_with_connector(
            &loaded,
            MaterialHunterStreamOptions {
                duration_seconds: 1,
                ..MaterialHunterStreamOptions::default()
            },
            Arc::new(connector),
            |_event, _summary| Ok(MaterialHunterStreamAction::Continue),
            |summary| {
                if summary.slot_updates >= 512 {
                    return Ok(MaterialHunterStreamAction::Stop);
                }
                Ok(MaterialHunterStreamAction::Continue)
            },
        )
        .await
        .expect("slot-only liveness should not pressure worker queue");
        assert_eq!(summary.provider_status, "stopped_by_hunter");
        assert_eq!(summary.slot_updates, 512);
        assert!(!summary.client_backpressure_detected);
        assert_ne!(
            summary.provider_blocker_class.as_deref(),
            Some("client_backpressure_detected")
        );
    }

    #[tokio::test]
    async fn material_hunter_partition_telemetry_is_reported() {
        let mut loaded = loaded_config();
        loaded.config.geyser.endpoint = "http://example.invalid:10000".to_owned();
        loaded.config.geyser.material_hunter_worker_partitions = 4;
        loaded
            .config
            .geyser
            .material_hunter_partition_queue_capacity = 64;
        loaded.config.geyser.material_hunter_router_queue_capacity = 256;
        let connector = MockGeyserConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![MockConnectorBatch {
                updates: vec![
                    Ok(geyser_pump_candidate_update(&loaded)),
                    Ok(geyser_pump_candidate_update(&loaded)),
                ],
            }])),
        };
        let summary = run_material_hunter_stream_with_connector(
            &loaded,
            MaterialHunterStreamOptions {
                duration_seconds: 1,
                ..MaterialHunterStreamOptions::default()
            },
            Arc::new(connector),
            |_event, _summary| Ok(MaterialHunterStreamAction::Continue),
            |_summary| Ok(MaterialHunterStreamAction::Continue),
        )
        .await
        .expect("partitioned stream should run");
        assert!(summary.partitioning_enabled);
        assert_eq!(summary.worker_partitions, 4);
        assert!(summary.router_updates_received > 0);
        assert_eq!(
            summary.router_updates_received,
            summary.router_updates_routed
        );
        assert!(summary.partition_updates_processed_total > 0);
        assert_eq!(summary.partition_updates_processed_by_partition.len(), 4);
        assert_eq!(summary.partition_queue_full_count_by_partition.len(), 4);
    }

    #[tokio::test]
    async fn material_hunter_single_worker_mode_still_reports_partition_shape() {
        let mut loaded = loaded_config();
        loaded.config.geyser.endpoint = "http://example.invalid:10000".to_owned();
        loaded.config.geyser.material_hunter_partitioning_enabled = false;
        loaded.config.geyser.material_hunter_worker_partitions = 4;
        let connector = MockGeyserConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![MockConnectorBatch {
                updates: vec![Ok(geyser_pump_candidate_update(&loaded))],
            }])),
        };
        let summary = run_material_hunter_stream_with_connector(
            &loaded,
            MaterialHunterStreamOptions {
                duration_seconds: 1,
                ..MaterialHunterStreamOptions::default()
            },
            Arc::new(connector),
            |_event, _summary| Ok(MaterialHunterStreamAction::Continue),
            |_summary| Ok(MaterialHunterStreamAction::Continue),
        )
        .await
        .expect("single-worker fallback should run");
        assert!(!summary.partitioning_enabled);
        assert_eq!(summary.worker_partitions, 1);
        assert_eq!(summary.partition_updates_processed_by_partition.len(), 1);
    }

    #[tokio::test]
    async fn material_hunter_stream_closed_before_deadline_is_non_countable() {
        let mut loaded = loaded_config();
        loaded.config.geyser.endpoint = "http://example.invalid:10000".to_owned();
        let connector = MockGeyserConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![MockConnectorBatch {
                updates: vec![Ok(update_wrap(
                    yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Slot(
                        SubscribeUpdateSlot {
                            slot: 78,
                            parent: Some(77),
                            status: SlotStatus::SlotProcessed as i32,
                            dead_error: None,
                        },
                    ),
                ))],
            }])),
        };
        let summary = run_material_hunter_stream_with_connector(
            &loaded,
            MaterialHunterStreamOptions {
                duration_seconds: 2,
                ..MaterialHunterStreamOptions::default()
            },
            Arc::new(connector),
            |_event, _summary| Ok(MaterialHunterStreamAction::Continue),
            |_summary| Ok(MaterialHunterStreamAction::Continue),
        )
        .await
        .expect("early stream close should be structured, not an error");
        assert_eq!(
            summary.provider_status,
            "provider_stream_closed_before_deadline"
        );
        assert_eq!(
            summary.provider_blocker_class.as_deref(),
            Some("provider_stream_closed_before_deadline")
        );
        assert!(summary.slot_updates > 0);
        assert!(!summary.stream_completed_normally);
    }

    #[tokio::test]
    async fn material_hunter_provider_lag_is_classified_without_err() {
        let mut loaded = loaded_config();
        loaded.config.geyser.endpoint = "http://example.invalid:10000".to_owned();
        let connector = MockGeyserConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![MockConnectorBatch {
                updates: vec![Err(
                    "code: 'Unrecoverable data loss or corruption', message: \"lagged\"".to_owned(),
                )],
            }])),
        };
        let summary = run_material_hunter_stream_with_connector(
            &loaded,
            MaterialHunterStreamOptions {
                duration_seconds: 1,
                ..MaterialHunterStreamOptions::default()
            },
            Arc::new(connector),
            |_event, _summary| Ok(MaterialHunterStreamAction::Continue),
            |_summary| Ok(MaterialHunterStreamAction::Continue),
        )
        .await
        .expect("lagged status should be structured, not an error");
        assert_eq!(summary.provider_status, "provider_lagged_data_loss");
        assert_eq!(
            summary.provider_blocker_class.as_deref(),
            Some("provider_lagged_data_loss")
        );
        assert!(summary.provider_data_loss_seen);
        assert!(summary.provider_lagged_count > 0);
        assert!(!summary.stream_completed_normally);
    }

    #[tokio::test]
    async fn material_hunter_provider_lag_can_reconnect_in_gap_tolerant_mode() {
        let mut loaded = loaded_config();
        loaded.config.geyser.endpoint = "http://example.invalid:10000".to_owned();
        loaded.config.geyser.max_reconnect_attempts = Some(3);
        let connector = MockGeyserConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![
                MockConnectorBatch {
                    updates: vec![Err(
                        "code: 'Unrecoverable data loss or corruption', message: \"lagged\""
                            .to_owned(),
                    )],
                },
                MockConnectorBatch {
                    updates: vec![Ok(update_wrap(
                        yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Slot(
                            SubscribeUpdateSlot {
                                slot: 101,
                                parent: Some(100),
                                status: SlotStatus::SlotProcessed as i32,
                                dead_error: None,
                            },
                        ),
                    ))],
                },
            ])),
        };
        let statuses = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let statuses_for_progress = statuses.clone();
        let summary = run_material_hunter_stream_with_connector(
            &loaded,
            MaterialHunterStreamOptions {
                duration_seconds: 1,
                gap_tolerant_segments: true,
            },
            Arc::new(connector),
            |_event, _summary| Ok(MaterialHunterStreamAction::Continue),
            move |summary| {
                statuses_for_progress
                    .lock()
                    .expect("statuses")
                    .push(summary.provider_status.clone());
                if summary.slot_updates > 0 {
                    return Ok(MaterialHunterStreamAction::Stop);
                }
                Ok(MaterialHunterStreamAction::Continue)
            },
        )
        .await
        .expect("gap-tolerant lag should reconnect");
        assert!(summary.provider_data_loss_seen);
        assert!(summary.provider_lagged_count > 0);
        assert!(summary.reconnect_attempts > 0);
        assert!(summary.slot_updates > 0);
        assert_eq!(summary.provider_status, "stopped_by_hunter");
        let statuses = statuses.lock().expect("statuses");
        assert!(
            statuses
                .iter()
                .any(|status| status == "provider_lagged_data_loss")
        );
    }

    #[tokio::test]
    async fn material_hunter_transient_stream_error_reconnects() {
        let mut loaded = loaded_config();
        loaded.config.geyser.endpoint = "http://example.invalid:10000".to_owned();
        loaded.config.geyser.max_reconnect_attempts = Some(3);
        let connector = MockGeyserConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![
                MockConnectorBatch {
                    updates: vec![Err("transport reset".to_owned())],
                },
                MockConnectorBatch {
                    updates: vec![Ok(update_wrap(
                        yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Slot(
                            SubscribeUpdateSlot {
                                slot: 88,
                                parent: Some(87),
                                status: SlotStatus::SlotProcessed as i32,
                                dead_error: None,
                            },
                        ),
                    ))],
                },
            ])),
        };
        let summary = run_material_hunter_stream_with_connector(
            &loaded,
            MaterialHunterStreamOptions {
                duration_seconds: 1,
                ..MaterialHunterStreamOptions::default()
            },
            Arc::new(connector),
            |_event, _summary| Ok(MaterialHunterStreamAction::Continue),
            |summary| {
                if summary.slot_updates > 0 {
                    return Ok(MaterialHunterStreamAction::Stop);
                }
                Ok(MaterialHunterStreamAction::Continue)
            },
        )
        .await
        .expect("transient stream error should reconnect");
        assert!(summary.reconnect_attempts > 0);
        assert!(summary.slot_updates > 0);
        assert_ne!(summary.provider_status, "stream_error");
        assert_ne!(summary.provider_status, "provider_lagged_data_loss");
        assert_eq!(summary.provider_blocker_class, None);
    }

    #[tokio::test]
    async fn material_hunter_reconnect_exhausted_is_structured_non_countable() {
        let mut loaded = loaded_config();
        loaded.config.geyser.endpoint = "http://example.invalid:10000".to_owned();
        loaded.config.geyser.max_reconnect_attempts = Some(2);
        let connector = MockGeyserConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![
                MockConnectorBatch {
                    updates: vec![Err("transport reset".to_owned())],
                },
                MockConnectorBatch {
                    updates: vec![Err("transport reset".to_owned())],
                },
            ])),
        };
        let summary = run_material_hunter_stream_with_connector(
            &loaded,
            MaterialHunterStreamOptions {
                duration_seconds: 1,
                ..MaterialHunterStreamOptions::default()
            },
            Arc::new(connector),
            |_event, _summary| Ok(MaterialHunterStreamAction::Continue),
            |_summary| Ok(MaterialHunterStreamAction::Continue),
        )
        .await
        .expect("reconnect exhaustion should be structured, not an error");
        assert_eq!(summary.provider_status, "provider_reconnect_exhausted");
        assert_eq!(
            summary.provider_blocker_class.as_deref(),
            Some("provider_reconnect_exhausted")
        );
        assert!(summary.reconnect_attempts >= 2);
        assert!(!summary.stream_completed_normally);
    }

    #[tokio::test]
    async fn connector_gap_and_recovery_emit_events() {
        let mut loaded = loaded_config();
        loaded.config.geyser.endpoint = "http://example.invalid:10000".to_owned();
        let normalizer = GeyserEventNormalizer::from_loaded(&loaded).expect("normalizer");
        let connector = MockGeyserConnector {
            batches: Arc::new(std::sync::Mutex::new(vec![
                MockConnectorBatch {
                    updates: vec![Err("disconnect".to_owned())],
                },
                MockConnectorBatch {
                    updates: vec![Ok(update_wrap(
                        yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Slot(
                            SubscribeUpdateSlot {
                                slot: 100,
                                parent: Some(99),
                                status: SlotStatus::SlotProcessed as i32,
                                dead_error: None,
                            },
                        ),
                    ))],
                },
            ])),
        };
        let (tx, mut rx) = mpsc::channel(16);
        let handle = tokio::spawn(run_geyser_source_with_connector(
            loaded.config.geyser.clone(),
            normalizer,
            Arc::new(connector),
            tx,
        ));
        let mut seen_gap = false;
        let mut seen_recovery = false;
        let deadline = tokio::time::Instant::now() + StdDuration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            if let Some(event) = tokio::time::timeout(StdDuration::from_millis(100), rx.recv())
                .await
                .ok()
                .flatten()
            {
                if let EventPayload::DataGap(payload) = event.payload {
                    if payload.trade_allowed {
                        seen_recovery = true;
                    } else {
                        seen_gap = true;
                    }
                }
            }
            if seen_gap && seen_recovery {
                break;
            }
        }
        handle.abort();
        assert!(seen_gap);
        assert!(seen_recovery);
    }
}
