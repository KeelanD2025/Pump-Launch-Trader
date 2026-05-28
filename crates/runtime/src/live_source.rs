use std::{
    collections::{BTreeMap, HashMap, HashSet},
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
    pump_virtual_reserve_price_sol_per_token,
};
use futures::Stream;
use idl::{AccountDecode, DecodedAccount, InstructionDecode, LoadedIdl};
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

use crate::{resolved_geyser_endpoint, resolved_geyser_metadata};

const SYSTEM_PROGRAM_ID: &str = "11111111111111111111111111111111";
const COMPUTE_BUDGET_PROGRAM_ID: &str = "ComputeBudget111111111111111111111111111111";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

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
        let max_size = resolved.max_decoded_message_size.max(1024 * 1024);
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
                            associated_bonding_curve_account: None,
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
    pub pump_create_decoded: u64,
    pub tracked_mint: Option<String>,
    pub errors: Vec<String>,
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
                        if summary.tracked_mint.is_none()
                            && payload.status != TransactionStatus::Failed
                        {
                            summary.tracked_mint = Some(payload.mint.to_string());
                        }
                    }
                    summary.normalized_events = summary.normalized_events.saturating_add(1);
                    events.push(event);
                }
                if summary.tracked_mint.is_some()
                    && launches_seen >= options.max_launches.max(1)
                    && tokio::time::Instant::now() >= deadline
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
        virtual_quote: value_decimal(&decoded.fields, "virtual_quote_reserves")
            .unwrap_or(Decimal::ZERO),
        virtual_token: value_decimal(&decoded.fields, "virtual_token_reserves")
            .unwrap_or(Decimal::ZERO),
        real_quote: value_decimal(&decoded.fields, "real_quote_reserves").unwrap_or(Decimal::ZERO),
        real_token: value_decimal(&decoded.fields, "real_token_reserves").unwrap_or(Decimal::ZERO),
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
            market_cap_source: Some("price_times_supply".to_owned()),
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
    use common::{Canonicality, EventPayload, EventSource, config::LoadedConfig};
    use ingest_geyser::{GeyserIngestService, TransactionTokenBalance, TransactionUpdate};
    use yellowstone_grpc_proto::prelude::{
        CompiledInstruction, Message, SlotStatus, SubscribeUpdate, SubscribeUpdateDeshred,
        SubscribeUpdateDeshredTransaction, SubscribeUpdateDeshredTransactionInfo,
        SubscribeUpdatePing, SubscribeUpdateSlot, SubscribeUpdateTransaction,
        SubscribeUpdateTransactionInfo, TokenBalance, Transaction, TransactionStatusMeta,
        UiTokenAmount, subscribe_update_deshred,
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
                                    data: vec![1, 2, 3, 4],
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
                    Some("price_times_supply")
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
