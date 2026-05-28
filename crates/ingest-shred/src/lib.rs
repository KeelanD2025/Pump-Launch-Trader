use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use common::{
    Canonicality, EventMeta, EventPayload, EventSource, NormalizedEvent, ObservedTransactionEvent,
    PumpBuyEvent, PumpSellEvent, TokenCreatedEvent, monotonic_now_ns, unix_now,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
};
use tracing::warn;

#[derive(Debug, Error)]
pub enum ShredIngestError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("decode error: {0}")]
    Decode(#[from] ShredDecodeError),
    #[error("invalid packet: {0}")]
    InvalidPacket(String),
}

#[derive(Debug, Error)]
pub enum ShredDecodeError {
    #[error("fixture decode error: {0}")]
    Fixture(#[from] serde_json::Error),
    #[error("production shred decoding unavailable: {0}")]
    DependencyUnavailable(String),
    #[error("unsupported shred packet: {0}")]
    Unsupported(String),
}

#[derive(Debug, Clone)]
pub struct ReceivedPacket {
    pub data: Vec<u8>,
    pub peer_addr: SocketAddr,
    pub received_at: time::OffsetDateTime,
    pub observed_at_monotonic_ns: u64,
    pub packet_hash: String,
}

#[derive(Debug, Clone, Default)]
pub struct ShredMetrics {
    packets_received_total: Arc<AtomicU64>,
    decode_success_total: Arc<AtomicU64>,
    decode_failure_total: Arc<AtomicU64>,
    events_tentative_total: Arc<AtomicU64>,
    reconciliation_success_total: Arc<AtomicU64>,
    reconciliation_failure_total: Arc<AtomicU64>,
    geyser_without_shred_total: Arc<AtomicU64>,
    false_positive_tentative_total: Arc<AtomicU64>,
    missing_later_confirmed_total: Arc<AtomicU64>,
    gap_count: Arc<AtomicU64>,
    packet_drop_count: Arc<AtomicU64>,
    queue_depth: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShredMetricsSnapshot {
    pub shred_packets_received_total: u64,
    pub shred_decode_success_total: u64,
    pub shred_decode_failure_total: u64,
    pub shred_events_tentative_total: u64,
    pub shred_reconciliation_success_total: u64,
    pub shred_reconciliation_failure_total: u64,
    pub geyser_without_shred_total: u64,
    pub false_positive_tentative_total: u64,
    pub missing_later_confirmed_total: u64,
    pub shred_gap_count: u64,
    pub shred_packet_drop_count: u64,
    pub shred_queue_depth: u64,
}

impl ShredMetrics {
    fn increment(counter: &Arc<AtomicU64>) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn set(counter: &Arc<AtomicU64>, value: u64) {
        counter.store(value, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ShredMetricsSnapshot {
        ShredMetricsSnapshot {
            shred_packets_received_total: self.packets_received_total.load(Ordering::Relaxed),
            shred_decode_success_total: self.decode_success_total.load(Ordering::Relaxed),
            shred_decode_failure_total: self.decode_failure_total.load(Ordering::Relaxed),
            shred_events_tentative_total: self.events_tentative_total.load(Ordering::Relaxed),
            shred_reconciliation_success_total: self
                .reconciliation_success_total
                .load(Ordering::Relaxed),
            shred_reconciliation_failure_total: self
                .reconciliation_failure_total
                .load(Ordering::Relaxed),
            geyser_without_shred_total: self.geyser_without_shred_total.load(Ordering::Relaxed),
            false_positive_tentative_total: self
                .false_positive_tentative_total
                .load(Ordering::Relaxed),
            missing_later_confirmed_total: self
                .missing_later_confirmed_total
                .load(Ordering::Relaxed),
            shred_gap_count: self.gap_count.load(Ordering::Relaxed),
            shred_packet_drop_count: self.packet_drop_count.load(Ordering::Relaxed),
            shred_queue_depth: self.queue_depth.load(Ordering::Relaxed),
        }
    }
}

pub struct ShredUdpReceiver {
    rx: mpsc::Receiver<ReceivedPacket>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    local_addr: SocketAddr,
    pub metrics: ShredMetrics,
}

impl ShredUdpReceiver {
    pub async fn bind(
        bind_addr: &str,
        max_packet_size: usize,
        queue_capacity: usize,
        metrics: ShredMetrics,
    ) -> Result<Self, ShredIngestError> {
        let socket = UdpSocket::bind(bind_addr).await?;
        let local_addr = socket.local_addr()?;
        let (tx, rx) = mpsc::channel(queue_capacity.max(1));
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let metrics_clone = metrics.clone();

        tokio::spawn(async move {
            let mut buffer = vec![0u8; max_packet_size.max(1)];
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => {
                        break;
                    }
                    packet = socket.recv_from(&mut buffer) => {
                        match packet {
                            Ok((size, peer_addr)) => {
                                ShredMetrics::increment(&metrics_clone.packets_received_total);
                                let payload = buffer[..size].to_vec();
                                let packet_hash = packet_hash(&payload);
                                let received_packet = ReceivedPacket {
                                    data: payload,
                                    peer_addr,
                                    received_at: unix_now(),
                                    observed_at_monotonic_ns: monotonic_now_ns(),
                                    packet_hash,
                                };
                                match tx.try_send(received_packet) {
                                    Ok(()) => {
                                        ShredMetrics::set(&metrics_clone.queue_depth, tx.max_capacity() as u64 - tx.capacity() as u64);
                                    }
                                    Err(error) => {
                                        if matches!(error, mpsc::error::TrySendError::Full(_)) {
                                            ShredMetrics::increment(&metrics_clone.packet_drop_count);
                                        }
                                    }
                                }
                            }
                            Err(error) => {
                                warn!(%error, "shred udp receiver exiting after socket error");
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            rx,
            shutdown_tx: Some(shutdown_tx),
            local_addr,
            metrics,
        })
    }

    pub async fn recv(&mut self) -> Option<ReceivedPacket> {
        let packet = self.rx.recv().await;
        ShredMetrics::set(&self.metrics.queue_depth, self.rx.len() as u64);
        packet
    }

    pub fn local_queue_depth(&self) -> usize {
        self.rx.len()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn shutdown(mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
    }
}

fn packet_hash(packet: &[u8]) -> String {
    format!("{:x}", Sha256::digest(packet))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecodedShredBatch {
    pub slot: Option<u64>,
    pub entry_index: Option<u32>,
    pub partial: bool,
    pub transactions: Vec<DecodedShredTransaction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecodedShredTransaction {
    pub signature: Option<String>,
    pub tx_position_estimate: Option<u32>,
    pub decode_confidence: Decimal,
    pub payload: DecodedShredPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DecodedShredPayload {
    TokenCreated {
        event: TokenCreatedEvent,
    },
    PumpBuy {
        event: PumpBuyEvent,
    },
    PumpSell {
        event: PumpSellEvent,
    },
    Generic {
        signature_hint: Option<String>,
        program_ids: Vec<String>,
        account_count: usize,
        instruction_count: usize,
    },
}

pub trait ShredDecoder: Send + Sync + 'static {
    fn decode_packet(&self, packet: &ReceivedPacket)
    -> Result<DecodedShredBatch, ShredDecodeError>;
}

#[derive(Debug, Default, Clone)]
pub struct FixtureShredDecoder;

impl ShredDecoder for FixtureShredDecoder {
    fn decode_packet(
        &self,
        packet: &ReceivedPacket,
    ) -> Result<DecodedShredBatch, ShredDecodeError> {
        serde_json::from_slice(&packet.data).map_err(ShredDecodeError::from)
    }
}

#[derive(Debug, Default, Clone)]
pub struct ProductionShredDecoder;

impl ShredDecoder for ProductionShredDecoder {
    fn decode_packet(
        &self,
        _packet: &ReceivedPacket,
    ) -> Result<DecodedShredBatch, ShredDecodeError> {
        Err(ShredDecodeError::DependencyUnavailable(
            "real Solana shred decoding is gated behind external ledger/shred dependencies that are not compiled into this workspace yet".to_owned(),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct ReconciliationConfig {
    pub ttl: time::Duration,
}

impl Default for ReconciliationConfig {
    fn default() -> Self {
        Self {
            ttl: time::Duration::seconds(5),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationResult {
    pub signature: String,
    pub shred_to_geyser_delay_ms: i64,
}

#[derive(Debug, Clone)]
struct TentativeRecord {
    inserted_at: time::OffsetDateTime,
    signature: Option<String>,
    key: String,
}

#[derive(Debug, Default)]
pub struct ShredReconciler {
    config: ReconciliationConfig,
    records: HashMap<String, TentativeRecord>,
    expired_signatures: BTreeSet<String>,
    order: VecDeque<String>,
}

impl ShredReconciler {
    pub fn new(config: ReconciliationConfig) -> Self {
        Self {
            config,
            ..Self::default()
        }
    }

    pub fn record_tentative(&mut self, event: &NormalizedEvent) {
        let key = reconciliation_key(event);
        let record = TentativeRecord {
            inserted_at: event.meta.received_at_wall_time,
            signature: event.signature().map(ToOwned::to_owned),
            key: key.clone(),
        };
        self.records.insert(key.clone(), record);
        self.order.push_back(key);
    }

    pub fn reconcile_canonical(
        &mut self,
        event: &NormalizedEvent,
        metrics: &ShredMetrics,
    ) -> Option<ReconciliationResult> {
        let key = reconciliation_key(event);
        if let Some(record) = self.records.remove(&key) {
            let delay_ms =
                (event.meta.received_at_wall_time - record.inserted_at).whole_milliseconds();
            ShredMetrics::increment(&metrics.reconciliation_success_total);
            return Some(ReconciliationResult {
                signature: record.signature.unwrap_or(key),
                shred_to_geyser_delay_ms: delay_ms as i64,
            });
        }

        if let Some(signature) = event.signature() {
            if self.expired_signatures.remove(signature) {
                ShredMetrics::increment(&metrics.missing_later_confirmed_total);
            } else {
                ShredMetrics::increment(&metrics.geyser_without_shred_total);
            }
        }
        None
    }

    pub fn expire(&mut self, now: time::OffsetDateTime, metrics: &ShredMetrics) -> Vec<String> {
        let mut expired = Vec::new();
        while let Some(front) = self.order.front() {
            let Some(record) = self.records.get(front) else {
                self.order.pop_front();
                continue;
            };
            if now - record.inserted_at < self.config.ttl {
                break;
            }
            let key = self.order.pop_front().expect("front exists");
            if let Some(record) = self.records.remove(&key) {
                if let Some(signature) = record.signature {
                    self.expired_signatures.insert(signature.clone());
                    expired.push(signature);
                } else {
                    expired.push(record.key);
                }
                ShredMetrics::increment(&metrics.reconciliation_failure_total);
                ShredMetrics::increment(&metrics.false_positive_tentative_total);
            }
        }
        expired
    }
}

fn reconciliation_key(event: &NormalizedEvent) -> String {
    if let Some(signature) = event.signature() {
        return format!("sig:{signature}");
    }
    format!(
        "slot:{}:kind:{}",
        event.meta.slot,
        match &event.payload {
            EventPayload::TokenCreated(_) => "token_created",
            EventPayload::PumpBuy(_) => "pump_buy",
            EventPayload::PumpSell(_) => "pump_sell",
            EventPayload::BondingCurveUpdate(_) => "bonding_curve_update",
            EventPayload::HolderBalanceUpdate(_) => "holder_balance_update",
            EventPayload::WalletFunding(_) => "wallet_funding",
            EventPayload::ObservedTransaction(_) => "observed_tx",
            EventPayload::TentativeSellIntentDetected(_) => "tentative_sell_intent_detected",
            EventPayload::TentativeMaliciousSellWarning(_) => "tentative_sell_warning",
            EventPayload::ShredEmergencyExitArmed(_) => "shred_exit_armed",
            EventPayload::ShredEmergencyExitTriggered(_) => "shred_exit_triggered",
            EventPayload::ShredSellIntentResolved(_) => "shred_sell_resolved",
            EventPayload::TokenTerminal(_) => "token_terminal",
            EventPayload::TradeDecision(_) => "trade_decision",
            EventPayload::SimulatedFill(_) => "sim_fill",
            EventPayload::LiveFill(_) => "live_fill",
            EventPayload::DataGap(_) => "data_gap",
        }
    )
}

pub struct ShredIngestService<D> {
    decoder: D,
    reconciler: ShredReconciler,
    pub metrics: ShredMetrics,
}

impl<D> ShredIngestService<D>
where
    D: ShredDecoder,
{
    pub fn new(decoder: D, reconciler: ShredReconciler, metrics: ShredMetrics) -> Self {
        Self {
            decoder,
            reconciler,
            metrics,
        }
    }

    pub fn process_packet(
        &mut self,
        packet: &ReceivedPacket,
    ) -> Result<Vec<NormalizedEvent>, ShredIngestError> {
        let decoded = match self.decoder.decode_packet(packet) {
            Ok(decoded) => {
                ShredMetrics::increment(&self.metrics.decode_success_total);
                decoded
            }
            Err(error) => {
                ShredMetrics::increment(&self.metrics.decode_failure_total);
                return Err(ShredIngestError::Decode(error));
            }
        };

        if decoded.partial {
            ShredMetrics::increment(&self.metrics.gap_count);
        }

        let mut events = Vec::new();
        for transaction in decoded.transactions {
            let mut meta = EventMeta::new(
                EventSource::ShredTentative,
                Canonicality::Tentative,
                decoded.slot.unwrap_or_default(),
            );
            meta.received_at_wall_time = packet.received_at;
            meta.observed_at_monotonic_ns = packet.observed_at_monotonic_ns;
            meta.source_latency_ms = Some(0);
            meta.signature = transaction.signature.clone();
            meta.transaction_index = transaction.tx_position_estimate;
            meta.decode_confidence = transaction.decode_confidence;
            if decoded.partial {
                meta.data_quality_flags
                    .push(common::DataQualityFlag::PartialShred);
            }
            meta.raw_reference = Some(common::RawEventReference {
                source_id: packet.packet_hash.clone(),
                cursor: Some(packet.peer_addr.to_string()),
                offset: decoded.entry_index.map(u64::from),
            });

            let payload = match transaction.payload {
                DecodedShredPayload::TokenCreated { event } => EventPayload::TokenCreated(event),
                DecodedShredPayload::PumpBuy { event } => EventPayload::PumpBuy(event),
                DecodedShredPayload::PumpSell { event } => EventPayload::PumpSell(event),
                DecodedShredPayload::Generic {
                    signature_hint,
                    program_ids,
                    account_count,
                    instruction_count,
                } => EventPayload::ObservedTransaction(ObservedTransactionEvent {
                    signature_hint,
                    slot_hint: decoded.slot,
                    entry_index: decoded.entry_index,
                    tx_position_estimate: transaction.tx_position_estimate,
                    signer: None,
                    program_ids,
                    account_count,
                    instruction_count,
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
                    raw_packet_hash: packet.packet_hash.clone(),
                    first_seen_by_shred_ns: packet.observed_at_monotonic_ns,
                    decode_confidence: transaction.decode_confidence,
                }),
            };
            let event = NormalizedEvent { meta, payload };
            self.reconciler.record_tentative(&event);
            ShredMetrics::increment(&self.metrics.events_tentative_total);
            events.push(event);
        }
        Ok(events)
    }

    pub fn reconcile_canonical(&mut self, event: &NormalizedEvent) -> Option<ReconciliationResult> {
        self.reconciler.reconcile_canonical(event, &self.metrics)
    }

    pub fn expire_tentative(&mut self, now: time::OffsetDateTime) -> Vec<String> {
        self.reconciler.expire(now, &self.metrics)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use common::{
        Canonicality, EventMeta, EventPayload, EventSource, NormalizedEvent, PubkeyValue,
        PumpBuyEvent, PumpSellEvent, QuoteAssetType, TokenCreatedEvent, TokenProgramType,
    };
    use rust_decimal::Decimal;
    use tokio::net::UdpSocket;

    use super::*;

    fn pubkey(value: &str) -> PubkeyValue {
        PubkeyValue::from_str(value).expect("valid pubkey")
    }

    fn service() -> ShredIngestService<FixtureShredDecoder> {
        ShredIngestService::new(
            FixtureShredDecoder,
            ShredReconciler::new(ReconciliationConfig::default()),
            ShredMetrics::default(),
        )
    }

    fn token_created_event() -> TokenCreatedEvent {
        TokenCreatedEvent {
            mint: pubkey("So11111111111111111111111111111111111111112"),
            token_program: TokenProgramType::SplToken,
            quote_mint: pubkey("So11111111111111111111111111111111111111112"),
            quote_asset_type: QuoteAssetType::WrappedSol,
            creator_wallet: pubkey("11111111111111111111111111111111"),
            payer: pubkey("11111111111111111111111111111111"),
            bonding_curve_account: pubkey("So11111111111111111111111111111111111111112"),
            associated_bonding_curve_account: None,
            metadata_account: None,
            name: "alpha".to_owned(),
            symbol: "ALP".to_owned(),
            uri: "https://example.invalid".to_owned(),
            create_instruction_variant: "create".to_owned(),
            initial_virtual_quote_reserves: Some(Decimal::from(10u64)),
            initial_virtual_token_reserves: Some(Decimal::from(20u64)),
            initial_real_quote_reserves: Some(Decimal::from(10u64)),
            initial_real_token_reserves: Some(Decimal::from(20u64)),
            initial_supply: Some(Decimal::from(1_000u64)),
            creator_initial_buy: Some(Decimal::from(2u64)),
            same_transaction_buys: 1,
            same_slot_buys: 1,
            fee_recipients: vec![],
            raw_account_list: vec![],
            launch_transaction_fingerprint: Some("fixture".to_owned()),
            status: common::TransactionStatus::Success,
        }
    }

    fn buy_event() -> PumpBuyEvent {
        PumpBuyEvent {
            mint: pubkey("So11111111111111111111111111111111111111112"),
            buyer: pubkey("11111111111111111111111111111111"),
            payer: pubkey("11111111111111111111111111111111"),
            quote_in: Decimal::from(3u64),
            token_out: Decimal::from(100u64),
            price_before: Some(Decimal::from(1u64)),
            price_after: Some(Decimal::from(2u64)),
            effective_price: Decimal::from(2u64),
            slippage_estimate: Some(Decimal::new(5, 2)),
            reserves_before: None,
            reserves_after: None,
            max_quote_cost: Some(Decimal::from(4u64)),
            compute_unit_limit: Some(200_000),
            compute_unit_price: Some(1_000),
            estimated_priority_fee_lamports: Some(common::Lamports(200)),
            estimated_base_fee_lamports: Some(common::Lamports(5_000)),
            estimated_tip_lamports: None,
            is_creator: false,
            is_known_cluster_member: false,
            is_first_buy: true,
            status: common::TransactionStatus::Success,
        }
    }

    fn sell_event() -> PumpSellEvent {
        PumpSellEvent {
            mint: pubkey("So11111111111111111111111111111111111111112"),
            seller: pubkey("11111111111111111111111111111111"),
            quote_out: Decimal::from(2u64),
            token_in: Decimal::from(50u64),
            price_before: Some(Decimal::from(2u64)),
            price_after: Some(Decimal::from(1u64)),
            effective_price: Decimal::from(1u64),
            slippage_estimate: Some(Decimal::new(10, 2)),
            reserves_before: None,
            reserves_after: None,
            min_quote_output: Some(Decimal::ONE),
            compute_unit_limit: Some(200_000),
            compute_unit_price: Some(1_000),
            estimated_priority_fee_lamports: Some(common::Lamports(200)),
            estimated_base_fee_lamports: Some(common::Lamports(5_000)),
            estimated_tip_lamports: None,
            is_creator: false,
            is_top_holder_pre_sell: false,
            is_known_cluster_member: false,
            status: common::TransactionStatus::Success,
        }
    }

    fn received_packet(payload: DecodedShredBatch) -> ReceivedPacket {
        let data = serde_json::to_vec(&payload).expect("serialize");
        ReceivedPacket {
            data: data.clone(),
            peer_addr: "127.0.0.1:9999".parse().expect("addr"),
            received_at: unix_now(),
            observed_at_monotonic_ns: monotonic_now_ns(),
            packet_hash: packet_hash(&data),
        }
    }

    fn canonical_event(signature: &str) -> NormalizedEvent {
        let mut meta = EventMeta::new(EventSource::GeyserProcessed, Canonicality::Processed, 11);
        meta.signature = Some(signature.to_owned());
        meta.received_at_wall_time = unix_now();
        NormalizedEvent {
            meta,
            payload: EventPayload::PumpBuy(buy_event()),
        }
    }

    #[test]
    fn fixture_decoder_round_trips_packet() {
        let packet = received_packet(DecodedShredBatch {
            slot: Some(11),
            entry_index: Some(2),
            partial: false,
            transactions: vec![DecodedShredTransaction {
                signature: Some("sig-a".to_owned()),
                tx_position_estimate: Some(1),
                decode_confidence: Decimal::ONE,
                payload: DecodedShredPayload::TokenCreated {
                    event: token_created_event(),
                },
            }],
        });
        let decoded = FixtureShredDecoder
            .decode_packet(&packet)
            .expect("fixture decode");
        assert_eq!(decoded.slot, Some(11));
        assert_eq!(decoded.transactions.len(), 1);
    }

    #[test]
    fn tentative_event_generation_covers_known_and_generic_payloads() {
        let mut service = service();
        let packet = received_packet(DecodedShredBatch {
            slot: Some(12),
            entry_index: Some(7),
            partial: false,
            transactions: vec![
                DecodedShredTransaction {
                    signature: Some("sig-b".to_owned()),
                    tx_position_estimate: Some(1),
                    decode_confidence: Decimal::new(95, 2),
                    payload: DecodedShredPayload::PumpBuy { event: buy_event() },
                },
                DecodedShredTransaction {
                    signature: Some("sig-c".to_owned()),
                    tx_position_estimate: Some(2),
                    decode_confidence: Decimal::new(75, 2),
                    payload: DecodedShredPayload::Generic {
                        signature_hint: Some("sig-c".to_owned()),
                        program_ids: vec!["Pump111111111111111111111111111111111111111".to_owned()],
                        account_count: 4,
                        instruction_count: 2,
                    },
                },
            ],
        });
        let events = service.process_packet(&packet).expect("events");
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0].payload, EventPayload::PumpBuy(_)));
        assert!(matches!(
            events[1].payload,
            EventPayload::ObservedTransaction(_)
        ));
        assert_eq!(service.metrics.snapshot().shred_events_tentative_total, 2);
    }

    #[test]
    fn reconciliation_success_tracks_latency() {
        let mut service = service();
        let packet = received_packet(DecodedShredBatch {
            slot: Some(11),
            entry_index: Some(2),
            partial: false,
            transactions: vec![DecodedShredTransaction {
                signature: Some("sig-d".to_owned()),
                tx_position_estimate: Some(1),
                decode_confidence: Decimal::ONE,
                payload: DecodedShredPayload::PumpBuy { event: buy_event() },
            }],
        });
        let _ = service.process_packet(&packet).expect("packet");
        let result = service
            .reconcile_canonical(&canonical_event("sig-d"))
            .expect("must reconcile");
        assert_eq!(result.signature, "sig-d");
        assert_eq!(
            service
                .metrics
                .snapshot()
                .shred_reconciliation_success_total,
            1
        );
    }

    #[test]
    fn reconciliation_timeout_marks_false_positive() {
        let mut service = service();
        let packet = received_packet(DecodedShredBatch {
            slot: Some(13),
            entry_index: Some(1),
            partial: true,
            transactions: vec![DecodedShredTransaction {
                signature: Some("sig-e".to_owned()),
                tx_position_estimate: Some(1),
                decode_confidence: Decimal::new(5, 1),
                payload: DecodedShredPayload::PumpSell {
                    event: sell_event(),
                },
            }],
        });
        let events = service.process_packet(&packet).expect("packet");
        assert_eq!(events.len(), 1);
        let expired = service.expire_tentative(packet.received_at + time::Duration::seconds(10));
        assert_eq!(expired, vec!["sig-e".to_owned()]);
        let snapshot = service.metrics.snapshot();
        assert_eq!(snapshot.shred_reconciliation_failure_total, 1);
        assert_eq!(snapshot.false_positive_tentative_total, 1);
        assert_eq!(snapshot.shred_gap_count, 1);
    }

    #[tokio::test]
    async fn udp_receiver_and_backpressure_work() {
        let metrics = ShredMetrics::default();
        let mut receiver = ShredUdpReceiver::bind("127.0.0.1:0", 1024, 1, metrics.clone())
            .await
            .expect("bind");
        let bind_addr = receiver.local_addr();
        let sender = UdpSocket::bind("127.0.0.1:0").await.expect("sender");
        for signature in ["sig-f", "sig-g", "sig-h"] {
            let packet = serde_json::to_vec(&DecodedShredBatch {
                slot: Some(14),
                entry_index: Some(0),
                partial: false,
                transactions: vec![DecodedShredTransaction {
                    signature: Some(signature.to_owned()),
                    tx_position_estimate: Some(1),
                    decode_confidence: Decimal::ONE,
                    payload: DecodedShredPayload::PumpBuy { event: buy_event() },
                }],
            })
            .expect("serialize");
            sender.send_to(&packet, bind_addr).await.expect("send");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _first = receiver.recv().await.expect("one packet should remain");
        let snapshot = metrics.snapshot();
        assert!(snapshot.shred_packets_received_total >= 1);
        assert!(snapshot.shred_packet_drop_count >= 1);
        receiver.shutdown().await;
    }

    #[test]
    fn metrics_increment_for_decode_failures() {
        let mut service = ShredIngestService::new(
            ProductionShredDecoder,
            ShredReconciler::new(ReconciliationConfig::default()),
            ShredMetrics::default(),
        );
        let packet = ReceivedPacket {
            data: vec![1, 2, 3],
            peer_addr: "127.0.0.1:9999".parse().expect("addr"),
            received_at: unix_now(),
            observed_at_monotonic_ns: monotonic_now_ns(),
            packet_hash: "abc".to_owned(),
        };
        let error = service.process_packet(&packet).expect_err("must fail");
        assert!(matches!(error, ShredIngestError::Decode(_)));
        let snapshot = service.metrics.snapshot();
        assert_eq!(snapshot.shred_decode_failure_total, 1);
    }

    #[test]
    fn late_canonical_after_expiry_is_tracked() {
        let mut service = service();
        let packet = received_packet(DecodedShredBatch {
            slot: Some(15),
            entry_index: Some(2),
            partial: false,
            transactions: vec![DecodedShredTransaction {
                signature: Some("sig-z".to_owned()),
                tx_position_estimate: Some(1),
                decode_confidence: Decimal::ONE,
                payload: DecodedShredPayload::TokenCreated {
                    event: token_created_event(),
                },
            }],
        });
        let _ = service.process_packet(&packet).expect("packet");
        let _ = service.expire_tentative(packet.received_at + time::Duration::seconds(6));
        let _ = service.reconcile_canonical(&canonical_event("sig-z"));
        assert_eq!(service.metrics.snapshot().missing_later_confirmed_total, 1);
    }
}
