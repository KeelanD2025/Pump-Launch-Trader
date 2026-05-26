use common::config::LoadedConfig;
use decoder::DecoderRegistry;
use event_bus::{EventBus, Priority};
use idl::anchor_discriminator;
use ingest_geyser::{
    GeyserEnvelope, GeyserIngestService, GeyserMessage, IngestOutput, SlotCommitment, SlotUpdate,
    TransactionInstruction, TransactionUpdate,
};
use rpc_budget::{RpcBudgetManager, RpcCallCategory, RpcCallRequest, RpcNetworkKind, RpcReason};
use std::{
    fs,
    path::{Path, PathBuf},
};
use time::OffsetDateTime;

fn config() -> LoadedConfig {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("config")
        .join("default.toml");
    LoadedConfig::from_file(path).expect("config")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .to_path_buf()
}

fn collect_rs_files(root: &Path, output: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.filter_map(|entry| entry.ok()) {
        let path = entry.path();
        if path.file_name().and_then(|value| value.to_str()) == Some("target") {
            continue;
        }
        if path.is_dir() {
            if path.file_name().and_then(|value| value.to_str()) == Some("tests") {
                continue;
            }
            collect_rs_files(&path, output);
        } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
            output.push(path);
        }
    }
}

fn collect_all_files(root: &Path, output: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.filter_map(|entry| entry.ok()) {
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        if matches!(name, "target" | ".git") {
            continue;
        }
        if path.is_dir() {
            collect_all_files(&path, output);
        } else {
            output.push(path);
        }
    }
}

#[test]
fn default_config_builds_decoder_registry() {
    let loaded = config();
    let registry = DecoderRegistry::from_config(&loaded).expect("registry");
    let mut data = Vec::new();
    data.extend_from_slice(&anchor_discriminator("global", "sell"));
    data.extend_from_slice(&9u64.to_le_bytes());
    data.extend_from_slice(&8u64.to_le_bytes());
    let decoded = registry
        .decode_instruction(&data)
        .expect("decode")
        .expect("known");
    assert_eq!(decoded.name, "sell");
}

#[tokio::test]
async fn event_bus_stays_bounded_and_prioritized() {
    let mut bus = EventBus::bounded(4);
    let publisher = bus.publisher();
    publisher.try_publish(Priority::Low, "l").expect("low");
    publisher.try_publish(Priority::High, "h").expect("high");
    assert_eq!(bus.recv().await, Some("h"));
    assert_eq!(bus.recv().await, Some("l"));
}

#[test]
fn geyser_service_produces_gap_plus_slot_events() {
    let loaded = config();
    let mut service = GeyserIngestService::new(loaded.config.geyser.clone());
    service.process_envelope(GeyserEnvelope {
        received_at: OffsetDateTime::UNIX_EPOCH,
        observed_at_monotonic_ns: 1,
        source_latency_ms: None,
        message: GeyserMessage::Slot(SlotUpdate {
            slot: 1,
            parent: None,
            status: SlotCommitment::Processed,
            block_time: None,
        }),
    });
    let outputs = service.process_envelope(GeyserEnvelope {
        received_at: OffsetDateTime::UNIX_EPOCH,
        observed_at_monotonic_ns: 2,
        source_latency_ms: Some(2),
        message: GeyserMessage::Slot(SlotUpdate {
            slot: 5,
            parent: Some(4),
            status: SlotCommitment::Processed,
            block_time: None,
        }),
    });
    assert!(
        outputs
            .iter()
            .any(|output| matches!(output, IngestOutput::DataGap { .. }))
    );
    assert!(
        outputs
            .iter()
            .any(|output| matches!(output, IngestOutput::Slot { .. }))
    );
}

#[test]
fn rpc_budget_blocks_cold_start_reason_in_live_mode() {
    let loaded = config();
    let mut manager = RpcBudgetManager::new(
        loaded.config.rpc_budget.clone(),
        loaded.config.execution.clone(),
        loaded.config.stream_only.clone(),
        loaded.config.rpc.clone(),
    );
    let result = manager.check_and_record(RpcCallRequest {
        timestamp: OffsetDateTime::UNIX_EPOCH,
        endpoint: "http://127.0.0.1:8899".to_owned(),
        method: "getLatestBlockhash".to_owned(),
        caller_module: "executor".to_owned(),
        reason: RpcReason::ColdStart,
        category: RpcCallCategory::Blockhash,
        network_kind: RpcNetworkKind::JsonRpc,
        related_token: None,
        related_signature: None,
        estimated_provider_credit_cost: 1,
        actual_provider_credit_cost: None,
        config_hash: loaded.hash.clone(),
        run_id: loaded.config.environment.run_id.clone(),
        live_mode: true,
    });
    assert!(result.is_err());
}

#[test]
fn transaction_messages_survive_dedup_once() {
    let loaded = config();
    let mut service = GeyserIngestService::new(loaded.config.geyser.clone());
    let update = TransactionUpdate {
        slot: 2,
        signature: "sig-a".to_owned(),
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
    let first = service.process_envelope(GeyserEnvelope {
        received_at: OffsetDateTime::UNIX_EPOCH,
        observed_at_monotonic_ns: 1,
        source_latency_ms: None,
        message: GeyserMessage::Transaction(update.clone()),
    });
    let second = service.process_envelope(GeyserEnvelope {
        received_at: OffsetDateTime::UNIX_EPOCH,
        observed_at_monotonic_ns: 2,
        source_latency_ms: None,
        message: GeyserMessage::Transaction(update),
    });
    assert_eq!(first.len(), 1);
    assert!(second.is_empty());
}

#[test]
fn default_config_validates_stream_only_contract() {
    let loaded = config();
    let summary = loaded.validate_stream_only().expect("stream-only");
    assert!(summary.passed);
    assert!(summary.stream_only_enabled);
    assert_eq!(summary.rpc_budget_daily_limit, 0);
}

#[test]
fn workspace_has_no_hidden_rpc_or_http_client_paths() {
    let root = workspace_root().join("crates");
    let mut files = Vec::new();
    collect_rs_files(&root, &mut files);
    let forbidden = [
        "solana_client::",
        "RpcClient::",
        "JsonRpcClient",
        "reqwest::Client",
        "reqwest::get(",
        "hyper::Client",
    ];
    let mut hits = Vec::new();
    for path in files {
        let content = fs::read_to_string(&path).expect("source");
        for needle in forbidden {
            if content.contains(needle) {
                hits.push(format!("{}::{needle}", path.display()));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "unexpected direct RPC/HTTP client paths found: {hits:?}"
    );
}

#[test]
fn workspace_has_no_secret_like_literals_in_repo_artifacts() {
    let root = workspace_root();
    let mut files = Vec::new();
    for dir in [
        "config", "docs", "deploy", "crates", "fixtures", "reports", "data",
    ] {
        collect_all_files(&root.join(dir), &mut files);
    }
    let forbidden = vec![
        ["cf", "k_"].concat(),
        ["?", "api", "-", "key", "="].concat(),
        [".", "erpc", ".", "global"].concat(),
        ["Pass", "word", ":"].concat(),
        ["User", "name", ":"].concat(),
        ["I", "P", ":"].concat(),
        ["mainnet", ".", "helius", "-", "rpc", ".", "com"].concat(),
    ];
    let mut hits = Vec::new();
    for path in files {
        let Ok(bytes) = fs::read(&path) else {
            continue;
        };
        if bytes.len() > 5 * 1024 * 1024 || bytes.contains(&0) {
            continue;
        }
        let content = String::from_utf8_lossy(&bytes);
        for needle in &forbidden {
            if content.contains(needle) {
                hits.push(format!("{}::{needle}", path.display()));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "secret-like literals found in workspace artifacts: {hits:?}"
    );
}
