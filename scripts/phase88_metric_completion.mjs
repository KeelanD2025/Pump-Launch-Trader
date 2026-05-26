#!/usr/bin/env node

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

const args = process.argv.slice(2);
function argValue(name, fallback = null) {
  const index = args.indexOf(name);
  if (index === -1 || index + 1 >= args.length) return fallback;
  return args[index + 1];
}
function hasArg(name) {
  return args.includes(name);
}

const outputDir = path.resolve(repoRoot, argValue("--output-dir", "research_output/phase88_metric_completion"));
const freshRunId = argValue("--fresh-run-id", "");
const freshDerivedRunId = argValue("--fresh-derived-run-id", "");
const pinnedRunSetPath = argValue("--pinned-run-set", "research_output/phase84_labels/pinned_run_set.json");
const datasetIndexPath = argValue("--dataset-index", "data/dataset_index.json");

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}
function writeJson(file, value) {
  ensureDir(path.dirname(file));
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
}
function writeText(file, value) {
  ensureDir(path.dirname(file));
  fs.writeFileSync(file, value);
}
function csvEscape(value) {
  if (value === null || value === undefined) return "";
  const raw = Array.isArray(value) ? value.join(";") : String(value);
  if (/[",\n]/.test(raw)) return `"${raw.replaceAll('"', '""')}"`;
  return raw;
}
function writeCsv(file, headers, rows) {
  const body = [
    headers.join(","),
    ...rows.map((row) => headers.map((header) => csvEscape(row[header])).join(",")),
  ].join("\n");
  writeText(file, `${body}\n`);
}
function readJsonIfExists(file) {
  try {
    return JSON.parse(fs.readFileSync(path.resolve(repoRoot, file), "utf8"));
  } catch {
    return null;
  }
}

const families = [
  ["run_identity_timing", "run identity and timing", "Edge/replay run identifiers, timestamps, duration, runtime role.", "dataset index + runtime summaries", "edge_collector", "dataset_index/runtime_health", "run/replay records", "source_run.run_id, source_run.wall_clock_duration_seconds", "seconds/id", "", "Exact from runtime/dataset index; unavailable if run metadata missing.", "implemented", "crates/runtime + crates/cli", true, true, true, true, true, 1],
  ["r2_manifest_proof", "R2 / manifest proof", "Segment/artifact upload and manifest consistency proof.", "verify-r2-upload and manifest comparison", "edge_collector", "artifact_manifest, segment_manifest, dataset_index", "segments/manifests", "r2_and_manifest_proof.*", "count/status", "", "Exact from R2 verifier; unavailable if R2 manifest missing.", "implemented", "crates/r2-storage + crates/cli", true, true, true, true, true, 1],
  ["stream_only_rpc_proof", "stream-only / RPC proof", "Proof that edge path used streams only and made no market-data RPC calls.", "runtime RPC ledger + stream-only validator", "edge_collector", "runtime_health, rpc_ledger", "runtime audit", "stream_only_proof.*", "count/status", "", "Exact from ledger; unavailable if ledger absent.", "implemented", "crates/runtime + rpc_budget", true, true, true, true, true, 1],
  ["normalized_event_counts", "normalized event counts", "Counts of normalized event segment rows and event types.", "segment row counts + replay counters", "research_replay", "normalized_events", "all normalized event kinds", "event_counts.*", "count", "", "Exact if normalized segments verified.", "implemented", "crates/runtime + crates/cli", true, true, true, true, true, 1],
  ["pump_create_launch_metadata", "Pump create / launch metadata", "Mint, creator/dev, bonding curve accounts, metadata URI, launch transaction fingerprint.", "TokenCreated normalized events", "edge_collector", "normalized_events", "token_created", "token_counts, metadata_social_metrics.metadata_uri_observed", "mixed", "", "High when create decoded; unavailable when create event missing.", "implemented", "crates/runtime/src/live_source.rs", true, true, true, true, false, 1],
  ["bonding_curve_reserves", "bonding curve reserves", "Virtual/real SOL and token reserves from bonding curve account updates.", "decode BondingCurve account fields", "edge_collector", "normalized_events", "bonding_curve_update", "curve.*", "raw token/lamports", "virtual/real reserve pair", "High when account update decoded; unavailable before curve account is mapped.", "implemented", "crates/runtime/src/live_source.rs", true, true, true, true, false, 1],
  ["reserve_implied_price", "reserve-implied price", "Canonical Pump.fun price from matching virtual reserve pair.", "virtual_quote_reserves / virtual_token_reserves, unit converted", "research_replay", "normalized_events/features", "bonding_curve_update", "price_sol_per_token", "SOL/token", "virtual_token_reserves", "High when virtual reserves and decimals exist.", "implemented", "crates/features/src/metric_engine.rs", true, true, true, true, false, 0.95],
  ["realized_trade_price", "realized buy/sell trade price", "Actual trade price from decoded amounts or confidence-scored fallback.", "quote_amount / token_amount with source priority", "research_replay", "normalized_events", "pump_buy,pump_sell", "trade_price_source/effective_price", "SOL/token", "token amount", "Confidence depends on program amounts vs balance/lamport fallback.", "partial", "crates/runtime/src/live_source.rs + research replay", true, true, true, false, false, 0.8],
  ["ohlc_price_path", "OHLC price path", "Open/high/low/close canonical token price path.", "bucket canonical reserve/trade prices by mint/time", "research_replay", "feature chunks/canonical_price_path", "curve/trade events", "token_metrics.price_*", "SOL/token", "", "Unavailable where no canonical price point exists.", "implemented", "generate-canonical-labels", true, true, true, false, false, 0.8],
  ["market_cap", "market cap", "Market cap using price times 1B and price times total supply.", "price_sol_per_token * supply", "research_replay", "metric_engine/features", "curve updates", "market_cap_1b_sol, market_cap_total_supply_sol", "SOL", "supply", "Unavailable when price or supply missing.", "implemented", "crates/features/src/metric_engine.rs", true, true, true, false, false, 0.9],
  ["curve_progress", "curve progress", "Pump.fun curve progress separate from complete flag.", "100 - (((balance_ui - reserved_ui) * 100) / initial_real_token_reserves_ui)", "research_replay", "curve state features", "bonding_curve_update", "curve_progress_pct", "percent", "initial real token reserve convention", "Unavailable without real token reserves/decimals.", "implemented", "crates/features/src/metric_engine.rs", true, true, true, false, false, 0.75],
  ["buy_sell_flow", "buy/sell flow", "Buy/sell counts, volume, net flow by mint/window.", "aggregate PumpBuy/PumpSell canonical amounts", "research_replay", "normalized_events", "pump_buy,pump_sell", "flow.*", "SOL/token/count", "", "Confidence follows trade amount source.", "implemented", "crates/state + crates/features", true, true, true, false, false, 0.85],
  ["unique_buyers_sellers", "unique buyers / sellers", "Distinct buyer/seller wallets by token/window.", "count unique buyer/seller pubkeys", "research_replay", "normalized_events", "pump_buy,pump_sell", "unique_buyers, unique_sellers", "count", "", "High when trade wallet decoded.", "implemented", "crates/state", true, true, true, false, false, 0.9],
  ["holder_count", "holder count", "Owner-wallet holder count using owner-summed token accounts.", "count positive owner balances with explicit exclusions", "research_replay", "holder balance updates", "holder_balance_update", "holder_count_*", "count", "owner balance map", "Unavailable when owner mapping/denominator missing.", "partial", "crates/state + crates/features", true, true, true, false, false, 0.7],
  ["holder_growth", "holder growth", "Holder count/supply growth through time.", "delta holder count/supply over windows", "research_replay", "holder snapshots", "holder_balance_update", "holder_growth_rate", "rate", "prior snapshot", "Unavailable with insufficient holder history.", "partial", "crates/features", true, true, true, false, false, 0.65],
  ["holder_churn", "holder churn", "Wallet entries/exits and balance turnover.", "owner first/last seen and balance sign transitions", "research_replay", "holder snapshots", "holder_balance_update", "holder_churn", "rate/count", "holder history", "Unavailable with sparse holder events.", "partial", "crates/features", true, true, true, false, false, 0.6],
  ["holder_stickiness", "holder stickiness", "Persistence of holders across windows.", "retained owners / prior owners", "research_replay", "holder snapshots", "holder_balance_update", "holder_stickiness", "ratio", "prior owner set", "Unavailable without enough history.", "partial", "crates/features", true, true, true, false, false, 0.6],
  ["top_holder_concentration", "top-holder concentration", "Top owner balance concentration with explicit denominator variants.", "max owner balance / observed, circulating, total supply denominators", "research_replay", "holder state", "holder_balance_update", "top_holder_pct_*", "ratio", "explicit denominator", "Unavailable when denominator missing; never clamped.", "partial", "crates/state + crates/features", true, true, true, false, false, 0.7],
  ["dev_holdings", "dev holdings", "Creator/dev wallet holdings with denominator variants.", "sum dev token accounts / total or circulating supply", "research_replay", "create + holder state", "token_created,holder_balance_update", "dev_holding_pct_*", "ratio", "total/circulating supply", "Unavailable when creator or denominator missing.", "partial", "crates/features/src/metric_engine.rs", true, true, true, false, false, 0.75],
  ["bundle_detection", "bundle detection", "Bundle-like launch/flow behavior from stream heuristics and optional enrichment.", "same slot/shape/funder/account clusters", "enrichment_worker", "normalized_events + enrichment cache", "transactions,wallet funding", "bundle_risk/bundle_evidence", "score/evidence", "", "Partial from stream; confirmed evidence requires enrichment/RPC.", "requires_enrichment", "run-enrichment-worker", true, false, true, false, false, 0.4],
  ["common_funder", "common funder", "Wallets sharing funding source.", "wallet funding graph common ancestor", "enrichment_worker", "wallet funding enrichment", "wallet funding/RPC", "common_funder_count", "count", "", "Requires off-VPS wallet history/funding API.", "requires_enrichment", "run-enrichment-worker", false, false, false, false, false, 0],
  ["funding_graph", "funding graph", "Wallet funding graph and clusters.", "off-VPS wallet history traversal within budget", "enrichment_worker", "enrichment_cache", "RPC/API wallet history", "funding_graph.csv", "graph", "", "Requires explicit off-VPS budget and provider.", "requires_enrichment", "run-enrichment-worker", false, false, false, false, false, 0],
  ["wallet_age_history", "wallet age / wallet history", "Wallet age and historical behavior summary.", "off-VPS history query/cache", "enrichment_worker", "enrichment_cache", "RPC/API wallet history", "wallet_age_summary.csv", "time/count", "", "Requires explicit off-VPS enrichment provider/budget.", "requires_enrichment", "run-enrichment-worker", false, false, false, false, false, 0],
  ["transaction_fingerprints", "transaction fingerprints", "Signer/account/instruction/fee shape fingerprints.", "hash account list and instruction shape; cluster by slot/shape", "edge_collector", "normalized_events", "observed_transaction", "account_list_hash,instruction_shape_hash", "hash/count", "", "Partial if provider lacks full tx meta.", "partial", "crates/runtime/src/live_source.rs", true, true, true, false, false, 0.75],
  ["fees_priority_compute", "fee / priority fee / compute budget", "Base tx fee, compute limit/price, estimated priority fee.", "decode compute budget instructions and tx meta fee", "edge_collector", "normalized_events", "observed_transaction,pump_buy,pump_sell", "compute_unit_*, fee_lamports", "lamports/CU", "", "High when transaction meta present.", "implemented", "crates/runtime/src/live_source.rs", true, true, true, false, false, 0.9],
  ["jito_bundle_evidence", "Jito/bundle-like evidence where observable", "Observable bundle-like signals without private mempool claim.", "same-slot/same-shape/priority fee clustering", "research_replay", "normalized_events", "observed_transaction", "bundle_like_evidence", "evidence", "", "Provider does not expose private bundle confirmation; enrichment required for stronger proof.", "partial", "features/enrichment", true, false, true, false, false, 0.35],
  ["fake_momentum", "fake momentum", "Momentum quality/risk from flow, holders, concentration, and price.", "risk formula over flow/holder/price features", "research_replay", "features", "curve/trade/holder", "fake_momentum_risk", "score", "", "Confidence follows input coverage.", "implemented", "crates/features + crates/risk", true, true, true, false, false, 0.75],
  ["sell_absorption", "sell absorption", "Buy absorption after sells and sell pressure resilience.", "post-sell buy/price response windows", "research_replay", "features", "pump_sell,pump_buy,price", "absorption_score", "score", "", "Unavailable without canonical price/flow window.", "partial", "crates/features", true, true, true, false, false, 0.65],
  ["cost_basis_profit_pressure", "cost basis / profit pressure", "Holder/token cost basis and profit overhang.", "position cost basis from observed buys/sells", "research_replay", "flow + holder state", "pump_buy,pump_sell,holder", "cost_basis/profit_pressure", "SOL/ratio", "", "Partial because wallets before observation window are unknown.", "partial", "crates/features", true, true, true, false, false, 0.55],
  ["cohort_relative_strength", "cohort relative strength", "Token strength relative to contemporaneous launches.", "rank/percentile within launch cohort", "research_replay", "features", "create/trade/curve", "cohort_rank_*", "rank/percentile", "cohort set", "Unavailable with sparse cohort window.", "implemented", "crates/features", true, true, true, false, false, 0.75],
  ["rug_detection", "rug detection", "Collapse/dev/top-holder sell/rug risk signals.", "risk rules over sell/price/dev/top-holder data", "research_replay", "features", "sell,holder,price", "rug_risk", "score/flag", "", "Confidence follows price/holder coverage.", "implemented", "crates/risk + crates/features", true, true, true, false, false, 0.7],
  ["early_sell_defense", "early sell defense", "Tentative sell/early exit defense metrics.", "deshred/raw/replay tentative sell resolution", "deshred_worker", "shred_exit artifacts", "tentative sell events", "early_sell_defense.*", "score/count", "", "Unavailable unless deshred/raw path enabled; not used by current edge collector.", "requires_deshred", "crates/runtime/src/shred_exit.rs", true, false, false, false, false, 0],
  ["deshred_metrics", "deshred metrics", "Pre-execution/deshred source metrics.", "provider deshred feed decoding", "deshred_worker", "deshred artifacts", "deshred updates", "deshred_*", "mixed", "", "Requires deshred provider feed.", "requires_deshred", "crates/runtime/src/shred_exit.rs", true, false, false, false, false, 0],
  ["raw_shred_metrics", "raw-shred metrics", "Raw shred/packet-level metrics.", "raw shred capture/decode", "raw_shred_worker", "raw shred artifacts", "raw shred packets", "raw_shred_*", "mixed", "", "Requires raw shred capture/provider.", "requires_raw_shred", "ingest-shred", true, false, false, false, false, 0],
  ["metadata_uri", "metadata URI", "Observed metadata URI from create instruction.", "TokenCreated uri", "edge_collector", "normalized_events", "token_created", "metadata_uri_observed", "url", "", "High when create decoded.", "implemented", "crates/runtime/src/live_source.rs", true, true, true, false, false, 0.9],
  ["metadata_socials", "website / X / Telegram / Discord", "Fetched metadata/social links.", "HTTP metadata fetch + JSON parse", "enrichment_worker", "enrichment_cache", "metadata URI HTTP", "has_website,has_twitter,has_telegram,has_discord", "bool/count", "", "Requires explicit off-VPS HTTP budget; may be unavailable for dead URIs.", "requires_enrichment", "run-enrichment-worker", false, false, true, false, false, 0],
  ["social_count", "social count", "Count of present social links.", "sum website/X/Telegram/Discord booleans", "enrichment_worker", "metadata social enrichment", "metadata URI HTTP", "social_count", "count", "", "Requires metadata_social enrichment.", "requires_enrichment", "run-enrichment-worker", false, false, true, false, false, 0],
  ["mfe_mae", "MFE / MAE", "Max favorable/adverse excursion labels from canonical prices.", "max/min price move from canonical price path", "research_replay", "canonical_price_path", "curve/trade price points", "mfe_pct,mae_pct", "percent", "entry or first price", "Unavailable when path/horizon insufficient.", "implemented", "generate-canonical-labels", true, true, true, false, false, 0.65],
  ["return_window_labels", "return-window labels", "Forward return windows for tokens/trades.", "price(t+horizon)/price(t)-1", "research_replay", "canonical_price_path", "curve/trade price points", "return_5s..return_10m", "percent", "start price", "Unavailable when horizon exceeds observed path.", "implemented", "generate-canonical-labels", true, true, true, false, false, 0.65],
  ["missed_winners", "missed winners", "Untraded/rejected tokens that later had executable upside.", "labels + decisions + fee model", "research_replay", "labels + decisions", "price labels, decisions", "missed_winners.csv", "count/class", "", "Requires canonical labels; unavailable labels remain uncertain.", "implemented", "scripts/phase85_diagnostic_lab.mjs", true, true, true, false, false, 0.6],
  ["false_discards", "false discards", "Discarded tokens later showing executable upside.", "labels + discard decisions", "research_replay", "labels + decisions", "price labels, decisions", "false_discards.csv", "count/class", "", "Requires canonical labels.", "implemented", "scripts/phase85_diagnostic_lab.mjs", true, true, true, false, false, 0.6],
  ["decisions_rejections", "decisions / rejections", "Strategy decisions and blockers.", "decision engine output", "research_replay", "decisions", "feature snapshots", "decision_counts, rejection reasons", "count/reason", "", "High when research replay generated decisions.", "implemented", "crates/decision", true, true, true, false, false, 0.9],
  ["fills_exits", "fills / exits", "Paper/live fills and exits; live disabled by default.", "executor/sim fill events", "research_replay", "fills", "decisions/fills", "fill_count, exit reasons", "count/SOL", "", "High for paper sim; no live fills unless enabled explicitly.", "implemented", "crates/sim + crates/executor", true, true, true, false, false, 0.9],
  ["raw_pnl", "raw PnL", "Audit PnL including artifact fills.", "sum fill net_pnl_quote raw", "research_replay", "fills", "fill events", "raw_pnl_total", "SOL", "", "Audit-only; may include artifact fills.", "implemented", "crates/sim + dossiers", true, true, false, false, false, 0.9],
  ["executable_pnl", "executable PnL", "Headline PnL excluding non-executable artifacts.", "sum executable fill PnL after sanity filter", "research_replay", "fills + pnl sanity", "fill events", "executable_pnl_total", "SOL", "", "High only after artifact sanity classification.", "implemented", "crates/sim + phase73 sanity", true, true, true, false, false, 0.9],
  ["baseline_strategy_metrics", "baseline strategy metrics", "Offline baseline comparisons.", "simulate fixed baselines on same costs/data filters", "research_replay", "diagnostic outputs", "features/labels", "baseline_comparison", "mixed", "", "Diagnostic only until labels/sample size improve.", "implemented", "quant diagnostics", true, true, false, false, false, 0.55],
  ["regime_metrics", "regime metrics", "PnL/features by market/data/risk regime.", "bucket features and aggregate outcomes", "research_replay", "diagnostic outputs", "features/labels", "regime_diagnostic", "mixed", "", "Confidence follows metric coverage.", "implemented", "quant diagnostics", true, true, false, false, false, 0.55],
  ["execution_sensitivity", "execution sensitivity", "Latency/slippage/fee/size stress tests.", "rerun fills under execution assumptions", "research_replay", "fills/labels", "fills", "execution_sensitivity", "mixed", "", "Diagnostic only on small sample.", "implemented", "quant diagnostics", true, true, false, false, false, 0.6],
  ["readiness_backtest_gates", "readiness / backtest gates", "Contract-gated diagnostic/backtest/tuning readiness.", "metric contract + parity + sample gates", "research_replay", "reports/dataset index", "all families", "readiness_v2, full_metric_completion_gate", "bool/reason", "", "Authoritative gate; never infer readiness from PnL alone.", "implemented", "validate-backtest-readiness-v2 + validate-metric-contract", true, true, true, false, false, 1],
  ["post_migration_pumpswap", "post-migration / PumpSwap metrics", "Post-migration pool detection, price, and volume.", "detect migration and PumpSwap pool; post-migration pricing if supported", "enrichment_worker", "normalized_events + enrichment", "migration/pool events", "pumpswap_*", "mixed", "pool reserves", "Currently unsupported unless PumpSwap support/enrichment is enabled.", "unsupported", "run-enrichment-worker", true, false, false, false, false, 0],
];

function contractRows() {
  return families.map((row, index) => ({
    metric_id: row[0],
    metric_name: row[1],
    metric_family: row[1],
    definition: row[2],
    formula_or_algorithm: row[3],
    required_layer: row[4],
    source_artifact: row[5],
    required_event_types: row[6],
    required_accounts: requiredAccounts(row[0]),
    output_field: row[7],
    unit: row[8],
    denominator: row[9],
    confidence_logic: row[10],
    unavailable_reason_if_missing: unavailableReason(row),
    implementation_status: row[11],
    code_owner_module: row[12],
    parity_test_required: row[13],
    live_collection_required: row[14],
    safe_for_diagnostics: row[15],
    safe_for_backtest: row[16],
    safe_for_threshold_tuning: row[17],
    coverage_requirement_for_tuning: row[18],
    ordinal: index + 1,
  }));
}

function requiredAccounts(metricId) {
  if (metricId.includes("holder") || metricId === "dev_holdings") return "mint token accounts, owners, curve/program/burn exclusions";
  if (metricId.includes("curve") || metricId.includes("price") || metricId.includes("market_cap")) return "mint, bonding curve, associated bonding curve where available";
  if (metricId.includes("funding") || metricId.includes("wallet")) return "wallets, funders, signatures";
  return "";
}

function unavailableReason(row) {
  const status = row[11];
  if (status === "requires_enrichment") return "Requires explicit off-VPS enrichment budget/provider; unavailable in stream-only edge data.";
  if (status === "requires_deshred") return "Requires deshred provider/feed; unavailable in current stream-only proof unless enabled.";
  if (status === "requires_raw_shred") return "Requires raw shred feed/capture; unavailable in current edge proof.";
  if (status === "unsupported") return "Unsupported currently; requires post-migration/PumpSwap implementation.";
  if (status === "partial") return "Unavailable where required source fields, denominators, history, or label horizons are missing.";
  return row[10] || "Required source artifact missing.";
}

function sourceFieldRows() {
  const present = (field, source, artifact, note = "") => ({ field, status: "present", source, artifact, note, required_for_fresh_proof: true });
  const partial = (field, source, artifact, note = "") => ({ field, status: "partial", source, artifact, note, required_for_fresh_proof: true });
  const unavailable = (field, reason, note = "") => ({ field, status: "unavailable", source: "", artifact: "", note: `${reason}${note ? `; ${note}` : ""}`, required_for_fresh_proof: false });
  return [
    present("source_run_id", "segment StoredRecord/EdgeEventRecord", "normalized_events/source_events"),
    present("timestamp", "EventMeta.received_at_wall_time/block_time", "normalized_events"),
    present("slot", "EventMeta.slot", "normalized_events"),
    present("sequence", "segment record sequence_number", "normalized_events/source_events"),
    present("signature", "EventMeta.signature or payload signature", "normalized_events"),
    partial("signer", "ObservedTransaction.signer after schema v2; fallback first account key", "normalized_events", "provider must expose account keys"),
    present("instruction_type", "EventPayload kind and decoded instruction name", "normalized_events"),
    present("mint", "payload.mint()", "normalized_events"),
    present("creator/dev wallet", "TokenCreated.creator_wallet", "normalized_events"),
    present("bonding curve account", "TokenCreated/BondingCurveUpdate context", "normalized_events"),
    partial("associated bonding curve account", "TokenCreated.associated_bonding_curve_account when decoded", "normalized_events", "not always decoded by current IDL/account map"),
    present("token account", "HolderBalanceUpdate.token_account", "normalized_events"),
    present("owner wallet", "HolderBalanceUpdate.owner_wallet", "normalized_events"),
    present("virtual_sol_reserves", "BondingCurveUpdate.virtual_quote_reserves", "normalized_events"),
    present("virtual_token_reserves", "BondingCurveUpdate.virtual_token_reserves", "normalized_events"),
    present("real_sol_reserves", "BondingCurveUpdate.real_quote_reserves", "normalized_events"),
    present("real_token_reserves", "BondingCurveUpdate.real_token_reserves", "normalized_events"),
    partial("token_total_supply", "schema v2 token_total_supply_raw/ui or Pump.fun constant fallback", "normalized_events/features"),
    present("token_decimals", "BondingCurveUpdate/HolderBalanceUpdate token_decimals", "normalized_events"),
    present("curve_complete_flag", "BondingCurveUpdate.curve_complete_flag", "normalized_events"),
    partial("migrated/post-migration flag", "TokenTerminal migrated or curve complete flag", "normalized_events", "PumpSwap pool mapping remains unsupported"),
    present("buy/sell direction", "PumpBuy/PumpSell payload kind", "normalized_events"),
    partial("token amount raw", "PumpBuy/PumpSell token_out/token_in; schema v2 amount source fields", "normalized_events"),
    partial("token amount ui", "computed from raw + decimals where available", "research replay"),
    partial("quote amount lamports", "PumpBuy/PumpSell quote_in/quote_out; schema v2 source fields", "normalized_events"),
    partial("quote amount SOL", "research replay unit conversion", "features"),
    partial("pre/post token balances", "TransactionUpdate token balances -> HolderBalanceUpdate old/new", "normalized_events"),
    partial("pre/post SOL balances", "used for lamport fallback; not fully persisted per account", "normalized_events", "store only derived spend/gain today"),
    present("compute unit limit", "PumpBuy/PumpSell/ObservedTransaction compute budget", "normalized_events"),
    present("compute unit price", "PumpBuy/PumpSell/ObservedTransaction compute budget", "normalized_events"),
    present("priority fee", "estimated_priority_fee_lamports", "normalized_events"),
    present("tx fee", "estimated_base_fee_lamports/ObservedTransaction.tx_fee_lamports", "normalized_events"),
    present("account list hash", "schema v2 ObservedTransaction.account_list_hash", "normalized_events"),
    present("instruction shape hash", "schema v2 ObservedTransaction.instruction_shape_hash", "normalized_events"),
    present("failed transaction flag", "TransactionStatus / ObservedTransaction.failed_transaction", "normalized_events"),
    partial("error code if failed", "schema v2 TransactionUpdate.error_code if provider sends tx err", "normalized_events"),
    present("program id", "ObservedTransaction.program_ids", "normalized_events"),
    unavailable("pool/migration account if observed", "PumpSwap/post-migration support not implemented", "requires post_migration_pumpswap enrichment/decoder path"),
  ];
}

function researchRows(contract) {
  return contract.map((row) => ({
    metric_id: row.metric_id,
    metric_family: row.metric_family,
    required_layer: row.required_layer,
    implementation_status: row.implementation_status,
    code_owner_module: row.code_owner_module,
    research_derivable: ["research_replay", "edge_collector"].includes(row.required_layer),
    remaining_gap: row.implementation_status === "implemented" ? "" : row.unavailable_reason_if_missing,
    output_field: row.output_field,
    parity_test_required: row.parity_test_required,
  }));
}

function enrichmentReport() {
  const env = process.env;
  const profiles = [
    { profile: "metadata_social", can_plan: true, required_env: [], required_budget: "max_http_calls > 0", status: "implemented_budgeted_canary", unlocks: ["website", "X/Twitter", "Telegram", "Discord", "social_count"] },
    { profile: "funding_bundle", can_plan: true, required_env: ["RPC_URL or provider-specific wallet history API"], required_budget: "max_rpc_calls/max_rpc_credits > 0", status: env.RPC_URL ? "ready_with_budget" : "blocked_missing_provider_or_budget", unlocks: ["first_funder", "common_funder", "funding_graph", "wallet_history", "bundle_evidence"] },
    { profile: "holder_denominator_repair", can_plan: true, required_env: ["RPC_URL"], required_budget: "selected snapshots + max_rpc_calls > 0", status: env.RPC_URL ? "ready_with_budget" : "blocked_missing_rpc_url", unlocks: ["mint supply/decimals repair", "owner mapping repair"] },
    { profile: "transaction_fingerprint_repair", can_plan: true, required_env: ["RPC_URL"], required_budget: "max_rpc_calls > 0", status: env.RPC_URL ? "ready_with_budget" : "blocked_missing_rpc_url", unlocks: ["account shape", "instruction shape", "same-slot cluster repair"] },
    { profile: "post_migration_pumpswap", can_plan: true, required_env: ["RPC_URL", "PumpSwap decoder/support"], required_budget: "max_rpc_calls > 0", status: "unsupported_requires_pumpswap_support", unlocks: ["post-migration price", "post-migration volume"] },
  ];
  return {
    schema_version: "phase88.enrichment_worker.v1",
    off_vps_only: true,
    hidden_rpc_calls: false,
    secret_values_printed: false,
    profiles,
    blockers: profiles.filter((p) => p.status.startsWith("blocked") || p.status.startsWith("unsupported")),
  };
}

function deshredDecision() {
  const rows = [
    { metric_family: "early sell defense", required_provider_feed: "deshred or raw shred feed", current_vps_receives_it: "optional/not required in current edge-only proof", can_be_added_edge_only: true, latency_storage_impact: "medium latency benefit, low persisted summary if disabled-by-default", needed_for_current_strategy: false, implementation_status: "implemented_disabled" },
    { metric_family: "deshred metrics", required_provider_feed: "deshred transaction feed", current_vps_receives_it: "not active unless configured", can_be_added_edge_only: true, latency_storage_impact: "medium", needed_for_current_strategy: false, implementation_status: "unavailable_requires_deshred" },
    { metric_family: "raw shred metrics", required_provider_feed: "raw shred packets/entries", current_vps_receives_it: "not active", can_be_added_edge_only: false, latency_storage_impact: "high storage/decoder cost", needed_for_current_strategy: false, implementation_status: "unavailable_requires_raw_shred" },
  ];
  return {
    schema_version: "phase88.deshred_rawshred_decision.v1",
    rows,
    summary: "Do not pretend raw/deshred metrics exist. Hooks remain disabled/unavailable unless a provider feed is explicitly enabled and budgeted.",
  };
}

function loadPinnedRuns() {
  const value = readJsonIfExists(pinnedRunSetPath);
  if (!value) return [];
  const rawRuns = Array.isArray(value) ? value : value.runs || value.pinned_runs || value.selected_runs || [];
  return rawRuns.map((run) => ({
    source_run_id: run.source_run_id || run.run_id || run.source || "",
    derived_run_id: run.derived_run_id || run.research_latest_derived_run_id || run.derived || "",
  })).filter((run) => run.source_run_id);
}

function loadDatasetIndex() {
  const local = readJsonIfExists(datasetIndexPath);
  if (local) return local;
  return { runs: [] };
}

function contractValidation(contract, runId) {
  return contract.map((row) => ({
    source_run_id: runId || "",
    metric_id: row.metric_id,
    metric_family: row.metric_family,
    implementation_status: row.implementation_status,
    required_layer: row.required_layer,
    can_use_for_diagnostics: row.safe_for_diagnostics && row.implementation_status !== "missing",
    can_use_for_backtest: row.safe_for_backtest && ["implemented", "partial"].includes(row.implementation_status),
    can_use_for_tuning: row.safe_for_threshold_tuning && row.implementation_status === "implemented",
    blocker: row.safe_for_threshold_tuning && row.implementation_status !== "implemented" ? row.unavailable_reason_if_missing : "",
  }));
}

function markdownContract(contract) {
  const rows = contract.map((row) => `| ${row.ordinal} | ${row.metric_family} | ${row.required_layer} | ${row.implementation_status} | ${row.code_owner_module} |`).join("\n");
  return `# Pump.fun Metric Contract\n\nThis is the authoritative Phase 88 metric contract. Missing values are unavailable, never zero. Threshold tuning is blocked unless this contract, real-run parity, sample-size, and walk-forward gates pass.\n\n| # | Metric family | Required layer | Status | Code owner |\n|---:|---|---|---|---|\n${rows}\n`;
}

function tomlContract(contract) {
  const lines = [
    'schema_version = "phase88.metric_contract.v1"',
    'authority = "docs/PUMPFUN_METRIC_CONTRACT.md"',
    'missing_value_policy = "unavailable_not_zero"',
    "threshold_tuning_requires_full_contract = true",
    "",
  ];
  for (const row of contract) {
    lines.push("[[metric]]");
    for (const key of ["metric_id", "metric_name", "metric_family", "required_layer", "source_artifact", "required_event_types", "required_accounts", "output_field", "unit", "denominator", "implementation_status", "code_owner_module", "unavailable_reason_if_missing"]) {
      lines.push(`${key} = ${JSON.stringify(String(row[key] ?? ""))}`);
    }
    for (const key of ["parity_test_required", "live_collection_required", "safe_for_diagnostics", "safe_for_backtest", "safe_for_threshold_tuning"]) {
      lines.push(`${key} = ${row[key] ? "true" : "false"}`);
    }
    lines.push(`coverage_requirement_for_tuning = ${Number(row.coverage_requirement_for_tuning)}`);
    lines.push("");
  }
  return lines.join("\n");
}

function summarizeStatuses(contract) {
  const counts = {};
  for (const row of contract) counts[row.implementation_status] = (counts[row.implementation_status] || 0) + 1;
  return counts;
}

function main() {
  ensureDir(outputDir);
  const contract = contractRows();
  const sourceFields = sourceFieldRows();
  const enrichment = enrichmentReport();
  const deshred = deshredDecision();
  const pinnedRuns = loadPinnedRuns();
  const datasetIndex = loadDatasetIndex();
  const freshProofBlocked = !freshRunId;
  const streamImplemented = contract.filter((row) => row.required_layer === "edge_collector" && row.implementation_status === "implemented").length;
  const researchImplemented = contract.filter((row) => row.required_layer === "research_replay" && ["implemented", "partial"].includes(row.implementation_status)).length;
  const missing = contract.filter((row) => ["missing", "scaffold_only"].includes(row.implementation_status));
  const partial = contract.filter((row) => row.implementation_status === "partial");
  const enrichmentRequired = contract.filter((row) => row.implementation_status === "requires_enrichment");
  const rawRequired = contract.filter((row) => row.implementation_status === "requires_raw_shred");
  const deshredRequired = contract.filter((row) => row.implementation_status === "requires_deshred");

  writeJson(path.join(outputDir, "metric_contract.json"), { schema_version: "phase88.metric_contract.v1", metrics: contract, status_counts: summarizeStatuses(contract), no_metric_family_omitted: contract.length >= 50 });
  writeCsv(path.join(outputDir, "metric_contract.csv"), Object.keys(contract[0]), contract);
  writeText(path.join(outputDir, "metric_contract.md"), markdownContract(contract));
  writeText(path.join(repoRoot, "docs/PUMPFUN_METRIC_CONTRACT.md"), markdownContract(contract));
  writeText(path.join(repoRoot, "config/metric_contract.toml"), tomlContract(contract));

  writeJson(path.join(outputDir, "edge_source_field_audit.json"), { schema_version: "phase88.edge_source_field_audit.v1", edge_schema_version_required: 2, fields: sourceFields, present: sourceFields.filter((r) => r.status === "present").length, partial: sourceFields.filter((r) => r.status === "partial").length, unavailable: sourceFields.filter((r) => r.status === "unavailable").length });
  writeCsv(path.join(outputDir, "edge_source_field_matrix.csv"), Object.keys(sourceFields[0]), sourceFields);
  writeText(path.join(outputDir, "edge_source_field_audit.md"), `# Phase 88 Edge Source Field Audit\n\n- present: ${sourceFields.filter((r) => r.status === "present").length}\n- partial: ${sourceFields.filter((r) => r.status === "partial").length}\n- unavailable: ${sourceFields.filter((r) => r.status === "unavailable").length}\n\nEvery unavailable stream field has an explicit provider/data requirement in the CSV/JSON matrix.\n`);

  const research = researchRows(contract);
  writeJson(path.join(outputDir, "research_metric_completion.json"), { schema_version: "phase88.research_metric_completion.v1", rows: research, no_research_derivable_metric_silently_zeroed: true });
  writeCsv(path.join(outputDir, "research_metric_matrix.csv"), Object.keys(research[0]), research);
  writeText(path.join(outputDir, "research_metric_completion.md"), `# Phase 88 Research Metric Completion\n\n- research/stream rows tracked: ${research.length}\n- implemented: ${research.filter((r) => r.implementation_status === "implemented").length}\n- partial: ${research.filter((r) => r.implementation_status === "partial").length}\n- requires enrichment/raw/deshred/unsupported: ${research.filter((r) => !["implemented", "partial"].includes(r.implementation_status)).length}\n\nMissing data is unavailable, never zero.\n`);

  writeJson(path.join(outputDir, "enrichment_worker.json"), enrichment);
  writeJson(path.join(outputDir, "enrichment_readiness.json"), enrichment);
  writeText(path.join(outputDir, "enrichment_worker.md"), `# Phase 88 Enrichment Worker\n\n- off_vps_only: true\n- profiles: ${enrichment.profiles.length}\n- blockers: ${enrichment.blockers.length}\n\nProfiles are budgeted/cached/resumable by contract. Funding graph, common funder, wallet history, bundle confirmations, metadata/socials, holder denominator repair, transaction fingerprint repair, and PumpSwap support remain explicit enrichment outputs, not hidden edge work.\n`);
  writeText(path.join(outputDir, "enrichment_readiness.md"), `# Phase 88 Enrichment Readiness\n\n${enrichment.profiles.map((p) => `- ${p.profile}: ${p.status}`).join("\n")}\n`);

  writeJson(path.join(outputDir, "deshred_rawshred_decision.json"), deshred);
  writeText(path.join(outputDir, "deshred_rawshred_decision.md"), `# Phase 88 Deshred / Raw-Shred Decision\n\n${deshred.rows.map((r) => `- ${r.metric_family}: ${r.implementation_status}`).join("\n")}\n`);

  const freshProof = {
    schema_version: "phase88.fresh_edge_run_proof.v1",
    fresh_post_change_run_required: true,
    fresh_run_id: freshRunId || null,
    fresh_derived_run_id: freshDerivedRunId || null,
    status: freshProofBlocked ? "blocked_missing_fresh_post_change_run" : "fresh_run_supplied_pending_external_verification",
    blocker: freshProofBlocked ? "A fresh post-change VPS edge run must be deployed, collected, uploaded, and verified before collection correctness can be claimed." : null,
    no_live_trading: true,
    no_live_path_rpc_polling: true,
  };
  writeJson(path.join(outputDir, "fresh_edge_run_proof.json"), freshProof);
  writeText(path.join(outputDir, "fresh_edge_run_proof.md"), `# Phase 88 Fresh Edge Run Proof\n\n- status: \`${freshProof.status}\`\n- fresh_run_id: \`${freshRunId || "missing"}\`\n\n${freshProof.blocker || "Fresh run id supplied. Verify segment integrity, R2 consistency, stream-only proof, and source-field coverage before claiming collection correctness."}\n`);

  const validationRows = contractValidation(contract, freshRunId);
  writeCsv(path.join(outputDir, "fresh_metric_contract_validation.csv"), Object.keys(validationRows[0]), validationRows);
  writeCsv(path.join(outputDir, "fresh_metric_parity_by_family.csv"), ["metric_family", "status", "required_fix"], contract.map((row) => ({ metric_family: row.metric_family, status: freshProofBlocked ? "not_proven_on_fresh_run" : row.implementation_status, required_fix: freshProofBlocked ? "collect fresh post-change edge run and replay from R2" : row.unavailable_reason_if_missing })));
  writeJson(path.join(outputDir, "fresh_replay_contract_validation.json"), { schema_version: "phase88.fresh_replay_contract_validation.v1", status: freshProofBlocked ? "blocked" : "pending_validation", fresh_run_id: freshRunId || null, validation_rows: validationRows });
  writeText(path.join(outputDir, "fresh_replay_contract_validation.md"), `# Phase 88 Fresh Replay Contract Validation\n\n- status: ${freshProofBlocked ? "`blocked`" : "`pending_validation`"}\n- reason: ${freshProofBlocked ? "fresh post-change run not supplied" : "fresh run supplied; replay/parity must be checked"}\n`);
  writeJson(path.join(outputDir, "fresh_backtest_readiness_v2.json"), { diagnostics_allowed: !freshProofBlocked, formal_backtesting_allowed: false, threshold_tuning_allowed: false, blockers: freshProofBlocked ? ["fresh post-change edge run proof missing"] : ["sample/fill/coverage gates still required"] });

  writeJson(path.join(outputDir, "contract_comparison.json"), { schema_version: "phase88.contract_comparison.v1", pinned_runs: pinnedRuns, fresh_run_id: freshRunId || null, old_replays_missing_fields_because_old_schema: true, fresh_runs_collecting_more_fields: !freshProofBlocked, metrics_requiring_new_collection: sourceFields.filter((r) => r.required_for_fresh_proof && r.status !== "present").map((r) => r.field), metrics_requiring_enrichment: enrichmentRequired.map((r) => r.metric_family), metrics_requiring_raw_shred: rawRequired.map((r) => r.metric_family), metrics_requiring_deshred: deshredRequired.map((r) => r.metric_family) });
  writeText(path.join(outputDir, "contract_comparison.md"), `# Phase 88 Contract Comparison\n\n- pinned runs inspected: ${pinnedRuns.length}\n- fresh run supplied: ${freshRunId ? "yes" : "no"}\n- old replays can miss fields because they were collected before schema completion: yes\n- metrics requiring enrichment: ${enrichmentRequired.length}\n- metrics requiring raw/deshred: ${rawRequired.length + deshredRequired.length}\n`);

  const gate = {
    schema_version: "phase88.full_metric_completion_gate.v1",
    collecting_all_stream_available_metrics_now: !freshProofBlocked && sourceFields.every((r) => r.status !== "unavailable" || !r.required_for_fresh_proof),
    computing_all_research_derivable_metrics_now: missing.length === 0,
    enrichment_required_metrics_implemented_and_ready: enrichment.blockers.length === 0,
    implemented_metric_families: contract.filter((r) => r.implementation_status === "implemented").map((r) => r.metric_family),
    partial_metric_families: partial.map((r) => r.metric_family),
    missing_metric_families: missing.map((r) => r.metric_family),
    enrichment_required_families: enrichmentRequired.map((r) => r.metric_family),
    raw_shred_required_families: rawRequired.map((r) => r.metric_family),
    deshred_required_families: deshredRequired.map((r) => r.metric_family),
    diagnostic_backtesting_allowed: !freshProofBlocked,
    formal_backtesting_allowed: false,
    threshold_tuning_allowed: false,
    blockers: [
      ...(freshProofBlocked ? ["fresh post-change edge collection proof missing"] : []),
      ...partial.map((r) => `${r.metric_family}: partial (${r.unavailable_reason_if_missing})`),
      ...enrichmentRequired.map((r) => `${r.metric_family}: requires enrichment`),
      ...rawRequired.map((r) => `${r.metric_family}: requires raw shred`),
      ...deshredRequired.map((r) => `${r.metric_family}: requires deshred`),
      "threshold tuning requires multiple fresh corrected-math runs, enough executable fills, label coverage, no outlier domination, and walk-forward split",
    ],
  };
  writeJson(path.join(outputDir, "full_metric_completion_gate.json"), gate);
  writeText(path.join(outputDir, "full_metric_completion_gate.md"), `# Phase 88 Full Metric Completion Gate\n\n- diagnostic_backtesting_allowed: \`${gate.diagnostic_backtesting_allowed}\`\n- formal_backtesting_allowed: \`${gate.formal_backtesting_allowed}\`\n- threshold_tuning_allowed: \`${gate.threshold_tuning_allowed}\`\n- implemented families: ${gate.implemented_metric_families.length}\n- partial families: ${gate.partial_metric_families.length}\n- enrichment required families: ${gate.enrichment_required_families.length}\n\nThreshold tuning remains false unless the full contract, parity, sample, label, and walk-forward gates pass.\n`);

  const projection = {
    schema_version: "phase88.dataset_index_projection.v1",
    metric_contract_version: "phase88.metric_contract.v1",
    edge_source_schema_version: 2,
    fresh_metric_collection_proof_run_id: freshRunId || null,
    implemented_metric_families: gate.implemented_metric_families,
    partial_metric_families: gate.partial_metric_families,
    missing_metric_families: gate.missing_metric_families,
    enrichment_required_families: gate.enrichment_required_families,
    raw_shred_required_families: gate.raw_shred_required_families,
    deshred_required_families: gate.deshred_required_families,
    diagnostic_backtesting_allowed: gate.diagnostic_backtesting_allowed,
    formal_backtesting_allowed: false,
    threshold_tuning_allowed: false,
    dataset_index_runs_seen: Array.isArray(datasetIndex.runs) ? datasetIndex.runs.length : 0,
  };
  writeJson(path.join(outputDir, "dataset_index_projection.json"), projection);
  writeText(path.join(outputDir, "dataset_index_projection.md"), `# Phase 88 Dataset Index Projection\n\nThis local projection is safe to merge only after fresh run proof and report upload verification.\n\n- metric_contract_version: ${projection.metric_contract_version}\n- fresh_metric_collection_proof_run_id: ${projection.fresh_metric_collection_proof_run_id || "missing"}\n- threshold_tuning_allowed: false\n`);

  console.log(JSON.stringify({
    output_dir: outputDir,
    metric_count: contract.length,
    status_counts: summarizeStatuses(contract),
    fresh_run_id: freshRunId || null,
    diagnostic_backtesting_allowed: gate.diagnostic_backtesting_allowed,
    formal_backtesting_allowed: gate.formal_backtesting_allowed,
    threshold_tuning_allowed: gate.threshold_tuning_allowed,
  }, null, 2));
}

main();
