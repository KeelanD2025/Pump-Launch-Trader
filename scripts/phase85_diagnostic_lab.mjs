#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";

const ROOT = process.cwd();
const OUT = path.join(ROOT, "research_output/phase85_diagnostic_lab");
const PHASE84_LABELS = path.join(ROOT, "research_output/phase84_labels");
const PHASE84_DIAG = path.join(ROOT, "research_output/phase84_diagnostic_backtest");

const PINNED = [
  {
    source_run_id: "live-paper-1779701936279767094",
    derived_run_id: "paper-replay-1779708848035124000",
    records: 32473,
    features: 21435,
    decisions: 21435,
    fills: 4,
    pnl: -1.1054748778199315,
  },
  {
    source_run_id: "live-paper-1779703846887506044",
    derived_run_id: "paper-replay-1779709705883959000",
    records: 37516,
    features: 26023,
    decisions: 26023,
    fills: 0,
    pnl: 0,
  },
  {
    source_run_id: "live-paper-1779705757144253928",
    derived_run_id: "paper-replay-1779710723230994000",
    records: 37056,
    features: 23989,
    decisions: 23989,
    fills: 0,
    pnl: 0,
  },
];

const BLOCKERS = [
  "LiveDisabled",
  "TopHolderDump",
  "NoBuyGap",
  "HighFakeMomentumRisk",
  "DataGapActive",
  "DevSoldEarly",
];

function mkdirp(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function readText(file) {
  return fs.existsSync(file) ? fs.readFileSync(file, "utf8") : "";
}

function readJson(file, fallback = {}) {
  try {
    return JSON.parse(readText(file));
  } catch {
    return fallback;
  }
}

function splitCsvLine(line) {
  const cells = [];
  let cell = "";
  let quoted = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"' && quoted && line[i + 1] === '"') {
      cell += '"';
      i += 1;
    } else if (ch === '"') {
      quoted = !quoted;
    } else if (ch === "," && !quoted) {
      cells.push(cell);
      cell = "";
    } else {
      cell += ch;
    }
  }
  cells.push(cell);
  return cells;
}

function parseCsv(text) {
  const lines = text.split(/\r?\n/).filter((line) => line.length);
  if (!lines.length) return [];
  const header = splitCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cells = splitCsvLine(line);
    const row = {};
    header.forEach((name, idx) => {
      row[name] = cells[idx] ?? "";
    });
    return row;
  });
}

function csvEscape(value) {
  if (value === null || value === undefined) return "";
  const text = Array.isArray(value) ? value.join("|") : String(value);
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function writeCsv(file, rows, header) {
  const cols = header ?? [...new Set(rows.flatMap((row) => Object.keys(row)))];
  const body = [cols.join(",")]
    .concat(rows.map((row) => cols.map((col) => csvEscape(row[col])).join(",")))
    .join("\n");
  fs.writeFileSync(file, `${body}\n`);
}

function writeJsonMd(baseName, payload, markdown) {
  fs.writeFileSync(path.join(OUT, `${baseName}.json`), `${JSON.stringify(payload, null, 2)}\n`);
  fs.writeFileSync(path.join(OUT, `${baseName}.md`), markdown);
}

function readCsvFile(file) {
  return parseCsv(readText(file));
}

function zstdCsv(file) {
  const text = execFileSync("zstd", ["-dc", file], {
    encoding: "utf8",
    maxBuffer: 1024 * 1024 * 512,
  });
  return parseCsv(text);
}

function listFiles(dir, suffix) {
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .filter((name) => name.endsWith(suffix))
    .sort()
    .map((name) => path.join(dir, name));
}

function decisionType(value) {
  const raw = String(value || "").toLowerCase();
  if (raw === "watchlight") return "WatchLight";
  if (raw === "watchdeep") return "WatchDeep";
  if (raw === "enterpaper") return "EnterPaper";
  if (raw === "stoptracking") return "StopTracking";
  if (raw === "emergencyexit") return "EmergencyExit";
  if (raw === "exit") return "Exit";
  if (raw === "hold") return "Hold";
  if (raw === "discard") return "Discard";
  return value || "Unknown";
}

function normalizeReason(reason) {
  const key = String(reason || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  const map = {
    livedisabled: "LiveDisabled",
    topholderdump: "TopHolderDump",
    nobuygap: "NoBuyGap",
    highfakemomentumrisk: "HighFakeMomentumRisk",
    datagapactive: "DataGapActive",
    devsoldearly: "DevSoldEarly",
  };
  return map[key] || reason;
}

function reasons(row) {
  return String(row.reason_codes || "")
    .split("|")
    .filter(Boolean)
    .map(normalizeReason);
}

function num(value) {
  if (value === undefined || value === null || String(value).trim() === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function sum(values) {
  return values.reduce((acc, value) => acc + (Number(value) || 0), 0);
}

function maxByCount(counts) {
  let best = "";
  let bestCount = -1;
  for (const [key, value] of Object.entries(counts)) {
    if (value > bestCount) {
      best = key;
      bestCount = value;
    }
  }
  return best || "none";
}

function countLines(file) {
  if (!fs.existsSync(file)) return 0;
  const text = readText(file).trim();
  return text ? Math.max(0, text.split(/\r?\n/).length - 1) : 0;
}

mkdirp(OUT);

const runData = new Map();
for (const run of PINNED) {
  const reportDir = path.join(ROOT, "data/reports", run.derived_run_id);
  const decisions = listFiles(reportDir, ".csv.zst")
    .filter((file) => path.basename(file).startsWith("decisions_"))
    .flatMap(zstdCsv)
    .map((row) => ({ ...row, decision_type_norm: decisionType(row.decision_type) }));
  const fills = listFiles(reportDir, ".csv.zst")
    .filter((file) => path.basename(file).startsWith("fills_"))
    .flatMap(zstdCsv);
  const labelDir = path.join(PHASE84_LABELS, "runs", run.source_run_id);
  const tokenLabels = readCsvFile(path.join(labelDir, "token_outcome_labels.csv"));
  const tradeLabels = readCsvFile(path.join(labelDir, "trade_outcome_labels.csv"));
  const pricePoints = readCsvFile(path.join(labelDir, "canonical_price_path_v2.csv"));
  runData.set(run.source_run_id, {
    run,
    decisions,
    fills,
    tokenLabels,
    tradeLabels,
    pricePoints,
  });
}

const tokenRows = [];
const noFillRows = [];
const allDecisions = [];
const allFills = [];
const allTradeRows = [];

for (const { run, decisions, fills, tokenLabels, tradeLabels, pricePoints } of runData.values()) {
  allDecisions.push(...decisions);
  allFills.push(...fills);
  const labelsByMint = new Map(tokenLabels.map((row) => [row.mint, row]));
  const filledMints = new Set(fills.map((row) => row.mint).filter(Boolean));
  const priceMints = new Set(
    pricePoints
      .filter((row) => row.price_sol_per_token)
      .map((row) => row.mint)
      .filter(Boolean),
  );
  const curveMints = new Set(
    pricePoints
      .filter((row) => row.curve_progress_pct)
      .map((row) => row.mint)
      .filter(Boolean),
  );
  const byMint = new Map();
  for (const decision of decisions) {
    if (!decision.mint) continue;
    const entry = byMint.get(decision.mint) || {
      decisions: [],
      reasonCounts: Object.fromEntries(BLOCKERS.map((name) => [name, 0])),
      typeCounts: {},
      strategies: new Set(),
      expectedEdges: [],
    };
    entry.decisions.push(decision);
    entry.typeCounts[decision.decision_type_norm] =
      (entry.typeCounts[decision.decision_type_norm] || 0) + 1;
    if (decision.strategy) entry.strategies.add(decision.strategy);
    for (const reason of reasons(decision)) {
      if (reason in entry.reasonCounts) entry.reasonCounts[reason] += 1;
    }
    const edge = num(decision.expected_net_edge_pct);
    if (edge !== null) entry.expectedEdges.push(edge);
    byMint.set(decision.mint, entry);
  }
  for (const [mint, info] of byMint) {
    info.decisions.sort((a, b) => Number(a.decision_time || 0) - Number(b.decision_time || 0));
    const label = labelsByMint.get(mint) || {};
    const hasMfe = Number(label.label_confidence || 0) > 0;
    const laterMfe = num(label.max_favorable_excursion_pct);
    const laterMae = num(label.max_adverse_excursion_pct);
    const highest = info.typeCounts.EnterPaper
      ? "EnterPaper"
      : info.typeCounts.WatchDeep
        ? "WatchDeep"
        : info.typeCounts.WatchLight
          ? "WatchLight"
          : info.typeCounts.StopTracking
            ? "StopTracking"
            : "Observed";
    const finalDecision = info.decisions.at(-1)?.decision_type_norm || "Unknown";
    const topBlocker = maxByCount(info.reasonCounts);
    const hasFill = filledMints.has(mint);
    const missingPrice = !priceMints.has(mint);
    const missingMfe = !hasMfe;
    let classification = "correctly_rejected_loser";
    if (missingMfe) {
      classification = "uncertain_due_to_missing_labels";
    } else if ((laterMfe ?? 0) > 25 && info.reasonCounts.TopHolderDump > 0) {
      classification = "missed_winner_due_to_top_holder_filter";
    } else if ((laterMfe ?? 0) > 25 && info.reasonCounts.NoBuyGap > 0) {
      classification = "missed_winner_due_to_no_buy_gap";
    } else if ((laterMfe ?? 0) > 25 && info.reasonCounts.HighFakeMomentumRisk > 0) {
      classification = "missed_winner_due_to_fake_momentum_filter";
    } else if ((laterMfe ?? 0) > 25 && info.reasonCounts.DataGapActive > 0) {
      classification = "missed_winner_due_to_data_gap";
    } else if ((laterMfe ?? 0) > 25) {
      classification = "missed_winner_due_to_missing_label";
    } else if ((laterMfe ?? 0) < 2) {
      classification = "not_executable_after_fees";
    }
    const rejectionJustified =
      !missingMfe && ((laterMfe ?? 0) < 2 || (laterMae ?? 0) <= -50 || topBlocker === "DataGapActive");
    const row = {
      source_run_id: run.source_run_id,
      derived_run_id: run.derived_run_id,
      mint,
      max_lifecycle_state: highest,
      highest_strategy_candidate: [...info.strategies].join("|") || "none",
      watch_light_seen: Boolean(info.typeCounts.WatchLight),
      watch_deep_seen: Boolean(info.typeCounts.WatchDeep),
      enter_candidate_seen: Boolean(info.typeCounts.EnterPaper),
      final_decision: finalDecision,
      top_rejection_reason: topBlocker,
      all_rejection_reasons: Object.entries(info.reasonCounts)
        .filter(([, value]) => value > 0)
        .map(([key, value]) => `${key}:${value}`)
        .join("|"),
      LiveDisabled_count: info.reasonCounts.LiveDisabled,
      TopHolderDump_count: info.reasonCounts.TopHolderDump,
      NoBuyGap_count: info.reasonCounts.NoBuyGap,
      HighFakeMomentumRisk_count: info.reasonCounts.HighFakeMomentumRisk,
      DataGapActive_count: info.reasonCounts.DataGapActive,
      DevSoldEarly_count: info.reasonCounts.DevSoldEarly,
      missing_price_label: missingPrice,
      missing_mfe_mae_label: missingMfe,
      top_holder_risk: info.reasonCounts.TopHolderDump > 0 ? "flagged_by_rejection_reason" : "",
      fake_momentum_risk: info.reasonCounts.HighFakeMomentumRisk > 0 ? "flagged_by_rejection_reason" : "",
      dev_risk: info.reasonCounts.DevSoldEarly > 0 ? "flagged_by_rejection_reason" : "",
      holder_growth: "",
      buy_sell_imbalance: "",
      fee_adjusted_edge: info.expectedEdges.length ? Math.max(...info.expectedEdges) : "",
      later_mfe_pct: laterMfe ?? "",
      later_mae_pct: laterMae ?? "",
      missed_winner_classification: classification,
      rejection_justified: rejectionJustified,
      confidence: missingMfe ? 0.35 : 0.75,
    };
    tokenRows.push({
      ...row,
      has_fill: hasFill,
      has_valid_canonical_price: priceMints.has(mint),
      has_curve_progress: curveMints.has(mint),
    });
    if (!hasFill) noFillRows.push(row);
  }
}

const stageCounts = {
  normalized_records: sum(PINNED.map((run) => run.records)),
  tokens_discovered: tokenRows.length,
  tokens_with_curve_state: tokenRows.filter((row) => row.has_curve_progress).length,
  tokens_with_valid_canonical_price: tokenRows.filter((row) => row.has_valid_canonical_price).length,
  tokens_with_mfe_mae_label: tokenRows.filter((row) => !row.missing_mfe_mae_label).length,
  WatchLight: new Set(allDecisions.filter((row) => row.decision_type_norm === "WatchLight").map((row) => `${row.source_run_id}|${row.mint}`)).size,
  WatchDeep: new Set(allDecisions.filter((row) => row.decision_type_norm === "WatchDeep").map((row) => `${row.source_run_id}|${row.mint}`)).size,
  EnterPaper_candidate: new Set(allDecisions.filter((row) => row.decision_type_norm === "EnterPaper").map((row) => `${row.source_run_id}|${row.mint}`)).size,
  actual_EnterPaper: allFills.filter((row) => String(row.side).toLowerCase() === "buy").length,
  fills: allFills.length,
  exits: allFills.filter((row) => String(row.side).toLowerCase() === "sell").length,
  StopTracking: allDecisions.filter((row) => row.decision_type_norm === "StopTracking").length,
  hard_discard: allDecisions.filter((row) => reasons(row).some((reason) => ["TopHolderDump", "DevSoldEarly", "HighFakeMomentumRisk"].includes(reason))).length,
  soft_discard: allDecisions.filter((row) => reasons(row).some((reason) => ["NoBuyGap", "LiveDisabled", "DataGapActive"].includes(reason))).length,
};

const funnelStages = Object.entries(stageCounts).map(([stage, count], idx, arr) => {
  const prev = idx === 0 ? count : arr[idx - 1][1];
  return {
    stage,
    count,
    pct_previous_stage: prev ? count / prev : null,
    top_blockers: stage.includes("discard") ? BLOCKERS.join("|") : "see no_fill_reason_summary",
    unavailable_metric_blockers:
      stage.includes("label") || stage.includes("price") || stage.includes("curve")
        ? "partial price/MFE/MAE coverage"
        : "",
    strategy_involved: stage.includes("Enter") || stage.includes("fill") || stage.includes("exit") ? "HolderGrowthContinuation" : "all",
    confidence: stage.includes("hard") || stage.includes("soft") ? 0.65 : 0.85,
  };
});

writeCsv(path.join(OUT, "decision_funnel_by_token.csv"), tokenRows, [
  "source_run_id",
  "derived_run_id",
  "mint",
  "max_lifecycle_state",
  "highest_strategy_candidate",
  "watch_light_seen",
  "watch_deep_seen",
  "enter_candidate_seen",
  "final_decision",
  "top_rejection_reason",
  "all_rejection_reasons",
  "has_valid_canonical_price",
  "has_curve_progress",
  "missing_mfe_mae_label",
  "has_fill",
  "later_mfe_pct",
  "later_mae_pct",
  "missed_winner_classification",
  "confidence",
]);
writeJsonMd(
  "decision_funnel",
  {
    schema_version: "phase85.decision_funnel.v1",
    pinned_runs: PINNED,
    stages: funnelStages,
    why_only_four_fills:
      "Only two HolderGrowthContinuation entry/exit trade pairs produced four fill events; all other strategies remained gated and most tokens never reached executable EnterPaper.",
    why_two_runs_zero_fills:
      "The second and third pinned runs had Watch/StopTracking activity but no eligible HolderGrowthContinuation entries survived the configured gates.",
    largest_dropoff: "tokens discovered to tokens with canonical MFE/MAE labels and strategy gates to actual EnterPaper",
    threshold_tuning_allowed: false,
  },
  `# Phase 85 Decision Funnel\n\n- normalized_records: \`${stageCounts.normalized_records}\`\n- tokens_discovered: \`${stageCounts.tokens_discovered}\`\n- tokens_with_valid_canonical_price: \`${stageCounts.tokens_with_valid_canonical_price}\`\n- tokens_with_mfe_mae_label: \`${stageCounts.tokens_with_mfe_mae_label}\`\n- actual EnterPaper entries: \`${stageCounts.actual_EnterPaper}\`\n- fills: \`${stageCounts.fills}\`\n\nOnly HolderGrowthContinuation filled. The largest practical drop-off is from broad watch/decision activity to two actual entries, with label/price coverage still sparse.\n`,
);

writeCsv(path.join(OUT, "no_fill_reason_ledger.csv"), noFillRows, [
  "source_run_id",
  "derived_run_id",
  "mint",
  "max_lifecycle_state",
  "highest_strategy_candidate",
  "watch_light_seen",
  "watch_deep_seen",
  "enter_candidate_seen",
  "final_decision",
  "top_rejection_reason",
  "all_rejection_reasons",
  "LiveDisabled_count",
  "TopHolderDump_count",
  "NoBuyGap_count",
  "HighFakeMomentumRisk_count",
  "DataGapActive_count",
  "DevSoldEarly_count",
  "missing_price_label",
  "missing_mfe_mae_label",
  "top_holder_risk",
  "fake_momentum_risk",
  "dev_risk",
  "holder_growth",
  "buy_sell_imbalance",
  "fee_adjusted_edge",
  "later_mfe_pct",
  "later_mae_pct",
  "missed_winner_classification",
  "rejection_justified",
  "confidence",
]);
const noFillClassCounts = {};
const noFillBlockerCounts = {};
for (const row of noFillRows) {
  noFillClassCounts[row.missed_winner_classification] =
    (noFillClassCounts[row.missed_winner_classification] || 0) + 1;
  noFillBlockerCounts[row.top_rejection_reason] = (noFillBlockerCounts[row.top_rejection_reason] || 0) + 1;
}
writeJsonMd(
  "no_fill_reason_summary",
  {
    schema_version: "phase85.no_fill_reason_summary.v1",
    no_fill_tokens: noFillRows.length,
    classification_counts: noFillClassCounts,
    top_rejection_reasons: noFillBlockerCounts,
    sparse_label_warning: "No-fill classification is confidence-scored because 1,188 MFE/MAE labels remain unavailable across Phase 84.",
    threshold_tuning_allowed: false,
  },
  `# Phase 85 No-Fill Reason Summary\n\n- no-fill tokens: \`${noFillRows.length}\`\n- top rejection reason: \`${maxByCount(noFillBlockerCounts)}\`\n- sparse-label warning: MFE/MAE labels remain partial, so uncertain no-fills are not tuning evidence.\n`,
);

const decisionsById = new Map(allDecisions.map((row) => [row.decision_id, row]));
const tradeLabelsByEntry = new Map();
for (const run of PINNED) {
  const labels = readCsvFile(path.join(PHASE84_LABELS, "runs", run.source_run_id, "trade_outcome_labels.csv"));
  for (const row of labels) tradeLabelsByEntry.set(row.entry_fill_id, row);
}

const fillsByPosition = new Map();
for (const fill of allFills) {
  const list = fillsByPosition.get(fill.position_id) || [];
  list.push(fill);
  fillsByPosition.set(fill.position_id, list);
}
const fillQuality = [];
for (const [positionId, fills] of fillsByPosition) {
  fills.sort((a, b) => Number(a.fill_time || 0) - Number(b.fill_time || 0));
  const entry = fills.find((fill) => String(fill.side).toLowerCase() === "buy") || fills[0];
  const exit = fills.find((fill) => String(fill.side).toLowerCase() === "sell");
  const entryDecision = decisionsById.get(entry.entry_decision_id) || decisionsById.get(entry.exit_decision_id) || {};
  const exitDecision = exit ? decisionsById.get(exit.exit_decision_id) || {} : {};
  const entryReasons = new Set(reasons(entryDecision));
  const exitReasons = new Set(reasons(exitDecision));
  const label = tradeLabelsByEntry.get(entry.fill_id) || {};
  const netPnl = num(exit?.net_pnl) ?? num(entry.net_pnl) ?? 0;
  const feeDrag = sum(fills.map((fill) => num(fill.base_fee) + num(fill.priority_fee) + num(fill.tip_fee)));
  const slippageDrag = sum(fills.map((fill) => num(fill.slippage_cost)));
  const impactDrag = sum(fills.map((fill) => num(fill.curve_impact_cost)));
  const exitTooLate = String(label.exit_too_late || "").toLowerCase() === "true";
  const feeKilled = String(label.fee_killed || "").toLowerCase() === "true";
  const topHolder = entryReasons.has("TopHolderDump") || exitReasons.has("TopHolderDump");
  const lossCause = netPnl < 0
    ? topHolder
      ? "top_holder_dump"
      : exitTooLate
        ? "exit_too_late"
        : feeKilled
          ? "fee_slippage_impact_killed"
          : "bad_alpha_or_timing"
    : "clean_or_partial_win";
  fillQuality.push({
    source_run_id: entry.source_run_id,
    derived_run_id: entry.run_id,
    position_id: positionId,
    mint: entry.mint,
    strategy: entry.strategy,
    entry_time: entry.fill_time,
    exit_time: exit?.fill_time || "",
    entry_price: entry.fill_price,
    exit_price: exit?.fill_price || "",
    gross_pnl: exit?.gross_pnl || entry.gross_pnl || "",
    executable_pnl: netPnl,
    fee_drag: feeDrag,
    slippage_drag: slippageDrag,
    impact_drag: impactDrag,
    mfe_after_entry: label.mfe_after_entry || "",
    mae_after_entry: label.mae_after_entry || "",
    entry_reason: [...entryReasons].join("|"),
    exit_reason: [...exitReasons].join("|") || exit?.exit_source || "",
    top_holder_risk_at_entry: entryReasons.has("TopHolderDump"),
    dev_risk_at_entry: entryReasons.has("DevSoldEarly"),
    fake_momentum_risk_at_entry: entryReasons.has("HighFakeMomentumRisk"),
    data_quality_state_at_entry: entryReasons.has("DataGapActive") ? "data_gap_active" : "clean_or_unflagged",
    entry_too_early: false,
    exit_too_late: exitTooLate,
    fee_killed: feeKilled,
    top_holder_dump_occurred: topHolder,
    loss_cause: lossCause,
    avoidability: netPnl < 0 ? "uncertain_existing_metrics_warned_partially" : "not_applicable",
  });
}
writeCsv(path.join(OUT, "fill_quality_by_trade.csv"), fillQuality);
const fillSummary = {
  schema_version: "phase85.fill_quality.v1",
  fill_events: allFills.length,
  trades: fillQuality.length,
  executable_pnl: sum(fillQuality.map((row) => row.executable_pnl)),
  holder_growth_continuation_negative: sum(fillQuality.map((row) => row.executable_pnl)) < 0,
  loss_causes: fillQuality.reduce((acc, row) => {
    acc[row.loss_cause] = (acc[row.loss_cause] || 0) + 1;
    return acc;
  }, {}),
  conclusion:
    "HolderGrowthContinuation lost on the pinned set; labels show severe adverse paths on the filled mint and sample size is too small for threshold work.",
  threshold_tuning_allowed: false,
};
writeJsonMd(
  "fill_quality_report",
  fillSummary,
  `# Phase 85 Fill Quality Report\n\n- fill events: \`${allFills.length}\`\n- trade pairs: \`${fillQuality.length}\`\n- executable PnL from filled trades: \`${fillSummary.executable_pnl}\`\n- conclusion: HolderGrowthContinuation remains negative and diagnostic-only.\n`,
);

function enrichCandidate(row, kind) {
  const noFill = noFillRows.find((candidate) => candidate.source_run_id === row.source_run_id && candidate.mint === row.mint) || {};
  const laterMfe = num(row.later_mfe) ?? num(noFill.later_mfe_pct);
  const laterMae = num(noFill.later_mae_pct);
  const missingLabel = noFill.missing_mfe_mae_label === true || noFill.missing_mfe_mae_label === "true";
  let classification = "uncertain";
  if (missingLabel) classification = "label_insufficient";
  else if (noFill.missing_price_label === true || noFill.missing_price_label === "true") classification = "price_coverage_insufficient";
  else if ((laterMfe ?? 0) < 2) classification = "not_executable_after_fees";
  else if (noFill.rejection_justified === true || noFill.rejection_justified === "true") classification = "risk_justified_miss";
  else if ((laterMfe ?? 0) > 25) classification = kind === "missed" ? "actionable_missed_winner" : "false_positive_missed_winner";
  return {
    ...row,
    rejection_reason: row.reason_not_entered || row.discard_reason || noFill.top_rejection_reason || "",
    later_mfe: laterMfe ?? "",
    later_mae: laterMae ?? "",
    fee_adjusted_executable_opportunity: laterMfe !== null ? Math.max(0, laterMfe - 2.5) : "",
    label_confidence: row.confidence || noFill.confidence || 0,
    price_coverage_confidence: noFill.missing_price_label ? 0 : 0.75,
    actually_executable: laterMfe !== null && laterMfe > 2.5 && !missingLabel,
    risk_justified: noFill.rejection_justified || false,
    gate_blocked: noFill.top_rejection_reason || "unknown",
    relaxing_gate_would_admit_known_losers: ["TopHolderDump", "HighFakeMomentumRisk", "DataGapActive"].includes(noFill.top_rejection_reason),
    classification,
  };
}
const missed = readCsvFile(path.join(PHASE84_DIAG, "missed_winners.csv")).map((row) => enrichCandidate(row, "missed"));
const falseDiscards = readCsvFile(path.join(PHASE84_DIAG, "false_discards.csv")).map((row) => enrichCandidate(row, "false"));
writeCsv(path.join(OUT, "missed_winners.csv"), missed);
writeCsv(path.join(OUT, "false_discards.csv"), falseDiscards);
const missedReview = {
  schema_version: "phase85.missed_false_discards.v1",
  missed_winners: missed.length,
  false_discards: falseDiscards.length,
  missed_classifications: missed.reduce((acc, row) => {
    acc[row.classification] = (acc[row.classification] || 0) + 1;
    return acc;
  }, {}),
  false_discard_classifications: falseDiscards.reduce((acc, row) => {
    acc[row.classification] = (acc[row.classification] || 0) + 1;
    return acc;
  }, {}),
  warning: "Candidates are diagnostic only; relaxing gates would also admit known losers and labels remain sparse.",
  threshold_tuning_allowed: false,
};
writeJsonMd(
  "missed_false_discards_review",
  missedReview,
  `# Phase 85 Missed Winner / False Discard Review\n\n- missed winners: \`${missed.length}\`\n- false discards: \`${falseDiscards.length}\`\n- warning: sparse labels and no counterfactual execution prevent tuning.\n`,
);

const strategyRows = [];
const byStrategy = new Map();
for (const decision of allDecisions) {
  const entry = byStrategy.get(decision.strategy) || {
    evaluated: 0,
    candidate: 0,
    near: 0,
    blockerCounts: {},
  };
  entry.evaluated += 1;
  if (["WatchLight", "WatchDeep", "EnterPaper"].includes(decision.decision_type_norm)) entry.candidate += 1;
  if (["WatchDeep", "EnterPaper"].includes(decision.decision_type_norm)) entry.near += 1;
  for (const reason of reasons(decision)) entry.blockerCounts[reason] = (entry.blockerCounts[reason] || 0) + 1;
  byStrategy.set(decision.strategy, entry);
}
for (const [strategy, entry] of byStrategy) {
  if (!strategy || strategy === "HolderGrowthContinuation") continue;
  strategyRows.push({
    strategy,
    evaluated_count: entry.evaluated,
    candidate_count: entry.candidate,
    near_entry_count: entry.near,
    top_blockers: Object.entries(entry.blockerCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 6)
      .map(([key, value]) => `${key}:${value}`)
      .join("|"),
    missing_required_metrics: "partial price/MFE/MAE coverage",
    threshold_blockers: entry.near > 0 ? "likely" : "possible",
    risk_blockers: "TopHolderDump|HighFakeMomentumRisk|DevSoldEarly where present",
    regime_blockers: "current pinned regimes did not produce executable entries",
    code_path_blockers: entry.evaluated > 0 ? "not_proven" : "possible_dead_code",
    dead_code: entry.evaluated === 0,
    plausible_offline_signal: strategy === "SellAbsorptionBounce" && entry.near > 0 ? "worth_offline_review" : "insufficient_evidence",
  });
}
writeJsonMd(
  "inactive_strategy_gate_analysis",
  {
    schema_version: "phase85.inactive_strategy_gate_analysis.v1",
    strategies: strategyRows,
    conclusion:
      "Inactive strategies are evaluated but gated by current thresholds/regimes and partial labels; no code-path bug is proven from the pinned set.",
    threshold_tuning_allowed: false,
  },
  `# Phase 85 Inactive Strategy Gate Analysis\n\nInactive strategies were evaluated, so this pass does not prove dead code. They remain gated by thresholds/regime/risk and are only candidates for offline hypothesis testing.\n`,
);

const currentPnl = PINNED.reduce((acc, run) => acc + run.pnl, 0);
function blockerCountField(blocker) {
  return `${blocker}_count`;
}

function countRecoverable(blocker) {
  return noFillRows.filter((row) => Number(row[blockerCountField(blocker)] || 0) > 0 && Number(row.later_mfe_pct || 0) > 25 && !row.missing_mfe_mae_label).length;
}
function countLosers(blocker) {
  return noFillRows.filter((row) => Number(row[blockerCountField(blocker)] || 0) > 0 && Number(row.later_mae_pct || 0) <= -50 && !row.missing_mfe_mae_label).length;
}
const variants = [
  ["current_strategy", 2, 4, currentPnl, "measured"],
  ["top_holder_dump_veto_stricter", 0, 0, null, "diagnostic_projection_would_remove_activity_not_a_replay"],
  ["top_holder_dump_veto_disabled", countRecoverable("TopHolderDump"), null, null, "invalid_without_counterfactual_execution"],
  ["no_buy_gap_relaxed", countRecoverable("NoBuyGap"), null, null, "invalid_without_counterfactual_execution"],
  ["fake_momentum_filter_relaxed", countRecoverable("HighFakeMomentumRisk"), null, null, "invalid_without_counterfactual_execution"],
  ["data_gap_filter_relaxed_for_noncritical", countRecoverable("DataGapActive"), null, null, "invalid_if_data_gap_is_material"],
  ["dev_sold_early_veto_stricter", 0, 0, null, "diagnostic_projection_would_reduce_candidates"],
  ["holder_growth_without_top_holder_gate", countRecoverable("TopHolderDump"), null, null, "invalid_without_counterfactual_execution"],
  ["holder_growth_with_price_label_required", fillQuality.length, allFills.length, currentPnl, "measured_subset_same_as_current_for_filled_trades"],
  ["holder_growth_with_mfe_mae_label_required", fillQuality.filter((row) => row.mfe_after_entry !== "").length, allFills.length, currentPnl, "measured_subset_same_as_current_for_labeled_filled_trades"],
].map(([variant, candidates, fills, pnl, validity]) => {
  const blocker = variant.includes("top_holder") ? "TopHolderDump" : variant.includes("buy_gap") ? "NoBuyGap" : variant.includes("fake") ? "HighFakeMomentumRisk" : variant.includes("data_gap") ? "DataGapActive" : "";
  return {
    variant,
    candidate_entries: candidates,
    fills,
    exits: fills ? Math.floor(Number(fills) / 2) : "",
    executable_pnl: pnl,
    raw_pnl: pnl,
    artifact_excluded_pnl: 0,
    hit_rate: variant === "current_strategy" ? fillQuality.filter((row) => Number(row.executable_pnl) > 0).length / Math.max(1, fillQuality.length) : "",
    average_win: variant === "current_strategy" ? Math.max(...fillQuality.map((row) => Number(row.executable_pnl))) : "",
    average_loss: variant === "current_strategy" ? Math.min(...fillQuality.map((row) => Number(row.executable_pnl))) : "",
    max_drawdown: variant === "current_strategy" ? currentPnl : "",
    missed_winners_recovered: blocker ? countRecoverable(blocker) : 0,
    losers_admitted: blocker ? countLosers(blocker) : 0,
    unavailable_label_exposure: noFillRows.filter((row) => row.missing_mfe_mae_label).length,
    fee_slippage_impact_drag: sum(fillQuality.map((row) => row.fee_drag + row.slippage_drag + row.impact_drag)),
    invalid_due_to_missing_labels: validity !== "measured" && validity !== "measured_subset_same_as_current_for_filled_trades",
    validity,
    production_thresholds_changed: false,
  };
});
writeCsv(path.join(OUT, "offline_ablation_trades.csv"), variants);
writeJsonMd(
  "offline_ablation_results",
  {
    schema_version: "phase85.offline_ablation_results.v1",
    variants,
    conclusion:
      "No variant is strategy-validating. Relaxed gates mostly create counterfactual candidates that require replay and more labels before any threshold discussion.",
    threshold_tuning_allowed: false,
  },
  `# Phase 85 Offline Ablation Results\n\nThe current measured strategy remains negative. Relaxed variants are diagnostic projections only, not executable backtests and not alpha evidence.\n`,
);

const hypotheses = [
  {
    name: "stricter_top_holder_dump_veto",
    reason: "Top-holder risk appears in blockers and filled-trade adverse paths.",
    evidence: "HGC remains negative; TopHolderDump is a broad blocker.",
    affected_blockers: "TopHolderDump",
    expected_effect: "reduce loss exposure, likely lower fills further",
    risk: "may also remove some missed winners",
    data_needed: "more labeled corrected-math runs and counterfactual replay",
    can_test_offline_now: true,
    requires_more_labels: true,
    requires_enrichment: false,
    should_be_rejected: false,
  },
  {
    name: "no_buy_gap_gate_review",
    reason: "NoBuyGap blocks many non-filled candidates.",
    evidence: "Missed-winner ledger contains candidates, but relaxing can admit losers.",
    affected_blockers: "NoBuyGap",
    expected_effect: "increase candidates",
    risk: "more false positives and fee-killed entries",
    data_needed: "counterfactual replay with canonical labels",
    can_test_offline_now: true,
    requires_more_labels: true,
    requires_enrichment: false,
    should_be_rejected: false,
  },
  {
    name: "fake_momentum_filter_review",
    reason: "HighFakeMomentumRisk is a frequent broad blocker.",
    evidence: "Current labels are too sparse to know if it overfilters winners.",
    affected_blockers: "HighFakeMomentumRisk",
    expected_effect: "recover some candidates if signal is too strict",
    risk: "fake momentum losses",
    data_needed: "more labels plus transaction fingerprint enrichment",
    can_test_offline_now: true,
    requires_more_labels: true,
    requires_enrichment: true,
    should_be_rejected: false,
  },
  {
    name: "current_regime_no_trade_bias",
    reason: "Only four fills and negative PnL suggests current regimes may not support HGC.",
    evidence: "Two pinned runs had zero fills; HGC PnL negative.",
    affected_blockers: "all",
    expected_effect: "avoid trading bad regimes",
    risk: "missed winners",
    data_needed: "more corrected-math runs",
    can_test_offline_now: true,
    requires_more_labels: true,
    requires_enrichment: false,
    should_be_rejected: false,
  },
];
writeJsonMd(
  "hypothesis_shortlist",
  {
    schema_version: "phase85.hypothesis_shortlist.v1",
    hypotheses,
    threshold_tuning_allowed: false,
    no_alpha_claim: true,
  },
  `# Phase 85 Hypothesis Shortlist\n\nTop offline-only hypotheses: stricter top-holder dump veto, no-buy-gap review, fake-momentum filter review, and current-regime no-trade bias. None justify production threshold changes.\n`,
);

const readiness = readJson(path.join(PHASE84_LABELS, "backtest_readiness_v2_batch.json"));
writeJsonMd(
  "readiness_after_diagnostics",
  {
    schema_version: "phase85.readiness_after_diagnostics.v1",
    diagnostic_backtesting_allowed: readiness.diagnostic_backtesting_allowed === true,
    single_run_backtest_allowed: readiness.use_cases?.single_run_backtest?.allowed === true,
    multi_run_backtest_allowed: readiness.use_cases?.multi_run_backtest?.allowed === true,
    walk_forward_allowed: readiness.use_cases?.walk_forward?.allowed === true,
    threshold_tuning_allowed: false,
    blockers: readiness.use_cases?.threshold_tuning?.blockers || [],
    warnings: readiness.use_cases?.diagnostics?.warnings || [],
    minimum_data_needed_next: [
      "more corrected-math labeled runs",
      "more executable fills without one-token/outlier domination",
      "lower price/MFE/MAE unavailable rate",
      "counterfactual offline replay for candidate gates",
    ],
  },
  `# Phase 85 Readiness After Diagnostics\n\n- diagnostic_backtesting_allowed: \`${readiness.diagnostic_backtesting_allowed === true}\`\n- threshold_tuning_allowed: \`false\`\n\nDiagnostics remain allowed. Formal backtesting and tuning remain blocked by sparse labels, partial price coverage, and only four fills.\n`,
);

fs.copyFileSync(path.join(OUT, "decision_funnel_by_token.csv"), path.join(OUT, "token_funnel.csv"));
writeJsonMd(
  "phase85_diagnostic_lab_review",
  {
    schema_version: "phase85.review.v1",
    decision_funnel: stageCounts,
    no_fill_summary: noFillClassCounts,
    fill_quality: fillSummary,
    missed_false_discards: missedReview,
    inactive_strategies: strategyRows,
    ablations: variants,
    hypotheses,
    diagnostic_backtesting_allowed: true,
    threshold_tuning_allowed: false,
    no_live_streams: true,
    no_live_trading: true,
    no_rpc_polling: true,
  },
  `# Phase 85 Diagnostic Lab Review\n\n- fills: \`${allFills.length}\`\n- HolderGrowthContinuation PnL: \`${currentPnl}\`\n- missed winners: \`${missed.length}\`\n- false discards: \`${falseDiscards.length}\`\n- diagnostic backtesting allowed: \`true\`\n- threshold tuning allowed: \`false\`\n\nNo production thresholds were changed. No winning strategy is claimed.\n`,
);

console.log(
  JSON.stringify(
    {
      output_dir: OUT,
      fills: allFills.length,
      trades: fillQuality.length,
      hgc_pnl: currentPnl,
      no_fill_tokens: noFillRows.length,
      missed_winners: missed.length,
      false_discards: falseDiscards.length,
      diagnostic_backtesting_allowed: true,
      threshold_tuning_allowed: false,
    },
    null,
    2,
  ),
);
