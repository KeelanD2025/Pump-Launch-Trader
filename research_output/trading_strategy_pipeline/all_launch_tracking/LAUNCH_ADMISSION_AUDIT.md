# Launch Admission Audit

Generated for the all-launch intake and tiered tracking patch.

## Findings

- Visible launch: a successful `TokenCreated` stream event detected by `is_successful_launch_create`.
- Attempted launch: a launch admitted into Tier 3 rich active tracking and written to `attempt_ledger.csv`.
- Prior cap behavior: `max_attempted_launches` was enforced before admission to `attempt_ledger.csv`, so launches after the cap or while rich slots were full could be invisible to compact strategy artifacts.
- New cap behavior: `max_attempted_launches` applies to Tier 3 rich tracking only. Tier 1 cheap launch intake writes `all_launch_intake_ledger.csv` before rich admission is considered.
- Rich slot cap: `max_concurrent_tracked_mints` still bounds Tier 3 active tracking. When full, launches remain cheap-only and get `rich_budget_full`.
- Capped/skipped launches: now recorded with explicit `rich_tracking_rejection_reason`, `skipped_due_to_budget`, `skipped_due_to_existing_tombstone`, and data-quality fields.
- Skipped later positives: audited through `missed_good_token_audit.csv`; this remains diagnostic and never creates replay or candidate eligibility.
- Dead-token removal: early rejected/tombstoned rich mints release slots and are recorded in `rich_tracking_slot_ledger.csv`.
- Tombstoned future updates: remain cheap-counted/skipped by the dispatcher and do not reactivate rich tracking.

## Tier Contract

- Tier 0: relay/stream intake receives configured provider frames.
- Tier 1: cheap launch intake indexes every visible launch compactly.
- Tier 2: candidate/early-burst watch is represented by promotion/watch reason codes and as-of diagnostics.
- Tier 3: bounded rich active tracking writes `attempt_ledger.csv`, rich token artifacts, candidates, rejected summaries, and as-of alpha feature rows.

## Safety

- The all-launch ledger is audit-only.
- Candidate checkpoints remain audit-only.
- Replay/backtesting/threshold tuning/trading remain disabled.
- Holder RPC remains disabled.
- RPC mint supply remains audit-only/non-canonical.
- R2 verification cannot override countability blockers.

## Patch Target

- Main source: `crates/cli/src/main.rs`
- Runtime relay remains relay-only; VPS artifact policy is unchanged.
- Supervisor summaries now surface all-launch counts from `all_launch_intake_summary.json`.

