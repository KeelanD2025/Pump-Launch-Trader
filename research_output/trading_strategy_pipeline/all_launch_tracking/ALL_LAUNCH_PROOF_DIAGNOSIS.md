# All-Launch Proof Diagnosis

Source run: `all-launch-tiered-proof-20260620T165054Z`

## Proof Facts

- all_visible_launches_indexed: `335`
- rich_tracked_launches: `15`
- cheap_only_launches: `320`
- skipped_due_budget: `320`
- rich_tracking_slots_released: `15`
- average_rich_tracking_duration_ms: `181887`
- missed_good_token_audit_rows: `320`
- candidate_checkpoints: `0`
- replay_eligible_candidates: `0`
- counted_phase107b_result: `True`
- r2_verified: `True`

## Diagnosis

The proof fixed launch discovery: every visible launch had an all-launch intake row. The remaining bottleneck was not stream visibility; it was the tracking policy after intake.

The 320 cheap-only launches remained cheap-only because the live path admitted only the first 15 rich-tracked launches while the active rich cap was saturated early. Every cheap-only launch carried `rich_budget_full`, and every row in the original missed-good audit was classified as `missed_due_to_unknown_lack_of_followup`.

Rich slots did release: `15` of `15` rich slots released, with an average rich tracking duration of `181887` ms. However, the old policy still treated `max_attempted_launches` as a total rich admission ceiling, so released slots did not enable later rich reuse after 15 admissions. In practice, the active-slot turnover existed, but the total-attempt cap prevented it from becoming reusable promotion capacity.

No cheap-only launch had enough retained follow-up to classify as correctly ignored, positive/high-positive, or should-have-promoted. The existing audit therefore could not distinguish dead/no-followthrough launches from under-observed launches.

## Answers

- Why did 320 launches remain cheap-only? `rich_budget_full` plus no bounded cheap follow-up.
- Was this due max_attempted_launches, max_concurrent_tracked_mints, max promotions, or another budget? It was a combination of active rich cap saturation and the old total `max_attempted_launches` admission ceiling. There was no separate promotion budget.
- Did rich slots release early enough to admit later launches? Slots did release, but the policy did not reuse them after the total rich admission ceiling was hit.
- Were released slots reused? No evidence of meaningful reuse; rich admissions stopped at 15.
- Did max_attempted_launches prevent reuse after 15 attempts? Yes, the old policy used it as a total rich admission cap.
- Did any cheap-only launch have enough follow-up to classify? No. The original audit classified all 320 as unknown/lack-of-follow-up.
- Did any cheap-only launch later show positive/high-positive evidence? Not in the retained compact artifacts; that could not be determined from the old proof.
- Is rich budget the current candidate-discovery bottleneck? Yes, specifically the lack of split budgets, cheap follow-up retention, and dynamic promotion/reuse accounting.

## Patch Target

The source patch should preserve all-launch intake, add compact cheap follow-up snapshots, split visible intake from rich admission budgets, allow released rich slots to be reused within explicit promotion budgets, and write a v2 missed-good audit that separates missing follow-up, rich budget, promotion budget, data quality, and manual-review cases.

Replay, formal backtesting, threshold tuning, paper/live trading, holder RPC, and canonical RPC mint supply remained blocked and must stay blocked.
