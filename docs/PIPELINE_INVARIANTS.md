# Pipeline Invariants

These invariants define the edge collector/R2/research contract. A run is not healthy unless these are enforced by code, tests, or explicit verification commands.

## Segment Invariants
1. No .open file is ever uploaded.
2. No segment is uploaded before writer close.
3. No segment checksum is computed before final bytes are stable.
4. Every JSONL record is newline-terminated.
5. Every JSONL segment has a final newline unless explicitly marked empty.
6. Zstd encoder is finished before checksum/upload.
7. Segment is atomically renamed from temp/open path to final path.
8. Final bytes are read back from disk before checksum.
9. Zstd decode is verified before upload.
10. JSONL parse is verified before upload.
11. Record count is verified before upload.
12. Segment manifest entry is written only after integrity validation.
13. R2 upload occurs only after local integrity validation.
14. R2 verification occurs after upload.
15. Local segment prune occurs only after R2 verification.
16. Invalid segment remains visible, never silently dropped.
17. Edge run cannot be marked complete if required segments are missing/unverified.

## Manifest Invariants
18. segment_manifest final bytes are immutable after artifact_manifest references them.
19. artifact_manifest must reference final segment_manifest checksum/size.
20. If segment_manifest changes, artifact_manifest is rebuilt automatically.
21. verify-r2-upload --check-manifest-consistency must pass for completed_verified.
22. completed_verified cannot be set while manifest drift exists.
23. completed_with_warnings can exist with data warnings, but not manifest drift.
24. partial_upload/partial_unverified must be used if pending/unverified artifacts remain.

## Dataset Index Invariants
25. Dataset index updates must merge authoritative R2 state, not overwrite with partial local state.
26. True verified fields must not regress to false without explicit failure evidence.
27. Research linkage must not be blanked by a generic upload.
28. normalized_events counts must not disappear.
29. Invalid segment lists merge by union.
30. Index lock prevents concurrent clobbering.

## Edge Collector Invariants
31. edge_collector never calls RPC/API/HTTP metadata.
32. edge_collector never computes heavy features/reports/backtests.
33. edge_collector emits normalized_events for every edge run.
34. edge source run has feature_snapshot_count=0, decision_count_total=0, fill_count_total=0.
35. edge run uses R2 as durable archive and local disk as spool only.

## Research Worker Invariants
36. Research worker does not connect to live streams.
37. Research worker prefers normalized_events.
38. Fallback to source_events only if allowed and recorded.
39. --require-normalized-events blocks fallback.
40. Research summaries must show true artifact type used.
41. Research summaries must not have stale stream_only/r2 fields.
42. Derived replay metadata must match artifact upload verification.

## Systemd Github Invariants
43. VPS must not build Rust in normal operation.
44. GitHub builds Linux binary.
45. Deploy validates before restart.
46. Rollback exists.
47. Timer-based oneshot autonomy must create edge runs without stale state.
