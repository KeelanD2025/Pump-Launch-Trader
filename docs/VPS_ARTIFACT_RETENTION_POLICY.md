# VPS Artifact Retention Policy

The VPS is an edge runtime, not a research archive.

Keep candidate directories, compact dead-token tombstones, manifests, R2 verification proof, and the latest configured housekeeping reports. Compact or delete rich rejected-token artifacts only after tombstone and R2 verification proof exist.

Do not delete unverified reports, unverified normalized events, active spool data, active `.open` files, current binary files, env files, or unrelated directories. Old rollback binaries are capped by policy. Build caches may be removed because Linux binaries are built in GitHub Actions and deployed prebuilt.

This policy prevents repeated copied artifacts and large CSV exports from clogging the main dataset or edge disk.
