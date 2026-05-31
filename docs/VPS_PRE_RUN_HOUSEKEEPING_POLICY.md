# VPS Pre-Run Housekeeping Policy

Pre-run housekeeping is mandatory before every VPS edge timer run, smoke run, launch canary, risk canary, material-candidate hunter, and future controlled dataset collection.

The gate runs safe cleanup before disk preflight. If cleanup cannot make the VPS safe, the command exits non-zero and collection must not start.

Never delete active run spool, current `.open` files, active locks without PID proof, unverified normalized events, unverified source segments, the current deployed binary, required rollback binaries, env files, secrets, or unrelated directories such as `/opt/arbo`.

Allowed cleanup is limited to stale `/dev/shm` build dirs, old target/build cache that is not required by the prebuilt runtime, old workflow temp artifacts, rollback binaries beyond the keep cap, and verified report CSV exports. Rejected/dead token rich artifacts may only be compacted after a tombstone exists and R2 verification is present.

Preserve compact tombstones, candidate artifacts, manifests, R2 proof, audit reports, and the latest configured verified local reports.

Threshold tuning remains disabled by this policy.
