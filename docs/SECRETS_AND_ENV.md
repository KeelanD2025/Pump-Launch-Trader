# Secrets and environment

This workspace treats provider, RPC, VPS, and Cloudflare credentials as env-only inputs.

## Required env vars

Cloudflare R2:

- `CF_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_ENDPOINT` optional; derived from `CF_ACCOUNT_ID` if omitted
- `R2_DATASET_BUCKET`
- `R2_REPORTS_BUCKET`
- `R2_CALIBRATION_BUCKET`
- `R2_PROVIDER_COMPAT_BUCKET` optional
- `R2_MANAGED_PREFIX` optional; defaults to `pump-launch-quant`

Optional Cloudflare API management:

- `CF_API_TOKEN`

Provider streams:

- `GEYSER_ENDPOINT`
- `GEYSER_AUTH_TOKEN`
- `SHREDSTREAM_ENDPOINT`
- `SHREDSTREAM_AUTH_TOKEN` optional

RPC:

- `RPC_URL` may exist in the environment, but stream-only market data must not use it.

## Hard rules

- Never commit secrets to the repo.
- Never place secrets in TOML config files.
- Never place secrets in generated systemd unit files.
- Never place secrets in reports, exports, manifests, or logs.
- Never paste secrets into source code.
- Prefer R2 S3-compatible access keys for object upload.
- Only use a Cloudflare API token when you actually need bucket-management operations.
- Treat any credential pasted into chat as compromised and rotate it before VPS use.

## Safe systemd usage

1. Generate the example env file with `install-systemd-example`.
2. Copy it outside the repo, for example to `~/.config/pump-launch-quant-autopilot.env`.
3. Populate the values locally.
4. Protect it with `chmod 600 ~/.config/pump-launch-quant-autopilot.env`.
5. Keep the systemd unit referencing env var names only.

## Rotation guidance

If any credential was exposed outside a secure env file:

1. rotate the affected credential immediately
2. replace the env file value
3. restart the service
4. confirm `inspect-r2`, `env-bootstrap-check`, and the provider precheck use the new env without writing the raw value anywhere

## What must not appear in logs or reports

- `GEYSER_AUTH_TOKEN`
- `SHREDSTREAM_AUTH_TOKEN`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `CF_API_TOKEN`
- VPS SSH passwords or private keys

The CLI and autopilot are designed to report only env-var presence, redacted endpoints, bucket names, checksums, and verification status.
