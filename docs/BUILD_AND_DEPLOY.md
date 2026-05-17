# Build And Deploy

## Why the VPS must not build releases

The production VPS is intentionally tiny and should stay dedicated to edge collection:

- low-latency stream ingest
- compact segment writing
- R2 upload and verification
- timer-based systemd collection

It should **not** run routine `cargo build` or `cargo run` release builds. The Linux CLI pulls in heavy dependencies such as `aws-sdk-s3`, and repeated on-box builds can:

- exhaust root disk
- trigger `rustc` `SIGKILL` from resource pressure
- pause collection at the worst time
- leave the collector in warning or critical disk mode

The normal path is:

1. GitHub Actions builds the Linux binary.
2. GitHub Actions or an operator copies only the compiled binary to the VPS.
3. The VPS validates the binary and keeps running `/home/ubuntu/pump-launch-quant/target/release/cli`.

## Detect the VPS target

Run these commands on the VPS if the target needs to be confirmed:

```bash
uname -m
ldd --version
cat /etc/os-release
```

For the current Ubuntu x86_64 VPS, the expected target is:

```text
x86_64-unknown-linux-gnu
```

That is the target used by the GitHub Actions workflow.

## GitHub Actions build flow

Workflow:

- file: `.github/workflows/build-linux-cli.yml`
- trigger: `workflow_dispatch`
- runner: `ubuntu-22.04`

Build job steps:

1. check out the repo
2. install Rust stable
3. cache cargo registry/git/target
4. run `cargo test -q`
5. run `cargo build --release -p cli`
6. run:
   - `target/release/cli validate-config --config config/default.toml --config-override config/local.example.toml`
   - `target/release/cli validate-stream-only --config config/default.toml --config-override config/local.example.toml`
   - `target/release/cli validate-edge-mode --config config/default.toml --config-override config/local.example.toml`
   - `target/release/cli scan-secrets --config config/default.toml`
7. publish:
   - `dist/cli`
   - `dist/cli.sha256`
   - `dist/build_info.json`

`build_info.json` records:

- git sha
- build timestamp
- rustc version
- target triple
- binary sha256
- package version

## Safe VPS deploy flow

The deploy job is optional and is only entered when `workflow_dispatch` sets `deploy_to_vps=true`.

Required GitHub Secrets:

- `VPS_HOST`
- `VPS_USER`
- `VPS_SSH_KEY`
- `VPS_PORT` optional
- `VPS_APP_DIR` optional, defaults to `/home/ubuntu/pump-launch-quant`

Deploy steps:

1. download the built artifact
2. verify `cli.sha256` locally
3. copy files to the VPS staging paths:
   - `/home/ubuntu/pump-launch-quant/target/release/cli.new`
   - `/home/ubuntu/pump-launch-quant/target/release/cli.new.sha256`
   - `/home/ubuntu/pump-launch-quant/target/release/build_info.json.new`
4. stop the edge timer/service safely
5. back up the current binary as `cli.prev.<timestamp>`
6. verify the staged sha256 on the VPS
7. atomically move the new binary into place
8. run:
   - `validate-config`
   - `validate-stream-only`
   - `validate-edge-mode`
   - `scan-secrets --env-file /home/ubuntu/pump-launch-quant.env`
9. if validation fails:
   - restore the previous binary
   - keep the timer stopped
10. if validation passes:
   - optionally restart the timer
   - optionally run a short edge-only smoke cycle

No secrets are stored in the workflow file, in repo config, or in systemd unit files.

## Local fallback deploy

If the GitHub deploy job is disabled, use:

```bash
scripts/deploy_prebuilt_cli.sh \
  --binary /path/to/cli \
  --sha256 /path/to/cli.sha256 \
  --build-info /path/to/build_info.json \
  --app-dir /home/ubuntu/pump-launch-quant \
  --config /home/ubuntu/pump-launch-quant/config/default.toml \
  --config-override /home/ubuntu/pump-launch-quant/config/local.toml \
  --env-file /home/ubuntu/pump-launch-quant.env \
  --restart-timer
```

That script never builds on the VPS. It only stages, verifies, backs up, installs, validates, and optionally restarts the timer.

## Rollback

Rollback is binary-only. Do not rebuild on the VPS for rollback.

Typical rollback procedure:

```bash
cp /home/ubuntu/pump-launch-quant/target/release/cli.prev.<timestamp> \
   /home/ubuntu/pump-launch-quant/target/release/cli
chmod +x /home/ubuntu/pump-launch-quant/target/release/cli
/home/ubuntu/pump-launch-quant/target/release/cli validate-config \
  --config /home/ubuntu/pump-launch-quant/config/default.toml \
  --config-override /home/ubuntu/pump-launch-quant/config/local.toml
/home/ubuntu/pump-launch-quant/target/release/cli validate-stream-only \
  --config /home/ubuntu/pump-launch-quant/config/default.toml \
  --config-override /home/ubuntu/pump-launch-quant/config/local.toml
/home/ubuntu/pump-launch-quant/target/release/cli validate-edge-mode \
  --config /home/ubuntu/pump-launch-quant/config/default.toml \
  --config-override /home/ubuntu/pump-launch-quant/config/local.toml
sudo systemctl start pump-launch-quant-edge-once.timer
```

If the collector had been paused manually or by a disk event, resume it explicitly:

```bash
/home/ubuntu/pump-launch-quant/target/release/cli autopilot-resume \
  --config /home/ubuntu/pump-launch-quant/config/default.toml \
  --config-override /home/ubuntu/pump-launch-quant/config/local.toml
```

## Production posture

- VPS: edge-only collector
- R2: durable dataset/archive layer
- GitHub Actions: Linux release builder
- local/larger machine: research worker, replay, backtests, exports

That separation is the intended steady state.
