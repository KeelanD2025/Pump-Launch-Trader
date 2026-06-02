#!/usr/bin/env bash
set -euo pipefail

echo "== material hunter pre-run release gate =="

git diff --check
cargo fmt --all -- --check

if [[ "${MATERIAL_HUNTER_RUN_CLIPPY:-0}" == "1" ]]; then
  cargo clippy --workspace --all-targets --all-features -- -D warnings
else
  echo "clippy: skipped by default; set MATERIAL_HUNTER_RUN_CLIPPY=1 to enforce workspace clippy"
fi

cargo test -q -p runtime material_hunter
cargo test -q -p cli phase107f
cargo test -q
cargo build -q -p cli --release
target/release/cli validate-stream-only --config config/default.toml --config-override config/local.example.toml
target/release/cli material-hunter-preflight --config config/default.toml --config-override config/local.example.toml --offline

echo "material hunter pre-run release gate: PASS"
