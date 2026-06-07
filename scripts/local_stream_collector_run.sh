#!/usr/bin/env bash
set -euo pipefail

CONFIG="${CONFIG:-config/default.toml}"
CONFIG_OVERRIDE="${CONFIG_OVERRIDE:-config/local.example.toml}"
OUTPUT_DIR="${OUTPUT_DIR:-research_output/local_stream_collector}"
LISTEN_URL="${LISTEN_URL:-}"
DURATION_SECONDS="${DURATION_SECONDS:-900}"
DRY_RUN="${DRY_RUN:-1}"

"$(dirname "$0")/local_stream_collector_preflight.sh"

ARGS=(
  --config "${CONFIG}"
  --config-override "${CONFIG_OVERRIDE}"
  local-stream-collector
  --output-dir "${OUTPUT_DIR}"
  --duration-seconds "${DURATION_SECONDS}"
  --json
)

if [[ -n "${LISTEN_URL}" ]]; then
  ARGS+=(--listen-url "${LISTEN_URL}")
fi
if [[ "${DRY_RUN}" == "1" || "${DRY_RUN}" == "true" ]]; then
  ARGS+=(--dry-run)
fi

target/release/cli "${ARGS[@]}"
