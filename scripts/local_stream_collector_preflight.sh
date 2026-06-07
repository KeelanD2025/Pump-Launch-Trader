#!/usr/bin/env bash
set -euo pipefail

CONFIG="${CONFIG:-config/default.toml}"
CONFIG_OVERRIDE="${CONFIG_OVERRIDE:-config/local.example.toml}"
OUTPUT_DIR="${OUTPUT_DIR:-research_output/local_stream_collector}"
STORAGE_MODE="${LOCAL_COLLECTOR_STORAGE_MODE:-local-mirror}"
PREFLIGHT_MODE="${LOCAL_COLLECTOR_PREFLIGHT_MODE:-collection}"
SPOOL_DIR="${LOCAL_COLLECTOR_R2_SPOOL_DIR:-}"
SPOOL_MAX_MB="${LOCAL_COLLECTOR_R2_SPOOL_MAX_MB:-}"
RETENTION_MODE="${LOCAL_COLLECTOR_RETENTION_MODE:-}"
R2_UPLOAD_REQUIRED="${LOCAL_COLLECTOR_R2_UPLOAD_REQUIRED:-}"
VERIFY_R2_HEALTH_LIVE="${LOCAL_COLLECTOR_VERIFY_R2_HEALTH_LIVE:-0}"
UPLOAD_R2="${UPLOAD_R2:-0}"

if [[ "$#" -gt 0 ]]; then
  exec target/release/cli \
    --config "${CONFIG}" \
    --config-override "${CONFIG_OVERRIDE}" \
    local-collector-preflight \
    "$@" \
    --json
fi

ARGS=(
  --config "${CONFIG}"
  --config-override "${CONFIG_OVERRIDE}"
  local-collector-preflight
  --output-dir "${OUTPUT_DIR}"
  --storage-mode "${STORAGE_MODE}"
  --mode "${PREFLIGHT_MODE}"
  --json
)

if [[ -n "${SPOOL_DIR}" ]]; then
  ARGS+=(--spool-dir "${SPOOL_DIR}")
fi
if [[ -n "${SPOOL_MAX_MB}" ]]; then
  ARGS+=(--r2-spool-max-mb "${SPOOL_MAX_MB}")
fi
if [[ -n "${RETENTION_MODE}" ]]; then
  ARGS+=(--retention-mode "${RETENTION_MODE}")
fi
if [[ -n "${R2_UPLOAD_REQUIRED}" ]]; then
  ARGS+=(--r2-upload-required "${R2_UPLOAD_REQUIRED}")
fi
if [[ "${UPLOAD_R2}" == "1" || "${UPLOAD_R2}" == "true" ]]; then
  ARGS+=(--upload-r2)
fi
if [[ "${VERIFY_R2_HEALTH_LIVE}" == "1" || "${VERIFY_R2_HEALTH_LIVE}" == "true" ]]; then
  ARGS+=(--verify-r2-health-live)
fi

target/release/cli "${ARGS[@]}"
