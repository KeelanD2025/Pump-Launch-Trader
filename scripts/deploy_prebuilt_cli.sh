#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  deploy_prebuilt_cli.sh \
    --binary /path/to/cli \
    --sha256 /path/to/cli.sha256 \
    [--build-info /path/to/build_info.json] \
    [--app-dir /home/ubuntu/pump-launch-quant] \
    [--config /home/ubuntu/pump-launch-quant/config/default.toml] \
    [--config-override /home/ubuntu/pump-launch-quant/config/local.toml] \
    [--env-file /home/ubuntu/pump-launch-quant.env] \
    [--restart-timer]

This script installs a prebuilt CLI binary on the VPS without running cargo.
EOF
}

BINARY_PATH=""
SHA256_PATH=""
BUILD_INFO_PATH=""
APP_DIR="/home/ubuntu/pump-launch-quant"
CONFIG_PATH=""
CONFIG_OVERRIDE_PATH=""
ENV_FILE_PATH=""
RESTART_TIMER="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary)
      BINARY_PATH="${2:-}"
      shift 2
      ;;
    --sha256)
      SHA256_PATH="${2:-}"
      shift 2
      ;;
    --build-info)
      BUILD_INFO_PATH="${2:-}"
      shift 2
      ;;
    --app-dir)
      APP_DIR="${2:-}"
      shift 2
      ;;
    --config)
      CONFIG_PATH="${2:-}"
      shift 2
      ;;
    --config-override)
      CONFIG_OVERRIDE_PATH="${2:-}"
      shift 2
      ;;
    --env-file)
      ENV_FILE_PATH="${2:-}"
      shift 2
      ;;
    --restart-timer)
      RESTART_TIMER="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${BINARY_PATH}" || -z "${SHA256_PATH}" ]]; then
  echo "--binary and --sha256 are required" >&2
  usage
  exit 1
fi

if [[ -z "${CONFIG_PATH}" ]]; then
  CONFIG_PATH="${APP_DIR}/config/default.toml"
fi

if [[ -z "${CONFIG_OVERRIDE_PATH}" ]]; then
  CONFIG_OVERRIDE_PATH="${APP_DIR}/config/local.toml"
fi

BIN_DIR="${APP_DIR}/target/release"
BIN_PATH="${BIN_DIR}/cli"
STAGED_BIN="${BIN_DIR}/cli.new"
STAGED_SHA="${BIN_DIR}/cli.new.sha256"
STAGED_BUILD_INFO="${BIN_DIR}/build_info.json.new"
BUILD_INFO_TARGET="${BIN_DIR}/build_info.json"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP_PATH="${BIN_PATH}.prev.${TIMESTAMP}"

mkdir -p "${BIN_DIR}"
cp "${BINARY_PATH}" "${STAGED_BIN}"
cp "${SHA256_PATH}" "${STAGED_SHA}"
if [[ -n "${BUILD_INFO_PATH}" ]]; then
  cp "${BUILD_INFO_PATH}" "${STAGED_BUILD_INFO}"
fi

rollback() {
  if [[ -f "${BACKUP_PATH}" ]]; then
    cp "${BACKUP_PATH}" "${BIN_PATH}"
    chmod +x "${BIN_PATH}"
  fi
}

on_error() {
  rollback
  exit 1
}

trap on_error ERR

sudo systemctl stop pump-launch-quant-edge-once.timer || true
sudo systemctl stop pump-launch-quant-edge-once.service || true

if [[ -f "${BIN_PATH}" ]]; then
  cp "${BIN_PATH}" "${BACKUP_PATH}"
fi

EXPECTED_SHA="$(tr -d '[:space:]' < "${STAGED_SHA}")"
ACTUAL_SHA="$(sha256sum "${STAGED_BIN}" | awk '{print $1}')"
test "${EXPECTED_SHA}" = "${ACTUAL_SHA}"

mv "${STAGED_BIN}" "${BIN_PATH}"
chmod +x "${BIN_PATH}"

"${BIN_PATH}" validate-config --config "${CONFIG_PATH}" --config-override "${CONFIG_OVERRIDE_PATH}"
"${BIN_PATH}" validate-stream-only --config "${CONFIG_PATH}" --config-override "${CONFIG_OVERRIDE_PATH}"
"${BIN_PATH}" validate-edge-mode --config "${CONFIG_PATH}" --config-override "${CONFIG_OVERRIDE_PATH}"

SCAN_ARGS=(
  --config "${CONFIG_PATH}"
  --config-override "${CONFIG_OVERRIDE_PATH}"
)
if [[ -n "${ENV_FILE_PATH}" ]]; then
  SCAN_ARGS+=(--env-file "${ENV_FILE_PATH}")
fi
"${BIN_PATH}" scan-secrets "${SCAN_ARGS[@]}"

mv "${STAGED_SHA}" "${BIN_PATH}.sha256"
if [[ -f "${STAGED_BUILD_INFO}" ]]; then
  mv "${STAGED_BUILD_INFO}" "${BUILD_INFO_TARGET}"
fi

trap - ERR

if [[ "${RESTART_TIMER}" == "true" ]]; then
  sudo systemctl start pump-launch-quant-edge-once.timer
fi

python3 - <<PY
import json
print(json.dumps({
    "app_dir": ${APP_DIR@Q},
    "binary_path": ${BIN_PATH@Q},
    "backup_path": ${BACKUP_PATH@Q},
    "restart_timer": ${RESTART_TIMER@Q} == "true",
    "validations_passed": True,
}, indent=2))
PY
