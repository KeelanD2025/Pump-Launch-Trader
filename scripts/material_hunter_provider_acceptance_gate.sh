#!/usr/bin/env bash
set -euo pipefail

DURATION_SECONDS="${MATERIAL_HUNTER_PROVIDER_PROBE_SECONDS:-900}"
MAX_UPDATES="${MATERIAL_HUNTER_PROVIDER_PROBE_MAX_UPDATES:-}"
REPORT_DIR="${MATERIAL_HUNTER_PROVIDER_PROBE_REPORT_DIR:-}"

echo "== material hunter provider acceptance gate =="
echo "duration_seconds=${DURATION_SECONDS}"
if [[ -n "${MAX_UPDATES}" ]]; then
  echo "max_updates=${MAX_UPDATES}"
fi

cmd=(
  target/release/cli provider-health-probe
  --config config/default.toml
  --config-override config/local.example.toml
  --duration-seconds "${DURATION_SECONDS}"
  --json
)

if [[ -n "${MAX_UPDATES}" ]]; then
  cmd+=(--max-updates "${MAX_UPDATES}")
fi

if [[ -n "${REPORT_DIR}" ]]; then
  cmd+=(--report-dir "${REPORT_DIR}")
fi

output="$("${cmd[@]}")"
echo "${output}"

acceptance_result="$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("acceptance_result","BLOCK"))' <<<"${output}")"
if [[ "${acceptance_result}" != "PASS" ]]; then
  echo "material hunter provider acceptance gate: BLOCK"
  exit 1
fi

echo "material hunter provider acceptance gate: PASS"
