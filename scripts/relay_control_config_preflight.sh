#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"
OUT_DIR="$REPO/research_output/trading_strategy_pipeline"
OUT_JSON="$OUT_DIR/relay_control_config_preflight.json"
OUT_MD="$OUT_DIR/relay_control_config_preflight.md"
CONTROL_ENV="${PUMP_RELAY_CONTROL_ENV_FILE:-$REPO/.codex_runtime_env/relay_control.env}"

mkdir -p "$OUT_DIR"

if [ -f "$CONTROL_ENV" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$CONTROL_ENV"
  set +a
fi

R2_ENV="${PUMP_RELAY_R2_ENV_FILE:-}"
if [ -n "$R2_ENV" ] && [ -f "$R2_ENV" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$R2_ENV"
  set +a
elif [ -f "$REPO/../pump-launch-quant.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$REPO/../pump-launch-quant.env"
  set +a
elif [ -f "$REPO/.codex_runtime_env/relay_r2.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$REPO/.codex_runtime_env/relay_r2.env"
  set +a
fi

python3 - "$REPO" "$OUT_JSON" "$OUT_MD" "$CONTROL_ENV" <<'PY'
from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import time
from urllib.parse import urlparse

repo = pathlib.Path(sys.argv[1])
out_json = pathlib.Path(sys.argv[2])
out_md = pathlib.Path(sys.argv[3])
control_env = pathlib.Path(sys.argv[4])


def present(name: str) -> bool:
    return bool(os.environ.get(name, "").strip())


def env_value(*names: str, default: str = "") -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return default


def is_loopback_tcp(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme == "tcp" and parsed.hostname in {"127.0.0.1", "localhost", "::1"} and parsed.port is not None


def run_json(cmd: list[str], timeout: int = 120) -> tuple[bool, dict[str, object], str]:
    try:
        proc = subprocess.run(cmd, cwd=repo, text=True, capture_output=True, timeout=timeout)
    except Exception as exc:  # noqa: BLE001 - surfaced as structured preflight.
        return False, {}, str(exc)
    text = (proc.stdout or "").strip()
    payload: dict[str, object] = {}
    if text:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            try:
                line = text.splitlines()[-1]
                if " " in line and line.split(" ", 1)[0].isupper():
                    line = line.split(" ", 1)[1]
                payload = json.loads(line)
            except json.JSONDecodeError:
                payload = {"parse_error": text[-1000:]}
    return proc.returncode == 0, payload, (proc.stderr or proc.stdout or "")[-1200:]


generated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
ssh_key = env_value("PUMP_RELAY_VPS_SSH_KEY", "PUMP_RELAY_SSH_KEY")
local_listen = env_value("PUMP_RELAY_LOCAL_LISTEN_URL", "PUMP_RELAY_REVERSE_TUNNEL_LOCAL", default="tcp://127.0.0.1:19097")
tunnel_remote = env_value("PUMP_RELAY_REVERSE_TUNNEL_REMOTE", default="tcp://127.0.0.1:19097")
tunnel_local = env_value("PUMP_RELAY_REVERSE_TUNNEL_LOCAL", "PUMP_RELAY_LOCAL_LISTEN_URL", default=local_listen)
remote_app_dir = env_value("PUMP_RELAY_REMOTE_APP_DIR", default="/home/ubuntu/pump-launch-quant")
remote_config = env_value("PUMP_RELAY_REMOTE_CONFIG", default="config/local.toml")
remote_health_root = env_value("PUMP_RELAY_REMOTE_HEALTH_ROOT", "PUMP_RELAY_VPS_HEALTH_ROOT", default="/run/user/1000")

checks: dict[str, object] = {
    "control_env_file_present": control_env.exists(),
    "PUMP_RELAY_VPS_SSH_TARGET_present": present("PUMP_RELAY_VPS_SSH_TARGET"),
    "PUMP_RELAY_VPS_SSH_KEY_present": bool(ssh_key),
    "PUMP_RELAY_VPS_SSH_KEY_readable": bool(ssh_key) and pathlib.Path(ssh_key).expanduser().is_file(),
    "PUMP_RELAY_REMOTE_APP_DIR_configured_or_defaulted": bool(remote_app_dir),
    "PUMP_RELAY_REMOTE_CONFIG_configured_or_defaulted": bool(remote_config),
    "PUMP_RELAY_REMOTE_HEALTH_ROOT_configured_or_defaulted": bool(remote_health_root),
    "local_receiver_bind_url_configured": bool(local_listen),
    "local_receiver_bind_loopback_only": is_loopback_tcp(local_listen),
    "reverse_tunnel_remote_loopback_only": is_loopback_tcp(tunnel_remote),
    "reverse_tunnel_local_loopback_only": is_loopback_tcp(tunnel_local),
    "R2_ENDPOINT_present": present("R2_ENDPOINT"),
    "R2_ACCESS_KEY_ID_present": present("R2_ACCESS_KEY_ID"),
    "R2_SECRET_ACCESS_KEY_present": present("R2_SECRET_ACCESS_KEY"),
    "R2_DATASET_BUCKET_present": present("R2_DATASET_BUCKET"),
}

blockers: list[str] = []
for key in (
    "PUMP_RELAY_VPS_SSH_TARGET_present",
    "PUMP_RELAY_VPS_SSH_KEY_present",
    "PUMP_RELAY_VPS_SSH_KEY_readable",
):
    if not checks[key]:
        blockers.append(key)
for key in (
    "local_receiver_bind_loopback_only",
    "reverse_tunnel_remote_loopback_only",
    "reverse_tunnel_local_loopback_only",
):
    if not checks[key]:
        blockers.append(key)
for key in (
    "R2_ENDPOINT_present",
    "R2_ACCESS_KEY_ID_present",
    "R2_SECRET_ACCESS_KEY_present",
    "R2_DATASET_BUCKET_present",
):
    if not checks[key]:
        blockers.append(key)

local_preflight_ok = False
local_preflight_payload: dict[str, object] = {}
if all(checks[k] for k in ("R2_ENDPOINT_present", "R2_ACCESS_KEY_ID_present", "R2_SECRET_ACCESS_KEY_present", "R2_DATASET_BUCKET_present")):
    local_preflight_ok, local_preflight_payload, local_preflight_error = run_json(
        [
            "./scripts/local_stream_collector_preflight.sh",
            "--storage-mode",
            "r2-primary",
            "--mode",
            "collection",
            "--verify-r2-health-live",
        ],
        timeout=180,
    )
else:
    local_preflight_error = "missing R2 env"
checks["local_r2_primary_preflight_ok"] = local_preflight_ok
if not local_preflight_ok:
    blockers.append("local_r2_primary_preflight_failed")

vps_safety_ok = False
vps_status_payload: dict[str, object] = {}
if not any(blocker.startswith("PUMP_RELAY_VPS_SSH") for blocker in blockers):
    status_ok, status_payload, status_error = run_json(
        [
            "python3",
            "scripts/run_relay_r2_primary_batch.py",
            "status",
            "--skip-preflight",
            "--target-gate",
            "EARLY_BURST_BACKTEST_READINESS",
            "--justification-id",
            "preflight-only",
            "--max-slices",
            "1",
        ],
        timeout=180,
    )
    vps_status_payload = status_payload
    vps_safety = (status_payload.get("STATUS") if "STATUS" in status_payload else status_payload) if status_payload else {}
    if "vps_safety" in vps_safety:
        vps_safety_ok = not bool((vps_safety.get("vps_safety") or {}).get("error"))
    else:
        vps_safety_ok = status_ok
else:
    status_error = "missing SSH config"
checks["vps_safety_ok"] = vps_safety_ok
if not vps_safety_ok:
    blockers.append("vps_safety_failed")

safety_flags = {
    "replay_allowed": bool(local_preflight_payload.get("replay_allowed")),
    "formal_backtesting_allowed": bool(local_preflight_payload.get("formal_backtesting_allowed")),
    "threshold_tuning_allowed": bool(local_preflight_payload.get("threshold_tuning_allowed")),
    "live_trading_enabled": bool(local_preflight_payload.get("live_trading_enabled")),
    "holder_rpc_enabled": bool(local_preflight_payload.get("holder_rpc_enabled")),
    "rpc_mint_supply_canonical": bool(local_preflight_payload.get("rpc_mint_supply_canonical")),
}
for key, value in safety_flags.items():
    checks[f"{key}_blocked_or_false"] = not value
    if value:
        blockers.append(key)

if any(blocker.startswith("PUMP_RELAY_VPS_SSH") or blocker.endswith("_present") for blocker in blockers):
    classification = "RELAY_CONTROL_CONFIG_BLOCK_MISSING_ENV"
elif not local_preflight_ok:
    classification = "RELAY_CONTROL_CONFIG_BLOCK_LOCAL_PREFLIGHT"
elif not vps_safety_ok:
    classification = "RELAY_CONTROL_CONFIG_BLOCK_VPS_SAFETY"
elif blockers:
    classification = "RELAY_CONTROL_CONFIG_BLOCK_STRUCTURAL"
else:
    classification = "RELAY_CONTROL_CONFIG_PASS"

missing_config_names = []
if not checks["PUMP_RELAY_VPS_SSH_TARGET_present"]:
    missing_config_names.append("PUMP_RELAY_VPS_SSH_TARGET")
if not checks["PUMP_RELAY_VPS_SSH_KEY_present"]:
    missing_config_names.append("PUMP_RELAY_VPS_SSH_KEY")
if checks["PUMP_RELAY_VPS_SSH_KEY_present"] and not checks["PUMP_RELAY_VPS_SSH_KEY_readable"]:
    missing_config_names.append("PUMP_RELAY_VPS_SSH_KEY_readable")
for key in ("R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_DATASET_BUCKET"):
    if not checks[f"{key}_present"]:
        missing_config_names.append(key)

payload = {
    "schema_version": "phase107j.relay_control_config_preflight.v1",
    "generated_at_utc": generated_at,
    "classification": classification,
    "ok": classification == "RELAY_CONTROL_CONFIG_PASS",
    "blockers": sorted(set(blockers)),
    "missing_config_names": sorted(set(missing_config_names)),
    "checks": checks,
    "local_preflight": {
        "ok": local_preflight_ok,
        "storage_mode": local_preflight_payload.get("storage_mode"),
        "preflight_mode": local_preflight_payload.get("preflight_mode"),
        "r2_health_verified": local_preflight_payload.get("r2_health_verified"),
        "free_mb_output": local_preflight_payload.get("free_mb_output"),
        "required_mb": local_preflight_payload.get("required_mb"),
        "error": None if local_preflight_ok else local_preflight_error,
    },
    "vps_safety": {
        "ok": vps_safety_ok,
        "error": None if vps_safety_ok else status_error,
    },
    "sanitized_config": {
        "remote_app_dir_configured_or_defaulted": bool(remote_app_dir),
        "remote_config_configured_or_defaulted": bool(remote_config),
        "remote_health_root_configured_or_defaulted": bool(remote_health_root),
        "local_receiver_loopback_only": checks["local_receiver_bind_loopback_only"],
        "reverse_tunnel_remote_loopback_only": checks["reverse_tunnel_remote_loopback_only"],
        "reverse_tunnel_local_loopback_only": checks["reverse_tunnel_local_loopback_only"],
    },
}

out_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
lines = [
    "# Relay Control Config Preflight",
    "",
    f"- generated_at_utc: `{generated_at}`",
    f"- classification: `{classification}`",
    f"- ok: `{str(payload['ok']).lower()}`",
    "",
    "## Missing Config Names",
]
if missing_config_names:
    lines.extend(f"- `{name}`" for name in sorted(set(missing_config_names)))
else:
    lines.append("- none")
lines.extend(["", "## Sanitized Checks"])
for key in sorted(checks):
    lines.append(f"- {key}: `{str(checks[key]).lower()}`")
lines.extend(
    [
        "",
        "## Safety",
        f"- replay_allowed: `{str(safety_flags['replay_allowed']).lower()}`",
        f"- formal_backtesting_allowed: `{str(safety_flags['formal_backtesting_allowed']).lower()}`",
        f"- threshold_tuning_allowed: `{str(safety_flags['threshold_tuning_allowed']).lower()}`",
        f"- live_trading_enabled: `{str(safety_flags['live_trading_enabled']).lower()}`",
        f"- holder_rpc_enabled: `{str(safety_flags['holder_rpc_enabled']).lower()}`",
        f"- rpc_mint_supply_canonical: `{str(safety_flags['rpc_mint_supply_canonical']).lower()}`",
    ]
)
out_md.write_text("\n".join(lines) + "\n")
print(json.dumps(payload, sort_keys=True))
sys.exit(0 if payload["ok"] else 2)
PY
