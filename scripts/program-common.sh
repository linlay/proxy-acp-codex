#!/usr/bin/env bash
set -euo pipefail

PROGRAM_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_ROOT="$(cd "$PROGRAM_COMMON_DIR/.." && pwd)"
APP_NAME="proxy-acp-codex"
MANIFEST_FILE="$BUNDLE_ROOT/manifest.json"
ENV_EXAMPLE_FILE="$BUNDLE_ROOT/.env.example"
ENV_FILE="${SERVICE_CONFIG_DIR:-$BUNDLE_ROOT}/.env"
BACKEND_BIN="$BUNDLE_ROOT/backend/$APP_NAME"
RUN_DIR="${SERVICE_STATE_DIR:-$BUNDLE_ROOT/run}"
LOG_DIR="${SERVICE_LOG_DIR:-$RUN_DIR}"
PID_FILE="$RUN_DIR/$APP_NAME.pid"
LOG_FILE="$LOG_DIR/$APP_NAME.log"

program_die() {
  echo "[program] $*" >&2
  exit 1
}

program_require_file() {
  local target="$1"
  [[ -f "$target" ]] || program_die "required file not found: $target"
}

program_validate_bundle() {
  program_require_file "$MANIFEST_FILE"
  program_require_file "$ENV_EXAMPLE_FILE"
  [[ -x "$BACKEND_BIN" ]] || program_die "backend binary is not executable: $BACKEND_BIN"
}

program_load_env() {
  [[ -f "$ENV_FILE" ]] || program_die "missing .env (copy from .env.example first)"
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
}

program_prepare_runtime_dirs() {
  mkdir -p "$RUN_DIR" "$LOG_DIR"
}

program_create_default_env() {
  mkdir -p "$(dirname "$ENV_FILE")"
  if [[ ! -f "$ENV_FILE" ]]; then
    cp "$ENV_EXAMPLE_FILE" "$ENV_FILE"
  fi
}

program_prepare_path() {
  export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"
}

program_check_dependencies() {
  local codex_cli="${CODEX_CLI:-codex}"

  if [[ "$codex_cli" == */* ]]; then
    [[ -x "$codex_cli" ]] || program_die "Codex CLI is not executable: $codex_cli"
    return
  fi

  command -v "$codex_cli" >/dev/null 2>&1 || program_die "$codex_cli is required in PATH"
}

program_read_pid() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid="$(cat "$PID_FILE")"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "$pid"
}

program_clear_stale_pid() {
  if [[ ! -f "$PID_FILE" ]]; then
    return
  fi
  local pid
  pid="$(program_read_pid || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    program_die "$APP_NAME is already running with pid $pid"
  fi
  rm -f "$PID_FILE"
}

program_start_backend_daemon() {
  local pid

  program_clear_stale_pid
  : >"$LOG_FILE"
  nohup python3 - "$BACKEND_BIN" "$ENV_FILE" >>"$LOG_FILE" 2>&1 <<'PY' &
import os
import signal
import subprocess
import sys
import time

backend_bin = sys.argv[1]
env_file = sys.argv[2]
child = subprocess.Popen(
    [backend_bin, "-env", env_file],
    stdin=subprocess.DEVNULL,
    start_new_session=True,
)

def terminate(_signum, _frame):
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        child.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    sys.exit(0)

def ignore_signal(signum, _frame):
    print(f"[program-supervisor] ignored signal {signum}", flush=True)

signal.signal(signal.SIGUSR1, terminate)
signal.signal(signal.SIGTERM, ignore_signal)
signal.signal(signal.SIGHUP, ignore_signal)
signal.signal(signal.SIGINT, ignore_signal)
status = child.wait()
print(f"[program-supervisor] {backend_bin} exited with status {status}", flush=True)
time.sleep(0.05)
sys.exit(status if status >= 0 else 128 + abs(status))
PY
  pid=$!
  printf '%s\n' "$pid" >"$PID_FILE"
  sleep 1
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    rm -f "$PID_FILE"
    program_die "backend failed to start; see $LOG_FILE"
  fi
  echo "[program-start] started $APP_NAME in daemon mode (pid=$pid)"
  echo "[program-start] log file: $LOG_FILE"
}

program_exec_backend() {
  exec "$BACKEND_BIN" -env "$ENV_FILE"
}

program_stop_backend() {
  local pid

  if [[ ! -f "$PID_FILE" ]]; then
    echo "[program-stop] pid file not found: $PID_FILE"
    return
  fi

  pid="$(program_read_pid || true)"
  [[ -n "$pid" ]] || program_die "pid file must contain a numeric pid: $PID_FILE"

  if ! kill -0 "$pid" >/dev/null 2>&1; then
    rm -f "$PID_FILE"
    echo "[program-stop] process $pid is not running; removed stale pid file"
    return
  fi

  if ! kill -USR1 "$pid" >/dev/null 2>&1; then
    kill "$pid"
  fi

  for _ in $(seq 1 30); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      rm -f "$PID_FILE"
      echo "[program-stop] stopped $APP_NAME (pid=$pid)"
      return
    fi
    sleep 1
  done

  program_die "process $pid did not stop within 30s"
}
