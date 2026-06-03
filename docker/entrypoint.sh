#!/usr/bin/env bash
set -euo pipefail

export CHATGPT_LOCAL_HOME="${CHATGPT_LOCAL_HOME:-/data}"

cmd="${1:-serve}"
shift || true

runtime="${CHATMOCK_RUNTIME:-auto}"

bool() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

resolve_runtime() {
  case "$runtime" in
    python|rust)
      printf '%s\n' "$runtime"
      ;;
    auto)
      if command -v chatmock-rs >/dev/null 2>&1; then
        printf 'rust\n'
      elif command -v chatmock >/dev/null 2>&1; then
        printf 'python\n'
      else
        printf 'No supported ChatMock runtime found.\n' >&2
        exit 127
      fi
      ;;
    *)
      printf 'Unsupported CHATMOCK_RUNTIME: %s\n' "$runtime" >&2
      exit 64
      ;;
  esac
}

map_bool_env() {
  local source_name="$1"
  local target_name="$2"
  local source_value="${!source_name:-}"
  local target_value="${!target_name:-}"

  if [[ -n "$source_value" && -z "$target_value" ]]; then
    export "$target_name=$source_value"
  fi
}

runtime_name="$(resolve_runtime)"

if [[ "$runtime_name" == "rust" ]]; then
  map_bool_env RESPONSES_WEBSOCKET_UPSTREAM CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM
  map_bool_env RESPONSES_WEBSOCKET_UPSTREAM_STATEFUL CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM_STATEFUL
fi

if [[ "$cmd" == "serve" ]]; then
  PORT="${PORT:-8000}"
  ARGS=(serve --host 0.0.0.0 --port "${PORT}")

  if [[ "$runtime_name" == "python" ]]; then
    if bool "${VERBOSE:-}" || bool "${CHATGPT_LOCAL_VERBOSE:-}"; then
      ARGS+=(--verbose)
    fi
    if bool "${VERBOSE_OBFUSCATION:-}" || bool "${CHATGPT_LOCAL_VERBOSE_OBFUSCATION:-}"; then
      ARGS+=(--verbose-obfuscation)
    fi
    if bool "${FAST_MODE:-}" || bool "${CHATGPT_LOCAL_FAST_MODE:-}"; then
      ARGS+=(--fast-mode)
    fi
  fi

  if [[ "$#" -gt 0 ]]; then
    ARGS+=("$@")
  fi

  if [[ "$runtime_name" == "rust" ]]; then
    exec chatmock-rs "${ARGS[@]}"
  fi

  exec chatmock "${ARGS[@]}"
elif [[ "$cmd" == "login" ]]; then
  ARGS=(login --no-browser)

  if bool "${VERBOSE:-}" || bool "${CHATGPT_LOCAL_VERBOSE:-}"; then
    ARGS+=(--verbose)
  fi

  if [[ "$#" -gt 0 ]]; then
    ARGS+=("$@")
  fi

  if [[ "$runtime_name" == "rust" ]]; then
    exec chatmock-rs "${ARGS[@]}"
  fi

  exec chatmock "${ARGS[@]}"
elif [[ "$cmd" == "info" ]]; then
  if [[ "$runtime_name" == "rust" ]]; then
    exec chatmock-rs info "$@"
  fi

  exec chatmock info "$@"
else
  exec "$cmd" "$@"
fi
