#!/usr/bin/env sh
set -eu

if [ "$#" -gt 0 ]; then
  exec "$@"
fi

provider="${AGENT_PROVIDER:-codex}"
model="${AGENT_MODEL:-}"
reasoning_effort="${AGENT_REASONING_EFFORT:-medium}"
prompt_path="${AGENT_PROMPT_PATH:-/agent-runs/prompt.txt}"
schema_path="${AGENT_SCHEMA_PATH:-/agent-runs/schema.json}"
final_json_path="${AGENT_FINAL_JSON_PATH:-/agent-runs/final.json}"
final_message_path="${AGENT_FINAL_MESSAGE_PATH:-/agent-runs/final.md}"
tools_dir="${AGENT_TOOLS_DIR:-}"
auth_source="${AGENT_AUTH_SOURCE:-}"
auth_output="${AGENT_AUTH_OUTPUT:-}"
codex_home="${CODEX_HOME:-/tmp/agent-codex-home}"
codex_sqlite_home="${CODEX_SQLITE_HOME:-/tmp/agent-codex-sqlite}"
claude_config_dir="${CLAUDE_CONFIG_DIR:-/tmp/agent-claude-config}"

mkdir -p "$codex_home" "$codex_sqlite_home" "$claude_config_dir" "${HOME:-/tmp/agent-home}"
if [ -n "$tools_dir" ] && [ -d "$tools_dir" ]; then
  export PATH="$tools_dir:$PATH"
fi

if [ ! -f "$prompt_path" ]; then
  echo "AGENT_PROMPT_PATH does not exist: $prompt_path" >&2
  exit 2
fi
if [ ! -f "$schema_path" ]; then
  echo "AGENT_SCHEMA_PATH does not exist: $schema_path" >&2
  exit 2
fi
if [ -z "$auth_source" ] || [ ! -s "$auth_source" ]; then
  echo "AGENT_AUTH_SOURCE does not contain a provider credential" >&2
  exit 2
fi
if [ -z "$auth_output" ]; then
  echo "AGENT_AUTH_OUTPUT is not set" >&2
  exit 2
fi
case "$provider" in
  codex|claude) ;;
  *)
    echo "Unsupported AGENT_PROVIDER: $provider" >&2
    exit 2
    ;;
esac

copy_auth_to_home() {
  case "$provider" in
    codex)
      cp "$auth_source" "$codex_home/auth.json"
      chmod 600 "$codex_home/auth.json"
      ;;
    claude)
      cp "$auth_source" "$claude_config_dir/.credentials.json"
      chmod 600 "$claude_config_dir/.credentials.json"
      ;;
  esac
}

copy_refreshed_auth_from_home() {
  case "$provider" in
    codex)
      refreshed_auth="$codex_home/auth.json"
      ;;
    claude)
      refreshed_auth="$claude_config_dir/.credentials.json"
      ;;
  esac
  if [ ! -s "$refreshed_auth" ]; then
    echo "Agent provider removed its credential instead of returning refreshed auth" >&2
    return 1
  fi
  auth_output_dir="$(dirname "$auth_output")"
  auth_output_tmp="${auth_output}.tmp.$$"
  mkdir -p "$auth_output_dir"
  cp "$refreshed_auth" "$auth_output_tmp"
  chmod 600 "$auth_output_tmp"
  mv "$auth_output_tmp" "$auth_output"
}

copy_auth_to_home
auth_copy_pending=1
provider_pid=""

copy_auth_on_exit() {
  if [ "$auth_copy_pending" -eq 1 ]; then
    copy_refreshed_auth_from_home || true
  fi
}

terminate_provider() {
  signal_exit_code="$1"
  if [ -n "$provider_pid" ]; then
    kill -TERM "$provider_pid" 2>/dev/null || true
  fi
  exit "$signal_exit_code"
}

trap copy_auth_on_exit EXIT
trap 'terminate_provider 129' HUP
trap 'terminate_provider 130' INT
trap 'terminate_provider 143' TERM

case "$provider" in
  codex)
    model="${model:-gpt-5.6-sol}"
    codex exec --json --skip-git-repo-check --dangerously-bypass-approvals-and-sandbox -c shell_environment_policy.inherit=all -c model_reasoning_effort="$reasoning_effort" --model "$model" --output-last-message "$final_message_path" --output-schema "$schema_path" - < "$prompt_path" &
    provider_pid=$!
    set +e
    wait "$provider_pid"
    provider_status=$?
    set -e
    provider_pid=""
    trap - HUP INT TERM
    copy_refreshed_auth_from_home
    auth_copy_pending=0
    trap - EXIT
    if [ "$provider_status" -ne 0 ]; then
      exit "$provider_status"
    fi
    if [ -f "$final_message_path" ]; then
      python3 - "$final_message_path" "$final_json_path" <<'PY'
import json
import pathlib
import sys

source = pathlib.Path(sys.argv[1])
target = pathlib.Path(sys.argv[2])
text = source.read_text(encoding="utf-8").strip()
if text.startswith("```"):
    lines = text.splitlines()
    if len(lines) >= 2 and lines[-1].strip() == "```":
        text = "\n".join(lines[1:-1]).strip()
json.loads(text)
target.write_text(text, encoding="utf-8")
PY
    fi
    ;;
  claude)
    if [ -n "$model" ]; then
      claude -p --model "$model" --output-format stream-json --json-schema "$(cat "$schema_path")" < "$prompt_path" &
    else
      claude -p --output-format stream-json --json-schema "$(cat "$schema_path")" < "$prompt_path" &
    fi
    provider_pid=$!
    set +e
    wait "$provider_pid"
    provider_status=$?
    set -e
    provider_pid=""
    trap - HUP INT TERM
    copy_refreshed_auth_from_home
    auth_copy_pending=0
    trap - EXIT
    exit "$provider_status"
    ;;
esac
