#!/usr/bin/env sh
set -eu

if [ "$#" -gt 0 ]; then
  exec "$@"
fi

provider="${AGENT_PROVIDER:-codex}"
model="${AGENT_MODEL:-}"
prompt_path="${AGENT_PROMPT_PATH:-/agent-runs/prompt.txt}"
schema_path="${AGENT_SCHEMA_PATH:-/agent-runs/schema.json}"
final_json_path="${AGENT_FINAL_JSON_PATH:-/agent-runs/final.json}"
final_message_path="${AGENT_FINAL_MESSAGE_PATH:-/agent-runs/final.md}"
tools_dir="${AGENT_TOOLS_DIR:-}"

mkdir -p "${CODEX_HOME:-/agent-auth/codex}" "${CLAUDE_CONFIG_DIR:-/agent-auth/claude}" "${HOME:-/tmp/agent-home}"
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

prompt="$(cat "$prompt_path")"

case "$provider" in
  codex)
    if [ -n "$model" ]; then
      codex exec --json --skip-git-repo-check --dangerously-bypass-approvals-and-sandbox -c shell_environment_policy.inherit=all --model "$model" --output-last-message "$final_message_path" --output-schema "$schema_path" "$prompt"
    else
      codex exec --json --skip-git-repo-check --dangerously-bypass-approvals-and-sandbox -c shell_environment_policy.inherit=all --output-last-message "$final_message_path" --output-schema "$schema_path" "$prompt"
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
      claude -p --model "$model" --output-format stream-json --json-schema "$(cat "$schema_path")" "$prompt"
    else
      claude -p --output-format stream-json --json-schema "$(cat "$schema_path")" "$prompt"
    fi
    ;;
  *)
    echo "Unsupported AGENT_PROVIDER: $provider" >&2
    exit 2
    ;;
esac
