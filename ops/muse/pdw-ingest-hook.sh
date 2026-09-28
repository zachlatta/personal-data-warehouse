#!/usr/bin/env bash
# Muse hook: upload this Muse agent's transcripts and workspace to PDW.
#
# Muse (Meta's hosted personal agent) runs on a VM that accepts no inbound
# connections and loses every process on restart; only the home directory
# persists. A Muse *hook* is a Bash script the runtime itself runs every
# poll_interval_secs, and a hook that ends with `silent` never wakes the model.
# That makes it the scheduler: no cron job (each run would be a model turn and
# a new transcript to ingest), no daemon to lose on restart.
#
# Install once, from a shell on the VM (see ops/muse/README.md):
#   cp ops/muse/pdw-ingest-hook.sh ~/hooks/scripts/pdw-ingest.sh
# then ask Muse to register it with hooks.add (id pdw-ingest, poll interval
# 300s), dry-run it, and enable it.
#
# Settings that must not live in this public repo (the account label) go in
# ~/.config/pdw/muse.env, e.g.  MUSE_ACCOUNT=you@example.com
set -uo pipefail

MUSE_HOME_DIR="${MUSE_HOME:-/home/hatch}"
runtime="${HATCH_HOOK_RUNTIME:-$MUSE_HOME_DIR/hooks/runtime/hatch_hook_runtime.sh}"
# shellcheck source=/dev/null
[ -r "$runtime" ] && source "$runtime"
if ! declare -F silent >/dev/null; then
    # Run by hand, outside the hook runtime.
    silent() { printf '%s\n' "${1-}"; exit 0; }
fi

export HOME="$MUSE_HOME_DIR"
# The background self-update check hangs behind Muse's egress proxy.
export PDW_NO_AUTO_UPDATE=1
env_file="$MUSE_HOME_DIR/.config/pdw/muse.env"
if [ -r "$env_file" ]; then
    set -a
    # shellcheck source=/dev/null
    source "$env_file"
    set +a
fi

pdw="${PDW_BIN:-$MUSE_HOME_DIR/.local/bin/pdw}"
state_dir="$MUSE_HOME_DIR/.local/state/pdw"
mkdir -p "$state_dir"
run_log="$state_dir/muse-upload.run.log"

if [ "${HATCH_HOOK_DRY_RUN:-0}" = "1" ]; then
    if "$pdw" ingest muse --help >/dev/null 2>&1; then
        silent "dry run: $("$pdw" version 2>/dev/null | head -1) can run pdw ingest muse"
    fi
    silent "dry run: $pdw cannot run pdw ingest muse"
fi

started=$(date +%s)
ran_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
output=$("$pdw" ingest muse --home "$MUSE_HOME_DIR" 2>&1)
rc=$?
duration=$(( $(date +%s) - started ))
{
    printf '=== %s rc=%s %ss\n' "$ran_at" "$rc" "$duration"
    printf '%s\n' "$output" | tail -n 40
} >>"$run_log"
# Keep the run log bounded; the hook runs every five minutes forever.
if [ "$(wc -c <"$run_log")" -gt 1048576 ]; then
    tail -c 524288 "$run_log" >"$run_log.tmp" && mv "$run_log.tmp" "$run_log"
fi

error=""
if [ "$rc" -ne 0 ]; then
    error=$(printf '%s\n' "$output" | grep -v '^$' | tail -n 1)
fi
"$pdw" heartbeat --pipeline muse --device muse --exit-code "$rc" \
    --duration-seconds "$duration" --ran-at "$ran_at" --error "$error" >/dev/null 2>&1 || true

silent "pdw ingest muse exited $rc in ${duration}s: $(printf '%s\n' "$output" | tail -n 1)"
