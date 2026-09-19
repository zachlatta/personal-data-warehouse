# Shared helpers for the personal-data-warehouse upload wrappers and their
# status helpers. This file is *sourced* (never executed directly) by
# bin/*-upload-launchd, bin/*-upload-systemd, and bin/*-status* so that a single
# implementation governs how run health is recorded, credentialed, and
# reported.
#
# Why this exists: the wrappers used to stamp a bare ISO timestamp into the
# heartbeat file on *every* run, regardless of exit code. A job that fired every
# interval but failed every time (e.g. an Apple Messages/Notes/Voice Memos
# uploader after macOS silently revoked Full Disk Access on a binary change)
# therefore looked perfectly healthy: the heartbeat kept advancing while no new
# data reached the warehouse for days. The status helpers, which just `cat` the
# heartbeat, could not tell the difference.
#
# The fix is to make the heartbeat carry the exit code and to track the last
# *successful* run separately:
#
#   <heartbeat-file>        last line: "<iso8601> exit_code=<n>"  (every run)
#   <base>.last-success     "<iso8601>"                           (exit 0 only)
#
# pdw_print_health then turns those two files into an OK / FAILING / STALE
# verdict so a chronically failing uploader is obvious at a glance.
#
# POSIX sh only: this is sourced into both `#!/bin/zsh` (macOS) and
# `#!/usr/bin/env bash` (Linux/systemd) scripts, so it must avoid shell-specific
# syntax.

# pdw_success_file HEARTBEAT_FILE -> prints the sibling last-success path.
pdw_success_file() {
  case "$1" in
    *.heartbeat) printf '%s\n' "${1%.heartbeat}.last-success" ;;
    *) printf '%s\n' "$1.last-success" ;;
  esac
}

# pdw_record_run HEARTBEAT_FILE ISO EXIT_CODE
# Write the heartbeat (always) and refresh the last-success marker (exit 0 only).
pdw_record_run() {
  _hb="$1"
  _iso="$2"
  _code="$3"
  printf '%s exit_code=%s\n' "$_iso" "$_code" > "$_hb"
  if [ "$_code" -eq 0 ]; then
    printf '%s\n' "$_iso" > "$(pdw_success_file "$_hb")"
  fi
}

# _pdw_config_value FILE KEY -> prints the JSON string at FILE[KEY], or nothing.
# The file is the flat object the Go CLI writes with encoding/json (one string
# per key, no nesting), so a sed extraction is exact for it and keeps Python
# out of the wrappers entirely. A JSON escape inside the value (\" or \\) is
# not decoded; neither a URL nor a token pdw issues contains one.
_pdw_config_value() {
  [ -r "$1" ] || return 0
  { tr -d '\n' < "$1"; echo; } | sed -n "s/.*\"$2\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p"
}

# pdw_resolve_bin -> prints the pdw binary every wrapper should exec.
# PDW_BIN wins (tests and operators), then the release install location
# (~/.local/bin/pdw, where `pdw update` writes), then whatever is on PATH.
# Prints nothing when none exists, so callers can decide whether that is fatal
# (an uploader) or a quiet skip (the heartbeat post).
#
# Every device-side job runs THROUGH this one binary now -- uploaders, the
# mutation workers, the browser-session publishers and the heartbeat post --
# with no uv or Python anywhere in the exec chain. That used to be avoided on
# purpose: pdw self-updates replaced its binary and macOS TCC keyed each Full
# Disk Access / Automation / Photos grant to that exact build, so an update
# silently revoked the grant. Release binaries are now signed with the stable
# `com.zachlatta.pdw` identity (see AGENTS.md "pdw CLI Full Disk Access vs
# self-updates"), so the grant follows the binary across updates and there is
# nothing left to protect by keeping pdw out of the chain. Each Mac needs the
# grants (Full Disk Access for the uploaders, Automation -> Notes/Contacts for
# the mutation workers, Photos for the photos uploader) re-issued ONCE to the
# signed pdw binary; after that they survive every `pdw update`.
pdw_resolve_bin() {
  _pdw="${PDW_BIN:-}"
  if [ -n "$_pdw" ] && [ -x "$_pdw" ]; then
    printf '%s\n' "$_pdw"
    return 0
  fi
  _pdw="$HOME/.local/bin/pdw"
  if [ -x "$_pdw" ]; then
    printf '%s\n' "$_pdw"
    return 0
  fi
  _pdw="$(command -v pdw 2>/dev/null || true)"
  if [ -n "$_pdw" ]; then
    printf '%s\n' "$_pdw"
    return 0
  fi
  return 0
}

# pdw_export_app_credentials
# Export PDW_API_URL / PDW_SECRET_TOKEN from pdw's own config file (whatever
# `pdw login` wrote), so a caller that is NOT the pdw CLI can still reach the
# app -- today that is only the wrappers' own `.env`-free environment and any
# operator tooling sourcing this lib; the CLI resolves the same file itself.
#
# Why this lives here rather than in each wrapper: when the heartbeat post was
# a `uv run python -m ...` DIRECTLY outside pdw, it inherited none of the
# URL/token `pdw ingest` resolved for the uploader beside it. The five Apple
# wrappers had each hand-rolled this same config read for their own uploaders,
# which incidentally made their heartbeats work; the two agent-sessions
# wrappers ran their uploader THROUGH pdw and so never needed to -- and their
# heartbeat consequently failed on every run from the day it shipped, ~288
# times a day per Mac, straight into a launchd error log nobody reads. The
# visible damage was on /pipelines: claude_code, codex, pi and openclaw all sat
# at last_run_at NULL, so for those four sources "the uploader died" and "Zach
# is not using this tool" were indistinguishable. The heartbeat is `pdw
# heartbeat` now and resolves the config itself, but one implementation of the
# read stays here so no wrapper ever grows its own copy again.
#
# Idempotent and non-destructive: an already-set value always wins, so an
# operator or a wrapper pointing at a different origin is never overridden, and
# a missing or unreadable config is a silent no-op (openclaw and CI have none).
pdw_export_app_credentials() {
  # Mirror the Go CLI's resolution exactly (app/internal/cliconfig/cliconfig.go):
  # $XDG_CONFIG_HOME (else ~/.config), directory "pdw", falling back to the
  # pre-rename "pdw-cli" when the canonical file is absent or empty. Checking
  # only the canonical path makes this a silent no-op on a host that logged in
  # before the rename and never re-ran `pdw login` -- which is exactly the
  # openclaw VM, whose uploader kept working (the Go CLI knows both paths)
  # while its heartbeat kept failing.
  _base="${XDG_CONFIG_HOME:-$HOME/.config}"
  _cfg="${PDW_CONFIG:-}"
  if [ -z "$_cfg" ]; then
    for _candidate in "$_base/pdw/config.json" "$_base/pdw-cli/config.json"; do
      if [ -r "$_candidate" ] && [ -s "$_candidate" ]; then
        _cfg="$_candidate"
        break
      fi
    done
  fi
  [ -n "$_cfg" ] || return 0
  [ -r "$_cfg" ] || return 0
  if [ -z "${PDW_API_URL:-}" ]; then
    _url="$(_pdw_config_value "$_cfg" base_url)"
    if [ -n "$_url" ]; then
      PDW_API_URL="$_url"
      export PDW_API_URL
    fi
  fi
  if [ -z "${PDW_SECRET_TOKEN:-}" ]; then
    _tok="$(_pdw_config_value "$_cfg" token)"
    if [ -n "$_tok" ]; then
      PDW_SECRET_TOKEN="$_tok"
      export PDW_SECRET_TOKEN
    fi
  fi
  return 0
}

# pdw_post_heartbeat PIPELINES ISO EXIT_CODE DURATION_SECONDS
# Post the run's verdict to the warehouse (ops.uploader_heartbeats) so
# marts_ops.pipeline_health can tell a failing uploader from a quiet source.
# PIPELINES is comma-separated (the agent-sessions uploader covers several).
# Runs `pdw heartbeat` (native Go; resolves the URL/token the way every other
# pdw command does). Best effort: never changes the uploader's own exit code,
# and a host with no pdw binary just skips.
pdw_post_heartbeat() {
  _pipelines="$1"
  _iso="$2"
  _code="$3"
  _duration="${4:-0}"
  _pdw="$(pdw_resolve_bin)"
  if [ -z "$_pdw" ]; then
    return 0
  fi
  pdw_export_app_credentials
  "$_pdw" heartbeat \
    --pipeline "$_pipelines" --ran-at "$_iso" --exit-code "$_code" --duration-seconds "$_duration" \
    >/dev/null 2>&1 || echo "[$_iso] heartbeat post failed for $_pipelines (ignored)" >&2
  return 0
}

# _pdw_file_mtime FILE -> epoch seconds of the file's mtime (GNU then BSD stat).
_pdw_file_mtime() {
  stat -c %Y "$1" 2>/dev/null || stat -f %m "$1" 2>/dev/null
}

# _pdw_humanize_age SECONDS -> compact "3d" / "4h" / "12m" / "9s".
_pdw_humanize_age() {
  _s="$1"
  if [ "$_s" -lt 0 ]; then _s=0; fi
  if [ "$_s" -ge 86400 ]; then
    printf '%dd\n' "$((_s / 86400))"
  elif [ "$_s" -ge 3600 ]; then
    printf '%dh\n' "$((_s / 3600))"
  elif [ "$_s" -ge 60 ]; then
    printf '%dm\n' "$((_s / 60))"
  else
    printf '%ds\n' "$_s"
  fi
}

# pdw_print_health HEARTBEAT_FILE [STALE_SECONDS]
# Print a "Health:" verdict plus last-run / last-success detail. STALE_SECONDS
# (default 1800) flags a successful-but-not-recently-run job; a non-zero last
# exit code is always reported as FAILING regardless of age.
pdw_print_health() {
  _hb="$1"
  _stale_after="${2:-1800}"
  _now="$(date +%s)"

  if [ ! -f "$_hb" ]; then
    echo "Health: UNKNOWN (no heartbeat yet at $_hb)"
    return
  fi

  _line="$(tail -n 1 "$_hb" 2>/dev/null)"
  # Parse the trailing "exit_code=<n>" if present (older heartbeats lack it).
  case "$_line" in
    *exit_code=*) _code="${_line##*exit_code=}" ;;
    *) _code="" ;;
  esac
  _run_mtime="$(_pdw_file_mtime "$_hb")"
  _run_age=""
  if [ -n "$_run_mtime" ]; then
    _run_age="$(_pdw_humanize_age "$((_now - _run_mtime))")"
  fi

  _success="$(pdw_success_file "$_hb")"
  _success_line="never"
  _success_age=""
  if [ -f "$_success" ]; then
    _success_line="$(tail -n 1 "$_success" 2>/dev/null)"
    _success_mtime="$(_pdw_file_mtime "$_success")"
    if [ -n "$_success_mtime" ]; then
      _success_age="$(_pdw_humanize_age "$((_now - _success_mtime))")"
    fi
  fi

  if [ -n "$_code" ] && [ "$_code" != "0" ]; then
    echo "Health: FAILING - last run exited $_code"
  elif [ -z "$_code" ]; then
    echo "Health: UNKNOWN (legacy heartbeat without exit_code; restart the agent to refresh)"
  elif [ -n "$_run_mtime" ] && [ "$((_now - _run_mtime))" -gt "$_stale_after" ]; then
    echo "Health: STALE - last run ${_run_age:-?} ago (> $((_stale_after / 60))m)"
  else
    echo "Health: OK"
  fi

  if [ -n "$_run_age" ]; then
    echo "  last run:     $_line (${_run_age} ago)"
  else
    echo "  last run:     $_line"
  fi
  if [ "$_success_line" = "never" ]; then
    echo "  last success: never"
  elif [ -n "$_success_age" ]; then
    echo "  last success: $_success_line (${_success_age} ago)"
  else
    echo "  last success: $_success_line"
  fi
}
