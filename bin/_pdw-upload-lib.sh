# Shared helpers for the personal-data-warehouse upload wrappers and their
# status helpers. This file is *sourced* (never executed directly) by
# bin/*-upload-launchd, bin/*-upload-systemd, and bin/*-status* so that a single
# implementation governs how run health is recorded and reported.
#
# Why this exists: the wrappers used to stamp a bare ISO timestamp into the
# heartbeat file on *every* run, regardless of exit code. A job that fired every
# interval but failed every time (e.g. an Apple Messages/Notes/Voice Memos
# uploader after macOS silently revoked Full Disk Access on a uv python bump)
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
