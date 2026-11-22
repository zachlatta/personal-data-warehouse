#!/system/bin/sh
# Approximate location acquisition and authenticated upload, at most once per 15 minutes.
# No tracking until this module is explicitly installed and started.
# $STATE/paused suspends both sampling and upload.
# Magisk starts service.sh under busybox ash with ASH_STANDALONE=1, which
# shadows /system/bin tools (sh, timeout, pgrep, pkill) with BusyBox applets.
# Re-exec under Android's /system/bin/sh (mksh) with ASH_STANDALONE cleared so
# timeout and the upload wrapper use the platform binaries and signal traps.
if [ "${ASH_STANDALONE:-}" = 1 ] && [ -x /system/bin/sh ]; then
  unset ASH_STANDALONE
  exec /system/bin/sh "$0" "$@"
fi

MODDIR=${0%/*}
STATE=/data/misc_ce/0/pdw-android-location
LOG=$STATE/logs/service.log
WAKE=pdw_android_location_run
FLOOR=900

umask 077

# /data/misc_ce/0 exists and ls /data/data succeeds before the first unlock too
# (the names are encrypted), so wait for Android to say CE storage is open.
# Without that, after the 2026-10-28 restart the supervisor started locked,
# its supervisor.pid and log writes failed, and it never logged "started".
until [ "$(getprop sys.boot_completed)" = 1 ] && [ "$(getprop sys.user.0.ce_available)" = true ] && [ -d /data/misc_ce/0 ] && ls /data/data >/dev/null 2>&1; do
  sleep 5
done
mkdir -p "$STATE/logs" "$STATE/home" "$STATE/tmp"
chmod 700 "$STATE"

# One supervisor at a time (Magisk at boot, or the installer).
if [ -f "$STATE/supervisor.pid" ]; then
  old=$(cat "$STATE/supervisor.pid")
  if kill -0 "$old" 2>/dev/null && grep -q pdw-android-location "/proc/$old/cmdline" 2>/dev/null; then
    exit 0
  fi
fi
echo $$ > "$STATE/supervisor.pid"

note() { echo "$(date '+%Y-%m-%d %H:%M:%S%z') $*" >> "$LOG"; }

note "started (network provider only; target ${FLOOR}s, Android may defer)"

runpid=""
stop() {
  trap - TERM INT HUP
  if [ -n "$runpid" ]; then
    # timeout may supervise the wrapper as a direct child; signal both so
    # pdw_run_stoppable can terminate the uploader and its content-read child.
    children=$(pgrep -P "$runpid" 2>/dev/null || true)
    kill -TERM "$runpid" $children 2>/dev/null || true
    wait "$runpid" 2>/dev/null || true
  fi
  echo "$WAKE" > /sys/power/wake_unlock 2>/dev/null
  exit 0
}
trap stop TERM INT HUP
last=0
while :; do
  now=$(date +%s)
  if [ -f "$LOG" ] && [ "$(stat -c %s "$LOG")" -gt 262144 ]; then
    mv -f "$LOG" "$LOG.1"
  fi
  if [ ! -f "$STATE/paused" ] && [ $((now - last)) -ge $FLOOR ]; then
    # Hold the kernel wakelock 30s past the 120s timeout so it stays active
    # while android-location-upload-android traps TERM and waits for pdw to exit.
    echo "$WAKE 150000000000" > /sys/power/wake_lock 2>/dev/null
    timeout 120 sh "$MODDIR/bin/android-location-upload-android" >/dev/null 2>&1 &
    runpid=$!
    wait "$runpid"
    code=$?
    runpid=""
    echo "$WAKE" > /sys/power/wake_unlock 2>/dev/null
    [ "$code" -ne 0 ] && note "upload run exited $code (see logs/android-location-upload.run.log)"
    last=$(date +%s)
  fi
  sleep "$FLOOR"
done
