#!/system/bin/sh
# Magisk late_start service for the pdw-android-screen-time module (any
# rooted Android device). Deployed by scripts/install-android-screen-time-upload
# in the sysadmin repository, which installs the PDW usage exporter APK
# (ops/android/pdw-usage-exporter) and copies this directory,
# bin/android-screen-time-upload-android, bin/_pdw-upload-lib.sh and a release
# linux/arm64 pdw into the module.
#
# Android's usage stats service records every app coming to the foreground
# or leaving it, the screen and lock state, and daily usage totals, and keeps
# them for about ten days. This service sends them to PDW every five minutes.
# Nothing is lost while the device sleeps or is offline for less than that
# window: the next run reads from where the last one stopped.
#
# Usage access for the exporter is re-granted at start: an app update or a
# settings reset can drop it, and nothing else would notice.
#
# Switch (a file in $STATE): `paused` stops uploads.
#
# $STATE is credential-encrypted storage, so nothing here runs before the
# first unlock after a boot.

# Magisk starts service.sh under busybox ash with ASH_STANDALONE=1, which
# shadows /system/bin tools (sh, timeout, pgrep, pkill) with BusyBox applets.
# Re-exec under Android's /system/bin/sh (mksh) with ASH_STANDALONE cleared so
# timeout and the upload wrapper use the platform binaries and signal traps.
if [ "${ASH_STANDALONE:-}" = 1 ] && [ -x /system/bin/sh ]; then
  unset ASH_STANDALONE
  exec /system/bin/sh "$0" "$@"
fi

MODDIR=${0%/*}
STATE=/data/misc_ce/0/pdw-android-screen-time
PKG=com.zachlatta.pdw.usage
LOG=$STATE/logs/service.log
WAKE=pdw_android_screen_time_run
FLOOR=300

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
  if kill -0 "$old" 2>/dev/null && grep -q pdw-android-screen-time "/proc/$old/cmdline" 2>/dev/null; then
    exit 0
  fi
fi
echo $$ > "$STATE/supervisor.pid"

note() { echo "$(date '+%Y-%m-%d %H:%M:%S%z') $*" >> "$LOG"; }

if appops set "$PKG" GET_USAGE_STATS allow >/dev/null 2>&1; then
  note "started (exporter has usage access; every ${FLOOR}s)"
else
  note "started, but could not grant the exporter usage access (is $PKG installed?)"
fi

last=0
while :; do
  now=$(date +%s)
  if [ -f "$LOG" ] && [ "$(stat -c %s "$LOG")" -gt 262144 ]; then
    mv -f "$LOG" "$LOG.1"
  fi
  if [ ! -f "$STATE/paused" ] && [ $((now - last)) -ge $FLOOR ]; then
    # Hold the kernel wakelock 30s past the 360s timeout so it stays active
    # while android-screen-time-upload-android traps TERM and waits for pdw to exit.
    echo "$WAKE 390000000000" > /sys/power/wake_lock 2>/dev/null
    timeout 360 sh "$MODDIR/bin/android-screen-time-upload-android" >/dev/null 2>&1
    code=$?
    echo "$WAKE" > /sys/power/wake_unlock 2>/dev/null
    [ "$code" -ne 0 ] && note "upload run exited $code (see logs/android-screen-time-upload.run.log)"
    last=$(date +%s)
  fi
  sleep 30
done
