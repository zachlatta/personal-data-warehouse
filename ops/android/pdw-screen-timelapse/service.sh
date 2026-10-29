#!/system/bin/sh
# Magisk late_start service for the pdw-screen-timelapse module (any rooted
# Android device running the pixel-screen-timelapse capture loop). Deployed by
# scripts/install-pixel-screen-timelapse in the sysadmin repository, which
# copies this directory, bin/screen-timelapse-upload-android,
# bin/_pdw-upload-lib.sh and a release linux/arm64 pdw into the module.
#
# Runs pdw ingest screen-timelapse --compress-png --delete-after-upload every
# five minutes, transcoding raw PNG screenshots to full-resolution WebP on the
# phone (~37 KB/frame average, ~10-18 MB/day) and uploading screenshots and
# accessibility trees directly to PDW over Wi-Fi or cellular.
#
# Switches (files in $STATE):
#   `paused`    stops uploads.
#   `wifi-only` defers uploads when the active default network is not Wi-Fi.
#
# $STATE is credential-encrypted storage, so nothing here runs before the
# first unlock after a boot.

MODDIR=${0%/*}
STATE=/data/misc_ce/0/pdw-screen-timelapse
LOG=$STATE/logs/service.log
WAKE=pdw_screen_timelapse_run
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
  if kill -0 "$old" 2>/dev/null && grep -q pdw-screen-timelapse "/proc/$old/cmdline" 2>/dev/null; then
    exit 0
  fi
fi
echo $$ > "$STATE/supervisor.pid"

note() { echo "$(date '+%Y-%m-%d %H:%M:%S%z') $*" >> "$LOG"; }
note "started (every ${FLOOR}s)"

last=0
while :; do
  now=$(date +%s)
  if [ -f "$LOG" ] && [ "$(stat -c %s "$LOG")" -gt 262144 ]; then
    mv -f "$LOG" "$LOG.1"
  fi
  if [ ! -f "$STATE/paused" ] && [ $((now - last)) -ge $FLOOR ]; then
    echo "$WAKE 360000000000" > /sys/power/wake_lock 2>/dev/null
    timeout 360 sh "$MODDIR/bin/screen-timelapse-upload-android" >/dev/null 2>&1
    code=$?
    echo "$WAKE" > /sys/power/wake_unlock 2>/dev/null
    [ "$code" -ne 0 ] && note "upload run exited $code (see logs/screen-timelapse-upload.run.log)"
    last=$(date +%s)
  fi
  sleep 30
done
