#!/system/bin/sh
# Magisk late_start service for the pdw-android-photos module (the Pixel).
# Deployed by scripts/install-pixel-photos-upload in the sysadmin repository,
# which copies this directory, bin/android-photos-upload-android,
# bin/_pdw-upload-lib.sh and a release linux/arm64 pdw into the module.
#
# Runs the photo uploader seconds after a shot or a screenshot is saved, and
# every five minutes as the floor (which also covers the gallery folders
# nothing watches: images apps save under Pictures/, videos under Movies/):
#
# - inotifyd watches the camera folder and the screenshots folder. The camera
#   writes a shot under a hidden pending name and renames it when finished, so
#   the rename (and a screenshot's close-after-write) runs
#   bin/android-photos-upload-wake, which marks a run due and takes an
#   auto-expiring kernel wakelock: with the screen off the phone would
#   otherwise suspend before the upload starts.
# - The loop below starts a run when one is due (at most one every two
#   seconds; a burst of shots is one run) or five minutes have passed,
#   holding its own wakelock for the run.
#
# Switch (a file in $STATE): `paused` stops uploads; the watcher keeps running.
#
# $STATE is credential-encrypted storage (it holds the warehouse token; see
# bin/android-photos-upload-android), so it, like the photos themselves, does
# not exist until the first unlock after a boot. Nothing is written before.

# Magisk starts service.sh under busybox ash with ASH_STANDALONE=1, which
# shadows /system/bin tools (sh, timeout, pgrep, pkill) with BusyBox applets.
# Re-exec under Android's /system/bin/sh (mksh) with ASH_STANDALONE cleared so
# timeout and the upload wrapper use the platform binaries and signal traps.
if [ "${ASH_STANDALONE:-}" = 1 ] && [ -x /system/bin/sh ]; then
  unset ASH_STANDALONE
  exec /system/bin/sh "$0" "$@"
fi

MODDIR=${0%/*}
STATE=/data/misc_ce/0/pdw-android-photos
MEDIA=/data/media/0
LOG=$STATE/logs/service.log
WAKE=pdw_android_photos_run
FLOOR=300
MIN_GAP=2

umask 077

until [ "$(getprop sys.boot_completed)" = 1 ] && [ -d "$MEDIA/DCIM/Camera" ] && [ -d "$(dirname "$STATE")" ]; do
  sleep 5
done
mkdir -p "$STATE/logs"
chmod 700 "$STATE"

# One supervisor at a time (Magisk at boot, or the installer).
if [ -f "$STATE/supervisor.pid" ]; then
  old=$(cat "$STATE/supervisor.pid")
  if kill -0 "$old" 2>/dev/null && grep -q service.sh "/proc/$old/cmdline" 2>/dev/null; then
    exit 0
  fi
fi
echo $$ > "$STATE/supervisor.pid"

note() { echo "$(date '+%Y-%m-%d %H:%M:%S%z') $*" >> "$LOG"; }
note "started (on every camera or screenshot write, every ${FLOOR}s as the floor)"

watch() {
  while :; do
    # The screenshots folder appears with the first screenshot. Never create
    # it here: a root-owned folder would refuse SystemUI's writes.
    if [ -d "$MEDIA/Pictures/Screenshots" ]; then
      inotifyd "$MODDIR/bin/android-photos-upload-wake" "$MEDIA/DCIM/Camera:wy" "$MEDIA/Pictures/Screenshots:wy"
    else
      inotifyd "$MODDIR/bin/android-photos-upload-wake" "$MEDIA/DCIM/Camera:wy"
    fi
    note "inotifyd exited ($?); restarting in 5s"
    sleep 5
  done
}
watch &
echo $! > "$STATE/watcher.pid"

last=0
while :; do
  now=$(date +%s)
  if [ -f "$LOG" ] && [ "$(stat -c %s "$LOG")" -gt 262144 ]; then
    mv -f "$LOG" "$LOG.1"
  fi
  if [ ! -f "$STATE/paused" ] && { { [ -f "$STATE/due" ] && [ $((now - last)) -ge $MIN_GAP ]; } || [ $((now - last)) -ge $FLOOR ]; }; then
    rm -f "$STATE/due"
    # Hold the kernel wakelock 30s past the 960s timeout so it stays active
    # while android-photos-upload-android traps TERM and waits for pdw to exit.
    echo "$WAKE 990000000000" > /sys/power/wake_lock 2>/dev/null
    timeout 960 sh "$MODDIR/bin/android-photos-upload-android" >/dev/null 2>&1
    code=$?
    echo "$WAKE" > /sys/power/wake_unlock 2>/dev/null
    [ "$code" -ne 0 ] && note "upload run exited $code (see logs/android-photos-upload.run.log)"
    last=$(date +%s)
  fi
  sleep 2
done
