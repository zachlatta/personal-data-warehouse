#!/system/bin/sh
# Magisk late_start service for the pdw-google-recorder module (the Pixel).
# Deployed by scripts/install-pixel-recorder-upload in the sysadmin
# repository, which copies this directory, bin/google-recorder-upload-android,
# bin/_pdw-upload-lib.sh and a release linux/arm64 pdw into the module.
#
# Runs the Google Recorder uploader seconds after Recorder saves a recording,
# and every five minutes as the floor:
#
# - inotifyd watches Recorder's database directory and its recordings. Every
#   write runs bin/google-recorder-upload-wake, which marks a run due and takes
#   an auto-expiring kernel wakelock. That matters with the screen off (a memo
#   started and stopped with the power button): without it the phone suspends
#   a second or two after Recorder saves, and the upload waits for the next
#   time something else wakes it.
# - The loop below starts a run when one is due (at most one every ten
#   seconds, since Recorder writes to its database while it records) or five
#   minutes have passed, holding its own wakelock for the run.
#
# Switch (a file in $STATE): `paused` stops uploads; the watcher keeps running.
#
# $STATE is credential-encrypted storage (it holds the warehouse token; see
# bin/google-recorder-upload-android), so it, like Recorder's own data, does
# not exist until the first unlock after a boot. Nothing is written before.

MODDIR=${0%/*}
STATE=/data/misc_ce/0/pdw-google-recorder
REC=/data/data/com.google.android.apps.recorder
LOG=$STATE/logs/service.log
WAKE=pdw_google_recorder_run
FLOOR=300
MIN_GAP=10

umask 077

until [ "$(getprop sys.boot_completed)" = 1 ] && [ -d "$REC/databases" ] && [ -d "$REC/files/recordings" ]; do
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
note "started (on every Recorder write, every ${FLOOR}s as the floor)"

watch() {
  while :; do
    inotifyd "$MODDIR/bin/google-recorder-upload-wake" "$REC/databases:c" "$REC/files/recordings:wnyd"
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
    echo "$WAKE 960000000000" > /sys/power/wake_lock 2>/dev/null
    timeout 960 sh "$MODDIR/bin/google-recorder-upload-android" >/dev/null 2>&1
    code=$?
    echo "$WAKE" > /sys/power/wake_unlock 2>/dev/null
    [ "$code" -ne 0 ] && note "upload run exited $code (see logs/google-recorder-upload.run.log)"
    last=$(date +%s)
  fi
  sleep 2
done
