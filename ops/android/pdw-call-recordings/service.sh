#!/system/bin/sh
# Magisk late_start service for the pdw-call-recordings module (the Pixel).
# Deployed by scripts/install-pixel-call-recordings-upload in the sysadmin
# repository, which copies this directory, bin/call-recordings-upload-android,
# bin/_pdw-upload-lib.sh and a release linux/arm64 pdw into the module.
#
# Runs the Android call recordings uploader seconds after BCR saves a recording,
# and every five minutes as the floor:
#
# - inotifyd watches BCR's output directory. Every
#   write runs bin/call-recordings-upload-wake, which marks a run due and takes
#   an auto-expiring kernel wakelock. That matters with the screen off (a memo
#   started and stopped with the power button): without it the phone suspends
#   a second or two after BCR saves, and the upload waits for the next
#   time something else wakes it.
# - The loop below starts a run when one is due (at most one every two
#   seconds, while the audio and sidecar settle) or five
#   minutes have passed, holding its own wakelock for the run.
#
# Switch (a file in $STATE): `paused` stops uploads; the watcher keeps running.
#
# $STATE is credential-encrypted storage (it holds the warehouse token; see
# bin/call-recordings-upload-android), so it, like BCR's own data, does
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
STATE=/data/misc_ce/0/pdw-call-recordings
REC=/storage/emulated/0/Recordings/Calls
LOG=$STATE/logs/service.log
WAKE=pdw_android_call_recordings_run
FLOOR=300
MIN_GAP=2

umask 077

until [ "$(getprop sys.boot_completed)" = 1 ] && [ -d /data/misc_ce/0/pdw-call-recordings ] && [ "$(cmd user is-user-unlocked 0)" = true ]; do
  sleep 5
done
mkdir -p "$STATE/logs" "$REC"
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
note "started (on every BCR write, every ${FLOOR}s as the floor)"

watch() {
  while :; do
    inotifyd "$MODDIR/bin/call-recordings-upload-wake" "$REC:wnyd"
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
    # while call-recordings-upload-android traps TERM and waits for pdw to exit.
    echo "$WAKE 990000000000" > /sys/power/wake_lock 2>/dev/null
    timeout 960 sh "$MODDIR/bin/call-recordings-upload-android" >/dev/null 2>&1
    code=$?
    echo "$WAKE" > /sys/power/wake_unlock 2>/dev/null
    [ "$code" -ne 0 ] && note "upload run exited $code (see logs/call-recordings-upload.run.log)"
    last=$(date +%s)
  fi
  sleep 2
done
