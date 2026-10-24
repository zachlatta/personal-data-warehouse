#!/system/bin/sh
# Magisk late_start service for the pdw-android-messages module (the Pixel).
# Deployed by scripts/install-pixel-messages-upload in the sysadmin
# repository, which copies this directory, bin/android-messages-upload-android,
# bin/_pdw-upload-lib.sh and a release linux/arm64 pdw into the module.
#
# Google Messages keeps every SMS, MMS and RCS message in its database
# (bugle_db). This service sends new and changed messages to PDW shortly
# after Messages writes them, and every five minutes as the floor:
#
# - inotifyd watches Messages' write-ahead log (bugle_db-wal). A write runs
#   bin/android-messages-upload-wake, which marks a run due and takes a 30 s
#   auto-expiring kernel wakelock, so a text that arrives with the screen off
#   is sent before the phone suspends.
# - The loop below starts a run when one is due (at most one every $MIN_GAP
#   seconds, so a conversation is a few batches, not one per message) or five
#   minutes have passed, holding its own wakelock for the run.
#
# Only messages since ANDROID_MESSAGES_SINCE (in $STATE/.env: when the phone
# took over the line) are ever read; the iPhone history in Messages stays on
# the phone.
#
# Switch (a file in $STATE): `paused` stops uploads; nothing is lost while
# paused, because the next run reads the database again.
#
# $STATE and Messages' own data are credential-encrypted storage, so nothing
# here runs before the first unlock after a boot.

MODDIR=${0%/*}
STATE=/data/misc_ce/0/pdw-android-messages
PKG=com.google.android.apps.messaging
DBDIR=/data/data/$PKG/databases
LOG=$STATE/logs/service.log
WAKE=pdw_android_messages_run
FLOOR=300
MIN_GAP=30

umask 077

until [ "$(getprop sys.boot_completed)" = 1 ] && [ -d "$DBDIR" ]; do
  sleep 5
done
mkdir -p "$STATE/logs" "$STATE/home" "$STATE/tmp"
chmod 700 "$STATE"

# One supervisor at a time (Magisk at boot, or the installer).
if [ -f "$STATE/supervisor.pid" ]; then
  old=$(cat "$STATE/supervisor.pid")
  if kill -0 "$old" 2>/dev/null && grep -q pdw-android-messages "/proc/$old/cmdline" 2>/dev/null; then
    exit 0
  fi
fi
echo $$ > "$STATE/supervisor.pid"

note() { echo "$(date '+%Y-%m-%d %H:%M:%S%z') $*" >> "$LOG"; }
note "started (on every Messages database write, every ${FLOOR}s as the floor)"

# The log exists only while Messages has its database open; wait for it.
watch() {
  while :; do
    if [ -f "$DBDIR/bugle_db-wal" ]; then
      inotifyd "$MODDIR/bin/android-messages-upload-wake" "$DBDIR/bugle_db-wal:c"
    else
      sleep 30
      continue
    fi
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
    echo "$WAKE 660000000000" > /sys/power/wake_lock 2>/dev/null
    timeout 660 sh "$MODDIR/bin/android-messages-upload-android" >/dev/null 2>&1
    code=$?
    echo "$WAKE" > /sys/power/wake_unlock 2>/dev/null
    [ "$code" -ne 0 ] && note "upload run exited $code (see logs/android-messages-upload.run.log)"
    last=$(date +%s)
  fi
  sleep 2
done
