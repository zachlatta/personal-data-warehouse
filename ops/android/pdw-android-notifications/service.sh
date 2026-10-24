#!/system/bin/sh
# Magisk late_start service for the pdw-android-notifications module (the
# Pixel). Deployed by scripts/install-pixel-notification-upload in the
# sysadmin repository, which installs the PDW notification listener APK and
# copies this directory, bin/android-notifications-upload-android,
# bin/_pdw-upload-lib.sh and a release linux/arm64 pdw into the module.
#
# The listener (com.zachlatta.pdw.notifications) writes one JSON file per
# notification event into its spool. This service sends the spool to PDW
# seconds after a file lands, and every five minutes as the floor:
#
# - inotifyd watches the spool. Each finished file (the listener renames it
#   into place) runs bin/android-notifications-upload-wake, which marks a run
#   due and takes a 30 s auto-expiring kernel wakelock, so a notification
#   that arrives with the screen off is sent before the phone suspends.
# - The loop below starts a run when one is due (at most one every
#   $MIN_GAP seconds, so a burst of notifications is one batch) or five
#   minutes have passed, holding its own wakelock for the run.
# - Notification access is re-granted at start: an app update or a settings
#   reset can drop it, and nothing else would notice.
#
# Switch (a file in $STATE): `paused` stops uploads; the listener keeps
# writing its spool, so nothing is lost while paused.
#
# $STATE and the listener's own data are credential-encrypted storage, so
# nothing here runs before the first unlock after a boot.

MODDIR=${0%/*}
STATE=/data/misc_ce/0/pdw-android-notifications
PKG=com.zachlatta.pdw.notifications
LISTENER=$PKG/$PKG.SpoolListenerService
SPOOL=/data/data/$PKG/files/spool
LOG=$STATE/logs/service.log
WAKE=pdw_android_notifications_run
FLOOR=300
MIN_GAP=15

umask 077

until [ "$(getprop sys.boot_completed)" = 1 ] && [ -d "/data/data/$PKG" ]; do
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

cmd notification allow_listener "$LISTENER" >/dev/null 2>&1
if settings get secure enabled_notification_listeners | grep -q "$LISTENER"; then
  note "started (listener has notification access; on every spool write, every ${FLOOR}s as the floor)"
else
  note "started, but the listener has NO notification access (cmd notification allow_listener failed)"
fi

# The spool appears with the listener's first event; until then watch its
# parent so the first file is still noticed.
watch() {
  while :; do
    if [ -d "$SPOOL" ]; then
      inotifyd "$MODDIR/bin/android-notifications-upload-wake" "$SPOOL:y"
    else
      sleep 30
      continue
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
    echo "$WAKE 660000000000" > /sys/power/wake_lock 2>/dev/null
    timeout 660 sh "$MODDIR/bin/android-notifications-upload-android" >/dev/null 2>&1
    code=$?
    echo "$WAKE" > /sys/power/wake_unlock 2>/dev/null
    [ "$code" -ne 0 ] && note "upload run exited $code (see logs/android-notifications-upload.run.log)"
    last=$(date +%s)
  fi
  sleep 2
done
