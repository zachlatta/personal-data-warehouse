# Android location: reviewed rollout and rollback

This runbook is for the landing operator. **Do not execute it during a build.**
The build makes no production writes and does not activate tracking. It adds one optional
helper and root uploader to the existing PDW runtime; no separate receiver/account is needed.

## Checks before activation

On mew-dev, use the reviewed commit and run `uv run pytest` plus `go test ./...` from `app/`.
Run the fake acquisition test in `tests/test_android_location_helper.py`. That test exercises
the acquisition core with synthetic fixes; it does not measure the Pixel's location provider,
Doze cadence, accuracy or battery use. Those remain unmeasured until the trial below.

Read-only inspection during the build found Android API 37 on the Pixel, existing messages,
notifications, Recorder and screen-time PDW modules, and no location helper/module. Nothing
was enabled. The inspected server images used public revision `1c6829b`; rollout moves them
to this reviewed change. The phone moves from no location helper/module to helper version 1
and module version 1; existing modules and their configurations stay in place. Do not log
coordinates, credentials, or actual account values into public evidence.

## Server first

1. Land the reviewed commit through the normal factory landing path. Main deploys both the
   Go API and Python Dagster on mew-coolify and publishes the CLI release. Do not activate the
   phone until both deployed revisions include this change. The new API route is
   `/ingest/android-location/batch`; authentication/signing are the existing PDW ingest path.
2. Reuse the existing shared object-store folder and account. No new environment variable is
   required: the asset loads with `require_android_location=True` and resolves the existing
   browser-history/shared Voice Memos folder and account. Optional overrides are
   `ANDROID_LOCATION_GOOGLE_DRIVE_FOLDER_ID` and `ANDROID_LOCATION_GOOGLE_DRIVE_ACCOUNT`;
   leave both unset for this rollout. Verify that the new
   asset and inbox sensor are loaded and the sensor is running. A registered endpoint alone
   does not mean batches will reach the warehouse.
3. Materialize the empty ingest asset once in the deployed Dagster container to provision
   the source before collection. This is production DDL and belongs only to landing:

   ```sh
   ssh mew-coolify 'D=$(sudo -n docker ps --format "{{.Names}} {{.Command}}" --no-trunc | awk "/dagster/ && !/postgres/ {print \$1}"); test "$(printf "%s\n" "$D" | wc -l)" = 1; test -n "$D"; sudo -n docker exec "$D" /app/.venv/bin/python -c "from dagster import materialize; from personal_data_warehouse.defs.android_location_drive_ingest import android_location_drive_ingest; raise SystemExit(0 if materialize([android_location_drive_ingest]).success else 1)"'
   ```

   Expect success with zero batches before activation. If container selection is ambiguous,
   stop and identify the exact deployed Dagster app by image/revision; do not run on the API
   or scanner. Read `pdw columns base_android_location.samples` and verify the table exists.
   Use the SQL checks below after the first real sample.

## Prepare artifacts on mew-dev

Keep artifacts in the ignored worktree scratch directory. Add `.factory-scratch/` to
`git rev-parse --git-path info/exclude` before creating it. Use the existing Android SDK
and signing key; do not create a new signing identity for updates.

```sh
mkdir -p .factory-scratch/location-build
PDW_BUILD_SCRATCH="$PWD/.factory-scratch/location-build" \
  bash ops/android/pdw-location-helper/build.sh "$PWD/.factory-scratch/location-helper.apk"
```

Build the arm64 CLI from the reviewed app source, using CI's CGO-free release flags:

```sh
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go -C app build -trimpath \
  -ldflags "-s -w -X main.version=reviewed-location" \
  -o "$PWD/.factory-scratch/pdw-arm64" ./cmd/pdw-cli
sha256sum .factory-scratch/pdw-arm64 .factory-scratch/location-helper.apk
```

Do not deploy an older CLI: it cannot upload this source. Record these hashes in private
rollout evidence and compare the phone binary hash after transfer. Verify the command help
before starting the service.

## Install paused, then explicitly enable

Run from the reviewed checkout on mew-dev. All commands in this section are production
changes reserved for landing. The target is `pichu` over root SSH. This initial installation
refuses an existing location module; upgrades need preservation of its signing key and queue.
The helper package is separate from the main PDW app, so installing it does not replace that
app or clear its notifications.

The module is `/data/adb/modules/pdw-android-location`; credentials, logs and pause switch
are `/data/misc_ce/0/pdw-android-location` (root 0700, after first unlock). The helper queue
is `/data/user/0/com.zachlatta.pdw.location/files/spool` in credential-encrypted app storage.

Before proceeding, unlock once after boot; verify `sys.user.0.ce_available` is `true`.
Choose the reviewed CLI artifact as `.factory-scratch/pdw-arm64`. The following transfers
only code and a helper APK; credentials are copied entirely on the phone from screen time.

```sh
ssh pichu 'test "$(getprop sys.user.0.ce_available)" = true && test ! -e /data/adb/modules/pdw-android-location'
ssh pichu 'umask 077; mkdir -p /data/misc_ce/0/pdw-android-location/config/pdw /data/misc_ce/0/pdw-android-location/home /data/misc_ce/0/pdw-android-location/tmp /data/misc_ce/0/pdw-android-location/logs; chmod 700 /data/misc_ce/0/pdw-android-location; touch /data/misc_ce/0/pdw-android-location/paused; cp /data/misc_ce/0/pdw-android-screen-time/config/pdw/config.json /data/misc_ce/0/pdw-android-location/config/pdw/config.json; sed -n "s/^ANDROID_SCREEN_TIME_DEVICE=/ANDROID_LOCATION_DEVICE=/p; s/^ANDROID_SCREEN_TIME_ACCOUNT=/ANDROID_LOCATION_ACCOUNT=/p" /data/misc_ce/0/pdw-android-screen-time/.env > /data/misc_ce/0/pdw-android-location/.env; chmod 600 /data/misc_ce/0/pdw-android-location/.env /data/misc_ce/0/pdw-android-location/config/pdw/config.json; test -s /data/misc_ce/0/pdw-android-location/.env'
ssh pichu 'mkdir -p /data/adb/modules/pdw-android-location/bin; touch /data/adb/modules/pdw-android-location/skip_mount'
scp ops/android/pdw-android-location/service.sh pichu:/data/adb/modules/pdw-android-location/service.sh
scp bin/android-location-upload-android bin/_pdw-upload-lib.sh pichu:/data/adb/modules/pdw-android-location/bin/
scp .factory-scratch/pdw-arm64 pichu:/data/adb/modules/pdw-android-location/bin/pdw
```

Install via stdin so no APK is left on the phone. `pm install` must report `Success`:

```sh
bytes=$(stat -c %s .factory-scratch/location-helper.apk)
ssh pichu "pm install -r -S $bytes" < .factory-scratch/location-helper.apk
ssh pichu 'printf "id=pdw-android-location\nname=PDW approximate location\nversion=1\nversionCode=1\nauthor=zrl\ndescription=Approximate network location every fifteen minutes\n" > /data/adb/modules/pdw-android-location/module.prop; chmod 755 /data/adb/modules/pdw-android-location/service.sh /data/adb/modules/pdw-android-location/bin/pdw /data/adb/modules/pdw-android-location/bin/android-location-upload-android'
```

Grant only coarse and background location. Do not grant fine location, switch on a disabled
system location setting, request GPS, or exempt the device globally from Doze:

```sh
ssh pichu 'pm grant com.zachlatta.pdw.location android.permission.ACCESS_COARSE_LOCATION; pm grant com.zachlatta.pdw.location android.permission.ACCESS_BACKGROUND_LOCATION; dumpsys package com.zachlatta.pdw.location'
```

Check both grants show granted, no fine permission, and the provider has DUMP protection.
If grants/provider fail, leave paused and report the failure; do not broaden permissions.
Check system location availability read-only with `cmd location is-location-enabled`.
If disabled, leave paused and request a separate decision to enable system location.
Check the binary's command while paused:

```sh
ssh pichu '/data/adb/modules/pdw-android-location/bin/pdw ingest android-location --help'
```

Start the paused supervisor, then remove the pause file only after the server checks pass:

```sh
ssh pichu 'setsid sh /data/adb/modules/pdw-android-location/service.sh </dev/null >/dev/null 2>&1 &'
ssh pichu 'unlink /data/misc_ce/0/pdw-android-location/paused'
```

This is the first activation of tracking. Expect one bounded network fix per run, about every
900 seconds while scheduling permits. A provider timeout is an error, not a fabricated fix.
No continuous GPS subscription is created. An outage retains queued samples for retry.

## Final verification and trial

Use `pdw columns` first; the following queries are bounded and avoid disclosing coordinates:

```sql
SELECT device, measured_at, received_at, accuracy_m
FROM base_android_location.samples ORDER BY measured_at DESC LIMIT 10;
SELECT occurred_at, priority, source_table
FROM timeline.events WHERE source = 'android_location'
ORDER BY occurred_at DESC LIMIT 10;
SELECT pipeline, status, newest_event_at, last_run_at, last_error
FROM marts_ops.pipeline_health WHERE pipeline = 'android_location';
```

Check queue count and heartbeat without printing sample contents:

```sh
ssh pichu 'ls /data/user/0/com.zachlatta.pdw.location/files/spool | wc -l; cat /data/misc_ce/0/pdw-android-location/logs/android-location-upload.heartbeat'
```

Check that a measurement reaches the raw table and timeline, has a positive accuracy radius,
keeps its original measured time, and has a separate server receipt time. Check offline
recovery on test data during build; do not disable the Pixel's network as a live test.
A fresh heartbeat cannot clear stale measurement health. Inspect acquisition errors and
queued counts if measurements stop. No coordinate-bearing logs should be published.

**Not yet measured:** physical-device fix acquisition, cadence under screen-off/Doze,
accuracy, provider availability, or extra battery drain. Collect two comparable baseline
days paused and two enabled days, noting unplugged hours, charging, screen use, travel and
signal conditions. Compare battery percentage points per unplugged hour and the Android
battery attribution, plus measurement gaps and age. Proposed acceptance: at most five extra
percentage points per day and useful approximately fifteen-minute measurements during normal
use. This is an acceptance target, not a prediction or guaranteed interval. Pause on excessive
drain or repeated stale/missing fixes; do not silently increase accuracy or wake frequency.

## Rollback

Pause first; verify the PID belongs to this service before stopping it. Leave all queued
samples, credentials and server history intact. No database rollback is needed for additive
objects; rolling back the server while a phone still uploads is unsafe.

```sh
ssh pichu 'touch /data/misc_ce/0/pdw-android-location/paused; touch /data/adb/modules/pdw-android-location/disable'
ssh pichu 'pid=$(cat /data/misc_ce/0/pdw-android-location/supervisor.pid); case "$pid" in ""|*[!0-9]*) exit 1;; esac; tr "\000" "\n" < /proc/$pid/cmdline | grep -Fxq /data/adb/modules/pdw-android-location/service.sh && kill "$pid"'
ssh pichu 'am force-stop com.zachlatta.pdw.location; pm revoke com.zachlatta.pdw.location android.permission.ACCESS_BACKGROUND_LOCATION; pm revoke com.zachlatta.pdw.location android.permission.ACCESS_COARSE_LOCATION'
```

An already running upload may finish; verify no child uploader remains and measurement count
stops increasing for at least two intervals. `disable` prevents restart at the next boot.
Do not uninstall the helper: uninstall deletes its offline queue. Do not delete Drive objects,
raw rows, timeline history or backups as part of rollback. If reverting server code, pause and
stop the phone first, then revert the release through normal reviewed deployment. To resume,
review the failure, preserve the same signing identity, restore permissions and remove both
pause and disable switches before starting exactly one supervisor.

## Landing communication

After successful deployment and checks, the landing operator replies in the original request
thread to its latest message with short plain HTML plus text, and archives only after success.
State that tracking is enabled only if activation and real acquisition actually passed; state
that battery/cadence results are pending. The builder must not send or archive.
