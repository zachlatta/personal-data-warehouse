import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const require = createRequire(import.meta.url);
const plugin = require('../../plugins/with-messaging-notifications.js');
const mobile = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');

test('the manifest swaps Expo FCM service for the one that draws alerts', () => {
  const manifest = { manifest: { $: {}, application: [{ $: { 'android:name': '.MainApplication' }, service: [] }] } };
  plugin.applyManifest(plugin.applyManifest(manifest));
  const services = manifest.manifest.application[0].service;
  assert.equal(manifest.manifest.$['xmlns:tools'], 'http://schemas.android.com/tools');
  assert.deepEqual(services.map((s) => [s.$['android:name'], s.$['tools:node'] ?? null]), [
    [plugin.EXPO_SERVICE, 'remove'],
    ['.PdwMessagingService', null],
  ]);
  assert.equal(services[1]['intent-filter'][0].action[0].$['android:name'], 'com.google.firebase.MESSAGING_EVENT');
});

test('the app module compiles against Firebase messaging, once', () => {
  const gradle = 'android {\n}\n\ndependencies {\n    implementation("com.facebook.react:react-android")\n}\n';
  const once = plugin.applyGradle(gradle);
  assert.equal(plugin.applyGradle(once), once);
  assert.match(once, /dependencies \{\n {4}implementation 'com\.google\.firebase:firebase-messaging:[\d.]+'/);
});

// The service subclasses expo-notifications internals. If an Expo upgrade
// moves or renames one, this fails here rather than in an EAS build, or worse,
// in a build whose alerts silently stop.
test('every Expo class the Kotlin uses exists in the installed expo-notifications', () => {
  const kotlin = plugin.KOTLIN('com.zachlatta.pdw');
  const root = path.join(mobile, 'node_modules/expo-notifications/android/src/main/java');
  const imports = [...kotlin.matchAll(/^import (expo\.modules\.notifications\.[\w.]+)$/gm)].map((m) => m[1]);
  assert.ok(imports.length >= 6);
  for (const name of imports) {
    const base = path.join(root, ...name.split('.'));
    assert.ok(existsSync(base + '.kt') || existsSync(base + '.java'), `${name} is gone from expo-notifications`);
  }
  const service = readFileSync(path.join(root, 'expo/modules/notifications/service/ExpoFirebaseMessagingService.kt'), 'utf8');
  assert.match(service, /open class ExpoFirebaseMessagingService/);
  assert.match(service, /protected open val firebaseMessagingDelegate/);
  const delegate = readFileSync(path.join(root, 'expo/modules/notifications/service/delegates/FirebaseMessagingDelegate.kt'), 'utf8');
  assert.match(delegate, /protected fun createNotification\(remoteMessage: RemoteMessage\)/);
  assert.match(delegate, /fun runTaskManagerTasks\(applicationContext: Context, bundle: Bundle\)/);
  const gradle = readFileSync(path.join(mobile, 'node_modules/expo-notifications/android/build.gradle'), 'utf8');
  for (const line of kotlinDependencies()) assert.ok(gradle.includes(line), `expo-notifications no longer uses ${line}`);
});

test('the native renderer and the server agree on the channels and payload keys', () => {
  const kotlin = plugin.KOTLIN('com.zachlatta.pdw');
  const goPush = readFileSync(path.join(mobile, '../app/internal/push/capabilities.go'), 'utf8');
  const goSender = readFileSync(path.join(mobile, '../app/internal/notifications/sender.go'), 'utf8');
  const channel = /AndroidMessagesChannelID = "(\w+)"/.exec(goPush)[1];
  assert.match(kotlin, new RegExp(`CHANNEL_ID = "${channel}"`));
  assert.match(goSender, /data\["android_style"\]/);
  assert.match(kotlin, /STYLE_KEY = "android_style"/);
  const app = JSON.parse(readFileSync(path.join(mobile, 'app.json'), 'utf8')).expo;
  const capability = /CapabilityAndroidNativeAlerts = "(\w+)"/.exec(goPush)[1];
  assert.deepEqual(app.extra.pushCapabilities, [capability]);
  assert.ok(app.plugins.includes('./plugins/with-messaging-notifications'));

  const goExpo = readFileSync(path.join(mobile, '../app/internal/push/expo.go'), 'utf8');
  const goNotifier = readFileSync(path.join(mobile, '../app/internal/push/notifier.go'), 'utf8');
  const pushTs = readFileSync(path.join(mobile, 'src/lib/push.ts'), 'utf8');
  assert.match(kotlin, new RegExp(`ALERT_KEY = "${/AndroidAlertKey = "(\w+)"/.exec(goExpo)[1]}"`));
  const alertChannel = /AndroidChannelID = "(\w+)"/.exec(goNotifier)[1];
  assert.match(kotlin, new RegExp(`ALERT_CHANNEL_ID = "${alertChannel}"`));
  assert.match(pushTs, new RegExp(`ANDROID_CHANNEL_ID = '${alertChannel}'`));
  const dataOnly = goExpo.slice(goExpo.indexOf('func (m Message) AndroidDataOnly'), goExpo.indexOf('// RichContent is the media'));
  const fields = [...dataOnly.matchAll(/"(\w+)":/g), ...dataOnly.matchAll(/alert\["(\w+)"\]/g)].map((m) => m[1]);
  assert.deepEqual([...fields].sort(), ['body', 'collapse_id', 'image_url', 'silent', 'subtitle', 'title']);
  for (const field of fields) assert.match(kotlin, new RegExp(`opt\\w+\\("${field}"\\)`), `the phone does not read ${field}`);
});

// The phone's notification listener uploads string extras. Stamping the
// push's delivery id on every alert the app draws is how the server learns
// which phone (listener account/device) a push device is, and so whose
// notifications may stand in for an alert to it.
test('every alert the app draws carries its delivery id, under the key the server reads', () => {
  const kotlin = plugin.KOTLIN('com.zachlatta.pdw');
  const goSender = readFileSync(path.join(mobile, '../app/internal/notifications/sender.go'), 'utf8');
  const goWitness = readFileSync(path.join(mobile, '../app/internal/notifications/suppression.go'), 'utf8');
  const key = /DELIVERY_ID_EXTRA = "([\w.]+)"/.exec(kotlin)[1];
  assert.equal(key, 'pdw.delivery_id');
  assert.match(goWitness, new RegExp(`'${key.replace('.', '\\.')}'`));
  assert.match(goSender, /"delivery_id": d\.ID/);
  assert.match(kotlin, /putString\(DELIVERY_ID_EXTRA, data\.optString\("delivery_id"\)\)/);
  // The chat, the plain chat and the alert (with or without a picture) all carry it.
  for (const fn of ['fun post(', 'fun postPlain(', 'private fun drawAlert(']) {
    const start = kotlin.indexOf(fn);
    const body = kotlin.slice(start, kotlin.indexOf('\n  }\n', start));
    assert.match(body, /addExtras\(deliveryExtras\(data\)\)/, `${fn} does not stamp the delivery id`);
  }
});

test('a non-chat alert goes through the same eviction, pacing and tap as a chat', () => {
  const kotlin = plugin.KOTLIN('com.zachlatta.pdw');
  assert.match(kotlin, /@Synchronized\s+fun postAlert\(/);
  assert.match(kotlin, /@Synchronized\s+fun postAlertPlain\(/);
  assert.match(kotlin, /evictIfNeeded\(context, ALERT_TAG, id\)\s+paceNotify\(\)\s+NotificationManagerCompat\.from\(context\)\.notify\(ALERT_TAG, id, builder\.build\(\)\)/);
  // Only chats are spared until last: an alert's own tag must be evicted before them.
  assert.equal(/ALERT_TAG = "([\w-]+)"/.exec(kotlin)[1], 'pdw-alert');
  assert.match(kotlin, /compareBy<StatusBarNotification> \{ it\.tag == NOTIFICATION_TAG \}/);
  // An alert without a collapse id must not replace the previous one.
  assert.match(kotlin, /optString\("collapse_id"\)\.ifBlank \{ tap\.notificationRequest\.identifier \}/);
  assert.match(kotlin, /base\(context, tap, ALERT_CHANNEL_ID\)/);
  assert.match(kotlin, /setSilent\(alert\.optBoolean\("silent"\)\)/);
  // One place builds the tap, so handleNotificationResponse sees one kind of alert.
  assert.equal(kotlin.match(/createNotificationResponseIntent/g).length, 1);
  // A native-drawn alert has no action buttons: an Approve with no running JS waits for the next launch.
  assert.doesNotMatch(kotlin, /addAction/);
  assert.match(kotlin, /if \(chat\) PdwNotifications\.postPlain\(context, data, tap\)\s+else PdwNotifications\.postAlertPlain\(/);
});

test('notifications evict above the 30-notification shade ceiling, merge 5s bursts, cache avatars, pace notify, and serialize event_ids as a string', () => {
  const kotlin = plugin.KOTLIN('com.zachlatta.pdw');
  assert.match(kotlin, /MAX_ACTIVE_NOTIFICATIONS = 30/);
  assert.match(kotlin, /FLAG_GROUP_SUMMARY/);
  assert.match(kotlin, /compareBy<StatusBarNotification> \{ it\.tag == NOTIFICATION_TAG \}\.thenBy \{ it\.postTime \}/);
  assert.match(kotlin, /manager\.cancel\(sbn\.tag, sbn\.id\)/);
  assert.match(kotlin, /while \(waited < CANCEL_POLL_MAX_MS\)/);
  assert.match(kotlin, /@Synchronized\s+fun post\(/);
  assert.match(kotlin, /@Synchronized\s+fun postPlain\(/);
  assert.match(kotlin, /RECENT_POST_TTL_MS = 5000L/);
  assert.match(kotlin, /recentPosts\.remove\(id\)/);
  // The last in-process post is the newest state; a same-length stale shade copy at the 25-message cap must not win.
  assert.match(kotlin, /recent\?\.shown \?:/);
  assert.match(kotlin, /recent\?\.messages\s+\?:/);
  assert.doesNotMatch(kotlin, /existingMessages\.size < recent\.messages\.size/);
  assert.match(kotlin, /LruCache<String, Bitmap>\(AVATAR_CACHE_SIZE\)/);
  assert.match(kotlin, /MIN_NOTIFY_INTERVAL_MS = 200L/);
  assert.match(kotlin, /SystemClock\.elapsedRealtime\(\)/);
  assert.match(kotlin, /putString\(EVENT_IDS_EXTRA, shown\.joinToString\("\\n"\)\)/);
  assert.match(kotlin, /getString\(EVENT_IDS_EXTRA\)/);
  assert.doesNotMatch(kotlin, /putStringArrayList/);
  assert.doesNotMatch(kotlin, /getStringArrayList/);
});

function kotlinDependencies() {
  const source = readFileSync(path.join(mobile, 'plugins/with-messaging-notifications.js'), 'utf8');
  return [...source.matchAll(/"(implementation '[^']+')"/g)].map((m) => m[1]);
}
