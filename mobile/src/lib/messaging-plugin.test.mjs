import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const require = createRequire(import.meta.url);
const plugin = require('../../plugins/with-messaging-notifications.js');
const mobile = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');

test('the manifest swaps Expo FCM service for the chat-drawing one', () => {
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
// in a build whose chat alerts silently stop.
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

test('the native renderer and the server agree on the channel and payload key', () => {
  const kotlin = plugin.KOTLIN('com.zachlatta.pdw');
  const goPush = readFileSync(path.join(mobile, '../app/internal/push/capabilities.go'), 'utf8');
  const goSender = readFileSync(path.join(mobile, '../app/internal/notifications/sender.go'), 'utf8');
  const channel = /AndroidMessagesChannelID = "(\w+)"/.exec(goPush)[1];
  assert.match(kotlin, new RegExp(`CHANNEL_ID = "${channel}"`));
  assert.match(goSender, /data\["android_style"\]/);
  assert.match(kotlin, /STYLE_KEY = "android_style"/);
  const app = JSON.parse(readFileSync(path.join(mobile, 'app.json'), 'utf8')).expo;
  const capability = /CapabilityAndroidMessagingStyle = "(\w+)"/.exec(goPush)[1];
  assert.deepEqual(app.extra.pushCapabilities, [capability]);
  assert.ok(app.plugins.includes('./plugins/with-messaging-notifications'));
});

function kotlinDependencies() {
  const source = readFileSync(path.join(mobile, 'plugins/with-messaging-notifications.js'), 'utf8');
  return [...source.matchAll(/"(implementation '[^']+')"/g)].map((m) => m[1]);
}
