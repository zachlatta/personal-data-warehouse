import { test } from 'node:test';
import assert from 'node:assert/strict';
import { pushCapabilities } from './push-capabilities.ts';

test('an Android build reports the capabilities its config declares', () => {
  assert.deepEqual(pushCapabilities({ pushCapabilities: ['android_native_alerts'] }, 'android'), ['android_native_alerts']);
});

test('iOS never claims an Android capability, and junk is ignored', () => {
  assert.deepEqual(pushCapabilities({ pushCapabilities: ['android_native_alerts'] }, 'ios'), []);
  assert.deepEqual(pushCapabilities({ pushCapabilities: 'android_native_alerts' }, 'android'), []);
  assert.deepEqual(pushCapabilities({ pushCapabilities: [1, null, 'android_native_alerts'] }, 'android'), ['android_native_alerts']);
  assert.deepEqual(pushCapabilities(undefined, 'android'), []);
});
