import { test } from 'node:test';
import assert from 'node:assert/strict';
import { pushCapabilities } from './push-capabilities.ts';

test('an Android build reports the capabilities its config declares', () => {
  assert.deepEqual(pushCapabilities({ pushCapabilities: ['android_messaging_style'] }, 'android'), ['android_messaging_style']);
});

test('iOS never claims an Android capability, and junk is ignored', () => {
  assert.deepEqual(pushCapabilities({ pushCapabilities: ['android_messaging_style'] }, 'ios'), []);
  assert.deepEqual(pushCapabilities({ pushCapabilities: 'android_messaging_style' }, 'android'), []);
  assert.deepEqual(pushCapabilities({ pushCapabilities: [1, null, 'android_messaging_style'] }, 'android'), ['android_messaging_style']);
  assert.deepEqual(pushCapabilities(undefined, 'android'), []);
});
