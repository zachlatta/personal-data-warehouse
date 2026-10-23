import assert from 'node:assert/strict';
import test from 'node:test';
import { defaultClientName, notificationSettingsName, secureStoreName } from './platform-copy.ts';

test('each phone names its own token store, settings app and client', () => {
  assert.equal(secureStoreName('ios'), 'the iOS Keychain');
  assert.equal(secureStoreName('android'), 'the Android Keystore');
  assert.equal(notificationSettingsName('ios'), 'iOS Settings');
  assert.equal(notificationSettingsName('android'), 'Android Settings → Apps → PDW → Notifications');
  assert.equal(defaultClientName('ios'), 'iphone');
  assert.equal(defaultClientName('android'), 'android');
});
