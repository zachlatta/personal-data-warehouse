import { test } from 'node:test';
import assert from 'node:assert/strict';
import { androidChatOf, linkCanOpen, openOrder } from './android-chat.ts';

test('an iMessage chat names BlueBubbles and its chat guid, 1:1 or group', () => {
  for (const guid of ['any;-;+15555550100', 'iMessage;+;chat123456789', 'SMS;-;test@example.test']) {
    assert.deepEqual(androidChatOf({ app: 'bluebubbles', chat_guid: guid }), { app: 'bluebubbles', chat_guid: guid });
  }
});

test('a text conversation names Google Messages and its members, or none for the conversation list', () => {
  assert.deepEqual(androidChatOf({ app: 'google_messages', recipients: '+15555550100' }), { app: 'google_messages', recipients: '+15555550100' });
  assert.deepEqual(androidChatOf({ app: 'google_messages', recipients: '+15555550100,test@example.test' }), { app: 'google_messages', recipients: '+15555550100,test@example.test' });
  assert.deepEqual(androidChatOf({ app: 'google_messages' }), { app: 'google_messages', recipients: '' });
  assert.deepEqual(androidChatOf({ app: 'google_messages', recipients: '' }), { app: 'google_messages', recipients: '' });
});

test('anything else in a push payload is refused, not launched', () => {
  for (const value of [
    null, undefined, 'bluebubbles', {},
    { app: 'com.evil.app', chat_guid: 'any;-;+15555550100' },
    { app: 'bluebubbles' },
    { app: 'bluebubbles', chat_guid: '' },
    { app: 'bluebubbles', chat_guid: 'not a guid' },
    { app: 'bluebubbles', chat_guid: 'any;+;chat1\n' },
    { app: 'bluebubbles', chat_guid: 'any;+;' + 'x'.repeat(300) },
    { app: 'google_messages', recipients: '+15555550100?body=hi' },
    { app: 'google_messages', recipients: '+15555550100;+15555550101' },
    { app: 'google_messages', recipients: 7 },
  ]) {
    assert.equal(androidChatOf(value), null, JSON.stringify(value));
  }
});

test('a link with only an Android target opens on Android and nowhere else', () => {
  const group = { url: '', label: 'BlueBubbles', android: { app: 'bluebubbles', chat_guid: 'any;+;chat1' } };
  assert.equal(linkCanOpen(group, 'android'), true);
  assert.equal(linkCanOpen(group, 'ios'), false);
  assert.equal(linkCanOpen(group, 'web'), false);
  assert.equal(linkCanOpen({ url: 'https://example.test', label: 'Web' }, 'ios'), true);
  assert.equal(linkCanOpen({ url: '', label: 'Broken', android: { app: 'other' } }, 'android'), false);
  assert.equal(linkCanOpen(undefined, 'android'), false);
});

test('Android tries the chat target, then the native scheme, then the URL', () => {
  const link = { url: 'imessage://+15555550100', app_url: 'imessage://+15555550100', label: 'BlueBubbles', android: { app: 'bluebubbles', chat_guid: 'any;-;+15555550100' } };
  assert.deepEqual(openOrder(link, 'android'), [
    { kind: 'android', chat: { app: 'bluebubbles', chat_guid: 'any;-;+15555550100' } },
    { kind: 'url', url: 'imessage://+15555550100' },
  ]);
  assert.deepEqual(openOrder(link, 'ios'), [{ kind: 'url', url: 'imessage://+15555550100' }]);
  assert.deepEqual(openOrder(link, 'web'), [{ kind: 'url', url: 'imessage://+15555550100' }]);
  assert.deepEqual(openOrder({ url: 'https://w.test', app_url: 'slack://x', label: 'Slack' }, 'android'), [
    { kind: 'url', url: 'slack://x' },
    { kind: 'url', url: 'https://w.test' },
  ]);
  assert.deepEqual(openOrder({ url: 'https://w.test', app_url: 'slack://x', label: 'Slack' }, 'web'), [{ kind: 'url', url: 'https://w.test' }]);
});
