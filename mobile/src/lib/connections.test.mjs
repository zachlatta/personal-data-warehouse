import assert from 'node:assert/strict';
import test from 'node:test';
import { accessLabel, authMessage, authResultFromUrl, clientNames, connectionState, scopeList, validConnectionName } from './connections.ts';

test('connection names match the server rule', () => {
  for (const ok of ['skills', 'hc', 'a1', 'my_tools']) assert.equal(validConnectionName(ok), true, ok);
  for (const bad of ['', 'Skills', '1x', 'x_', 'a__b', 'a'.repeat(25), 'has-dash']) assert.equal(validConnectionName(bad), false, bad);
});

test('client names and scopes are split and deduplicated', () => {
  assert.deepEqual(clientNames(' codex, claude ,,codex '), ['codex', 'claude']);
  assert.deepEqual(scopeList('  read  write '), ['read', 'write']);
  assert.deepEqual(scopeList(''), []);
});

test('state and access read the way the web page says them', () => {
  assert.deepEqual(connectionState({ enabled: true, status: 'connected' }), { label: 'Connected', tone: 'good' });
  assert.deepEqual(connectionState({ enabled: true, status: 'discovery_failed' }), { label: 'Discovery failed', tone: 'bad' });
  assert.deepEqual(connectionState({ enabled: true, status: 'not_connected' }), { label: 'Not connected', tone: 'neutral' });
  assert.deepEqual(connectionState({ enabled: false, status: 'connected' }), { label: 'Disabled', tone: 'muted' });
  assert.equal(accessLabel({ all_clients: true, clients: ['web'] }), 'All PDW clients');
  assert.equal(accessLabel({ all_clients: false, clients: null }), 'Not shared');
  assert.equal(accessLabel({ all_clients: false, clients: ['codex', 'android'] }), 'codex, android');
});

test('the OAuth return link carries the result', () => {
  assert.equal(authResultFromUrl('pdw://connections?auth=connected'), 'connected');
  assert.equal(authResultFromUrl('pdw://connections?auth=discovery_failed'), 'discovery_failed');
  assert.equal(authResultFromUrl('pdw://connections'), null);
  assert.match(authMessage('connected'), /^Connected/);
  assert.match(authMessage('failed'), /failed or expired/);
});
