import { test } from 'node:test';
import assert from 'node:assert/strict';
import { appPathForWebUrl } from './app-links.ts';

const web = 'https://personal-data-warehouse.zachlatta.com';

test('web review and timeline links map to the app screens', () => {
  assert.equal(appPathForWebUrl(`${web}/mutation-review/requests/req_abc123`), '/mutations/req_abc123');
  assert.equal(appPathForWebUrl(`${web}/mutation-review/requests`), '/mutations');
  assert.equal(appPathForWebUrl(`${web}/mutation-review/`), '/mutations');
  assert.equal(appPathForWebUrl(`${web}/timeline`), '/');
  assert.equal(appPathForWebUrl(`${web}/timeline/gmail/zach%40example.com%7C1a2b`), '/timeline/gmail/zach%40example.com%7C1a2b');
  assert.equal(appPathForWebUrl(`${web}/notifications`), '/notifications');
});

test('an event id with a slash stays one route segment', () => {
  assert.equal(appPathForWebUrl(`${web}/timeline/slack/C1/1700000000.0001`), '/timeline/slack/C1%2F1700000000.0001');
});

test('a search link opens the search tab', () => {
  assert.equal(appPathForWebUrl(`${web}/search?q=Mt%20Foolery&mode=hybrid`), '/search');
});

test('other hosts, schemes and paths are left alone', () => {
  assert.equal(appPathForWebUrl(`${web}/connections`), null);
  assert.equal(appPathForWebUrl(`${web}/api/timeline`), null);
  assert.equal(appPathForWebUrl('https://example.com/timeline'), null);
  assert.equal(appPathForWebUrl('http://personal-data-warehouse.zachlatta.com/timeline'), null);
  assert.equal(appPathForWebUrl('pdw://feed'), null);
  assert.equal(appPathForWebUrl('/mutations/abc'), null);
  assert.equal(appPathForWebUrl(`${web}/timeline/gmail/%E0%A4%A`), '/');
});
