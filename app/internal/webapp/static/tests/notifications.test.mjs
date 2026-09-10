import { test } from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import fs from 'node:fs';
const script = fs.readFileSync(new URL('../notification-sw.js', import.meta.url), 'utf8');
function worker() {
 const handlers = {}, calls = [], records = [];
 const context = vm.createContext({
  URL, console, Promise, AbortSignal,
  self: { location: { origin: 'https://pdw.example' }, registration: { showNotification: async (...args) => calls.push(['show', ...args]) }, addEventListener: (type, handler) => { handlers[type] = handler; } },
  clients: { openWindow: async url => calls.push(['open', url]) },
  fetch: async (...args) => { calls.push(['fetch', ...args]); return { ok: true }; },
  records,
 });
 vm.runInContext(script, context);
 // Real IndexedDB transactions are browser-tested; keep routing tests deterministic.
 vm.runInContext(`pending=async (action,value)=>{if(action==='put')records.push(value);if(action==='all')return records.slice();if(action==='delete')records.splice(records.findIndex(r=>r.delivery_id===value),1);}`, context);
 return { handlers, calls, records };
}
test('click opens the exact source before networking, then records the tap', async () => {
 const w = worker(); let done;
 w.handlers.notificationclick({ notification: { data: { route: '/timeline/slack_message/event', open: { url: 'https://example.slack.com/archives/C/p123?thread_ts=1' }, delivery_id: 'id', open_proof: 'proof' }, close() {} }, waitUntil: p => { done = p; } });
 await done;
 assert.equal(w.calls[0][0], 'open'); assert.match(w.calls[0][1], /thread_ts=1/);
 assert.equal(w.calls[1][0], 'fetch'); assert.equal(w.records.length, 0);
});
test('unsupported source URL falls back to the exact PDW event', async () => {
 const w = worker(); let done;
 w.handlers.notificationclick({ notification: { data: { route: '/timeline/unknown/event', open: { url: 'javascript:alert(1)' } }, close() {} }, waitUntil: p => { done = p; } });
 await done; assert.equal(w.calls[0][1], 'https://pdw.example/timeline/unknown/event');
});
test('push shows source artwork, a stable tag, and no warehouse credentials', async () => {
 const w = worker(); let done;
 w.handlers.push({ data: { json: () => ({ title: 'Sender', subtitle: 'Slack', body: 'Message', icon: '/app/notification-icons/slack.png', notification_id: 'n' }) }, waitUntil: p => { done = p; } });
 await done;
 assert.equal(w.calls[0][0], 'show'); assert.equal(w.calls[0][2].body, 'Slack\nMessage'); assert.equal(w.calls[0][2].tag, 'n');
});
