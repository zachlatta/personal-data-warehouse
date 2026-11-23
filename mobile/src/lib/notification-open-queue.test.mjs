import { test } from 'node:test';
import assert from 'node:assert/strict';
import { OpenQueue, notificationOpenLink } from './notification-open-queue.ts';

test('Android Messages source links survive notification validation', () => {
 for (const url of ['sms:+15555550123', 'sms:test+chat@example.test']) {
  const link = { url, app_url: url, label: 'Messages' };
  assert.deepEqual(notificationOpenLink(link), link);
 }
});

test('chat links keep their Android target, and a group link with no URL survives on it alone', () => {
 const oneToOne = { url: 'imessage://+15555550123', app_url: 'imessage://+15555550123', label: 'BlueBubbles', android: { app: 'bluebubbles', chat_guid: 'any;-;+15555550123' } };
 assert.deepEqual(notificationOpenLink(oneToOne), oneToOne);
 const group = { url: '', label: 'BlueBubbles', android: { app: 'bluebubbles', chat_guid: 'any;+;chat123' } };
 assert.deepEqual(notificationOpenLink(group), group);
 const rcsGroup = { url: '', label: 'Messages', android: { app: 'google_messages' } };
 assert.deepEqual(notificationOpenLink(rcsGroup), { url: '', label: 'Messages', android: { app: 'google_messages', recipients: '' } });
 // An unknown app is dropped; with no URL left the link is refused.
 assert.equal(notificationOpenLink({ url: '', label: 'Bad', android: { app: 'com.evil', chat_guid: 'x' } }), null);
 assert.deepEqual(notificationOpenLink({ url: 'sms:+15555550123', label: 'Messages', android: { app: 'com.evil' } }), { url: 'sms:+15555550123', label: 'Messages' });
});

test('offline opens survive a new queue instance and retries are idempotent', async () => {
 const values = new Map(); const kv = { get: async k => values.get(k) ?? null, set: async (k,v) => { values.set(k,v); }, remove: async k => { values.delete(k); } };
 const open = { delivery_id: 'one', open_proof: 'proof', baseUrl: 'https://example.test' };
 const q = new OpenQueue(kv);
 await q.add(open); await q.add(open);
 await q.flush(async () => { throw new Error('offline'); });
 let count = 0;
 await new OpenQueue(kv).flush(async record => { assert.deepEqual(record, open); count++; return true; });
 assert.equal(count, 1);
 await q.flush(async () => { throw new Error('already drained'); });
});
test('source link rejects executable schemes and malformed links', () => {
 assert.equal(notificationOpenLink({ url: 'javascript:alert(1)', label: 'Bad' }), null);
 assert.deepEqual(notificationOpenLink({ url: 'https://example.test', app_url: 'slack://channel?id=C', label: 'Slack' }), { url: 'https://example.test', app_url: 'slack://channel?id=C', label: 'Slack' });
});

test('a killed enqueue is recovered from its write-ahead record', async () => {
 for (const failAt of [2,3,4]) {
  const values = new Map(); let writes=0;
  const kv = { get: async k=>values.get(k)??null, set: async(k,v)=>{if(++writes===failAt)throw Error('killed');values.set(k,v);}, remove:async k=>{if(++writes===failAt)throw Error('killed');values.delete(k);} };
  const record={delivery_id:'recover',open_proof:'proof',baseUrl:'https://example.test'};
  await new OpenQueue(kv).add(record).catch(()=>{});
  writes=100;
  const sent=[]; await new OpenQueue(kv).flush(async r=>{sent.push(r);return true;});
  assert.deepEqual(sent,[record], 'crash at write '+failAt);
 }
});

test('source and PDW navigation do not wait for durable recording', async () => {
 const { prepareNotificationOpen } = await import('./notification-open-queue.ts');
 let finish; let persisted=false;
 const flow=await prepareNotificationOpen(async()=>false,()=>new Promise(resolve=>{finish=()=>{persisted=true;resolve()};}));
 assert.equal(flow.sourceOpened,false); assert.equal(persisted,false);
 finish();assert.equal(await flow.recorded,true);
 const failed=await prepareNotificationOpen(async()=>true,async()=>{throw Error('disk unavailable')});
 assert.equal(failed.sourceOpened,true);assert.equal(await failed.recorded,false);
});
