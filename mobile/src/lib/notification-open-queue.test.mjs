import { test } from 'node:test';
import assert from 'node:assert/strict';
import { OpenQueue, notificationOpenLink } from './notification-open-queue.ts';

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
