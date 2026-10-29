import test from 'node:test';
import assert from 'node:assert/strict';
import { requestJson, ApiError } from './request.ts';

test('a stalled connection times out and aborts instead of leaving the screen loading', async () => {
  let signal;
  await assert.rejects(requestJson('https://example.test', {}, 20, async (_url, init) => {
    signal = init.signal;
    return new Promise(() => {});
  }), (error) => error instanceof ApiError && error.status === 0 && /timed out/i.test(error.message));
  assert.equal(signal.aborted, true);
});

test('the deadline includes a response whose body never arrives', async () => {
  await assert.rejects(requestJson('https://example.test', {}, 20, async () => ({
    ok: true, text: () => new Promise(() => {}),
  })), /timed out/i);
});

test('a timed out write is never retried and warns that its result is uncertain', async () => {
  let calls = 0;
  await assert.rejects(requestJson('https://example.test', { method: 'POST' }, 20, async () => {
    calls++;
    return new Promise(() => {});
  }), /check.*before.*again/i);
  assert.equal(calls, 1);
});

test('authentication and server failures keep their status and useful message', async () => {
  await assert.rejects(requestJson('https://example.test', {}, 100, async () => new Response('{"error":"Sign in again"}', { status: 401 })),
    (error) => error.status === 401 && error.message === 'Sign in again');
  assert.deepEqual(await requestJson('https://example.test', {}, 100, async () => new Response('{"items":[]}')), { items: [] });
});

test('a gateway HTML response never becomes the screen error text', async () => {
  await assert.rejects(requestJson('https://example.test', {}, 100, async () => new Response('<html>gateway</html>', { status: 502 })), /PDW is unavailable/);
});
