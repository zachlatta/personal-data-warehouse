import test from 'node:test';
import assert from 'node:assert/strict';

import { notificationOutcome, summarizeNotifications } from './notifications.ts';

const base = { id: 'n', source: 'gmail', priority: 'direct', created_at: '2026-09-10T12:00:00Z', actor: '', title: '', body: '', status: 'fanned_out', devices: 2, accepted: 0, opened: 0, failed: 0, suppressed_read: 0, suppressed_replied: 0 };

test('an opened notification reads as opened before anything else', () => {
  assert.deepEqual(notificationOutcome({ ...base, accepted: 2, opened: 1 }), { label: 'opened', tone: 'good' });
});

test('accepted by the push service is delivered, not seen', () => {
  assert.deepEqual(notificationOutcome({ ...base, accepted: 2 }), { label: 'delivered', tone: 'neutral' });
});

test('a skip is named by its reason and is not a failure', () => {
  assert.deepEqual(notificationOutcome({ ...base, suppressed_read: 2 }), { label: 'skipped: already read', tone: 'muted' });
  assert.deepEqual(notificationOutcome({ ...base, suppressed_replied: 1, suppressed_read: 1 }), { label: 'skipped: already replied', tone: 'muted' });
});

test('failures, no devices, cancellation and pending each read distinctly', () => {
  assert.deepEqual(notificationOutcome({ ...base, failed: 2 }), { label: 'failed', tone: 'bad' });
  assert.deepEqual(notificationOutcome({ ...base, status: 'no_devices', devices: 0 }), { label: 'no devices', tone: 'bad' });
  assert.deepEqual(notificationOutcome({ ...base, status: 'cancelled' }), { label: 'cancelled', tone: 'muted' });
  assert.deepEqual(notificationOutcome({ ...base, status: 'pending' }), { label: 'sending', tone: 'neutral' });
  assert.deepEqual(notificationOutcome({ ...base }), { label: 'sending', tone: 'neutral' });
});

test('a partial failure beside a delivery still counts as delivered', () => {
  assert.deepEqual(notificationOutcome({ ...base, accepted: 1, failed: 1 }), { label: 'delivered', tone: 'neutral' });
});

test('the summary counts notifications, not deliveries, and reports an open rate over delivered ones', () => {
  const events = [
    { ...base, id: 'a', accepted: 2, opened: 1 },
    { ...base, id: 'b', accepted: 2 },
    { ...base, id: 'c', suppressed_read: 2 },
    { ...base, id: 'd', failed: 2 },
    { ...base, id: 'e', status: 'no_devices', devices: 0 },
  ];
  assert.deepEqual(summarizeNotifications(events), { total: 5, delivered: 2, opened: 1, skipped: 1, failed: 2, openRate: 0.5 });
});

test('an empty ledger has no open rate rather than a division by zero', () => {
  assert.deepEqual(summarizeNotifications([]), { total: 0, delivered: 0, opened: 0, skipped: 0, failed: 0, openRate: null });
});
