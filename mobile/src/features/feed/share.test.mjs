import test from 'node:test';
import assert from 'node:assert/strict';

import { shareDraft } from './share.ts';

test('the title is the subject and the link and summary are the body', () => {
  assert.deepEqual(shareDraft({ title: 'Show HN: A thing', url: 'https://example.com/a?b=1&c=2' }, 'It does  a\nthing.'), {
    subject: 'Show HN: A thing',
    body: 'Show HN: A thing\nhttps://example.com/a?b=1&c=2\n\nIt does a thing.',
  });
});

test('a card with no url or summary still makes a draft', () => {
  assert.deepEqual(shareDraft({ title: 'Briefing' }), { subject: 'Briefing', body: 'Briefing' });
  assert.deepEqual(shareDraft({ url: 'https://example.com' }), { subject: 'https://example.com', body: 'https://example.com' });
});

test('a long summary is cut at a sentence', () => {
  const long = `${'First sentence is here. '.repeat(12)}${'word '.repeat(80)}`;
  const { body } = shareDraft({ title: 'T' }, long);
  const summary = body.split('\n\n')[1];
  assert.ok(summary.length <= 400, String(summary.length));
  assert.ok(summary.endsWith('here.'), summary);
});

test('a long summary with no sentence break ends in an ellipsis', () => {
  const { body } = shareDraft({ title: 'T' }, 'word '.repeat(200));
  assert.ok(body.endsWith('word…'), body.slice(-20));
});
