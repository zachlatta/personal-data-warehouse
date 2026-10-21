import test from 'node:test';
import assert from 'node:assert/strict';

import { mailtoUrl, shareDraft, shareUrls } from './share.ts';

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

test('the Superhuman compose route carries the whole mailto, which reads back intact', () => {
  const draft = shareDraft({ title: 'A & B: 100%?', url: 'https://example.com/a?b=1&c=2#x' }, 'Plus+signs = fine.');
  const [scheme, universal, mailto] = shareUrls(draft);
  assert.equal(mailto, mailtoUrl(draft));
  assert.ok(scheme.startsWith('superhuman:///compose/mailto%3A%3Fsubject%3D'), scheme);
  assert.ok(universal.startsWith('https://mail.superhuman.com/compose/mailto%3A%3Fsubject%3D'), universal);
  // What Superhuman does: decode the path once, then read the mailto's query.
  const decoded = decodeURIComponent(universal.replace('https://mail.superhuman.com/compose/', ''));
  assert.equal(decoded, mailto);
  const params = new URLSearchParams(decoded.split('?')[1]);
  assert.equal(params.get('subject'), draft.subject);
  assert.equal(params.get('body'), draft.body);
});
