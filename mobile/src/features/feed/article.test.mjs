import test from 'node:test';
import assert from 'node:assert/strict';

import { articleDocument, articleUrl, imageKey, parseArticleMessage, readingMinutes } from './article.ts';

const colors = { text: '#000', secondary: '#666', background: '#fff', element: '#eee', rule: '#ddd', link: '#208AEF' };

test('an article card reads its page inline; X posts and videos do not', () => {
  assert.equal(articleUrl({ url: 'https://example.com/story' }, false), 'https://example.com/story');
  assert.equal(articleUrl({ url: 'https://x.com/a/status/1', embed: { type: 'x_post' } }, false), null);
  assert.equal(articleUrl({ url: 'https://www.youtube.com/watch?v=abc' }, true), null);
  assert.equal(articleUrl({}, false), null);
  assert.equal(articleUrl({ url: 'mailto:a@example.com' }, false), null);
});

test('a resized copy of a photo matches the original', () => {
  assert.equal(imageKey('https://cdn.example.com/2026/10/Rocket-Ring-1024x683.jpg?w=640'), 'rocket-ring');
  assert.equal(imageKey('https://cdn.example.com/2026/10/rocket-ring.JPG'), 'rocket-ring');
  assert.equal(imageKey('not a url'), '');
});

test('reading time rounds to whole minutes, at least one', () => {
  assert.equal(readingMinutes(40), 1);
  assert.equal(readingMinutes(2300), 10);
});

test('only height and http(s) or mailto link messages are accepted', () => {
  assert.deepEqual(parseArticleMessage('{"type":"height","height":812}'), { type: 'height', height: 812 });
  assert.deepEqual(parseArticleMessage('{"type":"link","url":"https://example.com/next"}'), { type: 'link', url: 'https://example.com/next' });
  assert.equal(parseArticleMessage('{"type":"link","url":"javascript:alert(1)"}'), null);
  assert.equal(parseArticleMessage('{"type":"height","height":0}'), null);
  assert.equal(parseArticleMessage('nope'), null);
});

test('the document carries the article, the theme and a nonce-only script policy', () => {
  const doc = articleDocument({ url: 'https://example.com/s', html: '<p>Body text</p>', words: 2, language: 'en' }, colors, [], 'abc123');
  assert.match(doc, /<p>Body text<\/p>/);
  assert.match(doc, /script-src 'nonce-abc123'/);
  assert.match(doc, /<script nonce="abc123">/);
  assert.match(doc, /color: #000/);
  assert.match(doc, /<html lang="en">/);
});

test('the lead image goes on top only when the card shows no photo and the article lacks it', () => {
  const article = { url: 'https://example.com/s', html: '<p>x</p>', words: 1, image_url: 'https://example.com/lead.jpg' };
  assert.match(articleDocument(article, colors, [], 'n'), /<img src="https:\/\/example.com\/lead.jpg"/);
  assert.doesNotMatch(articleDocument(article, colors, ['https://example.com/card.jpg'], 'n'), /lead.jpg/);
  const inBody = { ...article, html: '<img src="https://example.com/lead.jpg">' };
  assert.equal(articleDocument(inBody, colors, [], 'n').split('lead.jpg').length - 1, 1);
});

test('an image url cannot break out of its attribute', () => {
  const doc = articleDocument({ url: 'https://e.com', html: '', words: 0, image_url: 'https://e.com/a.jpg"><script>x()</script>' }, colors, [], 'n');
  assert.doesNotMatch(doc, /<script>x\(\)/);
});
