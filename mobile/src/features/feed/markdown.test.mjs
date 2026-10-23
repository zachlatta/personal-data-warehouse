import test from 'node:test';
import assert from 'node:assert/strict';

import { parseInline, parseMarkdown, plainText, previewSpans } from './markdown.ts';

test('inline bold, italics, code and links', () => {
  assert.deepEqual(parseInline('a **b** _c_ `d` [e](https://x.test)'), [
    { text: 'a ' }, { text: 'b', bold: true }, { text: ' ' }, { text: 'c', italic: true }, { text: ' ' },
    { text: 'd', code: true }, { text: ' ' }, { text: 'e', url: 'https://x.test' },
  ]);
});

test('blocks: headings, bullets, quotes, code and wrapped paragraphs', () => {
  const blocks = parseMarkdown('# Title\n\nline one\nline two\n\n- a\n2. b\n> said\n```\nx = 1\n```');
  assert.deepEqual(blocks.map((b) => b.kind), ['heading', 'paragraph', 'bullet', 'bullet', 'quote', 'code']);
  assert.equal(blocks[1].spans[0].text, 'line one line two');
  assert.equal(blocks[3].ordered, '2');
  assert.equal(blocks[5].text, 'x = 1');
});

test('the card preview is the text without markup', () => {
  assert.equal(plainText('## Why\n\n**Bold** move, see [here](https://x.test).\n- one'), 'Why Bold move, see here. one');
});

test('a preview keeps its links and bold in one run of text', () => {
  assert.deepEqual(previewSpans('Read **the paper** at [arXiv](https://arxiv.org/abs/1).\n\n- one *point*'), [
    { text: 'Read ' },
    { text: 'the paper', bold: true },
    { text: ' at ' },
    { text: 'arXiv', url: 'https://arxiv.org/abs/1' },
    { text: '.' },
    { text: ' ' },
    { text: 'one ' },
    { text: 'point' },
  ]);
});
