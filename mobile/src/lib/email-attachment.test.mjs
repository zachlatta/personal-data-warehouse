import assert from 'node:assert/strict';
import test from 'node:test';
import { attachmentPreviewKind, attachmentFilename, prepareAttachment } from './email-attachment.ts';

test('only passive formats are previewed; PDF preview is iOS-only', () => {
  assert.equal(attachmentPreviewKind('application/pdf', 'ios'), 'pdf');
  assert.equal(attachmentPreviewKind('application/pdf', 'android'), 'external');
  assert.equal(attachmentPreviewKind('image/png', 'ios'), 'image');
  assert.equal(attachmentPreviewKind('text/plain; charset=utf-8', 'ios'), 'text');
  for (const type of ['text/html', 'image/svg+xml', 'application/zip']) {
    assert.equal(attachmentPreviewKind(type, 'ios'), 'external');
  }
});

test('filenames cannot escape the private temporary directory', () => {
  assert.equal(attachmentFilename('../../invoice.pdf'), 'invoice.pdf');
  assert.equal(attachmentFilename('C:\\files\\invoice.pdf'), 'invoice.pdf');
  assert.equal(attachmentFilename('..'), 'attachment');
  assert.equal(attachmentFilename(''), 'attachment');
});

test('preparation writes exact bytes and cleanup owns only this preview', () => {
  const calls = [];
  const storage = { write(name, data) { calls.push([name, data]); return 'file:///unique/invoice.pdf'; }, remove() { calls.push('removed'); } };
  const prepared = prepareAttachment({ filename: '../invoice.pdf', data_base64: 'AAH/', content_type: 'application/pdf' }, storage);
  assert.equal(prepared.uri, 'file:///unique/invoice.pdf');
  assert.deepEqual(calls, [['invoice.pdf', 'AAH/']]);
  prepared.dispose();
  assert.equal(calls.at(-1), 'removed');
});

test('failed writes clean up and propagate an actionable error', () => {
  let removed = false;
  assert.throws(() => prepareAttachment({ filename: 'a', data_base64: 'YQ==', content_type: 'text/plain' }, {
    write() { throw new Error('disk full'); }, remove() { removed = true; },
  }), /disk full/);
  assert.equal(removed, true);
});

test('same-named attachments from different variants retain their own bytes', () => {
  const bytes = new Map();
  const attachment = { filename: 'invoice.pdf', content_type: 'application/pdf' };
  const makeStorage = (id) => ({
    write(name, data) { bytes.set(id, data); return `file:///${id}/${name}`; },
    remove() { bytes.delete(id); },
  });
  const first = prepareAttachment({ ...attachment, data_base64: 'Zmlyc3Q=' }, makeStorage('first'));
  const second = prepareAttachment({ ...attachment, data_base64: 'c2Vjb25k' }, makeStorage('second'));
  assert.notEqual(first.uri, second.uri);
  first.dispose();
  assert.equal(bytes.get('second'), 'c2Vjb25k');
  second.dispose();
  assert.equal(bytes.size, 0);
});

test('native viewer updates are isolated from binaries missing its native modules', async () => {
  const { readFileSync } = await import('node:fs');
  const config = JSON.parse(readFileSync(new URL('../../app.json', import.meta.url)));
  assert.equal(config.expo.runtimeVersion.policy, 'fingerprint');
});
