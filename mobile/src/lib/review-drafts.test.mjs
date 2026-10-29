import test from 'node:test';
import assert from 'node:assert/strict';
import { ReviewDrafts } from './review-drafts.ts';

test('unsaved edits survive leaving and reopening a review in the app', () => {
  const store = new ReviewDrafts();
  store.write('message', 'original', 'my draft');
  assert.equal(store.read('message', 'original', 'original'), 'my draft');
});

test('changed server content invalidates the old local edit rather than overwriting it silently', () => {
  const store = new ReviewDrafts();
  store.write('message', 'original', 'my draft');
  assert.equal(store.read('message', 'new baseline', 'new baseline'), 'new baseline');
});

test('discard and sign out remove local edits', () => {
  const store = new ReviewDrafts();
  store.write('one', 'original', 'draft');
  store.remove('one');
  assert.equal(store.read('one', 'original', 'original'), 'original');
  store.write('two', 'original', 'draft');
  store.clear();
  assert.equal(store.read('two', 'original', 'original'), 'original');
});
