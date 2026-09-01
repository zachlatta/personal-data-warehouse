import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

import {
  effectiveMobileSearchPriorities,
  MOBILE_DEFAULT_SEARCH_PRIORITIES,
  PRIORITIES,
} from './api.ts';

test('mobile intentionally defaults to the catalog attention scope while other surfaces stay all-tier', () => {
  const catalog = JSON.parse(
    readFileSync(new URL('../../../src/personal_data_warehouse/warehouse_catalog.json', import.meta.url)),
  );
  assert.deepEqual(MOBILE_DEFAULT_SEARCH_PRIORITIES, catalog.timeline_priorities.attention_priorities);
  assert.equal(catalog.timeline_priorities.default_scope, 'all');
});

test('selecting every real tier omits the one priorities filter', () => {
  assert.equal(effectiveMobileSearchPriorities(PRIORITIES), undefined);
  assert.deepEqual(
    effectiveMobileSearchPriorities(MOBILE_DEFAULT_SEARCH_PRIORITIES),
    ['self', 'direct', 'cc'],
  );
});
