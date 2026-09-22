import test from "node:test";
import assert from "node:assert/strict";
import { RequestCache } from "../request_cache.js";

test("a remembered request is served by id and refreshes its list row without mutations", () => {
  const cache = new RequestCache();
  assert.equal(cache.peekList(), null);
  assert.equal(cache.peek("r1"), null);
  cache.rememberList([{ id: "r1", status: "pending_review", title: "old" }, { id: "r2", status: "rejected" }]);
  cache.remember({ id: "r1", status: "approved", title: "new", mutations: [{ id: "m1" }] });
  assert.equal(cache.peek("r1").mutations.length, 1);
  assert.deepEqual(cache.peekList()[0], { id: "r1", status: "approved", title: "new" });
  cache.remember({ id: "r3", status: "pending_review", mutations: [] });
  assert.equal(cache.peekList()[0].id, "r3", "a request the list never saw is prepended");
  assert.equal(cache.peekList().length, 3);
});

test("invalidate forgets everything and junk is ignored", () => {
  const cache = new RequestCache();
  cache.rememberList([{ id: "r1" }]);
  cache.remember(null); cache.remember({}); cache.remember("r1");
  assert.equal(cache.peekList().length, 1);
  cache.invalidate();
  assert.equal(cache.peekList(), null);
  assert.equal(cache.peek("r1"), null);
});
