import test, { beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { search, PRIORITIES } from "../api.js";

// Only the DOM operations used by the search view; requests never use the network.
class Node {
  constructor() {
    this.children = []; this.value = ""; this.style = {}; this.events = {};
    this.className = "";
    this.classList = {
      contains: (name) => this.className.split(" ").includes(name),
      add: (name) => { if (!this.classList.contains(name)) this.className += " " + name; },
      remove: (name) => { this.className = this.className.split(" ").filter((n) => n !== name).join(" "); },
      toggle: (name) => this.classList.contains(name) ? this.classList.remove(name) : this.classList.add(name),
    };
  }
  set textContent(value) { this.text = String(value); this.children = []; }
  get textContent() { return (this.text || "") + this.children.map((n) => n.textContent).join(""); }
  set innerHTML(html) {
    this.textContent = "";
    for (const match of html.matchAll(/<[^>]+id="([^"]+)"[^>]*>/g)) {
      const node = nodes[match[1]] = new Node();
      node.value = match[0].match(/value="([^"]*)"/)?.[1] || "";
    }
  }
  appendChild(node) {
    this.children.push(node);
    if (node.value && this === nodes.smode && !this.value) this.value = node.value;
    return node;
  }
  addEventListener(event, fn) { this.events[event] = fn; }
  fire(event) { return this.events[event]?.({ preventDefault() {} }); }
  focus() {}
}
let nodes = { dclose: new Node(), drawer: new Node() };
globalThis.document = {
  getElementById: (id) => nodes[id],
  createElement: () => new Node(),
  createTextNode: (text) => { const n = new Node(); n.textContent = text; return n; },
  addEventListener() {},
  querySelectorAll: () => [],
};
const { mount } = await import("../search.js");
const originalRun = search.run;
let calls, controls, stats, url, view;
beforeEach(() => {
  nodes = { dclose: new Node(), drawer: new Node() };
  calls = []; stats = ""; url = ""; view = null;
  globalThis.location = { search: "" };
  globalThis.history = { replaceState(_state, _title, value) { url = value; } };
  search.run = async (input) => { calls.push(input); return response("current"); };
});
afterEach(() => { view?.cleanup(); search.run = originalRun; });
function start(query = "") {
  location.search = query;
  view = mount(new Node(), {
    setControls(value) { controls = value; },
    setStats(value) { stats = value; },
  });
}
function response(label) {
  return {
    total_rows: 1, mode: "keyword", priority_scope: "selected", selected_priorities: ["direct"],
    returned_priority_counts: { direct: 1 }, hint: label + " hint",
    rows: [{ title: label, source: "gmail", priority: "direct", event_ts: "2026-09-01T12:00:00Z" }],
  };
}

test("URL round trip restores every search parameter and visible controls before auto-run", async () => {
  start();
  nodes.sq.value = "receipt & invoice";
  nodes.smode.value = "exact";
  nodes.ssrc.value = "gmail, apple_messages";
  nodes.ssince.value = "2026-08-31";
  nodes.sdepth.value = "73";
  await nodes.spri.children[0].fire("click");
  await nodes.spri.children[1].fire("click");
  await nodes.sform.fire("submit");
  const expected = calls[0];
  const params = new URL(url, "https://example.test").searchParams;
  for (const key of ["q", "mode", "priorities", "sources", "since", "max_results"]) assert.ok(params.has(key), key);
  view.cleanup();
  start("?" + params);
  assert.deepEqual(calls[1], expected);
  assert.equal(nodes.sq.value, expected.query);
  assert.equal(nodes.smode.value, expected.mode);
  assert.equal(nodes.ssrc.value, expected.sources.join(", "));
  assert.equal(nodes.ssince.value, expected.since);
  assert.equal(Number(nodes.sdepth.value), expected.max_results);
  assert.deepEqual(nodes.spri.children.map((n) => n.classList.contains("on")), [true, true, false, false, false]);
});

test("old q/mode links and absent priorities keep the all-tier default", () => {
  start("?q=invoice&mode=keyword");
  assert.deepEqual(calls, [{ query: "invoice", mode: "keyword", max_results: 30 }]);
  assert.equal(nodes.spri.children.length, 5);
  assert.ok(nodes.spri.children.every((n) => !n.classList.contains("on")));
  assert.equal(new URL(url, "https://example.test").searchParams.has("priorities"), false);
});

test("each of the five tiers survives a URL round trip", async () => {
  for (const tier of PRIORITIES) {
    start("?q=test&priorities=" + tier);
    assert.deepEqual(calls.at(-1).priorities, [tier]);
    const saved = url;
    view.cleanup();
    start(new URL(saved, "https://example.test").search);
    assert.deepEqual(calls.at(-1).priorities, [tier]);
    view.cleanup();
  }
});

for (const scope of [
  "priorities=bogus", "priorities=self,bogus", "priorities=", "priorities=self,,direct",
  "priorities=self&priorities=noise", "sources=", "sources=gmail,,slack", "sources=%3Cscript%3E",
  "since=not-a-date", "since=2026-02-30", "since=", "max_results=0", "max_results=201",
  "max_results=1.5", "max_results=NaN", "max_results=", "mode=bogus",
]) {
  test("malformed link is blocked without silently widening scope: " + scope, async () => {
    start("?q=test&" + scope);
    assert.equal(calls.length, 0);
    assert.match(nodes.shint.textContent, /invalid search link/i);
    assert.equal(url, "");
    await nodes.sform.fire("submit");
    assert.equal(calls.length, 0, "resubmitting must not discard the invalid scope");
    await controls.fire("click");
    nodes.sq.value = "fresh";
    await nodes.sform.fire("submit");
    assert.equal(calls.length, 1, "explicit clear allows a fresh search");
  });
}

test("source tokens remain server-owned rather than silently filtered by a client allow-list", () => {
  start("?q=test&sources=future_source&since=2024-02-29&max_results=200");
  assert.deepEqual(calls[0].sources, ["future_source"]);
  assert.equal(calls[0].since, "2024-02-29");
  assert.equal(calls[0].max_results, 200);
});

function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
function pendingSearches() {
  const pending = [];
  search.run = (input) => {
    calls.push(input);
    const request = deferred();
    pending.push(request);
    return request.promise;
  };
  return pending;
}
function submit(query) { nodes.sq.value = query; return nodes.sform.fire("submit"); }
function snapshot() { return { rows: nodes.sresults.textContent, hint: nodes.shint.textContent, stats }; }

test("reverse completions render only the newest response, including server echo metadata", async () => {
  start();
  const pending = pendingSearches();
  const older = submit("older");
  const newer = submit("newer");
  pending[1].resolve(response("newer"));
  await newer;
  assert.equal(nodes.sresults.children.length, 1);
  assert.match(stats, /scope direct/);
  assert.match(nodes.shint.textContent, /direct=1/);
  const current = snapshot();
  pending[0].resolve({ ...response("older"), fallback_reason: "old fallback", error: "old error" });
  await older;
  assert.deepEqual(snapshot(), current);
  assert.equal(nodes.sresults.children.length, 1);
});

test("an older error cannot overwrite a newer success", async () => {
  start();
  const pending = pendingSearches();
  const older = submit("older");
  const newer = submit("newer");
  pending[1].resolve(response("newer"));
  await newer;
  const current = snapshot();
  pending[0].reject(new Error("old failure"));
  await older;
  assert.deepEqual(snapshot(), current);
});

test("a newer error stays visible when an older success finishes", async () => {
  start();
  const pending = pendingSearches();
  const older = submit("older");
  const newer = submit("newer");
  pending[1].reject(new Error("current failure"));
  await newer;
  assert.match(nodes.shint.textContent, /current failure/);
  const current = snapshot();
  pending[0].resolve(response("older"));
  await older;
  assert.deepEqual(snapshot(), current);
});

for (const action of ["clear", "unmount"]) {
  for (const outcome of ["success", "error"]) {
    test(action + " invalidates a pending " + outcome + " across rows, hints and stats", async () => {
      start();
      const pending = pendingSearches();
      const running = submit("pending");
      if (action === "clear") {
        await controls.fire("click");
        assert.deepEqual(snapshot(), { rows: "", hint: "", stats: "" });
        assert.equal(nodes.sq.value, "");
        assert.equal(new URL(url, "https://example.test").searchParams.has("q"), false);
      } else {
        view.cleanup();
        stats = "other view stats";
      }
      const current = snapshot();
      if (outcome === "success") pending[0].resolve(response("stale"));
      else pending[0].reject(new Error("stale failure"));
      await running;
      assert.deepEqual(snapshot(), current);
    });
  }
}

test("starting a new search clears old stats and rows while it waits", async () => {
  start();
  await submit("first");
  assert.notEqual(stats, "");
  const pending = pendingSearches();
  const running = submit("next");
  assert.equal(stats, "");
  assert.equal(nodes.sresults.children.length, 0);
  pending[0].resolve(response("next"));
  await running;
});

test("clear then search again cannot revive the pre-clear response", async () => {
  start();
  const pending = pendingSearches();
  const older = submit("older");
  await controls.fire("click");
  const newer = submit("newer");
  pending[1].resolve(response("newer"));
  await newer;
  const current = snapshot();
  pending[0].resolve(response("older"));
  await older;
  assert.deepEqual(snapshot(), current);
});

test("invalid form submission supersedes a pending valid request", async () => {
  start();
  const pending = pendingSearches();
  const older = submit("older");
  nodes.sdepth.value = "0";
  await submit("invalid");
  assert.equal(pending.length, 1);
  assert.match(nodes.shint.textContent, /invalid search/);
  const current = snapshot();
  pending[0].resolve(response("older"));
  await older;
  assert.deepEqual(snapshot(), current);
});

test("an explicitly malformed source control cannot become an all-source request", async () => {
  start();
  nodes.ssrc.value = ", ,";
  await submit("test");
  assert.equal(calls.length, 0);
  assert.match(nodes.shint.textContent, /invalid search/);
});
