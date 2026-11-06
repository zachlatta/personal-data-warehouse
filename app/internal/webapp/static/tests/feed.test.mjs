import test, { beforeEach, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import { ActiveClock, EventQueue, IDLE_MS, ViewabilityTracker, applyPendingDismissals, applySave, mergeSaved, stopwatch, visibleFraction } from "../feed_log.js";
import { articleDocument, articleUrl, cardContext, cardLinks, cardPlayer, cardVideo, fileVideoDocument, parseArticleMessage, parseInline, parseMarkdown, picture, playerUrl, safeUrl, xPhotos } from "../feed_card.js";

// Only the DOM the feed view uses. Geometry is set by hand: `y` is a card's
// place in the list and the scroller's scrollTop moves it.
const VIEW_HEIGHT = 800;
class Node {
  constructor(tag) {
    this.tag = tag; this.children = []; this.attrs = {}; this.style = {}; this.events = {}; this.className = ""; this.text = "";
    this.y = 0; this.height = 0; this.scrollTop = 0;
  }
  set textContent(value) { this.text = String(value); this.children = []; }
  get textContent() { return this.text + this.children.map((n) => n.textContent).join(""); }
  appendChild(node) { node.parent = this; this.children.push(node); return node; }
  insertBefore(node, ref) {
    node.parent = this;
    const at = ref ? this.children.indexOf(ref) : -1;
    if (at < 0) this.children.push(node); else this.children.splice(at, 0, node);
    return node;
  }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter((n) => n !== this); }
  closest() { return null; }
  setAttribute(name, value) { this.attrs[name] = String(value); }
  getAttribute(name) { return this.attrs[name]; }
  focus(options) { this.focused = options; }
  addEventListener(event, fn) { (this.events[event] ||= []).push(fn); }
  removeEventListener(event, fn) { this.events[event] = (this.events[event] || []).filter((f) => f !== fn); }
  fire(event, detail = {}) { for (const fn of this.events[event] || []) fn({ preventDefault() {}, ...detail }); }
  getBoundingClientRect() {
    if (this.id === "fv") return { top: 46, height: VIEW_HEIGHT };
    return { top: 46 + this.y - scroller().scrollTop, height: this.height };
  }
  all(match, out = []) {
    for (const child of this.children) { if (match(child)) out.push(child); child.all(match, out); }
    return out;
  }
}
const scroller = () => root.children[0];
const cards = () => root.all((n) => n.tag === "article");
const anchors = (card) => card.all((n) => n.tag === "a");

const byClass = (node, cls) => node.all((n) => n.className.split(" ").includes(cls));
const button = (node, label) => node.all((n) => n.tag === "button").find((b) => b.textContent.startsWith(label));

let root, frames, posts, stats, storage, observers, documentEvents, windowEvents, opened, gateOpen, holdPosts;
function setVisible(state) {
  document.visibilityState = state;
  for (const fn of documentEvents.visibilitychange || []) fn({});
}
function runFrames() { const pending = frames; frames = []; pending.forEach((fn) => fn()); }
const settle = () => new Promise((resolve) => setImmediate(resolve));
// Ids are made on the device, so the server counts a batch sent twice once.
const events = () => [...new Map(posts.flatMap((p) => p.body.events).map((e) => [e.event_id, e])).values()];
const ofType = (type) => events().filter((e) => e.type === type);

const CARDS = [
  { item_id: "hn:1", source: "hn", title: "First", url: "https://a.test/1", body: "Body **bold** [site](https://b.test/) [bad](javascript:alert(1))", image_url: "https://i.test/1.jpg", site: "a.test", links: [{ url: "https://paper.test/", title: "Paper" }], engagement: {}, created_at: "2026-11-05T00:00:00Z" },
  { item_id: "hn:2", source: "hn", title: "Second", url: "https://a.test/2", engagement: {}, created_at: "2026-11-05T00:00:00Z" },
  { item_id: "hn:3", source: "picks", title: "Third", engagement: {}, resurfaced: true, created_at: "2026-11-05T00:00:00Z" },
];
let feedResponse, articleResponse, articleAsks;

beforeEach(() => {
  mock.timers.enable({ apis: ["setTimeout", "setInterval", "Date"], now: 1_000_000 });
  root = new Node("div"); frames = []; posts = []; stats = ""; storage = {}; observers = []; documentEvents = {}; windowEvents = {}; opened = []; gateOpen = false; holdPosts = false;
  feedResponse = () => ({ status: 200, body: { items: CARDS, saved: [], generated_at: "2026-11-05T00:00:00Z" } });
  articleAsks = [];
  articleResponse = () => ({ status: 200, body: { url: "https://a.test/1", title: "Read me", byline: "A Writer", html: "<p>Words</p>", words: 321 } });
  globalThis.document = {
    visibilityState: "visible",
    createElement: (tag) => new Node(tag),
    createTextNode: (text) => { const n = new Node("#text"); n.textContent = text; return n; },
    addEventListener: (event, fn) => { (documentEvents[event] ||= []).push(fn); },
    removeEventListener: (event, fn) => { documentEvents[event] = (documentEvents[event] || []).filter((f) => f !== fn); },
    dispatchEvent() {},
    getElementById: (id) => (id === "gate" ? { classList: { contains: () => gateOpen } } : null),
  };
  globalThis.window = {
    addEventListener: (event, fn) => { (windowEvents[event] ||= []).push(fn); },
    removeEventListener: (event, fn) => { windowEvents[event] = (windowEvents[event] || []).filter((f) => f !== fn); },
    matchMedia: () => ({ matches: true }),
    open: (url, target, features) => { opened.push({ url, target, features }); return null; },
  };
  globalThis.localStorage = {
    getItem: (key) => (key in storage ? storage[key] : null),
    setItem: (key, value) => { storage[key] = String(value); },
    removeItem: (key) => { delete storage[key]; },
  };
  localStorage.setItem("pdw_timeline_token", "secret");
  globalThis.requestAnimationFrame = (fn) => { frames.push(fn); return frames.length; };
  globalThis.cancelAnimationFrame = () => { frames = []; };
  globalThis.ResizeObserver = class { constructor(fn) { this.fn = fn; observers.push(this); } observe() {} disconnect() { this.gone = true; } };
  globalThis.fetch = async (path, init = {}) => {
    if (path.startsWith("/api/feed/events")) {
      if (holdPosts) return new Promise(() => {});
      posts.push({ path, headers: init.headers, keepalive: init.keepalive, body: JSON.parse(init.body) });
      return { ok: true, status: 200, text: async () => "{}" };
    }
    if (path.startsWith("/api/feed/article")) {
      articleAsks.push(path);
      const read = await articleResponse();
      return { ok: read.status < 400, status: read.status, text: async () => JSON.stringify(read.body) };
    }
    const answer = await feedResponse();
    return { ok: answer.status < 400, status: answer.status, text: async () => JSON.stringify(answer.body) };
  };
});
afterEach(() => { view?.cleanup(); view = null; mock.timers.reset(); });

let view = null, mountCount = 0;
// A fresh module each time: the first sitting since the page started is a launch.
async function start({ visible = true, wait = true } = {}) {
  document.visibilityState = visible ? "visible" : "hidden";
  const { mount } = await import("../feed.js?fresh=" + ++mountCount);
  view = mount(root, { setStats(value) { stats = value; }, setControls() {}, setSubtitle() {} });
  if (wait) await settle();
  layOut();
}
test("the column takes the keyboard on arrival, so PageDown scrolls before any click", async () => {
  await start();
  assert.deepEqual(scroller().focused, { preventScroll: true });
});

// Three 400 px cards, one under the other, in an 800 px window.
function layOut() { cards().forEach((card, i) => { card.y = i * 400; card.height = 400; }); }
function advance(ms) { mock.timers.tick(ms); }
// The 15 s timer may have a send in flight; let it land before sending the rest.
async function flushed() { await settle(); await view.flushEvents(); await settle(); }
function scrollTo(y) { scroller().scrollTop = y; scroller().fire("scroll"); runFrames(); }

test("a card half on screen for over 300 ms posts one impression under the laptop's client name", async () => {
  await start();
  runFrames();
  advance(400);
  scrollTo(5000);
  await flushed();
  const impressions = ofType("impression");
  assert.deepEqual(impressions.map((e) => e.item_id), ["hn:1", "hn:2"]);
  const first = impressions[0];
  assert.equal(first.dwell_ms, 400);
  assert.equal(first.position, 0);
  assert.equal(first.visible_fraction, 1);
  assert.equal(first.metadata.view, "all");
  assert.equal(first.metadata.height, 400);
  assert.equal(first.occurred_at, new Date(1_000_000).toISOString());
  const post = posts.find((p) => p.body.events.some((e) => e.type === "impression"));
  assert.equal(post.body.device, "crobat");
  assert.equal(post.headers.get("Authorization"), "Bearer crobat-web:secret");
  assert.equal(post.headers.get("X-PDW-Client"), "crobat-web");
});

test("a card under half on screen posts nothing", async () => {
  await start();
  scroller().scrollTop = 0;
  cards().forEach((card, i) => { card.y = 700 + i * 400; });
  runFrames(); // hn:1 shows 100 of 400 px
  advance(2000);
  scrollTo(5000);
  await flushed();
  assert.deepEqual(ofType("impression"), []);
});

test("hidden-tab time does not count, and each visible stretch is its own impression", async () => {
  await start();
  runFrames();
  advance(1000);
  setVisible("hidden");
  advance(9 * 60_000); // under the ten minutes that reload the list
  setVisible("visible");
  runFrames();
  advance(500);
  scrollTo(5000);
  await flushed();
  const first = ofType("impression").filter((e) => e.item_id === "hn:1");
  assert.deepEqual(first.map((e) => e.dwell_ms), [1000, 500]);
  const ends = ofType("session_end");
  assert.equal(ends.length, 1);
  assert.equal(ends[0].metadata.reason, "background");
  assert.equal(ends[0].dwell_ms, 1000);
  assert.ok(ends[0].metadata.wall_ms >= 1000);
});

test("a tab loaded in the background fetches nothing and posts nothing until it is looked at", async () => {
  await start({ visible: false });
  runFrames();
  advance(60_000);
  await flushed();
  assert.deepEqual(events(), []);
  assert.equal(cards().length, 0);
  setVisible("visible");
  await settle();
  layOut();
  runFrames();
  advance(400);
  scrollTo(5000);
  await flushed();
  assert.equal(ofType("session_start").length, 1);
  assert.equal(ofType("impression").length, 2);
});

test("a list that arrives after the tab hid is not measured until the tab is visible", async () => {
  let answer;
  feedResponse = () => new Promise((resolve) => { answer = resolve; });
  await start({ wait: false });
  setVisible("hidden");
  answer({ status: 200, body: { items: CARDS, saved: [] } });
  await settle();
  layOut();
  runFrames();
  observers.forEach((o) => o.fn());
  scroller().fire("scroll");
  runFrames();
  advance(60_000);
  await flushed();
  assert.deepEqual(ofType("impression"), []);
  assert.equal(cards().length, 3);
  setVisible("visible");
  runFrames();
  advance(400);
  scrollTo(5000);
  await flushed();
  assert.deepEqual(ofType("impression").map((e) => [e.item_id, e.dwell_ms]), [["hn:1", 400], ["hn:2", 400]]);
});

test("leaving the page inside the app ends the sitting, keeps open stretches and stops listening", async () => {
  await start();
  runFrames();
  advance(2000);
  view.cleanup();
  const ended = view; view = null;
  await settle();
  const all = events();
  assert.deepEqual(all.filter((e) => e.type === "impression").map((e) => e.dwell_ms), [2000, 2000]);
  const end = all.find((e) => e.type === "session_end");
  assert.equal(end.metadata.reason, "blur");
  assert.equal(end.metadata.cards_seen, 2);
  assert.equal(end.metadata.deepest_position, 1);
  const start_ = all.find((e) => e.type === "session_start");
  assert.equal(end.session_id, start_.session_id);
  assert.ok(all.filter((e) => e.type === "impression").every((e) => e.session_id === start_.session_id));
  assert.deepEqual(documentEvents.visibilitychange, []);
  assert.deepEqual(windowEvents.pagehide, []);
  assert.ok(observers.every((o) => o.gone));
  const before = posts.length;
  advance(60_000);
  await settle();
  assert.equal(posts.length, before);
  ended.cleanup?.call(null);
  assert.equal(ofType("session_end").length, 1, "cleanup runs once");
});

test("a list that answers after leaving draws nothing and starts nothing", async () => {
  let answer;
  feedResponse = () => new Promise((resolve) => { answer = resolve; });
  await start({ wait: false });
  view.cleanup(); view = null;
  answer({ status: 200, body: { items: CARDS, saved: [] } });
  await settle();
  assert.equal(cards().length, 0);
});

test("closing the tab sends what is queued with fetch keepalive, and a late hide does not end it twice", async () => {
  await start();
  runFrames();
  advance(400);
  for (const fn of windowEvents.pagehide) fn({});
  setVisible("hidden");
  const last = posts.at(-1);
  assert.equal(last.keepalive, true);
  assert.ok(last.body.events.some((e) => e.type === "session_end"));
  assert.ok(last.body.events.some((e) => e.type === "impression"));
  assert.equal(events().filter((e) => e.type === "session_end").length, 1);
});

test("with no token the gate is open: the page starts nothing, and unlocking mounts it again", async () => {
  localStorage.removeItem("pdw_timeline_token");
  await start();
  await settle();
  assert.deepEqual(events(), []);
  assert.equal(root.children.length, 0);
  assert.equal(view.cleanup, undefined);
  view = null;
});

test("a sitting says what it was, and a load names what was served in order", async () => {
  await start();
  await flushed();
  const [begin] = ofType("session_start");
  assert.equal(begin.metadata.trigger, "launch");
  assert.equal(begin.metadata.update_id, "web");
  assert.equal(typeof begin.metadata.tz, "string");
  const [load] = ofType("load");
  assert.equal(load.session_id, begin.session_id);
  assert.equal(load.metadata.trigger, "first");
  assert.equal(load.metadata.ok, true);
  assert.deepEqual(load.metadata.item_ids, ["hn:1", "hn:2", "hn:3"]);
  assert.equal(stats, "3 cards");
});

test("a load that fails says so on the page and in the log", async () => {
  feedResponse = () => ({ status: 500, body: { error: "boom" } });
  await start();
  await flushed();
  const [load] = ofType("load");
  assert.equal(load.metadata.ok, false);
  assert.equal(load.metadata.status, 500);
  assert.match(root.textContent, /boom/);
});

test("the feed's order is the server's order, with title, body, picture, links and a seen-before mark", async () => {
  await start();
  assert.deepEqual(cards().map((c) => c.getAttribute("data-id")), ["hn:1", "hn:2", "hn:3"]);
  const first = cards()[0];
  assert.match(first.textContent, /First/);
  assert.match(first.textContent, /Body bold site bad/);
  assert.match(first.textContent, /Paper/);
  assert.match(cards()[2].textContent, /Seen before/);
  const image = first.all((n) => n.tag === "img")[0];
  assert.equal(image.src, "https://i.test/1.jpg");
  assert.equal(image.getAttribute("referrerpolicy"), "no-referrer");
  const box = first.all((n) => n.className === "fpic")[0];
  assert.equal(box.style.aspectRatio, "1.91");
});

test("every link opens a new tab and records the open, and only http(s) is ever linked", async () => {
  await start();
  const first = cards()[0];
  const links = anchors(first);
  assert.deepEqual(links.map((a) => a.href), ["https://a.test/1", "https://b.test/", "https://a.test/1", "https://paper.test/"]);
  for (const a of links) {
    assert.equal(a.target, "_blank");
    assert.equal(a.rel, "noopener noreferrer");
  }
  links[0].fire("click", { button: 0 });
  links[1].fire("click", { button: 0 });
  links[2].fire("auxclick", { button: 1 });
  links[3].fire("click", { button: 0 });
  links[3].fire("auxclick", { button: 2 }); // a right click opens nothing
  await flushed();
  assert.deepEqual(ofType("open").map((e) => [e.item_id, e.position, e.metadata.mode, e.metadata.via, e.metadata.url, e.metadata.link_index]), [
    ["hn:1", 0, "browser", "card", "https://a.test/1", 0],
    ["hn:1", 0, "browser", "body_link", "https://b.test/", -1],
    ["hn:1", 0, "browser", "card", "https://a.test/1", 0],
    ["hn:1", 0, "browser", "link", "https://paper.test/", 1],
  ]);
});

test("one scroll is one event, from the first move to rest", async () => {
  await start();
  runFrames();
  scrollTo(100);
  advance(50);
  scrollTo(500);
  advance(500);
  await flushed();
  const [scroll, ...rest] = ofType("scroll");
  assert.deepEqual(rest, []);
  assert.equal(scroll.dwell_ms, 50);
  assert.equal(scroll.metadata.to_y, 500);
  assert.equal(scroll.metadata.direction, "down");
  assert.equal(scroll.metadata.distance, 500);
  assert.equal(scroll.metadata.to_card, 1);
  assert.equal(scroll.metadata.view, "all");
});

test("the clock stops a minute after the last touch, and a touch restarts it", async () => {
  await start();
  runFrames();
  advance(5 * 60_000);
  for (const fn of windowEvents.keydown) fn({});
  advance(1000);
  scrollTo(5000);
  await flushed();
  assert.equal(ofType("impression")[0].dwell_ms, IDLE_MS + 1000);
});

// The phone's numbers (mobile/src/features/feed/viewability.test.mjs, active-time.test.mjs).
const screen = (top) => ({ top, height: 800 });
const place = (...rows) => new Map(rows.map(([id, y, height], position) => [id, { y, height, position }]));

test("visible fraction is of the card, or of the screen for a card taller than it", () => {
  assert.equal(visibleFraction({ y: 0, height: 400 }, screen(0)), 1);
  assert.equal(visibleFraction({ y: 600, height: 400 }, screen(0)), 0.5);
  assert.equal(visibleFraction({ y: 900, height: 400 }, screen(0)), 0);
  assert.equal(visibleFraction({ y: 0, height: 2000 }, screen(1600)), 0.5);
});

test("a card read and scrolled away is one impression with its dwell and peak visibility", () => {
  const t = new ViewabilityTracker();
  const layouts = place(["a", 0, 400], ["b", 400, 400]);
  t.update(0, screen(0), layouts);
  t.update(3000, screen(200), layouts);
  const a = t.update(4000, screen(900), layouts).find((i) => i.itemId === "a");
  assert.deepEqual(a, { itemId: "a", position: 0, startedAt: 0, endedAt: 4000, dwellMs: 4000, visibleMs: 4000, maxFraction: 1 });
});

test("a card that only peeked never counts, and time below the threshold is visible but not dwell", () => {
  const t = new ViewabilityTracker();
  const layouts = place(["a", 800, 400]);
  t.update(0, screen(100), layouts); // 100 of 400 = 0.25
  assert.deepEqual(t.update(500, screen(0), layouts), []);
  const u = new ViewabilityTracker();
  const one = place(["a", 0, 400]);
  u.update(0, screen(0), one);
  u.update(1000, screen(300), one);
  const [a] = u.flush(5000);
  assert.equal(a.dwellMs, 1000);
  assert.equal(a.visibleMs, 5000);
  assert.deepEqual(u.flush(6000), []);
});

test("dwell is active time, and the clock counts only up to a minute after a touch and never in the background", () => {
  const t = new ViewabilityTracker();
  t.update(0, screen(0), place(["a", 0, 400]), 0);
  assert.equal(t.flush(600_000, 70_000)[0].dwellMs, 70_000);
  const c = new ActiveClock(0);
  c.touch(10_000);
  assert.equal(c.read(10_000 + 5 * 60_000), 10_000 + IDLE_MS);
  const d = new ActiveClock(0);
  d.touch(2_000);
  d.setForeground(false, 4_000);
  assert.equal(d.read(14 * 60_000), 4_000);
  d.setForeground(true, 14 * 60_000);
  assert.equal(d.read(14 * 60_000 + 10_000), 14_000);
});

const memory = () => { const box = { value: null }; return { box, storage: { load: () => box.value, save: (v) => { box.value = v; } } }; };
const ev = (n, type = "impression") => ({ event_id: "e" + n, type, session_id: "s", occurred_at: "2026-11-05T00:00:00Z" });

test("the outbox clears only what the server acknowledged, and backs off on a network failure", async () => {
  const { storage: disk } = memory();
  let fail = true, calls = 0, now = 0;
  const q = new EventQueue(disk, async () => { calls++; if (fail) throw new Error("offline"); }, () => now);
  q.enqueue(ev(1));
  await q.flush();
  assert.equal(q.size, 1);
  await q.flush();
  assert.equal(calls, 1);
  fail = false;
  now = 6000;
  await q.flush();
  assert.equal(q.size, 0);
});

test("the outbox drops a batch the server refuses as malformed", async () => {
  const q = new EventQueue(memory().storage, async () => { throw Object.assign(new Error("bad"), { status: 400 }); });
  q.enqueue(ev(1));
  await q.flush();
  assert.equal(q.size, 0);
});

test("two tabs share one outbox: an event from either survives the other's write", async () => {
  const { storage: disk } = memory();
  const a = new EventQueue(disk, async () => {});
  const b = new EventQueue(disk, async () => {});
  a.enqueue(ev(1));
  b.enqueue(ev(2));
  a.enqueue(ev(3));
  assert.deepEqual(b.pending().map((e) => e.event_id), ["e1", "e2", "e3"]);
});

test("a keepalive flush fits the browser's 64 KB limit, ignores a send in flight, and leaves the rest queued", async () => {
  const { storage: disk } = memory();
  const big = (n) => ({ ...ev(n), metadata: { pad: "x".repeat(10_000) } });
  const sent = [];
  let hang = true;
  const q = new EventQueue(disk, (batch, options) => {
    sent.push({ size: JSON.stringify(batch).length, count: batch.length, keepalive: options?.keepalive });
    return hang ? new Promise(() => {}) : Promise.resolve();
  });
  for (let i = 0; i < 10; i++) q.enqueue(big(i));
  void q.flush();
  hang = false;
  await q.flush(true, true);
  const last = sent.at(-1);
  assert.equal(last.keepalive, true);
  assert.ok(last.size < 64_000, String(last.size));
  assert.ok(last.count < 10);
  assert.equal(q.size, 10 - last.count);
});

test("markdown renders as spans, and only an http(s) url is a link", () => {
  assert.deepEqual(parseInline("a **b** [c](https://c.test/)"), [{ text: "a " }, { text: "b", bold: true }, { text: " " }, { text: "c", url: "https://c.test/" }]);
  assert.deepEqual(parseMarkdown("# H\n\ntext\nmore\n- one\n> said\n```\ncode\n```").map((b) => b.kind), ["heading", "paragraph", "bullet", "quote", "code"]);
  assert.equal(safeUrl("https://a.test/x"), "https://a.test/x");
  for (const bad of ["javascript:alert(1)", "data:text/html,x", "/relative", "", undefined, "http//nope"]) assert.equal(safeUrl(bad), "");
});

test("links run main first, skip anything unsafe, and name their host", () => {
  const links = cardLinks({ url: "https://www.a.test/", links: [{ url: "javascript:x" }, { url: "https://b.test/", title: "B" }, { url: "https://www.c.test/" }] });
  assert.deepEqual(links.map((l) => [l.url, l.index, l.label]), [["https://www.a.test/", 0, "a.test"], ["https://b.test/", 2, "B"], ["https://www.c.test/", 3, "c.test"]]);
});

test("a picture's box is known before it loads: 1.91 when unknown, clamped when known", () => {
  assert.deepEqual(picture({ image_url: "https://i.test/a.jpg" }), { url: "https://i.test/a.jpg", aspect: 1.91, alt: "" });
  assert.equal(picture({ media: [{ type: "image", url: "https://i.test/a.jpg", width: 100, height: 1000 }] }).aspect, 0.8);
  assert.equal(picture({ media: [{ type: "image", url: "https://i.test/a.jpg", width: 4000, height: 1000 }] }).aspect, 1.91);
  assert.equal(picture({ media: [{ type: "video", url: "https://v.test/a.mp4", thumbnail_url: "https://i.test/p.jpg" }] }).url, "https://i.test/p.jpg");
  assert.equal(picture({ url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ" }).url, "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg");
  assert.equal(picture({ embed: { type: "x_post", media: [{ type: "image", url: "https://pbs.test/x.jpg" }] } }).url, "https://pbs.test/x.jpg");
  assert.equal(picture({ url: "https://a.test/" }), null);
  assert.equal(picture({ image_url: "javascript:alert(1)" }), null);
});

test("a Hacker News line becomes counts and a Muse card loses its curator's labels", () => {
  const hn = cardContext({ source: "hn", body: "Why it matters\n\n80 points · 14 comments on Hacker News · [discussion](https://news.ycombinator.com/item?id=1)" });
  assert.deepEqual(hn.hn, { points: 80, comments: 14, url: "https://news.ycombinator.com/item?id=1" });
  assert.equal(hn.preview, "Why it matters");
  const muse = cardContext({ source: "muse-wonder", author: "Muse Wonder", body: "*Essay*\n**For you:** read this" });
  assert.equal(muse.preview, "read this");
  assert.equal(muse.author, "");
});

// --- like, save, hide -----------------------------------------------------------

// Press a key the way the page gets it: the document's listener, then the window's touch.
function press(key, extra = {}) {
  let prevented = false;
  const ev = { key, target: scroller(), preventDefault() { prevented = true; }, ...extra };
  for (const fn of documentEvents.keydown || []) fn(ev);
  for (const fn of windowEvents.keydown || []) fn(ev);
  return prevented;
}

test("like and save send the phone's fields from a button or a key, and buttons show the state", async () => {
  await start();
  const first = cards()[0];
  assert.equal(button(first, "Like").title, "Like (l)");
  button(first, "Like").fire("click");
  assert.ok(button(first, "Liked"));
  press("l");
  assert.ok(button(first, "Like"));
  button(first, "Save").fire("click");
  assert.equal(button(first, "Saved").getAttribute("aria-pressed"), "true");
  press("s");
  await flushed();
  assert.deepEqual(events().filter((e) => ["like", "unlike", "save", "unsave"].includes(e.type)).map((e) => [e.type, e.item_id, e.position, e.metadata.via, e.metadata.source]), [
    ["like", "hn:1", 0, "button", "hn"],
    ["unlike", "hn:1", 0, "key", "hn"],
    ["save", "hn:1", 0, "button", "hn"],
    ["unsave", "hn:1", 0, "key", "hn"],
  ]);
});

test("the served page says what is already liked and saved", async () => {
  feedResponse = () => ({ status: 200, body: { items: [{ ...CARDS[0], engagement: { liked: true } }, CARDS[1]], saved: [CARDS[1]] } });
  await start();
  assert.ok(button(cards()[0], "Liked"));
  assert.ok(button(cards()[1], "Saved"));
});

test("hide sends the phone's fields, undo puts the card back in its place, and positions never renumber", async () => {
  await start();
  press("j"); // to hn:2
  runFrames();
  button(cards()[1], "Hide").fire("click");
  assert.deepEqual(cards().map((c) => c.getAttribute("data-id")), ["hn:1", "hn:3"]);
  assert.match(root.textContent, /Hidden/);
  scrollTo(0);
  press("z");
  assert.deepEqual(cards().map((c) => c.getAttribute("data-id")), ["hn:1", "hn:2", "hn:3"]);
  assert.equal(scroller().scrollTop, 400, "the restored card is where it was");
  await flushed();
  const [hide] = ofType("dismiss");
  assert.deepEqual([hide.item_id, hide.position, hide.metadata.reason, hide.metadata.source, hide.metadata.site], ["hn:2", 1, "not_interested", "hn", ""]);
  const [ui] = ofType("ui");
  assert.deepEqual([ui.metadata.action, ui.metadata.what], ["undo", "Hidden"]);
  const [back] = ofType("undismiss");
  assert.deepEqual([back.item_id, back.position], ["hn:2", 1]);
  assert.ok(events().indexOf(ui) < events().indexOf(back));

  layOut();
  runFrames();
  press("x"); // the current card, hn:2 again
  layOut();
  runFrames();
  scrollTo(5000);
  await flushed();
  const third = ofType("impression").filter((e) => e.item_id === "hn:3");
  assert.ok(third.every((e) => e.position === 2), "hn:3 keeps its served position after a card above it is hidden");
  const hidden = ofType("impression").filter((e) => e.item_id === "hn:2").at(-1);
  assert.equal(hidden.metadata.height, 400, "a hidden card's stretch still names its height");
});

test("undo lasts five seconds, and z with nothing to undo does nothing", async () => {
  await start();
  press("z");
  press("x");
  advance(5001);
  press("z");
  assert.equal(cards().length, 2);
  await flushed();
  assert.deepEqual(ofType("undismiss"), []);
  assert.deepEqual(ofType("ui"), []);
});

test("save offers undo, and undo is a save toggle with via undo", async () => {
  await start();
  press("s");
  assert.match(root.textContent, /Saved for later/);
  button(root, "Undo").fire("click");
  await flushed();
  assert.deepEqual(events().filter((e) => e.type === "save" || e.type === "unsave").map((e) => [e.type, e.metadata.via]), [["save", "key"], ["unsave", "undo"]]);
  assert.equal(ofType("ui")[0].metadata.what, "Saved for later");
});

test("hiding the last card says so, and undo brings it back", async () => {
  feedResponse = () => ({ status: 200, body: { items: [CARDS[0]], saved: [] } });
  await start();
  press("x");
  assert.match(root.textContent, /Nothing here yet/);
  press("z");
  assert.equal(cards().length, 1);
  assert.doesNotMatch(root.textContent, /Nothing here yet/);
});

test("a stale answer cannot bring back a card whose dismiss was sent before the answer came", async () => {
  await start();
  let answer;
  feedResponse = () => new Promise((resolve) => { answer = resolve; });
  press("x");
  press("r");
  await settle();
  assert.deepEqual(events().filter((e) => e.type === "dismiss").map((e) => e.item_id), ["hn:1"], "the dismiss has left the outbox");
  answer({ status: 200, body: { items: CARDS, saved: [] } });
  await settle();
  assert.deepEqual(cards().map((c) => c.getAttribute("data-id")), ["hn:2", "hn:3"]);
});

test("a stale answer cannot bring back a card hidden during the load", async () => {
  await start();
  let answer;
  feedResponse = () => new Promise((resolve) => { answer = resolve; });
  press("r");
  await settle();
  holdPosts = true;
  press("x");
  answer({ status: 200, body: { items: CARDS, saved: [] } });
  await settle();
  assert.deepEqual(cards().map((c) => c.getAttribute("data-id")), ["hn:2", "hn:3"]);
});

test("a tap stays until the server agrees, so a reload cannot undo it", async () => {
  let liked = false;
  feedResponse = () => ({ status: 200, body: { items: [{ ...CARDS[0], engagement: { liked } }], saved: liked ? [CARDS[0]] : [] } });
  await start();
  holdPosts = true;
  press("l");
  press("s");
  press("r");
  await settle();
  assert.ok(button(cards()[0], "Liked"), "the server has not heard yet");
  assert.ok(button(cards()[0], "Saved"));
  liked = true;
  holdPosts = false;
  await settle();
  press("r");
  await settle();
  assert.ok(button(cards()[0], "Liked"));
  assert.ok(button(cards()[0], "Saved"));
});

test("a like and a save still unsent when the page reloads stay on, and pressing again takes them back", async () => {
  await start();
  holdPosts = true;
  press("l");
  press("s");
  view.cleanup();
  await start(); // a new page: only the outbox remembers, and the server has heard neither
  assert.ok(button(cards()[0], "Liked"));
  assert.ok(button(cards()[0], "Saved"));
  press("l");
  press("s");
  view.cleanup();
  holdPosts = false; // a held send stays in flight for its page, so the next page sends the outbox
  await start();
  await flushed();
  assert.deepEqual(events().filter((e) => e.item_id === "hn:1" && /like|save/.test(e.type)).map((e) => e.type), ["like", "save", "unlike", "unsave"]);
  assert.equal(button(cards()[0], "Liked"), undefined, "the unlike is the last word");
  assert.equal(button(cards()[0], "Saved"), undefined);
});

test("the outbox and read-later rules are the phone's", () => {
  const items = [{ item_id: "a" }, { item_id: "b" }];
  assert.deepEqual(applyPendingDismissals(items, [{ type: "dismiss", item_id: "a" }]), [items[1]]);
  assert.deepEqual(applyPendingDismissals(items, [{ type: "dismiss", item_id: "a" }, { type: "undismiss", item_id: "a" }]), items);
  assert.deepEqual(applySave(["a", "b"], "b", true), ["b", "a"]);
  assert.deepEqual(applySave(["a", "b"], "b", false), ["a"]);
  const pending = new Map([["x", { save: true, at: 1 }], ["y", { save: false, at: 2 }]]);
  assert.deepEqual(mergeSaved(["y"], pending), ["x"]);
  assert.equal(pending.size, 2);
  assert.deepEqual(mergeSaved(["x"], pending), ["x"]);
  assert.deepEqual([...pending.keys()], [], "both agree now");
});

// --- keys -------------------------------------------------------------------------

test("j and k jump a card at a time with no animation, and k goes to the top of a part-read card first", async () => {
  await start();
  assert.equal(press("j"), true);
  assert.equal(scroller().scrollTop, 400);
  press("j");
  assert.equal(scroller().scrollTop, 800);
  press("j"); // the last card stays
  assert.equal(scroller().scrollTop, 800);
  scroller().scrollTop = 900;
  press("k");
  assert.equal(scroller().scrollTop, 800);
  press("k");
  assert.equal(scroller().scrollTop, 400);
});

test("the current card has an outline class and nothing else moves", async () => {
  await start();
  runFrames();
  assert.ok(cards()[0].className.includes("fcur"));
  scrollTo(400);
  assert.ok(cards()[1].className.includes("fcur"));
  assert.ok(!cards()[0].className.includes("fcur"));
});

test("holding a key does nothing more: no card marked seen per repeat, no card hidden per repeat, no tab per repeat", async () => {
  await start();
  press("j");
  const top = scroller().scrollTop;
  for (let i = 0; i < 5; i++) assert.equal(press("j", { repeat: true }), false);
  assert.equal(scroller().scrollTop, top);
  press("x");
  for (let i = 0; i < 5; i++) press("x", { repeat: true });
  assert.equal(cards().length, 2);
  press("v", { repeat: true });
  press("1", { repeat: true });
  assert.deepEqual(opened, []);
});

test("keys stay out of the way of modifiers, inputs, the token gate and a native Enter", async () => {
  await start();
  for (const mod of ["ctrlKey", "metaKey", "altKey"]) assert.equal(press("j", { [mod]: true }), false);
  const field = { closest: (selector) => (selector.includes("input") ? {} : null) };
  assert.equal(press("j", { target: field }), false);
  gateOpen = true;
  assert.equal(press("j"), false);
  gateOpen = false;
  const link = { closest: (selector) => (selector === "a, button" ? {} : null) };
  assert.equal(press("Enter", { target: link }), false);
  assert.equal(root.all((n) => n.className === "freader").length, 0);
  assert.equal(press("PageDown"), false);
  assert.equal(press(" "), false);
  assert.equal(press("ArrowDown"), false);
  assert.equal(scroller().scrollTop, 0);
});

test("v opens the main link, a number opens that extra link, and both record the open", async () => {
  await start();
  assert.match(byClass(cards()[0], "flinks")[0].textContent, /1.*Paper/);
  press("v");
  press("1");
  press("2"); // no second link
  assert.deepEqual(opened.map((o) => [o.url, o.target, o.features]), [
    ["https://a.test/1", "_blank", "noopener,noreferrer"],
    ["https://paper.test/", "_blank", "noopener,noreferrer"],
  ]);
  await flushed();
  assert.deepEqual(ofType("open").map((e) => [e.item_id, e.metadata.mode, e.metadata.via, e.metadata.url, e.metadata.link_index]), [
    ["hn:1", "browser", "card", "https://a.test/1", 0],
    ["hn:1", "browser", "link", "https://paper.test/", 1],
  ]);
});

test("cleanup removes the key listener", async () => {
  await start();
  view.cleanup(); view = null;
  assert.deepEqual(documentEvents.keydown, []);
  assert.deepEqual(windowEvents.message, []);
});

// --- the reader -------------------------------------------------------------------

const readerNode = () => byClass(root, "freader")[0];
const readerFrame = () => readerNode().all((n) => n.tag === "iframe")[0];
const nonceOf = (frame) => /script nonce="([0-9a-f]+)"/.exec(frame.getAttribute("srcdoc"))[1];
function fromFrame(frame, data, source = frame.contentWindow) {
  for (const fn of windowEvents.message) fn({ source, data });
}
async function openFrame() {
  press("o");
  await settle();
  const frame = readerFrame();
  frame.contentWindow = {};
  return frame;
}

test("o opens the article in a sealed frame, and logs open and ui article the way the phone does", async () => {
  await start();
  const frame = await openFrame();
  assert.equal(articleAsks.length, 1);
  assert.match(articleAsks[0], /\/api\/feed\/article\?url=https%3A%2F%2Fa\.test%2F1/);
  assert.equal(frame.getAttribute("sandbox"), "allow-scripts");
  assert.equal(frame.getAttribute("referrerpolicy"), "no-referrer");
  const html = frame.getAttribute("srcdoc");
  const nonce = nonceOf(frame);
  assert.match(nonce, /^[0-9a-f]{32}$/);
  assert.ok(html.includes(`script-src 'nonce-${nonce}'`));
  assert.ok(html.includes("default-src 'none'"));
  assert.ok(html.includes("base-uri 'none'"));
  assert.ok(html.includes("form-action 'none'"));
  assert.ok(html.includes("<p>Words</p>"));
  assert.match(readerNode().textContent, /Read me/);
  assert.match(readerNode().textContent, /A Writer/);
  await flushed();
  const [open] = ofType("open");
  assert.deepEqual([open.item_id, open.position, open.metadata.mode], ["hn:1", 0, "detail"]);
  const [ui] = ofType("ui");
  assert.deepEqual([ui.item_id, ui.metadata.action, ui.metadata.ok, ui.metadata.words], ["hn:1", "article", true, 321]);
  assert.equal(typeof ui.dwell_ms, "number");
});

test("each open gets its own nonce", async () => {
  await start();
  const a = nonceOf(await openFrame());
  press("Escape");
  const b = nonceOf(await openFrame());
  assert.notEqual(a, b);
});

test("a message from another window, with the wrong nonce or the wrong shape does nothing", async () => {
  await start();
  const frame = await openFrame();
  const nonce = nonceOf(frame);
  const height = frame.style.height;
  fromFrame(frame, { nonce, type: "height", height: 500 }, {});
  fromFrame(frame, { nonce: "nope", type: "height", height: 500 });
  fromFrame(frame, { type: "height", height: 500 });
  fromFrame(frame, "just text");
  fromFrame(frame, null);
  fromFrame(frame, { nonce, type: "height", height: -1 });
  fromFrame(frame, { nonce, type: "height", height: Infinity });
  fromFrame(frame, { nonce, type: "height", height: 1e9 });
  fromFrame(frame, { nonce, type: "height", height: "500" });
  fromFrame(frame, { nonce, type: "link", url: "javascript:alert(1)" });
  fromFrame(frame, { nonce, type: "link", url: "data:text/html,x" });
  fromFrame(frame, { nonce, type: "link", url: 5 });
  fromFrame(frame, { nonce, type: "key", key: "j" });
  fromFrame(frame, { nonce, type: "other" });
  assert.equal(frame.style.height, height);
  assert.deepEqual(opened, []);
  assert.ok(readerNode());
});

test("the frame sizes itself, hands links back to a new tab, and can pass Esc back", async () => {
  await start();
  const frame = await openFrame();
  const nonce = nonceOf(frame);
  fromFrame(frame, { nonce, type: "height", height: 2345 });
  assert.equal(frame.style.height, "2345px");
  fromFrame(frame, { nonce, type: "link", url: "https://elsewhere.test/x" });
  assert.deepEqual(opened, [{ url: "https://elsewhere.test/x", target: "_blank", features: "noopener,noreferrer" }]);
  fromFrame(frame, { nonce, type: "key", key: "Escape" });
  assert.equal(readerNode(), undefined);
  await flushed();
  const link = ofType("open").find((e) => e.metadata.mode === "browser");
  assert.deepEqual([link.item_id, link.position, link.metadata.via, link.metadata.url, link.metadata.link_index], ["hn:1", 0, "article_link", "https://elsewhere.test/x", -1]);
});

test("with the reader open only Esc acts, and closing gives the keyboard back", async () => {
  await start();
  await openFrame();
  for (const key of ["j", "l", "s", "x", "v", "p", "r", "1", "o"]) assert.equal(press(key), false, key);
  assert.equal(scroller().scrollTop, 0);
  assert.equal(cards().length, 3);
  assert.deepEqual(opened, []);
  scroller().focused = null;
  assert.equal(press("Escape"), true);
  assert.equal(readerNode(), undefined);
  assert.deepEqual(scroller().focused, { preventScroll: true });
  press("l");
  await flushed();
  assert.equal(ofType("like").length, 1);
});

test("nothing counts as seen while the reader covers the list, and measuring resumes on close", async () => {
  await start();
  runFrames();
  advance(400);
  await openFrame();
  await flushed();
  assert.deepEqual(ofType("impression").map((e) => [e.item_id, e.dwell_ms]), [["hn:1", 400], ["hn:2", 400]]);
  const before = ofType("impression").length;
  scroller().fire("scroll");
  observers.forEach((o) => o.fn());
  runFrames();
  advance(5000);
  await flushed();
  assert.equal(ofType("impression").length, before);
  press("Escape");
  runFrames();
  advance(300);
  scrollTo(5000);
  await flushed();
  assert.deepEqual(ofType("impression").slice(before).map((e) => e.dwell_ms), [300, 300]);
});

test("close carries active time, depth and clock time, with nothing nested", async () => {
  await start();
  await openFrame();
  const overlay = readerNode();
  overlay.scrollTop = 500; overlay.clientHeight = 500; overlay.scrollHeight = 2000;
  overlay.fire("scroll");
  advance(4000);
  press("Escape");
  await flushed();
  const [close] = ofType("close");
  assert.deepEqual([close.item_id, close.dwell_ms, close.metadata.mode, close.metadata.scroll_depth, close.metadata.wall_ms, close.metadata.nested_ms], ["hn:1", 4000, "detail", 0.5, 4000, 0]);
  assert.equal(close.metadata.resumed, undefined);
});

test("scrolling the reader keeps the clock going, because input inside the frame never reaches the page", async () => {
  await start();
  await openFrame();
  const overlay = readerNode();
  for (let i = 0; i < 5; i++) { advance(50_000); overlay.fire("scroll"); }
  advance(10_000);
  press("Escape");
  await flushed();
  assert.equal(ofType("close")[0].dwell_ms, 260_000);
});

test("hiding the tab with the reader open writes a close first, and coming back starts a resumed stretch", async () => {
  await start();
  await openFrame();
  advance(1000);
  setVisible("hidden");
  await settle();
  assert.ok(readerNode(), "the reader stays open");
  advance(60_000);
  setVisible("visible");
  advance(2000);
  press("Escape");
  await flushed();
  const closes = ofType("close");
  assert.deepEqual(closes.map((e) => [e.dwell_ms, e.metadata.resumed]), [[1000, undefined], [2000, true]]);
  assert.ok(events().indexOf(closes[0]) < events().indexOf(ofType("session_end")[0]));
  assert.equal(ofType("open").length, 1, "no extra open");
});

test("leaving the page with the reader open closes it once and removes it", async () => {
  await start();
  await openFrame();
  advance(1000);
  view.cleanup(); view = null;
  await settle();
  assert.equal(readerNode(), undefined);
  assert.equal(ofType("close").length, 1);
});

test("a page with no article says so, offers the page, and logs the failure", async () => {
  articleResponse = () => ({ status: 422, body: { error: "no article found" } });
  await start();
  press("o");
  await settle();
  assert.equal(readerNode().all((n) => n.tag === "iframe").length, 0);
  const link = anchors(readerNode()).find((a) => a.textContent === "Open the page");
  assert.equal(link.href, "https://a.test/1");
  link.fire("click", { button: 0 });
  await flushed();
  const [ui] = ofType("ui");
  assert.deepEqual([ui.metadata.action, ui.metadata.ok], ["article", false]);
  assert.match(ui.metadata.error, /no article/);
  assert.equal(ofType("open").at(-1).metadata.via, "card");
});

test("an answer after closing the reader draws nothing", async () => {
  let answer;
  articleResponse = () => new Promise((resolve) => { answer = resolve; });
  await start();
  press("o");
  await settle();
  press("Escape");
  answer({ status: 200, body: { html: "<p>x</p>", words: 90 } });
  await settle();
  assert.equal(readerNode(), undefined);
});

const X_CARD = { item_id: "x:1", source: "x", url: "https://x.com/a/status/1", engagement: {}, embed: { type: "x_post", author_name: "A Poster", author_handle: "poster", text: "Hello from X", media: [
  { type: "image", url: "https://pbs.test/1.jpg" }, { type: "image", url: "https://pbs.test/2.jpg" }, { type: "image", url: "https://pbs.test/3.jpg" }, { type: "image", url: "https://pbs.test/4.jpg" }, { type: "image", url: "https://pbs.test/5.jpg" },
] } };
const YT_CARD = { item_id: "yt:1", source: "yt", title: "A video", url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=90", body: "Summary", engagement: {} };
const VIMEO_CARD = { item_id: "vm:1", source: "vm", title: "Other video", url: "https://vimeo.com/123456", image_url: "https://i.test/vm.jpg", engagement: {} };
const FILE_CARD = { item_id: "f:1", source: "x", url: "https://x.com/a/status/2", engagement: {}, embed: { type: "x_post", author_name: "B", text: "Clip", media: [{ type: "video", url: "https://video.twimg.com/a.mp4?x=\"><script>", thumbnail_url: "https://pbs.test/p.jpg", width: 1280, height: 720 }] } };
const HLS_CARD = { item_id: "h:1", source: "m", title: "Stream", url: "https://h.test/page", media: [{ type: "video", url: "https://h.test/live.m3u8", thumbnail_url: "https://h.test/p.jpg" }], engagement: {} };
const TIKTOK_CARD = { item_id: "t:1", source: "t", title: "Tok", url: "https://www.tiktok.com/@a/video/1234567890123456", image_url: "https://i.test/t.jpg", engagement: {} };

function serve(...items) { feedResponse = () => ({ status: 200, body: { items, saved: [] } }); }

test("an X post, a video card and a card with no url show their own text without fetching anything", async () => {
  for (const [item, text] of [[X_CARD, /Hello from X/], [YT_CARD, /Summary/], [CARDS[2], /Third/], [TIKTOK_CARD, /Tok/]]) {
    serve(item);
    await start();
    press("o");
    await settle();
    assert.equal(articleAsks.length, 0, item.item_id);
    assert.match(readerNode().textContent, text);
    assert.equal(readerNode().all((n) => n.tag === "iframe").length, 0);
    await flushed();
    assert.equal(ofType("ui").length, 0, "no false article failure");
    view.cleanup(); view = null;
    root = new Node("div");
  }
});

test("the article's document is sealed and hands everything back by message", () => {
  const html = articleDocument({ html: "<p>x</p>", language: "en\"", image_url: "https://i.test/lead.jpg" }, "abc");
  assert.ok(html.includes(`<meta name="referrer" content="no-referrer">`));
  assert.ok(html.includes(`<script nonce="abc">`));
  assert.ok(html.includes("parent.postMessage"));
  assert.ok(html.includes("https://i.test/lead.jpg"));
  assert.ok(html.includes(`lang="en&quot;"`));
  assert.ok(!html.includes("unsafe-eval"));
  assert.ok(!/script-src[^;"]*unsafe-inline/.test(html));
  assert.ok(!articleDocument({ html: `<img src="https://i.test/lead.jpg">`, image_url: "https://i.test/lead.jpg" }, "abc").includes("<figure>"), "no second copy of the lead image");
});

// Runs the frame's own script against a stub page, as the sandbox would.
function runArticleScript() {
  const script = articleDocument({ html: "<p>x</p>" }, "abc").match(/<script nonce="abc">([\s\S]*?)<\/script>/)[1];
  const listeners = {};
  const sent = [];
  const page = {
    addEventListener(type, fn) { listeners[type] = fn; },
    getElementById() { return { getBoundingClientRect: () => ({ height: 10 }) }; },
    querySelectorAll() { return []; },
  };
  new Function("document", "window", "parent", script)(page, { addEventListener() {} }, { postMessage: (m) => sent.push(m) });
  const click = (href, resolved, extra = {}) => {
    let prevented = false;
    const anchor = href === null ? null : { getAttribute: () => href, href: resolved };
    listeners[extra.type || "click"]({ type: "click", button: 0, target: { closest: () => anchor }, preventDefault() { prevented = true; }, ...extra });
    return prevented;
  };
  return { click, links: () => sent.filter((m) => m.type === "link") };
}

test("a link within the article stays in the frame: following it would load the parent's url there and blank the reader", () => {
  const frame = runArticleScript();
  assert.equal(frame.click("#note-1", "https://pdw.test/feed#note-1"), true);
  assert.equal(frame.click("#note-1", "https://pdw.test/feed#note-1", { type: "auxclick", button: 1 }), true);
  assert.deepEqual(frame.links(), [], "nothing to open: the server strips the ids these links point at");
});

test("a link out of the article goes to the page by message, and other clicks are left alone", () => {
  const frame = runArticleScript();
  assert.equal(frame.click("https://a.test/x", "https://a.test/x"), true);
  assert.deepEqual(frame.links().map((m) => [m.url, m.nonce]), [["https://a.test/x", "abc"]]);
  assert.equal(frame.click(null), false);
  assert.equal(frame.links().length, 1);
});

test("a frame's message is checked for nonce, shape and bounds", () => {
  assert.deepEqual(parseArticleMessage({ nonce: "n", type: "height", height: 10 }, "n"), { type: "height", height: 10 });
  assert.deepEqual(parseArticleMessage({ nonce: "n", type: "link", url: "http://a.test/" }, "n"), { type: "link", url: "http://a.test/" });
  assert.deepEqual(parseArticleMessage({ nonce: "n", type: "key", key: "Escape" }, "n"), { type: "key", key: "Escape" });
  assert.equal(parseArticleMessage({ nonce: "m", type: "height", height: 10 }, "n"), null);
  assert.equal(parseArticleMessage({ nonce: "", type: "height", height: 10 }, ""), null);
  assert.equal(parseArticleMessage({ nonce: "n", type: "link", url: "mailto:a@b.test" }, "n"), null);
});

test("the article rule is the phone's: an X post or a video is not an article", () => {
  assert.equal(articleUrl({ url: "https://a.test/x" }, false), "https://a.test/x");
  assert.equal(articleUrl({ url: "https://a.test/x" }, true), null);
  assert.equal(articleUrl({ url: "https://a.test/x", embed: { type: "x_post" } }, false), null);
  assert.equal(articleUrl({}, false), null);
  assert.equal(articleUrl({ url: "ftp://a.test/x" }, false), null);
});

// --- video ------------------------------------------------------------------------

// A video card's box, placed for the geometry: y and height of the player's box.
function placeBoxes() { for (const card of cards()) for (const box of byClass(card, "fpic")) { box.y = card.y; box.height = 225; } }
const play = (card) => byClass(card, "fposter")[0].fire("click");
const playerOf = (card) => card.all((n) => n.tag === "iframe")[0];

test("a click swaps the poster for the player in the same box and logs media", async () => {
  serve(YT_CARD, VIMEO_CARD);
  await start();
  placeBoxes();
  const [first, second] = cards();
  const box = byClass(first, "fpic")[0];
  assert.equal(box.style.aspectRatio, String(16 / 9));
  play(first);
  assert.equal(byClass(first, "fpic")[0], box);
  assert.equal(box.style.aspectRatio, String(16 / 9));
  assert.equal(byClass(first, "fposter").length, 0);
  const frame = playerOf(first);
  assert.match(frame.src, /^https:\/\/www\.youtube\.com\/embed\/dQw4w9WgXcQ\?autoplay=1.*start=90/);
  assert.equal(frame.getAttribute("referrerpolicy"), "strict-origin-when-cross-origin", "YouTube refuses to play with no referrer");
  assert.match(frame.getAttribute("allow"), /autoplay/);
  press("j"); // moving on keeps it only while it stays on screen
  assert.ok(second);
  await flushed();
  const [media] = ofType("media");
  assert.deepEqual([media.item_id, media.position, media.metadata.action, media.metadata.inline], ["yt:1", 0, "play", true]);
});

test("one plays at a time, and the first's close says it was watched in the card", async () => {
  serve(YT_CARD, VIMEO_CARD);
  await start();
  placeBoxes();
  const [first, second] = cards();
  play(first);
  advance(3000);
  play(second);
  assert.equal(playerOf(first), undefined);
  assert.equal(byClass(first, "fposter").length, 1, "the poster is back");
  assert.match(playerOf(second).src, /^https:\/\/player\.vimeo\.com\/video\/123456\?autoplay=1/);
  await flushed();
  const [close] = ofType("close");
  assert.deepEqual([close.item_id, close.dwell_ms, close.metadata.mode, close.metadata.inline, close.metadata.autoplay, close.metadata.wall_ms], ["yt:1", 3000, "video", true, false, 3000]);
});

test("p plays the current card's video and p again stops it, and Esc stops it too", async () => {
  serve(YT_CARD, CARDS[1]);
  await start();
  placeBoxes();
  assert.equal(press("p"), true);
  assert.ok(playerOf(cards()[0]));
  press("p");
  assert.equal(playerOf(cards()[0]), undefined);
  press("p");
  press("Escape");
  assert.equal(playerOf(cards()[0]), undefined);
  await flushed();
  assert.equal(ofType("close").length, 2);
});

test("it stops when under a quarter of its box is on screen, and not before", async () => {
  serve(YT_CARD, CARDS[1], CARDS[2]);
  await start();
  placeBoxes();
  const first = cards()[0];
  play(first);
  scrollTo(150); // 75 of 225 px left, a third
  assert.ok(playerOf(first));
  scrollTo(200); // 25 px left, a ninth
  assert.equal(playerOf(first), undefined);
  await flushed();
  assert.equal(ofType("close").length, 1);
});

test("it stops when the tab hides, with the close before the session's end, and stays stopped on return", async () => {
  serve(YT_CARD, CARDS[1]);
  await start();
  placeBoxes();
  const first = cards()[0];
  play(first);
  advance(2000);
  setVisible("hidden");
  assert.equal(playerOf(first), undefined);
  setVisible("visible");
  assert.equal(playerOf(first), undefined);
  await flushed();
  const close = ofType("close")[0];
  assert.equal(close.dwell_ms, 2000);
  assert.ok(events().indexOf(close) < events().indexOf(ofType("session_end")[0]));
});

test("opening the reader, leaving the page or hiding the card stops it", async () => {
  serve(YT_CARD, CARDS[1]);
  await start();
  placeBoxes();
  const first = cards()[0];
  play(first);
  scroller().scrollTop = 500; // the reader is on hn:2
  press("o");
  await settle();
  assert.equal(playerOf(first), undefined);
  press("Escape");
  scroller().scrollTop = 0;
  play(first);
  press("x");
  assert.equal(playerOf(first), undefined);
  press("z");
  play(first);
  view.cleanup(); view = null;
  await settle();
  assert.equal(ofType("close").filter((e) => e.metadata.mode === "video").length, 3);
});

test("a playing video keeps the clock busy past the minute with no touches, and stopping lets it idle", async () => {
  serve(YT_CARD, CARDS[1]);
  await start();
  placeBoxes();
  const first = cards()[0];
  runFrames();
  play(first);
  advance(5 * 60_000);
  press("Escape");
  advance(5 * 60_000);
  await flushed();
  assert.equal(ofType("close")[0].dwell_ms, 5 * 60_000);
  scrollTo(5000);
  await flushed();
  assert.equal(ofType("impression")[0].dwell_ms, 5 * 60_000 + IDLE_MS);
});

test("a lone file video plays in a no-referrer document with escaped urls and reports its pause", async () => {
  serve(FILE_CARD);
  await start();
  placeBoxes();
  const first = cards()[0];
  assert.match(first.textContent, /B/);
  assert.match(first.textContent, /Clip/);
  assert.equal(byClass(first, "fpic")[0].style.aspectRatio, String(1280 / 720));
  play(first);
  const frame = playerOf(first);
  const html = frame.getAttribute("srcdoc");
  assert.equal(frame.getAttribute("referrerpolicy"), "no-referrer");
  assert.ok(html.includes(`<meta name="referrer" content="no-referrer">`));
  assert.ok(html.includes("default-src 'none'"));
  assert.ok(!html.includes(`"><script>`));
  const video = new Node("video");
  frame.contentDocument = { querySelector: () => video, addEventListener() {} };
  frame.fire("load");
  advance(10_000);
  video.fire("pause");
  advance(5 * 60_000);
  video.fire("play");
  advance(5 * 60_000);
  press("Escape");
  await flushed();
  assert.equal(ofType("close")[0].dwell_ms, 10_000 + IDLE_MS + 5 * 60_000);
});

test("HLS, TikTok and a page with no player stay a poster that opens the link", async () => {
  serve(HLS_CARD, TIKTOK_CARD);
  await start();
  placeBoxes();
  for (const card of cards()) {
    assert.equal(byClass(card, "fposter").length, 0);
    assert.equal(byClass(card, "fpic")[0].tag, "a");
  }
  assert.equal(press("p"), false);
  assert.equal(cardPlayer(HLS_CARD), null);
  assert.ok(cardVideo(HLS_CARD), "still a video, so it is not fetched as an article");
  assert.ok(cardVideo(TIKTOK_CARD));
});

test("the player rule is the phone's", () => {
  assert.equal(cardVideo({ url: "https://a.test/" }), null);
  assert.equal(cardVideo({ url: "https://youtu.be/dQw4w9WgXcQ" }).kind, "youtube");
  assert.equal(cardVideo({ url: "https://vimeo.com/123456" }).id, "123456");
  assert.equal(cardVideo({ url: "https://a.test/", media: [{ type: "video", url: "https://v.test/a.mp4" }] }).kind, "file");
  assert.equal(cardVideo({ url: "https://youtu.be/dQw4w9WgXcQ", media: [{ type: "video", url: "https://v.test/a.mp4" }, { type: "image", url: "https://i.test/a.jpg" }] }), null, "a carousel keeps its pages");
  assert.equal(cardPlayer({ embed: { type: "x_post", media: [{ type: "video", url: "https://video.twimg.com/a.mp4" }, { type: "image", url: "https://i.test/a.jpg" }] } }), null);
  assert.equal(cardPlayer({ embed: { type: "x_post", media: [{ type: "video", url: "javascript:x" }] } }), null);
  assert.match(playerUrl({ kind: "youtube", id: "dQw4w9WgXcQ" }), /^https:\/\/www\.youtube\.com\/embed\/dQw4w9WgXcQ\?/);
  assert.match(playerUrl({ kind: "vimeo", id: "123456" }), /^https:\/\/player\.vimeo\.com\/video\/123456\?/);
  assert.ok(!fileVideoDocument({ url: "https://v.test/a.mp4?a=1&b=\"", poster: "https://i.test/\"x" }).includes(`b="`));
});

test("a playing clock and a stopwatch keep the phone's numbers", () => {
  const c = new ActiveClock(0);
  c.setBusy(true, 0);
  assert.equal(c.read(5 * 60_000), 5 * 60_000);
  c.setForeground(false, 5 * 60_000);
  assert.equal(c.read(10 * 60_000), 5 * 60_000);
  c.setForeground(true, 10 * 60_000);
  c.setBusy(false, 11 * 60_000);
  assert.equal(c.read(11 * 60_000 + IDLE_MS + 30_000), 6 * 60_000 + IDLE_MS);
  const d = new ActiveClock(0);
  const watch = stopwatch(d, 0);
  assert.deepEqual(watch.stop(5 * 60_000), { activeMs: IDLE_MS, wallMs: 5 * 60_000 });
});

// --- X posts ----------------------------------------------------------------------

test("an X post shows its author, text and up to four photos in boxes of known shape", async () => {
  serve({ ...X_CARD, title: "A pick" });
  await start();
  const [card] = cards();
  assert.match(card.textContent, /A Poster @poster/);
  assert.match(card.textContent, /Hello from X/);
  const grid = byClass(card, "fgrid")[0];
  assert.ok(grid.className.includes("fgrid4"));
  assert.ok(grid.style.aspectRatio);
  assert.deepEqual(grid.all((n) => n.tag === "img").map((i) => i.src), ["https://pbs.test/1.jpg", "https://pbs.test/2.jpg", "https://pbs.test/3.jpg", "https://pbs.test/4.jpg"]);
  assert.ok(grid.all((n) => n.tag === "img").every((i) => i.getAttribute("referrerpolicy") === "no-referrer"));
  assert.equal(xPhotos({ media: [{ type: "image", url: "javascript:x" }, { type: "video", url: "https://v.test/a.mp4" }] }).length, 0);
});

test("an X post with one photo keeps the single-picture box", async () => {
  serve({ ...X_CARD, embed: { ...X_CARD.embed, media: [{ type: "image", url: "https://pbs.test/1.jpg", width: 100, height: 100 }] } });
  await start();
  assert.equal(byClass(cards()[0], "fgrid").length, 0);
  assert.equal(byClass(cards()[0], "fpic")[0].style.aspectRatio, "1");
});

// --- coming back, and r -----------------------------------------------------------

const NEW_CARDS = [{ ...CARDS[1], title: "New order" }, CARDS[0]];
function stale() { setVisible("hidden"); advance(10 * 60_000 + 1); setVisible("visible"); }

test("back after ten minutes measures nothing until the reload settles", async () => {
  await start();
  runFrames();
  advance(400);
  let answer;
  feedResponse = () => new Promise((resolve) => { answer = resolve; });
  stale();
  await settle();
  await flushed();
  const before = ofType("impression").length;
  layOut();
  scrollTo(100);
  advance(400);
  scrollTo(5000);
  advance(400);
  await flushed();
  assert.equal(ofType("impression").length, before);
  answer({ status: 200, body: { items: CARDS, saved: [] } });
  await settle();
  layOut();
  scrollTo(0);
  advance(300);
  scrollTo(5000);
  await flushed();
  assert.ok(ofType("impression").length > before);
});

test("a stale reload that fails also lets measuring resume, and keeps the cards", async () => {
  await start();
  feedResponse = () => ({ status: 500, body: { error: "boom" } });
  stale();
  await settle();
  assert.equal(cards().length, 3);
  layOut();
  runFrames();
  advance(300);
  scrollTo(5000);
  await flushed();
  assert.ok(ofType("impression").length > 0);
  assert.equal(ofType("load").at(-1).metadata.ok, false);
  assert.match(root.textContent, /boom/);
});

test("scrolled down, the list stays put, and the load still names what was served", async () => {
  await start();
  const before = cards();
  scrollTo(121);
  serve(...NEW_CARDS);
  stale();
  await settle();
  assert.deepEqual(cards(), before);
  assert.equal(scroller().scrollTop, 121);
  await flushed();
  const load = ofType("load").at(-1);
  assert.equal(load.metadata.trigger, "stale");
  assert.deepEqual(load.metadata.item_ids, ["hn:2", "hn:1"]);
});

test("near the top, the new list replaces the old one", async () => {
  await start();
  scrollTo(120);
  serve(...NEW_CARDS);
  stale();
  await settle();
  assert.deepEqual(cards().map((c) => c.getAttribute("data-id")), ["hn:2", "hn:1"]);
  assert.match(cards()[0].textContent, /New order/);
  await flushed();
  assert.equal(ofType("load").at(-1).metadata.trigger, "stale");
});

test("a return under ten minutes does not reload", async () => {
  await start();
  await flushed();
  setVisible("hidden");
  advance(10 * 60_000 - 1);
  setVisible("visible");
  await settle();
  await flushed();
  assert.equal(ofType("load").length, 1);
});

test("r ends open stretches, holds measuring, loads with pull, replaces the list and goes to the top", async () => {
  await start();
  runFrames();
  advance(400);
  let answer;
  feedResponse = () => new Promise((resolve) => { answer = resolve; });
  assert.equal(press("r"), true);
  await settle();
  await flushed();
  const during = ofType("impression").length;
  assert.deepEqual(ofType("impression").map((e) => e.item_id), ["hn:1", "hn:2"], "the cards still on screen when r was pressed");
  assert.equal(ofType("ui")[0].metadata.action, "refresh");
  layOut();
  scrollTo(100);
  advance(400);
  scrollTo(5000);
  advance(400);
  await flushed();
  assert.equal(ofType("impression").length, during);
  assert.match(root.textContent, /First/, "no flash of Loading over a list");
  assert.doesNotMatch(root.textContent, /Loading/);
  press("r"); // already loading
  answer({ status: 200, body: { items: NEW_CARDS, saved: [] } });
  await settle();
  assert.deepEqual(cards().map((c) => c.getAttribute("data-id")), ["hn:2", "hn:1"]);
  assert.equal(scroller().scrollTop, 0);
  scroller().fire("scroll");
  advance(500);
  await flushed();
  assert.deepEqual(ofType("load").map((e) => e.metadata.trigger), ["first", "pull"]);
  assert.equal(ofType("scroll").filter((e) => e.metadata.to_y === 0).length, 0, "the jump to the top is not a scroll he made");
});

test("a failed r keeps the cards on screen with a short note", async () => {
  await start();
  feedResponse = () => ({ status: 500, body: { error: "boom" } });
  press("r");
  await settle();
  assert.equal(cards().length, 3);
  assert.match(root.textContent, /boom/);
  await flushed();
  assert.deepEqual(ofType("load").map((e) => e.metadata.ok), [true, false]);
});
