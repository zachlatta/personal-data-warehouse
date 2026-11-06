import test, { beforeEach, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import { ActiveClock, EventQueue, IDLE_MS, ViewabilityTracker, visibleFraction } from "../feed_log.js";
import { cardContext, cardLinks, parseInline, parseMarkdown, picture, safeUrl } from "../feed_card.js";

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
  appendChild(node) { this.children.push(node); return node; }
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

let root, frames, posts, stats, storage, observers, documentEvents, windowEvents;
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
let feedResponse;

beforeEach(() => {
  mock.timers.enable({ apis: ["setTimeout", "setInterval", "Date"], now: 1_000_000 });
  root = new Node("div"); frames = []; posts = []; stats = ""; storage = {}; observers = []; documentEvents = {}; windowEvents = {};
  feedResponse = () => ({ status: 200, body: { items: CARDS, saved: [], generated_at: "2026-11-05T00:00:00Z" } });
  globalThis.document = {
    visibilityState: "visible",
    createElement: (tag) => new Node(tag),
    createTextNode: (text) => { const n = new Node("#text"); n.textContent = text; return n; },
    addEventListener: (event, fn) => { (documentEvents[event] ||= []).push(fn); },
    removeEventListener: (event, fn) => { documentEvents[event] = (documentEvents[event] || []).filter((f) => f !== fn); },
    dispatchEvent() {},
  };
  globalThis.window = {
    addEventListener: (event, fn) => { (windowEvents[event] ||= []).push(fn); },
    removeEventListener: (event, fn) => { windowEvents[event] = (windowEvents[event] || []).filter((f) => f !== fn); },
    matchMedia: () => ({ matches: true }),
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
      posts.push({ path, headers: init.headers, keepalive: init.keepalive, body: JSON.parse(init.body) });
      return { ok: true, status: 200, text: async () => "{}" };
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
  advance(10 * 60_000);
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
