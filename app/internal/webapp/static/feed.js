// The Feed page: the same cards in the same order as the phone's Feed tab, with
// the same events, so a card seen here leaves the fresh part there. No cached
// paint and no re-ranking: the list on screen is what GET /api/feed just served.
import { feed, getToken } from "./api.js";
import { h } from "./ui.js";
import { ActiveClock, EventQueue, FeedLog, ViewabilityTracker } from "./feed_log.js";
import { cardContext, cardLinks, parseMarkdown, picture, safeUrl, shortAge } from "./feed_card.js";

const OUTBOX_KEY = "pdw_feed_outbox";
const FLUSH_EVERY_MS = 15_000;
// A scroll is over when nothing has moved for this long.
const SCROLL_REST_MS = 150;
const VIEW = "all";
// The served order, under the server's 16 KB metadata cap (the phone's servedIds).
const SERVED_BYTES = 12_000;
const TOUCHES = ["pointerdown", "pointermove", "keydown", "wheel", "touchstart"];

// Outlives a mount: the first sitting since the page started is a launch.
const lifetime = { launched: false, startedAt: Date.now(), lastEndedAt: 0 };

function servedIds(items) {
  const ids = [];
  let bytes = 0;
  for (const item of items) {
    bytes += item.item_id.length + 3;
    if (bytes > SERVED_BYTES) break;
    ids.push(item.item_id);
  }
  return { item_ids: ids, item_ids_truncated: ids.length < items.length };
}

function deviceContext() {
  return {
    app_version: "web",
    update_id: "web",
    channel: "web",
    os: navigator.platform || "",
    color_scheme: window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light",
    tz: Intl.DateTimeFormat().resolvedOptions().timeZone,
  };
}

const isVisible = () => document.visibilityState === "visible";

export function mount(container, ctx) {
  // The token gate is open; unlocking mounts the view again.
  if (!getToken()) return {};

  container.textContent = "";
  const scroller = container.appendChild(h("div"));
  scroller.id = "fv";
  scroller.setAttribute("tabindex", "0");
  // So PageDown, Space and the arrows scroll the column before any click.
  scroller.focus({ preventScroll: true });
  const list = scroller.appendChild(h("div", "flist"));

  const clock = new ActiveClock(Date.now(), isVisible());
  const tracker = new ViewabilityTracker();
  const queue = new EventQueue(
    { load: () => localStorage.getItem(OUTBOX_KEY), save: (value) => localStorage.setItem(OUTBOX_KEY, value) },
    (events, options) => feed.postEvents(events, options),
    Date.now,
    (message) => console.warn(message),
  );
  const log = new FeedLog({ queue, clock, context: deviceContext, lifetime });

  let cards = []; // [{ id, node }] in served order
  let loadStarted = false;
  let dead = false;
  let frame = 0;
  let gesture = null;
  let restTimer = 0;
  let lastY = 0;

  function layouts() {
    const base = scroller.getBoundingClientRect();
    const out = new Map();
    cards.forEach(({ id, node }, position) => {
      const rect = node.getBoundingClientRect();
      out.set(id, { y: rect.top - base.top + scroller.scrollTop, height: rect.height, position });
    });
    return out;
  }

  let lastLayouts = new Map();
  function recordImpressions(ended) {
    for (const imp of ended) {
      log.record("impression", {
        at: imp.startedAt,
        item_id: imp.itemId,
        position: imp.position,
        dwell_ms: imp.dwellMs,
        visible_fraction: imp.maxFraction,
        metadata: {
          visible_ms: imp.visibleMs,
          ended_at: new Date(imp.endedAt).toISOString(),
          height: Math.round((lastLayouts.get(imp.itemId) || { height: 0 }).height),
          view: VIEW,
        },
      });
    }
  }

  // Never measure a tab nobody can see: any stretch that reaches 50%, even at
  // 0 ms, is an impression, and one impression takes a card out of the fresh part.
  function measure() {
    if (dead || !cards.length || !isVisible() || !log.inSession()) return;
    lastLayouts = layouts();
    const viewport = { top: scroller.scrollTop, height: scroller.getBoundingClientRect().height };
    const now = Date.now();
    recordImpressions(tracker.update(now, viewport, lastLayouts, clock.read(now)));
  }

  function schedule() {
    if (frame || dead) return;
    frame = requestAnimationFrame(() => { frame = 0; measure(); });
  }

  function cardAt(y) {
    for (const layout of layouts().values()) if (layout.y + layout.height > y) return layout.position;
    return -1;
  }

  function endScroll() {
    clearTimeout(restTimer);
    const g = gesture;
    gesture = null;
    if (!g || g.toY === g.fromY) return;
    const distance = Math.round(g.toY - g.fromY);
    log.record("scroll", {
      at: g.startedAt,
      dwell_ms: g.lastAt - g.startedAt,
      metadata: {
        from_y: Math.round(g.fromY),
        to_y: Math.round(g.toY),
        distance,
        direction: distance > 0 ? "down" : "up",
        from_card: cardAt(g.fromY),
        to_card: cardAt(g.toY),
        viewport: Math.round(scroller.getBoundingClientRect().height),
        view: VIEW,
      },
    });
  }

  function onScroll() {
    if (!isVisible()) return;
    const now = Date.now();
    clock.touch(now);
    gesture = gesture || { startedAt: now, fromY: lastY };
    lastY = scroller.scrollTop;
    gesture.lastAt = now;
    gesture.toY = lastY;
    clearTimeout(restTimer);
    restTimer = setTimeout(endScroll, SCROLL_REST_MS);
    schedule();
  }

  // --- rendering: text nodes and http(s) links only ---------------------------

  function makeLink(node, item, position, url, via, index) {
    node.href = url;
    node.target = "_blank";
    node.rel = "noopener noreferrer";
    const open = () => log.record("open", { item_id: item.item_id, position, metadata: { mode: "browser", via, url, link_index: index } });
    node.addEventListener("click", (ev) => { if (ev.button === 0) open(); });
    node.addEventListener("auxclick", (ev) => { if (ev.button === 1) open(); });
    return node;
  }

  function spansInto(parent, spans, item, position) {
    for (const span of spans) {
      const url = safeUrl(span.url);
      if (url) parent.appendChild(makeLink(h("a", "", span.text), item, position, url, "body_link", -1));
      else if (span.bold) parent.appendChild(h("b", "", span.text));
      else if (span.italic) parent.appendChild(h("i", "", span.text));
      else if (span.code) parent.appendChild(h("code", "", span.text));
      else parent.appendChild(document.createTextNode(span.text));
    }
  }

  function blockNode(block, item, position) {
    if (block.kind === "code") return h("pre", "", block.text);
    const tag = { heading: "h3", quote: "blockquote" }[block.kind] || "p";
    const node = h(tag, block.kind === "bullet" ? "fbullet" : "");
    if (block.kind === "bullet") node.appendChild(document.createTextNode((block.ordered ? block.ordered + ". " : "• ")));
    spansInto(node, block.spans, item, position);
    return node;
  }

  function cardNode(item, position) {
    const embed = item.embed && item.embed.type === "x_post" ? item.embed : null;
    const context = cardContext(item);
    const links = cardLinks(item);
    const main = links.find((link) => link.index === 0);
    const name = item.site || item.source;
    const node = h("article", "fcard");
    node.setAttribute("data-id", item.item_id);

    const byline = embed && [embed.author_name, embed.author_handle && "@" + embed.author_handle].filter(Boolean).join(" ");
    const title = item.title || byline || name;
    const heading = node.appendChild(h("h2", "ftitle"));
    if (main) heading.appendChild(makeLink(h("a", "", title), item, position, main.url, "card", 0));
    else heading.textContent = title;

    const age = shortAge(item.published_at || item.created_at);
    const sub = [item.title || byline ? name : "", context.author, item.site && item.source !== item.site ? item.source : "", age, item.resurfaced ? "Seen before" : ""];
    node.appendChild(h("div", "fsub", sub.filter(Boolean).join(" · ")));

    const blocks = embed ? (embed.text ? [{ kind: "paragraph", spans: [{ text: embed.text }] }] : []) : parseMarkdown(context.preview);
    if (blocks.length) {
      const body = node.appendChild(h("div", "fbody"));
      for (const block of blocks) body.appendChild(blockNode(block, item, position));
    }

    const shown = picture(item);
    if (shown) {
      const box = node.appendChild(main ? makeLink(h("a", "fpic"), item, position, main.url, "card", 0) : h("div", "fpic"));
      box.style.aspectRatio = String(shown.aspect);
      const image = box.appendChild(h("img"));
      image.src = shown.url;
      image.alt = shown.alt;
      image.setAttribute("loading", "lazy");
      image.setAttribute("referrerpolicy", "no-referrer");
    }

    const extras = links.filter((link) => link.index > 0);
    if (extras.length) {
      const row = node.appendChild(h("div", "flinks"));
      for (const link of extras) row.appendChild(makeLink(h("a", "", link.label), item, position, link.url, "link", link.index));
    }

    if (context.hn) {
      const text = `▲ ${context.hn.points} · ${context.hn.comments} comments`;
      const url = safeUrl(context.hn.url);
      node.appendChild(url ? makeLink(h("a", "fhn", text), item, position, url, "post_link", -1) : h("div", "fhn", text));
    }
    return node;
  }

  function showMessage(text, cls) {
    list.textContent = "";
    list.appendChild(h("div", "fmsg " + cls, text));
  }

  function render(items) {
    list.textContent = "";
    cards = items.map((item, position) => ({ id: item.item_id, node: list.appendChild(cardNode(item, position)) }));
    ctx.setStats(items.length === 1 ? "1 card" : items.length + " cards");
    if (!items.length) showMessage("Nothing here yet.", "");
    schedule();
  }

  async function load() {
    loadStarted = true;
    const startedAt = Date.now();
    const sessionId = log.sessionId();
    showMessage("Loading…", "");
    void log.flush(true);
    try {
      const page = await feed.page();
      const items = page.items || [];
      log.record("load", {
        at: startedAt, sessionId, dwell_ms: Date.now() - startedAt,
        metadata: { trigger: "first", ok: true, cards: items.length, saved: (page.saved || []).length, ...servedIds(items) },
      });
      if (!dead) render(items);
    } catch (e) {
      log.record("load", {
        at: startedAt, sessionId, dwell_ms: Date.now() - startedAt,
        metadata: { trigger: "first", ok: false, status: e.status || 0, error: String(e.message).slice(0, 300) },
      });
      if (!dead) showMessage(e.status === 404 ? "The feed is off." : e.message + " Reload the page to try again.", "bad");
    }
  }

  // --- one sitting: tab visible, on this page ---------------------------------

  function arrive(trigger) {
    if (dead || !isVisible() || log.inSession()) return;
    log.startSession(cards.length, trigger);
    if (loadStarted) schedule(); else void load();
  }

  function leave(reason) {
    if (!log.inSession()) return;
    endScroll();
    const now = Date.now();
    recordImpressions(tracker.flush(now, clock.read(now)));
    log.endSession(reason);
  }

  function onVisibility() {
    const now = Date.now();
    if (isVisible()) {
      clock.setForeground(true, now);
      arrive("foreground");
    } else {
      leave("background");
      clock.setForeground(false, now);
    }
  }
  const onPageHide = () => leave("background");
  const onPageShow = () => { if (isVisible()) { clock.setForeground(true, Date.now()); arrive("foreground"); } };
  const onTouch = () => clock.touch(Date.now());

  document.addEventListener("visibilitychange", onVisibility);
  window.addEventListener("pagehide", onPageHide);
  window.addEventListener("pageshow", onPageShow);
  for (const type of TOUCHES) window.addEventListener(type, onTouch);
  scroller.addEventListener("scroll", onScroll);
  const observer = new ResizeObserver(schedule);
  observer.observe(list);
  const flushTimer = setInterval(() => void log.flush(), FLUSH_EVERY_MS);

  arrive("focus");

  return {
    cleanup() {
      if (dead) return;
      leave("blur");
      dead = true;
      clearInterval(flushTimer);
      clearTimeout(restTimer);
      if (frame) cancelAnimationFrame(frame);
      observer.disconnect();
      document.removeEventListener("visibilitychange", onVisibility);
      window.removeEventListener("pagehide", onPageHide);
      window.removeEventListener("pageshow", onPageShow);
      for (const type of TOUCHES) window.removeEventListener(type, onTouch);
    },
    flushEvents: () => log.flush(true),
  };
}
