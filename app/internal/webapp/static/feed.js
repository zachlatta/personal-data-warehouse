// The Feed page: the same cards in the same order as the phone's Feed tab, with
// the same events, so a card seen here leaves the fresh part there. No cached
// paint and no re-ranking: the list on screen is what GET /api/feed just served.
import { feed, getToken } from "./api.js";
import { h } from "./ui.js";
import { ActiveClock, EventQueue, FeedLog, ViewabilityTracker, applyPendingDismissals, applySave, mergeSaved, stopwatch, visibleFraction } from "./feed_log.js";
import { articleDocument, articleUrl, cardContext, cardLinks, cardPlayer, cardVideo, fileVideoDocument, parseArticleMessage, parseMarkdown, picture, playerUrl, safeUrl, shortAge, videoAspect, xPhotos } from "./feed_card.js";

const OUTBOX_KEY = "pdw_feed_outbox";
const FLUSH_EVERY_MS = 15_000;
// A scroll is over when nothing has moved for this long.
const SCROLL_REST_MS = 150;
const VIEW = "all";
// The served order, under the server's 16 KB metadata cap (the phone's servedIds).
const SERVED_BYTES = 12_000;
const TOUCHES = ["pointerdown", "pointermove", "keydown", "wheel", "touchstart"];
// The phone's numbers (feed-screen.tsx, autoplay.ts).
const UNDO_MS = 5_000;
const STALE_MS = 10 * 60_000;
const PILL_GONE_Y = 120;
const STOP_BELOW = 0.25;
// The current card is the one under this line, in px from the top of the column.
const CURRENT_LINE = 24;
const OPEN_FEATURES = "noopener,noreferrer";

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

const newNonce = () => Array.from(crypto.getRandomValues(new Uint8Array(16)), (b) => b.toString(16).padStart(2, "0")).join("");

// Taps still in the outbox count as pending, after a reload or a return to the page.
function replayTaps(events, on, off, pending) {
  for (const e of events) {
    if (e.item_id && (e.type === on || e.type === off)) pending.set(e.item_id, { save: e.type === on, at: Date.parse(e.occurred_at) });
  }
}

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
  const bar = container.appendChild(h("div", "fbar"));

  const clock = new ActiveClock(Date.now(), isVisible());
  const tracker = new ViewabilityTracker();
  const queue = new EventQueue(
    { load: () => localStorage.getItem(OUTBOX_KEY), save: (value) => localStorage.setItem(OUTBOX_KEY, value) },
    (events, options) => feed.postEvents(events, options),
    Date.now,
    (message) => console.warn(message),
  );
  const log = new FeedLog({ queue, clock, context: deviceContext, lifetime });

  let cards = []; // [{ id, node, item, position, ... }]; position is what was served, for life
  let liked = [];
  let saved = [];
  const pendingLikes = new Map();
  const pendingSaves = new Map();
  let message = null; // the node saying the list is empty or loading
  let undo = null; // { text, run, timer }
  let reader = null; // the open overlay: { card, overlay, frame, nonce, watch, resumed, depth }
  let playing = null; // the one video playing: { card, box, watch, busy }
  let currentNode = null;
  let loadStarted = false;
  let loading = false; // nothing is measured until a load settles
  let loadedAt = 0;
  let dead = false;
  let frame = 0;
  let gesture = null;
  let restTimer = 0;
  let lastY = 0;

  function layouts() {
    const base = scroller.getBoundingClientRect();
    const out = new Map();
    for (const { id, node, position } of cards) {
      const rect = node.getBoundingClientRect();
      out.set(id, { y: rect.top - base.top + scroller.scrollTop, height: rect.height, position });
    }
    return out;
  }

  const heights = new Map(); // outlive a hidden card, whose stretch ends after it is gone
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
          height: Math.round(heights.get(imp.itemId) || 0),
          view: VIEW,
        },
      });
    }
  }

  // Never measure a tab nobody can see: any stretch that reaches 50%, even at
  // 0 ms, is an impression, and one impression takes a card out of the fresh part.
  // Nor one under the reader, nor a list about to be replaced.
  function measure() {
    if (dead || !isVisible() || !log.inSession() || reader || loading) return;
    const now = Date.now();
    const placed = layouts();
    for (const [id, layout] of placed) heights.set(id, layout.height);
    const viewport = { top: scroller.scrollTop, height: scroller.getBoundingClientRect().height };
    recordImpressions(tracker.update(now, viewport, placed, clock.read(now)));
  }

  function endStretches() {
    const now = Date.now();
    recordImpressions(tracker.flush(now, clock.read(now)));
  }

  function currentCard() {
    const line = scroller.scrollTop + CURRENT_LINE;
    const placed = layouts();
    return cards.find((card) => { const l = placed.get(card.id); return l.y + l.height > line; }) || cards[cards.length - 1] || null;
  }

  // An outline only, so nothing moves.
  function markCurrent() {
    const node = currentCard()?.node || null;
    if (node === currentNode) return;
    if (currentNode) currentNode.className = "fcard";
    if (node) node.className = "fcard fcur";
    currentNode = node;
  }

  function checkVideo() {
    if (!playing) return;
    const base = scroller.getBoundingClientRect();
    const rect = playing.box.getBoundingClientRect();
    if (visibleFraction({ y: rect.top - base.top, height: rect.height }, { top: 0, height: base.height }) < STOP_BELOW) stopVideo();
  }

  function schedule() {
    if (frame || dead) return;
    frame = requestAnimationFrame(() => { frame = 0; measure(); markCurrent(); checkVideo(); });
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

  function recordOpen(item, position, url, via, index) {
    log.record("open", { item_id: item.item_id, position, metadata: { mode: "browser", via, url, link_index: index } });
  }

  function makeLink(node, item, position, url, via, index) {
    node.href = url;
    node.target = "_blank";
    node.rel = "noopener noreferrer";
    const open = () => recordOpen(item, position, url, via, index);
    node.addEventListener("click", (ev) => { if (ev.button === 0) open(); });
    node.addEventListener("auxclick", (ev) => { if (ev.button === 1) open(); });
    return node;
  }

  function openUrl(item, position, url, via, index) {
    window.open(url, "_blank", OPEN_FEATURES);
    recordOpen(item, position, url, via, index);
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

  function pictureImage(url, alt) {
    const image = h("img");
    image.src = url;
    image.alt = alt;
    image.setAttribute("loading", "lazy");
    image.setAttribute("referrerpolicy", "no-referrer");
    return image;
  }

  function cardBlocks(item, embed, context) {
    return embed ? (embed.text ? [{ kind: "paragraph", spans: [{ text: embed.text }] }] : []) : parseMarkdown(context.preview);
  }

  // Up to four of an X post's photos, laid out as X does, in a box of known shape.
  function photoGrid(photos, item, position, main) {
    const grid = h("div", "fgrid fgrid" + photos.length);
    grid.style.aspectRatio = String(16 / 9);
    for (const photo of photos) {
      const cell = grid.appendChild(main ? makeLink(h("a"), item, position, main.url, "card", 0) : h("div"));
      cell.appendChild(pictureImage(photo.url, photo.alt || ""));
    }
    return grid;
  }

  function posterNode(card, shown) {
    const poster = h("button", "fposter");
    poster.title = "Play (p)";
    poster.setAttribute("aria-label", "Play video");
    if (shown) poster.appendChild(pictureImage(shown.url, shown.alt));
    poster.appendChild(h("span", "", "▶"));
    poster.addEventListener("click", () => playVideo(card));
    return poster;
  }

  function actionButton(row, action) {
    const node = row.appendChild(h("button"));
    node.addEventListener("click", () => action("button"));
    return node;
  }

  function showActions(card) {
    const isLiked = liked.includes(card.id);
    const isSaved = saved.includes(card.id);
    card.like.textContent = isLiked ? "Liked" : "Like";
    card.like.title = isLiked ? "Unlike (l)" : "Like (l)";
    card.like.setAttribute("aria-pressed", String(isLiked));
    card.save.textContent = isSaved ? "Saved" : "Save";
    card.save.title = isSaved ? "Remove from Saved (s)" : "Save (s)";
    card.save.setAttribute("aria-pressed", String(isSaved));
  }

  function cardNode(item, position) {
    const embed = item.embed && item.embed.type === "x_post" ? item.embed : null;
    const context = cardContext(item);
    const links = cardLinks(item);
    const main = links.find((link) => link.index === 0);
    const name = item.site || item.source;
    const node = h("article", "fcard");
    node.setAttribute("data-id", item.item_id);
    const card = { id: item.item_id, node, item, position, links, player: cardPlayer(item), box: null, poster: null };

    const byline = embed && [embed.author_name, embed.author_handle && "@" + embed.author_handle].filter(Boolean).join(" ");
    const title = item.title || byline || name;
    const heading = node.appendChild(h("h2", "ftitle"));
    if (main) heading.appendChild(makeLink(h("a", "", title), item, position, main.url, "card", 0));
    else heading.textContent = title;

    const age = shortAge(item.published_at || item.created_at);
    const sub = [item.title || byline ? name : "", item.title && byline ? byline : "", embed ? "" : context.author, item.site && item.source !== item.site ? item.source : "", age, item.resurfaced ? "Seen before" : ""];
    node.appendChild(h("div", "fsub", sub.filter(Boolean).join(" · ")));

    const blocks = cardBlocks(item, embed, context);
    if (blocks.length) {
      const body = node.appendChild(h("div", "fbody"));
      for (const block of blocks) body.appendChild(blockNode(block, item, position));
    }

    const shown = picture(item);
    const photos = embed ? xPhotos(embed) : [];
    if (card.player) {
      card.box = node.appendChild(h("div", "fpic"));
      card.box.style.aspectRatio = String(videoAspect(card.player, shown));
      card.poster = card.box.appendChild(posterNode(card, shown));
    } else if (photos.length > 1) {
      node.appendChild(photoGrid(photos, item, position, main));
    } else if (shown) {
      const box = node.appendChild(main ? makeLink(h("a", "fpic"), item, position, main.url, "card", 0) : h("div", "fpic"));
      box.style.aspectRatio = String(shown.aspect);
      box.appendChild(pictureImage(shown.url, shown.alt));
    }

    const extras = links.filter((link) => link.index > 0);
    if (extras.length) {
      const row = node.appendChild(h("div", "flinks"));
      extras.forEach((link, i) => {
        const anchor = makeLink(h("a"), item, position, link.url, "link", link.index);
        if (i < 9) anchor.appendChild(h("b", "", i + 1));
        anchor.appendChild(document.createTextNode(link.label));
        row.appendChild(anchor);
      });
    }

    if (context.hn) {
      const text = `▲ ${context.hn.points} · ${context.hn.comments} comments`;
      const url = safeUrl(context.hn.url);
      node.appendChild(url ? makeLink(h("a", "fhn", text), item, position, url, "post_link", -1) : h("div", "fhn", text));
    }

    const row = node.appendChild(h("div", "factions"));
    card.like = actionButton(row, (via) => toggleLike(card, via));
    card.save = actionButton(row, (via) => toggleSave(card, via));
    const hideButton = actionButton(row, () => hideCard(card));
    hideButton.textContent = "Hide";
    hideButton.title = "Hide (x)";
    const readButton = actionButton(row, () => openReader(card));
    readButton.textContent = "Read";
    readButton.title = "Read (o)";
    showActions(card);
    return card;
  }

  function showMessage(text, cls) {
    list.textContent = "";
    message = list.appendChild(h("div", "fmsg " + cls, text));
  }

  function showCount() {
    ctx.setStats(cards.length === 1 ? "1 card" : cards.length + " cards");
    if (!cards.length) showMessage("Nothing here yet.", "");
  }

  function render(items) {
    stopVideo();
    list.textContent = "";
    message = null;
    currentNode = null;
    cards = items.map((item, position) => {
      const card = cardNode(item, position);
      list.appendChild(card.node);
      return card;
    });
    showCount();
    schedule();
  }

  // --- like, save, hide, undo -----------------------------------------------------

  function clearBar() {
    if (undo) clearTimeout(undo.timer);
    undo = null;
    bar.className = "fbar";
    bar.textContent = "";
  }

  function showBar(text, run) {
    clearBar();
    bar.appendChild(h("span", "", text));
    if (run) {
      const button = bar.appendChild(h("button", "", "Undo (z)"));
      button.addEventListener("click", runUndo);
    }
    bar.className = "fbar on";
    undo = { text, run, timer: setTimeout(clearBar, UNDO_MS) };
  }

  function runUndo() {
    if (!undo || !undo.run) return;
    const { text, run } = undo;
    log.ui("undo", { metadata: { what: text } });
    clearBar();
    run();
  }

  function toggleLike(card, via) {
    const like = !liked.includes(card.id);
    liked = applySave(liked, card.id, like);
    pendingLikes.set(card.id, { save: like, at: Date.now() });
    log.record(like ? "like" : "unlike", { item_id: card.id, position: card.position, metadata: { via, source: card.item.source } });
    showActions(card);
  }

  function setSaved(card, via) {
    const save = !saved.includes(card.id);
    saved = applySave(saved, card.id, save);
    pendingSaves.set(card.id, { save, at: Date.now() });
    log.record(save ? "save" : "unsave", { item_id: card.id, position: card.position, metadata: { via, source: card.item.source } });
    showActions(card);
    return save;
  }

  function toggleSave(card, via) {
    const save = setSaved(card, via);
    showBar(save ? "Saved for later" : "Removed from Saved", () => setSaved(card, "undo"));
  }

  function hideCard(card) {
    const at = cards.indexOf(card);
    if (at < 0) return;
    if (playing && playing.card === card) stopVideo();
    const top = scroller.scrollTop;
    const { item, position } = card;
    log.record("dismiss", { item_id: card.id, position, metadata: { reason: "not_interested", source: item.source, site: item.site || "" } });
    cards.splice(at, 1);
    card.node.remove();
    if (currentNode === card.node) currentNode = null;
    showCount();
    schedule();
    showBar("Hidden", () => {
      log.record("undismiss", { item_id: card.id, position });
      if (message) { message.remove(); message = null; }
      const place = Math.min(at, cards.length);
      list.insertBefore(card.node, cards[place] ? cards[place].node : null);
      cards.splice(place, 0, card);
      // Back where he was, wherever he has scrolled since.
      scroller.scrollTop = top;
      showCount();
      schedule();
    });
  }

  // --- video: YouTube, Vimeo or a lone file, in the card, one at a time ----------

  function setVideoBusy(on) {
    if (!playing || playing.busy === on) return;
    playing.busy = on;
    clock.setBusy(on, Date.now());
  }

  function playerFrame(source) {
    const player = h("iframe");
    player.setAttribute("title", "Video");
    player.setAttribute("allow", "autoplay; encrypted-media; picture-in-picture; fullscreen");
    player.setAttribute("allowfullscreen", "");
    if (source.kind === "file") {
      const mine = playing;
      player.setAttribute("referrerpolicy", "no-referrer");
      player.setAttribute("srcdoc", fileVideoDocument(source));
      // Only a plain <video> says it paused; a site's player keeps the clock busy until it stops.
      player.addEventListener("load", () => {
        const inner = player.contentDocument;
        const video = inner && inner.querySelector("video");
        if (!video) return;
        video.addEventListener("pause", () => { if (playing === mine) setVideoBusy(false); });
        video.addEventListener("play", () => { if (playing === mine) setVideoBusy(true); });
        inner.addEventListener("keydown", onKey);
      });
    } else {
      // YouTube refuses to play with no referrer (its error 153).
      player.setAttribute("referrerpolicy", source.kind === "youtube" ? "strict-origin-when-cross-origin" : "no-referrer");
      player.src = playerUrl(source);
    }
    return player;
  }

  function playVideo(card) {
    if (!card.player) return;
    const again = playing && playing.card === card;
    stopVideo();
    if (again) return;
    const now = Date.now();
    log.record("media", { item_id: card.id, position: card.position, metadata: { action: "play", index: 0, inline: true } });
    playing = { card, box: card.box, watch: stopwatch(clock, now), busy: false };
    setVideoBusy(true);
    card.box.textContent = "";
    card.box.appendChild(playerFrame(card.player));
  }

  function stopVideo() {
    if (!playing) return;
    setVideoBusy(false);
    const { card, watch } = playing;
    playing = null;
    const { activeMs, wallMs } = watch.stop(Date.now());
    log.record("close", { item_id: card.id, dwell_ms: activeMs, metadata: { mode: "video", index: 0, inline: true, autoplay: false, wall_ms: wallMs } });
    card.box.textContent = "";
    card.box.appendChild(card.poster);
  }

  // --- the reader: an overlay, with the article in a sealed frame ------------------

  function endReaderStretch() {
    if (!reader || !reader.watch) return;
    noteDepth();
    const { activeMs, wallMs } = reader.watch.stop(Date.now());
    reader.watch = null;
    log.record("close", {
      item_id: reader.card.id,
      dwell_ms: activeMs,
      metadata: { mode: "detail", scroll_depth: Math.round(reader.depth * 100) / 100, wall_ms: wallMs, nested_ms: 0, ...(reader.resumed ? { resumed: true } : {}) },
    });
  }

  function noteDepth() {
    const { overlay } = reader;
    if (overlay.scrollHeight > 0) reader.depth = Math.max(reader.depth, Math.min(1, (overlay.scrollTop + overlay.clientHeight) / overlay.scrollHeight));
  }

  function closeReader() {
    if (!reader) return;
    endReaderStretch();
    reader.overlay.remove();
    reader = null;
    scroller.focus({ preventScroll: true });
    schedule();
  }

  // The app's tokens as they resolve right now (light or dark), for the frame.
  function readerPalette() {
    const css = getComputedStyle(document.documentElement);
    const token = (name) => css.getPropertyValue(name).trim();
    return { bg: token("--surface"), text: token("--text"), muted: token("--text-2"), border: token("--border"), code: token("--surface-2"), link: token("--accent") };
  }

  function readerFrame(article) {
    const nonce = newNonce();
    const frame = h("iframe", "frframe");
    frame.setAttribute("title", article.title || "Article");
    // No allow-same-origin: script in here has an opaque origin and no localStorage.
    frame.setAttribute("sandbox", "allow-scripts");
    frame.setAttribute("referrerpolicy", "no-referrer");
    frame.setAttribute("srcdoc", articleDocument(article, nonce, readerPalette()));
    reader.frame = frame;
    reader.nonce = nonce;
    return frame;
  }

  function onMessage(ev) {
    if (!reader || !reader.frame || ev.source !== reader.frame.contentWindow) return;
    const said = parseArticleMessage(ev.data, reader.nonce);
    if (!said) return;
    if (said.type === "height") reader.frame.style.height = said.height + "px";
    else if (said.type === "key") closeReader();
    else openUrl(reader.card.item, reader.card.position, said.url, "article_link", -1);
  }

  async function readArticle(state, url) {
    const { card, main } = state;
    const fields = { item_id: card.id, position: card.position, sessionId: log.sessionId() };
    const startedAt = Date.now();
    const waiting = main.appendChild(h("div", "fmsg", "Loading…"));
    try {
      const article = await feed.article(url);
      log.ui("article", { ...fields, dwell_ms: Date.now() - startedAt, metadata: { ok: true, words: article.words } });
      if (reader !== state) return;
      waiting.remove();
      if (article.byline) state.sub.textContent = article.byline;
      if (article.title) state.heading.textContent = article.title;
      main.appendChild(readerFrame(article));
    } catch (e) {
      log.ui("article", { ...fields, dwell_ms: Date.now() - startedAt, metadata: { ok: false, error: String(e.message).slice(0, 300) } });
      if (reader !== state) return;
      waiting.textContent = "This page has no article to show here.";
      waiting.appendChild(makeLink(h("a", "", "Open the page"), card.item, card.position, url, "card", 0));
    }
  }

  function showOwnText(state) {
    const { item, position } = state.card;
    const embed = item.embed && item.embed.type === "x_post" ? item.embed : null;
    const own = state.main.appendChild(h("div", "fbody frown"));
    for (const block of cardBlocks(item, embed, cardContext(item))) own.appendChild(blockNode(block, item, position));
    const photos = embed ? xPhotos(embed) : [];
    if (photos.length) state.main.appendChild(photoGrid(photos, item, position, null));
  }

  function openReader(card) {
    if (reader || !card) return;
    stopVideo();
    endStretches();
    const { item, position } = card;
    log.record("open", { item_id: card.id, position, metadata: { mode: "detail" } });

    const overlay = container.appendChild(h("div", "freader"));
    overlay.setAttribute("tabindex", "0");
    const head = overlay.appendChild(h("div", "frhead"));
    const close = head.appendChild(h("button", "", "Close (Esc)"));
    close.addEventListener("click", closeReader);
    const main = cardLinks(item).find((link) => link.index === 0);
    if (main) head.appendChild(makeLink(h("a", "", "Open the page"), item, position, main.url, "card", 0));
    const column = overlay.appendChild(h("div", "frmain"));
    const embed = item.embed && item.embed.type === "x_post" ? item.embed : null;
    const heading = column.appendChild(h("h2", "ftitle", item.title || (embed && embed.author_name) || item.site || item.source));
    const sub = column.appendChild(h("div", "fsub", cardContext(item).author || (embed && embed.author_handle ? "@" + embed.author_handle : "") || item.site || ""));

    const state = { card, overlay, main: column, heading, sub, frame: null, nonce: "", watch: stopwatch(clock, Date.now()), resumed: false, depth: 0 };
    reader = state;
    overlay.addEventListener("scroll", () => {
      // The frame's own input never reaches this page; scrolling is how a long read stays active.
      clock.touch(Date.now());
      if (reader === state) noteDepth();
    });
    overlay.focus({ preventScroll: true });
    const url = articleUrl(item, !!cardVideo(item));
    if (url) void readArticle(state, url); else showOwnText(state);
  }

  // --- keys ----------------------------------------------------------------------

  function jump(delta) {
    const card = currentCard();
    if (!card) return;
    const placed = layouts();
    const at = cards.indexOf(card);
    const partRead = delta < 0 && scroller.scrollTop > placed.get(card.id).y + 1;
    const target = partRead ? card : cards[at + delta];
    if (target) scroller.scrollTop = placed.get(target.id).y;
  }

  function openCurrentLink(number) {
    const card = currentCard();
    if (!card) return;
    const link = number === 0 ? card.links.find((l) => l.index === 0) : card.links.filter((l) => l.index > 0)[number - 1];
    if (link) openUrl(card.item, card.position, link.url, number === 0 ? "card" : "link", link.index);
  }

  function keyAction(key) {
    const onCurrent = (run) => () => { const card = currentCard(); if (card) run(card); };
    switch (key) {
      case "j": return () => jump(1);
      case "k": return () => jump(-1);
      case "o": case "Enter": return onCurrent(openReader);
      case "v": return () => openCurrentLink(0);
      case "p": return currentCard()?.player ? onCurrent(playVideo) : null;
      case "l": return onCurrent((card) => toggleLike(card, "key"));
      case "s": return onCurrent((card) => toggleSave(card, "key"));
      case "x": return onCurrent(hideCard);
      case "z": return runUndo;
      case "r": return refresh;
      case "Escape": return playing ? stopVideo : null;
      default: return /^[1-9]$/.test(key) ? () => openCurrentLink(Number(key)) : null;
    }
  }

  // Holding a key must not repeat its action: a held j would mark a card seen on
  // every repeat that reaches a frame, a held x hide card after card, a held v open tabs.
  function onKey(ev) {
    if (ev.ctrlKey || ev.metaKey || ev.altKey || ev.repeat) return;
    if (document.getElementById("gate")?.classList.contains("open")) return;
    const inside = (selector) => ev.target && ev.target.closest && ev.target.closest(selector);
    if (inside("input, textarea, select, [contenteditable]")) return;
    if (reader) {
      if (ev.key === "Escape") { ev.preventDefault(); closeReader(); }
      return;
    }
    if (ev.key === "Enter" && inside("a, button")) return;
    const run = keyAction(ev.key);
    if (!run) return;
    ev.preventDefault();
    run();
  }

  function adoptTaps(page, items) {
    const events = log.pending();
    replayTaps(events, "like", "unlike", pendingLikes);
    replayTaps(events, "save", "unsave", pendingSaves);
    const all = [...items, ...(page.saved || [])];
    liked = mergeSaved(all.filter((i) => i.engagement && i.engagement.liked).map((i) => i.item_id), pendingLikes);
    saved = mergeSaved((page.saved || []).map((i) => i.item_id), pendingSaves);
    for (const card of cards) showActions(card);
  }

  function refresh() {
    if (loading) return;
    log.ui("refresh");
    endScroll();
    endStretches();
    void load("pull");
  }

  // trigger: first, stale (back after STALE_MS) or pull (r). The load event logs
  // what was served in order, whether or not the list on screen changes.
  async function load(trigger) {
    if (loading) return;
    loading = true;
    loadStarted = true;
    const startedAt = Date.now();
    const sessionId = log.sessionId();
    if (!cards.length) showMessage("Loading…", "");
    const before = log.pending();
    void log.flush(true);
    try {
      const page = await feed.page();
      const items = applyPendingDismissals(page.items || [], [...before, ...log.pending()]);
      log.record("load", {
        at: startedAt, sessionId, dwell_ms: Date.now() - startedAt,
        metadata: { trigger, ok: true, cards: items.length, saved: (page.saved || []).length, ...servedIds(items) },
      });
      loadedAt = Date.now();
      if (dead) return;
      adoptTaps(page, items);
      // Scrolled down, the list stays put: nothing moves under him.
      if (trigger === "pull" || !cards.length || scroller.scrollTop <= PILL_GONE_Y) render(items);
      if (trigger === "pull") {
        lastY = 0;
        scroller.scrollTop = 0;
      }
    } catch (e) {
      log.record("load", {
        at: startedAt, sessionId, dwell_ms: Date.now() - startedAt,
        metadata: { trigger, ok: false, status: e.status || 0, error: String(e.message).slice(0, 300) },
      });
      if (dead) return;
      if (!cards.length) showMessage(e.status === 404 ? "The feed is off." : e.message + " Press r to try again.", "bad");
      else if (!undo) showBar("Could not reload: " + e.message);
    } finally {
      loading = false;
      schedule();
    }
  }

  // --- one sitting: tab visible, on this page ---------------------------------

  function arrive(trigger) {
    if (dead || !isVisible() || log.inSession()) return;
    log.startSession(cards.length, trigger);
    if (reader && !reader.watch) {
      reader.watch = stopwatch(clock, Date.now());
      reader.resumed = true;
    }
    if (!loadStarted || (loadedAt && Date.now() - loadedAt > STALE_MS)) void load(loadedAt ? "stale" : "first");
    else schedule();
  }

  function leave(reason) {
    if (!log.inSession()) return;
    endScroll();
    stopVideo();
    // The tab may never come back, so this stretch of reading ends now; the reader
    // stays open and a return starts another, whose close says it resumed.
    endReaderStretch();
    endStretches();
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
  document.addEventListener("keydown", onKey);
  window.addEventListener("message", onMessage);
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
      clearBar();
      if (reader) { reader.overlay.remove(); reader = null; }
      if (frame) cancelAnimationFrame(frame);
      observer.disconnect();
      document.removeEventListener("visibilitychange", onVisibility);
      document.removeEventListener("keydown", onKey);
      window.removeEventListener("message", onMessage);
      window.removeEventListener("pagehide", onPageHide);
      window.removeEventListener("pageshow", onPageShow);
      for (const type of TOUCHES) window.removeEventListener(type, onTouch);
    },
    flushEvents: () => log.flush(true),
  };
}
