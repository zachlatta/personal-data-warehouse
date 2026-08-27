// The event inspector: a drawer showing one timeline event, its conversation
// (timeline.context), media, child rows and the full source record. Shared by
// the timeline ledger and search results.
import { timeline } from "./api.js";
import { el, h, clear, section, kvTable, fmtTime, fmtFull, dayOf, isReal, hue } from "./ui.js";

let onClose = null;

export function closeDrawer() {
  el("drawer").classList.remove("open");
  if (onClose) { const fn = onClose; onClose = null; fn(); }
}
el("dclose").addEventListener("click", closeDrawer);
document.addEventListener("keydown", (ev) => { if (ev.key === "Escape") closeDrawer(); });

/* Inline media rendered through the pdw object proxy (signed, expiring
   /objects/ links minted server-side). Images click through to the raw
   file; anything a browser can't render inline falls back to a link. */
export function mediaNode(row, label) {
  if (!row || !row.media_url) return null;
  const wrap = h("div");
  const name = label || row.filename || row.title || "";
  if (row.media_kind === "image") {
    const img = h("img", "thumb");
    img.loading = "lazy"; img.src = row.media_url; img.alt = name;
    img.addEventListener("click", () => window.open(row.media_url, "_blank"));
    img.addEventListener("error", () => {
      const link = h("a", "filelink", "⇗ " + (name || "open file") + " (preview unavailable)");
      link.href = row.media_url; link.target = "_blank";
      wrap.replaceChild(link, img);
    });
    wrap.appendChild(img);
  } else if (row.media_kind === "audio") {
    const audio = h("audio", "player");
    audio.controls = true; audio.preload = "none"; audio.src = row.media_url;
    wrap.appendChild(audio);
  } else if (row.media_kind === "video") {
    const video = h("video", "player");
    video.controls = true; video.preload = "none"; video.src = row.media_url;
    wrap.appendChild(video);
  } else {
    const anchor = h("a", "filelink", "⇗ " + (name || "open file"));
    anchor.href = row.media_url; anchor.target = "_blank";
    wrap.appendChild(anchor);
  }
  return wrap;
}

function childRowNode(childRow) {
  const node = h("div", "childrow");
  const media = mediaNode(childRow);
  if (media) node.appendChild(media);
  let text = childRow.text || childRow.summary || "";
  if (!media && !text) text = childRow.filename || childRow.name || "";
  const metaBits = [];
  for (const key in childRow) {
    if (key === "text" || key === "summary" || key === "media_url" || key === "storage_file_id") continue;
    const val = childRow[key];
    if (val === "" || val === null || val === 0) continue;
    metaBits.push(key + "=" + String(val).slice(0, 60));
  }
  if (text) node.appendChild(h("div", "bigtext", String(text)));
  node.appendChild(h("div", "m", metaBits.join("  ")));
  return node;
}

function appendChildSection(body, item, name, rows, initialMeta) {
  const sect = section(name.replace(/_/g, " "));
  const status = h("div", "m");
  let loaded = rows.length;
  let meta = initialMeta || { has_more: false, next_offset: loaded };
  sect.appendChild(status);
  rows.forEach((row) => sect.appendChild(childRowNode(row)));
  const more = h("button", "", "load more");
  more.type = "button";

  function refreshStatus() {
    status.textContent = loaded + " loaded" + (meta.has_more ? " · more available" : " · complete");
    if (meta.has_more) { if (!more.parentNode) sect.appendChild(more); }
    else if (more.parentNode) more.parentNode.removeChild(more);
  }
  more.addEventListener("click", () => {
    more.disabled = true; more.textContent = "loading…";
    timeline.children(item.adapter, item.event_id, name, meta.next_offset).then((page) => {
      (page.rows || []).forEach((row) => sect.insertBefore(childRowNode(row), more.parentNode ? more : null));
      loaded += (page.rows || []).length;
      meta = page;
      more.disabled = false; more.textContent = "load more";
      refreshStatus();
    }).catch((err) => { more.disabled = false; more.textContent = "retry: " + err.message; });
  });
  refreshStatus();
  body.appendChild(sect);
}

/* ---- conversation context ----
   The conversation around the event, from timeline.context(): an email's
   thread, a Slack message's thread or its channel, the rest of an
   iMessage/WhatsApp chat, the neighboring turns of an agent session, and
   otherwise the adjacent events of the same (source, context) stream. */
function contextRowNode(row, lastDay, open) {
  const node = h("div", "crow p-" + (row.priority || "unclassified") + (row.is_anchor ? " anchor" : ""));
  const day = dayOf(row.event_ts);
  if (day !== lastDay.value) { lastDay.value = day; node.appendChild(h("div", "cd", day)); }
  node.appendChild(h("div", "ct", fmtTime(row.event_ts)));
  const main = h("div");
  main.appendChild(h("div", "ca", (row.actor || "—") + (row.title && row.title !== row.snippet ? "  ·  " + row.title : "")));
  const text = row.snippet || (row.title ? "" : "(" + row.kind + ")");
  if (text) main.appendChild(h("div", "cb", text));
  node.appendChild(main);
  if (!row.is_anchor) {
    node.title = "open this event";
    node.addEventListener("click", () => open(row));
  }
  return node;
}

function appendContextSection(body, item, page, beforeNode, open) {
  if (!page || !page.items || !page.items.length) return;
  const sect = section("conversation");
  const box = h("div", "convo");
  const win = { before: page.before || 0, after: page.after || 0 };
  const ctl = h("div", "convo-ctl");
  const earlier = h("button", "", "← earlier"); earlier.type = "button";
  const later = h("button", "", "later →"); later.type = "button";
  const status = h("div", "m");
  ctl.appendChild(earlier); ctl.appendChild(status); ctl.appendChild(later);

  function render(items) {
    clear(box);
    const lastDay = { value: "" };
    let anchorNode = null;
    items.forEach((row) => {
      const node = contextRowNode(row, lastDay, open);
      if (row.is_anchor) anchorNode = node;
      box.appendChild(node);
    });
    status.textContent = items.length + " events · " + win.before + " before / " + win.after + " after";
    earlier.disabled = win.before >= 50;
    later.disabled = win.after >= 50;
    if (anchorNode) setTimeout(() => {
      box.scrollTop = Math.max(0, anchorNode.offsetTop - box.clientHeight / 2 + anchorNode.offsetHeight / 2);
    }, 0);
  }
  function refetch() {
    status.textContent = "loading…";
    timeline.context(item.adapter, item.event_id, win.before, win.after).then((next) => {
      win.before = next.before; win.after = next.after;
      render(next.items || []);
    }).catch((err) => { status.textContent = "context failed: " + err.message; });
  }
  earlier.addEventListener("click", () => { win.before = Math.min(50, win.before + 15); refetch(); });
  later.addEventListener("click", () => { win.after = Math.min(50, win.after + 15); refetch(); });

  sect.appendChild(box); sect.appendChild(ctl);
  render(page.items);
  body.insertBefore(sect, beforeNode || null);
}

function openButton(open) {
  const sect = h("div", "sect");
  const btn = h("a", "openbtn", "⇗  open in " + open.label);
  btn.href = open.url; btn.target = "_blank"; btn.rel = "noopener";
  sect.appendChild(btn);
  return sect;
}

// openItem shows the drawer for a timeline row (list row, search hit, or a
// context row). options.onClose runs when the drawer closes; options.onOpen
// runs whenever an item is (re)opened from inside the drawer, for the URL.
export function openItem(item, options = {}) {
  onClose = options.onClose || onClose;
  if (options.onOpen) options.onOpen(item);
  const open = (row) => openItem(row, { onOpen: options.onOpen });
  const drawer = el("drawer");
  drawer.classList.add("open");
  const kindNode = el("dkind");
  kindNode.textContent = item.source + " / " + item.kind;
  kindNode.style.background = hue(item.source);
  el("dtitle").textContent = item.title || item.snippet || item.event_id;
  const body = clear(el("dbody"));

  if (item.open && item.open.url) body.appendChild(openButton(item.open));

  const head = section("event");
  head.appendChild(kvTable([
    ["when", fmtFull(item.event_ts)], ["until", isReal(item.end_ts) ? fmtFull(item.end_ts) : ""],
    ["actor", item.actor], ["context", item.context],
    ["priority", item.priority],
    ["adapter", item.adapter], ["event id", item.event_id],
    ["seq", item.seq], ["source table", item.source_table],
  ]));
  body.appendChild(head);

  if (item.snippet) {
    const snip = section("preview");
    snip.appendChild(h("div", "bigtext", item.snippet));
    body.appendChild(snip);
  }
  if (item.metadata && Object.keys(item.metadata).length) {
    const meta = section("metadata", item.metadata);
    meta.appendChild(kvTable(Object.keys(item.metadata).map((key) => [key, item.metadata[key]])));
    body.appendChild(meta);
  }
  const loading = section("source record");
  loading.appendChild(h("div", "m", "fetching full record…"));
  body.appendChild(loading);

  timeline.item(item.adapter, item.event_id).then((detail) => {
    body.removeChild(loading);
    // The detail row carries the deep link even when the list row
    // predates it (context rows opened from the transcript, search hits).
    if (detail.item && detail.item.open && !(item.open && item.open.url)) {
      item.open = detail.item.open;
      body.insertBefore(openButton(item.open), body.firstChild);
    }
    if (detail.context) {
      // right after the preview (or the event table when there is none)
      const anchorSect = body.children[item.open && item.open.url ? 3 : 2] || null;
      appendContextSection(body, item, detail.context, anchorSect, open);
    }
    if (detail.item_media) {
      const mediaSect = section("media");
      const node = mediaNode(detail.item_media, detail.item_media.filename);
      if (node) mediaSect.appendChild(node);
      body.insertBefore(mediaSect, body.children[1] || null);
    }
    const children = detail.children || {};
    const childrenMeta = detail.children_meta || {};
    Object.keys(children).sort().forEach((name) => {
      const rows = children[name];
      if (!rows || rows.error || !rows.length) return;
      appendChildSection(body, item, name, rows, childrenMeta[name]);
    });
    if (detail.source_row) {
      const raw = section("source row (full record)", detail.source_row);
      raw.appendChild(h("pre", "blob", JSON.stringify(detail.source_row, null, 2)));
      body.appendChild(raw);
    } else if (detail.source_row_error) {
      const errSect = section("source row");
      errSect.appendChild(h("div", "m", "unavailable: " + detail.source_row_error));
      body.appendChild(errSect);
    }
  }).catch((err) => { loading.textContent = "detail fetch failed: " + err.message; });
}

// openRef opens an item known only by adapter + event id (a deep link, a
// search hit): fetch the detail first, then show it.
export async function openRef(adapter, eventId, options = {}) {
  const detail = await timeline.item(adapter, eventId);
  if (!detail || !detail.item) throw new Error("event not found");
  openItem(detail.item, options);
  return detail.item;
}
