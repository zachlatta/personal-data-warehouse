// The unified timeline ledger: a filter rail (priority, source, kind, sync
// horizon), an infinite-scroll ledger grouped by day, and the inspector drawer.
import { timeline } from "./api.js";
import { el, h, clear, hue, fmtTime, dayOf, WEEKDAYS } from "./ui.js";
import { openItem, openRef, closeDrawer } from "./inspector.js";

export function mount(container, ctx) {
  container.innerHTML = `
    <div id="tl">
      <div id="rail">
        <div class="rail-h">Priority <span class="clear" id="clearsrc">Reset</span></div>
        <div id="prioritylist"></div>
        <div class="rail-h">Sources</div>
        <div id="srclist"></div>
        <div class="rail-h">Kinds</div>
        <div id="kindlist"></div>
        <div class="rail-h">Sync horizon</div>
        <div id="synclist"></div>
      </div>
      <div id="main">
        <div id="ledger">
          <div id="rows"></div>
          <div id="status"></div>
          <div id="sentinel"></div>
        </div>
      </div>
    </div>`;

  const controls = h("div", "ctl");
  const jumpLabel = h("label", "", "Jump to");
  const jump = h("input"); jump.type = "date"; jump.id = "jump";
  const latest = h("button", "", "Latest"); latest.type = "button";
  controls.appendChild(jumpLabel); controls.appendChild(jump); controls.appendChild(latest);
  ctx.setControls(controls);

  const state = {
    cursor: "", exhausted: false, loading: false,
    sources: {}, kinds: {}, priorities: {},
    lastDay: "", selected: null, count: 0, jump: "", dead: false,
    sourcesTimer: null,
  };

  /* ---- filters ---- */
  function activeCSV(map) { return Object.keys(map).filter((key) => map[key]).join(","); }
  function countLabel(count) { return (typeof count === "number" && isFinite(count)) ? count.toLocaleString() : "…"; }
  function renderChips(listNode, catalog, map, colorize) {
    clear(listNode);
    catalog.forEach((entry) => {
      const chip = h("div", "chip" + (map[entry.name] ? " on" : ""));
      const dot = h("span", "dot");
      dot.style.background = colorize ? hue(entry.name) : "var(--border-strong)";
      chip.appendChild(dot);
      chip.appendChild(h("span", "nm", entry.label || entry.name));
      chip.appendChild(h("span", "ct", countLabel(entry.count)));
      chip.addEventListener("click", () => {
        map[entry.name] = !map[entry.name];
        chip.classList.toggle("on");
        resetAndLoad();
      });
      listNode.appendChild(chip);
    });
  }

  function loadSources() {
    return timeline.sources().then((body) => {
      if (state.dead) return;
      renderCounts(body);
      renderSync(body);
    });
  }

  // Counts of common values are Postgres's statistics estimates (estimated:
  // true); rare values are exact. A null count is one the server would not
  // walk the table to find.
  function renderCounts(body) {
    function toCatalog(rows, key) {
      const list = (rows || []).map((row) => ({
        name: row[key],
        count: (typeof row.count === "number" && isFinite(row.count)) ? row.count : null,
      }));
      list.sort((a, b) => {
        const ak = typeof a.count === "number", bk = typeof b.count === "number";
        if (ak && bk && a.count !== b.count) return b.count - a.count;
        if (ak !== bk) return ak ? -1 : 1;
        return a.name.localeCompare(b.name);
      });
      return list;
    }
    renderChips(el("srclist"), toCatalog(body.sources, "source"), state.sources, true);
    renderChips(el("kindlist"), toCatalog(body.kinds, "kind"), state.kinds, false);
    const priorityCatalog = (body.priorities || []).map((row) => ({ name: row.priority, label: row.priority, count: row.count }));
    renderChips(el("prioritylist"), priorityCatalog, state.priorities, false);
    const total = typeof body.total === "number" ? body.total : 0;
    const oldest = body.oldest || "", newest = body.newest || "";
    const stats = h("span");
    stats.appendChild(h("b", "", "≈" + total.toLocaleString()));
    stats.appendChild(document.createTextNode(" events · " + (oldest ? oldest.slice(0, 10) : "—") + " → " + (newest ? newest.slice(0, 10) : "—")));
    ctx.setStats(stats);
  }

  function renderSync(body) {
    const syncNode = clear(el("synclist"));
    (body.sync || []).forEach((row) => {
      const item = h("div", "sy" + (row.backfill_done ? " done" : ""));
      const head = h("div");
      head.appendChild(h("b", "", row.adapter));
      head.appendChild(document.createTextNode(row.backfill_done ? " · complete" : " · loading ← " + String(row.backfill_cursor_event_ts || "").slice(0, 10)));
      item.appendChild(head);
      if (row.last_error) item.appendChild(h("div", "err", row.last_error.slice(0, 120)));
      const bar = h("div", "bar");
      const fill = h("i");
      fill.style.width = row.backfill_done ? "100%" : "38%";
      bar.appendChild(fill);
      item.appendChild(bar);
      syncNode.appendChild(item);
    });
  }

  el("clearsrc").addEventListener("click", () => {
    state.sources = {}; state.kinds = {}; state.priorities = {};
    loadSources().then(resetAndLoad).catch(resetAndLoad);
  });

  /* ---- ledger ---- */
  function appendItems(items) {
    const rowsNode = el("rows");
    items.forEach((item) => {
      const day = dayOf(item.occurred_at);
      if (day !== state.lastDay) {
        state.lastDay = day;
        const head = h("div", "day");
        head.appendChild(h("span", "d", day));
        let weekday = "";
        try { weekday = WEEKDAYS[new Date(item.occurred_at).getDay()]; } catch (e) { /* unknown day */ }
        head.appendChild(h("span", "w", weekday));
        rowsNode.appendChild(head);
      }
      rowsNode.appendChild(rowNode(item, (row) => {
        if (state.selected) state.selected.classList.remove("sel");
        state.selected = row;
        row.classList.add("sel");
        show(item);
      }));
    });
    state.count += items.length;
  }

  function setStatus(text, spinning) {
    const node = clear(el("status"));
    if (spinning) node.appendChild(h("span", "spin", "◴"));
    node.appendChild(document.createTextNode((spinning ? " " : "") + text));
  }

  function loadPage() {
    if (state.loading || state.exhausted || state.dead) return;
    state.loading = true;
    setStatus("reading the ledger…", true);
    const params = {
      before: state.cursor,
      sources: activeCSV(state.sources),
      kinds: activeCSV(state.kinds),
      priorities: activeCSV(state.priorities),
      limit: 80,
    };
    // Default the first page to now so the ledger opens at the present moment;
    // future items (upcoming calendar events) are reachable via jump.
    if (!state.cursor) params.jump = state.jump || new Date().toISOString();
    timeline.list(params).then((body) => {
      if (state.dead) return;
      appendItems(body.items || []);
      if (body.next_cursor) state.cursor = body.next_cursor; else state.exhausted = true;
      state.loading = false;
      setStatus(state.exhausted
        ? "— end of the loaded timeline · " + state.count.toLocaleString() + " events shown —"
        : state.count.toLocaleString() + " events shown · scroll for older");
      maybeFill();
    }).catch((err) => {
      state.loading = false;
      if (err.message !== "unauthorized") setStatus("error: " + err.message, false);
    });
  }

  function maybeFill() {
    const main = el("main");
    if (!state.exhausted && main.scrollHeight <= main.clientHeight + 200) loadPage();
  }

  function resetAndLoad() {
    clear(el("rows"));
    state.cursor = ""; state.exhausted = false; state.lastDay = ""; state.count = 0;
    el("main").scrollTop = 0;
    loadPage();
  }

  const observer = new IntersectionObserver((entries) => {
    entries.forEach((entry) => { if (entry.isIntersecting) loadPage(); });
  }, { root: el("main"), rootMargin: "600px" });
  observer.observe(el("sentinel"));

  jump.addEventListener("change", () => {
    // The picker gives a plain date; jump to that day's local end-of-day so
    // the page opens with the whole selected day visible.
    const picked = jump.value;
    state.jump = picked ? new Date(picked + "T23:59:59.999").toISOString() : "";
    resetAndLoad();
  });
  latest.addEventListener("click", () => { jump.value = ""; state.jump = ""; resetAndLoad(); });

  /* ---- inspector + deep links ---- */
  function itemPath(item) {
    return "/timeline/" + encodeURIComponent(item.adapter) + "/" + encodeURIComponent(item.event_id);
  }
  const drawerOptions = {
    onOpen(item) { history.replaceState(null, "", itemPath(item)); },
    onClose() {
      if (state.selected) state.selected.classList.remove("sel");
      state.selected = null;
      if (location.pathname.indexOf("/timeline/") === 0) history.replaceState(null, "", "/timeline");
    },
  };
  function show(item) { openItem(item, drawerOptions); }
  function openDeepLink(params) {
    if (!params.adapter || !params.eventId) return;
    openRef(params.adapter, params.eventId, drawerOptions).catch((err) => setStatus("could not open event: " + err.message, false));
  }

  /* ---- boot ---- */
  resetAndLoad();
  loadSources().catch(() => {});
  state.sourcesTimer = setInterval(() => loadSources().catch(() => {}), 120000);
  openDeepLink(ctx.params);

  return {
    update(params) {
      if (params.adapter && params.eventId) openDeepLink(params); else closeDrawer();
    },
    cleanup() {
      state.dead = true;
      observer.disconnect();
      clearInterval(state.sourcesTimer);
    },
  };
}

// rowNode renders one ledger row; shared with the search results list.
export function rowNode(item, onClick) {
  const row = h("div", "row p-" + (item.priority || "unclassified"));
  row.style.setProperty("--src", hue(item.source));
  row.appendChild(h("div", "t", fmtTime(item.occurred_at)));
  row.appendChild(h("div", "tick"));

  const who = h("div", "who");
  who.appendChild(h("div", "src", item.source));
  who.appendChild(h("div", "actor", item.actor || "—"));
  row.appendChild(who);

  const body = h("div", "body");
  const meta = item.metadata || {};
  const flags = [];
  if (meta.deleted) flags.push("DELETED");
  if (meta.tapback) flags.push("TAPBACK");
  if (meta.edited) flags.push("EDITED");
  if (meta.bot) flags.push("BOT");
  if (item.title) body.appendChild(h("div", "title", item.title));
  if (item.snippet && item.snippet !== item.title) body.appendChild(h("div", "snip", item.snippet));
  if (!item.title && !item.snippet) body.appendChild(h("div", "snip", "(" + item.kind + ")"));
  row.appendChild(body);

  const right = h("div", "right");
  right.appendChild(h("div", "ctx", item.context || ""));
  const badges = h("div", "flags");
  badges.appendChild(h("span", "pbadge", item.priority));
  if (flags.length) badges.appendChild(document.createTextNode(" " + flags.join(" · ")));
  if (item.open && item.open.url) {
    const openLink = h("a", "open", "⇗");
    openLink.href = item.open.url; openLink.target = "_blank"; openLink.rel = "noopener";
    openLink.title = "open in " + item.open.label;
    openLink.addEventListener("click", (ev) => ev.stopPropagation());
    badges.appendChild(openLink);
  }
  right.appendChild(badges);
  row.appendChild(right);
  row.addEventListener("click", () => onClick(row));
  return row;
}
