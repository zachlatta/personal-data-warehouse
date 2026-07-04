package server

// timelinePageHTML is the self-contained browser UI for the unified timeline.
// It is a static shell (no data is rendered server-side): the JS asks for the
// shared secret once, keeps it in localStorage, and calls the bearer-protected
// /api/timeline endpoints. No external assets — the page must work with the
// tightest CSP and offline against a local app build. NOTE: the file is a Go
// raw string literal, so the embedded JS deliberately avoids backticks.
const timelinePageHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>pdw — unified timeline</title>
<style>
:root {
  --bg: #0e1116; --bg2: #12161d; --surface: #161b23; --surface2: #1b212b;
  --line: #252c37; --line2: #2f3844;
  --text: #d8dde5; --dim: #8b94a1; --faint: #5d6673;
  --amber: #e8b45a; --amber-dim: #a97f35;
  --red: #e06c5f;
  --mono: "Berkeley Mono", "JetBrains Mono", "IBM Plex Mono", "SF Mono", Menlo, Consolas, monospace;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
html { height: 100%; }
body {
  height: 100%; background: var(--bg); color: var(--text);
  font: 13px/1.45 var(--mono);
  background-image:
    repeating-linear-gradient(0deg, rgba(255,255,255,.012) 0 1px, transparent 1px 3px);
}
::selection { background: rgba(232,180,90,.25); }
::-webkit-scrollbar { width: 10px; height: 10px; }
::-webkit-scrollbar-thumb { background: var(--line2); border-radius: 5px; border: 2px solid var(--bg); }
::-webkit-scrollbar-track { background: transparent; }

#app { display: grid; grid-template-columns: 246px 1fr; grid-template-rows: 46px 1fr; height: 100%; }

/* ---- top bar ---- */
#topbar {
  grid-column: 1 / 3; display: flex; align-items: center; gap: 14px;
  border-bottom: 1px solid var(--line); background: var(--bg2); padding: 0 14px; z-index: 30;
}
#wordmark { display: flex; align-items: baseline; gap: 9px; white-space: nowrap; }
#wordmark .glyph { color: var(--amber); font-weight: 700; }
#wordmark .name { letter-spacing: .18em; font-weight: 700; font-size: 12px; }
#wordmark .sub { color: var(--faint); font-size: 11px; letter-spacing: .08em; }
#stats { color: var(--dim); font-size: 11px; letter-spacing: .05em; white-space: nowrap; }
#stats b { color: var(--amber); font-weight: 600; }
#spacer { flex: 1; }
.ctl { display: flex; align-items: center; gap: 6px; }
.ctl label { color: var(--faint); font-size: 10px; letter-spacing: .12em; text-transform: uppercase; }
input[type="date"], input[type="password"] {
  background: var(--surface); color: var(--text); border: 1px solid var(--line2);
  border-radius: 3px; padding: 4px 7px; font: 12px var(--mono); color-scheme: dark;
}
input:focus { outline: 1px solid var(--amber-dim); }
button {
  background: var(--surface); color: var(--dim); border: 1px solid var(--line2);
  border-radius: 3px; padding: 4px 10px; font: 11px var(--mono); letter-spacing: .06em;
  cursor: pointer;
}
button:hover { color: var(--text); border-color: var(--amber-dim); }
button.primary { color: #10131a; background: var(--amber); border-color: var(--amber); font-weight: 700; }

/* ---- left rail ---- */
#rail {
  border-right: 1px solid var(--line); background: var(--bg2); overflow-y: auto; padding: 12px 10px 30px;
}
.rail-h {
  color: var(--faint); font-size: 10px; letter-spacing: .16em; text-transform: uppercase;
  margin: 14px 4px 6px; display: flex; justify-content: space-between; align-items: baseline;
}
.rail-h:first-child { margin-top: 2px; }
.rail-h .clear { cursor: pointer; color: var(--amber-dim); letter-spacing: .05em; text-transform: none; }
.rail-h .clear:hover { color: var(--amber); }
.chip {
  display: flex; align-items: center; gap: 8px; width: 100%;
  padding: 5px 7px; border-radius: 3px; cursor: pointer; user-select: none;
  color: var(--dim); border: 1px solid transparent;
}
.chip:hover { background: var(--surface); color: var(--text); }
.chip.on { background: var(--surface2); color: var(--text); border-color: var(--line2); }
.chip .dot { width: 8px; height: 8px; border-radius: 2px; flex: none; opacity: .9; }
.chip .nm { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 12px; }
.chip .ct { color: var(--faint); font-size: 10px; }
.chip.on .ct { color: var(--dim); }
#synclist .sy { padding: 5px 7px; font-size: 10px; color: var(--faint); line-height: 1.5; }
#synclist .sy b { color: var(--dim); font-weight: 600; }
#synclist .sy .bar { height: 3px; background: var(--surface2); border-radius: 2px; margin-top: 3px; overflow: hidden; }
#synclist .sy .bar i { display: block; height: 100%; background: var(--amber-dim); }
#synclist .sy.done .bar i { background: #4f7d5c; }
#synclist .sy .err { color: var(--red); }

/* ---- ledger ---- */
#main { position: relative; overflow-y: auto; overscroll-behavior: contain; }
#ledger { max-width: 1060px; padding: 0 18px 120px; }
.day {
  position: sticky; top: 0; z-index: 10; display: flex; align-items: baseline; gap: 10px;
  padding: 9px 0 5px; background: linear-gradient(var(--bg) 78%, transparent);
}
.day .d { color: var(--amber); font-weight: 700; letter-spacing: .1em; font-size: 12px; }
.day .w { color: var(--faint); font-size: 10px; letter-spacing: .18em; text-transform: uppercase; }
.day .rule { flex: 1; border-top: 1px dashed var(--line2); transform: translateY(-3px); }
.day .n { color: var(--faint); font-size: 10px; }

.row {
  display: grid; grid-template-columns: 52px 3px 108px 1fr auto; gap: 0 10px;
  padding: 5px 6px 5px 0; border-radius: 3px; cursor: pointer; align-items: baseline;
  content-visibility: auto; contain-intrinsic-size: auto 46px;
  animation: rowin .18s ease-out both;
}
@keyframes rowin { from { opacity: 0; transform: translateY(3px); } to { opacity: 1; transform: none; } }
.row:hover { background: var(--surface); }
.row.sel { background: var(--surface2); outline: 1px solid var(--line2); }
.row .t { color: var(--faint); font-size: 11px; text-align: right; }
.row .tick { align-self: stretch; border-radius: 1px; opacity: .85; min-height: 30px; }
.row .who { overflow: hidden; }
.row .src { font-size: 9px; letter-spacing: .14em; text-transform: uppercase; opacity: .95; }
.row .actor { color: var(--dim); font-size: 11px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.row .body { overflow: hidden; min-width: 0; }
.row .title { font-size: 12.5px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.row .snip { color: var(--dim); font-size: 11.5px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.row .ctx { color: var(--faint); font-size: 10px; max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.row .flags { color: var(--faint); font-size: 9px; letter-spacing: .08em; }

#sentinel { height: 60px; }
#status { padding: 18px 6px; color: var(--faint); font-size: 11px; letter-spacing: .08em; }
.spin { display: inline-block; animation: spin 1s steps(8) infinite; }
@keyframes spin { to { transform: rotate(360deg); } }

/* ---- inspector drawer ---- */
#drawer {
  position: fixed; top: 46px; right: 0; bottom: 0; width: min(620px, 92vw); z-index: 40;
  background: var(--bg2); border-left: 1px solid var(--line2);
  transform: translateX(102%); transition: transform .22s cubic-bezier(.2,.8,.2,1);
  display: flex; flex-direction: column; box-shadow: -18px 0 40px rgba(0,0,0,.45);
}
#drawer.open { transform: none; }
#dhead {
  display: flex; align-items: center; gap: 10px; padding: 10px 14px;
  border-bottom: 1px solid var(--line); background: var(--surface);
}
#dhead .k { font-size: 9px; letter-spacing: .14em; text-transform: uppercase; padding: 2px 7px; border-radius: 2px; color: #10131a; font-weight: 700; }
#dhead .t { flex: 1; font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
#dbody { overflow-y: auto; padding: 14px; flex: 1; }
.sect { margin-bottom: 18px; }
.sect > h3 {
  color: var(--faint); font-size: 10px; letter-spacing: .16em; text-transform: uppercase;
  border-bottom: 1px solid var(--line); padding-bottom: 4px; margin-bottom: 8px;
  display: flex; justify-content: space-between;
}
.sect .copy { cursor: pointer; color: var(--amber-dim); text-transform: none; letter-spacing: .04em; }
.sect .copy:hover { color: var(--amber); }
table.kv { width: 100%; border-collapse: collapse; }
table.kv td { padding: 3px 8px 3px 0; vertical-align: top; font-size: 12px; }
table.kv td.k { color: var(--faint); white-space: nowrap; width: 130px; }
table.kv td.v { color: var(--text); word-break: break-word; }
pre.blob {
  background: var(--bg); border: 1px solid var(--line); border-radius: 4px;
  padding: 10px; font-size: 11px; line-height: 1.5; overflow-x: auto;
  white-space: pre-wrap; word-break: break-word; max-height: 420px; overflow-y: auto;
}
.childrow { border-top: 1px dotted var(--line); padding: 6px 0; font-size: 11.5px; }
.childrow:first-of-type { border-top: 0; }
.childrow .m { color: var(--faint); font-size: 10px; }
.bigtext { white-space: pre-wrap; word-break: break-word; color: var(--dim); font-size: 11.5px; }
#dclose { margin-left: auto; }

/* ---- token overlay ---- */
#gate {
  position: fixed; inset: 0; z-index: 100; display: none; align-items: center; justify-content: center;
  background: rgba(10,12,16,.92); backdrop-filter: blur(3px);
}
#gate.open { display: flex; }
#gate .card {
  width: 420px; background: var(--surface); border: 1px solid var(--line2); border-radius: 6px;
  padding: 26px; box-shadow: 0 30px 80px rgba(0,0,0,.6);
}
#gate h2 { font-size: 13px; letter-spacing: .16em; margin-bottom: 6px; color: var(--amber); }
#gate p { color: var(--dim); font-size: 11.5px; margin-bottom: 14px; }
#gate form { display: flex; gap: 8px; }
#gate input { flex: 1; }
#gate .bad { color: var(--red); font-size: 11px; margin-top: 10px; display: none; }

@media (max-width: 860px) {
  #app { grid-template-columns: 0 1fr; }
  #rail { display: none; }
}
</style>
</head>
<body>
<div id="app">
  <div id="topbar">
    <div id="wordmark"><span class="glyph">▤</span><span class="name">PDW/TIMELINE</span><span class="sub">every recorded thing, in order</span></div>
    <div id="stats">—</div>
    <div id="spacer"></div>
    <div class="ctl"><label>jump</label><input type="date" id="jump"></div>
    <button id="latest">latest</button>
    <button id="lock" title="change access token">⌁ token</button>
  </div>
  <div id="rail">
    <div class="rail-h">sources <span class="clear" id="clearsrc">reset</span></div>
    <div id="srclist"></div>
    <div class="rail-h">kinds</div>
    <div id="kindlist"></div>
    <div class="rail-h">sync horizon</div>
    <div id="synclist"></div>
  </div>
  <div id="main">
    <div id="ledger">
      <div id="rows"></div>
      <div id="status"></div>
      <div id="sentinel"></div>
    </div>
  </div>
</div>

<div id="drawer">
  <div id="dhead">
    <span class="k" id="dkind">—</span>
    <span class="t" id="dtitle">—</span>
    <button id="dclose">esc ✕</button>
  </div>
  <div id="dbody"></div>
</div>

<div id="gate">
  <div class="card">
    <h2>▤ PDW/TIMELINE</h2>
    <p>This inspector reads the warehouse through the app's bearer-protected API. Paste the app secret token (PDW_SECRET_TOKEN); it stays in this browser's localStorage.</p>
    <form id="gateform">
      <input type="password" id="gatetoken" placeholder="secret token" autocomplete="off">
      <button class="primary" type="submit">unlock</button>
    </form>
    <div class="bad" id="gatebad">rejected — check the token and try again.</div>
  </div>
</div>

<script>
(function () {
  "use strict";

  var HUES = {
    gmail: "#e06c5f", slack: "#5fb0e0", apple_messages: "#62c98d", whatsapp: "#45c07a",
    apple_notes: "#e8975a", voice_memos: "#e05f9a", calendar: "#e8b45a",
    google_drive: "#d9c95f", contacts: "#7fd0c4", mutations: "#c46be0", warehouse: "#8b94a1",
    claude_code: "#b78ae8", codex: "#9a8ae8", openclaw: "#8aa6e8", claude_desktop: "#cf8ae8", chatgpt: "#8ae8c9",
    agent_sessions: "#b78ae8"
  };
  function hue(src) { return HUES[src] || "#8b94a1"; }

  // Accept a one-time token handoff via the URL fragment (#token=...) or, as
  // a fallback for terminals that cut links at the "#", a ?token= query
  // param. Either way it is stored and immediately stripped from the address
  // bar. (The request logger records only the path, not the query string.)
  if (location.hash.indexOf("#token=") === 0) {
    localStorage.setItem("pdw_timeline_token", decodeURIComponent(location.hash.slice(7)));
    history.replaceState(null, "", location.pathname);
  } else if (location.search.indexOf("token=") !== -1) {
    var qtoken = new URLSearchParams(location.search).get("token");
    if (qtoken) {
      localStorage.setItem("pdw_timeline_token", qtoken);
      history.replaceState(null, "", location.pathname);
    }
  }

  var state = {
    token: localStorage.getItem("pdw_timeline_token") || "",
    cursor: "", exhausted: false, loading: false,
    sources: {}, kinds: {}, lastDay: "", selected: null, count: 0
  };

  function el(id) { return document.getElementById(id); }
  function h(tag, cls, text) {
    var node = document.createElement(tag);
    if (cls) node.className = cls;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function api(path, params) {
    var qs = [];
    for (var key in params) if (params[key] !== "" && params[key] != null) qs.push(key + "=" + encodeURIComponent(params[key]));
    var url = path + (qs.length ? "?" + qs.join("&") : "");
    return fetch(url, { headers: { "Authorization": "Bearer timeline-ui:" + state.token } }).then(function (resp) {
      if (resp.status === 401 || resp.status === 403) { openGate(true); throw new Error("unauthorized"); }
      if (!resp.ok) return resp.json().then(function (body) { throw new Error(body.error || resp.statusText); });
      return resp.json();
    });
  }

  /* ---- token gate ---- */
  function openGate(rejected) {
    el("gate").classList.add("open");
    el("gatebad").style.display = rejected && state.token ? "block" : "none";
    el("gatetoken").focus();
  }
  el("gateform").addEventListener("submit", function (ev) {
    ev.preventDefault();
    state.token = el("gatetoken").value.trim();
    localStorage.setItem("pdw_timeline_token", state.token);
    el("gate").classList.remove("open");
    boot();
  });
  el("lock").addEventListener("click", function () { openGate(false); });

  /* ---- filters ---- */
  function activeCSV(map) {
    var on = [];
    for (var key in map) if (map[key]) on.push(key);
    return on.join(",");
  }
  function renderChips(listNode, catalog, map, colorize) {
    listNode.textContent = "";
    catalog.forEach(function (entry) {
      var chip = h("div", "chip" + (map[entry.name] ? " on" : ""));
      var dot = h("span", "dot");
      dot.style.background = colorize ? hue(entry.name) : "var(--line2)";
      chip.appendChild(dot);
      chip.appendChild(h("span", "nm", entry.name));
      chip.appendChild(h("span", "ct", entry.count.toLocaleString()));
      chip.addEventListener("click", function () {
        map[entry.name] = !map[entry.name];
        chip.classList.toggle("on");
        resetAndLoad();
      });
      listNode.appendChild(chip);
    });
  }

  function loadSources() {
    return api("/api/timeline/sources", {}).then(function (body) {
      var bySource = {}, byKind = {}, total = 0, oldest = "", newest = "";
      (body.sources || []).forEach(function (row) {
        total += row.count;
        bySource[row.source] = (bySource[row.source] || 0) + row.count;
        byKind[row.kind] = (byKind[row.kind] || 0) + row.count;
        if (!oldest || row.oldest < oldest) oldest = row.oldest;
        if (!newest || row.newest > newest) newest = row.newest;
      });
      function toCatalog(counts) {
        var list = [];
        for (var key in counts) list.push({ name: key, count: counts[key] });
        list.sort(function (a, b) { return b.count - a.count; });
        return list;
      }
      renderChips(el("srclist"), toCatalog(bySource), state.sources, true);
      renderChips(el("kindlist"), toCatalog(byKind), state.kinds, false);
      el("stats").innerHTML = "<b>" + total.toLocaleString() + "</b> events · " +
        (oldest ? oldest.slice(0, 10) : "—") + " → " + (newest ? newest.slice(0, 10) : "—");

      var syncNode = el("synclist");
      syncNode.textContent = "";
      (body.sync || []).forEach(function (row) {
        var item = h("div", "sy" + (row.backfill_done ? " done" : ""));
        var head = h("div");
        head.appendChild(h("b", "", row.adapter));
        head.appendChild(document.createTextNode(
          row.backfill_done ? " · complete" : " · loading ← " + String(row.backfill_cursor_event_ts || "").slice(0, 10)
        ));
        item.appendChild(head);
        if (row.last_error) item.appendChild(h("div", "err", row.last_error.slice(0, 120)));
        var bar = h("div", "bar");
        var fill = h("i");
        fill.style.width = row.backfill_done ? "100%" : "38%";
        bar.appendChild(fill);
        item.appendChild(bar);
        syncNode.appendChild(item);
      });
    });
  }

  el("clearsrc").addEventListener("click", function () {
    state.sources = {}; state.kinds = {};
    loadSources().then(resetAndLoad);
  });

  /* ---- ledger (all display times are the browser's local timezone) ---- */
  function pad2(n) { return (n < 10 ? "0" : "") + n; }
  function fmtTime(ts) {
    if (!ts) return "—";
    var d = new Date(ts);
    return pad2(d.getHours()) + ":" + pad2(d.getMinutes());
  }
  function dayOf(ts) {
    if (!ts) return "unknown";
    var d = new Date(ts);
    return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate());
  }
  function fmtFull(ts) { return ts ? new Date(ts).toLocaleString() : ""; }
  var WEEKDAYS = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

  function appendItems(items) {
    var rowsNode = el("rows");
    items.forEach(function (item, index) {
      var day = dayOf(item.event_ts);
      if (day !== state.lastDay) {
        state.lastDay = day;
        var head = h("div", "day");
        head.appendChild(h("span", "d", day));
        var weekday = "";
        try { weekday = WEEKDAYS[new Date(item.event_ts).getDay()]; } catch (e) {}
        head.appendChild(h("span", "w", weekday));
        head.appendChild(h("span", "rule"));
        rowsNode.appendChild(head);
      }
      var row = h("div", "row");
      row.style.animationDelay = Math.min(index, 12) * 12 + "ms";
      row.appendChild(h("div", "t", fmtTime(item.event_ts)));
      var tick = h("div", "tick");
      tick.style.background = hue(item.source);
      row.appendChild(tick);

      var who = h("div", "who");
      var src = h("div", "src", item.source);
      src.style.color = hue(item.source);
      who.appendChild(src);
      who.appendChild(h("div", "actor", item.actor || "—"));
      row.appendChild(who);

      var body = h("div", "body");
      var meta = item.metadata || {};
      var flags = [];
      if (meta.deleted) flags.push("DELETED");
      if (meta.tapback) flags.push("TAPBACK");
      if (meta.edited) flags.push("EDITED");
      if (meta.bot) flags.push("BOT");
      if (item.title) body.appendChild(h("div", "title", item.title));
      if (item.snippet && item.snippet !== item.title) body.appendChild(h("div", "snip", item.snippet));
      if (!item.title && !item.snippet) body.appendChild(h("div", "snip", "(" + item.kind + ")"));
      row.appendChild(body);

      var right = h("div");
      right.appendChild(h("div", "ctx", item.context || ""));
      if (flags.length) right.appendChild(h("div", "flags", flags.join(" · ")));
      row.appendChild(right);

      row.addEventListener("click", function () {
        if (state.selected) state.selected.classList.remove("sel");
        state.selected = row;
        row.classList.add("sel");
        openItem(item);
      });
      rowsNode.appendChild(row);
    });
    state.count += items.length;
  }

  function setStatus(text, spinning) {
    var node = el("status");
    node.textContent = "";
    if (spinning) node.appendChild(h("span", "spin", "◴"));
    node.appendChild(document.createTextNode((spinning ? " " : "") + text));
  }

  function loadPage() {
    if (state.loading || state.exhausted) return;
    state.loading = true;
    setStatus("reading the ledger…", true);
    var params = {
      before: state.cursor,
      sources: activeCSV(state.sources),
      kinds: activeCSV(state.kinds),
      limit: 80
    };
    // Default the first page to now so the ledger opens at the present moment;
    // future items (upcoming calendar events) are reachable via jump.
    if (!state.cursor) params.jump = state.jump || new Date().toISOString();
    api("/api/timeline", params).then(function (body) {
      appendItems(body.items || []);
      if (body.next_cursor) { state.cursor = body.next_cursor; } else { state.exhausted = true; }
      state.loading = false;
      setStatus(state.exhausted
        ? "— end of the loaded timeline · " + state.count.toLocaleString() + " events shown —"
        : state.count.toLocaleString() + " events shown · scroll for older");
      maybeFill();
    }).catch(function (err) {
      state.loading = false;
      if (err.message !== "unauthorized") setStatus("error: " + err.message, false);
    });
  }

  function maybeFill() {
    var main = el("main");
    if (!state.exhausted && main.scrollHeight <= main.clientHeight + 200) loadPage();
  }

  function resetAndLoad() {
    el("rows").textContent = "";
    state.cursor = ""; state.exhausted = false; state.lastDay = ""; state.count = 0;
    el("main").scrollTop = 0;
    loadPage();
  }

  var observer = new IntersectionObserver(function (entries) {
    entries.forEach(function (entry) { if (entry.isIntersecting) loadPage(); });
  }, { root: el("main"), rootMargin: "600px" });
  observer.observe(el("sentinel"));

  el("jump").addEventListener("change", function () {
    // The picker gives a plain date; jump to that day's local end-of-day so
    // the page opens with the whole selected day visible.
    var picked = el("jump").value;
    state.jump = picked ? new Date(picked + "T23:59:59.999").toISOString() : "";
    resetAndLoad();
  });
  el("latest").addEventListener("click", function () {
    el("jump").value = ""; state.jump = "";
    resetAndLoad();
  });

  /* ---- inspector ---- */
  function section(title, copyValue) {
    var sect = h("div", "sect");
    var head = h("h3", "", title);
    if (copyValue !== undefined) {
      var copy = h("span", "copy", "copy");
      copy.addEventListener("click", function () {
        navigator.clipboard.writeText(typeof copyValue === "string" ? copyValue : JSON.stringify(copyValue, null, 2));
        copy.textContent = "copied ✓";
        setTimeout(function () { copy.textContent = "copy"; }, 1200);
      });
      head.appendChild(copy);
    }
    sect.appendChild(head);
    return sect;
  }
  function kvTable(pairs) {
    var table = h("table", "kv");
    pairs.forEach(function (pair) {
      if (pair[1] === undefined || pair[1] === null || pair[1] === "") return;
      var tr = h("tr");
      tr.appendChild(h("td", "k", pair[0]));
      var value = pair[1];
      tr.appendChild(h("td", "v", typeof value === "object" ? JSON.stringify(value) : String(value)));
      table.appendChild(tr);
    });
    return table;
  }

  function openItem(item) {
    var drawer = el("drawer");
    drawer.classList.add("open");
    var kindNode = el("dkind");
    kindNode.textContent = item.source + " / " + item.kind;
    kindNode.style.background = hue(item.source);
    el("dtitle").textContent = item.title || item.snippet || item.event_id;
    var body = el("dbody");
    body.textContent = "";

    var head = section("event");
    head.appendChild(kvTable([
      ["when", fmtFull(item.event_ts)], ["until", isReal(item.end_ts) ? fmtFull(item.end_ts) : ""],
      ["actor", item.actor], ["context", item.context],
      ["adapter", item.adapter], ["event id", item.event_id],
      ["seq", item.seq], ["source table", item.source_table]
    ]));
    body.appendChild(head);

    if (item.snippet) {
      var snip = section("preview");
      snip.appendChild(h("div", "bigtext", item.snippet));
      body.appendChild(snip);
    }
    if (item.metadata && Object.keys(item.metadata).length) {
      var meta = section("metadata", item.metadata);
      meta.appendChild(kvTable(Object.keys(item.metadata).map(function (key) { return [key, item.metadata[key]]; })));
      body.appendChild(meta);
    }
    var loading = section("source record");
    loading.appendChild(h("div", "m", "fetching full record…"));
    body.appendChild(loading);

    api("/api/timeline/item", { adapter: item.adapter, event_id: item.event_id }).then(function (detail) {
      body.removeChild(loading);
      var children = detail.children || {};
      Object.keys(children).sort().forEach(function (name) {
        var rows = children[name];
        if (!rows || rows.error || !rows.length) return;
        var sect = section(name.replace(/_/g, " ") + " · " + rows.length, rows);
        rows.forEach(function (childRow) {
          var node = h("div", "childrow");
          var text = childRow.text || childRow.summary || childRow.filename || childRow.name || "";
          var metaBits = [];
          for (var key in childRow) {
            if (key === "text" || key === "summary") continue;
            var val = childRow[key];
            if (val === "" || val === null || val === 0) continue;
            metaBits.push(key + "=" + String(val).slice(0, 60));
          }
          if (text) node.appendChild(h("div", "bigtext", String(text)));
          node.appendChild(h("div", "m", metaBits.join("  ")));
          sect.appendChild(node);
        });
        body.appendChild(sect);
      });
      if (detail.source_row) {
        var raw = section("source row (full record)", detail.source_row);
        var pre = h("pre", "blob", JSON.stringify(detail.source_row, null, 2));
        raw.appendChild(pre);
        body.appendChild(raw);
      } else if (detail.source_row_error) {
        var errSect = section("source row");
        errSect.appendChild(h("div", "m", "unavailable: " + detail.source_row_error));
        body.appendChild(errSect);
      }
    }).catch(function (err) {
      loading.textContent = "detail fetch failed: " + err.message;
    });
  }
  function isReal(ts) { return ts && ts.slice(0, 4) !== "1970"; }

  function closeDrawer() {
    el("drawer").classList.remove("open");
    if (state.selected) state.selected.classList.remove("sel");
    state.selected = null;
  }
  el("dclose").addEventListener("click", closeDrawer);
  document.addEventListener("keydown", function (ev) { if (ev.key === "Escape") closeDrawer(); });

  /* ---- boot ---- */
  function boot() {
    if (!state.token) { openGate(false); return; }
    loadSources().then(resetAndLoad).catch(function () {});
    if (!boot.timer) boot.timer = setInterval(function () { loadSources().catch(function () {}); }, 60000);
  }
  boot();
})();
</script>
</body>
</html>
`
