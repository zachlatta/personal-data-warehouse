package server

// pipelinesPagePath is the browser route for the freshness dashboard. The
// timeline page links to it, so the constant is shared rather than spelled twice.
const pipelinesPagePath = "/pipelines"

// pipelinesPageHTML is the self-contained freshness dashboard: one row per
// pipeline, worst first, with the per-table detail behind a click. It follows
// the timeline shell's contract — static HTML, no external assets, the shared
// secret asked for once and kept in localStorage, all data from the
// bearer-protected /api/pipelines endpoint. NOTE: the file is a Go raw string
// literal, so the embedded JS deliberately avoids backticks.
const pipelinesPageHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>pdw — pipeline freshness</title>
<style>
:root {
  --bg: #0e1116; --bg2: #12161d; --surface: #161b23; --surface2: #1b212b;
  --line: #252c37; --line2: #2f3844;
  --text: #d8dde5; --dim: #8b94a1; --faint: #5d6673;
  --amber: #e8b45a; --amber-dim: #a97f35;
  --ok: #62c98d; --late: #e8b45a; --stale: #e06c5f; --failing: #ff5f52;
  --attention: #e8975a; --manual: #7fa8d0; --nodata: #5d6673; --unknown: #b78ae8;
  --mono: "Berkeley Mono", "JetBrains Mono", "IBM Plex Mono", "SF Mono", Menlo, Consolas, monospace;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
html { height: 100%; }
body {
  min-height: 100%; background: var(--bg); color: var(--text);
  font: 13px/1.45 var(--mono);
  background-image: repeating-linear-gradient(0deg, rgba(255,255,255,.012) 0 1px, transparent 1px 3px);
}
::selection { background: rgba(232,180,90,.25); }
::-webkit-scrollbar { width: 10px; height: 10px; }
::-webkit-scrollbar-thumb { background: var(--line2); border-radius: 5px; border: 2px solid var(--bg); }
a { color: var(--amber); text-decoration: none; }
a:hover { text-decoration: underline; }

#topbar {
  position: sticky; top: 0; z-index: 30; height: 46px; display: flex; align-items: center; gap: 14px;
  border-bottom: 1px solid var(--line); background: var(--bg2); padding: 0 14px;
}
#wordmark { display: flex; align-items: baseline; gap: 9px; white-space: nowrap; }
#wordmark .glyph { color: var(--amber); font-weight: 700; }
#wordmark .name { letter-spacing: .18em; font-weight: 700; font-size: 12px; }
#wordmark .sub { color: var(--faint); font-size: 11px; letter-spacing: .08em; }
#spacer { flex: 1; }
#snapshot { color: var(--dim); font-size: 11px; letter-spacing: .04em; white-space: nowrap; }
#snapshot b { color: var(--amber); font-weight: 600; }
button {
  background: var(--surface); color: var(--dim); border: 1px solid var(--line2);
  border-radius: 3px; padding: 4px 10px; font: 11px var(--mono); letter-spacing: .06em; cursor: pointer;
}
button:hover { color: var(--text); border-color: var(--amber-dim); }
button.primary { color: #10131a; background: var(--amber); border-color: var(--amber); font-weight: 700; }
button.on { color: var(--text); border-color: var(--amber); }
input[type="password"] {
  background: var(--surface); color: var(--text); border: 1px solid var(--line2);
  border-radius: 3px; padding: 4px 7px; font: 12px var(--mono);
}

#page { max-width: 1180px; margin: 0 auto; padding: 16px 16px 90px; }

/* ---- status tiles ---- */
#tiles { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 18px; }
.tile {
  flex: 1 1 116px; background: var(--surface); border: 1px solid var(--line);
  border-left: 3px solid var(--line2); border-radius: 4px; padding: 9px 11px; cursor: pointer;
}
.tile:hover { background: var(--surface2); }
.tile.on { border-color: var(--line2); background: var(--surface2); box-shadow: inset 0 0 0 1px var(--line2); }
.tile .n { font-size: 21px; font-weight: 700; letter-spacing: .02em; }
.tile .k { color: var(--faint); font-size: 10px; letter-spacing: .16em; text-transform: uppercase; margin-top: 2px; }

/* ---- pipeline groups ---- */
.group { margin-bottom: 22px; }
.group > h2 {
  color: var(--faint); font-size: 10px; letter-spacing: .18em; text-transform: uppercase;
  display: flex; align-items: baseline; gap: 10px; margin-bottom: 6px;
}
.group > h2 .rule { flex: 1; border-top: 1px dashed var(--line2); transform: translateY(-3px); }
.group > h2 .n { color: var(--faint); letter-spacing: .04em; }

.pl {
  border: 1px solid var(--line); border-left: 3px solid var(--line2); border-radius: 4px;
  background: var(--surface); margin-bottom: 5px; cursor: pointer;
}
.pl:hover { background: var(--surface2); }
.pl.open { background: var(--surface2); }
.plhead {
  display: grid; grid-template-columns: 10px 1fr 132px 132px 116px 92px 74px;
  gap: 0 12px; align-items: baseline; padding: 8px 11px;
}
.pl .dot { width: 9px; height: 9px; border-radius: 50%; align-self: center; }
.pl .nm { overflow: hidden; }
.pl .nm .lb { font-size: 13px; font-weight: 600; }
.pl .nm .id { color: var(--faint); font-size: 10.5px; margin-left: 7px; }
.pl .nm .cad { color: var(--dim); font-size: 10.5px; margin-top: 1px; }
.pl .col .l { color: var(--faint); font-size: 9px; letter-spacing: .13em; text-transform: uppercase; }
.pl .col .v { font-size: 12px; white-space: nowrap; }
.pl .col .v.dimv { color: var(--dim); }
.pl .st { font-size: 10px; letter-spacing: .12em; text-transform: uppercase; text-align: right; font-weight: 700; }
.pl .num { text-align: right; }
.bar { grid-column: 2 / -1; height: 3px; background: rgba(255,255,255,.05); border-radius: 2px; margin-top: 2px; overflow: hidden; }
.bar i { display: block; height: 100%; }
.plerr {
  color: var(--failing); font-size: 11px; padding: 0 11px 8px 33px;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.plnote { color: var(--faint); font-size: 10.5px; padding: 0 11px 8px 33px; }

/* ---- expanded detail ---- */
.detail { border-top: 1px solid var(--line); padding: 9px 11px 11px 33px; cursor: default; }
.detail .meta { color: var(--dim); font-size: 11px; margin-bottom: 8px; }
.detail .meta b { color: var(--faint); font-weight: 400; letter-spacing: .1em; text-transform: uppercase; font-size: 9px; }
table.tbl { width: 100%; border-collapse: collapse; }
table.tbl th {
  color: var(--faint); font-size: 9px; letter-spacing: .13em; text-transform: uppercase;
  text-align: left; font-weight: 400; padding: 3px 8px 3px 0; border-bottom: 1px solid var(--line);
}
table.tbl td { padding: 3px 8px 3px 0; font-size: 11.5px; border-bottom: 1px dotted var(--line); vertical-align: top; }
table.tbl td.n { text-align: right; }
table.tbl td.rel { color: var(--dim); }
table.tbl tr.support td, table.tbl tr.state td { color: var(--dim); }
.rolechip {
  font-size: 8.5px; letter-spacing: .1em; text-transform: uppercase; border: 1px solid var(--line2);
  border-radius: 2px; padding: 0 4px; color: var(--faint);
}
.probe { font-size: 10px; color: var(--faint); }
.probe.warn { color: var(--late); }
.probe.bad { color: var(--stale); }

#status { color: var(--faint); font-size: 11px; padding: 20px 2px; letter-spacing: .06em; }
#legend { color: var(--faint); font-size: 10.5px; line-height: 1.7; border-top: 1px dashed var(--line2); padding-top: 12px; }
#legend b { color: var(--dim); font-weight: 600; }

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
#gate .bad { color: var(--stale); font-size: 11px; margin-top: 10px; display: none; }

@media (max-width: 900px) {
  .plhead { grid-template-columns: 10px 1fr 110px 74px; }
  .pl .col.hide, .pl .num.hide { display: none; }
}
</style>
</head>
<body>
<div id="topbar">
  <div id="wordmark"><span class="glyph">◍</span><span class="name">PDW/PIPELINES</span><span class="sub">what is still arriving</span></div>
  <div id="spacer"></div>
  <div id="snapshot">—</div>
  <button id="attention">needs attention</button>
  <button id="reload">reload</button>
  <a href="/timeline"><button>▤ timeline</button></a>
  <button id="lock" title="change access token">⌁ token</button>
</div>

<div id="page">
  <div id="tiles"></div>
  <div id="groups"></div>
  <div id="marts"></div>
  <div id="adapters"></div>
  <div id="backups"></div>
  <div id="search"></div>
  <div id="benchmark"></div>
  <div id="priority"></div>
  <div id="agents"></div>
  <div id="slack"></div>
  <div id="plaid"></div>
  <div id="collation"></div>
  <div id="status"></div>
  <div id="legend"></div>
</div>

<div id="gate">
  <div class="card">
    <h2>◍ PDW/PIPELINES</h2>
    <p>This dashboard reads the warehouse through the app's bearer-protected API. Paste the app secret token (PDW_SECRET_TOKEN); it stays in this browser's localStorage.</p>
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

  // Worst first: the point of the page is the exceptions, so this order drives
  // both the tiles and the sort inside every group.
  var SEVERITY = [
    "failing", "stale", "attention", "late", "unknown",
    "backfilling", "no_data", "unmeasured", "unmonitored", "manual", "ok"
  ];
  var COLORS = {
    ok: "var(--ok)", late: "var(--late)", stale: "var(--stale)", failing: "var(--failing)",
    attention: "var(--attention)", manual: "var(--manual)", no_data: "var(--nodata)",
    unknown: "var(--unknown)", backfilling: "var(--manual)",
    unmeasured: "var(--nodata)", unmonitored: "var(--nodata)"
  };
  var KINDS = [
    ["source", "sources — data coming in from the outside world"],
    ["enrichment", "enrichment — AI and extraction passes over what arrived"],
    ["derived", "derived — models built from the sources"],
    ["internal", "internal — the warehouse acting on itself"]
  ];
  var STATUS_HELP = {
    ok: "delivering within its expected interval",
    late: "quiet for longer than expected, not yet alarming",
    stale: "quiet for much longer than expected — probably broken",
    failing: "its own sync state records an error",
    attention: "needs a manual step (a re-link, a re-login)",
    manual: "no cadence expected (manual uploads)",
    no_data: "nothing has ever arrived",
    unknown: "the freshness snapshot itself is stale — check the pipeline_health asset",
    backfilling: "still working through its historical backlog",
    unmeasured: "we did not look — the probe was too expensive to afford, which is not the same claim as no data",
    unmonitored: "nothing declares an expectation here, so there is nothing to be late against"
  };
  // The four levels this dashboard covers, worst first inside each. They are
  // deliberately separate surfaces rather than one flat list: a pipeline, a
  // mart, a timeline adapter and a corrupt index each fail for different
  // reasons and are repaired in different places.
  var LEVELS = [
    // Backups first. It is the only level whose failure is unrecoverable, and
    // it was on no surface at all until 2026-08-26, when production ran a day
    // with no valid backup while every other level here read green.
    ["backups", "backups — does a RESTORABLE backup exist, has a restore been VERIFIED, and is WAL still shipping"],
    ["marts", "marts — the read interface, judged on the freshness of what it reads"],
    ["adapters", "timeline adapters — is THIS kind of data reaching timeline.events"],
    ["search", "search — do chunks and embeddings converge with the timeline"],
    ["benchmark", "search benchmark — weekly latency and labeled quality through the search tool"],
    ["priority", "priority tiers — how each source's last seven days split across the catalog-defined attention order"],
    ["agents", "agent usage — do agents start at the timeline and scope by tier (measured from their sessions)"],
    // Per-source detectors. They are here because level 1 AGGREGATES a source
    // into one row, and that is how both of these outages hid: ~19k
    // public-channel messages a day kept Slack ok through a total group-DM
    // outage, and a Plaid re-link that minted a second live Item double-counted
    // net worth while the pipeline stayed green. Both had a SQL detector and
    // neither was on this page, so the page disagreed with the warehouse about
    // what healthy means.
    ["slack", "slack conversations — the SHARE re-listed per type, not the newest stamp"],
    ["plaid", "plaid items — which institution stops updating when it breaks"],
    ["collation", "integrity — collation drift and unique-index divergence"]
  ];

  var state = {
    token: localStorage.getItem("pdw_timeline_token") || "",
    pipelines: [], tables: [], marts: [], adapters: [], search: [], slack: [], plaid: [], collation: [],
    backups: [], priority: [], agents: [], benchmark: [],
    skew: 0, filter: "", attentionOnly: false, open: {}
  };

  // Share the timeline page's token handoff (#token= / ?token=) so one link
  // unlocks either page.
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

  function el(id) { return document.getElementById(id); }
  function h(tag, cls, text) {
    var node = document.createElement(tag);
    if (cls) node.className = cls;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function api(path) {
    return fetch(path, { headers: { "Authorization": "Bearer pipelines-ui:" + state.token } })
      .then(function (resp) {
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

  /* ---- formatting ---- */
  function ago(seconds) {
    if (seconds === null || seconds === undefined) return "—";
    var s = Math.max(0, Math.round(seconds));
    if (s < 60) return s + "s";
    var m = Math.floor(s / 60);
    if (m < 60) return m + "m";
    var hrs = Math.floor(m / 60);
    if (hrs < 48) return hrs + "h " + (m % 60) + "m";
    var d = Math.floor(hrs / 24);
    if (d < 60) return d + "d " + (hrs % 24) + "h";
    return Math.floor(d / 30) + "mo " + (d % 30) + "d";
  }
  // Ages are recomputed against the server's clock (offset measured at load),
  // so a skewed laptop clock cannot invent staleness.
  function ageOf(ts) {
    if (!ts) return null;
    return (Date.now() + state.skew - new Date(ts).getTime()) / 1000;
  }
  function stamp(ts) { return ts ? new Date(ts).toLocaleString() : "never"; }
  function rows(n) {
    if (n === null || n === undefined) return "—";
    if (n >= 1000000) return (n / 1000000).toFixed(n >= 10000000 ? 0 : 1) + "M";
    if (n >= 1000) return Math.round(n / 1000) + "k";
    return String(n);
  }
  function bytes(n) {
    if (!n) return "—";
    var units = ["B", "KiB", "MiB", "GiB", "TiB"], i = 0, v = n;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
    return (v >= 100 || i === 0 ? Math.round(v) : v.toFixed(1)) + " " + units[i];
  }
  // A Postgres text[] arrives from the query runner as the raw array literal
  // "{a,b,c}", not as a JSON array, so anything that treats it as one gets a
  // string whose .length is truthy and whose .join is undefined. That threw
  // inside the marts section and, because load()'s catch swallows it into the
  // status line, silently blanked every section rendered after it.
  function list(value) {
    if (Array.isArray(value)) return value;
    if (typeof value !== "string") return [];
    var body = value.replace(/^\{/, "").replace(/\}$/, "").trim();
    if (!body) return [];
    return body.split(",").map(function (item) {
      return item.replace(/^"/, "").replace(/"$/, "");
    });
  }

  function interval(seconds) {
    if (!seconds) return "no expectation";
    if (seconds < 3600) return "every " + Math.round(seconds / 60) + "m expected";
    if (seconds < 172800) return "every " + Math.round(seconds / 3600) + "h expected";
    return "every " + Math.round(seconds / 86400) + "d expected";
  }

  /* ---- render ---- */
  function visible() {
    return state.pipelines.filter(matchesFilter);
  }

  // Everything the dashboard monitors, across all four levels. The tiles are a
  // roll-up over this rather than over pipelines alone: a corrupt unique index
  // and a frozen timeline adapter are exceptions too, and burying them under a
  // pipelines-only count is how the page stops being the place you look.
  function everything() {
    return state.pipelines
      .concat(state.backups)
      .concat(state.marts)
      .concat(state.adapters)
      .concat(state.search)
      .concat(state.slack)
      .concat(state.plaid)
      .concat(state.collation);
  }

  function matchesFilter(row) {
    if (state.attentionOnly &&
        ["ok", "manual", "unmonitored", "unmeasured"].indexOf(row.status) !== -1) return false;
    if (state.filter && row.status !== state.filter) return false;
    return true;
  }

  function bySeverity(a, b) {
    return SEVERITY.indexOf(a.status) - SEVERITY.indexOf(b.status);
  }

  function renderTiles() {
    var counts = {};
    everything().forEach(function (p) { counts[p.status] = (counts[p.status] || 0) + 1; });
    var node = el("tiles");
    node.textContent = "";
    SEVERITY.forEach(function (status) {
      if (!counts[status]) return;
      var tile = h("div", "tile" + (state.filter === status ? " on" : ""));
      tile.style.borderLeftColor = COLORS[status];
      var n = h("div", "n", String(counts[status]));
      n.style.color = COLORS[status];
      tile.appendChild(n);
      tile.appendChild(h("div", "k", status.replace("_", " ")));
      tile.title = STATUS_HELP[status] || "";
      tile.addEventListener("click", function () {
        state.filter = state.filter === status ? "" : status;
        render();
      });
      node.appendChild(tile);
    });
  }

  function column(label, value, dim, title) {
    var col = h("div", "col" + (dim ? " hide" : ""));
    col.appendChild(h("div", "l", label));
    var v = h("div", "v" + (dim ? " dimv" : ""), value);
    if (title) v.title = title;
    col.appendChild(v);
    return col;
  }

  function pipelineNode(p) {
    var wrap = h("div", "pl" + (state.open[p.pipeline] ? " open" : ""));
    wrap.style.borderLeftColor = COLORS[p.status] || "var(--line2)";

    var head = h("div", "plhead");
    var dot = h("div", "dot");
    dot.style.background = COLORS[p.status] || "var(--nodata)";
    head.appendChild(dot);

    var nm = h("div", "nm");
    var line = h("div");
    line.appendChild(h("span", "lb", p.label));
    line.appendChild(h("span", "id", p.pipeline));
    nm.appendChild(line);
    nm.appendChild(h("div", "cad", p.cadence + " · " + interval(p.expected_data_interval_seconds)));
    head.appendChild(nm);

    var dataAge = ageOf(p.last_write_at);
    head.appendChild(column("last data", p.last_write_at ? ago(dataAge) + " ago" : "never",
      false, stamp(p.last_write_at)));
    // The newest real-world event, and — since 2026-08-23 — its verdict. This
    // column was rendered and never judged, which is how a green dot sat beside
    // an event 118 days old.
    var eventCol = column("newest event",
      p.newest_event_at ? ago(ageOf(p.newest_event_at)) + " ago" : "—",
      true,
      (p.newest_event_at ? stamp(p.newest_event_at) + " · " : "") +
        "event freshness: " + (p.event_status || "—") + " · " +
        (STATUS_HELP[p.event_status] || ""));
    if (p.event_status === "late" || p.event_status === "stale") {
      eventCol.querySelector(".v").style.color = COLORS[p.event_status];
    }
    head.appendChild(eventCol);
    var runAge = ageOf(p.last_run_at);
    head.appendChild(column("last run", p.last_run_at ? ago(runAge) + " ago" : "no heartbeat",
      true, p.last_run_at ? stamp(p.last_run_at) : "this pipeline keeps no run state in the warehouse"));

    var num = h("div", "col num hide");
    num.appendChild(h("div", "l", "rows"));
    num.appendChild(h("div", "v", rows(p.row_estimate)));
    head.appendChild(num);

    var st = h("div", "st", p.status.replace("_", " "));
    st.style.color = COLORS[p.status];
    st.title = STATUS_HELP[p.status] || "";
    head.appendChild(st);
    wrap.appendChild(head);

    // Freshness bar: how far through its own tolerance this pipeline has run.
    var expected = p.expected_data_interval_seconds;
    if (expected && dataAge !== null) {
      var bar = h("div", "bar");
      var fill = h("i");
      fill.style.width = Math.min(100, Math.max(2, (dataAge / (expected * 6)) * 100)) + "%";
      fill.style.background = COLORS[p.status];
      bar.appendChild(fill);
      var barWrap = h("div", "plhead");
      barWrap.style.padding = "0 11px 7px";
      barWrap.appendChild(h("div"));
      barWrap.appendChild(bar);
      wrap.appendChild(barWrap);
    }

    // A recorded error and a failing count are not the same thing: Plaid stamps
    // ITEM_LOGIN_REQUIRED on a scope whose status is action_required, which is a
    // manual step rather than a failure, and an error can outlive the status
    // that produced it.
    var alert = "";
    if (p.state_error_rows) {
      alert = "⚠ " + p.state_error_rows + " failing in " + p.state_table;
    } else if (p.state_attention_rows) {
      alert = "⚠ " + p.state_attention_rows + " scope(s) in " + p.state_table + " need a manual step";
    } else if (p.last_error) {
      alert = "⚠ last error recorded in " + p.state_table;
    }
    if (alert) {
      var err = h("div", "plerr", alert + (p.last_error ? ": " + p.last_error : ""));
      if (p.last_error) err.title = p.last_error;
      wrap.appendChild(err);
    }

    if (state.open[p.pipeline]) wrap.appendChild(detailNode(p));

    head.addEventListener("click", function () {
      state.open[p.pipeline] = !state.open[p.pipeline];
      render();
    });
    return wrap;
  }

  function detailNode(p) {
    var box = h("div", "detail");
    box.addEventListener("click", function (ev) { ev.stopPropagation(); });

    var meta = h("div", "meta");
    meta.appendChild(h("b", "", "transport "));
    meta.appendChild(document.createTextNode(p.transport));
    if (p.note) {
      meta.appendChild(h("br"));
      meta.appendChild(h("b", "", "note "));
      meta.appendChild(document.createTextNode(p.note));
    }
    if (p.state_table) {
      meta.appendChild(h("br"));
      meta.appendChild(h("b", "", "run state "));
      meta.appendChild(document.createTextNode(
        p.state_table + " · " + p.state_rows + " scope(s) · " +
        (p.expected_run_interval_seconds ? interval(p.expected_run_interval_seconds) : "no cadence expected")
      ));
    }
    // Where the data SLA came from. A month-long interval that nobody can
    // re-derive is a number that rots; seven of them did, and pi could not
    // reach 'late' inside sixty days as a result.
    if (p.data_basis) {
      meta.appendChild(h("br"));
      meta.appendChild(h("b", "", "data sla "));
      meta.appendChild(document.createTextNode(
        interval(p.expected_data_interval_seconds) + " — " + p.data_basis
      ));
    }
    if (p.expected_event_interval_seconds) {
      meta.appendChild(h("br"));
      meta.appendChild(h("b", "", "event sla "));
      meta.appendChild(document.createTextNode(
        interval(p.expected_event_interval_seconds) + " · " + (p.event_status || "—")
      ));
    }
    box.appendChild(meta);

    var table = h("table", "tbl");
    var head = h("tr");
    ["table", "", "last write", "newest event", "rows", "size", "probe"].forEach(function (label) {
      head.appendChild(h("th", "", label));
    });
    table.appendChild(head);
    state.tables.filter(function (t) { return t.pipeline === p.pipeline; }).forEach(function (t) {
      var tr = h("tr", t.role);
      var name = h("td", "", t.table_schema + "." + t.table_name);
      name.title = t.table_id + (t.note ? " — " + t.note : "");
      tr.appendChild(name);
      var role = h("td");
      role.appendChild(h("span", "rolechip", t.role));
      tr.appendChild(role);
      var write = h("td", "rel", t.last_write_at ? ago(ageOf(t.last_write_at)) + " ago" : "—");
      write.title = (t.written_at_column || "no column") + " · " + stamp(t.last_write_at);
      tr.appendChild(write);
      var event = h("td", "rel", t.newest_event_at ? stamp(t.newest_event_at).split(",")[0] : "—");
      event.title = (t.event_at_column || "no column") + " · " + stamp(t.newest_event_at);
      tr.appendChild(event);
      tr.appendChild(h("td", "n", rows(t.row_estimate)));
      tr.appendChild(h("td", "n", bytes(t.byte_size)));
      var probeClass = "probe";
      if (t.probe_status === "skipped_unindexed") probeClass += " warn";
      if (["timeout", "error", "missing"].indexOf(t.probe_status) !== -1) probeClass += " bad";
      var probe = h("td");
      var chip = h("span", probeClass, t.probe_status.replace(/_/g, " "));
      if (t.probe_detail) chip.title = t.probe_detail;
      probe.appendChild(chip);
      tr.appendChild(probe);
      table.appendChild(tr);
    });
    box.appendChild(table);
    return box;
  }

  function renderGroups() {
    var node = el("groups");
    node.textContent = "";
    var shown = visible();
    KINDS.forEach(function (entry) {
      var kind = entry[0];
      var members = shown.filter(function (p) { return p.kind === kind; });
      if (!members.length) return;
      members.sort(function (a, b) {
        var d = SEVERITY.indexOf(a.status) - SEVERITY.indexOf(b.status);
        if (d !== 0) return d;
        return (ageOf(b.last_write_at) || 0) - (ageOf(a.last_write_at) || 0);
      });
      var group = h("div", "group");
      var head = h("h2");
      head.appendChild(h("span", "", entry[1]));
      head.appendChild(h("span", "rule"));
      head.appendChild(h("span", "n", members.length + ""));
      group.appendChild(head);
      members.forEach(function (p) { group.appendChild(pipelineNode(p)); });
      node.appendChild(group);
    });
    if (!shown.length) {
      node.appendChild(h("div", "", state.pipelines.length
        ? "nothing matches that filter."
        : "no snapshot yet — the pipeline_health asset has not run."));
    }
  }

  /* ---- levels 2-4: marts, timeline adapters, integrity ---- */

  // One row renderer for all three, in the same shape as a pipeline row: dot,
  // name, up to three columns, status word, optional detail line. Extending the
  // existing idiom rather than inventing a second one keeps the whole page
  // readable at a glance and scanning worst-first the same way.
  function healthRow(item, name, subtitle, columns, detail) {
    var wrap = h("div", "pl");
    wrap.style.borderLeftColor = COLORS[item.status] || "var(--line2)";
    var head = h("div", "plhead");
    var dot = h("div", "dot");
    dot.style.background = COLORS[item.status] || "var(--nodata)";
    head.appendChild(dot);

    var nm = h("div", "nm");
    nm.appendChild(h("div", "", name));
    if (subtitle) nm.appendChild(h("div", "cad", subtitle));
    head.appendChild(nm);

    columns.forEach(function (col) {
      head.appendChild(column(col[0], col[1], col[3] === true, col[2]));
    });
    while (head.children.length < 6) head.appendChild(h("div", "col hide"));

    var st = h("div", "st", (item.status || "").replace(/_/g, " "));
    st.style.color = COLORS[item.status];
    st.title = STATUS_HELP[item.status] || "";
    head.appendChild(st);
    wrap.appendChild(head);
    if (detail) {
      var line = h("div", "plnote", detail);
      line.title = detail;
      if (item.status === "failing" || item.status === "stale") {
        line.style.color = COLORS[item.status];
      }
      wrap.appendChild(line);
    }
    return wrap;
  }

  function martNode(m) {
    var stalest = m.stalest_pipeline
      ? m.stalest_pipeline + " " + ago(ageOf(m.stalest_pipeline_at)) + " ago"
      : (m.inputs_unmeasured ? "unmeasured" : "—");
    var pipes = list(m.input_pipelines);
    var inputs = list(m.input_tables);
    return healthRow(m,
      m.view_schema + "." + m.view_name,
      m.input_count + " input table(s)" + (pipes.length ? " from " + pipes.join(", ") : "") +
        (inputs.length ? " · " + inputs.join(", ") : ""),
      [
        ["stalest pipeline", stalest, m.stalest_pipeline_expected_seconds
          ? "judged against that pipeline's own " + interval(m.stalest_pipeline_expected_seconds) +
            " — per pipeline, not per table: a pipeline's freshness is already a max() over its" +
            " data tables, so judging one quiet table against the whole interval invents staleness." +
            " marts_ops.table_freshness has the per-table detail."
          : "no expectation to judge against", false],
        ["rows?", m.has_rows ? "yes" : "no", "bounded SELECT 1 ... LIMIT 1", true],
        ["definition", m.first_seen_at ? ago(ageOf(m.first_seen_at)) + " old" : "—",
          "sha256 " + (m.definition_sha256 || "").slice(0, 12) +
          " — a redefinition that drops a source table changes no rows, only this", true]
      ],
      m.probe_status && m.probe_status !== "ok" && m.probe_status !== "empty"
        ? "probe " + m.probe_status.replace(/_/g, " ") + (m.probe_detail ? ": " + m.probe_detail : "")
        : "");
  }

  function adapterNode(a) {
    return healthRow(a,
      a.adapter,
      "backfill " + (a.backfill_done ? "done" : "in progress") +
        " · " + rows(a.backfill_rows) + " backfilled · " + rows(a.incremental_rows) + " incremental",
      [
        ["watermark", a.watermark_ingest_ts ? ago(ageOf(a.watermark_ingest_ts)) + " ago" : "—",
          "the honest signal: how far this adapter has consumed its source", false],
        ["last run", a.last_run_at ? ago(ageOf(a.last_run_at)) + " ago" : "—",
          "only stamped when a batch returned rows, so an idle adapter looks stalled — " +
          "do not alarm on this alone", true]
      ],
      a.last_error || "");
  }

  function collationNode(c) {
    var columns = [];
    if (c.scope === "index") {
      var keys = list(c.key_columns);
      columns.push(["rows", rows(c.heap_rows),
        (c.table_name || "") + (keys.length ? " (" + keys.join(", ") + ")" : ""), false]);
      columns.push(["distinct keys", rows(c.distinct_keys), "", true]);
      columns.push(["amcheck", c.amcheck_status || "unavailable",
        (c.amcheck_detail || "") + (c.amcheck_at ? " · checked " + stamp(c.amcheck_at) : "") +
        (c.amcheck_ms ? " · " + c.amcheck_ms + "ms" : ""), true]);
      columns.push(["excess", String(c.excess_rows || 0),
        c.is_partial ? "counted under the index predicate: " + c.predicate
                     : "no partial predicate", true]);
    } else {
      columns.push(["recorded", c.recorded_version || "none",
        "pg_database.datcollversion / pg_collation.collversion", false]);
      columns.push(["live", c.actual_version || "unknown",
        "what the collation library reports right now", true]);
      columns.push(["indexes", String(c.dependent_indexes || 0),
        "collatable indexes that depend on this collation", true]);
    }
    return healthRow(c, c.object_name,
      c.scope + (c.provider ? " · " + c.provider : ""),
      columns, c.detail || "");
  }

  function searchNode(s) {
    return healthRow(s, s.component,
      s.model ? "model " + s.model : "no active model",
      [
        ["caught up", s.caught_up ? "yes" : "no",
          s.component === "chunks"
            ? "timeline max seq " + s.timeline_max_seq + " · cursor " + s.chunk_cursor_seq
            : "false is an exact proof a backlog remains; counting the whole corpus is deliberately avoided", false],
        ["backlog", s.pending_count === null || s.pending_count === undefined
          ? "exists (bounded)" : rows(s.pending_count),
          "pending_count is exact at convergence; otherwise the bounded worker reports existence without a full scan", true],
        ["last success", s.last_success_at ? ago(ageOf(s.last_success_at)) + " ago" : "never",
          stamp(s.last_success_at), true]
      ], s.last_error || (!s.configured ? "hybrid falls back to keyword: embeddings unconfigured" : ""));
  }

  function priorityMixNode(p) {
    var share = Math.round((p.share_7d || 0) * 1000) / 10;
    return healthRow(p, p.source + " · " + p.priority,
      share + "% of " + rows(p.source_events_7d) + " events this week",
      [
        ["7 days", rows(p.events_7d), "events in this tier over the last seven days", false],
        ["24 hours", rows(p.events_1d), "events in this tier over the last day", true],
        ["newest", p.newest_event_at ? ago(ageOf(p.newest_event_at)) + " ago" : "—",
          stamp(p.newest_event_at), true]
      ],
      p.priority === "unclassified" ? "unclassified is not a tier: an adapter's classification did not run" : "");
  }

  function pct(v) { return v === null || v === undefined ? "—" : Math.round(v * 100) + "%"; }

  function agentUsageNode(a) {
    return healthRow(a, a.source,
      rows(a.pdw_sessions) + " of " + rows(a.sessions) + " sessions used PDW in " + a.window_days + " days",
      [
        ["search first", pct(a.search_first_rate),
          "share of PDW sessions whose FIRST call was a search (target 60%); schema-first " + rows(a.first_schema) + ", sql-first " + rows(a.first_sql), false],
        ["effective priority scope", pct(a.priority_filter_rate),
          rows(a.search_with_priority) + " successful, non-no-op filters (target 40%); attention-only " + rows(a.search_attention_only) + ", including noise/background " + rows(a.search_including_lower_tiers), true],
        ["scope mistakes", rows(a.search_noop_priority + a.search_invalid_or_failed_priority),
          rows(a.search_noop_priority) + " empty/all-five no-ops · " + rows(a.search_invalid_or_failed_priority) + " invalid or failed filters", true],
        ["bulk-hint retry", pct(a.bulk_hint_retry_rate),
          rows(a.bulk_hint_scoped_retries) + " of " + rows(a.bulk_hints_shown) + " hints led to a scoped retry; " + rows(a.bulk_hint_improved_retries) + " improved the returned mix (" + pct(a.bulk_hint_improvement_rate) + ")", true],
        ["base-only SQL", pct(a.sql_base_only_rate),
          rows(a.sql_base_only) + " SQL calls went straight to base_* with no timeline reference", true],
        ["SQL errors", pct(a.sql_error_session_rate),
          "sessions with an undefined-column/table, permission or timeout error (ceiling 10%); timeouts " + rows(a.sql_timeouts), true]
      ],
      a.invented_calls ? rows(a.invented_calls) + " invented pdw commands (pdw query, pdw --version, …)" : "");
  }

  function saturationDetail(b) {
    if (b.saturation === "unmeasured" || !b.saturation) { return "no /proc pressure sample during the probes"; }
    var pct = function (v) { return v === null || v === undefined ? "—" : Number(v).toFixed(1) + "%"; };
    return "io full " + pct(b.io_pressure_full_avg10) + " · cpu some " + pct(b.cpu_pressure_some_avg10) +
      " · load " + (b.load_1m === null || b.load_1m === undefined ? "—" : Number(b.load_1m).toFixed(1)) + " / " + rows(b.cpu_count) + " cores (PSI avg10, worse of start/end)";
  }

  function benchmarkNode(b) {
    return healthRow(b, b.mode + " search",
      rows(b.probe_queries) + " latency probes · " + rows(b.labeled_cases) + " labeled cases",
      [
        ["all-tier p50", b.latency_p50_ms !== null && b.latency_p50_ms !== undefined ? (b.latency_p50_ms / 1000).toFixed(2) + "s" : "—",
          "serial, through the app's search tool; the default remains all tiers and its goal is under 2s", false],
        ["p90", b.latency_p90_ms !== null && b.latency_p90_ms !== undefined ? (b.latency_p90_ms / 1000).toFixed(2) + "s" : "—",
          "max " + (b.latency_max_ms / 1000).toFixed(2) + "s", true],
        ["all-tier MRR", b.labeled_cases ? b.mrr : "—",
          b.labeled_cases ? "hit@1 " + rows(b.hit_at_1) + " · hit@5 " + rows(b.hit_at_5) + " · hit@10 " + rows(b.hit_at_10) + " of " + rows(b.labeled_cases) : "no labels published", true],
        ["attention p50", b.attention_probe_queries ? (b.attention_latency_p50_ms / 1000).toFixed(2) + "s" : "—",
          "paired catalog attention-scope probes; delta from all tiers " + (b.attention_latency_p50_delta_ms === null ? "unavailable" : (b.attention_latency_p50_delta_ms >= 0 ? "+" : "") + (b.attention_latency_p50_delta_ms / 1000).toFixed(2) + "s"), true],
        ["attention recall", b.attention_labeled_cases ? rows(b.attention_found) + "/" + rows(b.attention_labeled_cases) : "—",
          "MRR " + b.attention_mrr + " · lost " + rows(b.attention_recall_lost) + " of " + rows(b.attention_recall_lost + b.attention_recall_retained) + " comparable all-tier finds (" + pct(b.attention_recall_loss_rate) + ") · gained " + rows(b.attention_recall_gained) + " · retained " + rows(b.attention_recall_retained) + " across " + rows(b.attention_comparable_cases) + " successful pairs · all-tier relevant lower-tier hits " + rows(b.all_relevant_lower_tier), true],
        // C6: was the host being used while the probes ran? "idle" is the case
        // to fix first -- slow on a machine doing nothing. io_bound means the
        // disk was the saturated resource (2026-08-28: io full 20%, 38% iowait).
        ["host", b.saturation || "unmeasured", saturationDetail(b), b.saturation === "idle" || b.saturation === "io_bound" || b.saturation === "cpu_bound"],
        ["measured", b.collected_at ? ago(ageOf(b.collected_at)) + " ago" : "never", stamp(b.collected_at), true]
      ],
      b.note || (b.errors ? rows(b.errors) + " searches failed during the run" : ""));
  }

  function backupNode(b) {
    // Two independent facts, because reporting either alone is how this hid:
    // WAL shipped perfectly through the 2026-08-25 outage while no base backup
    // existed, and you cannot restore from WAL alone.
    return healthRow(b, b.stanza,
      b.backup_count ? rows(b.backup_count) + " backups" : "NO VALID BACKUP",
      [
        ["last full", b.last_full_at ? ago(ageOf(b.last_full_at)) + " ago" : "never",
          b.last_backup_label ? "label " + b.last_backup_label : "the repository holds no full backup", false],
        ["WAL shipped", b.last_archived_at ? ago(ageOf(b.last_archived_at)) + " ago" : "never",
          "archiving can be healthy while no base backup exists — that is a real state, not a contradiction", true],
        ["last attempt", b.last_attempt_at ? ago(ageOf(b.last_attempt_at)) + " ago" : "never",
          (b.last_attempt_ok ? "succeeded" : "FAILED") + (b.last_attempt_type ? " (" + b.last_attempt_type + ")" : ""), true],
        // A backup nobody has restored is a hypothesis. The drill is recorded
        // by hand after a restore into a throwaway cluster has been counted;
        // "never" and a stale drill both read attention.
        ["restore verified", b.last_restore_verified_at ? ago(ageOf(b.last_restore_verified_at)) + " ago" : "NEVER",
          b.last_restore_label ? "restored " + b.last_restore_label + " (" + rows(b.last_restore_rows) + " timeline rows)" + (b.last_restore_note ? " — " + b.last_restore_note : "")
            : "record one with personal_data_warehouse.pgbackrest_restore_drill after a restore drill", b.restore_status !== "ok"]
      ], b.last_error || b.repo_message || "");
  }

  function slackConversationNode(s) {
    // refreshed_fraction is THE number. Not max(synced_at): a walk that
    // restarts at page 1 re-stamps the first 200 rows every hour and looks
    // perfect while the tail is months old, which is exactly how 172 group DMs
    // and 1,948 public channels went missing for three months. Not the oldest
    // row either: ~1% were archived upstream and can never be re-listed, so
    // that rule is permanently red.
    var pct = s.refreshed_fraction === null || s.refreshed_fraction === undefined
      ? "unknown" : (Number(s.refreshed_fraction) * 100).toFixed(1) + "%";
    // Discovery is only half of it. A public channel Zach is not in is listed
    // by discovery, backfilled once, and then asked for nothing ever again --
    // the change feed only reports conversations he is in, and coverage drops a
    // channel once its history is complete. 11,488 sat frozen behind a 99.2%
    // re-listed number until 2026-08-27, so the poll share is shown beside it.
    var polled = s.history_polled_fraction === null || s.history_polled_fraction === undefined
      ? "n/a" : (Number(s.history_polled_fraction) * 100).toFixed(1) + "%";
    var landing = s.landing_p95_seconds === null || s.landing_p95_seconds === undefined
      ? (s.expected_landing_p95_seconds ? "no messages in 24h" : "n/a")
      : ago(Number(s.landing_p50_seconds)) + " / " + ago(Number(s.landing_p95_seconds)) +
        " (" + (s.landing_status || "unknown") + ")";
    return healthRow(s, s.conversation_type,
      rows(s.live_count) + " live",
      [
        ["re-listed", pct,
          s.refreshed_count + " of " + s.live_count +
          " re-listed within one discovery cycle — the share, not the newest stamp", false],
        ["re-read", polled,
          s.expected_history_cycle_seconds
            ? s.history_polled_count + " of " + s.live_count +
              " asked for new messages within one sweep cycle — listing a channel is not reading it"
            : "not judged: the change feed reports every conversation Zach is in, so an unpolled quiet one is evidence of nothing",
          false],
        ["discovery", s.discovery_status || "unknown",
          s.last_discovery_at ? "last walk " + ago(ageOf(s.last_discovery_at)) + " ago" : "never walked", true],
        // Neither half above can see a DM that lands an hour after it was
        // written -- measured 2026-08-28, 1:1 DMs p95 62 min and group DMs
        // p50 46 min while both read perfect. Landing is judged for the two DM
        // types only; a channel's landing time is the sweep rotation by design.
        ["landing", landing,
          s.expected_landing_p95_seconds
            ? "p50 / p95 of first_seen_at - event_ts over " + rows(s.landing_sample_24h) +
              " messages written in the last 24h; ok at p95 <= " + ago(Number(s.expected_landing_p95_seconds))
            : "not judged: a channel's landing time is the sweep's rate budget, not a fault",
          !s.expected_landing_p95_seconds],
        ["newest message", s.newest_message_at ? ago(ageOf(s.newest_message_at)) + " ago" : "none",
          "context only: mpim legitimately has zero-message days, so this is never the alarm", true]
      ], "");
  }

  function plaidItemNode(p) {
    return healthRow(p, p.institution_name || p.item_id,
      rows(p.account_count) + " accounts",
      [
        ["synced", p.synced_at ? ago(ageOf(p.synced_at)) + " ago" : "never", stamp(p.synced_at), false],
        ["newest txn", p.newest_transaction_at ? ago(ageOf(p.newest_transaction_at)) + " ago" : "none",
          "Capital One holds only 90 days and never pending rows; that is the bank, not a gap", true],
        ["accounts", p.account_names || "—", "what stops updating when this Item breaks", true]
      ],
      p.error_message || (p.status === "action_required"
        ? "re-run: pdw ingest plaid link  for this institution, then check for a DUPLICATE item"
        : ""));
  }

  function renderSection(id, heading, items, nodeFn, sortFn) {
    var node = el(id);
    node.textContent = "";
    var shown = items.filter(matchesFilter).sort(sortFn || bySeverity);
    if (!items.length) return;
    var group = h("div", "group");
    var head = h("h2");
    head.appendChild(h("span", "", heading));
    head.appendChild(h("span", "rule"));
    head.appendChild(h("span", "n", shown.length + " / " + items.length));
    group.appendChild(head);
    if (!shown.length) {
      group.appendChild(h("div", "plnote", "nothing matches that filter."));
    }
    shown.forEach(function (item) { group.appendChild(nodeFn(item)); });
    node.appendChild(group);
  }

  function renderLevels() {
    renderSection("backups", LEVELS[0][1], state.backups, backupNode);
    renderSection("marts", LEVELS[1][1], state.marts, martNode);
    renderSection("adapters", LEVELS[2][1], state.adapters, adapterNode);
    renderSection("search", LEVELS[3][1], state.search, searchNode);
    renderSection("benchmark", LEVELS[4][1], state.benchmark, benchmarkNode);
    renderSection("priority", LEVELS[5][1], state.priority, priorityMixNode,
      function (a, b) {
        if (a.source !== b.source) return a.source < b.source ? -1 : 1;
        return bySeverity(a, b);
      });
    renderSection("agents", LEVELS[6][1], state.agents, agentUsageNode,
      function (a, b) {
        if ((a.source === "all") !== (b.source === "all")) return a.source === "all" ? -1 : 1;
        return bySeverity(a, b);
      });
    renderSection("slack", LEVELS[7][1], state.slack, slackConversationNode);
    renderSection("plaid", LEVELS[8][1], state.plaid, plaidItemNode);
    // The database's own collation row first: it is the finding the other rows
    // corroborate, and it is the one that says this database cannot warn itself.
    renderSection("collation", LEVELS[9][1], state.collation, collationNode,
      function (a, b) {
        if ((a.scope === "database") !== (b.scope === "database")) {
          return a.scope === "database" ? -1 : 1;
        }
        return bySeverity(a, b);
      });
  }

  function renderSnapshot() {
    var newest = null;
    state.pipelines.forEach(function (p) {
      if (p.collected_at && (!newest || p.collected_at > newest)) newest = p.collected_at;
    });
    var node = el("snapshot");
    node.textContent = "";
    if (!newest) {
      node.textContent = "no snapshot";
      return;
    }
    var age = ageOf(newest);
    node.appendChild(document.createTextNode("measured "));
    var b = h("b", "", ago(age) + " ago");
    if (age > 3600) b.style.color = "var(--unknown)";
    node.appendChild(b);
    node.appendChild(document.createTextNode(" · " + state.pipelines.length + " pipelines · " +
      state.tables.length + " tables · " + state.marts.length + " marts · " +
      state.adapters.length + " adapters · " + state.collation.length + " integrity checks"));
    node.title = stamp(newest);
  }

  function renderLegend() {
    var node = el("legend");
    node.textContent = "";
    node.appendChild(h("b", "", "last data"));
    node.appendChild(document.createTextNode(
      " is the newest payload row the pipeline wrote — dimension and cursor tables are excluded so a daily" +
      " directory refresh cannot make a frozen source look healthy. "));
    node.appendChild(h("b", "", "last run"));
    node.appendChild(document.createTextNode(
      " is its heartbeat, read from its sync-state table; uploaders that push from a laptop keep no state here," +
      " so they show no heartbeat and only data freshness applies. Status is computed at read time against each" +
      " pipeline's own expected interval (late past 2x, stale past 6x), so it stays honest even if the collector stops. "));
    node.appendChild(h("b", "", "newest event"));
    node.appendChild(document.createTextNode(
      " is when the newest real-world event this pipeline holds actually happened, which is not the same as when a row" +
      " was written and is now judged on its own interval — the finance ledger dates observations by day, so its event" +
      " time trails its writes while working perfectly, and it says so. "));
    node.appendChild(h("b", "", "probe"));
    node.appendChild(document.createTextNode(
      " says how a table's timestamp was measured; skipped means an unindexed max() over a large heap would have" +
      " cost more than the answer is worth — 'unmeasured' means we did not look, which is a quieter claim than 'no data'."));
    node.appendChild(h("br"));
    node.appendChild(h("b", "", "marts"));
    node.appendChild(document.createTextNode(
      " have no stamped column and no relpages, so they cannot be probed the way a table is: each view is judged on the" +
      " freshness of the stalest relation it reads (inputs resolved from pg_depend, each against its OWN pipeline's" +
      " interval), on a bounded SELECT 1 ... LIMIT 1, and on whether its definition hash changed — a redefinition that" +
      " silently drops a source table changes no rows, only that. "));
    node.appendChild(h("b", "", "adapters"));
    node.appendChild(document.createTextNode(
      " answer whether one KIND of data is reaching timeline.events; the single timeline pipeline row is a max() over all" +
      " of them and hides a frozen one. Note last run is only stamped when a batch returned rows, so an idle adapter" +
      " looks stalled — the watermark is the honest signal. "));
    node.appendChild(h("b", "", "integrity"));
    node.appendChild(document.createTextNode(
      " is collation drift. This database has NO recorded baseline (pg_database.datcollversion is NULL) and REFRESH" +
      " COLLATION VERSION refuses to create one from NULL, so Postgres can never raise its own mismatch warning and this" +
      " is the only cover; the live library version is stored each run so a future change is visible by comparison. Only" +
      " collations an index actually depends on are shown. The duplicate-key probe applies each index's partial predicate" +
      " and is corroboration only: it cannot see a mis-ordered index that has no duplicates, and three of the seven" +
      " indexes damaged on 2026-08-23 were exactly that — the scheduled amcheck status on every index is the rigorous signal."));
    node.appendChild(h("br"));
    node.appendChild(document.createTextNode(
      "Everything here is queryable at parity: marts_ops.pipeline_health, marts_ops.table_freshness," +
      " marts_ops.mart_view_health, marts_ops.timeline_adapter_health, marts_ops.search_health, marts_ops.timeline_priority_mix, marts_ops.agent_usage, marts_ops.search_benchmark," +
      " marts_ops.collation_health."));
  }

  function render() {
    renderTiles();
    renderGroups();
    renderLevels();
    renderSnapshot();
    renderLegend();
  }

  el("attention").addEventListener("click", function () {
    state.attentionOnly = !state.attentionOnly;
    el("attention").classList.toggle("on", state.attentionOnly);
    state.filter = "";
    render();
  });
  el("reload").addEventListener("click", function () { load(); });

  function load() {
    el("status").textContent = "reading the freshness snapshot…";
    return api("/api/pipelines").then(function (body) {
      state.pipelines = body.pipelines || [];
      state.tables = body.tables || [];
      state.marts = body.marts || [];
      state.adapters = body.adapters || [];
      state.backups = body.backups || [];
      state.search = body.search || [];
      state.benchmark = body.benchmark || [];
      state.priority = body.priority || [];
      state.agents = body.agents || [];
      state.slack = body.slack || [];
      state.plaid = body.plaid || [];
      state.collation = body.collation || [];
      state.skew = body.server_now ? new Date(body.server_now).getTime() - Date.now() : 0;
      el("status").textContent = "";
      render();
    }).catch(function (err) {
      if (err.message !== "unauthorized") el("status").textContent = "error: " + err.message;
    });
  }

  function boot() {
    if (!state.token) { openGate(false); return; }
    load();
    // The snapshot refreshes every ten minutes; re-poll on the same order of
    // magnitude and let the rendered ages tick over in between.
    if (!boot.timer) {
      boot.timer = setInterval(function () { load(); }, 120000);
      setInterval(function () { if (state.pipelines.length) render(); }, 30000);
    }
  }
  boot();
})();
</script>
</body>
</html>
`
