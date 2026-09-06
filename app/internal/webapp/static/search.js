// Hybrid search over the timeline: the app's own `search` tool, the same call
// the phone and the CLI make. Hits open in the inspector drawer.
import { search, splitRef, PRIORITIES } from "./api.js";
import { el, h, clear, hue, fmtTime, dayOf, button } from "./ui.js";
import { openRef, closeDrawer } from "./inspector.js";
import { MODES, readSearch, searchParams } from "./search_state.js";

export function mount(container, ctx) {
  container.innerHTML = `
    <div id="sv">
      <form id="sform">
        <div class="srow">
          <input id="sq" type="search" placeholder="a few distinctive words the record would contain — a name, an id, an amount" autocomplete="off" autofocus>
          <select id="smode"></select>
          <button class="primary" type="submit">search</button>
        </div>
        <div class="srow sopts">
          <span class="lab">priority</span><span id="spri"></span>
          <span class="lab">sources</span><input id="ssrc" placeholder="gmail, slack, apple_messages…">
          <span class="lab">since</span><input id="ssince" type="date">
          <span class="lab">depth</span><input id="sdepth" type="number" min="1" max="200" value="30">
        </div>
      </form>
      <div id="shint"></div>
      <div id="sresults"></div>
    </div>`;

  const modeSelect = el("smode");
  MODES.forEach((mode) => { const opt = h("option", "", mode); opt.value = mode; modeSelect.appendChild(opt); });
  const state = { priorities: {}, dead: false };
  let generation = 0;
  let restoreError = "";
  let restored;
  try {
    restored = readSearch(new URLSearchParams(location.search));
  } catch (err) {
    restoreError = "invalid search link: " + err.message + " — clear to start a new search";
  }
  if (restored) {
    el("sq").value = restored.query;
    modeSelect.value = restored.mode;
    el("ssrc").value = (restored.sources || []).join(", ");
    el("ssince").value = restored.since || "";
    el("sdepth").value = String(restored.max_results);
    for (const tier of restored.priorities || []) state.priorities[tier] = true;
  }
  const priNode = el("spri");
  PRIORITIES.forEach((tier) => {
    const chip = h("span", "chip mini", tier);
    if (state.priorities[tier]) chip.classList.add("on");
    chip.addEventListener("click", () => { state.priorities[tier] = !state.priorities[tier]; chip.classList.toggle("on"); });
    priNode.appendChild(chip);
  });

  function hitRow(hit, index, isCurrent) {
    const row = h("div", "row p-" + (hit.priority || "unclassified"));
    row.style.animationDelay = Math.min(index, 12) * 12 + "ms";
    const when = hit.event_ts || hit.occurred_at;
    row.appendChild(h("div", "t", dayOf(when).slice(5) + " " + fmtTime(when)));
    const tick = h("div", "tick"); tick.style.background = hue(hit.source); row.appendChild(tick);
    const who = h("div", "who");
    const src = h("div", "src", hit.source + (hit.subsource ? " / " + hit.subsource : "")); src.style.color = hue(hit.source);
    who.appendChild(src); who.appendChild(h("div", "actor", hit.who || "—"));
    row.appendChild(who);
    const body = h("div", "body");
    if (hit.title) body.appendChild(h("div", "title", hit.title));
    body.appendChild(h("div", "snip wrap", hit.text || ""));
    row.appendChild(body);
    const right = h("div", "right");
    right.appendChild(h("div", "ctx", hit.context || ""));
    const badges = h("div", "flags");
    badges.appendChild(h("span", "pbadge", hit.priority || "?"));
    if (typeof hit.score === "number") badges.appendChild(document.createTextNode(" " + hit.score.toFixed(3)));
    right.appendChild(badges);
    row.appendChild(right);
    row.addEventListener("click", () => {
      if (!isCurrent()) return;
      const ref = splitRef(hit.ref);
      if (!ref) return;
      document.querySelectorAll("#sresults .row.sel").forEach((n) => n.classList.remove("sel"));
      row.classList.add("sel");
      openRef(ref.adapter, ref.eventId, { onClose() { if (isCurrent()) row.classList.remove("sel"); } })
        .catch((err) => { if (isCurrent()) el("shint").textContent = "could not open hit: " + err.message; });
    });
    return row;
  }

  async function run(ev) {
    if (ev) ev.preventDefault();
    if (state.dead || restoreError) return;
    // Every async mutation below belongs to this generation, including failures.
    const request = ++generation;
    const isCurrent = () => !state.dead && request === generation;
    const query = el("sq").value.trim();
    if (!query) { clearSearch(); return; }
    const input = { query, mode: modeSelect.value, max_results: Number(el("sdepth").value) };
    const tiers = PRIORITIES.filter((tier) => state.priorities[tier]);
    if (tiers.length) input.priorities = tiers;
    const sources = el("ssrc").value.trim();
    if (sources) input.sources = sources.split(",").map((s) => s.trim());
    if (el("ssince").value) input.since = el("ssince").value;
    const params = searchParams(input);
    try {
      readSearch(params);
    } catch (err) {
      clear(el("sresults"));
      el("shint").textContent = "invalid search: " + err.message;
      ctx.setStats("");
      return;
    }
    history.replaceState(null, "", "/search?" + params);
    ctx.setStats("");
    const results = clear(el("sresults"));
    const hint = clear(el("shint"));
    hint.appendChild(h("span", "spin", "◴")); hint.appendChild(document.createTextNode(" searching…"));
    const started = performance.now();
    try {
      const data = await search.run(input);
      if (!isCurrent()) return;
      clear(hint);
      const ms = Math.round(performance.now() - started);
      const reportedScope = data.priority_scope || (tiers.length ? "selected" : "all");
      const selected = data.selected_priorities || tiers;
      const scope = reportedScope === "all"
        ? "all tiers"
        : (reportedScope === "invalid" ? "invalid: " : "") + selected.join(", ");
      ctx.setStats(data.total_rows + " hits · " + data.mode + " · scope " + scope + " · " + ms + "ms");
      const mix = Object.entries(data.returned_priority_counts || {}).map(([tier, count]) => tier + "=" + count).join(", ");
      if (mix) hint.appendChild(h("div", "m", "returned priorities: " + mix));
      if (data.error) hint.appendChild(h("div", "bad", data.error));
      if (data.fallback_reason) hint.appendChild(h("div", "m", "fell back: " + data.fallback_reason));
      if (data.hint) hint.appendChild(h("div", "m", data.hint));
      if (!data.rows.length) hint.appendChild(h("div", "m", "no hits — try the words the record would contain, scope by priority, or lower the depth"));
      data.rows.forEach((hit, index) => results.appendChild(hitRow(hit, index, isCurrent)));
    } catch (err) {
      if (!isCurrent()) return;
      clear(hint);
      if (err.message !== "unauthorized") hint.appendChild(h("div", "bad", "search failed: " + err.message));
    }
  }
  el("sform").addEventListener("submit", run);
  function clearSearch() {
    ++generation;
    restoreError = "";
    history.replaceState(null, "", "/search");
    el("sq").value = "";
    clear(el("sresults"));
    clear(el("shint"));
    ctx.setStats("");
    el("sq").focus();
  }
  ctx.setControls(button("clear", "", clearSearch));
  if (restoreError) el("shint").textContent = restoreError;
  else if (el("sq").value) run();

  return {
    update() {},
    cleanup() { ++generation; state.dead = true; closeDrawer(); },
  };
}
