// Hybrid search over the timeline: the app's own `search` tool, the same call
// the phone and the CLI make. Hits open in the inspector drawer.
import { search, splitRef, PRIORITIES } from "./api.js";
import { el, h, clear, hue, fmtTime, dayOf, button } from "./ui.js";
import { openRef, closeDrawer } from "./inspector.js";

const MODES = ["hybrid", "keyword", "exact"];

export function mount(container, ctx) {
  container.innerHTML = `
    <div id="sv">
      <form id="sform">
        <div class="srow">
          <input id="sq" type="search" placeholder="the words the answering record would contain — not the question" autocomplete="off" autofocus>
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
  const priNode = el("spri");
  PRIORITIES.forEach((tier) => {
    const chip = h("span", "chip mini", tier);
    chip.addEventListener("click", () => { state.priorities[tier] = !state.priorities[tier]; chip.classList.toggle("on"); });
    priNode.appendChild(chip);
  });

  const restore = new URLSearchParams(location.search);
  if (restore.get("q")) el("sq").value = restore.get("q");
  if (restore.get("mode") && MODES.includes(restore.get("mode"))) modeSelect.value = restore.get("mode");

  function hitRow(hit, index) {
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
      const ref = splitRef(hit.ref);
      if (!ref) return;
      document.querySelectorAll("#sresults .row.sel").forEach((n) => n.classList.remove("sel"));
      row.classList.add("sel");
      openRef(ref.adapter, ref.eventId, { onClose() { row.classList.remove("sel"); } })
        .catch((err) => { el("shint").textContent = "could not open hit: " + err.message; });
    });
    return row;
  }

  async function run(ev) {
    if (ev) ev.preventDefault();
    const query = el("sq").value.trim();
    if (!query) return;
    const input = { query, mode: modeSelect.value, max_results: Number(el("sdepth").value) || 30 };
    const tiers = PRIORITIES.filter((tier) => state.priorities[tier]);
    if (tiers.length) input.priorities = tiers;
    const sources = el("ssrc").value.split(",").map((s) => s.trim()).filter(Boolean);
    if (sources.length) input.sources = sources;
    if (el("ssince").value) input.since = el("ssince").value;
    history.replaceState(null, "", "/search?" + new URLSearchParams({ q: query, mode: input.mode }).toString());
    const results = clear(el("sresults"));
    const hint = clear(el("shint"));
    hint.appendChild(h("span", "spin", "◴")); hint.appendChild(document.createTextNode(" searching…"));
    const started = performance.now();
    try {
      const data = await search.run(input);
      if (state.dead) return;
      clear(hint);
      const ms = Math.round(performance.now() - started);
      ctx.setStats(data.total_rows + " hits · " + data.mode + " · " + ms + "ms");
      if (data.error) hint.appendChild(h("div", "bad", data.error));
      if (data.fallback_reason) hint.appendChild(h("div", "m", "fell back: " + data.fallback_reason));
      if (data.hint) hint.appendChild(h("div", "m", data.hint));
      if (!data.rows.length) hint.appendChild(h("div", "m", "no hits — try the words the record would contain, scope by priority, or lower the depth"));
      data.rows.forEach((hit, index) => results.appendChild(hitRow(hit, index)));
    } catch (err) {
      clear(hint);
      if (err.message !== "unauthorized") hint.appendChild(h("div", "bad", "search failed: " + err.message));
    }
  }
  el("sform").addEventListener("submit", run);
  ctx.setControls(button("clear", "", () => { el("sq").value = ""; clear(el("sresults")); clear(el("shint")); ctx.setStats(""); el("sq").focus(); }));
  if (el("sq").value) run();

  return {
    update() {},
    cleanup() { state.dead = true; closeDrawer(); },
  };
}
