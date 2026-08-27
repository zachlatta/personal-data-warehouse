// The SPA shell: a tiny history-API router over the views, plus the token
// gate. Every view is a module exporting mount(container, ctx) that returns a
// cleanup function; ctx gives it the route params, navigation, and the topbar
// slots (controls, stats, subtitle) so views never reach into the shell's DOM.
import { adoptTokenFromLocation, getToken, setToken, probe, mutations } from "./api.js";
import { el, clear } from "./ui.js";
import * as timelineView from "./timeline.js";
import * as searchView from "./search.js";
import * as mutationsView from "./mutations.js";
import { closeDrawer } from "./inspector.js";

const ROUTES = [
  { pattern: /^\/timeline(?:\/([^/]+)\/(.+))?\/?$/, view: timelineView, nav: "timeline", params: (m) => ({ adapter: m[1] ? decodeURIComponent(m[1]) : "", eventId: m[2] ? decodeURIComponent(m[2]) : "" }) },
  { pattern: /^\/search\/?$/, view: searchView, nav: "search", params: () => ({}) },
  { pattern: /^\/mutation-review(?:\/requests\/([^/]+))?\/?$/, view: mutationsView, nav: "mutation-review", params: (m) => ({ id: m[1] ? decodeURIComponent(m[1]) : "" }) },
];

let current = { path: "", cleanup: null, view: null };

function navigate(path, { replace = false } = {}) {
  if (replace) history.replaceState(null, "", path); else history.pushState(null, "", path);
  render();
}

function matchRoute(path) {
  for (const route of ROUTES) {
    const m = route.pattern.exec(path);
    if (m) return { route, params: route.params(m) };
  }
  return null;
}

function render() {
  const path = location.pathname;
  const match = matchRoute(path);
  if (!match) { navigate("/timeline", { replace: true }); return; }
  const { route, params } = match;
  document.querySelectorAll("#nav a[data-nav]").forEach((a) => a.classList.toggle("on", a.getAttribute("data-nav") === route.nav));
  // A route change inside the same view (opening an item, a request) is the
  // view's own business: hand it the new params instead of remounting.
  if (current.view === route.view && current.update) {
    current.update(params);
    return;
  }
  if (current.cleanup) { try { current.cleanup(); } catch (e) { console.error(e); } }
  closeDrawer();
  clear(el("view")); clear(el("topctl")); clear(el("stats"));
  el("subtitle").textContent = route.nav.replace(/-/g, " ");
  const ctx = {
    params,
    navigate,
    setControls(node) { clear(el("topctl")); if (node) el("topctl").appendChild(node); },
    setStats(node) { const s = clear(el("stats")); if (node) s.appendChild(typeof node === "string" ? document.createTextNode(node) : node); },
    setSubtitle(text) { el("subtitle").textContent = text; },
  };
  const result = route.view.mount(el("view"), ctx) || {};
  current = { view: route.view, cleanup: result.cleanup || null, update: result.update || null };
}

// --- token gate --------------------------------------------------------------
function openGate(rejected) {
  el("gate").classList.add("open");
  el("gatebad").style.display = rejected && getToken() ? "block" : "none";
  el("gatetoken").focus();
}
function closeGate() { el("gate").classList.remove("open"); }

el("gateform").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  setToken(el("gatetoken").value);
  el("gatetoken").value = "";
  try {
    await probe();
  } catch (e) {
    if (e && e.status === 401) { openGate(true); return; }
    // Any other failure (no timeline configured) still lets the user in; the
    // view reports the API's error where it happens.
  }
  closeGate();
  current = { path: "", cleanup: null, view: null };
  render();
  refreshPendingCount();
});
el("lock").addEventListener("click", () => openGate(false));
document.addEventListener("pdw:unauthorized", () => openGate(true));

// --- pending review badge ---------------------------------------------------
async function refreshPendingCount() {
  if (!getToken()) return;
  try {
    const pending = await mutations.list({ statuses: ["pending_review"], limit: 100 });
    const badge = el("pendingcount");
    badge.textContent = String(pending.length);
    badge.hidden = pending.length === 0;
  } catch (e) { /* the badge is best-effort */ }
}
document.addEventListener("pdw:mutations-changed", refreshPendingCount);

// --- links & boot -------------------------------------------------------------
document.addEventListener("click", (ev) => {
  const a = ev.target.closest && ev.target.closest("a[data-link]");
  if (!a || ev.metaKey || ev.ctrlKey || ev.shiftKey || ev.button !== 0) return;
  ev.preventDefault();
  navigate(a.getAttribute("href"));
});
window.addEventListener("popstate", render);

adoptTokenFromLocation();
render();
if (!getToken()) openGate(false); else refreshPendingCount();
setInterval(refreshPendingCount, 60000);
