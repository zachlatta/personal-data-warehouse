import { request } from "./api.js";

function node(tag, text, className) {
  const n = document.createElement(tag); if (text) n.textContent = text; if (className) n.className = className; return n;
}
function keyBytes(key) { return Uint8Array.from(atob(key.replace(/-/g, "+").replace(/_/g, "/")), (c) => c.charCodeAt(0)); }
export function mount(container) {
  let stopped = false;
  const panel = node("section", "", "notification-panel"); container.append(panel);
  const heading = node("h1", "Your attention, in one place");
  const intro = node("p", "New direct and CC timeline events, excluding items already read or replied to when the synced source state confirms it. No history replay. Existing app notifications stay unchanged.");
  const health = node("p"); const controls = node("div", "", "notification-controls");
  const toggle = node("button"); const register = node("button", "Enable this browser"); const disable = node("button", "Disable this browser");
  const feedback = node("p", "", "notification-feedback"); feedback.setAttribute("role", "status");
  const list = node("div", "", "notification-history");
  const note = node("p", "Accepted means the push service accepted it, not that it appeared on screen. No observed open does not mean ignored. iOS keeps PDW’s header icon; source artwork is a thumbnail. Delivery includes the source’s sync delay.", "notification-note");
  controls.append(toggle, register, disable); panel.append(heading, intro, health, controls, feedback, note, list);
  let state = null;
  async function load() {
    try {
      const next = await request("/api/notifications"); if (stopped) return; state = next;
      health.textContent = `Experiment ${state.enabled ? "on" : "paused"} · ${state.status}${state.error ? " · " + state.error : ""}`;
      toggle.textContent = state.enabled ? "Pause all timeline notifications" : "Start experiment";
      register.disabled = !state.web_public_key || !("serviceWorker" in navigator) || !("PushManager" in window);
      if (!state.web_public_key) register.title = "Server web push keys are not configured";
      list.replaceChildren();
      if (!state.events.length) list.append(node("p", "No notifications yet. Start the experiment to capture new arrivals."));
      for (const item of state.events) {
        const card = node("article", "", "notification-card");
        const top = node("div", "", "notification-card-meta");
        top.append(node("span", `${item.source} · ${item.priority}`), node("time", new Date(item.created_at).toLocaleString()));
        const preview = item.preview || {};
        const content = node("div", "", "notification-card-content");
        if (preview.icon) { const icon = node("img"); icon.src = preview.icon; icon.alt = ""; icon.width = 44; icon.height = 44; content.append(icon); }
        const words = node("div");
        words.append(node("h2", preview.title || item.actor || item.title || "New item"));
        if (preview.subtitle || item.title) words.append(node("strong", preview.subtitle || item.title));
        words.append(node("p", preview.body ?? item.body));
        if (preview.route) { const link = node("a", "View timeline item"); link.href = preview.route; words.append(link); }
        content.append(words);
        card.append(top, content);
        card.append( node("small", `${item.status} · ${item.accepted}/${item.devices} accepted · ${item.opened} opened${item.suppressed_read ? " · " + item.suppressed_read + " skipped: already read" : ""}${item.suppressed_replied ? " · " + item.suppressed_replied + " skipped: already replied" : ""}${item.failed ? " · " + item.failed + " failed/unknown" : ""}`));
        list.append(card);
      }
    } catch (e) { if (!stopped) feedback.textContent = e.message; }
  }
  toggle.onclick = async () => { if (!state) return; toggle.disabled = true; try { await request("/api/notifications/settings", { method: "POST", body: { enabled: !state.enabled } }); await load(); } catch (e) { feedback.textContent = e.message; } finally { toggle.disabled = false; } };
  register.onclick = async () => {
    register.disabled = true;
    try {
      // Permission must be requested inside this explicit user gesture.
      if (await Notification.requestPermission() !== "granted") throw new Error("Notifications are blocked. Allow them in browser settings to try again.");
      const sw = await navigator.serviceWorker.register("/app/notification-sw.js");
      if (!sw.active) await new Promise((resolve, reject) => {
        const worker = sw.installing || sw.waiting;
        if (!worker) { reject(new Error("Notification worker did not start")); return; }
        worker.addEventListener("statechange", () => {
          if (worker.state === "activated") resolve();
          if (worker.state === "redundant") reject(new Error("Notification worker could not activate"));
        });
      });
      const sub = await sw.pushManager.getSubscription() || await sw.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: keyBytes(state.web_public_key) });
      await request("/api/notifications/web/register", { method: "POST", body: { subscription: sub.toJSON(), device_name: navigator.platform || "Browser" } });
      sw.active?.postMessage("flush-opens");
      feedback.textContent = "This browser is registered. Keep notifications allowed; the page need not stay open.";
    } catch (e) { feedback.textContent = e.message; } finally { register.disabled = false; }
  };
  disable.onclick = async () => {
    try {
      const sw = await navigator.serviceWorker.getRegistration("/app/"); const sub = await sw?.pushManager.getSubscription();
      if (sub) { await request("/api/notifications/web/disable", { method: "POST", body: { endpoint: sub.endpoint } }); await sub.unsubscribe(); }
      feedback.textContent = "This browser is disabled. Other devices are unchanged.";
    } catch (e) { feedback.textContent = e.message; }
  };
  const flush = () => { if ("serviceWorker" in navigator) void navigator.serviceWorker.getRegistration("/app/").then(sw => sw?.active?.postMessage("flush-opens")).catch(() => {}); };
  window.addEventListener("online", flush); window.addEventListener("focus", flush); flush();
  void load(); const timer = setInterval(load, 15000);
  return { cleanup() { stopped = true; clearInterval(timer); window.removeEventListener("online", flush); window.removeEventListener("focus", flush); } };
}
