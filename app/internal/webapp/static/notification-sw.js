/* No warehouse bearer lives here. Each open proof can only stamp one delivery. */
const DB_NAME = "pdw-notification-opens";
function database() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, 1);
    req.onupgradeneeded = () => req.result.createObjectStore("opens", { keyPath: "delivery_id" });
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}
async function pending(action, value) {
  const db = await database();
  try {
    return await new Promise((resolve, reject) => {
      const tx = db.transaction("opens", "readwrite");
      const store = tx.objectStore("opens");
      const req = action === "all" ? store.getAll() : action === "put" ? store.put(value) : store.delete(value);
      tx.oncomplete = () => resolve(req.result);
      tx.onerror = () => reject(tx.error);
    });
  } finally { db.close(); }
}
async function flushOpens() {
  const records = await pending("all");
  for (const record of records.slice(0, 100)) {
    try {
      const res = await fetch("/api/notifications/opened", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(record), signal: AbortSignal.timeout(5000) });
      if (res.ok || res.status === 403 || res.status === 404) await pending("delete", record.delivery_id);
      else return;
    } catch { return; }
  }
}
self.addEventListener("push", (event) => {
  event.waitUntil((async () => {
    const alert = event.data.json();
    await self.registration.showNotification(alert.title || "PDW", {
      body: [alert.subtitle, alert.body].filter(Boolean).join("\n"),
      icon: alert.icon, tag: alert.notification_id, data: alert,
    });
    await flushOpens();
  })());
});
self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const alert = event.notification.data || {};
  // Open immediately, before any network call can consume user activation.
  let target = new URL(typeof alert.route === "string" && alert.route.startsWith("/timeline/") ? alert.route : "/notifications", self.location.origin).href;
  try { const source = new URL(alert.open?.url); if (source.protocol === "https:") target = source.href; } catch { /* exact PDW event is the fallback */ }
  const open = clients.openWindow(target);
  event.waitUntil(Promise.all([open, (async () => {
    if (typeof alert.delivery_id === "string" && typeof alert.open_proof === "string") {
      await pending("put", { delivery_id: alert.delivery_id, open_proof: alert.open_proof });
      await flushOpens();
    }
  })()]));
});
self.addEventListener("message", (event) => { if (event.data === "flush-opens") event.waitUntil(flushOpens()); });
self.addEventListener("activate", (event) => event.waitUntil(flushOpens()));
