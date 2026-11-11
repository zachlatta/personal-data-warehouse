import { request } from "./api.js";
import { h, button, clear } from "./ui.js";

const scope = "feed:voice-memos";
const api = "/api/auth/tokens";
export function mount(container, ctx) {
  ctx.setSubtitle("External feeds");
  const panel = h("section", "sect");
  panel.append(h("h2", "", "Voice memos"), h("p", "", "Read-only memos recorded in the last 48 hours, after transcription and recording enrichment. Includes calendar matching; does not wait for session joining."));
  panel.append(h("p", "", "GET /api/external-feeds/voice-memos · Authorization: Bearer <client>:<key>. Poll once a second; send If-None-Match with the previous ETag. Follow next_offset for more pages. A master secret is required to manage keys."));
  const form = h("form");
  const name = h("input"); name.required = true; name.maxLength = 128; name.placeholder = "Connection name";
  const nameLabel = h("label", "", "Connection name "); nameLabel.append(name);
  const expiry = h("input"); expiry.type = "datetime-local";
  const expiryLabel = h("label", "", "Expires (optional, local time) "); expiryLabel.append(expiry);
  const submit = h("button", "", "Issue key"); submit.type = "submit";
  form.append(nameLabel, expiryLabel, submit);
  const message = h("p"); message.setAttribute("role", "status");
  const secret = h("pre");
  const dismiss = button("Hide key", "", () => { secret.textContent = ""; dismiss.hidden = true; }); dismiss.hidden = true;
  const listing = h("div");
  panel.append(form, message, secret, dismiss, listing); container.append(panel);
  let closed = false;
  async function load() {
    try {
      const data = await request(api); if (closed) return;
      clear(listing);
      const table = h("table", "kv");
      const head = h("tr");
      for (const text of ["Name", "Created", "Expires", "Last used", "Requests", "Status", ""]) head.append(h("th", "", text));
      table.append(head);
      for (const key of data.tokens.filter(k => k.scope === scope)) {
        const row = h("tr");
        const date = value => value && !value.startsWith("0001-") ? new Date(value).toLocaleString() : "Never";
        for (const text of [key.client_name, date(key.created_at), date(key.expires_at), key.request_count ? date(key.last_used_at) : "Never", key.request_count, key.status]) row.append(h("td", "", text));
        const action = h("td");
        if (key.status === "active") action.append(button("Revoke", "", async () => {
          try { await request(api + "/" + encodeURIComponent(key.id), { method: "DELETE" }); await load(); }
          catch (e) { message.textContent = e.message; }
        }));
        row.append(action); table.append(row);
      }
      listing.append(table);
    } catch (e) { if (!closed) message.textContent = e.message; }
  }
  form.addEventListener("submit", async event => {
    event.preventDefault(); submit.disabled = true; secret.textContent = ""; dismiss.hidden = true;
    try {
      const body = { client_name: name.value, scope };
      if (expiry.value) body.expires_at = new Date(expiry.value).toISOString();
      const issued = await request(api, { method: "POST", body });
      if (closed) return;
      message.textContent = "Save this key now. It is shown only once.";
      secret.textContent = issued.token; dismiss.hidden = false;
      await load();
    } catch (e) { if (!closed) message.textContent = e.message; }
    finally { submit.disabled = false; }
  });
  load(); const refresh = setInterval(load, 10000);
  return { cleanup() { closed = true; clearInterval(refresh); secret.textContent = ""; } };
}
