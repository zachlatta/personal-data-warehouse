import { request } from "./api.js";
import { h, button, clear, kvTable } from "./ui.js";

const api = "/api/auth/tokens";
export function mount(container, ctx) {
  ctx.setSubtitle("External feeds");
  const root = h("main", "connections external-feeds");
  root.append(h("h1", "", "External feeds"), h("p", "", "Give an external service a dedicated read-only key. Each key can reach only its selected feed. A master secret is required to manage keys."));
  const message = h("div", "connection-message"); message.setAttribute("role", "status");
  const form = h("form", "connection-card"); form.append(h("h2", "", "Issue a key"));
  const field = (label, input) => { const wrap = h("label", "connection-field", label); wrap.append(input); form.append(wrap); return input; };
  const select = field("Feed", h("select")); select.required = true;
  const description = h("p"); const endpoint = h("p", "connection-url"); form.append(description, endpoint);
  const name = field("Connection name", h("input")); name.required = true; name.maxLength = 128; name.placeholder = "My automation";
  const expiry = field("Expires (optional, local time)", h("input")); expiry.type = "datetime-local";
  const submit = h("button", "primary", "Issue key"); submit.type = "submit"; form.append(submit);
  const issuedCard = h("section", "connection-card"); issuedCard.hidden = true;
  const secret = h("pre", "external-feed-secret");
  const dismiss = button("Hide key", "", () => { secret.textContent = ""; issuedCard.hidden = true; });
  issuedCard.append(h("h2", "", "Save this key now"), h("p", "", "It is shown only once."), secret, dismiss);
  const listing = h("div", "connection-list"); root.append(message, form, issuedCard, listing); container.append(root);
  let closed = false; let feeds = [];
  const date = value => value && !value.startsWith("0001-") ? new Date(value).toLocaleString() : "Never";
  function selected() {
    const feed = feeds.find(f => f.scope === select.value);
    description.textContent = feed?.description || "";
    endpoint.textContent = feed ? `GET ${feed.path}` : "";
  }
  select.addEventListener("change", selected);
  async function load() {
    try {
      const [registry, data] = await Promise.all([request("/api/external-feeds"), request(api)]);
      if (closed) return;
      feeds = registry.feeds; const value = select.value; clear(select);
      for (const feed of feeds) { const option = h("option", "", feed.display_name); option.value = feed.scope; select.append(option); }
      if (feeds.some(f => f.scope === value)) select.value = value; selected();
      clear(listing); listing.append(h("h2", "", "API keys"));
      const keys = data.tokens.filter(k => feeds.some(f => f.scope === k.scope));
      if (!keys.length) listing.append(h("p", "", "No feed keys yet. Issue one above to connect a service."));
      for (const key of keys) {
        const card = h("section", "connection-card");
        card.append(h("h2", "", key.client_name), h("p", "connection-url", key.id), kvTable([
          ["Feed", feeds.find(f => f.scope === key.scope)?.display_name], ["Status", key.status],
          ["Created", date(key.created_at)], ["Expires", date(key.expires_at)],
          ["Last used", key.request_count ? date(key.last_used_at) : "Never"], ["Requests", key.request_count],
        ]));
        if (key.status === "active") {
          const actions = h("div", "connection-actions");
          const revoke = button("Revoke key", "", async () => {
            revoke.disabled = true;
            try { await request(api + "/" + encodeURIComponent(key.id), { method: "DELETE" }); await load(); }
            catch (e) { if (!closed) message.textContent = e.message; }
            finally { if (!closed) revoke.disabled = false; }
          }); actions.append(revoke); card.append(actions);
        }
        listing.append(card);
      }
    } catch (e) { if (!closed) message.textContent = e.message; }
  }
  form.addEventListener("submit", async event => {
    event.preventDefault(); submit.disabled = true; secret.textContent = ""; issuedCard.hidden = true;
    try {
      const body = { client_name: name.value, scope: select.value };
      if (expiry.value) body.expires_at = new Date(expiry.value).toISOString();
      const issued = await request(api, { method: "POST", body });
      if (closed) return;
      secret.textContent = issued.token; issuedCard.hidden = false;
      message.textContent = "Key issued. Use Authorization: Bearer <client>:<key>. Start with GET, then pass next_cursor as ?since= on each poll. Keep the same cursor on 304; drain pages while has_more is true.";
      await load();
    } catch (e) { if (!closed) message.textContent = e.message; }
    finally { submit.disabled = false; }
  });
  ctx.setControls(button("Reload", "", load)); load(); const refresh = setInterval(load, 10000);
  return { cleanup() { closed = true; clearInterval(refresh); secret.textContent = ""; } };
}
