import { request } from "./api.js";
import { h, button, clear } from "./ui.js";

export function clientNames(value) {
  return [...new Set(value.split(",").map((name) => name.trim()).filter(Boolean))];
}

export function mount(container, ctx) {
  ctx.setSubtitle("MCP connections");
  let disposed = false;
  const root = h("main", "connections");
  const message = h("div", "connection-message");
  message.setAttribute("role", "status");
  const list = h("div", "connection-list");
  root.append(h("h1", "", "One connection. All your tools."), h("p", "", "Connect Skills, Tasks, your data warehouse, or another remote MCP server. Authenticate here; use its tools through PDW everywhere."), message, list);
  container.append(root);

  const auth = new URLSearchParams(location.search).get("auth");
  if (auth) {
    message.textContent = auth === "connected" ? "Connected. Tools are ready. Some MCP apps need a reconnect to discover newly added tools." : auth === "discovery_failed" ? "Authorization succeeded, but tool discovery failed. Retry discovery below." : "Authorization failed or expired. Click Authenticate to try again in this browser.";
    history.replaceState(null, "", "/connections");
  }

  function field(form, label, type = "text", placeholder = "") {
    const wrap = h("label", "connection-field", label);
    const input = h("input"); input.type = type; input.placeholder = placeholder;
    if (type === "password") input.autocomplete = "off";
    wrap.append(input); form.append(wrap); return input;
  }
  const form = h("form", "connection-card");
  form.append(h("h2", "", "Add an MCP server"));
  const name = field(form, "Name / tool prefix", "text", "skills, tasks, or hc");
  name.required = true; name.pattern = "[a-z]([a-z0-9_]{0,22}[a-z0-9])?"; name.maxLength = 24;
  const url = field(form, "MCP endpoint", "url", "https://your-server.example/mcp"); url.required = true;
  form.append(h("p", "", "Remote HTTPS Streamable HTTP endpoints only. The server must be reachable from PDW. Local stdio servers, private-network endpoints, resources and prompts are not supported yet."));
  const advanced = h("details"); advanced.append(h("summary", "", "Bearer token or pre-registered OAuth client (optional)"));
  const token = field(advanced, "Upstream bearer token", "password");
  const clientID = field(advanced, "OAuth client ID");
  const clientSecret = field(advanced, "OAuth client secret", "password");
  const scopes = field(advanced, "OAuth scopes (space-separated)");
  form.append(advanced);
  const loopback = field(form, "Sign in through a localhost loopback redirect", "checkbox");
  form.append(h("p", "", "For providers such as Robinhood that reject PDW's callback address. After you approve, the browser lands on a 127.0.0.1 page that does not load; paste that page's address back here."));
  const clients = field(form, "Allowed PDW client names (comma-separated)", "text", "codex, claude, web");
  const all = field(form, "Share with every authenticated PDW client, including future clients", "checkbox");
  form.append(h("p", "connection-warning", "Sharing includes write tools. Calls run with your upstream account and its permissions, without adding PDW mutation review. Leave access empty to keep tools private until you choose clients. Holders of the PDW app secret have administrator access."));
  const submit = h("button", "primary", "Add connection"); submit.type = "submit"; form.append(submit); root.append(form);

  async function action(id, verb, body = {}) {
    return request(`/api/connections/${encodeURIComponent(id)}/${verb}`, { method: "POST", body });
  }
  async function run(control, fn) {
    control.disabled = true;
    try { await fn(); } catch (err) { if (!disposed) message.textContent = err.message; }
    finally { if (!disposed) control.disabled = false; }
  }
  async function load() {
    try {
      const data = await request("/api/connections");
      if (disposed) return;
      clear(list);
      if (!data.connections.length) list.append(h("p", "", "No connections yet. Add a server below, then authenticate."));
      for (const c of data.connections) {
        const card = h("section", "connection-card");
        card.append(h("h2", "", c.name), h("p", "connection-url", c.url), h("p", "", `${c.enabled ? c.status.replaceAll("_", " ") : "disabled"} · ${c.tool_count} tools · prefix ${c.name}__${c.loopback ? " · loopback sign-in" : ""}`));
        const paste = h("div", "connection-paste");
        const controls = h("div", "connection-actions");
        const authenticate = button("Authenticate", "primary");
        authenticate.addEventListener("click", () => run(authenticate, async () => {
          const data = await action(c.name, "authorize");
          if (disposed) return;
          if (!c.loopback) { window.location.assign(data.authorization_url); return; }
          clear(paste);
          const open = h("a", "", "Open the provider's sign-in in a new tab");
          open.href = data.authorization_url; open.target = "_blank"; open.rel = "noopener noreferrer";
          const redirect = field(paste, "After approving, paste the 127.0.0.1 address the browser was sent to", "url", "http://127.0.0.1:8976/callback?code=…");
          const finish = button("Finish sign-in", "primary");
          finish.addEventListener("click", () => run(finish, async () => {
            const result = await action(c.name, "complete", { url: redirect.value.trim() });
            if (disposed) return;
            await load();
            message.textContent = result.status === "connected" ? "Connected. Tools are ready. Some MCP apps need a reconnect to discover newly added tools." : "Authorization succeeded, but tool discovery failed. Retry discovery below.";
          }));
          paste.prepend(open);
          paste.append(finish);
          message.textContent = "Approve in the new tab, then paste the address it ends on. The sign-in link expires in 10 minutes.";
        }));
        const refresh = button("Refresh tools");
        refresh.addEventListener("click", () => run(refresh, async () => { await action(c.name, "refresh"); await load(); }));
        const toggle = button(c.enabled ? "Disable" : "Enable");
        toggle.addEventListener("click", () => run(toggle, async () => { await action(c.name, c.enabled ? "disable" : "enable", { version: c.version }); await load(); }));
        const remove = button("Remove");
        remove.addEventListener("click", () => run(remove, async () => {
          if (!window.confirm(`Remove ${c.name} and its stored credentials? This does not revoke the authorization at the upstream provider.`)) return;
          await action(c.name, "remove", { version: c.version }); await load();
        }));
        controls.append(authenticate, refresh, toggle, remove); card.append(controls, paste);
        const access = h("details"); access.append(h("summary", "", c.all_clients ? "Access: all authenticated PDW clients" : `Access: ${(c.clients || []).join(", ") || "not shared"}`));
        const names = field(access, "Allowed client names", "text"); names.value = (c.clients || []).join(", ");
        const everyone = field(access, "Share all tools, including writes, with all authenticated clients", "checkbox"); everyone.checked = c.all_clients;
        const save = button("Save access");
        save.addEventListener("click", () => run(save, async () => { await action(c.name, "access", { version: c.version, all_clients: everyone.checked, clients: clientNames(names.value) }); await load(); }));
        access.append(save); card.append(access); list.append(card);
      }
    } catch (err) { if (!disposed) message.textContent = err.message; }
  }
  form.addEventListener("submit", (ev) => {
    ev.preventDefault();
    run(submit, async () => {
      const body = { name: name.value.trim(), url: url.value.trim(), token: token.value, client_id: clientID.value.trim(), client_secret: clientSecret.value, scopes: scopes.value.trim().split(/\s+/).filter(Boolean), all_clients: all.checked, clients: clientNames(clients.value), loopback: loopback.checked };
      await request("/api/connections", { method: "POST", body });
      form.reset(); message.textContent = "Connection added. Click Authenticate for OAuth, or Refresh tools for a bearer-token or unauthenticated server.";
      await load();
    });
  });
  ctx.setControls(button("Reload", "", load));
  load();
  return { cleanup() { disposed = true; token.value = ""; clientSecret.value = ""; } };
}
