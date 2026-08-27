// Small DOM and formatting helpers shared by every view. Pure functions live
// in format.js so they can be unit-tested under node without a DOM.
export { pad2, fmtTime, dayOf, fmtFull, formatWhen, isReal, pretty, hue, WEEKDAYS, truncate } from "./format.js";

export function el(id) { return document.getElementById(id); }

export function h(tag, cls, text) {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
}

export function clear(node) { node.textContent = ""; return node; }

export function frag(...children) {
  const f = document.createDocumentFragment();
  for (const child of children) if (child) f.appendChild(typeof child === "string" ? document.createTextNode(child) : child);
  return f;
}

export function section(title, copyValue) {
  const sect = h("div", "sect");
  const head = h("h3", "", title);
  if (copyValue !== undefined) {
    const copy = h("span", "copy", "copy");
    copy.addEventListener("click", () => {
      navigator.clipboard.writeText(typeof copyValue === "string" ? copyValue : JSON.stringify(copyValue, null, 2));
      copy.textContent = "copied ✓";
      setTimeout(() => { copy.textContent = "copy"; }, 1200);
    });
    head.appendChild(copy);
  }
  sect.appendChild(head);
  return sect;
}

export function kvTable(pairs) {
  const table = h("table", "kv");
  pairs.forEach((pair) => {
    if (pair[1] === undefined || pair[1] === null || pair[1] === "") return;
    const tr = h("tr");
    tr.appendChild(h("td", "k", pair[0]));
    const value = pair[1];
    tr.appendChild(h("td", "v", typeof value === "object" ? JSON.stringify(value) : String(value)));
    table.appendChild(tr);
  });
  return table;
}

export function button(label, cls, onClick) {
  const node = h("button", cls, label);
  node.type = "button";
  if (onClick) node.addEventListener("click", onClick);
  return node;
}

// An <a> the SPA router handles (no full reload).
export function link(href, text, cls) {
  const a = h("a", cls, text);
  a.href = href;
  a.setAttribute("data-link", "");
  return a;
}

export function confirmDialog(message) { return window.confirm(message); }
