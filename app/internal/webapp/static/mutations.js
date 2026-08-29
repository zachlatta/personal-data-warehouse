// The mutation-review view: the request list at /mutation-review and one
// request's detail at /mutation-review/requests/<id>. It is the SPA twin of
// the old server-rendered review UI (app/internal/mutations/ui.go), over the
// same JSON API the iOS app uses. Every pure decision lives in
// mutation_view.js; this file only builds DOM and talks to the API.
import { mutations } from "./api.js";
import { h, clear, frag, button, link, confirmDialog, formatWhen, fmtFull } from "./ui.js";
import * as V from "./mutation_view.js";

const LIST_PATH = "/mutation-review";
function requestPath(id) { return LIST_PATH + "/requests/" + encodeURIComponent(id); }

function mutationsChanged() { document.dispatchEvent(new CustomEvent("pdw:mutations-changed")); }

// An "unauthorized" error is handled globally (the token gate opens); showing
// it inline would only duplicate the gate.
function errorText(err) {
  if (!err) return "";
  if (err.message === "unauthorized") return "";
  return err.message || String(err);
}

// --- shared bits ---------------------------------------------------------------

function pill(status) { return h("span", "mpill s-" + (status || "unknown"), status || "unknown"); }

function pre(value) { return h("pre", "blob", typeof value === "string" ? value : V.prettyJSON(value)); }

function details(summaryText, cls, open) {
  const node = h("details", cls);
  node.appendChild(h("summary", "", summaryText));
  if (open) node.open = true;
  return node;
}

function dl(pairs) {
  const node = h("dl", "mdl");
  for (const [k, v] of pairs) {
    if (v === undefined || v === null || v === "") continue;
    node.appendChild(h("dt", "", k));
    node.appendChild(h("dd", "", v));
  }
  return node;
}

function codeList(container, fields) {
  fields.forEach((field, index) => {
    if (index > 0) container.appendChild(document.createTextNode(", "));
    container.appendChild(h("code", "", field));
  });
}

// A sandboxed iframe over a self-contained white document: the body HTML is
// raw provider markup and must never run in the app's origin.
function bodyFrame(html, quoted) {
  const frame = document.createElement("iframe");
  frame.className = "mail-frame" + (quoted ? " quoted" : "");
  frame.setAttribute("sandbox", "");
  frame.setAttribute("referrerpolicy", "no-referrer");
  frame.style.height = V.gmailBodyFrameHeight(html, quoted) + "px";
  frame.srcdoc = V.emailHTMLDocument(html);
  return frame;
}

function mutationHead(eyebrow, title, statusText) {
  const head = h("div", "mhead");
  const left = h("div");
  left.appendChild(h("div", "eyebrow", eyebrow));
  left.appendChild(h("h3", "", title));
  head.appendChild(left);
  head.appendChild(pill(statusText));
  return head;
}

// --- gmail threads --------------------------------------------------------------

function renderGmailMessage(message, open) {
  const from = V.trimStr(message.from_address) || "Unknown sender";
  const to = V.stringSlice(message.to_addresses).join(", ");
  const cc = V.stringSlice(message.cc_addresses).join(", ");
  let preview = V.truncateRunes(V.trimStr(message.preview_text), 900);
  if (!preview) preview = V.truncateRunes(V.trimStr(message.snippet), 900);
  const node = h("details", "gmsg");
  if (open) node.open = true;
  const summary = h("summary", "gmsg-sum");
  summary.appendChild(h("span", "avatar", V.senderInitial(from)));
  const main = h("div", "gmsg-main");
  const header = h("div", "gmsg-head");
  const who = h("div");
  who.appendChild(h("strong", "", from));
  if (to) who.appendChild(h("span", "gto", "to " + to));
  if (cc) who.appendChild(h("span", "gto", "cc " + cc));
  header.appendChild(who);
  header.appendChild(h("time", "", V.formatGmailFullTime(message.internal_date)));
  main.appendChild(header);
  if (preview) main.appendChild(h("p", "gmsg-preview", preview));
  summary.appendChild(main);
  node.appendChild(summary);

  const rawBody = V.trimStr(message.body_html);
  if (rawBody) {
    // email.reply_threads messages arrive with quoted_html already split off;
    // preview.threads messages carry the whole thing and are split here.
    let body = rawBody; let quoted = V.trimStr(message.quoted_html);
    if (!quoted) ({ body, quoted } = V.splitGmailQuotedHTML(rawBody));
    const wrap = h("div", "gmsg-body");
    wrap.appendChild(bodyFrame(body, false));
    if (quoted) {
      const q = details("Quoted message", "gquoted");
      q.appendChild(bodyFrame(quoted, true));
      wrap.appendChild(q);
    }
    node.appendChild(wrap);
  } else if (preview) {
    const wrap = h("div", "gmsg-body");
    wrap.appendChild(h("p", "", preview));
    node.appendChild(wrap);
  }
  return node;
}

function renderGmailThread(thread, { open = false, afterMessages = null } = {}) {
  const t = V.gmailThreadSummary(thread);
  const node = h("details", "gthread");
  if (open) node.open = true;
  const summary = h("summary", "grow");
  const sender = h("span", "gsender", t.senderName);
  if (t.messageCount > 1) sender.appendChild(h("span", "gcount", " " + t.messageCount));
  summary.appendChild(sender);
  const labels = h("span", "glabels");
  t.labels.forEach((label) => labels.appendChild(h("span", "glabel", label)));
  summary.appendChild(labels);
  const subject = h("span", "gsubject");
  subject.appendChild(h("strong", "", t.subject));
  if (t.latestPreview) subject.appendChild(h("span", "gprev", " - " + t.latestPreview));
  summary.appendChild(subject);
  summary.appendChild(h("span", "gdate", V.formatGmailCompactTime(t.latestAt)));
  node.appendChild(summary);

  const expanded = h("div", "gexpanded");
  const subjectHead = h("div", "gexp-subject");
  subjectHead.appendChild(h("h4", "", t.subject));
  const meta = h("div", "gmeta m");
  if (t.messageCount > 0) meta.appendChild(h("span", "", t.messageCount + " message" + V.plural(t.messageCount)));
  t.labels.forEach((label) => meta.appendChild(h("span", "glabel", label)));
  if (t.threadId) meta.appendChild(h("span", "thread-id", t.threadId));
  subjectHead.appendChild(meta);
  expanded.appendChild(subjectHead);
  if (t.messages.length) {
    const list = h("div", "gmessages");
    t.messages.forEach((message, index) => list.appendChild(renderGmailMessage(message, V.gmailMessageOpen(message, index, t.messages, t.unread))));
    expanded.appendChild(list);
  }
  if (afterMessages) expanded.appendChild(afterMessages);
  node.appendChild(expanded);
  return node;
}

function renderGmailGroup(group) {
  const verb = group.operation === V.GMAIL_UNARCHIVE ? "Unarchive" : "Archive";
  const threads = V.gmailMutationGroupThreads(group.mutations);
  const article = h("article", "mut gmail");
  article.appendChild(mutationHead(verb, V.gmailMutationGroupTitle(verb, threads.length), V.gmailMutationGroupStatus(group.status, group.mutations.length)));
  article.appendChild(h("p", "mmeta m", group.operation + " for " + group.account));
  article.appendChild(h("p", "mreason", V.gmailMutationGroupActionText(group.operation, threads.length)));
  const list = h("div", "gthreads");
  threads.forEach((thread) => list.appendChild(renderGmailThread(thread)));
  article.appendChild(list);
  return article;
}

// --- gmail send_email --------------------------------------------------------------

// Trailing empty blocks in a contenteditable are cursor room, not content.
function stripTrailingEmptyBlocks(root) {
  const template = document.createElement("template");
  template.innerHTML = root.innerHTML || "";
  while (template.content.lastElementChild && template.content.lastElementChild.textContent.trim() === "" && !template.content.lastElementChild.querySelector("img")) {
    template.content.lastElementChild.remove();
  }
  return template.innerHTML.trim() || "<div><br></div>";
}

function renderComposer(request, mutation, variant, deliveryMode, hasVariants, actions) {
  const form = h("form", "composer");
  const state = { deliveryMode };

  const modes = h("div", "delivery");
  const modeName = "delivery-" + mutation.id + "-" + (variant.id || "default");
  for (const [value, label] of [["send", "Send email"], ["draft", "Create draft"]]) {
    const lab = h("label", "delivery-opt");
    const radio = h("input"); radio.type = "radio"; radio.name = modeName; radio.value = value; radio.checked = deliveryMode === value;
    radio.addEventListener("change", () => { if (radio.checked) state.deliveryMode = value; });
    lab.appendChild(radio); lab.appendChild(h("span", "", label));
    modes.appendChild(lab);
  }
  form.appendChild(modes);
  if (hasVariants) {
    const copy = h("p", "m", "Selected proposal: ");
    copy.appendChild(h("strong", "", variant.title || variant.id));
    form.appendChild(copy);
  }

  const fields = h("div", "cfields");
  const inputs = {};
  for (const [key, label, value] of [
    ["to", "To", V.stringSlice(variant.to).join(", ")],
    ["cc", "Cc", V.stringSlice(variant.cc).join(", ")],
    ["bcc", "Bcc", V.stringSlice(variant.bcc).join(", ")],
    ["subject", "Subject", V.str(variant.subject)],
  ]) {
    const lab = h("label", "cfield");
    lab.appendChild(h("span", "lab", label));
    const input = h("input"); input.value = value; input.autocomplete = "off";
    inputs[key] = input;
    lab.appendChild(input);
    fields.appendChild(lab);
  }
  form.appendChild(fields);

  const editorWrap = h("div", "editor-wrap");
  const toolbar = h("div", "toolbar");
  const editor = h("div", "editor");
  editor.contentEditable = "true";
  editor.spellcheck = true;
  // editor_html is server-produced from the stored message (already split
  // from its signature and quote); seeding a contenteditable with it is the
  // one place innerHTML with server data is intended here.
  editor.innerHTML = V.str(variant.editor_html) || "<div><br></div>";
  for (const [label, command, title] of [["B", "bold", "Bold"], ["I", "italic", "Italic"], ["• list", "insertUnorderedList", "Bulleted list"], ["1. list", "insertOrderedList", "Numbered list"]]) {
    const b = button(label, "tb", () => { editor.focus(); document.execCommand(command, false, null); });
    b.title = title;
    toolbar.appendChild(b);
  }
  editorWrap.appendChild(toolbar);
  editorWrap.appendChild(editor);
  const signatureHTML = V.trimStr(variant.signature_html);
  let signatureNode = null;
  if (signatureHTML) {
    signatureNode = h("div", "signature");
    signatureNode.innerHTML = signatureHTML; // sanitized server-side (email_view.go)
    editorWrap.appendChild(signatureNode);
  }
  const quotedHTML = V.trimStr(variant.quoted_html);
  if (quotedHTML) {
    const q = details("Quoted thread", "gquoted");
    q.appendChild(bodyFrame(quotedHTML, true));
    editorWrap.appendChild(q);
  }
  form.appendChild(editorWrap);

  const errorNode = h("div", "bad");
  form.appendChild(errorNode);

  async function submit(deliveryOverride) {
    const body = V.assembleEmailBody({
      editorHTML: stripTrailingEmptyBlocks(editor),
      editorText: editor.innerText,
      signatureHTML,
      signatureText: signatureNode ? signatureNode.innerText : "",
      quotedHTML,
      quotedText: V.str(variant.quoted_text),
    });
    const input = {
      delivery_mode: deliveryOverride || state.deliveryMode,
      selected_variant_id: V.str(variant.id),
      message: {
        to: V.splitEmailAddressList(inputs.to.value),
        cc: V.splitEmailAddressList(inputs.cc.value),
        bcc: V.splitEmailAddressList(inputs.bcc.value),
        subject: inputs.subject.value.trim(),
        body_text: body.body_text,
        body_html: body.body_html,
        reply_to_thread_id: V.str(variant.reply_to_thread_id),
        in_reply_to: V.str(variant.in_reply_to),
        references: V.stringSlice(variant.references),
      },
    };
    errorNode.textContent = "";
    try {
      await mutations.updateEmail(request.id, mutation.id, input);
      mutationsChanged();
      actions.reload();
    } catch (err) { errorNode.textContent = errorText(err); }
  }

  const buttons = h("div", "cactions");
  const primary = button(hasVariants ? "Use this version" : "Save email changes", "primary", () => submit(null));
  const draft = button("Save as draft instead", "", () => submit("draft"));
  buttons.appendChild(primary); buttons.appendChild(draft);
  form.appendChild(buttons);
  form.addEventListener("submit", (ev) => { ev.preventDefault(); submit(null); });
  return form;
}

function renderComposerPanels(request, mutation, email, actions) {
  const variants = V.mapSlice(email.variants);
  const hasVariants = email.has_variants === true && variants.length > 1;
  const wrap = h("div", "composers");
  const panels = [];
  if (hasVariants) {
    const tabs = h("div", "vtabs");
    variants.forEach((variant, index) => {
      const tab = button(variant.title || "Version " + (index + 1), "vtab" + (variant.selected ? " active" : ""), () => {
        tabs.querySelectorAll(".vtab").forEach((t, i) => t.classList.toggle("active", i === index));
        panels.forEach((panel, i) => { panel.hidden = i !== index; });
      });
      tabs.appendChild(tab);
    });
    wrap.appendChild(tabs);
  }
  variants.forEach((variant) => {
    const panel = h("div", "vpanel");
    panel.hidden = hasVariants && !variant.selected;
    panel.appendChild(renderComposer(request, mutation, variant, email.delivery_mode, hasVariants, actions));
    panels.push(panel);
    wrap.appendChild(panel);
  });
  return wrap;
}

function renderReplyContext(threads, composer) {
  const wrap = h("div", "reply-context");
  wrap.appendChild(h("div", "eyebrow", "Replying in thread"));
  const list = h("div", "gthreads");
  threads.forEach((thread, index) => {
    list.appendChild(renderGmailThread(thread, index === 0 && composer ? { open: true, afterMessages: composer } : {}));
  });
  wrap.appendChild(list);
  return wrap;
}

function renderGmailEmail(request, mutation, actions) {
  const email = V.asMap(mutation.email);
  const deliveryMode = email.delivery_mode === "draft" ? "draft" : "send";
  const message = V.asMap(email.message);
  const threads = V.mapSlice(email.reply_threads);
  const pending = request.status === "pending_review" && mutation.status === "pending_review";
  const article = h("article", "mut email");
  article.appendChild(mutationHead("Gmail Email", V.gmailEmailTitle(deliveryMode), mutation.status));
  article.appendChild(h("p", "mmeta m", mutation.operation + " for " + mutation.account));
  article.appendChild(h("p", "mreason", V.gmailEmailActionText(deliveryMode)));
  if (pending) {
    const composer = renderComposerPanels(request, mutation, email, actions);
    if (threads.length) article.appendChild(renderReplyContext(threads, composer));
    else article.appendChild(composer);
    const removeRow = h("div", "remove-row");
    const removeError = h("span", "bad");
    removeRow.appendChild(button("Don't send this one", "danger", async () => {
      if (!confirmDialog("Mark this email as do-not-send? It will be skipped when the request is approved.")) return;
      removeError.textContent = "";
      try { await mutations.remove(request.id, mutation.id); mutationsChanged(); actions.reload(); } catch (err) { removeError.textContent = errorText(err); }
    }));
    removeRow.appendChild(removeError);
    article.appendChild(removeRow);
  } else {
    if (threads.length) article.appendChild(renderReplyContext(threads, null));
    const ro = h("div", "email-ro");
    ro.appendChild(dl([
      ["Delivery", deliveryMode], ["To", V.stringSlice(message.to).join(", ")], ["Cc", V.stringSlice(message.cc).join(", ")],
      ["Bcc", V.stringSlice(message.bcc).join(", ")], ["Subject", V.str(message.subject)],
    ]));
    ro.appendChild(bodyFrame(V.trimStr(message.body_html), false));
    article.appendChild(ro);
  }
  return article;
}

// --- contacts -------------------------------------------------------------------

function renderContactPersonBlock(title, person) {
  if (!person || Object.keys(person).length === 0) return null;
  const s = V.contactSummaryFromPerson(person);
  const block = h("div", "person-block");
  block.appendChild(h("div", "eyebrow", title));
  block.appendChild(dl([["Name", s.displayName], ["Email", s.primaryEmail], ["Phone", s.primaryPhone], ["Organization", s.organization]]));
  return block;
}

function renderContactOperation({ operation, mutation }) {
  const op = V.canonicalContactOp(V.trimStr(operation.op));
  const summary = V.contactOperationSummary(operation);
  const node = h("div", "contact-op " + V.contactOperationClass(op));
  const main = h("div", "contact-main");
  main.appendChild(h("span", "avatar", V.senderInitial(summary.displayName)));
  const copy = h("div", "contact-copy");
  copy.appendChild(h("div", "eyebrow", V.contactOperationTitle(op)));
  copy.appendChild(h("h4", "", V.contactSummaryTitle(summary, operation)));
  const meta = h("div", "contact-meta m");
  [summary.primaryEmail, summary.primaryPhone, summary.organization].filter(Boolean).forEach((v) => meta.appendChild(h("span", "", v)));
  copy.appendChild(meta);

  const effect = V.contactEffect(operation, op);
  if (op === "update_contact") {
    const p = h("p", "effect", "Replaces ");
    if (effect.replaced.length === 0) p.appendChild(document.createTextNode("selected fields"));
    else codeList(p, effect.replaced);
    copy.appendChild(p);
    if (effect.cleared.length) {
      const clears = h("p", "effect clears", "Clears ");
      codeList(clears, effect.cleared);
      clears.appendChild(document.createTextNode(" — the current value is deleted."));
      copy.appendChild(clears);
    }
  } else if (effect.sentence) {
    copy.appendChild(h("p", "effect", effect.sentence));
  }
  const resource = V.trimStr(operation.resource_name);
  if (resource) { const p = h("p", "m"); p.appendChild(h("code", "", resource)); copy.appendChild(p); }
  const warning = V.contactEtagWarning(operation, op);
  if (warning) copy.appendChild(h("p", "effect clears", warning));
  main.appendChild(copy);
  main.appendChild(pill(mutation.status));
  node.appendChild(main);

  if (op === "update_contact") {
    const fields = V.contactUpdateFields(operation);
    const before = V.asMap(operation.before); const after = V.asMap(operation.after);
    node.appendChild(h("p", "m", "Fields not listed here are not part of this update."));
    const diff = h("div", "cdiff");
    for (const field of fields) {
      const row = h("div", "crow");
      row.appendChild(h("code", "", field));
      const b = V.contactFieldDisplayValue(before[field]); const a = V.contactFieldDisplayValue(after[field]);
      if (a === b) row.appendChild(h("span", "unchanged", a));
      else {
        const change = h("span", "change");
        change.appendChild(h("del", "old", b));
        change.appendChild(h("span", "arrow", "→"));
        change.appendChild(h("ins", "new", a));
        row.appendChild(change);
      }
      diff.appendChild(row);
    }
    if (fields.length === 0) diff.appendChild(h("p", "m", "No explicit update fields were provided."));
    node.appendChild(diff);
  } else if (op === "create_contact") {
    let person = V.asMap(operation.person);
    if (Object.keys(person).length === 0) person = V.personFromFlatOperation(operation);
    const block = renderContactPersonBlock("Contact to create", person);
    if (block) node.appendChild(block);
  } else if (op === "delete_contact") {
    let person = V.asMap(operation.before);
    if (Object.keys(person).length === 0) person = V.personFromFlatOperation(operation);
    const block = renderContactPersonBlock("Contact to delete", person);
    if (block) node.appendChild(block);
  }
  const raw = details("Raw contact operation", "raw");
  raw.appendChild(pre(operation));
  node.appendChild(raw);
  return node;
}

function renderContactGroup(group) {
  const operations = V.contactMutationGroupOperations(group.mutations);
  const article = h("article", "mut contacts");
  article.appendChild(mutationHead("Contacts", V.contactMutationGroupTitle(operations.length), V.gmailMutationGroupStatus(group.status, group.mutations.length)));
  article.appendChild(h("p", "mmeta m", "contacts.batch_mutation for " + group.account));
  const list = h("div", "contact-ops");
  operations.forEach((view) => list.appendChild(renderContactOperation(view)));
  article.appendChild(list);
  return article;
}

// --- calendar -------------------------------------------------------------------

function autolinked(text) {
  const out = document.createDocumentFragment();
  for (const seg of V.autolinkSegments(text)) {
    if (seg.url) {
      const a = h("a", "", seg.url); a.href = seg.url; a.target = "_blank"; a.rel = "nofollow noopener";
      out.appendChild(a);
    } else out.appendChild(document.createTextNode(seg.text));
  }
  return out;
}

function renderCalendar(mutation) {
  const preview = V.asMap(V.asMap(mutation.preview).event);
  const operation = V.calendarOperation(mutation);
  const verb = V.calendarVerbForOperation(operation);
  const calendarID = V.str(preview.calendar_id) || "primary";
  const sendUpdates = V.str(preview.send_updates) || "all";
  const eventID = V.str(preview.event_id); const etag = V.str(preview.expected_etag);
  const article = h("article", "mut calendar " + operation);

  const banner = h("div", "cal-banner");
  const left = h("div", "cal-banner-left");
  left.appendChild(h("span", "cal-op", verb));
  left.appendChild(h("span", "m", mutation.account));
  banner.appendChild(left);
  banner.appendChild(h("span", "mpill cal-" + V.calendarStatusClass(mutation.status), V.humanStatus(mutation.status)));
  article.appendChild(banner);

  const body = h("div", "cal-body");
  body.appendChild(h("h3", "cal-title", V.calendarTitle(mutation, preview, operation, verb)));
  if (V.trimStr(mutation.reason)) body.appendChild(h("p", "mreason", mutation.reason.trim()));

  const when = V.calendarWhen(preview);
  if (when) {
    const block = h("div", "cal-when");
    block.appendChild(h("span", "cal-icon", "🗓"));
    const inner = h("div");
    inner.appendChild(h("div", "cal-when-date", when.date));
    inner.appendChild(h("div", "cal-when-time", when.time));
    block.appendChild(inner);
    body.appendChild(block);
  }

  const meta = h("ul", "cal-meta");
  const calItem = h("li"); calItem.appendChild(h("span", "lab", "Calendar ")); calItem.appendChild(h("code", "", calendarID)); meta.appendChild(calItem);
  const notify = h("li", "notify-" + V.notifyClass(sendUpdates));
  notify.appendChild(h("span", "", V.notifyIcon(sendUpdates) + " ")); notify.appendChild(h("span", "lab", "Notifications ")); notify.appendChild(h("span", "", V.sendUpdatesLabel(sendUpdates)));
  meta.appendChild(notify);
  const rrules = V.stringSliceForList(preview.recurrence);
  if (rrules.length) { const r = h("li"); r.appendChild(h("span", "", "🔁 ")); r.appendChild(h("span", "lab", "Repeats ")); r.appendChild(h("span", "", V.humanRecurrence(rrules))); meta.appendChild(r); }
  body.appendChild(meta);

  const location = V.trimStr(preview.location);
  if (location) { const l = h("div", "cal-sect cal-location"); l.appendChild(h("span", "cal-icon", "📍")); l.appendChild(h("div", "", location)); body.appendChild(l); }
  const description = V.trimStr(preview.description);
  if (description) {
    const d = h("div", "cal-sect"); d.appendChild(h("h4", "", "Description"));
    const p = h("p", "cal-desc");
    description.split("\n").forEach((line, index) => { if (index > 0) p.appendChild(h("br")); p.appendChild(autolinked(line)); });
    d.appendChild(p); body.appendChild(d);
  }

  const attendees = V.calendarAttendees(preview.attendees);
  if (attendees.length) {
    const sect = h("div", "cal-sect"); sect.appendChild(h("h4", "", "Guests (" + attendees.length + ")"));
    const ul = h("ul", "cal-attendees");
    for (const a of attendees) {
      const li = h("li", "cal-attendee");
      li.appendChild(h("span", "avatar " + V.calendarAvatarColor(a.email + a.displayName), V.calendarInitials(a.displayName, a.email)));
      const ab = h("div");
      ab.appendChild(h("div", "", a.displayName || a.email));
      if (a.displayName) ab.appendChild(h("div", "m", a.email));
      const tags = h("div", "cal-tags");
      if (a.organizer) tags.appendChild(h("span", "tag organizer", "Organizer"));
      if (a.optional) tags.appendChild(h("span", "tag optional", "Optional"));
      if (a.responseStatus) tags.appendChild(h("span", "tag rsvp-" + a.responseStatus, V.humanResponseStatus(a.responseStatus)));
      if (tags.childNodes.length) ab.appendChild(tags);
      li.appendChild(ab); ul.appendChild(li);
    }
    sect.appendChild(ul); body.appendChild(sect);
  }

  if (operation === "update") {
    const patch = V.asMap(V.asMap(mutation.payload).patch);
    const keys = Object.keys(patch).sort();
    if (keys.length) {
      const sect = h("div", "cal-sect"); sect.appendChild(h("h4", "", "Changes"));
      const table = h("table", "cal-patch");
      const thead = h("thead"); const hr = h("tr"); hr.appendChild(h("th", "", "Field")); hr.appendChild(h("th", "", "New value")); thead.appendChild(hr); table.appendChild(thead);
      const tbody = h("tbody");
      for (const key of keys) {
        const tr = h("tr"); tr.appendChild(h("th", "", V.humanFieldName(key)));
        const td = h("td"); const value = V.calendarPatchValue(key, patch[key]);
        if (value.json !== undefined) td.appendChild(pre(value.json)); else td.textContent = value.text;
        tr.appendChild(td); tbody.appendChild(tr);
      }
      table.appendChild(tbody); sect.appendChild(table); body.appendChild(sect);
    }
  }
  if (operation === "delete") {
    const warn = h("div", "cal-sect cal-delete");
    warn.appendChild(h("strong", "", "This event will be cancelled. "));
    warn.appendChild(document.createTextNode(V.deleteNotificationSentence(sendUpdates)));
    body.appendChild(warn);
  }
  if (eventID || etag) {
    const tech = details("Technical details", "raw");
    tech.appendChild(dl([["Event ID", eventID], ["Expected etag", etag]]));
    body.appendChild(tech);
  }
  article.appendChild(body);
  return article;
}

// --- apple notes and the generic fallback -------------------------------------

function renderAppleNotes(mutation) {
  const note = V.appleNotesView(mutation);
  const article = h("article", "mut notes");
  article.appendChild(mutationHead("Apple Notes", note.heading, mutation.status));
  article.appendChild(h("p", "mmeta m", mutation.operation + " for " + mutation.account));
  article.appendChild(dl([["Title", note.name], ["Folder", note.folder], ["Note", note.noteId], ["Changes", note.changes.join(", ")]]));
  if (note.bodyPreview) article.appendChild(pre(note.bodyPreview));
  return article;
}

function renderSlackMarkRead(mutation) {
  const view = V.slackMarkReadView(mutation);
  const article = h("article", "mut slack-read");
  const title = view.conversationLabel ? "Mark " + view.conversationLabel + " read" : view.heading;
  article.appendChild(mutationHead("Slack · reviewed action", title, mutation.status));
  article.appendChild(h("p", "mmeta m", mutation.operation + " for " + mutation.account));

  const action = h("div", "slack-action");
  action.appendChild(h("strong", "", "What will happen"));
  action.appendChild(h("p", "", "Everything in this conversation up to and including the highlighted message will be marked read."));
  action.appendChild(h("p", "slack-boundary-note", "Messages shown after the boundary stay unread."));
  action.appendChild(dl([
    ["Conversation", view.conversationLabel || view.conversationId],
    ["Unread now", view.currentUnreadCount ? String(view.currentUnreadCount) : ""],
    ["Account", mutation.account],
  ]));
  article.appendChild(action);

  const context = h("section", "slack-context");
  const contextHead = h("div", "slack-context-head");
  contextHead.appendChild(h("h4", "", view.contextLabel));
  contextHead.appendChild(h("span", "m", view.messages.length + " message" + V.plural(view.messages.length)));
  context.appendChild(contextHead);
  if (!view.messages.length) {
    context.appendChild(h("p", "slack-context-missing", "Conversation context was unavailable when this proposal was created. Verify the exact IDs below before approving."));
  } else {
    const transcript = h("div", "slack-transcript");
    for (const message of view.messages) {
      const row = h("article", "slack-msg" + (message.isTarget ? " target" : "") + (message.isAfterBoundary ? " after" : ""));
      row.appendChild(h("span", "avatar", V.senderInitial(message.actorName)));
      const copy = h("div", "slack-msg-copy");
      const head = h("div", "slack-msg-head");
      head.appendChild(h("strong", "", message.isFromMe ? "You" : message.actorName));
      if (message.sentAt) {
        const when = h("time", "", formatWhen(message.sentAt));
        when.title = fmtFull(message.sentAt);
        head.appendChild(when);
      }
      copy.appendChild(head);
      copy.appendChild(h("p", "slack-msg-text", message.text || "(no text)"));
      const tags = h("div", "slack-msg-tags");
      if (message.isTarget) tags.appendChild(h("span", "slack-read-through", "read through here"));
      if (message.isAfterBoundary) tags.appendChild(h("span", "slack-stays-unread", "stays unread"));
      if (tags.childNodes.length) copy.appendChild(tags);
      row.appendChild(copy);
      transcript.appendChild(row);
    }
    context.appendChild(transcript);
  }
  article.appendChild(context);

  const technical = details("Exact Slack target", "raw");
  technical.appendChild(dl([
    ["Conversation ID", view.conversationId],
    ["Message timestamp", view.messageTs],
    ["Thread timestamp", view.threadTs],
    ["Current read cursor", view.currentLastRead],
  ]));
  article.appendChild(technical);
  if (mutation.reason) article.appendChild(h("p", "mreason", mutation.reason));
  if (mutation.error) article.appendChild(h("p", "bad", mutation.error));
  return article;
}

function renderGeneric(mutation) {
  const article = h("article", "mut generic");
  article.appendChild(mutationHead(mutation.provider || "mutation", mutation.title || mutation.operation || "mutation", mutation.status));
  article.appendChild(h("p", "mmeta m", [mutation.status, mutation.operation, "for", mutation.account].filter(Boolean).join(" ")));
  if (mutation.reason) article.appendChild(h("p", "mreason", mutation.reason));
  if (mutation.error) article.appendChild(h("p", "bad", mutation.error));
  article.appendChild(h("div", "eyebrow", "payload")); article.appendChild(pre(mutation.payload || {}));
  article.appendChild(h("div", "eyebrow", "preview")); article.appendChild(pre(mutation.preview || {}));
  return article;
}

function renderMutation(request, mutation, actions) {
  if (V.isGmailEmailMutation(mutation)) return renderGmailEmail(request, mutation, actions);
  if (V.isCalendarMutation(mutation)) return renderCalendar(mutation);
  if (V.isAppleNotesMutation(mutation)) return renderAppleNotes(mutation);
  if (V.isSlackMarkReadMutation(mutation)) return renderSlackMarkRead(mutation);
  return renderGeneric(mutation);
}

// --- request context -----------------------------------------------------------------

function renderContext(ctxMap) {
  const c = V.splitRequestContext(ctxMap);
  if (c.empty) return null;
  const sect = h("section", "rsect");
  sect.appendChild(h("h2", "", "Context"));
  if (c.source) { const p = h("p"); p.appendChild(h("span", "lab", "Source ")); p.appendChild(document.createTextNode(c.source)); sect.appendChild(p); }
  if (c.note) sect.appendChild(h("p", "note", c.note));
  if (c.identifications.length) {
    sect.appendChild(h("div", "eyebrow", "Identifications"));
    const list = h("div", "idents");
    for (const item of c.identifications) {
      const v = V.identificationView(item);
      const card = h("article", "ident");
      const head = h("div", "ident-head");
      head.appendChild(h("strong", "", v.name));
      const chips = h("div", "ident-chips");
      if (v.confidence) chips.appendChild(h("span", "conf " + V.confidenceCSSClass(v.confidence), v.confidence + " confidence"));
      if (v.action) chips.appendChild(h("span", "chip mini", v.action));
      if (v.maskedPhone) chips.appendChild(h("code", "", v.maskedPhone));
      head.appendChild(chips); card.appendChild(head);
      if (v.evidence.length) { const ul = h("ul", "evidence"); v.evidence.forEach((line) => ul.appendChild(h("li", "", line))); card.appendChild(ul); }
      list.appendChild(card);
    }
    sect.appendChild(list);
  }
  if (Object.keys(c.leftover).length) { const raw = details("Other context", "raw"); raw.appendChild(pre(c.leftover)); sect.appendChild(raw); }
  return sect;
}

// --- the two pages -----------------------------------------------------------------

function renderRequestTable(title, requests, emptyText) {
  const sect = h("section", "rsect");
  sect.appendChild(h("h2", "", title));
  if (!requests.length) { sect.appendChild(h("p", "m", emptyText)); return sect; }
  const table = h("table", "rtable");
  const thead = h("thead"); const hr = h("tr");
  ["status", "request", "mutations", "created"].forEach((t) => hr.appendChild(h("th", "", t)));
  thead.appendChild(hr); table.appendChild(thead);
  const tbody = h("tbody");
  for (const request of requests) {
    const tr = h("tr");
    const st = h("td"); st.appendChild(pill(V.requestListStatus(request))); tr.appendChild(st);
    const td = h("td", "rtitle");
    td.appendChild(link(requestPath(request.id), request.title || request.id));
    if (request.reason) td.appendChild(h("div", "m", request.reason));
    tr.appendChild(td);
    tr.appendChild(h("td", "num", V.requestMutationCount(request)));
    const created = h("td", "m", formatWhen(request.created_at)); created.title = fmtFull(request.created_at);
    tr.appendChild(created);
    tbody.appendChild(tr);
  }
  table.appendChild(tbody); sect.appendChild(table);
  return sect;
}

async function renderList(root, ctx, alive) {
  ctx.setSubtitle("mutation review");
  ctx.setStats("");
  const refresh = button("refresh", "", () => renderList(root, ctx, alive));
  ctx.setControls(refresh);
  clear(root).appendChild(h("p", "m", "loading…"));
  let requests;
  try { requests = await mutations.list({ limit: 200 }); } catch (err) {
    if (!alive()) return;
    clear(root).appendChild(h("p", "bad", errorText(err) || "could not load requests"));
    return;
  }
  if (!alive()) return;
  const { pending, past } = V.splitRequestsForList(requests);
  ctx.setStats(pending.length + " pending · " + past.length + " past");
  clear(root);
  root.appendChild(renderRequestTable("Pending review", pending, "No requests are waiting for review."));
  root.appendChild(renderRequestTable("Past requests", past, "No approved or denied requests yet."));
}

function renderRequestHeader(request, actions) {
  const sect = h("section", "rsect rhead");
  const titleRow = h("div", "rtitle-row");
  titleRow.appendChild(h("h1", "", request.title || request.id));
  titleRow.appendChild(pill(V.requestListStatus(request)));
  sect.appendChild(titleRow);
  if (request.reason) sect.appendChild(h("p", "mreason", request.reason));
  sect.appendChild(dl([
    ["Requested by", request.requested_by], ["Created", fmtFull(request.created_at)],
    ["Approved by", request.approved_by], ["Approved", fmtFull(request.approved_at)],
    ["Executed", fmtFull(request.executed_at)], ["Updated", fmtFull(request.updated_at)],
  ]));
  if (request.error) sect.appendChild(h("p", "bad", request.error));
  const errorNode = h("p", "bad");

  if (request.status === "pending_review") {
    const row = h("div", "actions");
    const count = V.requestMutationCount(request);
    row.appendChild(button("Approve", "ok", async () => {
      if (!confirmDialog("Approve? " + count + " mutation" + V.plural(count) + " will run upstream.")) return;
      errorNode.textContent = "";
      try { await mutations.approve(request.id); mutationsChanged(); actions.reload(); } catch (err) { errorNode.textContent = errorText(err); }
    }));
    const reason = h("input"); reason.placeholder = "reason (optional)"; reason.className = "reason";
    row.appendChild(reason);
    row.appendChild(button("Deny", "danger", async () => {
      errorNode.textContent = "";
      try { await mutations.reject(request.id, reason.value.trim()); mutationsChanged(); actions.reload(); } catch (err) { errorNode.textContent = errorText(err); }
    }));
    sect.appendChild(row);
  }
  const supersededBy = V.trimStr(request.superseded_by);
  if (supersededBy) {
    const p = h("p", "superseded", "Superseded by ");
    p.appendChild(link(requestPath(supersededBy), supersededBy, "code"));
    p.appendChild(document.createTextNode(". This request is kept as the record of what failed."));
    sect.appendChild(p);
  } else if (request.can_supersede === true) {
    const row = h("div", "actions");
    const input = h("input"); input.placeholder = "replacement request id"; input.className = "reason";
    row.appendChild(input);
    row.appendChild(button("mark superseded", "", async () => {
      errorNode.textContent = "";
      try { await mutations.supersede(request.id, input.value.trim()); mutationsChanged(); actions.reload(); } catch (err) { errorNode.textContent = errorText(err); }
    }));
    sect.appendChild(row);
  }
  sect.appendChild(errorNode);
  return sect;
}

async function renderDetail(root, ctx, id, alive) {
  ctx.setSubtitle("mutation review");
  ctx.setControls(link(LIST_PATH, "← all requests"));
  clear(root).appendChild(h("p", "m", "loading…"));
  let request;
  try { request = await mutations.get(id); } catch (err) {
    if (!alive()) return;
    clear(root);
    root.appendChild(link(LIST_PATH, "← all requests", "back"));
    root.appendChild(h("p", "bad", errorText(err) || "could not load request"));
    return;
  }
  if (!alive()) return;
  const actions = { reload() { if (alive()) renderDetail(root, ctx, id, alive); } };
  const list = Array.isArray(request.mutations) ? request.mutations : [];
  ctx.setStats(list.length + " mutation" + V.plural(list.length) + " · " + V.requestListStatus(request));
  clear(root);
  root.appendChild(link(LIST_PATH, "← all requests", "back"));
  root.appendChild(renderRequestHeader(request, actions));
  const context = renderContext(request.context);
  if (context) root.appendChild(context);
  const sect = h("section", "rsect");
  sect.appendChild(h("h2", "", "Mutations"));
  if (!list.length) sect.appendChild(h("p", "m", "This request carries no mutations."));
  for (const item of V.groupMutations(list)) {
    if (item.kind === "gmail") sect.appendChild(renderGmailGroup(item));
    else if (item.kind === "contact") sect.appendChild(renderContactGroup(item));
    else sect.appendChild(renderMutation(request, item.mutation, actions));
  }
  root.appendChild(sect);
  root.scrollTop = 0;
}

export function mount(container, ctx) {
  const root = h("div", "mv");
  container.appendChild(root);
  // Each render gets its own generation so a slow response from a page the
  // user already left cannot paint over the current one.
  let generation = 0;
  function render(params) {
    const mine = ++generation;
    const alive = () => mine === generation;
    if (params && params.id) renderDetail(root, ctx, params.id, alive);
    else renderList(root, ctx, alive);
  }
  render(ctx.params);
  return {
    update(params) { render(params); },
    cleanup() { generation += 1; },
  };
}
