// Pure view-model helpers for the mutation-review view. Nothing in here
// touches the DOM, so every function runs under node and is covered by
// tests/mutations.test.mjs. They are ports of the helpers the old
// server-rendered review UI (app/internal/mutations/ui.go, calendar_ui.go)
// used, kept behaviourally identical so the SPA reads the same way.

// --- small coercions (the JSON is loosely typed, exactly like the Go side) --

export function str(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return "";
}

export function trimStr(value) { return str(value).trim(); }

export function asMap(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

export function mapSlice(value) {
  if (!Array.isArray(value)) return [];
  return value.filter((item) => item && typeof item === "object" && !Array.isArray(item) && Object.keys(item).length > 0);
}

export function stringSlice(value) {
  if (!Array.isArray(value)) return [];
  return value.map((item) => str(item)).filter((item) => item !== "");
}

export function normalizeStringSlice(values) {
  return (values || []).map((value) => str(value).trim()).filter((value) => value !== "");
}

export function intFromAny(value) {
  const n = typeof value === "number" ? value : parseInt(str(value), 10);
  return Number.isFinite(n) ? Math.trunc(n) : 0;
}

export function compactWhitespace(value) { return str(value).trim().split(/\s+/).filter(Boolean).join(" "); }

export function prettyJSON(value) {
  try { return JSON.stringify(value === undefined ? {} : value, null, 2); } catch (e) { return "{}"; }
}

export function plural(n) { return n === 1 ? "" : "s"; }

// --- request list -----------------------------------------------------------

export function displayRequestStatus(status) { return status === "rejected" ? "denied" : (status || ""); }

// A resolved failure must not read as an open one in the list, which is where
// a stale red row is actually noticed.
export function requestListStatus(request) {
  const status = displayRequestStatus(request.status);
  return trimStr(request.superseded_by) ? status + " (superseded)" : status;
}

export function splitRequestsForList(requests) {
  const pending = []; const past = [];
  for (const request of requests || []) (request.status === "pending_review" ? pending : past).push(request);
  return { pending, past };
}

export function requestMutationCount(request) {
  const count = intFromAny(request.mutation_count);
  return count || (Array.isArray(request.mutations) ? request.mutations.length : 0);
}

// --- mutation classification and grouping -----------------------------------

export const GMAIL_ARCHIVE = "gmail.archive_threads";
export const GMAIL_UNARCHIVE = "gmail.unarchive_threads";
export const GMAIL_MODIFY_THREAD_LABELS = "gmail.modify_thread_labels";
export const GMAIL_SEND_EMAIL = "gmail.send_email";
export const SLACK_MARK_CONVERSATION_READ = "slack.mark_conversation_read";
const CALENDAR_OPS = ["calendar.create_event", "calendar.update_event", "calendar.delete_event"];
const APPLE_NOTES_OPS = ["apple_notes.create_note", "apple_notes.update_note"];

export function isGmailThreadMutation(m) {
  return m.provider === "gmail" && (
    m.operation === GMAIL_ARCHIVE || m.operation === GMAIL_UNARCHIVE || m.operation === GMAIL_MODIFY_THREAD_LABELS
  );
}
export function isGmailEmailMutation(m) { return m.provider === "gmail" && m.operation === GMAIL_SEND_EMAIL; }
export function isContactMutation(m) {
  return m.provider === "google_people" || m.operation === "contacts.batch_mutation" || m.operation === "google_people.contacts";
}
export function isCalendarMutation(m) { return m.provider === "google_calendar" || CALENDAR_OPS.includes(m.operation); }
export function isAppleNotesMutation(m) { return m.provider === "apple_notes" || APPLE_NOTES_OPS.includes(m.operation); }
export function isSlackMarkReadMutation(m) {
  return m.provider === "slack" && m.operation === SLACK_MARK_CONVERSATION_READ;
}

// groupMutations mirrors renderMutationList: Gmail thread mutations group by
// operation+account+label changes, contact mutations by account, everything
// else stands alone. Groups keep the position of their first member and report
// "mixed" when members disagree on status.
export function groupMutations(mutations) {
  const items = []; const gmail = new Map(); const contacts = new Map();
  for (const mutation of mutations || []) {
    if (isGmailThreadMutation(mutation)) {
      const payload = asMap(mutation.payload);
      const labelChangeKey = mutation.operation === GMAIL_MODIFY_THREAD_LABELS
        ? JSON.stringify([
          stringSlice(payload.add_labels),
          stringSlice(payload.create_and_add_labels),
          stringSlice(payload.remove_labels),
        ])
        : "";
      const key = mutation.operation + "\0" + mutation.account + "\0" + labelChangeKey;
      let group = gmail.get(key);
      if (!group) {
        group = { kind: "gmail", operation: mutation.operation, account: mutation.account, status: mutation.status, mutations: [] };
        gmail.set(key, group); items.push(group);
      }
      if (group.status !== mutation.status) group.status = "mixed";
      group.mutations.push(mutation);
      continue;
    }
    if (isContactMutation(mutation)) {
      let group = contacts.get(mutation.account);
      if (!group) {
        group = { kind: "contact", account: mutation.account, status: mutation.status, mutations: [] };
        contacts.set(mutation.account, group); items.push(group);
      }
      if (group.status !== mutation.status) group.status = "mixed";
      group.mutations.push(mutation);
      continue;
    }
    items.push({ kind: "single", mutation });
  }
  return items;
}

// --- gmail: sender names, labels, bodies -----------------------------------

const GENERIC_SENDER_LOCALS = ["", "no-reply", "noreply", "notifications", "notification", "receipts", "receipt", "support", "hello"];
export function isGenericSenderLocal(value) { return GENERIC_SENDER_LOCALS.includes(str(value).trim().toLowerCase()); }

export function titleSenderName(value) {
  return str(value).replace(/[._-]/g, " ").split(/\s+/).filter(Boolean)
    .map((part) => part[0].toUpperCase() + part.slice(1).toLowerCase()).join(" ");
}

export function gmailSenderDisplayName(from, subject) {
  from = trimStr(from);
  const lt = from.indexOf("<");
  if (lt >= 0) {
    const displayName = from.slice(0, lt).trim().replace(/^"+|"+$/g, "");
    if (displayName) return displayName;
  }
  const address = from.replace(/^[<>]+|[<>]+$/g, "");
  const at = address.indexOf("@");
  const hasDomain = at >= 0;
  const local = hasDomain ? address.slice(0, at) : address;
  const domain = hasDomain ? address.slice(at + 1) : "";
  let domainName = domain;
  const dot = domainName.indexOf(".");
  if (dot > 0) domainName = domainName.slice(0, dot);
  if (domain.includes("uber")) return "Uber Receipts";
  if (domain.includes("hcb")) return "HCB";
  if (domain.includes("turbotenant")) return "TurboTenant";
  if (domain.includes("dinobox")) return "dinobox";
  if (hasDomain && !isGenericSenderLocal(local)) return titleSenderName(local);
  if (hasDomain && domainName !== "") return titleSenderName(domainName);
  if (subject) return subject;
  return from;
}

// gmailMessageSender labels one message's sender: the display name and the
// address together, because a review has to be able to check the address.
export function gmailMessageSender(message) {
  const address = trimStr(asMap(message).from_address);
  const name = trimStr(asMap(message).from_name);
  if (name && address && name !== address) return name + " <" + address + ">";
  return name || address || "Unknown sender";
}

export function senderInitial(value) {
  const m = /[A-Za-z0-9]/.exec(str(value).trim());
  return m ? m[0].toUpperCase() : "?";
}

export function truncateRunes(value, max) {
  if (max <= 0) return "";
  const runes = Array.from(str(value));
  if (runes.length <= max) return str(value);
  if (max <= 3) return runes.slice(0, max).join("");
  return runes.slice(0, max - 3).join("") + "...";
}

export function formatGmailLabel(value) {
  const normalized = trimStr(value);
  if (normalized === "" || normalized.startsWith("Label_")) return "";
  switch (normalized) {
    case "INBOX": case "TRASH": case "SPAM": case "CATEGORY_PERSONAL": return "";
    case "UNREAD": return "Unread";
    case "IMPORTANT": return "Important";
    case "STARRED": return "Starred";
    case "SENT": return "Sent";
    case "CATEGORY_UPDATES": return "Updates";
    case "CATEGORY_PROMOTIONS": return "Promotions";
    case "CATEGORY_SOCIAL": return "Social";
    case "CATEGORY_FORUMS": return "Forums";
  }
  return normalized.replace(/^CATEGORY_/, "").replace(/_/g, " ");
}

export function appendVisibleGmailLabels(existing, labels) {
  const out = existing.slice(); const seen = new Set(out);
  for (const label of labels || []) {
    const normalized = formatGmailLabel(label);
    if (!normalized || seen.has(normalized)) continue;
    out.push(normalized); seen.add(normalized);
  }
  return out;
}

export function hasGmailUnreadLabel(labels) {
  return (labels || []).some((label) => str(label).trim().replace(/ /g, "_").toUpperCase() === "UNREAD");
}

// gmailThreadSummary is everything the thread row needs, resolved once.
export function gmailThreadSummary(thread) {
  const subject = trimStr(thread.subject) || "(no subject)";
  const sender = trimStr(thread.latest_from_address) || "Unknown sender";
  const messages = mapSlice(thread.messages);
  const messageCount = intFromAny(thread.message_count) || messages.length;
  const rawLabels = stringSlice(thread.labels);
  let labels = intFromAny(thread.inbox_message_count) > 0 ? ["Inbox"] : [];
  labels = appendVisibleGmailLabels(labels, rawLabels);
  return {
    threadId: trimStr(thread.thread_id),
    subject,
    // The real Gmail display name when the preview carries it (the warehouse
    // lifts the From header out of payload_json); the address-derived guess
    // only when it does not.
    senderName: trimStr(thread.latest_from_name) || gmailSenderDisplayName(sender, subject),
    latestPreview: truncateRunes(trimStr(thread.latest_preview), 420),
    latestAt: trimStr(thread.latest_at),
    messages,
    messageCount,
    labels,
    unread: hasGmailUnreadLabel(rawLabels),
  };
}

// A message opens when it carries UNREAD; when the thread is unread but its
// messages carry no label_ids at all, the last one opens instead.
export function gmailMessageOpen(message, index, messages, threadUnread) {
  if (hasGmailUnreadLabel(stringSlice(message.label_ids))) return true;
  const hasLabelIDs = Object.prototype.hasOwnProperty.call(message, "label_ids");
  return threadUnread && !hasLabelIDs && index === messages.length - 1;
}

export function gmailMutationGroupThreads(mutations) {
  const threads = [];
  for (const mutation of mutations) {
    let mutationThreads = mapSlice(asMap(mutation.preview).threads);
    if (mutationThreads.length === 0) {
      mutationThreads = gmailMutationThreadIDs(mutation).map((thread_id) => ({ thread_id }));
    }
    threads.push(...mutationThreads);
  }
  return threads;
}

export function gmailMutationThreadIDs(mutation) {
  const ids = stringSlice(asMap(mutation.payload).thread_ids);
  if (ids.length) return ids;
  return mapSlice(asMap(mutation.preview).threads).map((t) => trimStr(t.thread_id)).filter(Boolean);
}

export function gmailMutationGroupTitle(verb, threadCount) { return verb + " " + threadCount + " Gmail thread" + plural(threadCount); }

export function gmailMutationGroupVerb(operation) {
  if (operation === GMAIL_UNARCHIVE) return "Unarchive";
  if (operation === GMAIL_MODIFY_THREAD_LABELS) return "Update labels";
  return "Archive";
}

function humanList(values) {
  values = stringSlice(values);
  if (values.length <= 1) return values.join("");
  if (values.length === 2) return values.join(" and ");
  return values.slice(0, -1).join(", ") + ", and " + values[values.length - 1];
}

export function gmailMutationLabelChanges(payload = {}) {
  payload = asMap(payload);
  return [
    { kind: "created", heading: "Create if missing + add", symbol: "+", labels: stringSlice(payload.create_and_add_labels) },
    { kind: "added", heading: "Labels added", symbol: "+", labels: stringSlice(payload.add_labels) },
    { kind: "removed", heading: "Labels removed", symbol: "−", labels: stringSlice(payload.remove_labels) },
  ];
}

export function gmailMutationGroupActionText(operation, threadCount, payload = {}) {
  const noun = threadCount === 1 ? "this thread" : "these threads";
  if (operation === GMAIL_UNARCHIVE) return "Restores " + noun + " to the Inbox.";
  if (operation === GMAIL_MODIFY_THREAD_LABELS) {
    const [created, added, removed] = gmailMutationLabelChanges(payload);
    const createAndAdd = humanList(created.labels);
    const add = humanList(added.labels);
    const remove = humanList(removed.labels);
    const changes = [];
    if (createAndAdd) changes.push("Creates and adds " + createAndAdd);
    if (add) changes.push((changes.length ? "adds " : "Adds ") + add);
    if (remove) changes.push((changes.length ? "removes " : "Removes ") + remove);
    let summary = changes.join(" and ");
    if (changes.length > 2) summary = changes.slice(0, -1).join(", ") + ", and " + changes[changes.length - 1];
    return summary + " on " + noun + ".";
  }
  return "Removes " + noun + " from the Inbox.";
}

export function gmailMutationGroupStatus(status, mutationCount) {
  status = status || "unknown";
  return mutationCount <= 1 ? status : mutationCount + " " + status;
}

export function gmailEmailTitle(deliveryMode) { return deliveryMode === "draft" ? "Create Gmail draft" : "Send Gmail email"; }
export function gmailEmailActionText(deliveryMode) {
  return deliveryMode === "draft" ? "Creates a Gmail draft after approval." : "Will send this email after approval.";
}

const ENTITIES = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'", nbsp: " " };
export function unescapeHTML(value) {
  return str(value).replace(/&(#x[0-9a-f]+|#[0-9]+|[a-z]+);/gi, (whole, body) => {
    if (body[0] === "#") {
      const code = body[1].toLowerCase() === "x" ? parseInt(body.slice(2), 16) : parseInt(body.slice(1), 10);
      return Number.isFinite(code) ? String.fromCodePoint(code) : whole;
    }
    const named = ENTITIES[body.toLowerCase()];
    return named !== undefined ? named : whole;
  });
}

// htmlFragmentText is the Go helper of the same name: strip tags, turning
// <br>, </div> and </p> into line breaks, then drop blank lines.
export function htmlFragmentText(value) {
  value = str(value).replace(/\r\n/g, "\n");
  let out = ""; let index = 0;
  while (index < value.length) {
    if (value[index] !== "<") { out += value[index]; index += 1; continue; }
    const end = value.indexOf(">", index);
    if (end < 0) break;
    const tag = value.slice(index, end + 1).toLowerCase();
    if (tag.startsWith("<br") || tag.startsWith("</div") || tag.startsWith("</p")) out += "\n";
    index = end + 1;
  }
  return normalizeStringSlice(unescapeHTML(out).split("\n")).join("\n");
}

// gmailBodyFrameHeight estimates a sandboxed iframe's height from the text
// it will show, since a sandboxed frame cannot report its own scroll height.
export function gmailBodyFrameHeight(bodyHTML, quoted) {
  const lines = normalizeStringSlice(htmlFragmentText(bodyHTML).split("\n"));
  let visualLines = 1;
  if (lines.length > 0) {
    visualLines = 0;
    for (const line of lines) visualLines += Math.max(1, Math.floor((Array.from(line).length + 89) / 90));
  }
  let height = 76 + visualLines * 20;
  const lower = str(bodyHTML).toLowerCase();
  if (lower.includes("<img")) height += 140;
  if (lower.includes("<table")) height += 24;
  const [minHeight, maxHeight] = quoted ? [112, 200] : [128, 240];
  return Math.min(maxHeight, Math.max(minHeight, height));
}

export function emailHTMLDocument(bodyHTML) {
  return '<!doctype html><html><head><base target="_blank"><style>html,body{margin:0;padding:0;background:white;color:#202124;font-family:Arial,sans-serif;}img{max-width:100%;height:auto;}table{max-width:100%;}a{color:#1a73e8;}</style></head><body>'
    + str(bodyHTML) + "</body></html>";
}

export function splitGmailQuotedHTML(bodyHTML) {
  bodyHTML = trimStr(bodyHTML);
  for (const pattern of ['<div class="gmail_quote gmail_quote_container"', '<div class="gmail_quote"', '<blockquote class="gmail_quote"']) {
    const index = bodyHTML.indexOf(pattern);
    if (index > 0) return { body: bodyHTML.slice(0, index).trim(), quoted: bodyHTML.slice(index).trim() };
  }
  return { body: bodyHTML, quoted: "" };
}

export function parsePreviewTime(value) {
  value = trimStr(value);
  if (!value) return null;
  const normalized = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$/.test(value) ? value.replace(" ", "T") + "Z" : value;
  const d = new Date(normalized);
  return isNaN(d.getTime()) ? null : d;
}

const MONTHS_SHORT = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
export function formatGmailCompactTime(value) {
  const d = parsePreviewTime(value);
  return d ? MONTHS_SHORT[d.getUTCMonth()] + " " + d.getUTCDate() : str(value);
}
export function formatGmailFullTime(value) {
  const d = parsePreviewTime(value);
  if (!d) return trimStr(value);
  const p2 = (n) => (n < 10 ? "0" : "") + n;
  return MONTHS_SHORT[d.getUTCMonth()] + " " + d.getUTCDate() + ", " + d.getUTCFullYear() + " " + p2(d.getUTCHours()) + ":" + p2(d.getUTCMinutes()) + " UTC";
}

// normalizeEmailText is the old composer's text normalizer: CRLF to LF, trailing
// whitespace off each line, blank lines only kept when interior.
export function normalizeEmailText(value) {
  return str(value).replace(/\r\n/g, "\n").split("\n").map((line) => line.replace(/\s+$/, ""))
    .filter((line, index, lines) => line.trim() !== "" || (index > 0 && index < lines.length - 1)).join("\n").trim();
}

// assembleEmailBody is what the old form's editor sync produced for the
// hidden body_html/body_text fields: editor, then signature, then the quoted
// thread, in that order, separated by an empty div (HTML) or a blank line
// (text). The server splits the stored body on exactly those seams next time
// it builds the editor view, so the order is load-bearing.
export function assembleEmailBody({ editorHTML, editorText, signatureHTML, signatureText, quotedHTML, quotedText }) {
  const body_html = [trimStr(editorHTML), trimStr(signatureHTML), trimStr(quotedHTML)].filter(Boolean).join("<div><br></div>");
  const body_text = [str(editorText).replace(/\s+$/, ""), normalizeEmailText(signatureText), normalizeEmailText(quotedText)].filter(Boolean).join("\n\n") + "\n";
  return { body_html, body_text };
}

export function splitEmailAddressList(value) { return normalizeStringSlice(str(value).split(/[,\n\r]/)); }

// --- contacts ---------------------------------------------------------------

export function canonicalContactOp(op) {
  switch (op) {
    case "create": return "create_contact";
    case "update": return "update_contact";
    case "delete": return "delete_contact";
  }
  return op;
}

function emptySummary(s) { return !s.displayName && !s.primaryEmail && !s.primaryPhone && !s.organization; }

export function contactOperationSummary(operation) {
  operation = asMap(operation);
  let summary = contactSummaryFromSummaryMap(asMap(operation.summary));
  if (!emptySummary(summary)) return summary;
  for (const key of ["person", "after", "before"]) {
    summary = contactSummaryFromPerson(asMap(operation[key]));
    if (!emptySummary(summary)) return summary;
  }
  summary = contactSummaryFromFlatOperation(operation);
  if (!emptySummary(summary)) return summary;
  return { displayName: "", primaryEmail: "", primaryPhone: "", organization: "" };
}

export function contactSummaryFromSummaryMap(value) {
  return {
    displayName: trimStr(value.display_name), primaryEmail: trimStr(value.primary_email),
    primaryPhone: trimStr(value.primary_phone), organization: trimStr(value.organization),
  };
}

function contactPersonValue(person, ...keys) {
  for (const key of keys) if (Object.prototype.hasOwnProperty.call(person, key)) return person[key];
  return null;
}

export function contactSummaryFromPerson(person) {
  person = asMap(person);
  return {
    displayName: firstContactFieldValue(person.names, "displayName", "unstructuredName", "givenName"),
    primaryEmail: firstContactFieldValue(contactPersonValue(person, "emailAddresses", "email_addresses", "emails"), "value"),
    primaryPhone: firstContactFieldValue(contactPersonValue(person, "phoneNumbers", "phone_numbers", "phones"), "canonicalForm", "value"),
    organization: contactOrganizationSummary(contactPersonValue(person, "organizations")),
  };
}

// Some proposers put the fields straight on the operation instead of inside a
// Google Person; both shapes must read the same.
export function contactSummaryFromFlatOperation(operation) {
  let displayName = trimStr(operation.display_name);
  if (!displayName) displayName = (trimStr(operation.given_name) + " " + trimStr(operation.family_name)).trim();
  let organization = trimStr(operation.organization);
  const jobTitle = trimStr(operation.job_title);
  if (jobTitle) organization = organization ? jobTitle + ", " + organization : jobTitle;
  return { displayName, primaryEmail: trimStr(operation.primary_email), primaryPhone: trimStr(operation.primary_phone), organization };
}

export function personFromFlatOperation(operation) {
  operation = asMap(operation);
  const person = {};
  const given = trimStr(operation.given_name); const family = trimStr(operation.family_name);
  let displayName = trimStr(operation.display_name);
  if (!displayName) displayName = (given + " " + family).trim();
  if (displayName || given || family) {
    const name = {};
    if (displayName) name.displayName = displayName;
    if (given) name.givenName = given;
    if (family) name.familyName = family;
    person.names = [name];
  }
  const email = trimStr(operation.primary_email);
  if (email) person.emailAddresses = [{ value: email }];
  const phone = trimStr(operation.primary_phone);
  if (phone) person.phoneNumbers = [{ canonicalForm: phone, value: phone }];
  const organization = trimStr(operation.organization); const jobTitle = trimStr(operation.job_title);
  if (organization || jobTitle) {
    const org = {};
    if (organization) org.name = organization;
    if (jobTitle) org.title = jobTitle;
    person.organizations = [org];
  }
  return person;
}

export function contactOperationTitle(op) {
  switch (op) {
    case "create_contact": return "Create contact";
    case "update_contact": return "Update contact";
    case "delete_contact": return "Delete contact";
  }
  return "Change contact";
}

export function contactSummaryTitle(summary, operation) {
  for (const value of [summary.displayName, summary.primaryEmail, trimStr(asMap(operation).resource_name)]) if (value) return value;
  return "Unnamed contact";
}

export function contactClearedFields(operation) { return stringSlice(asMap(operation).clear_person_fields); }

export function contactUpdateFields(operation) {
  operation = asMap(operation);
  for (const key of ["update_person_fields", "updatePersonFields"]) {
    const value = operation[key];
    const values = stringSlice(value);
    if (values.length) return values;
    const text = trimStr(value);
    if (text) return normalizeStringSlice(text.split(","));
  }
  return [];
}

// contactEffect describes what approving the operation does. A masked field
// with no incoming value is a wipe, not an edit, so cleared fields are named
// separately: that sentence is the only thing between an approval click and
// silently emptying a field in Google Contacts.
export function contactEffect(operation, op) {
  const cleared = contactClearedFields(operation);
  if (op === "update_contact") {
    const replaced = contactUpdateFields(operation).filter((field) => !cleared.includes(field));
    return { replaced, cleared, sentence: "" };
  }
  if (op === "delete_contact") return { replaced: [], cleared: [], sentence: "Deletes this contact from Google Contacts." };
  if (op === "create_contact") return { replaced: [], cleared: [], sentence: "Creates a new Google Contact." };
  return { replaced: [], cleared: [], sentence: "" };
}

// contactEtagWarning: the executor refuses an operation whose contact has
// moved past the state the proposal was built on, so approving one can only
// produce a failed run. Say so at review time instead.
export function contactEtagWarning(operation, op) {
  if (op !== "update_contact" && op !== "delete_contact") return "";
  operation = asMap(operation);
  if (operation.contact_found === false) return "This contact is not in the synced Google Contacts copy, so the change cannot be previewed.";
  if (operation.etag_is_current === false) return "This contact changed since this was proposed, so the change will be refused. Re-propose it against the current contact.";
  return "";
}

export function contactFieldDisplayValue(value) { return contactFieldValueSummary(value) || "Not set"; }

export function contactFieldValueSummary(value) {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.map((item) => contactFieldMapSummary(asMap(item))).filter(Boolean).join("; ");
  if (typeof value === "object") return contactFieldMapSummary(value);
  return compactWhitespace(str(value));
}

export function contactFieldMapSummary(value) {
  const organization = contactOrganizationSummary([value]);
  if (organization) return organization;
  for (const keys of [["displayName", "unstructuredName", "givenName"], ["canonicalForm", "value"], ["value"], ["name"], ["title"]]) {
    const text = firstContactValueFromMap(value, ...keys);
    if (text) return text;
  }
  return compactWhitespace(prettyJSON(value));
}

export function firstContactFieldValue(value, ...keys) {
  for (const item of mapSlice(value)) {
    const text = firstContactValueFromMap(item, ...keys);
    if (text) return text;
  }
  return "";
}

export function firstContactValueFromMap(value, ...keys) {
  for (const key of keys) {
    const text = trimStr(asMap(value)[key]);
    if (text) return text;
  }
  return "";
}

export function contactOrganizationSummary(value) {
  for (const item of mapSlice(value)) {
    const name = trimStr(item.name); const title = trimStr(item.title);
    if (name && title) return title + ", " + name;
    if (name) return name;
    if (title) return title;
  }
  return "";
}

export function contactMutationGroupOperations(mutations) {
  const out = [];
  for (const mutation of mutations) {
    let ops = mapSlice(asMap(mutation.preview).operations);
    if (ops.length === 0) ops = mapSlice(asMap(mutation.payload).operations);
    for (const operation of ops) out.push({ operation, mutation });
  }
  return out;
}

export function contactMutationGroupTitle(count) { return "Apply " + count + " contact change" + plural(count); }

export function contactOperationClass(op) {
  switch (op) {
    case "delete_contact": return "destructive";
    case "create_contact": return "creating";
    case "update_contact": return "updating";
  }
  return "";
}

// --- request context --------------------------------------------------------

export function splitRequestContext(ctx) {
  ctx = asMap(ctx);
  const source = trimStr(ctx.source); const note = trimStr(ctx.note);
  const identifications = mapSlice(ctx.identifications);
  const handled = new Set();
  if (source) handled.add("source");
  if (note) handled.add("note");
  if (identifications.length) handled.add("identifications");
  const leftover = {};
  for (const key of Object.keys(ctx)) if (!handled.has(key)) leftover[key] = ctx[key];
  const empty = !source && !note && !identifications.length && Object.keys(leftover).length === 0;
  return { source, note, identifications, leftover, empty };
}

export function identificationView(item) {
  item = asMap(item);
  const name = trimStr(item.inferred_name) || trimStr(item.name) || trimStr(item.display_name) || "Unidentified";
  return {
    name, confidence: trimStr(item.confidence), action: trimStr(item.action), maskedPhone: trimStr(item.masked_phone),
    evidence: stringSlice(item.evidence).map((line) => line.trim()).filter(Boolean),
  };
}

export function confidenceCSSClass(confidence) {
  switch (trimStr(confidence).toLowerCase()) {
    case "high": return "confidence-high";
    case "medium-high": return "confidence-medium-high";
    case "medium": return "confidence-medium";
    case "medium-low": return "confidence-medium-low";
    case "low": return "confidence-low";
  }
  return "";
}

// --- calendar ---------------------------------------------------------------

export function calendarOperation(mutation) {
  const preview = asMap(asMap(mutation.preview).event);
  let operation = str(preview.operation).toLowerCase();
  if (!operation) {
    switch (mutation.operation) {
      case "calendar.create_event": operation = "create"; break;
      case "calendar.update_event": operation = "update"; break;
      case "calendar.delete_event": operation = "delete"; break;
    }
  }
  return operation;
}

export function calendarVerbForOperation(op) {
  switch (op) {
    case "create": return "Create event";
    case "update": return "Update event";
    case "delete": return "Delete event";
  }
  return "Change event";
}

export function calendarTitle(mutation, preview, operation, verb) {
  let title = trimStr(preview.summary);
  if (operation === "update" && !title) title = trimStr(asMap(asMap(mutation.payload).patch).summary);
  if (operation === "delete" && !title) title = "Cancel this event";
  if (!title) title = trimStr(mutation.title);
  if (!title) title = verb + " calendar event";
  return title;
}

export function calendarStatusClass(status) {
  switch (status) {
    case "pending_review": return "pending";
    case "approved": return "approved";
    case "succeeded": case "observed": return "succeeded";
    case "rejected": case "failed_terminal": case "blocked_missing_credentials": return "failed";
    case "failed_retryable": return "retry";
  }
  return "";
}

export function humanStatus(status) {
  switch (status) {
    case "pending_review": return "Pending review";
    case "blocked_missing_credentials": return "Blocked";
    case "failed_retryable": return "Retrying";
    case "failed_terminal": return "Failed";
  }
  if (!status) return "";
  return status[0].toUpperCase() + status.slice(1);
}

export function notifyClass(value) { return value === "none" ? "off" : value === "externalOnly" ? "external" : "all"; }
export function notifyIcon(value) { return value === "none" ? "🔕" : "🔔"; }
export function sendUpdatesLabel(value) {
  return value === "none" ? "Don't notify" : value === "externalOnly" ? "External guests only" : "All guests";
}
export function deleteNotificationSentence(value) {
  return value === "none" ? "Guests will not be notified." : value === "externalOnly" ? "External guests will be notified." : "Guests will be notified.";
}

export function humanFieldName(key) {
  const known = { summary: "Summary", description: "Description", location: "Location", start: "Start", end: "End", attendees: "Guests", recurrence: "Recurrence" };
  if (known[key]) return known[key];
  return key ? key[0].toUpperCase() + key.slice(1) : key;
}

export function humanResponseStatus(status) {
  switch (status) {
    case "accepted": return "Accepted";
    case "declined": return "Declined";
    case "tentative": return "Maybe";
    case "needsAction": return "Awaiting reply";
  }
  return status;
}

export function calendarInitials(name, email) {
  let source = trimStr(name) || trimStr(email);
  if (!source) return "?";
  const at = source.indexOf("@");
  if (at >= 0) source = source.slice(0, at);
  const fields = source.replace(/[._-]/g, " ").split(/\s+/).filter(Boolean);
  if (fields.length === 0) return Array.from(source)[0].toUpperCase();
  if (fields.length === 1) return Array.from(fields[0])[0].toUpperCase();
  return (Array.from(fields[0])[0] + Array.from(fields[fields.length - 1])[0]).toUpperCase();
}

const AVATAR_PALETTE = ["a", "b", "c", "d", "e", "f", "g", "h"];
export function calendarAvatarColor(seed) {
  if (!seed) return AVATAR_PALETTE[0];
  let sum = 0;
  for (const ch of seed) sum += ch.codePointAt(0);
  return "color-" + AVATAR_PALETTE[sum % AVATAR_PALETTE.length];
}

// parseCalendarTime reads a Google Calendar start/end object. An all-day
// value is a bare date (year/month/day, no instant); a timed value is an
// instant. A naive dateTime with no offset is read as the browser's local
// time — the Go renderer resolved the event's IANA timeZone there, which the
// browser cannot do without a tz database, and the only such payloads are
// hand-written proposals.
export function parseCalendarTime(value) {
  value = asMap(value);
  const timezone = trimStr(value.timeZone);
  const date = trimStr(value.date);
  if (date) {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(date);
    if (m) return { allDay: true, date: { year: +m[1], month: +m[2], day: +m[3] }, time: null, timezone: "" };
  }
  const dt = trimStr(value.dateTime);
  if (dt) {
    const parsed = new Date(dt);
    if (!isNaN(parsed.getTime())) return { allDay: false, date: null, time: parsed, timezone };
  }
  return null;
}

const DAY_MS = 86400000;
function dateToUTC(d) { return Date.UTC(d.year, d.month - 1, d.day); }
export function addDays(d, n) {
  const shifted = new Date(dateToUTC(d) + n * DAY_MS);
  return { year: shifted.getUTCFullYear(), month: shifted.getUTCMonth() + 1, day: shifted.getUTCDate() };
}
export function sameDate(a, b) { return a.year === b.year && a.month === b.month && a.day === b.day; }

// localParts breaks an instant into the reviewer's local calendar fields; an
// explicit timeZone makes the output deterministic for tests.
export function localParts(instant, timeZone) {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: timeZone || undefined, hour12: false, weekday: "long", year: "numeric", month: "long", day: "numeric",
    hour: "numeric", minute: "2-digit", timeZoneName: "short",
  });
  const parts = {};
  for (const part of fmt.formatToParts(instant)) parts[part.type] = part.value;
  const hour = parseInt(parts.hour, 10) % 24; // some ICU builds print midnight as "24"
  return { weekday: parts.weekday, month: parts.month, day: parseInt(parts.day, 10), year: parseInt(parts.year, 10), hour, minute: parseInt(parts.minute, 10), zone: parts.timeZoneName || "" };
}

const MONTHS_LONG = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
const WEEKDAYS_LONG = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

// formatFullDate accepts either a bare {year, month, day} (all-day) or an
// instant plus timeZone: "Wednesday, January 2, 2030".
export function formatFullDate(value, timeZone) {
  if (value instanceof Date) {
    const p = localParts(value, timeZone);
    return p.weekday + ", " + p.month + " " + p.day + ", " + p.year;
  }
  const weekday = WEEKDAYS_LONG[new Date(dateToUTC(value)).getUTCDay()];
  return weekday + ", " + MONTHS_LONG[value.month - 1] + " " + value.day + ", " + value.year;
}

export function formatTimeOfDay(instant, timeZone) {
  const p = localParts(instant, timeZone);
  const period = p.hour >= 12 ? "PM" : "AM";
  const hour12 = p.hour % 12 === 0 ? 12 : p.hour % 12;
  return hour12 + ":" + (p.minute < 10 ? "0" : "") + p.minute + " " + period;
}

export function calendarTimeZoneLabel(instant, timeZone) { return localParts(instant, timeZone).zone; }

// formatTimeRange: "6:00 – 6:30 PM UTC", merging the AM/PM when both ends share it.
export function formatTimeRange(start, end, timeZone) {
  let startStr = formatTimeOfDay(start, timeZone);
  const startTz = calendarTimeZoneLabel(start, timeZone);
  if (!end) return startTz ? startStr + " " + startTz : startStr;
  const endStr = formatTimeOfDay(end, timeZone);
  const endTz = calendarTimeZoneLabel(end, timeZone);
  const tz = startTz || endTz;
  if (startTz && endTz && startTz !== endTz) return startStr + " " + startTz + " – " + endStr + " " + endTz;
  if (startStr.endsWith(" AM") && endStr.endsWith(" AM")) startStr = startStr.slice(0, -3);
  else if (startStr.endsWith(" PM") && endStr.endsWith(" PM")) startStr = startStr.slice(0, -3);
  return tz ? startStr + " – " + endStr + " " + tz : startStr + " – " + endStr;
}

// calendarWhen is the hero "when" block: {date, time} strings, or null.
export function calendarWhen(preview, timeZone) {
  preview = asMap(preview);
  const start = parseCalendarTime(preview.start); const end = parseCalendarTime(preview.end);
  if (!start && !end) return null;
  if (start && start.allDay) {
    let time = "All day";
    if (end && end.allDay) {
      // Google's all-day end date is exclusive; show the inclusive range.
      const inclusive = addDays(end.date, -1);
      if (!sameDate(inclusive, start.date)) time = "All day · through " + formatFullDate(inclusive);
    }
    return { date: formatFullDate(start.date), time };
  }
  if (start && start.time) {
    return { date: formatFullDate(start.time, timeZone), time: formatTimeRange(start.time, end && end.time ? end.time : null, timeZone) };
  }
  return null;
}

export function formatCalendarPatchTime(value, timeZone) {
  const parsed = parseCalendarTime(value);
  if (!parsed) return "";
  if (parsed.allDay) return formatFullDate(parsed.date) + " (all day)";
  return formatFullDate(parsed.time, timeZone) + " " + formatTimeOfDay(parsed.time, timeZone) + " " + calendarTimeZoneLabel(parsed.time, timeZone);
}

// calendarPatchValue renders one patch field: {text} for a readable value,
// {json} when only the raw structure is honest.
export function calendarPatchValue(key, value, timeZone) {
  if (typeof value === "string") return { text: value };
  if (Array.isArray(value)) {
    if (key === "recurrence") return { text: humanRecurrence(stringSliceForList(value)) };
    if (key === "attendees") return { text: mapSlice(value).map((m) => trimStr(m.email)).filter(Boolean).join(", ") };
  } else if (value && typeof value === "object" && (key === "start" || key === "end")) {
    const formatted = formatCalendarPatchTime(value, timeZone);
    if (formatted) return { text: formatted };
  }
  return { json: prettyJSON(value) };
}

export function stringSliceForList(value) { return Array.isArray(value) ? normalizeStringSlice(value.map(str)) : []; }

export function humanRecurrence(rules) {
  return (rules || []).map((raw) => humanSingleRecurrence(raw) || raw).join("; ");
}

function formatUntil(until) {
  const m = /^(\d{4})(\d{2})(\d{2})(?:T\d{6}Z)?$/.exec(until);
  if (!m) return "";
  return MONTHS_LONG[+m[2] - 1] + " " + (+m[3]) + ", " + m[1];
}

export function humanSingleRecurrence(raw) {
  raw = str(raw);
  if (!raw.startsWith("RRULE:")) return "";
  const props = {};
  for (const segment of raw.slice(6).split(";")) {
    const eq = segment.indexOf("=");
    if (eq > 0) props[segment.slice(0, eq).toUpperCase()] = segment.slice(eq + 1);
  }
  const freq = str(props.FREQ).toUpperCase(); const interval = str(props.INTERVAL); const byDay = str(props.BYDAY);
  let base = "";
  switch (freq) {
    case "DAILY":
      base = "Daily";
      if (byDay === "MO,TU,WE,TH,FR") base = "Every weekday";
      else if (interval && interval !== "1") base = "Every " + interval + " days";
      break;
    case "WEEKLY":
      base = interval && interval !== "1" ? "Every " + interval + " weeks" : "Weekly";
      if (byDay && byDay !== "MO,TU,WE,TH,FR") base += " on " + humanByDay(byDay);
      break;
    case "MONTHLY":
      base = interval && interval !== "1" ? "Every " + interval + " months" : "Monthly";
      break;
    case "YEARLY":
      base = "Annually";
      break;
    default:
      return "";
  }
  const count = trimStr(props.COUNT); const until = trimStr(props.UNTIL);
  if (count) base += ", for " + count + " occurrences";
  else if (until) {
    const formatted = formatUntil(until);
    if (formatted) base += ", until " + formatted;
  }
  return base;
}

export function humanByDay(spec) {
  const names = { MO: "Monday", TU: "Tuesday", WE: "Wednesday", TH: "Thursday", FR: "Friday", SA: "Saturday", SU: "Sunday" };
  const out = str(spec).split(",").map((code) => names[code.trim().toUpperCase()]).filter(Boolean);
  if (out.length === 0) return spec;
  if (out.length === 1) return out[0];
  return out.slice(0, -1).join(", ") + " and " + out[out.length - 1];
}

export function calendarAttendees(value) {
  return mapSlice(value).map((item) => ({
    email: trimStr(item.email), displayName: trimStr(item.displayName), responseStatus: trimStr(item.responseStatus),
    organizer: item.organizer === true, optional: item.optional === true,
  })).filter((a) => a.email);
}

// autolinkSegments splits prose into text and URL segments so the DOM layer
// can build anchors without innerHTML. Trailing punctuation stays prose.
const URL_PATTERN = /https?:\/\/[^\s<>"']+/g;
export function autolinkSegments(text) {
  text = str(text);
  const out = []; let cursor = 0;
  for (const match of text.matchAll(URL_PATTERN)) {
    const start = match.index; const end = start + match[0].length;
    let trimmed = end;
    while (trimmed > start && ".,;:!?)\"'".includes(text[trimmed - 1])) trimmed -= 1;
    if (start > cursor) out.push({ text: text.slice(cursor, start) });
    out.push({ url: text.slice(start, trimmed) });
    if (trimmed < end) out.push({ text: text.slice(trimmed, end) });
    cursor = end;
  }
  if (cursor < text.length) out.push({ text: text.slice(cursor) });
  return out;
}

// --- apple notes -------------------------------------------------------------

export function appleNotesView(mutation) {
  const note = asMap(asMap(mutation.preview).note);
  return {
    heading: str(note.action) === "create" ? "Create Apple Note" : "Update Apple Note",
    name: str(note.name), folder: str(note.folder), noteId: str(note.note_id),
    changes: stringSlice(note.changes), bodyPreview: str(note.body_preview),
  };
}

// --- slack -------------------------------------------------------------------

export function slackMarkReadView(mutation) {
  const payload = asMap(mutation.payload);
  const preview = asMap(asMap(mutation.preview).slack_read);
  const conversationType = trimStr(preview.conversation_type);
  let conversationLabel = trimStr(preview.conversation_name) || trimStr(preview.conversation_id) || trimStr(payload.conversation_id);
  if ((conversationType === "public_channel" || conversationType === "private_channel") && conversationLabel && !conversationLabel.startsWith("#")) {
    conversationLabel = "#" + conversationLabel;
  }
  const messages = mapSlice(preview.messages).map((message) => {
    const position = trimStr(message.position) || (message.is_target === true ? "target" : "before");
    return {
      messageTs: trimStr(message.message_ts),
      sentAt: trimStr(message.sent_at),
      userId: trimStr(message.user_id),
      actorName: trimStr(message.actor_name) || "Unknown",
      text: str(message.text),
      isTarget: message.is_target === true,
      isFromMe: message.is_from_me === true,
      position,
      isAfterBoundary: position === "after",
      avatarUrl: trimStr(message.avatar_url),
      open: deepLink(message.open),
    };
  });
  const targetMessage = messages.find((message) => message.isTarget || message.position === "target") || null;
  if (conversationType === "im" && /^U[A-Z0-9]+$/i.test(conversationLabel) && targetMessage && !targetMessage.isFromMe && targetMessage.actorName !== "Unknown") {
    conversationLabel = targetMessage.actorName;
  } else if (conversationType === "mpim" && conversationLabel.startsWith("mpdm-")) {
    const selfNames = new Set(messages.filter((message) => message.isFromMe).flatMap((message) => {
      const name = compactWhitespace(message.actorName).toLowerCase();
      return name ? [name, name.replaceAll(" ", "."), name.replaceAll(" ", "")] : [];
    }));
    const participants = conversationLabel.slice(5).replace(/-\d+$/, "").split("--")
      .filter((name) => name && !selfNames.has(name.toLowerCase()))
      .map((name) => name.replaceAll(".", " "));
    if (participants.length) conversationLabel = participants.join(", ");
  }
  const contextKind = trimStr(preview.context_kind) || "conversation";
  return {
    heading: "Mark Slack conversation read",
    conversationId: trimStr(preview.conversation_id) || trimStr(payload.conversation_id),
    messageTs: trimStr(preview.message_ts) || trimStr(payload.message_ts),
    effect: trimStr(preview.effect) || "Moves the entire conversation read cursor through this message.",
    conversationType,
    conversationLabel,
    currentUnreadCount: intFromAny(preview.current_unread_count),
    currentLastRead: trimStr(preview.current_last_read),
    contextKind,
    contextLabel: contextKind === "thread" ? "Thread context" : "Conversation context",
    threadTs: trimStr(preview.thread_ts),
    avatarUrl: trimStr(preview.avatar_url) || (targetMessage ? targetMessage.avatarUrl : ""),
    open: deepLink(preview.open) || (targetMessage ? targetMessage.open : null),
    messages,
    targetMessage,
  };
}

// The link that opens a record in the app it came from, as both surfaces
// receive it. Only an https url is usable in a browser; app_url is the phone's
// native scheme and is ignored here.
export function deepLink(value) {
  const link = asMap(value);
  const url = trimStr(link.url);
  return url ? { url, label: trimStr(link.label) || "Slack" } : null;
}

const SLACK_REVIEW_GROUPS = [
  { key: "direct", label: "Direct messages", description: "One-to-one conversations", icon: "@", types: ["im"] },
  { key: "group", label: "Group DMs", description: "Small-group conversations", icon: "◎", types: ["mpim"] },
  { key: "private", label: "Private channels", description: "Private workspace channels", icon: "◈", types: ["private_channel"] },
  { key: "public", label: "Public channels", description: "Public workspace channels", icon: "#", types: ["public_channel"] },
  { key: "other", label: "Other conversations", description: "Slack conversations", icon: "•", types: [] },
];

// Large mark-read proposals are only reviewable when the 100+ nearly-identical
// actions scan as compact conversation groups. Keep the presentation grouping
// here so the web and mobile renderers can remain mechanical.
export function slackMarkReadGroups(mutations) {
  const groups = SLACK_REVIEW_GROUPS.map((definition) => ({ ...definition, items: [] }));
  for (const mutation of mutations || []) {
    if (!isSlackMarkReadMutation(mutation)) continue;
    const view = slackMarkReadView(mutation);
    const group = groups.find((candidate) => candidate.types.includes(view.conversationType)) || groups[groups.length - 1];
    group.items.push({ mutation, view });
  }
  return groups.filter((group) => group.items.length > 0);
}

const REVIEW_COUNT_DEFINITIONS = [
  ["generic_channel", "Generic channels", "#"],
  ["automated_dm", "Automated DMs", "⚙"],
  ["terminal_direct", "Direct acknowledgements", "@"],
  ["terminal_group", "Group acknowledgements", "◎"],
];

function humanContextCountLabel(key) {
  return compactWhitespace(key.replaceAll("_", " ")).replace(/^./, (c) => c.toUpperCase());
}

export function mutationReviewContext(value) {
  const context = asMap(value);
  const rawCounts = asMap(context.candidate_counts);
  const claimed = new Set();
  const counts = [];
  for (const [key, label, icon] of REVIEW_COUNT_DEFINITIONS) {
    if (!(key in rawCounts)) continue;
    claimed.add(key);
    counts.push({ key, label, icon, count: intFromAny(rawCounts[key]) });
  }
  for (const key of Object.keys(rawCounts).sort()) {
    if (claimed.has(key)) continue;
    counts.push({ key, label: humanContextCountLabel(key), icon: "•", count: intFromAny(rawCounts[key]) });
  }
  const leftover = { ...context };
  for (const key of ["source", "snapshot_utc", "total_conversations", "candidate_counts", "selection", "preserved"]) delete leftover[key];
  return {
    source: trimStr(context.source),
    snapshotAt: trimStr(context.snapshot_utc),
    counts,
    total: counts.reduce((sum, item) => sum + item.count, 0),
    selection: stringSlice(context.selection).map(compactWhitespace),
    preserved: stringSlice(context.preserved).map(compactWhitespace),
    leftover,
  };
}
