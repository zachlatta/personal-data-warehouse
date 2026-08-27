import test from "node:test";
import assert from "node:assert/strict";
import {
  gmailSenderDisplayName, splitGmailQuotedHTML, gmailBodyFrameHeight, htmlFragmentText, gmailThreadSummary,
  gmailMessageOpen, formatGmailLabel, appendVisibleGmailLabels, assembleEmailBody, splitEmailAddressList,
  humanRecurrence, humanByDay, parseCalendarTime, calendarWhen, formatTimeRange, formatCalendarPatchTime,
  calendarPatchValue, calendarInitials, autolinkSegments, calendarTitle, calendarOperation,
  contactOperationSummary, contactFieldDisplayValue, canonicalContactOp, contactUpdateFields, contactEffect,
  contactEtagWarning, personFromFlatOperation, contactSummaryFromPerson,
  groupMutations, requestListStatus, splitRequestsForList, splitRequestContext, identificationView, appleNotesView,
} from "../mutation_view.js";

// --- gmail --------------------------------------------------------------------

test("gmailSenderDisplayName prefers the display name, then recognises known senders", () => {
  assert.equal(gmailSenderDisplayName("HCB <receipts@hcb.example>", "Receipt"), "HCB");
  assert.equal(gmailSenderDisplayName('"Ada Example" <ada@example.test>', ""), "Ada Example");
  assert.equal(gmailSenderDisplayName("receipts@uber.example", ""), "Uber Receipts");
  assert.equal(gmailSenderDisplayName("noreply@hcb.example", ""), "HCB");
  assert.equal(gmailSenderDisplayName("hello@turbotenant.example", ""), "TurboTenant");
  assert.equal(gmailSenderDisplayName("bot@dinobox.example", ""), "dinobox");
  assert.equal(gmailSenderDisplayName("ada.lovelace@example.test", ""), "Ada Lovelace");
  assert.equal(gmailSenderDisplayName("no-reply@example.test", ""), "Example");
  assert.equal(gmailSenderDisplayName("notifications@some-service.example", ""), "Some Service");
  assert.equal(gmailSenderDisplayName("", "Subject only"), "Subject only");
  assert.equal(gmailSenderDisplayName("nonsense", ""), "nonsense");
});

test("splitGmailQuotedHTML splits at the Gmail quote container and leaves a quote-first body alone", () => {
  const { body, quoted } = splitGmailQuotedHTML('<div>reply</div><div class="gmail_quote gmail_quote_container"><blockquote class="gmail_quote">parent</blockquote></div>');
  assert.equal(body, "<div>reply</div>");
  assert.ok(quoted.includes("parent") && quoted.includes("gmail_quote"));
  assert.deepEqual(splitGmailQuotedHTML('<blockquote class="gmail_quote">only</blockquote>'), { body: '<blockquote class="gmail_quote">only</blockquote>', quoted: "" });
  assert.deepEqual(splitGmailQuotedHTML("<p>plain</p>"), { body: "<p>plain</p>", quoted: "" });
});

test("gmailBodyFrameHeight stays inside its bounds", () => {
  assert.ok(gmailBodyFrameHeight("<p>Short notification</p>", false) <= 180);
  assert.equal(gmailBodyFrameHeight("", false), 128);
  assert.equal(gmailBodyFrameHeight("<p>" + "Long body text. ".repeat(200) + "</p>", false), 240);
  assert.equal(gmailBodyFrameHeight("<p>" + "Quoted body text. ".repeat(200) + "</p>", true), 200);
  assert.equal(gmailBodyFrameHeight("<p>x</p>", true), 112);
  assert.ok(gmailBodyFrameHeight('<p>x</p><img src="a">', false) > gmailBodyFrameHeight("<p>x</p>", false));
});

test("htmlFragmentText strips tags, breaks on block ends and unescapes entities", () => {
  assert.equal(htmlFragmentText("<div>one&amp;two</div><p>three<br>four</p>"), "one&two\nthree\nfour");
  assert.equal(htmlFragmentText("&#39;q&#x41;"), "'qA");
});

test("gmail labels: Inbox is derived from the count and hidden labels are dropped", () => {
  assert.equal(formatGmailLabel("CATEGORY_UPDATES"), "Updates");
  assert.equal(formatGmailLabel("Label_123"), "");
  assert.equal(formatGmailLabel("INBOX"), "");
  assert.equal(formatGmailLabel("CATEGORY_SOMETHING_ELSE"), "SOMETHING ELSE");
  assert.deepEqual(appendVisibleGmailLabels(["Inbox"], ["UNREAD", "UNREAD", "IMPORTANT", "TRASH"]), ["Inbox", "Unread", "Important"]);
  const t = gmailThreadSummary({
    thread_id: "t1", subject: "", latest_from_address: "HCB <receipts@hcb.example>", inbox_message_count: 2,
    labels: ["CATEGORY_UPDATES", "UNREAD"], messages: [{ message_id: "a" }, { message_id: "b" }],
  });
  assert.equal(t.subject, "(no subject)");
  assert.equal(t.senderName, "HCB");
  assert.equal(t.messageCount, 2);
  assert.deepEqual(t.labels, ["Inbox", "Updates", "Unread"]);
  assert.equal(t.unread, true);
});

test("gmailMessageOpen opens unread messages, else the last one when only the thread is known unread", () => {
  const labelled = [{ label_ids: ["INBOX"] }, { label_ids: ["INBOX", "UNREAD"] }];
  assert.equal(gmailMessageOpen(labelled[0], 0, labelled, true), false);
  assert.equal(gmailMessageOpen(labelled[1], 1, labelled, true), true);
  const bare = [{ message_id: "a" }, { message_id: "b" }];
  assert.equal(gmailMessageOpen(bare[0], 0, bare, true), false);
  assert.equal(gmailMessageOpen(bare[1], 1, bare, true), true);
  assert.equal(gmailMessageOpen(bare[1], 1, bare, false), false);
});

test("assembleEmailBody orders editor, signature, quote and joins text with blank lines", () => {
  const out = assembleEmailBody({
    editorHTML: "<div>Hello</div>", editorText: "Hello\n", signatureHTML: '<div class="gmail_signature">--<br>Z</div>', signatureText: "--\nZ",
    quotedHTML: '<div class="gmail_quote">old</div>', quotedText: "old",
  });
  assert.equal(out.body_html, '<div>Hello</div><div><br></div><div class="gmail_signature">--<br>Z</div><div><br></div><div class="gmail_quote">old</div>');
  assert.equal(out.body_text, "Hello\n\n--\nZ\n\nold\n");
  assert.equal(assembleEmailBody({ editorHTML: "<div>x</div>", editorText: "x" }).body_html, "<div>x</div>");
  assert.deepEqual(splitEmailAddressList("a@example.test, b@example.test\n\nc@example.test"), ["a@example.test", "b@example.test", "c@example.test"]);
});

// --- calendar -----------------------------------------------------------------

test("humanRecurrence reads common RRULEs", () => {
  assert.equal(humanRecurrence(["RRULE:FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR"]), "Every weekday");
  assert.equal(humanRecurrence(["RRULE:FREQ=WEEKLY;BYDAY=MO,WE"]), "Weekly on Monday and Wednesday");
  assert.equal(humanRecurrence(["RRULE:FREQ=WEEKLY;INTERVAL=2;BYDAY=FR"]), "Every 2 weeks on Friday");
  assert.equal(humanRecurrence(["RRULE:FREQ=WEEKLY;COUNT=4"]), "Weekly, for 4 occurrences");
  assert.equal(humanRecurrence(["RRULE:FREQ=MONTHLY;UNTIL=20301231T000000Z"]), "Monthly, until December 31, 2030");
  assert.equal(humanRecurrence(["RRULE:FREQ=DAILY;INTERVAL=3;UNTIL=20300102"]), "Every 3 days, until January 2, 2030");
  assert.equal(humanRecurrence(["RRULE:FREQ=YEARLY", "EXDATE:20300101"]), "Annually; EXDATE:20300101");
  assert.equal(humanRecurrence(["RRULE:FREQ=HOURLY"]), "RRULE:FREQ=HOURLY");
  assert.equal(humanByDay("TU"), "Tuesday");
  assert.equal(humanByDay("XX"), "XX");
});

test("parseCalendarTime distinguishes all-day dates from RFC3339 instants", () => {
  assert.deepEqual(parseCalendarTime({ date: "2030-01-02" }), { allDay: true, date: { year: 2030, month: 1, day: 2 }, time: null, timezone: "" });
  const timed = parseCalendarTime({ dateTime: "2030-01-02T18:00:00-05:00", timeZone: "America/New_York" });
  assert.equal(timed.allDay, false);
  assert.equal(timed.time.toISOString(), "2030-01-02T23:00:00.000Z");
  assert.equal(timed.timezone, "America/New_York");
  assert.equal(parseCalendarTime({ dateTime: "garbage" }), null);
  assert.equal(parseCalendarTime(null), null);
});

test("calendarWhen formats timed and all-day events in the given timezone", () => {
  const timed = calendarWhen({ start: { dateTime: "2030-01-02T18:00:00Z" }, end: { dateTime: "2030-01-02T18:30:00Z" } }, "UTC");
  assert.equal(timed.date, "Wednesday, January 2, 2030");
  assert.equal(timed.time, "6:00 – 6:30 PM UTC");
  const crossing = calendarWhen({ start: { dateTime: "2030-01-02T11:30:00Z" }, end: { dateTime: "2030-01-02T12:30:00Z" } }, "UTC");
  assert.equal(crossing.time, "11:30 AM – 12:30 PM UTC");
  const single = calendarWhen({ start: { dateTime: "2030-01-02T18:00:00Z" } }, "UTC");
  assert.equal(single.time, "6:00 PM UTC");
  const allDay = calendarWhen({ start: { date: "2030-01-02" }, end: { date: "2030-01-03" } });
  assert.deepEqual(allDay, { date: "Wednesday, January 2, 2030", time: "All day" });
  const multi = calendarWhen({ start: { date: "2030-01-02" }, end: { date: "2030-01-05" } });
  assert.equal(multi.time, "All day · through Friday, January 4, 2030");
  assert.equal(calendarWhen({}), null);
  assert.equal(formatTimeRange(new Date("2030-01-02T18:00:00Z"), null, "America/New_York"), "1:00 PM EST");
});

test("calendar patch values read as prose where possible", () => {
  assert.deepEqual(calendarPatchValue("summary", "Renamed sync"), { text: "Renamed sync" });
  assert.deepEqual(calendarPatchValue("recurrence", ["RRULE:FREQ=WEEKLY;COUNT=4"]), { text: "Weekly, for 4 occurrences" });
  assert.deepEqual(calendarPatchValue("attendees", [{ email: "one@example.test" }, { email: "two@example.test" }, { name: "x" }]), { text: "one@example.test, two@example.test" });
  assert.deepEqual(calendarPatchValue("start", { dateTime: "2030-01-02T18:00:00Z" }, "UTC"), { text: "Wednesday, January 2, 2030 6:00 PM UTC" });
  assert.equal(formatCalendarPatchTime({ date: "2030-01-02" }), "Wednesday, January 2, 2030 (all day)");
  assert.deepEqual(calendarPatchValue("reminders", { useDefault: true }), { json: '{\n  "useDefault": true\n}' });
  assert.equal(calendarInitials("", "one@example.test"), "O");
  assert.equal(calendarInitials("Ada Example", ""), "AE");
  assert.deepEqual(autolinkSegments("see https://example.test/x, ok"), [{ text: "see " }, { url: "https://example.test/x" }, { text: "," }, { text: " ok" }]);
});

test("calendar title falls back the way the Go renderer did", () => {
  const update = { operation: "calendar.update_event", payload: { patch: { summary: "Renamed" } }, preview: { event: { operation: "update" } }, title: "t" };
  assert.equal(calendarOperation(update), "update");
  assert.equal(calendarTitle(update, update.preview.event, "update", "Update event"), "Renamed");
  assert.equal(calendarTitle({ operation: "calendar.delete_event" }, {}, "delete", "Delete event"), "Cancel this event");
  assert.equal(calendarTitle({ operation: "calendar.create_event" }, {}, "create", "Create event"), "Create event calendar event");
  assert.equal(calendarOperation({ operation: "calendar.delete_event", preview: {} }), "delete");
});

// --- contacts -----------------------------------------------------------------

test("contactOperationSummary resolves summary, person and flat shapes in that order", () => {
  assert.deepEqual(contactOperationSummary({ summary: { display_name: "Ada Lovelace", primary_email: "ada@example.test", organization: "Analytical Engines" } }),
    { displayName: "Ada Lovelace", primaryEmail: "ada@example.test", primaryPhone: "", organization: "Analytical Engines" });
  assert.deepEqual(contactOperationSummary({ person: {
    names: [{ displayName: "Grace Hopper" }], emailAddresses: [{ value: "grace@example.test" }],
    phoneNumbers: [{ value: "+1 555 0100" }], organizations: [{ name: "Navy", title: "Rear Admiral" }],
  } }), { displayName: "Grace Hopper", primaryEmail: "grace@example.test", primaryPhone: "+1 555 0100", organization: "Rear Admiral, Navy" });
  assert.deepEqual(contactOperationSummary({ before: { names: [{ displayName: "Old Duplicate" }] } }).displayName, "Old Duplicate");
  assert.deepEqual(contactOperationSummary({ op: "create", given_name: "Flat", family_name: "Shape", primary_email: "flat@example.test", organization: "Org", job_title: "Role" }),
    { displayName: "Flat Shape", primaryEmail: "flat@example.test", primaryPhone: "", organization: "Role, Org" });
  assert.deepEqual(contactOperationSummary({}), { displayName: "", primaryEmail: "", primaryPhone: "", organization: "" });
});

test("contactFieldDisplayValue summarises People API values", () => {
  assert.equal(contactFieldDisplayValue(undefined), "Not set");
  assert.equal(contactFieldDisplayValue([]), "Not set");
  assert.equal(contactFieldDisplayValue([{ name: "Old Lab", title: "Old Role" }]), "Old Role, Old Lab");
  assert.equal(contactFieldDisplayValue([{ value: "a@example.test" }, { value: "b@example.test" }]), "a@example.test; b@example.test");
  assert.equal(contactFieldDisplayValue([{ displayName: "Ada Old" }]), "Ada Old");
  assert.equal(contactFieldDisplayValue("  plain   text "), "plain text");
  assert.equal(contactFieldDisplayValue({ other: 1 }), '{ "other": 1 }');
});

test("canonicalContactOp and update-field parsing", () => {
  assert.equal(canonicalContactOp("create"), "create_contact");
  assert.equal(canonicalContactOp("update"), "update_contact");
  assert.equal(canonicalContactOp("delete"), "delete_contact");
  assert.equal(canonicalContactOp("update_contact"), "update_contact");
  assert.deepEqual(contactUpdateFields({ update_person_fields: ["names", "emailAddresses"] }), ["names", "emailAddresses"]);
  assert.deepEqual(contactUpdateFields({ updatePersonFields: "names, biographies" }), ["names", "biographies"]);
  assert.deepEqual(contactUpdateFields({}), []);
});

test("contactEffect names cleared fields separately and the etag warning fires only when stale", () => {
  const op = { op: "update_contact", update_person_fields: ["names", "biographies"], clear_person_fields: ["biographies"] };
  assert.deepEqual(contactEffect(op, "update_contact"), { replaced: ["names"], cleared: ["biographies"], sentence: "" });
  assert.equal(contactEffect({}, "delete_contact").sentence, "Deletes this contact from Google Contacts.");
  assert.equal(contactEffect({}, "create_contact").sentence, "Creates a new Google Contact.");
  assert.match(contactEtagWarning({ etag_is_current: false, contact_found: true }, "update_contact"), /changed since this was proposed/);
  assert.equal(contactEtagWarning({ etag_is_current: true, contact_found: true }, "update_contact"), "");
  assert.match(contactEtagWarning({ contact_found: false }, "delete_contact"), /not in the synced Google Contacts copy/);
  assert.equal(contactEtagWarning({ etag_is_current: false }, "create_contact"), "");
});

test("personFromFlatOperation synthesises a Person the person renderer understands", () => {
  const person = personFromFlatOperation({ given_name: "Flat", family_name: "Shape", primary_email: "flat@example.test", primary_phone: "+1 555 0100", organization: "Org", job_title: "Role" });
  assert.deepEqual(person.names, [{ displayName: "Flat Shape", givenName: "Flat", familyName: "Shape" }]);
  assert.deepEqual(contactSummaryFromPerson(person), { displayName: "Flat Shape", primaryEmail: "flat@example.test", primaryPhone: "+1 555 0100", organization: "Role, Org" });
  assert.deepEqual(personFromFlatOperation({}), {});
});

// --- grouping and the list ---------------------------------------------------

test("groupMutations groups gmail threads by operation+account, contacts by account, others alone", () => {
  const m = (id, provider, operation, account, status) => ({ id, provider, operation, account, status: status || "pending_review" });
  const items = groupMutations([
    m("a1", "gmail", "gmail.archive_threads", "z@example.test"),
    m("c1", "google_people", "contacts.batch_mutation", "z@example.test"),
    m("e1", "gmail", "gmail.send_email", "z@example.test"),
    m("a2", "gmail", "gmail.archive_threads", "z@example.test", "approved"),
    m("a3", "gmail", "gmail.archive_threads", "other@example.test"),
    m("u1", "gmail", "gmail.unarchive_threads", "z@example.test"),
    m("c2", "google_people", "google_people.contacts", "z@example.test"),
    m("cal", "google_calendar", "calendar.create_event", "z@example.test"),
  ]);
  assert.deepEqual(items.map((i) => i.kind), ["gmail", "contact", "single", "gmail", "gmail", "single"]);
  assert.deepEqual(items[0].mutations.map((x) => x.id), ["a1", "a2"]);
  assert.equal(items[0].status, "mixed");
  assert.deepEqual(items[1].mutations.map((x) => x.id), ["c1", "c2"]);
  assert.equal(items[1].status, "pending_review");
  assert.equal(items[2].mutation.id, "e1");
  assert.equal(items[3].account, "other@example.test");
  assert.equal(items[4].operation, "gmail.unarchive_threads");
  assert.equal(items[5].mutation.id, "cal");
  assert.deepEqual(groupMutations(undefined), []);
});

test("request list status and split", () => {
  assert.equal(requestListStatus({ status: "rejected" }), "denied");
  assert.equal(requestListStatus({ status: "failed_terminal", superseded_by: "req-2" }), "failed_terminal (superseded)");
  const { pending, past } = splitRequestsForList([{ status: "pending_review", id: 1 }, { status: "succeeded", id: 2 }]);
  assert.deepEqual([pending.length, past.length], [1, 1]);
});

test("request context splits known keys from the leftover JSON", () => {
  const c = splitRequestContext({ source: "PDW", note: "n", identifications: [{ inferred_name: "X", confidence: "high", evidence: [" e ", ""] }], extra: { a: 1 } });
  assert.equal(c.source, "PDW");
  assert.deepEqual(c.leftover, { extra: { a: 1 } });
  assert.deepEqual(identificationView(c.identifications[0]), { name: "X", confidence: "high", action: "", maskedPhone: "", evidence: ["e"] });
  assert.equal(identificationView({}).name, "Unidentified");
  assert.equal(splitRequestContext({}).empty, true);
  assert.equal(splitRequestContext(null).empty, true);
});

test("appleNotesView reads the note preview", () => {
  const v = appleNotesView({ preview: { note: { action: "create", name: "Title", folder: "PDW Agent", changes: ["body"], body_preview: "hi" } } });
  assert.equal(v.heading, "Create Apple Note");
  assert.deepEqual(v.changes, ["body"]);
  assert.equal(appleNotesView({ preview: {} }).heading, "Update Apple Note");
});
