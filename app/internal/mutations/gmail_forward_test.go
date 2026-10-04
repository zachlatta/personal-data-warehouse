package mutations

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func forwardRowFixture() gmailForwardRow {
	return gmailForwardRow{
		Account:          "zach@example.test",
		MessageID:        "orig-1",
		ThreadID:         "thread-orig",
		RFC822MessageID:  "<invoice-4831@vendor.test>",
		ReferencesHeader: "<earlier@vendor.test>",
		FromHeader:       "Vendor <billing@vendor.test>",
		ToHeader:         "zach@example.test",
		CCHeader:         "Books <books@example.test>",
		Subject:          "Invoice 4831",
		InternalDate:     time.Date(2026, 9, 29, 15, 4, 0, 0, time.UTC),
		BodyHTML:         `<html><body><p>Your invoice is attached.</p></body></html>`,
		BodyText:         "Your invoice is attached.",
		Attachments: []gmailForwardAttachment{
			{Filename: "invoice-4831.pdf", ContentType: "application/pdf", Size: 48213},
		},
	}
}

func forwardMutationFixture() storedMutation {
	message := map[string]any{
		"to":                 []string{"accountant@example.test"},
		"subject":            "",
		"body_text":          "Can you file this?\n\n--\nZach\n",
		"body_html":          `<div>Can you file this?</div><div><br></div><div class="gmail_signature"><div>--<br>Zach</div></div>`,
		"forward_message_id": "orig-1",
	}
	return storedMutation{
		Provider:  "gmail",
		Operation: GmailSendEmailOperation,
		Account:   "zach@example.test",
		Title:     gmailEmailRequestTitle("send", message),
		Payload:   map[string]any{"delivery_mode": "send", "message": message},
		Preview:   map[string]any{"email": map[string]any{"mode": emailPreviewMode(message)}},
	}
}

// A forward reads, and sends, the way Gmail's own Forward does: the note,
// then the signature, then the "Forwarded message" header block over the
// original — so the reviewer approves exactly the words that go out.
func TestApplyGmailForwardRowsWritesGmailsForwardBelowTheSignature(t *testing.T) {
	got, err := applyGmailForwardRows([]storedMutation{forwardMutationFixture()}, []gmailForwardRow{forwardRowFixture()})
	if err != nil {
		t.Fatalf("applyGmailForwardRows: %v", err)
	}
	message := mapFromAny(got[0].Payload["message"])
	if message["subject"] != "Fwd: Invoice 4831" {
		t.Fatalf("subject = %#v", message["subject"])
	}
	if got[0].Title != "Send email: Fwd: Invoice 4831" {
		t.Fatalf("title = %q", got[0].Title)
	}
	bodyHTML := stringFromAny(message["body_html"])
	signature := strings.Index(bodyHTML, "gmail_signature")
	forward := strings.Index(bodyHTML, gmailForwardMarker)
	if signature < 0 || forward < 0 || signature > forward {
		t.Fatalf("body_html must keep the signature above the forwarded block: %q", bodyHTML)
	}
	for _, want := range []string{
		`<div class="gmail_quote gmail_quote_container">`,
		`From: Vendor &lt;billing@vendor.test&gt;`,
		`Date: Tue, Sep 29, 2026 at 3:04 PM`,
		`Subject: Invoice 4831`,
		`To: zach@example.test`,
		`Cc: Books &lt;books@example.test&gt;`,
		`<p>Your invoice is attached.</p>`,
	} {
		if !strings.Contains(bodyHTML, want) {
			t.Fatalf("body_html missing %q: %q", want, bodyHTML)
		}
	}
	if strings.Contains(bodyHTML, "<body>") {
		t.Fatalf("the original's document wrapper must not be nested: %q", bodyHTML)
	}
	wantText := "--\nZach\n\n---------- Forwarded message ---------\n" +
		"From: Vendor <billing@vendor.test>\nDate: Tue, Sep 29, 2026 at 3:04 PM\nSubject: Invoice 4831\n" +
		"To: zach@example.test\nCc: Books <books@example.test>\n\nYour invoice is attached.\n"
	if bodyText := stringFromAny(message["body_text"]); !strings.HasSuffix(bodyText, wantText) {
		t.Fatalf("body_text = %q", bodyText)
	}
	// Threading headers, exactly as Gmail writes them on a forward.
	if message["in_reply_to"] != "<invoice-4831@vendor.test>" {
		t.Fatalf("in_reply_to = %#v", message["in_reply_to"])
	}
	if refs := strings.Join(stringSliceFromAny(message["references"]), " "); refs != "<earlier@vendor.test> <invoice-4831@vendor.test>" {
		t.Fatalf("references = %q", refs)
	}
	preview := mapFromAny(got[0].Preview["forward"])
	if preview["message_id"] != "orig-1" || preview["subject"] != "Invoice 4831" || preview["from"] != "Vendor <billing@vendor.test>" {
		t.Fatalf("forward preview = %#v", preview)
	}
	attachments := mapSliceFromAny(preview["attachments"])
	if len(attachments) != 1 || attachments[0]["filename"] != "invoice-4831.pdf" || attachments[0]["size"] != int64(48213) {
		t.Fatalf("forward preview attachments = %#v", preview["attachments"])
	}
	if email := mapFromAny(got[0].Preview["email"]); email["mode"] != "forward" || email["subject"] != "Fwd: Invoice 4831" {
		t.Fatalf("preview email = %#v", email)
	}
}

// Enrichment runs again on every reviewer edit, and the edited body comes back
// with the forwarded block already in it.
func TestApplyGmailForwardRowsIsIdempotent(t *testing.T) {
	once, err := applyGmailForwardRows([]storedMutation{forwardMutationFixture()}, []gmailForwardRow{forwardRowFixture()})
	if err != nil {
		t.Fatal(err)
	}
	twice, err := applyGmailForwardRows(once, []gmailForwardRow{forwardRowFixture()})
	if err != nil {
		t.Fatal(err)
	}
	message := mapFromAny(twice[0].Payload["message"])
	if n := strings.Count(stringFromAny(message["body_html"]), gmailForwardMarker); n != 1 {
		t.Fatalf("forwarded block appears %d times in body_html", n)
	}
	if n := strings.Count(stringFromAny(message["body_text"]), gmailForwardMarker); n != 1 {
		t.Fatalf("forwarded block appears %d times in body_text", n)
	}
	if message["subject"] != "Fwd: Invoice 4831" {
		t.Fatalf("subject = %#v", message["subject"])
	}
}

// An API client that edits only the plain text posts the forwarded block as
// text and no HTML at all; the HTML must carry the block once, as Gmail's.
func TestApplyGmailForwardRowsRebuildsTheHTMLOfATextOnlyEdit(t *testing.T) {
	once, err := applyGmailForwardRows([]storedMutation{forwardMutationFixture()}, []gmailForwardRow{forwardRowFixture()})
	if err != nil {
		t.Fatal(err)
	}
	message := mapFromAny(once[0].Payload["message"])
	message["body_html"] = ""
	once[0].Payload["message"] = message
	got, err := applyGmailForwardRows(once, []gmailForwardRow{forwardRowFixture()})
	if err != nil {
		t.Fatal(err)
	}
	bodyHTML := stringFromAny(mapFromAny(got[0].Payload["message"])["body_html"])
	if n := strings.Count(bodyHTML, gmailForwardMarker); n != 1 {
		t.Fatalf("forwarded block appears %d times in body_html: %q", n, bodyHTML)
	}
	if !strings.Contains(bodyHTML, `<div class="gmail_quote gmail_quote_container">`) || !strings.HasPrefix(bodyHTML, "<div>Can you file this?</div>") {
		t.Fatalf("body_html = %q", bodyHTML)
	}
}

func TestApplyGmailForwardRowsKeepsTheAgentsSubjectAndNeverDoublesFwd(t *testing.T) {
	mutation := forwardMutationFixture()
	mutation.Payload["message"].(map[string]any)["subject"] = "Invoice for the books"
	row := forwardRowFixture()
	got, err := applyGmailForwardRows([]storedMutation{mutation}, []gmailForwardRow{row})
	if err != nil {
		t.Fatal(err)
	}
	if subject := mapFromAny(got[0].Payload["message"])["subject"]; subject != "Invoice for the books" {
		t.Fatalf("subject = %#v", subject)
	}

	row.Subject = "FWD: Invoice 4831"
	got, err = applyGmailForwardRows([]storedMutation{forwardMutationFixture()}, []gmailForwardRow{row})
	if err != nil {
		t.Fatal(err)
	}
	if subject := mapFromAny(got[0].Payload["message"])["subject"]; subject != "FWD: Invoice 4831" {
		t.Fatalf("subject = %#v", subject)
	}
}

func TestApplyGmailForwardRowsFillsEveryVariant(t *testing.T) {
	mutation := forwardMutationFixture()
	base := mapFromAny(mutation.Payload["message"])
	short := cloneMap(base)
	short["body_text"] = "FYI"
	short["body_html"] = ""
	mutation.Payload["variants"] = []map[string]any{
		{"id": "variant_1", "title": "Full Note", "message": base},
		{"id": "variant_2", "title": "Short Note", "message": short},
	}
	mutation.Payload["selected_variant_id"] = "variant_2"
	got, err := applyGmailForwardRows([]storedMutation{mutation}, []gmailForwardRow{forwardRowFixture()})
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range normalizeStoredEmailVariants(got[0].Payload["variants"]) {
		message := mapFromAny(variant["message"])
		if !strings.Contains(stringFromAny(message["body_html"]), gmailForwardMarker) || message["subject"] != "Fwd: Invoice 4831" {
			t.Fatalf("variant %v was not filled: %#v", variant["id"], message)
		}
	}
	selected := mapFromAny(got[0].Payload["message"])
	if !strings.HasPrefix(stringFromAny(selected["body_text"]), "FYI") {
		t.Fatalf("the selected variant must stay the payload message: %#v", selected["body_text"])
	}
}

// Nothing to forward is a proposal the agent must fix, not a review that
// shows an empty forward.
func TestApplyGmailForwardRowsRefusesAMessageTheWarehouseDoesNotHave(t *testing.T) {
	_, err := applyGmailForwardRows([]storedMutation{forwardMutationFixture()}, nil)
	var inputErr *proposalInputError
	if !errors.As(err, &inputErr) {
		t.Fatalf("err = %v, want a proposal input error", err)
	}
	if !strings.Contains(err.Error(), "orig-1") || !strings.Contains(err.Error(), "base_gmail.messages") {
		t.Fatalf("err = %v", err)
	}
}

func TestGmailEmailViewFoldsTheForwardedMessageAndNamesItsFiles(t *testing.T) {
	enriched, err := applyGmailForwardRows([]storedMutation{forwardMutationFixture()}, []gmailForwardRow{forwardRowFixture()})
	if err != nil {
		t.Fatal(err)
	}
	view := gmailEmailView(Mutation{
		Provider:  enriched[0].Provider,
		Operation: enriched[0].Operation,
		Payload:   enriched[0].Payload,
		Preview:   enriched[0].Preview,
	})
	read := view["message"].(map[string]any)
	if read["editor_text"] != "Can you file this?" {
		t.Fatalf("editor_text = %q", read["editor_text"])
	}
	if !strings.Contains(stringFromAny(read["quoted_html"]), gmailForwardMarker) {
		t.Fatalf("quoted_html = %q", read["quoted_html"])
	}
	if read["forward_message_id"] != "orig-1" {
		t.Fatalf("forward_message_id = %#v", read["forward_message_id"])
	}
	forward := mapFromAny(view["forward"])
	if forward["message_id"] != "orig-1" || len(mapSliceFromAny(forward["attachments"])) != 1 {
		t.Fatalf("forward = %#v", view["forward"])
	}
}

func TestProposeMutationGmailForwardNeedsNeitherSubjectNorBody(t *testing.T) {
	store := &recordingStore{request: Request{ID: "req-fwd", Status: "pending_review"}}
	service := NewService(store, Config{})
	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Forward the invoice",
		Reason: "the accountant files it",
		Mutations: []map[string]any{{
			"type":    GmailSendEmailOperation,
			"account": "zach@example.test",
			"message": map[string]any{
				"to":                 []any{"accountant@example.test"},
				"forward_message_id": " orig-1 ",
			},
		}},
	})
	if err != nil {
		t.Fatalf("ProposeMutation: %v", err)
	}
	stored := normalizeMessageForStorage(store.createCalls[0].Mutations[0].Message)
	if stored["forward_message_id"] != "orig-1" {
		t.Fatalf("forward_message_id = %#v", stored["forward_message_id"])
	}
}

func TestProposeMutationRejectsAnAmbiguousForward(t *testing.T) {
	cases := map[string]map[string]any{
		"a reply and a forward": {
			"message": map[string]any{
				"to":                 []any{"a@example.test"},
				"forward_message_id": "orig-1",
				"reply_to_thread_id": "thread-1",
			},
		},
		"variants forwarding different messages": {
			"message": map[string]any{"to": []any{"a@example.test"}, "forward_message_id": "orig-1"},
			"variants": []any{
				map[string]any{"title": "First One"},
				map[string]any{"title": "Second One", "message": map[string]any{"forward_message_id": "orig-2"}},
			},
		},
		"a forward without a recipient": {
			"message": map[string]any{"forward_message_id": "orig-1"},
		},
	}
	for name, fields := range cases {
		t.Run(name, func(t *testing.T) {
			store := &recordingStore{}
			mutation := map[string]any{"type": GmailSendEmailOperation, "account": "zach@example.test"}
			for key, value := range fields {
				mutation[key] = value
			}
			_, err := NewService(store, Config{}).ProposeMutation(context.Background(), ProposeMutationInput{
				Title: "Forward", Reason: "test", Mutations: []map[string]any{mutation},
			})
			var inputErr *proposalInputError
			if !errors.As(err, &inputErr) {
				t.Fatalf("err = %v, want a proposal input error", err)
			}
			if len(store.createCalls) != 0 {
				t.Fatalf("an invalid forward reached the store")
			}
		})
	}
}
