package mutations

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

func TestUpstreamMutationSchemaDDLIsNotRelationQualified(t *testing.T) {
	if !strings.Contains(upstreamMutationSchemaCreateStatement, `CREATE SCHEMA IF NOT EXISTS "upstream_mutations"`) {
		t.Fatalf("unexpected schema DDL: %s", upstreamMutationSchemaCreateStatement)
	}
	if got := warehouse.QualifySQL(upstreamMutationSchemaCreateStatement); got != upstreamMutationSchemaCreateStatement {
		t.Fatalf("schema DDL should not be relation-qualified: %s", got)
	}
	for _, statement := range upstreamMutationSchemaStatements {
		if strings.Contains(statement, "CREATE SCHEMA") {
			t.Fatalf("schema DDL must stay outside the QualifySQL statement loop: %s", statement)
		}
		qualified := warehouse.QualifySQL(statement)
		if strings.Contains(qualified, `CREATE SCHEMA IF NOT EXISTS "upstream_mutations"."operations"`) {
			t.Fatalf("schema DDL was relation-qualified: %s", qualified)
		}
	}
}

func TestEnsureTablesLatchesAfterSuccess(t *testing.T) {
	s := &PostgresStore{ensured: true}
	if err := s.EnsureTables(context.Background()); err != nil {
		t.Fatalf("expected latched EnsureTables to no-op, got %v", err)
	}
	if err := s.EnsureTables(context.Background()); err != nil {
		t.Fatalf("expected second latched EnsureTables to no-op, got %v", err)
	}
}

func TestNormalizeForStorageMatchesWorkerPayloads(t *testing.T) {
	mutations, err := normalizeForStorage(CreateRequestInput{
		Title:   "Do reviewed work",
		Reason:  "agent found work",
		Context: map[string]any{"source": "test"},
		Mutations: []MutationInput{
			{Type: GmailArchiveOperation, Account: "zach@example.test", ThreadIDs: []string{"thread-1", "thread-2"}},
			{Type: GmailUnarchiveOperation, Account: "zach@example.test", ThreadIDs: []string{"thread-3"}},
			{
				Type:         GmailSendEmailOperation,
				Account:      "zach@example.test",
				DeliveryMode: "draft",
				Message: map[string]any{
					"to":                 []string{"one@example.test"},
					"subject":            "Hello",
					"body_text":          "Body",
					"reply_to_thread_id": "thread-4",
				},
			},
			{Type: GooglePeopleContactsOperation, Account: "zach@example.test", Operations: []map[string]any{{"op": "delete_contact", "resource_name": "people/1"}}},
		},
	})
	if err != nil {
		t.Fatalf("normalizeForStorage returned error: %v", err)
	}
	if len(mutations) != 5 {
		t.Fatalf("mutation count = %d", len(mutations))
	}

	if mutations[0].Operation != GmailArchiveOperation || mutations[0].Provider != "gmail" {
		t.Fatalf("archive mutation metadata = %#v", mutations[0])
	}
	if got := stringSliceFromAny(mutations[0].Payload["thread_ids"]); len(got) != 1 || got[0] != "thread-1" {
		t.Fatalf("archive thread_ids = %#v", got)
	}
	if got := stringSliceFromAny(mutations[0].Payload["remove_label_ids"]); len(got) != 1 || got[0] != "INBOX" {
		t.Fatalf("archive remove_label_ids = %#v", got)
	}
	if got := stringSliceFromAny(mutations[2].Payload["add_label_ids"]); len(got) != 1 || got[0] != "INBOX" {
		t.Fatalf("unarchive add_label_ids = %#v", got)
	}

	emailPayload := mutations[3].Payload
	if emailPayload["delivery_mode"] != "draft" {
		t.Fatalf("delivery_mode = %#v", emailPayload["delivery_mode"])
	}
	message, ok := emailPayload["message"].(map[string]any)
	if !ok {
		t.Fatalf("message payload = %#v", emailPayload["message"])
	}
	if message["reply_to_thread_id"] != "thread-4" || message["subject"] != "Hello" {
		t.Fatalf("message = %#v", message)
	}

	contactOps, ok := mutations[4].Payload["operations"].([]map[string]any)
	if !ok || len(contactOps) != 1 {
		t.Fatalf("contact operations = %#v", mutations[4].Payload["operations"])
	}
	if contactOps[0]["op"] != "delete_contact" || mutations[4].Operation != ContactsBatchMutationOperation {
		t.Fatalf("contact mutation = %#v", mutations[4])
	}
}

func TestUpdatedGmailEmailPayloadConvertsSendToDraft(t *testing.T) {
	mutation := Mutation{
		ID:        "mut-email",
		Provider:  "gmail",
		Operation: GmailSendEmailOperation,
		Title:     "Send email: Original",
		Payload: map[string]any{
			"delivery_mode": "send",
			"message": map[string]any{
				"to":                 []any{"old@example.test"},
				"subject":            "Original",
				"body_text":          "Original body",
				"reply_to_thread_id": "thread-1",
				"in_reply_to":        "<old@example.test>",
				"references":         []any{"<old@example.test>"},
			},
		},
		Preview: map[string]any{
			"context": map[string]any{"source": "test"},
			"email":   map[string]any{"subject": "Original", "body_text": "Original body"},
		},
	}

	payload, preview, title, err := updatedGmailEmailPayload(mutation, UpdateGmailEmailMutationInput{
		DeliveryMode: "draft",
		Message: map[string]any{
			"to":        []string{"new@example.test"},
			"cc":        []string{"copy@example.test"},
			"subject":   "Edited",
			"body_text": "Edited plain body",
			"body_html": "<p>Edited <strong>HTML</strong></p>",
		},
	})
	if err != nil {
		t.Fatalf("updatedGmailEmailPayload returned error: %v", err)
	}
	if title != "Create draft: Edited" {
		t.Fatalf("title = %q", title)
	}
	if payload["delivery_mode"] != "draft" {
		t.Fatalf("delivery_mode = %#v", payload["delivery_mode"])
	}
	message := mapFromAny(payload["message"])
	if got := stringSliceFromAny(message["to"]); len(got) != 1 || got[0] != "new@example.test" {
		t.Fatalf("to = %#v", message["to"])
	}
	if message["reply_to_thread_id"] != "thread-1" || message["in_reply_to"] != "<old@example.test>" {
		t.Fatalf("reply metadata was not preserved: %#v", message)
	}
	previewEmail := mapFromAny(preview["email"])
	if previewEmail["delivery_mode"] != "draft" || previewEmail["mode"] != "reply" {
		t.Fatalf("preview email = %#v", previewEmail)
	}
	if mapFromAny(preview["context"])["source"] != "test" {
		t.Fatalf("preview context = %#v", preview["context"])
	}
}

func TestApplyGmailReplyHeaderRowsPopulatesReplyMetadata(t *testing.T) {
	mutations := []storedMutation{{
		Provider:  "gmail",
		Operation: GmailSendEmailOperation,
		Account:   "zach@example.test",
		Payload: map[string]any{
			"delivery_mode": "draft",
			"message": map[string]any{
				"to":                 []string{"sender@example.test"},
				"subject":            "Re: Existing thread",
				"body_text":          "Reply body",
				"reply_to_thread_id": "thread-1",
			},
		},
		Preview: map[string]any{
			"email": map[string]any{"subject": "Re: Existing thread"},
		},
	}}

	got := applyGmailReplyHeaderRows(mutations, []gmailReplyHeaderRow{{
		Account:          "zach@example.test",
		ThreadID:         "thread-1",
		RFC822MessageID:  "<parent@example.test>",
		ReferencesHeader: "<root@example.test>",
		InReplyToHeader:  "<previous@example.test>",
	}})

	message := mapFromAny(got[0].Payload["message"])
	if message["in_reply_to"] != "<parent@example.test>" {
		t.Fatalf("in_reply_to = %#v", message["in_reply_to"])
	}
	references := stringSliceFromAny(message["references"])
	if strings.Join(references, " ") != "<root@example.test> <parent@example.test>" {
		t.Fatalf("references = %#v", references)
	}
	previewEmail := mapFromAny(got[0].Preview["email"])
	if previewEmail["in_reply_to"] != "<parent@example.test>" || previewEmail["mode"] != "reply" {
		t.Fatalf("preview email = %#v", previewEmail)
	}
}

func TestApplyGmailReplyHeaderRowsPopulatesVariantReplyMetadata(t *testing.T) {
	mutations := []storedMutation{{
		Provider:  "gmail",
		Operation: GmailSendEmailOperation,
		Account:   "zach@example.test",
		Payload: map[string]any{
			"delivery_mode":       "send",
			"selected_variant_id": "variant_2",
			"message": map[string]any{
				"to":        []string{"sender@example.test"},
				"subject":   "Re: Existing thread",
				"body_text": "Base body",
			},
			"variants": []map[string]any{{
				"id":    "variant_1",
				"title": "Direct Reply",
				"message": map[string]any{
					"to":                 []string{"sender@example.test"},
					"subject":            "Re: Existing thread",
					"body_text":          "Direct body",
					"reply_to_thread_id": "thread-1",
				},
			}, {
				"id":    "variant_2",
				"title": "Softer Ask",
				"message": map[string]any{
					"to":                 []string{"sender@example.test"},
					"subject":            "Re: Existing thread",
					"body_text":          "Softer body",
					"reply_to_thread_id": "thread-1",
				},
			}},
		},
		Preview: map[string]any{
			"email": map[string]any{"subject": "Re: Existing thread"},
		},
	}}

	got := applyGmailReplyHeaderRows(mutations, []gmailReplyHeaderRow{{
		Account:          "zach@example.test",
		ThreadID:         "thread-1",
		RFC822MessageID:  "<parent@example.test>",
		ReferencesHeader: "<root@example.test>",
	}})

	payload := got[0].Payload
	message := mapFromAny(payload["message"])
	if message["body_text"] != "Softer body" || message["in_reply_to"] != "<parent@example.test>" {
		t.Fatalf("selected message = %#v", message)
	}
	variants := normalizeStoredEmailVariants(payload["variants"])
	if len(variants) != 2 {
		t.Fatalf("variants = %#v", payload["variants"])
	}
	for _, variant := range variants {
		variantMessage := mapFromAny(variant["message"])
		if variantMessage["in_reply_to"] != "<parent@example.test>" {
			t.Fatalf("variant message = %#v", variantMessage)
		}
	}
	previewEmail := mapFromAny(got[0].Preview["email"])
	previewVariants := normalizeStoredEmailVariants(previewEmail["variants"])
	if len(previewVariants) != 2 || previewEmail["selected_variant_id"] != "variant_2" {
		t.Fatalf("preview variants = %#v", previewEmail)
	}
}

func TestApplyGmailReplyQuoteRowsAddsGmailStyleHistoryAfterSignature(t *testing.T) {
	mutations := []storedMutation{{
		Provider:  "gmail",
		Operation: GmailSendEmailOperation,
		Account:   "zach@example.test",
		Payload: map[string]any{
			"delivery_mode": "send",
			"message": map[string]any{
				"to":                 []string{"sender@example.test"},
				"subject":            "Re: Existing thread",
				"body_text":          "Reply body\n\n--\nZach",
				"body_html":          `<div>Reply body</div><div><br></div><div class="gmail_signature"><div>--<br>Zach</div></div>`,
				"reply_to_thread_id": "thread-1",
			},
		},
		Preview: map[string]any{
			"email": map[string]any{"subject": "Re: Existing thread"},
		},
	}}

	got := applyGmailReplyQuoteRows(mutations, []gmailReplyQuoteRow{{
		Account:      "zach@example.test",
		ThreadID:     "thread-1",
		FromAddress:  "Sender <sender@example.test>",
		InternalDate: time.Date(2026, 5, 23, 18, 4, 0, 0, time.UTC),
		BodyHTML:     `<p>Parent body</p>`,
		BodyText:     "Parent body",
	}})

	message := mapFromAny(got[0].Payload["message"])
	bodyHTML := stringFromAny(message["body_html"])
	signatureIndex := strings.Index(bodyHTML, "gmail_signature")
	quoteIndex := strings.Index(bodyHTML, "gmail_quote")
	if signatureIndex < 0 || quoteIndex < 0 || signatureIndex > quoteIndex {
		t.Fatalf("body_html did not keep signature before quote: %q", bodyHTML)
	}
	for _, want := range []string{`class="gmail_quote gmail_quote_container"`, `class="gmail_attr"`, `Sender &lt;sender@example.test&gt; wrote:`, `<blockquote class="gmail_quote"`, "Parent body"} {
		if !strings.Contains(bodyHTML, want) {
			t.Fatalf("body_html missing %q: %q", want, bodyHTML)
		}
	}
	bodyText := stringFromAny(message["body_text"])
	if !strings.Contains(bodyText, "--\nZach\n\nOn Sat, May 23, 2026 at 6:04 PM, Sender <sender@example.test> wrote:\n> Parent body") {
		t.Fatalf("body_text = %q", bodyText)
	}
	previewEmail := mapFromAny(got[0].Preview["email"])
	if !strings.Contains(stringFromAny(previewEmail["body_html"]), "gmail_quote") {
		t.Fatalf("preview email = %#v", previewEmail)
	}
}

func TestNormalizeForStorageStoresEmailVariantsWithSelectedMessage(t *testing.T) {
	mutations, err := normalizeForStorage(CreateRequestInput{
		Reason: "choose email",
		Mutations: []MutationInput{{
			Type:         GmailSendEmailOperation,
			Account:      "zach@example.test",
			DeliveryMode: "send",
			Message: map[string]any{
				"to":        []string{"one@example.test"},
				"subject":   "Base subject",
				"body_text": "Base body",
			},
			EmailVariants: []GmailEmailVariantInput{{
				Title:    "Direct Reply",
				BodyText: "Direct body",
			}, {
				Title:    "Softer Ask",
				Subject:  "Softer subject",
				BodyText: "Softer body",
			}},
		}},
	})
	if err != nil {
		t.Fatalf("normalizeForStorage returned error: %v", err)
	}
	payload := mutations[0].Payload
	if payload["selected_variant_id"] != "variant_1" {
		t.Fatalf("selected_variant_id = %#v", payload["selected_variant_id"])
	}
	message := mapFromAny(payload["message"])
	if message["body_text"] != "Direct body" || message["subject"] != "Base subject" {
		t.Fatalf("selected message = %#v", message)
	}
	variants := mapSliceFromAny(payload["variants"])
	if len(variants) != 2 {
		t.Fatalf("variants = %#v", payload["variants"])
	}
	if variants[0]["title"] != "Direct Reply" || mapFromAny(variants[1]["message"])["subject"] != "Softer subject" {
		t.Fatalf("variants = %#v", variants)
	}
	previewEmail := mapFromAny(mutations[0].Preview["email"])
	if previewEmail["selected_variant_id"] != "variant_1" || len(mapSliceFromAny(previewEmail["variants"])) != 2 {
		t.Fatalf("preview email = %#v", previewEmail)
	}
}

func TestUpdatedGmailEmailPayloadSelectsVariant(t *testing.T) {
	mutation := Mutation{
		ID:        "mut-email",
		Provider:  "gmail",
		Operation: GmailSendEmailOperation,
		Payload: map[string]any{
			"delivery_mode":       "send",
			"selected_variant_id": "variant_1",
			"message": map[string]any{
				"to":        []any{"one@example.test"},
				"subject":   "Direct",
				"body_text": "Direct body",
			},
			"variants": []map[string]any{{
				"id":    "variant_1",
				"title": "Direct Reply",
				"message": map[string]any{
					"to":        []any{"one@example.test"},
					"subject":   "Direct",
					"body_text": "Direct body",
				},
			}, {
				"id":    "variant_2",
				"title": "Softer Ask",
				"message": map[string]any{
					"to":        []any{"one@example.test"},
					"subject":   "Softer",
					"body_text": "Softer body",
				},
			}},
		},
		Preview: map[string]any{},
	}

	payload, preview, title, err := updatedGmailEmailPayload(mutation, UpdateGmailEmailMutationInput{
		DeliveryMode:      "draft",
		SelectedVariantID: "variant_2",
		Message: map[string]any{
			"body_text": "Edited softer body",
		},
	})
	if err != nil {
		t.Fatalf("updatedGmailEmailPayload returned error: %v", err)
	}
	if title != "Create draft: Softer" {
		t.Fatalf("title = %q", title)
	}
	if payload["selected_variant_id"] != "variant_2" || payload["delivery_mode"] != "draft" {
		t.Fatalf("payload metadata = %#v", payload)
	}
	message := mapFromAny(payload["message"])
	if message["subject"] != "Softer" || message["body_text"] != "Edited softer body" {
		t.Fatalf("selected message = %#v", message)
	}
	variants := mapSliceFromAny(payload["variants"])
	if mapFromAny(variants[0]["message"])["body_text"] != "Direct body" {
		t.Fatalf("first variant was modified: %#v", variants[0])
	}
	if mapFromAny(variants[1]["message"])["body_text"] != "Edited softer body" {
		t.Fatalf("second variant was not updated: %#v", variants[1])
	}
	previewEmail := mapFromAny(preview["email"])
	if previewEmail["selected_variant_id"] != "variant_2" || len(mapSliceFromAny(previewEmail["variants"])) != 2 {
		t.Fatalf("preview email = %#v", previewEmail)
	}
}

func TestUpdatedGmailEmailPayloadRejectsInvalidDraft(t *testing.T) {
	mutation := Mutation{
		Payload: map[string]any{
			"delivery_mode": "send",
			"message": map[string]any{
				"to":        []any{"old@example.test"},
				"subject":   "Original",
				"body_text": "Original body",
			},
		},
		Preview: map[string]any{},
	}

	if _, _, _, err := updatedGmailEmailPayload(mutation, UpdateGmailEmailMutationInput{
		DeliveryMode: "maybe",
		Message:      map[string]any{"to": []string{"new@example.test"}, "subject": "Edited", "body_text": "Body"},
	}); err == nil {
		t.Fatal("expected invalid delivery mode error")
	}
	if _, _, _, err := updatedGmailEmailPayload(mutation, UpdateGmailEmailMutationInput{
		DeliveryMode: "draft",
		Message:      map[string]any{"to": []string{}, "subject": "Edited", "body_text": "Body"},
	}); err == nil {
		t.Fatal("expected recipient error")
	}
}

func TestExtractGmailSignatureHTML(t *testing.T) {
	bodyHTML := `<html><body><div>Reply body</div><br><div class="gmail_signature"><div dir="ltr"><span>--</span><br><a href="https://hackclub.com">Hack Club</a></div></div><div class="gmail_quote">quoted</div></body></html>`

	signature := extractGmailSignatureHTML(bodyHTML)
	if !strings.Contains(signature, `class="gmail_signature"`) || !strings.Contains(signature, `https://hackclub.com`) {
		t.Fatalf("signature = %q", signature)
	}
	if strings.Contains(signature, "gmail_quote") {
		t.Fatalf("signature included quote: %q", signature)
	}
}

func TestAppendGmailSignatureToMessageAddsHTMLAndText(t *testing.T) {
	message := map[string]any{
		"to":        []string{"zach@example.test"},
		"subject":   "Hello",
		"body_text": "Hello there.",
		"body_html": "",
	}
	signature := gmailSignature{
		HTML: `<div class="gmail_signature"><div>--<br><a href="https://hackclub.com">Hack Club</a></div></div>`,
		Text: "--\nZach Latta\nHack Club",
	}

	out := appendGmailSignatureToMessage(message, signature)
	bodyHTML := stringFromAny(out["body_html"])
	if !strings.Contains(bodyHTML, "<div>Hello there.</div><div><br></div>") || !strings.Contains(bodyHTML, `class="gmail_signature"`) || !strings.Contains(bodyHTML, `https://hackclub.com`) {
		t.Fatalf("body_html = %q", bodyHTML)
	}
	bodyText := stringFromAny(out["body_text"])
	if !strings.Contains(bodyText, "Hello there.\n\n--\nZach Latta") {
		t.Fatalf("body_text = %q", bodyText)
	}
}

func TestAppendGmailSignatureToMessageReplacesPlainTextSignatureForHTML(t *testing.T) {
	message := map[string]any{
		"to":        []string{"zach@example.test"},
		"subject":   "Hello",
		"body_text": "Hello there.\n\n--\nZach Latta\nFounder",
		"body_html": "",
	}
	signature := gmailSignature{
		HTML: `<div class="gmail_signature"><div>--<br><a href="https://hackclub.com">Hack Club</a></div></div>`,
		Text: "--\nZach Latta\nHack Club",
	}

	out := appendGmailSignatureToMessage(message, signature)
	bodyHTML := stringFromAny(out["body_html"])
	if !strings.Contains(bodyHTML, "<div>Hello there.</div><div><br></div>") || !strings.Contains(bodyHTML, `class="gmail_signature"`) {
		t.Fatalf("body_html = %q", bodyHTML)
	}
	if strings.Contains(bodyHTML, "Founder") {
		t.Fatalf("plain text signature leaked into HTML body: %q", bodyHTML)
	}
	if bodyText := stringFromAny(out["body_text"]); bodyText != "Hello there.\n\n--\nZach Latta\nFounder" {
		t.Fatalf("body_text = %q", bodyText)
	}
}

func TestEmailPlainTextToHTMLUsesGmailStyleDivs(t *testing.T) {
	bodyHTML := emailPlainTextToHTML("First line\nSecond line\n\nNext paragraph")

	want := "<div>First line<br>Second line</div><div><br></div><div>Next paragraph</div>"
	if bodyHTML != want {
		t.Fatalf("body_html = %q, want %q", bodyHTML, want)
	}
}

func TestAppendGmailSignatureSkipsExistingSignature(t *testing.T) {
	message := map[string]any{
		"body_text": "Hello\n\n--\nExisting",
		"body_html": `<p>Hello</p><div class="gmail_signature">Existing</div>`,
	}

	if !gmailEmailHasSignature(message) {
		t.Fatal("expected existing signature to be detected")
	}
}
