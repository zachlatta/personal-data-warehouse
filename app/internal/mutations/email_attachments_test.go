package mutations

import (
	"encoding/base64"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmailAttachmentsValidationAndRoundTrip(t *testing.T) {
	attachments := []any{map[string]any{"filename": "résumé.txt", "content_type": "text/plain", "data_base64": base64.StdEncoding.EncodeToString([]byte("hello"))}}
	message := map[string]any{"to": []string{"a@example.test"}, "subject": "Files", "body_text": "Attached", "attachments": attachments}
	if err := validateEmailAttachments(message["attachments"]); err != nil {
		t.Fatal(err)
	}
	stored := normalizeMessageForStorage(message)
	if len(mapSliceFromAny(stored["attachments"])) != 1 {
		t.Fatal("storage dropped attachments")
	}
	view := gmailEmailMessageView(stored)
	if len(mapSliceFromAny(view["attachments"])) != 1 {
		t.Fatal("review dropped attachments")
	}
	mutation := Mutation{Payload: map[string]any{"message": stored}}
	payload, _, _, err := updatedGmailEmailPayload(mutation, UpdateGmailEmailMutationInput{Message: map[string]any{"subject": "Edited"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(mapSliceFromAny(mapFromAny(payload["message"])["attachments"])) != 1 {
		t.Fatal("edit dropped attachments")
	}
	input := gmailEmailUpdateInputFromJSON(apiUpdateEmailBody{Message: map[string]any{"attachments": []any{}}})
	if _, ok := input.Message["attachments"]; !ok {
		t.Fatal("explicit removal lost")
	}
	for _, invalid := range []any{"bad", []any{map[string]any{}}, []any{map[string]any{"filename": "../bad", "content_type": "text/plain", "data_base64": ""}}, []any{map[string]any{"filename": "ok", "content_type": "text/plain", "data_base64": "!"}}} {
		if validateEmailAttachments(invalid) == nil {
			t.Fatalf("accepted %#v", invalid)
		}
	}
}

func TestEmailAttachmentLimitsAndInvalidFields(t *testing.T) {
	valid := func() map[string]any {
		return map[string]any{"filename": "file.bin", "content_type": "application/octet-stream", "data_base64": ""}
	}
	for _, tc := range []struct {
		field string
		value any
	}{
		{"filename", ""}, {"filename", "."}, {"filename", ".."}, {"filename", "a\\b"},
		{"filename", "a\nb"}, {"filename", strings.Repeat("é", 128)}, {"filename", 42},
		{"content_type", "text/plain; charset=utf-8"}, {"content_type", "text/plain\r\nX: bad"},
		{"content_type", "multipart/mixed"}, {"content_type", "invalid"}, {"data_base64", "YQ"}, {"data_base64", "YR=="},
		{"data_base64", "YQ==\n"}, {"data_base64", "_w=="}, {"data_base64", nil},
		{"url", "https://example.test/file"}, {"path", "/tmp/file"},
	} {
		t.Run(tc.field+fmt.Sprint(tc.value), func(t *testing.T) {
			a := valid()
			a[tc.field] = tc.value
			if err := validateEmailAttachments([]any{a}); err == nil {
				t.Fatal("accepted invalid attachment")
			}
		})
	}
	items := make([]any, maxEmailAttachments+1)
	for i := range items {
		items[i] = valid()
	}
	if err := validateEmailAttachments(items); err == nil {
		t.Fatal("accepted 101 attachments")
	}
	if err := validateEmailAttachments(items[:maxEmailAttachments]); err != nil {
		t.Fatal(err)
	}
	a := valid()
	a["data_base64"] = base64.StdEncoding.EncodeToString(make([]byte, maxEmailAttachmentBytes))
	if err := validateEmailAttachments([]any{a}); err != nil {
		t.Fatal(err)
	}
	b := valid()
	b["data_base64"] = "AA=="
	if err := validateEmailAttachments([]any{a, b}); err == nil {
		t.Fatal("accepted excessive combined bytes")
	}
}

func TestEmailAttachmentsVariantsAndRemoval(t *testing.T) {
	a := []any{map[string]any{"filename": "file", "content_type": "text/plain", "data_base64": "YQ=="}}
	base := map[string]any{"to": []string{"a@example.test"}, "subject": "Files", "body_text": "Attached", "attachments": a}
	variants, err := normalizeEmailVariantInputs(base, []GmailEmailVariantInput{
		{Title: "Keep Files"}, {Title: "No Files", Message: map[string]any{"attachments": []any{}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(mapSliceFromAny(mapFromAny(variants[0]["message"])["attachments"])) != 1 {
		t.Fatal("inheritance failed")
	}
	if len(mapSliceFromAny(mapFromAny(variants[1]["message"])["attachments"])) != 0 {
		t.Fatal("removal failed")
	}
	mutation := Mutation{Payload: map[string]any{"message": base, "variants": variants}}
	payload, _, _, err := updatedGmailEmailPayload(mutation, UpdateGmailEmailMutationInput{SelectedVariantID: "variant_2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(mapSliceFromAny(mapFromAny(payload["message"])["attachments"])) != 0 {
		t.Fatal("selection restored removed files")
	}
	input := gmailEmailUpdateInputFromJSON(apiUpdateEmailBody{Message: map[string]any{
		"to": []string{"a@example.test"}, "subject": "Files", "body_text": "Attached", "attachments": []any{},
	}})
	payload, _, _, err = updatedGmailEmailPayload(Mutation{Payload: map[string]any{"message": base}}, input)
	if err != nil {
		t.Fatal(err)
	}
	if len(mapSliceFromAny(mapFromAny(payload["message"])["attachments"])) != 0 {
		t.Fatal("explicit removal failed")
	}
}

func TestEmailAttachmentBytesParticipateInIdempotency(t *testing.T) {
	input := CreateRequestInput{Title: "Files", Mutations: []MutationInput{{Type: GmailSendEmailOperation, Account: "sender@example.test",
		Message: map[string]any{"to": []string{"a@example.test"}, "subject": "Files", "body_text": "Attached",
			"attachments": []any{map[string]any{"filename": "file", "content_type": "text/plain", "data_base64": "YQ=="}}},
	}}}
	normalized, err := normalizeForStorage(input)
	if err != nil {
		t.Fatal(err)
	}
	first, err := requestIdempotencyKey(input, normalized)
	if err != nil {
		t.Fatal(err)
	}
	input.Mutations[0].Message["attachments"].([]any)[0].(map[string]any)["data_base64"] = "Yg=="
	normalized, err = normalizeForStorage(input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := requestIdempotencyKey(input, normalized)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("different attachment bytes deduplicated")
	}
}

func TestEmailAttachmentJSONBodyLimits(t *testing.T) {
	var result map[string]any
	for _, n := range []int{65536, 100000} {
		req := httptest.NewRequest("POST", "/", strings.NewReader(`{"data":"`+strings.Repeat("x", n)+`"}`))
		if err := decodeOptionalJSONLimit(req, &result, 32<<20); err != nil {
			t.Fatal(err)
		}
	}
	// A valid JSON prefix followed by excess whitespace must not bypass the cap.
	req := httptest.NewRequest("POST", "/", strings.NewReader("{}"+strings.Repeat(" ", 65536)))
	if err := decodeOptionalJSON(req, &result); err == nil {
		t.Fatal("accepted oversized JSON")
	}
}
