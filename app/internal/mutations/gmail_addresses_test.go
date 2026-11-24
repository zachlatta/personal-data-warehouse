package mutations

import (
	"reflect"
	"testing"
)

func TestFormatDisplayEmailAddress(t *testing.T) {
	cases := []struct {
		name string
		addr string
		want string
	}{
		{"Ada Example", "ada@example.test", "Ada Example <ada@example.test>"},
		{"Doe, Jane", "jane@example.test", `"Doe, Jane" <jane@example.test>`},
		{`Ada "Ace" \ Example`, "ada@example.test", `"Ada \"Ace\" \\ Example" <ada@example.test>`},
		{"Dr. Ada Example", "ada@example.test", `"Dr. Ada Example" <ada@example.test>`},
		{"Renée Müller", "renee@example.test", "Renée Müller <renee@example.test>"},
		{"", "ada@example.test", "ada@example.test"},
		{"ada@example.test", "ada@example.test", "ada@example.test"},
		{"ADA@EXAMPLE.TEST", "ada@example.test", "ada@example.test"},
		{"Ada\r\nExample", "ada@example.test", "Ada Example <ada@example.test>"},
	}
	for _, tc := range cases {
		if got := formatDisplayEmailAddress(tc.name, tc.addr); got != tc.want {
			t.Errorf("formatDisplayEmailAddress(%q, %q) = %q, want %q", tc.name, tc.addr, got, tc.want)
		}
	}
}

func TestSplitEmailAddressListHandlesQuotedCommas(t *testing.T) {
	input := "\"Doe, Jane\" <jane@example.test>, Ada Example <ada@example.test>\nbare@example.test, \"Smith, \\\"Bob\\\"\" <bob@example.test>"
	got := splitEmailAddressList(input)
	want := []string{
		`"Doe, Jane" <jane@example.test>`,
		"Ada Example <ada@example.test>",
		"bare@example.test",
		`"Smith, \"Bob\"" <bob@example.test>`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("splitEmailAddressList() = %#v, want %#v", got, want)
	}
}

func TestApplyGmailEmailDisplayNames(t *testing.T) {
	mutations := []storedMutation{{
		Provider:  "gmail",
		Operation: GmailSendEmailOperation,
		Account:   "sender@example.test",
		Payload: map[string]any{
			"delivery_mode": "send",
			"message": map[string]any{
				"to":        []string{"alice@example.test", "Custom Bob <bob@example.test>"},
				"cc":        []string{"comma@example.test", "unknown@example.test"},
				"bcc":       []string{"carol@example.test"},
				"subject":   "Hello",
				"body_text": "Body",
			},
			"variants": []map[string]any{
				{
					"id":    "variant_1",
					"title": "Short note",
					"message": map[string]any{
						"to":        []string{"alice@example.test", "Custom Bob <bob@example.test>"},
						"cc":        []string{"comma@example.test", "unknown@example.test"},
						"bcc":       []string{"carol@example.test"},
						"subject":   "Hello",
						"body_text": "Body",
					},
				},
				{
					"id":    "variant_2",
					"title": "Long note",
					"message": map[string]any{
						"to":        []string{"carol@example.test"},
						"cc":        []string{},
						"bcc":       []string{},
						"subject":   "Hello",
						"body_text": "Longer body",
					},
				},
			},
			"selected_variant_id": "variant_1",
		},
		Preview: map[string]any{
			"email": map[string]any{
				"to": []string{"alice@example.test", "Custom Bob <bob@example.test>"},
			},
		},
	}}

	names := map[string]string{
		"sender@example.test": "Ada Example",
		"alice@example.test":  "Alice Example",
		"bob@example.test":    "Warehouse Bob",
		"comma@example.test":  "Doe, Jane",
		"carol@example.test":  "Renée Carol",
	}

	got := applyGmailEmailDisplayNames(mutations, names)
	msg := mapFromAny(got[0].Payload["message"])
	if msg["from"] != "Ada Example <sender@example.test>" {
		t.Fatalf("from = %q", msg["from"])
	}
	wantTo := []string{"Alice Example <alice@example.test>", "Custom Bob <bob@example.test>"}
	if !reflect.DeepEqual(stringSliceFromAny(msg["to"]), wantTo) {
		t.Fatalf("to = %#v, want %#v", msg["to"], wantTo)
	}
	wantCC := []string{`"Doe, Jane" <comma@example.test>`, "unknown@example.test"}
	if !reflect.DeepEqual(stringSliceFromAny(msg["cc"]), wantCC) {
		t.Fatalf("cc = %#v, want %#v", msg["cc"], wantCC)
	}
	wantBCC := []string{"Renée Carol <carol@example.test>"}
	if !reflect.DeepEqual(stringSliceFromAny(msg["bcc"]), wantBCC) {
		t.Fatalf("bcc = %#v, want %#v", msg["bcc"], wantBCC)
	}

	variants := normalizeStoredEmailVariants(got[0].Payload["variants"])
	v2Msg := mapFromAny(variants[1]["message"])
	if v2Msg["from"] != "Ada Example <sender@example.test>" {
		t.Fatalf("variant 2 from = %q", v2Msg["from"])
	}
	if !reflect.DeepEqual(stringSliceFromAny(v2Msg["to"]), wantBCC) {
		t.Fatalf("variant 2 to = %#v, want %#v", v2Msg["to"], wantBCC)
	}

	previewEmail := mapFromAny(got[0].Preview["email"])
	if previewEmail["from"] != "Ada Example <sender@example.test>" {
		t.Fatalf("preview from = %q", previewEmail["from"])
	}
	if !reflect.DeepEqual(stringSliceFromAny(previewEmail["to"]), wantTo) {
		t.Fatalf("preview to = %#v, want %#v", previewEmail["to"], wantTo)
	}
}

func TestGmailEmailViewIncludesFromAndFallsBackToAccount(t *testing.T) {
	withFrom := sendEmailFixture().Mutations[0]
	withFrom.Payload["message"].(map[string]any)["from"] = "Ada Example <zach@example.test>"
	view := gmailEmailView(withFrom)
	if got := mapFromAny(view["message"])["from"]; got != "Ada Example <zach@example.test>" {
		t.Fatalf("message.from = %q", got)
	}

	withoutFrom := sendEmailFixture().Mutations[0]
	viewFallback := gmailEmailView(withoutFrom)
	if got := mapFromAny(viewFallback["message"])["from"]; got != withoutFrom.Account {
		t.Fatalf("fallback message.from = %q, want %q", got, withoutFrom.Account)
	}
	variants := viewFallback["variants"].([]map[string]any)
	if got := variants[0]["from"]; got != withoutFrom.Account {
		t.Fatalf("fallback variant[0].from = %q, want %q", got, withoutFrom.Account)
	}
}
