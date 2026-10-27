package mutations

import (
	"strings"
	"testing"
)

// The signature appended to a proposal comes from Zach's newest sent message.
// Its plain-text part runs on past the signature into whatever followed it, so
// on 2026-10-27 a forward sent the day before rode along as "signature" in the
// text part of a daily email. Only the gmail_signature block is the signature.
func TestGmailSignatureFromSentHTMLIgnoresTrailingContent(t *testing.T) {
	signatureHTML := `<div dir="ltr" class="gmail_signature" data-smartmail="gmail_signature"><div dir="ltr"><div dir="ltr"><span>--</span><br></div>` +
		`<div dir="ltr"><font>Zach Latta<br>Founder + Executive Director, <a href="https://hackclub.com">Hack Club</a></font></div>` +
		`<div dir="ltr"><font><a href="mailto:zach@hackclub.com">zach@hackclub.com</a> | <a href="https://hackclub.com/donate">Donate</a> | <a href="https://github.com/hackclub">GitHub</a></font></div><div></div></div></div>`
	cases := []struct{ name, trailer string }{
		{"forward", `<br><br><div class="gmail_quote"><div dir="ltr" class="gmail_attr">---------- Forwarded message ---------<br>From: &lt;<a href="mailto:someone@example.test">someone@example.test</a>&gt;<br>Subject: Private plans<br></div><br>Dear Zach, private text</div>`},
		{"wrapped reply attribution", `<div><br></div><div class="gmail_quote"><div dir="ltr" class="gmail_attr">On Sun, Sep 13, 2026 at 7:50 PM Someone Else &lt;someone@example.test&gt;<br>wrote:<br></div><blockquote class="gmail_quote">Hey Zach, private text</blockquote></div>`},
	}
	for _, tc := range cases {
		sentHTML := `<div dir="ltr"><div>fyi, see below</div><div><br></div><div>` + signatureHTML + `</div>` + tc.trailer + `</div>`

		signature := gmailSignatureFromSentHTML(sentHTML)
		if signature.HTML != signatureHTML {
			t.Fatalf("%s: signature HTML = %q", tc.name, signature.HTML)
		}
		out := appendGmailSignatureToMessage(map[string]any{"body_text": "Hello there.", "body_html": ""}, signature)
		want := "Hello there.\n\n--\nZach Latta\nFounder + Executive Director, Hack Club\nzach@hackclub.com | Donate | GitHub\n"
		if got := stringFromAny(out["body_text"]); got != want {
			t.Fatalf("%s: body_text = %q, want %q", tc.name, got, want)
		}
		if bodyHTML := stringFromAny(out["body_html"]); strings.Contains(bodyHTML, "private text") || strings.Contains(bodyHTML, "Forwarded message") {
			t.Fatalf("%s: body_html leaked the trailer: %q", tc.name, bodyHTML)
		}
	}
}

func TestGmailSignatureFromSentHTMLWithoutASignatureBlockIsEmpty(t *testing.T) {
	if signature := gmailSignatureFromSentHTML(`<div>Hi</div><div>--<br>Zach</div>`); !signature.Empty() {
		t.Fatalf("signature = %+v", signature)
	}
}

func TestExtractGmailSignatureHTMLIgnoresSignatureInsideOrAfterGmailQuote(t *testing.T) {
	otherSignatureHTML := `<div dir="ltr" class="gmail_signature" data-smartmail="gmail_signature"><div>--<br>Someone Else</div></div>`
	cases := []struct {
		name string
		html string
	}{
		{
			name: "nested in gmail_quote div",
			html: `<div dir="ltr"><div>Sounds good!</div><br><div class="gmail_quote"><div dir="ltr" class="gmail_attr">On Thu, Sep 24, 2026 Someone Else &lt;someone@example.test&gt; wrote:<br></div><blockquote class="gmail_quote"><div>Thanks!</div>` + otherSignatureHTML + `</blockquote></div></div>`,
		},
		{
			name: "nested in gmail_quote_container div",
			html: `<div dir="ltr"><div>Got it.</div><div class="gmail_quote gmail_quote_container"><blockquote class="gmail_quote">` + otherSignatureHTML + `</blockquote></div></div>`,
		},
		{
			name: "nested in gmail_quote blockquote without outer div",
			html: `<div>On Thu, Sep 24, 2026 Someone Else wrote:<br><blockquote class="gmail_quote" style="margin:0 0 0 .8ex">` + otherSignatureHTML + `</blockquote></div>`,
		},
		{
			name: "after first gmail_quote sibling",
			html: `<div>Reply</div><blockquote class="gmail_quote">Quoted text</blockquote>` + otherSignatureHTML,
		},
		{
			name: "same div carries gmail_signature and gmail_quote",
			html: `<div>Reply</div><div class="gmail_signature gmail_quote"><div>--<br>Someone Else</div></div>`,
		},
	}
	for _, tc := range cases {
		if got := extractGmailSignatureHTML(tc.html); got != "" {
			t.Fatalf("%s: extractGmailSignatureHTML = %q, want empty", tc.name, got)
		}
		if signature := gmailSignatureFromSentHTML(tc.html); !signature.Empty() {
			t.Fatalf("%s: gmailSignatureFromSentHTML = %+v, want empty", tc.name, signature)
		}
	}
}

func TestGmailSignatureFromSentMessagesFallsBackPastQuotedOnlyNewestMessage(t *testing.T) {
	otherSignatureHTML := `<div dir="ltr" class="gmail_signature"><div>--<br>Someone Else</div></div>`
	ownSignatureHTML := `<div dir="ltr" class="gmail_signature"><div>--<br>Zach Latta<br><a href="https://hackclub.com">Hack Club</a></div></div>`

	newestQuotedOnly := `<div dir="ltr"><div>On it!</div><div class="gmail_quote"><blockquote class="gmail_quote"><div>Can you check?</div>` + otherSignatureHTML + `</blockquote></div></div>`
	secondWithoutDiv := `<div>Quick note</div>`
	thirdWithOwnSignature := `<div dir="ltr"><div>Here you go.</div><div><br></div>` + ownSignatureHTML + `<div class="gmail_quote"><blockquote class="gmail_quote">` + otherSignatureHTML + `</blockquote></div></div>`

	got := gmailSignatureFromSentMessages([]string{newestQuotedOnly, secondWithoutDiv, thirdWithOwnSignature})
	if got.HTML != ownSignatureHTML {
		t.Fatalf("signature HTML = %q, want %q", got.HTML, ownSignatureHTML)
	}

	if none := gmailSignatureFromSentMessages([]string{newestQuotedOnly, secondWithoutDiv}); !none.Empty() {
		t.Fatalf("expected empty signature when all candidates are quote-only, got %+v", none)
	}
}

func TestGmailEmailMessageViewIgnoresQuotedSignatureWhenDraftHasNone(t *testing.T) {
	otherSignatureHTML := `<div dir="ltr" class="gmail_signature"><div>--<br>Someone Else</div></div>`
	ownSignatureHTML := `<div dir="ltr" class="gmail_signature"><div>--<br>Zach Latta</div></div>`

	withoutOwnSig := gmailEmailMessageView(map[string]any{
		"to":        []any{"someone@example.test"},
		"subject":   "Re: hello",
		"body_html": `<div>Sounds good.</div><div class="gmail_quote"><blockquote class="gmail_quote"><div>Hi Zach</div>` + otherSignatureHTML + `</blockquote></div>`,
	})
	if got := stringFromAny(withoutOwnSig["signature_html"]); got != "" {
		t.Fatalf("signature_html = %q, want empty", got)
	}
	if got := stringFromAny(withoutOwnSig["signature_text"]); got != "" {
		t.Fatalf("signature_text = %q, want empty", got)
	}
	if !strings.Contains(stringFromAny(withoutOwnSig["quoted_html"]), "Someone Else") {
		t.Fatalf("quoted_html = %q", withoutOwnSig["quoted_html"])
	}

	withOwnSig := gmailEmailMessageView(map[string]any{
		"to":        []any{"someone@example.test"},
		"subject":   "Re: hello",
		"body_html": `<div>Sounds good.</div><div><br></div>` + ownSignatureHTML + `<div class="gmail_quote"><blockquote class="gmail_quote"><div>Hi Zach</div>` + otherSignatureHTML + `</blockquote></div>`,
	})
	if got := stringFromAny(withOwnSig["signature_html"]); got != ownSignatureHTML {
		t.Fatalf("signature_html = %q, want %q", got, ownSignatureHTML)
	}
	if got := stringFromAny(withOwnSig["signature_text"]); got != "--\nZach Latta" {
		t.Fatalf("signature_text = %q, want %q", got, "--\nZach Latta")
	}
}
