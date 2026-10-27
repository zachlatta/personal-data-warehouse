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
