package mutations

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

const sampleRichEmailEditorHTML = `<p>Hi Alex and Jordan,</p>

<p>Here are three ways we could run the spring workshop:</p>

<ol>
  <li><strong>Option one:</strong> We host it in the library meeting room on a Saturday.</li>
  <li><strong>Option two:</strong> We split it into two shorter evening sessions.</li>
  <li><strong>Option three:</strong> People sign up at <a href="https://example.org/apply">https://example.org/apply</a> and we confirm in about 3 days.</li>
</ol>

<p>A few logistics notes:</p>

<ul>
  <li>We will keep each session under two hours.</li>
  <li>The room rules are at <a href="https://example.org/conduct">https://example.org/conduct</a> and <a href="https://example.org/safety">https://example.org/safety</a>.</li>
  <li>Bring a laptop if you have one; we have a few spares.</li>
</ul>

<p>The budget is about $100 per session, covering snacks, rooms, or supplies.</p>

<p>Best,<br>Zach</p>`

const sampleSignatureHTML = `<div dir="ltr" class="gmail_signature"><div dir="ltr">--<br>Zach Latta<br><a href="https://example.org">Example Org</a></div></div>`

const sampleQuotedHTML = `<div class="gmail_quote gmail_quote_container"><div dir="ltr" class="gmail_attr">On Tue, Mar 3, 2026 at 9:15 AM, alex@example.org wrote:<br></div><blockquote class="gmail_quote" style="margin:0 0 0 .8ex;border-left:1px solid #ccc;padding-left:1ex"><div>Hi Zach,<br>Looking forward to it!</div></blockquote></div>`

const sampleQuotedText = "On Tue, Mar 3, 2026 at 9:15 AM, alex@example.org wrote:\n> Hi Zach,\n> Looking forward to it!"

func sampleRichEmailMutation() Mutation {
	fullHTML := strings.Join([]string{sampleRichEmailEditorHTML, sampleSignatureHTML, sampleQuotedHTML}, "<div><br></div>")
	fullText := strings.Join([]string{htmlEmailText(sampleRichEmailEditorHTML), htmlFragmentText(sampleSignatureHTML), sampleQuotedText}, "\n\n") + "\n"
	return Mutation{
		ID:        "mut_rich_1",
		Provider:  "gmail",
		Operation: GmailSendEmailOperation,
		Account:   "zach@example.org",
		Status:    "pending_review",
		Payload: map[string]any{
			"delivery_mode": "send",
			"message": map[string]any{
				"to":                 []string{"alex@example.org"},
				"cc":                 []string{"jordan@example.org"},
				"bcc":                []string{},
				"subject":            "Re: Working together",
				"body_html":          fullHTML,
				"body_text":          fullText,
				"reply_to_thread_id": "thread_1",
			},
		},
		Preview: map[string]any{},
	}
}

func TestOldPlainTextClientEditPreservesRichHTMLFormatting(t *testing.T) {
	mutation := sampleRichEmailMutation()

	oldPlain := htmlEmailTextWithoutListMarkers(sampleRichEmailEditorHTML)
	editedPlain := strings.Replace(
		oldPlain,
		"covering snacks, rooms, or supplies.",
		"covering snacks, rooms, or supplies - same as last spring.",
		1,
	)
	if editedPlain == oldPlain {
		t.Fatal("test setup failed: expected replacement to match")
	}
	oldClientHTML := strings.Join([]string{
		emailPlainTextToHTML(editedPlain),
		sampleSignatureHTML,
		sampleQuotedHTML,
	}, "<div><br></div>")

	payload, _, _, err := updatedGmailEmailPayload(mutation, UpdateGmailEmailMutationInput{
		DeliveryMode: "send",
		Message: map[string]any{
			"to":        []string{"alex@example.org"},
			"cc":        []string{"jordan@example.org"},
			"subject":   "Re: Working together",
			"body_html": oldClientHTML,
			"body_text": editedPlain + "\n",
		},
	})
	if err != nil {
		t.Fatalf("updatedGmailEmailPayload: %v", err)
	}
	msg := mapFromAny(payload["message"])
	gotHTML := stringFromAny(msg["body_html"])
	gotText := stringFromAny(msg["body_text"])

	for _, want := range []string{
		"<ol>",
		"<li><strong>Option one:</strong>",
		"<li><strong>Option two:</strong>",
		`<a href="https://example.org/apply">https://example.org/apply</a>`,
		"<ul>",
		`<a href="https://example.org/conduct">https://example.org/conduct</a>`,
		`<a href="https://example.org/safety">https://example.org/safety</a>`,
		"covering snacks, rooms, or supplies - same as last spring.",
		`class="gmail_signature"`,
		`class="gmail_quote gmail_quote_container"`,
	} {
		if !strings.Contains(gotHTML, want) {
			t.Errorf("patched body_html missing %q\ngot:\n%s", want, gotHTML)
		}
	}

	for _, want := range []string{
		"1. Option one:",
		"2. Option two:",
		"3. Option three:",
		"- We will keep each session",
		"covering snacks, rooms, or supplies - same as last spring.",
		"> Hi Zach,",
	} {
		if !strings.Contains(gotText, want) {
			t.Errorf("patched body_text missing %q\ngot:\n%s", want, gotText)
		}
	}
}

func TestOldPlainTextClientEditInsideBoldAndLinkParagraphKeepsInlineTags(t *testing.T) {
	mutation := sampleRichEmailMutation()
	editedPlain := strings.Replace(
		htmlEmailText(sampleRichEmailEditorHTML),
		"in about 3 days.",
		"in about 2-3 days.",
		1,
	)
	oldClientHTML := strings.Join([]string{
		emailPlainTextToHTML(editedPlain),
		sampleSignatureHTML,
		sampleQuotedHTML,
	}, "<div><br></div>")

	payload, _, _, err := updatedGmailEmailPayload(mutation, UpdateGmailEmailMutationInput{
		DeliveryMode: "send",
		Message: map[string]any{
			"to":        []string{"alex@example.org"},
			"subject":   "Re: Working together",
			"body_html": oldClientHTML,
			"body_text": editedPlain,
		},
	})
	if err != nil {
		t.Fatalf("updatedGmailEmailPayload: %v", err)
	}
	gotHTML := stringFromAny(mapFromAny(payload["message"])["body_html"])
	if !strings.Contains(gotHTML, `<li><strong>Option three:</strong> People sign up at <a href="https://example.org/apply">https://example.org/apply</a> and we confirm in about 2-3 days.</li>`) {
		t.Fatalf("inline tags lost when editing list item:\n%s", gotHTML)
	}
}

// Table test covering all tricky old-client edit shapes: every case must either
// produce HTML whose visible words match the edited text exactly (with no
// untouched inline formatting lost), OR refuse with ErrOldClientEditUnsupported
// rather than sending wrong text or broken formatting.
func TestOldPlainTextClientEditCasesNeverSendWrongText(t *testing.T) {
	type tc struct {
		name      string
		html      string
		from      string
		to        string
		mustApply bool // common edits that must succeed rather than refuse
		wantHTML  []string
	}
	cases := []tc{
		{
			name:      "gmail compose bare first line in div",
			html:      `<div dir="ltr">Hi Alex,<div><br></div><div>I like <b>this</b> plan.</div><div><br></div><div>Zach</div></div>`,
			from:      "plan.",
			to:        "plan a lot.",
			mustApply: true,
			wantHTML:  []string{"<b>this</b>", "plan a lot."},
		},
		{
			name:      "bare text + br body",
			html:      `Hi Alex,<br><br>I like <b>this</b> plan, see <a href="https://example.org">here</a>.<br><br>Zach`,
			from:      "plan,",
			to:        "idea,",
			mustApply: true,
			wantHTML:  []string{"<b>this</b>", `<a href="https://example.org">here</a>`, "idea,"},
		},
		{
			name:      "nested list",
			html:      `<ul><li>Parent one<ul><li>Child <b>A</b></li><li>Child B</li></ul></li><li>Parent two</li></ul><p>End</p>`,
			from:      "End",
			to:        "The end",
			mustApply: true,
			wantHTML:  []string{"<b>A</b>", "The end"},
		},
		{
			name:      "loose list li>p",
			html:      `<ol><li><p><strong>First:</strong> one</p></li><li><p>Second</p></li></ol><p>End</p>`,
			from:      "End",
			to:        "The end",
			mustApply: true,
			wantHTML:  []string{"<strong>First:</strong>", "The end"},
		},
		{
			name:      "edit in multi-line p with inline",
			html:      `<p>Hi <b>Bob</b> there,<br>This is the second line.<br>Third line.</p>`,
			from:      "there,",
			to:        "friend,",
			mustApply: true,
			wantHTML:  []string{"<b>Bob</b>", "friend,", "This is the second line.", "Third line."},
		},
		{
			name:      "delete a br line",
			html:      `<p>Best,<br>Zach<br>Phone 555</p>`,
			from:      "\nPhone 555",
			to:        "",
			mustApply: true,
		},
		{
			name:      "add a br line at end",
			html:      `<p>Best,<br>Zach</p>`,
			from:      "Zach",
			to:        "Zach\nPresident",
			mustApply: true,
		},
		{
			name:      "insert line inside br para",
			html:      `<p>Line one<br>Line two<br>Line three</p>`,
			from:      "Line two",
			to:        "Line one and half\nLine two",
			mustApply: true,
		},
		{
			name:      "source newline + bold + link",
			html:      "<p>We have\n a <strong>bold</strong> thing and\n a <a href=\"https://example.org\">link</a> to see.</p>",
			from:      "see.",
			to:        "look at.",
			mustApply: true,
			wantHTML:  []string{"<strong>bold</strong>", `<a href="https://example.org">link</a>`, "look at."},
		},
		{
			name:      "two edits in para with bold and link",
			html:      `<p>We have a <strong>bold</strong> thing and a <a href="https://example.org">link</a> to see.</p>`,
			from:      "We have a bold thing and a link to see.",
			to:        "They have a bold thing and a link to look at.",
			mustApply: true,
			wantHTML:  []string{"<strong>bold</strong>", `<a href="https://example.org">link</a>`, "They have", "look at."},
		},
		{
			name:      "table cell edit",
			html:      `<table><tbody><tr><td>Item</td><td>Cost</td></tr><tr><td>Parts</td><td>$100</td></tr></tbody></table>`,
			from:      "$100",
			to:        "$150",
			mustApply: true,
			wantHTML:  []string{"<td>Parts</td>", "<td>$150</td>"},
		},
		{
			name:      "table first cell edit keeps bold and link",
			html:      `<table><tbody><tr><td><b>Parts</b></td><td>$100</td><td><a href="https://example.org">x</a></td></tr></tbody></table>`,
			from:      "Parts",
			to:        "Spare parts",
			mustApply: true,
			wantHTML:  []string{"<b>Spare parts</b>", "<td>$100</td>", `<a href="https://example.org">x</a>`},
		},
		{
			name:      "add paragraph after table lands outside tbody",
			html:      `<table><tbody><tr><td>A</td><td>B</td></tr></tbody></table><p>End</p>`,
			from:      "A\tB",
			to:        "A\tB\n\nNew text",
			mustApply: true,
			wantHTML:  []string{"</table><p>New text</p>"},
		},
		{
			name:      "para after list at end of doc stays paragraph not li",
			html:      `<p>Intro</p><ul><li>One</li><li>Two</li></ul>`,
			from:      "- Two",
			to:        "- Two\n\nBest,\nZach",
			mustApply: true,
			wantHTML:  []string{"</ul><p>Best,<br/>Zach</p>"},
		},
		{
			name:      "list item added at start",
			html:      `<p>Intro</p><ul><li>One</li><li>Two</li></ul>`,
			from:      "- One",
			to:        "- Zero\n- One",
			mustApply: true,
			wantHTML:  []string{"<li>Zero</li><li>One</li>"},
		},
		{
			name:      "append after bold-ended line",
			html:      `<p><b>Best,</b></p><p>Zach</p>`,
			from:      "Best,",
			to:        "Best, and thanks,",
			mustApply: true,
			wantHTML:  []string{"<b>Best,</b> and thanks,"},
		},
		{
			name:      "append after link does not extend href",
			html:      `<p>See <a href="https://example.org">https://example.org</a></p><p>x</p>`,
			from:      "https://example.org",
			to:        "https://example.org for details",
			mustApply: true,
			wantHTML:  []string{`<a href="https://example.org">https://example.org</a> for details`},
		},
		{
			name:      "blank line added between div lines",
			html:      `<div>One <b>b</b></div><div>Two</div>`,
			from:      "One b\n",
			to:        "One b\n\n",
			mustApply: true,
			wantHTML:  []string{"<div>One <b>b</b></div><div><br/></div><div>Two</div>"},
		},
		{
			name:      "blank line removed between div lines",
			html:      `<div>One <b>b</b></div><div><br></div><div>Two</div>`,
			from:      "One b\n\n",
			to:        "One b\n",
			mustApply: true,
			wantHTML:  []string{"<div>One <b>b</b></div><div>Two</div>"},
		},
		{
			name:      "li whose text starts with digits",
			html:      `<ul><li>1. Alpha</li><li>2. Beta</li></ul><p>End</p>`,
			from:      "End",
			to:        "The end",
			mustApply: true,
			wantHTML:  []string{"<li>1. Alpha</li>", "The end"},
		},
		{
			name:      "img kept when text near edited",
			html:      `<p><img src="cid:a1">Caption</p><p>End</p>`,
			from:      "Caption",
			to:        "New caption",
			mustApply: true,
			wantHTML:  []string{`<img src="cid:a1"`, "New caption"},
		},
		{
			name:      "nbsp preserved on edit",
			html:      `<p>Hi&nbsp;Bob,&nbsp; <b>x</b> y</p>`,
			from:      " y",
			to:        " z",
			mustApply: true,
			wantHTML:  []string{"<b>x</b>"},
		},
		{
			name:      "blockquote para",
			html:      `<blockquote><p>One</p><p>Two <b>b</b></p></blockquote><p>End</p>`,
			from:      "Two b",
			to:        "Two bb",
			mustApply: true,
			wantHTML:  []string{"<b>bb</b>"},
		},
		{
			name:      "heading + bare text + p",
			html:      `<h2>Title</h2>Bare text under<p>End <b>b</b></p>`,
			from:      "End b",
			to:        "End b.",
			mustApply: true,
			wantHTML:  []string{"<h2>Title</h2>", "Bare text under", "<b>b</b>."},
		},
		{
			name:      "center and span with bold",
			html:      `<center>Hello</center><span>Span <b>x</b></span><p>End</p>`,
			from:      "End",
			to:        "The end",
			mustApply: true,
			wantHTML:  []string{"<center>Hello</center>", "<b>x</b>", "The end"},
		},
		{
			name: "paragraph merge of p",
			html: `<p>One <b>b</b></p><p>Two <i>i</i></p>`,
			from: "One b\n\nTwo i",
			to:   "One b Two i",
		},
		{
			name: "swap paragraphs",
			html: `<p>One <b>b</b></p><p>Two <i>i</i></p><p>End</p>`,
			from: "One b\n\nTwo i",
			to:   "Two i\n\nOne b",
		},
		{
			name: "pre edit refuses safely",
			html: "<pre>a\n  b\nc</pre><p>End</p>",
			from: "  b",
			to:   "  bb",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			base := htmlEmailText(sanitizeEmailEditorHTML(c.html))
			idx := strings.LastIndex(base, c.from)
			if idx < 0 {
				t.Fatalf("setup: base %q does not contain %q", base, c.from)
			}
			edited := base[:idx] + c.to + base[idx+len(c.from):]
			got, err := patchRichHTMLWithPlainTextEdit(c.html, edited)
			if err != nil {
				if !errors.Is(err, ErrOldClientEditUnsupported) {
					t.Fatalf("unexpected error: %v", err)
				}
				if c.mustApply {
					t.Fatalf("expected common edit to succeed, got refusal: %v\nbase=%q\nedited=%q", err, base, edited)
				}
				return
			}
			// Whenever patch succeeds, visible words MUST match edited text exactly.
			wantLines := []string{}
			for _, l := range strings.Split(edited, "\n") {
				l = stripListPrefix(strings.TrimSpace(strings.Join(strings.Fields(strings.ReplaceAll(l, "\u00a0", " ")), " ")))
				if l != "" {
					wantLines = append(wantLines, l)
				}
			}
			gotLines := visibleLinesOfHTML(got)
			gotTexts := make([]string, len(gotLines))
			for i, gl := range gotLines {
				gotTexts[i] = gl.text
			}
			if strings.Join(wantLines, "\n") != strings.Join(gotTexts, "\n") {
				t.Fatalf("visible lines mismatch:\nwant: %q\ngot:  %q\nhtml: %s", wantLines, gotTexts, got)
			}
			for _, w := range c.wantHTML {
				if !strings.Contains(got, w) {
					t.Errorf("result HTML missing %q\ngot: %s", w, got)
				}
			}
		})
	}
}

func TestMetadataOnlyEditLeavesBodyVerbatim(t *testing.T) {
	mutation := sampleRichEmailMutation()
	origMsg := mapFromAny(mutation.Payload["message"])
	origHTML := stringFromAny(origMsg["body_html"])
	origText := stringFromAny(origMsg["body_text"])

	notEdited := false
	payload, _, _, err := updatedGmailEmailPayload(mutation, UpdateGmailEmailMutationInput{
		DeliveryMode: "draft",
		BodyEdited:   &notEdited,
		Message: map[string]any{
			"to":        []string{"alex@example.org", "sam@example.org"},
			"subject":   "Re: Updated subject",
			"body_html": "<div>would have clobbered</div>",
			"body_text": "would have clobbered",
		},
	})
	if err != nil {
		t.Fatalf("updatedGmailEmailPayload: %v", err)
	}
	msg := mapFromAny(payload["message"])
	if stringFromAny(msg["body_html"]) != origHTML {
		t.Fatalf("body_html changed when BodyEdited=false")
	}
	if stringFromAny(msg["body_text"]) != origText {
		t.Fatalf("body_text changed when BodyEdited=false")
	}
	if stringFromAny(msg["subject"]) != "Re: Updated subject" {
		t.Fatalf("subject = %q, want updated", msg["subject"])
	}
}

func TestRichHTMLClientEditSanitizesAndPreservesSignatureAndQuote(t *testing.T) {
	mutation := sampleRichEmailMutation()
	editedHTML := `<p>Hi Alex,</p><h2>Plan</h2><table><tr><td>Item</td><td>Cost</td></tr><tr><td>Parts</td><td>$100</td></tr></table><p><img src="https://example.org/pic.png" alt="chart" onerror="alert(1)"></p><script>alert(2)</script><p><a href="javascript:alert(3)">bad</a> and <a href="https://example.org">good</a></p><p>Set once = 5 and only=yes, online=true.</p>`

	payload, _, _, err := updatedGmailEmailPayload(mutation, UpdateGmailEmailMutationInput{
		DeliveryMode: "send",
		EditorFormat: "html",
		Message: map[string]any{
			"to":        []string{"alex@example.org"},
			"subject":   "Re: Working together",
			"body_html": editedHTML,
			"body_text": "ignored stale text",
		},
	})
	if err != nil {
		t.Fatalf("updatedGmailEmailPayload: %v", err)
	}
	msg := mapFromAny(payload["message"])
	gotHTML := stringFromAny(msg["body_html"])
	gotText := stringFromAny(msg["body_text"])

	if strings.Contains(strings.ToLower(gotHTML), "<script") || strings.Contains(strings.ToLower(gotHTML), "onerror") || strings.Contains(strings.ToLower(gotHTML), "javascript:") {
		t.Fatalf("sanitization failed:\n%s", gotHTML)
	}
	for _, want := range []string{
		"<h2>Plan</h2>",
		"<table>",
		`<img src="https://example.org/pic.png" alt="chart"`,
		`<a href="https://example.org">good</a>`,
		"Set once = 5 and only=yes, online=true.",
		`class="gmail_signature"`,
		`class="gmail_quote gmail_quote_container"`,
	} {
		if !strings.Contains(gotHTML, want) {
			t.Errorf("rich HTML edit missing %q\ngot:\n%s", want, gotHTML)
		}
	}
	for _, want := range []string{"Plan", "Item\tCost", "Parts\t$100", "Set once = 5 and only=yes, online=true.", "> Hi Zach,"} {
		if !strings.Contains(gotText, want) {
			t.Errorf("derived body_text missing %q\ngot:\n%s", want, gotText)
		}
	}
}

func TestSanitizeEmailEditorHTMLBlocksXSSAndKeepsVisibleOnText(t *testing.T) {
	// Visible text containing "on...=" must never be altered.
	prose := `<p>Set once = 5 and only=yes, then onclick="x" in the docs.</p>`
	if got := sanitizeEmailEditorHTML(prose); !strings.Contains(got, `Set once = 5 and only=yes, then onclick=&#34;x&#34; in the docs.`) && !strings.Contains(got, `Set once = 5 and only=yes, then onclick="x" in the docs.`) {
		t.Fatalf("visible text altered: %s", got)
	}

	// Attack vectors must be stripped and idempotent.
	attacks := []string{
		`<img src='x'onerror='window.__x=1'>`,
		`<svg/onload=window.__x=1>`,
		`<svg><a><animate attributeName=href values="javascript:window.__x=1"/><text>click</text></a></svg>`,
		`<a href="java	script:window.__x=1">c</a>`,
		`<a href="data:image/svg+xml,<svg onload=window.__x=1>">c</a>`,
		`<link rel=stylesheet href="https://evil.example/x.css">`,
		`<img srcset="javascript:window.__x=1" src="http://example.org/a.png">`,
		`<div style="background:url(javascript:window.__x=1)">x</div>`,
		`<iframe srcdoc="<script>window.__x=1</script>"></iframe>`,
		`<xmp><p title="</xmp><img src=x onerror=window.__x=1>">`,
		`<plaintext><img src=x onerror=window.__x=1>`,
	}
	for _, a := range attacks {
		clean := sanitizeEmailEditorHTML(a)
		lower := strings.ToLower(clean)
		for _, forbidden := range []string{"<script", "<svg", "<iframe", "<link", "<xmp", "<plaintext", "onerror", "onload", "javascript:", "srcset"} {
			if strings.Contains(lower, forbidden) {
				t.Errorf("attack %q left %q in %q", a, forbidden, clean)
			}
		}
		if again := sanitizeEmailEditorHTML(clean); again != clean {
			t.Errorf("sanitizer not idempotent on %q: %q vs %q", a, clean, again)
		}
	}
	// http:// image URLs must NOT be rewritten to https://.
	if got := sanitizeEmailEditorHTML(`<img src="http://example.org/pic.png">`); !strings.Contains(got, `src="http://example.org/pic.png"`) {
		t.Fatalf("http image URL was altered: %s", got)
	}
}

func TestPlainTextTypedQuoteIsNotDuplicated(t *testing.T) {
	// M9: A plain-text email containing a typed "On ... wrote:" line with NO
	// gmail_quote HTML must NOT populate quoted_text or duplicate the quote on save.
	bodyText := "Thanks!\n\nOn Tue, Mar 3, 2026 at 9:15 AM, alex@example.org wrote:\n> Hello there\n"
	msg := map[string]any{
		"to":        []string{"alex@example.org"},
		"subject":   "Re: Hi",
		"body_text": bodyText,
		"body_html": "",
	}
	view := gmailEmailMessageView(msg)
	if got := stringFromAny(view["quoted_text"]); got != "" {
		t.Fatalf("expected empty quoted_text when quoted_html is absent, got %q", got)
	}
	mutation := Mutation{
		ID:        "mut_plain_quote",
		Provider:  "gmail",
		Operation: GmailSendEmailOperation,
		Account:   "zach@example.org",
		Status:    "pending_review",
		Payload:   map[string]any{"delivery_mode": "send", "message": msg},
		Preview:   map[string]any{},
	}
	payload, _, _, err := updatedGmailEmailPayload(mutation, UpdateGmailEmailMutationInput{
		DeliveryMode: "send",
		EditorFormat: "html",
		Message: map[string]any{
			"to":        []string{"alex@example.org"},
			"subject":   "Re: Hi",
			"body_html": "<div>Thanks a lot!</div><div><br></div><div>On Tue, Mar 3, 2026 at 9:15 AM, alex@example.org wrote:</div><div>&gt; Hello there</div>",
			"body_text": "Thanks a lot!\n\nOn Tue, Mar 3, 2026 at 9:15 AM, alex@example.org wrote:\n> Hello there\n",
		},
	})
	if err != nil {
		t.Fatalf("updatedGmailEmailPayload: %v", err)
	}
	gotText := stringFromAny(mapFromAny(payload["message"])["body_text"])
	if strings.Count(gotText, "On Tue, Mar 3, 2026 at 9:15 AM, alex@example.org wrote:") != 1 {
		t.Fatalf("quote duplicated in body_text:\n%s", gotText)
	}
}

func TestHTMLEmailTextFormatsListsAndTables(t *testing.T) {
	in := `<p>Intro:</p><ol><li><strong>First</strong> item</li><li>Second item</li></ol><ul><li>Bullet A</li><li>Bullet B</li></ul><table><tr><th>Col1</th><th>Col2</th></tr><tr><td>A</td><td>B</td></tr></table>`
	got := htmlEmailText(in)
	want := "Intro:\n\n1. First item\n2. Second item\n\n- Bullet A\n- Bullet B\n\nCol1\tCol2\nA\tB"
	if got != want {
		t.Fatalf("htmlEmailText =\n%q\nwant:\n%q", got, want)
	}
}

func TestGmailEmailMessageViewSanitizesEditorHTMLAndKeepsQuotedTextPrefixes(t *testing.T) {
	mutation := sampleRichEmailMutation()
	msg := mapFromAny(mutation.Payload["message"])
	msg["body_html"] = `<p>Hello <script>alert(1)</script><b onclick="evil()">world</b></p><div><br></div>` + sampleSignatureHTML + `<div><br></div>` + sampleQuotedHTML

	view := gmailEmailMessageView(msg)
	editorHTML := stringFromAny(view["editor_html"])
	if strings.Contains(editorHTML, "<script") || strings.Contains(editorHTML, "onclick") {
		t.Fatalf("editor_html not sanitized: %s", editorHTML)
	}
	if !strings.Contains(editorHTML, "<b>world</b>") {
		t.Fatalf("editor_html dropped safe bold tag: %s", editorHTML)
	}
	if got := stringFromAny(view["quoted_text"]); !strings.Contains(got, "> Hi Zach,") {
		t.Fatalf("quoted_text lost quote prefixes: %q", got)
	}
}

func TestPatchRichHTMLWithPlainTextEditSendBack2Regressions(t *testing.T) {
	cases := []struct {
		name       string
		html       string
		from       string
		to         string
		wantHTML   []string
		forbidHTML []string
		wantErr    bool
	}{
		{
			name:     "reported edit shape: drop final period and append phrase",
			html:     `<p>Intro <b>x</b></p><ol><li><strong>Option A:</strong> details</li></ol><p>snacks, rooms, or supplies.</p><p>End</p>`,
			from:     "supplies.",
			to:       "supplies - same as last time",
			wantHTML: []string{"<ol><li><strong>Option A:</strong> details</li></ol>", "<p>snacks, rooms, or supplies - same as last time</p>"},
		},
		{
			name:     "exclamation replaced by words keeps space",
			html:     `<p>Thanks!</p><p><b>Alex</b></p>`,
			from:     "Thanks!",
			to:       "Thanks so much",
			wantHTML: []string{"<p>Thanks so much</p>", "<b>Alex</b>"},
		},
		{
			name:     "question mark replaced by words and question mark",
			html:     `<p>Can we <b>meet</b>?</p>`,
			from:     "meet?",
			to:       "meet tomorrow?",
			wantHTML: []string{"<p>Can we <b>meet</b> tomorrow?</p>"},
		},
		{
			name:     "insert before bold lead-in stays outside bold",
			html:     `<ul><li><b>Cost:</b> five</li></ul>`,
			from:     "Cost:",
			to:       "Total Cost:",
			wantHTML: []string{"<li>Total <b>Cost:</b> five</li>"},
		},
		{
			name:     "insert before link at start stays outside link",
			html:     `<p><a href="https://example.org">Here</a> is the doc</p>`,
			from:     "Here is",
			to:       "Look: Here is",
			wantHTML: []string{`<p>Look: <a href="https://example.org">Here</a> is the doc</p>`},
		},
		{
			name:     "insert right after bold in middle stays outside bold",
			html:     `<p>We <b>must</b> go</p>`,
			from:     "must go",
			to:       "must really go",
			wantHTML: []string{"<p>We <b>must</b> really go</p>"},
		},
		{
			name:     "insert right after link in middle stays outside link",
			html:     `<p>See <a href="https://example.org">here</a> for more</p>`,
			from:     "here for",
			to:       "here today for",
			wantHTML: []string{`<p>See <a href="https://example.org">here</a> today for more</p>`},
		},
		{
			name:     "parenthesized link survives edit",
			html:     `<p>Read it (<a href="https://example.org">guide</a>) today</p>`,
			from:     "today",
			to:       "tomorrow",
			wantHTML: []string{`(<a href="https://example.org">guide</a>) tomorrow`},
		},
		{
			name:     "quoted link survives edit",
			html:     `<p>Read "<a href="https://example.org">the guide</a>" today</p>`,
			from:     "today",
			to:       "tomorrow",
			wantHTML: []string{`<a href="https://example.org">the guide</a>`, "tomorrow"},
		},
		{
			name:     "bold possessive survives edit",
			html:     `<p><b>Alex</b>'s plan is good</p>`,
			from:     "good",
			to:       "great",
			wantHTML: []string{"<b>Alex</b>&#39;s plan is great"},
		},
		{
			name:     "slash bold survives edit",
			html:     `<p><b>yes</b>/<b>no</b> question</p>`,
			from:     "question",
			to:       "answer",
			wantHTML: []string{"<b>yes</b>/<b>no</b> answer"},
		},
		{
			name:     "typed dash bullets in non-list divs survive edit",
			html:     `<div>Plan <b>bold</b>:</div><div>- first</div><div>- second</div><div>End</div>`,
			from:     "End",
			to:       "The end",
			wantHTML: []string{"<div>- first</div>", "<div>- second</div>", "<div>The end</div>"},
		},
		{
			name:     "typed numbered lines in br paragraph survive edit",
			html:     `<p>Plan <b>bold</b>:<br>1. first<br>2. second</p><p>End</p>`,
			from:     "End",
			to:       "The end",
			wantHTML: []string{"1. first<br/>2. second", "<p>The end</p>"},
		},
		{
			name:    "deleting heading and editing body is refused rather than changing style",
			html:    `<h2>Summary</h2><p>The budget is ready for review.</p><p>End <b>xx</b></p>`,
			from:    "Summary\n\nThe budget is ready for review.",
			to:      "The budget is nearly ready for review.",
			wantErr: true,
		},
		{
			name:    "deleting h1 and editing body is refused rather than changing style",
			html:    `<p>Hi</p><h1>Big Title</h1><p>Body.</p>`,
			from:    "Hi\n\nBig Title\n\nBody.",
			to:      "Hi\n\nBody changed.",
			wantErr: true,
		},
		{
			name:     "retyping heading keeps h2",
			html:     `<h2>Summary</h2><p>The budget is <b>fine</b>.</p>`,
			from:     "Summary",
			to:       "Overview",
			wantHTML: []string{"<h2>Overview</h2>", "<b>fine</b>"},
		},
		{
			name:     "retyping styled paragraph keeps style",
			html:     `<p style="margin:0 0 12px;font-family:Georgia">Hi Alex,</p><p style="margin:0 0 12px;font-family:Georgia">Cheers</p>`,
			from:     "Cheers",
			to:       "Thanks",
			wantHTML: []string{`<p style="margin:0 0 12px;font-family:Georgia">Thanks</p>`},
		},
		{
			name:     "retyping styled div keeps style",
			html:     `<div style="font-weight:700">Tuesday</div><div>ok</div>`,
			from:     "Tuesday",
			to:       "Wednesday",
			wantHTML: []string{`<div style="font-weight:700">Wednesday</div>`},
		},
		{
			name:     "retyping bold list item keeps bold",
			html:     `<ol><li><b>Milk</b></li><li>Bread</li></ol>`,
			from:     "Milk",
			to:       "Eggs",
			wantHTML: []string{"<ol><li><b>Eggs</b></li><li>Bread</li></ol>"},
		},
		{
			name:     "retyping styled list item keeps style",
			html:     `<ul><li style="color:red">Milk</li><li style="color:red">Bread</li></ul><p>x <b>y</b></p>`,
			from:     "Milk",
			to:       "Eggs",
			wantHTML: []string{`<li style="color:red">Eggs</li>`},
		},
		{
			name:     "retyping whole table row keeps row in table",
			html:     `<table><tr><td>Parts</td><td>$100</td></tr><tr><td>Food</td><td>$50</td></tr></table><p>ok</p>`,
			from:     "Food\t$50",
			to:       "Meals\t$60",
			wantHTML: []string{"<tr><td>Meals</td><td>$60</td></tr>"},
		},
		{
			name:     "retyping one-cell table keeps styled td",
			html:     `<table><tr><td style="font-size:20px">12</td></tr></table><p>ok</p>`,
			from:     "12",
			to:       "13",
			wantHTML: []string{`<td style="font-size:20px">13</td>`},
		},
		{
			name:     "retyping link button label keeps link and button styles",
			html:     `<p style="text-align:center"><a href="https://example.org/a" style="background:red;color:white">Sign up</a></p><p>ok</p>`,
			from:     "Sign up",
			to:       "Register",
			wantHTML: []string{`<p style="text-align:center"><a href="https://example.org/a" style="background:red;color:white">Register</a></p>`},
		},
		{
			name:     "double blank lines in div body survive edit on another line",
			html:     `<div>One</div><div><br></div><div><br></div><div>Two <b>b</b></div><div><br></div><div>Three</div>`,
			from:     "Three",
			to:       "Three more",
			wantHTML: []string{"<div>One</div><div><br/></div><div><br/></div><div>Two <b>b</b></div><div><br/></div><div>Three more</div>"},
		},
		{
			name:     "triple br blank lines survive edit on another line",
			html:     `One <b>b</b><br><br><br>Two<br>Three`,
			from:     "Three",
			to:       "Three more",
			wantHTML: []string{"One <b>b</b><br/><br/><br/>Two<br/>Three more"},
		},
		{
			name:    "blank line removed after list combined with word edit is refused",
			html:    `<ul><li>One</li><li>Two</li></ul><p>After list.</p>`,
			from:    "- Two\n\nAfter list.",
			to:      "- Two edited\nAfter list.",
			wantErr: true,
		},
		{
			name:     "blank line combined with word edit in div body is applied",
			html:     `<div>One <b>b</b></div><div>Two</div><div>Three</div>`,
			from:     "One b\nTwo",
			to:       "One b\n\nTwo more",
			wantHTML: []string{"<div>One <b>b</b></div><div><br/></div><div>Two more</div><div>Three</div>"},
		},
		{
			name:     "blank line combined with word edit in br body is applied",
			html:     `One <b>b</b><br>Two<br>Three`,
			from:     "One b\nTwo",
			to:       "One b\n\nTwo more",
			wantHTML: []string{"One <b>b</b><br/><br/>Two more<br/>Three"},
		},
		{
			name:     "new paragraph in middle of div body keeps blank lines on both sides",
			html:     `<div>Hi <b>x</b></div><div><br></div><div>Alex</div>`,
			from:     "\n\nAlex",
			to:       "\n\nNew para\n\nAlex",
			wantHTML: []string{"<div>Hi <b>x</b></div><div><br/></div><div>New para</div><div><br/></div><div>Alex</div>"},
		},
		// N5: kept text must never shift into a deleted neighbor's block (T1, S2, T4).
		{
			name:    "N5 T1 delete heading, edit para, add para is refused",
			html:    `<p>Hi <b>A</b></p><h2>Summary</h2><p>The budget is fine for now.</p><p>End</p>`,
			from:    "Summary\n\nThe budget is fine for now.",
			to:      "The budget is fine for this year.\n\nAnother new paragraph here.",
			wantErr: true,
		},
		{
			name:    "N5 S2 split paragraph and delete following heading is refused",
			html:    `<p>First one. Second sentence.</p><h2>Head</h2><p>x <b>b</b></p>`,
			from:    "First one. Second sentence.\n\nHead",
			to:      "First one.\n\nSecond sentence.",
			wantErr: true,
		},
		{
			name:    "N5 T4 split paragraph and drop fine print is refused",
			html:    `<p>Hi <b>A</b></p><p>First sentence here. Second sentence there.</p><p style="font-size:10px;color:#999">fine print</p>`,
			from:    "First sentence here. Second sentence there.\n\nfine print",
			to:      "First sentence here.\n\nSecond sentence there.",
			wantErr: true,
		},
		// N4: deleting paragraphs in <div>/<br>/<p>&nbsp;</p> bodies removes their spacers.
		{
			name:       "N4 delete middle div paragraph leaves single spacer",
			html:       `<div>One <b>b</b></div><div><br></div><div>Two</div><div><br></div><div>Three</div>`,
			from:       "One b\n\nTwo\n\nThree",
			to:         "One b\n\nThree",
			wantHTML:   []string{"<div>One <b>b</b></div><div><br/></div><div>Three</div>"},
			forbidHTML: []string{"<div><br/></div><div><br/></div>"},
		},
		{
			name:       "N4 delete middle br paragraph leaves single blank line",
			html:       `One <b>b</b><br><br>Two<br><br>Three`,
			from:       "One b\n\nTwo\n\nThree",
			to:         "One b\n\nThree",
			wantHTML:   []string{"One <b>b</b><br/><br/>Three"},
			forbidHTML: []string{"<br/><br/><br/>"},
		},
		{
			name:       "N4 delete first div paragraph leaves no leading spacer",
			html:       `<div>One <b>b</b></div><div><br></div><div>Two</div><div><br></div><div>Three</div>`,
			from:       "One b\n\nTwo",
			to:         "Two",
			wantHTML:   []string{"<div>Two</div><div><br/></div><div>Three</div>"},
			forbidHTML: []string{"<div><br/></div><div>Two</div>"},
		},
		{
			name:       "N4 delete last div paragraph leaves no trailing spacer",
			html:       `<div>One <b>b</b></div><div><br></div><div>Two</div><div><br></div><div>Three</div>`,
			from:       "Two\n\nThree",
			to:         "Two",
			wantHTML:   []string{"<div>One <b>b</b></div><div><br/></div><div>Two</div>"},
			forbidHTML: []string{"<div>Two</div><div><br/></div>"},
		},
		{
			name:       "N4 delete two div paragraphs leaves single spacer",
			html:       `<div>One <b>b</b></div><div><br></div><div>Two</div><div><br></div><div>Three</div><div><br></div><div>Four</div>`,
			from:       "Two\n\nThree\n\nFour",
			to:         "Four",
			wantHTML:   []string{"<div>One <b>b</b></div><div><br/></div><div>Four</div>"},
			forbidHTML: []string{"<div><br/></div><div><br/></div>"},
		},
		{
			name:       "N4 delete p paragraph with nbsp spacers leaves single nbsp spacer",
			html:       `<p>One <b>b</b></p><p>&nbsp;</p><p>Two</p><p>&nbsp;</p><p>Three</p>`,
			from:       "One b\n\nTwo\n\nThree",
			to:         "One b\n\nThree",
			wantHTML:   []string{"<p>One <b>b</b></p><p>\u00a0</p><p>Three</p>"},
			forbidHTML: []string{"<p>\u00a0</p><p>\u00a0</p>"},
		},
		// N6: retyping a line must not spread a partial link or bold over unrelated new words.
		{
			name:    "N6 retype line with link sharing only stopword is refused",
			html:    `<p>See <a href="https://example.org">the doc here</a> now</p><p>End</p>`,
			from:    "See the doc here now",
			to:      "Look at the calendar tonight",
			wantErr: true,
		},
		{
			name:    "N6 S4c retype line with link keeping only stopword and end word is refused",
			html:    `<p>See <a href="https://example.org">the doc here</a> now</p><p>End</p>`,
			from:    "See the doc here now",
			to:      "I put the meeting on your calendar now",
			wantErr: true,
		},
		{
			name:    "N6 S10 retype list item with bold lead is refused",
			html:    `<ul><li><b>Cost:</b> five dollars</li><li>Two</li></ul><p>End</p>`,
			from:    "- Cost: five dollars",
			to:      "- Bring your own lunch",
			wantErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			base := htmlEmailText(sanitizeEmailEditorHTML(c.html))
			idx := strings.LastIndex(base, c.from)
			if idx < 0 {
				t.Fatalf("base text %q does not contain %q", base, c.from)
			}
			edited := base[:idx] + c.to + base[idx+len(c.from):]
			got, err := patchRichHTMLWithPlainTextEdit(c.html, edited)
			if c.wantErr {
				if !errors.Is(err, ErrOldClientEditUnsupported) {
					t.Fatalf("expected ErrOldClientEditUnsupported, got err=%v html=%s", err, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for _, w := range c.wantHTML {
				if !strings.Contains(got, w) {
					t.Errorf("missing %q in %s", w, got)
				}
			}
			for _, f := range c.forbidHTML {
				if strings.Contains(got, f) {
					t.Errorf("forbidden %q present in %s", f, got)
				}
			}
		})
	}
}

func TestSanitizeEmailEditorHTMLPreservesAttributeEntitiesAndChecksBackgroundURL(t *testing.T) {
	in := `<p><a href="https://example.org/x?a=1&amp;b=2">link</a><img src="http://example.org/i?x=1&amp;w=2" alt="A &amp; B"><span style="font-family:&#34;Helvetica Neue&#34;,Arial">t</span></p><table background="javascript:alert(1)"><tr><td>ok</td></tr></table>`
	got := sanitizeEmailEditorHTML(in)
	if strings.Contains(got, "&amp;amp;") {
		t.Fatalf("double-escaped entity in sanitized HTML: %s", got)
	}
	if strings.Contains(strings.ToLower(got), "javascript:") {
		t.Fatalf("unsafe background URL survived sanitization: %s", got)
	}
	for _, want := range []string{
		`href="https://example.org/x?a=1&amp;b=2"`,
		`src="http://example.org/i?x=1&amp;w=2"`,
		`alt="A &amp; B"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %s", want, got)
		}
	}
}

func TestPatchRichHTMLWithPlainTextEditLargeInputPerformanceAndBound(t *testing.T) {
	// 6,000-line email with 1 edited line must complete quickly via prefix/suffix trim.
	var htmlBuf, txtBuf strings.Builder
	for i := 0; i < 6000; i++ {
		fmt.Fprintf(&htmlBuf, "<p>Line %d with <b>bold</b></p>", i)
		if i > 0 {
			txtBuf.WriteString("\n\n")
		}
		if i == 3000 {
			fmt.Fprintf(&txtBuf, "Line %d with bold edited", i)
		} else {
			fmt.Fprintf(&txtBuf, "Line %d with bold", i)
		}
	}
	got, err := patchRichHTMLWithPlainTextEdit(htmlBuf.String(), txtBuf.String())
	if err != nil {
		t.Fatalf("6000-line single edit failed: %v", err)
	}
	if !strings.Contains(got, "<p>Line 3000 with <b>bold</b> edited</p>") {
		t.Fatalf("edited line missing in 6000-line patch")
	}

	// Over-limit fully changed email must refuse quickly instead of O(N^2) DP.
	var bigOld, bigNew strings.Builder
	for i := 0; i < maxOldClientLCSLines+100; i++ {
		fmt.Fprintf(&bigOld, "<p>Old line %d <b>x</b></p>", i)
		fmt.Fprintf(&bigNew, "New line %d y\n\n", i)
	}
	if _, err := patchRichHTMLWithPlainTextEdit(bigOld.String(), bigNew.String()); !errors.Is(err, ErrOldClientEditUnsupported) {
		t.Fatalf("expected ErrOldClientEditUnsupported for oversized diff, got %v", err)
	}
}
