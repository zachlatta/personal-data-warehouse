package mutations

import "testing"

func TestExtractGmailSignatureTextCutsWrappedQuoteAttribution(t *testing.T) {
	body := "Thanks\n\n--\nZach Latta\nhttps://zachlatta.com\n\n\nOn Sun, Sep 13, 2026 at 7:50 PM Someone Else <someone@example.com>\nwrote:\n\n> Hey Zach,\n> private text\n"
	got := extractGmailSignatureText(body)
	want := "--\nZach Latta\nhttps://zachlatta.com"
	if got != want {
		t.Fatalf("signature = %q, want %q", got, want)
	}
}
