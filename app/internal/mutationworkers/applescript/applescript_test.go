package applescript

import "testing"

func TestStringEscapesQuotesAndBackslashes(t *testing.T) {
	if got := String(`he said "hi"`); got != `"he said \"hi\""` {
		t.Fatalf("got %q", got)
	}
	if got := String(`back\slash`); got != `"back\\slash"` {
		t.Fatalf("got %q", got)
	}
}

func TestStringEncodesNewlinesAsConcatenatedLinefeeds(t *testing.T) {
	if got := String("a\nb"); got != `"a" & linefeed & "b"` {
		t.Fatalf("got %q", got)
	}
	if got := String(""); got != `""` {
		t.Fatalf("got %q", got)
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		message string
		status  string
	}{
		{"Not authorized to send Apple events to Notes. (-1743)", "blocked_missing_credentials"},
		{"Notes got an error: AppleEvent timed out. (-1712)", "failed_retryable"},
		{"Notes got an error: Can’t get note id \"x\". (-1728)", "failed_terminal"},
		{"Invalid key form", "failed_terminal"},
		{"Application isn't running. (-600)", "failed_retryable"},
		{"something else entirely", "failed_terminal"},
	}
	for _, tc := range cases {
		status, text := Classify(tc.message, "Notes.app")
		if status != tc.status {
			t.Errorf("%q: got %s want %s", tc.message, status, tc.status)
		}
		if status == "blocked_missing_credentials" && (text == tc.message || !contains(text, "Notes.app") || !contains(text, "Automation")) {
			t.Errorf("blocked message should name the app and the grant: %q", text)
		}
		if status != "blocked_missing_credentials" && text != tc.message {
			t.Errorf("non-blocked message must pass through: %q", text)
		}
	}
}

func TestDedent(t *testing.T) {
	got := Dedent("\n   a\n     b\n")
	if got != "a\nb" {
		t.Fatalf("got %q", got)
	}
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
