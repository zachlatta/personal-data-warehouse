package server

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"
)

// Mirrors tests/credential_redaction_properties.py: the same fixture, LCG,
// wrappers and order build the same corpus, pinned by corpus_sha256.
type credentialFixture struct {
	Secrets map[string][]string
	Cases   []struct {
		Name     string
		Input    string
		Expected *string
	}
	Property struct {
		Seed         uint32
		Prefixes     []string
		Suffixes     []string
		Wrappers     []string
		Iterations   int
		CorpusSHA256 string `json:"corpus_sha256"`
	}
}

var credentialTemplate = regexp.MustCompile(`<<(?:mask:([a-z_]+):)?([a-z0-9]+)>>`)
var testCredentialMarker = regexp.MustCompile(`\[REDACTED:[a-z_]+:[0-9a-f]{6}\]`)

func loadCredentialFixture(t *testing.T) credentialFixture {
	t.Helper()
	data, err := os.ReadFile("../../../tests/fixtures/credential_redaction_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture credentialFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (f credentialFixture) expand(template string) string {
	return credentialTemplate.ReplaceAllStringFunc(template, func(m string) string {
		parts := credentialTemplate.FindStringSubmatch(m)
		value := strings.Join(f.Secrets[parts[2]], "")
		if parts[1] == "" {
			return value
		}
		sum := sha256.Sum256([]byte(value))
		return "[REDACTED:" + parts[1] + ":" + hex.EncodeToString(sum[:])[:6] + "]"
	})
}

func testJSONQuoteRune(c rune) string {
	switch {
	case c == '\\':
		return `\\`
	case c == '"':
		return `\"`
	case c == '\n':
		return `\n`
	case c == '\t':
		return `\t`
	case c == '\r':
		return `\r`
	case c < 0x20:
		return fmt.Sprintf(`\u%04x`, c)
	}
	return string(c)
}

func testJSONQuote(s string) string {
	var out strings.Builder
	out.WriteByte('"')
	for _, c := range s {
		out.WriteString(testJSONQuoteRune(c))
	}
	out.WriteByte('"')
	return out.String()
}

func testEscapeLetters(s string, keepMarkers bool) string {
	var out strings.Builder
	spans := [][]int{}
	if keepMarkers {
		spans = testCredentialMarker.FindAllStringIndex(s, -1)
	}
	spans = append(spans, []int{len(s), len(s)})
	cursor := 0
	for _, span := range spans {
		for _, c := range s[cursor:span[0]] {
			if c < 0x80 && (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
				fmt.Fprintf(&out, `\u%04x`, c)
			} else {
				out.WriteString(testJSONQuoteRune(c))
			}
		}
		out.WriteString(s[span[0]:span[1]])
		cursor = span[1]
	}
	return out.String()
}

func testCredentialWrap(kind, text string, expected bool) string {
	switch kind {
	case "plain":
		return text
	case "json_text":
		return `{"type":"tool_result","text":` + testJSONQuote(text) + `}`
	case "nested_json":
		return `{"message":` + testJSONQuote(testCredentialWrap("json_text", text, expected)) + `}`
	case "escaped_json":
		return `{"text":"` + testEscapeLetters(text, expected) + `"}`
	}
	panic(kind)
}

type credentialCorpusItem struct{ name, input, expected string }

func (f credentialFixture) corpus() []credentialCorpusItem {
	config := f.Property
	state := config.Seed
	rand := func(n int) int { state = 1664525*state + 1013904223; return int(state % uint32(n)) }
	items := []credentialCorpusItem{}
	for _, c := range f.Cases {
		raw := f.expand(c.Input)
		want := raw
		if c.Expected != nil {
			want = f.expand(*c.Expected)
		}
		for _, kind := range config.Wrappers {
			if kind == "escaped_json" && strings.Contains(raw, "[REDACTED") {
				continue
			}
			for i := 0; i < config.Iterations; i++ {
				prefix := config.Prefixes[rand(len(config.Prefixes))]
				suffix := config.Suffixes[rand(len(config.Suffixes))]
				items = append(items, credentialCorpusItem{c.Name,
					testCredentialWrap(kind, prefix+raw+suffix, false),
					testCredentialWrap(kind, prefix+want+suffix, true)})
			}
		}
	}
	return items
}

func TestCredentialCorpusMatchesThePythonCorpus(t *testing.T) {
	fixture := loadCredentialFixture(t)
	digest := sha256.New()
	for _, item := range fixture.corpus() {
		digest.Write([]byte(item.input))
		digest.Write([]byte{0})
	}
	if got := hex.EncodeToString(digest.Sum(nil)); got != fixture.Property.CorpusSHA256 {
		t.Fatalf("corpus digest %s differs from the shared fixture", got)
	}
}

func TestEachCredentialShapeMasksOnlyTheSecret(t *testing.T) {
	fixture := loadCredentialFixture(t)
	for _, c := range fixture.Cases {
		value := fixture.expand(c.Input)
		want := value
		if c.Expected != nil {
			want = fixture.expand(*c.Expected)
		}
		if got := redactCredentials(value); got != want {
			t.Errorf("%s: got %q", c.Name, got)
		}
		added := len(testCredentialMarker.FindAllString(want, -1)) - len(testCredentialMarker.FindAllString(value, -1))
		if n := credentialCount(value); n != added {
			t.Errorf("%s: counted %d, masked %d", c.Name, n, added)
		}
		if credentialCount(want) != 0 || redactCredentials(want) != want {
			t.Errorf("%s: masking is not idempotent", c.Name)
		}
	}
}

func TestGeneratedCredentialWrappersMaskIdentically(t *testing.T) {
	for _, item := range loadCredentialFixture(t).corpus() {
		if got := redactCredentials(item.input); got != item.expected {
			t.Fatalf("%s: got %q want %q", item.name, got, item.expected)
		}
		if redactCredentials(item.expected) != item.expected {
			t.Fatalf("%s: not idempotent", item.name)
		}
	}
}

func TestCardAndCredentialBoundariesCompose(t *testing.T) {
	fixture := loadCredentialFixture(t)
	slack := fixture.expand("<<slack>>")
	value := `{"text":"card: 4111111111111111 CVV: 987 ` + slack + `"}`
	cleaned := redactCardText(value, false)
	if !strings.Contains(cleaned, "[card ****1111]") || !strings.Contains(cleaned, "[redacted]") ||
		strings.Contains(cleaned, slack) || !strings.Contains(cleaned, fixture.expand("<<mask:slack_token:slack>>")) {
		t.Fatal(cleaned)
	}
	if redactCardText("password=Pw4111111111111111x", true) == "password=Pw4111111111111111x" ||
		strings.Contains(redactCardText("password=Pw4111111111111111x", true), "[card") {
		t.Fatal("a credential must own its digits")
	}
}

// The upload boundary: every fixture shape inside a JSONL batch is stored
// with markers only.
func TestConversationBatchMasksEveryCredentialShape(t *testing.T) {
	fixture := loadCredentialFixture(t)
	lines := []string{}
	wants := []string{}
	for _, c := range fixture.Cases {
		if c.Expected == nil {
			continue
		}
		lines = append(lines, `{"type":"user","message":{"content":`+testJSONQuote(fixture.expand(c.Input))+`}}`)
		wants = append(wants, `{"type":"user","message":{"content":`+testJSONQuote(fixture.expand(*c.Expected))+`}}`)
	}
	batch, err := redactConversationBatch(conversationTestBatch(strings.Join(lines, "\n")), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	r, err := gzip.NewReader(bytes.NewReader(batch))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != strings.Join(wants, "\n") {
		t.Fatalf("batch was not masked to the expected markers")
	}
	for name := range fixture.Secrets {
		secret := strings.Join(fixture.Secrets[name], "")
		if name != "awsid" && name != "pemhead" && name != "pemfoot" && bytes.Contains(raw, []byte(secret)) {
			t.Fatalf("secret %s survived the batch", name)
		}
	}
}
