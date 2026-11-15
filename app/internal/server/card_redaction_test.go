package server

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestSharedAdversarialCardCases(t *testing.T) {
	data, err := os.ReadFile("../../../tests/fixtures/card_redaction_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name       string
		Input      string
		Expected   string
		JSONResult bool `json:"json_result"`
		CardCount  *int `json:"card_count"`
	}
	var fixture struct{ Cases json.RawMessage }
	if json.Unmarshal(data, &fixture) != nil || json.Unmarshal(fixture.Cases, &cases) != nil {
		t.Fatal("invalid shared fixture")
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			if c.CardCount != nil && cardCount(c.Input) != *c.CardCount {
				t.Fatal("decoded card count differs")
			}
			equal := func(actual string) bool {
				if !c.JSONResult {
					return actual == c.Expected
				}
				var got, want any
				return json.Unmarshal([]byte(actual), &got) == nil && json.Unmarshal([]byte(c.Expected), &want) == nil && reflect.DeepEqual(got, want)
			}
			for _, context := range []bool{false, true} {
				// A forced row context asserts a card exists elsewhere; it is
				// intentionally inapplicable to standalone non-card fixtures.
				if context && c.CardCount != nil && *c.CardCount == 0 {
					continue
				}
				if !equal(redactCardText(c.Input, context)) {
					t.Fatal("redaction contract failed")
				}
				if c.CardCount != nil && cardCount(redactCardText(c.Input, context)) != 0 {
					t.Fatal("card survived decoded scan")
				}
			}
			line, _ := json.Marshal(map[string]string{"text": c.Input})
			cleaned, err := redactConversationBatch(conversationTestBatch(string(line)), 100000)
			if err != nil {
				t.Fatal(err)
			}
			r, _ := gzip.NewReader(bytes.NewReader(cleaned))
			raw, _ := io.ReadAll(r)
			r.Close()
			var result map[string]string
			if json.Unmarshal(raw, &result) != nil || !equal(result["text"]) {
				t.Fatal("batch contract failed")
			}
		})
	}
}

func TestSharedIssuerAndIdentifierCases(t *testing.T) {
	data, err := os.ReadFile("../../../tests/fixtures/card_issuer_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name, Prefix, Before, After string
		Length                      int
		Mask                        bool
	}
	if json.Unmarshal(data, &cases) != nil {
		t.Fatal("invalid issuer fixtures")
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			stem := (c.Prefix + strings.Repeat("1", c.Length))[:c.Length-1]
			pan := ""
			for i := 0; i < 10; i++ {
				v := stem + string(rune('0'+i))
				if validCard(v) {
					pan = v
					break
				}
			}
			value := c.Before + pan + c.After
			want := value
			if c.Mask {
				want = c.Before + "[card ****" + pan[len(pan)-4:] + "]" + c.After
			}
			if redactCardText(value, false) != want || (cardCount(value) > 0) != c.Mask {
				t.Fatal("issuer or token boundary differs")
			}
			body, _ := json.Marshal(map[string]string{"text": value})
			cleaned, err := redactConversationBatch(conversationTestBatch(string(body)), 100000)
			if err != nil {
				t.Fatal(err)
			}
			r, _ := gzip.NewReader(bytes.NewReader(cleaned))
			raw, _ := io.ReadAll(r)
			r.Close()
			var got map[string]string
			if json.Unmarshal(raw, &got) != nil || got["text"] != want {
				t.Fatal("batch issuer boundary differs")
			}
		})
	}
}

func TestSecurityCodeJSONKeys(t *testing.T) {
	for _, label := range []string{"CVV2", "CVC2", "CID", "CSC", "security code", "card code"} {
		raw, _ := json.Marshal(map[string]any{"number": "4111111111111111", label: json.Number("987")})
		var result map[string]any
		if json.Unmarshal([]byte(redactCardText(string(raw), true)), &result) != nil {
			t.Fatal("invalid redacted JSON")
		}
		if result[label] != "[redacted]" {
			t.Fatal("code not masked")
		}
	}
}

func TestJSONFidelityThroughConversationBatch(t *testing.T) {
	data, err := os.ReadFile("../../../tests/fixtures/card_redaction_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct{ Name, Input, Expected string }
	}
	if json.Unmarshal(data, &fixture) != nil {
		t.Fatal("invalid shared fixture")
	}
	for _, c := range fixture.Cases {
		if !strings.HasPrefix(c.Name, "json_preserve") && !strings.HasPrefix(c.Name, "encoded_nested_") {
			continue
		}
		t.Run(c.Name, func(t *testing.T) {
			body := conversationTestBatch(c.Input)
			cleaned, err := redactConversationBatch(body, 100000)
			if err != nil {
				t.Fatal(err)
			}
			r, err := gzip.NewReader(bytes.NewReader(cleaned))
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(r)
			_ = r.Close()
			if err != nil || string(raw) != c.Expected {
				t.Fatal("batch changed unmasked JSON bytes")
			}
			if c.Input == c.Expected && !bytes.Equal(body, cleaned) {
				t.Fatal("unchanged batch was recompressed")
			}
		})
	}
}

func TestJSONScalarConversationBatchRemainsValid(t *testing.T) {
	body, err := redactConversationBatch(conversationTestBatch("4111111111111111\n"), 10000)
	if err != nil {
		t.Fatal(err)
	}
	r, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || string(raw) != "\"[card ****1111]\"\n" {
		t.Fatal("scalar mask must remain valid JSON")
	}
}

func TestAllCardLengthsWithAdjacentCode(t *testing.T) {
	for length := 13; length <= 19; length++ {
		issuer := "6"
		if length == 13 || length == 16 || length == 19 {
			issuer = "4"
		}
		prefix := issuer + strings.Repeat("1", length-2)
		number := ""
		for n := 0; n < 10; n++ {
			candidate := prefix + string(rune('0'+n))
			if validCard(candidate) {
				number = candidate
				break
			}
		}
		var groups []string
		for i := 0; i < len(number); i += 4 {
			end := i + 4
			if end > len(number) {
				end = len(number)
			}
			groups = append(groups, number[i:end])
		}
		result := redactCardText(strings.Join(groups, " ")+" 987", true)
		if !strings.HasPrefix(result, "[card ****") || !strings.HasSuffix(result, "] [redacted]") || strings.Contains(result, "987") {
			t.Fatal("adjacent code or card suffix survived")
		}
	}
}

func conversationTestBatch(s string) []byte {
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	_, _ = w.Write([]byte(s))
	_ = w.Close()
	return out.Bytes()
}

func TestConversationCardBoundary(t *testing.T) {
	for _, number := range []string{"4111111111111111", "4111-1111-1111-1111", "4111 1111 1111 1111", `\u0034` + strings.Repeat("1", 15)} {
		body := conversationTestBatch(`{"line":{"text":"` + number + `", "cvv":987,"id":"4111111111111112"}}`)
		cleaned, err := redactConversationBatch(body, 100000)
		if err != nil {
			t.Fatal("redaction failed")
		}
		r, _ := gzip.NewReader(bytes.NewReader(cleaned))
		raw, _ := io.ReadAll(r)
		_ = r.Close()
		if !strings.Contains(string(raw), "[card ****1111]") || strings.Contains(string(raw), "987") || !strings.Contains(string(raw), "4111111111111112") {
			t.Fatal("masking contract failed")
		}
	}
}

func TestConversationCardBoundaryRejectsInvalidAndOversize(t *testing.T) {
	if _, err := redactConversationBatch([]byte("bad"), 100); err == nil {
		t.Fatal("accepted invalid gzip")
	}
	if _, err := redactConversationBatch(conversationTestBatch(strings.Repeat("a", 100)), 10); err == nil {
		t.Fatal("accepted oversize batch")
	}
}

func TestSignedConversationBatchIsScrubbedBeforeStorage(t *testing.T) {
	svc, stores := ingestTestService()
	body := conversationTestBatch(`{"line":{"text":"4111-1111-1111-1111 CVV: 987"}}`)
	target := signedIngestTarget("/ingest/agent-sessions/batch", body, url.Values{"exported_at": {"2026-06-19T12:34:56+00:00"}})
	rec := postIngest(t, svc, target, body)
	if rec.Code != http.StatusOK {
		t.Fatal("signed upload rejected")
	}
	put := stores["agent_sessions"].lastFile
	if put == nil || put.ContentSHA256 == sha256Hex(body) || put.ContentSHA256 != sha256Hex(put.Content) {
		t.Fatal("stored hash must describe sanitized bytes")
	}
	r, _ := gzip.NewReader(bytes.NewReader(put.Content))
	raw, _ := io.ReadAll(r)
	_ = r.Close()
	if strings.Contains(string(raw), "987") || !strings.Contains(string(raw), "[card ****1111]") {
		t.Fatal("stored bytes not sanitized")
	}
}

func TestNestedEscapedConversationJSON(t *testing.T) {
	inner := strings.ReplaceAll(`{"number":"4111111111111111","cvv":"987"}`, "1", `\u0031`)
	line, _ := json.Marshal(map[string]any{"result": inner})
	cleaned, err := redactConversationBatch(conversationTestBatch(string(line)), 100000)
	if err != nil {
		t.Fatal("redaction failed")
	}
	r, _ := gzip.NewReader(bytes.NewReader(cleaned))
	raw, _ := io.ReadAll(r)
	_ = r.Close()
	var outer map[string]string
	if json.Unmarshal(raw, &outer) != nil {
		t.Fatal("invalid outer JSON")
	}
	var result map[string]string
	if json.Unmarshal([]byte(outer["result"]), &result) != nil || result["number"] != "[card ****1111]" || result["cvv"] != "[redacted]" {
		t.Fatal("nested JSON not sanitized")
	}
}
