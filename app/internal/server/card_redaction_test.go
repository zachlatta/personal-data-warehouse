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
	}
	var fixture struct{ Cases json.RawMessage }
	if json.Unmarshal(data, &fixture) != nil || json.Unmarshal(fixture.Cases, &cases) != nil {
		t.Fatal("invalid shared fixture")
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			equal := func(actual string) bool {
				if !c.JSONResult {
					return actual == c.Expected
				}
				var got, want any
				return json.Unmarshal([]byte(actual), &got) == nil && json.Unmarshal([]byte(c.Expected), &want) == nil && reflect.DeepEqual(got, want)
			}
			for _, context := range []bool{false, true} {
				if !equal(redactCardText(c.Input, context)) {
					t.Fatal("redaction contract failed")
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

func TestSecurityCodeJSONKeys(t *testing.T) {
	for _, label := range []string{"CVV2", "CVC2", "CID", "CSC", "security code", "card code"} {
		result := redactCardJSON(map[string]any{"number": "4111111111111111", label: json.Number("987")}, true).(map[string]any)
		if result[label] != "[redacted]" {
			t.Fatal("code not masked")
		}
	}
}

func TestAllCardLengthsWithAdjacentCode(t *testing.T) {
	for length := 13; length <= 19; length++ {
		prefix := "4" + strings.Repeat("1", length-2)
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
