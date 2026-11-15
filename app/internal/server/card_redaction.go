package server

// Conversation batches are scrubbed after signature verification and before
// object storage. Python applies the same boundary again at database insertion.
import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

var cardRun = regexp.MustCompile(`[0-9](?:[ -]*[0-9])*`)
var codeValue = regexp.MustCompile(`(?i)((?:cvv|cvc|security[ _-]*code)\b(?:\s+is)?[\s\\"':=,-]{0,24})([0-9]{3,4})(?:\b)`)
var codeKey = regexp.MustCompile(`(?i)^(?:cvv|cvc|security[ _-]*code)$`)
var cardEscape = regexp.MustCompile(`\\+u(003[0-9]|0020|002[dD])`)

func decodeCardEscapes(s string) string {
	return cardEscape.ReplaceAllStringFunc(s, func(v string) string { n, _ := strconv.ParseInt(v[len(v)-4:], 16, 32); return string(rune(n)) })
}

func validCard(s string) bool {
	s = strings.NewReplacer(" ", "", "-", "").Replace(s)
	if len(s) < 13 || len(s) > 19 {
		return false
	}
	sum := 0
	for i := len(s) - 1; i >= 0; i-- {
		n := int(s[i] - '0')
		if n < 0 || n > 9 {
			return false
		}
		if (len(s)-1-i)%2 == 1 {
			n *= 2
		}
		if n > 9 {
			n -= 9
		}
		sum += n
	}
	return sum%10 == 0
}

func redactCardText(s string, context bool) string {
	var parsed any
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	if dec.Decode(&parsed) == nil && (strings.HasPrefix(strings.TrimSpace(s), "{") || strings.HasPrefix(strings.TrimSpace(s), "[")) {
		cleaned := redactCardJSON(parsed, context)
		b, err := json.Marshal(cleaned)
		if err == nil {
			return string(b)
		}
	}
	s = cardRun.ReplaceAllStringFunc(decodeCardEscapes(s), func(v string) string {
		if validCard(v) {
			d := strings.NewReplacer(" ", "", "-", "").Replace(v)
			return "[card ****" + d[len(d)-4:] + "]"
		}
		return v
	})
	if context {
		s = codeValue.ReplaceAllString(s, "${1}[redacted]")
	}
	return s
}

func redactCardJSON(v any, context bool) any {
	switch x := v.(type) {
	case map[string]any:
		cleaned := make(map[string]any, len(x))
		for k, item := range x {
			key := redactCardText(k, context)
			if codeKey.MatchString(k) && context {
				s := fmt.Sprint(item)
				if len(s) == 3 || len(s) == 4 {
					if _, err := strconv.Atoi(s); err == nil {
						cleaned[key] = "[redacted]"
						continue
					}
				}
			}
			cleaned[key] = redactCardJSON(item, context)
		}
		return cleaned
	case []any:
		for i, item := range x {
			x[i] = redactCardJSON(item, context)
		}
		return x
	case string:
		return redactCardText(x, context)
	case json.Number:
		if validCard(string(x)) {
			return redactCardText(string(x), context)
		}
	}
	return v
}

func redactConversationBatch(body []byte, maxBytes int64) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("invalid conversation gzip")
	}
	defer reader.Close()
	raw, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil || int64(len(raw)) > maxBytes {
		return nil, fmt.Errorf("conversation batch too large")
	}
	lines := bytes.Split(raw, []byte("\n"))
	changed := false
	for i, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var v any
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		if dec.Decode(&v) != nil {
			return nil, fmt.Errorf("invalid conversation JSON")
		}
		// Marshal decoded JSON so unicode-escaped digits participate in detection.
		decoded, _ := json.Marshal(v)
		context := false
		for _, candidate := range cardRun.FindAllString(decodeCardEscapes(string(decoded)), -1) {
			if validCard(candidate) {
				context = true
				break
			}
		}
		if !context {
			continue
		}
		cleaned, err := json.Marshal(redactCardJSON(v, true))
		if err != nil {
			return nil, fmt.Errorf("cannot scrub conversation JSON")
		}
		lines[i] = cleaned
		changed = true
	}
	if !changed {
		return body, nil
	}
	var out bytes.Buffer
	writer := gzip.NewWriter(&out)
	if _, err = writer.Write(bytes.Join(lines, []byte("\n"))); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
