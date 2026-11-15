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
	"unicode"
	"unicode/utf8"
)

var cardRun = regexp.MustCompile(`[0-9](?:[ -]*[0-9])*`)

const cardCodeLabel = `(?:cvv2?|cvc2?|cid|csc|cvd|cvn|(?:security|card)[ _-]*code)`

var codeValue = regexp.MustCompile(`(?i)(\b` + cardCodeLabel + `\b(?:\s+is)?[\s\\"':=,-]{0,24})([0-9]{3,4})(?:\b)`)
var codeKey = regexp.MustCompile(`(?i)^` + cardCodeLabel + `$`)
var cardEscape = regexp.MustCompile(`\\+u(003[0-9]|0020|002[dD])`)
var cardDigits = regexp.MustCompile(`[0-9]+`)
var bareCardCode = regexp.MustCompile(`^[ -]+([0-9]{3,4})`)

func standaloneCardTokenEnd(s string, end int) bool {
	if end == len(s) {
		return true
	}
	next, _ := utf8.DecodeRuneInString(s[end:])
	return !unicode.IsLetter(next) && !unicode.IsDigit(next) && !strings.ContainsRune("_-/:", next) && !(next == '.' && end+1 < len(s) && s[end+1] >= '0' && s[end+1] <= '9')
}

// Only token-aligned windows are candidates. A long contiguous identifier is
// never split. Prefer contiguous PANs and standard groupings over a window that
// accidentally incorporates an expiry or security code.
func cardWindows(s string) [][3]int {
	var spans [][3]int
	for _, run := range cardRun.FindAllStringIndex(s, -1) {
		text := s[run[0]:run[1]]
		tokens := cardDigits.FindAllStringIndex(text, -1)
		for i := 0; i < len(tokens); {
			best, rank, bestLength, furthest := -1, 3, 20, -1
			digits := ""
			for j := i; j < len(tokens); j++ {
				digits += text[tokens[j][0]:tokens[j][1]]
				if len(digits) > 19 {
					break
				}
				if !validCard(digits) {
					continue
				}
				if j > i && !standaloneCardTokenEnd(s, run[0]+tokens[j][1]) {
					continue
				}
				furthest = j
				score := 2
				if j == i {
					score = 0
				} else if j-i == 3 && tokens[i][1]-tokens[i][0] == 4 && tokens[i+1][1]-tokens[i+1][0] == 4 && tokens[i+2][1]-tokens[i+2][0] == 4 && tokens[j][1]-tokens[j][0] == 4 {
					score = 1
				} else if j-i == 2 && tokens[i][1]-tokens[i][0] == 4 && tokens[i+1][1]-tokens[i+1][0] == 6 && tokens[j][1]-tokens[j][0] == 5 {
					score = 1
				}
				if score < rank || (score == rank && len(digits) < bestLength) {
					best = j
					rank = score
					bestLength = len(digits)
				}
			}
			if best < 0 {
				i++
				continue
			}
			// Cover every ambiguous valid window, retaining digits only from
			// the preferred PAN rather than a suffix that might be its code.
			spans = append(spans, [3]int{run[0] + tokens[i][0], run[0] + tokens[furthest][1], run[0] + tokens[best][1]})
			i = furthest + 1
		}
	}
	return spans
}

func maskCardWindows(s string) string {
	var out strings.Builder
	cursor := 0
	for _, span := range cardWindows(s) {
		start, end := span[0], span[1]
		if start < cursor {
			continue
		}
		d := strings.NewReplacer(" ", "", "-", "").Replace(s[start:span[2]])
		out.WriteString(s[cursor:start])
		out.WriteString("[card ****" + d[len(d)-4:] + "]")
		if end > span[2] {
			out.WriteString(" [redacted]")
		}
		if code := bareCardCode.FindStringSubmatchIndex(s[end:]); code != nil && standaloneCardTokenEnd(s, end+code[3]) {
			out.WriteString(s[end : end+code[2]])
			out.WriteString("[redacted]")
			end += code[3]
		}
		cursor = end
	}
	out.WriteString(s[cursor:])
	return out.String()
}

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
	first := dec.Decode(&parsed)
	var trailing any
	if first == nil && dec.Decode(&trailing) == io.EOF && (strings.HasPrefix(strings.TrimSpace(s), "{") || strings.HasPrefix(strings.TrimSpace(s), "[")) {
		cleaned := redactCardJSON(parsed, context)
		b, err := json.Marshal(cleaned)
		if err == nil {
			return string(b)
		}
	}
	s = maskCardWindows(decodeCardEscapes(s))
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
		var extra any
		if dec.Decode(&extra) != io.EOF {
			return nil, fmt.Errorf("invalid conversation JSON suffix")
		}
		// Marshal decoded JSON so unicode-escaped digits participate in detection.
		decoded, _ := json.Marshal(v)
		context := len(cardWindows(decodeCardEscapes(string(decoded)))) > 0
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
