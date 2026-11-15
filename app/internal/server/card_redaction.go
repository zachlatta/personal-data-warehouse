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
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const cardCodeLabel = `(?:cvv2?|cvc2?|cid|csc|cvd|cvn|(?:security|card)[ _-]*code)`

var codeValue = regexp.MustCompile(`(?i)(\b` + cardCodeLabel + `\b(?:\s+is)?[\s\\"':=,-]{0,24})([0-9]{3,4})(?:\b)`)
var codeKey = regexp.MustCompile(`(?i)^` + cardCodeLabel + `$`)
var cardEscape = regexp.MustCompile(`\\+u(003[0-9]|0020|002[dD])`)
var bareCardCode = regexp.MustCompile(`^[ -]+([0-9]{3,4})`)
var cardTokenPatterns = makeCardTokenPatterns()
var cardExpirySuffix = regexp.MustCompile(`^/[0-9]{2}/(?:[0-9]{4}|[0-9]{2})(?:[^0-9/]|$)`)

func standaloneCardTokenEnd(s string, end int) bool {
	if end == len(s) {
		return true
	}
	next, _ := utf8.DecodeRuneInString(s[end:])
	return !unicode.IsLetter(next) && !unicode.IsDigit(next) && !strings.ContainsRune("_-/:", next) && !(next == '.' && end+1 < len(s) && s[end+1] >= '0' && s[end+1] <= '9')
}

func panTokenEnd(s string, end int, contiguous bool) bool {
	if end == len(s) {
		return true
	}
	next, _ := utf8.DecodeRuneInString(s[end:])
	if unicode.IsNumber(next) {
		return false
	}
	if unicode.IsLetter(next) || next == '_' {
		return contiguous
	}
	if strings.ContainsRune("-/:.", next) && asciiDigit(s, end+1) {
		return next == '/' && cardExpirySuffix.MatchString(s[end:])
	}
	return true
}

func makeCardTokenPatterns() []*regexp.Regexp {
	patterns := []*regexp.Regexp{regexp.MustCompile(`[0-9]{13,19}`)}
	groups := [][]string{
		{`[0-9]{4}`, `[0-9]{4}`, `[0-9]{4}`, `[0-9]{1,7}`},
		{`[0-9]{4}`, `[0-9]{4}`, `[0-9]{4}`, `[0-9]{4}`, `[0-9]{1,3}`},
		{`[0-9]{4}`, `[0-9]{6}`, `[0-9]{5}`},
	}
	for _, sep := range []string{" ", "-"} {
		for _, group := range groups {
			patterns = append(patterns, regexp.MustCompile(strings.Join(group, sep)))
		}
	}
	return patterns
}

func asciiDigit(s string, i int) bool { return i >= 0 && i < len(s) && s[i] >= '0' && s[i] <= '9' }

// A candidate is ONE recognized token, never a window across arbitrary numeric
// prose. Separators are single and consistent. Phone/date/UUID groupings are
// outside this grammar, and numeric continuations cannot end a candidate.
// Only then apply length and Luhn.
func cardTokens(s string) [][2]int {
	candidates := map[[2]int]bool{}
	for index, pattern := range cardTokenPatterns {
		for _, m := range pattern.FindAllStringIndex(s, -1) {
			start, end := m[0], m[1]
			if asciiDigit(s, start-1) || (start > 0 && s[start-1] == '-' && asciiDigit(s, start-2)) {
				continue
			}
			if !panTokenEnd(s, end, index == 0) {
				continue
			}
			if validCard(s[start:end]) {
				candidates[[2]int{start, end}] = true
			}
		}
	}
	ordered := [][2]int{}
	for c := range candidates {
		ordered = append(ordered, c)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i][0] != ordered[j][0] {
			return ordered[i][0] < ordered[j][0]
		}
		return ordered[i][1] < ordered[j][1]
	})
	result := [][2]int{}
	cursor := 0
	for i := 0; i < len(ordered); {
		start := ordered[i][0]
		j := i
		for j < len(ordered) && ordered[j][0] == start {
			j++
		}
		if start >= cursor {
			end := ordered[j-1][1]
			// An isolated appended 3–4 digit code is suppressed separately, so
			// its digits can never become the PAN mask's retained suffix.
			for _, c := range ordered[i:j] {
				short := c[1]
				code := bareCardCode.FindStringSubmatchIndex(s[short:])
				if short < end && code != nil && short+code[3] == end && standaloneCardTokenEnd(s, end) {
					end = short
					break
				}
			}
			result = append(result, [2]int{start, end})
			cursor = end
		}
		i = j
	}
	return result
}

func maskCardTokens(s string) string {
	var out strings.Builder
	cursor := 0
	for _, span := range cardTokens(s) {
		start, end := span[0], span[1]
		if start < cursor {
			continue
		}
		digits := strings.NewReplacer(" ", "", "-", "").Replace(s[start:end])
		out.WriteString(s[cursor:start])
		out.WriteString("[card ****" + digits[len(digits)-4:] + "]")
		for {
			code := bareCardCode.FindStringSubmatchIndex(s[end:])
			if code == nil || !standaloneCardTokenEnd(s, end+code[3]) {
				break
			}
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
	if !context {
		context = len(cardTokens(decodeCardEscapes(s))) > 0
	}
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
	s = maskCardTokens(decodeCardEscapes(s))
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
		context := len(cardTokens(decodeCardEscapes(string(decoded)))) > 0
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
