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
var codeScalar = regexp.MustCompile(`^[0-9]{3,4}(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)
var cardEscape = regexp.MustCompile(`\\+u(003[0-9]|0020|002[dD])`)
var cardDigits = regexp.MustCompile(`[0-9]+`)
var cardSeparator = regexp.MustCompile(`^[ -]{1,3}$`)
var adjacentCardCode = regexp.MustCompile(`^(?:[ -]{1,3}|[:=] {0,3})([0-9]{3,4})`)
var cardExpirySuffix = regexp.MustCompile(`^/[0-9]{2}/(?:[0-9]{4}|[0-9]{2})(?:[^0-9/]|$)`)
var protectedUUID = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

type protectedPattern struct {
	kind    string
	pattern *regexp.Regexp
}

var protectedPatterns = []protectedPattern{
	{"phone", regexp.MustCompile(`^(?:\+[0-9]{1,3}[ -]{1,3})?[0-9]{3}[ -]{1,3}[0-9]{3}[ -]{1,3}[0-9]{4}`)},
	{"phone", regexp.MustCompile(`^\([0-9]{3}\) {0,3}[0-9]{3}[ -]{1,3}[0-9]{4}`)},
	{"date", regexp.MustCompile(`^[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])`)},
	{"date", regexp.MustCompile(`^[0-9]{4}/(?:0[1-9]|1[0-2])/(?:0[1-9]|[12][0-9]|3[01])`)},
	{"date", regexp.MustCompile(`^[0-9]{1,2}/[0-9]{2,4}`)},
	{"time", regexp.MustCompile(`^[0-9]{1,3}:[0-9]{2}(?::[0-9]{2})?(?:\.[0-9]+)?`)},
	{"uuid", regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)},
	{"id", regexp.MustCompile(`^[0-9]{1,4}\.[0-9]+`)},
	{"id", regexp.MustCompile(`^[0-9]{1,4}/[0-9]{2,4}`)},
	{"id", regexp.MustCompile(`^[0-9]{1,4}[a-zA-Z_][a-zA-Z_0-9]*`)},
}

func standaloneCardTokenEnd(s string, end int) bool {
	if end == len(s) {
		return true
	}
	next, _ := utf8.DecodeRuneInString(s[end:])
	return !unicode.IsLetter(next) && !unicode.IsDigit(next) && !strings.ContainsRune("_-/:", next) && !(next == '.' && end+1 < len(s) && s[end+1] >= '0' && s[end+1] <= '9')
}

func asciiDigit(s string, i int) bool { return i >= 0 && i < len(s) && s[i] >= '0' && s[i] <= '9' }
func digitCount(s string) int {
	n := 0
	for i := range s {
		if asciiDigit(s, i) {
			n++
		}
	}
	return n
}
func cardLayout(s string) bool {
	atoms := cardDigits.FindAllStringIndex(s, -1)
	g := []int{}
	for _, a := range atoms {
		g = append(g, a[1]-a[0])
	}
	return (len(g) == 4 && g[0] == 4 && g[1] == 4 && g[2] == 4 && g[3] >= 1 && g[3] <= 7) || (len(g) == 5 && g[0] == 4 && g[1] == 4 && g[2] == 4 && g[3] == 4 && g[4] >= 1 && g[4] <= 3) || (len(g) == 3 && g[0] == 4 && g[1] == 6 && g[2] == 5)
}

// Every digit-run start is evaluated, including overlaps. Candidate formatting
// never excludes a reading: only length, Luhn and recognized non-card tokens do.
func cardTokens(s string) [][2]int {
	atoms := cardDigits.FindAllStringIndex(s, -1)
	readings := [][2]int{}
	for i, a := range atoms {
		digits := ""
		for j := i; j < len(atoms); j++ {
			if j > i && !cardSeparator.MatchString(s[atoms[j-1][1]:atoms[j][0]]) {
				break
			}
			digits += s[atoms[j][0]:atoms[j][1]]
			if len(digits) > 19 {
				break
			}
			if validCard(digits) {
				readings = append(readings, [2]int{a[0], atoms[j][1]})
			}
		}
	}
	if len(readings) == 0 {
		return nil
	}
	tokenStarts := []int{}
	for _, a := range atoms {
		tokenStarts = append(tokenStarts, a[0])
		if a[0] > 0 && (s[a[0]-1] == '+' || s[a[0]-1] == '(') {
			tokenStarts = append(tokenStarts, a[0]-1)
		}
	}
	type token struct {
		kind                 string
		start, end, firstEnd int
	}
	protected := []token{}
	for _, p := range protectedPatterns {
		positions := tokenStarts
		if p.kind == "uuid" {
			positions = nil
			for _, m := range protectedUUID.FindAllStringIndex(s, -1) {
				positions = append(positions, m[0])
			}
		}
		for _, start := range positions {
			if asciiDigit(s, start-1) {
				continue
			}
			m := p.pattern.FindStringIndex(s[start:])
			if m == nil {
				continue
			}
			end := start + m[1]
			if asciiDigit(s, end) {
				continue
			}
			first := cardDigits.FindStringIndex(s[start:end])
			protected = append(protected, token{p.kind, start, end, start + first[1]})
		}
	}
	sort.Slice(readings, func(i, j int) bool {
		if readings[i][0] != readings[j][0] {
			return readings[i][0] < readings[j][0]
		}
		return readings[i][1] < readings[j][1]
	})
	valid := [][2]int{}
	for _, r := range readings {
		start, end := r[0], r[1]
		blocked := false
		for _, p := range protected {
			if start >= p.end || end <= p.start {
				continue
			}
			numericField := cardLayout(s[start:end]) && (p.kind == "date" || p.kind == "time" || p.kind == "id") && start < p.start && end == p.firstEnd
			earlier := false
			external := false
			for _, other := range readings {
				if other[0] < p.start && start < other[1] && other[1] <= p.start {
					earlier = true
					if other[0] < start {
						external = true
					}
				}
			}
			if numericField && (!earlier || cardExpirySuffix.MatchString(s[end:]) && !external) {
				continue
			}
			blocked = true
			break
		}
		if !blocked {
			valid = append(valid, r)
		}
	}
	codes := [][2]int{}
	for _, r := range valid {
		start, end := r[0], r[1]
		code := adjacentCardCode.FindStringSubmatchIndex(s[end:])
		if code == nil || !standaloneCardTokenEnd(s, end+code[3]) {
			continue
		}
		alternative := false
		for _, other := range valid {
			if other[0] > start && other[1] == end+code[3] && digitCount(s[other[0]:other[1]]) >= digitCount(s[start:end]) {
				alternative = true
				break
			}
		}
		if !alternative {
			codes = append(codes, [2]int{end + code[2], end + code[3]})
		}
	}
	result := [][2]int{}
	for _, r := range valid {
		blocked := false
		for _, code := range codes {
			if r[0] < code[1] && r[1] > code[0] {
				blocked = true
				break
			}
		}
		if blocked {
			continue
		}
		if len(result) > 0 && r[0] < result[len(result)-1][1] {
			if r[1] > result[len(result)-1][1] {
				result[len(result)-1][1] = r[1]
			}
		} else {
			result = append(result, r)
		}
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
			code := adjacentCardCode.FindStringSubmatchIndex(s[end:])
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
				s := strings.TrimSpace(fmt.Sprint(item))
				numericCode := false
				if number, ok := item.(json.Number); ok {
					if n, err := strconv.ParseFloat(string(number), 64); err == nil && n >= 0 && n <= 9999 && n == float64(int64(n)) {
						numericCode = true
					}
				}
				if codeScalar.MatchString(s) || numericCode {
					cleaned[key] = "[redacted]"
					continue
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
		if cleaned := redactCardText(string(x), context); cleaned != string(x) {
			return cleaned
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
