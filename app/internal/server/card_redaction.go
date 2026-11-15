package server

// Conversation batches are scrubbed after signature verification and before
// object storage. Python applies the same boundary again at database insertion.
import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/zachlatta/personal-data-warehouse/app/internal/cardpolicy"
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

func cardBrand(s string) bool {
	for _, issuer := range cardpolicy.Rules.Issuers {
		length := false
		for _, n := range issuer.Lengths {
			if n == len(s) {
				length = true
				break
			}
		}
		if !length {
			continue
		}
		for _, r := range issuer.Ranges {
			if len(s) >= len(r[0]) && s[:len(r[0])] >= r[0] && s[:len(r[0])] <= r[1] {
				return true
			}
		}
	}
	return false
}

func cardWordByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
}
func cardTokenBoundary(s string, start, end int) bool {
	left, right := start, end
	for left > 0 && cardWordByte(s[left-1]) {
		left--
	}
	for right < len(s) && cardWordByte(s[right]) {
		right++
	}
	allowed := func(word string) bool {
		for _, label := range cardpolicy.Rules.AttachedLabels {
			if strings.EqualFold(word, label) {
				return true
			}
		}
		return false
	}
	if !(left == start || allowed(s[left:start])) || !(right == end || allowed(s[end:right])) {
		return false
	}
	left, right = start, end
	for left > 0 && (cardWordByte(s[left-1]) || s[left-1] == '+' || s[left-1] == '/') {
		left--
	}
	for right < len(s) && (cardWordByte(s[right]) || s[right] == '+' || s[right] == '/') {
		right++
	}
	for _, side := range []string{s[left:start], s[end:right]} {
		letters := false
		for _, c := range []byte(side) {
			if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' {
				letters = true
			}
		}
		if letters && !allowed(strings.Trim(side, "+/")) {
			return false
		}
	}
	return true
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

// Explicit policy mirrored in Python and pinned by the shared fixture corpus.
func cardLayout(g []int) bool {
	switch len(g) {
	case 1:
		return g[0] >= 13 && g[0] <= 19
	case 3:
		return g[0] == 4 && (g[1] == 6 && (g[2] == 4 || g[2] == 5) || g[1] == 4 && g[2] == 5) || g[0] == 5 && g[1] == 5 && (g[2] == 5 || g[2] == 6)
	case 4:
		return g[0] == 4 && g[1] == 4 && g[2] == 4 && g[3] >= 1 && g[3] <= 4
	case 5:
		return g[0] == 4 && g[1] == 4 && g[2] == 4 && g[3] == 4 && g[4] >= 1 && g[4] <= 3
	}
	return false
}

// Every complete run/layout is evaluated at every start, including overlaps.
// A valid card wins over neighbouring phone/date/time interpretations.
func cardTokens(s string) [][2]int {
	atoms := cardDigits.FindAllStringIndex(s, -1)
	readings := [][2]int{}
	for i, a := range atoms {
		digits := ""
		groups := []int{}
		for j := i; j < len(atoms); j++ {
			if j > i && !cardSeparator.MatchString(s[atoms[j-1][1]:atoms[j][0]]) {
				break
			}
			digits += s[atoms[j][0]:atoms[j][1]]
			groups = append(groups, atoms[j][1]-atoms[j][0])
			if len(digits) > 19 {
				break
			}
			if cardLayout(groups) && validCard(digits) && cardBrand(digits) && cardTokenBoundary(s, a[0], atoms[j][1]) {
				readings = append(readings, [2]int{a[0], atoms[j][1]})
			}
		}
	}
	sort.Slice(readings, func(i, j int) bool {
		if readings[i][0] != readings[j][0] {
			return readings[i][0] < readings[j][0]
		}
		return readings[i][1] < readings[j][1]
	})
	valid := readings
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
	return applyCardEdits(s, cardRedactionEdits(s, context))
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
		if !json.Valid(line) {
			return nil, fmt.Errorf("invalid conversation JSON")
		}
		if cardCount(string(line)) == 0 {
			continue
		}
		cleaned := applyCardEdits(string(line), cardJSONEdits(string(line)))
		if cleaned == string(line) {
			continue
		}
		lines[i] = []byte(cleaned)
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
