package server

import (
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Validation and token edits preserve untouched bytes, including duplicate keys,
// numeric lexemes, whitespace and escapes. No parsed object is reserialized.
var cardJSONToken = regexp.MustCompile(`"(?:[^"\\]|\\.)*"|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|[{}\[\]:,]|true|false|null`)

type cardEdit struct {
	start, end  int
	replacement string
}

func applyCardEdits(s string, edits []cardEdit) string {
	var out strings.Builder
	cursor := 0
	for _, edit := range edits {
		out.WriteString(s[cursor:edit.start])
		out.WriteString(edit.replacement)
		cursor = edit.end
	}
	out.WriteString(s[cursor:])
	return out.String()
}

func cardTextEdits(s string) []cardEdit {
	// Retain original offsets while decoding escaped card digits. Unmasked
	// escapes must not change merely because another part contains a card.
	var decoded strings.Builder
	bounds := []int{0}
	for pos := 0; pos < len(s); {
		match := cardEscape.FindStringIndex(s[pos:])
		if match == nil {
			decoded.WriteString(s[pos:])
			for i := pos; i < len(s); i++ {
				bounds = append(bounds, i+1)
			}
			break
		}
		start, end := pos+match[0], pos+match[1]
		decoded.WriteString(s[pos:start])
		for i := pos; i < start; i++ {
			bounds = append(bounds, i+1)
		}
		n, _ := strconv.ParseInt(s[end-4:end], 16, 32)
		decoded.WriteByte(byte(n)) // The escape grammar contains ASCII only.
		bounds = append(bounds, end)
		pos = end
	}
	value := decoded.String()
	edits := []cardEdit{}
	for _, span := range cardTokens(value) {
		start, end := span[0], span[1]
		digits := strings.NewReplacer(" ", "", "-", "").Replace(value[start:end])
		edits = append(edits, cardEdit{start, end, "[card ****" + digits[len(digits)-4:] + "]"})
		for {
			code := adjacentCardCode.FindStringSubmatchIndex(value[end:])
			if code == nil || !standaloneCardTokenEnd(value, end+code[3]) {
				break
			}
			edits = append(edits, cardEdit{end + code[2], end + code[3], "[redacted]"})
			end += code[3]
		}
	}
	for _, match := range codeValue.FindAllStringSubmatchIndex(value, -1) {
		start, end := match[4], match[5]
		overlaps := false
		for _, edit := range edits {
			if start < edit.end && end > edit.start {
				overlaps = true
				break
			}
		}
		if !overlaps {
			edits = append(edits, cardEdit{start, end, "[redacted]"})
		}
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].start < edits[j].start })
	for i := range edits {
		edits[i].start = bounds[edits[i].start]
		edits[i].end = bounds[edits[i].end]
	}
	return edits
}

func cardJSONStringBounds(raw string) []int {
	bounds := []int{1}
	for pos := 1; pos < len(raw)-1; {
		start := pos
		_, width := utf8.DecodeRuneInString(raw[pos:])
		if raw[pos] == '\\' {
			pos += 2
			if raw[start+1] == 'u' {
				pos = start + 6
				high, _ := strconv.ParseUint(raw[start+2:pos], 16, 16)
				if high >= 0xD800 && high <= 0xDBFF && strings.HasPrefix(raw[pos:], `\u`) {
					low, _ := strconv.ParseUint(raw[pos+2:pos+6], 16, 16)
					if low >= 0xDC00 && low <= 0xDFFF {
						pos += 6
					}
				}
			}
			var decoded string
			_ = json.Unmarshal([]byte(`"`+raw[start:pos]+`"`), &decoded)
			width = len(decoded)
		} else {
			pos += width
			if width == 1 && raw[start] >= 0x80 {
				width = 3
			} // Invalid UTF-8 decodes as U+FFFD.
		}
		for i := 0; i < width; i++ {
			bounds = append(bounds, pos)
		}
	}
	return bounds
}

func cardCodeToken(raw string) bool {
	if strings.HasPrefix(raw, `"`) {
		var decoded string
		_ = json.Unmarshal([]byte(raw), &decoded)
		return codeScalar.MatchString(strings.TrimSpace(decoded))
	}
	if raw[0] != '-' && (raw[0] < '0' || raw[0] > '9') {
		return false
	}
	// Classify numeric code fields exactly without floats or allocating an
	// enormous integer for a large exponent. Unchanged lexemes stay untouched.
	parts := strings.SplitN(strings.ToLower(raw), "e", 2)
	mantissa := strings.TrimPrefix(parts[0], "-")
	fraction := 0
	if dot := strings.IndexByte(mantissa, '.'); dot >= 0 {
		fraction = len(mantissa) - dot - 1
	}
	digits := strings.TrimLeft(strings.ReplaceAll(mantissa, ".", ""), "0")
	if digits == "" {
		return true
	}
	if raw[0] == '-' {
		return false
	}
	exponent := 0
	if len(parts) == 2 {
		var err error
		exponent, err = strconv.Atoi(parts[1])
		if err != nil || exponent > len(raw)+4 || exponent < -len(raw)-4 {
			return false
		}
	}
	significant := strings.TrimRight(digits, "0")
	scale := exponent - fraction + len(digits) - len(significant)
	return scale >= 0 && scale <= 4 && len(significant)+scale <= 4
}

func cardRedactionEdits(s string, context bool) []cardEdit {
	if !context && cardCount(s) == 0 {
		return nil
	}
	if _, valid := cardJSONTokens(s); valid {
		return cardJSONEdits(s)
	}
	return cardTextEdits(s)
}

func cardJSONTokens(s string) ([][]int, bool) {
	trimmed := strings.TrimSpace(s)
	if len(trimmed) > 0 && strings.ContainsRune("{[\"", rune(trimmed[0])) && json.Valid([]byte(s)) {
		return cardJSONToken.FindAllStringIndex(s, -1), true
	}
	return nil, false
}

// Counting and editing discover the same decoded leaves. Do not use raw-byte
// detection as a negative filter: Unicode may encode escape backslashes too.
func cardCount(s string) int {
	if tokens, valid := cardJSONTokens(s); valid {
		count := 0
		for _, token := range tokens {
			raw := s[token[0]:token[1]]
			if raw[0] == '"' {
				var decoded string
				_ = json.Unmarshal([]byte(raw), &decoded)
				count += cardCount(decoded)
			} else if raw[0] == '-' || raw[0] >= '0' && raw[0] <= '9' {
				count += cardCount(raw)
			}
		}
		return count
	}
	return len(cardTokens(decodeCardEscapes(s)))
}

// The caller validates JSON and establishes card context. Unlike the text
// entry point, this also quotes masks replacing standalone numeric JSON values.
func cardJSONEdits(s string) []cardEdit {
	tokens := cardJSONToken.FindAllStringIndex(s, -1)
	codes := map[int]bool{}
	for i := 0; i+2 < len(tokens); i++ {
		raw := s[tokens[i][0]:tokens[i][1]]
		if !strings.HasPrefix(raw, `"`) || s[tokens[i+1][0]:tokens[i+1][1]] != ":" {
			continue
		}
		var key string
		_ = json.Unmarshal([]byte(raw), &key)
		if codeKey.MatchString(key) && cardCodeToken(s[tokens[i+2][0]:tokens[i+2][1]]) {
			codes[i+2] = true
		}
	}
	edits := []cardEdit{}
	for i, token := range tokens {
		raw := s[token[0]:token[1]]
		if codes[i] {
			edits = append(edits, cardEdit{token[0], token[1], `"[redacted]"`})
		} else if strings.HasPrefix(raw, `"`) {
			var decoded string
			_ = json.Unmarshal([]byte(raw), &decoded)
			inner := cardRedactionEdits(decoded, true)
			if len(inner) > 0 {
				bounds := cardJSONStringBounds(raw)
				for _, edit := range inner {
					encoded, _ := json.Marshal(edit.replacement)
					edits = append(edits, cardEdit{token[0] + bounds[edit.start], token[0] + bounds[edit.end], string(encoded[1 : len(encoded)-1])})
				}
			}
		} else if raw[0] == '-' || raw[0] >= '0' && raw[0] <= '9' {
			inner := cardTextEdits(raw)
			if len(inner) > 0 {
				encoded, _ := json.Marshal(applyCardEdits(raw, inner))
				edits = append(edits, cardEdit{token[0], token[1], string(encoded)})
			}
		}
	}
	return edits
}
