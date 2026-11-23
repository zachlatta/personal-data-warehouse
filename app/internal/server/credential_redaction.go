package server

// Credential boundary for conversation storage, mirrored in
// src/personal_data_warehouse/credential_redaction.py and pinned by
// tests/fixtures/credential_redaction_cases.json. Each secret becomes
// [REDACTED:<kind>:<first 6 hex of sha256(secret)>], so one leaked value stays
// matchable across rows without being stored. Masking is not revocation.
//
// Rules run in priority order; a lower-priority edit overlapping an accepted
// one is dropped. Valid JSON is walked leaf by leaf (decoded strings at every
// nesting level), and a "password": "<value>"-style field masks its value.
// Lengths count runes, as Python counts characters.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

const credentialIdentifierWalk = 128

const credentialDelimiters = " \t\n\r\f\v\"'=:,;()[]{}<>|`"

const credentialLabel = `(?:secret[ _-]?access[ _-]?key|client[ _-]?secret|secret[ _-]?key|private[ _-]?key|` +
	`api[ _-]?key|apikey|passphrase|password|passwd|secret|token)`

var (
	credentialURL     = regexp.MustCompile(`(?i)(?:postgres(?:ql)?|mysql|mariadb|rediss?|amqps?|mongodb(?:\+srv)?|https?)://([^ \t\n\r\f\v/@:?#'"<>\[\]\\]*):([^ \t\n\r\f\v/@?#'"<>\[\]\\]+)@`)
	credentialPDW     = regexp.MustCompile(`(pdw_[a-z0-9]{12,24}_)([A-Za-z0-9_-]{30,})`)
	credentialSlack   = regexp.MustCompile(`xox[abcdepr](?:\.xox[a-z])?-[A-Za-z0-9%-]{10,}`)
	credentialGitHub  = regexp.MustCompile(`gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{22,}`)
	credentialSK      = regexp.MustCompile(`sk-[A-Za-z0-9_-]{20,}`)
	credentialAWSID   = regexp.MustCompile(`(?:AKIA|ASIA)[A-Z0-9]{16}`)
	credentialAWSRun  = regexp.MustCompile(`[A-Za-z0-9/+=]+`)
	credentialPEMHead = regexp.MustCompile(`-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----`)
	credentialPEMFoot = regexp.MustCompile(`-----END [A-Z0-9 ]*PRIVATE KEY-----`)
	credentialPEMTail = regexp.MustCompile(`^[A-Za-z0-9+/=\r\n\\]*`)
	credentialBase64  = regexp.MustCompile(`[A-Za-z0-9+/]`)
	credentialBearer  = regexp.MustCompile(`(?i)bearer[ \t]+([A-Za-z0-9._~+/=-]{16,})`)
	// The label is matched alone and its identifier found by a bounded walk
	// back, as Python does (a lazy prefix is quadratic in Python's engine).
	credentialLabelled = regexp.MustCompile(`(?i)(` + credentialLabel + `)[ \t]*\\*["']?[ \t]*[:=][ \t]*\\*["']?[ \t]*` +
		"([^ \\t\\n\\r\\f\\v\"'\\\\`,;&|<>(){}\\[\\]]+)")
	credentialLabelKey    = regexp.MustCompile(`(?i)^(?:[A-Za-z0-9_. -]*?` + credentialLabel + `)$`)
	credentialNotSecret   = regexp.MustCompile(`(?i)(?:^|[_. -])(?:page|next|prev|previous|continuation|sync|cursor|csrf|xsrf|idempotency|pagination|max|min|num|total|input|output|cache|count|reasoning|completion|prompt)[ _-]?token$`)
	credentialEnvName     = regexp.MustCompile(`^[A-Z0-9_]*_[A-Z0-9_]*$`)
	credentialPlaceholder = []string{"...", "***", "xxxx", "redacted", "example", "placeholder", "changeme", "your", "dummy"}
	credentialWeakURLPass = map[string]bool{"password": true, "pass": true, "passwd": true, "pwd": true, "secret": true, "changeme": true, "admin": true, "root": true, "guest": true}
)

func credentialMarker(kind, value string) string {
	sum := sha256.Sum256([]byte(value))
	return "[REDACTED:" + kind + ":" + hex.EncodeToString(sum[:])[:6] + "]"
}

func hasLetterAndDigit(s string) bool {
	letter, digit := false, false
	for _, c := range []byte(s) {
		letter = letter || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
		digit = digit || c >= '0' && c <= '9'
	}
	return letter && digit
}

func credentialPlaceholderValue(s string) bool {
	first, _ := utf8.DecodeRuneInString(s)
	if strings.ContainsRune("$%<{*[.~/=#", first) {
		return true
	}
	same := true
	for _, c := range s {
		if c != first {
			same = false
			break
		}
	}
	if same {
		return true
	}
	lower := strings.ToLower(s)
	for _, word := range credentialPlaceholder {
		if strings.Contains(lower, word) {
			return true
		}
	}
	return false
}

func credentialSecretValue(s string) bool {
	return utf8.RuneCountInString(s) >= 8 && hasLetterAndDigit(s) && !credentialPlaceholderValue(s) && !credentialEnvName.MatchString(s)
}

func credentialBoundary(s string, start int, extra string) bool {
	return start == 0 || !cardWordByte(s[start-1]) && !strings.ContainsRune(extra, rune(s[start-1]))
}

type credentialCandidate struct {
	start, end int
	kind       string
}

func credentialTextEdits(s string) []cardEdit {
	candidates := []credentialCandidate{}
	for _, head := range credentialPEMHead.FindAllStringIndex(s, -1) {
		end := head[1] + len(credentialPEMTail.FindString(s[head[1]:]))
		if foot := credentialPEMFoot.FindStringIndex(s[head[1]:]); foot != nil {
			end = head[1] + foot[0]
		}
		if len(credentialBase64.FindAllStringIndex(s[head[1]:end], -1)) >= 64 {
			candidates = append(candidates, credentialCandidate{head[1], end, "private_key"})
		}
	}
	for _, m := range credentialURL.FindAllStringSubmatchIndex(s, -1) {
		user, password := s[m[2]:m[3]], s[m[4]:m[5]]
		if utf8.RuneCountInString(password) >= 6 && password != user && !credentialPlaceholderValue(password) && !credentialWeakURLPass[strings.ToLower(password)] {
			candidates = append(candidates, credentialCandidate{m[4], m[5], "url_password"})
		}
	}
	for _, m := range credentialPDW.FindAllStringSubmatchIndex(s, -1) {
		candidates = append(candidates, credentialCandidate{m[4], m[5], "pdw_token"})
	}
	for _, m := range credentialSlack.FindAllStringIndex(s, -1) {
		token := s[m[0]:m[1]]
		if credentialBoundary(s, m[0], "") && hasLetterAndDigit(token[5:]) && !credentialPlaceholderValue(token) {
			candidates = append(candidates, credentialCandidate{m[0], m[1], "slack_token"})
		}
	}
	for _, m := range credentialGitHub.FindAllStringIndex(s, -1) {
		token := s[m[0]:m[1]]
		if credentialBoundary(s, m[0], "") && hasLetterAndDigit(token) && !credentialPlaceholderValue(token) {
			candidates = append(candidates, credentialCandidate{m[0], m[1], "github_token"})
		}
	}
	for _, m := range credentialSK.FindAllStringIndex(s, -1) {
		token := s[m[0]:m[1]]
		if credentialBoundary(s, m[0], "-/.") && hasLetterAndDigit(token[3:]) && !credentialPlaceholderValue(token) {
			kind := "openai_key"
			if strings.HasPrefix(token, "sk-ant-") {
				kind = "anthropic_key"
			}
			candidates = append(candidates, credentialCandidate{m[0], m[1], kind})
		}
	}
	awsID := false
	for _, m := range credentialAWSID.FindAllStringIndex(s, -1) {
		if credentialBoundary(s, m[0], "") && (m[1] == len(s) || !cardWordByte(s[m[1]])) {
			awsID = true
			break
		}
	}
	if awsID {
		for _, m := range credentialAWSRun.FindAllStringIndex(s, -1) {
			run := s[m[0]:m[1]]
			upper, lower := false, false
			for _, c := range []byte(run) {
				upper = upper || c >= 'A' && c <= 'Z'
				lower = lower || c >= 'a' && c <= 'z'
			}
			// The run must stand alone: one cut out of a URL path or encoding
			// (.com/..., -2B..., %2F...) is part of a longer value.
			if len(run) == 40 && !strings.Contains(run, "=") &&
				(m[0] == 0 || strings.IndexByte(credentialDelimiters, s[m[0]-1]) >= 0) &&
				(m[1] == len(s) || strings.IndexByte(credentialDelimiters, s[m[1]]) >= 0) && upper && lower && hasLetterAndDigit(run) && !credentialPlaceholderValue(run) {
				candidates = append(candidates, credentialCandidate{m[0], m[1], "aws_secret_key"})
			}
		}
	}
	for _, m := range credentialBearer.FindAllStringSubmatchIndex(s, -1) {
		if credentialBoundary(s, m[0], "") && credentialSecretValue(s[m[2]:m[3]]) {
			candidates = append(candidates, credentialCandidate{m[2], m[3], "bearer_token"})
		}
	}
	for _, m := range credentialLabelled.FindAllStringSubmatchIndex(s, -1) {
		start := m[2]
		for start > 0 && start > m[2]-credentialIdentifierWalk && (cardWordByte(s[start-1]) || s[start-1] == '.' || s[start-1] == '-') {
			start--
		}
		label, secret := s[start:m[3]], s[m[4]:m[5]]
		if credentialNotSecret.MatchString(label) || !credentialSecretValue(secret) || m[1] < len(s) && s[m[1]] == '(' {
			continue
		}
		kind := "secret"
		if strings.Contains(strings.ToLower(label), "pass") {
			kind = "password"
		}
		candidates = append(candidates, credentialCandidate{m[4], m[5], kind})
	}
	accepted := []cardEdit{}
	for _, c := range candidates {
		overlaps := false
		for _, a := range accepted {
			if c.start < a.end && c.end > a.start {
				overlaps = true
				break
			}
		}
		if !overlaps {
			accepted = append(accepted, cardEdit{c.start, c.end, credentialMarker(c.kind, s[c.start:c.end])})
		}
	}
	sort.Slice(accepted, func(i, j int) bool { return accepted[i].start < accepted[j].start })
	return accepted
}

// credentialEdits returns spans in s; JSON lexemes outside them are untouched.
func credentialEdits(s string) []cardEdit {
	tokens, valid := cardJSONTokens(s)
	if !valid {
		return credentialTextEdits(s)
	}
	edits := []cardEdit{}
	for i, token := range tokens {
		raw := s[token[0]:token[1]]
		if !strings.HasPrefix(raw, `"`) {
			continue
		}
		var decoded string
		_ = json.Unmarshal([]byte(raw), &decoded)
		inner := credentialEdits(decoded)
		if len(inner) == 0 && i >= 2 && s[tokens[i-1][0]:tokens[i-1][1]] == ":" && strings.HasPrefix(s[tokens[i-2][0]:tokens[i-2][1]], `"`) {
			var key string
			_ = json.Unmarshal([]byte(s[tokens[i-2][0]:tokens[i-2][1]]), &key)
			if len(key) <= credentialIdentifierWalk && credentialLabelKey.MatchString(key) && !credentialNotSecret.MatchString(key) &&
				!strings.ContainsAny(decoded, " \t\n\r\f\v") && credentialSecretValue(decoded) {
				kind := "secret"
				if strings.Contains(strings.ToLower(key), "pass") {
					kind = "password"
				}
				inner = []cardEdit{{0, len(decoded), credentialMarker(kind, decoded)}}
			}
		}
		if len(inner) > 0 {
			bounds := cardJSONStringBounds(raw)
			for _, e := range inner {
				edits = append(edits, cardEdit{token[0] + bounds[e.start], token[0] + bounds[e.end], e.replacement})
			}
		}
	}
	return edits
}

func credentialCount(s string) int { return len(credentialEdits(s)) }

func redactCredentials(s string) string { return applyCardEdits(s, credentialEdits(s)) }

// withCredentials composes card edits with credential edits: a credential owns
// its whole span, so overlapping card edits are dropped.
func withCredentials(s string, cards []cardEdit) []cardEdit {
	credentials := credentialEdits(s)
	edits := append([]cardEdit{}, credentials...)
	for _, card := range cards {
		overlaps := false
		for _, c := range credentials {
			if card.start < c.end && card.end > c.start {
				overlaps = true
				break
			}
		}
		if !overlaps {
			edits = append(edits, card)
		}
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].start < edits[j].start })
	return edits
}
