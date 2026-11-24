package mutations

import (
	"context"
	"net/mail"
	"sort"
	"strings"
)

const (
	gmailSenderNameCandidateLimit    = 20
	gmailRecipientNameCandidateLimit = 10
)

// enrichGmailEmailAddresses resolves human-readable display names for the
// sender (From) and any bare recipient addresses (To, Cc, Bcc) on
// gmail.send_email mutations from contacts first and prior Gmail From headers
// second. Proposer- or reviewer-supplied display names are preserved as-is,
// and addresses with no known name remain bare.
func (s *PostgresStore) enrichGmailEmailAddresses(ctx context.Context, mutations []storedMutation) []storedMutation {
	if len(mutations) == 0 {
		return mutations
	}
	accounts, recipients := gmailAddressLookupTargets(mutations)
	resolved := map[string]string{}
	if s != nil && s.db != nil && (len(accounts) > 0 || len(recipients) > 0) {
		allEmails := make([]string, 0, len(accounts)+len(recipients))
		seen := map[string]bool{}
		for _, email := range accounts {
			if !seen[email] {
				seen[email] = true
				allEmails = append(allEmails, email)
			}
		}
		for _, email := range recipients {
			if !seen[email] {
				seen[email] = true
				allEmails = append(allEmails, email)
			}
		}
		for email, name := range s.lookupContactDisplayNames(ctx, allEmails) {
			resolved[email] = name
		}
		for _, account := range accounts {
			if resolved[account] != "" {
				continue
			}
			if name := s.lookupGmailSentFromDisplayName(ctx, account); name != "" {
				resolved[account] = name
			}
		}
		for _, email := range recipients {
			if resolved[email] != "" {
				continue
			}
			if name := s.lookupGmailFromDisplayName(ctx, email); name != "" {
				resolved[email] = name
			}
		}
	}
	return applyGmailEmailDisplayNames(mutations, resolved)
}

func gmailAddressLookupTargets(mutations []storedMutation) (accounts []string, recipients []string) {
	seenAccounts := map[string]bool{}
	seenRecipients := map[string]bool{}
	collectMessage := func(account string, message map[string]any) {
		if account != "" && !seenAccounts[account] {
			name, addr, ok := parseEmailAddressEntry(stringFromAny(message["from"]))
			if !ok || name == "" || !strings.EqualFold(addr, account) {
				seenAccounts[account] = true
				accounts = append(accounts, account)
			}
		}
		for _, field := range []string{"to", "cc", "bcc"} {
			for _, raw := range stringSliceFromAny(message[field]) {
				name, addr, ok := parseEmailAddressEntry(raw)
				if !ok || name != "" {
					continue
				}
				lower := strings.ToLower(addr)
				if lower != "" && !seenRecipients[lower] {
					seenRecipients[lower] = true
					recipients = append(recipients, lower)
				}
			}
		}
	}
	for _, mutation := range mutations {
		if mutation.Provider != "gmail" || mutation.Operation != GmailSendEmailOperation {
			continue
		}
		account := normalizeAccount(mutation.Account)
		payload := mapFromAny(mutation.Payload)
		collectMessage(account, mapFromAny(payload["message"]))
		for _, variant := range normalizeStoredEmailVariants(payload["variants"]) {
			collectMessage(account, mapFromAny(variant["message"]))
		}
	}
	sort.Strings(accounts)
	sort.Strings(recipients)
	return accounts, recipients
}

func (s *PostgresStore) lookupContactDisplayNames(ctx context.Context, emails []string) map[string]string {
	out := map[string]string{}
	if len(emails) == 0 {
		return out
	}
	rows, err := queryContext(ctx, s.db, `
		SELECT normalized_value, display_name
		FROM @clean_contact_points
		WHERE point_type = 'email'
		  AND normalized_value = ANY($1)
		  AND COALESCE(display_name, '') <> ''
		ORDER BY normalized_value, source_updated_at DESC, display_name ASC
	`, emails)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var normalized, displayName string
		if err := rows.Scan(&normalized, &displayName); err != nil {
			return out
		}
		email := strings.ToLower(strings.TrimSpace(normalized))
		name := cleanDisplayName(displayName)
		if email == "" || name == "" || strings.EqualFold(name, email) {
			continue
		}
		if _, exists := out[email]; !exists {
			out[email] = name
		}
	}
	return out
}

func (s *PostgresStore) lookupGmailSentFromDisplayName(ctx context.Context, account string) string {
	rows, err := queryContext(ctx, s.db, `
		SELECT COALESCE(
			substring(payload_json::text FROM '"name":\s*"From",\s*"value":\s*"((?:[^"\\]|\\.)*)"'),
			''
		)
		FROM @gmail_messages
		WHERE account = $1
		  AND is_deleted = 0
		  AND 'SENT' = ANY(label_ids)
		  AND lower(from_address) = $1
		ORDER BY internal_date DESC
		LIMIT $2
	`, account, gmailSenderNameCandidateLimit)
	if err != nil {
		return ""
	}
	defer rows.Close()
	for rows.Next() {
		var fromHeader string
		if err := rows.Scan(&fromHeader); err != nil {
			return ""
		}
		if name := cleanDisplayName(gmailHeaderDisplayName(fromHeader)); name != "" && !strings.EqualFold(name, account) {
			return name
		}
	}
	return ""
}

func (s *PostgresStore) lookupGmailFromDisplayName(ctx context.Context, email string) string {
	rows, err := queryContext(ctx, s.db, `
		SELECT COALESCE(
			substring(payload_json::text FROM '"name":\s*"From",\s*"value":\s*"((?:[^"\\]|\\.)*)"'),
			''
		)
		FROM @gmail_messages
		WHERE is_deleted = 0
		  AND from_address ILIKE $1 ESCAPE '\'
		  AND lower(from_address) = $2
		ORDER BY internal_date DESC
		LIMIT $3
	`, escapeLikeLiteral(email), email, gmailRecipientNameCandidateLimit)
	if err != nil {
		return ""
	}
	defer rows.Close()
	for rows.Next() {
		var fromHeader string
		if err := rows.Scan(&fromHeader); err != nil {
			return ""
		}
		if name := cleanDisplayName(gmailHeaderDisplayName(fromHeader)); name != "" && !strings.EqualFold(name, email) {
			return name
		}
	}
	return ""
}

func escapeLikeLiteral(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(value)
}

func applyGmailEmailDisplayNames(mutations []storedMutation, names map[string]string) []storedMutation {
	if len(mutations) == 0 {
		return mutations
	}
	out := make([]storedMutation, len(mutations))
	copy(out, mutations)
	for index := range out {
		mutation := &out[index]
		if mutation.Provider != "gmail" || mutation.Operation != GmailSendEmailOperation {
			continue
		}
		account := normalizeAccount(mutation.Account)
		payload := cloneMap(mutation.Payload)
		message := mapFromAny(payload["message"])
		variants := normalizeStoredEmailVariants(payload["variants"])
		selectedVariantID := strings.TrimSpace(stringFromAny(payload["selected_variant_id"]))
		changed := false

		if len(variants) > 0 {
			if selectedVariantID == "" {
				selectedVariantID = stringFromAny(variants[0]["id"])
				payload["selected_variant_id"] = selectedVariantID
				changed = true
			}
			for variantIndex, variant := range variants {
				variantMessage := mapFromAny(variant["message"])
				if enriched, ok := gmailMessageWithDisplayNames(account, variantMessage, names); ok {
					variants[variantIndex]["message"] = enriched
					variantMessage = enriched
					changed = true
				}
				if stringFromAny(variant["id"]) == selectedVariantID {
					message = variantMessage
				}
			}
			payload["variants"] = variants
		} else {
			if enriched, ok := gmailMessageWithDisplayNames(account, message, names); ok {
				message = enriched
				changed = true
			}
		}
		if !changed {
			continue
		}
		payload["message"] = message
		mutation.Payload = payload
		syncGmailEmailPreviewFromPayload(mutation, message, variants)
	}
	return out
}

func gmailMessageWithDisplayNames(account string, message map[string]any, names map[string]string) (map[string]any, bool) {
	out := cloneMap(message)
	changed := false

	if account != "" {
		existingFrom := strings.TrimSpace(stringFromAny(out["from"]))
		wantFrom := ""
		if name, addr, ok := parseEmailAddressEntry(existingFrom); ok && name != "" && strings.EqualFold(addr, account) {
			wantFrom = formatDisplayEmailAddress(name, account)
		} else if resolved := cleanDisplayName(names[account]); resolved != "" {
			wantFrom = formatDisplayEmailAddress(resolved, account)
		} else {
			wantFrom = account
		}
		if wantFrom != existingFrom {
			out["from"] = wantFrom
			changed = true
		}
	}

	for _, field := range []string{"to", "cc", "bcc"} {
		existing := stringSliceFromAny(out[field])
		if len(existing) == 0 {
			continue
		}
		updated := make([]string, len(existing))
		fieldChanged := false
		for i, raw := range existing {
			formatted := enrichRecipientAddress(raw, names)
			updated[i] = formatted
			if formatted != raw {
				fieldChanged = true
			}
		}
		if fieldChanged {
			out[field] = updated
			changed = true
		}
	}
	return out, changed
}

func enrichRecipientAddress(raw string, names map[string]string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	name, addr, ok := parseEmailAddressEntry(trimmed)
	if !ok {
		return trimmed
	}
	if name != "" {
		return formatDisplayEmailAddress(name, addr)
	}
	if resolved := cleanDisplayName(names[strings.ToLower(addr)]); resolved != "" {
		return formatDisplayEmailAddress(resolved, addr)
	}
	return addr
}

func parseEmailAddressEntry(raw string) (string, string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", "", false
	}
	if parsed, err := mail.ParseAddress(trimmed); err == nil {
		addr := strings.TrimSpace(parsed.Address)
		if addr == "" {
			return "", "", false
		}
		name := cleanDisplayName(parsed.Name)
		if strings.EqualFold(name, addr) {
			name = ""
		}
		return name, addr, true
	}
	if strings.HasSuffix(trimmed, ">") {
		if idx := strings.LastIndex(trimmed, "<"); idx >= 0 {
			candidateAddr := strings.TrimSpace(trimmed[idx+1 : len(trimmed)-1])
			if parsed, err := mail.ParseAddress(candidateAddr); err == nil && strings.TrimSpace(parsed.Address) != "" {
				addr := strings.TrimSpace(parsed.Address)
				name := cleanDisplayName(strings.Trim(strings.TrimSpace(trimmed[:idx]), `"`))
				if strings.EqualFold(name, addr) {
					name = ""
				}
				return name, addr, true
			}
		}
	}
	return "", "", false
}

// formatDisplayEmailAddress formats an email address for storage and human
// review: human-readable UTF-8 display names (not RFC 2047 noise in the UI),
// RFC 5322-quoted whenever the name contains special characters like commas,
// periods, or quotes. The Python MIME builder applies RFC 2047 wire encoding
// for non-ASCII names when sending.
func formatDisplayEmailAddress(name string, addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	clean := cleanDisplayName(name)
	if clean == "" || strings.EqualFold(clean, addr) {
		return addr
	}
	if needsRFC5322Quoting(clean) {
		escaped := strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(clean)
		return `"` + escaped + `" <` + addr + `>`
	}
	return clean + " <" + addr + ">"
}

func cleanDisplayName(value string) string {
	var b strings.Builder
	b.Grow(len(value))
	for _, r := range value {
		if r < 32 || r == 127 {
			b.WriteByte(' ')
		} else {
			b.WriteRune(r)
		}
	}
	return strings.Join(strings.Fields(b.String()), " ")
}

func needsRFC5322Quoting(name string) bool {
	return strings.ContainsAny(name, `"\\()<>@,;:[].`)
}
