package mutations

import (
	"regexp"
	"sort"
	"strings"
	"time"
)

var markdownImagePattern = regexp.MustCompile(`!\[[^\]]*\]\([^)]+\)`)
var gmailQuotedTextPattern = regexp.MustCompile(`(?i)\s+On\s+(Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+.+$`)

type gmailThreadPreviewRow struct {
	Account           string
	ThreadID          string
	MessageID         string
	Subject           string
	FromAddress       string
	ToAddresses       []string
	CCAddresses       []string
	LabelIDs          []string
	InternalDate      time.Time
	Snippet           string
	PreviewText       string
	BodyHTML          string
	MessageCount      int
	InboxMessageCount int
}

type gmailThreadPreviewKey struct {
	Account  string
	ThreadID string
}

func applyGmailThreadPreviewRows(mutations []Mutation, rows []gmailThreadPreviewRow) []Mutation {
	if len(mutations) == 0 || len(rows) == 0 {
		return mutations
	}
	rowsByThread := map[gmailThreadPreviewKey][]gmailThreadPreviewRow{}
	for _, row := range rows {
		key := gmailThreadPreviewKey{Account: normalizeAccount(row.Account), ThreadID: strings.TrimSpace(row.ThreadID)}
		if key.Account == "" || key.ThreadID == "" {
			continue
		}
		rowsByThread[key] = append(rowsByThread[key], row)
	}
	if len(rowsByThread) == 0 {
		return mutations
	}

	out := make([]Mutation, len(mutations))
	copy(out, mutations)
	for index, mutation := range out {
		switch {
		case isGmailThreadMutation(mutation):
			threadIDs := gmailMutationThreadIDs(mutation)
			threads, changed := mergeGmailThreadPreviews(mutation.Account, threadIDs, mapSliceFromAny(mutation.Preview["threads"]), rowsByThread)
			if changed {
				preview := cloneMap(mutation.Preview)
				preview["thread_count"] = len(threads)
				preview["threads"] = threads
				out[index].Preview = preview
			}
		case isGmailEmailMutation(mutation):
			threadIDs := gmailEmailReplyThreadIDs(mutation)
			threads, changed := mergeGmailThreadPreviews(mutation.Account, threadIDs, mapSliceFromAny(mutation.Preview["reply_threads"]), rowsByThread)
			if changed {
				preview := cloneMap(mutation.Preview)
				preview["reply_thread_count"] = len(threads)
				preview["reply_threads"] = threads
				out[index].Preview = preview
			}
		}
	}
	return out
}

func mergeGmailThreadPreviews(account string, threadIDs []string, existingThreads []map[string]any, rowsByThread map[gmailThreadPreviewKey][]gmailThreadPreviewRow) ([]map[string]any, bool) {
	if len(threadIDs) == 0 {
		return nil, false
	}
	existingByID := map[string]map[string]any{}
	for _, thread := range existingThreads {
		threadID := strings.TrimSpace(stringFromAny(thread["thread_id"]))
		if threadID != "" {
			existingByID[threadID] = thread
		}
	}

	threads := make([]map[string]any, 0, len(threadIDs))
	changed := false
	for _, threadID := range threadIDs {
		thread := cloneMap(existingByID[threadID])
		if len(thread) == 0 {
			thread["thread_id"] = threadID
		}
		if threadRows := rowsByThread[gmailThreadPreviewKey{Account: normalizeAccount(account), ThreadID: threadID}]; len(threadRows) > 0 {
			thread = gmailThreadPreviewFromRows(threadID, threadRows)
			changed = true
		}
		threads = append(threads, thread)
	}
	return threads, changed
}

func gmailThreadPreviewTargets(mutations []Mutation) []gmailThreadPreviewKey {
	targets := []gmailThreadPreviewKey{}
	seen := map[gmailThreadPreviewKey]bool{}
	for _, mutation := range mutations {
		account := normalizeAccount(mutation.Account)
		if account == "" {
			continue
		}
		threadIDs := []string{}
		if isGmailThreadMutation(mutation) {
			threadIDs = gmailMutationThreadIDs(mutation)
		} else if isGmailEmailMutation(mutation) {
			threadIDs = gmailEmailReplyThreadIDs(mutation)
		}
		for _, threadID := range threadIDs {
			key := gmailThreadPreviewKey{Account: account, ThreadID: strings.TrimSpace(threadID)}
			if key.ThreadID == "" || seen[key] {
				continue
			}
			targets = append(targets, key)
			seen[key] = true
		}
	}
	return targets
}

func gmailThreadPreviewFromRows(threadID string, rows []gmailThreadPreviewRow) map[string]any {
	ordered := make([]gmailThreadPreviewRow, 0, len(rows))
	for _, row := range rows {
		if strings.TrimSpace(row.ThreadID) == "" {
			row.ThreadID = threadID
		}
		ordered = append(ordered, row)
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if !ordered[i].InternalDate.Equal(ordered[j].InternalDate) {
			return ordered[i].InternalDate.Before(ordered[j].InternalDate)
		}
		return ordered[i].MessageID < ordered[j].MessageID
	})

	latest := ordered[0]
	for _, row := range ordered[1:] {
		if row.InternalDate.After(latest.InternalDate) || (row.InternalDate.Equal(latest.InternalDate) && row.MessageID < latest.MessageID) {
			latest = row
		}
	}
	messageCount := len(ordered)
	inboxMessageCount := 0
	labels := []string{}
	messages := make([]map[string]any, 0, len(ordered))
	for _, row := range ordered {
		if row.MessageCount > messageCount {
			messageCount = row.MessageCount
		}
		if row.InboxMessageCount > inboxMessageCount {
			inboxMessageCount = row.InboxMessageCount
		}
		labels = appendVisibleGmailLabels(labels, row.LabelIDs)
		message := map[string]any{
			"message_id":    row.MessageID,
			"from_address":  row.FromAddress,
			"to_addresses":  append([]string{}, row.ToAddresses...),
			"cc_addresses":  append([]string{}, row.CCAddresses...),
			"internal_date": formatPreviewTime(row.InternalDate),
			"snippet":       compactWhitespace(row.Snippet),
			"preview_text":  bestGmailPreviewText(row),
			"label_ids":     append([]string{}, row.LabelIDs...),
		}
		if bodyHTML := strings.TrimSpace(row.BodyHTML); bodyHTML != "" {
			message["body_html"] = bodyHTML
		}
		messages = append(messages, message)
	}
	if inboxMessageCount == 0 {
		for _, row := range ordered {
			if containsString(row.LabelIDs, "INBOX") {
				inboxMessageCount++
			}
		}
	}

	subject := strings.TrimSpace(latest.Subject)
	if subject == "" {
		subject = "(no subject)"
	}
	return map[string]any{
		"thread_id":           threadID,
		"subject":             subject,
		"latest_from_address": strings.TrimSpace(latest.FromAddress),
		"latest_at":           formatPreviewTime(latest.InternalDate),
		"latest_preview":      bestGmailPreviewText(latest),
		"message_count":       messageCount,
		"inbox_message_count": inboxMessageCount,
		"labels":              labels,
		"messages":            messages,
	}
}

func isGmailThreadMutation(mutation Mutation) bool {
	return mutation.Provider == "gmail" && (mutation.Operation == GmailArchiveOperation || mutation.Operation == GmailUnarchiveOperation)
}

func gmailEmailReplyThreadIDs(mutation Mutation) []string {
	seen := map[string]bool{}
	out := []string{}
	appendMessageThreadID := func(message map[string]any) {
		threadID := strings.TrimSpace(stringFromAny(message["reply_to_thread_id"]))
		if threadID == "" || seen[threadID] {
			return
		}
		out = append(out, threadID)
		seen[threadID] = true
	}

	payload := mapFromAny(mutation.Payload)
	appendMessageThreadID(mapFromAny(payload["message"]))
	for _, variant := range normalizeStoredEmailVariants(payload["variants"]) {
		appendMessageThreadID(mapFromAny(variant["message"]))
	}

	previewEmail := mapFromAny(mutation.Preview["email"])
	appendMessageThreadID(previewEmail)
	for _, variant := range normalizeStoredEmailVariants(previewEmail["variants"]) {
		appendMessageThreadID(mapFromAny(variant["message"]))
	}
	return out
}

func gmailMutationThreadIDs(mutation Mutation) []string {
	threadIDs := stringSliceFromAny(mutation.Payload["thread_ids"])
	if len(threadIDs) > 0 {
		return threadIDs
	}
	out := []string{}
	for _, thread := range mapSliceFromAny(mutation.Preview["threads"]) {
		threadID := strings.TrimSpace(stringFromAny(thread["thread_id"]))
		if threadID != "" {
			out = append(out, threadID)
		}
	}
	return out
}

func appendVisibleGmailLabels(existing []string, labels []string) []string {
	seen := map[string]bool{}
	for _, label := range existing {
		seen[label] = true
	}
	for _, label := range labels {
		normalized := formatGmailLabel(label)
		if normalized == "" || seen[normalized] {
			continue
		}
		existing = append(existing, normalized)
		seen[normalized] = true
	}
	return existing
}

func bestGmailPreviewText(row gmailThreadPreviewRow) string {
	if text := cleanGmailPreviewText(row.Snippet); text != "" {
		return text
	}
	return cleanGmailPreviewText(row.PreviewText)
}

func cleanGmailPreviewText(value string) string {
	value = markdownImagePattern.ReplaceAllString(value, "")
	value = gmailQuotedTextPattern.ReplaceAllString(value, "")
	value = strings.ReplaceAll(value, "|", " ")
	value = strings.ReplaceAll(value, "---", " ")
	return compactWhitespace(value)
}

func formatGmailLabel(value string) string {
	normalized := strings.TrimSpace(value)
	if normalized == "" || strings.HasPrefix(normalized, "Label_") {
		return ""
	}
	switch normalized {
	case "INBOX", "TRASH", "SPAM", "CATEGORY_PERSONAL":
		return ""
	case "UNREAD":
		return "Unread"
	case "IMPORTANT":
		return "Important"
	case "STARRED":
		return "Starred"
	case "CATEGORY_UPDATES":
		return "Updates"
	case "CATEGORY_PROMOTIONS":
		return "Promotions"
	case "CATEGORY_SOCIAL":
		return "Social"
	case "CATEGORY_FORUMS":
		return "Forums"
	}
	return strings.ReplaceAll(strings.TrimPrefix(normalized, "CATEGORY_"), "_", " ")
}

func compactWhitespace(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func formatPreviewTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
