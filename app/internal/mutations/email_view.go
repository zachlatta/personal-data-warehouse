package mutations

import (
	"regexp"
	"strings"
)

var (
	gmailSignatureScriptPattern                    = regexp.MustCompile(`(?is)<script\b[^>]*>.*?</script\s*>`)
	gmailSignatureEventAttrPattern                 = regexp.MustCompile(`(?is)\s+on[a-z0-9_-]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)`)
	gmailSignatureDoubleQuotedJavascriptURLPattern = regexp.MustCompile(`(?is)(\s+(?:href|src)\s*=\s*)"\s*javascript:[^"]*"`)
	gmailSignatureSingleQuotedJavascriptURLPattern = regexp.MustCompile(`(?is)(\s+(?:href|src)\s*=\s*)'\s*javascript:[^']*'`)
	gmailSignatureUnquotedJavascriptURLPattern     = regexp.MustCompile(`(?is)(\s+(?:href|src)\s*=\s*)javascript:[^\s>]+`)
)

// gmailEmailView is the reviewer's model of a gmail.send_email mutation,
// computed once on the server so every client (the web SPA, the iOS app)
// edits the same thing: the delivery mode, every proposed variant with the
// selected one marked, each body split into the editable part, the
// signature and the quoted thread, and the thread being replied to. It is
// attached to the mutation JSON as "email".
func gmailEmailView(mutation Mutation) map[string]any {
	email := gmailEmailPreview(mutation)
	deliveryMode := gmailEmailDeliveryMode(mutation, email)
	variants := gmailEmailVariants(mutation, email)
	out := make([]map[string]any, 0, len(variants))
	for _, variant := range variants {
		item := gmailEmailMessageView(variant.Message)
		item["id"] = variant.ID
		item["title"] = variant.Title
		item["selected"] = variant.Selected
		out = append(out, item)
	}
	threads := gmailEmailReplyThreads(mutation)
	threadViews := make([]map[string]any, 0, len(threads))
	for _, thread := range threads {
		threadViews = append(threadViews, gmailThreadView(thread))
	}
	return map[string]any{
		"delivery_mode": deliveryMode,
		"message":       gmailEmailMessageView(email),
		"variants":      out,
		"reply_threads": threadViews,
		"has_variants":  len(variants) > 1,
	}
}

// gmailEmailMessageView splits one email body the way the editor needs it:
// editor_html is what the reviewer types in, signature_html is shown below it
// read-only, quoted_html is the collapsed quoted thread, body_html is the
// whole thing as it will be sent.
func gmailEmailMessageView(email map[string]any) map[string]any {
	bodyHTML := strings.TrimSpace(stringFromAny(email["body_html"]))
	bodyText := stringFromAny(email["body_text"])
	if bodyHTML == "" {
		bodyHTML = emailPlainTextToHTML(bodyText)
	}
	fullBodyHTML := bodyHTML
	bodyHTML, quotedHTML := splitGmailQuotedHTML(bodyHTML)
	editorHTML, signatureHTML := splitEmailBodyAndSignatureHTML(bodyHTML)
	return map[string]any{
		"to":                 stringSliceFromAny(email["to"]),
		"cc":                 stringSliceFromAny(email["cc"]),
		"bcc":                stringSliceFromAny(email["bcc"]),
		"subject":            stringFromAny(email["subject"]),
		"body_text":          bodyText,
		"body_html":          fullBodyHTML,
		"editor_html":        editorHTML,
		"signature_html":     sanitizeGmailSignaturePreviewHTML(signatureHTML),
		"quoted_html":        quotedHTML,
		"quoted_text":        strings.TrimSpace(htmlFragmentText(quotedHTML)),
		"reply_to_thread_id": stringFromAny(email["reply_to_thread_id"]),
		"in_reply_to":        stringFromAny(email["in_reply_to"]),
		"references":         stringSliceFromAny(email["references"]),
	}
}

// gmailThreadView is a thread preview with each message's quoted part split
// off, so a client can collapse it without knowing Gmail's quote markup.
func gmailThreadView(thread map[string]any) map[string]any {
	out := cloneMap(thread)
	messages := mapSliceFromAny(thread["messages"])
	views := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		view := cloneMap(message)
		bodyHTML := strings.TrimSpace(stringFromAny(message["body_html"]))
		if bodyHTML != "" {
			body, quoted := splitGmailQuotedHTML(bodyHTML)
			view["body_html"] = body
			view["quoted_html"] = quoted
		}
		views = append(views, view)
	}
	out["messages"] = views
	return out
}

func gmailEmailPreview(mutation Mutation) map[string]any {
	email := mapFromAny(mapFromAny(mutation.Payload)["message"])
	preview := mapFromAny(mutation.Preview["email"])
	for key, value := range preview {
		email[key] = value
	}
	return email
}

func gmailEmailDeliveryMode(mutation Mutation, email map[string]any) string {
	payload := mapFromAny(mutation.Payload)
	mode := strings.TrimSpace(stringFromAny(payload["delivery_mode"]))
	if mode == "" {
		mode = strings.TrimSpace(stringFromAny(email["delivery_mode"]))
	}
	if mode == "draft" {
		return "draft"
	}
	return "send"
}

type gmailEmailVariant struct {
	ID       string
	Title    string
	Message  map[string]any
	Selected bool
}

func gmailEmailVariants(mutation Mutation, fallbackEmail map[string]any) []gmailEmailVariant {
	payload := mapFromAny(mutation.Payload)
	rawVariants := normalizeStoredEmailVariants(payload["variants"])
	if len(rawVariants) == 0 {
		return []gmailEmailVariant{{
			Message:  cloneMap(fallbackEmail),
			Selected: true,
		}}
	}
	selectedID := strings.TrimSpace(stringFromAny(payload["selected_variant_id"]))
	if selectedID == "" {
		selectedID = strings.TrimSpace(stringFromAny(mapFromAny(mutation.Preview["email"])["selected_variant_id"]))
	}
	out := make([]gmailEmailVariant, 0, len(rawVariants))
	selectedFound := false
	for index, raw := range rawVariants {
		id := strings.TrimSpace(stringFromAny(raw["id"]))
		if id == "" {
			id = emailVariantID(index)
		}
		selected := id == selectedID
		if selected {
			selectedFound = true
		}
		out = append(out, gmailEmailVariant{
			ID:       id,
			Title:    strings.TrimSpace(stringFromAny(raw["title"])),
			Message:  mapFromAny(raw["message"]),
			Selected: selected,
		})
	}
	if !selectedFound && len(out) > 0 {
		out[0].Selected = true
	}
	return out
}

func gmailEmailReplyThreads(mutation Mutation) []map[string]any {
	threadIDs := gmailEmailReplyThreadIDs(mutation)
	if len(threadIDs) == 0 {
		return nil
	}
	existingThreads := mapSliceFromAny(mutation.Preview["reply_threads"])
	existingByID := map[string]map[string]any{}
	for _, thread := range existingThreads {
		threadID := strings.TrimSpace(stringFromAny(thread["thread_id"]))
		if threadID != "" {
			existingByID[threadID] = thread
		}
	}

	threads := make([]map[string]any, 0, len(threadIDs))
	for _, threadID := range threadIDs {
		thread := cloneMap(existingByID[threadID])
		if len(thread) == 0 {
			thread["thread_id"] = threadID
		}
		threads = append(threads, thread)
	}
	return threads
}

func sanitizeGmailSignaturePreviewHTML(value string) string {
	value = gmailSignatureScriptPattern.ReplaceAllString(value, "")
	value = gmailSignatureEventAttrPattern.ReplaceAllString(value, "")
	value = gmailSignatureDoubleQuotedJavascriptURLPattern.ReplaceAllString(value, `${1}"#"`)
	value = gmailSignatureSingleQuotedJavascriptURLPattern.ReplaceAllString(value, `${1}'#'`)
	value = gmailSignatureUnquotedJavascriptURLPattern.ReplaceAllString(value, `${1}#`)
	return value
}

func splitEmailBodyAndSignatureHTML(bodyHTML string) (string, string) {
	signatureHTML := extractGmailSignatureHTML(bodyHTML)
	if signatureHTML == "" {
		return bodyHTML, ""
	}
	index := strings.Index(bodyHTML, signatureHTML)
	if index < 0 {
		return bodyHTML, ""
	}
	body := strings.TrimSpace(bodyHTML[:index])
	body = trimTrailingSignatureSeparatorHTML(body)
	if body == "" {
		body = "<div><br></div>"
	}
	return body, signatureHTML
}

func trimTrailingSignatureSeparatorHTML(bodyHTML string) string {
	bodyHTML = strings.TrimSpace(bodyHTML)
	for {
		trimmed := strings.TrimSpace(bodyHTML)
		next := strings.TrimSpace(strings.TrimSuffix(trimmed, "<div><br></div>"))
		next = strings.TrimSpace(strings.TrimSuffix(next, "<div><br/></div>"))
		next = strings.TrimSpace(strings.TrimSuffix(next, "<br>"))
		next = strings.TrimSpace(strings.TrimSuffix(next, "<br/>"))
		next = strings.TrimSpace(strings.TrimSuffix(next, "<br />"))
		if next == trimmed {
			return trimmed
		}
		bodyHTML = next
	}
}

func splitGmailQuotedHTML(bodyHTML string) (string, string) {
	bodyHTML = strings.TrimSpace(bodyHTML)
	for _, pattern := range []string{
		`<div class="gmail_quote gmail_quote_container"`,
		`<div class="gmail_quote"`,
		`<blockquote class="gmail_quote"`,
	} {
		if index := strings.Index(bodyHTML, pattern); index > 0 {
			return strings.TrimSpace(bodyHTML[:index]), strings.TrimSpace(bodyHTML[index:])
		}
	}
	return bodyHTML, ""
}

// gmailEmailUpdateInputFromJSON normalizes the JSON body of the update-email
// endpoint into the store's input: recipient lists accept either an array or
// a comma/newline separated string, exactly what the old form field carried.
func gmailEmailUpdateInputFromJSON(body apiUpdateEmailBody) UpdateGmailEmailMutationInput {
	source := body.Message
	if source == nil {
		source = map[string]any{}
	}
	message := map[string]any{
		"to":        addressListFromAny(source["to"]),
		"cc":        addressListFromAny(source["cc"]),
		"bcc":       addressListFromAny(source["bcc"]),
		"subject":   strings.TrimSpace(stringFromAny(source["subject"])),
		"body_text": stringFromAny(source["body_text"]),
		"body_html": stringFromAny(source["body_html"]),
	}
	if replyToThreadID := strings.TrimSpace(stringFromAny(source["reply_to_thread_id"])); replyToThreadID != "" {
		message["reply_to_thread_id"] = replyToThreadID
	}
	if inReplyTo := strings.TrimSpace(stringFromAny(source["in_reply_to"])); inReplyTo != "" {
		message["in_reply_to"] = inReplyTo
	}
	if references := addressListFromAny(source["references"]); len(references) > 0 {
		message["references"] = references
	}
	return UpdateGmailEmailMutationInput{
		DeliveryMode:      strings.TrimSpace(body.DeliveryMode),
		Message:           message,
		SelectedVariantID: strings.TrimSpace(body.SelectedVariantID),
	}
}

func addressListFromAny(value any) []string {
	if text, ok := value.(string); ok {
		return splitEmailAddressList(text)
	}
	return normalizeStringSlice(stringSliceFromAny(value))
}

func splitEmailAddressList(value string) []string {
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r'
	})
	return normalizeStringSlice(fields)
}
