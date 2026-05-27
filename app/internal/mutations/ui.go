package mutations

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const sessionCookieName = "pdw_mutation_ui_session"

var (
	gmailSignatureScriptPattern                    = regexp.MustCompile(`(?is)<script\b[^>]*>.*?</script\s*>`)
	gmailSignatureEventAttrPattern                 = regexp.MustCompile(`(?is)\s+on[a-z0-9_-]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)`)
	gmailSignatureDoubleQuotedJavascriptURLPattern = regexp.MustCompile(`(?is)(\s+(?:href|src)\s*=\s*)"\s*javascript:[^"]*"`)
	gmailSignatureSingleQuotedJavascriptURLPattern = regexp.MustCompile(`(?is)(\s+(?:href|src)\s*=\s*)'\s*javascript:[^']*'`)
	gmailSignatureUnquotedJavascriptURLPattern     = regexp.MustCompile(`(?is)(\s+(?:href|src)\s*=\s*)javascript:[^\s>]+`)
)

type sessionPayload struct {
	Subject string `json:"sub"`
	Issued  int64  `json:"iat"`
	Expires int64  `json:"exp"`
	CSRF    string `json:"csrf"`
}

func (s *Service) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(ReviewPath, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, ReviewPath+"/requests", http.StatusSeeOther)
	})
	mux.HandleFunc(ReviewPath+"/", s.reviewHandler)
	return mux
}

func (s *Service) reviewHandler(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.store == nil {
		http.Error(w, "mutation review is not configured", http.StatusServiceUnavailable)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, ReviewPath)
	if path == "" || path == "/" {
		http.Redirect(w, r, ReviewPath+"/requests", http.StatusSeeOther)
		return
	}
	if path == "/login" {
		if r.Method == http.MethodGet {
			s.renderLogin(w, r, "")
			return
		}
		if r.Method == http.MethodPost {
			s.handleLogin(w, r)
			return
		}
		methodNotAllowed(w)
		return
	}

	session, ok := s.requireSession(w, r)
	if !ok {
		return
	}
	if path == "/logout" && r.Method == http.MethodPost {
		if !s.validCSRF(r, session) {
			http.Error(w, "invalid csrf token", http.StatusForbidden)
			return
		}
		s.clearSession(w, r)
		http.Redirect(w, r, ReviewPath+"/login", http.StatusSeeOther)
		return
	}
	if path == "/requests" && r.Method == http.MethodGet {
		s.renderRequestList(w, r, session)
		return
	}
	if strings.HasPrefix(path, "/requests/") {
		s.handleRequestRoute(w, r, session, strings.TrimPrefix(path, "/requests/"))
		return
	}
	http.NotFound(w, r)
}

func (s *Service) handleRequestRoute(w http.ResponseWriter, r *http.Request, session sessionPayload, suffix string) {
	parts := strings.Split(strings.Trim(suffix, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	requestID, err := url.PathUnescape(parts[0])
	if err != nil || requestID == "" {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		s.renderRequestDetail(w, r, session, requestID, "")
		return
	}
	if len(parts) == 4 && parts[1] == "mutations" && parts[3] == "update-email" && r.Method == http.MethodPost {
		if !s.validCSRF(r, session) {
			http.Error(w, "invalid csrf token", http.StatusForbidden)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, "invalid form", http.StatusBadRequest)
			return
		}
		mutationID, err := url.PathUnescape(parts[2])
		if err != nil || mutationID == "" {
			http.NotFound(w, r)
			return
		}
		if _, err := s.store.UpdateGmailEmailMutation(r.Context(), requestID, mutationID, gmailEmailUpdateInputFromForm(r), reviewerActorID); err != nil {
			s.renderRequestDetail(w, r, session, requestID, err.Error())
			return
		}
		http.Redirect(w, r, ReviewPath+"/requests/"+url.PathEscape(requestID), http.StatusSeeOther)
		return
	}
	if len(parts) == 4 && parts[1] == "mutations" && parts[3] == "remove" && r.Method == http.MethodPost {
		if !s.validCSRF(r, session) {
			http.Error(w, "invalid csrf token", http.StatusForbidden)
			return
		}
		mutationID, err := url.PathUnescape(parts[2])
		if err != nil || mutationID == "" {
			http.NotFound(w, r)
			return
		}
		if _, err := s.store.RemoveMutation(r.Context(), requestID, mutationID, reviewerActorID); err != nil {
			s.renderRequestDetail(w, r, session, requestID, err.Error())
			return
		}
		http.Redirect(w, r, ReviewPath+"/requests/"+url.PathEscape(requestID), http.StatusSeeOther)
		return
	}
	if len(parts) == 2 && r.Method == http.MethodPost {
		if !s.validCSRF(r, session) {
			http.Error(w, "invalid csrf token", http.StatusForbidden)
			return
		}
		switch parts[1] {
		case "approve":
			if _, err := s.store.ApproveRequest(r.Context(), requestID, reviewerActorID); err != nil {
				s.renderRequestDetail(w, r, session, requestID, err.Error())
				return
			}
			http.Redirect(w, r, ReviewPath+"/requests/"+url.PathEscape(requestID), http.StatusSeeOther)
			return
		case "reject":
			reason := strings.TrimSpace(r.FormValue("reason"))
			if _, err := s.store.RejectRequest(r.Context(), requestID, reviewerActorID, reason); err != nil {
				s.renderRequestDetail(w, r, session, requestID, err.Error())
				return
			}
			http.Redirect(w, r, ReviewPath+"/requests/"+url.PathEscape(requestID), http.StatusSeeOther)
			return
		}
	}
	http.NotFound(w, r)
}

func (s *Service) renderLogin(w http.ResponseWriter, r *http.Request, message string) {
	if s.cfg.UIPassword == "" {
		http.Error(w, "mutation review password is not configured", http.StatusServiceUnavailable)
		return
	}
	next := safeNextPath(r.URL.Query().Get("next"))
	if next == "" {
		next = ReviewPath + "/requests"
	}
	writeHTMLHeader(w, "Mutation Review Login")
	fmt.Fprintf(w, `<main class="login"><h1>Mutation Review</h1>`)
	if message != "" {
		fmt.Fprintf(w, `<p class="error">%s</p>`, html.EscapeString(message))
	}
	fmt.Fprintf(w, `<form method="post" action="%s/login">`, ReviewPath)
	fmt.Fprintf(w, `<input type="hidden" name="next" value="%s">`, html.EscapeString(next))
	fmt.Fprintf(w, `<label>Password <input type="password" name="password" autofocus></label>`)
	fmt.Fprintf(w, `<button type="submit">Log In</button></form></main>`)
	writeHTMLFooter(w)
}

func (s *Service) handleLogin(w http.ResponseWriter, r *http.Request) {
	if s.cfg.UIPassword == "" {
		http.Error(w, "mutation review password is not configured", http.StatusServiceUnavailable)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	password := r.FormValue("password")
	if subtle.ConstantTimeCompare([]byte(password), []byte(s.cfg.UIPassword)) != 1 {
		s.renderLogin(w, r, "Invalid password")
		return
	}
	session, err := s.newSession()
	if err != nil {
		http.Error(w, "could not create session", http.StatusInternalServerError)
		return
	}
	s.setSessionCookie(w, r, session)
	next := safeNextPath(r.FormValue("next"))
	if next == "" {
		next = ReviewPath + "/requests"
	}
	http.Redirect(w, r, next, http.StatusSeeOther)
}

func (s *Service) renderRequestList(w http.ResponseWriter, r *http.Request, session sessionPayload) {
	requests, err := s.store.ListRequests(r.Context(), RequestFilter{Limit: 200})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	pending, past := splitRequestsForList(requests)
	writeHTMLHeader(w, "Mutation Requests")
	fmt.Fprintf(w, `<main><header><h1>Mutation Requests</h1><form method="post" action="%s/logout"><input type="hidden" name="csrf_token" value="%s"><button type="submit">Log Out</button></form></header>`, ReviewPath, html.EscapeString(session.CSRF))
	renderRequestTable(w, "Pending Review", pending, "No requests are waiting for review.")
	renderRequestTable(w, "Past Requests", past, "No approved or denied requests yet.")
	fmt.Fprint(w, `</main>`)
	writeHTMLFooter(w)
}

func splitRequestsForList(requests []Request) ([]Request, []Request) {
	pending := []Request{}
	past := []Request{}
	for _, request := range requests {
		if request.Status == "pending_review" {
			pending = append(pending, request)
			continue
		}
		past = append(past, request)
	}
	return pending, past
}

func renderRequestTable(w http.ResponseWriter, title string, requests []Request, emptyText string) {
	fmt.Fprintf(w, `<section class="request-list"><h2>%s</h2>`, html.EscapeString(title))
	if len(requests) == 0 {
		fmt.Fprintf(w, `<p class="empty">%s</p></section>`, html.EscapeString(emptyText))
		return
	}
	fmt.Fprint(w, `<table><thead><tr><th>Status</th><th>Request</th><th>Mutations</th><th>Created</th></tr></thead><tbody>`)
	for _, request := range requests {
		count := request.MutationCount
		if count == 0 {
			count = len(request.Mutations)
		}
		fmt.Fprintf(w, `<tr><td>%s</td><td><a href="%s/requests/%s">%s</a><div class="reason">%s</div></td><td>%d</td><td>%s</td></tr>`,
			html.EscapeString(displayRequestStatus(request.Status)),
			ReviewPath,
			url.PathEscape(request.ID),
			html.EscapeString(request.Title),
			html.EscapeString(request.Reason),
			count,
			html.EscapeString(formatTime(request.CreatedAt)),
		)
	}
	fmt.Fprint(w, `</tbody></table></section>`)
}

func displayRequestStatus(status string) string {
	if status == "rejected" {
		return "denied"
	}
	return status
}

func (s *Service) renderRequestDetail(w http.ResponseWriter, r *http.Request, session sessionPayload, requestID string, message string) {
	request, err := s.store.GetRequest(r.Context(), requestID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeHTMLHeader(w, request.Title)
	fmt.Fprintf(w, `<main><p><a href="%s/requests">Requests</a></p>`, ReviewPath)
	if message != "" {
		fmt.Fprintf(w, `<p class="error">%s</p>`, html.EscapeString(message))
	}
	fmt.Fprintf(w, `<section><h1>%s</h1><p class="status">%s</p><p>%s</p>`, html.EscapeString(request.Title), html.EscapeString(request.Status), html.EscapeString(request.Reason))
	if request.Error != "" {
		fmt.Fprintf(w, `<p class="error">%s</p>`, html.EscapeString(request.Error))
	}
	if request.Status == "pending_review" {
		fmt.Fprintf(w, `<form class="actions" method="post" action="%s/requests/%s/approve"><input type="hidden" name="csrf_token" value="%s"><button type="submit">Approve</button></form>`, ReviewPath, url.PathEscape(request.ID), html.EscapeString(session.CSRF))
		fmt.Fprintf(w, `<form class="actions" method="post" action="%s/requests/%s/reject"><input type="hidden" name="csrf_token" value="%s"><input name="reason" placeholder="Reason"><button type="submit">Deny</button></form>`, ReviewPath, url.PathEscape(request.ID), html.EscapeString(session.CSRF))
	}
	fmt.Fprint(w, `</section>`)
	renderRequestContext(w, request.Context)
	fmt.Fprint(w, `<section><h2>Mutations</h2>`)
	renderMutationList(w, request.Mutations, request.ID, request.Status, session.CSRF)
	fmt.Fprint(w, `</section></main>`)
	writeHTMLFooter(w)
}

type gmailMutationGroup struct {
	Operation string
	Account   string
	Status    string
	Mutations []Mutation
}

type contactMutationGroup struct {
	Account   string
	Status    string
	Mutations []Mutation
}

type mutationRenderItem struct {
	GroupKind string
	GroupKey  string
	Mutation  *Mutation
}

func renderMutationList(w http.ResponseWriter, mutations []Mutation, requestID string, requestStatus string, csrf string) {
	gmailGroups := map[string]*gmailMutationGroup{}
	contactGroups := map[string]*contactMutationGroup{}
	items := []mutationRenderItem{}
	for index := range mutations {
		mutation := mutations[index]
		if isGmailThreadMutation(mutation) {
			key := mutation.Operation + "\x00" + mutation.Account
			group := gmailGroups[key]
			if group == nil {
				group = &gmailMutationGroup{
					Operation: mutation.Operation,
					Account:   mutation.Account,
					Status:    mutation.Status,
				}
				gmailGroups[key] = group
				items = append(items, mutationRenderItem{GroupKind: "gmail", GroupKey: key})
			}
			if group.Status != mutation.Status {
				group.Status = "mixed"
			}
			group.Mutations = append(group.Mutations, mutation)
			continue
		}
		if isContactMutation(mutation) {
			key := mutation.Account
			group := contactGroups[key]
			if group == nil {
				group = &contactMutationGroup{Account: mutation.Account, Status: mutation.Status}
				contactGroups[key] = group
				items = append(items, mutationRenderItem{GroupKind: "contact", GroupKey: key})
			}
			if group.Status != mutation.Status {
				group.Status = "mixed"
			}
			group.Mutations = append(group.Mutations, mutation)
			continue
		}
		items = append(items, mutationRenderItem{Mutation: &mutation})
	}
	for _, item := range items {
		switch item.GroupKind {
		case "gmail":
			renderGmailMutationGroup(w, gmailGroups[item.GroupKey])
			continue
		case "contact":
			renderContactMutationGroup(w, contactGroups[item.GroupKey])
			continue
		}
		if item.Mutation != nil {
			renderMutationArticle(w, *item.Mutation, requestID, requestStatus, csrf)
		}
	}
}

func renderMutationArticle(w http.ResponseWriter, mutation Mutation, requestID string, requestStatus string, csrf string) {
	if isGmailEmailMutation(mutation) {
		renderGmailEmailMutation(w, mutation, requestID, requestStatus, csrf)
		return
	}
	if isCalendarMutation(mutation) {
		renderCalendarMutation(w, mutation)
		return
	}
	fmt.Fprintf(w, `<article class="mutation"><h3>%s</h3><p class="mutation-meta">%s %s for %s</p><pre>%s</pre><pre>%s</pre></article>`,
		html.EscapeString(mutation.Title),
		html.EscapeString(mutation.Status),
		html.EscapeString(mutation.Operation),
		html.EscapeString(mutation.Account),
		html.EscapeString(prettyJSON(mutation.Payload)),
		html.EscapeString(prettyJSON(mutation.Preview)),
	)
}

func isCalendarMutation(mutation Mutation) bool {
	if mutation.Provider == CalendarProvider {
		return true
	}
	switch mutation.Operation {
	case CalendarCreateEventOperation, CalendarUpdateEventOperation, CalendarDeleteEventOperation:
		return true
	}
	return false
}

func isContactMutation(mutation Mutation) bool {
	return mutation.Provider == "google_people" || mutation.Operation == ContactsBatchMutationOperation || mutation.Operation == GooglePeopleContactsOperation
}

func isGmailEmailMutation(mutation Mutation) bool {
	return mutation.Provider == "gmail" && mutation.Operation == GmailSendEmailOperation
}

func renderGmailEmailMutation(w http.ResponseWriter, mutation Mutation, requestID string, requestStatus string, csrf string) {
	email := gmailEmailPreview(mutation)
	deliveryMode := gmailEmailDeliveryMode(mutation, email)
	variants := gmailEmailVariants(mutation, email)
	pending := requestStatus == "pending_review" && mutation.Status == "pending_review"
	replyThreads := gmailEmailReplyThreads(mutation)
	fmt.Fprintf(w, `<article class="mutation gmail-email-mutation"><div class="mutation-head"><div><p class="eyebrow">Gmail Email</p><h3>%s</h3></div><span class="pill">%s</span></div>`,
		html.EscapeString(gmailEmailTitle(deliveryMode)),
		html.EscapeString(mutation.Status),
	)
	fmt.Fprintf(w, `<p class="mutation-meta">%s for %s</p>`, html.EscapeString(mutation.Operation), html.EscapeString(mutation.Account))
	fmt.Fprintf(w, `<p class="mutation-reason">%s</p>`, html.EscapeString(gmailEmailActionText(deliveryMode)))
	if pending {
		if len(replyThreads) > 0 {
			renderGmailEmailReplyContext(w, replyThreads, func(w http.ResponseWriter) {
				renderGmailInlineReplyComposer(w, mutation, requestID, variants, deliveryMode, csrf)
			})
		} else {
			renderGmailEmailEditForms(w, mutation, requestID, variants, deliveryMode, csrf)
		}
		renderGmailEmailRemoveForm(w, mutation, requestID, csrf)
		writeTiptapScript(w)
	} else {
		renderGmailEmailReplyContext(w, replyThreads, nil)
		renderGmailEmailReadOnly(w, email, deliveryMode)
	}
	fmt.Fprint(w, `</article>`)
}

func renderGmailEmailRemoveForm(w http.ResponseWriter, mutation Mutation, requestID string, csrf string) {
	action := ReviewPath + "/requests/" + url.PathEscape(requestID) + "/mutations/" + url.PathEscape(mutation.ID) + "/remove"
	fmt.Fprintf(w, `<form class="gmail-email-remove" method="post" action="%s" onsubmit="return confirm('Mark this email as do-not-send? It will be skipped when the request is approved.');">`, action)
	fmt.Fprintf(w, `<input type="hidden" name="csrf_token" value="%s">`, html.EscapeString(csrf))
	fmt.Fprint(w, `<button type="submit" class="danger">Don't send</button>`)
	fmt.Fprint(w, `</form>`)
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

func gmailEmailTitle(deliveryMode string) string {
	if deliveryMode == "draft" {
		return "Create Gmail draft"
	}
	return "Send Gmail email"
}

func gmailEmailActionText(deliveryMode string) string {
	if deliveryMode == "draft" {
		return "Creates a Gmail draft after approval."
	}
	return "Will send this email after approval."
}

func renderGmailEmailReplyContext(w http.ResponseWriter, threads []map[string]any, renderComposer func(http.ResponseWriter)) {
	if len(threads) == 0 {
		return
	}
	fmt.Fprint(w, `<div class="gmail-email-reply-context"><div class="gmail-email-reply-context-head">Replying in thread</div><div class="gmail-thread-list">`)
	for index, thread := range threads {
		if index == 0 && renderComposer != nil {
			renderGmailThreadWithOptions(w, thread, gmailThreadRenderOptions{
				Open:                true,
				RenderAfterMessages: renderComposer,
			})
			continue
		}
		renderGmailThread(w, thread)
	}
	fmt.Fprint(w, `</div></div>`)
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

func renderGmailInlineReplyComposer(w http.ResponseWriter, mutation Mutation, requestID string, variants []gmailEmailVariant, deliveryMode string, csrf string) {
	fmt.Fprint(w, `<div class="gmail-inline-reply">`)
	fmt.Fprintf(w, `<div class="gmail-avatar" aria-hidden="true">%s</div>`, html.EscapeString(senderInitial(mutation.Account)))
	fmt.Fprint(w, `<div class="gmail-inline-reply-main">`)
	renderGmailEmailEditForms(w, mutation, requestID, variants, deliveryMode, csrf)
	fmt.Fprint(w, `</div></div>`)
}

func renderGmailEmailEditForms(w http.ResponseWriter, mutation Mutation, requestID string, variants []gmailEmailVariant, deliveryMode string, csrf string) {
	if len(variants) > 1 {
		fmt.Fprint(w, `<div class="gmail-email-tabs" data-email-variant-tabs>`)
		for index, variant := range variants {
			activeClass := ""
			selectedAttr := "false"
			if variant.Selected {
				activeClass = " active"
				selectedAttr = "true"
			}
			fmt.Fprintf(w, `<button type="button" class="gmail-email-tab%s" data-email-variant-tab="%d" aria-selected="%s">%s</button>`, activeClass, index, selectedAttr, html.EscapeString(variant.Title))
		}
		fmt.Fprint(w, `</div>`)
	}
	for index, variant := range variants {
		hiddenAttr := ""
		if len(variants) > 1 && !variant.Selected {
			hiddenAttr = " hidden"
		}
		fmt.Fprintf(w, `<div class="gmail-email-variant-panel" data-email-variant-panel="%d"%s>`, index, hiddenAttr)
		renderGmailEmailEditForm(w, mutation, requestID, variant, deliveryMode, csrf, len(variants) > 1)
		fmt.Fprint(w, `</div>`)
	}
}

func renderGmailEmailEditForm(w http.ResponseWriter, mutation Mutation, requestID string, variant gmailEmailVariant, deliveryMode string, csrf string, hasVariants bool) {
	action := ReviewPath + "/requests/" + url.PathEscape(requestID) + "/mutations/" + url.PathEscape(mutation.ID) + "/update-email"
	email := variant.Message
	bodyHTML := strings.TrimSpace(stringFromAny(email["body_html"]))
	bodyText := stringFromAny(email["body_text"])
	if bodyHTML == "" {
		bodyHTML = emailPlainTextToHTML(bodyText)
	}
	fullBodyHTML := bodyHTML
	bodyHTML, quotedHTML := splitGmailQuotedHTML(bodyHTML)
	editorBodyHTML, signatureHTML := splitEmailBodyAndSignatureHTML(bodyHTML)
	quotedText := strings.TrimSpace(htmlFragmentText(quotedHTML))
	fmt.Fprintf(w, `<form class="gmail-email-form" method="post" action="%s">`, action)
	fmt.Fprintf(w, `<input type="hidden" name="csrf_token" value="%s">`, html.EscapeString(csrf))
	if variant.ID != "" {
		fmt.Fprintf(w, `<input type="hidden" name="selected_variant_id" value="%s">`, html.EscapeString(variant.ID))
	}
	fmt.Fprint(w, `<div class="gmail-email-delivery" role="group" aria-label="Delivery mode">`)
	renderDeliveryModeOption(w, "send", "Send email", deliveryMode == "send")
	renderDeliveryModeOption(w, "draft", "Create draft", deliveryMode == "draft")
	fmt.Fprint(w, `</div>`)
	if hasVariants {
		fmt.Fprintf(w, `<p class="gmail-email-variant-copy">Selected proposal: <strong>%s</strong></p>`, html.EscapeString(variant.Title))
	}
	fmt.Fprintf(w, `<div class="gmail-email-fields"><label>To<input name="to" value="%s"></label>`, html.EscapeString(strings.Join(stringSliceFromAny(email["to"]), ", ")))
	fmt.Fprintf(w, `<label>Cc<input name="cc" value="%s"></label>`, html.EscapeString(strings.Join(stringSliceFromAny(email["cc"]), ", ")))
	fmt.Fprintf(w, `<label>Bcc<input name="bcc" value="%s"></label>`, html.EscapeString(strings.Join(stringSliceFromAny(email["bcc"]), ", ")))
	fmt.Fprintf(w, `<label>Subject<input name="subject" value="%s"></label></div>`, html.EscapeString(stringFromAny(email["subject"])))
	fmt.Fprintf(w, `<input type="hidden" name="reply_to_thread_id" value="%s">`, html.EscapeString(stringFromAny(email["reply_to_thread_id"])))
	fmt.Fprintf(w, `<input type="hidden" name="in_reply_to" value="%s">`, html.EscapeString(stringFromAny(email["in_reply_to"])))
	fmt.Fprintf(w, `<input type="hidden" name="references" value="%s">`, html.EscapeString(strings.Join(stringSliceFromAny(email["references"]), "\n")))
	fmt.Fprintf(w, `<input type="hidden" name="body_text" value="%s">`, html.EscapeString(bodyText))
	fmt.Fprintf(w, `<input type="hidden" name="body_html" value="%s">`, html.EscapeString(fullBodyHTML))
	if quotedHTML != "" {
		fmt.Fprintf(w, `<input type="hidden" name="quoted_html" value="%s" data-quote-text="%s">`, html.EscapeString(quotedHTML), html.EscapeString(quotedText))
	}
	fmt.Fprint(w, `<div class="gmail-email-editor-wrap"><div class="gmail-email-toolbar" aria-label="Formatting toolbar"><button type="button" data-command="bold" aria-label="Bold"><strong>B</strong></button><button type="button" data-command="italic" aria-label="Italic"><em>I</em></button><button type="button" data-command="bulletList" aria-label="Bulleted list">Bullets</button><button type="button" data-command="orderedList" aria-label="Numbered list">1.</button></div>`)
	fmt.Fprintf(w, `<div class="gmail-email-editor" data-tiptap-editor data-initial-html="%s"></div>`, html.EscapeString(editorBodyHTML))
	if signatureHTML != "" {
		fmt.Fprintf(w, `<div class="gmail-email-signature-preview" data-gmail-signature-preview>%s</div>`, sanitizeGmailSignaturePreviewHTML(signatureHTML))
	}
	if quotedHTML != "" {
		fmt.Fprintf(w, `<details class="gmail-email-quote-preview"><summary>Quoted thread</summary><iframe class="gmail-body-frame quoted" style="height:%dpx" sandbox referrerpolicy="no-referrer" srcdoc="%s"></iframe></details>`, gmailBodyFrameHeight(quotedHTML, true), html.EscapeString(emailHTMLDocument(quotedHTML)))
	}
	fmt.Fprint(w, `</div>`)
	primaryLabel := "Save email changes"
	if hasVariants {
		primaryLabel = "Use this version"
	}
	fmt.Fprintf(w, `<div class="gmail-email-actions"><button type="submit">%s</button><button type="submit" name="set_delivery_mode" value="draft">Save as draft instead</button></div>`, html.EscapeString(primaryLabel))
	fmt.Fprint(w, `</form>`)
}

func sanitizeGmailSignaturePreviewHTML(value string) string {
	value = gmailSignatureScriptPattern.ReplaceAllString(value, "")
	value = gmailSignatureEventAttrPattern.ReplaceAllString(value, "")
	value = gmailSignatureDoubleQuotedJavascriptURLPattern.ReplaceAllString(value, `${1}"#"`)
	value = gmailSignatureSingleQuotedJavascriptURLPattern.ReplaceAllString(value, `${1}'#'`)
	value = gmailSignatureUnquotedJavascriptURLPattern.ReplaceAllString(value, `${1}#`)
	return value
}

func renderDeliveryModeOption(w http.ResponseWriter, value string, label string, checked bool) {
	checkedAttr := ""
	if checked {
		checkedAttr = " checked"
	}
	fmt.Fprintf(w, `<label class="delivery-option"><input type="radio" name="delivery_mode" value="%s"%s><span>%s</span></label>`, html.EscapeString(value), checkedAttr, html.EscapeString(label))
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

func renderGmailEmailReadOnly(w http.ResponseWriter, email map[string]any, deliveryMode string) {
	fmt.Fprint(w, `<div class="gmail-email-readonly">`)
	fmt.Fprintf(w, `<dl><dt>Delivery</dt><dd>%s</dd><dt>To</dt><dd>%s</dd><dt>Cc</dt><dd>%s</dd><dt>Bcc</dt><dd>%s</dd><dt>Subject</dt><dd>%s</dd></dl>`,
		html.EscapeString(deliveryMode),
		html.EscapeString(strings.Join(stringSliceFromAny(email["to"]), ", ")),
		html.EscapeString(strings.Join(stringSliceFromAny(email["cc"]), ", ")),
		html.EscapeString(strings.Join(stringSliceFromAny(email["bcc"]), ", ")),
		html.EscapeString(stringFromAny(email["subject"])),
	)
	bodyHTML := strings.TrimSpace(stringFromAny(email["body_html"]))
	if bodyHTML == "" {
		bodyHTML = emailPlainTextToHTML(stringFromAny(email["body_text"]))
	}
	fmt.Fprintf(w, `<iframe class="gmail-email-body-frame" sandbox referrerpolicy="no-referrer" srcdoc="%s"></iframe>`, html.EscapeString(emailHTMLDocument(bodyHTML)))
	fmt.Fprint(w, `</div>`)
}

func writeTiptapScript(w http.ResponseWriter) {
	fmt.Fprint(w, `<script type="module">
import { Editor } from "https://esm.sh/@tiptap/core@3.23.6";
import StarterKit from "https://esm.sh/@tiptap/starter-kit@3.23.6";
import Link from "https://esm.sh/@tiptap/extension-link@3.23.6";
import { TextStyleKit } from "https://esm.sh/@tiptap/extension-text-style@3.23.6";
function syncEmailEditor(editor, form) {
  const htmlInput = form.querySelector('input[name="body_html"]');
  const textInput = form.querySelector('input[name="body_text"]');
  const signature = form.querySelector("[data-gmail-signature-preview]");
  const quoteInput = form.querySelector('input[name="quoted_html"]');
  const editorHTML = stripTrailingEmptyParagraphs(editor.getHTML());
  const signatureHTML = signature ? signature.innerHTML.trim() : "";
  const quotedHTML = quoteInput ? quoteInput.value.trim() : "";
  if (htmlInput) {
    htmlInput.value = [editorHTML, signatureHTML, quotedHTML].filter(Boolean).join("<div><br></div>");
  }
  if (textInput) {
    const bodyText = editor.getText({ blockSeparator: "\n\n" }).trimEnd();
    const signatureText = signature ? normalizeEmailText(signature.innerText) : "";
    const quotedText = quoteInput ? normalizeEmailText(quoteInput.dataset.quoteText) : "";
    textInput.value = [bodyText, signatureText, quotedText].filter(Boolean).join("\n\n") + "\n";
  }
}
function stripTrailingEmptyParagraphs(value) {
  const template = document.createElement("template");
  template.innerHTML = value || "";
  while (template.content.lastElementChild?.textContent.trim() === "" && !template.content.lastElementChild.querySelector("img")) {
    template.content.lastElementChild.remove();
  }
  return template.innerHTML.trim() || "<div><br></div>";
}
function normalizeEmailText(value) {
  return (value || "").replace(/\r\n/g, "\n").split("\n").map((line) => line.trimEnd()).filter((line, index, lines) => line.trim() !== "" || (index > 0 && index < lines.length - 1)).join("\n").trim();
}
function normalizeInitialEmailHTML(value) {
  const template = document.createElement("template");
  template.innerHTML = value || "<p></p>";
  template.content.querySelectorAll("font").forEach((font) => {
    const span = document.createElement("span");
    const styles = [];
    if (font.getAttribute("face")) styles.push("font-family: " + font.getAttribute("face"));
    if (font.getAttribute("color")) styles.push("color: " + font.getAttribute("color"));
    if (font.getAttribute("style")) styles.push(font.getAttribute("style"));
    if (styles.length) span.setAttribute("style", styles.join("; "));
    while (font.firstChild) span.appendChild(font.firstChild);
    font.replaceWith(span);
  });
  template.content.querySelectorAll("div").forEach((div) => {
    if (div.textContent.trim() !== "--") return;
    div.querySelectorAll("br").forEach((br) => br.remove());
  });
  template.content.querySelectorAll("div").forEach((div) => {
    if (div.classList.contains("gmail_signature")) return;
    if (div.textContent.trim() === "" && !div.querySelector("img")) div.remove();
  });
  template.content.querySelectorAll("br").forEach((br) => {
    const next = br.nextElementSibling;
    if (next?.classList?.contains("gmail_signature")) br.remove();
  });
  return template.innerHTML;
}
function initEmailEditor(root) {
  if (root.dataset.ready === "true") return;
  root.dataset.ready = "true";
  const form = root.closest("form");
  const toolbar = form.querySelector(".gmail-email-toolbar");
  let bodyDirty = false;
  const editor = new Editor({
    element: root,
    extensions: [StarterKit, TextStyleKit, Link.configure({ openOnClick: false })],
    content: normalizeInitialEmailHTML(root.dataset.initialHtml),
    editorProps: { attributes: { class: "gmail-email-editor-surface" } },
    onUpdate: ({ editor }) => {
      bodyDirty = true;
      syncEmailEditor(editor, form);
    }
  });
  toolbar?.querySelectorAll("[data-command]").forEach((button) => {
    button.addEventListener("click", () => {
      const command = button.dataset.command;
      if (command === "bold") editor.chain().focus().toggleBold().run();
      if (command === "italic") editor.chain().focus().toggleItalic().run();
      if (command === "bulletList") editor.chain().focus().toggleBulletList().run();
      if (command === "orderedList") editor.chain().focus().toggleOrderedList().run();
      bodyDirty = true;
      syncEmailEditor(editor, form);
    });
  });
  form.addEventListener("submit", () => {
    if (bodyDirty) syncEmailEditor(editor, form);
  });
}
function initEmailVariantTabs(root) {
  if (root.dataset.ready === "true") return;
  root.dataset.ready = "true";
  const article = root.closest(".gmail-email-mutation");
  const tabs = Array.from(root.querySelectorAll("[data-email-variant-tab]"));
  const panels = Array.from(article?.querySelectorAll("[data-email-variant-panel]") || []);
  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      const target = tab.dataset.emailVariantTab;
      tabs.forEach((candidate) => {
        const active = candidate === tab;
        candidate.classList.toggle("active", active);
        candidate.setAttribute("aria-selected", active ? "true" : "false");
      });
      panels.forEach((panel) => {
        panel.hidden = panel.dataset.emailVariantPanel !== target;
      });
    });
  });
}
document.querySelectorAll("[data-tiptap-editor]").forEach(initEmailEditor);
document.querySelectorAll("[data-email-variant-tabs]").forEach(initEmailVariantTabs);
</script>`)
}

func gmailEmailUpdateInputFromForm(r *http.Request) UpdateGmailEmailMutationInput {
	deliveryMode := strings.TrimSpace(r.FormValue("set_delivery_mode"))
	if deliveryMode == "" {
		deliveryMode = r.FormValue("delivery_mode")
	}
	message := map[string]any{
		"to":        splitEmailAddressList(r.FormValue("to")),
		"cc":        splitEmailAddressList(r.FormValue("cc")),
		"bcc":       splitEmailAddressList(r.FormValue("bcc")),
		"subject":   strings.TrimSpace(r.FormValue("subject")),
		"body_text": r.FormValue("body_text"),
		"body_html": r.FormValue("body_html"),
	}
	if replyToThreadID := strings.TrimSpace(r.FormValue("reply_to_thread_id")); replyToThreadID != "" {
		message["reply_to_thread_id"] = replyToThreadID
	}
	if inReplyTo := strings.TrimSpace(r.FormValue("in_reply_to")); inReplyTo != "" {
		message["in_reply_to"] = inReplyTo
	}
	if references := splitEmailAddressList(r.FormValue("references")); len(references) > 0 {
		message["references"] = references
	}
	return UpdateGmailEmailMutationInput{
		DeliveryMode:      deliveryMode,
		Message:           message,
		SelectedVariantID: strings.TrimSpace(r.FormValue("selected_variant_id")),
	}
}

func splitEmailAddressList(value string) []string {
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r'
	})
	return normalizeStringSlice(fields)
}

func renderGmailMutationGroup(w http.ResponseWriter, group *gmailMutationGroup) {
	if group == nil {
		return
	}
	verb := "Archive"
	if group.Operation == GmailUnarchiveOperation {
		verb = "Unarchive"
	}
	threadGroups := gmailMutationGroupThreads(group.Mutations)
	fmt.Fprintf(w, `<article class="mutation gmail-mutation"><div class="mutation-head"><div><p class="eyebrow">%s</p><h3>%s</h3></div><span class="pill">%s</span></div>`,
		html.EscapeString(verb),
		html.EscapeString(gmailMutationGroupTitle(verb, len(threadGroups))),
		html.EscapeString(gmailMutationGroupStatus(group.Status, len(group.Mutations))),
	)
	fmt.Fprintf(w, `<p class="mutation-meta">%s for %s</p>`, html.EscapeString(group.Operation), html.EscapeString(group.Account))
	fmt.Fprintf(w, `<p class="mutation-reason">%s</p>`, html.EscapeString(gmailMutationGroupActionText(group.Operation, len(threadGroups))))

	fmt.Fprint(w, `<div class="gmail-thread-list">`)
	for _, thread := range threadGroups {
		renderGmailThread(w, thread)
	}
	fmt.Fprint(w, `</div></article>`)
}

func gmailMutationGroupThreads(mutations []Mutation) []map[string]any {
	threads := []map[string]any{}
	for _, mutation := range mutations {
		mutationThreads := mapSliceFromAny(mutation.Preview["threads"])
		if len(mutationThreads) == 0 {
			for _, threadID := range gmailMutationThreadIDs(mutation) {
				mutationThreads = append(mutationThreads, map[string]any{"thread_id": threadID})
			}
		}
		threads = append(threads, mutationThreads...)
	}
	return threads
}

func gmailMutationGroupTitle(verb string, threadCount int) string {
	return fmt.Sprintf("%s %d Gmail thread%s", verb, threadCount, plural(threadCount))
}

func gmailMutationGroupActionText(operation string, threadCount int) string {
	threadNoun := "thread"
	if threadCount != 1 {
		threadNoun = "threads"
	}
	if operation == GmailUnarchiveOperation {
		if threadCount == 1 {
			return "Restores this thread to the Inbox."
		}
		return "Restores these " + threadNoun + " to the Inbox."
	}
	if threadCount == 1 {
		return "Removes this thread from the Inbox."
	}
	return "Removes these " + threadNoun + " from the Inbox."
}

func gmailMutationGroupStatus(status string, mutationCount int) string {
	if status == "" {
		status = "unknown"
	}
	if mutationCount <= 1 {
		return status
	}
	return fmt.Sprintf("%d %s", mutationCount, status)
}

func renderContactMutationGroup(w http.ResponseWriter, group *contactMutationGroup) {
	if group == nil {
		return
	}
	operations := contactMutationGroupOperations(group.Mutations)
	fmt.Fprintf(w, `<article class="mutation contact-mutation"><div class="mutation-head"><div><p class="eyebrow">Contacts</p><h3>%s</h3></div><span class="pill">%s</span></div>`,
		html.EscapeString(contactMutationGroupTitle(len(operations))),
		html.EscapeString(gmailMutationGroupStatus(group.Status, len(group.Mutations))),
	)
	fmt.Fprintf(w, `<p class="mutation-meta">contacts.batch_mutation for %s</p>`, html.EscapeString(group.Account))
	fmt.Fprint(w, `<div class="contact-operations">`)
	for _, operation := range operations {
		renderContactOperation(w, operation)
	}
	fmt.Fprint(w, `</div></article>`)
}

type contactOperationView struct {
	Operation map[string]any
	Mutation  Mutation
}

func contactMutationGroupOperations(mutations []Mutation) []contactOperationView {
	operations := []contactOperationView{}
	for _, mutation := range mutations {
		opMaps := mapSliceFromAny(mutation.Preview["operations"])
		if len(opMaps) == 0 {
			opMaps = mapSliceFromAny(mutation.Payload["operations"])
		}
		for _, operation := range opMaps {
			operations = append(operations, contactOperationView{Operation: operation, Mutation: mutation})
		}
	}
	return operations
}

func contactMutationGroupTitle(operationCount int) string {
	return fmt.Sprintf("Apply %d contact change%s", operationCount, plural(operationCount))
}

func renderContactOperation(w http.ResponseWriter, view contactOperationView) {
	operation := view.Operation
	op := canonicalContactOp(strings.TrimSpace(stringFromAny(operation["op"])))
	summary := contactOperationSummary(operation)
	title := contactOperationTitle(op)
	classes := "contact-operation"
	switch op {
	case "delete_contact":
		classes += " destructive"
	case "create_contact":
		classes += " creating"
	case "update_contact":
		classes += " updating"
	}
	fmt.Fprintf(w, `<div class="%s">`, html.EscapeString(classes))
	fmt.Fprint(w, `<div class="contact-operation-main">`)
	fmt.Fprintf(w, `<div class="contact-avatar" aria-hidden="true">%s</div>`, html.EscapeString(senderInitial(summary.DisplayName)))
	fmt.Fprint(w, `<div class="contact-operation-copy">`)
	fmt.Fprintf(w, `<p class="contact-op">%s</p>`, html.EscapeString(title))
	fmt.Fprintf(w, `<h4>%s</h4>`, html.EscapeString(contactSummaryTitle(summary, operation)))
	renderContactSummaryMeta(w, summary)
	renderContactOperationEffect(w, operation, op)
	fmt.Fprint(w, `</div>`)
	fmt.Fprintf(w, `<span class="pill">%s</span>`, html.EscapeString(view.Mutation.Status))
	fmt.Fprint(w, `</div>`)
	renderContactOperationBody(w, operation, op)
	fmt.Fprintf(w, `<details class="raw-json"><summary>Raw contact operation</summary><pre class="json-code">%s</pre></details>`, html.EscapeString(prettyJSON(operation)))
	fmt.Fprint(w, `</div>`)
}

type contactSummary struct {
	DisplayName  string
	PrimaryEmail string
	PrimaryPhone string
	Organization string
}

func contactOperationSummary(operation map[string]any) contactSummary {
	if summary := contactSummaryFromSummaryMap(mapFromAny(operation["summary"])); !summary.Empty() {
		return summary
	}
	for _, key := range []string{"person", "after", "before"} {
		if summary := contactSummaryFromPerson(mapFromAny(operation[key])); !summary.Empty() {
			return summary
		}
	}
	if summary := contactSummaryFromFlatOperation(operation); !summary.Empty() {
		return summary
	}
	return contactSummary{}
}

// canonicalContactOp maps the short verb form ("create"/"update"/"delete") that
// some proposers emit to the canonical form used by the rest of this package.
func canonicalContactOp(op string) string {
	switch op {
	case "create":
		return "create_contact"
	case "update":
		return "update_contact"
	case "delete":
		return "delete_contact"
	}
	return op
}

// contactSummaryFromFlatOperation reads a contact summary from a flat-shape
// operation that puts fields directly on the operation map (display_name,
// primary_email, primary_phone, organization, job_title) rather than nesting
// them inside a Google `Person` object.
func contactSummaryFromFlatOperation(operation map[string]any) contactSummary {
	displayName := strings.TrimSpace(stringFromAny(operation["display_name"]))
	if displayName == "" {
		given := strings.TrimSpace(stringFromAny(operation["given_name"]))
		family := strings.TrimSpace(stringFromAny(operation["family_name"]))
		displayName = strings.TrimSpace(given + " " + family)
	}
	organization := strings.TrimSpace(stringFromAny(operation["organization"]))
	if jobTitle := strings.TrimSpace(stringFromAny(operation["job_title"])); jobTitle != "" {
		if organization != "" {
			organization = jobTitle + ", " + organization
		} else {
			organization = jobTitle
		}
	}
	return contactSummary{
		DisplayName:  displayName,
		PrimaryEmail: strings.TrimSpace(stringFromAny(operation["primary_email"])),
		PrimaryPhone: strings.TrimSpace(stringFromAny(operation["primary_phone"])),
		Organization: organization,
	}
}

// personFromFlatOperation synthesizes a Google `Person`-shaped map from the
// flat top-level fields of an operation so the existing person-block renderer
// can work on either shape.
func personFromFlatOperation(operation map[string]any) map[string]any {
	person := map[string]any{}
	displayName := strings.TrimSpace(stringFromAny(operation["display_name"]))
	given := strings.TrimSpace(stringFromAny(operation["given_name"]))
	family := strings.TrimSpace(stringFromAny(operation["family_name"]))
	if displayName == "" {
		displayName = strings.TrimSpace(given + " " + family)
	}
	if displayName != "" || given != "" || family != "" {
		nameEntry := map[string]any{}
		if displayName != "" {
			nameEntry["displayName"] = displayName
		}
		if given != "" {
			nameEntry["givenName"] = given
		}
		if family != "" {
			nameEntry["familyName"] = family
		}
		person["names"] = []any{nameEntry}
	}
	if email := strings.TrimSpace(stringFromAny(operation["primary_email"])); email != "" {
		person["emailAddresses"] = []any{map[string]any{"value": email}}
	}
	if phone := strings.TrimSpace(stringFromAny(operation["primary_phone"])); phone != "" {
		person["phoneNumbers"] = []any{map[string]any{"canonicalForm": phone, "value": phone}}
	}
	organization := strings.TrimSpace(stringFromAny(operation["organization"]))
	jobTitle := strings.TrimSpace(stringFromAny(operation["job_title"]))
	if organization != "" || jobTitle != "" {
		orgEntry := map[string]any{}
		if organization != "" {
			orgEntry["name"] = organization
		}
		if jobTitle != "" {
			orgEntry["title"] = jobTitle
		}
		person["organizations"] = []any{orgEntry}
	}
	return person
}

func contactSummaryFromSummaryMap(value map[string]any) contactSummary {
	return contactSummary{
		DisplayName:  strings.TrimSpace(stringFromAny(value["display_name"])),
		PrimaryEmail: strings.TrimSpace(stringFromAny(value["primary_email"])),
		PrimaryPhone: strings.TrimSpace(stringFromAny(value["primary_phone"])),
		Organization: strings.TrimSpace(stringFromAny(value["organization"])),
	}
}

func contactSummaryFromPerson(person map[string]any) contactSummary {
	return contactSummary{
		DisplayName:  firstContactFieldValue(person["names"], "displayName", "unstructuredName", "givenName"),
		PrimaryEmail: firstContactFieldValue(contactPersonValue(person, "emailAddresses", "email_addresses", "emails"), "value"),
		PrimaryPhone: firstContactFieldValue(contactPersonValue(person, "phoneNumbers", "phone_numbers", "phones"), "canonicalForm", "value"),
		Organization: contactOrganizationSummary(contactPersonValue(person, "organizations")),
	}
}

func (summary contactSummary) Empty() bool {
	return summary.DisplayName == "" && summary.PrimaryEmail == "" && summary.PrimaryPhone == "" && summary.Organization == ""
}

func contactOperationTitle(op string) string {
	switch op {
	case "create_contact":
		return "Create contact"
	case "update_contact":
		return "Update contact"
	case "delete_contact":
		return "Delete contact"
	default:
		return "Change contact"
	}
}

func contactSummaryTitle(summary contactSummary, operation map[string]any) string {
	for _, value := range []string{summary.DisplayName, summary.PrimaryEmail, strings.TrimSpace(stringFromAny(operation["resource_name"]))} {
		if value != "" {
			return value
		}
	}
	return "Unnamed contact"
}

func renderContactSummaryMeta(w http.ResponseWriter, summary contactSummary) {
	fmt.Fprint(w, `<div class="contact-meta">`)
	for _, value := range []string{summary.PrimaryEmail, summary.PrimaryPhone, summary.Organization} {
		if value != "" {
			fmt.Fprintf(w, `<span>%s</span>`, html.EscapeString(value))
		}
	}
	fmt.Fprint(w, `</div>`)
}

func renderContactOperationEffect(w http.ResponseWriter, operation map[string]any, op string) {
	switch op {
	case "update_contact":
		fields := contactUpdateFields(operation)
		fmt.Fprint(w, `<p class="contact-effect">Replaces `)
		if len(fields) == 0 {
			fmt.Fprint(w, `selected fields`)
		} else {
			for index, field := range fields {
				if index > 0 {
					fmt.Fprint(w, `, `)
				}
				fmt.Fprintf(w, `<code>%s</code>`, html.EscapeString(field))
			}
		}
		fmt.Fprint(w, `</p>`)
	case "delete_contact":
		fmt.Fprint(w, `<p class="contact-effect">Deletes this contact from Google Contacts.</p>`)
	case "create_contact":
		fmt.Fprint(w, `<p class="contact-effect">Creates a new Google Contact.</p>`)
	}
	if resource := strings.TrimSpace(stringFromAny(operation["resource_name"])); resource != "" {
		fmt.Fprintf(w, `<p class="contact-resource"><code>%s</code></p>`, html.EscapeString(resource))
	}
}

func renderContactOperationBody(w http.ResponseWriter, operation map[string]any, op string) {
	switch op {
	case "update_contact":
		renderContactUpdateDiff(w, operation)
	case "create_contact":
		person := mapFromAny(operation["person"])
		if len(person) == 0 {
			person = personFromFlatOperation(operation)
		}
		renderContactPersonBlock(w, "Contact to create", person)
	case "delete_contact":
		person := mapFromAny(operation["before"])
		if len(person) == 0 {
			person = personFromFlatOperation(operation)
		}
		renderContactPersonBlock(w, "Contact to delete", person)
	}
}

func renderContactUpdateDiff(w http.ResponseWriter, operation map[string]any) {
	fields := contactUpdateFields(operation)
	before := mapFromAny(operation["before"])
	after := mapFromAny(operation["after"])
	fmt.Fprint(w, `<p class="muted">Fields not listed here are not part of this update.</p>`)
	fmt.Fprint(w, `<div class="contact-diff contact-inline-diff" aria-label="Contact update diff">`)
	for _, field := range fields {
		renderContactInlineDiffRow(w, field, contactFieldDisplayValue(before[field]), contactFieldDisplayValue(after[field]))
	}
	if len(fields) == 0 {
		fmt.Fprint(w, `<p class="contact-diff-empty">No explicit update fields were provided.</p>`)
	}
	fmt.Fprint(w, `</div>`)
}

func renderContactInlineDiffRow(w http.ResponseWriter, field string, before string, after string) {
	fmt.Fprintf(w, `<div class="contact-inline-row"><code>%s</code>`, html.EscapeString(field))
	if before == after {
		fmt.Fprintf(w, `<span class="contact-inline-unchanged">%s</span></div>`, html.EscapeString(after))
		return
	}
	fmt.Fprintf(w, `<span class="contact-inline-change"><del class="contact-inline-old" aria-label="Before">%s</del><span class="contact-inline-arrow" aria-hidden="true">&rarr;</span><ins class="contact-inline-new" aria-label="After">%s</ins></span></div>`,
		html.EscapeString(before),
		html.EscapeString(after),
	)
}

func renderContactPersonBlock(w http.ResponseWriter, title string, person map[string]any) {
	if len(person) == 0 {
		return
	}
	summary := contactSummaryFromPerson(person)
	fmt.Fprintf(w, `<div class="contact-person-block"><p class="contact-block-title">%s</p>`, html.EscapeString(title))
	fmt.Fprint(w, `<dl>`)
	for _, row := range []struct {
		Label string
		Value string
	}{
		{"Name", summary.DisplayName},
		{"Email", summary.PrimaryEmail},
		{"Phone", summary.PrimaryPhone},
		{"Organization", summary.Organization},
	} {
		if row.Value != "" {
			fmt.Fprintf(w, `<dt>%s</dt><dd>%s</dd>`, html.EscapeString(row.Label), html.EscapeString(row.Value))
		}
	}
	fmt.Fprint(w, `</dl></div>`)
}

// renderRequestContext renders the free-form context map attached to a
// request. It pulls out well-known keys (source, note, identifications) so
// they can be skimmed, and folds anything else into a collapsed JSON block.
func renderRequestContext(w http.ResponseWriter, ctx map[string]any) {
	if len(ctx) == 0 {
		return
	}
	source := strings.TrimSpace(stringFromAny(ctx["source"]))
	note := strings.TrimSpace(stringFromAny(ctx["note"]))
	identifications := mapSliceFromAny(ctx["identifications"])
	handled := map[string]bool{}
	if source != "" {
		handled["source"] = true
	}
	if note != "" {
		handled["note"] = true
	}
	if len(identifications) > 0 {
		handled["identifications"] = true
	}
	leftover := map[string]any{}
	for key, value := range ctx {
		if !handled[key] {
			leftover[key] = value
		}
	}
	if source == "" && note == "" && len(identifications) == 0 && len(leftover) == 0 {
		return
	}
	fmt.Fprint(w, `<section class="request-context"><h2>Context</h2>`)
	if source != "" {
		fmt.Fprintf(w, `<p class="context-source"><span class="context-label">Source</span> %s</p>`, html.EscapeString(source))
	}
	if note != "" {
		fmt.Fprintf(w, `<p class="context-note">%s</p>`, html.EscapeString(note))
	}
	if len(identifications) > 0 {
		fmt.Fprint(w, `<h3 class="context-heading">Identifications</h3><div class="identifications">`)
		for _, item := range identifications {
			renderIdentification(w, item)
		}
		fmt.Fprint(w, `</div>`)
	}
	if len(leftover) > 0 {
		fmt.Fprintf(w, `<details class="raw-json"><summary>Other context</summary><pre class="json-code">%s</pre></details>`, html.EscapeString(prettyJSON(leftover)))
	}
	fmt.Fprint(w, `</section>`)
}

func renderIdentification(w http.ResponseWriter, item map[string]any) {
	name := strings.TrimSpace(stringFromAny(item["inferred_name"]))
	if name == "" {
		name = strings.TrimSpace(stringFromAny(item["name"]))
	}
	if name == "" {
		name = strings.TrimSpace(stringFromAny(item["display_name"]))
	}
	if name == "" {
		name = "Unidentified"
	}
	confidence := strings.TrimSpace(stringFromAny(item["confidence"]))
	action := strings.TrimSpace(stringFromAny(item["action"]))
	maskedPhone := strings.TrimSpace(stringFromAny(item["masked_phone"]))
	evidence := stringSliceFromAny(item["evidence"])

	fmt.Fprint(w, `<article class="identification">`)
	fmt.Fprintf(w, `<div class="identification-head"><strong>%s</strong><div class="identification-chips">`, html.EscapeString(name))
	if confidence != "" {
		fmt.Fprintf(w, `<span class="confidence-chip %s">%s confidence</span>`, html.EscapeString(confidenceCSSClass(confidence)), html.EscapeString(confidence))
	}
	if action != "" {
		fmt.Fprintf(w, `<span class="identification-action">%s</span>`, html.EscapeString(action))
	}
	if maskedPhone != "" {
		fmt.Fprintf(w, `<code class="identification-phone">%s</code>`, html.EscapeString(maskedPhone))
	}
	fmt.Fprint(w, `</div></div>`)
	if len(evidence) > 0 {
		fmt.Fprint(w, `<ul class="identification-evidence">`)
		for _, line := range evidence {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			fmt.Fprintf(w, `<li>%s</li>`, html.EscapeString(line))
		}
		fmt.Fprint(w, `</ul>`)
	}
	fmt.Fprint(w, `</article>`)
}

func confidenceCSSClass(confidence string) string {
	switch strings.ToLower(strings.TrimSpace(confidence)) {
	case "high":
		return "confidence-high"
	case "medium-high":
		return "confidence-medium-high"
	case "medium":
		return "confidence-medium"
	case "medium-low":
		return "confidence-medium-low"
	case "low":
		return "confidence-low"
	}
	return ""
}

func contactFieldDisplayValue(value any) string {
	text := contactFieldValueSummary(value)
	if text == "" {
		text = "Not set"
	}
	return text
}

func contactFieldValueSummary(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case []any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := contactFieldMapSummary(mapFromAny(item)); text != "" {
				values = append(values, text)
			}
		}
		return strings.Join(values, "; ")
	case []map[string]any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := contactFieldMapSummary(item); text != "" {
				values = append(values, text)
			}
		}
		return strings.Join(values, "; ")
	case map[string]any:
		return contactFieldMapSummary(typed)
	default:
		return compactWhitespace(stringFromAny(value))
	}
}

func contactFieldMapSummary(value map[string]any) string {
	if organization := contactOrganizationSummary([]map[string]any{value}); organization != "" {
		return organization
	}
	for _, keys := range [][]string{
		{"displayName", "unstructuredName", "givenName"},
		{"canonicalForm", "value"},
		{"value"},
		{"name"},
		{"title"},
	} {
		if text := firstContactValueFromMap(value, keys...); text != "" {
			return text
		}
	}
	return compactWhitespace(prettyJSON(value))
}

func contactUpdateFields(operation map[string]any) []string {
	for _, key := range []string{"update_person_fields", "updatePersonFields"} {
		value := operation[key]
		if values := stringSliceFromAny(value); len(values) > 0 {
			return values
		}
		if text := strings.TrimSpace(stringFromAny(value)); text != "" {
			return normalizeStringSlice(strings.Split(text, ","))
		}
	}
	return nil
}

func contactPersonValue(person map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := person[key]; ok {
			return value
		}
	}
	return nil
}

func firstContactFieldValue(value any, keys ...string) string {
	for _, item := range mapSliceFromAnyValue(value) {
		if text := firstContactValueFromMap(item, keys...); text != "" {
			return text
		}
	}
	return ""
}

func firstContactValueFromMap(value map[string]any, keys ...string) string {
	for _, key := range keys {
		if text := strings.TrimSpace(stringFromAny(value[key])); text != "" {
			return text
		}
	}
	return ""
}

func contactOrganizationSummary(value any) string {
	for _, item := range mapSliceFromAnyValue(value) {
		name := strings.TrimSpace(stringFromAny(item["name"]))
		title := strings.TrimSpace(stringFromAny(item["title"]))
		switch {
		case name != "" && title != "":
			return title + ", " + name
		case name != "":
			return name
		case title != "":
			return title
		}
	}
	return ""
}

func mapSliceFromAnyValue(value any) []map[string]any {
	switch typed := value.(type) {
	case []map[string]any:
		return typed
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			if itemMap := mapFromAny(item); len(itemMap) > 0 {
				out = append(out, itemMap)
			}
		}
		return out
	default:
		return nil
	}
}

type gmailThreadRenderOptions struct {
	Open                bool
	RenderAfterMessages func(http.ResponseWriter)
}

func renderGmailThread(w http.ResponseWriter, thread map[string]any) {
	renderGmailThreadWithOptions(w, thread, gmailThreadRenderOptions{})
}

func renderGmailThreadWithOptions(w http.ResponseWriter, thread map[string]any, options gmailThreadRenderOptions) {
	threadID := strings.TrimSpace(stringFromAny(thread["thread_id"]))
	subject := strings.TrimSpace(stringFromAny(thread["subject"]))
	if subject == "" {
		subject = "(no subject)"
	}
	sender := strings.TrimSpace(stringFromAny(thread["latest_from_address"]))
	if sender == "" {
		sender = "Unknown sender"
	}
	latestPreview := truncateRunes(strings.TrimSpace(stringFromAny(thread["latest_preview"])), 420)
	latestAt := strings.TrimSpace(stringFromAny(thread["latest_at"]))
	messages := mapSliceFromAny(thread["messages"])
	messageCount := intFromAny(thread["message_count"])
	if messageCount == 0 {
		messageCount = len(messages)
	}
	inboxMessageCount := intFromAny(thread["inbox_message_count"])
	labels := []string{}
	if inboxMessageCount > 0 {
		labels = append(labels, "Inbox")
	}
	rawLabels := stringSliceFromAny(thread["labels"])
	threadUnread := hasGmailUnreadLabel(rawLabels)
	labels = appendVisibleGmailLabels(labels, rawLabels)
	senderName := gmailSenderDisplayName(sender, subject)

	openAttr := ""
	if options.Open {
		openAttr = " open"
	}
	fmt.Fprintf(w, `<details class="gmail-thread"%s>`, openAttr)
	fmt.Fprint(w, `<summary class="gmail-row">`)
	fmt.Fprint(w, `<span class="gmail-checkbox" aria-hidden="true"></span><span class="gmail-star" aria-hidden="true">&#9734;</span><span class="gmail-important" aria-hidden="true"></span>`)
	fmt.Fprintf(w, `<span class="gmail-sender">%s`, html.EscapeString(senderName))
	if messageCount > 1 {
		fmt.Fprintf(w, ` <span class="gmail-count">%d</span>`, messageCount)
	}
	fmt.Fprint(w, `</span>`)
	fmt.Fprint(w, `<span class="gmail-list-labels">`)
	for _, label := range labels {
		fmt.Fprintf(w, `<span class="gmail-label">%s</span>`, html.EscapeString(label))
	}
	fmt.Fprint(w, `</span>`)
	fmt.Fprintf(w, `<span class="gmail-subject"><strong>%s</strong>`, html.EscapeString(subject))
	if latestPreview != "" {
		fmt.Fprintf(w, `<span> - %s</span>`, html.EscapeString(latestPreview))
	}
	fmt.Fprint(w, `</span>`)
	fmt.Fprintf(w, `<span class="gmail-date">%s</span>`, html.EscapeString(formatGmailCompactTime(latestAt)))
	fmt.Fprint(w, `</summary>`)

	fmt.Fprint(w, `<div class="gmail-expanded">`)
	fmt.Fprint(w, `<div class="gmail-expanded-subject">`)
	fmt.Fprintf(w, `<h4>%s</h4>`, html.EscapeString(subject))
	fmt.Fprint(w, `<div class="gmail-thread-meta">`)
	if messageCount > 0 {
		fmt.Fprintf(w, `<span>%d message%s</span>`, messageCount, plural(messageCount))
	}
	for _, label := range labels {
		fmt.Fprintf(w, `<span class="gmail-label">%s</span>`, html.EscapeString(label))
	}
	if threadID != "" {
		fmt.Fprintf(w, `<span class="thread-id">%s</span>`, html.EscapeString(threadID))
	}
	fmt.Fprint(w, `</div></div>`)

	if len(messages) > 0 {
		fmt.Fprint(w, `<div class="gmail-messages">`)
		for index, message := range messages {
			fallbackOpen := threadUnread && !gmailMessageHasLabelIDs(message) && index == len(messages)-1
			renderGmailMessage(w, message, fallbackOpen)
		}
		fmt.Fprint(w, `</div>`)
	}
	if options.RenderAfterMessages != nil {
		options.RenderAfterMessages(w)
	}
	fmt.Fprint(w, `</div></details>`)
}

func renderGmailMessage(w http.ResponseWriter, message map[string]any, fallbackOpen bool) {
	from := strings.TrimSpace(stringFromAny(message["from_address"]))
	if from == "" {
		from = "Unknown sender"
	}
	to := strings.Join(stringSliceFromAny(message["to_addresses"]), ", ")
	cc := strings.Join(stringSliceFromAny(message["cc_addresses"]), ", ")
	preview := truncateRunes(strings.TrimSpace(stringFromAny(message["preview_text"])), 900)
	if preview == "" {
		preview = truncateRunes(strings.TrimSpace(stringFromAny(message["snippet"])), 900)
	}
	bodyHTML := strings.TrimSpace(stringFromAny(message["body_html"]))
	openAttr := ""
	if hasGmailUnreadLabel(stringSliceFromAny(message["label_ids"])) || fallbackOpen {
		openAttr = " open"
	}
	fmt.Fprintf(w, `<details class="gmail-message"%s>`, openAttr)
	fmt.Fprint(w, `<summary class="gmail-message-summary">`)
	fmt.Fprintf(w, `<div class="gmail-avatar" aria-hidden="true">%s</div>`, html.EscapeString(senderInitial(from)))
	fmt.Fprint(w, `<div class="gmail-message-summary-main">`)
	fmt.Fprint(w, `<div class="gmail-message-header">`)
	fmt.Fprintf(w, `<div><strong>%s</strong>`, html.EscapeString(from))
	if to != "" {
		fmt.Fprintf(w, `<span class="gmail-to">to %s</span>`, html.EscapeString(to))
	}
	if cc != "" {
		fmt.Fprintf(w, `<span class="gmail-to">cc %s</span>`, html.EscapeString(cc))
	}
	fmt.Fprint(w, `</div>`)
	fmt.Fprintf(w, `<time>%s</time>`, html.EscapeString(formatGmailFullTime(stringFromAny(message["internal_date"]))))
	fmt.Fprint(w, `</div>`)
	if preview != "" {
		fmt.Fprintf(w, `<p class="gmail-message-preview">%s</p>`, html.EscapeString(preview))
	}
	fmt.Fprint(w, `</div></summary>`)
	if bodyHTML != "" {
		bodyHTML, quotedHTML := splitGmailQuotedHTML(bodyHTML)
		fmt.Fprint(w, `<div class="gmail-message-body">`)
		fmt.Fprintf(w, `<iframe class="gmail-body-frame" style="height:%dpx" sandbox referrerpolicy="no-referrer" srcdoc="%s"></iframe>`, gmailBodyFrameHeight(bodyHTML, false), html.EscapeString(emailHTMLDocument(bodyHTML)))
		if quotedHTML != "" {
			fmt.Fprintf(w, `<details class="gmail-quoted"><summary>Quoted message</summary><iframe class="gmail-body-frame quoted" style="height:%dpx" sandbox referrerpolicy="no-referrer" srcdoc="%s"></iframe></details>`, gmailBodyFrameHeight(quotedHTML, true), html.EscapeString(emailHTMLDocument(quotedHTML)))
		}
		fmt.Fprint(w, `</div>`)
	} else if preview != "" {
		fmt.Fprintf(w, `<div class="gmail-message-body"><p>%s</p></div>`, html.EscapeString(preview))
	}
	fmt.Fprint(w, `</details>`)
}

func gmailMessageHasLabelIDs(message map[string]any) bool {
	_, ok := message["label_ids"]
	return ok
}

func hasGmailUnreadLabel(labels []string) bool {
	for _, label := range labels {
		normalized := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(label), " ", "_"))
		if normalized == "UNREAD" {
			return true
		}
	}
	return false
}

func gmailBodyFrameHeight(bodyHTML string, quoted bool) int {
	text := htmlFragmentText(bodyHTML)
	lines := normalizeStringSlice(strings.Split(text, "\n"))
	visualLines := 1
	if len(lines) > 0 {
		visualLines = 0
		for _, line := range lines {
			lineLength := len([]rune(line))
			visualLines += max(1, (lineLength+89)/90)
		}
	}
	height := 76 + visualLines*20
	lowerHTML := strings.ToLower(bodyHTML)
	if strings.Contains(lowerHTML, "<img") {
		height += 140
	}
	if strings.Contains(lowerHTML, "<table") {
		height += 24
	}
	minHeight, maxHeight := 128, 240
	if quoted {
		minHeight, maxHeight = 112, 200
	}
	if height < minHeight {
		return minHeight
	}
	if height > maxHeight {
		return maxHeight
	}
	return height
}

func emailHTMLDocument(bodyHTML string) string {
	return `<!doctype html><html><head><base target="_blank"><style>html,body{margin:0;padding:0;background:white;color:#202124;font-family:Arial,sans-serif;}img{max-width:100%;height:auto;}table{max-width:100%;}a{color:#1a73e8;}</style></head><body>` + bodyHTML + `</body></html>`
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

func (s *Service) requireSession(w http.ResponseWriter, r *http.Request) (sessionPayload, bool) {
	session, ok := s.currentSession(r)
	if ok {
		return session, true
	}
	next := ReviewPath + "/login?next=" + url.QueryEscape(r.URL.RequestURI())
	http.Redirect(w, r, next, http.StatusSeeOther)
	return sessionPayload{}, false
}

func (s *Service) currentSession(r *http.Request) (sessionPayload, bool) {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil || cookie.Value == "" {
		return sessionPayload{}, false
	}
	payload, ok := s.verifySessionCookie(cookie.Value)
	if !ok {
		return sessionPayload{}, false
	}
	if payload.Expires <= s.now().Unix() || payload.Subject != "mutation-review" || payload.CSRF == "" {
		return sessionPayload{}, false
	}
	return payload, true
}

func (s *Service) validCSRF(r *http.Request, session sessionPayload) bool {
	if err := r.ParseForm(); err != nil {
		return false
	}
	got := r.FormValue("csrf_token")
	return got != "" && subtle.ConstantTimeCompare([]byte(got), []byte(session.CSRF)) == 1
}

func (s *Service) newSession() (sessionPayload, error) {
	csrf, err := randomToken()
	if err != nil {
		return sessionPayload{}, err
	}
	now := s.now()
	return sessionPayload{
		Subject: "mutation-review",
		Issued:  now.Unix(),
		Expires: now.Add(s.cfg.SessionTTL).Unix(),
		CSRF:    csrf,
	}, nil
}

func (s *Service) setSessionCookie(w http.ResponseWriter, r *http.Request, payload sessionPayload) {
	value := s.signSession(payload)
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    value,
		Path:     ReviewPath,
		Expires:  time.Unix(payload.Expires, 0).UTC(),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil || strings.HasPrefix(s.cfg.BaseURL, "https://"),
	})
}

func (s *Service) clearSession(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     ReviewPath,
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil || strings.HasPrefix(s.cfg.BaseURL, "https://"),
	})
}

func (s *Service) signSession(payload sessionPayload) string {
	data, _ := json.Marshal(payload)
	encoded := base64.RawURLEncoding.EncodeToString(data)
	mac := hmac.New(sha256.New, []byte(s.sessionSecret()))
	_, _ = mac.Write([]byte(encoded))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return encoded + "." + signature
}

func (s *Service) verifySessionCookie(value string) (sessionPayload, bool) {
	encoded, signature, ok := strings.Cut(value, ".")
	if !ok || encoded == "" || signature == "" {
		return sessionPayload{}, false
	}
	mac := hmac.New(sha256.New, []byte(s.sessionSecret()))
	_, _ = mac.Write([]byte(encoded))
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(signature), []byte(expected)) != 1 {
		return sessionPayload{}, false
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return sessionPayload{}, false
	}
	var payload sessionPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return sessionPayload{}, false
	}
	return payload, true
}

func (s *Service) sessionSecret() string {
	return s.cfg.SessionSecret
}

func (s *Service) now() time.Time {
	if s != nil && s.cfg.Now != nil {
		return s.cfg.Now()
	}
	return time.Now().UTC()
}

func randomToken() (string, error) {
	var data [32]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data[:]), nil
}

func safeNextPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if !strings.HasPrefix(value, ReviewPath+"/") && value != ReviewPath {
		return ""
	}
	if strings.Contains(value, "\n") || strings.Contains(value, "\r") {
		return ""
	}
	return value
}

func prettyJSON(value map[string]any) string {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "{}"
	}
	return string(data)
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		number, _ := typed.Int64()
		return int(number)
	default:
		return 0
	}
}

func gmailSenderDisplayName(from string, subject string) string {
	from = strings.TrimSpace(from)
	if displayName, _, ok := strings.Cut(from, "<"); ok {
		displayName = strings.Trim(strings.TrimSpace(displayName), `"`)
		if displayName != "" {
			return displayName
		}
	}
	address := strings.Trim(strings.TrimSpace(from), "<>")
	local, domain, hasDomain := strings.Cut(address, "@")
	domainName := domain
	if dot := strings.Index(domainName, "."); dot > 0 {
		domainName = domainName[:dot]
	}
	switch {
	case strings.Contains(domain, "uber"):
		return "Uber Receipts"
	case strings.Contains(domain, "hcb"):
		return "HCB"
	case strings.Contains(domain, "turbotenant"):
		return "TurboTenant"
	case strings.Contains(domain, "dinobox"):
		return "dinobox"
	case hasDomain && !isGenericSenderLocal(local):
		return titleSenderName(local)
	case hasDomain && domainName != "":
		return titleSenderName(domainName)
	}
	if subject != "" {
		return subject
	}
	return from
}

func isGenericSenderLocal(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "no-reply", "noreply", "notifications", "notification", "receipts", "receipt", "support", "hello":
		return true
	default:
		return false
	}
}

func titleSenderName(value string) string {
	parts := strings.Fields(strings.NewReplacer(".", " ", "-", " ", "_", " ").Replace(value))
	for index, part := range parts {
		if part == "" {
			continue
		}
		parts[index] = strings.ToUpper(part[:1]) + strings.ToLower(part[1:])
	}
	return strings.Join(parts, " ")
}

func senderInitial(value string) string {
	for _, r := range strings.TrimSpace(value) {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return strings.ToUpper(string(r))
		}
	}
	return "?"
}

func truncateRunes(value string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	if max <= 3 {
		return string(runes[:max])
	}
	return string(runes[:max-3]) + "..."
}

func formatGmailCompactTime(value string) string {
	parsed, ok := parsePreviewTime(value)
	if !ok {
		return value
	}
	return parsed.UTC().Format("Jan 2")
}

func formatGmailFullTime(value string) string {
	parsed, ok := parsePreviewTime(value)
	if !ok {
		return strings.TrimSpace(value)
	}
	return parsed.UTC().Format("Jan 2, 2006 15:04 UTC")
}

func parsePreviewTime(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05.000", "2006-01-02 15:04:05"} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func methodNotAllowed(w http.ResponseWriter) {
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func writeHTMLHeader(w http.ResponseWriter, title string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>%s</title><style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:0;background:#f6f7f8;color:#202124}
main{max-width:1120px;margin:0 auto;padding:28px}
main.login{max-width:420px}
header{display:flex;align-items:center;justify-content:space-between;gap:16px}
h1{font-size:28px;margin:0 0 14px} h2{font-size:20px;margin-top:28px} h3{font-size:16px;margin:0 0 8px}
table{width:100%%;border-collapse:collapse;background:white;border:1px solid #d8ddd8}
th,td{text-align:left;padding:10px 12px;border-bottom:1px solid #e5e7e2;vertical-align:top}
article,section{background:white;border:1px solid #dadce0;padding:16px;margin:14px 0;border-radius:8px}
button{background:#1a73e8;color:white;border:0;padding:9px 12px;border-radius:6px;cursor:pointer}
input{padding:8px;border:1px solid #bdc1c6;border-radius:6px}
label{display:grid;gap:6px;margin:14px 0}
pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f0f2ee;padding:10px;border-radius:6px}
.reason,.status,.mutation-meta,.empty,.muted{color:#5f6368}.error{color:#9b1c1c}.actions{display:inline-flex;gap:8px;margin-right:8px}
.mutation-head{display:flex;align-items:flex-start;justify-content:space-between;gap:16px}
.mutation-reason{margin:8px 0 10px;color:#3c4043}.eyebrow{margin:0 0 4px;color:#1a73e8;font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.04em}
.pill{background:#e8f0fe;color:#174ea6;border-radius:999px;padding:4px 9px;font-size:12px;font-weight:600;white-space:nowrap}
.gmail-mutation{padding:0;overflow:hidden}.gmail-mutation>.mutation-head,.gmail-mutation>.mutation-reason,.gmail-mutation>.mutation-meta,.gmail-mutation>.raw-json{margin-left:16px;margin-right:16px}.gmail-mutation>.mutation-head{margin-top:16px}
.gmail-thread{border-top:1px solid #dfe1e5}.gmail-thread summary{list-style:none}.gmail-thread summary::-webkit-details-marker{display:none}
.gmail-row{display:grid;grid-template-columns:24px 24px 24px minmax(130px,190px) auto minmax(260px,1fr) 72px;align-items:center;gap:8px;min-height:46px;padding:0 16px;background:#fff;cursor:pointer}
.gmail-row:hover{background:#f8fafd;box-shadow:0 1px 3px rgba(60,64,67,.22)}.gmail-thread[open]>.gmail-row{box-shadow:inset 3px 0 #dadce0}.gmail-checkbox{width:14px;height:14px;border:2px solid #5f6368;border-radius:2px}.gmail-star{color:#5f6368;font-size:18px;line-height:1}.gmail-important{width:13px;height:14px;clip-path:polygon(0 0,70%% 0,100%% 50%%,70%% 100%%,0 100%%,30%% 50%%);background:#5f6368}.gmail-sender{font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.gmail-count{font-weight:500;color:#5f6368}.gmail-list-labels{display:flex;gap:6px;align-items:center;overflow:hidden;white-space:nowrap}.gmail-subject{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.gmail-subject span{color:#5f6368;font-weight:400}.gmail-date{text-align:right;color:#5f6368;font-size:12px;font-weight:600;white-space:nowrap}
.gmail-label{background:#e8eaed;color:#3c4043;border-radius:4px;padding:2px 6px;font-size:12px;font-weight:500}.gmail-thread-meta{display:flex;flex-wrap:wrap;align-items:center;gap:8px;color:#5f6368;font-size:12px}.thread-id{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;color:#80868b}
.gmail-expanded{border-top:1px solid #e8eaed;background:#fff}.gmail-expanded-subject{display:flex;align-items:center;gap:18px;padding:18px 28px 8px 76px}.gmail-expanded-subject h4{font-size:22px;font-weight:400;margin:0;color:#202124}
.gmail-messages{padding:0 0 18px}.gmail-message{border-top:1px solid #f1f3f4}.gmail-message summary{list-style:none}.gmail-message summary::-webkit-details-marker{display:none}.gmail-message-summary{display:grid;grid-template-columns:48px minmax(0,1fr);gap:14px;padding:18px 28px;cursor:pointer}.gmail-message-summary:hover{background:#f8fafd}.gmail-avatar{width:40px;height:40px;border-radius:50%%;background:#5e7ce2;color:white;display:grid;place-items:center;font-weight:700;font-size:18px}.gmail-message-summary-main{min-width:0}.gmail-message-header{display:flex;justify-content:space-between;gap:16px;align-items:flex-start}.gmail-message-header strong{display:inline;font-size:15px}.gmail-message-header time{color:#5f6368;font-size:13px;white-space:nowrap}.gmail-to{display:block;color:#5f6368;font-size:12px;margin-top:3px}.gmail-message-preview{margin:8px 0 0;color:#5f6368;line-height:1.4;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.gmail-message[open] .gmail-message-preview{display:none}.gmail-message-body{padding:0 28px 22px 90px}.gmail-message-body p{margin:8px 0 0;color:#3c4043;line-height:1.45;white-space:pre-wrap}.gmail-body-frame{display:block;width:100%%;min-height:120px;max-height:520px;border:0;background:white}.gmail-quoted{margin-top:14px;border-left:3px solid #dadce0;padding-left:12px}.gmail-quoted>summary{color:#5f6368;cursor:pointer;font-size:13px;margin-bottom:8px}.gmail-body-frame.quoted{max-height:340px}.raw-json{border-top:1px solid #e8eaed;padding-top:12px;margin-bottom:16px}.raw-json summary{color:#5f6368;cursor:pointer}
.gmail-email-mutation{padding:0;overflow:hidden}.gmail-email-mutation>.mutation-head,.gmail-email-mutation>.mutation-meta,.gmail-email-mutation>.mutation-reason,.gmail-email-mutation>.gmail-email-form,.gmail-email-mutation>.gmail-email-readonly,.gmail-email-mutation>.gmail-email-tabs,.gmail-email-mutation>.gmail-email-variant-panel,.gmail-email-mutation>.gmail-email-reply-context,.gmail-email-mutation>.gmail-email-remove{margin-left:16px;margin-right:16px}.gmail-email-remove{display:flex;justify-content:flex-end;margin-top:-4px;margin-bottom:16px}.gmail-inline-reply .gmail-email-remove{margin:8px 0 0;justify-content:flex-end}button.danger{background:#c5221f}button.danger:hover{background:#a50e0e}.gmail-email-mutation>.mutation-head{margin-top:16px}.gmail-email-reply-context{margin-top:14px;border:1px solid #dadce0;border-radius:6px;overflow:hidden;background:#fff}.gmail-email-reply-context-head{padding:10px 12px;border-bottom:1px solid #e8eaed;color:#5f6368;font-size:13px;font-weight:700}.gmail-email-reply-context .gmail-thread:first-child{border-top:0}.gmail-inline-reply{display:grid;grid-template-columns:48px minmax(0,1fr);gap:14px;padding:18px 28px 24px;border-top:1px solid #f1f3f4}.gmail-inline-reply-main{min-width:0}.gmail-inline-reply .gmail-email-tabs{margin-top:0}.gmail-inline-reply .gmail-email-form{margin:0}.gmail-inline-reply .gmail-email-variant-panel{margin:0}.gmail-inline-reply .gmail-email-editor-surface{min-height:108px}.gmail-email-tabs{display:flex;gap:6px;flex-wrap:wrap;margin-top:14px;border-bottom:1px solid #e8eaed}.gmail-email-tab{border:0;border-bottom:3px solid transparent;border-radius:0;background:transparent;color:#5f6368;padding:9px 10px;font-weight:700}.gmail-email-tab.active{border-bottom-color:#1a73e8;color:#202124;background:#f8fafd}.gmail-email-form{display:grid;gap:12px;margin-top:14px;margin-bottom:16px}.gmail-email-variant-copy{margin:0;color:#5f6368}.gmail-email-delivery{display:flex;gap:8px;flex-wrap:wrap}.delivery-option{display:flex;align-items:center;gap:7px;margin:0;border:1px solid #dadce0;border-radius:6px;padding:7px 10px;background:#fff;color:#3c4043;font-weight:600}.delivery-option:has(input:checked){border-color:#1a73e8;background:#e8f0fe;color:#174ea6}.delivery-option input{margin:0}.gmail-email-fields{border:1px solid #dadce0;border-radius:6px;background:#fff}.gmail-email-fields label{display:grid;grid-template-columns:72px minmax(0,1fr);gap:12px;align-items:center;margin:0;padding:10px 12px;border-top:1px solid #eef0f2;color:#5f6368;font-size:13px}.gmail-email-fields label:first-child{border-top:0}.gmail-email-fields input{border:0;border-radius:0;padding:0;font:inherit;color:#202124;min-width:0}.gmail-email-fields input:focus{outline:0}.gmail-email-editor-wrap{border:1px solid #dadce0;border-radius:6px;background:#fff;overflow:hidden}.gmail-email-toolbar{display:flex;gap:4px;align-items:center;border-bottom:1px solid #e8eaed;background:#f8fafd;padding:6px}.gmail-email-toolbar button{background:#fff;color:#3c4043;border:1px solid #dadce0;padding:5px 8px;min-width:30px}.gmail-email-editor-surface{min-height:180px;padding:14px;outline:none;line-height:1.45}.gmail-email-editor:has(+ .gmail-email-signature-preview) .gmail-email-editor-surface{min-height:0;padding-bottom:6px}.gmail-email-editor-surface p{margin:0 0 10px}.gmail-email-editor-surface ul,.gmail-email-editor-surface ol{margin:8px 0 8px 22px;padding:0}.gmail-email-signature-preview{padding:0 14px 14px;color:#202124;line-height:normal}.gmail-email-signature-preview div{margin:0}.gmail-email-signature-preview div[dir=ltr]>span:first-child+br{display:none}.gmail-email-signature-preview a{color:#1a73e8}.gmail-email-quote-preview{border-top:1px solid #eef0f2;margin:4px 14px 14px;padding-top:10px}.gmail-email-quote-preview>summary{color:#5f6368;cursor:pointer;font-size:13px}.gmail-email-actions{display:flex;gap:8px;flex-wrap:wrap}.gmail-email-actions button[name=set_delivery_mode]{background:#137333}.gmail-email-readonly{display:grid;gap:12px;margin-top:14px;margin-bottom:16px}.gmail-email-readonly dl{display:grid;grid-template-columns:86px minmax(0,1fr);gap:8px 12px;margin:0;border:1px solid #dadce0;border-radius:6px;padding:12px;background:#fff}.gmail-email-readonly dt{color:#5f6368}.gmail-email-readonly dd{margin:0;overflow-wrap:anywhere}.gmail-email-body-frame{width:100%%;height:360px;border:1px solid #dadce0;border-radius:6px;background:#fff}
.calendar-mutation{padding:0;overflow:hidden;border:1px solid #dadce0;border-radius:8px;background:#fff;box-shadow:0 1px 3px rgba(60,64,67,.08)}
.calendar-mutation .calendar-banner{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:14px 20px;border-top-left-radius:8px;border-top-right-radius:8px;color:#fff}
.calendar-mutation.create .calendar-banner{background:linear-gradient(135deg,#137333,#1e8e3e)}
.calendar-mutation.update .calendar-banner{background:linear-gradient(135deg,#1a73e8,#4285f4)}
.calendar-mutation.delete .calendar-banner{background:linear-gradient(135deg,#a50e0e,#c5221f)}
.calendar-banner-left{display:flex;align-items:center;gap:14px;flex-wrap:wrap;min-width:0}
.calendar-op-badge{background:rgba(255,255,255,.22);padding:5px 12px;border-radius:999px;font-weight:700;font-size:13px;letter-spacing:.02em;white-space:nowrap}
.calendar-banner-account{color:rgba(255,255,255,.92);font-size:13px;font-weight:600;overflow-wrap:anywhere}
.calendar-status-pill{background:rgba(255,255,255,.18);color:#fff;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:700;white-space:nowrap}
.calendar-body{padding:18px 22px 22px}
.calendar-title{font-size:24px;font-weight:600;margin:0 0 4px;color:#202124;line-height:1.25}
.calendar-reason{margin:0 0 16px;color:#5f6368;font-size:14px}
.calendar-when{display:flex;gap:14px;align-items:flex-start;padding:14px 16px;border-radius:8px;background:#f1f3f4;margin-bottom:16px}
.calendar-when-icon{font-size:22px;line-height:1.2}
.calendar-when-body{display:flex;flex-direction:column;gap:2px;min-width:0}
.calendar-when-date{font-weight:700;color:#202124;font-size:16px}
.calendar-when-time{color:#3c4043;font-size:14px}
.calendar-meta{list-style:none;margin:0 0 14px;padding:0;display:flex;flex-wrap:wrap;gap:10px}
.calendar-meta-item{display:flex;align-items:center;gap:8px;background:#fff;border:1px solid #e0e3e7;border-radius:999px;padding:6px 12px;font-size:13px;color:#3c4043}
.calendar-meta-item code{background:transparent;color:#202124;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
.calendar-meta-label{display:block;color:#80868b;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.04em;line-height:1.1;margin-bottom:1px}
.calendar-meta-notify.off{background:#fdecea;border-color:#f6c9c6;color:#a50e0e}
.calendar-meta-notify.external{background:#fff7e0;border-color:#fbd982;color:#92660e}
.calendar-meta-recurrence{background:#e8f0fe;border-color:#c5d7fb;color:#174ea6}
.calendar-icon{font-size:16px;line-height:1}
.calendar-section{margin:18px 0 0;padding:0}
.calendar-section h4{font-size:13px;font-weight:700;color:#5f6368;text-transform:uppercase;letter-spacing:.04em;margin:0 0 8px}
.calendar-location{display:flex;gap:10px;align-items:flex-start;padding:10px 12px;background:#fafbfc;border-radius:8px;border:1px solid #e8eaed;font-size:14px;color:#202124}
.calendar-description p{margin:0;color:#202124;line-height:1.5}
.calendar-attendees ul{list-style:none;margin:0;padding:0;display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:8px}
.calendar-attendee{display:flex;align-items:center;gap:12px;padding:8px 10px;border:1px solid #e8eaed;border-radius:8px;background:#fff}
.calendar-avatar{width:36px;height:36px;flex-shrink:0;border-radius:50%%;display:grid;place-items:center;color:#fff;font-weight:700;font-size:14px}
.calendar-avatar.color-a{background:#1a73e8}
.calendar-avatar.color-b{background:#137333}
.calendar-avatar.color-c{background:#a142f4}
.calendar-avatar.color-d{background:#f9ab00}
.calendar-avatar.color-e{background:#d93025}
.calendar-avatar.color-f{background:#12b5cb}
.calendar-avatar.color-g{background:#f538a0}
.calendar-avatar.color-h{background:#7d3c98}
.calendar-attendee-body{min-width:0;flex:1}
.calendar-attendee-name{font-size:14px;font-weight:600;color:#202124;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.calendar-attendee-email{font-size:12px;color:#5f6368;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.calendar-attendee-tags{display:flex;gap:4px;flex-wrap:wrap;margin-top:4px}
.calendar-attendee-tag{background:#e8f0fe;color:#174ea6;border-radius:6px;padding:2px 7px;font-size:11px;font-weight:600}
.calendar-attendee-tag.organizer{background:#fef7e0;color:#92660e}
.calendar-attendee-tag.optional{background:#f1f3f4;color:#5f6368}
.calendar-attendee-tag.rsvp-accepted{background:#e6f4ea;color:#137333}
.calendar-attendee-tag.rsvp-declined{background:#fce8e6;color:#a50e0e}
.calendar-attendee-tag.rsvp-tentative{background:#fff7e0;color:#92660e}
.calendar-attendee-tag.rsvp-needsAction{background:#f1f3f4;color:#5f6368}
.calendar-patch table{border:1px solid #e8eaed;border-radius:8px;overflow:hidden;background:#fff}
.calendar-patch th,.calendar-patch td{border-bottom:1px solid #f1f3f4;padding:10px 12px}
.calendar-patch th{background:#f8fafd;font-weight:600;color:#202124;font-size:13px;width:140px;text-align:left}
.calendar-patch tr:last-child th,.calendar-patch tr:last-child td{border-bottom:0}
.calendar-patch tbody th{background:#f8fafd;font-weight:600;color:#3c4043}
.calendar-patch td{color:#202124}
.calendar-patch td pre{white-space:pre-wrap;margin:0;background:#f8fafd;padding:8px 10px;border-radius:6px;font-size:12px}
.calendar-delete-warning{background:#fce8e6;border:1px solid #f6c9c6;color:#a50e0e;border-radius:8px;padding:14px 16px;font-size:14px;line-height:1.45}
.calendar-tech{margin-top:18px;border-top:1px solid #e8eaed;padding-top:10px}
.calendar-tech summary{color:#5f6368;cursor:pointer;font-size:13px}
.calendar-tech dl{margin:10px 0 0;display:grid;grid-template-columns:140px minmax(0,1fr);gap:6px 12px}
.calendar-tech dt{color:#5f6368;font-size:12px}
.calendar-tech dd{margin:0;color:#202124;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;overflow-wrap:anywhere}
.contact-mutation{padding:0;overflow:hidden}.contact-mutation>.mutation-head,.contact-mutation>.mutation-meta{margin-left:16px;margin-right:16px}.contact-mutation>.mutation-head{margin-top:16px}.contact-operations{border-top:1px solid #dfe1e5;margin-top:16px}.contact-operation{border-top:1px solid #f1f3f4;padding:18px 16px}.contact-operation:first-child{border-top:0}.contact-operation.destructive{box-shadow:inset 3px 0 #c5221f}.contact-operation.creating{box-shadow:inset 3px 0 #137333}.contact-operation.updating{box-shadow:inset 3px 0 #1967d2}.contact-operation.creating .contact-avatar{background:#137333}.contact-operation.updating .contact-avatar{background:#1967d2}.contact-operation-main{display:grid;grid-template-columns:44px minmax(0,1fr) auto;gap:14px;align-items:flex-start}.contact-avatar{width:40px;height:40px;border-radius:50%%;background:#137333;color:white;display:grid;place-items:center;font-weight:700;font-size:18px}.contact-operation.destructive .contact-avatar{background:#c5221f}.contact-operation-copy{min-width:0}.contact-operation-copy h4{margin:2px 0 4px;font-size:18px}.contact-op{margin:0;color:#1a73e8;font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.04em}.contact-meta{display:flex;flex-wrap:wrap;gap:8px;color:#5f6368;font-size:13px}.contact-effect,.contact-resource{margin:8px 0 0;color:#3c4043}.contact-effect code,.contact-resource code{background:#f1f3f4;border-radius:4px;padding:2px 5px}.contact-diff{margin-top:12px;border:1px solid #e8eaed;border-radius:6px;background:white}.contact-inline-row{display:grid;grid-template-columns:150px minmax(0,1fr);gap:12px;align-items:center;border-top:1px solid #eef0f2;padding:10px 12px}.contact-inline-row:first-child{border-top:0}.contact-inline-row>code{background:#f8fafd;border-radius:4px;padding:4px 6px;justify-self:start}.contact-inline-change{display:flex;align-items:center;flex-wrap:wrap;gap:6px;min-width:0}.contact-inline-old,.contact-inline-new,.contact-inline-unchanged{border-radius:4px;padding:3px 6px;overflow-wrap:anywhere}.contact-inline-old{background:#fce8e6;color:#8c1d18;text-decoration:line-through;text-decoration-thickness:2px}.contact-inline-new{background:#e6f4ea;color:#137333;text-decoration:none}.contact-inline-arrow{color:#5f6368}.contact-diff-empty{margin:0;padding:12px;color:#5f6368}.contact-person-block{margin-top:12px;border-top:1px solid #e8eaed;padding-top:12px}.contact-block-title{margin:0 0 8px;font-weight:700}.contact-person-block dl{display:grid;grid-template-columns:120px minmax(0,1fr);gap:6px 12px;margin:0}.contact-person-block dt{color:#5f6368}.contact-person-block dd{margin:0}.contact-operation .raw-json{margin-bottom:0}
.request-context{padding:18px 20px;background:#fff;border:1px solid #dadce0;border-radius:8px;margin:14px 0}.request-context h2{margin:0 0 12px;font-size:18px}.request-context .context-heading{font-size:13px;font-weight:700;color:#5f6368;text-transform:uppercase;letter-spacing:.04em;margin:18px 0 10px}.context-source{margin:0 0 10px;color:#3c4043}.context-label{display:inline-block;color:#5f6368;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.04em;margin-right:6px}.context-note{margin:0 0 6px;color:#3c4043;white-space:pre-wrap}.identifications{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px}.identification{border:1px solid #e8eaed;border-radius:6px;padding:12px 14px;background:#fafbfc}.identification-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:6px}.identification-head strong{font-size:14px;color:#202124}.identification-chips{display:flex;gap:6px;flex-wrap:wrap;align-items:center}.confidence-chip{display:inline-block;border-radius:999px;padding:2px 9px;font-size:12px;font-weight:600;background:#e8eaed;color:#3c4043}.confidence-chip.confidence-high{background:#e6f4ea;color:#137333}.confidence-chip.confidence-medium-high,.confidence-chip.confidence-medium{background:#fef7e0;color:#92660e}.confidence-chip.confidence-medium-low{background:#fce8e6;color:#a50e0e}.confidence-chip.confidence-low{background:#fce8e6;color:#a50e0e}.identification-action{display:inline-block;background:#fef7e0;color:#92660e;border-radius:999px;padding:2px 9px;font-size:12px;font-weight:600}.identification-phone{background:#f1f3f4;color:#3c4043;border-radius:4px;padding:2px 6px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}.identification-evidence{margin:8px 0 0;padding-left:20px;color:#3c4043}.identification-evidence li{margin:3px 0}
@media (max-width:760px){main{padding:16px}.gmail-row{grid-template-columns:20px 20px minmax(0,1fr) 56px;gap:8px}.gmail-important,.gmail-list-labels{display:none}.gmail-sender{grid-column:3}.gmail-subject{grid-column:3 / 5;white-space:normal}.gmail-date{grid-column:4;grid-row:1}.gmail-expanded-subject{padding-left:18px;display:block}.gmail-message-summary{grid-template-columns:36px minmax(0,1fr);padding:18px}.gmail-message-body{padding-left:18px;padding-right:18px}.gmail-message-header{display:block}.gmail-message-header time{display:block;margin-top:4px}.gmail-body-frame{height:620px}.gmail-email-fields label,.gmail-email-readonly dl{grid-template-columns:1fr}.gmail-email-toolbar{flex-wrap:wrap}.contact-operation-main{grid-template-columns:40px minmax(0,1fr)}.contact-operation-main>.pill{grid-column:2}.contact-person-block dl{grid-template-columns:1fr}.contact-inline-row{grid-template-columns:1fr;gap:8px}.contact-inline-change{align-items:flex-start}}
</style></head><body>`, html.EscapeString(title))
}

func writeHTMLFooter(w http.ResponseWriter) {
	fmt.Fprint(w, `</body></html>`)
}
