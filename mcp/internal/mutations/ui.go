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
	"strings"
	"time"
)

const sessionCookieName = "pdw_mutation_ui_session"

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
	fmt.Fprint(w, `</section><section><h2>Mutations</h2>`)
	renderMutationList(w, request.Mutations)
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

func renderMutationList(w http.ResponseWriter, mutations []Mutation) {
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
			renderMutationArticle(w, *item.Mutation)
		}
	}
}

func renderMutationArticle(w http.ResponseWriter, mutation Mutation) {
	fmt.Fprintf(w, `<article class="mutation"><h3>%s</h3><p class="mutation-meta">%s %s for %s</p><pre>%s</pre><pre>%s</pre></article>`,
		html.EscapeString(mutation.Title),
		html.EscapeString(mutation.Status),
		html.EscapeString(mutation.Operation),
		html.EscapeString(mutation.Account),
		html.EscapeString(prettyJSON(mutation.Payload)),
		html.EscapeString(prettyJSON(mutation.Preview)),
	)
}

func isContactMutation(mutation Mutation) bool {
	return mutation.Provider == "google_people" || mutation.Operation == ContactsBatchMutationOperation || mutation.Operation == GooglePeopleContactsOperation
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
	op := strings.TrimSpace(stringFromAny(operation["op"]))
	summary := contactOperationSummary(operation)
	title := contactOperationTitle(op)
	classes := "contact-operation"
	if op == "delete_contact" {
		classes += " destructive"
	}
	fmt.Fprintf(w, `<div class="%s">`, html.EscapeString(classes))
	fmt.Fprint(w, `<div class="contact-operation-main">`)
	fmt.Fprintf(w, `<div class="contact-avatar" aria-hidden="true">%s</div>`, html.EscapeString(senderInitial(summary.DisplayName)))
	fmt.Fprint(w, `<div class="contact-operation-copy">`)
	fmt.Fprintf(w, `<p class="contact-op">%s</p>`, html.EscapeString(title))
	fmt.Fprintf(w, `<h4>%s</h4>`, html.EscapeString(contactSummaryTitle(summary, operation)))
	renderContactSummaryMeta(w, summary)
	renderContactOperationEffect(w, operation)
	fmt.Fprint(w, `</div>`)
	fmt.Fprintf(w, `<span class="pill">%s</span>`, html.EscapeString(view.Mutation.Status))
	fmt.Fprint(w, `</div>`)
	renderContactOperationBody(w, operation)
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
	return contactSummary{}
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

func renderContactOperationEffect(w http.ResponseWriter, operation map[string]any) {
	op := strings.TrimSpace(stringFromAny(operation["op"]))
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

func renderContactOperationBody(w http.ResponseWriter, operation map[string]any) {
	op := strings.TrimSpace(stringFromAny(operation["op"]))
	switch op {
	case "update_contact":
		renderContactUpdateDiff(w, operation)
	case "create_contact":
		renderContactPersonBlock(w, "Contact to create", mapFromAny(operation["person"]))
	case "delete_contact":
		renderContactPersonBlock(w, "Contact to delete", mapFromAny(operation["before"]))
	}
}

func renderContactUpdateDiff(w http.ResponseWriter, operation map[string]any) {
	fields := contactUpdateFields(operation)
	before := mapFromAny(operation["before"])
	after := mapFromAny(operation["after"])
	fmt.Fprint(w, `<p class="muted">Fields not listed here are not part of this update.</p>`)
	fmt.Fprint(w, `<table class="contact-diff"><thead><tr><th>Field</th><th>Before</th><th>After</th></tr></thead><tbody>`)
	for _, field := range fields {
		fmt.Fprintf(w, `<tr><td><code>%s</code></td><td>%s</td><td>%s</td></tr>`,
			html.EscapeString(field),
			renderContactFieldValue(before[field]),
			renderContactFieldValue(after[field]),
		)
	}
	if len(fields) == 0 {
		fmt.Fprint(w, `<tr><td colspan="3">No explicit update fields were provided.</td></tr>`)
	}
	fmt.Fprint(w, `</tbody></table>`)
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

func renderContactFieldValue(value any) string {
	text := contactFieldValueSummary(value)
	if text == "" {
		text = "Not set"
	}
	return `<span class="contact-field-value">` + html.EscapeString(text) + `</span>`
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

func renderGmailThread(w http.ResponseWriter, thread map[string]any) {
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

	fmt.Fprint(w, `<details class="gmail-thread">`)
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
		fmt.Fprintf(w, `<iframe class="gmail-body-frame" sandbox referrerpolicy="no-referrer" srcdoc="%s"></iframe>`, html.EscapeString(emailHTMLDocument(bodyHTML)))
		if quotedHTML != "" {
			fmt.Fprintf(w, `<details class="gmail-quoted"><summary>Quoted message</summary><iframe class="gmail-body-frame quoted" sandbox referrerpolicy="no-referrer" srcdoc="%s"></iframe></details>`, html.EscapeString(emailHTMLDocument(quotedHTML)))
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
.gmail-messages{padding:0 0 18px}.gmail-message{border-top:1px solid #f1f3f4}.gmail-message summary{list-style:none}.gmail-message summary::-webkit-details-marker{display:none}.gmail-message-summary{display:grid;grid-template-columns:48px minmax(0,1fr);gap:14px;padding:18px 28px;cursor:pointer}.gmail-message-summary:hover{background:#f8fafd}.gmail-avatar{width:40px;height:40px;border-radius:50%%;background:#5e7ce2;color:white;display:grid;place-items:center;font-weight:700;font-size:18px}.gmail-message-summary-main{min-width:0}.gmail-message-header{display:flex;justify-content:space-between;gap:16px;align-items:flex-start}.gmail-message-header strong{display:inline;font-size:15px}.gmail-message-header time{color:#5f6368;font-size:13px;white-space:nowrap}.gmail-to{display:block;color:#5f6368;font-size:12px;margin-top:3px}.gmail-message-preview{margin:8px 0 0;color:#5f6368;line-height:1.4;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.gmail-message[open] .gmail-message-preview{display:none}.gmail-message-body{padding:0 28px 22px 90px}.gmail-message-body p{margin:8px 0 0;color:#3c4043;line-height:1.45;white-space:pre-wrap}.gmail-body-frame{display:block;width:100%%;height:760px;border:0;background:white}.gmail-quoted{margin-top:14px;border-left:3px solid #dadce0;padding-left:12px}.gmail-quoted>summary{color:#5f6368;cursor:pointer;font-size:13px;margin-bottom:8px}.gmail-body-frame.quoted{height:520px}.raw-json{border-top:1px solid #e8eaed;padding-top:12px;margin-bottom:16px}.raw-json summary{color:#5f6368;cursor:pointer}
.contact-mutation{padding:0;overflow:hidden}.contact-mutation>.mutation-head,.contact-mutation>.mutation-meta{margin-left:16px;margin-right:16px}.contact-mutation>.mutation-head{margin-top:16px}.contact-operations{border-top:1px solid #dfe1e5;margin-top:16px}.contact-operation{border-top:1px solid #f1f3f4;padding:18px 16px}.contact-operation:first-child{border-top:0}.contact-operation.destructive{box-shadow:inset 3px 0 #c5221f}.contact-operation-main{display:grid;grid-template-columns:44px minmax(0,1fr) auto;gap:14px;align-items:flex-start}.contact-avatar{width:40px;height:40px;border-radius:50%%;background:#137333;color:white;display:grid;place-items:center;font-weight:700;font-size:18px}.contact-operation.destructive .contact-avatar{background:#c5221f}.contact-operation-copy{min-width:0}.contact-operation-copy h4{margin:2px 0 4px;font-size:18px}.contact-op{margin:0;color:#1a73e8;font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.04em}.contact-meta{display:flex;flex-wrap:wrap;gap:8px;color:#5f6368;font-size:13px}.contact-effect,.contact-resource{margin:8px 0 0;color:#3c4043}.contact-effect code,.contact-resource code{background:#f1f3f4;border-radius:4px;padding:2px 5px}.contact-diff{margin-top:12px}.contact-diff th,.contact-diff td{font-size:13px}.contact-field-value{display:block;line-height:1.4}.contact-person-block{margin-top:12px;border-top:1px solid #e8eaed;padding-top:12px}.contact-block-title{margin:0 0 8px;font-weight:700}.contact-person-block dl{display:grid;grid-template-columns:120px minmax(0,1fr);gap:6px 12px;margin:0}.contact-person-block dt{color:#5f6368}.contact-person-block dd{margin:0}.contact-operation .raw-json{margin-bottom:0}
@media (max-width:760px){main{padding:16px}.gmail-row{grid-template-columns:20px 20px minmax(0,1fr) 56px;gap:8px}.gmail-important,.gmail-list-labels{display:none}.gmail-sender{grid-column:3}.gmail-subject{grid-column:3 / 5;white-space:normal}.gmail-date{grid-column:4;grid-row:1}.gmail-expanded-subject{padding-left:18px;display:block}.gmail-message-summary{grid-template-columns:36px minmax(0,1fr);padding:18px}.gmail-message-body{padding-left:18px;padding-right:18px}.gmail-message-header{display:block}.gmail-message-header time{display:block;margin-top:4px}.gmail-body-frame{height:620px}.contact-operation-main{grid-template-columns:40px minmax(0,1fr)}.contact-operation-main>.pill{grid-column:2}.contact-person-block dl{grid-template-columns:1fr}.contact-diff{display:block;overflow-x:auto}}
</style></head><body>`, html.EscapeString(title))
}

func writeHTMLFooter(w http.ResponseWriter) {
	fmt.Fprint(w, `</body></html>`)
}
