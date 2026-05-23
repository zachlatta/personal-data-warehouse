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
	requests, err := s.store.ListRequests(r.Context(), RequestFilter{Statuses: []string{"pending_review"}, Limit: 100})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeHTMLHeader(w, "Pending Mutation Requests")
	fmt.Fprintf(w, `<main><header><h1>Pending Mutation Requests</h1><form method="post" action="%s/logout"><input type="hidden" name="csrf_token" value="%s"><button type="submit">Log Out</button></form></header>`, ReviewPath, html.EscapeString(session.CSRF))
	fmt.Fprint(w, `<table><thead><tr><th>Status</th><th>Request</th><th>Mutations</th><th>Created</th></tr></thead><tbody>`)
	for _, request := range requests {
		count := request.MutationCount
		if count == 0 {
			count = len(request.Mutations)
		}
		fmt.Fprintf(w, `<tr><td>%s</td><td><a href="%s/requests/%s">%s</a><div class="reason">%s</div></td><td>%d</td><td>%s</td></tr>`,
			html.EscapeString(request.Status),
			ReviewPath,
			url.PathEscape(request.ID),
			html.EscapeString(request.Title),
			html.EscapeString(request.Reason),
			count,
			html.EscapeString(formatTime(request.CreatedAt)),
		)
	}
	fmt.Fprint(w, `</tbody></table></main>`)
	writeHTMLFooter(w)
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
	for _, mutation := range request.Mutations {
		renderMutationArticle(w, mutation)
	}
	fmt.Fprint(w, `</section></main>`)
	writeHTMLFooter(w)
}

func renderMutationArticle(w http.ResponseWriter, mutation Mutation) {
	if isGmailThreadMutation(mutation) {
		renderGmailThreadMutation(w, mutation)
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

func renderGmailThreadMutation(w http.ResponseWriter, mutation Mutation) {
	verb := "Archive"
	if mutation.Operation == GmailUnarchiveOperation {
		verb = "Unarchive"
	}
	fmt.Fprintf(w, `<article class="mutation gmail-mutation"><div class="mutation-head"><div><p class="eyebrow">%s</p><h3>%s</h3></div><span class="pill">%s</span></div>`,
		html.EscapeString(verb),
		html.EscapeString(mutation.Title),
		html.EscapeString(mutation.Status),
	)
	if mutation.Reason != "" {
		fmt.Fprintf(w, `<p class="mutation-reason">%s</p>`, html.EscapeString(mutation.Reason))
	}
	fmt.Fprintf(w, `<p class="mutation-meta">%s for %s</p>`, html.EscapeString(mutation.Operation), html.EscapeString(mutation.Account))

	threads := mapSliceFromAny(mutation.Preview["threads"])
	if len(threads) == 0 {
		for _, threadID := range gmailMutationThreadIDs(mutation) {
			threads = append(threads, map[string]any{"thread_id": threadID})
		}
	}
	for _, thread := range threads {
		renderGmailThread(w, thread)
	}
	fmt.Fprintf(w, `<details class="raw-json"><summary>Raw mutation payload</summary><pre class="json-code">%s</pre></details>`, html.EscapeString(prettyJSON(mutation.Payload)))
	fmt.Fprint(w, `</article>`)
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
	labels := stringSliceFromAny(thread["labels"])

	fmt.Fprint(w, `<div class="gmail-thread">`)
	fmt.Fprint(w, `<div class="gmail-row">`)
	fmt.Fprint(w, `<span class="gmail-checkbox" aria-hidden="true"></span><span class="gmail-star" aria-hidden="true">&#9734;</span>`)
	fmt.Fprintf(w, `<div class="gmail-sender">%s</div>`, html.EscapeString(sender))
	fmt.Fprintf(w, `<div class="gmail-subject"><strong>%s</strong>`, html.EscapeString(subject))
	if latestPreview != "" {
		fmt.Fprintf(w, `<span> - %s</span>`, html.EscapeString(latestPreview))
	}
	fmt.Fprint(w, `</div>`)
	fmt.Fprintf(w, `<div class="gmail-date">%s</div>`, html.EscapeString(formatGmailCompactTime(latestAt)))
	fmt.Fprint(w, `</div>`)

	fmt.Fprint(w, `<div class="gmail-thread-meta">`)
	if messageCount > 0 {
		fmt.Fprintf(w, `<span>%d message%s</span>`, messageCount, plural(messageCount))
	}
	if inboxMessageCount > 0 {
		fmt.Fprintf(w, `<span>%d in inbox</span>`, inboxMessageCount)
	}
	for _, label := range labels {
		fmt.Fprintf(w, `<span class="gmail-label">%s</span>`, html.EscapeString(label))
	}
	if threadID != "" {
		fmt.Fprintf(w, `<span class="thread-id">%s</span>`, html.EscapeString(threadID))
	}
	fmt.Fprint(w, `</div>`)

	if len(messages) > 0 {
		fmt.Fprint(w, `<div class="gmail-messages">`)
		for _, message := range messages {
			renderGmailMessage(w, message)
		}
		fmt.Fprint(w, `</div>`)
	}
	fmt.Fprint(w, `</div>`)
}

func renderGmailMessage(w http.ResponseWriter, message map[string]any) {
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
	fmt.Fprint(w, `<div class="gmail-message">`)
	fmt.Fprintf(w, `<div class="gmail-avatar" aria-hidden="true">%s</div>`, html.EscapeString(senderInitial(from)))
	fmt.Fprint(w, `<div class="gmail-message-main">`)
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
		fmt.Fprintf(w, `<p>%s</p>`, html.EscapeString(preview))
	}
	fmt.Fprint(w, `</div></div>`)
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
.reason,.status,.mutation-meta{color:#5f6368}.error{color:#9b1c1c}.actions{display:inline-flex;gap:8px;margin-right:8px}
.mutation-head{display:flex;align-items:flex-start;justify-content:space-between;gap:16px}
.mutation-reason{margin:8px 0 10px;color:#3c4043}.eyebrow{margin:0 0 4px;color:#1a73e8;font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.04em}
.pill{background:#e8f0fe;color:#174ea6;border-radius:999px;padding:4px 9px;font-size:12px;font-weight:600;white-space:nowrap}
.gmail-mutation{padding:0;overflow:hidden}.gmail-mutation>.mutation-head,.gmail-mutation>.mutation-reason,.gmail-mutation>.mutation-meta,.gmail-mutation>.raw-json{margin-left:16px;margin-right:16px}.gmail-mutation>.mutation-head{margin-top:16px}
.gmail-thread{border-top:1px solid #e8eaed;margin-top:14px}.gmail-row{display:grid;grid-template-columns:24px 24px minmax(120px,190px) minmax(240px,1fr) 72px;align-items:center;gap:10px;min-height:48px;padding:0 16px;background:#fff}
.gmail-checkbox{width:14px;height:14px;border:2px solid #bdc1c6;border-radius:2px}.gmail-star{color:#9aa0a6;font-size:18px;line-height:1}.gmail-sender{font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.gmail-subject{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.gmail-subject span{color:#5f6368;font-weight:400}.gmail-date{text-align:right;color:#5f6368;font-size:12px;font-weight:600;white-space:nowrap}
.gmail-thread-meta{display:flex;flex-wrap:wrap;align-items:center;gap:8px;padding:0 16px 12px 74px;color:#5f6368;font-size:12px}.gmail-label{background:#fce8b2;color:#5f4520;border-radius:4px;padding:2px 6px;font-weight:600}.thread-id{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;color:#80868b}
.gmail-messages{padding:4px 16px 16px 74px}.gmail-message{display:grid;grid-template-columns:34px minmax(0,1fr);gap:12px;padding:12px 0;border-top:1px solid #f1f3f4}.gmail-avatar{width:32px;height:32px;border-radius:50%%;background:#188038;color:white;display:grid;place-items:center;font-weight:700}.gmail-message-header{display:flex;justify-content:space-between;gap:16px;align-items:flex-start}.gmail-message-header strong{display:block}.gmail-message-header time{color:#5f6368;font-size:12px;white-space:nowrap}.gmail-to{display:block;color:#5f6368;font-size:12px;margin-top:2px}.gmail-message p{margin:8px 0 0;color:#3c4043;line-height:1.45;white-space:pre-wrap}.raw-json{border-top:1px solid #e8eaed;padding-top:12px;margin-bottom:16px}.raw-json summary{color:#5f6368;cursor:pointer}
@media (max-width:760px){main{padding:16px}.gmail-row{grid-template-columns:20px 20px minmax(0,1fr) 56px;gap:8px}.gmail-sender{grid-column:3}.gmail-subject{grid-column:3 / 5;white-space:normal}.gmail-date{grid-column:4;grid-row:1}.gmail-thread-meta,.gmail-messages{padding-left:16px}.gmail-message-header{display:block}.gmail-message-header time{display:block;margin-top:4px}}
</style></head><body>`, html.EscapeString(title))
}

func writeHTMLFooter(w http.ResponseWriter) {
	fmt.Fprint(w, `</body></html>`)
}
