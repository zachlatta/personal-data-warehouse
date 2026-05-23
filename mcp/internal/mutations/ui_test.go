package mutations

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"
)

type reviewStore struct {
	requests []Request
	approved []string
	rejected []string
}

func (s *reviewStore) CreateRequest(context.Context, CreateRequestInput) (Request, error) {
	return Request{}, nil
}

func (s *reviewStore) ListRequests(context.Context, RequestFilter) ([]Request, error) {
	return s.requests, nil
}

func (s *reviewStore) GetRequest(_ context.Context, id string) (Request, error) {
	for _, request := range s.requests {
		if request.ID == id {
			return request, nil
		}
	}
	return Request{}, ErrNotFound
}

func (s *reviewStore) ApproveRequest(_ context.Context, id string, actor string) (Request, error) {
	s.approved = append(s.approved, id+":"+actor)
	for index := range s.requests {
		if s.requests[index].ID == id {
			s.requests[index].Status = "approved"
			return s.requests[index], nil
		}
	}
	return Request{}, ErrNotFound
}

func (s *reviewStore) RejectRequest(_ context.Context, id string, actor string, reason string) (Request, error) {
	s.rejected = append(s.rejected, id+":"+actor+":"+reason)
	for index := range s.requests {
		if s.requests[index].ID == id {
			s.requests[index].Status = "rejected"
			s.requests[index].Error = reason
			return s.requests[index], nil
		}
	}
	return Request{}, ErrNotFound
}

func TestReviewUIRequiresLoginAndApprovesRequest(t *testing.T) {
	store := &reviewStore{requests: []Request{{
		ID:        "req-123",
		Status:    "pending_review",
		Title:     "Archive stale mail",
		Reason:    "clear stale mail",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Mutations: []Mutation{{
			ID:        "mut-123",
			Provider:  "gmail",
			Operation: GmailArchiveOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Archive: Hello",
			Payload:   map[string]any{"thread_ids": []any{"thread-1"}},
			Preview:   map[string]any{"thread_count": 1},
		}},
	}}}
	service := NewService(store, Config{
		BaseURL:       "https://mcp.example.test",
		UIPassword:    "correct horse battery staple",
		SessionSecret: "0123456789abcdef0123456789abcdef",
		SessionTTL:    time.Hour,
		Now:           func() time.Time { return time.Unix(1700000000, 0).UTC() },
	})
	handler := service.HTTPHandler()

	unauthenticated := httptest.NewRecorder()
	handler.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodGet, "/mutation-review/requests", nil))
	if unauthenticated.Code != http.StatusSeeOther {
		t.Fatalf("unauthenticated status = %d", unauthenticated.Code)
	}
	if location := unauthenticated.Header().Get("Location"); !strings.HasPrefix(location, "/mutation-review/login") {
		t.Fatalf("unauthenticated redirect = %q", location)
	}

	loginForm := url.Values{"password": {"correct horse battery staple"}}
	loginResponse := httptest.NewRecorder()
	loginRequest := httptest.NewRequest(http.MethodPost, "/mutation-review/login", strings.NewReader(loginForm.Encode()))
	loginRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	handler.ServeHTTP(loginResponse, loginRequest)
	if loginResponse.Code != http.StatusSeeOther {
		t.Fatalf("login status = %d body=%q", loginResponse.Code, loginResponse.Body.String())
	}
	cookies := loginResponse.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("login did not set a session cookie")
	}

	detailResponse := httptest.NewRecorder()
	detailRequest := httptest.NewRequest(http.MethodGet, "/mutation-review/requests/req-123", nil)
	for _, cookie := range cookies {
		detailRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(detailResponse, detailRequest)
	if detailResponse.Code != http.StatusOK {
		t.Fatalf("detail status = %d body=%q", detailResponse.Code, detailResponse.Body.String())
	}
	body := detailResponse.Body.String()
	if !strings.Contains(body, "Archive stale mail") || !strings.Contains(body, "thread-1") {
		t.Fatalf("detail page missing request content: %q", body)
	}
	csrfToken := hiddenFieldValue(t, body, "csrf_token")

	approveForm := url.Values{"csrf_token": {csrfToken}}
	approveResponse := httptest.NewRecorder()
	approveRequest := httptest.NewRequest(http.MethodPost, "/mutation-review/requests/req-123/approve", strings.NewReader(approveForm.Encode()))
	approveRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for _, cookie := range cookies {
		approveRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(approveResponse, approveRequest)
	if approveResponse.Code != http.StatusSeeOther {
		t.Fatalf("approve status = %d body=%q", approveResponse.Code, approveResponse.Body.String())
	}
	if len(store.approved) != 1 || store.approved[0] != "req-123:web-ui" {
		t.Fatalf("approved calls = %#v", store.approved)
	}
}

func TestReviewUIRendersGmailThreadPreview(t *testing.T) {
	store := &reviewStore{requests: []Request{{
		ID:        "req-123",
		Status:    "pending_review",
		Title:     "Archive notification",
		Reason:    "automated notification",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Mutations: []Mutation{{
			ID:        "mut-123",
			Provider:  "gmail",
			Operation: GmailArchiveOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Archive: Receipt received",
			Payload:   map[string]any{"thread_ids": []any{"thread-1"}},
			Preview: map[string]any{
				"thread_count": 1,
				"threads": []any{map[string]any{
					"thread_id":           "thread-1",
					"subject":             "Receipt received",
					"latest_from_address": "HCB <receipts@hcb.example>",
					"latest_at":           "2026-05-20T09:30:00Z",
					"latest_preview":      "Everything is synced. No action is required.",
					"message_count":       2,
					"inbox_message_count": 2,
					"labels":              []any{"CATEGORY_UPDATES", "UNREAD"},
					"messages": []any{map[string]any{
						"message_id":    "message-older",
						"from_address":  "HCB <receipts@hcb.example>",
						"to_addresses":  []any{"zach@example.test"},
						"internal_date": "2026-05-19T12:00:00Z",
						"preview_text":  "We received your receipt and attached it to the transaction.",
						"label_ids":     []any{"INBOX", "CATEGORY_UPDATES"},
					}, map[string]any{
						"message_id":    "message-latest",
						"from_address":  "HCB <receipts@hcb.example>",
						"to_addresses":  []any{"zach@example.test"},
						"internal_date": "2026-05-20T09:30:00Z",
						"preview_text":  "Everything is synced. No action is required.",
						"label_ids":     []any{"INBOX", "UNREAD"},
					}},
				}},
			},
		}},
	}}}
	service := NewService(store, Config{
		BaseURL:       "https://mcp.example.test",
		UIPassword:    "correct horse battery staple",
		SessionSecret: "0123456789abcdef0123456789abcdef",
		SessionTTL:    time.Hour,
		Now:           func() time.Time { return time.Unix(1700000000, 0).UTC() },
	})
	handler := service.HTTPHandler()
	cookies := loginCookies(t, handler)

	detailResponse := httptest.NewRecorder()
	detailRequest := httptest.NewRequest(http.MethodGet, "/mutation-review/requests/req-123", nil)
	for _, cookie := range cookies {
		detailRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(detailResponse, detailRequest)

	if detailResponse.Code != http.StatusOK {
		t.Fatalf("detail status = %d body=%q", detailResponse.Code, detailResponse.Body.String())
	}
	body := detailResponse.Body.String()
	for _, want := range []string{
		`class="gmail-thread"`,
		"Receipt received",
		"HCB &lt;receipts@hcb.example&gt;",
		"Everything is synced. No action is required.",
		"2 messages",
		"CATEGORY_UPDATES",
		"zach@example.test",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("detail page missing %q: %q", want, body)
		}
	}
	if strings.Contains(body, `<pre>{`) {
		t.Fatalf("gmail preview should not lead with raw JSON: %q", body)
	}
}

func loginCookies(t *testing.T, handler http.Handler) []*http.Cookie {
	t.Helper()
	loginForm := url.Values{"password": {"correct horse battery staple"}}
	loginResponse := httptest.NewRecorder()
	loginRequest := httptest.NewRequest(http.MethodPost, "/mutation-review/login", strings.NewReader(loginForm.Encode()))
	loginRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	handler.ServeHTTP(loginResponse, loginRequest)
	if loginResponse.Code != http.StatusSeeOther {
		t.Fatalf("login status = %d body=%q", loginResponse.Code, loginResponse.Body.String())
	}
	cookies := loginResponse.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("login did not set a session cookie")
	}
	return cookies
}

func hiddenFieldValue(t *testing.T, body string, name string) string {
	t.Helper()
	pattern := regexp.MustCompile(`name="` + regexp.QuoteMeta(name) + `" value="([^"]+)"`)
	match := pattern.FindStringSubmatch(body)
	if len(match) != 2 {
		t.Fatalf("hidden field %q not found in %q", name, body)
	}
	return match[1]
}
