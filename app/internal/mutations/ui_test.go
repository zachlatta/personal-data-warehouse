package mutations

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"
)

type reviewStore struct {
	requests     []Request
	approved     []string
	rejected     []string
	emailUpdates []emailUpdateCall
	removed      []removeMutationCall
}

type removeMutationCall struct {
	RequestID  string
	MutationID string
	Actor      string
}

type emailUpdateCall struct {
	RequestID  string
	MutationID string
	Input      UpdateGmailEmailMutationInput
	Actor      string
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

func (s *reviewStore) UpdateGmailEmailMutation(_ context.Context, requestID string, mutationID string, input UpdateGmailEmailMutationInput, actor string) (Mutation, error) {
	s.emailUpdates = append(s.emailUpdates, emailUpdateCall{RequestID: requestID, MutationID: mutationID, Input: input, Actor: actor})
	for requestIndex := range s.requests {
		if s.requests[requestIndex].ID != requestID {
			continue
		}
		for mutationIndex := range s.requests[requestIndex].Mutations {
			mutation := &s.requests[requestIndex].Mutations[mutationIndex]
			if mutation.ID != mutationID {
				continue
			}
			mutation.Payload["delivery_mode"] = input.DeliveryMode
			mutation.Payload["message"] = cloneMap(input.Message)
			if input.SelectedVariantID != "" {
				mutation.Payload["selected_variant_id"] = input.SelectedVariantID
			}
			previewEmail := cloneMap(input.Message)
			previewEmail["delivery_mode"] = input.DeliveryMode
			if input.SelectedVariantID != "" {
				previewEmail["selected_variant_id"] = input.SelectedVariantID
			}
			mutation.Preview["email"] = previewEmail
			return *mutation, nil
		}
	}
	return Mutation{}, ErrNotFound
}

func (s *reviewStore) RemoveMutation(_ context.Context, requestID string, mutationID string, actor string) (Mutation, error) {
	s.removed = append(s.removed, removeMutationCall{RequestID: requestID, MutationID: mutationID, Actor: actor})
	for requestIndex := range s.requests {
		if s.requests[requestIndex].ID != requestID {
			continue
		}
		remaining := 0
		for _, mutation := range s.requests[requestIndex].Mutations {
			if mutation.ID != mutationID && mutation.Status == "pending_review" {
				remaining++
			}
		}
		if remaining == 0 {
			return Mutation{}, errors.New("cannot remove every pending mutation from a request; deny the request instead")
		}
		for mutationIndex := range s.requests[requestIndex].Mutations {
			mutation := &s.requests[requestIndex].Mutations[mutationIndex]
			if mutation.ID != mutationID {
				continue
			}
			mutation.Status = "rejected"
			mutation.Error = "removed during review"
			return *mutation, nil
		}
	}
	return Mutation{}, ErrNotFound
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

func TestReviewUIListShowsPendingAndPastRequests(t *testing.T) {
	store := &reviewStore{requests: []Request{
		{
			ID:            "req-pending",
			Status:        "pending_review",
			Title:         "Archive stale mail",
			Reason:        "needs review",
			MutationCount: 2,
			CreatedAt:     time.Unix(1700000300, 0).UTC(),
		},
		{
			ID:            "req-approved",
			Status:        "approved",
			Title:         "Send follow-up",
			Reason:        "already approved",
			MutationCount: 1,
			CreatedAt:     time.Unix(1700000200, 0).UTC(),
		},
		{
			ID:            "req-rejected",
			Status:        "rejected",
			Title:         "Archive important mail",
			Reason:        "denied by reviewer",
			MutationCount: 1,
			CreatedAt:     time.Unix(1700000100, 0).UTC(),
		},
	}}
	service := NewService(store, Config{
		BaseURL:       "https://mcp.example.test",
		UIPassword:    "correct horse battery staple",
		SessionSecret: "0123456789abcdef0123456789abcdef",
		SessionTTL:    time.Hour,
		Now:           func() time.Time { return time.Unix(1700000000, 0).UTC() },
	})
	handler := service.HTTPHandler()
	cookies := loginCookies(t, handler)

	listResponse := httptest.NewRecorder()
	listRequest := httptest.NewRequest(http.MethodGet, "/mutation-review/requests", nil)
	for _, cookie := range cookies {
		listRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(listResponse, listRequest)

	if listResponse.Code != http.StatusOK {
		t.Fatalf("list status = %d body=%q", listResponse.Code, listResponse.Body.String())
	}
	body := listResponse.Body.String()
	for _, want := range []string{
		"Mutation Requests",
		"Pending Review",
		"Past Requests",
		"Archive stale mail",
		"Send follow-up",
		"Archive important mail",
		"approved",
		"denied",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("list page missing %q: %q", want, body)
		}
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
			Reason:    "automated notification",
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
						"body_html":     `<main><h1>Receipt body</h1></main><div class="gmail_quote gmail_quote_container"><blockquote class="gmail_quote">Parent body</blockquote></div>`,
						"label_ids":     []any{"INBOX", "CATEGORY_UPDATES"},
					}, map[string]any{
						"message_id":    "message-latest",
						"from_address":  "HCB <receipts@hcb.example>",
						"to_addresses":  []any{"zach@example.test"},
						"internal_date": "2026-05-20T09:30:00Z",
						"preview_text":  "Everything is synced. No action is required.",
						"body_html":     "<main><h1>Latest body</h1></main>",
						"label_ids":     []any{"INBOX", "UNREAD"},
					}},
				}},
			},
		}, {
			ID:        "mut-456",
			Provider:  "gmail",
			Operation: GmailArchiveOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Archive: Shipment delivered",
			Reason:    "automated notification",
			Payload:   map[string]any{"thread_ids": []any{"thread-2"}},
			Preview: map[string]any{
				"thread_count": 1,
				"threads": []any{map[string]any{
					"thread_id":           "thread-2",
					"subject":             "Shipment delivered",
					"latest_from_address": "dinobox <shipments@dinobox.example>",
					"latest_at":           "2026-05-21T10:00:00Z",
					"latest_preview":      "Your order has shipped.",
					"message_count":       1,
					"inbox_message_count": 1,
					"labels":              []any{"CATEGORY_UPDATES"},
					"messages": []any{map[string]any{
						"message_id":    "message-shipment",
						"from_address":  "dinobox <shipments@dinobox.example>",
						"to_addresses":  []any{"zach@example.test"},
						"internal_date": "2026-05-21T10:00:00Z",
						"preview_text":  "Your order has shipped.",
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
		"Archive 2 Gmail threads",
		"Removes these threads from the Inbox.",
		`<details class="gmail-thread">`,
		`<summary class="gmail-row">`,
		`class="gmail-body-frame"`,
		`class="gmail-quoted"`,
		"Quoted message",
		"Receipt received",
		"Shipment delivered",
		"HCB &lt;receipts@hcb.example&gt;",
		"Everything is synced. No action is required.",
		"2 messages",
		"Inbox",
		"Updates",
		"zach@example.test",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("detail page missing %q: %q", want, body)
		}
	}
	if got := strings.Count(body, `<details class="gmail-thread">`); got != 2 {
		t.Fatalf("gmail thread row count = %d body=%q", got, body)
	}
	if got := strings.Count(body, `<details class="gmail-message">`); got != 2 {
		t.Fatalf("read gmail message count = %d body=%q", got, body)
	}
	if got := strings.Count(body, `<details class="gmail-message" open>`); got != 1 {
		t.Fatalf("unread gmail message open count = %d body=%q", got, body)
	}
	if got := strings.Count(body, `class="mutation gmail-mutation"`); got != 1 {
		t.Fatalf("gmail mutation group count = %d body=%q", got, body)
	}
	if got := strings.Count(body, "automated notification"); got != 1 {
		t.Fatalf("request reason should only render once, got %d body=%q", got, body)
	}
	if strings.Contains(body, `<pre>{`) {
		t.Fatalf("gmail preview should not lead with raw JSON: %q", body)
	}
	if strings.Contains(body, "gmail-toolbar") || strings.Contains(body, "&#128229;") {
		t.Fatalf("gmail preview should not render fake toolbar buttons: %q", body)
	}
	if strings.Contains(body, "background:#f2f6fc") || strings.Contains(body, "inset 3px 0 #1a73e8") {
		t.Fatalf("gmail thread rows should not use selected blue styling by default: %q", body)
	}
}

func TestReviewUIRendersGmailUnarchiveThreadPreview(t *testing.T) {
	store := &reviewStore{requests: []Request{{
		ID:        "req-unarchive",
		Status:    "pending_review",
		Title:     "Restore client thread",
		Reason:    "needs to return to inbox",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Mutations: []Mutation{{
			ID:        "mut-unarchive",
			Provider:  "gmail",
			Operation: GmailUnarchiveOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Unarchive: Client follow-up",
			Reason:    "needs to return to inbox",
			Payload:   map[string]any{"thread_ids": []any{"thread-unarchive"}},
			Preview: map[string]any{
				"thread_count": 1,
				"threads": []any{map[string]any{
					"thread_id":           "thread-unarchive",
					"subject":             "Client follow-up",
					"latest_from_address": "Alex <alex@example.test>",
					"latest_at":           "2026-05-22T15:15:00Z",
					"latest_preview":      "Can we bring this back into focus next week?",
					"message_count":       1,
					"inbox_message_count": 0,
					"labels":              []any{"CATEGORY_UPDATES"},
					"messages": []any{map[string]any{
						"message_id":    "message-unarchive",
						"from_address":  "Alex <alex@example.test>",
						"to_addresses":  []any{"zach@example.test"},
						"internal_date": "2026-05-22T15:15:00Z",
						"preview_text":  "Can we bring this back into focus next week?",
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
	detailRequest := httptest.NewRequest(http.MethodGet, "/mutation-review/requests/req-unarchive", nil)
	for _, cookie := range cookies {
		detailRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(detailResponse, detailRequest)

	if detailResponse.Code != http.StatusOK {
		t.Fatalf("detail status = %d body=%q", detailResponse.Code, detailResponse.Body.String())
	}
	body := detailResponse.Body.String()
	for _, want := range []string{
		"Unarchive 1 Gmail thread",
		"Restores this thread to the Inbox.",
		"gmail.unarchive_threads for zach@example.test",
		"Client follow-up",
		"Alex &lt;alex@example.test&gt;",
		"Can we bring this back into focus next week?",
		"Updates",
		`<details class="gmail-thread">`,
		`<summary class="gmail-row">`,
		`class="mutation gmail-mutation"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("unarchive detail page missing %q: %q", want, body)
		}
	}
	if strings.Contains(body, `<pre>{`) {
		t.Fatalf("unarchive preview should not lead with raw JSON: %q", body)
	}
}

func TestReviewUIRendersGmailSendEmailEditor(t *testing.T) {
	store := &reviewStore{requests: []Request{gmailEmailReviewRequest()}}
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
	detailRequest := httptest.NewRequest(http.MethodGet, "/mutation-review/requests/req-email", nil)
	for _, cookie := range cookies {
		detailRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(detailResponse, detailRequest)

	if detailResponse.Code != http.StatusOK {
		t.Fatalf("detail status = %d body=%q", detailResponse.Code, detailResponse.Body.String())
	}
	body := detailResponse.Body.String()
	for _, want := range []string{
		`class="mutation gmail-email-mutation"`,
		"Send Gmail email",
		"Will send this email after approval.",
		`data-tiptap-editor`,
		`@tiptap/core@3.23.6`,
		`@tiptap/extension-link@3.23.6`,
		`@tiptap/extension-text-style@3.23.6`,
		`name="delivery_mode" value="send" checked`,
		`name="delivery_mode" value="draft"`,
		"Create draft",
		"one@example.test",
		"cc@example.test",
		"secret@example.test",
		"Original subject",
		"Original body",
		`name="reply_to_thread_id" value="thread-reply"`,
		`name="in_reply_to" value="&lt;message@example.test&gt;"`,
		`name="body_html"`,
		`data-initial-html="&lt;p&gt;Original body&lt;/p&gt;"`,
		`name="quoted_html"`,
		"Quoted thread",
		"Prior note",
		`class="gmail-email-reply-context"`,
		"Replying in thread",
		`<details class="gmail-thread" open>`,
		`<summary class="gmail-row">`,
		`class="gmail-inline-reply"`,
		"Existing customer thread",
		"Customer &lt;customer@example.test&gt;",
		"Can you take a look?",
		"Save email changes",
		"Save as draft instead",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("email editor page missing %q: %q", want, body)
		}
	}
	if strings.Contains(body, `<pre>{`) {
		t.Fatalf("email editor should not lead with raw JSON: %q", body)
	}
	if strings.Index(body, `class="gmail-message-body"`) > strings.Index(body, `class="gmail-inline-reply"`) {
		t.Fatalf("inline reply composer should render after existing messages: %q", body)
	}
}

func TestReviewUIRendersGmailSendEmailVariantTabs(t *testing.T) {
	store := &reviewStore{requests: []Request{gmailEmailVariantReviewRequest()}}
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
	detailRequest := httptest.NewRequest(http.MethodGet, "/mutation-review/requests/req-email-variants", nil)
	for _, cookie := range cookies {
		detailRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(detailResponse, detailRequest)

	if detailResponse.Code != http.StatusOK {
		t.Fatalf("detail status = %d body=%q", detailResponse.Code, detailResponse.Body.String())
	}
	body := detailResponse.Body.String()
	for _, want := range []string{
		`class="gmail-email-tabs"`,
		`data-email-variant-tab="0"`,
		`data-email-variant-tab="1"`,
		"Direct Reply",
		"Softer Ask",
		`name="selected_variant_id" value="variant_1"`,
		`name="selected_variant_id" value="variant_2"`,
		"Selected proposal: <strong>Direct Reply</strong>",
		"Selected proposal: <strong>Softer Ask</strong>",
		"Direct body",
		"Softer body",
		"Use this version",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("variant email editor page missing %q: %q", want, body)
		}
	}
}

func TestReviewUIUpdatesSelectedGmailEmailVariant(t *testing.T) {
	store := &reviewStore{requests: []Request{gmailEmailVariantReviewRequest()}}
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
	detailRequest := httptest.NewRequest(http.MethodGet, "/mutation-review/requests/req-email-variants", nil)
	for _, cookie := range cookies {
		detailRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(detailResponse, detailRequest)
	csrfToken := hiddenFieldValue(t, detailResponse.Body.String(), "csrf_token")

	updateForm := url.Values{
		"csrf_token":          {csrfToken},
		"selected_variant_id": {"variant_2"},
		"delivery_mode":       {"send"},
		"to":                  {"one@example.test"},
		"subject":             {"Softer subject"},
		"body_text":           {"Edited softer body"},
		"body_html":           {"<p>Edited softer body</p>"},
	}
	updateResponse := httptest.NewRecorder()
	updateRequest := httptest.NewRequest(http.MethodPost, "/mutation-review/requests/req-email-variants/mutations/mut-email-variants/update-email", strings.NewReader(updateForm.Encode()))
	updateRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for _, cookie := range cookies {
		updateRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(updateResponse, updateRequest)

	if updateResponse.Code != http.StatusSeeOther {
		t.Fatalf("update status = %d body=%q", updateResponse.Code, updateResponse.Body.String())
	}
	if len(store.emailUpdates) != 1 {
		t.Fatalf("email updates = %#v", store.emailUpdates)
	}
	update := store.emailUpdates[0]
	if update.Input.SelectedVariantID != "variant_2" {
		t.Fatalf("selected variant = %q", update.Input.SelectedVariantID)
	}
	if update.Input.Message["subject"] != "Softer subject" || update.Input.Message["body_text"] != "Edited softer body" {
		t.Fatalf("message = %#v", update.Input.Message)
	}
}

func TestReviewUIUpdatesGmailSendEmailMutation(t *testing.T) {
	store := &reviewStore{requests: []Request{gmailEmailReviewRequest()}}
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
	detailRequest := httptest.NewRequest(http.MethodGet, "/mutation-review/requests/req-email", nil)
	for _, cookie := range cookies {
		detailRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(detailResponse, detailRequest)
	csrfToken := hiddenFieldValue(t, detailResponse.Body.String(), "csrf_token")

	updateForm := url.Values{
		"csrf_token":         {csrfToken},
		"delivery_mode":      {"send"},
		"set_delivery_mode":  {"draft"},
		"to":                 {"two@example.test, three@example.test"},
		"cc":                 {"edited-cc@example.test"},
		"bcc":                {"edited-bcc@example.test"},
		"subject":            {"Edited subject"},
		"body_text":          {"Edited plain body"},
		"body_html":          {"<p><strong>Edited HTML body</strong></p>"},
		"reply_to_thread_id": {"thread-reply"},
		"in_reply_to":        {"<message@example.test>"},
		"references":         {"<message@example.test>"},
	}
	updateResponse := httptest.NewRecorder()
	updateRequest := httptest.NewRequest(http.MethodPost, "/mutation-review/requests/req-email/mutations/mut-email/update-email", strings.NewReader(updateForm.Encode()))
	updateRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for _, cookie := range cookies {
		updateRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(updateResponse, updateRequest)

	if updateResponse.Code != http.StatusSeeOther {
		t.Fatalf("update status = %d body=%q", updateResponse.Code, updateResponse.Body.String())
	}
	if location := updateResponse.Header().Get("Location"); location != "/mutation-review/requests/req-email" {
		t.Fatalf("update redirect = %q", location)
	}
	if len(store.emailUpdates) != 1 {
		t.Fatalf("email updates = %#v", store.emailUpdates)
	}
	update := store.emailUpdates[0]
	if update.RequestID != "req-email" || update.MutationID != "mut-email" || update.Actor != "web-ui" {
		t.Fatalf("unexpected update call = %#v", update)
	}
	if update.Input.DeliveryMode != "draft" {
		t.Fatalf("delivery mode = %q", update.Input.DeliveryMode)
	}
	message := update.Input.Message
	if strings.Join(stringSliceFromAny(message["to"]), ",") != "two@example.test,three@example.test" {
		t.Fatalf("to = %#v", message["to"])
	}
	if message["subject"] != "Edited subject" || message["body_text"] != "Edited plain body" || message["body_html"] != "<p><strong>Edited HTML body</strong></p>" {
		t.Fatalf("message = %#v", message)
	}
	if message["reply_to_thread_id"] != "thread-reply" || message["in_reply_to"] != "<message@example.test>" {
		t.Fatalf("reply metadata = %#v", message)
	}
}

func TestReviewUIRemovesGmailEmailMutation(t *testing.T) {
	request := gmailEmailReviewRequest()
	request.Mutations = append(request.Mutations, Mutation{
		ID:        "mut-email-second",
		RequestID: request.ID,
		Provider:  "gmail",
		Operation: GmailSendEmailOperation,
		Account:   "zach@example.test",
		Status:    "pending_review",
		Title:     "Send email: Second",
		Payload: map[string]any{
			"delivery_mode": "send",
			"message": map[string]any{
				"to":        []any{"other@example.test"},
				"subject":   "Second",
				"body_text": "Second body",
			},
		},
		Preview: map[string]any{},
	})
	store := &reviewStore{requests: []Request{request}}
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
	detailRequest := httptest.NewRequest(http.MethodGet, "/mutation-review/requests/req-email", nil)
	for _, cookie := range cookies {
		detailRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(detailResponse, detailRequest)
	body := detailResponse.Body.String()
	if !strings.Contains(body, `/mutations/mut-email/remove`) {
		t.Fatalf("detail page missing remove form action: %q", body)
	}
	if !strings.Contains(body, "Don't send") {
		t.Fatalf("detail page missing Don't send button: %q", body)
	}
	csrfToken := hiddenFieldValue(t, body, "csrf_token")

	removeForm := url.Values{"csrf_token": {csrfToken}}
	removeResponse := httptest.NewRecorder()
	removeRequest := httptest.NewRequest(http.MethodPost, "/mutation-review/requests/req-email/mutations/mut-email/remove", strings.NewReader(removeForm.Encode()))
	removeRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for _, cookie := range cookies {
		removeRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(removeResponse, removeRequest)

	if removeResponse.Code != http.StatusSeeOther {
		t.Fatalf("remove status = %d body=%q", removeResponse.Code, removeResponse.Body.String())
	}
	if location := removeResponse.Header().Get("Location"); location != "/mutation-review/requests/req-email" {
		t.Fatalf("remove redirect = %q", location)
	}
	if len(store.removed) != 1 {
		t.Fatalf("removed calls = %#v", store.removed)
	}
	call := store.removed[0]
	if call.RequestID != "req-email" || call.MutationID != "mut-email" || call.Actor != "web-ui" {
		t.Fatalf("unexpected remove call = %#v", call)
	}
	if store.requests[0].Mutations[0].Status != "rejected" {
		t.Fatalf("first mutation status = %q", store.requests[0].Mutations[0].Status)
	}
	if store.requests[0].Mutations[0].Error != "removed during review" {
		t.Fatalf("first mutation error = %q", store.requests[0].Mutations[0].Error)
	}
}

func TestReviewUIRejectsRemovingLastPendingMutation(t *testing.T) {
	store := &reviewStore{requests: []Request{gmailEmailReviewRequest()}}
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
	detailRequest := httptest.NewRequest(http.MethodGet, "/mutation-review/requests/req-email", nil)
	for _, cookie := range cookies {
		detailRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(detailResponse, detailRequest)
	csrfToken := hiddenFieldValue(t, detailResponse.Body.String(), "csrf_token")

	removeForm := url.Values{"csrf_token": {csrfToken}}
	removeResponse := httptest.NewRecorder()
	removeRequest := httptest.NewRequest(http.MethodPost, "/mutation-review/requests/req-email/mutations/mut-email/remove", strings.NewReader(removeForm.Encode()))
	removeRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for _, cookie := range cookies {
		removeRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(removeResponse, removeRequest)

	if removeResponse.Code != http.StatusOK {
		t.Fatalf("remove status = %d body=%q", removeResponse.Code, removeResponse.Body.String())
	}
	if !strings.Contains(removeResponse.Body.String(), "cannot remove every pending mutation") {
		t.Fatalf("expected error message, got %q", removeResponse.Body.String())
	}
	if store.requests[0].Mutations[0].Status != "pending_review" {
		t.Fatalf("mutation should remain pending, got %q", store.requests[0].Mutations[0].Status)
	}
}

func TestRenderGmailThreadOpensUnreadMessagesOnly(t *testing.T) {
	response := httptest.NewRecorder()

	renderGmailThread(response, map[string]any{
		"thread_id":           "thread-unread",
		"subject":             "Unread thread",
		"latest_from_address": "Notifier <notify@example.test>",
		"latest_at":           "2026-05-23T12:00:00Z",
		"message_count":       2,
		"labels":              []any{"Unread"},
		"messages": []any{map[string]any{
			"message_id":    "read-message",
			"from_address":  "Notifier <notify@example.test>",
			"internal_date": "2026-05-22T12:00:00Z",
			"preview_text":  "Already read",
			"label_ids":     []any{"INBOX"},
		}, map[string]any{
			"message_id":    "unread-message",
			"from_address":  "Notifier <notify@example.test>",
			"internal_date": "2026-05-23T12:00:00Z",
			"preview_text":  "Needs review",
			"label_ids":     []any{"unread"},
		}},
	})

	body := response.Body.String()
	if got := strings.Count(body, `<details class="gmail-message" open>`); got != 1 {
		t.Fatalf("unread open count = %d body=%q", got, body)
	}
	if got := strings.Count(body, `<details class="gmail-message">`); got != 1 {
		t.Fatalf("read collapsed count = %d body=%q", got, body)
	}
}

func TestRenderGmailThreadOpensLatestMessageWhenOnlyThreadUnreadIsKnown(t *testing.T) {
	response := httptest.NewRecorder()

	renderGmailThread(response, map[string]any{
		"thread_id":           "thread-unread",
		"subject":             "Unread thread",
		"latest_from_address": "Notifier <notify@example.test>",
		"latest_at":           "2026-05-23T12:00:00Z",
		"message_count":       2,
		"labels":              []any{"UNREAD"},
		"messages": []any{map[string]any{
			"message_id":    "older-message",
			"from_address":  "Notifier <notify@example.test>",
			"internal_date": "2026-05-22T12:00:00Z",
			"preview_text":  "Older message",
		}, map[string]any{
			"message_id":    "latest-message",
			"from_address":  "Notifier <notify@example.test>",
			"internal_date": "2026-05-23T12:00:00Z",
			"preview_text":  "Latest message",
		}},
	})

	body := response.Body.String()
	if got := strings.Count(body, `<details class="gmail-message" open>`); got != 1 {
		t.Fatalf("fallback unread open count = %d body=%q", got, body)
	}
	if got := strings.Count(body, `<details class="gmail-message">`); got != 1 {
		t.Fatalf("fallback read collapsed count = %d body=%q", got, body)
	}
}

func TestReviewUIRendersContactMutationPreview(t *testing.T) {
	store := &reviewStore{requests: []Request{{
		ID:        "req-contact",
		Status:    "pending_review",
		Title:     "Clean up contacts",
		Reason:    "dedupe stale contacts",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Mutations: []Mutation{{
			ID:        "mut-update",
			Provider:  "google_people",
			Operation: ContactsBatchMutationOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Update contact: Ada Lovelace",
			Payload: map[string]any{"operations": []any{map[string]any{
				"op":                   "update_contact",
				"resource_name":        "people/ada",
				"update_person_fields": []any{"names", "emailAddresses", "organizations"},
			}}},
			Preview: map[string]any{"operations": []any{map[string]any{
				"op":                   "update_contact",
				"op_index":             0,
				"resource_name":        "people/ada",
				"update_person_fields": []any{"names", "emailAddresses", "organizations"},
				"summary": map[string]any{
					"display_name":  "Ada Lovelace",
					"primary_email": "ada@example.test",
					"organization":  "Analytical Engines",
				},
				"before": map[string]any{
					"names":          []any{map[string]any{"displayName": "Ada Old"}},
					"emailAddresses": []any{map[string]any{"value": "ada.old@example.test"}},
					"organizations":  []any{map[string]any{"name": "Old Lab", "title": "Old Role"}},
				},
				"after": map[string]any{
					"names":          []any{map[string]any{"displayName": "Ada Lovelace"}},
					"emailAddresses": []any{map[string]any{"value": "ada@example.test"}},
					"organizations":  []any{map[string]any{"name": "Analytical Engines", "title": "Computer Scientist"}},
				},
			}}},
		}, {
			ID:        "mut-create",
			Provider:  "google_people",
			Operation: ContactsBatchMutationOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Create contact: Grace Hopper",
			Payload: map[string]any{"operations": []any{map[string]any{
				"op": "create_contact",
			}}},
			Preview: map[string]any{"operations": []any{map[string]any{
				"op":       "create_contact",
				"op_index": 1,
				"person": map[string]any{
					"names":          []any{map[string]any{"displayName": "Grace Hopper"}},
					"emailAddresses": []any{map[string]any{"value": "grace@example.test"}},
					"phoneNumbers":   []any{map[string]any{"value": "+1 555 0100"}},
					"organizations":  []any{map[string]any{"name": "Navy", "title": "Rear Admiral"}},
				},
			}}},
		}, {
			ID:        "mut-delete",
			Provider:  "google_people",
			Operation: ContactsBatchMutationOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Delete contact: Old Duplicate",
			Payload: map[string]any{"operations": []any{map[string]any{
				"op":            "delete_contact",
				"resource_name": "people/old",
			}}},
			Preview: map[string]any{"operations": []any{map[string]any{
				"op":            "delete_contact",
				"op_index":      2,
				"resource_name": "people/old",
				"before": map[string]any{
					"names":          []any{map[string]any{"displayName": "Old Duplicate"}},
					"emailAddresses": []any{map[string]any{"value": "old@example.test"}},
				},
			}}},
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
	detailRequest := httptest.NewRequest(http.MethodGet, "/mutation-review/requests/req-contact", nil)
	for _, cookie := range cookies {
		detailRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(detailResponse, detailRequest)

	if detailResponse.Code != http.StatusOK {
		t.Fatalf("detail status = %d body=%q", detailResponse.Code, detailResponse.Body.String())
	}
	body := detailResponse.Body.String()
	for _, want := range []string{
		"Apply 3 contact changes",
		`class="mutation contact-mutation"`,
		"Update contact",
		"Create contact",
		"Delete contact",
		"Ada Lovelace",
		"ada@example.test",
		"Grace Hopper",
		"grace@example.test",
		"Old Duplicate",
		"people/old",
		`class="contact-diff contact-inline-diff"`,
		`class="contact-inline-row"`,
		`class="contact-inline-old" aria-label="Before"`,
		`class="contact-inline-new" aria-label="After"`,
		`class="contact-inline-arrow" aria-hidden="true">&rarr;`,
		"names",
		"emailAddresses",
		"organizations",
		"Old Role, Old Lab",
		"Computer Scientist, Analytical Engines",
		"Fields not listed here are not part of this update.",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("contact detail page missing %q: %q", want, body)
		}
	}
	operationCount := strings.Count(body, `<div class="contact-operation updating">`) +
		strings.Count(body, `<div class="contact-operation creating">`) +
		strings.Count(body, `<div class="contact-operation destructive">`)
	if operationCount != 3 {
		t.Fatalf("contact operation count = %d body=%q", operationCount, body)
	}
	if strings.Contains(body, `<pre>{`) {
		t.Fatalf("contact preview should not lead with raw JSON: %q", body)
	}
}

func TestSplitGmailQuotedHTML(t *testing.T) {
	body, quote := splitGmailQuotedHTML(`<div>reply</div><div class="gmail_quote gmail_quote_container"><blockquote class="gmail_quote">parent</blockquote></div>`)

	if body != "<div>reply</div>" {
		t.Fatalf("body = %q", body)
	}
	if !strings.Contains(quote, "parent") || !strings.Contains(quote, "gmail_quote") {
		t.Fatalf("quote = %q", quote)
	}
}

func TestSplitEmailBodyAndSignatureHTML(t *testing.T) {
	body, signature := splitEmailBodyAndSignatureHTML(`<div>Hello</div><div><br></div><div class="gmail_signature"><div dir="ltr">--<br>Zach</div></div>`)

	if body != "<div>Hello</div>" {
		t.Fatalf("body = %q", body)
	}
	if !strings.Contains(signature, `class="gmail_signature"`) || !strings.Contains(signature, "Zach") {
		t.Fatalf("signature = %q", signature)
	}
}

func TestSanitizeGmailSignaturePreviewHTML(t *testing.T) {
	preview := sanitizeGmailSignaturePreviewHTML(`<div class="gmail_signature"><script>alert("x")</script><img src=x onerror="alert(1)"><a href="javascript:alert(2)">bad</a><a href="mailto:zach@example.test">ok</a></div>`)

	for _, forbidden := range []string{"<script", "onerror", "javascript:"} {
		if strings.Contains(strings.ToLower(preview), forbidden) {
			t.Fatalf("preview contains %q: %q", forbidden, preview)
		}
	}
	if !strings.Contains(preview, `href="#"`) || !strings.Contains(preview, `mailto:zach@example.test`) {
		t.Fatalf("preview did not preserve safe signature content: %q", preview)
	}
}

func TestGmailBodyFrameHeightKeepsShortEmailsCompact(t *testing.T) {
	short := gmailBodyFrameHeight("<p>Short notification</p>", false)
	long := gmailBodyFrameHeight("<p>"+strings.Repeat("Long body text. ", 200)+"</p>", false)
	quoted := gmailBodyFrameHeight("<p>"+strings.Repeat("Quoted body text. ", 200)+"</p>", true)

	if short > 180 {
		t.Fatalf("short email height = %d", short)
	}
	if long > 240 {
		t.Fatalf("long email height = %d", long)
	}
	if quoted > 200 {
		t.Fatalf("quoted email height = %d", quoted)
	}
}

func gmailEmailReviewRequest() Request {
	message := map[string]any{
		"to":                 []any{"one@example.test"},
		"cc":                 []any{"cc@example.test"},
		"bcc":                []any{"secret@example.test"},
		"subject":            "Original subject",
		"body_text":          "Original body",
		"body_html":          `<p>Original body</p><div class="gmail_quote gmail_quote_container"><div class="gmail_attr">On Fri, May 22, 2026, Customer wrote:<br></div><blockquote class="gmail_quote">Prior note</blockquote></div>`,
		"reply_to_thread_id": "thread-reply",
		"in_reply_to":        "<message@example.test>",
		"references":         []any{"<message@example.test>"},
	}
	return Request{
		ID:        "req-email",
		Status:    "pending_review",
		Title:     "Send proposed email",
		Reason:    "reply to customer",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Mutations: []Mutation{{
			ID:        "mut-email",
			RequestID: "req-email",
			Provider:  "gmail",
			Operation: GmailSendEmailOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Send email: Original subject",
			Reason:    "reply to customer",
			Payload: map[string]any{
				"delivery_mode": "send",
				"message":       cloneMap(message),
			},
			Preview: map[string]any{
				"email": map[string]any{
					"mode":          "reply",
					"delivery_mode": "send",
					"to":            message["to"],
					"cc":            message["cc"],
					"bcc":           message["bcc"],
					"subject":       message["subject"],
					"body_text":     message["body_text"],
					"body_html":     message["body_html"],
				},
				"reply_thread_count": 1,
				"reply_threads": []any{map[string]any{
					"thread_id":           "thread-reply",
					"subject":             "Existing customer thread",
					"latest_from_address": "Customer <customer@example.test>",
					"latest_at":           "2026-05-22T15:45:00Z",
					"latest_preview":      "Can you take a look?",
					"message_count":       1,
					"inbox_message_count": 1,
					"labels":              []any{"INBOX"},
					"messages": []any{map[string]any{
						"message_id":    "message-parent",
						"from_address":  "Customer <customer@example.test>",
						"to_addresses":  []any{"zach@example.test"},
						"internal_date": "2026-05-22T15:45:00Z",
						"preview_text":  "Can you take a look?",
						"body_html":     "<p>Can you take a look?</p>",
						"label_ids":     []any{"INBOX"},
					}},
				}},
			},
		}},
	}
}

func gmailEmailVariantReviewRequest() Request {
	directMessage := map[string]any{
		"to":        []any{"one@example.test"},
		"subject":   "Direct subject",
		"body_text": "Direct body",
		"body_html": "<p>Direct body</p>",
	}
	softerMessage := map[string]any{
		"to":        []any{"one@example.test"},
		"subject":   "Softer subject",
		"body_text": "Softer body",
		"body_html": "<p>Softer body</p>",
	}
	variants := []map[string]any{{
		"id":      "variant_1",
		"title":   "Direct Reply",
		"message": cloneMap(directMessage),
	}, {
		"id":      "variant_2",
		"title":   "Softer Ask",
		"message": cloneMap(softerMessage),
	}}
	return Request{
		ID:        "req-email-variants",
		Status:    "pending_review",
		Title:     "Send proposed email",
		Reason:    "reply to customer",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Mutations: []Mutation{{
			ID:        "mut-email-variants",
			RequestID: "req-email-variants",
			Provider:  "gmail",
			Operation: GmailSendEmailOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Send email: Direct subject",
			Reason:    "reply to customer",
			Payload: map[string]any{
				"delivery_mode":       "send",
				"selected_variant_id": "variant_1",
				"message":             cloneMap(directMessage),
				"variants":            variants,
			},
			Preview: map[string]any{
				"email": map[string]any{
					"mode":                "new_thread",
					"delivery_mode":       "send",
					"selected_variant_id": "variant_1",
					"to":                  directMessage["to"],
					"subject":             directMessage["subject"],
					"body_text":           directMessage["body_text"],
					"body_html":           directMessage["body_html"],
					"variants":            variants,
				},
			},
		}},
	}
}

func TestReviewUIRendersRequestContextAndFlatShapeContact(t *testing.T) {
	store := &reviewStore{requests: []Request{{
		ID:        "req-flat",
		Status:    "pending_review",
		Title:     "Add Barnav Mitra and Reem Khalifa to Contacts",
		Reason:    "Add high-confidence identifications from recent Apple Messages handles.",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Context: map[string]any{
			"source": "Apple Messages + Gmail/Slack lookup via PDW",
			"note":   "Phone numbers are masked in tool output, so contacts are added by email.",
			"identifications": []any{
				map[string]any{
					"inferred_name": "Barnav Mitra",
					"confidence":    "high",
					"masked_phone":  "+197****4215",
					"evidence": []any{
						"Zach addressed them as Barnav in messages",
						"Slack user barnav.mitra has email barnav.mitra@icloud.com",
					},
				},
				map[string]any{
					"inferred_name": "Nick (last name unknown)",
					"confidence":    "medium-low",
					"action":        "not proposed",
					"masked_phone":  "+168****1483",
					"evidence":      []any{"Zach sent 'Hi Nick...' but no reliable full name found"},
				},
			},
			"unhandled_key": map[string]any{"foo": "bar"},
		},
		Mutations: []Mutation{{
			ID:        "mut-flat-create",
			Provider:  "google_people",
			Operation: ContactsBatchMutationOperation,
			Account:   "zach@hackclub.com",
			Status:    "pending_review",
			Title:     "Create contact: Barnav Mitra",
			Payload: map[string]any{"operations": []any{map[string]any{
				"op":            "create",
				"given_name":    "Barnav",
				"family_name":   "Mitra",
				"display_name":  "Barnav Mitra",
				"primary_email": "barnav.mitra@icloud.com",
				"organization":  "Hack Club",
				"job_title":     "Gap Year participant",
			}}},
			Preview: map[string]any{"operations": []any{map[string]any{
				"op":            "create",
				"op_index":      0,
				"given_name":    "Barnav",
				"family_name":   "Mitra",
				"display_name":  "Barnav Mitra",
				"primary_email": "barnav.mitra@icloud.com",
				"organization":  "Hack Club",
				"job_title":     "Gap Year participant",
			}}},
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
	detailRequest := httptest.NewRequest(http.MethodGet, "/mutation-review/requests/req-flat", nil)
	for _, cookie := range cookies {
		detailRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(detailResponse, detailRequest)

	if detailResponse.Code != http.StatusOK {
		t.Fatalf("detail status = %d body=%q", detailResponse.Code, detailResponse.Body.String())
	}
	body := detailResponse.Body.String()
	for _, want := range []string{
		// Context block surfaces source/note/identifications instead of dumping raw JSON.
		`class="request-context"`,
		"Apple Messages + Gmail/Slack lookup via PDW",
		"Phone numbers are masked",
		"Identifications",
		"Barnav Mitra",
		`class="confidence-chip confidence-high"`,
		"high confidence",
		"Nick (last name unknown)",
		`class="confidence-chip confidence-medium-low"`,
		`class="identification-action"`,
		"not proposed",
		`class="identification-phone"`,
		"+197****4215",
		"Zach addressed them as Barnav in messages",
		// Unhandled context keys fall back to JSON so nothing is silently lost.
		"Other context",
		"unhandled_key",
		// Flat-shape contact op renders as a regular create card with person details.
		`<div class="contact-operation creating">`,
		"Create contact",
		"barnav.mitra@icloud.com",
		"Hack Club",
		"Gap Year participant, Hack Club",
		"Creates a new Google Contact.",
		// Person block extracted from flat top-level fields.
		"Contact to create",
		"<dt>Email</dt><dd>barnav.mitra@icloud.com</dd>",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("flat contact + context page missing %q: %q", want, body)
		}
	}
}

func TestReviewUIOmitsContextSectionWhenEmpty(t *testing.T) {
	store := &reviewStore{requests: []Request{{
		ID:        "req-no-context",
		Status:    "pending_review",
		Title:     "No context",
		Reason:    "no context provided",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		// No Context field set.
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
	detailRequest := httptest.NewRequest(http.MethodGet, "/mutation-review/requests/req-no-context", nil)
	for _, cookie := range cookies {
		detailRequest.AddCookie(cookie)
	}
	handler.ServeHTTP(detailResponse, detailRequest)

	body := detailResponse.Body.String()
	if strings.Contains(body, `class="request-context"`) {
		t.Fatalf("expected no request-context section, got: %q", body)
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
