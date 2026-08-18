package mutations

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func loggedInReview(t *testing.T, requests []Request) (*reviewStore, http.Handler, []*http.Cookie) {
	t.Helper()
	store := &reviewStore{requests: requests}
	service := NewService(store, Config{BaseURL: "https://mcp.example.test", UIPassword: "pw"})
	handler := service.HTTPHandler()
	loginResponse := httptest.NewRecorder()
	loginForm := url.Values{"password": {"pw"}}
	loginRequest := httptest.NewRequest(http.MethodPost, ReviewPath+"/login", strings.NewReader(loginForm.Encode()))
	loginRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	handler.ServeHTTP(loginResponse, loginRequest)
	if loginResponse.Code != http.StatusSeeOther {
		t.Fatalf("login status = %d", loginResponse.Code)
	}
	return store, handler, loginResponse.Result().Cookies()
}

func reviewDetailBody(t *testing.T, handler http.Handler, cookies []*http.Cookie, requestID string) string {
	t.Helper()
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, ReviewPath+"/requests/"+requestID, nil)
	for _, cookie := range cookies {
		request.AddCookie(cookie)
	}
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("detail status = %d body=%q", response.Code, response.Body.String())
	}
	return response.Body.String()
}

func postSupersede(t *testing.T, handler http.Handler, cookies []*http.Cookie, requestID string, csrfToken string, supersededBy string) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{"csrf_token": {csrfToken}, "superseded_by": {supersededBy}}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, ReviewPath+"/requests/"+requestID+"/supersede", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for _, cookie := range cookies {
		request.AddCookie(cookie)
	}
	handler.ServeHTTP(response, request)
	return response
}

// A terminally failed request stays in the review list forever with nothing
// saying it was ever dealt with. The 2026-08-15 contacts request is the case
// that prompted this: it failed, the fix shipped, a replacement ran, and the
// original still reads as an unresolved red row.
func TestSupersedeRequestRecordsTheReplacement(t *testing.T) {
	store, handler, cookies := loggedInReview(t, []Request{
		{ID: "req-old", Status: "failed_terminal", Title: "Old", CreatedAt: time.Unix(1700000000, 0).UTC()},
		{ID: "req-new", Status: "succeeded", Title: "New", CreatedAt: time.Unix(1700000100, 0).UTC()},
	})
	body := reviewDetailBody(t, handler, cookies, "req-old")
	csrfToken := hiddenFieldValue(t, body, "csrf_token")

	response := postSupersede(t, handler, cookies, "req-old", csrfToken, "req-new")
	if response.Code != http.StatusSeeOther {
		t.Fatalf("status = %d body=%q", response.Code, response.Body.String())
	}
	if len(store.superseded) != 1 {
		t.Fatalf("supersede calls = %#v", store.superseded)
	}
	if store.superseded[0].RequestID != "req-old" || store.superseded[0].SupersededBy != "req-new" || store.superseded[0].Actor != reviewerActorID {
		t.Fatalf("supersede call = %#v", store.superseded[0])
	}
}

func TestSupersedeRequestRejectsABlankReplacement(t *testing.T) {
	store, handler, cookies := loggedInReview(t, []Request{
		{ID: "req-old", Status: "failed_terminal", Title: "Old", CreatedAt: time.Unix(1700000000, 0).UTC()},
	})
	body := reviewDetailBody(t, handler, cookies, "req-old")
	csrfToken := hiddenFieldValue(t, body, "csrf_token")

	response := postSupersede(t, handler, cookies, "req-old", csrfToken, "   ")
	if len(store.superseded) != 0 {
		t.Fatalf("supersede was called with a blank replacement: %#v", store.superseded)
	}
	if !strings.Contains(response.Body.String(), "request id") {
		t.Fatalf("body does not explain the problem: %q", response.Body.String())
	}
}

func TestSupersedeRequestRefusesToPointAtItself(t *testing.T) {
	store, handler, cookies := loggedInReview(t, []Request{
		{ID: "req-old", Status: "failed_terminal", Title: "Old", CreatedAt: time.Unix(1700000000, 0).UTC()},
	})
	body := reviewDetailBody(t, handler, cookies, "req-old")
	csrfToken := hiddenFieldValue(t, body, "csrf_token")

	response := postSupersede(t, handler, cookies, "req-old", csrfToken, "req-old")
	if len(store.superseded) != 0 {
		t.Fatalf("supersede was called with itself: %#v", store.superseded)
	}
	if !strings.Contains(response.Body.String(), "itself") {
		t.Fatalf("body does not explain the problem: %q", response.Body.String())
	}
}

// The control only makes sense on a request nothing else can move. Offering it
// on a pending request would invite closing out work that has not run.
func TestReviewUIOffersSupersedeOnlyOnTerminallyFailedRequests(t *testing.T) {
	_, handler, cookies := loggedInReview(t, []Request{
		{ID: "req-old", Status: "failed_terminal", Title: "Old", CreatedAt: time.Unix(1700000000, 0).UTC()},
		{ID: "req-pending", Status: "pending_review", Title: "Pending", CreatedAt: time.Unix(1700000000, 0).UTC()},
	})
	if failed := reviewDetailBody(t, handler, cookies, "req-old"); !strings.Contains(failed, "/supersede") {
		t.Fatalf("failed request has no supersede control: %q", failed)
	}
	if pending := reviewDetailBody(t, handler, cookies, "req-pending"); strings.Contains(pending, "/supersede") {
		t.Fatalf("pending request offers a supersede control: %q", pending)
	}
}

func TestReviewUIShowsTheSupersedingRequest(t *testing.T) {
	_, handler, cookies := loggedInReview(t, []Request{{
		ID:           "req-old",
		Status:       "failed_terminal",
		Title:        "Old",
		SupersededBy: "req-new",
		CreatedAt:    time.Unix(1700000000, 0).UTC(),
	}})
	body := reviewDetailBody(t, handler, cookies, "req-old")
	if !strings.Contains(body, "Superseded by") || !strings.Contains(body, "req-new") {
		t.Fatalf("body does not show the replacement: %q", body)
	}
	// Already resolved, so do not offer to resolve it again.
	if strings.Contains(body, `action="`+ReviewPath+`/requests/req-old/supersede"`) {
		t.Fatalf("body still offers the supersede form: %q", body)
	}
}

// The request list is where a stale red row is actually noticed, so the
// resolution has to be visible there too.
func TestReviewUIListMarksSupersededRequests(t *testing.T) {
	_, handler, cookies := loggedInReview(t, []Request{{
		ID:           "req-old",
		Status:       "failed_terminal",
		Title:        "Old",
		SupersededBy: "req-new",
		CreatedAt:    time.Unix(1700000000, 0).UTC(),
	}})
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, ReviewPath+"/requests", nil)
	for _, cookie := range cookies {
		request.AddCookie(cookie)
	}
	handler.ServeHTTP(response, request)
	if !strings.Contains(response.Body.String(), "superseded") {
		t.Fatalf("request list does not mark the superseded request: %q", response.Body.String())
	}
}

func TestSupersedeDDLAddsTheColumnToExistingDatabases(t *testing.T) {
	var found bool
	for _, statement := range upstreamMutationSchemaStatements {
		if !strings.Contains(statement, "superseded_by_request_id") {
			continue
		}
		if !strings.Contains(statement, "@upstream_mutation_requests") {
			t.Fatalf("supersede DDL does not name its relation through the catalog: %s", statement)
		}
		if strings.Contains(statement, "ADD COLUMN IF NOT EXISTS superseded_by_request_id") {
			found = true
		}
	}
	if !found {
		t.Fatal("no idempotent ALTER adds superseded_by_request_id to existing databases")
	}
}

func TestRequestIsSupersedable(t *testing.T) {
	for _, status := range []string{"failed_terminal", "blocked_missing_credentials", "failed_retryable"} {
		if !requestIsSupersedable(status) {
			t.Fatalf("%q should be supersedable", status)
		}
	}
	for _, status := range []string{"pending_review", "approved", "executing", "succeeded", "observed", "rejected"} {
		if requestIsSupersedable(status) {
			t.Fatalf("%q should not be supersedable", status)
		}
	}
}
