package mutations

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type apiFakeStore struct {
	requests  map[string]Request
	approved  []string
	rejected  map[string]string
	removed   []string
	actors    []string
	listCalls []RequestFilter
}

func newAPIFakeStore(requests ...Request) *apiFakeStore {
	s := &apiFakeStore{requests: map[string]Request{}, rejected: map[string]string{}}
	for _, r := range requests {
		s.requests[r.ID] = r
	}
	return s
}

func (s *apiFakeStore) CreateRequest(context.Context, CreateRequestInput) (Request, error) {
	return Request{}, errors.New("not used")
}
func (s *apiFakeStore) ListRequests(_ context.Context, filter RequestFilter) ([]Request, error) {
	s.listCalls = append(s.listCalls, filter)
	out := []Request{}
	for _, r := range s.requests {
		if len(filter.Statuses) == 0 {
			out = append(out, r)
			continue
		}
		for _, st := range filter.Statuses {
			if r.Status == st {
				out = append(out, r)
			}
		}
	}
	return out, nil
}
func (s *apiFakeStore) GetRequest(_ context.Context, id string) (Request, error) {
	r, ok := s.requests[id]
	if !ok {
		return Request{}, ErrNotFound
	}
	return r, nil
}
func (s *apiFakeStore) UpdateGmailEmailMutation(context.Context, string, string, UpdateGmailEmailMutationInput, string) (Mutation, error) {
	return Mutation{}, errors.New("not used")
}
func (s *apiFakeStore) RemoveMutation(_ context.Context, requestID, mutationID, actor string) (Mutation, error) {
	if _, ok := s.requests[requestID]; !ok {
		return Mutation{}, ErrNotFound
	}
	s.removed = append(s.removed, mutationID)
	s.actors = append(s.actors, actor)
	return Mutation{ID: mutationID, RequestID: requestID, Status: "removed"}, nil
}
func (s *apiFakeStore) ApproveRequest(_ context.Context, id, actor string) (Request, error) {
	r, ok := s.requests[id]
	if !ok {
		return Request{}, ErrNotFound
	}
	if r.Status != "pending_review" {
		return Request{}, errors.New("request is not pending review")
	}
	r.Status, r.ApprovedBy, r.ApprovedAt = "approved", actor, time.Date(2026, 8, 26, 1, 2, 3, 0, time.UTC)
	s.requests[id] = r
	s.approved = append(s.approved, id)
	s.actors = append(s.actors, actor)
	return r, nil
}
func (s *apiFakeStore) RejectRequest(_ context.Context, id, actor, reason string) (Request, error) {
	r, ok := s.requests[id]
	if !ok {
		return Request{}, ErrNotFound
	}
	r.Status = "rejected"
	s.requests[id] = r
	s.rejected[id] = reason
	s.actors = append(s.actors, actor)
	return r, nil
}
func (s *apiFakeStore) SupersedeRequest(context.Context, string, string, string) (Request, error) {
	return Request{}, errors.New("not used")
}

func passthrough(next http.Handler) http.Handler { return next }

func newAPIServer(t *testing.T, store Store) *httptest.Server {
	t.Helper()
	svc := NewService(store, Config{BaseURL: "https://pdw.example"})
	mux := http.NewServeMux()
	svc.RegisterAPI(mux, passthrough)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func decodeBody(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return body
}

func pendingFixture() Request {
	return Request{
		ID: "req-1", Status: "pending_review", Title: "Archive newsletters", Reason: "inbox zero",
		CreatedAt: time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC),
		// The warehouse stores "not yet" as the epoch; the API must read it as null.
		ApprovedAt: time.Unix(0, 0).UTC(), ExecutedAt: time.Unix(0, 0).UTC(),
		MutationCount: 1,
		Mutations: []Mutation{{
			ID: "mut-1", RequestID: "req-1", Provider: "gmail", Operation: GmailArchiveOperation,
			Account: "zach@example.test", Status: "pending_review",
			Payload:   map[string]any{"thread_ids": []any{"t1"}},
			CreatedAt: time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC), ApprovedAt: time.Unix(0, 0).UTC(),
		}},
	}
}

func TestAPIListFiltersByStatusAndRejectsUnknownStatus(t *testing.T) {
	store := newAPIFakeStore(pendingFixture(), Request{ID: "req-2", Status: "rejected"})
	srv := newAPIServer(t, store)

	resp, err := http.Get(srv.URL + APIPath + "/requests?status=pending_review")
	if err != nil {
		t.Fatal(err)
	}
	body := decodeBody(t, resp)
	requests := body["requests"].([]any)
	if len(requests) != 1 || requests[0].(map[string]any)["id"] != "req-1" {
		t.Fatalf("unexpected list %v", body)
	}
	first := requests[0].(map[string]any)
	if first["approved_at"] != nil || first["executed_at"] != nil {
		t.Fatalf("epoch sentinel leaked through the read interface: %v", first)
	}
	if first["review_url"] != "https://pdw.example"+ReviewPath+"/requests/req-1" {
		t.Fatalf("review_url wrong: %v", first["review_url"])
	}
	if _, has := first["mutations"]; has {
		t.Fatal("list rows must not carry full mutation bodies")
	}

	// Production writes statuses the API was not told about (succeeded); the
	// filter must pass them through and refuse only malformed tokens.
	ok, _ := http.Get(srv.URL + APIPath + "/requests?status=succeeded")
	if ok.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for an unlisted but well-formed status, got %d", ok.StatusCode)
	}
	bad, _ := http.Get(srv.URL + APIPath + "/requests?status=Bogus%20Status")
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for a malformed status, got %d", bad.StatusCode)
	}
}

func TestAPIGetReturnsMutationsAndNotFound(t *testing.T) {
	srv := newAPIServer(t, newAPIFakeStore(pendingFixture()))
	resp, _ := http.Get(srv.URL + APIPath + "/requests/req-1")
	body := decodeBody(t, resp)
	request := body["request"].(map[string]any)
	mutations := request["mutations"].([]any)
	if len(mutations) != 1 {
		t.Fatalf("expected the mutation body, got %v", request)
	}
	m := mutations[0].(map[string]any)
	if m["operation"] != GmailArchiveOperation || m["approved_at"] != nil {
		t.Fatalf("mutation JSON wrong: %v", m)
	}
	missing, _ := http.Get(srv.URL + APIPath + "/requests/nope")
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", missing.StatusCode)
	}
}

func TestAPIApproveRejectAndRemoveUseAnAppActor(t *testing.T) {
	store := newAPIFakeStore(pendingFixture(), Request{ID: "req-3", Status: "pending_review"})
	srv := newAPIServer(t, store)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+APIPath+"/requests/req-1/approve", nil)
	req.Header.Set("X-PDW-Client", "zach-iphone")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK || body["request"].(map[string]any)["status"] != "approved" {
		t.Fatalf("approve failed: %d %v", resp.StatusCode, body)
	}
	if body["request"].(map[string]any)["approved_at"] != "2026-08-26T01:02:03Z" {
		t.Fatalf("approved_at not rendered: %v", body)
	}

	again, _ := http.Post(srv.URL+APIPath+"/requests/req-1/approve", "application/json", nil)
	if again.StatusCode != http.StatusConflict {
		t.Fatalf("approving twice must be a conflict, got %d", again.StatusCode)
	}

	resp, _ = http.Post(srv.URL+APIPath+"/requests/req-3/reject", "application/json", strings.NewReader(`{"reason":"not now"}`))
	if resp.StatusCode != http.StatusOK || store.rejected["req-3"] != "not now" {
		t.Fatalf("reject failed: %d %q", resp.StatusCode, store.rejected["req-3"])
	}

	resp, _ = http.Post(srv.URL+APIPath+"/requests/req-1/mutations/mut-1/remove", "application/json", nil)
	if resp.StatusCode != http.StatusOK || len(store.removed) != 1 || store.removed[0] != "mut-1" {
		t.Fatalf("remove failed: %d %v", resp.StatusCode, store.removed)
	}

	for _, actor := range store.actors {
		if !strings.HasPrefix(actor, apiReviewerActorPrefix) {
			t.Fatalf("actor %q is not attributed to the app", actor)
		}
	}
	if store.actors[0] != "app:zach-iphone" {
		t.Fatalf("client header not used as the actor: %q", store.actors[0])
	}

	get, _ := http.Get(srv.URL + APIPath + "/requests/req-1/approve")
	if get.StatusCode != http.StatusNotFound {
		t.Fatalf("GET on an action must 404, got %d", get.StatusCode)
	}
}

func TestProposeCallsTheRequestCreatedHook(t *testing.T) {
	var got Request
	store := &recordingCreateStore{apiFakeStore: newAPIFakeStore()}
	svc := NewService(store, Config{BaseURL: "https://pdw.example", GmailAccounts: []string{"zach@example.test"}})
	svc.SetRequestCreated(func(_ context.Context, r Request) { got = r })
	_, err := svc.ProposeMutation(context.Background(), ProposeMutationInput{
		Title: "Archive", Reason: "why",
		Mutations: []map[string]any{{"type": GmailArchiveOperation, "account": "zach@example.test", "thread_ids": []any{"t1"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "created-1" {
		t.Fatalf("hook not called with the stored request: %+v", got)
	}
}

type recordingCreateStore struct{ *apiFakeStore }

func (s *recordingCreateStore) CreateRequest(_ context.Context, input CreateRequestInput) (Request, error) {
	return Request{ID: "created-1", Status: "pending_review", Title: input.Title, Reason: input.Reason, MutationCount: len(input.Mutations)}, nil
}

func (s *apiFakeStore) recordSupersede(id, by, actor string) (Request, error) {
	r, ok := s.requests[id]
	if !ok {
		return Request{}, ErrNotFound
	}
	if !requestIsSupersedable(r.Status) {
		return Request{}, errors.New("request is not supersedable")
	}
	r.SupersededBy = by
	s.requests[id] = r
	s.actors = append(s.actors, actor)
	return r, nil
}

// supersedingStore is the API fake with supersede and email edits wired up.
type supersedingStore struct {
	*apiFakeStore
	emailEdits []UpdateGmailEmailMutationInput
}

func (s *supersedingStore) SupersedeRequest(_ context.Context, id, by, actor string) (Request, error) {
	return s.recordSupersede(id, by, actor)
}

func (s *supersedingStore) UpdateGmailEmailMutation(_ context.Context, requestID, mutationID string, input UpdateGmailEmailMutationInput, actor string) (Mutation, error) {
	r, ok := s.requests[requestID]
	if !ok {
		return Mutation{}, ErrNotFound
	}
	if r.Status != "pending_review" {
		return Mutation{}, errors.New("cannot edit mutation for request with status " + r.Status)
	}
	s.emailEdits = append(s.emailEdits, input)
	s.actors = append(s.actors, actor)
	return Mutation{ID: mutationID, RequestID: requestID, Status: "pending_review", Payload: map[string]any{"message": input.Message, "delivery_mode": input.DeliveryMode}}, nil
}

func sendEmailFixture() Request {
	return Request{
		ID: "req-email", Status: "pending_review", Title: "Reply to the vendor", MutationCount: 1,
		CreatedAt: time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC),
		Mutations: []Mutation{{
			ID: "mut-email", RequestID: "req-email", Provider: "gmail", Operation: GmailSendEmailOperation,
			Account: "zach@example.test", Status: "pending_review",
			Payload: map[string]any{
				"delivery_mode": "send",
				"message": map[string]any{
					"to": []any{"vendor@example.test"}, "subject": "Re: quote",
					"body_html":          `<div>Sounds good.</div><div><br></div><div class="gmail_signature"><b>Zach</b></div><div class="gmail_quote">On Mon, they wrote:<br>hi</div>`,
					"reply_to_thread_id": "t-9",
				},
				"variants": []any{
					map[string]any{"id": "variant-1", "title": "Direct", "message": map[string]any{"to": []any{"vendor@example.test"}, "subject": "Re: quote", "body_text": "Sounds good."}},
					map[string]any{"id": "variant-2", "title": "Softer", "message": map[string]any{"to": []any{"vendor@example.test"}, "subject": "Re: quote", "body_text": "Maybe."}},
				},
				"selected_variant_id": "variant-2",
			},
			Preview: map[string]any{
				"reply_threads": []any{map[string]any{"thread_id": "t-9", "subject": "quote", "messages": []any{map[string]any{"from_address": "vendor@example.test", "body_html": `<p>hi</p><div class="gmail_quote">older</div>`}}}},
			},
		}},
	}
}

// The web SPA edits an email before approving it, exactly as the old HTML form
// did; the JSON twin has to accept the same fields and hand the store the same
// normalized input.
func TestAPIUpdateEmailNormalizesTheMessageAndUsesTheAppActor(t *testing.T) {
	store := &supersedingStore{apiFakeStore: newAPIFakeStore(sendEmailFixture())}
	srv := newAPIServer(t, store)
	body := `{"delivery_mode":"draft","selected_variant_id":"variant-1","message":{"to":"a@example.test, b@example.test","cc":["c@example.test"],"subject":"  Re: quote ","body_text":"hi","body_html":"<p>hi</p>","reply_to_thread_id":"t-9","references":"<x@y>\n<z@y>"}}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+APIPath+"/requests/req-email/mutations/mut-email/update-email", strings.NewReader(body))
	req.Header.Set("X-PDW-Client", "web")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	got := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK || got["mutation"] == nil {
		t.Fatalf("update-email failed: %d %v", resp.StatusCode, got)
	}
	if len(store.emailEdits) != 1 {
		t.Fatalf("store received %d edits", len(store.emailEdits))
	}
	edit := store.emailEdits[0]
	if edit.DeliveryMode != "draft" || edit.SelectedVariantID != "variant-1" {
		t.Fatalf("edit = %+v", edit)
	}
	if to := edit.Message["to"].([]string); len(to) != 2 || to[0] != "a@example.test" || to[1] != "b@example.test" {
		t.Fatalf("to not split: %v", edit.Message["to"])
	}
	if cc := edit.Message["cc"].([]string); len(cc) != 1 || cc[0] != "c@example.test" {
		t.Fatalf("cc not normalized: %v", edit.Message["cc"])
	}
	if edit.Message["subject"] != "Re: quote" || edit.Message["reply_to_thread_id"] != "t-9" {
		t.Fatalf("message = %v", edit.Message)
	}
	if refs := edit.Message["references"].([]string); len(refs) != 2 {
		t.Fatalf("references = %v", edit.Message["references"])
	}
	if store.actors[0] != "app:web" {
		t.Fatalf("actor = %q", store.actors[0])
	}

	bad, _ := http.Post(srv.URL+APIPath+"/requests/req-email/mutations/mut-email/update-email", "application/json", strings.NewReader(`{"message":`))
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed JSON must be 400, got %d", bad.StatusCode)
	}
	missing, _ := http.Post(srv.URL+APIPath+"/requests/nope/mutations/mut-email/update-email", "application/json", strings.NewReader(`{"message":{}}`))
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown request must be 404, got %d", missing.StatusCode)
	}
}

// A terminally failed request is closed out by naming its replacement. The
// API refuses a blank or self-referential replacement before touching the
// store, and reports whether the control applies at all (can_supersede), so
// no client has to keep its own copy of the supersedable status list.
func TestAPISupersedeRecordsTheReplacementAndValidatesInput(t *testing.T) {
	store := &supersedingStore{apiFakeStore: newAPIFakeStore(
		Request{ID: "req-old", Status: "failed_terminal", Title: "Old"},
		Request{ID: "req-pending", Status: "pending_review", Title: "Pending"},
	)}
	srv := newAPIServer(t, store)

	old := decodeBody(t, mustGet(t, srv.URL+APIPath+"/requests/req-old"))
	if old["request"].(map[string]any)["can_supersede"] != true {
		t.Fatalf("failed request must be supersedable: %v", old)
	}
	pending := decodeBody(t, mustGet(t, srv.URL+APIPath+"/requests/req-pending"))
	if pending["request"].(map[string]any)["can_supersede"] != false {
		t.Fatalf("pending request must not be supersedable: %v", pending)
	}

	for _, body := range []string{`{"superseded_by":""}`, `{"superseded_by":"req-old"}`} {
		resp, _ := http.Post(srv.URL+APIPath+"/requests/req-old/supersede", "application/json", strings.NewReader(body))
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("body %s must be rejected with 400, got %d", body, resp.StatusCode)
		}
	}
	resp, _ := http.Post(srv.URL+APIPath+"/requests/req-old/supersede", "application/json", strings.NewReader(`{"superseded_by":"req-new"}`))
	got := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK || got["request"].(map[string]any)["superseded_by"] != "req-new" {
		t.Fatalf("supersede failed: %d %v", resp.StatusCode, got)
	}
	if got["request"].(map[string]any)["can_supersede"] != false {
		t.Fatalf("a superseded request must not offer supersede again: %v", got)
	}
	conflict, _ := http.Post(srv.URL+APIPath+"/requests/req-pending/supersede", "application/json", strings.NewReader(`{"superseded_by":"req-new"}`))
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("superseding a pending request must conflict, got %d", conflict.StatusCode)
	}
}

// The review surface needs the email the way the reviewer edits it: which
// variant is selected, the editable body with the signature and quoted thread
// split off, and the thread being replied to. Computing that once, server-side,
// keeps every client (web, iOS) from re-deriving it differently.
func TestAPIGetRendersTheGmailEmailView(t *testing.T) {
	store := newAPIFakeStore(sendEmailFixture())
	srv := newAPIServer(t, store)
	body := decodeBody(t, mustGet(t, srv.URL+APIPath+"/requests/req-email"))
	mutation := body["request"].(map[string]any)["mutations"].([]any)[0].(map[string]any)
	email, _ := mutation["email"].(map[string]any)
	if email == nil {
		t.Fatalf("send_email mutation has no email view: %v", mutation)
	}
	if email["delivery_mode"] != "send" {
		t.Fatalf("delivery_mode = %v", email["delivery_mode"])
	}
	variants := email["variants"].([]any)
	if len(variants) != 2 {
		t.Fatalf("variants = %v", variants)
	}
	second := variants[1].(map[string]any)
	if second["id"] != "variant-2" || second["selected"] != true || second["title"] != "Softer" {
		t.Fatalf("selected variant not marked: %v", second)
	}
	if second["editor_html"] == "" || !strings.Contains(second["editor_html"].(string), "Maybe.") {
		t.Fatalf("plain-text variant must be offered as HTML for the editor: %v", second)
	}
	threads := email["reply_threads"].([]any)
	if len(threads) != 1 || threads[0].(map[string]any)["thread_id"] != "t-9" {
		t.Fatalf("reply threads = %v", threads)
	}
	message := threads[0].(map[string]any)["messages"].([]any)[0].(map[string]any)
	if message["body_html"] != "<p>hi</p>" || message["quoted_html"] != `<div class="gmail_quote">older</div>` {
		t.Fatalf("thread message body must be split from its quote: %v", message)
	}

	// The archive mutation is not an email and must not grow a view it cannot fill.
	archive := decodeBody(t, mustGet(t, newAPIServer(t, newAPIFakeStore(pendingFixture())).URL+APIPath+"/requests/req-1"))
	if _, ok := archive["request"].(map[string]any)["mutations"].([]any)[0].(map[string]any)["email"]; ok {
		t.Fatal("archive mutation must not carry an email view")
	}
}

// The merged email (payload message + preview) is what a read-only review shows.
func TestGmailEmailViewSplitsSignatureAndQuoteFromTheBody(t *testing.T) {
	view := gmailEmailView(sendEmailFixture().Mutations[0])
	if view["delivery_mode"] != "send" {
		t.Fatalf("delivery_mode = %v", view["delivery_mode"])
	}
	// The variants override the message body; the merged read view keeps the
	// original message, split the same way.
	read := view["message"].(map[string]any)
	if read["editor_html"] != "<div>Sounds good.</div>" {
		t.Fatalf("editor_html = %q", read["editor_html"])
	}
	if !strings.Contains(read["signature_html"].(string), "<b>Zach</b>") {
		t.Fatalf("signature_html = %q", read["signature_html"])
	}
	if !strings.HasPrefix(read["quoted_html"].(string), `<div class="gmail_quote">`) {
		t.Fatalf("quoted_html = %q", read["quoted_html"])
	}
}

func TestAPIListHonorsALimit(t *testing.T) {
	store := newAPIFakeStore(pendingFixture())
	srv := newAPIServer(t, store)
	mustGet(t, srv.URL+APIPath+"/requests?limit=120")
	if len(store.listCalls) != 1 || store.listCalls[0].Limit != 120 {
		t.Fatalf("limit not forwarded: %+v", store.listCalls)
	}
	resp, _ := http.Get(srv.URL + APIPath + "/requests?limit=9999")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("an absurd limit must be rejected, got %d", resp.StatusCode)
	}
}

func mustGet(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}
