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

	bad, _ := http.Get(srv.URL + APIPath + "/requests?status=bogus")
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for an unknown status, got %d", bad.StatusCode)
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
