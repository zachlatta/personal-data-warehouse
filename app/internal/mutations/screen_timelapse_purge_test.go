package mutations

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func screenTimelapsePurgeService(t *testing.T) (*Service, *recordingStore) {
	t.Helper()
	store := &recordingStore{request: Request{ID: "req-1", Status: "pending_review"}}
	return NewService(store, Config{BaseURL: "https://example.test"}), store
}

func TestProposeScreenTimelapsePurgeStoresTheMoment(t *testing.T) {
	service, store := screenTimelapsePurgeService(t)
	if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title: "Delete a leaked frame", Reason: "Zach asked",
		Mutations: []map[string]any{
			{"type": ScreenTimelapsePurgeMomentOperation, "device": "Laptop", "captured_at": "2026-10-25T10:03:00-04:00", "reason": "card number on screen"},
			{"type": ScreenTimelapsePurgeMomentOperation, "device": "Laptop", "captured_at": "2026-10-25T14:04:00Z", "display_index": float64(2), "reason": "card number on screen"},
		},
	}); err != nil {
		t.Fatalf("ProposeMutation: %v", err)
	}
	stored, err := normalizeForStorage(store.createCalls[0])
	if err != nil {
		t.Fatalf("normalizeForStorage: %v", err)
	}
	if len(stored) != 2 {
		t.Fatalf("stored %d", len(stored))
	}
	all := stored[0]
	if all.Provider != ScreenTimelapseProvider || all.Operation != ScreenTimelapsePurgeMomentOperation || all.Account != "laptop" {
		t.Fatalf("stored[0] = %#v", all)
	}
	// The device keeps its case (it is matched exactly); the time is UTC.
	want := map[string]any{"device": "Laptop", "captured_at": "2026-10-25T14:03:00Z", "reason": "card number on screen"}
	for key, value := range want {
		if all.Payload[key] != value {
			t.Fatalf("payload[%s] = %#v, want %#v (payload %#v)", key, all.Payload[key], value, all.Payload)
		}
	}
	if _, ok := all.Payload["display_index"]; ok {
		t.Fatalf("an omitted display_index means every display: %#v", all.Payload)
	}
	if stored[1].Payload["display_index"] != 2 || !strings.Contains(stored[1].Title, "display 2") {
		t.Fatalf("stored[1] = %#v", stored[1])
	}
}

func TestProposeScreenTimelapsePurgeRefusesAnIncompleteMoment(t *testing.T) {
	for _, tc := range []struct {
		entry map[string]any
		want  string
	}{
		{map[string]any{"captured_at": "2026-10-25T14:03:00Z", "reason": "r"}, "account"},
		{map[string]any{"account": "laptop", "captured_at": "2026-10-25T14:03:00Z", "reason": "r"}, "must include device"},
		{map[string]any{"device": "laptop", "reason": "r"}, "must include captured_at"},
		{map[string]any{"device": "laptop", "captured_at": "yesterday 3pm", "reason": "r"}, "RFC 3339"},
		{map[string]any{"device": "laptop", "captured_at": "2026-10-25T14:03:00Z", "display_index": float64(0), "reason": "r"}, "display_index"},
		{map[string]any{"device": "laptop", "captured_at": "2026-10-25T14:03:00Z", "display_index": 1.5, "reason": "r"}, "display_index"},
		{map[string]any{"device": "laptop", "captured_at": "2026-10-25T14:03:00Z", "display_index": "main", "reason": "r"}, "display_index"},
		{map[string]any{"device": "laptop", "captured_at": "2026-10-25T14:03:00Z"}, "must include reason"},
		{map[string]any{"device": "laptop", "captured_at": "2026-10-25T14:03:00Z", "reason": "r", "storage_file_id": "abc"}, "unknown field(s) storage_file_id"},
	} {
		service, store := screenTimelapsePurgeService(t)
		tc.entry["type"] = ScreenTimelapsePurgeMomentOperation
		_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{Title: "t", Reason: "r", Mutations: []map[string]any{tc.entry}})
		if err == nil || !strings.Contains(err.Error(), tc.want) || len(store.createCalls) != 0 {
			t.Fatalf("entry %#v: err = %v, want %q", tc.entry, err, tc.want)
		}
	}
}

func TestScreenTimelapsePurgeRefusesAMomentThatMatchesNothing(t *testing.T) {
	at := time.Date(2026, 10, 25, 14, 3, 0, 0, time.UTC)
	tree := map[string]any{"storage_file_id": "tree-1"}
	shot := []map[string]any{{"display_index": float64(1), "storage_file_id": "shot-1"}}
	for _, tc := range []struct {
		name    string
		display int
		moment  screenTimelapseMoment
		want    string
	}{
		{"nothing at all", 0, screenTimelapseMoment{}, "matches nothing"},
		{"a display the moment lacks", 3, screenTimelapseMoment{Tree: tree}, "no display 3"},
		{"a moment with screenshots", 0, screenTimelapseMoment{Screenshots: shot}, ""},
		{"a tree whose screenshot already went", 0, screenTimelapseMoment{Tree: tree}, ""},
	} {
		err := screenTimelapsePurgeMatches(screenTimelapsePurgeTarget{Device: "laptop", CapturedAt: at, DisplayIndex: tc.display}, tc.moment)
		if tc.want == "" && err != nil {
			t.Fatalf("%s: unexpected refusal %v", tc.name, err)
		}
		if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
			t.Fatalf("%s: err = %v, want %q", tc.name, err, tc.want)
		}
	}
}

func TestScreenTimelapsePurgePreviewNamesTheTimelineEventOnlyWithDisplayOne(t *testing.T) {
	at := time.Date(2026, 10, 25, 14, 3, 0, 0, time.UTC)
	main := screenTimelapsePurgePreview(screenTimelapsePurgeTarget{Device: "laptop", CapturedAt: at},
		screenTimelapseMoment{Screenshots: []map[string]any{{"display_index": float64(1)}, {"display_index": float64(2)}}})
	if main["removes_timeline"] != true || main["timeline_event_id"] != "laptop|2026-10-25T14:03:00Z" ||
		main["timeline_path"] != "/timeline/screen_capture/laptop%7C2026-10-25T14:03:00Z" || main["displays"] != "all" {
		t.Fatalf("preview = %#v", main)
	}
	if !strings.Contains(stringFromAny(main["effect"]), "slowking") {
		t.Fatalf("the preview must say what is not touched: %#v", main["effect"])
	}
	second := screenTimelapsePurgePreview(screenTimelapsePurgeTarget{Device: "laptop", CapturedAt: at, DisplayIndex: 2},
		screenTimelapseMoment{Screenshots: []map[string]any{{"display_index": float64(2)}}})
	if second["removes_timeline"] != false || second["timeline_event_id"] != nil || second["displays"] != 2 {
		t.Fatalf("preview = %#v", second)
	}
}

// Invoked by tests/test_screen_timelapse_purge.py in an isolated database
// whose base_screen_timelapse tables hold one moment (device "laptop" at
// 2026-10-25T14:03:00Z, displays 1 and 2, a tree, readings; a neighbour at
// 14:04 with display 2's bytes). Refuses a
// moment that matches nothing, proposes the real one, checks the preview
// carries ids and no text, and approves it through the review API, leaving
// the row for the real Python worker.
func TestScreenTimelapsePurgeProposalReviewApproval(t *testing.T) {
	url := os.Getenv("PDW_SCREEN_TIMELAPSE_PURGE_TEST_URL")
	if url == "" {
		t.Skip("run through tests/test_screen_timelapse_purge.py")
	}
	store, err := NewPostgresStore(url, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := NewService(store, Config{})
	server := newAPIServer(t, store)
	ctx := context.Background()

	for _, entry := range []map[string]any{
		{"device": "laptop", "captured_at": "2026-10-25T14:05:00Z"},
		{"device": "phone", "captured_at": "2026-10-25T14:03:00Z"},
		{"device": "laptop", "captured_at": "2026-10-25T14:03:00Z", "display_index": float64(3)},
	} {
		entry["type"] = ScreenTimelapsePurgeMomentOperation
		entry["reason"] = "isolated test"
		_, err := service.ProposeMutation(ctx, ProposeMutationInput{Title: "Purge nothing", Reason: "isolated test", Mutations: []map[string]any{entry}})
		if err == nil || !strings.Contains(err.Error(), "base_screen_timelapse.screenshots") {
			t.Fatalf("a purge of %#v must be refused, got %v", entry, err)
		}
	}

	proposal, err := service.ProposeMutation(ctx, ProposeMutationInput{
		Title: "Delete a leaked frame", Reason: "isolated test",
		Mutations: []map[string]any{{
			"type": ScreenTimelapsePurgeMomentOperation, "device": "laptop",
			"captured_at": "2026-10-25T14:03:00Z", "reason": "card number on screen",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := store.GetRequest(ctx, proposal.RequestID, AllMutations)
	if err != nil {
		t.Fatal(err)
	}
	preview := mapFromAny(request.Mutations[0].Preview["screen_timelapse_purge"])
	shots := mapSliceFromAny(preview["screenshots"])
	if len(shots) != 2 || shots[0]["storage_file_id"] != "drive-shot-1" || shots[1]["storage_file_id"] != "drive-shot-2" {
		t.Fatalf("preview screenshots = %#v", preview["screenshots"])
	}
	if mapFromAny(preview["accessibility_tree"])["storage_file_id"] != "drive-tree" || preview["removes_timeline"] != true {
		t.Fatalf("preview = %#v", preview)
	}
	if int64FromAny(preview["descriptions"]) != 3 || int64FromAny(preview["same_content_frames"]) != 1 {
		t.Fatalf("preview counts = %#v", preview)
	}
	raw, _ := json.Marshal(request)
	if bytes.Contains(raw, []byte("4111")) {
		t.Fatalf("the request must carry no text the frame showed: %s", raw)
	}

	response, err := http.Post(server.URL+"/api/mutations/requests/"+request.ID+"/approve", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if body := decodeBody(t, response); response.StatusCode != http.StatusOK {
		t.Fatalf("approve: status %d: %v", response.StatusCode, body)
	}
}
