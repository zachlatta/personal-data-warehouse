package mutations

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestDriveProposalReviewApproval(t *testing.T) {
	url := os.Getenv("PDW_DRIVE_TEST_URL")
	if url == "" {
		t.Skip("run via tests/test_google_drive_mutations_e2e.py")
	}
	store, err := NewPostgresStore(url, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := NewService(store, Config{GoogleDriveAccounts: []string{"drive@example.test"}})
	ctx := context.Background()
	propose := func(account, parent string) (ProposalResponse, error) {
		return service.ProposeMutation(ctx, ProposeMutationInput{Title: "Create project folder", Reason: "isolated test", Mutations: []map[string]any{{"type": "google_drive.create_folder", "account": account, "parent_id": parent, "name": "Project"}}})
	}
	if _, err := propose("other@example.test", "parent"); err == nil {
		t.Fatal("accepted unconfigured account")
	}
	if _, err := propose("drive@example.test", "missing"); err == nil {
		t.Fatal("accepted missing folder")
	}
	response, err := propose("drive@example.test", "parent")
	if err != nil {
		t.Fatal(err)
	}
	request, err := store.GetRequest(ctx, response.RequestID, AllMutations)
	if err != nil {
		t.Fatal(err)
	}
	if len(request.Mutations) != 1 || request.Mutations[0].Status != "pending_review" {
		t.Fatalf("%#v", request)
	}
	preview := mapFromAny(request.Mutations[0].Preview["google_drive"])
	if preview["parent_id_name"] != "Destination" || preview["name"] != "Project" {
		t.Fatalf("%#v", preview)
	}
	repeat, err := propose("drive@example.test", "parent")
	if err != nil || repeat.RequestID != response.RequestID {
		t.Fatalf("duplicate: %#v %v", repeat, err)
	}
	if _, err := store.ApproveRequest(ctx, response.RequestID, "app:test"); err != nil {
		t.Fatal(err)
	}
}
