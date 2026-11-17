package mutations

import (
	"context"
	"os"
	"testing"
	"time"
)

// Called only after the real source runner populated root, empty, shared-root
// and just-created folder rows from a fake Drive API (no INSERT fixtures).
func TestDriveSyncedFoldersAcceptMutations(t *testing.T) {
	url := os.Getenv("PDW_DRIVE_TEST_URL")
	if url == "" {
		t.Skip("run through Python sync e2e")
	}
	store, err := NewPostgresStore(url, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.stageUploads = func(_ context.Context, ms []storedMutation) error {
		for i := range ms {
			delete(ms[i].Payload, "data_base64")
			ms[i].Payload["staging_key"] = "pending/test"
		}
		return nil
	}
	svc := NewService(store, Config{GoogleDriveAccounts: []string{"drive@example.test"}})
	for _, id := range []string{"parent", "empty-folder", "shared-root", "new-folder"} {
		for _, op := range []string{GoogleDriveUploadFileOperation, GoogleDriveCreateFolderOperation, GoogleDriveAddPermissionOperation} {
			entry := map[string]any{"type": op, "account": "drive@example.test"}
			if op == GoogleDriveAddPermissionOperation {
				entry["file_id"] = id
				entry["permission_type"] = "anyone"
				entry["role"] = "reader"
				entry["send_notification_email"] = false
			} else {
				entry["name"] = "New item"
				entry["parent_id"] = id
			}
			if op == GoogleDriveUploadFileOperation {
				entry["mime_type"] = "text/plain"
				entry["data_base64"] = "aGVsbG8="
			}
			if _, err := svc.ProposeMutation(context.Background(), ProposeMutationInput{Title: op + " " + id, Reason: "real sync regression", Mutations: []map[string]any{entry}}); err != nil {
				t.Fatalf("%s %s: %v", op, id, err)
			}
		}
	}
}

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
		return service.ProposeMutation(ctx, ProposeMutationInput{Title: "Create project folder", Reason: "isolated test", Mutations: []map[string]any{{"type": "google_drive.create_folder", "account": account, "parent_id": parent, "name": "Project", "initial_permissions": []any{map[string]any{"permission_type": "user", "email_address": "person@example.com", "role": "reader", "send_notification_email": false}}}}})
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
	permissions := mapSliceFromAny(preview["initial_permissions"])
	if len(permissions) != 1 || permissions[0]["email_address"] != "person@example.com" || permissions[0]["send_notification_email"] != false {
		t.Fatalf("sharing missing from review: %#v", preview)
	}
	repeat, err := propose("drive@example.test", "parent")
	if err != nil || repeat.RequestID != response.RequestID {
		t.Fatalf("duplicate: %#v %v", repeat, err)
	}
	if _, err := store.ApproveRequest(ctx, response.RequestID, "app:test"); err != nil {
		t.Fatal(err)
	}
}

func TestDriveUploadReproposalAfterTerminalOrExpiry(t *testing.T) {
	url := os.Getenv("PDW_DRIVE_TEST_URL")
	if url == "" {
		t.Skip("run via Python e2e")
	}
	store, err := NewPostgresStore(url, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	staged := 0
	store.stageUploads = func(_ context.Context, ms []storedMutation) error {
		staged++
		for i := range ms {
			delete(ms[i].Payload, "data_base64")
			ms[i].Payload["staging_key"] = "pending/test"
		}
		return nil
	}
	ctx := context.Background()
	service := NewService(store, Config{GoogleDriveAccounts: []string{"drive@example.test"}})
	propose := func() (ProposalResponse, error) {
		return service.ProposeMutation(ctx, ProposeMutationInput{Title: "Upload", Reason: "test", Mutations: []map[string]any{{"type": "google_drive.upload_file", "account": "drive@example.test", "name": "report.txt", "parent_id": "parent", "mime_type": "text/plain", "data_base64": "aGVsbG8="}}})
	}
	first, err := propose()
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"failed_terminal", "rejected", "withdrawn", "expired"} {
		if status == "expired" {
			if _, err := execContext(ctx, store.db, `UPDATE @upstream_mutation_requests SET created_at=now()-interval '8 days' WHERE id=$1`, first.RequestID); err != nil {
				t.Fatal(err)
			}
			_, err := service.ProposeMutation(ctx, ProposeMutationInput{Title: "Upload", Reason: "test", ReplacesRequestID: "another-request", ReplacesReason: "test", Mutations: []map[string]any{{"type": "google_drive.upload_file", "account": "drive@example.test", "name": "report.txt", "parent_id": "parent", "mime_type": "text/plain", "data_base64": "aGVsbG8="}}})
			if err == nil {
				t.Fatal("accepted unrelated replacement while leaving expired upload approvable")
			}
		} else {
			if _, err := execContext(ctx, store.db, `UPDATE @upstream_mutations SET status=$1 WHERE request_id=$2`, status, first.RequestID); err != nil {
				t.Fatal(err)
			}
			requestStatus := status
			if status == "failed_terminal" {
				requestStatus = "failed"
			}
			if _, err := execContext(ctx, store.db, `UPDATE @upstream_mutation_requests SET status=$1 WHERE id=$2`, requestStatus, first.RequestID); err != nil {
				t.Fatal(err)
			}
		}
		next, err := propose()
		if err != nil {
			t.Fatal(err)
		}
		if next.RequestID == first.RequestID || next.Status != "pending_review" {
			t.Fatalf("%s returned dead request: %#v", status, next)
		}
		repeat, err := propose()
		if err != nil || repeat.RequestID != next.RequestID {
			t.Fatalf("pending request did not deduplicate: %v", err)
		}
		if status == "expired" {
			old, err := store.GetRequest(ctx, first.RequestID, AllMutations)
			if err != nil || old.Status != "withdrawn" {
				t.Fatalf("expired request remains approvable: %#v %v", old, err)
			}
		}
		first = next
	}
	if staged != 5 {
		t.Fatalf("staged %d times, want 5", staged)
	}
	if _, err := execContext(ctx, store.db, `UPDATE @upstream_mutation_requests SET revision=2, created_at=now()-interval '8 days' WHERE id=$1`, first.RequestID); err != nil {
		t.Fatal(err)
	}
	if _, err := propose(); err == nil {
		t.Fatal("silently replaced reviewer-edited expired upload")
	}
	if staged != 5 {
		t.Fatal("staged bytes despite refusing edited request")
	}
}
