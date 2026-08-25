package mutations

import (
	"context"
	"strings"
	"testing"
)

func appleNotesService(t *testing.T) (*Service, *recordingStore) {
	t.Helper()
	store := &recordingStore{request: Request{ID: "req-1", Status: "pending_review"}}
	service := NewService(store, Config{
		BaseURL:            "https://example.test",
		AppleNotesAccounts: []string{"you@example.com"},
	})
	return service, store
}

func TestProposeAppleNotesCreateNoteStoresPayloadAndPreview(t *testing.T) {
	service, store := appleNotesService(t)

	if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Capture the runway numbers",
		Reason: "Zach asked for a note with the current burn figures",
		Mutations: []map[string]any{{
			"type":    AppleNotesCreateNoteOperation,
			"account": "you@example.com",
			"folder":  "PDW Agent Sandbox",
			"name":    "Runway",
			"body":    "12 months of cash remaining",
		}},
	}); err != nil {
		t.Fatalf("ProposeMutation: %v", err)
	}

	if len(store.createCalls) != 1 {
		t.Fatalf("expected 1 stored request, got %d", len(store.createCalls))
	}
	stored := store.createCalls[0].Mutations
	if len(stored) != 1 {
		t.Fatalf("expected 1 stored mutation, got %d", len(stored))
	}
	if stored[0].Type != AppleNotesCreateNoteOperation {
		t.Fatalf("unexpected stored type %q", stored[0].Type)
	}
	if stored[0].Folder != "PDW Agent Sandbox" {
		t.Fatalf("unexpected folder %q", stored[0].Folder)
	}
	if stored[0].Name != "Runway" {
		t.Fatalf("unexpected name %q", stored[0].Name)
	}
}

func TestAppleNotesCreateNoteNormalizesToStoredMutation(t *testing.T) {
	stored, err := normalizeForStorage(CreateRequestInput{
		Title:  "Capture the runway numbers",
		Reason: "because Zach asked",
		Mutations: []MutationInput{{
			Type:    AppleNotesCreateNoteOperation,
			Account: "you@example.com",
			Folder:  "PDW Agent Sandbox",
			Name:    "Runway",
			Body:    "12 months of cash remaining",
		}},
	})
	if err != nil {
		t.Fatalf("normalizeForStorage: %v", err)
	}
	if len(stored) != 1 {
		t.Fatalf("expected 1 stored mutation, got %d", len(stored))
	}
	mutation := stored[0]
	if mutation.Provider != AppleNotesProvider {
		t.Fatalf("unexpected provider %q", mutation.Provider)
	}
	if mutation.Operation != AppleNotesCreateNoteOperation {
		t.Fatalf("unexpected operation %q", mutation.Operation)
	}
	if got := stringFromAny(mutation.Payload["folder"]); got != "PDW Agent Sandbox" {
		t.Fatalf("unexpected payload folder %q", got)
	}
	if got := stringFromAny(mutation.Payload["name"]); got != "Runway" {
		t.Fatalf("unexpected payload name %q", got)
	}
	if got := stringFromAny(mutation.Payload["body"]); got != "12 months of cash remaining" {
		t.Fatalf("unexpected payload body %q", got)
	}
	preview := mapFromAny(mutation.Preview["note"])
	if got := stringFromAny(preview["action"]); got != "create" {
		t.Fatalf("unexpected preview action %q", got)
	}
	if got := stringFromAny(preview["body_preview"]); !strings.Contains(got, "12 months") {
		t.Fatalf("preview should carry the body text, got %q", got)
	}
}

func TestAppleNotesUpdateNoteRequiresNoteIDAndAChange(t *testing.T) {
	service, _ := appleNotesService(t)

	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Edit note",
		Reason: "testing",
		Mutations: []map[string]any{{
			"type":    AppleNotesUpdateNoteOperation,
			"account": "you@example.com",
			"body":    "new body",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "note_id") {
		t.Fatalf("expected a note_id error, got %v", err)
	}

	_, err = service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Edit note",
		Reason: "testing",
		Mutations: []map[string]any{{
			"type":    AppleNotesUpdateNoteOperation,
			"account": "you@example.com",
			"note_id": "x-coredata://ABC/ICNote/p1",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "name, body, or append_body") {
		t.Fatalf("expected a no-change error, got %v", err)
	}
}

func TestAppleNotesUpdateNoteRejectsBodyAndAppendTogether(t *testing.T) {
	service, _ := appleNotesService(t)

	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Edit note",
		Reason: "testing",
		Mutations: []map[string]any{{
			"type":        AppleNotesUpdateNoteOperation,
			"account":     "you@example.com",
			"note_id":     "x-coredata://ABC/ICNote/p1",
			"body":        "replacement",
			"append_body": "extra",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("expected a mutually-exclusive error, got %v", err)
	}
}

func TestAppleNotesRejectsUnconfiguredAccount(t *testing.T) {
	service, _ := appleNotesService(t)

	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Make a note",
		Reason: "testing",
		Mutations: []map[string]any{{
			"type":    AppleNotesCreateNoteOperation,
			"account": "someone@else.com",
			"body":    "hi",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "APPLE_NOTES_ACCOUNTS") {
		t.Fatalf("expected an APPLE_NOTES_ACCOUNTS error, got %v", err)
	}
}

func TestAppleNotesCreateNoteRequiresBody(t *testing.T) {
	service, _ := appleNotesService(t)

	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Make a note",
		Reason: "testing",
		Mutations: []map[string]any{{
			"type":    AppleNotesCreateNoteOperation,
			"account": "you@example.com",
			"name":    "Title only",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "body") {
		t.Fatalf("expected a body error, got %v", err)
	}
}

func TestMutationHelpDocumentsAppleNotes(t *testing.T) {
	help := MutationHelp()
	seen := map[string]bool{}
	for _, entry := range help.Mutations {
		seen[entry.Type] = true
	}
	if !seen[AppleNotesCreateNoteOperation] || !seen[AppleNotesUpdateNoteOperation] {
		t.Fatalf("propose_mutation_help must document the Apple Notes operations, got %v", seen)
	}
}
