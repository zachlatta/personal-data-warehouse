package mutations

import (
	"context"
	"strings"
	"testing"
)

const testKeepCard = "8537DF38-BF0D-4468-9061-D2D41468E05A:ABPerson"
const testMergeCard = "44C1F82A-1F0E-4C2B-9E1B-2A5E4F7B8C9D:ABPerson"

func appleContactsService(t *testing.T) (*Service, *recordingStore) {
	t.Helper()
	store := &recordingStore{request: Request{ID: "req-1", Status: "pending_review"}}
	service := NewService(store, Config{
		BaseURL:               "https://example.test",
		AppleContactsAccounts: []string{"you@example.com"},
	})
	return service, store
}

func TestProposeAppleContactsCreateStoresNormalizedContact(t *testing.T) {
	service, store := appleContactsService(t)

	if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Add Ada",
		Reason: "Zach worked with her this month and has no card",
		Mutations: []map[string]any{{
			"type":    AppleContactsCreateContactOperation,
			"account": "you@example.com",
			"contact": map[string]any{
				"given_name":   " Ada ",
				"family_name":  "Lovelace",
				"organization": "Hack Club",
				"emails":       []any{map[string]any{"label": "work", "value": "ada@example.com"}, map[string]any{"value": "ADA@example.com"}},
				"phones":       []any{"+1 802 555 0100"},
			},
		}},
	}); err != nil {
		t.Fatalf("ProposeMutation: %v", err)
	}
	stored, err := normalizeForStorage(store.createCalls[0])
	if err != nil {
		t.Fatalf("normalizeForStorage: %v", err)
	}
	mutation := stored[0]
	if mutation.Provider != AppleContactsProvider || mutation.Operation != AppleContactsCreateContactOperation {
		t.Fatalf("unexpected provider/operation %q/%q", mutation.Provider, mutation.Operation)
	}
	if mutation.Title != "Create contact: Ada Lovelace" {
		t.Fatalf("unexpected title %q", mutation.Title)
	}
	contact := mapFromAny(mutation.Payload["contact"])
	if got := stringFromAny(contact["given_name"]); got != "Ada" {
		t.Fatalf("given_name not trimmed: %q", got)
	}
	emails, _ := contact["emails"].([]map[string]any)
	if len(emails) != 1 {
		t.Fatalf("duplicate email should collapse case-insensitively, got %v", contact["emails"])
	}
	phones, _ := contact["phones"].([]map[string]any)
	if len(phones) != 1 || stringFromAny(phones[0]["value"]) != "+1 802 555 0100" {
		t.Fatalf("bare string phone should become an entry, got %v", contact["phones"])
	}
	preview := mapFromAny(mutation.Preview["contact"])
	if stringFromAny(preview["action"]) != "create" || stringFromAny(preview["name"]) != "Ada Lovelace" {
		t.Fatalf("unexpected preview %v", preview)
	}
}

func TestAppleContactsCreateRejectsWhatTheExecutorCouldNotRecoverFrom(t *testing.T) {
	service, _ := appleContactsService(t)
	cases := map[string]map[string]any{
		"no name or org":     {"note": "hi"},
		"bad email":          {"given_name": "A", "emails": []any{"not-an-email"}},
		"bad phone":          {"given_name": "A", "phones": []any{"12"}},
		"unknown field":      {"given_name": "A", "email": "a@b.c"},
		"note and append":    {"given_name": "A", "note": "x", "append_note": "y"},
		"append on a create": {"given_name": "A", "append_note": "y"},
	}
	for name, contact := range cases {
		_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
			Title: "t", Reason: "r",
			Mutations: []map[string]any{{"type": AppleContactsCreateContactOperation, "account": "you@example.com", "contact": contact}},
		})
		if err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
}

func TestAppleContactsUpdateNeedsACardIDAndAChange(t *testing.T) {
	service, _ := appleContactsService(t)
	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title: "t", Reason: "r",
		Mutations: []map[string]any{{"type": AppleContactsUpdateContactOperation, "account": "you@example.com", "card_id": "nope", "contact": map[string]any{"organization": "x"}}},
	})
	if err == nil || !strings.Contains(err.Error(), "card_id") {
		t.Fatalf("expected a card_id error, got %v", err)
	}
	_, err = service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title: "t", Reason: "r",
		Mutations: []map[string]any{{"type": AppleContactsUpdateContactOperation, "account": "you@example.com", "card_id": testKeepCard}},
	})
	if err == nil || !strings.Contains(err.Error(), "must change something") {
		t.Fatalf("expected a no-change error, got %v", err)
	}
}

func TestAppleContactsUpdatePreviewNamesRemovalsAsDestructive(t *testing.T) {
	stored, err := normalizeForStorage(CreateRequestInput{
		Title: "t", Reason: "r",
		Mutations: []MutationInput{{
			Type: AppleContactsUpdateContactOperation, Account: "you@example.com", CardID: testKeepCard,
			Contact: map[string]any{"organization": "Hack Club", "append_note": "seen 2026-09"},
			Remove:  map[string]any{"emails": []any{"old@example.com"}},
		}},
	})
	if err != nil {
		t.Fatalf("normalizeForStorage: %v", err)
	}
	preview := mapFromAny(stored[0].Preview["contact"])
	changes := stringSliceFromAny(preview["changes"])
	want := []string{"note (appended)", "organization", "emails (removed)"}
	if strings.Join(changes, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected changes %v", changes)
	}
	if stringFromAny(stored[0].Payload["card_id"]) != testKeepCard {
		t.Fatalf("payload should carry card_id, got %v", stored[0].Payload)
	}
}

func TestAppleContactsMergeValidation(t *testing.T) {
	service, _ := appleContactsService(t)
	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title: "t", Reason: "r",
		Mutations: []map[string]any{{"type": AppleContactsMergeContactsOperation, "account": "you@example.com", "keep_card_id": testKeepCard, "merge_card_ids": []any{testKeepCard}}},
	})
	if err == nil || !strings.Contains(err.Error(), "must not contain keep_card_id") {
		t.Fatalf("expected a self-merge error, got %v", err)
	}
	_, err = service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title: "t", Reason: "r",
		Mutations: []map[string]any{{"type": AppleContactsMergeContactsOperation, "account": "you@example.com", "keep_card_id": testKeepCard}},
	})
	if err == nil || !strings.Contains(err.Error(), "merge_card_ids") {
		t.Fatalf("expected a merge_card_ids error, got %v", err)
	}
	stored, err := normalizeForStorage(CreateRequestInput{
		Title: "t", Reason: "r",
		Mutations: []MutationInput{{
			Type: AppleContactsMergeContactsOperation, Account: "you@example.com",
			KeepCardID: testKeepCard, MergeCardIDs: []string{testMergeCard}, Contact: map[string]any{"family_name": "Lovelace"},
		}},
	})
	if err != nil {
		t.Fatalf("normalizeForStorage: %v", err)
	}
	if stored[0].Title != "Merge 2 contact cards into "+testKeepCard {
		t.Fatalf("unexpected title %q", stored[0].Title)
	}
	preview := mapFromAny(stored[0].Preview["contact"])
	changes := stringSliceFromAny(preview["changes"])
	if len(changes) == 0 || !strings.Contains(changes[len(changes)-1], "deleted after merge") {
		t.Fatalf("merge preview must name the deletion, got %v", changes)
	}
}

func TestAppleContactsRequiresAConfiguredAccount(t *testing.T) {
	store := &recordingStore{request: Request{ID: "req-1"}}
	service := NewService(store, Config{BaseURL: "https://example.test", AppleContactsAccounts: []string{"someone-else@example.com"}})
	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title: "t", Reason: "r",
		Mutations: []map[string]any{{"type": AppleContactsCreateContactOperation, "account": "you@example.com", "contact": map[string]any{"given_name": "A"}}},
	})
	if err == nil || !strings.Contains(err.Error(), "APPLE_CONTACTS_ACCOUNTS") {
		t.Fatalf("expected an account configuration error, got %v", err)
	}
}

func TestApplyAppleContactsCardRowsOrdersKeptCardFirstAndMarksMissing(t *testing.T) {
	mutations := []Mutation{{
		Provider:  AppleContactsProvider,
		Operation: AppleContactsMergeContactsOperation,
		Payload:   map[string]any{"keep_card_id": testKeepCard, "merge_card_ids": []any{testMergeCard}},
		Preview:   map[string]any{"contact": map[string]any{"action": "merge"}},
	}}
	ids := appleContactsCardIDsForMutations(mutations)
	if len(ids) != 2 || ids[0] != testKeepCard {
		t.Fatalf("unexpected ids %v", ids)
	}
	out := applyAppleContactsCardRows(mutations, map[string]map[string]any{
		testKeepCard: {"card_id": testKeepCard, "display_name": "Ada"},
	})
	cards, _ := mapFromAny(mapFromAny(out[0].Preview["contact"]))["cards"].([]map[string]any)
	if len(cards) != 2 || stringFromAny(cards[0]["display_name"]) != "Ada" || cards[1]["missing"] != true {
		t.Fatalf("unexpected cards %v", cards)
	}
}
