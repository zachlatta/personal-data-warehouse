package mutations

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// Invoked by tests/test_gmail_settings_e2e.py in an isolated database whose
// base_gmail tables hold, for gmail-settings@example.test: labels Newsletters
// (Label_7, on one message) and Old Label (Label_9), filter ANe1Bold
// (from news@example.com → Label_7, skip inbox), and three recent messages
// from shop@example.com in two threads plus one 200 days old. The proposals
// below are refused or stored exactly as the warehouse says they must be, the
// review reads like the rules, and the good request is approved for the real
// Python worker.
func TestGmailSettingsProposalReviewApproval(t *testing.T) {
	url := os.Getenv("PDW_GMAIL_SETTINGS_TEST_URL")
	if url == "" {
		t.Skip("run through tests/test_gmail_settings_e2e.py")
	}
	const account = "gmail-settings@example.test"
	store, err := NewPostgresStore(url, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := NewService(store, Config{GmailAccounts: []string{account}})
	ctx := context.Background()
	propose := func(entries ...map[string]any) (ProposalResponse, error) {
		for _, entry := range entries {
			entry["account"] = account
		}
		return service.ProposeMutation(ctx, ProposeMutationInput{Title: "Gmail rules", Reason: "isolated test", Mutations: entries})
	}

	for _, tc := range []struct {
		entry map[string]any
		want  string
	}{
		{map[string]any{"type": GmailDeleteFilterOperation, "filter_id": "nope"}, "base_gmail.filters"},
		{map[string]any{"type": GmailCreateLabelOperation, "name": "newsletters"}, "already exists"},
		{map[string]any{"type": GmailDeleteLabelOperation, "label": "INBOX"}, "system label"},
		{map[string]any{"type": GmailUpdateLabelOperation, "label": "Old Label", "new_name": "Newsletters"}, "already has that name"},
		{map[string]any{"type": GmailCreateFilterOperation, "criteria": map[string]any{"from": "x@example.com"}, "action": map[string]any{"add_labels": []any{"Nope"}}}, "create_and_add_labels"},
		{map[string]any{"type": GmailCreateFilterOperation, "criteria": map[string]any{"from": "NEWS@example.com"}, "action": map[string]any{"add_labels": []any{"Newsletters"}, "skip_inbox": true}}, "already does exactly this"},
	} {
		if _, err := propose(tc.entry); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%v: err = %v, want %q", tc.entry, err, tc.want)
		}
	}

	proposal, err := propose(
		map[string]any{"type": GmailCreateLabelOperation, "name": "Receipts"},
		map[string]any{"type": GmailCreateFilterOperation, "criteria": map[string]any{"from": "shop@example.com"}, "action": map[string]any{"add_labels": []any{"Receipts"}, "mark_read": true}},
		map[string]any{"type": GmailReplaceFilterOperation, "filter_id": "ANe1Bold", "criteria": map[string]any{"from": "news@example.com"}, "action": map[string]any{"add_labels": []any{"Newsletters"}, "skip_inbox": true, "mark_read": true}},
		map[string]any{"type": GmailDeleteLabelOperation, "label": "Old Label"},
		map[string]any{"type": GmailCreateFilterOperation, "criteria": map[string]any{"query": "list:announce"}, "action": map[string]any{"trash": true}},
	)
	if err != nil {
		t.Fatal(err)
	}
	request, err := store.GetRequest(ctx, proposal.RequestID, AllMutations)
	if err != nil {
		t.Fatal(err)
	}
	filter := mapFromAny(request.Mutations[1].Preview["gmail_filter"])
	if rule := mapFromAny(filter["rule"]); rule["text"] != `From shop@example.com → mark read, label "Receipts"` {
		t.Fatalf("rule = %#v", rule)
	}
	match := mapFromAny(filter["match"])
	if match["status"] != "approximate" || match["thread_count"] != int64(2) || match["message_count"] != int64(3) {
		t.Fatalf("match = %#v", match)
	}
	examples := mapSliceFromAny(match["examples"])
	if len(examples) != 2 || examples[0]["from_name"] != "Shop" || examples[0]["subject"] != "Order 2" {
		t.Fatalf("examples = %#v", examples)
	}

	replace := request.Mutations[2]
	if replace.Payload["filter_id"] != "ANe1Bold" || len(mapFromAny(replace.Payload["expected_filter"])) != 2 {
		t.Fatalf("replace payload = %#v", replace.Payload)
	}
	replaceView := mapFromAny(replace.Preview["gmail_filter"])
	if previous := mapFromAny(replaceView["previous_rule"]); previous["text"] != `From news@example.com → skip inbox, label "Newsletters"` {
		t.Fatalf("previous rule = %#v", previous)
	}

	deleteLabel := request.Mutations[3]
	if deleteLabel.Payload["label_id"] != "Label_9" || deleteLabel.Payload["expected_name"] != "Old Label" {
		t.Fatalf("delete label payload = %#v", deleteLabel.Payload)
	}
	labelView := mapFromAny(deleteLabel.Preview["gmail_label"])
	if int64FromAny(labelView["message_count"]) != 1 || len(stringSliceFromAny(labelView["warnings"])) == 0 {
		t.Fatalf("delete label view = %#v", labelView)
	}

	queryOnly := mapFromAny(request.Mutations[4].Preview["gmail_filter"])
	if mapFromAny(queryOnly["match"])["status"] != "unavailable" {
		t.Fatalf("a query-only rule must not claim a count: %#v", queryOnly["match"])
	}
	if !strings.Contains(strings.Join(stringSliceFromAny(queryOnly["warnings"]), " "), "Trash") {
		t.Fatalf("a trash rule must warn: %#v", queryOnly["warnings"])
	}

	if _, err := store.ApproveRequest(ctx, proposal.RequestID, "app:test"); err != nil {
		t.Fatal(err)
	}
}
