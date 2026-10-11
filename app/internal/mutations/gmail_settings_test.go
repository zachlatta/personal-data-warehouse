package mutations

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func gmailSettingsService(t *testing.T) (*Service, *recordingStore) {
	t.Helper()
	store := &recordingStore{request: Request{ID: "req-1", Status: "pending_review"}}
	return NewService(store, Config{BaseURL: "https://example.test", GmailAccounts: []string{"you@example.com"}}), store
}

// jsonEntry decodes like the tool layer does, so numbers are float64.
func jsonEntry(t *testing.T, raw string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestProposeGmailCreateFilterStoresTheRuleAndReadsLikeIt(t *testing.T) {
	service, store := gmailSettingsService(t)
	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title: "Newsletter rule", Reason: "Zach asked",
		Mutations: []map[string]any{jsonEntry(t, `{
			"type": "gmail.create_filter", "account": "you@example.com",
			"criteria": {"from": " newsletter@x.com ", "hasAttachment": false},
			"action": {"skip_inbox": true, "add_labels": ["Newsletters"], "star": false}
		}`)},
	})
	if err != nil {
		t.Fatalf("ProposeMutation: %v", err)
	}
	stored, err := normalizeForStorage(store.createCalls[0])
	if err != nil {
		t.Fatal(err)
	}
	got := stored[0]
	if got.Provider != "gmail" || got.Operation != GmailCreateFilterOperation {
		t.Fatalf("stored = %#v", got)
	}
	criteria := mapFromAny(got.Payload["criteria"])
	if len(criteria) != 1 || criteria["from"] != "newsletter@x.com" {
		t.Fatalf("criteria = %#v", criteria)
	}
	action := mapFromAny(got.Payload["action"])
	if len(action) != 2 || action["skip_inbox"] != true {
		t.Fatalf("action = %#v", action)
	}
	rule := mapFromAny(mapFromAny(got.Preview["gmail_filter"])["rule"])
	if rule["text"] != `From newsletter@x.com → skip inbox, label "Newsletters"` {
		t.Fatalf("rule = %#v", rule)
	}
	if got.Title != `Create Gmail filter: From newsletter@x.com → skip inbox, label "Newsletters"` {
		t.Fatalf("title = %q", got.Title)
	}
}

func TestProposeGmailFilterRefusals(t *testing.T) {
	for _, tc := range []struct {
		entry string
		want  string
	}{
		{`{"type":"gmail.create_filter","account":"you@example.com","criteria":{"from":"a@x.com"},"action":{"forward":"b@y.com"}}`, "forward"},
		{`{"type":"gmail.create_filter","account":"you@example.com","criteria":{"from":"a@x.com"},"action":{"star":true},"forward":"b@y.com"}`, "forward"},
		{`{"type":"gmail.create_filter","account":"you@example.com","criteria":{},"action":{"star":true}}`, "criteria"},
		{`{"type":"gmail.create_filter","account":"you@example.com","criteria":{"exclude_chats":true},"action":{"star":true}}`, "which mail"},
		{`{"type":"gmail.create_filter","account":"you@example.com","criteria":{"from":"  "},"action":{"star":true}}`, "blank"},
		{`{"type":"gmail.create_filter","account":"you@example.com","criteria":{"sender":"a@x.com"},"action":{"star":true}}`, "does not take sender"},
		{`{"type":"gmail.create_filter","account":"you@example.com","criteria":{"from":"a@x.com"},"action":{}}`, "action"},
		{`{"type":"gmail.create_filter","account":"you@example.com","criteria":{"from":"a@x.com"},"action":{"star":false}}`, "do something"},
		{`{"type":"gmail.create_filter","account":"you@example.com","criteria":{"from":"a@x.com"},"action":{"archive":true}}`, "does not take archive"},
		{`{"type":"gmail.create_filter","account":"you@example.com","criteria":{"from":"a@x.com"},"action":{"add_labels":["INBOX"]}}`, "skip_inbox"},
		{`{"type":"gmail.create_filter","account":"you@example.com","criteria":{"from":"a@x.com"},"action":{"mark_important":true,"never_important":true}}`, "both"},
		{`{"type":"gmail.create_filter","account":"you@example.com","criteria":{"from":"a@x.com"},"action":{"category":"news"}}`, "category"},
		{`{"type":"gmail.create_filter","account":"you@example.com","criteria":{"size":100},"action":{"star":true}}`, "size_comparison"},
		{`{"type":"gmail.create_filter","account":"you@example.com","criteria":{"from":"a@x.com"},"action":{"add_labels":["A"],"remove_labels":["a"]}}`, "both"},
		{`{"type":"gmail.create_filter","account":"other@example.com","criteria":{"from":"a@x.com"},"action":{"star":true}}`, "GMAIL_ACCOUNTS"},
		{`{"type":"gmail.replace_filter","account":"you@example.com","criteria":{"from":"a@x.com"},"action":{"star":true}}`, "filter_id"},
		{`{"type":"gmail.delete_filter","account":"you@example.com"}`, "filter_id"},
		{`{"type":"gmail.create_label","account":"you@example.com","name":"Inbox"}`, "system label"},
		{`{"type":"gmail.create_label","account":"you@example.com","name":"A//B"}`, "nesting"},
		{`{"type":"gmail.create_label","account":"you@example.com","name":"A","color":{"background_color":"#123456","text_color":"#ffffff"}}`, "palette"},
		{`{"type":"gmail.create_label","account":"you@example.com","name":"A","label_list_visibility":"visible"}`, "labelShow"},
		{`{"type":"gmail.update_label","account":"you@example.com","label":"A"}`, "change something"},
		{`{"type":"gmail.update_label","account":"you@example.com","label":"A","rename":"B"}`, "does not take rename"},
		{`{"type":"gmail.delete_label","account":"you@example.com"}`, "label"},
	} {
		service, store := gmailSettingsService(t)
		_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{Title: "t", Reason: "r", Mutations: []map[string]any{jsonEntry(t, tc.entry)}})
		if err == nil || !strings.Contains(err.Error(), tc.want) || len(store.createCalls) != 0 {
			t.Fatalf("%s: err = %v, want %q", tc.entry, err, tc.want)
		}
	}
}

func TestGmailLabelProposalsStoreTheirChange(t *testing.T) {
	service, store := gmailSettingsService(t)
	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title: "Labels", Reason: "tidy",
		Mutations: []map[string]any{
			jsonEntry(t, `{"type":"gmail.create_label","account":"you@example.com","name":"pdw-test","color":{"background_color":"#16A766","text_color":"#ffffff"}}`),
			jsonEntry(t, `{"type":"gmail.update_label","account":"you@example.com","label":"Old","new_name":"New"}`),
			jsonEntry(t, `{"type":"gmail.delete_label","account":"you@example.com","label":"Label_9"}`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := normalizeForStorage(store.createCalls[0])
	if err != nil {
		t.Fatal(err)
	}
	if color := mapFromAny(stored[0].Payload["color"]); color["background_color"] != "#16a766" {
		t.Fatalf("color = %#v", color)
	}
	if stored[0].Title != `Create Gmail label "pdw-test"` || stored[1].Title != `Rename Gmail label "Old" to "New"` || stored[2].Title != `Delete Gmail label "Label_9"` {
		t.Fatalf("titles = %q / %q / %q", stored[0].Title, stored[1].Title, stored[2].Title)
	}
	if kind := mapFromAny(stored[2].Preview["gmail_label"])["kind"]; kind != "delete" {
		t.Fatalf("kind = %v", kind)
	}
}

func TestGmailFilterAPIActionsReadAnExistingRuleInTheSameWords(t *testing.T) {
	criteria := gmailCriteriaFromAPI(map[string]any{"from": "a@x.com", "hasAttachment": true, "size": float64(5 << 20), "sizeComparison": "larger"})
	actions := gmailFilterAPIActions(map[string]any{
		"addLabelIds":    []any{"Label_7", "STARRED", "CATEGORY_PERSONAL", "TRASH"},
		"removeLabelIds": []any{"INBOX", "UNREAD", "SPAM", "Label_8"},
	}, map[string]string{"Label_7": "Newsletters"})
	rule := gmailRule(gmailFilterWhen(criteria), actions)
	want := `From a@x.com, Has attachment, Larger than 5 MB → skip inbox, mark read, label "Newsletters", star, remove label "Label_8", never send to spam, categorize as Primary, delete it`
	if rule["text"] != want {
		t.Fatalf("rule = %q\nwant  %q", rule["text"], want)
	}
}

// The help is what agents copy; every Gmail filter/label example must be a
// proposal the validator accepts, and the notes must carry the rules that
// are not visible in a field list.
func TestMutationHelpGmailSettingsExamplesAreValidProposals(t *testing.T) {
	seen := map[string]bool{}
	for _, entry := range MutationHelp().Mutations {
		if !isGmailSettingsOperation(entry.Type) {
			continue
		}
		seen[entry.Type] = true
		raw, err := json.Marshal(entry.Example)
		if err != nil {
			t.Fatal(err)
		}
		example := jsonEntry(t, string(raw))
		example["account"] = "you@example.com"
		service, _ := gmailSettingsService(t)
		if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{Title: "t", Reason: "r", Mutations: []map[string]any{example}}); err != nil {
			t.Fatalf("%s example is refused: %v", entry.Type, err)
		}
	}
	for _, operation := range []string{GmailCreateFilterOperation, GmailReplaceFilterOperation, GmailDeleteFilterOperation, GmailCreateLabelOperation, GmailUpdateLabelOperation, GmailDeleteLabelOperation} {
		if !seen[operation] {
			t.Fatalf("propose_mutation_help does not document %s", operation)
		}
	}
	notes := ""
	for _, entry := range MutationHelp().Mutations {
		if entry.Type == GmailCreateFilterOperation {
			notes = entry.ExtraNotes
		}
	}
	for _, want := range []string{"forward", "new mail", "gmail.modify_thread_labels", "base_gmail.labels"} {
		if !strings.Contains(notes, want) {
			t.Fatalf("create_filter notes do not mention %q: %q", want, notes)
		}
	}
}
