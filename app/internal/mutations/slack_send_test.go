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

func slackSendService(store Store) *Service {
	return NewService(store, Config{BaseURL: "https://mcp.example.test", SlackAccounts: []string{"zrl"}})
}

func TestProposeMutationSlackSendMessageToAConversationThreadAndPerson(t *testing.T) {
	store := &recordingStore{request: Request{ID: "req-slack-send", Status: "pending_review", Mutations: []Mutation{{ID: "mut-1"}, {ID: "mut-2"}, {ID: "mut-3"}}}}
	service := slackSendService(store)

	response, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Answer three people",
		Reason: "they asked",
		Mutations: []map[string]any{
			{"type": SlackSendMessageOperation, "account": "ZRL", "conversation_id": "C012ABCDEF", "text": "  On it.  "},
			{"type": SlackSendMessageOperation, "account": "zrl", "conversation_id": "C012ABCDEF", "thread_ts": "1593473566.000200", "reply_broadcast": "true", "text": "Replying in the thread."},
			{"type": SlackSendMessageOperation, "account": "zrl", "user_id": "U0ABCDEF1", "text": "Hi — quick question."},
		},
	})
	if err != nil {
		t.Fatalf("ProposeMutation returned error: %v", err)
	}
	if response.RequestID != "req-slack-send" || response.Status != "pending_review" {
		t.Fatalf("unexpected response: %#v", response)
	}
	inputs := store.createCalls[0].Mutations
	if len(inputs) != 3 {
		t.Fatalf("stored %d mutations: %#v", len(inputs), inputs)
	}
	if inputs[0].Account != "zrl" || inputs[0].ConversationID != "C012ABCDEF" || strings.TrimSpace(inputs[0].Text) != "On it." {
		t.Fatalf("conversation send = %#v", inputs[0])
	}
	if inputs[1].ThreadTS != "1593473566.000200" || !inputs[1].ReplyBroadcast {
		t.Fatalf("thread reply = %#v", inputs[1])
	}
	if inputs[2].UserID != "U0ABCDEF1" || inputs[2].ConversationID != "" {
		t.Fatalf("direct message = %#v", inputs[2])
	}
}

func TestProposeMutationSlackSendMessageRejectsWhatTheExecutorWouldRefuse(t *testing.T) {
	service := slackSendService(&recordingStore{})
	long := strings.Repeat("x", slackMessageTextMaxLength+1)
	tests := []struct {
		name string
		raw  map[string]any
		want string
	}{
		{name: "no recipient", raw: map[string]any{"text": "hi"}, want: "conversation_id"},
		{name: "two recipients", raw: map[string]any{"conversation_id": "C1", "user_id": "U1", "text": "hi"}, want: "not both"},
		{name: "bad conversation", raw: map[string]any{"conversation_id": "not-a-channel", "text": "hi"}, want: "conversation_id must be"},
		{name: "bad user", raw: map[string]any{"user_id": "C1", "text": "hi"}, want: "user_id must be"},
		{name: "blank text", raw: map[string]any{"conversation_id": "C1", "text": "   "}, want: "text"},
		{name: "long text", raw: map[string]any{"conversation_id": "C1", "text": long}, want: "4000"},
		{name: "bad thread", raw: map[string]any{"conversation_id": "C1", "thread_ts": "yesterday", "text": "hi"}, want: "thread_ts"},
		{name: "thread without conversation", raw: map[string]any{"user_id": "U1", "thread_ts": "1.2", "text": "hi"}, want: "needs conversation_id"},
		{name: "broadcast without thread", raw: map[string]any{"conversation_id": "C1", "reply_broadcast": true, "text": "hi"}, want: "reply_broadcast"},
		{name: "unconfigured account", raw: map[string]any{"account": "someone-else", "conversation_id": "C1", "text": "hi"}, want: "SLACK_ACCOUNTS"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw := map[string]any{"type": SlackSendMessageOperation, "account": "zrl"}
			for key, value := range test.raw {
				raw[key] = value
			}
			_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{Title: "Send", Reason: "test", Mutations: []map[string]any{raw}})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %q error, got %v", test.want, err)
			}
			var inputErr *proposalInputError
			if !errors.As(err, &inputErr) {
				t.Fatalf("validation error is not a proposal input error: %T", err)
			}
		})
	}
}

func TestNormalizeSlackSendMessageForStorage(t *testing.T) {
	stored, err := normalizeForStorage(CreateRequestInput{
		Reason:  "they asked",
		Context: map[string]any{"source": "timeline"},
		Mutations: []MutationInput{
			{Type: SlackSendMessageOperation, Account: "zrl", ConversationID: "C012ABCDEF", ThreadTS: "1593473566.000200", ReplyBroadcast: true, Text: " Replying. "},
			{Type: SlackSendMessageOperation, Account: "zrl", UserID: "U0ABCDEF1", Text: "Hi."},
		},
	})
	if err != nil {
		t.Fatalf("normalizeForStorage: %v", err)
	}
	if len(stored) != 2 {
		t.Fatalf("stored = %#v", stored)
	}
	reply := stored[0]
	if reply.Provider != SlackProvider || reply.Operation != SlackSendMessageOperation || reply.Reason != "they asked" {
		t.Fatalf("stored reply = %#v", reply)
	}
	if reply.Payload["conversation_id"] != "C012ABCDEF" || reply.Payload["thread_ts"] != "1593473566.000200" || reply.Payload["text"] != "Replying." || reply.Payload["reply_broadcast"] != true || reply.Payload["user_id"] != "" {
		t.Fatalf("reply payload = %#v", reply.Payload)
	}
	if reply.Title != "Reply in Slack thread in C012ABCDEF" {
		t.Fatalf("reply title = %q", reply.Title)
	}
	preview := mapFromAny(reply.Preview["slack_message"])
	if preview["delivery"] != slackSendDeliveryThreadReply || !strings.Contains(stringFromAny(preview["effect"]), "cannot be unsent") || !strings.Contains(stringFromAny(preview["effect"]), "broadcast") {
		t.Fatalf("reply preview must state its irreversible effect: %#v", preview)
	}
	if mapFromAny(reply.Preview["context"])["source"] != "timeline" {
		t.Fatalf("preview lost the request context: %#v", reply.Preview)
	}
	dm := stored[1]
	if dm.Payload["reply_broadcast"] != false || dm.Payload["conversation_id"] != "" || dm.Payload["user_id"] != "U0ABCDEF1" || dm.Payload["thread_ts"] != "" {
		t.Fatalf("dm payload = %#v", dm.Payload)
	}
	if dm.Title != "Send Slack DM to U0ABCDEF1" || mapFromAny(dm.Preview["slack_message"])["delivery"] != slackSendDeliveryDM {
		t.Fatalf("dm stored = %#v", dm)
	}
}

func TestUpdatedSlackMessagePayloadChangesOnlyTheText(t *testing.T) {
	mutation := Mutation{
		Provider: SlackProvider, Operation: SlackSendMessageOperation, Title: "Reply in Slack thread in #ops",
		Payload: map[string]any{"conversation_id": "C1", "user_id": "", "text": "Old words", "thread_ts": "1.2", "reply_broadcast": false},
		Preview: map[string]any{"slack_message": map[string]any{"conversation_id": "C1", "text": "Old words", "recipient_label": "#ops", "warnings": []any{}}},
	}
	payload, preview, title, err := updatedSlackMessagePayload(mutation, UpdateSlackMessageMutationInput{Text: "  New words.  "})
	if err != nil {
		t.Fatalf("updatedSlackMessagePayload: %v", err)
	}
	if payload["text"] != "New words." || payload["conversation_id"] != "C1" || payload["thread_ts"] != "1.2" {
		t.Fatalf("payload = %#v", payload)
	}
	slackMessage := mapFromAny(preview["slack_message"])
	if slackMessage["text"] != "New words." || slackMessage["edited"] != true || slackMessage["recipient_label"] != "#ops" {
		t.Fatalf("preview = %#v", slackMessage)
	}
	if title != mutation.Title {
		t.Fatalf("title changed to %q", title)
	}
	// The stored mutation is untouched: the caller decides what to persist.
	if mutation.Payload["text"] != "Old words" || mapFromAny(mutation.Preview["slack_message"])["text"] != "Old words" {
		t.Fatalf("edit mutated its input: %#v", mutation)
	}
	for _, bad := range []string{"", "   ", strings.Repeat("y", slackMessageTextMaxLength+1)} {
		if _, _, _, err := updatedSlackMessagePayload(mutation, UpdateSlackMessageMutationInput{Text: bad}); err == nil {
			t.Fatalf("edit accepted %q", bad)
		}
	}
}

func TestApplySlackSendMessagePreviewDetailsNamesTheRecipientAndWarns(t *testing.T) {
	parentAt := time.Date(2026, 9, 24, 14, 0, 0, 0, time.UTC)
	payload := map[string]any{"conversation_id": "C1", "user_id": "", "text": "On it.", "thread_ts": "1593473566.000200", "reply_broadcast": false}
	mutations := []storedMutation{
		{
			Provider: SlackProvider, Operation: SlackSendMessageOperation, Account: "zrl",
			Title:   slackSendMessageTitle(payload),
			Payload: payload,
			Preview: map[string]any{"slack_message": slackSendMessagePreview(payload)},
		},
		{
			Provider: SlackProvider, Operation: SlackSendMessageOperation, Account: "zrl",
			Title:   "Hand-written title",
			Payload: map[string]any{"conversation_id": "", "user_id": "U-GONE", "text": "Hi", "thread_ts": "", "reply_broadcast": false},
			Preview: map[string]any{"slack_message": slackSendMessagePreview(map[string]any{"user_id": "U-GONE", "text": "Hi"})},
		},
	}
	contexts := []slackSendPreviewContext{
		{
			Key: slackSendPreviewKey{Account: "zrl", ConversationID: "C1", ThreadTS: "1593473566.000200"},
			Detail: slackSendPreviewDetail{
				Account: "zrl", TeamID: "T1", TeamDomain: "example", SelfUserID: "U-ME",
				ConversationID: "C1", ConversationFound: true, ConversationType: "public_channel", ConversationName: "ops",
				IsMember: false, ThreadTS: "1593473566.000200", ThreadFound: true,
			},
			Rows: []slackMarkReadPreviewRow{
				{Account: "zrl", ConversationID: "C1", MessageTS: "1593473600.000300", SentAt: parentAt.Add(time.Minute), UserID: "U-ME", ActorName: "You", Text: "later reply", ThreadTS: "1593473566.000200", IsFromMe: true},
				{Account: "zrl", ConversationID: "C1", MessageTS: "1593473566.000200", SentAt: parentAt, UserID: "U-MARCUS", ActorName: "Marcus", Text: "Can someone look?", ThreadTS: "1593473566.000200"},
			},
		},
		{
			Key:    slackSendPreviewKey{Account: "zrl", UserID: "U-GONE"},
			Detail: slackSendPreviewDetail{Account: "zrl", TeamID: "T1", RecipientUserID: "U-GONE", RecipientFound: false},
		},
	}

	applySlackSendMessagePreviewDetails(mutations, contexts)

	reply := mapFromAny(mutations[0].Preview["slack_message"])
	if reply["team_id"] != "T1" || reply["conversation_type"] != "public_channel" || reply["recipient_label"] != "#ops" {
		t.Fatalf("reply preview = %#v", reply)
	}
	if mutations[0].Title != "Reply in Slack thread in #ops" {
		t.Fatalf("default title was not resolved: %q", mutations[0].Title)
	}
	warnings := reply["warnings"].([]string)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "not a member") {
		t.Fatalf("warnings = %#v", warnings)
	}
	messages := mapSliceFromAny(reply["messages"])
	if len(messages) != 2 || messages[0]["is_thread_parent"] != true || messages[0]["actor_name"] != "Marcus" || messages[1]["is_from_me"] != true {
		t.Fatalf("thread context = %#v", messages)
	}
	if got := previewLinkURL(messages[1]["open"]); got != "https://example.slack.com/archives/C1/p1593473600000300?thread_ts=1593473566.000200&cid=C1" {
		t.Fatalf("reply link = %q", got)
	}
	if got := previewLinkURL(reply["open"]); got != "https://example.slack.com/archives/C1/p1593473566000200" {
		t.Fatalf("thread link = %q", got)
	}
	// The thread parent itself is not a reply: no thread query string.
	if got := previewLinkURL(messages[0]["open"]); got != "https://example.slack.com/archives/C1/p1593473566000200" {
		t.Fatalf("parent link = %q", got)
	}

	dm := mapFromAny(mutations[1].Preview["slack_message"])
	if dm["recipient_found"] != false || dm["recipient_label"] != "U-GONE" {
		t.Fatalf("dm preview = %#v", dm)
	}
	if warnings := dm["warnings"].([]string); len(warnings) != 1 || !strings.Contains(warnings[0], "not in the warehouse") {
		t.Fatalf("dm warnings = %#v", warnings)
	}
	if mutations[1].Title != "Hand-written title" {
		t.Fatalf("a caller's own title was overwritten: %q", mutations[1].Title)
	}
}

func TestSlackSendPreviewWarningsCoverEveryRefusal(t *testing.T) {
	cases := []struct {
		name     string
		detail   slackSendPreviewDetail
		delivery string
		want     string
	}{
		{"archived", slackSendPreviewDetail{ConversationFound: true, IsArchived: true, IsMember: true, ConversationType: "private_channel"}, slackSendDeliveryConversation, "archived"},
		{"missing conversation", slackSendPreviewDetail{}, slackSendDeliveryConversation, "not in the warehouse"},
		{"deactivated person", slackSendPreviewDetail{RecipientFound: true, RecipientDeleted: true}, slackSendDeliveryDM, "deactivated"},
		{"bot", slackSendPreviewDetail{RecipientFound: true, RecipientIsBot: true}, slackSendDeliveryDM, "bot"},
		{"missing thread", slackSendPreviewDetail{ConversationFound: true, IsMember: true, ConversationType: "im"}, slackSendDeliveryThreadReply, "parent message"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			warnings := slackSendPreviewWarnings(test.detail, test.delivery)
			if len(warnings) != 1 || !strings.Contains(warnings[0], test.want) {
				t.Fatalf("warnings = %#v", warnings)
			}
		})
	}
	if warnings := slackSendPreviewWarnings(slackSendPreviewDetail{ConversationFound: true, ConversationType: "im", IsMember: true, RecipientFound: true}, slackSendDeliveryConversation); len(warnings) != 0 {
		t.Fatalf("a clean DM warned: %#v", warnings)
	}
}

func TestApplySlackSendMessagePreviewLinksHydratesFacesAndLinksOnRead(t *testing.T) {
	mutations := []Mutation{{
		ID: "mut-1", Provider: SlackProvider, Operation: SlackSendMessageOperation, Account: "example",
		Preview: map[string]any{"slack_message": map[string]any{
			"team_id": "T1", "conversation_id": "", "user_id": "U-MARCUS", "resolved_conversation_id": "D1",
			"recipient_user_id": "U-MARCUS",
			"messages": []any{
				map[string]any{"message_ts": "1593473500.000100", "user_id": "U-ME", "actor_name": "You"},
				map[string]any{"message_ts": "1593473566.000200", "user_id": "U-MARCUS", "actor_name": "Marcus"},
			},
		}},
	}}
	got := applySlackPreviewLinks(
		mutations,
		map[slackTeamKey]string{{Account: "example", TeamID: "T1"}: "example"},
		map[slackUserKey]string{{Account: "example", TeamID: "T1", UserID: "U-MARCUS"}: "https://avatars.example.test/marcus.png"},
	)
	slackMessage := mapFromAny(got[0].Preview["slack_message"])
	if slackMessage["team_domain"] != "example" || slackMessage["avatar_url"] != "https://avatars.example.test/marcus.png" {
		t.Fatalf("hydrated preview = %#v", slackMessage)
	}
	// The conversation link opens at the newest context message.
	if url := previewLinkURL(slackMessage["open"]); url != "https://example.slack.com/archives/D1/p1593473566000200" {
		t.Fatalf("open = %q", url)
	}
	messages := mapSliceFromAny(slackMessage["messages"])
	if messages[1]["avatar_url"] != "https://avatars.example.test/marcus.png" {
		t.Fatalf("message avatar = %#v", messages[1])
	}
	if _, ok := messages[0]["avatar_url"]; ok {
		t.Fatalf("a speaker with no stored image carries an avatar: %#v", messages[0])
	}
	if _, ok := mapFromAny(mutations[0].Preview["slack_message"])["open"]; ok {
		t.Fatal("hydration mutated the stored snapshot")
	}
	teams, users := slackPreviewLinkTargets(mutations)
	if len(teams) != 1 || len(users) != 2 {
		t.Fatalf("targets = %#v / %#v", teams, users)
	}
	// A preview with no workspace has nothing to look up and is left alone.
	untouched := applySlackPreviewLinks([]Mutation{{Provider: SlackProvider, Operation: SlackSendMessageOperation, Account: "example", Preview: map[string]any{"slack_message": map[string]any{"conversation_id": "C1"}}}}, nil, nil)
	if _, ok := mapFromAny(untouched[0].Preview["slack_message"])["open"]; ok {
		t.Fatalf("a preview without a team grew a link: %#v", untouched[0].Preview)
	}
}

func TestMutationHelpDocumentsSlackSendMessage(t *testing.T) {
	var entry MutationHelpType
	for _, candidate := range MutationHelp().Mutations {
		if candidate.Type == SlackSendMessageOperation {
			entry = candidate
		}
	}
	if entry.Type == "" {
		t.Fatal("propose_mutation_help does not document slack.send_message")
	}
	fields := map[string]bool{}
	for _, field := range entry.Fields {
		fields[field.Name] = true
	}
	for _, want := range []string{"conversation_id", "user_id", "text", "thread_ts", "reply_broadcast"} {
		if !fields[want] {
			t.Fatalf("help is missing %q: %#v", want, entry.Fields)
		}
	}
	for _, want := range []string{"approves", "client_msg_id", "publish-session", "edit the text"} {
		if !strings.Contains(entry.ExtraNotes, want) {
			t.Fatalf("help notes do not mention %q: %q", want, entry.ExtraNotes)
		}
	}
	// The documented example has to pass the proposer's own validation.
	input, err := mutationInputFromMap(entry.Example, 0)
	if err != nil {
		t.Fatalf("example does not parse: %v", err)
	}
	if err := validateSlackSendMessage(input); err != nil {
		t.Fatalf("documented example is rejected: %v", err)
	}
}

// slackEditingStore is the API fake with the Slack edit wired up.
type slackEditingStore struct {
	*apiFakeStore
	edits []UpdateSlackMessageMutationInput
}

func (s *slackEditingStore) UpdateSlackMessageMutation(_ context.Context, requestID, mutationID string, input UpdateSlackMessageMutationInput, actor string) (Mutation, error) {
	r, ok := s.requests[requestID]
	if !ok {
		return Mutation{}, ErrNotFound
	}
	if r.Status != "pending_review" {
		return Mutation{}, errors.New("cannot edit mutation for request with status " + r.Status)
	}
	s.edits = append(s.edits, input)
	s.actors = append(s.actors, actor)
	return Mutation{ID: mutationID, RequestID: requestID, Provider: SlackProvider, Operation: SlackSendMessageOperation, Status: "pending_review", Payload: map[string]any{"text": input.Text}}, nil
}

func TestAPIUpdateSlackMessageEditsTheTextWithTheAppActor(t *testing.T) {
	store := &slackEditingStore{apiFakeStore: newAPIFakeStore(Request{
		ID: "req-slack", Status: "pending_review", Title: "Reply to Marcus", MutationCount: 1,
		Mutations: []Mutation{{ID: "mut-slack", RequestID: "req-slack", Provider: SlackProvider, Operation: SlackSendMessageOperation, Account: "zrl", Status: "pending_review",
			Payload: map[string]any{"conversation_id": "D1", "text": "old"}}},
	})}
	service := NewService(store, Config{})
	srv := httptest.NewServer(service.APIHandler(func(next http.Handler) http.Handler { return next }))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+APIPath+"/requests/req-slack/mutations/mut-slack/update-slack-message", strings.NewReader(`{"text":"new words"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-PDW-Client", "web")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("update-slack-message: %v", err)
	}
	defer resp.Body.Close()
	var got map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("update-slack-message failed: %d %v", resp.StatusCode, got)
	}
	if len(store.edits) != 1 || store.edits[0].Text != "new words" || store.actors[len(store.actors)-1] != "app:web" {
		t.Fatalf("store saw edits %#v by %#v", store.edits, store.actors)
	}
	if mapFromAny(mapFromAny(got["mutation"])["payload"])["text"] != "new words" {
		t.Fatalf("response = %v", got)
	}
	missing, _ := http.Post(srv.URL+APIPath+"/requests/nope/mutations/mut-slack/update-slack-message", "application/json", strings.NewReader(`{"text":"x"}`))
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown request = %d", missing.StatusCode)
	}
	bad, _ := http.Post(srv.URL+APIPath+"/requests/req-slack/mutations/mut-slack/update-slack-message", "application/json", strings.NewReader(`{"text":`))
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed body = %d", bad.StatusCode)
	}
}
