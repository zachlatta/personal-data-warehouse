package push

import "sort"

// Category is a notification category: the set of action buttons iOS shows
// under an alert. The server is the single source of truth — the app fetches
// GET /api/push/categories on launch and registers each one with
// setNotificationCategoryAsync — so adding a category or an action here is
// the whole change; the app needs no release for it (only the handling of a
// NEW action id does, see mobile/src/lib/push.ts).
type Category struct {
	ID      string   `json:"id"`
	Actions []Action `json:"actions"`
}

// Action is one button. OpensApp false runs the handler in the background
// without bringing the app forward, which is what a one-tap Approve wants.
type Action struct {
	ID          string     `json:"id"`
	Title       string     `json:"title"`
	Destructive bool       `json:"destructive"`
	OpensApp    bool       `json:"opens_app"`
	TextInput   *TextInput `json:"text_input,omitempty"`
}

// TextInput turns the action into an inline reply field.
type TextInput struct {
	Placeholder string `json:"placeholder"`
	SubmitTitle string `json:"submit_title"`
}

const (
	// CategoryMutationReview is the alert for a request in pending_review:
	// approve or deny from the lock screen, or open the review screen.
	CategoryMutationReview = "mutation_review"
	// CategoryLink is a plain alert whose only action is to open its route.
	CategoryLink = "link"
	// CategoryReply is an alert with an inline text field; what is typed
	// comes back to the app as userText with data intact, for a future
	// reply-from-the-notification flow.
	CategoryReply = "reply"

	ActionApprove = "approve"
	ActionDeny    = "deny"
	ActionOpen    = "open"
	ActionReply   = "reply"
)

// Categories lists every category the app registers, in a stable order.
func Categories() []Category {
	return []Category{
		{ID: CategoryMutationReview, Actions: []Action{
			{ID: ActionApprove, Title: "Approve"},
			{ID: ActionDeny, Title: "Deny", Destructive: true},
			{ID: ActionOpen, Title: "Review", OpensApp: true},
		}},
		{ID: CategoryLink, Actions: []Action{
			{ID: ActionOpen, Title: "Open", OpensApp: true},
		}},
		{ID: CategoryReply, Actions: []Action{
			{ID: ActionReply, Title: "Reply", TextInput: &TextInput{Placeholder: "Message", SubmitTitle: "Send"}},
			{ID: ActionOpen, Title: "Open", OpensApp: true},
		}},
	}
}

func categoryIDs() map[string]bool {
	ids := map[string]bool{}
	for _, c := range Categories() {
		ids[c.ID] = true
	}
	return ids
}

func categoryNames() []string {
	names := make([]string, 0, len(Categories()))
	for id := range categoryIDs() {
		names = append(names, id)
	}
	sort.Strings(names)
	return names
}
