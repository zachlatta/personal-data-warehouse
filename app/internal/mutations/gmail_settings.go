package mutations

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

// Gmail filters and labels as reviewed mutations. An agent proposes a rule in
// friendly terms (from/to/subject → skip inbox, label "Newsletters"); the
// proposal is validated here and snapshotted against base_gmail.labels /
// base_gmail.filters (gmail_settings_store.go); the review reads like the rule;
// the Python executor (gmail_settings_mutations.py) applies it after approval.
//
// Forwarding is refused: it needs a verified forwarding address and the
// gmail.settings.sharing scope, and sending Zach's mail somewhere else is a
// decision for a person, not a side effect of a filter.

const (
	GmailCreateFilterOperation  = "gmail.create_filter"
	GmailReplaceFilterOperation = "gmail.replace_filter"
	GmailDeleteFilterOperation  = "gmail.delete_filter"
	GmailCreateLabelOperation   = "gmail.create_label"
	GmailUpdateLabelOperation   = "gmail.update_label"
	GmailDeleteLabelOperation   = "gmail.delete_label"

	gmailLabelNameMaxLength = 225
	gmailCriterionMaxLength = 1500
)

func isGmailSettingsOperation(operation string) bool {
	switch operation {
	case GmailCreateFilterOperation, GmailReplaceFilterOperation, GmailDeleteFilterOperation,
		GmailCreateLabelOperation, GmailUpdateLabelOperation, GmailDeleteLabelOperation:
		return true
	}
	return false
}

func isGmailFilterOperation(operation string) bool {
	return operation == GmailCreateFilterOperation || operation == GmailReplaceFilterOperation || operation == GmailDeleteFilterOperation
}

// gmailLabelColorPalette is the only set of colors Gmail accepts for a label's
// backgroundColor and textColor (users.labels reference); anything else is a
// 400 after approval, so it is refused at proposal.
var gmailLabelColorPalette = func() map[string]bool {
	out := map[string]bool{}
	for _, color := range strings.Fields(`#000000 #434343 #666666 #999999 #cccccc #efefef #f3f3f3 #ffffff #fb4c2f #ffad47 #fad165 #16a766 #43d692 #4a86e8 #a479e2 #f691b3 #f6c5be #ffe6c7 #fef1d1 #b9e4d0 #c6f3de #c9daf8 #e4d7f5 #fcdee8 #efa093 #ffd6a2 #fce8b3 #89d3b2 #a0eac9 #a4c2f4 #d0bcf1 #fbc8d9 #e66550 #ffbc6b #fcda83 #44b984 #68dfa9 #6d9eeb #b694e8 #f7a7c0 #cc3a21 #eaa041 #f2c960 #149e60 #3dc789 #3c78d8 #8e63ce #e07798 #ac2b16 #cf8933 #d5ae49 #0b804b #2a9c68 #285bac #653e9b #b65775 #822111 #a46a21 #aa8831 #076239 #1a764d #1c4587 #41236d #83334c #464646 #e7e7e7 #0d3472 #b6cff5 #0d3b44 #98d7e4 #3d188e #e3d7ff #711a36 #fbd3e0 #8a1c0a #f2b2a8 #7a2e0b #ffc8af #7a4706 #ffdeb5 #594c05 #fbe983 #684e07 #fdedc1 #0b4f30 #b3efd3 #04502e #a2dcc1 #c2c2c2 #4986e7 #2da2bb #b99aff #994a64 #f691b2 #ff7537 #ffad46 #662e37 #ebdbde #cca6ac #094228 #42d692 #16a765`) {
		out[color] = true
	}
	return out
}()

// gmailSystemLabelActions names, for each system label an agent might put in a
// label list, the filter boolean that is the one way to say it.
var gmailSystemLabelActions = map[string]string{
	"inbox":               "skip_inbox",
	"unread":              "mark_read",
	"starred":             "star",
	"important":           "mark_important or never_important",
	"spam":                "never_spam",
	"trash":               "trash",
	"category_personal":   "category",
	"category_social":     "category",
	"category_promotions": "category",
	"category_updates":    "category",
	"category_forums":     "category",
}

// gmailReservedLabelNames cannot be user label names: Gmail owns them.
var gmailReservedLabelNames = map[string]bool{
	"inbox": true, "sent": true, "draft": true, "drafts": true, "spam": true, "trash": true,
	"starred": true, "important": true, "unread": true, "chat": true, "chats": true,
	"all mail": true, "snoozed": true, "scheduled": true, "outbox": true,
	"category_personal": true, "category_social": true, "category_promotions": true,
	"category_updates": true, "category_forums": true,
}

var gmailFilterCategories = map[string]string{
	"primary":    "CATEGORY_PERSONAL",
	"social":     "CATEGORY_SOCIAL",
	"updates":    "CATEGORY_UPDATES",
	"forums":     "CATEGORY_FORUMS",
	"promotions": "CATEGORY_PROMOTIONS",
}

var gmailSettingsKeys = map[string][]string{
	GmailCreateFilterOperation:  {"criteria", "action"},
	GmailReplaceFilterOperation: {"filter_id", "criteria", "action"},
	GmailDeleteFilterOperation:  {"filter_id"},
	GmailCreateLabelOperation:   {"name", "color", "label_list_visibility", "message_list_visibility"},
	GmailUpdateLabelOperation:   {"label", "new_name", "color", "label_list_visibility", "message_list_visibility"},
	GmailDeleteLabelOperation:   {"label"},
}

func forwardingRefused() error {
	return errors.New("Gmail filters that forward mail are not supported: forwarding needs a verified forwarding address and the gmail.settings.sharing scope, and sending Zach's mail elsewhere must be a separate, deliberate decision. Propose the filter without forward")
}

// validateGmailSettingsMutation refuses unknown keys as well as bad values: a
// misspelled field would otherwise become an approved rule that does less
// than the reviewer read.
func validateGmailSettingsMutation(mutation MutationInput) error {
	allowed := map[string]bool{"type": true, "operation": true, "account": true, "title": true, "reason": true}
	for _, key := range gmailSettingsKeys[mutation.Type] {
		allowed[key] = true
	}
	var unknown []string
	for key := range mutation.Raw {
		if key == "forward" || key == "forward_to" {
			return forwardingRefused()
		}
		if !allowed[key] {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("%s does not take %s (fields: %s)", mutation.Type, strings.Join(unknown, ", "), strings.Join(gmailSettingsKeys[mutation.Type], ", "))
	}
	switch mutation.Type {
	case GmailCreateFilterOperation, GmailReplaceFilterOperation:
		if mutation.Type == GmailReplaceFilterOperation && strings.TrimSpace(mutation.FilterID) == "" {
			return errors.New("must include filter_id: the Gmail filter to replace (base_gmail.filters.filter_id)")
		}
		if _, err := normalizeGmailFilterCriteria(mutation.Raw["criteria"]); err != nil {
			return err
		}
		if _, err := normalizeGmailFilterAction(mutation.Raw["action"]); err != nil {
			return err
		}
	case GmailDeleteFilterOperation:
		if strings.TrimSpace(mutation.FilterID) == "" {
			return errors.New("must include filter_id: the Gmail filter to delete (base_gmail.filters.filter_id)")
		}
	case GmailCreateLabelOperation:
		if err := validateGmailLabelName(mutation.Name, "name"); err != nil {
			return err
		}
		if _, err := normalizeGmailLabelAppearance(mutation.Raw); err != nil {
			return err
		}
	case GmailUpdateLabelOperation:
		if strings.TrimSpace(mutation.Label) == "" {
			return errors.New("must include label: the label's current name or id (base_gmail.labels)")
		}
		appearance, err := normalizeGmailLabelAppearance(mutation.Raw)
		if err != nil {
			return err
		}
		if strings.TrimSpace(mutation.NewName) != "" {
			if err := validateGmailLabelName(mutation.NewName, "new_name"); err != nil {
				return err
			}
		} else if len(appearance) == 0 {
			return errors.New("must change something: new_name, color, label_list_visibility or message_list_visibility")
		}
	case GmailDeleteLabelOperation:
		if strings.TrimSpace(mutation.Label) == "" {
			return errors.New("must include label: the label's current name or id (base_gmail.labels)")
		}
	}
	return nil
}

func validateGmailLabelName(value string, field string) error {
	name := strings.TrimSpace(value)
	if name == "" {
		return fmt.Errorf("must include %s", field)
	}
	if len([]rune(name)) > gmailLabelNameMaxLength {
		return fmt.Errorf("%s must be at most %d characters", field, gmailLabelNameMaxLength)
	}
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") || strings.Contains(name, "//") {
		return fmt.Errorf("%s %q has an empty nesting level; nest labels as Parent/Child", field, name)
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%s must not contain control characters", field)
		}
	}
	if gmailReservedLabelNames[strings.ToLower(name)] {
		return fmt.Errorf("%s %q is a Gmail system label; pick another name", field, name)
	}
	return nil
}

// normalizeGmailLabelAppearance reads color and the two visibilities.
func normalizeGmailLabelAppearance(raw map[string]any) (map[string]any, error) {
	out := map[string]any{}
	if value, ok := raw["color"]; ok && value != nil {
		color, isMap := value.(map[string]any)
		if !isMap {
			return nil, errors.New("color must be an object {background_color, text_color}")
		}
		for key := range color {
			if key != "background_color" && key != "text_color" {
				return nil, fmt.Errorf("color does not take %s (fields: background_color, text_color)", key)
			}
		}
		background := strings.ToLower(strings.TrimSpace(stringFromAny(color["background_color"])))
		text := strings.ToLower(strings.TrimSpace(stringFromAny(color["text_color"])))
		if background == "" || text == "" {
			return nil, errors.New("color needs both background_color and text_color")
		}
		for _, c := range []string{background, text} {
			if !gmailLabelColorPalette[c] {
				return nil, fmt.Errorf("color %s is not in Gmail's label palette (for example #16a766 green, #4a86e8 blue, #fb4c2f red, #ffad47 orange, #000000, #ffffff); see propose_mutation_help", c)
			}
		}
		out["color"] = map[string]any{"background_color": background, "text_color": text}
	}
	if value := strings.TrimSpace(stringFromAny(raw["label_list_visibility"])); value != "" {
		if value != "labelShow" && value != "labelShowIfUnread" && value != "labelHide" {
			return nil, fmt.Errorf("label_list_visibility must be labelShow, labelShowIfUnread or labelHide; got %q", value)
		}
		out["label_list_visibility"] = value
	}
	if value := strings.TrimSpace(stringFromAny(raw["message_list_visibility"])); value != "" {
		if value != "show" && value != "hide" {
			return nil, fmt.Errorf("message_list_visibility must be show or hide; got %q", value)
		}
		out["message_list_visibility"] = value
	}
	return out, nil
}

var gmailCriteriaAliases = map[string]string{
	"from": "from", "to": "to", "subject": "subject", "query": "query",
	"negated_query": "negated_query", "negatedQuery": "negated_query",
	"has_attachment": "has_attachment", "hasAttachment": "has_attachment",
	"exclude_chats": "exclude_chats", "excludeChats": "exclude_chats",
	"size": "size", "size_comparison": "size_comparison", "sizeComparison": "size_comparison",
}

// normalizeGmailFilterCriteria returns the criteria in the snake_case form the
// executor reads, with only the fields that say something.
func normalizeGmailFilterCriteria(value any) (map[string]any, error) {
	raw, ok := value.(map[string]any)
	if !ok || len(raw) == 0 {
		return nil, errors.New("must include criteria: an object with at least one of from, to, subject, query, negated_query, has_attachment, size")
	}
	out := map[string]any{}
	for key, item := range raw {
		field, known := gmailCriteriaAliases[key]
		if !known {
			return nil, fmt.Errorf("criteria does not take %s (fields: from, to, subject, query, negated_query, has_attachment, exclude_chats, size, size_comparison)", key)
		}
		switch field {
		case "from", "to", "subject", "query", "negated_query":
			text, isString := item.(string)
			if !isString {
				return nil, fmt.Errorf("criteria.%s must be a string", field)
			}
			text = strings.TrimSpace(text)
			if text == "" {
				return nil, fmt.Errorf("criteria.%s must not be blank; leave it out instead", field)
			}
			if len(text) > gmailCriterionMaxLength {
				return nil, fmt.Errorf("criteria.%s must be at most %d characters", field, gmailCriterionMaxLength)
			}
			out[field] = text
		case "has_attachment", "exclude_chats":
			flag, isBool := item.(bool)
			if !isBool {
				return nil, fmt.Errorf("criteria.%s must be true or false", field)
			}
			if flag {
				out[field] = true
			}
		case "size":
			number, isNumber := item.(float64)
			if !isNumber || number <= 0 || number != math.Trunc(number) {
				return nil, errors.New("criteria.size must be a positive whole number of bytes")
			}
			out[field] = int64(number)
		case "size_comparison":
			comparison := strings.TrimSpace(stringFromAny(item))
			if comparison != "larger" && comparison != "smaller" {
				return nil, fmt.Errorf("criteria.size_comparison must be larger or smaller; got %q", comparison)
			}
			out[field] = comparison
		}
	}
	_, hasSize := out["size"]
	_, hasComparison := out["size_comparison"]
	if hasSize != hasComparison {
		return nil, errors.New("criteria.size and criteria.size_comparison go together")
	}
	matching := 0
	for _, field := range []string{"from", "to", "subject", "query", "negated_query", "has_attachment", "size"} {
		if _, ok := out[field]; ok {
			matching++
		}
	}
	if matching == 0 {
		return nil, errors.New("criteria must say which mail the filter matches: at least one of from, to, subject, query, negated_query, has_attachment, size")
	}
	return out, nil
}

var gmailFilterActionBooleans = []string{"skip_inbox", "mark_read", "star", "mark_important", "never_important", "never_spam", "trash"}

// normalizeGmailFilterAction returns the action in friendly form: label lists
// by name plus booleans that stand for Gmail's system labels.
func normalizeGmailFilterAction(value any) (map[string]any, error) {
	raw, ok := value.(map[string]any)
	if !ok || len(raw) == 0 {
		return nil, errors.New("must include action: an object with at least one of add_labels, create_and_add_labels, remove_labels, skip_inbox, mark_read, star, mark_important, never_important, never_spam, trash, category")
	}
	out := map[string]any{}
	booleans := map[string]bool{}
	for _, key := range gmailFilterActionBooleans {
		booleans[key] = true
	}
	for key, item := range raw {
		switch {
		case key == "forward" || key == "forward_to":
			return nil, forwardingRefused()
		case key == "add_labels" || key == "create_and_add_labels" || key == "remove_labels":
			list, err := gmailLabelNameList(item, key)
			if err != nil {
				return nil, err
			}
			if len(list) > 0 {
				out[key] = list
			}
		case booleans[key]:
			flag, isBool := item.(bool)
			if !isBool {
				return nil, fmt.Errorf("action.%s must be true or false", key)
			}
			if flag {
				out[key] = true
			}
		case key == "category":
			category := strings.ToLower(strings.TrimSpace(stringFromAny(item)))
			if _, known := gmailFilterCategories[category]; !known {
				return nil, fmt.Errorf("action.category must be primary, social, updates, forums or promotions; got %q", stringFromAny(item))
			}
			out[key] = category
		default:
			return nil, fmt.Errorf("action does not take %s (fields: add_labels, create_and_add_labels, remove_labels, %s, category)", key, strings.Join(gmailFilterActionBooleans, ", "))
		}
	}
	if len(out) == 0 {
		return nil, errors.New("action must do something: name a label or set one of the action booleans to true")
	}
	if out["mark_important"] == true && out["never_important"] == true {
		return nil, errors.New("action cannot both mark_important and never_important")
	}
	add, create, remove := stringSliceFromAny(out["add_labels"]), stringSliceFromAny(out["create_and_add_labels"]), stringSliceFromAny(out["remove_labels"])
	if len(add)+len(create)+len(remove) > 0 {
		if err := validateGmailLabelChanges(add, create, remove); err != nil {
			return nil, fmt.Errorf("action %w", err)
		}
	}
	return out, nil
}

func gmailLabelNameList(value any, field string) ([]string, error) {
	items, ok := value.([]any)
	if !ok {
		if typed, isStrings := value.([]string); isStrings {
			for _, item := range typed {
				items = append(items, item)
			}
		} else {
			return nil, fmt.Errorf("action.%s must be an array of label names", field)
		}
	}
	out := []string{}
	seen := map[string]bool{}
	for _, item := range items {
		name := strings.TrimSpace(stringFromAny(item))
		if name == "" {
			continue
		}
		if use, system := gmailSystemLabelActions[strings.ToLower(name)]; system {
			return nil, fmt.Errorf("action.%s names the system label %s; use the %s action instead", field, name, use)
		}
		if field == "create_and_add_labels" {
			if err := validateGmailLabelName(name, "action.create_and_add_labels label"); err != nil {
				return nil, err
			}
		}
		if !seen[strings.ToLower(name)] {
			out = append(out, name)
			seen[strings.ToLower(name)] = true
		}
	}
	if len(out) > 100 {
		return nil, fmt.Errorf("action.%s must name at most 100 labels", field)
	}
	return out, nil
}

// --- the rule as words -------------------------------------------------------

// gmailFilterWhen renders snake_case criteria as the condition half of a rule.
func gmailFilterWhen(criteria map[string]any) string {
	parts := []string{}
	if value := stringFromAny(criteria["from"]); value != "" {
		parts = append(parts, "From "+value)
	}
	if value := stringFromAny(criteria["to"]); value != "" {
		parts = append(parts, "To "+value)
	}
	if value := stringFromAny(criteria["subject"]); value != "" {
		parts = append(parts, fmt.Sprintf("Subject has %q", value))
	}
	if value := stringFromAny(criteria["query"]); value != "" {
		parts = append(parts, fmt.Sprintf("Matches %q", value))
	}
	if value := stringFromAny(criteria["negated_query"]); value != "" {
		parts = append(parts, fmt.Sprintf("Doesn't match %q", value))
	}
	if criteria["has_attachment"] == true {
		parts = append(parts, "Has attachment")
	}
	if size := int64FromAny(criteria["size"]); size > 0 {
		word := "Larger"
		if stringFromAny(criteria["size_comparison"]) == "smaller" {
			word = "Smaller"
		}
		parts = append(parts, word+" than "+humanBytes(size))
	}
	if criteria["exclude_chats"] == true {
		parts = append(parts, "not chats")
	}
	if len(parts) == 0 {
		return "Any mail"
	}
	return strings.Join(parts, ", ")
}

func int64FromAny(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	}
	return 0
}

func humanBytes(size int64) string {
	switch {
	case size >= 1<<20 && size%(1<<20) == 0:
		return fmt.Sprintf("%d MB", size>>20)
	case size >= 1<<10 && size%(1<<10) == 0:
		return fmt.Sprintf("%d KB", size>>10)
	}
	return fmt.Sprintf("%d bytes", size)
}

func titleWord(value string) string {
	if value == "" {
		return value
	}
	return strings.ToUpper(value[:1]) + value[1:]
}

// gmailFilterFriendlyActions renders a proposal's action in Gmail's own words.
func gmailFilterFriendlyActions(action map[string]any) []string {
	out := []string{}
	if action["skip_inbox"] == true {
		out = append(out, "skip inbox")
	}
	if action["mark_read"] == true {
		out = append(out, "mark read")
	}
	if action["star"] == true {
		out = append(out, "star")
	}
	for _, name := range stringSliceFromAny(action["add_labels"]) {
		out = append(out, fmt.Sprintf("label %q", name))
	}
	for _, name := range stringSliceFromAny(action["create_and_add_labels"]) {
		out = append(out, fmt.Sprintf("label %q (created if missing)", name))
	}
	for _, name := range stringSliceFromAny(action["remove_labels"]) {
		out = append(out, fmt.Sprintf("remove label %q", name))
	}
	if action["mark_important"] == true {
		out = append(out, "mark important")
	}
	if action["never_important"] == true {
		out = append(out, "never mark important")
	}
	if action["never_spam"] == true {
		out = append(out, "never send to spam")
	}
	if category := stringFromAny(action["category"]); category != "" {
		out = append(out, "categorize as "+titleWord(category))
	}
	if action["trash"] == true {
		out = append(out, "delete it")
	}
	return out
}

// gmailFilterAPIActions renders an existing filter's Gmail action (label ids)
// in the same words, naming user labels from base_gmail.labels.
func gmailFilterAPIActions(action map[string]any, labelNames map[string]string) []string {
	out := []string{}
	remove := stringSliceFromAny(action["removeLabelIds"])
	add := stringSliceFromAny(action["addLabelIds"])
	name := func(id string) string {
		if label := labelNames[id]; label != "" {
			return label
		}
		return id
	}
	for _, id := range remove {
		switch id {
		case "INBOX":
			out = append(out, "skip inbox")
		case "UNREAD":
			out = append(out, "mark read")
		}
	}
	for _, id := range add {
		switch {
		case id == "STARRED":
			out = append(out, "star")
		case id == "IMPORTANT", id == "TRASH", strings.HasPrefix(id, "CATEGORY_"):
		default:
			out = append(out, fmt.Sprintf("label %q", name(id)))
		}
	}
	for _, id := range remove {
		switch id {
		case "INBOX", "UNREAD", "IMPORTANT", "SPAM":
		default:
			out = append(out, fmt.Sprintf("remove label %q", name(id)))
		}
	}
	for _, id := range add {
		if id == "IMPORTANT" {
			out = append(out, "mark important")
		}
	}
	for _, id := range remove {
		switch id {
		case "IMPORTANT":
			out = append(out, "never mark important")
		case "SPAM":
			out = append(out, "never send to spam")
		}
	}
	for _, id := range add {
		if strings.HasPrefix(id, "CATEGORY_") {
			category := strings.ToLower(strings.TrimPrefix(id, "CATEGORY_"))
			if category == "personal" {
				category = "primary"
			}
			out = append(out, "categorize as "+titleWord(category))
		}
	}
	for _, id := range add {
		if id == "TRASH" {
			out = append(out, "delete it")
		}
	}
	if forward := stringFromAny(action["forward"]); forward != "" {
		out = append(out, "forward to "+forward)
	}
	return out
}

// gmailCriteriaFromAPI maps Gmail's Filter.criteria to the snake_case form.
func gmailCriteriaFromAPI(criteria map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range criteria {
		field, ok := gmailCriteriaAliases[key]
		if !ok {
			continue
		}
		if field == "size_comparison" && stringFromAny(value) == "unspecified" {
			continue
		}
		out[field] = value
	}
	return out
}

func gmailRule(when string, actions []string) map[string]any {
	text := when
	if len(actions) > 0 {
		text += " → " + strings.Join(actions, ", ")
	}
	return map[string]any{"when": when, "actions": actions, "text": text}
}

// --- storage -----------------------------------------------------------------

func gmailSettingsPayload(mutation MutationInput) map[string]any {
	payload := map[string]any{}
	switch mutation.Type {
	case GmailCreateFilterOperation, GmailReplaceFilterOperation:
		criteria, _ := normalizeGmailFilterCriteria(mutation.Raw["criteria"])
		action, _ := normalizeGmailFilterAction(mutation.Raw["action"])
		payload["criteria"] = criteria
		payload["action"] = action
		if mutation.Type == GmailReplaceFilterOperation {
			payload["filter_id"] = strings.TrimSpace(mutation.FilterID)
		}
	case GmailDeleteFilterOperation:
		payload["filter_id"] = strings.TrimSpace(mutation.FilterID)
	case GmailCreateLabelOperation:
		payload["name"] = strings.TrimSpace(mutation.Name)
		appearance, _ := normalizeGmailLabelAppearance(mutation.Raw)
		for key, value := range appearance {
			payload[key] = value
		}
	case GmailUpdateLabelOperation:
		payload["label"] = strings.TrimSpace(mutation.Label)
		if name := strings.TrimSpace(mutation.NewName); name != "" {
			payload["new_name"] = name
		}
		appearance, _ := normalizeGmailLabelAppearance(mutation.Raw)
		for key, value := range appearance {
			payload[key] = value
		}
	case GmailDeleteLabelOperation:
		payload["label"] = strings.TrimSpace(mutation.Label)
	}
	return payload
}

// gmailSettingsPreview is what the proposal knows without the warehouse; the
// store adds the snapshot (gmail_settings_store.go) and the read path adds the
// match preview and the current state.
func gmailSettingsPreview(operation string, payload map[string]any) map[string]any {
	switch operation {
	case GmailCreateFilterOperation, GmailReplaceFilterOperation:
		criteria := mapFromAny(payload["criteria"])
		action := mapFromAny(payload["action"])
		kind := "create"
		if operation == GmailReplaceFilterOperation {
			kind = "replace"
		}
		view := map[string]any{
			"kind":     kind,
			"criteria": criteria,
			"action":   action,
			"rule":     gmailRule(gmailFilterWhen(criteria), gmailFilterFriendlyActions(action)),
			"effect":   "Gmail applies a filter to mail that arrives after it is created; mail already in the mailbox is not moved.",
		}
		if kind == "replace" {
			view["filter_id"] = stringFromAny(payload["filter_id"])
			view["effect"] = "Creates the new filter first, then deletes the old one, so the mailbox is never without the rule. Applies to new mail only."
		}
		return map[string]any{"gmail_filter": view}
	case GmailDeleteFilterOperation:
		return map[string]any{"gmail_filter": map[string]any{
			"kind":      "delete",
			"filter_id": stringFromAny(payload["filter_id"]),
			"effect":    "Gmail stops applying this rule to new mail. Mail it already filed stays where it is.",
		}}
	case GmailCreateLabelOperation, GmailUpdateLabelOperation, GmailDeleteLabelOperation:
		view := map[string]any{"kind": strings.TrimPrefix(strings.TrimSuffix(operation, "_label"), "gmail.")}
		for _, key := range []string{"name", "label", "new_name", "color", "label_list_visibility", "message_list_visibility"} {
			if value, ok := payload[key]; ok {
				view[key] = value
			}
		}
		return map[string]any{"gmail_label": view}
	}
	return map[string]any{}
}

func gmailSettingsTitle(operation string, payload map[string]any) string {
	switch operation {
	case GmailCreateFilterOperation:
		return "Create Gmail filter: " + stringFromAny(mapFromAny(gmailSettingsPreview(operation, payload)["gmail_filter"])["rule"].(map[string]any)["text"])
	case GmailReplaceFilterOperation:
		return "Replace Gmail filter: " + stringFromAny(mapFromAny(gmailSettingsPreview(operation, payload)["gmail_filter"])["rule"].(map[string]any)["text"])
	case GmailDeleteFilterOperation:
		return "Delete Gmail filter " + stringFromAny(payload["filter_id"])
	case GmailCreateLabelOperation:
		return "Create Gmail label " + strconvQuote(stringFromAny(payload["name"]))
	case GmailUpdateLabelOperation:
		if name := stringFromAny(payload["new_name"]); name != "" {
			return "Rename Gmail label " + strconvQuote(stringFromAny(payload["label"])) + " to " + strconvQuote(name)
		}
		return "Update Gmail label " + strconvQuote(stringFromAny(payload["label"]))
	case GmailDeleteLabelOperation:
		return "Delete Gmail label " + strconvQuote(stringFromAny(payload["label"]))
	}
	return operation
}

func strconvQuote(value string) string { return fmt.Sprintf("%q", value) }
