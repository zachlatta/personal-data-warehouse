package mutations

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// The warehouse half of Gmail filter and label proposals. The review server
// holds no Google credentials, so base_gmail.labels / base_gmail.filters (a
// snapshot the gmail_settings_sync job takes every 30 minutes and right after
// one of these mutations succeeds) is where "what this filter does today"
// comes from.
//
// At PROPOSAL time the label or filter being changed is resolved and
// snapshotted into the payload: the executor refuses to touch it if Gmail's
// copy no longer matches what the reviewer approved. On READ the review is
// refreshed from the warehouse: label ids become today's names, the match
// preview counts recent mail, and a filter or label that has since gone is
// called out. A failed lookup reads "unknown", never zero.

const (
	gmailFilterMatchWindowDays   = 90
	gmailFilterMatchExamples     = 5
	gmailFilterMatchTimeout      = 3 * time.Second
	gmailFilterMatchPerPage      = 10
	gmailFilterBroadThreadCount  = 100
	gmailLabelManyMessagesWarned = 1
)

type gmailLabelRow struct {
	ID                    string
	Name                  string
	Type                  string
	BackgroundColor       string
	TextColor             string
	LabelListVisibility   string
	MessageListVisibility string
}

func (row gmailLabelRow) view() map[string]any {
	out := map[string]any{"label_id": row.ID, "name": row.Name, "label_type": row.Type}
	if row.BackgroundColor != "" || row.TextColor != "" {
		out["color"] = map[string]any{"background_color": row.BackgroundColor, "text_color": row.TextColor}
	}
	if row.LabelListVisibility != "" {
		out["label_list_visibility"] = row.LabelListVisibility
	}
	if row.MessageListVisibility != "" {
		out["message_list_visibility"] = row.MessageListVisibility
	}
	return out
}

type gmailFilterRow struct {
	ID       string
	Criteria map[string]any
	Action   map[string]any
}

type gmailMailbox struct {
	labels  []gmailLabelRow
	filters []gmailFilterRow
}

func (m gmailMailbox) labelNames() map[string]string {
	out := map[string]string{}
	for _, label := range m.labels {
		out[label.ID] = label.Name
	}
	return out
}

func (m gmailMailbox) labelByID(id string) (gmailLabelRow, bool) {
	for _, label := range m.labels {
		if label.ID == id {
			return label, true
		}
	}
	return gmailLabelRow{}, false
}

// resolveLabel finds a label by id, then exact name, then a unique
// case-insensitive name: the same order the executor resolves names in.
func (m gmailMailbox) resolveLabel(token string) (gmailLabelRow, error) {
	token = strings.TrimSpace(token)
	if label, ok := m.labelByID(token); ok {
		return label, nil
	}
	var folded []gmailLabelRow
	for _, label := range m.labels {
		if label.Name == token {
			return label, nil
		}
		if strings.EqualFold(label.Name, token) {
			folded = append(folded, label)
		}
	}
	if len(folded) == 1 {
		return folded[0], nil
	}
	if len(folded) > 1 {
		return gmailLabelRow{}, fmt.Errorf("label %q is ambiguous; use its label_id from base_gmail.labels", token)
	}
	return gmailLabelRow{}, errNoGmailLabel
}

var errNoGmailLabel = errors.New("no such label")

func (m gmailMailbox) filterByID(id string) (gmailFilterRow, bool) {
	for _, item := range m.filters {
		if item.ID == id {
			return item, true
		}
	}
	return gmailFilterRow{}, false
}

func (s *PostgresStore) loadGmailMailbox(ctx context.Context, account string) (gmailMailbox, error) {
	mailbox := gmailMailbox{}
	rows, err := queryContext(ctx, s.db, `
		SELECT label_id, name, label_type, background_color, text_color, label_list_visibility, message_list_visibility
		FROM @gmail_labels
		WHERE account = $1 AND is_deleted = 0
		ORDER BY name, label_id
	`, account)
	if err != nil {
		return mailbox, err
	}
	for rows.Next() {
		var row gmailLabelRow
		if err := rows.Scan(&row.ID, &row.Name, &row.Type, &row.BackgroundColor, &row.TextColor, &row.LabelListVisibility, &row.MessageListVisibility); err != nil {
			rows.Close()
			return mailbox, err
		}
		mailbox.labels = append(mailbox.labels, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return mailbox, err
	}
	filterRows, err := queryContext(ctx, s.db, `
		SELECT filter_id, criteria_json::text, action_json::text
		FROM @gmail_filters
		WHERE account = $1 AND is_deleted = 0
		ORDER BY filter_id
	`, account)
	if err != nil {
		return mailbox, err
	}
	defer filterRows.Close()
	for filterRows.Next() {
		var id, criteria, action string
		if err := filterRows.Scan(&id, &criteria, &action); err != nil {
			return mailbox, err
		}
		mailbox.filters = append(mailbox.filters, gmailFilterRow{ID: id, Criteria: decodeJSONMap([]byte(criteria)), Action: decodeJSONMap([]byte(action))})
	}
	return mailbox, filterRows.Err()
}

// gmailFilterKey is the executor's equality test (_filter_key in
// gmail_settings_mutations.py): criteria compared case-insensitively, label
// id lists as sets.
func gmailFilterKey(criteria map[string]any, action map[string]any) string {
	parts := []string{}
	for key, value := range criteria {
		if text, ok := value.(string); ok {
			if strings.TrimSpace(text) == "" || (key == "sizeComparison" && text == "unspecified") {
				continue
			}
			parts = append(parts, key+"="+strings.ToLower(strings.TrimSpace(text)))
			continue
		}
		if value == nil || value == false {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%v", key, value))
	}
	sort.Strings(parts)
	add := stringSliceFromAny(action["addLabelIds"])
	remove := stringSliceFromAny(action["removeLabelIds"])
	sort.Strings(add)
	sort.Strings(remove)
	return strings.Join(parts, "&") + "|+" + strings.Join(add, ",") + "|-" + strings.Join(remove, ",") + "|>" + strings.ToLower(stringFromAny(action["forward"]))
}

// gmailAPICriteria is the snake_case criteria in Gmail's field names.
func gmailAPICriteria(criteria map[string]any) map[string]any {
	names := map[string]string{"negated_query": "negatedQuery", "has_attachment": "hasAttachment", "exclude_chats": "excludeChats", "size_comparison": "sizeComparison"}
	out := map[string]any{}
	for key, value := range criteria {
		if name, ok := names[key]; ok {
			out[name] = value
			continue
		}
		out[key] = value
	}
	return out
}

// gmailAPIAction resolves a friendly action to Gmail label ids; ok is false
// when a label it names does not exist yet (so no existing filter can match).
func gmailAPIAction(action map[string]any, mailbox gmailMailbox) (map[string]any, bool) {
	add, remove := []string{}, []string{}
	resolve := func(names []string, into *[]string) bool {
		for _, name := range names {
			label, err := mailbox.resolveLabel(name)
			if err != nil {
				return false
			}
			*into = append(*into, label.ID)
		}
		return true
	}
	if !resolve(stringSliceFromAny(action["add_labels"]), &add) || !resolve(stringSliceFromAny(action["create_and_add_labels"]), &add) || !resolve(stringSliceFromAny(action["remove_labels"]), &remove) {
		return nil, false
	}
	for key, id := range map[string]string{"star": "STARRED", "mark_important": "IMPORTANT", "trash": "TRASH"} {
		if action[key] == true {
			add = append(add, id)
		}
	}
	if category := stringFromAny(action["category"]); category != "" {
		add = append(add, gmailFilterCategories[category])
	}
	for key, id := range map[string]string{"skip_inbox": "INBOX", "mark_read": "UNREAD", "never_important": "IMPORTANT", "never_spam": "SPAM"} {
		if action[key] == true {
			remove = append(remove, id)
		}
	}
	out := map[string]any{}
	if len(add) > 0 {
		out["addLabelIds"] = add
	}
	if len(remove) > 0 {
		out["removeLabelIds"] = remove
	}
	return out, true
}

// enrichGmailSettings resolves and snapshots what each proposal changes, and
// refuses a proposal the warehouse says cannot be right.
func (s *PostgresStore) enrichGmailSettings(ctx context.Context, mutations []storedMutation) ([]storedMutation, error) {
	accounts := map[string]bool{}
	for _, mutation := range mutations {
		if mutation.Provider == "gmail" && isGmailSettingsOperation(mutation.Operation) {
			accounts[mutation.Account] = true
		}
	}
	if len(accounts) == 0 {
		return mutations, nil
	}
	if s == nil || s.db == nil {
		return nil, errors.New("cannot check the Gmail labels and filters: the warehouse is not connected")
	}
	mailboxes := map[string]gmailMailbox{}
	for account := range accounts {
		mailbox, err := s.loadGmailMailbox(ctx, account)
		if err != nil {
			return nil, fmt.Errorf("read base_gmail.labels / base_gmail.filters: %w", err)
		}
		mailboxes[account] = mailbox
	}
	// Labels this request itself creates count as existing for the filters in it.
	created := map[string]map[string]bool{}
	for _, mutation := range mutations {
		if created[mutation.Account] == nil {
			created[mutation.Account] = map[string]bool{}
		}
		if mutation.Operation == GmailCreateLabelOperation {
			created[mutation.Account][strings.ToLower(stringFromAny(mutation.Payload["name"]))] = true
		}
		if isGmailFilterOperation(mutation.Operation) {
			for _, name := range stringSliceFromAny(mapFromAny(mutation.Payload["action"])["create_and_add_labels"]) {
				created[mutation.Account][strings.ToLower(name)] = true
			}
		}
	}
	out := make([]storedMutation, len(mutations))
	copy(out, mutations)
	for index := range out {
		mutation := &out[index]
		if mutation.Provider != "gmail" || !isGmailSettingsOperation(mutation.Operation) {
			continue
		}
		mailbox := mailboxes[mutation.Account]
		if err := s.enrichGmailSettingsMutation(ctx, index, mutation, mailbox, created[mutation.Account]); err != nil {
			return nil, invalidProposalInput(err)
		}
	}
	return out, nil
}

func noLabelsSynced(account string) error {
	return fmt.Errorf("base_gmail.labels holds no labels for %s yet (gmail_settings_sync snapshots them every 30 minutes); try again after it has run", account)
}

func (s *PostgresStore) enrichGmailSettingsMutation(ctx context.Context, index int, mutation *storedMutation, mailbox gmailMailbox, created map[string]bool) error {
	payload := cloneMap(mutation.Payload)
	preview := cloneMap(mutation.Preview)
	synced := len(mailbox.labels) > 0
	switch mutation.Operation {
	case GmailCreateLabelOperation:
		name := stringFromAny(payload["name"])
		if existing, err := mailbox.resolveLabel(name); err == nil && strings.EqualFold(existing.Name, name) {
			return fmt.Errorf("mutation %d: Gmail label %q already exists (%s); use it as it is, or gmail.update_label to change it", index, existing.Name, existing.ID)
		}
	case GmailUpdateLabelOperation, GmailDeleteLabelOperation:
		if !synced {
			return fmt.Errorf("mutation %d: %w", index, noLabelsSynced(mutation.Account))
		}
		label, err := mailbox.resolveLabel(stringFromAny(payload["label"]))
		if errors.Is(err, errNoGmailLabel) {
			return fmt.Errorf("mutation %d: no Gmail label %q for %s in base_gmail.labels; read the label's name or label_id there", index, stringFromAny(payload["label"]), mutation.Account)
		}
		if err != nil {
			return fmt.Errorf("mutation %d: %w", index, err)
		}
		if label.Type == "system" {
			return fmt.Errorf("mutation %d: %s is a Gmail system label, which cannot be changed or deleted", index, label.Name)
		}
		payload["label_id"] = label.ID
		payload["expected_name"] = label.Name
		view := mapFromAny(preview["gmail_label"])
		view["label_id"] = label.ID
		view["current"] = label.view()
		nested := 0
		for _, other := range mailbox.labels {
			if strings.HasPrefix(other.Name, label.Name+"/") {
				nested++
			}
		}
		view["nested_label_count"] = nested
		filters := 0
		for _, item := range mailbox.filters {
			for _, id := range append(stringSliceFromAny(item.Action["addLabelIds"]), stringSliceFromAny(item.Action["removeLabelIds"])...) {
				if id == label.ID {
					filters++
					break
				}
			}
		}
		view["filter_count"] = filters
		if mutation.Operation == GmailUpdateLabelOperation {
			changes := []string{}
			if name := stringFromAny(payload["new_name"]); name != "" && name != label.Name {
				for _, other := range mailbox.labels {
					if other.ID != label.ID && strings.EqualFold(other.Name, name) {
						return fmt.Errorf("mutation %d: cannot rename %q to %q: label %s already has that name", index, label.Name, name, other.ID)
					}
				}
				changes = append(changes, fmt.Sprintf("name %q → %q", label.Name, name))
			}
			if color := mapFromAny(payload["color"]); len(color) > 0 && (color["background_color"] != label.BackgroundColor || color["text_color"] != label.TextColor) {
				changes = append(changes, fmt.Sprintf("color %s/%s → %s/%s", orNone(label.BackgroundColor), orNone(label.TextColor), color["background_color"], color["text_color"]))
			}
			if value := stringFromAny(payload["label_list_visibility"]); value != "" && value != label.LabelListVisibility {
				changes = append(changes, fmt.Sprintf("label list %s → %s", orNone(label.LabelListVisibility), value))
			}
			if value := stringFromAny(payload["message_list_visibility"]); value != "" && value != label.MessageListVisibility {
				changes = append(changes, fmt.Sprintf("message list %s → %s", orNone(label.MessageListVisibility), value))
			}
			if len(changes) == 0 {
				return fmt.Errorf("mutation %d: label %q already looks like that; the update changes nothing", index, label.Name)
			}
			view["changes"] = changes
		} else {
			messages, threads, ok := s.gmailLabelMessageCount(ctx, mutation.Account, label.ID)
			if ok {
				view["message_count"] = messages
				view["thread_count"] = threads
			}
		}
		preview["gmail_label"] = view
	case GmailCreateFilterOperation, GmailReplaceFilterOperation:
		action := mapFromAny(payload["action"])
		if synced {
			for _, field := range []string{"add_labels", "remove_labels"} {
				for _, name := range stringSliceFromAny(action[field]) {
					if _, err := mailbox.resolveLabel(name); errors.Is(err, errNoGmailLabel) && !created[strings.ToLower(name)] {
						return fmt.Errorf("mutation %d: action.%s names %q, which is not a label on %s (base_gmail.labels); use create_and_add_labels to create it, or propose gmail.create_label first in this request", index, field, name, mutation.Account)
					} else if err != nil && !errors.Is(err, errNoGmailLabel) {
						return fmt.Errorf("mutation %d: %w", index, err)
					}
				}
			}
		}
		apiAction, resolvable := gmailAPIAction(action, mailbox)
		newKey := ""
		if resolvable {
			newKey = gmailFilterKey(gmailAPICriteria(mapFromAny(payload["criteria"])), apiAction)
		}
		if mutation.Operation == GmailReplaceFilterOperation {
			old, err := snapshotGmailFilter(index, payload, preview, mutation.Account, mailbox)
			if err != nil {
				return err
			}
			if newKey != "" && newKey == gmailFilterKey(old.Criteria, old.Action) {
				return fmt.Errorf("mutation %d: the replacement is identical to filter %s; nothing would change", index, old.ID)
			}
		}
		if newKey != "" {
			for _, item := range mailbox.filters {
				if item.ID != stringFromAny(payload["filter_id"]) && gmailFilterKey(item.Criteria, item.Action) == newKey {
					return fmt.Errorf("mutation %d: filter %s already does exactly this on %s", index, item.ID, mutation.Account)
				}
			}
		}
	case GmailDeleteFilterOperation:
		if _, err := snapshotGmailFilter(index, payload, preview, mutation.Account, mailbox); err != nil {
			return err
		}
	}
	mutation.Payload = payload
	mutation.Preview = preview
	return nil
}

func orNone(value string) string {
	if value == "" {
		return "default"
	}
	return value
}

// snapshotGmailFilter copies the filter being replaced or deleted into the
// payload (expected_filter, Gmail's own shape) and its rule into the preview.
func snapshotGmailFilter(index int, payload map[string]any, preview map[string]any, account string, mailbox gmailMailbox) (gmailFilterRow, error) {
	id := stringFromAny(payload["filter_id"])
	old, ok := mailbox.filterByID(id)
	if !ok {
		return gmailFilterRow{}, fmt.Errorf("mutation %d: no live Gmail filter %s for %s in base_gmail.filters; read the filter_id there (it is snapshotted every 30 minutes)", index, id, account)
	}
	payload["expected_filter"] = map[string]any{"criteria": old.Criteria, "action": old.Action}
	view := mapFromAny(preview["gmail_filter"])
	previous := gmailRule(gmailFilterWhen(gmailCriteriaFromAPI(old.Criteria)), gmailFilterAPIActions(old.Action, mailbox.labelNames()))
	if stringFromAny(view["kind"]) == "delete" {
		view["rule"] = previous
	} else {
		view["previous_rule"] = previous
	}
	preview["gmail_filter"] = view
	return old, nil
}

// gmailLabelMessageCount is what deleting a label takes the label off. ok is
// false when the count could not be made in time: "unknown", never zero.
func (s *PostgresStore) gmailLabelMessageCount(ctx context.Context, account string, labelID string) (int64, int64, bool) {
	ctx, cancel := context.WithTimeout(ctx, gmailFilterMatchTimeout)
	defer cancel()
	var messages, threads int64
	err := queryRowContext(ctx, s.db, `
		SELECT count(*)::bigint, count(DISTINCT thread_id)::bigint
		FROM @gmail_messages
		WHERE account = $1 AND is_deleted = 0 AND label_ids @> ARRAY[$2]::text[]
	`, account, labelID).Scan(&messages, &threads)
	return messages, threads, err == nil
}

// --- read path ---------------------------------------------------------------

func (s *PostgresStore) hydrateGmailSettingsPreviews(ctx context.Context, mutations []Mutation) []Mutation {
	if s == nil || s.db == nil {
		return mutations
	}
	mailboxes := map[string]*gmailMailbox{}
	matched := 0
	for index := range mutations {
		mutation := &mutations[index]
		if mutation.Provider != "gmail" || !isGmailSettingsOperation(mutation.Operation) {
			continue
		}
		mailbox, ok := mailboxes[mutation.Account]
		if !ok {
			loaded, err := s.loadGmailMailbox(ctx, mutation.Account)
			if err == nil {
				mailbox = &loaded
			}
			mailboxes[mutation.Account] = mailbox
		}
		var match map[string]any
		if (mutation.Operation == GmailCreateFilterOperation || mutation.Operation == GmailReplaceFilterOperation) && matched < gmailFilterMatchPerPage {
			matched++
			match = s.gmailFilterMatchPreview(ctx, mutation.Account, mapFromAny(mutation.Payload["criteria"]))
		}
		mutation.Preview = applyGmailSettingsReadState(*mutation, mailbox, match)
	}
	return mutations
}

// applyGmailSettingsReadState refreshes a stored preview with what the
// warehouse says now. mailbox is nil when it could not be read.
func applyGmailSettingsReadState(mutation Mutation, mailbox *gmailMailbox, match map[string]any) map[string]any {
	preview := cloneMap(mutation.Preview)
	warnings := []string{}
	if mailbox == nil {
		warnings = append(warnings, "The current Gmail labels and filters could not be read from the warehouse; what is shown is the proposal-time snapshot.")
	}
	if isGmailFilterOperation(mutation.Operation) {
		view := mapFromAny(preview["gmail_filter"])
		if len(view) == 0 {
			view = mapFromAny(gmailSettingsPreview(mutation.Operation, mutation.Payload)["gmail_filter"])
		}
		action := mapFromAny(mutation.Payload["action"])
		if expected := mapFromAny(mutation.Payload["expected_filter"]); len(expected) > 0 && mailbox != nil {
			rule := gmailRule(gmailFilterWhen(gmailCriteriaFromAPI(mapFromAny(expected["criteria"]))), gmailFilterAPIActions(mapFromAny(expected["action"]), mailbox.labelNames()))
			if mutation.Operation == GmailDeleteFilterOperation {
				view["rule"] = rule
			} else {
				view["previous_rule"] = rule
			}
			if _, live := mailbox.filterByID(stringFromAny(mutation.Payload["filter_id"])); !live && mutation.Status == StatusPendingReview {
				warnings = append(warnings, "This filter is no longer in Gmail as of the last snapshot; approving changes nothing (a replace creates nothing).")
			}
		}
		if action["trash"] == true {
			warnings = append(warnings, "Matching mail will go straight to Trash, where Gmail deletes it after 30 days.")
		}
		if mutation.Operation == GmailDeleteFilterOperation {
			warnings = append(warnings, "Deleting this filter: new mail it would have caught lands in the inbox untouched.")
		}
		if match != nil {
			view["match"] = match
			if count, ok := match["thread_count"].(int64); ok && count >= gmailFilterBroadThreadCount {
				warnings = append(warnings, fmt.Sprintf("Broad rule: it would have matched %d threads in the last %d days.", count, gmailFilterMatchWindowDays))
			}
		}
		view["warnings"] = warnings
		preview["gmail_filter"] = view
		return preview
	}
	view := mapFromAny(preview["gmail_label"])
	if len(view) == 0 {
		view = mapFromAny(gmailSettingsPreview(mutation.Operation, mutation.Payload)["gmail_label"])
	}
	if mailbox != nil && mutation.Operation != GmailCreateLabelOperation {
		labelID := stringFromAny(mutation.Payload["label_id"])
		current, live := mailbox.labelByID(labelID)
		switch {
		case !live && mutation.Status == StatusPendingReview:
			warnings = append(warnings, "This label is no longer in Gmail as of the last snapshot.")
		case live:
			view["current"] = current.view()
			if expected := stringFromAny(mutation.Payload["expected_name"]); expected != "" && current.Name != expected && mutation.Status == StatusPendingReview {
				warnings = append(warnings, fmt.Sprintf("This label has been renamed to %q since it was proposed; approving will fail rather than change it.", current.Name))
			}
		}
	}
	if mutation.Operation == GmailDeleteLabelOperation {
		if _, known := view["message_count"]; !known {
			warnings = append(warnings, "How many messages carry this label could not be counted.")
		} else if count := int64FromAny(view["message_count"]); count >= gmailLabelManyMessagesWarned {
			warnings = append(warnings, fmt.Sprintf("%d messages carry this label; deleting it removes the label from them (the messages themselves stay in All Mail).", count))
		}
	}
	if filters := int64FromAny(view["filter_count"]); filters > 0 {
		warnings = append(warnings, fmt.Sprintf("%d filter(s) use this label.", filters))
	}
	if nested := int64FromAny(view["nested_label_count"]); nested > 0 && (mutation.Operation == GmailDeleteLabelOperation || stringFromAny(mutation.Payload["new_name"]) != "") {
		warnings = append(warnings, fmt.Sprintf("%d nested label(s) under it are separate labels and are not changed by this.", nested))
	}
	view["warnings"] = warnings
	preview["gmail_label"] = view
	return preview
}

// gmailCriterionTokenPattern is a from/to value the preview can match as a
// substring of an address: one token, no Gmail search operators.
var gmailCriterionTokenPattern = regexp.MustCompile(`^[^\s(){}"|]+$`)
var gmailSubjectUntranslatable = regexp.MustCompile(`[(){}"|]|(^|\s)(OR|AND)(\s|$)|(^|\s)-`)

func likeEscape(value string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(value)
}

// gmailFilterMatchSQL translates what it faithfully can of the criteria into
// a predicate over base_gmail.messages. ignored names the criteria it could
// not apply; a preview that ignored any is an upper bound, and one that
// applied none is unavailable rather than a count of everything.
func gmailFilterMatchSQL(criteria map[string]any, firstArg int) (where []string, args []any, applied []string, ignored []string) {
	add := func(clause string, value any) {
		args = append(args, value)
		where = append(where, fmt.Sprintf(clause, firstArg+len(args)-1))
	}
	if from := stringFromAny(criteria["from"]); from != "" {
		if gmailCriterionTokenPattern.MatchString(from) {
			add(`m.from_address ILIKE $%d`, "%"+likeEscape(from)+"%")
			applied = append(applied, "from")
		} else {
			ignored = append(ignored, "from")
		}
	}
	if to := stringFromAny(criteria["to"]); to != "" {
		if gmailCriterionTokenPattern.MatchString(to) {
			add(`array_to_string(m.to_addresses || m.cc_addresses || m.bcc_addresses, ' ') ILIKE $%d`, "%"+likeEscape(to)+"%")
			applied = append(applied, "to")
		} else {
			ignored = append(ignored, "to")
		}
	}
	if subject := stringFromAny(criteria["subject"]); subject != "" {
		if gmailSubjectUntranslatable.MatchString(subject) {
			ignored = append(ignored, "subject")
		} else {
			for _, word := range strings.Fields(subject) {
				add(`m.subject ILIKE $%d`, "%"+likeEscape(word)+"%")
			}
			applied = append(applied, "subject")
		}
	}
	if criteria["has_attachment"] == true {
		where = append(where, `EXISTS (SELECT 1 FROM @gmail_attachments AS a WHERE a.account = m.account AND a.message_id = m.message_id AND a.filename <> '')`)
		applied = append(applied, "has_attachment")
	}
	if size := int64FromAny(criteria["size"]); size > 0 {
		operator := ">"
		if stringFromAny(criteria["size_comparison"]) == "smaller" {
			operator = "<"
		}
		add(`m.size_estimate `+operator+` $%d`, size)
		applied = append(applied, "size")
	}
	for _, field := range []string{"query", "negated_query"} {
		if stringFromAny(criteria[field]) != "" {
			ignored = append(ignored, field)
		}
	}
	return where, args, applied, ignored
}

func (s *PostgresStore) gmailFilterMatchPreview(ctx context.Context, account string, criteria map[string]any) map[string]any {
	where, args, applied, ignored := gmailFilterMatchSQL(criteria, 2)
	out := map[string]any{"window_days": gmailFilterMatchWindowDays, "applied": applied, "ignored": ignored}
	if len(applied) == 0 {
		out["status"] = "unavailable"
		out["note"] = "No preview: Gmail search syntax (" + strings.Join(ignored, ", ") + ") cannot be checked against the warehouse faithfully."
		return out
	}
	ctx, cancel := context.WithTimeout(ctx, gmailFilterMatchTimeout)
	defer cancel()
	rows, err := queryContext(ctx, s.db, fmt.Sprintf(`
		WITH matched AS MATERIALIZED (
			SELECT m.thread_id, m.message_id, m.from_address, m.subject, m.internal_date,
			       COALESCE(substring(m.payload_json from '"name"\s*:\s*"From"\s*,\s*"value"\s*:\s*"((?:[^"\\]|\\.)*)"'), '') AS from_header
			FROM @gmail_messages AS m
			WHERE m.account = $1
			  AND m.is_deleted = 0
			  AND m.internal_date >= now() - interval '%d days'
			  AND %s
		)
		SELECT counts.messages, counts.threads,
		       COALESCE(example.thread_id, ''), COALESCE(example.from_address, ''), COALESCE(example.from_header, ''),
		       COALESCE(example.subject, ''), example.internal_date
		FROM (SELECT count(*)::bigint AS messages, count(DISTINCT thread_id)::bigint AS threads FROM matched) AS counts
		LEFT JOIN LATERAL (
			SELECT * FROM (
				SELECT DISTINCT ON (thread_id) * FROM matched ORDER BY thread_id, internal_date DESC
			) AS latest
			ORDER BY internal_date DESC
			LIMIT %d
		) AS example ON TRUE
	`, gmailFilterMatchWindowDays, strings.Join(where, " AND "), gmailFilterMatchExamples), append([]any{account}, args...)...)
	if err != nil {
		out["status"] = "unknown"
		out["note"] = "The match preview could not be computed in time; this is unknown, not zero."
		return out
	}
	defer rows.Close()
	examples := []map[string]any{}
	var messages, threads int64
	for rows.Next() {
		var threadID, fromAddress, fromHeader, subject string
		var at *time.Time
		if err := rows.Scan(&messages, &threads, &threadID, &fromAddress, &fromHeader, &subject, &at); err != nil {
			out["status"] = "unknown"
			return out
		}
		if threadID == "" || at == nil {
			continue
		}
		examples = append(examples, map[string]any{
			"thread_id":    threadID,
			"from_address": fromAddress,
			"from_name":    gmailHeaderDisplayName(fromHeader),
			"subject":      subject,
			"date":         at.UTC().Format(time.RFC3339),
		})
	}
	if rows.Err() != nil {
		out["status"] = "unknown"
		out["note"] = "The match preview could not be computed in time; this is unknown, not zero."
		return out
	}
	out["message_count"] = messages
	out["thread_count"] = threads
	out["examples"] = examples
	if len(ignored) > 0 {
		out["status"] = "upper_bound"
		out["note"] = fmt.Sprintf("Approximate upper bound: matched on %s only; %s use Gmail search syntax the warehouse cannot apply, so the real rule matches fewer.", strings.Join(applied, ", "), strings.Join(ignored, ", "))
	} else {
		out["status"] = "approximate"
		out["note"] = fmt.Sprintf("Approximate: substring match on %s over the last %d days of synced mail.", strings.Join(applied, ", "), gmailFilterMatchWindowDays)
	}
	return out
}
