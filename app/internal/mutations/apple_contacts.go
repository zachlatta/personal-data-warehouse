package mutations

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Apple Contacts mutations are executed on a Mac, not in the cloud worker: iCloud
// Contacts has no write API, so the only write path is Contacts.app itself over
// AppleScript. The proposal and review halves live here with every other mutation
// type; the executor is the local apple-contacts uploader, which claims exactly this
// provider (personal_data_warehouse_apple_contacts.mutation_worker).
//
// A card is addressed by base_apple_contacts.cards.card_id, which is also Contacts'
// own AppleScript id (`<UUID>:ABPerson`), so the id an agent finds is the id the
// executor accepts.

var appleContactsCardIDPattern = regexp.MustCompile(`^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}:ABPerson$`)

// appleContactsScalarFields are the settable single-valued card properties, in the
// order previews list them.
var appleContactsScalarFields = []string{
	"given_name", "family_name", "middle_name", "nickname", "organization", "job_title", "department",
}

// appleContactsListFields are the multi-valued properties; each entry is {label, value}.
var appleContactsListFields = []string{"emails", "phones", "urls"}

var appleContactsPhoneDigits = regexp.MustCompile(`\d`)

func isAppleContactsMutation(mutation Mutation) bool {
	if mutation.Provider == AppleContactsProvider {
		return true
	}
	switch mutation.Operation {
	case AppleContactsCreateContactOperation, AppleContactsUpdateContactOperation, AppleContactsMergeContactsOperation:
		return true
	}
	return false
}

// normalizeAppleContact validates and canonicalizes the `contact` object of a
// proposal: trimmed scalars, {label, value} list entries, no unknown keys. Unknown
// keys are an error rather than silently dropped, because a misspelled field
// ("email" for "emails") would otherwise produce an approved mutation that changes
// nothing while reporting success.
func normalizeAppleContact(raw map[string]any) (map[string]any, error) {
	out := map[string]any{}
	for key, value := range raw {
		switch key {
		case "given_name", "family_name", "middle_name", "nickname", "organization", "job_title", "department", "note", "append_note":
			text := strings.TrimSpace(stringFromAny(value))
			if key == "note" || key == "append_note" {
				text = stringFromAny(value)
			}
			if strings.TrimSpace(text) != "" {
				out[key] = text
			}
		case "emails", "phones", "urls":
			entries, err := normalizeAppleContactEntries(key, value)
			if err != nil {
				return nil, err
			}
			if len(entries) > 0 {
				out[key] = entries
			}
		default:
			return nil, fmt.Errorf("contact has unsupported field %q; supported: given_name, family_name, middle_name, nickname, organization, job_title, department, note, append_note, emails, phones, urls", key)
		}
	}
	if _, hasNote := out["note"]; hasNote {
		if _, hasAppend := out["append_note"]; hasAppend {
			return nil, errors.New("contact must set note or append_note, not both: note replaces the note, append_note adds to the end")
		}
	}
	return out, nil
}

func normalizeAppleContactEntries(kind string, value any) ([]map[string]any, error) {
	items, ok := value.([]any)
	if !ok {
		if value == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("contact.%s must be an array of {label, value} objects", kind)
	}
	out := make([]map[string]any, 0, len(items))
	seen := map[string]bool{}
	for index, item := range items {
		var label, text string
		switch entry := item.(type) {
		case map[string]any:
			label = strings.TrimSpace(stringFromAny(entry["label"]))
			text = strings.TrimSpace(stringFromAny(entry["value"]))
		case string:
			text = strings.TrimSpace(entry)
		default:
			return nil, fmt.Errorf("contact.%s[%d] must be a {label, value} object", kind, index)
		}
		if text == "" {
			return nil, fmt.Errorf("contact.%s[%d] must include value", kind, index)
		}
		switch kind {
		case "emails":
			if !strings.Contains(text, "@") || strings.ContainsAny(text, " \t\n") {
				return nil, fmt.Errorf("contact.emails[%d] %q is not an email address", index, text)
			}
		case "phones":
			if len(appleContactsPhoneDigits.FindAllString(text, -1)) < 7 {
				return nil, fmt.Errorf("contact.phones[%d] %q is not a phone number", index, text)
			}
		}
		key := strings.ToLower(text)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, map[string]any{"label": label, "value": text})
	}
	return out, nil
}

func normalizeAppleContactRemove(raw map[string]any) (map[string]any, error) {
	out := map[string]any{}
	for key, value := range raw {
		switch key {
		case "emails", "phones", "urls":
			values := normalizeUniqueStringSlice(stringSliceFromAny(value))
			if len(values) > 0 {
				out[key] = values
			}
		default:
			return nil, fmt.Errorf("remove has unsupported field %q; supported: emails, phones, urls", key)
		}
	}
	return out, nil
}

func normalizeAppleContactsCardID(value string) (string, error) {
	id := strings.TrimSpace(value)
	if !appleContactsCardIDPattern.MatchString(id) {
		return "", fmt.Errorf("card id %q must be a base_apple_contacts.cards.card_id (<UUID>:ABPerson)", id)
	}
	return id, nil
}

// validateAppleContactsMutation rejects at proposal time what the AppleScript
// executor could only discover after a human had already approved it.
func validateAppleContactsMutation(mutation MutationInput) error {
	contact, err := normalizeAppleContact(mutation.Contact)
	if err != nil {
		return err
	}
	remove, err := normalizeAppleContactRemove(mutation.Remove)
	if err != nil {
		return err
	}
	switch mutation.Type {
	case AppleContactsCreateContactOperation:
		if strings.TrimSpace(mutation.CardID) != "" || strings.TrimSpace(mutation.KeepCardID) != "" || len(mutation.MergeCardIDs) > 0 {
			return errors.New("create_contact takes no card ids")
		}
		if len(remove) > 0 {
			return errors.New("remove is only valid on " + AppleContactsUpdateContactOperation)
		}
		if _, ok := contact["append_note"]; ok {
			return errors.New("append_note is only valid on update_contact or merge_contacts; use note")
		}
		if contact["given_name"] == nil && contact["family_name"] == nil && contact["organization"] == nil {
			return errors.New("contact must include given_name, family_name, or organization")
		}
	case AppleContactsUpdateContactOperation:
		if _, err := normalizeAppleContactsCardID(mutation.CardID); err != nil {
			return fmt.Errorf("must include card_id: %w", err)
		}
		if len(contact) == 0 && len(remove) == 0 {
			return errors.New("must change something: set a field in contact, or list values in remove")
		}
	case AppleContactsMergeContactsOperation:
		keep, err := normalizeAppleContactsCardID(mutation.KeepCardID)
		if err != nil {
			return fmt.Errorf("must include keep_card_id: %w", err)
		}
		if len(mutation.MergeCardIDs) == 0 {
			return errors.New("must include merge_card_ids: the cards to fold into keep_card_id and delete")
		}
		if len(remove) > 0 {
			return errors.New("remove is only valid on " + AppleContactsUpdateContactOperation)
		}
		for _, raw := range mutation.MergeCardIDs {
			id, err := normalizeAppleContactsCardID(raw)
			if err != nil {
				return fmt.Errorf("merge_card_ids: %w", err)
			}
			if id == keep {
				return errors.New("merge_card_ids must not contain keep_card_id")
			}
		}
	}
	return nil
}

func appleContactsDisplayName(contact map[string]any) string {
	parts := []string{}
	for _, key := range []string{"given_name", "middle_name", "family_name"} {
		if text := strings.TrimSpace(stringFromAny(contact[key])); text != "" {
			parts = append(parts, text)
		}
	}
	if len(parts) == 0 {
		return strings.TrimSpace(stringFromAny(contact["organization"]))
	}
	return strings.Join(parts, " ")
}

func appleContactsTitle(mutation MutationInput) string {
	contact, _ := normalizeAppleContact(mutation.Contact)
	name := appleContactsDisplayName(contact)
	switch mutation.Type {
	case AppleContactsCreateContactOperation:
		if name == "" {
			return "Create contact"
		}
		return "Create contact: " + name
	case AppleContactsMergeContactsOperation:
		return fmt.Sprintf("Merge %d contact cards into %s", len(mutation.MergeCardIDs)+1, strings.TrimSpace(mutation.KeepCardID))
	default:
		if name == "" {
			return "Update contact " + strings.TrimSpace(mutation.CardID)
		}
		return "Update contact: " + name
	}
}

func appleContactsPayload(mutation MutationInput) map[string]any {
	contact, _ := normalizeAppleContact(mutation.Contact)
	payload := map[string]any{"contact": contact}
	switch mutation.Type {
	case AppleContactsUpdateContactOperation:
		payload["card_id"] = strings.TrimSpace(mutation.CardID)
		remove, _ := normalizeAppleContactRemove(mutation.Remove)
		payload["remove"] = remove
	case AppleContactsMergeContactsOperation:
		payload["keep_card_id"] = strings.TrimSpace(mutation.KeepCardID)
		ids := make([]string, 0, len(mutation.MergeCardIDs))
		for _, id := range mutation.MergeCardIDs {
			ids = append(ids, strings.TrimSpace(id))
		}
		payload["merge_card_ids"] = ids
	}
	return payload
}

// appleContactsPreview is what the reviewer sees before hydration: the action, the
// fields the proposal touches, and a `changes` list that names the destructive ones
// (removals, a replaced note, deleted cards) so a client can flag them. The read path
// adds `cards`, the current state of every card the proposal names.
func appleContactsPreview(mutation MutationInput) map[string]any {
	contact, _ := normalizeAppleContact(mutation.Contact)
	remove, _ := normalizeAppleContactRemove(mutation.Remove)
	changes := []string{}
	keys := make([]string, 0, len(contact))
	for key := range contact {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		switch key {
		case "note":
			changes = append(changes, "note (replaced)")
		case "append_note":
			changes = append(changes, "note (appended)")
		case "emails", "phones", "urls":
			changes = append(changes, key+" (added)")
		default:
			changes = append(changes, key)
		}
	}
	removeKeys := make([]string, 0, len(remove))
	for key := range remove {
		removeKeys = append(removeKeys, key)
	}
	sort.Strings(removeKeys)
	for _, key := range removeKeys {
		changes = append(changes, key+" (removed)")
	}
	preview := map[string]any{
		"name":    appleContactsDisplayName(contact),
		"contact": contact,
		"changes": changes,
	}
	switch mutation.Type {
	case AppleContactsCreateContactOperation:
		preview["action"] = "create"
	case AppleContactsUpdateContactOperation:
		preview["action"] = "update"
		preview["card_id"] = strings.TrimSpace(mutation.CardID)
		preview["remove"] = remove
	case AppleContactsMergeContactsOperation:
		preview["action"] = "merge"
		preview["keep_card_id"] = strings.TrimSpace(mutation.KeepCardID)
		ids := make([]string, 0, len(mutation.MergeCardIDs))
		for _, id := range mutation.MergeCardIDs {
			ids = append(ids, strings.TrimSpace(id))
		}
		preview["merge_card_ids"] = ids
		preview["changes"] = append(changes, fmt.Sprintf("%d card(s) deleted after merge", len(ids)))
	}
	return preview
}

// appleContactsCardIDsForMutations lists every card a request names, for the read-path
// hydration that shows the reviewer what those cards hold today.
func appleContactsCardIDsForMutations(mutations []Mutation) []string {
	seen := map[string]bool{}
	out := []string{}
	add := func(value string) {
		id := strings.TrimSpace(value)
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, id)
	}
	for _, mutation := range mutations {
		if !isAppleContactsMutation(mutation) {
			continue
		}
		add(stringFromAny(mutation.Payload["card_id"]))
		add(stringFromAny(mutation.Payload["keep_card_id"]))
		for _, id := range stringSliceFromAny(mutation.Payload["merge_card_ids"]) {
			add(id)
		}
	}
	return out
}

// applyAppleContactsCardRows attaches the current card rows to each mutation's
// preview under contact.cards, in the order the proposal names them (kept card
// first for a merge). A card the warehouse no longer holds is reported as such
// rather than omitted, so a merge of a vanished card is visible before approval.
func applyAppleContactsCardRows(mutations []Mutation, cards map[string]map[string]any) []Mutation {
	for index := range mutations {
		mutation := &mutations[index]
		if !isAppleContactsMutation(*mutation) {
			continue
		}
		ids := []string{}
		if id := strings.TrimSpace(stringFromAny(mutation.Payload["card_id"])); id != "" {
			ids = append(ids, id)
		}
		if id := strings.TrimSpace(stringFromAny(mutation.Payload["keep_card_id"])); id != "" {
			ids = append(ids, id)
		}
		ids = append(ids, stringSliceFromAny(mutation.Payload["merge_card_ids"])...)
		if len(ids) == 0 {
			continue
		}
		rows := make([]map[string]any, 0, len(ids))
		for _, id := range ids {
			if card, ok := cards[id]; ok {
				rows = append(rows, card)
				continue
			}
			rows = append(rows, map[string]any{"card_id": id, "missing": true})
		}
		preview := cloneMap(mutation.Preview)
		contact := cloneMap(mapFromAny(preview["contact"]))
		contact["cards"] = rows
		preview["contact"] = contact
		mutation.Preview = preview
	}
	return mutations
}
