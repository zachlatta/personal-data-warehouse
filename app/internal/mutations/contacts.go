package mutations

import (
	"fmt"
	"sort"
	"strings"
)

// contactUpdatablePersonFields is Google People API's `updatePersonFields`
// mask vocabulary. Fields the API returns but refuses to update — `metadata`,
// `photos`, `coverPhotos`, `ageRanges` — are deliberately absent: naming one in
// the mask is a 400, and deriving one from a person body would produce the same
// empty-or-invalid mask that failed the 2026-08-15 gap-year cohort request.
var contactUpdatablePersonFields = map[string]struct{}{
	"addresses":      {},
	"biographies":    {},
	"birthdays":      {},
	"calendarUrls":   {},
	"clientData":     {},
	"emailAddresses": {},
	"events":         {},
	"externalIds":    {},
	"genders":        {},
	"imClients":      {},
	"interests":      {},
	"locales":        {},
	"locations":      {},
	"memberships":    {},
	"miscKeywords":   {},
	"names":          {},
	"nicknames":      {},
	"occupations":    {},
	"organizations":  {},
	"phoneNumbers":   {},
	"relations":      {},
	"sipAddresses":   {},
	"urls":           {},
	"userDefined":    {},
}

// normalizeContactOperations turns whatever shape an agent proposed into the
// single contract the Python executor reads. Before this existed the proposer
// stored agent operations verbatim, so a payload that passed "account is
// configured and operations is non-empty" could still be unexecutable — and
// only discovered that after a human had approved it.
func normalizeContactOperations(operations []map[string]any) ([]map[string]any, error) {
	normalized := make([]map[string]any, 0, len(operations))
	for index, raw := range operations {
		operation, err := normalizeContactOperation(raw, index)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, operation)
	}
	return normalized, nil
}

func normalizeContactOperation(raw map[string]any, index int) (map[string]any, error) {
	op := canonicalContactOp(strings.TrimSpace(stringFromAny(raw["op"])))
	clientOpID := strings.TrimSpace(stringFromAny(raw["client_op_id"]))
	if clientOpID == "" {
		clientOpID = fmt.Sprintf("op-%d", index)
	}
	switch op {
	case "create_contact":
		person := contactPersonFromOperation(raw)
		if len(person) == 0 {
			return nil, fmt.Errorf("contact operation %d create_contact must include person", index)
		}
		if strings.TrimSpace(stringFromAny(person["resourceName"])) != "" {
			return nil, fmt.Errorf("contact operation %d create_contact person must not include resourceName", index)
		}
		delete(person, "etag")
		return map[string]any{"op": op, "client_op_id": clientOpID, "person": person}, nil
	case "update_contact":
		resourceName, err := contactResourceName(raw, index)
		if err != nil {
			return nil, err
		}
		person := contactPersonFromOperation(raw)
		if len(person) == 0 {
			return nil, fmt.Errorf("contact operation %d update_contact must include person", index)
		}
		etag := contactExpectedEtag(raw, person)
		if etag == "" {
			// The People API rejects an update whose body carries no etag, and
			// an etag is also what lets the executor prove the contact still
			// looks the way the reviewer saw it.
			return nil, fmt.Errorf("contact operation %d update_contact must include etag (the current etag from base_google_contacts.cards)", index)
		}
		fields, clears, err := contactUpdateFieldMask(raw, person, index)
		if err != nil {
			return nil, err
		}
		person["resourceName"] = resourceName
		person["etag"] = etag
		normalized := map[string]any{
			"op":                   op,
			"client_op_id":         clientOpID,
			"resource_name":        resourceName,
			"expected_etag":        etag,
			"update_person_fields": fields,
			"person":               person,
		}
		if len(clears) > 0 {
			normalized["clear_person_fields"] = clears
		}
		return normalized, nil
	case "delete_contact":
		resourceName, err := contactResourceName(raw, index)
		if err != nil {
			return nil, err
		}
		etag := contactExpectedEtag(raw, nil)
		if etag == "" {
			return nil, fmt.Errorf("contact operation %d delete_contact must include etag so the deletion is checked against the reviewed contact", index)
		}
		normalized := map[string]any{
			"op":            op,
			"client_op_id":  clientOpID,
			"resource_name": resourceName,
			"expected_etag": etag,
		}
		if reason := strings.TrimSpace(stringFromAny(raw["reason"])); reason != "" {
			normalized["reason"] = reason
		}
		return normalized, nil
	default:
		return nil, fmt.Errorf("contact operation %d has unsupported op %q; expected create_contact, update_contact, or delete_contact", index, stringFromAny(raw["op"]))
	}
}

// contactPersonFromOperation accepts the `after` alias because the review UI
// has always rendered diff-shaped operations, so agents learned to send them.
func contactPersonFromOperation(raw map[string]any) map[string]any {
	for _, key := range []string{"person", "after"} {
		if person := mapFromAny(raw[key]); len(person) > 0 {
			return person
		}
	}
	return map[string]any{}
}

func contactResourceName(raw map[string]any, index int) (string, error) {
	person := contactPersonFromOperation(raw)
	for _, value := range []string{
		stringFromAny(raw["resource_name"]),
		stringFromAny(raw["resourceName"]),
		stringFromAny(person["resourceName"]),
	} {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if !strings.HasPrefix(trimmed, "people/") {
			return "", fmt.Errorf("contact operation %d resource_name must be a people/* resource name, got %q", index, trimmed)
		}
		return trimmed, nil
	}
	return "", fmt.Errorf("contact operation %d must include resource_name", index)
}

func contactExpectedEtag(raw map[string]any, person map[string]any) string {
	candidates := []string{
		stringFromAny(raw["expected_etag"]),
		stringFromAny(raw["etag"]),
	}
	if person != nil {
		candidates = append(candidates, stringFromAny(person["etag"]))
	}
	for _, value := range candidates {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// contactUpdateFieldMask returns the mask to send and the fields that mask
// CLEARS. An explicit mask entry with no matching person value wipes that field
// in Google Contacts, so it is reported separately instead of hiding inside the
// raw payload where a reviewer would never see it.
func contactUpdateFieldMask(raw map[string]any, person map[string]any, index int) ([]string, []string, error) {
	requested := contactUpdateFields(raw)
	if len(requested) == 0 {
		derived := make([]string, 0, len(person))
		for key := range person {
			if _, ok := contactUpdatablePersonFields[key]; ok {
				derived = append(derived, key)
			}
		}
		if len(derived) == 0 {
			return nil, nil, fmt.Errorf("contact operation %d update_contact person contains no updatable fields; expected one of %s", index, contactUpdatablePersonFieldList())
		}
		sort.Strings(derived)
		return derived, nil, nil
	}

	fields := make([]string, 0, len(requested))
	clears := []string{}
	seen := map[string]struct{}{}
	for _, field := range requested {
		if _, ok := contactUpdatablePersonFields[field]; !ok {
			return nil, nil, fmt.Errorf("contact operation %d update_person_fields contains unsupported field %q; expected one of %s", index, field, contactUpdatablePersonFieldList())
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		fields = append(fields, field)
		if _, ok := person[field]; !ok {
			clears = append(clears, field)
		}
	}
	return fields, clears, nil
}

func contactUpdatablePersonFieldList() string {
	fields := make([]string, 0, len(contactUpdatablePersonFields))
	for field := range contactUpdatablePersonFields {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return strings.Join(fields, ", ")
}

// contactCardKey identifies one synced Google Contacts row.
type contactCardKey struct {
	Account      string
	ResourceName string
}

// contactOperationPreview builds what the review UI renders. It used to be a
// verbatim clone of the agent's operation, which is why a person-shaped update
// rendered "No explicit update fields were provided" and a human approved a
// blank diff. The `before` half is filled in later from the synced contact.
func contactOperationPreview(operation map[string]any, index int) map[string]any {
	preview := cloneMap(operation)
	preview["op_index"] = index
	if canonicalContactOp(strings.TrimSpace(stringFromAny(operation["op"]))) == "update_contact" {
		preview["after"] = mapFromAny(operation["person"])
	}
	return preview
}

func contactCardKeysForMutations(mutations []storedMutation) []contactCardKey {
	keys := []contactCardKey{}
	seen := map[contactCardKey]bool{}
	for _, mutation := range mutations {
		if mutation.Provider != "google_people" || mutation.Operation != ContactsBatchMutationOperation {
			continue
		}
		account := normalizeAccount(mutation.Account)
		if account == "" {
			continue
		}
		for _, operation := range storedContactOperations(mutation.Payload) {
			op := canonicalContactOp(strings.TrimSpace(stringFromAny(operation["op"])))
			if op != "update_contact" && op != "delete_contact" {
				continue
			}
			key := contactCardKey{Account: account, ResourceName: strings.TrimSpace(stringFromAny(operation["resource_name"]))}
			if key.ResourceName == "" || seen[key] {
				continue
			}
			keys = append(keys, key)
			seen[key] = true
		}
	}
	return keys
}

// applyContactCardRows folds the synced contact into each preview so the
// reviewer sees the real before/after, and flags a proposal whose etag has
// already moved — that update is guaranteed to be refused at execution time,
// and saying so beats approving a change that cannot land.
func applyContactCardRows(mutations []storedMutation, cards map[contactCardKey]map[string]any) {
	if len(cards) == 0 {
		return
	}
	for _, mutation := range mutations {
		if mutation.Provider != "google_people" || mutation.Operation != ContactsBatchMutationOperation {
			continue
		}
		account := normalizeAccount(mutation.Account)
		previews, ok := mutation.Preview["operations"].([]map[string]any)
		if !ok {
			continue
		}
		for _, preview := range previews {
			op := canonicalContactOp(strings.TrimSpace(stringFromAny(preview["op"])))
			if op != "update_contact" && op != "delete_contact" {
				continue
			}
			key := contactCardKey{Account: account, ResourceName: strings.TrimSpace(stringFromAny(preview["resource_name"]))}
			card, found := cards[key]
			if !found {
				preview["contact_found"] = false
				continue
			}
			preview["contact_found"] = true
			preview["before"] = card
			currentEtag := strings.TrimSpace(stringFromAny(card["etag"]))
			preview["current_etag"] = currentEtag
			preview["etag_is_current"] = currentEtag != "" && currentEtag == strings.TrimSpace(stringFromAny(preview["expected_etag"]))
		}
	}
}

func storedContactOperations(payload map[string]any) []map[string]any {
	if payload == nil {
		return nil
	}
	if operations, ok := payload["operations"].([]map[string]any); ok {
		return operations
	}
	return mapSliceFromAny(payload["operations"])
}
