package mutations

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

// The 2026-08-15 gap-year cohort request is the regression this file exists
// for: the agent proposed `{op, etag, person, resource_name}` with no
// `update_person_fields`, the Go proposer stored it verbatim, and the Python
// executor sent `updatePersonFields=` with no `person.etag` — People API 400.
func TestNormalizeContactOperationsDerivesUpdateFieldsAndCarriesEtag(t *testing.T) {
	operations, err := normalizeContactOperations([]map[string]any{
		{
			"op":            "update_contact",
			"etag":          "etag-live",
			"resource_name": "people/c1000000000000000001",
			"person": map[string]any{
				"names":          []any{map[string]any{"givenName": "Ada", "familyName": "Lovelace"}},
				"emailAddresses": []any{map[string]any{"type": "home", "value": "ada@example.test"}},
				"organizations":  []any{map[string]any{"name": "Hack Club", "title": "2026 Gap Year Fellow"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("normalizeContactOperations returned error: %v", err)
	}
	if len(operations) != 1 {
		t.Fatalf("operation count = %d", len(operations))
	}
	op := operations[0]
	if op["op"] != "update_contact" {
		t.Fatalf("op = %#v", op["op"])
	}
	if op["expected_etag"] != "etag-live" {
		t.Fatalf("expected_etag = %#v", op["expected_etag"])
	}
	fields := stringSliceFromAny(op["update_person_fields"])
	want := []string{"emailAddresses", "names", "organizations"}
	if strings.Join(fields, ",") != strings.Join(want, ",") {
		t.Fatalf("update_person_fields = %#v, want %#v", fields, want)
	}
	person := mapFromAny(op["person"])
	if person["etag"] != "etag-live" {
		t.Fatalf("person.etag = %#v; the People API rejects an update without it", person["etag"])
	}
	if person["resourceName"] != "people/c1000000000000000001" {
		t.Fatalf("person.resourceName = %#v", person["resourceName"])
	}
	if clears := stringSliceFromAny(op["clear_person_fields"]); len(clears) != 0 {
		t.Fatalf("clear_person_fields = %#v; a derived mask can never clear a field", clears)
	}
}

func TestNormalizeContactOperationsRejectsUpdateWithoutEtag(t *testing.T) {
	_, err := normalizeContactOperations([]map[string]any{
		{
			"op":            "update_contact",
			"resource_name": "people/1",
			"person":        map[string]any{"names": []any{map[string]any{"givenName": "Ada"}}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "etag") {
		t.Fatalf("error = %v, want an etag complaint", err)
	}
}

func TestNormalizeContactOperationsRejectsUpdateWithoutPerson(t *testing.T) {
	_, err := normalizeContactOperations([]map[string]any{
		{"op": "update_contact", "resource_name": "people/1", "etag": "etag-1"},
	})
	if err == nil || !strings.Contains(err.Error(), "person") {
		t.Fatalf("error = %v, want a person complaint", err)
	}
}

func TestNormalizeContactOperationsRejectsUpdateWithoutResourceName(t *testing.T) {
	_, err := normalizeContactOperations([]map[string]any{
		{"op": "update_contact", "etag": "etag-1", "person": map[string]any{"names": []any{map[string]any{"givenName": "Ada"}}}},
	})
	if err == nil || !strings.Contains(err.Error(), "resource_name") {
		t.Fatalf("error = %v, want a resource_name complaint", err)
	}
}

// A person body carrying only unmodifiable fields yields an empty field mask,
// which is exactly the `updatePersonFields=` request that failed in prod.
func TestNormalizeContactOperationsRejectsPersonWithNoUpdatableFields(t *testing.T) {
	_, err := normalizeContactOperations([]map[string]any{
		{
			"op":            "update_contact",
			"resource_name": "people/1",
			"etag":          "etag-1",
			"person":        map[string]any{"photos": []any{map[string]any{"url": "https://example.test/a.png"}}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "updatable") {
		t.Fatalf("error = %v, want an updatable-fields complaint", err)
	}
}

// An explicit mask naming a field the person body omits CLEARS that field in
// Google Contacts. That is legal but destructive, so it must be surfaced to the
// reviewer rather than hidden inside the raw payload.
func TestNormalizeContactOperationsRecordsExplicitFieldClears(t *testing.T) {
	operations, err := normalizeContactOperations([]map[string]any{
		{
			"op":                   "update_contact",
			"resource_name":        "people/1",
			"expected_etag":        "etag-1",
			"update_person_fields": []any{"names", "biographies"},
			"person":               map[string]any{"names": []any{map[string]any{"givenName": "Ada"}}},
		},
	})
	if err != nil {
		t.Fatalf("normalizeContactOperations returned error: %v", err)
	}
	if got := stringSliceFromAny(operations[0]["clear_person_fields"]); len(got) != 1 || got[0] != "biographies" {
		t.Fatalf("clear_person_fields = %#v", got)
	}
}

func TestNormalizeContactOperationsRejectsUnsupportedUpdateField(t *testing.T) {
	_, err := normalizeContactOperations([]map[string]any{
		{
			"op":                   "update_contact",
			"resource_name":        "people/1",
			"expected_etag":        "etag-1",
			"update_person_fields": []any{"names", "photos"},
			"person":               map[string]any{"names": []any{map[string]any{"givenName": "Ada"}}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "photos") {
		t.Fatalf("error = %v, want a photos complaint", err)
	}
}

func TestNormalizeContactOperationsAcceptsAfterAsPersonAlias(t *testing.T) {
	operations, err := normalizeContactOperations([]map[string]any{
		{
			"op":            "update",
			"resource_name": "people/1",
			"etag":          "etag-1",
			"after":         map[string]any{"nicknames": []any{map[string]any{"value": "Ada"}}},
		},
	})
	if err != nil {
		t.Fatalf("normalizeContactOperations returned error: %v", err)
	}
	if operations[0]["op"] != "update_contact" {
		t.Fatalf("op = %#v", operations[0]["op"])
	}
	if got := stringSliceFromAny(operations[0]["update_person_fields"]); len(got) != 1 || got[0] != "nicknames" {
		t.Fatalf("update_person_fields = %#v", got)
	}
}

func TestNormalizeContactOperationsCreateRejectsResourceName(t *testing.T) {
	_, err := normalizeContactOperations([]map[string]any{
		{"op": "create_contact", "person": map[string]any{"resourceName": "people/1", "names": []any{map[string]any{"givenName": "Ada"}}}},
	})
	if err == nil || !strings.Contains(err.Error(), "resourceName") {
		t.Fatalf("error = %v, want a resourceName complaint", err)
	}
}

func TestNormalizeContactOperationsCreateRequiresPerson(t *testing.T) {
	_, err := normalizeContactOperations([]map[string]any{{"op": "create_contact"}})
	if err == nil || !strings.Contains(err.Error(), "person") {
		t.Fatalf("error = %v, want a person complaint", err)
	}
}

// Deletes are irreversible, so the reviewer must be approving a known state.
func TestNormalizeContactOperationsDeleteRequiresEtag(t *testing.T) {
	_, err := normalizeContactOperations([]map[string]any{
		{"op": "delete_contact", "resource_name": "people/1"},
	})
	if err == nil || !strings.Contains(err.Error(), "etag") {
		t.Fatalf("error = %v, want an etag complaint", err)
	}
}

func TestNormalizeContactOperationsRejectsUnknownOp(t *testing.T) {
	_, err := normalizeContactOperations([]map[string]any{{"op": "merge_contact"}})
	if err == nil || !strings.Contains(err.Error(), "merge_contact") {
		t.Fatalf("error = %v", err)
	}
}

func TestNormalizeContactOperationsAssignsClientOpIDs(t *testing.T) {
	operations, err := normalizeContactOperations([]map[string]any{
		{"op": "create_contact", "person": map[string]any{"names": []any{map[string]any{"givenName": "Ada"}}}},
		{"op": "create_contact", "client_op_id": "explicit", "person": map[string]any{"names": []any{map[string]any{"givenName": "Bob"}}}},
	})
	if err != nil {
		t.Fatalf("normalizeContactOperations returned error: %v", err)
	}
	if operations[0]["client_op_id"] != "op-0" || operations[1]["client_op_id"] != "explicit" {
		t.Fatalf("client_op_ids = %#v, %#v", operations[0]["client_op_id"], operations[1]["client_op_id"])
	}
}

// The stored payload is what the Python executor reads, so it must be JSON
// round-trippable into exactly the contract that executor expects.
func TestNormalizeForStorageStoresExecutorReadyContactOperations(t *testing.T) {
	mutations, err := normalizeForStorage(CreateRequestInput{
		Title:  "Update a contact",
		Reason: "roster sync",
		Mutations: []MutationInput{{
			Type:    GooglePeopleContactsOperation,
			Account: "zach@example.test",
			Operations: []map[string]any{{
				"op":            "update_contact",
				"etag":          "etag-live",
				"resource_name": "people/1",
				"person":        map[string]any{"biographies": []any{map[string]any{"value": "note", "contentType": "TEXT_PLAIN"}}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("normalizeForStorage returned error: %v", err)
	}
	encoded, err := json.Marshal(mutations[0].Payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	var decoded struct {
		Operations []struct {
			Op                 string   `json:"op"`
			ResourceName       string   `json:"resource_name"`
			ExpectedEtag       string   `json:"expected_etag"`
			UpdatePersonFields []string `json:"update_person_fields"`
			Person             struct {
				Etag         string `json:"etag"`
				ResourceName string `json:"resourceName"`
			} `json:"person"`
		} `json:"operations"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(decoded.Operations) != 1 {
		t.Fatalf("operations = %s", encoded)
	}
	stored := decoded.Operations[0]
	if stored.ExpectedEtag != "etag-live" || stored.Person.Etag != "etag-live" {
		t.Fatalf("etags = %#v", stored)
	}
	if stored.Person.ResourceName != "people/1" {
		t.Fatalf("person.resourceName = %q", stored.Person.ResourceName)
	}
	if len(stored.UpdatePersonFields) != 1 || stored.UpdatePersonFields[0] != "biographies" {
		t.Fatalf("update_person_fields = %#v", stored.UpdatePersonFields)
	}
}

// The review UI renders the diff from preview `before`/`after`. Before this
// change the preview was a verbatim clone of the agent's op, so a person-shaped
// update rendered "No explicit update fields were provided" and a human
// approved a blank diff.
func TestContactOperationPreviewCarriesDiffFields(t *testing.T) {
	operations, err := normalizeContactOperations([]map[string]any{{
		"op":            "update_contact",
		"etag":          "etag-live",
		"resource_name": "people/1",
		"person":        map[string]any{"names": []any{map[string]any{"givenName": "Ada"}}},
	}})
	if err != nil {
		t.Fatalf("normalizeContactOperations returned error: %v", err)
	}
	preview := contactOperationPreview(operations[0], 0)
	if preview["op_index"] != 0 {
		t.Fatalf("op_index = %#v", preview["op_index"])
	}
	after := mapFromAny(preview["after"])
	if _, ok := after["names"]; !ok {
		t.Fatalf("preview after = %#v", after)
	}
	if got := stringSliceFromAny(preview["update_person_fields"]); len(got) != 1 || got[0] != "names" {
		t.Fatalf("preview update_person_fields = %#v", got)
	}
}

func TestApplyContactCardRowsFillsPreviewBefore(t *testing.T) {
	operations, err := normalizeContactOperations([]map[string]any{{
		"op":            "update_contact",
		"etag":          "etag-live",
		"resource_name": "people/1",
		"person":        map[string]any{"names": []any{map[string]any{"givenName": "Ada"}}},
	}})
	if err != nil {
		t.Fatalf("normalizeContactOperations returned error: %v", err)
	}
	stored := []storedMutation{{
		Provider:  "google_people",
		Operation: ContactsBatchMutationOperation,
		Account:   "zach@example.test",
		Payload:   map[string]any{"operations": []map[string]any{operations[0]}},
		Preview: map[string]any{
			"operation_count": 1,
			"operations":      []map[string]any{contactOperationPreview(operations[0], 0)},
		},
	}}
	rows := map[contactCardKey]map[string]any{
		{Account: "zach@example.test", ResourceName: "people/1"}: {
			"names": []any{map[string]any{"givenName": "Ada", "displayName": "Ada Lovelace"}},
			"etag":  "etag-live",
		},
	}
	applyContactCardRows(stored, rows)
	previews, ok := stored[0].Preview["operations"].([]map[string]any)
	if !ok || len(previews) != 1 {
		t.Fatalf("preview operations = %#v", stored[0].Preview["operations"])
	}
	before := mapFromAny(previews[0]["before"])
	if _, ok := before["names"]; !ok {
		t.Fatalf("preview before = %#v", before)
	}
	if previews[0]["etag_is_current"] != true {
		t.Fatalf("etag_is_current = %#v", previews[0]["etag_is_current"])
	}
}

// A proposal built on a stale etag would be rejected at execution time. Say so
// in the review UI instead of letting the reviewer approve a doomed change.
func TestApplyContactCardRowsFlagsStaleEtag(t *testing.T) {
	operations, err := normalizeContactOperations([]map[string]any{{
		"op":            "update_contact",
		"etag":          "etag-old",
		"resource_name": "people/1",
		"person":        map[string]any{"names": []any{map[string]any{"givenName": "Ada"}}},
	}})
	if err != nil {
		t.Fatalf("normalizeContactOperations returned error: %v", err)
	}
	stored := []storedMutation{{
		Provider:  "google_people",
		Operation: ContactsBatchMutationOperation,
		Account:   "zach@example.test",
		Payload:   map[string]any{"operations": []map[string]any{operations[0]}},
		Preview: map[string]any{
			"operation_count": 1,
			"operations":      []map[string]any{contactOperationPreview(operations[0], 0)},
		},
	}}
	rows := map[contactCardKey]map[string]any{
		{Account: "zach@example.test", ResourceName: "people/1"}: {"etag": "etag-new"},
	}
	applyContactCardRows(stored, rows)
	previews := stored[0].Preview["operations"].([]map[string]any)
	if previews[0]["etag_is_current"] != false {
		t.Fatalf("etag_is_current = %#v", previews[0]["etag_is_current"])
	}
	if previews[0]["current_etag"] != "etag-new" {
		t.Fatalf("current_etag = %#v", previews[0]["current_etag"])
	}
}

func TestContactCardKeysForMutationsCollectsUpdateAndDeleteTargets(t *testing.T) {
	operations, err := normalizeContactOperations([]map[string]any{
		{"op": "update_contact", "resource_name": "people/1", "etag": "e1", "person": map[string]any{"names": []any{map[string]any{"givenName": "A"}}}},
		{"op": "delete_contact", "resource_name": "people/2", "etag": "e2"},
		{"op": "create_contact", "person": map[string]any{"names": []any{map[string]any{"givenName": "C"}}}},
	})
	if err != nil {
		t.Fatalf("normalizeContactOperations returned error: %v", err)
	}
	stored := make([]storedMutation, 0, len(operations))
	for index, operation := range operations {
		stored = append(stored, storedMutation{
			Provider:  "google_people",
			Operation: ContactsBatchMutationOperation,
			Account:   "zach@example.test",
			Payload:   map[string]any{"operations": []map[string]any{operation}},
			Preview:   map[string]any{"operations": []map[string]any{contactOperationPreview(operation, index)}},
		})
	}
	keys := contactCardKeysForMutations(stored)
	if len(keys) != 2 {
		t.Fatalf("keys = %#v", keys)
	}
}

// contractPath points at the fixture the Python executor's tests read too. The
// producer and the consumer live in different languages, so the only way to
// stop them drifting is to make both assert against the same file.
const contactContractPath = "../../../tests/contracts/google_contacts_mutation_operations.json"

func TestNormalizeContactOperationsMatchesTheSharedContract(t *testing.T) {
	data, err := os.ReadFile(contactContractPath)
	if err != nil {
		t.Fatalf("read contract: %v", err)
	}
	var contract struct {
		Cases []struct {
			Name       string         `json:"name"`
			Proposed   map[string]any `json:"proposed"`
			Normalized map[string]any `json:"normalized"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatalf("decode contract: %v", err)
	}
	if len(contract.Cases) == 0 {
		t.Fatal("contract has no cases")
	}
	for _, testCase := range contract.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			operations, err := normalizeContactOperations([]map[string]any{testCase.Proposed})
			if err != nil {
				t.Fatalf("normalizeContactOperations returned error: %v", err)
			}
			got, err := json.Marshal(operations[0])
			if err != nil {
				t.Fatalf("marshal normalized: %v", err)
			}
			want, err := json.Marshal(testCase.Normalized)
			if err != nil {
				t.Fatalf("marshal expected: %v", err)
			}
			var gotAny, wantAny any
			_ = json.Unmarshal(got, &gotAny)
			_ = json.Unmarshal(want, &wantAny)
			if !reflect.DeepEqual(gotAny, wantAny) {
				t.Fatalf("normalized operation\n got: %s\nwant: %s", got, want)
			}
		})
	}
}
