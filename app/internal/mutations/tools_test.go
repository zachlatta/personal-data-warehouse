package mutations

import (
	"strings"
	"testing"
)

// propose_mutation_help only ever documented create_contact, so agents guessed
// the update shape. The 2026-08-15 gap-year cohort request guessed one that
// stored fine and then failed at execution with a People API 400.
func TestMutationHelpDocumentsContactUpdateAndDeleteShapes(t *testing.T) {
	var contacts MutationHelpType
	for _, entry := range MutationHelp().Mutations {
		if entry.Type == GooglePeopleContactsOperation {
			contacts = entry
		}
	}
	if contacts.Type == "" {
		t.Fatal("propose_mutation_help does not document the contacts mutation type")
	}
	for _, want := range []string{"update_contact", "delete_contact", "etag", "update_person_fields"} {
		if !strings.Contains(contacts.ExtraNotes, want) {
			t.Fatalf("contacts notes do not mention %q: %q", want, contacts.ExtraNotes)
		}
	}
	operations, ok := contacts.Example["operations"].([]map[string]any)
	if !ok {
		t.Fatalf("contacts example operations = %#v", contacts.Example["operations"])
	}
	ops := map[string]map[string]any{}
	for _, operation := range operations {
		ops[stringFromAny(operation["op"])] = operation
	}
	update, ok := ops["update_contact"]
	if !ok {
		t.Fatalf("contacts example has no update_contact operation: %#v", ops)
	}
	for _, field := range []string{"resource_name", "etag", "person"} {
		if _, ok := update[field]; !ok {
			t.Fatalf("update_contact example is missing %q: %#v", field, update)
		}
	}
	if _, ok := ops["delete_contact"]; !ok {
		t.Fatalf("contacts example has no delete_contact operation: %#v", ops)
	}
	// The documented example has to survive the proposer's own normalizer,
	// otherwise the help text teaches agents a shape that gets rejected.
	if _, err := normalizeContactOperations(operations); err != nil {
		t.Fatalf("documented contacts example does not normalize: %v", err)
	}
}

func TestMutationHelpDocumentsGmailThreadLabelChanges(t *testing.T) {
	var labels MutationHelpType
	for _, entry := range MutationHelp().Mutations {
		if entry.Type == GmailModifyThreadLabelsOperation {
			labels = entry
			break
		}
	}
	if labels.Type == "" {
		t.Fatal("propose_mutation_help does not document Gmail label changes")
	}
	fieldNames := map[string]bool{}
	for _, field := range labels.Fields {
		fieldNames[field.Name] = true
	}
	for _, want := range []string{"thread_ids", "add_labels", "create_and_add_labels", "remove_labels"} {
		if !fieldNames[want] {
			t.Fatalf("Gmail label help is missing %q: %#v", want, labels.Fields)
		}
	}
	if !strings.Contains(labels.ExtraNotes, "display name") || !strings.Contains(labels.ExtraNotes, "label ID") || !strings.Contains(labels.ExtraNotes, "created only when explicitly") {
		t.Fatalf("Gmail label notes do not explain names and IDs: %q", labels.ExtraNotes)
	}
}
