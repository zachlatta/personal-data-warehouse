package mutations

import "strings"

// canonicalContactOp maps the short verb form ("create"/"update"/"delete") that
// some proposers emit to the canonical form used by the rest of this package.
func canonicalContactOp(op string) string {
	switch op {
	case "create":
		return "create_contact"
	case "update":
		return "update_contact"
	case "delete":
		return "delete_contact"
	}
	return op
}

func contactUpdateFields(operation map[string]any) []string {
	for _, key := range []string{"update_person_fields", "updatePersonFields"} {
		value := operation[key]
		if values := stringSliceFromAny(value); len(values) > 0 {
			return values
		}
		if text := strings.TrimSpace(stringFromAny(value)); text != "" {
			return normalizeStringSlice(strings.Split(text, ","))
		}
	}
	return nil
}
