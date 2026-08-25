package mutations

import (
	"errors"
	"fmt"
	"html"
	"net/http"
	"strings"
)

// Apple Notes mutations are executed on a Mac, not in the cloud worker: Notes has no
// server API, so the only write path is Notes.app itself over AppleScript. The proposal
// and review halves live here with every other mutation type; the executor is the local
// apple-notes uploader, which claims exactly this provider.

const appleNotesBodyPreviewLimit = 400

// appleNotesDefaultFolder is where a create lands when the proposal names no folder. It is
// deliberately NOT "Notes": an agent-authored note should be separable from hand-written
// ones without reading every note's provenance.
const appleNotesDefaultFolder = "PDW Agent"

func isAppleNotesMutation(mutation Mutation) bool {
	if mutation.Provider == AppleNotesProvider {
		return true
	}
	switch mutation.Operation {
	case AppleNotesCreateNoteOperation, AppleNotesUpdateNoteOperation:
		return true
	}
	return false
}

func normalizeAppleNotesFolder(value string) string {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		return trimmed
	}
	return appleNotesDefaultFolder
}

// validateAppleNotesMutation rejects at proposal time what the AppleScript executor
// could only discover after a human had already approved it.
func validateAppleNotesMutation(mutation MutationInput) error {
	name := strings.TrimSpace(mutation.Name)
	body := strings.TrimSpace(mutation.Body)
	appendBody := strings.TrimSpace(mutation.AppendBody)

	switch mutation.Type {
	case AppleNotesCreateNoteOperation:
		if body == "" {
			return errors.New("must include body")
		}
		if appendBody != "" {
			return errors.New("append_body is only valid on " + AppleNotesUpdateNoteOperation)
		}
	case AppleNotesUpdateNoteOperation:
		if strings.TrimSpace(mutation.NoteID) == "" {
			return errors.New("must include note_id (read base_apple_notes.notes.note_id, or the id returned by a create)")
		}
		if body != "" && appendBody != "" {
			return errors.New("must set body or append_body, not both: body replaces the note, append_body adds to the end")
		}
		if name == "" && body == "" && appendBody == "" {
			return errors.New("must change something: set name, body, or append_body")
		}
	}
	return nil
}

func appleNotesTitle(mutation MutationInput) string {
	name := strings.TrimSpace(mutation.Name)
	switch mutation.Type {
	case AppleNotesCreateNoteOperation:
		if name == "" {
			return "Create note"
		}
		return "Create note: " + name
	default:
		if name == "" {
			return "Update note " + strings.TrimSpace(mutation.NoteID)
		}
		return "Update note: " + name
	}
}

func appleNotesBodyPreview(body string) string {
	trimmed := strings.TrimSpace(body)
	if len(trimmed) <= appleNotesBodyPreviewLimit {
		return trimmed
	}
	return trimmed[:appleNotesBodyPreviewLimit] + "…"
}

func appleNotesPayload(mutation MutationInput) map[string]any {
	payload := map[string]any{
		"name":        strings.TrimSpace(mutation.Name),
		"body":        mutation.Body,
		"append_body": mutation.AppendBody,
	}
	if mutation.Type == AppleNotesCreateNoteOperation {
		payload["folder"] = normalizeAppleNotesFolder(mutation.Folder)
		return payload
	}
	payload["note_id"] = strings.TrimSpace(mutation.NoteID)
	if folder := strings.TrimSpace(mutation.Folder); folder != "" {
		payload["folder"] = folder
	}
	return payload
}

func appleNotesPreview(mutation MutationInput) map[string]any {
	action := "update"
	if mutation.Type == AppleNotesCreateNoteOperation {
		action = "create"
	}
	body := mutation.Body
	changes := []string{}
	if strings.TrimSpace(mutation.Name) != "" {
		changes = append(changes, "title")
	}
	if strings.TrimSpace(mutation.Body) != "" {
		changes = append(changes, "body (replaced)")
	}
	if strings.TrimSpace(mutation.AppendBody) != "" {
		changes = append(changes, "body (appended)")
		body = mutation.AppendBody
	}
	preview := map[string]any{
		"action":       action,
		"name":         strings.TrimSpace(mutation.Name),
		"body_preview": appleNotesBodyPreview(body),
		"body_bytes":   len(body),
		"changes":      changes,
	}
	if mutation.Type == AppleNotesCreateNoteOperation {
		preview["folder"] = normalizeAppleNotesFolder(mutation.Folder)
	} else {
		preview["note_id"] = strings.TrimSpace(mutation.NoteID)
	}
	return preview
}

func renderAppleNotesMutation(w http.ResponseWriter, mutation Mutation) {
	note := mapFromAny(mutation.Preview["note"])
	action := stringFromAny(note["action"])
	heading := "Update Apple Note"
	if action == "create" {
		heading = "Create Apple Note"
	}
	fmt.Fprintf(w, `<article class="mutation apple-notes-mutation"><div class="mutation-head"><div><p class="eyebrow">Apple Notes</p><h3>%s</h3></div><span class="pill">%s</span></div>`,
		html.EscapeString(heading),
		html.EscapeString(mutation.Status),
	)
	fmt.Fprintf(w, `<p class="mutation-meta">%s for %s</p>`, html.EscapeString(mutation.Operation), html.EscapeString(mutation.Account))
	if title := stringFromAny(note["name"]); title != "" {
		fmt.Fprintf(w, `<p><strong>Title:</strong> %s</p>`, html.EscapeString(title))
	}
	if folder := stringFromAny(note["folder"]); folder != "" {
		fmt.Fprintf(w, `<p><strong>Folder:</strong> %s</p>`, html.EscapeString(folder))
	}
	if noteID := stringFromAny(note["note_id"]); noteID != "" {
		fmt.Fprintf(w, `<p><strong>Note:</strong> <code>%s</code></p>`, html.EscapeString(noteID))
	}
	if changes := stringSliceFromAny(note["changes"]); len(changes) > 0 {
		fmt.Fprintf(w, `<p><strong>Changes:</strong> %s</p>`, html.EscapeString(strings.Join(changes, ", ")))
	}
	if body := stringFromAny(note["body_preview"]); body != "" {
		fmt.Fprintf(w, `<pre class="apple-notes-body">%s</pre>`, html.EscapeString(body))
	}
	fmt.Fprint(w, `</article>`)
}
