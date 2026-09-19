// Package applenotes applies approved apple_notes.* mutations through
// Notes.app, the Go twin of personal_data_warehouse.apple_notes_mutations and
// personal_data_warehouse_apple_notes.mutation_worker.
//
// Apple Notes has no server API. iCloud exposes no write endpoint, the
// desktop app keeps its content as gzipped protobuf inside a Core Data store,
// and the only supported way to change a note is to ask Notes.app itself. So
// this executor is AppleScript, and it must run on a Mac that is signed in to
// the account, which is why the cloud mutation worker deliberately does not
// claim this provider.
//
// The AppleScript is built here rather than kept as a template so that every
// value crossing into it goes through applescript.String. AppleScript has no
// parameter binding: a note body is spliced into the script text, so an
// unescaped quote is not a failed write but an arbitrary-script-execution bug.
package applenotes

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/applescript"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

const (
	Provider            = "apple_notes"
	CreateNoteOperation = "apple_notes.create_note"
	UpdateNoteOperation = "apple_notes.update_note"

	// DefaultFolder is where a create with no folder lands.
	DefaultFolder = "PDW Agent"

	// LockID is distinct from the cloud worker's and the Contacts worker's:
	// the queues are disjoint by provider, so sharing a lock would make a long
	// cloud batch block Notes writes for no reason.
	LockID int64 = 7_403_111_851

	// resultSeparator keeps the id and the resulting title apart.
	resultSeparator = "\n"
	coreDataPrefix  = "x-coredata://"
)

var (
	markupRE = regexp.MustCompile(`<[a-zA-Z/!]`)
	uuidRE   = regexp.MustCompile(`^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`)
	// leadingDivRE matches the first <div>…</div>, which is the line Notes
	// titles from.
	leadingDivRE = regexp.MustCompile(`(?s)^\s*<div>.*?</div>`)
)

// ErrNoteNotFound is raised when a proposed note reference matches no note in
// the local store; it is terminal.
var ErrNoteNotFound = errors.New("apple note not found")

// Lookup resolves a warehouse note UUID to (store uuid, Z_PK); ok is false
// when the local store has no such note.
type Lookup func(uuid string) (storeUUID string, primaryKey int64, ok bool, err error)

// Executor executes apple_notes.* mutations claimed from
// ops.upstream_mutation_operations.
type Executor struct {
	runner applescript.Runner
	lookup Lookup
}

// NewExecutor builds an executor; a nil runner uses osascript and a nil
// lookup reads the local NoteStore snapshot.
func NewExecutor(runner applescript.Runner, lookup Lookup) *Executor {
	if runner == nil {
		runner = applescript.Run
	}
	if lookup == nil {
		lookup = func(uuid string) (string, int64, bool, error) {
			return NotePrimaryKeyFromStore(uuid, "")
		}
	}
	return &Executor{runner: runner, lookup: lookup}
}

// Execute implements queue.Executor.
func (e *Executor) Execute(mutation queue.Mutation) queue.Result {
	if mutation.Provider != Provider || (mutation.Operation != CreateNoteOperation && mutation.Operation != UpdateNoteOperation) {
		// Never burn an unrecognized row to failed_terminal: a newer worker
		// may understand it. This mirrors the cloud worker's unknown-provider
		// handling.
		return queue.Result{
			Status: queue.StatusFailedRetryable,
			Error:  fmt.Sprintf("unsupported mutation operation %s.%s; deferring", mutation.Provider, mutation.Operation),
		}
	}
	payload := mutation.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	var result queue.Result
	var err error
	if mutation.Operation == CreateNoteOperation {
		result, err = e.createNote(payload)
	} else {
		result, err = e.updateNote(payload)
	}
	if err == nil {
		return result
	}
	if errors.Is(err, applescript.ErrTimeout) {
		return queue.Result{
			Status: queue.StatusFailedRetryable,
			Error:  fmt.Sprintf("Notes.app did not answer within %ds", int(applescript.DefaultScriptTimeout.Seconds())),
		}
	}
	if errors.Is(err, ErrNoteNotFound) {
		return queue.Result{Status: queue.StatusFailedTerminal, Error: err.Error()}
	}
	if errors.Is(err, ErrStoreUnavailable) {
		// The Python executor let a store read failure propagate, which
		// crashed the run and left the (idempotent) update to be reclaimed;
		// the honest port of "try again later" is failed_retryable.
		return queue.Result{Status: queue.StatusFailedRetryable, Error: err.Error()}
	}
	status, message := applescript.Classify(err.Error(), "Notes.app")
	return queue.Result{Status: status, Error: message}
}

func (e *Executor) createNote(payload map[string]any) (queue.Result, error) {
	folder := strings.TrimSpace(stringValue(payload["folder"]))
	if folder == "" {
		folder = DefaultFolder
	}
	name := strings.TrimSpace(stringValue(payload["name"]))
	bodyHTML := BodyToHTML(stringValue(payload["body"]))
	// Notes takes the note's title from the first line of the body, so a
	// proposal that sets `name` gets that name promoted into a leading
	// heading. Setting the `name` property alone does not survive: Notes
	// recomputes it from the body.
	if name != "" {
		bodyHTML = "<div><h1>" + escapeHTML(name) + "</h1></div>" + bodyHTML
	}
	script := fmt.Sprintf(`
		with timeout of %d seconds
		  tell application "Notes"
		    set folderName to %s
		    set targetAccount to account 1
		    tell targetAccount
		      if not (exists folder folderName) then
		        make new folder with properties {name:folderName}
		      end if
		      set targetFolder to folder folderName
		      set newNote to make new note at targetFolder with properties {body:%s}
		      return (id of newNote) & %s & (name of newNote)
		    end tell
		  end tell
		end timeout
	`, applescript.InScriptTimeoutSeconds, applescript.String(folder), applescript.String(bodyHTML), applescript.String(resultSeparator))
	raw, err := e.runner(applescript.Dedent(script))
	if err != nil {
		return queue.Result{}, err
	}
	noteID, noteName := splitResult(raw)
	return queue.Result{
		Status: queue.StatusSucceeded,
		ResultJSON: map[string]any{
			"note_id": noteID,
			"name":    noteName,
			"folder":  folder,
			"action":  "create",
		},
	}, nil
}

func (e *Executor) updateNote(payload map[string]any) (queue.Result, error) {
	noteID, err := ResolveNoteReference(strings.TrimSpace(stringValue(payload["note_id"])), e.lookup)
	if err != nil {
		return queue.Result{}, err
	}
	name := strings.TrimSpace(stringValue(payload["name"]))
	body := stringValue(payload["body"])
	appendBody := stringValue(payload["append_body"])

	// Read the current body first, for two reasons: an append needs it, and
	// a replacement should leave the reviewer a way back. The read is a
	// separate script so a note that has vanished fails before anything is
	// written.
	readScript := fmt.Sprintf(`
		with timeout of %d seconds
		  tell application "Notes"
		    return body of note id %s
		  end tell
		end timeout
	`, applescript.InScriptTimeoutSeconds, applescript.String(noteID))
	previousBody, err := e.runner(applescript.Dedent(readScript))
	if err != nil {
		return queue.Result{}, err
	}

	var newBody, change string
	switch {
	case strings.TrimSpace(appendBody) != "":
		newBody = previousBody + BodyToHTML(appendBody)
		change = "append_body"
	case strings.TrimSpace(body) != "":
		newBody = BodyToHTML(body)
		change = "body"
	default:
		newBody = previousBody
		change = "name"
	}
	if name != "" {
		newBody = replaceLeadingHeading(newBody, name)
	}

	writeScript := fmt.Sprintf(`
		with timeout of %d seconds
		  tell application "Notes"
		    set targetNote to note id %s
		    set body of targetNote to %s
		    return (id of targetNote) & %s & (name of targetNote)
		  end tell
		end timeout
	`, applescript.InScriptTimeoutSeconds, applescript.String(noteID), applescript.String(newBody), applescript.String(resultSeparator))
	raw, err := e.runner(applescript.Dedent(writeScript))
	if err != nil {
		return queue.Result{}, err
	}
	resolvedID, resolvedName := splitResult(raw)
	if resolvedID == "" {
		resolvedID = noteID
	}
	return queue.Result{
		Status: queue.StatusSucceeded,
		ResultJSON: map[string]any{
			"note_id":       resolvedID,
			"name":          resolvedName,
			"action":        "update",
			"changed":       change,
			"previous_body": previousBody,
		},
	}, nil
}

// BodyToHTML converts a proposal body into the HTML Notes stores.
//
// Notes' `body` property is HTML. A proposal that already carries markup is
// passed through untouched (that is how an agent asks for a heading or a
// list); anything else is treated as plain text, escaped, and wrapped one
// <div> per line. Escaping matters: a plain-text body containing `<` would
// otherwise silently lose everything after it.
func BodyToHTML(body string) string {
	if markupRE.MatchString(body) {
		return body
	}
	var out strings.Builder
	for _, line := range strings.Split(body, "\n") {
		out.WriteString("<div>")
		out.WriteString(escapeHTML(line))
		out.WriteString("</div>")
	}
	return out.String()
}

// escapeHTML matches Python's html.escape(quote=True): & < > " and '.
func escapeHTML(value string) string {
	replacer := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&#x27;")
	return replacer.Replace(value)
}

// ResolveNoteReference turns whatever an agent supplied into the id Notes'
// AppleScript accepts.
//
// Two identifiers name the same note and they do not look alike. Notes'
// scripting `id` is x-coredata://<store-uuid>/ICNote/p<Z_PK>; the warehouse
// column an agent would naturally read, base_apple_notes.notes.note_id, is the
// store's ZIDENTIFIER UUID. Requiring the Core Data form would mean the one
// id a proposal can discover is the one the executor rejects, so a UUID is
// resolved through the local store instead.
func ResolveNoteReference(noteID string, lookup Lookup) (string, error) {
	reference := strings.TrimSpace(noteID)
	if strings.HasPrefix(reference, coreDataPrefix) {
		return reference, nil
	}
	if !uuidRE.MatchString(reference) {
		return "", fmt.Errorf("%w: note_id %q is neither an x-coredata:// id nor a base_apple_notes.notes.note_id UUID", ErrNoteNotFound, reference)
	}
	if lookup == nil {
		lookup = func(uuid string) (string, int64, bool, error) { return NotePrimaryKeyFromStore(uuid, "") }
	}
	storeUUID, primaryKey, ok, err := lookup(reference)
	if err != nil {
		if errors.Is(err, ErrStoreUnavailable) {
			return "", err
		}
		return "", fmt.Errorf("%w: %v", ErrStoreUnavailable, err)
	}
	if !ok {
		return "", fmt.Errorf("%w: no local Apple note has note_id %s; it may live on another Mac or have been deleted", ErrNoteNotFound, reference)
	}
	return fmt.Sprintf("%s%s/ICNote/p%d", coreDataPrefix, storeUUID, primaryKey), nil
}

// replaceLeadingHeading retitles a note by rewriting its first line, which is
// what Notes titles from.
func replaceLeadingHeading(bodyHTML string, name string) string {
	heading := "<div><h1>" + escapeHTML(name) + "</h1></div>"
	if loc := leadingDivRE.FindStringIndex(bodyHTML); loc != nil {
		return heading + bodyHTML[loc[1]:]
	}
	return heading + bodyHTML
}

func splitResult(raw string) (string, string) {
	id, name, _ := strings.Cut(raw, resultSeparator)
	return strings.TrimSpace(id), strings.TrimSpace(name)
}

func stringValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}
