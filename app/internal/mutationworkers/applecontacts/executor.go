// Package applecontacts applies approved apple_contacts.* mutations through
// Contacts.app, the Go twin of personal_data_warehouse.apple_contacts_mutations
// and personal_data_warehouse_apple_contacts.mutation_worker.
//
// iCloud Contacts has no public write API, so, exactly like Apple Notes, the
// only supported way to change a card is to ask Contacts.app itself on a Mac
// that is signed in. The proposal and review halves live with every other
// mutation type (app/internal/mutations/apple_contacts.go); this executor is
// what the local apple-contacts worker runs after a human has approved the
// row.
//
// Two properties make Contacts easier than Notes: the AppleScript id of a
// person is the same <UUID>:ABPerson string the uploader stores in
// base_apple_contacts.cards.card_id, so an agent addresses the card it found;
// and every field is a real property rather than HTML.
package applecontacts

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/applescript"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

const (
	Provider               = "apple_contacts"
	CreateContactOperation = "apple_contacts.create_contact"
	UpdateContactOperation = "apple_contacts.update_contact"
	MergeContactsOperation = "apple_contacts.merge_contacts"

	// LockID is distinct from the cloud worker's and the Notes worker's.
	LockID int64 = 7_403_111_853

	// The read script separates fields with the ASCII unit separator and
	// records with the record separator, because a note can contain any
	// printable character including newlines and pipes.
	FieldSeparator  = "\x1f"
	RecordSeparator = "\x1e"

	missingValue = "missing value"
)

// Field maps a payload key to the Contacts AppleScript property or element.
type Field struct {
	Key      string
	Property string
}

// ScalarFields are the scalar contact fields, in the order the read dump
// emits them, mapped to the AppleScript property holding each one.
var ScalarFields = []Field{
	{"given_name", "first name"},
	{"family_name", "last name"},
	{"middle_name", "middle name"},
	{"nickname", "nickname"},
	{"organization", "organization"},
	{"job_title", "job title"},
	{"department", "department"},
}

// ListFields are the list fields, mapped to the AppleScript element class.
var ListFields = []Field{{"emails", "email"}, {"phones", "phone"}, {"urls", "url"}}

var (
	labelWrapperRE = regexp.MustCompile(`^_\$!<(.*)>!\$_$`)
	nonDigitRE     = regexp.MustCompile(`\D`)
)

// ErrCardNotFound and ErrInvalidMutation are terminal.
var (
	ErrCardNotFound    = errors.New("apple contacts card not found")
	ErrInvalidMutation = errors.New("invalid apple contacts mutation")
)

// Entry is one labelled list value (an email, phone or url).
type Entry struct {
	Label string
	Value string
}

// Card is a parsed read of one Contacts person.
type Card struct {
	CardID  string
	Scalars map[string]string
	Note    string
	Lists   map[string][]Entry
}

// Map renders the card the way the Python executor recorded it in
// result_json.previous_card(s).
func (c Card) Map() map[string]any {
	out := map[string]any{"card_id": c.CardID, "note": c.Note}
	for _, field := range ScalarFields {
		out[field.Key] = c.Scalars[field.Key]
	}
	for _, field := range ListFields {
		entries := make([]map[string]any, 0, len(c.Lists[field.Key]))
		for _, entry := range c.Lists[field.Key] {
			entries = append(entries, map[string]any{"label": entry.Label, "value": entry.Value})
		}
		out[field.Key] = entries
	}
	return out
}

// NormalizeLabel: `_$!<Work>!$_` is how Contacts spells its built-in labels;
// `work` is ours.
func NormalizeLabel(raw string) string {
	text := strings.TrimSpace(raw)
	if text == missingValue {
		return ""
	}
	if m := labelWrapperRE.FindStringSubmatch(text); m != nil {
		return strings.ToLower(strings.TrimSpace(m[1]))
	}
	return text
}

func scalar(value string) string {
	if value == missingValue {
		return ""
	}
	return value
}

// ParseCardDump parses the read script's output.
func ParseCardDump(raw string) (Card, error) {
	var records []string
	for _, r := range strings.Split(raw, RecordSeparator) {
		if r != "" {
			records = append(records, r)
		}
	}
	if len(records) == 0 || !strings.HasPrefix(records[0], "card"+FieldSeparator) {
		return Card{}, fmt.Errorf("%w: Contacts.app returned no card: %q", ErrCardNotFound, raw)
	}
	head := strings.Split(records[0], FieldSeparator)
	card := Card{Scalars: map[string]string{}, Lists: map[string][]Entry{}}
	if len(head) > 1 {
		card.CardID = head[1]
	}
	// card, id, then the scalar fields in ScalarFields order, then the note.
	for index, field := range ScalarFields {
		if len(head) > 2+index {
			card.Scalars[field.Key] = scalar(head[2+index])
		} else {
			card.Scalars[field.Key] = ""
		}
	}
	if len(head) > 2+len(ScalarFields) {
		card.Note = scalar(head[2+len(ScalarFields)])
	}
	kinds := map[string]string{}
	for _, field := range ListFields {
		card.Lists[field.Key] = []Entry{}
		kinds[field.Property] = field.Key
	}
	for _, record := range records[1:] {
		parts := strings.Split(record, FieldSeparator)
		if len(parts) < 3 {
			continue
		}
		key, ok := kinds[parts[0]]
		if !ok {
			continue
		}
		card.Lists[key] = append(card.Lists[key], Entry{Label: NormalizeLabel(parts[1]), Value: scalar(parts[2])})
	}
	return card, nil
}

func valueKey(kind string, value string) string {
	text := strings.TrimSpace(value)
	switch kind {
	case "emails":
		return strings.ToLower(text)
	case "phones":
		digits := nonDigitRE.ReplaceAllString(text, "")
		if len(digits) >= 10 {
			return digits[len(digits)-10:]
		}
		return digits
	}
	return strings.ToLower(strings.TrimRight(text, "/"))
}

// entries reads a payload list field: maps with label/value, or bare strings.
func entries(value any) []Entry {
	var out []Entry
	list, ok := value.([]any)
	if !ok {
		return out
	}
	for _, item := range list {
		switch v := item.(type) {
		case map[string]any:
			text := strings.TrimSpace(stringValue(v["value"]))
			if text != "" {
				out = append(out, Entry{Label: strings.TrimSpace(stringValue(v["label"])), Value: text})
			}
		case string:
			if strings.TrimSpace(v) != "" {
				out = append(out, Entry{Value: strings.TrimSpace(v)})
			}
		}
	}
	return out
}

func stringList(value any) []string {
	var out []string
	list, ok := value.([]any)
	if !ok {
		return out
	}
	for _, item := range list {
		text := stringValue(item)
		if strings.TrimSpace(text) != "" {
			out = append(out, text)
		}
	}
	return out
}

func readScript(cardID string) string {
	scalarReads := make([]string, 0, len(ScalarFields))
	for _, field := range ScalarFields {
		scalarReads = append(scalarReads, fmt.Sprintf("my txt(%s of p)", field.Property))
	}
	listReads := make([]string, 0, len(ListFields))
	for _, field := range ListFields {
		listReads = append(listReads, fmt.Sprintf(`repeat with e in %s of p
		set out to out & rs & "%s" & us & my txt(label of e) & us & my txt(value of e)
		end repeat`, field.Key, field.Property))
	}
	return fmt.Sprintf(`
	on txt(v)
	  if v is missing value then return ""
	  return v as text
	end txt
	with timeout of %d seconds
	  tell application "Contacts"
	    set us to (ASCII character 31)
	    set rs to (ASCII character 30)
	    set p to person id %s
	    set out to "card" & us & (id of p) & us & %s & us & my txt(note of p)
	    %s
	    return out
	  end tell
	end timeout
	`, applescript.InScriptTimeoutSeconds, applescript.String(cardID), strings.Join(scalarReads, " & us & "), strings.Join(listReads, "\n"))
}

// changes is the "contact" half of a payload, or the merge union: what to
// set and add on a card.
type changes struct {
	Scalars    map[string]string
	Note       string
	AppendNote string
	Lists      map[string][]Entry
}

func changesFromPayload(contact map[string]any) changes {
	out := changes{Scalars: map[string]string{}, Lists: map[string][]Entry{}}
	for _, field := range ScalarFields {
		out.Scalars[field.Key] = stringValue(contact[field.Key])
	}
	out.Note = stringValue(contact["note"])
	out.AppendNote = stringValue(contact["append_note"])
	for _, field := range ListFields {
		out.Lists[field.Key] = entries(contact[field.Key])
	}
	return out
}

// Executor executes apple_contacts.* mutations claimed from
// ops.upstream_mutation_operations.
type Executor struct {
	runner     applescript.Runner
	mergedInto MergedInto
}

// MergedInto names the card a deleted card was merged into ("" when nobody
// merged it).
//
// Two requests proposed from one warehouse snapshot can name the same card: one
// merges it away, the other updates it. The merge runs first and Contacts.app
// then answers -1728 for the update, which used to die failed_terminal although
// the surviving card is exactly where the values belong. The resolver is backed
// by the mutation ledger's own succeeded merge_contacts rows
// (queue.PostgresStore.AppleContactsMergedCardTarget); without one, a missing
// card is simply missing.
type MergedInto func(cardID string) (string, error)

const (
	// CardNotFoundErrorCode is Contacts.app's "Can't get person id": the record
	// is gone, and it will stay gone.
	CardNotFoundErrorCode = "-1728"
	// MaxMergeChain bounds following merge chains (A into B, B into C, ...).
	MaxMergeChain = 8
)

// errLedgerUnavailable marks a resolver failure: "we could not ask" must not
// retire a mutation the way "nobody merged it" does.
var errLedgerUnavailable = errors.New("mutation ledger unavailable")

func isCardNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), CardNotFoundErrorCode)
}

// WithMergedInto installs the merged-card resolver and returns the executor.
func (e *Executor) WithMergedInto(resolver MergedInto) *Executor {
	e.mergedInto = resolver
	return e
}

// survivingCard follows succeeded merges from cardID to the card that still
// exists; "" when nobody merged it.
func (e *Executor) survivingCard(cardID string) (string, error) {
	if e.mergedInto == nil {
		return "", nil
	}
	seen := map[string]bool{cardID: true}
	current, target := cardID, ""
	for i := 0; i < MaxMergeChain; i++ {
		next, err := e.mergedInto(current)
		if err != nil {
			return "", fmt.Errorf("%w: %v", errLedgerUnavailable, err)
		}
		next = strings.TrimSpace(next)
		if next == "" || seen[next] {
			break
		}
		seen[next] = true
		target, current = next, next
	}
	return target, nil
}

// NewExecutor builds an executor; a nil runner uses osascript.
func NewExecutor(runner applescript.Runner) *Executor {
	if runner == nil {
		runner = applescript.Run
	}
	return &Executor{runner: runner}
}

// Execute implements queue.Executor.
func (e *Executor) Execute(mutation queue.Mutation) queue.Result {
	if mutation.Provider != Provider {
		return deferred(mutation)
	}
	switch mutation.Operation {
	case CreateContactOperation, UpdateContactOperation, MergeContactsOperation:
	default:
		return deferred(mutation)
	}
	payload := mutation.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	var result queue.Result
	var err error
	switch mutation.Operation {
	case CreateContactOperation:
		result, err = e.create(payload)
	case UpdateContactOperation:
		result, err = e.update(payload)
	default:
		result, err = e.merge(payload)
	}
	if err == nil {
		return result
	}
	if errors.Is(err, applescript.ErrTimeout) {
		return queue.Result{
			Status: queue.StatusFailedRetryable,
			Error:  fmt.Sprintf("Contacts.app did not answer within %ds", int(applescript.DefaultScriptTimeout.Seconds())),
		}
	}
	if errors.Is(err, errLedgerUnavailable) {
		return queue.Result{Status: queue.StatusFailedRetryable, Error: err.Error()}
	}
	if errors.Is(err, ErrCardNotFound) || errors.Is(err, ErrInvalidMutation) {
		return queue.Result{Status: queue.StatusFailedTerminal, Error: err.Error()}
	}
	status, message := applescript.Classify(err.Error(), "Contacts.app")
	return queue.Result{Status: status, Error: message}
}

func deferred(mutation queue.Mutation) queue.Result {
	// Never burn an unrecognized row to failed_terminal: a newer worker may
	// understand it. This mirrors the cloud worker's unknown-provider handling.
	return queue.Result{
		Status: queue.StatusFailedRetryable,
		Error:  fmt.Sprintf("unsupported mutation operation %s.%s; deferring", mutation.Provider, mutation.Operation),
	}
}

// -- create ------------------------------------------------------------------

func (e *Executor) create(payload map[string]any) (queue.Result, error) {
	contact := mapValue(payload["contact"])
	var properties []string
	for _, field := range ScalarFields {
		value := strings.TrimSpace(stringValue(contact[field.Key]))
		if value != "" {
			properties = append(properties, fmt.Sprintf("%s:%s", field.Property, applescript.String(value)))
		}
	}
	if len(properties) == 0 {
		return queue.Result{}, fmt.Errorf("%w: create_contact needs at least a name or an organization", ErrInvalidMutation)
	}
	lines := []string{fmt.Sprintf("set newPerson to make new person with properties {%s}", strings.Join(properties, ", "))}
	// Setting organization inside the make-properties record is not reliable
	// on every macOS release; set it explicitly as well.
	for _, field := range ScalarFields {
		value := strings.TrimSpace(stringValue(contact[field.Key]))
		if value != "" && (field.Key == "organization" || field.Key == "job_title" || field.Key == "department") {
			lines = append(lines, fmt.Sprintf("set %s of newPerson to %s", field.Property, applescript.String(value)))
		}
	}
	note := stringValue(contact["note"])
	if strings.TrimSpace(note) != "" {
		lines = append(lines, fmt.Sprintf("set note of newPerson to %s", applescript.String(note)))
	}
	for _, field := range ListFields {
		seen := map[string]bool{}
		for _, entry := range entries(contact[field.Key]) {
			dedupe := valueKey(field.Key, entry.Value)
			if seen[dedupe] {
				continue
			}
			seen[dedupe] = true
			lines = append(lines, makeLine("newPerson", field, entry))
		}
	}
	script := fmt.Sprintf(`
	with timeout of %d seconds
	tell application "Contacts"
	%s
	save
	return (id of newPerson) & (ASCII character 31) & (name of newPerson)
	end tell
	end timeout
	`, applescript.InScriptTimeoutSeconds, strings.Join(lines, "\n"))
	raw, err := e.runner(applescript.Dedent(script))
	if err != nil {
		return queue.Result{}, err
	}
	cardID, name := splitResult(raw)
	return queue.Result{
		Status:     queue.StatusSucceeded,
		ResultJSON: map[string]any{"card_id": cardID, "name": name, "action": "create"},
	}, nil
}

// -- update ------------------------------------------------------------------

func (e *Executor) update(payload map[string]any) (queue.Result, error) {
	cardID := strings.TrimSpace(stringValue(payload["card_id"]))
	if cardID == "" {
		return queue.Result{}, fmt.Errorf("%w: update_contact needs card_id", ErrInvalidMutation)
	}
	contact := changesFromPayload(mapValue(payload["contact"]))
	remove := mapValue(payload["remove"])
	redirectedFrom := ""
	current, err := e.read(cardID)
	if err != nil {
		if !isCardNotFound(err) {
			return queue.Result{}, err
		}
		survivor, ledgerErr := e.survivingCard(cardID)
		if ledgerErr != nil {
			return queue.Result{}, ledgerErr
		}
		if survivor == "" {
			return queue.Result{}, err
		}
		redirectedFrom, cardID = cardID, survivor
		if current, err = e.read(cardID); err != nil {
			return queue.Result{}, err
		}
	}
	withProvenance := func(result map[string]any) map[string]any {
		if redirectedFrom != "" {
			result["redirected_from"] = redirectedFrom
		}
		return result
	}
	lines, added, removed, noteChanged := applyChanges("p", current, contact, remove)
	if len(lines) == 0 {
		return queue.Result{
			Status: queue.StatusSucceeded,
			ResultJSON: withProvenance(map[string]any{
				"card_id":       cardID,
				"action":        "update",
				"changed":       false,
				"previous_card": current.Map(),
			}),
		}, nil
	}
	cardIDOut, name, err := e.write(cardID, lines)
	if err != nil {
		return queue.Result{}, err
	}
	if cardIDOut == "" {
		cardIDOut = cardID
	}
	return queue.Result{
		Status: queue.StatusSucceeded,
		ResultJSON: withProvenance(map[string]any{
			"card_id":       cardIDOut,
			"name":          name,
			"action":        "update",
			"changed":       true,
			"added":         added,
			"removed":       removed,
			"note_changed":  noteChanged,
			"previous_card": current.Map(),
		}),
	}, nil
}

// -- merge -------------------------------------------------------------------

func (e *Executor) merge(payload map[string]any) (queue.Result, error) {
	keepID := strings.TrimSpace(stringValue(payload["keep_card_id"]))
	var mergeIDs []string
	for _, id := range stringList(payload["merge_card_ids"]) {
		if trimmed := strings.TrimSpace(id); trimmed != "" {
			mergeIDs = append(mergeIDs, trimmed)
		}
	}
	if keepID == "" || len(mergeIDs) == 0 {
		return queue.Result{}, fmt.Errorf("%w: merge_contacts needs keep_card_id and at least one merge_card_ids entry", ErrInvalidMutation)
	}
	for _, id := range mergeIDs {
		if id == keepID {
			return queue.Result{}, fmt.Errorf("%w: merge_card_ids must not contain keep_card_id", ErrInvalidMutation)
		}
	}
	overrides := mapValue(payload["contact"])

	keep, err := e.read(keepID)
	if err != nil {
		return queue.Result{}, err
	}
	// A card an earlier merge already folded into keepID is done, not an error;
	// one folded into some OTHER card is a conflict a human has to look at.
	others := make([]Card, 0, len(mergeIDs))
	toDelete := []string{}
	alreadyMerged := []string{}
	for _, other := range mergeIDs {
		card, err := e.read(other)
		if err != nil {
			if !isCardNotFound(err) {
				return queue.Result{}, err
			}
			survivor, ledgerErr := e.survivingCard(other)
			if ledgerErr != nil {
				return queue.Result{}, ledgerErr
			}
			if survivor == keepID {
				alreadyMerged = append(alreadyMerged, other)
				continue
			}
			if survivor != "" {
				return queue.Result{}, fmt.Errorf(
					"%w: card %s was already merged into %s, not into %s; Contacts.app said: %v",
					ErrInvalidMutation, other, survivor, keepID, err)
			}
			return queue.Result{}, err
		}
		others = append(others, card)
		toDelete = append(toDelete, other)
	}

	// The union: every list value the kept card lacks, every scalar it has
	// empty.
	union := changes{Scalars: map[string]string{}, Lists: map[string][]Entry{}}
	for _, field := range ListFields {
		union.Lists[field.Key] = []Entry{}
	}
	for _, other := range others {
		for _, field := range ScalarFields {
			if strings.TrimSpace(keep.Scalars[field.Key]) == "" &&
				strings.TrimSpace(other.Scalars[field.Key]) != "" &&
				strings.TrimSpace(stringValue(overrides[field.Key])) == "" {
				union.Scalars[field.Key] = other.Scalars[field.Key]
			}
		}
		for _, field := range ListFields {
			union.Lists[field.Key] = append(union.Lists[field.Key], other.Lists[field.Key]...)
		}
		otherNote := strings.TrimSpace(other.Note)
		if otherNote != "" && !strings.Contains(keep.Note, otherNote) {
			union.AppendNote = strings.Trim(union.AppendNote+"\n"+otherNote, "\n")
		}
	}
	for key, value := range overrides {
		switch key {
		case "emails", "phones", "urls":
			union.Lists[key] = append(union.Lists[key], entries(value)...)
		case "note":
			if text := stringValue(value); text != "" {
				union.Note = text
			}
		case "append_note":
			if text := stringValue(value); text != "" {
				union.AppendNote = text
			}
		default:
			if text := stringValue(value); text != "" {
				union.Scalars[key] = text
			}
		}
	}

	lines, added, _, noteChanged := applyChanges("p", keep, union, nil)
	for _, other := range toDelete {
		lines = append(lines, fmt.Sprintf("delete person id %s", applescript.String(other)))
	}
	cardIDOut, name, err := e.write(keepID, lines)
	if err != nil {
		return queue.Result{}, err
	}
	if cardIDOut == "" {
		cardIDOut = keepID
	}
	previous := []map[string]any{keep.Map()}
	for _, other := range others {
		previous = append(previous, other.Map())
	}
	return queue.Result{
		Status: queue.StatusSucceeded,
		ResultJSON: map[string]any{
			"card_id":                 cardIDOut,
			"name":                    name,
			"action":                  "merge",
			"added":                   added,
			"note_changed":            noteChanged,
			"deleted_card_ids":        toDelete,
			"already_merged_card_ids": alreadyMerged,
			"previous_cards":          previous,
		},
	}, nil
}

// -- plumbing ----------------------------------------------------------------

func (e *Executor) read(cardID string) (Card, error) {
	raw, err := e.runner(applescript.Dedent(readScript(cardID)))
	if err != nil {
		return Card{}, err
	}
	return ParseCardDump(raw)
}

func (e *Executor) write(cardID string, lines []string) (string, string, error) {
	script := fmt.Sprintf(`
	with timeout of %d seconds
	tell application "Contacts"
	set p to person id %s
	%s
	save
	return (id of p) & (ASCII character 31) & (name of p)
	end tell
	end timeout
	`, applescript.InScriptTimeoutSeconds, applescript.String(cardID), strings.Join(lines, "\n"))
	raw, err := e.runner(applescript.Dedent(script))
	if err != nil {
		return "", "", err
	}
	id, name := splitResult(raw)
	return id, name, nil
}

func makeLine(target string, field Field, entry Entry) string {
	var props []string
	if label := strings.TrimSpace(entry.Label); label != "" {
		props = append(props, fmt.Sprintf("label:%s", applescript.String(label)))
	}
	props = append(props, fmt.Sprintf("value:%s", applescript.String(entry.Value)))
	return fmt.Sprintf("make new %s at end of %s of %s with properties {%s}", field.Property, field.Key, target, strings.Join(props, ", "))
}

// applyChanges computes the script lines that take current to current+contact
// minus remove, and reports what was added, removed, and whether the note
// changed. Scalars are SET when non-empty and different; list values are
// ADDED when the card lacks them (after removals); `note` replaces while
// `append_note` appends.
func applyChanges(target string, current Card, contact changes, remove map[string]any) ([]string, map[string][]string, map[string][]string, bool) {
	var lines []string
	for _, field := range ScalarFields {
		value := strings.TrimSpace(contact.Scalars[field.Key])
		if value != "" && value != strings.TrimSpace(current.Scalars[field.Key]) {
			lines = append(lines, fmt.Sprintf("set %s of %s to %s", field.Property, target, applescript.String(value)))
		}
	}

	noteChanged := false
	if contact.Note != "" && contact.Note != current.Note {
		lines = append(lines, fmt.Sprintf("set note of %s to %s", target, applescript.String(contact.Note)))
		noteChanged = true
	} else if strings.TrimSpace(contact.AppendNote) != "" {
		combined := contact.AppendNote
		if current.Note != "" {
			combined = current.Note + "\n" + contact.AppendNote
		}
		lines = append(lines, fmt.Sprintf("set note of %s to %s", target, applescript.String(combined)))
		noteChanged = true
	}

	removed := map[string][]string{}
	for _, field := range ListFields {
		removed[field.Key] = []string{}
		wanted := map[string]bool{}
		if remove != nil {
			for _, v := range stringList(remove[field.Key]) {
				wanted[valueKey(field.Key, v)] = true
			}
		}
		for _, entry := range current.Lists[field.Key] {
			if wanted[valueKey(field.Key, entry.Value)] {
				lines = append(lines, fmt.Sprintf("delete (every %s of %s whose value is %s)", field.Property, target, applescript.String(entry.Value)))
				removed[field.Key] = append(removed[field.Key], entry.Value)
			}
		}
	}

	added := map[string][]string{}
	for _, field := range ListFields {
		added[field.Key] = []string{}
		have := map[string]bool{}
		for _, entry := range current.Lists[field.Key] {
			have[valueKey(field.Key, entry.Value)] = true
		}
		for _, v := range removed[field.Key] {
			delete(have, valueKey(field.Key, v))
		}
		for _, entry := range contact.Lists[field.Key] {
			dedupe := valueKey(field.Key, entry.Value)
			if have[dedupe] {
				continue
			}
			have[dedupe] = true
			lines = append(lines, makeLine(target, field, entry))
			added[field.Key] = append(added[field.Key], entry.Value)
		}
	}
	return lines, added, removed, noteChanged
}

func splitResult(raw string) (string, string) {
	id, name, _ := strings.Cut(raw, FieldSeparator)
	return strings.TrimSpace(id), strings.TrimSpace(name)
}

func mapValue(value any) map[string]any {
	if m, ok := value.(map[string]any); ok {
		return m
	}
	return map[string]any{}
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
