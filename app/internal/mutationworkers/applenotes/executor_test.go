package applenotes

import (
	"errors"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/applescript"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

// fakeRunner is the Python _FakeRunner: canned results, recorded scripts.
type fakeRunner struct {
	results []string
	err     error
	scripts []string
}

func (f *fakeRunner) run(script string) (string, error) {
	f.scripts = append(f.scripts, script)
	if f.err != nil {
		return "", f.err
	}
	if len(f.results) == 0 {
		return "", errors.New("fake runner: no result left")
	}
	result := f.results[0]
	f.results = f.results[1:]
	return result, nil
}

func mutation(operation string, payload map[string]any) queue.Mutation {
	return queue.Mutation{ID: "mut-1", Provider: Provider, Operation: operation, Account: "you@example.com", Payload: payload}
}

func unreachableLookup(t *testing.T) Lookup {
	return func(string) (string, int64, bool, error) {
		t.Fatal("a core-data id must not trigger a store lookup")
		return "", 0, false, nil
	}
}

func TestBodyToHTMLWrapsPlainTextLinesInDivs(t *testing.T) {
	if got := BodyToHTML("one\ntwo"); got != "<div>one</div><div>two</div>" {
		t.Fatalf("got %q", got)
	}
}

func TestBodyToHTMLEscapesHTMLSpecialCharacters(t *testing.T) {
	if got := BodyToHTML("a < b & c"); got != "<div>a &lt; b &amp; c</div>" {
		t.Fatalf("got %q", got)
	}
	// html.escape(quote=True) also escapes both quote characters.
	if got := BodyToHTML(`it's "x"`); got != "<div>it&#x27;s &quot;x&quot;</div>" {
		t.Fatalf("got %q", got)
	}
}

func TestBodyToHTMLPassesExistingMarkupThrough(t *testing.T) {
	if got := BodyToHTML("<div><b>bold</b></div>"); got != "<div><b>bold</b></div>" {
		t.Fatalf("got %q", got)
	}
}

func TestCreateNoteReturnsTheNewNoteID(t *testing.T) {
	runner := &fakeRunner{results: []string{"x-coredata://ABC/ICNote/p9\ncreated title"}}
	executor := NewExecutor(runner.run, unreachableLookup(t))

	result := executor.Execute(mutation(CreateNoteOperation, map[string]any{
		"folder": "PDW Agent", "name": "Runway", "body": "12 months",
	}))

	if result.Status != queue.StatusSucceeded {
		t.Fatalf("status %q error %q", result.Status, result.Error)
	}
	if result.ResultJSON["note_id"] != "x-coredata://ABC/ICNote/p9" || result.ResultJSON["folder"] != "PDW Agent" ||
		result.ResultJSON["name"] != "created title" || result.ResultJSON["action"] != "create" {
		t.Fatalf("result %v", result.ResultJSON)
	}
	script := runner.scripts[0]
	for _, want := range []string{
		`set folderName to "PDW Agent"`,
		"make new folder",
		`with timeout of 120 seconds`,
		// The name is promoted into a leading heading, then the body follows.
		`with properties {body:"<div><h1>Runway</h1></div><div>12 months</div>"}`,
		`return (id of newNote) & "" & linefeed & "" & (name of newNote)`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script lacks %q:\n%s", want, script)
		}
	}
	if strings.Contains(script, "\n ") || strings.Contains(script, "\t") {
		t.Fatalf("script is not dedented:\n%s", script)
	}
}

func TestCreateNoteDefaultsTheFolderAndEscapesEveryValue(t *testing.T) {
	runner := &fakeRunner{results: []string{"x-coredata://ABC/ICNote/p1\nt"}}
	result := NewExecutor(runner.run, unreachableLookup(t)).Execute(mutation(CreateNoteOperation, map[string]any{
		"body": `say "hi"` + "\n" + `back\slash`,
	}))
	if result.Status != queue.StatusSucceeded || result.ResultJSON["folder"] != DefaultFolder {
		t.Fatalf("result %+v", result)
	}
	script := runner.scripts[0]
	if !strings.Contains(script, `set folderName to "PDW Agent"`) {
		t.Fatalf("default folder missing:\n%s", script)
	}
	// Quotes become HTML entities before they reach AppleScript, and the
	// backslash is escaped for the AppleScript literal; no raw quote survives.
	if !strings.Contains(script, `{body:"<div>say &quot;hi&quot;</div><div>back\\slash</div>"}`) {
		t.Fatalf("body not escaped:\n%s", script)
	}
}

func TestUpdateNoteRecordsThePreviousBodySoAReplacementIsRecoverable(t *testing.T) {
	runner := &fakeRunner{results: []string{"<div>old body</div>", "x-coredata://ABC/ICNote/p9\nNew title"}}
	executor := NewExecutor(runner.run, unreachableLookup(t))

	result := executor.Execute(mutation(UpdateNoteOperation, map[string]any{
		"note_id": "x-coredata://ABC/ICNote/p9", "body": "replacement",
	}))

	if result.Status != queue.StatusSucceeded {
		t.Fatalf("status %q error %q", result.Status, result.Error)
	}
	if result.ResultJSON["previous_body"] != "<div>old body</div>" || result.ResultJSON["note_id"] != "x-coredata://ABC/ICNote/p9" ||
		result.ResultJSON["changed"] != "body" || result.ResultJSON["action"] != "update" || result.ResultJSON["name"] != "New title" {
		t.Fatalf("result %v", result.ResultJSON)
	}
	if len(runner.scripts) != 2 {
		t.Fatalf("expected a read then a write, got %d scripts", len(runner.scripts))
	}
	if !strings.Contains(runner.scripts[0], `return body of note id "x-coredata://ABC/ICNote/p9"`) {
		t.Fatalf("read script:\n%s", runner.scripts[0])
	}
	write := runner.scripts[1]
	if !strings.Contains(write, `set targetNote to note id "x-coredata://ABC/ICNote/p9"`) ||
		!strings.Contains(write, `set body of targetNote to "<div>replacement</div>"`) {
		t.Fatalf("write script:\n%s", write)
	}
}

func TestAppendBodyKeepsTheExistingBodyAndAddsToIt(t *testing.T) {
	runner := &fakeRunner{results: []string{"<div>old</div>", "x-coredata://ABC/ICNote/p9\nTitle"}}
	executor := NewExecutor(runner.run, unreachableLookup(t))

	result := executor.Execute(mutation(UpdateNoteOperation, map[string]any{
		"note_id": "x-coredata://ABC/ICNote/p9", "append_body": "more",
	}))

	if result.Status != queue.StatusSucceeded || result.ResultJSON["changed"] != "append_body" {
		t.Fatalf("result %+v", result)
	}
	if !strings.Contains(runner.scripts[1], `set body of targetNote to "<div>old</div><div>more</div>"`) {
		t.Fatalf("write script:\n%s", runner.scripts[1])
	}
}

func TestAppendBodyWinsOverBodyWhenBothArePresent(t *testing.T) {
	// The proposal side rejects the pair; the executor's own precedence is
	// the safe one, keeping the old body.
	runner := &fakeRunner{results: []string{"<div>old</div>", "x-coredata://ABC/ICNote/p9\nTitle"}}
	result := NewExecutor(runner.run, unreachableLookup(t)).Execute(mutation(UpdateNoteOperation, map[string]any{
		"note_id": "x-coredata://ABC/ICNote/p9", "append_body": "more", "body": "replacement",
	}))
	if result.ResultJSON["changed"] != "append_body" || !strings.Contains(runner.scripts[1], "<div>old</div><div>more</div>") {
		t.Fatalf("result %+v script %s", result, runner.scripts[1])
	}
}

func TestRenamingRewritesTheFirstLineBecauseNotesTitlesFromIt(t *testing.T) {
	runner := &fakeRunner{results: []string{"<div><h1>Old title</h1></div><div>kept</div>", "x-coredata://ABC/ICNote/p9\nNew & improved"}}
	result := NewExecutor(runner.run, unreachableLookup(t)).Execute(mutation(UpdateNoteOperation, map[string]any{
		"note_id": "x-coredata://ABC/ICNote/p9", "name": "New & improved",
	}))
	if result.Status != queue.StatusSucceeded || result.ResultJSON["changed"] != "name" {
		t.Fatalf("result %+v", result)
	}
	if !strings.Contains(runner.scripts[1], `set body of targetNote to "<div><h1>New &amp; improved</h1></div><div>kept</div>"`) {
		t.Fatalf("write script:\n%s", runner.scripts[1])
	}

	// A body with no leading <div> gets the heading prepended.
	runner = &fakeRunner{results: []string{"plain", "x-coredata://ABC/ICNote/p9\nT"}}
	NewExecutor(runner.run, unreachableLookup(t)).Execute(mutation(UpdateNoteOperation, map[string]any{
		"note_id": "x-coredata://ABC/ICNote/p9", "name": "T", "body": "<p>x</p>",
	}))
	if !strings.Contains(runner.scripts[1], `to "<div><h1>T</h1></div><p>x</p>"`) {
		t.Fatalf("write script:\n%s", runner.scripts[1])
	}
}

func TestUpdateNoteFallsBackToTheRequestedIDWhenNotesReturnsNone(t *testing.T) {
	runner := &fakeRunner{results: []string{"<div>old</div>", ""}}
	result := NewExecutor(runner.run, unreachableLookup(t)).Execute(mutation(UpdateNoteOperation, map[string]any{
		"note_id": "x-coredata://ABC/ICNote/p9", "body": "x",
	}))
	if result.ResultJSON["note_id"] != "x-coredata://ABC/ICNote/p9" || result.ResultJSON["name"] != "" {
		t.Fatalf("result %v", result.ResultJSON)
	}
}

func TestUpdateNoteResolvesAWarehouseUUIDThroughTheStore(t *testing.T) {
	runner := &fakeRunner{results: []string{"<div>old</div>", "x-coredata://STORE/ICNote/p2230\nT"}}
	lookup := func(uuid string) (string, int64, bool, error) {
		if uuid != "0A1B2C3D-4E5F-6071-8293-A4B5C6D7E8F9" {
			t.Fatalf("lookup got %q", uuid)
		}
		return "1A2B3C4D-5E6F-7081-9203-A4B5C6D7E8F9", 2230, true, nil
	}
	result := NewExecutor(runner.run, lookup).Execute(mutation(UpdateNoteOperation, map[string]any{
		"note_id": "0A1B2C3D-4E5F-6071-8293-A4B5C6D7E8F9", "append_body": "x",
	}))
	if result.Status != queue.StatusSucceeded {
		t.Fatalf("result %+v", result)
	}
	if !strings.Contains(runner.scripts[0], `note id "x-coredata://1A2B3C4D-5E6F-7081-9203-A4B5C6D7E8F9/ICNote/p2230"`) {
		t.Fatalf("read script:\n%s", runner.scripts[0])
	}
}

func TestAMissingNoteIsTerminalNotRetryable(t *testing.T) {
	runner := &fakeRunner{err: errors.New(`Notes got an error: Can’t get note id "x". (-1728)`)}
	result := NewExecutor(runner.run, unreachableLookup(t)).Execute(mutation(UpdateNoteOperation, map[string]any{
		"note_id": "x-coredata://ABC/ICNote/gone", "append_body": "more",
	}))
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, "-1728") {
		t.Fatalf("result %+v", result)
	}
}

func TestAnAppleEventTimeoutIsRetryableBecauseNotesMayJustBeBusy(t *testing.T) {
	runner := &fakeRunner{err: errors.New("Notes got an error: AppleEvent timed out. (-1712)")}
	result := NewExecutor(runner.run, unreachableLookup(t)).Execute(mutation(CreateNoteOperation, map[string]any{
		"folder": "PDW Agent", "body": "hello",
	}))
	if result.Status != queue.StatusFailedRetryable {
		t.Fatalf("result %+v", result)
	}
}

func TestASubprocessTimeoutIsRetryableWithOurOwnMessage(t *testing.T) {
	runner := &fakeRunner{err: applescript.ErrTimeout}
	result := NewExecutor(runner.run, unreachableLookup(t)).Execute(mutation(CreateNoteOperation, map[string]any{"body": "hello"}))
	if result.Status != queue.StatusFailedRetryable || result.Error != "Notes.app did not answer within 180s" {
		t.Fatalf("result %+v", result)
	}
}

func TestADeniedAutomationGrantIsBlockedNotFailed(t *testing.T) {
	runner := &fakeRunner{err: errors.New("Not authorized to send Apple events to Notes. (-1743)")}
	result := NewExecutor(runner.run, unreachableLookup(t)).Execute(mutation(CreateNoteOperation, map[string]any{
		"folder": "PDW Agent", "body": "hello",
	}))
	if result.Status != queue.StatusBlockedMissingCredentials || !strings.Contains(result.Error, "Automation") || !strings.Contains(result.Error, "Notes.app") {
		t.Fatalf("result %+v", result)
	}
}

func TestAForeignProviderIsLeftForAnotherWorker(t *testing.T) {
	runner := &fakeRunner{}
	for _, m := range []queue.Mutation{
		{Provider: "gmail", Operation: "gmail.archive_threads"},
		{Provider: Provider, Operation: "apple_notes.delete_note"},
	} {
		result := NewExecutor(runner.run, unreachableLookup(t)).Execute(m)
		if result.Status != queue.StatusFailedRetryable || !strings.Contains(result.Error, "deferring") {
			t.Fatalf("%v: result %+v", m, result)
		}
	}
	if len(runner.scripts) != 0 {
		t.Fatal("a deferred row must not run any script")
	}
}

func TestACoreDataNoteIDIsPassedThroughUntouched(t *testing.T) {
	reference := "x-coredata://STORE/ICNote/p12"
	got, err := ResolveNoteReference(reference, unreachableLookup(t))
	if err != nil || got != reference {
		t.Fatalf("got %q err %v", got, err)
	}
}

func TestAWarehouseUUIDIsResolvedToTheCoreDataIDAppleScriptNeeds(t *testing.T) {
	lookup := func(uuid string) (string, int64, bool, error) {
		if uuid != "0A1B2C3D-4E5F-6071-8293-A4B5C6D7E8F9" {
			t.Fatalf("lookup got %q", uuid)
		}
		return "1A2B3C4D-5E6F-7081-9203-A4B5C6D7E8F9", 2230, true, nil
	}
	got, err := ResolveNoteReference("0A1B2C3D-4E5F-6071-8293-A4B5C6D7E8F9", lookup)
	if err != nil || got != "x-coredata://1A2B3C4D-5E6F-7081-9203-A4B5C6D7E8F9/ICNote/p2230" {
		t.Fatalf("got %q err %v", got, err)
	}
}

func TestAnUnresolvableNoteReferenceFailsLoudly(t *testing.T) {
	missing := func(string) (string, int64, bool, error) { return "", 0, false, nil }
	if _, err := ResolveNoteReference("00000000-0000-0000-0000-000000000000", missing); !errors.Is(err, ErrNoteNotFound) {
		t.Fatalf("err %v", err)
	}
	if _, err := ResolveNoteReference("not-an-id", unreachableLookup(t)); !errors.Is(err, ErrNoteNotFound) {
		t.Fatalf("err %v", err)
	}
	// Through the executor both are terminal and touch no script.
	runner := &fakeRunner{}
	result := NewExecutor(runner.run, missing).Execute(mutation(UpdateNoteOperation, map[string]any{
		"note_id": "00000000-0000-0000-0000-000000000000", "body": "x",
	}))
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, "no local Apple note") || len(runner.scripts) != 0 {
		t.Fatalf("result %+v scripts %d", result, len(runner.scripts))
	}
}

func TestAStoreThatCannotBeReadIsRetryable(t *testing.T) {
	broken := func(string) (string, int64, bool, error) { return "", 0, false, errors.New("operation not permitted") }
	result := NewExecutor((&fakeRunner{}).run, broken).Execute(mutation(UpdateNoteOperation, map[string]any{
		"note_id": "00000000-0000-0000-0000-000000000000", "body": "x",
	}))
	if result.Status != queue.StatusFailedRetryable {
		t.Fatalf("result %+v", result)
	}
	_, _, _, err := NotePrimaryKeyFromStore("00000000-0000-0000-0000-000000000000", t.TempDir()+"/missing.sqlite")
	if !errors.Is(err, ErrStoreUnavailable) {
		t.Fatalf("err %v", err)
	}
	result = NewExecutor((&fakeRunner{}).run, nil).Execute(mutation(UpdateNoteOperation, map[string]any{
		"note_id": "00000000-0000-0000-0000-000000000000", "body": "x",
	}))
	if result.Status != queue.StatusFailedRetryable {
		t.Fatalf("result %+v", result)
	}
}

func TestSpecIsTheAppleNotesWorker(t *testing.T) {
	if Spec.Provider != "apple_notes" || Spec.WorkerName != "apple_notes_mutation_worker" || Spec.LockID != 7_403_111_851 || Spec.EnvPrefix != "APPLE_NOTES" {
		t.Fatalf("spec %+v", Spec)
	}
	// A create is NOT idempotent: replaying it makes a second note, so only
	// the update path is reclaimed.
	if len(Spec.Idempotent) != 1 || Spec.Idempotent[0] != (queue.Operation{Provider: Provider, Operation: UpdateNoteOperation}) {
		t.Fatalf("idempotent %v", Spec.Idempotent)
	}
	if !Enabled(func(string) string { return "" }) || Enabled(func(string) string { return "0" }) {
		t.Fatal("kill switch")
	}
}
