package applecontacts

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/applescript"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

const (
	keep  = "8537DF38-BF0D-4468-9061-D2D41468E05A:ABPerson"
	other = "44C1F82A-0000-4468-9061-D2D41468E05A:ABPerson"
)

type pair struct{ label, value string }

// dump is the Python test helper of the same name: a card read as the read
// script emits it.
func dump(cardID, first, last, org, title, note string, emails, phones, urls []pair) string {
	head := strings.Join([]string{"card", cardID, first, last, "", "", org, title, "", note}, FieldSeparator)
	records := []string{head}
	for _, e := range emails {
		records = append(records, strings.Join([]string{"email", e.label, e.value}, FieldSeparator))
	}
	for _, p := range phones {
		records = append(records, strings.Join([]string{"phone", p.label, p.value}, FieldSeparator))
	}
	for _, u := range urls {
		records = append(records, strings.Join([]string{"url", u.label, u.value}, FieldSeparator))
	}
	return strings.Join(records, RecordSeparator)
}

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

func entryList(entries ...pair) []any {
	out := make([]any, 0, len(entries))
	for _, e := range entries {
		out = append(out, map[string]any{"label": e.label, "value": e.value})
	}
	return out
}

func TestNormalizeLabelStripsTheContactsWrapper(t *testing.T) {
	for raw, want := range map[string]string{"_$!<Work>!$_": "work", "_$!<Mobile>!$_": "mobile", "Outlook": "Outlook", "missing value": ""} {
		if got := NormalizeLabel(raw); got != want {
			t.Fatalf("%q: got %q want %q", raw, got, want)
		}
	}
}

func TestParseCardDumpReadsScalarsAndLists(t *testing.T) {
	card, err := ParseCardDump(dump(keep, "Melanie", "Smith", "Hack Club", "", "hi",
		[]pair{{"_$!<Home>!$_", "melanie@hackclub.com"}}, []pair{{"_$!<Other>!$_", "(413) 552-8582"}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if card.CardID != keep || card.Scalars["given_name"] != "Melanie" || card.Scalars["organization"] != "Hack Club" || card.Note != "hi" {
		t.Fatalf("card %+v", card)
	}
	if !reflect.DeepEqual(card.Lists["emails"], []Entry{{"home", "melanie@hackclub.com"}}) ||
		!reflect.DeepEqual(card.Lists["phones"], []Entry{{"other", "(413) 552-8582"}}) ||
		len(card.Lists["urls"]) != 0 {
		t.Fatalf("lists %+v", card.Lists)
	}
	// The result_json shape the reviewer reads back.
	m := card.Map()
	if m["card_id"] != keep || m["given_name"] != "Melanie" || !reflect.DeepEqual(m["urls"], []map[string]any{}) ||
		!reflect.DeepEqual(m["emails"], []map[string]any{{"label": "home", "value": "melanie@hackclub.com"}}) {
		t.Fatalf("map %v", m)
	}
	if _, err := ParseCardDump(""); !errors.Is(err, ErrCardNotFound) {
		t.Fatalf("empty dump err %v", err)
	}
	// "missing value" scalars read as empty.
	card, _ = ParseCardDump(strings.Join([]string{"card", keep, "missing value", "L"}, FieldSeparator))
	if card.Scalars["given_name"] != "" || card.Scalars["family_name"] != "L" || card.Note != "" {
		t.Fatalf("card %+v", card)
	}
}

func TestCreateContactBuildsTheCardAndReturnsItsID(t *testing.T) {
	runner := &fakeRunner{results: []string{"NEW-ID:ABPerson" + FieldSeparator + "Rebeka Lawrence-Gomez"}}
	result := NewExecutor(runner.run).Execute(mutation(CreateContactOperation, map[string]any{
		"contact": map[string]any{
			"given_name": "Rebeka", "family_name": "Lawrence-Gomez", "organization": "Hack Club",
			"job_title": "Deputy to the Founder",
			"emails":    entryList(pair{"work", "rebeka@hackclub.com"}),
			"phones":    entryList(pair{"mobile", "+18027528709"}),
		},
	}))
	if result.Status != queue.StatusSucceeded || result.ResultJSON["card_id"] != "NEW-ID:ABPerson" || result.ResultJSON["action"] != "create" || result.ResultJSON["name"] != "Rebeka Lawrence-Gomez" {
		t.Fatalf("result %+v", result)
	}
	script := runner.scripts[0]
	for _, want := range []string{
		`make new person with properties {first name:"Rebeka", last name:"Lawrence-Gomez", organization:"Hack Club", job title:"Deputy to the Founder"}`,
		`make new email at end of emails of newPerson with properties {label:"work", value:"rebeka@hackclub.com"}`,
		`make new phone at end of phones of newPerson with properties {label:"mobile", value:"+18027528709"}`,
		`set organization of newPerson to "Hack Club"`,
		`set job title of newPerson to "Deputy to the Founder"`,
		"return (id of newPerson) & (ASCII character 31) & (name of newPerson)",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script lacks %q:\n%s", want, script)
		}
	}
	if strings.Count(script, "\nsave\n") != 1 {
		t.Fatalf("expected exactly one save:\n%s", script)
	}
}

func TestCreateContactEscapesQuotesInEveryValue(t *testing.T) {
	runner := &fakeRunner{results: []string{"X:ABPerson" + FieldSeparator + "n"}}
	NewExecutor(runner.run).Execute(mutation(CreateContactOperation, map[string]any{
		"contact": map[string]any{"given_name": `Ann "Quotes"`, "organization": "O", "note": `say "hi"`},
	}))
	if !strings.Contains(runner.scripts[0], `first name:"Ann \"Quotes\""`) || !strings.Contains(runner.scripts[0], `set note of newPerson to "say \"hi\""`) {
		t.Fatalf("script:\n%s", runner.scripts[0])
	}
}

func TestCreateContactDedupesListValuesAndNeedsAName(t *testing.T) {
	runner := &fakeRunner{results: []string{"X:ABPerson" + FieldSeparator + "n"}}
	NewExecutor(runner.run).Execute(mutation(CreateContactOperation, map[string]any{
		"contact": map[string]any{"organization": "O", "emails": []any{"A@x.com", map[string]any{"label": "work", "value": "a@x.com"}, ""}},
	}))
	if strings.Count(runner.scripts[0], "make new email") != 1 || !strings.Contains(runner.scripts[0], `value:"A@x.com"`) {
		t.Fatalf("script:\n%s", runner.scripts[0])
	}
	runner = &fakeRunner{}
	result := NewExecutor(runner.run).Execute(mutation(CreateContactOperation, map[string]any{"contact": map[string]any{"note": "only"}}))
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, "at least a name") || len(runner.scripts) != 0 {
		t.Fatalf("result %+v", result)
	}
}

func TestUpdateContactAddsOnlyValuesTheCardLacksAndRecordsThePreviousCard(t *testing.T) {
	runner := &fakeRunner{results: []string{
		dump(keep, "Melanie", "Smith", "", "", "", []pair{{"_$!<Home>!$_", "melanie@hackclub.com"}}, nil, nil),
		keep + FieldSeparator + "Melanie Smith",
	}}
	result := NewExecutor(runner.run).Execute(mutation(UpdateContactOperation, map[string]any{
		"card_id": keep,
		"contact": map[string]any{
			"organization": "Hack Club", "job_title": "Director of Operations, HCB",
			"emails": entryList(pair{"work", "Melanie@HackClub.com"}, pair{"home", "mel@example.com"}),
		},
	}))
	if result.Status != queue.StatusSucceeded {
		t.Fatalf("result %+v", result)
	}
	previous := result.ResultJSON["previous_card"].(map[string]any)
	if previous["given_name"] != "Melanie" || previous["card_id"] != keep {
		t.Fatalf("previous_card %v", previous)
	}
	if !reflect.DeepEqual(result.ResultJSON["added"], map[string][]string{"emails": {"mel@example.com"}, "phones": {}, "urls": {}}) {
		t.Fatalf("added %v", result.ResultJSON["added"])
	}
	if result.ResultJSON["changed"] != true || result.ResultJSON["note_changed"] != false || result.ResultJSON["name"] != "Melanie Smith" {
		t.Fatalf("result %v", result.ResultJSON)
	}
	write := runner.scripts[1]
	for _, want := range []string{`person id "` + keep + `"`, `set organization of p to "Hack Club"`, `set job title of p to "Director of Operations, HCB"`, `value:"mel@example.com"`} {
		if !strings.Contains(write, want) {
			t.Fatalf("write lacks %q:\n%s", want, write)
		}
	}
	if strings.Contains(write, "Melanie@HackClub.com") { // already on the card, case-insensitively
		t.Fatalf("write re-adds an existing email:\n%s", write)
	}
}

func TestUpdateContactCanRemoveAValueAndAppendToTheNote(t *testing.T) {
	runner := &fakeRunner{results: []string{
		dump(keep, "Max", "Wofford", "", "", "old note", []pair{{"_$!<Work>!$_", "max@hackedu.us"}}, nil, nil),
		keep + FieldSeparator + "Max Wofford",
	}}
	result := NewExecutor(runner.run).Execute(mutation(UpdateContactOperation, map[string]any{
		"card_id": keep,
		"contact": map[string]any{"append_note": "Left HQ 2026"},
		"remove":  map[string]any{"emails": []any{"max@hackedu.us"}},
	}))
	if result.Status != queue.StatusSucceeded || result.ResultJSON["note_changed"] != true {
		t.Fatalf("result %+v", result)
	}
	write := runner.scripts[1]
	if !strings.Contains(write, `delete (every email of p whose value is "max@hackedu.us")`) ||
		!strings.Contains(write, `set note of p to "old note" & linefeed & "Left HQ 2026"`) {
		t.Fatalf("write:\n%s", write)
	}
	if !reflect.DeepEqual(result.ResultJSON["removed"], map[string][]string{"emails": {"max@hackedu.us"}, "phones": {}, "urls": {}}) {
		t.Fatalf("removed %v", result.ResultJSON["removed"])
	}
}

func TestUpdateContactNoteReplacesAndARemovedValueCanBeReAdded(t *testing.T) {
	runner := &fakeRunner{results: []string{
		dump(keep, "Max", "Wofford", "", "", "old", nil, []pair{{"_$!<Mobile>!$_", "(310) 414-7928"}}, nil),
		keep + FieldSeparator + "Max Wofford",
	}}
	result := NewExecutor(runner.run).Execute(mutation(UpdateContactOperation, map[string]any{
		"card_id": keep,
		"contact": map[string]any{"note": "new", "append_note": "ignored", "phones": entryList(pair{"work", "+13104147928"})},
		"remove":  map[string]any{"phones": []any{"310-414-7928"}},
	}))
	if result.Status != queue.StatusSucceeded {
		t.Fatalf("result %+v", result)
	}
	write := runner.scripts[1]
	if !strings.Contains(write, `set note of p to "new"`) || strings.Contains(write, "ignored") {
		t.Fatalf("note replace:\n%s", write)
	}
	// Phones match on their last ten digits, so the removal finds the stored
	// spelling and the re-add is then allowed.
	if !strings.Contains(write, `delete (every phone of p whose value is "(310) 414-7928")`) ||
		!strings.Contains(write, `make new phone at end of phones of p with properties {label:"work", value:"+13104147928"}`) {
		t.Fatalf("phone remove/add:\n%s", write)
	}
}

func TestUpdateContactWithNothingNewStillSucceedsWithoutWriting(t *testing.T) {
	runner := &fakeRunner{results: []string{dump(keep, "Max", "Wofford", "", "", "", []pair{{"_$!<Work>!$_", "max@hackclub.com"}}, nil, nil)}}
	result := NewExecutor(runner.run).Execute(mutation(UpdateContactOperation, map[string]any{
		"card_id": keep, "contact": map[string]any{"emails": entryList(pair{"work", "max@hackclub.com"}), "given_name": "Max"},
	}))
	if result.Status != queue.StatusSucceeded || result.ResultJSON["changed"] != false || result.ResultJSON["card_id"] != keep {
		t.Fatalf("result %+v", result)
	}
	if _, ok := result.ResultJSON["previous_card"]; !ok {
		t.Fatal("previous_card must be recorded even when nothing changed")
	}
	if len(runner.scripts) != 1 {
		t.Fatalf("expected the read only, got %d scripts", len(runner.scripts))
	}
}

func TestUpdateContactNeedsACardID(t *testing.T) {
	runner := &fakeRunner{}
	result := NewExecutor(runner.run).Execute(mutation(UpdateContactOperation, map[string]any{"contact": map[string]any{"organization": "x"}}))
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, "card_id") || len(runner.scripts) != 0 {
		t.Fatalf("result %+v", result)
	}
}

func TestMergeContactsUnionsTheListsOntoTheKeptCardAndDeletesTheRest(t *testing.T) {
	runner := &fakeRunner{results: []string{
		dump(keep, "Katie", "Latta", "", "", "", []pair{{"_$!<Home>!$_", "katie@example.com"}}, nil, nil),
		dump(other, "Katie", "Latta", "Acme", "", "", nil, []pair{{"_$!<Mobile>!$_", "(310) 414-7928"}}, nil),
		keep + FieldSeparator + "Katie Latta",
	}}
	result := NewExecutor(runner.run).Execute(mutation(MergeContactsOperation, map[string]any{
		"keep_card_id": keep, "merge_card_ids": []any{other},
	}))
	if result.Status != queue.StatusSucceeded {
		t.Fatalf("result %+v", result)
	}
	write := runner.scripts[2]
	for _, want := range []string{
		`make new phone at end of phones of p with properties {label:"mobile", value:"(310) 414-7928"}`,
		`set organization of p to "Acme"`, // a scalar the kept card lacked
		`delete person id "` + other + `"`,
	} {
		if !strings.Contains(write, want) {
			t.Fatalf("write lacks %q:\n%s", want, write)
		}
	}
	if strings.Index(write, "delete person id") > strings.Index(write, "\nsave") {
		t.Fatalf("delete must precede save:\n%s", write)
	}
	previous := result.ResultJSON["previous_cards"].([]map[string]any)
	if len(previous) != 2 || previous[0]["card_id"] != keep || previous[1]["card_id"] != other {
		t.Fatalf("previous_cards %v", previous)
	}
	if !reflect.DeepEqual(result.ResultJSON["deleted_card_ids"], []string{other}) || result.ResultJSON["action"] != "merge" {
		t.Fatalf("result %v", result.ResultJSON)
	}
	if !reflect.DeepEqual(result.ResultJSON["added"], map[string][]string{"emails": {}, "phones": {"(310) 414-7928"}, "urls": {}}) {
		t.Fatalf("added %v", result.ResultJSON["added"])
	}
}

func TestMergeContactsAppliesFieldOverridesOnTopOfTheUnion(t *testing.T) {
	runner := &fakeRunner{results: []string{
		dump(keep, "Rebeka", "Hack Club", "", "", "", nil, nil, nil),
		dump(other, "Rebeka", "Lawrence-Gomez", "", "", "", nil, []pair{{"_$!<Mobile>!$_", "+18027528709"}}, nil),
		keep + FieldSeparator + "Rebeka Lawrence-Gomez",
	}}
	result := NewExecutor(runner.run).Execute(mutation(MergeContactsOperation, map[string]any{
		"keep_card_id": keep, "merge_card_ids": []any{other}, "contact": map[string]any{"family_name": "Lawrence-Gomez"},
	}))
	if result.Status != queue.StatusSucceeded || !strings.Contains(runner.scripts[2], `set last name of p to "Lawrence-Gomez"`) {
		t.Fatalf("result %+v script:\n%s", result, runner.scripts[2])
	}
}

func TestMergeContactsCarriesTheOtherCardsNoteAndListOverrides(t *testing.T) {
	runner := &fakeRunner{results: []string{
		dump(keep, "K", "L", "", "", "kept note", nil, nil, nil),
		dump(other, "K", "L", "", "", "other note", nil, nil, nil),
		keep + FieldSeparator + "K L",
	}}
	result := NewExecutor(runner.run).Execute(mutation(MergeContactsOperation, map[string]any{
		"keep_card_id": keep, "merge_card_ids": []any{other},
		"contact": map[string]any{"urls": entryList(pair{"homepage", "https://example.com/"})},
	}))
	if result.Status != queue.StatusSucceeded || result.ResultJSON["note_changed"] != true {
		t.Fatalf("result %+v", result)
	}
	write := runner.scripts[2]
	if !strings.Contains(write, `set note of p to "kept note" & linefeed & "other note"`) ||
		!strings.Contains(write, `make new url at end of urls of p with properties {label:"homepage", value:"https://example.com/"}`) {
		t.Fatalf("write:\n%s", write)
	}
}

func TestMergeRefusesToDeleteTheKeptCard(t *testing.T) {
	runner := &fakeRunner{}
	result := NewExecutor(runner.run).Execute(mutation(MergeContactsOperation, map[string]any{"keep_card_id": keep, "merge_card_ids": []any{keep}}))
	if result.Status != queue.StatusFailedTerminal || len(runner.scripts) != 0 {
		t.Fatalf("result %+v", result)
	}
	result = NewExecutor(runner.run).Execute(mutation(MergeContactsOperation, map[string]any{"keep_card_id": keep}))
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, "merge_card_ids") {
		t.Fatalf("result %+v", result)
	}
}

func TestAMissingCardIsTerminalNotRetryable(t *testing.T) {
	runner := &fakeRunner{err: errors.New(`Can’t get person id "nope". (-1728)`)}
	result := NewExecutor(runner.run).Execute(mutation(UpdateContactOperation, map[string]any{"card_id": "nope:ABPerson", "contact": map[string]any{"organization": "x"}}))
	if result.Status != queue.StatusFailedTerminal {
		t.Fatalf("result %+v", result)
	}
}

func TestADeniedAutomationGrantIsBlockedNotFailed(t *testing.T) {
	runner := &fakeRunner{err: errors.New("Not authorized to send Apple events to Contacts. (-1743)")}
	result := NewExecutor(runner.run).Execute(mutation(UpdateContactOperation, map[string]any{"card_id": keep, "contact": map[string]any{"organization": "x"}}))
	if result.Status != queue.StatusBlockedMissingCredentials || !strings.Contains(result.Error, "Contacts.app") {
		t.Fatalf("result %+v", result)
	}
}

func TestAnAppleEventTimeoutIsRetryable(t *testing.T) {
	runner := &fakeRunner{err: errors.New("Contacts got an error: AppleEvent timed out. (-1712)")}
	result := NewExecutor(runner.run).Execute(mutation(UpdateContactOperation, map[string]any{"card_id": keep, "contact": map[string]any{"organization": "x"}}))
	if result.Status != queue.StatusFailedRetryable {
		t.Fatalf("result %+v", result)
	}
	runner = &fakeRunner{err: applescript.ErrTimeout}
	result = NewExecutor(runner.run).Execute(mutation(UpdateContactOperation, map[string]any{"card_id": keep, "contact": map[string]any{"organization": "x"}}))
	if result.Status != queue.StatusFailedRetryable || result.Error != "Contacts.app did not answer within 180s" {
		t.Fatalf("result %+v", result)
	}
}

func TestAForeignProviderIsLeftForAnotherWorker(t *testing.T) {
	runner := &fakeRunner{}
	for _, m := range []queue.Mutation{
		{ID: "m", Provider: "gmail", Operation: "gmail.send_email"},
		{ID: "m", Provider: Provider, Operation: "apple_contacts.delete_contact"},
	} {
		if result := NewExecutor(runner.run).Execute(m); result.Status != queue.StatusFailedRetryable {
			t.Fatalf("%v: result %+v", m, result)
		}
	}
	if len(runner.scripts) != 0 {
		t.Fatal("a deferred row must not run any script")
	}
}

func TestSpecIsTheAppleContactsWorker(t *testing.T) {
	if Spec.Provider != "apple_contacts" || Spec.WorkerName != "apple_contacts_mutation_worker" || Spec.LockID != 7_403_111_853 || Spec.EnvPrefix != "APPLE_CONTACTS" {
		t.Fatalf("spec %+v", Spec)
	}
	// A replayed create duplicates a card and a replayed merge deletes cards
	// that are already gone; only update_contact is reclaimed.
	if len(Spec.Idempotent) != 1 || Spec.Idempotent[0] != (queue.Operation{Provider: Provider, Operation: UpdateContactOperation}) {
		t.Fatalf("idempotent %v", Spec.Idempotent)
	}
	if !Enabled(func(string) string { return "" }) || Enabled(func(string) string { return "no" }) {
		t.Fatal("kill switch")
	}
}

// -- a card deleted by an earlier merge ---------------------------------------
//
// Two requests proposed from one warehouse snapshot can name the same card: one
// merges it away, the other updates it. The merge executes first, Contacts.app
// then answers -1728 for the update, and the update used to die failed_terminal
// even though the surviving card is exactly where its values belong. The
// executor asks the mutation ledger which card the missing one was merged into
// and applies the update there.

// raisingThen returns the string entries and raises the error entries, in order.
type raisingThen struct {
	results []any
	scripts []string
}

func (f *raisingThen) run(script string) (string, error) {
	f.scripts = append(f.scripts, script)
	if len(f.results) == 0 {
		return "", errors.New("fake runner: no result left")
	}
	item := f.results[0]
	f.results = f.results[1:]
	if err, ok := item.(error); ok {
		return "", err
	}
	return item.(string), nil
}

func missingCard(cardID string) error {
	return errors.New(`199:256: execution error: Contacts got an error: Can’t get person id "` + cardID + `". (-1728)`)
}

func mergedInto(targets map[string]string) MergedInto {
	return func(cardID string) (string, error) { return targets[cardID], nil }
}

func TestUpdateOfACardMergedAwayIsRedirectedToTheSurvivingCard(t *testing.T) {
	runner := &raisingThen{results: []any{
		missingCard(other),
		dump(keep, "Sam", "Griffis", "", "", "", []pair{{"_$!<Other>!$_", "sam@example.com"}}, nil, nil),
		keep + FieldSeparator + "Sam Griffis",
	}}
	executor := NewExecutor(runner.run).WithMergedInto(mergedInto(map[string]string{other: keep}))
	result := executor.Execute(mutation(UpdateContactOperation, map[string]any{
		"card_id": other, "contact": map[string]any{"emails": entryList(pair{"work", "sam@hackclub.example"})},
	}))
	if result.Status != queue.StatusSucceeded {
		t.Fatalf("status = %s (%s)", result.Status, result.Error)
	}
	if result.ResultJSON["card_id"] != keep || result.ResultJSON["redirected_from"] != other {
		t.Fatalf("result = %v", result.ResultJSON)
	}
	added := result.ResultJSON["added"].(map[string][]string)
	if !reflect.DeepEqual(added["emails"], []string{"sam@hackclub.example"}) {
		t.Fatalf("added = %v", added)
	}
	if !strings.Contains(runner.scripts[2], `person id "`+keep+`"`) {
		t.Fatalf("write did not target the surviving card: %s", runner.scripts[2])
	}
}

func TestUpdateOfACardMergedAwayFollowsAChainOfMerges(t *testing.T) {
	middle := "11111111-0000-4468-9061-D2D41468E05A:ABPerson"
	runner := &raisingThen{results: []any{missingCard(other), dump(keep, "Sam", "Griffis", "", "", "", nil, nil, nil), keep + FieldSeparator + "Sam Griffis"}}
	executor := NewExecutor(runner.run).WithMergedInto(mergedInto(map[string]string{other: middle, middle: keep}))
	result := executor.Execute(mutation(UpdateContactOperation, map[string]any{
		"card_id": other, "contact": map[string]any{"organization": "Hack Club"},
	}))
	if result.Status != queue.StatusSucceeded || result.ResultJSON["card_id"] != keep || result.ResultJSON["redirected_from"] != other {
		t.Fatalf("result = %+v", result)
	}
}

func TestAMergeCycleInTheLedgerTerminates(t *testing.T) {
	runner := &raisingThen{results: []any{missingCard(other), missingCard(keep)}}
	executor := NewExecutor(runner.run).WithMergedInto(mergedInto(map[string]string{other: keep, keep: other}))
	result := executor.Execute(mutation(UpdateContactOperation, map[string]any{
		"card_id": other, "contact": map[string]any{"organization": "Hack Club"},
	}))
	if result.Status != queue.StatusFailedTerminal {
		t.Fatalf("result = %+v", result)
	}
}

func TestAnUnchangedRedirectedUpdateStillRecordsWhereItWent(t *testing.T) {
	runner := &raisingThen{results: []any{missingCard(other), dump(keep, "Sam", "Griffis", "Hack Club", "", "", nil, nil, nil)}}
	executor := NewExecutor(runner.run).WithMergedInto(mergedInto(map[string]string{other: keep}))
	result := executor.Execute(mutation(UpdateContactOperation, map[string]any{
		"card_id": other, "contact": map[string]any{"organization": "Hack Club"},
	}))
	if result.Status != queue.StatusSucceeded || result.ResultJSON["changed"] != false || result.ResultJSON["redirected_from"] != other {
		t.Fatalf("result = %+v", result)
	}
}

func TestUpdateOfACardNobodyMergedStaysTerminal(t *testing.T) {
	runner := &raisingThen{results: []any{missingCard(other)}}
	executor := NewExecutor(runner.run).WithMergedInto(mergedInto(nil))
	result := executor.Execute(mutation(UpdateContactOperation, map[string]any{
		"card_id": other, "contact": map[string]any{"organization": "Hack Club"},
	}))
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, "-1728") || len(runner.scripts) != 1 {
		t.Fatalf("result = %+v scripts=%d", result, len(runner.scripts))
	}
	if _, ok := result.ResultJSON["redirected_from"]; ok {
		t.Fatal("an unredirected failure must not claim a redirect")
	}
}

func TestALedgerThatCannotBeAskedIsRetriedNotRetired(t *testing.T) {
	runner := &raisingThen{results: []any{missingCard(other)}}
	executor := NewExecutor(runner.run).WithMergedInto(func(string) (string, error) { return "", errors.New("connection refused") })
	result := executor.Execute(mutation(UpdateContactOperation, map[string]any{
		"card_id": other, "contact": map[string]any{"organization": "Hack Club"},
	}))
	if result.Status != queue.StatusFailedRetryable || !strings.Contains(result.Error, "connection refused") {
		t.Fatalf("result = %+v", result)
	}
}

func TestMergeSkipsACardAlreadyMergedIntoTheKeptCard(t *testing.T) {
	runner := &raisingThen{results: []any{
		dump(keep, "Sam", "Griffis", "", "", "", nil, nil, nil),
		missingCard(other),
		keep + FieldSeparator + "Sam Griffis",
	}}
	executor := NewExecutor(runner.run).WithMergedInto(mergedInto(map[string]string{other: keep}))
	result := executor.Execute(mutation(MergeContactsOperation, map[string]any{
		"keep_card_id": keep, "merge_card_ids": []any{other}, "contact": map[string]any{"organization": "Hack Club"},
	}))
	if result.Status != queue.StatusSucceeded {
		t.Fatalf("result = %+v", result)
	}
	if !reflect.DeepEqual(result.ResultJSON["deleted_card_ids"], []string{}) ||
		!reflect.DeepEqual(result.ResultJSON["already_merged_card_ids"], []string{other}) {
		t.Fatalf("result = %v", result.ResultJSON)
	}
	if strings.Contains(runner.scripts[2], "delete person id") || !strings.Contains(runner.scripts[2], `set organization of p to "Hack Club"`) {
		t.Fatalf("script = %s", runner.scripts[2])
	}
}

func TestMergeRefusesACardAlreadyMergedSomewhereElse(t *testing.T) {
	elsewhere := "22222222-0000-4468-9061-D2D41468E05A:ABPerson"
	runner := &raisingThen{results: []any{dump(keep, "Sam", "Griffis", "", "", "", nil, nil, nil), missingCard(other)}}
	executor := NewExecutor(runner.run).WithMergedInto(mergedInto(map[string]string{other: elsewhere}))
	result := executor.Execute(mutation(MergeContactsOperation, map[string]any{
		"keep_card_id": keep, "merge_card_ids": []any{other},
	}))
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, elsewhere) || len(runner.scripts) != 2 {
		t.Fatalf("result = %+v scripts=%d", result, len(runner.scripts))
	}
}
