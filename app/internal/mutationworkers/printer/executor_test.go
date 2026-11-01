package printer

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

// The ipptool -t outputs below were captured from ipptool 2.4.2 (Debian
// bookworm) against ippeveprinter.
const (
	// testURI is a documentation address (RFC 5737); the real one is config.
	testURI   = "ipp://192.0.2.10/ipp/print"
	printPass = `"print_job.test":
    Print-Job                                                            [PASS]
        job-id (integer) = 41
        job-state (enum) = pending
`
	printBusy = `"print_job.test":
    Print-Job                                                            [FAIL]
        RECEIVED: 122 bytes in response
        status-code = server-error-busy (Currently printing another job.)
        EXPECTED: STATUS successful-ok (got server-error-busy)
        EXPECTED: STATUS successful-ok-ignored-or-substituted-attributes (got server-error-busy)
        status-message="Currently printing another job."
        EXPECTED: job-id
`
	printRefused = `"print_job.test":
    Print-Job                                                            [FAIL]
        RECEIVED: 105 bytes in response
        status-code = client-error-attributes-or-values-not-supported (Unsupported copies integer value.)
        EXPECTED: STATUS successful-ok (got client-error-attributes-or-values-not-supported)
`
	unreachable = `ipptool: Unable to connect to "192.0.2.10" on port 631 - Host is down
`
	statusProcessing = `"job_status.test":
    Get-Job-Attributes                                                   [PASS]
        job-impressions-completed (integer) = 0
        job-state (enum) = processing
        job-state-reasons (keyword) = job-printing
`
	statusCompleted = `"job_status.test":
    Get-Job-Attributes                                                   [PASS]
        job-impressions-completed (integer) = 2
        job-state (enum) = completed
        job-state-reasons (keyword) = job-completed-successfully
`
	statusAborted = `"job_status.test":
    Get-Job-Attributes                                                   [PASS]
        job-impressions-completed (integer) = 1
        job-state (enum) = aborted
        job-state-reasons (keyword) = aborted-by-system
`
)

var testPDF = []byte("%PDF-1.4 test\n")

type fakeCommands struct {
	pages    string
	gsErr    error
	print    string
	printErr error
	statuses []string
	calls    []string
	printArg []string
}

func (f *fakeCommands) run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.calls = append(f.calls, name)
	switch name {
	case "pdfinfo":
		if _, err := os.Stat(args[0]); err != nil {
			return nil, err
		}
		return []byte("Producer:       test\nPages:          " + f.pages + "\nPage size:      612 x 792 pts (letter)\n"), nil
	case "gs":
		return nil, f.gsErr
	case "ipptool":
		for _, arg := range args {
			if strings.HasSuffix(arg, "print_job.test") {
				f.printArg = args
				return []byte(f.print), f.printErr
			}
		}
		if len(f.statuses) == 0 {
			return []byte(unreachable), errors.New("exit status 1")
		}
		next := f.statuses[0]
		f.statuses = f.statuses[1:]
		return []byte(next), nil
	}
	return nil, errors.New("unexpected command " + name)
}

func (f *fakeCommands) count(name string) int {
	n := 0
	for _, call := range f.calls {
		if call == name {
			n++
		}
	}
	return n
}

func printMutation() queue.Mutation {
	sum := sha256.Sum256(testPDF)
	return queue.Mutation{
		ID: "mut-1", Provider: Provider, Operation: PrintDocumentOperation, Account: "home-brother",
		Payload: map[string]any{
			"filename": "a.pdf", "data_base64": base64.StdEncoding.EncodeToString(testPDF),
			"sha256": hex.EncodeToString(sum[:]), "page_count": float64(2), "copies": float64(3), "sides": "one-sided",
		},
	}
}

func testExecutor(commands *fakeCommands) *Executor {
	executor := NewExecutor(testURI, commands.run, true)
	clock := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	executor.now = func() time.Time { return clock }
	executor.sleep = func(d time.Duration) { clock = clock.Add(d) }
	return executor
}

func TestAPrintThatCompletesSucceedsWithItsJob(t *testing.T) {
	commands := &fakeCommands{pages: "2", print: printPass, statuses: []string{statusProcessing, statusCompleted}}
	result := testExecutor(commands).Execute(printMutation())
	if result.Status != queue.StatusSucceeded || result.ResultJSON["job_id"] != "41" || result.ResultJSON["job_state"] != "completed" ||
		result.ResultJSON["verified"] != true || result.ResultJSON["impressions_completed"] != 2 || result.ResultJSON["job_name"] != "pdw-mut-1" {
		t.Fatalf("result = %#v", result)
	}
	args := strings.Join(commands.printArg, " ")
	for _, want := range []string{"-d job_name=pdw-mut-1", "-d copies=3", testURI, "-L"} {
		if !strings.Contains(args, want) {
			t.Fatalf("print args %q lack %q", args, want)
		}
	}
}

func TestAJobStillPrintingAfterTheFollowUpIsUnverified(t *testing.T) {
	statuses := []string{}
	for i := 0; i < 100; i++ {
		statuses = append(statuses, statusProcessing)
	}
	commands := &fakeCommands{pages: "2", print: printPass, statuses: statuses}
	result := testExecutor(commands).Execute(printMutation())
	if result.Status != queue.StatusSucceeded || result.ResultJSON["verified"] != false || result.ResultJSON["job_state"] != "processing" {
		t.Fatalf("result = %#v", result)
	}
	if commands.count("ipptool") > int(followFor/followEvery)+1 {
		t.Fatalf("followed past the deadline: %d calls", commands.count("ipptool"))
	}
}

func TestAMissedStatusReadKeepsFollowing(t *testing.T) {
	// One unreachable status read (the fake answers unreachable when the
	// list runs out) does not fail a job the printer accepted.
	commands := &fakeCommands{pages: "2", print: printPass, statuses: []string{statusProcessing}}
	result := testExecutor(commands).Execute(printMutation())
	if result.Status != queue.StatusSucceeded || result.ResultJSON["verified"] != false {
		t.Fatalf("result = %#v", result)
	}
}

func TestAnAbortedJobFailsTerminal(t *testing.T) {
	commands := &fakeCommands{pages: "2", print: printPass, statuses: []string{statusAborted}}
	result := testExecutor(commands).Execute(printMutation())
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, "aborted") || result.ResultJSON["job_id"] != "41" {
		t.Fatalf("result = %#v", result)
	}
}

func TestAnUnreachablePrinterIsRetriedBecauseNothingWasSent(t *testing.T) {
	commands := &fakeCommands{pages: "2", print: unreachable, printErr: errors.New("exit status 1")}
	result := testExecutor(commands).Execute(printMutation())
	if result.Status != queue.StatusFailedRetryable || !strings.Contains(result.Error, "nothing was sent") {
		t.Fatalf("result = %#v", result)
	}
}

func TestABusyPrinterIsRetried(t *testing.T) {
	commands := &fakeCommands{pages: "2", print: printBusy, printErr: errors.New("exit status 1")}
	result := testExecutor(commands).Execute(printMutation())
	if result.Status != queue.StatusFailedRetryable || !strings.Contains(result.Error, "server-error-busy") {
		t.Fatalf("result = %#v", result)
	}
}

func TestARefusedJobFailsTerminalWithNothingPrinted(t *testing.T) {
	commands := &fakeCommands{pages: "2", print: printRefused, printErr: errors.New("exit status 1")}
	result := testExecutor(commands).Execute(printMutation())
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, "client-error-attributes-or-values-not-supported") || !strings.Contains(result.Error, "nothing was printed") {
		t.Fatalf("result = %#v", result)
	}
}

func TestAFailureAfterSendingIsNeverRetried(t *testing.T) {
	commands := &fakeCommands{pages: "2", print: "\"print_job.test\":\n    Print-Job ", printErr: errors.New("signal: killed")}
	result := testExecutor(commands).Execute(printMutation())
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, "may have printed") {
		t.Fatalf("result = %#v", result)
	}
}

func TestAPageCountMismatchPrintsNothing(t *testing.T) {
	commands := &fakeCommands{pages: "3", print: printPass}
	result := testExecutor(commands).Execute(printMutation())
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, "has 3 pages, not the 2") || commands.count("gs") != 0 || commands.count("ipptool") != 0 {
		t.Fatalf("result = %#v calls=%v", result, commands.calls)
	}
}

func TestChangedBytesPrintNothing(t *testing.T) {
	mutation := printMutation()
	mutation.Payload["data_base64"] = base64.StdEncoding.EncodeToString([]byte("%PDF-1.4 other\n"))
	commands := &fakeCommands{pages: "2", print: printPass}
	result := testExecutor(commands).Execute(mutation)
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, "not the reviewed") || len(commands.calls) != 0 {
		t.Fatalf("result = %#v calls=%v", result, commands.calls)
	}
}

func TestARenderFailurePrintsNothing(t *testing.T) {
	commands := &fakeCommands{pages: "2", gsErr: errors.New("exit status 1"), print: printPass}
	result := testExecutor(commands).Execute(printMutation())
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, "Ghostscript") || commands.count("ipptool") != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestTheKillSwitchDefersWithoutPrinting(t *testing.T) {
	commands := &fakeCommands{pages: "2", print: printPass}
	executor := NewExecutor(testURI, commands.run, false)
	result := executor.Execute(printMutation())
	if result.Status != queue.StatusFailedRetryable || !strings.Contains(result.Error, "PRINTER_MUTATIONS_ENABLED=0") || len(commands.calls) != 0 {
		t.Fatalf("result = %#v", result)
	}
	if (queue.Spec{EnvPrefix: envPrefix}).Enabled(func(string) string { return "0" }) {
		t.Fatal("PRINTER_MUTATIONS_ENABLED=0 must disable")
	}
}

func TestAMissingPrinterURIDefersWithoutPrinting(t *testing.T) {
	commands := &fakeCommands{pages: "2", print: printPass}
	result := NewExecutor("", commands.run, true).Execute(printMutation())
	if result.Status != queue.StatusFailedRetryable || !strings.Contains(result.Error, "PRINTER_URI") || len(commands.calls) != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestTheWorkerRefusesToStartWithoutPrinterURI(t *testing.T) {
	var stdout, stderr strings.Builder
	code := Run(nil, strings.NewReader(""), &stdout, &stderr, func(string) string { return "" }, ingestclient.Config{})
	if code != 2 || !strings.Contains(stderr.String(), "PRINTER_URI must be set") {
		t.Fatalf("code = %d, stderr = %q", code, stderr.String())
	}
}

func TestUnknownOperationsAreDeferredNotBurned(t *testing.T) {
	mutation := printMutation()
	mutation.Operation = "printer.print_photo"
	result := testExecutor(&fakeCommands{}).Execute(mutation)
	if result.Status != queue.StatusFailedRetryable {
		t.Fatalf("result = %#v", result)
	}
}

func TestTheWorkerNeverReclaimsAStaleClaim(t *testing.T) {
	if len(Spec.Idempotent) != 0 || Spec.Provider != "printer" || Spec.EnvPrefix != "PRINTER" {
		t.Fatalf("spec = %#v", Spec)
	}
}
