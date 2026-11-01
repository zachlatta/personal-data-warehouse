// Package printer prints approved printer.print_document mutations on the
// home Brother MFC-L2710DW. It runs beside the scanner service on
// mew-coolify (app/scanner-compose.yaml), the one host with a leg on the
// home LAN, and uses standard tools rather than its own printer code:
// pdfinfo counts the pages, Ghostscript renders them as PWG raster (the
// printer takes no PDF) and ipptool sends one IPP Print-Job and reads the
// job's state.
//
// One approval prints at most once. Only a failure that proves nothing
// reached the printer (ipptool could not connect, or the printer answered
// busy) is retried; anything after the document may have been sent fails
// terminal and asks a human to check the tray. A stale `executing` claim is
// never reclaimed (Spec.Idempotent is empty).
package printer

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

const (
	Provider               = "printer"
	PrintDocumentOperation = "printer.print_document"

	// LockID is this provider's own advisory lock.
	LockID int64 = 7_403_111_881

	// URIEnv is the printer's IPP endpoint.
	URIEnv = "PRINTER_URI"
)

var (
	//go:embed print_job.test
	printJobTest []byte
	//go:embed job_status.test
	jobStatusTest []byte
)

// Timeouts and the job follow-up. Package vars so tests can shrink them.
var (
	pdfinfoTimeout = time.Minute
	renderTimeout  = 30 * time.Minute
	sendTimeout    = 10 * time.Minute
	statusTimeout  = 30 * time.Second
	followFor      = 5 * time.Minute
	followEvery    = 5 * time.Second
)

// Runner runs one command and returns its combined stdout and stderr.
type Runner func(ctx context.Context, name string, args ...string) ([]byte, error)

// ExecRunner runs the real binary.
func ExecRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

// Executor prints printer.print_document mutations.
type Executor struct {
	uri     string
	run     Runner
	enabled bool
	sleep   func(time.Duration)
	now     func() time.Time
}

// NewExecutor builds an executor for the printer at uri. A disabled
// executor (PRINTER_MUTATIONS_ENABLED=0) prints nothing and defers every row.
func NewExecutor(uri string, run Runner, enabled bool) *Executor {
	return &Executor{uri: uri, run: run, enabled: enabled, sleep: time.Sleep, now: time.Now}
}

// Execute implements queue.Executor.
func (e *Executor) Execute(mutation queue.Mutation) queue.Result {
	if mutation.Provider != Provider || mutation.Operation != PrintDocumentOperation {
		// Never burn an unrecognized row to failed_terminal: a newer worker
		// may understand it.
		return retryable(fmt.Sprintf("unsupported mutation operation %s.%s; deferring", mutation.Provider, mutation.Operation))
	}
	if !e.enabled {
		return retryable("printing is paused (PRINTER_MUTATIONS_ENABLED=0); nothing was sent")
	}
	if strings.TrimSpace(e.uri) == "" {
		return retryable("PRINTER_URI is not set; nothing was sent")
	}
	payload := mutation.Payload
	data, err := base64.StdEncoding.Strict().DecodeString(stringValue(payload["data_base64"]))
	if err != nil || len(data) == 0 {
		return terminal("payload has no readable data_base64; nothing was sent", nil)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != stringValue(payload["sha256"]) {
		return terminal(fmt.Sprintf("the PDF's sha256 %s is not the reviewed %s; nothing was sent", got, stringValue(payload["sha256"])), nil)
	}
	wantPages := intValue(payload["page_count"])
	copies := intValue(payload["copies"])
	if wantPages < 1 || copies < 1 {
		return terminal("payload has no page_count or copies; nothing was sent", nil)
	}
	result := map[string]any{
		"printer_uri": e.uri, "sha256": stringValue(payload["sha256"]),
		"page_count": wantPages, "copies": copies,
	}

	dir, err := os.MkdirTemp("", "pdw-print-")
	if err != nil {
		return retryable("cannot make a work directory: " + err.Error())
	}
	defer os.RemoveAll(dir)
	pdf := filepath.Join(dir, "document.pdf")
	pwg := filepath.Join(dir, "document.pwg")
	printTest := filepath.Join(dir, "print_job.test")
	statusTest := filepath.Join(dir, "job_status.test")
	for path, content := range map[string][]byte{pdf: data, printTest: printJobTest, statusTest: jobStatusTest} {
		if err := os.WriteFile(path, content, 0o600); err != nil {
			return retryable("cannot write the work directory: " + err.Error())
		}
	}

	pages, err := e.pageCount(pdf)
	if err != nil {
		return terminal("pdfinfo cannot read the PDF ("+err.Error()+"); nothing was sent", result)
	}
	if pages != wantPages {
		return terminal(fmt.Sprintf("the PDF has %d pages, not the %d the request showed; nothing was sent", pages, wantPages), result)
	}
	if out, err := e.runFor(renderTimeout, "gs", "-q", "-dSAFER", "-dBATCH", "-dNOPAUSE",
		"-sDEVICE=pwgraster", "-r600", "-sPAPERSIZE=letter", "-dFIXEDMEDIA", "-dPDFFitPage",
		"-dcupsColorSpace=18", "-dcupsBitsPerColor=8",
		"-sOutputFile="+pwg, pdf); err != nil {
		return terminal("Ghostscript cannot render the PDF ("+commandError(err, out)+"); nothing was sent", result)
	}

	jobName := "pdw-" + mutation.ID
	result["job_name"] = jobName
	out, err := e.runFor(sendTimeout, "ipptool", "-t", "-L", "-T", "60",
		"-d", "job_name="+jobName, "-d", "copies="+strconv.Itoa(copies),
		"-f", pwg, e.uri, printTest)
	report := parseReport(out)
	if err != nil || report.Status == "" || report.Attrs["job-id"] == "" {
		return sendFailure(out, err, report, result)
	}
	jobID := report.Attrs["job-id"]
	result["job_id"] = jobID
	return e.follow(jobID, statusTest, report.Attrs["job-state"], result)
}

// sendFailure classifies a Print-Job that did not return a job id.
func sendFailure(out []byte, err error, report ippReport, result map[string]any) queue.Result {
	detail := commandError(err, out)
	switch {
	case bytes.Contains(out, []byte("Unable to connect to")):
		return retryable("cannot reach the printer (" + detail + "); nothing was sent")
	case report.Status == "server-error-busy" || report.Status == "server-error-service-unavailable":
		// An error answer creates no job.
		return retryable("the printer is busy (" + report.Status + "); nothing was printed")
	case strings.HasPrefix(report.Status, "client-error-") || strings.HasPrefix(report.Status, "server-error-"):
		return terminal("the printer refused the job ("+report.Status+" "+report.Message+"); nothing was printed", result)
	}
	return terminal("the connection to the printer failed after the document may have been sent ("+detail+"); it may have printed, check the printer. It is not sent again: propose a new request to reprint", result)
}

// follow polls the job until it ends or followFor passes.
func (e *Executor) follow(jobID, statusTest, state string, result map[string]any) queue.Result {
	deadline := e.now().Add(followFor)
	for {
		switch state {
		case "completed":
			result["job_state"] = state
			result["verified"] = true
			return queue.Result{Status: queue.StatusSucceeded, ResultJSON: result}
		case "canceled", "aborted":
			result["job_state"] = state
			return terminal(fmt.Sprintf("the printer ended job %s as %s (%v); part of it may have printed", jobID, state, result["job_state_reasons"]), result)
		}
		if !e.now().Before(deadline) {
			result["job_state"] = state
			result["verified"] = false
			result["note"] = fmt.Sprintf("still %s after %s; the printer has the job", state, followFor)
			return queue.Result{Status: queue.StatusSucceeded, ResultJSON: result}
		}
		e.sleep(followEvery)
		out, err := e.runFor(statusTimeout, "ipptool", "-t", "-T", "20", "-d", "job_id="+jobID, e.uri, statusTest)
		report := parseReport(out)
		if err != nil || report.Attrs["job-state"] == "" {
			// The job is at the printer; a missed status read is not a
			// failure. Keep the last state and ask again.
			continue
		}
		state = report.Attrs["job-state"]
		result["job_state_reasons"] = report.Attrs["job-state-reasons"]
		if impressions, err := strconv.Atoi(report.Attrs["job-impressions-completed"]); err == nil {
			result["impressions_completed"] = impressions
		}
	}
}

var pagesLine = regexp.MustCompile(`(?m)^Pages:\s+(\d+)\s*$`)

func (e *Executor) pageCount(pdf string) (int, error) {
	out, err := e.runFor(pdfinfoTimeout, "pdfinfo", pdf)
	if err != nil {
		return 0, fmt.Errorf("%s", commandError(err, out))
	}
	match := pagesLine.FindSubmatch(out)
	if match == nil {
		return 0, fmt.Errorf("no Pages line")
	}
	return strconv.Atoi(string(match[1]))
}

func (e *Executor) runFor(timeout time.Duration, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return e.run(ctx, name, args...)
}

// ippReport is what `ipptool -t` printed for one test: the response's
// status-code and the DISPLAYed attributes.
type ippReport struct {
	Status  string
	Message string
	Attrs   map[string]string
}

var (
	statusLine = regexp.MustCompile(`(?m)^\s+status-code = (\S+)(?: \((.*)\))?\s*$`)
	attrLine   = regexp.MustCompile(`(?m)^\s+([a-z0-9-]+) \([a-zA-Z0-9 ]+\) = (.*?)\s*$`)
	passLine   = regexp.MustCompile(`\[PASS\]`)
)

func parseReport(out []byte) ippReport {
	report := ippReport{Attrs: map[string]string{}}
	if match := statusLine.FindSubmatch(out); match != nil {
		report.Status = string(match[1])
		report.Message = string(match[2])
	} else if passLine.Match(out) {
		// A passing test prints no status line: it matched a STATUS.
		report.Status = "successful-ok"
	}
	for _, match := range attrLine.FindAllSubmatch(out, -1) {
		report.Attrs[string(match[1])] = string(match[2])
	}
	return report
}

func commandError(err error, out []byte) string {
	text := strings.TrimSpace(string(out))
	if len(text) > 500 {
		text = text[len(text)-500:]
	}
	switch {
	case err == nil:
		return text
	case text == "":
		return err.Error()
	}
	return err.Error() + ": " + text
}

func retryable(message string) queue.Result {
	return queue.Result{Status: queue.StatusFailedRetryable, Error: message}
}

func terminal(message string, result map[string]any) queue.Result {
	return queue.Result{Status: queue.StatusFailedTerminal, Error: message, ResultJSON: result}
}

func stringValue(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func intValue(value any) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	case int64:
		return int(typed)
	}
	return 0
}
