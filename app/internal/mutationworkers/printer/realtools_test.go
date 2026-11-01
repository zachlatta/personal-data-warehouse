package printer

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

// TestRealToolsPrintToAFakePrinter runs the executor with the real pdfinfo,
// Ghostscript and ipptool against PDW_PRINTER_TEST_URI, which must be a fake
// IPP printer (ippeveprinter), never the Brother. It is skipped otherwise.
// Run it in the printer image: see docs/agents/mutations-and-app.md.
func TestRealToolsPrintToAFakePrinter(t *testing.T) {
	uri := os.Getenv("PDW_PRINTER_TEST_URI")
	if uri == "" {
		t.Skip("PDW_PRINTER_TEST_URI is not set")
	}
	data, err := os.ReadFile("testdata/two-pages.pdf")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	mutation := queue.Mutation{
		ID: "realtools-" + time.Now().Format("150405"), Provider: Provider, Operation: PrintDocumentOperation,
		Payload: map[string]any{
			"filename": "two-pages.pdf", "data_base64": base64.StdEncoding.EncodeToString(data),
			"sha256": hex.EncodeToString(sum[:]), "page_count": float64(2), "copies": float64(2),
		},
	}
	followFor = time.Minute
	followEvery = 2 * time.Second
	result := NewExecutor(uri, ExecRunner, true).Execute(mutation)
	t.Logf("result: %#v", result)
	if result.Status != queue.StatusSucceeded || result.ResultJSON["job_id"] == "" {
		t.Fatalf("result = %#v", result)
	}
	// A wrong page count is refused before anything is sent.
	mutation.Payload["page_count"] = float64(3)
	if refused := NewExecutor(uri, ExecRunner, true).Execute(mutation); refused.Status != queue.StatusFailedTerminal {
		t.Fatalf("mismatch = %#v", refused)
	}
}
