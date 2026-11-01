package mutations

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
)

var testPDF = []byte("%PDF-1.4\n1 0 obj << >> endobj\ntrailer << >>\n%%EOF\n")

func printService(t *testing.T) (*Service, *recordingStore) {
	t.Helper()
	store := &recordingStore{request: Request{ID: "req-1", Status: "pending_review"}}
	return NewService(store, Config{BaseURL: "https://example.test"}), store
}

func printEntry(extra map[string]any) map[string]any {
	entry := map[string]any{
		"type":         PrinterPrintDocumentOperation,
		"filename":     "boarding-pass.pdf",
		"content_type": "application/pdf",
		"data_base64":  base64.StdEncoding.EncodeToString(testPDF),
		"page_count":   float64(2),
	}
	for key, value := range extra {
		if value == nil {
			delete(entry, key)
			continue
		}
		entry[key] = value
	}
	return entry
}

func TestProposePrintDocumentStoresTheBytesAndAPreview(t *testing.T) {
	service, store := printService(t)
	if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title: "Print the boarding pass", Reason: "flight tomorrow",
		Mutations: []map[string]any{printEntry(map[string]any{"copies": float64(3)})},
	}); err != nil {
		t.Fatalf("ProposeMutation: %v", err)
	}
	stored, err := normalizeForStorage(store.createCalls[0])
	if err != nil {
		t.Fatalf("normalizeForStorage: %v", err)
	}
	if len(stored) != 1 {
		t.Fatalf("stored %d", len(stored))
	}
	got := stored[0]
	// The account defaults to the only printer.
	if got.Provider != PrinterProvider || got.Operation != PrinterPrintDocumentOperation || got.Account != PrinterAccount {
		t.Fatalf("stored = %#v", got)
	}
	sum := sha256.Sum256(testPDF)
	want := map[string]any{
		"filename": "boarding-pass.pdf", "content_type": "application/pdf",
		"data_base64": base64.StdEncoding.EncodeToString(testPDF), "size_bytes": len(testPDF),
		"sha256": hex.EncodeToString(sum[:]), "page_count": 2, "copies": 3, "sides": "one-sided",
	}
	for key, value := range want {
		if got.Payload[key] != value {
			t.Fatalf("payload[%s] = %#v, want %#v", key, got.Payload[key], value)
		}
	}
	if len(got.Payload) != len(want) {
		t.Fatalf("payload has extra keys: %#v", got.Payload)
	}
	preview, _ := got.Preview["print_document"].(map[string]any)
	if preview["sheets"] != 6 || preview["sha256"] != want["sha256"] || preview["sides"] != "one-sided" || preview["data_base64"] != nil {
		t.Fatalf("preview = %#v", preview)
	}
	// The request keeps the agent's title; the mutation names the job.
	if got.Title != "Print boarding-pass.pdf (2 pages × 3 copies) on the home Brother" {
		t.Fatalf("title = %q", got.Title)
	}
}

func TestPrintDocumentTitleNamesPagesAndCopies(t *testing.T) {
	doc := printDocument{Filename: "a.pdf", PageCount: 1, Copies: 1}
	if got := printDocumentTitle(doc); got != "Print a.pdf (1 page) on the home Brother" {
		t.Fatalf("title = %q", got)
	}
	doc.PageCount, doc.Copies = 3, 2
	if got := printDocumentTitle(doc); got != "Print a.pdf (3 pages × 2 copies) on the home Brother" {
		t.Fatalf("title = %q", got)
	}
}

func TestProposePrintDocumentRefusesWhatCannotPrint(t *testing.T) {
	big := base64.StdEncoding.EncodeToString(append([]byte("%PDF-"), make([]byte, maxPrintDocumentBytes)...))
	for _, tc := range []struct {
		extra map[string]any
		want  string
	}{
		{map[string]any{"account": "office-printer"}, `account must be "home-brother"`},
		{map[string]any{"filename": nil}, "needs a filename"},
		{map[string]any{"filename": "../etc/passwd"}, "needs a filename"},
		{map[string]any{"content_type": "image/png"}, "content_type must be application/pdf"},
		{map[string]any{"data_base64": nil}, "needs data_base64"},
		{map[string]any{"data_base64": "not base64!"}, "not valid standard base64"},
		{map[string]any{"data_base64": base64.StdEncoding.EncodeToString([]byte("hello"))}, "not a PDF"},
		{map[string]any{"data_base64": big}, "at most 20 MiB"},
		{map[string]any{"page_count": nil}, "must include page_count"},
		{map[string]any{"page_count": float64(201)}, "page_count must be a whole number from 1 to 200"},
		{map[string]any{"page_count": 1.5}, "page_count must be"},
		{map[string]any{"copies": float64(0)}, "copies must be a whole number from 1 to 20"},
		{map[string]any{"copies": float64(21)}, "copies must be"},
		{map[string]any{"sides": "two-sided-long-edge"}, "unknown field(s) sides"},
		{map[string]any{"url": "https://example.test/a.pdf"}, "unknown field(s) url"},
	} {
		service, store := printService(t)
		entry := printEntry(tc.extra)
		_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{Title: "t", Reason: "r", Mutations: []map[string]any{entry}})
		if err == nil || !strings.Contains(err.Error(), tc.want) || len(store.createCalls) != 0 {
			t.Fatalf("extra %v: err = %v, want %q", tc.extra, err, tc.want)
		}
	}
}

func TestReviewReadsLeaveThePDFBytesOut(t *testing.T) {
	mutations := hydratePrintDocuments([]Mutation{
		{Operation: PrinterPrintDocumentOperation, Payload: map[string]any{"filename": "a.pdf", "data_base64": "JVBERi0=", "sha256": "abc"}},
		{Operation: GmailSendEmailOperation, Payload: map[string]any{"data_base64": "kept"}},
	})
	if _, ok := mutations[0].Payload["data_base64"]; ok || mutations[0].Payload["sha256"] != "abc" || mutations[0].Payload["filename"] != "a.pdf" {
		t.Fatalf("print payload = %#v", mutations[0].Payload)
	}
	if mutations[1].Payload["data_base64"] != "kept" {
		t.Fatalf("other types are untouched: %#v", mutations[1].Payload)
	}
}

func TestMutationHelpDocumentsPrinting(t *testing.T) {
	help, err := MutationHelpFor(ProposeMutationHelpInput{Type: PrinterPrintDocumentOperation})
	if err != nil || len(help.Mutations) != 1 {
		t.Fatalf("help: %#v err=%v", help, err)
	}
	entry := help.Mutations[0]
	for _, field := range []string{"filename", "content_type", "data_base64", "page_count", "copies"} {
		found := false
		for _, arg := range entry.Fields {
			found = found || arg.Name == field
		}
		if !found {
			t.Fatalf("help lacks %s: %#v", field, entry.Fields)
		}
	}
	if !strings.Contains(entry.ExtraNotes, "different title or reason") {
		t.Fatalf("help must say how to reprint: %s", entry.ExtraNotes)
	}
}
