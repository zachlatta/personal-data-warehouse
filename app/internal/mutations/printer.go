package mutations

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// printer.print_document prints one PDF on Zach's home printer, the Brother
// MFC-L2710DW the scanner service also talks to. The executor is
// `pdw mutations printer` beside the scanner on mew-coolify
// (mutationworkers/printer): it checks the bytes against the sha256 stored
// here, counts the pages, turns them into PWG raster with Ghostscript and
// sends one IPP Print-Job with ipptool.
//
// The PDF travels inline as data_base64, like an email attachment: what a
// human approves is exactly what prints. Review reads drop the bytes
// (hydratePrintDocuments) and show the sha256, size and page count instead,
// so a 20 MiB request does not land in a review page or a push alert.
// One-sided only: the printer rotates the back of a duplex sheet, and that
// is unchecked on the real printer.

const (
	maxPrintDocumentBytes = 20 << 20
	maxPrintPages         = 200
	maxPrintCopies        = 20
	printDocumentSides    = "one-sided"
	printDocumentPrinter  = "Brother MFC-L2710DW (home)"
)

var printDocumentKeys = map[string]bool{
	"type": true, "operation": true, "account": true, "title": true, "reason": true,
	"filename": true, "content_type": true, "data_base64": true, "page_count": true, "copies": true,
}

// printDocument is a validated print request.
type printDocument struct {
	Filename  string
	Encoded   string
	Size      int
	SHA256    string
	PageCount int
	Copies    int
}

func printDocumentFromInput(mutation MutationInput) (printDocument, error) {
	raw := mutation.Raw
	var unknown []string
	for key := range raw {
		if !printDocumentKeys[key] {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return printDocument{}, fmt.Errorf("has unknown field(s) %s; %s takes filename, content_type, data_base64, page_count, copies, title and reason (one-sided only)", strings.Join(unknown, ", "), PrinterPrintDocumentOperation)
	}
	if account := normalizeAccount(mutation.Account); account != PrinterAccount {
		return printDocument{}, fmt.Errorf("account must be %q (the only printer), not %q", PrinterAccount, account)
	}
	name, _ := raw["filename"].(string)
	if strings.TrimSpace(name) == "" || len(name) > 255 || name == "." || name == ".." || strings.ContainsAny(name, "/\\") || strings.ContainsFunc(name, func(r rune) bool { return r < 32 || r == 127 }) {
		return printDocument{}, errors.New("needs a filename without paths or control characters (max 255 UTF-8 bytes)")
	}
	if contentType, _ := raw["content_type"].(string); strings.TrimSpace(contentType) != "application/pdf" {
		return printDocument{}, fmt.Errorf("content_type must be application/pdf, not %q: only PDFs print", contentType)
	}
	encoded, ok := raw["data_base64"].(string)
	if !ok || encoded == "" || len(encoded) > base64.StdEncoding.EncodedLen(maxPrintDocumentBytes) || strings.ContainsAny(encoded, "\r\n") {
		return printDocument{}, errors.New("needs data_base64: the PDF as standard base64 on one line, at most 20 MiB decoded")
	}
	data, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return printDocument{}, errors.New("data_base64 is not valid standard base64")
	}
	if !bytes.HasPrefix(data, []byte("%PDF-")) {
		return printDocument{}, errors.New("data_base64 is not a PDF (it does not start with %PDF-)")
	}
	pages, err := printWholeNumber(raw["page_count"], "page_count", 1, maxPrintPages, 0)
	if err != nil {
		return printDocument{}, err
	}
	if pages == 0 {
		return printDocument{}, fmt.Errorf("must include page_count, the PDF's page count (1 to %d); the printer refuses a PDF whose pages differ", maxPrintPages)
	}
	copies, err := printWholeNumber(raw["copies"], "copies", 1, maxPrintCopies, 1)
	if err != nil {
		return printDocument{}, err
	}
	sum := sha256.Sum256(data)
	return printDocument{
		Filename: name, Encoded: encoded, Size: len(data), SHA256: hex.EncodeToString(sum[:]),
		PageCount: pages, Copies: copies,
	}, nil
}

// printWholeNumber reads an optional whole number in [min, max]; absent is
// fallback.
func printWholeNumber(value any, field string, min, max, fallback int) (int, error) {
	invalid := fmt.Errorf("%s must be a whole number from %d to %d", field, min, max)
	var n float64
	switch typed := value.(type) {
	case nil:
		return fallback, nil
	case float64:
		n = typed
	case int:
		n = float64(typed)
	case int64:
		n = float64(typed)
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return 0, invalid
		}
		n = parsed
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		if err != nil {
			return 0, invalid
		}
		n = float64(parsed)
	default:
		return 0, invalid
	}
	if n != math.Trunc(n) || n < float64(min) || n > float64(max) {
		return 0, invalid
	}
	return int(n), nil
}

func validatePrintDocument(mutation MutationInput) error {
	_, err := printDocumentFromInput(mutation)
	return err
}

func printDocumentPayload(doc printDocument) map[string]any {
	return map[string]any{
		"filename":     doc.Filename,
		"content_type": "application/pdf",
		"data_base64":  doc.Encoded,
		"size_bytes":   doc.Size,
		"sha256":       doc.SHA256,
		"page_count":   doc.PageCount,
		"copies":       doc.Copies,
		"sides":        printDocumentSides,
	}
}

func printDocumentPreview(doc printDocument) map[string]any {
	return map[string]any{
		"printer":    printDocumentPrinter,
		"filename":   doc.Filename,
		"page_count": doc.PageCount,
		"copies":     doc.Copies,
		"sheets":     doc.PageCount * doc.Copies,
		"sides":      printDocumentSides,
		"paper":      "US Letter",
		"size_bytes": doc.Size,
		"sha256":     doc.SHA256,
		"effect":     "Prints these exact bytes once on the home Brother, in black and white. A job that may have printed is never sent again on its own.",
	}
}

func printDocumentTitle(doc printDocument) string {
	pages := fmt.Sprintf("%d page%s", doc.PageCount, plural(doc.PageCount))
	if doc.Copies > 1 {
		pages += fmt.Sprintf(" × %d copies", doc.Copies)
	}
	return fmt.Sprintf("Print %s (%s) on the home Brother", doc.Filename, pages)
}

// hydratePrintDocuments drops the PDF's bytes from what a review read
// returns. The stored payload keeps them for the worker; the reviewer sees
// the sha256, size and pages the preview carries.
func hydratePrintDocuments(mutations []Mutation) []Mutation {
	for index := range mutations {
		if mutations[index].Operation != PrinterPrintDocumentOperation {
			continue
		}
		payload := make(map[string]any, len(mutations[index].Payload))
		for key, value := range mutations[index].Payload {
			if key != "data_base64" {
				payload[key] = value
			}
		}
		mutations[index].Payload = payload
	}
	return mutations
}
