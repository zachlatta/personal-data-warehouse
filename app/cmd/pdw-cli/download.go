package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
)

// getObjectResponse mirrors the fields of the server's get_object output that
// the download command consumes.
type getObjectResponse struct {
	Exists        bool   `json:"exists"`
	Filename      string `json:"filename"`
	SizeBytes     int64  `json:"size_bytes"`
	ContentSHA256 string `json:"content_sha256"`
	DownloadURL   string `json:"download_url"`
	ExpiresAt     string `json:"expires_at"`
	Error         string `json:"error"`
}

// runDownload fetches a stored blob to a local file: it calls get_object for
// the signed download link, then GETs that link (the signature is the
// credential, so no bearer header is sent).
func runDownload(client *cliclient.Client, args []string, stdout, stderr io.Writer) int {
	// The storage_file_id may appear before or after --output, so pull it out
	// the same way `call` extracts its tool name before parsing flags.
	fileID, rest, err := extractToolName(args)
	if err != nil {
		fmt.Fprintln(stderr, "pdw download: storage_file_id is required (usage: pdw download <storage_file_id> [--output PATH])")
		return 2
	}
	fileID = strings.TrimSpace(fileID)
	fs := flag.NewFlagSet("download", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	output := fs.String("output", "", "destination path (defaults to the object's filename)")
	if err := fs.Parse(rest); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stdout, usage)
			return 0
		}
		fmt.Fprintln(stderr, "pdw download:", err)
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintln(stderr, "pdw download: unexpected extra arguments; pass a single storage_file_id")
		return 2
	}

	input, err := json.Marshal(map[string]string{"storage_file_id": fileID})
	if err != nil {
		fmt.Fprintln(stderr, "pdw download:", err)
		return 1
	}
	out, err := client.CallTool(context.Background(), "get_object", input)
	if err != nil {
		var apiErr *cliclient.APIError
		if errors.As(err, &apiErr) {
			fmt.Fprintf(stderr, "pdw download: %s (http %d): %s\n", apiErr.Code, apiErr.Status, apiErr.Message)
			return 1
		}
		fmt.Fprintln(stderr, "pdw download:", err)
		return 1
	}
	var obj getObjectResponse
	if err := json.Unmarshal(out, &obj); err != nil {
		fmt.Fprintln(stderr, "pdw download: unexpected get_object response:", err)
		return 1
	}
	if obj.Error != "" {
		fmt.Fprintln(stderr, "pdw download:", obj.Error)
		return 1
	}
	if !obj.Exists {
		fmt.Fprintf(stderr, "pdw download: no stored object with storage_file_id %q\n", fileID)
		return 1
	}
	if obj.DownloadURL == "" {
		fmt.Fprintln(stderr, "pdw download: server returned no download_url; is it running an older version?")
		return 1
	}

	dest := strings.TrimSpace(*output)
	if dest == "" {
		dest = downloadFilename(obj.Filename, fileID)
	}

	data, err := fetchDownloadURL(obj.DownloadURL)
	if err != nil {
		fmt.Fprintln(stderr, "pdw download:", err)
		return 1
	}
	if obj.ContentSHA256 != "" {
		sum := sha256.Sum256(data)
		if got := hex.EncodeToString(sum[:]); !strings.EqualFold(got, obj.ContentSHA256) {
			fmt.Fprintf(stderr, "pdw download: content hash mismatch: got %s, want %s\n", got, obj.ContentSHA256)
			return 1
		}
	}
	if err := os.WriteFile(dest, data, 0o644); err != nil {
		fmt.Fprintln(stderr, "pdw download:", err)
		return 1
	}
	fmt.Fprintf(stdout, "saved %s (%d bytes)\n", dest, len(data))
	return 0
}

func fetchDownloadURL(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("download failed (http %d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return io.ReadAll(resp.Body)
}

// downloadFilename picks a safe local filename: the object's own filename
// stripped of any path components, falling back to the storage file ID.
func downloadFilename(filename, fileID string) string {
	base := filepath.Base(strings.TrimSpace(filename))
	if base == "" || base == "." || base == string(filepath.Separator) {
		return fileID
	}
	return base
}
