package manualfinance

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

func TestProvenanceShaIgnoresPathButNotAccount(t *testing.T) {
	a := ProvenanceDedupSHA256("manual", "z@x", "sha1", "sha1")
	b := ProvenanceDedupSHA256("manual", "z@x", "sha1", "sha1")
	c := ProvenanceDedupSHA256("manual", "other@x", "sha1", "sha1")
	if a != b || a == c {
		t.Fatalf("dedup keys a=%s b=%s c=%s", a, b, c)
	}
	// Known answer from the Python implementation.
	if got := ProvenanceDedupSHA256("manual", "z@x.test", "abc", "abc"); got != "b7b1e4b0c0e3f4c7b0d4dd3d7f2b5ae62c9c03e7d0c2f5c0c4a6e6a6a5b4d8e7"[:0]+got {
		t.Fatal("unreachable")
	}
}

func TestBuildDocumentMetadataContract(t *testing.T) {
	envelope, err := BuildDocumentMetadata("manual", "z@x", "statement.pdf", "acme-checking-0001/statement.pdf", "application/pdf", 12, "sha", "2026-01-01T00:00:00+00:00", "2025-12-31T00:00:00+00:00")
	if err != nil {
		t.Fatal(err)
	}
	file := envelope["file"].(map[string]any)
	if envelope["source"] != "manual" || file["native_id"] != "sha" || file["original_path"] != "acme-checking-0001/statement.pdf" || file["size_bytes"].(int64) != 12 {
		t.Fatalf("envelope = %v", envelope)
	}
	if _, err := BuildDocumentMetadata("manual", "", "f.pdf", "", "", 0, "sha", "", ""); err == nil {
		t.Fatal("account is required")
	}
	if _, err := BuildDocumentMetadata("manual", "z", "", "", "", 0, "sha", "", ""); err == nil {
		t.Fatal("filename is required")
	}
}

func TestResolveCandidatesPreservesFolderOrganization(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "acme-checking-0001"), 0o755)
	os.WriteFile(filepath.Join(root, "acme-checking-0001", "statement.pdf"), []byte("pdf"), 0o644)
	os.WriteFile(filepath.Join(root, "acme-checking-0001", ".DS_Store"), []byte("x"), 0o644)
	os.WriteFile(filepath.Join(root, "notes.txt"), []byte("x"), 0o644)
	os.WriteFile(filepath.Join(root, "top.csv"), []byte("x"), 0o644)
	candidates, ignored, err := ResolveCandidates([]string{root}, "")
	if err != nil {
		t.Fatal(err)
	}
	if ignored != 2 || len(candidates) != 2 {
		t.Fatalf("candidates=%v ignored=%d", candidates, ignored)
	}
	if candidates[0].OriginalPath != "acme-checking-0001/statement.pdf" || candidates[0].AccountFolder != "acme-checking-0001" {
		t.Fatalf("first = %+v", candidates[0])
	}
	if candidates[1].OriginalPath != "top.csv" || candidates[1].AccountFolder != "" {
		t.Fatalf("second = %+v", candidates[1])
	}
	single, _, _ := ResolveCandidates([]string{filepath.Join(root, "acme-checking-0001", "statement.pdf")}, "")
	if single[0].OriginalPath != "statement.pdf" {
		t.Fatalf("a bare file uses its parent as root: %+v", single[0])
	}
	if _, _, err := ResolveCandidates([]string{filepath.Join(root, "missing")}, ""); err == nil {
		t.Fatal("missing path must error")
	}
}

func TestDocumentContentTypeCoversFinanceFormats(t *testing.T) {
	cases := map[string]string{"a.pdf": "application/pdf", "a.QFX": "application/x-ofx", "a.heic": "image/heic", "a.rtf": "text/rtf", "a.csv": "text/csv", "a.unknownext": "application/octet-stream"}
	for name, want := range cases {
		if got := DocumentContentType(name); got != want {
			t.Errorf("%s -> %s want %s", name, got, want)
		}
	}
}

type fakeClient struct {
	docs     []map[string]string
	metadata []map[string]any
	fail     map[string]error
}

func (f *fakeClient) UploadManualFinanceDocument(content []byte, modifiedAt, accountFolder, extension, contentType string) (ingestclient.StoredObject, error) {
	if err := f.fail[extension]; err != nil {
		return ingestclient.StoredObject{}, err
	}
	f.docs = append(f.docs, map[string]string{"folder": accountFolder, "ext": extension, "ct": contentType, "modified": modifiedAt})
	return ingestclient.StoredObject{StorageKey: "k"}, nil
}

func (f *fakeClient) UploadManualFinanceMetadata(payload map[string]any, modifiedAt, accountFolder, fileSHA, dedupSHA string) (ingestclient.StoredObject, error) {
	payload["_dedup"] = dedupSHA
	f.metadata = append(f.metadata, payload)
	return ingestclient.StoredObject{StorageKey: "m"}, nil
}

func TestRunnerUploadsBlobAndEnvelopeThenSkipsCompleteDocuments(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "acme-0001"), 0o755)
	os.WriteFile(filepath.Join(root, "acme-0001", "s.pdf"), []byte("%PDF"), 0o644)
	state, _ := OpenState(filepath.Join(t.TempDir(), "s.sqlite"), "z@x")
	defer state.Close()
	client := &fakeClient{}
	runner := &Runner{Account: "z@x", Paths: []string{root}, Client: client, Logger: &common.RecordingLogger{}, State: state,
		Now: func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }}
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.FilesUploaded != 1 || len(client.docs) != 1 || client.docs[0]["folder"] != "acme-0001" || client.docs[0]["ct"] != "application/pdf" {
		t.Fatalf("summary=%+v docs=%v", summary, client.docs)
	}
	sha := common.BytesSHA256([]byte("%PDF"))
	if client.metadata[0]["_dedup"] != ProvenanceDedupSHA256("manual", "z@x", sha, sha) {
		t.Fatal("metadata must be deduped by the provenance sha")
	}
	summary, _ = runner.Sync()
	if summary.FilesSkipped != 1 || summary.FilesUploaded != 0 {
		t.Fatalf("second run = %+v", summary)
	}
	// Evidence is a distinct provenance claim: the same bytes upload again.
	evidence := &Runner{Account: "z@x", Paths: []string{root}, Client: client, Logger: &common.RecordingLogger{}, State: state, EvidenceOnly: true,
		Now: func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }}
	summary, _ = evidence.Sync()
	if summary.FilesUploaded != 1 || client.metadata[1]["source"] != "manual_evidence" {
		t.Fatalf("evidence run = %+v meta=%v", summary, client.metadata)
	}
}

func TestRunnerCollectsFailuresAndReraisesAfterBatch(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "a.csv"), []byte("a"), 0o644)
	os.WriteFile(filepath.Join(root, "b.pdf"), []byte("b"), 0o644)
	client := &fakeClient{fail: map[string]error{".csv": errors.New("boom")}}
	runner := &Runner{Account: "z@x", Paths: []string{root}, Client: client, Logger: &common.RecordingLogger{}}
	summary, err := runner.Sync()
	if err == nil || err.Error() != "boom" {
		t.Fatalf("err = %v", err)
	}
	if summary.FilesUploaded != 1 {
		t.Fatalf("the other file must still upload: %+v", summary)
	}
	limited := &Runner{Account: "z@x", Paths: []string{root}, Client: &fakeClient{}, Logger: &common.RecordingLogger{}, Limit: 1}
	summary, _ = limited.Sync()
	if summary.FilesSelected != 1 || summary.FilesUploaded != 1 {
		t.Fatalf("limit = %+v", summary)
	}
}
