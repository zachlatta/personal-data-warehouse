package voicememos

import (
	"bytes"
	"compress/flate"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// hostOS is runtime.GOOS; a package var so the macOS-only paths are testable
// on the Linux CI runner.
var hostOS = runtime.GOOS

// Renames must be real Core Data saves, not raw SQL: CloudRecordings.db
// belongs to NSPersistentCloudKitContainer (mirrored by voicememod), and only
// a save that records persistent history is exported to CloudKit and synced
// to the other devices. A direct sqlite UPDATE would stay local and be
// clobbered by the next import.
//
// Go cannot drive Core Data, so the save is done by a small Swift helper
// (macos/VoiceMemosWriteback.swift, embedded below) compiled on demand with
// swiftc and cached under Application Support, rebuilt only when the embedded
// source changes -- the same arrangement the photos exporter uses for its
// PhotoKit helper. The helper does exactly what the Python store_writer did:
// model from the store's own Z_MODELCACHE, migration disabled, persistent
// history tracking + remote change notification, transaction author
// TransactionAuthor, every row re-checked as still auto-named, one save.

// Status values the writer reports per plan item.
const (
	StatusRenamed             = "renamed"
	StatusWouldRename         = "would_rename"
	StatusSkippedNotAutoNamed = "skipped_not_auto_named"
	StatusSkippedMissing      = "skipped_missing"
)

//go:embed macos/VoiceMemosWriteback.swift
var writebackHelperSource []byte

const (
	helperExecutableName    = "voice-memos-writeback"
	helperBuildTimeout      = 10 * time.Minute
	helperRunTimeout        = 5 * time.Minute
	helperDirectoryName     = "voice-memos-writeback"
	helperSourceStampName   = "source.sha256"
	helperBuildLockFileName = "build.lock"
)

// LoadCachedModelData reads the archived managed object model cached inside
// the store. Core Data stores it as a raw-DEFLATE-compressed keyed archive in
// Z_MODELCACHE (Apple's COMPRESSION_ZLIB is raw deflate, no zlib header).
func LoadCachedModelData(storePath string) ([]byte, error) {
	db, err := common.OpenSQLiteReadOnly(storePath)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := common.Query(db, "SELECT Z_CONTENT FROM Z_MODELCACHE LIMIT 1")
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 || rows[0].Get("Z_CONTENT") == nil {
		return nil, fmt.Errorf("store has no cached managed object model: %s", storePath)
	}
	blob := rows[0].Bytes("Z_CONTENT")
	if blob == nil {
		return nil, fmt.Errorf("store has no cached managed object model: %s", storePath)
	}
	reader := flate.NewReader(bytes.NewReader(blob))
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("could not decode cached model archive: %w", err)
	}
	return data, nil
}

// HelperSourceSHA256 is the embedded helper source's digest, the cache key
// for the compiled binary.
func HelperSourceSHA256() string {
	sum := sha256.Sum256(writebackHelperSource)
	return hex.EncodeToString(sum[:])
}

// DefaultHelperDir is where the compiled helper lives:
// ~/Library/Application Support/personal-data-warehouse/voice-memos-writeback.
func DefaultHelperDir() string {
	return filepath.Join(common.ApplicationSupportDir(), helperDirectoryName)
}

// HelperBinaryPath is the compiled helper inside root.
func HelperBinaryPath(root string) string {
	return filepath.Join(root, helperExecutableName)
}

// HelperIsCurrent reports whether root holds a binary built from the embedded
// source (the stamp file matches the source digest).
func HelperIsCurrent(root string) bool {
	info, err := os.Stat(HelperBinaryPath(root))
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	stamp, err := os.ReadFile(filepath.Join(root, helperSourceStampName))
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(stamp)) == HelperSourceSHA256()
}

// BuildWritebackHelper compiles the embedded Swift helper into root when the
// cached binary is missing or stale, and returns the binary path. compile is
// the swiftc invocation (nil for the real one); the build lock serialises
// concurrent uploader runs.
func BuildWritebackHelper(root string, compile func(source, output string) error) (string, error) {
	if hostOS != "darwin" {
		return "", errors.New("Voice Memos write-back is only available on macOS")
	}
	if compile == nil {
		compile = compileSwiftHelper
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", err
	}
	lockFile, err := os.OpenFile(filepath.Join(root, helperBuildLockFileName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return "", err
	}
	defer lockFile.Close()
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return "", err
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
	binary := HelperBinaryPath(root)
	if HelperIsCurrent(root) {
		return binary, nil
	}
	temporary, err := os.MkdirTemp(root, "pdw-voice-memos-writeback-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(temporary)
	source := filepath.Join(temporary, "VoiceMemosWriteback.swift")
	if err := os.WriteFile(source, writebackHelperSource, 0o644); err != nil {
		return "", err
	}
	output := filepath.Join(temporary, helperExecutableName)
	if err := compile(source, output); err != nil {
		return "", err
	}
	if err := os.Rename(output, binary); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(root, helperSourceStampName), []byte(HelperSourceSHA256()+"\n"), 0o644); err != nil {
		return "", err
	}
	return binary, nil
}

func compileSwiftHelper(source, output string) error {
	xcrun, err := exec.LookPath("xcrun")
	if err != nil {
		return errors.New("Building the Voice Memos write-back helper requires the macOS Command Line Tools")
	}
	cmd := exec.Command(xcrun, "swiftc", "-O", source, "-o", output)
	var stderr, stdout bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout
	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			detail := strings.TrimSpace(stderr.String())
			if detail == "" {
				detail = strings.TrimSpace(stdout.String())
			}
			return fmt.Errorf("Could not compile the Voice Memos write-back helper: %s", detail)
		}
		return nil
	case <-time.After(helperBuildTimeout):
		_ = cmd.Process.Kill()
		return errors.New("Could not compile the Voice Memos write-back helper: swiftc timed out")
	}
}

// helperRequest is the JSON the helper reads on stdin.
type helperRequest struct {
	StorePath string           `json:"store_path"`
	ModelPath string           `json:"model_path"`
	Author    string           `json:"author"`
	DryRun    bool             `json:"dry_run"`
	Items     []RenamePlanItem `json:"items"`
}

// DefaultStoreWriter applies the plan through the Swift helper.
func DefaultStoreWriter(storePath string, items []RenamePlanItem, author string, dryRun bool) ([]WriteResult, error) {
	binary, err := BuildWritebackHelper(DefaultHelperDir(), nil)
	if err != nil {
		return nil, err
	}
	return RunStoreWriterHelper(binary, storePath, items, author, dryRun)
}

// RunStoreWriterHelper invokes a built helper: the model archive is inflated
// here (LoadCachedModelData) and handed over as a temp file, the plan goes in
// on stdin, and the results come back as a JSON array on stdout.
func RunStoreWriterHelper(binary, storePath string, items []RenamePlanItem, author string, dryRun bool) ([]WriteResult, error) {
	if author == "" {
		author = TransactionAuthor
	}
	model, err := LoadCachedModelData(storePath)
	if err != nil {
		return nil, err
	}
	modelFile, err := os.CreateTemp("", "pdw-voice-memos-model-*.plist")
	if err != nil {
		return nil, err
	}
	defer os.Remove(modelFile.Name())
	if _, err := modelFile.Write(model); err != nil {
		modelFile.Close()
		return nil, err
	}
	if err := modelFile.Close(); err != nil {
		return nil, err
	}
	if items == nil {
		items = []RenamePlanItem{}
	}
	request, err := json.Marshal(helperRequest{StorePath: storePath, ModelPath: modelFile.Name(), Author: author, DryRun: dryRun, Items: items})
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(binary)
	cmd.Stdin = bytes.NewReader(request)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			detail := strings.TrimSpace(stderr.String())
			if detail == "" {
				detail = err.Error()
			}
			return nil, fmt.Errorf("Voice Memos store writer failed: %s", detail)
		}
	case <-time.After(helperRunTimeout):
		_ = cmd.Process.Kill()
		return nil, errors.New("Voice Memos store writer timed out")
	}
	var results []WriteResult
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &results); err != nil {
		return nil, fmt.Errorf("Voice Memos store writer returned invalid output: %w", err)
	}
	if results == nil {
		results = []WriteResult{}
	}
	return results, nil
}
