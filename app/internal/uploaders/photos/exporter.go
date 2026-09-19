package photos

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// The native PhotoKit helper. The Photos library's originals/ tree is only a
// local cache when Optimize Mac Storage is enabled; the helper has an
// embedded privacy usage description, so macOS can grant it stable Photos
// access, and it requests original resources with iCloud network access
// enabled.

//go:embed macos/PhotoExporter.swift
var helperSwiftSource []byte

//go:embed macos/Info.plist
var helperInfoPlist []byte

const (
	HelperIdentifier     = "com.zachlatta.pdw.photos-exporter"
	HelperAppName        = "PDW Photos Exporter.app"
	HelperExecutableName = "pdw-photos-exporter"
	HelperBuildTimeout   = 120 * time.Second
	// PhotosAuthorizedStatus is PHAuthorizationStatus.authorized.
	PhotosAuthorizedStatus = 3
	// DefaultExportTimeout bounds one PhotoKit export (iCloud download included).
	DefaultExportTimeout = 3600 * time.Second
)

// ExportError means PhotoKit could not produce a complete original resource.
type ExportError struct{ Message string }

func (e *ExportError) Error() string { return e.Message }

func exportErrorf(format string, args ...any) error {
	return &ExportError{Message: fmt.Sprintf(format, args...)}
}

// ExportedFile is one exported original on local disk.
type ExportedFile struct {
	Path      string
	Filename  string
	Extension string
	MimeType  string
	SizeBytes int64
}

// Exporter is what the runner needs: export one candidate's full original
// into destinationDir. Tests inject a fake; production uses PhotoKitExporter.
type Exporter interface {
	Export(candidate Candidate, destinationDir string) (ExportedFile, error)
}

// CommandResult is what a CommandRunner observed of the `open` launch.
type CommandResult struct {
	ReturnCode int
	Stdout     string
	Stderr     string
}

// ErrCommandTimeout is what a CommandRunner returns when the launch itself
// did not finish within the timeout.
var ErrCommandTimeout = errors.New("command timed out")

// CommandRunner runs the LaunchServices `open` command. Tests substitute one
// that writes the redirected result files the helper would have written.
type CommandRunner func(command []string, timeout time.Duration) (CommandResult, error)

// PhotoKitExporter synchronously exports one original PHAssetResource,
// downloading it from iCloud as needed, through the native helper app.
type PhotoKitExporter struct {
	// Timeout bounds one helper invocation; zero means DefaultExportTimeout.
	Timeout time.Duration
	// HelperPath is a prebuilt helper app bundle; empty means build (or
	// reuse) the cached one under Application Support.
	HelperPath string
	// CommandRunner runs `open`; nil means exec it.
	CommandRunner CommandRunner
	// LookPath resolves `open`; nil means exec.LookPath.
	LookPath func(string) (string, error)
}

func (e *PhotoKitExporter) timeout() time.Duration {
	if e.Timeout > 0 {
		return e.Timeout
	}
	return DefaultExportTimeout
}

// Export exports the candidate's original resource into destinationDir.
func (e *PhotoKitExporter) Export(candidate Candidate, destinationDir string) (ExportedFile, error) {
	if err := os.MkdirAll(destinationDir, 0o755); err != nil {
		return ExportedFile{}, err
	}
	destination := filepath.Join(destinationDir, safeID(candidate.NativeID)+"-"+candidate.Role+candidate.Extension)
	payload, err := e.runHelper(
		"export",
		"--uuid", candidate.NativeID,
		"--role", candidate.Role,
		"--kind", candidate.AssetKind,
		"--filename", candidate.Filename,
		"--destination", destination,
	)
	if err != nil {
		return ExportedFile{}, err
	}
	filename := payloadString(payload, "filename")
	if filename == "" {
		filename = candidate.Filename
	}
	extension := suffix(filename)
	if extension == "" {
		extension = candidate.Extension
	}
	mime := mimeType(payloadString(payload, "uti"), extension)
	info, err := os.Stat(destination)
	if err != nil {
		return ExportedFile{}, exportErrorf("PhotoKit reported success but did not export %s", candidate.Filename)
	}
	sizeBytes := info.Size()
	if sizeBytes <= 0 {
		os.Remove(destination)
		return ExportedFile{}, exportErrorf("PhotoKit exported an empty original for %s", candidate.Filename)
	}
	resourceSize := common.ToInt(payload["size_bytes"])
	expected := candidate.ExpectedSizeBytes
	if expected == 0 {
		expected = resourceSize
	}
	if expected != 0 && sizeBytes != expected {
		os.Remove(destination)
		return ExportedFile{}, exportErrorf("PhotoKit exported %d bytes for %s, but Photos metadata says the full original is %d bytes",
			sizeBytes, candidate.Filename, expected)
	}
	return ExportedFile{Path: destination, Filename: filename, Extension: extension, MimeType: mime, SizeBytes: sizeBytes}, nil
}

// RequestAuthorization prompts for one-time Full Photos access and returns
// the resulting PHAuthorizationStatus.
func (e *PhotoKitExporter) RequestAuthorization() (int64, error) {
	payload, err := e.runHelper("authorize")
	if err != nil {
		return -1, err
	}
	return statusOf(payload), nil
}

// AuthorizationStatus returns the current PhotoKit read/write status.
func (e *PhotoKitExporter) AuthorizationStatus() (int64, error) {
	payload, err := e.runHelper("status")
	if err != nil {
		return -1, err
	}
	return statusOf(payload), nil
}

func statusOf(payload map[string]any) int64 {
	if _, ok := payload["status"]; !ok {
		return -1
	}
	return common.ToInt(payload["status"])
}

func payloadString(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case bool:
		if !v {
			return ""
		}
		return "True"
	case float64:
		if v == 0 {
			return ""
		}
		return common.FloatRepr(v)
	default:
		return fmt.Sprint(v)
	}
}

// safeID mirrors the Python destination-name sanitizer: alphanumerics and
// "._-" pass through, everything else becomes "_".
func safeID(nativeID string) string {
	var b strings.Builder
	for _, r := range nativeID {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func (e *PhotoKitExporter) runHelper(arguments ...string) (map[string]any, error) {
	helper := e.HelperPath
	if helper == "" {
		built, err := BuildHelper("")
		if err != nil {
			return nil, err
		}
		helper = built
	}
	lookPath := e.LookPath
	if lookPath == nil {
		lookPath = exec.LookPath
	}
	openCommand, err := lookPath("open")
	if err != nil || openCommand == "" {
		return nil, exportErrorf("Could not find the macOS LaunchServices `open` command")
	}
	runner := e.CommandRunner
	if runner == nil {
		runner = execCommand
	}
	timeout := e.timeout()
	timeoutText := strconv.FormatFloat(timeout.Seconds(), 'g', -1, 64)

	// Always launch the app bundle through LaunchServices. Directly execing
	// a command-line Mach-O makes TCC attribute Photos access to its
	// responsible parent (Ghostty interactively, launchd when scheduled), so
	// the two contexts observe different grants.
	invocationDir, err := os.MkdirTemp("", "pdw-photos-invoke-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(invocationDir)
	stdoutPath := filepath.Join(invocationDir, "stdout.json")
	stderrPath := filepath.Join(invocationDir, "stderr.txt")
	command := append([]string{openCommand, "-n", "-j", "--stdout", stdoutPath, "--stderr", stderrPath, helper, "--args"}, arguments...)
	result, err := runner(command, timeout)
	if err != nil {
		if errors.Is(err, ErrCommandTimeout) {
			return nil, exportErrorf("Timed out after %ss waiting for Apple Photos/iCloud", timeoutText)
		}
		return nil, err
	}
	// `open -W` has a race when a short-lived app exits before `open`
	// installs its process watcher ("initial call to kevent() failed: No
	// such process"). Launch the app asynchronously, then wait on the
	// redirected result channel that the app itself owns.
	deadline := time.Now().Add(timeout)
	helperStdout, helperStderr := "", ""
	for result.ReturnCode == 0 {
		helperStdout = readFileIfExists(stdoutPath)
		helperStderr = readFileIfExists(stderrPath)
		if strings.TrimSpace(helperStdout) != "" || strings.TrimSpace(helperStderr) != "" {
			break
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, exportErrorf("Timed out after %ss waiting for Apple Photos/iCloud", timeoutText)
		}
		if remaining > 50*time.Millisecond {
			remaining = 50 * time.Millisecond
		}
		time.Sleep(remaining)
	}
	if result.ReturnCode != 0 || strings.TrimSpace(helperStdout) == "" {
		detail := common.FirstNonEmpty(helperStderr, result.Stderr, result.Stdout)
		if detail == "" {
			detail = "unknown PhotoKit error"
		}
		return nil, &ExportError{Message: detail}
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(helperStdout), &payload); err != nil || payload == nil {
		return nil, exportErrorf("The native PhotoKit helper returned invalid output")
	}
	return payload, nil
}

func readFileIfExists(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}

// execCommand is the production CommandRunner.
func execCommand(command []string, timeout time.Duration) (CommandResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return CommandResult{}, ErrCommandTimeout
	}
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		} else {
			return CommandResult{}, err
		}
	}
	return CommandResult{ReturnCode: code, Stdout: stdout.String(), Stderr: stderr.String()}, nil
}

// HelperSourceDigest is the sha256 the helper build is stamped with: the
// embedded Swift source and Info.plist, NUL-separated.
func HelperSourceDigest() string {
	digest := sha256.New()
	digest.Write(helperSwiftSource)
	digest.Write([]byte{0})
	digest.Write(helperInfoPlist)
	return hex.EncodeToString(digest.Sum(nil))
}

// DefaultHelperRoot is where the helper app bundle is cached.
func DefaultHelperRoot() string {
	return filepath.Join(common.ApplicationSupportDir(), "photos-helper")
}

// BuildHelper builds and caches the hidden app bundle with a stable PhotoKit
// identity, rebuilding only when the embedded sources change. It returns the
// app bundle path.
func BuildHelper(destinationDir string) (string, error) {
	if runtime.GOOS != "darwin" {
		return "", exportErrorf("Apple Photos export is only available on macOS")
	}
	digest := HelperSourceDigest()
	root := destinationDir
	if root == "" {
		root = DefaultHelperRoot()
	}
	appBundle := filepath.Join(root, HelperAppName)
	binary := filepath.Join(appBundle, "Contents", "MacOS", HelperExecutableName)
	stamp := filepath.Join(root, "source.sha256")
	lockPath := filepath.Join(root, "build.lock")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", err
	}
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return "", err
	}
	defer lockFile.Close()
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return "", err
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)

	if isFile(binary) && isFile(stamp) && strings.TrimSpace(readFileIfExists(stamp)) == digest {
		return appBundle, nil
	}
	xcrun, err := exec.LookPath("xcrun")
	codesign, err2 := exec.LookPath("codesign")
	if err != nil || err2 != nil {
		return "", exportErrorf("Building the Apple Photos helper requires the macOS Command Line Tools")
	}
	temporary, err := os.MkdirTemp(root, "pdw-photos-helper-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(temporary)
	temporaryApp := filepath.Join(temporary, HelperAppName)
	temporaryContents := filepath.Join(temporaryApp, "Contents")
	temporaryMacOS := filepath.Join(temporaryContents, "MacOS")
	if err := os.MkdirAll(temporaryMacOS, 0o755); err != nil {
		return "", err
	}
	temporaryBinary := filepath.Join(temporaryMacOS, HelperExecutableName)
	if err := os.WriteFile(filepath.Join(temporaryContents, "Info.plist"), helperInfoPlist, 0o644); err != nil {
		return "", err
	}
	source := filepath.Join(temporary, "PhotoExporter.swift")
	if err := os.WriteFile(source, helperSwiftSource, 0o644); err != nil {
		return "", err
	}
	if out, err := runBounded(HelperBuildTimeout, xcrun, "swiftc", "-O", source, "-o", temporaryBinary); err != nil {
		return "", exportErrorf("Could not compile the native Apple Photos helper: %s", out)
	}
	if out, err := runBounded(HelperBuildTimeout, codesign, "--force", "--sign", "-", "--identifier", HelperIdentifier, temporaryApp); err != nil {
		return "", exportErrorf("Could not sign the native Apple Photos helper: %s", out)
	}
	if err := os.RemoveAll(appBundle); err != nil {
		return "", err
	}
	if err := os.Rename(temporaryApp, appBundle); err != nil {
		return "", err
	}
	temporaryStamp := filepath.Join(temporary, filepath.Base(stamp))
	if err := os.WriteFile(temporaryStamp, []byte(digest+"\n"), 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(temporaryStamp, stamp); err != nil {
		return "", err
	}
	// Completely replace the original loose-binary flow so TCC can no longer
	// fall back to terminal/launchd attribution.
	os.Remove(filepath.Join(root, HelperExecutableName))
	return appBundle, nil
}

func runBounded(timeout time.Duration, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	detail := strings.TrimSpace(stderr.String())
	if detail == "" {
		detail = strings.TrimSpace(stdout.String())
	}
	if err != nil && detail == "" {
		detail = err.Error()
	}
	return detail, err
}

func isFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
