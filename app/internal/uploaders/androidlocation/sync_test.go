package androidlocation

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

type fakeUploader struct {
	fail  bool
	calls int
}

func (f *fakeUploader) UploadAndroidLocationBatch(b []byte, s string) (ingestclient.StoredObject, error) {
	f.calls++
	if f.fail {
		return ingestclient.StoredObject{}, errors.New("offline")
	}
	return ingestclient.StoredObject{StorageKey: "stored"}, nil
}
func TestOfflineRetryAndDryRun(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "001.json")
	if err := os.WriteFile(p, []byte(`{"sample_id":"fake-1","latitude":0,"longitude":0,"accuracy_m":500,"provider":"network","measured_at_ms":1700000000000}`), 0600); err != nil {
		t.Fatal(err)
	}
	c := &fakeUploader{fail: true}
	r := Runner{SpoolDir: dir, Account: "test@example.invalid", Device: "fake", Client: c}
	if _, _, err := r.Sync(); err == nil {
		t.Fatal("wanted offline failure")
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatal("offline lost sample")
	}
	r.DryRun = true
	if n, _, err := r.Sync(); err != nil || n != 1 {
		t.Fatalf("dry run %d %v", n, err)
	}
	if c.calls != 1 {
		t.Fatal("dry run uploaded")
	}
	r.DryRun = false
	c.fail = false
	if n, _, err := r.Sync(); err != nil || n != 1 {
		t.Fatalf("retry %d %v", n, err)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatal("acknowledged file retained")
	}
}
func TestRefusesGPSAndMalformedSamples(t *testing.T) {
	for _, rec := range []map[string]any{{}, {"sample_id": "x", "provider": "gps"}, {"sample_id": "x", "provider": "network", "latitude": 91.0}} {
		if Validate(rec) == nil {
			t.Fatalf("invalid sample accepted: %v", rec)
		}
	}
}

func TestCLIDryRunDoesNotAcquireOrDelete(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "001.json")
	os.WriteFile(p, []byte(`{"sample_id":"fake-1","latitude":0,"longitude":0,"accuracy_m":500,"provider":"network","measured_at_ms":1700000000000}`), 0600)
	// A fake content executable would mark any acquisition attempt. PATH is
	// process-wide so this test is deliberately not parallel.
	marker := filepath.Join(dir, "acquired")
	os.WriteFile(filepath.Join(dir, "content"), []byte("#!/bin/sh\ntouch '"+marker+"'\n"), 0700)
	t.Setenv("PATH", dir)
	env := func(k string) string {
		switch k {
		case "PDW_INGEST_PROJECT_DIR":
			return dir
		}
		return ""
	}
	var out bytes.Buffer
	code := Run([]string{"--spool-dir", dir, "--lock-file", filepath.Join(dir, "lock"), "--account", "test@example.invalid", "--dry-run"}, strings.NewReader(""), &out, &out, env, ingestclient.Config{})
	if code != 0 {
		t.Fatalf("dry-run %d: %s", code, out.String())
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("dry-run acquired location")
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatal("dry-run deleted queued file")
	}
}

func TestAcquisitionFailureReturnsNonzeroAfterDrainingEmptyQueue(t *testing.T) {
	for _, script := range []string{`#!/bin/sh
exit 0
`, `#!/bin/sh
echo 'Error while accessing provider'
`, `#!/bin/sh
echo '{"queued":""}'
`, "#!/bin/sh\nexit 1\n", "#!/bin/sh\necho 'Error while accessing provider' >&2\nexit 0\n"} {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "content"), []byte(script), 0700)
		t.Setenv("PATH", dir)
		var out bytes.Buffer
		env := func(k string) string {
			if k == "PDW_INGEST_PROJECT_DIR" {
				return dir
			}
			return ""
		}
		code := Run([]string{"--spool-dir", dir, "--lock-file", filepath.Join(dir, "lock"), "--account", "test@example.invalid"}, strings.NewReader(""), &out, &out, env, ingestclient.Config{BaseURL: "http://127.0.0.1:1", Token: "fake"})
		if code == 0 || !strings.Contains(out.String(), "pdw ingest android-location: ") || !strings.Contains(out.String(), "queued samples drained") {
			t.Fatalf("failure reported success: %d %s", code, out.String())
		}
	}
}

func TestAcquisitionAcknowledgementSucceeds(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "content"), []byte(`#!/bin/sh
echo '{"queued":"fake-sample.json"}'
`), 0700)
	t.Setenv("PATH", dir)
	env := func(k string) string {
		if k == "PDW_INGEST_PROJECT_DIR" {
			return dir
		}
		return ""
	}
	var out bytes.Buffer
	code := Run([]string{"--spool-dir", dir, "--lock-file", filepath.Join(dir, "lock"), "--account", "test@example.invalid"}, strings.NewReader(""), &out, &out, env, ingestclient.Config{BaseURL: "http://127.0.0.1:1", Token: "fake"})
	if code != 0 {
		t.Fatalf("acknowledged acquisition failed: %d %s", code, out.String())
	}
}

func TestTransientAcquisitionMissesToleratedUntilThresholdThenAlert(t *testing.T) {
	if DefaultTransientFailureThreshold != 18 {
		t.Fatalf("DefaultTransientFailureThreshold = %d, want 18 (18 tolerated misses + 6 failed runs = 24 runs ≈ 6h)", DefaultTransientFailureThreshold)
	}
	dir := t.TempDir()
	contentPath := filepath.Join(dir, "content")
	writeContent := func(stdout string) {
		t.Helper()
		if err := os.WriteFile(contentPath, []byte("#!/bin/sh\nprintf '%s\\n' '"+stdout+"'\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir)
	env := func(k string) string {
		if k == "PDW_INGEST_PROJECT_DIR" {
			return dir
		}
		return ""
	}
	runOnce := func(extraArgs ...string) (int, string) {
		t.Helper()
		args := append([]string{
			"--spool-dir", dir,
			"--lock-file", filepath.Join(dir, "lock"),
			"--account", "test@example.invalid",
			"--transient-failure-threshold", "2",
		}, extraArgs...)
		var out bytes.Buffer
		code := Run(args, strings.NewReader(""), &out, &out, env, ingestclient.Config{BaseURL: "http://127.0.0.1:1", Token: "fake"})
		return code, out.String()
	}

	// Transient provider misses (timeout, no_fresh_fix) exit 0 up through threshold (2 misses).
	writeContent(`{"error_code":"timeout","error":"network location timeout"}`)
	if code, out := runOnce(); code != 0 || !strings.Contains(out, "transiently unavailable") {
		t.Fatalf("transient run 1 should exit 0, got %d: %s", code, out)
	}
	writeContent(`{"error_code":"no_fresh_fix","error":"no fresh valid network location"}`)
	if code, out := runOnce(); code != 0 || !strings.Contains(out, "transiently unavailable") {
		t.Fatalf("transient run 2 should exit 0, got %d: %s", code, out)
	}

	// Past the threshold (run N+1 = 3), runs exit non-zero so prolonged outages still alert.
	writeContent(`{"error_code":"timeout","error":"network location timeout"}`)
	if code, out := runOnce(); code == 0 || !strings.Contains(out, "pdw ingest android-location: ") || !strings.Contains(out, "timeout") || !strings.Contains(out, "3 consecutive") {
		t.Fatalf("transient run 3 should fail, got %d: %s", code, out)
	}

	// A real fix resets the transient streak so a later single blip exits 0 again.
	writeContent(`{"queued":"fake-sample.json"}`)
	if code, out := runOnce(); code != 0 {
		t.Fatalf("recovery run should exit 0, got %d: %s", code, out)
	}
	writeContent(`{"error_code":"timeout","error":"network location timeout"}`)
	if code, out := runOnce(); code != 0 {
		t.Fatalf("first transient miss after recovery should exit 0, got %d: %s", code, out)
	}
}

func TestHardHelperErrorCodesFailImmediately(t *testing.T) {
	for _, tc := range []struct {
		code string
		msg  string
	}{
		{"permission_denied", "location permission denied"},
		{"location_disabled", "network location provider disabled"},
		{"storage_error", "cannot commit location sample"},
		{"error", "unexpected helper failure"},
	} {
		dir := t.TempDir()
		script := "#!/bin/sh\nprintf '{\"error_code\":\"" + tc.code + "\",\"error\":\"" + tc.msg + "\"}\\n'\n"
		if err := os.WriteFile(filepath.Join(dir, "content"), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", dir)
		env := func(k string) string {
			if k == "PDW_INGEST_PROJECT_DIR" {
				return dir
			}
			return ""
		}
		var out bytes.Buffer
		code := Run([]string{
			"--spool-dir", dir,
			"--lock-file", filepath.Join(dir, "lock"),
			"--account", "test@example.invalid",
			"--transient-failure-threshold", "18",
		}, strings.NewReader(""), &out, &out, env, ingestclient.Config{BaseURL: "http://127.0.0.1:1", Token: "fake"})
		if code == 0 || !strings.Contains(out.String(), "pdw ingest android-location: ") || !strings.Contains(out.String(), tc.code) {
			t.Fatalf("hard error %s should fail immediately, got %d: %s", tc.code, code, out.String())
		}
	}
}

func TestPreflightDeferralKeepsQueueAndExitsZero(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "001.json")
	if err := os.WriteFile(p, []byte(`{"sample_id":"fake-1","latitude":0,"longitude":0,"accuracy_m":500,"provider":"network","measured_at_ms":1700000000000}`), 0600); err != nil {
		t.Fatal(err)
	}
	var logBuf bytes.Buffer
	c := &fakeUploader{}
	r := Runner{
		SpoolDir: dir, Account: "test@example.invalid", Device: "fake", Client: c,
		Logger:            common.NewWriterLogger(&logBuf),
		BeforeUploadCheck: func() string { return "app ingest preflight failed: offline" },
	}
	n, deferred, err := r.Sync()
	if err != nil || !deferred || n != 0 {
		t.Fatalf("preflight deferral got n=%d deferred=%t err=%v", n, deferred, err)
	}
	if !strings.Contains(logBuf.String(), "Skipping Android location upload: app ingest preflight failed: offline") {
		t.Fatalf("deferral reason not logged: %q", logBuf.String())
	}
	if c.calls != 0 {
		t.Fatal("deferred run should not call uploader")
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatal("deferred run deleted queued sample")
	}
}
