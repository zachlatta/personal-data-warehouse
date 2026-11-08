package androidlocation

import (
	"bytes"
	"errors"
	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	if _, err := r.Sync(); err == nil {
		t.Fatal("wanted offline failure")
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatal("offline lost sample")
	}
	r.DryRun = true
	if n, err := r.Sync(); err != nil || n != 1 {
		t.Fatalf("dry run %d %v", n, err)
	}
	if c.calls != 1 {
		t.Fatal("dry run uploaded")
	}
	r.DryRun = false
	c.fail = false
	if n, err := r.Sync(); err != nil || n != 1 {
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
		if code == 0 || !strings.Contains(out.String(), "queued samples drained") {
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
