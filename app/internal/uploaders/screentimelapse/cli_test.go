package screentimelapse

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
)

// runDryRun runs `pdw ingest screen-timelapse --dry-run` over one phone folder
// holding a tree-less screenshot written age ago, with extra flags.
func runDryRun(t *testing.T, age time.Duration, extra ...string) (int, string, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "phone-screenshot-2026-11-19_21-08-50-0500.png")
	if err := os.WriteFile(path, []byte("pixels"), 0o600); err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-age)
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatal(err)
	}
	project := t.TempDir()
	args := append([]string{
		"--dry-run",
		"--sources", "pichu=" + dir,
		"--lock-file", filepath.Join(project, "run.lock"),
		"--state-file", filepath.Join(project, "state.sqlite"),
	}, extra...)
	getenv := func(key string) string {
		if key == "PDW_INGEST_PROJECT_DIR" {
			return project
		}
		return ""
	}
	var stdout, stderr strings.Builder
	code := Run(args, strings.NewReader(""), &stdout, &stderr, getenv, ingestclient.Config{})
	return code, stdout.String(), stderr.String()
}

// The default waits suit the Mac, whose loop gzips its tree in place after the
// image lands. The phone's loop renames its tree into place before the
// screenshot, so its uploader waits seconds, which is what lets the screenshot
// taken when a voice memo starts reach the warehouse before the memo's
// transcript does.
func TestRunHoldsAFreshTreeLessScreenshotForTheDefaultWaits(t *testing.T) {
	code, stdout, stderr := runDryRun(t, 10*time.Second)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if strings.Contains(stdout, "would send") || !strings.Contains(stdout, "unsettled=1") {
		t.Fatalf("stdout = %q", stdout)
	}
}

func TestRunSendsAFreshTreeLessScreenshotWithShortSettleAndTreeWaits(t *testing.T) {
	code, stdout, stderr := runDryRun(t, 10*time.Second, "--settle-age", "3s", "--no-tree-wait", "3s")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "would send pichu/phone-screenshot-2026-11-19_21-08-50-0500.png") {
		t.Fatalf("stdout = %q", stdout)
	}
}

func TestRunRejectsANonPositiveWait(t *testing.T) {
	for _, flags := range [][]string{{"--settle-age", "0s"}, {"--no-tree-wait", "-1s"}} {
		code, _, stderr := runDryRun(t, time.Minute, flags...)
		if code == 0 || !strings.Contains(stderr, "must be greater than 0") {
			t.Fatalf("%v: exit %d stderr %q", flags, code, stderr)
		}
	}
}
