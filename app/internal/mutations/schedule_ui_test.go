package mutations

import (
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A scheduled request's "Runs at" and deadline reach both review clients:
// the API's request JSON (list and detail) carries not_before / not_after, and
// the web and phone helpers that draw the header read them from that JSON.
func TestAScheduledRequestShowsRunsAtFromTheAPIToBothClients(t *testing.T) {
	fixture := pendingFixture()
	fixture.Status = "approved"
	fixture.NotBefore = time.Date(2026, 12, 4, 12, 30, 0, 0, time.UTC)
	fixture.NotAfter = time.Date(2026, 12, 4, 14, 30, 0, 0, time.UTC)
	fixture.Mutations[0].NotBefore = fixture.NotBefore
	fixture.Mutations[0].NotAfter = fixture.NotAfter
	fixture.Mutations[0].Precondition = map[string]any{"kind": "not_home", "within_minutes": 120.0}
	srv := newAPIServer(t, newAPIFakeStore(fixture))
	read := func(path string) string {
		resp, err := http.Get(srv.URL + APIPath + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}
	detail := read("/requests/req-1")
	list := read("/requests")
	for name, body := range map[string]string{"detail": detail, "list": list} {
		if !strings.Contains(body, `"not_before":"2026-12-04T12:30:00Z"`) || !strings.Contains(body, `"not_after":"2026-12-04T14:30:00Z"`) {
			t.Fatalf("%s JSON lacks the schedule: %s", name, body)
		}
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed; the client half is skipped")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "detail.json"), []byte(detail), 0o600); err != nil {
		t.Fatal(err)
	}
	web, _ := filepath.Abs("../webapp/static/mutation_view.js")
	phone, _ := filepath.Abs("../../../mobile/src/lib/mutation-review.ts")
	script := `
import { readFileSync } from "node:fs";
const request = JSON.parse(readFileSync(process.argv[1], "utf8")).request;
const web = await import(process.argv[2]);
const phone = await import(process.argv[3]);
for (const [name, schedule] of [["web", web.requestSchedule(request)], ["phone", phone.requestSchedule(request)]]) {
  if (schedule.runsAt !== "2026-12-04T12:30:00Z" || schedule.deadline !== "2026-12-04T14:30:00Z") {
    console.error(name, JSON.stringify(schedule));
    process.exit(1);
  }
}
// Each step's own times and precondition, as the step cards draw them.
const step = request.mutations[0];
for (const [name, schedule] of [["web", web.mutationSchedule(step)], ["phone", phone.mutationSchedule(step)]]) {
  if (schedule.runsAt !== "2026-12-04T12:30:00Z" || schedule.deadline !== "2026-12-04T14:30:00Z" || !/at home/.test(schedule.precondition) || !/120 minutes/.test(schedule.precondition)) {
    console.error(name, JSON.stringify(schedule));
    process.exit(1);
  }
}
`
	cmd := exec.Command(node, "--experimental-strip-types", "--no-warnings", "--input-type=module", "-e", script, filepath.Join(dir, "detail.json"), "file://"+web, "file://"+phone)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the clients did not read the schedule: %v\n%s", err, out)
	}
}
