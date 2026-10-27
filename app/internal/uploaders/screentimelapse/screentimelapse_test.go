package screentimelapse

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"errors"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

type recordedUpload struct {
	file    ingestclient.ScreenTimelapseFile
	content string
}

type fakeUploader struct {
	uploads []recordedUpload
	failOn  string
}

func (f *fakeUploader) UploadScreenTimelapseFile(content []byte, file ingestclient.ScreenTimelapseFile) (ingestclient.ScreenTimelapseStored, error) {
	if f.failOn != "" && file.SourceFilename == f.failOn {
		return ingestclient.ScreenTimelapseStored{}, errors.New("HTTP 502")
	}
	f.uploads = append(f.uploads, recordedUpload{file: file, content: string(content)})
	return ingestclient.ScreenTimelapseStored{StoredObject: ingestclient.StoredObject{StorageKey: "k"}}, nil
}

func (f *fakeUploader) names() []string {
	var out []string
	for _, u := range f.uploads {
		out = append(out, u.file.Device+"/"+u.file.SourceFilename)
	}
	return out
}

var testNow = time.Date(2026, 10, 22, 18, 30, 0, 0, time.UTC)

func newYork(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

// writeCapture writes a file whose mtime is ageAgo before testNow.
func writeCapture(t *testing.T, dir, name, content string, ageAgo time.Duration) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	at := testNow.Add(-ageAgo)
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatal(err)
	}
	return path
}

func newRunner(t *testing.T, client Uploader, sources ...Source) *Runner {
	t.Helper()
	state, err := OpenState(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	return &Runner{
		Sources:  sources,
		Client:   client,
		State:    state,
		Logger:   common.NewWriterLogger(&strings.Builder{}),
		Now:      func() time.Time { return testNow },
		Location: newYork(t),
	}
}

func TestSyncUploadsEverySettledCaptureFileFromEachDevice(t *testing.T) {
	mac, phone := t.TempDir(), t.TempDir()
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-00-54-display-1.heic", "pixels", 20*time.Minute)
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-00-54.ax.json.gz", "tree", 20*time.Minute)
	writeCapture(t, phone, "phone-screenshot-2026-10-22_14-01-03.png", "phone pixels", 20*time.Minute)
	// Not capture files, or not finished being written.
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-29-30.ax.json", "being gzipped", 10*time.Minute)
	writeCapture(t, mac, ".DS_Store", "", time.Hour)
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-29-30-display-1.heic", "just written", 30*time.Second)

	client := &fakeUploader{}
	summary, err := newRunner(t, client, Source{Device: "crobat", Dir: mac}, Source{Device: "joltik", Dir: phone}).Sync()
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(client.names(), ",")
	for _, want := range []string{
		"crobat/periodic-screenshot-2026-10-22_14-00-54-display-1.heic",
		"crobat/periodic-screenshot-2026-10-22_14-00-54.ax.json.gz",
		"joltik/phone-screenshot-2026-10-22_14-01-03.png",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("uploads %s missing %s", got, want)
		}
	}
	if len(client.uploads) != 3 || summary.Uploaded != 3 || summary.Unsettled != 1 {
		t.Fatalf("uploads = %v, summary = %+v", got, summary)
	}
	for _, u := range client.uploads {
		if u.file.SourceFilename == "periodic-screenshot-2026-10-22_14-00-54-display-1.heic" {
			// 14:00:54 EDT, the capture host's zone.
			if u.file.CapturedAt != "2026-10-22T18:00:54Z" || u.file.ContentType != "image/heic" || u.content != "pixels" {
				t.Fatalf("upload = %+v", u)
			}
		}
	}
}

func TestSyncSendsAFileOnceAndAgainOnlyWhenItChanges(t *testing.T) {
	mac := t.TempDir()
	path := writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-00-54-display-1.heic", "pixels", 20*time.Minute)
	client := &fakeUploader{}
	runner := newRunner(t, client, Source{Device: "crobat", Dir: mac})
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 1 {
		t.Fatalf("an acknowledged file was sent again: %v", client.names())
	}
	if err := os.WriteFile(path, []byte("re-encoded"), 0o644); err != nil {
		t.Fatal(err)
	}
	at := testNow.Add(-15 * time.Minute)
	_ = os.Chtimes(path, at, at)
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 2 || client.uploads[1].content != "re-encoded" {
		t.Fatalf("a changed file must be sent again: %v", client.uploads)
	}
}

func TestSyncStopsAtAFailedUploadAndLeavesItUnacknowledged(t *testing.T) {
	mac := t.TempDir()
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-00-54-display-1.heic", "a", 20*time.Minute)
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_13-59-52-display-1.heic", "b", 21*time.Minute)
	client := &fakeUploader{failOn: "periodic-screenshot-2026-10-22_14-00-54-display-1.heic"}
	runner := newRunner(t, client, Source{Device: "crobat", Dir: mac})
	if _, err := runner.Sync(); err == nil {
		t.Fatal("a failed upload must fail the run, so the heartbeat says so")
	}
	// Newest first, and nothing after the failure was attempted.
	if len(client.uploads) != 0 {
		t.Fatalf("uploads after a failure: %v", client.names())
	}
	client.failOn = ""
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 2 {
		t.Fatalf("the failed file must be retried next run: %v", client.names())
	}
}

func TestSyncSendsTheNewestFilesFirstAndStopsAtTheLimit(t *testing.T) {
	mac := t.TempDir()
	writeCapture(t, mac, "periodic-screenshot-2026-05-20_21-52-22.heic", "old", 150*24*time.Hour)
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_13-59-52-display-1.heic", "newer", 31*time.Minute)
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-00-54-display-1.heic", "newest", 30*time.Minute)
	client := &fakeUploader{}
	runner := newRunner(t, client, Source{Device: "crobat", Dir: mac})
	runner.Limit = 2
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	// A backfill drains behind the live minute instead of in front of it.
	if got := strings.Join(client.names(), ","); got != "crobat/periodic-screenshot-2026-10-22_14-00-54-display-1.heic,crobat/periodic-screenshot-2026-10-22_13-59-52-display-1.heic" {
		t.Fatalf("uploads = %s", got)
	}
	if summary.Remaining != 1 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestSyncFailsOnAMissingSourceFolder(t *testing.T) {
	client := &fakeUploader{}
	_, err := newRunner(t, client, Source{Device: "joltik", Dir: filepath.Join(t.TempDir(), "absent")}).Sync()
	if err == nil {
		t.Fatal("a configured folder that does not exist is a misconfiguration, not an empty source")
	}
}

func TestSyncDefersWhenTheNetworkGuardSaysNo(t *testing.T) {
	mac := t.TempDir()
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-00-54-display-1.heic", "a", 20*time.Minute)
	client := &fakeUploader{}
	runner := newRunner(t, client, Source{Device: "crobat", Dir: mac})
	runner.BeforeUploadCheck = func() string { return "tethered to a phone" }
	summary, err := runner.Sync()
	if err != nil || !summary.Deferred || len(client.uploads) != 0 {
		t.Fatalf("summary = %+v err = %v uploads = %v", summary, err, client.names())
	}
}

func TestParseSources(t *testing.T) {
	got, err := ParseSources("crobat=/Users/z/Pictures/Screenshots/periodic, joltik=/Users/z/Pictures/PhoneScreenshots/joltik")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != (Source{Device: "crobat", Dir: "/Users/z/Pictures/Screenshots/periodic"}) ||
		got[1] != (Source{Device: "joltik", Dir: "/Users/z/Pictures/PhoneScreenshots/joltik"}) {
		t.Fatalf("sources = %+v", got)
	}
	for _, bad := range []string{"crobat", "=/x", "Crobat=/x", "crobat=/x,crobat=/y"} {
		if _, err := ParseSources(bad); err == nil {
			t.Fatalf("ParseSources(%q) should fail", bad)
		}
	}
}

// --- the card guard ---

const (
	testCard     = "4111 1111 1111 1111" // Visa's documented test number
	testCardMiss = "4111 1111 1111 1112" // one digit off: fails Luhn
)

// pngCodec stands in for sips: every test image is a PNG whatever its name.
type pngCodec struct{}

func (pngCodec) Decode(content []byte, _ string) (image.Image, error) {
	return SystemCodec{}.Decode(content, "image/png")
}

func (pngCodec) Encode(img image.Image, _ string) ([]byte, error) {
	return SystemCodec{}.Encode(img, "image/png")
}

var testBlue = color.RGBA{B: 255, A: 255}

func solidPNG(t *testing.T, w, h int) string {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	draw.Draw(img, img.Bounds(), &image.Uniform{C: testBlue}, image.Point{}, draw.Src)
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func gzipped(t *testing.T, text string) string {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write([]byte(text)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func gunzipped(t *testing.T, content string) string {
	t.Helper()
	zr, err := gzip.NewReader(strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// macTree is a Mac snapshot: two displays in global points, a checkout form
// on the first with the card number and its expiry.
func macTree(card string, displays string) string {
	return `{"apps":[{"name":"Safari","windows":[{"role":"AXWindow","frame":[0,0,100,50],"children":[` +
		`{"role":"AXGroup","frame":[5,5,60,40],"children":[` +
		`{"role":"AXTextField","description":"Card number","value":"` + card + `","frame":[10,10,40,10]},` +
		`{"role":"AXTextField","description":"MM / YY","value":"12/29","frame":[10,25,20,8]}]}]}]}],` +
		`"displays":` + displays + `,"focused_element":null,"screen_windows":[]}`
}

func cardRunner(t *testing.T, client Uploader, logs *strings.Builder, sources ...Source) *Runner {
	t.Helper()
	runner := newRunner(t, client, sources...)
	runner.Codec = pngCodec{}
	runner.Logger = common.NewWriterLogger(logs)
	return runner
}

func uploadNamed(t *testing.T, client *fakeUploader, name string) recordedUpload {
	t.Helper()
	for _, u := range client.uploads {
		if u.file.SourceFilename == name {
			return u
		}
	}
	t.Fatalf("%s was not uploaded: %v", name, client.names())
	return recordedUpload{}
}

func pixel(t *testing.T, content string, x, y int) color.RGBA {
	t.Helper()
	img, err := png.Decode(strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	r, g, b, a := img.At(x, y).RGBA()
	return color.RGBA{R: uint8(r >> 8), G: uint8(g >> 8), B: uint8(b >> 8), A: uint8(a >> 8)}
}

const (
	macStamp    = "periodic-screenshot-2026-10-22_14-00-54"
	macTreeGz   = macStamp + ".ax.json.gz"
	macDisplay1 = macStamp + "-display-1.heic"
	macDisplay2 = macStamp + "-display-2.heic"
)

// The capture's display-N order need not be the tree's display order, so an
// image is placed by its size: here display-1 is the small second screen and
// display-2 the checkout's.
func writeCardMoment(t *testing.T, dir, card string) {
	t.Helper()
	writeCapture(t, dir, macTreeGz, gzipped(t, macTree(card, `[[0,0,100,50],[100,0,80,40]]`)), 20*time.Minute)
	writeCapture(t, dir, macDisplay1, solidPNG(t, 160, 80), 20*time.Minute)
	writeCapture(t, dir, macDisplay2, solidPNG(t, 200, 100), 20*time.Minute)
}

func TestSyncBoxesACardNumberAndWithholdsTheScreenTheBoxMisses(t *testing.T) {
	mac := t.TempDir()
	writeCardMoment(t, mac, testCard)
	client := &fakeUploader{}
	logs := &strings.Builder{}
	runner := cardRunner(t, client, logs, Source{Device: "crobat", Dir: mac})
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(client.names(), ","); strings.Contains(got, macDisplay1) || len(client.uploads) != 2 {
		t.Fatalf("uploads = %s; the screen the box does not reach must be withheld", got)
	}
	if summary.Redacted != 2 || summary.Withheld != 1 || summary.Uploaded != 2 {
		t.Fatalf("summary = %+v", summary)
	}
	tree := gunzipped(t, uploadNamed(t, client, macTreeGz).content)
	if strings.Contains(tree, "4111") || !strings.Contains(tree, `"value":"[redacted card number]"`) {
		t.Fatalf("uploaded tree = %s", tree)
	}
	boxed := uploadNamed(t, client, macDisplay2).content
	red := color.RGBA{R: 255, A: 255}
	// The number's frame [10,10,40,10] at 2x is (20,20)-(100,40), padded 8.
	for _, at := range [][2]int{{30, 30}, {13, 13}, {106, 46}} {
		if got := pixel(t, boxed, at[0], at[1]); got != red {
			t.Fatalf("pixel %v = %v, want the red box", at, got)
		}
	}
	// The expiry beside it, [10,25,20,8] -> (20,50)-(60,66).
	if got := pixel(t, boxed, 30, 58); got != red {
		t.Fatalf("expiry pixel = %v, want the red box", got)
	}
	if got := pixel(t, boxed, 150, 90); got != testBlue {
		t.Fatalf("pixel away from the card = %v, want it untouched", got)
	}
	log := logs.String()
	for _, want := range []string{
		"Redacting " + filepath.Join(mac, macTreeGz) + ": card number in accessibility tree",
		"Withholding " + filepath.Join(mac, macDisplay1) + ": card number in accessibility tree",
	} {
		if !strings.Contains(log, want) {
			t.Fatalf("log %q missing %q", log, want)
		}
	}
	if strings.Contains(log, "4111") || strings.Contains(log, "1111") {
		t.Fatalf("the log carries the digits: %s", log)
	}
	// Judged once: the withheld screen is recorded and never retried.
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 2 {
		t.Fatalf("a judged moment was sent again: %v", client.names())
	}
}

func TestSyncWithholdsEveryScreenWhenTwoDisplaysShareASize(t *testing.T) {
	mac := t.TempDir()
	writeCapture(t, mac, macTreeGz, gzipped(t, macTree(testCard, `[[0,0,100,50],[100,0,100,50]]`)), 20*time.Minute)
	writeCapture(t, mac, macDisplay1, solidPNG(t, 200, 100), 20*time.Minute)
	writeCapture(t, mac, macDisplay2, solidPNG(t, 200, 100), 20*time.Minute)
	client := &fakeUploader{}
	summary, err := cardRunner(t, client, &strings.Builder{}, Source{Device: "crobat", Dir: mac}).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(client.names(), ","); got != "crobat/"+macTreeGz || summary.Withheld != 2 {
		t.Fatalf("uploads = %s summary = %+v; an image that could be either screen cannot be boxed", got, summary)
	}
	if tree := gunzipped(t, client.uploads[0].content); strings.Contains(tree, "4111") {
		t.Fatalf("uploaded tree = %s", tree)
	}
}

func TestSyncBoxesACardNumberOnTheAndroidPhone(t *testing.T) {
	pixel7 := t.TempDir()
	tree := `<?xml version='1.0' encoding='UTF-8' standalone='yes' ?><hierarchy rotation="0">` +
		`<node index="0" text="" content-desc="" bounds="[0,0][60,120]">` +
		`<node index="0" text="4111-1111-1111-1111" resource-id="card_number" content-desc="" bounds="[5,10][55,20]"/>` +
		`</node></hierarchy>`
	writeCapture(t, pixel7, "phone-ax-2026-10-23_21-09-00-0400.xml.gz", gzipped(t, tree), 20*time.Minute)
	writeCapture(t, pixel7, "phone-screenshot-2026-10-23_21-09-00-0400.avif", solidPNG(t, 60, 120), 20*time.Minute)
	client := &fakeUploader{}
	summary, err := cardRunner(t, client, &strings.Builder{}, Source{Device: "pichu", Dir: pixel7}).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Redacted != 2 || summary.Withheld != 0 {
		t.Fatalf("summary = %+v", summary)
	}
	boxed := uploadNamed(t, client, "phone-screenshot-2026-10-23_21-09-00-0400.avif").content
	if got := pixel(t, boxed, 30, 15); got != (color.RGBA{R: 255, A: 255}) {
		t.Fatalf("pixel = %v, want the red box at the node's own pixels", got)
	}
	if got := pixel(t, boxed, 30, 100); got != testBlue {
		t.Fatalf("pixel = %v, want it untouched", got)
	}
	if text := gunzipped(t, uploadNamed(t, client, "phone-ax-2026-10-23_21-09-00-0400.xml.gz").content); strings.Contains(text, "4111") {
		t.Fatalf("uploaded tree = %s", text)
	}
}

func TestSyncWithholdsTheScreenWhenTheTreeHasNoFrames(t *testing.T) {
	iphone := t.TempDir()
	writeCapture(t, iphone, "phone-ax-2026-10-18_22-39-13.json", `[{"caption":"Card number, `+testCard+`"}]`, 20*time.Minute)
	writeCapture(t, iphone, "phone-screenshot-2026-10-18_22-39-13.png", solidPNG(t, 40, 80), 20*time.Minute)
	client := &fakeUploader{}
	summary, err := cardRunner(t, client, &strings.Builder{}, Source{Device: "joltik", Dir: iphone}).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(client.names(), ","); got != "joltik/phone-ax-2026-10-18_22-39-13.json" || summary.Withheld != 1 {
		t.Fatalf("uploads = %s summary = %+v", got, summary)
	}
	if strings.Contains(client.uploads[0].content, "4111") {
		t.Fatalf("uploaded tree = %s", client.uploads[0].content)
	}
}

func TestSyncSendsAMomentWhoseNumberFailsLuhnAsWritten(t *testing.T) {
	mac := t.TempDir()
	writeCardMoment(t, mac, testCardMiss)
	client := &fakeUploader{}
	summary, err := cardRunner(t, client, &strings.Builder{}, Source{Device: "crobat", Dir: mac}).Sync()
	if err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 3 || summary.Redacted != 0 || summary.Withheld != 0 {
		t.Fatalf("uploads = %v summary = %+v", client.names(), summary)
	}
	for _, name := range []string{macTreeGz, macDisplay1, macDisplay2} {
		written, _ := os.ReadFile(filepath.Join(mac, name))
		if uploadNamed(t, client, name).content != string(written) {
			t.Fatalf("%s was not sent exactly as written", name)
		}
	}
}

func TestSyncHoldsAScreenshotThatSettledBeforeItsTree(t *testing.T) {
	mac := t.TempDir()
	writeCapture(t, mac, macDisplay2, solidPNG(t, 200, 100), 5*time.Minute)
	// The loop has renamed the image into place and is gzipping the tree.
	plain := writeCapture(t, mac, macStamp+".ax.json", macTree(testCard, `[[0,0,100,50]]`), 5*time.Minute)
	client := &fakeUploader{}
	runner := cardRunner(t, client, &strings.Builder{}, Source{Device: "crobat", Dir: mac})
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 0 || summary.Held != 1 {
		t.Fatalf("uploads = %v summary = %+v; the image shipped before its tree was checked", client.names(), summary)
	}
	// Gzipped, but not yet settled: still held.
	if err := os.Remove(plain); err != nil {
		t.Fatal(err)
	}
	writeCapture(t, mac, macTreeGz, gzipped(t, macTree(testCard, `[[0,0,100,50]]`)), time.Minute)
	if summary, err = runner.Sync(); err != nil || len(client.uploads) != 0 || summary.Held != 1 {
		t.Fatalf("uploads = %v summary = %+v err = %v", client.names(), summary, err)
	}
	writeCapture(t, mac, macTreeGz, gzipped(t, macTree(testCard, `[[0,0,100,50]]`)), 3*time.Minute)
	if summary, err = runner.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 2 || summary.Redacted != 2 {
		t.Fatalf("uploads = %v summary = %+v", client.names(), summary)
	}
	if got := pixel(t, uploadNamed(t, client, macDisplay2).content, 30, 30); got != (color.RGBA{R: 255, A: 255}) {
		t.Fatalf("pixel = %v, want the red box", got)
	}
}

func TestSyncSendsAMomentWithNoTreeOnceTheWaitForOnePasses(t *testing.T) {
	mac := t.TempDir()
	path := writeCapture(t, mac, macDisplay1, "pixels", 5*time.Minute)
	client := &fakeUploader{}
	runner := cardRunner(t, client, &strings.Builder{}, Source{Device: "crobat", Dir: mac})
	summary, err := runner.Sync()
	if err != nil || len(client.uploads) != 0 || summary.Held != 1 {
		t.Fatalf("uploads = %v summary = %+v err = %v", client.names(), summary, err)
	}
	at := testNow.Add(-(DefaultSettleAge + DefaultNoTreeWait))
	_ = os.Chtimes(path, at, at)
	if summary, err = runner.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 1 || client.uploads[0].content != "pixels" || summary.Held != 0 {
		t.Fatalf("uploads = %v summary = %+v", client.uploads, summary)
	}
}

func TestSyncSendsAWithheldScreenshotAsItIsOnceItIsEdited(t *testing.T) {
	mac := t.TempDir()
	writeCardMoment(t, mac, testCard)
	client := &fakeUploader{}
	logs := &strings.Builder{}
	runner := cardRunner(t, client, logs, Source{Device: "crobat", Dir: mac})
	if _, err := runner.Sync(); err != nil {
		t.Fatal(err)
	}
	// Zach blacks out the withheld screen by hand. The tree still holds the
	// number, so a scan would withhold the edit again: the edit is the review.
	writeCapture(t, mac, macDisplay1, "blacked out by hand", 10*time.Minute)
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 3 || client.uploads[2].file.SourceFilename != macDisplay1 ||
		client.uploads[2].content != "blacked out by hand" || summary.Uploaded != 1 || summary.Redacted != 0 {
		t.Fatalf("uploads = %v summary = %+v", client.names(), summary)
	}
	if !strings.Contains(logs.String(), "Sending edited withheld file "+filepath.Join(mac, macDisplay1)) {
		t.Fatalf("log = %s", logs.String())
	}
}

func TestSyncKeepsAMomentTogetherAcrossTheLimit(t *testing.T) {
	mac := t.TempDir()
	writeCapture(t, mac, "periodic-screenshot-2026-10-22_14-05-00-display-1.heic", "newest", 15*time.Minute)
	writeCardMoment(t, mac, testCardMiss)
	client := &fakeUploader{}
	runner := cardRunner(t, client, &strings.Builder{}, Source{Device: "crobat", Dir: mac})
	runner.Limit = 2
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 1 || summary.Remaining != 3 {
		t.Fatalf("uploads = %v summary = %+v; a three-file moment must not be split by a limit of 2", client.names(), summary)
	}
	if summary, err = runner.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 4 || summary.Remaining != 0 {
		t.Fatalf("uploads = %v summary = %+v; the whole moment goes in one run", client.names(), summary)
	}
}

func TestSyncWithholdsAMomentWhoseTreeCannotBeRead(t *testing.T) {
	mac := t.TempDir()
	writeCapture(t, mac, macTreeGz, "\x1f\x8b truncated", 20*time.Minute)
	writeCapture(t, mac, macDisplay1, solidPNG(t, 200, 100), 20*time.Minute)
	client := &fakeUploader{}
	summary, err := cardRunner(t, client, &strings.Builder{}, Source{Device: "crobat", Dir: mac}).Sync()
	if err != nil || len(client.uploads) != 0 || summary.Withheld != 2 {
		t.Fatalf("uploads = %v summary = %+v err = %v", client.names(), summary, err)
	}
}

func TestFindCardNumbers(t *testing.T) {
	cases := []struct {
		text string
		hits int
	}{
		{"4111111111111111", 1},
		{"card 4111 1111 1111 1111 exp", 1},
		{"4111-1111-1111-1111", 1},
		{"4111 1111 1111 1111 123", 1}, // the CVC typed after it
		{`"\n4111111111111111"`, 1},    // after a JSON escape
		{"5555555555554444", 1},        // Mastercard
		{"2223003122003222", 1},        // Mastercard 2-series
		{"378282246310005", 1},         // Amex
		{"3782 822463 10005", 1},       // Amex as printed
		{"6011111111111117", 1},        // Discover
		{"4111111111111112", 0},        // Luhn fails
		{"1111111111111117", 0},        // no issuer
		{"378282246310005000", 0},      // Amex prefix, wrong length
		{"4111 1111 11 11 1111", 0},    // not a card grouping
		{"id4111111111111111", 0},      // glued to a word
		{`"url":"https:\/\/www.instagram.com\/stories\/someone\/4738291056382910454\/?r=1"`, 0}, // a URL path id
		{"https://example.com/p/4738291056382910454", 0},
		{"Card 4738291056382910454", 1}, // the same digits outside a URL
		{"4111111111111111111111111", 0},
		{"1761429600000 1761429600001", 0},
	}
	for _, tc := range cases {
		if got := len(findCardNumbers(tc.text)); got != tc.hits {
			t.Errorf("findCardNumbers(%q) = %d hits, want %d", tc.text, got, tc.hits)
		}
	}
	if got := redactCardNumbers("pay 4111 1111 1111 1111 now", findCardNumbers("pay 4111 1111 1111 1111 now")); got != "pay [redacted card number] now" {
		t.Fatalf("redacted = %q", got)
	}
}

func makeSamplePNG(t *testing.T, width, height int) []byte {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.SetNRGBA(x, y, color.NRGBA{R: uint8(x % 255), G: uint8(y % 255), B: 180, A: 255})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestEmbeddedWebpHelperMatchesSource(t *testing.T) {
	zr, err := zip.NewReader(bytes.NewReader(embeddedWebpJar), int64(len(embeddedWebpJar)))
	if err != nil {
		t.Fatalf("embeddedWebpJar is not a valid zip: %v", err)
	}
	if len(zr.File) != 1 || zr.File[0].Name != "classes.dex" {
		t.Fatalf("embeddedWebpJar files = %+v, want [classes.dex]", zr.File)
	}
	rc, err := zr.File[0].Open()
	if err != nil {
		t.Fatal(err)
	}
	dex, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(dex, []byte("dex\n")) {
		t.Fatalf("classes.dex missing DEX magic header")
	}
	for _, needle := range []string{"PdwPngCompress", "WEBP_LOSSY"} {
		if !bytes.Contains(dex, []byte(needle)) {
			t.Fatalf("classes.dex missing %q", needle)
		}
	}

	buildScript := filepath.Join("..", "..", "..", "..", "ops", "android", "pdw-screen-timelapse", "build.sh")
	if _, err := os.Stat(buildScript); err == nil {
		if _, err := exec.LookPath("javac"); err == nil {
			hasSDK := false
			for _, candidate := range []string{
				os.Getenv("ANDROID_HOME"),
				filepath.Join(os.Getenv("HOME"), "Android", "Sdk"),
				"/opt/homebrew/share/android-commandlinetools",
			} {
				if candidate != "" {
					if _, err := os.Stat(filepath.Join(candidate, "platforms", "android-36", "android.jar")); err == nil {
						hasSDK = true
						break
					}
				}
			}
			if hasSDK {
				out, err := exec.Command(buildScript, "--check").CombinedOutput()
				if err != nil {
					t.Fatalf("build.sh --check failed: %v (%s)", err, strings.TrimSpace(string(out)))
				}
			}
		}
	}
}

func TestDefaultPNGCompressorUsesAndroidAppProcessAndFallsBackToJPEG(t *testing.T) {
	raw := makeSamplePNG(t, 120, 240)

	// Pure-Go fallback at full resolution (MaxEdge = 0).
	c := &DefaultPNGCompressor{Quality: 75, MaxEdge: 0, AppProcessPath: "/nonexistent/app_process64"}
	gotFull, err := c.CompressPNG(raw)
	if err != nil {
		t.Fatal(err)
	}
	if gotFull.Extension != ".jpg" || gotFull.MimeType != "image/jpeg" {
		t.Fatalf("got %+v", gotFull)
	}
	decodedFull, err := jpeg.Decode(bytes.NewReader(gotFull.Content))
	if err != nil {
		t.Fatal(err)
	}
	if decodedFull.Bounds().Dx() != 120 || decodedFull.Bounds().Dy() != 240 {
		t.Fatalf("bounds = %v, want 120x240", decodedFull.Bounds())
	}

	// Pure-Go fallback with MaxEdge scaling (MaxEdge = 120 -> 60x120).
	cScaled := &DefaultPNGCompressor{Quality: 75, MaxEdge: 120, AppProcessPath: "/nonexistent/app_process64"}
	gotScaled, err := cScaled.CompressPNG(raw)
	if err != nil {
		t.Fatal(err)
	}
	decodedScaled, err := jpeg.Decode(bytes.NewReader(gotScaled.Content))
	if err != nil {
		t.Fatal(err)
	}
	if decodedScaled.Bounds().Dx() != 60 || decodedScaled.Bounds().Dy() != 120 {
		t.Fatalf("scaled bounds = %v, want 60x120", decodedScaled.Bounds())
	}

	// Android app_process64 path producing WebP.
	fakeAppProc := filepath.Join(t.TempDir(), "app_process64")
	script := "#!/bin/sh\n" +
		"[ \"$1\" = \"/\" ] || exit 11\n" +
		"[ \"$2\" = \"" + androidHelperClass + "\" ] || exit 12\n" +
		"[ \"$3\" = \"75\" ] || exit 13\n" +
		"[ \"$4\" = \"0\" ] || exit 14\n" +
		"[ -f \"$5\" ] || exit 15\n" +
		"[ -n \"$CLASSPATH\" ] && [ -f \"$CLASSPATH\" ] || exit 16\n" +
		"printf 'RIFFxxxxWEBPVP8 fake-webp-bytes' > \"$6\"\n"
	if err := os.WriteFile(fakeAppProc, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	cAndroid := &DefaultPNGCompressor{Quality: 75, MaxEdge: 0, AppProcessPath: fakeAppProc}
	gotWebP, err := cAndroid.CompressPNG(raw)
	if err != nil {
		t.Fatal(err)
	}
	if gotWebP.Extension != ".webp" || gotWebP.MimeType != "image/webp" || !bytes.HasPrefix(gotWebP.Content, []byte("RIFF")) {
		t.Fatalf("gotWebP = %+v", gotWebP)
	}
}

func TestSyncCompressesPNGScreenshotsAndDeletesAcknowledgedFiles(t *testing.T) {
	phone := t.TempDir()
	rawPNG := makeSamplePNG(t, 80, 160)
	shotPath := writeCapture(t, phone, "phone-screenshot-2026-10-27_08-09-01-0700.png", string(rawPNG), 20*time.Minute)
	treePath := writeCapture(t, phone, "phone-ax-2026-10-27_08-09-01-0700.xml.gz", gzipped(t, "<hierarchy/>"), 20*time.Minute)
	youngPath := writeCapture(t, phone, "phone-screenshot-2026-10-27_08-29-45-0700.png", string(rawPNG), 15*time.Second)

	client := &fakeUploader{}
	runner := newRunner(t, client, Source{Device: "pichu", Dir: phone})
	runner.CompressPNG = true
	runner.DeleteAfterUpload = true
	runner.Compressor = &DefaultPNGCompressor{Quality: 75, MaxEdge: 0, AppProcessPath: "/nonexistent/app_process64"}

	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Uploaded != 2 || summary.Compressed != 1 || summary.Deleted != 2 || summary.Unsettled != 1 {
		t.Fatalf("summary = %+v", summary)
	}
	got := strings.Join(client.names(), ",")
	for _, want := range []string{
		"pichu/phone-screenshot-2026-10-27_08-09-01-0700.jpg",
		"pichu/phone-ax-2026-10-27_08-09-01-0700.xml.gz",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("uploads %s missing %s", got, want)
		}
	}
	for _, u := range client.uploads {
		if strings.HasSuffix(u.file.SourceFilename, ".jpg") {
			if u.file.ContentType != "image/jpeg" || !strings.HasPrefix(u.content, "\xff\xd8\xff") {
				t.Fatalf("compressed upload = %+v", u.file)
			}
		}
	}
	if _, err := os.Stat(shotPath); !os.IsNotExist(err) {
		t.Fatalf("acknowledged screenshot %s should be deleted", shotPath)
	}
	if _, err := os.Stat(treePath); !os.IsNotExist(err) {
		t.Fatalf("acknowledged tree %s should be deleted", treePath)
	}
	if _, err := os.Stat(youngPath); err != nil {
		t.Fatalf("unsettled screenshot %s must remain on disk: %v", youngPath, err)
	}
}

func TestSyncBoxesCardNumberOnAndroidBeforeCompressingToWebpAndDeletesAfterUpload(t *testing.T) {
	phone := t.TempDir()
	tree := `<?xml version='1.0' encoding='UTF-8' standalone='yes' ?><hierarchy rotation="0">` +
		`<node index="0" text="" content-desc="" bounds="[0,0][60,120]">` +
		`<node index="0" text="4111-1111-1111-1111" resource-id="card_number" content-desc="" bounds="[5,10][55,20]"/>` +
		`<node index="1" text="12/29" resource-id="expiry" content-desc="Expiration" bounds="[5,25][35,35]"/>` +
		`</node></hierarchy>`
	treePath := writeCapture(t, phone, "phone-ax-2026-10-23_21-09-00-0400.xml.gz", gzipped(t, tree), 20*time.Minute)
	shotPath := writeCapture(t, phone, "phone-screenshot-2026-10-23_21-09-00-0400.png", solidPNG(t, 60, 120), 20*time.Minute)

	// Fake app_process64 saves the input PNG ($5) it received so we can verify
	// the red box was drawn BEFORE WebP compression ran.
	capturedInputPNG := filepath.Join(t.TempDir(), "compressor-input.png")
	fakeAppProc := filepath.Join(t.TempDir(), "app_process64")
	script := "#!/bin/sh\n" +
		"cp \"$5\" \"" + capturedInputPNG + "\"\n" +
		"printf 'RIFFxxxxWEBPVP8 boxed-webp' > \"$6\"\n"
	if err := os.WriteFile(fakeAppProc, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	client := &fakeUploader{}
	runner := newRunner(t, client, Source{Device: "pichu", Dir: phone})
	runner.CompressPNG = true
	runner.DeleteAfterUpload = true
	runner.Compressor = &DefaultPNGCompressor{Quality: 75, MaxEdge: 0, AppProcessPath: fakeAppProc}

	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Uploaded != 2 || summary.Redacted != 2 || summary.Compressed != 1 || summary.Deleted != 2 || summary.Withheld != 0 {
		t.Fatalf("summary = %+v", summary)
	}
	// Verify the PNG handed to the WebP compressor already had the card number
	// and expiry boxed in solid red, and unboxed pixels untouched.
	compressorInput, err := os.ReadFile(capturedInputPNG)
	if err != nil {
		t.Fatalf("compressor did not receive input PNG: %v", err)
	}
	red := color.RGBA{R: 255, A: 255}
	if got := pixel(t, string(compressorInput), 30, 15); got != red {
		t.Fatalf("card number pixel passed to compressor = %v, want red box", got)
	}
	if got := pixel(t, string(compressorInput), 20, 30); got != red {
		t.Fatalf("expiry pixel passed to compressor = %v, want red box", got)
	}
	if got := pixel(t, string(compressorInput), 30, 100); got != testBlue {
		t.Fatalf("unboxed pixel passed to compressor = %v, want untouched blue", got)
	}

	webpUpload := uploadNamed(t, client, "phone-screenshot-2026-10-23_21-09-00-0400.webp")
	if webpUpload.file.ContentType != "image/webp" || webpUpload.content != "RIFFxxxxWEBPVP8 boxed-webp" {
		t.Fatalf("webpUpload = %+v", webpUpload)
	}
	treeUpload := gunzipped(t, uploadNamed(t, client, "phone-ax-2026-10-23_21-09-00-0400.xml.gz").content)
	if strings.Contains(treeUpload, "4111") || !strings.Contains(treeUpload, "[redacted card number]") {
		t.Fatalf("uploaded tree = %s", treeUpload)
	}
	if _, err := os.Stat(shotPath); !os.IsNotExist(err) {
		t.Fatalf("boxed and uploaded screenshot should be deleted: %v", err)
	}
	if _, err := os.Stat(treePath); !os.IsNotExist(err) {
		t.Fatalf("redacted and uploaded tree should be deleted: %v", err)
	}
}

func TestSyncNeverDeletesOrSendsWithheldScreenshotWhenDeleteAfterUploadIsEnabled(t *testing.T) {
	phone := t.TempDir()
	// Card number is in the tree, but the screen bounds [0,0][100,200] do not
	// match the screenshot's 60x120 dimensions, so the screenshot must be withheld.
	tree := `<?xml version='1.0' encoding='UTF-8' standalone='yes' ?><hierarchy rotation="0">` +
		`<node index="0" text="" content-desc="" bounds="[0,0][100,200]">` +
		`<node index="0" text="4111-1111-1111-1111" resource-id="card_number" content-desc="" bounds="[5,10][55,20]"/>` +
		`</node></hierarchy>`
	treePath := writeCapture(t, phone, "phone-ax-2026-10-23_21-09-00-0400.xml.gz", gzipped(t, tree), 20*time.Minute)
	shotPath := writeCapture(t, phone, "phone-screenshot-2026-10-23_21-09-00-0400.png", solidPNG(t, 60, 120), 20*time.Minute)

	client := &fakeUploader{}
	runner := newRunner(t, client, Source{Device: "pichu", Dir: phone})
	runner.CompressPNG = true
	runner.DeleteAfterUpload = true
	runner.Compressor = &DefaultPNGCompressor{Quality: 75, MaxEdge: 0, AppProcessPath: "/nonexistent/app_process64"}

	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(client.names(), ","); got != "pichu/phone-ax-2026-10-23_21-09-00-0400.xml.gz" {
		t.Fatalf("uploads = %s; withheld screenshot must not be sent", got)
	}
	if summary.Uploaded != 1 || summary.Redacted != 1 || summary.Withheld != 1 || summary.Deleted != 0 {
		t.Fatalf("summary = %+v", summary)
	}
	if _, err := os.Stat(shotPath); err != nil {
		t.Fatalf("withheld screenshot %s must not be deleted: %v", shotPath, err)
	}
	if _, err := os.Stat(treePath); err != nil {
		t.Fatalf("tree %s of a withheld moment must remain on disk: %v", treePath, err)
	}

	// Second run: the withheld screenshot is remembered in the ledger and not retried or deleted.
	summary2, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if len(client.uploads) != 1 || summary2.Uploaded != 0 || summary2.Withheld != 0 || summary2.Deleted != 0 {
		t.Fatalf("second run summary = %+v, uploads = %v", summary2, client.names())
	}
}

func TestSyncRetainsTreeWhenScreenshotUploadFailsAndBoxesCardNumberOnRetry(t *testing.T) {
	phone := t.TempDir()
	tree := `<?xml version='1.0' encoding='UTF-8' standalone='yes' ?><hierarchy rotation="0">` +
		`<node index="0" text="" content-desc="" bounds="[0,0][60,120]">` +
		`<node index="0" text="4111-1111-1111-1111" resource-id="card_number" content-desc="" bounds="[5,10][55,20]"/>` +
		`</node></hierarchy>`
	// On pichu the tree is written after the screenshot, so the tree has a
	// newer mtime and is sent first within the moment.
	shotPath := writeCapture(t, phone, "phone-screenshot-2026-10-23_21-09-00-0400.png", solidPNG(t, 60, 120), 20*time.Minute+2*time.Second)
	treePath := writeCapture(t, phone, "phone-ax-2026-10-23_21-09-00-0400.xml.gz", gzipped(t, tree), 20*time.Minute)

	capturedInputPNG := filepath.Join(t.TempDir(), "compressor-input.png")
	fakeAppProc := filepath.Join(t.TempDir(), "app_process64")
	script := "#!/bin/sh\n" +
		"cp \"$5\" \"" + capturedInputPNG + "\"\n" +
		"printf 'RIFFxxxxWEBPVP8 boxed-webp' > \"$6\"\n"
	if err := os.WriteFile(fakeAppProc, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	// Run 1: tree uploads first and succeeds; screenshot upload fails with HTTP 502.
	client := &fakeUploader{failOn: "phone-screenshot-2026-10-23_21-09-00-0400.webp"}
	runner := newRunner(t, client, Source{Device: "pichu", Dir: phone})
	runner.CompressPNG = true
	runner.DeleteAfterUpload = true
	runner.Compressor = &DefaultPNGCompressor{Quality: 75, MaxEdge: 0, AppProcessPath: fakeAppProc}

	summary1, err := runner.Sync()
	if err == nil {
		t.Fatal("expected screenshot upload failure to fail the run")
	}
	if summary1.Uploaded != 1 || summary1.Deleted != 0 {
		t.Fatalf("run 1 summary = %+v; tree must not be deleted before screenshot upload succeeds", summary1)
	}
	if _, err := os.Stat(treePath); err != nil {
		t.Fatalf("tree %s must remain on disk when screenshot upload fails: %v", treePath, err)
	}
	if _, err := os.Stat(shotPath); err != nil {
		t.Fatalf("screenshot %s must remain on disk when its upload fails: %v", shotPath, err)
	}

	// Run 2: 3 hours later (well past DefaultNoTreeWait). Because the tree was
	// retained on disk, judgeMoment still reads it and boxes out the card number.
	_ = os.Remove(capturedInputPNG)
	client.failOn = ""
	runner.Now = func() time.Time { return testNow.Add(3 * time.Hour) }

	summary2, err := runner.Sync()
	if err != nil {
		t.Fatalf("run 2 failed: %v", err)
	}
	if summary2.Uploaded != 1 || summary2.Redacted != 1 || summary2.Compressed != 1 || summary2.Deleted != 2 {
		t.Fatalf("run 2 summary = %+v", summary2)
	}
	compressorInput, err := os.ReadFile(capturedInputPNG)
	if err != nil {
		t.Fatalf("run 2 compressor did not receive input PNG: %v", err)
	}
	red := color.RGBA{R: 255, A: 255}
	if got := pixel(t, string(compressorInput), 30, 15); got != red {
		t.Fatalf("run 2 screenshot sent with card-number region UNBOXED: %v, want %v", got, red)
	}
	if _, err := os.Stat(shotPath); !os.IsNotExist(err) {
		t.Fatalf("screenshot should be deleted after moment completes: %v", err)
	}
	if _, err := os.Stat(treePath); !os.IsNotExist(err) {
		t.Fatalf("tree should be deleted after moment completes: %v", err)
	}
}

func TestCheckAndroidWiFi(t *testing.T) {
	wifiDump := `
Active default network: 104
  NetworkAgentInfo{network{104}  handle{449730285581}  ni{WIFI CONNECTED extra: "Home"}}
`
	if reason := CheckAndroidWiFi(wifiDump); reason != "" {
		t.Fatalf("CheckAndroidWiFi(wifi) = %q, want empty", reason)
	}

	lteDump := `
Active default network: 100
  NetworkAgentInfo{network{100}  handle{432550416397}  ni{MOBILE[LTE] CONNECTED}}
  NetworkAgentInfo{network{104}  handle{449730285581}  ni{WIFI DISCONNECTED}}
`
	if reason := CheckAndroidWiFi(lteDump); reason == "" {
		t.Fatal("CheckAndroidWiFi(lte) should defer when default network is MOBILE[LTE]")
	}

	noneDump := "Active default network: none\n"
	if reason := CheckAndroidWiFi(noneDump); reason == "" {
		t.Fatal("CheckAndroidWiFi(none) should defer when no default network is active")
	}
}
