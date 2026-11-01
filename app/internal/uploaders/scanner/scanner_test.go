package scanner

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// fakeScanner is a WS-Scan service: each CreateScanJob serves the next
// queued pass page by page, then forgets the job the way a Brother does.
type fakeScanner struct {
	mu      sync.Mutex
	passes  [][][]byte
	tickets []string
	jobs    map[string][][]byte
	nextJob int
}

func (f *fakeScanner) queue(pages ...[]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.passes = append(f.passes, pages)
}

var actionRE = regexp.MustCompile(`<wsa:Action>([^<]+)</wsa:Action>`)
var jobIDRE = regexp.MustCompile(`<sca:JobId>([^<]+)</sca:JobId>`)

func fault(w http.ResponseWriter, subcode string) {
	w.Header().Set("Content-Type", "application/soap+xml")
	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprintf(w, `<?xml version="1.0"?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope" xmlns:wscn="http://schemas.microsoft.com/windows/2006/08/wdp/scan"><SOAP-ENV:Body><SOAP-ENV:Fault><SOAP-ENV:Code><SOAP-ENV:Value>SOAP-ENV:Sender</SOAP-ENV:Value><SOAP-ENV:Subcode><SOAP-ENV:Value>wscn:%s</SOAP-ENV:Value></SOAP-ENV:Subcode></SOAP-ENV:Code><SOAP-ENV:Reason><SOAP-ENV:Text xml:lang="en">fault</SOAP-ENV:Text></SOAP-ENV:Reason></SOAP-ENV:Fault></SOAP-ENV:Body></SOAP-ENV:Envelope>`, subcode)
}

func soapResponse(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/soap+xml")
	fmt.Fprintf(w, `<?xml version="1.0"?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:wse="http://schemas.xmlsoap.org/ws/2004/08/eventing" xmlns:wscn="http://schemas.microsoft.com/windows/2006/08/wdp/scan"><SOAP-ENV:Body>%s</SOAP-ENV:Body></SOAP-ENV:Envelope>`, body)
}

func (f *fakeScanner) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	body := string(raw)
	m := actionRE.FindStringSubmatch(body)
	if m == nil {
		http.Error(w, "no action", 400)
		return
	}
	action := m[1][strings.LastIndexByte(m[1], '/')+1:]
	f.mu.Lock()
	defer f.mu.Unlock()
	switch action {
	case "GetScannerElements":
		soapResponse(w, `<wscn:GetScannerElementsResponse><wscn:ScannerElements><wscn:ElementData Name="wscn:ScannerDescription" Valid="true"><wscn:ScannerDescription><wscn:ScannerName xml:lang="en-US">Test MFC Scanner</wscn:ScannerName></wscn:ScannerDescription></wscn:ElementData></wscn:ScannerElements></wscn:GetScannerElementsResponse>`)
	case "CreateScanJob":
		f.tickets = append(f.tickets, body)
		if len(f.passes) == 0 {
			fault(w, "ServerErrorNotAcceptingJobs")
			return
		}
		f.nextJob++
		id := fmt.Sprint(f.nextJob)
		if f.jobs == nil {
			f.jobs = map[string][][]byte{}
		}
		f.jobs[id] = f.passes[0]
		f.passes = f.passes[1:]
		soapResponse(w, `<wscn:CreateScanJobResponse><wscn:JobId>`+id+`</wscn:JobId><wscn:JobToken>urn:uuid:job-`+id+`</wscn:JobToken></wscn:CreateScanJobResponse>`)
	case "RetrieveImage":
		id := jobIDRE.FindStringSubmatch(body)[1]
		pages := f.jobs[id]
		if len(pages) == 0 {
			delete(f.jobs, id)
			fault(w, "ClientErrorJobIdNotFound")
			return
		}
		f.jobs[id] = pages[1:]
		boundary := "b9dd7049-1890-4c80-82ee-000000000000"
		w.Header().Set("Content-Type", `Multipart/Related; boundary="`+boundary+`"; type="application/xop+xml"; start="<root@uuid>"; start-info="application/soap+xml"`)
		var b bytes.Buffer
		b.WriteString("\r\n--" + boundary + "\r\nContent-Type: application/xop+xml; type=\"application/soap+xml\"; charset=utf-8\r\nContent-Transfer-Encoding: binary\r\nContent-ID: <root@uuid>\r\n\r\n")
		b.WriteString(`<?xml version="1.0"?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope" xmlns:wscn="http://schemas.microsoft.com/windows/2006/08/wdp/scan"><SOAP-ENV:Body><wscn:RetrieveImageResponse><wscn:ScanData><xop:Include xmlns:xop="http://www.w3.org/2004/08/xop/include" href="cid:image@uuid"/></wscn:ScanData></wscn:RetrieveImageResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`)
		b.WriteString("\r\n--" + boundary + "\r\nContent-Type: application/binary\r\nContent-Transfer-Encoding: binary\r\nContent-ID: <image@uuid>\r\n\r\n")
		b.Write(pages[0])
		b.WriteString("\r\n--" + boundary + "--\r\n")
		_, _ = w.Write(b.Bytes())
	default:
		http.Error(w, "unknown action "+action, 400)
	}
}

func testdata(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// markedJPEG is the test JPEG with a trailing comment naming it, so a test can
// tell pages apart in the PDF.
func markedJPEG(t *testing.T, label string) []byte {
	t.Helper()
	data := testdata(t, "page-color.jpg")
	// Insert a COM segment after SOI.
	com := append([]byte{0xFF, 0xFE, 0, byte(len(label) + 2)}, []byte(label)...)
	return append(append(append([]byte{}, data[:2]...), com...), data[2:]...)
}

type fakeUploader struct {
	mu        sync.Mutex
	documents [][]byte
	batches   []map[string]any
	fail      bool
}

func (u *fakeUploader) UploadScanDocument(path, scannedAt, extension, contentType, sha string) (ingestclient.StoredObject, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.fail {
		return ingestclient.StoredObject{}, fmt.Errorf("offline")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ingestclient.StoredObject{}, err
	}
	if common.BytesSHA256(data) != sha || extension != ".pdf" || contentType != "application/pdf" || scannedAt == "" {
		return ingestclient.StoredObject{}, fmt.Errorf("bad document upload")
	}
	u.documents = append(u.documents, data)
	return ingestclient.StoredObject{StorageBackend: "google_drive", StorageKey: "scans/library/documents/x-" + sha + ".pdf", StorageFileID: "file-" + sha[:8]}, nil
}

func (u *fakeUploader) UploadScansBatch(gz []byte, exportedAt string) (ingestclient.StoredObject, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return ingestclient.StoredObject{}, err
	}
	raw, _ := io.ReadAll(zr)
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		var envelope map[string]any
		if err := json.Unmarshal(line, &envelope); err != nil {
			return ingestclient.StoredObject{}, err
		}
		u.batches = append(u.batches, envelope)
	}
	return ingestclient.StoredObject{StorageKey: "scans/inbox/batches/x.jsonl.gz"}, nil
}

func newTestScanner(t *testing.T, fake *fakeScanner) (*Scanner, *fakeUploader, *time.Time) {
	t.Helper()
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	now := time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)
	uploader := &fakeUploader{}
	s := &Scanner{
		Client:   &Client{ServiceURL: server.URL + "/WebServices/ScannerService"},
		Uploader: uploader, Logger: &common.RecordingLogger{}, Account: "me@example.com", Device: "scanbox",
		ScannerName: "Test MFC Scanner", SpoolDir: t.TempDir(), Resolution: 300, PageSize: "letter",
		DuplexWindow: 10 * time.Minute, Now: func() time.Time { return now },
	}
	return s, uploader, &now
}

func markers(t *testing.T, pdf []byte) []string {
	t.Helper()
	var out []string
	for _, m := range regexp.MustCompile(`(page-[a-z][0-9])`).FindAllSubmatch(pdf, -1) {
		out = append(out, string(m[1]))
	}
	return out
}

func TestColorScanIsFiledAsAPDFAndSentWithItsRecord(t *testing.T) {
	fake := &fakeScanner{}
	fake.queue(markedJPEG(t, "page-f1"), markedJPEG(t, "page-f2"))
	s, uploader, _ := newTestScanner(t, fake)
	s.ADFLoaded = func(context.Context) (bool, error) { return true, nil }
	profile, _ := ProfileByContext("pdw-color")
	if err := s.HandleScan(context.Background(), profile); err != nil {
		t.Fatal(err)
	}
	ticket := fake.tickets[0]
	for _, want := range []string{"<sca:Format>exif<", "<sca:ColorProcessing>RGB24<", "<sca:InputSource>ADF<", "<sca:Width>300<"} {
		if !strings.Contains(ticket, want) {
			t.Fatalf("ticket lacks %s:\n%s", want, ticket)
		}
	}
	sent, err := s.SendOutbox()
	if err != nil || sent != 1 {
		t.Fatalf("SendOutbox = %d, %v", sent, err)
	}
	if got := markers(t, uploader.documents[0]); strings.Join(got, ",") != "page-f1,page-f2" {
		t.Fatalf("pages = %v", got)
	}
	if len(uploader.batches) != 1 {
		t.Fatalf("batches = %d", len(uploader.batches))
	}
	envelope := uploader.batches[0]
	record := envelope["record"].(map[string]any)
	if envelope["account"] != "me@example.com" || envelope["device"] != "scanbox" || envelope["source"] != "scans" || envelope["record_type"] != "scan" {
		t.Fatalf("envelope = %v", envelope)
	}
	checks := map[string]any{
		"profile": "pdw-color", "color_mode": "color", "page_count": 2.0, "is_duplex": false, "duplex_status": "",
		"input_source": "ADF", "content_type": "application/pdf", "scanner_name": "Test MFC Scanner",
		"storage_backend": "google_drive", "resolution_dpi": 300.0, "scanned_at": "2026-11-01T14:00:00Z",
	}
	for key, want := range checks {
		if record[key] != want {
			t.Fatalf("record[%s] = %#v, want %#v (record %v)", key, record[key], want, record)
		}
	}
	if record["content_sha256"] != common.BytesSHA256(uploader.documents[0]) || record["storage_file_id"] == "" {
		t.Fatalf("record does not point at the stored PDF: %v", record)
	}
	if entries, _ := os.ReadDir(s.outboxDir()); len(entries) != 0 {
		t.Fatalf("outbox not emptied: %v", entries)
	}
}

func TestAScanStaysSpooledUntilTheWarehouseHasIt(t *testing.T) {
	fake := &fakeScanner{}
	fake.queue(testdata(t, "page-lsb.tif"))
	s, uploader, _ := newTestScanner(t, fake)
	uploader.fail = true
	s.ADFLoaded = func(context.Context) (bool, error) { return false, nil }
	profile, _ := ProfileByContext("pdw-bw")
	if err := s.HandleScan(context.Background(), profile); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SendOutbox(); err == nil {
		t.Fatal("SendOutbox succeeded while offline")
	}
	if entries, _ := os.ReadDir(s.outboxDir()); len(entries) != 1 {
		t.Fatalf("scan was not kept: %v", entries)
	}
	uploader.fail = false
	if sent, err := s.SendOutbox(); err != nil || sent != 1 {
		t.Fatalf("SendOutbox = %d, %v", sent, err)
	}
	record := uploader.batches[0]["record"].(map[string]any)
	if record["color_mode"] != "black_and_white" || record["input_source"] != "Platen" {
		t.Fatalf("record = %v", record)
	}
	if !strings.Contains(fake.tickets[0], "<sca:Format>tiff-single-g4<") || !strings.Contains(fake.tickets[0], "<sca:ColorProcessing>BlackAndWhite1<") {
		t.Fatalf("ticket = %s", fake.tickets[0])
	}
}

func TestFakeDuplexInterleavesTheFlippedStacksBacks(t *testing.T) {
	fake := &fakeScanner{}
	// Three sheets: the fronts in order, then the flipped stack's backs,
	// last sheet first.
	fake.queue(markedJPEG(t, "page-f1"), markedJPEG(t, "page-f2"), markedJPEG(t, "page-f3"))
	fake.queue(markedJPEG(t, "page-b3"), markedJPEG(t, "page-b2"), markedJPEG(t, "page-b1"))
	s, uploader, now := newTestScanner(t, fake)
	profile := Profile{Context: "pdw-duplex-color", PanelName: "PDW-DuplexColor", Format: "exif", Color: "RGB24", Duplex: true}
	if err := s.HandleScan(context.Background(), profile); err != nil {
		t.Fatal(err)
	}
	if sent, _ := s.SendOutbox(); sent != 0 {
		t.Fatal("a first pass was sent before its second")
	}
	if got := s.PendingDuplex(); len(got) != 1 || got[0] != "pdw-duplex-color" {
		t.Fatalf("pending = %v", got)
	}
	*now = now.Add(4 * time.Minute)
	if err := s.HandleScan(context.Background(), profile); err != nil {
		t.Fatal(err)
	}
	if sent, err := s.SendOutbox(); err != nil || sent != 1 {
		t.Fatalf("SendOutbox = %d, %v", sent, err)
	}
	if got := strings.Join(markers(t, uploader.documents[0]), ","); got != "page-f1,page-b1,page-f2,page-b2,page-f3,page-b3" {
		t.Fatalf("pages = %s", got)
	}
	record := uploader.batches[0]["record"].(map[string]any)
	if record["duplex_status"] != DuplexPaired || record["page_count"] != 6.0 || record["front_page_count"] != 3.0 ||
		record["back_page_count"] != 3.0 || record["is_duplex"] != true || record["scanned_at"] != "2026-11-01T14:00:00Z" {
		t.Fatalf("record = %v", record)
	}
	if len(s.PendingDuplex()) != 0 {
		t.Fatal("the pair was not cleared")
	}
}

func TestADuplexFirstPassIsFiledAloneWhenItsWindowCloses(t *testing.T) {
	fake := &fakeScanner{}
	fake.queue(markedJPEG(t, "page-f1"), markedJPEG(t, "page-f2"))
	fake.queue(markedJPEG(t, "page-n1"))
	s, uploader, now := newTestScanner(t, fake)
	profile := Profile{Context: "pdw-duplex-color", PanelName: "PDW-DuplexColor", Format: "exif", Color: "RGB24", Duplex: true}
	if err := s.HandleScan(context.Background(), profile); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(11 * time.Minute)
	// A scan after the window is a new first pass, not the old one's backs.
	if err := s.HandleScan(context.Background(), profile); err != nil {
		t.Fatal(err)
	}
	if sent, err := s.SendOutbox(); err != nil || sent != 1 {
		t.Fatalf("SendOutbox = %d, %v", sent, err)
	}
	if got := strings.Join(markers(t, uploader.documents[0]), ","); got != "page-f1,page-f2" {
		t.Fatalf("pages = %s", got)
	}
	if record := uploader.batches[0]["record"].(map[string]any); record["duplex_status"] != DuplexFrontsOnly || record["back_page_count"] != 0.0 {
		t.Fatalf("record = %v", record)
	}
	*now = now.Add(11 * time.Minute)
	if err := s.FlushExpired(); err != nil {
		t.Fatal(err)
	}
	if sent, err := s.SendOutbox(); err != nil || sent != 1 {
		t.Fatalf("SendOutbox = %d, %v", sent, err)
	}
	if got := strings.Join(markers(t, uploader.documents[1]), ","); got != "page-n1" {
		t.Fatalf("pages = %s", got)
	}
}

func TestADuplexFirstPassSurvivesARestart(t *testing.T) {
	fake := &fakeScanner{}
	fake.queue(markedJPEG(t, "page-f1"))
	fake.queue(markedJPEG(t, "page-b1"), markedJPEG(t, "page-b0"))
	s, uploader, _ := newTestScanner(t, fake)
	profile := Profile{Context: "pdw-duplex-color", PanelName: "PDW-DuplexColor", Format: "exif", Color: "RGB24", Duplex: true}
	if err := s.HandleScan(context.Background(), profile); err != nil {
		t.Fatal(err)
	}
	restarted := &Scanner{
		Client: s.Client, Uploader: uploader, Logger: s.Logger, Account: s.Account, Device: s.Device,
		SpoolDir: s.SpoolDir, Resolution: 300, PageSize: "letter", DuplexWindow: s.DuplexWindow, Now: s.Now,
	}
	if err := restarted.HandleScan(context.Background(), profile); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.SendOutbox(); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(markers(t, uploader.documents[0]), ","); got != "page-f1,page-b0,page-b1" {
		t.Fatalf("pages = %s", got)
	}
	if record := uploader.batches[0]["record"].(map[string]any); record["duplex_status"] != DuplexMismatched {
		t.Fatalf("record = %v", record)
	}
}

func TestAnUnknownFeederStateFallsBackToTheGlass(t *testing.T) {
	fake := &fakeScanner{}
	s, _, _ := newTestScanner(t, fake)
	// The first CreateScanJob (feeder) is refused because nothing is queued;
	// queue the glass page once it has been asked for.
	fakeRefuseOnce := &refuseFirstJob{fakeScanner: fake, page: markedJPEG(t, "page-g1")}
	server := httptest.NewServer(fakeRefuseOnce)
	defer server.Close()
	s.Client = &Client{ServiceURL: server.URL}
	profile, _ := ProfileByContext("pdw-color")
	if err := s.HandleScan(context.Background(), profile); err != nil {
		t.Fatal(err)
	}
	if len(fake.tickets) != 2 || !strings.Contains(fake.tickets[0], "<sca:InputSource>ADF<") || !strings.Contains(fake.tickets[1], "<sca:InputSource>Platen<") {
		t.Fatalf("tickets = %v", fake.tickets)
	}
}

type refuseFirstJob struct {
	*fakeScanner
	page    []byte
	refused bool
}

func (r *refuseFirstJob) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	raw, _ := io.ReadAll(req.Body)
	if strings.Contains(string(raw), "CreateScanJob") && !r.refused {
		r.refused = true
		r.fakeScanner.mu.Lock()
		r.fakeScanner.tickets = append(r.fakeScanner.tickets, string(raw))
		r.fakeScanner.mu.Unlock()
		fault(w, "ServerErrorNoPaperInFeeder")
		return
	}
	if strings.Contains(string(raw), "CreateScanJob") {
		r.fakeScanner.queue(r.page)
	}
	req.Body = io.NopCloser(bytes.NewReader(raw))
	r.fakeScanner.ServeHTTP(w, req)
}

func TestBuildPDFCarriesG4PagesInEitherBitOrderUnchanged(t *testing.T) {
	msb, err := g4Image(Page{Format: "tiff-single-g4", Data: testdata(t, "page-msb.tif"), DPI: 300})
	if err != nil {
		t.Fatal(err)
	}
	lsb, err := g4Image(Page{Format: "tiff-single-g4", Data: testdata(t, "page-lsb.tif"), DPI: 300})
	if err != nil {
		t.Fatal(err)
	}
	// FillOrder 2 is bit-reversed back into the order a PDF decoder reads.
	if !bytes.Equal(msb.stream, lsb.stream) {
		t.Fatal("an LSB-first G4 strip was not reversed into the MSB-first order")
	}
	if msb.width != 600 || msb.height != 300 || msb.dpiX != 300 {
		t.Fatalf("image = %dx%d at %v dpi", msb.width, msb.height, msb.dpiX)
	}
	// These fixtures are MinIsBlack; Brother's pages are MinIsWhite.
	if !strings.Contains(msb.dict, "/BlackIs1 true") || !strings.Contains(msb.dict, "/K -1 /Columns 600 /Rows 300") {
		t.Fatalf("dict = %s", msb.dict)
	}
	pdf, err := BuildPDF([]Page{
		{Format: "tiff-single-g4", Data: testdata(t, "page-lsb.tif"), DPI: 300},
		{Format: "exif", Data: testdata(t, "page-color.jpg"), DPI: 300},
		{Format: "exif", Data: testdata(t, "page-gray.jpg"), DPI: 300},
	}, "Scan (test)", time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	text := string(pdf)
	for _, want := range []string{"/Count 3", "/CCITTFaxDecode", "/DeviceRGB", "/DeviceGray /BitsPerComponent 8", "/MediaBox [0 0 144.00 72.00]", "/Title (Scan \\(test\\))", "%%EOF"} {
		if !strings.Contains(text, want) {
			t.Fatalf("PDF lacks %q", want)
		}
	}
	// Every xref offset points at its object.
	xref := regexp.MustCompile(`(\d{10}) 00000 n`).FindAllStringSubmatch(text, -1)
	for i, m := range xref {
		off, _ := strconv.Atoi(m[1])
		if !strings.HasPrefix(text[off:], fmt.Sprintf("%d 0 obj", i+1)) {
			t.Fatalf("xref entry %d points at %q", i+1, text[off:off+10])
		}
	}
}

func TestBuildPDFRefusesWhatItCannotCarry(t *testing.T) {
	if _, err := BuildPDF(nil, "x", time.Now()); err == nil {
		t.Fatal("an empty scan built a PDF")
	}
	if _, err := BuildPDF([]Page{{Format: "tiff-single-g4", Data: testdata(t, "page-color.jpg")}}, "x", time.Now()); err == nil {
		t.Fatal("a JPEG was accepted as a TIFF")
	}
	if _, err := BuildPDF([]Page{{Format: "png", Data: []byte("x")}}, "x", time.Now()); err == nil {
		t.Fatal("an unknown format was accepted")
	}
}

func TestInterleave(t *testing.T) {
	p := func(names ...string) []Page {
		out := []Page{}
		for _, n := range names {
			out = append(out, Page{Format: n})
		}
		return out
	}
	names := func(pages []Page) string {
		out := []string{}
		for _, pg := range pages {
			out = append(out, pg.Format)
		}
		return strings.Join(out, ",")
	}
	cases := []struct {
		fronts, backs []Page
		want          string
	}{
		{p("f1", "f2"), p("b2", "b1"), "f1,b1,f2,b2"},
		{p("f1"), p("b1"), "f1,b1"},
		{p("f1", "f2", "f3"), p("b2", "b1"), "f1,b1,f2,b2,f3"},
		{p("f1"), p("b2", "b1"), "f1,b1,b2"},
	}
	for _, c := range cases {
		if got := names(Interleave(c.fronts, c.backs)); got != c.want {
			t.Errorf("Interleave = %s, want %s", got, c.want)
		}
	}
}

func TestParsesTheScannersMessages(t *testing.T) {
	press, ok := ParseButtonEvent([]byte("\x02\x00{0TYPE=BR;BUTTON=SCAN;USER=\"PDW-DuplexBW\";FUNC=FILE;HOST=192.0.2.41:54925;APPNUM=5;P1=0;P2=0;P3=0;P4=0;REGID=46310;SEQ=1;"))
	if !ok || press != (ButtonEvent{User: "PDW-DuplexBW", Function: "FILE", RegID: "46310", Seq: "1"}) {
		t.Fatalf("press = %+v, %v", press, ok)
	}
	if _, ok := ParseButtonEvent([]byte("hello")); ok {
		t.Fatal("noise parsed as a press")
	}
	if got := RegistrationValue("PDW-Color", "FILE", "192.0.2.41:54925", 5); got != `TYPE=BR;BUTTON=SCAN;USER="PDW-Color";FUNC=FILE;HOST=192.0.2.41:54925;APPNUM=5;DURATION=360;` {
		t.Fatalf("registration = %s", got)
	}
	loaded, err := ParseADFState([]byte(`<scan:ScannerStatus><pwg:State>Idle</pwg:State><scan:AdfState>ScannerAdfLoaded</scan:AdfState></scan:ScannerStatus>`))
	if err != nil || !loaded {
		t.Fatalf("loaded = %v, %v", loaded, err)
	}
	loaded, err = ParseADFState([]byte(`<scan:AdfState>ScannerAdfEmpty</scan:AdfState>`))
	if err != nil || loaded {
		t.Fatalf("empty = %v, %v", loaded, err)
	}
	if _, err := ParseADFState([]byte(`<scan:ScannerStatus/>`)); err == nil {
		t.Fatal("a status without AdfState read as known")
	}
	if got, err := ServiceURL("192.0.2.216"); err != nil || got != "http://192.0.2.216/WebServices/ScannerService" {
		t.Fatalf("ServiceURL = %q, %v", got, err)
	}
	if _, err := ServiceURL("http://x/y"); err == nil {
		t.Fatal("a URL was accepted as a host")
	}
	for _, p := range Profiles {
		if len(p.PanelName) > 15 || strings.ContainsAny(p.PanelName, " ") {
			t.Fatalf("panel name %q does not fit the panel", p.PanelName)
		}
	}
}

// fakeSNMP answers SetRequests like a scanner: status 0 for the write
// community, noAccess for any other, and records each registration.
type fakeSNMP struct {
	conn net.PacketConn
	mu   sync.Mutex
	sets []string
}

func startFakeSNMP(t *testing.T) *fakeSNMP {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeSNMP{conn: conn}
	t.Cleanup(func() { conn.Close() })
	go func() {
		buf := make([]byte, 4096)
		for {
			n, from, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			r := berReader{data: buf[:n]}
			msg, _ := r.next(0x30)
			m := berReader{data: msg}
			m.nextInt()
			community, _ := m.next(0x04)
			pdu, _ := m.next(0xa3)
			p := berReader{data: pdu}
			id, _ := p.nextInt()
			p.nextInt()
			p.nextInt()
			vbs, _ := p.next(0x30)
			v := berReader{data: vbs}
			vb, _ := v.next(0x30)
			b := berReader{data: vb}
			b.next(0x06)
			value, _ := b.next(0x04)
			status := 0
			if string(community) != "internal" {
				status = 6
			} else {
				f.mu.Lock()
				f.sets = append(f.sets, string(value))
				f.mu.Unlock()
			}
			resp := berSeq(berInt(0), berOctets(community), berTLV(0xa2, append(append(append(berInt(id), berInt(status)...), berInt(0)...), berSeq()...)))
			conn.WriteTo(resp, from)
		}
	}()
	return f
}

func TestRegistrationPutsEveryNameUnderEveryFunction(t *testing.T) {
	snmp := startFakeSNMP(t)
	_, port, _ := net.SplitHostPort(snmp.conn.LocalAddr().String())
	old := snmpPort
	snmpPort = port
	defer func() { snmpPort = old }()
	if err := Register(context.Background(), "127.0.0.1", "internal", "192.0.2.41:54925", PanelNames()); err != nil {
		t.Fatal(err)
	}
	snmp.mu.Lock()
	sets := append([]string(nil), snmp.sets...)
	snmp.mu.Unlock()
	if len(sets) != 16 {
		t.Fatalf("registrations = %d, want 4 names x 4 functions", len(sets))
	}
	if !strings.Contains(strings.Join(sets, "\n"), `USER="PDW-DuplexBW";FUNC=FILE;HOST=192.0.2.41:54925;APPNUM=5;`) {
		t.Fatalf("registrations = %v", sets)
	}
	err := Register(context.Background(), "127.0.0.1", "public", "192.0.2.41:54925", PanelNames())
	if err == nil || !strings.Contains(err.Error(), "noAccess") {
		t.Fatalf("a refused write = %v", err)
	}
}

func TestTheDaemonScansOncePerPressAndReportsItsHealth(t *testing.T) {
	snmp := startFakeSNMP(t)
	_, port, _ := net.SplitHostPort(snmp.conn.LocalAddr().String())
	old := snmpPort
	snmpPort = port
	defer func() { snmpPort = old }()
	fake := &fakeScanner{}
	fake.queue(markedJPEG(t, "page-p1"))
	s, uploader, _ := newTestScanner(t, fake)
	heartbeats := &fakeHeartbeats{}
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	d := &Daemon{Scanner: s, Heartbeats: heartbeats, ScannerHost: "127.0.0.1", Community: "internal", HostPort: conn.LocalAddr().String()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Serve(ctx, conn) }()
	waitFor(t, func() bool { snmp.mu.Lock(); defer snmp.mu.Unlock(); return len(snmp.sets) == 16 })
	// The scanner sends a press twice; it is one scan.
	press := []byte("\x02\x00{0TYPE=BR;BUTTON=SCAN;USER=\"PDW-Color\";FUNC=FILE;HOST=x;APPNUM=5;P1=0;P2=0;P3=0;P4=0;REGID=7;SEQ=1;")
	client, err := net.Dial("udp", conn.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.Write(press)
	client.Write(press)
	waitFor(t, func() bool { uploader.mu.Lock(); defer uploader.mu.Unlock(); return len(uploader.batches) == 1 })
	time.Sleep(100 * time.Millisecond)
	fake.mu.Lock()
	jobs := len(fake.tickets)
	fake.mu.Unlock()
	if jobs != 1 {
		t.Fatalf("scan jobs = %d, want one per press", jobs)
	}
	if record := uploader.batches[0]["record"].(map[string]any); record["profile"] != "pdw-color" {
		t.Fatalf("record = %v", record)
	}
	cancel()
	<-done
	heartbeats.mu.Lock()
	defer heartbeats.mu.Unlock()
	if len(heartbeats.sent) == 0 || heartbeats.sent[0].Pipeline != "scans" || heartbeats.sent[0].ExitCode != 0 || heartbeats.sent[0].Device != "scanbox" {
		t.Fatalf("heartbeats = %+v", heartbeats.sent)
	}
}

type fakeHeartbeats struct {
	mu   sync.Mutex
	sent []ingestclient.Heartbeat
}

func (f *fakeHeartbeats) PostHeartbeat(h ingestclient.Heartbeat) (map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, h)
	return nil, nil
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out")
}
