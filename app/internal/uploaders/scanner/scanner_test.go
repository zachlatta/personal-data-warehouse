package scanner

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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

// fakeScanner is a Brother scanner's native scan port: each X request
// streams the next queued pass in the record format an MFC-L2710DW sends
// (JPEG data in 64 KiB records, a page-end record per page, then 0x80).
type fakeScanner struct {
	mu       sync.Mutex
	passes   [][][]byte
	requests []string
	// busy answers this many connections "-NG 401" first.
	busy     int
	listener net.Listener
}

func (f *fakeScanner) queue(pages ...[]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.passes = append(f.passes, pages)
}

func (f *fakeScanner) start(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.listener = listener
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go f.serve(conn)
		}
	}()
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	return port
}

func readRequest(r *bufio.Reader) (string, error) {
	if b, err := r.ReadByte(); err != nil || b != 0x1b {
		return "", fmt.Errorf("bad request start")
	}
	raw, err := r.ReadBytes(0x80)
	if err != nil {
		return "", err
	}
	return string(raw[:len(raw)-1]), nil
}

func (f *fakeScanner) serve(conn net.Conn) {
	defer conn.Close()
	f.mu.Lock()
	if f.busy > 0 {
		f.busy--
		f.mu.Unlock()
		conn.Write([]byte("-NG 401\r\n"))
		return
	}
	f.mu.Unlock()
	conn.Write([]byte("+OK 200\r\n"))
	r := bufio.NewReader(conn)
	for {
		req, err := readRequest(r)
		if err != nil {
			return
		}
		f.mu.Lock()
		f.requests = append(f.requests, req)
		f.mu.Unlock()
		switch req[0] {
		case 'I':
			info := "300,300,1,209,2479,0,0,\x00"
			conn.Write(append([]byte{0, byte(len(info)), 0}, info...))
		case 'X':
			f.mu.Lock()
			if len(f.passes) == 0 {
				f.mu.Unlock()
				conn.Write([]byte{0xc2, 0x00})
				return
			}
			pages := f.passes[0]
			f.passes = f.passes[1:]
			f.mu.Unlock()
			var out []byte
			for n, page := range pages {
				header := []byte{byte(n + 1), 0, 0x84, 0x20, 0x01, 0, 0}
				for off := 0; off < len(page); off += 1000 {
					chunk := page[off:min(off+1000, len(page))]
					out = append(out, 0x64, 7, 0)
					out = append(out, header...)
					out = append(out, byte(len(chunk)), byte(len(chunk)>>8))
					out = append(out, chunk...)
				}
				out = append(out, 0x82, 7, 0, byte(n+1), 0, 0x84, 0, 0, 0, 0)
			}
			conn.Write(append(out, 0x80))
			return
		}
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
	port := fake.start(t)
	now := time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)
	uploader := &fakeUploader{}
	s := &Scanner{
		Client:   &NativeClient{Host: "127.0.0.1", Port: port, BusyDelay: time.Millisecond},
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
	profile, _ := ProfileByContext("pdw-color")
	if err := s.HandleScan(context.Background(), profile); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(fake.requests, "|"); got != "I\nR=300,300\nM=CGRAY\n|X\nR=300,300\nM=CGRAY\nC=JPEG\nJ=MID\nB=50\nN=50\nA=0,0,2480,3300\nD=SIN\nG=1\nL=128\n" {
		t.Fatalf("requests = %q", got)
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
		"input_source": "auto", "content_type": "application/pdf", "scanner_name": "Test MFC Scanner",
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

func TestABusyScannerIsWaitedOutAndAnEmptyOneIsAnError(t *testing.T) {
	fake := &fakeScanner{busy: 3}
	fake.queue(markedJPEG(t, "page-f1"))
	s, _, _ := newTestScanner(t, fake)
	profile, _ := ProfileByContext("pdw-color")
	if err := s.HandleScan(context.Background(), profile); err != nil {
		t.Fatal(err)
	}
	err := s.HandleScan(context.Background(), profile)
	if !errors.Is(err, ErrNoPaper) {
		t.Fatalf("an empty scanner = %v", err)
	}
}

func TestAScanStaysSpooledUntilTheWarehouseHasIt(t *testing.T) {
	fake := &fakeScanner{}
	fake.queue(testdata(t, "page-gray.jpg"))
	s, uploader, _ := newTestScanner(t, fake)
	uploader.fail = true
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
	if record["color_mode"] != "grayscale" {
		t.Fatalf("record = %v", record)
	}
	if !strings.Contains(fake.requests[1], "M=GRAY64\n") {
		t.Fatalf("requests = %q", fake.requests)
	}
}

func TestFakeDuplexInterleavesTheFlippedStacksBacks(t *testing.T) {
	fake := &fakeScanner{}
	// Three sheets: the fronts in order, then the flipped stack's backs,
	// last sheet first.
	fake.queue(markedJPEG(t, "page-f1"), markedJPEG(t, "page-f2"), markedJPEG(t, "page-f3"))
	fake.queue(markedJPEG(t, "page-b3"), markedJPEG(t, "page-b2"), markedJPEG(t, "page-b1"))
	s, uploader, now := newTestScanner(t, fake)
	profile := Profile{Context: "pdw-duplex-color", PanelName: "PDW-DuplexColor", Mode: ModeColor, Duplex: true}
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
	profile := Profile{Context: "pdw-duplex-color", PanelName: "PDW-DuplexColor", Mode: ModeColor, Duplex: true}
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
	profile := Profile{Context: "pdw-duplex-color", PanelName: "PDW-DuplexColor", Mode: ModeColor, Duplex: true}
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

func TestBuildPDFCarriesJPEGPagesUnchanged(t *testing.T) {
	color, gray := testdata(t, "page-color.jpg"), testdata(t, "page-gray.jpg")
	pdf, err := BuildPDF([]Page{
		{Format: "exif", Data: color, DPI: 300},
		{Format: "exif", Data: gray, DPI: 300},
	}, "Scan (test)", time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	text := string(pdf)
	for _, want := range []string{"/Count 2", "/DeviceRGB", "/DeviceGray /BitsPerComponent 8", "/MediaBox [0 0 72.00 48.00]", "/Title (Scan \\(test\\))", "%%EOF"} {
		if !strings.Contains(text, want) {
			t.Fatalf("PDF lacks %q", want)
		}
	}
	if !bytes.Contains(pdf, color) || !bytes.Contains(pdf, gray) {
		t.Fatal("a page was re-encoded")
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
	if _, err := BuildPDF([]Page{{Format: "exif", Data: []byte("not a jpeg")}}, "x", time.Now()); err == nil {
		t.Fatal("a non-JPEG was accepted")
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
	if got, err := ParseMakeAndModel([]byte(`<scan:ScannerCapabilities><pwg:MakeAndModel>Test MFC Scanner</pwg:MakeAndModel>`)); err != nil || got != "Test MFC Scanner" {
		t.Fatalf("ParseMakeAndModel = %q, %v", got, err)
	}
	if err := checkHost("192.0.2.216"); err != nil {
		t.Fatal(err)
	}
	if err := checkHost("http://x/y"); err == nil {
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
	jobs := 0
	for _, r := range fake.requests {
		if strings.HasPrefix(r, "X") {
			jobs++
		}
	}
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
