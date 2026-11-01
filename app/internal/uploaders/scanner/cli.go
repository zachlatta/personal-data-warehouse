package scanner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Usage is `pdw ingest scanner --help`.
const Usage = `pdw ingest scanner - scan into the warehouse from a Brother scanner's panel.

USAGE
  pdw ingest scanner --scanner HOST [--account EMAIL] [--device NAME]
                     [--listen ADDR] [--host-address IP] [--community NAME]
                     [--spool-dir DIR] [--resolution DPI] [--page-size letter|legal|a4]
                     [--duplex-window DURATION] [--lock-file PATH]
  pdw ingest scanner --scanner HOST --scan pdw-color|pdw-bw [--dry-run]

Runs on a computer on the scanner's LAN, as a long-lived service. It puts
four names on the scanner's Scan to PC button (Scan > to PC > File, or
Image, OCR, E-mail):

  PDW-Color         color, 300 dpi
  PDW-BW            grayscale (reads as black and white), 300 dpi
  PDW-DuplexColor   two passes: fronts, then the flipped stack's backs
  PDW-DuplexBW      the same in black and white

Pressing Start on one sends the scan here. The pages become one PDF, sent
with its record to the warehouse (/ingest/scans), where it lands in
base_scans.documents and on the timeline, and a push notification says it
arrived. Paper in the feeder is scanned from the feeder, otherwise from the
glass.

Duplex: scan the stack on a Duplex name, flip it over without reordering,
and scan it again on the same name within the duplex window. The pages are
interleaved front, back, front, back. A first pass with no second one is
filed on its own when the window closes.

The scanner must allow SNMP v1/v2c writes (web admin: Network > Protocol >
SNMP > "SNMP v1/v2c read-write access"; power-cycle it after changing it).

Scans are spooled under --spool-dir until the warehouse has them, so an
outage or restart delays a scan rather than losing it. A heartbeat is posted
every ten minutes (pipeline "scans").

FLAGS
  --scanner HOST         The scanner's address (default SCANNER_HOST).
  --account EMAIL        Account the scans are keyed by (default
                         SCANNER_ACCOUNT, else the first GMAIL_ACCOUNTS).
  --device NAME          This computer's name in the warehouse (default
                         SCANNER_DEVICE, else the hostname).
  --listen ADDR          UDP address button presses arrive on (default :54925).
  --host-address IP      This computer's address as the scanner reaches it
                         (default: the local address on the route to it).
  --community NAME       SNMP write community (default internal).
  --spool-dir DIR        Scans waiting to be sent (default: the state directory).
  --resolution DPI       100, 200, 300 or 600 (default 300).
  --page-size SIZE       letter, legal or a4 (default letter).
  --duplex-window D      How long a duplex first pass waits (default 10m).
  --scan PROFILE         Scan once from here (no panel) and exit: pdw-color or pdw-bw.
  --dry-run              With --scan: write the PDF to the spool and send nothing.
  --lock-file PATH       Lock against a second copy (default: the state directory).

ENVIRONMENT
  SCANNER_HOST, SCANNER_ACCOUNT, SCANNER_DEVICE
  PDW_API_URL, PDW_SECRET_TOKEN   (or pdw's own config file)
`

const (
	// Registrations last RegistrationLifetime (6 min); they are written
	// again every two, so a scanner restart costs at most two minutes.
	registerEvery     = 2 * time.Minute
	housekeepingEvery = 30 * time.Second
	heartbeatEvery    = 10 * time.Minute
	// The scanner repeats a press's datagram; one press is one scan.
	pressDedupWindow = 2 * time.Minute
	// HeartbeatPipeline is the pipeline the daemon's heartbeat is filed under.
	HeartbeatPipeline = "scans"
)

// Run is the `pdw ingest scanner` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest scanner")
	scannerFlag := fs.String("scanner", "", "")
	account := fs.String("account", "", "")
	device := fs.String("device", "", "")
	listen := fs.String("listen", ":54925", "")
	hostAddress := fs.String("host-address", "", "")
	community := fs.String("community", "internal", "")
	spoolDir := fs.String("spool-dir", common.DefaultStateFile("scanner"), "")
	resolution := fs.Int("resolution", 300, "")
	pageSize := fs.String("page-size", "letter", "")
	duplexWindow := fs.Duration("duplex-window", 10*time.Minute, "")
	scanOnce := fs.String("scan", "", "")
	dryRun := fs.Bool("dry-run", false, "")
	lockFile := fs.String("lock-file", common.DefaultStateFile("scanner.lock"), "")
	if _, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr); done {
		return code
	}
	switch *resolution {
	case 100, 200, 300, 600:
	default:
		return common.UsageError(fs, stderr, "--resolution must be 100, 200, 300 or 600")
	}
	if _, ok := PageHeights[*pageSize]; !ok {
		return common.UsageError(fs, stderr, "--page-size must be letter, legal or a4")
	}
	if *duplexWindow <= 0 {
		return common.UsageError(fs, stderr, "--duplex-window must be positive")
	}
	if *dryRun && *scanOnce == "" {
		return common.UsageError(fs, stderr, "--dry-run needs --scan")
	}
	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest scanner: %v\n", err)
		return common.ExitFailure
	}
	cfg = cfg.WithEnvFallback(env)
	host := strings.TrimSpace(common.FirstNonEmpty(*scannerFlag, env.Env("SCANNER_HOST")))
	if err := checkHost(host); err != nil {
		fmt.Fprintf(stderr, "pdw ingest scanner: %v\n", err)
		return common.ExitFailure
	}
	accountName := common.FirstNonEmpty(*account, env.Env("SCANNER_ACCOUNT"), env.DefaultAccount())
	if accountName == "" {
		fmt.Fprintln(stderr, "pdw ingest scanner: --account, SCANNER_ACCOUNT or GMAIL_ACCOUNTS must be set")
		return common.ExitFailure
	}
	deviceName := common.FirstNonEmpty(*device, env.Env("SCANNER_DEVICE"), common.LocalDeviceName())
	logger := common.NewWriterLogger(stdout)

	lock, code, done := common.AcquireRunLock(*lockFile, "pdw ingest scanner: another copy is already running", stdout, stderr)
	if done {
		return code
	}
	defer lock.Release()

	s := &Scanner{
		Client: &NativeClient{Host: host}, Logger: logger, Account: accountName, Device: deviceName,
		SpoolDir: *spoolDir, Resolution: *resolution, PageSize: *pageSize, DuplexWindow: *duplexWindow,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if name, err := ScannerNameESCL(ctx, host); err == nil {
		s.ScannerName = name
	} else {
		logger.Warningf("Could not read the scanner's name: %v", err)
	}

	if *scanOnce != "" {
		profile, ok := ProfileByContext(*scanOnce)
		if !ok || profile.Duplex {
			return common.UsageError(fs, stderr, "--scan must be pdw-color or pdw-bw")
		}
		if *dryRun {
			s.OnFiled = func(record map[string]any) {
				fmt.Fprintf(stdout, "Dry run: scan %s spooled in %s (not sent)\n", record["scan_id"], filepath.Join(s.outboxDir(), fmt.Sprint(record["scan_id"])))
			}
		}
		if err := s.HandleScan(ctx, profile); err != nil {
			fmt.Fprintf(stderr, "pdw ingest scanner: %v\n", err)
			return common.ExitFailure
		}
		if *dryRun {
			return 0
		}
	}

	ingest, err := ingestclient.FromEnv(env, cfg, logger)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest scanner: %v\n", err)
		return common.ExitFailure
	}
	s.Uploader = ingest
	if *scanOnce != "" {
		if _, err := s.SendOutbox(); err != nil {
			fmt.Fprintf(stderr, "pdw ingest scanner: %v (the scan stays spooled for the service to send)\n", err)
			return common.ExitFailure
		}
		return 0
	}

	if *hostAddress == "" {
		*hostAddress, err = localAddressToward(host)
		if err != nil {
			fmt.Fprintf(stderr, "pdw ingest scanner: %v; pass --host-address\n", err)
			return common.ExitFailure
		}
	}
	conn, err := net.ListenPacket("udp", *listen)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest scanner: listen %s: %v\n", *listen, err)
		return common.ExitFailure
	}
	_, port, _ := net.SplitHostPort(conn.LocalAddr().String())
	d := &Daemon{
		Scanner: s, Heartbeats: ingest, ScannerHost: host, Community: *community,
		HostPort: net.JoinHostPort(*hostAddress, port),
	}
	logger.Infof("Listening on %s for button presses from %s (registered as %s)", *listen, host, d.HostPort)
	if err := d.Serve(ctx, conn); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(stderr, "pdw ingest scanner: %v\n", err)
		return common.ExitFailure
	}
	return 0
}

func checkHost(host string) error {
	if host == "" {
		return fmt.Errorf("--scanner or SCANNER_HOST must be set")
	}
	if strings.ContainsAny(host, "/:") {
		return fmt.Errorf("--scanner %q must be a host name or address", host)
	}
	return nil
}

// localAddressToward is this computer's address on the route to the scanner.
func localAddressToward(scannerHost string) (string, error) {
	conn, err := net.Dial("udp", net.JoinHostPort(scannerHost, "80"))
	if err != nil {
		return "", fmt.Errorf("find this computer's address toward %s: %w", scannerHost, err)
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String(), nil
}

// ScannerNameESCL reads the scanner's make and model from its eSCL
// (AirScan) capabilities.
func ScannerNameESCL(ctx context.Context, host string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+host+"/eSCL/ScannerCapabilities", nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("eSCL capabilities: HTTP %d", resp.StatusCode)
	}
	return ParseMakeAndModel(body)
}

// ParseMakeAndModel reads an eSCL ScannerCapabilities' MakeAndModel.
func ParseMakeAndModel(body []byte) (string, error) {
	text := string(body)
	start := strings.Index(text, "MakeAndModel>")
	if start < 0 {
		return "", fmt.Errorf("eSCL capabilities have no MakeAndModel")
	}
	rest := text[start+len("MakeAndModel>"):]
	end := strings.IndexByte(rest, '<')
	if end < 0 {
		return "", fmt.Errorf("eSCL MakeAndModel is malformed")
	}
	return strings.TrimSpace(rest[:end]), nil
}

// HeartbeatPoster is what the daemon needs to report its health.
type HeartbeatPoster interface {
	PostHeartbeat(h ingestclient.Heartbeat) (map[string]any, error)
}

// Daemon keeps the panel names registered and turns presses into scans.
type Daemon struct {
	Scanner     *Scanner
	Heartbeats  HeartbeatPoster
	ScannerHost string
	Community   string
	HostPort    string
	Now         func() time.Time

	mu        sync.Mutex
	regErr    error
	sendErr   error
	presses   map[string]time.Time
	kick      chan struct{}
	inflight  sync.WaitGroup
	scanQueue chan Profile
}

func (d *Daemon) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// Serve runs until ctx ends.
func (d *Daemon) Serve(ctx context.Context, conn net.PacketConn) error {
	d.kick = make(chan struct{}, 1)
	d.presses = map[string]time.Time{}
	// Presses are scanned one at a time, in order: a duplex second pass
	// must follow its first.
	d.scanQueue = make(chan Profile, 16)
	go d.scanLoop(ctx)
	readErr := make(chan error, 1)
	go func() { readErr <- d.readPresses(conn) }()
	defer conn.Close()

	d.register(ctx)
	register := time.NewTicker(registerEvery)
	defer register.Stop()
	housekeeping := time.NewTicker(housekeepingEvery)
	defer housekeeping.Stop()
	heartbeat := time.NewTicker(heartbeatEvery)
	defer heartbeat.Stop()
	d.housekeep()
	d.postHeartbeat()
	for {
		select {
		case <-ctx.Done():
			_ = conn.Close()
			d.inflight.Wait()
			d.housekeep()
			return ctx.Err()
		case err := <-readErr:
			return fmt.Errorf("button listener stopped: %w", err)
		case <-register.C:
			d.register(ctx)
		case <-housekeeping.C:
			d.housekeep()
		case <-d.kick:
			d.housekeep()
		case <-heartbeat.C:
			d.postHeartbeat()
		}
	}
}

func (d *Daemon) register(ctx context.Context) {
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err := Register(callCtx, d.ScannerHost, d.Community, d.HostPort, PanelNames())
	d.mu.Lock()
	previous := d.regErr
	d.regErr = err
	d.mu.Unlock()
	switch {
	case err != nil:
		d.Scanner.Logger.Warningf("Could not put the PDW names on the scanner's panel: %v", err)
	case previous != nil:
		d.Scanner.Logger.Infof("The PDW names are back on the scanner's panel")
	}
}

func (d *Daemon) readPresses(conn net.PacketConn) error {
	buf := make([]byte, 4096)
	for {
		n, from, err := conn.ReadFrom(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		d.handlePress(buf[:n], from)
	}
}

// handlePress queues the scan a press asks for, once per press.
func (d *Daemon) handlePress(packet []byte, from net.Addr) {
	press, ok := ParseButtonEvent(packet)
	if !ok {
		return
	}
	profile, known := ProfileByPanelName(press.User)
	if !known {
		d.Scanner.Logger.Warningf("Button press for unknown name %q ignored", press.User)
		return
	}
	key := press.User + "|" + press.RegID + "|" + press.Seq
	now := d.now()
	d.mu.Lock()
	for k, at := range d.presses {
		if now.Sub(at) > pressDedupWindow {
			delete(d.presses, k)
		}
	}
	_, seen := d.presses[key]
	d.presses[key] = now
	d.mu.Unlock()
	if seen {
		return
	}
	d.Scanner.Logger.Infof("Scan to PC pressed on %q (%s, from %s)", press.User, press.Function, from)
	d.inflight.Add(1)
	select {
	case d.scanQueue <- profile:
	default:
		d.inflight.Done()
		d.Scanner.Logger.Warningf("Too many presses waiting; %q ignored", press.User)
	}
}

func (d *Daemon) scanLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case profile := <-d.scanQueue:
			scanCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			if err := d.Scanner.HandleScan(scanCtx, profile); err != nil {
				d.Scanner.Logger.Warningf("Scan on %q failed: %v", profile.PanelName, err)
			}
			cancel()
			d.inflight.Done()
			select {
			case d.kick <- struct{}{}:
			default:
			}
		}
	}
}

func (d *Daemon) housekeep() {
	if err := d.Scanner.FlushExpired(); err != nil {
		d.Scanner.Logger.Warningf("Filing expired duplex passes: %v", err)
	}
	_, err := d.Scanner.SendOutbox()
	if err != nil {
		d.Scanner.Logger.Warningf("Sending scans: %v (they stay spooled)", err)
	}
	d.mu.Lock()
	d.sendErr = err
	d.mu.Unlock()
}

func (d *Daemon) postHeartbeat() {
	if d.Heartbeats == nil {
		return
	}
	d.mu.Lock()
	problems := []string{}
	if d.regErr != nil {
		problems = append(problems, d.regErr.Error())
	}
	if d.sendErr != nil {
		problems = append(problems, d.sendErr.Error())
	}
	d.mu.Unlock()
	h := ingestclient.Heartbeat{
		Pipeline: HeartbeatPipeline, Device: d.Scanner.Device,
		RanAt: common.ISOFormat(time.Now().UTC()), Error: strings.Join(problems, "; "),
	}
	if len(problems) > 0 {
		h.ExitCode = 1
	}
	if _, err := d.Heartbeats.PostHeartbeat(h); err != nil {
		d.Scanner.Logger.Warningf("Posting the heartbeat failed: %v", err)
	}
}
