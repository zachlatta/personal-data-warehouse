package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// heartbeatHostname resolves the default --device (the short hostname, the
// way socket.gethostname().split(".")[0] did). A package var so tests pin it.
var heartbeatHostname = os.Hostname

const heartbeatUsage = `pdw heartbeat - record one uploader run's verdict in the warehouse.

USAGE
  pdw heartbeat --pipeline NAME[,NAME...] --exit-code N [--duration-seconds F]
                [--device HOST] [--ran-at ISO-8601] [--error TEXT]

Run by the uploader wrappers (bin/_pdw-upload-lib.sh, pdw_post_heartbeat)
after every remote-device uploader run, with the exit code the wrapper
observed. It is the only in-warehouse heartbeat those uploaders have: their
data tables go quiet both when nothing changed and when macOS silently revoked
Full Disk Access, and only a run record (ops.uploader_heartbeats, on
/pipelines) can tell the two apart.

Best effort by design: a failure here is reported and exits non-zero, but the
wrapper never lets it change the uploader's own exit code.

FLAGS
  --pipeline NAMES       Pipeline id(s) as registered in pipeline_health,
                         comma-separated. Required.
  --exit-code N          The uploader run's exit status. Required.
  --duration-seconds F   How long the run took (default 0; fractions are rounded).
  --device HOST          Device label (default: this machine's short hostname).
  --ran-at ISO-8601      When the run happened (default: now, UTC).
  --error TEXT           The failure summary, if any (truncated to 500 chars).

The record is posted over the same URL + token pdw uses for everything else:
run "pdw login" once (or set PDW_API_URL + PDW_SECRET_TOKEN). The .env under
PDW_INGEST_PROJECT_DIR (default: the current directory) fills either when
still unset.

EXIT STATUS
  0  every pipeline recorded
  1  at least one pipeline could not be recorded (each is named on stderr)
  2  bad arguments, or no warehouse URL/token configured
`

// defaultHeartbeatDevice is Python's default_device(): the hostname up to
// its first dot, or "unknown".
func defaultHeartbeatDevice() string {
	name, err := heartbeatHostname()
	if err != nil {
		return "unknown"
	}
	short, _, _ := strings.Cut(name, ".")
	if strings.TrimSpace(short) == "" {
		return "unknown"
	}
	return short
}

// runHeartbeat implements `pdw heartbeat`. It posts one record per pipeline
// and never prints the token.
func runHeartbeat(args []string, stdout, stderr io.Writer, getenv func(string) string, flagBaseURL, flagToken string) int {
	fs := flag.NewFlagSet("pdw heartbeat", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	pipeline := fs.String("pipeline", "", "")
	device := fs.String("device", "", "")
	exitCode := fs.Int("exit-code", 0, "")
	duration := fs.Float64("duration-seconds", 0, "")
	ranAt := fs.String("ran-at", "", "")
	errText := fs.String("error", "", "")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stdout, heartbeatUsage)
			return 0
		}
		fmt.Fprintf(stderr, "pdw heartbeat: %v\n", err)
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "pdw heartbeat: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	var pipelines []string
	for _, p := range strings.Split(*pipeline, ",") {
		if p = strings.TrimSpace(p); p != "" {
			pipelines = append(pipelines, p)
		}
	}
	if len(pipelines) == 0 {
		fmt.Fprintln(stderr, "pdw heartbeat: --pipeline is required")
		return 2
	}
	exitCodeSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "exit-code" {
			exitCodeSet = true
		}
	})
	if !exitCodeSet {
		fmt.Fprintln(stderr, "pdw heartbeat: --exit-code is required")
		return 2
	}
	if math.IsNaN(*duration) || math.IsInf(*duration, 0) || *duration < 0 {
		fmt.Fprintln(stderr, "pdw heartbeat: --duration-seconds must be a non-negative number")
		return 2
	}
	if *device == "" {
		*device = defaultHeartbeatDevice()
	}
	if *ranAt == "" {
		*ranAt = time.Now().UTC().Format(time.RFC3339Nano)
	}

	// The wrappers run outside any uploader, so this is where the project
	// .env is consulted for a URL/token the login config does not hold.
	cfg := resolveLocalConfig(getenv, flagBaseURL, flagToken)
	if env, err := common.LoadProjectDotenv(getenv); err == nil {
		cfg = cfg.WithEnvFallback(env)
	}
	if problem := cfg.Problem(); problem != "" {
		fmt.Fprintf(stderr, "pdw heartbeat: %s\n", problem)
		return 2
	}
	client, err := ingestclient.New(cfg.BaseURL, cfg.Token, ingestclient.WithTimeout(30*time.Second))
	if err != nil {
		fmt.Fprintf(stderr, "pdw heartbeat: %v\n", err)
		return 2
	}

	failures := 0
	for _, p := range pipelines {
		_, err := client.PostHeartbeat(ingestclient.Heartbeat{
			Pipeline:        p,
			Device:          *device,
			RanAt:           *ranAt,
			ExitCode:        *exitCode,
			DurationSeconds: int(math.Round(*duration)),
			Error:           *errText,
		})
		if err != nil {
			failures++
			fmt.Fprintf(stderr, "pdw heartbeat: %s: %v\n", p, err)
			continue
		}
		fmt.Fprintf(stdout, "pdw heartbeat: recorded %s on %s (exit %d)\n", p, *device, *exitCode)
	}
	if failures > 0 {
		return 1
	}
	return 0
}
