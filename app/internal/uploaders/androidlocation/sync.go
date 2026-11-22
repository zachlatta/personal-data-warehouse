// Package androidlocation drains the Android helper's durable location spool.
package androidlocation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const DefaultSpoolDir = "/data/user/0/com.zachlatta.pdw.location/files/spool"

// DefaultTransientFailureThreshold is how many consecutive transient provider
// misses (timeout, no_fresh_fix, busy) exit 0 before runs start failing.
// After N transient misses, run N+1 fails, and the server pushes an alert once
// 6 consecutive runs fail (uploaderFailureAlertRuns = 6), i.e. on run N+6.
// At the 15-minute cadence, N=18 gives 18 failing-free misses + 6 failed runs
// = 24 runs ≈ 6 hours: long enough for a flight or dead zone without paging,
// short enough that a persistent provider outage still alerts. Hard failures
// (permission_denied, location_disabled, storage_error, helper crash, upload
// errors) always fail immediately and push after 6 runs (~90 min).
const DefaultTransientFailureThreshold = 18

const Usage = `pdw ingest android-location - collect one approximate network location and drain the offline queue.

USAGE
  pdw ingest android-location [--spool-dir DIR] [--account EMAIL] [--device NAME]
                              [--lock-file PATH] [--transient-failure-threshold N] [--dry-run]

Runs as root on Android. Requests a bounded network-only fix through the PDW
location helper, then uploads queued samples using existing PDW authentication.
A failed fix still drains older samples; an upload failure retains unsent files.
--dry-run reads existing queued files only: no acquisition, upload or deletion.
The helper requires approximate and background location permission. Android may
delay fixes; the 15 minute service interval is a target, not a guarantee.

ENVIRONMENT
  ANDROID_LOCATION_ACCOUNT, ANDROID_LOCATION_DEVICE, ANDROID_LOCATION_SPOOL_DIR
  ANDROID_LOCATION_TRANSIENT_FAILURE_THRESHOLD
  PDW_API_URL, PDW_SECRET_TOKEN (or pdw login configuration)
`

type Uploader interface {
	UploadAndroidLocationBatch([]byte, string) (ingestclient.StoredObject, error)
}
type Runner struct {
	SpoolDir, Account, Device string
	Client                    Uploader
	Logger                    common.Logger
	DryRun                    bool
	Now                       func() time.Time
	BeforeUploadCheck         func() string
}

func (r *Runner) Sync() (int, bool, error) {
	if r.Now == nil {
		r.Now = time.Now
	}
	entries, err := os.ReadDir(r.SpoolDir)
	if err != nil {
		return 0, false, err
	}
	sent := 0
	for _, entry := range entries {
		if !entry.Type().IsRegular() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		p := filepath.Join(r.SpoolDir, entry.Name())
		raw, err := os.ReadFile(p)
		if err != nil {
			return sent, false, err
		}
		var rec map[string]any
		if err = json.Unmarshal(raw, &rec); err != nil {
			return sent, false, fmt.Errorf("invalid queued sample %s: %w", entry.Name(), err)
		}
		if err = Validate(rec); err != nil {
			return sent, false, fmt.Errorf("invalid queued sample %s: %w", entry.Name(), err)
		}
		if r.DryRun {
			sent++
			continue
		}
		if r.BeforeUploadCheck != nil {
			if reason := r.BeforeUploadCheck(); reason != "" {
				if r.Logger != nil {
					r.Logger.Warningf("Skipping Android location upload: %s", reason)
				}
				return sent, true, nil
			}
		}
		exported := common.ISOFormat(r.Now().UTC())
		body, err := common.GzipJSONL([]map[string]any{{"schema_version": 1, "source": "android_location", "record_type": "location", "account": r.Account, "device": r.Device, "exported_at": exported, "record": rec}})
		if err != nil {
			return sent, false, err
		}
		if _, err = r.Client.UploadAndroidLocationBatch(body, exported); err != nil {
			return sent, false, err
		}
		// Delete only after the server confirms durable storage. A crash before this
		// step resends the stable sample_id, which the warehouse upserts.
		if err = os.Remove(p); err != nil {
			return sent, false, err
		}
		sent++
	}
	return sent, false, nil
}

func Validate(rec map[string]any) error {
	id, _ := rec["sample_id"].(string)
	if id == "" {
		return fmt.Errorf("missing sample_id")
	}
	if rec["provider"] != "network" {
		return fmt.Errorf("provider must be network")
	}
	for k, bounds := range map[string][2]float64{"latitude": {-90, 90}, "longitude": {-180, 180}, "accuracy_m": {0, 1e7}, "measured_at_ms": {1, 1e15}} {
		n, ok := rec[k].(float64)
		if k == "accuracy_m" && n <= 0 {
			return fmt.Errorf("invalid accuracy_m")
		}
		if !ok || math.IsNaN(n) || math.IsInf(n, 0) || n < bounds[0] || n > bounds[1] {
			return fmt.Errorf("invalid %s", k)
		}
	}
	return nil
}

func isTransientHelperError(code string) bool {
	switch code {
	case "timeout", "no_fresh_fix", "busy":
		return true
	default:
		return false
	}
}

func transientStreakPath(lockPath string) string {
	return lockPath + ".transient-streak"
}

func readTransientStreak(path string) int {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func writeTransientStreak(path string, count int) {
	if count <= 0 {
		_ = os.Remove(path)
		return
	}
	_ = os.WriteFile(path, []byte(strconv.Itoa(count)+"\n"), 0600)
}

func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	fs := common.NewFlagSet("pdw ingest android-location")
	dir := fs.String("spool-dir", "", "")
	account := fs.String("account", "", "")
	device := fs.String("device", "", "")
	lockPath := fs.String("lock-file", common.DefaultStateFile("android-location-upload.lock"), "")
	transientThreshold := fs.Int("transient-failure-threshold", 0, "")
	dry := fs.Bool("dry-run", false, "")
	if _, code, done := common.ParseArgs(fs, args, Usage, stdout, stderr); done {
		return code
	}
	env, err := common.LoadProjectDotenv(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest android-location: %v\n", err)
		return 1
	}
	cfg = cfg.WithEnvFallback(env)
	acct := common.FirstNonEmpty(*account, env.Env("ANDROID_LOCATION_ACCOUNT"))
	if acct == "" {
		fmt.Fprintln(stderr, "ANDROID_LOCATION_ACCOUNT or --account is required")
		return 1
	}
	threshold := *transientThreshold
	if threshold <= 0 {
		if raw := strings.TrimSpace(env.Env("ANDROID_LOCATION_TRANSIENT_FAILURE_THRESHOLD")); raw != "" {
			if parsed, parseErr := strconv.Atoi(raw); parseErr == nil && parsed > 0 {
				threshold = parsed
			}
		}
	}
	if threshold <= 0 {
		threshold = DefaultTransientFailureThreshold
	}
	lock, code, done := common.AcquireRunLock(*lockPath, "Android location uploader already active", stdout, stderr)
	if done {
		return code
	}
	defer lock.Release()
	streakPath := transientStreakPath(*lockPath)
	var client Uploader
	var acquisitionErr error
	if !*dry {
		client, err = ingestclient.FromEnv(env, cfg, common.NewWriterLogger(stdout))
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		cmd := exec.CommandContext(ctx, "content", "read", "--uri", "content://com.zachlatta.pdw.location/sample")
		var captureStderr, captureStdout bytes.Buffer
		cmd.Stdout = &captureStdout
		cmd.Stderr = &captureStderr
		// Never print the response: it may include coordinates. Acquisition failure
		// must not block upload of the durable offline queue.
		acquisitionErr = cmd.Run()
		if acquisitionErr == nil && captureStderr.Len() > 0 {
			acquisitionErr = fmt.Errorf("helper reported acquisition failure")
		}
		if acquisitionErr == nil {
			// Android's content command can swallow reliable-pipe errors and exit
			// zero with empty stdout. Only the helper's durable-queue acknowledgement
			// proves acquisition succeeded; structured error_code distinguishes transient
			// provider misses from permanent failures. Never log arbitrary provider output.
			var ack struct {
				Queued    string `json:"queued"`
				ErrorCode string `json:"error_code"`
				Error     string `json:"error"`
			}
			if json.Unmarshal(captureStdout.Bytes(), &ack) != nil {
				acquisitionErr = fmt.Errorf("helper did not acknowledge a queued location sample")
			} else if ack.Queued != "" {
				writeTransientStreak(streakPath, 0)
			} else if ack.ErrorCode != "" {
				detail := strings.TrimSpace(ack.Error)
				if detail == "" {
					detail = ack.ErrorCode
				}
				if isTransientHelperError(ack.ErrorCode) {
					streak := readTransientStreak(streakPath) + 1
					writeTransientStreak(streakPath, streak)
					if streak > threshold {
						acquisitionErr = fmt.Errorf("helper %s (%d consecutive runs): %s", ack.ErrorCode, streak, detail)
					} else {
						fmt.Fprintf(stdout, "Android location acquisition transiently unavailable (%s: %s; streak=%d/%d); draining existing queue\n", ack.ErrorCode, detail, streak, threshold)
					}
				} else {
					acquisitionErr = fmt.Errorf("helper %s: %s", ack.ErrorCode, detail)
				}
			} else {
				acquisitionErr = fmt.Errorf("helper did not acknowledge a queued location sample")
			}
		}
		if acquisitionErr != nil {
			fmt.Fprintln(stderr, "Location acquisition failed; draining existing queue")
		}
		cancel()
	}
	runner := Runner{SpoolDir: common.FirstNonEmpty(*dir, env.Env("ANDROID_LOCATION_SPOOL_DIR"), DefaultSpoolDir), Account: acct, Device: common.FirstNonEmpty(*device, env.Env("ANDROID_LOCATION_DEVICE"), common.LocalDeviceName()), Client: client, Logger: common.NewWriterLogger(stdout), DryRun: *dry}
	if !*dry {
		runner.BeforeUploadCheck = common.PreflightGuard(env, "ANDROID_LOCATION", cfg.BaseURL)
	}
	n, deferred, err := runner.Sync()
	if err == nil && acquisitionErr != nil {
		err = fmt.Errorf("location acquisition failed; queued samples drained: %w", acquisitionErr)
	}
	fmt.Fprintf(stdout, "Android location queue: samples=%d deferred=%t dry_run=%t\n", n, deferred, *dry)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest android-location: %v\n", err)
		return 1
	}
	return 0
}
