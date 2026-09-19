package queue

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

func envOf(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

var testSpec = Spec{
	DisplayName: "Apple Notes",
	Command:     "apple-notes",
	Provider:    "apple_notes",
	WorkerName:  "apple_notes_mutation_worker",
	LockID:      1,
	EnvPrefix:   "APPLE_NOTES",
	Idempotent:  []Operation{{"apple_notes", "apple_notes.update_note"}},
	NewExecutor: func(Store) Executor { return &fakeExecutor{} },
}

func TestKillSwitchMatchesThePythonPredicate(t *testing.T) {
	for value, want := range map[string]bool{"": true, "1": true, "yes": true, "0": false, "false": false, "no": false, " no ": false, "off": true} {
		if got := testSpec.Enabled(envOf(map[string]string{"APPLE_NOTES_MUTATIONS_ENABLED": value})); got != want {
			t.Fatalf("%q: got %v want %v", value, got, want)
		}
	}
}

func TestLockEnvPrefixAndURLs(t *testing.T) {
	if LockEnvPrefix("apple_notes_mutation_worker") != "APPLE_NOTES_MUTATION_WORKER" || LockEnvPrefix("a-b.c") != "A_B_C" {
		t.Fatal("lock env prefix")
	}
	lock := SyncLock{Name: "apple_notes_mutation_worker", Getenv: envOf(map[string]string{"DATABASE_URL": "postgres://db"})}
	if lock.PostgresURL() != "postgres://db" {
		t.Fatalf("url %q", lock.PostgresURL())
	}
	lock.Getenv = envOf(map[string]string{"DATABASE_URL": "x", "DAGSTER_POSTGRES_URL": "y", "APPLE_NOTES_MUTATION_WORKER_SYNC_LOCK_POSTGRES_URL": "z"})
	if lock.PostgresURL() != "z" {
		t.Fatalf("url %q", lock.PostgresURL())
	}
	lock.Getenv = envOf(map[string]string{"APPLE_NOTES_MUTATION_WORKER_SYNC_LOCK_PATH": "~/x.lock"})
	if lock.PostgresURL() != "" || !strings.HasSuffix(lock.Path(), "/x.lock") || strings.HasPrefix(lock.Path(), "~") {
		t.Fatalf("path %q", lock.Path())
	}
	lock.Getenv = envOf(nil)
	if !strings.HasSuffix(lock.Path(), "personal-data-warehouse-apple_notes_mutation_worker-sync.lock") {
		t.Fatalf("default path %q", lock.Path())
	}
	for in, want := range map[string]string{
		"postgres://u@h/db": "postgresql://u@h/db", "postgresql+psycopg2://u@h/db": "postgresql://u@h/db",
		" postgresql://u@h/db ": "postgresql://u@h/db", "": "",
	} {
		if got := NormalizePostgresURL(in); got != want {
			t.Fatalf("%q: got %q", in, got)
		}
	}
}

func TestTheFileLockIsANonBlockingTryLock(t *testing.T) {
	path := t.TempDir() + "/w.lock"
	lock := SyncLock{Name: "w", Getenv: envOf(map[string]string{"W_SYNC_LOCK_PATH": path})}
	release, acquired, err := lock.Acquire(context.Background())
	if err != nil || !acquired {
		t.Fatalf("first acquire: %v %v", acquired, err)
	}
	if _, again, err := lock.Acquire(context.Background()); err != nil || again {
		t.Fatalf("second acquire must be refused: %v %v", again, err)
	}
	release()
	if release, acquired, err := lock.Acquire(context.Background()); err != nil || !acquired {
		t.Fatalf("after release: %v %v", acquired, err)
	} else {
		release()
	}
}

func TestDeriveRequestStatusFollowsThePrecedenceLadder(t *testing.T) {
	cases := []struct {
		active []string
		want   string
	}{
		{nil, "rejected"},
		{[]string{"succeeded", "pending_review"}, "pending_review"},
		{[]string{"succeeded", "executing"}, "executing"},
		{[]string{"failed_terminal", "approved"}, "approved"},
		{[]string{"failed_terminal", "failed_retryable"}, "failed_retryable"},
		{[]string{"failed_terminal", "blocked_missing_credentials"}, "blocked_missing_credentials"},
		{[]string{"succeeded", "failed_terminal"}, "failed_terminal"},
		{[]string{"observed", "observed"}, "observed"},
		{[]string{"succeeded", "observed"}, "succeeded"},
		{[]string{"something_new"}, "current"},
	}
	for _, c := range cases {
		if got := DeriveRequestStatus(c.active, "current"); got != c.want {
			t.Fatalf("%v: got %q want %q", c.active, got, c.want)
		}
	}
}

func TestRunHelpAndUnknownFlags(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := testSpec.Run([]string{"--help"}, strings.NewReader(""), &out, &errOut, envOf(nil), ingestclient.Config{}); code != 0 {
		t.Fatalf("code %d", code)
	}
	for _, want := range []string{"pdw mutations apple-notes [--once]", "--once", "APPLE_NOTES_MUTATIONS_ENABLED", "APPLE_NOTES_MUTATION_BATCH_SIZE", "APPLE_NOTES_MUTATION_WORKER_SYNC_LOCK_POSTGRES_URL"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("help lacks %q:\n%s", want, out.String())
		}
	}
	out.Reset()
	if code := testSpec.Run([]string{"--dry-run"}, strings.NewReader(""), &out, &errOut, envOf(nil), ingestclient.Config{}); code != 2 || !strings.Contains(errOut.String(), `unknown flag "--dry-run"`) {
		t.Fatalf("code %d stderr %q", code, errOut.String())
	}
}

func TestOnceWithoutAWarehouseReportsItAndExitsNonZero(t *testing.T) {
	var out, errOut bytes.Buffer
	env := envOf(map[string]string{"PDW_INGEST_PROJECT_DIR": t.TempDir()})
	code := testSpec.Run([]string{"--once"}, strings.NewReader(""), &out, &errOut, env, ingestclient.Config{})
	if code != 1 || !strings.Contains(out.String(), "Apple Notes mutations skipped: warehouse unavailable (POSTGRES_DATABASE_URL must be set)") {
		t.Fatalf("code %d out %q", code, out.String())
	}
}

func TestOnceHonoursTheKillSwitch(t *testing.T) {
	var out, errOut bytes.Buffer
	env := envOf(map[string]string{"APPLE_NOTES_MUTATIONS_ENABLED": "0", "PDW_INGEST_PROJECT_DIR": t.TempDir()})
	code := testSpec.Run([]string{"--once"}, strings.NewReader(""), &out, &errOut, env, ingestclient.Config{})
	if code != 0 || !strings.Contains(out.String(), "Apple Notes mutations disabled (APPLE_NOTES_MUTATIONS_ENABLED=0)") {
		t.Fatalf("code %d out %q", code, out.String())
	}
	message, applied := testSpec.RunApplyOnce(context.Background(), env)
	if applied || !strings.Contains(message, "disabled") {
		t.Fatalf("applied %v message %q", applied, message)
	}
}

func TestResidentModeRequiresTheWarehouseURL(t *testing.T) {
	var out, errOut bytes.Buffer
	code := testSpec.Run(nil, strings.NewReader(""), &out, &errOut, envOf(map[string]string{"PDW_INGEST_PROJECT_DIR": t.TempDir()}), ingestclient.Config{})
	if code != 2 || !strings.Contains(errOut.String(), "POSTGRES_DATABASE_URL is required by the Apple Notes mutation worker") {
		t.Fatalf("code %d stderr %q", code, errOut.String())
	}
}

func TestTheProjectDotenvFillsGapsButTheEnvironmentWins(t *testing.T) {
	dir := t.TempDir()
	if err := writeFile(dir+"/.env", "POSTGRES_DATABASE_URL=postgres://from-file\nAPPLE_NOTES_MUTATION_BATCH_SIZE=3\n"); err != nil {
		t.Fatal(err)
	}
	getenv := Getenv(envOf(map[string]string{"PDW_INGEST_PROJECT_DIR": dir, "APPLE_NOTES_MUTATION_BATCH_SIZE": "9"}))
	if DatabaseURL(getenv) != "postgresql://from-file" || testSpec.batchSize(getenv) != 9 {
		t.Fatalf("url %q batch %d", DatabaseURL(getenv), testSpec.batchSize(getenv))
	}
	if testSpec.batchSize(envOf(nil)) != DefaultBatchSize || testSpec.floatSeconds(envOf(map[string]string{"APPLE_NOTES_MUTATION_POLL_SECONDS": "0.5"}), "_MUTATION_POLL_SECONDS", 30) != 500*time.Millisecond {
		t.Fatal("defaults")
	}
}

// fakeListener hands out a scripted sequence of Wait outcomes.
type fakeListener struct {
	mu      sync.Mutex
	waits   []waitOutcome
	calls   int
	closed  int
	timeout []time.Duration
}

type waitOutcome struct {
	notified bool
	err      error
	block    bool
	then     func()
}

func (l *fakeListener) Wait(ctx context.Context, timeout time.Duration) (bool, error) {
	l.mu.Lock()
	l.calls++
	l.timeout = append(l.timeout, timeout)
	var outcome waitOutcome
	if len(l.waits) > 0 {
		outcome = l.waits[0]
		l.waits = l.waits[1:]
	} else {
		outcome = waitOutcome{block: true}
	}
	l.mu.Unlock()
	if outcome.then != nil {
		outcome.then()
	}
	if outcome.block {
		<-ctx.Done()
		return false, ctx.Err()
	}
	return outcome.notified, outcome.err
}

func (l *fakeListener) Close() error { l.mu.Lock(); l.closed++; l.mu.Unlock(); return nil }

func TestNotificationLoopDrainsUntilTheQueueIsEmptyThenWaits(t *testing.T) {
	// process_pending reports true twice (a capped batch), then false; the
	// loop must call it again after each true and only then wait. After a
	// notification it drains once more (false) and waits again; that second
	// wait ends the test by cancelling the context.
	pending := []bool{true, true, false, false}
	var calls int
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener := &fakeListener{waits: []waitOutcome{{notified: true}, {then: cancel, err: context.Canceled}}}
	process := func() (bool, error) {
		calls++
		more := pending[0]
		pending = pending[1:]
		return more, nil
	}
	if err := RunNotificationLoop(ctx, listener, process, 7*time.Second); err != nil {
		t.Fatal(err)
	}
	if calls != 4 || listener.calls != 2 || listener.closed != 1 || listener.timeout[0] != 7*time.Second {
		t.Fatalf("calls %d waits %d closed %d timeouts %v", calls, listener.calls, listener.closed, listener.timeout)
	}
}

func TestNotificationLoopStopsOnCancelAndSurfacesListenerFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	listener := &fakeListener{}
	if err := RunNotificationLoop(ctx, listener, func() (bool, error) { t.Fatal("must not process"); return false, nil }, time.Second); err != nil || listener.closed != 1 {
		t.Fatalf("err %v closed %d", err, listener.closed)
	}

	listener = &fakeListener{waits: []waitOutcome{{err: errors.New("connection lost")}}}
	err := RunNotificationLoop(context.Background(), listener, func() (bool, error) { return false, nil }, time.Second)
	if err == nil || err.Error() != "connection lost" || listener.closed != 1 {
		t.Fatalf("err %v closed %d", err, listener.closed)
	}

	listener = &fakeListener{}
	err = RunNotificationLoop(context.Background(), listener, func() (bool, error) { return false, errors.New("process failed") }, time.Second)
	if err == nil || err.Error() != "process failed" {
		t.Fatalf("err %v", err)
	}
}

func TestResidentWorkerClaimsAsMacResidentAndReconnectsAfterAFailure(t *testing.T) {
	store := &fakeStore{claimable: []Mutation{{ID: "m1", Provider: "apple_notes"}}}
	executor := &fakeExecutor{results: []Result{{Status: StatusSucceeded}}}
	opens := 0
	ctx, cancel := context.WithCancel(context.Background())
	var listeners []*fakeListener
	deps := Deps{
		OpenStore: func(context.Context, string) (Store, error) {
			opens++
			if opens == 1 {
				return nil, errors.New("db not up yet")
			}
			return store, nil
		},
		NewListener: func(context.Context, string) (Listener, error) {
			l := &fakeListener{}
			listeners = append(listeners, l)
			return l, nil
		},
		NewExecutor: func(Store) Executor { return executor },
		Lock:        alwaysAcquire,
		Hostname:    "porygon",
	}
	var log bytes.Buffer
	env := envOf(map[string]string{"POSTGRES_DATABASE_URL": "postgres://x", "APPLE_NOTES_MUTATION_RETRY_SECONDS": "0.01", "APPLE_NOTES_MUTATION_BATCH_SIZE": "2"})
	done := make(chan struct{})
	go func() {
		defer close(done)
		testSpec.RunResident(ctx, env, deps, common.NewWriterLogger(&log))
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(store.completed) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if opens != 2 || !strings.Contains(log.String(), "db not up yet") {
		t.Fatalf("opens %d log %q", opens, log.String())
	}
	if store.ensured != 1 || len(store.completed) != 1 || store.completed[0].actor != "mac-resident:porygon:apple_notes_mutation_worker" {
		t.Fatalf("store %+v", store)
	}
	if !reflect.DeepEqual(store.claimCalls[0].providers, []string{"apple_notes"}) || store.claimCalls[0].limit != 2 {
		t.Fatalf("claim %+v", store.claimCalls[0])
	}
	if store.closed != 1 || len(listeners) != 1 || listeners[0].closed != 1 {
		t.Fatalf("closed store %d listeners %d", store.closed, len(listeners))
	}
	if !strings.Contains(log.String(), "Apple Notes mutations: claimed=1 succeeded=1") {
		t.Fatalf("log %q", log.String())
	}
}

func TestRunWithDepsRejectsUnknownFlagsBeforeConnecting(t *testing.T) {
	opened := false
	deps := Deps{
		OpenStore:   func(context.Context, string) (Store, error) { opened = true; return &fakeStore{}, nil },
		NewListener: func(context.Context, string) (Listener, error) { return &fakeListener{}, nil },
		NewExecutor: func(Store) Executor { return &fakeExecutor{} },
		Lock:        alwaysAcquire,
		Hostname:    "h",
	}
	var out, errOut bytes.Buffer
	if code := testSpec.RunWithDeps([]string{"--bogus"}, &out, &errOut, envOf(map[string]string{"POSTGRES_DATABASE_URL": "postgres://x"}), &deps); code != 2 || opened {
		t.Fatalf("code %d opened %v", code, opened)
	}
}
