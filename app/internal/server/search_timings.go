package server

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// Contract C6 asks whether a search over two seconds used the whole host.
// Until this writer existed the only per-request answer was the "search
// completed" / "slow search" log lines in Loki, which no warehouse surface or
// contract check reads: on 2026-10-25 half of 1,165 real searches took over
// two seconds while the weekly nine-probe benchmark read ok. Every search the
// app answers now leaves one row in ops.search_request_timings: its duration,
// leg timings and the host's pressure at that moment -- never the query text.
//
// The write is best-effort and off the request path. Search hands the timing
// to Record, which queues it without blocking and drops it when the queue is
// full; one goroutine samples /proc and inserts. A dead or failing writer is
// visible through its own heartbeat row (ops.search_request_timing_state),
// refreshed every ten minutes whatever the traffic, which the search_requests
// pipeline reads -- so it cannot pass for a quiet night.

const (
	searchTimingQueueSize     = 1024
	searchTimingWriteTimeout  = 5 * time.Second
	searchTimingHeartbeat     = 10 * time.Minute
	searchTimingWarnEvery     = time.Minute
	searchTimingWriterName    = "search_requests"
	searchTimingStatusOK      = "ok"
	searchTimingStatusError   = "error"
	searchTimingStatusDropped = "dropped"
)

// searchTimingTick is one heartbeat: what the writer did since the last one.
type searchTimingTick struct {
	TickedAt    time.Time
	LastWriteAt time.Time
	Status      string
	Error       string
	Written     int64
	Dropped     int64
}

type searchTimingSink interface {
	insert(context.Context, query.SearchTiming, query.HostPressure) error
	heartbeat(context.Context, searchTimingTick) error
}

type searchTimingWriter struct {
	sink         searchTimingSink
	queue        chan query.SearchTiming
	readHostFile func(string) ([]byte, error)
	logger       *slog.Logger
	writeTimeout time.Duration
	now          func() time.Time

	dropped atomic.Int64

	// Owned by the worker goroutine.
	written     int64
	lastErr     string
	lastWriteAt time.Time
	lastWarnAt  time.Time

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

func newSearchTimingWriter(sink searchTimingSink, logger *slog.Logger) *searchTimingWriter {
	if logger == nil {
		logger = slog.Default()
	}
	return &searchTimingWriter{
		sink:         sink,
		queue:        make(chan query.SearchTiming, searchTimingQueueSize),
		readHostFile: os.ReadFile,
		logger:       logger.With("component", "search_timings"),
		writeTimeout: searchTimingWriteTimeout,
		now:          time.Now,
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
}

// Record queues one search's timing. It never blocks: a full queue (the
// database slower than the searches) drops the timing and counts it, and the
// next heartbeat reports the drop.
func (w *searchTimingWriter) Record(timing query.SearchTiming) {
	if timing.RequestID == "" {
		timing.RequestID = newSearchRequestID()
	}
	select {
	case w.queue <- timing:
	default:
		w.dropped.Add(1)
	}
}

// start runs the worker until stop: it writes queued timings and beats every
// interval, starting with one beat so a fresh deploy reports at once.
func (w *searchTimingWriter) start(interval time.Duration) {
	go func() {
		defer close(w.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		w.beat()
		for {
			select {
			case timing := <-w.queue:
				w.write(timing)
			case <-ticker.C:
				w.beat()
			case <-w.stop:
				return
			}
		}
	}()
}

func (w *searchTimingWriter) close() {
	w.stopOnce.Do(func() { close(w.stop) })
	<-w.done
}

func (w *searchTimingWriter) write(timing query.SearchTiming) {
	// Sampled as the worker takes the timing, a few milliseconds after the
	// search: PSI avg10 is a ten-second average, so the lag does not move it.
	pressure := query.SampleHostPressure(w.readHostFile)
	ctx, cancel := context.WithTimeout(context.Background(), w.writeTimeout)
	defer cancel()
	if err := w.sink.insert(ctx, timing, pressure); err != nil {
		w.lastErr = err.Error()
		w.warn("search timing write failed", err)
		return
	}
	w.written++
	w.lastWriteAt = w.now().UTC()
}

func (w *searchTimingWriter) beat() {
	tick := searchTimingTick{
		TickedAt:    w.now().UTC(),
		LastWriteAt: w.lastWriteAt,
		Status:      searchTimingStatusOK,
		Written:     w.written,
		Dropped:     w.dropped.Swap(0),
	}
	switch {
	case w.lastErr != "":
		tick.Status, tick.Error = searchTimingStatusError, w.lastErr
	case tick.Dropped > 0:
		tick.Status = searchTimingStatusDropped
		tick.Error = fmt.Sprintf("dropped %d search timings: the write queue was full", tick.Dropped)
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.writeTimeout)
	defer cancel()
	if err := w.sink.heartbeat(ctx, tick); err != nil {
		// The counts carry into the next beat rather than vanishing.
		w.dropped.Add(tick.Dropped)
		w.warn("search timing heartbeat failed", err)
		return
	}
	w.written, w.lastErr = 0, ""
}

// warn logs at most once a minute: a database outage would otherwise log one
// line per search.
func (w *searchTimingWriter) warn(msg string, err error) {
	now := w.now()
	if now.Sub(w.lastWarnAt) < searchTimingWarnEvery {
		return
	}
	w.lastWarnAt = now
	w.logger.Warn(msg, "error", err)
}

func newSearchRequestID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// withSearchSurface tags every request through an entry point with its
// surface, so a stored timing says whether MCP or the HTTP API (pdw CLI, web
// and phone app) asked.
func withSearchSurface(surface string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(query.WithSearchSurface(r.Context(), surface)))
	})
}

type searchTimingStore struct {
	db *sql.DB
	// The catalog's ops relations; the integration test points them at an
	// isolated pdw_test_* schema.
	schema        string
	timings       string
	state         string
	ensureMu      sync.Mutex
	ensured       bool
	writerName    string
	ensureTimeout time.Duration
}

func newSearchTimingStore(databaseURL string) (*searchTimingStore, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	// One worker writes; the second connection is headroom for the heartbeat
	// racing a slow insert's cleanup, not for throughput.
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	return &searchTimingStore{
		db:            db,
		schema:        warehouse.SchemaOf("search_request_timings"),
		timings:       warehouse.SQLRelation("search_request_timings"),
		state:         warehouse.SQLRelation("search_request_timing_state"),
		writerName:    searchTimingWriterName,
		ensureTimeout: searchTimingWriteTimeout,
	}, nil
}

func (s *searchTimingStore) Close() error { return s.db.Close() }

// searchTimingDDL is identical to the Python TableSpecs for
// search_request_timings and search_request_timing_state (text ”, bigint 0,
// epoch-sentinel timestamptz, and -1 for the unmeasured legs and gauges), so
// whichever side provisions first leaves the same tables. The app needs its
// own copy because it deploys before the Python ensure path runs.
// tests/test_search_request_timing_store.py compares the two.
func searchTimingDDL(schema, timings, state string) []string {
	const epoch = `'1970-01-01 00:00:00+00'::timestamptz`
	return []string{
		"CREATE SCHEMA IF NOT EXISTS " + warehouse.QuoteIdent(schema),
		`CREATE TABLE IF NOT EXISTS ` + timings + ` (
	occurred_at timestamptz NOT NULL DEFAULT ` + epoch + `,
	request_id text NOT NULL DEFAULT '',
	surface text NOT NULL DEFAULT '',
	client text NOT NULL DEFAULT '',
	mode text NOT NULL DEFAULT '',
	status text NOT NULL DEFAULT '',
	has_priorities bigint NOT NULL DEFAULT 0,
	has_sources bigint NOT NULL DEFAULT 0,
	result_rows bigint NOT NULL DEFAULT 0,
	duration_ms bigint NOT NULL DEFAULT 0,
	embed_ms bigint NOT NULL DEFAULT -1,
	lexical_ms bigint NOT NULL DEFAULT -1,
	exact_ms bigint NOT NULL DEFAULT -1,
	semantic_ms bigint NOT NULL DEFAULT -1,
	fuse_ms bigint NOT NULL DEFAULT -1,
	slowest_leg text NOT NULL DEFAULT '',
	io_pressure_full_avg10 double precision NOT NULL DEFAULT -1,
	cpu_pressure_some_avg10 double precision NOT NULL DEFAULT -1,
	load_1m double precision NOT NULL DEFAULT -1,
	cpu_count bigint NOT NULL DEFAULT -1,
	PRIMARY KEY (occurred_at, request_id)
)`,
		`CREATE TABLE IF NOT EXISTS ` + state + ` (
	writer text NOT NULL DEFAULT '',
	ticked_at timestamptz NOT NULL DEFAULT ` + epoch + `,
	last_write_at timestamptz NOT NULL DEFAULT ` + epoch + `,
	status text NOT NULL DEFAULT '',
	error text NOT NULL DEFAULT '',
	written_since_tick bigint NOT NULL DEFAULT 0,
	dropped_since_tick bigint NOT NULL DEFAULT 0,
	updated_at timestamptz NOT NULL DEFAULT ` + epoch + `,
	PRIMARY KEY (writer)
)`,
	}
}

// ensure runs the DDL once per process. A failure is retried on the next
// write, and surfaces through the write's error until it succeeds.
func (s *searchTimingStore) ensure(ctx context.Context) error {
	s.ensureMu.Lock()
	defer s.ensureMu.Unlock()
	if s.ensured {
		return nil
	}
	for _, statement := range searchTimingDDL(s.schema, s.timings, s.state) {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	s.ensured = true
	return nil
}

func searchTimingMillis(hybrid bool, d time.Duration) int64 {
	if !hybrid {
		return -1
	}
	return d.Milliseconds()
}

func searchTimingFlag(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func (s *searchTimingStore) insert(ctx context.Context, t query.SearchTiming, p query.HostPressure) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO `+s.timings+`
			(occurred_at, request_id, surface, client, mode, status,
			 has_priorities, has_sources, result_rows, duration_ms,
			 embed_ms, lexical_ms, exact_ms, semantic_ms, fuse_ms, slowest_leg,
			 io_pressure_full_avg10, cpu_pressure_some_avg10, load_1m, cpu_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
		        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
		ON CONFLICT (occurred_at, request_id) DO NOTHING`,
		t.OccurredAt.UTC(), t.RequestID, t.Surface, t.Client, t.Mode, t.Status,
		searchTimingFlag(t.HasPriorities), searchTimingFlag(t.HasSources), int64(t.Rows), t.Duration.Milliseconds(),
		searchTimingMillis(t.Hybrid, t.Embed), searchTimingMillis(t.Hybrid, t.Lexical),
		searchTimingMillis(t.Hybrid, t.Exact), searchTimingMillis(t.Hybrid, t.Semantic),
		searchTimingMillis(t.Hybrid, t.Fuse), t.SlowestLeg,
		p.IOFullAvg10, p.CPUSomeAvg10, p.Load1m, int64(p.CPUCount),
	)
	return err
}

// heartbeat upserts the ONE state row. One row, not one per container: the
// pipeline's StateSource counts every error row and never ages one out, so a
// container replaced by a deploy must not leave its last verdict behind.
func (s *searchTimingStore) heartbeat(ctx context.Context, tick searchTimingTick) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	lastWrite := tick.LastWriteAt
	if lastWrite.IsZero() {
		lastWrite = time.Unix(0, 0).UTC()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO `+s.state+` AS st
			(writer, ticked_at, last_write_at, status, error,
			 written_since_tick, dropped_since_tick, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now())
		ON CONFLICT (writer) DO UPDATE SET
			ticked_at = EXCLUDED.ticked_at,
			last_write_at = GREATEST(st.last_write_at, EXCLUDED.last_write_at),
			status = EXCLUDED.status,
			error = EXCLUDED.error,
			written_since_tick = EXCLUDED.written_since_tick,
			dropped_since_tick = EXCLUDED.dropped_since_tick,
			updated_at = now()`,
		s.writerName, tick.TickedAt, lastWrite, tick.Status, tick.Error, tick.Written, tick.Dropped,
	)
	return err
}
