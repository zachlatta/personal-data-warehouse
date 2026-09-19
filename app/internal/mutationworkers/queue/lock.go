package queue

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Lock is the cross-process sync lock the Python `exclusive_sync_lock` took:
// a Postgres advisory lock when a lock database is configured, otherwise a
// flock on a per-worker file. It is a non-blocking try-lock: Acquire reports
// false the instant another holder has it so the caller can skip gracefully.
type Lock interface {
	Acquire(ctx context.Context) (release func(), acquired bool, err error)
}

// LockFunc adapts a function to Lock (tests use it to force either outcome).
type LockFunc func(ctx context.Context) (func(), bool, error)

// Acquire implements Lock.
func (f LockFunc) Acquire(ctx context.Context) (func(), bool, error) { return f(ctx) }

// SyncLock is the production Lock for a named worker and its advisory-lock id.
type SyncLock struct {
	Name   string
	LockID int64
	Getenv func(string) string
}

// LockEnvPrefix mirrors sync_locks.lock_env_prefix.
func LockEnvPrefix(name string) string {
	var out strings.Builder
	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			out.WriteRune(unicode.ToUpper(r))
		} else {
			out.WriteRune('_')
		}
	}
	return out.String()
}

// PostgresURL is sync_locks.sync_lock_postgres_url: the lock database, when
// one is configured, else "".
func (l SyncLock) PostgresURL() string {
	prefix := LockEnvPrefix(l.Name)
	return common.FirstNonEmpty(
		l.Getenv(prefix+"_SYNC_LOCK_POSTGRES_URL"),
		l.Getenv("DAGSTER_POSTGRES_URL"),
		l.Getenv("DATABASE_URL"),
	)
}

// Path is sync_locks.sync_lock_path: the flock file used when no lock
// database is configured.
func (l SyncLock) Path() string {
	prefix := LockEnvPrefix(l.Name)
	fallback := filepath.Join(os.TempDir(), fmt.Sprintf("personal-data-warehouse-%s-sync.lock", l.Name))
	return common.ExpandUser(common.FirstNonEmpty(l.Getenv(prefix+"_SYNC_LOCK_PATH"), fallback))
}

// Acquire implements Lock.
func (l SyncLock) Acquire(ctx context.Context) (func(), bool, error) {
	if url := l.PostgresURL(); url != "" {
		return acquireAdvisoryLock(ctx, NormalizePostgresURL(url), l.LockID)
	}
	lock, acquired, err := common.TryRunLock(l.Path())
	if err != nil || !acquired {
		return nil, false, err
	}
	return lock.Release, true, nil
}

func acquireAdvisoryLock(ctx context.Context, url string, lockID int64) (func(), bool, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, false, err
	}
	// One dedicated session: an advisory lock is session-scoped, so the pool
	// must never hand the release to a different connection.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, false, err
	}
	var acquired bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, lockID).Scan(&acquired); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, false, err
	}
	if !acquired {
		_ = conn.Close()
		_ = db.Close()
		return nil, false, nil
	}
	release := func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(releaseCtx, `SELECT pg_advisory_unlock_all()`)
		_ = conn.Close()
		_ = db.Close()
	}
	return release, true, nil
}
