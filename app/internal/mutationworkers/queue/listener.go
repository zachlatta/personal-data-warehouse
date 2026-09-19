package queue

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutations"
)

// NotificationChannel is the LISTEN channel the app NOTIFYs after an
// approval commits. The rows are the durable queue: NOTIFY only removes the
// polling delay, so the resident worker drains at startup and after every
// bounded wait even when nothing arrived.
const NotificationChannel = mutations.UpstreamMutationNotificationChannel

// DefaultQueuePollSeconds mirrors DEFAULT_MUTATION_QUEUE_POLL_SECONDS.
const DefaultQueuePollSeconds = 30.0

// Listener waits for approval notifications.
type Listener interface {
	// Wait blocks up to timeout and reports whether a notification arrived.
	Wait(ctx context.Context, timeout time.Duration) (bool, error)
	Close() error
}

// PostgresListener is a dedicated LISTEN connection.
type PostgresListener struct {
	conn *pgx.Conn
}

// NewPostgresListener connects and issues LISTEN.
func NewPostgresListener(ctx context.Context, databaseURL string) (*PostgresListener, error) {
	conn, err := pgx.Connect(ctx, NormalizePostgresURL(databaseURL))
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, `LISTEN "`+NotificationChannel+`"`); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}
	return &PostgresListener{conn: conn}, nil
}

// Wait implements Listener.
func (l *PostgresListener) Wait(ctx context.Context, timeout time.Duration) (bool, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := l.conn.WaitForNotification(waitCtx)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		return false, nil
	}
	return false, err
}

// Close implements Listener.
func (l *PostgresListener) Close() error {
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return l.conn.Close(closeCtx)
}

// RunNotificationLoop is mutation_notifications.run_notification_loop: drain
// now, then after every notification or fallback poll interval. processPending
// reports true when it claimed a batch; it is called again until the durable
// queue is empty so a batch-size cap cannot strand work until the next
// approval. It returns when ctx is cancelled or the listener fails.
func RunNotificationLoop(ctx context.Context, listener Listener, processPending func() (bool, error), pollInterval time.Duration) error {
	defer listener.Close()
	for ctx.Err() == nil {
		for {
			more, err := processPending()
			if err != nil {
				return err
			}
			if !more {
				break
			}
			if ctx.Err() != nil {
				return nil
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		if _, err := listener.Wait(ctx, pollInterval); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
	return nil
}
