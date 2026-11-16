package server

import (
	"context"
	"errors"
	"testing"
	"time"
)

type blockedRefreshHeartbeat struct {
	run uploaderHeartbeat
}

func (w *blockedRefreshHeartbeat) upsert(ctx context.Context, run uploaderHeartbeat) (uploaderHeartbeatStreak, error) {
	w.run = run
	<-ctx.Done()
	return uploaderHeartbeatStreak{}, ctx.Err()
}

func TestMCPRefreshHeartbeatBoundsDatabaseWrite(t *testing.T) {
	writer := &blockedRefreshHeartbeat{}
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		done <- recordMCPRefreshHeartbeat(ctx, writer, 10*time.Millisecond, errors.New("example: discovery failed"))
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("heartbeat write: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("heartbeat blocked the next discovery sweep")
	}
	if writer.run.Pipeline != "mcp_proxy" || writer.run.Device != "app" || writer.run.ExitCode != 1 || writer.run.Error != "example: discovery failed" {
		t.Fatalf("sweep failure lost: %+v", writer.run)
	}
}
