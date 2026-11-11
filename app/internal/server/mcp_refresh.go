package server

import (
	"context"
	"time"
)

// The sweep's lifetime context has no deadline. Bound the direct heartbeat
// write just as the HTTP heartbeat handler does, so a stalled database cannot
// prevent the next discovery sweep.
func recordMCPRefreshHeartbeat(ctx context.Context, store uploaderHeartbeatWriter, timeout time.Duration, refreshErr error) error {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	run := uploaderHeartbeat{Pipeline: "mcp_proxy", Device: "app", RanAt: time.Now().UTC().Format(time.RFC3339)}
	if refreshErr != nil {
		run.ExitCode = 1
		run.Error = refreshErr.Error()
	}
	_, err := store.upsert(ctx, run)
	return err
}
