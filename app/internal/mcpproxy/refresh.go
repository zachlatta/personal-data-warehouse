package mcpproxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ToolRefreshInterval bounds how long upstream definition changes go unnoticed
// without tying discovery latency or availability to downstream requests.
const ToolRefreshInterval = 5 * time.Minute

// RunRefreshLoop refreshes on startup and every five minutes until shutdown.
// report records a completed sweep's heartbeat, including partial failures.
func (s *Service) RunRefreshLoop(ctx context.Context, report func(error)) {
	s.runRefreshLoop(ctx, ToolRefreshInterval, report)
}

func (s *Service) runRefreshLoop(ctx context.Context, interval time.Duration, report func(error)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		err := s.RefreshAll(ctx)
		if ctx.Err() != nil {
			return
		}
		if report != nil {
			report(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// RefreshAll uses independent one-minute discovery budgets and at most four
// concurrent upstream sessions. Disabled/deleted connections are never polled.
func (s *Service) RefreshAll(ctx context.Context) error {
	listCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	records, err := s.store.List(listCtx)
	cancel()
	if err != nil {
		slog.Warn("MCP tool refresh could not read connections")
		return errors.New("connection store unavailable")
	}
	jobs := make(chan record)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []error
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range jobs {
				if err := s.Refresh(ctx, c.Name); err != nil {
					slog.Warn("MCP tool refresh failed", "connection", c.Name)
					mu.Lock()
					failures = append(failures, fmt.Errorf("%s: discovery failed", c.Name))
					mu.Unlock()
				} else {
					slog.Info("MCP tools refreshed", "connection", c.Name)
				}
			}
		}()
	}
send:
	for _, c := range records {
		if !c.Enabled || c.Deleted {
			continue
		}
		select {
		case jobs <- c:
		case <-ctx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()
	return errors.Join(append(failures, ctx.Err())...)
}
