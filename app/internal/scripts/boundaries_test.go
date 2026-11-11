package scripts

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWorkerDeath(t *testing.T) {
	s := testService()
	s.executable = "/bin/false"
	r := s.Run(context.Background(), Input{Code: `return 1;`})
	if r.Error == nil || r.Error.Code != "worker_error" {
		t.Fatalf("%+v", r)
	}
}
func TestCaughtErrorsRemainObservableAndNoResultsAreRecorded(t *testing.T) {
	var recorded Record
	s := newTestService(func(context.Context, string, json.RawMessage) (any, error) {
		return nil, fault("bad_read", "private example")
	}, true, func(_ context.Context, r Record) error { recorded = r; return nil })
	r := s.Run(context.Background(), Input{Code: `try{await pdw.search({query:'anchor'});}catch(e){} return "private output";`})
	if r.Error != nil || len(recorded.Operations) != 1 || recorded.Operations[0].Error == nil || recorded.Value != nil || recorded.Sources != nil {
		t.Fatalf("%+v / %+v", r, recorded)
	}
}
func TestExclusionsApplyBeforeProjection(t *testing.T) {
	s := testService()
	ctx := WithExclusions(context.Background(), Exclusions{Refs: []string{"test:1"}})
	r := s.Run(ctx, Input{Code: `const a=await pdw.search({query:'anchor'});return a.rows.length;`})
	if r.Error != nil || string(r.Value) != "0" || r.DroppedRows != 1 || len(r.Sources) != 0 {
		t.Fatalf("%+v", r)
	}
}
func TestTimeoutCancelsOutstandingBrokerWork(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	s := newTestService(func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	}, true, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Response, 1)
	go func() { done <- s.Run(ctx, Input{Code: `return await pdw.sql({sql:'select 1'});`}) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("worker did not dispatch")
	}
	cancel()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("query not cancelled")
	}
	select {
	case r := <-done:
		if r.Error == nil {
			t.Fatal("cancel succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("worker not reaped")
	}
}
func TestPlaceholdersAreLiteralData(t *testing.T) {
	r := testService().Run(context.Background(), Input{Code: `return "__ARGS__ __CODE__";`, Args: json.RawMessage(`{"value":"__CODE__"}`)})
	if r.Error != nil || string(r.Value) != `"__ARGS__ __CODE__"` {
		t.Fatalf("%+v", r)
	}
}
func TestEngineHasPinnedHash(t *testing.T) {
	b, err := os.ReadFile("engine.sha256")
	if err != nil || len(b) != 65 {
		t.Fatal("missing artifact hash")
	}
}

func TestForgedWorkerRequestNeverReachesBroker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "forged-worker")
	if e := os.WriteFile(path, []byte("#!/bin/sh\nread input\nprintf '%s\\n' '{\"calls\":[{\"id\":1,\"method\":\"connection_call\",\"input\":{}}],\"done\":false}'\n"), 0700); e != nil {
		t.Fatal(e)
	}
	called := false
	s := newTestService(func(context.Context, string, json.RawMessage) (any, error) { called = true; return nil, nil }, true, nil)
	s.executable = path
	r := s.Run(context.Background(), Input{Code: `return 1;`})
	if called || r.Error == nil || r.Error.Code != "worker_protocol" {
		t.Fatalf("%+v called=%v", r, called)
	}
}

func TestGuestErrorsCannotPersistPrivateOutcomeCategories(t *testing.T) {
	var record Record
	s := newTestService(nil, true, func(_ context.Context, r Record) error { record = r; return nil })
	r := s.Run(context.Background(), Input{Code: `throw {code:"private data",message:"example"};`})
	if r.Error == nil || record.Outcome != "script_error" {
		t.Fatalf("%+v", r)
	}
}

func TestLargeBoundedOperationCanBeReducedInWorker(t *testing.T) {
	s := newTestService(func(context.Context, string, json.RawMessage) (any, error) {
		return map[string]any{"rows": []any{map[string]any{"text": strings.Repeat("x", 200000)}}}, nil
	}, true, nil)
	r := s.Run(context.Background(), Input{Code: `const r=await pdw.sql({sql:'select 1'});return r.rows[0].text.length;`})
	if r.Error != nil || string(r.Value) != "200000" {
		t.Fatalf("%+v", r)
	}
}
