package scripts

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

func testService() *Service {
	return newTestService(func(ctx context.Context, method string, input json.RawMessage) (any, error) {
		return map[string]any{"rows": []any{map[string]any{"ref": "test:1", "priority": "self", "large": int64(9007199254740993)}}}, nil
	}, true, nil)
}

func TestClientNamesDivideSharedCredentialCapacity(t *testing.T) {
	started, release := make(chan struct{}, 2), make(chan struct{})
	s := newTestService(func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
		started <- struct{}{}
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, true, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan struct{}, 2)
	defer func() {
		close(release)
		cancel()
		for i := 0; i < 2; i++ {
			<-done
		}
	}()
	a := auth.WithClientNameHolder(ctx)
	auth.SetClientName(a, "client-a")
	b := auth.WithClientNameHolder(ctx)
	auth.SetClientName(b, "client-b")
	// Fill both slots with real blocked reads; another client sharing the
	// credential can still execute within the global four-run cap.
	for i := 0; i < 2; i++ {
		go func() { s.Run(a, Input{Code: `return await pdw.search({query:'hold'});`}); done <- struct{}{} }()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("reads did not start")
		}
	}
	if r := s.Run(a, Input{Code: `return 1;`}); r.Error == nil || r.Error.Code != "busy" {
		t.Fatalf("same client: %+v", r)
	}
	if r := s.Run(b, Input{Code: `return 2;`}); r.Error != nil || string(r.Value) != "2" {
		t.Fatalf("other client: %+v", r)
	}
}

func TestRealClockAndCatchableStackLimit(t *testing.T) {
	r := testService().Run(context.Background(), Input{Code: `return Date.now();`})
	var now int64
	if r.Error != nil || json.Unmarshal(r.Value, &now) != nil || time.Since(time.UnixMilli(now)).Abs() > 5*time.Second {
		t.Fatalf("clock: %+v", r)
	}
	r = testService().Run(context.Background(), Input{Code: `try { function f(){return f()}; f(); } catch(e) {return e.name+':'+e.message;}`})
	if r.Error != nil || string(r.Value) != `"InternalError:stack overflow"` {
		t.Fatalf("stack: %+v", r)
	}
	r = testService().Run(context.Background(), Input{Code: `function f(){return f()}; f();`})
	if r.Error == nil || r.Error.Code != "stack_limit" {
		t.Fatalf("uncaught stack: %+v", r)
	}
}

// Race instrumentation needs its shadow heap; use a plain production worker
// while racing the parent broker so the real 128 MiB RSS limit stays intact.
func newTestService(invoke Invoke, role bool, record Recorder) *Service {
	s := New(invoke, role, record)
	if path := os.Getenv("PDW_SCRIPT_TEST_WORKER"); path != "" {
		s.executable = path
	}
	return s
}

func TestCompositionAndCitations(t *testing.T) {
	s := testService()
	r := s.Run(context.Background(), Input{Code: `const a=await pdw.search({query:"anchor",priorities:["self"]}); await pdw.context({ref:a.rows[0].ref}); return a.rows[0].large;`})
	if r.Error != nil || string(r.Value) != `"9007199254740993"` || len(r.Operations) != 2 || len(r.Sources) != 1 {
		t.Fatalf("%+v", r)
	}
}

func TestHostileScripts(t *testing.T) {
	for _, code := range []string{
		`while(true){}`, `return new ArrayBuffer(1000000000);`,
		`async function storm(){await Promise.resolve();return storm()}; await storm();`,
		`return await import("os");`, `return await import("node:fs");`,
		`return "x".repeat(200000);`, `return await pdw.connection_call({});`,
		`return await pdw.sql({sql:"DELETE FROM timeline.events"});`,
	} {
		t.Run(code[:min(24, len(code))], func(t *testing.T) {
			r := testService().Run(context.Background(), Input{Code: code, TimeoutMS: 250})
			if r.Error == nil {
				t.Fatalf("hostile script succeeded: %s", r.Value)
			}
		})
	}
}

func TestFreshStateAndNoAmbientCapabilities(t *testing.T) {
	s := testService()
	r := s.Run(context.Background(), Input{Code: `globalThis.leak=42; return Object.isFrozen(pdw);`})
	if r.Error != nil || string(r.Value) != "true" {
		t.Fatalf("%+v", r)
	}
	r = s.Run(context.Background(), Input{Code: `return [typeof leak,typeof fetch,typeof process,typeof require,typeof setTimeout];`})
	if r.Error != nil || strings.Count(string(r.Value), "undefined") != 5 {
		t.Fatalf("%+v", r)
	}
}

func TestMissingRoleAndCancellation(t *testing.T) {
	s := newTestService(nil, false, nil)
	if r := s.Run(context.Background(), Input{Code: `return 1;`}); r.Error == nil || r.Error.Code != "query_role_required" {
		t.Fatalf("%+v", r)
	}
	s = newTestService(func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, true, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	r := s.Run(ctx, Input{Code: `return await pdw.sql({sql:"select 1"});`})
	if r.Error == nil || time.Since(time.Now()) > time.Second {
		t.Fatalf("%+v", r)
	}
}

func TestBridgeRejectsForgedMethodsAndBudgets(t *testing.T) {
	r := testService().Run(context.Background(), Input{Code: `await Promise.all(Array.from({length:20},()=>pdw.search({query:"x"})));`})
	if r.Error == nil {
		t.Fatal("operation budget bypass")
	}
}
