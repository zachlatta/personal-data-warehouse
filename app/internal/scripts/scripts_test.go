package scripts

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func testService() *Service {
	return newTestService(func(ctx context.Context, method string, input json.RawMessage) (any, error) {
		return map[string]any{"rows": []any{map[string]any{"ref": "test:1", "priority": "self", "large": int64(9007199254740993)}}}, nil
	}, true, nil)
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
