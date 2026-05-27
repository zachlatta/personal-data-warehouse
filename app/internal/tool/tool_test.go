package tool_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

type echoInput struct {
	Value string `json:"value"`
	N     int    `json:"n,omitempty"`
}

type echoOutput struct {
	Value string `json:"value"`
	N     int    `json:"n"`
	Error string `json:"error,omitempty"`
}

func newEcho(handler func(context.Context, echoInput) (echoOutput, error), isErr func(echoOutput) bool) *tool.Typed[echoInput, echoOutput] {
	return &tool.Typed[echoInput, echoOutput]{
		NameStr:        "echo",
		TitleStr:       "Echo",
		DescriptionStr: "echo a value",
		Handle:         handler,
		IsError:        isErr,
	}
}

func TestTypedReportsMetadata(t *testing.T) {
	tt := newEcho(func(_ context.Context, in echoInput) (echoOutput, error) {
		return echoOutput{Value: in.Value, N: in.N}, nil
	}, nil)
	if tt.Name() != "echo" {
		t.Fatalf("Name = %q", tt.Name())
	}
	if tt.Title() != "Echo" {
		t.Fatalf("Title = %q", tt.Title())
	}
	if tt.Description() != "echo a value" {
		t.Fatalf("Description = %q", tt.Description())
	}
}

func TestTypedInvokeDecodesInputAndReturnsOutput(t *testing.T) {
	tt := newEcho(func(_ context.Context, in echoInput) (echoOutput, error) {
		return echoOutput{Value: in.Value, N: in.N}, nil
	}, nil)
	out, isErr, err := tt.Invoke(context.Background(), json.RawMessage(`{"value":"hi","n":3}`))
	if err != nil {
		t.Fatalf("Invoke returned err: %v", err)
	}
	if isErr {
		t.Fatal("Invoke reported isError on a successful call")
	}
	got, ok := out.(echoOutput)
	if !ok {
		t.Fatalf("output type = %T", out)
	}
	if got.Value != "hi" || got.N != 3 {
		t.Fatalf("unexpected output: %#v", got)
	}
}

func TestTypedInvokeAllowsEmptyInputForZeroStruct(t *testing.T) {
	tt := newEcho(func(_ context.Context, in echoInput) (echoOutput, error) {
		return echoOutput{Value: in.Value, N: in.N}, nil
	}, nil)
	out, isErr, err := tt.Invoke(context.Background(), nil)
	if err != nil || isErr {
		t.Fatalf("expected success on empty input, got err=%v isErr=%v", err, isErr)
	}
	got := out.(echoOutput)
	if got.Value != "" || got.N != 0 {
		t.Fatalf("expected zero output, got %#v", got)
	}
}

func TestTypedInvokeReturnsDecodeError(t *testing.T) {
	tt := newEcho(func(_ context.Context, in echoInput) (echoOutput, error) {
		t.Fatal("handler must not run when decode fails")
		return echoOutput{}, nil
	}, nil)
	_, isErr, err := tt.Invoke(context.Background(), json.RawMessage(`{not json}`))
	if err == nil {
		t.Fatal("expected decode error")
	}
	if !isErr {
		t.Fatal("decode failure must set isError=true")
	}
}

func TestTypedInvokeReturnsHandlerError(t *testing.T) {
	boom := errors.New("boom")
	tt := newEcho(func(_ context.Context, _ echoInput) (echoOutput, error) {
		return echoOutput{}, boom
	}, nil)
	_, isErr, err := tt.Invoke(context.Background(), json.RawMessage(`{}`))
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom, got %v", err)
	}
	if !isErr {
		t.Fatal("handler error must set isError=true")
	}
}

func TestTypedInvokeUsesIsErrorPredicate(t *testing.T) {
	tt := newEcho(func(_ context.Context, _ echoInput) (echoOutput, error) {
		return echoOutput{Error: "soft"}, nil
	}, func(o echoOutput) bool { return o.Error != "" })
	_, isErr, err := tt.Invoke(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Invoke returned err: %v", err)
	}
	if !isErr {
		t.Fatal("IsError predicate must promote soft error to isErr=true")
	}
}

func TestRegistryReturnsToolsInRegistrationOrder(t *testing.T) {
	a := newEcho(nil, nil)
	a.NameStr = "a"
	b := newEcho(nil, nil)
	b.NameStr = "b"
	c := newEcho(nil, nil)
	c.NameStr = "c"
	reg := tool.NewRegistry([]tool.Tool{a, b}, []tool.Tool{c}, nil, []tool.Tool{nil})
	names := []string{}
	for _, t := range reg.All() {
		names = append(names, t.Name())
	}
	if len(names) != 3 || names[0] != "a" || names[1] != "b" || names[2] != "c" {
		t.Fatalf("All() order = %v", names)
	}
}

func TestRegistryByNameFindsAndMisses(t *testing.T) {
	a := newEcho(nil, nil)
	a.NameStr = "a"
	reg := tool.NewRegistry([]tool.Tool{a})
	if got := reg.ByName("a"); got == nil || got.Name() != "a" {
		t.Fatalf("ByName(a) = %v", got)
	}
	if got := reg.ByName("missing"); got != nil {
		t.Fatalf("ByName(missing) = %v, want nil", got)
	}
}

type recordedResult struct {
	name    string
	output  any
	isError bool
	err     error
}

type resultRecorder struct {
	mu      sync.Mutex
	results []recordedResult
}

func (r *resultRecorder) record(_ context.Context, name string, output any, isError bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, recordedResult{name: name, output: output, isError: isError, err: err})
}

func (r *resultRecorder) snapshot() []recordedResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedResult, len(r.results))
	copy(out, r.results)
	return out
}

func runOverMCP(t *testing.T, tt tool.Tool, hooks tool.Hooks, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	srv := mcp.NewServer(&mcp.Implementation{Name: "tool-test", Version: "0.1.0"}, nil)
	tt.RegisterMCP(srv, hooks)
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.Run(ctx, serverTransport) }()
	client := mcp.NewClient(&mcp.Implementation{Name: "tool-test-client", Version: "0.1.0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	return result
}

func TestRegisterMCPFiresOnResultWithOutputOnSuccess(t *testing.T) {
	tt := newEcho(func(_ context.Context, in echoInput) (echoOutput, error) {
		return echoOutput{Value: in.Value, N: in.N}, nil
	}, nil)
	rec := &resultRecorder{}
	runOverMCP(t, tt, tool.Hooks{OnResult: rec.record}, "echo", map[string]any{"value": "hi", "n": 2})

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 OnResult call, got %d (%#v)", len(got), got)
	}
	if got[0].name != "echo" {
		t.Fatalf("name = %q", got[0].name)
	}
	if got[0].isError {
		t.Fatal("isError should be false on success")
	}
	if got[0].err != nil {
		t.Fatalf("err = %v", got[0].err)
	}
	out, ok := got[0].output.(echoOutput)
	if !ok {
		t.Fatalf("output type = %T", got[0].output)
	}
	if out.Value != "hi" || out.N != 2 {
		t.Fatalf("output = %#v", out)
	}
}

func TestRegisterMCPFiresOnResultWithSoftIsErrorTrue(t *testing.T) {
	tt := newEcho(func(_ context.Context, _ echoInput) (echoOutput, error) {
		return echoOutput{Error: "partial"}, nil
	}, func(o echoOutput) bool { return o.Error != "" })
	rec := &resultRecorder{}
	runOverMCP(t, tt, tool.Hooks{OnResult: rec.record}, "echo", map[string]any{"value": "ignored"})

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 OnResult call, got %d", len(got))
	}
	if !got[0].isError {
		t.Fatal("soft IsError predicate must surface as isError=true on the hook")
	}
	if got[0].err != nil {
		t.Fatalf("err must be nil on soft errors, got %v", got[0].err)
	}
	if _, ok := got[0].output.(echoOutput); !ok {
		t.Fatalf("output type = %T; soft errors must still carry the payload", got[0].output)
	}
}

func TestRegisterMCPFiresOnResultWithHandlerError(t *testing.T) {
	boom := errors.New("boom")
	tt := newEcho(func(_ context.Context, _ echoInput) (echoOutput, error) {
		return echoOutput{}, boom
	}, nil)
	rec := &resultRecorder{}
	runOverMCP(t, tt, tool.Hooks{OnResult: rec.record}, "echo", map[string]any{"value": "ignored"})

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 OnResult call, got %d", len(got))
	}
	if !got[0].isError {
		t.Fatal("handler error must surface as isError=true")
	}
	if got[0].err == nil || got[0].err.Error() != "boom" {
		t.Fatalf("err = %v", got[0].err)
	}
	if got[0].output != nil {
		t.Fatalf("output must be nil on handler error, got %#v", got[0].output)
	}
}

func TestRegisterMCPWithoutOnResultHookStillWorks(t *testing.T) {
	tt := newEcho(func(_ context.Context, in echoInput) (echoOutput, error) {
		return echoOutput{Value: in.Value}, nil
	}, nil)
	// nil hooks must not panic and the call should still complete.
	result := runOverMCP(t, tt, tool.Hooks{}, "echo", map[string]any{"value": "ok"})
	if result.IsError {
		t.Fatalf("call failed: %#v", result.Content)
	}
}

func TestRegistryRejectsDuplicateNames(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate tool names")
		}
	}()
	a := newEcho(nil, nil)
	a.NameStr = "dup"
	b := newEcho(nil, nil)
	b.NameStr = "dup"
	_ = tool.NewRegistry([]tool.Tool{a, b})
}
