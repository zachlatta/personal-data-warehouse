package tool_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

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
