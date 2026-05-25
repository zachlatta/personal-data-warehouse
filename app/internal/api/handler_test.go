package api_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/api"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

type echoInput struct {
	Value string `json:"value"`
	N     int    `json:"n,omitempty"`
}

type echoOutput struct {
	Value string `json:"value"`
	N     int    `json:"n"`
	Soft  string `json:"soft,omitempty"`
}

func newEcho(name string) *tool.Typed[echoInput, echoOutput] {
	return &tool.Typed[echoInput, echoOutput]{
		NameStr:        name,
		TitleStr:       "Echo",
		DescriptionStr: "echo input back",
		Handle: func(_ context.Context, in echoInput) (echoOutput, error) {
			return echoOutput{Value: in.Value, N: in.N}, nil
		},
	}
}

func newFailingTool(name string, err error) *tool.Typed[echoInput, echoOutput] {
	return &tool.Typed[echoInput, echoOutput]{
		NameStr:        name,
		TitleStr:       name,
		DescriptionStr: "always errors",
		Handle: func(_ context.Context, _ echoInput) (echoOutput, error) {
			return echoOutput{}, err
		},
	}
}

func newSoftErrorTool(name, soft string) *tool.Typed[echoInput, echoOutput] {
	return &tool.Typed[echoInput, echoOutput]{
		NameStr:        name,
		TitleStr:       name,
		DescriptionStr: "soft error",
		Handle: func(_ context.Context, _ echoInput) (echoOutput, error) {
			return echoOutput{Soft: soft}, nil
		},
		IsError: func(o echoOutput) bool { return o.Soft != "" },
	}
}

func newHandler(t *testing.T, tools ...tool.Tool) http.Handler {
	t.Helper()
	registry := tool.NewRegistry(tools)
	return api.NewHandler(registry, nil)
}

func TestListToolsReturnsRegistryEntries(t *testing.T) {
	h := newHandler(t, newEcho("echo"), newSoftErrorTool("soft", "nope"))
	req := httptest.NewRequest(http.MethodGet, "/api/tools", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("content-type = %q", got)
	}
	var resp struct {
		Data []struct {
			Name        string         `json:"name"`
			Title       string         `json:"title"`
			Description string         `json:"description"`
			InputSchema map[string]any `json:"input_schema"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode list response: %v body=%s", err, rec.Body.String())
	}
	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 tools, got %d", len(resp.Data))
	}
	if resp.Data[0].Name != "echo" || resp.Data[1].Name != "soft" {
		t.Fatalf("unexpected order: %v", resp.Data)
	}
	if resp.Data[0].InputSchema == nil || resp.Data[0].InputSchema["type"] != "object" {
		t.Fatalf("expected input_schema for echo, got %#v", resp.Data[0].InputSchema)
	}
}

func TestCallToolSuccessReturnsDataEnvelope(t *testing.T) {
	h := newHandler(t, newEcho("echo"))
	req := httptest.NewRequest(http.MethodPost, "/api/tools/echo", strings.NewReader(`{"value":"hi","n":2}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Data echoOutput `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Data.Value != "hi" || resp.Data.N != 2 {
		t.Fatalf("data = %#v", resp.Data)
	}
}

func TestCallToolAcceptsEmptyBodyForZeroInput(t *testing.T) {
	h := newHandler(t, newEcho("echo"))
	req := httptest.NewRequest(http.MethodPost, "/api/tools/echo", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestCallToolUnknownNameReturns404(t *testing.T) {
	h := newHandler(t, newEcho("echo"))
	req := httptest.NewRequest(http.MethodPost, "/api/tools/missing", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d", rec.Code)
	}
	var resp struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body.String())
	}
	if resp.Error.Code != "tool_not_found" {
		t.Fatalf("error code = %q", resp.Error.Code)
	}
}

func TestCallToolMalformedBodyReturns400(t *testing.T) {
	h := newHandler(t, newEcho("echo"))
	req := httptest.NewRequest(http.MethodPost, "/api/tools/echo", strings.NewReader(`{not json`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "invalid_input") {
		t.Fatalf("body = %s", body)
	}
}

func TestCallToolHandlerErrorReturns502(t *testing.T) {
	h := newHandler(t, newFailingTool("broken", errBoom))
	req := httptest.NewRequest(http.MethodPost, "/api/tools/broken", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d", rec.Code)
	}
	var resp struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "tool_error" || !strings.Contains(resp.Error.Message, "boom") {
		t.Fatalf("error = %#v", resp.Error)
	}
}

func TestCallToolSoftErrorReturns200WithData(t *testing.T) {
	// Per the API contract: tools that today flag IsError on MCP without
	// failing — e.g. query returning per-statement errors — must still
	// return 200 over HTTP so callers can inspect partial-success bodies.
	// The "soft" IsError predicate does NOT promote to a 4xx/5xx.
	h := newHandler(t, newSoftErrorTool("soft", "partial"))
	req := httptest.NewRequest(http.MethodPost, "/api/tools/soft", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"soft":"partial"`) {
		t.Fatalf("body lost soft field: %s", rec.Body.String())
	}
}

func TestCallToolWrongMethodReturns405(t *testing.T) {
	h := newHandler(t, newEcho("echo"))
	req := httptest.NewRequest(http.MethodGet, "/api/tools/echo", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Header().Get("Allow"); got != "POST" {
		t.Fatalf("Allow header = %q", got)
	}
}

func TestListToolsWrongMethodReturns405(t *testing.T) {
	h := newHandler(t, newEcho("echo"))
	req := httptest.NewRequest(http.MethodPost, "/api/tools", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestUnknownPathReturns404(t *testing.T) {
	h := newHandler(t, newEcho("echo"))
	req := httptest.NewRequest(http.MethodGet, "/api/unrelated", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d", rec.Code)
	}
}

var errBoom = boomError("boom")

type boomError string

func (e boomError) Error() string { return string(e) }
