// Package api exposes the shared tool.Registry over a CLI-friendly HTTP
// surface. Every tool listed on MCP is also reachable here as
// POST /api/tools/{name}. Auth is the static PDW_SECRET_TOKEN bearer; the
// handler itself does not check auth — wrap it in auth.RequireStaticBearer.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

// NewHandler returns the API mux serving /api/tools and /api/tools/{name}.
// Pass nil for logger to use slog.Default().
func NewHandler(registry *tool.Registry, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	h := &handler{registry: registry, logger: logger.With("component", "api")}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/tools", h.handleTools)
	mux.HandleFunc("/api/tools/", h.handleToolCall)
	return mux
}

type handler struct {
	registry *tool.Registry
	logger   *slog.Logger
}

type listEntry struct {
	Name        string `json:"name"`
	Title       string `json:"title"`
	Description string `json:"description"`
	InputSchema any    `json:"input_schema"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func (h *handler) handleTools(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	entries := make([]listEntry, 0, len(h.registry.All()))
	for _, t := range h.registry.All() {
		schema, err := t.InputSchema()
		if err != nil {
			h.logger.ErrorContext(r.Context(), "input schema derivation failed", "tool", t.Name(), "error", err)
			writeError(w, http.StatusInternalServerError, "schema_error", "failed to derive input schema for "+t.Name())
			return
		}
		entries = append(entries, listEntry{
			Name:        t.Name(),
			Title:       t.Title(),
			Description: t.Description(),
			InputSchema: schema,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": entries})
}

func (h *handler) handleToolCall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/api/tools/")
	if name == "" || strings.Contains(name, "/") {
		writeError(w, http.StatusNotFound, "tool_not_found", "no tool named "+name)
		return
	}
	t := h.registry.ByName(name)
	if t == nil {
		writeError(w, http.StatusNotFound, "tool_not_found", "no tool named "+name)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_input", "failed to read request body: "+err.Error())
		return
	}
	body = trimWhitespace(body)

	h.logger.InfoContext(r.Context(), "API tool called", "tool", name, "client", pdwauth.ClientNameFromContext(r.Context()))
	out, isErr, callErr := t.Invoke(r.Context(), body)
	if callErr != nil {
		// Decode failures bubble up as callErr too; tell them apart by type.
		var syntaxErr *json.SyntaxError
		var typeErr *json.UnmarshalTypeError
		if errors.As(callErr, &syntaxErr) || errors.As(callErr, &typeErr) {
			writeError(w, http.StatusBadRequest, "invalid_input", "request body is not valid JSON for this tool: "+callErr.Error())
			return
		}
		writeError(w, http.StatusBadGateway, "tool_error", callErr.Error())
		return
	}
	// Soft IsError (per-statement errors etc.) returns 200 with data;
	// callers inspect partial-success fields in the body. Matches MCP.
	_ = isErr
	writeJSON(w, http.StatusOK, map[string]any{"data": out})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	data, err := json.Marshal(body)
	if err != nil {
		http.Error(w, "encode error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": errorBody{Code: code, Message: message}})
}

func writeMethodNotAllowed(w http.ResponseWriter, allowed string) {
	w.Header().Set("Allow", allowed)
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use "+allowed)
}

func trimWhitespace(b []byte) []byte {
	start := 0
	for start < len(b) && isJSONSpace(b[start]) {
		start++
	}
	end := len(b)
	for end > start && isJSONSpace(b[end-1]) {
		end--
	}
	return b[start:end]
}

func isJSONSpace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r':
		return true
	}
	return false
}
