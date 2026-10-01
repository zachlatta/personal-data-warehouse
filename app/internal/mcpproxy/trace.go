package mcpproxy

import (
	"context"
	"net/http"
	"strings"
)

// The proxy opens a fresh upstream MCP session for every call, so an upstream
// that groups work by session (the skills service deduplicates repeated skill
// reads within one trace) saw every proxied call as a new session. The trace id
// fixes that: it is read from the inbound request (X-Trace-ID, which the pdw CLI
// derives from the agent process it runs under, else a W3C traceparent's trace
// id) and forwarded upstream as X-Trace-ID.

// TraceHeader is the header carrying the caller's trace id in both directions.
const TraceHeader = "X-Trace-ID"

type traceKey struct{}

// ContextWithTraceID returns ctx carrying a sanitized trace id; an empty or
// unusable id leaves ctx unchanged.
func ContextWithTraceID(ctx context.Context, id string) context.Context {
	id = SanitizeTraceID(id)
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, traceKey{}, id)
}

// TraceIDFromContext returns the trace id set by ContextWithTraceID, or "".
func TraceIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(traceKey{}).(string)
	return id
}

// TraceMiddleware records the inbound request's trace id on its context so a
// proxied tool call can forward it upstream.
func TraceMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(TraceHeader)
		if id == "" {
			id = traceparentTraceID(r.Header.Get("traceparent"))
		}
		if id != "" {
			r = r.WithContext(ContextWithTraceID(r.Context(), id))
		}
		next.ServeHTTP(w, r)
	})
}

// traceparentTraceID extracts the trace-id field of a W3C traceparent
// ("00-<32 hex>-<16 hex>-<2 hex>").
func traceparentTraceID(v string) string {
	parts := strings.Split(strings.TrimSpace(v), "-")
	if len(parts) != 4 || len(parts[1]) != 32 {
		return ""
	}
	return parts[1]
}

// SanitizeTraceID keeps a trace id header-safe and bounded: letters, digits,
// and -_.: survive, anything else becomes '_', and it is cut at 96 bytes.
func SanitizeTraceID(v string) string {
	v = strings.TrimSpace(v)
	var b strings.Builder
	for _, r := range v {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.', r == ':':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := b.String()
	if len(out) > 96 {
		out = out[:96]
	}
	return out
}
