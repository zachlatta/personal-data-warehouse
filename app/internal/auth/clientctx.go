package auth

import (
	"context"
	"strings"
	"unicode"
)

// MaxClientNameLen caps how long a client-provided name can be. The name ends
// up in every request log line, so we don't want pathological values.
const MaxClientNameLen = 64

type clientNameKey struct{}

// clientNameHolder is shared via context so any auth middleware can write the
// authenticated client's name and the outer logging middleware can read it
// back out after the handler returns. Context values are normally immutable,
// so we share a pointer to a mutable struct instead.
type clientNameHolder struct {
	name string
}

// WithClientNameHolder installs an empty client-name slot on the context.
// Call this once at the top of the request pipeline (in the request logging
// middleware) so downstream auth middleware has somewhere to write.
func WithClientNameHolder(ctx context.Context) context.Context {
	return context.WithValue(ctx, clientNameKey{}, &clientNameHolder{})
}

// SetClientName records the authenticated client name on the request's
// holder. No-op if WithClientNameHolder was not called for this context.
func SetClientName(ctx context.Context, name string) {
	if h, ok := ctx.Value(clientNameKey{}).(*clientNameHolder); ok {
		h.name = name
	}
}

// ClientNameFromContext returns the authenticated client name, or "" if the
// request hasn't been authenticated yet (or the holder was not installed).
func ClientNameFromContext(ctx context.Context) string {
	if h, ok := ctx.Value(clientNameKey{}).(*clientNameHolder); ok {
		return h.name
	}
	return ""
}

// ValidateClientName trims surrounding whitespace and enforces the rules we
// rely on elsewhere: non-empty, <= MaxClientNameLen runes, no control
// characters (so log lines stay one-line), and no ':' (the static-bearer
// format uses ':' as the name/token separator). Returns the cleaned name and
// ok=false if the input fails any check.
func ValidateClientName(raw string) (string, bool) {
	name := strings.TrimSpace(raw)
	if name == "" {
		return "", false
	}
	if len([]rune(name)) > MaxClientNameLen {
		return "", false
	}
	for _, r := range name {
		if r == ':' || unicode.IsControl(r) {
			return "", false
		}
	}
	return name, true
}
