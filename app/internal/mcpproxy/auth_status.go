package mcpproxy

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2"
)

// Connection statuses the token refresh can set. Until 2026-11-23 every
// refresh error, whatever it was, became "upstream authorization expired;
// reconnect in Connections", the cause was dropped, and the status was
// sealed inside the encrypted payload, where no health view could read it:
// hack_club_data_warehouse failed that way for about 11 hours unseen.
const (
	statusConnected           = "connected"
	statusAuthorizationFailed = "authorization_failed"
)

// refreshError is a failed token refresh, classified. A transient one (the
// token endpoint unreachable, a 5xx or a 429) leaves the connection's status
// alone; anything else the token endpoint refuses means the grant is gone and
// the connection needs a reconnect.
type refreshError struct {
	transient bool
	cause     string
}

func (e *refreshError) Error() string {
	if e.transient {
		return "upstream token refresh failed transiently (" + e.cause + "); the connection is unchanged, try again shortly"
	}
	return "upstream authorization failed (" + e.cause + "); reconnect in Connections"
}

// refreshCauseLimit caps the stored last_error.
const refreshCauseLimit = 300

// lastError is the redacted text stored outside the encrypted payload.
func (e *refreshError) lastError() string {
	if e.transient {
		return "transient: " + e.cause
	}
	return e.cause
}

// classifyRefreshError names a token refresh failure without credentials.
// secrets are the connection's own token values, removed from any text the
// endpoint echoed back.
func classifyRefreshError(err error, secrets ...string) *refreshError {
	var retrieve *oauth2.RetrieveError
	if errors.As(err, &retrieve) {
		status := 0
		if retrieve.Response != nil {
			status = retrieve.Response.StatusCode
		}
		parts := []string{fmt.Sprintf("token endpoint answered %d", status)}
		if retrieve.ErrorCode != "" {
			parts = append(parts, retrieve.ErrorCode)
		}
		if retrieve.ErrorDescription != "" {
			parts = append(parts, retrieve.ErrorDescription)
		}
		cause := redactCause(strings.Join(parts, " "), refreshCauseLimit, secrets...)
		transient := status >= http.StatusInternalServerError || status == http.StatusTooManyRequests
		return &refreshError{transient: transient, cause: cause}
	}
	// No answer from the token endpoint at all: a network failure.
	cause := "token endpoint unreachable"
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		cause += ": " + urlErr.Err.Error()
	} else if err != nil {
		cause += ": " + err.Error()
	}
	return &refreshError{transient: true, cause: redactCause(cause, refreshCauseLimit, secrets...)}
}

// setStatus records a status change and when it happened; a connected
// connection has no last error.
func setStatus(c *record, status string) {
	if c.Status != status {
		c.Status = status
		c.StatusChangedAt = time.Now().UTC()
	}
	if status == statusConnected {
		c.LastError = ""
	}
}

// OnAuthorizationFailed registers fn to run once each time a connection
// enters authorization_failed (not on every failing call while it stays
// there), with the connection's name and the redacted cause.
func (s *Service) OnAuthorizationFailed(fn func(connection, cause string)) {
	s.onAuthorizationFailed = fn
}
