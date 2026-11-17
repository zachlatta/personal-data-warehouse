package mcpproxy

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"strings"
)

var causeURL = regexp.MustCompile(`https?://[^\s<>"']+`)
var causeToken = regexp.MustCompile(`(?i)bearer\s+[^\s,"']+|pdw_[a-z0-9]{12,24}_[a-z0-9_-]{30,}|[a-z0-9_+/=-]{30,}`)

func upstreamFailure(ctx context.Context, connection, tool string, readOnly bool, secret string, err error) error {
	cause := err.Error()
	if secret != "" {
		cause = strings.ReplaceAll(cause, secret, "[REDACTED]")
	}
	cause = causeURL.ReplaceAllStringFunc(cause, func(raw string) string {
		u, e := url.Parse(raw)
		if e != nil {
			return "[URL]"
		}
		u.RawQuery = ""
		u.ForceQuery = false
		u.Fragment = ""
		u.User = nil
		return u.String()
	})
	cause = causeToken.ReplaceAllString(cause, "[REDACTED]")
	if len(cause) > 500 {
		cause = cause[:500] + "…"
	}
	slog.WarnContext(ctx, "upstream MCP call failed", "connection", connection, "tool", tool, "error", cause)
	retry := "it was not retried, as a write may already have occurred"
	if readOnly {
		retry = "this read-only call was not retried"
	}
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("the %s connection did not answer within %s, PDW's budget for one live upstream call; %s. A long read query will not finish on a rerun: narrow it (tighter filters, LIMIT, fewer joins) or split it; cause: %s", connection, upstreamCallBudget, retry, cause)
	}
	return fmt.Errorf("upstream MCP call failed; %s; cause: %s", retry, cause)
}
