package server

import (
	"strings"
	"testing"
)

func TestADeadMCPConnectionAlertNamesTheConnectionAndCause(t *testing.T) {
	n := mcpAuthorizationFailedNotification("warehouse", "token endpoint answered 400 invalid_grant")
	if !strings.Contains(n.Title, "warehouse") || !strings.Contains(n.Body, "invalid_grant") || !strings.Contains(n.Body, "Connections") {
		t.Fatalf("notification = %+v", n)
	}
	// One alert per connection: a later one replaces it rather than stacking.
	if n.CollapseID != "mcp-authorization-warehouse" || n.ThreadID != "mcp-connections" {
		t.Fatalf("collapse/thread = %q/%q", n.CollapseID, n.ThreadID)
	}
}
