package server

import "github.com/zachlatta/personal-data-warehouse/app/internal/push"

// mcpAuthorizationFailedNotification is the one alert sent when a proxied MCP
// connection enters authorization_failed (its provider refused the refresh
// grant). Until 2026-11-23 a dead connection showed only in agent
// transcripts; it is also a row in marts_ops.mcp_connection_health.
func mcpAuthorizationFailedNotification(connection, cause string) push.Notification {
	return push.Notification{
		Title:      "MCP connection " + connection + " needs reconnecting",
		Body:       cause + ". Reconnect it in Connections on the PDW web interface.",
		ThreadID:   "mcp-connections",
		CollapseID: "mcp-authorization-" + connection,
	}
}
