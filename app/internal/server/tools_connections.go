package server

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

// Connected upstream MCP servers used to be flattened into the warehouse's
// own tools/list, one entry per upstream tool. Measured on 2026-09-22 that was
// 196 tools and 153 KB of definitions -- 142 KB of them proxied, ~90 from one
// Kubernetes PaaS -- paid by every MCP session before its first question, and
// the sessions that pay it most (claude.ai, Claude Desktop, a Claude Code
// session with the connector attached) call a proxied tool almost never.
//
// So the list now carries two tools instead: `connections` to discover a
// connection's tools (names and summaries, or one tool's full schema) and
// `connection_call` to invoke one. The CLI is unchanged (`pdw list` /
// `pdw call` already were this shape), and PDW_MCP_LIST_CONNECTION_TOOLS=1
// restores the flat listing for a client that needs it.

const connectionToolsEnv = "PDW_MCP_LIST_CONNECTION_TOOLS"

func listConnectionToolsFlat() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(connectionToolsEnv))) {
	case "1", "true", "yes":
		return true
	}
	return false
}

// isConnectionTool reports whether a registry entry is a proxied upstream
// tool, which the proxy names <connection>__<tool>.
func isConnectionTool(t tool.Tool) bool {
	return strings.Contains(t.Name(), "__")
}

type connectionsInput struct {
	Connection string `json:"connection,omitempty" jsonschema:"one connection's name to list its tools with summaries; omit to list every connection"`
	Tool       string `json:"tool,omitempty" jsonschema:"one tool's full name (<connection>__<tool>) to return its description and input schema"`
}

type connectionToolSummary struct {
	Name    string `json:"name"`
	Summary string `json:"summary"`
}

type connectionSummary struct {
	Name  string                  `json:"name"`
	Tools []connectionToolSummary `json:"tools,omitempty"`
	Count int                     `json:"tool_count"`
}

type connectionsOutput struct {
	Connections []connectionSummary `json:"connections,omitempty"`
	Tool        *connectionToolDoc  `json:"tool,omitempty"`
	Next        string              `json:"next,omitempty"`
	Error       string              `json:"error,omitempty"`
}

type connectionToolDoc struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	InputSchema any    `json:"input_schema"`
}

type connectionCallInput struct {
	Tool      string         `json:"tool" jsonschema:"full tool name, <connection>__<tool>, from connections"`
	Arguments map[string]any `json:"arguments,omitempty" jsonschema:"the tool's arguments, shaped by its input_schema from connections"`
}

// connectionCallOutput passes the upstream CallToolResult through unchanged:
// content blocks, structured content and the error flag are the upstream's.
type connectionCallOutput struct {
	Result *mcp.CallToolResult `json:"result"`
}

func (o connectionCallOutput) MCPCallToolResult(isError bool) *mcp.CallToolResult {
	if o.Result == nil {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "no result"}}, IsError: true}
	}
	result := *o.Result
	result.IsError = result.IsError || isError
	return &result
}

const connectionsDescription = "List the connected upstream MCP servers (skills, tasks, other warehouses, deployment platforms) and their tools. These are live upstream calls with the owner's account, not warehouse queries, and they may write. With no arguments: every connection with its tool count. With connection: that connection's tools with one-line summaries. With tool (<connection>__<tool>): the tool's description and input schema -- read it before the first connection_call."

const connectionCallDescription = "Invoke one connected upstream MCP tool by its full name (<connection>__<tool>) with arguments shaped by the input_schema that connections returns. The upstream's own result is returned as-is (content, structured content, error flag). Writes execute upstream with the owner's account and are not retried; check the schema first rather than guessing argument names."

func connectionTools(proxied []tool.Tool) []tool.Tool {
	byName := map[string]tool.Tool{}
	byConnection := map[string][]tool.Tool{}
	var connections []string
	for _, t := range proxied {
		byName[t.Name()] = t
		conn, _, _ := strings.Cut(t.Name(), "__")
		if _, seen := byConnection[conn]; !seen {
			connections = append(connections, conn)
		}
		byConnection[conn] = append(byConnection[conn], t)
	}
	sort.Strings(connections)
	list := &tool.Typed[connectionsInput, connectionsOutput]{
		NameStr:        "connections",
		TitleStr:       "Connected MCP Servers",
		DescriptionStr: connectionsDescription,
		SurfacesField:  tool.SurfaceMCPOnly,
		IsError:        func(o connectionsOutput) bool { return o.Error != "" },
		Handle: func(_ context.Context, in connectionsInput) (connectionsOutput, error) {
			if name := strings.TrimSpace(in.Tool); name != "" {
				t, ok := byName[name]
				if !ok {
					return connectionsOutput{Error: "no connected tool " + name + "; connections are " + strings.Join(connections, ", ") + " (call connections with a connection name to list its tools)"}, nil
				}
				var schema any
				if raw, ok := t.(interface{ RawInputSchema() any }); ok {
					schema = raw.RawInputSchema()
				} else if s, err := t.InputSchema(); err == nil {
					schema = s
				}
				return connectionsOutput{Tool: &connectionToolDoc{Name: t.Name(), Description: t.Description(), InputSchema: schema}, Next: "connection_call with tool and arguments"}, nil
			}
			if conn := strings.TrimSpace(in.Connection); conn != "" {
				tools, ok := byConnection[conn]
				if !ok {
					return connectionsOutput{Error: "no connection " + conn + "; connections are " + strings.Join(connections, ", ")}, nil
				}
				summary := connectionSummary{Name: conn, Count: len(tools)}
				for _, t := range tools {
					summary.Tools = append(summary.Tools, connectionToolSummary{Name: t.Name(), Summary: firstSentence(strings.TrimPrefix(t.Description(), "["+conn+"] "))})
				}
				return connectionsOutput{Connections: []connectionSummary{summary}, Next: "connections with tool: <name> for its input schema, then connection_call"}, nil
			}
			out := connectionsOutput{Next: "connections with connection: <name> to list its tools"}
			for _, conn := range connections {
				out.Connections = append(out.Connections, connectionSummary{Name: conn, Count: len(byConnection[conn])})
			}
			return out, nil
		},
	}
	call := &tool.Typed[connectionCallInput, connectionCallOutput]{
		NameStr:        "connection_call",
		TitleStr:       "Call Connected MCP Tool",
		DescriptionStr: connectionCallDescription,
		SurfacesField:  tool.SurfaceMCPOnly,
		IsError:        func(o connectionCallOutput) bool { return o.Result != nil && o.Result.IsError },
		Handle: func(ctx context.Context, in connectionCallInput) (connectionCallOutput, error) {
			name := strings.TrimSpace(in.Tool)
			t, ok := byName[name]
			if !ok {
				return connectionCallOutput{}, &tool.InvalidInputError{Message: "no connected tool " + name + "; call connections to list them"}
			}
			args := in.Arguments
			if args == nil {
				args = map[string]any{}
			}
			raw, err := json.Marshal(args)
			if err != nil {
				return connectionCallOutput{}, err
			}
			out, isError, err := t.Invoke(ctx, raw)
			if err != nil {
				return connectionCallOutput{}, err
			}
			result, ok := out.(*mcp.CallToolResult)
			if !ok {
				encoded, _ := json.Marshal(out)
				result = &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(encoded)}}}
			}
			if isError {
				result.IsError = true
			}
			return connectionCallOutput{Result: result}, nil
		},
	}
	return []tool.Tool{list, call}
}

func firstSentence(s string) string {
	s = strings.TrimSpace(s)
	if idx := strings.Index(s, ". "); idx > 0 && idx < 160 {
		return s[:idx+1]
	}
	if len(s) > 160 {
		return s[:160] + "…"
	}
	return s
}
