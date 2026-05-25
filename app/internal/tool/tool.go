// Package tool defines the shared abstraction backing both the MCP and HTTP
// surfaces of the personal data warehouse app. A Tool carries the metadata
// adapters need to describe and invoke it; concrete tools are built per
// domain (query, mutations) and bundled in a Registry handed to each adapter.
package tool

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Tool is the type-erased view both adapters consume.
//
// The MCP adapter calls RegisterMCP to preserve generic SDK schema
// reflection on the underlying input type. The HTTP adapter calls Invoke
// with raw JSON; it does not need compile-time knowledge of the input type.
type Tool interface {
	Name() string
	Title() string
	Description() string

	// Invoke decodes rawInput into the tool's input type, runs the handler,
	// and returns the output value. isError is true when the call should be
	// surfaced to the caller as an error (decode failure, handler error, or
	// the tool's own IsError predicate firing on a successful return).
	Invoke(ctx context.Context, rawInput json.RawMessage) (output any, isError bool, err error)

	// RegisterMCP registers the tool against an MCP server using mcp.AddTool
	// so the SDK's schema reflection runs on the concrete input type. Hooks
	// lets the caller observe each call without the tool package depending
	// on a specific logger.
	RegisterMCP(server *mcp.Server, hooks Hooks)
}

// Hooks are optional callbacks adapters can supply. All fields may be nil.
type Hooks struct {
	// OnCall fires at the start of every tool invocation with the tool name.
	OnCall func(ctx context.Context, name string)
}

// MultiContentMarshaler lets a tool's output produce a custom MCP
// CallToolResult instead of the default single-JSON-text-content shape.
// schema_overview implements this to emit one TextContent per result.
//
// HTTP and other JSON-based adapters ignore this interface and JSON-encode
// the value directly.
type MultiContentMarshaler interface {
	MCPCallToolResult(isError bool) *mcp.CallToolResult
}

// Registry holds a deduplicated, registration-ordered set of tools.
// Nil entries in the input slices are ignored so domain Tools() helpers
// can return nil for disabled tools without callers having to filter.
type Registry struct {
	tools []Tool
	index map[string]Tool
}

// NewRegistry bundles one or more tool slices into a Registry. Duplicate
// names panic (programmer error — caught at startup, not request time).
func NewRegistry(toolSets ...[]Tool) *Registry {
	r := &Registry{index: map[string]Tool{}}
	for _, set := range toolSets {
		for _, t := range set {
			if t == nil {
				continue
			}
			name := t.Name()
			if _, exists := r.index[name]; exists {
				panic(fmt.Sprintf("tool: duplicate tool name %q", name))
			}
			r.index[name] = t
			r.tools = append(r.tools, t)
		}
	}
	return r
}

func (r *Registry) All() []Tool { return r.tools }

func (r *Registry) ByName(name string) Tool { return r.index[name] }
