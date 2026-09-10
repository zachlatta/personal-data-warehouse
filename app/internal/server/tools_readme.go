package server

import (
	"context"
	"errors"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/zachlatta/personal-data-warehouse/app/internal/guide"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

// readmeTool is the MCP twin of `pdw readme`: the agent guide, rendered with
// MCP spellings. It is MCP-only because the CLI renders the same guide locally
// (no network, no token) and a second path to the same text through `pdw call`
// would be exactly the redundant route C9 forbids.
var readmeDescription = "READ THIS FIRST, before any other tool. The agent guide to this warehouse: the search-first workflow, the command map, the priority tiers, the SQL rules that prevent the recurring failures, where each domain lives, and what a negative result means. Takes no arguments for the main guide; pass topic for a domain deep-dive (" + readmeTopicList + "). Re-read a topic when a question enters that domain."

var readmeTopicList = joinTopicNames()

func joinTopicNames() string {
	names := guide.TopicNames(guide.SurfaceMCP)
	out := ""
	for i, name := range names {
		if i > 0 {
			out += ", "
		}
		out += name
	}
	return out
}

type readmeInput struct {
	Topic string `json:"topic,omitempty" jsonschema:"optional topic name for a deep-dive section; omit for the main guide; an unknown name errors listing the valid topics"`
}

// readmeOutput renders as one plain-text content block rather than a JSON
// envelope, because the whole value is prose meant to be read, not parsed.
type readmeOutput struct {
	Text  string `json:"text"`
	Error string `json:"error,omitempty"`
}

func (o readmeOutput) MCPCallToolResult(isError bool) *mcp.CallToolResult {
	text := o.Text
	if o.Error != "" {
		text = o.Error
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: text}},
		IsError: isError,
	}
}

func readmeTool() tool.Tool {
	return &tool.Typed[readmeInput, readmeOutput]{
		NameStr:        "readme",
		TitleStr:       "Agent Guide",
		DescriptionStr: readmeDescription,
		SurfacesField:  tool.SurfaceMCPOnly,
		Handle: func(_ context.Context, in readmeInput) (readmeOutput, error) {
			text, err := guide.Render(guide.SurfaceMCP, in.Topic)
			if err != nil {
				var unknown *guide.UnknownTopicError
				if errors.As(err, &unknown) {
					return readmeOutput{Error: err.Error()}, nil
				}
				return readmeOutput{}, err
			}
			return readmeOutput{Text: text}, nil
		},
		IsError: func(o readmeOutput) bool { return o.Error != "" },
	}
}
