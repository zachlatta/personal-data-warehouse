package server

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

func readOnlyTools(svc *query.Service) []tool.Tool {
	return []tool.Tool{
		queryTool(svc),
		searchTool(svc),
		schemaOverviewTool(svc),
		describeTableTool(svc),
		sqlTool(svc),
	}
}

func queryTool(svc *query.Service) tool.Tool {
	return &tool.Typed[queryInput, query.FullQueryBatchResponse]{
		NameStr:        "query",
		TitleStr:       "Query Postgres",
		DescriptionStr: queryDescription,
		SurfacesField:  tool.SurfaceMCPOnly,
		Handle: func(ctx context.Context, in queryInput) (query.FullQueryBatchResponse, error) {
			return svc.ExecuteBatchFull(ctx, queryStatementsFromInput(in.Queries), in.Format), nil
		},
		IsError:               queryResponseHasError,
		NormalizeMCPArguments: normalizeStringifiedQueriesArgument,
	}
}

func normalizeStringifiedQueriesArgument(input json.RawMessage) (json.RawMessage, error) {
	if len(input) == 0 {
		return input, nil
	}
	var args map[string]json.RawMessage
	if err := json.Unmarshal(input, &args); err != nil {
		return input, nil
	}
	rawQueries, ok := args["queries"]
	if !ok {
		return input, nil
	}
	var stringifiedQueries string
	if err := json.Unmarshal(rawQueries, &stringifiedQueries); err != nil {
		return input, nil
	}
	var decodedQueries json.RawMessage
	if err := json.Unmarshal([]byte(strings.TrimSpace(stringifiedQueries)), &decodedQueries); err != nil {
		return input, nil
	}
	args["queries"] = decodedQueries
	normalized, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	return normalized, nil
}

// searchTool is registered on both MCP and the HTTP API. MCP agents call it
// directly; the first-class `pdw search` command uses the same HTTP tool, so
// both surfaces share one retrieval implementation and response contract.
func searchTool(svc *query.Service) tool.Tool {
	return &tool.Typed[searchInput, query.SearchResponse]{
		NameStr:        "search",
		TitleStr:       "Search Timeline",
		DescriptionStr: searchDescription,
		Handle: func(ctx context.Context, in searchInput) (query.SearchResponse, error) {
			return svc.Search(ctx, query.SearchRequest{
				Query:      in.Query,
				MaxResults: in.MaxResults,
				Sources:    in.Sources,
				Since:      in.Since,
				Mode:       in.Mode,
				Priorities: in.Priorities,
			}), nil
		},
		IsError: func(r query.SearchResponse) bool { return r.Error != "" },
	}
}

func sqlTool(svc *query.Service) tool.Tool {
	return &tool.Typed[sqlInput, query.FullQueryResponse]{
		NameStr:        "sql",
		TitleStr:       "Run SQL",
		DescriptionStr: sqlDescription,
		SurfacesField:  tool.SurfaceCLIOnly,
		Handle: func(ctx context.Context, in sqlInput) (query.FullQueryResponse, error) {
			return svc.ExecuteFull(ctx, in.Question, in.SQL, in.Format), nil
		},
		IsError: func(r query.FullQueryResponse) bool { return r.Error != "" },
	}
}

func schemaOverviewTool(svc *query.Service) tool.Tool {
	return &tool.Typed[schemaOverviewInput, schemaOverviewOutput]{
		NameStr:        "schema_overview",
		TitleStr:       "Schema Overview",
		DescriptionStr: schemaOverviewDescription,
		Handle: func(ctx context.Context, _ schemaOverviewInput) (schemaOverviewOutput, error) {
			return schemaOverviewOutput{Response: svc.SchemaOverview(ctx)}, nil
		},
		IsError: func(o schemaOverviewOutput) bool { return o.hasError() },
	}
}

// describeTableTool is registered on every surface, not MCP-only: the CLI is
// where most warehouse SQL is written, and per-table discovery existing only as
// a client-side subcommand is why it went almost unused.
func describeTableTool(svc *query.Service) tool.Tool {
	return &tool.Typed[describeTableInput, schemaOverviewOutput]{
		NameStr:        "describe_table",
		TitleStr:       "Describe Table",
		DescriptionStr: describeTableDescription,
		Handle: func(ctx context.Context, in describeTableInput) (schemaOverviewOutput, error) {
			return schemaOverviewOutput{Response: svc.DescribeTable(ctx, in.Relation)}, nil
		},
		IsError: func(o schemaOverviewOutput) bool { return o.hasError() },
	}
}

// schemaOverviewOutput wraps query.Response so it can implement
// tool.MultiContentMarshaler without query depending on the MCP SDK. Both
// catalog tools return the same shape, so both use it.
type schemaOverviewOutput struct {
	query.Response
}

func (o schemaOverviewOutput) hasError() bool {
	for _, r := range o.Results {
		if r.Error != "" {
			return true
		}
	}
	return false
}

func (o schemaOverviewOutput) MCPCallToolResult(isError bool) *mcp.CallToolResult {
	content := make([]mcp.Content, 0, len(o.Results))
	for _, result := range o.Results {
		content = append(content, &mcp.TextContent{Text: result.CSV})
		if !result.Truncated.Empty() {
			content = append(content, &mcp.TextContent{Text: result.Truncated.CSV()})
		}
	}
	return &mcp.CallToolResult{
		Content: content,
		IsError: isError,
	}
}
