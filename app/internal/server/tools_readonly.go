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
		getRowsTool(svc),
		getFieldTool(svc),
		grepRowsTool(svc),
		schemaOverviewTool(svc),
		sqlTool(svc),
	}
}

func queryTool(svc *query.Service) tool.Tool {
	return &tool.Typed[queryInput, query.QueryResponse]{
		NameStr:        "query",
		TitleStr:       "Query Postgres",
		DescriptionStr: queryDescription,
		SurfacesField:  tool.SurfaceMCPOnly,
		Handle: func(ctx context.Context, in queryInput) (query.QueryResponse, error) {
			return svc.Execute(ctx, queryStatementsFromInput(in.Queries), in.PreviewRows, in.Format), nil
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

func getRowsTool(svc *query.Service) tool.Tool {
	return &tool.Typed[getRowsInput, query.RowsResponse]{
		NameStr:        "get_rows",
		TitleStr:       "Get Cached Rows",
		DescriptionStr: getRowsDescription,
		SurfacesField:  tool.SurfaceMCPOnly,
		Handle: func(ctx context.Context, in getRowsInput) (query.RowsResponse, error) {
			return svc.GetRows(ctx, in.QueryID, in.Offset, in.Limit, in.Format), nil
		},
		IsError: func(r query.RowsResponse) bool { return r.Error != "" },
	}
}

func getFieldTool(svc *query.Service) tool.Tool {
	return &tool.Typed[getFieldInput, query.FieldResponse]{
		NameStr:        "get_field",
		TitleStr:       "Get Cached Field",
		DescriptionStr: getFieldDescription,
		SurfacesField:  tool.SurfaceMCPOnly,
		Handle: func(ctx context.Context, in getFieldInput) (query.FieldResponse, error) {
			return svc.GetField(ctx, in.QueryID, in.Row, in.Column, in.Offset, in.Length), nil
		},
		IsError: func(r query.FieldResponse) bool { return r.Error != "" },
	}
}

func grepRowsTool(svc *query.Service) tool.Tool {
	return &tool.Typed[grepRowsInput, query.GrepResponse]{
		NameStr:        "grep_rows",
		TitleStr:       "Grep Cached Rows",
		DescriptionStr: grepRowsDescription,
		SurfacesField:  tool.SurfaceMCPOnly,
		Handle: func(ctx context.Context, in grepRowsInput) (query.GrepResponse, error) {
			return svc.GrepRows(ctx, in.QueryID, in.Pattern, in.Columns, in.Limit, in.ContextChars), nil
		},
		IsError: func(r query.GrepResponse) bool { return r.Error != "" },
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

// schemaOverviewOutput wraps query.Response so it can implement
// tool.MultiContentMarshaler without query depending on the MCP SDK.
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
