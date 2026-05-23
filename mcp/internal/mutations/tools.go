package mutations

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const mutationToolDescription = "Create a pending upstream mutation request for human review in the Personal Data Warehouse. The tool does not execute the mutation; it returns a request_id and approval_url for the web review UI."

func RegisterTools(server *mcp.Server, service *Service) {
	if server == nil || service == nil {
		return
	}
	logger := slog.Default().With("component", "mutations")
	mcp.AddTool(server, &mcp.Tool{
		Name:        "propose_mutation_request",
		Title:       "Propose Mutation Request",
		Description: mutationToolDescription + " Use this when a task requires multiple Gmail or Google Contacts changes to be reviewed together.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ProposeMutationRequestInput) (*mcp.CallToolResult, any, error) {
		logger.InfoContext(ctx, "MCP mutation tool called", "tool", "propose_mutation_request", "mutations", len(input.Mutations))
		resp, err := service.ProposeMutationRequest(ctx, input)
		return mutationToolResult(resp, err), nil, nil
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "propose_gmail_archive_threads",
		Title:       "Propose Gmail Archive",
		Description: mutationToolDescription + " Proposes removing INBOX from one or more Gmail threads.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ProposeGmailArchiveThreadsInput) (*mcp.CallToolResult, any, error) {
		logger.InfoContext(ctx, "MCP mutation tool called", "tool", "propose_gmail_archive_threads", "threads", len(input.ThreadIDs))
		resp, err := service.ProposeGmailArchiveThreads(ctx, input)
		return mutationToolResult(resp, err), nil, nil
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "propose_gmail_unarchive_threads",
		Title:       "Propose Gmail Unarchive",
		Description: mutationToolDescription + " Proposes adding INBOX back to one or more Gmail threads.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ProposeGmailUnarchiveThreadsInput) (*mcp.CallToolResult, any, error) {
		logger.InfoContext(ctx, "MCP mutation tool called", "tool", "propose_gmail_unarchive_threads", "threads", len(input.ThreadIDs))
		resp, err := service.ProposeGmailUnarchiveThreads(ctx, input)
		return mutationToolResult(resp, err), nil, nil
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "propose_gmail_send_email",
		Title:       "Propose Gmail Email",
		Description: mutationToolDescription + " Proposes sending or drafting a Gmail email.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ProposeGmailSendEmailInput) (*mcp.CallToolResult, any, error) {
		logger.InfoContext(ctx, "MCP mutation tool called", "tool", "propose_gmail_send_email", "delivery_mode", input.DeliveryMode)
		resp, err := service.ProposeGmailSendEmail(ctx, input)
		return mutationToolResult(resp, err), nil, nil
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "propose_contact_mutations",
		Title:       "Propose Contact Mutations",
		Description: mutationToolDescription + " Proposes reviewed Google Contacts create, update, or delete operations.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ProposeContactMutationsInput) (*mcp.CallToolResult, any, error) {
		logger.InfoContext(ctx, "MCP mutation tool called", "tool", "propose_contact_mutations", "operations", len(input.Operations))
		resp, err := service.ProposeContactMutations(ctx, input)
		return mutationToolResult(resp, err), nil, nil
	})
}

func mutationToolResult(value any, err error) *mcp.CallToolResult {
	isError := err != nil
	if err != nil {
		value = map[string]string{"error": err.Error()}
	}
	data, marshalErr := json.MarshalIndent(value, "", "  ")
	if marshalErr != nil {
		data = []byte(`{"error":"failed to encode tool response"}`)
		isError = true
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
		IsError: isError,
	}
}
