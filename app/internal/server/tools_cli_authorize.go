package server

import (
	"context"
	"fmt"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/accesstokens"
	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

const cliAuthorizeToolName = "cli_authorize"

const cliAuthorizeDescription = "Get a one-line shell command that installs and logs in the pdw CLI in the environment you are running in (a sandbox such as Claude Cowork, a fresh VM or container), so you can use `pdw search`, `pdw sql` and the rest from a shell instead of these MCP tools. " +
	"Run the returned command once, in that shell, within 15 minutes; it works one time only. The CLI it sets up holds a token that reaches only the warehouse tools (the same tools as this MCP, including propose_mutation, never mutation review) and stops working after lifetime_hours (default 24, max 168). " +
	"Do not paste the command anywhere else: until it is run it is a credential."

type cliAuthorizeInput struct {
	ClientName    string `json:"client_name,omitempty" jsonschema:"name the CLI reports on every request and in the token list, e.g. cowork or a sandbox name; default cli"`
	LifetimeHours int    `json:"lifetime_hours,omitempty" jsonschema:"how long the CLI's token lives once redeemed, 1-168 hours; default 24. Use stops it there however busy the CLI is"`
}

type cliAuthorizeOutput struct {
	Command       string    `json:"command"`
	CodeExpiresAt time.Time `json:"code_expires_at"`
	LifetimeHours int       `json:"token_lifetime_hours"`
	ClientName    string    `json:"client_name"`
	Scope         string    `json:"scope"`
	Next          string    `json:"next"`
}

// cliAuthorizeTool is MCP-only: the point is to get a CLI into an environment
// that has only MCP. Its result is a credential, so mcpToolHooks redacts it.
func cliAuthorizeTool(tokens *accesstokens.Service, baseURL string) tool.Tool {
	return &tool.Typed[cliAuthorizeInput, cliAuthorizeOutput]{
		NameStr:        cliAuthorizeToolName,
		TitleStr:       "Authorize a pdw CLI here",
		DescriptionStr: cliAuthorizeDescription,
		SurfacesField:  tool.SurfaceMCPOnly,
		Handle: func(ctx context.Context, in cliAuthorizeInput) (cliAuthorizeOutput, error) {
			name := in.ClientName
			if name == "" {
				name = "cli"
			}
			hours := in.LifetimeHours
			if hours == 0 {
				hours = int(accesstokens.DefaultAgentTTL / time.Hour)
			}
			maxHours := int(accesstokens.MaxAgentTTL / time.Hour)
			if hours < 1 || hours > maxHours {
				return cliAuthorizeOutput{}, &tool.InvalidInputError{Message: fmt.Sprintf("lifetime_hours must be between 1 and %d", maxHours)}
			}
			code, err := tokens.CreateBootstrap(ctx, accesstokens.BootstrapRequest{
				ClientName: name,
				TokenTTL:   time.Duration(hours) * time.Hour,
				CreatedBy:  pdwauth.ClientNameFromContext(ctx),
			})
			if err != nil {
				return cliAuthorizeOutput{}, &tool.InvalidInputError{Message: err.Error()}
			}
			return cliAuthorizeOutput{
				Command:       accesstokens.BootstrapCommand(baseURL, code.Code),
				CodeExpiresAt: code.ExpiresAt.UTC(),
				LifetimeHours: hours,
				ClientName:    name,
				Scope:         string(pdwauth.ScopeAgent),
				Next:          "Run the command in your shell now (it needs curl and network access to this app and github.com). Then `pdw` (the brief guide) and `pdw search ...` work there. When the token expires, call cli_authorize again.",
			}, nil
		},
	}
}
