package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
)

const tokenUsage = `usage: pdw token <list|create|revoke>

Issued access tokens sit beside the master secret (PDW_SECRET_TOKEN); see
docs/agents/access-tokens.md. Managing them needs the master secret: an
issued token cannot list, mint or revoke tokens.

  pdw token list [--output text|json]
  pdw token create --name CLIENT [--label TEXT] [--idle 30d] [--ttl DURATION] [--scope full|agent]
        Print a new token once. It dies after --idle without use (default 30d)
        and, with --ttl, at a hard expiry. Use it with "pdw login --token" or
        paste it into the iOS app or a web page.
  pdw token revoke ID
`

const tokensAPIPath = "/api/auth/tokens"

func runToken(client *cliclient.Client, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, tokenUsage)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("token list", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		output := fs.String("output", "text", "text or json")
		if err := fs.Parse(args[1:]); err != nil || fs.NArg() > 0 {
			fmt.Fprint(stderr, tokenUsage)
			return 2
		}
		raw, err := client.Request(ctx, http.MethodGet, tokensAPIPath, nil)
		if err != nil {
			return tokenError(stderr, err)
		}
		if *output == "json" {
			_, _ = stdout.Write(raw)
			return 0
		}
		return printTokens(raw, stdout, stderr)
	case "create":
		fs := flag.NewFlagSet("token create", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		name := fs.String("name", "", "client name the token is for")
		label := fs.String("label", "", "free-text note")
		idle := fs.String("idle", "", "idle timeout, e.g. 30d")
		ttl := fs.String("ttl", "", "hard expiry, e.g. 90d")
		scope := fs.String("scope", "", "full (default) or agent")
		if err := fs.Parse(args[1:]); err != nil || fs.NArg() > 0 || strings.TrimSpace(*name) == "" {
			fmt.Fprint(stderr, tokenUsage)
			return 2
		}
		raw, err := client.Request(ctx, http.MethodPost, tokensAPIPath, map[string]string{
			"client_name": *name, "label": *label, "idle_timeout": *idle, "ttl": *ttl, "scope": *scope,
		})
		if err != nil {
			return tokenError(stderr, err)
		}
		var created struct {
			Token string    `json:"token"`
			Info  tokenView `json:"info"`
		}
		if err := json.Unmarshal(raw, &created); err != nil {
			fmt.Fprintln(stderr, "pdw token create: decode response:", err)
			return 1
		}
		fmt.Fprintf(stdout, "Issued %s for %s (scope %s, idle limit %s%s).\n", created.Info.ID, created.Info.ClientName, created.Info.Scope, created.Info.IdleTimeout, expirySuffix(created.Info.ExpiresAt))
		fmt.Fprintln(stdout, "This is the only time the token is shown:")
		fmt.Fprintln(stdout, created.Token)
		return 0
	case "revoke":
		if len(args) != 2 || strings.TrimSpace(args[1]) == "" {
			fmt.Fprint(stderr, tokenUsage)
			return 2
		}
		if _, err := client.Request(ctx, http.MethodDelete, tokensAPIPath+"/"+url.PathEscape(strings.TrimSpace(args[1])), nil); err != nil {
			return tokenError(stderr, err)
		}
		fmt.Fprintf(stdout, "Revoked %s.\n", args[1])
		return 0
	default:
		fmt.Fprintf(stderr, "pdw token: unknown subcommand %q\n", args[0])
		fmt.Fprint(stderr, tokenUsage)
		return 2
	}
}

type tokenView struct {
	ID            string     `json:"id"`
	Kind          string     `json:"kind"`
	Scope         string     `json:"scope"`
	Status        string     `json:"status"`
	ClientName    string     `json:"client_name"`
	Label         string     `json:"label"`
	CreatedBy     string     `json:"created_by"`
	CreatedAt     time.Time  `json:"created_at"`
	LastUsedAt    time.Time  `json:"last_used_at"`
	IdleTimeout   string     `json:"idle_timeout"`
	IdleExpiresAt *time.Time `json:"idle_expires_at"`
	ExpiresAt     *time.Time `json:"expires_at"`
}

func expirySuffix(at *time.Time) string {
	if at == nil {
		return ""
	}
	return ", expires " + at.UTC().Format(time.RFC3339)
}

func printTokens(raw []byte, stdout, stderr io.Writer) int {
	var listed struct {
		Tokens []tokenView `json:"tokens"`
	}
	if err := json.Unmarshal(raw, &listed); err != nil {
		fmt.Fprintln(stderr, "pdw token list: decode response:", err)
		return 1
	}
	tw := tabwriter.NewWriter(stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tKIND\tSCOPE\tSTATUS\tCLIENT\tLAST USED\tDIES\tCREATED BY\tLABEL")
	for _, t := range listed.Tokens {
		dies := "-"
		switch {
		case t.ExpiresAt != nil && (t.IdleExpiresAt == nil || !t.ExpiresAt.After(*t.IdleExpiresAt)):
			dies = t.ExpiresAt.UTC().Format(time.RFC3339)
		case t.IdleExpiresAt != nil:
			dies = t.IdleExpiresAt.UTC().Format(time.RFC3339) + " (idle)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", t.ID, t.Kind, t.Scope, t.Status, t.ClientName,
			t.LastUsedAt.UTC().Format(time.RFC3339), dies, t.CreatedBy, t.Label)
	}
	_ = tw.Flush()
	return 0
}

func tokenError(stderr io.Writer, err error) int {
	var apiErr *cliclient.APIError
	if errors.As(err, &apiErr) {
		fmt.Fprintf(stderr, "pdw token: http %d: %s\n", apiErr.Status, apiErr.Message)
		return 1
	}
	fmt.Fprintln(stderr, "pdw token:", err)
	return 1
}
