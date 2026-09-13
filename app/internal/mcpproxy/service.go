package mcpproxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
	"golang.org/x/oauth2"
)

var namePattern = regexp.MustCompile(`^[a-z]([a-z0-9_]{0,22}[a-z0-9])?$`)
var toolPattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
var errUnavailable = errors.New("MCP connection is unavailable; check Connections in the PDW web interface")

type Service struct {
	store   Store
	baseURL string
	client  *http.Client
}

func New(store Store, baseURL string, client *http.Client) *Service {
	if client == nil {
		client = safeClient()
	}
	return &Service{store: store, baseURL: strings.TrimRight(baseURL, "/"), client: client}
}
func allowed(ctx context.Context, c *record) bool {
	return c.Enabled && !c.Deleted && (c.AllClients || slices.Contains(c.Clients, pdwauth.ClientNameFromContext(ctx)))
}
func exists(c *record) error {
	if c.Version == 0 || c.Deleted {
		return errors.New("connection not found")
	}
	return nil
}
func (s *Service) Tools(ctx context.Context) ([]tool.Tool, error) {
	records, err := s.store.List(ctx)
	if err != nil {
		return nil, err
	}
	result := []tool.Tool{}
	for _, c := range records {
		if !allowed(ctx, &c) {
			continue
		}
		for _, def := range c.Tools {
			result = append(result, &proxyTool{service: s, connection: c.Name, url: c.URL, generation: c.Generation, definition: def})
		}
	}
	return result, nil
}

type bearerTransport struct {
	base     http.RoundTripper
	token    string
	endpoint string
}

func (t bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.String() != t.endpoint {
		return nil, errors.New("upstream changed the credential destination")
	}
	copy := r.Clone(r.Context())
	copy.Header = r.Header.Clone()
	copy.Header.Del("Authorization")
	if t.token != "" {
		copy.Header.Set("Authorization", "Bearer "+t.token)
	}
	return t.base.RoundTrip(copy)
}
func (s *Service) authorizedClient(ctx context.Context, c *record) (*http.Client, error) {
	if c.OAuth.ClientID != "" {
		if c.Token.AccessToken == "" {
			return nil, errUnavailable
		}
		oauthClient := *s.client
		base := s.client.Transport
		if base == nil {
			base = http.DefaultTransport
		}
		oauthClient.Transport = resourceTransport{base: base, endpoint: c.OAuth.Endpoint.TokenURL, resource: c.Resource}
		oauthCtx := context.WithValue(ctx, oauth2.HTTPClient, &oauthClient)
		token, err := c.OAuth.TokenSource(oauthCtx, &c.Token).Token()
		if err != nil {
			return nil, errors.New("upstream authorization expired; reconnect in Connections")
		}
		c.Token = *token
	}
	base := s.client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	return &http.Client{Transport: bearerTransport{base: base, token: c.Token.AccessToken, endpoint: c.URL}, Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}

// prepare commits rotated credentials before an MCP session or call can time out.
// Only the token refresh holds the database lock, not the upstream tool call.
func (s *Service) prepare(ctx context.Context, id string, check func(*record) error) (record, *http.Client, error) {
	var prepared record
	var client *http.Client
	var authErr error
	err := s.store.Update(ctx, id, func(c *record) error {
		if err := exists(c); err != nil {
			return err
		}
		if check != nil {
			if err := check(c); err != nil {
				return err
			}
		}
		var err error
		client, err = s.authorizedClient(ctx, c)
		if err != nil {
			c.Status = "authorization_failed"
			authErr = err
			return nil
		}
		prepared = *c
		return nil
	})
	if err != nil {
		return record{}, nil, err
	}
	return prepared, client, authErr
}
func openSession(ctx context.Context, c record, client *http.Client) (*mcp.ClientSession, error) {
	session, err := mcp.NewClient(&mcp.Implementation{Name: "pdw-proxy", Version: "1"}, nil).Connect(ctx, &mcp.StreamableClientTransport{Endpoint: c.URL, HTTPClient: client, MaxRetries: -1, DisableStandaloneSSE: true}, nil)
	if err != nil {
		return nil, errors.New("could not connect to upstream MCP; authenticate or check its availability")
	}
	return session, nil
}
func (s *Service) status(ctx context.Context, original record, status string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	_ = s.store.Update(ctx, original.Name, func(c *record) error {
		if c.Deleted || c.Generation != original.Generation || c.URL != original.URL {
			return errUnavailable
		}
		c.Status = status
		return nil
	})
}
func validSchema(schema any) bool {
	raw, err := json.Marshal(schema)
	if err != nil {
		return false
	}
	var object map[string]any
	return json.Unmarshal(raw, &object) == nil && object["type"] == "object"
}

// Refresh replaces the complete snapshot, leaving the previous set on failure.
func (s *Service) Refresh(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	c, client, err := s.prepare(ctx, id, nil)
	if err != nil {
		return err
	}
	session, err := openSession(ctx, c, client)
	if err != nil {
		s.status(ctx, c, "connection_failed")
		return err
	}
	defer session.Close()
	defs := []*mcp.Tool{}
	seen := map[string]bool{}
	for def, err := range session.Tools(ctx, nil) {
		if err != nil {
			s.status(ctx, c, "discovery_failed")
			return errors.New("could not discover upstream tools")
		}
		if def == nil || !toolPattern.MatchString(def.Name) || len(id)+2+len(def.Name) > 128 || seen[def.Name] || len(defs) >= 1000 || !validSchema(def.InputSchema) || (def.OutputSchema != nil && !validSchema(def.OutputSchema)) {
			s.status(ctx, c, "discovery_failed")
			return errors.New("upstream tool definitions are invalid or exceed limits")
		}
		seen[def.Name] = true
		defs = append(defs, def)
	}
	return s.store.Update(ctx, id, func(current *record) error {
		if current.Deleted || current.Generation != c.Generation || current.URL != c.URL {
			return errUnavailable
		}
		current.Tools = defs
		current.Status = "connected"
		return nil
	})
}

type proxyTool struct {
	service    *Service
	connection string
	url        string
	generation string
	definition *mcp.Tool
}

func (t *proxyTool) Name() string           { return t.connection + "__" + t.definition.Name }
func (t *proxyTool) Title() string          { return t.definition.Title }
func (t *proxyTool) Description() string    { return "[" + t.connection + "] " + t.definition.Description }
func (t *proxyTool) Surfaces() tool.Surface { return tool.SurfaceAll }
func (t *proxyTool) LogOutput() bool        { return false }
func (t *proxyTool) RawInputSchema() any    { return t.definition.InputSchema }
func (t *proxyTool) InputSchema() (*jsonschema.Schema, error) {
	raw, err := json.Marshal(t.definition.InputSchema)
	if err != nil {
		return nil, err
	}
	var schema jsonschema.Schema
	err = json.Unmarshal(raw, &schema)
	return &schema, err
}
func (t *proxyTool) Invoke(ctx context.Context, input json.RawMessage) (any, bool, error) {
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(input, &object); err != nil || object == nil {
		return nil, true, &tool.InvalidInputError{Message: "tool arguments must be a JSON object"}
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	c, client, err := t.service.prepare(ctx, t.connection, func(c *record) error {
		if !allowed(ctx, c) || c.URL != t.url || c.Generation != t.generation {
			return errUnavailable
		}
		for _, def := range c.Tools {
			if def.Name == t.definition.Name {
				return nil
			}
		}
		return errUnavailable
	})
	if err != nil {
		return nil, true, err
	}
	session, err := openSession(ctx, c, client)
	if err != nil {
		t.service.status(ctx, c, "connection_failed")
		return nil, true, err
	}
	defer session.Close()
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: t.definition.Name, Arguments: input})
	if err != nil {
		t.service.status(ctx, c, "call_failed")
		return nil, true, errors.New("upstream MCP call failed; it was not retried, as a write may already have occurred")
	}
	t.service.status(ctx, c, "connected")
	if result == nil {
		return nil, true, errUnavailable
	}

	return result, result.IsError, nil
}
func (t *proxyTool) RegisterMCP(server *mcp.Server, hooks tool.Hooks) {
	def := *t.definition
	def.Name = t.Name()
	def.Description = t.Description()
	server.AddTool(&def, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if hooks.OnCall != nil {
			hooks.OnCall(ctx, t.Name())
		}
		out, isError, err := t.Invoke(ctx, req.Params.Arguments)
		if hooks.OnResult != nil {
			hooks.OnResult(ctx, t.Name(), "<not logged>", isError, err)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", t.Name(), err)
		}
		return out.(*mcp.CallToolResult), nil
	})
}

// Preserve the MCP resource audience when renewing a token, too. oauth2's
// built-in refresher otherwise omits RFC 8707 resource indicators.
type resourceTransport struct {
	base               http.RoundTripper
	endpoint, resource string
}

func (t resourceTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.String() != t.endpoint || t.resource == "" || r.Method != http.MethodPost {
		return t.base.RoundTrip(r)
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	r.Body.Close()
	if err != nil {
		return nil, err
	}
	values, err := url.ParseQuery(string(raw))
	if err != nil {
		return nil, err
	}
	values.Set("resource", t.resource)
	copy := r.Clone(r.Context())
	encoded := values.Encode()
	copy.Body = io.NopCloser(strings.NewReader(encoded))
	copy.ContentLength = int64(len(encoded))
	copy.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(encoded)), nil }
	return t.base.RoundTrip(copy)
}
