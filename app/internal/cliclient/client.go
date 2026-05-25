// Package cliclient is a thin HTTP client for the personal data warehouse
// /api/tools surface. It mirrors the contract described in app/README.md:
// GET /api/tools to discover tools, POST /api/tools/{name} to invoke one.
// Auth is a single Bearer header of the form "<client_name>:<token>".
package cliclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client talks to the warehouse HTTP API.
type Client struct {
	baseURL    string
	clientName string
	token      string
	http       *http.Client
}

// ToolInfo mirrors one entry of GET /api/tools' data array.
type ToolInfo struct {
	Name        string          `json:"name"`
	Title       string          `json:"title"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// APIError carries the structured error envelope the server returns. When the
// body is not the expected JSON shape, Code is set to "http_error" and
// Message holds the response body verbatim.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("%s (http %d): %s", e.Code, e.Status, e.Message)
	}
	return fmt.Sprintf("http %d: %s", e.Status, e.Message)
}

// New returns a Client. baseURL must be an http(s) URL; clientName must be
// non-empty and contain no colon (the API uses ':' to split it from the
// token); token must be non-empty.
func New(baseURL, clientName, token string) (*Client, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, fmt.Errorf("base url is required")
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("base url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("base url scheme must be http or https, got %q", u.Scheme)
	}
	if strings.TrimSpace(clientName) == "" {
		return nil, fmt.Errorf("client name is required")
	}
	if strings.ContainsAny(clientName, ":\r\n\t") {
		return nil, fmt.Errorf("client name must not contain ':' or control characters")
	}
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("token is required")
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		clientName: clientName,
		token:      token,
		http:       &http.Client{Timeout: 5 * time.Minute},
	}, nil
}

// SetHTTPClient overrides the default *http.Client. Useful for tests and for
// callers that want a custom transport or timeout.
func (c *Client) SetHTTPClient(h *http.Client) {
	if h != nil {
		c.http = h
	}
}

// ListTools fetches GET /api/tools.
func (c *Client) ListTools(ctx context.Context) ([]ToolInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/tools", nil)
	if err != nil {
		return nil, err
	}
	c.authorize(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, decodeAPIError(resp.StatusCode, body)
	}
	var wrapper struct {
		Data []ToolInfo `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		return nil, fmt.Errorf("decode tool list: %w", err)
	}
	return wrapper.Data, nil
}

// CallTool invokes POST /api/tools/{name} with input as the request body and
// returns the raw "data" field of the response.
func (c *Client) CallTool(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("tool name is required")
	}
	if strings.ContainsAny(name, "/?#") {
		return nil, fmt.Errorf("tool name must not contain '/', '?', or '#'")
	}
	var body io.Reader
	if len(input) > 0 {
		body = bytes.NewReader(input)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/tools/"+name, body)
	if err != nil {
		return nil, err
	}
	if len(input) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	c.authorize(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, decodeAPIError(resp.StatusCode, raw)
	}
	var wrapper struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		return nil, fmt.Errorf("decode tool call response: %w", err)
	}
	return wrapper.Data, nil
}

func (c *Client) authorize(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+c.clientName+":"+c.token)
}

func decodeAPIError(status int, body []byte) error {
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Error.Code != "" {
		return &APIError{Status: status, Code: envelope.Error.Code, Message: envelope.Error.Message}
	}
	msg := strings.TrimSpace(string(body))
	if status == http.StatusUnauthorized {
		return &APIError{Status: status, Code: "unauthorized", Message: msg}
	}
	return &APIError{Status: status, Code: "http_error", Message: msg}
}
