package plaid

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// APIError is the Go twin of PlaidAPIError: a failed Plaid HTTP request. Its
// message is "<CODE>: <message>" when Plaid itself produced it.
type APIError struct {
	Message string
}

func (e *APIError) Error() string { return e.Message }

// ErrorCode extracts the leading Plaid error code from a "<CODE>: <message>"
// string (plaid_error_code in plaid_sync.py). Only messages Plaid produced
// carry a code; anything else returns "".
func ErrorCode(message string) string {
	code, rest, ok := strings.Cut(message, ":")
	if !ok {
		_ = rest
		return ""
	}
	code = strings.TrimSpace(code)
	if code == "" || strings.ToUpper(code) != code || strings.ToLower(code) == code {
		return ""
	}
	return code
}

// Redact replaces every non-empty credential in message with "[redacted]".
func Redact(message string, credentials ...string) string {
	for _, credential := range credentials {
		if credential != "" {
			message = strings.ReplaceAll(message, credential, "[redacted]")
		}
	}
	return message
}

// API is the subset of Plaid the CLI calls, behind an interface so commands
// are testable with a fake.
type API interface {
	CreateLinkToken(account, accessToken string) (map[string]any, error)
	ExchangePublicToken(publicToken string) (map[string]any, error)
	AccountsGet(accessToken string) (map[string]any, error)
	ItemRemove(accessToken string) (map[string]any, error)
}

// Client is the HTTP Plaid client (PlaidClient in plaid_sync.py).
type Client struct {
	cfg  Config
	http *http.Client
	base string
}

// NewClient builds a client for cfg. httpClient may be nil.
func NewClient(cfg Config, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: time.Duration(cfg.RequestTimeoutSeconds) * time.Second}
	}
	return &Client{cfg: cfg, http: httpClient, base: strings.TrimRight(cfg.EffectiveBaseURL(), "/")}
}

// BaseURL is the resolved Plaid host.
func (c *Client) BaseURL() string { return c.base }

// ClientUserID is the opaque, PII-free user id sent to Plaid for an account
// label: Plaid forbids an email address in client_user_id.
func ClientUserID(account string) string {
	digest := sha256.Sum256([]byte(account))
	return "pdw-" + hex.EncodeToString(digest[:])
}

// CreateLinkToken requests a Link token. With accessToken set it is an update
// (consent repair) token for that existing Item and initializes no new
// products; otherwise it is a fresh Link for the configured products.
func (c *Client) CreateLinkToken(account, accessToken string) (map[string]any, error) {
	if account == "" {
		account = c.cfg.Account
	}
	payload := map[string]any{
		"client_name":   c.cfg.ClientName,
		"user":          map[string]any{"client_user_id": ClientUserID(account)},
		"country_codes": append([]string{}, c.cfg.CountryCodes...),
		"language":      c.cfg.Language,
	}
	if accessToken != "" {
		if strings.TrimSpace(accessToken) == "" {
			return nil, errors.New("an existing Item access token is required for update mode")
		}
		payload["access_token"] = accessToken
		payload["update"] = map[string]any{"account_selection_enabled": true}
	} else {
		configured := append([]string{}, c.cfg.Products...)
		required := configured
		if contains(configured, "transactions") {
			required = []string{"transactions"}
		}
		payload["products"] = required
		var additional []string
		for _, product := range configured {
			if !contains(required, product) {
				additional = append(additional, product)
			}
		}
		if len(additional) > 0 {
			payload["additional_consented_products"] = additional
		}
		if contains(c.cfg.Products, "transactions") {
			payload["transactions"] = map[string]any{"days_requested": c.cfg.TransactionsLookbackDay}
		}
	}
	if c.cfg.RedirectURI != "" {
		payload["redirect_uri"] = c.cfg.RedirectURI
	}
	if c.cfg.Webhook != "" {
		payload["webhook"] = c.cfg.Webhook
	}
	return c.post("/link/token/create", payload)
}

// ExchangePublicToken trades a Link public token for an access token + item id.
func (c *Client) ExchangePublicToken(publicToken string) (map[string]any, error) {
	return c.post("/item/public_token/exchange", map[string]any{"public_token": publicToken})
}

// AccountsGet lists the accounts an Item currently reports.
func (c *Client) AccountsGet(accessToken string) (map[string]any, error) {
	return c.post("/accounts/get", map[string]any{"access_token": accessToken})
}

// ItemRemove revokes an Item at Plaid: the undo for Link and the only write
// this integration makes.
func (c *Client) ItemRemove(accessToken string) (map[string]any, error) {
	return c.post("/item/remove", map[string]any{"access_token": accessToken})
}

// ItemGet reads an Item's metadata.
func (c *Client) ItemGet(accessToken string) (map[string]any, error) {
	return c.post("/item/get", map[string]any{"access_token": accessToken})
}

func (c *Client) post(endpoint string, payload map[string]any) (map[string]any, error) {
	body := map[string]any{"client_id": c.cfg.ClientID, "secret": c.cfg.Secret}
	for key, value := range payload {
		body[key] = value
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequest(http.MethodPost, c.base+endpoint, bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return nil, &APIError{Message: Redact(err.Error(), c.cfg.Secret, c.cfg.ClientID)}
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, &APIError{Message: Redact(err.Error(), c.cfg.Secret, c.cfg.ClientID)}
	}
	var data any
	decodeErr := json.Unmarshal(raw, &data)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if decodeErr == nil {
			if object, ok := data.(map[string]any); ok {
				return nil, &APIError{Message: errorFromJSON(object)}
			}
		}
		return nil, &APIError{Message: fmt.Sprintf("Plaid HTTP %d", response.StatusCode)}
	}
	if decodeErr != nil {
		return nil, &APIError{Message: fmt.Sprintf("Plaid %s returned non-JSON response", endpoint)}
	}
	object, ok := data.(map[string]any)
	if !ok {
		return nil, &APIError{Message: fmt.Sprintf("Plaid %s returned unexpected response shape", endpoint)}
	}
	if code := stringValue(object["error_code"]); code != "" {
		return nil, &APIError{Message: errorFromJSON(object)}
	}
	return object, nil
}

func errorFromJSON(data map[string]any) string {
	code := stringValue(data["error_code"])
	if code == "" {
		code = "PLAID_ERROR"
	}
	message := stringValue(data["error_message"])
	if message == "" {
		message = stringValue(data["display_message"])
	}
	if message == "" {
		message = "Plaid request failed"
	}
	return code + ": " + message
}

func stringValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case bool:
		if v {
			return "True"
		}
		return ""
	case float64:
		if v == 0 {
			return ""
		}
		return fmt.Sprint(v)
	default:
		return fmt.Sprint(v)
	}
}

func contains(values []string, needle string) bool {
	for _, v := range values {
		if v == needle {
			return true
		}
	}
	return false
}
