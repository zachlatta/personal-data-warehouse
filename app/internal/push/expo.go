package push

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// DefaultExpoEndpoint is Expo's push API.
const DefaultExpoEndpoint = "https://exp.host/--/api/v2/push/send"

// expoBatchSize is the documented maximum messages per request.
const expoBatchSize = 100

// Message is one notification for one device.
type Message struct {
	To       string         `json:"to"`
	Title    string         `json:"title,omitempty"`
	Body     string         `json:"body,omitempty"`
	Data     map[string]any `json:"data,omitempty"`
	Sound    string         `json:"sound,omitempty"`
	Priority string         `json:"priority,omitempty"`
	// ChannelID is Android-only; harmless on iOS.
	ChannelID string `json:"channelId,omitempty"`
}

// Ticket is Expo's per-message acknowledgement. A ticket with Status "error"
// and Details.Error == "DeviceNotRegistered" means the token is dead.
type Ticket struct {
	Status  string `json:"status"`
	ID      string `json:"id,omitempty"`
	Message string `json:"message,omitempty"`
	Details struct {
		Error string `json:"error,omitempty"`
	} `json:"details,omitempty"`
}

// DeviceNotRegistered is the Expo error that retires a token.
const DeviceNotRegistered = "DeviceNotRegistered"

// ExpoClient posts messages to the Expo push API.
type ExpoClient struct {
	Endpoint    string
	AccessToken string
	HTTP        *http.Client
}

func NewExpoClient(accessToken string) *ExpoClient {
	return &ExpoClient{Endpoint: DefaultExpoEndpoint, AccessToken: accessToken, HTTP: &http.Client{Timeout: 20 * time.Second}}
}

type expoResponse struct {
	Data   []Ticket `json:"data"`
	Errors []struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
}

// Send delivers messages in batches and returns one ticket per message, in
// order. A transport or top-level API failure is returned as an error; a
// per-message failure is a ticket with Status "error".
func (c *ExpoClient) Send(ctx context.Context, messages []Message) ([]Ticket, error) {
	if c == nil {
		return nil, errors.New("expo push client is not configured")
	}
	tickets := make([]Ticket, 0, len(messages))
	for start := 0; start < len(messages); start += expoBatchSize {
		end := min(start+expoBatchSize, len(messages))
		batch, err := c.sendBatch(ctx, messages[start:end])
		if err != nil {
			return tickets, err
		}
		tickets = append(tickets, batch...)
	}
	return tickets, nil
}

func (c *ExpoClient) sendBatch(ctx context.Context, messages []Message) ([]Ticket, error) {
	body, err := json.Marshal(messages)
	if err != nil {
		return nil, err
	}
	endpoint := c.Endpoint
	if endpoint == "" {
		endpoint = DefaultExpoEndpoint
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if token := strings.TrimSpace(c.AccessToken); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("expo push request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("expo push response: %w", err)
	}
	var parsed expoResponse
	if jerr := json.Unmarshal(raw, &parsed); jerr != nil {
		return nil, fmt.Errorf("expo push returned HTTP %d with a non-JSON body", resp.StatusCode)
	}
	if len(parsed.Errors) > 0 {
		return nil, fmt.Errorf("expo push rejected the request: %s: %s", parsed.Errors[0].Code, parsed.Errors[0].Message)
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("expo push returned HTTP %d", resp.StatusCode)
	}
	if len(parsed.Data) != len(messages) {
		return nil, fmt.Errorf("expo push returned %d tickets for %d messages", len(parsed.Data), len(messages))
	}
	return parsed.Data, nil
}
