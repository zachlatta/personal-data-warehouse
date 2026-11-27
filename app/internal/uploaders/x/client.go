package x

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultBaseURL is x.com; tests point it at a fake.
	DefaultBaseURL = "https://x.com"
	// DefaultUserAgent is a desktop Chrome on macOS, which is what the
	// session's own browser is. X_USER_AGENT overrides it.
	DefaultUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
	// maxBody bounds one response read (a 40-post timeline page is ~1 MB).
	maxBody = 32 << 20
)

// Operation is one GraphQL query the web client knows: its rotating query
// id, and the feature switches and field toggles it sends with it.
type Operation struct {
	QueryID      string
	Features     []string
	FieldToggles []string
}

// fallbackOperations are the query ids and switches read out of the x.com web
// client on 2026-11-27. The live client's bundles are read first on every
// run (Discover) and win; these are only used for an operation the bundles
// did not name, so a rotated id is picked up without a release.
var fallbackOperations = map[string]Operation{
	"UserByScreenName":      {QueryID: "AMIBMjtxEEATh4z8V9GtRg", Features: userFeatures, FieldToggles: userFieldToggles},
	"UserTweetsAndReplies":  {QueryID: "FmuxExUC21Xn_C2NwtX5sQ", Features: timelineFeatures, FieldToggles: timelineFieldToggles},
	"Likes":                 {QueryID: "ZjyvI1V9gajq5eclHCObzA", Features: timelineFeatures, FieldToggles: timelineFieldToggles},
	"Bookmarks":             {QueryID: "fjf5b5Lu82lYMSBRhppK2g", Features: timelineFeatures, FieldToggles: timelineFieldToggles},
	"NotificationsTimeline": {QueryID: "XANkUc-HLIcCl8q85rGC2A", Features: timelineFeatures, FieldToggles: timelineFieldToggles},
}

// AuthError means x.com refused the session: the browser is signed out, or
// the cookie was revoked. Nothing else in the run can succeed.
type AuthError struct {
	Status int
	Detail string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("x.com rejected the browser session (HTTP %d%s); sign in to x.com in Chrome on this Mac and retry", e.Status, e.Detail)
}

// RateLimitError is HTTP 429: the stream stops for this run and resumes on the next.
type RateLimitError struct{ Reset time.Time }

func (e *RateLimitError) Error() string {
	if e.Reset.IsZero() {
		return "x.com rate limit reached"
	}
	return "x.com rate limit reached until " + e.Reset.UTC().Format(time.RFC3339)
}

// Client calls the x.com web client's GraphQL API with a captured session.
type Client struct {
	HTTP      *http.Client
	BaseURL   string
	UserAgent string
	Session   Session

	bearer     string
	operations map[string]Operation
	// lastRemaining is the x-rate-limit-remaining of the last response, or -1.
	lastRemaining int
}

// NewClient targets x.com with the session.
func NewClient(session Session, baseURL, userAgent string) *Client {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	if userAgent == "" {
		userAgent = DefaultUserAgent
	}
	return &Client{
		HTTP:          &http.Client{Timeout: 60 * time.Second},
		BaseURL:       strings.TrimRight(baseURL, "/"),
		UserAgent:     userAgent,
		Session:       session,
		lastRemaining: -1,
	}
}

var (
	clientScriptRe = regexp.MustCompile(`https?://[A-Za-z0-9.:-]+/responsive-web/client-web(?:-legacy)?/[A-Za-z0-9._~-]+\.js`)
	bearerRe       = regexp.MustCompile(`"Bearer (AAAAAAAAAAAAAAAAAAAAA[A-Za-z0-9%]+)"`)
	operationRe    = regexp.MustCompile(`queryId:"([^"]+)",operationName:"([^"]+)",operationType:"[a-z]+",metadata:\{featureSwitches:\[([^\]]*)\],fieldToggles:\[([^\]]*)\]`)
	quotedRe       = regexp.MustCompile(`"([^"]+)"`)
)

// wantedBundle says whether a client script can name an operation the
// uploader reads: the main bundle, and the lazily loaded Bookmarks and
// Notifications pages when the page names them.
func wantedBundle(scriptURL string) bool {
	name := scriptURL[strings.LastIndex(scriptURL, "/")+1:]
	return strings.HasPrefix(name, "main.") ||
		strings.HasPrefix(name, "bundle.Bookmarks.") ||
		strings.HasPrefix(name, "bundle.Notifications.")
}

// Discover reads the signed-in x.com page's client bundles for the API's
// bearer token and the current query ids. The bearer is the web client's own
// public app token (it ships in every page), never Zach's credential; the
// credential is the cookie.
func (c *Client) Discover() error {
	page, err := c.get(c.BaseURL+"/home", nil, false)
	if err != nil {
		return fmt.Errorf("could not load x.com: %w", err)
	}
	seen := map[string]bool{}
	var scripts []string
	for _, script := range clientScriptRe.FindAllString(string(page), -1) {
		// Only X's own CDN (or x.com itself): a page is never a reason to
		// fetch code from anywhere else.
		trusted := strings.HasPrefix(script, "https://abs.twimg.com/") || strings.HasPrefix(script, c.BaseURL+"/")
		if trusted && !seen[script] && wantedBundle(script) {
			seen[script] = true
			scripts = append(scripts, script)
		}
	}
	sort.Strings(scripts)
	operations := map[string]Operation{}
	for _, script := range scripts {
		body, err := c.get(script, nil, false)
		if err != nil {
			return fmt.Errorf("could not load the x.com client bundle %s: %w", script, err)
		}
		if c.bearer == "" {
			if m := bearerRe.FindSubmatch(body); m != nil {
				if bearer, err := url.QueryUnescape(string(m[1])); err == nil {
					c.bearer = bearer
				}
			}
		}
		for name, op := range ParseOperations(string(body)) {
			operations[name] = op
		}
	}
	if c.bearer == "" {
		return errors.New("the x.com page named no client bundle carrying the API token; X changed its web client")
	}
	for name, op := range fallbackOperations {
		if _, ok := operations[name]; !ok {
			operations[name] = op
		}
	}
	c.operations = operations
	return nil
}

// ParseOperations extracts every GraphQL operation a client bundle declares.
func ParseOperations(bundle string) map[string]Operation {
	found := map[string]Operation{}
	for _, m := range operationRe.FindAllStringSubmatch(bundle, -1) {
		found[m[2]] = Operation{QueryID: m[1], Features: quotedList(m[3]), FieldToggles: quotedList(m[4])}
	}
	return found
}

func quotedList(raw string) []string {
	var out []string
	for _, m := range quotedRe.FindAllStringSubmatch(raw, -1) {
		out = append(out, m[1])
	}
	return out
}

// Query runs one GraphQL query and returns its decoded `data`.
func (c *Client) Query(operation string, variables map[string]any) (map[string]any, error) {
	op, ok := c.operations[operation]
	if !ok {
		return nil, fmt.Errorf("unknown x.com operation %s (Discover first)", operation)
	}
	params := url.Values{}
	vars, err := json.Marshal(variables)
	if err != nil {
		return nil, err
	}
	params.Set("variables", string(vars))
	features := map[string]bool{}
	for _, f := range op.Features {
		features[f] = true
	}
	encoded, _ := json.Marshal(features)
	params.Set("features", string(encoded))
	if len(op.FieldToggles) > 0 {
		toggles := map[string]bool{}
		for _, f := range op.FieldToggles {
			toggles[f] = false
		}
		encoded, _ := json.Marshal(toggles)
		params.Set("fieldToggles", string(encoded))
	}
	target := c.BaseURL + "/i/api/graphql/" + url.PathEscape(op.QueryID) + "/" + operation + "?" + params.Encode()
	body, err := c.get(target, map[string]string{
		"Authorization":             "Bearer " + c.bearer,
		"x-csrf-token":              c.Session.CSRFToken(),
		"x-twitter-auth-type":       "OAuth2Session",
		"x-twitter-active-user":     "yes",
		"x-twitter-client-language": "en",
		"Content-Type":              "application/json",
		"Referer":                   c.BaseURL + "/",
	}, true)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	var decoded struct {
		Data   map[string]any `json:"data"`
		Errors []struct {
			Message string `json:"message"`
			Code    int    `json:"code"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("%s: undecodable response: %w", operation, err)
	}
	if len(decoded.Data) == 0 && len(decoded.Errors) > 0 {
		messages := make([]string, 0, len(decoded.Errors))
		for _, e := range decoded.Errors {
			messages = append(messages, e.Message)
		}
		return nil, fmt.Errorf("%s: %s", operation, strings.Join(messages, "; "))
	}
	return decoded.Data, nil
}

// RemainingRequests is the last response's x-rate-limit-remaining, or -1.
func (c *Client) RemainingRequests() int { return c.lastRemaining }

func (c *Client) get(target string, headers map[string]string, api bool) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	if strings.HasPrefix(target, c.BaseURL) {
		// The cookie goes to x.com only, never to the bundle CDN.
		req.Header.Set("Cookie", c.Session.CookieHeader())
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if api {
		c.lastRemaining = -1
		if v := resp.Header.Get("x-rate-limit-remaining"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				c.lastRemaining = n
			}
		}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		limit := &RateLimitError{}
		if v, err := strconv.ParseInt(resp.Header.Get("x-rate-limit-reset"), 10, 64); err == nil {
			limit.Reset = time.Unix(v, 0)
		}
		return nil, limit
	case api && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden):
		return nil, &AuthError{Status: resp.StatusCode, Detail: apiErrorDetail(body)}
	case resp.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("HTTP %d%s", resp.StatusCode, apiErrorDetail(body))
	}
	return body, nil
}

// apiErrorDetail is the API's own error message, never the request.
func apiErrorDetail(body []byte) string {
	var decoded struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if json.Unmarshal(body, &decoded) != nil || len(decoded.Errors) == 0 {
		return ""
	}
	return ": " + decoded.Errors[0].Message
}
