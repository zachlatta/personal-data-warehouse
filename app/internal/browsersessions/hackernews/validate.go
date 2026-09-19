package hackernews

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// DefaultSiteBaseURL is news.ycombinator.com.
	DefaultSiteBaseURL = "https://news.ycombinator.com"
	// UserAgent is the same descriptive UA the server-side poller sends.
	UserAgent = "personal-data-warehouse hacker_news_sync (+https://github.com/zachlatta/personal-data-warehouse)"
	// CheckList is the login-only list a cookie is checked against.
	CheckList = "upvoted"
	// loginPageHead bounds how much of a page the login-form check reads,
	// as the poller's parser does.
	loginPageHead = 2000
)

// Validator checks a cookie header against a login-only HN page.
type Validator struct {
	BaseURL string
	HTTP    *http.Client
}

// NewValidator targets the real news.ycombinator.com.
func NewValidator() *Validator {
	return &Validator{BaseURL: DefaultSiteBaseURL, HTTP: &http.Client{Timeout: 30 * time.Second}}
}

// LooksLikeLoginPage recognises HN answering the login form instead of a
// list: the poller's own test, applied to the head of the page.
func LooksLikeLoginPage(body string) bool {
	head := body
	if len(head) > loginPageHead {
		head = head[:loginPageHead]
	}
	return strings.Contains(head, "Please log in") ||
		(strings.Contains(head, `<form method="post"`) && strings.Contains(head, "acct") && strings.Contains(head, "pw"))
}

// CheckLogin fetches the first page of the user's login-only list with the
// cookie. False means HN answered the login page (the cookie is dead); an
// error means the check could not be made (throttled, non-200, network).
func (v *Validator) CheckLogin(cookieHeader, userID string) (bool, error) {
	httpClient := v.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	target := strings.TrimRight(v.BaseURL, "/") + "/" + CheckList + "?" + url.Values{"id": {userID}}.Encode()
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Cookie", cookieHeader)
	req.Header.Set("User-Agent", UserAgent)
	resp, err := httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("/%s request failed: %w", CheckList, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
		return false, fmt.Errorf("HTTP %d from %s: news.ycombinator.com is throttling; retry later", resp.StatusCode, target)
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("/%s: HTTP %d", CheckList, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return false, err
	}
	return !LooksLikeLoginPage(string(body)), nil
}
