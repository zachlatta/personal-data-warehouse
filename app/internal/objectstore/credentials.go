package objectstore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"golang.org/x/oauth2"
)

// DriveScope is the OAuth scope the Drive backend needs. Matches the Python
// GOOGLE_DRIVE_SCOPE.
const DriveScope = "https://www.googleapis.com/auth/drive"

const defaultGoogleTokenURL = "https://oauth2.googleapis.com/token"

// authorizedUserToken is the Python-compatible OAuth token JSON written by the
// Python google_auth flow (an "authorized user" credential). The JSON also
// carries a short-lived access token (token/access_token), deliberately not
// parsed here — see HTTPClientFromAuthorizedUserJSON.
type authorizedUserToken struct {
	RefreshToken string `json:"refresh_token"`
	TokenURI     string `json:"token_uri"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

// HTTPClientFromAuthorizedUserJSON builds an auto-refreshing *http.Client from
// the same authorized-user token JSON the Python side persists, so the Go app
// authenticates to Drive with the identical credential. Token refresh happens at
// request time; construction needs no network.
func HTTPClientFromAuthorizedUserJSON(ctx context.Context, tokenJSON string) (*http.Client, error) {
	var parsed authorizedUserToken
	if err := json.Unmarshal([]byte(tokenJSON), &parsed); err != nil {
		return nil, fmt.Errorf("objectstore: invalid Google token JSON: %w", err)
	}
	if parsed.RefreshToken == "" {
		return nil, fmt.Errorf("objectstore: Google token JSON is missing refresh_token")
	}
	if parsed.ClientID == "" || parsed.ClientSecret == "" {
		return nil, fmt.Errorf("objectstore: Google token JSON is missing client_id/client_secret")
	}
	tokenURL := parsed.TokenURI
	if tokenURL == "" {
		tokenURL = defaultGoogleTokenURL
	}
	config := &oauth2.Config{
		ClientID:     parsed.ClientID,
		ClientSecret: parsed.ClientSecret,
		Endpoint:     oauth2.Endpoint{TokenURL: tokenURL},
		Scopes:       []string{DriveScope},
	}
	// Ignore any stored access token: a Token without Expiry never refreshes
	// (oauth2 treats zero Expiry as non-expiring), so a stale access token
	// would be replayed forever. Leaving it empty forces a refresh-token
	// exchange on first use, after which oauth2 caches the fresh token.
	token := &oauth2.Token{RefreshToken: parsed.RefreshToken}
	return config.Client(ctx, token), nil
}
