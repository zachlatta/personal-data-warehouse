package mcpproxy

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"
	"golang.org/x/oauth2"
)

const callbackPath = "/connections/oauth/callback"

func randomSecret() string {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}
func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
func (s *Service) discover(ctx context.Context, endpoint string) (*oauthex.ProtectedResourceMetadata, *oauthex.AuthServerMeta, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Accept", "application/json, text/event-stream")
	response, err := s.client.Do(req)
	if err != nil {
		return nil, nil, errors.New("MCP endpoint could not be reached")
	}
	response.Body.Close()
	candidates := []string{}
	challenges, _ := oauthex.ParseWWWAuthenticate(response.Header.Values("WWW-Authenticate"))
	for _, challenge := range challenges {
		if challenge.Scheme == "bearer" && challenge.Params["resource_metadata"] != "" {
			candidates = append(candidates, challenge.Params["resource_metadata"])
		}
	}
	u, _ := url.Parse(endpoint)
	origin := u.Scheme + "://" + u.Host
	candidates = append(candidates, origin+"/.well-known/oauth-protected-resource"+strings.TrimRight(u.Path, "/"), origin+"/.well-known/oauth-protected-resource")
	var resource *oauthex.ProtectedResourceMetadata
	for _, candidate := range candidates {
		if validateURL(candidate) != nil {
			continue
		}
		metadata, err := oauthex.GetProtectedResourceMetadata(ctx, candidate, endpoint, s.client)
		if err == nil && len(metadata.AuthorizationServers) > 0 {
			resource = metadata
			break
		}
	}
	if resource == nil {
		return nil, nil, errors.New("no matching OAuth protected-resource metadata; use a bearer token if this server does not support OAuth")
	}
	issuer := resource.AuthorizationServers[0]
	if err := validateURL(issuer); err != nil {
		return nil, nil, err
	}
	metadata, err := mcpauth.GetAuthServerMetadata(ctx, issuer, s.client)
	if err != nil || metadata == nil {
		return nil, nil, errors.New("OAuth server metadata could not be verified")
	}
	if !slices.Contains(metadata.CodeChallengeMethodsSupported, "S256") {
		return nil, nil, errors.New("upstream OAuth must support PKCE S256")
	}
	for _, raw := range []string{metadata.AuthorizationEndpoint, metadata.TokenEndpoint} {
		if err := validateURL(raw); err != nil {
			return nil, nil, err
		}
	}
	return resource, metadata, nil
}
func (s *Service) BeginOAuth(ctx context.Context, id string) (authURL, browser string, err error) {
	if err = validateResourceURL(s.baseURL); err != nil {
		return "", "", errors.New("MCP_BASE_URL must be HTTPS for browser OAuth")
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	browser = randomSecret()
	err = s.store.Update(ctx, id, func(c *record) error {
		if err := exists(c); err != nil {
			return err
		}
		resource, metadata, err := s.discover(ctx, c.URL)
		if err != nil {
			return err
		}
		cfg := c.OAuth
		// A registration is bound to one issuer. Never send its secret elsewhere.
		if c.Issuer != "" && c.Issuer != metadata.Issuer {
			return errors.New("upstream issuer changed; remove and recreate this connection")
		}
		cfg.RedirectURL = s.baseURL + callbackPath
		cfg.Endpoint = oauth2.Endpoint{AuthURL: metadata.AuthorizationEndpoint, TokenURL: metadata.TokenEndpoint, AuthStyle: oauth2.AuthStyleInParams}
		if len(cfg.Scopes) == 0 {
			cfg.Scopes = resource.ScopesSupported
		}
		if cfg.ClientID == "" {
			if validateURL(metadata.RegistrationEndpoint) != nil {
				return errors.New("upstream requires a pre-registered OAuth client ID; enter it when adding the connection")
			}
			registration, err := oauthex.RegisterClient(ctx, metadata.RegistrationEndpoint, &oauthex.ClientRegistrationMetadata{ClientName: "PDW MCP gateway", RedirectURIs: []string{cfg.RedirectURL}, TokenEndpointAuthMethod: "none", GrantTypes: []string{"authorization_code", "refresh_token"}, ResponseTypes: []string{"code"}}, s.client)
			if err != nil || registration.ClientID == "" {
				return errors.New("upstream OAuth client registration failed")
			}
			cfg.ClientID = registration.ClientID
			cfg.ClientSecret = registration.ClientSecret
			switch registration.TokenEndpointAuthMethod {
			case "none", "client_secret_post":
			case "client_secret_basic", "":
				if cfg.ClientSecret != "" {
					cfg.Endpoint.AuthStyle = oauth2.AuthStyleInHeader
				}
			default:
				return errors.New("unsupported upstream OAuth client authentication method")
			}
		} else if cfg.ClientSecret != "" && (len(metadata.TokenEndpointAuthMethodsSupported) == 0 || slices.Contains(metadata.TokenEndpointAuthMethodsSupported, "client_secret_basic")) {
			cfg.Endpoint.AuthStyle = oauth2.AuthStyleInHeader
		}
		pending := &pendingAuth{State: id + "." + randomSecret(), Browser: browser, Verifier: oauth2.GenerateVerifier(), Expires: time.Now().Add(10 * time.Minute)}
		c.OAuth = cfg
		c.Resource = resource.Resource
		c.Issuer = metadata.Issuer
		c.Pending = pending
		c.Status = "awaiting_authorization"
		authURL = cfg.AuthCodeURL(pending.State, oauth2.S256ChallengeOption(pending.Verifier), oauth2.SetAuthURLParam("resource", resource.Resource))
		return nil
	})
	return authURL, browser, err
}
func (s *Service) CompleteOAuth(ctx context.Context, state, code, browser, issuer string) (string, error) {
	id, _, ok := strings.Cut(state, ".")
	if !ok || !namePattern.MatchString(id) {
		return "", errors.New("invalid OAuth state")
	}
	var exchangeErr error
	err := s.store.Update(ctx, id, func(c *record) error {
		p := c.Pending
		if c.Deleted || p == nil || time.Now().After(p.Expires) || subtle.ConstantTimeCompare([]byte(p.State), []byte(state)) != 1 || browser == "" || subtle.ConstantTimeCompare([]byte(p.Browser), []byte(browser)) != 1 {
			return errors.New("OAuth state expired or does not belong to this browser; reconnect")
		}
		if issuer != "" && issuer != c.Issuer {
			return errors.New("OAuth issuer mismatch")
		}
		c.Pending = nil // Consume even if the provider rejects the exchange.
		if code == "" {
			c.Status = "authorization_denied"
			exchangeErr = errors.New("upstream authorization was denied")
			return nil
		}
		oauthCtx := context.WithValue(ctx, oauth2.HTTPClient, s.client)
		token, err := c.OAuth.Exchange(oauthCtx, code, oauth2.VerifierOption(p.Verifier), oauth2.SetAuthURLParam("resource", c.Resource))
		if err != nil || token.AccessToken == "" {
			c.Status = "authorization_failed"
			exchangeErr = errors.New("upstream token exchange failed; reconnect")
			return nil
		}
		if token.Type() != "Bearer" {
			c.Status = "authorization_failed"
			exchangeErr = errors.New("only bearer upstream tokens are supported")
			return nil
		}
		c.Token = *token
		c.Status = "authorized"
		return nil
	})
	if err != nil {
		return "", err
	}
	if exchangeErr != nil {
		return "", exchangeErr
	}
	return id, nil
}
