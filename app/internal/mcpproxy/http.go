package mcpproxy

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"golang.org/x/oauth2"
)

type summary struct {
	Name          string   `json:"name"`
	URL           string   `json:"url"`
	Enabled       bool     `json:"enabled"`
	AllClients    bool     `json:"all_clients"`
	Clients       []string `json:"clients"`
	Status        string   `json:"status"`
	ToolCount     int      `json:"tool_count"`
	Version       int64    `json:"version"`
	Authenticated bool     `json:"authenticated"`
}

func summarize(c record) summary {
	return summary{Name: c.Name, URL: c.URL, Enabled: c.Enabled, AllClients: c.AllClients, Clients: c.Clients, Status: c.Status, ToolCount: len(c.Tools), Version: c.Version, Authenticated: c.Token.AccessToken != ""}
}
func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
func failure(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
func decode(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return errors.New("invalid request JSON")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("request must contain one JSON object")
	}
	return nil
}
func (s *Service) Register(mux *http.ServeMux, protect func(http.Handler) http.Handler) {
	mux.Handle("/api/connections", protect(http.HandlerFunc(s.connections)))
	mux.Handle("/api/connections/", protect(http.HandlerFunc(s.connection)))
	mux.HandleFunc(callbackPath, s.callback)
}
func (s *Service) connections(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		records, err := s.store.List(r.Context())
		if err != nil {
			failure(w, 503, "connection store unavailable")
			return
		}
		result := []summary{}
		for _, c := range records {
			result = append(result, summarize(c))
		}
		writeJSON(w, 200, map[string]any{"connections": result})
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		failure(w, 405, "method not allowed")
		return
	}
	var input struct {
		Name         string   `json:"name"`
		URL          string   `json:"url"`
		Token        string   `json:"token"`
		ClientID     string   `json:"client_id"`
		ClientSecret string   `json:"client_secret"`
		Scopes       []string `json:"scopes"`
		AllClients   bool     `json:"all_clients"`
		Clients      []string `json:"clients"`
	}
	if err := decode(w, r, &input); err != nil {
		failure(w, 400, err.Error())
		return
	}
	if !namePattern.MatchString(input.Name) || strings.Contains(input.Name, "__") {
		failure(w, 400, "name must start with a lowercase letter, end with a letter or digit, use only lowercase letters, digits or single underscores, and be at most 24 characters")
		return
	}
	if err := validateResourceURL(input.URL); err != nil {
		failure(w, 400, err.Error())
		return
	}
	for _, name := range input.Clients {
		if _, ok := pdwauth.ValidateClientName(name); !ok {
			failure(w, 400, "invalid client name")
			return
		}
	}
	var out summary
	err := s.store.Update(r.Context(), input.Name, func(c *record) error {
		if c.Version > 0 && !c.Deleted {
			return errors.New("a connection with this name already exists")
		}
		*c = record{Generation: randomSecret(), Name: input.Name, URL: input.URL, Enabled: true, AllClients: input.AllClients, Clients: input.Clients, Token: oauth2.Token{AccessToken: input.Token, TokenType: "Bearer"}, OAuth: oauth2.Config{ClientID: input.ClientID, ClientSecret: input.ClientSecret, Scopes: input.Scopes}, Status: "not_connected", Version: c.Version}
		out = summarize(*c)
		out.Version++
		return nil
	})
	if err != nil {
		failure(w, 409, "could not create connection; check for a duplicate name and database availability")
		return
	}
	writeJSON(w, 201, out)
}
func (s *Service) connection(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/connections/"), "/")
	if len(parts) != 2 || !namePattern.MatchString(parts[0]) {
		failure(w, 404, "connection action not found")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		failure(w, 405, "method not allowed")
		return
	}
	id, action := parts[0], parts[1]
	switch action {
	case "authorize":
		authURL, browser, err := s.BeginOAuth(r.Context(), id)
		if err != nil {
			failure(w, 400, err.Error())
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "pdw_mcp_" + id, Value: browser, Path: callbackPath, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: 600})
		writeJSON(w, 200, map[string]string{"authorization_url": authURL})
		return
	case "refresh":
		if err := s.Refresh(r.Context(), id); err != nil {
			failure(w, 502, "upstream discovery failed; authenticate and retry")
			return
		}
	case "access", "disable", "enable", "remove":
		var input struct {
			Version    int64    `json:"version"`
			AllClients bool     `json:"all_clients"`
			Clients    []string `json:"clients"`
		}
		if err := decode(w, r, &input); err != nil {
			failure(w, 400, err.Error())
			return
		}
		for _, client := range input.Clients {
			if _, ok := pdwauth.ValidateClientName(client); !ok {
				failure(w, 400, "invalid client name")
				return
			}
		}
		err := s.store.Update(r.Context(), id, func(c *record) error {
			if err := exists(c); err != nil {
				return err
			}
			if c.Version != input.Version {
				return errors.New("connection changed; reload and retry")
			}
			switch action {
			case "access":
				c.AllClients = input.AllClients
				c.Clients = input.Clients
			case "disable":
				c.Enabled = false
				c.Pending = nil
			case "enable":
				c.Enabled = true
			case "remove":
				*c = record{Name: id, Deleted: true, Version: c.Version}
			}
			return nil
		})
		if err != nil {
			failure(w, 409, "connection changed or is unavailable; reload and retry")
			return
		}
	default:
		failure(w, 404, "connection action not found")
		return
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
}
func (s *Service) callback(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if r.Method != http.MethodGet {
		failure(w, 405, "method not allowed")
		return
	}
	state := r.URL.Query().Get("state")
	id, _, _ := strings.Cut(state, ".")
	browser := ""
	if namePattern.MatchString(id) {
		if cookie, err := r.Cookie("pdw_mcp_" + id); err == nil {
			browser = cookie.Value
		}
	}
	id, err := s.CompleteOAuth(r.Context(), state, r.URL.Query().Get("code"), browser, r.URL.Query().Get("iss"))
	if err != nil {
		http.Redirect(w, r, "/connections?auth=failed", http.StatusSeeOther)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: "pdw_mcp_" + id, Path: callbackPath, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: -1})
	status := "connected"
	if err := s.Refresh(r.Context(), id); err != nil {
		status = "discovery_failed"
	}
	http.Redirect(w, r, "/connections?auth="+status, http.StatusSeeOther)
}
