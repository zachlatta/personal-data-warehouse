package plaid

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// LinkResult is what Plaid Link handed back through the local page.
type LinkResult struct {
	PublicToken     string
	InstitutionID   string
	InstitutionName string
}

// LinkMode distinguishes a fresh Link from an update (consent repair) flow.
type LinkMode string

const (
	ModeLink   LinkMode = "link"
	ModeUpdate LinkMode = "update"
)

// LinkServer is the localhost callback server the Plaid Link page reports to
// (LocalPlaidLinkServer in cli.py). It serves the Link page on "/", accepts
// the outcome on POST /exchange?state=<state token>, and stops after the
// first outcome.
type LinkServer struct {
	Mode       LinkMode
	LinkToken  string
	ClientName string
	Host       string
	Port       int
	StateToken string

	mu       sync.Mutex
	result   *LinkResult
	errText  string
	done     chan struct{}
	started  chan struct{}
	once     sync.Once
	listener net.Listener
	server   *http.Server
}

// NewLinkServer prepares a server; Start binds it.
func NewLinkServer(mode LinkMode, linkToken, clientName, host string, port int) (*LinkServer, error) {
	if mode != ModeLink && mode != ModeUpdate {
		return nil, errors.New("unknown Plaid Link mode")
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return &LinkServer{
		Mode:       mode,
		LinkToken:  linkToken,
		ClientName: clientName,
		Host:       host,
		Port:       port,
		StateToken: randomToken(18),
		done:       make(chan struct{}),
		started:    make(chan struct{}),
	}, nil
}

func randomToken(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}

// Start binds the listener (port 0 picks a free port) and serves in the
// background.
func (s *LinkServer) Start() error {
	listener, err := net.Listen("tcp", net.JoinHostPort(s.Host, strconv.Itoa(s.Port)))
	if err != nil {
		return err
	}
	s.listener = listener
	if addr, ok := listener.Addr().(*net.TCPAddr); ok {
		s.Port = addr.Port
	}
	s.server = &http.Server{Handler: s.Handler()}
	go func() { _ = s.server.Serve(listener) }()
	close(s.started)
	return nil
}

// Started is closed once Start has bound the listener, so another goroutine
// can wait for URL and Port to be final.
func (s *LinkServer) Started() <-chan struct{} { return s.started }

// URL is the local page the browser opens.
func (s *LinkServer) URL() string {
	return fmt.Sprintf("http://%s:%d/", s.Host, s.Port)
}

// Result is the outcome recorded so far (nil until success).
func (s *LinkServer) Result() *LinkResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result
}

// Handler is the HTTP surface, exposed so tests can drive it without a port.
func (s *LinkServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			switch r.URL.Path {
			case "/":
				writeHTML(w, LinkPage(s.LinkToken, s.ClientName, s.StateToken))
			case "/done":
				writeHTML(w, "<html><body><h1>Plaid Link complete</h1><p>You can close this tab.</p></body></html>")
			default:
				http.Error(w, "404 Not Found", http.StatusNotFound)
			}
		case http.MethodPost:
			s.handleExchange(w, r)
		default:
			http.Error(w, "501 Unsupported method", http.StatusNotImplemented)
		}
	})
	return mux
}

func (s *LinkServer) handleExchange(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/exchange" {
		http.Error(w, "404 Not Found", http.StatusNotFound)
		return
	}
	if r.URL.Query().Get("state") != s.StateToken {
		http.Error(w, "403 Forbidden", http.StatusForbidden)
		return
	}
	raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	data := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &data); err != nil {
			http.Error(w, "400 Bad Request", http.StatusBadRequest)
			return
		}
	}
	publicToken := stringValue(data["public_token"])
	errorText := stringValue(data["error"])
	var failed bool
	if s.Mode == ModeUpdate {
		success, _ := data["success"].(bool)
		failed = errorText != "" || !success
	} else {
		failed = errorText != "" || publicToken == ""
	}
	if failed {
		var message string
		if s.Mode == ModeUpdate {
			message = "Plaid update canceled or failed; existing Item was kept."
		} else {
			if errorText == "" {
				errorText = "Plaid Link did not return a public token"
			}
			message = Redact(errorText, s.LinkToken)
		}
		s.mu.Lock()
		s.errText = message
		s.mu.Unlock()
		writeJSON(w, map[string]any{"ok": false, "error": message})
		s.finish()
		return
	}
	result := LinkResult{PublicToken: publicToken}
	if metadata, ok := data["metadata"].(map[string]any); ok {
		if institution, ok := metadata["institution"].(map[string]any); ok {
			result.InstitutionID = stringValue(institution["institution_id"])
			result.InstitutionName = stringValue(institution["name"])
		}
	}
	s.mu.Lock()
	s.result = &result
	s.mu.Unlock()
	writeJSON(w, map[string]any{"ok": true})
	s.finish()
}

func (s *LinkServer) finish() { s.once.Do(func() { close(s.done) }) }

// WaitForResult blocks until the page reports an outcome, stops the server,
// and returns the result or the surfaced error.
func (s *LinkServer) WaitForResult() (LinkResult, error) {
	if s.server == nil {
		return LinkResult{}, errors.New("Plaid Link server is not running")
	}
	<-s.done
	s.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.result != nil {
		return *s.result, nil
	}
	if s.errText != "" {
		return LinkResult{}, errors.New(s.errText)
	}
	return LinkResult{}, errors.New("Plaid Link did not complete")
}

// Close stops the server if it is running.
func (s *LinkServer) Close() {
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.server.Shutdown(ctx)
		s.server = nil
	}
}

func writeHTML(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, body)
}

func writeJSON(w http.ResponseWriter, payload map[string]any) {
	data, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// LinkPage renders the local Plaid Link page (the _link_page HTML in cli.py).
func LinkPage(linkToken, clientName, stateToken string) string {
	linkTokenJSON, _ := json.Marshal(linkToken)
	stateJSON, _ := json.Marshal(stateToken)
	clientNameHTML := html.EscapeString(clientName)
	return `<!doctype html>
<html>
<head><meta charset="utf-8"><title>` + clientNameHTML + ` Plaid Link</title></head>
<body>
  <h1>` + clientNameHTML + ` Plaid Link</h1>
  <p>Click the button below to open Plaid Link. Complete OAuth/MFA in the Plaid flow, then this local page will report completion to the CLI.</p>
  <button id="link">Open Plaid Link</button>
  <pre id="status"></pre>
  <script src="https://cdn.plaid.com/link/v2/stable/link-initialize.js"></script>
  <script>
    const status = document.getElementById('status');
    const config = {
      token: ` + string(linkTokenJSON) + `,
      onSuccess: async (public_token, metadata) => {
        status.textContent = 'Plaid Link completed; notifying local CLI...';
        const response = await fetch('/exchange?state=' + encodeURIComponent(` + string(stateJSON) + `), {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({success: true, public_token, metadata}),
        });
        const payload = await response.json();
        if (!payload.ok) { throw new Error(payload.error || 'exchange failed'); }
        window.location = '/done';
      },
      onExit: async (err, metadata) => {
        const message = err
          ? (err.error_message || err.error_code || 'Plaid Link exited with an error')
          : 'Plaid Link exited before an account was linked';
        status.textContent = message;
        await fetch('/exchange?state=' + encodeURIComponent(` + string(stateJSON) + `), {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({error: message}),
        });
      },
    };
    // Plaid OAuth redirects back with oauth_state_id. Re-initializing Link with
    // the same token and the exact received URI resumes the institution flow.
    if (new URLSearchParams(window.location.search).has('oauth_state_id')) {
      config.receivedRedirectUri = window.location.href;
    }
    const handler = Plaid.create(config);
    document.getElementById('link').onclick = () => handler.open();
    handler.open();
  </script>
</body>
</html>`
}
