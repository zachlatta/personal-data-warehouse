package feed

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

// EnabledEnv turns the module off when set to 0, false, off or no.
const EnabledEnv = "PDW_FEED_ENABLED"

// Options is everything the module needs from the app.
type Options struct {
	DatabaseURL string
	Timeout     time.Duration
	// Auth wraps the phone's routes; it is the app's static bearer.
	Auth   func(http.Handler) http.Handler
	Getenv func(string) string
	Logger *slog.Logger
}

// Enabled reads the feature flag. The module is on unless switched off.
func Enabled(getenv func(string) string) bool {
	if getenv == nil {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(getenv(EnabledEnv))) {
	case "0", "false", "off", "no":
		return false
	}
	return true
}

// Register mounts the phone's routes on mux and returns the agent tools for
// the shared registry. It is the module's only hook into the app: when the
// flag is off or there is no database it mounts nothing and returns nil.
func Register(mux *http.ServeMux, opts Options) []tool.Tool {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "feed")
	if !Enabled(opts.Getenv) {
		logger.Info("feed module disabled", "env", EnabledEnv)
		return nil
	}
	if opts.DatabaseURL == "" {
		logger.Info("feed module needs the application database; disabled")
		return nil
	}
	store, err := NewStore(opts.DatabaseURL, opts.Timeout)
	if err != nil {
		logger.Error("feed store failed to initialize; feed disabled", "error", err.Error())
		return nil
	}
	store.ResolveX = ResolveXPost
	(&handler{store: store, articles: NewArticleReader(), logger: logger}).register(mux, opts.Auth)
	logger.Info("feed module enabled", "api", APIPath)
	return Tools(store)
}
