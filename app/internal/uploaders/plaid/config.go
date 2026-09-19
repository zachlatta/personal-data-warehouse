// Package plaid is the Go port of `pdw ingest plaid`: it links, repairs, lists
// and retires Plaid Items against the warehouse Postgres directly, mirroring
// the Python personal_data_warehouse_plaid.cli it replaces.
//
// The `sync` subcommand is deliberately not ported: syncing is the Dagster
// `plaid_finance_sync` asset's job, every thirty minutes in production, and
// the CLI copy of it only ever duplicated that code path.
package plaid

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const (
	DefaultEnvironment             = "development"
	DefaultClientName              = "Personal Data Warehouse"
	DefaultRequestTimeoutSeconds   = 30
	DefaultTransactionsLookbackDay = 730
	MaxTransactionsLookbackDays    = 730
)

var (
	DefaultProducts     = []string{"transactions", "investments", "liabilities"}
	DefaultCountryCodes = []string{"US"}
	Environments        = []string{"sandbox", "development", "production"}
	SupportedProducts   = []string{"transactions", "investments", "liabilities"}

	// EnvBaseURLs mirrors PLAID_ENV_BASE_URLS in plaid_sync.py.
	EnvBaseURLs = map[string]string{
		"sandbox":     "https://sandbox.plaid.com",
		"development": "https://development.plaid.com",
		"production":  "https://production.plaid.com",
	}
)

// Config is the Go twin of PlaidConfig in config.py.
type Config struct {
	Account                 string
	ClientID                string
	Secret                  string
	Environment             string
	Products                []string
	CountryCodes            []string
	ClientName              string
	Language                string
	RedirectURI             string
	Webhook                 string
	BaseURL                 string
	RequestTimeoutSeconds   int
	TransactionsLookbackDay int

	// DatabaseURL is POSTGRES_DATABASE_URL, the warehouse the Item tokens
	// live in.
	DatabaseURL string
}

// EffectiveBaseURL is PLAID_BASE_URL when set, else the environment's host.
func (c Config) EffectiveBaseURL() string {
	if c.BaseURL != "" {
		return strings.TrimRight(c.BaseURL, "/")
	}
	return EnvBaseURLs[c.Environment]
}

// LoadConfig reads the Plaid settings exactly as load_settings(require_plaid=True)
// does, including its validation messages.
func LoadConfig(getenv func(string) string) (Config, error) {
	env := common.Getenv(getenv)
	cfg := Config{
		Account:  env.PlaidAccount(),
		ClientID: strings.TrimSpace(getenv("PLAID_CLIENT_ID")),
		Secret:   strings.TrimSpace(getenv("PLAID_SECRET")),
	}
	var missing []string
	if cfg.Account == "" {
		missing = append(missing, "PLAID_ACCOUNT")
	}
	if cfg.ClientID == "" {
		missing = append(missing, "PLAID_CLIENT_ID")
	}
	if cfg.Secret == "" {
		missing = append(missing, "PLAID_SECRET")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("%s must be set for Plaid finance sync", strings.Join(missing, ", "))
	}
	cfg.Environment = strings.ToLower(strings.TrimSpace(getenv("PLAID_ENV")))
	if cfg.Environment == "" {
		cfg.Environment = DefaultEnvironment
	}
	if _, ok := EnvBaseURLs[cfg.Environment]; !ok {
		return Config{}, fmt.Errorf("PLAID_ENV must be one of: %s", strings.Join(Environments, ", "))
	}
	cfg.Products = lowerAll(env.CSV("PLAID_PRODUCTS"))
	if len(cfg.Products) == 0 {
		cfg.Products = append([]string(nil), DefaultProducts...)
	}
	if unsupported := unsupportedProducts(cfg.Products); len(unsupported) > 0 {
		return Config{}, fmt.Errorf(
			"PLAID_PRODUCTS currently supports read-only products: %s (unsupported: %s)",
			strings.Join(SupportedProducts, ", "), strings.Join(unsupported, ", "),
		)
	}
	cfg.CountryCodes = upperAll(env.CSV("PLAID_COUNTRY_CODES"))
	if len(cfg.CountryCodes) == 0 {
		cfg.CountryCodes = append([]string(nil), DefaultCountryCodes...)
	}
	timeout, err := intEnv(getenv, "PLAID_REQUEST_TIMEOUT_SECONDS", DefaultRequestTimeoutSeconds)
	if err != nil {
		return Config{}, err
	}
	if timeout < 1 {
		return Config{}, fmt.Errorf("PLAID_REQUEST_TIMEOUT_SECONDS must be at least 1")
	}
	cfg.RequestTimeoutSeconds = timeout
	lookback, err := intEnv(getenv, "PLAID_TRANSACTIONS_LOOKBACK_DAYS", DefaultTransactionsLookbackDay)
	if err != nil {
		return Config{}, err
	}
	if lookback < 1 {
		return Config{}, fmt.Errorf("PLAID_TRANSACTIONS_LOOKBACK_DAYS must be at least 1")
	}
	if lookback > MaxTransactionsLookbackDays {
		return Config{}, fmt.Errorf("PLAID_TRANSACTIONS_LOOKBACK_DAYS must be at most %d", MaxTransactionsLookbackDays)
	}
	cfg.TransactionsLookbackDay = lookback
	cfg.ClientName = strings.TrimSpace(getenv("PLAID_CLIENT_NAME"))
	if cfg.ClientName == "" {
		cfg.ClientName = DefaultClientName
	}
	cfg.Language = strings.TrimSpace(getenv("PLAID_LANGUAGE"))
	if cfg.Language == "" {
		cfg.Language = "en"
	}
	cfg.RedirectURI = strings.TrimSpace(getenv("PLAID_REDIRECT_URI"))
	cfg.Webhook = strings.TrimSpace(getenv("PLAID_WEBHOOK"))
	cfg.BaseURL = strings.TrimRight(strings.TrimSpace(getenv("PLAID_BASE_URL")), "/")
	cfg.DatabaseURL = strings.TrimSpace(getenv("POSTGRES_DATABASE_URL"))
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("POSTGRES_DATABASE_URL must be set")
	}
	return cfg, nil
}

func intEnv(getenv func(string) string, name string, fallback int) (int, error) {
	raw := strings.TrimSpace(getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	return value, nil
}

func lowerAll(values []string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, strings.ToLower(v))
	}
	return out
}

func upperAll(values []string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, strings.ToUpper(v))
	}
	return out
}

func unsupportedProducts(products []string) []string {
	supported := map[string]bool{}
	for _, p := range SupportedProducts {
		supported[p] = true
	}
	seen := map[string]bool{}
	var out []string
	for _, p := range products {
		if !supported[p] && !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// WithDotenv returns a getenv that falls back to the repo's .env the way the
// Python CLI's load_dotenv() did: the process environment wins, and the file
// is found by walking up from PDW_INGEST_PROJECT_DIR (when set) or the
// current directory. Only the lookup is layered; nothing is exported.
func WithDotenv(getenv func(string) string) func(string) string {
	start := strings.TrimSpace(getenv("PDW_INGEST_PROJECT_DIR"))
	if start == "" {
		if cwd, err := os.Getwd(); err == nil {
			start = cwd
		}
	}
	values := map[string]string{}
	if path := findDotenv(start); path != "" {
		if loaded, err := common.LoadDotenv(path); err == nil {
			values = loaded
		}
	}
	return func(name string) string {
		if v := getenv(name); v != "" {
			return v
		}
		return values[name]
	}
}

func findDotenv(start string) string {
	dir := start
	for dir != "" {
		candidate := filepath.Join(dir, ".env")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
	return ""
}
