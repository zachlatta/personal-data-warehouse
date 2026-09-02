package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const MinSecretTokenLength = 32

type Config struct {
	Addr                string
	BaseURL             string
	PostgresDatabaseURL string
	// QueryPostgresRole is the NOLOGIN database role assumed for every
	// user-authored read-only query. It has SELECT on queryable schemas but no
	// USAGE/SELECT on the private schema.
	QueryPostgresRole string
	// TimelineDatabaseURL is where the unified timeline tables live. Unset in
	// production (they share the warehouse database); a local development app
	// can point it at a locally-built timeline while detail views still read
	// source rows from PostgresDatabaseURL.
	TimelineDatabaseURL string
	// TimelineMediaBaseURL/TimelineMediaSigningKey control which app serves the
	// timeline UI's inline media through signed /objects/ links. Unset in
	// production (this app signs and serves its own); a local development app
	// without object-store credentials points these at the production app and
	// its secret so media still streams through the pdw proxy.
	TimelineMediaBaseURL    string
	TimelineMediaSigningKey string
	SecretToken             string
	MaxRows                 int
	MaxFieldChars           int
	QueryTimeout            time.Duration
	GmailAccounts           []string
	ContactGoogleAccounts   []string
	CalendarAccounts        []string
	AppleNotesAccounts      []string
	// ExpoAccessToken authenticates sends to the Expo push service. Optional:
	// Expo accepts unauthenticated sends unless the project enables enhanced
	// push security, in which case this is required.
	ExpoAccessToken string

	// Object storage (optional). When configured, the app can read blob objects
	// (Gmail attachments, Apple Notes/Messages attachments, Voice Memo audio,
	// ...) directly through its object storage layer.
	ObjectStoreBackend             string
	ObjectStoreGoogleDriveFolderID string
	ObjectStoreGoogleTokenJSON     string
	// Drive REST base-URL overrides and an auth bypass, for local integration
	// testing against a fake Drive. Never set in production.
	ObjectStoreGoogleDriveAPIBaseURL    string
	ObjectStoreGoogleDriveUploadBaseURL string
	ObjectStoreGoogleDriveDisableAuth   bool
	// ObjectStoreMaxObjectBytes caps how large an object the signed download
	// endpoint will serve; the Drive backend buffers whole objects in memory.
	ObjectStoreMaxObjectBytes int64
	// ObjectStoreURLTTL is how long signed object download links stay valid.
	ObjectStoreURLTTL time.Duration

	// IngestFolderIDs is the Drive folder id each ingestion source writes into,
	// keyed by source slug (e.g. "agent_sessions"). The app owns these so client
	// devices never hold a folder id or credential. Each falls back to
	// ObjectStoreGoogleDriveFolderID when its per-source override is unset.
	IngestFolderIDs map[string]string
	// IngestMaxObjectBytes caps the body size the ingestion endpoints accept.
	IngestMaxObjectBytes int64

	// SlackAccounts are the Slack workspaces whose files the app can fetch
	// live from the Slack API (slack_files rows only record metadata). Parsed
	// from the same SLACK_ACCOUNTS / SLACK_<SLUG>_TOKEN env vars as the Python
	// sync; accounts without a token are skipped.
	SlackAccounts []SlackAccount
	// SlackMutationAccounts names every configured Slack account, including
	// accounts that use only a private xoxc + d-cookie session for mutations.
	SlackMutationAccounts []string

	// Search embeddings (optional). The search tool's hybrid mode embeds the
	// query through an OpenAI-compatible /embeddings endpoint; when neither the
	// API key nor an explicit base URL is set, hybrid searches fall back to
	// keyword search. An explicit base URL alone counts as configured because
	// self-hosted embedding servers need no key.
	SearchEmbeddingsBaseURL    string
	SearchEmbeddingsAPIKey     string
	SearchEmbeddingsModel      string
	SearchEmbeddingsDimensions int
	// SearchEmbeddingsQueryPrefix is prepended to query text before embedding.
	// Instruction-tuned retrieval models (Qwen3-Embedding) embed documents raw
	// but expect queries wrapped in a task instruction; the Python indexing
	// runner deliberately never applies it.
	SearchEmbeddingsQueryPrefix string
	// DeprecatedEnvVars names environment variables that are set but no longer
	// do anything, each with the reason. The server logs them at startup.
	DeprecatedEnvVars []string

	// DriveSourceTokensByAccount maps a Google Drive *source* account email to
	// its OAuth token JSON, so the account-aware download proxy can stream a
	// google_drive_source file live from the account that owns it. Populated
	// from GOOGLE_DRIVE_SOURCE_ACCOUNTS (falling back to GMAIL_ACCOUNTS) plus
	// the per-account GOOGLE_<SLUG>_TOKEN_JSON[_B64] / GMAIL_<SLUG>_... env vars
	// the Python sync uses; accounts without a token are skipped.
	DriveSourceTokensByAccount map[string]string
}

// SlackAccount is one Slack workspace the app holds an API token for.
type SlackAccount struct {
	Name  string
	Token string
}

// SlackTokens returns the configured Slack API tokens in account order.
func (c Config) SlackTokens() []string {
	tokens := make([]string, 0, len(c.SlackAccounts))
	for _, account := range c.SlackAccounts {
		tokens = append(tokens, account.Token)
	}
	return tokens
}

// ObjectStoreEnabled reports whether enough is configured to build the app's
// object storage layer (Google Drive backend).
func (c Config) ObjectStoreEnabled() bool {
	return c.ObjectStoreBackend == "google_drive" &&
		c.ObjectStoreGoogleDriveFolderID != "" &&
		(c.ObjectStoreGoogleTokenJSON != "" || c.ObjectStoreGoogleDriveDisableAuth)
}

// ingestSourceEnvInfix maps each ingestion source slug to the infix used in its
// per-source folder override env var (PDW_INGEST_<INFIX>_FOLDER_ID).
var ingestSourceEnvInfix = map[string]string{
	"agent_sessions":    "AGENT_SESSIONS",
	"apple_contacts":    "APPLE_CONTACTS",
	"apple_messages":    "APPLE_MESSAGES",
	"apple_voice_memos": "VOICE_MEMOS",
	"apple_notes":       "APPLE_NOTES",
	"photos":            "PHOTOS",
	"manual_finance":    "MANUAL_FINANCE",
}

// IngestEnabled reports whether the app can accept client uploads: it needs the
// Drive credential (the same one the read path uses) so it can write on behalf
// of clients.
func (c Config) IngestEnabled() bool {
	return c.ObjectStoreBackend == "google_drive" &&
		(c.ObjectStoreGoogleTokenJSON != "" || c.ObjectStoreGoogleDriveDisableAuth)
}

func LoadFromEnv(getenv func(string) string) (Config, error) {
	cfg := Config{
		Addr:                    valueOrDefault(getenv("MCP_ADDR"), ":8080"),
		BaseURL:                 strings.TrimRight(strings.TrimSpace(getenv("MCP_BASE_URL")), "/"),
		PostgresDatabaseURL:     normalizePostgresURL(getenv("POSTGRES_DATABASE_URL")),
		QueryPostgresRole:       valueOrDefault(strings.TrimSpace(getenv("PDW_QUERY_POSTGRES_ROLE")), "pdw_query"),
		TimelineDatabaseURL:     normalizePostgresURL(getenv("PDW_TIMELINE_DATABASE_URL")),
		TimelineMediaBaseURL:    strings.TrimRight(strings.TrimSpace(getenv("PDW_TIMELINE_MEDIA_BASE_URL")), "/"),
		TimelineMediaSigningKey: strings.TrimSpace(getenv("PDW_TIMELINE_MEDIA_SIGNING_KEY")),
		SecretToken:             firstNonEmpty(getenv("PDW_SECRET_TOKEN"), getenv("MCP_SECRET_TOKEN")),
		MaxRows:                 100000,
		MaxFieldChars:           4000,
		// One end-to-end query budget. No caller can wait longer than the
		// public edge allows (~100s at the CDN; the CLI default is aligned to
		// this value), so a larger server-side ceiling only produced orphaned
		// queries that burned the database for minutes after every client had
		// given up — and invited retry storms on top. Deployments that need
		// longer diagnostics can raise MCP_QUERY_TIMEOUT; ad-hoc admin work
		// should use a direct Postgres connection instead.
		QueryTimeout:          60 * time.Second,
		GmailAccounts:         parseCSV(getenv("GMAIL_ACCOUNTS")),
		ContactGoogleAccounts: parseCSV(getenv("CONTACT_GOOGLE_ACCOUNTS")),
		CalendarAccounts:      parseCSV(firstNonEmpty(getenv("CALENDAR_ACCOUNTS"), getenv("GMAIL_ACCOUNTS"))),
		AppleNotesAccounts:    parseCSV(firstNonEmpty(getenv("APPLE_NOTES_ACCOUNTS"), getenv("APPLE_NOTES_ACCOUNT"), getenv("GMAIL_ACCOUNTS"))),
		ExpoAccessToken:       strings.TrimSpace(getenv("PDW_EXPO_ACCESS_TOKEN")),
	}

	var missing []string
	if cfg.PostgresDatabaseURL == "" {
		missing = append(missing, "POSTGRES_DATABASE_URL")
	}
	if cfg.SecretToken == "" {
		missing = append(missing, "PDW_SECRET_TOKEN (or legacy MCP_SECRET_TOKEN)")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}
	if len(cfg.SecretToken) < MinSecretTokenLength {
		return Config{}, fmt.Errorf("PDW_SECRET_TOKEN (or legacy MCP_SECRET_TOKEN) must be at least %d characters", MinSecretTokenLength)
	}
	if matched, _ := regexp.MatchString(`^[A-Za-z_][A-Za-z0-9_]*$`, cfg.QueryPostgresRole); !matched {
		return Config{}, fmt.Errorf("PDW_QUERY_POSTGRES_ROLE must be a Postgres identifier")
	}

	var err error
	if cfg.MaxRows, err = parsePositiveInt(getenv("MCP_MAX_ROWS"), cfg.MaxRows, "MCP_MAX_ROWS"); err != nil {
		return Config{}, err
	}
	if cfg.MaxFieldChars, err = parsePositiveInt(getenv("MCP_MAX_FIELD_CHARS"), cfg.MaxFieldChars, "MCP_MAX_FIELD_CHARS"); err != nil {
		return Config{}, err
	}
	cfg.ObjectStoreBackend = valueOrDefault(getenv("PDW_OBJECT_STORE_BACKEND"), "google_drive")
	cfg.ObjectStoreGoogleDriveFolderID = strings.TrimSpace(getenv("PDW_OBJECT_STORE_GOOGLE_DRIVE_FOLDER_ID"))
	cfg.ObjectStoreGoogleTokenJSON = jsonEnvValue(getenv, "PDW_OBJECT_STORE_GOOGLE_TOKEN_JSON")
	cfg.ObjectStoreGoogleDriveAPIBaseURL = strings.TrimSpace(getenv("PDW_OBJECT_STORE_GOOGLE_DRIVE_API_BASE_URL"))
	cfg.ObjectStoreGoogleDriveUploadBaseURL = strings.TrimSpace(getenv("PDW_OBJECT_STORE_GOOGLE_DRIVE_UPLOAD_BASE_URL"))
	cfg.ObjectStoreGoogleDriveDisableAuth = parseBool(getenv("PDW_OBJECT_STORE_GOOGLE_DRIVE_DISABLE_AUTH"))
	cfg.ObjectStoreMaxObjectBytes = 100 * 1024 * 1024
	if cfg.ObjectStoreMaxObjectBytes, err = parsePositiveInt64(getenv("PDW_OBJECT_STORE_MAX_OBJECT_BYTES"), cfg.ObjectStoreMaxObjectBytes, "PDW_OBJECT_STORE_MAX_OBJECT_BYTES"); err != nil {
		return Config{}, err
	}
	cfg.ObjectStoreURLTTL = time.Hour
	if raw := strings.TrimSpace(getenv("PDW_OBJECT_URL_TTL")); raw != "" {
		cfg.ObjectStoreURLTTL, err = time.ParseDuration(raw)
		if err != nil || cfg.ObjectStoreURLTTL <= 0 {
			return Config{}, fmt.Errorf("PDW_OBJECT_URL_TTL must be a positive Go duration")
		}
	}

	cfg.IngestFolderIDs = map[string]string{}
	for slug, infix := range ingestSourceEnvInfix {
		folder := firstNonEmpty(
			getenv("PDW_INGEST_"+infix+"_FOLDER_ID"),
			cfg.ObjectStoreGoogleDriveFolderID,
		)
		if folder := strings.TrimSpace(folder); folder != "" {
			cfg.IngestFolderIDs[slug] = folder
		}
	}
	cfg.IngestMaxObjectBytes = 512 * 1024 * 1024
	if cfg.IngestMaxObjectBytes, err = parsePositiveInt64(getenv("PDW_INGEST_MAX_OBJECT_BYTES"), cfg.IngestMaxObjectBytes, "PDW_INGEST_MAX_OBJECT_BYTES"); err != nil {
		return Config{}, err
	}

	cfg.SlackMutationAccounts = parseCSV(getenv("SLACK_ACCOUNTS"))
	for _, name := range cfg.SlackMutationAccounts {
		if token := strings.TrimSpace(getenv("SLACK_" + envSlug(name) + "_TOKEN")); token != "" {
			cfg.SlackAccounts = append(cfg.SlackAccounts, SlackAccount{Name: name, Token: token})
		}
	}

	driveSourceAccounts := parseCSV(getenv("GOOGLE_DRIVE_SOURCE_ACCOUNTS"))
	if len(driveSourceAccounts) == 0 {
		driveSourceAccounts = cfg.GmailAccounts
	}
	cfg.DriveSourceTokensByAccount = map[string]string{}
	for _, account := range driveSourceAccounts {
		if token := googleTokenJSONForAccount(getenv, account); token != "" {
			cfg.DriveSourceTokensByAccount[account] = token
		}
	}

	cfg.SearchEmbeddingsBaseURL = strings.TrimRight(strings.TrimSpace(getenv("SEARCH_EMBEDDINGS_BASE_URL")), "/")
	cfg.SearchEmbeddingsAPIKey = strings.TrimSpace(getenv("SEARCH_EMBEDDINGS_API_KEY"))
	cfg.SearchEmbeddingsModel = valueOrDefault(strings.TrimSpace(getenv("SEARCH_EMBEDDINGS_MODEL")), "text-embedding-3-small")
	// Decoded, not raw: the instruction wants a real newline before "Query:"
	// (worth ~0.02 MRR on the labeled benchmark against a space), but a
	// newline inside an environment value does not survive every deploy
	// pipeline -- Coolify silently truncated production's prefix at the
	// newline, so the app ran for weeks with the instruction's second half
	// missing and nothing reported it. Writing the escape as the two
	// characters \n keeps the value single-line all the way through.
	cfg.SearchEmbeddingsQueryPrefix = decodeEnvEscapes(getenv("SEARCH_EMBEDDINGS_QUERY_PREFIX"))
	// SEARCH_EMBEDDINGS_QUERY_RAW_WEIGHT blended instructed and raw vectors.
	// Retrieval now uses only the measured-best instructed vector, so the knob
	// and the raw-vector leg are both gone.
	// A still-set value is reported rather than ignored: silently inert config
	// is how a deployment ends up believing it is tuned when it is not.
	// The mutation review UI used to be a server-rendered page behind its own
	// password cookie. It is now the web SPA over the same bearer-protected
	// JSON API the iOS app uses, so these three no longer do anything.
	for _, name := range []string{"PDW_MUTATION_UI_PASSWORD", "PDW_MUTATION_UI_SESSION_SECRET", "PDW_MUTATION_UI_SESSION_TTL_SECONDS"} {
		if strings.TrimSpace(getenv(name)) != "" {
			cfg.DeprecatedEnvVars = append(cfg.DeprecatedEnvVars,
				name+" (removed: mutation review is the web app at "+"/mutation-review"+", authenticated with PDW_SECRET_TOKEN like the API; unset it)")
		}
	}
	for _, name := range []string{"MCP_QUERY_CACHE_MAX_BYTES", "MCP_GET_FIELD_MAX_CHARS", "MCP_QUERY_CACHE_TTL", "MCP_DEBUG_CACHE_TOOL"} {
		if strings.TrimSpace(getenv(name)) != "" {
			cfg.DeprecatedEnvVars = append(cfg.DeprecatedEnvVars,
				name+" (removed: MCP query now returns one bounded complete result and has no cursor cache; unset it)")
		}
	}
	if strings.TrimSpace(getenv("SEARCH_EMBEDDINGS_QUERY_RAW_WEIGHT")) != "" {
		cfg.DeprecatedEnvVars = append(cfg.DeprecatedEnvVars,
			"SEARCH_EMBEDDINGS_QUERY_RAW_WEIGHT (removed: search now uses the single measured-best instructed query vector; unset it)")
	}
	if cfg.SearchEmbeddingsDimensions, err = parsePositiveInt(getenv("SEARCH_EMBEDDINGS_DIMENSIONS"), 512, "SEARCH_EMBEDDINGS_DIMENSIONS"); err != nil {
		return Config{}, err
	}

	if raw := strings.TrimSpace(getenv("MCP_QUERY_TIMEOUT")); raw != "" {
		cfg.QueryTimeout, err = time.ParseDuration(raw)
		if err != nil || cfg.QueryTimeout <= 0 {
			return Config{}, fmt.Errorf("MCP_QUERY_TIMEOUT must be a positive Go duration")
		}
	}

	return cfg, nil
}

func normalizePostgresURL(raw string) string {
	value := strings.TrimSpace(raw)
	if strings.HasPrefix(value, "postgres://") {
		return "postgresql://" + strings.TrimPrefix(value, "postgres://")
	}
	if strings.HasPrefix(value, "postgresql+psycopg2://") {
		return "postgresql://" + strings.TrimPrefix(value, "postgresql+psycopg2://")
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// decodeEnvEscapes turns the escape sequences \n and \t in an environment
// value into the characters they name, accepting the doubled forms \\n and
// \\t as well. Nothing else is decoded: this is a transport fix for values
// that must stay on one line, not a general unescaper.
//
// Both forms are accepted because control planes re-escape on the way through.
// Coolify stores a submitted \n as \\n, and it truncates a real newline
// outright -- production ran for weeks with an instruction prefix cut in half
// at the newline and nothing reported it. Decoding both means the value works
// however the control plane chose to store it.
func decodeEnvEscapes(raw string) string {
	if !strings.Contains(raw, "\\") {
		return raw
	}
	replacer := strings.NewReplacer(
		"\\\\n", "\n",
		"\\\\t", "\t",
		"\\n", "\n",
		"\\t", "\t",
	)
	return replacer.Replace(raw)
}

func valueOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func parsePositiveInt(raw string, fallback int, name string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, errors.New(name + " must be a positive integer")
	}
	return value, nil
}

func parsePositiveInt64(raw string, fallback int64, name string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, errors.New(name + " must be a positive integer")
	}
	return value, nil
}

func parseBool(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// jsonEnvValue reads a JSON value from name, or from name+"_B64" (base64) if the
// plain var is empty. Mirrors the Python _json_env_value/_B64 convention.
func jsonEnvValue(getenv func(string) string, name string) string {
	if value := strings.TrimSpace(getenv(name)); value != "" {
		return value
	}
	encoded := strings.TrimSpace(getenv(name + "_B64"))
	if encoded == "" {
		return ""
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(decoded))
}

// googleTokenJSONForAccount resolves an account's Google OAuth token JSON from
// the same env names the Python sync writes (GOOGLE_<SLUG>_TOKEN_JSON[_B64] and
// the GMAIL_<SLUG>_... aliases), so the Go app reuses tokens without new config.
func googleTokenJSONForAccount(getenv func(string) string, account string) string {
	slug := envSlug(account)
	for _, name := range []string{"GOOGLE_" + slug + "_TOKEN_JSON", "GMAIL_" + slug + "_TOKEN_JSON"} {
		if token := jsonEnvValue(getenv, name); token != "" {
			return token
		}
	}
	return ""
}

// envSlug mirrors the Python env_slug: non-alphanumeric runs become "_",
// trimmed and uppercased, so SLACK_<SLUG>_TOKEN names match the Python sync.
func envSlug(value string) string {
	slug := strings.Trim(nonAlphanumericRun.ReplaceAllString(value, "_"), "_")
	if slug == "" {
		return "ACCOUNT"
	}
	return strings.ToUpper(slug)
}

var nonAlphanumericRun = regexp.MustCompile(`[^a-zA-Z0-9]+`)

func parseCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, strings.ToLower(value))
		}
	}
	return values
}
