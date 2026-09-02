package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadFromEnvRequiresPostgresURLAndSecret(t *testing.T) {
	_, err := LoadFromEnv(func(string) string { return "" })
	if err == nil {
		t.Fatal("expected missing env error")
	}
	if !strings.Contains(err.Error(), "POSTGRES_DATABASE_URL") || !strings.Contains(err.Error(), "MCP_SECRET_TOKEN") {
		t.Fatalf("error should mention both missing env vars, got %q", err.Error())
	}
}

func TestLoadFromEnvDefaultsAndOverrides(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL":          "postgres://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":               "0123456789abcdef0123456789abcdef",
		"MCP_ADDR":                       ":9090",
		"MCP_BASE_URL":                   "https://mcp.example.com/",
		"MCP_MAX_ROWS":                   "7",
		"MCP_MAX_FIELD_CHARS":            "12",
		"MCP_QUERY_CACHE_MAX_BYTES":      "1048576",
		"MCP_GET_FIELD_MAX_CHARS":        "345",
		"MCP_QUERY_CACHE_TTL":            "15m",
		"MCP_DEBUG_CACHE_TOOL":           "true",
		"MCP_QUERY_TIMEOUT":              "42s",
		"PDW_QUERY_POSTGRES_ROLE":        "warehouse_reader",
		"PDW_MUTATION_UI_PASSWORD":       "review-password",
		"GMAIL_ACCOUNTS":                 "zach@example.test, work@example.test",
		"CONTACT_GOOGLE_ACCOUNTS":        "contacts@example.test",
		"SEARCH_EMBEDDINGS_BASE_URL":     "https://embeddings.example.test/v1/",
		"SEARCH_EMBEDDINGS_API_KEY":      "sk-embed-test",
		"SEARCH_EMBEDDINGS_MODEL":        "custom-embedding-model",
		"SEARCH_EMBEDDINGS_DIMENSIONS":   "256",
		"SEARCH_EMBEDDINGS_QUERY_PREFIX": "Instruct: retrieve personal data\nQuery:",
	}

	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv returned error: %v", err)
	}

	if cfg.Addr != ":9090" {
		t.Fatalf("Addr = %q", cfg.Addr)
	}
	if cfg.BaseURL != "https://mcp.example.com" {
		t.Fatalf("BaseURL = %q", cfg.BaseURL)
	}
	if cfg.MaxRows != 7 || cfg.MaxFieldChars != 12 {
		t.Fatalf("limits = rows %d chars %d", cfg.MaxRows, cfg.MaxFieldChars)
	}
	for _, retired := range []string{"MCP_QUERY_CACHE_MAX_BYTES", "MCP_GET_FIELD_MAX_CHARS", "MCP_QUERY_CACHE_TTL", "MCP_DEBUG_CACHE_TOOL"} {
		if !containsPrefix(cfg.DeprecatedEnvVars, retired) {
			t.Fatalf("%s should be reported as deprecated, got %v", retired, cfg.DeprecatedEnvVars)
		}
	}
	if cfg.QueryTimeout != 42*time.Second {
		t.Fatalf("QueryTimeout = %s", cfg.QueryTimeout)
	}
	if cfg.QueryPostgresRole != "warehouse_reader" {
		t.Fatalf("QueryPostgresRole = %q", cfg.QueryPostgresRole)
	}
	// The review UI's own password is gone: the web app authenticates with
	// the API token. A still-set value is reported, never silently ignored.
	if !containsPrefix(cfg.DeprecatedEnvVars, "PDW_MUTATION_UI_PASSWORD") {
		t.Fatalf("PDW_MUTATION_UI_PASSWORD should be reported as deprecated, got %v", cfg.DeprecatedEnvVars)
	}
	if strings.Join(cfg.GmailAccounts, ",") != "zach@example.test,work@example.test" {
		t.Fatalf("GmailAccounts = %#v", cfg.GmailAccounts)
	}
	if strings.Join(cfg.ContactGoogleAccounts, ",") != "contacts@example.test" {
		t.Fatalf("ContactGoogleAccounts = %#v", cfg.ContactGoogleAccounts)
	}
	if strings.Join(cfg.CalendarAccounts, ",") != "zach@example.test,work@example.test" {
		t.Fatalf("CalendarAccounts = %#v", cfg.CalendarAccounts)
	}
	if cfg.PostgresDatabaseURL != "postgresql://default:secret@example.com/default" {
		t.Fatalf("PostgresDatabaseURL = %q", cfg.PostgresDatabaseURL)
	}
	if cfg.SearchEmbeddingsBaseURL != "https://embeddings.example.test/v1" {
		t.Fatalf("SearchEmbeddingsBaseURL = %q", cfg.SearchEmbeddingsBaseURL)
	}
	if cfg.SearchEmbeddingsAPIKey != "sk-embed-test" || cfg.SearchEmbeddingsModel != "custom-embedding-model" || cfg.SearchEmbeddingsDimensions != 256 || cfg.SearchEmbeddingsQueryPrefix != "Instruct: retrieve personal data\nQuery:" {
		t.Fatalf("search embeddings config = key %q model %q dims %d prefix %q", cfg.SearchEmbeddingsAPIKey, cfg.SearchEmbeddingsModel, cfg.SearchEmbeddingsDimensions, cfg.SearchEmbeddingsQueryPrefix)
	}
}

func TestLoadFromEnvSearchEmbeddingsDefaults(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL": "postgres://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":      "0123456789abcdef0123456789abcdef",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv returned error: %v", err)
	}
	// BaseURL and APIKey stay empty when unset — that pair is what marks
	// embeddings unconfigured — while model and dimensions carry defaults.
	if cfg.SearchEmbeddingsBaseURL != "" || cfg.SearchEmbeddingsAPIKey != "" {
		t.Fatalf("embeddings should be unconfigured by default: base %q key %q", cfg.SearchEmbeddingsBaseURL, cfg.SearchEmbeddingsAPIKey)
	}
	if cfg.SearchEmbeddingsModel != "text-embedding-3-small" || cfg.SearchEmbeddingsDimensions != 512 {
		t.Fatalf("embeddings defaults = model %q dims %d", cfg.SearchEmbeddingsModel, cfg.SearchEmbeddingsDimensions)
	}
}

func TestLoadFromEnvReportsTheRemovedQueryRawWeight(t *testing.T) {
	// The blend knob and the raw neighbourhood are gone. A deployment that still sets it must be
	// told, not silently ignored: inert config that looks live is how a
	// deployment ends up believing it is tuned when it is not.
	env := map[string]string{
		"POSTGRES_DATABASE_URL":              "postgres://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":                   "0123456789abcdef0123456789abcdef",
		"SEARCH_EMBEDDINGS_QUERY_RAW_WEIGHT": "0.5",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if len(cfg.DeprecatedEnvVars) != 1 || !strings.Contains(cfg.DeprecatedEnvVars[0], "SEARCH_EMBEDDINGS_QUERY_RAW_WEIGHT") {
		t.Fatalf("deprecated env vars = %#v", cfg.DeprecatedEnvVars)
	}
}

func TestLoadFromEnvDefaultQueryTimeoutMatchesCallerBudget(t *testing.T) {
	// The server-side statement budget must not exceed what callers can
	// actually wait for (the public edge cuts requests off around 100s, the
	// CLI default is aligned to this value): a larger ceiling only produced
	// orphaned statements that kept burning the database after every client
	// had given up.
	env := map[string]string{
		"POSTGRES_DATABASE_URL": "postgres://d:s@example.com/d",
		"MCP_SECRET_TOKEN":      "0123456789abcdef0123456789abcdef",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.QueryTimeout != 60*time.Second {
		t.Fatalf("QueryTimeout = %s, want 60s", cfg.QueryTimeout)
	}
}

func TestLoadFromEnvDefaultsQueryPostgresRole(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL": "postgres://d:s@example.com/d",
		"MCP_SECRET_TOKEN":      "0123456789abcdef0123456789abcdef",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.QueryPostgresRole != "pdw_query" {
		t.Fatalf("QueryPostgresRole = %q, want pdw_query", cfg.QueryPostgresRole)
	}
}

func TestLoadFromEnvRejectsUnsafeQueryPostgresRole(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL":   "postgres://d:s@example.com/d",
		"MCP_SECRET_TOKEN":        "0123456789abcdef0123456789abcdef",
		"PDW_QUERY_POSTGRES_ROLE": `pdw_query"; RESET ROLE; --`,
	}
	if _, err := LoadFromEnv(func(key string) string { return env[key] }); err == nil {
		t.Fatal("expected unsafe query role to be rejected")
	}
}

func TestLoadFromEnvIngestFolderResolution(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL":                   "postgres://d:s@example.com/d",
		"MCP_SECRET_TOKEN":                        "0123456789abcdef0123456789abcdef",
		"PDW_OBJECT_STORE_GOOGLE_TOKEN_JSON":      `{"type":"authorized_user"}`,
		"PDW_OBJECT_STORE_GOOGLE_DRIVE_FOLDER_ID": "shared-folder",
		"PDW_INGEST_AGENT_SESSIONS_FOLDER_ID":     "agent-folder",
		"PDW_INGEST_MANUAL_FINANCE_FOLDER_ID":     "manual-finance-folder",
		"PDW_INGEST_APPLE_CONTACTS_FOLDER_ID":     "apple-contacts-folder",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv returned error: %v", err)
	}
	if !cfg.IngestEnabled() {
		t.Fatal("IngestEnabled() = false, want true when token is set")
	}
	if got := cfg.IngestFolderIDs["agent_sessions"]; got != "agent-folder" {
		t.Fatalf("agent_sessions folder = %q, want per-source override", got)
	}
	if got := cfg.IngestFolderIDs["manual_finance"]; got != "manual-finance-folder" {
		t.Fatalf("manual_finance folder = %q, want per-source override", got)
	}
	if got := cfg.IngestFolderIDs["apple_contacts"]; got != "apple-contacts-folder" {
		t.Fatalf("apple_contacts folder = %q, want per-source override", got)
	}
	// Sources without an override fall back to the shared object-store folder.
	if got := cfg.IngestFolderIDs["apple_messages"]; got != "shared-folder" {
		t.Fatalf("apple_messages folder = %q, want shared fallback", got)
	}
}

func TestLoadFromEnvIngestDisabledWithoutToken(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL": "postgres://d:s@example.com/d",
		"MCP_SECRET_TOKEN":      "0123456789abcdef0123456789abcdef",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv returned error: %v", err)
	}
	if cfg.IngestEnabled() {
		t.Fatal("IngestEnabled() = true, want false without a Drive token")
	}
}

func TestLoadFromEnvPrefersPDWSecretTokenOverMCPSecretToken(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL": "postgresql://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":      "legacy0123456789abcdef0123456789abcd",
		"PDW_SECRET_TOKEN":      "preferred0123456789abcdef0123456789abcd",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.SecretToken != "preferred0123456789abcdef0123456789abcd" {
		t.Fatalf("SecretToken = %q; PDW_SECRET_TOKEN must win over legacy MCP_SECRET_TOKEN", cfg.SecretToken)
	}
}

func TestLoadFromEnvCalendarAccountsFallbackIgnoresBlankOverride(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL": "postgresql://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":      "legacy0123456789abcdef0123456789abcd",
		"GMAIL_ACCOUNTS":        "zach@example.test",
		"CALENDAR_ACCOUNTS":     " ",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if strings.Join(cfg.CalendarAccounts, ",") != "zach@example.test" {
		t.Fatalf("CalendarAccounts = %#v", cfg.CalendarAccounts)
	}
}

func TestLoadFromEnvFallsBackToMCPSecretToken(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL": "postgresql://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":      "legacy0123456789abcdef0123456789abcd",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.SecretToken != "legacy0123456789abcdef0123456789abcd" {
		t.Fatalf("SecretToken = %q; expected MCP_SECRET_TOKEN fallback", cfg.SecretToken)
	}
}

func TestLoadFromEnvErrorMentionsPDWSecretToken(t *testing.T) {
	_, err := LoadFromEnv(func(string) string { return "" })
	if err == nil {
		t.Fatal("expected missing env error")
	}
	if !strings.Contains(err.Error(), "PDW_SECRET_TOKEN") {
		t.Fatalf("missing-env error should mention PDW_SECRET_TOKEN, got %q", err.Error())
	}
}

func TestLoadFromEnvRejectsWeakSecretToken(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL": "postgresql://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":      "short",
	}
	_, err := LoadFromEnv(func(key string) string { return env[key] })
	if err == nil {
		t.Fatal("expected weak secret error")
	}
	if !strings.Contains(err.Error(), "at least 32 characters") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadFromEnvParsesSlackAccounts(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL":   "postgres://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":        "0123456789abcdef0123456789abcdef",
		"SLACK_ACCOUNTS":          "hackclub, My-Personal, tokenless",
		"SLACK_HACKCLUB_TOKEN":    "xoxp-hackclub",
		"SLACK_MY_PERSONAL_TOKEN": "xoxp-personal",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv returned error: %v", err)
	}
	if len(cfg.SlackAccounts) != 2 {
		t.Fatalf("SlackAccounts = %#v", cfg.SlackAccounts)
	}
	if cfg.SlackAccounts[0] != (SlackAccount{Name: "hackclub", Token: "xoxp-hackclub"}) {
		t.Fatalf("SlackAccounts[0] = %#v", cfg.SlackAccounts[0])
	}
	if cfg.SlackAccounts[1] != (SlackAccount{Name: "my-personal", Token: "xoxp-personal"}) {
		t.Fatalf("SlackAccounts[1] = %#v", cfg.SlackAccounts[1])
	}
	if strings.Join(cfg.SlackTokens(), ",") != "xoxp-hackclub,xoxp-personal" {
		t.Fatalf("SlackTokens = %#v", cfg.SlackTokens())
	}
	if strings.Join(cfg.SlackMutationAccounts, ",") != "hackclub,my-personal,tokenless" {
		t.Fatalf("SlackMutationAccounts = %#v", cfg.SlackMutationAccounts)
	}
}

func TestLoadFromEnvNoSlackAccounts(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL": "postgres://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":      "0123456789abcdef0123456789abcdef",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv returned error: %v", err)
	}
	if len(cfg.SlackAccounts) != 0 || len(cfg.SlackTokens()) != 0 || len(cfg.SlackMutationAccounts) != 0 {
		t.Fatalf("expected no slack accounts, got %#v", cfg.SlackAccounts)
	}
}

func TestLoadFromEnvDriveSourceTokensByAccount(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL": "postgres://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":      "0123456789abcdef0123456789abcdef",
		"GMAIL_ACCOUNTS":        "zach@hackclub.com, zach@zachlatta.com",
		// hackclub via the GOOGLE_<SLUG>_TOKEN_JSON form, zachlatta via the
		// GMAIL_<SLUG>_TOKEN_JSON_B64 alias, to cover both lookups.
		"GOOGLE_ZACH_HACKCLUB_COM_TOKEN_JSON":     `{"token":"hc"}`,
		"GMAIL_ZACH_ZACHLATTA_COM_TOKEN_JSON_B64": "eyJ0b2tlbiI6Inp6In0=",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if got := cfg.DriveSourceTokensByAccount["zach@hackclub.com"]; got != `{"token":"hc"}` {
		t.Fatalf("hackclub token = %q", got)
	}
	if got := cfg.DriveSourceTokensByAccount["zach@zachlatta.com"]; got != `{"token":"zz"}` {
		t.Fatalf("zachlatta token = %q", got)
	}
}

func TestLoadFromEnvDriveSourceAccountsOverrideGmail(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL":               "postgres://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":                    "0123456789abcdef0123456789abcdef",
		"GMAIL_ACCOUNTS":                      "zach@hackclub.com",
		"GOOGLE_DRIVE_SOURCE_ACCOUNTS":        "work@example.test",
		"GOOGLE_WORK_EXAMPLE_TEST_TOKEN_JSON": `{"token":"w"}`,
		"GOOGLE_ZACH_HACKCLUB_COM_TOKEN_JSON": `{"token":"hc"}`,
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if _, ok := cfg.DriveSourceTokensByAccount["zach@hackclub.com"]; ok {
		t.Fatal("gmail account must not be used when GOOGLE_DRIVE_SOURCE_ACCOUNTS is set")
	}
	if got := cfg.DriveSourceTokensByAccount["work@example.test"]; got != `{"token":"w"}` {
		t.Fatalf("work token = %q", got)
	}
}

func TestLoadFromEnvDecodesQueryPrefixEscapes(t *testing.T) {
	// Qwen3's instruction format puts the query on its own line. A real
	// newline in an environment value does not survive every deploy pipeline
	// (Coolify truncated production's prefix at the newline, so the app ran
	// with half an instruction and nothing said so), and the truncated form
	// measurably retrieves worse. The two-character escape keeps the value on
	// one line from the control plane to the process.
	env := map[string]string{
		"POSTGRES_DATABASE_URL":          "postgres://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":               "0123456789abcdef0123456789abcdef",
		"SEARCH_EMBEDDINGS_QUERY_PREFIX": `Instruct: find it\nQuery: `,
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.SearchEmbeddingsQueryPrefix != "Instruct: find it\nQuery: " {
		t.Fatalf("prefix = %q", cfg.SearchEmbeddingsQueryPrefix)
	}
}

func TestLoadFromEnvKeepsRealNewlinesInQueryPrefix(t *testing.T) {
	env := map[string]string{
		"POSTGRES_DATABASE_URL":          "postgres://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":               "0123456789abcdef0123456789abcdef",
		"SEARCH_EMBEDDINGS_QUERY_PREFIX": "Instruct: find it\nQuery: ",
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.SearchEmbeddingsQueryPrefix != "Instruct: find it\nQuery: " {
		t.Fatalf("prefix = %q", cfg.SearchEmbeddingsQueryPrefix)
	}
}

func TestLoadFromEnvDecodesDoubledPrefixEscapes(t *testing.T) {
	// Coolify stores a submitted \n as \\n. Decoding only the single form
	// would leave a stray backslash in the instruction, changing the text the
	// embedding model is asked to follow.
	env := map[string]string{
		"POSTGRES_DATABASE_URL":          "postgres://default:secret@example.com/default",
		"MCP_SECRET_TOKEN":               "0123456789abcdef0123456789abcdef",
		"SEARCH_EMBEDDINGS_QUERY_PREFIX": `Instruct: find it\\nQuery: `,
	}
	cfg, err := LoadFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.SearchEmbeddingsQueryPrefix != "Instruct: find it\nQuery: " {
		t.Fatalf("prefix = %q", cfg.SearchEmbeddingsQueryPrefix)
	}
}

func containsPrefix(values []string, prefix string) bool {
	for _, value := range values {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}
