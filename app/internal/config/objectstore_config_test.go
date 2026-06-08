package config

import (
	"encoding/base64"
	"testing"
)

func baseEnv(extra map[string]string) func(string) string {
	values := map[string]string{
		"POSTGRES_DATABASE_URL": "postgres://localhost/pdw",
		"PDW_SECRET_TOKEN":      "0123456789012345678901234567890123456789",
	}
	for k, v := range extra {
		values[k] = v
	}
	return func(key string) string { return values[key] }
}

func TestObjectStoreDisabledByDefault(t *testing.T) {
	cfg, err := LoadFromEnv(baseEnv(nil))
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.ObjectStoreEnabled() {
		t.Fatalf("object store should be disabled without config")
	}
	if cfg.ObjectStoreBackend != "google_drive" {
		t.Fatalf("default backend = %q", cfg.ObjectStoreBackend)
	}
	if cfg.ObjectStoreMaxObjectBytes != 5*1024*1024 {
		t.Fatalf("default max bytes = %d", cfg.ObjectStoreMaxObjectBytes)
	}
}

func TestObjectStoreEnabledWithFolderAndToken(t *testing.T) {
	cfg, err := LoadFromEnv(baseEnv(map[string]string{
		"PDW_OBJECT_STORE_GOOGLE_DRIVE_FOLDER_ID": "folder-1",
		"PDW_OBJECT_STORE_GOOGLE_TOKEN_JSON":      `{"refresh_token":"rt"}`,
	}))
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if !cfg.ObjectStoreEnabled() {
		t.Fatalf("object store should be enabled")
	}
	if cfg.ObjectStoreGoogleTokenJSON != `{"refresh_token":"rt"}` {
		t.Fatalf("token = %q", cfg.ObjectStoreGoogleTokenJSON)
	}
}

func TestObjectStoreTokenFromBase64(t *testing.T) {
	token := `{"refresh_token":"rt"}`
	cfg, err := LoadFromEnv(baseEnv(map[string]string{
		"PDW_OBJECT_STORE_GOOGLE_DRIVE_FOLDER_ID": "folder-1",
		"PDW_OBJECT_STORE_GOOGLE_TOKEN_JSON_B64":  base64.StdEncoding.EncodeToString([]byte(token)),
	}))
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.ObjectStoreGoogleTokenJSON != token {
		t.Fatalf("decoded token = %q", cfg.ObjectStoreGoogleTokenJSON)
	}
	if !cfg.ObjectStoreEnabled() {
		t.Fatalf("object store should be enabled via base64 token")
	}
}
