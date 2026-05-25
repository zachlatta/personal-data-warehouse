// Package cliconfig handles persistent pdw-cli configuration: where to read
// or write it, the on-disk schema, and safe file permissions. Everything
// I/O-related goes through here so the command layer can be tested with a
// pure env stub and a temp directory.
package cliconfig

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Config is the on-disk shape. Fields are lowercase JSON so the file is
// readable by humans and editable in a pinch.
type Config struct {
	BaseURL    string `json:"base_url,omitempty"`
	Token      string `json:"token,omitempty"`
	ClientName string `json:"client_name,omitempty"`
}

// Path resolves the config file path from environment variables. The
// caller passes a getenv func so tests don't have to mutate process env.
//
// Precedence: $XDG_CONFIG_HOME/pdw-cli/config.json, then
// $HOME/.config/pdw-cli/config.json. Returns an error if neither is set.
func Path(getenv func(string) string) (string, error) {
	if x := getenv("XDG_CONFIG_HOME"); x != "" {
		return filepath.Join(x, "pdw-cli", "config.json"), nil
	}
	if h := getenv("HOME"); h != "" {
		return filepath.Join(h, ".config", "pdw-cli", "config.json"), nil
	}
	return "", fmt.Errorf("cannot determine config path: neither XDG_CONFIG_HOME nor HOME is set")
}

// Load reads the config file. A missing file returns a zero Config and a
// nil error — there's no need for a sentinel for "not configured yet".
func Load(path string) (Config, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Config{}, nil
		}
		return Config{}, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg Config
	if err := json.Unmarshal(body, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return cfg, nil
}

// Save writes the config atomically with 0o600 permissions, creating its
// parent directory with 0o700 if needed. The token field is sensitive, so
// keep it readable only by the owner.
func Save(path string, cfg Config) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	// MkdirAll respects an existing dir's mode; tighten it in case the
	// dir existed already with looser perms.
	if err := os.Chmod(dir, 0o700); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("chmod %s: %w", dir, err)
	}
	body, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".config.json.*")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		cleanup()
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		cleanup()
		return fmt.Errorf("chmod temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close temp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		cleanup()
		return fmt.Errorf("rename %s -> %s: %w", tmpPath, path, err)
	}
	return nil
}

// Delete removes the config file. A missing file is not an error.
func Delete(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove %s: %w", path, err)
	}
	return nil
}

// Redacted returns a copy with the token replaced by a fixed-shape mask
// so callers can safely print configuration without leaking the secret.
func (c Config) Redacted() Config {
	red := c
	if c.Token != "" {
		// Avoid '<' / '>' so json.Marshal's default HTML-escaping doesn't
		// turn the marker into '<redacted>' in `config show`.
		red.Token = "[REDACTED]"
	}
	return red
}
