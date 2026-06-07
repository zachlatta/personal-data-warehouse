// Package cliconfig handles persistent pdw configuration: where to read
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

// configDir is the per-user config subdirectory. legacyConfigDir is the
// directory the CLI used when it shipped as "pdw-cli"; reads fall back to it
// so users who logged in before the rename don't have to re-authenticate.
const (
	configDir       = "pdw"
	legacyConfigDir = "pdw-cli"
	configFile      = "config.json"
)

// Config is the on-disk shape. Fields are lowercase JSON so the file is
// readable by humans and editable in a pinch.
type Config struct {
	BaseURL    string `json:"base_url,omitempty"`
	Token      string `json:"token,omitempty"`
	ClientName string `json:"client_name,omitempty"`
}

// Path resolves the canonical config file path from environment variables.
// New writes always land here. The caller passes a getenv func so tests don't
// have to mutate process env.
//
// Precedence: $XDG_CONFIG_HOME/pdw/config.json, then
// $HOME/.config/pdw/config.json. Returns an error if neither is set.
func Path(getenv func(string) string) (string, error) {
	return pathIn(getenv, configDir)
}

// LegacyPath resolves the pre-rename ("pdw-cli") config file path. It exists
// only so reads can transparently fall back to a config written by an older
// binary; nothing writes here anymore.
func LegacyPath(getenv func(string) string) (string, error) {
	return pathIn(getenv, legacyConfigDir)
}

func pathIn(getenv func(string) string, dir string) (string, error) {
	if x := getenv("XDG_CONFIG_HOME"); x != "" {
		return filepath.Join(x, dir, configFile), nil
	}
	if h := getenv("HOME"); h != "" {
		return filepath.Join(h, ".config", dir, configFile), nil
	}
	return "", fmt.Errorf("cannot determine config path: neither XDG_CONFIG_HOME nor HOME is set")
}

// Resolve returns the effective config and the path it was loaded from. It
// prefers the canonical pdw path but transparently falls back to the legacy
// pdw-cli path when the canonical file is absent or empty, so logins made
// before the rename keep working. When neither exists it returns a zero
// Config and the canonical path — the place a future Save will write.
func Resolve(getenv func(string) string) (Config, string, error) {
	path, err := Path(getenv)
	if err != nil {
		return Config{}, "", err
	}
	cfg, err := Load(path)
	if err != nil {
		return Config{}, path, err
	}
	if cfg != (Config{}) {
		return cfg, path, nil
	}
	// Canonical file missing/empty: try the legacy location.
	if legacy, lerr := LegacyPath(getenv); lerr == nil {
		lcfg, lerr2 := Load(legacy)
		if lerr2 != nil {
			return Config{}, legacy, lerr2
		}
		if lcfg != (Config{}) {
			return lcfg, legacy, nil
		}
	}
	return Config{}, path, nil
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

// DeleteAll removes both the canonical pdw config and the legacy pdw-cli
// config so `logout` fully de-authenticates regardless of which file a prior
// login wrote. It returns the paths it actually removed (so callers can report
// them) and the first error encountered.
func DeleteAll(getenv func(string) string) (removed []string, err error) {
	seen := map[string]bool{}
	for _, resolve := range []func(func(string) string) (string, error){Path, LegacyPath} {
		path, perr := resolve(getenv)
		if perr != nil {
			if err == nil {
				err = perr
			}
			continue
		}
		if seen[path] {
			continue
		}
		seen[path] = true
		if _, statErr := os.Stat(path); statErr != nil {
			if os.IsNotExist(statErr) {
				continue // nothing to remove at this location
			}
			if err == nil {
				err = fmt.Errorf("stat %s: %w", path, statErr)
			}
			continue
		}
		if derr := Delete(path); derr != nil {
			if err == nil {
				err = derr
			}
			continue
		}
		removed = append(removed, path)
	}
	return removed, err
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
