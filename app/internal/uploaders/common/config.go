package common

import (
	"os"
	"strings"
)

// Getenv is the environment lookup every uploader is built with, so tests can
// inject one.
type Getenv func(string) string

// Env resolves the value of the first non-empty variable in names.
func (g Getenv) Env(names ...string) string {
	for _, name := range names {
		if v := strings.TrimSpace(g(name)); v != "" {
			return v
		}
	}
	return ""
}

// CSVFirst returns the first non-empty comma-separated entry of a variable.
func (g Getenv) CSVFirst(name string) string {
	for _, part := range strings.Split(g(name), ",") {
		if v := strings.TrimSpace(part); v != "" {
			return v
		}
	}
	return ""
}

// CSV returns the non-empty comma-separated entries of a variable.
func (g Getenv) CSV(name string) []string {
	var out []string
	for _, part := range strings.Split(g(name), ",") {
		if v := strings.TrimSpace(part); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// Bool mirrors config.py's boolean parsing (1/true/yes/y/on).
func (g Getenv) Bool(name string, fallback bool) bool {
	value := g(name)
	if value == "" {
		return fallback
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

// Enabled mirrors the "not in {0,false,no,off}" checks used for the kill
// switches (APPLE_NOTES_MUTATIONS_ENABLED, VOICE_MEMOS_WRITEBACK_ENABLED, ...).
func (g Getenv) Enabled(name string) bool {
	value := strings.ToLower(strings.TrimSpace(g(name)))
	switch value {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

// The account label chains below reproduce personal_data_warehouse.config so a
// Go uploader keys its uploads and state exactly as the Python one did.

// DefaultAccount is the first GMAIL_ACCOUNTS entry, the last resort for every
// source's account label.
func (g Getenv) DefaultAccount() string { return g.CSVFirst("GMAIL_ACCOUNTS") }

// VoiceMemosAccount: VOICE_MEMOS_ACCOUNT, else the default.
func (g Getenv) VoiceMemosAccount() string {
	return firstNonEmpty(g.Env("VOICE_MEMOS_ACCOUNT"), g.DefaultAccount())
}

// AppleNotesAccount: APPLE_NOTES_ACCOUNT, VOICE_MEMOS_ACCOUNT, default.
func (g Getenv) AppleNotesAccount() string {
	return firstNonEmpty(g.Env("APPLE_NOTES_ACCOUNT"), g.Env("VOICE_MEMOS_ACCOUNT"), g.DefaultAccount())
}

// AppleMessagesAccount: APPLE_MESSAGES_ACCOUNT, APPLE_NOTES_ACCOUNT,
// VOICE_MEMOS_ACCOUNT, default.
func (g Getenv) AppleMessagesAccount() string {
	return firstNonEmpty(g.Env("APPLE_MESSAGES_ACCOUNT"), g.Env("APPLE_NOTES_ACCOUNT"), g.Env("VOICE_MEMOS_ACCOUNT"), g.DefaultAccount())
}

// AppleContactsAccount: APPLE_CONTACTS_ACCOUNT, then the Messages chain.
func (g Getenv) AppleContactsAccount() string {
	return firstNonEmpty(g.Env("APPLE_CONTACTS_ACCOUNT"), g.AppleMessagesAccount())
}

// PhotosAccount: PHOTOS_ACCOUNT, then the Messages chain.
func (g Getenv) PhotosAccount() string {
	return firstNonEmpty(g.Env("PHOTOS_ACCOUNT"), g.AppleMessagesAccount())
}

// ManualFinanceAccount: MANUAL_FINANCE_ACCOUNT, then the Photos chain.
func (g Getenv) ManualFinanceAccount() string {
	return firstNonEmpty(g.Env("MANUAL_FINANCE_ACCOUNT"), g.PhotosAccount())
}

// AgentSessionsAccount: AGENT_SESSIONS_ACCOUNT, then the Messages chain.
func (g Getenv) AgentSessionsAccount() string {
	return firstNonEmpty(g.Env("AGENT_SESSIONS_ACCOUNT"), g.AppleMessagesAccount())
}

// PlaidAccount: PLAID_ACCOUNT, AGENT_SESSIONS_ACCOUNT, then the Messages chain.
func (g Getenv) PlaidAccount() string {
	return firstNonEmpty(g.Env("PLAID_ACCOUNT"), g.Env("AGENT_SESSIONS_ACCOUNT"), g.AppleMessagesAccount())
}

// DeviceName is the short hostname (AGENT_SESSIONS_DEVICE overrides it).
func (g Getenv) DeviceName() string {
	if v := g.Env("AGENT_SESSIONS_DEVICE"); v != "" {
		return v
	}
	return LocalDeviceName()
}

// LocalDeviceName is the short hostname, "unknown-device" when unknown.
func LocalDeviceName() string {
	name, err := os.Hostname()
	if err != nil || strings.TrimSpace(name) == "" {
		return "unknown-device"
	}
	name = strings.TrimSpace(name)
	if idx := strings.Index(name, "."); idx >= 0 {
		name = name[:idx]
	}
	if name == "" {
		return "unknown-device"
	}
	return name
}

// Path resolves a path variable with a default, expanding "~".
func (g Getenv) Path(name, fallback string) string {
	value := g(name)
	if strings.TrimSpace(value) == "" {
		value = fallback
	}
	return ExpandUser(value)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// FirstNonEmpty is the exported spelling.
func FirstNonEmpty(values ...string) string { return firstNonEmpty(values...) }
