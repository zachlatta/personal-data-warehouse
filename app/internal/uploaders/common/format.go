package common

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// FormatBytes renders a byte count as the uploaders' run logs do:
// "1234 B", "1.5 KiB", ...
func FormatBytes(size int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	value := float64(size)
	for i, unit := range units {
		if value < 1024 || i == len(units)-1 {
			if unit == "B" {
				return fmt.Sprintf("%d B", int64(value))
			}
			return fmt.Sprintf("%.1f %s", value, unit)
		}
		value /= 1024
	}
	return fmt.Sprintf("%d B", size)
}

// FileSHA256 hashes a file in 1 MiB chunks.
func FileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// BytesSHA256 hashes a byte slice.
func BytesSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// GzipJSONL encodes records as canonical JSON lines and gzips them (mtime 0,
// no filename), the batch body every gzip endpoint receives.
func GzipJSONL(records []map[string]any) ([]byte, error) {
	var lines bytes.Buffer
	for _, record := range records {
		encoded, err := CanonicalJSON(record)
		if err != nil {
			return nil, err
		}
		lines.Write(encoded)
		lines.WriteByte('\n')
	}
	var out bytes.Buffer
	writer := gzip.NewWriter(&out)
	writer.ModTime = time.Time{}
	if _, err := writer.Write(lines.Bytes()); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// ExpandUser expands a leading "~/" to the home directory, like
// os.path.expanduser.
func ExpandUser(path string) string {
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return path
		}
		return filepath.Join(home, strings.TrimPrefix(path, "~"))
	}
	return path
}

// HomeDir returns the user's home directory or "" when unknown.
func HomeDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return home
}

// ApplicationSupportDir is where every uploader keeps its state files:
// ~/Library/Application Support/personal-data-warehouse.
func ApplicationSupportDir() string {
	return filepath.Join(HomeDir(), "Library", "Application Support", "personal-data-warehouse")
}

// WithSuffix mirrors pathlib's with_suffix for the lock file beside a state file.
func WithSuffix(path, suffix string) string {
	ext := filepath.Ext(path)
	return strings.TrimSuffix(path, ext) + suffix
}

// ShortSHA256 is the first 12 characters, as the run logs print.
func ShortSHA256(value string) string {
	if len(value) <= 12 {
		return value
	}
	return value[:12]
}

// WriteFileAtomic writes data to path through a sibling temp file and rename.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}
