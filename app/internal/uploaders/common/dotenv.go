package common

import (
	"bufio"
	"os"
	"strings"
)

// LoadDotenv reads a .env file and returns its assignments, in file order.
// Only the subset python-dotenv accepts for these files is supported:
// KEY=VALUE, optional `export `, single/double quotes, and `#` comments. A
// missing file is not an error; nothing is exported here.
func LoadDotenv(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil
		}
		return nil, err
	}
	defer file.Close()
	values := map[string]string{}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, raw, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		values[key] = parseDotenvValue(strings.TrimSpace(raw))
	}
	return values, scanner.Err()
}

func parseDotenvValue(raw string) string {
	if raw == "" {
		return ""
	}
	switch raw[0] {
	case '"':
		end := strings.LastIndex(raw, `"`)
		if end > 0 {
			inner := raw[1:end]
			inner = strings.ReplaceAll(inner, `\n`, "\n")
			inner = strings.ReplaceAll(inner, `\"`, `"`)
			return inner
		}
	case '\'':
		end := strings.LastIndex(raw, `'`)
		if end > 0 {
			return raw[1:end]
		}
	}
	// Unquoted: strip a trailing comment.
	if idx := strings.Index(raw, " #"); idx >= 0 {
		raw = raw[:idx]
	}
	return strings.TrimSpace(raw)
}

// ApplyDotenv loads path and sets every variable that is not already set in
// the process environment (python-dotenv's default: the environment wins).
func ApplyDotenv(path string) error {
	values, err := LoadDotenv(path)
	if err != nil {
		return err
	}
	for key, value := range values {
		if _, exists := os.LookupEnv(key); !exists {
			os.Setenv(key, value)
		}
	}
	return nil
}
