package externalfeeds

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

func encodeCursor(n uint64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatUint(n, 10)))
}
func decodeCursor(raw string) (uint64, error) {
	if raw == "" {
		return 0, nil
	}
	b, e := base64.RawURLEncoding.DecodeString(raw)
	if e != nil {
		return 0, e
	}
	n, e := strconv.ParseUint(string(b), 10, 63)
	return n, e
}

func NewHandler(feed Definition, store Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private, no-cache")
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", 405)
			return
		}
		if r.URL.Query().Has("offset") {
			http.Error(w, "use the since cursor, not offset", 400)
			return
		}
		since, err := decodeCursor(r.URL.Query().Get("since"))
		if err != nil {
			http.Error(w, "invalid since cursor", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		rows, next, more, err := store.Page(ctx, feed, since, 200)
		if err != nil {
			http.Error(w, "feed temporarily unavailable", 503)
			return
		}
		if rows == nil {
			rows = []map[string]any{}
		}
		body, err := json.Marshal(map[string]any{"feed": feed.Scope, "recordings": rows, "next_cursor": encodeCursor(next), "has_more": more})
		if err != nil {
			http.Error(w, "could not encode feed", 500)
			return
		}
		etag := fmt.Sprintf(`"%x"`, sha256.Sum256(body))
		w.Header().Set("ETag", etag)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(304)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
}
