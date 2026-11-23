package server

import (
	"container/list"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

const (
	notificationAvatarPath       = "/api/notifications/avatar"
	notificationAvatarSignPrefix = "avatar:"
	notificationAvatarTTL        = 24 * time.Hour
	notificationAvatarMaxBytes   = 2 << 20 // 2 MiB
	notificationAvatarCacheSize  = 256
)

// signAvatarURL mints a short-lived HMAC-authenticated URL for a sender avatar.
// The key names the warehouse record (never a raw upstream URL), so personal
// avatar URLs never leak into push payloads and every fetch is authenticated.
func signAvatarURL(baseURL string, signer *pdwauth.Service, now func() time.Time, key string) string {
	if !strings.HasPrefix(baseURL, "https://") || signer == nil || key == "" {
		return ""
	}
	if now == nil {
		now = time.Now
	}
	exp := now().Add(notificationAvatarTTL)
	sig := signer.SignObjectDownload(notificationAvatarSignPrefix+key, "", exp)
	return fmt.Sprintf("%s%s?k=%s&exp=%d&sig=%s",
		strings.TrimRight(baseURL, "/"),
		notificationAvatarPath,
		url.QueryEscape(key),
		exp.Unix(),
		url.QueryEscape(sig),
	)
}

// avatarCacheKey returns a short, stable, opaque cache key for a person's
// avatar so the Android app caches the face across rotating signed URLs.
func avatarCacheKey(key string) string {
	if key == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:12])
}

// isAllowedAvatarURL restricts upstream avatar fetches to known HTTPS avatar
// CDNs so the proxy can never be abused for SSRF.
func isAllowedAvatarURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Port() != "" || u.Host == "" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	switch {
	case host == "slack-edge.com" || strings.HasSuffix(host, ".slack-edge.com"):
		return true
	case strings.HasSuffix(host, ".googleusercontent.com"):
		return true
	case host == "gravatar.com" || strings.HasSuffix(host, ".gravatar.com"):
		return true
	default:
		return false
	}
}

type cachedAvatar struct {
	key         string
	contentType string
	body        []byte
	expiresAt   time.Time
}

type avatarCache struct {
	mu      sync.Mutex
	maxSize int
	order   *list.List
	items   map[string]*list.Element
}

func newAvatarCache(maxSize int) *avatarCache {
	return &avatarCache{
		maxSize: maxSize,
		order:   list.New(),
		items:   make(map[string]*list.Element, maxSize),
	}
}

func (c *avatarCache) get(key string, now time.Time) (cachedAvatar, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return cachedAvatar{}, false
	}
	entry := elem.Value.(cachedAvatar)
	if now.After(entry.expiresAt) {
		c.order.Remove(elem)
		delete(c.items, key)
		return cachedAvatar{}, false
	}
	c.order.MoveToFront(elem)
	return entry, true
}

func (c *avatarCache) put(entry cachedAvatar) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[entry.key]; ok {
		elem.Value = entry
		c.order.MoveToFront(elem)
		return
	}
	elem := c.order.PushFront(entry)
	c.items[entry.key] = elem
	for c.order.Len() > c.maxSize {
		oldest := c.order.Back()
		if oldest == nil {
			break
		}
		c.order.Remove(oldest)
		delete(c.items, oldest.Value.(cachedAvatar).key)
	}
}

type avatarProxy struct {
	signer     *pdwauth.Service
	resolveURL func(ctx context.Context, key string) (string, error)
	client     *http.Client
	allowURL   func(string) bool
	cache      *avatarCache
	now        func() time.Time
	logger     *slog.Logger
}

func newAvatarProxy(db *sql.DB, signer *pdwauth.Service, logger *slog.Logger) *avatarProxy {
	if logger == nil {
		logger = slog.Default()
	}
	p := &avatarProxy{
		signer:   signer,
		allowURL: isAllowedAvatarURL,
		cache:    newAvatarCache(notificationAvatarCacheSize),
		now:      time.Now,
		logger:   logger,
	}
	p.client = &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return http.ErrUseLastResponse
			}
			if !p.allowURL(req.URL.String()) {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}
	p.resolveURL = func(ctx context.Context, key string) (string, error) {
		return resolveAvatarKeyFromDB(ctx, db, key)
	}
	return p
}

func (p *avatarProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	key := q.Get("k")
	if key == "" || p.signer == nil {
		http.Error(w, "invalid or expired link", http.StatusForbidden)
		return
	}
	if err := p.signer.VerifyObjectDownload(notificationAvatarSignPrefix+key, "", q.Get("exp"), q.Get("sig")); err != nil {
		http.Error(w, "invalid or expired link", http.StatusForbidden)
		return
	}
	now := p.now()
	if cached, ok := p.cache.get(key, now); ok {
		writeAvatarResponse(w, r, cached.contentType, cached.body)
		return
	}
	upstreamURL, err := p.resolveURL(r.Context(), key)
	if err != nil || upstreamURL == "" {
		http.NotFound(w, r)
		return
	}
	if !p.allowURL(upstreamURL) {
		http.NotFound(w, r)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, upstreamURL, nil)
	if err != nil {
		http.Error(w, "avatar unavailable", http.StatusBadGateway)
		return
	}
	resp, err := p.client.Do(req)
	if err != nil {
		http.Error(w, "avatar unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		http.NotFound(w, r)
		return
	}
	contentType := strings.TrimSpace(strings.Split(resp.Header.Get("Content-Type"), ";")[0])
	if !strings.HasPrefix(strings.ToLower(contentType), "image/") {
		http.Error(w, "avatar unavailable", http.StatusBadGateway)
		return
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, notificationAvatarMaxBytes+1))
	if err != nil || len(data) == 0 || len(data) > notificationAvatarMaxBytes {
		http.Error(w, "avatar unavailable", http.StatusBadGateway)
		return
	}
	p.cache.put(cachedAvatar{
		key:         key,
		contentType: contentType,
		body:        data,
		expiresAt:   now.Add(notificationAvatarTTL),
	})
	writeAvatarResponse(w, r, contentType, data)
}

func writeAvatarResponse(w http.ResponseWriter, r *http.Request, contentType string, data []byte) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "private, max-age=3600")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = w.Write(data)
}

func resolveAvatarKeyFromDB(ctx context.Context, db *sql.DB, key string) (string, error) {
	if db == nil {
		return "", sql.ErrNoRows
	}
	parts := strings.Split(key, "|")
	switch parts[0] {
	case "slack":
		if len(parts) != 4 {
			return "", sql.ErrNoRows
		}
		var rawURL string
		err := db.QueryRowContext(ctx, warehouse.ExpandRelations(slackUserAvatarByKeySQL), parts[1], parts[2], parts[3]).Scan(&rawURL)
		return rawURL, err
	case "contact":
		if len(parts) != 6 {
			return "", sql.ErrNoRows
		}
		var rawURL string
		err := db.QueryRowContext(ctx, warehouse.ExpandRelations(contactPhotoByKeySQL), parts[1], parts[2], parts[3], parts[4], parts[5]).Scan(&rawURL)
		if err != nil {
			return "", err
		}
		return googlePhotoSize.ReplaceAllString(rawURL, "=s256-c"), nil
	default:
		return "", sql.ErrNoRows
	}
}

const slackUserAvatarByKeySQL = `
SELECT COALESCE(NULLIF(u.raw_json, '')::jsonb->'profile'->>'image_192', NULLIF(u.raw_json, '')::jsonb->'profile'->>'image_72', '')
FROM @slack_users u
WHERE u.account = $1 AND u.team_id = $2 AND u.user_id = $3
LIMIT 1`

const contactPhotoByKeySQL = `
SELECT photo->>'url'
FROM @clean_contacts c
CROSS JOIN LATERAL jsonb_array_elements(c.photos) photo
WHERE c.source = $1 AND c.account = $2 AND c.source_kind = $3 AND c.address_book_id = $4 AND c.card_id = $5
  AND photo->>'url' LIKE 'https://%' AND NOT COALESCE((photo->>'default')::boolean, false)
LIMIT 1`
