package feed

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

// APIPath is the phone's surface: GET the ranked feed, POST engagement.
// Agents use the feed_* tools (`pdw feed ...`), which share this Store.
const APIPath = "/api/feed"

type handler struct {
	store    *Store
	articles *ArticleReader
	logger   *slog.Logger
}

func (h *handler) register(mux *http.ServeMux, auth func(http.Handler) http.Handler) {
	mux.Handle("GET "+APIPath, auth(bounded(h.feed)))
	mux.Handle("POST "+APIPath+"/events", auth(bounded(h.events)))
	mux.Handle("GET "+APIPath+"/article", auth(bounded(h.article)))
	mux.Handle("GET "+APIPath+"/search", auth(bounded(h.search)))
}

func bounded(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		w.Header().Set("Cache-Control", "no-store")
		next(w, r.WithContext(ctx))
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	var invalidErr *ValidationError
	if errors.As(err, &invalidErr) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": invalidErr.Message})
		return
	}
	h.logger.ErrorContext(r.Context(), "feed request failed", "path", r.URL.Path, "error", err.Error())
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
}

type feedResponse struct {
	Items []Item `json:"items"`
	// Saved is his read-later queue, newest saved first; its cards can also
	// be in Items, and stay here after they leave the feed.
	Saved       []Item    `json:"saved"`
	GeneratedAt time.Time `json:"generated_at"`
}

func (h *handler) feed(w http.ResponseWriter, r *http.Request) {
	limit := 200
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > 500 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be 1 to 500"})
			return
		}
		limit = n
	}
	items, err := h.store.Feed(r.Context(), limit)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	saved, err := h.store.Saved(r.Context(), 500)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	forPhone(items, saved)
	writeJSON(w, http.StatusOK, feedResponse{Items: items, Saved: saved, GeneratedAt: h.store.now()})
}

// forPhone keeps the payload to what a card needs.
func forPhone(lists ...[]Item) {
	for _, list := range lists {
		for i := range list {
			list[i].Metadata = phoneMetadata(list[i].Metadata)
			list[i].PushedBy = ""
		}
	}
}

type searchResponse struct {
	Query string `json:"query"`
	Items []Item `json:"items"`
}

// search is the feed screen's search box: GET ?q=words&limit=n over every
// card he has been shown (Store.Search).
func (h *handler) search(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > 500 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be 1 to 500"})
			return
		}
		limit = n
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if len(query) > 200 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "q must be at most 200 characters"})
		return
	}
	items, err := h.store.Search(r.Context(), query, limit)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	forPhone(items)
	writeJSON(w, http.StatusOK, searchResponse{Query: query, Items: items})
}

// phoneCardKeys are the metadata keys a card shows: Hacker News counts (its
// counts row). Not the interest a pick was made for: Zach did not want a
// line saying why a card is here (2026-10-24).
var phoneCardKeys = []string{"hn_points", "hn_comments"}

func phoneMetadata(metadata map[string]any) map[string]any {
	var out map[string]any
	for _, key := range phoneCardKeys {
		if value, ok := metadata[key]; ok {
			if out == nil {
				out = map[string]any{}
			}
			out[key] = value
		}
	}
	return out
}

type eventsRequest struct {
	Device string  `json:"device"`
	Events []Event `json:"events"`
}

type eventsResponse struct {
	Accepted   int `json:"accepted"`
	Duplicates int `json:"duplicates"`
}

func (h *handler) events(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	var req eventsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be {\"events\": [...]}: " + err.Error()})
		return
	}
	if len(req.Events) == 0 {
		writeJSON(w, http.StatusOK, eventsResponse{})
		return
	}
	if len(req.Events) > MaxEventBatch {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "at most 500 events per batch"})
		return
	}
	now := h.store.now()
	for i := range req.Events {
		if err := ValidateEvent(&req.Events[i], now); err != nil {
			h.fail(w, r, err)
			return
		}
	}
	accepted, duplicates, err := h.store.RecordEvents(r.Context(), req.Events,
		pdwauth.ClientNameFromContext(r.Context()), strings.TrimSpace(req.Device))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, eventsResponse{Accepted: accepted, Duplicates: duplicates})
}

// article is the readable text of a card's page, for the detail view. A
// page with no article (or one that will not load) is a 422 the phone
// shows as "open the page" instead.
func (h *handler) article(w http.ResponseWriter, r *http.Request) {
	a, err := h.articles.Read(r.Context(), r.URL.Query().Get("url"))
	var invalidErr *ValidationError
	switch {
	case errors.As(err, &invalidErr):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": invalidErr.Message})
	case err != nil:
		h.logger.InfoContext(r.Context(), "feed article unreadable", "url", r.URL.Query().Get("url"), "error", err.Error())
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusOK, a)
	}
}
