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
	store  *Store
	logger *slog.Logger
}

func (h *handler) register(mux *http.ServeMux, auth func(http.Handler) http.Handler) {
	mux.Handle("GET "+APIPath, auth(bounded(h.feed)))
	mux.Handle("POST "+APIPath+"/events", auth(bounded(h.events)))
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
	Items       []Item    `json:"items"`
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
	for i := range items {
		// The phone never shows these; keep the payload to what a card needs.
		items[i].Metadata = nil
		items[i].PushedBy = ""
	}
	writeJSON(w, http.StatusOK, feedResponse{Items: items, GeneratedAt: h.store.now()})
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
