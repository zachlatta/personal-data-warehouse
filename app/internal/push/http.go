package push

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

const (
	RegisterPath   = "/api/push/register"
	TestPath       = "/api/push/test"
	SendPath       = "/api/push/send"
	CategoriesPath = "/api/push/categories"
)

// testImageURL is the picture the self-test attaches, so a phone can prove
// its service extension downloads and shows images.
const testImageURL = "https://assets.hackclub.com/flag-orpheus-top.png"

// Handler serves device registration and a self-test send. Both sit behind
// the static bearer, so the client name the token carries is what is stored.
type Handler struct {
	store    Store
	notifier *Notifier
	now      func() time.Time
	logger   *slog.Logger
}

func NewHandler(store Store, notifier *Notifier, now func() time.Time, logger *slog.Logger) *Handler {
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{store: store, notifier: notifier, now: now, logger: logger.With("component", "push")}
}

type registerRequest struct {
	ExpoPushToken string `json:"expo_push_token"`
	DeviceName    string `json:"device_name"`
	Platform      string `json:"platform"`
	AppVersion    string `json:"app_version"`
}

func (h *Handler) Register(mux *http.ServeMux, requireAuth func(http.Handler) http.Handler) {
	mux.Handle(RegisterPath, requireAuth(http.HandlerFunc(h.handleRegister)))
	mux.Handle(TestPath, requireAuth(http.HandlerFunc(h.handleTest)))
	mux.Handle(SendPath, requireAuth(http.HandlerFunc(h.handleSend)))
	mux.Handle(CategoriesPath, requireAuth(http.HandlerFunc(h.handleCategories)))
}

func (h *Handler) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var input registerRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := ValidateToken(input.ExpoPushToken); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	device, err := h.store.Register(r.Context(), Device{
		ExpoPushToken: strings.TrimSpace(input.ExpoPushToken),
		ClientName:    auth.ClientNameFromContext(r.Context()),
		DeviceName:    strings.TrimSpace(input.DeviceName),
		Platform:      strings.TrimSpace(input.Platform),
		AppVersion:    strings.TrimSpace(input.AppVersion),
	}, h.now())
	if err != nil {
		if errors.Is(err, ErrInvalidToken) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		h.logger.ErrorContext(r.Context(), "push device registration failed", "error", err.Error())
		writeError(w, http.StatusInternalServerError, "could not register device")
		return
	}
	h.logger.InfoContext(r.Context(), "push device registered", "client", device.ClientName, "device", device.DeviceName, "platform", device.Platform)
	writeJSON(w, http.StatusOK, map[string]any{"device": device})
}

// handleSend delivers a caller-built notification to every active device. It
// is the surface for iterating on notification UX: the notify tool and
// `pdw call notify` post here in spirit, and curl can post here literally.
func (h *Handler) handleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.notifier == nil {
		writeError(w, http.StatusServiceUnavailable, "push sending is not configured")
		return
	}
	var notification Notification
	decoder := json.NewDecoder(io.LimitReader(r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&notification); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if err := notification.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if notification.Data == nil {
		notification.Data = map[string]any{}
	}
	notification.Data["sent_by"] = auth.ClientNameFromContext(r.Context())
	report, err := h.notifier.Notify(r.Context(), notification)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	h.logger.InfoContext(r.Context(), "push sent", "title", notification.Title, "category", notification.Category, "image", notification.ImageURL != "", "sent", report.Sent, "failed", report.Failed)
	writeJSON(w, http.StatusOK, map[string]any{"report": report})
}

// handleCategories publishes the categories the app must register.
func (h *Handler) handleCategories(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"categories": Categories()})
}

// handleTest sends a notification to every active device; it is how the app's
// settings screen proves the whole path works — including the rich path, so
// it carries an image, a subtitle and a category.
func (h *Handler) handleTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.notifier == nil {
		writeError(w, http.StatusServiceUnavailable, "push sending is not configured")
		return
	}
	report, err := h.notifier.Notify(r.Context(), Notification{
		Title:    "PDW test notification",
		Subtitle: "Rich push check",
		Body:     "Push is working. If you see an image beside this and an Open button when you long-press, the rich path works too. Sent " + h.now().UTC().Format(time.RFC3339) + ".",
		ImageURL: testImageURL,
		Category: CategoryLink,
		ThreadID: "test",
		Route:    "/settings",
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"report": report})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}
