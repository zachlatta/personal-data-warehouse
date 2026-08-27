package mutations

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// APIPath is the JSON surface the iOS app reviews mutations through. It is
// the same store and the same review semantics as the HTML UI at ReviewPath —
// approve, deny, drop one email from a request — behind the static bearer
// instead of the UI's password cookie. Nothing here executes a mutation.
const APIPath = "/api/mutations"

const apiReviewerActorPrefix = "app:"

// APIHandler serves APIPath. requireAuth is the static-bearer middleware.
func (s *Service) APIHandler(requireAuth func(http.Handler) http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(APIPath+"/requests", requireAuth(http.HandlerFunc(s.apiListRequests)))
	mux.Handle(APIPath+"/requests/", requireAuth(http.HandlerFunc(s.apiRequestRoute)))
	return mux
}

// RegisterAPI mounts the JSON surface on mux.
func (s *Service) RegisterAPI(mux *http.ServeMux, requireAuth func(http.Handler) http.Handler) {
	handler := s.APIHandler(requireAuth)
	mux.Handle(APIPath+"/", handler)
}

// A status is whatever the store writes (pending_review, approved, rejected,
// executing, succeeded, failed, superseded, ...); the API only refuses tokens
// that could not be a status at all, so a new terminal state needs no edit here.
var apiStatusPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// apiMaxListLimit bounds one page of the request list; the web review list
// asks for a few hundred, the phone for fifty.
const apiMaxListLimit = 500

type apiUpdateEmailBody struct {
	DeliveryMode      string         `json:"delivery_mode"`
	SelectedVariantID string         `json:"selected_variant_id"`
	Message           map[string]any `json:"message"`
}

func (s *Service) apiListRequests(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		apiError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	filter := RequestFilter{Limit: 50}
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 || limit > apiMaxListLimit {
			apiError(w, http.StatusBadRequest, "limit must be between 1 and "+strconv.Itoa(apiMaxListLimit))
			return
		}
		filter.Limit = limit
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("status")); raw != "" {
		for _, status := range strings.Split(raw, ",") {
			status = strings.TrimSpace(status)
			if status == "" {
				continue
			}
			if !apiStatusPattern.MatchString(status) {
				apiError(w, http.StatusBadRequest, "malformed status "+status)
				return
			}
			filter.Statuses = append(filter.Statuses, status)
		}
	}
	requests, err := s.store.ListRequests(r.Context(), filter)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	items := make([]map[string]any, 0, len(requests))
	for _, request := range requests {
		items = append(items, s.requestJSON(request, false))
	}
	apiJSON(w, http.StatusOK, map[string]any{"requests": items})
}

func (s *Service) apiRequestRoute(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, APIPath+"/requests/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	requestID := parts[0]
	actor := apiReviewerActorPrefix + strings.TrimSpace(strings.TrimPrefix(r.Header.Get("X-PDW-Client"), ""))
	if actor == apiReviewerActorPrefix {
		actor = apiReviewerActorPrefix + "ios"
	}
	switch {
	case len(parts) == 1 && r.Method == http.MethodGet:
		request, err := s.store.GetRequest(r.Context(), requestID)
		if err != nil {
			apiStoreError(w, err)
			return
		}
		apiJSON(w, http.StatusOK, map[string]any{"request": s.requestJSON(request, true)})
	case len(parts) == 2 && parts[1] == "approve" && r.Method == http.MethodPost:
		request, err := s.store.ApproveRequest(r.Context(), requestID, actor)
		if err != nil {
			apiStoreError(w, err)
			return
		}
		apiJSON(w, http.StatusOK, map[string]any{"request": s.requestJSON(request, true)})
	case len(parts) == 2 && parts[1] == "reject" && r.Method == http.MethodPost:
		var body struct {
			Reason string `json:"reason"`
		}
		if err := decodeOptionalJSON(r, &body); err != nil {
			apiError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		request, err := s.store.RejectRequest(r.Context(), requestID, actor, strings.TrimSpace(body.Reason))
		if err != nil {
			apiStoreError(w, err)
			return
		}
		apiJSON(w, http.StatusOK, map[string]any{"request": s.requestJSON(request, true)})
	case len(parts) == 2 && parts[1] == "supersede" && r.Method == http.MethodPost:
		var body struct {
			SupersededBy string `json:"superseded_by"`
		}
		if err := decodeOptionalJSON(r, &body); err != nil {
			apiError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		supersededBy := strings.TrimSpace(body.SupersededBy)
		if err := validateSupersedeInput(requestID, supersededBy); err != nil {
			apiError(w, http.StatusBadRequest, err.Error())
			return
		}
		request, err := s.store.SupersedeRequest(r.Context(), requestID, supersededBy, actor)
		if err != nil {
			apiStoreError(w, err)
			return
		}
		apiJSON(w, http.StatusOK, map[string]any{"request": s.requestJSON(request, true)})
	case len(parts) == 4 && parts[1] == "mutations" && parts[3] == "update-email" && r.Method == http.MethodPost:
		var body apiUpdateEmailBody
		if err := decodeOptionalJSON(r, &body); err != nil {
			apiError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		mutation, err := s.store.UpdateGmailEmailMutation(r.Context(), requestID, parts[2], gmailEmailUpdateInputFromJSON(body), actor)
		if err != nil {
			apiStoreError(w, err)
			return
		}
		apiJSON(w, http.StatusOK, map[string]any{"mutation": mutationJSON(mutation)})
	case len(parts) == 4 && parts[1] == "mutations" && parts[3] == "remove" && r.Method == http.MethodPost:
		mutation, err := s.store.RemoveMutation(r.Context(), requestID, parts[2], actor)
		if err != nil {
			apiStoreError(w, err)
			return
		}
		apiJSON(w, http.StatusOK, map[string]any{"mutation": mutationJSON(mutation)})
	default:
		http.NotFound(w, r)
	}
}

func decodeOptionalJSON(r *http.Request, into any) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil
	}
	return json.Unmarshal(raw, into)
}

func apiStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrNotFound) {
		apiError(w, http.StatusNotFound, err.Error())
		return
	}
	// Store errors on approve/reject are state conflicts ("request is not
	// pending") far more often than outages; 409 lets the app say so.
	apiError(w, http.StatusConflict, err.Error())
}

func apiJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func apiError(w http.ResponseWriter, status int, message string) {
	apiJSON(w, status, map[string]any{"error": message})
}

// nullableTime translates the warehouse's "absent" sentinel (the epoch, never
// NULL) back to JSON null at the read interface, per the warehouse convention.
func nullableTime(value time.Time) any {
	if value.IsZero() || value.Unix() <= 0 {
		return nil
	}
	return value.UTC().Format(time.RFC3339)
}

func (s *Service) requestJSON(request Request, withMutations bool) map[string]any {
	item := map[string]any{
		"id":             request.ID,
		"status":         request.Status,
		"title":          request.Title,
		"reason":         request.Reason,
		"context":        emptyMap(request.Context),
		"result":         emptyMap(request.Result),
		"error":          request.Error,
		"superseded_by":  request.SupersededBy,
		"revision":       request.Revision,
		"requested_by":   request.RequestedBy,
		"approved_by":    request.ApprovedBy,
		"created_at":     nullableTime(request.CreatedAt),
		"updated_at":     nullableTime(request.UpdatedAt),
		"approved_at":    nullableTime(request.ApprovedAt),
		"executed_at":    nullableTime(request.ExecutedAt),
		"observed_at":    nullableTime(request.ObservedAt),
		"mutation_count": request.MutationCount,
		"review_url":     s.requestURL(request.ID),
		// Only a request nothing else can move may be closed out by naming
		// its replacement; the status list lives here, not in each client.
		"can_supersede": strings.TrimSpace(request.SupersededBy) == "" && requestIsSupersedable(request.Status),
	}
	if withMutations {
		mutations := make([]map[string]any, 0, len(request.Mutations))
		for _, mutation := range request.Mutations {
			mutations = append(mutations, mutationJSON(mutation))
		}
		item["mutations"] = mutations
	}
	return item
}

func mutationJSON(mutation Mutation) map[string]any {
	item := map[string]any{
		"id":            mutation.ID,
		"request_id":    mutation.RequestID,
		"request_index": mutation.RequestIndex,
		"provider":      mutation.Provider,
		"operation":     mutation.Operation,
		"account":       mutation.Account,
		"status":        mutation.Status,
		"title":         mutation.Title,
		"reason":        mutation.Reason,
		"payload":       emptyMap(mutation.Payload),
		"preview":       emptyMap(mutation.Preview),
		"result":        emptyMap(mutation.Result),
		"error":         mutation.Error,
		"revision":      mutation.Revision,
		"attempt_count": mutation.AttemptCount,
		"requested_by":  mutation.RequestedBy,
		"approved_by":   mutation.ApprovedBy,
		"created_at":    nullableTime(mutation.CreatedAt),
		"updated_at":    nullableTime(mutation.UpdatedAt),
		"approved_at":   nullableTime(mutation.ApprovedAt),
		"executed_at":   nullableTime(mutation.ExecutedAt),
		"observed_at":   nullableTime(mutation.ObservedAt),
	}
	if isGmailEmailMutation(mutation) {
		item["email"] = gmailEmailView(mutation)
	}
	return item
}

func isGmailEmailMutation(mutation Mutation) bool {
	return mutation.Provider == "gmail" && mutation.Operation == GmailSendEmailOperation
}

func emptyMap(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	return value
}
