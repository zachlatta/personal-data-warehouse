package mutations

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

// PolicyAPIPath is the reviewer surface for the switches: list policies and
// grants, set a policy's mode, and the global kill switch. Every write here is
// reviewer-only; an agent turns things off through a pdw.mutation_policy
// proposal, which PDW applies at once when it only narrows.
const PolicyAPIPath = APIPath + "/policies"

// PolicyAdminStore is what the policy switches need from the store.
type PolicyAdminStore interface {
	ListPolicies(ctx context.Context) (PolicyOverview, error)
	SetPolicyMode(ctx context.Context, policyID, mode, actor string) (bool, error)
	SetAutoApprove(ctx context.Context, enabled bool, actor string) error
	ClaimPolicyOutcomes(ctx context.Context, limit int) ([]PolicyOutcome, error)
}

// PolicyOverview is every policy's current version, its grants, and the
// switch.
type PolicyOverview struct {
	Policies           []Policy
	Grants             []PolicyGrant
	AutoApproveEnabled bool
	// AutoApproveForcedOff is true when PDW_MUTATION_AUTO_APPROVE=0 wins over
	// the settings row.
	AutoApproveForcedOff bool
}

func (s *PostgresStore) ListPolicies(ctx context.Context) (PolicyOverview, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.EnsureTables(ctx); err != nil {
		return PolicyOverview{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return PolicyOverview{}, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := queryContext(ctx, tx, `
		SELECT `+policyColumns+` FROM @mutation_policies AS policy
		WHERE version = (SELECT max(version) FROM @mutation_policies AS newer WHERE newer.id = policy.id)
		ORDER BY id`)
	if err != nil {
		return PolicyOverview{}, err
	}
	var overview PolicyOverview
	for rows.Next() {
		policy, err := scanPolicy(rows)
		if err != nil {
			rows.Close()
			return PolicyOverview{}, err
		}
		overview.Policies = append(overview.Policies, policy)
	}
	rows.Close()
	grantRows, err := queryContext(ctx, tx, `SELECT `+grantColumns+` FROM @mutation_policy_grants ORDER BY policy_id, created_at`)
	if err != nil {
		return PolicyOverview{}, err
	}
	for grantRows.Next() {
		grant, err := scanGrant(grantRows)
		if err != nil {
			grantRows.Close()
			return PolicyOverview{}, err
		}
		overview.Grants = append(overview.Grants, grant)
	}
	grantRows.Close()
	overview.AutoApproveForcedOff = autoApproveForcedOffByEnv()
	// FOR SHARE is not allowed in a read-only transaction; read the row plain.
	var raw []byte
	err = queryRowContext(ctx, tx, `SELECT value FROM @mutation_policy_settings WHERE key = $1`, AutoApproveSettingKey).Scan(&raw)
	switch {
	case err == nil:
		overview.AutoApproveEnabled = settingEnabled(raw)
	case errors.Is(err, sql.ErrNoRows):
		overview.AutoApproveEnabled = true
	default:
		return PolicyOverview{}, err
	}
	if overview.AutoApproveForcedOff {
		overview.AutoApproveEnabled = false
	}
	return overview, nil
}

func (s *PostgresStore) SetPolicyMode(ctx context.Context, policyID, mode, actor string) (bool, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.EnsureTables(ctx); err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	// The policy's lock: a mode change waits for an evaluation in flight.
	if err := lockPolicies(ctx, tx, []string{policyID}); err != nil {
		return false, err
	}
	changed, err := setPolicyMode(ctx, tx, policyID, mode, actor, "mode set by "+actor, time.Now().UTC())
	if err != nil {
		return false, err
	}
	return changed, tx.Commit()
}

func (s *PostgresStore) SetAutoApprove(ctx context.Context, enabled bool, actor string) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.EnsureTables(ctx); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// The switch's lock: a claim re-checking it waits for this, or this
	// waits for the claim.
	if err := lockPolicySettings(ctx, tx); err != nil {
		return err
	}
	value, _ := json.Marshal(map[string]any{"enabled": enabled})
	if _, err := execContext(ctx, tx, `
		INSERT INTO @mutation_policy_settings (key, value, updated_by, updated_at)
		VALUES ($1, $2::jsonb, $3, now())
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_by = EXCLUDED.updated_by, updated_at = EXCLUDED.updated_at
	`, AutoApproveSettingKey, string(value), actor); err != nil {
		return err
	}
	return tx.Commit()
}

func autoApproveForcedOffByEnv() bool {
	value := strings.TrimSpace(os.Getenv(AutoApproveEnv))
	return value == "0" || strings.EqualFold(value, "false") || strings.EqualFold(value, "off")
}

func settingEnabled(raw []byte) bool {
	var setting struct {
		Enabled *bool `json:"enabled"`
	}
	if err := json.Unmarshal(raw, &setting); err != nil || setting.Enabled == nil {
		return false
	}
	return *setting.Enabled
}

func policyJSON(policy Policy) map[string]any {
	return map[string]any{
		"id": policy.ID, "version": policy.Version, "operation": policy.Operation, "account": policy.Account,
		"match": policy.Match, "deny": policy.Deny, "limits": policy.Limits, "mode": policy.Mode,
		"valid_from": nullableTime(policy.ValidFrom), "valid_until": nullableTime(policy.ValidUntil),
		"created_by": policy.CreatedBy, "approved_by": policy.ApprovedBy, "created_at": nullableTime(policy.CreatedAt),
		"unreadable": policy.Unreadable,
	}
}

func grantJSON(grant PolicyGrant, now time.Time) map[string]any {
	return map[string]any{
		"id": grant.ID, "policy_id": grant.PolicyID, "token_id": grant.TokenID, "label": grant.Label,
		"expires_at": nullableTime(grant.ExpiresAt), "revoked_at": nullableTime(grant.RevokedAt), "revoked_by": grant.RevokedBy,
		"created_by": grant.CreatedBy, "created_at": nullableTime(grant.CreatedAt), "active": grant.Active(now),
	}
}

// apiPolicies serves PolicyAPIPath. GET lists; POST <id>/mode and POST
// settings change a switch, and only a reviewer may.
func (s *Service) apiPolicies(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && !requestCanReview(r) {
		apiError(w, http.StatusForbidden, "only Zach's web or phone session can change a mutation policy's switches; an agent proposes a pdw.mutation_policy change")
		return
	}
	admin, ok := s.store.(PolicyAdminStore)
	if !ok {
		apiError(w, http.StatusNotImplemented, "mutation policies need the Postgres store")
		return
	}
	rest := strings.Trim(strings.TrimPrefix(r.URL.Path, PolicyAPIPath), "/")
	actor := reviewerActor(r)
	parts := strings.Split(rest, "/")
	switch {
	case rest == "" && r.Method == http.MethodGet:
	case rest == "settings" && r.Method == http.MethodPost:
		var body struct {
			AutoApprove *bool `json:"auto_approve"`
		}
		if err := decodeOptionalJSON(r, &body); err != nil || body.AutoApprove == nil {
			apiError(w, http.StatusBadRequest, `body must be {"auto_approve": true|false}`)
			return
		}
		if err := admin.SetAutoApprove(r.Context(), *body.AutoApprove, actor); err != nil {
			apiError(w, http.StatusInternalServerError, err.Error())
			return
		}
	case len(parts) == 2 && parts[1] == "mode" && r.Method == http.MethodPost:
		var body struct {
			Mode string `json:"mode"`
		}
		if err := decodeOptionalJSON(r, &body); err != nil {
			apiError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if _, err := admin.SetPolicyMode(r.Context(), parts[0], strings.TrimSpace(body.Mode), actor); err != nil {
			apiError(w, http.StatusBadRequest, err.Error())
			return
		}
	default:
		http.NotFound(w, r)
		return
	}
	overview, err := admin.ListPolicies(r.Context())
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	now := time.Now().UTC()
	policies := make([]map[string]any, 0, len(overview.Policies))
	for _, policy := range overview.Policies {
		policies = append(policies, policyJSON(policy))
	}
	grants := make([]map[string]any, 0, len(overview.Grants))
	for _, grant := range overview.Grants {
		grants = append(grants, grantJSON(grant, now))
	}
	apiJSON(w, http.StatusOK, map[string]any{
		"auto_approve":            overview.AutoApproveEnabled,
		"auto_approve_forced_off": overview.AutoApproveForcedOff,
		"policies":                policies,
		"grants":                  grants,
	})
}

// reviewerActor is the actor a review surface records: app:<X-PDW-Client>.
func reviewerActor(r *http.Request) string {
	client := strings.TrimSpace(r.Header.Get("X-PDW-Client"))
	if client == "" {
		client = "ios"
	}
	return apiReviewerActorPrefix + client
}

// RunPolicyOutcomes queues policy outcomes into the notification outbox every
// interval until ctx ends. It sends nothing itself: the outbox worker
// (internal/notifications) delivers, retries and records each alert.
func (s *Service) RunPolicyOutcomes(ctx context.Context, interval time.Duration) {
	admin, ok := s.store.(PolicyAdminStore)
	if !ok {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, err := admin.ClaimPolicyOutcomes(ctx, 50); err != nil && !errors.Is(err, context.Canceled) {
			slog.Default().WarnContext(ctx, "policy outcome loop failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
