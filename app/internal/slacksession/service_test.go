package slacksession

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

const testSecret = "0123456789abcdef0123456789abcdef"

var testNow = time.Unix(1_700_000_000, 0).UTC()

func testSigner() *pdwauth.Service {
	return pdwauth.NewService([]byte(testSecret), func() time.Time { return testNow })
}

type fakeStore struct{ creds []Credential }

func (f *fakeStore) Upsert(_ context.Context, cred Credential, now time.Time) (Ack, error) {
	f.creds = append(f.creds, cred)
	sum := sha256.Sum256([]byte(cred.SessionToken))
	return Ack{
		Account: cred.Account, SessionKey: cred.SessionKey, SourceApp: cred.SourceApp,
		TokenSHA256: hex.EncodeToString(sum[:]), TeamID: cred.TeamID,
		EnterpriseID: cred.EnterpriseID, UserID: cred.UserID,
		CookieExpiresAt: cred.CookieExpiresAt, UpdatedAt: now,
	}, nil
}

func post(t *testing.T, store Store, body string) *httptest.ResponseRecorder {
	t.Helper()
	signer := testSigner()
	sum := sha256.Sum256([]byte(body))
	sha := hex.EncodeToString(sum[:])
	exp := testNow.Add(10 * time.Minute)
	sig := signer.SignObjectUpload(Endpoint, sha, exp)
	q := url.Values{
		"content_sha256": {sha},
		"exp":            {strconv.FormatInt(exp.Unix(), 10)},
		"sig":            {sig},
	}
	req := httptest.NewRequest(http.MethodPost, Endpoint+"?"+q.Encode(), strings.NewReader(body))
	rec := httptest.NewRecorder()
	NewService(store, signer, func() time.Time { return testNow }, slog.Default()).Handler().ServeHTTP(rec, req)
	return rec
}

func TestPublishStoresBothHalvesOfTheSession(t *testing.T) {
	store := &fakeStore{}
	rec := post(t, store, `{"account":"zrl","session_token":"xoxc-tok","session_cookie":"xoxd-ck",
		"team_id":"T0266FRGM","enterprise_id":"E09V59WQY1E","user_id":"U1",
		"source_app":"slack-app","cookie_expires_at":"2027-09-28T03:42:00Z"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(store.creds) != 1 {
		t.Fatalf("want 1 credential, got %d", len(store.creds))
	}
	got := store.creds[0]
	if got.SessionToken != "xoxc-tok" || got.SessionCookie != "xoxd-ck" {
		t.Fatalf("both halves must be persisted, got %+v", got)
	}
	if got.TeamID != "T0266FRGM" || got.EnterpriseID != "E09V59WQY1E" {
		t.Fatalf("workspace and org ids must stay separate, got %+v", got)
	}
	var ack Ack
	if err := json.Unmarshal(rec.Body.Bytes(), &ack); err != nil {
		t.Fatalf("ack not JSON: %v", err)
	}
	if strings.Contains(rec.Body.String(), "xoxc-tok") || strings.Contains(rec.Body.String(), "xoxd-ck") {
		t.Fatal("the acknowledgement must not echo the credential")
	}
}

func TestPublishRejectsATokenWithoutItsCookie(t *testing.T) {
	// An xoxc token without the `d` cookie authenticates as nobody. Accepting it
	// would store a credential that looks present on every dashboard and then
	// 401s on first use, which is far worse than refusing the publish.
	store := &fakeStore{}
	rec := post(t, store, `{"account":"zrl","session_token":"xoxc-tok"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
	if len(store.creds) != 0 {
		t.Fatal("nothing should have been stored")
	}
}

func TestPublishRejectsAnEnterpriseIDInTheTeamIDField(t *testing.T) {
	// Hack Club is Enterprise Grid: auth.test on a client session returns the
	// org's E-id while all ~45M warehouse rows are keyed by the workspace T-id.
	// Writing one as the other forks the dataset with no error anywhere, so the
	// swap is refused at the boundary.
	store := &fakeStore{}
	rec := post(t, store, `{"account":"zrl","session_token":"xoxc-tok","session_cookie":"xoxd-ck",
		"team_id":"E09V59WQY1E"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(store.creds) != 0 {
		t.Fatal("an enterprise id must never be stored as a team id")
	}
}

func TestPublishRequiresAValidSignature(t *testing.T) {
	store := &fakeStore{}
	body := `{"account":"zrl","session_token":"xoxc-tok","session_cookie":"xoxd-ck"}`
	sum := sha256.Sum256([]byte(body))
	q := url.Values{
		"content_sha256": {hex.EncodeToString(sum[:])},
		"exp":            {strconv.FormatInt(testNow.Add(time.Minute).Unix(), 10)},
		"sig":            {"not-a-signature"},
	}
	req := httptest.NewRequest(http.MethodPost, Endpoint+"?"+q.Encode(), strings.NewReader(body))
	rec := httptest.NewRecorder()
	NewService(store, testSigner(), func() time.Time { return testNow }, slog.Default()).Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("want 403, got %d", rec.Code)
	}
}
