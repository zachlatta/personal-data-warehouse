package whoopsession

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

type fakeStore struct {
	creds []Credential
	err   error
}

func (f *fakeStore) Upsert(_ context.Context, cred Credential, now time.Time) (Ack, error) {
	if f.err != nil {
		return Ack{}, f.err
	}
	f.creds = append(f.creds, cred)
	sum := sha256.Sum256([]byte(cred.RefreshToken))
	return Ack{
		Account:            cred.Account,
		SessionKey:         cred.SessionKey,
		SourceBrowser:      cred.SourceBrowser,
		RefreshTokenSHA256: hex.EncodeToString(sum[:]),
		AccessExpiresAt:    cred.AccessExpiresAt,
		RefreshExpiresAt:   cred.RefreshExpiresAt,
		UpdatedAt:          now,
	}, nil
}

func signedTarget(body []byte) string {
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	exp := testNow.Add(time.Hour)
	q := url.Values{}
	q.Set("content_sha256", sha)
	q.Set("exp", strconv.FormatInt(exp.Unix(), 10))
	q.Set("sig", testSigner().SignObjectUpload(Endpoint, sha, exp))
	return Endpoint + "?" + q.Encode()
}

func post(svc *Service, target string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(rec, req)
	return rec
}

func newService(store Store) *Service {
	return NewService(store, testSigner(), func() time.Time { return testNow }, slog.Default())
}

func validBody() []byte {
	body, _ := json.Marshal(publishRequest{
		Account:          "zach@example.com",
		AccessToken:      "access-token",
		RefreshToken:     "refresh-token",
		AccessExpiresAt:  "2026-08-24T16:15:44Z",
		RefreshExpiresAt: "2026-09-22T21:03:24Z",
		SourceBrowser:    "Google Chrome",
	})
	return body
}

func TestPublishStoresTheCredentialAndNeverEchoesTokens(t *testing.T) {
	store := &fakeStore{}
	body := validBody()

	rec := post(newService(store), signedTarget(body), body)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	if len(store.creds) != 1 {
		t.Fatalf("stored %d credentials, want 1", len(store.creds))
	}
	got := store.creds[0]
	if got.RefreshToken != "refresh-token" || got.AccessToken != "access-token" {
		t.Fatalf("tokens not passed through: %+v", got)
	}
	if got.SessionKey != "default" {
		t.Fatalf("session key = %q, want the default", got.SessionKey)
	}
	if !got.RefreshExpiresAt.Equal(time.Date(2026, 9, 22, 21, 3, 24, 0, time.UTC)) {
		t.Fatalf("refresh expiry = %v", got.RefreshExpiresAt)
	}
	// The acknowledgement is what lands in a run log; a token in it would leak.
	if strings.Contains(rec.Body.String(), "refresh-token") || strings.Contains(rec.Body.String(), "access-token") {
		t.Fatalf("acknowledgement leaked a token: %s", rec.Body.String())
	}
}

func TestPublishRejectsAnUnsignedRequest(t *testing.T) {
	store := &fakeStore{}
	body := validBody()

	rec := post(newService(store), Endpoint+"?content_sha256=deadbeef&exp=1&sig=nope", body)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	if len(store.creds) != 0 {
		t.Fatal("an unsigned request reached the store")
	}
}

func TestPublishRejectsABodyThatDoesNotMatchItsDeclaredSHA(t *testing.T) {
	store := &fakeStore{}
	body := validBody()
	target := signedTarget(body)

	rec := post(newService(store), target, []byte(`{"account":"someone-else","refresh_token":"x"}`))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if len(store.creds) != 0 {
		t.Fatal("a tampered body reached the store")
	}
}

func TestPublishRequiresTheRefreshToken(t *testing.T) {
	// The access token expires in 24h and the poller can always mint another,
	// but a credential without a refresh token is dead on arrival.
	store := &fakeStore{}
	body, _ := json.Marshal(publishRequest{Account: "zach@example.com", AccessToken: "access-only"})

	rec := post(newService(store), signedTarget(body), body)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestAMissingExpiryFallsBackToTheEpochSentinelRatherThanRejecting(t *testing.T) {
	store := &fakeStore{}
	body, _ := json.Marshal(publishRequest{
		Account: "zach@example.com", RefreshToken: "refresh-token", AccessExpiresAt: "not-a-time",
	})

	rec := post(newService(store), signedTarget(body), body)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !store.creds[0].AccessExpiresAt.Equal(time.Unix(0, 0).UTC()) {
		t.Fatalf("access expiry = %v, want the epoch sentinel", store.creds[0].AccessExpiresAt)
	}
}

func TestAStoreFailureIsReportedNotSwallowed(t *testing.T) {
	store := &fakeStore{err: errors.New("postgres is down")}
	body := validBody()

	rec := post(newService(store), signedTarget(body), body)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}
