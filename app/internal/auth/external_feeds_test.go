package auth

import (
	"github.com/zachlatta/personal-data-warehouse/app/internal/externalfeeds"
	"testing"
)

func TestRegisteredFeedScopesAreConfinedToTheirEndpoints(t *testing.T) {
	for _, feed := range externalfeeds.All() {
		scope := Scope(feed.Scope)
		p := Principal{Scope: scope}
		if !scope.Issuable() || !p.Allows(feed.Path) {
			t.Fatalf("registered feed unavailable: %s", feed.Scope)
		}
		for _, path := range []string{"/api/tools", "/api/auth/tokens", "/api/external-feeds", feed.Path + "/other"} {
			if p.Allows(path) {
				t.Fatalf("%s allowed %s", feed.Scope, path)
			}
		}
	}
	if Scope("feed:unknown").Issuable() {
		t.Fatal("unregistered feed scope accepted")
	}
}
