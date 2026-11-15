package mutations

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// The ladder is one contract with Python's derive_request_status: both read
// tests/contracts/request_status_ladder.json.
func TestDeriveRequestStatusMatchesTheSharedLadder(t *testing.T) {
	raw, err := os.ReadFile("../../../tests/contracts/request_status_ladder.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract struct {
		Cases []struct {
			Statuses []string `json:"statuses"`
			Want     string   `json:"want"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatal(err)
	}
	for _, c := range contract.Cases {
		if got := DeriveRequestStatus(c.Statuses, "current"); got != c.Want {
			t.Fatalf("%s: got %q want %q", strings.Join(c.Statuses, "+"), got, c.Want)
		}
	}
}
