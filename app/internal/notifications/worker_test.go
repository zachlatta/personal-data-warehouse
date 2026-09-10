package notifications

import (
	"testing"
	"time"
)

func TestRetryBoundAndOpenProof(t *testing.T) {
	if retryDelay(1) != 10*time.Second || retryDelay(6) > 10*time.Minute {
		t.Fatal("unbounded retry")
	}
	s := &Service{Secret: []byte("test-secret")}
	proof := s.openProof("one")
	if !s.validProof("one", proof) || s.validProof("two", proof) || s.validProof("one", "") {
		t.Fatal("open proof is not scoped")
	}
}
