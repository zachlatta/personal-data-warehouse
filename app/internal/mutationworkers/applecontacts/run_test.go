package applecontacts

import (
	"context"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

type ledgerStore struct {
	queue.Store
	asked []string
}

func (s *ledgerStore) AppleContactsMergedCardTarget(_ context.Context, cardID string) (string, error) {
	s.asked = append(s.asked, cardID)
	return "kept", nil
}

func TestTheDefaultExecutorResolvesMergedCardsThroughTheLedger(t *testing.T) {
	store := &ledgerStore{}
	executor, ok := Spec.NewExecutor(store).(*Executor)
	if !ok || executor.mergedInto == nil {
		t.Fatalf("executor = %#v", executor)
	}
	target, err := executor.mergedInto("anything")
	if err != nil || target != "kept" || len(store.asked) != 1 || store.asked[0] != "anything" {
		t.Fatalf("target = %q err = %v asked = %v", target, err, store.asked)
	}
}

func TestTheRealLedgerStoreSatisfiesTheResolver(t *testing.T) {
	var _ mergedCardLedger = (*queue.PostgresStore)(nil)
}
