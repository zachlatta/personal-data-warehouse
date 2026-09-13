package mcpproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"time"
)

type memoryStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemoryStore() *memoryStore { return &memoryStore{data: map[string][]byte{}} }
func (s *memoryStore) List(context.Context) ([]record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []record{}
	for _, raw := range s.data {
		var c record
		_ = json.Unmarshal(raw, &c)
		if !c.Deleted {
			out = append(out, c)
		}
	}
	return out, nil
}
func (s *memoryStore) Update(ctx context.Context, id string, fn func(*record) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := record{Name: id}
	if raw := s.data[id]; raw != nil {
		_ = json.Unmarshal(raw, &c)
	}
	before, _ := json.Marshal(c)
	if err := fn(&c); err != nil {
		return err
	}
	after, _ := json.Marshal(c)
	if bytes.Equal(before, after) && s.data[id] != nil {
		return nil
	}
	c.Version++
	c.UpdatedAt = time.Now()
	raw, err := json.Marshal(c)
	if err == nil {
		s.data[id] = raw
	}
	return err
}
