package externalfeeds

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
)

type Item struct {
	Key     string
	Payload map[string]any
}
type memoryItem struct {
	payload []byte
	seq     uint64
}

// MemoryStore is used without a database. Replace projects a source snapshot,
// retaining arrival order across source insertions, revisions and deletions.
type MemoryStore struct {
	mu    sync.Mutex
	seq   uint64
	feeds map[string]map[string]memoryItem
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{feeds: map[string]map[string]memoryItem{}} }
func (s *MemoryStore) Replace(feed string, items []Item) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.feeds[feed]
	next := map[string]memoryItem{}
	for _, item := range items {
		b, _ := json.Marshal(item.Payload)
		v, ok := old[item.Key]
		if !ok || string(v.payload) != string(b) {
			s.seq++
			v = memoryItem{b, s.seq}
		}
		next[item.Key] = v
	}
	s.feeds[feed] = next
}
func (s *MemoryStore) Page(_ context.Context, feed Definition, since uint64, limit int) ([]map[string]any, uint64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var items []memoryItem
	for _, v := range s.feeds[feed.Scope] {
		if v.seq > since {
			items = append(items, v)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].seq < items[j].seq })
	more := len(items) > limit
	if more {
		items = items[:limit]
	}
	rows := []map[string]any{}
	for _, v := range items {
		var row map[string]any
		if err := json.Unmarshal(v.payload, &row); err != nil {
			return nil, since, false, err
		}
		rows = append(rows, row)
		since = v.seq
	}
	return rows, since, more, nil
}
