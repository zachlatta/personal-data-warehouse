package accesstokens

import (
	"context"
	"sort"
	"sync"
	"time"
)

type memoryRow struct {
	token Token
	hash  string
}

// MemoryStore is the Store for tests and for a deployment without Postgres
// (where issued tokens then live only as long as the process).
type MemoryStore struct {
	mu      sync.Mutex
	rows    map[string]memoryRow
	touches int
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{rows: map[string]memoryRow{}} }

func (m *MemoryStore) Insert(_ context.Context, token Token, hash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows[token.ID] = memoryRow{token: token, hash: hash}
	return nil
}

func (m *MemoryStore) Get(_ context.Context, id string) (Token, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.rows[id]
	if !ok {
		return Token{}, "", ErrNotFound
	}
	return row.token, row.hash, nil
}

func (m *MemoryStore) Touch(_ context.Context, id string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.rows[id]
	if !ok {
		return ErrNotFound
	}
	if at.After(row.token.LastUsedAt) {
		row.token.LastUsedAt = at
		m.rows[id] = row
		m.touches++
	}
	return nil
}

func (m *MemoryStore) List(context.Context) ([]Token, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Token, 0, len(m.rows))
	for _, row := range m.rows {
		out = append(out, row.token)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func (m *MemoryStore) Revoke(_ context.Context, id string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.rows[id]
	if !ok {
		return ErrNotFound
	}
	if row.token.RevokedAt.IsZero() {
		row.token.RevokedAt = at
		m.rows[id] = row
	}
	return nil
}

func (m *MemoryStore) Redeem(_ context.Context, codeID string, mint func(Token, string) (Token, error), mintedHash string, at time.Time) (Token, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.rows[codeID]
	if !ok {
		return Token{}, ErrNotFound
	}
	minted, err := mint(row.token, row.hash)
	if err != nil {
		return Token{}, err
	}
	m.rows[minted.ID] = memoryRow{token: minted, hash: mintedHash}
	row.token.RedeemedAt = at
	row.token.RedeemedTokenID = minted.ID
	m.rows[codeID] = row
	return minted, nil
}
