// Package mcpproxy connects owner-managed remote MCP servers to PDW. Upstream
// credentials are separate from PDW credentials and never reach downstream clients.
package mcpproxy

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
	"golang.org/x/oauth2"
)

type pendingAuth struct {
	State    string
	Browser  string
	Verifier string
	Expires  time.Time
	// Phone flows only: the app cannot set the browser cookie itself, so it
	// opens a one-time start link that sets it and forwards to AuthURL, and
	// the callback returns to the app instead of the web page.
	AuthURL string
	Handoff string
	Return  string
	// Loopback flows finish by pasting the redirect into the authenticated
	// API, which stands in for the browser cookie as the flow's binding.
	Loopback bool
}
type record struct {
	Generation string
	Name       string
	URL        string
	Enabled    bool
	AllClients bool
	Clients    []string
	// Loopback redirects OAuth to loopbackRedirectURL instead of PDW's
	// callback, for providers that accept only local clients' redirects.
	Loopback  bool
	Deleted   bool
	Version   int64
	Token     oauth2.Token
	OAuth     oauth2.Config
	Resource  string
	Issuer    string
	Pending   *pendingAuth
	Tools     []*mcp.Tool
	Status    string
	UpdatedAt time.Time
}

// Store serializes all changes to a connection, including rotating refresh
// tokens. Implementations must roll back when the callback returns an error.
type Store interface {
	List(context.Context) ([]record, error)
	Update(context.Context, string, func(*record) error) error
}
type PostgresStore struct {
	mu          sync.Mutex
	initialized bool
	schema      string
	relation    string
	db          *sql.DB
	// boxes[0] seals; every box opens, so a row sealed under a previous
	// master secret still reads during a rotation window.
	boxes []cipher.AEAD
}

// NewPostgresStore keeps connections sealed under secret. previous are
// master secrets being rotated out: rows sealed under them still open, and
// ResealUnderCurrentSecret moves them to secret.
func NewPostgresStore(databaseURL, secret string, previous ...string) (*PostgresStore, error) {
	if secret == "" {
		return nil, errors.New("MCP credential encryption requires the PDW app secret")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	boxes := []cipher.AEAD{newCipher(secret)}
	for _, p := range previous {
		if p != "" && p != secret {
			boxes = append(boxes, newCipher(p))
		}
	}
	return &PostgresStore{db: db, boxes: boxes, schema: warehouse.SchemaOf("mcp_connections"), relation: warehouse.SQLRelation("mcp_connections")}, nil
}
func (s *PostgresStore) Close() error { return s.db.Close() }
func newCipher(secret string) cipher.AEAD {
	key := sha256.Sum256([]byte("pdw/mcp-connections/v1\x00" + secret))
	block, _ := aes.NewCipher(key[:])
	box, _ := cipher.NewGCM(block)
	return box
}
func seal(box cipher.AEAD, id string, raw []byte) (string, error) {
	nonce := make([]byte, box.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.RawStdEncoding.EncodeToString(box.Seal(nonce, nonce, raw, []byte(id))), nil
}
func unseal(box cipher.AEAD, id, encoded string) ([]byte, error) {
	raw, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil || len(raw) < box.NonceSize() {
		return nil, errors.New("invalid encrypted MCP connection")
	}
	return box.Open(nil, raw[:box.NonceSize()], raw[box.NonceSize():], []byte(id))
}

// open unseals with the current box, then the previous ones, and reports
// whether the current box was the one that opened it.
func (s *PostgresStore) open(id, encoded string) ([]byte, bool, error) {
	var err error
	for i, box := range s.boxes {
		var raw []byte
		if raw, err = unseal(box, id, encoded); err == nil {
			return raw, i == 0, nil
		}
	}
	return nil, false, err
}

// ResealUnderCurrentSecret rewrites every connection still sealed under a
// previous master secret, so the previous secret can be dropped. It returns
// how many it rewrote.
func (s *PostgresStore) ResealUnderCurrentSecret(ctx context.Context) (int, error) {
	if len(s.boxes) < 2 {
		return 0, nil
	}
	if err := s.ensure(ctx); err != nil {
		return 0, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT name FROM `+s.relation+` WHERE payload <> '' ORDER BY name`)
	if err != nil {
		return 0, err
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return 0, err
		}
		names = append(names, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	resealed := 0
	for _, name := range names {
		changed := false
		if err := s.update(ctx, name, func(*record) error { return nil }, &changed); err != nil {
			return resealed, fmt.Errorf("reseal %s: %w", name, err)
		}
		if changed {
			resealed++
		}
	}
	return resealed, nil
}

func (s *PostgresStore) ensure(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.initialized {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS `+warehouse.QuoteIdent(s.schema)+`; CREATE TABLE IF NOT EXISTS `+s.relation+` (name text PRIMARY KEY, payload text NOT NULL DEFAULT '', updated_at timestamptz NOT NULL DEFAULT now()); REVOKE ALL ON `+s.relation+` FROM PUBLIC`)
	if err == nil {
		s.initialized = true
	}
	return err
}
func (s *PostgresStore) List(ctx context.Context) ([]record, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT name,payload FROM `+s.relation+` ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []record{}
	for rows.Next() {
		var id, payload string
		if err := rows.Scan(&id, &payload); err != nil {
			return nil, err
		}
		if payload == "" {
			continue
		}
		raw, _, err := s.open(id, payload)
		if err != nil {
			return nil, err
		}
		var c record
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		if !c.Deleted {
			result = append(result, c)
		}
	}
	return result, rows.Err()
}
func (s *PostgresStore) Update(ctx context.Context, id string, fn func(*record) error) error {
	return s.update(ctx, id, fn, nil)
}

// update runs fn on the connection under its row lock and writes it back if
// fn changed it or it is still sealed under a previous secret. written, when
// set, reports whether a row was written.
func (s *PostgresStore) update(ctx context.Context, id string, fn func(*record) error, written *bool) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if err := s.ensure(ctx); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO `+s.relation+` (name) VALUES ($1) ON CONFLICT DO NOTHING`, id); err != nil {
		return err
	}
	var payload string
	if err = tx.QueryRowContext(ctx, `SELECT payload FROM `+s.relation+` WHERE name=$1 FOR UPDATE`, id).Scan(&payload); err != nil {
		return err
	}
	c := record{Name: id}
	sealedCurrent := true
	if payload != "" {
		raw, current, err := s.open(id, payload)
		sealedCurrent = current
		if err != nil {
			return err
		}
		if err = json.Unmarshal(raw, &c); err != nil {
			return err
		}
	}
	before, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if err = fn(&c); err != nil {
		return err
	}
	after, err := json.Marshal(c)
	if err != nil {
		return err
	}
	unchanged := bytes.Equal(before, after) && payload != ""
	if unchanged && sealedCurrent {
		return tx.Commit()
	}
	if !unchanged {
		// A re-encryption alone is not a change to the connection.
		c.Version++
		c.UpdatedAt = time.Now().UTC()
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}
	payload, err = seal(s.boxes[0], id, raw)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE `+s.relation+` SET payload=$2,updated_at=now() WHERE name=$1`, id, payload); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if written != nil {
		*written = true
	}
	return nil
}
