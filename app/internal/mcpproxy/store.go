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
}
type record struct {
	Generation string
	Name       string
	URL        string
	Enabled    bool
	AllClients bool
	Clients    []string
	Deleted    bool
	Version    int64
	Token      oauth2.Token
	OAuth      oauth2.Config
	Resource   string
	Issuer     string
	Pending    *pendingAuth
	Tools      []*mcp.Tool
	Status     string
	UpdatedAt  time.Time
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
	box         cipher.AEAD
}

func NewPostgresStore(databaseURL, secret string) (*PostgresStore, error) {
	if secret == "" {
		return nil, errors.New("MCP credential encryption requires the PDW app secret")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{db: db, box: newCipher(secret), schema: warehouse.SchemaOf("mcp_connections"), relation: warehouse.SQLRelation("mcp_connections")}, nil
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
		raw, err := unseal(s.box, id, payload)
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
	if payload != "" {
		raw, err := unseal(s.box, id, payload)
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
	if bytes.Equal(before, after) && payload != "" {
		return tx.Commit()
	}
	c.Version++
	c.UpdatedAt = time.Now().UTC()
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}
	payload, err = seal(s.box, id, raw)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE `+s.relation+` SET payload=$2,updated_at=$3 WHERE name=$1`, id, payload, c.UpdatedAt); err != nil {
		return err
	}
	return tx.Commit()
}
