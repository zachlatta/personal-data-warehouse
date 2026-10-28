// Package push delivers notifications to the PDW iOS app through the Expo
// push service and keeps the registry of devices that can receive them.
//
// The device table is private.push_devices; the Python warehouse creates the
// same table in ensure_upstream_mutation_tables, and the CREATE here is the
// idempotent twin so the first registration (before any Dagster run) succeeds.
package push

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

const (
	StatusActive   = "active"
	StatusDisabled = "disabled"
)

// Device is one registered app install.
type Device struct {
	ExpoPushToken string `json:"expo_push_token"`
	ClientName    string `json:"client_name"`
	DeviceName    string `json:"device_name"`
	Platform      string `json:"platform"`
	AppVersion    string `json:"app_version"`
	// Capabilities are what this install's native code can render (see
	// CapabilityAndroidMessagingStyle). They are replaced, never merged, on
	// every registration: a phone rolled back to an older build re-registers
	// without them and gets the alerts that build can show.
	Capabilities []string  `json:"capabilities"`
	Status       string    `json:"status"`
	Error        string    `json:"error,omitempty"`
	RegisteredAt time.Time `json:"registered_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Store is the device registry.
type Store interface {
	Register(ctx context.Context, device Device, now time.Time) (Device, error)
	ListActive(ctx context.Context) ([]Device, error)
	// MarkSent records a successful delivery; MarkFailed records the provider's
	// reason and, when disable is set, retires the device from future fan-out.
	MarkSent(ctx context.Context, token string, now time.Time) error
	MarkFailed(ctx context.Context, token string, reason string, disable bool, now time.Time) error
}

var ErrInvalidToken = errors.New("expo push token must look like ExponentPushToken[...]")

// ValidateToken accepts the two shapes Expo issues.
func ValidateToken(token string) error {
	token = strings.TrimSpace(token)
	for _, prefix := range []string{"ExponentPushToken[", "ExpoPushToken["} {
		if strings.HasPrefix(token, prefix) && strings.HasSuffix(token, "]") && len(token) > len(prefix)+1 {
			return nil
		}
	}
	return ErrInvalidToken
}

// PostgresStore is the production Store.
type PostgresStore struct {
	db *sql.DB
	// Expand is overridden only by integration tests with isolated schemas.
	Expand   func(string) string
	timeout  time.Duration
	ensureMu sync.Mutex
	ensured  bool
}

func NewPostgresStore(databaseURL string, timeout time.Duration) (*PostgresStore, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{db: db, timeout: timeout}, nil
}

func (s *PostgresStore) Close() error { return s.db.Close() }

var createSchemaSQL = "CREATE SCHEMA IF NOT EXISTS " +
	warehouse.QuoteIdent(warehouse.SchemaOf("push_devices"))

var ensureStatements = []string{
	`CREATE TABLE IF NOT EXISTS @push_devices (
		expo_push_token text PRIMARY KEY,
		client_name text NOT NULL DEFAULT '',
		device_name text NOT NULL DEFAULT '',
		platform text NOT NULL DEFAULT '',
		app_version text NOT NULL DEFAULT '',
		status text NOT NULL DEFAULT 'active',
		error text NOT NULL DEFAULT '',
		registered_at timestamptz NOT NULL DEFAULT now(),
		updated_at timestamptz NOT NULL DEFAULT now(),
		last_sent_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
		last_error_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
		capabilities text NOT NULL DEFAULT ''
	)`,
	// CREATE TABLE IF NOT EXISTS leaves a table created before the column alone.
	`ALTER TABLE @push_devices ADD COLUMN IF NOT EXISTS capabilities text NOT NULL DEFAULT ''`,
	`CREATE INDEX IF NOT EXISTS push_devices_status_updated_idx ON @push_devices (status, updated_at)`,
}

func (s *PostgresStore) ensure(ctx context.Context) error {
	s.ensureMu.Lock()
	defer s.ensureMu.Unlock()
	if s.ensured {
		return nil
	}
	if s.Expand == nil {
		if _, err := s.db.ExecContext(ctx, createSchemaSQL); err != nil {
			return fmt.Errorf("ensure private schema: %w", err)
		}
	}
	for _, statement := range ensureStatements {
		if _, err := s.db.ExecContext(ctx, s.q(statement)); err != nil {
			return fmt.Errorf("ensure push_devices: %w", err)
		}
	}
	s.ensured = true
	return nil
}

func (s *PostgresStore) q(statement string) string {
	if s.Expand != nil {
		return s.Expand(statement)
	}
	return warehouse.ExpandRelations(statement)
}

func (s *PostgresStore) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.timeout)
}

// Register upserts a device. Re-registering a disabled token reactivates it:
// the app only calls this with a token the OS just handed it, which is the
// strongest evidence available that the device is reachable again.
func (s *PostgresStore) Register(ctx context.Context, device Device, now time.Time) (Device, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.ensure(ctx); err != nil {
		return Device{}, err
	}
	if err := ValidateToken(device.ExpoPushToken); err != nil {
		return Device{}, err
	}
	now = now.UTC()
	row := s.db.QueryRowContext(ctx, s.q(`
		INSERT INTO @push_devices (expo_push_token, client_name, device_name, platform, app_version, capabilities, status, error, registered_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $7, 'active', '', $6, $6)
		ON CONFLICT (expo_push_token) DO UPDATE SET
			client_name = EXCLUDED.client_name,
			device_name = EXCLUDED.device_name,
			platform = EXCLUDED.platform,
			app_version = EXCLUDED.app_version,
			capabilities = EXCLUDED.capabilities,
			status = 'active',
			error = '',
			updated_at = EXCLUDED.updated_at
		RETURNING expo_push_token, client_name, device_name, platform, app_version, capabilities, status, error, registered_at, updated_at`),
		strings.TrimSpace(device.ExpoPushToken), device.ClientName, device.DeviceName, device.Platform, device.AppVersion, now,
		strings.Join(NormalizeCapabilities(device.Capabilities), ","))
	return scanDevice(row)
}

func (s *PostgresStore) ListActive(ctx context.Context) ([]Device, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, s.q(`
		SELECT expo_push_token, client_name, device_name, platform, app_version, capabilities, status, error, registered_at, updated_at
		FROM @push_devices WHERE status = 'active' ORDER BY registered_at, expo_push_token`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	devices := []Device{}
	for rows.Next() {
		device, err := scanDevice(rows)
		if err != nil {
			return nil, err
		}
		devices = append(devices, device)
	}
	return devices, rows.Err()
}

func (s *PostgresStore) MarkSent(ctx context.Context, token string, now time.Time) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	_, err := s.db.ExecContext(ctx, s.q(
		`UPDATE @push_devices SET last_sent_at = $2, updated_at = $2 WHERE expo_push_token = $1`), token, now.UTC())
	return err
}

func (s *PostgresStore) MarkFailed(ctx context.Context, token string, reason string, disable bool, now time.Time) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	status := StatusActive
	if disable {
		status = StatusDisabled
	}
	_, err := s.db.ExecContext(ctx, s.q(
		`UPDATE @push_devices SET error = $2, status = $3, last_error_at = $4, updated_at = $4 WHERE expo_push_token = $1`),
		token, reason, status, now.UTC())
	return err
}

type scanner interface{ Scan(dest ...any) error }

func scanDevice(row scanner) (Device, error) {
	var d Device
	var capabilities string
	if err := row.Scan(&d.ExpoPushToken, &d.ClientName, &d.DeviceName, &d.Platform, &d.AppVersion, &capabilities, &d.Status, &d.Error, &d.RegisteredAt, &d.UpdatedAt); err != nil {
		return Device{}, err
	}
	d.Capabilities = ParseCapabilities(capabilities)
	return d, nil
}

// MemoryStore is the in-process Store used by tests and by deployments without
// Postgres (where registrations are accepted but never survive a restart).
type MemoryStore struct {
	mu      sync.Mutex
	devices map[string]Device
	Sent    map[string]time.Time
	Failed  map[string]string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{devices: map[string]Device{}, Sent: map[string]time.Time{}, Failed: map[string]string{}}
}

func (m *MemoryStore) Register(_ context.Context, device Device, now time.Time) (Device, error) {
	if err := ValidateToken(device.ExpoPushToken); err != nil {
		return Device{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	device.ExpoPushToken = strings.TrimSpace(device.ExpoPushToken)
	existing, ok := m.devices[device.ExpoPushToken]
	if ok {
		device.RegisteredAt = existing.RegisteredAt
	} else {
		device.RegisteredAt = now
	}
	device.Status, device.Error, device.UpdatedAt = StatusActive, "", now
	device.Capabilities = NormalizeCapabilities(device.Capabilities)
	m.devices[device.ExpoPushToken] = device
	return device, nil
}

func (m *MemoryStore) ListActive(context.Context) ([]Device, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Device{}
	for _, d := range m.devices {
		if d.Status == StatusActive {
			out = append(out, d)
		}
	}
	// Same order as the Postgres store, so tickets line up predictably.
	sort.Slice(out, func(i, j int) bool {
		if !out[i].RegisteredAt.Equal(out[j].RegisteredAt) {
			return out[i].RegisteredAt.Before(out[j].RegisteredAt)
		}
		return out[i].ExpoPushToken < out[j].ExpoPushToken
	})
	return out, nil
}

func (m *MemoryStore) MarkSent(_ context.Context, token string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Sent[token] = now
	return nil
}

func (m *MemoryStore) MarkFailed(_ context.Context, token string, reason string, disable bool, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Failed[token] = reason
	if d, ok := m.devices[token]; ok {
		d.Error, d.UpdatedAt = reason, now
		if disable {
			d.Status = StatusDisabled
		}
		m.devices[token] = d
	}
	return nil
}

// Get is a test helper.
func (m *MemoryStore) Get(token string) (Device, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.devices[token]
	return d, ok
}
