package plaid

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// LinkedItem is the Go twin of PlaidLinkedItem: one row of the private Item
// token table.
type LinkedItem struct {
	Account         string
	ItemID          string
	AccessToken     string
	InstitutionID   string
	InstitutionName string
}

// ItemAccount is one account an Item reports, for operator-facing output.
type ItemAccount struct {
	AccountID      string
	Name           string
	Mask           string
	Type           string
	Subtype        string
	CurrentBalance float64
	IsRemoved      int64
}

// ItemScopedTables lists, by logical id, every table keyed by (account,
// item_id) — what `unlink` counts and deletes (PLAID_ITEM_SCOPED_TABLES).
// plaid_investment_securities is deliberately absent: it is keyed by account,
// not item, and is shared across Items.
var ItemScopedTables = []string{
	"plaid_items",
	"plaid_accounts",
	"plaid_transactions",
	"plaid_investment_holdings",
	"plaid_investment_transactions",
	"plaid_liabilities",
	"plaid_sync_state",
	"plaid_item_tokens",
}

// Store is the warehouse access the commands need, behind an interface so
// unit tests run against a fake rather than a live database.
type Store interface {
	// EnsurePlaidTables verifies the Plaid relations exist. The Python CLI
	// created them; the Go port expects the warehouse (Dagster / schema
	// provisioning) to have done so and fails clearly otherwise.
	EnsurePlaidTables(ctx context.Context) error
	LoadItemTokens(ctx context.Context) ([]LinkedItem, error)
	UpsertItemToken(ctx context.Context, item LinkedItem, linkedAt time.Time) error
	LoadItemAccounts(ctx context.Context, account, itemID string) ([]ItemAccount, error)
	CountItemRows(ctx context.Context, account, itemID string) (map[string]int64, error)
	DeleteItem(ctx context.Context, account, itemID string) (map[string]int64, error)
	Close() error
}

// PostgresStore is the production Store over the warehouse Postgres.
type PostgresStore struct {
	db *sql.DB
}

// OpenPostgres opens a lazy pool on databaseURL.
func OpenPostgres(databaseURL string) (*PostgresStore, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) Close() error { return s.db.Close() }

func (s *PostgresStore) EnsurePlaidTables(ctx context.Context) error {
	for _, logical := range ItemScopedTables {
		var found sql.NullString
		display := warehouse.DisplayRelation(logical)
		if err := s.db.QueryRowContext(ctx, "SELECT to_regclass($1)::text", display).Scan(&found); err != nil {
			return err
		}
		if !found.Valid || found.String == "" {
			return fmt.Errorf(
				"warehouse relation %s does not exist; provision the Plaid tables from the Python warehouse (ensure_plaid_tables) before using pdw ingest plaid",
				display,
			)
		}
	}
	return nil
}

func (s *PostgresStore) LoadItemTokens(ctx context.Context) ([]LinkedItem, error) {
	rows, err := s.db.QueryContext(ctx, warehouse.ExpandRelations(`
		SELECT account, item_id, access_token, institution_id, institution_name
		FROM @plaid_item_tokens
		ORDER BY account, institution_name, item_id`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []LinkedItem
	for rows.Next() {
		var item LinkedItem
		if err := rows.Scan(&item.Account, &item.ItemID, &item.AccessToken, &item.InstitutionID, &item.InstitutionName); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *PostgresStore) UpsertItemToken(ctx context.Context, item LinkedItem, linkedAt time.Time) error {
	linkedAt = linkedAt.UTC()
	syncVersion := linkedAt.UnixMicro()
	_, err := s.db.ExecContext(ctx, warehouse.ExpandRelations(`
		INSERT INTO @plaid_item_tokens AS target
			(account, item_id, access_token, institution_id, institution_name, linked_at, updated_at, sync_version)
		VALUES ($1, $2, $3, $4, $5, $6, $6, $7)
		ON CONFLICT (account, item_id) DO UPDATE SET
			access_token = EXCLUDED.access_token,
			institution_id = EXCLUDED.institution_id,
			institution_name = EXCLUDED.institution_name,
			linked_at = EXCLUDED.linked_at,
			updated_at = EXCLUDED.updated_at,
			sync_version = EXCLUDED.sync_version
		WHERE target.sync_version <= EXCLUDED.sync_version`),
		item.Account, item.ItemID, item.AccessToken, item.InstitutionID, item.InstitutionName, linkedAt, syncVersion,
	)
	return err
}

func (s *PostgresStore) LoadItemAccounts(ctx context.Context, account, itemID string) ([]ItemAccount, error) {
	rows, err := s.db.QueryContext(ctx, warehouse.ExpandRelations(`
		SELECT account_id, name, mask, type, subtype, current_balance, is_removed
		FROM @plaid_accounts
		WHERE account = $1 AND item_id = $2
		ORDER BY mask, account_id`), account, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var accounts []ItemAccount
	for rows.Next() {
		var row ItemAccount
		var balance sql.NullFloat64
		var removed sql.NullInt64
		if err := rows.Scan(&row.AccountID, &row.Name, &row.Mask, &row.Type, &row.Subtype, &balance, &removed); err != nil {
			return nil, err
		}
		row.CurrentBalance = balance.Float64
		row.IsRemoved = removed.Int64
		accounts = append(accounts, row)
	}
	return accounts, rows.Err()
}

func (s *PostgresStore) CountItemRows(ctx context.Context, account, itemID string) (map[string]int64, error) {
	counts := map[string]int64{}
	for _, logical := range ItemScopedTables {
		var count int64
		statement := "SELECT count(*) FROM " + warehouse.SQLRelation(logical) + " WHERE account = $1 AND item_id = $2"
		if err := s.db.QueryRowContext(ctx, statement, account, itemID).Scan(&count); err != nil {
			return nil, err
		}
		counts[logical] = count
	}
	return counts, nil
}

// DeleteItem deletes every row belonging to one Item in a single statement,
// so a partial delete cannot leave a half-retired Item behind.
func (s *PostgresStore) DeleteItem(ctx context.Context, account, itemID string) (map[string]int64, error) {
	var deletes, selects []string
	var args []any
	for i, logical := range ItemScopedTables {
		alias := warehouse.QuoteIdent("d_" + logical)
		deletes = append(deletes, fmt.Sprintf(
			"%s AS (DELETE FROM %s WHERE account = $%d AND item_id = $%d RETURNING 1)",
			alias, warehouse.SQLRelation(logical), 2*i+1, 2*i+2,
		))
		selects = append(selects, "(SELECT count(*) FROM "+alias+")")
		args = append(args, account, itemID)
	}
	statement := "WITH " + strings.Join(deletes, ", ") + " SELECT " + strings.Join(selects, ", ")
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("delete returned no row")
	}
	values := make([]int64, len(ItemScopedTables))
	targets := make([]any, len(values))
	for i := range values {
		targets[i] = &values[i]
	}
	if err := rows.Scan(targets...); err != nil {
		return nil, err
	}
	deleted := map[string]int64{}
	for i, logical := range ItemScopedTables {
		deleted[logical] = values[i]
	}
	return deleted, rows.Err()
}
