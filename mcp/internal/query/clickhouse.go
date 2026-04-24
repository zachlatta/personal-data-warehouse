package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHouseRunner struct {
	db *sql.DB
}

func NewClickHouseRunner(clickhouseURL string, timeout time.Duration) (*ClickHouseRunner, error) {
	opts, err := clickhouse.ParseDSN(clickhouseURL)
	if err != nil {
		return nil, err
	}
	if timeout > 0 {
		opts.DialTimeout = timeout
		opts.ReadTimeout = timeout
	}
	db := clickhouse.OpenDB(opts)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	if timeout <= 0 {
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	}
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &ClickHouseRunner{db: db}, nil
}

func (r *ClickHouseRunner) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

func (r *ClickHouseRunner) Query(ctx context.Context, statement string, maxRows int) (RawResult, error) {
	rows, err := r.db.QueryContext(ctx, statement)
	if err != nil {
		return RawResult{}, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return RawResult{}, err
	}
	result := RawResult{Columns: columns}
	for rows.Next() {
		if maxRows > 0 && len(result.Rows) >= maxRows {
			break
		}
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return RawResult{}, err
		}
		row := make(map[string]any, len(columns))
		for i, column := range columns {
			row[column] = normalizeValue(values[i])
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return RawResult{}, err
	}
	return result, nil
}

func normalizeValue(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return string(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	}
	if _, err := json.Marshal(value); err == nil {
		return value
	}
	return fmt.Sprint(value)
}
