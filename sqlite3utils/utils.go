package sqlite3utils

import (
	"context"
	"database/sql"
	"github.com/skypbc/dbx"
	"github.com/skypbc/goutils/gfmt"
	"strings"
)

func CreateIndexMany(ctx context.Context, db dbx.IDB, table string, fields ...[]string) error {
	if len(fields) == 0 {
		return nil
	}
	for _, item := range fields {
		if err := CreateIndex(ctx, db, table, item...); err != nil {
			return err
		}
	}
	return nil
}

func CreateIndex(ctx context.Context, db dbx.IDB, table string, fields ...string) error {
	fieldsName := strings.Join(fields, ",")
	indexName := strings.Join(fields, "_")

	query := gfmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_%s_idx ON %s (%s)", table, indexName, table, fieldsName)

	if _, err := db.Exec(ctx, query, nil); err != nil {
		return err
	}

	return nil
}

func DropAllTable(ctx context.Context, db dbx.IDB) error {
	query := "SELECT name FROM sqlite_master WHERE type is 'table'"

	var tables []string
	if err := db.Query(ctx, query, nil, func(rows *sql.Rows) (err error) {
		var table string

		for rows.Next() {
			err := rows.Scan(&table)
			if err != nil {
				return err
			}
			tables = append(tables, table)
		}
		return nil
	}); err != nil {
		return err
	}

	for _, table := range tables {
		query := gfmt.Sprintf("DROP TABLE IF EXISTS %s", table)
		if _, err := db.Exec(ctx, query, nil); err != nil {
			return err
		}
	}

	return nil
}
