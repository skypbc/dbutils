package pgutils

import (
	"context"
	"database/sql"
	"github.com/skypbc/dbx"
	"github.com/skypbc/goutils/gerrors"
	"github.com/skypbc/goutils/gfmt"
	"strings"
)

func DropAllTable(ctx context.Context, db dbx.IDB, schemas ...string) error {
	query := `SELECT 'DROP TABLE IF EXISTS "' || tablename || '" CASCADE;' FROM pg_tables WHERE schemaname=?;`

	if len(schemas) == 0 {
		schemas = append(schemas, "public")
	}

	var tables []string
	for _, schema := range schemas {
		data := []any{schema}
		db.Query(ctx, query, data, func(rows *sql.Rows) (err error) { //nolint:errcheck
			var table string

			for rows.Next() {
				err := rows.Scan(&table)
				if err != nil {
					return err
				}
				tables = append(tables, table)
			}
			return nil
		})
	}

	for _, query := range tables {
		if _, err := db.Exec(ctx, query, nil); err != nil {
			return err
		}
	}

	return nil
}

func DropTable(ctx context.Context, db dbx.IDB, qname ...string) error {
	if len(qname) == 0 {
		return nil
	}
	var query string
	for _, qn := range qname {
		schema, table := SplitQName(qn)
		query = gfmt.Sprintf("DROP TABLE IF EXISTS %s.%s CASCADE", schema, table)
		if _, err := db.Exec(ctx, query, nil); err != nil {
			return err
		}
	}
	return nil
}

// Очистка DELETE FROM
func DeleteAll(ctx context.Context, db dbx.IDB, qname ...string) error {
	if len(qname) == 0 {
		return nil
	}
	for _, qn := range qname {
		schema, table := SplitQName(qn)
		exists, err := TableExists(ctx, db, table, schema)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		query := gfmt.Sprintf(`DELETE FROM %s.%s;`, schema, table)
		if _, err = db.Exec(ctx, query, nil); err != nil {
			return err
		}
	}
	return nil
}

// Очистка TRUNCATE (быстрее, чем DELETE)
func TruncateCascade(ctx context.Context, db dbx.IDB, qname ...string) error {
	if len(qname) == 0 {
		return nil
	}
	for _, qn := range qname {
		schema, table := SplitQName(qn)
		exists, err := TableExists(ctx, db, table, schema)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		query := gfmt.Sprintf(`TRUNCATE TABLE %s.%s CASCADE;`, schema, table)
		if _, err = db.Exec(ctx, query, nil); err != nil {
			return err
		}
	}
	return nil
}

func TruncateCascadeAndRestartIdentity(ctx context.Context, db dbx.IDB, qname ...string) error {
	if len(qname) == 0 {
		return nil
	}
	for _, qn := range qname {
		schema, table := SplitQName(qn)
		exists, err := TableExists(ctx, db, table, schema)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		query := gfmt.Sprintf(`TRUNCATE TABLE %s.%s RESTART IDENTITY CASCADE;`, schema, table)
		if _, err = db.Exec(ctx, query, nil); err != nil {
			return err
		}
	}
	return nil
}

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

type CreateDeleteConstraintArgs struct {
	Table    string
	Field    string
	RefTable string
	RefField string
}

func CreateDeleteConstraint(ctx context.Context, db dbx.IDB,
	args CreateDeleteConstraintArgs,
) error {
	query := gfmt.Sprintf(`
		ALTER TABLE 
			%s 
		ADD CONSTRAINT 
			%s_%s_fkey
		FOREIGN KEY (%s) 
		REFERENCES %s(%s) 
		ON DELETE CASCADE
	`,
		args.Table,
		args.Table, args.Field,
		args.Field,
		args.RefTable, args.RefField,
	)
	if _, err := db.Exec(ctx, query, nil); err != nil {
		return err
	}

	return nil
}

func ParseStringArray(s string) []string {
	s = strings.Trim(s, "{}")
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

// split "schema.table" -> ("schema","table"), default schema = public
func SplitQName(q string) (schema, table string) {
	if i := strings.IndexByte(q, '.'); i >= 0 {
		return q[:i], q[i+1:]
	}
	return "public", q
}

// Проверка существования таблицы (универсальная)
func TableExists(ctx context.Context, db dbx.IDB, tableName string, schema ...string) (exists bool, err error) {
	var sch string
	if len(schema) > 0 {
		sch = schema[0]
	} else {
		sch = "public"
	}
	query := gfmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s
		)`,
		db.Escape(sch), db.Escape(tableName),
	)
	if err := db.QueryRow(ctx, query, nil, func(row *sql.Row) (err error) {
		return row.Scan(&exists)
	}); err != nil {
		return false, gerrors.Wrap(err).
			SetTemplate(`failed to check if table "{schema}.{table}" exists`).
			AddStr("schema", sch).
			AddStr("table", tableName)
	}
	return exists, nil
}

// Проверка существования таблицы (pg-only)
func TableExists2(ctx context.Context, db dbx.IDB, tableName string, schema ...string) (bool, error) {
	var exists bool

	var sch string
	if len(schema) > 0 {
		sch = schema[0]
	} else {
		sch = "public"
	}

	query := gfmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname=%s AND tablename=%s
		);
	`, db.Escape(sch), db.Escape(tableName))

	err := db.QueryRow(ctx, query, nil, func(row *sql.Row) (err error) {
		return row.Scan(&exists)
	})

	return exists, err
}
