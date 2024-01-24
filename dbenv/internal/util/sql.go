package util

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/quenbyako/ext/slices"
	
	"github.com/quenbyako/sqltest/dbenv"
)

type Tx interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

var _ Tx = (*sql.DB)(nil)

type ColScanner interface {
	Columns() ([]string, error)
	Scan(dest ...any) error
}

var _ ColScanner = (*sql.Rows)(nil)

func MapScan(r ColScanner) (map[string]driver.Value, error) {
	// ignore r.started, since we needn't use reflect for anything.
	columns, err := r.Columns()
	if err != nil {
		return nil, err
	}

	values := slices.Generate(len(columns), func(int) any { return new(any) })
	if err = r.Scan(values...); err != nil {
		return nil, err
	}

	dest := make(map[string]driver.Value, len(values))
	for i, column := range columns {
		dest[column] = *(values[i].(*any))
	}

	return dest, nil
}

const allTablesQuery = `
SELECT
	tables.table_schema,
    tables.table_name,
    constraints.constraint_name,
    string_agg(columns.column_name, ', ') AS key_columns
FROM information_schema.tables AS tables
LEFT JOIN information_schema.table_constraints constraints ON
	constraints.table_schema = tables.table_schema AND
    constraints.table_name = tables.table_name AND
    constraints.constraint_type = 'PRIMARY KEY'
LEFT JOIN information_schema.key_column_usage columns ON
    columns.constraint_name = constraints.constraint_name AND
    columns.constraint_schema = constraints.constraint_schema AND
    columns.constraint_name = constraints.constraint_name
WHERE
	tables.table_schema NOT IN ('pg_catalog', 'information_schema') AND
    tables.table_type = 'BASE TABLE'
GROUP BY
	tables.table_schema,
    tables.table_name,
    constraints.constraint_name
ORDER BY
	tables.table_schema,
    tables.table_name
`

type tableInfo struct {
	Schema      string   `db:"table_schema"`
	Name        string   `db:"table_name"`
	PrimaryKeys []string `db:"primary_keys"`
}

const tableColumnsQuery = `
SELECT column_name::text, data_type::text, udt_name::text
FROM information_schema.columns WHERE table_name = $1
ORDER BY ordinal_position
`

type tableColumnsRow struct {
	ColumnName string `db:"column_name"`
	Type       string `db:"data_type"`
	UDTName    string `db:"udt_name"`
}

func GetAllSchemaTables(ctx context.Context, tx Tx) (map[string]dbenv.TableSchema, error) {
	rows, err := tx.QueryContext(ctx, allTablesQuery)
	if err != nil {
		panic(err)
	}

	tables := []tableInfo{}
	for rows.Next() {
		var i tableInfo
		if err := rows.Scan(&i.Schema, &i.Name, &i.PrimaryKeys); err != nil {
			return nil, err
		}
		tables = append(tables, i)
	}

	res := make(map[string]dbenv.TableSchema)
	for _, table := range tables {
		rows, err = tx.QueryContext(ctx, tableColumnsQuery, table.Name)
		if err != nil {
			panic(err)
		}

		returns := []tableColumnsRow{}
		for rows.Next() {
			var i tableColumnsRow
			if err := rows.Scan(&i.ColumnName, &i.Type, &i.UDTName); err != nil {
				return nil, err
			}
			returns = append(returns, i)
		}

		res[table.Name] = dbenv.TableSchema{
			PrimaryKeys: table.PrimaryKeys,
			Types: slices.Remap(returns, func(r tableColumnsRow) dbenv.ColumnType {
				typ := r.Type
				if r.Type == "USER-DEFINED" {
					typ = r.UDTName
				}

				return dbenv.ColumnType{Name: r.ColumnName, Typ: typ}
			}),
		}
	}

	return res, nil
}
