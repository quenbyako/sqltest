package dbenv

import (
	"context"
	"database/sql/driver"
)

type TableData struct {
	Schema TableSchema
	Rows   []TableRow
}

type TableRow map[string]driver.Value

type Container interface {
	// ConnString returns the connection string for the database container, using the default 5432 port, and
	// obtaining the host and exposed port from the container. It also accepts a variadic list of extra arguments
	// which will be appended to the connection string. The format of the extra arguments is the same as the
	// connection string format, e.g. "connect_timeout=10" or "application_name=myapp"
	ConnString(context.Context) (string, error)
	Close() error

	Dump(context.Context) (map[string]TableData, error)
	Flush(context.Context, map[string][]TableRow) error
}

type TableSchema struct {
	PrimaryKeys []string
	Types       []ColumnType
}

func (s TableSchema) TypeMap() map[string]string {
	res := make(map[string]string, len(s.Types))
	for _, t := range s.Types {
		res[t.Name] = t.Typ
	}

	return res
}

type ColumnType struct{ Name, Typ string }
