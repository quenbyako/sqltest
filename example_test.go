package sqltest_test

import (
	"context"
	"database/sql/driver"
	"testing"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/quenbyako/sqltest/dbenv"
	"github.com/quenbyako/sqltest/dbenv/postgres"
	"github.com/quenbyako/sqltest/tabsync"
	"github.com/stretchr/testify/require"
)

func TestCustom(t *testing.T) {
	ctx := context.Background()
	c, err := postgres.New(ctx, postgres.WithSetupSchema(nil, []string{
		"CREATE TABLE names (id integer, name text, group_id integer",
		"CREATE TABLE groups (id integer, name text)",
		"ALTER TABLE names ADD CONSTRAINT names_pkey PRIMARY KEY (id)",
		"ALTER TABLE names ADD CONSTRAINT names_group_id_fkey FOREIGN KEY (group_id) REFERENCES groups (id)",
	}))
	require.NoError(t, err)

	tabsync.FlushRaw(c, map[string][]map[string]driver.Value{
		"names": {
			{"id": 1, "name": "John", "group_id": 1},
			{"id": 2, "name": "Jane", "group_id": 2},
		},
		"groups": {
			{"id": 1, "name": "Admins"},
			{"id": 2, "name": "Users"},
		},
	})

	conn, err := dbenv.SetupConn(ctx, c, "pgx")
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "INSERT INTO names (id, name, group_id) VALUES (3, 'Bob', 2)")
	require.NoError(t, err)

	// tabsync.ValidateRaw(c, map[string][]map[string]tabsync.Validator{
	// 	"names": {
	// 		{"id": 1, "name": "John", "group_id": 1},
	// 		{"id": 2, "name": "Jane", "group_id": 2},
	// 	},
	// })

}
