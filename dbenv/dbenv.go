package dbenv

import (
	"context"
	"database/sql"
	"fmt"
)

func SetupConn(ctx context.Context, container Container, driverName string) (*sql.DB, error) {
	connString, err := container.ConnString(ctx)
	if err != nil {
		return nil, fmt.Errorf("can't get connection string %w", err)
	}

	if db, err := sql.Open(driverName, connString); err != nil {
		return nil, err
	} else if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	} else {
		return db, nil
	}
}
