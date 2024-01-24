package postgres

import (
	"context"
	"fmt"
	"net/url"

	"github.com/docker/go-connections/nat"
	"github.com/quenbyako/sqltest/dbenv/internal/util"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func pgHostFromEnv(ctx context.Context, c testcontainers.Container) (connStr string, err error) {
	env, err := util.GetContainerEnv(ctx, c)
	if err != nil {
		panic(err)
	}

	host, port, err := util.ContainerHostPort(ctx, c, "5432/tcp")
	if err != nil {
		panic(err)
	}

	var args url.Values
	if argsRaw, ok := env["POSTGRES_CONN_ARGS"]; ok {
		if args, err = url.ParseQuery(argsRaw); err != nil {
			panic(err)
		}
	}

	return pgHost(env["POSTGRES_USER"], env["POSTGRES_PASSWORD"], host, port, env["POSTGRES_DB"], args), nil
}

func pgHost(user, password, host string, port int, dbName string, opts url.Values) string {
	return fmt.Sprintf(
		"postgresql://%v:%v@%v:%v/%v?%v",
		user,
		password,
		host,
		port,
		dbName,
		opts.Encode(),
	)
}

func withWaitSQL(dbName string, args url.Values) testcontainers.ContainerCustomizer {
	return testcontainers.WithWaitStrategy(
		wait.ForSQL("5432/tcp", "pgx", func(host string, port nat.Port) string {
			return pgHost("postgres", "postgres", host, port.Int(), dbName, args)
		}),
	)
}
