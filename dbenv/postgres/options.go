package postgres

import (
	"context"
	"net/url"
	"path/filepath"

	"github.com/quenbyako/sqltest/dbenv/internal/util"
	"github.com/testcontainers/testcontainers-go"
)

func WithConnArgs(args url.Values) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) {
		req.Env["POSTGRES_CONN_ARGS"] = args.Encode()
	}
}

// WithConfigFile sets the config file to be used for the postgres container
// It will also set the "config_file" parameter to the path of the config file
// as a command line argument to the container
func WithConfigFile(cfg string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) {
		cfgFile := testcontainers.ContainerFile{
			HostFilePath:      cfg,
			ContainerFilePath: "/etc/postgresql.conf",
			FileMode:          0o755,
		}

		req.Files = append(req.Files, cfgFile)
		req.Cmd = append(req.Cmd, "-c", "config_file=/etc/postgresql.conf")
	}
}

// WithInitScripts sets the init scripts to be run when the container starts
func WithInitScripts(scripts ...string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) {
		initScripts := []testcontainers.ContainerFile{}
		for _, script := range scripts {
			cf := testcontainers.ContainerFile{
				HostFilePath:      script,
				ContainerFilePath: "/docker-entrypoint-initdb.d/" + filepath.Base(script),
				FileMode:          0o755,
			}
			initScripts = append(initScripts, cf)
		}
		req.Files = append(req.Files, initScripts...)
	}
}

func WithSetupSchema(queries []string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) {
		req.LifecycleHooks = append(req.LifecycleHooks, testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{
				func(ctx context.Context, c testcontainers.Container) error {
					connString, err := pgHostFromEnv(ctx, c)
					if err != nil {
						panic(err)
					}

					conn, err := util.ConnectSQLContext(ctx, "pgx", connString)
					if err != nil {
						panic(err)
					}
					defer conn.Close()

					// поскольку мы проверяем все запросы, констрейнты нам не нужны, так что
					// говорим что сессия в режиме реплики
					if _, err = conn.Exec("SET session_replication_role = 'replica'"); err != nil {
						panic(err)
					}

					for _, query := range queries {
						if _, err := conn.Exec(query); err != nil {
							panic(err)
						}
					}

					req.Logger.Printf("🎉 Testing environment is setted up!")

					return nil
				},
			},
		})
	}
}

/*
// WithUsername sets the initial username to be created when the container starts
// It is used in conjunction with WithPassword to set a user and its password.
// It will create the specified user with superuser power and a database with the same name.
// If it is not specified, then the default user of postgres will be used.
func WithUsername(user string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) {
		req.Env["POSTGRES_USER"] = user
	}
}

// WithPassword sets the initial password of the user to be created when the container starts
// It is required for you to use the PostgreSQL image. It must not be empty or undefined.
// This environment variable sets the superuser password for PostgreSQL.
func WithPassword(password string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) {
		req.Env["POSTGRES_PASSWORD"] = password
	}
}

// WithDatabase sets the initial database to be created when the container starts
// It can be used to define a different name for the default database that is created when the image is first started.
// If it is not specified, then the value of WithUser will be used.
func WithDatabase(dbName string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) {
		req.Env["POSTGRES_DB"] = dbName
	}
}
*/
