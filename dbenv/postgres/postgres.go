package postgres

import (
	"context"
	"net/url"
	"time"

	"github.com/quenbyako/sqltest/dbenv"
	"github.com/quenbyako/sqltest/dbenv/internal/util"
	"github.com/testcontainers/testcontainers-go"
)

const (
	defaultUser          = "postgres"
	defaultPassword      = "postgres"
	defaultPostgresImage = "postgres:16-alpine"
)

// container represents the postgres container type used in the module
type container struct {
	testcontainers.Container
}

var _ dbenv.Container = (*container)(nil)

// RunContainer creates an instance of the postgres container type
func New(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (_ dbenv.Container, err error) {
	req := testcontainers.ContainerRequest{
		Image: defaultPostgresImage,
		Env: map[string]string{
			"POSTGRES_USER":     defaultUser,
			"POSTGRES_PASSWORD": defaultPassword,
			"POSTGRES_DB":       defaultUser, // defaults to the user name
		},
		ExposedPorts: []string{"5432/tcp"},
		Cmd:          []string{"postgres", "-c", "fsync=off"},
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	for _, opt := range opts {
		opt.Customize(&genericContainerReq)
	}

	var args url.Values
	if argsRaw, ok := req.Env["POSTGRES_CONN_ARGS"]; ok {
		if args, err = url.ParseQuery(argsRaw); err != nil {
			panic(err)
		}
	}
	withWaitSQL(req.Env["POSTGRES_DB"], args).Customize(&genericContainerReq)

	c, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return nil, err
	}

	return &container{Container: c}, nil
}

const defaultStopTimeout = 5 * time.Second

func (c *container) Close() error {
	return c.Container.Stop(context.Background(), ptr(defaultStopTimeout))
}

func (c *container) ConnString(ctx context.Context) (string, error) {
	return pgHostFromEnv(ctx, c.Container)
}

func (c *container) Flush(ctx context.Context, data map[string][]dbenv.TableRow) error {
	connString, err := pgHostFromEnv(ctx, c)
	if err != nil {
		panic(err)
	}

	conn, err := util.ConnectSQLContext(ctx, "pgx", connString)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	tables, err := util.GetAllSchemaTables(ctx, conn)
	if err != nil {
		panic(err)
	}
	for name, schema := range tables {
		// мы не можем здесь без шаманства с запросом, так как prepared запрос
		// не поддерживает динамическое изменение названия таблицы. Это связано
		// с тем, что prepare готовит план запроса под конкретную схему данных,
		// поэтому любая динамическая схема невозможна впринципе.
		_, err := conn.ExecContext(ctx, "DELETE FROM "+name)
		if err != nil {
			panic(err)
		}
		if values, ok := data[name]; ok {
			if err := util.InsertData(ctx, conn, name, schema, values); err != nil {
				panic(err)
			}
		}
	}

	return nil
}

func (c *container) Dump(ctx context.Context) (map[string]dbenv.TableData, error) {
	connString, err := pgHostFromEnv(ctx, c)
	if err != nil {
		panic(err)
	}

	conn, err := util.ConnectSQLContext(ctx, "pgx", connString)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	tables, err := util.GetAllSchemaTables(ctx, conn)
	if err != nil {
		panic(err)
	}

	res := make(map[string]dbenv.TableData)
	for name, schema := range tables {
		data, err := util.DumpTable(ctx, conn, name, schema)
		if err != nil {
			panic(err)
		}

		res[name] = dbenv.TableData{
			Schema: schema,
			Rows:   data,
		}
	}

	return res, nil
}

func ptr[T any](t T) *T { return &t }
