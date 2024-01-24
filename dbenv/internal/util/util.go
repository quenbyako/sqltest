package util

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/quenbyako/ext/slices"
	"github.com/testcontainers/testcontainers-go"

	"github.com/quenbyako/sqltest/dbenv"
)

func GetContainerEnv(ctx context.Context, c testcontainers.Container) (map[string]string, error) {
	exit, outReader, err := c.Exec(ctx, []string{"/usr/bin/env"})
	if err != nil || exit != 0 {
		panic(fmt.Sprintf("%v %v", exit, err))
	}

	stdout, _, err := DemultiplexeDockerOut(outReader)
	if err != nil {
		panic(err)
	}

	env := map[string]string{}
	for _, line := range strings.Split(string(stdout), "\n") {
		if line = strings.TrimSpace(line); line == "" {
			continue
		}

		i := strings.IndexRune(line, '=')
		env[line[:i]] = line[i+1:]
	}
	return env, nil
}

func DemultiplexeDockerOut(r io.Reader) (stdout, stderr []byte, err error) {
	var outBuf bytes.Buffer
	var errBuf bytes.Buffer

	if _, err := stdcopy.StdCopy(&outBuf, &errBuf, r); err != nil {
		return nil, nil, err
	}

	return outBuf.Bytes(), errBuf.Bytes(), nil
}

func ContainerHostPort(ctx context.Context, c testcontainers.Container, p nat.Port) (string, int, error) {
	host, err := c.Host(ctx)
	if err != nil {
		return "", 0, err
	}

	containerPort, err := c.MappedPort(ctx, p)
	if err != nil {
		return "", 0, err
	}

	return host, containerPort.Int(), nil
}

func ConnectSQLContext(ctx context.Context, driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return db, err
	}
	err = db.PingContext(ctx)
	return db, err
}

func DumpTable(ctx context.Context, tx Tx, tableName string, schema dbenv.TableSchema) (data []dbenv.TableRow, err error) {
	// мы не можем здесь без шаманства с запросом, так как prepared запрос не
	// поддерживает динамическое изменение названия таблицы. Это связано с тем,
	// что prepare готовит план запроса под конкретную схему данных, поэтому
	// любая динамическая схема невозможна впринципе.
	rows, err := tx.QueryContext(ctx, "SELECT * FROM "+tableName+"ORDER BY "+strings.Join(slices.Remap(schema.PrimaryKeys, func(s string) string { return s + " ASC" }), ", "))
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		m, err := MapScan(rows)
		if err != nil {
			return nil, err
		}

		data = append(data, m)
	}

	return data, nil
}

func InsertData(ctx context.Context, tx Tx, tableName string, schema dbenv.TableSchema, data []dbenv.TableRow) error {
	panic("unimplemented")
}
