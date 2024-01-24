package tabsync

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"time"

	"github.com/quenbyako/sqltest"
)


func FlushFS(container sqltest.Container, fsys fs.FS, path string) error {
	panic("Unimplemented")
}

func FlushCSV(container sqltest.Container, data map[string]io.Reader) error {
	panic("Unimplemented")
}

func FlushRaw(container sqltest.Container, data map[string][]map[string]driver.Value) error {
	panic("Unimplemented")
}

func ValidateTableFS(container sqltest.Container, fsys fs.FS, path string) error {
	panic("Unimplemented")
}

func ValidateTableCSV(container sqltest.Container, data map[string]io.Reader) error {
	panic("Unimplemented")
}

func ValidateTableRaw(container sqltest.Container, validators map[string][]map[string]Validator) error {
	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	dumped, err := container.Dump(ctx)
	if err != nil {
		return fmt.Errorf("can't fetch database schema %w", err)
	}

	var errs []error
	for tableName, validators := range validators {
		data, ok := dumped[tableName]
		if !ok {
			errs = append(errs, fmt.Errorf("table %#v: not exists in database", tableName))
		}

		if err := validateTable(data, validators); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}


func ValidateResultFS(container sqltest.Container, fsys fs.FS, path string) error {
	panic("Unimplemented")
}

func ValidateResultCSV(container sqltest.Container, data map[string]io.Reader) error {
	panic("Unimplemented")
}

func ValidateResultRaw(container sqltest.Container, validators map[string][]map[string]Validator) error {
	panic("Unimplemented")
}
