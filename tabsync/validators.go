package tabsync

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/quenbyako/ext/slices"
	"github.com/quenbyako/sqltest"
)

type Validator interface {
	Validate(driver.Value) error
	AsValue() (driver.Value, bool)
}

func newValue(column, typ, s string) (driver.Value, error) {
	if s[0] != '=' {
		return convertTo(typ, s)
	}

	e := newValueExprEnv()
	prog, err := expr.Compile(s[1:], expr.Env(e))
	if err != nil {
		return "", err
	}

	value, err := expr.Run(prog, e)
	if err != nil {
		return "", err
	}

	return convertTo(typ, fmt.Sprint(value))
}

func newValidator(pkeys []string) func(column, typ, s string) (Validator, error) {
	return func(column, typ, s string) (Validator, error) {
		if s[0] != '=' {
			value, err := convertTo(typ, s)
			if err != nil {
				return nil, err
			}
			return constValidator{value: value}, nil
		}

		if slices.Contains(pkeys, column) {
			return nil, fmt.Errorf("found %#v value for %#v column: primary keys can't be formulas", s, column)
		}

		fuzzed, nullable := getType(typ)

		// предварительная проверка что оно хотя бы заработает без ошибок
		e := newValidatorExprEnv(typ, fuzzed)
		prog, err := expr.Compile(s[1:], expr.Env(e), expr.AsBool())
		if err != nil {
			return nil, err
		}

		val, err := expr.Run(prog, e)
		if err != nil {
			return nil, fmt.Errorf("evaluating: %w", err)
		}

		if _, ok := val.(bool); !ok {
			return nil, fmt.Errorf("expression returns non boolean value: %v", s)
		}

		return exprValidator{typName: typ, typ: fuzzed, nullable: nullable, prog: prog}, nil
	}
}

func validateTable(got sqltest.TableData, want []map[string]Validator) error {
	if len(got.Schema.PrimaryKeys) == 0 {
		return errors.New("required at least one primary key, otherwise can't match rows")
	}

	got.Rows = slices.SortFunc(got.Rows, func(a, b sqltest.TableRow) int {
		for _, key := range got.Schema.PrimaryKeys {
			if v := cmpValue(false)(a[key], b[key]); v != 0 {
				return v
			}
		}
		return 0
	})
	want = slices.SortFunc(want, func(a, b map[string]Validator) int {
		// if value is primary key and can't be converted to constant value, it should be sorted as last
		for _, key := range got.Schema.PrimaryKeys {
			aVal, ok := a[key]
			if !ok {
				if _, ok := b[key]; !ok {
					return 0
				}
				return 1
			}
			bVal, ok := b[key]
			if !ok {
				return -1
			}

			if v := cmpValue(false)(aVal, bVal); v != 0 {
				return v
			}
		}

		return 0
	})

	var errs []error
	for _, want := range want {
		rowPkeys, err := rowValidatorPkeys(want, got.Schema.PrimaryKeys)
		if err != nil {
			panic(err)
		}

		i, ok := slices.BinarySearchFunc(got.Rows, 0, func(row sqltest.TableRow, i int) int {
			for _, key := range got.Schema.PrimaryKeys {
				if v := cmpValue(false)(row[key], rowPkeys[key]); v != 0 {
					return v
				}
			}
			return 0
		})
		if !ok {
			return fmt.Errorf("row %#v: not found in database", rowPkeys)
		}

		for k, wantItem := range want {
			if gotItem, ok := got.Rows[i][k]; !ok {
				errs = append(errs, fmt.Errorf("row %#v: key %q: not found", rowPkeys, k))
			} else if err := wantItem.Validate(gotItem); err != nil {
				errs = append(errs, fmt.Errorf("row %#v: key %q: %w", rowPkeys, k, err))
			}
		}
	}

	return errors.Join(errs...)
}

type constValidator struct {
	value driver.Value
}

// for sorting
func cmpConst(pkeys []string, a, b map[string]Validator) int {
	for _, key := range pkeys {
		var aVal driver.Value
		var aValid bool
		if aValidator, ok := a[key]; ok {
			aVal, aValid = aValidator.AsValue()
		}
		var bVal driver.Value
		var bValid bool
		if bValidator, ok := b[key]; ok {
			bVal, bValid = bValidator.AsValue()
		}

		// if value is primary key and can't be converted to constant value, it
		// should be sorted as last. If both values are not constant values,
		// then they are equal
		if !(aValid || bValid) { // if both are not valid
			continue
		} else if aValid && bValid { // if both are valid
			if v := cmpValue(false)(aVal, bVal); v != 0 {
				return v
			}
			continue
		}

		// one of values is not valid
		if aValid {
			return 1
		} else {
			return -1
		}
	}

	return 0
}

func (c constValidator) Validate(s driver.Value) error {
	if s == nil && c.value != s {
		return fmt.Errorf("mismatched values: got %#v, want %#v", s, c.value)
	} else if reflect.TypeOf(c.value) != reflect.TypeOf(s) {
		return fmt.Errorf("mismatched types: got %T, want %T", s, c.value)
	} else if !reflect.DeepEqual(c.value, s) {
		return fmt.Errorf("mismatched values: got %#v, want %#v", s, c.value)
	}

	return nil
}

func (c constValidator) AsValue() (driver.Value, bool) { return c.value, true }

type exprValidator struct {
	typName  string
	typ      driver.Value
	nullable bool
	prog     *vm.Program
}

func (c exprValidator) Validate(s driver.Value) error {
	if s == nil {
		if !c.nullable {
			return fmt.Errorf("not expected null value, got %#v", s)
		}
	} else if reflect.TypeOf(c.typ) != reflect.TypeOf(s) {
		return fmt.Errorf("mismatched types: got %T, want %T", s, c.typ)
	}

	val, err := expr.Run(c.prog, newValidatorExprEnv(c.typName, s))
	if err != nil {
		panic(err)
	}

	if valid, ok := val.(bool); !ok {
		panic(err)
	} else if !valid {
		return fmt.Errorf("on %[1]q (type %[1]T), fails this expression: %[2]v", s, c.prog.Source().Content())
	}

	return nil
}

func (c exprValidator) AsValue() (driver.Value, bool) { return nil, false }

func newValueExprEnv() map[string]any {
	return map[string]any{}
}

func newValidatorExprEnv(typ string, value driver.Value) map[string]any {
	return map[string]any{
		"value": value,
		"type":  typ,
	}
}
