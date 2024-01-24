package tabsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_cmpConst(t *testing.T) {
	for _, tt := range []struct {
		name  string
		pkeys []string
		a, b  map[string]Validator
		want  int
	}{{
		name:  "Same values",
		pkeys: []string{"id"},
		a: map[string]Validator{
			"id": mustValidator("int", "1"),
		},
		b: map[string]Validator{
			"id": mustValidator("int", "1"),
		},
		want: 0,
	}, {
		name:  "Same values, different types",
		pkeys: []string{"id"},
		a: map[string]Validator{
			"id": mustValidator("int", "1"),
		},
		b: map[string]Validator{
			"id": mustValidator("text", "1"),
		},
		want: -1,
	}, {
		name:  "Different values",
		pkeys: []string{"id"},
		a: map[string]Validator{
			"id": mustValidator("int", "1"),
		},
		b: map[string]Validator{
			"id": mustValidator("int", "2"),
		},
		want: -1,
	}, {
		name:  "No key is error and should be last",
		pkeys: []string{"id"},
		a:     map[string]Validator{},
		b: map[string]Validator{
			"id": mustValidator("int", "1"),
		},
		want: -1,
	}, {
		name:  "Primary key is not a constant and different types",
		pkeys: []string{"id"},
		a:     map[string]Validator{
			"id": mustValidator("int", "=true"),
		},
		b: map[string]Validator{
			"id": mustValidator("text", "=false"),
		},
		want: 0,
	}, {
		name:  "Constant and expression	comparison",
		pkeys: []string{"id"},
		a:     map[string]Validator{
			"id": mustValidator("text", "abcd"),
		},
		b: map[string]Validator{
			"id": mustValidator("text", "=true"),
		},
		want: 1,
	}} {
		t.Run(tt.name, func(t *testing.T) {
			got := cmpConst(tt.pkeys, tt.a, tt.b)
			require.Equal(t, tt.want, got)
		})
	}
}

func mustValidator(typ, expr string) Validator {
	v, err := newValidator(nil)("", typ, expr)
	if err != nil {
		panic(err)
	}
	return v
}
