package tabsync

import (
	"cmp"
	"database/sql/driver"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

func cmpValue(nullFirst bool) func(_, _ driver.Value) int {
	return func(a, b driver.Value) int {
		if v, hasNil := cmpNil(nullFirst, a, b); hasNil {
			return v
		}

		// now both values are not nil.

		switch a := a.(type) {
		case int64:
			switch b := b.(type) {
			case int64:
				return cmp.Compare(a, b)
			case float64:
				return cmp.Compare(float64(a), b)
			case bool:
				return -cmpBool(b, a) // invert value
			case []byte, string, time.Time:
				return 1 // int always higher priority
			}
		case float64:
			switch b := b.(type) {
			case int64:
				return cmp.Compare(a, float64(b))
			case float64:
				return cmp.Compare(a, b)
			case bool:
				return -cmpBool(b, a) // invert value
			case []byte, string, time.Time:
				return 1 // float64 always higher priority
			}
		case bool:
			return cmpBool(a, b)
		case []byte:
			switch b := b.(type) {
			case int64, float64:
				return -1 // bytes goes after numbers
			case bool:
				return -cmpBool(b, a) // invert value
			case []byte:
				return slices.Compare(a, b)
			case string:
				return slices.Compare(a, []byte(b))
			case time.Time:
				return 1
			}
		case string:
			switch b := b.(type) {
			case int64, float64:
				return -1 // strings goes after numbers
			case bool:
				return -cmpBool(b, a) // invert value
			case []byte:
				return strings.Compare(a, string(b))
			case string:
				return strings.Compare(a, b)
			case time.Time:
				return 1
			}
		case time.Time:
			switch b := b.(type) {
			case int64, float64, bool, []byte, string:
				return -1 // time is lowest priority
			case time.Time:
				return a.Compare(b)
			}
		}

		panic(fmt.Sprintf("unsupported types %T %T", a, b))
	}
}

// at least one value must be nil
func cmpNil(nullFirst bool, a, b any) (res int, hasNil bool) {
	sign := +1
	if nullFirst {
		sign = -1
	}

	if a == nil || b == nil {
		switch {
		case a == nil && b == nil:
			return 0, true
		case a != nil:
			return +1 * sign, true
		case b != nil:
			return -1 * sign, true
		default:
			panic("unreachable")
		}
	}

	return 0, false
}

func cmpBool(a bool, b driver.Value) int {
	var err error
	if b, err = driver.Bool.ConvertValue(b); err != nil {
		switch b.(type) {
		case int64, float64:
			return 1
		default:
			return -1
		}
	}
	switch b := b.(bool); {
	case a == b:
		return 0
	case a:
		return 1
	case b:
		return -1
	default:
		panic("unreachable")
	}
}

func rowValidatorPkeys(v map[string]Validator, pkeys []string) (map[string]driver.Value, error) {
	res := map[string]driver.Value{}
	for _, key := range pkeys {
		if vRaw, ok := v[key]; !ok {
			return nil, fmt.Errorf("required pkey %#v not found", key)
		} else if v, ok := vRaw.AsValue(); ok {
			res[key] = v
		} else {
			return nil, fmt.Errorf("pkey %#v is not a constant", key)
		}
	}

	return res, nil
}

func convertTo(typ, value string) (driver.Value, error) {
	if strings.HasPrefix(typ, "?") {
		if strings.EqualFold(value, "null") {
			return nil, nil
		}
		typ = strings.TrimPrefix(typ, "?")
	}

	switch typ {
	case "text":
		return driver.String.ConvertValue(value)
	case "uuid":
		return uuid.Parse(value)
	case "integer", "int":
		return driver.Int32.ConvertValue(value)
	default:
		panic(fmt.Sprintf("type %#v not found", typ))
	}
}

func getType(typ string) (ref driver.Value, nullable bool) {
	if strings.HasPrefix(typ, "?") {
		nullable = true
		typ = strings.TrimPrefix(typ, "?")
	}

	switch typ {
	case "text":
		ref = string("")
	case "uuid":
		ref = string("")
	case "integer", "int":
		ref = int64(0)
	default:
		panic(fmt.Sprintf("type %#v not found", typ))
	}

	return ref, nullable
}
