package types

import (
	"strings"
	"time"
)

// typeDict contains all supported types for MySQL.
var typeDict = map[string]string{
	"DATETIME": "datetime",
	"INT":      "int64",
	"DATE":     "date",
	"TEXT":     "string",
}

// ToUniversal makes transformation of a value from source to universal data type.
func ToUniversal(dataType, value string) (newValue string, err error) {
	switch dataType {
	case "DATETIME":
		dt, err := time.Parse("2006-01-02 15:04:05", value)
		if err != nil {
			return "", err
		}
		return dt.Format("2006-01-02T15:04:05.999Z07:00"), nil
	default:
		return value, nil
	}
}

// Update updates of data type names.
func Update(types []string) {
	for idx := range types {
		if new, ok := typeDict[types[idx]]; ok {
			types[idx] = new
			continue
		}
		types[idx] = "string"
	}
}

// FromUniversal makes transformation of a value from universal data type to type of storage.
func FromUniversal(dataType, value string) (newValue string, err error) {
	switch dataType {
	case "int64", "date":
		return value, nil
	case "datetime":
		dt, err := time.Parse("2006-01-02T15:04:05.999Z07:00", value)
		if err != nil {
			return "", err
		}
		return `'` + dt.Format("2006-01-02 15:04:05") + `'`, nil
	default:
		return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`, nil
	}
}
