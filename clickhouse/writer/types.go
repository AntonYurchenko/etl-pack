package main

import (
	"strings"
	"time"
)

// transform makes transformation of a value from universal data type to type of storage.
func transform(dataType, value string) (newValue string, err error) {
	switch dataType {
	case "uint8", "uint16", "uint32", "uint64", "int8", "int16", "int32", "int64", "date":
		return value, nil
	case "datetime":
		dt, err := time.Parse("2006-01-02T15:04:05.999Z07:00", value)
		if err != nil {
			return "", err
		}
		return dt.Format("2006-01-02 15:04:05"), nil
	default:
		return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`, nil
	}
}
