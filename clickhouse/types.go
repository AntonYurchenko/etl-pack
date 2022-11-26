package clickhouse

import (
	"time"
)

// castTypeDict contains all supported types.
var castTypeDict = map[string]string{
	"DateTime": "datetime",
	"UInt8":    "uint8",
	"UInt16":   "uint16",
	"UInt32":   "uint32",
	"UInt64":   "uint64",
	"Int8":     "int8",
	"Int16":    "int16",
	"Int32":    "int32",
	"Int64":    "int64",
	"Date":     "date",
	"String":   "string",
}

// transform makes transformation of a value from source to universal data type.
func transform(dataType, value string) (newValue string, err error) {
	switch dataType {
	case "DateTime":
		dt, err := time.Parse("2006-01-02 15:04:05", value)
		if err != nil {
			return "", err
		}
		return dt.Format("2006-01-02T15:04:05.999Z07:00"), nil
	default:
		return value, nil
	}
}

// updateTypes updates of data type names.
func updateTypes(types []string) {
	for idx := range types {
		if new, ok := castTypeDict[types[idx]]; ok {
			types[idx] = new
			continue
		}
		types[idx] = "string"
	}
}
