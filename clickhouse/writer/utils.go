package main

import (
	"fmt"
	"strings"
)

// createHeader creates a header of SQL insert for clickhouse.
func createHeader(target string, names []string) (header string) {
	header = fmt.Sprintf("INSERT INTO %s FORMAT TSV", target)
	if len(names) != 0 {
		header = fmt.Sprintf("INSERT INTO %s (%s) FORMAT TSV", target, strings.Join(names, ","))
	}
	return header
}
