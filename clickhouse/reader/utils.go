package main

import (
	"fmt"
	"strings"
)

// createFilter generates WHERE section of SQL query.
func createFilter(window string) (where string, err error) {
	if window == "" {
		return "", nil
	}
	arr := strings.SplitN(window, ":", 3)
	if len(arr) != 3 {
		return "", fmt.Errorf("invalid format of window `%s`", window)
	}
	return fmt.Sprintf("WHERE %s BETWEEN %s AND %s", arr[0], arr[1], arr[2]), nil
}

// createOrderBy generates ORDER BY section of SQL query.
func createOrderBy(order string) (orderBy string) {
	if order == "" {
		return ""
	}
	return fmt.Sprintf("ORDER BY %s", order)
}
