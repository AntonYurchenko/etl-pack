package etl

import (
	"crypto/md5"
	"etl/contract"
	"fmt"
	"sort"
	"strings"
)

// Conn is an universal connection interface to all storages
type Conn interface {
	Do(query string) (batch *contract.Batch, err error)
}

// InsertBatch is an entity which defines a batch for saving to storage.
// It contains a query and count of rows for saving to storage.
type InsertBatch struct {
	Query     string
	CountRows int
}

// filter returns batch with values of rows whose hash sums are not in the dictionary.
// If all hash sums of rows the batch have in a dictionary function will return nil.
func filter(batch *contract.Batch, incDict map[string]bool) (newBatch *contract.Batch) {

	// Increment mode is disabled.
	if len(incDict) == 0 {
		return batch
	}

	// Filtering of rows which are in target storage already.
	newBatch = new(contract.Batch)
	iterByRows(batch, func(row [][]byte) {
		if !incDict[hashFunc(row)] {
			newBatch.Values = append(newBatch.Values, row...)
		}
	})

	if len(newBatch.Values) == 0 {
		return nil
	}

	newBatch.Names = batch.Names
	newBatch.Types = batch.Types

	return newBatch
}

// iterByRows do iteration by rows of data selected batch.
func iterByRows(batch *contract.Batch, fn func(row [][]byte)) {
	countCols := len(batch.Names)
	for i := 0; i < len(batch.Values); i += countCols {
		row := batch.Values[i : i+countCols]
		fn(row)
	}
}

// hashFunc returns a hash of row.
// In this implementation using md5sum.
func hashFunc(row [][]byte) (hash string) {
	var acc []string
	for i := range row {
		acc = append(acc, strings.ToLower(string(row[i])))
	}
	sort.Strings(acc)
	return fmt.Sprintf("%x", md5.Sum([]byte(strings.Join(acc, ""))))
}
