package clickhouse

import (
	"encoding/json"
	"errors"
	"etl/contract"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	logger "github.com/AntonYurchenko/log-go"
)

// initer is used for one time initialisation.
var initer = new(sync.Once)

// Conn is a structure for connection to clickhouse.
type Conn struct {
	httpClient *http.Client
	Address    string
	User       string
	Password   string
}

// Do executes a query on clickhouse.
func (c *Conn) Do(query string) (batch *contract.Batch, err error) {

	// Initialisation.
	initer.Do(func() {
		c.httpClient = http.DefaultClient
		logger.Info("Initialisation of clickhouse client")
	})

	// Definition a format for SELECT queries.
	query = strings.TrimSpace(query)
	isReadQuery := strings.HasPrefix(query, "SELECT")
	if isReadQuery {
		query += " FORMAT JSON"
	}

	logger.DebugF("query = %s", query)

	// New request.
	req, err := http.NewRequest("POST", c.Address, strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.User, c.Password)

	// Send request.
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	logger.DebugF("resp.StatusCode = %d", resp.StatusCode)

	// If a response has no status code 200 then response is an error.
	if resp.StatusCode != 200 {
		msg, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(string(msg))
	}

	// Parsing result olny for SELECT queries.
	if isReadQuery {
		chBatch := new(Batch)
		err = json.NewDecoder(resp.Body).Decode(chBatch)
		if err != nil {
			return nil, err
		}
		return convert(chBatch), nil
	}
	return nil, nil
}

// Batch is a structure for parsing result of SQL from clickhouse.
type Batch struct {
	Meta Meta `json:"meta"`
	Data Data `json:"data"`
}
type Meta []map[string]string
type Data []map[string]any

// convert describes logic of convertation *Batch to *contract.Batch
func convert(chBatch *Batch) (wrBatch *contract.Batch) {

	wrBatch = new(contract.Batch)
	wrBatch.Names, wrBatch.Types = flatMeta(chBatch.Meta)
	wrBatch.Values = make([][]byte, 0, len(wrBatch.Names))

	for _, row := range chBatch.Data {

		for idx, name := range wrBatch.Names {

			value, err := transform(wrBatch.Types[idx], fmt.Sprint(row[name]))
			if err != nil {
				panic(err)
			}
			wrBatch.Values = append(wrBatch.Values, []byte(value))
		}

		logger.DebugF("wrBatch = %v", wrBatch)
	}

	updateTypes(wrBatch.Types)

	return wrBatch
}

// flatMeta returns two arrays, first with names of fields, second with data types from meta data of clickhouse.
func flatMeta(meta Meta) (names, types []string) {
	names, types = make([]string, 0, len(meta)), make([]string, 0, len(meta))
	for _, m := range meta {
		names = append(names, m["name"])
		types = append(types, m["type"])
	}
	return names, types
}
