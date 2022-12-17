package mysql

import (
	"database/sql"
	"etl/contract"
	"etl/mysql/types"
	"fmt"
	"strings"
	"sync"

	logger "github.com/AntonYurchenko/log-go"
	_ "github.com/go-sql-driver/mysql"
)

// initer is used for one time initialisation.
var initer = new(sync.Once)

// Conn is a structure for connection to mysql.
type Conn struct {
	db       *sql.DB
	Address  string
	User     string
	Password string
	PoolSize int
}

// Close closes a mysql connection.
func (c *Conn) Close() (err error) {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Do executes a query on mysql.
func (c *Conn) Do(query string) (batch *contract.Batch, err error) {

	// Initialisation of a connection pool one time.
	initer.Do(func() {
		logger.InfoF("Initialisation of connector to MySQL %q", c.Address)
		dataSourceName := fmt.Sprintf("%s:%s@%s", c.User, c.Password, c.Address)
		if c.db, err = sql.Open("mysql", dataSourceName); err != nil {
			return
		}
		c.db.SetMaxOpenConns(c.PoolSize)
		c.db.SetMaxIdleConns(c.PoolSize)
	})
	if err != nil {
		return nil, err
	}
	if err = c.db.Ping(); err != nil {
		return nil, err
	}

	// Execution of a query.
	query = strings.TrimSpace(query)
	logger.DebugF("query = %s", query)
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Parsing result olny for SELECT queries.
	isReadQuery := strings.HasPrefix(query, "SELECT")
	if isReadQuery {
		return convert(rows)
	}
	return nil, nil
}

// convert creates *contract.Batch from *sql.Rows.
func convert(rows *sql.Rows) (batch *contract.Batch, err error) {
	batch = new(contract.Batch)

	// Receiving meta data.
	cts, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	batch.Names = make([]string, len(cts))
	batch.Types = make([]string, len(batch.Names))
	for idx := range cts {
		batch.Names[idx] = cts[idx].Name()
		batch.Types[idx] = cts[idx].DatabaseTypeName()
	}
	logger.DebugF("batch.Names = %v", batch.Names)
	logger.DebugF("batch.Types = %v", batch.Types)

	// Mapping of columns.
	batch.Values = make([][]byte, 0, len(batch.Names))
	values := make([]sql.RawBytes, len(batch.Names))
	colsMapping := make([]interface{}, len(values))
	for i := range values {
		colsMapping[i] = &values[i]
	}

	// Reading values.
	for rows.Next() {

		err = rows.Scan(colsMapping...)
		if err != nil {
			return nil, err
		}

		var value string
		for idx, col := range values {
			if col == nil {
				value = ""
			} else {
				value = string(col)
			}
			dataType := batch.Types[idx%len(batch.Types)]
			typedValue, err := types.ToUniversal(dataType, value)
			if err != nil {
				return nil, err
			}
			batch.Values = append(batch.Values, []byte(typedValue))
		}
	}

	types.Update(batch.Types)

	logger.DebugF("batch = %v", batch)
	return batch, nil
}
