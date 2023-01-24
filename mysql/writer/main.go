package main

import (
	"context"
	"errors"
	"etl"
	"etl/contract"
	"etl/mysql"
	"etl/mysql/types"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	logger "github.com/AntonYurchenko/log-go"
)

const version = "v0.1.0"

// Configuration of reader.
var (
	endpoint = flag.String("endpoint", "0.0.0.0:24190", "gRPC enndpoint.")
	host     = flag.String("host", "0.0.0.0", "Connection host.")
	port     = flag.Uint("port", 3306, "Connection port.")
	user     = flag.String("user", "root", "User for connecting.")
	password = flag.String("password", "", "Password of an user.")
	workers  = flag.Uint("workers", 1, "Count of workers which will execute queries on MySQL.")
	log      = flag.String("log", "INFO", "Level of logger.")
)

// Reading of arguments.
func readConf() (err error) {
	flag.Parse()
	logger.SetLevelStr(*log)

	logger.InfoF("Start of clickhouse gateway version %s", version)
	logger.InfoF("Endpoint of data consumer %s", *endpoint)
	logger.InfoF("MySQL endpoint %s:%d", *host, *port)
	logger.InfoF("Workers %d", *workers)

	// Check of arguments.
	var errorMessage string
	switch {
	case *user == "":
		errorMessage = "User should be not empty"
	}
	if errorMessage != "" {
		return errors.New(errorMessage)
	}
	return nil
}

func main() {

	if err := readConf(); err != nil {
		logger.ErrorF("Invalid arguments, error: %v", err)
		os.Exit(1)
	}

	conn := &mysql.Conn{
		Address:  fmt.Sprintf("tcp(%s:%d)/", *host, *port),
		User:     *user,
		Password: *password,
		PoolSize: int(*workers),
	}
	defer conn.Close()

	consumer := etl.Consumer{
		Endpoint:      *endpoint,
		Conn:          conn,
		Workers:       int(*workers),
		Converter:     messageToQuery,
		SnapshotQuery: createSnapshotQuery,
	}

	// Waiting of an exit signal.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
	}()

	// Up gRPC server.
	if err := consumer.Up(ctx); err != nil {
		logger.ErrorF("I cannot up a consumer, error: %v", err)
		os.Exit(1)
	}

	logger.Info("Have a good day :)")

}

// messageToQuery creates an insert query from message.
func messageToQuery(message *contract.Message) (query etl.InsertBatch, err error) {
	count := len(message.Batch.Values) / len(message.Batch.Names)
	logger.InfoF("%d row(s) have been read from stream", count)
	sql := createHeader(message.Target, message.Batch.Names)

	// Adding all values of batch to sql query.
	for idx, value := range message.Batch.Values {
		typeIdx := idx % len(message.Batch.Names)
		if typeIdx == 0 {
			if idx == 0 {
				sql += "("
			} else {
				sql += "),("
			}
		} else {
			sql += ","
		}
		strValue, err := types.FromUniversal(message.Batch.Types[typeIdx], string(value))
		if err != nil {
			return etl.InsertBatch{}, err
		}
		sql += string(strValue)
	}

	return etl.InsertBatch{Query: sql + ")", CountRows: count}, nil
}

// createHeader creates a header of SQL insert for MySQL.
func createHeader(target string, names []string) (header string) {
	header = fmt.Sprintf("INSERT INTO %s VALUES ", target)
	if len(names) != 0 {
		header = fmt.Sprintf("INSERT INTO %s (%s) VALUES ", target, strings.Join(names, ","))
	}
	return header
}

// createSnapshotQuery returns a query for recieving a snapshot.
func createSnapshotQuery(fields, table, cursor, cursorMin, cursorMax string) (query string) {
	var filter string
	if cursor != "" && cursorMin != "" && cursorMax != "" {
		filter = fmt.Sprintf("WHERE %s BETWEEN %s AND %s", cursor, cursorMin, cursorMax)
	}
	return fmt.Sprintf("SELECT %s FROM %s %s", fields, table, filter)
}
