package main

import (
	"context"
	"etl"
	"etl/clickhouse"
	"etl/clickhouse/types"
	"etl/contract"
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
	port     = flag.Uint("port", 8123, "Connection port.")
	user     = flag.String("user", "default", "User for connecting.")
	password = flag.String("password", "", "Password of an user.")
	workers  = flag.Uint("workers", 1, "Count of workers which will execute queries on clickhouse.")
	log      = flag.String("log", "INFO", "Level of logger.")
)

func init() {
	flag.Parse()
	logger.SetLevelStr(*log)

	logger.InfoF("Start of clickhouse gateway version %s", version)
	logger.InfoF("Endpoint of data consumer %s", *endpoint)
	logger.InfoF("Clickhouse endpoint %s:%d", *host, *port)
	logger.InfoF("Workers %d", *workers)

	// Check of arguments.
	var errorMessage string
	switch {
	case *user == "":
		errorMessage = "User should be not empty"
	}
	if errorMessage != "" {
		logger.ErrorF("Invalid arguments, error: %s", errorMessage)
		os.Exit(1)
	}
}

func main() {

	consumer := etl.Consumer{
		Endpoint: *endpoint,
		Conn: &clickhouse.Conn{
			Address:  fmt.Sprintf("http://%s:%d", *host, *port),
			User:     *user,
			Password: *password,
		},
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

	sql := fmt.Sprintf("INSERT INTO %s FORMAT TSV", message.Target)
	if len(message.Batch.Names) != 0 {
		sql = fmt.Sprintf("INSERT INTO %s (%s) FORMAT TSV", message.Target, strings.Join(message.Batch.Names, ","))
	}

	// Adding all values of batch to sql query.
	for idx, value := range message.Batch.Values {
		typeIdx := idx % len(message.Batch.Names)
		if typeIdx == 0 {
			sql += "\n"
		} else {
			sql += "\t"
		}
		strValue, err := types.FromUniversal(message.Batch.Types[typeIdx], string(value))
		if err != nil {
			return etl.InsertBatch{}, err
		}
		sql += string(strValue)
	}

	return etl.InsertBatch{Query: sql, CountRows: count}, nil
}

// createSnapshotQuery returns a query for recieving a snapshot.
func createSnapshotQuery(fields, table, cursor, cursorMin, cursorMax string) (query string) {
	var filter string
	if cursor != "" && cursorMin != "" && cursorMax != "" {
		filter = fmt.Sprintf("WHERE %s BETWEEN %s AND %s", cursor, cursorMin, cursorMax)
	}
	return fmt.Sprintf("SELECT %s FROM %s %s", fields, table, filter)
}
