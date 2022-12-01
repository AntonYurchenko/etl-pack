package main

import (
	"context"
	"etl"
	"etl/mysql"
	"flag"
	"fmt"
	"strings"

	logger "github.com/AntonYurchenko/log-go"
)

const version = "v0.1.0"

// Configuration of reader.
var (
	comsumer  = flag.String("comsumer", "0.0.0.0:24190", "gRPC endpoint of data consumer.")
	host      = flag.String("host", "0.0.0.0", "Connection host.")
	port      = flag.Uint("port", 3306, "Connection port.")
	user      = flag.String("user", "root", "User for connecting.")
	password  = flag.String("password", "", "Password of an user.")
	tableFrom = flag.String("from", "", "Source table in format `data_base.table_name`.")
	tableTo   = flag.String("to", "", "Target table in format `data_base.table_name`.")
	fields    = flag.String("fields", "*", "List of fields from source table in format `field1,field2,...,fieldN`.")
	order     = flag.String("order", "", "Ordering column name or some names split by comma.")
	batch     = flag.Uint("batch", 1000, "Size of selected batch in one query.")
	workers   = flag.Uint("workers", 1, "Count of workers which will execute queries on clickhouse.")
	window    = flag.String("window", "", "Window of data for processing in format `column:from:to`.")
	log       = flag.String("log", "INFO", "Level of logger.")
)

// Reading of arguments.
func init() {
	flag.Parse()
	logger.SetLevelStr(*log)

	logger.InfoF("Start of clickhouse reader version: %s", version)
	logger.InfoF("Endpoint of data consumer: %s", *comsumer)
	logger.InfoF("Clickhouse endpoint: %s:%d", *host, *port)
	logger.InfoF("Source table: %s", *tableFrom)
	logger.InfoF("Target table: %s", *tableTo)
	logger.InfoF("Selected fields: %s", *fields)
	logger.InfoF("Workers: %d", *workers)
	logger.InfoF("Reading batch size: %d", *batch)
	logger.InfoF("Ordering by: %s", *order)
	logger.InfoF("Window of data for processing is: %s", *window)

	// Check of arguments.
	var errorMessage string
	switch {
	case *user == "":
		errorMessage = "user should be not empty"
	case *tableFrom == "":
		errorMessage = "source table should be not empty"
	case *tableTo == "":
		errorMessage = "target table should be not empty"
	case *fields == "":
		errorMessage = "list of fields should be not empty"
	case *fields != "*":
		for _, field := range strings.Split(*fields, ",") {
			if strings.Contains(strings.TrimSpace(field), " ") {
				errorMessage = "invalid list of fields"
			}
		}
	}
	if errorMessage != "" {
		panic(errorMessage)
	}
}

// Definition of data reading pipeline.
func main() {

	// Initialisation.
	grpcConn := &etl.GrpcClient{
		Endpoint: *comsumer,
	}
	defer grpcConn.Close()

	conn := &mysql.Conn{
		Address:  fmt.Sprintf("tcp(%s:%d)/", *host, *port),
		User:     *user,
		Password: *password,
		PoolSize: int(*workers),
	}
	defer conn.Close()

	provider := etl.DataProvider{
		Target:     *tableTo,
		Workers:    int(*workers),
		GrpcClient: grpcConn,
		Conn:       conn,
		Generator:  sqlGenerator,
	}

	provider.Up()
	logger.Info("Have a good day :)")
}

// sqlGenerator is a concurrent query generator.
func sqlGenerator(ctx context.Context, workers int) (queries <-chan string) {

	out := make(chan string, workers)
	go func() {
		defer close(out)

		// Creation of WHERE section.
		filter := createFilter(*window)
		logger.DebugF("filter = %s", filter)

		// Creation of ORDER BY section.
		orderBy := createOrderBy(*order)
		logger.DebugF("orderBy = %s", orderBy)

		batchSize := int(*batch)

		// Generation of queries with offset.
		var offset int
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case out <- fmt.Sprintf("SELECT %s FROM %s %s %s LIMIT %d, %d", *fields, *tableFrom, filter, orderBy, offset, batchSize):
				offset += batchSize
				logger.DebugF("offset = %d", offset)
			}
		}
	}()

	return out
}

// createFilter generates WHERE section of SQL query.
func createFilter(window string) (where string) {
	if window == "" {
		return ""
	}
	arr := strings.SplitN(window, ":", 3)
	if len(arr) != 3 {
		return ""
	}
	return fmt.Sprintf("WHERE %s BETWEEN %s AND %s", arr[0], arr[1], arr[2])
}

// createOrderBy generates ORDER BY section of SQL query.
func createOrderBy(order string) (orderBy string) {
	if order == "" {
		return ""
	}
	return fmt.Sprintf("ORDER BY %s", order)
}
