package main

import (
	"context"
	"errors"
	"etl"
	"etl/mysql"
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
	window    = flag.String("window", "", "Window of data for processing in format `column:from:to`.")
	workers   = flag.Uint("workers", 1, "Count of workers which will execute queries on clickhouse.")
	increment = flag.Bool("increment", false, "If increment is true batch will deduplucate before sending.")
	schedule  = flag.String("schedule", "@midnight", "Cron rule for starting a reader.")
	log       = flag.String("log", "INFO", "Level of logger.")
	cursor    string
	cursorMin string
	cursorMax string
)

// Reading of arguments.
func readConf() (err error) {
	flag.Parse()
	logger.SetLevelStr(*log)

	arr := strings.SplitN(*window, ":", 3)
	if len(arr) == 3 {
		cursor, cursorMin, cursorMax = arr[0], arr[1], arr[2]
	}

	logger.InfoF("Start of clickhouse reader version: %s", version)
	logger.InfoF("Endpoint of data consumer: %s", *comsumer)
	logger.InfoF("Clickhouse endpoint: %s:%d", *host, *port)
	logger.InfoF("Source table: %s", *tableFrom)
	logger.InfoF("Target table: %s", *tableTo)
	logger.InfoF("Selected fields: %s", *fields)
	logger.InfoF("Reading batch size: %d", *batch)
	logger.InfoF("Ordering by: %s", *order)
	logger.InfoF("Window of data for processing is: (cursor: %s, from: %s, to: %s)", cursor, cursorMin, cursorMax)
	logger.InfoF("Workers %d", *workers)
	logger.InfoF("Increment mode: %v", *increment)
	logger.InfoF("Schedule: %s", *schedule)

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
	case *schedule == "":
		errorMessage = "schedule should be not empty"
	}
	if errorMessage != "" {
		return errors.New(errorMessage)
	}
	return nil
}

// Definition of data reading pipeline.
func main() {

	if err := readConf(); err != nil {
		logger.ErrorF("Invalid arguments, error: %v", err)
		os.Exit(1)
	}

	// Initialisation.
	conn := &mysql.Conn{
		Address:  fmt.Sprintf("tcp(%s:%d)/", *host, *port),
		User:     *user,
		Password: *password,
		PoolSize: int(*workers),
	}
	defer conn.Close()

	provider := etl.Provider{
		Fields:    *fields,
		Target:    *tableTo,
		Workers:   int(*workers),
		Increment: *increment,
		Endpoint:  *comsumer,
		CronRule:  *schedule,
		Conn:      conn,
		Generator: sqlGenerator,
	}

	// Waiting of an exit signal.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
	}()

	// Start of a reader as service.
	if err := provider.Up(ctx); err != nil {
		logger.ErrorF("I cannot up a provider, error: %v", err)
		os.Exit(1)
	}

	logger.Info("Have a good day :)")
}

// sqlGenerator is a concurrent query generator.
func sqlGenerator(ctx context.Context, workers int) (queries <-chan string) {

	out := make(chan string, workers)
	go func() {
		defer close(out)

		// Creation of WHERE section.
		var filter string
		if cursor != "" && cursorMin != "" && cursorMax != "" {
			filter = fmt.Sprintf("WHERE %s BETWEEN %s AND %s", cursor, cursorMin, cursorMax)
		}
		logger.DebugF("filter = %s", filter)

		// Creation of ORDER BY section.
		var orderBy string
		if *order != "" {
			orderBy = fmt.Sprintf("ORDER BY %s", *order)
		}
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
