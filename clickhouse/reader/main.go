package main

import (
	"context"
	"etl/clickhouse"
	"etl/contract"
	"flag"
	"fmt"
	"io"
	"sync"
	"time"

	logger "github.com/AntonYurchenko/log-go"
)

const version = "v0.1.0"

// Configuration of reader.
var (
	comsumer  = flag.String("comsumer", "0.0.0.0:24190", "gRPC endpoint of data consumer.")
	host      = flag.String("host", "0.0.0.0", "Connection host.")
	port      = flag.Uint("port", 8123, "Connection port.")
	user      = flag.String("user", "default", "User for connecting.")
	password  = flag.String("password", "", "Password of an user.")
	tableFrom = flag.String("from", "", "Source table in format `data_base.table_name`.")
	tableTo   = flag.String("to", "", "Target table in format `data_base.table_name`.")
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

	logger.InfoF("Start of clickhouse reader version %s", version)
	logger.InfoF("Endpoint of data consumer %s", *comsumer)
	logger.InfoF("Clickhouse endpoint %s:%d", *host, *port)
	logger.InfoF("Source table %s", *tableFrom)
	logger.InfoF("Target table %s", *tableTo)
	logger.InfoF("Workers %d", *workers)
	logger.InfoF("Reading batch size %d", *batch)
	logger.InfoF("Ordering by: %s", *order)
	logger.InfoF("Window of data for processing is: %s", *window)

	// Check of arguments.
	var errorMessage string
	switch {
	case *user == "":
		errorMessage = "User should be not empty"
	case *tableFrom == "":
		errorMessage = "Source table should be not empty"
	case *tableTo == "":
		errorMessage = "Target table should be not empty"
	}
	if errorMessage != "" {
		panic(errorMessage)
	}
}

// Definition of data reading pipeline.
func main() {
	ts := time.Now()

	// Initialisation.
	grpcConn := &GrpcClient{
		Endpoint: *comsumer,
	}
	defer grpcConn.Close()

	chConn := &clickhouse.Conn{
		Address:  fmt.Sprintf("http://%s:%d", *host, *port),
		User:     *user,
		Password: *password,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Building data processing pipeline.
	workerPool, batchSize := int(*workers), int(*batch)
	queries := queryGenerator(ctx, workerPool, batchSize, *tableFrom, *order, *window)
	batches := executor(chConn, queries, workerPool)
	done := sender(grpcConn, *tableTo, batches, workerPool)

	// Waiting of processing
	<-done
	logger.InfoF("Clickhouse reader has been successfull finished [ DURATION: %s ]", time.Since(ts))
	logger.Info("Have a good day :)")
}

// queryGenerator is a concurrent query generator.
func queryGenerator(ctx context.Context, workers, batchSize int, table, order, window string) (queries <-chan string) {
	logger.Info("Start of query generator")

	out := make(chan string, workers)
	go func() {
		defer close(out)
		ts := time.Now()

		// Creation of WHERE section.
		filter, err := createFilter(window)
		if err != nil {
			panic(err)
		}
		logger.DebugF("filter = %s", filter)

		// Creation of ORDER BY section.
		orderBy := createOrderBy(order)
		logger.DebugF("orderBy = %s", orderBy)

		// Generation of queries with offset.
		var offset int
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case out <- fmt.Sprintf("SELECT * FROM %s %s %s LIMIT %d, %d", table, filter, orderBy, offset, batchSize):
				offset += batchSize
				logger.DebugF("offset = %d", offset)
			}
		}

		logger.InfoF("Query generator has been finished [ DURATION: %s ]", time.Since(ts))
	}()

	return out
}

// executor is a concurrent query executor.
func executor(conn *clickhouse.Conn, queries <-chan string, workers int) (batches <-chan *contract.Batch) {
	logger.Info("Start of query executor")

	// Execution of queries.
	out := make(chan *contract.Batch)
	wg := new(sync.WaitGroup)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for query := range queries {

				batch, err := conn.Do(query)
				if err != nil {
					panic(err)
				}

				count := len(batch.Values) / len(batch.Names)
				logger.DebugF("count = %d", count)
				if count == 0 {
					break
				}

				out <- batch
				logger.InfoF("%d row(s) have been read", count)
			}
		}()
	}

	// Waiting of all queries execution.
	go func() {
		defer close(out)
		ts := time.Now()
		wg.Wait()
		logger.InfoF("Query executor has been finished [ DURATION: %s ]", time.Since(ts))
	}()

	return out
}

// converter is a concurrent data converter.
// It converts data batch from clickhouse to data batch for sending to gRPC writer.
// func converter(target string, inBatches <-chan *clickhouse.Batch, workers int) (outBarches <-chan *contract.Batch) {
// 	logger.Info("Start of data converter")

// 	// Convertation of data.
// 	out := make(chan *contract.Batch)
// 	wg := new(sync.WaitGroup)
// 	for i := 0; i < workers; i++ {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			for batch := range inBatches {
// 				out <- convert(target, batch)
// 			}
// 		}()
// 	}

// 	// Waiting of all data convertation.
// 	go func() {
// 		defer close(out)
// 		ts := time.Now()
// 		wg.Wait()
// 		logger.InfoF("Data converter has been finished [ DURATION: %s ]", time.Since(ts))

// 	}()

// 	return out
// }

// sender is a concurrent sender to gRPC writer.
func sender(conn *GrpcClient, target string, batches <-chan *contract.Batch, workers int) (done <-chan struct{}) {
	logger.Info("Start of gRPC sender")

	out := make(chan struct{})
	outStream, err := conn.NewStream()
	if err != nil {
		panic(err)
	}

	// Reaging statuses.
	countRows := make(chan int)
	go func() {
		defer close(countRows)

		for {
			status, err := outStream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				panic(err)
			}
			logger.DebugF("status = %v", status)

			if status.Success == 0 {
				panic(status.Error)
			}
			logger.InfoF("%d row(s) have been written", status.Success)
			countRows <- int(status.Success)
		}
	}()

	// Writting batches.
	go func() {
		defer outStream.CloseSend()

		for batch := range batches {
			message := &contract.Message{Target: target, Batch: batch}
			err := outStream.Send(message)
			if err != nil {
				panic(err)
			}
		}
	}()

	// Waiting of all statuses.
	go func() {
		defer close(out)
		ts := time.Now()
		var count int
		for cr := range countRows {
			count += cr
		}
		logger.InfoF("gRPC sender has been finished [ TOTAL: %d row(s), DURATION: %s ]", count, time.Since(ts))
	}()

	return out
}
