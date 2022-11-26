package main

import (
	"etl"
	"etl/clickhouse"
	"etl/contract"
	"flag"
	"fmt"
	"net"
	"sync"
	"time"

	logger "github.com/AntonYurchenko/log-go"
	"google.golang.org/grpc"
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
		panic(errorMessage)
	}
}

func main() {

	// Initialisation.
	lis, err := net.Listen("tcp", *endpoint)
	if err != nil {
		panic(err)
	}

	consumer := etl.DataConsumer{
		Conn: &clickhouse.Conn{
			Address:  fmt.Sprintf("http://%s:%d", *host, *port),
			User:     *user,
			Password: *password,
		},
		Workers: int(*workers),
		Pipeline: etl.Pipeline{
			Generator: queryGenerator,
			Executor:  executor,
		},
	}

	opts := []grpc.ServerOption{}
	server := grpc.NewServer(opts...)
	contract.RegisterDataConsumerServer(server, &consumer)

	// Up gRPC server.
	server.Serve(lis)

}

// DataConsumer is a service for processing data stream.
// type DataConsumer struct {
// 	Conn    *clickhouse.Conn
// 	Workers int
// 	contract.UnimplementedDataConsumerServer
// }

// // Exchange is implementation of receiving data from stream and sending them to data processing pipeline.
// func (dc *DataConsumer) Exchange(stream contract.DataConsumer_ExchangeServer) (err error) {
// 	logger.Info("Start processing of exchange stream")
// 	ts := time.Now()

// 	messages, statuses := make(chan *contract.Message, dc.Workers), make(chan *contract.Status, dc.Workers)

// 	// Writting statuses to stream.
// 	errors := make(chan error)
// 	go func() {
// 		defer close(errors)

// 		for status := range statuses {
// 			if err := stream.Send(status); err != nil {
// 				errors <- err
// 				return
// 			}
// 		}
// 	}()

// 	// Up data processing pipeline.
// 	queries := queryGenerator(messages, dc.Workers)
// 	executor(dc.Conn, statuses, queries, dc.Workers)

// 	// Reading data from stream.
// 	go func() {
// 		defer close(messages)

// 		for {
// 			batch, err := stream.Recv()
// 			if err == io.EOF {
// 				return
// 			}
// 			if err != nil {
// 				errors <- err
// 				return
// 			}
// 			logger.DebugF("batch = %v", batch)
// 			messages <- batch
// 		}
// 	}()

// 	// Error handler.
// 	err = <-errors
// 	if err != nil {
// 		logger.WarnF("Processing of exchange stream has been stoped, error: %v", err)
// 		return err
// 	}
// 	logger.InfoF("Processing of exchange stream has been successfull finished [ DURATION: %s ]", time.Since(ts))
// 	return nil
// }

// InsertBatch is an entity which defines a batch for saving to clickhouse.
// It contains a query and count of rows for saving to clickhouse.
// type InsertBatch struct {
// 	query     string
// 	countRows int
// }

// queryGenerator is a concurrent query generator.
func queryGenerator(messages <-chan *contract.Message, workers int) (queries <-chan etl.InsertBatch) {
	logger.Info("Start generator of queries")

	// Generation of queries in work pool.
	out := make(chan etl.InsertBatch, workers)
	wg := new(sync.WaitGroup)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Processing each batch.
			for message := range messages {
				count := len(message.Batch.Values) / len(message.Batch.Names)
				logger.InfoF("%d row(s) have been read from stream", count)
				sql := createHeader(message.Target, message.Batch.Names)

				// Adding all values of batch to sql query.
				for idx, value := range message.Batch.Values {
					typeIdx := idx % len(message.Batch.Names)
					if typeIdx == 0 {
						sql += "\n"
					} else {
						sql += "\t"
					}
					strValue, err := transform(message.Batch.Types[typeIdx], string(value))
					if err != nil {
						panic(err)
					}
					sql += string(strValue)
				}

				out <- etl.InsertBatch{Query: sql, CountRows: count}
			}
		}()
	}

	// Waiting processing of all batches.
	go func() {
		defer close(out)
		ts := time.Now()
		wg.Wait()
		logger.InfoF("Query generator has been finished [ DURATION: %s ]", time.Since(ts))
	}()

	return out
}

// executor is a concurrent query executor.
func executor(conn etl.Conn, statuses chan<- *contract.Status, queries <-chan etl.InsertBatch, workers int) {
	logger.Info("Start executer of queries")

	// Execution of all queries.
	countRows := make(chan int, workers)
	wg := new(sync.WaitGroup)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for q := range queries {

				if _, err := conn.Do(q.Query); err != nil {
					statuses <- &contract.Status{Error: err.Error()}
					return
				}

				statuses <- &contract.Status{Success: int32(q.CountRows)}
				countRows <- q.CountRows
				logger.InfoF("%d row(s) have been written", q.CountRows)
			}
		}()
	}

	// Waiting of all queries execution.
	go func() {
		defer close(countRows)
		wg.Wait()
	}()

	// Calculation of statictic.
	go func() {
		defer close(statuses)
		ts := time.Now()
		var total int
		for count := range countRows {
			total += count
		}
		logger.InfoF("Query executor has been finished [ TOTAL: %d row(s), DURATION: %s ]", total, time.Since(ts))
	}()
}
