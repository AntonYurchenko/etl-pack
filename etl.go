package etl

import (
	"context"
	"errors"
	"etl/contract"
	"io"
	"sync"
	"time"

	logger "github.com/AntonYurchenko/log-go"
	"github.com/robfig/cron/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DataConsumer is an input data stream processor.
type DataConsumer struct {
	Converter func(message *contract.Message) (query InsertBatch)
	Workers   int
	contract.UnimplementedDataConsumerServer
	Conn
}

// Exchange is implementation of receiving data from stream and sending them to data processing pipeline.
func (dc *DataConsumer) Exchange(stream contract.DataConsumer_ExchangeServer) (err error) {
	logger.Info("Start processing of exchange stream")
	ts := time.Now()

	messages, statuses := make(chan *contract.Message, dc.Workers), make(chan *contract.Status, dc.Workers)

	// Writting statuses to stream.
	errors := make(chan error)
	go func() {
		defer close(errors)

		for status := range statuses {
			if err := stream.Send(status); err != nil {
				errors <- err
				return
			}
		}
	}()

	// Up data processing pipeline.
	queries := dc.generator(messages)
	dc.executor(statuses, queries)

	// Reading data from stream.
	go func() {
		defer close(messages)

		for {
			message, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				errors <- err
				return
			}
			logger.DebugF("message = %v", message)
			messages <- message
		}
	}()

	// Error handler.
	err = <-errors
	if err != nil {
		logger.WarnF("Processing of exchange stream has been stoped, error: %v", err)
		return err
	}
	logger.InfoF("Processing of exchange stream has been successfull finished [ DURATION: %s ]", time.Since(ts))
	return nil
}

// generator is a concurrent query generator.
func (dc *DataConsumer) generator(messages <-chan *contract.Message) (queries <-chan InsertBatch) {
	logger.Info("Start generator of queries")

	// Generation of queries in work pool.
	out := make(chan InsertBatch, dc.Workers)
	wg := new(sync.WaitGroup)
	for i := 0; i < dc.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for message := range messages {
				out <- dc.Converter(message)
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
func (dc *DataConsumer) executor(statuses chan<- *contract.Status, queries <-chan InsertBatch) {
	logger.Info("Start executer of queries")

	// Execution of all queries.
	countRows := make(chan int, dc.Workers)
	wg := new(sync.WaitGroup)
	for i := 0; i < dc.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for q := range queries {

				if _, err := dc.Conn.Do(q.Query); err != nil {
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

// DataProvider is a producer of output data stream.
type DataProvider struct {
	Target    string
	Workers   int
	CronRule  string
	Generator func(ctx context.Context, workers int) (queries <-chan string)
	Conn
	*GrpcClient
}

// Up is a start function.
func (dp *DataProvider) Up(ctx context.Context) (err error) {
	scheduler := cron.New()

	// Registration of an job and start cron.
	logger.InfoF("Registration a job in scheduler with cron rule: '%s'", dp.CronRule)
	jobID, err := scheduler.AddFunc(dp.CronRule, dp.start)
	if err != nil {
		return err
	}
	logger.DebugF("jobID = %v", jobID)
	scheduler.Start()

	// Waiting of extit from application.
	<-ctx.Done()
	scheduler.Remove(jobID)
	scheduler.Stop()
	logger.InfoF("Sheduler has been stoped.")
	return nil
}

// Up is a start function.
func (dp *DataProvider) start() {
	ts := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Building data processing pipeline.
	queries := dp.Generator(ctx, dp.Workers)
	batches := dp.executor(queries)
	done := dp.sender(batches)

	// Waiting of processing.
	<-done
	logger.InfoF("A reader has been successfull finished [ DURATION: %s ]", time.Since(ts))
}

// executor is a concurrent query executor.
func (dp *DataProvider) executor(queries <-chan string) (batches <-chan *contract.Batch) {
	logger.Info("Start of query executor")

	// Execution of queries.
	out := make(chan *contract.Batch)
	wg := new(sync.WaitGroup)
	for i := 0; i < dp.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for query := range queries {

				batch, err := dp.Conn.Do(query)
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

// sender is a concurrent sender to gRPC writer.
func (dp *DataProvider) sender(batches <-chan *contract.Batch) (done <-chan struct{}) {
	logger.Info("Start of gRPC sender")

	out := make(chan struct{})
	outStream, err := dp.GrpcClient.NewStream()
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
			message := &contract.Message{Target: dp.Target, Batch: batch}
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

// initer is used for one time initialisation.
var initer = new(sync.Once)

// GrpcClient is a gRPC client to writer.
type GrpcClient struct {
	Endpoint string
	conn     *grpc.ClientConn
	client   contract.DataConsumerClient
}

// Close closes gPRC client.
func (gc *GrpcClient) Close() (err error) {
	if gc.conn != nil {
		gc.client = nil
		return gc.conn.Close()
	}
	return nil
}

// NewStream returns a new stream in according with contract.
// See: etl/contract/contract.proto.
func (gc *GrpcClient) NewStream() (stream contract.DataConsumer_ExchangeClient, err error) {

	// Initialisation.
	if gc.Endpoint == "" {
		return nil, errors.New("empty gRPC endpoint")
	}
	initer.Do(func() {
		opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
		gc.conn, err = grpc.Dial(gc.Endpoint, opts...)
		if err != nil {
			return
		}
		gc.client = contract.NewDataConsumerClient(gc.conn)
		logger.Info("Initialisation of a gRPC client")
	})

	return gc.client.Exchange(context.Background())
}
