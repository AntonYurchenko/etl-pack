package etl

import (
	"context"
	"etl/contract"
	"io"
	"net"
	"sync"
	"time"

	logger "github.com/AntonYurchenko/log-go"
	"google.golang.org/grpc"
)

// Consumer is an input data stream processor.
type Consumer struct {
	Endpoint      string
	Converter     func(message *contract.Message) (query InsertBatch, err error)
	SnapshotQuery func(fields, table, cursor, cursorMin, cursorMax string) (query string)
	Workers       int
	contract.UnimplementedDataConsumerServer
	Conn
}

// Exchange is implementation of receiving data from stream and sending them to data processing pipeline.
func (c *Consumer) Exchange(stream contract.DataConsumer_ExchangeServer) (err error) {
	logger.Info("Start processing of exchange stream")
	ts := time.Now()

	messages, statuses := make(chan *contract.Message, c.Workers), make(chan *contract.Status, c.Workers)

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
	queries := c.generator(messages)
	c.executor(statuses, queries)

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

// GetSnapshot is implementation of funcrion for receiving a set of hash all rows of snapshot from target entity.
func (c *Consumer) GetSnapshot(ctx context.Context, filter *contract.SnapshotFilter) (sh *contract.SnapshotHash,
	err error) {

	// Receiving data.
	batch, err := c.Conn.Do(c.SnapshotQuery(filter.Fields, filter.Target, filter.Column, filter.From, filter.To))
	if err != nil {
		return nil, err
	}

	// Initialisation.
	sh = new(contract.SnapshotHash)
	sh.Set = map[string]bool{}

	// Calculation of a hashes foreache row.
	iterByRows(batch, func(row [][]byte) {
		hash := hashFunc(row)
		sh.Set[hash] = true
	})

	return sh, nil
}

// Up is a start function.
func (c *Consumer) Up(ctx context.Context) (err error) {

	// Initialisation.
	lis, err := net.Listen("tcp", c.Endpoint)
	if err != nil {
		return err
	}

	opts := []grpc.ServerOption{}
	server := grpc.NewServer(opts...)
	contract.RegisterDataConsumerServer(server, c)

	// Up gRPC server.
	failure := make(chan error)
	go func() {
		defer close(failure)
		if err = server.Serve(lis); err != nil {
			failure <- err
		}
	}()

	select {
	case <-ctx.Done():
		server.Stop()
		return nil
	case err = <-failure:
		return err
	}
}

// generator is a concurrent query generator.
func (c *Consumer) generator(messages <-chan *contract.Message) (queries <-chan InsertBatch) {
	logger.Info("Start generator of queries")

	// Generation of queries in work pool.
	out := make(chan InsertBatch, c.Workers)
	wg := new(sync.WaitGroup)
	for i := 0; i < c.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for message := range messages {
				insertBatch, err := c.Converter(message)
				if err != nil {
					logger.ErrorF("I cannot convert a data batch to query, error: %v", err)
					return
				}
				out <- insertBatch
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
func (c *Consumer) executor(statuses chan<- *contract.Status, queries <-chan InsertBatch) {
	logger.Info("Start executer of queries")

	// Execution of all queries.
	countRows := make(chan int, c.Workers)
	wg := new(sync.WaitGroup)
	for i := 0; i < c.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for q := range queries {

				if _, err := c.Conn.Do(q.Query); err != nil {
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
