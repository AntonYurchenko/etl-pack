package etl

import (
	"etl/contract"
	"io"
	"time"

	logger "github.com/AntonYurchenko/log-go"
)

// DataConsumer is a service for processing data stream.
type DataConsumer struct {
	Workers int
	contract.UnimplementedDataConsumerServer
	Pipeline
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
	queries := dc.Generator(messages, dc.Workers)
	dc.Executor(dc.Conn, statuses, queries, dc.Workers)

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

// Conn is an universal connection interface to all storages
type Conn interface {
	Do(query string) (batch *contract.Batch, err error)
}

// Pipeline defines a pipeline of saving data batches to storage.
type Pipeline struct {
	Generator func(messages <-chan *contract.Message, workers int) (queries <-chan InsertBatch)
	Executor  func(conn Conn, statuses chan<- *contract.Status, queries <-chan InsertBatch, workers int)
}

// InsertBatch is an entity which defines a batch for saving to storage.
// It contains a query and count of rows for saving to storage.
type InsertBatch struct {
	Query     string
	CountRows int
}
