package etl

import (
	"context"
	"etl/contract"
	"io"
	"sync"
	"time"

	logger "github.com/AntonYurchenko/log-go"
	"github.com/robfig/cron/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Provider is a producer of output data stream.
type Provider struct {
	Fields     string
	Target     string
	Workers    int
	Increment  bool
	Cursor     string
	CursorMin  string
	CursorMax  string
	CronRule   string
	Endpoint   string
	Generator  func(ctx context.Context, workers int) (queries <-chan string)
	grpcConn   *grpc.ClientConn
	grpcClient contract.DataConsumerClient
	Conn
}

// connect is an initialisation of gRPC connection to server.
func (p *Provider) connect() (err error) {

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	p.grpcConn, err = grpc.Dial(p.Endpoint, opts...)
	if err != nil {
		return err
	}

	p.grpcClient = contract.NewDataConsumerClient(p.grpcConn)
	logger.Info("Initialisation of a gRPC client")
	return nil
}

// Close closes gPRC client.
func (p *Provider) disconnect() (err error) {
	if p.grpcConn != nil {
		p.grpcClient = nil
		return p.grpcConn.Close()
	}
	return nil
}

// newStream returns a new stream in according with contract.
// See: etl/contract/contract.proto.
func (p *Provider) newStream() (stream contract.DataConsumer_ExchangeClient, err error) {
	return p.grpcClient.Exchange(context.Background())
}

// getSnapshot returns a snapshot hash in according with contract.
// See: etl/contract/contract.proto.
func (p *Provider) getSnapshot(filter *contract.SnapshotFilter) (sh *contract.SnapshotHash, err error) {
	return p.grpcClient.GetSnapshot(context.Background(), filter)
}

// Up is a start function.
func (p *Provider) Up(ctx context.Context) (err error) {
	scheduler := cron.New()

	// Registration of an job and start cron.
	logger.InfoF("Registration a job in scheduler with cron rule: '%s'", p.CronRule)
	jobID, err := scheduler.AddFunc(p.CronRule, p.start)
	if err != nil {
		return err
	}
	logger.DebugF("jobID = %v", jobID)
	scheduler.Start()

	// Waiting of extit from application.
	<-ctx.Done()
	scheduler.Remove(jobID)
	scheduler.Stop()
	logger.InfoF("Sheduler has been stoped")
	return nil
}

// Up is a start function.
func (p *Provider) start() {
	ts := time.Now()

	if err := p.connect(); err != nil {
		logger.ErrorF("Ooops! I cannot connect to consumer '%s', error: %v", p.Endpoint, err)
		return
	}
	defer p.disconnect()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Building data processing pipeline.
	queries := p.Generator(ctx, p.Workers)
	batches := p.executor(queries)
	done := p.sender(batches)

	// Waiting of processing.
	<-done
	logger.InfoF("A reader has been successfull finished [ DURATION: %s ]", time.Since(ts))
}

// executor is a concurrent query executor.
func (p *Provider) executor(queries <-chan string) (batches <-chan *contract.Batch) {
	logger.Info("Start of query executor")

	// Execution of queries.
	out := make(chan *contract.Batch)
	wg := new(sync.WaitGroup)
	for i := 0; i < p.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for query := range queries {

				batch, err := p.Conn.Do(query)
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
func (p *Provider) sender(batches <-chan *contract.Batch) (done <-chan struct{}) {
	logger.Info("Start of gRPC sender")

	// Receiving increment dictionary.
	var incrementDictionary map[string]bool
	if p.Increment {
		logger.Info("Receiving a snapshot hash set")
		filter := &contract.SnapshotFilter{
			Fields: p.Fields,
			Target: p.Target,
			Column: p.Cursor,
			From:   p.CursorMin,
			To:     p.CursorMax,
		}
		snapshotHash, err := p.getSnapshot(filter)
		if err != nil {
			panic(err)
		}
		incrementDictionary = snapshotHash.Set
	}
	logger.DebugF("incrementDictionary = %v", incrementDictionary)

	out := make(chan struct{})
	outStream, err := p.newStream()
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
			newBatch := filter(batch, incrementDictionary)
			if newBatch != nil {
				message := &contract.Message{Target: p.Target, Batch: newBatch}
				err := outStream.Send(message)
				if err != nil {
					panic(err)
				}
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
