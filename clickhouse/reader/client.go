package main

import (
	"context"
	"errors"
	"etl/contract"
	"sync"

	logger "github.com/AntonYurchenko/log-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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
