package etl

import (
	"context"
	"etl/contract"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConsumer_Up(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type fields struct {
		Endpoint                        string
		Converter                       func(message *contract.Message) (query InsertBatch, err error)
		SnapshotQuery                   func(fields, table, cursor, cursorMin, cursorMax string) (query string)
		Workers                         int
		UnimplementedDataConsumerServer contract.UnimplementedDataConsumerServer
		Conn                            Conn
	}
	type args struct {
		ctx     context.Context
		message *contract.Message
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Check work of a consumer on mock source Conn",
			fields: fields{
				Endpoint: "0.0.0.0:24191",
				Converter: func(message *contract.Message) (query InsertBatch, err error) {
					return InsertBatch{CountRows: len(message.Batch.Values)}, err
				},
				SnapshotQuery: func(fields, table, cursor, cursorMin, cursorMax string) (query string) {
					return query
				},
				Workers: 5,
				Conn:    &MockConn{},
			},
			args: args{
				ctx: ctx,
				message: &contract.Message{
					Target: TARGET_STORAGE,
					Batch: &contract.Batch{
						Names:  TARGET_BATCH_NAMES,
						Types:  TARGET_BATCH_TYPES,
						Values: [][]byte{[]byte(TARGET_BATCH_VALUE)},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			c := &Consumer{
				Endpoint:                        tt.fields.Endpoint,
				Converter:                       tt.fields.Converter,
				SnapshotQuery:                   tt.fields.SnapshotQuery,
				Workers:                         tt.fields.Workers,
				UnimplementedDataConsumerServer: tt.fields.UnimplementedDataConsumerServer,
				Conn:                            tt.fields.Conn,
			}

			go func() {
				if err := c.Up(tt.args.ctx); (err != nil) != tt.wantErr {
					t.Errorf("Consumer.Up() error = %v, wantErr %v", err, tt.wantErr)
				}
			}()

			opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
			grpcConn, err := grpc.Dial(tt.fields.Endpoint, opts...)
			if err != nil {
				t.Errorf("I cannot connect to consumer %s, error = %v", tt.fields.Endpoint, err)
			}

			grpcClient := contract.NewDataConsumerClient(grpcConn)

			// Exchange
			stream, err := grpcClient.Exchange(tt.args.ctx)
			if err != nil {
				t.Errorf("grpcClient.Exchange() error = %v", err)
			}
			err = stream.Send(tt.args.message)
			if err != nil {
				t.Errorf("stream.Send() error = %v", err)
			}
			status, err := stream.Recv()
			if err != nil {
				t.Errorf("stream.Recv() error = %v", err)
			}
			if status.Success != 1 {
				t.Errorf("Invalid count of saved rows %d, want: 1", status.Success)
			}

			// GetSnapshot
			snapshot, err := grpcClient.GetSnapshot(tt.args.ctx, &contract.SnapshotFilter{})
			if err != nil {
				t.Errorf("grpcClient.GetSnapshot() error = %v", err)
			}
			if len(snapshot.Set) != 1 {
				t.Errorf("Invalid count of hashes in a snapshot set %d, want: 1", len(snapshot.Set))
			}

		})
	}
}
