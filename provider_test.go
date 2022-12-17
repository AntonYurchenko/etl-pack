package etl

import (
	"context"
	"etl/contract"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"
)

func TestProvider_start(t *testing.T) {
	type fields struct {
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
		Conn       Conn
	}
	tests := []struct {
		name        string
		fields      fields
		wantMessage *contract.Message
	}{
		{
			name: "Check work of a provider on mock source Conn",
			fields: fields{
				Fields:    "*",
				Target:    TARGET_STORAGE,
				Workers:   5,
				Increment: true,
				Cursor:    "test_cursor",
				CursorMin: "0",
				CursorMax: "10",
				Endpoint:  "0.0.0.0:24190",
				Generator: func(ctx context.Context, workers int) (queries <-chan string) {
					out := make(chan string)
					go func() {
						defer close(out)
						out <- TARGET_BATCH_VALUE
					}()
					return out
				},
				Conn: &MockConn{},
			},
			wantMessage: &contract.Message{
				Target: TARGET_STORAGE,
				Batch: &contract.Batch{
					Names:  TARGET_BATCH_NAMES,
					Types:  TARGET_BATCH_TYPES,
					Values: [][]byte{[]byte(TARGET_BATCH_VALUE)},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			lis, err := net.Listen("tcp", tt.fields.Endpoint)
			if err != nil {
				t.Errorf("I cannot bind mock consumer on %s, error: %v", tt.fields.Endpoint, err)
			}

			opts := []grpc.ServerOption{}
			server := grpc.NewServer(opts...)
			consumer := MockConsumer{}
			contract.RegisterDataConsumerServer(server, &consumer)

			defer server.Stop()
			go func() {
				if err = server.Serve(lis); err != nil {
					t.Errorf("I cannot up mock consumer on %s, error: %v", tt.fields.Endpoint, err)
				}
			}()

			p := &Provider{
				Fields:     tt.fields.Fields,
				Target:     tt.fields.Target,
				Workers:    tt.fields.Workers,
				Increment:  tt.fields.Increment,
				Cursor:     tt.fields.Cursor,
				CursorMin:  tt.fields.CursorMin,
				CursorMax:  tt.fields.CursorMax,
				CronRule:   tt.fields.CronRule,
				Endpoint:   tt.fields.Endpoint,
				Generator:  tt.fields.Generator,
				grpcConn:   tt.fields.grpcConn,
				grpcClient: tt.fields.grpcClient,
				Conn:       tt.fields.Conn,
			}
			p.start()

			if len(consumer.Messages) != 1 {
				t.Errorf("Invalid count of request messages %d, want: 1", len(consumer.Messages))
			}
			if fmt.Sprintf("%v", consumer.Messages[0]) != fmt.Sprintf("%v", tt.wantMessage) {
				t.Errorf("consumer.Messages[0] = %v, want %v", consumer.Messages[0], tt.wantMessage)
			}

		})
	}
}
