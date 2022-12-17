package etl

import (
	"context"
	"etl/contract"
	"io"
)

var (
	TARGET_STORAGE     = "test_target_storege"
	TARGET_BATCH_NAMES = []string{"test_name"}
	TARGET_BATCH_TYPES = []string{"string"}
	TARGET_BATCH_VALUE = "Test message"
)

type MockConn struct{}

func (msc *MockConn) Do(query string) (batch *contract.Batch, err error) {
	batch = &contract.Batch{
		Names:  TARGET_BATCH_NAMES,
		Types:  TARGET_BATCH_TYPES,
		Values: [][]byte{[]byte(TARGET_BATCH_VALUE)},
	}
	return batch, nil
}

type MockConsumer struct {
	contract.UnimplementedDataConsumerServer
	Messages []*contract.Message
}

func (mc *MockConsumer) Exchange(stream contract.DataConsumer_ExchangeServer) (err error) {
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		mc.Messages = append(mc.Messages, message)
		if err = stream.Send(&contract.Status{Success: 1}); err != nil {
			return err
		}
	}
}

func (mc *MockConsumer) GetSnapshot(ctx context.Context, filter *contract.SnapshotFilter) (sh *contract.SnapshotHash,
	err error) {
	sh = &contract.SnapshotHash{
		Set: map[string]bool{},
	}
	return sh, nil
}
