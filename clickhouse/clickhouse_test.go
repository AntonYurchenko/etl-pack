package clickhouse

import (
	"etl/contract"
	"reflect"
	"testing"
)

func Test_convert(t *testing.T) {
	type args struct {
		data *Batch
	}
	tests := []struct {
		name      string
		args      args
		wantBatch *contract.Batch
		wantErr   bool
	}{
		{
			name: "Converatation of a valid response from Clickhouse to batch",
			args: args{
				data: &Batch{
					Meta: Meta{
						{"name": "col_1", "type": "UInt32"},
						{"name": "col_2", "type": "String"},
						{"name": "col_3", "type": "DateTime"},
					},
					Rows: Rows{
						{"col_1": 1000, "col_2": "Hello", "col_3": "2022-06-19 19:54:01"},
						{"col_1": 2000, "col_2": "World", "col_3": "2022-06-19 19:54:02"},
						{"col_1": 2001, "col_2": "!", "col_3": "2022-06-19 19:54:03"},
					},
				},
			},
			wantBatch: &contract.Batch{
				Names: []string{"col_1", "col_2", "col_3"},
				Types: []string{"uint32", "string", "datetime"},
				Values: [][]byte{
					[]byte("1000"), []byte("Hello"), []byte("2022-06-19T19:54:01Z"),
					[]byte("2000"), []byte("World"), []byte("2022-06-19T19:54:02Z"),
					[]byte("2001"), []byte("!"), []byte("2022-06-19T19:54:03Z"),
				},
			},
		},
		{
			name: "Converatation of a response with invalid date time format from Clickhouse to batch",
			args: args{
				data: &Batch{
					Meta: Meta{
						{"name": "col_1", "type": "DateTime"},
					},
					Rows: Rows{
						{"col_3": "fgegdgfbsfg"},
						{"col_3": "fdvsdfbqerg"},
						{"col_3": "fbsdfvbsdfv"},
					},
				},
			},
			wantBatch: nil,
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBatch, err := convert(tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("convert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotBatch, tt.wantBatch) {
				t.Errorf("convert() = %v, want %v", gotBatch, tt.wantBatch)
			}
		})
	}
}

func Test_flatMeta(t *testing.T) {
	type args struct {
		meta Meta
	}
	tests := []struct {
		name      string
		args      args
		wantNames []string
		wantTypes []string
	}{
		{
			name: "Check convertation of a meta data from Clickhouse",
			args: args{
				meta: Meta{
					{"name": "col_1", "type": "UInt32"},
					{"name": "col_2", "type": "String"},
					{"name": "col_3", "type": "DateTime"},
				},
			},
			wantNames: []string{"col_1", "col_2", "col_3"},
			wantTypes: []string{"UInt32", "String", "DateTime"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNames, gotTypes := flatMeta(tt.args.meta)
			if !reflect.DeepEqual(gotNames, tt.wantNames) {
				t.Errorf("flatMeta() gotNames = %v, want %v", gotNames, tt.wantNames)
			}
			if !reflect.DeepEqual(gotTypes, tt.wantTypes) {
				t.Errorf("flatMeta() gotTypes = %v, want %v", gotTypes, tt.wantTypes)
			}
		})
	}
}
