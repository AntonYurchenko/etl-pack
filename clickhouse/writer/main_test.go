package main

import (
	"etl"
	"etl/contract"
	"reflect"
	"testing"
)

func Test_messageToQuery(t *testing.T) {
	type args struct {
		message *contract.Message
	}
	tests := []struct {
		name      string
		args      args
		wantQuery etl.InsertBatch
		wantErr   bool
	}{
		{
			name: "Convertation of a message",
			args: args{
				message: &contract.Message{
					Target: "DB.table",
					Batch: &contract.Batch{
						Names: []string{"col1", "col2", "col3"},
						Types: []string{"int32", "uint32", "string"},
						Values: [][]byte{
							[]byte("-1"), []byte("2"), []byte("Hello!"),
							[]byte("-3"), []byte("4"), []byte("World!"),
						},
					},
				},
			},
			wantQuery: etl.InsertBatch{
				CountRows: 2,
				Query:     "INSERT INTO DB.table (col1,col2,col3) FORMAT TSV\n-1\t2\tHello!\n-3\t4\tWorld!",
			},
		},
		{
			name: "Convertation of a message with error",
			args: args{
				message: &contract.Message{
					Target: "DB.table",
					Batch: &contract.Batch{
						Names: []string{"col1"},
						Types: []string{"datetime"},
						Values: [][]byte{
							[]byte("-1"),
							[]byte("-3"),
						},
					},
				},
			},
			wantQuery: etl.InsertBatch{},
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotQuery, err := messageToQuery(tt.args.message)
			if (err != nil) != tt.wantErr {
				t.Errorf("messageToQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotQuery, tt.wantQuery) {
				t.Errorf("messageToQuery() = %v, want %v", gotQuery, tt.wantQuery)
			}
		})
	}
}

func Test_createSnapshotQuery(t *testing.T) {
	type args struct {
		fields    string
		table     string
		cursor    string
		cursorMin string
		cursorMax string
	}
	tests := []struct {
		name      string
		args      args
		wantQuery string
	}{
		{
			name: "Simple snapshot query",
			args: args{
				fields: "*",
				table:  "DB.table",
			},
			wantQuery: "SELECT * FROM DB.table ",
		},
		{
			name: "Snapshot query with filter",
			args: args{
				fields: "*",
				table:  "DB.table",
				cursor: "event_date",
				cursorMin: "'2022-06-19'",
				cursorMax: "'2022-12-12'",
			},
			wantQuery: "SELECT * FROM DB.table WHERE event_date BETWEEN '2022-06-19' AND '2022-12-12'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotQuery := createSnapshotQuery(tt.args.fields, tt.args.table, tt.args.cursor, tt.args.cursorMin, tt.args.cursorMax); gotQuery != tt.wantQuery {
				t.Errorf("createSnapshotQuery() = %v, want %v", gotQuery, tt.wantQuery)
			}
		})
	}
}
