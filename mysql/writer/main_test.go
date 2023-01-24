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
						Types: []string{"int64", "uint32", "string"}, // uint32 is not supported in this version.
						Values: [][]byte{
							[]byte("-1"), []byte("2"), []byte("Hello!"),
							[]byte("-3"), []byte("4"), []byte("World!"),
						},
					},
				},
			},
			wantQuery: etl.InsertBatch{
				CountRows: 2,
				Query:     "INSERT INTO DB.table (col1,col2,col3) VALUES (-1,'2','Hello!'),(-3,'4','World!')",
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

func Test_createHeader(t *testing.T) {
	type args struct {
		target string
		names  []string
	}
	tests := []struct {
		name       string
		args       args
		wantHeader string
	}{
		{
			name: "Query header with columns",
			args: args{
				target: "DB.table",
				names:  []string{"col1", "col2", "col3"},
			},
			wantHeader: "INSERT INTO DB.table (col1,col2,col3) VALUES ",
		},
		{
			name: "Query header without columns",
			args: args{
				target: "DB.table",
				names:  []string{},
			},
			wantHeader: "INSERT INTO DB.table VALUES ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotHeader := createHeader(tt.args.target, tt.args.names); gotHeader != tt.wantHeader {
				t.Errorf("createHeader() = %v, want %v", gotHeader, tt.wantHeader)
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
				cursorMin: "STR_TO_DATE('2022-06-19', '%Y-%m-%d')",
				cursorMax: "STR_TO_DATE('2022-12-12', '%Y-%m-%d')",
			},
			wantQuery: "SELECT * FROM DB.table WHERE event_date BETWEEN STR_TO_DATE('2022-06-19', '%Y-%m-%d') AND STR_TO_DATE('2022-12-12', '%Y-%m-%d')",
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
