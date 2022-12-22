package main

import (
	"context"
	"testing"
)

func Test_sqlGenerator(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type args struct {
		ctx       context.Context
		workers   int
		fields    string
		table     string
		cursor    string
		cursorMin string
		cursorMax string
		orderBy   string
	}
	tests := []struct {
		name      string
		args      args
		wantQuery string
	}{
		{
			name: "Generation of a simple query for data reading",
			args: args{
				ctx:     ctx,
				workers: 1,
				fields:  "*",
				table:   "DB.table",
			},
			wantQuery: "SELECT * FROM DB.table   LIMIT 0, 1000",
		},
		{
			name: "Generation of a query with filter for data reading",
			args: args{
				ctx:       ctx,
				workers:   1,
				fields:    "*",
				table:     "DB.table",
				cursor:    "event_date",
				cursorMin: "toDate('2022-06-19')",
				cursorMax: "today()",
			},
			wantQuery: "SELECT * FROM DB.table WHERE event_date BETWEEN toDate('2022-06-19') AND today()  LIMIT 0, 1000",
		},
		{
			name: "Generation of a query with order by section for data reading",
			args: args{
				ctx:     ctx,
				workers: 1,
				fields:  "*",
				table:   "DB.table",
				orderBy: "evenr_date DESC",
			},
			wantQuery: "SELECT * FROM DB.table  ORDER BY evenr_date DESC LIMIT 0, 1000",
		},
		{
			name: "Generation of a query with filter and order by section for data reading",
			args: args{
				ctx:       ctx,
				workers:   1,
				fields:    "*",
				table:     "DB.table",
				cursor:    "event_date",
				cursorMin: "toDate('2022-06-19')",
				cursorMax: "today()",
				orderBy: "evenr_date DESC",
			},
			wantQuery: "SELECT * FROM DB.table WHERE event_date BETWEEN toDate('2022-06-19') AND today() ORDER BY evenr_date DESC LIMIT 0, 1000",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			*fields, *tableFrom, *order = tt.args.fields, tt.args.table, tt.args.orderBy
			cursor, cursorMin, cursorMax = tt.args.cursor, tt.args.cursorMin, tt.args.cursorMax
			query := <-sqlGenerator(tt.args.ctx, tt.args.workers)
			if query != tt.wantQuery {
				t.Errorf("sqlGenerator(), query = %v, want %v", query, tt.wantQuery)
			}
		})
	}
}
