package mysql

import (
	"database/sql"
	"etl/contract"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql"
)

func toRows(mockRows *sqlmock.Rows) *sql.Rows {
	db, mock, _ := sqlmock.New()
	mock.ExpectQuery("select").WillReturnRows(mockRows)
	rows, _ := db.Query("select")
	return rows
}

func Test_convert(t *testing.T) {

	validRow := toRows(sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("col_1").OfType("INT", nil),
		sqlmock.NewColumn("col_2").OfType("TEXT", nil),
		sqlmock.NewColumn("col_3").OfType("DATETIME", nil),
	).
		AddRow(1000, "Hello", "2022-06-19 19:54:01").
		AddRow(2000, "World", "2022-06-19 19:54:02").
		AddRow(2001, "!", "2022-06-19 19:54:03"),
	)

	inValidRow := toRows(sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("col_3").OfType("DATETIME", nil),
	).
		AddRow("fgegdgfbsfg").
		AddRow("fdvsdfbqerg").
		AddRow("fbsdfvbsdfv"),
	)

	type args struct {
		rows *sql.Rows
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
				rows: validRow,
			},
			wantBatch: &contract.Batch{
				Names: []string{"col_1", "col_2", "col_3"},
				Types: []string{"int64", "string", "datetime"},
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
				rows: inValidRow,
			},
			wantBatch: nil,
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBatch, err := convert(tt.args.rows)
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
