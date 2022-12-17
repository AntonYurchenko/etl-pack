package etl

import (
	"etl/contract"
	"reflect"
	"testing"
)

func Test_hashFunc(t *testing.T) {
	type args struct {
		row [][]byte
	}
	tests := []struct {
		name     string
		args     args
		wantHash string
	}{
		{
			name: "Hash of a sorted row 1 2 3 4",
			args: args{
				row: [][]byte{
					[]byte("1"), []byte("2"), []byte("3"), []byte("4"),
				},
			},
			wantHash: "81dc9bdb52d04dc20036dbd8313ed055",
		},
		{
			name: "Hash of a unsorted row 5 7 6 8",
			args: args{
				row: [][]byte{
					[]byte("5"), []byte("7"), []byte("6"), []byte("8"),
				},
			},
			wantHash: "674f3c2c1a8a6f90461e8a66fb5550ba",
		},
		{
			name: "Hash of sorted row 9 10 11 12",
			args: args{
				row: [][]byte{
					[]byte("9"), []byte("10"), []byte("11"), []byte("12"),
				},
			},
			wantHash: "dfd9bfe9dab8fc185ea63bfdc614f9e7",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotHash := hashFunc(tt.args.row); gotHash != tt.wantHash {
				t.Errorf("hashFunc() = %v, want %v", gotHash, tt.wantHash)
			}
		})
	}
}

func Test_iterByRows(t *testing.T) {
	countCols, countRows := 0, 0
	type args struct {
		batch *contract.Batch
		fn    func(row [][]byte)
	}
	tests := []struct {
		name          string
		args          args
		wantCountCols int
		wantCountRows int
	}{
		{
			name: "Iteration by rows of batch",
			args: args{
				batch: &contract.Batch{
					Names: []string{"col1", "col2", "col3", "col4"},
					Types: []string{"string", "string", "string", "string"},
					Values: [][]byte{
						[]byte("1"), []byte("2"), []byte("3"), []byte("4"),
						[]byte("5"), []byte("6"), []byte("7"), []byte("8"),
						[]byte("9"), []byte("10"), []byte("11"), []byte("12"),
					},
				},
				fn: func(row [][]byte) {
					countCols = len(row)
					countRows++
				},
			},
			wantCountCols: 4,
			wantCountRows: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iterByRows(tt.args.batch, tt.args.fn)
			if countCols != tt.wantCountCols {
				t.Errorf("Calculated countCols = %v, want %v", countCols, tt.wantCountCols)
			}
			if countRows != tt.wantCountRows {
				t.Errorf("Calculated countRows = %v, want %v", countRows, tt.wantCountRows)
			}
		})
	}
}

func Test_filter(t *testing.T) {
	type args struct {
		batch   *contract.Batch
		incDict map[string]bool
	}
	tests := []struct {
		name         string
		args         args
		wantNewBatch *contract.Batch
	}{
		{
			name: "Filter by empty dictionary",
			args: args{
				batch: &contract.Batch{
					Names: []string{"col1", "col2", "col3", "col4"},
					Types: []string{"string", "string", "string", "string"},
					Values: [][]byte{
						[]byte("1"), []byte("2"), []byte("3"), []byte("4"),
						[]byte("5"), []byte("6"), []byte("7"), []byte("8"),
						[]byte("9"), []byte("10"), []byte("11"), []byte("12"),
					},
				},
				incDict: map[string]bool{},
			},
			wantNewBatch: &contract.Batch{
				Names: []string{"col1", "col2", "col3", "col4"},
				Types: []string{"string", "string", "string", "string"},
				Values: [][]byte{
					[]byte("1"), []byte("2"), []byte("3"), []byte("4"),
					[]byte("5"), []byte("6"), []byte("7"), []byte("8"),
					[]byte("9"), []byte("10"), []byte("11"), []byte("12"),
				},
			},
		},
		{
			name: "Filter by nil dictionary",
			args: args{
				batch: &contract.Batch{
					Names: []string{"col1", "col2", "col3", "col4"},
					Types: []string{"string", "string", "string", "string"},
					Values: [][]byte{
						[]byte("1"), []byte("2"), []byte("3"), []byte("4"),
						[]byte("5"), []byte("6"), []byte("7"), []byte("8"),
						[]byte("9"), []byte("10"), []byte("11"), []byte("12"),
					},
				},
				incDict: nil,
			},
			wantNewBatch: &contract.Batch{
				Names: []string{"col1", "col2", "col3", "col4"},
				Types: []string{"string", "string", "string", "string"},
				Values: [][]byte{
					[]byte("1"), []byte("2"), []byte("3"), []byte("4"),
					[]byte("5"), []byte("6"), []byte("7"), []byte("8"),
					[]byte("9"), []byte("10"), []byte("11"), []byte("12"),
				},
			},
		},
		{
			name: "Filter by not empty dictionary with invalid hashes",
			args: args{
				batch: &contract.Batch{
					Names: []string{"col1", "col2", "col3", "col4"},
					Types: []string{"string", "string", "string", "string"},
					Values: [][]byte{
						[]byte("1"), []byte("2"), []byte("3"), []byte("4"),
						[]byte("5"), []byte("6"), []byte("7"), []byte("8"),
						[]byte("9"), []byte("10"), []byte("11"), []byte("12"),
					},
				},
				incDict: map[string]bool{
					"81dc9bdb52d04dc20036db1111111111": true,
				},
			},
			wantNewBatch: &contract.Batch{
				Names: []string{"col1", "col2", "col3", "col4"},
				Types: []string{"string", "string", "string", "string"},
				Values: [][]byte{
					[]byte("1"), []byte("2"), []byte("3"), []byte("4"),
					[]byte("5"), []byte("6"), []byte("7"), []byte("8"),
					[]byte("9"), []byte("10"), []byte("11"), []byte("12"),
				},
			},
		},
		{
			name: "Filter by not empty dictionary with one valid hash",
			args: args{
				batch: &contract.Batch{
					Names: []string{"col1", "col2", "col3", "col4"},
					Types: []string{"string", "string", "string", "string"},
					Values: [][]byte{
						[]byte("1"), []byte("2"), []byte("3"), []byte("4"),
						[]byte("5"), []byte("6"), []byte("7"), []byte("8"),
						[]byte("9"), []byte("10"), []byte("11"), []byte("12"),
					},
				},
				incDict: map[string]bool{
					"81dc9bdb52d04dc20036dbd8313ed055": true,
				},
			},
			wantNewBatch: &contract.Batch{
				Names: []string{"col1", "col2", "col3", "col4"},
				Types: []string{"string", "string", "string", "string"},
				Values: [][]byte{
					[]byte("5"), []byte("6"), []byte("7"), []byte("8"),
					[]byte("9"), []byte("10"), []byte("11"), []byte("12"),
				},
			},
		},
		{
			name: "Filter by not empty dictionary with all valid hashes",
			args: args{
				batch: &contract.Batch{
					Names: []string{"col1", "col2", "col3", "col4"},
					Types: []string{"string", "string", "string", "string"},
					Values: [][]byte{
						[]byte("1"), []byte("2"), []byte("3"), []byte("4"),
						[]byte("5"), []byte("6"), []byte("7"), []byte("8"),
						[]byte("9"), []byte("10"), []byte("11"), []byte("12"),
					},
				},
				incDict: map[string]bool{
					"81dc9bdb52d04dc20036dbd8313ed055": true,
					"674f3c2c1a8a6f90461e8a66fb5550ba": true,
					"dfd9bfe9dab8fc185ea63bfdc614f9e7": true,
				},
			},
			wantNewBatch: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotNewBatch := filter(tt.args.batch, tt.args.incDict); !reflect.DeepEqual(gotNewBatch, tt.wantNewBatch) {
				t.Errorf("filter() = %v, want %v", gotNewBatch, tt.wantNewBatch)
			}
		})
	}
}
