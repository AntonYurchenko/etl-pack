package types

import (
	"testing"
)

func TestToUniversal(t *testing.T) {
	type args struct {
		dataType string
		value    string
	}
	tests := []struct {
		name         string
		args         args
		wantNewValue string
		wantErr      bool
	}{
		{name: "DATETIME", args: args{dataType: "DATETIME", value: "2022-06-19 19:47:10"}, wantNewValue: "2022-06-19T19:47:10Z", wantErr: false},
		{name: "DATETIME_err", args: args{dataType: "DATETIME", value: ";kjfoiwrhf"}, wantNewValue: "", wantErr: true},
		{name: "INT", args: args{dataType: "INT", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "DATE", args: args{dataType: "DATE", value: "2022-06-19"}, wantNewValue: "2022-06-19", wantErr: false},
		{name: "TEXT", args: args{dataType: "TEXT", value: "wehfui34r"}, wantNewValue: "wehfui34r", wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNewValue, err := ToUniversal(tt.args.dataType, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToUniversal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotNewValue != tt.wantNewValue {
				t.Errorf("ToUniversal() = %v, want %v", gotNewValue, tt.wantNewValue)
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	type args struct {
		types []string
	}
	tests := []struct {
		name      string
		args      args
		wantTypes []string
	}{
		{
			name: "Upadte of all supported typrs",
			args: args{types: []string{
				"DATETIME",
				"INT",
				"DATE",
				"TEXT",
				"AnyNotSupportType",
			}},
			wantTypes: []string{
				"datetime",
				"int64",
				"date",
				"string",
				"string",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Update(tt.args.types)
		})
	}
}

func TestFromUniversal(t *testing.T) {
	type args struct {
		dataType string
		value    string
	}
	tests := []struct {
		name         string
		args         args
		wantNewValue string
		wantErr      bool
	}{
		{name: "datetime", args: args{dataType: "datetime", value: "2022-06-19T10:00:12Z"}, wantNewValue: "'2022-06-19 10:00:12'", wantErr: false},
		{name: "datetime_err", args: args{dataType: "datetime", value: ";kjfoiwrhf"}, wantNewValue: "", wantErr: true},
		{name: "int64", args: args{dataType: "int64", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "date", args: args{dataType: "date", value: "2022-06-19"}, wantNewValue: "2022-06-19", wantErr: false},
		{name: "string", args: args{dataType: "string", value: "Hello!"}, wantNewValue: "'Hello!'", wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNewValue, err := FromUniversal(tt.args.dataType, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("FromUniversal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotNewValue != tt.wantNewValue {
				t.Errorf("FromUniversal() = %v, want %v", gotNewValue, tt.wantNewValue)
			}
		})
	}
}
