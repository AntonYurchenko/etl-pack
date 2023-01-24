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
		{name: "Int8", args: args{dataType: "Int8", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "Int16", args: args{dataType: "Int16", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "Int32", args: args{dataType: "Int32", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "Int64", args: args{dataType: "Int64", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "UInt8", args: args{dataType: "UInt8", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "UInt16", args: args{dataType: "UInt16", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "UInt32", args: args{dataType: "UInt32", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "UInt64", args: args{dataType: "UInt64", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "Float32", args: args{dataType: "Float32", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "Float64", args: args{dataType: "Float64", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "Date", args: args{dataType: "Date", value: "2022-06-19"}, wantNewValue: "2022-06-19", wantErr: false},
		{name: "DateTime", args: args{dataType: "DateTime", value: "2022-06-19 10:00:12"}, wantNewValue: "2022-06-19T10:00:12Z", wantErr: false},
		{name: "DateTime_err", args: args{dataType: "DateTime", value: ";kjfoiwrhf"}, wantNewValue: "", wantErr: true},
		{name: "String", args: args{dataType: "String", value: "Hello!"}, wantNewValue: "Hello!", wantErr: false},
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
				"DateTime",
				"UInt8",
				"UInt16",
				"UInt32",
				"UInt64",
				"Int8",
				"Int16",
				"Int32",
				"Int64",
				"Date",
				"String",
				"AnyNotSupportType",
			}},
			wantTypes: []string{
				"datetime",
				"uint8",
				"uint16",
				"uint32",
				"uint64",
				"int8",
				"int16",
				"int32",
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
		{name: "int8", args: args{dataType: "int8", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "int16", args: args{dataType: "int16", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "int32", args: args{dataType: "int32", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "int64", args: args{dataType: "int64", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "uint8", args: args{dataType: "uint8", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "uint16", args: args{dataType: "uint16", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "uint32", args: args{dataType: "uint32", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "uint64", args: args{dataType: "uint64", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "float32", args: args{dataType: "float32", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "float64", args: args{dataType: "float64", value: "1"}, wantNewValue: "1", wantErr: false},
		{name: "date", args: args{dataType: "date", value: "2022-06-19"}, wantNewValue: "2022-06-19", wantErr: false},
		{name: "datetime", args: args{dataType: "datetime", value: "2022-06-19T10:00:12Z"}, wantNewValue: "2022-06-19 10:00:12", wantErr: false},
		{name: "datetime_err", args: args{dataType: "datetime", value: ";kjfoiwrhf"}, wantNewValue: "", wantErr: true},
		{name: "string", args: args{dataType: "string", value: "Hello!"}, wantNewValue: "Hello!", wantErr: false},
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
