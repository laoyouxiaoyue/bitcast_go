package utils

import (
	"reflect"
	"testing"
)

func TestGetTestKey(t *testing.T) {
	type args struct {
		i int
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "test1",
			args: args{i: 999},
			want: []byte("bitcast-go-key-999"),
		},
		{
			name: "test2",
			args: args{i: 9989},
			want: []byte("bitcast-go-key-9989"),
		},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetTestKey(tt.args.i); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetTestKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
