package tasks

import (
	"reflect"
	"testing"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
)

func TestMarshal(t *testing.T) {
	tests := []struct {
		name   string
		value  interface{}
		expect func(t *testing.T, result []machineryv1tasks.Arg, err error)
	}{
		{
			name: "marshal successed",
			value: struct {
				I int64   `json:"i" binding:"required"`
				F float64 `json:"f" binding:"required"`
				S string  `json:"s" binding:"required"`
			}{
				I: 1,
				F: 1.1,
				S: "foo",
			},
			expect: func(t *testing.T, result []machineryv1tasks.Arg, err error) {

			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			arg, err := Marshal(tc.value)
			tc.expect(t, arg, err)
		})
	}
}

func TestUnmarshal(t *testing.T) {
	tests := []struct {
		name   string
		data   []reflect.Value
		value  interface{}
		expect func(t *testing.T, result interface{}, err error)
	}{
		{
			name: "unmarshal successed",
			data: []reflect.Value{{}},
			value: struct {
				I int64   `json:"i" binding:"required"`
				F float64 `json:"f" binding:"required"`
				S string  `json:"s" binding:"required"`
			}{},
			expect: func(t *testing.T, result interface{}, err error) {

			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Unmarshal(tc.data, tc.value)
			tc.expect(t, tc.value, err)
		})
	}
}
