/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package job

import (
	"reflect"
	"testing"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestJob_MarshalResponse(t *testing.T) {
	tests := []struct {
		name   string
		v      any
		expect func(t *testing.T, arg string, err error)
	}{
		{
			name: "marshal common struct",
			v: struct {
				I int64   `json:"i" binding:"required"`
				F float64 `json:"f" binding:"required"`
				S string  `json:"s" binding:"required"`
			}{
				I: 1,
				F: 1.1,
				S: "foo",
			},
			expect: func(t *testing.T, arg string, err error) {
				assert := assert.New(t)
				assert.Equal("{\"i\":1,\"f\":1.1,\"s\":\"foo\"}", arg)
			},
		},
		{
			name: "marshal empty struct",
			v: struct {
				I int64   `json:"i" binding:"omitempty"`
				F float64 `json:"f" binding:"omitempty"`
				S string  `json:"s" binding:"omitempty"`
			}{},
			expect: func(t *testing.T, arg string, err error) {
				assert := assert.New(t)
				assert.Equal("{\"i\":0,\"f\":0,\"s\":\"\"}", arg)
			},
		},
		{
			name: "marshal struct with slice",
			v: struct {
				S []string `json:"s" binding:"required"`
			}{
				S: []string{},
			},
			expect: func(t *testing.T, arg string, err error) {
				assert := assert.New(t)
				assert.Equal("{\"s\":[]}", arg)
			},
		},
		{
			name: "marshal struct with nil slice",
			v: struct {
				S []string `json:"s" binding:"omitempty"`
			}{},
			expect: func(t *testing.T, arg string, err error) {
				assert := assert.New(t)
				assert.Equal("{\"s\":null}", arg)
			},
		},
		{
			name: "marshal nil",
			v:    nil,
			expect: func(t *testing.T, arg string, err error) {
				assert := assert.New(t)
				assert.Equal("null", arg)
			},
		},
		{
			name: "marshal unsupported type",
			v: struct {
				C chan struct{} `json:"c" binding:"required"`
			}{
				C: make(chan struct{}),
			},
			expect: func(t *testing.T, arg string, err error) {
				assert := assert.New(t)
				assert.Equal("json: unsupported type: chan struct {}", err.Error())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			arg, err := MarshalResponse(tc.v)
			tc.expect(t, arg, err)
		})
	}
}

func TestJob_MarshalRequest(t *testing.T) {
	tests := []struct {
		name   string
		value  any
		expect func(t *testing.T, result []machineryv1tasks.Arg, err error)
	}{
		{
			name: "marshal common struct",
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
				assert := assert.New(t)
				assert.Equal([]machineryv1tasks.Arg{{Type: "string", Value: "{\"i\":1,\"f\":1.1,\"s\":\"foo\"}"}}, result)
			},
		},
		{
			name: "marshal empty struct",
			value: struct {
				I int64   `json:"i" binding:"omitempty"`
				F float64 `json:"f" binding:"omitempty"`
				S string  `json:"s" binding:"omitempty"`
			}{},
			expect: func(t *testing.T, result []machineryv1tasks.Arg, err error) {
				assert := assert.New(t)
				assert.Equal([]machineryv1tasks.Arg{{Name: "", Type: "string", Value: "{\"i\":0,\"f\":0,\"s\":\"\"}"}}, result)
			},
		},
		{
			name: "marshal struct with slice",
			value: struct {
				S []string `json:"s" binding:"required"`
			}{
				S: []string{},
			},
			expect: func(t *testing.T, result []machineryv1tasks.Arg, err error) {
				assert := assert.New(t)
				assert.Equal([]machineryv1tasks.Arg{{Name: "", Type: "string", Value: "{\"s\":[]}"}}, result)
			},
		},
		{
			name: "marshal struct with nil slice",
			value: struct {
				S []string `json:"s" binding:"omitempty"`
			}{},
			expect: func(t *testing.T, result []machineryv1tasks.Arg, err error) {
				assert := assert.New(t)
				assert.Equal([]machineryv1tasks.Arg{{Name: "", Type: "string", Value: "{\"s\":null}"}}, result)
			},
		},
		{
			name:  "marshal nil",
			value: nil,
			expect: func(t *testing.T, result []machineryv1tasks.Arg, err error) {
				assert := assert.New(t)
				assert.Equal([]machineryv1tasks.Arg{{Name: "", Type: "string", Value: "null"}}, result)
			},
		},
		{
			name: "marshal unsupported type",
			value: struct {
				C chan struct{} `json:"c" binding:"required"`
			}{
				C: make(chan struct{}),
			},
			expect: func(t *testing.T, result []machineryv1tasks.Arg, err error) {
				assert := assert.New(t)
				assert.Equal("json: unsupported type: chan struct {}", err.Error())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			arg, err := MarshalRequest(tc.value)
			tc.expect(t, arg, err)
		})
	}
}

func TestJob_UnmarshalResponse(t *testing.T) {
	tests := []struct {
		name   string
		data   []reflect.Value
		value  any
		expect func(t *testing.T, result any, err error)
	}{
		{
			name: "unmarshal common struct",
			data: []reflect.Value{
				reflect.ValueOf("{\"i\":1,\"f\":1.1,\"s\":\"foo\"}"),
			},
			value: &struct {
				I int64   `json:"i" binding:"omitempty"`
				F float64 `json:"f" binding:"omitempty"`
				S string  `json:"s" binding:"omitempty"`
			}{},
			expect: func(t *testing.T, result any, err error) {
				assert := assert.New(t)
				assert.Equal(&struct {
					I int64   `json:"i" binding:"omitempty"`
					F float64 `json:"f" binding:"omitempty"`
					S string  `json:"s" binding:"omitempty"`
				}{1, 1.1, "foo"}, result)
			},
		},
		{
			name: "unmarshal struct lack of parameters",
			data: []reflect.Value{
				reflect.ValueOf("{}"),
			},
			value: &struct {
				I int64   `json:"i" binding:"omitempty"`
				F float64 `json:"f" binding:"omitempty"`
				S string  `json:"s" binding:"omitempty"`
			}{},
			expect: func(t *testing.T, result any, err error) {
				assert := assert.New(t)
				assert.Equal(&struct {
					I int64   `json:"i" binding:"omitempty"`
					F float64 `json:"f" binding:"omitempty"`
					S string  `json:"s" binding:"omitempty"`
				}{0, 0, ""}, result)
			},
		},
		{
			name: "unmarshal struct with slice",
			data: []reflect.Value{
				reflect.ValueOf("{\"s\":[]}"),
			},
			value: &struct {
				S []string `json:"s" binding:"required"`
			}{},
			expect: func(t *testing.T, result any, err error) {
				assert := assert.New(t)
				assert.Equal(&struct {
					S []string `json:"s" binding:"required"`
				}{S: []string{}}, result)
			},
		},
		{
			name: "unmarshal struct with nil slice",
			data: []reflect.Value{
				reflect.ValueOf("{\"s\":null}"),
			},
			value: &struct {
				S []string `json:"s" binding:"required"`
			}{},
			expect: func(t *testing.T, result any, err error) {
				assert := assert.New(t)
				assert.Equal(&struct {
					S []string `json:"s" binding:"required"`
				}{S: nil}, result)
			},
		},
		{
			name: "unmarshal nil data",
			data: []reflect.Value{},
			value: &struct {
				S []string `json:"s" binding:"required"`
			}{},
			expect: func(t *testing.T, result any, err error) {
				assert := assert.New(t)
				assert.Equal("empty data is not specified", err.Error())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := UnmarshalResponse(tc.data, tc.value)
			tc.expect(t, tc.value, err)
		})
	}
}

func TestJob_UnmarshalRequest(t *testing.T) {
	tests := []struct {
		name   string
		data   string
		value  any
		expect func(t *testing.T, result any, err error)
	}{
		{
			name: "unmarshal common struct",
			data: "{\"i\":1,\"f\":1.1,\"s\":\"foo\"}",
			value: &struct {
				I int64   `json:"i" binding:"omitempty"`
				F float64 `json:"f" binding:"omitempty"`
				S string  `json:"s" binding:"omitempty"`
			}{},
			expect: func(t *testing.T, result any, err error) {
				assert := assert.New(t)
				assert.Equal(&struct {
					I int64   `json:"i" binding:"omitempty"`
					F float64 `json:"f" binding:"omitempty"`
					S string  `json:"s" binding:"omitempty"`
				}{1, 1.1, "foo"}, result)
			},
		},
		{
			name: "unmarshal empty struct",
			data: "{}",
			value: &struct {
				I int64   `json:"i" binding:"omitempty"`
				F float64 `json:"f" binding:"omitempty"`
				S string  `json:"s" binding:"omitempty"`
			}{},
			expect: func(t *testing.T, result any, err error) {
				assert := assert.New(t)
				assert.Equal(&struct {
					I int64   `json:"i" binding:"omitempty"`
					F float64 `json:"f" binding:"omitempty"`
					S string  `json:"s" binding:"omitempty"`
				}{0, 0, ""}, result)
			},
		},
		{
			name: "unmarshal struct with slice",
			data: "{\"s\":[]}",
			value: &struct {
				S []string `json:"s" binding:"required"`
			}{},
			expect: func(t *testing.T, result any, err error) {
				assert := assert.New(t)
				assert.Equal(&struct {
					S []string `json:"s" binding:"required"`
				}{S: []string{}}, result)
			},
		},
		{
			name: "unmarshal struct with nil slice",
			data: "{\"s\":null}",
			value: &struct {
				S []string `json:"s" binding:"required"`
			}{},
			expect: func(t *testing.T, result any, err error) {
				assert := assert.New(t)
				assert.Equal(&struct {
					S []string `json:"s" binding:"required"`
				}{S: nil}, result)
			},
		},
		{
			name: "unmarshal nil data",
			data: "",
			value: &struct {
				S []string `json:"s" binding:"required"`
			}{},
			expect: func(t *testing.T, result any, err error) {
				assert := assert.New(t)
				assert.Equal("unexpected end of JSON input", err.Error())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := UnmarshalRequest(tc.data, tc.value)
			tc.expect(t, tc.value, err)
		})
	}
}
