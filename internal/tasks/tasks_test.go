package tasks

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

type testStruct struct {
	TestInt int64 `json:"testInt" binding:"required"`
	TestFloat float64 `json:"testFloat" binding:"required"`
	TestString string `json:"testString" binding:"required"`
}

func TestMarshalAndUnmarshal(t *testing.T) {
	arg := &testStruct{TestInt: 1, TestFloat: 0.5, TestString: "test"}
	marshaled, err := MarshalTaskArg(arg)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(marshaled), 1)
	value := []reflect.Value{reflect.ValueOf(marshaled[0].Value)}
	result := &testStruct{}
	err = UnmarshalTaskResult(value, result)
	assert.Equal(t, err, nil)
	assert.Equal(t, result, arg)
}