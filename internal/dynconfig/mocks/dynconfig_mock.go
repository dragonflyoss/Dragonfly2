// Code generated by MockGen. DO NOT EDIT.
// Source: dynconfig.go

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// Mockstrategy is a mock of strategy interface.
type Mockstrategy struct {
	ctrl     *gomock.Controller
	recorder *MockstrategyMockRecorder
}

// MockstrategyMockRecorder is the mock recorder for Mockstrategy.
type MockstrategyMockRecorder struct {
	mock *Mockstrategy
}

// NewMockstrategy creates a new mock instance.
func NewMockstrategy(ctrl *gomock.Controller) *Mockstrategy {
	mock := &Mockstrategy{ctrl: ctrl}
	mock.recorder = &MockstrategyMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *Mockstrategy) EXPECT() *MockstrategyMockRecorder {
	return m.recorder
}

// Unmarshal mocks base method.
func (m *Mockstrategy) Unmarshal(rawVal any) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Unmarshal", rawVal)
	ret0, _ := ret[0].(error)
	return ret0
}

// Unmarshal indicates an expected call of Unmarshal.
func (mr *MockstrategyMockRecorder) Unmarshal(rawVal interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Unmarshal", reflect.TypeOf((*Mockstrategy)(nil).Unmarshal), rawVal)
}
