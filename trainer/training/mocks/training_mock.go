// Code generated by MockGen. DO NOT EDIT.
// Source: training.go
//
// Generated by this command:
//
//	mockgen -destination mocks/training_mock.go -source training.go -package mocks
//

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockTraining is a mock of Training interface.
type MockTraining struct {
	ctrl     *gomock.Controller
	recorder *MockTrainingMockRecorder
}

// MockTrainingMockRecorder is the mock recorder for MockTraining.
type MockTrainingMockRecorder struct {
	mock *MockTraining
}

// NewMockTraining creates a new mock instance.
func NewMockTraining(ctrl *gomock.Controller) *MockTraining {
	mock := &MockTraining{ctrl: ctrl}
	mock.recorder = &MockTrainingMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTraining) EXPECT() *MockTrainingMockRecorder {
	return m.recorder
}

// Train mocks base method.
func (m *MockTraining) Train(arg0 context.Context, arg1, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Train", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Train indicates an expected call of Train.
func (mr *MockTrainingMockRecorder) Train(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Train", reflect.TypeOf((*MockTraining)(nil).Train), arg0, arg1, arg2)
}
