// Code generated by MockGen. DO NOT EDIT.
// Source: trainer.go

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockTrainer is a mock of Trainer interface.
type MockTrainer struct {
	ctrl     *gomock.Controller
	recorder *MockTrainerMockRecorder
}

// MockTrainerMockRecorder is the mock recorder for MockTrainer.
type MockTrainerMockRecorder struct {
	mock *MockTrainer
}

// NewMockTrainer creates a new mock instance.
func NewMockTrainer(ctrl *gomock.Controller) *MockTrainer {
	mock := &MockTrainer{ctrl: ctrl}
	mock.recorder = &MockTrainerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTrainer) EXPECT() *MockTrainerMockRecorder {
	return m.recorder
}

// Serve mocks base method.
func (m *MockTrainer) Serve() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Serve")
	ret0, _ := ret[0].(error)
	return ret0
}

// Serve indicates an expected call of Serve.
func (mr *MockTrainerMockRecorder) Serve() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Serve", reflect.TypeOf((*MockTrainer)(nil).Serve))
}

// Stop mocks base method.
func (m *MockTrainer) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockTrainerMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockTrainer)(nil).Stop))
}
