// Code generated by MockGen. DO NOT EDIT.
// Source: training.go

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
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

// GNNTrain mocks base method.
func (m *MockTraining) GNNTrain() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GNNTrain")
	ret0, _ := ret[0].(error)
	return ret0
}

// GNNTrain indicates an expected call of GNNTrain.
func (mr *MockTrainingMockRecorder) GNNTrain() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GNNTrain", reflect.TypeOf((*MockTraining)(nil).GNNTrain))
}

// MLPTrain mocks base method.
func (m *MockTraining) MLPTrain() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MLPTrain")
	ret0, _ := ret[0].(error)
	return ret0
}

// MLPTrain indicates an expected call of MLPTrain.
func (mr *MockTrainingMockRecorder) MLPTrain() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MLPTrain", reflect.TypeOf((*MockTraining)(nil).MLPTrain))
}

// Train mocks base method.
func (m *MockTraining) Train() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Train")
}

// Train indicates an expected call of Train.
func (mr *MockTrainingMockRecorder) Train() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Train", reflect.TypeOf((*MockTraining)(nil).Train))
}
