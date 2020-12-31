// Code generated by MockGen. DO NOT EDIT.
// Source: ../../download/manager.go

// Package mock_download is a generated GoMock package.
package mock_download

import (
	net "net"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockManager is a mock of Manager interface
type MockManager struct {
	ctrl     *gomock.Controller
	recorder *MockManagerMockRecorder
}

// MockManagerMockRecorder is the mock recorder for MockManager
type MockManagerMockRecorder struct {
	mock *MockManager
}

// NewMockManager creates a new mock instance
func NewMockManager(ctrl *gomock.Controller) *MockManager {
	mock := &MockManager{ctrl: ctrl}
	mock.recorder = &MockManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockManager) EXPECT() *MockManagerMockRecorder {
	return m.recorder
}

// ServeGRPC mocks base method
func (m *MockManager) ServeDaemon(lis net.Listener) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ServeDaemon", lis)
	ret0, _ := ret[0].(error)
	return ret0
}

// ServeGRPC indicates an expected call of ServeGRPC
func (mr *MockManagerMockRecorder) ServeGRPC(lis interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ServeDaemon", reflect.TypeOf((*MockManager)(nil).ServeDaemon), lis)
}

// ServeProxy mocks base method
func (m *MockManager) ServeProxy(lis net.Listener) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ServeProxy", lis)
	ret0, _ := ret[0].(error)
	return ret0
}

// ServeProxy indicates an expected call of ServeProxy
func (mr *MockManagerMockRecorder) ServeProxy(lis interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ServeProxy", reflect.TypeOf((*MockManager)(nil).ServeProxy), lis)
}

// Stop mocks base method
func (m *MockManager) Stop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop")
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop
func (mr *MockManagerMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockManager)(nil).Stop))
}
