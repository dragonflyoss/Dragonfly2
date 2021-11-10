// Source: d7y.io/dragonfly/v2/scheduler/supervisor (interfaces: HostManager)

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	supervisor "d7y.io/dragonfly/v2/scheduler/supervisor"
	gomock "github.com/golang/mock/gomock"
)

// MockHostManager is a mock of HostManager interface.
type MockHostManager struct {
	ctrl     *gomock.Controller
	recorder *MockHostManagerMockRecorder
}

// MockHostManagerMockRecorder is the mock recorder for MockHostManager.
type MockHostManagerMockRecorder struct {
	mock *MockHostManager
}

// NewMockHostManager creates a new mock instance.
func NewMockHostManager(ctrl *gomock.Controller) *MockHostManager {
	mock := &MockHostManager{ctrl: ctrl}
	mock.recorder = &MockHostManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHostManager) EXPECT() *MockHostManagerMockRecorder {
	return m.recorder
}

// Add mocks base method.
func (m *MockHostManager) Add(arg0 *supervisor.Host) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Add", arg0)
}

// Add indicates an expected call of Add.
func (mr *MockHostManagerMockRecorder) Add(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Add", reflect.TypeOf((*MockHostManager)(nil).Add), arg0)
}

// Delete mocks base method.
func (m *MockHostManager) Delete(arg0 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Delete", arg0)
}

// Delete indicates an expected call of Delete.
func (mr *MockHostManagerMockRecorder) Delete(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockHostManager)(nil).Delete), arg0)
}

// Get mocks base method.
func (m *MockHostManager) Get(arg0 string) (*supervisor.Host, bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", arg0)
	ret0, _ := ret[0].(*supervisor.Host)
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockHostManagerMockRecorder) Get(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockHostManager)(nil).Get), arg0)
}
