// Code generated by MockGen. DO NOT EDIT.
// Source: network_topology.go

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"
	time "time"

	networktopology "d7y.io/dragonfly/v2/scheduler/networktopology"
	gomock "github.com/golang/mock/gomock"
)

// MockNetworkTopology is a mock of NetworkTopology interface.
type MockNetworkTopology struct {
	ctrl     *gomock.Controller
	recorder *MockNetworkTopologyMockRecorder
}

// MockNetworkTopologyMockRecorder is the mock recorder for MockNetworkTopology.
type MockNetworkTopologyMockRecorder struct {
	mock *MockNetworkTopology
}

// NewMockNetworkTopology creates a new mock instance.
func NewMockNetworkTopology(ctrl *gomock.Controller) *MockNetworkTopology {
	mock := &MockNetworkTopology{ctrl: ctrl}
	mock.recorder = &MockNetworkTopologyMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockNetworkTopology) EXPECT() *MockNetworkTopologyMockRecorder {
	return m.recorder
}

// DeleteHost mocks base method.
func (m *MockNetworkTopology) DeleteHost(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteHost", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteHost indicates an expected call of DeleteHost.
func (mr *MockNetworkTopologyMockRecorder) DeleteHost(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteHost", reflect.TypeOf((*MockNetworkTopology)(nil).DeleteHost), arg0)
}

// FindDestHostIDs mocks base method.
func (m *MockNetworkTopology) FindDestHostIDs(arg0 string) []string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FindDestHostIDs", arg0)
	ret0, _ := ret[0].([]string)
	return ret0
}

// FindDestHostIDs indicates an expected call of FindDestHostIDs.
func (mr *MockNetworkTopologyMockRecorder) FindDestHostIDs(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FindDestHostIDs", reflect.TypeOf((*MockNetworkTopology)(nil).FindDestHostIDs), arg0)
}

// Has mocks base method.
func (m *MockNetworkTopology) Has(arg0, arg1 string) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Has", arg0, arg1)
	ret0, _ := ret[0].(bool)
	return ret0
}

// Has indicates an expected call of Has.
func (mr *MockNetworkTopologyMockRecorder) Has(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Has", reflect.TypeOf((*MockNetworkTopology)(nil).Has), arg0, arg1)
}

// ProbedAt mocks base method.
func (m *MockNetworkTopology) ProbedAt(arg0 string) (time.Time, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ProbedAt", arg0)
	ret0, _ := ret[0].(time.Time)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ProbedAt indicates an expected call of ProbedAt.
func (mr *MockNetworkTopologyMockRecorder) ProbedAt(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProbedAt", reflect.TypeOf((*MockNetworkTopology)(nil).ProbedAt), arg0)
}

// ProbedCount mocks base method.
func (m *MockNetworkTopology) ProbedCount(arg0 string) (uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ProbedCount", arg0)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ProbedCount indicates an expected call of ProbedCount.
func (mr *MockNetworkTopologyMockRecorder) ProbedCount(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProbedCount", reflect.TypeOf((*MockNetworkTopology)(nil).ProbedCount), arg0)
}

// Probes mocks base method.
func (m *MockNetworkTopology) Probes(arg0, arg1 string) networktopology.Probes {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Probes", arg0, arg1)
	ret0, _ := ret[0].(networktopology.Probes)
	return ret0
}

// Probes indicates an expected call of Probes.
func (mr *MockNetworkTopologyMockRecorder) Probes(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Probes", reflect.TypeOf((*MockNetworkTopology)(nil).Probes), arg0, arg1)
}

// Serve mocks base method.
func (m *MockNetworkTopology) Serve() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Serve")
	ret0, _ := ret[0].(error)
	return ret0
}

// Serve indicates an expected call of Serve.
func (mr *MockNetworkTopologyMockRecorder) Serve() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Serve", reflect.TypeOf((*MockNetworkTopology)(nil).Serve))
}

// Snapshot mocks base method.
func (m *MockNetworkTopology) Snapshot() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Snapshot")
	ret0, _ := ret[0].(error)
	return ret0
}

// Snapshot indicates an expected call of Snapshot.
func (mr *MockNetworkTopologyMockRecorder) Snapshot() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Snapshot", reflect.TypeOf((*MockNetworkTopology)(nil).Snapshot))
}

// Stop mocks base method.
func (m *MockNetworkTopology) Stop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop")
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockNetworkTopologyMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockNetworkTopology)(nil).Stop))
}

// Store mocks base method.
func (m *MockNetworkTopology) Store(arg0, arg1 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Store", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Store indicates an expected call of Store.
func (mr *MockNetworkTopologyMockRecorder) Store(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Store", reflect.TypeOf((*MockNetworkTopology)(nil).Store), arg0, arg1)
}
