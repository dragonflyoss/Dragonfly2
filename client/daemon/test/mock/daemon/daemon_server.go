// Code generated by MockGen. DO NOT EDIT.
// Source: server.go

// Package mock_server is a generated GoMock package.
package mock_server

import (
	context "context"
	reflect "reflect"

	base "d7y.io/dragonfly/v2/pkg/rpc/base"
	dfdaemon "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	gomock "github.com/golang/mock/gomock"
)

// MockDaemonServer is a mock of DaemonServer interface.
type MockDaemonServer struct {
	ctrl     *gomock.Controller
	recorder *MockDaemonServerMockRecorder
}

// MockDaemonServerMockRecorder is the mock recorder for MockDaemonServer.
type MockDaemonServerMockRecorder struct {
	mock *MockDaemonServer
}

// NewMockDaemonServer creates a new mock instance.
func NewMockDaemonServer(ctrl *gomock.Controller) *MockDaemonServer {
	mock := &MockDaemonServer{ctrl: ctrl}
	mock.recorder = &MockDaemonServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDaemonServer) EXPECT() *MockDaemonServerMockRecorder {
	return m.recorder
}

// CheckHealth mocks base method.
func (m *MockDaemonServer) CheckHealth(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckHealth", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CheckHealth indicates an expected call of CheckHealth.
func (mr *MockDaemonServerMockRecorder) CheckHealth(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckHealth", reflect.TypeOf((*MockDaemonServer)(nil).CheckHealth), arg0)
}

// Download mocks base method.
func (m *MockDaemonServer) Download(arg0 context.Context, arg1 *dfdaemon.DownRequest, arg2 chan<- *dfdaemon.DownResult) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Download", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Download indicates an expected call of Download.
func (mr *MockDaemonServerMockRecorder) Download(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Download", reflect.TypeOf((*MockDaemonServer)(nil).Download), arg0, arg1, arg2)
}

// ExportTask mocks base method.
func (m *MockDaemonServer) ExportTask(arg0 context.Context, arg1 *dfdaemon.ExportTaskRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExportTask", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// ExportTask indicates an expected call of ExportTask.
func (mr *MockDaemonServerMockRecorder) ExportTask(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExportTask", reflect.TypeOf((*MockDaemonServer)(nil).ExportTask), arg0, arg1)
}

// GetPieceTasks mocks base method.
func (m *MockDaemonServer) GetPieceTasks(arg0 context.Context, arg1 *base.PieceTaskRequest) (*base.PiecePacket, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPieceTasks", arg0, arg1)
	ret0, _ := ret[0].(*base.PiecePacket)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPieceTasks indicates an expected call of GetPieceTasks.
func (mr *MockDaemonServerMockRecorder) GetPieceTasks(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPieceTasks", reflect.TypeOf((*MockDaemonServer)(nil).GetPieceTasks), arg0, arg1)
}

// ImportTask mocks base method.
func (m *MockDaemonServer) ImportTask(arg0 context.Context, arg1 *dfdaemon.ImportTaskRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ImportTask", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// ImportTask indicates an expected call of ImportTask.
func (mr *MockDaemonServerMockRecorder) ImportTask(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ImportTask", reflect.TypeOf((*MockDaemonServer)(nil).ImportTask), arg0, arg1)
}

// StatTask mocks base method.
func (m *MockDaemonServer) StatTask(arg0 context.Context, arg1 *dfdaemon.StatTaskRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StatTask", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// StatTask indicates an expected call of StatTask.
func (mr *MockDaemonServerMockRecorder) StatTask(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StatTask", reflect.TypeOf((*MockDaemonServer)(nil).StatTask), arg0, arg1)
}

// SyncPieceTasks mocks base method.
func (m *MockDaemonServer) SyncPieceTasks(arg0 dfdaemon.Daemon_SyncPieceTasksServer) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SyncPieceTasks", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SyncPieceTasks indicates an expected call of SyncPieceTasks.
func (mr *MockDaemonServerMockRecorder) SyncPieceTasks(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SyncPieceTasks", reflect.TypeOf((*MockDaemonServer)(nil).SyncPieceTasks), arg0)
}
