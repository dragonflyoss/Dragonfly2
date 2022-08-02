// Code generated by MockGen. DO NOT EDIT.
// Source: client.go

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	reflect "reflect"

	v1 "d7y.io/api/pkg/apis/common/v1"
	v10 "d7y.io/api/pkg/apis/dfdaemon/v1"
	dfnet "d7y.io/dragonfly/v2/pkg/dfnet"
	client "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	gomock "github.com/golang/mock/gomock"
	grpc "google.golang.org/grpc"
)

// MockDaemonClient is a mock of DaemonClient interface.
type MockDaemonClient struct {
	ctrl     *gomock.Controller
	recorder *MockDaemonClientMockRecorder
}

// MockDaemonClientMockRecorder is the mock recorder for MockDaemonClient.
type MockDaemonClientMockRecorder struct {
	mock *MockDaemonClient
}

// NewMockDaemonClient creates a new mock instance.
func NewMockDaemonClient(ctrl *gomock.Controller) *MockDaemonClient {
	mock := &MockDaemonClient{ctrl: ctrl}
	mock.recorder = &MockDaemonClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDaemonClient) EXPECT() *MockDaemonClientMockRecorder {
	return m.recorder
}

// CheckHealth mocks base method.
func (m *MockDaemonClient) CheckHealth(ctx context.Context, target dfnet.NetAddr, opts ...grpc.CallOption) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, target}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CheckHealth", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// CheckHealth indicates an expected call of CheckHealth.
func (mr *MockDaemonClientMockRecorder) CheckHealth(ctx, target interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, target}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckHealth", reflect.TypeOf((*MockDaemonClient)(nil).CheckHealth), varargs...)
}

// Close mocks base method.
func (m *MockDaemonClient) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockDaemonClientMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockDaemonClient)(nil).Close))
}

// DeleteTask mocks base method.
func (m *MockDaemonClient) DeleteTask(ctx context.Context, req *v10.DeleteTaskRequest, opts ...grpc.CallOption) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, req}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DeleteTask", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteTask indicates an expected call of DeleteTask.
func (mr *MockDaemonClientMockRecorder) DeleteTask(ctx, req interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, req}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteTask", reflect.TypeOf((*MockDaemonClient)(nil).DeleteTask), varargs...)
}

// Download mocks base method.
func (m *MockDaemonClient) Download(ctx context.Context, req *v10.DownRequest, opts ...grpc.CallOption) (*client.DownResultStream, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, req}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Download", varargs...)
	ret0, _ := ret[0].(*client.DownResultStream)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Download indicates an expected call of Download.
func (mr *MockDaemonClientMockRecorder) Download(ctx, req interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, req}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Download", reflect.TypeOf((*MockDaemonClient)(nil).Download), varargs...)
}

// ExportTask mocks base method.
func (m *MockDaemonClient) ExportTask(ctx context.Context, req *v10.ExportTaskRequest, opts ...grpc.CallOption) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, req}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ExportTask", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// ExportTask indicates an expected call of ExportTask.
func (mr *MockDaemonClientMockRecorder) ExportTask(ctx, req interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, req}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExportTask", reflect.TypeOf((*MockDaemonClient)(nil).ExportTask), varargs...)
}

// GetPieceTasks mocks base method.
func (m *MockDaemonClient) GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, ptr *v1.PieceTaskRequest, opts ...grpc.CallOption) (*v1.PiecePacket, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, addr, ptr}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetPieceTasks", varargs...)
	ret0, _ := ret[0].(*v1.PiecePacket)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPieceTasks indicates an expected call of GetPieceTasks.
func (mr *MockDaemonClientMockRecorder) GetPieceTasks(ctx, addr, ptr interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, addr, ptr}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPieceTasks", reflect.TypeOf((*MockDaemonClient)(nil).GetPieceTasks), varargs...)
}

// ImportTask mocks base method.
func (m *MockDaemonClient) ImportTask(ctx context.Context, req *v10.ImportTaskRequest, opts ...grpc.CallOption) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, req}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ImportTask", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// ImportTask indicates an expected call of ImportTask.
func (mr *MockDaemonClientMockRecorder) ImportTask(ctx, req interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, req}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ImportTask", reflect.TypeOf((*MockDaemonClient)(nil).ImportTask), varargs...)
}

// StatTask mocks base method.
func (m *MockDaemonClient) StatTask(ctx context.Context, req *v10.StatTaskRequest, opts ...grpc.CallOption) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, req}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "StatTask", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// StatTask indicates an expected call of StatTask.
func (mr *MockDaemonClientMockRecorder) StatTask(ctx, req interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, req}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StatTask", reflect.TypeOf((*MockDaemonClient)(nil).StatTask), varargs...)
}

// SyncPieceTasks mocks base method.
func (m *MockDaemonClient) SyncPieceTasks(ctx context.Context, addr dfnet.NetAddr, ptr *v1.PieceTaskRequest, opts ...grpc.CallOption) (v10.Daemon_SyncPieceTasksClient, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, addr, ptr}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "SyncPieceTasks", varargs...)
	ret0, _ := ret[0].(v10.Daemon_SyncPieceTasksClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SyncPieceTasks indicates an expected call of SyncPieceTasks.
func (mr *MockDaemonClientMockRecorder) SyncPieceTasks(ctx, addr, ptr interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, addr, ptr}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SyncPieceTasks", reflect.TypeOf((*MockDaemonClient)(nil).SyncPieceTasks), varargs...)
}
