// Code generated by MockGen. DO NOT EDIT.
// Source: client_v2.go
//
// Generated by this command:
//
//	mockgen -destination mocks/client_v2_mock.go -source client_v2.go -package mocks
//

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	reflect "reflect"

	v2 "d7y.io/api/v2/pkg/apis/common/v2"
	v20 "d7y.io/api/v2/pkg/apis/dfdaemon/v2"
	gomock "go.uber.org/mock/gomock"
	grpc "google.golang.org/grpc"
)

// MockV2 is a mock of V2 interface.
type MockV2 struct {
	ctrl     *gomock.Controller
	recorder *MockV2MockRecorder
}

// MockV2MockRecorder is the mock recorder for MockV2.
type MockV2MockRecorder struct {
	mock *MockV2
}

// NewMockV2 creates a new mock instance.
func NewMockV2(ctrl *gomock.Controller) *MockV2 {
	mock := &MockV2{ctrl: ctrl}
	mock.recorder = &MockV2MockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockV2) EXPECT() *MockV2MockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockV2) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockV2MockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockV2)(nil).Close))
}

// DeletePersistentCacheTask mocks base method.
func (m *MockV2) DeletePersistentCacheTask(arg0 context.Context, arg1 *v20.DeletePersistentCacheTaskRequest, arg2 ...grpc.CallOption) error {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DeletePersistentCacheTask", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeletePersistentCacheTask indicates an expected call of DeletePersistentCacheTask.
func (mr *MockV2MockRecorder) DeletePersistentCacheTask(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeletePersistentCacheTask", reflect.TypeOf((*MockV2)(nil).DeletePersistentCacheTask), varargs...)
}

// DeleteTask mocks base method.
func (m *MockV2) DeleteTask(arg0 context.Context, arg1 *v20.DeleteTaskRequest, arg2 ...grpc.CallOption) error {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DeleteTask", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteTask indicates an expected call of DeleteTask.
func (mr *MockV2MockRecorder) DeleteTask(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteTask", reflect.TypeOf((*MockV2)(nil).DeleteTask), varargs...)
}

// DownloadPersistentCacheTask mocks base method.
func (m *MockV2) DownloadPersistentCacheTask(arg0 context.Context, arg1 *v20.DownloadPersistentCacheTaskRequest, arg2 ...grpc.CallOption) (v20.DfdaemonUpload_DownloadPersistentCacheTaskClient, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DownloadPersistentCacheTask", varargs...)
	ret0, _ := ret[0].(v20.DfdaemonUpload_DownloadPersistentCacheTaskClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DownloadPersistentCacheTask indicates an expected call of DownloadPersistentCacheTask.
func (mr *MockV2MockRecorder) DownloadPersistentCacheTask(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DownloadPersistentCacheTask", reflect.TypeOf((*MockV2)(nil).DownloadPersistentCacheTask), varargs...)
}

// DownloadPiece mocks base method.
func (m *MockV2) DownloadPiece(arg0 context.Context, arg1 *v20.DownloadPieceRequest, arg2 ...grpc.CallOption) (*v20.DownloadPieceResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DownloadPiece", varargs...)
	ret0, _ := ret[0].(*v20.DownloadPieceResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DownloadPiece indicates an expected call of DownloadPiece.
func (mr *MockV2MockRecorder) DownloadPiece(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DownloadPiece", reflect.TypeOf((*MockV2)(nil).DownloadPiece), varargs...)
}

// DownloadTask mocks base method.
func (m *MockV2) DownloadTask(arg0 context.Context, arg1 string, arg2 *v20.DownloadTaskRequest, arg3 ...grpc.CallOption) (v20.DfdaemonUpload_DownloadTaskClient, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1, arg2}
	for _, a := range arg3 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DownloadTask", varargs...)
	ret0, _ := ret[0].(v20.DfdaemonUpload_DownloadTaskClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DownloadTask indicates an expected call of DownloadTask.
func (mr *MockV2MockRecorder) DownloadTask(arg0, arg1, arg2 any, arg3 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1, arg2}, arg3...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DownloadTask", reflect.TypeOf((*MockV2)(nil).DownloadTask), varargs...)
}

// StatPersistentCacheTask mocks base method.
func (m *MockV2) StatPersistentCacheTask(arg0 context.Context, arg1 *v20.StatPersistentCacheTaskRequest, arg2 ...grpc.CallOption) (*v2.PersistentCacheTask, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "StatPersistentCacheTask", varargs...)
	ret0, _ := ret[0].(*v2.PersistentCacheTask)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StatPersistentCacheTask indicates an expected call of StatPersistentCacheTask.
func (mr *MockV2MockRecorder) StatPersistentCacheTask(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StatPersistentCacheTask", reflect.TypeOf((*MockV2)(nil).StatPersistentCacheTask), varargs...)
}

// StatTask mocks base method.
func (m *MockV2) StatTask(arg0 context.Context, arg1 *v20.StatTaskRequest, arg2 ...grpc.CallOption) (*v2.Task, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "StatTask", varargs...)
	ret0, _ := ret[0].(*v2.Task)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StatTask indicates an expected call of StatTask.
func (mr *MockV2MockRecorder) StatTask(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StatTask", reflect.TypeOf((*MockV2)(nil).StatTask), varargs...)
}

// SyncPieces mocks base method.
func (m *MockV2) SyncPieces(arg0 context.Context, arg1 *v20.SyncPiecesRequest, arg2 ...grpc.CallOption) (v20.DfdaemonUpload_SyncPiecesClient, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "SyncPieces", varargs...)
	ret0, _ := ret[0].(v20.DfdaemonUpload_SyncPiecesClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SyncPieces indicates an expected call of SyncPieces.
func (mr *MockV2MockRecorder) SyncPieces(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SyncPieces", reflect.TypeOf((*MockV2)(nil).SyncPieces), varargs...)
}
