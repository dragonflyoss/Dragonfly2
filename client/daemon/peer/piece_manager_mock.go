// Code generated by MockGen. DO NOT EDIT.
// Source: piece_manager.go
//
// Generated by this command:
//
//	mockgen -destination piece_manager_mock.go -source piece_manager.go -package peer
//

// Package peer is a generated GoMock package.
package peer

import (
	context "context"
	io "io"
	reflect "reflect"

	dfdaemon "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
	scheduler "d7y.io/api/v2/pkg/apis/scheduler/v1"
	storage "d7y.io/dragonfly/v2/client/daemon/storage"
	http "d7y.io/dragonfly/v2/pkg/net/http"
	gomock "go.uber.org/mock/gomock"
)

// MockPieceManager is a mock of PieceManager interface.
type MockPieceManager struct {
	ctrl     *gomock.Controller
	recorder *MockPieceManagerMockRecorder
}

// MockPieceManagerMockRecorder is the mock recorder for MockPieceManager.
type MockPieceManagerMockRecorder struct {
	mock *MockPieceManager
}

// NewMockPieceManager creates a new mock instance.
func NewMockPieceManager(ctrl *gomock.Controller) *MockPieceManager {
	mock := &MockPieceManager{ctrl: ctrl}
	mock.recorder = &MockPieceManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPieceManager) EXPECT() *MockPieceManagerMockRecorder {
	return m.recorder
}

// DownloadPiece mocks base method.
func (m *MockPieceManager) DownloadPiece(ctx context.Context, request *DownloadPieceRequest) (*DownloadPieceResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DownloadPiece", ctx, request)
	ret0, _ := ret[0].(*DownloadPieceResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DownloadPiece indicates an expected call of DownloadPiece.
func (mr *MockPieceManagerMockRecorder) DownloadPiece(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DownloadPiece", reflect.TypeOf((*MockPieceManager)(nil).DownloadPiece), ctx, request)
}

// DownloadSource mocks base method.
func (m *MockPieceManager) DownloadSource(ctx context.Context, pt Task, request *scheduler.PeerTaskRequest, parsedRange *http.Range) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DownloadSource", ctx, pt, request, parsedRange)
	ret0, _ := ret[0].(error)
	return ret0
}

// DownloadSource indicates an expected call of DownloadSource.
func (mr *MockPieceManagerMockRecorder) DownloadSource(ctx, pt, request, parsedRange any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DownloadSource", reflect.TypeOf((*MockPieceManager)(nil).DownloadSource), ctx, pt, request, parsedRange)
}

// Import mocks base method.
func (m *MockPieceManager) Import(ctx context.Context, ptm storage.PeerTaskMetadata, tsd storage.TaskStorageDriver, contentLength int64, reader io.Reader) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Import", ctx, ptm, tsd, contentLength, reader)
	ret0, _ := ret[0].(error)
	return ret0
}

// Import indicates an expected call of Import.
func (mr *MockPieceManagerMockRecorder) Import(ctx, ptm, tsd, contentLength, reader any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Import", reflect.TypeOf((*MockPieceManager)(nil).Import), ctx, ptm, tsd, contentLength, reader)
}

// ImportFile mocks base method.
func (m *MockPieceManager) ImportFile(ctx context.Context, ptm storage.PeerTaskMetadata, tsd storage.TaskStorageDriver, req *dfdaemon.ImportTaskRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ImportFile", ctx, ptm, tsd, req)
	ret0, _ := ret[0].(error)
	return ret0
}

// ImportFile indicates an expected call of ImportFile.
func (mr *MockPieceManagerMockRecorder) ImportFile(ctx, ptm, tsd, req any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ImportFile", reflect.TypeOf((*MockPieceManager)(nil).ImportFile), ctx, ptm, tsd, req)
}
