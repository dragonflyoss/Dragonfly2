// Code generated by MockGen. DO NOT EDIT.
// Source: seed_peer_client.go

// Package resource is a generated GoMock package.
package resource

import (
	context "context"
	reflect "reflect"

	cdnsystem "d7y.io/api/v2/pkg/apis/cdnsystem/v1"
	common "d7y.io/api/v2/pkg/apis/common/v1"
	config "d7y.io/dragonfly/v2/scheduler/config"
	gomock "github.com/golang/mock/gomock"
	grpc "google.golang.org/grpc"
)

// MockSeedPeerClient is a mock of SeedPeerClient interface.
type MockSeedPeerClient struct {
	ctrl     *gomock.Controller
	recorder *MockSeedPeerClientMockRecorder
}

// MockSeedPeerClientMockRecorder is the mock recorder for MockSeedPeerClient.
type MockSeedPeerClientMockRecorder struct {
	mock *MockSeedPeerClient
}

// NewMockSeedPeerClient creates a new mock instance.
func NewMockSeedPeerClient(ctrl *gomock.Controller) *MockSeedPeerClient {
	mock := &MockSeedPeerClient{ctrl: ctrl}
	mock.recorder = &MockSeedPeerClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSeedPeerClient) EXPECT() *MockSeedPeerClientMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockSeedPeerClient) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockSeedPeerClientMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockSeedPeerClient)(nil).Close))
}

// GetPieceTasks mocks base method.
func (m *MockSeedPeerClient) GetPieceTasks(arg0 context.Context, arg1 *common.PieceTaskRequest, arg2 ...grpc.CallOption) (*common.PiecePacket, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetPieceTasks", varargs...)
	ret0, _ := ret[0].(*common.PiecePacket)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPieceTasks indicates an expected call of GetPieceTasks.
func (mr *MockSeedPeerClientMockRecorder) GetPieceTasks(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPieceTasks", reflect.TypeOf((*MockSeedPeerClient)(nil).GetPieceTasks), varargs...)
}

// ObtainSeeds mocks base method.
func (m *MockSeedPeerClient) ObtainSeeds(arg0 context.Context, arg1 *cdnsystem.SeedRequest, arg2 ...grpc.CallOption) (cdnsystem.Seeder_ObtainSeedsClient, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ObtainSeeds", varargs...)
	ret0, _ := ret[0].(cdnsystem.Seeder_ObtainSeedsClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ObtainSeeds indicates an expected call of ObtainSeeds.
func (mr *MockSeedPeerClientMockRecorder) ObtainSeeds(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ObtainSeeds", reflect.TypeOf((*MockSeedPeerClient)(nil).ObtainSeeds), varargs...)
}

// OnNotify mocks base method.
func (m *MockSeedPeerClient) OnNotify(arg0 *config.DynconfigData) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "OnNotify", arg0)
}

// OnNotify indicates an expected call of OnNotify.
func (mr *MockSeedPeerClientMockRecorder) OnNotify(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OnNotify", reflect.TypeOf((*MockSeedPeerClient)(nil).OnNotify), arg0)
}

// SyncPieceTasks mocks base method.
func (m *MockSeedPeerClient) SyncPieceTasks(arg0 context.Context, arg1 *common.PieceTaskRequest, arg2 ...grpc.CallOption) (cdnsystem.Seeder_SyncPieceTasksClient, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "SyncPieceTasks", varargs...)
	ret0, _ := ret[0].(cdnsystem.Seeder_SyncPieceTasksClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SyncPieceTasks indicates an expected call of SyncPieceTasks.
func (mr *MockSeedPeerClientMockRecorder) SyncPieceTasks(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SyncPieceTasks", reflect.TypeOf((*MockSeedPeerClient)(nil).SyncPieceTasks), varargs...)
}
