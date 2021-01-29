/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mock

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/golang/mock/gomock"
	"reflect"
)

// MockCDNMgr is a mock of CDNMgr interface
type MockCDNMgr struct {
	ctrl     *gomock.Controller
	recorder *MockCDNMgrMockRecorder
}

// MockCDNMgrMockRecorder is the mock recorder for MockCDNMgr
type MockCDNMgrMockRecorder struct {
	mock *MockCDNMgr
}

// NewMockCDNMgr creates a new mock instance
func NewMockCDNMgr(ctrl *gomock.Controller) *MockCDNMgr {
	mock := &MockCDNMgr{ctrl: ctrl}
	mock.recorder = &MockCDNMgrMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockCDNMgr) EXPECT() *MockCDNMgrMockRecorder {
	return m.recorder
}

// TriggerCDN mocks base method
func (m *MockCDNMgr) TriggerCDN(ctx context.Context, taskInfo *types.SeedTask) (*types.SeedTask, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TriggerCDN", ctx, taskInfo)
	ret0, _ := ret[0].(*types.SeedTask)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TriggerCDN indicates an expected call of TriggerCDN
func (mr *MockCDNMgrMockRecorder) TriggerCDN(ctx, taskInfo interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TriggerCDN", reflect.TypeOf((*MockCDNMgr)(nil).TriggerCDN), ctx, taskInfo)
}

// GetHTTPPath mocks base method
func (m *MockCDNMgr) GetHTTPPath(ctx context.Context, taskInfo *types.SeedTask) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetHTTPPath", ctx, taskInfo)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetHTTPPath indicates an expected call of GetHTTPPath
func (mr *MockCDNMgrMockRecorder) GetHTTPPath(ctx, taskInfo interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetHTTPPath", reflect.TypeOf((*MockCDNMgr)(nil).GetHTTPPath), ctx, taskInfo)
}

// GetStatus mocks base method
func (m *MockCDNMgr) GetStatus(ctx context.Context, taskID string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStatus", ctx, taskID)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStatus indicates an expected call of GetStatus
func (mr *MockCDNMgrMockRecorder) GetStatus(ctx, taskID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStatus", reflect.TypeOf((*MockCDNMgr)(nil).GetStatus), ctx, taskID)
}

// GetGCTaskIDs mocks base method
func (m *MockCDNMgr) GetGCTaskIDs(ctx context.Context, taskMgr mgr.SeedTaskMgr) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetGCTaskIDs", ctx, taskMgr)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetGCTaskIDs indicates an expected call of GetGCTaskIDs
func (mr *MockCDNMgrMockRecorder) GetGCTaskIDs(ctx, taskMgr interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetGCTaskIDs", reflect.TypeOf((*MockCDNMgr)(nil).GetGCTaskIDs), ctx, taskMgr)
}

// GetPieceMD5 mocks base method
func (m *MockCDNMgr) GetPieceMD5(ctx context.Context, taskID string, pieceNum int, pieceRange, source string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPieceMD5", ctx, taskID, pieceNum, pieceRange, source)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPieceMD5 indicates an expected call of GetPieceMD5
func (mr *MockCDNMgrMockRecorder) GetPieceMD5(ctx, taskID, pieceNum, pieceRange, source interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPieceMD5", reflect.TypeOf((*MockCDNMgr)(nil).GetPieceMD5), ctx, taskID, pieceNum, pieceRange, source)
}

// CheckFile mocks base method
func (m *MockCDNMgr) CheckFile(ctx context.Context, taskID string) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckFile", ctx, taskID)
	ret0, _ := ret[0].(bool)
	return ret0
}

// CheckFile indicates an expected call of CheckFile
func (mr *MockCDNMgrMockRecorder) CheckFile(ctx, taskID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckFile", reflect.TypeOf((*MockCDNMgr)(nil).CheckFile), ctx, taskID)
}

// Delete mocks base method
func (m *MockCDNMgr) Delete(ctx context.Context, taskID string, force bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", ctx, taskID, force)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete
func (mr *MockCDNMgrMockRecorder) Delete(ctx, taskID, force interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockCDNMgr)(nil).Delete), ctx, taskID, force)
}
