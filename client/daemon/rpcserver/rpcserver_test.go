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

package rpcserver

import (
	"context"
	"io"
	"net"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/distribution/distribution/v3/uuid"
	testifyassert "github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/daemon/storage/mocks"
	"d7y.io/dragonfly/v2/client/util"
	utilmocks "d7y.io/dragonfly/v2/client/util/mocks"
	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	dfdaemonclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	dfdaemonserver "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	schedulerclientmocks "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client/mocks"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

func TestServer_New(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name   string
		mock   func(mockKeepAlive *utilmocks.MockKeepAliveMockRecorder)
		expect func(t *testing.T, err error)
	}{
		{
			name: "init success",
			expect: func(t *testing.T, err error) {
				assert := testifyassert.New(t)
				assert.Nil(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockpeerHost := &schedulerv1.PeerHost{}
			mockpeerTaskManager := peer.NewMockTaskManager(ctrl)
			mockStorageManger := mocks.NewMockManager(ctrl)
			mockSchedulerClient := schedulerclientmocks.NewMockV1(ctrl)
			var mockdownloadOpts []grpc.ServerOption
			var mockpeerOpts []grpc.ServerOption
			_, err := New(mockpeerHost, mockpeerTaskManager, mockStorageManger, mockSchedulerClient, 16, 0, mockdownloadOpts, mockpeerOpts)
			tc.expect(t, err)
		})
	}
}

func TestServer_DeleteTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name   string
		r      *dfdaemonv1.DeleteTaskRequest
		mock   func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager)
		expect func(t *testing.T, r *dfdaemonv1.DeleteTaskRequest, err error)
	}{
		{
			name: "task not found, skip delete",
			r: &dfdaemonv1.DeleteTaskRequest{
				UrlMeta: &commonv1.UrlMeta{},
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mocktsd *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(nil)
			},
			expect: func(t *testing.T, r *dfdaemonv1.DeleteTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Nil(err)
			},
		},
		{
			name: "failed to UnregisterTask",
			r: &dfdaemonv1.DeleteTaskRequest{
				UrlMeta: &commonv1.UrlMeta{},
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mocktsd *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(&storage.ReusePeerTask{})
				mockStorageManger.UnregisterTask(gomock.Any(), gomock.Any()).Return(dferrors.ErrInvalidArgument)
			},
			expect: func(t *testing.T, r *dfdaemonv1.DeleteTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Error(err)
			},
		},
		{
			name: "success",
			r: &dfdaemonv1.DeleteTaskRequest{
				UrlMeta: &commonv1.UrlMeta{},
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mocktsd *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(&storage.ReusePeerTask{})
				mockStorageManger.UnregisterTask(gomock.Any(), gomock.Any()).Return(nil)
			},
			expect: func(t *testing.T, r *dfdaemonv1.DeleteTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Nil(err)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStorageManger := mocks.NewMockManager(ctrl)
			mockTaskManager := peer.NewMockTaskManager(ctrl)
			mocktsd := mocks.NewMockTaskStorageDriver(ctrl)
			pieceManager := peer.NewMockPieceManager(ctrl)
			tc.mock(mockStorageManger.EXPECT(), mockTaskManager.EXPECT(), mocktsd, pieceManager)
			s := &server{
				KeepAlive:       util.NewKeepAlive("test"),
				peerHost:        &schedulerv1.PeerHost{},
				storageManager:  mockStorageManger,
				peerTaskManager: mockTaskManager,
			}
			_, err := s.DeleteTask(context.Background(), tc.r)
			tc.expect(t, tc.r, err)
		})
	}
}

func TestServer_ExportTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name   string
		r      *dfdaemonv1.ExportTaskRequest
		mock   func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager)
		expect func(t *testing.T, r *dfdaemonv1.ExportTaskRequest, err error)
	}{
		{
			name: "use local cache and task doesn't exist, return error",
			r: &dfdaemonv1.ExportTaskRequest{
				UrlMeta:   &commonv1.UrlMeta{},
				LocalOnly: true,
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mocktsd *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(nil)
			},
			expect: func(t *testing.T, r *dfdaemonv1.ExportTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.True(dferrors.CheckError(err, commonv1.Code_PeerTaskNotFound))
			},
		},
		{
			name: "task doesn't exist, return error",
			r: &dfdaemonv1.ExportTaskRequest{
				UrlMeta:   &commonv1.UrlMeta{},
				LocalOnly: false,
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mocktsd *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(nil)
				mockTaskManager.StatTask(gomock.Any(), gomock.Any()).Return(&schedulerv1.Task{}, storage.ErrTaskNotFound)
			},
			expect: func(t *testing.T, r *dfdaemonv1.ExportTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Error(err)
			},
		},
		{
			name: "task found in peers but not available for download",
			r: &dfdaemonv1.ExportTaskRequest{
				UrlMeta:   &commonv1.UrlMeta{},
				LocalOnly: false,
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mocktsd *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(nil)
				mockTaskManager.StatTask(gomock.Any(), gomock.Any()).Return(&schedulerv1.Task{
					State:            resource.TaskStateSucceeded,
					HasAvailablePeer: false,
				}, nil)
			},
			expect: func(t *testing.T, r *dfdaemonv1.ExportTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.True(dferrors.CheckError(err, commonv1.Code_PeerTaskNotFound))
			},
		},
		{
			name: "Task exists in peers",
			r: &dfdaemonv1.ExportTaskRequest{
				Url:    "http://localhost/test",
				Output: "./testdata/file1",
				UrlMeta: &commonv1.UrlMeta{
					Tag: "unit test",
				},
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mocktsd *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(nil)
				mockTaskManager.StatTask(gomock.Any(), gomock.Any()).Return(&schedulerv1.Task{
					State:            resource.TaskStateSucceeded,
					HasAvailablePeer: true,
				}, nil)
				mockTaskManager.StartFileTask(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, req *peer.FileTaskRequest) (chan *peer.FileTaskProgress, bool, error) {
						ch := make(chan *peer.FileTaskProgress)
						go func() {
							for i := 0; i <= 100; i++ {
								ch <- &peer.FileTaskProgress{
									State: &peer.ProgressState{
										Success: true,
									},
									TaskID:          "",
									PeerID:          "",
									ContentLength:   100,
									CompletedLength: int64(i),
									PeerTaskDone:    i == 100,
									DoneCallback:    func() {},
								}
							}
							close(ch)
						}()
						return ch, false, nil
					})
			},
			expect: func(t *testing.T, r *dfdaemonv1.ExportTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Nil(err)
			},
		},
		{
			name: "task exists with error",
			r: &dfdaemonv1.ExportTaskRequest{
				UrlMeta:   &commonv1.UrlMeta{},
				LocalOnly: false,
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mocktsd *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(&storage.ReusePeerTask{})
				mockStorageManger.Store(gomock.Any(), gomock.Any()).Return(storage.ErrTaskNotFound)
			},
			expect: func(t *testing.T, r *dfdaemonv1.ExportTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Error(err)
			},
		},
		{
			name: "task exists with no error",
			r: &dfdaemonv1.ExportTaskRequest{
				UrlMeta:   &commonv1.UrlMeta{},
				LocalOnly: false,
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mocktsd *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(&storage.ReusePeerTask{})
				mockStorageManger.Store(gomock.Any(), gomock.Any()).Return(nil)
			},
			expect: func(t *testing.T, r *dfdaemonv1.ExportTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Nil(err)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStorageManger := mocks.NewMockManager(ctrl)
			mockTaskManager := peer.NewMockTaskManager(ctrl)
			mocktsd := mocks.NewMockTaskStorageDriver(ctrl)
			pieceManager := peer.NewMockPieceManager(ctrl)
			tc.mock(mockStorageManger.EXPECT(), mockTaskManager.EXPECT(), mocktsd, pieceManager)
			s := &server{
				KeepAlive:       util.NewKeepAlive("test"),
				peerHost:        &schedulerv1.PeerHost{},
				storageManager:  mockStorageManger,
				peerTaskManager: mockTaskManager,
			}
			_, err := s.ExportTask(context.Background(), tc.r)
			tc.expect(t, tc.r, err)
		})
	}
}

func TestServer_ImportTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name   string
		r      *dfdaemonv1.ImportTaskRequest
		mock   func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager)
		expect func(t *testing.T, r *dfdaemonv1.ImportTaskRequest, err error)
	}{
		{
			name: "0. Task exists in local storage",
			r: &dfdaemonv1.ImportTaskRequest{
				UrlMeta: &commonv1.UrlMeta{},
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mocktsd *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(&storage.ReusePeerTask{})
				mockTaskManager.AnnouncePeerTask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			expect: func(t *testing.T, r *dfdaemonv1.ImportTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Nil(err)
			},
		},
		{
			name: "0. Task exists in local storage with err",
			r: &dfdaemonv1.ImportTaskRequest{
				UrlMeta: &commonv1.UrlMeta{},
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mocktsd *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(&storage.ReusePeerTask{})
				mockTaskManager.AnnouncePeerTask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(storage.ErrTaskNotFound).AnyTimes()
			},
			expect: func(t *testing.T, r *dfdaemonv1.ImportTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Nil(err)
			},
		},
		{
			name: "1. Register to storageManager with err",
			r: &dfdaemonv1.ImportTaskRequest{
				UrlMeta: &commonv1.UrlMeta{},
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mocktsd *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(nil)
				mockTaskManager.AnnouncePeerTask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(storage.ErrTaskNotFound).AnyTimes()
				mockStorageManger.RegisterTask(gomock.Any(), gomock.Any()).Return(mocktsd, dferrors.ErrInvalidArgument)
			},
			expect: func(t *testing.T, r *dfdaemonv1.ImportTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Error(err)
			},
		},
		{
			name: "2. Import task file with err",
			r: &dfdaemonv1.ImportTaskRequest{
				UrlMeta: &commonv1.UrlMeta{},
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mocktsd *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(nil)
				mockTaskManager.AnnouncePeerTask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(storage.ErrTaskNotFound).AnyTimes()
				mockStorageManger.RegisterTask(gomock.Any(), gomock.Any()).Return(mocktsd, nil)
				mockTaskManager.GetPieceManager().Return(mockPieceManager)
				mockPieceManager.EXPECT().ImportFile(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dferrors.ErrInvalidArgument)
			},
			expect: func(t *testing.T, r *dfdaemonv1.ImportTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Error(err)
			},
		},
		{
			name: "import file succeeded",
			r: &dfdaemonv1.ImportTaskRequest{
				UrlMeta: &commonv1.UrlMeta{},
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mocktsd *mocks.MockTaskStorageDriver, mockPieceManager *peer.MockPieceManager) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(nil)
				mockTaskManager.AnnouncePeerTask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(storage.ErrTaskNotFound).AnyTimes()
				mockStorageManger.RegisterTask(gomock.Any(), gomock.Any()).Return(mocktsd, nil)
				mockTaskManager.GetPieceManager().Return(mockPieceManager)
				mockPieceManager.EXPECT().ImportFile(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				mockTaskManager.AnnouncePeerTask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(storage.ErrTaskNotFound).AnyTimes()
			},
			expect: func(t *testing.T, r *dfdaemonv1.ImportTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Nil(err)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStorageManger := mocks.NewMockManager(ctrl)
			mockTaskManager := peer.NewMockTaskManager(ctrl)
			mocktsd := mocks.NewMockTaskStorageDriver(ctrl)
			pieceManager := peer.NewMockPieceManager(ctrl)
			tc.mock(mockStorageManger.EXPECT(), mockTaskManager.EXPECT(), mocktsd, pieceManager)
			s := &server{
				KeepAlive:       util.NewKeepAlive("test"),
				peerHost:        &schedulerv1.PeerHost{},
				storageManager:  mockStorageManger,
				peerTaskManager: mockTaskManager,
			}
			_, err := s.ImportTask(context.Background(), tc.r)
			tc.expect(t, tc.r, err)
		})
	}
}

func TestServer_StatTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name   string
		r      *dfdaemonv1.StatTaskRequest
		mock   func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *peer.MockTask)
		expect func(t *testing.T, r *dfdaemonv1.StatTaskRequest, err error)
	}{
		{
			name: "completed",
			r: &dfdaemonv1.StatTaskRequest{
				UrlMeta: &commonv1.UrlMeta{},
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *peer.MockTask) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(&storage.ReusePeerTask{})
			},
			expect: func(t *testing.T, r *dfdaemonv1.StatTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Nil(err)
			},
		},
		{
			name: "stat local cache and task doesn't exist",
			r: &dfdaemonv1.StatTaskRequest{
				UrlMeta:   &commonv1.UrlMeta{},
				LocalOnly: true,
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *peer.MockTask) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(nil)
			},
			expect: func(t *testing.T, r *dfdaemonv1.StatTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.True(dferrors.CheckError(err, commonv1.Code_PeerTaskNotFound))
			},
		},
		{
			name: "other peers hold the task",
			r: &dfdaemonv1.StatTaskRequest{
				UrlMeta:   &commonv1.UrlMeta{},
				LocalOnly: false,
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *peer.MockTask) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(nil)
				mockTaskManager.StatTask(gomock.Any(), gomock.Any()).Return(&schedulerv1.Task{}, storage.ErrTaskNotFound)
			},
			expect: func(t *testing.T, r *dfdaemonv1.StatTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Error(err)
			},
		},
		{
			name: "task is in succeeded state and has available peer",
			r: &dfdaemonv1.StatTaskRequest{
				UrlMeta:   &commonv1.UrlMeta{},
				LocalOnly: false,
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *peer.MockTask) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(nil)
				mockTaskManager.StatTask(gomock.Any(), gomock.Any()).Return(&schedulerv1.Task{
					State:            resource.TaskStateSucceeded,
					HasAvailablePeer: true,
				}, nil)
			},
			expect: func(t *testing.T, r *dfdaemonv1.StatTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Nil(err)
			},
		},
		{
			name: "task is in succeeded state and has available peer",
			r: &dfdaemonv1.StatTaskRequest{
				UrlMeta:   &commonv1.UrlMeta{},
				LocalOnly: false,
			},
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *peer.MockTask) {
				mockStorageManger.FindCompletedTask(gomock.Any()).Return(nil)
				mockTaskManager.StatTask(gomock.Any(), gomock.Any()).Return(&schedulerv1.Task{
					State:            resource.TaskStateSucceeded,
					HasAvailablePeer: false,
				}, nil)
			},
			expect: func(t *testing.T, r *dfdaemonv1.StatTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.True(dferrors.CheckError(err, commonv1.Code_PeerTaskNotFound))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStorageManger := mocks.NewMockManager(ctrl)
			mockTaskManager := peer.NewMockTaskManager(ctrl)
			mockTask := peer.NewMockTask(ctrl)
			tc.mock(mockStorageManger.EXPECT(), mockTaskManager.EXPECT(), mockTask)
			s := &server{
				KeepAlive:       util.NewKeepAlive("test"),
				peerHost:        &schedulerv1.PeerHost{},
				storageManager:  mockStorageManger,
				peerTaskManager: mockTaskManager,
			}
			_, err := s.StatTask(context.Background(), tc.r)
			tc.expect(t, tc.r, err)
		})
	}
}

func TestServer_GetPieceTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name   string
		mock   func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *peer.MockTask, r *commonv1.PieceTaskRequest)
		expect func(t *testing.T, p *commonv1.PiecePacket, r *commonv1.PieceTaskRequest, err error)
	}{
		{
			name: "error invalid argument",
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *peer.MockTask, r *commonv1.PieceTaskRequest) {
				mockStorageManger.GetPieces(gomock.Any(), gomock.Any()).Return(&commonv1.PiecePacket{}, dferrors.ErrInvalidArgument)
			},
			expect: func(t *testing.T, p *commonv1.PiecePacket, r *commonv1.PieceTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.True(dferrors.CheckError(err, commonv1.Code_BadRequest))
			},
		},
		{
			name: "error task not found and no running task",
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *peer.MockTask, r *commonv1.PieceTaskRequest) {
				gomock.InOrder(
					mockStorageManger.GetPieces(gomock.Any(), gomock.Any()).Return(&commonv1.PiecePacket{}, storage.ErrTaskNotFound),
					mockTaskManager.IsPeerTaskRunning(gomock.Any(), gomock.Any()).Return(mockTask, false),
				)
			},
			expect: func(t *testing.T, p *commonv1.PiecePacket, r *commonv1.PieceTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.True(dferrors.CheckError(err, commonv1.Code_PeerTaskNotFound))
			},
		},
		{
			name: "error task not found and the running peer is the source peer",
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *peer.MockTask, r *commonv1.PieceTaskRequest) {
				gomock.InOrder(
					mockStorageManger.GetPieces(gomock.Any(), gomock.Any()).Return(&commonv1.PiecePacket{}, storage.ErrTaskNotFound),
					mockTaskManager.IsPeerTaskRunning(gomock.Any(), gomock.Any()).Return(mockTask, true),
					mockTask.EXPECT().GetPeerID().Return(r.SrcPid),
				)
			},
			expect: func(t *testing.T, p *commonv1.PiecePacket, r *commonv1.PieceTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.True(dferrors.CheckError(err, commonv1.Code_PeerTaskNotFound))
			},
		},
		{
			name: "error task not found and task.GetPeerID() != request.GetDstPid() with no err",
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *peer.MockTask, r *commonv1.PieceTaskRequest) {
				var testDstPid string = "test"
				gomock.InOrder(
					mockStorageManger.GetPieces(gomock.Any(), gomock.Any()).Return(&commonv1.PiecePacket{}, storage.ErrTaskNotFound),
					mockTaskManager.IsPeerTaskRunning(gomock.Any(), gomock.Any()).Return(mockTask, true),
					mockTask.EXPECT().GetPeerID().Return(testDstPid).AnyTimes(),
					mockStorageManger.GetPieces(gomock.Any(), &commonv1.PieceTaskRequest{
						SrcPid: r.SrcPid,
						DstPid: testDstPid,
					}).Return(&commonv1.PiecePacket{
						DstPid: testDstPid,
					}, nil),
				)
			},
			expect: func(t *testing.T, p *commonv1.PiecePacket, r *commonv1.PieceTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Nil(err)
				assert.Equal(p.DstAddr, "")
				assert.Equal(p.DstPid, "test")
			},
		},
		{
			name: "error task not found and task.GetPeerID() != request.GetDstPid() with err",
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *peer.MockTask, r *commonv1.PieceTaskRequest) {
				var testDstPid string = "test"
				gomock.InOrder(
					mockStorageManger.GetPieces(gomock.Any(), gomock.Any()).Return(&commonv1.PiecePacket{}, storage.ErrTaskNotFound),
					mockTaskManager.IsPeerTaskRunning(gomock.Any(), gomock.Any()).Return(mockTask, true),
					mockTask.EXPECT().GetPeerID().Return(testDstPid).AnyTimes(),
					mockStorageManger.GetPieces(gomock.Any(), gomock.Any()).Return(&commonv1.PiecePacket{}, storage.ErrTaskNotFound),
				)
			},
			expect: func(t *testing.T, p *commonv1.PiecePacket, r *commonv1.PieceTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.True(dferrors.CheckError(err, commonv1.Code_PeerTaskNotFound))
			},
		},
		{
			name: "error task not found and task.GetPeerID() == request.GetDstPid()",
			mock: func(mockStorageManger *mocks.MockManagerMockRecorder, mockTaskManager *peer.MockTaskManagerMockRecorder, mockTask *peer.MockTask, r *commonv1.PieceTaskRequest) {
				gomock.InOrder(
					mockStorageManger.GetPieces(gomock.Any(), gomock.Any()).Return(&commonv1.PiecePacket{}, storage.ErrTaskNotFound),
					mockTaskManager.IsPeerTaskRunning(gomock.Any(), gomock.Any()).Return(mockTask, true),
					mockTask.EXPECT().GetPeerID().Return(r.DstPid).Times(2),
				)
			},
			expect: func(t *testing.T, p *commonv1.PiecePacket, r *commonv1.PieceTaskRequest, err error) {
				assert := testifyassert.New(t)
				assert.Nil(err)
				assert.Equal(p, &commonv1.PiecePacket{
					TaskId:        r.TaskId,
					DstPid:        r.DstPid,
					DstAddr:       "",
					PieceInfos:    nil,
					TotalPiece:    -1,
					ContentLength: -1,
					PieceMd5Sign:  "",
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStorageManger := mocks.NewMockManager(ctrl)
			mockTaskManager := peer.NewMockTaskManager(ctrl)
			mockTask := peer.NewMockTask(ctrl)
			r := commonv1.PieceTaskRequest{
				SrcPid: idgen.PeerIDV1(ip.IPv4.String()),
				DstPid: idgen.PeerIDV1(ip.IPv4.String()),
			}
			tc.mock(mockStorageManger.EXPECT(), mockTaskManager.EXPECT(), mockTask, &r)
			s := &server{
				KeepAlive:       util.NewKeepAlive("test"),
				peerHost:        &schedulerv1.PeerHost{},
				storageManager:  mockStorageManger,
				peerTaskManager: mockTaskManager,
			}
			p, err := s.GetPieceTasks(context.Background(), &r)
			tc.expect(t, p, &r, err)
		})
	}
}

func TestServer_ServeDownload(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPeerTaskManager := peer.NewMockTaskManager(ctrl)
	mockPeerTaskManager.EXPECT().StartFileTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *peer.FileTaskRequest) (chan *peer.FileTaskProgress, bool, error) {
			ch := make(chan *peer.FileTaskProgress)
			go func() {
				for i := 0; i <= 100; i++ {
					ch <- &peer.FileTaskProgress{
						State: &peer.ProgressState{
							Success: true,
						},
						TaskID:          "",
						PeerID:          "",
						ContentLength:   100,
						CompletedLength: int64(i),
						PeerTaskDone:    i == 100,
						DoneCallback:    func() {},
					}
				}
				close(ch)
			}()
			return ch, false, nil
		})
	s := &server{
		KeepAlive:       util.NewKeepAlive("test"),
		peerHost:        &schedulerv1.PeerHost{},
		peerTaskManager: mockPeerTaskManager,
	}

	socketDir, err := os.MkdirTemp(os.TempDir(), "d7y-test-***")
	assert.Nil(err, "make temp dir should be ok")
	socketPath := path.Join(socketDir, "rpc.sock")
	defer os.RemoveAll(socketDir)

	client := setupPeerServerAndClient(t, socketPath, s, assert, s.ServeDownload)
	request := &dfdaemonv1.DownRequest{
		Uuid:              uuid.Generate().String(),
		Url:               "http://localhost/test",
		Output:            "./testdata/file1",
		DisableBackSource: false,
		UrlMeta: &commonv1.UrlMeta{
			Tag: "unit test",
		},
	}
	down, err := client.Download(context.Background(), request)
	assert.Nil(err, "client download grpc call should be ok")

	var (
		lastResult *dfdaemonv1.DownResult
		curResult  *dfdaemonv1.DownResult
	)
	for {
		curResult, err = down.Recv()
		if err == io.EOF {
			break
		}
		assert.Nil(err)
		lastResult = curResult
	}
	assert.NotNil(lastResult)
	assert.True(lastResult.Done)
}

func TestServer_ServePeer(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var maxPieceNum uint32 = 10
	mockStorageManger := mocks.NewMockManager(ctrl)
	mockStorageManger.EXPECT().GetPieces(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, req *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
		var (
			pieces    []*commonv1.PieceInfo
			pieceSize = uint32(1024)
		)
		for i := req.StartNum; i < req.Limit+req.StartNum && i < maxPieceNum; i++ {
			pieces = append(pieces, &commonv1.PieceInfo{
				PieceNum:    int32(i),
				RangeStart:  uint64(i * pieceSize),
				RangeSize:   pieceSize,
				PieceMd5:    "",
				PieceOffset: uint64(i * pieceSize),
				PieceStyle:  commonv1.PieceStyle_PLAIN,
			})
		}
		return &commonv1.PiecePacket{
			TaskId:        "",
			DstPid:        "",
			DstAddr:       "",
			PieceInfos:    pieces,
			TotalPiece:    10,
			ContentLength: 10 * int64(pieceSize),
			PieceMd5Sign:  "",
		}, nil
	})
	s := &server{
		KeepAlive:      util.NewKeepAlive("test"),
		peerHost:       &schedulerv1.PeerHost{},
		storageManager: mockStorageManger,
	}

	socketDir, err := os.MkdirTemp(os.TempDir(), "d7y-test-***")
	assert.Nil(err, "make temp dir should be ok")
	socketPath := path.Join(socketDir, "rpc.sock")
	defer os.RemoveAll(socketDir)

	client := setupPeerServerAndClient(t, socketPath, s, assert, s.ServePeer)
	defer s.peerServer.GracefulStop()

	var tests = []struct {
		request           *commonv1.PieceTaskRequest
		responsePieceSize int
	}{
		{
			request: &commonv1.PieceTaskRequest{
				TaskId:   idgen.TaskIDV1("http://www.test.com", &commonv1.UrlMeta{}),
				SrcPid:   idgen.PeerIDV1(ip.IPv4.String()),
				DstPid:   idgen.PeerIDV1(ip.IPv4.String()),
				StartNum: 0,
				Limit:    1,
			},
			// 0
			responsePieceSize: 1,
		},
		{
			request: &commonv1.PieceTaskRequest{
				TaskId:   idgen.TaskIDV1("http://www.test.com", &commonv1.UrlMeta{}),
				SrcPid:   idgen.PeerIDV1(ip.IPv4.String()),
				DstPid:   idgen.PeerIDV1(ip.IPv4.String()),
				StartNum: 0,
				Limit:    4,
			},
			// 0 1 2 3
			responsePieceSize: 4,
		},
		{
			request: &commonv1.PieceTaskRequest{
				TaskId:   idgen.TaskIDV1("http://www.test.com", &commonv1.UrlMeta{}),
				SrcPid:   idgen.PeerIDV1(ip.IPv4.String()),
				DstPid:   idgen.PeerIDV1(ip.IPv4.String()),
				StartNum: 8,
				Limit:    1,
			},
			// 8
			responsePieceSize: 1,
		},
		{
			request: &commonv1.PieceTaskRequest{
				TaskId:   idgen.TaskIDV1("http://www.test.com", &commonv1.UrlMeta{}),
				SrcPid:   idgen.PeerIDV1(ip.IPv4.String()),
				DstPid:   idgen.PeerIDV1(ip.IPv4.String()),
				StartNum: 8,
				Limit:    4,
			},
			// 8 9
			responsePieceSize: 2,
		},
	}

	for _, tc := range tests {
		response, err := client.GetPieceTasks(context.Background(), tc.request)
		assert.Nil(err, "client get piece tasks grpc call should be ok")
		assert.Equal(tc.responsePieceSize, len(response.PieceInfos))
	}
}

func TestServer_SyncPieceTasks(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		pieceSize = uint32(1024)
	)

	type pieceRange struct {
		start int
		end   int
	}
	var tests = []struct {
		name            string
		existTaskID     string       // test for non-exists task
		existPieces     []pieceRange // already exist pieces in storage
		followingPieces []pieceRange // following pieces in running task subscribe channel
		requestPieces   []int32
		limit           uint32
		totalPieces     uint32
		success         bool
		verify          func(t *testing.T, assert *testifyassert.Assertions)
	}{
		{
			name: "already exists in storage",
			existPieces: []pieceRange{
				{
					start: 0,
					end:   10,
				},
			},
			totalPieces: 11,
			success:     true,
			verify: func(t *testing.T, assert *testifyassert.Assertions) {
			},
		},
		{
			name: "already exists in storage with extra get piece request",
			existPieces: []pieceRange{
				{
					start: 0,
					end:   10,
				},
			},
			totalPieces:   11,
			requestPieces: []int32{1, 3, 5, 7},
			success:       true,
			verify: func(t *testing.T, assert *testifyassert.Assertions) {
			},
		},
		{
			name: "already exists in storage - large",
			existPieces: []pieceRange{
				{
					start: 0,
					end:   1000,
				},
			},
			totalPieces: 1001,
			success:     true,
			verify: func(t *testing.T, assert *testifyassert.Assertions) {
			},
		},
		{
			name: "already exists in storage - large with extra get piece request",
			existPieces: []pieceRange{
				{
					start: 0,
					end:   1000,
				},
			},
			totalPieces:   1001,
			requestPieces: []int32{1, 3, 5, 7, 100, 500, 1000},
			success:       true,
			verify: func(t *testing.T, assert *testifyassert.Assertions) {
			},
		},
		{
			name: "partial exists in storage",
			existPieces: []pieceRange{
				{
					start: 0,
					end:   10,
				},
			},
			followingPieces: []pieceRange{
				{
					start: 11,
					end:   20,
				},
			},
			totalPieces: 21,
			success:     true,
			verify: func(t *testing.T, assert *testifyassert.Assertions) {
			},
		},
		{
			name: "partial exists in storage - large",
			existPieces: []pieceRange{
				{
					start: 0,
					end:   1000,
				},
			},
			followingPieces: []pieceRange{
				{
					start: 1001,
					end:   2000,
				},
			},
			totalPieces:   2001,
			requestPieces: []int32{1000, 1010, 1020, 1040},
			success:       true,
			verify: func(t *testing.T, assert *testifyassert.Assertions) {
			},
		},
		{
			name: "not exists in storage",
			followingPieces: []pieceRange{
				{
					start: 0,
					end:   20,
				},
			},
			totalPieces: 21,
			success:     true,
			verify: func(t *testing.T, assert *testifyassert.Assertions) {
			},
		},
		{
			name: "not exists in storage - large",
			followingPieces: []pieceRange{
				{
					start: 0,
					end:   2000,
				},
			},
			totalPieces:   2001,
			requestPieces: []int32{1000, 1010, 1020, 1040},
			success:       true,
			verify: func(t *testing.T, assert *testifyassert.Assertions) {
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, delay := range []bool{false, true} {
				delay := delay
				mockStorageManger := mocks.NewMockManager(ctrl)

				if tc.limit == 0 {
					tc.limit = 1024
				}

				var (
					totalPieces []*commonv1.PieceInfo
					lock        sync.Mutex
				)

				var addedPieces = make(map[uint32]*commonv1.PieceInfo)
				for _, p := range tc.existPieces {
					if p.end == 0 {
						p.end = p.start
					}
					for i := p.start; i <= p.end; i++ {
						if _, ok := addedPieces[uint32(i)]; ok {
							continue
						}
						piece := &commonv1.PieceInfo{
							PieceNum:    int32(i),
							RangeStart:  uint64(i) * uint64(pieceSize),
							RangeSize:   pieceSize,
							PieceOffset: uint64(i) * uint64(pieceSize),
							PieceStyle:  commonv1.PieceStyle_PLAIN,
						}
						totalPieces = append(totalPieces, piece)
						addedPieces[uint32(i)] = piece
					}
				}

				mockStorageManger.EXPECT().GetPieces(gomock.Any(),
					gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, req *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
						var pieces []*commonv1.PieceInfo
						lock.Lock()
						for i := req.StartNum; i < tc.totalPieces; i++ {
							if piece, ok := addedPieces[i]; ok {
								if piece.PieceNum >= int32(req.StartNum) && len(pieces) < int(req.Limit) {
									pieces = append(pieces, piece)
								}
							}
						}
						lock.Unlock()
						return &commonv1.PiecePacket{
							TaskId:        req.TaskId,
							DstPid:        req.DstPid,
							DstAddr:       "",
							PieceInfos:    pieces,
							TotalPiece:    int32(tc.totalPieces),
							ContentLength: int64(tc.totalPieces) * int64(pieceSize),
							PieceMd5Sign:  "",
						}, nil
					})
				mockStorageManger.EXPECT().GetExtendAttribute(gomock.Any(),
					gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, req *storage.PeerTaskMetadata) (*commonv1.ExtendAttribute, error) {
						return &commonv1.ExtendAttribute{
							Header: map[string]string{
								"Test": "test",
							},
						}, nil
					})
				mockTaskManager := peer.NewMockTaskManager(ctrl)
				mockTaskManager.EXPECT().Subscribe(gomock.Any()).AnyTimes().DoAndReturn(
					func(request *commonv1.PieceTaskRequest) (*peer.SubscribeResponse, bool) {
						ch := make(chan *peer.PieceInfo)
						success := make(chan struct{})
						fail := make(chan struct{})

						go func(followingPieces []pieceRange) {
							for i, p := range followingPieces {
								if p.end == 0 {
									p.end = p.start
								}
								for j := p.start; j <= p.end; j++ {
									lock.Lock()
									if _, ok := addedPieces[uint32(j)]; ok {
										continue
									}
									piece := &commonv1.PieceInfo{
										PieceNum:    int32(j),
										RangeStart:  uint64(j) * uint64(pieceSize),
										RangeSize:   pieceSize,
										PieceOffset: uint64(j) * uint64(pieceSize),
										PieceStyle:  commonv1.PieceStyle_PLAIN,
									}
									totalPieces = append(totalPieces, piece)
									addedPieces[uint32(j)] = piece
									lock.Unlock()

									var finished bool
									if i == len(followingPieces)-1 && j == p.end {
										finished = true
									}
									if !delay {
										ch <- &peer.PieceInfo{
											Num:      int32(j),
											Finished: finished,
										}
									}
								}
							}
							close(success)
						}(tc.followingPieces)

						return &peer.SubscribeResponse{
							Storage:          mockStorageManger,
							PieceInfoChannel: ch,
							Success:          success,
							Fail:             fail,
						}, true
					})

				s := &server{
					KeepAlive:       util.NewKeepAlive("test"),
					peerHost:        &schedulerv1.PeerHost{},
					storageManager:  mockStorageManger,
					peerTaskManager: mockTaskManager,
				}

				socketDir, err := os.MkdirTemp(os.TempDir(), "d7y-test-***")
				assert.Nil(err, "make temp dir should be ok")
				socketPath := path.Join(socketDir, "rpc.sock")
				defer os.RemoveAll(socketDir)

				client := setupPeerServerAndClient(t, socketPath, s, assert, s.ServePeer)
				syncClient, err := client.SyncPieceTasks(
					context.Background(),
					&commonv1.PieceTaskRequest{
						TaskId:   tc.name,
						SrcPid:   idgen.PeerIDV1(ip.IPv4.String()),
						DstPid:   idgen.PeerIDV1(ip.IPv4.String()),
						StartNum: 0,
						Limit:    tc.limit,
					})
				assert.Nil(err, "client sync piece tasks grpc call should be ok")

				var (
					total       = make(map[int32]bool)
					maxNum      int32
					requestSent = make(chan bool)
				)
				if len(tc.requestPieces) == 0 {
					close(requestSent)
				} else {
					go func() {
						for _, n := range tc.requestPieces {
							request := &commonv1.PieceTaskRequest{
								TaskId:   tc.name,
								SrcPid:   idgen.PeerIDV1(ip.IPv4.String()),
								DstPid:   idgen.PeerIDV1(ip.IPv4.String()),
								StartNum: uint32(n),
								Limit:    tc.limit,
							}
							assert.Nil(syncClient.Send(request))
						}
						close(requestSent)
					}()
				}
				for {
					p, err := syncClient.Recv()
					if err == io.EOF {
						break
					}
					for _, info := range p.PieceInfos {
						total[info.PieceNum] = true
						if info.PieceNum >= maxNum {
							maxNum = info.PieceNum
						}
					}
					if tc.success {
						assert.Nil(err, "receive piece tasks should be ok")
					}
					if int(p.TotalPiece) == len(total) {
						<-requestSent
						err = syncClient.CloseSend()
						assert.Nil(err)
					}
				}
				if tc.success {
					assert.Equal(int(maxNum+1), len(total))
				}
				s.peerServer.GracefulStop()
			}

		})
	}
}

func setupPeerServerAndClient(t *testing.T, socket string, srv *server, assert *testifyassert.Assertions, serveFunc func(listener net.Listener) error) dfdaemonclient.V1 {
	if srv.healthServer == nil {
		srv.healthServer = health.NewServer()
	}
	srv.downloadServer = dfdaemonserver.New(srv, srv.healthServer)
	srv.peerServer = dfdaemonserver.New(srv, srv.healthServer)

	ln, err := net.Listen("unix", socket)
	assert.Nil(err, "listen unix socket should be ok")
	go func() {
		if err := serveFunc(ln); err != nil {
			t.Error(err)
		}
	}()

	netAddr := &dfnet.NetAddr{
		Type: dfnet.UNIX,
		Addr: socket,
	}
	client, err := dfdaemonclient.GetInsecureV1(context.Background(), netAddr.String())
	assert.Nil(err, "grpc dial should be ok")
	return client
}
