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
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	rpcschedulermocks "d7y.io/dragonfly/v2/pkg/rpc/scheduler/mocks"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
	schedulermocks "d7y.io/dragonfly/v2/scheduler/scheduler/mocks"
	"d7y.io/dragonfly/v2/scheduler/service/mocks"
)

var (
	mockRawHost = &rpcscheduler.PeerHost{
		Uuid:           idgen.HostID("hostname", 8003),
		Ip:             "127.0.0.1",
		RpcPort:        8003,
		DownPort:       8001,
		HostName:       "hostname",
		SecurityDomain: "security_domain",
		Location:       "location",
		Idc:            "idc",
		NetTopology:    "net_topology",
	}
	mockTaskURLMeta = &base.UrlMeta{
		Digest: "digest",
		Tag:    "tag",
		Range:  "range",
		Filter: "filter",
		Header: map[string]string{
			"content-length": "100",
		},
	}
	mockTaskURL               = "http://example.com/foo"
	mockTaskBackToSourceLimit = 200
	mockTaskID                = idgen.TaskID(mockTaskURL, mockTaskURLMeta)
	mockPeerID                = idgen.PeerID("127.0.0.1")
)

func TestRPCServer_New(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s interface{})
	}{
		{
			name: "new server",
			expect: func(t *testing.T, s interface{}) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "Server")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			svc := mocks.NewMockService(ctl)
			svr := New(svc)
			tc.expect(t, svr)
		})
	}
}

func TestRPCServer_RegisterPeerTask(t *testing.T) {
	tests := []struct {
		name   string
		req    *rpcscheduler.PeerTaskRequest
		mock   func(req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockHost *resource.Host, mockTask *resource.Task, scheduler scheduler.Scheduler, ms *mocks.MockServiceMockRecorder, msched *schedulermocks.MockSchedulerMockRecorder)
		expect func(t *testing.T, result *rpcscheduler.RegisterResult, err error)
	}{
		{
			name: "service register failed",
			req:  &rpcscheduler.PeerTaskRequest{},
			mock: func(req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockHost *resource.Host, mockTask *resource.Task, scheduler scheduler.Scheduler, ms *mocks.MockServiceMockRecorder, msched *schedulermocks.MockSchedulerMockRecorder) {
				ms.RegisterTask(context.Background(), req).Return(nil, errors.New("foo"))
			},
			expect: func(t *testing.T, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedTaskStatusError)
			},
		},
		{
			name: "task state is TaskStatePending",
			req:  &rpcscheduler.PeerTaskRequest{},
			mock: func(req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockHost *resource.Host, mockTask *resource.Task, scheduler scheduler.Scheduler, ms *mocks.MockServiceMockRecorder, msched *schedulermocks.MockSchedulerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)

				gomock.InOrder(
					ms.RegisterTask(context.Background(), req).Return(mockTask, nil).Times(1),
					ms.LoadOrStoreHost(context.Background(), req).Return(mockHost, true).Times(1),
					ms.LoadOrStorePeer(context.Background(), req, gomock.Any(), gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.Equal(result.TaskId, mockTaskID)
				assert.Equal(result.SizeScope, base.SizeScope_NORMAL)
			},
		},
		{
			name: "task state is TaskStatePending and peer state is PeerStateFailed",
			req:  &rpcscheduler.PeerTaskRequest{},
			mock: func(req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockHost *resource.Host, mockTask *resource.Task, scheduler scheduler.Scheduler, ms *mocks.MockServiceMockRecorder, msched *schedulermocks.MockSchedulerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.FSM.SetState(resource.PeerStateFailed)

				gomock.InOrder(
					ms.RegisterTask(context.Background(), req).Return(mockTask, nil).Times(1),
					ms.LoadOrStoreHost(context.Background(), req).Return(mockHost, true).Times(1),
					ms.LoadOrStorePeer(context.Background(), req, gomock.Any(), gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedError)
			},
		},
		{
			name: "task state is TaskStateRunning",
			req:  &rpcscheduler.PeerTaskRequest{},
			mock: func(req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockHost *resource.Host, mockTask *resource.Task, scheduler scheduler.Scheduler, ms *mocks.MockServiceMockRecorder, msched *schedulermocks.MockSchedulerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateRunning)

				gomock.InOrder(
					ms.RegisterTask(context.Background(), req).Return(mockTask, nil).Times(1),
					ms.LoadOrStoreHost(context.Background(), req).Return(mockHost, true).Times(1),
					ms.LoadOrStorePeer(context.Background(), req, gomock.Any(), gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.Equal(result.TaskId, mockTaskID)
				assert.Equal(result.SizeScope, base.SizeScope_NORMAL)
			},
		},
		{
			name: "task state is TaskStateRunning and peer state is PeerStateFailed",
			req:  &rpcscheduler.PeerTaskRequest{},
			mock: func(req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockHost *resource.Host, mockTask *resource.Task, scheduler scheduler.Scheduler, ms *mocks.MockServiceMockRecorder, msched *schedulermocks.MockSchedulerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateFailed)

				gomock.InOrder(
					ms.RegisterTask(context.Background(), req).Return(mockTask, nil).Times(1),
					ms.LoadOrStoreHost(context.Background(), req).Return(mockHost, true).Times(1),
					ms.LoadOrStorePeer(context.Background(), req, gomock.Any(), gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedError)
			},
		},
		{
			name: "task state is TaskStateSucceeded and sizeScope is SizeScope_TINY",
			req:  &rpcscheduler.PeerTaskRequest{},
			mock: func(req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockHost *resource.Host, mockTask *resource.Task, scheduler scheduler.Scheduler, ms *mocks.MockServiceMockRecorder, msched *schedulermocks.MockSchedulerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateSucceeded)
				mockTask.ContentLength.Store(1)
				mockTask.DirectPiece = []byte{1}
				gomock.InOrder(
					ms.RegisterTask(context.Background(), req).Return(mockTask, nil).Times(1),
					ms.LoadOrStoreHost(context.Background(), req).Return(mockHost, true).Times(1),
					ms.LoadOrStorePeer(context.Background(), req, gomock.Any(), gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.Equal(result.TaskId, mockTaskID)
				assert.Equal(result.SizeScope, base.SizeScope_TINY)
				assert.Equal(result.DirectPiece, &rpcscheduler.RegisterResult_PieceContent{
					PieceContent: []byte{1},
				})
			},
		},
		{
			name: "task state is TaskStateSucceeded and sizeScope is SizeScope_TINY, but piece data error and find parent failed",
			req:  &rpcscheduler.PeerTaskRequest{},
			mock: func(req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockHost *resource.Host, mockTask *resource.Task, scheduler scheduler.Scheduler, ms *mocks.MockServiceMockRecorder, msched *schedulermocks.MockSchedulerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateSucceeded)
				mockTask.ContentLength.Store(2)
				gomock.InOrder(
					ms.RegisterTask(context.Background(), req).Return(mockTask, nil).Times(1),
					ms.LoadOrStoreHost(context.Background(), req).Return(mockHost, true).Times(1),
					ms.LoadOrStorePeer(context.Background(), req, gomock.Any(), gomock.Any()).Return(mockPeer, true).Times(1),
					ms.Scheduler().Return(scheduler).Times(1),
					msched.FindParent(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.Equal(result.TaskId, mockTaskID)
				assert.Equal(result.SizeScope, base.SizeScope_NORMAL)
			},
		},
		{
			name: "task state is TaskStateSucceeded and sizeScope is SizeScope_SMALL, but find parent failed",
			req:  &rpcscheduler.PeerTaskRequest{},
			mock: func(req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockHost *resource.Host, mockTask *resource.Task, scheduler scheduler.Scheduler, ms *mocks.MockServiceMockRecorder, msched *schedulermocks.MockSchedulerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateSucceeded)
				mockTask.ContentLength.Store(resource.TinyFileSize + 1)
				mockTask.TotalPieceCount.Store(1)
				gomock.InOrder(
					ms.RegisterTask(context.Background(), req).Return(mockTask, nil).Times(1),
					ms.LoadOrStoreHost(context.Background(), req).Return(mockHost, true).Times(1),
					ms.LoadOrStorePeer(context.Background(), req, gomock.Any(), gomock.Any()).Return(mockPeer, true).Times(1),
					ms.Scheduler().Return(scheduler).Times(1),
					msched.FindParent(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.Equal(result.TaskId, mockTaskID)
				assert.Equal(result.SizeScope, base.SizeScope_NORMAL)
			},
		},
		{
			name: "task state is TaskStateSucceeded and sizeScope is SizeScope_SMALL, but can not find piece info",
			req:  &rpcscheduler.PeerTaskRequest{},
			mock: func(req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockHost *resource.Host, mockTask *resource.Task, scheduler scheduler.Scheduler, ms *mocks.MockServiceMockRecorder, msched *schedulermocks.MockSchedulerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateSucceeded)
				mockTask.ContentLength.Store(resource.TinyFileSize + 1)
				mockTask.TotalPieceCount.Store(1)

				gomock.InOrder(
					ms.RegisterTask(context.Background(), req).Return(mockTask, nil).Times(1),
					ms.LoadOrStoreHost(context.Background(), req).Return(mockHost, true).Times(1),
					ms.LoadOrStorePeer(context.Background(), req, gomock.Any(), gomock.Any()).Return(mockPeer, true).Times(1),
					ms.Scheduler().Return(scheduler).Times(1),
					msched.FindParent(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.Equal(result.TaskId, mockTaskID)
				assert.Equal(result.SizeScope, base.SizeScope_NORMAL)
			},
		},
		{
			name: "task state is TaskStateSucceeded and sizeScope is SizeScope_SMALL",
			req:  &rpcscheduler.PeerTaskRequest{},
			mock: func(req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockHost *resource.Host, mockTask *resource.Task, scheduler scheduler.Scheduler, ms *mocks.MockServiceMockRecorder, msched *schedulermocks.MockSchedulerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateSucceeded)
				mockTask.ContentLength.Store(resource.TinyFileSize + 1)
				mockTask.TotalPieceCount.Store(1)
				mockTask.StorePiece(&base.PieceInfo{
					PieceNum: 0,
				})

				gomock.InOrder(
					ms.RegisterTask(context.Background(), req).Return(mockTask, nil).Times(1),
					ms.LoadOrStoreHost(context.Background(), req).Return(mockHost, true).Times(1),
					ms.LoadOrStorePeer(context.Background(), req, gomock.Any(), gomock.Any()).Return(mockPeer, true).Times(1),
					ms.Scheduler().Return(scheduler).Times(1),
					msched.FindParent(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.Equal(result.TaskId, mockTaskID)
				assert.Equal(result.SizeScope, base.SizeScope_SMALL)
				assert.EqualValues(result.DirectPiece, &rpcscheduler.RegisterResult_SinglePiece{
					SinglePiece: &rpcscheduler.SinglePiece{
						DstPid:  mockPeerID,
						DstAddr: fmt.Sprintf("%s:%d", mockRawHost.Ip, mockRawHost.DownPort),
						PieceInfo: &base.PieceInfo{
							PieceNum: 0,
						},
					},
				})
			},
		},
		{
			name: "task state is TaskStateSucceeded and sizeScope is SizeScope_NORMAL",
			req:  &rpcscheduler.PeerTaskRequest{},
			mock: func(req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockHost *resource.Host, mockTask *resource.Task, scheduler scheduler.Scheduler, ms *mocks.MockServiceMockRecorder, msched *schedulermocks.MockSchedulerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateSucceeded)
				mockTask.ContentLength.Store(resource.TinyFileSize + 1)
				mockTask.TotalPieceCount.Store(2)

				gomock.InOrder(
					ms.RegisterTask(context.Background(), req).Return(mockTask, nil).Times(1),
					ms.LoadOrStoreHost(context.Background(), req).Return(mockHost, true).Times(1),
					ms.LoadOrStorePeer(context.Background(), req, gomock.Any(), gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.Equal(result.TaskId, mockTaskID)
				assert.Equal(result.SizeScope, base.SizeScope_NORMAL)
			},
		},
		{
			name: "task state is TaskStateSucceeded and sizeScope is SizeScope_NORMAL, but peer state is PeerStateFailed",
			req:  &rpcscheduler.PeerTaskRequest{},
			mock: func(req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockHost *resource.Host, mockTask *resource.Task, scheduler scheduler.Scheduler, ms *mocks.MockServiceMockRecorder, msched *schedulermocks.MockSchedulerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateSucceeded)
				mockTask.ContentLength.Store(resource.TinyFileSize + 1)
				mockTask.TotalPieceCount.Store(2)
				mockPeer.FSM.SetState(resource.PeerStateFailed)

				gomock.InOrder(
					ms.RegisterTask(context.Background(), req).Return(mockTask, nil).Times(1),
					ms.LoadOrStoreHost(context.Background(), req).Return(mockHost, true).Times(1),
					ms.LoadOrStorePeer(context.Background(), req, gomock.Any(), gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedError)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			svc := mocks.NewMockService(ctl)
			scheduler := schedulermocks.NewMockScheduler(ctl)

			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			tc.mock(tc.req, mockPeer, mockHost, mockTask, scheduler, svc.EXPECT(), scheduler.EXPECT())
			svr := New(svc)
			result, err := svr.RegisterPeerTask(context.Background(), tc.req)
			tc.expect(t, result, err)
		})
	}
}

func TestRPCServer_ReportPieceResult(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockPeer *resource.Peer, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *mocks.MockServiceMockRecorder, mstream *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder)
		expect func(t *testing.T, mockPeer *resource.Peer, err error)
	}{
		{
			name: "receive begin of piece failed",
			mock: func(mockPeer *resource.Peer, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *mocks.MockServiceMockRecorder, mstream *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				gomock.InOrder(
					mstream.Context().Return(context.Background()).Times(1),
					mstream.Recv().Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "receive begin of piece failed because of io EOF",
			mock: func(mockPeer *resource.Peer, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *mocks.MockServiceMockRecorder, mstream *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				gomock.InOrder(
					mstream.Context().Return(context.Background()).Times(1),
					mstream.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "peer not found",
			mock: func(mockPeer *resource.Peer, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *mocks.MockServiceMockRecorder, mstream *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				gomock.InOrder(
					mstream.Context().Return(context.Background()).Times(1),
					mstream.Recv().Return(&rpcscheduler.PieceResult{
						SrcPid: mockPeerID,
					}, nil).Times(1),
					ms.LoadPeer(gomock.Eq(mockPeerID)).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedPeerNotFound)
			},
		},
		{
			name: "context canceled",
			mock: func(mockPeer *resource.Peer, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *mocks.MockServiceMockRecorder, mstream *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				gomock.InOrder(
					mstream.Context().Return(ctx).Times(1),
					mstream.Recv().Return(&rpcscheduler.PieceResult{
						SrcPid: mockPeerID,
					}, nil).Times(1),
					ms.LoadPeer(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.HandlePiece(gomock.Any(), gomock.Any(), gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "context canceled")
			},
		},
		{
			name: "receive piece failed",
			mock: func(mockPeer *resource.Peer, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *mocks.MockServiceMockRecorder, mstream *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				gomock.InOrder(
					mstream.Context().Return(context.Background()).Times(1),
					mstream.Recv().Return(&rpcscheduler.PieceResult{
						SrcPid: mockPeerID,
					}, nil).Times(1),
					ms.LoadPeer(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.HandlePiece(gomock.Any(), gomock.Any(), gomock.Any()).Return().Times(1),
					mstream.Recv().Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "receive piece failed because of io EOF",
			mock: func(mockPeer *resource.Peer, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *mocks.MockServiceMockRecorder, mstream *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				gomock.InOrder(
					mstream.Context().Return(context.Background()).Times(1),
					mstream.Recv().Return(&rpcscheduler.PieceResult{
						SrcPid: mockPeerID,
					}, nil).Times(1),
					ms.LoadPeer(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.HandlePiece(gomock.Any(), gomock.Any(), gomock.Any()).Return().Times(1),
					mstream.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			svc := mocks.NewMockService(ctl)
			stream := rpcschedulermocks.NewMockScheduler_ReportPieceResultServer(ctl)

			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			mockPeer.StoreStream(stream)
			tc.mock(mockPeer, stream, svc.EXPECT(), stream.EXPECT())
			svr := New(svc)
			tc.expect(t, mockPeer, svr.ReportPieceResult(stream))
		})
	}
}

func TestRPCServer_ReportPeerResult(t *testing.T) {
	tests := []struct {
		name   string
		req    *rpcscheduler.PeerResult
		mock   func(mockPeer *resource.Peer, ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, err error)
	}{
		{
			name: "peer not found",
			req: &rpcscheduler.PeerResult{
				PeerId: mockPeerID,
			},
			mock: func(mockPeer *resource.Peer, ms *mocks.MockServiceMockRecorder) {
				ms.LoadPeer(gomock.Eq(mockPeerID)).Return(nil, false).Times(1)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedPeerNotFound)
			},
		},
		{
			name: "report peer success",
			req: &rpcscheduler.PeerResult{
				PeerId: mockPeerID,
			},
			mock: func(mockPeer *resource.Peer, ms *mocks.MockServiceMockRecorder) {
				gomock.InOrder(
					ms.LoadPeer(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.HandlePeer(gomock.Any(), gomock.Eq(mockPeer), gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			svc := mocks.NewMockService(ctl)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)

			tc.mock(mockPeer, svc.EXPECT())
			svr := New(svc)
			tc.expect(t, svr.ReportPeerResult(context.Background(), tc.req))
		})
	}
}

func TestRPCServer_LeaveTask(t *testing.T) {
	tests := []struct {
		name   string
		req    *rpcscheduler.PeerTarget
		mock   func(mockPeer *resource.Peer, ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, err error)
	}{
		{
			name: "peer not found",
			req: &rpcscheduler.PeerTarget{
				PeerId: mockPeerID,
			},
			mock: func(mockPeer *resource.Peer, ms *mocks.MockServiceMockRecorder) {
				ms.LoadPeer(gomock.Eq(mockPeerID)).Return(nil, false).Times(1)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedPeerNotFound)
			},
		},
		{
			name: "peer leave",
			req: &rpcscheduler.PeerTarget{
				PeerId: mockPeerID,
			},
			mock: func(mockPeer *resource.Peer, ms *mocks.MockServiceMockRecorder) {
				gomock.InOrder(
					ms.LoadPeer(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.HandlePeerLeave(gomock.Any(), gomock.Eq(mockPeer)).Return().Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			svc := mocks.NewMockService(ctl)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)

			tc.mock(mockPeer, svc.EXPECT())
			svr := New(svc)
			tc.expect(t, svr.LeaveTask(context.Background(), tc.req))
		})
	}
}
