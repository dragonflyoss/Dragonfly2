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

package service

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	rpcschedulermocks "d7y.io/dragonfly/v2/pkg/rpc/scheduler/mocks"
	"d7y.io/dragonfly/v2/scheduler/config"
	configmocks "d7y.io/dragonfly/v2/scheduler/config/mocks"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
	"d7y.io/dragonfly/v2/scheduler/scheduler/mocks"
	storagemocks "d7y.io/dragonfly/v2/scheduler/storage/mocks"
)

var (
	mockSchedulerConfig = &config.SchedulerConfig{
		RetryLimit:           10,
		RetryBackSourceLimit: 3,
		RetryInterval:        10 * time.Millisecond,
		BackSourceCount:      int(mockTaskBackToSourceLimit),
	}

	mockRawHost = &rpcscheduler.PeerHost{
		Id:             idgen.HostID("hostname", 8003),
		Ip:             "127.0.0.1",
		RpcPort:        8003,
		DownPort:       8001,
		HostName:       "hostname",
		SecurityDomain: "security_domain",
		Location:       "location",
		Idc:            "idc",
		NetTopology:    "net_topology",
	}

	mockRawSeedHost = &rpcscheduler.PeerHost{
		Id:             idgen.HostID("hostname_seed", 8003),
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

	mockTaskURL                     = "http://example.com/foo"
	mockTaskBackToSourceLimit int32 = 200
	mockTaskID                      = idgen.TaskID(mockTaskURL, mockTaskURLMeta)
	mockPeerID                      = idgen.PeerID("127.0.0.1")
	mockSeedPeerID                  = idgen.SeedPeerID("127.0.0.1")
	mockURL                         = "d7y://foo"
)

func TestService_New(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s any)
	}{
		{
			name: "new service",
			expect: func(t *testing.T, s any) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "Service")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			resource := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			tc.expect(t, New(&config.Config{Scheduler: mockSchedulerConfig}, resource, scheduler, dynconfig, storage))
		})
	}
}

func TestService_RegisterPeerTask(t *testing.T) {
	tests := []struct {
		name string
		req  *rpcscheduler.PeerTaskRequest
		mock func(
			req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
			scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
			ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
		)
		expect func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error)
	}{
		{
			name: "task register failed",
			req: &rpcscheduler.PeerTaskRequest{
				UrlMeta: &base.UrlMeta{},
			},
			mock: func(
				req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateRunning)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockPeer.Task, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedTaskStatusError)
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task state is TaskStateFailed",
			req: &rpcscheduler.PeerTaskRequest{
				UrlMeta: &base.UrlMeta{},
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawHost.Id,
				},
			},
			mock: func(
				req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateRunning)
				mockPeer.Task.StorePeer(mockSeedPeer)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, base.SizeScope_NORMAL)
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task state is TaskStateFailed and peer state is PeerStateFailed",
			req: &rpcscheduler.PeerTaskRequest{
				UrlMeta: &base.UrlMeta{},
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawHost.Id,
				},
			},
			mock: func(
				req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateRunning)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedError)
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "get task scope size failed",
			req: &rpcscheduler.PeerTaskRequest{
				UrlMeta: &base.UrlMeta{},
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawHost.Id,
				},
			},
			mock: func(
				req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(-1)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, base.SizeScope_NORMAL)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedNormal))
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_TINY",
			req: &rpcscheduler.PeerTaskRequest{
				UrlMeta: &base.UrlMeta{},
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawHost.Id,
				},
			},
			mock: func(
				req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(1)
				mockPeer.Task.TotalPieceCount.Store(1)
				mockPeer.Task.DirectPiece = []byte{1}
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, base.SizeScope_TINY)
				assert.Equal(result.DirectPiece, &rpcscheduler.RegisterResult_PieceContent{
					PieceContent: peer.Task.DirectPiece,
				})
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_TINY and direct piece content is empty",
			req: &rpcscheduler.PeerTaskRequest{
				UrlMeta: &base.UrlMeta{},
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawHost.Id,
				},
			},
			mock: func(
				req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(1)
				mockPeer.Task.TotalPieceCount.Store(1)
				mockPeer.Task.DirectPiece = []byte{1}
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedError)
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_TINY and direct piece content is error, peer state is PeerStateFailed",
			req: &rpcscheduler.PeerTaskRequest{
				UrlMeta: &base.UrlMeta{},
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawHost.Id,
				},
			},
			mock: func(
				req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(1)
				mockPeer.Task.TotalPieceCount.Store(1)
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
					ms.FindParent(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedError)
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_TINY and direct piece content is error",
			req: &rpcscheduler.PeerTaskRequest{
				UrlMeta: &base.UrlMeta{},
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawHost.Id,
				},
			},
			mock: func(
				req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(1)
				mockPeer.Task.TotalPieceCount.Store(1)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
					ms.FindParent(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, base.SizeScope_NORMAL)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedNormal))
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_SMALL and load piece error, parent state is PeerStateRunning",
			req: &rpcscheduler.PeerTaskRequest{
				UrlMeta: &base.UrlMeta{},
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawHost.Id,
				},
			},
			mock: func(
				req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(129)
				mockPeer.Task.StorePiece(&base.PieceInfo{
					PieceNum: 0,
				})
				mockPeer.Task.TotalPieceCount.Store(1)
				mockPeer.FSM.SetState(resource.PeerStatePending)
				mockSeedPeer.FSM.SetState(resource.PeerStateRunning)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
					ms.FindParent(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSeedPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, base.SizeScope_NORMAL)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedNormal))
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_SMALL and load piece error, peer state is PeerStateFailed",
			req: &rpcscheduler.PeerTaskRequest{
				UrlMeta: &base.UrlMeta{},
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawHost.Id,
				},
			},
			mock: func(
				req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(129)
				mockPeer.Task.StorePiece(&base.PieceInfo{
					PieceNum: 0,
				})
				mockPeer.Task.TotalPieceCount.Store(1)
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				mockSeedPeer.FSM.SetState(resource.PeerStateSucceeded)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
					ms.FindParent(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSeedPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedError)
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_SMALL and peer state is PeerStateFailed",
			req: &rpcscheduler.PeerTaskRequest{
				UrlMeta: &base.UrlMeta{},
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawHost.Id,
				},
			},
			mock: func(
				req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(129)
				mockPeer.Task.StorePiece(&base.PieceInfo{
					PieceNum: 0,
				})
				mockPeer.Task.TotalPieceCount.Store(1)
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				mockSeedPeer.FSM.SetState(resource.PeerStateSucceeded)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
					ms.FindParent(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSeedPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedError)
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_SMALL",
			req: &rpcscheduler.PeerTaskRequest{
				UrlMeta: &base.UrlMeta{},
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawHost.Id,
				},
			},
			mock: func(
				req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(129)
				mockPeer.Task.StorePiece(&base.PieceInfo{
					PieceNum: 0,
				})
				mockPeer.Task.TotalPieceCount.Store(1)
				mockSeedPeer.FSM.SetState(resource.PeerStateSucceeded)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
					ms.FindParent(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSeedPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, base.SizeScope_SMALL)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedSmall))
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_NORMAL and peer state is PeerStateFailed",
			req: &rpcscheduler.PeerTaskRequest{
				UrlMeta: &base.UrlMeta{},
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawHost.Id,
				},
			},
			mock: func(
				req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(129)
				mockPeer.Task.TotalPieceCount.Store(2)
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedError)
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
		{
			name: "task scope size is SizeScope_NORMAL",
			req: &rpcscheduler.PeerTaskRequest{
				UrlMeta: &base.UrlMeta{},
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawHost.Id,
				},
			},
			mock: func(
				req *rpcscheduler.PeerTaskRequest, mockPeer *resource.Peer, mockSeedPeer *resource.Peer,
				scheduler scheduler.Scheduler, res resource.Resource, hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder,
			) {
				mockPeer.Task.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.ContentLength.Store(129)
				mockPeer.Task.TotalPieceCount.Store(2)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockPeer.Task, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockPeer.Host.ID)).Return(mockPeer.Host, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, result *rpcscheduler.RegisterResult, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(result.TaskId, peer.Task.ID)
				assert.Equal(result.SizeScope, base.SizeScope_NORMAL)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedNormal))
				assert.Equal(peer.NeedBackToSource.Load(), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			taskManager := resource.NewMockTaskManager(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler, dynconfig, storage)

			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			mockSeedHost := resource.NewHost(mockRawSeedHost)
			mockSeedPeer := resource.NewPeer(mockSeedPeerID, mockTask, mockSeedHost)
			tc.mock(
				tc.req, mockPeer, mockSeedPeer,
				scheduler, res, hostManager, taskManager, peerManager,
				scheduler.EXPECT(), res.EXPECT(), hostManager.EXPECT(), taskManager.EXPECT(), peerManager.EXPECT(),
			)

			result, err := svc.RegisterPeerTask(context.Background(), tc.req)
			tc.expect(t, mockPeer, result, err)
		})
	}
}

func TestService_ReportPieceResult(t *testing.T) {
	tests := []struct {
		name string
		mock func(
			mockPeer *resource.Peer,
			res resource.Resource, peerManager resource.PeerManager,
			mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder,
		)
		expect func(t *testing.T, peer *resource.Peer, err error)
	}{
		{
			name: "context was done",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				ctx, cancel := context.WithCancel(context.Background())
				gomock.InOrder(
					ms.Context().Return(ctx).Times(1),
				)
				cancel()
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "context canceled")
			},
		},
		{
			name: "receive error",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "receive EOF",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "load peer error",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(&rpcscheduler.PieceResult{
						SrcPid: mockPeerID,
					}, nil).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedPeerNotFound)
			},
		},
		{
			name: "revice begin of piece",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				mockPeer.FSM.SetState(resource.PeerStateBackToSource)
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(&rpcscheduler.PieceResult{
						SrcPid: mockPeerID,
						PieceInfo: &base.PieceInfo{
							PieceNum: common.BeginOfPiece,
						},
					}, nil).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				_, ok := peer.LoadStream()
				assert.False(ok)
			},
		},
		{
			name: "revice end of piece",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(&rpcscheduler.PieceResult{
						SrcPid: mockPeerID,
						PieceInfo: &base.PieceInfo{
							PieceNum: common.EndOfPiece,
						},
					}, nil).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				_, ok := peer.LoadStream()
				assert.False(ok)
			},
		},
		{
			name: "revice successful piece",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(&rpcscheduler.PieceResult{
						SrcPid:  mockPeerID,
						Success: true,
						PieceInfo: &base.PieceInfo{
							PieceNum: 1,
						},
					}, nil).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				_, ok := peer.LoadStream()
				assert.False(ok)
			},
		},
		{
			name: "revice Code_ClientWaitPieceReady code",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(&rpcscheduler.PieceResult{
						SrcPid: mockPeerID,
						Code:   base.Code_ClientWaitPieceReady,
					}, nil).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				_, ok := peer.LoadStream()
				assert.False(ok)
			},
		},
		{
			name: "revice Code_PeerTaskNotFound code",
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder,

			) {
				mockPeer.FSM.SetState(resource.PeerStateBackToSource)
				gomock.InOrder(
					ms.Context().Return(context.Background()).Times(1),
					ms.Recv().Return(&rpcscheduler.PieceResult{
						SrcPid: mockPeerID,
						Code:   base.Code_PeerTaskNotFound,
					}, nil).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				_, ok := peer.LoadStream()
				assert.False(ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			stream := rpcschedulermocks.NewMockScheduler_ReportPieceResultServer(ctl)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler, dynconfig, storage)

			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			tc.mock(mockPeer, res, peerManager, res.EXPECT(), peerManager.EXPECT(), stream.EXPECT())
			tc.expect(t, mockPeer, svc.ReportPieceResult(stream))
		})
	}
}

func TestService_ReportPeerResult(t *testing.T) {
	tests := []struct {
		name string
		req  *rpcscheduler.PeerResult
		mock func(
			mockPeer *resource.Peer,
			res resource.Resource, peerManager resource.PeerManager,
			mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
		)
		expect func(t *testing.T, peer *resource.Peer, err error)
	}{
		{
			name: "peer not found",
			req: &rpcscheduler.PeerResult{
				PeerId: mockPeerID,
			},
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
			) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedPeerNotFound)
			},
		},
		{
			name: "receive peer failed",
			req: &rpcscheduler.PeerResult{
				Success: false,
				PeerId:  mockPeerID,
			},
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
			) {
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				mockPeer.StoreParent(mockPeer)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Create(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "receive peer failed, and peer state is PeerStateBackToSource",
			req: &rpcscheduler.PeerResult{
				Success: false,
				PeerId:  mockPeerID,
			},
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
			) {
				mockPeer.FSM.SetState(resource.PeerStateBackToSource)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Create(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "receive peer success",
			req: &rpcscheduler.PeerResult{
				Success: true,
				PeerId:  mockPeerID,
			},
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
			) {
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				mockPeer.StoreParent(mockPeer)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Create(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "receive peer success, and peer state is PeerStateBackToSource",
			req: &rpcscheduler.PeerResult{
				Success: true,
				PeerId:  mockPeerID,
			},
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
			) {
				mockPeer.FSM.SetState(resource.PeerStateBackToSource)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Create(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "receive peer success, and peer state is PeerStateBackToSource",
			req: &rpcscheduler.PeerResult{
				Success: true,
				PeerId:  mockPeerID,
			},
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
			) {
				mockPeer.FSM.SetState(resource.PeerStateBackToSource)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Create(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "receive peer success and create record failed",
			req: &rpcscheduler.PeerResult{
				Success: true,
				PeerId:  mockPeerID,
			},
			mock: func(
				mockPeer *resource.Peer,
				res resource.Resource, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder,
			) {
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				mockPeer.StoreParent(mockPeer)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
					ms.Create(gomock.Any()).Return(errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler, dynconfig, storage)

			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			tc.mock(mockPeer, res, peerManager, res.EXPECT(), peerManager.EXPECT(), storage.EXPECT())
			tc.expect(t, mockPeer, svc.ReportPeerResult(context.Background(), tc.req))
		})
	}
}

func TestService_StatTask(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockTask *resource.Task, taskManager resource.TaskManager, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder)
		expect func(t *testing.T, task *rpcscheduler.Task, err error)
	}{
		{
			name: "task not found",
			mock: func(mockTask *resource.Task, taskManager resource.TaskManager, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder) {
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, task *rpcscheduler.Task, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name: "stat task",
			mock: func(mockTask *resource.Task, taskManager resource.TaskManager, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder) {
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(mockTask, true).Times(1),
				)
			},
			expect: func(t *testing.T, task *rpcscheduler.Task, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(task, &rpcscheduler.Task{
					Id:               mockTaskID,
					Type:             base.TaskType_Normal,
					ContentLength:    0,
					TotalPieceCount:  0,
					State:            resource.TaskStatePending,
					PeerCount:        0,
					HasAvailablePeer: false,
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			taskManager := resource.NewMockTaskManager(ctl)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig, Metrics: &config.MetricsConfig{EnablePeerHost: true}}, res, scheduler, dynconfig, storage)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))

			tc.mock(mockTask, taskManager, res.EXPECT(), taskManager.EXPECT())
			task, err := svc.StatTask(context.Background(), &rpcscheduler.StatTaskRequest{TaskId: mockTaskID})
			tc.expect(t, task, err)
		})
	}
}

func TestService_AnnounceTask(t *testing.T) {
	tests := []struct {
		name string
		req  *rpcscheduler.AnnounceTaskRequest
		mock func(mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
			hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
			mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder)
		expect func(t *testing.T, mockTask *resource.Task, mockPeer *resource.Peer, err error)
	}{
		{
			name: "task state is TaskStateSucceeded and peer state is PeerStateSucceeded",
			req: &rpcscheduler.AnnounceTaskRequest{
				TaskId:  mockTaskID,
				Url:     mockURL,
				UrlMeta: &base.UrlMeta{},
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawHost.Id,
				},
				PiecePacket: &base.PiecePacket{
					PieceInfos: []*base.PieceInfo{{PieceNum: 1}},
					TotalPiece: 1,
				},
			},
			mock: func(mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateSucceeded)
				mockPeer.FSM.SetState(resource.PeerStateSucceeded)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(mockHost, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, mockTask *resource.Task, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateSucceeded)
				assert.Equal(mockPeer.FSM.Current(), resource.PeerStateSucceeded)
			},
		},
		{
			name: "task state is TaskStatePending and peer state is PeerStateSucceeded",
			req: &rpcscheduler.AnnounceTaskRequest{
				TaskId:   mockTaskID,
				Url:      mockURL,
				UrlMeta:  &base.UrlMeta{},
				PeerHost: mockRawHost,
				PiecePacket: &base.PiecePacket{
					PieceInfos:    []*base.PieceInfo{{PieceNum: 1, DownloadCost: 1}},
					TotalPiece:    1,
					ContentLength: 1000,
				},
			},
			mock: func(mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.FSM.SetState(resource.PeerStateSucceeded)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(mockHost, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, mockTask *resource.Task, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateSucceeded)
				assert.Equal(mockTask.TotalPieceCount.Load(), int32(1))
				assert.Equal(mockTask.ContentLength.Load(), int64(1000))
				piece, ok := mockTask.LoadPiece(1)
				assert.True(ok)
				assert.EqualValues(piece, &base.PieceInfo{PieceNum: 1, DownloadCost: 1})

				assert.Equal(mockPeer.Pieces.Count(), uint(1))
				assert.Equal(mockPeer.PieceCosts()[0], int64(1*time.Millisecond))
				assert.Equal(mockPeer.FSM.Current(), resource.PeerStateSucceeded)
			},
		},
		{
			name: "task state is TaskStateFailed and peer state is PeerStateSucceeded",
			req: &rpcscheduler.AnnounceTaskRequest{
				TaskId:   mockTaskID,
				Url:      mockURL,
				UrlMeta:  &base.UrlMeta{},
				PeerHost: mockRawHost,
				PiecePacket: &base.PiecePacket{
					PieceInfos:    []*base.PieceInfo{{PieceNum: 1, DownloadCost: 1}},
					TotalPiece:    1,
					ContentLength: 1000,
				},
			},
			mock: func(mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateFailed)
				mockPeer.FSM.SetState(resource.PeerStateSucceeded)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(mockHost, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, mockTask *resource.Task, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateSucceeded)
				assert.Equal(mockTask.TotalPieceCount.Load(), int32(1))
				assert.Equal(mockTask.ContentLength.Load(), int64(1000))
				piece, ok := mockTask.LoadPiece(1)
				assert.True(ok)

				assert.EqualValues(piece, &base.PieceInfo{PieceNum: 1, DownloadCost: 1})
				assert.Equal(mockPeer.Pieces.Count(), uint(1))
				assert.Equal(mockPeer.PieceCosts()[0], int64(1*time.Millisecond))
				assert.Equal(mockPeer.FSM.Current(), resource.PeerStateSucceeded)
			},
		},
		{
			name: "task state is TaskStatePending and peer state is PeerStatePending",
			req: &rpcscheduler.AnnounceTaskRequest{
				TaskId:   mockTaskID,
				Url:      mockURL,
				UrlMeta:  &base.UrlMeta{},
				PeerHost: mockRawHost,
				PiecePacket: &base.PiecePacket{
					PieceInfos:    []*base.PieceInfo{{PieceNum: 1, DownloadCost: 1}},
					TotalPiece:    1,
					ContentLength: 1000,
				},
			},
			mock: func(mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.FSM.SetState(resource.PeerStatePending)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(mockHost, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, mockTask *resource.Task, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateSucceeded)
				assert.Equal(mockTask.TotalPieceCount.Load(), int32(1))
				assert.Equal(mockTask.ContentLength.Load(), int64(1000))
				piece, ok := mockTask.LoadPiece(1)
				assert.True(ok)

				assert.EqualValues(piece, &base.PieceInfo{PieceNum: 1, DownloadCost: 1})
				assert.Equal(mockPeer.Pieces.Count(), uint(1))
				assert.Equal(mockPeer.PieceCosts()[0], int64(1*time.Millisecond))
				assert.Equal(mockPeer.FSM.Current(), resource.PeerStateSucceeded)
			},
		},
		{
			name: "task state is TaskStatePending and peer state is PeerStateReceivedNormal",
			req: &rpcscheduler.AnnounceTaskRequest{
				TaskId:   mockTaskID,
				Url:      mockURL,
				UrlMeta:  &base.UrlMeta{},
				PeerHost: mockRawHost,
				PiecePacket: &base.PiecePacket{
					PieceInfos:    []*base.PieceInfo{{PieceNum: 1, DownloadCost: 1}},
					TotalPiece:    1,
					ContentLength: 1000,
				},
			},
			mock: func(mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager,
				mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				mockPeer.FSM.SetState(resource.PeerStateReceivedNormal)

				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(mockHost, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, mockTask *resource.Task, mockPeer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(mockTask.FSM.Current(), resource.TaskStateSucceeded)
				assert.Equal(mockTask.TotalPieceCount.Load(), int32(1))
				assert.Equal(mockTask.ContentLength.Load(), int64(1000))
				piece, ok := mockTask.LoadPiece(1)
				assert.True(ok)

				assert.EqualValues(piece, &base.PieceInfo{PieceNum: 1, DownloadCost: 1})
				assert.Equal(mockPeer.Pieces.Count(), uint(1))
				assert.Equal(mockPeer.PieceCosts()[0], int64(1*time.Millisecond))
				assert.Equal(mockPeer.FSM.Current(), resource.PeerStateSucceeded)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			taskManager := resource.NewMockTaskManager(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig, Metrics: &config.MetricsConfig{EnablePeerHost: true}}, res, scheduler, dynconfig, storage)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)

			tc.mock(mockHost, mockTask, mockPeer, hostManager, taskManager, peerManager, res.EXPECT(), hostManager.EXPECT(), taskManager.EXPECT(), peerManager.EXPECT())
			tc.expect(t, mockTask, mockPeer, svc.AnnounceTask(context.Background(), tc.req))
		})
	}
}

func TestService_LeaveTask(t *testing.T) {
	mockHost := resource.NewHost(mockRawHost)
	mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))

	tests := []struct {
		name   string
		peer   *resource.Peer
		child  *resource.Peer
		mock   func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, err error)
	}{
		{
			name:  "peer not found",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStatePending)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedPeerNotFound)
				assert.True(peer.FSM.Is(resource.PeerStatePending))
			},
		},
		{
			name:  "peer state is PeerStatePending",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStatePending)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedTaskStatusError)
				assert.True(peer.FSM.Is(resource.PeerStatePending))
			},
		},
		{
			name:  "peer state is PeerStateReceivedSmall",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedSmall)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedTaskStatusError)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedSmall))
			},
		},
		{
			name:  "peer state is PeerStateReceivedNormal",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedNormal)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedTaskStatusError)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedNormal))
			},
		},
		{
			name:  "peer state is PeerStateRunning",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedTaskStatusError)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name:  "peer state is PeerStateBackToSource",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedTaskStatusError)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
			},
		},
		{
			name:  "peer state is PeerStateLeave",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateLeave)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				dferr, ok := err.(*dferrors.DfError)
				assert.True(ok)
				assert.Equal(dferr.Code, base.Code_SchedTaskStatusError)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name:  "peer state is PeerStateSucceeded and children need to be scheduled",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.StoreChild(child)
				peer.FSM.SetState(resource.PeerStateSucceeded)
				child.FSM.SetState(resource.PeerStateRunning)

				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(child), gomock.Eq(set.NewSafeSet())).Return().Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Eq(peer.ID)).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name:  "peer state is PeerStateSucceeded and it has no children",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateSucceeded)

				blocklist := set.NewSafeSet()
				blocklist.Add(peer.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Eq(peer.ID)).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name:  "peer state is PeerStateFailed and children need to be scheduled",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.StoreChild(child)
				peer.FSM.SetState(resource.PeerStateFailed)
				child.FSM.SetState(resource.PeerStateRunning)

				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(child), gomock.Eq(set.NewSafeSet())).Return().Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Eq(peer.ID)).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name:  "peer state is PeerStateFailed and it has no children",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateFailed)

				blocklist := set.NewSafeSet()
				blocklist.Add(peer.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Eq(peer.ID)).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig, Metrics: &config.MetricsConfig{EnablePeerHost: true}}, res, scheduler, dynconfig, storage)

			tc.mock(tc.peer, tc.child, peerManager, scheduler.EXPECT(), res.EXPECT(), peerManager.EXPECT())
			tc.expect(t, tc.peer, svc.LeaveTask(context.Background(), &rpcscheduler.PeerTarget{}))
		})
	}
}

func TestService_registerTask(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		req    *rpcscheduler.PeerTaskRequest
		run    func(t *testing.T, svc *Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, hostManager resource.HostManager, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mh *resource.MockHostManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder)
	}{
		{
			name: "task already exists and state is TaskStatePending",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: &config.SeedPeerConfig{
					Enable: true,
				},
			},
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
			},
			run: func(t *testing.T, svc *Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, hostManager resource.HostManager, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mh *resource.MockHostManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateRunning)
				mockTask.StorePeer(mockPeer)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
				)

				task, needBackToSource, err := svc.registerTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.False(needBackToSource)
				assert.EqualValues(mockTask, task)
			},
		},
		{
			name: "task already exists and state is TaskStateRunning",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: &config.SeedPeerConfig{
					Enable: true,
				},
			},
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
			},
			run: func(t *testing.T, svc *Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, hostManager resource.HostManager, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mh *resource.MockHostManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateRunning)
				mockTask.StorePeer(mockPeer)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
				)

				task, needBackToSource, err := svc.registerTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.False(needBackToSource)
				assert.EqualValues(mockTask, task)
			},
		},
		{
			name: "task already exists and state is TaskStateSucceeded",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: &config.SeedPeerConfig{
					Enable: true,
				},
			},
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
			},
			run: func(t *testing.T, svc *Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, hostManager resource.HostManager, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mh *resource.MockHostManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateSucceeded)
				mockTask.StorePeer(mockPeer)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
				)

				task, needBackToSource, err := svc.registerTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.False(needBackToSource)
				assert.EqualValues(mockTask, task)
			},
		},
		{
			name: "task state is TaskStatePending",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: &config.SeedPeerConfig{
					Enable: true,
				},
			},
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawSeedHost.Id,
				},
			},
			run: func(t *testing.T, svc *Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, hostManager resource.HostManager, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mh *resource.MockHostManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(2)
				defer wg.Wait()

				mockTask.FSM.SetState(resource.TaskStatePending)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, false).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
					mr.SeedPeer().Do(func() { wg.Done() }).Return(seedPeer).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, task *resource.Task) { wg.Done() }).Return(mockPeer, &rpcscheduler.PeerResult{}, nil).Times(1),
				)

				task, needBackToSource, err := svc.registerTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.False(needBackToSource)
				assert.EqualValues(mockTask, task)
			},
		},
		{
			name: "task state is TaskStateFailed",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: &config.SeedPeerConfig{
					Enable: true,
				},
			},
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawSeedHost.Id,
				},
			},
			run: func(t *testing.T, svc *Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, hostManager resource.HostManager, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mh *resource.MockHostManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(2)
				defer wg.Wait()

				mockTask.FSM.SetState(resource.TaskStateFailed)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false),
					mr.SeedPeer().Do(func() { wg.Done() }).Return(seedPeer).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, task *resource.Task) { wg.Done() }).Return(mockPeer, &rpcscheduler.PeerResult{}, nil).Times(1),
				)

				task, needBackToSource, err := svc.registerTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.False(needBackToSource)
				assert.EqualValues(mockTask, task)
			},
		},
		{
			name: "task state is TaskStateFailed and host type is HostTypeSuperSeed",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: &config.SeedPeerConfig{
					Enable: true,
				},
			},
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawSeedHost.Id,
				},
			},
			run: func(t *testing.T, svc *Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, hostManager resource.HostManager, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mh *resource.MockHostManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				mockHost := resource.NewHost(mockRawSeedHost, resource.WithHostType(resource.HostTypeSuperSeed))
				mockTask.FSM.SetState(resource.TaskStateFailed)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(mockHost, true).Times(1),
				)

				task, needBackToSource, err := svc.registerTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.True(needBackToSource)
				assert.EqualValues(mockTask, task)
			},
		},
		{
			name: "task state is TaskStatePending, but trigger seedPeer failed",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: &config.SeedPeerConfig{
					Enable: true,
				},
			},
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawSeedHost.Id,
				},
			},
			run: func(t *testing.T, svc *Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, hostManager resource.HostManager, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mh *resource.MockHostManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(2)
				defer wg.Wait()

				mockTask.FSM.SetState(resource.TaskStatePending)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, false).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
					mr.SeedPeer().Do(func() { wg.Done() }).Return(seedPeer).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, task *resource.Task) { wg.Done() }).Return(mockPeer, &rpcscheduler.PeerResult{}, errors.New("foo")).Times(1),
				)

				task, needBackToSource, err := svc.registerTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.False(needBackToSource)
				assert.EqualValues(mockTask, task)
			},
		},
		{
			name: "task state is TaskStateFailed, but trigger seedPeer failed",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: &config.SeedPeerConfig{
					Enable: true,
				},
			},
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawSeedHost.Id,
				},
			},
			run: func(t *testing.T, svc *Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, hostManager resource.HostManager, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mh *resource.MockHostManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(2)
				defer wg.Wait()

				mockTask.FSM.SetState(resource.TaskStateFailed)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
					mr.SeedPeer().Do(func() { wg.Done() }).Return(seedPeer).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, task *resource.Task) { wg.Done() }).Return(mockPeer, &rpcscheduler.PeerResult{}, errors.New("foo")).Times(1),
				)

				task, needBackToSource, err := svc.registerTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.False(needBackToSource)
				assert.EqualValues(mockTask, task)
			},
		},
		{
			name: "task state is TaskStatePending and disable seed peer",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: &config.SeedPeerConfig{
					Enable: false,
				},
			},
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawSeedHost.Id,
				},
			},
			run: func(t *testing.T, svc *Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, hostManager resource.HostManager, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mh *resource.MockHostManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStatePending)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, false).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
				)

				task, needBackToSource, err := svc.registerTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.True(needBackToSource)
				assert.EqualValues(mockTask, task)
			},
		},
		{
			name: "task state is TaskStateFailed and disable seed peer",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer: &config.SeedPeerConfig{
					Enable: false,
				},
			},
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
				PeerHost: &rpcscheduler.PeerHost{
					Id: mockRawSeedHost.Id,
				},
			},
			run: func(t *testing.T, svc *Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, hostManager resource.HostManager, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mh *resource.MockHostManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateFailed)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
				)

				task, needBackToSource, err := svc.registerTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.True(needBackToSource)
				assert.EqualValues(mockTask, task)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			svc := New(tc.config, res, scheduler, dynconfig, storage)

			taskManager := resource.NewMockTaskManager(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			seedPeer := resource.NewMockSeedPeer(ctl)
			tc.run(t, svc, tc.req, mockTask, mockPeer, taskManager, hostManager, seedPeer, res.EXPECT(), taskManager.EXPECT(), hostManager.EXPECT(), seedPeer.EXPECT())
		})
	}
}

func TestService_registerHost(t *testing.T) {
	tests := []struct {
		name   string
		req    *rpcscheduler.PeerTaskRequest
		mock   func(mockHost *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
		expect func(t *testing.T, host *resource.Host)
	}{
		{
			name: "host already exists",
			req: &rpcscheduler.PeerTaskRequest{
				Url:      mockTaskURL,
				UrlMeta:  mockTaskURLMeta,
				PeerHost: mockRawHost,
			},
			mock: func(mockHost *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockRawHost.Id)).Return(mockHost, true).Times(1),
				)
			},
			expect: func(t *testing.T, host *resource.Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.Id)
			},
		},
		{
			name: "host does not exist",
			req: &rpcscheduler.PeerTaskRequest{
				Url:      mockTaskURL,
				UrlMeta:  mockTaskURLMeta,
				PeerHost: mockRawHost,
			},
			mock: func(mockHost *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockRawHost.Id)).Return(nil, false).Times(1),
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{LoadLimit: 10}, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Store(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, host *resource.Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.Id)
				assert.Equal(host.UploadLoadLimit.Load(), int32(10))
			},
		},
		{
			name: "host does not exist and dynconfig get cluster client config failed",
			req: &rpcscheduler.PeerTaskRequest{
				Url:      mockTaskURL,
				UrlMeta:  mockTaskURLMeta,
				PeerHost: mockRawHost,
			},
			mock: func(mockHost *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockRawHost.Id)).Return(nil, false).Times(1),
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{}, false).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Store(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, host *resource.Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.Id)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler, dynconfig, storage)
			hostManager := resource.NewMockHostManager(ctl)
			mockHost := resource.NewHost(mockRawHost)

			tc.mock(mockHost, hostManager, res.EXPECT(), hostManager.EXPECT(), dynconfig.EXPECT())
			host := svc.registerHost(context.Background(), tc.req.PeerHost)
			tc.expect(t, host)
		})
	}
}

func TestService_triggerSeedPeerTask(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(task *resource.Task, peer *resource.Peer, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder)
		expect func(t *testing.T, task *resource.Task, peer *resource.Peer)
	}{
		{
			name: "trigger seed peer task",
			mock: func(task *resource.Task, peer *resource.Peer, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				task.FSM.SetState(resource.TaskStateRunning)
				peer.FSM.SetState(resource.PeerStateRunning)
				gomock.InOrder(
					mr.SeedPeer().Return(seedPeer).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any()).Return(peer, &rpcscheduler.PeerResult{
						TotalPieceCount: 3,
						ContentLength:   1024,
					}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, task *resource.Task, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateSucceeded))
				assert.Equal(task.TotalPieceCount.Load(), int32(3))
				assert.Equal(task.ContentLength.Load(), int64(1024))
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
			},
		},
		{
			name: "trigger seed peer task failed",
			mock: func(task *resource.Task, peer *resource.Peer, seedPeer resource.SeedPeer, mr *resource.MockResourceMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				task.FSM.SetState(resource.TaskStateRunning)
				gomock.InOrder(
					mr.SeedPeer().Return(seedPeer).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any()).Return(peer, &rpcscheduler.PeerResult{}, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, task *resource.Task, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateFailed))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			seedPeer := resource.NewMockSeedPeer(ctl)
			mockHost := resource.NewHost(mockRawHost)
			task := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := resource.NewPeer(mockPeerID, task, mockHost)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler, dynconfig, storage)

			tc.mock(task, peer, seedPeer, res.EXPECT(), seedPeer.EXPECT())
			svc.triggerSeedPeerTask(context.Background(), task)
			tc.expect(t, task, peer)
		})
	}
}

func TestService_handleBeginOfPiece(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(peer *resource.Peer, scheduler *mocks.MockSchedulerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name: "peer state is PeerStateBackToSource",
			mock: func(peer *resource.Peer, scheduler *mocks.MockSchedulerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
			},
		},
		{
			name: "peer state is PeerStateReceivedTiny",
			mock: func(peer *resource.Peer, scheduler *mocks.MockSchedulerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedTiny)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateReceivedSmall",
			mock: func(peer *resource.Peer, scheduler *mocks.MockSchedulerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedSmall)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateReceivedNormal",
			mock: func(peer *resource.Peer, scheduler *mocks.MockSchedulerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedNormal)
				scheduler.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(set.NewSafeSet())).Return().Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateSucceeded",
			mock: func(peer *resource.Peer, scheduler *mocks.MockSchedulerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateSucceeded)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler, dynconfig, storage)

			tc.mock(peer, scheduler.EXPECT())
			svc.handleBeginOfPiece(context.Background(), peer)
			tc.expect(t, peer)
		})
	}
}

func TestService_registerPeer(t *testing.T) {
	tests := []struct {
		name   string
		req    *rpcscheduler.PeerTaskRequest
		mock   func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name: "peer already exists",
			req: &rpcscheduler.PeerTaskRequest{
				PeerId:  mockPeerID,
				UrlMeta: &base.UrlMeta{},
			},
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.Equal(peer.BizTag, resource.DefaultBizTag)
			},
		},
		{
			name: "peer does not exists",
			req: &rpcscheduler.PeerTaskRequest{
				PeerId:  mockPeerID,
				UrlMeta: &base.UrlMeta{},
			},
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.Equal(peer.BizTag, resource.DefaultBizTag)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler, dynconfig, storage)
			peerManager := resource.NewMockPeerManager(ctl)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)

			tc.mock(mockPeer, peerManager, res.EXPECT(), peerManager.EXPECT())
			peer := svc.registerPeer(context.Background(), tc.req.PeerId, mockTask, mockHost, tc.req.UrlMeta.Tag)
			tc.expect(t, peer)
		})
	}
}

func TestService_handlePieceSuccess(t *testing.T) {
	mockHost := resource.NewHost(mockRawHost)
	mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
	now := time.Now()

	tests := []struct {
		name   string
		piece  *rpcscheduler.PieceResult
		peer   *resource.Peer
		mock   func(peer *resource.Peer)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name: "piece success",
			piece: &rpcscheduler.PieceResult{
				PieceInfo: &base.PieceInfo{
					PieceNum: 0,
					PieceMd5: "ac32345ef819f03710e2105c81106fdd",
				},
				BeginTime: uint64(now.UnixNano()),
				EndTime:   uint64(now.Add(1 * time.Millisecond).UnixNano()),
			},
			peer: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateRunning)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.Pieces.Count(), uint(1))
				assert.Equal(peer.PieceCosts(), []int64{1})
			},
		},
		{
			name: "piece state is PeerStateBackToSource",
			piece: &rpcscheduler.PieceResult{
				PieceInfo: &base.PieceInfo{
					PieceNum: 0,
					PieceMd5: "ac32345ef819f03710e2105c81106fdd",
				},
				BeginTime: uint64(now.UnixNano()),
				EndTime:   uint64(now.Add(1 * time.Millisecond).UnixNano()),
			},
			peer: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.Pieces.Count(), uint(1))
				assert.Equal(peer.PieceCosts(), []int64{1})
				piece, ok := peer.Task.LoadPiece(0)
				assert.True(ok)
				assert.EqualValues(piece, &base.PieceInfo{
					PieceNum: 0,
					PieceMd5: "ac32345ef819f03710e2105c81106fdd",
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig, Metrics: &config.MetricsConfig{EnablePeerHost: true}}, res, scheduler, dynconfig, storage)

			tc.mock(tc.peer)
			svc.handlePieceSuccess(context.Background(), tc.peer, tc.piece)
			tc.expect(t, tc.peer)
		})
	}
}

func TestService_handlePieceFail(t *testing.T) {
	mockHost := resource.NewHost(mockRawHost)
	mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))

	tests := []struct {
		name   string
		config *config.Config
		piece  *rpcscheduler.PieceResult
		peer   *resource.Peer
		parent *resource.Peer
		run    func(t *testing.T, svc *Service, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, seedPeer resource.SeedPeer, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder)
	}{
		{
			name: "peer state is PeerStateBackToSource",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer:  &config.SeedPeerConfig{Enable: true},
				Metrics:   &config.MetricsConfig{EnablePeerHost: true},
			},
			piece:  &rpcscheduler.PieceResult{},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			run: func(t *testing.T, svc *Service, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, seedPeer resource.SeedPeer, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateBackToSource)

				svc.handlePieceFail(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
			},
		},
		{
			name: "can not found parent",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer:  &config.SeedPeerConfig{Enable: true},
				Metrics:   &config.MetricsConfig{EnablePeerHost: true},
			},
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_ClientWaitPieceReady,
				DstPid: mockSeedPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			run: func(t *testing.T, svc *Service, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, seedPeer resource.SeedPeer, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(mockSeedPeerID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(nil, false).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return().Times(1),
				)

				svc.handlePieceFail(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "piece result code is Code_PeerTaskNotFound and parent state set PeerEventDownloadFailed",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer:  &config.SeedPeerConfig{Enable: true},
				Metrics:   &config.MetricsConfig{EnablePeerHost: true},
			},
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_PeerTaskNotFound,
				DstPid: mockSeedPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			run: func(t *testing.T, svc *Service, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, seedPeer resource.SeedPeer, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return().Times(1),
				)

				svc.handlePieceFail(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStateFailed))
			},
		},
		{
			name: "piece result code is Code_ClientPieceNotFound and parent is not seed peer",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer:  &config.SeedPeerConfig{Enable: true},
				Metrics:   &config.MetricsConfig{EnablePeerHost: true},
			},
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_ClientPieceNotFound,
				DstPid: mockSeedPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			run: func(t *testing.T, svc *Service, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, seedPeer resource.SeedPeer, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Host.Type = resource.HostTypeNormal
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return().Times(1),
				)

				svc.handlePieceFail(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "piece result code is Code_ClientPieceRequestFail",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer:  &config.SeedPeerConfig{Enable: true},
				Metrics:   &config.MetricsConfig{EnablePeerHost: true},
			},
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_ClientPieceRequestFail,
				DstPid: mockSeedPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			run: func(t *testing.T, svc *Service, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, seedPeer resource.SeedPeer, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return().Times(1),
				)

				svc.handlePieceFail(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "piece result code is unknow",
			config: &config.Config{
				Scheduler: mockSchedulerConfig,
				SeedPeer:  &config.SeedPeerConfig{Enable: true},
				Metrics:   &config.MetricsConfig{EnablePeerHost: true},
			},
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_ClientPieceRequestFail,
				DstPid: mockSeedPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			run: func(t *testing.T, svc *Service, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, seedPeer resource.SeedPeer, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockSeedPeerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return().Times(1),
				)

				svc.handlePieceFail(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStateRunning))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			seedPeer := resource.NewMockSeedPeer(ctl)
			svc := New(tc.config, res, scheduler, dynconfig, storage)

			tc.run(t, svc, tc.peer, tc.parent, tc.piece, peerManager, seedPeer, scheduler.EXPECT(), res.EXPECT(), peerManager.EXPECT(), seedPeer.EXPECT())
		})
	}
}

func TestService_handlePeerSuccess(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write([]byte{1}); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer s.Close()

	tests := []struct {
		name   string
		mock   func(peer *resource.Peer)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name: "peer is tiny type and download piece success",
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
				peer.Task.ContentLength.Store(1)
				peer.Task.TotalPieceCount.Store(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.Task.DirectPiece, []byte{1})
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
			},
		},
		{
			name: "get task size scope failed",
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
				peer.Task.ContentLength.Store(-1)
				peer.Task.TotalPieceCount.Store(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.Task.DirectPiece, []byte(nil))
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
			},
		},
		{
			name: "peer is tiny type and download piece failed",
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Empty(peer.Task.DirectPiece)
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
			},
		},
		{
			name: "peer is small and state is PeerStateBackToSource",
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
				peer.Task.ContentLength.Store(resource.TinyFileSize + 1)
				peer.Task.TotalPieceCount.Store(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Empty(peer.Task.DirectPiece)
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
			},
		},
		{
			name: "peer is small and state is PeerStateRunning",
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.ContentLength.Store(resource.TinyFileSize + 1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Empty(peer.Task.DirectPiece)
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)

			url, err := url.Parse(s.URL)
			if err != nil {
				t.Fatal(err)
			}

			ip, rawPort, err := net.SplitHostPort(url.Host)
			if err != nil {
				t.Fatal(err)
			}

			port, err := strconv.ParseInt(rawPort, 10, 32)
			if err != nil {
				t.Fatal(err)
			}

			mockRawHost.Ip = ip
			mockRawHost.DownPort = int32(port)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig, Metrics: &config.MetricsConfig{EnablePeerHost: true}}, res, scheduler, dynconfig, storage)

			tc.mock(peer)
			svc.handlePeerSuccess(context.Background(), peer)
			tc.expect(t, peer)
		})
	}
}

func TestService_handlePeerFail(t *testing.T) {
	mockHost := resource.NewHost(mockRawHost)
	mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))

	tests := []struct {
		name   string
		peer   *resource.Peer
		child  *resource.Peer
		mock   func(peer *resource.Peer, child *resource.Peer, ms *mocks.MockSchedulerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, child *resource.Peer)
	}{
		{
			name:  "peer state is PeerStateFailed",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, ms *mocks.MockSchedulerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateFailed)
			},
			expect: func(t *testing.T, peer *resource.Peer, child *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateFailed))
			},
		},
		{
			name:  "peer state is PeerStateLeave",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, ms *mocks.MockSchedulerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateLeave)
			},
			expect: func(t *testing.T, peer *resource.Peer, child *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name:  "peer state is PeerStateRunning and children need to be scheduled",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, ms *mocks.MockSchedulerMockRecorder) {
				peer.StoreChild(child)
				peer.FSM.SetState(resource.PeerStateRunning)
				child.FSM.SetState(resource.PeerStateRunning)

				ms.ScheduleParent(gomock.Any(), gomock.Eq(child), gomock.Eq(set.NewSafeSet())).Return().Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, child *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateFailed))
			},
		},
		{
			name:  "peer state is PeerStateRunning and it has no children",
			peer:  resource.NewPeer(mockSeedPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, ms *mocks.MockSchedulerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
			},
			expect: func(t *testing.T, peer *resource.Peer, child *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateFailed))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig, Metrics: &config.MetricsConfig{EnablePeerHost: true}}, res, scheduler, dynconfig, storage)

			tc.mock(tc.peer, tc.child, scheduler.EXPECT())
			svc.handlePeerFail(context.Background(), tc.peer)
			tc.expect(t, tc.peer, tc.child)
		})
	}
}

func TestService_handleTaskSuccess(t *testing.T) {
	tests := []struct {
		name   string
		result *rpcscheduler.PeerResult
		mock   func(task *resource.Task)
		expect func(t *testing.T, task *resource.Task)
	}{
		{
			name:   "task state is TaskStatePending",
			result: &rpcscheduler.PeerResult{},
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStatePending)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStatePending))
			},
		},
		{
			name:   "task state is TaskStateSucceeded",
			result: &rpcscheduler.PeerResult{},
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateSucceeded)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateSucceeded))
			},
		},
		{
			name: "task state is TaskStateRunning",
			result: &rpcscheduler.PeerResult{
				TotalPieceCount: 1,
				ContentLength:   1,
			},
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateRunning)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateSucceeded))
				assert.Equal(task.TotalPieceCount.Load(), int32(1))
				assert.Equal(task.ContentLength.Load(), int64(1))
			},
		},
		{
			name: "task state is TaskStateFailed",
			result: &rpcscheduler.PeerResult{
				TotalPieceCount: 1,
				ContentLength:   1,
			},
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateFailed)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateSucceeded))
				assert.Equal(task.TotalPieceCount.Load(), int32(1))
				assert.Equal(task.ContentLength.Load(), int64(1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig, Metrics: &config.MetricsConfig{EnablePeerHost: true}}, res, scheduler, dynconfig, storage)
			task := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))

			tc.mock(task)
			svc.handleTaskSuccess(context.Background(), task, tc.result)
			tc.expect(t, task)
		})
	}
}

func TestService_handleTaskFail(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(task *resource.Task)
		expect func(t *testing.T, task *resource.Task)
	}{
		{
			name: "task state is TaskStatePending",
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStatePending)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStatePending))
			},
		},
		{
			name: "task state is TaskStateSucceeded",
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateSucceeded)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateSucceeded))
			},
		},
		{
			name: "task state is TaskStateRunning",
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateRunning)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateFailed))
			},
		},
		{
			name: "task state is TaskStateFailed",
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateFailed)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateFailed))
			},
		},
		{
			name: "number of failed peers in the task is greater than FailedPeerCountLimit",
			mock: func(task *resource.Task) {
				task.FSM.SetState(resource.TaskStateFailed)
				task.PeerFailedCount.Store(201)
			},
			expect: func(t *testing.T, task *resource.Task) {
				assert := assert.New(t)
				assert.True(task.FSM.Is(resource.TaskStateFailed))
				assert.Equal(task.PeerFailedCount.Load(), int32(0))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig, Metrics: &config.MetricsConfig{EnablePeerHost: true}}, res, scheduler, dynconfig, storage)
			task := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))

			tc.mock(task)
			svc.handleTaskFail(context.Background(), task)
			tc.expect(t, task)
		})
	}
}
