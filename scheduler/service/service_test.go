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
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	configmocks "d7y.io/dragonfly/v2/scheduler/config/mocks"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
	"d7y.io/dragonfly/v2/scheduler/scheduler/mocks"
)

func TestService_New(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s interface{})
	}{
		{
			name: "new service",
			expect: func(t *testing.T, s interface{}) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "service")
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
			tc.expect(t, New(&config.Config{Scheduler: mockSchedulerConfig}, resource, scheduler, dynconfig))
		})
	}
}

func TestService_Scheduler(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s scheduler.Scheduler)
	}{
		{
			name: "get scheduler interface",
			expect: func(t *testing.T, s scheduler.Scheduler) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "MockScheduler")
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
			svc := New(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler, dynconfig)
			tc.expect(t, svc.Scheduler())
		})
	}
}

func TestService_CDN(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(cdn resource.CDN, mr *resource.MockResourceMockRecorder)
		expect func(t *testing.T, c resource.CDN)
	}{
		{
			name: "get cdn interface",
			mock: func(cdn resource.CDN, mr *resource.MockResourceMockRecorder) {
				mr.CDN().Return(cdn).Times(1)
			},
			expect: func(t *testing.T, c resource.CDN) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(c).Elem().Name(), "MockCDN")
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
			cdn := resource.NewMockCDN(ctl)
			svc := New(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler, dynconfig)
			tc.mock(cdn, res.EXPECT())
			tc.expect(t, svc.CDN())
		})
	}
}

func TestService_RegisterTask(t *testing.T) {
	tests := []struct {
		name string
		req  *rpcscheduler.PeerTaskRequest
		run  func(svc Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, cdn resource.CDN, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mc *resource.MockCDNMockRecorder)
	}{
		{
			name: "task already exists and state is TaskStateRunning",
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
			},
			run: func(svc Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, cdn resource.CDN, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateRunning)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
				)

				task, err := svc.RegisterTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(mockTask, task)
			},
		},
		{
			name: "task already exists and state is TaskStateSucceeded",
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
			},
			run: func(svc Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, cdn resource.CDN, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				mockTask.FSM.SetState(resource.TaskStateSucceeded)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
				)

				task, err := svc.RegisterTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(mockTask, task)
			},
		},
		{
			name: "task state is TaskStatePending",
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
			},
			run: func(svc Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, cdn resource.CDN, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(2)
				defer wg.Wait()

				mockTask.FSM.SetState(resource.TaskStatePending)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.CDN().Do(func() { wg.Done() }).Return(cdn).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, task *resource.Task) { wg.Done() }).Return(mockPeer, &rpcscheduler.PeerResult{}, nil).Times(1),
				)

				task, err := svc.RegisterTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(mockTask, task)
			},
		},
		{
			name: "task state is TaskStateFailed",
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
			},
			run: func(svc Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, cdn resource.CDN, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(2)
				defer wg.Wait()

				mockTask.FSM.SetState(resource.TaskStateFailed)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.CDN().Do(func() { wg.Done() }).Return(cdn).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, task *resource.Task) { wg.Done() }).Return(mockPeer, &rpcscheduler.PeerResult{}, nil).Times(1),
				)

				task, err := svc.RegisterTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(mockTask, task)
			},
		},
		{
			name: "task state is TaskStatePending, but trigger cdn failed",
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
			},
			run: func(svc Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, cdn resource.CDN, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(2)
				defer wg.Wait()

				mockTask.FSM.SetState(resource.TaskStatePending)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.CDN().Do(func() { wg.Done() }).Return(cdn).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, task *resource.Task) { wg.Done() }).Return(mockPeer, &rpcscheduler.PeerResult{}, errors.New("foo")).Times(1),
				)

				task, err := svc.RegisterTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(mockTask, task)
			},
		},
		{
			name: "task state is TaskStateFailed, but trigger cdn failed",
			req: &rpcscheduler.PeerTaskRequest{
				Url:     mockTaskURL,
				UrlMeta: mockTaskURLMeta,
			},
			run: func(svc Service, req *rpcscheduler.PeerTaskRequest, mockTask *resource.Task, mockPeer *resource.Peer, taskManager resource.TaskManager, cdn resource.CDN, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(2)
				defer wg.Wait()

				mockTask.FSM.SetState(resource.TaskStateFailed)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.LoadOrStore(gomock.Any()).Return(mockTask, true).Times(1),
					mr.CDN().Do(func() { wg.Done() }).Return(cdn).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, task *resource.Task) { wg.Done() }).Return(mockPeer, &rpcscheduler.PeerResult{}, errors.New("foo")).Times(1),
				)

				task, err := svc.RegisterTask(context.Background(), req)
				assert := assert.New(t)
				assert.NoError(err)
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
			svc := New(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler, dynconfig)
			taskManager := resource.NewMockTaskManager(ctl)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			cdn := resource.NewMockCDN(ctl)
			tc.run(svc, tc.req, mockTask, mockPeer, taskManager, cdn, res.EXPECT(), taskManager.EXPECT(), cdn.EXPECT())
		})
	}
}

func TestService_LoadOrStoreHost(t *testing.T) {
	tests := []struct {
		name   string
		req    *rpcscheduler.PeerTaskRequest
		mock   func(mockHost *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
		expect func(t *testing.T, host *resource.Host, loaded bool)
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
					mh.Load(gomock.Eq(mockRawHost.Uuid)).Return(mockHost, true).Times(1),
				)
			},
			expect: func(t *testing.T, host *resource.Host, loaded bool) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.Uuid)
				assert.True(loaded)
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
					mh.Load(gomock.Eq(mockRawHost.Uuid)).Return(nil, false).Times(1),
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{LoadLimit: 10}, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Store(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, host *resource.Host, loaded bool) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.Uuid)
				assert.Equal(host.UploadLoadLimit.Load(), int32(10))
				assert.False(loaded)
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
					mh.Load(gomock.Eq(mockRawHost.Uuid)).Return(nil, false).Times(1),
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{}, false).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Store(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, host *resource.Host, loaded bool) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.Uuid)
				assert.False(loaded)
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
			svc := New(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler, dynconfig)
			hostManager := resource.NewMockHostManager(ctl)
			mockHost := resource.NewHost(mockRawHost)

			tc.mock(mockHost, hostManager, res.EXPECT(), hostManager.EXPECT(), dynconfig.EXPECT())
			host, loaded := svc.LoadOrStoreHost(context.Background(), tc.req)
			tc.expect(t, host, loaded)
		})
	}
}

func TestService_LoadOrStorePeer(t *testing.T) {
	tests := []struct {
		name   string
		req    *rpcscheduler.PeerTaskRequest
		mock   func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, loaded bool)
	}{
		{
			name: "peer already exists",
			req: &rpcscheduler.PeerTaskRequest{
				PeerId: mockPeerID,
			},
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.True(loaded)
			},
		},
		{
			name: "peer does not exists",
			req: &rpcscheduler.PeerTaskRequest{
				PeerId: mockPeerID,
			},
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.LoadOrStore(gomock.Any()).Return(mockPeer, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.False(loaded)
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
			svc := New(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler, dynconfig)
			peerManager := resource.NewMockPeerManager(ctl)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)

			tc.mock(mockPeer, peerManager, res.EXPECT(), peerManager.EXPECT())
			peer, loaded := svc.LoadOrStorePeer(context.Background(), tc.req, mockTask, mockHost)
			tc.expect(t, peer, loaded)
		})
	}
}

func TestService_LoadPeer(t *testing.T) {
	tests := []struct {
		name   string
		req    *rpcscheduler.PeerTaskRequest
		mock   func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, ok bool)
	}{
		{
			name: "peer already exists",
			req: &rpcscheduler.PeerTaskRequest{
				PeerId: mockPeerID,
			},
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.Equal(ok, true)
			},
		},
		{
			name: "peer does not exists",
			req: &rpcscheduler.PeerTaskRequest{
				PeerId: mockPeerID,
			},
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeerID)).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, false)
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
			svc := New(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler, dynconfig)
			peerManager := resource.NewMockPeerManager(ctl)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)

			tc.mock(mockPeer, peerManager, res.EXPECT(), peerManager.EXPECT())
			peer, ok := svc.LoadPeer(mockPeerID)
			tc.expect(t, peer, ok)
		})
	}
}

func TestService_HandlePiece(t *testing.T) {
	tests := []struct {
		name   string
		piece  *rpcscheduler.PieceResult
		mock   func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name: "piece success and peer state is PeerStateReceivedNormal",
			piece: &rpcscheduler.PieceResult{
				DstPid: mockCDNPeerID,
				PieceInfo: &base.PieceInfo{
					PieceNum: 0,
				},
				BeginTime: uint64(time.Now().Unix()),
				EndTime:   uint64(time.Now().Add(1 * time.Second).Unix()),
				Success:   true,
			},
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockPeer.FSM.SetState(resource.PeerStateReceivedNormal)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockCDNPeerID)).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.Equal(peer.Pieces.Count(), uint(1))
				assert.Equal(peer.PieceCosts(), []int64{1})
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "piece success and peer state is PeerStateReceivedSmall",
			piece: &rpcscheduler.PieceResult{
				DstPid: mockCDNPeerID,
				PieceInfo: &base.PieceInfo{
					PieceNum: 0,
				},
				BeginTime: uint64(time.Now().Unix()),
				EndTime:   uint64(time.Now().Add(1 * time.Second).Unix()),
				Success:   true,
			},
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockPeer.FSM.SetState(resource.PeerStateReceivedSmall)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockCDNPeerID)).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.Equal(peer.Pieces.Count(), uint(1))
				assert.Equal(peer.PieceCosts(), []int64{1})
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "piece success and peer state is PeerStatePending",
			piece: &rpcscheduler.PieceResult{
				DstPid: mockCDNPeerID,
				PieceInfo: &base.PieceInfo{
					PieceNum: 0,
				},
				BeginTime: uint64(time.Now().Unix()),
				EndTime:   uint64(time.Now().Add(1 * time.Second).Unix()),
				Success:   true,
			},
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockPeer.FSM.SetState(resource.PeerStatePending)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockCDNPeerID)).Return(mockPeer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.True(peer.FSM.Is(resource.PeerStatePending))
			},
		},
		{
			name: "piece success and load peer failed",
			piece: &rpcscheduler.PieceResult{
				DstPid: mockCDNPeerID,
				PieceInfo: &base.PieceInfo{
					PieceNum: 0,
				},
				BeginTime: uint64(time.Now().Unix()),
				EndTime:   uint64(time.Now().Add(1 * time.Second).Unix()),
				Success:   true,
			},
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockCDNPeerID)).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.Equal(peer.Pieces.Count(), uint(1))
				assert.Equal(peer.PieceCosts(), []int64{1})
			},
		},
		{
			name: "receive begin of piece and peer state is PeerStateBackToSource",
			piece: &rpcscheduler.PieceResult{
				PieceInfo: &base.PieceInfo{
					PieceNum: common.BeginOfPiece,
				},
			},
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockPeer.FSM.SetState(resource.PeerStateBackToSource)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
			},
		},
		{
			name: "receive code is Code_ClientWaitPieceReady",
			piece: &rpcscheduler.PieceResult{
				Code: base.Code_ClientWaitPieceReady,
			},
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockPeer.FSM.SetState(resource.PeerStateBackToSource)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
			},
		},
		{
			name: "receive failed piece",
			piece: &rpcscheduler.PieceResult{
				Code: base.Code_CDNError,
			},
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockPeer.FSM.SetState(resource.PeerStateBackToSource)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
			},
		},
		{
			name: "receive unknow piece",
			piece: &rpcscheduler.PieceResult{
				Code: base.Code_Success,
			},
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.True(peer.FSM.Is(resource.PeerStatePending))
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
			svc := New(&config.Config{Scheduler: mockSchedulerConfig, Metrics: &config.MetricsConfig{EnablePeerHost: true}}, res, scheduler, dynconfig)
			peerManager := resource.NewMockPeerManager(ctl)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)

			tc.mock(mockPeer, peerManager, res.EXPECT(), peerManager.EXPECT())
			svc.HandlePiece(context.Background(), mockPeer, tc.piece)
			tc.expect(t, mockPeer)
		})
	}
}

func TestService_HandlePeer(t *testing.T) {
	tests := []struct {
		name   string
		result *rpcscheduler.PeerResult
		mock   func(mockPeer *resource.Peer)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name: "peer failed",
			result: &rpcscheduler.PeerResult{
				Success: false,
			},
			mock: func(mockPeer *resource.Peer) {
				mockPeer.FSM.SetState(resource.PeerStateRunning)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.True(peer.FSM.Is(resource.PeerStateFailed))
			},
		},
		{
			name: "peer back-to-source failed",
			result: &rpcscheduler.PeerResult{
				Success: false,
			},
			mock: func(mockPeer *resource.Peer) {
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.Task.FSM.SetState(resource.TaskStateRunning)
				mockPeer.Task.BackToSourcePeers.Add(mockPeer)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.True(peer.FSM.Is(resource.PeerStateFailed))
				assert.True(peer.Task.FSM.Is(resource.TaskStateFailed))
			},
		},
		{
			name: "peer success",
			result: &rpcscheduler.PeerResult{
				Success: true,
			},
			mock: func(mockPeer *resource.Peer) {
				mockPeer.FSM.SetState(resource.PeerStateRunning)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
			},
		},
		{
			name: "peer back-to-source success",
			result: &rpcscheduler.PeerResult{
				Success: true,
			},
			mock: func(mockPeer *resource.Peer) {
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.Task.FSM.SetState(resource.TaskStateRunning)
				mockPeer.Task.BackToSourcePeers.Add(mockPeer)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.True(peer.FSM.Is(resource.PeerStateSucceeded))
				assert.True(peer.Task.FSM.Is(resource.TaskStateSucceeded))
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
			svc := New(&config.Config{Scheduler: mockSchedulerConfig, Metrics: &config.MetricsConfig{EnablePeerHost: true}}, res, scheduler, dynconfig)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)

			tc.mock(mockPeer)
			svc.HandlePeer(context.Background(), mockPeer, tc.result)
			tc.expect(t, mockPeer)
		})
	}
}

func TestService_HandlePeerLeave(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name: "peer leave",
			mock: func(mockPeer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				mockPeer.FSM.SetState(resource.PeerStateSucceeded)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Eq(mockPeerID)).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
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
			svc := New(&config.Config{Scheduler: mockSchedulerConfig, Metrics: &config.MetricsConfig{EnablePeerHost: true}}, res, scheduler, dynconfig)
			peerManager := resource.NewMockPeerManager(ctl)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			mockPeer := resource.NewPeer(mockPeerID, mockTask, mockHost)

			tc.mock(mockPeer, peerManager, res.EXPECT(), peerManager.EXPECT())
			svc.HandlePeerLeave(context.Background(), mockPeer)
			tc.expect(t, mockPeer)
		})
	}
}
