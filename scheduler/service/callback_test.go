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
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	rpcschedulermocks "d7y.io/dragonfly/v2/pkg/rpc/scheduler/mocks"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler/mocks"
)

var (
	mockSchedulerConfig = &config.SchedulerConfig{
		RetryLimit:    3,
		RetryInterval: 10 * time.Millisecond,
	}
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
	mockCDNPeerID             = idgen.CDNPeerID("127.0.0.1")
)

func TestCallback_newCallback(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, c interface{})
	}{
		{
			name: "new callback",
			expect: func(t *testing.T, c interface{}) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(c).Elem().Name(), "callback")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			resource := resource.NewMockResource(ctl)

			tc.expect(t, newCallback(&config.Config{Scheduler: mockSchedulerConfig}, resource, scheduler))
		})
	}
}

func TestCallback_ScheduleParent(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(cancel context.CancelFunc, peer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, ms *mocks.MockSchedulerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name: "context was done",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, ms *mocks.MockSchedulerMockRecorder) {
				cancel()
			},
			expect: func(t *testing.T, peer *resource.Peer) {},
		},
		{
			name: "schedule parent",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, ms *mocks.MockSchedulerMockRecorder) {
				ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {},
		},
		{
			name: "reschedule parent",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, ms *mocks.MockSchedulerMockRecorder) {
				gomock.InOrder(
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, false).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {},
		},
		{
			name: "schedule parent failed and return error to client",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, ms *mocks.MockSchedulerMockRecorder) {
				peer.Task.BackToSourceLimit.Store(0)
				peer.StoreStream(stream)
				gomock.InOrder(
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, false).Times(3),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)

				dferr := <-peer.StopChannel
				assert.Equal(dferr.Code, base.Code_SchedTaskStatusError)
			},
		},
		{
			name: "schedule parent failed and load peer stream error",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, ms *mocks.MockSchedulerMockRecorder) {
				peer.Task.BackToSourceLimit.Store(0)
				gomock.InOrder(
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, false).Times(3),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {},
		},
		{
			name: "schedule parent failed and peer back-to-source",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, ms *mocks.MockSchedulerMockRecorder) {
				peer.Task.BackToSourceLimit.Store(1)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.FSM.SetState(resource.TaskStateFailed)
				peer.StoreStream(stream)
				gomock.InOrder(
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, false).Times(3),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)

				dferr := <-peer.StopChannel
				assert.Equal(dferr.Code, base.Code_SchedNeedBackSource)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
				assert.True(peer.Task.FSM.Is(resource.TaskStateRunning))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			stream := rpcschedulermocks.NewMockScheduler_ReportPieceResultServer(ctl)
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			ctx, cancel := context.WithCancel(context.Background())
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			callback := newCallback(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler)
			blocklist := set.NewSafeSet()

			tc.mock(cancel, peer, blocklist, stream, stream.EXPECT(), scheduler.EXPECT())
			callback.ScheduleParent(ctx, peer, blocklist)
			tc.expect(t, peer)
		})
	}
}

func TestCallback_BeginOfPiece(t *testing.T) {
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
				blocklist := set.NewSafeSet()
				blocklist.Add(peer.ID)
				scheduler.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1)
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
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			callback := newCallback(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler)

			tc.mock(peer, scheduler.EXPECT())
			callback.BeginOfPiece(context.Background(), peer)
			tc.expect(t, peer)
		})
	}
}

func TestCallback_PieceFail(t *testing.T) {
	mockHost := resource.NewHost(mockRawHost)
	mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)

	tests := []struct {
		name   string
		piece  *rpcscheduler.PieceResult
		peer   *resource.Peer
		parent *resource.Peer
		mock   func(peer *resource.Peer, parent *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, parent *resource.Peer)
	}{
		{
			name:   "peer state is PeerStateBackToSource",
			piece:  &rpcscheduler.PieceResult{},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, parent *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
			},
			expect: func(t *testing.T, peer *resource.Peer, parent *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
			},
		},
		{
			name: "piece result code is Code_ClientWaitPieceReady",
			piece: &rpcscheduler.PieceResult{
				Code: base.Code_ClientWaitPieceReady,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, parent *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
			},
			expect: func(t *testing.T, peer *resource.Peer, parent *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "piece result code is Code_ClientPieceDownloadFail and parent state set PeerEventDownloadFailed",
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_ClientPieceDownloadFail,
				DstPid: mockCDNPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, parent *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, parent *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStateFailed))
			},
		},
		{
			name: "piece result code is Code_PeerTaskNotFound and parent state set PeerEventDownloadFailed",
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_PeerTaskNotFound,
				DstPid: mockCDNPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, parent *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, parent *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStateFailed))
			},
		},
		{
			name: "piece result code is Code_CDNTaskNotFound and parent state set PeerEventDownloadFailed",
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_CDNTaskNotFound,
				DstPid: mockCDNPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, parent *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, parent *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStateFailed))
			},
		},
		{
			name: "piece result code is Code_CDNError and parent state set PeerEventDownloadFailed",
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_CDNError,
				DstPid: mockCDNPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, parent *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, parent *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStateFailed))
			},
		},
		{
			name: "piece result code is Code_CDNTaskDownloadFail and parent state set PeerEventDownloadFailed",
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_CDNTaskDownloadFail,
				DstPid: mockCDNPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, parent *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, parent *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStateFailed))
			},
		},
		{
			name: "piece result code is Code_ClientPieceRequestFail",
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_ClientPieceRequestFail,
				DstPid: mockCDNPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, parent *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, parent *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "piece result code is unknow",
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_ClientPieceRequestFail,
				DstPid: mockCDNPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, parent *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, parent *resource.Peer) {
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
			peerManager := resource.NewMockPeerManager(ctl)
			callback := newCallback(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler)

			tc.mock(tc.peer, tc.parent, peerManager, scheduler.EXPECT(), res.EXPECT(), peerManager.EXPECT())
			callback.PieceFail(context.Background(), tc.peer, tc.piece)
			tc.expect(t, tc.peer, tc.parent)
		})
	}
}
