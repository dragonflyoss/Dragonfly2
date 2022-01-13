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
		RetryLimit:      3,
		BackSourceCount: mockTaskBackToSourceLimit,
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
					mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError})).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
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
					mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedNeedBackSource})).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
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

func TestCallback_PieceSuccess(t *testing.T) {
	mockHost := resource.NewHost(mockRawHost)
	mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)

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
				BeginTime: uint64(time.Now().Unix()),
				EndTime:   uint64(time.Now().Add(1 * time.Second).Unix()),
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
				BeginTime: uint64(time.Now().Unix()),
				EndTime:   uint64(time.Now().Add(1 * time.Second).Unix()),
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
			callback := newCallback(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler)

			tc.mock(tc.peer)
			callback.PieceSuccess(context.Background(), tc.peer, tc.piece)
			tc.expect(t, tc.peer)
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
		run    func(t *testing.T, callback Callback, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, cdn resource.CDN, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockCDNMockRecorder)
	}{
		{
			name:   "peer state is PeerStateBackToSource",
			piece:  &rpcscheduler.PieceResult{},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			run: func(t *testing.T, callback Callback, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, cdn resource.CDN, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				peer.FSM.SetState(resource.PeerStateBackToSource)

				callback.PieceFail(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
			},
		},
		{
			name: "piece result code is Code_ClientWaitPieceReady",
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_ClientWaitPieceReady,
				DstPid: mockCDNPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			run: func(t *testing.T, callback Callback, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, cdn resource.CDN, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
				)

				callback.PieceFail(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "can not found parent",
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_ClientWaitPieceReady,
				DstPid: mockCDNPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			run: func(t *testing.T, callback Callback, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, cdn resource.CDN, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(nil, false).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(set.NewSafeSet())).Return(nil, true).Times(1),
				)

				callback.PieceFail(context.Background(), peer, piece)
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
			run: func(t *testing.T, callback Callback, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, cdn resource.CDN, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)

				callback.PieceFail(context.Background(), peer, piece)
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
			run: func(t *testing.T, callback Callback, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, cdn resource.CDN, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)

				callback.PieceFail(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStateFailed))
			},
		},
		{
			name: "piece result code is Code_CDNTaskNotFound",
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_CDNTaskNotFound,
				DstPid: mockCDNPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			run: func(t *testing.T, callback Callback, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, cdn resource.CDN, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateSucceeded)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				var wg sync.WaitGroup
				wg.Add(2)
				defer wg.Wait()

				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
					mr.CDN().Do(func() { wg.Done() }).Return(cdn).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, task *resource.Task) { wg.Done() }).Return(parent, &rpcscheduler.PeerResult{}, nil).Times(1),
				)

				callback.PieceFail(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStatePending))
			},
		},
		{
			name: "piece result code is Code_ClientPieceNotFound and parent is CDN",
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_ClientPieceNotFound,
				DstPid: mockCDNPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			run: func(t *testing.T, callback Callback, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, cdn resource.CDN, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateSucceeded)
				peer.Host.IsCDN = true
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				var wg sync.WaitGroup
				wg.Add(2)
				defer wg.Wait()

				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
					mr.CDN().Do(func() { wg.Done() }).Return(cdn).Times(1),
					mc.TriggerTask(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, task *resource.Task) { wg.Done() }).Return(parent, &rpcscheduler.PeerResult{}, nil).Times(1),
				)

				callback.PieceFail(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
				assert.True(parent.FSM.Is(resource.PeerStatePending))
			},
		},
		{
			name: "piece result code is Code_ClientPieceNotFound and parent is not CDN",
			piece: &rpcscheduler.PieceResult{
				Code:   base.Code_ClientPieceNotFound,
				DstPid: mockCDNPeerID,
			},
			peer:   resource.NewPeer(mockPeerID, mockTask, mockHost),
			parent: resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			run: func(t *testing.T, callback Callback, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, cdn resource.CDN, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Host.IsCDN = false
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)

				callback.PieceFail(context.Background(), peer, piece)
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
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
			run: func(t *testing.T, callback Callback, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, cdn resource.CDN, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)

				callback.PieceFail(context.Background(), peer, piece)
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
			run: func(t *testing.T, callback Callback, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, cdn resource.CDN, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)

				callback.PieceFail(context.Background(), peer, piece)
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
			run: func(t *testing.T, callback Callback, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, cdn resource.CDN, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)

				callback.PieceFail(context.Background(), peer, piece)
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
			run: func(t *testing.T, callback Callback, peer *resource.Peer, parent *resource.Peer, piece *rpcscheduler.PieceResult, peerManager resource.PeerManager, cdn resource.CDN, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, mc *resource.MockCDNMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				parent.FSM.SetState(resource.PeerStateRunning)
				blocklist := set.NewSafeSet()
				blocklist.Add(parent.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(parent.ID)).Return(parent, true).Times(1),
					ms.ScheduleParent(gomock.Any(), gomock.Eq(peer), gomock.Eq(blocklist)).Return(nil, true).Times(1),
				)

				callback.PieceFail(context.Background(), peer, piece)
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
			cdn := resource.NewMockCDN(ctl)
			callback := newCallback(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler)

			tc.run(t, callback, tc.peer, tc.parent, tc.piece, peerManager, cdn, scheduler.EXPECT(), res.EXPECT(), peerManager.EXPECT(), cdn.EXPECT())
		})
	}
}

func TestCallback_PeerSuccess(t *testing.T) {
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
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(peer.Task.DirectPiece, []byte{1})
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
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			callback := newCallback(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler)

			tc.mock(peer)
			callback.PeerSuccess(context.Background(), peer)
			tc.expect(t, peer)
		})
	}
}

func TestCallback_PeerFail(t *testing.T) {
	mockHost := resource.NewHost(mockRawHost)
	mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)

	tests := []struct {
		name   string
		peer   *resource.Peer
		child  *resource.Peer
		mock   func(peer *resource.Peer, child *resource.Peer, ms *mocks.MockSchedulerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, child *resource.Peer)
	}{
		{
			name:  "peer state is PeerStateFailed",
			peer:  resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
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
			peer:  resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
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
			peer:  resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, ms *mocks.MockSchedulerMockRecorder) {
				peer.StoreChild(child)
				peer.FSM.SetState(resource.PeerStateRunning)
				child.FSM.SetState(resource.PeerStateRunning)

				blocklist := set.NewSafeSet()
				blocklist.Add(peer.ID)
				ms.ScheduleParent(gomock.Any(), gomock.Eq(child), gomock.Eq(blocklist)).Return(nil, true).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, child *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateFailed))
			},
		},
		{
			name:  "peer state is PeerStateRunning and it has no children",
			peer:  resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
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
			callback := newCallback(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler)

			tc.mock(tc.peer, tc.child, scheduler.EXPECT())
			callback.PeerFail(context.Background(), tc.peer)
			tc.expect(t, tc.peer, tc.child)
		})
	}
}

func TestCallback_PeerLeave(t *testing.T) {
	mockHost := resource.NewHost(mockRawHost)
	mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)

	tests := []struct {
		name   string
		peer   *resource.Peer
		child  *resource.Peer
		mock   func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name:  "peer state is PeerStatePending",
			peer:  resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStatePending)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStatePending))
			},
		},
		{
			name:  "peer state is PeerStateReceivedSmall",
			peer:  resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedSmall)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedSmall))
			},
		},
		{
			name:  "peer state is PeerStateReceivedNormal",
			peer:  resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedNormal)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateReceivedNormal))
			},
		},
		{
			name:  "peer state is PeerStateRunning",
			peer:  resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name:  "peer state is PeerStateBackToSource",
			peer:  resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
			},
		},
		{
			name:  "peer state is PeerStateLeave",
			peer:  resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateLeave)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name:  "peer state is PeerStateSucceeded and children need to be scheduled",
			peer:  resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.StoreChild(child)
				peer.FSM.SetState(resource.PeerStateSucceeded)
				child.FSM.SetState(resource.PeerStateRunning)

				blocklist := set.NewSafeSet()
				blocklist.Add(peer.ID)
				gomock.InOrder(
					ms.ScheduleParent(gomock.Any(), gomock.Eq(child), gomock.Eq(blocklist)).Return(nil, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Eq(peer.ID)).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name:  "peer state is PeerStateSucceeded and it has no children",
			peer:  resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateSucceeded)

				blocklist := set.NewSafeSet()
				blocklist.Add(peer.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Eq(peer.ID)).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name:  "peer state is PeerStateFailed and children need to be scheduled",
			peer:  resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.StoreChild(child)
				peer.FSM.SetState(resource.PeerStateFailed)
				child.FSM.SetState(resource.PeerStateRunning)

				blocklist := set.NewSafeSet()
				blocklist.Add(peer.ID)
				gomock.InOrder(
					ms.ScheduleParent(gomock.Any(), gomock.Eq(child), gomock.Eq(blocklist)).Return(nil, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Eq(peer.ID)).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateLeave))
			},
		},
		{
			name:  "peer state is PeerStateFailed and it has no children",
			peer:  resource.NewPeer(mockCDNPeerID, mockTask, mockHost),
			child: resource.NewPeer(mockPeerID, mockTask, mockHost),
			mock: func(peer *resource.Peer, child *resource.Peer, peerManager resource.PeerManager, ms *mocks.MockSchedulerMockRecorder, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateFailed)

				blocklist := set.NewSafeSet()
				blocklist.Add(peer.ID)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Delete(gomock.Eq(peer.ID)).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
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
			peerManager := resource.NewMockPeerManager(ctl)
			callback := newCallback(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler)

			tc.mock(tc.peer, tc.child, peerManager, scheduler.EXPECT(), res.EXPECT(), peerManager.EXPECT())
			callback.PeerLeave(context.Background(), tc.peer)
			tc.expect(t, tc.peer)
		})
	}
}

func TestCallback_TaskSuccess(t *testing.T) {
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
			callback := newCallback(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler)
			task := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)

			tc.mock(task)
			callback.TaskSuccess(context.Background(), task, tc.result)
			tc.expect(t, task)
		})
	}
}

func TestCallback_TaskFail(t *testing.T) {
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduler := mocks.NewMockScheduler(ctl)
			res := resource.NewMockResource(ctl)
			callback := newCallback(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduler)
			task := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)

			tc.mock(task)
			callback.TaskFail(context.Background(), task)
			tc.expect(t, task)
		})
	}
}
