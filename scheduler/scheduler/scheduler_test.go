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

package scheduler

import (
	"context"
	"errors"
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
	"d7y.io/dragonfly/v2/scheduler/scheduler/evaluator"
)

var (
	mockPluginDir       = "plugin_dir"
	mockSchedulerConfig = &config.SchedulerConfig{
		RetryLimit:           2,
		RetryBackSourceLimit: 1,
		RetryInterval:        10 * time.Millisecond,
		BackSourceCount:      mockTaskBackToSourceLimit,
		Algorithm:            evaluator.DefaultAlgorithm,
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
	mockRawCDNHost = &rpcscheduler.PeerHost{
		Uuid:           idgen.CDNHostID("hostname", 8003),
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

func TestScheduler_New(t *testing.T) {
	tests := []struct {
		name      string
		pluginDir string
		expect    func(t *testing.T, s interface{})
	}{
		{
			name:      "new scheduler",
			pluginDir: "bar",
			expect: func(t *testing.T, s interface{}) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "scheduler")
			},
		},
		{
			name:      "new scheduler with empty pluginDir",
			pluginDir: "",
			expect: func(t *testing.T, s interface{}) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "scheduler")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, New(mockSchedulerConfig, tc.pluginDir))
		})
	}
}

func TestCallback_ScheduleParent(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name: "context was done",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				cancel()
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "cdn peer state is PeerStateFailed and peer stream load failed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				task.StorePeer(cdnPeer)
				peer.FSM.SetState(resource.PeerStateRunning)
				cdnPeer.FSM.SetState(resource.PeerStateFailed)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "cdn peer state is PeerStateFailed and send Code_SchedNeedBackSource code failed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				task.StorePeer(cdnPeer)
				peer.FSM.SetState(resource.PeerStateRunning)
				cdnPeer.FSM.SetState(resource.PeerStateFailed)
				peer.StoreParent(cdnPeer)
				peer.StoreStream(stream)

				mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedNeedBackSource})).Return(errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.True(ok)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "cdn peer state is PeerStateFailed and send Code_SchedNeedBackSource code success",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				task.StorePeer(cdnPeer)
				cdnPeer.FSM.SetState(resource.PeerStateFailed)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.StoreParent(cdnPeer)
				peer.StoreStream(stream)

				mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedNeedBackSource})).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
				assert.True(peer.Task.FSM.Is(resource.TaskStatePending))
			},
		},
		{
			name: "cdn peer state is PeerStateFailed and task state is PeerStateFailed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				task.StorePeer(cdnPeer)
				cdnPeer.FSM.SetState(resource.PeerStateFailed)
				peer.FSM.SetState(resource.PeerStateRunning)
				task.FSM.SetState(resource.TaskStateFailed)
				peer.StoreParent(cdnPeer)
				peer.StoreStream(stream)

				mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedNeedBackSource})).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
				assert.True(peer.Task.FSM.Is(resource.TaskStateRunning))
			},
		},
		{
			name: "cdn peer state is PeerStateFailed and task state is PeerStateFailed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				task.StorePeer(cdnPeer)
				peer.FSM.SetState(resource.PeerStateRunning)
				cdnPeer.FSM.SetState(resource.PeerStateFailed)
				task.FSM.SetState(resource.TaskStateFailed)
				peer.StoreParent(cdnPeer)
				peer.StoreStream(stream)

				mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedNeedBackSource})).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
				assert.True(peer.Task.FSM.Is(resource.TaskStateRunning))
			},
		},
		{
			name: "schedule exceeds RetryBackSourceLimit and peer stream load failed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "cdn peer state is PeerStateFailed and send Code_SchedNeedBackSource code failed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.StoreParent(cdnPeer)
				peer.StoreStream(stream)

				mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedNeedBackSource})).Return(errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.True(ok)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "cdn peer state is PeerStateFailed and send Code_SchedNeedBackSource code success",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.StoreParent(cdnPeer)
				peer.StoreStream(stream)

				mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedNeedBackSource})).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
				assert.True(peer.Task.FSM.Is(resource.TaskStatePending))
			},
		},
		{
			name: "cdn peer state is PeerStateFailed and task state is PeerStateFailed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				task.FSM.SetState(resource.TaskStateFailed)
				peer.StoreParent(cdnPeer)
				peer.StoreStream(stream)

				mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedNeedBackSource})).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
				assert.True(peer.Task.FSM.Is(resource.TaskStateRunning))
			},
		},
		{
			name: "cdn peer state is PeerStateFailed and task state is PeerStateFailed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				task.FSM.SetState(resource.TaskStateFailed)
				peer.StoreParent(cdnPeer)
				peer.StoreStream(stream)

				mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedNeedBackSource})).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
				assert.True(peer.Task.FSM.Is(resource.TaskStateRunning))
			},
		},
		{
			name: "schedule exceeds RetryLimit and peer stream load failed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.BackToSourceLimit.Store(0)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "schedule exceeds RetryLimit and send Code_SchedTaskStatusError code failed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.BackToSourceLimit.Store(0)
				peer.StoreStream(stream)

				mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError})).Return(errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "schedule exceeds RetryLimit and send Code_SchedTaskStatusError code success",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.BackToSourceLimit.Store(0)
				peer.StoreStream(stream)

				mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError})).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "schedule succeeded",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, cdnPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				task.StorePeer(cdnPeer)
				peer.FSM.SetState(resource.PeerStateRunning)
				cdnPeer.FSM.SetState(resource.PeerStateRunning)
				peer.StoreStream(stream)
				mr.Send(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.True(ok)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			stream := rpcschedulermocks.NewMockScheduler_ReportPieceResultServer(ctl)
			ctx, cancel := context.WithCancel(context.Background())
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			mockCDNHost := resource.NewHost(mockRawCDNHost, resource.WithIsCDN(true))
			cdnPeer := resource.NewPeer(mockCDNPeerID, mockTask, mockCDNHost)
			blocklist := set.NewSafeSet()

			tc.mock(cancel, peer, cdnPeer, blocklist, stream, stream.EXPECT())
			scheduler := New(mockSchedulerConfig, mockPluginDir)
			scheduler.ScheduleParent(ctx, peer, blocklist)
			tc.expect(t, peer)
		})
	}
}

func TestScheduler_NotifyAndFindParent(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder)
		expect func(t *testing.T, parents []*resource.Peer, ok bool)
	}{
		{
			name: "peer state is PeerStatePending",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStatePending)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateReceivedSmall",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedSmall)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateReceivedNormal",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedNormal)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateBackToSource",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateSucceeded",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateSucceeded)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateFailed",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateFailed)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateLeave",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateLeave)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "task peers is empty",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "task contains only one peer and peer is itself",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer is in blocklist",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				blocklist.Add(mockPeer.ID)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer is bad node",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.FSM.SetState(resource.PeerStateFailed)
				peer.Task.StorePeer(mockPeer)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "parent is peer's descendant",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				peer.StoreChild(mockPeer)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "parent is peer's ancestor",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				mockPeer.StoreChild(peer)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "parent free upload load is zero",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				mockPeer.Host.UploadLoadLimit.Store(0)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer stream is empty",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				mockPeer.Pieces.Set(0)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer stream send failed",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				mockPeer.Pieces.Set(0)
				peer.StoreStream(stream)
				ms.Send(gomock.Eq(constructSuccessPeerPacket(peer, mockPeer, []*resource.Peer{}))).Return(errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "schedule parent",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				mockPeer.Pieces.Set(0)
				peer.StoreStream(stream)
				ms.Send(gomock.Eq(constructSuccessPeerPacket(peer, mockPeer, []*resource.Peer{}))).Return(nil).Times(1)
			},
			expect: func(t *testing.T, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(len(parents), 1)
				assert.True(ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			stream := rpcschedulermocks.NewMockScheduler_ReportPieceResultServer(ctl)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			mockPeer := resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost)
			blocklist := set.NewSafeSet()

			tc.mock(peer, mockPeer, blocklist, stream, stream.EXPECT())
			scheduler := New(mockSchedulerConfig, mockPluginDir)
			parents, ok := scheduler.NotifyAndFindParent(context.Background(), peer, blocklist)
			tc.expect(t, parents, ok)
		})
	}
}

func TestScheduler_FindParent(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet)
		expect func(t *testing.T, parent *resource.Peer, ok bool)
	}{
		{
			name: "task peers is empty",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet) {
				peer.FSM.SetState(resource.PeerStateRunning)
			},
			expect: func(t *testing.T, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "task contains only one peer and peer is itself",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
			},
			expect: func(t *testing.T, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer is in blocklist",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				blocklist.Add(mockPeer.ID)
			},
			expect: func(t *testing.T, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer is bad node",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateFailed)
				peer.Task.StorePeer(mockPeer)
			},
			expect: func(t *testing.T, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "parent is peer's descendant",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				peer.StoreChild(mockPeer)
			},
			expect: func(t *testing.T, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "parent free upload load is zero",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				mockPeer.Host.UploadLoadLimit.Store(0)
			},
			expect: func(t *testing.T, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "find parent",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				mockPeer.Pieces.Set(0)
			},
			expect: func(t *testing.T, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.True(ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			mockPeer := resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost)
			blocklist := set.NewSafeSet()

			tc.mock(peer, mockPeer, blocklist)
			scheduler := New(mockSchedulerConfig, mockPluginDir)
			parent, ok := scheduler.FindParent(context.Background(), peer, blocklist)
			tc.expect(t, parent, ok)
		})
	}
}
