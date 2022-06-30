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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	rpcschedulermocks "d7y.io/dragonfly/v2/pkg/rpc/scheduler/mocks"
	"d7y.io/dragonfly/v2/scheduler/config"
	configmocks "d7y.io/dragonfly/v2/scheduler/config/mocks"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler/evaluator"
)

var (
	mockPluginDir       = "plugin_dir"
	mockSchedulerConfig = &config.SchedulerConfig{
		RetryLimit:           2,
		RetryBackSourceLimit: 1,
		RetryInterval:        10 * time.Millisecond,
		BackSourceCount:      int(mockTaskBackToSourceLimit),
		Algorithm:            evaluator.DefaultAlgorithm,
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

	mockTaskBackToSourceLimit int32 = 200
	mockTaskURL                     = "http://example.com/foo"
	mockTaskID                      = idgen.TaskID(mockTaskURL, mockTaskURLMeta)
	mockPeerID                      = idgen.PeerID("127.0.0.1")
	mockSeedPeerID                  = idgen.SeedPeerID("127.0.0.1")
)

func TestScheduler_New(t *testing.T) {
	tests := []struct {
		name      string
		pluginDir string
		expect    func(t *testing.T, s any)
	}{
		{
			name:      "new scheduler",
			pluginDir: "bar",
			expect: func(t *testing.T, s any) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "scheduler")
			},
		},
		{
			name:      "new scheduler with empty pluginDir",
			pluginDir: "",
			expect: func(t *testing.T, s any) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "scheduler")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			tc.expect(t, New(mockSchedulerConfig, dynconfig, tc.pluginDir))
		})
	}
}

func TestScheduler_ScheduleParent(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name: "context was done",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				cancel()
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "peer needs back-to-source and peer stream load failed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.NeedBackToSource.Store(true)
				peer.FSM.SetState(resource.PeerStateRunning)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "peer needs back-to-source and send Code_SchedNeedBackSource code failed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.NeedBackToSource.Store(true)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.StoreStream(stream)

				mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedNeedBackSource})).Return(errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				_, ok := peer.LoadParent()
				assert.False(ok)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "peer needs back-to-source and send Code_SchedNeedBackSource code success",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.NeedBackToSource.Store(true)
				peer.FSM.SetState(resource.PeerStateRunning)
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
			name: "peer needs back-to-source and task state is TaskStateFailed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.NeedBackToSource.Store(true)
				peer.FSM.SetState(resource.PeerStateRunning)
				task.FSM.SetState(resource.TaskStateFailed)
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
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "schedule exceeds RetryLimit and peer stream load failed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.BackToSourceLimit.Store(0)
				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(2)
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
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.BackToSourceLimit.Store(0)
				peer.StoreStream(stream)

				gomock.InOrder(
					md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(2),
					mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError})).Return(errors.New("foo")).Times(1),
				)
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
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.BackToSourceLimit.Store(0)
				peer.StoreStream(stream)

				gomock.InOrder(
					md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(2),
					mr.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError})).Return(nil).Times(1),
				)
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
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, mr *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				task.StorePeer(seedPeer)
				peer.FSM.SetState(resource.PeerStateRunning)
				seedPeer.FSM.SetState(resource.PeerStateRunning)
				peer.StoreStream(stream)
				gomock.InOrder(
					md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1),
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{
						ParallelCount: 2,
					}, true).Times(1),
					mr.Send(gomock.Any()).Return(nil).Times(1),
				)
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
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			ctx, cancel := context.WithCancel(context.Background())
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			mockSeedHost := resource.NewHost(mockRawSeedHost, resource.WithHostType(resource.HostTypeSuperSeed))
			seedPeer := resource.NewPeer(mockSeedPeerID, mockTask, mockSeedHost)
			blocklist := set.NewSafeSet()

			tc.mock(cancel, peer, seedPeer, blocklist, stream, stream.EXPECT(), dynconfig.EXPECT())
			scheduler := New(mockSchedulerConfig, dynconfig, mockPluginDir)
			scheduler.ScheduleParent(ctx, peer, blocklist)
			tc.expect(t, peer)
		})
	}
}

func TestScheduler_NotifyAndFindParent(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool)
	}{
		{
			name: "peer state is PeerStatePending",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStatePending)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateReceivedSmall",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedSmall)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateReceivedNormal",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedNormal)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateBackToSource",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateSucceeded",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateSucceeded)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateFailed",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateFailed)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateLeave",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateLeave)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "task peers is empty",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "task contains only one peer and peer is itself",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer is in blocklist",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				blocklist.Add(mockPeer.ID)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer is in steal peers",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				peer.StealPeers.Add(mockPeer.ID)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer is bad node",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.FSM.SetState(resource.PeerStateFailed)
				peer.Task.StorePeer(mockPeer)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "parent is peer's descendant",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				peer.StoreChild(mockPeer)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "parent is peer's ancestor",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				mockPeer.StoreChild(peer)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "parent free upload load is zero",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				mockPeer.Host.UploadLoadLimit.Store(0)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer stream is empty",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				mockPeer.Pieces.Set(0)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer stream send failed",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.BackToSourcePeers.Add(mockPeer)
				mockPeer.IsBackToSource.Store(true)
				peer.Task.StorePeer(mockPeer)
				mockPeer.Pieces.Set(0)
				peer.StoreStream(stream)
				gomock.InOrder(
					md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1),
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{
						ParallelCount: 2,
					}, true).Times(1),
					ms.Send(gomock.Any()).Return(errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "schedule parent",
			mock: func(peer *resource.Peer, mockPeer *resource.Peer, blocklist set.SafeSet, stream rpcscheduler.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				mockHost := resource.NewHost(mockRawHost)
				mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
				stealPeer := resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost)
				stealPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeer)
				peer.Task.StorePeer(stealPeer)
				peer.Task.BackToSourcePeers.Add(mockPeer)
				peer.Task.BackToSourcePeers.Add(stealPeer)
				mockPeer.IsBackToSource.Store(true)
				stealPeer.IsBackToSource.Store(true)
				mockPeer.Pieces.Set(0)
				peer.StoreStream(stream)
				gomock.InOrder(
					md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1),
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{
						ParallelCount: 2,
					}, true).Times(1),
					ms.Send(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(len(parents), 2)
				assert.Equal(peer.StealPeers.Len(), uint(1))
				assert.True(ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			stream := rpcschedulermocks.NewMockScheduler_ReportPieceResultServer(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			mockPeer := resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost)
			blocklist := set.NewSafeSet()

			tc.mock(peer, mockPeer, blocklist, stream, dynconfig, stream.EXPECT(), dynconfig.EXPECT())
			scheduler := New(mockSchedulerConfig, dynconfig, mockPluginDir)
			parents, ok := scheduler.NotifyAndFindParent(context.Background(), peer, blocklist)
			tc.expect(t, peer, parents, ok)
		})
	}
}

func TestScheduler_FindParent(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet, md *configmocks.MockDynconfigInterfaceMockRecorder)
		expect func(t *testing.T, mockPeers []*resource.Peer, parent *resource.Peer, ok bool)
	}{
		{
			name: "task peers is empty",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "task contains only one peer and peer is itself",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer is in blocklist",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeers[0])
				blocklist.Add(mockPeers[0].ID)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer is bad node",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateFailed)
				peer.Task.StorePeer(mockPeers[0])

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "parent is peer's descendant",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeers[0])
				peer.StoreChild(mockPeers[0])

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "parent free upload load is zero",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeers[0])
				mockPeers[0].Host.UploadLoadLimit.Store(0)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "find back-to-source parent",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateRunning)
				mockPeers[1].FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeers[0])
				peer.Task.StorePeer(mockPeers[1])
				peer.Task.BackToSourcePeers.Add(mockPeers[0])
				peer.Task.BackToSourcePeers.Add(mockPeers[1])
				mockPeers[0].IsBackToSource.Store(true)
				mockPeers[1].IsBackToSource.Store(true)
				mockPeers[0].Pieces.Set(0)
				mockPeers[1].Pieces.Set(0)
				mockPeers[1].Pieces.Set(1)
				mockPeers[1].Pieces.Set(2)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.True(ok)
				assert.Equal(mockPeers[1].ID, parent.ID)
			},
		},
		{
			name: "find seed peer parent",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateRunning)
				mockPeers[1].FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeers[0])
				peer.Task.StorePeer(mockPeers[1])
				mockPeers[0].Host.Type = resource.HostTypeSuperSeed
				mockPeers[1].Host.Type = resource.HostTypeSuperSeed
				mockPeers[0].Pieces.Set(0)
				mockPeers[1].Pieces.Set(0)
				mockPeers[1].Pieces.Set(1)
				mockPeers[1].Pieces.Set(2)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.True(ok)
				assert.Equal(mockPeers[1].ID, parent.ID)
			},
		},
		{
			name: "parent state is PeerStateSucceeded",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateSucceeded)
				mockPeers[1].FSM.SetState(resource.PeerStateSucceeded)
				peer.Task.StorePeer(mockPeers[0])
				peer.Task.StorePeer(mockPeers[1])
				mockPeers[0].Pieces.Set(0)
				mockPeers[1].Pieces.Set(0)
				mockPeers[1].Pieces.Set(1)
				mockPeers[1].Pieces.Set(2)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.True(ok)
				assert.Equal(mockPeers[1].ID, parent.ID)
			},
		},
		{
			name: "find parent with ancestor",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateRunning)
				mockPeers[1].FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeers[0])
				peer.Task.StorePeer(mockPeers[1])
				mockPeers[0].StoreParent(mockPeers[3])
				mockPeers[1].StoreParent(mockPeers[3])
				mockPeers[0].Pieces.Set(0)
				mockPeers[1].Pieces.Set(0)
				mockPeers[1].Pieces.Set(1)
				mockPeers[1].Pieces.Set(2)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.True(ok)
				assert.Equal(mockPeers[1].ID, parent.ID)
			},
		},
		{
			name: "find parent and fetch filterParentLimit from manager dynconfig",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateRunning)
				mockPeers[1].FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(mockPeers[0])
				peer.Task.StorePeer(mockPeers[1])
				peer.Task.BackToSourcePeers.Add(mockPeers[0])
				peer.Task.BackToSourcePeers.Add(mockPeers[1])
				mockPeers[0].IsBackToSource.Store(true)
				mockPeers[1].IsBackToSource.Store(true)
				mockPeers[0].Pieces.Set(0)
				mockPeers[1].Pieces.Set(0)
				mockPeers[1].Pieces.Set(1)
				mockPeers[1].Pieces.Set(2)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{
					FilterParentLimit: 1,
				}, true).Times(1)
			},
			expect: func(t *testing.T, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.True(ok)
				assert.Contains([]string{mockPeers[0].ID, mockPeers[1].ID}, parent.ID)
			},
		},
		{
			name: "exceeds the depth limit of the tree",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateRunning)
				for i := 0; i < 10; i++ {
					mockPeers[i].StoreParent(mockPeers[i+1])
				}
				peer.Task.StorePeer(mockPeers[0])

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)

			var mockPeers []*resource.Peer
			for i := 0; i < 11; i++ {
				peer := resource.NewPeer(idgen.PeerID(fmt.Sprintf("127.0.0.%d", i)), mockTask, mockHost)
				mockPeers = append(mockPeers, peer)
			}

			blocklist := set.NewSafeSet()
			tc.mock(peer, mockPeers, blocklist, dynconfig.EXPECT())
			scheduler := New(mockSchedulerConfig, dynconfig, mockPluginDir)
			parent, ok := scheduler.FindParent(context.Background(), peer, blocklist)
			tc.expect(t, mockPeers, parent, ok)
		})
	}
}

func TestScheduler_constructSuccessPeerPacket(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(md *configmocks.MockDynconfigInterfaceMockRecorder)
		expect func(t *testing.T, packet *rpcscheduler.PeerPacket, parent *resource.Peer, candidateParents []*resource.Peer)
	}{
		{
			name: "get parallelCount from dynconfig",
			mock: func(md *configmocks.MockDynconfigInterfaceMockRecorder) {
				md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{
					ParallelCount: 1,
				}, true).Times(1)
			},
			expect: func(t *testing.T, packet *rpcscheduler.PeerPacket, parent *resource.Peer, candidateParents []*resource.Peer) {
				assert := assert.New(t)
				assert.EqualValues(packet, &rpcscheduler.PeerPacket{
					TaskId:        mockTaskID,
					SrcPid:        mockPeerID,
					ParallelCount: 1,
					MainPeer: &rpcscheduler.PeerPacket_DestPeer{
						Ip:      parent.Host.IP,
						RpcPort: parent.Host.Port,
						PeerId:  parent.ID,
					},
					StealPeers: []*rpcscheduler.PeerPacket_DestPeer{
						{
							Ip:      candidateParents[0].Host.IP,
							RpcPort: candidateParents[0].Host.Port,
							PeerId:  candidateParents[0].ID,
						},
					},
					Code: base.Code_Success,
				})
			},
		},
		{
			name: "use default parallelCount",
			mock: func(md *configmocks.MockDynconfigInterfaceMockRecorder) {
				md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{}, false).Times(1)
			},
			expect: func(t *testing.T, packet *rpcscheduler.PeerPacket, parent *resource.Peer, candidateParents []*resource.Peer) {
				assert := assert.New(t)
				assert.EqualValues(packet, &rpcscheduler.PeerPacket{
					TaskId:        mockTaskID,
					SrcPid:        mockPeerID,
					ParallelCount: 4,
					MainPeer: &rpcscheduler.PeerPacket_DestPeer{
						Ip:      parent.Host.IP,
						RpcPort: parent.Host.Port,
						PeerId:  parent.ID,
					},
					StealPeers: []*rpcscheduler.PeerPacket_DestPeer{
						{
							Ip:      candidateParents[0].Host.IP,
							RpcPort: candidateParents[0].Host.Port,
							PeerId:  candidateParents[0].ID,
						},
					},
					Code: base.Code_Success,
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))

			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			parent := resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost)
			candidateParents := []*resource.Peer{resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost)}

			tc.mock(dynconfig.EXPECT())
			tc.expect(t, constructSuccessPeerPacket(dynconfig, peer, parent, candidateParents), parent, candidateParents)
		})
	}
}
