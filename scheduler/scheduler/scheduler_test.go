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
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"
	"d7y.io/api/pkg/apis/scheduler/v1/mocks"

	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/idgen"
	pkgtypes "d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	configmocks "d7y.io/dragonfly/v2/scheduler/config/mocks"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler/evaluator"
)

var (
	mockPluginDir       = "plugin_dir"
	mockSchedulerConfig = &config.SchedulerConfig{
		RetryLimit:             2,
		RetryBackToSourceLimit: 1,
		RetryInterval:          10 * time.Millisecond,
		BackToSourceCount:      int(mockTaskBackToSourceLimit),
		Algorithm:              evaluator.DefaultAlgorithm,
	}

	mockRawHost = resource.Host{
		ID:              idgen.HostID("hostname", 8003),
		Type:            pkgtypes.HostTypeNormal,
		Hostname:        "hostname",
		IP:              "127.0.0.1",
		Port:            8003,
		DownloadPort:    8001,
		OS:              "darwin",
		Platform:        "darwin",
		PlatformFamily:  "Standalone Workstation",
		PlatformVersion: "11.1",
		KernelVersion:   "20.2.0",
		CPU:             mockCPU,
		Memory:          mockMemory,
		Network:         mockNetwork,
		Disk:            mockDisk,
		Build:           mockBuild,
	}

	mockRawSeedHost = resource.Host{
		ID:              idgen.HostID("hostname_seed", 8003),
		Type:            pkgtypes.HostTypeSuperSeed,
		Hostname:        "hostname_seed",
		IP:              "127.0.0.1",
		Port:            8003,
		DownloadPort:    8001,
		OS:              "darwin",
		Platform:        "darwin",
		PlatformFamily:  "Standalone Workstation",
		PlatformVersion: "11.1",
		KernelVersion:   "20.2.0",
		CPU:             mockCPU,
		Memory:          mockMemory,
		Network:         mockNetwork,
		Disk:            mockDisk,
		Build:           mockBuild,
	}

	mockCPU = resource.CPU{
		LogicalCount:   4,
		PhysicalCount:  2,
		Percent:        1,
		ProcessPercent: 0.5,
		Times: resource.CPUTimes{
			User:      240662.2,
			System:    317950.1,
			Idle:      3393691.3,
			Nice:      0,
			Iowait:    0,
			Irq:       0,
			Softirq:   0,
			Steal:     0,
			Guest:     0,
			GuestNice: 0,
		},
	}

	mockMemory = resource.Memory{
		Total:              17179869184,
		Available:          5962813440,
		Used:               11217055744,
		UsedPercent:        65.291858,
		ProcessUsedPercent: 41.525125,
		Free:               2749598908,
	}

	mockNetwork = resource.Network{
		TCPConnectionCount:       10,
		UploadTCPConnectionCount: 1,
		SecurityDomain:           "security_domain",
		Location:                 "location",
		IDC:                      "idc",
	}

	mockDisk = resource.Disk{
		Total:             499963174912,
		Free:              37226479616,
		Used:              423809622016,
		UsedPercent:       91.92547406065952,
		InodesTotal:       4882452880,
		InodesUsed:        7835772,
		InodesFree:        4874617108,
		InodesUsedPercent: 0.1604884305611568,
	}

	mockBuild = resource.Build{
		GitVersion: "v1.0.0",
		GitCommit:  "221176b117c6d59366d68f2b34d38be50c935883",
		GoVersion:  "1.18",
		Platform:   "darwin",
	}

	mockTaskURLMeta = &commonv1.UrlMeta{
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
		mock   func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, mr *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
		expect func(t *testing.T, peer *resource.Peer)
	}{
		{
			name: "context was done",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, mr *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
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
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, mr *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
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
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, mr *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.NeedBackToSource.Store(true)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.StoreReportPieceResultStream(stream)

				mr.Send(gomock.Eq(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedNeedBackSource})).Return(errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(len(peer.Parents()), 0)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "peer needs back-to-source and send Code_SchedNeedBackSource code success",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, mr *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.NeedBackToSource.Store(true)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.StoreReportPieceResultStream(stream)

				mr.Send(gomock.Eq(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedNeedBackSource})).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(len(peer.Parents()), 0)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
				assert.True(peer.Task.FSM.Is(resource.TaskStatePending))
			},
		},
		{
			name: "peer needs back-to-source and task state is TaskStateFailed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, mr *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.NeedBackToSource.Store(true)
				peer.FSM.SetState(resource.PeerStateRunning)
				task.FSM.SetState(resource.TaskStateFailed)
				peer.StoreReportPieceResultStream(stream)

				mr.Send(gomock.Eq(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedNeedBackSource})).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(len(peer.Parents()), 0)
				assert.True(peer.FSM.Is(resource.PeerStateBackToSource))
				assert.True(peer.Task.FSM.Is(resource.TaskStateRunning))
			},
		},
		{
			name: "schedule exceeds RetryBackToSourceLimit and peer stream load failed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, mr *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "schedule exceeds RetryLimit and peer stream load failed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, mr *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.BackToSourceLimit.Store(-1)
				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(2)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(len(peer.Parents()), 0)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "schedule exceeds RetryLimit and send Code_SchedTaskStatusError code failed",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, mr *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.BackToSourceLimit.Store(-1)
				peer.StoreReportPieceResultStream(stream)

				gomock.InOrder(
					md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(2),
					mr.Send(gomock.Eq(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError})).Return(errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(len(peer.Parents()), 0)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "schedule exceeds RetryLimit and send Code_SchedTaskStatusError code success",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, mr *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.BackToSourceLimit.Store(-1)
				peer.StoreReportPieceResultStream(stream)

				gomock.InOrder(
					md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(2),
					mr.Send(gomock.Eq(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError})).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(len(peer.Parents()), 0)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
		{
			name: "schedule succeeded",
			mock: func(cancel context.CancelFunc, peer *resource.Peer, seedPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, mr *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				task := peer.Task
				task.StorePeer(peer)
				task.StorePeer(seedPeer)
				peer.FSM.SetState(resource.PeerStateRunning)
				seedPeer.FSM.SetState(resource.PeerStateRunning)
				peer.StoreReportPieceResultStream(stream)
				gomock.InOrder(
					md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1),
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{
						ParallelCount: 2,
					}, nil).Times(1),
					mr.Send(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer) {
				assert := assert.New(t)
				assert.Equal(len(peer.Parents()), 1)
				assert.True(peer.FSM.Is(resource.PeerStateRunning))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			stream := mocks.NewMockScheduler_ReportPieceResultServer(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			ctx, cancel := context.WithCancel(context.Background())
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			mockSeedHost := resource.NewHost(
				mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
				mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)
			seedPeer := resource.NewPeer(mockSeedPeerID, mockTask, mockSeedHost)
			blocklist := set.NewSafeSet[string]()

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
		mock   func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool)
	}{
		{
			name: "peer state is PeerStatePending",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStatePending)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateReceivedSmall",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedSmall)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateReceivedNormal",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateReceivedNormal)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateBackToSource",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateBackToSource)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateSucceeded",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateSucceeded)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateFailed",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateFailed)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer state is PeerStateLeave",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateLeave)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "task peers is empty",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "task contains only one peer and peer is itself",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer is in blocklist",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeer)
				blocklist.Add(mockPeer.ID)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer is bad node",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
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
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeer)
				if err := peer.Task.AddPeerEdge(peer, mockPeer); err != nil {
					t.Fatal(err)
				}

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "parent is peer's ancestor",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeer)
				if err := mockPeer.Task.AddPeerEdge(mockPeer, peer); err != nil {
					t.Fatal(err)
				}

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "parent free upload load is zero",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeer)
				mockPeer.Host.ConcurrentUploadLimit.Store(0)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer stream is empty",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeer)
				mockPeer.FinishedPieces.Set(0)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer stream send failed",
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.BackToSourcePeers.Add(mockPeer.ID)
				mockPeer.IsBackToSource.Store(true)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeer)
				mockPeer.FinishedPieces.Set(0)
				peer.StoreReportPieceResultStream(stream)
				gomock.InOrder(
					md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1),
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{
						ParallelCount: 2,
					}, nil).Times(1),
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
			mock: func(peer *resource.Peer, mockTask *resource.Task, mockPeer *resource.Peer, blocklist set.SafeSet[string], stream schedulerv1.Scheduler_ReportPieceResultServer, dynconfig config.DynconfigInterface, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeer.FSM.SetState(resource.PeerStateRunning)
				candidatePeer := resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, resource.NewHost(
					idgen.HostID(uuid.New().String(), 8003), mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type))
				candidatePeer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeer)
				peer.Task.StorePeer(candidatePeer)
				peer.Task.BackToSourcePeers.Add(mockPeer.ID)
				peer.Task.BackToSourcePeers.Add(candidatePeer.ID)
				mockPeer.IsBackToSource.Store(true)
				candidatePeer.IsBackToSource.Store(true)
				mockPeer.FinishedPieces.Set(0)
				peer.StoreReportPieceResultStream(stream)
				gomock.InOrder(
					md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1),
					md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{
						ParallelCount: 2,
					}, nil).Times(1),
					ms.Send(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, parents []*resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(len(parents), 2)
				assert.True(ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			stream := mocks.NewMockScheduler_ReportPieceResultServer(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			mockPeer := resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, resource.NewHost(
				idgen.HostID(uuid.New().String(), 8003), mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type))
			blocklist := set.NewSafeSet[string]()

			tc.mock(peer, mockTask, mockPeer, blocklist, stream, dynconfig, stream.EXPECT(), dynconfig.EXPECT())
			scheduler := New(mockSchedulerConfig, dynconfig, mockPluginDir)
			parents, ok := scheduler.NotifyAndFindParent(context.Background(), peer, blocklist)
			tc.expect(t, peer, parents, ok)
		})
	}
}

func TestScheduler_FindParent(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet[string], md *configmocks.MockDynconfigInterfaceMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, mockPeers []*resource.Peer, parent *resource.Peer, ok bool)
	}{
		{
			name: "task peers is empty",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet[string], md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "task contains only one peer and peer is itself",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet[string], md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer is in blocklist",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet[string], md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeers[0])
				blocklist.Add(mockPeers[0].ID)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "peer is bad node",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet[string], md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateFailed)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeers[0])

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "parent is peer's descendant",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet[string], md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeers[0])
				if err := peer.Task.AddPeerEdge(peer, mockPeers[0]); err != nil {
					t.Fatal(err)
				}

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "parent free upload load is zero",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet[string], md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeers[0])
				mockPeers[0].Host.ConcurrentUploadLimit.Store(0)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
		{
			name: "find back-to-source parent",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet[string], md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateRunning)
				mockPeers[1].FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeers[0])
				peer.Task.StorePeer(mockPeers[1])
				peer.Task.BackToSourcePeers.Add(mockPeers[0].ID)
				peer.Task.BackToSourcePeers.Add(mockPeers[1].ID)
				mockPeers[0].IsBackToSource.Store(true)
				mockPeers[1].IsBackToSource.Store(true)
				mockPeers[0].FinishedPieces.Set(0)
				mockPeers[1].FinishedPieces.Set(0)
				mockPeers[1].FinishedPieces.Set(1)
				mockPeers[1].FinishedPieces.Set(2)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.True(ok)
				assert.Equal(mockPeers[1].ID, parent.ID)
			},
		},
		{
			name: "find seed peer parent",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet[string], md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateRunning)
				mockPeers[1].FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeers[0])
				peer.Task.StorePeer(mockPeers[1])
				peer.Task.StorePeer(mockPeers[2])
				mockPeers[0].Host.Type = pkgtypes.HostTypeSuperSeed
				mockPeers[1].Host.Type = pkgtypes.HostTypeSuperSeed
				mockPeers[0].FinishedPieces.Set(0)
				mockPeers[1].FinishedPieces.Set(0)
				mockPeers[1].FinishedPieces.Set(1)
				mockPeers[1].FinishedPieces.Set(2)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.True(ok)
				assert.Equal(mockPeers[1].ID, parent.ID)
			},
		},
		{
			name: "parent state is PeerStateSucceeded",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet[string], md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateSucceeded)
				mockPeers[1].FSM.SetState(resource.PeerStateSucceeded)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeers[0])
				peer.Task.StorePeer(mockPeers[1])
				mockPeers[0].FinishedPieces.Set(0)
				mockPeers[1].FinishedPieces.Set(0)
				mockPeers[1].FinishedPieces.Set(1)
				mockPeers[1].FinishedPieces.Set(2)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.True(ok)
				assert.Equal(mockPeers[1].ID, parent.ID)
			},
		},
		{
			name: "find parent with ancestor",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet[string], md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateRunning)
				mockPeers[1].FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeers[0])
				peer.Task.StorePeer(mockPeers[1])
				peer.Task.StorePeer(mockPeers[2])
				if err := peer.Task.AddPeerEdge(mockPeers[2], mockPeers[0]); err != nil {
					t.Fatal(err)
				}

				if err := peer.Task.AddPeerEdge(mockPeers[2], mockPeers[1]); err != nil {
					t.Fatal(err)
				}

				mockPeers[0].FinishedPieces.Set(0)
				mockPeers[1].FinishedPieces.Set(0)
				mockPeers[1].FinishedPieces.Set(1)
				mockPeers[1].FinishedPieces.Set(2)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.True(ok)
				assert.Equal(mockPeers[1].ID, parent.ID)
			},
		},
		{
			name: "find parent with same host",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet[string], md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateRunning)
				mockPeers[1].FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].IsBackToSource.Store(true)
				mockPeers[1].Host = peer.Host
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeers[0])
				peer.Task.StorePeer(mockPeers[1])
				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.True(ok)
				assert.Equal(mockPeers[0].ID, parent.ID)
			},
		},
		{
			name: "find parent and fetch filterParentLimit from manager dynconfig",
			mock: func(peer *resource.Peer, mockPeers []*resource.Peer, blocklist set.SafeSet[string], md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.FSM.SetState(resource.PeerStateRunning)
				mockPeers[0].FSM.SetState(resource.PeerStateRunning)
				mockPeers[1].FSM.SetState(resource.PeerStateRunning)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(mockPeers[0])
				peer.Task.StorePeer(mockPeers[1])
				peer.Task.BackToSourcePeers.Add(mockPeers[0].ID)
				peer.Task.BackToSourcePeers.Add(mockPeers[1].ID)
				mockPeers[0].IsBackToSource.Store(true)
				mockPeers[1].IsBackToSource.Store(true)
				mockPeers[0].FinishedPieces.Set(0)
				mockPeers[1].FinishedPieces.Set(0)
				mockPeers[1].FinishedPieces.Set(1)
				mockPeers[1].FinishedPieces.Set(2)

				md.GetSchedulerClusterConfig().Return(types.SchedulerClusterConfig{
					FilterParentLimit: 3,
				}, nil).Times(1)
			},
			expect: func(t *testing.T, peer *resource.Peer, mockPeers []*resource.Peer, parent *resource.Peer, ok bool) {
				assert := assert.New(t)
				assert.True(ok)
				assert.Contains([]string{mockPeers[0].ID, mockPeers[1].ID, peer.ID}, parent.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)

			var mockPeers []*resource.Peer
			for i := 0; i < 11; i++ {
				mockHost := resource.NewHost(
					idgen.HostID(uuid.New().String(), 8003), mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
				peer := resource.NewPeer(idgen.PeerID(fmt.Sprintf("127.0.0.%d", i)), mockTask, mockHost)
				mockPeers = append(mockPeers, peer)
			}

			blocklist := set.NewSafeSet[string]()
			tc.mock(peer, mockPeers, blocklist, dynconfig.EXPECT())
			scheduler := New(mockSchedulerConfig, dynconfig, mockPluginDir)
			parent, found := scheduler.FindParent(context.Background(), peer, blocklist)
			tc.expect(t, peer, mockPeers, parent, found)
		})
	}
}

func TestScheduler_constructSuccessPeerPacket(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(md *configmocks.MockDynconfigInterfaceMockRecorder)
		expect func(t *testing.T, packet *schedulerv1.PeerPacket, parent *resource.Peer, candidateParents []*resource.Peer)
	}{
		{
			name: "get parallelCount from dynconfig",
			mock: func(md *configmocks.MockDynconfigInterfaceMockRecorder) {
				md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{
					ParallelCount: 1,
				}, nil).Times(1)
			},
			expect: func(t *testing.T, packet *schedulerv1.PeerPacket, parent *resource.Peer, candidateParents []*resource.Peer) {
				assert := assert.New(t)
				assert.EqualValues(packet, &schedulerv1.PeerPacket{
					TaskId:        mockTaskID,
					SrcPid:        mockPeerID,
					ParallelCount: 1,
					MainPeer: &schedulerv1.PeerPacket_DestPeer{
						Ip:      parent.Host.IP,
						RpcPort: parent.Host.Port,
						PeerId:  parent.ID,
					},
					CandidatePeers: []*schedulerv1.PeerPacket_DestPeer{
						{
							Ip:      candidateParents[0].Host.IP,
							RpcPort: candidateParents[0].Host.Port,
							PeerId:  candidateParents[0].ID,
						},
					},
					Code: commonv1.Code_Success,
				})
			},
		},
		{
			name: "use default parallelCount",
			mock: func(md *configmocks.MockDynconfigInterfaceMockRecorder) {
				md.GetSchedulerClusterClientConfig().Return(types.SchedulerClusterClientConfig{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, packet *schedulerv1.PeerPacket, parent *resource.Peer, candidateParents []*resource.Peer) {
				assert := assert.New(t)
				assert.EqualValues(packet, &schedulerv1.PeerPacket{
					TaskId:        mockTaskID,
					SrcPid:        mockPeerID,
					ParallelCount: 4,
					MainPeer: &schedulerv1.PeerPacket_DestPeer{
						Ip:      parent.Host.IP,
						RpcPort: parent.Host.Port,
						PeerId:  parent.ID,
					},
					CandidatePeers: []*schedulerv1.PeerPacket_DestPeer{
						{
							Ip:      candidateParents[0].Host.IP,
							RpcPort: candidateParents[0].Host.Port,
							PeerId:  candidateParents[0].ID,
						},
					},
					Code: commonv1.Code_Success,
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))

			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			parent := resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost)
			candidateParents := []*resource.Peer{resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost)}

			tc.mock(dynconfig.EXPECT())
			tc.expect(t, constructSuccessPeerPacket(dynconfig, peer, parent, candidateParents), parent, candidateParents)
		})
	}
}
