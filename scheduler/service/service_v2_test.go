/*
 *     Copyright 2023 The Dragonfly Authors
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
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
	dfdaemonv2 "d7y.io/api/v2/pkg/apis/dfdaemon/v2"
	managerv2 "d7y.io/api/v2/pkg/apis/manager/v2"
	schedulerv2 "d7y.io/api/v2/pkg/apis/scheduler/v2"
	schedulerv2mocks "d7y.io/api/v2/pkg/apis/scheduler/v2/mocks"

	managertypes "d7y.io/dragonfly/v2/manager/types"
	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
	pkgtypes "d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	configmocks "d7y.io/dragonfly/v2/scheduler/config/mocks"
	"d7y.io/dragonfly/v2/scheduler/resource/persistentcache"
	"d7y.io/dragonfly/v2/scheduler/resource/standard"
	schedulingmocks "d7y.io/dragonfly/v2/scheduler/scheduling/mocks"
)

var (
	mockRawHost = standard.Host{
		ID:              mockHostID,
		Type:            pkgtypes.HostTypeNormal,
		Hostname:        "foo",
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
		CreatedAt:       atomic.NewTime(time.Now()),
		UpdatedAt:       atomic.NewTime(time.Now()),
	}

	mockRawSeedHost = standard.Host{
		ID:              mockSeedHostID,
		Type:            pkgtypes.HostTypeSuperSeed,
		Hostname:        "bar",
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
		CreatedAt:       atomic.NewTime(time.Now()),
		UpdatedAt:       atomic.NewTime(time.Now()),
	}

	mockCPU = standard.CPU{
		LogicalCount:   4,
		PhysicalCount:  2,
		Percent:        1,
		ProcessPercent: 0.5,
		Times: standard.CPUTimes{
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

	mockMemory = standard.Memory{
		Total:              17179869184,
		Available:          5962813440,
		Used:               11217055744,
		UsedPercent:        65.291858,
		ProcessUsedPercent: 41.525125,
		Free:               2749598908,
	}

	mockNetwork = standard.Network{
		TCPConnectionCount:       10,
		UploadTCPConnectionCount: 1,
		Location:                 mockHostLocation,
		IDC:                      mockHostIDC,
	}

	mockDisk = standard.Disk{
		Total:             499963174912,
		Free:              37226479616,
		Used:              423809622016,
		UsedPercent:       91.92547406065952,
		InodesTotal:       4882452880,
		InodesUsed:        7835772,
		InodesFree:        4874617108,
		InodesUsedPercent: 0.1604884305611568,
	}

	mockBuild = standard.Build{
		GitVersion: "v1.0.0",
		GitCommit:  "221176b117c6d59366d68f2b34d38be50c935883",
		GoVersion:  "1.18",
		Platform:   "darwin",
	}

	mockInterval = durationpb.New(5 * time.Minute).AsDuration()

	mockRawPersistentCacheHost = persistentcache.Host{
		ID:              mockHostID,
		Type:            pkgtypes.HostTypeNormal,
		Hostname:        "foo",
		IP:              "127.0.0.1",
		Port:            8003,
		DownloadPort:    8001,
		OS:              "darwin",
		Platform:        "darwin",
		PlatformFamily:  "Standalone Workstation",
		PlatformVersion: "11.1",
		KernelVersion:   "20.2.0",
		CPU:             mockPersistentCacheCPU,
		Memory:          mockPersistentCacheMemory,
		Network:         mockPersistentCacheNetwork,
		Disk:            mockPersistentCacheDisk,
		Build:           mockPersistentCacheBuild,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}

	mockPersistentCacheCPU = persistentcache.CPU{
		LogicalCount:   4,
		PhysicalCount:  2,
		Percent:        1,
		ProcessPercent: 0.5,
		Times: persistentcache.CPUTimes{
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

	mockPersistentCacheMemory = persistentcache.Memory{
		Total:              17179869184,
		Available:          5962813440,
		Used:               11217055744,
		UsedPercent:        65.291858,
		ProcessUsedPercent: 41.525125,
		Free:               2749598908,
	}

	mockPersistentCacheNetwork = persistentcache.Network{
		TCPConnectionCount:       10,
		UploadTCPConnectionCount: 1,
		Location:                 mockHostLocation,
		IDC:                      mockHostIDC,
	}

	mockPersistentCacheDisk = persistentcache.Disk{
		Total:             499963174912,
		Free:              37226479616,
		Used:              423809622016,
		UsedPercent:       91.92547406065952,
		InodesTotal:       4882452880,
		InodesUsed:        7835772,
		InodesFree:        4874617108,
		InodesUsedPercent: 0.1604884305611568,
	}

	mockPersistentCacheBuild = persistentcache.Build{
		GitVersion: "v1.0.0",
		GitCommit:  "221176b117c6d59366d68f2b34d38be50c935883",
		GoVersion:  "1.18",
		Platform:   "darwin",
	}

	mockPersistentCacheInterval = durationpb.New(5 * time.Minute).AsDuration()
)

func TestService_NewV2(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s any)
	}{
		{
			name: "new service",
			expect: func(t *testing.T, s any) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "V2")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			tc.expect(t, NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, persistentCacheResource, scheduling, dynconfig))
		})
	}
}

func TestServiceV2_StatPeer(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder)
		expect func(t *testing.T, peer *standard.Peer, resp *commonv2.Peer, err error)
	}{
		{
			name: "peer not found",
			mock: func(peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *standard.Peer, resp *commonv2.Peer, err error) {
				assert := assert.New(t)
				assert.ErrorIs(err, status.Errorf(codes.NotFound, "peer %s not found", mockPeerID))
			},
		},
		{
			name: "peer has been loaded",
			mock: func(peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				peer.StorePiece(&mockPiece)
				peer.Task.StorePiece(&mockPiece)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *standard.Peer, resp *commonv2.Peer, err error) {
				dgst := peer.Task.Digest.String()

				assert := assert.New(t)
				assert.EqualValues(resp, &commonv2.Peer{
					Id: peer.ID,
					Range: &commonv2.Range{
						Start:  uint64(peer.Range.Start),
						Length: uint64(peer.Range.Length),
					},
					Priority: peer.Priority,
					Pieces: []*commonv2.Piece{
						{
							Number:      uint32(mockPiece.Number),
							ParentId:    &mockPiece.ParentID,
							Offset:      mockPiece.Offset,
							Length:      mockPiece.Length,
							Digest:      mockPiece.Digest.String(),
							TrafficType: &mockPiece.TrafficType,
							Cost:        durationpb.New(mockPiece.Cost),
							CreatedAt:   timestamppb.New(mockPiece.CreatedAt),
						},
					},
					Cost:  durationpb.New(peer.Cost.Load()),
					State: peer.FSM.Current(),
					Task: &commonv2.Task{
						Id:                  peer.Task.ID,
						Type:                peer.Task.Type,
						Url:                 peer.Task.URL,
						Digest:              &dgst,
						Tag:                 &peer.Task.Tag,
						Application:         &peer.Task.Application,
						FilteredQueryParams: peer.Task.FilteredQueryParams,
						RequestHeader:       peer.Task.Header,
						PieceLength:         uint64(peer.Task.PieceLength),
						ContentLength:       uint64(peer.Task.ContentLength.Load()),
						PieceCount:          uint32(peer.Task.TotalPieceCount.Load()),
						SizeScope:           peer.Task.SizeScope(),
						Pieces: []*commonv2.Piece{
							{
								Number:      uint32(mockPiece.Number),
								ParentId:    &mockPiece.ParentID,
								Offset:      mockPiece.Offset,
								Length:      mockPiece.Length,
								Digest:      mockPiece.Digest.String(),
								TrafficType: &mockPiece.TrafficType,
								Cost:        durationpb.New(mockPiece.Cost),
								CreatedAt:   timestamppb.New(mockPiece.CreatedAt),
							},
						},
						State:     peer.Task.FSM.Current(),
						PeerCount: uint32(peer.Task.PeerCount()),
						CreatedAt: timestamppb.New(peer.Task.CreatedAt.Load()),
						UpdatedAt: timestamppb.New(peer.Task.UpdatedAt.Load()),
					},
					Host: &commonv2.Host{
						Id:              peer.Host.ID,
						Type:            uint32(peer.Host.Type),
						Hostname:        peer.Host.Hostname,
						Ip:              peer.Host.IP,
						Port:            peer.Host.Port,
						DownloadPort:    peer.Host.DownloadPort,
						Os:              peer.Host.OS,
						Platform:        peer.Host.Platform,
						PlatformFamily:  peer.Host.PlatformFamily,
						PlatformVersion: peer.Host.PlatformVersion,
						KernelVersion:   peer.Host.KernelVersion,
						Cpu: &commonv2.CPU{
							LogicalCount:   peer.Host.CPU.LogicalCount,
							PhysicalCount:  peer.Host.CPU.PhysicalCount,
							Percent:        peer.Host.CPU.Percent,
							ProcessPercent: peer.Host.CPU.ProcessPercent,
							Times: &commonv2.CPUTimes{
								User:      peer.Host.CPU.Times.User,
								System:    peer.Host.CPU.Times.System,
								Idle:      peer.Host.CPU.Times.Idle,
								Nice:      peer.Host.CPU.Times.Nice,
								Iowait:    peer.Host.CPU.Times.Iowait,
								Irq:       peer.Host.CPU.Times.Irq,
								Softirq:   peer.Host.CPU.Times.Softirq,
								Steal:     peer.Host.CPU.Times.Steal,
								Guest:     peer.Host.CPU.Times.Guest,
								GuestNice: peer.Host.CPU.Times.GuestNice,
							},
						},
						Memory: &commonv2.Memory{
							Total:              peer.Host.Memory.Total,
							Available:          peer.Host.Memory.Available,
							Used:               peer.Host.Memory.Used,
							UsedPercent:        peer.Host.Memory.UsedPercent,
							ProcessUsedPercent: peer.Host.Memory.ProcessUsedPercent,
							Free:               peer.Host.Memory.Free,
						},
						Network: &commonv2.Network{
							TcpConnectionCount:       peer.Host.Network.TCPConnectionCount,
							UploadTcpConnectionCount: peer.Host.Network.UploadTCPConnectionCount,
							Location:                 &peer.Host.Network.Location,
							Idc:                      &peer.Host.Network.IDC,
							DownloadRate:             peer.Host.Network.DownloadRate,
							DownloadRateLimit:        peer.Host.Network.DownloadRateLimit,
							UploadRate:               peer.Host.Network.UploadRate,
							UploadRateLimit:          peer.Host.Network.UploadRateLimit,
						},
						Disk: &commonv2.Disk{
							Total:             peer.Host.Disk.Total,
							Free:              peer.Host.Disk.Free,
							Used:              peer.Host.Disk.Used,
							UsedPercent:       peer.Host.Disk.UsedPercent,
							InodesTotal:       peer.Host.Disk.InodesTotal,
							InodesUsed:        peer.Host.Disk.InodesUsed,
							InodesFree:        peer.Host.Disk.InodesFree,
							InodesUsedPercent: peer.Host.Disk.InodesUsedPercent,
						},
						Build: &commonv2.Build{
							GitVersion: peer.Host.Build.GitVersion,
							GitCommit:  &peer.Host.Build.GitCommit,
							GoVersion:  &peer.Host.Build.GoVersion,
							Platform:   &peer.Host.Build.Platform,
						},
					},
					NeedBackToSource: peer.NeedBackToSource.Load(),
					CreatedAt:        timestamppb.New(peer.CreatedAt.Load()),
					UpdatedAt:        timestamppb.New(peer.UpdatedAt.Load()),
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			peerManager := standard.NewMockPeerManager(ctl)
			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockSeedPeerID, mockTask, mockHost, standard.WithRange(mockPeerRange))
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.mock(peer, peerManager, resource.EXPECT(), peerManager.EXPECT())
			resp, err := svc.StatPeer(context.Background(), &schedulerv2.StatPeerRequest{TaskId: mockTaskID, PeerId: mockPeerID})
			tc.expect(t, peer, resp, err)
		})
	}
}

func TestServiceV2_DeletePeer(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder)
		expect func(t *testing.T, err error)
	}{
		{
			name: "peer not found",
			mock: func(peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.ErrorIs(err, status.Errorf(codes.NotFound, "peer %s not found", mockPeerID))
			},
		},
		{
			name: "peer fsm event failed",
			mock: func(peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(standard.PeerStateLeave)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.ErrorIs(err, status.Error(codes.FailedPrecondition, "peer fsm event failed: event Leave inappropriate in current state Leave"))
			},
		},
		{
			name: "peer leaves succeeded",
			mock: func(peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
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
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			peerManager := standard.NewMockPeerManager(ctl)
			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockSeedPeerID, mockTask, mockHost, standard.WithRange(mockPeerRange))
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.mock(peer, peerManager, resource.EXPECT(), peerManager.EXPECT())
			tc.expect(t, svc.DeletePeer(context.Background(), &schedulerv2.DeletePeerRequest{TaskId: mockTaskID, PeerId: mockPeerID}))
		})
	}
}

func TestServiceV2_StatTask(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(task *standard.Task, taskManager standard.TaskManager, mr *standard.MockResourceMockRecorder, mt *standard.MockTaskManagerMockRecorder)
		expect func(t *testing.T, task *standard.Task, resp *commonv2.Task, err error)
	}{
		{
			name: "task not found",
			mock: func(task *standard.Task, taskManager standard.TaskManager, mr *standard.MockResourceMockRecorder, mt *standard.MockTaskManagerMockRecorder) {
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, task *standard.Task, resp *commonv2.Task, err error) {
				assert := assert.New(t)
				assert.ErrorIs(err, status.Errorf(codes.NotFound, "task %s not found", mockTaskID))
			},
		},
		{
			name: "task has been loaded",
			mock: func(task *standard.Task, taskManager standard.TaskManager, mr *standard.MockResourceMockRecorder, mt *standard.MockTaskManagerMockRecorder) {
				task.StorePiece(&mockPiece)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(task, true).Times(1),
				)
			},
			expect: func(t *testing.T, task *standard.Task, resp *commonv2.Task, err error) {
				dgst := task.Digest.String()

				assert := assert.New(t)
				assert.EqualValues(resp, &commonv2.Task{
					Id:                  task.ID,
					Type:                task.Type,
					Url:                 task.URL,
					Digest:              &dgst,
					Tag:                 &task.Tag,
					Application:         &task.Application,
					FilteredQueryParams: task.FilteredQueryParams,
					RequestHeader:       task.Header,
					PieceLength:         uint64(task.PieceLength),
					ContentLength:       uint64(task.ContentLength.Load()),
					PieceCount:          uint32(task.TotalPieceCount.Load()),
					SizeScope:           task.SizeScope(),
					Pieces: []*commonv2.Piece{
						{
							Number:      uint32(mockPiece.Number),
							ParentId:    &mockPiece.ParentID,
							Offset:      mockPiece.Offset,
							Length:      mockPiece.Length,
							Digest:      mockPiece.Digest.String(),
							TrafficType: &mockPiece.TrafficType,
							Cost:        durationpb.New(mockPiece.Cost),
							CreatedAt:   timestamppb.New(mockPiece.CreatedAt),
						},
					},
					State:     task.FSM.Current(),
					PeerCount: uint32(task.PeerCount()),
					CreatedAt: timestamppb.New(task.CreatedAt.Load()),
					UpdatedAt: timestamppb.New(task.UpdatedAt.Load()),
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			taskManager := standard.NewMockTaskManager(ctl)
			task := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.mock(task, taskManager, resource.EXPECT(), taskManager.EXPECT())
			resp, err := svc.StatTask(context.Background(), &schedulerv2.StatTaskRequest{TaskId: mockTaskID})
			tc.expect(t, task, resp, err)
		})
	}
}

func TestServiceV2_AnnounceHost(t *testing.T) {
	tests := []struct {
		name string
		req  *schedulerv2.AnnounceHostRequest
		run  func(t *testing.T, svc *V2, req *schedulerv2.AnnounceHostRequest, host *standard.Host, persistentCacheHost *persistentcache.Host, hostManager standard.HostManager, persistentCacheHostManager persistentcache.HostManager, mr *standard.MockResourceMockRecorder, mpr *persistentcache.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder, mph *persistentcache.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "host not found and persistent cache host not found",
			req: &schedulerv2.AnnounceHostRequest{
				Host: &commonv2.Host{
					Id:              mockHostID,
					Type:            uint32(pkgtypes.HostTypeNormal),
					Hostname:        "hostname",
					Ip:              "127.0.0.1",
					Port:            8003,
					DownloadPort:    8001,
					DisableShared:   true,
					Os:              "darwin",
					Platform:        "darwin",
					PlatformFamily:  "Standalone Workstation",
					PlatformVersion: "11.1",
					KernelVersion:   "20.2.0",
					Cpu: &commonv2.CPU{
						LogicalCount:   mockCPU.LogicalCount,
						PhysicalCount:  mockCPU.PhysicalCount,
						Percent:        mockCPU.Percent,
						ProcessPercent: mockCPU.ProcessPercent,
						Times: &commonv2.CPUTimes{
							User:      mockCPU.Times.User,
							System:    mockCPU.Times.System,
							Idle:      mockCPU.Times.Idle,
							Nice:      mockCPU.Times.Nice,
							Iowait:    mockCPU.Times.Iowait,
							Irq:       mockCPU.Times.Irq,
							Softirq:   mockCPU.Times.Softirq,
							Steal:     mockCPU.Times.Steal,
							Guest:     mockCPU.Times.Guest,
							GuestNice: mockCPU.Times.GuestNice,
						},
					},
					Memory: &commonv2.Memory{
						Total:              mockMemory.Total,
						Available:          mockMemory.Available,
						Used:               mockMemory.Used,
						UsedPercent:        mockMemory.UsedPercent,
						ProcessUsedPercent: mockMemory.ProcessUsedPercent,
						Free:               mockMemory.Free,
					},
					Network: &commonv2.Network{
						TcpConnectionCount:       mockNetwork.TCPConnectionCount,
						UploadTcpConnectionCount: mockNetwork.UploadTCPConnectionCount,
						Location:                 &mockNetwork.Location,
						Idc:                      &mockNetwork.IDC,
						DownloadRate:             mockNetwork.DownloadRate,
						DownloadRateLimit:        mockNetwork.DownloadRateLimit,
						UploadRate:               mockNetwork.UploadRate,
						UploadRateLimit:          mockNetwork.UploadRateLimit,
					},
					Disk: &commonv2.Disk{
						Total:             mockDisk.Total,
						Free:              mockDisk.Free,
						Used:              mockDisk.Used,
						UsedPercent:       mockDisk.UsedPercent,
						InodesTotal:       mockDisk.InodesTotal,
						InodesUsed:        mockDisk.InodesUsed,
						InodesFree:        mockDisk.InodesFree,
						InodesUsedPercent: mockDisk.InodesUsedPercent,
					},
					Build: &commonv2.Build{
						GitVersion: mockBuild.GitVersion,
						GitCommit:  &mockBuild.GitCommit,
						GoVersion:  &mockBuild.GoVersion,
						Platform:   &mockBuild.Platform,
					},
				},
				Interval: durationpb.New(5 * time.Minute),
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.AnnounceHostRequest, host *standard.Host, persistentCacheHost *persistentcache.Host, hostManager standard.HostManager, persistentCacheHostManager persistentcache.HostManager, mr *standard.MockResourceMockRecorder, mpr *persistentcache.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder, mph *persistentcache.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					md.GetSchedulerClusterClientConfig().Return(managertypes.SchedulerClusterClientConfig{LoadLimit: 10}, nil).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Store(gomock.Any()).Do(func(host *standard.Host) {
						assert := assert.New(t)
						assert.Equal(host.ID, req.Host.Id)
						assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
						assert.Equal(host.Hostname, req.Host.Hostname)
						assert.Equal(host.IP, req.Host.Ip)
						assert.Equal(host.Port, req.Host.Port)
						assert.Equal(host.DownloadPort, req.Host.DownloadPort)
						assert.Equal(host.DisableShared, req.Host.DisableShared)
						assert.Equal(host.OS, req.Host.Os)
						assert.Equal(host.Platform, req.Host.Platform)
						assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
						assert.Equal(host.KernelVersion, req.Host.KernelVersion)
						assert.EqualValues(host.CPU, mockCPU)
						assert.EqualValues(host.Memory, mockMemory)
						assert.EqualValues(host.Network, mockNetwork)
						assert.EqualValues(host.Disk, mockDisk)
						assert.EqualValues(host.Build, mockBuild)
						assert.EqualValues(host.AnnounceInterval, mockInterval)
						assert.Equal(host.ConcurrentUploadLimit.Load(), int32(10))
						assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
						assert.Equal(host.UploadCount.Load(), int64(0))
						assert.Equal(host.UploadFailedCount.Load(), int64(0))
						assert.NotNil(host.Peers)
						assert.Equal(host.PeerCount.Load(), int32(0))
						assert.NotEqual(host.CreatedAt.Load().Nanosecond(), 0)
						assert.NotEqual(host.UpdatedAt.Load().Nanosecond(), 0)
						assert.NotNil(host.Log)
					}).Return().Times(1),
					mpr.HostManager().Return(persistentCacheHostManager).Times(1),
					mph.Load(gomock.Any(), gomock.Any()).Return(nil, false).Times(1),
					mpr.HostManager().Return(persistentCacheHostManager).Times(1),
					mph.Store(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, host *persistentcache.Host) {
						assert := assert.New(t)
						assert.Equal(host.ID, req.Host.Id)
						assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
						assert.Equal(host.Hostname, req.Host.Hostname)
						assert.Equal(host.IP, req.Host.Ip)
						assert.Equal(host.Port, req.Host.Port)
						assert.Equal(host.DownloadPort, req.Host.DownloadPort)
						assert.Equal(host.DisableShared, req.Host.DisableShared)
						assert.Equal(host.OS, req.Host.Os)
						assert.Equal(host.Platform, req.Host.Platform)
						assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
						assert.Equal(host.KernelVersion, req.Host.KernelVersion)
						assert.EqualValues(host.CPU, mockPersistentCacheCPU)
						assert.EqualValues(host.Memory, mockPersistentCacheMemory)
						assert.EqualValues(host.Network, mockPersistentCacheNetwork)
						assert.EqualValues(host.Disk, mockPersistentCacheDisk)
						assert.EqualValues(host.Build, mockPersistentCacheBuild)
						assert.EqualValues(host.AnnounceInterval, mockPersistentCacheInterval)
						assert.Equal(host.ConcurrentUploadLimit, int32(10))
						assert.Equal(host.ConcurrentUploadCount, int32(0))
						assert.Equal(host.UploadCount, int64(0))
						assert.Equal(host.UploadFailedCount, int64(0))
						assert.NotEqual(host.CreatedAt.Nanosecond(), 0)
						assert.NotEqual(host.UpdatedAt.Nanosecond(), 0)
						assert.NotNil(host.Log)
					}).Return(nil).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.AnnounceHost(context.Background(), req))
			},
		},
		{
			name: "host not found and persistent cache host not found, dynconfig returns error",
			req: &schedulerv2.AnnounceHostRequest{
				Host: &commonv2.Host{
					Id:              mockHostID,
					Type:            uint32(pkgtypes.HostTypeNormal),
					Hostname:        "hostname",
					Ip:              "127.0.0.1",
					Port:            8003,
					DownloadPort:    8001,
					DisableShared:   false,
					Os:              "darwin",
					Platform:        "darwin",
					PlatformFamily:  "Standalone Workstation",
					PlatformVersion: "11.1",
					KernelVersion:   "20.2.0",
					Cpu: &commonv2.CPU{
						LogicalCount:   mockCPU.LogicalCount,
						PhysicalCount:  mockCPU.PhysicalCount,
						Percent:        mockCPU.Percent,
						ProcessPercent: mockCPU.ProcessPercent,
						Times: &commonv2.CPUTimes{
							User:      mockCPU.Times.User,
							System:    mockCPU.Times.System,
							Idle:      mockCPU.Times.Idle,
							Nice:      mockCPU.Times.Nice,
							Iowait:    mockCPU.Times.Iowait,
							Irq:       mockCPU.Times.Irq,
							Softirq:   mockCPU.Times.Softirq,
							Steal:     mockCPU.Times.Steal,
							Guest:     mockCPU.Times.Guest,
							GuestNice: mockCPU.Times.GuestNice,
						},
					},
					Memory: &commonv2.Memory{
						Total:              mockMemory.Total,
						Available:          mockMemory.Available,
						Used:               mockMemory.Used,
						UsedPercent:        mockMemory.UsedPercent,
						ProcessUsedPercent: mockMemory.ProcessUsedPercent,
						Free:               mockMemory.Free,
					},
					Network: &commonv2.Network{
						TcpConnectionCount:       mockNetwork.TCPConnectionCount,
						UploadTcpConnectionCount: mockNetwork.UploadTCPConnectionCount,
						Location:                 &mockNetwork.Location,
						Idc:                      &mockNetwork.IDC,
						DownloadRate:             mockNetwork.DownloadRate,
						DownloadRateLimit:        mockNetwork.DownloadRateLimit,
						UploadRate:               mockNetwork.UploadRate,
						UploadRateLimit:          mockNetwork.UploadRateLimit,
					},
					Disk: &commonv2.Disk{
						Total:             mockDisk.Total,
						Free:              mockDisk.Free,
						Used:              mockDisk.Used,
						UsedPercent:       mockDisk.UsedPercent,
						InodesTotal:       mockDisk.InodesTotal,
						InodesUsed:        mockDisk.InodesUsed,
						InodesFree:        mockDisk.InodesFree,
						InodesUsedPercent: mockDisk.InodesUsedPercent,
					},
					Build: &commonv2.Build{
						GitVersion: mockBuild.GitVersion,
						GitCommit:  &mockBuild.GitCommit,
						GoVersion:  &mockBuild.GoVersion,
						Platform:   &mockBuild.Platform,
					},
				},
				Interval: durationpb.New(5 * time.Minute),
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.AnnounceHostRequest, host *standard.Host, persistentCacheHost *persistentcache.Host, hostManager standard.HostManager, persistentCacheHostManager persistentcache.HostManager, mr *standard.MockResourceMockRecorder, mpr *persistentcache.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder, mph *persistentcache.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					md.GetSchedulerClusterClientConfig().Return(managertypes.SchedulerClusterClientConfig{}, errors.New("foo")).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Store(gomock.Any()).Do(func(host *standard.Host) {
						assert := assert.New(t)
						assert.Equal(host.ID, req.Host.Id)
						assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
						assert.Equal(host.Hostname, req.Host.Hostname)
						assert.Equal(host.IP, req.Host.Ip)
						assert.Equal(host.Port, req.Host.Port)
						assert.Equal(host.DownloadPort, req.Host.DownloadPort)
						assert.Equal(host.DisableShared, req.Host.DisableShared)
						assert.Equal(host.OS, req.Host.Os)
						assert.Equal(host.Platform, req.Host.Platform)
						assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
						assert.Equal(host.KernelVersion, req.Host.KernelVersion)
						assert.EqualValues(host.CPU, mockCPU)
						assert.EqualValues(host.Memory, mockMemory)
						assert.EqualValues(host.Network, mockNetwork)
						assert.EqualValues(host.Disk, mockDisk)
						assert.EqualValues(host.Build, mockBuild)
						assert.EqualValues(host.AnnounceInterval, mockInterval)
						assert.Equal(host.ConcurrentUploadLimit.Load(), int32(200))
						assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
						assert.Equal(host.UploadCount.Load(), int64(0))
						assert.Equal(host.UploadFailedCount.Load(), int64(0))
						assert.NotNil(host.Peers)
						assert.Equal(host.PeerCount.Load(), int32(0))
						assert.NotEqual(host.CreatedAt.Load().Nanosecond(), 0)
						assert.NotEqual(host.UpdatedAt.Load().Nanosecond(), 0)
						assert.NotNil(host.Log)
					}).Return().Times(1),
					mpr.HostManager().Return(persistentCacheHostManager).Times(1),
					mph.Load(gomock.Any(), gomock.Any()).Return(nil, false).Times(1),
					mpr.HostManager().Return(persistentCacheHostManager).Times(1),
					mph.Store(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, host *persistentcache.Host) {
						assert := assert.New(t)
						assert.Equal(host.ID, req.Host.Id)
						assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
						assert.Equal(host.Hostname, req.Host.Hostname)
						assert.Equal(host.IP, req.Host.Ip)
						assert.Equal(host.Port, req.Host.Port)
						assert.Equal(host.DownloadPort, req.Host.DownloadPort)
						assert.Equal(host.DisableShared, req.Host.DisableShared)
						assert.Equal(host.OS, req.Host.Os)
						assert.Equal(host.Platform, req.Host.Platform)
						assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
						assert.Equal(host.KernelVersion, req.Host.KernelVersion)
						assert.EqualValues(host.CPU, mockPersistentCacheCPU)
						assert.EqualValues(host.Memory, mockPersistentCacheMemory)
						assert.EqualValues(host.Network, mockPersistentCacheNetwork)
						assert.EqualValues(host.Disk, mockPersistentCacheDisk)
						assert.EqualValues(host.Build, mockPersistentCacheBuild)
						assert.EqualValues(host.AnnounceInterval, mockPersistentCacheInterval)
						assert.Equal(host.ConcurrentUploadLimit, int32(200))
						assert.Equal(host.ConcurrentUploadCount, int32(0))
						assert.Equal(host.UploadCount, int64(0))
						assert.Equal(host.UploadFailedCount, int64(0))
						assert.NotEqual(host.CreatedAt.Nanosecond(), 0)
						assert.NotEqual(host.UpdatedAt.Nanosecond(), 0)
						assert.NotNil(host.Log)
					}).Return(nil).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.AnnounceHost(context.Background(), req))
			},
		},
		{
			name: "host already exists and persistent cache host already exists",
			req: &schedulerv2.AnnounceHostRequest{
				Host: &commonv2.Host{
					Id:              mockHostID,
					Type:            uint32(pkgtypes.HostTypeNormal),
					Hostname:        "foo",
					Ip:              "127.0.0.1",
					Port:            8003,
					DownloadPort:    8001,
					DisableShared:   true,
					Os:              "darwin",
					Platform:        "darwin",
					PlatformFamily:  "Standalone Workstation",
					PlatformVersion: "11.1",
					KernelVersion:   "20.2.0",
					Cpu: &commonv2.CPU{
						LogicalCount:   mockCPU.LogicalCount,
						PhysicalCount:  mockCPU.PhysicalCount,
						Percent:        mockCPU.Percent,
						ProcessPercent: mockCPU.ProcessPercent,
						Times: &commonv2.CPUTimes{
							User:      mockCPU.Times.User,
							System:    mockCPU.Times.System,
							Idle:      mockCPU.Times.Idle,
							Nice:      mockCPU.Times.Nice,
							Iowait:    mockCPU.Times.Iowait,
							Irq:       mockCPU.Times.Irq,
							Softirq:   mockCPU.Times.Softirq,
							Steal:     mockCPU.Times.Steal,
							Guest:     mockCPU.Times.Guest,
							GuestNice: mockCPU.Times.GuestNice,
						},
					},
					Memory: &commonv2.Memory{
						Total:              mockMemory.Total,
						Available:          mockMemory.Available,
						Used:               mockMemory.Used,
						UsedPercent:        mockMemory.UsedPercent,
						ProcessUsedPercent: mockMemory.ProcessUsedPercent,
						Free:               mockMemory.Free,
					},
					Network: &commonv2.Network{
						TcpConnectionCount:       mockNetwork.TCPConnectionCount,
						UploadTcpConnectionCount: mockNetwork.UploadTCPConnectionCount,
						Location:                 &mockNetwork.Location,
						Idc:                      &mockNetwork.IDC,
						DownloadRate:             mockNetwork.DownloadRate,
						DownloadRateLimit:        mockNetwork.DownloadRateLimit,
						UploadRate:               mockNetwork.UploadRate,
						UploadRateLimit:          mockNetwork.UploadRateLimit,
					},
					Disk: &commonv2.Disk{
						Total:             mockDisk.Total,
						Free:              mockDisk.Free,
						Used:              mockDisk.Used,
						UsedPercent:       mockDisk.UsedPercent,
						InodesTotal:       mockDisk.InodesTotal,
						InodesUsed:        mockDisk.InodesUsed,
						InodesFree:        mockDisk.InodesFree,
						InodesUsedPercent: mockDisk.InodesUsedPercent,
					},
					Build: &commonv2.Build{
						GitVersion: mockBuild.GitVersion,
						GitCommit:  &mockBuild.GitCommit,
						GoVersion:  &mockBuild.GoVersion,
						Platform:   &mockBuild.Platform,
					},
				},
				Interval: durationpb.New(5 * time.Minute),
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.AnnounceHostRequest, host *standard.Host, persistentCacheHost *persistentcache.Host, hostManager standard.HostManager, persistentCacheHostManager persistentcache.HostManager, mr *standard.MockResourceMockRecorder, mpr *persistentcache.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder, mph *persistentcache.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					md.GetSchedulerClusterClientConfig().Return(managertypes.SchedulerClusterClientConfig{LoadLimit: 10}, nil).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mpr.HostManager().Return(persistentCacheHostManager).Times(1),
					mph.Load(gomock.Any(), gomock.Any()).Return(persistentCacheHost, true).Times(1),
					mpr.HostManager().Return(persistentCacheHostManager).Times(1),
					mph.Store(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, host *persistentcache.Host) {
						assert := assert.New(t)
						assert.Equal(host.ID, req.Host.Id)
						assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
						assert.Equal(host.Hostname, req.Host.Hostname)
						assert.Equal(host.IP, req.Host.Ip)
						assert.Equal(host.Port, req.Host.Port)
						assert.Equal(host.DownloadPort, req.Host.DownloadPort)
						assert.Equal(host.DisableShared, req.Host.DisableShared)
						assert.Equal(host.OS, req.Host.Os)
						assert.Equal(host.Platform, req.Host.Platform)
						assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
						assert.Equal(host.KernelVersion, req.Host.KernelVersion)
						assert.EqualValues(host.CPU, mockPersistentCacheCPU)
						assert.EqualValues(host.Memory, mockPersistentCacheMemory)
						assert.EqualValues(host.Network, mockPersistentCacheNetwork)
						assert.EqualValues(host.Disk, mockPersistentCacheDisk)
						assert.EqualValues(host.Build, mockPersistentCacheBuild)
						assert.EqualValues(host.AnnounceInterval, mockPersistentCacheInterval)
						assert.Equal(host.ConcurrentUploadLimit, int32(10))
						assert.Equal(host.ConcurrentUploadCount, int32(0))
						assert.Equal(host.UploadCount, int64(0))
						assert.Equal(host.UploadFailedCount, int64(0))
						assert.NotEqual(host.CreatedAt.Nanosecond(), 0)
						assert.NotEqual(host.UpdatedAt.Nanosecond(), 0)
						assert.NotNil(host.Log)
					}).Return(nil).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.AnnounceHost(context.Background(), req))
				assert.Equal(host.ID, req.Host.Id)
				assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
				assert.Equal(host.Hostname, req.Host.Hostname)
				assert.Equal(host.IP, req.Host.Ip)
				assert.Equal(host.Port, req.Host.Port)
				assert.Equal(host.DownloadPort, req.Host.DownloadPort)
				assert.Equal(host.DisableShared, req.Host.DisableShared)
				assert.Equal(host.OS, req.Host.Os)
				assert.Equal(host.Platform, req.Host.Platform)
				assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
				assert.Equal(host.KernelVersion, req.Host.KernelVersion)
				assert.EqualValues(host.CPU, mockCPU)
				assert.EqualValues(host.Memory, mockMemory)
				assert.EqualValues(host.Network, mockNetwork)
				assert.EqualValues(host.Disk, mockDisk)
				assert.EqualValues(host.Build, mockBuild)
				assert.EqualValues(host.AnnounceInterval, mockInterval)
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(10))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEqual(host.CreatedAt.Load().Nanosecond(), 0)
				assert.NotEqual(host.UpdatedAt.Load().Nanosecond(), 0)
				assert.NotNil(host.Log)
			},
		},
		{
			name: "host already exists and persistent cache host already exists, dynconfig returns error",
			req: &schedulerv2.AnnounceHostRequest{
				Host: &commonv2.Host{
					Id:              mockHostID,
					Type:            uint32(pkgtypes.HostTypeNormal),
					Hostname:        "foo",
					Ip:              "127.0.0.1",
					Port:            8003,
					DownloadPort:    8001,
					DisableShared:   false,
					Os:              "darwin",
					Platform:        "darwin",
					PlatformFamily:  "Standalone Workstation",
					PlatformVersion: "11.1",
					KernelVersion:   "20.2.0",
					Cpu: &commonv2.CPU{
						LogicalCount:   mockCPU.LogicalCount,
						PhysicalCount:  mockCPU.PhysicalCount,
						Percent:        mockCPU.Percent,
						ProcessPercent: mockCPU.ProcessPercent,
						Times: &commonv2.CPUTimes{
							User:      mockCPU.Times.User,
							System:    mockCPU.Times.System,
							Idle:      mockCPU.Times.Idle,
							Nice:      mockCPU.Times.Nice,
							Iowait:    mockCPU.Times.Iowait,
							Irq:       mockCPU.Times.Irq,
							Softirq:   mockCPU.Times.Softirq,
							Steal:     mockCPU.Times.Steal,
							Guest:     mockCPU.Times.Guest,
							GuestNice: mockCPU.Times.GuestNice,
						},
					},
					Memory: &commonv2.Memory{
						Total:              mockMemory.Total,
						Available:          mockMemory.Available,
						Used:               mockMemory.Used,
						UsedPercent:        mockMemory.UsedPercent,
						ProcessUsedPercent: mockMemory.ProcessUsedPercent,
						Free:               mockMemory.Free,
					},
					Network: &commonv2.Network{
						TcpConnectionCount:       mockNetwork.TCPConnectionCount,
						UploadTcpConnectionCount: mockNetwork.UploadTCPConnectionCount,
						Location:                 &mockNetwork.Location,
						Idc:                      &mockNetwork.IDC,
						DownloadRate:             mockNetwork.DownloadRate,
						DownloadRateLimit:        mockNetwork.DownloadRateLimit,
						UploadRate:               mockNetwork.UploadRate,
						UploadRateLimit:          mockNetwork.UploadRateLimit,
					},
					Disk: &commonv2.Disk{
						Total:             mockDisk.Total,
						Free:              mockDisk.Free,
						Used:              mockDisk.Used,
						UsedPercent:       mockDisk.UsedPercent,
						InodesTotal:       mockDisk.InodesTotal,
						InodesUsed:        mockDisk.InodesUsed,
						InodesFree:        mockDisk.InodesFree,
						InodesUsedPercent: mockDisk.InodesUsedPercent,
					},
					Build: &commonv2.Build{
						GitVersion: mockBuild.GitVersion,
						GitCommit:  &mockBuild.GitCommit,
						GoVersion:  &mockBuild.GoVersion,
						Platform:   &mockBuild.Platform,
					},
				},
				Interval: durationpb.New(5 * time.Minute),
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.AnnounceHostRequest, host *standard.Host, persistentCacheHost *persistentcache.Host, hostManager standard.HostManager, persistentCacheHostManager persistentcache.HostManager, mr *standard.MockResourceMockRecorder, mpr *persistentcache.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder, mph *persistentcache.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					md.GetSchedulerClusterClientConfig().Return(managertypes.SchedulerClusterClientConfig{}, errors.New("foo")).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mpr.HostManager().Return(persistentCacheHostManager).Times(1),
					mph.Load(gomock.Any(), gomock.Any()).Return(persistentCacheHost, true).Times(1),
					mpr.HostManager().Return(persistentCacheHostManager).Times(1),
					mph.Store(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, host *persistentcache.Host) {
						assert := assert.New(t)
						assert.Equal(host.ID, req.Host.Id)
						assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
						assert.Equal(host.Hostname, req.Host.Hostname)
						assert.Equal(host.IP, req.Host.Ip)
						assert.Equal(host.Port, req.Host.Port)
						assert.Equal(host.DownloadPort, req.Host.DownloadPort)
						assert.Equal(host.DisableShared, req.Host.DisableShared)
						assert.Equal(host.OS, req.Host.Os)
						assert.Equal(host.Platform, req.Host.Platform)
						assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
						assert.Equal(host.KernelVersion, req.Host.KernelVersion)
						assert.EqualValues(host.CPU, mockPersistentCacheCPU)
						assert.EqualValues(host.Memory, mockPersistentCacheMemory)
						assert.EqualValues(host.Network, mockPersistentCacheNetwork)
						assert.EqualValues(host.Disk, mockPersistentCacheDisk)
						assert.EqualValues(host.Build, mockPersistentCacheBuild)
						assert.EqualValues(host.AnnounceInterval, mockPersistentCacheInterval)
						assert.Equal(host.ConcurrentUploadLimit, int32(200))
						assert.Equal(host.ConcurrentUploadCount, int32(0))
						assert.Equal(host.UploadCount, int64(0))
						assert.Equal(host.UploadFailedCount, int64(0))
						assert.NotEqual(host.CreatedAt.Nanosecond(), 0)
						assert.NotEqual(host.UpdatedAt.Nanosecond(), 0)
						assert.NotNil(host.Log)
					}).Return(nil).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.AnnounceHost(context.Background(), req))
				assert.Equal(host.ID, req.Host.Id)
				assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
				assert.Equal(host.Hostname, req.Host.Hostname)
				assert.Equal(host.IP, req.Host.Ip)
				assert.Equal(host.Port, req.Host.Port)
				assert.Equal(host.DownloadPort, req.Host.DownloadPort)
				assert.Equal(host.DisableShared, req.Host.DisableShared)
				assert.Equal(host.OS, req.Host.Os)
				assert.Equal(host.Platform, req.Host.Platform)
				assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
				assert.Equal(host.KernelVersion, req.Host.KernelVersion)
				assert.EqualValues(host.CPU, mockCPU)
				assert.EqualValues(host.Memory, mockMemory)
				assert.EqualValues(host.Network, mockNetwork)
				assert.EqualValues(host.Disk, mockDisk)
				assert.EqualValues(host.Build, mockBuild)
				assert.EqualValues(host.AnnounceInterval, mockInterval)
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(200))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEqual(host.CreatedAt.Load().Nanosecond(), 0)
				assert.NotEqual(host.UpdatedAt.Load().Nanosecond(), 0)
				assert.NotNil(host.Log)
			},
		},
		{
			name: "host not found and persistent cache host not found, store persistent cache host failed",
			req: &schedulerv2.AnnounceHostRequest{
				Host: &commonv2.Host{
					Id:              mockHostID,
					Type:            uint32(pkgtypes.HostTypeNormal),
					Hostname:        "hostname",
					Ip:              "127.0.0.1",
					Port:            8003,
					DownloadPort:    8001,
					DisableShared:   true,
					Os:              "darwin",
					Platform:        "darwin",
					PlatformFamily:  "Standalone Workstation",
					PlatformVersion: "11.1",
					KernelVersion:   "20.2.0",
					Cpu: &commonv2.CPU{
						LogicalCount:   mockCPU.LogicalCount,
						PhysicalCount:  mockCPU.PhysicalCount,
						Percent:        mockCPU.Percent,
						ProcessPercent: mockCPU.ProcessPercent,
						Times: &commonv2.CPUTimes{
							User:      mockCPU.Times.User,
							System:    mockCPU.Times.System,
							Idle:      mockCPU.Times.Idle,
							Nice:      mockCPU.Times.Nice,
							Iowait:    mockCPU.Times.Iowait,
							Irq:       mockCPU.Times.Irq,
							Softirq:   mockCPU.Times.Softirq,
							Steal:     mockCPU.Times.Steal,
							Guest:     mockCPU.Times.Guest,
							GuestNice: mockCPU.Times.GuestNice,
						},
					},
					Memory: &commonv2.Memory{
						Total:              mockMemory.Total,
						Available:          mockMemory.Available,
						Used:               mockMemory.Used,
						UsedPercent:        mockMemory.UsedPercent,
						ProcessUsedPercent: mockMemory.ProcessUsedPercent,
						Free:               mockMemory.Free,
					},
					Network: &commonv2.Network{
						TcpConnectionCount:       mockNetwork.TCPConnectionCount,
						UploadTcpConnectionCount: mockNetwork.UploadTCPConnectionCount,
						Location:                 &mockNetwork.Location,
						Idc:                      &mockNetwork.IDC,
						DownloadRate:             mockNetwork.DownloadRate,
						DownloadRateLimit:        mockNetwork.DownloadRateLimit,
						UploadRate:               mockNetwork.UploadRate,
						UploadRateLimit:          mockNetwork.UploadRateLimit,
					},
					Disk: &commonv2.Disk{
						Total:             mockDisk.Total,
						Free:              mockDisk.Free,
						Used:              mockDisk.Used,
						UsedPercent:       mockDisk.UsedPercent,
						InodesTotal:       mockDisk.InodesTotal,
						InodesUsed:        mockDisk.InodesUsed,
						InodesFree:        mockDisk.InodesFree,
						InodesUsedPercent: mockDisk.InodesUsedPercent,
					},
					Build: &commonv2.Build{
						GitVersion: mockBuild.GitVersion,
						GitCommit:  &mockBuild.GitCommit,
						GoVersion:  &mockBuild.GoVersion,
						Platform:   &mockBuild.Platform,
					},
				},
				Interval: durationpb.New(5 * time.Minute),
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.AnnounceHostRequest, host *standard.Host, persistentCacheHost *persistentcache.Host, hostManager standard.HostManager, persistentCacheHostManager persistentcache.HostManager, mr *standard.MockResourceMockRecorder, mpr *persistentcache.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder, mph *persistentcache.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					md.GetSchedulerClusterClientConfig().Return(managertypes.SchedulerClusterClientConfig{LoadLimit: 10}, nil).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Store(gomock.Any()).Do(func(host *standard.Host) {
						assert := assert.New(t)
						assert.Equal(host.ID, req.Host.Id)
						assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
						assert.Equal(host.Hostname, req.Host.Hostname)
						assert.Equal(host.IP, req.Host.Ip)
						assert.Equal(host.Port, req.Host.Port)
						assert.Equal(host.DownloadPort, req.Host.DownloadPort)
						assert.Equal(host.DisableShared, req.Host.DisableShared)
						assert.Equal(host.OS, req.Host.Os)
						assert.Equal(host.Platform, req.Host.Platform)
						assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
						assert.Equal(host.KernelVersion, req.Host.KernelVersion)
						assert.EqualValues(host.CPU, mockCPU)
						assert.EqualValues(host.Memory, mockMemory)
						assert.EqualValues(host.Network, mockNetwork)
						assert.EqualValues(host.Disk, mockDisk)
						assert.EqualValues(host.Build, mockBuild)
						assert.EqualValues(host.AnnounceInterval, mockInterval)
						assert.Equal(host.ConcurrentUploadLimit.Load(), int32(10))
						assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
						assert.Equal(host.UploadCount.Load(), int64(0))
						assert.Equal(host.UploadFailedCount.Load(), int64(0))
						assert.NotNil(host.Peers)
						assert.Equal(host.PeerCount.Load(), int32(0))
						assert.NotEqual(host.CreatedAt.Load().Nanosecond(), 0)
						assert.NotEqual(host.UpdatedAt.Load().Nanosecond(), 0)
						assert.NotNil(host.Log)
					}).Return().Times(1),
					mpr.HostManager().Return(persistentCacheHostManager).Times(1),
					mph.Load(gomock.Any(), gomock.Any()).Return(nil, false).Times(1),
					mpr.HostManager().Return(persistentCacheHostManager).Times(1),
					mph.Store(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, host *persistentcache.Host) {
						assert := assert.New(t)
						assert.Equal(host.ID, req.Host.Id)
						assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
						assert.Equal(host.Hostname, req.Host.Hostname)
						assert.Equal(host.IP, req.Host.Ip)
						assert.Equal(host.Port, req.Host.Port)
						assert.Equal(host.DownloadPort, req.Host.DownloadPort)
						assert.Equal(host.DisableShared, req.Host.DisableShared)
						assert.Equal(host.OS, req.Host.Os)
						assert.Equal(host.Platform, req.Host.Platform)
						assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
						assert.Equal(host.KernelVersion, req.Host.KernelVersion)
						assert.EqualValues(host.CPU, mockPersistentCacheCPU)
						assert.EqualValues(host.Memory, mockPersistentCacheMemory)
						assert.EqualValues(host.Network, mockPersistentCacheNetwork)
						assert.EqualValues(host.Disk, mockPersistentCacheDisk)
						assert.EqualValues(host.Build, mockPersistentCacheBuild)
						assert.EqualValues(host.AnnounceInterval, mockPersistentCacheInterval)
						assert.Equal(host.ConcurrentUploadLimit, int32(10))
						assert.Equal(host.ConcurrentUploadCount, int32(0))
						assert.Equal(host.UploadCount, int64(0))
						assert.Equal(host.UploadFailedCount, int64(0))
						assert.NotEqual(host.CreatedAt.Nanosecond(), 0)
						assert.NotEqual(host.UpdatedAt.Nanosecond(), 0)
						assert.NotNil(host.Log)
					}).Return(errors.New("bar")).Times(1),
				)

				assert := assert.New(t)
				assert.Error(svc.AnnounceHost(context.Background(), req))
			},
		},
		{
			name: "host already exists and persistent cache host already exists, store persistent cache host failed",
			req: &schedulerv2.AnnounceHostRequest{
				Host: &commonv2.Host{
					Id:              mockHostID,
					Type:            uint32(pkgtypes.HostTypeNormal),
					Hostname:        "foo",
					Ip:              "127.0.0.1",
					Port:            8003,
					DownloadPort:    8001,
					DisableShared:   false,
					Os:              "darwin",
					Platform:        "darwin",
					PlatformFamily:  "Standalone Workstation",
					PlatformVersion: "11.1",
					KernelVersion:   "20.2.0",
					Cpu: &commonv2.CPU{
						LogicalCount:   mockCPU.LogicalCount,
						PhysicalCount:  mockCPU.PhysicalCount,
						Percent:        mockCPU.Percent,
						ProcessPercent: mockCPU.ProcessPercent,
						Times: &commonv2.CPUTimes{
							User:      mockCPU.Times.User,
							System:    mockCPU.Times.System,
							Idle:      mockCPU.Times.Idle,
							Nice:      mockCPU.Times.Nice,
							Iowait:    mockCPU.Times.Iowait,
							Irq:       mockCPU.Times.Irq,
							Softirq:   mockCPU.Times.Softirq,
							Steal:     mockCPU.Times.Steal,
							Guest:     mockCPU.Times.Guest,
							GuestNice: mockCPU.Times.GuestNice,
						},
					},
					Memory: &commonv2.Memory{
						Total:              mockMemory.Total,
						Available:          mockMemory.Available,
						Used:               mockMemory.Used,
						UsedPercent:        mockMemory.UsedPercent,
						ProcessUsedPercent: mockMemory.ProcessUsedPercent,
						Free:               mockMemory.Free,
					},
					Network: &commonv2.Network{
						TcpConnectionCount:       mockNetwork.TCPConnectionCount,
						UploadTcpConnectionCount: mockNetwork.UploadTCPConnectionCount,
						Location:                 &mockNetwork.Location,
						Idc:                      &mockNetwork.IDC,
						DownloadRate:             mockNetwork.DownloadRate,
						DownloadRateLimit:        mockNetwork.DownloadRateLimit,
						UploadRate:               mockNetwork.UploadRate,
						UploadRateLimit:          mockNetwork.UploadRateLimit,
					},
					Disk: &commonv2.Disk{
						Total:             mockDisk.Total,
						Free:              mockDisk.Free,
						Used:              mockDisk.Used,
						UsedPercent:       mockDisk.UsedPercent,
						InodesTotal:       mockDisk.InodesTotal,
						InodesUsed:        mockDisk.InodesUsed,
						InodesFree:        mockDisk.InodesFree,
						InodesUsedPercent: mockDisk.InodesUsedPercent,
					},
					Build: &commonv2.Build{
						GitVersion: mockBuild.GitVersion,
						GitCommit:  &mockBuild.GitCommit,
						GoVersion:  &mockBuild.GoVersion,
						Platform:   &mockBuild.Platform,
					},
				},
				Interval: durationpb.New(5 * time.Minute),
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.AnnounceHostRequest, host *standard.Host, persistentCacheHost *persistentcache.Host, hostManager standard.HostManager, persistentCacheHostManager persistentcache.HostManager, mr *standard.MockResourceMockRecorder, mpr *persistentcache.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder, mph *persistentcache.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					md.GetSchedulerClusterClientConfig().Return(managertypes.SchedulerClusterClientConfig{}, errors.New("foo")).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mpr.HostManager().Return(persistentCacheHostManager).Times(1),
					mph.Load(gomock.Any(), gomock.Any()).Return(persistentCacheHost, true).Times(1),
					mpr.HostManager().Return(persistentCacheHostManager).Times(1),
					mph.Store(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, host *persistentcache.Host) {
						assert := assert.New(t)
						assert.Equal(host.ID, req.Host.Id)
						assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
						assert.Equal(host.Hostname, req.Host.Hostname)
						assert.Equal(host.IP, req.Host.Ip)
						assert.Equal(host.Port, req.Host.Port)
						assert.Equal(host.DownloadPort, req.Host.DownloadPort)
						assert.Equal(host.DisableShared, req.Host.DisableShared)
						assert.Equal(host.OS, req.Host.Os)
						assert.Equal(host.Platform, req.Host.Platform)
						assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
						assert.Equal(host.KernelVersion, req.Host.KernelVersion)
						assert.EqualValues(host.CPU, mockPersistentCacheCPU)
						assert.EqualValues(host.Memory, mockPersistentCacheMemory)
						assert.EqualValues(host.Network, mockPersistentCacheNetwork)
						assert.EqualValues(host.Disk, mockPersistentCacheDisk)
						assert.EqualValues(host.Build, mockPersistentCacheBuild)
						assert.EqualValues(host.AnnounceInterval, mockPersistentCacheInterval)
						assert.Equal(host.ConcurrentUploadLimit, int32(200))
						assert.Equal(host.ConcurrentUploadCount, int32(0))
						assert.Equal(host.UploadCount, int64(0))
						assert.Equal(host.UploadFailedCount, int64(0))
						assert.NotEqual(host.CreatedAt.Nanosecond(), 0)
						assert.NotEqual(host.UpdatedAt.Nanosecond(), 0)
						assert.NotNil(host.Log)
					}).Return(errors.New("bar")).Times(1),
				)

				assert := assert.New(t)
				assert.Error(svc.AnnounceHost(context.Background(), req))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			hostManager := standard.NewMockHostManager(ctl)
			persistentcacheHostManager := persistentcache.NewMockHostManager(ctl)
			host := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			persistentCacheHost := persistentcache.NewHost(
				mockRawPersistentCacheHost.ID, mockRawPersistentCacheHost.Hostname, mockRawPersistentCacheHost.IP,
				mockRawPersistentCacheHost.OS, mockRawPersistentCacheHost.Platform, mockRawPersistentCacheHost.PlatformFamily, mockRawPersistentCacheHost.PlatformVersion, mockRawPersistentCacheHost.KernelVersion,
				mockRawPersistentCacheHost.Port, mockRawPersistentCacheHost.DownloadPort, mockRawPersistentCacheHost.ConcurrentUploadCount,
				mockRawPersistentCacheHost.UploadCount, mockRawPersistentCacheHost.UploadFailedCount, mockRawPersistentCacheHost.DisableShared, pkgtypes.HostType(mockRawPersistentCacheHost.Type),
				mockRawPersistentCacheHost.CPU, mockRawPersistentCacheHost.Memory, mockRawPersistentCacheHost.Network, mockRawPersistentCacheHost.Disk,
				mockRawPersistentCacheHost.Build, mockRawPersistentCacheHost.AnnounceInterval, mockRawPersistentCacheHost.CreatedAt, mockRawPersistentCacheHost.UpdatedAt, mockRawHost.Log)

			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, tc.req, host, persistentCacheHost, hostManager, persistentcacheHostManager, resource.EXPECT(), persistentCacheResource.EXPECT(), hostManager.EXPECT(), persistentcacheHostManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV2_ListHosts(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(host *standard.Host, hostManager standard.HostManager, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder)
		expect func(t *testing.T, host *standard.Host, resp []*commonv2.Host, err error)
	}{
		{
			name: "host manager is empty",
			mock: func(host *standard.Host, hostManager standard.HostManager, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Range(gomock.Any()).Do(func(f func(key, value any) bool) {
						f(nil, nil)
					}).Return().Times(1),
				)
			},
			expect: func(t *testing.T, host *standard.Host, resp []*commonv2.Host, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(len(resp), 0)
			},
		},
		{
			name: "host manager is not empty",
			mock: func(host *standard.Host, hostManager standard.HostManager, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Range(gomock.Any()).Do(func(f func(key, value any) bool) {
						f(nil, host)
					}).Return().Times(1),
				)
			},
			expect: func(t *testing.T, host *standard.Host, resp []*commonv2.Host, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(len(resp), 1)
				assert.EqualValues(resp[0], &commonv2.Host{
					Id:           mockHostID,
					Type:         uint32(pkgtypes.HostTypeNormal),
					Hostname:     "foo",
					Ip:           "127.0.0.1",
					Port:         8003,
					DownloadPort: mockRawHost.DownloadPort,
					Cpu: &commonv2.CPU{
						LogicalCount:   mockCPU.LogicalCount,
						PhysicalCount:  mockCPU.PhysicalCount,
						Percent:        mockCPU.Percent,
						ProcessPercent: mockCPU.ProcessPercent,
						Times: &commonv2.CPUTimes{
							User:      mockCPU.Times.User,
							System:    mockCPU.Times.System,
							Idle:      mockCPU.Times.Idle,
							Nice:      mockCPU.Times.Nice,
							Iowait:    mockCPU.Times.Iowait,
							Irq:       mockCPU.Times.Irq,
							Softirq:   mockCPU.Times.Softirq,
							Steal:     mockCPU.Times.Steal,
							Guest:     mockCPU.Times.Guest,
							GuestNice: mockCPU.Times.GuestNice,
						},
					},
					Memory: &commonv2.Memory{
						Total:              mockMemory.Total,
						Available:          mockMemory.Available,
						Used:               mockMemory.Used,
						UsedPercent:        mockMemory.UsedPercent,
						ProcessUsedPercent: mockMemory.ProcessUsedPercent,
						Free:               mockMemory.Free,
					},
					Network: &commonv2.Network{
						TcpConnectionCount:       mockNetwork.TCPConnectionCount,
						UploadTcpConnectionCount: mockNetwork.UploadTCPConnectionCount,
						Location:                 &mockNetwork.Location,
						Idc:                      &mockNetwork.IDC,
						DownloadRate:             mockNetwork.DownloadRate,
						DownloadRateLimit:        mockNetwork.DownloadRateLimit,
						UploadRate:               mockNetwork.UploadRate,
						UploadRateLimit:          mockNetwork.UploadRateLimit,
					},
					Disk: &commonv2.Disk{
						Total:             mockDisk.Total,
						Free:              mockDisk.Free,
						Used:              mockDisk.Used,
						UsedPercent:       mockDisk.UsedPercent,
						InodesTotal:       mockDisk.InodesTotal,
						InodesUsed:        mockDisk.InodesUsed,
						InodesFree:        mockDisk.InodesFree,
						InodesUsedPercent: mockDisk.InodesUsedPercent,
					},
					Build: &commonv2.Build{
						GitVersion: mockBuild.GitVersion,
						GitCommit:  &mockBuild.GitCommit,
						GoVersion:  &mockBuild.GoVersion,
						Platform:   &mockBuild.Platform,
					},
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			hostManager := standard.NewMockHostManager(ctl)
			host := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname, mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type,
				standard.WithCPU(mockCPU), standard.WithMemory(mockMemory), standard.WithNetwork(mockNetwork), standard.WithDisk(mockDisk), standard.WithBuild(mockBuild))
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.mock(host, hostManager, resource.EXPECT(), hostManager.EXPECT())
			resp, err := svc.ListHosts(context.Background())
			tc.expect(t, host, resp.Hosts, err)
		})
	}
}

func TestServiceV2_DeleteHost(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(host *standard.Host, mockPeer *standard.Peer, hostManager standard.HostManager, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder)
		expect func(t *testing.T, peer *standard.Peer, err error)
	}{
		{
			name: "host not found",
			mock: func(host *standard.Host, mockPeer *standard.Peer, hostManager standard.HostManager, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *standard.Peer, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name: "host has not peers",
			mock: func(host *standard.Host, mockPeer *standard.Peer, hostManager standard.HostManager, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *standard.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "peer leaves succeeded",
			mock: func(host *standard.Host, mockPeer *standard.Peer, hostManager standard.HostManager, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder) {
				host.Peers.Store(mockPeer.ID, mockPeer)
				mockPeer.FSM.SetState(standard.PeerStatePending)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Delete(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, peer *standard.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(peer.FSM.Current(), standard.PeerStateLeave)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			hostManager := standard.NewMockHostManager(ctl)
			host := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			mockPeer := standard.NewPeer(mockSeedPeerID, mockTask, host)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.mock(host, mockPeer, hostManager, resource.EXPECT(), hostManager.EXPECT())
			tc.expect(t, mockPeer, svc.DeleteHost(context.Background(), &schedulerv2.DeleteHostRequest{HostId: mockHostID}))
		})
	}
}

func TestServiceV2_handleRegisterPeerRequest(t *testing.T) {
	dgst := mockTaskDigest.String()

	tests := []struct {
		name string
		req  *schedulerv2.RegisterPeerRequest
		run  func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *standard.Peer, seedPeer *standard.Peer, hostManager standard.HostManager, taskManager standard.TaskManager,
			peerManager standard.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
			mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder)
	}{
		{
			name: "host not found",
			req:  &schedulerv2.RegisterPeerRequest{},
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *standard.Peer, seedPeer *standard.Peer, hostManager standard.HostManager, taskManager standard.TaskManager,
				peerManager standard.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(peer.Host.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleRegisterPeerRequest(context.Background(), stream, peer.Host.ID, peer.Task.ID, peer.ID, req),
					status.Errorf(codes.NotFound, "host %s not found", peer.Host.ID))
			},
		},
		{
			name: "can not found available peer and download task failed",
			req: &schedulerv2.RegisterPeerRequest{
				Download: &commonv2.Download{
					Digest: &dgst,
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *standard.Peer, seedPeer *standard.Peer, hostManager standard.HostManager, taskManager standard.TaskManager,
				peerManager standard.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(peer.Host.ID)).Return(peer.Host, true).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(peer.Task.ID)).Return(peer.Task, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.Priority = commonv2.Priority_LEVEL1

				assert := assert.New(t)
				assert.ErrorIs(svc.handleRegisterPeerRequest(context.Background(), stream, peer.Host.ID, peer.Task.ID, peer.ID, req),
					status.Errorf(codes.FailedPrecondition, "%s peer is forbidden", commonv2.Priority_LEVEL1.String()))
			},
		},
		{
			name: "task state is TaskStateFailed and download task failed",
			req: &schedulerv2.RegisterPeerRequest{
				Download: &commonv2.Download{
					Digest: &dgst,
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *standard.Peer, seedPeer *standard.Peer, hostManager standard.HostManager, taskManager standard.TaskManager,
				peerManager standard.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(peer.Host.ID)).Return(peer.Host, true).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(peer.Task.ID)).Return(peer.Task, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.Priority = commonv2.Priority_LEVEL1
				peer.Task.FSM.SetState(standard.TaskStateFailed)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(seedPeer)
				seedPeer.FSM.SetState(standard.PeerStateRunning)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleRegisterPeerRequest(context.Background(), stream, peer.Host.ID, peer.Task.ID, peer.ID, req),
					status.Errorf(codes.FailedPrecondition, "%s peer is forbidden", commonv2.Priority_LEVEL1.String()))
			},
		},
		{
			name: "size scope is SizeScope_EMPTY and load AnnouncePeerStream failed",
			req: &schedulerv2.RegisterPeerRequest{
				Download: &commonv2.Download{
					Digest: &dgst,
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *standard.Peer, seedPeer *standard.Peer, hostManager standard.HostManager, taskManager standard.TaskManager,
				peerManager standard.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(peer.Host.ID)).Return(peer.Host, true).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(peer.Task.ID)).Return(peer.Task, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.Task.ContentLength.Store(0)
				peer.Priority = commonv2.Priority_LEVEL6

				assert := assert.New(t)
				assert.ErrorIs(svc.handleRegisterPeerRequest(context.Background(), nil, peer.Host.ID, peer.Task.ID, peer.ID, req),
					status.Error(codes.NotFound, "AnnouncePeerStream not found"))
				assert.Equal(peer.FSM.Current(), standard.PeerStatePending)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStateRunning)
			},
		},
		{
			name: "size scope is SizeScope_EMPTY and event PeerEventRegisterEmpty failed",
			req: &schedulerv2.RegisterPeerRequest{
				Download: &commonv2.Download{
					Digest: &dgst,
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *standard.Peer, seedPeer *standard.Peer, hostManager standard.HostManager, taskManager standard.TaskManager,
				peerManager standard.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(peer.Host.ID)).Return(peer.Host, true).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(peer.Task.ID)).Return(peer.Task, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.Task.ContentLength.Store(0)
				peer.Priority = commonv2.Priority_LEVEL6
				peer.StoreAnnouncePeerStream(stream)
				peer.FSM.SetState(standard.PeerStateReceivedEmpty)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleRegisterPeerRequest(context.Background(), nil, peer.Host.ID, peer.Task.ID, peer.ID, req),
					status.Errorf(codes.Internal, "event RegisterEmpty inappropriate in current state ReceivedEmpty"))
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStateRunning)
			},
		},
		{
			name: "size scope is SizeScope_EMPTY and send EmptyTaskResponse failed",
			req: &schedulerv2.RegisterPeerRequest{
				Download: &commonv2.Download{
					Digest: &dgst,
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *standard.Peer, seedPeer *standard.Peer, hostManager standard.HostManager, taskManager standard.TaskManager,
				peerManager standard.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(peer.Host.ID)).Return(peer.Host, true).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(peer.Task.ID)).Return(peer.Task, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					ma.Send(gomock.Eq(&schedulerv2.AnnouncePeerResponse{
						Response: &schedulerv2.AnnouncePeerResponse_EmptyTaskResponse{
							EmptyTaskResponse: &schedulerv2.EmptyTaskResponse{},
						},
					})).Return(errors.New("foo")).Times(1),
				)

				peer.Task.ContentLength.Store(0)
				peer.Priority = commonv2.Priority_LEVEL6
				peer.StoreAnnouncePeerStream(stream)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleRegisterPeerRequest(context.Background(), nil, peer.Host.ID, peer.Task.ID, peer.ID, req),
					status.Errorf(codes.Internal, "foo"))
				assert.Equal(peer.FSM.Current(), standard.PeerStateReceivedEmpty)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStateRunning)
			},
		},
		{
			name: "size scope is SizeScope_NORMAL and need back-to-source",
			req: &schedulerv2.RegisterPeerRequest{
				Download: &commonv2.Download{
					Digest:           &dgst,
					NeedBackToSource: true,
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *standard.Peer, seedPeer *standard.Peer, hostManager standard.HostManager, taskManager standard.TaskManager,
				peerManager standard.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(peer.Host.ID)).Return(peer.Host, true).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(peer.Task.ID)).Return(peer.Task, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					ms.ScheduleCandidateParents(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1),
				)

				peer.Task.ContentLength.Store(129)
				peer.Task.TotalPieceCount.Store(2)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(seedPeer)
				peer.Priority = commonv2.Priority_LEVEL6
				peer.NeedBackToSource.Store(true)
				peer.StoreAnnouncePeerStream(stream)

				assert := assert.New(t)
				assert.NoError(svc.handleRegisterPeerRequest(context.Background(), nil, peer.Host.ID, peer.Task.ID, peer.ID, req))
				assert.Equal(peer.FSM.Current(), standard.PeerStateReceivedNormal)
				assert.Equal(peer.NeedBackToSource.Load(), true)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStateRunning)
			},
		},
		{
			name: "size scope is SizeScope_NORMAL",
			req: &schedulerv2.RegisterPeerRequest{
				Download: &commonv2.Download{
					Digest: &dgst,
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *standard.Peer, seedPeer *standard.Peer, hostManager standard.HostManager, taskManager standard.TaskManager,
				peerManager standard.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(peer.Host.ID)).Return(peer.Host, true).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(peer.Task.ID)).Return(peer.Task, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					ms.ScheduleCandidateParents(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1),
				)

				peer.Task.ContentLength.Store(129)
				peer.Task.TotalPieceCount.Store(2)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(seedPeer)
				peer.Priority = commonv2.Priority_LEVEL6
				peer.StoreAnnouncePeerStream(stream)

				assert := assert.New(t)
				assert.NoError(svc.handleRegisterPeerRequest(context.Background(), nil, peer.Host.ID, peer.Task.ID, peer.ID, req))
				assert.Equal(peer.FSM.Current(), standard.PeerStateReceivedNormal)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStateRunning)
			},
		},
		{
			name: "size scope is SizeScope_UNKNOW",
			req: &schedulerv2.RegisterPeerRequest{
				Download: &commonv2.Download{
					Digest: &dgst,
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *standard.Peer, seedPeer *standard.Peer, hostManager standard.HostManager, taskManager standard.TaskManager,
				peerManager standard.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(peer.Host.ID)).Return(peer.Host, true).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(peer.Task.ID)).Return(peer.Task, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					ms.ScheduleCandidateParents(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1),
				)

				peer.Priority = commonv2.Priority_LEVEL6

				assert := assert.New(t)
				assert.NoError(svc.handleRegisterPeerRequest(context.Background(), nil, peer.Host.ID, peer.Task.ID, peer.ID, req))
				assert.Equal(peer.FSM.Current(), standard.PeerStateReceivedNormal)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStateRunning)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			hostManager := standard.NewMockHostManager(ctl)
			peerManager := standard.NewMockPeerManager(ctl)
			taskManager := standard.NewMockTaskManager(ctl)
			stream := schedulerv2mocks.NewMockScheduler_AnnouncePeerServer(ctl)

			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockPeerID, mockTask, mockHost)
			seedPeer := standard.NewPeer(mockSeedPeerID, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, tc.req, peer, seedPeer, hostManager, taskManager, peerManager, stream, resource.EXPECT(), hostManager.EXPECT(), taskManager.EXPECT(), peerManager.EXPECT(), stream.EXPECT(), scheduling.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPeerStartedRequest(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerStartedRequest(context.Background(), peer.ID), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
			},
		},
		{
			name: "peer state is PeerStateRunning",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateRunning)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerStartedRequest(context.Background(), peer.ID))
			},
		},
		{
			name: "task state is TaskStateRunning",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateReceivedNormal)
				peer.Task.FSM.SetState(standard.TaskStateRunning)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerStartedRequest(context.Background(), peer.ID))
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.NotEqual(peer.Task.UpdatedAt.Load(), 0)
			},
		},
		{
			name: "task state is TaskStatePending",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateReceivedNormal)
				peer.Task.FSM.SetState(standard.TaskStatePending)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerStartedRequest(context.Background(), peer.ID))
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.NotEqual(peer.Task.UpdatedAt.Load(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			peerManager := standard.NewMockPeerManager(ctl)

			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, peer, peerManager, resource.EXPECT(), peerManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPeerBackToSourceStartedRequest(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceStartedRequest(context.Background(), peer.ID), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
			},
		},
		{
			name: "peer state is PeerStateRunning",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateBackToSource)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceStartedRequest(context.Background(), peer.ID), status.Error(codes.Internal, "event DownloadBackToSource inappropriate in current state BackToSource"))
			},
		},
		{
			name: "task state is TaskStateRunning",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateReceivedNormal)
				peer.Task.FSM.SetState(standard.TaskStateRunning)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerBackToSourceStartedRequest(context.Background(), peer.ID))
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.NotEqual(peer.Task.UpdatedAt.Load(), 0)
			},
		},
		{
			name: "task state is TaskStatePending",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateReceivedNormal)
				peer.Task.FSM.SetState(standard.TaskStatePending)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerBackToSourceStartedRequest(context.Background(), peer.ID))
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.NotEqual(peer.Task.UpdatedAt.Load(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			peerManager := standard.NewMockPeerManager(ctl)

			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, peer, peerManager, resource.EXPECT(), peerManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV2_handleRescheduleRequest(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
			mp *standard.MockPeerManagerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleRescheduleRequest(context.Background(), peer.ID, []*commonv2.Peer{}), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
			},
		},
		{
			name: "reschedule failed",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					ms.ScheduleCandidateParents(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("foo")).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleRescheduleRequest(context.Background(), peer.ID, []*commonv2.Peer{}), status.Error(codes.FailedPrecondition, "foo"))
			},
		},
		{
			name: "reschedule succeeded",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					ms.ScheduleCandidateParents(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.handleRescheduleRequest(context.Background(), peer.ID, []*commonv2.Peer{}))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			peerManager := standard.NewMockPeerManager(ctl)

			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, peer, peerManager, resource.EXPECT(), peerManager.EXPECT(), scheduling.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPeerFinishedRequest(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerFinishedRequest(context.Background(), peer.ID), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
			},
		},
		{
			name: "peer state is PeerStateSucceeded",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateSucceeded)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerFinishedRequest(context.Background(), peer.ID), status.Error(codes.Internal, "event DownloadSucceeded inappropriate in current state Succeeded"))
				assert.NotEqual(peer.Cost.Load(), 0)
			},
		},
		{
			name: "peer state is PeerStateRunning",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateRunning)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerFinishedRequest(context.Background(), peer.ID))
				assert.Equal(peer.FSM.Current(), standard.PeerStateSucceeded)
				assert.NotEqual(peer.Cost.Load(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			peerManager := standard.NewMockPeerManager(ctl)

			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, peer, peerManager, resource.EXPECT(), peerManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPeerBackToSourceFinishedRequest(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write([]byte{1}); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer s.Close()

	tests := []struct {
		name string
		req  *schedulerv2.DownloadPeerBackToSourceFinishedRequest
		run  func(t *testing.T, svc *V2, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
			mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			req:  &schedulerv2.DownloadPeerBackToSourceFinishedRequest{},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceFinishedRequest(context.Background(), peer.ID, req), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
				assert.Equal(peer.Task.ContentLength.Load(), int64(-1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(0))
				assert.Equal(len(peer.Task.DirectPiece), 0)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStatePending)
			},
		},
		{
			name: "peer state is PeerStateSucceeded",
			req:  &schedulerv2.DownloadPeerBackToSourceFinishedRequest{},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateSucceeded)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceFinishedRequest(context.Background(), peer.ID, req), status.Error(codes.Internal, "event DownloadSucceeded inappropriate in current state Succeeded"))
				assert.NotEqual(peer.Cost.Load(), 0)
				assert.Equal(peer.Task.ContentLength.Load(), int64(-1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(0))
				assert.Equal(len(peer.Task.DirectPiece), 0)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStatePending)
			},
		},
		{
			name: "peer has range",
			req:  &schedulerv2.DownloadPeerBackToSourceFinishedRequest{},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateRunning)
				peer.Range = &nethttp.Range{}

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerBackToSourceFinishedRequest(context.Background(), peer.ID, req))
				assert.NotEqual(peer.Cost.Load(), 0)
				assert.Equal(peer.FSM.Current(), standard.PeerStateSucceeded)
				assert.Equal(peer.Task.ContentLength.Load(), int64(-1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(0))
				assert.Equal(len(peer.Task.DirectPiece), 0)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStatePending)
			},
		},
		{
			name: "task state is TaskStateSucceeded",
			req:  &schedulerv2.DownloadPeerBackToSourceFinishedRequest{},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateRunning)
				peer.Task.FSM.SetState(standard.TaskStateSucceeded)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerBackToSourceFinishedRequest(context.Background(), peer.ID, req))
				assert.NotEqual(peer.Cost.Load(), 0)
				assert.Equal(peer.FSM.Current(), standard.PeerStateSucceeded)
				assert.Equal(peer.Task.ContentLength.Load(), int64(-1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(0))
				assert.Equal(len(peer.Task.DirectPiece), 0)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStateSucceeded)
			},
		},
		{
			name: "task state is TaskStatePending",
			req: &schedulerv2.DownloadPeerBackToSourceFinishedRequest{
				ContentLength: 1024,
				PieceCount:    10,
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateRunning)
				peer.Task.FSM.SetState(standard.TaskStatePending)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceFinishedRequest(context.Background(), peer.ID, req), status.Error(codes.Internal, "event DownloadSucceeded inappropriate in current state Pending"))
				assert.NotEqual(peer.Cost.Load(), 0)
				assert.Equal(peer.FSM.Current(), standard.PeerStateSucceeded)
				assert.Equal(peer.Task.ContentLength.Load(), int64(1024))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(10))
				assert.Equal(len(peer.Task.DirectPiece), 0)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStatePending)
			},
		},
		{
			name: "task state is TaskStateRunning",
			req: &schedulerv2.DownloadPeerBackToSourceFinishedRequest{
				ContentLength: 1024,
				PieceCount:    10,
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateRunning)
				peer.Task.FSM.SetState(standard.TaskStateRunning)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerBackToSourceFinishedRequest(context.Background(), peer.ID, req))
				assert.NotEqual(peer.Cost.Load(), 0)
				assert.Equal(peer.FSM.Current(), standard.PeerStateSucceeded)
				assert.Equal(peer.Task.ContentLength.Load(), int64(1024))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(10))
				assert.Equal(len(peer.Task.DirectPiece), 0)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStateSucceeded)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			peerManager := standard.NewMockPeerManager(ctl)

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

			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockHost.IP = ip
			mockHost.DownloadPort = int32(port)

			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, tc.req, peer, peerManager, resource.EXPECT(), peerManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPeerFailedRequest(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerFailedRequest(context.Background(), peer.ID), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
			},
		},
		{
			name: "peer state is PeerEventDownloadFailed",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.FSM.SetState(standard.PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerFailedRequest(context.Background(), peer.ID), status.Error(codes.Internal, "event DownloadFailed inappropriate in current state DownloadFailed"))
			},
		},
		{
			name: "peer state is PeerStateRunning",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateRunning)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerFailedRequest(context.Background(), peer.ID))
				assert.Equal(peer.FSM.Current(), standard.PeerStateFailed)
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			peerManager := standard.NewMockPeerManager(ctl)

			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, peer, peerManager, resource.EXPECT(), peerManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPeerBackToSourceFailedRequest(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				peer.Task.ContentLength.Store(1)
				peer.Task.TotalPieceCount.Store(1)
				peer.Task.DirectPiece = []byte{1}

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceFailedRequest(context.Background(), peer.ID), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
				assert.Equal(peer.FSM.Current(), standard.PeerStatePending)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStatePending)
				assert.Equal(peer.Task.ContentLength.Load(), int64(1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(1))
				assert.Equal(peer.Task.DirectPiece, []byte{1})
			},
		},
		{
			name: "peer state is PeerStateFailed",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateFailed)
				peer.Task.ContentLength.Store(1)
				peer.Task.TotalPieceCount.Store(1)
				peer.Task.DirectPiece = []byte{1}

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceFailedRequest(context.Background(), peer.ID), status.Error(codes.Internal, "event DownloadFailed inappropriate in current state Failed"))
				assert.Equal(peer.FSM.Current(), standard.PeerStateFailed)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStatePending)
				assert.Equal(peer.Task.ContentLength.Load(), int64(1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(1))
				assert.Equal(peer.Task.DirectPiece, []byte{1})
			},
		},
		{
			name: "task state is TaskStateFailed",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateRunning)
				peer.Task.FSM.SetState(standard.TaskStateFailed)
				peer.Task.ContentLength.Store(1)
				peer.Task.TotalPieceCount.Store(1)
				peer.Task.DirectPiece = []byte{1}

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceFailedRequest(context.Background(), peer.ID), status.Error(codes.Internal, "event DownloadFailed inappropriate in current state Failed"))
				assert.Equal(peer.FSM.Current(), standard.PeerStateFailed)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStateFailed)
				assert.Equal(peer.Task.ContentLength.Load(), int64(-1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(0))
				assert.Equal(peer.Task.DirectPiece, []byte{})
			},
		},
		{
			name: "task state is TaskStateRunning",
			run: func(t *testing.T, svc *V2, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(standard.PeerStateRunning)
				peer.Task.FSM.SetState(standard.TaskStateRunning)
				peer.Task.ContentLength.Store(1)
				peer.Task.TotalPieceCount.Store(1)
				peer.Task.DirectPiece = []byte{1}

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerBackToSourceFailedRequest(context.Background(), peer.ID))
				assert.Equal(peer.FSM.Current(), standard.PeerStateFailed)
				assert.Equal(peer.Task.FSM.Current(), standard.TaskStateFailed)
				assert.Equal(peer.Task.ContentLength.Load(), int64(-1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(0))
				assert.Equal(peer.Task.DirectPiece, []byte{})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			peerManager := standard.NewMockPeerManager(ctl)

			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, peer, peerManager, resource.EXPECT(), peerManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPieceFinishedRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *schedulerv2.DownloadPieceFinishedRequest
		run  func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder)
	}{
		{
			name: "invalid digest",
			req: &schedulerv2.DownloadPieceFinishedRequest{
				Piece: &commonv2.Piece{
					Number:      uint32(mockPiece.Number),
					ParentId:    &mockPiece.ParentID,
					Offset:      mockPiece.Offset,
					Length:      mockPiece.Length,
					Digest:      "foo",
					TrafficType: &mockPiece.TrafficType,
					Cost:        durationpb.New(mockPiece.Cost),
					CreatedAt:   timestamppb.New(mockPiece.CreatedAt),
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPieceFinishedRequest(peer.ID, req), status.Error(codes.InvalidArgument, "invalid digest"))
			},
		},
		{
			name: "peer can not be loaded",
			req: &schedulerv2.DownloadPieceFinishedRequest{
				Piece: &commonv2.Piece{
					Number:      uint32(mockPiece.Number),
					ParentId:    &mockPiece.ParentID,
					Offset:      mockPiece.Offset,
					Length:      mockPiece.Length,
					Digest:      mockPiece.Digest.String(),
					TrafficType: &mockPiece.TrafficType,
					Cost:        durationpb.New(mockPiece.Cost),
					CreatedAt:   timestamppb.New(mockPiece.CreatedAt),
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPieceFinishedRequest(peer.ID, req), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
			},
		},
		{
			name: "parent can not be loaded",
			req: &schedulerv2.DownloadPieceFinishedRequest{
				Piece: &commonv2.Piece{
					Number:      uint32(mockPiece.Number),
					ParentId:    &mockPiece.ParentID,
					Offset:      mockPiece.Offset,
					Length:      mockPiece.Length,
					Digest:      mockPiece.Digest.String(),
					TrafficType: &mockPiece.TrafficType,
					Cost:        durationpb.New(mockPiece.Cost),
					CreatedAt:   timestamppb.New(mockPiece.CreatedAt),
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(req.Piece.GetParentId())).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPieceFinishedRequest(peer.ID, req))

				piece, loaded := peer.LoadPiece(int32(req.Piece.Number))
				assert.True(loaded)
				assert.Equal(piece.Number, mockPiece.Number)
				assert.Equal(piece.ParentID, mockPiece.ParentID)
				assert.Equal(piece.Offset, mockPiece.Offset)
				assert.Equal(piece.Length, mockPiece.Length)
				assert.Equal(piece.Digest.String(), mockPiece.Digest.String())
				assert.Equal(piece.TrafficType, mockPiece.TrafficType)
				assert.Equal(piece.Cost, mockPiece.Cost)
				assert.True(piece.CreatedAt.Equal(mockPiece.CreatedAt))
				assert.Equal(peer.FinishedPieces.Count(), uint(1))
				assert.Equal(len(peer.PieceCosts()), 1)
				assert.NotEqual(peer.PieceUpdatedAt.Load(), 0)
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.NotEqual(peer.Task.UpdatedAt.Load(), 0)
			},
		},
		{
			name: "parent can be loaded",
			req: &schedulerv2.DownloadPieceFinishedRequest{
				Piece: &commonv2.Piece{
					Number:      uint32(mockPiece.Number),
					ParentId:    &mockPiece.ParentID,
					Offset:      mockPiece.Offset,
					Length:      mockPiece.Length,
					Digest:      mockPiece.Digest.String(),
					TrafficType: &mockPiece.TrafficType,
					Cost:        durationpb.New(mockPiece.Cost),
					CreatedAt:   timestamppb.New(mockPiece.CreatedAt),
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(req.Piece.GetParentId())).Return(peer, true).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPieceFinishedRequest(peer.ID, req))

				piece, loaded := peer.LoadPiece(int32(req.Piece.Number))
				assert.True(loaded)
				assert.Equal(piece.Number, mockPiece.Number)
				assert.Equal(piece.ParentID, mockPiece.ParentID)
				assert.Equal(piece.Offset, mockPiece.Offset)
				assert.Equal(piece.Length, mockPiece.Length)
				assert.Equal(piece.Digest.String(), mockPiece.Digest.String())
				assert.Equal(piece.TrafficType, mockPiece.TrafficType)
				assert.Equal(piece.Cost, mockPiece.Cost)
				assert.True(piece.CreatedAt.Equal(mockPiece.CreatedAt))
				assert.Equal(peer.FinishedPieces.Count(), uint(1))
				assert.Equal(len(peer.PieceCosts()), 1)
				assert.NotEqual(peer.PieceUpdatedAt.Load(), 0)
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.NotEqual(peer.Task.UpdatedAt.Load(), 0)
				assert.NotEqual(peer.Host.UpdatedAt.Load(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			peerManager := standard.NewMockPeerManager(ctl)

			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, tc.req, peer, peerManager, resource.EXPECT(), peerManager.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPieceBackToSourceFinishedRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *schedulerv2.DownloadPieceBackToSourceFinishedRequest
		run  func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceBackToSourceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder)
	}{
		{
			name: "invalid digest",
			req: &schedulerv2.DownloadPieceBackToSourceFinishedRequest{
				Piece: &commonv2.Piece{
					Number:      uint32(mockPiece.Number),
					ParentId:    &mockPiece.ParentID,
					Offset:      mockPiece.Offset,
					Length:      mockPiece.Length,
					Digest:      "foo",
					TrafficType: &mockPiece.TrafficType,
					Cost:        durationpb.New(mockPiece.Cost),
					CreatedAt:   timestamppb.New(mockPiece.CreatedAt),
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceBackToSourceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPieceBackToSourceFinishedRequest(context.Background(), peer.ID, req), status.Error(codes.InvalidArgument, "invalid digest"))
			},
		},
		{
			name: "peer can not be loaded",
			req: &schedulerv2.DownloadPieceBackToSourceFinishedRequest{
				Piece: &commonv2.Piece{
					Number:      uint32(mockPiece.Number),
					ParentId:    &mockPiece.ParentID,
					Offset:      mockPiece.Offset,
					Length:      mockPiece.Length,
					Digest:      mockPiece.Digest.String(),
					TrafficType: &mockPiece.TrafficType,
					Cost:        durationpb.New(mockPiece.Cost),
					CreatedAt:   timestamppb.New(mockPiece.CreatedAt),
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceBackToSourceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPieceBackToSourceFinishedRequest(context.Background(), peer.ID, req), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
			},
		},
		{
			name: "peer can be loaded",
			req: &schedulerv2.DownloadPieceBackToSourceFinishedRequest{
				Piece: &commonv2.Piece{
					Number:      uint32(mockPiece.Number),
					ParentId:    &mockPiece.ParentID,
					Offset:      mockPiece.Offset,
					Length:      mockPiece.Length,
					Digest:      mockPiece.Digest.String(),
					TrafficType: &mockPiece.TrafficType,
					Cost:        durationpb.New(mockPiece.Cost),
					CreatedAt:   timestamppb.New(mockPiece.CreatedAt),
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceBackToSourceFinishedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPieceBackToSourceFinishedRequest(context.Background(), peer.ID, req))

				piece, loaded := peer.LoadPiece(int32(req.Piece.Number))
				assert.True(loaded)
				assert.Equal(piece.Number, mockPiece.Number)
				assert.Equal(piece.ParentID, mockPiece.ParentID)
				assert.Equal(piece.Offset, mockPiece.Offset)
				assert.Equal(piece.Length, mockPiece.Length)
				assert.Equal(piece.Digest.String(), mockPiece.Digest.String())
				assert.Equal(piece.TrafficType, mockPiece.TrafficType)
				assert.Equal(piece.Cost, mockPiece.Cost)
				assert.True(piece.CreatedAt.Equal(mockPiece.CreatedAt))
				assert.Equal(peer.FinishedPieces.Count(), uint(1))
				assert.Equal(len(peer.PieceCosts()), 1)
				assert.NotEqual(peer.PieceUpdatedAt.Load(), 0)
				assert.NotEqual(peer.UpdatedAt.Load(), 0)

				piece, loaded = peer.Task.LoadPiece(int32(req.Piece.Number))
				assert.True(loaded)
				assert.Equal(piece.Number, mockPiece.Number)
				assert.Equal(piece.ParentID, mockPiece.ParentID)
				assert.Equal(piece.Offset, mockPiece.Offset)
				assert.Equal(piece.Length, mockPiece.Length)
				assert.Equal(piece.Digest.String(), mockPiece.Digest.String())
				assert.Equal(piece.TrafficType, mockPiece.TrafficType)
				assert.Equal(piece.Cost, mockPiece.Cost)
				assert.True(piece.CreatedAt.Equal(mockPiece.CreatedAt))
				assert.NotEqual(peer.Task.UpdatedAt.Load(), 0)
				assert.NotEqual(peer.Host.UpdatedAt.Load(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			peerManager := standard.NewMockPeerManager(ctl)

			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, tc.req, peer, peerManager, resource.EXPECT(), peerManager.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPieceFailedRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *schedulerv2.DownloadPieceFailedRequest
		run  func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFailedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
			mp *standard.MockPeerManagerMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			req: &schedulerv2.DownloadPieceFailedRequest{
				ParentId:  mockSeedPeerID,
				Temporary: true,
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFailedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPieceFailedRequest(context.Background(), peer.ID, req), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
			},
		},
		{
			name: "temporary is false",
			req: &schedulerv2.DownloadPieceFailedRequest{
				ParentId:  mockSeedPeerID,
				Temporary: false,
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFailedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPieceFailedRequest(context.Background(), peer.ID, req), status.Error(codes.FailedPrecondition, "download piece failed"))
			},
		},
		{
			name: "parent can not be loaded",
			req: &schedulerv2.DownloadPieceFailedRequest{
				ParentId:  mockSeedPeerID,
				Temporary: true,
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFailedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(req.GetParentId())).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPieceFailedRequest(context.Background(), peer.ID, req))
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.True(peer.BlockParents.Contains(req.GetParentId()))
				assert.NotEqual(peer.Task.UpdatedAt.Load(), 0)
			},
		},
		{
			name: "parent can be loaded",
			req: &schedulerv2.DownloadPieceFailedRequest{
				ParentId:  mockSeedPeerID,
				Temporary: true,
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFailedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(req.GetParentId())).Return(peer, true).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPieceFailedRequest(context.Background(), peer.ID, req))
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.True(peer.BlockParents.Contains(req.GetParentId()))
				assert.NotEqual(peer.Task.UpdatedAt.Load(), 0)
				assert.Equal(peer.Host.UploadFailedCount.Load(), int64(1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			peerManager := standard.NewMockPeerManager(ctl)

			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, tc.req, peer, peerManager, resource.EXPECT(), peerManager.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPieceBackToSourceFailedRequest(t *testing.T) {
	mockPieceNumber := uint32(mockPiece.Number)

	tests := []struct {
		name string
		req  *schedulerv2.DownloadPieceBackToSourceFailedRequest
		run  func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceBackToSourceFailedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
			mp *standard.MockPeerManagerMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			req:  &schedulerv2.DownloadPieceBackToSourceFailedRequest{},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceBackToSourceFailedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPieceBackToSourceFailedRequest(context.Background(), peer.ID, req), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
			},
		},
		{
			name: "peer can be loaded",
			req: &schedulerv2.DownloadPieceBackToSourceFailedRequest{
				PieceNumber: &mockPieceNumber,
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceBackToSourceFailedRequest, peer *standard.Peer, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder,
				mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPieceBackToSourceFailedRequest(context.Background(), peer.ID, req), status.Error(codes.Internal, "download piece from source failed"))
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.NotEqual(peer.Task.UpdatedAt.Load(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			peerManager := standard.NewMockPeerManager(ctl)

			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, tc.req, peer, peerManager, resource.EXPECT(), peerManager.EXPECT())
		})
	}
}

func TestServiceV2_handleResource(t *testing.T) {
	dgst := mockTaskDigest.String()
	mismatchDgst := "foo"

	tests := []struct {
		name     string
		download *commonv2.Download
		run      func(t *testing.T, svc *V2, download *commonv2.Download, stream schedulerv2.Scheduler_AnnouncePeerServer, mockHost *standard.Host, mockTask *standard.Task, mockPeer *standard.Peer,
			hostManager standard.HostManager, taskManager standard.TaskManager, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
			mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder)
	}{
		{
			name:     "host can not be loaded",
			download: &commonv2.Download{},
			run: func(t *testing.T, svc *V2, download *commonv2.Download, stream schedulerv2.Scheduler_AnnouncePeerServer, mockHost *standard.Host, mockTask *standard.Task, mockPeer *standard.Peer,
				hostManager standard.HostManager, taskManager standard.TaskManager, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				_, _, _, err := svc.handleResource(context.Background(), stream, mockHost.ID, mockTask.ID, mockPeer.ID, download)
				assert.ErrorIs(err, status.Errorf(codes.NotFound, "host %s not found", mockHost.ID))
			},
		},
		{
			name: "task can be loaded",
			download: &commonv2.Download{
				Url:                 "foo",
				FilteredQueryParams: []string{"bar"},
				RequestHeader:       map[string]string{"baz": "bas"},
			},
			run: func(t *testing.T, svc *V2, download *commonv2.Download, stream schedulerv2.Scheduler_AnnouncePeerServer, mockHost *standard.Host, mockTask *standard.Task, mockPeer *standard.Peer,
				hostManager standard.HostManager, taskManager standard.TaskManager, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(mockHost, true).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(mockTask.ID)).Return(mockTask, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeer.ID)).Return(mockPeer, true).Times(1),
				)

				assert := assert.New(t)
				host, task, _, err := svc.handleResource(context.Background(), stream, mockHost.ID, mockTask.ID, mockPeer.ID, download)
				assert.NoError(err)
				assert.EqualValues(host, mockHost)
				assert.Equal(task.ID, mockTask.ID)
				assert.Equal(task.URL, download.Url)
				assert.EqualValues(task.FilteredQueryParams, download.FilteredQueryParams)
				assert.EqualValues(task.Header, download.RequestHeader)
			},
		},
		{
			name: "task can not be loaded",
			download: &commonv2.Download{
				Url:                 "foo",
				FilteredQueryParams: []string{"bar"},
				RequestHeader:       map[string]string{"baz": "bas"},
				Digest:              &dgst,
			},
			run: func(t *testing.T, svc *V2, download *commonv2.Download, stream schedulerv2.Scheduler_AnnouncePeerServer, mockHost *standard.Host, mockTask *standard.Task, mockPeer *standard.Peer,
				hostManager standard.HostManager, taskManager standard.TaskManager, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(mockHost, true).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(mockTask.ID)).Return(nil, false).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Store(gomock.Any()).Return().Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeer.ID)).Return(mockPeer, true).Times(1),
				)

				assert := assert.New(t)
				host, task, _, err := svc.handleResource(context.Background(), stream, mockHost.ID, mockTask.ID, mockPeer.ID, download)
				assert.NoError(err)
				assert.EqualValues(host, mockHost)
				assert.Equal(task.ID, mockTask.ID)
				assert.Equal(task.Digest.String(), download.GetDigest())
				assert.Equal(task.URL, download.GetUrl())
				assert.EqualValues(task.FilteredQueryParams, download.GetFilteredQueryParams())
				assert.EqualValues(task.Header, download.RequestHeader)
			},
		},
		{
			name: "invalid digest",
			download: &commonv2.Download{
				Digest: &mismatchDgst,
			},
			run: func(t *testing.T, svc *V2, download *commonv2.Download, stream schedulerv2.Scheduler_AnnouncePeerServer, mockHost *standard.Host, mockTask *standard.Task, mockPeer *standard.Peer,
				hostManager standard.HostManager, taskManager standard.TaskManager, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(mockHost, true).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(mockTask.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				_, _, _, err := svc.handleResource(context.Background(), stream, mockHost.ID, mockTask.ID, mockPeer.ID, download)
				assert.ErrorIs(err, status.Error(codes.InvalidArgument, "invalid digest"))
			},
		},
		{
			name: "peer can be loaded",
			download: &commonv2.Download{
				Url:                 "foo",
				FilteredQueryParams: []string{"bar"},
				RequestHeader:       map[string]string{"baz": "bas"},
				Digest:              &dgst,
			},
			run: func(t *testing.T, svc *V2, download *commonv2.Download, stream schedulerv2.Scheduler_AnnouncePeerServer, mockHost *standard.Host, mockTask *standard.Task, mockPeer *standard.Peer,
				hostManager standard.HostManager, taskManager standard.TaskManager, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(mockHost, true).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(mockTask.ID)).Return(mockTask, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeer.ID)).Return(mockPeer, true).Times(1),
				)

				assert := assert.New(t)
				host, task, peer, err := svc.handleResource(context.Background(), stream, mockHost.ID, mockTask.ID, mockPeer.ID, download)
				assert.NoError(err)
				assert.EqualValues(host, mockHost)
				assert.Equal(task.ID, mockTask.ID)
				assert.Equal(task.Digest.String(), download.GetDigest())
				assert.Equal(task.URL, download.GetUrl())
				assert.EqualValues(task.FilteredQueryParams, download.GetFilteredQueryParams())
				assert.EqualValues(task.Header, download.RequestHeader)
				assert.EqualValues(peer, mockPeer)
			},
		},
		{
			name: "peer can not be loaded",
			download: &commonv2.Download{
				Url:                 "foo",
				FilteredQueryParams: []string{"bar"},
				RequestHeader:       map[string]string{"baz": "bas"},
				Digest:              &dgst,
				Priority:            commonv2.Priority_LEVEL1,
				Range: &commonv2.Range{
					Start:  uint64(mockPeerRange.Start),
					Length: uint64(mockPeerRange.Length),
				},
			},
			run: func(t *testing.T, svc *V2, download *commonv2.Download, stream schedulerv2.Scheduler_AnnouncePeerServer, mockHost *standard.Host, mockTask *standard.Task, mockPeer *standard.Peer,
				hostManager standard.HostManager, taskManager standard.TaskManager, peerManager standard.PeerManager, mr *standard.MockResourceMockRecorder, mh *standard.MockHostManagerMockRecorder,
				mt *standard.MockTaskManagerMockRecorder, mp *standard.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(mockHost, true).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(mockTask.ID)).Return(mockTask, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(mockPeer.ID)).Return(nil, false).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Store(gomock.Any()).Return().Times(1),
				)

				assert := assert.New(t)
				host, task, peer, err := svc.handleResource(context.Background(), stream, mockHost.ID, mockTask.ID, mockPeer.ID, download)
				assert.NoError(err)
				assert.EqualValues(host, mockHost)
				assert.Equal(task.ID, mockTask.ID)
				assert.Equal(task.Digest.String(), download.GetDigest())
				assert.Equal(task.URL, download.GetUrl())
				assert.EqualValues(task.FilteredQueryParams, download.GetFilteredQueryParams())
				assert.EqualValues(task.Header, download.RequestHeader)
				assert.Equal(peer.ID, mockPeer.ID)
				assert.Equal(peer.Priority, download.Priority)
				assert.Equal(peer.Range.Start, int64(download.Range.Start))
				assert.Equal(peer.Range.Length, int64(download.Range.Length))
				assert.NotNil(peer.AnnouncePeerStream)
				assert.EqualValues(peer.Host, mockHost)
				assert.EqualValues(peer.Task, mockTask)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			hostManager := standard.NewMockHostManager(ctl)
			taskManager := standard.NewMockTaskManager(ctl)
			peerManager := standard.NewMockPeerManager(ctl)
			stream := schedulerv2mocks.NewMockScheduler_AnnouncePeerServer(ctl)

			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			mockPeer := standard.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, tc.download, stream, mockHost, mockTask, mockPeer, hostManager, taskManager, peerManager, resource.EXPECT(), hostManager.EXPECT(), taskManager.EXPECT(), peerManager.EXPECT())
		})
	}
}

func TestServiceV2_downloadTaskBySeedPeer(t *testing.T) {
	tests := []struct {
		name   string
		config config.Config
		run    func(t *testing.T, svc *V2, peer *standard.Peer, seedPeerClient standard.SeedPeer, mr *standard.MockResourceMockRecorder, ms *standard.MockSeedPeerMockRecorder)
	}{
		{
			name: "priority is Priority_LEVEL6 and enable seed peer",
			config: config.Config{
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V2, peer *standard.Peer, seedPeerClient standard.SeedPeer, mr *standard.MockResourceMockRecorder, ms *standard.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()

				gomock.InOrder(
					mr.SeedPeer().Return(seedPeerClient).Times(1),
					ms.TriggerDownloadTask(gomock.All(), gomock.Any(), gomock.Any()).Do(func(context.Context, string, *dfdaemonv2.DownloadTaskRequest) { wg.Done() }).Return(nil).Times(1),
				)

				peer.Priority = commonv2.Priority_LEVEL6

				assert := assert.New(t)
				assert.NoError(svc.downloadTaskBySeedPeer(context.Background(), mockTaskID, &commonv2.Download{}, peer))
				assert.False(peer.NeedBackToSource.Load())
			},
		},
		{
			name: "priority is Priority_LEVEL6, enable seed peer and download task failed",
			config: config.Config{
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V2, peer *standard.Peer, seedPeerClient standard.SeedPeer, mr *standard.MockResourceMockRecorder, ms *standard.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()

				gomock.InOrder(
					mr.SeedPeer().Return(seedPeerClient).Times(1),
					ms.TriggerDownloadTask(gomock.All(), gomock.Any(), gomock.Any()).Do(func(context.Context, string, *dfdaemonv2.DownloadTaskRequest) { wg.Done() }).Return(errors.New("foo")).Times(1),
				)

				peer.Priority = commonv2.Priority_LEVEL6

				assert := assert.New(t)
				assert.NoError(svc.downloadTaskBySeedPeer(context.Background(), mockTaskID, &commonv2.Download{}, peer))
				assert.False(peer.NeedBackToSource.Load())
			},
		},
		{
			name: "priority is Priority_LEVEL6 and disable seed peer",
			config: config.Config{
				SeedPeer: config.SeedPeerConfig{
					Enable: false,
				},
			},
			run: func(t *testing.T, svc *V2, peer *standard.Peer, seedPeerClient standard.SeedPeer, mr *standard.MockResourceMockRecorder, ms *standard.MockSeedPeerMockRecorder) {
				peer.Priority = commonv2.Priority_LEVEL6

				assert := assert.New(t)
				assert.NoError(svc.downloadTaskBySeedPeer(context.Background(), mockTaskID, &commonv2.Download{}, peer))
				assert.True(peer.NeedBackToSource.Load())
			},
		},
		{
			name: "priority is Priority_LEVEL5 and enable seed peer",
			config: config.Config{
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V2, peer *standard.Peer, seedPeerClient standard.SeedPeer, mr *standard.MockResourceMockRecorder, ms *standard.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()

				gomock.InOrder(
					mr.SeedPeer().Return(seedPeerClient).Times(1),
					ms.TriggerDownloadTask(gomock.All(), gomock.Any(), gomock.Any()).Do(func(context.Context, string, *dfdaemonv2.DownloadTaskRequest) { wg.Done() }).Return(nil).Times(1),
				)

				peer.Priority = commonv2.Priority_LEVEL5

				assert := assert.New(t)
				assert.NoError(svc.downloadTaskBySeedPeer(context.Background(), mockTaskID, &commonv2.Download{}, peer))
				assert.False(peer.NeedBackToSource.Load())
			},
		},
		{
			name: "priority is Priority_LEVEL5, enable seed peer and download task failed",
			config: config.Config{
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V2, peer *standard.Peer, seedPeerClient standard.SeedPeer, mr *standard.MockResourceMockRecorder, ms *standard.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()

				gomock.InOrder(
					mr.SeedPeer().Return(seedPeerClient).Times(1),
					ms.TriggerDownloadTask(gomock.All(), gomock.Any(), gomock.Any()).Do(func(context.Context, string, *dfdaemonv2.DownloadTaskRequest) { wg.Done() }).Return(errors.New("foo")).Times(1),
				)

				peer.Priority = commonv2.Priority_LEVEL5

				assert := assert.New(t)
				assert.NoError(svc.downloadTaskBySeedPeer(context.Background(), mockTaskID, &commonv2.Download{}, peer))
				assert.False(peer.NeedBackToSource.Load())
			},
		},
		{
			name: "priority is Priority_LEVEL5 and disable seed peer",
			config: config.Config{
				SeedPeer: config.SeedPeerConfig{
					Enable: false,
				},
			},
			run: func(t *testing.T, svc *V2, peer *standard.Peer, seedPeerClient standard.SeedPeer, mr *standard.MockResourceMockRecorder, ms *standard.MockSeedPeerMockRecorder) {
				peer.Priority = commonv2.Priority_LEVEL5

				assert := assert.New(t)
				assert.NoError(svc.downloadTaskBySeedPeer(context.Background(), mockTaskID, &commonv2.Download{}, peer))
				assert.True(peer.NeedBackToSource.Load())
			},
		},
		{
			name: "priority is Priority_LEVEL4 and enable seed peer",
			config: config.Config{
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V2, peer *standard.Peer, seedPeerClient standard.SeedPeer, mr *standard.MockResourceMockRecorder, ms *standard.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()

				gomock.InOrder(
					mr.SeedPeer().Return(seedPeerClient).Times(1),
					ms.TriggerDownloadTask(gomock.All(), gomock.Any(), gomock.Any()).Do(func(context.Context, string, *dfdaemonv2.DownloadTaskRequest) { wg.Done() }).Return(nil).Times(1),
				)

				peer.Priority = commonv2.Priority_LEVEL4

				assert := assert.New(t)
				assert.NoError(svc.downloadTaskBySeedPeer(context.Background(), mockTaskID, &commonv2.Download{}, peer))
				assert.False(peer.NeedBackToSource.Load())
			},
		},
		{
			name: "priority is Priority_LEVEL4, enable seed peer and download task failed",
			config: config.Config{
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V2, peer *standard.Peer, seedPeerClient standard.SeedPeer, mr *standard.MockResourceMockRecorder, ms *standard.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()

				gomock.InOrder(
					mr.SeedPeer().Return(seedPeerClient).Times(1),
					ms.TriggerDownloadTask(gomock.All(), gomock.Any(), gomock.Any()).Do(func(context.Context, string, *dfdaemonv2.DownloadTaskRequest) { wg.Done() }).Return(errors.New("foo")).Times(1),
				)

				peer.Priority = commonv2.Priority_LEVEL4

				assert := assert.New(t)
				assert.NoError(svc.downloadTaskBySeedPeer(context.Background(), mockTaskID, &commonv2.Download{}, peer))
				assert.False(peer.NeedBackToSource.Load())
			},
		},
		{
			name: "priority is Priority_LEVEL4 and disable seed peer",
			config: config.Config{
				SeedPeer: config.SeedPeerConfig{
					Enable: false,
				},
			},
			run: func(t *testing.T, svc *V2, peer *standard.Peer, seedPeerClient standard.SeedPeer, mr *standard.MockResourceMockRecorder, ms *standard.MockSeedPeerMockRecorder) {
				peer.Priority = commonv2.Priority_LEVEL4

				assert := assert.New(t)
				assert.NoError(svc.downloadTaskBySeedPeer(context.Background(), mockTaskID, &commonv2.Download{}, peer))
				assert.True(peer.NeedBackToSource.Load())
			},
		},
		{
			name: "priority is Priority_LEVEL3",
			config: config.Config{
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V2, peer *standard.Peer, seedPeerClient standard.SeedPeer, mr *standard.MockResourceMockRecorder, ms *standard.MockSeedPeerMockRecorder) {
				peer.Priority = commonv2.Priority_LEVEL3

				assert := assert.New(t)
				assert.NoError(svc.downloadTaskBySeedPeer(context.Background(), mockTaskID, &commonv2.Download{}, peer))
				assert.True(peer.NeedBackToSource.Load())
			},
		},
		{
			name: "priority is Priority_LEVEL2",
			config: config.Config{
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V2, peer *standard.Peer, seedPeerClient standard.SeedPeer, mr *standard.MockResourceMockRecorder, ms *standard.MockSeedPeerMockRecorder) {
				peer.Priority = commonv2.Priority_LEVEL2

				assert := assert.New(t)
				assert.ErrorIs(svc.downloadTaskBySeedPeer(context.Background(), mockTaskID, &commonv2.Download{}, peer), status.Errorf(codes.NotFound, "%s peer not found candidate peers", commonv2.Priority_LEVEL2.String()))
			},
		},
		{
			name: "priority is Priority_LEVEL1",
			config: config.Config{
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V2, peer *standard.Peer, seedPeerClient standard.SeedPeer, mr *standard.MockResourceMockRecorder, ms *standard.MockSeedPeerMockRecorder) {
				peer.Priority = commonv2.Priority_LEVEL1

				assert := assert.New(t)
				assert.ErrorIs(svc.downloadTaskBySeedPeer(context.Background(), mockTaskID, &commonv2.Download{}, peer), status.Errorf(codes.FailedPrecondition, "%s peer is forbidden", commonv2.Priority_LEVEL1.String()))
			},
		},
		{
			name: "priority is Priority_LEVEL0",
			config: config.Config{
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V2, peer *standard.Peer, seedPeerClient standard.SeedPeer, mr *standard.MockResourceMockRecorder, ms *standard.MockSeedPeerMockRecorder) {
				peer.Priority = commonv2.Priority(100)

				assert := assert.New(t)
				assert.ErrorIs(svc.downloadTaskBySeedPeer(context.Background(), mockTaskID, &commonv2.Download{}, peer), status.Errorf(codes.InvalidArgument, "invalid priority %#v", peer.Priority))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			resource := standard.NewMockResource(ctl)
			persistentCacheResource := persistentcache.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			seedPeerClient := standard.NewMockSeedPeer(ctl)

			mockHost := standard.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := standard.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_STANDARD, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, standard.WithDigest(mockTaskDigest), standard.WithPieceLength(mockTaskPieceLength))
			peer := standard.NewPeer(mockPeerID, mockTask, mockHost)
			svc := NewV2(&tc.config, resource, persistentCacheResource, scheduling, dynconfig)

			tc.run(t, svc, peer, seedPeerClient, resource.EXPECT(), seedPeerClient.EXPECT())
		})
	}
}
