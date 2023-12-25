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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
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
	"d7y.io/dragonfly/v2/scheduler/networktopology"
	networktopologymocks "d7y.io/dragonfly/v2/scheduler/networktopology/mocks"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduling/mocks"
	schedulingmocks "d7y.io/dragonfly/v2/scheduler/scheduling/mocks"
	storagemocks "d7y.io/dragonfly/v2/scheduler/storage/mocks"
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
			resource := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)

			tc.expect(t, NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, scheduling, dynconfig, storage, networkTopology))
		})
	}
}

func TestServiceV2_StatPeer(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, resp *commonv2.Peer, err error)
	}{
		{
			name: "peer not found",
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, resp *commonv2.Peer, err error) {
				assert := assert.New(t)
				assert.ErrorIs(err, status.Errorf(codes.NotFound, "peer %s not found", mockPeerID))
			},
		},
		{
			name: "peer has been loaded",
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.StorePiece(&mockPiece)
				peer.Task.StorePiece(&mockPiece)
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Any()).Return(peer, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, resp *commonv2.Peer, err error) {
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
						Id:            peer.Task.ID,
						Type:          peer.Task.Type,
						Url:           peer.Task.URL,
						Digest:        &dgst,
						Tag:           &peer.Task.Tag,
						Application:   &peer.Task.Application,
						Filters:       peer.Task.Filters,
						Header:        peer.Task.Header,
						PieceLength:   uint32(peer.Task.PieceLength),
						ContentLength: uint64(peer.Task.ContentLength.Load()),
						PieceCount:    uint32(peer.Task.TotalPieceCount.Load()),
						SizeScope:     peer.Task.SizeScope(),
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
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockSeedPeerID, mockResourceConfig, mockTask, mockHost, resource.WithRange(mockPeerRange))
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig, storage, networkTopology)

			tc.mock(peer, peerManager, res.EXPECT(), peerManager.EXPECT())
			resp, err := svc.StatPeer(context.Background(), &schedulerv2.StatPeerRequest{TaskId: mockTaskID, PeerId: mockPeerID})
			tc.expect(t, peer, resp, err)
		})
	}
}

func TestServiceV2_LeavePeer(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
		expect func(t *testing.T, err error)
	}{
		{
			name: "peer not found",
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
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
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				peer.FSM.SetState(resource.PeerStateLeave)
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
			mock: func(peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
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
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockSeedPeerID, mockResourceConfig, mockTask, mockHost, resource.WithRange(mockPeerRange))
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig, storage, networkTopology)

			tc.mock(peer, peerManager, res.EXPECT(), peerManager.EXPECT())
			tc.expect(t, svc.LeavePeer(context.Background(), &schedulerv2.LeavePeerRequest{TaskId: mockTaskID, PeerId: mockPeerID}))
		})
	}
}

func TestServiceV2_StatTask(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(task *resource.Task, taskManager resource.TaskManager, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder)
		expect func(t *testing.T, task *resource.Task, resp *commonv2.Task, err error)
	}{
		{
			name: "task not found",
			mock: func(task *resource.Task, taskManager resource.TaskManager, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder) {
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, task *resource.Task, resp *commonv2.Task, err error) {
				assert := assert.New(t)
				assert.ErrorIs(err, status.Errorf(codes.NotFound, "task %s not found", mockTaskID))
			},
		},
		{
			name: "task has been loaded",
			mock: func(task *resource.Task, taskManager resource.TaskManager, mr *resource.MockResourceMockRecorder, mt *resource.MockTaskManagerMockRecorder) {
				task.StorePiece(&mockPiece)
				gomock.InOrder(
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Any()).Return(task, true).Times(1),
				)
			},
			expect: func(t *testing.T, task *resource.Task, resp *commonv2.Task, err error) {
				dgst := task.Digest.String()

				assert := assert.New(t)
				assert.EqualValues(resp, &commonv2.Task{
					Id:            task.ID,
					Type:          task.Type,
					Url:           task.URL,
					Digest:        &dgst,
					Tag:           &task.Tag,
					Application:   &task.Application,
					Filters:       task.Filters,
					Header:        task.Header,
					PieceLength:   uint32(task.PieceLength),
					ContentLength: uint64(task.ContentLength.Load()),
					PieceCount:    uint32(task.TotalPieceCount.Load()),
					SizeScope:     task.SizeScope(),
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
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			taskManager := resource.NewMockTaskManager(ctl)
			task := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig, storage, networkTopology)

			tc.mock(task, taskManager, res.EXPECT(), taskManager.EXPECT())
			resp, err := svc.StatTask(context.Background(), &schedulerv2.StatTaskRequest{Id: mockTaskID})
			tc.expect(t, task, resp, err)
		})
	}
}

func TestServiceV2_AnnounceHost(t *testing.T) {
	tests := []struct {
		name string
		req  *schedulerv2.AnnounceHostRequest
		run  func(t *testing.T, svc *V2, req *schedulerv2.AnnounceHostRequest, host *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "host not found",
			req: &schedulerv2.AnnounceHostRequest{
				Host: &commonv2.Host{
					Id:              mockHostID,
					Type:            uint32(pkgtypes.HostTypeNormal),
					Hostname:        "hostname",
					Ip:              "127.0.0.1",
					Port:            8003,
					DownloadPort:    8001,
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
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.AnnounceHostRequest, host *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					md.GetSchedulerClusterClientConfig().Return(managertypes.SchedulerClusterClientConfig{LoadLimit: 10}, nil).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Store(gomock.Any()).Do(func(host *resource.Host) {
						assert := assert.New(t)
						assert.Equal(host.ID, req.Host.Id)
						assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
						assert.Equal(host.Hostname, req.Host.Hostname)
						assert.Equal(host.IP, req.Host.Ip)
						assert.Equal(host.Port, req.Host.Port)
						assert.Equal(host.DownloadPort, req.Host.DownloadPort)
						assert.Equal(host.OS, req.Host.Os)
						assert.Equal(host.Platform, req.Host.Platform)
						assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
						assert.Equal(host.KernelVersion, req.Host.KernelVersion)
						assert.EqualValues(host.CPU, mockCPU)
						assert.EqualValues(host.Memory, mockMemory)
						assert.EqualValues(host.Network, mockNetwork)
						assert.EqualValues(host.Disk, mockDisk)
						assert.EqualValues(host.Build, mockBuild)
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
				)

				assert := assert.New(t)
				assert.NoError(svc.AnnounceHost(context.Background(), req))
			},
		},
		{
			name: "host not found and dynconfig returns error",
			req: &schedulerv2.AnnounceHostRequest{
				Host: &commonv2.Host{
					Id:              mockHostID,
					Type:            uint32(pkgtypes.HostTypeNormal),
					Hostname:        "hostname",
					Ip:              "127.0.0.1",
					Port:            8003,
					DownloadPort:    8001,
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
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.AnnounceHostRequest, host *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					md.GetSchedulerClusterClientConfig().Return(managertypes.SchedulerClusterClientConfig{}, errors.New("foo")).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Store(gomock.Any()).Do(func(host *resource.Host) {
						assert := assert.New(t)
						assert.Equal(host.ID, req.Host.Id)
						assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
						assert.Equal(host.Hostname, req.Host.Hostname)
						assert.Equal(host.IP, req.Host.Ip)
						assert.Equal(host.Port, req.Host.Port)
						assert.Equal(host.DownloadPort, req.Host.DownloadPort)
						assert.Equal(host.OS, req.Host.Os)
						assert.Equal(host.Platform, req.Host.Platform)
						assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
						assert.Equal(host.KernelVersion, req.Host.KernelVersion)
						assert.EqualValues(host.CPU, mockCPU)
						assert.EqualValues(host.Memory, mockMemory)
						assert.EqualValues(host.Network, mockNetwork)
						assert.EqualValues(host.Disk, mockDisk)
						assert.EqualValues(host.Build, mockBuild)
						assert.Equal(host.ConcurrentUploadLimit.Load(), int32(50))
						assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
						assert.Equal(host.UploadCount.Load(), int64(0))
						assert.Equal(host.UploadFailedCount.Load(), int64(0))
						assert.NotNil(host.Peers)
						assert.Equal(host.PeerCount.Load(), int32(0))
						assert.NotEqual(host.CreatedAt.Load().Nanosecond(), 0)
						assert.NotEqual(host.UpdatedAt.Load().Nanosecond(), 0)
						assert.NotNil(host.Log)
					}).Return().Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.AnnounceHost(context.Background(), req))
			},
		},
		{
			name: "host already exists",
			req: &schedulerv2.AnnounceHostRequest{
				Host: &commonv2.Host{
					Id:              mockHostID,
					Type:            uint32(pkgtypes.HostTypeNormal),
					Hostname:        "foo",
					Ip:              "127.0.0.1",
					Port:            8003,
					DownloadPort:    8001,
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
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.AnnounceHostRequest, host *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					md.GetSchedulerClusterClientConfig().Return(managertypes.SchedulerClusterClientConfig{LoadLimit: 10}, nil).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.AnnounceHost(context.Background(), req))
				assert.Equal(host.ID, req.Host.Id)
				assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
				assert.Equal(host.Hostname, req.Host.Hostname)
				assert.Equal(host.IP, req.Host.Ip)
				assert.Equal(host.Port, req.Host.Port)
				assert.Equal(host.DownloadPort, req.Host.DownloadPort)
				assert.Equal(host.OS, req.Host.Os)
				assert.Equal(host.Platform, req.Host.Platform)
				assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
				assert.Equal(host.KernelVersion, req.Host.KernelVersion)
				assert.EqualValues(host.CPU, mockCPU)
				assert.EqualValues(host.Memory, mockMemory)
				assert.EqualValues(host.Network, mockNetwork)
				assert.EqualValues(host.Disk, mockDisk)
				assert.EqualValues(host.Build, mockBuild)
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
			name: "host already exists and dynconfig returns error",
			req: &schedulerv2.AnnounceHostRequest{
				Host: &commonv2.Host{
					Id:              mockHostID,
					Type:            uint32(pkgtypes.HostTypeNormal),
					Hostname:        "foo",
					Ip:              "127.0.0.1",
					Port:            8003,
					DownloadPort:    8001,
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
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.AnnounceHostRequest, host *resource.Host, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					md.GetSchedulerClusterClientConfig().Return(managertypes.SchedulerClusterClientConfig{}, errors.New("foo")).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.AnnounceHost(context.Background(), req))
				assert.Equal(host.ID, req.Host.Id)
				assert.Equal(host.Type, pkgtypes.HostType(req.Host.Type))
				assert.Equal(host.Hostname, req.Host.Hostname)
				assert.Equal(host.IP, req.Host.Ip)
				assert.Equal(host.Port, req.Host.Port)
				assert.Equal(host.DownloadPort, req.Host.DownloadPort)
				assert.Equal(host.OS, req.Host.Os)
				assert.Equal(host.Platform, req.Host.Platform)
				assert.Equal(host.PlatformVersion, req.Host.PlatformVersion)
				assert.Equal(host.KernelVersion, req.Host.KernelVersion)
				assert.EqualValues(host.CPU, mockCPU)
				assert.EqualValues(host.Memory, mockMemory)
				assert.EqualValues(host.Network, mockNetwork)
				assert.EqualValues(host.Disk, mockDisk)
				assert.EqualValues(host.Build, mockBuild)
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(50))
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			host := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, tc.req, host, hostManager, res.EXPECT(), hostManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV2_LeaveHost(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mnt *networktopologymocks.MockNetworkTopologyMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, err error)
	}{
		{
			name: "host not found",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mnt *networktopologymocks.MockNetworkTopologyMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(nil, false).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name: "host has not peers",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mnt *networktopologymocks.MockNetworkTopologyMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mnt.DeleteHost(host.ID).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "peer leaves succeeded",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder, mnt *networktopologymocks.MockNetworkTopologyMockRecorder) {
				host.Peers.Store(mockPeer.ID, mockPeer)
				mockPeer.FSM.SetState(resource.PeerStatePending)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
					mnt.DeleteHost(host.ID).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(peer.FSM.Current(), resource.PeerStateLeave)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			host := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			mockPeer := resource.NewPeer(mockSeedPeerID, mockResourceConfig, mockTask, host)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig, storage, networkTopology)

			tc.mock(host, mockPeer, hostManager, res.EXPECT(), hostManager.EXPECT(), networkTopology.EXPECT())
			tc.expect(t, mockPeer, svc.LeaveHost(context.Background(), &schedulerv2.LeaveHostRequest{Id: mockHostID}))
		})
	}
}

func TestServiceV2_SyncProbes(t *testing.T) {
	tests := []struct {
		name string
		mock func(svc *V2, mr *resource.MockResourceMockRecorder, probes *networktopologymocks.MockProbes, mp *networktopologymocks.MockProbesMockRecorder,
			mn *networktopologymocks.MockNetworkTopologyMockRecorder, hostManager resource.HostManager, mh *resource.MockHostManagerMockRecorder,
			ms *schedulerv2mocks.MockScheduler_SyncProbesServerMockRecorder)
		expect func(t *testing.T, err error)
	}{
		{
			name: "network topology is not enabled",
			mock: func(svc *V2, mr *resource.MockResourceMockRecorder, probes *networktopologymocks.MockProbes, mp *networktopologymocks.MockProbesMockRecorder,
				mn *networktopologymocks.MockNetworkTopologyMockRecorder, hostManager resource.HostManager, mh *resource.MockHostManagerMockRecorder,
				ms *schedulerv2mocks.MockScheduler_SyncProbesServerMockRecorder) {
				svc.networkTopology = nil
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "rpc error: code = Unimplemented desc = network topology is not enabled")
			},
		},
		{
			name: "synchronize probes when receive ProbeStartedRequest",
			mock: func(svc *V2, mr *resource.MockResourceMockRecorder, probes *networktopologymocks.MockProbes, mp *networktopologymocks.MockProbesMockRecorder,
				mn *networktopologymocks.MockNetworkTopologyMockRecorder, hostManager resource.HostManager, mh *resource.MockHostManagerMockRecorder,
				ms *schedulerv2mocks.MockScheduler_SyncProbesServerMockRecorder) {
				gomock.InOrder(
					ms.Recv().Return(&schedulerv2.SyncProbesRequest{
						Host: &commonv2.Host{
							Id:              mockSeedHostID,
							Type:            uint32(pkgtypes.HostTypeSuperSeed),
							Hostname:        "bar",
							Ip:              "127.0.0.1",
							Port:            8003,
							DownloadPort:    8001,
							Os:              "darwin",
							Platform:        "darwin",
							PlatformFamily:  "Standalone Workstation",
							PlatformVersion: "11.1",
							KernelVersion:   "20.2.0",
							Cpu:             mockV2Probe.Host.Cpu,
							Memory:          mockV2Probe.Host.Memory,
							Network:         mockV2Probe.Host.Network,
							Disk:            mockV2Probe.Host.Disk,
							Build:           mockV2Probe.Host.Build,
						},
						Request: &schedulerv2.SyncProbesRequest_ProbeStartedRequest{
							ProbeStartedRequest: &schedulerv2.ProbeStartedRequest{},
						},
					}, nil).Times(1),
					mn.FindProbedHosts(gomock.Eq(mockRawSeedHost.ID)).Return([]*resource.Host{&mockRawHost}, nil).Times(1),
					ms.Send(gomock.Eq(&schedulerv2.SyncProbesResponse{
						Hosts: []*commonv2.Host{
							{
								Id:              mockRawHost.ID,
								Type:            uint32(mockRawHost.Type),
								Hostname:        mockRawHost.Hostname,
								Ip:              mockRawHost.IP,
								Port:            mockRawHost.Port,
								DownloadPort:    mockRawHost.DownloadPort,
								Os:              mockRawHost.OS,
								Platform:        mockRawHost.Platform,
								PlatformFamily:  mockRawHost.PlatformFamily,
								PlatformVersion: mockRawHost.PlatformVersion,
								KernelVersion:   mockRawHost.KernelVersion,
								Cpu:             mockV2Probe.Host.Cpu,
								Memory:          mockV2Probe.Host.Memory,
								Network:         mockV2Probe.Host.Network,
								Disk:            mockV2Probe.Host.Disk,
								Build:           mockV2Probe.Host.Build,
							},
						},
					})).Return(nil).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "synchronize probes when receive ProbeFinishedRequest",
			mock: func(svc *V2, mr *resource.MockResourceMockRecorder, probes *networktopologymocks.MockProbes, mp *networktopologymocks.MockProbesMockRecorder,
				mn *networktopologymocks.MockNetworkTopologyMockRecorder, hostManager resource.HostManager, mh *resource.MockHostManagerMockRecorder,
				ms *schedulerv2mocks.MockScheduler_SyncProbesServerMockRecorder) {
				gomock.InOrder(
					ms.Recv().Return(&schedulerv2.SyncProbesRequest{
						Host: &commonv2.Host{
							Id:              mockSeedHostID,
							Type:            uint32(pkgtypes.HostTypeSuperSeed),
							Hostname:        "bar",
							Ip:              "127.0.0.1",
							Port:            8003,
							DownloadPort:    8001,
							Os:              "darwin",
							Platform:        "darwin",
							PlatformFamily:  "Standalone Workstation",
							PlatformVersion: "11.1",
							KernelVersion:   "20.2.0",
							Cpu:             mockV2Probe.Host.Cpu,
							Memory:          mockV2Probe.Host.Memory,
							Network:         mockV2Probe.Host.Network,
							Disk:            mockV2Probe.Host.Disk,
							Build:           mockV2Probe.Host.Build,
						},
						Request: &schedulerv2.SyncProbesRequest_ProbeFinishedRequest{
							ProbeFinishedRequest: &schedulerv2.ProbeFinishedRequest{
								Probes: []*schedulerv2.Probe{mockV2Probe},
							},
						},
					}, nil).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockRawHost.ID)).Return(&mockRawHost, true),
					mn.Store(gomock.Eq(mockRawSeedHost.ID), gomock.Eq(mockRawHost.ID)).Return(nil).Times(1),
					mn.Probes(gomock.Eq(mockRawSeedHost.ID), gomock.Eq(mockRawHost.ID)).Return(probes).Times(1),
					mp.Enqueue(gomock.Eq(&networktopology.Probe{
						Host:      &mockRawHost,
						RTT:       mockV2Probe.Rtt.AsDuration(),
						CreatedAt: mockV2Probe.CreatedAt.AsTime(),
					})).Return(nil).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "synchronize probes when receive ProbeFailedRequest",
			mock: func(svc *V2, mr *resource.MockResourceMockRecorder, probes *networktopologymocks.MockProbes, mp *networktopologymocks.MockProbesMockRecorder,
				mn *networktopologymocks.MockNetworkTopologyMockRecorder, hostManager resource.HostManager, mh *resource.MockHostManagerMockRecorder,
				ms *schedulerv2mocks.MockScheduler_SyncProbesServerMockRecorder) {
				gomock.InOrder(
					ms.Recv().Return(&schedulerv2.SyncProbesRequest{
						Host: &commonv2.Host{
							Id:              mockSeedHostID,
							Type:            uint32(pkgtypes.HostTypeSuperSeed),
							Hostname:        "bar",
							Ip:              "127.0.0.1",
							Port:            8003,
							DownloadPort:    8001,
							Os:              "darwin",
							Platform:        "darwin",
							PlatformFamily:  "Standalone Workstation",
							PlatformVersion: "11.1",
							KernelVersion:   "20.2.0",
							Cpu:             mockV2Probe.Host.Cpu,
							Memory:          mockV2Probe.Host.Memory,
							Network:         mockV2Probe.Host.Network,
							Disk:            mockV2Probe.Host.Disk,
							Build:           mockV2Probe.Host.Build,
						},
						Request: &schedulerv2.SyncProbesRequest_ProbeFailedRequest{
							ProbeFailedRequest: &schedulerv2.ProbeFailedRequest{
								Probes: []*schedulerv2.FailedProbe{
									{
										Host: &commonv2.Host{
											Id:              mockRawHost.ID,
											Type:            uint32(mockRawHost.Type),
											Hostname:        mockRawHost.Hostname,
											Ip:              mockRawHost.IP,
											Port:            mockRawHost.Port,
											DownloadPort:    mockRawHost.DownloadPort,
											Os:              mockRawHost.OS,
											Platform:        mockRawHost.Platform,
											PlatformFamily:  mockRawHost.PlatformFamily,
											PlatformVersion: mockRawHost.PlatformVersion,
											KernelVersion:   mockRawHost.KernelVersion,
											Cpu:             mockV2Probe.Host.Cpu,
											Memory:          mockV2Probe.Host.Memory,
											Network:         mockV2Probe.Host.Network,
											Disk:            mockV2Probe.Host.Disk,
											Build:           mockV2Probe.Host.Build,
										},
									},
								},
							},
						},
					}, nil).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "synchronize probes when receive fail type request",
			mock: func(svc *V2, mr *resource.MockResourceMockRecorder, probes *networktopologymocks.MockProbes, mp *networktopologymocks.MockProbesMockRecorder,
				mn *networktopologymocks.MockNetworkTopologyMockRecorder, hostManager resource.HostManager, mh *resource.MockHostManagerMockRecorder,
				ms *schedulerv2mocks.MockScheduler_SyncProbesServerMockRecorder) {
				ms.Recv().Return(&schedulerv2.SyncProbesRequest{
					Host: &commonv2.Host{
						Id:              mockSeedHostID,
						Type:            uint32(pkgtypes.HostTypeSuperSeed),
						Hostname:        "bar",
						Ip:              "127.0.0.1",
						Port:            8003,
						DownloadPort:    8001,
						Os:              "darwin",
						Platform:        "darwin",
						PlatformFamily:  "Standalone Workstation",
						PlatformVersion: "11.1",
						KernelVersion:   "20.2.0",
						Cpu:             mockV2Probe.Host.Cpu,
						Memory:          mockV2Probe.Host.Memory,
						Network:         mockV2Probe.Host.Network,
						Disk:            mockV2Probe.Host.Disk,
						Build:           mockV2Probe.Host.Build,
					},
					Request: nil,
				}, nil).Times(1)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "rpc error: code = FailedPrecondition desc = receive unknow request: <nil>")
			},
		},
		{
			name: "receive error",
			mock: func(svc *V2, mr *resource.MockResourceMockRecorder, probes *networktopologymocks.MockProbes, mp *networktopologymocks.MockProbesMockRecorder,
				mn *networktopologymocks.MockNetworkTopologyMockRecorder, hostManager resource.HostManager, mh *resource.MockHostManagerMockRecorder,
				ms *schedulerv2mocks.MockScheduler_SyncProbesServerMockRecorder) {
				ms.Recv().Return(nil, errors.New("receive error")).Times(1)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "receive error")
			},
		},
		{
			name: "receive end of file",
			mock: func(svc *V2, mr *resource.MockResourceMockRecorder, probes *networktopologymocks.MockProbes, mp *networktopologymocks.MockProbesMockRecorder,
				mn *networktopologymocks.MockNetworkTopologyMockRecorder, hostManager resource.HostManager, mh *resource.MockHostManagerMockRecorder,
				ms *schedulerv2mocks.MockScheduler_SyncProbesServerMockRecorder) {
				ms.Recv().Return(nil, io.EOF).Times(1)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "find probed host ids error",
			mock: func(svc *V2, mr *resource.MockResourceMockRecorder, probes *networktopologymocks.MockProbes, mp *networktopologymocks.MockProbesMockRecorder,
				mn *networktopologymocks.MockNetworkTopologyMockRecorder, hostManager resource.HostManager, mh *resource.MockHostManagerMockRecorder,
				ms *schedulerv2mocks.MockScheduler_SyncProbesServerMockRecorder) {
				gomock.InOrder(
					ms.Recv().Return(&schedulerv2.SyncProbesRequest{
						Host: &commonv2.Host{
							Id:              mockSeedHostID,
							Type:            uint32(pkgtypes.HostTypeSuperSeed),
							Hostname:        "bar",
							Ip:              "127.0.0.1",
							Port:            8003,
							DownloadPort:    8001,
							Os:              "darwin",
							Platform:        "darwin",
							PlatformFamily:  "Standalone Workstation",
							PlatformVersion: "11.1",
							KernelVersion:   "20.2.0",
							Cpu:             mockV2Probe.Host.Cpu,
							Memory:          mockV2Probe.Host.Memory,
							Network:         mockV2Probe.Host.Network,
							Disk:            mockV2Probe.Host.Disk,
							Build:           mockV2Probe.Host.Build,
						},
						Request: &schedulerv2.SyncProbesRequest_ProbeStartedRequest{
							ProbeStartedRequest: &schedulerv2.ProbeStartedRequest{},
						},
					}, nil).Times(1),
					mn.FindProbedHosts(gomock.Eq(mockRawSeedHost.ID)).Return(nil, errors.New("find probed host ids error")).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "rpc error: code = FailedPrecondition desc = find probed host ids error")
			},
		},
		{
			name: "send synchronize probes response error",
			mock: func(svc *V2, mr *resource.MockResourceMockRecorder, probes *networktopologymocks.MockProbes, mp *networktopologymocks.MockProbesMockRecorder,
				mn *networktopologymocks.MockNetworkTopologyMockRecorder, hostManager resource.HostManager, mh *resource.MockHostManagerMockRecorder,
				ms *schedulerv2mocks.MockScheduler_SyncProbesServerMockRecorder) {
				gomock.InOrder(
					ms.Recv().Return(&schedulerv2.SyncProbesRequest{
						Host: &commonv2.Host{
							Id:              mockSeedHostID,
							Type:            uint32(pkgtypes.HostTypeSuperSeed),
							Hostname:        "bar",
							Ip:              "127.0.0.1",
							Port:            8003,
							DownloadPort:    8001,
							Os:              "darwin",
							Platform:        "darwin",
							PlatformFamily:  "Standalone Workstation",
							PlatformVersion: "11.1",
							KernelVersion:   "20.2.0",
							Cpu:             mockV2Probe.Host.Cpu,
							Memory:          mockV2Probe.Host.Memory,
							Network:         mockV2Probe.Host.Network,
							Disk:            mockV2Probe.Host.Disk,
							Build:           mockV2Probe.Host.Build,
						},
						Request: &schedulerv2.SyncProbesRequest_ProbeStartedRequest{
							ProbeStartedRequest: &schedulerv2.ProbeStartedRequest{},
						},
					}, nil).Times(1),
					mn.FindProbedHosts(gomock.Eq(mockRawSeedHost.ID)).Return([]*resource.Host{&mockRawHost}, nil).Times(1),
					ms.Send(gomock.Eq(&schedulerv2.SyncProbesResponse{
						Hosts: []*commonv2.Host{
							{
								Id:              mockRawHost.ID,
								Type:            uint32(mockRawHost.Type),
								Hostname:        mockRawHost.Hostname,
								Ip:              mockRawHost.IP,
								Port:            mockRawHost.Port,
								DownloadPort:    mockRawHost.DownloadPort,
								Os:              mockRawHost.OS,
								Platform:        mockRawHost.Platform,
								PlatformFamily:  mockRawHost.PlatformFamily,
								PlatformVersion: mockRawHost.PlatformVersion,
								KernelVersion:   mockRawHost.KernelVersion,
								Cpu:             mockV2Probe.Host.Cpu,
								Memory:          mockV2Probe.Host.Memory,
								Network:         mockV2Probe.Host.Network,
								Disk:            mockV2Probe.Host.Disk,
								Build:           mockV2Probe.Host.Build,
							},
						},
					})).Return(errors.New("send synchronize probes response error")).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "send synchronize probes response error")
			},
		},
		{
			name: "load host error when receive ProbeFinishedRequest",
			mock: func(svc *V2, mr *resource.MockResourceMockRecorder, probes *networktopologymocks.MockProbes, mp *networktopologymocks.MockProbesMockRecorder,
				mn *networktopologymocks.MockNetworkTopologyMockRecorder, hostManager resource.HostManager, mh *resource.MockHostManagerMockRecorder,
				ms *schedulerv2mocks.MockScheduler_SyncProbesServerMockRecorder) {
				gomock.InOrder(
					ms.Recv().Return(&schedulerv2.SyncProbesRequest{
						Host: &commonv2.Host{
							Id:              mockSeedHostID,
							Type:            uint32(pkgtypes.HostTypeSuperSeed),
							Hostname:        "bar",
							Ip:              "127.0.0.1",
							Port:            8003,
							DownloadPort:    8001,
							Os:              "darwin",
							Platform:        "darwin",
							PlatformFamily:  "Standalone Workstation",
							PlatformVersion: "11.1",
							KernelVersion:   "20.2.0",
							Cpu:             mockV2Probe.Host.Cpu,
							Memory:          mockV2Probe.Host.Memory,
							Network:         mockV2Probe.Host.Network,
							Disk:            mockV2Probe.Host.Disk,
							Build:           mockV2Probe.Host.Build,
						},
						Request: &schedulerv2.SyncProbesRequest_ProbeFinishedRequest{
							ProbeFinishedRequest: &schedulerv2.ProbeFinishedRequest{
								Probes: []*schedulerv2.Probe{mockV2Probe},
							},
						},
					}, nil).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockRawHost.ID)).Return(nil, false),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "store error when receive ProbeFinishedRequest",
			mock: func(svc *V2, mr *resource.MockResourceMockRecorder, probes *networktopologymocks.MockProbes, mp *networktopologymocks.MockProbesMockRecorder,
				mn *networktopologymocks.MockNetworkTopologyMockRecorder, hostManager resource.HostManager, mh *resource.MockHostManagerMockRecorder,
				ms *schedulerv2mocks.MockScheduler_SyncProbesServerMockRecorder) {
				gomock.InOrder(
					ms.Recv().Return(&schedulerv2.SyncProbesRequest{
						Host: &commonv2.Host{
							Id:              mockSeedHostID,
							Type:            uint32(pkgtypes.HostTypeSuperSeed),
							Hostname:        "bar",
							Ip:              "127.0.0.1",
							Port:            8003,
							DownloadPort:    8001,
							Os:              "darwin",
							Platform:        "darwin",
							PlatformFamily:  "Standalone Workstation",
							PlatformVersion: "11.1",
							KernelVersion:   "20.2.0",
							Cpu:             mockV2Probe.Host.Cpu,
							Memory:          mockV2Probe.Host.Memory,
							Network:         mockV2Probe.Host.Network,
							Disk:            mockV2Probe.Host.Disk,
							Build:           mockV2Probe.Host.Build,
						},
						Request: &schedulerv2.SyncProbesRequest_ProbeFinishedRequest{
							ProbeFinishedRequest: &schedulerv2.ProbeFinishedRequest{
								Probes: []*schedulerv2.Probe{mockV2Probe},
							},
						},
					}, nil).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockRawHost.ID)).Return(&mockRawHost, true),
					mn.Store(gomock.Eq(mockRawSeedHost.ID), gomock.Eq(mockRawHost.ID)).Return(errors.New("store error")).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "enqueue probe error when receive ProbeFinishedRequest",
			mock: func(svc *V2, mr *resource.MockResourceMockRecorder, probes *networktopologymocks.MockProbes, mp *networktopologymocks.MockProbesMockRecorder,
				mn *networktopologymocks.MockNetworkTopologyMockRecorder, hostManager resource.HostManager, mh *resource.MockHostManagerMockRecorder,
				ms *schedulerv2mocks.MockScheduler_SyncProbesServerMockRecorder) {
				gomock.InOrder(
					ms.Recv().Return(&schedulerv2.SyncProbesRequest{
						Host: &commonv2.Host{
							Id:              mockSeedHostID,
							Type:            uint32(pkgtypes.HostTypeSuperSeed),
							Hostname:        "bar",
							Ip:              "127.0.0.1",
							Port:            8003,
							DownloadPort:    8001,
							Os:              "darwin",
							Platform:        "darwin",
							PlatformFamily:  "Standalone Workstation",
							PlatformVersion: "11.1",
							KernelVersion:   "20.2.0",
							Cpu:             mockV2Probe.Host.Cpu,
							Memory:          mockV2Probe.Host.Memory,
							Network:         mockV2Probe.Host.Network,
							Disk:            mockV2Probe.Host.Disk,
							Build:           mockV2Probe.Host.Build,
						},
						Request: &schedulerv2.SyncProbesRequest_ProbeFinishedRequest{
							ProbeFinishedRequest: &schedulerv2.ProbeFinishedRequest{
								Probes: []*schedulerv2.Probe{mockV2Probe},
							},
						},
					}, nil).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockRawHost.ID)).Return(&mockRawHost, true),
					mn.Store(gomock.Eq(mockRawSeedHost.ID), gomock.Eq(mockRawHost.ID)).Return(nil).Times(1),
					mn.Probes(gomock.Eq(mockRawSeedHost.ID), gomock.Eq(mockRawHost.ID)).Return(probes).Times(1),
					mp.Enqueue(gomock.Any()).Return(errors.New("enqueue probe error")).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
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

			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			probes := networktopologymocks.NewMockProbes(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			stream := schedulerv2mocks.NewMockScheduler_SyncProbesServer(ctl)
			svc := NewV2(&config.Config{NetworkTopology: mockNetworkTopologyConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig, storage, networkTopology)

			tc.mock(svc, res.EXPECT(), probes, probes.EXPECT(), networkTopology.EXPECT(), hostManager, hostManager.EXPECT(), stream.EXPECT())
			tc.expect(t, svc.SyncProbes(stream))
		})
	}
}

func TestServiceV2_handleRegisterPeerRequest(t *testing.T) {
	dgst := mockTaskDigest.String()

	tests := []struct {
		name string
		req  *schedulerv2.RegisterPeerRequest
		run  func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *resource.Peer, seedPeer *resource.Peer, hostManager resource.HostManager, taskManager resource.TaskManager,
			peerManager resource.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
			mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder)
	}{
		{
			name: "host not found",
			req:  &schedulerv2.RegisterPeerRequest{},
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *resource.Peer, seedPeer *resource.Peer, hostManager resource.HostManager, taskManager resource.TaskManager,
				peerManager resource.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
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
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *resource.Peer, seedPeer *resource.Peer, hostManager resource.HostManager, taskManager resource.TaskManager,
				peerManager resource.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
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
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *resource.Peer, seedPeer *resource.Peer, hostManager resource.HostManager, taskManager resource.TaskManager,
				peerManager resource.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(peer.Host.ID)).Return(peer.Host, true).Times(1),
					mr.TaskManager().Return(taskManager).Times(1),
					mt.Load(gomock.Eq(peer.Task.ID)).Return(peer.Task, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.Priority = commonv2.Priority_LEVEL1
				peer.Task.FSM.SetState(resource.TaskStateFailed)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(seedPeer)
				seedPeer.FSM.SetState(resource.PeerStateRunning)

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
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *resource.Peer, seedPeer *resource.Peer, hostManager resource.HostManager, taskManager resource.TaskManager,
				peerManager resource.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
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
				assert.Equal(peer.FSM.Current(), resource.PeerStatePending)
			},
		},
		{
			name: "size scope is SizeScope_EMPTY and event PeerEventRegisterEmpty failed",
			req: &schedulerv2.RegisterPeerRequest{
				Download: &commonv2.Download{
					Digest: &dgst,
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *resource.Peer, seedPeer *resource.Peer, hostManager resource.HostManager, taskManager resource.TaskManager,
				peerManager resource.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
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
				peer.FSM.SetState(resource.PeerStateReceivedEmpty)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleRegisterPeerRequest(context.Background(), nil, peer.Host.ID, peer.Task.ID, peer.ID, req),
					status.Errorf(codes.Internal, "event RegisterEmpty inappropriate in current state ReceivedEmpty"))
			},
		},
		{
			name: "size scope is SizeScope_EMPTY and send EmptyTaskResponse failed",
			req: &schedulerv2.RegisterPeerRequest{
				Download: &commonv2.Download{
					Digest: &dgst,
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *resource.Peer, seedPeer *resource.Peer, hostManager resource.HostManager, taskManager resource.TaskManager,
				peerManager resource.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
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
				assert.Equal(peer.FSM.Current(), resource.PeerStateReceivedEmpty)
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
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *resource.Peer, seedPeer *resource.Peer, hostManager resource.HostManager, taskManager resource.TaskManager,
				peerManager resource.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
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
				assert.Equal(peer.FSM.Current(), resource.PeerStateReceivedNormal)
				assert.Equal(peer.NeedBackToSource.Load(), true)
			},
		},
		{
			name: "size scope is SizeScope_NORMAL",
			req: &schedulerv2.RegisterPeerRequest{
				Download: &commonv2.Download{
					Digest: &dgst,
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *resource.Peer, seedPeer *resource.Peer, hostManager resource.HostManager, taskManager resource.TaskManager,
				peerManager resource.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
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
				assert.Equal(peer.FSM.Current(), resource.PeerStateReceivedNormal)
			},
		},
		{
			name: "size scope is SizeScope_UNKNOW",
			req: &schedulerv2.RegisterPeerRequest{
				Download: &commonv2.Download{
					Digest: &dgst,
				},
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.RegisterPeerRequest, peer *resource.Peer, seedPeer *resource.Peer, hostManager resource.HostManager, taskManager resource.TaskManager,
				peerManager resource.PeerManager, stream schedulerv2.Scheduler_AnnouncePeerServer, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder, ma *schedulerv2mocks.MockScheduler_AnnouncePeerServerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
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
				assert.Equal(peer.FSM.Current(), resource.PeerStateReceivedNormal)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			taskManager := resource.NewMockTaskManager(ctl)
			stream := schedulerv2mocks.NewMockScheduler_AnnouncePeerServer(ctl)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			seedPeer := resource.NewPeer(mockSeedPeerID, mockResourceConfig, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, tc.req, peer, seedPeer, hostManager, taskManager, peerManager, stream, res.EXPECT(), hostManager.EXPECT(), taskManager.EXPECT(), peerManager.EXPECT(), stream.EXPECT(), scheduling.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPeerStartedRequest(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateRunning)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerStartedRequest(context.Background(), peer.ID), status.Error(codes.Internal, "event Download inappropriate in current state Running"))
			},
		},
		{
			name: "task state is TaskStateRunning",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateReceivedNormal)
				peer.Task.FSM.SetState(resource.TaskStateRunning)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerStartedRequest(context.Background(), peer.ID))
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.NotEqual(peer.Task.UpdatedAt.Load(), 0)
			},
		},
		{
			name: "task state is TaskStatePending",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateReceivedNormal)
				peer.Task.FSM.SetState(resource.TaskStatePending)

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
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			peerManager := resource.NewMockPeerManager(ctl)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, peer, peerManager, res.EXPECT(), peerManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPeerBackToSourceStartedRequest(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateBackToSource)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceStartedRequest(context.Background(), peer.ID), status.Error(codes.Internal, "event DownloadBackToSource inappropriate in current state BackToSource"))
			},
		},
		{
			name: "task state is TaskStateRunning",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateReceivedNormal)
				peer.Task.FSM.SetState(resource.TaskStateRunning)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerBackToSourceStartedRequest(context.Background(), peer.ID))
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.NotEqual(peer.Task.UpdatedAt.Load(), 0)
			},
		},
		{
			name: "task state is TaskStatePending",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateReceivedNormal)
				peer.Task.FSM.SetState(resource.TaskStatePending)

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
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			peerManager := resource.NewMockPeerManager(ctl)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, peer, peerManager, res.EXPECT(), peerManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV2_handleRescheduleRequest(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
			mp *resource.MockPeerManagerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleRescheduleRequest(context.Background(), peer.ID), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
			},
		},
		{
			name: "reschedule failed",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					ms.ScheduleCandidateParents(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("foo")).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleRescheduleRequest(context.Background(), peer.ID), status.Error(codes.FailedPrecondition, "foo"))
			},
		},
		{
			name: "reschedule succeeded",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, ms *schedulingmocks.MockSchedulingMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					ms.ScheduleCandidateParents(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.handleRescheduleRequest(context.Background(), peer.ID))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			peerManager := resource.NewMockPeerManager(ctl)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, peer, peerManager, res.EXPECT(), peerManager.EXPECT(), scheduling.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPeerFinishedRequest(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateSucceeded)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerFinishedRequest(context.Background(), peer.ID), status.Error(codes.Internal, "event DownloadSucceeded inappropriate in current state Succeeded"))
				assert.NotEqual(peer.Cost.Load(), 0)
			},
		},
		{
			name: "peer state is PeerStateRunning",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateRunning)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerFinishedRequest(context.Background(), peer.ID))
				assert.Equal(peer.FSM.Current(), resource.PeerStateSucceeded)
				assert.NotEqual(peer.Cost.Load(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			peerManager := resource.NewMockPeerManager(ctl)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, peer, peerManager, res.EXPECT(), peerManager.EXPECT(), dynconfig.EXPECT())
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
		run  func(t *testing.T, svc *V2, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
			mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			req:  &schedulerv2.DownloadPeerBackToSourceFinishedRequest{},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceFinishedRequest(context.Background(), peer.ID, req), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
				assert.Equal(peer.Task.ContentLength.Load(), int64(-1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(0))
				assert.Equal(len(peer.Task.DirectPiece), 0)
				assert.Equal(peer.Task.FSM.Current(), resource.TaskStatePending)
			},
		},
		{
			name: "peer state is PeerStateSucceeded",
			req:  &schedulerv2.DownloadPeerBackToSourceFinishedRequest{},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateSucceeded)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceFinishedRequest(context.Background(), peer.ID, req), status.Error(codes.Internal, "event DownloadSucceeded inappropriate in current state Succeeded"))
				assert.NotEqual(peer.Cost.Load(), 0)
				assert.Equal(peer.Task.ContentLength.Load(), int64(-1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(0))
				assert.Equal(len(peer.Task.DirectPiece), 0)
				assert.Equal(peer.Task.FSM.Current(), resource.TaskStatePending)
			},
		},
		{
			name: "peer has range",
			req:  &schedulerv2.DownloadPeerBackToSourceFinishedRequest{},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Range = &nethttp.Range{}

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerBackToSourceFinishedRequest(context.Background(), peer.ID, req))
				assert.NotEqual(peer.Cost.Load(), 0)
				assert.Equal(peer.FSM.Current(), resource.PeerStateSucceeded)
				assert.Equal(peer.Task.ContentLength.Load(), int64(-1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(0))
				assert.Equal(len(peer.Task.DirectPiece), 0)
				assert.Equal(peer.Task.FSM.Current(), resource.TaskStatePending)
			},
		},
		{
			name: "task state is TaskStateSucceeded",
			req:  &schedulerv2.DownloadPeerBackToSourceFinishedRequest{},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.FSM.SetState(resource.TaskStateSucceeded)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerBackToSourceFinishedRequest(context.Background(), peer.ID, req))
				assert.NotEqual(peer.Cost.Load(), 0)
				assert.Equal(peer.FSM.Current(), resource.PeerStateSucceeded)
				assert.Equal(peer.Task.ContentLength.Load(), int64(-1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(0))
				assert.Equal(len(peer.Task.DirectPiece), 0)
				assert.Equal(peer.Task.FSM.Current(), resource.TaskStateSucceeded)
			},
		},
		{
			name: "task state is TaskStatePending",
			req: &schedulerv2.DownloadPeerBackToSourceFinishedRequest{
				ContentLength: 1024,
				PieceCount:    10,
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.FSM.SetState(resource.TaskStatePending)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceFinishedRequest(context.Background(), peer.ID, req), status.Error(codes.Internal, "event DownloadSucceeded inappropriate in current state Pending"))
				assert.NotEqual(peer.Cost.Load(), 0)
				assert.Equal(peer.FSM.Current(), resource.PeerStateSucceeded)
				assert.Equal(peer.Task.ContentLength.Load(), int64(1024))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(10))
				assert.Equal(len(peer.Task.DirectPiece), 0)
				assert.Equal(peer.Task.FSM.Current(), resource.TaskStatePending)
			},
		},
		{
			name: "task state is TaskStateRunning",
			req: &schedulerv2.DownloadPeerBackToSourceFinishedRequest{
				ContentLength: 1024,
				PieceCount:    10,
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.FSM.SetState(resource.TaskStateRunning)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerBackToSourceFinishedRequest(context.Background(), peer.ID, req))
				assert.NotEqual(peer.Cost.Load(), 0)
				assert.Equal(peer.FSM.Current(), resource.PeerStateSucceeded)
				assert.Equal(peer.Task.ContentLength.Load(), int64(1024))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(10))
				assert.Equal(len(peer.Task.DirectPiece), 0)
				assert.Equal(peer.Task.FSM.Current(), resource.TaskStateSucceeded)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			peerManager := resource.NewMockPeerManager(ctl)

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

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockHost.IP = ip
			mockHost.DownloadPort = int32(port)

			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, tc.req, peer, peerManager, res.EXPECT(), peerManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPeerFailedRequest(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.FSM.SetState(resource.PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerFailedRequest(context.Background(), peer.ID), status.Error(codes.Internal, "event DownloadFailed inappropriate in current state DownloadFailed"))
			},
		},
		{
			name: "peer state is PeerStateRunning",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateRunning)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerFailedRequest(context.Background(), peer.ID))
				assert.Equal(peer.FSM.Current(), resource.PeerStateFailed)
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			scheduling := schedulingmocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			peerManager := resource.NewMockPeerManager(ctl)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, peer, peerManager, res.EXPECT(), peerManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPeerBackToSourceFailedRequest(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				peer.Task.ContentLength.Store(1)
				peer.Task.TotalPieceCount.Store(1)
				peer.Task.DirectPiece = []byte{1}

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceFailedRequest(context.Background(), peer.ID), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
				assert.Equal(peer.FSM.Current(), resource.PeerStatePending)
				assert.Equal(peer.Task.FSM.Current(), resource.TaskStatePending)
				assert.Equal(peer.Task.ContentLength.Load(), int64(1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(1))
				assert.Equal(peer.Task.DirectPiece, []byte{1})
			},
		},
		{
			name: "peer state is PeerStateFailed",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateFailed)
				peer.Task.ContentLength.Store(1)
				peer.Task.TotalPieceCount.Store(1)
				peer.Task.DirectPiece = []byte{1}

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceFailedRequest(context.Background(), peer.ID), status.Error(codes.Internal, "event DownloadFailed inappropriate in current state Failed"))
				assert.Equal(peer.FSM.Current(), resource.PeerStateFailed)
				assert.Equal(peer.Task.FSM.Current(), resource.TaskStatePending)
				assert.Equal(peer.Task.ContentLength.Load(), int64(1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(1))
				assert.Equal(peer.Task.DirectPiece, []byte{1})
			},
		},
		{
			name: "task state is TaskStateFailed",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.FSM.SetState(resource.TaskStateFailed)
				peer.Task.ContentLength.Store(1)
				peer.Task.TotalPieceCount.Store(1)
				peer.Task.DirectPiece = []byte{1}

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPeerBackToSourceFailedRequest(context.Background(), peer.ID), status.Error(codes.Internal, "event DownloadFailed inappropriate in current state Failed"))
				assert.Equal(peer.FSM.Current(), resource.PeerStateFailed)
				assert.Equal(peer.Task.FSM.Current(), resource.TaskStateFailed)
				assert.Equal(peer.Task.ContentLength.Load(), int64(-1))
				assert.Equal(peer.Task.TotalPieceCount.Load(), int32(0))
				assert.Equal(peer.Task.DirectPiece, []byte{})
			},
		},
		{
			name: "task state is TaskStateRunning",
			run: func(t *testing.T, svc *V2, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1),
				)

				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Task.FSM.SetState(resource.TaskStateRunning)
				peer.Task.ContentLength.Store(1)
				peer.Task.TotalPieceCount.Store(1)
				peer.Task.DirectPiece = []byte{1}

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPeerBackToSourceFailedRequest(context.Background(), peer.ID))
				assert.Equal(peer.FSM.Current(), resource.PeerStateFailed)
				assert.Equal(peer.Task.FSM.Current(), resource.TaskStateFailed)
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
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			peerManager := resource.NewMockPeerManager(ctl)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, peer, peerManager, res.EXPECT(), peerManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPieceFinishedRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *schedulerv2.DownloadPieceFinishedRequest
		run  func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
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
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPieceFinishedRequest(context.Background(), peer.ID, req), status.Error(codes.InvalidArgument, "invalid digest"))
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
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.ErrorIs(svc.handleDownloadPieceFinishedRequest(context.Background(), peer.ID, req), status.Errorf(codes.NotFound, "peer %s not found", peer.ID))
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
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(req.Piece.GetParentId())).Return(nil, false).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPieceFinishedRequest(context.Background(), peer.ID, req))

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
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
				gomock.InOrder(
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(peer.ID)).Return(peer, true).Times(1),
					mr.PeerManager().Return(peerManager).Times(1),
					mp.Load(gomock.Eq(req.Piece.GetParentId())).Return(peer, true).Times(1),
				)

				assert := assert.New(t)
				assert.NoError(svc.handleDownloadPieceFinishedRequest(context.Background(), peer.ID, req))

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
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			peerManager := resource.NewMockPeerManager(ctl)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, tc.req, peer, peerManager, res.EXPECT(), peerManager.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPieceBackToSourceFinishedRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *schedulerv2.DownloadPieceBackToSourceFinishedRequest
		run  func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceBackToSourceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder)
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
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceBackToSourceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
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
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceBackToSourceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
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
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceBackToSourceFinishedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
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
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			peerManager := resource.NewMockPeerManager(ctl)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, tc.req, peer, peerManager, res.EXPECT(), peerManager.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPieceFailedRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *schedulerv2.DownloadPieceFailedRequest
		run  func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFailedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
			mp *resource.MockPeerManagerMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			req: &schedulerv2.DownloadPieceFailedRequest{
				ParentId:  mockSeedPeerID,
				Temporary: true,
			},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFailedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder) {
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
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFailedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder) {
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
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFailedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder) {
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
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceFailedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder) {
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
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			peerManager := resource.NewMockPeerManager(ctl)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, tc.req, peer, peerManager, res.EXPECT(), peerManager.EXPECT())
		})
	}
}

func TestServiceV2_handleDownloadPieceBackToSourceFailedRequest(t *testing.T) {
	mockPieceNumber := uint32(mockPiece.Number)

	tests := []struct {
		name string
		req  *schedulerv2.DownloadPieceBackToSourceFailedRequest
		run  func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceBackToSourceFailedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
			mp *resource.MockPeerManagerMockRecorder)
	}{
		{
			name: "peer can not be loaded",
			req:  &schedulerv2.DownloadPieceBackToSourceFailedRequest{},
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceBackToSourceFailedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder) {
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
			run: func(t *testing.T, svc *V2, req *schedulerv2.DownloadPieceBackToSourceFailedRequest, peer *resource.Peer, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder,
				mp *resource.MockPeerManagerMockRecorder) {
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
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			peerManager := resource.NewMockPeerManager(ctl)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, tc.req, peer, peerManager, res.EXPECT(), peerManager.EXPECT())
		})
	}
}

func TestServiceV2_handleResource(t *testing.T) {
	dgst := mockTaskDigest.String()
	mismatchDgst := "foo"

	tests := []struct {
		name     string
		download *commonv2.Download
		run      func(t *testing.T, svc *V2, download *commonv2.Download, stream schedulerv2.Scheduler_AnnouncePeerServer, mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
			hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
			mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder)
	}{
		{
			name:     "host can not be loaded",
			download: &commonv2.Download{},
			run: func(t *testing.T, svc *V2, download *commonv2.Download, stream schedulerv2.Scheduler_AnnouncePeerServer, mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
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
				Url:     "foo",
				Filters: []string{"bar"},
				Header:  map[string]string{"baz": "bas"},
			},
			run: func(t *testing.T, svc *V2, download *commonv2.Download, stream schedulerv2.Scheduler_AnnouncePeerServer, mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
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
				assert.EqualValues(task.Filters, download.Filters)
				assert.EqualValues(task.Header, download.Header)
			},
		},
		{
			name: "task can not be loaded",
			download: &commonv2.Download{
				Url:     "foo",
				Filters: []string{"bar"},
				Header:  map[string]string{"baz": "bas"},
				Digest:  &dgst,
			},
			run: func(t *testing.T, svc *V2, download *commonv2.Download, stream schedulerv2.Scheduler_AnnouncePeerServer, mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
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
				assert.EqualValues(task.Filters, download.GetFilters())
				assert.EqualValues(task.Header, download.Header)
			},
		},
		{
			name: "invalid digest",
			download: &commonv2.Download{
				Digest: &mismatchDgst,
			},
			run: func(t *testing.T, svc *V2, download *commonv2.Download, stream schedulerv2.Scheduler_AnnouncePeerServer, mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
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
				Url:     "foo",
				Filters: []string{"bar"},
				Header:  map[string]string{"baz": "bas"},
				Digest:  &dgst,
			},
			run: func(t *testing.T, svc *V2, download *commonv2.Download, stream schedulerv2.Scheduler_AnnouncePeerServer, mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
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
				assert.EqualValues(task.Filters, download.GetFilters())
				assert.EqualValues(task.Header, download.Header)
				assert.EqualValues(peer, mockPeer)
			},
		},
		{
			name: "peer can not be loaded",
			download: &commonv2.Download{
				Url:      "foo",
				Filters:  []string{"bar"},
				Header:   map[string]string{"baz": "bas"},
				Digest:   &dgst,
				Priority: commonv2.Priority_LEVEL1,
				Range: &commonv2.Range{
					Start:  uint64(mockPeerRange.Start),
					Length: uint64(mockPeerRange.Length),
				},
			},
			run: func(t *testing.T, svc *V2, download *commonv2.Download, stream schedulerv2.Scheduler_AnnouncePeerServer, mockHost *resource.Host, mockTask *resource.Task, mockPeer *resource.Peer,
				hostManager resource.HostManager, taskManager resource.TaskManager, peerManager resource.PeerManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder,
				mt *resource.MockTaskManagerMockRecorder, mp *resource.MockPeerManagerMockRecorder) {
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
				assert.EqualValues(task.Filters, download.GetFilters())
				assert.EqualValues(task.Header, download.Header)
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
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			taskManager := resource.NewMockTaskManager(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			stream := schedulerv2mocks.NewMockScheduler_AnnouncePeerServer(ctl)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			mockPeer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig}, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, tc.download, stream, mockHost, mockTask, mockPeer, hostManager, taskManager, peerManager, res.EXPECT(), hostManager.EXPECT(), taskManager.EXPECT(), peerManager.EXPECT())
		})
	}
}

func TestServiceV2_downloadTaskBySeedPeer(t *testing.T) {
	tests := []struct {
		name   string
		config config.Config
		run    func(t *testing.T, svc *V2, peer *resource.Peer, seedPeerClient resource.SeedPeer, mr *resource.MockResourceMockRecorder, ms *resource.MockSeedPeerMockRecorder)
	}{
		{
			name: "priority is Priority_LEVEL6 and enable seed peer",
			config: config.Config{
				SeedPeer: config.SeedPeerConfig{
					Enable: true,
				},
			},
			run: func(t *testing.T, svc *V2, peer *resource.Peer, seedPeerClient resource.SeedPeer, mr *resource.MockResourceMockRecorder, ms *resource.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()

				gomock.InOrder(
					mr.SeedPeer().Return(seedPeerClient).Times(1),
					ms.TriggerDownloadTask(gomock.All(), gomock.Any(), gomock.Any()).Do(func(context.Context, string, *dfdaemonv2.TriggerDownloadTaskRequest) { wg.Done() }).Return(nil).Times(1),
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, seedPeerClient resource.SeedPeer, mr *resource.MockResourceMockRecorder, ms *resource.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()

				gomock.InOrder(
					mr.SeedPeer().Return(seedPeerClient).Times(1),
					ms.TriggerDownloadTask(gomock.All(), gomock.Any(), gomock.Any()).Do(func(context.Context, string, *dfdaemonv2.TriggerDownloadTaskRequest) { wg.Done() }).Return(errors.New("foo")).Times(1),
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, seedPeerClient resource.SeedPeer, mr *resource.MockResourceMockRecorder, ms *resource.MockSeedPeerMockRecorder) {
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, seedPeerClient resource.SeedPeer, mr *resource.MockResourceMockRecorder, ms *resource.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()

				gomock.InOrder(
					mr.SeedPeer().Return(seedPeerClient).Times(1),
					ms.TriggerDownloadTask(gomock.All(), gomock.Any(), gomock.Any()).Do(func(context.Context, string, *dfdaemonv2.TriggerDownloadTaskRequest) { wg.Done() }).Return(nil).Times(1),
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, seedPeerClient resource.SeedPeer, mr *resource.MockResourceMockRecorder, ms *resource.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()

				gomock.InOrder(
					mr.SeedPeer().Return(seedPeerClient).Times(1),
					ms.TriggerDownloadTask(gomock.All(), gomock.Any(), gomock.Any()).Do(func(context.Context, string, *dfdaemonv2.TriggerDownloadTaskRequest) { wg.Done() }).Return(errors.New("foo")).Times(1),
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, seedPeerClient resource.SeedPeer, mr *resource.MockResourceMockRecorder, ms *resource.MockSeedPeerMockRecorder) {
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, seedPeerClient resource.SeedPeer, mr *resource.MockResourceMockRecorder, ms *resource.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()

				gomock.InOrder(
					mr.SeedPeer().Return(seedPeerClient).Times(1),
					ms.TriggerDownloadTask(gomock.All(), gomock.Any(), gomock.Any()).Do(func(context.Context, string, *dfdaemonv2.TriggerDownloadTaskRequest) { wg.Done() }).Return(nil).Times(1),
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, seedPeerClient resource.SeedPeer, mr *resource.MockResourceMockRecorder, ms *resource.MockSeedPeerMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()

				gomock.InOrder(
					mr.SeedPeer().Return(seedPeerClient).Times(1),
					ms.TriggerDownloadTask(gomock.All(), gomock.Any(), gomock.Any()).Do(func(context.Context, string, *dfdaemonv2.TriggerDownloadTaskRequest) { wg.Done() }).Return(errors.New("foo")).Times(1),
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, seedPeerClient resource.SeedPeer, mr *resource.MockResourceMockRecorder, ms *resource.MockSeedPeerMockRecorder) {
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, seedPeerClient resource.SeedPeer, mr *resource.MockResourceMockRecorder, ms *resource.MockSeedPeerMockRecorder) {
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, seedPeerClient resource.SeedPeer, mr *resource.MockResourceMockRecorder, ms *resource.MockSeedPeerMockRecorder) {
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, seedPeerClient resource.SeedPeer, mr *resource.MockResourceMockRecorder, ms *resource.MockSeedPeerMockRecorder) {
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
			run: func(t *testing.T, svc *V2, peer *resource.Peer, seedPeerClient resource.SeedPeer, mr *resource.MockResourceMockRecorder, ms *resource.MockSeedPeerMockRecorder) {
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
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			seedPeerClient := resource.NewMockSeedPeer(ctl)

			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			svc := NewV2(&tc.config, res, scheduling, dynconfig, storage, networkTopology)

			tc.run(t, svc, peer, seedPeerClient, res.EXPECT(), seedPeerClient.EXPECT())
		})
	}
}
