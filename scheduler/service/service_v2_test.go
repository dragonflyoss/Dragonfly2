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
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv2 "d7y.io/api/pkg/apis/common/v2"
	schedulerv2 "d7y.io/api/pkg/apis/scheduler/v2"

	managertypes "d7y.io/dragonfly/v2/manager/types"
	pkgtypes "d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	configmocks "d7y.io/dragonfly/v2/scheduler/config/mocks"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduling/mocks"
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
			scheduling := mocks.NewMockScheduling(ctl)
			resource := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			tc.expect(t, NewV2(&config.Config{Scheduler: mockSchedulerConfig}, resource, scheduling, dynconfig, storage))
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
				assert := assert.New(t)
				assert.EqualValues(resp, &commonv2.Peer{
					Id: peer.ID,
					Range: &commonv2.Range{
						Start:  peer.Range.Start,
						Length: peer.Range.Length,
					},
					Priority: peer.Priority,
					Pieces: []*commonv2.Piece{
						{
							Number:      mockPiece.Number,
							ParentId:    mockPiece.ParentID,
							Offset:      mockPiece.Offset,
							Length:      mockPiece.Length,
							Digest:      mockPiece.Digest.String(),
							TrafficType: mockPiece.TrafficType,
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
						Digest:        peer.Task.Digest.String(),
						Tag:           peer.Task.Tag,
						Application:   peer.Task.Application,
						Filters:       peer.Task.Filters,
						Header:        peer.Task.Header,
						PieceLength:   peer.Task.PieceLength,
						ContentLength: peer.Task.ContentLength.Load(),
						PieceCount:    peer.Task.TotalPieceCount.Load(),
						SizeScope:     peer.Task.SizeScope(),
						Pieces: []*commonv2.Piece{
							{
								Number:      mockPiece.Number,
								ParentId:    mockPiece.ParentID,
								Offset:      mockPiece.Offset,
								Length:      mockPiece.Length,
								Digest:      mockPiece.Digest.String(),
								TrafficType: mockPiece.TrafficType,
								Cost:        durationpb.New(mockPiece.Cost),
								CreatedAt:   timestamppb.New(mockPiece.CreatedAt),
							},
						},
						State:     peer.Task.FSM.Current(),
						PeerCount: int32(peer.Task.PeerCount()),
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
							SecurityDomain:           peer.Host.Network.SecurityDomain,
							Location:                 peer.Host.Network.Location,
							Idc:                      peer.Host.Network.IDC,
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
							GitCommit:  peer.Host.Build.GitCommit,
							GoVersion:  peer.Host.Build.GoVersion,
							Platform:   peer.Host.Build.Platform,
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
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockSeedPeerID, mockTask, mockHost, resource.WithRange(mockPeerRange))
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig, storage)

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
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			peerManager := resource.NewMockPeerManager(ctl)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockSeedPeerID, mockTask, mockHost, resource.WithRange(mockPeerRange))
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig, storage)

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
				assert := assert.New(t)
				assert.EqualValues(resp, &commonv2.Task{
					Id:            task.ID,
					Type:          task.Type,
					Url:           task.URL,
					Digest:        task.Digest.String(),
					Tag:           task.Tag,
					Application:   task.Application,
					Filters:       task.Filters,
					Header:        task.Header,
					PieceLength:   task.PieceLength,
					ContentLength: task.ContentLength.Load(),
					PieceCount:    task.TotalPieceCount.Load(),
					SizeScope:     task.SizeScope(),
					Pieces: []*commonv2.Piece{
						{
							Number:      mockPiece.Number,
							ParentId:    mockPiece.ParentID,
							Offset:      mockPiece.Offset,
							Length:      mockPiece.Length,
							Digest:      mockPiece.Digest.String(),
							TrafficType: mockPiece.TrafficType,
							Cost:        durationpb.New(mockPiece.Cost),
							CreatedAt:   timestamppb.New(mockPiece.CreatedAt),
						},
					},
					State:     task.FSM.Current(),
					PeerCount: int32(task.PeerCount()),
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
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			taskManager := resource.NewMockTaskManager(ctl)
			task := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig, storage)

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
						SecurityDomain:           mockNetwork.SecurityDomain,
						Location:                 mockNetwork.Location,
						Idc:                      mockNetwork.IDC,
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
						GitCommit:  mockBuild.GitCommit,
						GoVersion:  mockBuild.GoVersion,
						Platform:   mockBuild.Platform,
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
						SecurityDomain:           mockNetwork.SecurityDomain,
						Location:                 mockNetwork.Location,
						Idc:                      mockNetwork.IDC,
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
						GitCommit:  mockBuild.GitCommit,
						GoVersion:  mockBuild.GoVersion,
						Platform:   mockBuild.Platform,
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
						SecurityDomain:           mockNetwork.SecurityDomain,
						Location:                 mockNetwork.Location,
						Idc:                      mockNetwork.IDC,
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
						GitCommit:  mockBuild.GitCommit,
						GoVersion:  mockBuild.GoVersion,
						Platform:   mockBuild.Platform,
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
						SecurityDomain:           mockNetwork.SecurityDomain,
						Location:                 mockNetwork.Location,
						Idc:                      mockNetwork.IDC,
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
						GitCommit:  mockBuild.GitCommit,
						GoVersion:  mockBuild.GoVersion,
						Platform:   mockBuild.Platform,
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
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			host := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig, storage)

			tc.run(t, svc, tc.req, host, hostManager, res.EXPECT(), hostManager.EXPECT(), dynconfig.EXPECT())
		})
	}
}

func TestServiceV2_LeaveHost(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder)
		expect func(t *testing.T, peer *resource.Peer, err error)
	}{
		{
			name: "host not found",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
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
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
				)
			},
			expect: func(t *testing.T, peer *resource.Peer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "peer leaves succeeded",
			mock: func(host *resource.Host, mockPeer *resource.Peer, hostManager resource.HostManager, mr *resource.MockResourceMockRecorder, mh *resource.MockHostManagerMockRecorder) {
				host.Peers.Store(mockPeer.ID, mockPeer)
				mockPeer.FSM.SetState(resource.PeerStatePending)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Any()).Return(host, true).Times(1),
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
			scheduling := mocks.NewMockScheduling(ctl)
			res := resource.NewMockResource(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			host := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			mockPeer := resource.NewPeer(mockSeedPeerID, mockTask, host)
			svc := NewV2(&config.Config{Scheduler: mockSchedulerConfig, Metrics: config.MetricsConfig{EnableHost: true}}, res, scheduling, dynconfig, storage)

			tc.mock(host, mockPeer, hostManager, res.EXPECT(), hostManager.EXPECT())
			tc.expect(t, mockPeer, svc.LeaveHost(context.Background(), &schedulerv2.LeaveHostRequest{Id: mockHostID}))
		})
	}
}
