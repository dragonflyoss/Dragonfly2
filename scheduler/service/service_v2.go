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
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv2 "d7y.io/api/pkg/apis/common/v2"
	schedulerv2 "d7y.io/api/pkg/apis/scheduler/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/networktopology"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduling"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

// V2 is the interface for v2 version of the service.
type V2 struct {
	// Resource interface.
	resource resource.Resource

	// Scheduling interface.
	scheduling scheduling.Scheduling

	// Scheduler service config.
	config *config.Config

	// Dynamic config.
	dynconfig config.DynconfigInterface

	// Storage interface.
	storage storage.Storage

	// Network topology interface.
	networkTopology networktopology.NetworkTopology
}

// New v2 version of service instance.
func NewV2(
	cfg *config.Config,
	resource resource.Resource,
	scheduling scheduling.Scheduling,
	dynconfig config.DynconfigInterface,
	storage storage.Storage,
	networkTopology networktopology.NetworkTopology,
) *V2 {
	return &V2{
		resource:        resource,
		scheduling:      scheduling,
		config:          cfg,
		dynconfig:       dynconfig,
		storage:         storage,
		networkTopology: networkTopology,
	}
}

// AnnouncePeer announces peer to scheduler.
func (v *V2) AnnouncePeer(stream schedulerv2.Scheduler_AnnouncePeerServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			logger.Infof("context was done")
			return ctx.Err()
		default:
		}

		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			logger.Errorf("receive error: %s", err.Error())
			return err
		}

		logger := logger.WithPeer(req.HostId, req.TaskId, req.PeerId)
		switch announcePeerRequest := req.GetRequest().(type) {
		case *schedulerv2.AnnouncePeerRequest_RegisterPeerRequest:
			logger.Infof("receive AnnouncePeerRequest_RegisterPeerRequest: %s", announcePeerRequest.RegisterPeerRequest.Download.Url)
			if err := v.handleRegisterPeerRequest(ctx, stream, req.HostId, req.TaskId, req.PeerId, announcePeerRequest.RegisterPeerRequest); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv2.AnnouncePeerRequest_RegisterSeedPeerRequest:
			logger.Infof("receive AnnouncePeerRequest_RegisterSeedPeerRequest: %s", announcePeerRequest.RegisterSeedPeerRequest.Download.Url)
			if err := v.handleRegisterSeedPeerRequest(ctx, stream, req.HostId, req.TaskId, req.PeerId, announcePeerRequest.RegisterSeedPeerRequest); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv2.AnnouncePeerRequest_DownloadPeerStartedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPeerStartedRequest: %#v", announcePeerRequest.DownloadPeerStartedRequest)
			if err := v.handleDownloadPeerStartedRequest(ctx, req.PeerId); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv2.AnnouncePeerRequest_DownloadPeerBackToSourceStartedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPeerBackToSourceStartedRequest: %#v", announcePeerRequest.DownloadPeerBackToSourceStartedRequest)
			if err := v.handleDownloadPeerBackToSourceStartedRequest(ctx, req.PeerId); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv2.AnnouncePeerRequest_DownloadPeerFinishedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPeerFinishedRequest: %#v", announcePeerRequest.DownloadPeerFinishedRequest)
			if err := v.handleDownloadPeerFinishedRequest(ctx, req.PeerId); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv2.AnnouncePeerRequest_DownloadPeerBackToSourceFinishedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPeerBackToSourceFinishedRequest: %#v", announcePeerRequest.DownloadPeerBackToSourceFinishedRequest)
			if err := v.handleDownloadPeerBackToSourceFinishedRequest(ctx, req.PeerId, announcePeerRequest.DownloadPeerBackToSourceFinishedRequest); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv2.AnnouncePeerRequest_DownloadPeerFailedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPeerFailedRequest: %#v", announcePeerRequest.DownloadPeerFailedRequest)
			if err := v.handleDownloadPeerFailedRequest(ctx, req.PeerId); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv2.AnnouncePeerRequest_DownloadPeerBackToSourceFailedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPeerBackToSourceFailedRequest: %#v", announcePeerRequest.DownloadPeerBackToSourceFailedRequest)
			if err := v.handleDownloadPeerBackToSourceFailedRequest(ctx, req.PeerId); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv2.AnnouncePeerRequest_DownloadPieceFinishedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPieceFinishedRequest: %#v", announcePeerRequest.DownloadPieceFinishedRequest)
			if err := v.handleDownloadPieceFinishedRequest(ctx, req.PeerId, announcePeerRequest.DownloadPieceFinishedRequest); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv2.AnnouncePeerRequest_DownloadPieceBackToSourceFinishedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPieceBackToSourceFinishedRequest: %#v", announcePeerRequest.DownloadPieceBackToSourceFinishedRequest)
			if err := v.handleDownloadPieceBackToSourceFinishedRequest(ctx, req.PeerId, announcePeerRequest.DownloadPieceBackToSourceFinishedRequest); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv2.AnnouncePeerRequest_DownloadPieceFailedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPieceFailedRequest: %#v", announcePeerRequest.DownloadPieceFailedRequest)
			if err := v.handleDownloadPieceFailedRequest(ctx, req.PeerId, announcePeerRequest.DownloadPieceFailedRequest); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv2.AnnouncePeerRequest_DownloadPieceBackToSourceFailedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPieceBackToSourceFailedRequest: %#v", announcePeerRequest.DownloadPieceBackToSourceFailedRequest)
			if err := v.handleDownloadPieceBackToSourceFailedRequest(ctx, req.PeerId, announcePeerRequest.DownloadPieceBackToSourceFailedRequest); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv2.AnnouncePeerRequest_SyncPiecesFailedRequest:
			logger.Infof("receive AnnouncePeerRequest_SyncPiecesFailedRequest: %#v", announcePeerRequest.SyncPiecesFailedRequest)
			v.handleSyncPiecesFailedRequest(ctx, announcePeerRequest.SyncPiecesFailedRequest)
		default:
			msg := fmt.Sprintf("receive unknow request: %#v", announcePeerRequest)
			logger.Error(msg)
			return status.Error(codes.FailedPrecondition, msg)
		}
	}
}

// StatPeer checks information of peer.
func (v *V2) StatPeer(ctx context.Context, req *schedulerv2.StatPeerRequest) (*commonv2.Peer, error) {
	logger.WithTaskID(req.TaskId).Infof("stat peer request: %#v", req)

	peer, loaded := v.resource.PeerManager().Load(req.PeerId)
	if !loaded {
		return nil, status.Errorf(codes.NotFound, "peer %s not found", req.PeerId)
	}

	resp := &commonv2.Peer{
		Id:               peer.ID,
		Priority:         peer.Priority,
		Cost:             durationpb.New(peer.Cost.Load()),
		State:            peer.FSM.Current(),
		NeedBackToSource: peer.NeedBackToSource.Load(),
		CreatedAt:        timestamppb.New(peer.CreatedAt.Load()),
		UpdatedAt:        timestamppb.New(peer.UpdatedAt.Load()),
	}

	// Set range to response.
	if peer.Range != nil {
		resp.Range = &commonv2.Range{
			Start:  peer.Range.Start,
			Length: peer.Range.Length,
		}
	}

	// Set pieces to response.
	peer.Pieces.Range(func(key, value any) bool {
		piece, ok := value.(*resource.Piece)
		if !ok {
			peer.Log.Errorf("invalid piece %s %#v", key, value)
			return true
		}

		respPiece := &commonv2.Piece{
			Number:      piece.Number,
			ParentId:    piece.ParentID,
			Offset:      piece.Offset,
			Length:      piece.Length,
			TrafficType: piece.TrafficType,
			Cost:        durationpb.New(piece.Cost),
			CreatedAt:   timestamppb.New(piece.CreatedAt),
		}

		if piece.Digest != nil {
			respPiece.Digest = piece.Digest.String()
		}

		resp.Pieces = append(resp.Pieces, respPiece)
		return true
	})

	// Set task to response.
	resp.Task = &commonv2.Task{
		Id:            peer.Task.ID,
		Type:          peer.Task.Type,
		Url:           peer.Task.URL,
		Tag:           peer.Task.Tag,
		Application:   peer.Task.Application,
		Filters:       peer.Task.Filters,
		Header:        peer.Task.Header,
		PieceLength:   peer.Task.PieceLength,
		ContentLength: peer.Task.ContentLength.Load(),
		PieceCount:    peer.Task.TotalPieceCount.Load(),
		SizeScope:     peer.Task.SizeScope(),
		State:         peer.Task.FSM.Current(),
		PeerCount:     int32(peer.Task.PeerCount()),
		CreatedAt:     timestamppb.New(peer.Task.CreatedAt.Load()),
		UpdatedAt:     timestamppb.New(peer.Task.UpdatedAt.Load()),
	}

	// Set digest to task response.
	if peer.Task.Digest != nil {
		resp.Task.Digest = peer.Task.Digest.String()
	}

	// Set pieces to task response.
	peer.Task.Pieces.Range(func(key, value any) bool {
		piece, ok := value.(*resource.Piece)
		if !ok {
			peer.Task.Log.Errorf("invalid piece %s %#v", key, value)
			return true
		}

		respPiece := &commonv2.Piece{
			Number:      piece.Number,
			ParentId:    piece.ParentID,
			Offset:      piece.Offset,
			Length:      piece.Length,
			TrafficType: piece.TrafficType,
			Cost:        durationpb.New(piece.Cost),
			CreatedAt:   timestamppb.New(piece.CreatedAt),
		}

		if piece.Digest != nil {
			respPiece.Digest = piece.Digest.String()
		}

		resp.Task.Pieces = append(resp.Task.Pieces, respPiece)
		return true
	})

	// Set host to response.
	resp.Host = &commonv2.Host{
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
	}

	return resp, nil
}

// LeavePeer releases peer in scheduler.
func (v *V2) LeavePeer(ctx context.Context, req *schedulerv2.LeavePeerRequest) error {
	logger.WithTaskAndPeerID(req.TaskId, req.PeerId).Infof("leave peer request: %#v", req)

	peer, loaded := v.resource.PeerManager().Load(req.PeerId)
	if !loaded {
		msg := fmt.Sprintf("peer %s not found", req.PeerId)
		logger.Error(msg)
		return status.Error(codes.NotFound, msg)
	}

	if err := peer.FSM.Event(ctx, resource.PeerEventLeave); err != nil {
		msg := fmt.Sprintf("peer fsm event failed: %s", err.Error())
		peer.Log.Error(msg)
		return status.Error(codes.FailedPrecondition, msg)
	}

	return nil
}

// TODO Implement function.
// ExchangePeer exchanges peer information.
func (v *V2) ExchangePeer(ctx context.Context, req *schedulerv2.ExchangePeerRequest) (*schedulerv2.ExchangePeerResponse, error) {
	return nil, nil
}

// StatTask checks information of task.
func (v *V2) StatTask(ctx context.Context, req *schedulerv2.StatTaskRequest) (*commonv2.Task, error) {
	logger.WithTaskID(req.Id).Infof("stat task request: %#v", req)

	task, loaded := v.resource.TaskManager().Load(req.Id)
	if !loaded {
		msg := fmt.Sprintf("task %s not found", req.Id)
		logger.Error(msg)
		return nil, status.Error(codes.NotFound, msg)
	}

	resp := &commonv2.Task{
		Id:            task.ID,
		Type:          task.Type,
		Url:           task.URL,
		Tag:           task.Tag,
		Application:   task.Application,
		Filters:       task.Filters,
		Header:        task.Header,
		PieceLength:   task.PieceLength,
		ContentLength: task.ContentLength.Load(),
		PieceCount:    task.TotalPieceCount.Load(),
		SizeScope:     task.SizeScope(),
		State:         task.FSM.Current(),
		PeerCount:     int32(task.PeerCount()),
		CreatedAt:     timestamppb.New(task.CreatedAt.Load()),
		UpdatedAt:     timestamppb.New(task.UpdatedAt.Load()),
	}

	// Set digest to response.
	if task.Digest != nil {
		resp.Digest = task.Digest.String()
	}

	// Set pieces to response.
	task.Pieces.Range(func(key, value any) bool {
		piece, ok := value.(*resource.Piece)
		if !ok {
			task.Log.Errorf("invalid piece %s %#v", key, value)
			return true
		}

		respPiece := &commonv2.Piece{
			Number:      piece.Number,
			ParentId:    piece.ParentID,
			Offset:      piece.Offset,
			Length:      piece.Length,
			TrafficType: piece.TrafficType,
			Cost:        durationpb.New(piece.Cost),
			CreatedAt:   timestamppb.New(piece.CreatedAt),
		}

		if piece.Digest != nil {
			respPiece.Digest = piece.Digest.String()
		}

		resp.Pieces = append(resp.Pieces, respPiece)
		return true
	})

	return resp, nil
}

// AnnounceHost announces host to scheduler.
func (v *V2) AnnounceHost(ctx context.Context, req *schedulerv2.AnnounceHostRequest) error {
	logger.WithHostID(req.Host.Id).Infof("announce host request: %#v", req.Host)

	// Get scheduler cluster client config by manager.
	var concurrentUploadLimit int32
	if clientConfig, err := v.dynconfig.GetSchedulerClusterClientConfig(); err == nil {
		concurrentUploadLimit = int32(clientConfig.LoadLimit)
	}

	host, loaded := v.resource.HostManager().Load(req.Host.Id)
	if !loaded {
		options := []resource.HostOption{
			resource.WithOS(req.Host.Os),
			resource.WithPlatform(req.Host.Platform),
			resource.WithPlatformFamily(req.Host.PlatformFamily),
			resource.WithPlatformVersion(req.Host.PlatformVersion),
			resource.WithKernelVersion(req.Host.KernelVersion),
		}

		if concurrentUploadLimit > 0 {
			options = append(options, resource.WithConcurrentUploadLimit(concurrentUploadLimit))
		}

		if req.Host.Cpu != nil {
			options = append(options, resource.WithCPU(resource.CPU{
				LogicalCount:   req.Host.Cpu.LogicalCount,
				PhysicalCount:  req.Host.Cpu.PhysicalCount,
				Percent:        req.Host.Cpu.Percent,
				ProcessPercent: req.Host.Cpu.ProcessPercent,
				Times: resource.CPUTimes{
					User:      req.Host.Cpu.Times.User,
					System:    req.Host.Cpu.Times.System,
					Idle:      req.Host.Cpu.Times.Idle,
					Nice:      req.Host.Cpu.Times.Nice,
					Iowait:    req.Host.Cpu.Times.Iowait,
					Irq:       req.Host.Cpu.Times.Irq,
					Softirq:   req.Host.Cpu.Times.Softirq,
					Steal:     req.Host.Cpu.Times.Steal,
					Guest:     req.Host.Cpu.Times.Guest,
					GuestNice: req.Host.Cpu.Times.GuestNice,
				},
			}))
		}

		if req.Host.Memory != nil {
			options = append(options, resource.WithMemory(resource.Memory{
				Total:              req.Host.Memory.Total,
				Available:          req.Host.Memory.Available,
				Used:               req.Host.Memory.Used,
				UsedPercent:        req.Host.Memory.UsedPercent,
				ProcessUsedPercent: req.Host.Memory.ProcessUsedPercent,
				Free:               req.Host.Memory.Free,
			}))
		}

		if req.Host.Network != nil {
			options = append(options, resource.WithNetwork(resource.Network{
				TCPConnectionCount:       req.Host.Network.TcpConnectionCount,
				UploadTCPConnectionCount: req.Host.Network.UploadTcpConnectionCount,
				Location:                 req.Host.Network.Location,
				IDC:                      req.Host.Network.Idc,
			}))
		}

		if req.Host.Disk != nil {
			options = append(options, resource.WithDisk(resource.Disk{
				Total:             req.Host.Disk.Total,
				Free:              req.Host.Disk.Free,
				Used:              req.Host.Disk.Used,
				UsedPercent:       req.Host.Disk.UsedPercent,
				InodesTotal:       req.Host.Disk.InodesTotal,
				InodesUsed:        req.Host.Disk.InodesUsed,
				InodesFree:        req.Host.Disk.InodesFree,
				InodesUsedPercent: req.Host.Disk.InodesUsedPercent,
			}))
		}

		if req.Host.Build != nil {
			options = append(options, resource.WithBuild(resource.Build{
				GitVersion: req.Host.Build.GitVersion,
				GitCommit:  req.Host.Build.GitCommit,
				GoVersion:  req.Host.Build.GoVersion,
				Platform:   req.Host.Build.Platform,
			}))
		}

		host = resource.NewHost(
			req.Host.Id, req.Host.Ip, req.Host.Hostname,
			req.Host.Port, req.Host.DownloadPort, types.HostType(req.Host.Type),
			options...,
		)

		v.resource.HostManager().Store(host)
		host.Log.Infof("announce new host: %#v", req)
		return nil
	}

	// Host already exists and updates properties.
	host.Port = req.Host.Port
	host.DownloadPort = req.Host.DownloadPort
	host.Type = types.HostType(req.Host.Type)
	host.OS = req.Host.Os
	host.Platform = req.Host.Platform
	host.PlatformFamily = req.Host.PlatformFamily
	host.PlatformVersion = req.Host.PlatformVersion
	host.KernelVersion = req.Host.KernelVersion
	host.UpdatedAt.Store(time.Now())

	if concurrentUploadLimit > 0 {
		host.ConcurrentUploadLimit.Store(concurrentUploadLimit)
	}

	if req.Host.Cpu != nil {
		host.CPU = resource.CPU{
			LogicalCount:   req.Host.Cpu.LogicalCount,
			PhysicalCount:  req.Host.Cpu.PhysicalCount,
			Percent:        req.Host.Cpu.Percent,
			ProcessPercent: req.Host.Cpu.ProcessPercent,
			Times: resource.CPUTimes{
				User:      req.Host.Cpu.Times.User,
				System:    req.Host.Cpu.Times.System,
				Idle:      req.Host.Cpu.Times.Idle,
				Nice:      req.Host.Cpu.Times.Nice,
				Iowait:    req.Host.Cpu.Times.Iowait,
				Irq:       req.Host.Cpu.Times.Irq,
				Softirq:   req.Host.Cpu.Times.Softirq,
				Steal:     req.Host.Cpu.Times.Steal,
				Guest:     req.Host.Cpu.Times.Guest,
				GuestNice: req.Host.Cpu.Times.GuestNice,
			},
		}
	}

	if req.Host.Memory != nil {
		host.Memory = resource.Memory{
			Total:              req.Host.Memory.Total,
			Available:          req.Host.Memory.Available,
			Used:               req.Host.Memory.Used,
			UsedPercent:        req.Host.Memory.UsedPercent,
			ProcessUsedPercent: req.Host.Memory.ProcessUsedPercent,
			Free:               req.Host.Memory.Free,
		}
	}

	if req.Host.Network != nil {
		host.Network = resource.Network{
			TCPConnectionCount:       req.Host.Network.TcpConnectionCount,
			UploadTCPConnectionCount: req.Host.Network.UploadTcpConnectionCount,
			Location:                 req.Host.Network.Location,
			IDC:                      req.Host.Network.Idc,
		}
	}

	if req.Host.Disk != nil {
		host.Disk = resource.Disk{
			Total:             req.Host.Disk.Total,
			Free:              req.Host.Disk.Free,
			Used:              req.Host.Disk.Used,
			UsedPercent:       req.Host.Disk.UsedPercent,
			InodesTotal:       req.Host.Disk.InodesTotal,
			InodesUsed:        req.Host.Disk.InodesUsed,
			InodesFree:        req.Host.Disk.InodesFree,
			InodesUsedPercent: req.Host.Disk.InodesUsedPercent,
		}
	}

	if req.Host.Build != nil {
		host.Build = resource.Build{
			GitVersion: req.Host.Build.GitVersion,
			GitCommit:  req.Host.Build.GitCommit,
			GoVersion:  req.Host.Build.GoVersion,
			Platform:   req.Host.Build.Platform,
		}
	}

	return nil
}

// LeaveHost releases host in scheduler.
func (v *V2) LeaveHost(ctx context.Context, req *schedulerv2.LeaveHostRequest) error {
	logger.WithHostID(req.Id).Infof("leave host request: %#v", req)

	host, loaded := v.resource.HostManager().Load(req.Id)
	if !loaded {
		msg := fmt.Sprintf("host %s not found", req.Id)
		logger.Error(msg)
		return status.Error(codes.NotFound, msg)
	}

	host.LeavePeers()
	return nil
}

// SyncProbes sync probes of the host.
func (v *V2) SyncProbes(stream schedulerv2.Scheduler_SyncProbesServer) error {
	if v.networkTopology == nil {
		return status.Errorf(codes.Unimplemented, "network topology is not enabled")
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			logger.Errorf("receive error: %s", err.Error())
			return err
		}

		logger := logger.WithHost(req.Host.Id, req.Host.Hostname, req.Host.Ip)
		switch syncProbesRequest := req.GetRequest().(type) {
		case *schedulerv2.SyncProbesRequest_ProbeStartedRequest:
			// Find probed hosts in network topology. Based on the source host information,
			// the most candidate hosts will be evaluated.
			logger.Info("receive SyncProbesRequest_ProbeStartedRequest")
			probedHostIDs, err := v.networkTopology.FindProbedHostIDs(req.Host.Id)
			if err != nil {
				logger.Error(err)
				return status.Error(codes.FailedPrecondition, err.Error())
			}

			var probedHosts []*commonv2.Host
			for _, probedHostID := range probedHostIDs {
				probedHost, loaded := v.resource.HostManager().Load(probedHostID)
				if !loaded {
					logger.Warnf("probed host %s not found", probedHostID)
					continue
				}

				probedHosts = append(probedHosts, &commonv2.Host{
					Id:              probedHost.ID,
					Type:            uint32(probedHost.Type),
					Hostname:        probedHost.Hostname,
					Ip:              probedHost.IP,
					Port:            probedHost.Port,
					DownloadPort:    probedHost.DownloadPort,
					Os:              probedHost.OS,
					Platform:        probedHost.Platform,
					PlatformFamily:  probedHost.PlatformFamily,
					PlatformVersion: probedHost.PlatformVersion,
					KernelVersion:   probedHost.KernelVersion,
					Cpu: &commonv2.CPU{
						LogicalCount:   probedHost.CPU.LogicalCount,
						PhysicalCount:  probedHost.CPU.PhysicalCount,
						Percent:        probedHost.CPU.Percent,
						ProcessPercent: probedHost.CPU.ProcessPercent,
						Times: &commonv2.CPUTimes{
							User:      probedHost.CPU.Times.User,
							System:    probedHost.CPU.Times.System,
							Idle:      probedHost.CPU.Times.Idle,
							Nice:      probedHost.CPU.Times.Nice,
							Iowait:    probedHost.CPU.Times.Iowait,
							Irq:       probedHost.CPU.Times.Irq,
							Softirq:   probedHost.CPU.Times.Softirq,
							Steal:     probedHost.CPU.Times.Steal,
							Guest:     probedHost.CPU.Times.Guest,
							GuestNice: probedHost.CPU.Times.GuestNice,
						},
					},
					Memory: &commonv2.Memory{
						Total:              probedHost.Memory.Total,
						Available:          probedHost.Memory.Available,
						Used:               probedHost.Memory.Used,
						UsedPercent:        probedHost.Memory.UsedPercent,
						ProcessUsedPercent: probedHost.Memory.ProcessUsedPercent,
						Free:               probedHost.Memory.Free,
					},
					Network: &commonv2.Network{
						TcpConnectionCount:       probedHost.Network.TCPConnectionCount,
						UploadTcpConnectionCount: probedHost.Network.UploadTCPConnectionCount,
						Location:                 probedHost.Network.Location,
						Idc:                      probedHost.Network.IDC,
					},
					Disk: &commonv2.Disk{
						Total:             probedHost.Disk.Total,
						Free:              probedHost.Disk.Free,
						Used:              probedHost.Disk.Used,
						UsedPercent:       probedHost.Disk.UsedPercent,
						InodesTotal:       probedHost.Disk.InodesTotal,
						InodesUsed:        probedHost.Disk.InodesUsed,
						InodesFree:        probedHost.Disk.InodesFree,
						InodesUsedPercent: probedHost.Disk.InodesUsedPercent,
					},
					Build: &commonv2.Build{
						GitVersion: probedHost.Build.GitVersion,
						GitCommit:  probedHost.Build.GitCommit,
						GoVersion:  probedHost.Build.GoVersion,
						Platform:   probedHost.Build.Platform,
					},
				})
			}

			if len(probedHosts) == 0 {
				logger.Error("probed host not found")
				return status.Error(codes.NotFound, "probed host not found")
			}

			logger.Infof("probe started: %#v", probedHosts)
			if err := stream.Send(&schedulerv2.SyncProbesResponse{
				Hosts: probedHosts,
			}); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv2.SyncProbesRequest_ProbeFinishedRequest:
			// Store probes in network topology. First create the association between
			// source host and destination host, and then store the value of probe.
			logger.Info("receive SyncProbesRequest_ProbeFinishedRequest")
			for _, probe := range syncProbesRequest.ProbeFinishedRequest.Probes {
				probedHost, loaded := v.resource.HostManager().Load(probe.Host.Id)
				if !loaded {
					logger.Errorf("host %s not found", probe.Host.Id)
					continue
				}

				if err := v.networkTopology.Store(req.Host.Id, probedHost.ID); err != nil {
					logger.Errorf("store failed: %s", err.Error())
					continue
				}

				if err := v.networkTopology.Probes(req.Host.Id, probe.Host.Id).Enqueue(&networktopology.Probe{
					Host:      probedHost,
					RTT:       probe.Rtt.AsDuration(),
					CreatedAt: probe.CreatedAt.AsTime(),
				}); err != nil {
					logger.Errorf("enqueue failed: %s", err.Error())
					continue
				}

				logger.Infof("probe finished: %#v", probe)
			}
		case *schedulerv2.SyncProbesRequest_ProbeFailedRequest:
			// Log failed probes.
			logger.Info("receive SyncProbesRequest_ProbeFailedRequest")
			var failedProbedHostIDs []string
			for _, failedProbe := range syncProbesRequest.ProbeFailedRequest.Probes {
				failedProbedHostIDs = append(failedProbedHostIDs, failedProbe.Host.Id)
			}

			logger.Warnf("probe failed: %#v", failedProbedHostIDs)
		default:
			msg := fmt.Sprintf("receive unknow request: %#v", syncProbesRequest)
			logger.Error(msg)
			return status.Error(codes.FailedPrecondition, msg)
		}
	}
}

// handleRegisterPeerRequest handles RegisterPeerRequest of AnnouncePeerRequest.
func (v *V2) handleRegisterPeerRequest(ctx context.Context, stream schedulerv2.Scheduler_AnnouncePeerServer, hostID, taskID, peerID string, req *schedulerv2.RegisterPeerRequest) error {
	// Handle resource included host, task, and peer.
	_, task, peer, err := v.handleResource(ctx, stream, hostID, taskID, peerID, req.Download)
	if err != nil {
		return err
	}

	// Collect RegisterPeerCount metrics.
	priority := peer.CalculatePriority(v.dynconfig)
	metrics.RegisterPeerCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()

	// When there are no available peers for a task, the scheduler needs to trigger
	// the first task download in the p2p cluster.
	blocklist := set.NewSafeSet[string]()
	blocklist.Add(peer.ID)
	if task.FSM.Is(resource.TaskStateFailed) || !task.HasAvailablePeer(blocklist) {
		if err := v.downloadTaskBySeedPeer(ctx, peer); err != nil {
			// Collect RegisterPeerFailureCount metrics.
			metrics.RegisterPeerFailureCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
				peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
			return err
		}
	}

	// Scheduling parent for the peer.
	if err := v.schedule(ctx, peer); err != nil {
		// Collect RegisterPeerFailureCount metrics.
		metrics.RegisterPeerFailureCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
			peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
		return err
	}

	return nil
}

// handleRegisterSeedPeerRequest handles RegisterSeedPeerRequest of AnnouncePeerRequest.
func (v *V2) handleRegisterSeedPeerRequest(ctx context.Context, stream schedulerv2.Scheduler_AnnouncePeerServer, hostID, taskID, peerID string, req *schedulerv2.RegisterSeedPeerRequest) error {
	// Handle resource included host, task, and peer.
	_, task, peer, err := v.handleResource(ctx, stream, hostID, taskID, peerID, req.Download)
	if err != nil {
		return err
	}

	// Collect RegisterPeerCount metrics.
	priority := peer.CalculatePriority(v.dynconfig)
	metrics.RegisterPeerCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()

	// When there are no available peers for a task, the scheduler needs to trigger
	// the first task download in the p2p cluster.
	blocklist := set.NewSafeSet[string]()
	blocklist.Add(peer.ID)
	if task.FSM.Is(resource.TaskStateFailed) || !task.HasAvailablePeer(blocklist) {
		// When the task has no available peer,
		// the seed peer will download back-to-source directly.
		peer.NeedBackToSource.Store(true)
	}

	// Scheduling parent for the peer.
	if err := v.schedule(ctx, peer); err != nil {
		// Collect RegisterPeerFailureCount metrics.
		metrics.RegisterPeerFailureCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
			peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
		return err
	}

	return nil
}

// handleDownloadPeerStartedRequest handles DownloadPeerStartedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPeerStartedRequest(ctx context.Context, peerID string) error {
	peer, loaded := v.resource.PeerManager().Load(peerID)
	if !loaded {
		return status.Errorf(codes.NotFound, "peer %s not found", peerID)
	}

	// Collect DownloadPeerStartedCount metrics.
	priority := peer.CalculatePriority(v.dynconfig)
	metrics.DownloadPeerStartedCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()

	// Handle peer with peer started request.
	if err := peer.FSM.Event(ctx, resource.PeerEventDownload); err != nil {
		// Collect DownloadPeerStartedFailureCount metrics.
		metrics.DownloadPeerStartedFailureCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
			peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
		return status.Error(codes.Internal, err.Error())
	}

	// Handle task with peer started request.
	if !peer.Task.FSM.Is(resource.TaskStateRunning) {
		if err := peer.Task.FSM.Event(ctx, resource.TaskEventDownload); err != nil {
			// Collect DownloadPeerStartedFailureCount metrics.
			metrics.DownloadPeerStartedFailureCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
				peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
			return status.Error(codes.Internal, err.Error())
		}
	} else {
		peer.Task.UpdatedAt.Store(time.Now())
	}

	return nil
}

// handleDownloadPeerBackToSourceStartedRequest handles DownloadPeerBackToSourceStartedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPeerBackToSourceStartedRequest(ctx context.Context, peerID string) error {
	peer, loaded := v.resource.PeerManager().Load(peerID)
	if !loaded {
		return status.Errorf(codes.NotFound, "peer %s not found", peerID)
	}

	// Collect DownloadPeerBackToSourceStartedCount metrics.
	priority := peer.CalculatePriority(v.dynconfig)
	metrics.DownloadPeerBackToSourceStartedCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()

	// Handle peer with peer back-to-source started request.
	if err := peer.FSM.Event(ctx, resource.PeerEventDownloadBackToSource); err != nil {
		// Collect DownloadPeerBackToSourceStartedFailureCount metrics.
		metrics.DownloadPeerBackToSourceStartedFailureCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
			peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
		return status.Error(codes.Internal, err.Error())
	}

	// Handle task with peer back-to-source started request.
	if !peer.Task.FSM.Is(resource.TaskStateRunning) {
		if err := peer.Task.FSM.Event(ctx, resource.TaskEventDownload); err != nil {
			// Collect DownloadPeerBackToSourceStartedFailureCount metrics.
			metrics.DownloadPeerBackToSourceStartedFailureCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
				peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
			return status.Error(codes.Internal, err.Error())
		}
	} else {
		peer.Task.UpdatedAt.Store(time.Now())
	}

	return nil
}

// handleDownloadPeerFinishedRequest handles DownloadPeerFinishedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPeerFinishedRequest(ctx context.Context, peerID string) error {
	peer, loaded := v.resource.PeerManager().Load(peerID)
	if !loaded {
		return status.Errorf(codes.NotFound, "peer %s not found", peerID)
	}

	// Handle peer with peer finished request.
	peer.Cost.Store(time.Since(peer.CreatedAt.Load()))
	if err := peer.FSM.Event(ctx, resource.PeerEventDownloadSucceeded); err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	// Collect DownloadPeerCount and DownloadPeerDuration metrics.
	priority := peer.CalculatePriority(v.dynconfig)
	metrics.DownloadPeerCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
	// TODO to be determined which traffic type to use, temporarily use TrafficType_REMOTE_PEER instead
	metrics.DownloadPeerDuration.WithLabelValues(priority.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Observe(float64(peer.Cost.Load()))

	return nil
}

// handleDownloadPeerBackToSourceFinishedRequest handles DownloadPeerBackToSourceFinishedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPeerBackToSourceFinishedRequest(ctx context.Context, peerID string, req *schedulerv2.DownloadPeerBackToSourceFinishedRequest) error {
	peer, loaded := v.resource.PeerManager().Load(peerID)
	if !loaded {
		return status.Errorf(codes.NotFound, "peer %s not found", peerID)
	}

	// Handle peer with peer back-to-source finished request.
	peer.Cost.Store(time.Since(peer.CreatedAt.Load()))
	if err := peer.FSM.Event(ctx, resource.PeerEventDownloadSucceeded); err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	// Handle task with peer back-to-source finished request, peer can only represent
	// a successful task after downloading the complete task.
	if peer.Range == nil && !peer.Task.FSM.Is(resource.TaskStateSucceeded) {
		peer.Task.ContentLength.Store(req.ContentLength)
		peer.Task.TotalPieceCount.Store(req.PieceCount)
		if err := peer.Task.FSM.Event(ctx, resource.TaskEventDownloadSucceeded); err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		// If the task size scope is tiny, scheduler needs to download the tiny file from peer and
		// store the data in task DirectPiece.
		if peer.Task.SizeScope() == commonv2.SizeScope_TINY {
			data, err := peer.DownloadTinyFile()
			if err != nil {
				peer.Log.Errorf("download failed: %s", err.Error())
				return nil
			}

			if len(data) != int(peer.Task.ContentLength.Load()) {
				peer.Log.Errorf("data length %d is not equal content length %d", len(data), peer.Task.ContentLength.Load())
				return nil
			}

			peer.Task.DirectPiece = data
		}
	}

	// Collect DownloadPeerCount and DownloadPeerDuration metrics.
	priority := peer.CalculatePriority(v.dynconfig)
	metrics.DownloadPeerCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
	// TODO to be determined which traffic type to use, temporarily use TrafficType_REMOTE_PEER instead
	metrics.DownloadPeerDuration.WithLabelValues(priority.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Observe(float64(peer.Cost.Load()))

	return nil
}

// handleDownloadPeerFailedRequest handles DownloadPeerFailedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPeerFailedRequest(ctx context.Context, peerID string) error {
	peer, loaded := v.resource.PeerManager().Load(peerID)
	if !loaded {
		return status.Errorf(codes.NotFound, "peer %s not found", peerID)
	}

	// Handle peer with peer failed request.
	if err := peer.FSM.Event(ctx, resource.PeerEventDownloadFailed); err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	// Handle task with peer failed request.
	peer.Task.UpdatedAt.Store(time.Now())

	// Collect DownloadPeerCount and DownloadPeerFailureCount metrics.
	priority := peer.CalculatePriority(v.dynconfig)
	metrics.DownloadPeerCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
	metrics.DownloadPeerFailureCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()

	return nil
}

// handleDownloadPeerBackToSourceFailedRequest handles DownloadPeerBackToSourceFailedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPeerBackToSourceFailedRequest(ctx context.Context, peerID string) error {
	peer, loaded := v.resource.PeerManager().Load(peerID)
	if !loaded {
		return status.Errorf(codes.NotFound, "peer %s not found", peerID)
	}

	// Handle peer with peer back-to-source failed request.
	if err := peer.FSM.Event(ctx, resource.PeerEventDownloadFailed); err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	// Handle task with peer back-to-source failed request.
	peer.Task.ContentLength.Store(-1)
	peer.Task.TotalPieceCount.Store(0)
	peer.Task.DirectPiece = []byte{}
	if err := peer.Task.FSM.Event(ctx, resource.TaskEventDownloadFailed); err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	// Collect DownloadPeerCount and DownloadPeerBackToSourceFailureCount metrics.
	priority := peer.CalculatePriority(v.dynconfig)
	metrics.DownloadPeerCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
	metrics.DownloadPeerBackToSourceFailureCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()

	return nil
}

// handleDownloadPieceFinishedRequest handles DownloadPieceFinishedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPieceFinishedRequest(ctx context.Context, peerID string, req *schedulerv2.DownloadPieceFinishedRequest) error {
	// Construct piece.
	piece := &resource.Piece{
		Number:      req.Piece.Number,
		ParentID:    req.Piece.ParentId,
		Offset:      req.Piece.Offset,
		Length:      req.Piece.Length,
		TrafficType: req.Piece.TrafficType,
		Cost:        req.Piece.Cost.AsDuration(),
		CreatedAt:   req.Piece.CreatedAt.AsTime(),
	}

	if len(req.Piece.Digest) > 0 {
		d, err := digest.Parse(req.Piece.Digest)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, err.Error())
		}

		piece.Digest = d
	}

	peer, loaded := v.resource.PeerManager().Load(peerID)
	if !loaded {
		return status.Errorf(codes.NotFound, "peer %s not found", peerID)
	}

	// Handle peer with piece finished request. When the piece is downloaded successfully, peer.UpdatedAt needs
	// to be updated to prevent the peer from being GC during the download process.
	peer.StorePiece(piece)
	peer.FinishedPieces.Set(uint(piece.Number))
	peer.AppendPieceCost(piece.Cost)
	peer.PieceUpdatedAt.Store(time.Now())
	peer.UpdatedAt.Store(time.Now())

	// When the piece is downloaded successfully, parent.UpdatedAt needs to be updated
	// to prevent the parent from being GC during the download process.
	parent, loadedParent := v.resource.PeerManager().Load(piece.ParentID)
	if loadedParent {
		parent.UpdatedAt.Store(time.Now())
		parent.Host.UpdatedAt.Store(time.Now())
	}

	// Handle task with piece finished request.
	peer.Task.UpdatedAt.Store(time.Now())

	// Collect piece and traffic metrics.
	metrics.DownloadPieceCount.WithLabelValues(piece.TrafficType.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
	metrics.Traffic.WithLabelValues(piece.TrafficType.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Add(float64(piece.Length))
	if v.config.Metrics.EnableHost {
		metrics.HostTraffic.WithLabelValues(metrics.HostTrafficDownloadType, peer.Task.Type.String(), peer.Task.Tag, peer.Task.Application,
			peer.Host.Type.Name(), peer.Host.ID, peer.Host.IP, peer.Host.Hostname).Add(float64(piece.Length))
		if loadedParent {
			metrics.HostTraffic.WithLabelValues(metrics.HostTrafficUploadType, peer.Task.Type.String(), peer.Task.Tag, peer.Task.Application,
				parent.Host.Type.Name(), parent.Host.ID, parent.Host.IP, parent.Host.Hostname).Add(float64(piece.Length))
		}
	}

	return nil
}

// handleDownloadPieceBackToSourceFinishedRequest handles DownloadPieceBackToSourceFinishedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPieceBackToSourceFinishedRequest(ctx context.Context, peerID string, req *schedulerv2.DownloadPieceBackToSourceFinishedRequest) error {
	// Construct piece.
	piece := &resource.Piece{
		Number:      req.Piece.Number,
		ParentID:    req.Piece.ParentId,
		Offset:      req.Piece.Offset,
		Length:      req.Piece.Length,
		TrafficType: req.Piece.TrafficType,
		Cost:        req.Piece.Cost.AsDuration(),
		CreatedAt:   req.Piece.CreatedAt.AsTime(),
	}

	if len(req.Piece.Digest) > 0 {
		d, err := digest.Parse(req.Piece.Digest)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, err.Error())
		}

		piece.Digest = d
	}

	peer, loaded := v.resource.PeerManager().Load(peerID)
	if !loaded {
		return status.Errorf(codes.NotFound, "peer %s not found", peerID)
	}

	// Handle peer with piece back-to-source finished request. When the piece is downloaded successfully, peer.UpdatedAt
	// needs to be updated to prevent the peer from being GC during the download process.
	peer.StorePiece(piece)
	peer.FinishedPieces.Set(uint(piece.Number))
	peer.AppendPieceCost(piece.Cost)
	peer.PieceUpdatedAt.Store(time.Now())
	peer.UpdatedAt.Store(time.Now())

	// Handle task with piece back-to-source finished request.
	peer.Task.StorePiece(piece)
	peer.Task.UpdatedAt.Store(time.Now())

	// Collect piece and traffic metrics.
	metrics.DownloadPieceCount.WithLabelValues(piece.TrafficType.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
	metrics.Traffic.WithLabelValues(piece.TrafficType.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Add(float64(piece.Length))
	if v.config.Metrics.EnableHost {
		metrics.HostTraffic.WithLabelValues(metrics.HostTrafficDownloadType, peer.Task.Type.String(), peer.Task.Tag, peer.Task.Application,
			peer.Host.Type.Name(), peer.Host.ID, peer.Host.IP, peer.Host.Hostname).Add(float64(piece.Length))
	}

	return nil
}

// handleDownloadPieceFailedRequest handles DownloadPieceFailedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPieceFailedRequest(ctx context.Context, peerID string, req *schedulerv2.DownloadPieceFailedRequest) error {
	peer, loaded := v.resource.PeerManager().Load(peerID)
	if !loaded {
		return status.Errorf(codes.NotFound, "peer %s not found", peerID)
	}

	// Collect DownloadPieceCount and DownloadPieceFailureCount metrics.
	metrics.DownloadPieceCount.WithLabelValues(req.Piece.TrafficType.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
	metrics.DownloadPieceFailureCount.WithLabelValues(req.Piece.TrafficType.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()

	if req.Temporary {
		// Handle peer with piece temporary failed request.
		peer.UpdatedAt.Store(time.Now())
		peer.BlockParents.Add(req.Piece.ParentId)
		if err := v.scheduling.ScheduleCandidateParents(ctx, peer, peer.BlockParents); err != nil {
			return status.Error(codes.FailedPrecondition, err.Error())
		}

		if parent, loaded := v.resource.PeerManager().Load(req.Piece.ParentId); loaded {
			parent.Host.UploadFailedCount.Inc()
		}

		// Handle task with piece temporary failed request.
		peer.Task.UpdatedAt.Store(time.Now())
		return nil
	}

	return status.Error(codes.FailedPrecondition, "download piece failed")
}

// handleDownloadPieceBackToSourceFailedRequest handles DownloadPieceBackToSourceFailedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPieceBackToSourceFailedRequest(ctx context.Context, peerID string, req *schedulerv2.DownloadPieceBackToSourceFailedRequest) error {
	peer, loaded := v.resource.PeerManager().Load(peerID)
	if !loaded {
		return status.Errorf(codes.NotFound, "peer %s not found", peerID)
	}

	// Handle peer with piece back-to-source failed request.
	peer.UpdatedAt.Store(time.Now())

	// Handle task with piece back-to-source failed request.
	peer.Task.UpdatedAt.Store(time.Now())

	// Collect DownloadPieceCount and DownloadPieceFailureCount metrics.
	metrics.DownloadPieceCount.WithLabelValues(req.Piece.TrafficType.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()
	metrics.DownloadPieceFailureCount.WithLabelValues(req.Piece.TrafficType.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()

	return status.Error(codes.Internal, "download piece from source failed")
}

// TODO Implement function.
// handleSyncPiecesFailedRequest handles SyncPiecesFailedRequest of AnnouncePeerRequest.
func (v *V2) handleSyncPiecesFailedRequest(ctx context.Context, req *schedulerv2.SyncPiecesFailedRequest) {
}

// handleResource handles resource included host, task, and peer.
func (v *V2) handleResource(ctx context.Context, stream schedulerv2.Scheduler_AnnouncePeerServer, hostID, taskID, peerID string, download *commonv2.Download) (*resource.Host, *resource.Task, *resource.Peer, error) {
	// If the host does not exist and the host address cannot be found,
	// it may cause an exception.
	host, loaded := v.resource.HostManager().Load(hostID)
	if !loaded {
		return nil, nil, nil, status.Errorf(codes.NotFound, "host %s not found", hostID)
	}

	// Store new task or update task.
	task, loaded := v.resource.TaskManager().Load(taskID)
	if !loaded {
		options := []resource.TaskOption{resource.WithPieceLength(download.PieceLength)}
		if download.Digest != "" {
			d, err := digest.Parse(download.Digest)
			if err != nil {
				return nil, nil, nil, status.Error(codes.InvalidArgument, err.Error())
			}

			// If request has invalid digest, then new task with the nil digest.
			options = append(options, resource.WithDigest(d))
		}

		task = resource.NewTask(taskID, download.Url, download.Tag, download.Application, download.Type,
			download.Filters, download.Header, int32(v.config.Scheduler.BackToSourceCount), options...)
		v.resource.TaskManager().Store(task)
	} else {
		task.URL = download.Url
		task.Filters = download.Filters
		task.Header = download.Header
	}

	// Store new peer or load peer.
	peer, loaded := v.resource.PeerManager().Load(peerID)
	if !loaded {
		options := []resource.PeerOption{resource.WithPriority(download.Priority), resource.WithAnnouncePeerStream(stream)}
		if download.Range != nil {
			options = append(options, resource.WithRange(http.Range{Start: download.Range.Start, Length: download.Range.Length}))
		}

		peer = resource.NewPeer(peerID, task, host, options...)
		v.resource.PeerManager().Store(peer)
	}

	return host, task, peer, nil
}

// downloadTaskBySeedPeer downloads task by seed peer.
func (v *V2) downloadTaskBySeedPeer(ctx context.Context, peer *resource.Peer) error {
	// Trigger the first download task based on different priority levels,
	// refer to https://github.com/dragonflyoss/api/blob/main/pkg/apis/common/v2/common.proto#L74.
	priority := peer.CalculatePriority(v.dynconfig)
	peer.Log.Infof("peer priority is %s", priority.String())
	switch priority {
	case commonv2.Priority_LEVEL6, commonv2.Priority_LEVEL0:
		// Super peer is first triggered to download back-to-source.
		if v.config.SeedPeer.Enable && !peer.Task.IsSeedPeerFailed() {
			go func(ctx context.Context, peer *resource.Peer, hostType types.HostType) {
				if err := v.resource.SeedPeer().DownloadTask(context.Background(), peer.Task, hostType); err != nil {
					peer.Log.Errorf("%s seed peer downloads task failed %s", hostType.Name(), err.Error())
					return
				}
			}(ctx, peer, types.HostTypeSuperSeed)
			break
		}

		fallthrough
	case commonv2.Priority_LEVEL5:
		// Strong peer is first triggered to download back-to-source.
		if v.config.SeedPeer.Enable && !peer.Task.IsSeedPeerFailed() {
			go func(ctx context.Context, peer *resource.Peer, hostType types.HostType) {
				if err := v.resource.SeedPeer().DownloadTask(context.Background(), peer.Task, hostType); err != nil {
					peer.Log.Errorf("%s seed peer downloads task failed %s", hostType.Name(), err.Error())
					return
				}
			}(ctx, peer, types.HostTypeStrongSeed)
			break
		}

		fallthrough
	case commonv2.Priority_LEVEL4:
		// Weak peer is first triggered to download back-to-source.
		if v.config.SeedPeer.Enable && !peer.Task.IsSeedPeerFailed() {
			go func(ctx context.Context, peer *resource.Peer, hostType types.HostType) {
				if err := v.resource.SeedPeer().DownloadTask(context.Background(), peer.Task, hostType); err != nil {
					peer.Log.Errorf("%s seed peer downloads task failed %s", hostType.Name(), err.Error())
					return
				}
			}(ctx, peer, types.HostTypeWeakSeed)
			break
		}

		fallthrough
	case commonv2.Priority_LEVEL3:
		// When the task has no available peer,
		// the peer is first to download back-to-source.
		peer.NeedBackToSource.Store(true)
	case commonv2.Priority_LEVEL2:
		// Peer is first to download back-to-source.
		return status.Errorf(codes.NotFound, "%s peer not found candidate peers", commonv2.Priority_LEVEL2.String())
	case commonv2.Priority_LEVEL1:
		// Download task is forbidden.
		return status.Errorf(codes.FailedPrecondition, "%s peer is forbidden", commonv2.Priority_LEVEL1.String())
	default:
		return status.Errorf(codes.InvalidArgument, "invalid priority %#v", priority)
	}

	return nil
}

// schedule provides different scheduling strategies for different task type.
func (v *V2) schedule(ctx context.Context, peer *resource.Peer) error {
	sizeScope := peer.Task.SizeScope()
	switch sizeScope {
	case commonv2.SizeScope_EMPTY:
		// Return an EmptyTaskResponse directly.
		peer.Log.Info("scheduling as SizeScope_EMPTY")
		stream, loaded := peer.LoadAnnouncePeerStream()
		if !loaded {
			return status.Error(codes.NotFound, "AnnouncePeerStream not found")
		}

		if err := peer.FSM.Event(ctx, resource.PeerEventRegisterEmpty); err != nil {
			return status.Errorf(codes.Internal, err.Error())
		}

		if err := stream.Send(&schedulerv2.AnnouncePeerResponse{
			Response: &schedulerv2.AnnouncePeerResponse_EmptyTaskResponse{
				EmptyTaskResponse: &schedulerv2.EmptyTaskResponse{},
			},
		}); err != nil {
			peer.Log.Error(err)
			return status.Error(codes.Internal, err.Error())
		}

		return nil
	case commonv2.SizeScope_TINY:
		// If the task.DirectPiece of the task can be reused, the data of
		// the task will be included in the TinyTaskResponse.
		// If the task.DirectPiece cannot be reused,
		// it will be scheduled as a Normal Task.
		peer.Log.Info("scheduling as SizeScope_TINY")
		if !peer.Task.CanReuseDirectPiece() {
			peer.Log.Warnf("can not reuse direct piece %d %d", len(peer.Task.DirectPiece), peer.Task.ContentLength.Load())
			break
		}

		stream, loaded := peer.LoadAnnouncePeerStream()
		if !loaded {
			return status.Error(codes.NotFound, "AnnouncePeerStream not found")
		}

		if err := peer.FSM.Event(ctx, resource.PeerEventRegisterTiny); err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		if err := stream.Send(&schedulerv2.AnnouncePeerResponse{
			Response: &schedulerv2.AnnouncePeerResponse_TinyTaskResponse{
				TinyTaskResponse: &schedulerv2.TinyTaskResponse{
					Data: peer.Task.DirectPiece,
				},
			},
		}); err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		return nil
	case commonv2.SizeScope_SMALL:
		// If a parent with the state of PeerStateSucceeded can be found in the task,
		// its information will be returned. If a parent with the state of
		// PeerStateSucceeded cannot be found in the task,
		// it will be scheduled as a Normal Task.
		peer.Log.Info("scheduling as SizeScope_SMALL")
		parent, found := v.scheduling.FindSuccessParent(ctx, peer, set.NewSafeSet[string]())
		if !found {
			peer.Log.Warn("candidate parents not found")
			break
		}

		// Delete inedges of peer.
		if err := peer.Task.DeletePeerInEdges(peer.ID); err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		// Add edges between success parent and peer.
		if err := peer.Task.AddPeerEdge(parent, peer); err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		stream, loaded := peer.LoadAnnouncePeerStream()
		if !loaded {
			return status.Error(codes.NotFound, "AnnouncePeerStream not found")
		}

		if err := peer.FSM.Event(ctx, resource.PeerEventRegisterSmall); err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		if err := stream.Send(&schedulerv2.AnnouncePeerResponse{
			Response: scheduling.ConstructSuccessSmallTaskResponse(parent),
		}); err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		return nil
	case commonv2.SizeScope_NORMAL, commonv2.SizeScope_UNKNOW:
	default:
		return status.Errorf(codes.FailedPrecondition, "invalid size cope %#v", sizeScope)
	}

	// Scheduling as a normal task, it will control how peers download tasks
	// based on RetryLimit and RetryBackToSourceLimit configurations.
	peer.Log.Info("scheduling as SizeScope_NORMAL")
	if err := peer.FSM.Event(ctx, resource.PeerEventRegisterNormal); err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	if err := v.scheduling.ScheduleCandidateParents(ctx, peer, set.NewSafeSet[string]()); err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	return nil
}
