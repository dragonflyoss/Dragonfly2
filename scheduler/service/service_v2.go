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
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
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
}

// New v2 version of service instance.
func NewV2(
	cfg *config.Config,
	resource resource.Resource,
	scheduling scheduling.Scheduling,
	dynconfig config.DynconfigInterface,
	storage storage.Storage,
) *V2 {
	return &V2{
		resource:   resource,
		scheduling: scheduling,
		config:     cfg,
		dynconfig:  dynconfig,
		storage:    storage,
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
			logger.Infof("receive AnnouncePeerRequest_RegisterPeerRequest: %#v", announcePeerRequest.RegisterPeerRequest.Download)
			if err := v.handleRegisterPeerRequest(req.HostId, req.TaskId, req.PeerId, announcePeerRequest.RegisterPeerRequest); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv2.AnnouncePeerRequest_DownloadPeerStartedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPeerStartedRequest: %#v", announcePeerRequest.DownloadPeerStartedRequest)
			v.handleDownloadPeerStartedRequest(announcePeerRequest.DownloadPeerStartedRequest)
		case *schedulerv2.AnnouncePeerRequest_DownloadPeerBackToSourceStartedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPeerBackToSourceStartedRequest: %#v", announcePeerRequest.DownloadPeerBackToSourceStartedRequest)
			v.handleDownloadPeerBackToSourceStartedRequest(announcePeerRequest.DownloadPeerBackToSourceStartedRequest)
		case *schedulerv2.AnnouncePeerRequest_DownloadPeerFinishedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPeerFinishedRequest: %#v", announcePeerRequest.DownloadPeerFinishedRequest)
			v.handleDownloadPeerFinishedRequest(announcePeerRequest.DownloadPeerFinishedRequest)
		case *schedulerv2.AnnouncePeerRequest_DownloadPeerBackToSourceFinishedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPeerBackToSourceFinishedRequest: %#v", announcePeerRequest.DownloadPeerBackToSourceFinishedRequest)
			v.handleDownloadPeerBackToSourceFinishedRequest(announcePeerRequest.DownloadPeerBackToSourceFinishedRequest)
		case *schedulerv2.AnnouncePeerRequest_DownloadPieceFinishedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPieceFinishedRequest: %#v", announcePeerRequest.DownloadPieceFinishedRequest)
			v.handleDownloadPieceFinishedRequest(announcePeerRequest.DownloadPieceFinishedRequest)
		case *schedulerv2.AnnouncePeerRequest_DownloadPieceBackToSourceFinishedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPieceBackToSourceFinishedRequest: %#v", announcePeerRequest.DownloadPieceBackToSourceFinishedRequest)
			v.handleDownloadPieceBackToSourceFinishedRequest(announcePeerRequest.DownloadPieceBackToSourceFinishedRequest)
		case *schedulerv2.AnnouncePeerRequest_DownloadPieceFailedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPieceFailedRequest: %#v", announcePeerRequest.DownloadPieceFailedRequest)
			v.handleDownloadPieceFailedRequest(announcePeerRequest.DownloadPieceFailedRequest)
		case *schedulerv2.AnnouncePeerRequest_DownloadPieceBackToSourceFailedRequest:
			logger.Infof("receive AnnouncePeerRequest_DownloadPieceBackToSourceFailedRequest: %#v", announcePeerRequest.DownloadPieceBackToSourceFailedRequest)
			v.handleDownloadPieceBackToSourceFailedRequest(announcePeerRequest.DownloadPieceBackToSourceFailedRequest)
		case *schedulerv2.AnnouncePeerRequest_SyncPiecesFailedRequest:
			logger.Infof("receive AnnouncePeerRequest_SyncPiecesFailedRequest: %#v", announcePeerRequest.SyncPiecesFailedRequest)
			v.handleSyncPiecesFailedRequest(announcePeerRequest.SyncPiecesFailedRequest)
		default:
			msg := fmt.Sprintf("receive unknow request: %#v", announcePeerRequest)
			logger.Error(msg)
			return status.Error(codes.FailedPrecondition, msg)
		}
	}
}

// TODO Implement function.
// handleRegisterPeerRequest handles RegisterPeerRequest of AnnouncePeerRequest.
func (v *V2) handleRegisterPeerRequest(hostID, taskID, peerID string, req *schedulerv2.RegisterPeerRequest) error {
	return nil
}

// TODO Implement function.
// handleDownloadPeerStartedRequest handles DownloadPeerStartedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPeerStartedRequest(req *schedulerv2.DownloadPeerStartedRequest) {
}

// TODO Implement function.
// handleDownloadPeerBackToSourceStartedRequest handles DownloadPeerBackToSourceStartedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPeerBackToSourceStartedRequest(req *schedulerv2.DownloadPeerBackToSourceStartedRequest) {
}

// TODO Implement function.
// handleDownloadPeerFinishedRequest handles DownloadPeerFinishedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPeerFinishedRequest(req *schedulerv2.DownloadPeerFinishedRequest) {
}

// TODO Implement function.
// handleDownloadPeerBackToSourceFinishedRequest handles DownloadPeerBackToSourceFinishedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPeerBackToSourceFinishedRequest(req *schedulerv2.DownloadPeerBackToSourceFinishedRequest) {
}

// TODO Implement function.
// handleDownloadPieceFinishedRequest handles DownloadPieceFinishedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPieceFinishedRequest(req *schedulerv2.DownloadPieceFinishedRequest) {
}

// TODO Implement function.
// handleDownloadPieceBackToSourceFinishedRequest handles DownloadPieceBackToSourceFinishedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPieceBackToSourceFinishedRequest(req *schedulerv2.DownloadPieceBackToSourceFinishedRequest) {
}

// TODO Implement function.
// handleDownloadPieceFailedRequest handles DownloadPieceFailedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPieceFailedRequest(req *schedulerv2.DownloadPieceFailedRequest) {
}

// TODO Implement function.
// handleDownloadPieceBackToSourceFailedRequest handles DownloadPieceBackToSourceFailedRequest of AnnouncePeerRequest.
func (v *V2) handleDownloadPieceBackToSourceFailedRequest(req *schedulerv2.DownloadPieceBackToSourceFailedRequest) {
}

// TODO Implement function.
// handleSyncPiecesFailedRequest handles SyncPiecesFailedRequest of AnnouncePeerRequest.
func (v *V2) handleSyncPiecesFailedRequest(req *schedulerv2.SyncPiecesFailedRequest) {
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

// TODO exchange peer api definition.
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
				SecurityDomain:           req.Host.Network.SecurityDomain,
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
			SecurityDomain:           req.Host.Network.SecurityDomain,
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
