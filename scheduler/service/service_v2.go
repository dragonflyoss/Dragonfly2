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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv2 "d7y.io/api/pkg/apis/common/v2"
	schedulerv2 "d7y.io/api/pkg/apis/scheduler/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduling"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

// TODO Implement v2 version of the service functions.
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
	return nil
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

	// Set digest to response.
	if peer.Task.Digest != nil {
		resp.Task.Digest = peer.Task.Digest.String()
	}

	// Set pieces to response.
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
		metrics.LeavePeerFailureCount.Inc()
		msg := fmt.Sprintf("peer %s not found", req.PeerId)
		logger.Error(msg)
		return status.Error(codes.NotFound, msg)
	}
	metrics.LeavePeerCount.Inc()

	if err := peer.FSM.Event(ctx, resource.PeerEventLeave); err != nil {
		metrics.LeavePeerFailureCount.Inc()
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

// Checks information of task.
func (v *V2) StatTask(ctx context.Context, req *schedulerv2.StatTaskRequest) (*commonv2.Task, error) {
	return nil, nil
}

// AnnounceHost announces host to scheduler.
func (v *V2) AnnounceHost(ctx context.Context, req *schedulerv2.AnnounceHostRequest) error {
	return nil
}

// LeaveHost releases host in scheduler.
func (v *V2) LeaveHost(ctx context.Context, req *schedulerv2.LeaveHostRequest) error {
	return nil
}
