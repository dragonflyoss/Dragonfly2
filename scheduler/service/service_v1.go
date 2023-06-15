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
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	commonv2 "d7y.io/api/pkg/apis/common/v2"
	errordetailsv1 "d7y.io/api/pkg/apis/errordetails/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/rpc/common"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/networktopology"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduling"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

// V1 is the interface for v1 version of the service.
type V1 struct {
	// Resource interface.
	resource resource.Resource

	// Scheduling interface.
	scheduling scheduling.Scheduling

	// Scheduelr service config.
	config *config.Config

	// Dynamic config.
	dynconfig config.DynconfigInterface

	// Storage interface.
	storage storage.Storage

	// Network topology interface.
	networkTopology networktopology.NetworkTopology
}

// New v1 version of service instance.
func NewV1(
	cfg *config.Config,
	resource resource.Resource,
	scheduling scheduling.Scheduling,
	dynconfig config.DynconfigInterface,
	storage storage.Storage,
	networktopology networktopology.NetworkTopology,
) *V1 {
	return &V1{
		resource:        resource,
		scheduling:      scheduling,
		config:          cfg,
		dynconfig:       dynconfig,
		storage:         storage,
		networkTopology: networktopology,
	}
}

// RegisterPeerTask registers peer and triggers seed peer download task.
func (v *V1) RegisterPeerTask(ctx context.Context, req *schedulerv1.PeerTaskRequest) (*schedulerv1.RegisterResult, error) {
	logger.WithPeer(req.PeerHost.Id, req.TaskId, req.PeerId).Infof("register peer task request: %#v", req)

	// Store resource.
	task := v.storeTask(ctx, req, commonv2.TaskType_DFDAEMON)
	host := v.storeHost(ctx, req.PeerHost)
	peer := v.storePeer(ctx, req.PeerId, req.UrlMeta.Priority, req.UrlMeta.Range, task, host)

	// Trigger the first download of the task.
	if err := v.triggerTask(ctx, req, task, host, peer, v.dynconfig); err != nil {
		peer.Log.Error(err)
		v.handleRegisterFailure(ctx, peer)
		return nil, dferrors.New(commonv1.Code_SchedForbidden, err.Error())
	}

	// If the task does not succeed, it is scheduled as a normal task.
	if !task.FSM.Is(resource.TaskStateSucceeded) {
		peer.Log.Infof("register as normal task, because of task state is %s",
			task.FSM.Current())

		result, err := v.registerNormalTask(ctx, peer)
		if err != nil {
			peer.Log.Error(err)
			v.handleRegisterFailure(ctx, peer)
			return nil, dferrors.New(commonv1.Code_SchedError, err.Error())
		}

		return result, nil
	}

	// If SizeScope is SizeScope_UNKNOW, then register as SizeScope_NORMAL.
	sizeScope := types.SizeScopeV2ToV1(task.SizeScope())
	peer.Log.Infof("task size scope is %s", sizeScope)

	// The task state is TaskStateSucceeded and SizeScope is not invalid.
	switch sizeScope {
	case commonv1.SizeScope_EMPTY:
		result, err := v.registerEmptyTask(ctx, peer)
		if err != nil {
			peer.Log.Error(err)
			v.handleRegisterFailure(ctx, peer)
			return nil, dferrors.New(commonv1.Code_SchedError, err.Error())
		}

		return result, nil
	case commonv1.SizeScope_TINY:
		// Validate data of direct piece.
		if !peer.Task.CanReuseDirectPiece() {
			peer.Log.Warnf("register as normal task, because of length of direct piece is %d, content length is %d",
				len(task.DirectPiece), task.ContentLength.Load())
			break
		}

		result, err := v.registerTinyTask(ctx, peer)
		if err != nil {
			peer.Log.Warnf("register as normal task, because of %s", err.Error())
			break
		}

		return result, nil
	case commonv1.SizeScope_SMALL:
		result, err := v.registerSmallTask(ctx, peer)
		if err != nil {
			peer.Log.Warnf("register as normal task, because of %s", err.Error())
			break
		}

		return result, nil
	}

	result, err := v.registerNormalTask(ctx, peer)
	if err != nil {
		peer.Log.Error(err)
		v.handleRegisterFailure(ctx, peer)
		return nil, dferrors.New(commonv1.Code_SchedError, err.Error())
	}

	peer.Log.Info("register as normal task, because of invalid size scope")
	return result, nil
}

// ReportPieceResult handles the piece information reported by dfdaemon.
func (v *V1) ReportPieceResult(stream schedulerv1.Scheduler_ReportPieceResultServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	var (
		peer        *resource.Peer
		initialized bool
		loaded      bool
	)
	for {
		select {
		case <-ctx.Done():
			logger.Infof("context was done")
			return ctx.Err()
		default:
		}

		piece, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			logger.Errorf("receive piece failed: %s", err.Error())
			return err
		}

		if !initialized {
			initialized = true

			// Get peer from peer manager.
			peer, loaded = v.resource.PeerManager().Load(piece.SrcPid)
			if !loaded {
				// If the scheduler cannot find the peer,
				// then the peer has not been registered in this scheduler.
				msg := fmt.Sprintf("peer %s not found", piece.SrcPid)
				logger.Error(msg)
				return dferrors.New(commonv1.Code_SchedReregister, msg)
			}

			// Peer setting stream.
			peer.StoreReportPieceResultStream(stream)
			defer peer.DeleteReportPieceResultStream()
		}

		if piece.PieceInfo != nil {
			// Handle begin of piece.
			if piece.PieceInfo.PieceNum == common.BeginOfPiece {
				peer.Log.Infof("receive begin of piece: %#v %#v", piece, piece.PieceInfo)
				v.handleBeginOfPiece(ctx, peer)
				continue
			}

			// Handle end of piece.
			if piece.PieceInfo.PieceNum == common.EndOfPiece {
				peer.Log.Infof("receive end of piece: %#v %#v", piece, piece.PieceInfo)
				v.handleEndOfPiece(ctx, peer)
				continue
			}
		}

		// Handle piece download successfully.
		if piece.Success {
			peer.Log.Infof("receive success piece: %#v %#v", piece, piece.PieceInfo)
			v.handlePieceSuccess(ctx, peer, piece)

			// Collect host traffic metrics.
			if v.config.Metrics.Enable && v.config.Metrics.EnableHost {
				metrics.HostTraffic.WithLabelValues(metrics.HostTrafficDownloadType, peer.Task.Type.String(), peer.Task.Tag, peer.Task.Application,
					peer.Host.Type.Name(), peer.Host.ID, peer.Host.IP, peer.Host.Hostname).Add(float64(piece.PieceInfo.RangeSize))
				if parent, loaded := v.resource.PeerManager().Load(piece.DstPid); loaded {
					metrics.HostTraffic.WithLabelValues(metrics.HostTrafficUploadType, peer.Task.Type.String(), peer.Task.Tag, peer.Task.Application,
						parent.Host.Type.Name(), parent.Host.ID, parent.Host.IP, parent.Host.Hostname).Add(float64(piece.PieceInfo.RangeSize))
				} else if !resource.IsPieceBackToSource(piece.DstPid) {
					peer.Log.Warnf("dst peer %s not found", piece.DstPid)
				}
			}

			// Collect traffic metrics.
			if !resource.IsPieceBackToSource(piece.DstPid) {
				metrics.Traffic.WithLabelValues(commonv2.TrafficType_REMOTE_PEER.String(), peer.Task.Type.String(),
					peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Add(float64(piece.PieceInfo.RangeSize))
			} else {
				metrics.Traffic.WithLabelValues(commonv2.TrafficType_BACK_TO_SOURCE.String(), peer.Task.Type.String(),
					peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Add(float64(piece.PieceInfo.RangeSize))
			}
			continue
		}

		// Handle piece download code.
		if piece.Code != commonv1.Code_Success {
			if piece.Code == commonv1.Code_ClientWaitPieceReady {
				peer.Log.Debug("receive wait piece")
				continue
			}

			// Handle piece download failed.
			peer.Log.Errorf("receive failed piece: %#v", piece)
			v.handlePieceFailure(ctx, peer, piece)
			continue
		}

		peer.Log.Warnf("receive unknow piece: %#v %#v", piece, piece.PieceInfo)
	}
}

// ReportPeerResult handles peer result reported by dfdaemon.
func (v *V1) ReportPeerResult(ctx context.Context, req *schedulerv1.PeerResult) error {
	logger.WithTaskAndPeerID(req.TaskId, req.PeerId).Infof("report peer result request: %#v", req)

	peer, loaded := v.resource.PeerManager().Load(req.PeerId)
	if !loaded {
		msg := fmt.Sprintf("peer %s not found", req.PeerId)
		logger.Error(msg)
		return dferrors.New(commonv1.Code_SchedPeerNotFound, msg)
	}

	// Collect DownloadPeerCount metrics.
	priority := peer.CalculatePriority(v.dynconfig)
	metrics.DownloadPeerCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()

	parents := peer.Parents()
	if !req.Success {
		peer.Log.Error("report failed peer")
		if peer.FSM.Is(resource.PeerStateBackToSource) {
			// Collect DownloadPeerBackToSourceFailureCount metrics.
			metrics.DownloadPeerBackToSourceFailureCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
				peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()

			go v.createDownloadRecord(peer, parents, req)
			v.handleTaskFailure(ctx, peer.Task, req.GetSourceError(), nil)
			v.handlePeerFailure(ctx, peer)
			return nil
		}

		// Collect DownloadPeerFailureCount metrics.
		metrics.DownloadPeerFailureCount.WithLabelValues(priority.String(), peer.Task.Type.String(),
			peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Inc()

		go v.createDownloadRecord(peer, parents, req)
		v.handlePeerFailure(ctx, peer)
		return nil
	}

	peer.Log.Info("report success peer")
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		go v.createDownloadRecord(peer, parents, req)
		v.handleTaskSuccess(ctx, peer.Task, req)
		v.handlePeerSuccess(ctx, peer)
		metrics.DownloadPeerDuration.WithLabelValues(priority.String(), peer.Task.Type.String(),
			peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Observe(float64(req.Cost))
		return nil
	}

	metrics.DownloadPeerDuration.WithLabelValues(priority.String(), peer.Task.Type.String(),
		peer.Task.Tag, peer.Task.Application, peer.Host.Type.Name()).Observe(float64(req.Cost))

	go v.createDownloadRecord(peer, parents, req)
	v.handlePeerSuccess(ctx, peer)
	return nil
}

// AnnounceTask informs scheduler a peer has completed task.
func (v *V1) AnnounceTask(ctx context.Context, req *schedulerv1.AnnounceTaskRequest) error {
	logger.WithPeer(req.PeerHost.Id, req.TaskId, req.PiecePacket.DstPid).Infof("announce task request: %#v %#v %#v %#v",
		req, req.UrlMeta, req.PeerHost, req.PiecePacket,
	)

	taskID := req.TaskId
	peerID := req.PiecePacket.DstPid
	options := []resource.TaskOption{}
	if d, err := digest.Parse(req.UrlMeta.Digest); err == nil {
		options = append(options, resource.WithDigest(d))
	}

	task := resource.NewTask(taskID, req.Url, req.UrlMeta.Tag, req.UrlMeta.Application, types.TaskTypeV1ToV2(req.TaskType),
		strings.Split(req.UrlMeta.Filter, idgen.URLFilterSeparator), req.UrlMeta.Header, int32(v.config.Scheduler.BackToSourceCount), options...)
	task, _ = v.resource.TaskManager().LoadOrStore(task)
	host := v.storeHost(ctx, req.PeerHost)
	peer := v.storePeer(ctx, peerID, req.UrlMeta.Priority, req.UrlMeta.Range, task, host)

	// If the task state is not TaskStateSucceeded,
	// advance the task state to TaskStateSucceeded.
	if !task.FSM.Is(resource.TaskStateSucceeded) {
		if task.FSM.Can(resource.TaskEventDownload) {
			if err := task.FSM.Event(ctx, resource.TaskEventDownload); err != nil {
				msg := fmt.Sprintf("task fsm event failed: %s", err.Error())
				peer.Log.Error(msg)
				return dferrors.New(commonv1.Code_SchedError, msg)
			}
		}

		// Construct piece.
		for _, pieceInfo := range req.PiecePacket.PieceInfos {
			piece := &resource.Piece{
				Number:      pieceInfo.PieceNum,
				ParentID:    req.PiecePacket.DstPid,
				Offset:      pieceInfo.RangeStart,
				Length:      uint64(pieceInfo.RangeSize),
				TrafficType: commonv2.TrafficType_LOCAL_PEER,
				Cost:        0,
				CreatedAt:   time.Now(),
			}

			if len(pieceInfo.PieceMd5) > 0 {
				piece.Digest = digest.New(digest.AlgorithmMD5, pieceInfo.PieceMd5)
			}

			peer.StorePiece(piece)
			peer.FinishedPieces.Set(uint(pieceInfo.PieceNum))
			peer.AppendPieceCost(piece.Cost)
			task.StorePiece(piece)
		}

		v.handleTaskSuccess(ctx, task, &schedulerv1.PeerResult{
			TotalPieceCount: req.PiecePacket.TotalPiece,
			ContentLength:   req.PiecePacket.ContentLength,
		})
	}

	// If the peer state is not PeerStateSucceeded,
	// advance the peer state to PeerStateSucceeded.
	if !peer.FSM.Is(resource.PeerStateSucceeded) {
		if peer.FSM.Is(resource.PeerStatePending) {
			if err := peer.FSM.Event(ctx, resource.PeerEventRegisterNormal); err != nil {
				msg := fmt.Sprintf("peer fsm event failed: %s", err.Error())
				peer.Log.Error(msg)
				return dferrors.New(commonv1.Code_SchedError, msg)
			}
		}

		if peer.FSM.Is(resource.PeerStateReceivedTiny) ||
			peer.FSM.Is(resource.PeerStateReceivedSmall) ||
			peer.FSM.Is(resource.PeerStateReceivedNormal) {
			if err := peer.FSM.Event(ctx, resource.PeerEventDownload); err != nil {
				msg := fmt.Sprintf("peer fsm event failed: %s", err.Error())
				peer.Log.Error(msg)
				return dferrors.New(commonv1.Code_SchedError, msg)
			}

			v.handlePeerSuccess(ctx, peer)
		}
	}

	return nil
}

// StatTask checks the current state of the task.
func (v *V1) StatTask(ctx context.Context, req *schedulerv1.StatTaskRequest) (*schedulerv1.Task, error) {
	logger.WithTaskID(req.TaskId).Infof("stat task request: %#v", req)

	task, loaded := v.resource.TaskManager().Load(req.TaskId)
	if !loaded {
		msg := fmt.Sprintf("task %s not found", req.TaskId)
		logger.Info(msg)
		return nil, dferrors.New(commonv1.Code_PeerTaskNotFound, msg)
	}

	return &schedulerv1.Task{
		Id:               task.ID,
		Type:             types.TaskTypeV2ToV1(task.Type),
		ContentLength:    task.ContentLength.Load(),
		TotalPieceCount:  task.TotalPieceCount.Load(),
		State:            task.FSM.Current(),
		PeerCount:        int32(task.PeerCount()),
		HasAvailablePeer: task.HasAvailablePeer(set.NewSafeSet[string]()),
	}, nil
}

// LeaveTask releases peer in scheduler.
func (v *V1) LeaveTask(ctx context.Context, req *schedulerv1.PeerTarget) error {
	logger.WithTaskAndPeerID(req.TaskId, req.PeerId).Infof("leave task request: %#v", req)

	peer, loaded := v.resource.PeerManager().Load(req.PeerId)
	if !loaded {
		msg := fmt.Sprintf("peer %s not found", req.PeerId)
		logger.Error(msg)
		return dferrors.New(commonv1.Code_SchedPeerNotFound, msg)
	}

	if err := peer.FSM.Event(ctx, resource.PeerEventLeave); err != nil {
		msg := fmt.Sprintf("peer fsm event failed: %s", err.Error())
		peer.Log.Error(msg)
		return dferrors.New(commonv1.Code_SchedTaskStatusError, msg)
	}

	return nil
}

// AnnounceHost announces host to scheduler.
func (v *V1) AnnounceHost(ctx context.Context, req *schedulerv1.AnnounceHostRequest) error {
	// Get scheduler cluster client config by manager.
	var concurrentUploadLimit int32
	if clientConfig, err := v.dynconfig.GetSchedulerClusterClientConfig(); err == nil {
		concurrentUploadLimit = int32(clientConfig.LoadLimit)
	}

	host, loaded := v.resource.HostManager().Load(req.Id)
	if !loaded {
		options := []resource.HostOption{
			resource.WithOS(req.Os),
			resource.WithPlatform(req.Platform),
			resource.WithPlatformFamily(req.PlatformFamily),
			resource.WithPlatformVersion(req.PlatformVersion),
			resource.WithKernelVersion(req.KernelVersion),
		}

		if concurrentUploadLimit > 0 {
			options = append(options, resource.WithConcurrentUploadLimit(concurrentUploadLimit))
		}

		if req.Cpu != nil {
			options = append(options, resource.WithCPU(resource.CPU{
				LogicalCount:   req.Cpu.LogicalCount,
				PhysicalCount:  req.Cpu.PhysicalCount,
				Percent:        req.Cpu.Percent,
				ProcessPercent: req.Cpu.ProcessPercent,
				Times: resource.CPUTimes{
					User:      req.Cpu.Times.User,
					System:    req.Cpu.Times.System,
					Idle:      req.Cpu.Times.Idle,
					Nice:      req.Cpu.Times.Nice,
					Iowait:    req.Cpu.Times.Iowait,
					Irq:       req.Cpu.Times.Irq,
					Softirq:   req.Cpu.Times.Softirq,
					Steal:     req.Cpu.Times.Steal,
					Guest:     req.Cpu.Times.Guest,
					GuestNice: req.Cpu.Times.GuestNice,
				},
			}))
		}

		if req.Memory != nil {
			options = append(options, resource.WithMemory(resource.Memory{
				Total:              req.Memory.Total,
				Available:          req.Memory.Available,
				Used:               req.Memory.Used,
				UsedPercent:        req.Memory.UsedPercent,
				ProcessUsedPercent: req.Memory.ProcessUsedPercent,
				Free:               req.Memory.Free,
			}))
		}

		if req.Network != nil {
			options = append(options, resource.WithNetwork(resource.Network{
				TCPConnectionCount:       req.Network.TcpConnectionCount,
				UploadTCPConnectionCount: req.Network.UploadTcpConnectionCount,
				Location:                 req.Network.Location,
				IDC:                      req.Network.Idc,
			}))
		}

		if req.Disk != nil {
			options = append(options, resource.WithDisk(resource.Disk{
				Total:             req.Disk.Total,
				Free:              req.Disk.Free,
				Used:              req.Disk.Used,
				UsedPercent:       req.Disk.UsedPercent,
				InodesTotal:       req.Disk.InodesTotal,
				InodesUsed:        req.Disk.InodesUsed,
				InodesFree:        req.Disk.InodesFree,
				InodesUsedPercent: req.Disk.InodesUsedPercent,
			}))
		}

		if req.Build != nil {
			options = append(options, resource.WithBuild(resource.Build{
				GitVersion: req.Build.GitVersion,
				GitCommit:  req.Build.GitCommit,
				GoVersion:  req.Build.GoVersion,
				Platform:   req.Build.Platform,
			}))
		}

		host = resource.NewHost(
			req.Id, req.Ip, req.Hostname,
			req.Port, req.DownloadPort, types.ParseHostType(req.Type),
			options...,
		)
		v.resource.HostManager().Store(host)
		host.Log.Infof("announce new host: %#v", req)
		return nil
	}

	// Host already exists and updates properties.
	host.Port = req.Port
	host.DownloadPort = req.DownloadPort
	host.Type = types.ParseHostType(req.Type)
	host.OS = req.Os
	host.Platform = req.Platform
	host.PlatformFamily = req.PlatformFamily
	host.PlatformVersion = req.PlatformVersion
	host.KernelVersion = req.KernelVersion
	host.UpdatedAt.Store(time.Now())

	if concurrentUploadLimit > 0 {
		host.ConcurrentUploadLimit.Store(concurrentUploadLimit)
	}

	if req.Cpu != nil {
		host.CPU = resource.CPU{
			LogicalCount:   req.Cpu.LogicalCount,
			PhysicalCount:  req.Cpu.PhysicalCount,
			Percent:        req.Cpu.Percent,
			ProcessPercent: req.Cpu.ProcessPercent,
			Times: resource.CPUTimes{
				User:      req.Cpu.Times.User,
				System:    req.Cpu.Times.System,
				Idle:      req.Cpu.Times.Idle,
				Nice:      req.Cpu.Times.Nice,
				Iowait:    req.Cpu.Times.Iowait,
				Irq:       req.Cpu.Times.Irq,
				Softirq:   req.Cpu.Times.Softirq,
				Steal:     req.Cpu.Times.Steal,
				Guest:     req.Cpu.Times.Guest,
				GuestNice: req.Cpu.Times.GuestNice,
			},
		}
	}

	if req.Memory != nil {
		host.Memory = resource.Memory{
			Total:              req.Memory.Total,
			Available:          req.Memory.Available,
			Used:               req.Memory.Used,
			UsedPercent:        req.Memory.UsedPercent,
			ProcessUsedPercent: req.Memory.ProcessUsedPercent,
			Free:               req.Memory.Free,
		}
	}

	if req.Network != nil {
		host.Network = resource.Network{
			TCPConnectionCount:       req.Network.TcpConnectionCount,
			UploadTCPConnectionCount: req.Network.UploadTcpConnectionCount,
			Location:                 req.Network.Location,
			IDC:                      req.Network.Idc,
		}
	}

	if req.Disk != nil {
		host.Disk = resource.Disk{
			Total:             req.Disk.Total,
			Free:              req.Disk.Free,
			Used:              req.Disk.Used,
			UsedPercent:       req.Disk.UsedPercent,
			InodesTotal:       req.Disk.InodesTotal,
			InodesUsed:        req.Disk.InodesUsed,
			InodesFree:        req.Disk.InodesFree,
			InodesUsedPercent: req.Disk.InodesUsedPercent,
		}
	}

	if req.Build != nil {
		host.Build = resource.Build{
			GitVersion: req.Build.GitVersion,
			GitCommit:  req.Build.GitCommit,
			GoVersion:  req.Build.GoVersion,
			Platform:   req.Build.Platform,
		}
	}

	return nil
}

// LeaveHost releases host in scheduler.
func (v *V1) LeaveHost(ctx context.Context, req *schedulerv1.LeaveHostRequest) error {
	logger.WithHostID(req.Id).Infof("leave host request: %#v", req)

	host, loaded := v.resource.HostManager().Load(req.Id)
	if !loaded {
		msg := fmt.Sprintf("host %s not found", req.Id)
		logger.Error(msg)
		return dferrors.New(commonv1.Code_BadRequest, msg)
	}

	host.LeavePeers()
	return nil
}

// SyncProbes sync probes of the host.
func (v *V1) SyncProbes(stream schedulerv1.Scheduler_SyncProbesServer) error {
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
		case *schedulerv1.SyncProbesRequest_ProbeStartedRequest:
			// Find probed hosts in network topology. Based on the source host information,
			// the most candidate hosts will be evaluated.
			logger.Info("receive SyncProbesRequest_ProbeStartedRequest")
			probedHostIDs, err := v.networkTopology.FindProbedHostIDs(req.Host.Id)
			if err != nil {
				logger.Error(err)
				return status.Error(codes.FailedPrecondition, err.Error())
			}

			var probedHosts []*commonv1.Host
			for _, probedHostID := range probedHostIDs {
				probedHost, loaded := v.resource.HostManager().Load(probedHostID)
				if !loaded {
					logger.Warnf("probed host %s not found", probedHostID)
					continue
				}

				probedHosts = append(probedHosts, &commonv1.Host{
					Id:           probedHost.ID,
					Ip:           probedHost.IP,
					Hostname:     probedHost.Hostname,
					Port:         probedHost.Port,
					DownloadPort: probedHost.DownloadPort,
					Location:     probedHost.Network.Location,
					Idc:          probedHost.Network.IDC,
				})
			}

			if len(probedHosts) == 0 {
				logger.Error("probed host not found")
				return status.Error(codes.NotFound, "probed host not found")
			}

			logger.Infof("probe started: %#v", probedHosts)
			if err := stream.Send(&schedulerv1.SyncProbesResponse{
				Hosts: probedHosts,
			}); err != nil {
				logger.Error(err)
				return err
			}
		case *schedulerv1.SyncProbesRequest_ProbeFinishedRequest:
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
		case *schedulerv1.SyncProbesRequest_ProbeFailedRequest:
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

// triggerTask triggers the first download of the task.
func (v *V1) triggerTask(ctx context.Context, req *schedulerv1.PeerTaskRequest, task *resource.Task, host *resource.Host, peer *resource.Peer, dynconfig config.DynconfigInterface) error {
	// If task has available peer, peer does not need to be triggered.
	blocklist := set.NewSafeSet[string]()
	blocklist.Add(peer.ID)
	if (task.FSM.Is(resource.TaskStateRunning) ||
		task.FSM.Is(resource.TaskStateSucceeded)) &&
		task.HasAvailablePeer(blocklist) {
		peer.Log.Info("peer does not need to trigger")
		return nil
	}

	// If the task triggers the TaskEventDownload failed and it has no available peer,
	// let the peer do the scheduling.
	if task.FSM.Can(resource.TaskEventDownload) {
		if err := task.FSM.Event(ctx, resource.TaskEventDownload); err != nil {
			peer.Log.Errorf("task fsm event failed: %s", err.Error())
			return err
		}
	}

	// If host type is not HostTypeNormal, then it needs to back-to-source.
	if host.Type != types.HostTypeNormal {
		peer.Log.Infof("peer back-to-source, because of host type is %d", host.Type)
		peer.NeedBackToSource.Store(true)
		return nil
	}

	// The first download is triggered according to
	// the different priorities of the peer and
	// priority of the RegisterPeerTask parameter is
	// higher than parameter of the application.
	var priority commonv1.Priority
	if req.UrlMeta.Priority != commonv1.Priority_LEVEL0 {
		priority = req.UrlMeta.Priority
	} else {
		// Compatible with v1 version of priority enum.
		priority = types.PriorityV2ToV1(peer.CalculatePriority(dynconfig))
	}
	peer.Log.Infof("peer priority is %d", priority)

	switch priority {
	case commonv1.Priority_LEVEL6, commonv1.Priority_LEVEL0:
		if v.config.SeedPeer.Enable && !task.IsSeedPeerFailed() {
			if len(req.UrlMeta.Range) > 0 {
				if rg, err := http.ParseURLMetaRange(req.UrlMeta.Range, math.MaxInt64); err == nil {
					go v.triggerSeedPeerTask(ctx, &rg, task)
					return nil
				}

				peer.Log.Errorf("range %s is invalid", req.UrlMeta.Range)
			} else {
				go v.triggerSeedPeerTask(ctx, nil, task)
				return nil
			}
		}
		fallthrough
	case commonv1.Priority_LEVEL5:
		fallthrough
	case commonv1.Priority_LEVEL4:
		fallthrough
	case commonv1.Priority_LEVEL3:
		peer.Log.Infof("peer back-to-source, because of hitting priority %d", commonv1.Priority_LEVEL3)
		peer.NeedBackToSource.Store(true)
		return nil
	case commonv1.Priority_LEVEL2:
		return fmt.Errorf("priority is %d and no available peers", commonv1.Priority_LEVEL2)
	case commonv1.Priority_LEVEL1:
		return fmt.Errorf("priority is %d", commonv1.Priority_LEVEL1)
	}

	peer.Log.Infof("peer back-to-source, because of peer has invalid priority %d", priority)
	peer.NeedBackToSource.Store(true)
	return nil
}

// triggerSeedPeerTask starts to trigger seed peer task.
func (v *V1) triggerSeedPeerTask(ctx context.Context, rg *http.Range, task *resource.Task) {
	ctx, cancel := context.WithCancel(trace.ContextWithSpan(context.Background(), trace.SpanFromContext(ctx)))
	defer cancel()

	task.Log.Info("trigger seed peer")
	seedPeer, endOfPiece, err := v.resource.SeedPeer().TriggerTask(ctx, rg, task)
	if err != nil {
		task.Log.Errorf("trigger seed peer failed: %s", err.Error())
		v.handleTaskFailure(ctx, task, nil, err)
		return
	}

	// Update the task status first to help peer scheduling evaluation and scoring.
	seedPeer.Log.Info("trigger seed peer successfully")
	v.handleTaskSuccess(ctx, task, endOfPiece)
	v.handlePeerSuccess(ctx, seedPeer)
}

// storeTask stores a new task or reuses a previous task.
func (v *V1) storeTask(ctx context.Context, req *schedulerv1.PeerTaskRequest, typ commonv2.TaskType) *resource.Task {
	filters := strings.Split(req.UrlMeta.Filter, idgen.URLFilterSeparator)

	task, loaded := v.resource.TaskManager().Load(req.TaskId)
	if !loaded {
		options := []resource.TaskOption{}
		if d, err := digest.Parse(req.UrlMeta.Digest); err == nil {
			options = append(options, resource.WithDigest(d))
		}

		task := resource.NewTask(req.TaskId, req.Url, req.UrlMeta.Tag, req.UrlMeta.Application,
			typ, filters, req.UrlMeta.Header, int32(v.config.Scheduler.BackToSourceCount), options...)
		v.resource.TaskManager().Store(task)
		task.Log.Info("create new task")
		return task
	}

	// Task is the pointer, if the task already exists, the next request will
	// update the task's Url and UrlMeta in task manager.
	task.URL = req.Url
	task.Filters = filters
	task.Header = req.UrlMeta.Header
	task.Log.Info("task already exists")
	return task
}

// storeHost stores a new host or reuses a previous host.
func (v *V1) storeHost(ctx context.Context, peerHost *schedulerv1.PeerHost) *resource.Host {
	host, loaded := v.resource.HostManager().Load(peerHost.Id)
	if !loaded {
		options := []resource.HostOption{resource.WithNetwork(resource.Network{
			Location: peerHost.Location,
			IDC:      peerHost.Idc,
		})}
		if clientConfig, err := v.dynconfig.GetSchedulerClusterClientConfig(); err == nil && clientConfig.LoadLimit > 0 {
			options = append(options, resource.WithConcurrentUploadLimit(int32(clientConfig.LoadLimit)))
		}

		host := resource.NewHost(
			peerHost.Id, peerHost.Ip, peerHost.Hostname,
			peerHost.RpcPort, peerHost.DownPort, types.HostTypeNormal,
			options...,
		)

		v.resource.HostManager().Store(host)
		host.Log.Info("create new host")
		return host
	}

	host.Port = peerHost.RpcPort
	host.DownloadPort = peerHost.DownPort
	host.Network.Location = peerHost.Location
	host.Network.IDC = peerHost.Idc
	host.UpdatedAt.Store(time.Now())
	host.Log.Info("host already exists")
	return host
}

// storePeer stores a new peer or reuses a previous peer.
func (v *V1) storePeer(ctx context.Context, id string, priority commonv1.Priority, rg string, task *resource.Task, host *resource.Host) *resource.Peer {
	peer, loaded := v.resource.PeerManager().Load(id)
	if !loaded {
		options := []resource.PeerOption{}
		if priority != commonv1.Priority_LEVEL0 {
			options = append(options, resource.WithPriority(types.PriorityV1ToV2(priority)))
		}

		if len(rg) > 0 {
			if r, err := http.ParseURLMetaRange(rg, math.MaxInt64); err == nil {
				options = append(options, resource.WithRange(r))
			} else {
				logger.WithPeer(host.ID, task.ID, id).Error(err)
			}
		}

		peer := resource.NewPeer(id, task, host, options...)
		v.resource.PeerManager().Store(peer)
		peer.Log.Info("create new peer")
		return peer
	}

	peer.Log.Info("peer already exists")
	return peer
}

// registerEmptyTask registers the empty task.
func (v *V1) registerEmptyTask(ctx context.Context, peer *resource.Peer) (*schedulerv1.RegisterResult, error) {
	if err := peer.FSM.Event(ctx, resource.PeerEventRegisterEmpty); err != nil {
		return nil, err
	}

	return &schedulerv1.RegisterResult{
		TaskId:    peer.Task.ID,
		TaskType:  types.TaskTypeV2ToV1(peer.Task.Type),
		SizeScope: commonv1.SizeScope_EMPTY,
		DirectPiece: &schedulerv1.RegisterResult_PieceContent{
			PieceContent: []byte{},
		},
	}, nil
}

// registerEmptyTask registers the tiny task.
func (v *V1) registerTinyTask(ctx context.Context, peer *resource.Peer) (*schedulerv1.RegisterResult, error) {
	if err := peer.FSM.Event(ctx, resource.PeerEventRegisterTiny); err != nil {
		return nil, err
	}

	return &schedulerv1.RegisterResult{
		TaskId:    peer.Task.ID,
		TaskType:  types.TaskTypeV2ToV1(peer.Task.Type),
		SizeScope: commonv1.SizeScope_TINY,
		DirectPiece: &schedulerv1.RegisterResult_PieceContent{
			PieceContent: peer.Task.DirectPiece,
		},
	}, nil
}

// registerSmallTask registers the small task.
func (v *V1) registerSmallTask(ctx context.Context, peer *resource.Peer) (*schedulerv1.RegisterResult, error) {
	candidateParents, found := v.scheduling.FindCandidateParents(ctx, peer, set.NewSafeSet[string]())
	if !found {
		return nil, errors.New("candidate parent not found")
	}
	candidateParent := candidateParents[0]

	// When task size scope is small, parent must be downloaded successfully
	// before returning to the parent directly.
	if !candidateParent.FSM.Is(resource.PeerStateSucceeded) {
		return nil, fmt.Errorf("candidate parent state is %s", candidateParent.FSM.Current())
	}

	piece, loaded := peer.Task.LoadPiece(0)
	if !loaded {
		return nil, fmt.Errorf("first piece not found")
	}

	// Delete inedges of peer.
	if err := peer.Task.DeletePeerInEdges(peer.ID); err != nil {
		return nil, err
	}

	// Add edges between parent and peer.
	if err := peer.Task.AddPeerEdge(candidateParent, peer); err != nil {
		return nil, err
	}

	if err := peer.FSM.Event(ctx, resource.PeerEventRegisterSmall); err != nil {
		return nil, err
	}

	// Construct piece information.
	pieceInfo := &commonv1.PieceInfo{
		PieceNum:    piece.Number,
		RangeStart:  piece.Offset,
		RangeSize:   uint32(piece.Length),
		PieceOffset: piece.Offset,
		// FIXME Use value of piece.PieceStyle.
		PieceStyle:   commonv1.PieceStyle_PLAIN,
		DownloadCost: uint64(piece.Cost.Milliseconds()),
	}

	if piece.Digest != nil {
		pieceInfo.PieceMd5 = piece.Digest.Encoded
	}

	return &schedulerv1.RegisterResult{
		TaskId:    peer.Task.ID,
		TaskType:  types.TaskTypeV2ToV1(peer.Task.Type),
		SizeScope: commonv1.SizeScope_SMALL,
		DirectPiece: &schedulerv1.RegisterResult_SinglePiece{
			SinglePiece: &schedulerv1.SinglePiece{
				DstPid:    candidateParent.ID,
				DstAddr:   fmt.Sprintf("%s:%d", candidateParent.Host.IP, candidateParent.Host.DownloadPort),
				PieceInfo: pieceInfo,
			},
		},
	}, nil
}

// registerNormalTask registers the tiny task.
func (v *V1) registerNormalTask(ctx context.Context, peer *resource.Peer) (*schedulerv1.RegisterResult, error) {
	if err := peer.FSM.Event(ctx, resource.PeerEventRegisterNormal); err != nil {
		return nil, err
	}

	return &schedulerv1.RegisterResult{
		TaskId:    peer.Task.ID,
		TaskType:  types.TaskTypeV2ToV1(peer.Task.Type),
		SizeScope: commonv1.SizeScope_NORMAL,
	}, nil
}

// handleRegisterFailure handles failure of register.
func (v *V1) handleRegisterFailure(ctx context.Context, peer *resource.Peer) {
	if err := peer.FSM.Event(ctx, resource.PeerEventLeave); err != nil {
		peer.Log.Error(err)
	}

	v.resource.PeerManager().Delete(peer.ID)
	return
}

// handleBeginOfPiece handles begin of piece.
func (v *V1) handleBeginOfPiece(ctx context.Context, peer *resource.Peer) {
	state := peer.FSM.Current()
	peer.Log.Infof("peer state is %s", state)

	switch state {
	case resource.PeerStateBackToSource:
		// Back to the source download process, peer directly returns.
		return
	case resource.PeerStateReceivedTiny:
		// When the task is tiny,
		// the peer has already returned to piece data when registering.
		if err := peer.FSM.Event(ctx, resource.PeerEventDownload); err != nil {
			peer.Log.Errorf("peer fsm event failed: %s", err.Error())
			return
		}
	case resource.PeerStateReceivedSmall:
		// When the task is small,
		// the peer has already returned to the parent when registering.
		if err := peer.FSM.Event(ctx, resource.PeerEventDownload); err != nil {
			peer.Log.Errorf("peer fsm event failed: %s", err.Error())
			return
		}
	case resource.PeerStateReceivedNormal:
		if err := peer.FSM.Event(ctx, resource.PeerEventDownload); err != nil {
			peer.Log.Errorf("peer fsm event failed: %s", err.Error())
			return
		}

		v.scheduling.ScheduleParentAndCandidateParents(ctx, peer, set.NewSafeSet[string]())
	default:
	}
}

// handleEndOfPiece handles end of piece.
func (v *V1) handleEndOfPiece(ctx context.Context, peer *resource.Peer) {}

// handlePieceSuccess handles successful piece.
func (v *V1) handlePieceSuccess(ctx context.Context, peer *resource.Peer, pieceResult *schedulerv1.PieceResult) {
	// Distinguish traffic type.
	trafficType := commonv2.TrafficType_REMOTE_PEER
	if resource.IsPieceBackToSource(pieceResult.DstPid) {
		trafficType = commonv2.TrafficType_BACK_TO_SOURCE
	}

	// Construct piece.
	cost := time.Duration(int64(pieceResult.PieceInfo.DownloadCost) * int64(time.Millisecond))
	piece := &resource.Piece{
		Number:      pieceResult.PieceInfo.PieceNum,
		ParentID:    pieceResult.DstPid,
		Offset:      pieceResult.PieceInfo.RangeStart,
		Length:      uint64(pieceResult.PieceInfo.RangeSize),
		TrafficType: trafficType,
		Cost:        cost,
		CreatedAt:   time.Now().Add(-cost),
	}

	if len(pieceResult.PieceInfo.PieceMd5) > 0 {
		piece.Digest = digest.New(digest.AlgorithmMD5, pieceResult.PieceInfo.PieceMd5)
	}

	peer.StorePiece(piece)
	peer.FinishedPieces.Set(uint(piece.Number))
	peer.AppendPieceCost(piece.Cost)

	// When the piece is downloaded successfully,
	// peer's UpdatedAt needs to be updated
	// to prevent the peer from being GC during the download process.
	peer.UpdatedAt.Store(time.Now())
	peer.PieceUpdatedAt.Store(time.Now())

	// When the piece is downloaded successfully,
	// dst peer's UpdatedAt needs to be updated
	// to prevent the dst peer from being GC during the download process.
	if !resource.IsPieceBackToSource(pieceResult.DstPid) {
		if destPeer, loaded := v.resource.PeerManager().Load(pieceResult.DstPid); loaded {
			destPeer.UpdatedAt.Store(time.Now())
			destPeer.Host.UpdatedAt.Store(time.Now())
		}
	}

	// When the peer downloads back-to-source,
	// piece downloads successfully updates the task piece info.
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		peer.Task.StorePiece(piece)
	}
}

// handlePieceFailure handles failed piece.
func (v *V1) handlePieceFailure(ctx context.Context, peer *resource.Peer, piece *schedulerv1.PieceResult) {
	// Failed to download piece back-to-source.
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		return
	}

	// If parent can not found, reschedule parent.
	parent, loaded := v.resource.PeerManager().Load(piece.DstPid)
	if !loaded {
		peer.Log.Errorf("parent %s not found", piece.DstPid)
		peer.BlockParents.Add(piece.DstPid)
		v.scheduling.ScheduleParentAndCandidateParents(ctx, peer, peer.BlockParents)
		return
	}

	// host upload failed and UploadErrorCount needs to be increased.
	parent.Host.UploadFailedCount.Inc()

	// It’s not a case of back-to-source downloading failed,
	// to help peer to reschedule the parent node.
	code := piece.Code
	peer.Log.Infof("piece error code is %s", code)

	switch code {
	case commonv1.Code_PeerTaskNotFound:
		if err := parent.FSM.Event(ctx, resource.PeerEventDownloadFailed); err != nil {
			peer.Log.Errorf("peer fsm event failed: %s", err.Error())
			break
		}
	case commonv1.Code_ClientPieceNotFound:
		// Dfdaemon downloading piece data from parent returns http error code 404.
		// If the parent is not a seed peer, reschedule parent for peer.
		// If the parent is a seed peer, scheduler need to trigger seed peer to download again.
		if parent.Host.Type == types.HostTypeNormal {
			peer.Log.Infof("parent %s host type is normal", piece.DstPid)
			break
		}

		peer.Log.Infof("parent %s is seed peer", piece.DstPid)
		v.handleLegacySeedPeer(ctx, parent)

		// Start trigger seed peer task.
		if v.config.SeedPeer.Enable {
			go v.triggerSeedPeerTask(ctx, peer.Range, parent.Task)
		}
	default:
	}

	// Peer state is PeerStateRunning will be rescheduled.
	if !peer.FSM.Is(resource.PeerStateRunning) {
		peer.Log.Infof("peer state is %s and can not be rescheduled", peer.FSM.Current())

		// Returns an scheduling error if the peer
		// state is not PeerStateRunning.
		stream, loaded := peer.LoadReportPieceResultStream()
		if !loaded {
			peer.Log.Error("load stream failed")
			return
		}

		if err := stream.Send(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedError}); err != nil {
			peer.Log.Error(err)
			return
		}

		return
	}

	peer.Log.Infof("reschedule parent because of failed piece")
	peer.BlockParents.Add(parent.ID)
	v.scheduling.ScheduleParentAndCandidateParents(ctx, peer, peer.BlockParents)
}

// handlePeerSuccess handles successful peer.
func (v *V1) handlePeerSuccess(ctx context.Context, peer *resource.Peer) {
	if err := peer.FSM.Event(ctx, resource.PeerEventDownloadSucceeded); err != nil {
		peer.Log.Errorf("peer fsm event failed: %s", err.Error())
		return
	}

	// Update peer cost of downloading.
	peer.Cost.Store(time.Since(peer.CreatedAt.Load()))

	// If the peer type is tiny and back-to-source,
	// it needs to directly download the tiny file and store the data in task DirectPiece.
	if types.SizeScopeV2ToV1(peer.Task.SizeScope()) == commonv1.SizeScope_TINY && len(peer.Task.DirectPiece) == 0 {
		data, err := peer.DownloadTinyFile()
		if err != nil {
			peer.Log.Errorf("download tiny task failed: %s", err.Error())
			return
		}

		if len(data) != int(peer.Task.ContentLength.Load()) {
			peer.Log.Errorf("download tiny task length of data is %d, task content length is %d", len(data), peer.Task.ContentLength.Load())
			return
		}

		// Tiny file downloaded successfully.
		peer.Task.DirectPiece = data
	}
}

// handlePeerFailure handles failed peer.
func (v *V1) handlePeerFailure(ctx context.Context, peer *resource.Peer) {
	if err := peer.FSM.Event(ctx, resource.PeerEventDownloadFailed); err != nil {
		peer.Log.Errorf("peer fsm event failed: %s", err.Error())
		return
	}

	// Reschedule a new parent to children of peer to exclude the current failed peer.
	for _, child := range peer.Children() {
		child.Log.Infof("reschedule parent because of parent peer %s is failed", peer.ID)
		v.scheduling.ScheduleParentAndCandidateParents(ctx, child, child.BlockParents)
	}
}

// handleLegacySeedPeer handles seed server's task has left,
// but did not notify the scheduler to leave the task.
func (v *V1) handleLegacySeedPeer(ctx context.Context, peer *resource.Peer) {
	if err := peer.FSM.Event(ctx, resource.PeerEventLeave); err != nil {
		peer.Log.Errorf("peer fsm event failed: %s", err.Error())
		return
	}

	// Reschedule a new parent to children of peer to exclude the current failed peer.
	for _, child := range peer.Children() {
		child.Log.Infof("reschedule parent because of parent peer %s is failed", peer.ID)
		v.scheduling.ScheduleParentAndCandidateParents(ctx, child, child.BlockParents)
	}
}

// Conditions for the task to switch to the TaskStateSucceeded are:
// 1. Seed peer downloads the resource successfully.
// 2. Dfdaemon back-to-source to download successfully.
// 3. Peer announces it has the task.
func (v *V1) handleTaskSuccess(ctx context.Context, task *resource.Task, req *schedulerv1.PeerResult) {
	if task.FSM.Is(resource.TaskStateSucceeded) {
		return
	}

	// Update task total piece count and content length.
	task.TotalPieceCount.Store(req.TotalPieceCount)
	task.ContentLength.Store(req.ContentLength)

	if err := task.FSM.Event(ctx, resource.TaskEventDownloadSucceeded); err != nil {
		task.Log.Errorf("task fsm event failed: %s", err.Error())
		return
	}
}

// Conditions for the task to switch to the TaskStateSucceeded are:
// 1. Seed peer downloads the resource failed.
// 2. Dfdaemon back-to-source to download failed.
func (v *V1) handleTaskFailure(ctx context.Context, task *resource.Task, backToSourceErr *errordetailsv1.SourceError, seedPeerErr error) {
	// If peer back-to-source fails due to an unrecoverable error,
	// notify other peers of the failure,
	// and return the source metadata to peer.
	if backToSourceErr != nil {
		if !backToSourceErr.Temporary {
			task.ReportPieceResultToPeers(&schedulerv1.PeerPacket{
				Code: commonv1.Code_BackToSourceAborted,
				Errordetails: &schedulerv1.PeerPacket_SourceError{
					SourceError: backToSourceErr,
				},
			}, resource.PeerEventDownloadFailed)
			task.PeerFailedCount.Store(0)
		}
	} else if seedPeerErr != nil {
		// If seed peer back-to-source fails due to an unrecoverable error,
		// notify other peers of the failure,
		// and return the source metadata to peer.
		if st, ok := status.FromError(seedPeerErr); ok {
			for _, detail := range st.Details() {
				switch d := detail.(type) {
				case *errordetailsv1.SourceError:
					task.Log.Infof("download back-to-source error: %#v", d)
					if !d.Temporary {
						task.ReportPieceResultToPeers(&schedulerv1.PeerPacket{
							Code: commonv1.Code_BackToSourceAborted,
							Errordetails: &schedulerv1.PeerPacket_SourceError{
								SourceError: d,
							},
						}, resource.PeerEventDownloadFailed)
						task.PeerFailedCount.Store(0)
					}
				}
			}
		}
	} else if task.PeerFailedCount.Load() > resource.FailedPeerCountLimit {
		// If the number of failed peers in the task is greater than FailedPeerCountLimit,
		// then scheduler notifies running peers of failure.
		task.ReportPieceResultToPeers(&schedulerv1.PeerPacket{
			Code: commonv1.Code_SchedTaskStatusError,
		}, resource.PeerEventDownloadFailed)
		task.PeerFailedCount.Store(0)
	}

	if task.FSM.Is(resource.TaskStateFailed) {
		return
	}

	if err := task.FSM.Event(ctx, resource.TaskEventDownloadFailed); err != nil {
		task.Log.Errorf("task fsm event failed: %s", err.Error())
		return
	}
}

// createDownloadRecord stores peer download records.
func (v *V1) createDownloadRecord(peer *resource.Peer, parents []*resource.Peer, req *schedulerv1.PeerResult) {
	var parentRecords []storage.Parent
	for _, parent := range parents {
		parentRecord := storage.Parent{
			ID:               parent.ID,
			Tag:              parent.Task.Tag,
			Application:      parent.Task.Application,
			State:            parent.FSM.Current(),
			Cost:             parent.Cost.Load().Nanoseconds(),
			UploadPieceCount: 0,
			CreatedAt:        parent.CreatedAt.Load().UnixNano(),
			UpdatedAt:        parent.UpdatedAt.Load().UnixNano(),
			Host: storage.Host{
				ID:                    parent.Host.ID,
				Type:                  parent.Host.Type.Name(),
				Hostname:              parent.Host.Hostname,
				IP:                    parent.Host.IP,
				Port:                  parent.Host.Port,
				DownloadPort:          parent.Host.DownloadPort,
				OS:                    parent.Host.OS,
				Platform:              parent.Host.Platform,
				PlatformFamily:        parent.Host.PlatformFamily,
				PlatformVersion:       parent.Host.PlatformVersion,
				KernelVersion:         parent.Host.KernelVersion,
				ConcurrentUploadLimit: parent.Host.ConcurrentUploadLimit.Load(),
				ConcurrentUploadCount: parent.Host.ConcurrentUploadCount.Load(),
				UploadCount:           parent.Host.UploadCount.Load(),
				UploadFailedCount:     parent.Host.UploadFailedCount.Load(),
				CreatedAt:             parent.Host.CreatedAt.Load().UnixNano(),
				UpdatedAt:             parent.Host.UpdatedAt.Load().UnixNano(),
			},
		}

		parentRecord.Host.CPU = resource.CPU{
			LogicalCount:   parent.Host.CPU.LogicalCount,
			PhysicalCount:  parent.Host.CPU.PhysicalCount,
			Percent:        parent.Host.CPU.Percent,
			ProcessPercent: parent.Host.CPU.ProcessPercent,
			Times: resource.CPUTimes{
				User:      parent.Host.CPU.Times.User,
				System:    parent.Host.CPU.Times.System,
				Idle:      parent.Host.CPU.Times.Idle,
				Nice:      parent.Host.CPU.Times.Nice,
				Iowait:    parent.Host.CPU.Times.Iowait,
				Irq:       parent.Host.CPU.Times.Irq,
				Softirq:   parent.Host.CPU.Times.Softirq,
				Steal:     parent.Host.CPU.Times.Steal,
				Guest:     parent.Host.CPU.Times.Guest,
				GuestNice: parent.Host.CPU.Times.GuestNice,
			},
		}

		parentRecord.Host.Memory = resource.Memory{
			Total:              parent.Host.Memory.Total,
			Available:          parent.Host.Memory.Available,
			Used:               parent.Host.Memory.Used,
			UsedPercent:        parent.Host.Memory.UsedPercent,
			ProcessUsedPercent: parent.Host.Memory.ProcessUsedPercent,
			Free:               parent.Host.Memory.Free,
		}

		parentRecord.Host.Network = resource.Network{
			TCPConnectionCount:       parent.Host.Network.TCPConnectionCount,
			UploadTCPConnectionCount: parent.Host.Network.UploadTCPConnectionCount,
			Location:                 parent.Host.Network.Location,
			IDC:                      parent.Host.Network.IDC,
		}

		parentRecord.Host.Disk = resource.Disk{
			Total:             parent.Host.Disk.Total,
			Free:              parent.Host.Disk.Free,
			Used:              parent.Host.Disk.Used,
			UsedPercent:       parent.Host.Disk.UsedPercent,
			InodesTotal:       parent.Host.Disk.InodesTotal,
			InodesUsed:        parent.Host.Disk.InodesUsed,
			InodesFree:        parent.Host.Disk.InodesFree,
			InodesUsedPercent: parent.Host.Disk.InodesUsedPercent,
		}

		parentRecord.Host.Build = resource.Build{
			GitVersion: parent.Host.Build.GitVersion,
			GitCommit:  parent.Host.Build.GitCommit,
			GoVersion:  parent.Host.Build.GoVersion,
			Platform:   parent.Host.Build.Platform,
		}

		peer.Pieces.Range(func(key, value any) bool {
			piece, ok := value.(*resource.Piece)
			if !ok {
				return true
			}

			if piece.ParentID == parent.ID {
				parentRecord.UploadPieceCount++
			}

			return true
		})

		parentRecords = append(parentRecords, parentRecord)
	}

	download := storage.Download{
		ID:          peer.ID,
		Tag:         peer.Task.Tag,
		Application: peer.Task.Application,
		State:       peer.FSM.Current(),
		Cost:        peer.Cost.Load().Nanoseconds(),
		Parents:     parentRecords,
		CreatedAt:   peer.CreatedAt.Load().UnixNano(),
		UpdatedAt:   peer.UpdatedAt.Load().UnixNano(),
		Task: storage.Task{
			ID:                    peer.Task.ID,
			URL:                   peer.Task.URL,
			Type:                  peer.Task.Type.String(),
			ContentLength:         peer.Task.ContentLength.Load(),
			TotalPieceCount:       peer.Task.TotalPieceCount.Load(),
			BackToSourceLimit:     peer.Task.BackToSourceLimit.Load(),
			BackToSourcePeerCount: int32(peer.Task.BackToSourcePeers.Len()),
			State:                 peer.Task.FSM.Current(),
			CreatedAt:             peer.Task.CreatedAt.Load().UnixNano(),
			UpdatedAt:             peer.Task.UpdatedAt.Load().UnixNano(),
		},
		Host: storage.Host{
			ID:                    peer.Host.ID,
			Type:                  peer.Host.Type.Name(),
			Hostname:              peer.Host.Hostname,
			IP:                    peer.Host.IP,
			Port:                  peer.Host.Port,
			DownloadPort:          peer.Host.DownloadPort,
			OS:                    peer.Host.OS,
			Platform:              peer.Host.Platform,
			PlatformFamily:        peer.Host.PlatformFamily,
			PlatformVersion:       peer.Host.PlatformVersion,
			KernelVersion:         peer.Host.KernelVersion,
			ConcurrentUploadLimit: peer.Host.ConcurrentUploadLimit.Load(),
			ConcurrentUploadCount: peer.Host.ConcurrentUploadCount.Load(),
			UploadCount:           peer.Host.UploadCount.Load(),
			UploadFailedCount:     peer.Host.UploadFailedCount.Load(),
			CreatedAt:             peer.Host.CreatedAt.Load().UnixNano(),
			UpdatedAt:             peer.Host.UpdatedAt.Load().UnixNano(),
		},
	}

	download.Host.CPU = resource.CPU{
		LogicalCount:   peer.Host.CPU.LogicalCount,
		PhysicalCount:  peer.Host.CPU.PhysicalCount,
		Percent:        peer.Host.CPU.Percent,
		ProcessPercent: peer.Host.CPU.ProcessPercent,
		Times: resource.CPUTimes{
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
	}

	download.Host.Memory = resource.Memory{
		Total:              peer.Host.Memory.Total,
		Available:          peer.Host.Memory.Available,
		Used:               peer.Host.Memory.Used,
		UsedPercent:        peer.Host.Memory.UsedPercent,
		ProcessUsedPercent: peer.Host.Memory.ProcessUsedPercent,
		Free:               peer.Host.Memory.Free,
	}

	download.Host.Network = resource.Network{
		TCPConnectionCount:       peer.Host.Network.TCPConnectionCount,
		UploadTCPConnectionCount: peer.Host.Network.UploadTCPConnectionCount,
		Location:                 peer.Host.Network.Location,
		IDC:                      peer.Host.Network.IDC,
	}

	download.Host.Disk = resource.Disk{
		Total:             peer.Host.Disk.Total,
		Free:              peer.Host.Disk.Free,
		Used:              peer.Host.Disk.Used,
		UsedPercent:       peer.Host.Disk.UsedPercent,
		InodesTotal:       peer.Host.Disk.InodesTotal,
		InodesUsed:        peer.Host.Disk.InodesUsed,
		InodesFree:        peer.Host.Disk.InodesFree,
		InodesUsedPercent: peer.Host.Disk.InodesUsedPercent,
	}

	download.Host.Build = resource.Build{
		GitVersion: peer.Host.Build.GitVersion,
		GitCommit:  peer.Host.Build.GitCommit,
		GoVersion:  peer.Host.Build.GoVersion,
		Platform:   peer.Host.Build.Platform,
	}

	if req.Code != commonv1.Code_Success {
		download.Error = storage.Error{
			Code: req.Code.String(),
		}
	}

	if err := v.storage.CreateDownload(download); err != nil {
		peer.Log.Error(err)
	}
}
