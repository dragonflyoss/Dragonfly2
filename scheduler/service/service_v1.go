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
	"net/url"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/status"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	commonv2 "d7y.io/api/pkg/apis/common/v2"
	errordetailsv1 "d7y.io/api/pkg/apis/errordetails/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/rpc/common"
	pkgtime "d7y.io/dragonfly/v2/pkg/time"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

// V1 is the interface for v1 version of the service.
type V1 struct {
	// Resource interface.
	resource resource.Resource

	// Scheduler interface.
	scheduler scheduler.Scheduler

	// Scheduelr service config.
	config *config.Config

	// Dynamic config.
	dynconfig config.DynconfigInterface

	// Storage interface.
	storage storage.Storage
}

// New v1 version of service instance.
func NewV1(
	cfg *config.Config,
	resource resource.Resource,
	scheduler scheduler.Scheduler,
	dynconfig config.DynconfigInterface,
	storage storage.Storage,
) *V1 {
	return &V1{
		resource:  resource,
		scheduler: scheduler,
		config:    cfg,
		dynconfig: dynconfig,
		storage:   storage,
	}
}

// RegisterPeerTask registers peer and triggers seed peer download task.
func (v *V1) RegisterPeerTask(ctx context.Context, req *schedulerv1.PeerTaskRequest) (*schedulerv1.RegisterResult, error) {
	logger.WithPeer(req.PeerHost.Id, req.TaskId, req.PeerId).Infof("register peer task request: %#v %#v",
		req, req.UrlMeta)

	// Store resource.
	task := v.storeTask(ctx, req, commonv1.TaskType_Normal)
	host := v.storeHost(ctx, req.PeerHost)
	peer := v.storePeer(ctx, req.PeerId, task, host, req.UrlMeta.Tag, req.UrlMeta.Application)

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

	// If SizeScope is invalid, then register as SizeScope_NORMAL.
	sizeScope, err := task.SizeScope()
	if err != nil {
		peer.Log.Warnf("scope size is invalid: %s", err.Error())
	}
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
	ctx := stream.Context()
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
			peer.StoreReportPieceStream(stream)
			defer peer.DeleteReportPieceStream()
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

			// Collect peer host traffic metrics.
			if v.config.Metrics.Enable && v.config.Metrics.EnablePeerHost {
				metrics.PeerHostTraffic.WithLabelValues(peer.Tag, peer.Application, metrics.PeerHostTrafficDownloadType, peer.Host.ID, peer.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
				if parent, loaded := v.resource.PeerManager().Load(piece.DstPid); loaded {
					metrics.PeerHostTraffic.WithLabelValues(peer.Tag, peer.Application, metrics.PeerHostTrafficUploadType, parent.Host.ID, parent.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
				} else if !resource.IsPieceBackToSource(piece.DstPid) {
					peer.Log.Warnf("dst peer %s not found", piece.DstPid)
				}
			}

			// Collect traffic metrics.
			if !resource.IsPieceBackToSource(piece.DstPid) {
				metrics.Traffic.WithLabelValues(peer.Tag, peer.Application, metrics.TrafficP2PType).Add(float64(piece.PieceInfo.RangeSize))
			} else {
				metrics.Traffic.WithLabelValues(peer.Tag, peer.Application, metrics.TrafficBackToSourceType).Add(float64(piece.PieceInfo.RangeSize))
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
	metrics.DownloadCount.WithLabelValues(peer.Tag, peer.Application).Inc()

	parents := peer.Parents()
	if !req.Success {
		peer.Log.Error("report failed peer")
		if peer.FSM.Is(resource.PeerStateBackToSource) {
			metrics.DownloadFailureCount.WithLabelValues(peer.Tag, peer.Application, metrics.DownloadFailureBackToSourceType, req.Code.String()).Inc()
			go v.createRecord(peer, parents, req)
			v.handleTaskFailure(ctx, peer.Task, req.GetSourceError(), nil)
			v.handlePeerFailure(ctx, peer)
			return nil
		}

		metrics.DownloadFailureCount.WithLabelValues(peer.Tag, peer.Application, metrics.DownloadFailureP2PType, req.Code.String()).Inc()
		go v.createRecord(peer, parents, req)
		v.handlePeerFailure(ctx, peer)
		return nil
	}
	metrics.PeerTaskDownloadDuration.WithLabelValues(peer.Tag, peer.Application).Observe(float64(req.Cost))

	peer.Log.Info("report success peer")
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		go v.createRecord(peer, parents, req)
		v.handleTaskSuccess(ctx, peer.Task, req)
		v.handlePeerSuccess(ctx, peer)
		return nil
	}

	go v.createRecord(peer, parents, req)
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
	task := resource.NewTask(taskID, req.Url, req.TaskType, req.UrlMeta)
	task, _ = v.resource.TaskManager().LoadOrStore(task)
	host := v.storeHost(ctx, req.PeerHost)
	peer := v.storePeer(ctx, peerID, task, host, req.UrlMeta.Tag, req.UrlMeta.Application)

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

		// Load downloaded piece infos.
		for _, pieceInfo := range req.PiecePacket.PieceInfos {
			peer.Pieces.Add(&resource.Piece{
				Number:      uint32(pieceInfo.PieceNum),
				ParentID:    req.PiecePacket.DstPid,
				Offset:      pieceInfo.PieceOffset,
				Size:        uint64(pieceInfo.RangeSize),
				Digest:      digest.New("md5", pieceInfo.PieceMd5).String(),
				TrafficType: commonv2.TrafficType_LOCAL_PEER,
				Cost:        0,
				CreatedAt:   time.Now(),
			})
			peer.FinishedPieces.Set(uint(pieceInfo.PieceNum))
			peer.AppendPieceCost(int64(pieceInfo.DownloadCost) * int64(time.Millisecond))
			task.StorePiece(pieceInfo)
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
		Type:             task.Type,
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
	metrics.LeaveTaskCount.WithLabelValues(peer.Tag, peer.Application).Inc()

	if err := peer.FSM.Event(ctx, resource.PeerEventLeave); err != nil {
		metrics.LeaveTaskFailureCount.WithLabelValues(peer.Tag, peer.Application).Inc()
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
				SecurityDomain:           req.Network.SecurityDomain,
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

	host.IP = req.Ip
	host.DownloadPort = req.DownloadPort
	host.Type = types.ParseHostType(req.Type)
	host.OS = req.Os
	host.Platform = req.Platform
	host.PlatformFamily = req.PlatformFamily
	host.PlatformVersion = req.PlatformVersion
	host.KernelVersion = req.KernelVersion

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
			SecurityDomain:           req.Network.SecurityDomain,
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

	if concurrentUploadLimit > 0 {
		host.ConcurrentUploadLimit.Store(concurrentUploadLimit)
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
	if !task.FSM.Is(resource.TaskStateRunning) {
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
		priority = commonv1.Priority(peer.GetPriority(dynconfig))
	}
	peer.Log.Infof("peer priority is %d", priority)

	switch priority {
	case commonv1.Priority_LEVEL6, commonv1.Priority_LEVEL0:
		if v.config.SeedPeer.Enable && !task.IsSeedPeerFailed() {
			go v.triggerSeedPeerTask(ctx, task)
			return nil
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
func (v *V1) triggerSeedPeerTask(ctx context.Context, task *resource.Task) {
	ctx = trace.ContextWithSpan(context.Background(), trace.SpanFromContext(ctx))

	task.Log.Info("trigger seed peer")
	peer, endOfPiece, err := v.resource.SeedPeer().TriggerTask(ctx, task)
	if err != nil {
		task.Log.Errorf("trigger seed peer failed: %s", err.Error())
		v.handleTaskFailure(ctx, task, nil, err)
		return
	}

	// Update the task status first to help peer scheduling evaluation and scoring.
	peer.Log.Info("trigger seed peer successfully")
	v.handleTaskSuccess(ctx, task, endOfPiece)
	v.handlePeerSuccess(ctx, peer)
}

// storeTask stores a new task or reuses a previous task.
func (v *V1) storeTask(ctx context.Context, req *schedulerv1.PeerTaskRequest, taskType commonv1.TaskType) *resource.Task {
	task, loaded := v.resource.TaskManager().Load(req.TaskId)
	if !loaded {
		// Create a task for the first time.
		task = resource.NewTask(req.TaskId, req.Url, taskType, req.UrlMeta, resource.WithBackToSourceLimit(int32(v.config.Scheduler.BackToSourceCount)))
		v.resource.TaskManager().Store(task)
		task.Log.Info("create new task")
		return task
	}

	// Task is the pointer, if the task already exists, the next request will
	// update the task's Url and UrlMeta in task manager.
	task.URL = req.Url
	task.URLMeta = req.UrlMeta
	task.Log.Info("task already exists")
	return task
}

// storeHost stores a new host or reuses a previous host.
func (v *V1) storeHost(ctx context.Context, peerHost *schedulerv1.PeerHost) *resource.Host {
	host, loaded := v.resource.HostManager().Load(peerHost.Id)
	if !loaded {
		// Get scheduler cluster client config by manager.
		options := []resource.HostOption{resource.WithNetwork(resource.Network{
			SecurityDomain: peerHost.SecurityDomain,
			Location:       peerHost.Location,
			IDC:            peerHost.Idc,
		})}
		if clientConfig, err := v.dynconfig.GetSchedulerClusterClientConfig(); err == nil && clientConfig.LoadLimit > 0 {
			options = append(options, resource.WithConcurrentUploadLimit(int32(clientConfig.LoadLimit)))
		}

		host = resource.NewHost(
			peerHost.Id, peerHost.Ip, peerHost.HostName,
			peerHost.RpcPort, peerHost.DownPort, types.HostTypeNormal,
			options...,
		)

		v.resource.HostManager().Store(host)
		host.Log.Info("create new host")
		return host
	}

	host.Log.Info("host already exists")
	return host
}

// storePeer stores a new peer or reuses a previous peer.
func (v *V1) storePeer(ctx context.Context, peerID string, task *resource.Task, host *resource.Host, tag, application string) *resource.Peer {
	var options []resource.PeerOption
	if tag != "" {
		options = append(options, resource.WithTag(tag))
	}

	if application != "" {
		options = append(options, resource.WithApplication(application))
	}

	peer, loaded := v.resource.PeerManager().LoadOrStore(resource.NewPeer(peerID, task, host, options...))
	if !loaded {
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
		TaskType:  peer.Task.Type,
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
		TaskType:  peer.Task.Type,
		SizeScope: commonv1.SizeScope_TINY,
		DirectPiece: &schedulerv1.RegisterResult_PieceContent{
			PieceContent: peer.Task.DirectPiece,
		},
	}, nil
}

// registerSmallTask registers the small task.
func (v *V1) registerSmallTask(ctx context.Context, peer *resource.Peer) (*schedulerv1.RegisterResult, error) {
	parent, found := v.scheduler.FindParent(ctx, peer, set.NewSafeSet[string]())
	if !found {
		return nil, errors.New("parent not found")
	}

	// When task size scope is small, parent must be downloaded successfully
	// before returning to the parent directly.
	if !parent.FSM.Is(resource.PeerStateSucceeded) {
		return nil, fmt.Errorf("parent state is %s", parent.FSM.Current())
	}

	firstPiece, loaded := peer.Task.LoadPiece(0)
	if !loaded {
		return nil, fmt.Errorf("first piece not found")
	}

	// Delete inedges of peer.
	if err := peer.Task.DeletePeerInEdges(peer.ID); err != nil {
		return nil, err
	}

	// Add edges between parent and peer.
	if err := peer.Task.AddPeerEdge(parent, peer); err != nil {
		return nil, err
	}

	if err := peer.FSM.Event(ctx, resource.PeerEventRegisterSmall); err != nil {
		return nil, err
	}

	return &schedulerv1.RegisterResult{
		TaskId:    peer.Task.ID,
		TaskType:  peer.Task.Type,
		SizeScope: commonv1.SizeScope_SMALL,
		DirectPiece: &schedulerv1.RegisterResult_SinglePiece{
			SinglePiece: &schedulerv1.SinglePiece{
				DstPid:  parent.ID,
				DstAddr: fmt.Sprintf("%s:%d", parent.Host.IP, parent.Host.DownloadPort),
				PieceInfo: &commonv1.PieceInfo{
					PieceNum:    firstPiece.PieceNum,
					RangeStart:  firstPiece.RangeStart,
					RangeSize:   firstPiece.RangeSize,
					PieceMd5:    firstPiece.PieceMd5,
					PieceOffset: firstPiece.PieceOffset,
					PieceStyle:  firstPiece.PieceStyle,
				},
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
		TaskType:  peer.Task.Type,
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

		v.scheduler.ScheduleParent(ctx, peer, set.NewSafeSet[string]())
	default:
	}
}

// handleEndOfPiece handles end of piece.
func (v *V1) handleEndOfPiece(ctx context.Context, peer *resource.Peer) {}

// handlePieceSuccess handles successful piece.
func (v *V1) handlePieceSuccess(ctx context.Context, peer *resource.Peer, piece *schedulerv1.PieceResult) {
	// Distinguish traffic type.
	trafficType := commonv2.TrafficType_REMOTE_PEER
	if resource.IsPieceBackToSource(piece.DstPid) {
		trafficType = commonv2.TrafficType_BACK_TO_SOURCE
	}

	// Update peer piece info.
	cost := time.Duration(int64(piece.PieceInfo.DownloadCost) * int64(time.Millisecond))
	peer.Pieces.Add(&resource.Piece{
		Number:      uint32(piece.PieceInfo.PieceNum),
		ParentID:    piece.DstPid,
		Offset:      piece.PieceInfo.PieceOffset,
		Size:        uint64(piece.PieceInfo.RangeSize),
		Digest:      digest.New("md5", piece.PieceInfo.PieceMd5).String(),
		TrafficType: trafficType,
		Cost:        cost,
		CreatedAt:   time.Now().Add(-cost),
	})
	peer.FinishedPieces.Set(uint(piece.PieceInfo.PieceNum))
	peer.AppendPieceCost(pkgtime.SubNano(int64(piece.EndTime), int64(piece.BeginTime)).Milliseconds())

	// When the piece is downloaded successfully,
	// peer's UpdatedAt needs to be updated
	// to prevent the peer from being GC during the download process.
	peer.UpdatedAt.Store(time.Now())
	peer.PieceUpdatedAt.Store(time.Now())

	// When the piece is downloaded successfully,
	// dst peer's UpdatedAt needs to be updated
	// to prevent the dst peer from being GC during the download process.
	if !resource.IsPieceBackToSource(piece.DstPid) {
		if destPeer, loaded := v.resource.PeerManager().Load(piece.DstPid); loaded {
			destPeer.UpdatedAt.Store(time.Now())
		}
	}

	// When the peer downloads back-to-source,
	// piece downloads successfully updates the task piece info.
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		peer.Task.StorePiece(piece.PieceInfo)
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
		v.scheduler.ScheduleParent(ctx, peer, peer.BlockParents)
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
			go v.triggerSeedPeerTask(ctx, parent.Task)
		}
	default:
	}

	// Peer state is PeerStateRunning will be rescheduled.
	if !peer.FSM.Is(resource.PeerStateRunning) {
		peer.Log.Infof("peer state is %s and can not be rescheduled", peer.FSM.Current())

		// Returns an scheduling error if the peer
		// state is not PeerStateRunning.
		stream, loaded := peer.LoadReportPieceStream()
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
	v.scheduler.ScheduleParent(ctx, peer, peer.BlockParents)
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
	sizeScope, err := peer.Task.SizeScope()
	if err != nil {
		peer.Log.Error(err)
		return
	}

	if sizeScope == commonv1.SizeScope_TINY && len(peer.Task.DirectPiece) == 0 {
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
		v.scheduler.ScheduleParent(ctx, child, child.BlockParents)
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
		v.scheduler.ScheduleParent(ctx, child, child.BlockParents)
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
			task.NotifyPeers(&schedulerv1.PeerPacket{
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
					var proto = "unknown"
					if u, err := url.Parse(task.URL); err == nil {
						proto = u.Scheme
					}

					task.Log.Infof("source error: %#v", d)
					// TODO currently, metrics.PeerTaskSourceErrorCounter is only updated for seed peer source error, need update for normal peer
					if d.Metadata != nil {
						metrics.PeerTaskSourceErrorCounter.WithLabelValues(
							task.URLMeta.Tag, task.URLMeta.Application, proto, fmt.Sprintf("%d", d.Metadata.StatusCode)).Inc()
					} else {
						metrics.PeerTaskSourceErrorCounter.WithLabelValues(
							task.URLMeta.Tag, task.URLMeta.Application, proto, "0").Inc()
					}
					if !d.Temporary {
						task.NotifyPeers(&schedulerv1.PeerPacket{
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
		task.NotifyPeers(&schedulerv1.PeerPacket{
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

// createRecord stores peer download records.
func (v *V1) createRecord(peer *resource.Peer, parents []*resource.Peer, req *schedulerv1.PeerResult) {
	var parentRecords []storage.Parent
	for _, parent := range parents {
		parentRecord := storage.Parent{
			ID:               parent.ID,
			Tag:              parent.Tag,
			Application:      parent.Application,
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
			SecurityDomain:           parent.Host.Network.SecurityDomain,
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

		for _, piece := range peer.Pieces.Values() {
			if piece.ParentID == parent.ID {
				parentRecord.UploadPieceCount++
			}
		}

		parentRecords = append(parentRecords, parentRecord)
	}

	record := storage.Record{
		ID:          peer.ID,
		Tag:         peer.Tag,
		Application: peer.Application,
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

	record.Host.CPU = resource.CPU{
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

	record.Host.Memory = resource.Memory{
		Total:              peer.Host.Memory.Total,
		Available:          peer.Host.Memory.Available,
		Used:               peer.Host.Memory.Used,
		UsedPercent:        peer.Host.Memory.UsedPercent,
		ProcessUsedPercent: peer.Host.Memory.ProcessUsedPercent,
		Free:               peer.Host.Memory.Free,
	}

	record.Host.Network = resource.Network{
		TCPConnectionCount:       peer.Host.Network.TCPConnectionCount,
		UploadTCPConnectionCount: peer.Host.Network.UploadTCPConnectionCount,
		SecurityDomain:           peer.Host.Network.SecurityDomain,
		Location:                 peer.Host.Network.Location,
		IDC:                      peer.Host.Network.IDC,
	}

	record.Host.Disk = resource.Disk{
		Total:             peer.Host.Disk.Total,
		Free:              peer.Host.Disk.Free,
		Used:              peer.Host.Disk.Used,
		UsedPercent:       peer.Host.Disk.UsedPercent,
		InodesTotal:       peer.Host.Disk.InodesTotal,
		InodesUsed:        peer.Host.Disk.InodesUsed,
		InodesFree:        peer.Host.Disk.InodesFree,
		InodesUsedPercent: peer.Host.Disk.InodesUsedPercent,
	}

	record.Host.Build = resource.Build{
		GitVersion: peer.Host.Build.GitVersion,
		GitCommit:  peer.Host.Build.GitCommit,
		GoVersion:  peer.Host.Build.GoVersion,
		Platform:   peer.Host.Build.Platform,
	}

	if req.Code != commonv1.Code_Success {
		record.Error = storage.Error{
			Code: req.Code.String(),
		}
	}

	if err := v.storage.Create(record); err != nil {
		peer.Log.Error(err)
	}
}
