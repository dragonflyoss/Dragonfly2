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
	"fmt"
	"io"
	"sync"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core"
	"d7y.io/dragonfly/v2/scheduler/types"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type SchedulerServer struct {
	service *core.SchedulerService
}

// NewSchedulerServer returns a new transparent scheduler server from the given options
func NewSchedulerServer(cfg *config.SchedulerConfig, dynConfig config.DynconfigInterface) (*SchedulerServer, error) {
	service, err := core.NewSchedulerService(cfg, dynConfig)
	if err != nil {
		return nil, errors.Wrap(err, "create scheduler service")
	}
	return &SchedulerServer{
		service: service,
	}, nil
}

func (s *SchedulerServer) RegisterPeerTask(ctx context.Context, request *scheduler.PeerTaskRequest) (resp *scheduler.RegisterResult, err error) {
	logger.Debugf("register peer task, req: %+v", request)
	resp = new(scheduler.RegisterResult)
	if verifyErr := validateParams(request); verifyErr != nil {
		err = dferrors.Newf(dfcodes.BadRequest, "bad request param: %v", verifyErr)
		logger.Errorf("validate register request failed: %v", err)
		return
	}
	taskID := s.service.GenerateTaskID(request.Url, request.Filter, request.UrlMeta, request.BizId, request.PeerId)
	task := types.NewTask(taskID, request.Url, request.Filter, request.BizId, request.UrlMeta)
	task, err = s.service.GetOrCreateTask(ctx, task)
	if err != nil {
		err = dferrors.Newf(dfcodes.SchedCDNSeedFail, "create task failed: %v", err)
		logger.Errorf("get or create task failed: %v", err)
		return
	}
	if task.IsFail() {
		err = dferrors.Newf(dfcodes.SchedTaskStatusError, "task status is %d", task.GetStatus())
		logger.Errorf("task status is health: %d", task.GetStatus())
		return
	}
	resp.SizeScope = getTaskSizeScope(task)
	resp.TaskId = taskID
	switch resp.SizeScope {
	case base.SizeScope_TINY:
		resp.DirectPiece = &scheduler.RegisterResult_PieceContent{
			PieceContent: task.DirectPiece,
		}
		return
	case base.SizeScope_SMALL:
		peer, regErr := s.service.RegisterPeerTask(request, task)
		if regErr != nil {
			err = dferrors.Newf(dfcodes.SchedPeerRegisterFail, "failed to register peer: %v", regErr)
			return
		}
		parent, schErr := s.service.ScheduleParent(peer)
		if schErr != nil {
			err = dferrors.Newf(dfcodes.SchedPeerScheduleFail, "failed to schedule peer %v: %v", peer, schErr)
		}
		singlePiece := task.GetPiece(0)
		resp.DirectPiece = &scheduler.RegisterResult_SinglePiece{
			SinglePiece: &scheduler.SinglePiece{
				DstPid:  parent.PeerID,
				DstAddr: fmt.Sprintf("%s:%d", parent.Host.IP, parent.Host.DownloadPort),
				PieceInfo: &base.PieceInfo{
					PieceNum:    singlePiece.PieceNum,
					RangeStart:  singlePiece.RangeStart,
					RangeSize:   singlePiece.RangeSize,
					PieceMd5:    singlePiece.PieceMd5,
					PieceOffset: singlePiece.PieceOffset,
					PieceStyle:  singlePiece.PieceStyle,
				},
			},
		}
		return
	default:
		_, regErr := s.service.RegisterPeerTask(request, task)
		if regErr != nil {
			err = dferrors.Newf(dfcodes.SchedPeerRegisterFail, "failed to register peer: %v", regErr)
		}
		return
	}
}

func (s *SchedulerServer) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
	peerPacketChan := make(chan *scheduler.PeerPacket, 1)
	var once sync.Once
	g := errgroup.Group{}
	g.Go(func() error {
		for {
			pieceResult, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return dferrors.Newf(dfcodes.SchedPeerPieceResultReportFail, "peer piece result report error")
			}
			peer, ok := s.service.GetPeerTask(pieceResult.SrcPid)
			if !ok {
				return dferrors.Newf(dfcodes.SchedPeerNotFound, "peer %s not found", pieceResult.SrcPid)
			}
			once.Do(func() {
				peer.BindSendChannel(peerPacketChan)
			})
			if err := s.service.HandlePieceResult(peer, pieceResult); err != nil {
				logger.Errorf("handle piece result %v fail: %v", pieceResult, err)
			}
		}
	})

	g.Go(func() error {
		for pp := range peerPacketChan {
			err := stream.Send(pp)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return g.Wait()
}

func (s *SchedulerServer) ReportPeerResult(ctx context.Context, result *scheduler.PeerResult) (err error) {
	logger.Debugf("report peer result %+v", result)
	peer, ok := s.service.GetPeerTask(result.PeerId)
	if !ok {
		logger.Warnf("report peer result: peer %s is not exists", result.PeerId)
		return dferrors.Newf(dfcodes.SchedPeerNotFound, "peer %s not found", result.PeerId)
	}
	return s.service.HandlePeerResult(peer, result)
}

func (s *SchedulerServer) LeaveTask(ctx context.Context, target *scheduler.PeerTarget) (err error) {
	logger.Debugf("leave task %+v", target)
	peer, ok := s.service.GetPeerTask(target.PeerId)
	if !ok {
		logger.Warnf("leave task: peer %d is not exists", target.PeerId)
		return dferrors.Newf(dfcodes.SchedPeerNotFound, "peer %s not found", target.PeerId)
	}
	return s.service.HandleLeaveTask(peer)
}

// validateParams validates the params of scheduler.PeerTaskRequest.
func validateParams(req *scheduler.PeerTaskRequest) error {
	if !urlutils.IsValidURL(req.Url) {
		return fmt.Errorf("invalid url: %s", req.Url)
	}

	if stringutils.IsEmpty(req.PeerId) {
		return fmt.Errorf("empty peerID")
	}
	return nil
}

func getTaskSizeScope(task *types.Task) base.SizeScope {
	if task.ContentLength <= types.TinyFileSize {
		return base.SizeScope_TINY
	}
	if task.PieceTotal == 1 {
		return base.SizeScope_SMALL
	}
	return base.SizeScope_NORMAL
}
