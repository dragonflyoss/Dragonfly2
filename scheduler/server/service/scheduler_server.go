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

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/rpc/base"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core"
	"d7y.io/dragonfly/v2/scheduler/types"
	"golang.org/x/sync/errgroup"
)

type SchedulerServer struct {
	service *core.SchedulerService
	worker  *core.Worker
	config  *config.SchedulerConfig
}

// NewSchedulerServer returns a new transparent scheduler server from the given options
func NewSchedulerServer(cfg *config.SchedulerConfig) *SchedulerServer {
	service, _ := core.NewSchedulerService()
	return &SchedulerServer{
		service: service,
		worker:  pool,
		config:  cfg,
	}
}

func (s *SchedulerServer) RegisterPeerTask(ctx context.Context, request *scheduler.PeerTaskRequest) (resp *scheduler.RegisterResult, err error) {
	if verifyErr := validateParams(request); verifyErr != nil {
		err = dferrors.Newf(dfcodes.BadRequest, "bad request param: %v", verifyErr)
		return
	}
	taskID := s.service.GenerateTaskID(request.Url, request.Filter, request.UrlMeta, request.BizId, request.PeerId)
	task := types.NewTask(taskID, request.Url, request.Filter, request.BizId, request.UrlMeta)
	err = s.service.GetOrCreateTask(ctx, task)
	if err != nil {

	}
	// todo 任务状态有问题
	if types.IsBadTask(task.Status) {
		err = dferrors.Newf(dfcodes.SchedNeedBackSource, "task status is %s", task.Status)
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
		peerNode, regErr := s.service.RegisterPeerTask(request, task)
		if regErr != nil {
			err = dferrors.Newf(dfcodes.SchedPeerRegisterFail, "failed to register peer: %v", regErr)
			return
		}
		parent, _, schErr := s.service.ScheduleParent(peerNode)
		if schErr != nil {
			err = dferrors.Newf(dfcodes.SchedPeerScheduleFail, "failed to schedule peerNode %s: %v", peerNode, schErr)
		}
		singlePiece := task.PieceList[0]
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
					PieceStyle:  base.PieceStyle(singlePiece.PieceStyle),
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
	peerPacketChan := make(chan *scheduler.PeerPacket)
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
			_, ok := s.service.GetPeerTask(pieceResult.SrcPid)
			if !ok {
				return dferrors.Newf(dfcodes.SchedPeerNotFound, "peer %s not found", pieceResult.SrcPid)
			}
			s.worker.Submit(core.NewReportPieceResultTask(s.service, pieceResult))
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
	//for {
	//	pieceResult, err := stream.Recv()
	//	if err == io.EOF {
	//		logger.Infof("read all piece result")
	//		return nil
	//	}
	//	if err != nil {
	//
	//	}
	//	if pieceResult.PieceNum == common.EndOfPiece {
	//
	//	}
	//	if err != nil {
	//		// 处理piece error
	//		return err
	//	}
	//	err == nil {
	//		log.Println(tem)
	//	} else {
	//		log.Println("break, err :", err)
	//		break
	//	}
	//	select {
	//	case <-stream.Context().Done():
	//		logger.Infof("")
	//	case <-stream.Recv():
	//
	//	}
	//}
	//err = worker.NewClient(stream, s.worker, s.service).Serve()
	//return
}

func (s *SchedulerServer) ReportPeerResult(ctx context.Context, result *scheduler.PeerResult) (err error) {
	_, ok := s.service.GetPeerTask(result.PeerId)
	if !ok {
		logger.Warnf("report peer result: peer %s is not exists", result.PeerId)
		return dferrors.Newf(dfcodes.SchedPeerNotFound, "peer %s not found", result.PeerId)
	}
	return s.worker.Submit(core.NewReportPeerResultTask(s.service, result))
}

func (s *SchedulerServer) LeaveTask(ctx context.Context, target *scheduler.PeerTarget) (err error) {
	_, ok := s.service.GetPeerTask(target.PeerId)
	if !ok {
		logger.Warnf("leave task: peer %d is not exists", target.PeerId)
		return dferrors.Newf(dfcodes.SchedPeerNotFound, "peer %s not found", target.PeerId)
	}
	return s.worker.Submit(core.NewLeaveTask(s.service, target))
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

const TinyFileSize = 128

func getTaskSizeScope(task *types.Task) base.SizeScope {
	if task.ContentLength <= TinyFileSize {
		return base.SizeScope_TINY
	}
	if task.PieceTotal == 1 {
		return base.SizeScope_SMALL
	}
	return base.SizeScope_NORMAL
}
