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

package rpcserver

import (
	"context"
	"fmt"
	"io"
	"sync"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler/server"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var tracer trace.Tracer

func init() {
	tracer = otel.Tracer("scheduler-server")
}

type SchedulerServer struct {
	service *core.SchedulerService
}

// NewSchedulerServer returns a new transparent scheduler server from the given options
func NewSchedulerServer(service *core.SchedulerService) (server.SchedulerServer, error) {
	return &SchedulerServer{
		service: service,
	}, nil
}

func (s *SchedulerServer) RegisterPeerTask(ctx context.Context, request *scheduler.PeerTaskRequest) (resp *scheduler.RegisterResult, err error) {
	defer func() {
		logger.Debugf("peer %s register result %v, err: %v", request.PeerId, resp, err)
	}()
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanPeerRegister, trace.WithSpanKind(trace.SpanKindServer))
	span.SetAttributes(config.AttributePeerRegisterRequest.String(request.String()))
	defer span.End()
	logger.Debugf("register peer task, req: %+v", request)
	resp = new(scheduler.RegisterResult)
	if verifyErr := validateParams(request); verifyErr != nil {
		err = dferrors.Newf(dfcodes.BadRequest, "bad request param: %v", verifyErr)
		logger.Errorf("validate register request failed: %v", err)
		span.RecordError(err)
		return
	}
	taskID := s.service.GenerateTaskID(request.Url, request.UrlMeta, request.PeerId)
	span.SetAttributes(config.AttributeTaskID.String(taskID))
	task := s.service.GetOrCreateTask(ctx, supervisor.NewTask(taskID, request.Url, request.UrlMeta))
	if task.IsFail() {
		err = dferrors.Newf(dfcodes.SchedTaskStatusError, "task status is %s", task.GetStatus())
		logger.Errorf("task is unhealthy, task status is: %s", task.GetStatus())
		span.RecordError(err)
		return
	}
	resp.SizeScope = getTaskSizeScope(task)
	resp.TaskId = taskID
	switch resp.SizeScope {
	case base.SizeScope_TINY:
		resp.DirectPiece = &scheduler.RegisterResult_PieceContent{
			PieceContent: task.DirectPiece,
		}
		span.SetAttributes(config.AttributeTaskSizeScope.String(resp.SizeScope.String()))
		return
	case base.SizeScope_SMALL:
		span.SetAttributes(config.AttributeTaskSizeScope.String(resp.SizeScope.String()))
		peer, regErr := s.service.RegisterPeerTask(request, task)
		if regErr != nil {
			err = dferrors.Newf(dfcodes.SchedPeerRegisterFail, "failed to register peer: %v", regErr)
			span.RecordError(err)
			return
		}
		parent, schErr := s.service.ScheduleParent(peer)
		if schErr != nil {
			span.SetAttributes(config.AttributeTaskSizeScope.String(base.SizeScope_NORMAL.String()))
			span.AddEvent(config.EventScheduleParentFail)
			resp.SizeScope = base.SizeScope_NORMAL
			resp.TaskId = taskID
			//err = dferrors.Newf(dfcodes.SchedPeerScheduleFail, "failed to schedule peer %v: %v", peer.PeerID, schErr)
			return
		}
		firstPiece := task.GetPiece(0)
		singlePiece := &scheduler.SinglePiece{
			DstPid:  parent.PeerID,
			DstAddr: fmt.Sprintf("%s:%d", parent.Host.IP, parent.Host.DownloadPort),
			PieceInfo: &base.PieceInfo{
				PieceNum:    firstPiece.PieceNum,
				RangeStart:  firstPiece.RangeStart,
				RangeSize:   firstPiece.RangeSize,
				PieceMd5:    firstPiece.PieceMd5,
				PieceOffset: firstPiece.PieceOffset,
				PieceStyle:  firstPiece.PieceStyle,
			},
		}
		resp.DirectPiece = &scheduler.RegisterResult_SinglePiece{
			SinglePiece: singlePiece,
		}
		span.SetAttributes(config.AttributeSinglePiece.String(singlePiece.String()))
		return
	default:
		span.SetAttributes(config.AttributeTaskSizeScope.String(resp.SizeScope.String()))
		_, regErr := s.service.RegisterPeerTask(request, task)
		if regErr != nil {
			err = dferrors.Newf(dfcodes.SchedPeerRegisterFail, "failed to register peer: %v", regErr)
			span.RecordError(regErr)
		}
		return
	}
}

func (s *SchedulerServer) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
	var span trace.Span
	ctx, span := tracer.Start(stream.Context(), config.SpanReportPieceResult, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	peerPacketChan := make(chan *scheduler.PeerPacket, 1)
	var peer *supervisor.Peer
	initialized := false
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)
	var once sync.Once
	g.Go(func() error {
		defer func() {
			cancel()
			once.Do(peer.UnBindSendChannel)
		}()
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				pieceResult, err := stream.Recv()
				if err == io.EOF {
					span.AddEvent("report piece process exited because client has terminated sending the request")
					return nil
				}
				if err != nil {
					if status.Code(err) == codes.Canceled {
						span.AddEvent("report piece process exited because an error exception was received")
						if peer != nil {
							logger.Info("peer %s canceled", peer.PeerID)
							return nil
						}
					}
					return dferrors.Newf(dfcodes.SchedPeerPieceResultReportFail, "peer piece result report error: %v", err)
				}
				logger.Debugf("report piece result %v of peer %s", pieceResult, pieceResult.SrcPid)
				var ok bool
				peer, ok = s.service.GetPeerTask(pieceResult.SrcPid)
				if !ok {
					return dferrors.Newf(dfcodes.SchedPeerNotFound, "peer %s not found", pieceResult.SrcPid)
				}
				if !initialized {
					peer.BindSendChannel(peerPacketChan)
					peer.SetStatus(supervisor.PeerStatusRunning)
					initialized = true
					span.SetAttributes(config.AttributePeerID.String(peer.PeerID))
					span.AddEvent("init")
				}
				if pieceResult.PieceNum == common.EndOfPiece {
					return nil
				}
				if err := s.service.HandlePieceResult(ctx, peer, pieceResult); err != nil {
					logger.Errorf("handle piece result %v fail: %v", pieceResult, err)
				}
			}
		}
	})

	g.Go(func() error {
		defer func() {
			cancel()
			once.Do(peer.UnBindSendChannel)
		}()
		for {
			select {
			case <-ctx.Done():
				return nil
			case pp, ok := <-peerPacketChan:
				if !ok {
					span.AddEvent("exit report piece process due to send channel has closed")
					return nil
				}
				span.AddEvent("schedule event", trace.WithAttributes(config.AttributeSchedulePacket.String(pp.String())))
				err := stream.Send(pp)
				if err != nil {
					logger.Errorf("send peer %s schedule packet %v failed: %v", pp.SrcPid, pp, err)
					return dferrors.Newf(dfcodes.SchedPeerPieceResultReportFail, "peer piece result report error: %v", err)
				}
			}
		}
	})
	err := g.Wait()
	logger.Debugf("report piece result: %v", err)
	return err
}

func (s *SchedulerServer) ReportPeerResult(ctx context.Context, result *scheduler.PeerResult) (err error) {
	logger.Debugf("report peer result %+v", result)
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanReportPeerResult, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	span.SetAttributes(config.AttributeReportPeerID.String(result.PeerId))
	span.SetAttributes(config.AttributePeerDownloadSuccess.Bool(result.Success))
	span.SetAttributes(config.AttributePeerDownloadResult.String(result.String()))
	peer, ok := s.service.GetPeerTask(result.PeerId)
	if !ok {
		logger.Warnf("report peer result: peer %s is not exists", result.PeerId)
		err = dferrors.Newf(dfcodes.SchedPeerNotFound, "peer %s not found", result.PeerId)
		span.RecordError(err)
	}
	return s.service.HandlePeerResult(ctx, peer, result)
}

func (s *SchedulerServer) LeaveTask(ctx context.Context, target *scheduler.PeerTarget) (err error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanPeerLeave, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	span.SetAttributes(config.AttributeLeavePeerID.String(target.PeerId))
	span.SetAttributes(config.AttributeLeaveTaskID.String(target.TaskId))
	logger.Debugf("leave task %+v", target)
	peer, ok := s.service.GetPeerTask(target.PeerId)
	if !ok {
		logger.Warnf("leave task: peer %s is not exists", target.PeerId)
		err = dferrors.Newf(dfcodes.SchedPeerNotFound, "peer %s not found", target.PeerId)
		span.RecordError(err)
		return
	}
	return s.service.HandleLeaveTask(ctx, peer)
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

func getTaskSizeScope(task *supervisor.Task) base.SizeScope {
	if task.IsSuccess() {
		if task.ContentLength <= supervisor.TinyFileSize {
			return base.SizeScope_TINY
		}
		if task.PieceTotal == 1 {
			return base.SizeScope_SMALL
		}
	}
	return base.SizeScope_NORMAL
}
