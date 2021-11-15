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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerserver "d7y.io/dragonfly/v2/pkg/rpc/scheduler/server"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
)

var tracer = otel.Tracer("scheduler-server")

type server struct {
	*grpc.Server
	service *core.SchedulerService
}

// New returns a new transparent scheduler server from the given options
func New(service *core.SchedulerService, opts ...grpc.ServerOption) (*grpc.Server, error) {
	svr := &server{
		service: service,
	}

	svr.Server = schedulerserver.New(svr, opts...)
	return svr.Server, nil
}

func (s *server) RegisterPeerTask(ctx context.Context, request *scheduler.PeerTaskRequest) (resp *scheduler.RegisterResult, err error) {
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
		logger.Errorf("register request: %v", err)
		span.RecordError(err)
		return
	}

	taskID := s.service.GenerateTaskID(request.Url, request.UrlMeta, request.PeerId)
	span.SetAttributes(config.AttributeTaskID.String(taskID))
	task := s.service.GetOrCreateTask(ctx, supervisor.NewTask(taskID, request.Url, request.UrlMeta))
	if task.IsFail() {
		err = dferrors.New(dfcodes.SchedTaskStatusError, "task status is fail")
		logger.Errorf("task %s status is fail", task.ID)
		span.RecordError(err)
		return
	}
	resp.SizeScope = getTaskSizeScope(task)
	span.SetAttributes(config.AttributeTaskSizeScope.String(resp.SizeScope.String()))
	resp.TaskId = taskID
	switch resp.SizeScope {
	case base.SizeScope_TINY:
		resp.DirectPiece = &scheduler.RegisterResult_PieceContent{
			PieceContent: task.DirectPiece,
		}
		return
	case base.SizeScope_SMALL:
		peer := s.service.RegisterPeerTask(request, task)
		parent, schErr := s.service.SelectParent(peer)
		if schErr != nil {
			span.AddEvent(config.EventSmallTaskSelectParentFail)
			resp.SizeScope = base.SizeScope_NORMAL
			resp.TaskId = taskID
			return
		}
		firstPiece, _ := task.GetPiece(0)
		singlePiece := &scheduler.SinglePiece{
			DstPid:  parent.ID,
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
		s.service.RegisterPeerTask(request, task)
		return
	}
}

func (s *server) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
	var span trace.Span
	ctx, span := tracer.Start(stream.Context(), config.SpanReportPieceResult, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	pieceResult, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		err = dferrors.Newf(dfcodes.SchedPeerPieceResultReportFail, "receive an error from peer stream: %v", err)
		span.RecordError(err)
		return err
	}
	logger.Debugf("peer %s start report piece result", pieceResult.SrcPid)

	peer, ok := s.service.GetPeer(pieceResult.SrcPid)
	if !ok {
		err = dferrors.Newf(dfcodes.SchedPeerNotFound, "peer %s not found", pieceResult.SrcPid)
		span.RecordError(err)
		return err
	}

	if peer.Task.IsFail() {
		err = dferrors.Newf(dfcodes.SchedTaskStatusError, "peer's task status is fail, task status %s", peer.Task.GetStatus())
		span.RecordError(err)
		return err
	}

	conn, ok := peer.BindNewConn(stream)
	if !ok {
		err = dferrors.Newf(dfcodes.SchedPeerPieceResultReportFail, "peer can not bind conn")
		span.RecordError(err)
		return err
	}
	logger.Infof("peer %s is connected", peer.ID)

	defer func() {
		logger.Infof("peer %s is disconnect: %v", peer.ID, conn.Error())
		span.RecordError(conn.Error())
	}()
	if err := s.service.HandlePieceResult(ctx, peer, pieceResult); err != nil {
		logger.Errorf("peer %s handle piece result %v fail: %v", peer.ID, pieceResult, err)
	}
	for {
		select {
		case <-conn.Done():
			return conn.Error()
		case piece := <-conn.Receiver():
			if piece == nil {
				logger.Infof("peer %s channel has been closed", peer.ID)
				continue
			}
			if err := s.service.HandlePieceResult(ctx, peer, piece); err != nil {
				logger.Errorf("peer %s handle piece result %v fail: %v", peer.ID, piece, err)
			}
		}
	}
}

func (s *server) ReportPeerResult(ctx context.Context, result *scheduler.PeerResult) (err error) {
	logger.Debugf("report peer result %v", result)
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanReportPeerResult, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	span.SetAttributes(config.AttributeReportPeerID.String(result.PeerId))
	span.SetAttributes(config.AttributePeerDownloadSuccess.Bool(result.Success))
	span.SetAttributes(config.AttributePeerDownloadResult.String(result.String()))
	peer, ok := s.service.GetPeer(result.PeerId)
	if !ok {
		logger.Warnf("report peer result: peer %s is not exists", result.PeerId)
		err = dferrors.Newf(dfcodes.SchedPeerNotFound, "peer %s not found", result.PeerId)
		span.RecordError(err)
		return err
	}
	return s.service.HandlePeerResult(ctx, peer, result)
}

func (s *server) LeaveTask(ctx context.Context, target *scheduler.PeerTarget) (err error) {
	logger.Debugf("leave task %v", target)
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanPeerLeave, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	span.SetAttributes(config.AttributeLeavePeerID.String(target.PeerId))
	span.SetAttributes(config.AttributeLeaveTaskID.String(target.TaskId))
	peer, ok := s.service.GetPeer(target.PeerId)
	if !ok {
		logger.Warnf("leave task: peer %s is not exists", target.PeerId)
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
		if task.ContentLength.Load() <= supervisor.TinyFileSize {
			return base.SizeScope_TINY
		}
		if task.TotalPieceCount.Load() == 1 {
			return base.SizeScope_SMALL
		}
	}
	return base.SizeScope_NORMAL
}
