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

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerserver "d7y.io/dragonfly/v2/pkg/rpc/scheduler/server"
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

func (s *server) RegisterPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (*scheduler.RegisterResult, error) {
	taskID := idgen.TaskID(req.Url, req.UrlMeta)
	log := logger.WithTaskAndPeerID(taskID, req.PeerId)
	log.Infof("register peer task, req: %#v", req)

	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanPeerRegister, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	span.SetAttributes(config.AttributePeerRegisterRequest.String(req.String()))
	span.SetAttributes(config.AttributeTaskID.String(taskID))

	// Get task or add new task
	task := s.service.GetOrAddTask(ctx, supervisor.NewTask(taskID, req.Url, req.UrlMeta))
	if task.IsFail() {
		dferr := dferrors.New(base.Code_SchedTaskStatusError, "task status is fail")
		log.Error(dferr.Message)
		span.RecordError(dferr)
		return nil, dferr
	}

	// Task has been successful
	if task.IsSuccess() {
		log.Info("task has been successful")
		sizeScope := task.GetSizeScope()
		span.SetAttributes(config.AttributeTaskSizeScope.String(sizeScope.String()))
		switch sizeScope {
		case base.SizeScope_TINY:
			// when task.DirectPiece length is 0, data is downloaded by common peers, is not cdn
			if int64(len(task.DirectPiece)) == task.ContentLength.Load() {
				log.Info("task size scope is tiny and return piece content directly")
				return &scheduler.RegisterResult{
					TaskId:    taskID,
					SizeScope: sizeScope,
					DirectPiece: &scheduler.RegisterResult_PieceContent{
						PieceContent: task.DirectPiece,
					},
				}, nil
			}
			// fallback to base.SizeScope_SMALL
			log.Warnf("task size scope is tiny, but task.DirectPiece length is %d, not %d",
				len(task.DirectPiece), task.ContentLength.Load())
			fallthrough
		case base.SizeScope_SMALL:
			log.Info("task size scope is small")
			peer := s.service.RegisterTask(req, task)
			parent, err := s.service.SelectParent(peer)
			if err != nil {
				log.Warn("task size scope is small and it can not select parent")
				span.AddEvent(config.EventSmallTaskSelectParentFail)
				return &scheduler.RegisterResult{
					TaskId:    taskID,
					SizeScope: sizeScope,
				}, nil
			}

			firstPiece, ok := task.GetPiece(0)
			if !ok {
				log.Warn("task size scope is small and it can not get first piece")
				return &scheduler.RegisterResult{
					TaskId:    taskID,
					SizeScope: sizeScope,
				}, nil
			}

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
			log.Infof("task size scope is small and return single piece %#v", sizeScope)
			span.SetAttributes(config.AttributeSinglePiece.String(singlePiece.String()))
			return &scheduler.RegisterResult{
				TaskId:    taskID,
				SizeScope: sizeScope,
				DirectPiece: &scheduler.RegisterResult_SinglePiece{
					SinglePiece: singlePiece,
				},
			}, nil
		default:
			log.Info("task size scope is normal and needs to be register")
			s.service.RegisterTask(req, task)
			return &scheduler.RegisterResult{
				TaskId:    taskID,
				SizeScope: sizeScope,
			}, nil
		}
	}

	// Task is unsuccessful
	log.Info("task is unsuccessful and needs to be register")
	s.service.RegisterTask(req, task)
	return &scheduler.RegisterResult{
		TaskId:    taskID,
		SizeScope: base.SizeScope_NORMAL,
	}, nil
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
		err = dferrors.Newf(base.Code_SchedPeerPieceResultReportFail, "receive an error from peer stream: %v", err)
		span.RecordError(err)
		return err
	}
	logger.Debugf("peer %s start report piece result", pieceResult.SrcPid)

	peer, ok := s.service.GetPeer(pieceResult.SrcPid)
	if !ok {
		err = dferrors.Newf(base.Code_SchedPeerNotFound, "peer %s not found", pieceResult.SrcPid)
		span.RecordError(err)
		return err
	}

	if peer.Task.IsFail() {
		err = dferrors.Newf(base.Code_SchedTaskStatusError, "peer's task status is fail, task status %s", peer.Task.GetStatus())
		span.RecordError(err)
		return err
	}

	conn, ok := peer.BindNewConn(stream)
	if !ok {
		err = dferrors.Newf(base.Code_SchedPeerPieceResultReportFail, "peer can not bind conn")
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
		err = dferrors.Newf(base.Code_SchedPeerNotFound, "peer %s not found", result.PeerId)
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
