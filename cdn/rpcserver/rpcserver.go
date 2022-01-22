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
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"d7y.io/dragonfly/v2/cdn/constants"
	"d7y.io/dragonfly/v2/cdn/supervisor"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	cdnserver "d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/server"
	"d7y.io/dragonfly/v2/pkg/util/hostutils"
)

var tracer = otel.Tracer("cdn-server")

type Server struct {
	*grpc.Server
	config  Config
	service supervisor.CDNService
}

// New returns a new Manager Object.
func New(config Config, cdnService supervisor.CDNService, opts ...grpc.ServerOption) (*Server, error) {
	svr := &Server{
		config:  config,
		service: cdnService,
	}
	svr.Server = cdnserver.New(svr, opts...)
	return svr, nil
}

func (css *Server) ObtainSeeds(ctx context.Context, req *cdnsystem.SeedRequest, psc chan<- *cdnsystem.PieceSeed) (err error) {
	clientAddr := "unknown"
	if pe, ok := peer.FromContext(ctx); ok {
		clientAddr = pe.Addr.String()
	}
	logger.Infof("trigger obtain seed for taskID: %s, url: %s, urlMeta: %s client: %s", req.TaskId, req.Url, req.UrlMeta, clientAddr)
	var span trace.Span
	ctx, span = tracer.Start(ctx, constants.SpanObtainSeeds, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	span.SetAttributes(constants.AttributeObtainSeedsRequest.String(req.String()))
	span.SetAttributes(constants.AttributeTaskID.String(req.TaskId))
	defer func() {
		if r := recover(); r != nil {
			err = dferrors.Newf(base.Code_UnknownError, "obtain task(%s) seeds encounter an panic: %v", req.TaskId, r)
			span.RecordError(err)
			logger.WithTaskID(req.TaskId).Errorf("%v", err)
		}
	}()
	// register seed task
	registeredTask, pieceChan, err := css.service.RegisterSeedTask(ctx, clientAddr, task.NewSeedTask(req.TaskId, req.Url, req.UrlMeta))
	if err != nil {
		if supervisor.IsResourcesLacked(err) {
			err = dferrors.Newf(base.Code_ResourceLacked, "resources lacked for task(%s): %v", req.TaskId, err)
			span.RecordError(err)
			return err
		}
		err = dferrors.Newf(base.Code_CDNTaskRegistryFail, "failed to register seed task(%s): %v", req.TaskId, err)
		span.RecordError(err)
		return err
	}
	peerID := idgen.CDNPeerID(css.config.AdvertiseIP)
	hostID := idgen.CDNHostID(hostutils.FQDNHostname, int32(css.config.ListenPort))
	for piece := range pieceChan {
		pieceSeed := &cdnsystem.PieceSeed{
			PeerId:   peerID,
			HostUuid: hostID,
			PieceInfo: &base.PieceInfo{
				PieceNum:    int32(piece.PieceNum),
				RangeStart:  piece.PieceRange.StartIndex,
				RangeSize:   piece.PieceLen,
				PieceMd5:    piece.PieceMd5,
				PieceOffset: piece.OriginRange.StartIndex,
				PieceStyle:  piece.PieceStyle,
			},
			Done:            false,
			ContentLength:   registeredTask.SourceFileLength,
			TotalPieceCount: registeredTask.TotalPieceCount,
		}
		psc <- pieceSeed
		jsonPiece, err := json.Marshal(pieceSeed)
		if err != nil {
			logger.Errorf("failed to json marshal seed piece: %v", err)
		}
		logger.Debugf("send piece seed: %s to client: %s", jsonPiece, clientAddr)
	}
	seedTask, err := css.service.GetSeedTask(req.TaskId)
	if err != nil {
		err = dferrors.Newf(base.Code_CDNError, "failed to get task(%s): %v", req.TaskId, err)
		if task.IsTaskNotFound(err) {
			err = dferrors.Newf(base.Code_CDNTaskNotFound, "failed to get task(%s): %v", req.TaskId, err)
			span.RecordError(err)
			return err
		}
		err = dferrors.Newf(base.Code_CDNError, "failed to get task(%s): %v", req.TaskId, err)
		span.RecordError(err)
		return err
	}
	if !seedTask.IsSuccess() {
		err = dferrors.Newf(base.Code_CDNTaskDownloadFail, "task(%s) status error , status: %s", req.TaskId, seedTask.CdnStatus)
		span.RecordError(err)
		return err
	}
	pieceSeed := &cdnsystem.PieceSeed{
		PeerId:          peerID,
		HostUuid:        hostID,
		Done:            true,
		ContentLength:   seedTask.SourceFileLength,
		TotalPieceCount: seedTask.TotalPieceCount,
	}
	psc <- pieceSeed
	jsonPiece, err := json.Marshal(pieceSeed)
	if err != nil {
		logger.Errorf("failed to json marshal seed piece: %v", err)
	}
	logger.Debugf("send piece seed: %s to client: %s", jsonPiece, clientAddr)
	return nil
}

func (css *Server) GetPieceTasks(ctx context.Context, req *base.PieceTaskRequest) (piecePacket *base.PiecePacket, err error) {
	var span trace.Span
	_, span = tracer.Start(ctx, constants.SpanGetPieceTasks, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	span.SetAttributes(constants.AttributeGetPieceTasksRequest.String(req.String()))
	span.SetAttributes(constants.AttributeTaskID.String(req.TaskId))
	logger.Infof("get piece tasks: %#v", req)
	defer func() {
		if r := recover(); r != nil {
			err = dferrors.Newf(base.Code_UnknownError, "get task(%s) piece tasks encounter an panic: %v", req.TaskId, r)
			span.RecordError(err)
			logger.WithTaskID(req.TaskId).Errorf("get piece tasks failed: %v", err)
		}
		logger.WithTaskID(req.TaskId).Infof("get piece tasks result success: %t", err == nil)
	}()
	logger.Infof("get piece tasks: %#v", req)
	seedTask, err := css.service.GetSeedTask(req.TaskId)
	if err != nil {
		if task.IsTaskNotFound(err) {
			err = dferrors.Newf(base.Code_CDNTaskNotFound, "failed to get task(%s): %v", req.TaskId, err)
			span.RecordError(err)
			return nil, err
		}
		err = dferrors.Newf(base.Code_CDNError, "failed to get task(%s): %v", req.TaskId, err)
		span.RecordError(err)
		return nil, err
	}
	if seedTask.IsError() {
		err = dferrors.Newf(base.Code_CDNTaskDownloadFail, "task(%s) status is FAIL, cdnStatus: %s", seedTask.ID, seedTask.CdnStatus)
		span.RecordError(err)
		return nil, err
	}
	pieces, err := css.service.GetSeedPieces(req.TaskId)
	if err != nil {
		err = dferrors.Newf(base.Code_CDNError, "failed to get pieces of task(%s) from cdn: %v", seedTask.ID, err)
		span.RecordError(err)
		return nil, err
	}
	pieceInfos := make([]*base.PieceInfo, 0, len(pieces))
	var count uint32 = 0
	for _, piece := range pieces {
		if piece.PieceNum >= req.StartNum && (count < req.Limit || req.Limit <= 0) {
			p := &base.PieceInfo{
				PieceNum:    int32(piece.PieceNum),
				RangeStart:  piece.PieceRange.StartIndex,
				RangeSize:   piece.PieceLen,
				PieceMd5:    piece.PieceMd5,
				PieceOffset: piece.OriginRange.StartIndex,
				PieceStyle:  piece.PieceStyle,
			}
			pieceInfos = append(pieceInfos, p)
			count++
		}
	}
	pp := &base.PiecePacket{
		TaskId:        req.TaskId,
		DstPid:        req.DstPid,
		DstAddr:       fmt.Sprintf("%s:%d", css.config.AdvertiseIP, css.config.DownloadPort),
		PieceInfos:    pieceInfos,
		TotalPiece:    seedTask.TotalPieceCount,
		ContentLength: seedTask.SourceFileLength,
		PieceMd5Sign:  seedTask.PieceMd5Sign,
	}
	span.SetAttributes(constants.AttributePiecePacketResult.String(pp.String()))
	return pp, nil
}

func (css *Server) ListenAndServe() error {
	// Generate GRPC listener
	lis, _, err := rpc.ListenWithPortRange(css.config.AdvertiseIP, css.config.ListenPort, css.config.ListenPort)
	if err != nil {
		return err
	}
	//Started GRPC server
	logger.Infof("====starting grpc server at %s://%s====", lis.Addr().Network(), lis.Addr().String())
	return css.Server.Serve(lis)
}

const (
	gracefulStopTimeout = 10 * time.Second
)

func (css *Server) Shutdown() error {
	defer logger.Infof("====stopped rpc server====")
	stopped := make(chan struct{})
	go func() {
		css.Server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-time.After(gracefulStopTimeout):
		css.Server.Stop()
	case <-stopped:
	}
	return nil
}
