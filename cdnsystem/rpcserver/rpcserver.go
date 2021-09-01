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
	"strings"

	"d7y.io/dragonfly/v2/cdnsystem/cdnutil"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	cdnerrors "d7y.io/dragonfly/v2/cdnsystem/errors"
	"d7y.io/dragonfly/v2/cdnsystem/supervisor"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var tracer trace.Tracer

func init() {
	tracer = otel.Tracer("cdn-server")
}

type options struct {
	tracer trace.Tracer
}

type Option func(*options)

func WithTracer(tracer trace.Tracer) Option {
	return func(o *options) {
		o.tracer = tracer
	}
}

// CdnSeedServer is used to implement cdnsystem.SeederServer.
type CdnSeedServer struct {
	taskMgr supervisor.SeedTaskMgr
	cfg     *config.Config
}

// NewCdnSeedServer returns a new Manager Object.
func NewCdnSeedServer(cfg *config.Config, taskMgr supervisor.SeedTaskMgr) (*CdnSeedServer, error) {
	return &CdnSeedServer{
		taskMgr: taskMgr,
		cfg:     cfg,
	}, nil
}

func constructRegisterRequest(req *cdnsystem.SeedRequest) (*types.TaskRegisterRequest, error) {
	if err := checkSeedRequestParams(req); err != nil {
		return nil, err
	}
	meta := req.UrlMeta
	header := make(map[string]string)
	if meta != nil {
		if !stringutils.IsBlank(meta.Digest) {
			digest := digestutils.Parse(meta.Digest)
			if _, ok := digestutils.Algorithms[digest[0]]; !ok {
				return nil, errors.Errorf("unsupported digest algorithm")
			}
			header["digest"] = meta.Digest
		}
		if !stringutils.IsBlank(meta.Range) {
			header["range"] = meta.Range
		}
		for k, v := range meta.Header {
			header[k] = v
		}
	}
	return &types.TaskRegisterRequest{
		Header: header,
		URL:    req.Url,
		Digest: header["digest"],
		TaskID: req.TaskId,
		Filter: strings.Split(req.UrlMeta.Filter, "&"),
	}, nil
}

// checkSeedRequestParams check the params of SeedRequest.
func checkSeedRequestParams(req *cdnsystem.SeedRequest) error {
	if !urlutils.IsValidURL(req.Url) {
		return errors.Errorf("resource url: %s is invalid", req.Url)
	}
	if stringutils.IsBlank(req.TaskId) {
		return errors.New("taskId is empty")
	}
	return nil
}

func (css *CdnSeedServer) ObtainSeeds(ctx context.Context, req *cdnsystem.SeedRequest, psc chan<- *cdnsystem.PieceSeed) (err error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanObtainSeeds, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	span.SetAttributes(config.AttributeObtainSeedsRequest.String(req.String()))
	span.SetAttributes(config.AttributeTaskID.String(req.TaskId))
	defer func() {
		if r := recover(); r != nil {
			err = dferrors.Newf(dfcodes.UnknownError, "encounter an panic: %v", r)
			span.RecordError(err)
			logger.WithTaskID(req.TaskId).Errorf("failed to obtain task(%s) seeds, req=%+v: %v", req.TaskId, req, err)
		}

		if err != nil {
			span.RecordError(err)
			logger.WithTaskID(req.TaskId).Errorf("failed to obtain task(%s) seeds, req=%+v: %v", req.TaskId, req, err)
		}
	}()
	logger.Infof("obtain seeds request: %+v", req)

	registerRequest, err := constructRegisterRequest(req)
	if err != nil {
		err = dferrors.Newf(dfcodes.BadRequest, "bad seed request for task(%s): %v", req.TaskId, err)
		span.RecordError(err)
		return err
	}
	// register task
	pieceChan, err := css.taskMgr.Register(ctx, registerRequest)
	if err != nil {
		if cdnerrors.IsResourcesLacked(err) {
			err = dferrors.Newf(dfcodes.ResourceLacked, "resources lacked for task(%s): %v", req.TaskId, err)
			span.RecordError(err)
			return err
		}
		err = dferrors.Newf(dfcodes.CdnTaskRegistryFail, "failed to register seed task(%s): %v", req.TaskId, err)
		span.RecordError(err)
		return err
	}
	task, err := css.taskMgr.Get(req.TaskId)
	if err != nil {
		err = dferrors.Newf(dfcodes.CdnError, "failed to get task(%s): %v", req.TaskId, err)
		span.RecordError(err)
		return err
	}
	peerID := cdnutil.GenCDNPeerID(req.TaskId)
	for piece := range pieceChan {
		psc <- &cdnsystem.PieceSeed{
			PeerId:   peerID,
			HostUuid: idgen.CDNUUID(iputils.HostName, int32(css.cfg.ListenPort)),
			PieceInfo: &base.PieceInfo{
				PieceNum:    piece.PieceNum,
				RangeStart:  piece.PieceRange.StartIndex,
				RangeSize:   piece.PieceLen,
				PieceMd5:    piece.PieceMd5,
				PieceOffset: piece.OriginRange.StartIndex,
				PieceStyle:  base.PieceStyle(piece.PieceStyle),
			},
			Done:          false,
			ContentLength: task.SourceFileLength,
		}
	}
	if task.CdnStatus != types.TaskInfoCdnStatusSuccess {
		err = dferrors.Newf(dfcodes.CdnTaskDownloadFail, "task(%s) status error , status: %s", req.TaskId, task.CdnStatus)
		span.RecordError(err)
		return err
	}
	psc <- &cdnsystem.PieceSeed{
		PeerId:          peerID,
		HostUuid:        idgen.CDNUUID(iputils.HostName, int32(css.cfg.ListenPort)),
		Done:            true,
		ContentLength:   task.SourceFileLength,
		TotalPieceCount: task.PieceTotal,
	}
	return nil
}

func (css *CdnSeedServer) GetPieceTasks(ctx context.Context, req *base.PieceTaskRequest) (piecePacket *base.PiecePacket, err error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanGetPieceTasks, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	span.SetAttributes(config.AttributeGetPieceTasksRequest.String(req.String()))
	span.SetAttributes(config.AttributeTaskID.String(req.TaskId))
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("encounter an panic: %v", r)
			span.RecordError(err)
			logger.WithTaskID(req.TaskId).Errorf("failed to get piece tasks, req=%+v: %v", req, err)
		}
		if err != nil {
			span.RecordError(err)
			logger.WithTaskID(req.TaskId).Errorf("failed to get piece tasks, req=%+v: %v", req, err)
		}
	}()
	logger.Infof("get piece tasks: %+v", req)
	if err := checkPieceTasksRequestParams(req); err != nil {
		err = dferrors.Newf(dfcodes.BadRequest, "failed to validate seed request for task(%s): %v", req.TaskId, err)
		span.RecordError(err)
		return nil, err
	}
	task, err := css.taskMgr.Get(req.TaskId)
	if err != nil {
		if cdnerrors.IsDataNotFound(err) {
			err = dferrors.Newf(dfcodes.CdnTaskNotFound, "failed to get task(%s) from cdn: %v", req.TaskId, err)
			span.RecordError(err)
			return nil, err
		}
		err = dferrors.Newf(dfcodes.CdnError, "failed to get task(%s) from cdn: %v", req.TaskId, err)
		span.RecordError(err)
		return nil, err
	}
	if task.IsError() {
		err = dferrors.Newf(dfcodes.CdnTaskDownloadFail, "fail to download task(%s), cdnStatus: %s", task.TaskID, task.CdnStatus)
		span.RecordError(err)
		return nil, err
	}
	pieces, err := css.taskMgr.GetPieces(ctx, req.TaskId)
	if err != nil {
		err = dferrors.Newf(dfcodes.CdnError, "failed to get pieces of task(%s) from cdn: %v", task.TaskID, err)
		span.RecordError(err)
		return nil, err
	}
	pieceInfos := make([]*base.PieceInfo, 0)
	var count int32 = 0
	for _, piece := range pieces {
		if piece.PieceNum >= req.StartNum && (count < req.Limit || req.Limit == 0) {
			p := &base.PieceInfo{
				PieceNum:    piece.PieceNum,
				RangeStart:  piece.PieceRange.StartIndex,
				RangeSize:   piece.PieceLen,
				PieceMd5:    piece.PieceMd5,
				PieceOffset: piece.OriginRange.StartIndex,
				PieceStyle:  base.PieceStyle(piece.PieceStyle),
			}
			pieceInfos = append(pieceInfos, p)
			count++
		}
	}
	pp := &base.PiecePacket{
		TaskId:        req.TaskId,
		DstPid:        fmt.Sprintf("%s-%s_%s", iputils.HostName, req.TaskId, "CDN"),
		DstAddr:       fmt.Sprintf("%s:%d", css.cfg.AdvertiseIP, css.cfg.DownloadPort),
		PieceInfos:    pieceInfos,
		TotalPiece:    task.PieceTotal,
		ContentLength: task.SourceFileLength,
		PieceMd5Sign:  task.PieceMd5Sign,
	}
	span.SetAttributes(config.AttributePiecePacketResult.String(pp.String()))
	return pp, nil
}

func checkPieceTasksRequestParams(req *base.PieceTaskRequest) error {
	if stringutils.IsBlank(req.TaskId) {
		return errors.Wrap(cdnerrors.ErrInvalidValue, "taskId is nil")
	}
	if stringutils.IsBlank(req.SrcPid) {
		return errors.Wrapf(cdnerrors.ErrInvalidValue, "src peer id is nil")
	}
	if req.StartNum < 0 {
		return errors.Wrapf(cdnerrors.ErrInvalidValue, "invalid starNum %d", req.StartNum)
	}
	if req.Limit < 0 {
		return errors.Wrapf(cdnerrors.ErrInvalidValue, "invalid limit %d", req.Limit)
	}
	return nil
}
