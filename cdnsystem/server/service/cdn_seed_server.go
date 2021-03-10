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
	"os"
	"strconv"
	"strings"

	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
)

// CdnSeedServer is used to implement cdnsystem.SeederServer.
type CdnSeedServer struct {
	taskMgr mgr.SeedTaskMgr
	cfg     *config.Config
}

// NewManager returns a new Manager Object.
func NewCdnSeedServer(cfg *config.Config, taskMgr mgr.SeedTaskMgr) (*CdnSeedServer, error) {
	return &CdnSeedServer{
		taskMgr: taskMgr,
		cfg:     cfg,
	}, nil
}

func constructRequestHeader(meta *base.UrlMeta) map[string]string {
	header := make(map[string]string)
	if meta != nil {
		if !stringutils.IsBlank(meta.Md5) {
			header["md5"] = meta.Md5
		}
		if !stringutils.IsBlank(meta.Range) {
			header["range"] = meta.Range
		}
		for k, v := range meta.Header {
			header[k] = v
		}
	}
	return header
}

// validateSeedRequestParams validates the params of SeedRequest.
func validateSeedRequestParams(req *cdnsystem.SeedRequest) error {
	if !urlutils.IsValidURL(req.Url) {
		return errors.Errorf("resource url:%s is invalid", req.Url)
	}
	if stringutils.IsBlank(req.TaskId) {
		return errors.New("taskId is empty")
	}
	return nil
}

func (css *CdnSeedServer) ObtainSeeds(ctx context.Context, req *cdnsystem.SeedRequest,
	psc chan<- *cdnsystem.PieceSeed) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.WithTaskID(req.TaskId).Errorf("failed to obtain seeds: %v", r)
		}

		if err != nil {
			logger.WithTaskID(req.TaskId).Errorf("failed to obtain seeds: %v", err)
		}
	}()
	if err := validateSeedRequestParams(req); err != nil {
		return dferrors.Newf(dfcodes.BadRequest, "validate seed request fail: %v", err)
	}
	headers := constructRequestHeader(req.GetUrlMeta())
	registerRequest := &types.TaskRegisterRequest{
		Headers: headers,
		URL:     req.Url,
		Md5:     headers["md5"],
		TaskID:  req.TaskId,
		Filter:  strings.Split(req.Filter, "&"),
	}
	// register task
	pieceChan, err := css.taskMgr.Register(ctx, registerRequest)

	if err != nil {
		return dferrors.Newf(dfcodes.CdnTaskRegistryFail, "register seed task fail, registerRequest:%+v:%v",
			registerRequest, err)
	}
	peerId := fmt.Sprintf("%s-%s_%s", iputils.HostName, req.TaskId, "CDN")
	for piece := range pieceChan {
		pieceRange := strings.Split(piece.PieceRange, "-")
		pieceStart, _ := strconv.ParseUint(pieceRange[0], 10, 64)
		switch piece.Type {
		case types.PieceType:
			psc <- &cdnsystem.PieceSeed{
				State:      common.NewState(dfcodes.Success, "success"),
				PeerId:     peerId,
				SeederName: iputils.HostName,
				PieceInfo: &base.PieceInfo{
					PieceNum:    piece.PieceNum,
					RangeStart:  pieceStart,
					RangeSize:   piece.PieceLen,
					PieceMd5:    piece.PieceMd5,
					PieceOffset: piece.PieceOffset,
					PieceStyle:  base.PieceStyle(piece.PieceStyle),
				},
				Done:          false,
				ContentLength: 0,
			}
		case types.TaskType:
			var state *base.ResponseState
			if !piece.Result.Success {
				state = common.NewState(dfcodes.CdnTaskStatusError, piece.Result.Msg)
			} else {
				state = common.NewState(dfcodes.Success, "success")
			}
			psc <- &cdnsystem.PieceSeed{
				State:         state,
				PeerId:        peerId,
				SeederName:    iputils.HostName,
				Done:          true,
				ContentLength: piece.ContentLength,
			}
		}
	}
	return nil
}

func (css *CdnSeedServer) GetPieceTasks(ctx context.Context, req *base.PieceTaskRequest) (piecePacket *base.PiecePacket, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.WithTaskID(req.TaskId).Errorf("failed to get piece tasks, req=%+v: %v", req, r)
		}
		if err != nil {
			logger.WithTaskID(req.TaskId).Errorf("failed to get piece tasks, req=%+v :%v", req, err)
		}
	}()
	if err := validateGetPieceTasksRequestParams(req); err != nil {
		return nil, dferrors.Newf(dfcodes.BadRequest, "validate seed request fail, seedReq: %v, err: %v", req, err)
	}
	task, err := css.taskMgr.Get(ctx, req.TaskId)
	logger.Debugf("task:%+v", task)
	if err != nil {
		if cdnerrors.IsDataNotFound(err) {
			return nil, dferrors.Newf(dfcodes.CdnTaskNotFound, "failed to get task from cdn: %v", err)
		}
		return nil, dferrors.Newf(dfcodes.CdnError, "failed to get task from cdn: %v", err)
	}
	pieces, err := css.taskMgr.GetPieces(ctx, req.TaskId)
	if err != nil {
		return nil, dferrors.Newf(dfcodes.CdnError, "failed to get pieces from cdn: %v", err)
	}
	pieceInfos := make([]*base.PieceInfo, 0)
	var count int32 = 0
	for _, piece := range pieces {
		if piece.PieceNum >= req.StartNum && count < req.Limit {
			pieceRange := strings.Split(piece.PieceRange, "-")
			pieceStart, _ := strconv.ParseUint(pieceRange[0], 10, 64)
			pieceInfos = append(pieceInfos, &base.PieceInfo{
				PieceNum:    piece.PieceNum,
				RangeStart:  pieceStart,
				RangeSize:   piece.PieceLen,
				PieceMd5:    piece.PieceMd5,
				PieceOffset: piece.PieceOffset,
				PieceStyle:  base.PieceStyle(piece.PieceStyle),
			})
			count++
		}
	}
	hostName, _ := os.Hostname()

	return &base.PiecePacket{
		State:         common.NewState(dfcodes.Success, "success"),
		TaskId:        req.TaskId,
		DstPid:        fmt.Sprintf("%s-%s_%s", hostName, req.TaskId, "CDN"),
		DstAddr:       fmt.Sprintf("%s:%d", css.cfg.AdvertiseIP, css.cfg.DownloadPort),
		PieceInfos:    pieceInfos,
		TotalPiece:    task.PieceTotal,
		ContentLength: task.SourceFileLength,
		PieceMd5Sign:  task.PieceMd5Sign,
	}, nil
}

func validateGetPieceTasksRequestParams(req *base.PieceTaskRequest) error {
	if stringutils.IsBlank(req.TaskId) {
		return errors.Wrap(dferrors.ErrEmptyValue, "taskId")
	}
	if !iputils.IsIPv4(req.SrcIp) {
		return errors.Wrapf(dferrors.ErrInvalidArgument, "invalid ip %s", req.SrcIp)
	}
	if req.StartNum < 0 {
		return errors.Wrapf(dferrors.ErrInvalidArgument, "invalid starNum %d", req.StartNum)
	}
	if req.Limit < 0 {
		return errors.Wrapf(dferrors.ErrInvalidArgument, "invalid limit %d", req.Limit)
	}
	return nil
}
