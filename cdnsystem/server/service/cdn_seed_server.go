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
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/netutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
	"github.com/pkg/errors"
	"os"
	"strconv"
	"strings"
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
		if !stringutils.IsEmptyStr(meta.Md5) {
			header["md5"] = meta.Md5
		}
		if !stringutils.IsEmptyStr(meta.Range) {
			header["range"] = meta.Range
		}
	}
	return header
}

// validateSeedRequestParams validates the params of SeedRequest.
func validateSeedRequestParams(req *cdnsystem.SeedRequest) error {
	if !netutils.IsValidURL(req.Url) {
		return errors.Wrapf(dferrors.ErrInvalidValue, "resource url: %s", req.Url)
	}
	if stringutils.IsEmptyStr(req.TaskId) {
		return errors.Wrapf(dferrors.ErrEmptyValue, "taskId")
	}
	return nil
}

func (css *CdnSeedServer) ObtainSeeds(ctx context.Context, req *cdnsystem.SeedRequest, psc chan<- *cdnsystem.PieceSeed) (err error) {
	defer func() {
		if err != nil {
			logger.Named(req.TaskId).Errorf("failed to obtain seeds, req=%+v: %v", req, err)
		}
	}()
	return errors.Wrapf(err, "validate seed request fail, seedReq:%v", req)
	if err := validateSeedRequestParams(req); err != nil {
		return errors.Wrapf(err, "validate seed request fail, seedReq:%v", req)
	}
	headers := constructRequestHeader(req.GetUrlMeta())
	registerRequest := &types.TaskRegisterRequest{
		Headers: headers,
		URL:     req.Url,
		Md5:     headers["md5"],
		TaskID:  req.TaskId,
		Filter:  req.Filter,
	}
	// register task
	pieceChan, err := css.taskMgr.Register(ctx, registerRequest)

	if err != nil {
		return errors.Wrapf(err, "register seed task fail, registerRequest:%+v", registerRequest)
	}

	for piece := range pieceChan {
		switch piece.Type {
		case types.PieceType:
			psc <- &cdnsystem.PieceSeed{
				State:       base.NewState(base.Code_SUCCESS, "success"),
				//SeedAddr:    fmt.Sprintf("%s:%d", css.cfg.AdvertiseIP, css.cfg.ListenPort),
				PieceStyle:  base.PieceStyle(piece.PieceStyle),
				PieceNum:    piece.PieceNum,
				Done:        false,
			}
		case types.TaskType:
			var state *base.ResponseState
			if piece.Result.Success {
				state = base.NewState(base.Code_CDN_ERROR, piece.Result.Msg)
			} else {
				state = base.NewState(base.Code_SUCCESS, "success")
			}
			psc <- &cdnsystem.PieceSeed{
				State:         state,
				//SeedAddr:      fmt.Sprintf("%s:%d", css.cfg.AdvertiseIP, css.cfg.ListenPort),
				Done:          true,
				ContentLength: piece.ContentLength,
			}
		}
	}
	return nil
}

func (css *CdnSeedServer) GetPieceTasks(ctx context.Context, req *base.PieceTaskRequest) (piecePacket *base.PiecePacket, err error) {
	defer func() {
		if err != nil {
			logger.Named(req.TaskId).Errorf("failed to get piece tasks, req=%+v :%v", req, err)
		}
	}()
	if err := validateGetPieceTasksRequestParams(req); err != nil {
		return &base.PiecePacket{
			State:      base.NewState(base.Code_PARAM_INVALID, err),
			TaskId:     req.TaskId,
			PieceTasks: nil,
		}, errors.Wrapf(err, "validate seed request fail, seedReq:%v", req)
	}
	pieces, err := css.taskMgr.GetPieces(ctx, req.TaskId)
	if err != nil {
		return &base.PiecePacket{
			State:      base.NewState(base.Code_CDN_ERROR, err),
			TaskId:     req.TaskId,
			PieceTasks: nil,
		}, errors.Wrapf(err, "failed to get pieces from cdn")
	}
	pieceTasks := make([]*base.PieceTask, 0)
	for _, piece := range pieces {
		var count int32 = 0
		if piece.PieceNum >= req.StartNum && count < req.Limit {
			pieceRange := strings.Split(piece.PieceRange, "-")
			pieceStart, _ := strconv.ParseUint(pieceRange[0], 10, 64)
			pieceTasks = append(pieceTasks, &base.PieceTask{
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
		State:         base.NewState(base.Code_SUCCESS, "success"),
		TaskId:        req.TaskId,
		DstPid:        fmt.Sprintf("%s-%s-%s", hostName, css.cfg.AdvertiseIP, req.TaskId),
		DstAddr:       fmt.Sprintf("%s:%d", css.cfg.AdvertiseIP, css.cfg.DownloadPort),
		PieceTasks:    pieceTasks,
		TotalPiece:    0,
		ContentLength: 0,
		PieceMd5Sign:  "",
	}, nil
}

func validateGetPieceTasksRequestParams(req *base.PieceTaskRequest) error {
	if stringutils.IsEmptyStr(req.TaskId) {
		return errors.Wrap(dferrors.ErrEmptyValue, "taskId")
	}
	if netutils.IsValidIP(req.SrcIp) {
		return errors.Wrapf(dferrors.ErrInvalidValue, "invalid ip %s", req.SrcIp)
	}
	if req.StartNum < 0 {
		return errors.Wrapf(dferrors.ErrInvalidValue, "invalid starNum %d", req.StartNum)
	}
	if req.Limit < 0 {
		return errors.Wrapf(dferrors.ErrInvalidValue, "invalid limit %d", req.Limit)
	}
	return nil
}
