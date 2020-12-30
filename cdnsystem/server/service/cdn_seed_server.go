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
		return errors.Wrapf(dferrors.ErrEmptyValue, "taskId: %s", req.TaskId)
	}
	return nil
}

func (ss *CdnSeedServer) ObtainSeeds(ctx context.Context, req *cdnsystem.SeedRequest, psc chan<- *cdnsystem.PieceSeed) (err error) {
	if err := validateSeedRequestParams(req); err != nil {
		return errors.Wrapf(err, "validate seed request fail, seedReq:%v", req)
	}
	headers := constructRequestHeader(req.GetUrlMeta())
	registerRequest := &types.TaskRegisterRequest{
		Headers: headers,
		URL:     req.GetUrl(),
		Md5:     headers["md5"],
		TaskID:  req.GetTaskId(),
	}

	pieceCh, err := ss.taskMgr.Register(ctx, registerRequest)

	if err != nil {
		logger.Named(req.TaskId).Errorf("register seed task fail, registerRequest=%v: %v", err)
		return errors.Wrapf(err, "register seed task fail, registerRequest:%v", registerRequest)
	}

	for piece := range pieceCh {

		switch piece.Type {
		case types.PieceType:
			psc <- &cdnsystem.PieceSeed{
				State:       base.NewState(base.Code_SUCCESS, "success"),
				SeedAddr:    fmt.Sprintf("%s:%d", ss.cfg.AdvertiseIP, ss.cfg.ListenPort),
				PieceStyle:  base.PieceStyle(piece.PieceStyle),
				PieceNum:    piece.PieceNum,
				PieceMd5:    piece.PieceMd5,
				PieceRange:  piece.PieceRange,
				PieceOffset: piece.PieceOffset,
				Done:        false,
			}
		case types.TaskType:
			psc <- &cdnsystem.PieceSeed{
				State:         base.NewState(base.Code_SUCCESS, "success"),
				SeedAddr:      fmt.Sprintf("%s:%d", ss.cfg.AdvertiseIP, ss.cfg.ListenPort),
				Done:          true,
				ContentLength: piece.ContentLength,
				TotalTraffic:  piece.BackSourceLength,
			}
		}
	}
	return nil
}
