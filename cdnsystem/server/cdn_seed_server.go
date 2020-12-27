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

package server

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/netutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
	"github.com/pkg/errors"
	"time"
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
	}, nil
}

func constructRequestHeader(meta *base.UrlMeta) map[string]string {
	header := make(map[string]string)
	if !stringutils.IsEmptyStr(meta.Md5) {
		header["md5"] = meta.Md5
	}
	if !stringutils.IsEmptyStr(meta.Range) {
		header["range"] = meta.Range
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
		Md5:     req.UrlMeta.Md5,
		TaskID:  req.GetTaskId(),
	}

	pieceCh, err := ss.taskMgr.Register(ctx, registerRequest)

	if err != nil {
		return errors.Wrapf(err, "register seed task fail, registerRequest:%v", registerRequest)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case piece, ok := <-pieceCh:
			if ok {
				psc <- &cdnsystem.PieceSeed{
					State:       base.NewState(base.Code_SUCCESS, "success"),
					SeedAddr:    fmt.Sprintf(ss.cfg.AdvertiseIP, ":", ss.cfg.ListenPort),
					PieceStyle:  base.PieceStyle(piece.PieceStyle),
					PieceNum:    piece.PieceNum,
					PieceMd5:    piece.PieceMd5,
					PieceRange:  piece.PieceRange,
					PieceOffset: piece.PieceOffset,
					Done:        false,
				}
			} else {
				break
			}
		default:
			time.Sleep(3 * time.Second)
		}
	}
	seedTask := ss.taskMgr.Get(ctx, req.GetTaskId())
	seedTask.CdnStatus
	psc <- &cdnsystem.PieceSeed{
		State:    base.NewState(base.Code_SUCCESS, "success"),
		SeedAddr: fmt.Sprintf(ss.cfg.AdvertiseIP, ":", ss.cfg.ListenPort),
		Done:     true,
		ContentLength: 324,
		TotalTraffic: 100,
	}
	return
	var i = 5
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if i < 0 {
				psc <- &cdnsystem.PieceSeed{
					State:         base.NewState(base.Code_SUCCESS, "success"),
					SeedAddr:      "localhost:12345",
					Done:          true,
					ContentLength: 100,
					TotalTraffic:  100,
				}
				return
			}
			psc <- &cdnsystem.PieceSeed{State: base.NewState(base.Code_SUCCESS, "success"), SeedAddr: "localhost:12345"}
			time.Sleep(1 * time.Second)
			i--
		}
	}
}
