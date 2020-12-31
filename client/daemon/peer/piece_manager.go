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

package peer

import (
	"context"
	"fmt"
	"io"
	"time"

	"golang.org/x/time/rate"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	"github.com/dragonflyoss/Dragonfly2/client/util"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
)

type PieceManager interface {
	PieceDownloader
	PullPieces(peerTask PeerTask, piecePacket *base.PiecePacket)
}

type pieceManager struct {
	*rate.Limiter
	storageManager  storage.TaskStorageDriver
	pieceDownloader PieceDownloader
}

func NewPieceManager(s storage.TaskStorageDriver, opts ...func(*pieceManager)) (PieceManager, error) {
	pm := &pieceManager{
		storageManager: s,
	}
	for _, opt := range opts {
		opt(pm)
	}

	// set default value
	if pm.pieceDownloader == nil {
		pm.pieceDownloader, _ = NewPieceDownloader()
	}
	return pm, nil
}

func WithPieceDownloader(d PieceDownloader) func(*pieceManager) {
	return func(pm *pieceManager) {
		pm.pieceDownloader = d
	}
}

// WithLimiter sets upload rate limiter, the burst size must big than piece size
func WithLimiter(limiter *rate.Limiter) func(*pieceManager) {
	return func(manager *pieceManager) {
		manager.Limiter = limiter
	}
}

func (pm *pieceManager) PullPieces(pt PeerTask, piecePacket *base.PiecePacket) {
	for _, p := range piecePacket.PieceTasks {
		logger.Debugf("peer manager receive piece task, "+
			"peer id: %s, piece num: %d, range start: %d, range size: %d",
			pt.GetPeerID(), p.PieceNum, p.RangeStart, p.RangeSize)
		go pm.pullPiece(pt, p)
	}
}

func (pm *pieceManager) pullPiece(pt PeerTask, pieceTask *base.PieceTask) {
	var (
		success bool
		start   = time.Now().UnixNano()
		end     int64
	)
	defer func() {
		if success {
			pm.pushSuccessResult(pt, pieceTask, start, end)
		} else {
			pm.pushFailResult(pt, pieceTask, start, end)
		}
	}()

	// 1. download piece from other peers
	if pm.Limiter != nil {
		if err := pm.Limiter.WaitN(context.Background(), int(pieceTask.RangeSize)); err != nil {
			logger.Errorf("require rate limit access error: %s", err)
			return
		}
	}
	rc, err := pm.DownloadPiece(&DownloadPieceRequest{
		TaskID:    pt.GetTaskID(),
		PieceTask: pieceTask,
	})
	if err != nil {
		logger.Errorf("download piece failed, piece num: %d, error: %s", pieceTask.PieceNum, err)
		return
	}
	end = time.Now().UnixNano()
	defer rc.Close()

	// 2. save to storage
	err = pm.storageManager.WritePiece(context.Background(), &storage.WritePieceRequest{
		PeerTaskMetaData: storage.PeerTaskMetaData{
			PeerID: pt.GetPeerID(),
			TaskID: pt.GetTaskID(),
		},
		PieceMetaData: storage.PieceMetaData{
			Num:    pieceTask.PieceNum,
			Md5:    pieceTask.PieceMd5,
			Offset: pieceTask.PieceOffset,
			Range: util.Range{
				Start:  int64(pieceTask.RangeStart),
				Length: int64(pieceTask.RangeSize),
			},
		},
		Reader: rc,
	})
	if err != nil {
		logger.Errorf("put piece to storage failed, piece num: %d, error: %s", pieceTask.PieceNum, err)
		return
	}
	success = true
}

func (pm *pieceManager) pushSuccessResult(peerTask PeerTask, pieceTask *base.PieceTask, start int64, end int64) {
	err := peerTask.PushPieceResult(
		&scheduler.PieceResult{
			PieceNum: pieceTask.PieceNum,
			PieceRange: fmt.Sprintf("bytes=%d-%d", pieceTask.RangeStart,
				pieceTask.RangeStart+uint64(pieceTask.RangeSize)-1),

			TaskId:    peerTask.GetTaskID(),
			SrcPid:    pieceTask.SrcPid,
			DstPid:    pieceTask.DstPid,
			Success:   true,
			Code:      base.Code_SUCCESS,
			BeginTime: uint64(start),
			EndTime:   uint64(end),
		})
	if err != nil {
		logger.Errorf("report piece task error: %v", err)
	}
}

func (pm *pieceManager) pushFailResult(peerTask PeerTask, pieceTask *base.PieceTask, start int64, end int64) {
	err := peerTask.PushPieceResult(
		&scheduler.PieceResult{
			PieceNum: pieceTask.PieceNum,
			PieceRange: fmt.Sprintf("bytes=%d-%d", pieceTask.RangeStart,
				pieceTask.RangeStart+uint64(pieceTask.RangeSize)-1),

			TaskId:    peerTask.GetTaskID(),
			SrcPid:    pieceTask.SrcPid,
			DstPid:    pieceTask.DstPid,
			Success:   false,
			Code:      base.Code_CLIENT_ERROR,
			BeginTime: uint64(start),
			EndTime:   uint64(end),
		})
	if err != nil {
		logger.Errorf("report piece task error: %v", err)
	}
}

func (pm *pieceManager) DownloadPiece(req *DownloadPieceRequest) (io.ReadCloser, error) {
	return pm.pieceDownloader.DownloadPiece(req)
}
