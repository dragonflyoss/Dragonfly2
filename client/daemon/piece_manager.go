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

package daemon

import (
	"context"
	"io"

	"github.com/dragonflyoss/Dragonfly2/client/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
	"github.com/sirupsen/logrus"
)

type PieceManager interface {
	PieceDownloader
	PullPieces(peerTask PeerTask, piecePackage *scheduler.PiecePackage)
}

type pieceManager struct {
	storageManager  StorageDriver
	pieceDownloader PieceDownloader
}

func NewPieceManager(s StorageDriver, opts ...func(*pieceManager) error) (PieceManager, error) {
	pm := &pieceManager{
		storageManager: s,
	}
	for _, opt := range opts {
		if err := opt(pm); err != nil {
			return nil, err
		}
	}

	// set default value
	if pm.pieceDownloader == nil {
		pm.pieceDownloader, _ = NewPieceDownloader()
	}
	return pm, nil
}

func WithPieceDownloader(d PieceDownloader) func(*pieceManager) error {
	return func(pm *pieceManager) error {
		pm.pieceDownloader = d
		return nil
	}
}

func (pm *pieceManager) PullPieces(peerTask PeerTask, piecePackage *scheduler.PiecePackage) {
	for _, p := range piecePackage.PieceTasks {
		go func(pieceTask *scheduler.PiecePackage_PieceTask) {
			// TODO store piece info in memory
			logrus.Infof("peer task %s loaded piece task %s", peerTask.GetPeerID(), pieceTask.PieceRange)

			// download piece from other peers
			rc, err := pm.DownloadPiece(&DownloadPieceRequest{
				TaskID:                 peerTask.GetTaskID(),
				PiecePackage_PieceTask: pieceTask,
			})
			if err != nil {
				logrus.Errorf("download piece failed, piece task: %#v", pieceTask)
				return
			}
			defer rc.Close()
			err = pm.storageManager.PutPiece(context.Background(), &PutPieceRequest{
				PieceMetaData: PieceMetaData{
					TaskID: peerTask.GetTaskID(),
					Num:    pieceTask.PieceNum,
					Md5:    pieceTask.PieceMd5,
					Offset: pieceTask.PieceOffset,
					Range:  util.MustParseRange(pieceTask.PieceRange, int64(peerTask.GetContentLength())),
				},
				Reader: rc,
			})
			if err != nil {
				logrus.Errorf("download piece failed, piece task: %#v", pieceTask)
				// TODO push failed piece result
				return
			}
			// TODO calculate time cost
			cost := uint32(0)
			err = peerTask.PushPieceResult(
				&scheduler.PieceResult{
					TaskId:     peerTask.GetTaskID(),
					SrcPid:     pieceTask.SrcPid,
					DstPid:     pieceTask.DstPid,
					PieceNum:   pieceTask.PieceNum,
					PieceRange: pieceTask.PieceRange,
					Success:    true,
					ErrorCode:  base.Code_SUCCESS,
					Cost:       cost,
				})
			if err != nil {
				logrus.Errorf("report piece task error: %v", err)
			}
		}(p)
	}
}

func (pm *pieceManager) DownloadPiece(req *DownloadPieceRequest) (io.ReadCloser, error) {
	return pm.pieceDownloader.DownloadPiece(req)
}
