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
	"io"
	"math"
	"time"

	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/source"
	_ "d7y.io/dragonfly/v2/cdnsystem/source/httpprotocol"
	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type PieceManager interface {
	DownloadSource(ctx context.Context, pt PeerTask, url string, headers map[string]string) error
	DownloadPieces(peerTask PeerTask, piecePacket *base.PiecePacket)
	ReadPiece(ctx context.Context, req *storage.ReadPieceRequest) (io.Reader, io.Closer, error)
}

type pieceManager struct {
	*rate.Limiter
	storageManager   storage.TaskStorageDriver
	pieceDownloader  PieceDownloader
	resourceClient   source.ResourceClient
	computePieceSize func(length int64) int32
}

func NewPieceManager(s storage.TaskStorageDriver, opts ...func(*pieceManager)) (PieceManager, error) {
	resourceClient, err := source.NewSourceClient()
	if err != nil {
		return nil, err
	}
	pm := &pieceManager{
		storageManager:   s,
		resourceClient:   resourceClient,
		computePieceSize: computePieceSize,
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

func (pm *pieceManager) DownloadPieces(pt PeerTask, piecePacket *base.PiecePacket) {
	for _, piece := range piecePacket.PieceInfos {
		logger.Debugf("peer manager receive piece task, "+
			"peer id: %s, piece num: %d, range start: %d, range size: %d",
			pt.GetPeerID(), piece.PieceNum, piece.RangeStart, piece.RangeSize)
		go pm.downloadPiece(pt, piecePacket.DstPid, piecePacket.DstAddr, piece)
	}
}

func (pm *pieceManager) downloadPiece(pt PeerTask, dstPid, dstAddr string, pieceTask *base.PieceInfo) {
	var (
		success bool
		start   = time.Now().UnixNano()
		end     int64
	)
	defer func() {
		if success {
			pm.pushSuccessResult(pt, dstPid, pieceTask, start, end)
		} else {
			pm.pushFailResult(pt, dstPid, pieceTask, start, end)
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
		TaskID:  pt.GetTaskID(),
		DstPid:  dstPid,
		DstAddr: dstAddr,
		piece:   pieceTask,
	})
	if err != nil {
		logger.Errorf("download piece failed, piece num: %d, error: %s", pieceTask.PieceNum, err)
		return
	}
	end = time.Now().UnixNano()
	defer rc.Close()

	// 2. save to storage
	n, err := pm.storageManager.WritePiece(context.Background(), &storage.WritePieceRequest{
		PeerTaskMetaData: storage.PeerTaskMetaData{
			PeerID: pt.GetPeerID(),
			TaskID: pt.GetTaskID(),
		},
		PieceMetaData: storage.PieceMetaData{
			Num:    pieceTask.PieceNum,
			Md5:    pieceTask.PieceMd5,
			Offset: pieceTask.PieceOffset,
			Range: clientutil.Range{
				Start:  int64(pieceTask.RangeStart),
				Length: int64(pieceTask.RangeSize),
			},
		},
		Reader: rc,
	})
	pt.AddTraffic(n)
	if err != nil {
		logger.Errorf("put piece to storage failed, piece num: %d, wrote: %d, error: %s",
			pieceTask.PieceNum, n, err)
		return
	}
	success = true
}

func (pm *pieceManager) pushSuccessResult(peerTask PeerTask, dstPid string, piece *base.PieceInfo, start int64, end int64) {
	err := peerTask.ReportPieceResult(
		piece,
		&scheduler.PieceResult{
			TaskId:        peerTask.GetTaskID(),
			SrcPid:        peerTask.GetPeerID(),
			DstPid:        dstPid,
			PieceNum:      piece.PieceNum,
			BeginTime:     uint64(start),
			EndTime:       uint64(end),
			Success:       true,
			Code:          dfcodes.Success,
			HostLoad:      nil, // TODO(jim): update host load
			FinishedCount: 0,   // update by peer task
		})
	if err != nil {
		logger.Errorf("report piece task error: %v", err)
	}
}

func (pm *pieceManager) pushFailResult(peerTask PeerTask, dstPid string, piece *base.PieceInfo, start int64, end int64) {
	err := peerTask.ReportPieceResult(
		piece,
		&scheduler.PieceResult{
			TaskId:        peerTask.GetTaskID(),
			SrcPid:        peerTask.GetPeerID(),
			DstPid:        dstPid,
			PieceNum:      piece.PieceNum,
			BeginTime:     uint64(start),
			EndTime:       uint64(end),
			Success:       false,
			Code:          dfcodes.UnknownError,
			HostLoad:      nil,
			FinishedCount: 0,
		})
	if err != nil {
		logger.Errorf("report piece task error: %v", err)
	}
}

func (pm *pieceManager) DownloadPiece(req *DownloadPieceRequest) (io.ReadCloser, error) {
	return pm.pieceDownloader.DownloadPiece(req)
}

func (pm *pieceManager) ReadPiece(ctx context.Context, req *storage.ReadPieceRequest) (io.Reader, io.Closer, error) {
	return pm.storageManager.ReadPiece(ctx, req)
}

func (pm *pieceManager) processPieceFromSource(pt PeerTask,
	reader io.Reader, contentLength int64, pieceNum int32, pieceOffset uint64, pieceSize int32) (int64, error) {
	var (
		success bool
		start   = time.Now().UnixNano()
		end     int64
	)

	var (
		size          = pieceSize
		unknownLength = contentLength == - 1
	)

	defer func() {
		if success {
			pm.pushSuccessResult(pt, pt.GetPeerID(),
				&base.PieceInfo{
					PieceNum:    pieceNum,
					RangeStart:  pieceOffset,
					RangeSize:   size,
					PieceMd5:    "",
					PieceOffset: pieceOffset,
					PieceStyle:  0,
				}, start, end)
		} else {
			pm.pushFailResult(pt, pt.GetPeerID(),
				&base.PieceInfo{
					PieceNum:    pieceNum,
					RangeStart:  pieceOffset,
					RangeSize:   size,
					PieceMd5:    "",
					PieceOffset: pieceOffset,
					PieceStyle:  0,
				}, start, end)
		}
	}()

	if pm.Limiter != nil {
		if err := pm.Limiter.WaitN(context.Background(), int(size)); err != nil {
			logger.Errorf("require rate limit access error: %s", err)
			return 0, err
		}
	}
	pieceReader := io.LimitReader(reader, int64(size))
	n, err := pm.storageManager.WritePiece(
		context.Background(),
		&storage.WritePieceRequest{
			UnknownLength: unknownLength,
			PeerTaskMetaData: storage.PeerTaskMetaData{
				PeerID: pt.GetPeerID(),
				TaskID: pt.GetTaskID(),
			},
			PieceMetaData: storage.PieceMetaData{
				Num:    pieceNum,
				Md5:    "",
				Offset: pieceOffset,
				Range: clientutil.Range{
					Start:  int64(pieceOffset),
					Length: int64(size),
				},
			},
			Reader: pieceReader,
		})
	if n != int64(size) {
		size = int32(n)
	}
	end = time.Now().UnixNano()
	pt.AddTraffic(n)
	if err != nil {
		logger.Errorf("put piece to storage failed, piece num: %d, wrote: %d, error: %s", pieceNum, n, err)
		return n, err
	}
	success = true
	return n, nil
}

func (pm *pieceManager) DownloadSource(ctx context.Context, pt PeerTask, url string, headers map[string]string) error {
	contentLength, err := pm.resourceClient.GetContentLength(url, headers)
	log := logger.With("peer", pt.GetPeerID(), "task", pt.GetTaskID())
	if err != nil {
		log.Warnf("can not get content length for %s", url)
		contentLength = - 1
	} else {
		pm.storageManager.UpdateTask(ctx,
			&storage.UpdateTaskRequest{
				PeerTaskMetaData: storage.PeerTaskMetaData{
					PeerID: pt.GetPeerID(),
					TaskID: pt.GetTaskID(),
				},
				ContentLength: contentLength,
			})
	}
	log.Debugf("get content length: %d", contentLength)
	// 1. download piece from source
	response, err := pm.resourceClient.Download(url, headers)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	// 2. save to storage
	pieceSize := pm.computePieceSize(contentLength)
	// handle resource which content length is unknown
	if contentLength == -1 {
		var (
			n  int64
			er error
		)
		for pieceNum := int32(0); ; pieceNum++ {
			size := pieceSize
			offset := uint64(pieceNum) * uint64(pieceSize)

			n, er = pm.processPieceFromSource(pt, response.Body, contentLength, pieceNum, offset, size)
			if er != nil {
				return err
			}
			if n != int64(size) {
				contentLength = int64(pieceNum*pieceSize + size)
				pm.storageManager.UpdateTask(ctx,
					&storage.UpdateTaskRequest{
						PeerTaskMetaData: storage.PeerTaskMetaData{
							PeerID: pt.GetPeerID(),
							TaskID: pt.GetTaskID(),
						},
						ContentLength: contentLength,
					})
				return pt.SetContentLength(contentLength)
			}
		}
		//return nil
	}

	maxPieceNum := int32(math.Ceil(float64(contentLength) / float64(pieceSize)))
	for pieceNum := int32(0); pieceNum < maxPieceNum; pieceNum++ {
		size := pieceSize
		offset := uint64(pieceNum) * uint64(pieceSize)
		// calculate piece size for last piece
		if contentLength > 0 && int64(offset)+int64(size) > contentLength {
			size = int32(contentLength - int64(offset))
		}

		n, er := pm.processPieceFromSource(pt, response.Body, contentLength, pieceNum, offset, size)
		if er != nil {
			return er
		}
		if n != int64(size) {
			return storage.ErrShortRead
		}
	}
	return nil
}

// TODO copy from cdnsystem/daemon/mgr/task/manager_util.go
// computePieceSize computes the piece size with specified fileLength.
//
// If the fileLength<=0, which means failed to get fileLength
// and then use the DefaultPieceSize.
func computePieceSize(length int64) int32 {
	if length <= 0 || length <= 200*1024*1024 {
		return config.DefaultPieceSize
	}

	gapCount := length / int64(100*1024*1024)
	mpSize := (gapCount-2)*1024*1024 + config.DefaultPieceSize
	if mpSize > config.DefaultPieceSizeLimit {
		return config.DefaultPieceSizeLimit
	}
	return int32(mpSize)
}
