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
	DownloadSource(ctx context.Context, pt PeerTask, request *scheduler.PeerTaskRequest) error
	DownloadPiece(peerTask PeerTask, request *DownloadPieceRequest) bool
	ReadPiece(ctx context.Context, req *storage.ReadPieceRequest) (io.Reader, io.Closer, error)
}

type pieceManager struct {
	*rate.Limiter
	storageManager   storage.TaskStorageDriver
	pieceDownloader  PieceDownloader
	resourceClient   source.ResourceClient
	computePieceSize func(contentLength int64) int32

	calculateDigest bool
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
		calculateDigest:  true,
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

func WithCalculateDigest(enable bool) func(*pieceManager) {
	return func(pm *pieceManager) {
		logger.Infof("set calculateDigest to %t for piece manager", enable)
		pm.calculateDigest = enable
	}
}

// WithLimiter sets upload rate limiter, the burst size must big than piece size
func WithLimiter(limiter *rate.Limiter) func(*pieceManager) {
	return func(manager *pieceManager) {
		manager.Limiter = limiter
	}
}

func (pm *pieceManager) DownloadPiece(pt PeerTask, request *DownloadPieceRequest) (success bool) {
	var (
		start = time.Now().UnixNano()
		end   int64
	)
	defer func() {
		if success {
			pm.pushSuccessResult(pt, request.DstPid, request.piece, start, end)
		} else {
			pm.pushFailResult(pt, request.DstPid, request.piece, start, end)
		}
	}()

	// 1. download piece from other peers
	if pm.Limiter != nil {
		if err := pm.Limiter.WaitN(pt.Context(), int(request.piece.RangeSize)); err != nil {
			pt.Log().Errorf("require rate limit access error: %s", err)
			return
		}
	}
	request.CalcDigest = pm.calculateDigest && request.piece.PieceMd5 != ""
	r, c, err := pm.pieceDownloader.DownloadPiece(request)
	if err != nil {
		pt.Log().Errorf("download piece failed, piece num: %d, error: %s", request.piece.PieceNum, err)
		return
	}
	end = time.Now().UnixNano()
	defer c.Close()

	// 2. save to storage
	n, err := pm.storageManager.WritePiece(pt.Context(), &storage.WritePieceRequest{
		PeerTaskMetaData: storage.PeerTaskMetaData{
			PeerID: pt.GetPeerID(),
			TaskID: pt.GetTaskID(),
		},
		PieceMetaData: storage.PieceMetaData{
			Num:    request.piece.PieceNum,
			Md5:    request.piece.PieceMd5,
			Offset: request.piece.PieceOffset,
			Range: clientutil.Range{
				Start:  int64(request.piece.RangeStart),
				Length: int64(request.piece.RangeSize),
			},
		},
		Reader: r,
	})
	pt.AddTraffic(n)
	if err != nil {
		pt.Log().Errorf("put piece to storage failed, piece num: %d, wrote: %d, error: %s",
			request.piece.PieceNum, n, err)
		return
	}
	success = true
	return
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
		peerTask.Log().Errorf("report piece task error: %v", err)
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
			Code:          dfcodes.ClientPieceDownloadFail,
			HostLoad:      nil,
			FinishedCount: 0, // update by peer task
		})
	if err != nil {
		peerTask.Log().Errorf("report piece task error: %v", err)
	}
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
		unknownLength = contentLength == -1
		md5           = ""
	)

	defer func() {
		if success {
			pm.pushSuccessResult(pt, pt.GetPeerID(),
				&base.PieceInfo{
					PieceNum:    pieceNum,
					RangeStart:  pieceOffset,
					RangeSize:   size,
					PieceMd5:    md5,
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
		if err := pm.Limiter.WaitN(pt.Context(), int(size)); err != nil {
			pt.Log().Errorf("require rate limit access error: %s", err)
			return 0, err
		}
	}
	if pm.calculateDigest {
		reader = clientutil.NewDigestReader(reader, "")
	}
	n, err := pm.storageManager.WritePiece(
		pt.Context(),
		&storage.WritePieceRequest{
			UnknownLength: unknownLength,
			PeerTaskMetaData: storage.PeerTaskMetaData{
				PeerID: pt.GetPeerID(),
				TaskID: pt.GetTaskID(),
			},
			PieceMetaData: storage.PieceMetaData{
				Num: pieceNum,
				// storage manager will get digest from DigestReader, keep empty here is ok
				Md5:    "",
				Offset: pieceOffset,
				Range: clientutil.Range{
					Start:  int64(pieceOffset),
					Length: int64(size),
				},
			},
			Reader: reader,
		})
	if n != int64(size) {
		size = int32(n)
	}
	end = time.Now().UnixNano()
	pt.AddTraffic(n)
	if err != nil {
		pt.Log().Errorf("put piece to storage failed, piece num: %d, wrote: %d, error: %s", pieceNum, n, err)
		return n, err
	}
	if pm.calculateDigest {
		md5 = reader.(clientutil.DigestReader).Digest()
	}
	success = true
	return n, nil
}

func (pm *pieceManager) DownloadSource(ctx context.Context, pt PeerTask, request *scheduler.PeerTaskRequest) error {
	if request.UrlMata == nil {
		request.UrlMata = &base.UrlMeta{
			Header: map[string]string{},
		}
	} else if request.UrlMata.Header == nil {
		request.UrlMata.Header = map[string]string{}
	}
	if request.UrlMata.Range != "" {
		request.UrlMata.Header["Range"] = request.UrlMata.Range
	}
	log := pt.Log()
	log.Infof("start to download from source")
	contentLength, err := pm.resourceClient.GetContentLength(request.Url, request.UrlMata.Header)
	if err != nil {
		log.Warnf("get content length error: %s for %s", err, request.Url)
	}
	if contentLength == -1 {
		log.Warnf("can not get content length for %s", request.Url)
	} else {
		err = pm.storageManager.UpdateTask(ctx,
			&storage.UpdateTaskRequest{
				PeerTaskMetaData: storage.PeerTaskMetaData{
					PeerID: pt.GetPeerID(),
					TaskID: pt.GetTaskID(),
				},
				ContentLength: contentLength,
			})
		if err != nil {
			return err
		}
	}
	log.Debugf("get content length: %d", contentLength)
	// 1. download piece from source
	body, _, err := pm.resourceClient.Download(request.Url, request.UrlMata.Header)
	if err != nil {
		return err
	}
	defer body.Close()
	reader := body.(io.Reader)

	// calc total md5
	if pm.calculateDigest && request.UrlMata.Md5 != "" {
		reader = clientutil.NewDigestReader(body, request.UrlMata.Md5)
	}

	// 2. save to storage
	pieceSize := pm.computePieceSize(contentLength)
	// handle resource which content length is unknown
	if contentLength == -1 {
		var n int64
		for pieceNum := int32(0); ; pieceNum++ {
			size := pieceSize
			offset := uint64(pieceNum) * uint64(pieceSize)
			log.Debugf("download piece %d", pieceNum)
			n, err = pm.processPieceFromSource(pt, reader, contentLength, pieceNum, offset, size)
			if err != nil {
				log.Errorf("download piece %d error: %s", pieceNum, err)
				return err
			}
			// last piece, piece size maybe 0
			if n < int64(size) {
				contentLength = int64(pieceNum*pieceSize) + n
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
		// unreachable code
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

		log.Debugf("download piece %d", pieceNum)
		n, er := pm.processPieceFromSource(pt, reader, contentLength, pieceNum, offset, size)
		if er != nil {
			log.Errorf("download piece %d error: %s", pieceNum, err)
			return er
		}
		if n != int64(size) {
			log.Errorf("download piece %d size not match, desired: %d, actual: %d", pieceNum, size, n)
			return storage.ErrShortRead
		}
	}
	log.Infof("download from source ok")
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
