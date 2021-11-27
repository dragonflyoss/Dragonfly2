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

	"d7y.io/dragonfly/v2/cdn/cdnutil"
	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
)

type PieceManager interface {
	DownloadSource(ctx context.Context, pt Task, request *scheduler.PeerTaskRequest) error
	DownloadPiece(ctx context.Context, peerTask Task, request *DownloadPieceRequest) bool
	ReadPiece(ctx context.Context, req *storage.ReadPieceRequest) (io.Reader, io.Closer, error)
}

type pieceManager struct {
	*rate.Limiter
	storageManager   storage.TaskStorageDriver
	pieceDownloader  PieceDownloader
	computePieceSize func(contentLength int64) uint32

	calculateDigest bool
}

var _ PieceManager = (*pieceManager)(nil)

func NewPieceManager(s storage.TaskStorageDriver, pieceDownloadTimeout time.Duration, opts ...func(*pieceManager)) (PieceManager, error) {
	pm := &pieceManager{
		storageManager:   s,
		computePieceSize: cdnutil.ComputePieceSize,
		calculateDigest:  true,
	}
	for _, opt := range opts {
		opt(pm)
	}

	// set default value
	if pm.pieceDownloader == nil {
		pm.pieceDownloader, _ = NewPieceDownloader(pieceDownloadTimeout)
	}
	return pm, nil
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

func (pm *pieceManager) DownloadPiece(ctx context.Context, pt Task, request *DownloadPieceRequest) (success bool) {
	var (
		start = time.Now().UnixNano()
		end   int64
		err   error
	)
	defer func() {
		_, rspan := tracer.Start(ctx, config.SpanPushPieceResult)
		rspan.SetAttributes(config.AttributeWritePieceSuccess.Bool(success))
		if success {
			pm.pushSuccessResult(pt, request.DstPid, request.piece, start, end)
		} else {
			pm.pushFailResult(pt, request.DstPid, request.piece, start, end, err, false)
		}
		rspan.End()
	}()

	// 1. download piece from other peers
	if pm.Limiter != nil {
		if err = pm.Limiter.WaitN(ctx, int(request.piece.RangeSize)); err != nil {
			pt.Log().Errorf("require rate limit access error: %s", err)
			return
		}
	}
	ctx, span := tracer.Start(ctx, config.SpanWritePiece)
	request.CalcDigest = pm.calculateDigest && request.piece.PieceMd5 != ""
	span.SetAttributes(config.AttributeTargetPeerID.String(request.DstPid))
	span.SetAttributes(config.AttributeTargetPeerAddr.String(request.DstAddr))
	span.SetAttributes(config.AttributePiece.Int(int(request.piece.PieceNum)))

	var (
		r io.Reader
		c io.Closer
	)
	r, c, err = pm.pieceDownloader.DownloadPiece(ctx, request)
	if err != nil {
		span.RecordError(err)
		span.End()
		pt.Log().Errorf("download piece failed, piece num: %d, error: %s, from peer: %s",
			request.piece.PieceNum, err, request.DstPid)
		return
	}
	defer c.Close()

	// 2. save to storage
	var n int64
	n, err = pm.storageManager.WritePiece(ctx, &storage.WritePieceRequest{
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
	end = time.Now().UnixNano()
	span.RecordError(err)
	span.End()
	if n > 0 {
		pt.AddTraffic(uint64(n))
	}
	if err != nil {
		pt.Log().Errorf("put piece to storage failed, piece num: %d, wrote: %d, error: %s",
			request.piece.PieceNum, n, err)
		return
	}
	success = true
	return
}

func (pm *pieceManager) pushSuccessResult(peerTask Task, dstPid string, piece *base.PieceInfo, start int64, end int64) {
	err := peerTask.ReportPieceResult(
		&pieceTaskResult{
			piece: piece,
			pieceResult: &scheduler.PieceResult{
				TaskId:        peerTask.GetTaskID(),
				SrcPid:        peerTask.GetPeerID(),
				DstPid:        dstPid,
				PieceInfo:     piece,
				BeginTime:     uint64(start),
				EndTime:       uint64(end),
				Success:       true,
				Code:          base.Code_Success,
				HostLoad:      nil,                // TODO(jim): update host load
				FinishedCount: piece.PieceNum + 1, // update by peer task
				// TODO range_start, range_size, piece_md5, piece_offset, piece_style
			},
			err: nil,
		})
	if err != nil {
		peerTask.Log().Errorf("report piece task error: %v", err)
	}
}

func (pm *pieceManager) pushFailResult(peerTask Task, dstPid string, piece *base.PieceInfo, start int64, end int64, err error, notRetry bool) {
	err = peerTask.ReportPieceResult(
		&pieceTaskResult{
			piece: piece,
			pieceResult: &scheduler.PieceResult{
				TaskId:        peerTask.GetTaskID(),
				SrcPid:        peerTask.GetPeerID(),
				DstPid:        dstPid,
				PieceInfo:     piece,
				BeginTime:     uint64(start),
				EndTime:       uint64(end),
				Success:       false,
				Code:          base.Code_ClientPieceDownloadFail,
				HostLoad:      nil,
				FinishedCount: 0, // update by peer task
			},
			err:      err,
			notRetry: notRetry,
		})
	if err != nil {
		peerTask.Log().Errorf("report piece task error: %v", err)
	}
}

func (pm *pieceManager) ReadPiece(ctx context.Context, req *storage.ReadPieceRequest) (io.Reader, io.Closer, error) {
	return pm.storageManager.ReadPiece(ctx, req)
}

func (pm *pieceManager) processPieceFromSource(pt Task,
	reader io.Reader, contentLength int64, pieceNum int32, pieceOffset uint64, pieceSize uint32) (int64, error) {
	var (
		success bool
		start   = time.Now().UnixNano()
		end     int64
		err     error
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
				}, start, end, err, true)
		}
	}()

	if pm.Limiter != nil {
		if err = pm.Limiter.WaitN(pt.Context(), int(size)); err != nil {
			pt.Log().Errorf("require rate limit access error: %s", err)
			return 0, err
		}
	}
	if pm.calculateDigest {
		pt.Log().Debugf("calculate digest")
		reader = digestutils.NewDigestReader(pt.Log(), reader)
	}
	var n int64
	n, err = pm.storageManager.WritePiece(
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
	if n != int64(size) && n > 0 {
		size = uint32(n)
	}
	end = time.Now().UnixNano()
	if n > 0 {
		pt.AddTraffic(uint64(n))
	}
	if err != nil {
		pt.Log().Errorf("put piece to storage failed, piece num: %d, wrote: %d, error: %s", pieceNum, n, err)
		return n, err
	}
	if pm.calculateDigest {
		md5 = reader.(digestutils.DigestReader).Digest()
	}
	success = true
	return n, nil
}

func (pm *pieceManager) DownloadSource(ctx context.Context, pt Task, request *scheduler.PeerTaskRequest) error {
	if request.UrlMeta == nil {
		request.UrlMeta = &base.UrlMeta{
			Header: map[string]string{},
		}
	} else if request.UrlMeta.Header == nil {
		request.UrlMeta.Header = map[string]string{}
	}
	if request.UrlMeta.Range != "" {
		request.UrlMeta.Header["Range"] = request.UrlMeta.Range
	}
	log := pt.Log()
	log.Infof("start to download from source")
	contentLength, err := source.GetContentLength(ctx, request.Url, request.UrlMeta.Header)
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
	body, err := source.Download(ctx, request.Url, request.UrlMeta.Header)
	if err != nil {
		return err
	}
	defer body.Close()
	reader := body.(io.Reader)

	// calc total md5
	if pm.calculateDigest && request.UrlMeta.Digest != "" {
		reader = digestutils.NewDigestReader(pt.Log(), body, request.UrlMeta.Digest)
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
				contentLength = int64(pieceNum)*int64(pieceSize) + n
				if err := pm.storageManager.UpdateTask(ctx,
					&storage.UpdateTaskRequest{
						PeerTaskMetaData: storage.PeerTaskMetaData{
							PeerID: pt.GetPeerID(),
							TaskID: pt.GetTaskID(),
						},
						ContentLength:  contentLength,
						GenPieceDigest: true,
					}); err != nil {
					log.Errorf("update task failed %s", err)
				}
				pt.SetTotalPieces(pieceNum + 1)
				return pt.SetContentLength(contentLength)
			}
		}
		//unreachable code
	}

	maxPieceNum := int32(math.Ceil(float64(contentLength) / float64(pieceSize)))
	for pieceNum := int32(0); pieceNum < maxPieceNum; pieceNum++ {
		size := pieceSize
		offset := uint64(pieceNum) * uint64(pieceSize)
		// calculate piece size for last piece
		if contentLength > 0 && int64(offset)+int64(size) > contentLength {
			size = uint32(contentLength - int64(offset))
		}

		log.Debugf("download piece %d", pieceNum)
		n, er := pm.processPieceFromSource(pt, reader, contentLength, pieceNum, offset, size)
		if er != nil {
			log.Errorf("download piece %d error: %s", pieceNum, er)
			return er
		}
		if n != int64(size) {
			log.Errorf("download piece %d size not match, desired: %d, actual: %d", pieceNum, size, n)
			return storage.ErrShortRead
		}
	}
	if maxPieceNum > 0 {
		pt.SetTotalPieces(maxPieceNum)
	}
	if err := pt.SetContentLength(contentLength); err != nil {
		log.Errorf("set content length failed %s", err)
	}

	if err := pm.storageManager.UpdateTask(ctx,
		&storage.UpdateTaskRequest{
			PeerTaskMetaData: storage.PeerTaskMetaData{
				PeerID: pt.GetPeerID(),
				TaskID: pt.GetTaskID(),
			},
			ContentLength:  contentLength,
			TotalPieces:    maxPieceNum,
			GenPieceDigest: true,
		}); err != nil {
		log.Errorf("update task failed %s", err)
	}
	log.Infof("download from source ok")
	return nil
}
