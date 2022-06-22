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
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/util"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/source"
)

type PieceManager interface {
	DownloadSource(ctx context.Context, pt Task, request *scheduler.PeerTaskRequest) error
	DownloadPiece(ctx context.Context, request *DownloadPieceRequest) (*DownloadPieceResult, error)
	ImportFile(ctx context.Context, ptm storage.PeerTaskMetadata, tsd storage.TaskStorageDriver, req *dfdaemon.ImportTaskRequest) error
}

type pieceManager struct {
	*rate.Limiter
	pieceDownloader  PieceDownloader
	computePieceSize func(contentLength int64) uint32

	calculateDigest bool
}

var _ PieceManager = (*pieceManager)(nil)

func NewPieceManager(pieceDownloadTimeout time.Duration, opts ...func(*pieceManager)) (PieceManager, error) {
	pm := &pieceManager{
		computePieceSize: util.ComputePieceSize,
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

// WithLimiter sets upload rate limiter, the burst size must be bigger than piece size
func WithLimiter(limiter *rate.Limiter) func(*pieceManager) {
	return func(manager *pieceManager) {
		manager.Limiter = limiter
	}
}

func WithTransportOption(opt *config.TransportOption) func(*pieceManager) {
	return func(manager *pieceManager) {
		if opt == nil {
			return
		}
		if opt.IdleConnTimeout > 0 {
			defaultTransport.(*http.Transport).IdleConnTimeout = opt.IdleConnTimeout
		}
		if opt.DialTimeout > 0 && opt.KeepAlive > 0 {
			defaultTransport.(*http.Transport).DialContext = (&net.Dialer{
				Timeout:   opt.DialTimeout,
				KeepAlive: opt.KeepAlive,
				DualStack: true,
			}).DialContext
		}
		if opt.MaxIdleConns > 0 {
			defaultTransport.(*http.Transport).MaxIdleConns = opt.MaxIdleConns
		}
		if opt.ExpectContinueTimeout > 0 {
			defaultTransport.(*http.Transport).ExpectContinueTimeout = opt.ExpectContinueTimeout
		}
		if opt.ResponseHeaderTimeout > 0 {
			defaultTransport.(*http.Transport).ResponseHeaderTimeout = opt.ResponseHeaderTimeout
		}
		if opt.TLSHandshakeTimeout > 0 {
			defaultTransport.(*http.Transport).TLSHandshakeTimeout = opt.TLSHandshakeTimeout
		}

		logger.Infof("default transport: %#v", defaultTransport)
	}
}

func (pm *pieceManager) DownloadPiece(ctx context.Context, request *DownloadPieceRequest) (*DownloadPieceResult, error) {
	var result = &DownloadPieceResult{
		Size:       -1,
		BeginTime:  time.Now().UnixNano(),
		FinishTime: 0,
	}

	// prepare trace and limit
	ctx, span := tracer.Start(ctx, config.SpanWritePiece)
	defer span.End()
	if pm.Limiter != nil {
		if err := pm.Limiter.WaitN(ctx, int(request.piece.RangeSize)); err != nil {
			result.FinishTime = time.Now().UnixNano()
			request.log.Errorf("require rate limit access error: %s", err)
			return result, err
		}
	}
	request.CalcDigest = pm.calculateDigest && request.piece.PieceMd5 != ""
	span.SetAttributes(config.AttributeTargetPeerID.String(request.DstPid))
	span.SetAttributes(config.AttributeTargetPeerAddr.String(request.DstAddr))
	span.SetAttributes(config.AttributePiece.Int(int(request.piece.PieceNum)))

	// 1. download piece
	r, c, err := pm.pieceDownloader.DownloadPiece(ctx, request)
	if err != nil {
		result.FinishTime = time.Now().UnixNano()
		span.RecordError(err)
		request.log.Errorf("download piece failed, piece num: %d, error: %s, from peer: %s",
			request.piece.PieceNum, err, request.DstPid)
		return result, err
	}
	defer c.Close()

	// 2. save to storage
	writePieceRequest := &storage.WritePieceRequest{
		Reader: r,
		PeerTaskMetadata: storage.PeerTaskMetadata{
			PeerID: request.PeerID,
			TaskID: request.TaskID,
		},
		PieceMetadata: storage.PieceMetadata{
			Num:    request.piece.PieceNum,
			Md5:    request.piece.PieceMd5,
			Offset: request.piece.PieceOffset,
			Range: clientutil.Range{
				Start:  int64(request.piece.RangeStart),
				Length: int64(request.piece.RangeSize),
			},
		},
	}

	result.Size, err = request.storage.WritePiece(ctx, writePieceRequest)
	result.FinishTime = time.Now().UnixNano()

	span.RecordError(err)
	if err != nil {
		request.log.Errorf("put piece to storage failed, piece num: %d, wrote: %d, error: %s",
			request.piece.PieceNum, result.Size, err)
		return result, err
	}
	return result, nil
}

func (pm *pieceManager) processPieceFromSource(pt Task,
	reader io.Reader, contentLength int64, pieceNum int32, pieceOffset uint64, pieceSize uint32,
	isLastPiece func(n int64) (totalPieces int32, contentLength int64, ok bool)) (
	result *DownloadPieceResult, md5 string, err error) {
	result = &DownloadPieceResult{
		Size:       -1,
		BeginTime:  time.Now().UnixNano(),
		FinishTime: 0,
	}

	var (
		unknownLength = contentLength == -1
	)

	if pm.Limiter != nil {
		if err = pm.Limiter.WaitN(pt.Context(), int(pieceSize)); err != nil {
			result.FinishTime = time.Now().UnixNano()
			pt.Log().Errorf("require rate limit access error: %s", err)
			return
		}
	}
	if pm.calculateDigest {
		pt.Log().Debugf("calculate digest")
		reader, _ = digest.NewReader(reader, digest.WithLogger(pt.Log()))
	}
	var n int64
	result.Size, err = pt.GetStorage().WritePiece(
		pt.Context(),
		&storage.WritePieceRequest{
			UnknownLength: unknownLength,
			PeerTaskMetadata: storage.PeerTaskMetadata{
				PeerID: pt.GetPeerID(),
				TaskID: pt.GetTaskID(),
			},
			PieceMetadata: storage.PieceMetadata{
				Num: pieceNum,
				// storage manager will get digest from Reader, keep empty here is ok
				Md5:    "",
				Offset: pieceOffset,
				Range: clientutil.Range{
					Start:  int64(pieceOffset),
					Length: int64(pieceSize),
				},
			},
			Reader:      reader,
			GenMetadata: isLastPiece,
		})

	result.FinishTime = time.Now().UnixNano()
	if n > 0 {
		pt.AddTraffic(uint64(n))
	}
	if err != nil {
		pt.Log().Errorf("put piece to storage failed, piece num: %d, wrote: %d, error: %s", pieceNum, n, err)
		return
	}
	if pm.calculateDigest {
		md5 = reader.(digest.Reader).Encoded()
	}
	return
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
		// in http source package, adapter will update the real range, we inject "X-Dragonfly-Range" here
		request.UrlMeta.Header[source.Range] = request.UrlMeta.Range
	}
	log := pt.Log()
	log.Infof("start to download from source")

	// 1. download pieces from source
	downloadRequest, err := source.NewRequestWithContext(ctx, request.Url, request.UrlMeta.Header)
	if err != nil {
		return err
	}
	response, err := source.Download(downloadRequest)
	// TODO update expire info
	if err != nil {
		return err
	}
	err = response.Validate()
	if err != nil {
		return err
	}
	contentLength := response.ContentLength
	if contentLength < 0 {
		log.Warnf("can not get content length for %s", request.Url)
	} else {
		log.Debugf("back source content length: %d", contentLength)
		err = pt.GetStorage().UpdateTask(ctx,
			&storage.UpdateTaskRequest{
				PeerTaskMetadata: storage.PeerTaskMetadata{
					PeerID: pt.GetPeerID(),
					TaskID: pt.GetTaskID(),
				},
				ContentLength: contentLength,
				Header:        &response.Header,
			})
		if err != nil {
			return err
		}
	}
	defer response.Body.Close()
	reader := response.Body.(io.Reader)

	// calc total
	if pm.calculateDigest {
		reader, err = digest.NewReader(response.Body, digest.WithDigest(request.UrlMeta.Digest), digest.WithLogger(pt.Log()))
		if err != nil {
			log.Errorf("init digest reader error: %s", err.Error())
			return err
		}
	}
	// we must calculate piece size
	pieceSize := pm.computePieceSize(contentLength)

	// 2. save to storage
	// handle resource which content length is unknown
	if contentLength < 0 {
		return pm.downloadUnknownLengthSource(pt, pieceSize, reader)
	}

	return pm.downloadKnownLengthSource(pt, contentLength, pieceSize, reader)
}

func (pm *pieceManager) downloadKnownLengthSource(pt Task, contentLength int64, pieceSize uint32, reader io.Reader) error {
	log := pt.Log()
	maxPieceNum := util.ComputePieceNum(contentLength, pieceSize)
	pt.SetContentLength(contentLength)
	pt.SetTotalPieces(maxPieceNum)

	for pieceNum := int32(0); pieceNum < maxPieceNum; pieceNum++ {
		size := pieceSize
		offset := uint64(pieceNum) * uint64(pieceSize)
		// calculate piece size for last piece
		if contentLength > 0 && int64(offset)+int64(size) > contentLength {
			size = uint32(contentLength - int64(offset))
		}

		log.Debugf("download piece %d", pieceNum)
		result, md5, err := pm.processPieceFromSource(
			pt, reader, contentLength, pieceNum, offset, size,
			func(int64) (int32, int64, bool) {
				return maxPieceNum, contentLength, pieceNum == maxPieceNum-1
			})
		request := &DownloadPieceRequest{
			TaskID: pt.GetTaskID(),
			PeerID: pt.GetPeerID(),
			piece: &base.PieceInfo{
				PieceNum:    pieceNum,
				RangeStart:  offset,
				RangeSize:   uint32(result.Size),
				PieceMd5:    md5,
				PieceOffset: offset,
				PieceStyle:  0,
			},
		}
		if err != nil {
			log.Errorf("download piece %d error: %s", pieceNum, err)
			pt.ReportPieceResult(request, result, detectBackSourceError(err))
			return err
		}

		if result.Size != int64(size) {
			log.Errorf("download piece %d size not match, desired: %d, actual: %d", pieceNum, size, result.Size)
			pt.ReportPieceResult(request, result, detectBackSourceError(err))
			return storage.ErrShortRead
		}

		pt.ReportPieceResult(request, result, nil)
		pt.PublishPieceInfo(pieceNum, uint32(result.Size))
	}

	log.Infof("download from source ok")
	return nil
}

func (pm *pieceManager) downloadUnknownLengthSource(pt Task, pieceSize uint32, reader io.Reader) error {
	var (
		contentLength int64 = -1
		totalPieces   int32 = -1
	)
	log := pt.Log()
	for pieceNum := int32(0); ; pieceNum++ {
		size := pieceSize
		offset := uint64(pieceNum) * uint64(pieceSize)
		log.Debugf("download piece %d", pieceNum)
		result, md5, err := pm.processPieceFromSource(
			pt, reader, contentLength, pieceNum, offset, size,
			func(n int64) (int32, int64, bool) {
				if n >= int64(pieceSize) {
					return -1, -1, false
				}

				// last piece, piece size maybe 0
				contentLength = int64(pieceSize)*int64(pieceNum) + n
				// when n == 0, content length is aligned at piece size, need ignore current piece
				if n == 0 {
					totalPieces = pieceNum
				} else {
					totalPieces = pieceNum + 1
				}
				return totalPieces, contentLength, true
			})
		request := &DownloadPieceRequest{
			TaskID: pt.GetTaskID(),
			PeerID: pt.GetPeerID(),
			piece: &base.PieceInfo{
				PieceNum:    pieceNum,
				RangeStart:  offset,
				RangeSize:   uint32(result.Size),
				PieceMd5:    md5,
				PieceOffset: offset,
				PieceStyle:  0,
			},
		}
		if err != nil {
			pt.ReportPieceResult(request, result, detectBackSourceError(err))
			log.Errorf("download piece %d error: %s", pieceNum, err)
			return err
		}

		if result.Size == int64(size) {
			pt.ReportPieceResult(request, result, nil)
			pt.PublishPieceInfo(pieceNum, uint32(result.Size))
			log.Debugf("piece %d downloaded, size: %d", pieceNum, result.Size)
			continue
		} else if result.Size > int64(size) {
			err = fmt.Errorf("piece %d size %d should not great than %d", pieceNum, result.Size, size)
			log.Errorf(err.Error())
			pt.ReportPieceResult(request, result, detectBackSourceError(err))
			return err
		}

		// content length is aligning at piece size
		if result.Size == 0 {
			pt.SetTotalPieces(totalPieces)
			pt.SetContentLength(contentLength)
			log.Debugf("final piece is %d", pieceNum-1)
			break
		}

		pt.SetTotalPieces(totalPieces)
		pt.SetContentLength(contentLength)
		pt.ReportPieceResult(request, result, nil)
		pt.PublishPieceInfo(pieceNum, uint32(result.Size))
		log.Debugf("final piece %d downloaded, size: %d", pieceNum, result.Size)
		break
	}

	log.Infof("download from source ok")
	return nil
}

func detectBackSourceError(err error) error {
	// TODO ensure all source plugin use *url.Error for back source
	if e, ok := err.(*url.Error); ok {
		return &backSourceError{err: e}
	}
	return err
}

func (pm *pieceManager) processPieceFromFile(ctx context.Context, ptm storage.PeerTaskMetadata,
	tsd storage.TaskStorageDriver, r io.Reader, pieceNum int32, pieceOffset uint64,
	pieceSize uint32, isLastPiece func(n int64) (int32, int64, bool)) (int64, error) {
	var (
		n      int64
		reader = r
		log    = logger.With("function", "processPieceFromFile", "taskID", ptm.TaskID)
	)

	if pm.calculateDigest {
		log.Debugf("calculate digest in processPieceFromFile")
		reader, _ = digest.NewReader(r, digest.WithLogger(log))
	}
	n, err := tsd.WritePiece(ctx,
		&storage.WritePieceRequest{
			UnknownLength:    false,
			PeerTaskMetadata: ptm,
			PieceMetadata: storage.PieceMetadata{
				Num: pieceNum,
				// storage manager will get digest from Reader, keep empty here is ok
				Md5:    "",
				Offset: pieceOffset,
				Range: clientutil.Range{
					Start:  int64(pieceOffset),
					Length: int64(pieceSize),
				},
			},
			Reader:      reader,
			GenMetadata: isLastPiece,
		})
	if err != nil {
		msg := fmt.Sprintf("put piece of task %s to storage failed, piece num: %d, wrote: %d, error: %s", ptm.TaskID, pieceNum, n, err)
		return n, errors.New(msg)
	}
	return n, nil
}

func (pm *pieceManager) ImportFile(ctx context.Context, ptm storage.PeerTaskMetadata, tsd storage.TaskStorageDriver, req *dfdaemon.ImportTaskRequest) error {
	log := logger.With("function", "ImportFile", "URL", req.Url, "taskID", ptm.TaskID)
	// get file size and compute piece size and piece count
	stat, err := os.Stat(req.Path)
	if err != nil {
		msg := fmt.Sprintf("stat file %s failed: %s", req.Path, err)
		log.Error(msg)
		return errors.New(msg)
	}
	contentLength := stat.Size()
	pieceSize := pm.computePieceSize(contentLength)
	maxPieceNum := util.ComputePieceNum(contentLength, pieceSize)

	file, err := os.Open(req.Path)
	if err != nil {
		msg := fmt.Sprintf("open file %s failed: %s", req.Path, err)
		log.Error(msg)
		return errors.New(msg)
	}
	defer file.Close()

	reader := file
	for pieceNum := int32(0); pieceNum < maxPieceNum; pieceNum++ {
		size := pieceSize
		offset := uint64(pieceNum) * uint64(pieceSize)
		isLastPiece := func(int64) (int32, int64, bool) { return maxPieceNum, contentLength, pieceNum == maxPieceNum-1 }
		// calculate piece size for last piece
		if contentLength > 0 && int64(offset)+int64(size) > contentLength {
			size = uint32(contentLength - int64(offset))
		}

		log.Debugf("import piece %d", pieceNum)
		n, er := pm.processPieceFromFile(ctx, ptm, tsd, reader, pieceNum, offset, size, isLastPiece)
		if er != nil {
			log.Errorf("import piece %d of task %s error: %s", pieceNum, ptm.TaskID, er)
			return er
		}
		if n != int64(size) {
			log.Errorf("import piece %d of task %s size not match, desired: %d, actual: %d", pieceNum, ptm.TaskID, size, n)
			return storage.ErrShortRead
		}
	}

	// Update task with length and piece count
	err = tsd.UpdateTask(ctx, &storage.UpdateTaskRequest{
		PeerTaskMetadata: ptm,
		ContentLength:    contentLength,
		TotalPieces:      maxPieceNum,
	})
	if err != nil {
		msg := fmt.Sprintf("update task(%s) failed: %s", ptm.TaskID, err)
		log.Error(msg)
		return errors.New(msg)
	}

	// Save metadata
	err = tsd.Store(ctx, &storage.StoreRequest{
		CommonTaskRequest: storage.CommonTaskRequest{
			PeerID: ptm.PeerID,
			TaskID: ptm.TaskID,
		},
		MetadataOnly:  true,
		StoreDataOnly: false,
	})
	if err != nil {
		msg := fmt.Sprintf("store task(%s) failed: %s", ptm.TaskID, err)
		log.Error(msg)
		return errors.New(msg)
	}

	return nil
}
