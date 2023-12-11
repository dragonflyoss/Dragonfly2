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

//go:generate mockgen -destination piece_manager_mock.go -source piece_manager.go -package peer

package peer

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/go-http-utils/headers"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
	errordetailsv1 "d7y.io/api/v2/pkg/apis/errordetails/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/util"
	"d7y.io/dragonfly/v2/pkg/digest"
	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/retry"
	"d7y.io/dragonfly/v2/pkg/source"
)

type PieceManager interface {
	DownloadSource(ctx context.Context, pt Task, request *schedulerv1.PeerTaskRequest, parsedRange *nethttp.Range) error
	DownloadPiece(ctx context.Context, request *DownloadPieceRequest) (*DownloadPieceResult, error)
	ImportFile(ctx context.Context, ptm storage.PeerTaskMetadata, tsd storage.TaskStorageDriver, req *dfdaemonv1.ImportTaskRequest) error
	Import(ctx context.Context, ptm storage.PeerTaskMetadata, tsd storage.TaskStorageDriver, contentLength int64, reader io.Reader) error
}

type pieceManager struct {
	*rate.Limiter
	pieceDownloader   PieceDownloader
	computePieceSize  func(contentLength int64) uint32
	calculateDigest   bool
	concurrentOption  *config.ConcurrentOption
	syncPieceViaHTTPS bool
	certPool          *x509.CertPool
}

type PieceManagerOption func(*pieceManager)

func NewPieceManager(pieceDownloadTimeout time.Duration, opts ...PieceManagerOption) (PieceManager, error) {
	pm := &pieceManager{
		computePieceSize: util.ComputePieceSize,
		calculateDigest:  true,
	}

	for _, opt := range opts {
		opt(pm)
	}

	pm.pieceDownloader = NewPieceDownloader(pieceDownloadTimeout, pm.certPool)

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
		logger.Infof("set download limiter %f for piece manager", limiter.Limit())
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

func WithConcurrentOption(opt *config.ConcurrentOption) func(*pieceManager) {
	return func(manager *pieceManager) {
		manager.concurrentOption = opt
		if manager.concurrentOption == nil {
			return
		}
		if manager.concurrentOption.GoroutineCount <= 0 {
			manager.concurrentOption.GoroutineCount = 4
		}
		if manager.concurrentOption.InitBackoff <= 0 {
			manager.concurrentOption.InitBackoff = 0.5
		}
		if manager.concurrentOption.MaxBackoff <= 0 {
			manager.concurrentOption.MaxBackoff = 3
		}
		if manager.concurrentOption.MaxAttempts <= 0 {
			manager.concurrentOption.MaxAttempts = 3
		}
	}
}

func WithSyncPieceViaHTTPS(caCertPEM string) func(*pieceManager) {
	return func(pm *pieceManager) {
		logger.Infof("enable syncPieceViaHTTPS for piece manager")
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM([]byte(caCertPEM)) {
			logger.Fatalf("invalid ca cert pem: %s", caCertPEM)
		}
		pm.syncPieceViaHTTPS = true
		pm.certPool = certPool
	}
}

func (pm *pieceManager) DownloadPiece(ctx context.Context, request *DownloadPieceRequest) (*DownloadPieceResult, error) {
	var result = &DownloadPieceResult{
		Size:       -1,
		BeginTime:  time.Now().UnixNano(),
		FinishTime: 0,
		DstPeerID:  request.DstPid,
		Fail:       false,
		pieceInfo:  request.piece,
	}

	// prepare trace and limit
	ctx, span := tracer.Start(ctx, config.SpanWritePiece)
	defer span.End()
	if pm.Limiter != nil {
		if err := pm.Limiter.WaitN(ctx, int(request.piece.RangeSize)); err != nil {
			result.FinishTime = time.Now().UnixNano()
			result.Fail = true
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
		result.Fail = true
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
			Range: nethttp.Range{
				Start:  int64(request.piece.RangeStart),
				Length: int64(request.piece.RangeSize),
			},
		},
	}

	result.Size, err = request.storage.WritePiece(ctx, writePieceRequest)
	result.FinishTime = time.Now().UnixNano()

	span.RecordError(err)
	if err != nil {
		result.Fail = true
		request.log.Errorf("put piece to storage failed, piece num: %d, wrote: %d, error: %s",
			request.piece.PieceNum, result.Size, err)
		return result, err
	}
	return result, nil
}

// pieceOffset is the offset in the peer task, not the original range start from source
func (pm *pieceManager) processPieceFromSource(pt Task,
	reader io.Reader, contentLength int64, pieceNum int32, pieceOffset uint64, pieceSize uint32,
	isLastPiece func(n int64) (totalPieces int32, contentLength int64, ok bool)) (
	result *DownloadPieceResult, md5 string, err error) {
	result = &DownloadPieceResult{
		Size:       -1,
		BeginTime:  time.Now().UnixNano(),
		FinishTime: 0,
		DstPeerID:  "",
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
		pt.Log().Debugf("piece %d calculate digest", pieceNum)
		reader, _ = digest.NewReader(digest.AlgorithmMD5, reader, digest.WithLogger(pt.Log()))
	}

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
				Range: nethttp.Range{
					Start:  int64(pieceOffset),
					Length: int64(pieceSize),
				},
			},
			Reader:          reader,
			NeedGenMetadata: isLastPiece,
		})

	result.FinishTime = time.Now().UnixNano()
	if result.Size > 0 {
		pt.AddTraffic(uint64(result.Size))
	}
	if err != nil {
		pt.Log().Errorf("put piece to storage failed, piece num: %d, wrote: %d, error: %s", pieceNum, result.Size, err)
		return
	}
	if pm.calculateDigest {
		md5 = reader.(digest.Reader).Encoded()
	}
	return
}

func (pm *pieceManager) DownloadSource(ctx context.Context, pt Task, peerTaskRequest *schedulerv1.PeerTaskRequest, parsedRange *nethttp.Range) error {
	if peerTaskRequest.UrlMeta == nil {
		peerTaskRequest.UrlMeta = &commonv1.UrlMeta{
			Header: map[string]string{},
		}
	} else if peerTaskRequest.UrlMeta.Header == nil {
		peerTaskRequest.UrlMeta.Header = map[string]string{}
	}
	if peerTaskRequest.UrlMeta.Range != "" {
		// FIXME refactor source package, normal Range header is enough
		// in http source package, adapter will update the real range, we inject "X-Dragonfly-Range" here
		peerTaskRequest.UrlMeta.Header[source.Range] = peerTaskRequest.UrlMeta.Range
	}

	log := pt.Log()
	log.Infof("start to download from source")

	backSourceRequest, err := source.NewRequestWithContext(ctx, peerTaskRequest.Url, peerTaskRequest.UrlMeta.Header)
	if err != nil {
		return err
	}
	var (
		metadata            *source.Metadata
		supportConcurrent   bool
		targetContentLength int64
	)
	if pm.concurrentOption != nil {
		// check metadata
		// 1. support range request
		// 2. target content length is greater than concurrentOption.ThresholdSize
		metadata, err = source.GetMetadata(backSourceRequest)
		if err == nil && metadata.Validate != nil && metadata.Validate() == nil {
			if !metadata.SupportRange || metadata.TotalContentLength == -1 {
				goto singleDownload
			}
			supportConcurrent = true
			if parsedRange != nil {
				// we have the total content length, parse the real range
				newRanges, err := nethttp.ParseURLMetaRange(peerTaskRequest.UrlMeta.Range, metadata.TotalContentLength)
				if err != nil {
					log.Errorf("update task error: %s", err)
					return err
				}
				parsedRange.Start = newRanges.Start
				parsedRange.Length = newRanges.Length
			} else {
				// for non-ranged request, add a dummy range
				parsedRange = &nethttp.Range{
					Start:  0,
					Length: metadata.TotalContentLength,
				}
				targetContentLength = parsedRange.Length
			}

			if targetContentLength > int64(pm.concurrentOption.ThresholdSize.Limit) {
				err = pt.GetStorage().UpdateTask(ctx,
					&storage.UpdateTaskRequest{
						PeerTaskMetadata: storage.PeerTaskMetadata{
							PeerID: pt.GetPeerID(),
							TaskID: pt.GetTaskID(),
						},
						ContentLength: targetContentLength,
						TotalPieces:   pt.GetTotalPieces(),
						Header:        &metadata.Header,
					})
				if err != nil {
					log.Errorf("update task error: %s", err)
					return err
				}
				// use concurrent piece download mode
				return pm.concurrentDownloadSource(ctx, pt, peerTaskRequest, parsedRange, 0)
			}
		}
	}

singleDownload:
	// 1. download pieces from source
	response, err := source.Download(backSourceRequest)
	// TODO update expire info
	if err != nil {
		return err
	}
	err = response.Validate()
	if err != nil {
		log.Errorf("back source status code %d/%s", response.StatusCode, response.Status)
		// convert error details to status
		st := status.Newf(codes.Aborted,
			fmt.Sprintf("source response %d/%s is not valid", response.StatusCode, response.Status))
		hdr := map[string]string{}
		for k, v := range response.Header {
			if len(v) > 0 {
				hdr[k] = response.Header.Get(k)
			}
		}
		srcErr := &errordetailsv1.SourceError{
			Temporary: response.Temporary,
			Metadata: &commonv1.ExtendAttribute{
				Header:     hdr,
				StatusCode: int32(response.StatusCode),
				Status:     response.Status,
			},
		}
		st, err = st.WithDetails(srcErr)
		if err != nil {
			log.Errorf("convert source error details error: %s", err.Error())
			return err
		}
		pt.UpdateSourceErrorStatus(st)
		return &backSourceError{
			err: st.Err(),
			st:  st,
		}
	}
	contentLength := response.ContentLength
	// we must calculate piece size
	pieceSize := pm.computePieceSize(contentLength)
	if contentLength < 0 {
		log.Warnf("can not get content length for %s", peerTaskRequest.Url)
	} else {
		log.Debugf("back source content length: %d", contentLength)

		pt.SetContentLength(contentLength)
		pt.SetTotalPieces(util.ComputePieceCount(contentLength, pieceSize))

		err = pt.GetStorage().UpdateTask(ctx,
			&storage.UpdateTaskRequest{
				PeerTaskMetadata: storage.PeerTaskMetadata{
					PeerID: pt.GetPeerID(),
					TaskID: pt.GetTaskID(),
				},
				ContentLength: contentLength,
				TotalPieces:   pt.GetTotalPieces(),
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
		if len(peerTaskRequest.UrlMeta.Digest) != 0 {
			d, err := digest.Parse(peerTaskRequest.UrlMeta.Digest)
			if err != nil {
				return err
			}

			reader, err = digest.NewReader(d.Algorithm, response.Body, digest.WithEncoded(d.Encoded), digest.WithLogger(pt.Log()))
			if err != nil {
				log.Errorf("init digest reader error: %s", err.Error())
				return err
			}
		} else {
			reader, err = digest.NewReader(digest.AlgorithmMD5, response.Body, digest.WithLogger(pt.Log()))
			if err != nil {
				log.Errorf("init digest reader error: %s", err.Error())
				return err
			}
		}
	}

	// 2. save to storage
	// handle resource which content length is unknown
	if contentLength < 0 {
		return pm.downloadUnknownLengthSource(pt, pieceSize, reader)
	}

	if parsedRange != nil {
		parsedRange.Length = contentLength
		log.Infof("update range length: %d", parsedRange.Length)
	}

	return pm.downloadKnownLengthSource(ctx, pt, contentLength, pieceSize, reader, response, peerTaskRequest, parsedRange, supportConcurrent)
}

func (pm *pieceManager) downloadKnownLengthSource(ctx context.Context, pt Task, contentLength int64, pieceSize uint32, reader io.Reader, response *source.Response, peerTaskRequest *schedulerv1.PeerTaskRequest, parsedRange *nethttp.Range, supportConcurrent bool) error {
	log := pt.Log()
	maxPieceNum := pt.GetTotalPieces()
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
			piece: &commonv1.PieceInfo{
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
		if supportConcurrent && pieceNum+2 < maxPieceNum {
			// the time unit of FinishTime and BeginTime is ns
			speed := float64(pieceSize) / float64((result.FinishTime-result.BeginTime)/1000000)
			if speed < float64(pm.concurrentOption.ThresholdSpeed) {
				response.Body.Close()
				return pm.concurrentDownloadSource(ctx, pt, peerTaskRequest, parsedRange, pieceNum+1)
			}
		}
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
			piece: &commonv1.PieceInfo{
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
		reader, _ = digest.NewReader(digest.AlgorithmMD5, r, digest.WithLogger(log))
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
				Range: nethttp.Range{
					Start:  int64(pieceOffset),
					Length: int64(pieceSize),
				},
			},
			Reader:          reader,
			NeedGenMetadata: isLastPiece,
		})
	if err != nil {
		msg := fmt.Sprintf("put piece of task %s to storage failed, piece num: %d, wrote: %d, error: %s", ptm.TaskID, pieceNum, n, err)
		return n, errors.New(msg)
	}
	return n, nil
}

func (pm *pieceManager) ImportFile(ctx context.Context, ptm storage.PeerTaskMetadata, tsd storage.TaskStorageDriver, req *dfdaemonv1.ImportTaskRequest) (err error) {
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
	maxPieceNum := util.ComputePieceCount(contentLength, pieceSize)

	file, err := os.Open(req.Path)
	if err != nil {
		msg := fmt.Sprintf("open file %s failed: %s", req.Path, err)
		log.Error(msg)
		return errors.New(msg)
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

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

func (pm *pieceManager) Import(ctx context.Context, ptm storage.PeerTaskMetadata, tsd storage.TaskStorageDriver, contentLength int64, reader io.Reader) error {
	log := logger.WithTaskAndPeerID(ptm.TaskID, ptm.PeerID)
	pieceSize := pm.computePieceSize(contentLength)
	maxPieceNum := util.ComputePieceCount(contentLength, pieceSize)

	for pieceNum := int32(0); pieceNum < maxPieceNum; pieceNum++ {
		size := pieceSize
		offset := uint64(pieceNum) * uint64(pieceSize)

		// Calculate piece size for last piece.
		if contentLength > 0 && int64(offset)+int64(size) > contentLength {
			size = uint32(contentLength - int64(offset))
		}

		log.Debugf("import piece %d", pieceNum)
		n, err := pm.processPieceFromFile(ctx, ptm, tsd, reader, pieceNum, offset, size, func(int64) (int32, int64, bool) {
			return maxPieceNum, contentLength, pieceNum == maxPieceNum-1
		})
		if err != nil {
			log.Errorf("import piece %d error: %s", pieceNum, err)
			return err
		}

		if n != int64(size) {
			log.Errorf("import piece %d size not match, desired: %d, actual: %d", pieceNum, size, n)
			return storage.ErrShortRead
		}
	}

	// Update task with length and piece count.
	if err := tsd.UpdateTask(ctx, &storage.UpdateTaskRequest{
		PeerTaskMetadata: ptm,
		ContentLength:    contentLength,
		TotalPieces:      maxPieceNum,
	}); err != nil {
		msg := fmt.Sprintf("update task failed: %s", err)
		log.Error(msg)
		return errors.New(msg)
	}

	// Store metadata.
	if err := tsd.Store(ctx, &storage.StoreRequest{
		CommonTaskRequest: storage.CommonTaskRequest{
			PeerID: ptm.PeerID,
			TaskID: ptm.TaskID,
		},
		MetadataOnly:  true,
		StoreDataOnly: false,
	}); err != nil {
		msg := fmt.Sprintf("store task failed: %s", err)
		log.Error(msg)
		return errors.New(msg)
	}

	return nil
}

func (pm *pieceManager) concurrentDownloadSource(ctx context.Context, pt Task, peerTaskRequest *schedulerv1.PeerTaskRequest, parsedRange *nethttp.Range, continuePieceNum int32) error {
	// parsedRange is always exist
	pieceSize := pm.computePieceSize(parsedRange.Length)
	pieceCount := util.ComputePieceCount(parsedRange.Length, pieceSize)

	pt.SetContentLength(parsedRange.Length)
	pt.SetTotalPieces(pieceCount)

	pieceCountToDownload := pieceCount - continuePieceNum

	con := pm.concurrentOption.GoroutineCount
	// Fix int overflow
	if int(pieceCountToDownload) > 0 && int(pieceCountToDownload) < con {
		con = int(pieceCountToDownload)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return pm.concurrentDownloadSourceByPieceGroup(ctx, pt, peerTaskRequest, parsedRange, continuePieceNum, pieceCount, pieceCountToDownload, con, pieceSize, cancel)
}

func (pm *pieceManager) concurrentDownloadSourceByPieceGroup(
	ctx context.Context, pt Task, peerTaskRequest *schedulerv1.PeerTaskRequest,
	parsedRange *nethttp.Range, startPieceNum int32, pieceCount int32, pieceCountToDownload int32,
	con int, pieceSize uint32, cancel context.CancelFunc) error {
	log := pt.Log()
	log.Infof("start concurrentDownloadSourceByPieceGroup, startPieceNum: %d, pieceCount: %d, pieceCountToDownload: %d, con: %d, pieceSize: %d",
		startPieceNum, pieceCount, pieceCountToDownload, con, pieceSize)

	var downloadError atomic.Value
	downloadedPieces := mapset.NewSet[int32]()

	wg := sync.WaitGroup{}
	wg.Add(con)

	minPieceCountPerGroup := pieceCountToDownload / int32(con)
	reminderPieces := pieceCountToDownload % int32(con)

	// piece group eg:
	// con = 4, piece = 5:
	//   worker 0: 2
	//   worker 1: 1
	//   worker 2: 1
	//   worker 3: 1
	//   worker 4: 1
	for i := int32(0); i < int32(con); i++ {
		go func(i int32) {
			pg := newPieceGroup(i, reminderPieces, startPieceNum, minPieceCountPerGroup, pieceSize, parsedRange)
			log.Infof("concurrent worker %d start to download piece %d-%d, byte %d-%d", i, pg.start, pg.end, pg.startByte, pg.endByte)
			_, _, retryErr := retry.Run(ctx,
				pm.concurrentOption.InitBackoff,
				pm.concurrentOption.MaxBackoff,
				pm.concurrentOption.MaxAttempts,
				func() (data any, cancel bool, err error) {
					err = pm.downloadPieceGroupFromSource(ctx, pt, log,
						peerTaskRequest, pg, pieceCount, pieceCountToDownload, downloadedPieces)
					return nil, errors.Is(err, context.Canceled), err
				})
			if retryErr != nil {
				// download piece error after many retry, cancel task
				cancel()
				downloadError.Store(&backSourceError{err: retryErr})
				log.Infof("concurrent worker %d failed to download piece group after %d retries, last error: %s",
					i, pm.concurrentOption.MaxAttempts, retryErr.Error())
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	// check error
	if downloadError.Load() != nil {
		return downloadError.Load().(*backSourceError).err
	}

	return nil
}

type pieceGroup struct {
	start, end         int32
	startByte, endByte int64
	// store original task metadata
	pieceSize   uint32
	parsedRange *nethttp.Range
}

func newPieceGroup(i int32, reminderPieces int32, startPieceNum int32, minPieceCountPerGroup int32, pieceSize uint32, parsedRange *nethttp.Range) *pieceGroup {
	var (
		start int32
		end   int32
	)

	if i < reminderPieces {
		start = i*minPieceCountPerGroup + i
		end = start + minPieceCountPerGroup
	} else {
		start = i*minPieceCountPerGroup + reminderPieces
		end = start + minPieceCountPerGroup - 1
	}

	// adjust by startPieceNum
	start += startPieceNum
	end += startPieceNum

	// calculate piece group first and last range byte with parsedRange.Start
	startByte := int64(start) * int64(pieceSize)
	endByte := int64(end+1)*int64(pieceSize) - 1
	if endByte > parsedRange.Length-1 {
		endByte = parsedRange.Length - 1
	}

	// adjust by range start
	startByte += parsedRange.Start
	endByte += parsedRange.Start

	pg := &pieceGroup{
		start:       start,
		end:         end,
		startByte:   startByte,
		endByte:     endByte,
		pieceSize:   pieceSize,
		parsedRange: parsedRange,
	}
	return pg
}

func (pm *pieceManager) concurrentDownloadSourceByPiece(
	ctx context.Context, pt Task, peerTaskRequest *schedulerv1.PeerTaskRequest,
	parsedRange *nethttp.Range, startPieceNum int32, pieceCount int32, pieceCountToDownload int32,
	con int, pieceSize uint32, cancel context.CancelFunc) error {

	log := pt.Log()
	var downloadError atomic.Value
	var pieceCh = make(chan int32, con)

	wg := sync.WaitGroup{}
	wg.Add(int(pieceCountToDownload))

	downloadedPieceCount := atomic.NewInt32(startPieceNum)

	for i := 0; i < con; i++ {
		go func(i int) {
			for {
				select {
				case <-ctx.Done():
					log.Warnf("concurrent worker %d context done due to %s", i, ctx.Err())
					return
				case pieceNum, ok := <-pieceCh:
					if !ok {
						log.Debugf("concurrent worker %d exit", i)
						return
					}
					log.Infof("concurrent worker %d start to download piece %d", i, pieceNum)
					_, _, retryErr := retry.Run(ctx,
						pm.concurrentOption.InitBackoff,
						pm.concurrentOption.MaxBackoff,
						pm.concurrentOption.MaxAttempts,
						func() (data any, cancel bool, err error) {
							err = pm.downloadPieceFromSource(ctx, pt, log,
								peerTaskRequest, pieceSize, pieceNum,
								parsedRange, pieceCount, downloadedPieceCount)
							return nil, err == context.Canceled, err
						})
					if retryErr != nil {
						// download piece error after many retry, cancel task
						cancel()
						downloadError.Store(&backSourceError{err: retryErr})
						log.Infof("concurrent worker %d failed to download piece %d after %d retries, last error: %s",
							i, pieceNum, pm.concurrentOption.MaxAttempts, retryErr.Error())
					}
					wg.Done()
				}
			}
		}(i)
	}

	for i := startPieceNum; i < pieceCount; i++ {
		select {
		case <-ctx.Done():
			log.Warnf("context cancelled")
			if downloadError.Load() != nil {
				return downloadError.Load().(*backSourceError).err
			}
			return ctx.Err()
		case pieceCh <- i:
		}
	}

	// wait all pieces processed
	// the downloadError will be updated if there is any errors before wg.Wait() returned
	wg.Wait()

	// let all working goroutines exit
	close(pieceCh)

	// check error
	if downloadError.Load() != nil {
		return downloadError.Load().(*backSourceError).err
	}

	return nil
}

func (pm *pieceManager) downloadPieceFromSource(ctx context.Context,
	pt Task, log *logger.SugaredLoggerOnWith,
	peerTaskRequest *schedulerv1.PeerTaskRequest,
	pieceSize uint32, pieceNum int32,
	parsedRange *nethttp.Range,
	totalPieceCount int32,
	downloadedPieceCount *atomic.Int32) error {
	backSourceRequest, err := source.NewRequestWithContext(ctx, peerTaskRequest.Url, peerTaskRequest.UrlMeta.Header)
	if err != nil {
		log.Errorf("build piece %d back source request error: %s", pieceNum, err)
		return err
	}
	size := pieceSize
	offset := uint64(pieceNum) * uint64(pieceSize)
	// calculate piece size for last piece
	if int64(offset)+int64(size) > parsedRange.Length {
		size = uint32(parsedRange.Length - int64(offset))
	}

	// offset is the position for current peer task, if this peer task already has range
	// we need add the start to the offset when download from source
	rg := fmt.Sprintf("%d-%d", offset+uint64(parsedRange.Start), offset+uint64(parsedRange.Start)+uint64(size)-1)
	// FIXME refactor source package, normal Range header is enough
	backSourceRequest.Header.Set(source.Range, rg)
	backSourceRequest.Header.Set(headers.Range, "bytes="+rg)
	log.Debugf("piece %d back source header: %#v", pieceNum, backSourceRequest.Header)

	response, err := source.Download(backSourceRequest)
	if err != nil {
		log.Errorf("piece %d back source response error: %s", pieceNum, err)
		return err
	}
	defer response.Body.Close()

	err = response.Validate()
	if err != nil {
		log.Errorf("piece %d back source response validate error: %s", pieceNum, err)
		return err
	}

	log.Debugf("piece %d back source response ok, offset: %d, size: %d", pieceNum, offset, size)
	result, md5, err := pm.processPieceFromSource(
		pt, response.Body, parsedRange.Length, pieceNum, offset, size,
		func(int64) (int32, int64, bool) {
			downloadedPieceCount.Inc()
			return totalPieceCount, parsedRange.Length, downloadedPieceCount.Load() == totalPieceCount
		})
	request := &DownloadPieceRequest{
		TaskID: pt.GetTaskID(),
		PeerID: pt.GetPeerID(),
		piece: &commonv1.PieceInfo{
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
	return nil
}

func (pm *pieceManager) downloadPieceGroupFromSource(ctx context.Context,
	pt Task, log *logger.SugaredLoggerOnWith,
	peerTaskRequest *schedulerv1.PeerTaskRequest,
	pg *pieceGroup,
	totalPieceCount int32,
	totalPieceCountToDownload int32,
	downloadedPieces mapset.Set[int32]) error {

	backSourceRequest, err := source.NewRequestWithContext(ctx, peerTaskRequest.Url, peerTaskRequest.UrlMeta.Header)
	if err != nil {
		log.Errorf("build piece %d-%d back source request error: %s", pg.start, pg.end, err)
		return err
	}

	pieceGroupRange := fmt.Sprintf("%d-%d", pg.startByte, pg.endByte)
	// FIXME refactor source package, normal Range header is enough
	backSourceRequest.Header.Set(source.Range, pieceGroupRange)
	backSourceRequest.Header.Set(headers.Range, "bytes="+pieceGroupRange)

	log.Debugf("piece %d-%d back source header: %#v", pg.start, pg.end, backSourceRequest.Header)

	response, err := source.Download(backSourceRequest)
	if err != nil {
		log.Errorf("piece %d-%d back source response error: %s", pg.start, pg.end, err)
		return err
	}
	defer response.Body.Close()

	err = response.Validate()
	if err != nil {
		log.Errorf("piece %d-%d back source response validate error: %s", pg.start, pg.end, err)
		return err
	}

	log.Debugf("piece %d-%d back source response ok", pg.start, pg.end)

	for i := pg.start; i <= pg.end; i++ {
		pieceNum := i
		offset := uint64(pg.startByte) + uint64(i-pg.start)*uint64(pg.pieceSize)
		size := pg.pieceSize
		// update last piece size
		if offset+uint64(size)-1 > uint64(pg.endByte) {
			size = uint32(uint64(pg.endByte) + 1 - offset)
		}

		result, md5, err := pm.processPieceFromSource(
			pt, response.Body, pg.parsedRange.Length, pieceNum, offset-uint64(pg.parsedRange.Start), size,
			func(int64) (int32, int64, bool) {
				downloadedPieces.Add(pieceNum)
				return totalPieceCount, pg.parsedRange.Length, downloadedPieces.Cardinality() == int(totalPieceCountToDownload)
			})
		request := &DownloadPieceRequest{
			TaskID: pt.GetTaskID(),
			PeerID: pt.GetPeerID(),
			piece: &commonv1.PieceInfo{
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

		log.Debugf("piece %d done", pieceNum)
	}
	return nil
}
