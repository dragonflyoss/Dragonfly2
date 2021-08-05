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

	"github.com/go-http-utils/headers"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/internal/dfcodes"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
)

// StreamPeerTask represents a peer task with stream io for reading directly without once more disk io
type StreamPeerTask interface {
	Task
	// Start start the special peer task, return a io.Reader for stream io
	// when all data transferred, reader return a io.EOF
	// attribute stands some extra data, like HTTP response Header
	Start(ctx context.Context) (reader io.Reader, attribute map[string]string, err error)
}

type streamPeerTask struct {
	peerTask
	successPieceCh chan int32
}

var _ StreamPeerTask = (*streamPeerTask)(nil)

func newStreamPeerTask(ctx context.Context,
	host *scheduler.PeerHost,
	pieceManager PieceManager,
	request *scheduler.PeerTaskRequest,
	schedulerClient schedulerclient.SchedulerClient,
	schedulerOption config.SchedulerOption,
	perPeerRateLimit rate.Limit) (context.Context, *streamPeerTask, *TinyData, error) {
	ctx, span := tracer.Start(ctx, config.SpanStreamPeerTask, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(config.AttributePeerHost.String(host.Uuid))
	span.SetAttributes(semconv.NetHostIPKey.String(host.Ip))
	span.SetAttributes(config.AttributePeerID.String(request.PeerId))
	span.SetAttributes(semconv.HTTPURLKey.String(request.Url))

	logger.Debugf("request overview, pid: %s, url: %s, filter: %s, meta: %s, tag: %s",
		request.PeerId, request.Url, request.UrlMeta.Filter, request.UrlMeta, request.UrlMeta.Tag)
	// trace register
	_, regSpan := tracer.Start(ctx, config.SpanRegisterTask)
	result, err := schedulerClient.RegisterPeerTask(ctx, request)
	logger.Infof("step 1: peer %s start to register", request.PeerId)
	regSpan.RecordError(err)
	regSpan.End()

	var needBackSource bool
	if err != nil {
		needBackSource = true
		// can not detect source or scheduler error, create a new dummy scheduler client
		schedulerClient = &dummySchedulerClient{}
		logger.Warnf("register peer task failed: %s, peer id: %s, try to back source", err, request.PeerId)
	}
	if result == nil {
		defer span.End()
		span.RecordError(err)
		err = errors.Errorf("empty schedule result")
		return ctx, nil, nil, err
	}
	span.SetAttributes(config.AttributeTaskID.String(result.TaskId))
	logger.Infof("register task success, task id: %s, peer id: %s, SizeScope: %s",
		result.TaskId, request.PeerId, base.SizeScope_name[int32(result.SizeScope)])

	var singlePiece *scheduler.SinglePiece
	if !needBackSource {
		switch result.SizeScope {
		case base.SizeScope_SMALL:
			span.SetAttributes(config.AttributePeerTaskSizeScope.String("small"))
			logger.Debugf("%s/%s size scope: small", result.TaskId, request.PeerId)
			if piece, ok := result.DirectPiece.(*scheduler.RegisterResult_SinglePiece); ok {
				singlePiece = piece.SinglePiece
			}
		case base.SizeScope_TINY:
			defer span.End()
			span.SetAttributes(config.AttributePeerTaskSizeScope.String("tiny"))
			logger.Debugf("%s/%s size scope: tiny", result.TaskId, request.PeerId)
			if piece, ok := result.DirectPiece.(*scheduler.RegisterResult_PieceContent); ok {
				return ctx, nil, &TinyData{
					span:    span,
					TaskID:  result.TaskId,
					PeerID:  request.PeerId,
					Content: piece.PieceContent,
				}, nil
			}
			err = errors.Errorf("scheduler return tiny piece but can not parse piece content")
			span.RecordError(err)
			return ctx, nil, nil, err
		case base.SizeScope_NORMAL:
			span.SetAttributes(config.AttributePeerTaskSizeScope.String("normal"))
			logger.Debugf("%s/%s size scope: normal", result.TaskId, request.PeerId)
		}
	}

	peerPacketStream, err := schedulerClient.ReportPieceResult(ctx, result.TaskId, request)
	logger.Infof("step 2: start report peer %s piece result", request.PeerId)
	if err != nil {
		defer span.End()
		span.RecordError(err)
		return ctx, nil, nil, err
	}
	var limiter *rate.Limiter
	if perPeerRateLimit > 0 {
		limiter = rate.NewLimiter(perPeerRateLimit, int(perPeerRateLimit))
	}
	return ctx, &streamPeerTask{
		peerTask: peerTask{
			ctx:                 ctx,
			host:                host,
			needBackSource:      needBackSource,
			request:             request,
			peerPacketStream:    peerPacketStream,
			pieceManager:        pieceManager,
			peerPacketReady:     make(chan bool, 1),
			peerID:              request.PeerId,
			taskID:              result.TaskId,
			singlePiece:         singlePiece,
			done:                make(chan struct{}),
			span:                span,
			readyPieces:         NewBitmap(),
			requestedPieces:     NewBitmap(),
			failedPieceCh:       make(chan int32, config.DefaultPieceChanSize),
			failedReason:        failedReasonNotSet,
			failedCode:          dfcodes.UnknownError,
			contentLength:       -1,
			totalPiece:          -1,
			schedulerOption:     schedulerOption,
			schedulerClient:     schedulerClient,
			limiter:             limiter,
			completedLength:     atomic.NewInt64(0),
			usedTraffic:         atomic.NewInt64(0),
			SugaredLoggerOnWith: logger.With("peer", request.PeerId, "task", result.TaskId, "component", "streamPeerTask"),
		},
		successPieceCh: make(chan int32),
	}, nil, nil
}

func (s *streamPeerTask) ReportPieceResult(piece *base.PieceInfo, pieceResult *scheduler.PieceResult) error {
	defer s.recoverFromPanic()
	// retry failed piece
	if !pieceResult.Success {
		_ = s.peerPacketStream.Send(pieceResult)
		s.failedPieceCh <- pieceResult.PieceNum
		return nil
	}

	s.lock.Lock()
	if s.readyPieces.IsSet(pieceResult.PieceNum) {
		s.lock.Unlock()
		s.Warnf("piece %d is already reported, skipped", pieceResult.PieceNum)
		return nil
	}
	// mark piece processed
	s.readyPieces.Set(pieceResult.PieceNum)
	s.completedLength.Add(int64(piece.RangeSize))
	s.lock.Unlock()

	pieceResult.FinishedCount = s.readyPieces.Settled()
	_ = s.peerPacketStream.Send(pieceResult)
	s.successPieceCh <- piece.PieceNum
	s.Debugf("success piece %d sent", piece.PieceNum)
	select {
	case <-s.ctx.Done():
		s.Warnf("peer task context done due to %s", s.ctx.Err())
		return s.ctx.Err()
	default:
	}

	if !s.isCompleted() {
		return nil
	}

	return s.finish()
}

func (s *streamPeerTask) Start(ctx context.Context) (io.Reader, map[string]string, error) {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.backSourceFunc = s.backSource
	if s.needBackSource {
		go s.backSource()
	} else {
		s.pullPieces(s, s.cleanUnfinished)
	}

	// wait first piece to get content length and attribute (eg, response header for http/https)
	var firstPiece int32
	select {
	case <-s.ctx.Done():
		var err error
		if s.failedReason != failedReasonNotSet {
			err = errors.Errorf(s.failedReason)
		} else {
			err = errors.Errorf("ctx.PeerTaskDone due to: %s", s.ctx.Err())
		}
		s.Errorf("%s", err)
		s.span.RecordError(err)
		s.span.End()
		attr := map[string]string{}
		attr[config.HeaderDragonflyTask] = s.taskID
		attr[config.HeaderDragonflyPeer] = s.peerID
		return nil, attr, err
	case <-s.done:
		var err error
		if s.failedReason != failedReasonNotSet {
			err = errors.Errorf(s.failedReason)
		} else {
			err = errors.Errorf("stream peer task early done")
		}
		s.Errorf("%s", err)
		s.span.RecordError(err)
		s.span.End()
		attr := map[string]string{}
		attr[config.HeaderDragonflyTask] = s.taskID
		attr[config.HeaderDragonflyPeer] = s.peerID
		return nil, attr, err
	case first := <-s.successPieceCh:
		//if !ok {
		//	s.Warnf("successPieceCh closed unexpect")
		//	return nil, nil, errors.NewDaemonConfig("early done")
		//}
		firstPiece = first
	}

	pr, pw := io.Pipe()
	attr := map[string]string{}
	var reader io.Reader = pr
	var writer io.Writer = pw
	if s.contentLength != -1 {
		attr[headers.ContentLength] = fmt.Sprintf("%d", s.contentLength)
	} else {
		attr[headers.TransferEncoding] = "chunked"
	}
	attr[config.HeaderDragonflyTask] = s.taskID
	attr[config.HeaderDragonflyPeer] = s.peerID

	go func(first int32) {
		defer func() {
			s.cancel()
			s.span.End()
		}()
		var (
			desired int32
			cur     int32
			wrote   int64
			err     error
			//ok      bool
			cache = make(map[int32]bool)
		)
		// update first piece to cache and check cur with desired
		cache[first] = true
		cur = first
		for {
			if desired == cur {
				for {
					delete(cache, desired)
					_, span := tracer.Start(s.ctx, config.SpanWriteBackPiece)
					span.SetAttributes(config.AttributePiece.Int(int(desired)))
					wrote, err = s.writeTo(writer, desired)
					if err != nil {
						span.RecordError(err)
						span.End()
						s.Errorf("write to pipe error: %s", err)
						_ = pw.CloseWithError(err)
						return
					}
					span.SetAttributes(config.AttributePieceSize.Int(int(wrote)))
					s.Debugf("wrote piece %d to pipe, size %d", desired, wrote)
					span.End()
					desired++
					cached := cache[desired]
					if !cached {
						break
					}
				}
			} else {
				// not desired piece, cache it
				cache[cur] = true
				if cur < desired {
					s.Warnf("piece number should be equal or greater than %d, received piece number: %d", desired, cur)
				}
			}

			select {
			case <-s.ctx.Done():
				s.Errorf("ctx.PeerTaskDone due to: %s", s.ctx.Err())
				s.span.RecordError(s.ctx.Err())
				if err := pw.CloseWithError(s.ctx.Err()); err != nil {
					s.Errorf("CloseWithError failed: %s", err)
				}
				return
			case <-s.done:
				for {
					// all data is wrote to local storage, and all data is wrote to pipe write
					if s.readyPieces.Settled() == desired {
						pw.Close()
						return
					}
					_, span := tracer.Start(s.ctx, config.SpanWriteBackPiece)
					span.SetAttributes(config.AttributePiece.Int(int(desired)))
					wrote, err = s.writeTo(pw, desired)
					if err != nil {
						span.RecordError(err)
						span.End()
						s.span.RecordError(err)
						s.Errorf("write to pipe error: %s", err)
						_ = pw.CloseWithError(err)
						return
					}
					span.SetAttributes(config.AttributePieceSize.Int(int(wrote)))
					span.End()
					s.Debugf("wrote piece %d to pipe, size %d", desired, wrote)
					desired++
				}
			case cur = <-s.successPieceCh:
				continue
			}
		}
	}(firstPiece)

	return reader, attr, nil
}

func (s *streamPeerTask) finish() error {
	// send last progress
	s.once.Do(func() {
		// send EOF piece result to scheduler
		_ = s.peerPacketStream.Send(
			scheduler.NewEndPieceResult(s.taskID, s.peerID, s.readyPieces.Settled()))
		s.Debugf("end piece result sent")
		close(s.done)
		//close(s.successPieceCh)
		if err := s.callback.Done(s); err != nil {
			s.span.RecordError(err)
			s.Errorf("done callback error: %s", err)
		}
		s.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
	})
	return nil
}

func (s *streamPeerTask) cleanUnfinished() {
	// send last progress
	s.once.Do(func() {
		// send EOF piece result to scheduler
		_ = s.peerPacketStream.Send(
			scheduler.NewEndPieceResult(s.taskID, s.peerID, s.readyPieces.Settled()))
		s.Debugf("end piece result sent")
		close(s.done)
		//close(s.successPieceCh)
		if err := s.callback.Fail(s, s.failedCode, s.failedReason); err != nil {
			s.span.RecordError(err)
			s.Errorf("fail callback error: %s", err)
		}
		s.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		s.span.SetAttributes(config.AttributePeerTaskCode.Int(int(s.failedCode)))
		s.span.SetAttributes(config.AttributePeerTaskMessage.String(s.failedReason))
	})
}

func (s *streamPeerTask) SetContentLength(i int64) error {
	s.contentLength = i
	if !s.isCompleted() {
		return nil
	}
	return s.finish()
}

func (s *streamPeerTask) writeTo(w io.Writer, pieceNum int32) (int64, error) {
	pr, pc, err := s.pieceManager.ReadPiece(s.ctx, &storage.ReadPieceRequest{
		PeerTaskMetaData: storage.PeerTaskMetaData{
			PeerID: s.peerID,
			TaskID: s.taskID,
		},
		PieceMetaData: storage.PieceMetaData{
			Num: pieceNum,
		},
	})
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(w, pr)
	if err != nil {
		pc.Close()
		return n, err
	}
	return n, pc.Close()
}

func (s *streamPeerTask) backSource() {
	s.contentLength = -1
	_ = s.callback.Init(s)
	err := s.pieceManager.DownloadSource(s.ctx, s, s.request)
	if err != nil {
		s.Errorf("download from source error: %s", err)
		s.failedReason = err.Error()
		s.cleanUnfinished()
		return
	}
	s.Debugf("download from source ok")
	_ = s.finish()
	return
}
