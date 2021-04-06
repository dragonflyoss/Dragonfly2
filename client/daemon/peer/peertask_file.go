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
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
)

type FilePeerTaskRequest struct {
	scheduler.PeerTaskRequest
	Output string
}

// FilePeerTask represents a peer task to download a file
type FilePeerTask interface {
	PeerTask
	// Start start the special peer task, return a *PeerTaskProgress channel for updating download progress
	Start(ctx context.Context) (chan *PeerTaskProgress, error)
}

type filePeerTask struct {
	peerTask
	// progressCh holds progress status
	progressCh     chan *PeerTaskProgress
	progressStopCh chan bool
}

type ProgressState struct {
	Success bool
	Code    base.Code
	Msg     string
}

type PeerTaskProgress struct {
	State           *ProgressState
	TaskId          string
	PeerID          string
	ContentLength   int64
	CompletedLength int64
	PeerTaskDone    bool
	DoneCallback    func()
}

func newFilePeerTask(ctx context.Context,
	host *scheduler.PeerHost,
	pieceManager PieceManager,
	request *scheduler.PeerTaskRequest,
	schedulerClient schedulerclient.SchedulerClient,
	schedulerOption config.SchedulerOption) (context.Context, FilePeerTask, *TinyData, error) {
	ctx, span := tracer.Start(ctx, "file-peer-task", trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(attribute.String("host", host.Uuid))
	span.SetAttributes(attribute.String("peer", request.PeerId))
	span.SetAttributes(semconv.HTTPURLKey.String(request.Url))
	result, err := schedulerClient.RegisterPeerTask(ctx, request)
	var backSource bool
	if err != nil {
		// check if it is back source error
		if de, ok := err.(*dferrors.DfError); ok && de.Code == dfcodes.SchedNeedBackSource {
			backSource = true
		}
		// not back source
		if !backSource {
			span.RecordError(err)
			span.End()
			logger.Errorf("register peer task failed: %s, peer id: %s", err, request.PeerId)
			return ctx, nil, nil, err
		}
	}
	if result == nil {
		defer span.End()
		span.RecordError(err)
		err = errors.Errorf("empty schedule result")
		return ctx, nil, nil, err
	}
	span.SetAttributes(attribute.String("task", result.TaskId))

	var singlePiece *scheduler.SinglePiece
	if !backSource {
		switch result.SizeScope {
		case base.SizeScope_SMALL:
			span.SetAttributes(attribute.String("size", "small"))
			logger.Debugf("%s/%s size scope: small", result.TaskId, request.PeerId)
			if piece, ok := result.DirectPiece.(*scheduler.RegisterResult_SinglePiece); ok {
				singlePiece = piece.SinglePiece
			}
		case base.SizeScope_TINY:
			defer span.End()
			span.SetAttributes(attribute.String("size", "tiny"))
			logger.Debugf("%s/%s size scope: tiny", result.TaskId, request.PeerId)
			if piece, ok := result.DirectPiece.(*scheduler.RegisterResult_PieceContent); ok {
				return ctx, nil, &TinyData{
					TaskId:  result.TaskId,
					PeerID:  request.PeerId,
					Content: piece.PieceContent,
				}, nil
			}
			err = errors.Errorf("scheduler return tiny piece but can not parse piece content")
			span.RecordError(err)
			return ctx, nil, nil, err
		case base.SizeScope_NORMAL:
			span.SetAttributes(attribute.String("size", "normal"))
			logger.Debugf("%s/%s size scope: normal", result.TaskId, request.PeerId)
		}
	}

	peerPacketStream, err := schedulerClient.ReportPieceResult(ctx, result.TaskId, request)
	if err != nil {
		defer span.End()
		span.RecordError(err)
		return ctx, nil, nil, err
	}
	logger.Infof("register task success, task id: %s, peer id: %s, SizeScope: %s",
		result.TaskId, request.PeerId, base.SizeScope_name[int32(result.SizeScope)])
	return ctx, &filePeerTask{
		progressCh:     make(chan *PeerTaskProgress),
		progressStopCh: make(chan bool),
		peerTask: peerTask{
			host:             host,
			backSource:       backSource,
			request:          request,
			peerPacketStream: peerPacketStream,
			pieceManager:     pieceManager,
			peerPacketReady:  make(chan bool),
			peerId:           request.PeerId,
			taskId:           result.TaskId,
			singlePiece:      singlePiece,
			done:             make(chan struct{}),
			span:             span,
			once:             sync.Once{},
			readyPieces:      NewBitmap(),
			requestedPieces:  NewBitmap(),
			lock:             &sync.Mutex{},
			failedPieceCh:    make(chan int32, 4),
			failedReason:     "unknown",
			failedCode:       dfcodes.UnknownError,
			contentLength:    -1,
			totalPiece:       -1,
			schedulerOption:  schedulerOption,

			SugaredLoggerOnWith: logger.With("peer", request.PeerId, "task", result.TaskId, "component", "filePeerTask"),
		},
	}, nil, nil
}

func (pt *filePeerTask) Start(ctx context.Context) (chan *PeerTaskProgress, error) {
	pt.ctx, pt.cancel = context.WithCancel(ctx)
	if pt.backSource {
		pt.contentLength = -1
		_ = pt.callback.Init(pt)
		go func() {
			defer pt.cleanUnfinished()
			err := pt.pieceManager.DownloadSource(ctx, pt, pt.request)
			if err != nil {
				pt.Errorf("download from source error: %s", err)
				return
			}
			pt.Errorf("download from source ok")
			pt.finish()
		}()
		return pt.progressCh, nil
	}

	pt.pullPieces(pt, pt.cleanUnfinished)

	// return a progress channel for request download progress
	return pt.progressCh, nil
}

func (pt *filePeerTask) ReportPieceResult(piece *base.PieceInfo, pieceResult *scheduler.PieceResult) error {
	// goroutine safe for channel and send on closed channel
	defer func() {
		if r := recover(); r != nil {
			pt.Warnf("recover from %s", r)
		}
	}()
	pt.Debugf("report piece %d result, success: %t", piece.PieceNum, pieceResult.Success)

	// retry failed piece
	if !pieceResult.Success {
		pieceResult.FinishedCount = pt.readyPieces.Settled()
		_ = pt.peerPacketStream.Send(pieceResult)
		pt.failedPieceCh <- pieceResult.PieceNum
		pt.Errorf("%d download failed, retry later", piece.PieceNum)
		return nil
	}

	pt.lock.Lock()
	if pt.readyPieces.IsSet(pieceResult.PieceNum) {
		pt.lock.Unlock()
		pt.Warnf("piece %d is already reported, skipped", pieceResult.PieceNum)
		return nil
	}
	// mark piece processed
	pt.readyPieces.Set(pieceResult.PieceNum)
	atomic.AddInt64(&pt.completedLength, int64(piece.RangeSize))
	pt.lock.Unlock()

	pieceResult.FinishedCount = pt.readyPieces.Settled()
	_ = pt.peerPacketStream.Send(pieceResult)
	// send progress first to avoid close channel panic
	p := &PeerTaskProgress{
		State: &ProgressState{
			Success: pieceResult.Success,
			Code:    pieceResult.Code,
			Msg:     "downloading",
		},
		TaskId:          pt.taskId,
		PeerID:          pt.peerId,
		ContentLength:   pt.contentLength,
		CompletedLength: pt.completedLength,
		PeerTaskDone:    false,
	}

	select {
	case <-pt.progressStopCh:
	case pt.progressCh <- p:
		pt.Debugf("progress sent, %d/%d", p.CompletedLength, p.ContentLength)
	case <-pt.ctx.Done():
		pt.Warnf("send progress failed, peer task context done due to %s", pt.ctx.Err())
		return pt.ctx.Err()
	}

	if !pt.isCompleted() {
		return nil
	}

	return pt.finish()
}

func (pt *filePeerTask) finish() error {
	var err error
	// send last progress
	pt.once.Do(func() {
		defer func() {
			if rerr := recover(); rerr != nil {
				pt.Errorf("finish recover from: %s", rerr)
				err = fmt.Errorf("%v", rerr)
			}
			pt.span.End()
		}()
		// send EOF piece result to scheduler
		_ = pt.peerPacketStream.Send(
			scheduler.NewEndPieceResult(pt.taskId, pt.peerId, pt.readyPieces.Settled()))
		pt.Debugf("finish end piece result sent")

		var (
			success = true
			code    = dfcodes.Success
			message = "Success"
		)

		// callback to store data to output
		if err = pt.callback.Done(pt); err != nil {
			pt.Errorf("peer task done callback failed: %s", err)
			success = false
			code = dfcodes.ClientError
			message = err.Error()
		}

		pg := &PeerTaskProgress{
			State: &ProgressState{
				Success: success,
				Code:    code,
				Msg:     message,
			},
			TaskId:          pt.taskId,
			PeerID:          pt.peerId,
			ContentLength:   pt.contentLength,
			CompletedLength: pt.completedLength,
			PeerTaskDone:    true,
			DoneCallback: func() {
				pt.peerTaskDone = true
				close(pt.progressStopCh)
			},
		}

		// wait client received progress
		pt.Infof("try to send finish progress, completed length: %d, state: (%t, %d, %s)",
			pg.CompletedLength, pg.State.Success, pg.State.Code, pg.State.Msg)
		select {
		case pt.progressCh <- pg:
			pt.Infof("finish progress sent")
		case <-pt.ctx.Done():
			pt.Warnf("finish progress sent failed, context done")
		}
		// wait progress stopped
		select {
		case <-pt.progressStopCh:
			pt.Infof("progress stopped")
		case <-pt.ctx.Done():
			if pt.peerTaskDone {
				pt.Debugf("progress stopped and context done")
			} else {
				pt.Warnf("wait progress stopped failed, context done, but progress not stopped")
			}
		}
		pt.Debugf("finished: close done channel")
		close(pt.done)
	})
	return err
}

func (pt *filePeerTask) cleanUnfinished() {
	defer pt.cancel()

	// send last progress
	pt.once.Do(func() {
		defer func() {
			if err := recover(); err != nil {
				pt.Errorf("cleanUnfinished recover from: %s", err)
			}
			pt.span.End()
		}()
		// send EOF piece result to scheduler
		_ = pt.peerPacketStream.Send(
			scheduler.NewEndPieceResult(pt.taskId, pt.peerId, pt.readyPieces.Settled()))
		pt.Debugf("clean up end piece result sent")

		pg := &PeerTaskProgress{
			State: &ProgressState{
				Success: false,
				Code:    pt.failedCode,
				Msg:     pt.failedReason,
			},
			TaskId:          pt.taskId,
			PeerID:          pt.peerId,
			ContentLength:   pt.contentLength,
			CompletedLength: pt.completedLength,
			PeerTaskDone:    true,
			DoneCallback: func() {
				pt.peerTaskDone = true
				close(pt.progressStopCh)
			},
		}

		// wait client received progress
		pt.Infof("try to send unfinished progress, completed length: %d, state: (%t, %d, %s)",
			pg.CompletedLength, pg.State.Success, pg.State.Code, pg.State.Msg)
		select {
		case pt.progressCh <- pg:
			pt.Debugf("unfinished progress sent")
		case <-pt.ctx.Done():
			pt.Debugf("send unfinished progress failed, context done: %v", pt.ctx.Err())
		}
		// wait progress stopped
		select {
		case <-pt.progressStopCh:
			pt.Infof("progress stopped")
		case <-pt.ctx.Done():
			if pt.peerTaskDone {
				pt.Debugf("progress stopped and context done")
			} else {
				pt.Warnf("wait progress stopped failed, context done, but progress not stopped")
			}
		}

		if err := pt.callback.Fail(pt, pt.failedCode, pt.failedReason); err != nil {
			pt.Errorf("peer task fail callback failed: %s", err)
		}

		pt.Debugf("clean unfinished: close done channel")
		close(pt.done)
	})
}

func (pt *filePeerTask) SetContentLength(i int64) error {
	pt.contentLength = i
	if !pt.isCompleted() {
		return errors.New("SetContentLength should call after task completed")
	}

	return pt.finish()
}
