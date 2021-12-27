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
	"sync"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
)

type FilePeerTaskRequest struct {
	scheduler.PeerTaskRequest
	Output            string
	Limit             float64
	DisableBackSource bool
	Pattern           string
	Callsystem        string
}

// FilePeerTask represents a peer task to download a file
type FilePeerTask interface {
	Task
	// Start start the special peer task, return a *FilePeerTaskProgress channel for updating download progress
	Start(ctx context.Context) (chan *FilePeerTaskProgress, error)
}

type filePeerTask struct {
	peerTask
	// progressCh holds progress status
	progressCh     chan *FilePeerTaskProgress
	progressStopCh chan bool

	// disableBackSource indicates not back source when failed
	disableBackSource bool
	pattern           string
	callsystem        string
}

var _ FilePeerTask = (*filePeerTask)(nil)

type ProgressState struct {
	Success bool
	Code    base.Code
	Msg     string
}

type FilePeerTaskProgress struct {
	State           *ProgressState
	TaskID          string
	PeerID          string
	ContentLength   int64
	CompletedLength int64
	PeerTaskDone    bool
	DoneCallback    func()
}

func newFilePeerTask(ctx context.Context,
	host *scheduler.PeerHost,
	pieceManager PieceManager,
	request *FilePeerTaskRequest,
	schedulerClient schedulerclient.SchedulerClient,
	schedulerOption config.SchedulerOption,
	perPeerRateLimit rate.Limit,
	getPiecesMaxRetry int) (context.Context, *filePeerTask, *TinyData, error) {
	metrics.PeerTaskCount.Add(1)
	ctx, span := tracer.Start(ctx, config.SpanFilePeerTask, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(config.AttributePeerHost.String(host.Uuid))
	span.SetAttributes(semconv.NetHostIPKey.String(host.Ip))
	span.SetAttributes(config.AttributePeerID.String(request.PeerId))
	span.SetAttributes(semconv.HTTPURLKey.String(request.Url))

	logger.Infof("request overview, url: %s, filter: %s, meta: %s, biz: %s, peer: %s", request.Url, request.UrlMeta.Filter, request.UrlMeta, request.UrlMeta.Tag, request.PeerId)
	// trace register
	regCtx, regSpan := tracer.Start(ctx, config.SpanRegisterTask)
	logger.Infof("step 1: peer %s start to register", request.PeerId)
	result, err := schedulerClient.RegisterPeerTask(regCtx, &request.PeerTaskRequest)
	regSpan.RecordError(err)
	regSpan.End()

	var needBackSource bool
	if err != nil {
		logger.Errorf("step 1: peer %s register failed: %v", request.PeerId, err)
		if schedulerOption.DisableAutoBackSource {
			logger.Errorf("register peer task failed: %s, peer id: %s, auto back source disabled", err, request.PeerId)
			span.RecordError(err)
			span.End()
			return ctx, nil, nil, err
		}
		needBackSource = true
		// can not detect source or scheduler error, create a new dummy scheduler client
		schedulerClient = &dummySchedulerClient{}
		result = &scheduler.RegisterResult{TaskId: idgen.TaskID(request.Url, request.UrlMeta)}
		logger.Warnf("register peer task failed: %s, peer id: %s, try to back source", err, request.PeerId)
	}

	if result == nil {
		defer span.End()
		span.RecordError(err)
		err = errors.Errorf("step 1: peer register result is nil")
		return ctx, nil, nil, err
	}
	span.SetAttributes(config.AttributeTaskID.String(result.TaskId))
	logger.Infof("step 1: register task success, task id: %s, peer id: %s, SizeScope: %s",
		result.TaskId, request.PeerId, base.SizeScope_name[int32(result.SizeScope)])

	var singlePiece *scheduler.SinglePiece
	if !needBackSource {
		switch result.SizeScope {
		case base.SizeScope_SMALL:
			span.SetAttributes(config.AttributePeerTaskSizeScope.String("small"))
			logger.Infof("%s/%s size scope: small", result.TaskId, request.PeerId)
			if piece, ok := result.DirectPiece.(*scheduler.RegisterResult_SinglePiece); ok {
				singlePiece = piece.SinglePiece
			}
		case base.SizeScope_TINY:
			defer span.End()
			span.SetAttributes(config.AttributePeerTaskSizeScope.String("tiny"))
			logger.Infof("%s/%s size scope: tiny", result.TaskId, request.PeerId)
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
			logger.Infof("%s/%s size scope: normal", result.TaskId, request.PeerId)
		}
	}
	logger.Infof("step 2: start report peer %s piece result", request.PeerId)
	peerPacketStream, err := schedulerClient.ReportPieceResult(ctx, result.TaskId, &request.PeerTaskRequest)
	if err != nil {
		logger.Errorf("step 2: peer %s report piece failed: err", request.PeerId, err)
		defer span.End()
		span.RecordError(err)
		return ctx, nil, nil, err
	}

	var limiter *rate.Limiter
	if perPeerRateLimit > 0 {
		limiter = rate.NewLimiter(perPeerRateLimit, int(perPeerRateLimit))
	}
	pt := &filePeerTask{
		progressCh:        make(chan *FilePeerTaskProgress),
		progressStopCh:    make(chan bool),
		disableBackSource: request.DisableBackSource,
		pattern:           request.Pattern,
		callsystem:        request.Callsystem,
		peerTask: peerTask{
			host:                host,
			needBackSource:      needBackSource,
			request:             &request.PeerTaskRequest,
			peerPacketStream:    peerPacketStream,
			pieceManager:        pieceManager,
			peerPacketReady:     make(chan bool, 1),
			peerID:              request.PeerId,
			taskID:              result.TaskId,
			singlePiece:         singlePiece,
			done:                make(chan struct{}),
			span:                span,
			once:                sync.Once{},
			readyPieces:         NewBitmap(),
			requestedPieces:     NewBitmap(),
			failedPieceCh:       make(chan int32, config.DefaultPieceChanSize),
			failedReason:        failedReasonNotSet,
			failedCode:          base.Code_UnknownError,
			contentLength:       atomic.NewInt64(-1),
			pieceParallelCount:  atomic.NewInt32(0),
			totalPiece:          -1,
			getPiecesMaxRetry:   getPiecesMaxRetry,
			schedulerOption:     schedulerOption,
			schedulerClient:     schedulerClient,
			limiter:             limiter,
			completedLength:     atomic.NewInt64(0),
			usedTraffic:         atomic.NewUint64(0),
			SugaredLoggerOnWith: logger.With("peer", request.PeerId, "task", result.TaskId, "component", "filePeerTask"),
		},
	}
	// bind func that base peer task did not implement
	pt.backSourceFunc = pt.backSource
	pt.setContentLengthFunc = pt.SetContentLength
	pt.setTotalPiecesFunc = pt.SetTotalPieces
	pt.reportPieceResultFunc = pt.ReportPieceResult
	return ctx, pt, nil, nil
}

func (pt *filePeerTask) Start(ctx context.Context) (chan *FilePeerTaskProgress, error) {
	pt.ctx, pt.cancel = context.WithCancel(ctx)

	if pt.needBackSource {
		pt.contentLength.Store(-1)
		_ = pt.callback.Init(pt)
		go pt.backSource()
		return pt.progressCh, nil
	}

	pt.pullPieces(pt.cleanUnfinished)

	// return a progress channel for request download progress
	return pt.progressCh, nil
}

func (pt *filePeerTask) ReportPieceResult(result *pieceTaskResult) error {
	// goroutine safe for channel and send on closed channel
	defer pt.recoverFromPanic()
	pt.Debugf("report piece %d result, success: %t", result.piece.PieceNum, result.pieceResult.Success)

	// retry failed piece
	if !result.pieceResult.Success {
		result.pieceResult.FinishedCount = pt.readyPieces.Settled()
		_ = pt.peerPacketStream.Send(result.pieceResult)
		if result.notRetry {
			pt.Warnf("piece %d download failed, no retry", result.piece.PieceNum)
			return nil
		}
		select {
		case <-pt.done:
			pt.Infof("peer task done, stop to send failed piece")
		case <-pt.ctx.Done():
			pt.Debugf("context done due to %s, stop to send failed piece", pt.ctx.Err())
		case pt.failedPieceCh <- result.pieceResult.PieceInfo.PieceNum:
			pt.Warnf("piece %d download failed, retry later", result.piece.PieceNum)
		}

		return nil
	}

	pt.lock.Lock()
	if pt.readyPieces.IsSet(result.pieceResult.PieceInfo.PieceNum) {
		pt.lock.Unlock()
		pt.Warnf("piece %d is already reported, skipped", result.pieceResult.PieceInfo.PieceNum)
		return nil
	}
	// mark piece processed
	pt.readyPieces.Set(result.pieceResult.PieceInfo.PieceNum)
	pt.completedLength.Add(int64(result.piece.RangeSize))
	pt.lock.Unlock()

	result.pieceResult.FinishedCount = pt.readyPieces.Settled()
	_ = pt.peerPacketStream.Send(result.pieceResult)
	// send progress first to avoid close channel panic
	p := &FilePeerTaskProgress{
		State: &ProgressState{
			Success: result.pieceResult.Success,
			Code:    result.pieceResult.Code,
			Msg:     "downloading",
		},
		TaskID:          pt.taskID,
		PeerID:          pt.peerID,
		ContentLength:   pt.contentLength.Load(),
		CompletedLength: pt.completedLength.Load(),
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
	if err = pt.callback.ValidateDigest(pt); err != nil {
		pt.Errorf("validate digest error: %s", err)
		pt.span.RecordError(err)
		pt.cleanUnfinished()
		return err
	}
	// send last progress
	pt.once.Do(func() {
		defer pt.recoverFromPanic()
		// send EOF piece result to scheduler
		_ = pt.peerPacketStream.Send(
			scheduler.NewEndPieceResult(pt.taskID, pt.peerID, pt.readyPieces.Settled()))
		pt.Debugf("finish end piece result sent")

		var (
			success      = true
			code         = base.Code_Success
			message      = "Success"
			progressDone bool
		)

		// callback to store data to output
		if err = pt.callback.Done(pt); err != nil {
			pt.Errorf("peer task done callback failed: %s", err)
			pt.span.RecordError(err)
			success = false
			code = base.Code_ClientError
			message = err.Error()
		}

		pg := &FilePeerTaskProgress{
			State: &ProgressState{
				Success: success,
				Code:    code,
				Msg:     message,
			},
			TaskID:          pt.taskID,
			PeerID:          pt.peerID,
			ContentLength:   pt.contentLength.Load(),
			CompletedLength: pt.completedLength.Load(),
			PeerTaskDone:    true,
			DoneCallback: func() {
				progressDone = true
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
			if progressDone {
				pt.Debugf("progress stopped and context done")
			} else {
				pt.Warnf("wait progress stopped failed, context done, but progress not stopped")
			}
		}
		pt.Debugf("finished: close channel")
		pt.success = true
		close(pt.done)
		pt.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
		pt.span.End()
	})
	return err
}

func (pt *filePeerTask) cleanUnfinished() {
	defer pt.cancel()
	// send last progress
	pt.once.Do(func() {
		metrics.PeerTaskFailedCount.Add(1)
		defer pt.recoverFromPanic()
		// send EOF piece result to scheduler
		_ = pt.peerPacketStream.Send(
			scheduler.NewEndPieceResult(pt.taskID, pt.peerID, pt.readyPieces.Settled()))
		pt.Debugf("clean up end piece result sent")

		if err := pt.callback.Fail(pt, pt.failedCode, pt.failedReason); err != nil {
			pt.span.RecordError(err)
			pt.Errorf("peer task fail callback failed: %s", err)
		}

		var progressDone bool
		pg := &FilePeerTaskProgress{
			State: &ProgressState{
				Success: false,
				Code:    pt.failedCode,
				Msg:     pt.failedReason,
			},
			TaskID:          pt.taskID,
			PeerID:          pt.peerID,
			ContentLength:   pt.contentLength.Load(),
			CompletedLength: pt.completedLength.Load(),
			PeerTaskDone:    true,
			DoneCallback: func() {
				progressDone = true
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
			if progressDone {
				pt.Debugf("progress stopped and context done")
			} else {
				pt.Warnf("wait progress stopped failed, context done, but progress not stopped")
			}
		}

		pt.Debugf("clean unfinished: close channel")
		close(pt.done)
		pt.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		pt.span.SetAttributes(config.AttributePeerTaskCode.Int(int(pt.failedCode)))
		pt.span.SetAttributes(config.AttributePeerTaskMessage.String(pt.failedReason))
		pt.span.End()
	})
}

// TODO SetContentLength 需要和pt.finish解绑，以便在下载进度处可以看到文件长度
func (pt *filePeerTask) SetContentLength(i int64) error {
	pt.contentLength.Store(i)
	if !pt.isCompleted() {
		return errors.New("SetContentLength should call after task completed")
	}

	return pt.finish()
}

func (pt *filePeerTask) SetTotalPieces(i int32) {
	pt.totalPiece = i
}

func (pt *filePeerTask) backSource() {
	backSourceCtx, backSourceSpan := tracer.Start(pt.ctx, config.SpanBackSource)
	defer backSourceSpan.End()
	defer pt.cleanUnfinished()
	if pt.disableBackSource {
		pt.Errorf(reasonBackSourceDisabled)
		pt.failedReason = reasonBackSourceDisabled
		return
	}
	_ = pt.callback.Init(pt)
	reportPieceCtx, reportPieceSpan := tracer.Start(backSourceCtx, config.SpanReportPieceResult)
	defer reportPieceSpan.End()
	if peerPacketStream, err := pt.schedulerClient.ReportPieceResult(reportPieceCtx, pt.taskID, pt.request); err != nil {
		logger.Errorf("step 2: peer %s report piece failed: err", pt.request.PeerId, err)
	} else {
		pt.peerPacketStream = peerPacketStream
	}
	logger.Infof("step 2: start report peer %s back source piece result", pt.request.PeerId)
	err := pt.pieceManager.DownloadSource(pt.ctx, pt, pt.request)
	if err != nil {
		pt.Errorf("download from source error: %s", err)
		pt.failedReason = err.Error()
		backSourceSpan.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		backSourceSpan.RecordError(err)
		return
	}
	pt.Infof("download from source ok")
	backSourceSpan.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
	_ = pt.finish()
}
