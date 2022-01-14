/*
 *     Copyright 2022 The Dragonfly Authors
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
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
)

const (
	reasonContextCanceled       = "context canceled"
	reasonScheduleTimeout       = "wait first peer packet from scheduler timeout"
	reasonReScheduleTimeout     = "wait more available peers from scheduler timeout"
	reasonPeerGoneFromScheduler = "scheduler says client should disconnect"
	reasonBackSourceDisabled    = "download from source disabled"

	failedReasonNotSet = "unknown"
)

var errPeerPacketChanged = errors.New("peer packet changed")

var _ Task = (*peerTaskConductor)(nil)

// peerTaskConductor will fetch all pieces from other peers and send pieces info to broker
type peerTaskConductor struct {
	*logger.SugaredLoggerOnWith
	// ctx is with span info for tracing
	// we did not use cancel with ctx, use successCh and failCh instead
	ctx context.Context

	// host info about current host
	host *scheduler.PeerHost
	// request is the original PeerTaskRequest
	request *scheduler.PeerTaskRequest

	// needBackSource indicates downloading resource from instead of other peers
	needBackSource *atomic.Bool

	// pieceManager will be used for downloading piece
	pieceManager    PieceManager
	storageManager  storage.Manager
	peerTaskManager *peerTaskManager

	// schedule options
	schedulerOption config.SchedulerOption
	schedulerClient schedulerclient.SchedulerClient

	// peer task meta info
	peerID          string
	taskID          string
	totalPiece      int32
	digest          string
	contentLength   *atomic.Int64
	completedLength *atomic.Int64
	usedTraffic     *atomic.Uint64

	broker *pieceBroker

	sizeScope   base.SizeScope
	singlePiece *scheduler.SinglePiece
	tinyData    *TinyData

	// peerPacketStream stands schedulerclient.PeerPacketStream from scheduler
	peerPacketStream schedulerclient.PeerPacketStream
	// peerPacket is the latest available peers from peerPacketCh
	peerPacket atomic.Value // *scheduler.PeerPacket
	// peerPacketReady will receive a ready signal for peerPacket ready
	peerPacketReady chan bool
	// pieceParallelCount stands the piece parallel count from peerPacket
	pieceParallelCount *atomic.Int32
	// pieceTaskPoller pulls piece task from other peers
	pieceTaskPoller *pieceTaskPoller

	// same actions must be done only once, like close done channel and so on
	statusOnce sync.Once
	cancelOnce sync.Once
	// done channel will be closed when peer task success
	successCh chan struct{}
	// fail channel will be closed after peer task fail
	failCh chan struct{}

	// span stands open telemetry trace span
	span trace.Span

	// failedPieceCh will hold all pieces which download failed,
	// those pieces will be retried later
	failedPieceCh chan int32
	// failedReason will be set when peer task failed
	failedReason string
	// failedReason will be set when peer task failed
	failedCode base.Code

	// readyPieces stands all pieces download status
	readyPieces *Bitmap
	// requestedPieces stands all pieces requested from peers
	requestedPieces *Bitmap
	// lock used by piece result manage, when update readyPieces, lock first
	lock sync.RWMutex
	// limiter will be used when enable per peer task rate limit
	limiter *rate.Limiter

	start time.Time
}

func (ptm *peerTaskManager) newPeerTaskConductor(
	ctx context.Context,
	request *scheduler.PeerTaskRequest,
	limit rate.Limit) (*peerTaskConductor, error) {

	metrics.PeerTaskCount.Add(1)
	// use a new context with span info
	ctx = trace.ContextWithSpan(context.Background(), trace.SpanFromContext(ctx))
	ctx, span := tracer.Start(ctx, config.SpanPeerTask, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(config.AttributePeerHost.String(ptm.host.Uuid))
	span.SetAttributes(semconv.NetHostIPKey.String(ptm.host.Ip))
	span.SetAttributes(config.AttributePeerID.String(request.PeerId))
	span.SetAttributes(semconv.HTTPURLKey.String(request.Url))

	logger.Debugf("request overview, pid: %s, url: %s, filter: %s, meta: %s, tag: %s",
		request.PeerId, request.Url, request.UrlMeta.Filter, request.UrlMeta, request.UrlMeta.Tag)
	// trace register
	regCtx, cancel := context.WithTimeout(ctx, ptm.schedulerOption.ScheduleTimeout.Duration)
	defer cancel()
	regCtx, regSpan := tracer.Start(regCtx, config.SpanRegisterTask)
	logger.Infof("step 1: peer %s start to register", request.PeerId)
	result, err := ptm.schedulerClient.RegisterPeerTask(regCtx, request)
	regSpan.RecordError(err)
	regSpan.End()

	var needBackSource bool
	if err != nil {
		if err == context.DeadlineExceeded {
			logger.Errorf("scheduler did not response in %s", ptm.schedulerOption.ScheduleTimeout.Duration)
		}
		logger.Errorf("step 1: peer %s register failed: %s", request.PeerId, err)
		if ptm.schedulerOption.DisableAutoBackSource {
			logger.Errorf("register peer task failed: %s, peer id: %s, auto back source disabled", err, request.PeerId)
			span.RecordError(err)
			span.End()
			return nil, err
		}
		needBackSource = true
		// can not detect source or scheduler error, create a new dummy scheduler client
		ptm.schedulerClient = &dummySchedulerClient{}
		result = &scheduler.RegisterResult{TaskId: idgen.TaskID(request.Url, request.UrlMeta)}
		logger.Warnf("register peer task failed: %s, peer id: %s, try to back source", err, request.PeerId)
	}

	if result == nil {
		defer span.End()
		span.RecordError(err)
		err = errors.Errorf("empty schedule result")
		return nil, err
	}
	log := logger.With("peer", request.PeerId, "task", result.TaskId, "component", "PeerTask")
	span.SetAttributes(config.AttributeTaskID.String(result.TaskId))
	log.Infof("register task success, SizeScope: %s", base.SizeScope_name[int32(result.SizeScope)])

	var (
		singlePiece *scheduler.SinglePiece
		tinyData    *TinyData
	)
	if !needBackSource {
		switch result.SizeScope {
		case base.SizeScope_NORMAL:
			span.SetAttributes(config.AttributePeerTaskSizeScope.String("normal"))
		case base.SizeScope_SMALL:
			span.SetAttributes(config.AttributePeerTaskSizeScope.String("small"))
			if piece, ok := result.DirectPiece.(*scheduler.RegisterResult_SinglePiece); ok {
				singlePiece = piece.SinglePiece
			}
		case base.SizeScope_TINY:
			defer span.End()
			span.SetAttributes(config.AttributePeerTaskSizeScope.String("tiny"))
			if piece, ok := result.DirectPiece.(*scheduler.RegisterResult_PieceContent); ok {
				tinyData = &TinyData{
					span:    span,
					TaskID:  result.TaskId,
					PeerID:  request.PeerId,
					Content: piece.PieceContent,
				}
			} else {
				err = errors.Errorf("scheduler return tiny piece but can not parse piece content")
				span.RecordError(err)
				log.Errorf("%s", err)
				return nil, err
			}
		}
	}

	peerPacketStream, err := ptm.schedulerClient.ReportPieceResult(ctx, result.TaskId, request)
	log.Infof("step 2: start report piece result")
	if err != nil {
		defer span.End()
		span.RecordError(err)
		return nil, err
	}

	ptc := &peerTaskConductor{
		start:               time.Now(),
		ctx:                 ctx,
		broker:              newPieceBroker(),
		host:                ptm.host,
		needBackSource:      atomic.NewBool(needBackSource),
		request:             request,
		peerPacketStream:    peerPacketStream,
		pieceManager:        ptm.pieceManager,
		storageManager:      ptm.storageManager,
		peerTaskManager:     ptm,
		peerPacketReady:     make(chan bool, 1),
		peerID:              request.PeerId,
		taskID:              result.TaskId,
		sizeScope:           result.SizeScope,
		singlePiece:         singlePiece,
		tinyData:            tinyData,
		successCh:           make(chan struct{}),
		failCh:              make(chan struct{}),
		span:                span,
		readyPieces:         NewBitmap(),
		requestedPieces:     NewBitmap(),
		failedPieceCh:       make(chan int32, config.DefaultPieceChanSize),
		failedReason:        failedReasonNotSet,
		failedCode:          base.Code_UnknownError,
		contentLength:       atomic.NewInt64(-1),
		pieceParallelCount:  atomic.NewInt32(0),
		totalPiece:          -1,
		schedulerOption:     ptm.schedulerOption,
		schedulerClient:     ptm.schedulerClient,
		limiter:             rate.NewLimiter(limit, int(limit)),
		completedLength:     atomic.NewInt64(0),
		usedTraffic:         atomic.NewUint64(0),
		SugaredLoggerOnWith: log,
	}
	ptc.pieceTaskPoller = &pieceTaskPoller{
		getPiecesMaxRetry: ptm.getPiecesMaxRetry,
		peerTaskConductor: ptc,
	}
	return ptc, nil
}

func (pt *peerTaskConductor) run() {
	go pt.broker.Start()
	go pt.pullPieces()
}

func (pt *peerTaskConductor) GetPeerID() string {
	return pt.peerID
}

func (pt *peerTaskConductor) GetTaskID() string {
	return pt.taskID
}

func (pt *peerTaskConductor) GetContentLength() int64 {
	return pt.contentLength.Load()
}

func (pt *peerTaskConductor) SetContentLength(i int64) {
	pt.contentLength.Store(i)
}

func (pt *peerTaskConductor) AddTraffic(n uint64) {
	pt.usedTraffic.Add(n)
}

func (pt *peerTaskConductor) GetTraffic() uint64 {
	return pt.usedTraffic.Load()
}

func (pt *peerTaskConductor) GetTotalPieces() int32 {
	return pt.totalPiece
}

func (pt *peerTaskConductor) SetTotalPieces(i int32) {
	pt.totalPiece = i
}

func (pt *peerTaskConductor) SetPieceMd5Sign(md5 string) {
	pt.digest = md5
}

func (pt *peerTaskConductor) GetPieceMd5Sign() string {
	return pt.digest
}

func (pt *peerTaskConductor) Context() context.Context {
	return pt.ctx
}

func (pt *peerTaskConductor) Log() *logger.SugaredLoggerOnWith {
	return pt.SugaredLoggerOnWith
}

func (pt *peerTaskConductor) cancel(code base.Code, reason string) {
	pt.cancelOnce.Do(func() {
		pt.failedCode = code
		pt.failedReason = reason
		pt.Fail()
	})
}

func (pt *peerTaskConductor) backSource() {
	backSourceCtx, backSourceSpan := tracer.Start(pt.ctx, config.SpanBackSource)
	defer backSourceSpan.End()
	pt.contentLength.Store(-1)
	if err := pt.InitStorage(); err != nil {
		pt.cancel(base.Code_ClientError, err.Error())
		return
	}
	err := pt.pieceManager.DownloadSource(backSourceCtx, pt, pt.request)
	if err != nil {
		pt.Errorf("download from source error: %s", err)
		backSourceSpan.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		backSourceSpan.RecordError(err)
		pt.cancel(base.Code_ClientError, err.Error())
		return
	}
	pt.Done()
	pt.Infof("download from source ok")
	backSourceSpan.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
	return
}

func (pt *peerTaskConductor) pullPieces() {
	if pt.needBackSource.Load() {
		pt.backSource()
		return
	}
	switch pt.sizeScope {
	case base.SizeScope_TINY:
		pt.storeTinyPeerTask()
	case base.SizeScope_SMALL:
		go pt.pullSinglePiece()
	case base.SizeScope_NORMAL:
		go pt.receivePeerPacket()
		go pt.pullPiecesFromPeers()
	default:
		pt.cancel(base.Code_ClientError, fmt.Sprintf("unknown size scope: %d", pt.sizeScope))
	}
}

func (pt *peerTaskConductor) storeTinyPeerTask() {
	// TODO store tiny data asynchronous
	l := int64(len(pt.tinyData.Content))
	pt.SetContentLength(l)
	pt.SetTotalPieces(1)
	ctx := pt.ctx
	err := pt.peerTaskManager.storageManager.RegisterTask(ctx,
		storage.RegisterTaskRequest{
			CommonTaskRequest: storage.CommonTaskRequest{
				PeerID: pt.tinyData.PeerID,
				TaskID: pt.tinyData.TaskID,
			},
			ContentLength: l,
			TotalPieces:   1,
			// TODO check digest
		})
	if err != nil {
		logger.Errorf("register tiny data storage failed: %s", err)
		pt.cancel(base.Code_ClientError, err.Error())
		return
	}
	n, err := pt.peerTaskManager.storageManager.WritePiece(ctx,
		&storage.WritePieceRequest{
			PeerTaskMetadata: storage.PeerTaskMetadata{
				PeerID: pt.tinyData.PeerID,
				TaskID: pt.tinyData.TaskID,
			},
			PieceMetadata: storage.PieceMetadata{
				Num:    0,
				Md5:    "",
				Offset: 0,
				Range: clientutil.Range{
					Start:  0,
					Length: l,
				},
				Style: 0,
			},
			UnknownLength: false,
			Reader:        bytes.NewBuffer(pt.tinyData.Content),
			GenPieceDigest: func(n int64) (int32, bool) {
				return 1, true
			},
		})
	if err != nil {
		logger.Errorf("write tiny data storage failed: %s", err)
		pt.cancel(base.Code_ClientError, err.Error())
		return
	}
	if n != l {
		logger.Errorf("write tiny data storage failed", n, l)
		pt.cancel(base.Code_ClientError, err.Error())
		return
	}

	err = pt.UpdateStorage()
	if err != nil {
		logger.Errorf("update tiny data storage failed: %s", err)
		pt.cancel(base.Code_ClientError, err.Error())
		return
	}

	logger.Debugf("store tiny data, len: %d", l)
	pt.PublishPieceInfo(0, uint32(l))
}

func (pt *peerTaskConductor) receivePeerPacket() {
	var (
		peerPacket    *scheduler.PeerPacket
		err           error
		firstSpanDone bool
	)
	// only record first schedule result
	// other schedule result will record as an event in peer task span
	_, firstPeerSpan := tracer.Start(pt.ctx, config.SpanFirstSchedule)
	defer func() {
		if !firstSpanDone {
			firstPeerSpan.End()
		}
		if pt.needBackSource.Load() {
			return
		}
		select {
		case <-pt.successCh:
		case <-pt.failCh:
		default:
			pt.Errorf("receivePeerPacket exit, but peer task not success or fail")
			pt.Fail()
		}
	}()
loop:
	for {
		select {
		case <-pt.successCh:
			pt.Infof("peer task success, stop wait peer packet from scheduler")
			break loop
		case <-pt.failCh:
			pt.Infof("peer task fail, stop wait peer packet from scheduler")
			break loop
		default:
		}

		peerPacket, err = pt.peerPacketStream.Recv()
		if err == io.EOF {
			pt.Debugf("peerPacketStream closed")
			break loop
		}
		if err != nil {
			pt.confirmReceivePeerPacketError(err)
			if !firstSpanDone {
				firstPeerSpan.RecordError(err)
			}
			break loop
		}

		pt.Debugf("receive peerPacket %v for peer %s", peerPacket, pt.peerID)
		if peerPacket.Code != base.Code_Success {
			if peerPacket.Code == base.Code_SchedNeedBackSource {
				pt.needBackSource.Store(true)
				close(pt.peerPacketReady)
				pt.Infof("receive back source code")
				return
			}
			pt.Errorf("receive peer packet with error: %d", peerPacket.Code)
			if pt.isExitPeerPacketCode(peerPacket) {
				pt.Errorf(pt.failedReason)
				pt.cancel(pt.failedCode, pt.failedReason)
				if !firstSpanDone {
					firstPeerSpan.RecordError(fmt.Errorf(pt.failedReason))
				}
				pt.span.AddEvent("receive exit peer packet",
					trace.WithAttributes(config.AttributePeerPacketCode.Int(int(peerPacket.Code))))
				pt.span.RecordError(fmt.Errorf(pt.failedReason))
				break
			} else {
				pt.span.AddEvent("receive not success peer packet",
					trace.WithAttributes(config.AttributePeerPacketCode.Int(int(peerPacket.Code))))
			}
			continue
		}

		if peerPacket.MainPeer == nil && peerPacket.StealPeers == nil {
			pt.Warnf("scheduler client send a peerPacket with empty peers")
			continue
		}
		pt.Infof("receive new peer packet, main peer: %s, parallel count: %d",
			peerPacket.MainPeer.PeerId, peerPacket.ParallelCount)
		pt.span.AddEvent("receive new peer packet",
			trace.WithAttributes(config.AttributeMainPeer.String(peerPacket.MainPeer.PeerId)))
		if !firstSpanDone {
			firstSpanDone = true
			firstPeerSpan.SetAttributes(config.AttributeMainPeer.String(peerPacket.MainPeer.PeerId))
			firstPeerSpan.End()
		}

		pt.peerPacket.Store(peerPacket)
		pt.pieceParallelCount.Store(peerPacket.ParallelCount)

		// send peerPacketReady
		select {
		case pt.peerPacketReady <- true:
		case <-pt.successCh:
			pt.Infof("peer task success, stop wait peer packet from scheduler")
			break loop
		case <-pt.failCh:
			pt.Errorf("peer task fail, stop wait peer packet from scheduler")
			break loop
		default:
		}
	}
}

func (pt *peerTaskConductor) confirmReceivePeerPacketError(err error) {
	select {
	case <-pt.successCh:
		return
	case <-pt.failCh:
		return
	default:
	}
	var (
		failedCode   = base.Code_UnknownError
		failedReason string
	)
	de, ok := err.(*dferrors.DfError)
	if ok && de.Code == base.Code_SchedNeedBackSource {
		pt.needBackSource.Store(true)
		close(pt.peerPacketReady)
		pt.Infof("receive back source code")
		return
	} else if ok && de.Code != base.Code_SchedNeedBackSource {
		failedCode = de.Code
		failedReason = de.Message
		pt.Errorf("receive peer packet failed: %s", pt.failedReason)
	} else {
		pt.Errorf("receive peer packet failed: %s", err)
	}
	pt.cancel(failedCode, failedReason)
	return
}

func (pt *peerTaskConductor) isExitPeerPacketCode(pp *scheduler.PeerPacket) bool {
	switch pp.Code {
	case base.Code_ResourceLacked, base.Code_BadRequest,
		base.Code_PeerTaskNotFound, base.Code_UnknownError, base.Code_RequestTimeOut:
		// 1xxx
		pt.failedCode = pp.Code
		pt.failedReason = fmt.Sprintf("receive exit peer packet with code %d", pp.Code)
		return true
	case base.Code_SchedError, base.Code_SchedTaskStatusError, base.Code_SchedPeerNotFound:
		// 5xxx
		pt.failedCode = pp.Code
		pt.failedReason = fmt.Sprintf("receive exit peer packet with code %d", pp.Code)
		return true
	case base.Code_SchedPeerGone:
		pt.failedReason = reasonPeerGoneFromScheduler
		pt.failedCode = base.Code_SchedPeerGone
		return true
	case base.Code_CDNError, base.Code_CDNTaskRegistryFail, base.Code_CDNTaskDownloadFail:
		// 6xxx
		pt.failedCode = pp.Code
		pt.failedReason = fmt.Sprintf("receive exit peer packet with code %d", pp.Code)
		return true
	}
	return false
}

func (pt *peerTaskConductor) pullSinglePiece() {
	pt.Infof("single piece, dest peer id: %s, piece num: %d, size: %d",
		pt.singlePiece.DstPid, pt.singlePiece.PieceInfo.PieceNum, pt.singlePiece.PieceInfo.RangeSize)

	ctx, span := tracer.Start(pt.ctx, fmt.Sprintf(config.SpanDownloadPiece, pt.singlePiece.PieceInfo.PieceNum))
	span.SetAttributes(config.AttributePiece.Int(int(pt.singlePiece.PieceInfo.PieceNum)))

	pt.contentLength.Store(int64(pt.singlePiece.PieceInfo.RangeSize))
	pt.SetTotalPieces(1)
	pt.SetPieceMd5Sign(digestutils.Sha256(pt.singlePiece.PieceInfo.PieceMd5))
	if err := pt.InitStorage(); err != nil {
		pt.cancel(base.Code_ClientError, err.Error())
		span.RecordError(err)
		span.SetAttributes(config.AttributePieceSuccess.Bool(false))
		span.End()
		return
	}

	request := &DownloadPieceRequest{
		piece:   pt.singlePiece.PieceInfo,
		log:     pt.Log(),
		TaskID:  pt.GetTaskID(),
		PeerID:  pt.GetPeerID(),
		DstPid:  pt.singlePiece.DstPid,
		DstAddr: pt.singlePiece.DstAddr,
	}

	if result, err := pt.pieceManager.DownloadPiece(ctx, request); err == nil {
		pt.reportSuccessResult(request, result)
		pt.PublishPieceInfo(request.piece.PieceNum, request.piece.RangeSize)

		span.SetAttributes(config.AttributePieceSuccess.Bool(true))
		span.End()
		pt.Infof("single piece download success")
	} else {
		// fallback to download from other peers
		span.SetAttributes(config.AttributePieceSuccess.Bool(false))
		span.End()

		pt.Warnf("single piece download failed, switch to download from other peers")
		pt.ReportPieceResult(request, result, err)

		go pt.receivePeerPacket()
		pt.pullPiecesFromPeers()
	}
}

func (pt *peerTaskConductor) pullPiecesFromPeers() {
	if ok, backSource := pt.waitFirstPeerPacket(); !ok {
		if backSource {
			return
		}
		pt.Errorf("wait first peer packet error")
		return
	}
	var (
		num            int32
		ok             bool
		limit          uint32
		initialized    bool
		pieceRequestCh chan *DownloadPieceRequest
		// keep same size with pt.failedPieceCh for avoiding dead-lock
		pieceBufferSize = uint32(config.DefaultPieceChanSize)
	)
	limit = pieceBufferSize
loop:
	for {
		// 1, check whether catch exit signal or get a failed piece
		// if nothing got, process normal pieces
		select {
		case <-pt.successCh:
			pt.Infof("peer task success, stop get pieces from peer")
			break loop
		case <-pt.failCh:
			pt.Error("peer task fail, stop get pieces from peer")
			break loop
		case failed := <-pt.failedPieceCh:
			pt.Warnf("download piece %d failed, retry", failed)
			num = failed
			limit = 1
		default:
		}

		// 2, try to get pieces
		pt.Debugf("try to get pieces, number: %d, limit: %d", num, limit)
		piecePacket, err := pt.pieceTaskPoller.preparePieceTasks(
			&base.PieceTaskRequest{
				TaskId:   pt.taskID,
				SrcPid:   pt.peerID,
				StartNum: uint32(num),
				Limit:    limit,
			})

		if err != nil {
			pt.Warnf("get piece task error: %s, wait available peers from scheduler", err.Error())
			pt.span.RecordError(err)
			if num, ok = pt.waitAvailablePeerPacket(); !ok {
				break loop
			}
			continue loop
		}

		if !initialized {
			initialized = true
			if pieceRequestCh, ok = pt.init(piecePacket, pieceBufferSize); !ok {
				break loop
			}
		}

		// update total piece
		if piecePacket.TotalPiece > pt.totalPiece {
			pt.totalPiece = piecePacket.TotalPiece
			_ = pt.UpdateStorage()
			pt.Debugf("update total piece count: %d", pt.totalPiece)
		}

		// update digest
		if len(piecePacket.PieceMd5Sign) > 0 && len(pt.digest) == 0 {
			pt.digest = piecePacket.PieceMd5Sign
			_ = pt.UpdateStorage()
			pt.Debugf("update digest: %s", pt.digest)
		}

		// update content length
		if piecePacket.ContentLength > -1 {
			pt.SetContentLength(piecePacket.ContentLength)
			_ = pt.UpdateStorage()
			pt.Debugf("update content length: %d", pt.GetContentLength())
		}

		// 3. dispatch piece request to all workers
		pt.dispatchPieceRequest(pieceRequestCh, piecePacket)

		// 4. get next piece
		if num, ok = pt.getNextPieceNum(num); ok {
			// get next piece success
			limit = pieceBufferSize
			continue
		}

		// 5. wait failed pieces
		pt.Infof("all pieces requests sent, just wait failed pieces")
		// get failed piece
		if num, ok = pt.waitFailedPiece(); !ok {
			// when ok == false, indicates than need break loop
			break loop
		}
		// just need one piece
		limit = 1
	}
}

func (pt *peerTaskConductor) init(piecePacket *base.PiecePacket, pieceBufferSize uint32) (chan *DownloadPieceRequest, bool) {
	pt.contentLength.Store(piecePacket.ContentLength)
	if piecePacket.ContentLength > -1 {
		pt.span.SetAttributes(config.AttributeTaskContentLength.Int64(piecePacket.ContentLength))
	}
	if err := pt.InitStorage(); err != nil {
		pt.span.RecordError(err)
		pt.cancel(base.Code_ClientError, err.Error())
		return nil, false
	}
	pc := pt.peerPacket.Load().(*scheduler.PeerPacket).ParallelCount
	pieceRequestCh := make(chan *DownloadPieceRequest, pieceBufferSize)
	for i := int32(0); i < pc; i++ {
		go pt.downloadPieceWorker(i, pieceRequestCh)
	}
	return pieceRequestCh, true
}

func (pt *peerTaskConductor) waitFirstPeerPacket() (done bool, backSource bool) {
	// wait first available peer
	select {
	case _, ok := <-pt.peerPacketReady:
		if ok {
			// preparePieceTasksByPeer func already send piece result with error
			pt.Infof("new peer client ready, scheduler time cost: %dus, main peer: %s",
				time.Now().Sub(pt.start).Microseconds(), pt.peerPacket.Load().(*scheduler.PeerPacket).MainPeer)
			return true, false
		}
		// when scheduler says base.Code_SchedNeedBackSource, receivePeerPacket will close pt.peerPacketReady
		pt.Infof("start download from source due to base.Code_SchedNeedBackSource")
		pt.span.AddEvent("back source due to scheduler says need back source")
		pt.needBackSource.Store(true)
		pt.backSource()
		return false, true
	case <-time.After(pt.schedulerOption.ScheduleTimeout.Duration):
		if pt.schedulerOption.DisableAutoBackSource {
			pt.cancel(base.Code_ClientScheduleTimeout, reasonBackSourceDisabled)
			err := fmt.Errorf("%s, auto back source disabled", pt.failedReason)
			pt.span.RecordError(err)
			pt.Errorf(err.Error())
			return false, false
		}
		pt.Warnf("start download from source due to %s", reasonScheduleTimeout)
		pt.span.AddEvent("back source due to schedule timeout")
		pt.needBackSource.Store(true)
		pt.backSource()
		return false, true
	}
}

func (pt *peerTaskConductor) waitAvailablePeerPacket() (int32, bool) {
	// only <-pt.peerPacketReady continue loop, others break
	select {
	// when peer task without content length or total pieces count, match here
	case <-pt.successCh:
		pt.Infof("peer task success, stop wait available peer packet")
	case <-pt.failCh:
		pt.Infof("peer task fail, stop wait available peer packet")
	case _, ok := <-pt.peerPacketReady:
		if ok {
			// preparePieceTasksByPeer func already send piece result with error
			pt.Infof("new peer client ready, main peer: %s", pt.peerPacket.Load().(*scheduler.PeerPacket).MainPeer)
			// research from piece 0
			return 0, true
		}
		// when scheduler says base.Code_SchedNeedBackSource, receivePeerPacket will close pt.peerPacketReady
		pt.Infof("start download from source due to base.Code_SchedNeedBackSource")
		pt.span.AddEvent("back source due to scheduler says need back source ")
		pt.needBackSource.Store(true)
		// TODO optimize back source when already downloaded some pieces
		pt.backSource()
	case <-time.After(pt.schedulerOption.ScheduleTimeout.Duration):
		if pt.schedulerOption.DisableAutoBackSource {
			pt.cancel(base.Code_ClientScheduleTimeout, reasonBackSourceDisabled)
			err := fmt.Errorf("%s, auto back source disabled", pt.failedReason)
			pt.span.RecordError(err)
			pt.Errorf(err.Error())
		} else {
			pt.Warnf("start download from source due to %s", reasonReScheduleTimeout)
			pt.span.AddEvent("back source due to schedule timeout")
			pt.needBackSource.Store(true)
			pt.backSource()
		}
	}
	return -1, false
}

func (pt *peerTaskConductor) dispatchPieceRequest(pieceRequestCh chan *DownloadPieceRequest, piecePacket *base.PiecePacket) {
	pt.Debugf("dispatch piece request, piece count: %d", len(piecePacket.PieceInfos))
	for _, piece := range piecePacket.PieceInfos {
		pt.Infof("get piece %d from %s/%s, digest: %s, start: %d, size: %d",
			piece.PieceNum, piecePacket.DstAddr, piecePacket.DstPid, piece.PieceMd5, piece.RangeStart, piece.RangeSize)
		// FIXME when set total piece but no total digest, fetch again
		if !pt.requestedPieces.IsSet(piece.PieceNum) {
			pt.requestedPieces.Set(piece.PieceNum)
		}
		req := &DownloadPieceRequest{
			piece:   piece,
			log:     pt.Log(),
			TaskID:  pt.GetTaskID(),
			PeerID:  pt.GetPeerID(),
			DstPid:  piecePacket.DstPid,
			DstAddr: piecePacket.DstAddr,
		}
		select {
		case pieceRequestCh <- req:
		case <-pt.successCh:
			pt.Infof("peer task success, stop dispatch piece request")
		case <-pt.failCh:
			pt.Warnf("peer task fail, stop dispatch piece request")
		}
	}
}

func (pt *peerTaskConductor) waitFailedPiece() (int32, bool) {
	if pt.isCompleted() {
		return -1, false
	}
	// use no default branch select to wait failed piece or exit
	select {
	case <-pt.successCh:
		pt.Infof("peer task success, stop to wait failed piece")
		return -1, false
	case <-pt.failCh:
		pt.Debugf("peer task fail, stop to wait failed piece")
		return -1, false
	case failed := <-pt.failedPieceCh:
		pt.Warnf("download piece/%d failed, retry", failed)
		return failed, true
	}
}

func (pt *peerTaskConductor) downloadPieceWorker(id int32, requests chan *DownloadPieceRequest) {
	for {
		select {
		case request := <-requests:
			pt.lock.RLock()
			if pt.readyPieces.IsSet(request.piece.PieceNum) {
				pt.lock.RUnlock()
				pt.Log().Debugf("piece %d is already downloaded, skip", request.piece.PieceNum)
				continue
			}
			pt.lock.RUnlock()

			ctx, span := tracer.Start(pt.ctx, fmt.Sprintf(config.SpanDownloadPiece, request.piece.PieceNum))
			span.SetAttributes(config.AttributePiece.Int(int(request.piece.PieceNum)))
			span.SetAttributes(config.AttributePieceWorker.Int(int(id)))

			// wait limit
			if pt.limiter != nil && !pt.waitLimit(ctx, request) {
				span.SetAttributes(config.AttributePieceSuccess.Bool(false))
				span.End()
				return
			}

			pt.Debugf("peer download worker #%d receive piece task, "+
				"dest peer id: %s, piece num: %d, range start: %d, range size: %d",
				id, request.DstPid, request.piece.PieceNum, request.piece.RangeStart, request.piece.RangeSize)
			// download piece
			// result is always not nil, pieceManager will report begin and end time
			result, err := pt.pieceManager.DownloadPiece(ctx, request)
			if err != nil {
				// send to fail chan and retry
				pt.failedPieceCh <- request.piece.PieceNum
				pt.ReportPieceResult(request, result, err)
				span.SetAttributes(config.AttributePieceSuccess.Bool(false))
				span.End()
				continue
			} else {
				// broadcast success piece
				pt.reportSuccessResult(request, result)
				pt.PublishPieceInfo(request.piece.PieceNum, request.piece.RangeSize)
			}

			span.SetAttributes(config.AttributePieceSuccess.Bool(true))
			span.End()
		case <-pt.successCh:
			pt.Infof("peer task success, peer download worker #%d exit", id)
			return
		case <-pt.failCh:
			pt.Errorf("peer task fail, peer download worker #%d exit", id)
			return
		}
	}
}

func (pt *peerTaskConductor) waitLimit(ctx context.Context, request *DownloadPieceRequest) bool {
	_, waitSpan := tracer.Start(ctx, config.SpanWaitPieceLimit)
	err := pt.limiter.WaitN(pt.ctx, int(request.piece.RangeSize))
	if err == nil {
		waitSpan.End()
		return true
	}

	pt.Errorf("request limiter error: %s", err)
	waitSpan.RecordError(err)
	waitSpan.End()

	// send error piece result
	sendError := pt.peerPacketStream.Send(&scheduler.PieceResult{
		TaskId:        pt.GetTaskID(),
		SrcPid:        pt.GetPeerID(),
		DstPid:        request.DstPid,
		PieceInfo:     request.piece,
		Success:       false,
		Code:          base.Code_ClientRequestLimitFail,
		HostLoad:      nil,
		FinishedCount: 0, // update by peer task
	})
	if sendError != nil {
		pt.Errorf("report piece result failed %s", err)
	}

	pt.cancel(base.Code_ClientRequestLimitFail, err.Error())
	return false
}

func (pt *peerTaskConductor) isCompleted() bool {
	return pt.completedLength.Load() == pt.contentLength.Load()
}

func (pt *peerTaskConductor) getNextPieceNum(cur int32) (int32, bool) {
	if pt.isCompleted() {
		return -1, false
	}
	i := cur
	// try to find next not requested piece
	for ; pt.requestedPieces.IsSet(i); i++ {
	}
	if pt.totalPiece > 0 && i >= pt.totalPiece {
		// double check, re-search not success or not requested pieces
		for i = int32(0); pt.requestedPieces.IsSet(i); i++ {
		}
		if pt.totalPiece > 0 && i >= pt.totalPiece {
			return -1, false
		}
	}
	return i, true
}

func (pt *peerTaskConductor) recoverFromPanic() {
	if r := recover(); r != nil {
		pt.Errorf("recovered from panic %q. Call stack:\n%v", r, string(debug.Stack()))
	}
}

func (pt *peerTaskConductor) ReportPieceResult(request *DownloadPieceRequest, result *DownloadPieceResult, err error) {
	if err == nil {
		pt.reportSuccessResult(request, result)
		return
	}
	code := base.Code_ClientPieceDownloadFail
	if isConnectionError(err) {
		code = base.Code_ClientConnectionError
	} else if isPieceNotFound(err) {
		code = base.Code_ClientPieceNotFound
	}
	pt.reportFailResult(request, result, code)
}

func (pt *peerTaskConductor) reportSuccessResult(request *DownloadPieceRequest, result *DownloadPieceResult) {
	_, span := tracer.Start(pt.ctx, config.SpanPushPieceResult)
	span.SetAttributes(config.AttributeWritePieceSuccess.Bool(true))

	err := pt.peerPacketStream.Send(
		&scheduler.PieceResult{
			TaskId:        pt.GetTaskID(),
			SrcPid:        pt.GetPeerID(),
			DstPid:        request.DstPid,
			PieceInfo:     request.piece,
			BeginTime:     uint64(result.BeginTime),
			EndTime:       uint64(result.FinishTime),
			Success:       true,
			Code:          base.Code_Success,
			HostLoad:      nil, // TODO(jim): update host load
			FinishedCount: pt.readyPieces.Settled(),
			// TODO range_start, range_size, piece_md5, piece_offset, piece_style
		})
	if err != nil {
		pt.Errorf("report piece task error: %v", err)
	}

	span.End()
}

func (pt *peerTaskConductor) reportFailResult(request *DownloadPieceRequest, result *DownloadPieceResult, code base.Code) {
	_, span := tracer.Start(pt.ctx, config.SpanPushPieceResult)
	span.SetAttributes(config.AttributeWritePieceSuccess.Bool(false))

	err := pt.peerPacketStream.Send(&scheduler.PieceResult{
		TaskId:        pt.GetTaskID(),
		SrcPid:        pt.GetPeerID(),
		DstPid:        request.DstPid,
		PieceInfo:     request.piece,
		BeginTime:     uint64(result.BeginTime),
		EndTime:       uint64(result.FinishTime),
		Success:       false,
		Code:          code,
		HostLoad:      nil,
		FinishedCount: pt.readyPieces.Settled(),
	})
	if err != nil {
		pt.Errorf("report piece task error: %v", err)
	}
	span.End()
}

func (pt *peerTaskConductor) InitStorage() error {
	// prepare storage
	err := pt.storageManager.RegisterTask(pt.ctx,
		storage.RegisterTaskRequest{
			CommonTaskRequest: storage.CommonTaskRequest{
				PeerID: pt.GetPeerID(),
				TaskID: pt.GetTaskID(),
			},
			ContentLength: pt.GetContentLength(),
			TotalPieces:   pt.GetTotalPieces(),
			PieceMd5Sign:  pt.GetPieceMd5Sign(),
		})
	if err != nil {
		pt.Log().Errorf("register task to storage manager failed: %s", err)
	}
	return err
}

func (pt *peerTaskConductor) UpdateStorage() error {
	// update storage
	err := pt.storageManager.UpdateTask(pt.ctx,
		&storage.UpdateTaskRequest{
			PeerTaskMetadata: storage.PeerTaskMetadata{
				PeerID: pt.GetPeerID(),
				TaskID: pt.GetTaskID(),
			},
			ContentLength: pt.GetContentLength(),
			TotalPieces:   pt.GetTotalPieces(),
			PieceMd5Sign:  pt.GetPieceMd5Sign(),
		})
	if err != nil {
		pt.Log().Errorf("update task to storage manager failed: %s", err)
		return err
	}

	return nil
}

func (pt *peerTaskConductor) Done() {
	pt.statusOnce.Do(pt.done)
}

func (pt *peerTaskConductor) done() {
	defer pt.span.End()
	defer pt.broker.Stop()
	var (
		cost    = time.Now().Sub(pt.start).Milliseconds()
		success = true
		code    = base.Code_Success
	)
	pt.Log().Infof("peer task done, cost: %dms", cost)
	// TODO merge error handle
	// update storage metadata
	if err := pt.UpdateStorage(); err == nil {
		// validate digest
		if err = pt.Validate(); err == nil {
			close(pt.successCh)
			pt.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
		} else {
			close(pt.failCh)
			success = false
			code = base.Code_ClientError
			pt.failedCode = base.Code_ClientError
			pt.failedReason = err.Error()

			pt.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
			pt.span.SetAttributes(config.AttributePeerTaskCode.Int(int(pt.failedCode)))
			pt.span.SetAttributes(config.AttributePeerTaskMessage.String(pt.failedReason))
			pt.Errorf("validate digest failed: %s", err)
			metrics.PeerTaskFailedCount.Add(1)
		}
	} else {
		close(pt.failCh)
		success = false
		code = base.Code_ClientError
		pt.failedCode = base.Code_ClientError
		pt.failedReason = err.Error()

		pt.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		pt.span.SetAttributes(config.AttributePeerTaskCode.Int(int(pt.failedCode)))
		pt.span.SetAttributes(config.AttributePeerTaskMessage.String(pt.failedReason))
		pt.Errorf("update storage error: %v", err)
		metrics.PeerTaskFailedCount.Add(1)
	}

	pt.peerTaskManager.PeerTaskDone(pt.taskID)
	peerResultCtx, peerResultSpan := tracer.Start(pt.ctx, config.SpanReportPeerResult)
	defer peerResultSpan.End()

	// send EOF piece result to scheduler
	err := pt.peerPacketStream.Send(
		scheduler.NewEndPieceResult(pt.taskID, pt.peerID, pt.readyPieces.Settled()))
	pt.Debugf("end piece result sent: %v, peer task finished", err)

	err = pt.schedulerClient.ReportPeerResult(
		peerResultCtx,
		&scheduler.PeerResult{
			TaskId:          pt.GetTaskID(),
			PeerId:          pt.GetPeerID(),
			SrcIp:           pt.host.Ip,
			SecurityDomain:  pt.peerTaskManager.host.SecurityDomain,
			Idc:             pt.peerTaskManager.host.Idc,
			Url:             pt.request.Url,
			ContentLength:   pt.GetContentLength(),
			Traffic:         pt.GetTraffic(),
			TotalPieceCount: pt.totalPiece,
			Cost:            uint32(cost),
			Success:         success,
			Code:            code,
		})
	if err != nil {
		peerResultSpan.RecordError(err)
		pt.Errorf("step 3: report successful peer result, error: %v", err)
	} else {
		pt.Infof("step 3: report successful peer result ok")
	}
}

func (pt *peerTaskConductor) Fail() {
	pt.statusOnce.Do(pt.fail)
}

func (pt *peerTaskConductor) fail() {
	metrics.PeerTaskFailedCount.Add(1)
	defer pt.span.End()
	defer pt.broker.Stop()
	defer close(pt.failCh)
	pt.peerTaskManager.PeerTaskDone(pt.taskID)
	var end = time.Now()
	pt.Log().Errorf("stream peer task failed, code: %d, reason: %s", pt.failedCode, pt.failedReason)

	// send EOF piece result to scheduler
	err := pt.peerPacketStream.Send(
		scheduler.NewEndPieceResult(pt.taskID, pt.peerID, pt.readyPieces.Settled()))
	pt.Debugf("end piece result sent: %v, peer task finished", err)

	ctx := trace.ContextWithSpan(context.Background(), trace.SpanFromContext(pt.ctx))
	peerResultCtx, peerResultSpan := tracer.Start(ctx, config.SpanReportPeerResult)
	defer peerResultSpan.End()
	err = pt.schedulerClient.ReportPeerResult(
		peerResultCtx,
		&scheduler.PeerResult{
			TaskId:          pt.GetTaskID(),
			PeerId:          pt.GetPeerID(),
			SrcIp:           pt.peerTaskManager.host.Ip,
			SecurityDomain:  pt.peerTaskManager.host.SecurityDomain,
			Idc:             pt.peerTaskManager.host.Idc,
			Url:             pt.request.Url,
			ContentLength:   pt.GetContentLength(),
			Traffic:         pt.GetTraffic(),
			TotalPieceCount: pt.totalPiece,
			Cost:            uint32(end.Sub(pt.start).Milliseconds()),
			Success:         false,
			Code:            pt.failedCode,
		})
	if err != nil {
		peerResultSpan.RecordError(err)
		pt.Log().Errorf("step 3: report fail peer result, error: %v", err)
	} else {
		pt.Log().Infof("step 3: report fail peer result ok")
	}

	pt.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
	pt.span.SetAttributes(config.AttributePeerTaskCode.Int(int(pt.failedCode)))
	pt.span.SetAttributes(config.AttributePeerTaskMessage.String(pt.failedReason))
}

// Validate stores metadata and validates digest
func (pt *peerTaskConductor) Validate() error {
	err := pt.peerTaskManager.storageManager.Store(pt.ctx,
		&storage.StoreRequest{
			CommonTaskRequest: storage.CommonTaskRequest{
				PeerID: pt.peerID,
				TaskID: pt.taskID,
			},
			MetadataOnly: true,
			TotalPieces:  pt.totalPiece,
		})
	if err != nil {
		pt.Errorf("store metadata error: %s", err)
		return err
	}

	if !pt.peerTaskManager.calculateDigest {
		return nil
	}
	err = pt.storageManager.ValidateDigest(
		&storage.PeerTaskMetadata{
			PeerID: pt.GetPeerID(),
			TaskID: pt.GetTaskID(),
		})
	if err != nil {
		pt.Errorf("validate digest error: %s", err)
		return err
	}
	pt.Debugf("validate digest ok")

	return err
}

func (pt *peerTaskConductor) PublishPieceInfo(pieceNum int32, size uint32) {
	// mark piece ready
	pt.lock.Lock()
	if pt.readyPieces.IsSet(pieceNum) {
		pt.lock.Unlock()
		pt.Warnf("piece %d is already reported, skipped", pieceNum)
		return
	}
	// mark piece processed
	pt.readyPieces.Set(pieceNum)
	pt.completedLength.Add(int64(size))
	pt.lock.Unlock()

	finished := pt.isCompleted()
	if finished {
		pt.Done()
	}
	pt.broker.Publish(
		&pieceInfo{
			num:      pieceNum,
			finished: finished,
		})
}
