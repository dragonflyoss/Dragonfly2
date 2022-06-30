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
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"sync"
	"time"

	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/util"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	"d7y.io/dragonfly/v2/pkg/source"
)

const (
	// TODO implement peer task health check
	// reasonContextCanceled       = "context canceled"
	// reasonReScheduleTimeout     = "wait more available peers from scheduler timeout"
	reasonScheduleTimeout       = "wait first peer packet from scheduler timeout"
	reasonPeerGoneFromScheduler = "scheduler says client should disconnect"
	reasonBackSourceDisabled    = "download from source disabled"

	failedReasonNotSet = "unknown"
)

var errPeerPacketChanged = errors.New("peer packet changed")

var _ Task = (*peerTaskConductor)(nil)

// peerTaskConductor will fetch all pieces from other peers and send pieces info to broker
type peerTaskConductor struct {
	*logger.SugaredLoggerOnWith
	ptm *peerTaskManager
	// ctx is with span info for tracing
	// we use successCh and failCh mark task success or fail
	ctx context.Context
	// piece download uses this context
	pieceDownloadCtx context.Context
	// when back source, cancel all piece download action
	pieceDownloadCancel context.CancelFunc

	// host info about current host
	host *scheduler.PeerHost
	// request is the original PeerTaskRequest
	request *scheduler.PeerTaskRequest

	// needBackSource indicates downloading resource from instead of other peers
	needBackSource *atomic.Bool
	seed           bool

	// pieceManager will be used for downloading piece
	pieceManager    PieceManager
	storageManager  storage.Manager
	peerTaskManager *peerTaskManager

	storage storage.TaskStorageDriver

	// schedule options
	schedulerOption config.SchedulerOption
	schedulerClient schedulerclient.Client

	// peer task meta info
	peerID          string
	taskID          string
	totalPiece      *atomic.Int32
	digest          *atomic.String
	contentLength   *atomic.Int64
	completedLength *atomic.Int64
	usedTraffic     *atomic.Uint64
	header          atomic.Value

	broker *pieceBroker

	sizeScope   base.SizeScope
	singlePiece *scheduler.SinglePiece
	tinyData    *TinyData

	// peerPacketStream stands schedulerclient.PeerPacketStream from scheduler
	peerPacketStream scheduler.Scheduler_ReportPieceResultClient
	// peerPacket is the latest available peers from peerPacketCh
	// Deprecated: remove in future release
	peerPacket      atomic.Value // *scheduler.PeerPacket
	legacyPeerCount *atomic.Int64
	// peerPacketReady will receive a ready signal for peerPacket ready
	peerPacketReady chan bool
	// pieceTaskPoller pulls piece task from other peers
	// Deprecated: pieceTaskPoller is deprecated, use pieceTaskSyncManager
	pieceTaskPoller *pieceTaskPoller
	// pieceTaskSyncManager syncs piece task from other peers
	pieceTaskSyncManager *pieceTaskSyncManager

	// same actions must be done only once, like close done channel and so on
	statusOnce sync.Once
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

	// readyPieces stands all downloaded pieces
	readyPieces *Bitmap
	// lock used by piece result manage, when update readyPieces, lock first
	readyPiecesLock sync.RWMutex
	// runningPieces stands all downloading pieces
	runningPieces *Bitmap
	// lock used by piece download worker
	runningPiecesLock sync.Mutex
	// requestedPieces stands all pieces requested from peers
	requestedPieces *Bitmap
	// lock used by piece download worker
	requestedPiecesLock sync.RWMutex
	// lock used by send piece result
	sendPieceResultLock sync.Mutex
	// limiter will be used when enable per peer task rate limit
	limiter *rate.Limiter

	startTime time.Time

	// subtask only
	parent *peerTaskConductor
	rg     *util.Range
}

func (ptm *peerTaskManager) newPeerTaskConductor(
	ctx context.Context,
	request *scheduler.PeerTaskRequest,
	limit rate.Limit,
	parent *peerTaskConductor,
	rg *util.Range,
	seed bool) *peerTaskConductor {
	// use a new context with span info
	ctx = trace.ContextWithSpan(context.Background(), trace.SpanFromContext(ctx))
	ctx, span := tracer.Start(ctx, config.SpanPeerTask, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(config.AttributePeerHost.String(ptm.host.Id))
	span.SetAttributes(semconv.NetHostIPKey.String(ptm.host.Ip))
	span.SetAttributes(config.AttributePeerID.String(request.PeerId))
	span.SetAttributes(semconv.HTTPURLKey.String(request.Url))

	taskID := idgen.TaskID(request.Url, request.UrlMeta)
	request.TaskId = taskID

	var (
		log     *logger.SugaredLoggerOnWith
		traceID = span.SpanContext().TraceID()
	)

	if traceID.IsValid() {
		log = logger.With(
			"peer", request.PeerId,
			"task", taskID,
			"component", "PeerTask",
			"trace", traceID.String())
	} else {
		log = logger.With(
			"peer", request.PeerId,
			"task", taskID,
			"component", "PeerTask")
	}

	span.SetAttributes(config.AttributeTaskID.String(taskID))

	ptc := &peerTaskConductor{
		ptm:                 ptm,
		startTime:           time.Now(),
		ctx:                 ctx,
		broker:              newPieceBroker(),
		host:                ptm.host,
		request:             request,
		pieceManager:        ptm.pieceManager,
		storageManager:      ptm.storageManager,
		peerTaskManager:     ptm,
		peerPacketReady:     make(chan bool, 1),
		peerID:              request.PeerId,
		taskID:              taskID,
		successCh:           make(chan struct{}),
		failCh:              make(chan struct{}),
		legacyPeerCount:     atomic.NewInt64(0),
		span:                span,
		readyPieces:         NewBitmap(),
		runningPieces:       NewBitmap(),
		requestedPieces:     NewBitmap(),
		failedPieceCh:       make(chan int32, config.DefaultPieceChanSize),
		failedReason:        failedReasonNotSet,
		failedCode:          base.Code_UnknownError,
		contentLength:       atomic.NewInt64(-1),
		totalPiece:          atomic.NewInt32(-1),
		digest:              atomic.NewString(""),
		schedulerOption:     ptm.schedulerOption,
		limiter:             rate.NewLimiter(limit, int(limit)),
		completedLength:     atomic.NewInt64(0),
		usedTraffic:         atomic.NewUint64(0),
		SugaredLoggerOnWith: log,
		seed:                seed,

		parent: parent,
		rg:     rg,
	}

	ptc.pieceTaskPoller = &pieceTaskPoller{
		getPiecesMaxRetry: ptm.getPiecesMaxRetry,
		peerTaskConductor: ptc,
	}

	ptc.pieceDownloadCtx, ptc.pieceDownloadCancel = context.WithCancel(ptc.ctx)

	return ptc
}

// register to scheduler, if error and disable auto back source, return error, otherwise return nil
func (pt *peerTaskConductor) register() error {
	pt.Debugf("request overview, pid: %s, url: %s, filter: %s, tag: %s, range: %s, digest: %s, header: %#v",
		pt.request.PeerId, pt.request.Url, pt.request.UrlMeta.Filter, pt.request.UrlMeta.Tag, pt.request.UrlMeta.Range, pt.request.UrlMeta.Digest, pt.request.UrlMeta.Header)
	// trace register
	regCtx, cancel := context.WithTimeout(pt.ctx, pt.peerTaskManager.schedulerOption.ScheduleTimeout.Duration)
	defer cancel()
	regCtx, regSpan := tracer.Start(regCtx, config.SpanRegisterTask)

	var (
		needBackSource bool
		sizeScope      base.SizeScope
		singlePiece    *scheduler.SinglePiece
		tinyData       *TinyData
	)

	pt.Infof("step 1: peer %s start to register", pt.request.PeerId)
	pt.schedulerClient = pt.peerTaskManager.schedulerClient

	result, err := pt.schedulerClient.RegisterPeerTask(regCtx, pt.request)
	regSpan.RecordError(err)
	regSpan.End()

	if err != nil {
		if err == context.DeadlineExceeded {
			pt.Errorf("scheduler did not response in %s", pt.peerTaskManager.schedulerOption.ScheduleTimeout.Duration)
		}
		pt.Errorf("step 1: peer %s register failed: %s", pt.request.PeerId, err)
		if pt.peerTaskManager.schedulerOption.DisableAutoBackSource {
			// when peer register failed, some actions need to do with peerPacketStream
			pt.peerPacketStream = &dummyPeerPacketStream{}
			pt.Errorf("register peer task failed: %s, peer id: %s, auto back source disabled", err, pt.request.PeerId)
			pt.span.RecordError(err)
			pt.cancel(base.Code_SchedError, err.Error())
			return err
		}
		needBackSource = true
		// can not detect source or scheduler error, create a new dummy scheduler client
		pt.schedulerClient = &dummySchedulerClient{}
		result = &scheduler.RegisterResult{TaskId: pt.taskID}
		pt.Warnf("register peer task failed: %s, peer id: %s, try to back source", err, pt.request.PeerId)
	} else {
		pt.Infof("register task success, SizeScope: %s", base.SizeScope_name[int32(result.SizeScope)])
	}

	var header map[string]string
	if !needBackSource {
		sizeScope = result.SizeScope
		switch result.SizeScope {
		case base.SizeScope_NORMAL:
			pt.span.SetAttributes(config.AttributePeerTaskSizeScope.String("normal"))
		case base.SizeScope_SMALL:
			pt.span.SetAttributes(config.AttributePeerTaskSizeScope.String("small"))
			if piece, ok := result.DirectPiece.(*scheduler.RegisterResult_SinglePiece); ok {
				singlePiece = piece.SinglePiece
			}
			if result.ExtendAttribute != nil {
				header = result.ExtendAttribute.Header
			}
		case base.SizeScope_TINY:
			pt.span.SetAttributes(config.AttributePeerTaskSizeScope.String("tiny"))
			if piece, ok := result.DirectPiece.(*scheduler.RegisterResult_PieceContent); ok {
				tinyData = &TinyData{
					TaskID:  result.TaskId,
					PeerID:  pt.request.PeerId,
					Content: piece.PieceContent,
				}
			} else {
				err = errors.New("scheduler return tiny piece but can not parse piece content")
				// when peer register failed, some actions need to do with peerPacketStream
				pt.peerPacketStream = &dummyPeerPacketStream{}
				pt.span.RecordError(err)
				pt.Errorf("%s", err)
				pt.cancel(base.Code_SchedError, err.Error())
				return err
			}
			if result.ExtendAttribute != nil {
				header = result.ExtendAttribute.Header
			}
		}
	}

	peerPacketStream, err := pt.schedulerClient.ReportPieceResult(pt.ctx, pt.request)
	pt.Infof("step 2: start report piece result")
	if err != nil {
		// when peer register failed, some actions need to do with peerPacketStream
		pt.peerPacketStream = &dummyPeerPacketStream{}
		pt.span.RecordError(err)
		pt.cancel(base.Code_SchedError, err.Error())
		return err
	}

	pt.peerPacketStream = peerPacketStream
	pt.sizeScope = sizeScope
	pt.singlePiece = singlePiece
	pt.tinyData = tinyData
	pt.needBackSource = atomic.NewBool(needBackSource)

	if len(header) > 0 {
		pt.SetHeader(header)
	}
	return nil
}

func (pt *peerTaskConductor) start() error {
	// when is seed task, setup back source
	if pt.seed {
		pt.peerPacketStream = &dummyPeerPacketStream{}
		pt.schedulerClient = &dummySchedulerClient{}
		pt.sizeScope = base.SizeScope_NORMAL
		pt.needBackSource = atomic.NewBool(true)
	} else {
		// register to scheduler
		if err := pt.register(); err != nil {
			return err
		}
	}

	go pt.broker.Start()
	go pt.pullPieces()
	return nil
}

func (pt *peerTaskConductor) GetPeerID() string {
	return pt.peerID
}

func (pt *peerTaskConductor) GetTaskID() string {
	return pt.taskID
}

func (pt *peerTaskConductor) GetStorage() storage.TaskStorageDriver {
	return pt.storage
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
	return pt.totalPiece.Load()
}

func (pt *peerTaskConductor) SetTotalPieces(i int32) {
	pt.totalPiece.Store(i)
}

func (pt *peerTaskConductor) SetPieceMd5Sign(md5 string) {
	pt.digest.Store(md5)
}

func (pt *peerTaskConductor) GetPieceMd5Sign() string {
	return pt.digest.Load()
}

func (pt *peerTaskConductor) SetHeader(header map[string]string) {
	var hdr = &source.Header{}
	for k, v := range header {
		hdr.Set(k, v)
	}
	pt.header.Store(hdr)
}

func (pt *peerTaskConductor) GetHeader() *source.Header {
	hdr := pt.header.Load()
	if hdr != nil {
		return hdr.(*source.Header)
	}
	return nil
}

func (pt *peerTaskConductor) Context() context.Context {
	return pt.ctx
}

func (pt *peerTaskConductor) Log() *logger.SugaredLoggerOnWith {
	return pt.SugaredLoggerOnWith
}

func (pt *peerTaskConductor) cancel(code base.Code, reason string) {
	pt.statusOnce.Do(func() {
		pt.failedCode = code
		pt.failedReason = reason
		pt.fail()
	})
}

// only use when receive back source code from scheduler
func (pt *peerTaskConductor) markBackSource() {
	pt.needBackSource.Store(true)
	// when close peerPacketReady, pullPiecesFromPeers will invoke backSource
	close(pt.peerPacketReady)
	// let legacy mode exit
	pt.peerPacket.Store(&scheduler.PeerPacket{
		TaskId:        pt.taskID,
		SrcPid:        pt.peerID,
		ParallelCount: 1,
		MainPeer:      nil,
		StealPeers: []*scheduler.PeerPacket_DestPeer{
			{
				Ip:      pt.host.Ip,
				RpcPort: pt.host.RpcPort,
				PeerId:  pt.peerID,
			},
		},
		Code: base.Code_SchedNeedBackSource,
	})
}

// only use when legacy get piece from peers schedule timeout
func (pt *peerTaskConductor) forceBackSource() {
	pt.needBackSource.Store(true)
	pt.backSource()
}

func (pt *peerTaskConductor) backSource() {
	// cancel all piece download
	pt.pieceDownloadCancel()
	// cancel all sync pieces
	if pt.pieceTaskSyncManager != nil {
		pt.pieceTaskSyncManager.cancel()
	}

	ctx, span := tracer.Start(pt.ctx, config.SpanBackSource)
	pt.SetContentLength(-1)
	err := pt.pieceManager.DownloadSource(ctx, pt, pt.request)
	if err != nil {
		pt.Errorf("download from source error: %s", err)
		span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		span.RecordError(err)
		if isBackSourceError(err) {
			pt.cancel(base.Code_ClientBackSourceError, err.Error())
		} else {
			pt.cancel(base.Code_ClientError, err.Error())
		}
		span.End()
		return
	}
	pt.Done()
	pt.Infof("download from source ok")
	span.SetAttributes(config.AttributePeerTaskSuccess.Bool(true))
	span.End()
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
		pt.pullSinglePiece()
	case base.SizeScope_NORMAL:
		pt.pullPiecesWithP2P()
	default:
		pt.cancel(base.Code_ClientError, fmt.Sprintf("unknown size scope: %d", pt.sizeScope))
	}
}

func (pt *peerTaskConductor) pullPiecesWithP2P() {
	var (
		// keep same size with pt.failedPieceCh for avoiding deadlock
		pieceBufferSize = uint32(config.DefaultPieceChanSize)
		pieceRequestCh  = make(chan *DownloadPieceRequest, pieceBufferSize)
	)
	ctx, cancel := context.WithCancel(pt.ctx)

	pt.pieceTaskSyncManager = &pieceTaskSyncManager{
		ctx:               ctx,
		ctxCancel:         cancel,
		peerTaskConductor: pt,
		pieceRequestCh:    pieceRequestCh,
		workers:           map[string]*pieceTaskSynchronizer{},
	}
	go pt.pullPiecesFromPeers(pieceRequestCh)
	pt.receivePeerPacket(pieceRequestCh)
}

func (pt *peerTaskConductor) storeTinyPeerTask() {
	contentLength := int64(len(pt.tinyData.Content))
	pt.SetContentLength(contentLength)
	pt.SetTotalPieces(1)
	ctx := pt.ctx
	var err error
	storageDriver, err := pt.peerTaskManager.storageManager.RegisterTask(ctx,
		&storage.RegisterTaskRequest{
			PeerTaskMetadata: storage.PeerTaskMetadata{
				PeerID: pt.tinyData.PeerID,
				TaskID: pt.tinyData.TaskID,
			},
			DesiredLocation: "",
			ContentLength:   contentLength,
			TotalPieces:     1,
			// TODO check digest
		})
	pt.storage = storageDriver
	if err != nil {
		pt.Errorf("register tiny data storage failed: %s", err)
		pt.cancel(base.Code_ClientError, err.Error())
		return
	}
	n, err := pt.GetStorage().WritePiece(ctx,
		&storage.WritePieceRequest{
			PeerTaskMetadata: storage.PeerTaskMetadata{
				PeerID: pt.tinyData.PeerID,
				TaskID: pt.tinyData.TaskID,
			},
			PieceMetadata: storage.PieceMetadata{
				Num:    0,
				Md5:    "",
				Offset: 0,
				Range: util.Range{
					Start:  0,
					Length: contentLength,
				},
				Style: 0,
			},
			UnknownLength: false,
			Reader:        bytes.NewBuffer(pt.tinyData.Content),
			GenMetadata: func(n int64) (int32, int64, bool) {
				return 1, contentLength, true
			},
		})
	if err != nil {
		pt.Errorf("write tiny data storage failed: %s", err)
		pt.cancel(base.Code_ClientError, err.Error())
		return
	}
	if n != contentLength {
		pt.Errorf("write tiny data storage failed, want: %d, wrote: %d", contentLength, n)
		pt.cancel(base.Code_ClientError, err.Error())
		return
	}

	err = pt.UpdateStorage()
	if err != nil {
		pt.Errorf("update tiny data storage failed: %s", err)
		pt.cancel(base.Code_ClientError, err.Error())
		return
	}

	pt.Debugf("store tiny data, len: %d", contentLength)
	pt.PublishPieceInfo(0, uint32(contentLength))
}

func (pt *peerTaskConductor) receivePeerPacket(pieceRequestCh chan *DownloadPieceRequest) {
	var (
		lastNotReadyPiece   int32 = 0
		peerPacket          *scheduler.PeerPacket
		err                 error
		firstPacketReceived bool
	)
	// only record first schedule result
	// other schedule result will record as an event in peer task span
	_, firstPeerSpan := tracer.Start(pt.ctx, config.SpanFirstSchedule)
	defer func() {
		if !firstPacketReceived {
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
			if !firstPacketReceived {
				firstPeerSpan.RecordError(err)
			}
			break loop
		}

		pt.Debugf("receive peerPacket %v", peerPacket)
		if peerPacket.Code != base.Code_Success {
			if peerPacket.Code == base.Code_SchedNeedBackSource {
				pt.markBackSource()
				pt.Infof("receive back source code")
				return
			}
			pt.Errorf("receive peer packet with error: %d", peerPacket.Code)
			if pt.isExitPeerPacketCode(peerPacket) {
				pt.Errorf(pt.failedReason)
				pt.cancel(pt.failedCode, pt.failedReason)
				if !firstPacketReceived {
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

		if !firstPacketReceived {
			pt.initDownloadPieceWorkers(peerPacket.ParallelCount, pieceRequestCh)
			firstPeerSpan.SetAttributes(config.AttributeMainPeer.String(peerPacket.MainPeer.PeerId))
			firstPeerSpan.End()
		}
		// updateSynchronizer will update legacy peers to peerPacket.StealPeers only
		lastNotReadyPiece = pt.updateSynchronizer(lastNotReadyPiece, peerPacket)
		if !firstPacketReceived {
			// trigger legacy get piece once to avoid first schedule timeout
			firstPacketReceived = true
		} else if len(peerPacket.StealPeers) == 0 {
			pt.Debugf("no legacy peers, skip to send peerPacketReady")
			pt.legacyPeerCount.Store(0)
			continue
		}

		legacyPeerCount := int64(len(peerPacket.StealPeers))
		pt.Debugf("connect to %d legacy peers", legacyPeerCount)
		pt.legacyPeerCount.Store(legacyPeerCount)

		// legacy mode: update peer packet, then send peerPacketReady
		pt.peerPacket.Store(peerPacket)
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

// updateSynchronizer will convert peers to synchronizer, if failed, will update failed peers to scheduler.PeerPacket
func (pt *peerTaskConductor) updateSynchronizer(lastNum int32, p *scheduler.PeerPacket) int32 {
	desiredPiece, ok := pt.getNextNotReadyPieceNum(lastNum)
	if !ok {
		pt.Infof("all pieces is ready, peer task completed, skip to synchronize")
		p.MainPeer = nil
		p.StealPeers = nil
		return desiredPiece
	}
	var peers = []*scheduler.PeerPacket_DestPeer{p.MainPeer}
	peers = append(peers, p.StealPeers...)

	legacyPeers := pt.pieceTaskSyncManager.newMultiPieceTaskSynchronizer(peers, desiredPiece)

	p.MainPeer = nil
	p.StealPeers = legacyPeers
	return desiredPiece
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
		pt.markBackSource()
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
	case base.Code_CDNTaskRegistryFail:
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

	pt.SetContentLength(int64(pt.singlePiece.PieceInfo.RangeSize))
	pt.SetTotalPieces(1)
	pt.SetPieceMd5Sign(digest.SHA256FromStrings(pt.singlePiece.PieceInfo.PieceMd5))

	request := &DownloadPieceRequest{
		storage: pt.GetStorage(),
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
		span.RecordError(err)
		span.SetAttributes(config.AttributePieceSuccess.Bool(false))
		span.End()

		pt.Warnf("single piece download failed, switch to download from other peers")
		pt.ReportPieceResult(request, result, err)

		pt.pullPiecesWithP2P()
	}
}

// Deprecated
func (pt *peerTaskConductor) pullPiecesFromPeers(pieceRequestCh chan *DownloadPieceRequest) {
	if ok, backSource := pt.waitFirstPeerPacket(); !ok {
		if backSource {
			return
		}
		pt.Errorf("wait first peer packet error")
		return
	}
	var (
		num   int32
		ok    bool
		limit uint32
	)

	// ensure first peer packet is not nil
	peerPacket := pt.peerPacket.Load().(*scheduler.PeerPacket)
	if len(peerPacket.StealPeers) == 0 {
		num, ok = pt.waitAvailablePeerPacket()
		if !ok {
			return
		}
	}

	limit = config.DefaultPieceChanSize
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

	retry:
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

		pt.updateMetadata(piecePacket)

		// 3. dispatch piece request to all workers
		pt.dispatchPieceRequest(pieceRequestCh, piecePacket)

		// 4. get next not request piece
		if num, ok = pt.getNextPieceNum(num); ok {
			// get next piece success
			limit = config.DefaultPieceChanSize
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
		goto retry
	}
}

func (pt *peerTaskConductor) updateMetadata(piecePacket *base.PiecePacket) {
	// update total piece
	var metadataChanged bool
	if piecePacket.TotalPiece > pt.GetTotalPieces() {
		metadataChanged = true
		pt.SetTotalPieces(piecePacket.TotalPiece)
		pt.Debugf("update total piece count: %d", piecePacket.TotalPiece)
	}

	// update digest
	if len(piecePacket.PieceMd5Sign) > 0 && len(pt.GetPieceMd5Sign()) == 0 {
		metadataChanged = true
		pt.SetPieceMd5Sign(piecePacket.PieceMd5Sign)
		pt.Debugf("update digest: %s", piecePacket.PieceMd5Sign)
	}

	// update content length
	if piecePacket.ContentLength > -1 && pt.GetContentLength() == -1 {
		metadataChanged = true
		pt.SetContentLength(piecePacket.ContentLength)
		pt.span.SetAttributes(config.AttributeTaskContentLength.Int64(piecePacket.ContentLength))
		pt.Debugf("update content length: %d", piecePacket.ContentLength)
	}

	if piecePacket.ExtendAttribute != nil && len(piecePacket.ExtendAttribute.Header) > 0 && pt.GetHeader() == nil {
		metadataChanged = true
		pt.SetHeader(piecePacket.ExtendAttribute.Header)
		pt.Debugf("update response header: %#v", piecePacket.ExtendAttribute.Header)
	}

	if metadataChanged {
		err := pt.UpdateStorage()
		if err != nil {
			pt.Errorf("update storage error: %s", err)
		}
	}
}

func (pt *peerTaskConductor) initDownloadPieceWorkers(count int32, pieceRequestCh chan *DownloadPieceRequest) {
	if count < 1 {
		count = 4
	}
	for i := int32(0); i < count; i++ {
		go pt.downloadPieceWorker(i, pieceRequestCh)
	}
}

func (pt *peerTaskConductor) waitFirstPeerPacket() (done bool, backSource bool) {
	// wait first available peer
	select {
	case _, ok := <-pt.peerPacketReady:
		if ok {
			// preparePieceTasksByPeer func already send piece result with error
			pt.Infof("new peer client ready, scheduler time cost: %dus, peer count: %d",
				time.Since(pt.startTime).Microseconds(), len(pt.peerPacket.Load().(*scheduler.PeerPacket).StealPeers))
			return true, false
		}
		// when scheduler says base.Code_SchedNeedBackSource, receivePeerPacket will close pt.peerPacketReady
		pt.Infof("start download from source due to base.Code_SchedNeedBackSource")
		pt.span.AddEvent("back source due to scheduler says need back source")
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
		pt.forceBackSource()
		return false, true
	}
}

// Deprecated
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
			pt.Infof("new peer client ready, peer count: %d", len(pt.peerPacket.Load().(*scheduler.PeerPacket).StealPeers))
			// research from piece 0
			return 0, true
		}
		// when scheduler says base.Code_SchedNeedBackSource, receivePeerPacket will close pt.peerPacketReady
		pt.Infof("start download from source due to base.Code_SchedNeedBackSource")
		pt.span.AddEvent("back source due to scheduler says need back source ")
		// TODO optimize back source when already downloaded some pieces
		pt.backSource()
	}
	return -1, false
}

// Deprecated
func (pt *peerTaskConductor) dispatchPieceRequest(pieceRequestCh chan *DownloadPieceRequest, piecePacket *base.PiecePacket) {
	pieceCount := len(piecePacket.PieceInfos)
	pt.Debugf("dispatch piece request, piece count: %d", pieceCount)
	// fix cdn return zero piece info, but with total piece count and content length
	if pieceCount == 0 {
		finished := pt.isCompleted()
		if finished {
			pt.Done()
		}
	}
	for _, piece := range piecePacket.PieceInfos {
		pt.Infof("get piece %d from %s/%s, digest: %s, start: %d, size: %d",
			piece.PieceNum, piecePacket.DstAddr, piecePacket.DstPid, piece.PieceMd5, piece.RangeStart, piece.RangeSize)
		// FIXME when set total piece but no total digest, fetch again
		pt.requestedPiecesLock.Lock()
		if !pt.requestedPieces.IsSet(piece.PieceNum) {
			pt.requestedPieces.Set(piece.PieceNum)
		}
		pt.requestedPiecesLock.Unlock()
		req := &DownloadPieceRequest{
			storage: pt.GetStorage(),
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
wait:
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
	case _, ok := <-pt.peerPacketReady:
		if ok {
			// preparePieceTasksByPeer func already send piece result with error
			pt.Infof("new peer client ready, but all pieces are already downloading, just wait failed pieces")
			goto wait
		}
		// when scheduler says base.Code_SchedNeedBackSource, receivePeerPacket will close pt.peerPacketReady
		pt.Infof("start download from source due to base.Code_SchedNeedBackSource")
		pt.span.AddEvent("back source due to scheduler says need back source")
		pt.backSource()
		return -1, false
	}
}

func (pt *peerTaskConductor) downloadPieceWorker(id int32, requests chan *DownloadPieceRequest) {
	for {
		select {
		case request := <-requests:
			pt.readyPiecesLock.RLock()
			if pt.readyPieces.IsSet(request.piece.PieceNum) {
				pt.readyPiecesLock.RUnlock()
				pt.Log().Debugf("piece %d is already downloaded, skip", request.piece.PieceNum)
				continue
			}
			pt.readyPiecesLock.RUnlock()
			pt.downloadPiece(id, request)
		case <-pt.pieceDownloadCtx.Done():
			pt.Infof("piece download cancelled, peer download worker #%d exit", id)
			return
		case <-pt.successCh:
			pt.Infof("peer task success, peer download worker #%d exit", id)
			return
		case <-pt.failCh:
			pt.Errorf("peer task fail, peer download worker #%d exit", id)
			return
		}
	}
}

func (pt *peerTaskConductor) downloadPiece(workerID int32, request *DownloadPieceRequest) {
	// only downloading piece in one worker at same time
	pt.runningPiecesLock.Lock()
	if pt.runningPieces.IsSet(request.piece.PieceNum) {
		pt.runningPiecesLock.Unlock()
		pt.Log().Debugf("piece %d is downloading, skip", request.piece.PieceNum)
		// TODO save to queue for failed pieces
		return
	}
	pt.runningPieces.Set(request.piece.PieceNum)
	pt.runningPiecesLock.Unlock()

	defer func() {
		pt.runningPiecesLock.Lock()
		pt.runningPieces.Clean(request.piece.PieceNum)
		pt.runningPiecesLock.Unlock()
	}()

	ctx, span := tracer.Start(pt.pieceDownloadCtx, fmt.Sprintf(config.SpanDownloadPiece, request.piece.PieceNum))
	span.SetAttributes(config.AttributePiece.Int(int(request.piece.PieceNum)))
	span.SetAttributes(config.AttributePieceWorker.Int(int(workerID)))

	// wait limit
	if pt.limiter != nil && !pt.waitLimit(ctx, request) {
		span.SetAttributes(config.AttributePieceSuccess.Bool(false))
		span.End()
		return
	}

	pt.Debugf("peer download worker #%d receive piece task, "+
		"dest peer id: %s, piece num: %d, range start: %d, range size: %d",
		workerID, request.DstPid, request.piece.PieceNum, request.piece.RangeStart, request.piece.RangeSize)
	// download piece
	// result is always not nil, pieceManager will report begin and end time
	result, err := pt.pieceManager.DownloadPiece(ctx, request)
	if err != nil {
		pt.ReportPieceResult(request, result, err)
		span.SetAttributes(config.AttributePieceSuccess.Bool(false))
		span.End()
		if pt.needBackSource.Load() {
			pt.Infof("switch to back source, skip send failed piece")
			return
		}
		attempt, success := pt.pieceTaskSyncManager.acquire(
			&base.PieceTaskRequest{
				Limit:    1,
				TaskId:   pt.taskID,
				SrcPid:   pt.peerID,
				StartNum: uint32(request.piece.PieceNum),
			})
		pt.Infof("send failed piece %d to remote, attempt: %d, success: %d",
			request.piece.PieceNum, attempt, success)

		// when there is no legacy peers, skip send to failedPieceCh for legacy peers in background
		if pt.legacyPeerCount.Load() == 0 {
			pt.Infof("there is no legacy peers, skip send to failedPieceCh for legacy peers")
			return
		}
		// Deprecated
		// send to fail chan and retry
		// try to send directly first, if failed channel is busy, create a new goroutine to do this
		select {
		case pt.failedPieceCh <- request.piece.PieceNum:
			pt.Infof("success to send failed piece %d to failedPieceCh", request.piece.PieceNum)
		default:
			pt.Infof("start to send failed piece %d to failedPieceCh in background", request.piece.PieceNum)
			go func() {
				pt.failedPieceCh <- request.piece.PieceNum
				pt.Infof("success to send failed piece %d to failedPieceCh in background", request.piece.PieceNum)
			}()
		}
		return
	}
	// broadcast success piece
	pt.reportSuccessResult(request, result)
	pt.PublishPieceInfo(request.piece.PieceNum, request.piece.RangeSize)

	span.SetAttributes(config.AttributePieceSuccess.Bool(true))
	span.End()
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
	sendError := pt.sendPieceResult(&scheduler.PieceResult{
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
	if pt.completedLength.Load() == pt.GetContentLength() {
		pt.Infof("completed content length: %d", pt.completedLength.Load())
		return true
	}

	return false
}

// for legacy peers only
func (pt *peerTaskConductor) getNextPieceNum(cur int32) (int32, bool) {
	if pt.isCompleted() {
		return -1, false
	}
	i := cur
	// try to find next not requested piece
	pt.requestedPiecesLock.RLock()
	defer pt.requestedPiecesLock.RUnlock()

	for ; pt.requestedPieces.IsSet(i); i++ {
	}
	totalPiece := pt.GetTotalPieces()
	if totalPiece > 0 && i >= totalPiece {
		// double check, re-search not success or not requested pieces
		for i = int32(0); pt.requestedPieces.IsSet(i); i++ {
		}
		if totalPiece > 0 && i >= totalPiece {
			return -1, false
		}
	}
	return i, true
}

func (pt *peerTaskConductor) getNextNotReadyPieceNum(cur int32) (int32, bool) {
	if pt.isCompleted() {
		return 0, false
	}
	i := cur
	// try to find next not ready piece
	pt.readyPiecesLock.RLock()
	defer pt.readyPiecesLock.RUnlock()

	for ; pt.readyPieces.IsSet(i); i++ {
	}
	totalPiece := pt.GetTotalPieces()
	if totalPiece > 0 && i >= totalPiece {
		// double check, re-search
		for i = int32(0); pt.readyPieces.IsSet(i); i++ {
		}
		if totalPiece > 0 && i >= totalPiece {
			return 0, false
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
	} else if isBackSourceError(err) {
		code = base.Code_ClientBackSourceError
	}
	pt.reportFailResult(request, result, code)
}

func (pt *peerTaskConductor) reportSuccessResult(request *DownloadPieceRequest, result *DownloadPieceResult) {
	metrics.PieceTaskCount.Add(1)
	_, span := tracer.Start(pt.ctx, config.SpanReportPieceResult)
	span.SetAttributes(config.AttributeWritePieceSuccess.Bool(true))

	err := pt.sendPieceResult(
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
		span.RecordError(err)
	}

	span.End()
}

func (pt *peerTaskConductor) reportFailResult(request *DownloadPieceRequest, result *DownloadPieceResult, code base.Code) {
	metrics.PieceTaskFailedCount.Add(1)
	_, span := tracer.Start(pt.ctx, config.SpanReportPieceResult)
	span.SetAttributes(config.AttributeWritePieceSuccess.Bool(false))

	err := pt.sendPieceResult(&scheduler.PieceResult{
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

func (pt *peerTaskConductor) initStorage(desiredLocation string) (err error) {
	// prepare storage
	if pt.parent == nil {
		pt.storage, err = pt.storageManager.RegisterTask(pt.ctx,
			&storage.RegisterTaskRequest{
				PeerTaskMetadata: storage.PeerTaskMetadata{
					PeerID: pt.GetPeerID(),
					TaskID: pt.GetTaskID(),
				},
				DesiredLocation: desiredLocation,
				ContentLength:   pt.GetContentLength(),
				TotalPieces:     pt.GetTotalPieces(),
				PieceMd5Sign:    pt.GetPieceMd5Sign(),
			})
	} else {
		pt.storage, err = pt.storageManager.RegisterSubTask(pt.ctx,
			&storage.RegisterSubTaskRequest{
				Parent: storage.PeerTaskMetadata{
					PeerID: pt.parent.GetPeerID(),
					TaskID: pt.parent.GetTaskID(),
				},
				SubTask: storage.PeerTaskMetadata{
					PeerID: pt.GetPeerID(),
					TaskID: pt.GetTaskID(),
				},
				Range: pt.rg,
			})
	}
	if err != nil {
		pt.Log().Errorf("register task to storage manager failed: %s", err)
	}
	return err
}

func (pt *peerTaskConductor) UpdateStorage() error {
	// update storage
	err := pt.GetStorage().UpdateTask(pt.ctx,
		&storage.UpdateTaskRequest{
			PeerTaskMetadata: storage.PeerTaskMetadata{
				PeerID: pt.GetPeerID(),
				TaskID: pt.GetTaskID(),
			},
			ContentLength: pt.GetContentLength(),
			TotalPieces:   pt.GetTotalPieces(),
			PieceMd5Sign:  pt.GetPieceMd5Sign(),
			Header:        pt.GetHeader(),
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
	defer func() {
		pt.broker.Stop()
		pt.span.End()
		pt.pieceDownloadCancel()
		if pt.pieceTaskSyncManager != nil {
			pt.pieceTaskSyncManager.cancel()
		}
	}()
	var (
		cost    = time.Since(pt.startTime).Milliseconds()
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
			metrics.PeerTaskFailedCount.WithLabelValues(metrics.FailTypeP2P).Add(1)
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
		metrics.PeerTaskFailedCount.WithLabelValues(metrics.FailTypeP2P).Add(1)
	}

	pt.peerTaskManager.PeerTaskDone(pt.taskID)
	peerResultCtx, peerResultSpan := tracer.Start(pt.ctx, config.SpanReportPeerResult)
	defer peerResultSpan.End()

	// send EOF piece result to scheduler
	err := pt.sendPieceResult(
		schedulerclient.NewEndOfPiece(pt.taskID, pt.peerID, pt.readyPieces.Settled()))
	pt.Debugf("peer task finished, end piece result sent result: %v", err)

	err = pt.peerPacketStream.CloseSend()
	pt.Debugf("close stream result: %v", err)

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
			TotalPieceCount: pt.GetTotalPieces(),
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
	if pt.failedCode == base.Code_ClientBackSourceError {
		metrics.PeerTaskFailedCount.WithLabelValues(metrics.FailTypeBackSource).Add(1)
	} else {
		metrics.PeerTaskFailedCount.WithLabelValues(metrics.FailTypeP2P).Add(1)
	}
	defer func() {
		close(pt.failCh)
		pt.broker.Stop()
		pt.span.End()
		pt.pieceDownloadCancel()
		if pt.pieceTaskSyncManager != nil {
			pt.pieceTaskSyncManager.cancel()
		}
	}()
	pt.peerTaskManager.PeerTaskDone(pt.taskID)
	var end = time.Now()
	pt.Log().Errorf("peer task failed, code: %d, reason: %s", pt.failedCode, pt.failedReason)

	// send EOF piece result to scheduler
	err := pt.sendPieceResult(
		schedulerclient.NewEndOfPiece(pt.taskID, pt.peerID, pt.readyPieces.Settled()))
	pt.Debugf("end piece result sent: %v, peer task finished", err)

	err = pt.peerPacketStream.CloseSend()
	pt.Debugf("close stream result: %v", err)

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
			TotalPieceCount: pt.GetTotalPieces(),
			Cost:            uint32(end.Sub(pt.startTime).Milliseconds()),
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
	err := pt.GetStorage().Store(pt.ctx,
		&storage.StoreRequest{
			CommonTaskRequest: storage.CommonTaskRequest{
				PeerID: pt.peerID,
				TaskID: pt.taskID,
			},
			MetadataOnly: true,
			TotalPieces:  pt.GetTotalPieces(),
		})
	if err != nil {
		pt.Errorf("store metadata error: %s", err)
		return err
	}

	if !pt.peerTaskManager.calculateDigest {
		return nil
	}
	err = pt.GetStorage().ValidateDigest(
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
	pt.readyPiecesLock.Lock()
	if pt.readyPieces.IsSet(pieceNum) {
		pt.readyPiecesLock.Unlock()
		pt.Warnf("piece %d is already reported, skipped", pieceNum)
		return
	}
	// mark piece processed
	pt.readyPieces.Set(pieceNum)
	pt.completedLength.Add(int64(size))
	pt.readyPiecesLock.Unlock()

	finished := pt.isCompleted()
	if finished {
		pt.Done()
	}
	pt.broker.Publish(
		&PieceInfo{
			Num:      pieceNum,
			Finished: finished,
		})
}

func (pt *peerTaskConductor) sendPieceResult(pr *scheduler.PieceResult) error {
	pt.sendPieceResultLock.Lock()
	err := pt.peerPacketStream.Send(pr)
	pt.sendPieceResultLock.Unlock()
	return err
}
