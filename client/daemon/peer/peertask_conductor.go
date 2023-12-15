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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	errordetailsv1 "d7y.io/api/v2/pkg/apis/errordetails/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/idgen"
	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/rpc/common"
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

var _ Task = (*peerTaskConductor)(nil)

// peerTaskConductor will fetch all pieces from other peers and send pieces info to broker
type peerTaskConductor struct {
	TaskOption
	*logger.SugaredLoggerOnWith

	// ctx is with span info for tracing
	// we use successCh and failCh mark task success or fail
	ctx       context.Context
	ctxCancel context.CancelFunc
	// piece download uses this context
	pieceDownloadCtx context.Context
	// when back source, cancel all piece download action
	pieceDownloadCancel context.CancelFunc

	// request is the original PeerTaskRequest
	request *schedulerv1.PeerTaskRequest

	// needBackSource indicates downloading resource from instead of other peers
	needBackSource *atomic.Bool
	seed           bool

	peerTaskManager *peerTaskManager

	storage storage.TaskStorageDriver

	schedulerClient schedulerclient.V1

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

	sizeScope   commonv1.SizeScope
	singlePiece *schedulerv1.SinglePiece
	tinyData    *TinyData

	// peerPacketStream stands schedulerclient.PeerPacketStream from scheduler
	peerPacketStream schedulerv1.Scheduler_ReportPieceResultClient
	legacyPeerCount  *atomic.Int64
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

	// failedReason will be set when peer task failed
	failedReason string
	// failedReason will be set when peer task failed
	failedCode commonv1.Code

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
	// trafficShaper used to automatically allocate bandwidth for every peer task
	trafficShaper TrafficShaper
	// limiter will be used when enable per peer task rate limit
	limiter *rate.Limiter

	startTime time.Time

	// subtask only
	parent *peerTaskConductor
	rg     *nethttp.Range

	sourceErrorStatus *status.Status
}

type TaskOption struct {
	// PeerHost info about current PeerHost
	PeerHost *schedulerv1.PeerHost
	// PieceManager will be used for downloading piece
	PieceManager   PieceManager
	StorageManager storage.Manager
	// schedule options
	SchedulerOption config.SchedulerOption
	CalculateDigest bool
	GRPCCredentials credentials.TransportCredentials
	GRPCDialTimeout time.Duration
	// WatchdogTimeout > 0 indicates to start watch dog for every single peer task
	WatchdogTimeout time.Duration
}

func (ptm *peerTaskManager) newPeerTaskConductor(
	ctx context.Context,
	request *schedulerv1.PeerTaskRequest,
	limit rate.Limit,
	parent *peerTaskConductor,
	rg *nethttp.Range,
	seed bool) *peerTaskConductor {
	// use a new context with span info
	ctx = trace.ContextWithSpan(context.Background(), trace.SpanFromContext(ctx))
	ctx, span := tracer.Start(ctx, config.SpanPeerTask, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(config.AttributePeerHost.String(ptm.PeerHost.Id))
	span.SetAttributes(semconv.NetHostIPKey.String(ptm.PeerHost.Ip))
	span.SetAttributes(config.AttributePeerID.String(request.PeerId))
	span.SetAttributes(semconv.HTTPURLKey.String(request.Url))

	taskID := idgen.TaskIDV1(request.Url, request.UrlMeta)
	request.TaskId = taskID

	// init log with values
	var (
		log     *logger.SugaredLoggerOnWith
		traceID = span.SpanContext().TraceID()
	)

	logKV := []any{
		"peer", request.PeerId,
		"task", taskID,
		"component", "PeerTask",
	}
	if traceID.IsValid() {
		logKV = append(logKV, "trace", traceID.String())
	}
	log = logger.With(logKV...)

	span.SetAttributes(config.AttributeTaskID.String(taskID))

	ctx, cancel := context.WithCancel(ctx)
	ptc := &peerTaskConductor{
		TaskOption:          ptm.TaskOption,
		peerTaskManager:     ptm,
		request:             request,
		startTime:           time.Now(),
		ctx:                 ctx,
		ctxCancel:           cancel,
		broker:              newPieceBroker(),
		peerID:              request.PeerId,
		taskID:              taskID,
		successCh:           make(chan struct{}),
		failCh:              make(chan struct{}),
		legacyPeerCount:     atomic.NewInt64(0),
		span:                span,
		readyPieces:         NewBitmap(),
		runningPieces:       NewBitmap(),
		requestedPieces:     NewBitmap(),
		failedReason:        failedReasonNotSet,
		failedCode:          commonv1.Code_UnknownError,
		contentLength:       atomic.NewInt64(-1),
		totalPiece:          atomic.NewInt32(-1),
		digest:              atomic.NewString(""),
		trafficShaper:       ptm.trafficShaper,
		limiter:             rate.NewLimiter(limit, int(limit)),
		completedLength:     atomic.NewInt64(0),
		usedTraffic:         atomic.NewUint64(0),
		SugaredLoggerOnWith: log,
		seed:                seed,
		parent:              parent,
		rg:                  rg,
	}

	ptc.pieceDownloadCtx, ptc.pieceDownloadCancel = context.WithCancel(ptc.ctx)

	return ptc
}

// register to scheduler, if error and disable auto back source, return error, otherwise return nil
func (pt *peerTaskConductor) register() error {
	pt.Debugf("request overview, pid: %s, url: %s, filter: %s, tag: %s, range: %s, digest: %s, header: %#v",
		pt.request.PeerId, pt.request.Url, pt.request.UrlMeta.Filter, pt.request.UrlMeta.Tag, pt.request.UrlMeta.Range, pt.request.UrlMeta.Digest, pt.request.UrlMeta.Header)
	// trace register
	regCtx, cancel := context.WithTimeout(pt.ctx, pt.SchedulerOption.ScheduleTimeout.Duration)
	defer cancel()
	regCtx, regSpan := tracer.Start(regCtx, config.SpanRegisterTask)

	var (
		needBackSource bool
		sizeScope      commonv1.SizeScope
		singlePiece    *schedulerv1.SinglePiece
		tinyData       *TinyData
	)

	pt.Infof("step 1: peer %s start to register", pt.request.PeerId)
	pt.schedulerClient = pt.peerTaskManager.SchedulerClient

	result, err := pt.schedulerClient.RegisterPeerTask(regCtx, pt.request)
	regSpan.RecordError(err)
	regSpan.End()

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			pt.Errorf("scheduler did not response in %s", pt.SchedulerOption.ScheduleTimeout.Duration)
		}
		pt.Errorf("step 1: peer %s register failed: %s", pt.request.PeerId, err)
		if pt.SchedulerOption.DisableAutoBackSource {
			// when peer register failed, some actions need to do with peerPacketStream
			pt.peerPacketStream = &dummyPeerPacketStream{}
			pt.Errorf("register peer task failed: %s, peer id: %s, auto back source disabled", err, pt.request.PeerId)
			pt.span.RecordError(err)
			pt.cancel(commonv1.Code_SchedError, err.Error())
			return err
		}
		needBackSource = true
		// can not detect source or scheduler error, create a new dummy scheduler client
		pt.schedulerClient = &dummySchedulerClient{}
		result = &schedulerv1.RegisterResult{TaskId: pt.taskID}
		pt.Warnf("register peer task failed: %s, peer id: %s, try to back source", err, pt.request.PeerId)
	} else {
		pt.Infof("register task success, SizeScope: %s", commonv1.SizeScope_name[int32(result.SizeScope)])
	}

	var header map[string]string
	if !needBackSource {
		sizeScope = result.SizeScope
		switch result.SizeScope {
		case commonv1.SizeScope_NORMAL:
			pt.span.SetAttributes(config.AttributePeerTaskSizeScope.String("normal"))
		case commonv1.SizeScope_SMALL:
			pt.span.SetAttributes(config.AttributePeerTaskSizeScope.String("small"))
			if piece, ok := result.DirectPiece.(*schedulerv1.RegisterResult_SinglePiece); ok {
				singlePiece = piece.SinglePiece
			}
			if result.ExtendAttribute != nil {
				header = result.ExtendAttribute.Header
			}
		case commonv1.SizeScope_TINY:
			pt.span.SetAttributes(config.AttributePeerTaskSizeScope.String("tiny"))
			if piece, ok := result.DirectPiece.(*schedulerv1.RegisterResult_PieceContent); ok {
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
				pt.cancel(commonv1.Code_SchedError, err.Error())
				return err
			}
			if result.ExtendAttribute != nil {
				header = result.ExtendAttribute.Header
			}
		case commonv1.SizeScope_EMPTY:
			tinyData = &TinyData{
				TaskID:  result.TaskId,
				PeerID:  pt.request.PeerId,
				Content: []byte{},
			}
			pt.span.SetAttributes(config.AttributePeerTaskSizeScope.String("empty"))
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
		pt.cancel(commonv1.Code_SchedError, err.Error())
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
		pt.sizeScope = commonv1.SizeScope_NORMAL
		pt.needBackSource = atomic.NewBool(true)
	} else {
		// register to scheduler
		if err := pt.register(); err != nil {
			return err
		}
	}

	pt.trafficShaper.AddTask(pt.peerTaskManager.getRunningTaskKey(pt.taskID, pt.peerID), pt)
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

func (pt *peerTaskConductor) UpdateSourceErrorStatus(st *status.Status) {
	pt.sourceErrorStatus = st
}

func (pt *peerTaskConductor) cancel(code commonv1.Code, reason string) {
	pt.statusOnce.Do(func() {
		pt.failedCode = code
		pt.failedReason = reason
		pt.fail()
	})
}

func (pt *peerTaskConductor) cancelNotRegisterred(code commonv1.Code, reason string) {
	pt.statusOnce.Do(func() {
		pt.failedCode = code
		pt.failedReason = reason

		metrics.PeerTaskFailedCount.WithLabelValues(metrics.FailTypeInit).Add(1)

		pt.peerTaskManager.PeerTaskDone(pt.taskID, pt.peerID)
		pt.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		pt.span.SetAttributes(config.AttributePeerTaskCode.Int(int(pt.failedCode)))
		pt.span.SetAttributes(config.AttributePeerTaskMessage.String(pt.failedReason))

		close(pt.failCh)
		pt.broker.Stop()
		pt.span.End()
		pt.pieceDownloadCancel()
		if pt.pieceTaskSyncManager != nil {
			pt.pieceTaskSyncManager.cancel()
		}
	})
}

// only use when receive back source code from scheduler
func (pt *peerTaskConductor) markBackSource() {
	pt.needBackSource.Store(true)
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
	err := pt.PieceManager.DownloadSource(ctx, pt, pt.request, pt.rg)
	if err != nil {
		pt.Errorf("download from source error: %s", err)
		span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		span.RecordError(err)
		if isBackSourceError(err) {
			pt.cancel(commonv1.Code_ClientBackSourceError, err.Error())
		} else {
			pt.cancel(commonv1.Code_ClientError, err.Error())
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
	case commonv1.SizeScope_EMPTY:
		pt.storeEmptyPeerTask()
	case commonv1.SizeScope_TINY:
		pt.storeTinyPeerTask()
	case commonv1.SizeScope_SMALL:
		pt.pullSinglePiece()
	case commonv1.SizeScope_NORMAL:
		pt.pullPiecesWithP2P()
	default:
		pt.cancel(commonv1.Code_ClientError, fmt.Sprintf("unknown size scope: %d", pt.sizeScope))
	}
}

func (pt *peerTaskConductor) pullPiecesWithP2P() {
	var (
		// keep same size with pt.failedPieceCh for avoiding deadlock
		pieceRequestQueue = NewPieceDispatcher(config.DefaultPieceDispatcherRandomRatio, pt.Log())
	)
	ctx, cancel := context.WithCancel(pt.ctx)

	pt.pieceTaskSyncManager = &pieceTaskSyncManager{
		ctx:               ctx,
		ctxCancel:         cancel,
		peerTaskConductor: pt,
		pieceRequestQueue: pieceRequestQueue,
		workers:           map[string]*pieceTaskSynchronizer{},
	}
	pt.receivePeerPacket(pieceRequestQueue)
}

func (pt *peerTaskConductor) storeEmptyPeerTask() {
	pt.SetContentLength(0)
	pt.SetTotalPieces(0)
	ctx := pt.ctx
	var err error
	storageDriver, err := pt.StorageManager.RegisterTask(ctx,
		&storage.RegisterTaskRequest{
			PeerTaskMetadata: storage.PeerTaskMetadata{
				PeerID: pt.peerID,
				TaskID: pt.taskID,
			},
			DesiredLocation: "",
			ContentLength:   0,
			TotalPieces:     0,
		})
	pt.storage = storageDriver
	if err != nil {
		pt.Errorf("register tiny data storage failed: %s", err)
		pt.cancel(commonv1.Code_ClientError, err.Error())
		return
	}

	if err = pt.UpdateStorage(); err != nil {
		pt.Errorf("update tiny data storage failed: %s", err)
		pt.cancel(commonv1.Code_ClientError, err.Error())
		return
	}
	pt.Debug("store empty metadata")
	pt.Done()
}

func (pt *peerTaskConductor) storeTinyPeerTask() {
	contentLength := int64(len(pt.tinyData.Content))
	pt.SetContentLength(contentLength)
	pt.SetTotalPieces(1)
	ctx := pt.ctx
	var err error
	storageDriver, err := pt.StorageManager.RegisterTask(ctx,
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
		pt.cancel(commonv1.Code_ClientError, err.Error())
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
				Range: nethttp.Range{
					Start:  0,
					Length: contentLength,
				},
				Style: 0,
			},
			UnknownLength: false,
			Reader:        bytes.NewBuffer(pt.tinyData.Content),
			NeedGenMetadata: func(n int64) (int32, int64, bool) {
				return 1, contentLength, true
			},
		})
	if err != nil {
		pt.Errorf("write tiny data storage failed: %s", err)
		pt.cancel(commonv1.Code_ClientError, err.Error())
		return
	}
	if n != contentLength {
		pt.Errorf("write tiny data storage failed, want: %d, wrote: %d", contentLength, n)
		pt.cancel(commonv1.Code_ClientError, err.Error())
		return
	}

	err = pt.UpdateStorage()
	if err != nil {
		pt.Errorf("update tiny data storage failed: %s", err)
		pt.cancel(commonv1.Code_ClientError, err.Error())
		return
	}

	pt.Debugf("store tiny data, len: %d", contentLength)
	pt.PublishPieceInfo(0, uint32(contentLength))
}

func (pt *peerTaskConductor) receivePeerPacket(pieceRequestQueue PieceDispatcher) {
	var (
		lastNotReadyPiece   int32 = 0
		peerPacket          *schedulerv1.PeerPacket
		err                 error
		firstPacketReceived bool
		firstPacketDone     = make(chan bool)
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

	go pt.waitFirstPeerPacket(firstPacketDone)
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
			// some errors, like commonv1.Code_SchedReregister, after reregister success,
			// we can continue to receive peer packet from the new scheduler
			cont := pt.confirmReceivePeerPacketError(err)
			if cont {
				continue
			}
			if !firstPacketReceived {
				firstPeerSpan.RecordError(err)
			}
			break loop
		}

		pt.Debugf("receive peerPacket %v", peerPacket)
		if peerPacket.Code != commonv1.Code_Success {
			if peerPacket.Code == commonv1.Code_SchedNeedBackSource {
				// fix back source directly, then waitFirstPeerPacket timeout
				if !firstPacketReceived {
					close(firstPacketDone)
				}
				pt.forceBackSource()
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

		if peerPacket.MainPeer == nil && peerPacket.CandidatePeers == nil {
			pt.Warnf("scheduler client send a peerPacket with empty peers")
			continue
		}
		pt.Infof("receive new peer packet, main peer: %s", peerPacket.MainPeer.PeerId)
		pt.span.AddEvent("receive new peer packet",
			trace.WithAttributes(config.AttributeMainPeer.String(peerPacket.MainPeer.PeerId)))

		if !firstPacketReceived {
			pt.initDownloadPieceWorkers(pieceRequestQueue)
			firstPeerSpan.SetAttributes(config.AttributeMainPeer.String(peerPacket.MainPeer.PeerId))
			firstPeerSpan.End()
		}

		lastNotReadyPiece = pt.updateSynchronizers(lastNotReadyPiece, peerPacket)
		if !firstPacketReceived {
			// trigger legacy get piece once to avoid first schedule timeout
			firstPacketReceived = true
			close(firstPacketDone)
		}
	}

	// double check to avoid waitFirstPeerPacket timeout
	if !firstPacketReceived {
		close(firstPacketDone)
	}
}

// updateSynchronizers will convert peers to synchronizer, if failed, will update failed peers to schedulerv1.PeerPacket
func (pt *peerTaskConductor) updateSynchronizers(lastNum int32, p *schedulerv1.PeerPacket) int32 {
	desiredPiece, ok := pt.getNextNotReadyPieceNum(lastNum)
	if !ok {
		pt.Infof("all pieces is ready, peer task completed, skip to synchronize")
		p.MainPeer = nil
		p.CandidatePeers = nil
		return desiredPiece
	}
	var peers = []*schedulerv1.PeerPacket_DestPeer{p.MainPeer}
	peers = append(peers, p.CandidatePeers...)

	pt.pieceTaskSyncManager.syncPeers(peers, desiredPiece)
	return desiredPiece
}

func (pt *peerTaskConductor) confirmReceivePeerPacketError(err error) (cont bool) {
	select {
	case <-pt.successCh:
		return false
	case <-pt.failCh:
		return false
	default:
	}
	var (
		failedCode   = commonv1.Code_UnknownError
		failedReason string
	)
	// extract DfError for grpc status
	de, ok := dferrors.IsGRPCDfError(err)
	if ok {
		switch de.Code {
		case commonv1.Code_SchedNeedBackSource:
			pt.forceBackSource()
			pt.Infof("receive back source code")
			return false
		case commonv1.Code_SchedReregister:
			pt.Infof("receive reregister code")
			regErr := pt.register()
			if regErr == nil {
				pt.Infof("reregister ok")
				return true
			}
			pt.Errorf("reregister to scheduler error: %s", regErr)
			fallthrough
		default:
			failedCode = de.Code
			failedReason = de.Message
			pt.Errorf("receive peer packet failed: %s", pt.failedReason)
		}
	} else {
		pt.Errorf("receive peer packet failed: %s", err)
	}
	pt.cancel(failedCode, failedReason)
	return false
}

func (pt *peerTaskConductor) isExitPeerPacketCode(pp *schedulerv1.PeerPacket) bool {
	switch pp.Code {
	case commonv1.Code_ResourceLacked, commonv1.Code_BadRequest,
		commonv1.Code_PeerTaskNotFound, commonv1.Code_UnknownError, commonv1.Code_RequestTimeOut:
		// 1xxx
		pt.failedCode = pp.Code
		pt.failedReason = fmt.Sprintf("receive exit peer packet with code %d", pp.Code)
		return true
	case commonv1.Code_SchedError, commonv1.Code_SchedTaskStatusError, commonv1.Code_SchedPeerNotFound, commonv1.Code_SchedForbidden:
		// 5xxx
		pt.failedCode = pp.Code
		pt.failedReason = fmt.Sprintf("receive exit peer packet with code %d", pp.Code)
		return true
	case commonv1.Code_SchedPeerGone:
		pt.failedReason = reasonPeerGoneFromScheduler
		pt.failedCode = commonv1.Code_SchedPeerGone
		return true
	case commonv1.Code_CDNTaskRegistryFail:
		// 6xxx
		pt.failedCode = pp.Code
		pt.failedReason = fmt.Sprintf("receive exit peer packet with code %d", pp.Code)
		return true
	case commonv1.Code_BackToSourceAborted:
		st := status.Newf(codes.Aborted, "source response is not valid")
		st, err := st.WithDetails(pp.GetSourceError())
		if err != nil {
			pt.Errorf("convert source error details error: %s", err.Error())
			return false
		}

		pt.sourceErrorStatus = st
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

	if result, err := pt.PieceManager.DownloadPiece(ctx, request); err == nil {
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

func (pt *peerTaskConductor) updateMetadata(piecePacket *commonv1.PiecePacket) {
	// update total piece
	var metadataChanged bool
	if piecePacket.TotalPiece > pt.GetTotalPieces() {
		metadataChanged = true
		pt.SetTotalPieces(piecePacket.TotalPiece)
		pt.Debugf("update total piece count: %d, dst peer %s", piecePacket.TotalPiece, piecePacket.DstPid)
	}

	// update digest
	if len(piecePacket.PieceMd5Sign) > 0 && len(pt.GetPieceMd5Sign()) == 0 {
		metadataChanged = true
		pt.SetPieceMd5Sign(piecePacket.PieceMd5Sign)
		pt.Debugf("update digest: %s, dst peer %s", piecePacket.PieceMd5Sign, piecePacket.DstPid)
	}

	// update content length
	if piecePacket.ContentLength > -1 && pt.GetContentLength() == -1 {
		metadataChanged = true
		pt.SetContentLength(piecePacket.ContentLength)
		pt.span.SetAttributes(config.AttributeTaskContentLength.Int64(piecePacket.ContentLength))
		pt.Debugf("update content length: %d, dst peer %s", piecePacket.ContentLength, piecePacket.DstPid)
	} else if piecePacket.ContentLength > -1 && piecePacket.ContentLength != pt.GetContentLength() {
		// corrupt data check
		reason := fmt.Sprintf("corrupt data - content length did not match, current: %d, from piece packet: %d",
			pt.GetContentLength(), piecePacket.ContentLength)
		pt.Errorf(reason)
		pt.cancel(commonv1.Code_ClientError, reason)
		return
	}

	if piecePacket.ExtendAttribute != nil && len(piecePacket.ExtendAttribute.Header) > 0 && pt.GetHeader() == nil {
		metadataChanged = true
		pt.SetHeader(piecePacket.ExtendAttribute.Header)
		pt.Debugf("update response header: %#v, dst peer %s", piecePacket.ExtendAttribute.Header, piecePacket.DstPid)
	}

	if metadataChanged {
		err := pt.UpdateStorage()
		if err != nil {
			pt.Errorf("update storage error: %s", err)
		}
	}
}

func (pt *peerTaskConductor) initDownloadPieceWorkers(pieceRequestQueue PieceDispatcher) {
	count := 4
	for i := int32(0); i < int32(count); i++ {
		go pt.downloadPieceWorker(i, pieceRequestQueue)
	}
}

func (pt *peerTaskConductor) waitFirstPeerPacket(done chan bool) {
	// wait first available peer
	select {
	case <-pt.successCh:
		pt.Infof("peer task succeed, no need to wait first peer")
		return
	case <-pt.failCh:
		pt.Warnf("peer task failed, no need to wait first peer")
		return
	case <-done:
		pt.Debugf("first peer packet received")
		return
	case <-time.After(pt.SchedulerOption.ScheduleTimeout.Duration):
		if pt.SchedulerOption.DisableAutoBackSource {
			pt.cancel(commonv1.Code_ClientScheduleTimeout, reasonBackSourceDisabled)
			err := fmt.Errorf("%s, auto back source disabled", pt.failedReason)
			pt.span.RecordError(err)
			pt.Errorf(err.Error())
			return
		}
		pt.Warnf("start download from source due to %s", reasonScheduleTimeout)
		pt.span.AddEvent("back source due to schedule timeout")
		pt.forceBackSource()
		return
	}
}

func (pt *peerTaskConductor) downloadPieceWorker(id int32, requests PieceDispatcher) {
	for {
		request, err := requests.Get()
		if errors.Is(err, ErrNoValidPieceTemporarily) {
			continue
		}
		if err != nil {
			pt.Infof("piece download queue cancelled, peer download worker #%d exit, err: %v", id, err)
			return
		}
		pt.readyPiecesLock.RLock()
		if pt.readyPieces.IsSet(request.piece.PieceNum) {
			pt.readyPiecesLock.RUnlock()
			pt.Log().Debugf("piece %d is already downloaded, skip", request.piece.PieceNum)
			continue
		}
		pt.readyPiecesLock.RUnlock()
		result := pt.downloadPiece(id, request)
		if result != nil {
			requests.Report(result)
		}
		select {
		case <-pt.pieceDownloadCtx.Done():
			pt.Infof("piece download cancelled, peer download worker #%d exit", id)
			return
		case <-pt.successCh:
			pt.Infof("peer task success, peer download worker #%d exit", id)
			return
		case <-pt.failCh:
			pt.Errorf("peer task fail, peer download worker #%d exit", id)
			return
		default:
		}
	}
}

func (pt *peerTaskConductor) downloadPiece(workerID int32, request *DownloadPieceRequest) *DownloadPieceResult {
	// only downloading piece in one worker at same time
	pt.runningPiecesLock.Lock()
	if pt.runningPieces.IsSet(request.piece.PieceNum) {
		pt.runningPiecesLock.Unlock()
		pt.Log().Debugf("piece %d is downloading, skip", request.piece.PieceNum)
		// TODO save to queue for failed pieces
		return nil
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
		return nil
	}

	pt.Debugf("peer download worker #%d receive piece task, "+
		"dest peer id: %s, piece num: %d, range start: %d, range size: %d",
		workerID, request.DstPid, request.piece.PieceNum, request.piece.RangeStart, request.piece.RangeSize)
	// download piece
	// result is always not nil, PieceManager will report begin and end time
	result, err := pt.PieceManager.DownloadPiece(ctx, request)
	if err != nil {
		pt.ReportPieceResult(request, result, err)
		span.SetAttributes(config.AttributePieceSuccess.Bool(false))
		span.End()
		if pt.needBackSource.Load() {
			pt.Infof("switch to back source, skip send failed piece")
			return result
		}
		attempt, success := pt.pieceTaskSyncManager.acquire(
			&commonv1.PieceTaskRequest{
				Limit:    1,
				TaskId:   pt.taskID,
				SrcPid:   pt.peerID,
				StartNum: uint32(request.piece.PieceNum),
			})
		pt.Infof("send failed piece %d to remote, attempt: %d, success: %d",
			request.piece.PieceNum, attempt, success)
		return result
	}
	// broadcast success piece
	pt.reportSuccessResult(request, result)
	pt.PublishPieceInfo(request.piece.PieceNum, request.piece.RangeSize)

	span.SetAttributes(config.AttributePieceSuccess.Bool(true))
	span.End()
	return result
}

func (pt *peerTaskConductor) waitLimit(ctx context.Context, request *DownloadPieceRequest) bool {
	_, waitSpan := tracer.Start(ctx, config.SpanWaitPieceLimit)
	pt.trafficShaper.Record(pt.peerTaskManager.getRunningTaskKey(request.TaskID, request.PeerID), int(request.piece.RangeSize))
	err := pt.limiter.WaitN(pt.ctx, int(request.piece.RangeSize))
	if err == nil {
		waitSpan.End()
		return true
	}

	pt.Errorf("request limiter error: %s", err)
	waitSpan.RecordError(err)
	waitSpan.End()

	// send error piece result
	sendError := pt.sendPieceResult(&schedulerv1.PieceResult{
		TaskId:        pt.GetTaskID(),
		SrcPid:        pt.GetPeerID(),
		DstPid:        request.DstPid,
		PieceInfo:     request.piece,
		Success:       false,
		Code:          commonv1.Code_ClientRequestLimitFail,
		FinishedCount: 0, // update by peer task
	})
	if sendError != nil {
		pt.Errorf("report piece result failed %s", err)
	}

	pt.cancel(commonv1.Code_ClientRequestLimitFail, err.Error())
	return false
}

func (pt *peerTaskConductor) isCompleted() bool {
	if pt.completedLength.Load() == pt.GetContentLength() {
		pt.Infof("completed content length: %d", pt.completedLength.Load())
		return true
	}

	// corrupt data check and avoid hang for mismatch completed length
	if pt.readyPieces.Settled() == pt.totalPiece.Load() {
		msg := fmt.Sprintf("corrupt data - ready piece count %d seems finished, but completed length %d is not match with content length: %d",
			pt.totalPiece.Load(), pt.completedLength.Load(), pt.GetContentLength())
		pt.Errorf(msg)
		pt.cancel(commonv1.Code_ClientError, msg)
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
	code := commonv1.Code_ClientPieceDownloadFail
	if isConnectionError(err) {
		code = commonv1.Code_ClientConnectionError
	} else if isPieceNotFound(err) {
		code = commonv1.Code_ClientPieceNotFound
	} else if isBackSourceError(err) {
		code = commonv1.Code_ClientBackSourceError
	}
	pt.reportFailResult(request, result, code)
}

func (pt *peerTaskConductor) reportSuccessResult(request *DownloadPieceRequest, result *DownloadPieceResult) {
	metrics.PieceTaskCount.Add(1)
	_, span := tracer.Start(pt.ctx, config.SpanReportPieceResult)
	span.SetAttributes(config.AttributeWritePieceSuccess.Bool(true))

	err := pt.sendPieceResult(
		&schedulerv1.PieceResult{
			TaskId:        pt.GetTaskID(),
			SrcPid:        pt.GetPeerID(),
			DstPid:        request.DstPid,
			PieceInfo:     request.piece,
			BeginTime:     uint64(result.BeginTime),
			EndTime:       uint64(result.FinishTime),
			Success:       true,
			Code:          commonv1.Code_Success,
			FinishedCount: pt.readyPieces.Settled(),
			// TODO range_start, range_size, piece_md5, piece_offset, piece_style
		})
	if err != nil {
		pt.Errorf("report piece task error: %v", err)
		span.RecordError(err)
	}

	span.End()
}

func (pt *peerTaskConductor) reportFailResult(request *DownloadPieceRequest, result *DownloadPieceResult, code commonv1.Code) {
	metrics.PieceTaskFailedCount.Add(1)
	_, span := tracer.Start(pt.ctx, config.SpanReportPieceResult)
	span.SetAttributes(config.AttributeWritePieceSuccess.Bool(false))

	err := pt.sendPieceResult(&schedulerv1.PieceResult{
		TaskId:        pt.GetTaskID(),
		SrcPid:        pt.GetPeerID(),
		DstPid:        request.DstPid,
		PieceInfo:     request.piece,
		BeginTime:     uint64(result.BeginTime),
		EndTime:       uint64(result.FinishTime),
		Success:       false,
		Code:          code,
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
		pt.storage, err = pt.StorageManager.RegisterTask(pt.ctx,
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
		pt.storage, err = pt.StorageManager.RegisterSubTask(pt.ctx,
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
		pt.ctxCancel()
	}()
	var (
		cost    = time.Since(pt.startTime).Milliseconds()
		success = true
		code    = commonv1.Code_Success
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
			code = commonv1.Code_ClientError
			pt.failedCode = commonv1.Code_ClientError
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
		code = commonv1.Code_ClientError
		pt.failedCode = commonv1.Code_ClientError
		pt.failedReason = err.Error()

		pt.span.SetAttributes(config.AttributePeerTaskSuccess.Bool(false))
		pt.span.SetAttributes(config.AttributePeerTaskCode.Int(int(pt.failedCode)))
		pt.span.SetAttributes(config.AttributePeerTaskMessage.String(pt.failedReason))
		pt.Errorf("update storage error: %v", err)
		metrics.PeerTaskFailedCount.WithLabelValues(metrics.FailTypeP2P).Add(1)
	}

	pt.peerTaskManager.PeerTaskDone(pt.taskID, pt.peerID)
	peerResultCtx, peerResultSpan := tracer.Start(pt.ctx, config.SpanReportPeerResult)
	defer peerResultSpan.End()

	// Send EOF piece result to scheduler.
	err := pt.sendPieceResult(
		&schedulerv1.PieceResult{
			TaskId:        pt.taskID,
			SrcPid:        pt.peerID,
			FinishedCount: pt.readyPieces.Settled(),
			PieceInfo: &commonv1.PieceInfo{
				PieceNum: common.EndOfPiece,
			},
		})
	pt.Debugf("peer task finished, end piece result sent result: %v", err)

	err = pt.peerPacketStream.CloseSend()
	pt.Debugf("close stream result: %v", err)

	err = pt.schedulerClient.ReportPeerResult(
		peerResultCtx,
		&schedulerv1.PeerResult{
			TaskId:          pt.GetTaskID(),
			PeerId:          pt.GetPeerID(),
			SrcIp:           pt.PeerHost.Ip,
			Idc:             pt.PeerHost.Idc,
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
	if pt.failedCode == commonv1.Code_ClientBackSourceError {
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
		// mark storage to reclaim
		_ = pt.StorageManager.UnregisterTask(
			pt.ctx,
			storage.CommonTaskRequest{
				PeerID: pt.peerID,
				TaskID: pt.taskID,
			})
		pt.ctxCancel()
	}()
	pt.peerTaskManager.PeerTaskDone(pt.taskID, pt.peerID)
	var end = time.Now()
	pt.Log().Errorf("peer task failed, code: %d, reason: %s", pt.failedCode, pt.failedReason)

	// Send EOF piece result to scheduler.
	err := pt.sendPieceResult(&schedulerv1.PieceResult{
		TaskId:        pt.taskID,
		SrcPid:        pt.peerID,
		FinishedCount: pt.readyPieces.Settled(),
		PieceInfo: &commonv1.PieceInfo{
			PieceNum: common.EndOfPiece,
		},
	})
	pt.Debugf("end piece result sent: %v, peer task finished", err)

	err = pt.peerPacketStream.CloseSend()
	pt.Debugf("close stream result: %v", err)

	ctx := trace.ContextWithSpan(context.Background(), trace.SpanFromContext(pt.ctx))
	peerResultCtx, peerResultSpan := tracer.Start(ctx, config.SpanReportPeerResult)
	defer peerResultSpan.End()

	var sourceError *errordetailsv1.SourceError
	if pt.sourceErrorStatus != nil {
		for _, detail := range pt.sourceErrorStatus.Details() {
			switch d := detail.(type) {
			case *errordetailsv1.SourceError:
				sourceError = d
			}
		}
	}
	peerResult := &schedulerv1.PeerResult{
		TaskId:          pt.GetTaskID(),
		PeerId:          pt.GetPeerID(),
		SrcIp:           pt.PeerHost.Ip,
		Idc:             pt.PeerHost.Idc,
		Url:             pt.request.Url,
		ContentLength:   pt.GetContentLength(),
		Traffic:         pt.GetTraffic(),
		TotalPieceCount: pt.GetTotalPieces(),
		Cost:            uint32(end.Sub(pt.startTime).Milliseconds()),
		Success:         false,
		Code:            pt.failedCode,
	}
	if sourceError != nil {
		peerResult.Errordetails = &schedulerv1.PeerResult_SourceError{
			SourceError: sourceError,
		}
	}
	err = pt.schedulerClient.ReportPeerResult(peerResultCtx, peerResult)
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

	if !pt.CalculateDigest {
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

func (pt *peerTaskConductor) sendPieceResult(pr *schedulerv1.PieceResult) error {
	pt.sendPieceResultLock.Lock()
	err := pt.peerPacketStream.Send(pr)
	pt.sendPieceResultLock.Unlock()
	return err
}

func (pt *peerTaskConductor) getFailedError() error {
	if pt.sourceErrorStatus != nil {
		return pt.sourceErrorStatus.Err()
	}
	return fmt.Errorf("peer task failed: %d/%s", pt.failedCode, pt.failedReason)
}
