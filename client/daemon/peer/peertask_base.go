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
	"runtime/debug"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/rpc/base"
	dfclient "d7y.io/dragonfly/v2/internal/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/internal/rpc/scheduler/client"
)

const (
	reasonScheduleTimeout       = "wait first peer packet from scheduler timeout"
	reasonReScheduleTimeout     = "wait more available peers from scheduler timeout"
	reasonContextCanceled       = "context canceled"
	reasonPeerGoneFromScheduler = "scheduler says client should disconnect"

	failedCodeNotSet = 0
)

var errPeerPacketChanged = errors.New("peer packet changed")

type peerTask struct {
	*logger.SugaredLoggerOnWith
	ctx    context.Context
	cancel context.CancelFunc

	// backSource indicates downloading resource from instead of other peers
	backSource bool
	request    *scheduler.PeerTaskRequest

	// pieceManager will be used for downloading piece
	pieceManager PieceManager
	// host info about current host
	host *scheduler.PeerHost
	// callback holds some actions, like init, done, fail actions
	callback TaskCallback

	// schedule options
	schedulerOption config.SchedulerOption

	// peer task meta info
	peerID          string
	taskID          string
	contentLength   int64
	totalPiece      int32
	completedLength *atomic.Int64
	usedTraffic     *atomic.Int64

	//sizeScope   base.SizeScope
	singlePiece *scheduler.SinglePiece

	// TODO peerPacketStream
	peerPacketStream schedulerclient.PeerPacketStream
	// peerPacket is the latest available peers from peerPacketCh
	peerPacket *scheduler.PeerPacket
	// peerPacketReady will receive a ready signal for peerPacket ready
	peerPacketReady chan bool
	// pieceParallelCount stands the piece parallel count from peerPacket
	pieceParallelCount int32

	// done channel will be close when peer task is finished
	done chan struct{}
	// peerTaskDone will be true after peer task done
	peerTaskDone bool
	// span stands open telemetry trace span
	span trace.Span

	// same actions must be done only once, like close done channel and so on
	once sync.Once

	// failedPieceCh will hold all pieces which download failed,
	// those pieces will be retry later
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
	lock sync.Mutex
	// limiter will be used when enable per peer task rate limit
	limiter *rate.Limiter
}

var _ Task = (*peerTask)(nil)

func (pt *peerTask) ReportPieceResult(pieceTask *base.PieceInfo, pieceResult *scheduler.PieceResult) error {
	panic("implement me")
}

func (pt *peerTask) SetCallback(callback TaskCallback) {
	pt.callback = callback
}

func (pt *peerTask) GetPeerID() string {
	return pt.peerID
}

func (pt *peerTask) GetTaskID() string {
	return pt.taskID
}

func (pt *peerTask) GetContentLength() int64 {
	return pt.contentLength
}

func (pt *peerTask) SetContentLength(i int64) error {
	panic("implement me")
}

func (pt *peerTask) AddTraffic(n int64) {
	pt.usedTraffic.Add(n)
}

func (pt *peerTask) GetTraffic() int64 {
	return pt.usedTraffic.Load()
}

func (pt *peerTask) GetTotalPieces() int32 {
	return pt.totalPiece
}

func (pt *peerTask) Context() context.Context {
	return pt.ctx
}

func (pt *peerTask) Log() *logger.SugaredLoggerOnWith {
	return pt.SugaredLoggerOnWith
}

func (pt *peerTask) pullPieces(pti Task, cleanUnfinishedFunc func()) {
	// when there is a single piece, try to download first
	if pt.singlePiece != nil {
		go pt.pullSinglePiece(pti, cleanUnfinishedFunc)
	} else {
		go pt.receivePeerPacket()
		go pt.pullPiecesFromPeers(pti, cleanUnfinishedFunc)
	}
}

func (pt *peerTask) receivePeerPacket() {
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
	}()
loop:
	for {
		select {
		case <-pt.ctx.Done():
			pt.Infof("context done due to %s", pt.ctx.Err())
			break loop
		case <-pt.done:
			pt.Infof("peer task done, stop wait peer packet from scheduler")
			break loop
		default:
		}

		peerPacket, err = pt.peerPacketStream.Recv()
		if err == io.EOF {
			pt.Debugf("peerPacketStream closed")
			break loop
		}
		if err != nil {
			pt.failedCode = dfcodes.UnknownError
			if de, ok := err.(*dferrors.DfError); ok {
				pt.failedCode = de.Code
				pt.failedReason = de.Message
				pt.Errorf("receive peer packet failed: %s", pt.failedReason)
			} else {
				pt.Errorf("receive peer packet failed: %s", err)
			}
			pt.cancel()
			if !firstSpanDone {
				firstPeerSpan.RecordError(err)
			}
			break loop
		}

		if peerPacket.Code != dfcodes.Success {
			pt.Errorf("receive peer packet with error: %d", peerPacket.Code)
			if pt.isExitPeerPacketCode(peerPacket) {
				pt.cancel()
				pt.Errorf(pt.failedReason)
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

		pt.peerPacket = peerPacket
		pt.pieceParallelCount = pt.peerPacket.ParallelCount
		select {
		case pt.peerPacketReady <- true:
		case <-pt.ctx.Done():
			pt.Infof("context done due to %s", pt.ctx.Err())
			break loop
		case <-pt.done:
			pt.Infof("peer task done, stop wait peer packet from scheduler")
			break loop
		default:
		}
	}
}

func (pt *peerTask) isExitPeerPacketCode(pp *scheduler.PeerPacket) bool {
	switch pp.Code {
	case dfcodes.ResourceLacked, dfcodes.BadRequest, dfcodes.PeerTaskNotFound, dfcodes.UnknownError, dfcodes.RequestTimeOut:
		// 1xxx
		pt.failedCode = pp.Code
		pt.failedReason = fmt.Sprintf("receive exit peer packet with code %d", pp.Code)
		return true
	case dfcodes.SchedError:
		// 5xxx
		pt.failedCode = pp.Code
		pt.failedReason = fmt.Sprintf("receive exit peer packet with code %d", pp.Code)
		return true
	case dfcodes.SchedPeerGone:
		pt.failedReason = reasonPeerGoneFromScheduler
		pt.failedCode = dfcodes.SchedPeerGone
		return true
	case dfcodes.CdnError, dfcodes.CdnTaskRegistryFail, dfcodes.CdnTaskDownloadFail:
		// 6xxx
		pt.failedCode = pp.Code
		pt.failedReason = fmt.Sprintf("receive exit peer packet with code %d", pp.Code)
		return true
	}
	return false
}

func (pt *peerTask) pullSinglePiece(pti Task, cleanUnfinishedFunc func()) {
	pt.Infof("single piece, dest peer id: %s, piece num: %d, size: %d",
		pt.singlePiece.DstPid, pt.singlePiece.PieceInfo.PieceNum, pt.singlePiece.PieceInfo.RangeSize)

	ctx, span := tracer.Start(pt.ctx, fmt.Sprintf(config.SpanDownloadPiece, pt.singlePiece.PieceInfo.PieceNum))
	span.SetAttributes(config.AttributePiece.Int(int(pt.singlePiece.PieceInfo.PieceNum)))

	pt.contentLength = int64(pt.singlePiece.PieceInfo.RangeSize)
	if err := pt.callback.Init(pti); err != nil {
		pt.failedReason = err.Error()
		pt.failedCode = dfcodes.ClientError
		cleanUnfinishedFunc()
		span.RecordError(err)
		span.SetAttributes(config.AttributePieceSuccess.Bool(false))
		span.End()
		return
	}

	request := &DownloadPieceRequest{
		TaskID:  pt.GetTaskID(),
		DstPid:  pt.singlePiece.DstPid,
		DstAddr: pt.singlePiece.DstAddr,
		piece:   pt.singlePiece.PieceInfo,
	}
	if pt.pieceManager.DownloadPiece(ctx, pti, request) {
		pt.Infof("single piece download success")
		span.SetAttributes(config.AttributePieceSuccess.Bool(true))
		span.End()
	} else {
		// fallback to download from other peers
		span.SetAttributes(config.AttributePieceSuccess.Bool(false))
		span.End()
		pt.Warnf("single piece download failed, switch to download from other peers")
		go pt.receivePeerPacket()
		pt.pullPiecesFromPeers(pti, cleanUnfinishedFunc)
	}
}

// TODO when main peer is not available, switch to steel peers
// piece manager need peer task interface, pti make it compatibility for stream peer task
func (pt *peerTask) pullPiecesFromPeers(pti Task, cleanUnfinishedFunc func()) {
	defer func() {
		close(pt.failedPieceCh)
		cleanUnfinishedFunc()
	}()
	// wait first available peer
	select {
	case <-pt.peerPacketReady:
		// preparePieceTasksByPeer func already send piece result with error
		pt.Infof("new peer client ready, scheduler time cost: %dus, main peer: %s",
			time.Now().Sub(pt.callback.GetStartTime()).Microseconds(), pt.peerPacket.MainPeer)
	case <-time.After(pt.schedulerOption.ScheduleTimeout.Duration):
		pt.failedReason = reasonScheduleTimeout
		pt.failedCode = dfcodes.ClientScheduleTimeout
		pt.Errorf(pt.failedReason)
		return
	}
	var (
		num             int32
		limit           int32
		initialized     bool
		pieceRequestCh  chan *DownloadPieceRequest
		pieceBufferSize = int32(16)
	)
loop:
	for {
		limit = pieceBufferSize
		// check whether catch exit signal or get a failed piece
		// if nothing got, process normal pieces
		select {
		case <-pt.done:
			pt.Infof("peer task done, stop get pieces from peer")
			break loop
		case <-pt.ctx.Done():
			pt.Debugf("context done due to %s", pt.ctx.Err())
			if !pt.peerTaskDone {
				if pt.failedCode == failedCodeNotSet {
					pt.failedReason = reasonContextCanceled
					pt.failedCode = dfcodes.ClientContextCanceled
					pt.callback.Fail(pt, pt.failedCode, pt.ctx.Err().Error())
				} else {
					pt.callback.Fail(pt, pt.failedCode, pt.failedReason)
				}
			}
			break loop
		case failed := <-pt.failedPieceCh:
			pt.Warnf("download piece/%d failed, retry", failed)
			num = failed
			limit = 1
		default:
		}

		pt.Debugf("try to get pieces, number: %d, limit: %d", num, limit)
		piecePacket, err := pt.preparePieceTasks(
			&base.PieceTaskRequest{
				TaskId:   pt.taskID,
				SrcPid:   pt.peerID,
				StartNum: num,
				Limit:    limit,
			})

		if err != nil {
			pt.Warnf("get piece task error: %s, wait available peers from scheduler", err)
			pt.span.RecordError(err)
			select {
			// when peer task without content length or total pieces count, match here
			case <-pt.done:
				pt.Infof("peer task done, stop get pieces from peer")
			case <-pt.ctx.Done():
				pt.Debugf("context done due to %s", pt.ctx.Err())
				if !pt.peerTaskDone {
					if pt.failedCode == failedCodeNotSet {
						pt.failedReason = reasonContextCanceled
						pt.failedCode = dfcodes.ClientContextCanceled
					}
				}
			case <-pt.peerPacketReady:
				// preparePieceTasksByPeer func already send piece result with error
				pt.Infof("new peer client ready, main peer: %s", pt.peerPacket.MainPeer)
				// research from piece 0
				num = pt.getNextPieceNum(0)
				continue loop
			case <-time.After(pt.schedulerOption.ScheduleTimeout.Duration):
				pt.failedReason = reasonReScheduleTimeout
				pt.failedCode = dfcodes.ClientScheduleTimeout
				pt.Errorf(pt.failedReason)
			}
			// only <-pt.peerPacketReady continue loop, others break
			break loop
		}

		if !initialized {
			pt.contentLength = piecePacket.ContentLength
			if pt.contentLength > 0 {
				pt.span.SetAttributes(config.AttributeTaskContentLength.Int64(pt.contentLength))
			}
			initialized = true
			if err = pt.callback.Init(pt); err != nil {
				pt.span.RecordError(err)
				pt.failedReason = err.Error()
				pt.failedCode = dfcodes.ClientError
				break loop
			}
			pc := pt.peerPacket.ParallelCount
			pieceRequestCh = make(chan *DownloadPieceRequest, pieceBufferSize)
			for i := int32(0); i < pc; i++ {
				go pt.downloadPieceWorker(i, pti, pieceRequestCh)
			}
		}

		// update total piece
		if piecePacket.TotalPiece > pt.totalPiece {
			pt.totalPiece = piecePacket.TotalPiece
			_ = pt.callback.Update(pt)
			pt.Debugf("update total piece count: %d", pt.totalPiece)
		}

		// trigger DownloadPiece
		for _, piece := range piecePacket.PieceInfos {
			pt.Infof("get piece %d from %s/%s", piece.PieceNum, piecePacket.DstAddr, piecePacket.DstPid)
			if !pt.requestedPieces.IsSet(piece.PieceNum) {
				pt.requestedPieces.Set(piece.PieceNum)
			}
			req := &DownloadPieceRequest{
				TaskID:  pt.GetTaskID(),
				DstPid:  piecePacket.DstPid,
				DstAddr: piecePacket.DstAddr,
				piece:   piece,
			}
			select {
			case pieceRequestCh <- req:
			case <-pt.done:
				pt.Warnf("peer task done, but still some piece request not process")
			case <-pt.ctx.Done():
				pt.Warnf("context done due to %s", pt.ctx.Err())
				if !pt.peerTaskDone {
					if pt.failedCode == failedCodeNotSet {
						pt.failedReason = reasonContextCanceled
						pt.failedCode = dfcodes.ClientContextCanceled
					}
				}
			}
		}

		num = pt.getNextPieceNum(num)
		if num == -1 {
			pt.Infof("all pieces requests send, just wait failed pieces")
			if pt.isCompleted() {
				break loop
			}
			// use no default branch select to wait failed piece or exit
			select {
			case <-pt.done:
				pt.Infof("peer task done, stop get pieces from peer")
				break loop
			case <-pt.ctx.Done():
				if !pt.peerTaskDone {
					if pt.failedCode == failedCodeNotSet {
						pt.failedReason = reasonContextCanceled
						pt.failedCode = dfcodes.ClientContextCanceled
					}
					pt.Errorf("context done due to %s, progress is not done", pt.ctx.Err())
				} else {
					pt.Debugf("context done due to %s, progress is already done", pt.ctx.Err())
				}
				break loop
			case failed := <-pt.failedPieceCh:
				pt.Warnf("download piece/%d failed, retry", failed)
				num = failed
				limit = 1
			}
		}
	}
}

func (pt *peerTask) downloadPieceWorker(id int32, pti Task, requests chan *DownloadPieceRequest) {
	for {
		select {
		case request := <-requests:
			ctx, span := tracer.Start(pt.ctx, fmt.Sprintf(config.SpanDownloadPiece, request.piece.PieceNum))
			span.SetAttributes(config.AttributePiece.Int(int(request.piece.PieceNum)))
			span.SetAttributes(config.AttributePieceWorker.Int(int(id)))
			if pt.limiter != nil {
				_, waitSpan := tracer.Start(ctx, config.SpanWaitPieceLimit)
				if err := pt.limiter.WaitN(pt.ctx, int(request.piece.RangeSize)); err != nil {
					pt.Errorf("request limiter error: %s", err)
					waitSpan.RecordError(err)
					waitSpan.End()
					pti.ReportPieceResult(request.piece,
						&scheduler.PieceResult{
							TaskId:        pt.GetTaskID(),
							SrcPid:        pt.GetPeerID(),
							DstPid:        request.DstPid,
							PieceNum:      request.piece.PieceNum,
							Success:       false,
							Code:          dfcodes.ClientRequestLimitFail,
							HostLoad:      nil,
							FinishedCount: 0, // update by peer task
						})
					pt.failedReason = err.Error()
					pt.failedCode = dfcodes.ClientRequestLimitFail
					pt.cancel()
					span.SetAttributes(config.AttributePieceSuccess.Bool(false))
					span.End()
					return
				}
				waitSpan.End()
			}
			pt.Debugf("peer download worker #%d receive piece task, "+
				"dest peer id: %s, piece num: %d, range start: %d, range size: %d",
				id, request.DstPid, request.piece.PieceNum, request.piece.RangeStart, request.piece.RangeSize)
			success := pt.pieceManager.DownloadPiece(ctx, pti, request)

			span.SetAttributes(config.AttributePieceSuccess.Bool(success))
			span.End()
		case <-pt.done:
			pt.Debugf("peer task done, peer download worker #%d exit", id)
			return
		case <-pt.ctx.Done():
			pt.Debugf("peer task context done, peer download worker #%d exit", id)
			return
		}
	}
}

func (pt *peerTask) isCompleted() bool {
	return pt.completedLength.Load() == pt.contentLength
}

func (pt *peerTask) preparePieceTasks(request *base.PieceTaskRequest) (p *base.PiecePacket, err error) {
	defer pt.recoverFromPanic()
prepare:
	pt.pieceParallelCount = pt.peerPacket.ParallelCount
	request.DstPid = pt.peerPacket.MainPeer.PeerId
	p, err = pt.preparePieceTasksByPeer(pt.peerPacket, pt.peerPacket.MainPeer, request)
	if err == nil {
		return
	}
	if err == errPeerPacketChanged {
		goto prepare
	}
	for _, peer := range pt.peerPacket.StealPeers {
		request.DstPid = peer.PeerId
		p, err = pt.preparePieceTasksByPeer(pt.peerPacket, peer, request)
		if err == nil {
			return
		}
		if err == errPeerPacketChanged {
			goto prepare
		}
	}
	return
}

func (pt *peerTask) preparePieceTasksByPeer(curPeerPacket *scheduler.PeerPacket, peer *scheduler.PeerPacket_DestPeer, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	if peer == nil {
		return nil, fmt.Errorf("empty peer")
	}
	var span trace.Span
	_, span = tracer.Start(pt.ctx, config.SpanGetPieceTasks)
	span.SetAttributes(config.AttributeTargetPeerID.String(peer.PeerId))
	span.SetAttributes(config.AttributeGetPieceStartNum.Int(int(request.StartNum)))
	span.SetAttributes(config.AttributeGetPieceLimit.Int(int(request.Limit)))
	defer span.End()

	// when cdn returns dfcodes.CdnTaskNotFound, report it to scheduler and wait cdn download it.
retry:
	pt.Debugf("get piece task from peer %s, piece num: %d, limit: %d\"", peer.PeerId, request.StartNum, request.Limit)
	p, err := pt.getPieceTasks(span, curPeerPacket, peer, request)
	if err == nil {
		pt.Infof("get piece task from peer %s ok, pieces length: %d", peer.PeerId, len(p.PieceInfos))
		span.SetAttributes(config.AttributeGetPieceCount.Int(len(p.PieceInfos)))
		return p, nil
	}
	span.RecordError(err)
	if err == errPeerPacketChanged {
		return nil, err
	}
	pt.Debugf("get piece task error: %#v", err)

	// grpc error
	if se, ok := err.(interface{ GRPCStatus() *status.Status }); ok {
		pt.Debugf("get piece task with grpc error, code: %d", se.GRPCStatus().Code())
		// context canceled, just exit
		if se.GRPCStatus().Code() == codes.Canceled {
			span.AddEvent("context canceled")
			pt.Warnf("get piece task from peer(%s) canceled: %s", peer.PeerId, err)
			return nil, err
		}
	}
	code := dfcodes.ClientPieceRequestFail
	// not grpc error
	if de, ok := err.(*dferrors.DfError); ok && uint32(de.Code) > uint32(codes.Unauthenticated) {
		pt.Debugf("get piece task from peer %s with df error, code: %d", peer.PeerId, de.Code)
		code = de.Code
	}
	pt.Errorf("get piece task from peer(%s) error: %s, code: %d", peer.PeerId, err, code)
	perr := pt.peerPacketStream.Send(&scheduler.PieceResult{
		TaskId:        pt.taskID,
		SrcPid:        pt.peerID,
		DstPid:        peer.PeerId,
		Success:       false,
		Code:          code,
		HostLoad:      nil,
		FinishedCount: -1,
	})
	if perr != nil {
		span.RecordError(perr)
		pt.Errorf("send piece result error: %s, code: %d", err, code)
	}

	if code == dfcodes.CdnTaskNotFound && curPeerPacket == pt.peerPacket {
		span.AddEvent("retry for CdnTaskNotFound")
		goto retry
	}
	return nil, err
}

func (pt *peerTask) getPieceTasks(span trace.Span, curPeerPacket *scheduler.PeerPacket, peer *scheduler.PeerPacket_DestPeer, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	var (
		peerPacketChanged bool
		count             int
	)
	p, _, err := clientutil.Retry(pt.ctx, func() (interface{}, bool, error) {
		pp, getErr := dfclient.GetPieceTasks(pt.ctx, peer, request)
		// when GetPieceTasks returns err, exit retry
		if getErr != nil {
			span.RecordError(getErr)
			// fast way to exit retry
			if curPeerPacket != pt.peerPacket {
				pt.Warnf("get piece tasks with error: %s, but peer packet changed, switch to new peer packet", getErr)
				peerPacketChanged = true
				return nil, true, nil
			}
			return nil, true, getErr
		}
		// by santong: when peer return empty, retry later
		if len(pp.PieceInfos) == 0 {
			count++
			er := pt.peerPacketStream.Send(&scheduler.PieceResult{
				TaskId:        pt.taskID,
				SrcPid:        pt.peerID,
				DstPid:        peer.PeerId,
				Success:       false,
				Code:          dfcodes.ClientWaitPieceReady,
				HostLoad:      nil,
				FinishedCount: pt.readyPieces.Settled(),
			})
			if er != nil {
				span.RecordError(er)
				pt.Errorf("send piece result error: %s, code: %d", peer.PeerId, er)
			}
			// fast way to exit retry
			if curPeerPacket != pt.peerPacket {
				pt.Warnf("get empty pieces and peer packet changed, switch to new peer packet")
				peerPacketChanged = true
				return nil, true, nil
			}
			span.AddEvent("retry due to empty pieces",
				trace.WithAttributes(config.AttributeGetPieceRetry.Int(count)))
			pt.Warnf("peer %s returns success but with empty pieces, retry later", peer.PeerId)
			return nil, false, dferrors.ErrEmptyValue
		}
		return pp, false, nil
	}, 0.05, 0.2, 40, nil)
	if peerPacketChanged {
		return nil, errPeerPacketChanged
	}

	if err == nil {
		return p.(*base.PiecePacket), nil
	}
	return nil, err
}

func (pt *peerTask) getNextPieceNum(cur int32) int32 {
	if pt.isCompleted() {
		return -1
	}
	i := cur
	for ; pt.requestedPieces.IsSet(i); i++ {
	}
	if pt.totalPiece > 0 && i >= pt.totalPiece {
		// double check, re-search not success or not requested pieces
		for i = int32(0); pt.requestedPieces.IsSet(i); i++ {
		}
		if pt.totalPiece > 0 && i >= pt.totalPiece {
			return -1
		}
	}
	return i
}

func (pt *peerTask) recoverFromPanic() {
	if r := recover(); r != nil {
		pt.Errorf("recovered from panic %q. Call stack:\n%v", r, string(debug.Stack()))
	}
}
