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
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	dfclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
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

const (
	reasonScheduleTimeout       = "wait first peer packet from scheduler timeout"
	reasonReScheduleTimeout     = "wait more available peers from scheduler timeout"
	reasonContextCanceled       = "context canceled"
	reasonPeerGoneFromScheduler = "scheduler says client should disconnect"
)

type filePeerTask struct {
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
	callback PeerTaskCallback

	// schedule options
	schedulerOption config.SchedulerOption

	// peer task meta info
	peerId          string
	taskId          string
	contentLength   int64
	totalPiece      int32
	completedLength int64
	usedTraffic     int64

	// pieceResultCh is the channel for sending piece result to scheduler
	pieceResultCh chan<- *scheduler.PieceResult
	// peerPacketCh is the channel for receiving available peers from scheduler
	peerPacketCh <-chan *scheduler.PeerPacket
	// peerPacket is the latest available peers from peerPacketCh
	peerPacket *scheduler.PeerPacket
	// peerPacketReady will receive a ready signal for peerPacket ready
	peerPacketReady chan bool
	// pieceParallelCount stands the piece parallel count from peerPacket
	pieceParallelCount int32

	// done channel will be close when peer task is finished
	done chan struct{}
	// same actions must be done only once, like close done channel and so son
	once sync.Once

	// failedPieceCh will hold all pieces which download failed,
	// those pieces will be retry later
	failedPieceCh chan int32
	// failedReason will be set when peer task failed
	failedReason string
	// failedReason will be set when peer task failed
	failedCode base.Code
	// progressCh holds progress status
	progressCh     chan *PeerTaskProgress
	progressStopCh chan bool
	// progressDone will be true when peer task done and caller call ProgressDone func in progress channel
	progressDone bool

	// readyPieces stands all pieces download status
	readyPieces *Bitmap
	// requestedPieces stands all pieces requested from peers
	requestedPieces *Bitmap
	// lock used by piece result manage, when update readyPieces, lock first
	lock sync.Locker
}

func (pt *filePeerTask) SetCallback(callback PeerTaskCallback) {
	pt.callback = callback
}

type PeerTaskProgress struct {
	State           *base.ResponseState
	TaskId          string
	PeerID          string
	ContentLength   int64
	CompletedLength int64
	PeerTaskDone    bool
	ProgressDone    func()
}

func NewFilePeerTask(ctx context.Context,
	host *scheduler.PeerHost,
	pieceManager PieceManager,
	request *scheduler.PeerTaskRequest,
	schedulerClient schedulerclient.SchedulerClient,
	schedulerOption config.SchedulerOption) (FilePeerTask, error) {
	result, err := schedulerClient.RegisterPeerTask(ctx, request)
	if err != nil {
		logger.Errorf("register peer task failed: %s, peer id: %s", err, request.PeerId)
		return nil, err
	}
	if !result.State.Success {
		return nil, fmt.Errorf("regist error: %s/%s", result.State.Code, result.State.Msg)
	}
	schedPieceResultCh, schedPeerPacketCh, err := schedulerClient.ReportPieceResult(ctx, result.TaskId, request)
	if err != nil {
		return nil, err
	}
	logger.Infof("register task success, task id: %s, peer id: %s, SizeScope: %s",
		result.TaskId, request.PeerId, base.SizeScope_name[int32(result.SizeScope)])
	return &filePeerTask{
		host:            host,
		backSource:      result.State.Code == dfcodes.SchedNeedBackSource,
		request:         request,
		pieceResultCh:   schedPieceResultCh,
		peerPacketCh:    schedPeerPacketCh,
		pieceManager:    pieceManager,
		peerPacketReady: make(chan bool),
		peerId:          request.PeerId,
		taskId:          result.TaskId,
		done:            make(chan struct{}),
		once:            sync.Once{},
		readyPieces:     NewBitmap(),
		requestedPieces: NewBitmap(),
		lock:            &sync.Mutex{},
		failedPieceCh:   make(chan int32, 4),
		failedReason:    "unknown",
		failedCode:      dfcodes.UnknownError,
		progressCh:      make(chan *PeerTaskProgress),
		progressStopCh:  make(chan bool),
		contentLength:   -1,
		totalPiece:      -1,
		schedulerOption: schedulerOption,

		SugaredLoggerOnWith: logger.With("peer", request.PeerId, "task", result.TaskId, "component", "filePeerTask"),
	}, nil
}

func (pt *filePeerTask) GetPeerID() string {
	return pt.peerId
}

func (pt *filePeerTask) GetTaskID() string {
	return pt.taskId
}

func (pt *filePeerTask) GetContentLength() int64 {
	return pt.contentLength
}

func (pt *filePeerTask) AddTraffic(n int64) {
	atomic.AddInt64(&pt.usedTraffic, n)
}

func (pt *filePeerTask) GetTraffic() int64 {
	return pt.usedTraffic
}

func (pt *filePeerTask) GetTotalPieces() int32 {
	return pt.totalPiece
}

func (pt *filePeerTask) GetContext() context.Context {
	return pt.ctx
}

func (pt *filePeerTask) Log() *logger.SugaredLoggerOnWith {
	return pt.SugaredLoggerOnWith
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
	go pt.receivePeerPacket()
	go pt.pullPiecesFromPeers(pt, pt.cleanUnfinished)
	// return a progress channel for request download progress
	return pt.progressCh, nil
}

func (pt *filePeerTask) receivePeerPacket() {
	var (
		peerPacket *scheduler.PeerPacket
		ok         bool
	)
loop:
	for {
		select {
		case <-pt.ctx.Done():
			pt.Debugf("context done due to %s", pt.ctx.Err())
			break loop
		case <-pt.done:
			pt.Infof("peer task done, stop wait peer packet from scheduler")
			break loop
		default:
		}

		peerPacket, ok = <-pt.peerPacketCh
		if !ok {
			pt.Debugf("scheduler client close PeerPacket channel")
			break
		}

		if peerPacket == nil {
			pt.Warnf("scheduler client send a empty peerPacket")
			continue
		}
		if peerPacket.State.Code == dfcodes.SchedPeerGone {
			pt.failedReason = reasonPeerGoneFromScheduler
			pt.failedCode = dfcodes.SchedPeerGone
			pt.cancel()
			pt.Errorf(pt.failedReason)
			break
		}

		if !peerPacket.State.Success {
			pt.Errorf("receive peer packet with error: %d/%s", peerPacket.State.Code, peerPacket.State.Msg)
			// TODO when receive error, cancel ?
			// pt.cancel()
			continue
		}

		pt.Debugf("receive peer packet: %#v, main peer: %#v", peerPacket, peerPacket.MainPeer)

		if peerPacket.MainPeer == nil && peerPacket.StealPeers == nil {
			pt.Warnf("scheduler client send a peerPacket with empty peers")
			continue
		}

		pt.peerPacket = peerPacket
		pt.pieceParallelCount = pt.peerPacket.ParallelCount
		select {
		case pt.peerPacketReady <- true:
		case <-pt.ctx.Done():
			pt.Debugf("context done due to %s", pt.ctx.Err())
			break loop
		case <-pt.done:
			pt.Infof("peer task done, stop wait peer packet from scheduler")
			break loop
		default:
		}
	}
	close(pt.peerPacketReady)
}

// TODO when main peer is not available, switch to steel peers
// piece manager need peer task interface, pti make it compatibility for stream peer task
func (pt *filePeerTask) pullPiecesFromPeers(pti PeerTask, cleanUnfinishedFunc func()) {
	defer func() {
		close(pt.failedPieceCh)
		cleanUnfinishedFunc()
	}()
	// wait available peer daemon
	select {
	case <-pt.peerPacketReady:
		// preparePieceTasksByPeer func already send piece result with error
		pt.Infof("new peer client ready")
	case <-time.After(pt.schedulerOption.ScheduleTimeout.Duration):
		pt.failedReason = reasonScheduleTimeout
		pt.failedCode = dfcodes.ClientScheduleTimeout
		pt.Errorf(pt.failedReason)
		return
	}
	var (
		num         int32
		limit       int32
		initialized bool
	)
loop:
	for {
		limit = pt.pieceParallelCount
		// check whether catch exit signal or get a failed piece
		// if nothing got, process normal pieces
		select {
		case <-pt.done:
			pt.Infof("peer task done, stop get pieces from peer")
			break loop
		case <-pt.ctx.Done():
			pt.Debugf("context done due to %s", pt.ctx.Err())
			if !pt.progressDone {
				pt.callback.Fail(pt, pt.ctx.Err().Error())
				pt.failedReason = reasonContextCanceled
				pt.failedCode = dfcodes.ClientContextCanceled
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
				TaskId:   pt.taskId,
				SrcIp:    pt.host.Ip,
				StartNum: num,
				Limit:    limit,
			})

		if err != nil {
			pt.Warnf("get piece task error: %s, wait available peers from scheduler", err)
			select {
			// when peer task without content length or total pieces count, match here
			case <-pt.done:
				pt.Infof("peer task done, stop get pieces from peer")
				break loop
			case <-pt.ctx.Done():
				pt.Debugf("context done due to %s", pt.ctx.Err())
				if !pt.progressDone {
					pt.failedReason = reasonContextCanceled
					pt.failedCode = dfcodes.ClientContextCanceled
				}
				break loop
			case <-pt.peerPacketReady:
				// preparePieceTasksByPeer func already send piece result with error
				pt.Infof("new peer client ready")
			case <-time.After(pt.schedulerOption.ScheduleTimeout.Duration):
				pt.failedReason = reasonReScheduleTimeout
				pt.failedCode = dfcodes.ClientScheduleTimeout
				pt.Errorf(pt.failedReason)
			}
			continue
		}

		if !initialized {
			pt.contentLength = piecePacket.ContentLength
			_ = pt.callback.Init(pt)
			initialized = true
		}

		// trigger DownloadPieces
		if len(piecePacket.PieceInfos) > 0 {
			pt.pieceManager.DownloadPieces(pti, piecePacket)
		}

		// update total piece
		if piecePacket.TotalPiece > pt.totalPiece {
			pt.totalPiece = piecePacket.TotalPiece
			_ = pt.callback.Update(pt)
		}

		for _, p := range piecePacket.PieceInfos {
			pt.Infof("get piece %d from %s", p.PieceNum, piecePacket.DstPid)
			if !pt.requestedPieces.IsSet(p.PieceNum) {
				pt.requestedPieces.Set(p.PieceNum)
			}
		}

		num = pt.getNextPieceNum(num, limit)
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
				if !pt.progressDone {
					pt.failedReason = reasonContextCanceled
					pt.failedCode = dfcodes.ClientContextCanceled
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

func (pt *filePeerTask) ReportPieceResult(piece *base.PieceInfo, pieceResult *scheduler.PieceResult) error {
	// goroutine safe for channel and send on closed channel
	defer func() {
		if r := recover(); r != nil {
			pt.Warnf("recover from %s", r)
		}
	}()
	// retry failed piece
	if !pieceResult.Success {
		pieceResult.FinishedCount = pt.readyPieces.Settled()
		pt.pieceResultCh <- pieceResult
		pt.failedPieceCh <- pieceResult.PieceNum
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
	pt.pieceResultCh <- pieceResult
	// send progress first to avoid close channel panic
	p := &PeerTaskProgress{
		State: &base.ResponseState{
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

func (pt *filePeerTask) isCompleted() bool {
	return pt.completedLength == pt.contentLength
}

func (pt *filePeerTask) preparePieceTasks(request *base.PieceTaskRequest) (p *base.PiecePacket, err error) {
	defer func() {
		if rerr := recover(); rerr != nil {
			pt.Errorf("preparePieceTasks recover from: %s", rerr)
			err = fmt.Errorf("%v", rerr)
		}
	}()
	pt.pieceParallelCount = pt.peerPacket.ParallelCount
	request.DstPid = pt.peerPacket.MainPeer.PeerId
	p, err = pt.preparePieceTasksByPeer(pt.peerPacket.MainPeer, request)
	if err == nil {
		return
	}
	for _, peer := range pt.peerPacket.StealPeers {
		request.DstPid = peer.PeerId
		p, err = pt.preparePieceTasksByPeer(peer, request)
		if err == nil {
			return
		}
	}
	err = fmt.Errorf("no peers available")
	return
}

func (pt *filePeerTask) preparePieceTasksByPeer(peer *scheduler.PeerPacket_DestPeer, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	if peer == nil {
		return nil, fmt.Errorf("empty peer")
	}
	pt.Debugf("get piece task from peer %s, request: %#v", peer.PeerId, request)
	p, err := pt.getPieceTasks(peer, request)
	if err == nil {
		pt.Infof("get piece task from peer %s ok, pieces packet: %#v, length: %d", peer.PeerId, p, len(p.PieceInfos))
		return p, nil
	}

	// context canceled, just exit
	if status.Code(err) == codes.Canceled {
		pt.Warnf("get piece task from peer(%s) canceled: %s", peer.PeerId, err)
		return nil, err
	}
	code := dfcodes.ClientPieceTaskRequestFail
	// not grpc error
	if de, ok := err.(*dferrors.DfError); ok && uint32(de.Code) > uint32(codes.Unauthenticated) {
		code = de.Code
	}
	// may be panic here due to unknown content length or total piece count
	// recover by preparePieceTasks
	pt.pieceResultCh <- &scheduler.PieceResult{
		TaskId:        pt.taskId,
		SrcPid:        pt.peerId,
		DstPid:        peer.PeerId,
		Success:       false,
		Code:          code,
		HostLoad:      nil,
		FinishedCount: -1,
	}
	pt.Errorf("get piece task from peer(%s) error: %s, code: %d", peer.PeerId, err, code)
	return nil, err
}

func (pt *filePeerTask) getPieceTasks(peer *scheduler.PeerPacket_DestPeer, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	p, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		pp, getErr := dfclient.GetPieceTasks(peer, pt.ctx, request)
		if getErr != nil {
			return nil, getErr
		}
		// by santong: when peer return empty, retry later
		if len(pp.PieceInfos) == 0 {
			pt.pieceResultCh <- &scheduler.PieceResult{
				TaskId:        pt.taskId,
				SrcPid:        pt.peerId,
				DstPid:        peer.PeerId,
				Success:       false,
				Code:          dfcodes.ClientWaitPieceReady,
				HostLoad:      nil,
				FinishedCount: pt.readyPieces.Settled(),
			}
			pt.Warnf("peer %s returns success but with empty pieces, retry later", peer.PeerId)
			return nil, dferrors.ErrEmptyValue
		}
		return pp, nil
	}, 1, 8, 8, nil)
	if err == nil {
		return p.(*base.PiecePacket), nil
	}
	return nil, err
}

func (pt *filePeerTask) getNextPieceNum(cur, limit int32) int32 {
	if pt.isCompleted() {
		return -1
	}
	i := cur + limit
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

func (pt *filePeerTask) finish() error {
	var err error
	// send last progress
	pt.once.Do(func() {
		defer func() {
			if rerr := recover(); rerr != nil {
				pt.Errorf("finish recover from: %s", rerr)
				err = fmt.Errorf("%v", rerr)
			}
		}()
		// send EOF piece result to scheduler
		pt.pieceResultCh <- scheduler.NewEndPieceResult(pt.taskId, pt.peerId, pt.readyPieces.Settled())
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
			code = dfcodes.UnknownError
			message = err.Error()
		}

		pg := &PeerTaskProgress{
			State: &base.ResponseState{
				Success: success,
				Code:    code,
				Msg:     message,
			},
			TaskId:          pt.taskId,
			PeerID:          pt.peerId,
			ContentLength:   pt.contentLength,
			CompletedLength: pt.completedLength,
			PeerTaskDone:    true,
			ProgressDone: func() {
				pt.progressDone = true
				close(pt.progressStopCh)
			},
		}

		// wait client received progress
		pt.Infof("try to send finish progress: %#v, state: %#v", pg, pg.State)
		select {
		case pt.progressCh <- pg:
			pt.Infof("finish progress sent")
		case <-pt.ctx.Done():
			pt.Warnf("finish progress sent failed: %#v, context done", pg)
		}
		// wait progress stopped
		select {
		case <-pt.progressStopCh:
			pt.Infof("progress stopped")
		case <-pt.ctx.Done():
			if pt.progressDone {
				pt.Debugf("progress stopped and context done")
			} else {
				pt.Warnf("wait progress stopped failed: %#v, context done, but progress not stopped", pg)
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
		}()
		// send EOF piece result to scheduler
		pt.pieceResultCh <- scheduler.NewEndPieceResult(pt.taskId, pt.peerId, pt.readyPieces.Settled())
		pt.Debugf("clean up end piece result sent")

		pg := &PeerTaskProgress{
			State: &base.ResponseState{
				Success: false,
				Code:    pt.failedCode,
				Msg:     pt.failedReason,
			},
			TaskId:          pt.taskId,
			PeerID:          pt.peerId,
			ContentLength:   pt.contentLength,
			CompletedLength: pt.completedLength,
			PeerTaskDone:    true,
			ProgressDone: func() {
				pt.progressDone = true
				close(pt.progressStopCh)
			},
		}

		// wait client received progress
		select {
		case pt.progressCh <- pg:
			pt.Debugf("unfinished progress sent: %#v, state: %#v", pg, pg.State)
		case <-pt.ctx.Done():
			pt.Debugf("send unfinished progress failed: %#v, context done: %v", pg, pt.ctx.Err())
		}
		// wait progress stopped
		select {
		case <-pt.progressStopCh:
			pt.Infof("progress stopped")
		case <-pt.ctx.Done():
			if pt.progressDone {
				pt.Debugf("progress stopped and context done")
			} else {
				pt.Warnf("wait progress stopped failed: %#v, context done, but progress not stopped", pg)
			}
		}

		if err := pt.callback.Fail(pt, pt.failedReason); err != nil {
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
