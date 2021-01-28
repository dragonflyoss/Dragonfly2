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

	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon"
	dfclient "github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon/client"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	schedulerclient "github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler/client"
)

// FilePeerTask represents a peer task to download a file
type FilePeerTask interface {
	PeerTask
	// Start start the special peer task, return a *PeerTaskProgress channel for updating download progress
	Start(ctx context.Context) (chan *PeerTaskProgress, error)
}

type filePeerTask struct {
	ctx          context.Context
	host         *scheduler.PeerHost
	pieceManager PieceManager

	schedPieceResultCh chan<- *scheduler.PieceResult
	schedPeerPacketCh  <-chan *scheduler.PeerPacket

	// peerPacket in the fly
	peerPacket *scheduler.PeerPacket

	peerClient      dfdaemon.DaemonClient
	peerClientReady chan bool

	pieceParallelCount int32

	done     chan struct{}
	doneOnce sync.Once

	peerId          string
	taskId          string
	contentLength   int64
	completedLength int64

	failedPieceCh chan int32
	progressCh    chan *PeerTaskProgress
	callback      PeerTaskCallback
	log           *logger.SugaredLoggerOnWith

	// used buy piece result manage
	lock   sync.Locker
	bitmap *Bitmap
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
	Done            bool
}

func NewFilePeerTask(ctx context.Context,
	host *scheduler.PeerHost,
	schedulerClient schedulerclient.SchedulerClient,
	pieceManager PieceManager,
	request *scheduler.PeerTaskRequest) (FilePeerTask, error) {
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
		ctx:                ctx,
		host:               host,
		schedPieceResultCh: schedPieceResultCh,
		schedPeerPacketCh:  schedPeerPacketCh,
		pieceManager:       pieceManager,
		peerClientReady:    make(chan bool),
		peerId:             request.PeerId,
		taskId:             result.TaskId,
		done:               make(chan struct{}),
		doneOnce:           sync.Once{},
		bitmap:             NewBitmap(),
		lock:               &sync.Mutex{},
		log:                logger.With("peer", request.PeerId, "task", result.TaskId, "component", "filePeerTask"),
		failedPieceCh:      make(chan int32, 4),
		progressCh:         make(chan *PeerTaskProgress),
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

func (pt *filePeerTask) Start(ctx context.Context) (chan *PeerTaskProgress, error) {
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
WaitScheduler:
	for {
		select {
		case <-pt.ctx.Done():
			pt.log.Debugf("context done due to %s", pt.ctx.Err())
			break WaitScheduler
		case <-pt.done:
			pt.log.Infof("peer task done, stop wait peer packet from scheduler")
			break WaitScheduler
		default:
		}

		peerPacket, ok = <-pt.schedPeerPacketCh
		if !ok {
			pt.log.Debugf("scheduler client close PeerPacket channel")
			break
		}
		if peerPacket == nil {
			pt.log.Warnf("scheduler client send nil PeerPacket")
			continue
		}
		pt.log.Debugf("receive peer packet: %#v", peerPacket)
		if peerPacket.MainPeer == nil && peerPacket.StealPeers == nil {
			pt.log.Warnf("scheduler client send a PeerPacket will empty peers")
			continue
		}
		pt.peerPacket = peerPacket
		pt.pieceParallelCount = pt.peerPacket.ParallelCount

		select {
		case pt.peerClientReady <- true:
		case <-pt.ctx.Done():
			pt.log.Debugf("context done due to %s", pt.ctx.Err())
			break WaitScheduler
		case <-pt.done:
			pt.log.Infof("peer task done, stop wait peer packet from scheduler")
			break WaitScheduler
		default:
		}
	}
	close(pt.peerClientReady)
}

// TODO when main peer is not available, switch to steel peers
// piece manager need peer task interface, pti make it compatibility for stream peer task
func (pt *filePeerTask) pullPiecesFromPeers(pti PeerTask, cleanFunc func()) {
	// wait available peer daemon
	<-pt.peerClientReady
	var (
		num         int32
		limit       int32
		initialized bool
	)
getPiecesTasks:
	for {
		limit = pt.pieceParallelCount
		select {
		case <-pt.done:
			pt.log.Infof("peer task done, stop get pieces from peer")
			break getPiecesTasks
		case failed := <-pt.failedPieceCh:
			pt.log.Warnf("download piece/%d failed, retry", failed)
			num = failed
			limit = 1
		case <-pt.ctx.Done():
			pt.log.Debugf("context done due to %s", pt.ctx.Err())
			pt.callback.Fail(pt, pt.ctx.Err().Error())
			break getPiecesTasks
		default:
		}

		piecePacket, err := pt.getPieceTasks(
			&base.PieceTaskRequest{
				TaskId:   pt.taskId,
				SrcIp:    pt.host.Ip,
				StartNum: num,
				Limit:    limit,
			})
		if err != nil || !piecePacket.State.Success {
			// FIXME(jim): push failed result to scheduler
			pt.log.Warnf("get piece task error: %s, wait available peers from scheduler", err)
			select {
			case <-pt.ctx.Done():
				pt.log.Debugf("context done due to %s", pt.ctx.Err())
				pt.callback.Fail(pt, pt.ctx.Err().Error())
				break getPiecesTasks
			case <-pt.peerClientReady:
				pt.log.Infof("new peer client ready")
			}

			continue
		}

		if !initialized {
			pt.contentLength = piecePacket.ContentLength
			_ = pt.callback.Init(pt)
			initialized = true
		}

		num = pt.getNextPieceNum(num, limit)
		if num == -1 {
			pt.log.Infof("peer task done, stop get pieces from peer")
			break getPiecesTasks
		}
		pt.pieceManager.PullPieces(pti, piecePacket)
	}
	close(pt.failedPieceCh)

	cleanFunc()
}

func (pt *filePeerTask) ReportPieceResult(piece *base.PieceInfo, pieceResult *scheduler.PieceResult) error {
	// FIXME goroutine safe for channel and send on closed channel
	defer func() {
		if r := recover(); r != nil {
			logger.Warnf("recover from %s", r)
		}
	}()
	// retry failed piece
	if !pieceResult.Success {
		pt.schedPieceResultCh <- pieceResult
		pt.failedPieceCh <- pieceResult.PieceNum
		return nil
	}
	pieceResult.FinishedCount = pt.bitmap.Settled()
	pt.schedPieceResultCh <- pieceResult
	// send progress first to avoid close channel panic
	p := &PeerTaskProgress{
		State: &base.ResponseState{
			Success: pieceResult.Success,
			Code:    pieceResult.Code,
			Msg:     "",
		},
		TaskId:          pt.taskId,
		PeerID:          pt.peerId,
		ContentLength:   pt.contentLength,
		CompletedLength: pt.completedLength + int64(piece.RangeSize),
		Done:            false,
	}
	select {
	case pt.progressCh <- p:
		pt.log.Debugf("progress sent, %d/%d", p.CompletedLength, p.ContentLength)
	case <-pt.ctx.Done():
		pt.log.Warnf("peer task context done due to %s", pt.ctx.Err())
		return pt.ctx.Err()
	}

	pt.lock.Lock()
	defer pt.lock.Unlock()
	if pt.bitmap.IsSet(pieceResult.PieceNum) {
		pt.log.Warnf("piece %d is already reported, skipped", pieceResult.PieceNum)
		return nil
	}
	// mark piece processed
	pt.bitmap.Set(pieceResult.PieceNum)
	atomic.AddInt64(&pt.completedLength, int64(piece.RangeSize))

	if !pt.isCompleted() {
		return nil
	}

	return pt.finish()
}

func (pt *filePeerTask) isCompleted() bool {
	return pt.completedLength == pt.contentLength
}

func (pt *filePeerTask) getPieceTasks(request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	pt.pieceParallelCount = pt.peerPacket.ParallelCount
	if pt.peerPacket.MainPeer != nil {
		p, err := dfclient.GetPieceTasks(pt.peerPacket.MainPeer, pt.ctx, request)
		if err == nil {
			return p, nil
		}
		pt.log.Errorf("get piece task from main peer(%s) error: %s", err, pt.peerPacket.MainPeer.PeerId)
	}
	for _, peer := range pt.peerPacket.StealPeers {
		if peer == nil {
			continue
		}
		p, err := dfclient.GetPieceTasks(peer, pt.ctx, request)
		if err != nil {
			pt.log.Errorf("get piece task from peer(%s) error: %s", err, peer.PeerId)
			continue
		}
		return p, nil
	}
	// TODO report no peer available error
	return nil, fmt.Errorf("no peers available")
}

func (pt *filePeerTask) getNextPieceNum(cur, limit int32) int32 {
	if pt.isCompleted() {
		return -1
	}
	i := cur + limit
	for ; pt.bitmap.IsSet(i); i++ {
	}
	return i
}

func (pt *filePeerTask) finish() error {
	var err error
	// send last progress
	pt.doneOnce.Do(func() {
		// send EOF piece result to scheduler
		pt.schedPieceResultCh <- scheduler.NewEndPieceResult(pt.bitmap.Settled(), pt.taskId, pt.peerId)
		pt.log.Debugf("end piece result sent")

		pt.progressCh <- &PeerTaskProgress{
			State: &base.ResponseState{
				Success: true,
				Code:    base.Code_SUCCESS,
			},
			TaskId:          pt.taskId,
			PeerID:          pt.peerId,
			ContentLength:   pt.contentLength,
			CompletedLength: pt.completedLength,
			Done:            true,
		}
		// callback to store data to output
		if err = pt.callback.Done(pt); err != nil {
			pt.progressCh <- &PeerTaskProgress{
				State: &base.ResponseState{
					Success: false,
					Code:    base.Code_CLIENT_ERROR,
					Msg:     fmt.Sprintf("peer task callback failed: %s", err),
				},
				TaskId:          pt.taskId,
				PeerID:          pt.peerId,
				ContentLength:   pt.contentLength,
				CompletedLength: pt.completedLength,
				Done:            true,
			}
		}
		close(pt.done)
		close(pt.progressCh)
	})
	return err
}

func (pt *filePeerTask) cleanUnfinished() {
	// send last progress
	pt.doneOnce.Do(func() {
		// send EOF piece result to scheduler
		pt.schedPieceResultCh <- scheduler.NewEndPieceResult(pt.bitmap.Settled(), pt.taskId, pt.peerId)
		pt.log.Debugf("end piece result sent")

		pt.progressCh <- &PeerTaskProgress{
			State: &base.ResponseState{
				Success: false,
				Code:    base.Code_CLIENT_ERROR,
				Msg:     "",
			},
			TaskId:          pt.taskId,
			PeerID:          pt.peerId,
			ContentLength:   pt.contentLength,
			CompletedLength: pt.completedLength,
			Done:            true,
		}
		close(pt.done)
		close(pt.progressCh)
	})
}
