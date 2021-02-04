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

	logger "github.com/dragonflyoss/Dragonfly/v2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/base"
	dfclient "github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/dfdaemon/client"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/scheduler/client"
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
	*logger.SugaredLoggerOnWith
	ctx context.Context

	// pieceManager will be used for downloading piece
	pieceManager PieceManager
	// host info about current host
	host *scheduler.PeerHost
	// callback holds some actions, like init, done, fail actions
	callback PeerTaskCallback

	// peer task meta info
	peerId          string
	taskId          string
	contentLength   int64
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
	// progressCh holds progress status
	progressCh chan *PeerTaskProgress

	// bitmap stands all pieces download status
	bitmap *Bitmap
	// lock used by piece result manage, when update bitmap, lock first
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
		ctx:             ctx,
		host:            host,
		pieceResultCh:   schedPieceResultCh,
		peerPacketCh:    schedPeerPacketCh,
		pieceManager:    pieceManager,
		peerPacketReady: make(chan bool),
		peerId:          request.PeerId,
		taskId:          result.TaskId,
		done:            make(chan struct{}),
		once:            sync.Once{},
		bitmap:          NewBitmap(),
		lock:            &sync.Mutex{},
		failedPieceCh:   make(chan int32, 4),
		progressCh:      make(chan *PeerTaskProgress),
		contentLength:   -1,

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
			pt.Warnf("scheduler client send nil PeerPacket")
			continue
		}
		pt.Debugf("receive peer packet: %#v", peerPacket)
		if peerPacket.MainPeer == nil && peerPacket.StealPeers == nil {
			pt.Warnf("scheduler client send a PeerPacket will empty peers")
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
func (pt *filePeerTask) pullPiecesFromPeers(pti PeerTask, cleanFunc func()) {
	// wait available peer daemon
	<-pt.peerPacketReady
	var (
		num         int32
		limit       int32
		initialized bool
	)
loop:
	for {
		limit = pt.pieceParallelCount
		select {
		case <-pt.done:
			pt.Infof("peer task done, stop get pieces from peer")
			break loop
		case <-pt.ctx.Done():
			pt.Debugf("context done due to %s", pt.ctx.Err())
			pt.callback.Fail(pt, pt.ctx.Err().Error())
			break loop
		case failed := <-pt.failedPieceCh:
			pt.Warnf("download piece/%d failed, retry", failed)
			num = failed
			limit = 1
		default:
		}

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
			case <-pt.ctx.Done():
				pt.Debugf("context done due to %s", pt.ctx.Err())
				pt.callback.Fail(pt, pt.ctx.Err().Error())
				break loop
			case <-pt.peerPacketReady:
				pt.Infof("new peer client ready")
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
			pt.Infof("no more pieces, stop get pieces from peer")
			break loop
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
			pt.Warnf("recover from %s", r)
		}
	}()
	// retry failed piece
	if !pieceResult.Success {
		pt.pieceResultCh <- pieceResult
		pt.failedPieceCh <- pieceResult.PieceNum
		return nil
	}
	pieceResult.FinishedCount = pt.bitmap.Settled()
	pt.pieceResultCh <- pieceResult
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
		pt.Debugf("progress sent, %d/%d", p.CompletedLength, p.ContentLength)
	case <-pt.ctx.Done():
		pt.Warnf("peer task context done due to %s", pt.ctx.Err())
		return pt.ctx.Err()
	}

	pt.lock.Lock()
	defer pt.lock.Unlock()
	if pt.bitmap.IsSet(pieceResult.PieceNum) {
		pt.Warnf("piece %d is already reported, skipped", pieceResult.PieceNum)
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

func (pt *filePeerTask) preparePieceTasks(request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	pt.pieceParallelCount = pt.peerPacket.ParallelCount
	var failedPeers []*scheduler.PeerPacket_DestPeer
	request.DstPid = pt.peerPacket.MainPeer.PeerId
	p, err := pt.preparePieceTasksByPeer(pt.peerPacket.MainPeer, request)
	if err == nil {
		return p, nil
	}
	failedPeers = append(failedPeers, pt.peerPacket.MainPeer)
	for _, peer := range pt.peerPacket.StealPeers {
		request.DstPid = peer.PeerId
		p, err = pt.preparePieceTasksByPeer(peer, request)
		if err == nil {
			return p, nil
		}
		failedPeers = append(failedPeers, peer)
	}
	if len(failedPeers) == 0 {
		pt.pieceResultCh <- &scheduler.PieceResult{
			TaskId:        pt.taskId,
			SrcPid:        pt.peerId,
			DstPid:        "",
			Success:       false,
			Code:          401,
			HostLoad:      nil,
			FinishedCount: 0,
		}
	} else {
		// TODO(jim): report all not available peers
		pt.pieceResultCh <- &scheduler.PieceResult{
			TaskId:        pt.taskId,
			SrcPid:        pt.peerId,
			DstPid:        failedPeers[0].PeerId,
			Success:       false,
			Code:          401,
			HostLoad:      nil,
			FinishedCount: 0,
		}
	}
	return nil, fmt.Errorf("no peers available")
}

func (pt *filePeerTask) preparePieceTasksByPeer(peer *scheduler.PeerPacket_DestPeer, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	if peer == nil {
		return nil, fmt.Errorf("empty peer")
	}
	p, err := dfclient.GetPieceTasks(peer, pt.ctx, request)
	if err != nil {
		pt.Errorf("get piece task from peer(%s) error: %s", peer.PeerId, err)
		return nil, err
	}
	if p.State.Success {
		return p, nil
	}
	pt.Warnf("get piece task from peer(%s) failed: %d/%s", peer.PeerId, p.State.Code, p.State.Msg)
	return nil, fmt.Errorf("get piece failed: %d/%s", p.State.Code, p.State.Msg)
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
	pt.once.Do(func() {
		// send EOF piece result to scheduler
		pt.pieceResultCh <- scheduler.NewEndPieceResult(pt.bitmap.Settled(), pt.taskId, pt.peerId)
		pt.Debugf("finish end piece result sent")

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
	pt.once.Do(func() {
		// send EOF piece result to scheduler
		pt.pieceResultCh <- scheduler.NewEndPieceResult(pt.bitmap.Settled(), pt.taskId, pt.peerId)
		pt.Debugf("clean up end piece result sent")

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
