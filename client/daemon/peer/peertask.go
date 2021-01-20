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
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"

	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	schedulerclient "github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler/client"
)

// PeerTask represents common interface to operate a peer task
type PeerTask interface {
	ReportPieceResult(pieceTask *base.PieceInfo, pieceResult *scheduler.PieceResult) error
	GetPeerID() string
	GetTaskID() string
	GetContentLength() int64
	SetCallback(PeerTaskCallback)
}

// FilePeerTask represents a peer task to download a file
type FilePeerTask interface {
	PeerTask
	// Start start the special peer task, return a *PeerTaskProgress channel for updating download progress
	Start(ctx context.Context) (chan *PeerTaskProgress, error)
}

// StreamPeerTask represents a peer task with stream io for reading directly without once more disk io
type StreamPeerTask interface {
	PeerTask
	// Start start the special peer task, return a io.Reader for stream io
	// when all data transferred, reader return a io.EOF
	// attribute stands some extra data, like HTTP response Header
	Start(ctx context.Context) (reader io.Reader, attribute map[string]string, err error)
}

type filePeerTask struct {
	ctx          context.Context
	pieceManager PieceManager

	schedPieceResultCh chan<- *scheduler.PieceResult
	schedPeerPacketCh  <-chan *scheduler.PeerPacket

	// peerPacket in the fly
	peerPacket *scheduler.PeerPacket

	peerClient      dfdaemon.DaemonClient
	peerClientConn  *grpc.ClientConn
	peerClientInfo  *scheduler.PeerPacket_DestPeer
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
	schedulerClient schedulerclient.SchedulerClient,
	pieceManager PieceManager,
	request *scheduler.PeerTaskRequest) (*filePeerTask, error) {
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
		log:                logger.With("peer", request.PeerId, "task", result.TaskId),
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
	go pt.pullPiecesFromPeers()
	// return a progress channel for request download progress
	return pt.progressCh, nil
}

func (pt *filePeerTask) receivePeerPacket() {
	var (
		peerPacket *scheduler.PeerPacket
		ok         bool
	)
	defer close(pt.peerClientReady)
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
			pt.log.Warnf("scheduler piece client send nil PeerPacket")
			continue
		}
		pt.peerPacket = peerPacket
		if updated := pt.updateAvailablePeerClient(); updated {
			pt.peerClientReady <- true
		}
	}
}

// TODO when main peer is not available, switch to steel peers
func (pt *filePeerTask) pullPiecesFromPeers() {
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
			break getPiecesTasks
		default:
		}

		piecePacket, err := pt.peerClient.GetPieceTasks(pt.ctx, &base.PieceTaskRequest{
			TaskId:   pt.taskId,
			SrcIp:    "",
			StartNum: num,
			Limit:    limit,
		})
		if err != nil || !piecePacket.State.Success {
			// FIXME(jim): push failed result to scheduler
			pt.log.Errorf("get piece task error: %s, try to find available peer", err)
			<-pt.peerClientReady
			pt.log.Infof("find new available peer %s:%d", pt.peerClientInfo.Ip, pt.peerClientInfo.RpcPort)
		}

		if !initialized {
			pt.callback.Init(piecePacket.ContentLength)
			pt.contentLength = piecePacket.ContentLength
			initialized = true
		}

		// TODO(jim): next round start piece num
		num += limit
		pt.pieceManager.PullPieces(pt, piecePacket)
	}

	err := pt.peerClientConn.Close()
	if err != nil {
		pt.log.Warnf("close grpc client error: %s", err)
	}
	//close(pt.schedPieceResultCh)
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
		pt.failedPieceCh <- pieceResult.PieceNum
	}
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

	var err error
	// send last progress
	pt.doneOnce.Do(func() {
		// send EOF piece result to scheduler
		pt.schedPieceResultCh <- scheduler.NewEndPieceResult(pt.bitmap.Settled(), pt.taskId, pt.peerId)
		pt.log.Debugf("end piece result sent")
		// callback to store data to output
		if err = pt.callback.Done(); err != nil {
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
		pt.progressCh <- &PeerTaskProgress{
			State: &base.ResponseState{
				Success: pieceResult.Success,
				Code:    pieceResult.Code,
				// Msg: "",
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

	return err
}

func (pt *filePeerTask) isCompleted() bool {
	return pt.completedLength == pt.contentLength
}

func (pt *filePeerTask) updateAvailablePeerClient() bool {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	// close old grpc conn
	if pt.peerClientConn != nil {
		pt.peerClientConn.Close()
	}
	// try main peer when current peer client is not main peer
	if err := pt.connectPeerClient(pt.peerPacket.MainPeer); err == nil {
		pt.pieceParallelCount = pt.peerPacket.ParallelCount
		return true
	} else {
		pt.log.Warnf("connect main peer from scheduler error: %s", err)
	}
	// try other peers
	for _, peer := range pt.peerPacket.StealPeers {
		err := pt.connectPeerClient(peer)
		if err == nil {
			pt.pieceParallelCount = pt.peerPacket.ParallelCount
			return true
		}
	}
	return false
}

func (pt *filePeerTask) connectPeerClient(peer *scheduler.PeerPacket_DestPeer) error {
	if peer == nil {
		return fmt.Errorf("nil dest peer")
	}
	if pt.peerClientInfo != nil && pt.peerClientInfo.PeerId == peer.PeerId {
		return fmt.Errorf("same dest peer with current connected")
	}
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", peer.Ip, peer.RpcPort),
		grpc.WithInsecure())
	if err != nil {
		return err
	}
	daemonClient := dfdaemon.NewDaemonClient(conn)
	r, err := daemonClient.CheckHealth(context.Background(), &base.EmptyRequest{})
	if err == nil && r.Success {
		pt.peerClient = daemonClient
		pt.peerClientConn = conn
		pt.peerClientInfo = peer
		return nil
	}
	if err != nil {
		pt.log.Errorf("check daemon client failed: %s", err)
	}
	if r != nil {
		pt.log.Errorf("check daemon client failed, response: %#v", r)
	}
	return fmt.Errorf("connect to peer error: %s", err)
}
