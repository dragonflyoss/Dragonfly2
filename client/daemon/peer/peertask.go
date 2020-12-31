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
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"

	"github.com/dragonflyoss/Dragonfly2/client/util"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
)

// PeerTask represents common interface to operate a peer task
type PeerTask interface {
	PushPieceResult(pieceResult *scheduler.PieceResult) error
	GetPeerID() string
	GetTaskID() string
	GetContentLength() uint64
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
	ctx                        context.Context
	schedulerClient            scheduler.SchedulerClient
	schedulerPieceResultClient scheduler.Scheduler_ReportPieceResultClient
	pieceManager               PieceManager

	// peerPacket in the fly
	peerPacket      *scheduler.PeerPacket
	peerClient      dfdaemon.DaemonClient
	peerClientInfo  *scheduler.PeerHost
	peerClientReady sync.Cond

	done     chan struct{}
	doneOnce sync.Once

	peerId          string
	taskId          string
	contentLength   uint64
	completedLength uint64

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
	ContentLength   uint64
	CompletedLength uint64
	Done            bool
}

func NewFilePeerTask(ctx context.Context,
	schedulerClient scheduler.SchedulerClient,
	pieceManager PieceManager,
	request *scheduler.PeerTaskRequest) (*filePeerTask, error) {
	// register peer task and pull first piece package
	peerPacket, err := schedulerClient.RegisterPeerTask(ctx, request)
	if err != nil {
		logger.Errorf("register peer task failed: %s, peer id: %s", err, request.PeerId)
		return nil, err
	}
	logger.Infof("register task success, task id: %s, peer id: %s, content length: %d",
		peerPacket.TaskId, request.PeerId, peerPacket.ContentLength)
	return &filePeerTask{
		ctx:             ctx,
		schedulerClient: schedulerClient,
		pieceManager:    pieceManager,
		peerPacket:      peerPacket,
		peerClientReady: sync.Cond{},
		peerId:          request.PeerId,
		taskId:          peerPacket.TaskId,
		contentLength:   uint64(peerPacket.ContentLength),
		done:            make(chan struct{}),
		doneOnce:        sync.Once{},
		bitmap:          NewBitmap(),
		lock:            &sync.Mutex{},
		log:             logger.With("peer", request.PeerId, "task", peerPacket.TaskId),
		failedPieceCh:   make(chan int32, peerPacket.ParallelCount),
		progressCh:      make(chan *PeerTaskProgress),
	}, nil
}

func (pt *filePeerTask) GetPeerID() string {
	return pt.peerId
}

func (pt *filePeerTask) GetTaskID() string {
	return pt.taskId
}

func (pt *filePeerTask) GetContentLength() uint64 {
	return pt.contentLength
}

func (pt *filePeerTask) Start(ctx context.Context) (chan *PeerTaskProgress, error) {
	var err error
	pt.schedulerPieceResultClient, err = pt.schedulerClient.ReportPieceResult(ctx)
	if err != nil {
		return nil, err
	}
	go pt.receivePeerPacket()
	go pt.pullPiecesFromPeers()

	// return a progress channel for request download progress
	return pt.progressCh, nil
}

func (pt *filePeerTask) receivePeerPacket() {
	var err error
WaitScheduler:
	for {
		select {
		case <-pt.ctx.Done():
			pt.log.Debugf("context done due to %s", pt.ctx.Err())
			break WaitScheduler
		case <-pt.done:
			pt.log.Infof("peer task done")
			break WaitScheduler
		default:
		}

		pt.peerPacket, err = pt.schedulerPieceResultClient.Recv()
		if err == io.EOF {
			pt.log.Infof("scheduler piece client close, all piece should be downloaded")
			if !pt.allPiecesDownloaded() {
				pt.log.Errorf("FIXME scheduler says all piece downloaded, but not in daemon")
			}
			break
		}

		if err != nil || pt.peerPacket == nil {
			pt.log.Infof("scheduler piece client recv error or nil PeerPacket: %s", err)
			// TODO cancel task or reconnect scheduler
			continue
		}
		if pt.peerPacket != nil {
			if updated := pt.updateAvailablePeerClient(); updated {
				pt.peerClientReady.Broadcast()
			}
		}
	}
}

func (pt *filePeerTask) pullPiecesFromPeers() {
	// find available peer daemon
	pt.updateAvailablePeerClient()
	if pt.peerClient == nil {
		pt.peerClientReady.Wait()
	}

	var num int32
	var limit int32
getPiecesTasks:
	for {
		limit = pt.peerPacket.ParallelCount
		select {
		case <-pt.done:
			pt.log.Infof("task done")
			break getPiecesTasks
		case failed := <-pt.failedPieceCh:
			pt.log.Warnf("download piece/%d failed, retry", failed)
			num = failed
			limit = 1
		default:
		}
		piecePacket, err := pt.peerClient.GetPieceTasks(pt.ctx, &base.PieceTaskRequest{
			TaskId:   pt.taskId,
			SrcIp:    "",
			StartNum: num,
			Limit:    limit,
		})
		num += limit
		if err != nil {
			// TODO push failed result to scheduler
			pt.log.Errorf("get piece task error: %s, try to find available peer", err)
			if updated := pt.updateAvailablePeerClient(); !updated {
				pt.log.Infof("wait scheduler for more available peers")
				pt.peerClientReady.Wait()
			}
			pt.log.Infof("find new available peer %s:%d", pt.peerClientInfo.Ip, pt.peerClientInfo.Port)
		}

		pt.pieceManager.PullPieces(pt, piecePacket)
	}

	err := pt.schedulerPieceResultClient.CloseSend()
	if err != nil {
		pt.log.Warnf("close piece client error: %s", err)
	}
}

// FIXME goroutine safe for channel
func (pt *filePeerTask) PushPieceResult(pieceResult *scheduler.PieceResult) error {
	// retry failed piece
	if !pieceResult.Success {
		pt.failedPieceCh <- pieceResult.PieceNum
	}
	err := pt.schedulerPieceResultClient.Send(pieceResult)
	if err != nil {
		pt.log.Warnf("report piece result failed: %s", err)
	}
	r := util.MustParseRange(pieceResult.PieceRange, int64(pt.contentLength))
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
		CompletedLength: pt.completedLength + uint64(r.Length),
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
	if pt.bitmap.IsSet(int(pieceResult.PieceNum)) {
		pt.log.Warnf("piece %d is already reported, skipped", pieceResult.PieceNum)
		return nil
	}
	// mark piece processed
	pt.bitmap.Set(int(pieceResult.PieceNum))
	atomic.AddUint64(&pt.completedLength, uint64(r.Length))

	if !pt.allPiecesDownloaded() {
		return nil
	}

	// send last progress
	pt.doneOnce.Do(func() {
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

func (pt *filePeerTask) allPiecesDownloaded() bool {
	return pt.completedLength == pt.contentLength
}

func (pt *filePeerTask) updateAvailablePeerClient() bool {
	// try main peer when current peer client is not main peer
	if pt.peerPacket.MainPeer != nil &&
		(pt.peerClientInfo == nil || pt.peerClientInfo.Uuid != pt.peerPacket.MainPeer.Uuid) {
		dc, err := pt.connectPeerClient(pt.peerPacket.MainPeer.Ip, pt.peerPacket.MainPeer.Port)
		if err == nil {
			pt.peerClient = dc
			pt.peerClientInfo = pt.peerPacket.MainPeer
			return true
		}
	} else {
		pt.log.Infof("received empty main peer from scheduler")
	}
	// try other peers
	for _, peer := range pt.peerPacket.StealPeers {
		// skip current peer daemon
		if pt.peerClientInfo != nil && pt.peerClientInfo.Uuid == peer.Uuid {
			continue
		}
		dc, err := pt.connectPeerClient(peer.Ip, peer.Port)
		if err == nil {
			pt.peerClient = dc
			pt.peerClientInfo = peer
			return true
		}
	}
	return false
}

func (pt *filePeerTask) connectPeerClient(ip string, port int32) (dfdaemon.DaemonClient, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", ip, port),
		grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	daemonClient := dfdaemon.NewDaemonClient(conn)
	r, err := daemonClient.CheckHealth(context.Background(), &base.EmptyRequest{})
	if err == nil && r.Success {
		return daemonClient, nil
	}
	if err != nil {
		pt.log.Errorf("check daemon client failed: %s", err)
	}
	if r != nil {
		pt.log.Errorf("check daemon client failed, response: %#v", r)
	}
	return nil, err
}

type Bitmap struct {
	bits    []byte
	cap     int
	settled int
}

func NewBitmap() *Bitmap {
	return &Bitmap{
		bits: make([]byte, 8),
		cap:  8 * 8,
	}
}

func NewBitmapWithCap(c int) *Bitmap {
	return &Bitmap{
		bits: make([]byte, c),
		cap:  c * 8,
	}
}

func (b *Bitmap) IsSet(i int) bool {
	if i >= b.cap {
		return false
	}
	return b.bits[i/8]&(1<<uint(7-i%8)) != 0
}

func (b *Bitmap) Set(i int) {
	if i >= b.cap {
		b.bits = append(b.bits, make([]byte, b.cap/8)...)
		b.cap *= 2
	}
	//if b.IsSet(i) {
	//	return
	//}
	b.settled++
	b.bits[i/8] |= 1 << uint(7-i%8)
}

func (b *Bitmap) IsSettled() int {
	return b.settled
}

//func (b *Bitmap) Clear(i int) {
//	b.bits[i/8] &^= 1 << uint(7-i%8)
//}

func (b *Bitmap) Sets(xs ...int) {
	for _, x := range xs {
		b.Set(x)
	}
}

func (b Bitmap) String() string {
	return hex.EncodeToString(b.bits[:])
}
