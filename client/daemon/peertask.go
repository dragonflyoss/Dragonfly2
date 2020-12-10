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

package daemon

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/dragonflyoss/Dragonfly2/client/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
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

// SchedulerPeerTaskClient represents some peer task interface from the dragonfly scheduler
type SchedulerPeerTaskClient interface {
	// register task for specified peer
	RegisterPeerTask(ctx context.Context,
		in *scheduler.PeerTaskRequest,
		opts ...grpc.CallOption) (*scheduler.PiecePackage, error)
	// push piece result and pull piece tasks
	PullPieceTasks(ctx context.Context,
		opts ...grpc.CallOption) (scheduler.Scheduler_PullPieceTasksClient, error)
}

// SchedulerPieceClient represents some piece interface from the dragonfly scheduler
type SchedulerPieceClient interface {
	grpc.ClientStream
	Send(*scheduler.PieceResult) error
	Recv() (*scheduler.PiecePackage, error)
}

type peerTask struct {
	ctx                     context.Context
	schedulerPeerTaskClient SchedulerPeerTaskClient
	schedulerPieceClient    SchedulerPieceClient
	pieceManager            PieceManager

	// piecePackage in the fly
	piecePackage *scheduler.PiecePackage

	done     chan struct{}
	doneOnce sync.Once

	pieceScheduled    uint64
	pieceProcessed    uint64
	allPieceScheduled bool

	peerId          string
	taskId          string
	contentLength   uint64
	completedLength uint64

	progressCh   chan *PeerTaskProgress
	progressOnce sync.Once
	callback     PeerTaskCallback

	log *logrus.Entry
}

func (pt *peerTask) SetCallback(callback PeerTaskCallback) {
	pt.callback = callback
}

type PeerTaskProgress struct {
	State           *base.ResponseState
	TaskId          string
	ContentLength   uint64
	CompletedLength uint64
	Done            bool
}

func NewFilePeerTask(ctx context.Context,
	schedulerClient SchedulerPeerTaskClient,
	pieceManager PieceManager,
	request *scheduler.PeerTaskRequest) (*peerTask, error) {
	// register peer task and pull first piece package
	piecePackage, err := schedulerClient.RegisterPeerTask(ctx, request)
	if err != nil {
		return nil, err
	}

	return &peerTask{
		schedulerPeerTaskClient: schedulerClient,
		pieceManager:            pieceManager,
		piecePackage:            piecePackage,
		peerId:                  request.Pid,
		taskId:                  piecePackage.TaskId,
		contentLength:           piecePackage.ContentLength,
		done:                    make(chan struct{}),
		doneOnce:                sync.Once{},
		progressOnce:            sync.Once{},

		log: logrus.WithFields(
			logrus.Fields{
				"taskID": piecePackage.TaskId,
				"peerID": request.Pid,
			}),
	}, nil
}

func (pt *peerTask) GetPeerID() string {
	return pt.peerId
}

func (pt *peerTask) GetTaskID() string {
	return pt.taskId
}

func (pt *peerTask) GetContentLength() uint64 {
	return pt.contentLength
}

func (pt *peerTask) Start(ctx context.Context) (chan *PeerTaskProgress, error) {
	pt.ctx = ctx
	pullPieceTasksClient, err := pt.schedulerPeerTaskClient.PullPieceTasks(ctx)
	if err != nil {
		return nil, err
	}

	// FIXME buffered channel
	pt.progressCh = make(chan *PeerTaskProgress)
	pt.schedulerPieceClient = pullPieceTasksClient

	go pt.pullPieces()

	// return a progress channel for request download progress
	return pt.progressCh, nil
}

func (pt *peerTask) pullPieces() {
	var err error
	for {
		// RegisterPeerTask return a PiecePackage, process it first
		pt.pieceScheduled += uint64(len(pt.piecePackage.PieceTasks))
		if pt.piecePackage.Last {
			pt.allPieceScheduled = true
		}

		pt.pieceManager.PullPieces(pt, pt.piecePackage)
		if pt.piecePackage.Last {
			// when last piece package sent
			// scheduler will close the client, just break here
			break
		}

		pt.piecePackage, err = pt.schedulerPieceClient.Recv()
		if err != nil {
			// FIXME
			panic("FIXME")
		}
		if pt.piecePackage == nil {
			// FIXME
			panic("FIXME")
		}
	}

	<-pt.done
	// FIXME log err
	err = pt.schedulerPieceClient.CloseSend()
	if err != nil {
		pt.log.Warnf("close piece client error: %s", err)
	}
}

// FIXME goroutine safe for channel
func (pt *peerTask) PushPieceResult(pieceResult *scheduler.PieceResult) error {
	err := pt.schedulerPieceClient.Send(pieceResult)
	if err != nil {
		return err
	}
	// FIXME 防止重入
	if pt.allPiecesDownloaded() {
		return nil
	}
	r := util.MustParseRange(pieceResult.PieceRange, int64(pt.contentLength))
	// send progress first to avoid close channel panic
	p := &PeerTaskProgress{
		State: &base.ResponseState{
			Success: pieceResult.Success,
			Code:    pieceResult.ErrorCode,
			// sMsg: "",
		},
		TaskId:          pt.taskId,
		ContentLength:   pt.contentLength,
		CompletedLength: pt.completedLength + uint64(r.Length),
		Done:            false,
	}
	select {
	case pt.progressCh <- p:
		pt.log.Debugf("peer task %s(task id %s) progress sent", pt.peerId, pt.taskId)
	case <-pt.ctx.Done():
		pt.log.Warnf("peer task context done due to %s", pt.ctx.Err())
		// TODO
	}

	atomic.AddUint64(&pt.pieceProcessed, 1)
	atomic.AddUint64(&pt.completedLength, uint64(r.Length))

	// FIXME double check, ensure there is only one goroutine ?
	if !pt.allPiecesDownloaded() {
		return nil
	}

	pt.doneOnce.Do(func() {
		// callback to store data to output
		if err = pt.callback.Done(); err != nil {
			pt.progressCh <- &PeerTaskProgress{
				State: &base.ResponseState{
					Success: false,
					Code:    base.Code_ERROR,
					Msg:     fmt.Sprintf("peer task callback failed: %s", err),
				},
				TaskId:          pt.taskId,
				ContentLength:   pt.contentLength,
				CompletedLength: pt.completedLength,
				Done:            true,
			}
		} else {
			// send last progress
			pt.progressCh <- &PeerTaskProgress{
				State: &base.ResponseState{
					Success: pieceResult.Success,
					Code:    pieceResult.ErrorCode,
					// Msg: "",
				},
				TaskId:          pt.taskId,
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

func (pt *peerTask) allPiecesDownloaded() bool {
	if !pt.allPieceScheduled {
		return false
	}
	// TODO use bitmap or segment tree
	piece := pt.pieceScheduled == pt.pieceProcessed
	length := pt.completedLength == pt.contentLength
	if piece && length {
		return true
	}
	return false
}
