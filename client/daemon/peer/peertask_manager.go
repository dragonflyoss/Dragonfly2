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
	"io"
	"sync"
	"time"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	schedulerclient "github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler/client"
)

type FilePeerTaskRequest struct {
	scheduler.PeerTaskRequest
	Output string
}

// PeerTaskManager processes all peer tasks request
type PeerTaskManager interface {
	// StartFilePeerTask starts a peer task to download a file
	// return a progress channel for request download progress
	StartFilePeerTask(ctx context.Context, req *FilePeerTaskRequest) (
		chan *PeerTaskProgress, error)
	// StartStreamPeerTask starts a peer task with stream io for reading directly without once more disk io
	StartStreamPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (
		reader io.Reader, attribute map[string]string, err error)
	// Stop stops the PeerTaskManager
	Stop(ctx context.Context) error
}

// PeerTask represents common interface to operate a peer task
type PeerTask interface {
	ReportPieceResult(pieceTask *base.PieceInfo, pieceResult *scheduler.PieceResult) error
	GetPeerID() string
	GetTaskID() string
	GetContentLength() int64
	SetCallback(PeerTaskCallback)
}

// PeerTaskCallback inserts some operations for peer task download lifecycle
type PeerTaskCallback interface {
	Init(pt PeerTask) error
	Done(pt PeerTask) error
	Fail(pt PeerTask, reason string) error
}

type peerTaskManager struct {
	host           *scheduler.PeerHost
	scheduler      schedulerclient.SchedulerClient
	pieceManager   PieceManager
	storageManager storage.Manager

	runningPeerTasks sync.Map
}

func NewPeerTaskManager(host *scheduler.PeerHost, pieceManager PieceManager, storageManager storage.Manager, schedulerClient schedulerclient.SchedulerClient) (PeerTaskManager, error) {
	ptm := &peerTaskManager{
		host:             host,
		runningPeerTasks: sync.Map{},
		pieceManager:     pieceManager,
		storageManager:   storageManager,
		scheduler:        schedulerClient,
	}
	return ptm, nil
}

func (ptm *peerTaskManager) StartFilePeerTask(ctx context.Context, req *FilePeerTaskRequest) (chan *PeerTaskProgress, error) {
	// TODO ensure scheduler is ok first
	pt, err := NewFilePeerTask(ctx, ptm.host, ptm.scheduler, ptm.pieceManager, &req.PeerTaskRequest)
	if err != nil {
		return nil, err
	}
	pt.SetCallback(&filePeerTaskCallback{
		ctx:   ctx,
		ptm:   ptm,
		req:   req,
		start: time.Now(),
	})

	ptm.runningPeerTasks.Store(req.PeerId, pt)

	// FIXME 1. merge same task id
	// FIXME 2. when failed due to schedulerClient error, relocate schedulerClient and retry
	//go pt.pullPiecesFromPeers()

	return pt.Start(ctx)
}

func (ptm *peerTaskManager) StartStreamPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (reader io.Reader, attribute map[string]string, err error) {
	pt, err := NewStreamPeerTask(ctx, ptm.host, ptm.scheduler, ptm.pieceManager, req)
	if err != nil {
		return nil, nil, err
	}
	pt.SetCallback(&streamPeerTaskCallback{
		ctx:   ctx,
		ptm:   ptm,
		req:   req,
		start: time.Now(),
	})

	ptm.runningPeerTasks.Store(req.PeerId, pt)

	// FIXME 1. merge same task id
	// FIXME 2. when failed due to schedulerClient error, relocate schedulerClient and retry
	//go pt.pullPiecesFromPeers()

	return pt.Start(ctx)
}

func (ptm *peerTaskManager) Stop(ctx context.Context) error {
	// TODO
	return nil
}

func (ptm *peerTaskManager) PeerTaskDone(pid string) {
	ptm.runningPeerTasks.Delete(pid)
	// TODO report peer result
	// ptm.scheduler.ReportPeerResult()
}
