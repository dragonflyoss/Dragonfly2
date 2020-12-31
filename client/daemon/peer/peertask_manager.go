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

	"github.com/dragonflyoss/Dragonfly2/client/daemon/misc"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
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

// PeerTaskCallback inserts some operations for peer task download lifecycle
type PeerTaskCallback interface {
	Done() error
}

type peerTaskManager struct {
	scheduler        scheduler.SchedulerClient
	schedulerLocator misc.SchedulerLocator
	pieceManager     PieceManager
	storageManager   storage.Manager

	runningPeerTasks sync.Map
}

type peerTaskCallback struct {
	DoneFunc func() error
}

func (p *peerTaskCallback) Done() error {
	if p.DoneFunc != nil {
		return p.DoneFunc()
	}
	return nil
}

func NewPeerTaskManager(pieceManager PieceManager, storageManager storage.Manager, locator misc.SchedulerLocator) (PeerTaskManager, error) {
	ptm := &peerTaskManager{
		runningPeerTasks: sync.Map{},
		pieceManager:     pieceManager,
		storageManager:   storageManager,
		schedulerLocator: locator,
	}
	// TODO handle error
	locator.Refresh(nil)
	ptm.AdmitScheduler()
	return ptm, nil
}

func (ptm *peerTaskManager) StartFilePeerTask(ctx context.Context, req *FilePeerTaskRequest) (chan *PeerTaskProgress, error) {
	// TODO ensure scheduler is ok first
	pt, err := NewFilePeerTask(ctx, ptm.scheduler, ptm.pieceManager, &req.PeerTaskRequest)
	if err != nil {
		return nil, err
	}
	// when peer task done, call peer task manager to store data
	pt.SetCallback(&peerTaskCallback{
		DoneFunc: func() error {
			err := ptm.storageManager.Store(
				context.Background(),
				&storage.StoreRequest{
					PeerID:      pt.GetPeerID(),
					TaskID:      pt.GetTaskID(),
					Destination: req.Output,
				})
			if err != nil {
				return err
			}
			ptm.PeerTaskDone(req.PeerId)
			return nil
		},
	})

	// prepare storage
	err = ptm.storageManager.RegisterTask(ctx, &storage.RegisterTaskRequest{
		PeerID:      pt.peerId,
		TaskID:      pt.taskId,
		Destination: req.Output,
	})
	if err != nil {
		logger.Errorf("register task to storage manager failed: %s", err)
		return nil, err
	}
	ptm.runningPeerTasks.Store(req.PeerId, pt)

	// FIXME 1. merge same task id
	// FIXME 2. when failed due to schedulerClient error, relocate schedulerClient and retry
	//go pt.pullPiecesFromPeers()

	return pt.Start(ctx)
}

func (ptm *peerTaskManager) StartStreamPeerTask(ctx context.Context, request *scheduler.PeerTaskRequest) (reader io.Reader, attribute map[string]string, err error) {
	panic("implement me")
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

// TODO check scheduler alive periodicity
func (ptm *peerTaskManager) AdmitScheduler() error {
	sched, all, err := ptm.schedulerLocator.Next()
	if err != nil {
		return err
	}
	ptm.scheduler = sched
	if all {
		err = ptm.schedulerLocator.Refresh(nil)
		if err != nil {
			return err
		}
	}
	return nil
}
