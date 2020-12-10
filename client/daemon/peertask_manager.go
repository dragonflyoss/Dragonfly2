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
	"io"
	"sync"

	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
)

type FilePeerTaskRequest struct {
	scheduler.PeerTaskRequest
	Output string
}

// PeerTaskManager processes all peer tasks request
type PeerTaskManager interface {
	// StartFilePeerTask starts a peer task to download a file
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
	schedulerLocator SchedulerLocator
	pieceManager     PieceManager
	storageManager   StorageManager

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

func NewPeerTaskManager(pieceManager PieceManager, storageManager StorageManager, locator SchedulerLocator) (PeerTaskManager, error) {
	ptm := &peerTaskManager{
		runningPeerTasks: sync.Map{},
		pieceManager:     pieceManager,
		storageManager:   storageManager,
		schedulerLocator: locator,
	}
	ptm.AdmitScheduler()
	return ptm, nil
}

func (ptm *peerTaskManager) StartFilePeerTask(ctx context.Context, req *FilePeerTaskRequest) (chan *PeerTaskProgress, error) {
	pt, err := NewFilePeerTask(ctx, ptm.scheduler, ptm.pieceManager, &req.PeerTaskRequest)
	if err != nil {
		return nil, err
	}
	pt.SetCallback(&peerTaskCallback{
		DoneFunc: func() error {
			err := ptm.storageManager.StoreTaskData(
				context.Background(),
				&StoreRequest{
					TaskID:      pt.GetTaskID(),
					Destination: req.Output,
				})
			if err != nil {
				return err
			}
			ptm.PeerTaskDone(req.Pid)
			return nil
		},
	})
	ptm.runningPeerTasks.Store(req.Pid, pt)
	// FIXME 1. merge same task id
	// FIXME 2. when failed due to schedulerPeerTaskClient error, relocate schedulerPeerTaskClient and retry
	progress, err := pt.Start(ctx)
	if err != nil {
		return nil, err
	}

	return progress, nil
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
}

func (ptm *peerTaskManager) AdmitScheduler() {
	//ptm.scheduler = ptm.schedulerLocator.Locate()
}
