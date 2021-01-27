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
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
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
	// StartStreamPeerTask starts a peer task with stream io for reading directly without once more disk io
	StartStreamPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (
		reader io.Reader, attribute map[string]string, err error)
	// Stop stops the PeerTaskManager
	Stop(ctx context.Context) error
}

// PeerTaskCallback inserts some operations for peer task download lifecycle
type PeerTaskCallback interface {
	Init(pt PeerTask) error
	Done(pt PeerTask) error
	Fail(pt PeerTask, reason string) error
}

type peerTaskManager struct {
	scheduler      schedulerclient.SchedulerClient
	pieceManager   PieceManager
	storageManager storage.Manager

	runningPeerTasks sync.Map
}

type peerTaskCallback struct {
	FailFunc func(pt PeerTask, reason string) error
	DoneFunc func(pt PeerTask) error
	InitFunc func(pt PeerTask) error
}

func (p *peerTaskCallback) Done(pt PeerTask) error {
	if p.DoneFunc != nil {
		return p.DoneFunc(pt)
	}
	return nil
}

func (p *peerTaskCallback) Init(pt PeerTask) error {
	if p.InitFunc != nil {
		return p.InitFunc(pt)
	}
	return nil
}

func (p *peerTaskCallback) Fail(pt PeerTask, reason string) error {
	if p.FailFunc != nil {
		return p.FailFunc(pt, reason)
	}
	return nil
}

func NewPeerTaskManager(pieceManager PieceManager, storageManager storage.Manager, schedulerClient schedulerclient.SchedulerClient) (PeerTaskManager, error) {
	ptm := &peerTaskManager{
		runningPeerTasks: sync.Map{},
		pieceManager:     pieceManager,
		storageManager:   storageManager,
		scheduler:        schedulerClient,
	}
	return ptm, nil
}

func (ptm *peerTaskManager) StartFilePeerTask(ctx context.Context, req *FilePeerTaskRequest) (chan *PeerTaskProgress, error) {
	// TODO ensure scheduler is ok first
	pt, err := NewFilePeerTask(ctx, ptm.scheduler, ptm.pieceManager, &req.PeerTaskRequest)
	if err != nil {
		return nil, err
	}
	var start = time.Now()
	// when peer task done, call peer task manager to store data
	pt.SetCallback(&peerTaskCallback{
		InitFunc: func(pt PeerTask) error {
			// prepare storage
			err = ptm.storageManager.RegisterTask(ctx,
				storage.RegisterTaskRequest{
					CommonTaskRequest: storage.CommonTaskRequest{
						PeerID:      pt.GetPeerID(),
						TaskID:      pt.GetTaskID(),
						Destination: req.Output,
					},
					ContentLength: pt.GetContentLength(),
				})
			if err != nil {
				logger.Errorf("register task to storage manager failed: %s", err)
			}
			return err
		},
		DoneFunc: func(pt PeerTask) error {
			e := ptm.storageManager.Store(
				context.Background(),
				&storage.StoreRequest{
					PeerID:      pt.GetPeerID(),
					TaskID:      pt.GetTaskID(),
					Destination: req.Output,
				})
			if e != nil {
				return e
			}
			ptm.PeerTaskDone(req.PeerId)
			var end = time.Now()
			ptm.scheduler.ReportPeerResult(ctx, &scheduler.PeerResult{
				TaskId:         pt.GetTaskID(),
				PeerId:         pt.GetPeerID(),
				SrcIp:          "",
				SecurityDomain: "",
				Idc:            "",
				Url:            "",
				ContentLength:  pt.GetContentLength(),
				Traffic:        0,
				Cost:           uint32(end.Sub(start).Milliseconds()),
				Success:        true,
				Code:           base.Code_SUCCESS,
			})
			return nil
		},
		FailFunc: func(pt PeerTask, reason string) error {
			ptm.PeerTaskDone(req.PeerId)

			var end = time.Now()
			ptm.scheduler.ReportPeerResult(ctx, &scheduler.PeerResult{
				TaskId:         pt.GetTaskID(),
				PeerId:         pt.GetPeerID(),
				SrcIp:          "",
				SecurityDomain: "",
				Idc:            "",
				Url:            "",
				ContentLength:  pt.GetContentLength(),
				Traffic:        0,
				Cost:           uint32(end.Sub(start).Milliseconds()),
				Success:        false,
				Code:           base.Code_CLIENT_ERROR,
			})
			return nil
		},
	})

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
