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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/go-http-utils/headers"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
)

// PeerTaskManager processes all peer tasks request
type PeerTaskManager interface {
	// StartFilePeerTask starts a peer task to download a file
	// return a progress channel for request download progress
	// tiny stands task file is tiny and task is done
	StartFilePeerTask(ctx context.Context, req *FilePeerTaskRequest) (
		progress chan *PeerTaskProgress, tiny *TinyData, err error)
	// StartStreamPeerTask starts a peer task with stream io
	// tiny stands task file is tiny and task is done
	StartStreamPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (
		reader io.Reader, attribute map[string]string, err error)

	IsPeerTaskRunning(pid string) bool

	// Stop stops the PeerTaskManager
	Stop(ctx context.Context) error
}

// PeerTask represents common interface to operate a peer task
type PeerTask interface {
	Context() context.Context
	Log() *logger.SugaredLoggerOnWith
	ReportPieceResult(pieceTask *base.PieceInfo, pieceResult *scheduler.PieceResult) error
	GetPeerID() string
	GetTaskID() string
	GetTotalPieces() int32
	GetContentLength() int64
	// SetContentLength will called after download completed, when download from source without content length
	SetContentLength(int64) error
	SetCallback(PeerTaskCallback)
	AddTraffic(int64)
	GetTraffic() int64
}

// PeerTaskCallback inserts some operations for peer task download lifecycle
type PeerTaskCallback interface {
	Init(pt PeerTask) error
	Done(pt PeerTask) error
	Update(pt PeerTask) error
	Fail(pt PeerTask, code base.Code, reason string) error
	GetStartTime() time.Time
}

type TinyData struct {
	TaskId  string
	PeerID  string
	Content []byte
}

var tracer trace.Tracer

func init() {
	tracer = otel.Tracer("dfget-daemon")
}

type peerTaskManager struct {
	host            *scheduler.PeerHost
	schedulerClient schedulerclient.SchedulerClient
	schedulerOption config.SchedulerOption
	pieceManager    PieceManager
	storageManager  storage.Manager

	runningPeerTasks sync.Map
}

func NewPeerTaskManager(
	host *scheduler.PeerHost,
	pieceManager PieceManager,
	storageManager storage.Manager,
	schedulerClient schedulerclient.SchedulerClient,
	schedulerOption config.SchedulerOption) (PeerTaskManager, error) {

	ptm := &peerTaskManager{
		host:             host,
		runningPeerTasks: sync.Map{},
		pieceManager:     pieceManager,
		storageManager:   storageManager,
		schedulerClient:  schedulerClient,
		schedulerOption:  schedulerOption,
	}
	return ptm, nil
}

func (ptm *peerTaskManager) StartFilePeerTask(ctx context.Context, req *FilePeerTaskRequest) (chan *PeerTaskProgress, *TinyData, error) {
	// TODO ensure scheduler is ok first

	start := time.Now()
	ctx, pt, tiny, err := newFilePeerTask(ctx, ptm.host, ptm.pieceManager, &req.PeerTaskRequest, ptm.schedulerClient, ptm.schedulerOption)
	if err != nil {
		return nil, nil, err
	}
	// tiny file content is returned by scheduler, just write to output
	if tiny != nil {
		// TODO enable trace for tiny peer task
		//defer pt.Span().End()
		_, err = os.Stat(req.Output)
		if err == nil {
			// remove exist file
			logger.Infof("destination file %q exists, purge it first", req.Output)
			os.Remove(req.Output)
		}
		dstFile, err := os.OpenFile(req.Output, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			//pt.Span().RecordError(err)
			logger.Errorf("open tasks destination file error: %s", err)
			return nil, nil, err
		}
		defer dstFile.Close()
		n, err := dstFile.Write(tiny.Content)
		if err != nil {
			//pt.Span().RecordError(err)
			return nil, nil, err
		}
		logger.Debugf("copied tasks data %d bytes to %s", n, req.Output)
		return nil, tiny, nil
	}
	pt.SetCallback(&filePeerTaskCallback{
		ctx:   ctx,
		ptm:   ptm,
		req:   req,
		start: start,
	})

	ptm.runningPeerTasks.Store(req.PeerId, pt)

	// FIXME 1. merge same task id
	// FIXME 2. when failed due to schedulerClient error, relocate schedulerClient and retry
	progress, err := pt.Start(ctx)
	return progress, nil, err
}

func (ptm *peerTaskManager) StartStreamPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (reader io.Reader, attribute map[string]string, err error) {
	start := time.Now()
	ctx, pt, tiny, err := newStreamPeerTask(ctx, ptm.host, ptm.pieceManager, req, ptm.schedulerClient, ptm.schedulerOption)
	if err != nil {
		return nil, nil, err
	}
	// tiny file content is returned by scheduler, just write to output
	if tiny != nil {
		return bytes.NewBuffer(tiny.Content), map[string]string{
			headers.ContentLength: fmt.Sprintf("%d", len(tiny.Content)),
		}, nil
	}

	pt.SetCallback(&streamPeerTaskCallback{
		ctx:   ctx,
		ptm:   ptm,
		req:   req,
		start: start,
	})

	ptm.runningPeerTasks.Store(req.PeerId, pt)

	// FIXME 1. merge same task id
	// FIXME 2. when failed due to schedulerClient error, relocate schedulerClient and retry

	reader, attribute, err = pt.Start(ctx)
	return reader, attribute, err
}

func (ptm *peerTaskManager) Stop(ctx context.Context) error {
	// TODO
	return nil
}

func (ptm *peerTaskManager) PeerTaskDone(pid string) {
	ptm.runningPeerTasks.Delete(pid)
}

func (ptm *peerTaskManager) IsPeerTaskRunning(pid string) bool {
	_, ok := ptm.runningPeerTasks.Load(pid)
	return ok
}
