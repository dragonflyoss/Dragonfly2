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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
)

// TaskManager processes all peer tasks request
type TaskManager interface {
	// StartFileTask starts a peer task to download a file
	// return a progress channel for request download progress
	// tiny stands task file is tiny and task is done
	StartFileTask(ctx context.Context, req *FileTaskRequest) (
		progress chan *FileTaskProgress, tiny *TinyData, err error)
	// StartStreamTask starts a peer task with stream io
	// tiny stands task file is tiny and task is done
	StartStreamTask(ctx context.Context, req *StreamTaskRequest) (
		readCloser io.ReadCloser, attribute map[string]string, err error)

	IsPeerTaskRunning(id string) bool

	// Stop stops the PeerTaskManager
	Stop(ctx context.Context) error
}

//go:generate mockgen -source peertask_manager.go -package peer -self_package d7y.io/dragonfly/v2/client/daemon/peer -destination peertask_manager_mock_test.go
//go:generate mockgen -source peertask_manager.go -destination ../test/mock/peer/peertask_manager.go
// Task represents common interface to operate a peer task
type Task interface {
	Logger
	Context() context.Context
	Log() *logger.SugaredLoggerOnWith

	GetStorage() storage.TaskStorageDriver

	GetPeerID() string
	GetTaskID() string

	GetTotalPieces() int32
	SetTotalPieces(int32)

	GetContentLength() int64
	SetContentLength(int64)

	AddTraffic(uint64)
	GetTraffic() uint64

	SetPieceMd5Sign(string)
	GetPieceMd5Sign() string

	PublishPieceInfo(pieceNum int32, size uint32)
	ReportPieceResult(request *DownloadPieceRequest, result *DownloadPieceResult, err error)
}

type Logger interface {
	Log() *logger.SugaredLoggerOnWith
}

type TinyData struct {
	// span is used by peer task manager to record events without peer task
	span    trace.Span
	TaskID  string
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

	conductorLock    sync.Locker
	runningPeerTasks sync.Map

	perPeerRateLimit rate.Limit

	// enableMultiplex indicates reusing completed peer task storage
	// currently, only check completed peer task after register to scheduler
	// TODO multiplex the running peer task
	enableMultiplex bool

	calculateDigest bool

	getPiecesMaxRetry int
}

func NewPeerTaskManager(
	host *scheduler.PeerHost,
	pieceManager PieceManager,
	storageManager storage.Manager,
	schedulerClient schedulerclient.SchedulerClient,
	schedulerOption config.SchedulerOption,
	perPeerRateLimit rate.Limit,
	multiplex bool,
	calculateDigest bool,
	getPiecesMaxRetry int) (TaskManager, error) {

	ptm := &peerTaskManager{
		host:              host,
		runningPeerTasks:  sync.Map{},
		conductorLock:     &sync.Mutex{},
		pieceManager:      pieceManager,
		storageManager:    storageManager,
		schedulerClient:   schedulerClient,
		schedulerOption:   schedulerOption,
		perPeerRateLimit:  perPeerRateLimit,
		enableMultiplex:   multiplex,
		calculateDigest:   calculateDigest,
		getPiecesMaxRetry: getPiecesMaxRetry,
	}
	return ptm, nil
}

var _ TaskManager = (*peerTaskManager)(nil)

func (ptm *peerTaskManager) findPeerTaskConductor(taskID string) (*peerTaskConductor, bool) {
	pt, ok := ptm.runningPeerTasks.Load(taskID)
	if !ok {
		return nil, false
	}
	return pt.(*peerTaskConductor), true
}

func (ptm *peerTaskManager) getOrCreatePeerTaskConductor(ctx context.Context, taskID string, request *scheduler.PeerTaskRequest, limit rate.Limit) (*peerTaskConductor, error) {
	if ptc, ok := ptm.findPeerTaskConductor(taskID); ok {
		logger.Debugf("peer task found: %s/%s", ptc.taskID, ptc.peerID)
		return ptc, nil
	}
	ptc, err := ptm.newPeerTaskConductor(ctx, request, limit)
	if err != nil {
		return nil, err
	}

	ptm.conductorLock.Lock()
	// double check
	if p, ok := ptm.findPeerTaskConductor(taskID); ok {
		ptm.conductorLock.Unlock()
		logger.Debugf("same peer task found: %s/%s, cancel created peer task %s/%s",
			p.taskID, p.peerID, ptc.taskID, ptc.peerID)
		// cancel duplicate peer task
		ptc.cancel(base.Code_ClientContextCanceled, reasonContextCanceled)
		return p, nil
	}
	ptm.runningPeerTasks.Store(taskID, ptc)
	ptm.conductorLock.Unlock()

	ptc.run()
	return ptc, nil
}

func (ptm *peerTaskManager) StartFileTask(ctx context.Context, req *FileTaskRequest) (chan *FileTaskProgress, *TinyData, error) {
	if ptm.enableMultiplex {
		progress, ok := ptm.tryReuseFilePeerTask(ctx, req)
		if ok {
			metrics.PeerTaskReuseCount.Add(1)
			return progress, nil, nil
		}
	}
	// TODO ensure scheduler is ok first
	var limit = rate.Inf
	if ptm.perPeerRateLimit > 0 {
		limit = ptm.perPeerRateLimit
	}
	if req.Limit > 0 {
		limit = rate.Limit(req.Limit)
	}
	ctx, pt, err := ptm.newFileTask(ctx, req, limit)
	if err != nil {
		return nil, nil, err
	}

	// FIXME when failed due to schedulerClient error, relocate schedulerClient and retry
	progress, err := pt.Start(ctx)
	return progress, nil, err
}

func (ptm *peerTaskManager) StartStreamTask(ctx context.Context, req *StreamTaskRequest) (io.ReadCloser, map[string]string, error) {
	peerTaskRequest := &scheduler.PeerTaskRequest{
		Url:         req.URL,
		UrlMeta:     req.URLMeta,
		PeerId:      req.PeerID,
		PeerHost:    ptm.host,
		HostLoad:    nil,
		IsMigrating: false,
	}

	if ptm.enableMultiplex {
		r, attr, ok := ptm.tryReuseStreamPeerTask(ctx, peerTaskRequest)
		if ok {
			metrics.PeerTaskReuseCount.Add(1)
			return r, attr, nil
		}
	}

	pt, err := ptm.newStreamTask(ctx, peerTaskRequest)
	if err != nil {
		return nil, nil, err
	}

	// FIXME when failed due to schedulerClient error, relocate schedulerClient and retry
	readCloser, attribute, err := pt.Start(ctx)
	return readCloser, attribute, err
}

func (ptm *peerTaskManager) Stop(ctx context.Context) error {
	// TODO
	return nil
}

func (ptm *peerTaskManager) PeerTaskDone(taskID string) {
	logger.Debugf("delete done task %s in running tasks", taskID)
	ptm.runningPeerTasks.Delete(taskID)
}

func (ptm *peerTaskManager) IsPeerTaskRunning(taskID string) bool {
	_, ok := ptm.runningPeerTasks.Load(taskID)
	return ok
}
