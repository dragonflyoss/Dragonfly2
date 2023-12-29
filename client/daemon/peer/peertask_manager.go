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

//go:generate mockgen -destination peertask_manager_mock.go -source peertask_manager.go -package peer

package peer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/go-http-utils/headers"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/status"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/daemon/metrics"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/util"
	"d7y.io/dragonfly/v2/pkg/idgen"
	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
)

// TaskManager processes all peer tasks request
type TaskManager interface {
	// StartFileTask starts a peer task to download a file
	// return a progress channel for request download progress
	// tiny stands task file is tiny and task is done
	StartFileTask(ctx context.Context, req *FileTaskRequest) (
		progress chan *FileTaskProgress, err error)
	// StartStreamTask starts a peer task with stream io
	StartStreamTask(ctx context.Context, req *StreamTaskRequest) (
		readCloser io.ReadCloser, attribute map[string]string, err error)
	// StartSeedTask starts a seed peer task
	StartSeedTask(ctx context.Context, req *SeedTaskRequest) (
		seedTaskResult *SeedTaskResponse, reuse bool, err error)

	Subscribe(request *commonv1.PieceTaskRequest) (*SubscribeResponse, bool)

	IsPeerTaskRunning(taskID string, peerID string) (Task, bool)

	// StatTask checks whether the given task exists in P2P network
	StatTask(ctx context.Context, taskID string) (*schedulerv1.Task, error)

	// AnnouncePeerTask announces peer task info to P2P network
	AnnouncePeerTask(ctx context.Context, meta storage.PeerTaskMetadata, url string, taskType commonv1.TaskType, urlMeta *commonv1.UrlMeta) error

	GetPieceManager() PieceManager

	// Stop stops the PeerTaskManager
	Stop(ctx context.Context) error
}

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

	UpdateSourceErrorStatus(st *status.Status)
}

type Logger interface {
	Log() *logger.SugaredLoggerOnWith
}

type TinyData struct {
	TaskID  string
	PeerID  string
	Content []byte
}

var tracer trace.Tracer

func init() {
	tracer = otel.Tracer("dfget-daemon")
}

type peerTaskManager struct {
	TaskManagerOption
	conductorLock    sync.Locker
	runningPeerTasks sync.Map
	trafficShaper    TrafficShaper
}

type TaskManagerOption struct {
	TaskOption
	SchedulerClient   schedulerclient.V1
	PerPeerRateLimit  rate.Limit
	TotalRateLimit    rate.Limit
	TrafficShaperType string
	// Multiplex indicates to reuse the data of completed peer tasks
	Multiplex bool
	// Prefetch indicates to prefetch the whole files of ranged requests
	Prefetch          bool
	GetPiecesMaxRetry int
	SplitRunningTasks bool
}

func NewPeerTaskManager(opt *TaskManagerOption) (TaskManager, error) {
	ptm := &peerTaskManager{
		TaskManagerOption: *opt,
		runningPeerTasks:  sync.Map{},
		conductorLock:     &sync.Mutex{},
		trafficShaper:     NewTrafficShaper(opt.TrafficShaperType, opt.TotalRateLimit, util.ComputePieceSize),
	}
	ptm.trafficShaper.Start()
	return ptm, nil
}

func (ptm *peerTaskManager) findPeerTaskConductor(key string) (*peerTaskConductor, bool) {
	pt, ok := ptm.runningPeerTasks.Load(key)
	if !ok {
		return nil, false
	}
	return pt.(*peerTaskConductor), true
}

func (ptm *peerTaskManager) getPeerTaskConductor(ctx context.Context,
	taskID string,
	request *schedulerv1.PeerTaskRequest,
	limit rate.Limit,
	parent *peerTaskConductor,
	rg *nethttp.Range,
	desiredLocation string,
	seed bool) (*peerTaskConductor, error) {
	var (
		ptc     *peerTaskConductor
		created bool
		err     error
	)

	if ptm.SplitRunningTasks {
		ptc, created, err = ptm.createSplitedPeerTaskConductor(
			ctx, taskID, request, limit, parent, rg, desiredLocation, seed)
	} else {
		ptc, created, err = ptm.getOrCreatePeerTaskConductor(
			ctx, taskID, request, limit, parent, rg, desiredLocation, seed)
	}

	if err != nil {
		return nil, err
	}

	if created {
		if err = ptc.start(); err != nil {
			return nil, err
		}
	}
	return ptc, err
}

// getOrCreatePeerTaskConductor will get or create a peerTaskConductor,
// if created, return (ptc, true, nil), otherwise return (ptc, false, nil)
func (ptm *peerTaskManager) getOrCreatePeerTaskConductor(
	ctx context.Context,
	taskID string,
	request *schedulerv1.PeerTaskRequest,
	limit rate.Limit,
	parent *peerTaskConductor,
	rg *nethttp.Range,
	desiredLocation string,
	seed bool) (*peerTaskConductor, bool, error) {
	if ptc, ok := ptm.findPeerTaskConductor(taskID); ok {
		logger.Debugf("peer task found: %s/%s", ptc.taskID, ptc.peerID)
		return ptc, false, nil
	}
	ptc := ptm.newPeerTaskConductor(ctx, request, limit, parent, rg, seed)

	ptm.conductorLock.Lock()
	// double check
	if p, ok := ptm.findPeerTaskConductor(taskID); ok {
		ptm.conductorLock.Unlock()
		logger.Debugf("peer task found: %s/%s", p.taskID, p.peerID)
		if seed && !p.seed && !p.needBackSource.Load() {
			p.Warnf("new seed request received, switch to back source, may be produced by multiple schedulers")
			p.markBackSource()
		}
		metrics.PeerTaskCacheHitCount.Add(1)
		return p, false, nil
	}
	ptm.runningPeerTasks.Store(taskID, ptc)
	ptm.conductorLock.Unlock()
	metrics.PeerTaskCount.Add(1)
	logger.Debugf("peer task created: %s/%s", ptc.taskID, ptc.peerID)

	err := ptc.initStorage(desiredLocation)
	if err != nil {
		ptc.Errorf("init storage error: %s", err)
		ptc.cancelNotRegisterred(commonv1.Code_ClientError, err.Error())
		return nil, false, err
	}
	return ptc, true, nil
}

func (ptm *peerTaskManager) createSplitedPeerTaskConductor(
	ctx context.Context,
	taskID string,
	request *schedulerv1.PeerTaskRequest,
	limit rate.Limit,
	parent *peerTaskConductor,
	rg *nethttp.Range,
	desiredLocation string,
	seed bool) (*peerTaskConductor, bool, error) {
	ptc := ptm.newPeerTaskConductor(ctx, request, limit, parent, rg, seed)

	ptm.runningPeerTasks.Store(taskID+"/"+ptc.peerID, ptc)
	metrics.PeerTaskCount.Add(1)
	logger.Debugf("standalone peer task created: %s/%s", ptc.taskID, ptc.peerID)

	err := ptc.initStorage(desiredLocation)
	if err != nil {
		ptc.Errorf("init storage error: %s", err)
		ptc.cancelNotRegisterred(commonv1.Code_ClientError, err.Error())
		return nil, false, err
	}
	return ptc, true, nil
}

func (ptm *peerTaskManager) enabledPrefetch(rg *nethttp.Range) bool {
	return ptm.Prefetch && rg != nil
}

func (ptm *peerTaskManager) prefetchParentTask(request *schedulerv1.PeerTaskRequest, desiredLocation string) *peerTaskConductor {
	req := &schedulerv1.PeerTaskRequest{
		Url:         request.Url,
		PeerId:      request.PeerId,
		PeerHost:    ptm.PeerHost,
		IsMigrating: request.IsMigrating,
		UrlMeta: &commonv1.UrlMeta{
			Digest: request.UrlMeta.Digest,
			Tag:    request.UrlMeta.Tag,
			Filter: request.UrlMeta.Filter,
			Header: map[string]string{},
		},
	}
	for k, v := range request.UrlMeta.Header {
		if k == headers.Range {
			continue
		}
		req.UrlMeta.Header[k] = v
	}
	taskID := idgen.TaskIDV1(req.Url, req.UrlMeta)
	req.PeerId = idgen.PeerIDV1(req.PeerHost.Ip)

	var limit = rate.Inf
	if ptm.PerPeerRateLimit > 0 {
		limit = ptm.PerPeerRateLimit
	}

	logger.Infof("prefetch peer task %s/%s", taskID, req.PeerId)
	prefetch, err := ptm.getPeerTaskConductor(context.Background(), taskID, req, limit, nil, nil, desiredLocation, false)
	if err != nil {
		logger.Errorf("prefetch peer task %s error: %s", taskID, err)
		return nil
	}

	if prefetch != nil && prefetch.peerID == req.PeerId {
		metrics.PrefetchTaskCount.Add(1)
	}
	return prefetch
}

func (ptm *peerTaskManager) StartFileTask(ctx context.Context, req *FileTaskRequest) (chan *FileTaskProgress, error) {
	if req.KeepOriginalOffset && !ptm.Prefetch {
		return nil, fmt.Errorf("please enable prefetch when use original offset feature")
	}
	if ptm.Multiplex {
		progress, ok := ptm.tryReuseFilePeerTask(ctx, req)
		if ok {
			metrics.PeerTaskCacheHitCount.Add(1)
			return progress, nil
		}
	}
	// TODO ensure scheduler is ok first
	var limit = rate.Inf
	if ptm.PerPeerRateLimit > 0 {
		limit = ptm.PerPeerRateLimit
	}
	if req.Limit > 0 {
		limit = rate.Limit(req.Limit)
	}
	ctx, pt, err := ptm.newFileTask(ctx, req, limit)
	if err != nil {
		return nil, err
	}

	// FIXME when failed due to SchedulerClient error, relocate SchedulerClient and retry
	progress, err := pt.Start(ctx)
	return progress, err
}

func (ptm *peerTaskManager) StartStreamTask(ctx context.Context, req *StreamTaskRequest) (io.ReadCloser, map[string]string, error) {
	peerTaskRequest := &schedulerv1.PeerTaskRequest{
		Url:         req.URL,
		UrlMeta:     req.URLMeta,
		PeerId:      req.PeerID,
		PeerHost:    ptm.PeerHost,
		IsMigrating: false,
	}

	taskID := idgen.TaskIDV1(req.URL, req.URLMeta)
	if ptm.Multiplex {
		// try breakpoint resume for task has range header
		if req.Range != nil && !ptm.SplitRunningTasks {
			// find running parent task
			parentTaskID := idgen.ParentTaskIDV1(req.URL, req.URLMeta)
			parentTask, ok := ptm.findPeerTaskConductor(parentTaskID)
			if ok && parentTask.GetContentLength() > 0 {
				// only allow resume for range from breakpoint to end
				if req.Range.Start+req.Range.Length == parentTask.GetContentLength() {
					pt := ptm.newResumeStreamTask(ctx, parentTask, req.Range)
					return pt.Start(ctx)
				}
			}
		}

		// reuse by completed task
		r, attr, ok := ptm.tryReuseStreamPeerTask(ctx, taskID, req)
		if ok {
			metrics.PeerTaskCacheHitCount.Add(1)
			return r, attr, nil
		}
	}

	pt, err := ptm.newStreamTask(ctx, taskID, peerTaskRequest, req.Range)
	if err != nil {
		return nil, nil, err
	}

	// FIXME when failed due to SchedulerClient error, relocate SchedulerClient and retry
	readCloser, attribute, err := pt.Start(ctx)
	return readCloser, attribute, err
}

func (ptm *peerTaskManager) StartSeedTask(ctx context.Context, req *SeedTaskRequest) (response *SeedTaskResponse, reuse bool, err error) {
	response, ok := ptm.tryReuseSeedPeerTask(ctx, req)
	if ok {
		metrics.PeerTaskCacheHitCount.Add(1)
		return response, true, nil
	}

	var limit = rate.Inf
	if ptm.PerPeerRateLimit > 0 {
		limit = ptm.PerPeerRateLimit
	}
	if req.Limit > 0 {
		limit = rate.Limit(req.Limit)
	}

	response, err = ptm.newSeedTask(ctx, req, limit)
	if err != nil {
		return nil, false, err
	}

	return response, false, nil
}

type SubscribeResponse struct {
	Storage          storage.TaskStorageDriver
	PieceInfoChannel chan *PieceInfo
	Success          chan struct{}
	Fail             chan struct{}
	FailReason       func() error
}

func (ptm *peerTaskManager) getRunningTaskKey(taskID, peerID string) string {
	if ptm.SplitRunningTasks {
		return taskID + "/" + peerID
	}
	return taskID
}

func (ptm *peerTaskManager) Subscribe(request *commonv1.PieceTaskRequest) (*SubscribeResponse, bool) {
	ptc, ok := ptm.findPeerTaskConductor(ptm.getRunningTaskKey(request.TaskId, request.DstPid))
	if !ok {
		return nil, false
	}

	result := &SubscribeResponse{
		Storage:          ptc.storage,
		PieceInfoChannel: ptc.broker.Subscribe(),
		Success:          ptc.successCh,
		Fail:             ptc.failCh,
		FailReason:       ptc.getFailedError,
	}
	return result, true
}

func (ptm *peerTaskManager) Stop(ctx context.Context) error {
	// TODO
	if ptm.trafficShaper != nil {
		ptm.trafficShaper.Stop()
	}
	return nil
}

func (ptm *peerTaskManager) PeerTaskDone(taskID, peerID string) {
	key := ptm.getRunningTaskKey(taskID, peerID)
	logger.Debugf("delete done task %s in running tasks", key)
	ptm.runningPeerTasks.Delete(key)
	if ptm.trafficShaper != nil {
		ptm.trafficShaper.RemoveTask(key)
	}
}

func (ptm *peerTaskManager) IsPeerTaskRunning(taskID, peerID string) (Task, bool) {
	ptc, ok := ptm.runningPeerTasks.Load(ptm.getRunningTaskKey(taskID, peerID))
	if ok {
		return ptc.(*peerTaskConductor), ok
	}
	return nil, ok
}

func (ptm *peerTaskManager) StatTask(ctx context.Context, taskID string) (*schedulerv1.Task, error) {
	req := &schedulerv1.StatTaskRequest{
		TaskId: taskID,
	}

	return ptm.SchedulerClient.StatTask(ctx, req)
}

func (ptm *peerTaskManager) GetPieceManager() PieceManager {
	return ptm.PieceManager
}

func (ptm *peerTaskManager) AnnouncePeerTask(ctx context.Context, meta storage.PeerTaskMetadata, url string, taskType commonv1.TaskType, urlMeta *commonv1.UrlMeta) error {
	// Check if the given task is completed in local StorageManager.
	if ptm.StorageManager.FindCompletedTask(meta.TaskID) == nil {
		return errors.New("task not found in local storage")
	}

	// Prepare AnnounceTaskRequest.
	totalPieces, err := ptm.StorageManager.GetTotalPieces(ctx, &meta)
	if err != nil {
		return err
	}

	piecePacket, err := ptm.StorageManager.GetPieces(ctx, &commonv1.PieceTaskRequest{
		TaskId:   meta.TaskID,
		DstPid:   meta.PeerID,
		StartNum: 0,
		Limit:    uint32(totalPieces),
	})
	if err != nil {
		return err
	}
	piecePacket.DstAddr = fmt.Sprintf("%s:%d", ptm.PeerHost.Ip, ptm.PeerHost.DownPort)

	// Announce peer task to scheduler
	if err := ptm.SchedulerClient.AnnounceTask(ctx, &schedulerv1.AnnounceTaskRequest{
		TaskId:      meta.TaskID,
		TaskType:    taskType,
		Url:         url,
		UrlMeta:     urlMeta,
		PeerHost:    ptm.PeerHost,
		PiecePacket: piecePacket,
	}); err != nil {
		return err
	}

	return nil
}
