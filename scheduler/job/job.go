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

//go:generate mockgen -destination mocks/job_mock.go -source job.go -package mocks

package job

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/RichardKnop/machinery/v1"
	"github.com/go-playground/validator/v10"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cdnsystemv1 "d7y.io/api/v2/pkg/apis/cdnsystem/v1"
	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
	dfdaemonv2 "d7y.io/api/v2/pkg/apis/dfdaemon/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/pkg/idgen"
	dfdaemonclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

const (
	// preheatTimeout is timeout of preheating.
	preheatTimeout = 20 * time.Minute

	// deleteTaskTimeout is timeout of deleting task.
	deleteTaskTimeout = 20 * time.Minute
)

// Job is an interface for job.
type Job interface {
	Serve()
}

// job is an implementation of Job.
type job struct {
	globalJob    *internaljob.Job
	schedulerJob *internaljob.Job
	localJob     *internaljob.Job
	resource     resource.Resource
	config       *config.Config
}

// New creates a new Job.
func New(cfg *config.Config, resource resource.Resource) (Job, error) {
	redisConfig := &internaljob.Config{
		Addrs:      cfg.Database.Redis.Addrs,
		MasterName: cfg.Database.Redis.MasterName,
		Username:   cfg.Database.Redis.Username,
		Password:   cfg.Database.Redis.Password,
		BrokerDB:   cfg.Database.Redis.BrokerDB,
		BackendDB:  cfg.Database.Redis.BackendDB,
	}

	globalJob, err := internaljob.New(redisConfig, internaljob.GlobalQueue)
	if err != nil {
		logger.Errorf("create global job queue error: %s", err.Error())
		return nil, err
	}
	logger.Infof("create global job queue: %v", globalJob)

	schedulerJob, err := internaljob.New(redisConfig, internaljob.SchedulersQueue)
	if err != nil {
		logger.Errorf("create scheduler job queue error: %s", err.Error())
		return nil, err
	}
	logger.Infof("create scheduler job queue: %v", schedulerJob)

	localQueue, err := internaljob.GetSchedulerQueue(cfg.Manager.SchedulerClusterID, cfg.Server.Host)
	if err != nil {
		logger.Errorf("get local job queue name error: %s", err.Error())
		return nil, err
	}

	localJob, err := internaljob.New(redisConfig, localQueue)
	if err != nil {
		logger.Errorf("create local job queue error: %s", err.Error())
		return nil, err
	}
	logger.Infof("create local job queue: %v", localQueue)

	t := &job{
		globalJob:    globalJob,
		schedulerJob: schedulerJob,
		localJob:     localJob,
		resource:     resource,
		config:       cfg,
	}

	namedJobFuncs := map[string]any{
		internaljob.PreheatJob:    t.preheat,
		internaljob.SyncPeersJob:  t.syncPeers,
		internaljob.GetTaskJob:    t.getTask,
		internaljob.DeleteTaskJob: t.deleteTask,
	}

	if err := localJob.RegisterJob(namedJobFuncs); err != nil {
		logger.Errorf("register preheat job to local queue error: %s", err.Error())
		return nil, err
	}

	return t, nil
}

// Serve starts the job.
func (j *job) Serve() {
	go func() {
		logger.Infof("ready to launch %d worker(s) on global queue", j.config.Job.GlobalWorkerNum)
		if err := j.globalJob.LaunchWorker("global_worker", int(j.config.Job.GlobalWorkerNum)); err != nil {
			if !errors.Is(err, machinery.ErrWorkerQuitGracefully) {
				logger.Fatalf("global queue worker error: %s", err.Error())
			}
		}
	}()

	go func() {
		logger.Infof("ready to launch %d worker(s) on scheduler queue", j.config.Job.SchedulerWorkerNum)
		if err := j.schedulerJob.LaunchWorker("scheduler_worker", int(j.config.Job.SchedulerWorkerNum)); err != nil {
			if !errors.Is(err, machinery.ErrWorkerQuitGracefully) {
				logger.Fatalf("scheduler queue worker error: %s", err.Error())
			}
		}
	}()

	go func() {
		logger.Infof("ready to launch %d worker(s) on local queue", j.config.Job.LocalWorkerNum)
		if err := j.localJob.LaunchWorker("local_worker", int(j.config.Job.LocalWorkerNum)); err != nil {
			if !errors.Is(err, machinery.ErrWorkerQuitGracefully) {
				logger.Fatalf("scheduler queue worker error: %s", err.Error())
			}
		}
	}()
}

// preheat is a job to preheat, it is not supported to preheat
// with range requests.
func (j *job) preheat(ctx context.Context, data string) error {
	ctx, cancel := context.WithTimeout(ctx, preheatTimeout)
	defer cancel()

	// If seed peer is disabled, return error.
	if !j.config.SeedPeer.Enable {
		return fmt.Errorf("cluster %d scheduler %s has disabled seed peer", j.config.Manager.SchedulerClusterID, j.config.Server.AdvertiseIP)
	}

	// If scheduler has no available seed peer, return error.
	if len(j.resource.SeedPeer().Client().Addrs()) == 0 {
		return fmt.Errorf("cluster %d scheduler %s has no available seed peer", j.config.Manager.SchedulerClusterID, j.config.Server.AdvertiseIP)
	}

	req := &internaljob.PreheatRequest{}
	if err := internaljob.UnmarshalRequest(data, req); err != nil {
		logger.Errorf("unmarshal request err: %s, request body: %s", err.Error(), data)
		return err
	}

	if err := validator.New().Struct(req); err != nil {
		logger.Errorf("preheat %s validate failed: %s", req.URL, err.Error())
		return err
	}

	// Preheat by v2 grpc protocol. If seed peer does not support
	// v2 protocol, preheat by v1 grpc protocol.
	if err := j.preheatV2(ctx, req); err != nil {
		logger.Errorf("preheat %s failed: %s", req.URL, err.Error())

		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.Unimplemented {
				if err := j.preheatV1(ctx, req); err != nil {
					return err
				}

				return nil
			}
		}

		return err
	}

	return nil
}

// preheatV1 preheats job by v1 grpc protocol.
func (j *job) preheatV1(ctx context.Context, req *internaljob.PreheatRequest) error {
	urlMeta := &commonv1.UrlMeta{
		Digest:      req.Digest,
		Tag:         req.Tag,
		Filter:      req.FilteredQueryParams,
		Header:      req.Headers,
		Application: req.Application,
		Priority:    commonv1.Priority(req.Priority),
	}

	// Trigger seed peer download seeds.
	taskID := idgen.TaskIDV1(req.URL, urlMeta)
	log := logger.WithTask(taskID, req.URL)
	log.Infof("preheat(v1) %s tag: %s, filtered query params: %s, digest: %s, headers: %#v",
		req.URL, urlMeta.Tag, urlMeta.Filter, urlMeta.Digest, urlMeta.Header)

	stream, err := j.resource.SeedPeer().Client().ObtainSeeds(ctx, &cdnsystemv1.SeedRequest{
		TaskId:  taskID,
		Url:     req.URL,
		UrlMeta: urlMeta,
	})
	if err != nil {
		log.Errorf("preheat(v1) %s failed: %s", req.URL, err.Error())
		return err
	}

	for {
		piece, err := stream.Recv()
		if err != nil {
			log.Errorf("preheat(v1) %s recive piece failed: %s", req.URL, err.Error())
			return err
		}

		if piece.Done == true {
			log.Infof("preheat(v1) %s succeeded", req.URL)
			return nil
		}
	}
}

// preheatV2 preheats job by v2 grpc protocol.
func (j *job) preheatV2(ctx context.Context, req *internaljob.PreheatRequest) error {
	filteredQueryParams := strings.Split(req.FilteredQueryParams, idgen.FilteredQueryParamsSeparator)
	taskID := idgen.TaskIDV2(req.URL, req.Digest, req.Tag, req.Application, filteredQueryParams)

	log := logger.WithTask(taskID, req.URL)
	log.Infof("preheat(v2) %s tag: %s, filtered query params: %s, digest: %s, headers: %#v",
		req.URL, req.Tag, req.FilteredQueryParams, req.Digest, req.Headers)

	stream, err := j.resource.SeedPeer().Client().DownloadTask(ctx, taskID, &dfdaemonv2.DownloadTaskRequest{
		Download: &commonv2.Download{
			Url:                 req.URL,
			Digest:              &req.Digest,
			Type:                commonv2.TaskType_DFDAEMON,
			Tag:                 &req.Tag,
			Application:         &req.Application,
			Priority:            commonv2.Priority(req.Priority),
			FilteredQueryParams: filteredQueryParams,
			RequestHeader:       req.Headers,
		}})
	if err != nil {
		logger.Errorf("preheat(v2) %s failed: %s", req.URL, err.Error())
		return err
	}

	// Wait for the download task to complete.
	for {
		_, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Infof("preheat(v2) %s succeeded", req.URL)
				return nil
			}

			log.Errorf("preheat(v2) %s recive piece failed: %s", req.URL, err.Error())
			return err
		}
	}
}

// syncPeers is a job to sync peers.
func (j *job) syncPeers() (string, error) {
	var hosts []*resource.Host
	j.resource.HostManager().Range(func(key, value any) bool {
		host, ok := value.(*resource.Host)
		if !ok {
			logger.Errorf("invalid host %v %v", key, value)
			return true
		}

		hosts = append(hosts, host)
		return true
	})

	return internaljob.MarshalResponse(hosts)
}

// getTask is a job to get task.
func (j *job) getTask(ctx context.Context, data string) (string, error) {
	req := &internaljob.GetTaskRequest{}
	if err := internaljob.UnmarshalRequest(data, req); err != nil {
		logger.Errorf("unmarshal request err: %s, request body: %s", err.Error(), data)
		return "", err
	}

	if err := validator.New().Struct(req); err != nil {
		logger.Errorf("getTask %s validate failed: %s", req.TaskID, err.Error())
		return "", err
	}

	task, ok := j.resource.TaskManager().Load(req.TaskID)
	if !ok {
		logger.Errorf("task %s not found", req.TaskID)
		return "", fmt.Errorf("task %s not found", req.TaskID)
	}

	return internaljob.MarshalResponse(&internaljob.GetTaskResponse{
		Peers: task.LoadPeers(),
	})
}

// deleteTask is a job to delete task.
func (j *job) deleteTask(ctx context.Context, data string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, deleteTaskTimeout)
	defer cancel()

	req := &internaljob.DeleteTaskRequest{}
	if err := internaljob.UnmarshalRequest(data, req); err != nil {
		logger.Errorf("unmarshal request err: %s, request body: %s", err.Error(), data)
		return "", err
	}

	if err := validator.New().Struct(req); err != nil {
		logger.Errorf("deleteTask %s validate failed: %s", req.TaskID, err.Error())
		return "", err
	}

	task, ok := j.resource.TaskManager().Load(req.TaskID)
	if !ok {
		logger.Errorf("task %s not found", req.TaskID)
		return "", fmt.Errorf("task %s not found", req.TaskID)
	}

	successPeers := []*internaljob.DeletePeerResponse{}
	failurePeers := []*internaljob.DeletePeerResponse{}

	finishedPeers := task.LoadFinishedPeers()
	for _, finishedPeer := range finishedPeers {
		log := logger.WithPeer(finishedPeer.Host.ID, finishedPeer.Task.ID, finishedPeer.ID)

		addr := fmt.Sprintf("%s:%d", finishedPeer.Host.IP, finishedPeer.Host.Port)
		dfdaemonClient, err := dfdaemonclient.GetV2ByAddr(ctx, addr)
		if err != nil {
			log.Errorf("get client from %s failed: %s", addr, err.Error())
			failurePeers = append(failurePeers, &internaljob.DeletePeerResponse{
				Peer:        finishedPeer,
				Description: err.Error(),
			})

			continue
		}

		if err = dfdaemonClient.DeleteTask(ctx, &dfdaemonv2.DeleteTaskRequest{
			TaskId: req.TaskID,
		}); err != nil {
			logger.Errorf("delete task failed: %s", err.Error())
			failurePeers = append(failurePeers, &internaljob.DeletePeerResponse{
				Peer:        finishedPeer,
				Description: err.Error(),
			})

			continue
		}

		successPeers = append(successPeers, &internaljob.DeletePeerResponse{
			Peer:        finishedPeer,
			Description: "",
		})
	}

	return internaljob.MarshalResponse(&internaljob.DeleteTaskResponse{
		FailurePeers: failurePeers,
		SuccessPeers: successPeers,
	})
}
