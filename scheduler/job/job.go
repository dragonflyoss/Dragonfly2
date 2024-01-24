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
	"strings"
	"time"

	"github.com/RichardKnop/machinery/v1"
	"github.com/go-http-utils/headers"
	"github.com/go-playground/validator/v10"

	cdnsystemv1 "d7y.io/api/v2/pkg/apis/cdnsystem/v1"
	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

const (
	// preheatTimeout is timeout of preheating.
	preheatTimeout = 20 * time.Minute
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
		internaljob.PreheatJob:   t.preheat,
		internaljob.SyncPeersJob: t.syncPeers,
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

// preheat is a job to preheat.
func (j *job) preheat(ctx context.Context, req string) error {
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

	preheat := &internaljob.PreheatRequest{}
	if err := internaljob.UnmarshalRequest(req, preheat); err != nil {
		logger.Errorf("unmarshal request err: %s, request body: %s", err.Error(), req)
		return err
	}

	if err := validator.New().Struct(preheat); err != nil {
		logger.Errorf("preheat %s validate failed: %s", preheat.URL, err.Error())
		return err
	}

	urlMeta := &commonv1.UrlMeta{
		Digest:      preheat.Digest,
		Tag:         preheat.Tag,
		Filter:      preheat.FilteredQueryParams,
		Header:      preheat.Headers,
		Application: preheat.Application,
		Priority:    commonv1.Priority(preheat.Priority),
	}
	if preheat.Headers != nil {
		if r, ok := preheat.Headers[headers.Range]; ok {
			// Range in dragonfly is without "bytes=".
			urlMeta.Range = strings.TrimPrefix(r, http.RangePrefix)
		}
	}

	// Trigger seed peer download seeds.
	taskID := idgen.TaskIDV1(preheat.URL, urlMeta)
	log := logger.WithTask(taskID, preheat.URL)
	log.Infof("preheat %s tag: %s, range: %s, filtered query params: %s, digest: %s",
		preheat.URL, urlMeta.Tag, urlMeta.Range, urlMeta.Filter, urlMeta.Digest)
	log.Debugf("preheat %s headers: %#v", preheat.URL, urlMeta.Header)

	stream, err := j.resource.SeedPeer().Client().ObtainSeeds(ctx, &cdnsystemv1.SeedRequest{
		TaskId:  taskID,
		Url:     preheat.URL,
		UrlMeta: urlMeta,
	})
	if err != nil {
		log.Errorf("preheat %s failed: %s", preheat.URL, err.Error())
		return err
	}

	for {
		piece, err := stream.Recv()
		if err != nil {
			log.Errorf("preheat %s recive piece failed: %s", preheat.URL, err.Error())
			return err
		}

		if piece.Done == true {
			log.Infof("preheat %s succeeded", preheat.URL)
			return nil
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
