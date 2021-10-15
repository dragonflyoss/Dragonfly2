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

package job

import (
	"context"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/idgen"
	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core"
	"github.com/go-playground/validator/v10"
)

type Job interface {
	Serve() error
	Stop()
}

type job struct {
	globalJob    *internaljob.Job
	schedulerJob *internaljob.Job
	localJob     *internaljob.Job
	ctx          context.Context
	service      *core.SchedulerService
	cfg          *config.JobConfig
}

func New(ctx context.Context, cfg *config.JobConfig, clusterID uint, hostname string, service *core.SchedulerService) (Job, error) {
	redisConfig := &internaljob.Config{
		Host:      cfg.Redis.Host,
		Port:      cfg.Redis.Port,
		Password:  cfg.Redis.Password,
		BrokerDB:  cfg.Redis.BrokerDB,
		BackendDB: cfg.Redis.BackendDB,
	}

	globalJob, err := internaljob.New(redisConfig, internaljob.GlobalQueue)
	if err != nil {
		logger.Errorf("create global job queue error: %v", err)
		return nil, err
	}

	schedulerJob, err := internaljob.New(redisConfig, internaljob.SchedulersQueue)
	if err != nil {
		logger.Errorf("create scheduler job queue error: %v", err)
		return nil, err
	}

	localQueue, err := internaljob.GetSchedulerQueue(clusterID, hostname)
	if err != nil {
		logger.Errorf("get local job queue name error: %v", err)
		return nil, err
	}

	localJob, err := internaljob.New(redisConfig, localQueue)
	if err != nil {
		logger.Errorf("create local job queue error: %v", err)
		return nil, err
	}

	t := &job{
		globalJob:    globalJob,
		schedulerJob: schedulerJob,
		localJob:     localJob,
		ctx:          ctx,
		service:      service,
		cfg:          cfg,
	}

	namedJobFuncs := map[string]interface{}{
		internaljob.PreheatJob: t.preheat,
	}

	if err := localJob.RegisterJob(namedJobFuncs); err != nil {
		logger.Errorf("register preheat job to local queue error: %v", err)
		return nil, err
	}

	return t, nil
}

func (t *job) Serve() error {
	go func() {
		logger.Infof("ready to launch %d worker(s) on global queue", t.cfg.GlobalWorkerNum)
		if err := t.globalJob.LaunchWorker("global_worker", int(t.cfg.GlobalWorkerNum)); err != nil {
			logger.Fatalf("global queue worker error: %v", err)
		}
	}()

	go func() {
		logger.Infof("ready to launch %d worker(s) on scheduler queue", t.cfg.SchedulerWorkerNum)
		if err := t.schedulerJob.LaunchWorker("scheduler_worker", int(t.cfg.SchedulerWorkerNum)); err != nil {
			logger.Fatalf("scheduler queue worker error: %v", err)
		}
	}()

	logger.Infof("ready to launch %d worker(s) on local queue", t.cfg.LocalWorkerNum)
	return t.localJob.LaunchWorker("local_worker", int(t.cfg.LocalWorkerNum))
}

func (t *job) Stop() {
	t.globalJob.Worker.Quit()
	t.schedulerJob.Worker.Quit()
	t.localJob.Worker.Quit()
}

func (t *job) preheat(req string) error {
	request := &internaljob.PreheatRequest{}
	if err := internaljob.UnmarshalRequest(req, request); err != nil {
		logger.Errorf("unmarshal request err: %v, request body: %s", err, req)
		return err
	}

	if err := validator.New().Struct(request); err != nil {
		logger.Errorf("url %s validate failed: %v", request.URL, err)
		return err
	}

	// Generate meta
	meta := &base.UrlMeta{
		Header: request.Headers,
		Tag:    request.Tag,
		Filter: request.Filter,
		Digest: request.Digest,
	}

	if request.Headers != nil {
		if rg := request.Headers["Range"]; len(rg) > 0 {
			meta.Range = rg
		}
	}

	// Generate taskID
	taskID := idgen.TaskID(request.URL, meta)

	// Trigger CDN download seeds
	plogger := logger.WithTaskIDAndURL(taskID, request.URL)
	plogger.Info("ready to preheat")
	stream, err := t.service.CDN.GetClient().ObtainSeeds(t.ctx, &cdnsystem.SeedRequest{
		TaskId:  taskID,
		Url:     request.URL,
		UrlMeta: meta,
	})
	if err != nil {
		plogger.Errorf("preheat failed: %v", err)
		return err
	}

	for {
		piece, err := stream.Recv()
		if err != nil {
			plogger.Errorf("preheat recive piece failed: %v", err)
			return err
		}

		if piece.Done == true {
			plogger.Info("preheat successed")
			return nil
		}
	}
}
