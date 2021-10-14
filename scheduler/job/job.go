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
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/idgen"
	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"
)

const (
	interval = time.Second
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
		logger.Errorf("request url \"%s\" is invalid, error: %v", request.URL, err)
		return errors.Errorf("invalid url: %s", request.URL)
	}

	meta := &base.UrlMeta{
		Header: request.Headers,
		Tag:    request.Tag,
		Filter: request.Filter,
		Digest: request.Digest,
	}

	// Generate range
	if request.Headers != nil {
		if rg := request.Headers["Range"]; len(rg) > 0 {
			meta.Range = rg
		}
	}

	taskID := idgen.TaskID(request.URL, meta)
	logger.Infof("ready to preheat \"%s\", taskID = %s", request.URL, taskID)

	task := supervisor.NewTask(taskID, request.URL, meta)
	task = t.service.GetOrCreateTask(t.ctx, task)
	return getPreheatResult(task)
}

//TODO(@zzy987) check better ways to get result
func getPreheatResult(task *supervisor.Task) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			switch task.GetStatus() {
			case supervisor.TaskStatusRunning, supervisor.TaskStatusSeeding:
			case supervisor.TaskStatusSuccess:
				logger.Info("preheat task %v successed", task.ID)
				return nil
			case supervisor.TaskStatusWaiting:
				// New task for the first time
				if task.CreateAt.Load() == task.LastTriggerAt.Load() {
					break
				}
				fallthrough
			default:
				logger.Errorf("preheat task %v failed", task.ID)
				return errors.Errorf("preheat task fail")
			}
		}
	}
}
