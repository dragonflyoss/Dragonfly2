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
	"strings"

	"github.com/go-http-utils/headers"
	"github.com/go-playground/validator/v10"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

type Job interface {
	Serve()
	Stop()
}

type job struct {
	globalJob    *internaljob.Job
	schedulerJob *internaljob.Job
	localJob     *internaljob.Job
	resource     resource.Resource
	config       *config.Config
}

func New(cfg *config.Config, resource resource.Resource) (Job, error) {
	redisConfig := &internaljob.Config{
		Host:      cfg.Job.Redis.Host,
		Port:      cfg.Job.Redis.Port,
		Password:  cfg.Job.Redis.Password,
		BrokerDB:  cfg.Job.Redis.BrokerDB,
		BackendDB: cfg.Job.Redis.BackendDB,
	}

	globalJob, err := internaljob.New(redisConfig, internaljob.GlobalQueue)
	if err != nil {
		logger.Errorf("create global job queue error: %v", err)
		return nil, err
	}
	logger.Infof("create global job queue: %v", globalJob)

	schedulerJob, err := internaljob.New(redisConfig, internaljob.SchedulersQueue)
	if err != nil {
		logger.Errorf("create scheduler job queue error: %v", err)
		return nil, err
	}
	logger.Infof("create scheduler job queue: %v", schedulerJob)

	localQueue, err := internaljob.GetSchedulerQueue(cfg.Manager.SchedulerClusterID, cfg.Server.Host)
	if err != nil {
		logger.Errorf("get local job queue name error: %v", err)
		return nil, err
	}

	localJob, err := internaljob.New(redisConfig, localQueue)
	if err != nil {
		logger.Errorf("create local job queue error: %v", err)
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

	namedJobFuncs := map[string]interface{}{
		internaljob.PreheatJob: t.preheat,
	}

	if err := localJob.RegisterJob(namedJobFuncs); err != nil {
		logger.Errorf("register preheat job to local queue error: %v", err)
		return nil, err
	}

	return t, nil
}

func (t *job) Serve() {
	go func() {
		logger.Infof("ready to launch %d worker(s) on global queue", t.config.Job.GlobalWorkerNum)
		if err := t.globalJob.LaunchWorker("global_worker", int(t.config.Job.GlobalWorkerNum)); err != nil {
			logger.Fatalf("global queue worker error: %v", err)
		}
	}()

	go func() {
		logger.Infof("ready to launch %d worker(s) on scheduler queue", t.config.Job.SchedulerWorkerNum)
		if err := t.schedulerJob.LaunchWorker("scheduler_worker", int(t.config.Job.SchedulerWorkerNum)); err != nil {
			logger.Fatalf("scheduler queue worker error: %v", err)
		}
	}()

	go func() {
		logger.Infof("ready to launch %d worker(s) on local queue", t.config.Job.LocalWorkerNum)
		if err := t.localJob.LaunchWorker("local_worker", int(t.config.Job.LocalWorkerNum)); err != nil {
			logger.Fatalf("scheduler queue worker error: %v", err)
		}
	}()
}

func (t *job) Stop() {
	t.globalJob.Worker.Quit()
	t.schedulerJob.Worker.Quit()
	t.localJob.Worker.Quit()
}

func (t *job) preheat(ctx context.Context, req string) error {
	request := &internaljob.PreheatRequest{}
	if err := internaljob.UnmarshalRequest(req, request); err != nil {
		logger.Errorf("unmarshal request err: %v, request body: %s", err, req)
		return err
	}

	if err := validator.New().Struct(request); err != nil {
		logger.Errorf("url %s validate failed: %v", request.URL, err)
		return err
	}

	urlMeta := &base.UrlMeta{
		Header: request.Headers,
		Tag:    request.Tag,
		Filter: request.Filter,
		Digest: request.Digest,
	}
	if request.Headers != nil {
		if r, ok := request.Headers[headers.Range]; ok {
			// Range in dragonfly is without "bytes="
			urlMeta.Range = strings.TrimLeft(r, "bytes=")
		}
	}

	taskID := idgen.TaskID(request.URL, urlMeta)

	// Trigger CDN download seeds
	log := logger.WithTaskIDAndURL(taskID, request.URL)
	log.Infof("preheat %s headers: %#v, tag: %s, range: %s, filter: %s, digest: %s",
		request.URL, urlMeta.Header, urlMeta.Tag, urlMeta.Range, urlMeta.Filter, urlMeta.Digest)
	stream, err := t.resource.CDN().Client().ObtainSeeds(ctx, &cdnsystem.SeedRequest{
		TaskId:  taskID,
		Url:     request.URL,
		UrlMeta: urlMeta,
	})
	if err != nil {
		log.Errorf("preheat failed: %v", err)
		return err
	}

	for {
		piece, err := stream.Recv()
		if err != nil {
			log.Errorf("preheat recive piece failed: %v", err)
			return err
		}

		if piece.Done == true {
			log.Info("preheat succeeded")
			return nil
		}
	}
}
