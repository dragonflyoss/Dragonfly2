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

	namedJobFuncs := map[string]interface{}{
		internaljob.PreheatJob: t.preheat,
	}

	if err := localJob.RegisterJob(namedJobFuncs); err != nil {
		logger.Errorf("register preheat job to local queue error: %s", err.Error())
		return nil, err
	}

	return t, nil
}

func (j *job) Serve() {
	go func() {
		logger.Infof("ready to launch %d worker(s) on global queue", j.config.Job.GlobalWorkerNum)
		if err := j.globalJob.LaunchWorker("global_worker", int(j.config.Job.GlobalWorkerNum)); err != nil {
			logger.Fatalf("global queue worker error: %s", err.Error())
		}
	}()

	go func() {
		logger.Infof("ready to launch %d worker(s) on scheduler queue", j.config.Job.SchedulerWorkerNum)
		if err := j.schedulerJob.LaunchWorker("scheduler_worker", int(j.config.Job.SchedulerWorkerNum)); err != nil {
			logger.Fatalf("scheduler queue worker error: %s", err.Error())
		}
	}()

	go func() {
		logger.Infof("ready to launch %d worker(s) on local queue", j.config.Job.LocalWorkerNum)
		if err := j.localJob.LaunchWorker("local_worker", int(j.config.Job.LocalWorkerNum)); err != nil {
			logger.Fatalf("scheduler queue worker error: %s", err.Error())
		}
	}()
}

func (j *job) Stop() {
	j.globalJob.Worker.Quit()
	j.schedulerJob.Worker.Quit()
	j.localJob.Worker.Quit()
}

func (j *job) preheat(ctx context.Context, req string) error {
	if !j.config.SeedPeer.Enable {
		return errors.New("scheduler has disabled seed peer")
	}

	request := &internaljob.PreheatRequest{}
	if err := internaljob.UnmarshalRequest(req, request); err != nil {
		logger.Errorf("unmarshal request err: %s, request body: %s", err.Error(), req)
		return err
	}

	if err := validator.New().Struct(request); err != nil {
		logger.Errorf("url %s validate failed: %s", request.URL, err.Error())
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
			// Range in dragonfly is without "bytes=".
			urlMeta.Range = strings.TrimLeft(r, "bytes=")
		}
	}

	taskID := idgen.TaskID(request.URL, urlMeta)

	// Trigger seed peer download seeds.
	log := logger.WithTaskIDAndURL(taskID, request.URL)
	log.Infof("preheat %s headers: %#v, tag: %s, range: %s, filter: %s, digest: %s",
		request.URL, urlMeta.Header, urlMeta.Tag, urlMeta.Range, urlMeta.Filter, urlMeta.Digest)
	stream, err := j.resource.SeedPeer().Client().ObtainSeeds(ctx, &cdnsystem.SeedRequest{
		TaskId:  taskID,
		Url:     request.URL,
		UrlMeta: urlMeta,
	})
	if err != nil {
		log.Errorf("preheat failed: %s", err.Error())
		return err
	}

	for {
		piece, err := stream.Recv()
		if err != nil {
			log.Errorf("preheat recive piece failed: %s", err.Error())
			return err
		}

		if piece.Done == true {
			log.Info("preheat succeeded")
			return nil
		}
	}
}
