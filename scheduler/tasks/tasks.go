package tasks

import (
	"context"
	"time"

	"d7y.io/dragonfly/v2/scheduler/supervise"
	"github.com/go-playground/validator/v10"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/idgen"
	internaltasks "d7y.io/dragonfly/v2/internal/tasks"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	interval = time.Second
)

type Tasks interface {
	Serve() error
	Stop()
}

type tasks struct {
	globalTasks    *internaltasks.Tasks
	schedulerTasks *internaltasks.Tasks
	localTasks     *internaltasks.Tasks
	ctx            context.Context
	service        *core.SchedulerService
	cfg            *config.TaskConfig
}

func New(ctx context.Context, cfg *config.TaskConfig, hostname string, service *core.SchedulerService) (Tasks, error) {
	redisConfig := &internaltasks.Config{
		Host:      cfg.Redis.Host,
		Port:      cfg.Redis.Port,
		Password:  cfg.Redis.Password,
		BrokerDB:  cfg.Redis.BrokerDB,
		BackendDB: cfg.Redis.BackendDB,
	}

	globalTask, err := internaltasks.New(redisConfig, internaltasks.GlobalQueue)
	if err != nil {
		logger.Errorf("create global tasks queue error: %v", err)
		return nil, err
	}

	schedulerTask, err := internaltasks.New(redisConfig, internaltasks.SchedulersQueue)
	if err != nil {
		logger.Errorf("create scheduler tasks queue error: %v", err)
		return nil, err
	}

	localQueue, err := internaltasks.GetSchedulerQueue(hostname)
	if err != nil {
		logger.Errorf("get local tasks queue name error: %v", err)
		return nil, err
	}

	localTask, err := internaltasks.New(redisConfig, localQueue)
	if err != nil {
		logger.Errorf("create local tasks queue error: %v", err)
		return nil, err
	}

	t := &tasks{
		globalTasks:    globalTask,
		schedulerTasks: schedulerTask,
		localTasks:     localTask,
		ctx:            ctx,
		service:        service,
		cfg:            cfg,
	}

	namedTaskFuncs := map[string]interface{}{
		internaltasks.PreheatTask: t.preheat,
	}

	if err := localTask.RegisterTasks(namedTaskFuncs); err != nil {
		logger.Errorf("register preheat tasks to local queue error: %v", err)
		return nil, err
	}

	return t, nil
}

func (t *tasks) Serve() error {
	g := errgroup.Group{}
	g.Go(func() error {
		logger.Debugf("ready to launch %d worker(s) on global queue", t.cfg.GlobalWorkerNum)
		err := t.globalTasks.LaunchWorker("global_worker", int(t.cfg.GlobalWorkerNum))
		if err != nil {
			logger.Errorf("global queue worker error: %v", err)
		}
		return err
	})

	g.Go(func() error {
		logger.Debugf("ready to launch %d worker(s) on scheduler queue", t.cfg.SchedulerWorkerNum)
		err := t.schedulerTasks.LaunchWorker("scheduler_worker", int(t.cfg.SchedulerWorkerNum))
		if err != nil {
			logger.Errorf("scheduler queue worker error: %v", err)
		}
		return err
	})

	g.Go(func() error {
		logger.Debugf("ready to launch %d worker(s) on local queue", t.cfg.LocalWorkerNum)
		err := t.localTasks.LaunchWorker("local_worker", int(t.cfg.LocalWorkerNum))
		if err != nil {
			logger.Errorf("local queue worker error: %v", err)
		}
		return err
	})

	werr := g.Wait()
	t.Stop()
	return werr
}

func (t *tasks) Stop() {
	t.globalTasks.Worker.Quit()
	t.schedulerTasks.Worker.Quit()
	t.localTasks.Worker.Quit()
}

func (t *tasks) preheat(req string) error {
	request := &internaltasks.PreheatRequest{}
	if err := internaltasks.UnmarshalRequest(req, request); err != nil {
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
	if rg := request.Headers["Range"]; len(rg) > 0 {
		meta.Range = rg
	}

	taskID := idgen.TaskID(request.URL, meta)
	logger.Debugf("ready to preheat \"%s\", taskID = %s", request.URL, taskID)

	task := supervise.NewTask(taskID, request.URL, meta)
	task, err := t.service.GetOrCreateTask(t.ctx, task)
	if err != nil {
		return dferrors.Newf(dfcodes.SchedCDNSeedFail, "create task failed: %v", err)
	}

	return getPreheatResult(task)
}

//TODO(@zzy987) check better ways to get result
func getPreheatResult(task *supervise.Task) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			switch task.GetStatus() {
			case supervise.TaskStatusFailed, supervise.TaskStatusCDNRegisterFail, supervise.TaskStatusSourceError:
				return errors.Errorf("preheat task fail")
			case supervise.TaskStatusSuccess:
				return nil
			default:
			}
		}
	}
}
