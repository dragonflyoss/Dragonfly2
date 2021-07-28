package task

import (
	"context"
	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/idgen"
	internaltasks "d7y.io/dragonfly/v2/internal/tasks"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core"
	"d7y.io/dragonfly/v2/scheduler/types"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"time"
)

type Task interface {
	Serve() error
}

type task struct {
	globalTasks    *internaltasks.Tasks
	schedulerTasks *internaltasks.Tasks
	localTasks     *internaltasks.Tasks
	ctx            context.Context
	service        *core.SchedulerService
	cfg            *config.TaskConfig
}

func New(ctx context.Context, cfg *config.TaskConfig, hostname string, service *core.SchedulerService) (Task, error) {
	redisConfig := &internaltasks.Config{
		Host:      cfg.Redis.Host,
		Port:      cfg.Redis.Port,
		Password:  cfg.Redis.Password,
		BrokerDB:  cfg.Redis.BrokerDB,
		BackendDB: cfg.Redis.BackendDB,
	}
	globalTask, err := internaltasks.New(redisConfig, internaltasks.GlobalQueue)
	if err != nil {
		return nil, err
	}
	schedulerTask, err := internaltasks.New(redisConfig, internaltasks.SchedulersQueue)
	if err != nil {
		return nil, err
	}
	localQueue, err := internaltasks.GetSchedulerQueue(hostname)
	if err != nil {
		return nil, err
	}
	localTask, err := internaltasks.New(redisConfig, localQueue)
	if err != nil {
		return nil, err
	}

	t := &task{
		globalTasks:    globalTask,
		schedulerTasks: schedulerTask,
		localTasks:     localTask,
		ctx:            ctx,
		service:        service,
		cfg:            cfg,
	}
	err = globalTask.RegisterTask(internaltasks.PreheatTask, t.preheat)
	if err != nil {
		return nil, err
	}
	err = schedulerTask.RegisterTask(internaltasks.PreheatTask, t.preheat)
	if err != nil {
		return nil, err
	}
	err = localTask.RegisterTask(internaltasks.PreheatTask, t.preheat)
	if err != nil {
		return nil, err
	}

	return t, nil
}

// TODO: add log
func (t *task) Serve() error {
	g := errgroup.Group{}
	g.Go(func() error {
		err := t.globalTasks.LaunchWorker("global_worker", t.cfg.GlobalWorkerNum)
		return err
	})
	g.Go(func() error {
		err := t.schedulerTasks.LaunchWorker("scheduler_worker", t.cfg.SchedulerWorkerNum)
		return err
	})
	g.Go(func() error {
		err := t.localTasks.LaunchWorker("local_worker", t.cfg.LocalWorkerNum)
		return err
	})
	return g.Wait()
}

func (t *task) preheat(req string) (string, error) {
	request := &internaltasks.PreheatRequest{}
	err := internaltasks.UnmarshalRequest(req, request)
	if err != nil {
		return "", err
	}
	if !urlutils.IsValidURL(request.URL) {
		return "", errors.Errorf("invalid url: %s", request.URL)
	}

	meta := &base.UrlMeta{Header: map[string]string{}, Digest: request.Digest, Tag: request.Tag}
	if rg := request.Headers["Range"]; len(rg) > 0 {
		meta.Range = rg
	}
	taskID := idgen.TaskID(request.URL, request.Filter, meta, request.Tag)
	task := types.NewTask(taskID, request.URL, request.Filter, request.Tag, meta)
	task, err = t.service.GetOrCreateTask(t.ctx, task)
	if err != nil {
		err = dferrors.Newf(dfcodes.SchedCDNSeedFail, "create task failed: %v", err)
		logger.Errorf("get or create task failed: %v", err)
		return "", err
	}

	//TODO: check better ways to get result
	for {
		switch task.GetStatus() {
		case types.TaskStatusFailed, types.TaskStatusCDNRegisterFail, types.TaskStatusSourceError:
			return internaltasks.MarshalResult(&internaltasks.PreheatResponse{})
		case types.TaskStatusSuccess:
			return internaltasks.MarshalResult(&internaltasks.PreheatResponse{})
		default:
			time.Sleep(time.Second)
		}
	}
}
