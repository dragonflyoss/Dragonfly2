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
	"strings"
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
		logger.Errorf("create global task queue error: %v", err)
		return nil, err
	}
	schedulerTask, err := internaltasks.New(redisConfig, internaltasks.SchedulersQueue)
	if err != nil {
		logger.Errorf("create scheduler task queue error: %v", err)
		return nil, err
	}
	localQueue, err := internaltasks.GetSchedulerQueue(hostname)
	if err != nil {
		logger.Errorf("get local task queue name error: %v", err)
		return nil, err
	}
	localTask, err := internaltasks.New(redisConfig, localQueue)
	if err != nil {
		logger.Errorf("create local task queue error: %v", err)
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
		logger.Errorf("register preheat task to global queue error: %v", err)
		return nil, err
	}
	err = schedulerTask.RegisterTask(internaltasks.PreheatTask, t.preheat)
	if err != nil {
		logger.Errorf("register preheat task to scheduler queue error: %v", err)
		return nil, err
	}
	err = localTask.RegisterTask(internaltasks.PreheatTask, t.preheat)
	if err != nil {
		logger.Errorf("register preheat task to local queue error: %v", err)
		return nil, err
	}

	return t, nil
}

func (t *task) Serve() error {
	g := errgroup.Group{}
	g.Go(func() error {
		logger.Debugf("ready to launch %d worker(s) on global queue", t.cfg.GlobalWorkerNum)
		err := t.globalTasks.LaunchWorker("global_worker", t.cfg.GlobalWorkerNum)
		if err != nil {
			logger.Errorf("global queue worker error: %v", err)
		}
		return err
	})
	g.Go(func() error {
		logger.Debugf("ready to launch %d worker(s) on scheduler queue", t.cfg.SchedulerWorkerNum)
		err := t.schedulerTasks.LaunchWorker("scheduler_worker", t.cfg.SchedulerWorkerNum)
		if err != nil {
			logger.Errorf("scheduler queue worker error: %v", err)
		}
		return err
	})
	g.Go(func() error {
		logger.Debugf("ready to launch %d worker(s) on local queue", t.cfg.LocalWorkerNum)
		err := t.localTasks.LaunchWorker("local_worker", t.cfg.LocalWorkerNum)
		if err != nil {
			logger.Errorf("local queue worker error: %v", err)
		}
		return err
	})
	return g.Wait()
}

func (t *task) preheat(req string) (string, error) {
	request := &internaltasks.PreheatRequest{}
	err := internaltasks.UnmarshalRequest(req, request)
	if err != nil {
		logger.Errorf("unmarshal request err: %v, request body: %s", err, req)
		return "", err
	}
	if !urlutils.IsValidURL(request.URL) {
		logger.Errorf("request url \"%s\" is invalid", request.URL)
		return "", errors.Errorf("invalid url: %s", request.URL)
	}

	meta := &base.UrlMeta{Header: request.Headers, Tag: request.Tag, Filter: request.Filter}
	// CDN only support MD5 now.
	if strings.HasPrefix(request.Digest, "md5") {
		meta.Digest = request.Digest
	}
	if rg := request.Headers["Range"]; len(rg) > 0 {
		meta.Range = rg
	}
	taskID := idgen.TaskID(request.URL, request.Filter, meta, request.Tag)
	logger.Debugf("ready to preheat \"%s\", taskID = %s", request.URL, taskID)
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
			return "", errors.Errorf("preheat task %s fail.", taskID)
		case types.TaskStatusSuccess:
			return internaltasks.MarshalResponse(&internaltasks.PreheatResponse{})
		default:
			time.Sleep(time.Second)
		}
	}
}
