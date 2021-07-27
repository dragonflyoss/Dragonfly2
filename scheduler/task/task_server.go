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
	"encoding/json"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"time"
)

type Task interface {
	Serve()
}

type task struct {
	globalTasks    *internaltasks.Tasks
	schedulerTasks *internaltasks.Tasks
	localTasks     *internaltasks.Tasks
	ctx            context.Context
	service        *core.SchedulerService
}

func New(ctx context.Context, cfg *config.RedisConfig, hostname string, service *core.SchedulerService) (*task, error) {
	taskConfig := &internaltasks.Config{
		Host:      cfg.Host,
		Port:      cfg.Port,
		Password:  cfg.Password,
		BrokerDB:  cfg.BrokerDB,
		BackendDB: cfg.BackendDB,
	}
	globalTask, err := internaltasks.New(taskConfig, internaltasks.GlobalQueue)
	if err != nil {
		return nil, err
	}
	schedulerTask, err := internaltasks.New(taskConfig, internaltasks.SchedulersQueue)
	if err != nil {
		return nil, err
	}
	localTask, err := internaltasks.New(taskConfig, internaltasks.GetCDNQueue(hostname))
	if err != nil {
		return nil, err
	}

	t := &task{
		globalTasks:    globalTask,
		schedulerTasks: schedulerTask,
		localTasks:     localTask,
		ctx:            ctx,
		service:        service,
	}
	err = globalTask.Server.RegisterTask(internaltasks.PreheatTask, t.preheat)
	if err != nil {
		return nil, err
	}
	err = schedulerTask.Server.RegisterTask(internaltasks.PreheatTask, t.preheat)
	if err != nil {
		return nil, err
	}
	err = localTask.Server.RegisterTask(internaltasks.PreheatTask, t.preheat)
	if err != nil {
		return nil, err
	}

	return t, nil
}

// TODO: add log
func (t *task) Serve() error {
	g := errgroup.Group{}
	g.Go(func() error {
		err := t.globalTasks.LaunchWorker("global_worker", 1)
		return err
	})
	g.Go(func() error {
		err := t.schedulerTasks.LaunchWorker("scheduler_worker", 1)
		return err
	})
	g.Go(func() error {
		err := t.localTasks.LaunchWorker("local_worker", 5)
		return err
	})
	return g.Wait()
}

func (t *task) preheat(req string) (string, error) {
	request := &internaltasks.PreheatRequest{}
	err := unmarshal(req, request)
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

	for {
		switch task.GetStatus() {
		case types.TaskStatusFailed, types.TaskStatusCDNRegisterFail, types.TaskStatusSourceError:
			return marshal(&internaltasks.PreheatResponse{Success: false})
		case types.TaskStatusSuccess:
			return marshal(&internaltasks.PreheatResponse{Success: true})
		default:
			time.Sleep(time.Second)
		}
	}
}


func unmarshal(data string, v interface{}) error {
	return json.Unmarshal([]byte(data), v)
}

func marshal(v interface{}) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}