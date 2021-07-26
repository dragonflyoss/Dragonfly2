package task

import (
	"context"
	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/idgen"
	internaltasks "d7y.io/dragonfly/v2/internal/tasks"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core"
	"d7y.io/dragonfly/v2/scheduler/types"
	"encoding/json"
)

const (
	preHeatTask = "preheat"
)

type Server interface {
	Serve()
}

type taskServer struct {
	globalTask *internaltasks.Tasks
	schedulerTask *internaltasks.Tasks
	localTask *internaltasks.Tasks
	ctx context.Context
	service *core.SchedulerService
}

func New(ctx context.Context, cfg *config.RedisConfig, hostname string, service *core.SchedulerService) (*taskServer, error) {
	s := &taskServer{}
	internaltasks.PutTask(preHeatTask, s.preheatHandler)
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

	return &taskServer{
		globalTask: globalTask,
		schedulerTask: schedulerTask,
		localTask: localTask,
		ctx: ctx,
		service: service,
	}, nil
}

func (s *taskServer) Serve() <-chan error {
	//wg := sync.WaitGroup{}
	//wg.Add(3)
	c := make(chan error)
	go func() {
		//defer wg.Done()
		err := s.globalTask.LaunchWorker("global_worker", 1)
		c <- err
	}()
	go func() {
		//defer wg.Done()
		err := s.schedulerTask.LaunchWorker("scheduler_worker", 1)
		c <- err
	}()
	go func() {
		//defer wg.Done()
		err := s.localTask.LaunchWorker("local_worker", 5)
		c <- err
	}()
	return c
}

func (s *taskServer) preheatHandler(requestJson string) (string, error) {
	request := &internaltasks.PreheatRequest{}
	err := json.Unmarshal([]byte(requestJson), request)
	if err != nil {
		return "", err
	}

	meta := &base.UrlMeta{Header: map[string]string{}, Digest: request.Digest, Tag: request.Tag}
	if rg := request.Headers["Range"]; len(rg) > 0 {
		meta.Range = rg
	}
	taskID := idgen.TaskID(request.URL, request.Filter, meta, request.Tag)
	task := types.NewTask(taskID, request.URL, request.Filter, request.Tag, meta)
	task, err = s.service.GetOrCreateTask(s.ctx, task)
	if err != nil {
		err = dferrors.Newf(dfcodes.SchedCDNSeedFail, "create task failed: %v", err)
		logger.Errorf("get or create task failed: %v", err)
		return "", err
	}


}
