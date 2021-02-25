package service

import (
	"errors"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	types2 "d7y.io/dragonfly/v2/pkg/util/types"
	"d7y.io/dragonfly/v2/scheduler/types"
)

func (s *SchedulerService) GenerateTaskId(url string, filter string, meta *base.UrlMeta) (taskId string) {
	return types2.GenerateTaskId(url, filter, meta)
}

func (s *SchedulerService) GetTask(taskId string) (task *types.Task, err error) {
	task, _ = s.taskMgr.GetTask(taskId)
	if task == nil {
		err = errors.New("peer task not exited: " + taskId)
	}
	return
}

func (s *SchedulerService) AddTask(task *types.Task) (ret *types.Task, err error) {
	ret, added := s.taskMgr.AddTask(task)
	if added {
		go s.cdnMgr.TriggerTask(ret)
	}
	return
}
