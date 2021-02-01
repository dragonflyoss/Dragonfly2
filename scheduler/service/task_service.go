package service

import (
	"errors"
	types2 "github.com/dragonflyoss/Dragonfly2/pkg/util/types"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

func (s *SchedulerService) GenerateTaskId(url string, filter string) (taskId string) {
	return types2.GenerateTaskId(url, filter)
}

func (s *SchedulerService) GetTask(taskId string) (task *types.Task, err error) {
	task, _ = s.taskMgr.GetTask(taskId)
	if task == nil {
		err = errors.New("peer task not exited: " + taskId)
	}
	return
}

func (s *SchedulerService) AddTask(task *types.Task) (ret *types.Task, err error) {
	ret = s.taskMgr.AddTask(task)
	go s.cdnMgr.TriggerTask(ret)
	return
}
