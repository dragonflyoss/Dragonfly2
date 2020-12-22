package service

import (
	"crypto/md5"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

func (s *SchedulerService) GenerateTaskId(url string, filter string) (taskId string) {
	taskId = fmt.Sprintf("%x", md5.Sum([]byte(url)))
	return
}

func (s *SchedulerService) GetTask(taskId string) (task *types.Task, err error) {
	task, _ = s.taskMgr.GetTask(taskId)
	return
}

func (s *SchedulerService) AddTask(task *types.Task) (ret *types.Task, err error) {
	ret = s.taskMgr.AddTask(task)
	s.cdnMgr.TriggerTask(task)
	return
}
