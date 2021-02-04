package service

import "github.com/dragonflyoss/Dragonfly/v2/scheduler/types"

func (s *SchedulerService) SchedulerParent(task *types.PeerTask)  ( primary *types.PeerTask,
	secondary []*types.PeerTask, err error) {
	return s.scheduler.SchedulerParent(task)
}


func (s *SchedulerService) SchedulerChildren (task *types.PeerTask) (children []*types.PeerTask, err error)  {
	return s.scheduler.SchedulerChildren(task)
}

