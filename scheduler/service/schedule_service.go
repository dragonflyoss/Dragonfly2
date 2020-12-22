package service

import "github.com/dragonflyoss/Dragonfly2/scheduler/types"

func (s *SchedulerService) Scheduler(task *types.PeerTask) (result []*types.PieceTask, err error) {
	return s.scheduler.Scheduler(task)
}
