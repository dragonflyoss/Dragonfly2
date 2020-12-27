package service

import "github.com/dragonflyoss/Dragonfly2/scheduler/types"

func (s *SchedulerService) Scheduler(task *types.PeerTask) (result []*types.PieceTask, err error) {
	 result, _, err = s.scheduler.Scheduler(task)
	 return
}
