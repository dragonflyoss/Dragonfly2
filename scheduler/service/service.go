package service

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler"
)

type SchedulerService struct {
	taskMgr *mgr.TaskManager
	hostMgr *mgr.HostManager
	peerTaskMgr *mgr.PeerTaskManager
	scheduler *scheduler.Scheduler
}

func CreateSchedulerService() *SchedulerService{
	s := &SchedulerService{
		taskMgr: mgr.GetTaskManager(),
		hostMgr: mgr.CreateHostManager(),
		peerTaskMgr: mgr.CreatePeerTaskManager(),
		scheduler: scheduler.CreateScheduler(),
	}
	return s
}

func (s *SchedulerService) Start() {
	s.scheduler.Start()
}

func (s *SchedulerService) GetScheduler() *scheduler.Scheduler {
	return s.scheduler
}

