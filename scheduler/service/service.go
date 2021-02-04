package service

import (
	"d7y.io/dragonfly/v2/scheduler/mgr"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
)

type SchedulerService struct {
	cdnMgr      *mgr.CDNManager
	taskMgr     *mgr.TaskManager
	hostMgr     *mgr.HostManager
	peerTaskMgr *mgr.PeerTaskManager
	scheduler   *scheduler.Scheduler
}

func CreateSchedulerService() *SchedulerService {
	s := &SchedulerService{
		cdnMgr:      mgr.GetCDNManager(),
		taskMgr:     mgr.GetTaskManager(),
		hostMgr:     mgr.GetHostManager(),
		peerTaskMgr: mgr.GetPeerTaskManager(),
		scheduler:   scheduler.CreateScheduler(),
	}
	s.cdnMgr.InitCDNClient()
	return s
}

func (s *SchedulerService) GetScheduler() *scheduler.Scheduler {
	return s.scheduler
}
