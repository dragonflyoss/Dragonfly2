package service

import "github.com/dragonflyoss/Dragonfly2/scheduler/types"

func (s *SchedulerService) GetHost(hostId string) (task *types.Host, err error) {
	task, _ = s.hostMgr.GetHost(hostId)
	return
}

func (s *SchedulerService) AddHost(host *types.Host) (ret *types.Host, err error) {
	ret = s.hostMgr.AddHost(host)
	return
}
