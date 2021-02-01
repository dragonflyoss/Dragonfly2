package service

import (
	"errors"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

func (s *SchedulerService) GetHost(hostId string) (host *types.Host, err error) {
	host, _ = s.hostMgr.GetHost(hostId)
	if host == nil {
		err = errors.New("peer task not exited: " + hostId)
	}
	return
}

func (s *SchedulerService) AddHost(host *types.Host) (ret *types.Host, err error) {
	ret = s.hostMgr.AddHost(host)
	return
}
