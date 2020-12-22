package service

import "github.com/dragonflyoss/Dragonfly2/scheduler/types"

func (s *SchedulerService) GetPeerTask(peerTaskId string) (peerTask *types.PeerTask, err error) {
	peerTask, _ = s.peerTaskMgr.GetPeerTask(peerTaskId)
	return
}

func (s *SchedulerService) AddPeerTask(pid string, task *types.Task, host *types.Host) (ret *types.PeerTask, err error) {
	ret = s.peerTaskMgr.AddPeerTask(pid, task, host)
	host.AddPeerTask(ret)
	return
}

func (s *SchedulerService) DeletePeerTask(peerTaskId string) (err error) {
	peerTask, err := s.GetPeerTask(peerTaskId)
	if err != nil {
		return
	}
	// delete from manager
	s.peerTaskMgr.DeletePeerTask(peerTaskId)
	// delete from host
	peerTask.Host.DeletePeerTask(peerTaskId)
	// delete from piece lazy
	peerTask.SetDown()
	return
}
