package mgr

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"sync"
)

type PeerTaskManager struct {
	data *sync.Map
}

func CreatePeerTaskManager() *PeerTaskManager {
	return &PeerTaskManager{
		data: new(sync.Map),
	}
}

func (m *PeerTaskManager) AddPeerTask(pid string, task *types.Task, host *types.Host) *types.PeerTask {
	v, ok := m.data.Load(pid)
	if ok {
		return v.(*types.PeerTask)
	}

	pt := types.NewPeerTask(pid, task, host)
	m.data.Store(pid, pt)
	return pt
}

func (m *PeerTaskManager) DeletePeerTask(pid string) {
	m.data.Delete(pid)
	return
}

func (m *PeerTaskManager) GetPeerTask(pid string) (h *types.PeerTask, ok bool) {
	data, ok := m.data.Load(pid)
	if !ok {
		return
	}
	h = data.(*types.PeerTask)
	return
}
