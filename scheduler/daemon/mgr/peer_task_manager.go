package mgr

import (
	"sync"

	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

type PeerTaskManager struct {
	data *sync.Map
}

func (mgr *PeerTaskManager) GetTask(pid string) (task *types.PeerTask, err error) {
	value, ok := mgr.data.Load(pid)
	if !ok {
		return
	}
	task = value.(*types.PeerTask)
	return
}
