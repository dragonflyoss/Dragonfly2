package mgr

import (
	"sync"

	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

type TaskManager struct {
	data *sync.Map
}

func (mgr *TaskManager) GetTask(bizId string) (task *types.Task, err error) {
	value, ok := mgr.data.Load(bizId)
	if !ok {
		return
	}
	task = value.(*types.Task)
	return
}
