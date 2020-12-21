package mgr

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"sync"
)

type TaskManager struct {
	data *sync.Map
}

func CreateTaskManager() *TaskManager {
	return &TaskManager{
		data: new(sync.Map),
	}
}


func (m *TaskManager) AddTask(task *types.Task) (*types.Task) {
	v, ok := m.data.Load(task.TaskId)
	if ok {
		return v.(*types.Task)
	}

	copyTask := types.CopyTask(task)

	m.data.Store(task.BizId, copyTask)
	return copyTask
}

func (m *TaskManager) DeleteTask(taskId string) {
	m.data.Delete(taskId)
	return
}

func (m *TaskManager) GetTask(taskId string) (h *types.Task, ok bool) {
	data, ok := m.data.Load(taskId)
	if !ok {
		return
	}
	h = data.(*types.Task)
	return
}

