package mgr

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"sync"
	"time"
)

type TaskManager struct {
	data *sync.Map
	gcDelayTime time.Duration
}

func createTaskManager() *TaskManager {
	delay := time.Hour * 48
	if  config.GetConfig().GC.TaskDelay > 0 {
		delay = time.Duration(config.GetConfig().GC.TaskDelay) * time.Millisecond
	}
	tm := &TaskManager{
		data: new(sync.Map),
		gcDelayTime: delay,
	}
	go tm.gcWorkingLoop()
	return tm
}

func (m *TaskManager) AddTask(task *types.Task) *types.Task {
	v, ok := m.data.Load(task.TaskId)
	if ok {
		return v.(*types.Task)
	}

	copyTask := types.CopyTask(task)

	m.data.Store(task.TaskId, copyTask)
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

func (m *TaskManager) gcWorkingLoop() {
	for {
		time.Sleep(time.Hour)
		m.data.Range(func(key interface{}, value interface{}) bool {
			task, _ := value.(*types.Task)
			if task != nil && time.Now().After(task.CreateTime.Add(m.gcDelayTime)) {
				m.data.Delete(key)
			}
			return true
		})
	}
}
