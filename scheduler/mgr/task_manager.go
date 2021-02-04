package mgr

import (
	"github.com/dragonflyoss/Dragonfly/v2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly/v2/scheduler/types"
	"sync"
	"time"
)

type TaskManager struct {
	lock        *sync.RWMutex
	data        map[string]*types.Task
	gcDelayTime time.Duration
}

func createTaskManager() *TaskManager {
	delay := time.Hour * 48
	if config.GetConfig().GC.TaskDelay > 0 {
		delay = time.Duration(config.GetConfig().GC.TaskDelay) * time.Millisecond
	}
	tm := &TaskManager{
		lock:        new(sync.RWMutex),
		data:        make(map[string]*types.Task),
		gcDelayTime: delay,
	}
	go tm.gcWorkingLoop()
	return tm
}

func (m *TaskManager) AddTask(task *types.Task) (*types.Task, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	v, ok := m.data[task.TaskId]
	if ok {
		return v, false
	}

	copyTask := types.CopyTask(task)

	m.data[task.TaskId] = copyTask
	return copyTask, true
}

func (m *TaskManager) DeleteTask(taskId string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.data, taskId)
	return
}

func (m *TaskManager) GetTask(taskId string) (h *types.Task, ok bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	h, ok = m.data[taskId]
	return
}

func (m *TaskManager) gcWorkingLoop() {
	for {
		time.Sleep(time.Hour)
		var needDeleteKeys []string
		m.lock.RLock()
		for taskId, task := range m.data {
			if task != nil && time.Now().After(task.CreateTime.Add(m.gcDelayTime)) {
				needDeleteKeys = append(needDeleteKeys, taskId)
			}
		}
		m.lock.RUnlock()

		if len(needDeleteKeys) > 0 {
			m.lock.Lock()
			for _, taskId := range needDeleteKeys {
				delete(m.data, taskId)
			}
			m.lock.Unlock()
		}
	}
}
