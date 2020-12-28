package mgr

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"k8s.io/client-go/util/workqueue"
	"sync"
	"time"
)

type PeerTaskManager struct {
	data *sync.Map
	gcQueue workqueue.DelayingInterface
	gcDelayTime time.Duration
}

func createPeerTaskManager() *PeerTaskManager {
	delay := time.Hour
	if time.Duration(config.GetConfig().GC.PeerTaskDelay) * time.Millisecond > delay {
		delay =  time.Duration(config.GetConfig().GC.PeerTaskDelay) * time.Millisecond
	}
	ptm := &PeerTaskManager{
		data: new(sync.Map),
		gcQueue: workqueue.NewDelayingQueue(),
		gcDelayTime: delay,
	}

	go ptm.gcWorkingLoop()

	return ptm
}

func (m *PeerTaskManager) AddPeerTask(pid string, task *types.Task, host *types.Host) *types.PeerTask {
	v, ok := m.data.Load(pid)
	if ok {
		return v.(*types.PeerTask)
	}

	pt := types.NewPeerTask(pid, task, host, m.addToGCQueue)
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

func (m *PeerTaskManager) addToGCQueue(pt *types.PeerTask) {
	m.gcQueue.AddAfter(pt, m.gcDelayTime)
}

func (m *PeerTaskManager) cleanPeerTask(pt *types.PeerTask) {
	defer m.gcQueue.Done(pt)
	if pt == nil {
		return
	}
	m.data.Delete(pt.Pid)
	if pt.Host != nil {
		host, _ := GetHostManager().GetHost(pt.Host.Uuid)
		if host != nil {
			host.DeletePeerTask(pt.Pid)
			if host.GetPeerTaskNum() < 0 {
				GetHostManager().DeleteHost(pt.Host.Uuid)
			}
		}
	}
}

func (m *PeerTaskManager) gcWorkingLoop() {
	for {
		v, shutdown := m.gcQueue.Get()
		if shutdown {
			break
		}
		pt, _ := v.(*types.PeerTask)
		if pt != nil {
			m.cleanPeerTask(pt)
		}
	}
}