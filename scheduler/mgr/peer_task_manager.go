package mgr

import (
	"fmt"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/log"
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"k8s.io/client-go/util/workqueue"
	"sort"
	"strings"
	"sync"
	"time"
)

type PeerTaskManager struct {
	data        *sync.Map
	gcQueue     workqueue.DelayingInterface
	gcDelayTime time.Duration
}

func createPeerTaskManager() *PeerTaskManager {
	delay := time.Hour
	if time.Duration(config.GetConfig().GC.PeerTaskDelay)*time.Millisecond > delay {
		delay = time.Duration(config.GetConfig().GC.PeerTaskDelay) * time.Millisecond
	}
	ptm := &PeerTaskManager{
		data:        new(sync.Map),
		gcQueue:     workqueue.NewDelayingQueue(),
		gcDelayTime: delay,
	}

	go ptm.gcWorkingLoop()

	go ptm.printDebugInfoLoop()

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

func (m *PeerTaskManager) AddFakePeerTask(pid string, task *types.Task) *types.PeerTask {
	v, ok := m.data.Load(pid)
	if ok {
		return v.(*types.PeerTask)
	}

	pt := types.NewPeerTask(pid, task, nil, m.addToGCQueue)
	m.data.Store(pid, pt)
	pt.SetDown()
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

func (m *PeerTaskManager) Walker(walker func(pt *types.PeerTask) bool) {
	if walker == nil || m.data == nil {
		return
	}
	m.data.Range(func(key interface{}, value interface{}) bool {
		pt, _ := value.(*types.PeerTask)
		return walker(pt)
	})
}

func (m *PeerTaskManager) GetGCDelayTime() time.Duration {
	return m.gcDelayTime
}

func (m *PeerTaskManager) SetGCDelayTime(delay time.Duration) {
	m.gcDelayTime = delay
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
			if host.GetPeerTaskNum() <= 0 {
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

func (m *PeerTaskManager) printDebugInfoLoop() {
	for {
		time.Sleep(time.Second * 10)
		logger.Debugf(m.printDebugInfo())
	}
}

func (m *PeerTaskManager) printDebugInfo() string {
	msgMap := make(map[string]string)
	var task *types.Task
	m.data.Range(func(key interface{}, value interface{}) (ok bool) {
		ok = true
		peerTask, _ := value.(*types.PeerTask)
		if peerTask == nil {
			return
		}
		if task == nil {
			task = peerTask.Task
		}
		msgMap[peerTask.Pid] = fmt.Sprintf("%s: finishedNum[%2d] hostLoad[%d]",
			peerTask.Pid, peerTask.GetFinishedNum(),peerTask.GetFreeLoad())
		return
	})
	var keys, msgs []string
	for key := range msgMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		msgs = append(msgs, msgMap[key])
	}

	msgs = append(msgs, "-----")

	for i := int32(0); i <= task.GetMaxPieceNum(); i++ {
		piece := task.GetPiece(i)
		if piece == nil {
			continue
		}
		pMsg := fmt.Sprintf("piece[%2d]: ready[", piece.PieceNum)
		for _, pt := range piece.GetReadyPeerTaskList() {
			pMsg += pt.Pid + ","
		}
		pMsg += "] wait["
		for _, pt := range piece.GetWaitingPeerTaskList() {
			pMsg += pt.Pid + ","
		}
		pMsg += "]"
		msgs = append(msgs, pMsg)
	}

	msg := "============\n" + strings.Join(msgs, "\n") + "\n==============="
	return msg
}
