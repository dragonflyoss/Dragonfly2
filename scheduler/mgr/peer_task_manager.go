/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mgr

import (
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/util/workqueue"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/types"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type PeerTaskManager struct {
	data                    *sync.Map
	gcQueue                 workqueue.DelayingInterface
	gcDelayTime             time.Duration
	downloadMonitorQueue    workqueue.DelayingInterface
	downloadMonitorCallBack func(*types.PeerTask)
}

func createPeerTaskManager() *PeerTaskManager {
	delay := time.Hour
	if time.Duration(config.GetConfig().GC.PeerTaskDelay)*time.Millisecond > delay {
		delay = time.Duration(config.GetConfig().GC.PeerTaskDelay) * time.Millisecond
	}
	ptm := &PeerTaskManager{
		data:                 new(sync.Map),
		downloadMonitorQueue: workqueue.NewDelayingQueue(),
		gcQueue:              workqueue.NewDelayingQueue(),
		gcDelayTime:          delay,
	}

	go ptm.downloadMonitorWorkingLoop()

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
		m.gcQueue.Done(v)
	}
}

func (m *PeerTaskManager) printDebugInfoLoop() {
	for {
		time.Sleep(time.Second * 10)
		if config.GetConfig().Debug {
			logger.Debugf(m.printDebugInfo())
		}
	}
}

func (m *PeerTaskManager) printDebugInfo() string {
	msgMap := make(map[string]string)
	var task *types.Task
	var roots []*types.PeerTask
	m.data.Range(func(key interface{}, value interface{}) (ok bool) {
		ok = true
		peerTask, _ := value.(*types.PeerTask)
		if peerTask == nil {
			return
		}
		if task == nil {
			task = peerTask.Task
		}
		if peerTask.GetParent() == nil {
			roots = append(roots, peerTask)
		}
		msgMap[peerTask.Pid] = fmt.Sprintf("%s: finishedNum[%2d] hostLoad[%d]",
			peerTask.Pid, peerTask.GetFinishedNum(), peerTask.GetFreeLoad())
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

	var printTree func(node *types.PeerTask, path []string)
	printTree = func(node *types.PeerTask, path []string) {
		if node == nil {
			return
		}
		nPath := append(path, fmt.Sprintf("%s(%d)", node.Pid, node.GetSubTreeNodesNum()))
		msgs = append(msgs, node.Pid+" || "+strings.Join(nPath, "-"))
		for _, child := range node.GetChildren() {
			if child == nil || child.SrcPeerTask == nil {
				continue
			}
			printTree(child.SrcPeerTask, nPath)
		}
	}

	for _, root := range roots {
		printTree(root, nil)
	}

	msg := "============\n" + strings.Join(msgs, "\n") + "\n==============="
	return msg
}

func (m *PeerTaskManager) RefreshDownloadMonitor(pt *types.PeerTask) {
	if pt.GetNodeStatus() != types.PeerTaskStatusHealth {
		m.downloadMonitorQueue.AddAfter(pt, time.Second*2)
	} else if pt.IsWaiting() {
		m.downloadMonitorQueue.AddAfter(pt, time.Second*2)
	} else {
		m.downloadMonitorQueue.AddAfter(pt, time.Millisecond*time.Duration(pt.GetCost()*2))
	}
}

func (m *PeerTaskManager) SetDownloadingMonitorCallBack(callback func(*types.PeerTask)) {
	m.downloadMonitorCallBack = callback
}

func (m *PeerTaskManager) downloadMonitorWorkingLoop() {
	for {
		v, shutdown := m.downloadMonitorQueue.Get()
		if shutdown {
			break
		}
		if m.downloadMonitorCallBack != nil {
			pt, _ := v.(*types.PeerTask)
			if pt != nil {
				if pt.GetNodeStatus() != types.PeerTaskStatusHealth ||
					pt.GetParent() == nil || !pt.IsWaiting() {
					m.downloadMonitorCallBack(pt)
				}
				if _, ok := m.GetPeerTask(pt.Pid); ok && !pt.Success {
					m.RefreshDownloadMonitor(pt)
				}
			}
		}

		m.downloadMonitorQueue.Done(v)
	}
}
