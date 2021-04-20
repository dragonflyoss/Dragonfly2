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
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/structure/sortedlist"
	"d7y.io/dragonfly/v2/pkg/structure/workqueue"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/types"
	"github.com/olekukonko/tablewriter"
)

const (
	PeerGoneTimeout      = int64(time.Second * 10)
	PeerForceGoneTimeout = int64(time.Minute * 2)
)

type PeerTaskManager struct {
	data                    *sync.Map
	dataRanger              *sync.Map
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
		dataRanger:           new(sync.Map),
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

	GetTaskManager().TouchTask(task.TaskId)

	r, ok := m.dataRanger.Load(pt.Task)
	if ok {
		ranger, ok := r.(*sortedlist.SortedList)
		if ok {
			ranger.Add(pt)
		}
	}

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
	data, ok := m.data.Load(pid)
	if ok {
		if pt, ok := data.(*types.PeerTask); ok {
			v, ok := m.dataRanger.Load(pt.Task)
			if ok {
				ranger, ok := v.(*sortedlist.SortedList)
				if ok {
					ranger.Delete(pt)
				}
			}

		}
		m.data.Delete(pid)
	}
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

func (m *PeerTaskManager) AddTask(task *types.Task) {
	m.dataRanger.LoadOrStore(task, sortedlist.NewSortedList())
}

func (m *PeerTaskManager) DeleteTask(task *types.Task) {
	// notify client cnd error
	m.data.Range(func(key, value interface{}) bool {
		peerTask, _ := value.(*types.PeerTask)
		if peerTask == nil {
			return true
		}
		if peerTask.Task != task {
			return true
		}
		if task.CDNError != nil {
			peerTask.SendError(task.CDNError)
		}
		m.data.Delete(key)
		return true
	})

	m.dataRanger.Delete(task)
}

func (m *PeerTaskManager) UpdatePeerTask(pt *types.PeerTask) {
	if pt == nil || m.dataRanger == nil {
		return
	}
	v, ok := m.dataRanger.Load(pt.Task)
	if !ok {
		return
	}
	ranger, ok := v.(*sortedlist.SortedList)
	if !ok {
		return
	}

	if pt.GetFreeLoad() == 0 || pt.IsDown() {
		ranger.Delete(pt)
		return
	}

	status := pt.GetNodeStatus()
	switch status {
	case types.PeerTaskStatusLeaveNode, types.PeerTaskStatusNodeGone:
		ranger.Delete(pt)
	default:
		ranger.UpdateOrAdd(pt)
	}
}

func (m *PeerTaskManager) Walker(task *types.Task, limit int, walker func(pt *types.PeerTask) bool) {
	if walker == nil || m.dataRanger == nil {
		return
	}
	v, ok := m.dataRanger.Load(task)
	if !ok {
		return
	}
	ranger, ok := v.(*sortedlist.SortedList)
	if !ok {
		return
	}
	ranger.RangeLimit(limit, func(data sortedlist.Item) bool {
		pt, _ := data.(*types.PeerTask)
		return walker(pt)
	})
}

func (m *PeerTaskManager) WalkerReverse(task *types.Task, limit int, walker func(pt *types.PeerTask) bool) {
	if walker == nil || m.dataRanger == nil {
		return
	}
	v, ok := m.dataRanger.Load(task)
	if !ok {
		return
	}
	ranger, ok := v.(*sortedlist.SortedList)
	if !ok {
		return
	}
	ranger.RangeReverseLimit(limit, func(data sortedlist.Item) bool {
		pt, _ := data.(*types.PeerTask)
		return walker(pt)
	})
}

func (m *PeerTaskManager) ClearPeerTask() {
	m.data.Range(func(key interface{}, value interface{}) bool {
		pt, _ := value.(*types.PeerTask)
		if pt != nil && pt.Task != nil && pt.Task.Removed {
			m.data.Delete(pt.Pid)
		}
		return true
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
		if config.GetConfig().Verbose {
			logger.Debugf(m.printDebugInfo())
		}
	}
}

func (m *PeerTaskManager) printDebugInfo() string {
	var task *types.Task
	var roots []*types.PeerTask

	buffer := bytes.NewBuffer([]byte{})
	table := tablewriter.NewWriter(buffer)
	table.SetHeader([]string{"PeerId", "Finished Piece Num", "Download Finished", "Free Load", "Peer Down"})

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
		// do not print finished node witch do not has child
		if !(peerTask.Success && peerTask.Host != nil && peerTask.Host.GetUploadLoadPercent() < 0.001) {
			table.Append([]string{peerTask.Pid, strconv.Itoa(int(peerTask.GetFinishedNum())),
				strconv.FormatBool(peerTask.Success), strconv.Itoa(int(peerTask.GetFreeLoad())), strconv.FormatBool(peerTask.IsDown())})
		}
		return
	})
	table.Render()

	var msgs []string
	msgs = append(msgs, buffer.String())

	var printTree func(node *types.PeerTask, path []string)
	printTree = func(node *types.PeerTask, path []string) {
		if node == nil {
			return
		}
		nPath := append(path, fmt.Sprintf("%s(%d)", node.Pid, node.GetSubTreeNodesNum()))
		if len(path) > 1 {
			msgs = append(msgs, node.Pid+" || "+strings.Join(nPath, "-"))
		}
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
	logger.Debugf("[%s][%s] downloadMonitorWorkingLoop refresh ", pt.Task.TaskId, pt.Pid)
	status := pt.GetNodeStatus()
	if status != types.PeerTaskStatusHealth {
		m.downloadMonitorQueue.AddAfter(pt, time.Second*2)
	} else if pt.IsWaiting() {
		m.downloadMonitorQueue.AddAfter(pt, time.Second*2)
	} else {
		delay := time.Millisecond * time.Duration(pt.GetCost()*10)
		if delay < time.Millisecond*20 {
			delay = time.Millisecond * 20
		}
		m.downloadMonitorQueue.AddAfter(pt, delay)
	}
}

func (m *PeerTaskManager) CDNCallback(pt *types.PeerTask, err *dferrors.DfError) {
	if err != nil {
		pt.SendError(err)
	} else {
		m.downloadMonitorQueue.Add(pt)
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
				logger.Debugf("[%s][%s] downloadMonitorWorkingLoop status[%d]", pt.Task.TaskId, pt.Pid, pt.GetNodeStatus())
				if pt.Success || (pt.Host != nil && pt.Host.Type == types.HostTypeCdn) {
					// clear from monitor
				} else {
					if pt.GetNodeStatus() != types.PeerTaskStatusHealth {
						// peer do not report for a long time, peer gone
						if time.Now().UnixNano() > pt.GetLastActiveTime()+PeerGoneTimeout {
							pt.SetNodeStatus(types.PeerTaskStatusNodeGone)
							pt.SendError(dferrors.New(dfcodes.SchedPeerGone, "report time out"))
						}
						m.downloadMonitorCallBack(pt)
					} else if !pt.IsWaiting() {
						m.downloadMonitorCallBack(pt)
					} else {
						if time.Now().UnixNano() > pt.GetLastActiveTime()+PeerForceGoneTimeout {
							pt.SetNodeStatus(types.PeerTaskStatusNodeGone)
							pt.SendError(dferrors.New(dfcodes.SchedPeerGone, "report fource time out"))
						}
						m.downloadMonitorCallBack(pt)
					}
					_, ok := m.GetPeerTask(pt.Pid)
					status := pt.GetNodeStatus()
					if ok && !pt.Success && status != types.PeerTaskStatusNodeGone && status != types.PeerTaskStatusLeaveNode {
						m.RefreshDownloadMonitor(pt)
					}
				}
			}
		}

		m.downloadMonitorQueue.Done(v)
	}
}
